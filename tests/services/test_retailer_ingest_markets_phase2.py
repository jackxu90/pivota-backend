"""Multi-market storefronts ADR, Phase 2 (approved by Peng 2026-09-26): acquisition markets and the
session-verified Shopify-Markets capture.

  "Allow USD offers from non-USD stores that show US buyers USD prices, but only when the store's cart
   confirms USD for a US session -- brand stores first."

Every rule here has a test that fails when the rule is removed (each section names its refusing twin):
  1. AU/JP are ACQUISITION markets: stored with their real market + currency, never served, and an AU job
     refuses to run in a process where nothing would stop an AUD row from serving.
  2. Currency = market wherever this lane writes: the offer builder, the apply chokepoint, the capture.
  3. The seed partition stays US (external_product_seeds.market); only catalog_offers.market moves.
  4. The readback: scoped to the job's storefront; an acquisition row lands stored-not-served, and one
     that reads back SERVED without a served-region price is the leak it fails on.
  5. source=shopify_markets: /meta.json ships to US -> multipart /localization -> /cart.js USD BEFORE any
     price -> /products/<handle>.js in-session, re-checked; siblings for base-crawl rows only.
  6. End to end, in the operator's order: an AUD-base store's AU crawl lands unservable; the capture adds
     USD siblings; the content_keys become serving-eligible for US only.
Store shapes are the ADR's measured ones (section 1.1: Go-To, gotoskincare.com, AUD base, ships to 227
countries, prices USD to US visitors via Shopify Markets).
"""
from __future__ import annotations

import json
import sqlite3
from urllib.parse import urlsplit

import httpx
import pytest

import services.agent_decision_gates as gates
import services.index_pipeline_state_service as ips
from db import retailer_ingest as ledger_db
from services import curated_brand_feed as feed, storefront_currency
from services.catalog_enrichment_agent import apply as writer
from services.catalog_enrichment_agent.ingestion import SEED_PARTITION_MARKET, ingest_validated_jsonl
from services.catalog_offer_writer_guard import LIVE_SKU_KEYS_SQL
from services.catalog_onboard_worker import normalize_curated_brand_payload
from services.crawl_politeness import RobotsDisallowed
from services.region_pricing import has_offer_priced_for_any_region_sql, require_market_currency
from services.retailer_ingest import pipeline, shopify_markets as markets
from tests.services.test_retailer_ingest_explicit_market import _Catalog, quiet_writer  # noqa: F401 -- fixture
from tests.services.test_retailer_ingest_pipeline import env, job  # noqa: F401 -- the state-machine fixture

HOST, BRAND = "gotoskincare.com", "Go-To"
GOTO_META = {"name": "Go-To Skin Care", "myshopify_domain": "go-to-skincare.myshopify.com", "currency": "AUD",
             "ships_to_countries": ["AU", "NZ", "US"]}


@pytest.fixture(autouse=True)
def _clean_meta_cache():
    storefront_currency.clear_cache()
    yield
    storefront_currency.clear_cache()


@pytest.fixture
def gates_on(monkeypatch):
    """The agent-decision gate as prod runs it (ENABLE_KBEAUTY_AGENT_DECISION_GATES=true)."""
    monkeypatch.setattr(gates, "agent_decision_gates_enabled", lambda: True)
    monkeypatch.setattr(ips, "agent_decision_gates_enabled", lambda: True)
    monkeypatch.setattr(gates, "evidence_gates_enabled", lambda: False)


def goto_product(handle, title, variants, *, currency="AUD", pid=None, ptype="Face Oil",
                 body="<p>A face oil for dry skin: squalane, rosehip and jojoba.</p>"):
    return feed.shopify_product_to_record(
        {"id": pid or abs(hash(handle)) % 10**9, "vendor": BRAND, "title": title, "handle": handle,
         "product_type": ptype, "body_html": body,
         "images": [{"src": f"https://cdn.shopify.com/goto/{handle}.jpg"}], "variants": variants},
        domain=HOST, category_path="beauty", brand_override=BRAND, currency=currency,
        source_role="brand_official", emit_native_variants=True)


def goto_records(currency="AUD"):
    return [
        goto_product("face-hero", "Face Hero 30ml", [
            {"id": 44101000000001, "price": "45.00", "available": True, "sku": "FH30"},
            {"id": 44101000000002, "price": "80.00", "available": False, "sku": "FH60"}], currency=currency, pid=41),
        goto_product("exceptionally-clever-cleanser", "Exceptionally Clever Cleanser", [
            {"id": 44201000000001, "price": "39.00", "available": True, "sku": "ECC"}], currency=currency, pid=42,
            ptype="Cleanser", body="<p>A gentle gel cleanser that lifts makeup without stripping skin.</p>"),
    ]


#: What the store quotes a US session (cents) vs its AUD base -- merchant-set, not FX (A$45 -> US$32).
USD_CENTS = {44101000000001: 3200, 44101000000002: 5800, 44201000000001: 2700}
AUD_CENTS = {44101000000001: 4500, 44101000000002: 8000, 44201000000001: 3900}
HANDLE_VARIANTS = {"face-hero": [44101000000001, 44101000000002], "exceptionally-clever-cleanser": [44201000000001]}


def markets_job(status="queued", **options):
    return {"id": "rij_m", "domain": HOST, "brand": BRAND, "status": status, "attempts": 0, "max_attempts": 6,
            "options": {"vendors": [BRAND], "source_role": "brand_official", "source": "shopify_markets", **options}}


def au_job(status="queued", **options):
    return {"id": "rij_au", "domain": HOST, "brand": BRAND, "status": status, "attempts": 0, "max_attempts": 6,
            "options": {"vendors": [BRAND], "source_role": "brand_official", "market": "AU", **options}}


# ================================================================== 1. acquisition markets: options

@pytest.mark.parametrize("market,currency", [("AU", "AUD"), ("JP", "JPY"), ("au", "AUD")])
def test_au_and_jp_are_ingest_markets_with_their_own_currency(market, currency):
    o = pipeline.validate_options({"vendors": ["X"], "market": market})
    assert pipeline.job_market(o) == market.upper() and pipeline.job_currency(o) == currency
    assert pipeline.validate_options({"vendors": ["X"], "market": market, "require_currency": currency})


@pytest.mark.parametrize("market,wrong", [("AU", "USD"), ("JP", "USD"), ("AU", "JPY")])
def test_an_acquisition_job_cannot_claim_another_currency(market, wrong):
    with pytest.raises(ValueError, match=f"is not market {market}'s currency"):
        pipeline.validate_options({"vendors": ["X"], "market": market, "require_currency": wrong})


def test_the_acquisition_markets_are_ingest_markets_and_us_is_not_one():
    assert set(pipeline.ACQUISITION_MARKETS) == {"AU", "JP"} <= set(pipeline.INGEST_MARKETS)
    assert pipeline.DEFAULT_MARKET not in pipeline.ACQUISITION_MARKETS


def test_an_acquisition_market_is_a_base_currency_storefront_crawl_only():
    from tests.services.test_affiliate_feed_source import FEED as feed_opts
    assert pipeline.validate_options({"vendors": ["X"], "source": "affiliate_feed", "feed": dict(feed_opts)})
    with pytest.raises(ValueError, match="acquisition market"):
        pipeline.validate_options({"vendors": ["X"], "market": "AU", "source": "affiliate_feed", "feed": feed_opts})


def test_the_onboard_queue_still_refuses_every_market_but_us():
    """Its drain stamps no market on its offers, so an AU row there would be AUD stamped 'US'."""
    with pytest.raises(ValueError, match="market US only"):
        normalize_curated_brand_payload({"domain": HOST, "brand": BRAND, "market": "AU"})
    assert normalize_curated_brand_payload({"domain": HOST, "brand": BRAND, "market": "AU"},
                                           markets=pipeline.INGEST_MARKETS)["market"] == "AU"


def test_the_lane_payload_carries_the_acquisition_market_and_its_currency():
    payload = pipeline.ingest_payload(au_job())
    assert (payload["market"], payload["require_currency"]) == ("AU", "AUD")


def test_an_au_job_is_its_own_cohort():
    base = {"vendors": [BRAND], "source_role": "brand_official"}
    assert ledger_db.scope_key(HOST, BRAND, {**base, "market": "AU"}) != ledger_db.scope_key(HOST, BRAND, base)
    capture = {**base, "source": "shopify_markets"}
    assert ledger_db.scope_key(HOST, BRAND, capture) not in {
        ledger_db.scope_key(HOST, BRAND, base), ledger_db.scope_key(HOST, BRAND, {**base, "market": "AU"})}


def test_enqueue_accepts_both_jobs_of_the_runbook_and_refuses_a_retailer_capture():
    from scripts.enqueue_retailer_ingest import _row_to_job
    au = _row_to_job({"domain": HOST, "brand": BRAND, "vendors": [BRAND],
                      "options": {"source_role": "brand_official", "market": "AU"}})
    assert au["options"]["market"] == "AU"
    us = _row_to_job({"domain": HOST, "brand": BRAND, "vendors": [BRAND],
                      "options": {"source_role": "brand_official", "source": "shopify_markets"}})
    assert us["options"]["source"] == "shopify_markets"
    with pytest.raises(ValueError, match="retailers follow later"):
        _row_to_job({"domain": "kiokii.com", "brand": "DHC", "vendors": ["DHC"],
                     "options": {"source_role": "retailer", "source": "shopify_markets"}})


# ================================================================== 1b. acquisition markets: never served

async def test_an_au_job_refuses_to_run_where_nothing_stops_its_rows_serving(env, monkeypatch):  # noqa: F811
    monkeypatch.setattr(gates, "agent_decision_gates_enabled", lambda: False)
    env.crawl_error = AssertionError("must not crawl")
    out = await pipeline.run_stage(au_job(), db=env.db)
    assert (out["status"], out["outcome"]) == ("failed", "acquisition_unguarded")
    assert "ENABLE_KBEAUTY_AGENT_DECISION_GATES" in out["reason"]


async def test_a_us_job_needs_no_such_gate(env, monkeypatch):  # noqa: F811
    monkeypatch.setattr(gates, "agent_decision_gates_enabled", lambda: False)
    assert (await pipeline.run_stage(job(), db=env.db))["status"] == "apply_due"


def _sqlite_offers(offers):
    """catalog_products + catalog_offers in SQLite, enough for the REAL served-region predicate."""
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE catalog_products (product_key text, content_key text)")
    con.execute("CREATE TABLE catalog_offers (offer_id text, product_key text, currency text, market text, "
                "list_price real, merchant_effective_price real, suppressed_at text)")
    for o in offers:
        con.execute("INSERT INTO catalog_offers VALUES (?,?,?,?,?,?,NULL)",
                    (o["offer_id"], o["product_key"], o["currency"], o.get("market") or "US", o["list_price"],
                     o.get("merchant_effective_price")))
    for key in {o["product_key"] for o in offers}:
        con.execute("INSERT INTO catalog_products VALUES (?, ?)", (key, "ck:" + key))
    return con


def _priced_for(con, product_key, regions):
    sql = f"SELECT {has_offer_priced_for_any_region_sql('cp.product_key', regions)} FROM catalog_products cp " \
          f"WHERE cp.product_key = ?"
    return bool(con.execute(sql, (product_key,)).fetchone()[0])


def _classify(has_serving_region_offer, has_acquisition_market_offer=None):
    """The index's REAL verdict for a content-complete row (services/index_pipeline_state_service)."""
    row = {"content_key": "ck", "sync_status": "live", "pdp_sync_status": "live", "pdp_title": "Face Hero",
           "seed_title": "Face Hero", "pdp_description": "x" * 200, "image_url": "https://cdn/x.jpg",
           "content_quality_score": 90.0, "has_price": True, "has_us_offer": has_serving_region_offer,
           "has_serving_region_offer": has_serving_region_offer, "product_group_id": "pg",
           "category_path": "beauty/skincare/face-oil",
           "has_acquisition_market_offer": has_acquisition_market_offer}
    return ips._classify_product(row, set())


@pytest.mark.parametrize("currency,market", [("AUD", "AU"), ("JPY", "JP")])
@pytest.mark.parametrize("regions", [["US"], ["US", "SG"], ["SG"]])
def test_acquisition_rows_are_priced_for_no_served_region_and_never_serve(gates_on, currency, market, regions):
    """The serving gate is the offer's CURRENCY against PIVOTA_SERVING_PRICING_REGIONS (live: US,SG). An
    AUD/JPY offer, whatever its market stamp, satisfies neither US nor SG: blocked as no_us_offer."""
    con = _sqlite_offers([{"offer_id": "o1", "product_key": "p1", "currency": currency, "market": market,
                           "list_price": 45.0}])
    assert _priced_for(con, "p1", regions) is False
    verdict = _classify(_priced_for(con, "p1", regions))
    assert (verdict["serving_eligible"], verdict["blocker_code"]) == (False, gates.BLOCKER_NO_US_OFFER)


def test_the_default_serving_region_is_us_and_au_is_not_served():
    assert "AU" not in ips.serving_pricing_regions() and "JP" not in ips.serving_pricing_regions()


# ================================================================== 2/3. writer: currency = market, seeds stay US

def test_an_au_plan_stamps_every_offer_au_in_aud_and_keeps_its_seeds_in_the_us_partition():
    plan = ingest_validated_jsonl(goto_records("AUD"), market="AU")
    assert len(plan["offers"]) == 5  # 2 canonical + 3 variant offers
    assert {(o["market"], o["currency"]) for o in plan["offers"]} == {("AU", "AUD")}
    # The seed is the SERVING PARTITION, not a destination claim: 'US', with its true price currency.
    assert SEED_PARTITION_MARKET == "US"
    assert {(s["market"], s["price_currency"]) for s in plan["seeds"]} == {("US", "AUD")}


def test_a_declared_market_refuses_a_record_priced_in_another_currency():
    with pytest.raises(ValueError, match="currency_market_mismatch: an offer for market AU must be priced in AUD"):
        ingest_validated_jsonl(goto_records("USD"), market="AU")
    with pytest.raises(ValueError, match="currency_market_mismatch"):
        ingest_validated_jsonl(goto_records("AUD"), market="US")


def test_an_undeclared_market_writes_exactly_what_it_wrote_before():
    """Every other lane (and the SG rows: market 'US', currency 'SGD') declares nothing: no market bind."""
    legacy = ingest_validated_jsonl(goto_records("AUD"))
    assert all("market" not in o for o in legacy["offers"])
    declared = ingest_validated_jsonl(goto_records("AUD"), market="AU")
    # Same rows, same ids: the declaration adds the stamp and changes nothing else.
    assert [{k: v for k, v in o.items() if k != "market"} for o in declared["offers"]] == legacy["offers"]


@pytest.mark.parametrize("market,currency", [("US", "USD"), ("au", "aud"), ("JP", " JPY ")])
def test_require_market_currency_accepts_the_markets_own(market, currency):
    assert require_market_currency(market, currency) == market.upper()


@pytest.mark.parametrize("market,currency", [("US", "AUD"), ("AU", None), ("AU", ""), ("DE", "EUR"), ("", "USD")])
def test_require_market_currency_refuses_everything_else(market, currency):
    with pytest.raises(ValueError):
        require_market_currency(market, currency)


def test_the_market_upsert_differs_from_the_legacy_one_only_by_the_market_insert_column():
    legacy, declared = writer._OFFER_UPSERT_SQL, writer._OFFER_UPSERT_MARKET_SQL
    assert declared.replace("is_first_party, market,", "is_first_party,").replace(
        ":is_first_party, :market,", ":is_first_party,") == legacy
    conflict = declared.split("ON CONFLICT", 1)[1]
    assert "market" not in conflict and "currency =" not in conflict  # first-written, never restamped
    assert writer._offer_sql_for({"market": "AU"}) is declared
    assert writer._offer_sql_for({}) is legacy and writer._offer_sql_for({"market": None}) is legacy


@pytest.mark.parametrize("batch", [False, True])
async def test_the_apply_writes_the_declared_market_and_leaves_legacy_rows_byte_identical(quiet_writer, batch):  # noqa: F811
    db = _Catalog()
    await writer.apply_ingest_plan(ingest_validated_jsonl(goto_records("AUD"), market="AU"), batch_label="t",
                                   db=db, batch=batch, market="AU")
    offers = db.written("catalog_offers")
    assert len(offers) == 5 and {o["market"] for o in offers} == {"AU"}
    assert all("market" in q for q, v in db.statements if "INSERT INTO catalog_offers" in q)
    legacy_db = _Catalog()
    await writer.apply_ingest_plan(ingest_validated_jsonl(goto_records("AUD")), batch_label="t", db=legacy_db,
                                   batch=batch)
    stmts = [q for q, v in legacy_db.statements if "INSERT INTO catalog_offers" in q]
    assert stmts and all(", market," not in q for q in stmts)
    assert all("market" not in o for o in legacy_db.written("catalog_offers"))


async def test_the_apply_refuses_a_mis_priced_declared_offer_before_writing_anything(quiet_writer):  # noqa: F811
    plan = ingest_validated_jsonl(goto_records("AUD"), market="AU")
    plan["offers"][3]["currency"] = "USD"  # a plan edited anywhere after the builder
    db = _Catalog()
    with pytest.raises(ValueError, match="currency_market_mismatch"):
        await writer.apply_ingest_plan(plan, batch_label="t", db=db, market="AU")
    assert db.statements == []


# ================================================================== 4. readback

class _RB:
    def __init__(self, **row):
        self.row, self.sql, self.values = row, None, None

    async def fetch_all(self, sql, values):
        self.sql, self.values = sql, values
        base = {"category_path": "beauty/skincare/face-oil", "serving": False, "pipeline_stage": "quality_gated",
                "blocker_code": "no_us_offer", "lifecycle": "published", "row_priced": True, "row_image": True,
                "row_identity": True, "offers": 3, "offers_in_currency": 3, "offers_in_market": 3,
                "content_served_region_priced": False}
        return [{**base, "product_key": k, **self.row} for k in values["keys"]]


async def test_an_acquisition_row_blocked_as_no_us_offer_is_stored_not_served():
    db = _RB()
    out = await pipeline._readback(["p1"], "AUD", db, market="AU", domain=HOST)
    assert out["ok"], out["problems"]
    assert [n["kind"] for n in out["notes"]] == ["acquisition_not_served"]
    assert (db.values["currency"], db.values["market"]) == ("AUD", "AU")


async def test_no_us_offer_on_a_us_job_is_still_a_problem():
    out = await pipeline._readback(["p1"], "USD", _RB(), market="US", domain=HOST)
    assert out["problems"] == [{"product_key": "p1", "problem": "not serving-eligible (blocker no_us_offer)"}]


@pytest.mark.parametrize("row", [{"row_priced": False}, {"row_identity": False}])
async def test_no_us_offer_does_not_excuse_an_acquisition_row_that_lost_its_price_or_identity(row):
    out = await pipeline._readback(["p1"], "AUD", _RB(**row), market="AU", domain=HOST)
    assert not out["ok"]


async def test_a_served_acquisition_row_without_a_served_region_price_is_the_leak():
    out = await pipeline._readback(["p1"], "AUD", _RB(serving=True, blocker_code="none"), market="AU", domain=HOST)
    assert not out["ok"] and "must never serve on its AUD offer" in out["problems"][0]["problem"]
    fine = await pipeline._readback(["p1"], "AUD", _RB(serving=True, blocker_code="none",
                                                       content_served_region_priced=True), market="AU", domain=HOST)
    assert fine["ok"], fine["problems"]


async def test_the_leak_detector_asks_the_served_regions_on_the_content_key(monkeypatch):
    monkeypatch.setenv("PIVOTA_SERVING_PRICING_REGIONS", "US,SG")
    db = _RB()
    await pipeline._readback(["p1"], "AUD", db, market="AU", domain=HOST)
    assert "cp.content_key = p.content_key" in db.sql and "'USD'" in db.sql and "'SGD'" in db.sql
    us = _RB()
    await pipeline._readback(["p1"], "USD", us, market="US", domain=HOST)
    assert "content_served_region_priced" not in us.sql  # US SQL is unchanged by the acquisition branch


async def test_the_readback_counts_only_the_jobs_storefronts_offers():
    db = _RB()
    await pipeline._readback(["p1"], "AUD", db, market="AU", domain="www.GoToSkincare.com")
    assert db.values["hosts"] == ["gotoskincare.com", "www.gotoskincare.com"]
    assert db.sql.count("lower(o.source_domain) = ANY(:hosts)") == 3
    # ...and to the offers THIS lane writes there: a shopify_markets sibling on the same host is not the
    # crawl's (review of #2358, D1).
    assert db.sql.count("o.source_system = :lane_source_system") == 3
    assert db.values["lane_source_system"] == "catalog_enrichment_agent_v1" != markets.SOURCE_SYSTEM
    unscoped = _RB()
    await pipeline._readback(["p1"], "AUD", unscoped, market="AU")
    assert "hosts" not in unscoped.values and ":hosts" not in unscoped.sql


async def test_the_apply_reads_back_the_jobs_own_storefront(env, monkeypatch):  # noqa: F811
    seen = []
    real = pipeline._readback

    async def spy(*a, **kw):
        seen.append(kw.get("domain"))
        return await real(*a, **kw)
    monkeypatch.setattr(pipeline, "_readback", spy)
    await pipeline.run_stage(job("apply_due"), db=env.db)
    assert seen == ["k-touch.us"]


@pytest.mark.parametrize("owned", [1, writer.CANONICAL_OWNER_KEPT_CAP + 1])
async def test_an_au_job_over_products_the_us_store_owns_attaches_offers_and_ends_done(
        env, monkeypatch, quiet_writer, gates_on, owned):  # noqa: F811
    """frankbody.com (AU home store) after us.frankbody.com (the US store, canonical owner): the REAL apply
    keeps us.frankbody.com's copy (the canonical-owner guard, live for AU), writes the AUD offers stamped
    AU, and the REAL apply gate accepts a readiness report whose canonical_urls are the owner's because the
    apply tallied them as kept for THIS host. Without that, the gate failed the job after its writes -- and
    with the gate reading the 50-row display sample, it still did at 51 products (review of #2358, D2)."""
    from services.catalog_enrichment_agent import primary_ingestion as pi, primary_readiness as pr
    from tests.services.test_retailer_ingest_explicit_market import _REAL_APPLY, _REAL_REQUIRE_APPLY

    monkeypatch.setattr(writer, "apply_ingest_plan", _REAL_APPLY)
    monkeypatch.setattr(pi, "require_primary_apply", _REAL_REQUIRE_APPLY)
    scrubs = [feed.shopify_product_to_record(
        {"id": 8101 + i, "vendor": "Frank Body", "title": f"Original Coffee Scrub No {i} 200g",
         "handle": f"original-coffee-scrub-{i}", "product_type": "Scrub",
         "body_html": "<p>A coffee scrub that polishes rough skin.</p>",
         "images": [{"src": f"https://cdn.shopify.com/fb/scrub-{i}.jpg"}],
         "variants": [{"id": 44810200000001 + i, "price": "24.95", "available": True, "sku": f"OCS{i}"}]},
        domain="frankbody.com", category_path="beauty", brand_override="Frank Body", currency="AUD",
        source_role="brand_official", emit_native_variants=True) for i in range(owned)]

    async def crawl(**kw):
        batch = feed.ShopifyProductBatch(scrubs, scanned_products=owned, pages=1)
        batch.crawl_report["storefront"] = {"name": "Frank Body", "myshopify_domain": "letsbefrank.myshopify.com",
                                            "currency": "AUD", "ships_to_countries": ["AU", "NZ"]}
        return batch
    monkeypatch.setattr(feed, "records_for_brand", crawl)
    keys = [p["product_key"] for p in ingest_validated_jsonl(scrubs)["pdps"]]

    class Owned(_Catalog):
        async def fetch_all(self, query, values=None):
            if "offers_in_market" in query:  # served: the content_key carries us.frankbody.com's USD offer
                return [{"product_key": k, "category_path": "beauty/body/scrub", "serving": True,
                         "pipeline_stage": "public_indexed", "blocker_code": "none", "lifecycle": "published",
                         "row_priced": True, "row_image": True, "row_identity": True, "offers": 1,
                         "offers_in_currency": 1, "offers_in_market": 1, "content_served_region_priced": True}
                        for k in values["keys"]]
            return await super().fetch_all(query, values)
    db = Owned([{"product_key": key, "brand": "Frank Body", "source_domain": "us.frankbody.com",
                 "canonical_url": f"https://us.frankbody.com/products/scrub-{i}",
                 "image_url": "https://cdn.shopify.com/fbus/scrub.jpg",
                 "product_payload": json.dumps({"enrichment_meta": {"source_role": "brand_official"}})}
                for i, key in enumerate(keys)])

    async def readiness(plan, *, db):
        written = {r["product_key"]: r for r in db.written("catalog_products")}
        return {"status": "complete", "products": [
            {"product_key": k, "canonical_url": written[k]["canonical_url"]} for k in sorted(written)]}
    monkeypatch.setattr(pr, "materialize_primary_readiness", readiness)

    j = {"id": "rij_fb", "domain": "frankbody.com", "brand": "Frank Body", "status": "apply_due", "attempts": 0,
         "max_attempts": 6, "options": {"vendors": ["Frank Body"], "source_role": "brand_official", "market": "AU"}}
    out = await pipeline.run_stage(j, db=db)
    assert (out["status"], out["outcome"]) == ("done", "applied"), env.ledger.transitions[-1]
    assert len(db.written("catalog_products")) == owned
    assert all(r["canonical_url"].startswith("https://us.frankbody.com/") for r in db.written("catalog_products"))
    assert {(o["market"], o["currency"], o["source_domain"]) for o in db.written("catalog_offers")} == {
        ("AU", "AUD", "frankbody.com")}


# ================================================================== 5. shopify_markets: options

@pytest.mark.parametrize("role", [None, "retailer"])
def test_the_capture_is_brand_stores_only(role):
    o = {"vendors": [BRAND], "source": "shopify_markets"}
    if role:
        o["source_role"] = role
    with pytest.raises(ValueError, match="retailers follow later"):
        pipeline.validate_options(o)


@pytest.mark.parametrize("market", ["AU", "JP"])
def test_the_capture_writes_a_served_market_only(market):
    # US only until 2026-10-10; SG since (tests/services/test_retailer_ingest_markets_sg.py).
    with pytest.raises(ValueError, match="shopify_markets captures the US and SG markets only"):
        pipeline.validate_options({"vendors": [BRAND], "source": "shopify_markets", "source_role": "brand_official",
                                   "market": market})


async def test_a_retailer_capture_job_fails_before_any_request(env, monkeypatch):  # noqa: F811
    monkeypatch.setattr(markets, "HTTP_TRANSPORT", httpx.MockTransport(lambda r: pytest.fail("no request")))
    out = await pipeline.run_stage(markets_job(source_role="retailer"), db=env.db)
    assert (out["status"], out["outcome"]) == ("failed", "invalid_job")


# ================================================================== 5b. the capture itself

class Store:
    """A Shopify-Markets storefront: AUD base; a session whose multipart PUT to /localization names US gets
    a localization cookie, and then /cart.js and /products/<h>.js answer in USD."""

    def __init__(self, *, meta=None, honors_localization=True, overrides=None, loses_session_after=None):
        self.meta = {**GOTO_META, **(meta or {})}
        self.honors, self.overrides = honors_localization, dict(overrides or {})
        self.loses_session_after = loses_session_after
        self.log, self.carts = [], 0

    def handler(self, request):
        path = request.url.path
        self.log.append((request.method, request.url.host, path, request.url.query.decode()))
        if (request.method, path) in self.overrides:
            return self.overrides[(request.method, path)](request)
        us = "localization=US" in request.headers.get("cookie", "")
        if path == "/meta.json":
            return httpx.Response(200, json=self.meta)
        if path == "/":
            return httpx.Response(200, text="<html>store</html>", headers={"content-type": "text/html"})
        if path == "/localization" and request.method == "POST":
            body = request.content
            multipart = b"multipart/form-data" in request.headers.get("content-type", "").encode()
            says_us = (b'name="_method"' in body and b"put" in body and b'name="country_code"' in body
                       and b"\r\n\r\nUS\r\n" in body)
            headers = {"location": "/"}
            if self.honors and multipart and says_us:
                headers["set-cookie"] = "localization=US; Path=/"
            return httpx.Response(302, headers=headers)
        if path == "/cart.js":
            self.carts += 1
            if self.loses_session_after is not None and self.carts > self.loses_session_after:
                us = False
            return httpx.Response(200, json={"currency": "USD" if us else self.meta["currency"], "item_count": 0})
        if path.startswith("/products/") and path.endswith(".js"):
            handle = path[len("/products/"):-3]
            if handle not in HANDLE_VARIANTS:
                return httpx.Response(404, text="not found", headers={"content-type": "text/html"})
            cents = USD_CENTS if us else AUD_CENTS
            return httpx.Response(200, json={
                "id": 1, "handle": handle, "price": min(cents[v] for v in HANDLE_VARIANTS[handle]),
                "variants": [{"id": v, "price": cents[v], "available": v != 44101000000002} for v in HANDLE_VARIANTS[handle]]})
        return httpx.Response(404)

    def paths(self):
        return [(m, p) for m, _h, p, _q in self.log]


class Polite:
    """crawl_politeness as the capture must call it: every request gated (robots + pacing), every answer
    noted. It has no await_slot on purpose: nothing may take the pacing without the robots check."""

    def __init__(self, disallow=()):
        self.calls, self.disallow = [], set(disallow)

    async def robots_allows(self, url, *, user_agent):
        path = urlsplit(url).path
        self.calls.append(("robots", path))
        return path not in self.disallow

    async def before_request(self, url, *, user_agent, max_wait):
        path = urlsplit(url).path
        self.calls.append(("robots+slot", path))
        if path in self.disallow:
            raise RobotsDisallowed(f"robots.txt disallows {url}")

    def note_response(self, url, status_code, *, retry_after=None, headers=None):
        # crawl_politeness.note_response's signature: the capture passes the response headers (#2473).
        self.calls.append(("note", urlsplit(url).path, status_code))


def base_rows(plan=None):
    """The candidate rows CANDIDATES_SQL returns for a base-currency (AU) crawl's plan."""
    plan = plan or ingest_validated_jsonl(goto_records("AUD"), market="AU")
    skus = {s["sku_key"]: s for s in plan["skus"]}
    pdps = {p["product_key"]: p for p in plan["pdps"]}
    return [{"base_offer_id": o["offer_id"], "product_key": o["product_key"], "sku_key": o["sku_key"],
             "source_ref": o["source_ref"], "currency": o["currency"], "market": o["market"],
             "content_key": pdps[o["product_key"]]["content_key"], "brand": pdps[o["product_key"]]["brand"],
             "source_variant_id": skus[o["sku_key"]]["source_variant_id"]} for o in plan["offers"]]


class CandidatesDB:
    def __init__(self, rows):
        self.rows, self.reads = rows, []

    async def fetch_all(self, sql, values):
        assert sql == markets.CANDIDATES_SQL
        self.reads.append(values)
        return [dict(r) for r in self.rows]


async def _capture(store, rows=None, *, polite=None, max_products=200):
    return await markets.capture(markets_job(), db=CandidatesDB(base_rows() if rows is None else rows),
                                 max_products=max_products, polite=polite or Polite(),
                                 transport=httpx.MockTransport(store.handler))


async def test_a_proven_us_session_prices_every_base_offer_in_usd():
    store = Store()
    out = await _capture(store)
    planned = {(r["sku_key"].rsplit("::", 1)[-1], r["list_price"], r["availability"]) for r in out["planned"]}
    # canonical = the base crawl's own rule (first sellable variant); each variant SKU its own variant.
    assert planned == {("canonical", 32.0, "in_stock"), ("v:44101000000001", 32.0, "in_stock"), ("v:44101000000002", 58.0, "out_of_stock"),
                       ("canonical", 27.0, "in_stock"), ("v:44201000000001", 27.0, "in_stock")}
    assert {(r["market"], r["currency"], r["source_system"]) for r in out["planned"]} == {
        ("US", "USD", "shopify_markets_us_localization")}
    ev = out["checks"]["markets_capture"]
    assert (ev["cart_currency"], ev["localization_status"], ev["storefront"]["ships_to_countries"]) == (
        "USD", 302, ["AU", "NZ", "US"])
    assert ev["robots"] == {"session_paths_checked": ["/localization", "/cart.js"]}
    payload = json.loads(out["planned"][0]["offer_payload"])
    assert payload["cart_currency"] == "USD" and payload["base_currency"] == "AUD"


async def test_each_sibling_hangs_on_its_base_offer_and_is_stable_across_runs():
    rows = base_rows()
    out = await _capture(Store(), rows)
    by_base = {r["base_offer_id"]: r for r in out["planned"]}
    assert set(by_base) == {r["base_offer_id"] for r in rows}
    for base in rows:
        sib = by_base[base["base_offer_id"]]
        assert (sib["product_key"], sib["sku_key"]) == (base["product_key"], base["sku_key"])
        assert sib["offer_id"] == markets.sibling_offer_id(base["base_offer_id"]) != base["base_offer_id"]
        assert sib["offer_id"].startswith("offer:shopify_markets_us:")
    again = await _capture(Store(), rows)
    assert [r["offer_id"] for r in again["planned"]] == [r["offer_id"] for r in out["planned"]]


async def test_the_cart_is_proven_usd_before_any_price_is_read_and_no_country_query_is_ever_sent():
    store = Store()
    await _capture(store)
    paths = store.paths()
    first_price = next(i for i, (m, p) in enumerate(paths) if p.startswith("/products/"))
    first_cart = paths.index(("GET", "/cart.js"))
    assert paths.index(("POST", "/localization")) < first_cart < first_price
    assert paths[-1] == ("GET", "/cart.js")  # the final re-check commits what was read
    assert all("country" not in q for _m, _h, _p, q in store.log)


async def test_a_store_whose_cart_stays_aud_is_a_clean_refusal_with_no_price_read():
    store = Store(honors_localization=False)
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    r = refused.value
    assert (r.outcome, r.status, r.transient) == ("us_session_unproven", "nothing", False)
    assert "reports AUD" in r.reason
    assert r.checks["markets_capture"]["cart_currency"] == "AUD"
    assert not any(p.startswith("/products/") for _m, p in store.paths())


async def test_a_urlencoded_localization_is_not_a_us_session():
    """The script's probe: a urlencoded POST without _method silently no-ops. The capture must send
    MULTIPART, or this store (which honours only multipart) never confirms USD."""
    store = Store()
    await _capture(store)  # passes only because the POST was multipart with _method=put, country_code=US
    posted = [e for e in store.log if e[0] == "POST"]
    assert posted == [("POST", HOST, "/localization", "")]


async def test_a_store_that_does_not_ship_to_us_is_refused_before_any_session():
    store = Store(meta={"ships_to_countries": ["AU", "NZ"]})
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    assert (refused.value.outcome, refused.value.status) == ("store_does_not_ship_to_us", "nothing")
    assert store.paths() == [("GET", "/meta.json")]


async def test_a_usd_base_store_is_not_captured():
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(Store(meta={"currency": "USD", "ships_to_countries": ["US"]}))
    assert refused.value.outcome == "base_currency_is_usd"


def _answer(status, **kw):
    return lambda request: httpx.Response(status, **kw)


@pytest.mark.parametrize("where", [("GET", "/cart.js"), ("POST", "/localization"), ("GET", "/meta.json"),
                                   ("GET", "/products/face-hero.js")])
async def test_a_403_anywhere_is_unverifiable_never_worked_around(where):
    store = Store(overrides={where: _answer(403, text="denied", headers={"content-type": "text/html"})})
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    assert (refused.value.outcome, refused.value.status, refused.value.transient) == (
        "capture_unverifiable", "failed", False)
    assert sum(1 for m, p in store.paths() if (m, p) == where) == 1  # asked once, never retried


@pytest.mark.parametrize("status", [429, 503])
async def test_a_throttle_is_transient(status):
    store = Store(overrides={("GET", "/cart.js"): _answer(status, headers={"retry-after": "30"})})
    polite = Polite()
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store, polite=polite)
    assert refused.value.transient is True
    assert ("note", "/cart.js", status) in polite.calls  # the gate backs the host off


async def test_a_bot_wall_that_answers_200_html_is_unverifiable():
    store = Store(overrides={("GET", "/cart.js"): _answer(200, text="<html>Are you human?</html>",
                                                          headers={"content-type": "text/html"})})
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    assert refused.value.outcome == "capture_unverifiable" and "bot wall" in refused.value.reason


async def test_a_redirect_to_another_regional_store_is_unverifiable():
    def geo_router(request):
        if request.url.host == HOST:
            return httpx.Response(302, headers={"location": "https://us.gotoskincare.com/"})
        return httpx.Response(200, text="<html>US store</html>", headers={"content-type": "text/html"})
    store = Store(overrides={("GET", "/"): geo_router})
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    assert refused.value.outcome == "capture_unverifiable" and "us.gotoskincare.com" in refused.value.reason


async def test_a_session_that_stops_reporting_usd_voids_the_capture(monkeypatch):
    monkeypatch.setattr(markets, "SESSION_RECHECK_EVERY", 1)
    store = Store(loses_session_after=1)  # the first /cart.js proves USD, the re-check does not
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    assert (refused.value.outcome, refused.value.status) == ("us_session_lost", "failed")


async def test_the_session_is_re_checked_every_n_products(monkeypatch):
    monkeypatch.setattr(markets, "SESSION_RECHECK_EVERY", 1)
    store = Store()
    out = await _capture(store)
    assert store.paths().count(("GET", "/cart.js")) == 1 + 2  # the proof, then after each of 2 products
    assert out["checks"]["markets_capture"]["session_rechecks"] == 2


async def test_every_request_is_robots_checked_and_paced_with_no_special_case():
    polite = Polite()
    store = Store()
    await _capture(store, polite=polite)
    gated = [c[1] for c in polite.calls if c[0] == "robots+slot"]
    assert gated == [p for _m, p in store.paths()]  # every request, the session endpoints included
    assert {"/localization", "/cart.js"} <= set(gated)
    assert len([c for c in polite.calls if c[0] == "note"]) == len(gated)
    # ...and robots was asked about both session paths before the first request to the store.
    first = polite.calls.index(("robots+slot", "/meta.json"))
    assert [c for c in polite.calls[:first] if c[0] == "robots"] == [("robots", "/localization"), ("robots", "/cart.js")]


@pytest.mark.parametrize("path", ["/localization", "/cart.js"])
async def test_a_store_that_disallows_a_session_path_is_refused_before_any_request(path):
    store = Store()
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store, polite=Polite(disallow={path}))
    r = refused.value
    assert (r.outcome, r.status, r.transient) == ("robots_disallowed", "nothing", False)
    assert r.reason.startswith(f"robots_disallowed:{path} ")
    assert r.checks["markets_capture"]["robots"]["disallowed"] == path
    assert store.log == []  # not one request to the store, least of all to the disallowed path


async def test_a_robots_disallowed_product_read_is_refused():
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(Store(), polite=Polite(disallow={"/products/face-hero.js"}))
    assert (refused.value.outcome, refused.value.status) == ("robots_disallowed", "nothing")
    assert refused.value.reason.startswith("robots_disallowed:/products/face-hero.js ")


# ---- the same, through the REAL crawl gate: crawl_politeness's own parser, cache and fail-open policy

@pytest.fixture
def real_gate(monkeypatch):
    """crawl_politeness itself, fed robots.txt through its ROBOTS_TRANSPORT_FACTORY seam."""
    from services import crawl_politeness

    monkeypatch.setenv("CRAWL_MIN_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("CRAWL_ROBOTS_ENABLED", "true")
    crawl_politeness.reset_for_tests()
    served = {}

    def robots(request):
        served.setdefault("hits", 0)
        served["hits"] += 1
        answer = served["answer"]
        if isinstance(answer, Exception):
            raise answer
        status, body = answer
        return httpx.Response(status, text=body, headers={"content-type": "text/plain"})
    token = crawl_politeness.ROBOTS_TRANSPORT_FACTORY.set(lambda: httpx.MockTransport(robots))
    yield served
    crawl_politeness.ROBOTS_TRANSPORT_FACTORY.reset(token)
    crawl_politeness.reset_for_tests()


#: The entries Shopify's Help Center lists as its default's "key entries" (2026-09-26), verbatim.
SHOPIFY_DEFAULT_KEY_ENTRIES = """User-agent: *
Disallow: /admin
Disallow: /cart/
Disallow: /checkout
Disallow: /checkouts/
Disallow: /account
Disallow: /orders
Disallow: /collections/*+*
Disallow: /collections/*sort_by*
"""


async def _real_capture(store):
    return await markets.capture(markets_job(), db=CandidatesDB(base_rows()), max_products=200,
                                 transport=httpx.MockTransport(store.handler))


@pytest.mark.parametrize("path", ["/cart.js", "/localization"])
@pytest.mark.parametrize("agent", ["*", "PivotaCommerceIndex"])
async def test_the_real_gate_refuses_a_disallowed_session_path(real_gate, path, agent):
    real_gate["answer"] = (200, f"User-agent: {agent}\nDisallow: {path}\n")
    store = Store()
    with pytest.raises(markets.MarketsRefused) as refused:
        await _real_capture(store)
    assert (refused.value.outcome, refused.value.status) == ("robots_disallowed", "nothing")
    assert refused.value.reason.startswith(f"robots_disallowed:{path} ") and store.log == []


async def test_shopify_default_cart_rule_does_not_block_cart_js(real_gate):
    """`Disallow: /cart/` (the published default, WITH the slash) is not a prefix of /cart.js."""
    real_gate["answer"] = (200, SHOPIFY_DEFAULT_KEY_ENTRIES)
    store = Store()
    out = await _real_capture(store)
    assert len(out["planned"]) == 5 and ("GET", "/cart.js") in store.paths()
    assert real_gate["hits"] == 1  # one robots.txt fetch, then the gate's cache


async def test_another_agents_disallow_is_not_ours(real_gate):
    real_gate["answer"] = (200, "User-agent: Googlebot\nDisallow: /cart.js\nDisallow: /localization\n")
    assert len((await _real_capture(Store()))["planned"]) == 5


@pytest.mark.parametrize("answer", [(404, "not found"), (503, "unavailable"), httpx.ConnectError("refused")])
async def test_a_robots_fetch_failure_is_what_the_crawl_gate_does_everywhere_fail_open(real_gate, answer):
    """crawl_politeness caches a robots.txt it could not read (404, 5xx, a transport error) as "no
    restrictions" for every path of every lane; the capture neither tightens nor loosens that."""
    real_gate["answer"] = answer
    assert len((await _real_capture(Store()))["planned"]) == 5


async def test_only_this_brands_rows_on_this_host_in_the_stores_base_currency_are_captured():
    rows = base_rows()
    other_brand = {**rows[0], "brand": "Sukin", "base_offer_id": "o-sukin"}
    other_host = {**rows[0], "source_ref": "https://us.gotoskincare.com/products/face-hero", "base_offer_id": "o-us"}
    stale = {**rows[0], "currency": "NZD", "base_offer_id": "o-nzd"}
    out = await _capture(Store(), rows + [other_brand, other_host, stale])
    assert {r["base_offer_id"] for r in out["planned"]} == {r["base_offer_id"] for r in rows}
    skipped = out["checks"]["markets_capture"]["skipped"]
    assert skipped["no_handle_on_this_host"] == 1 and skipped["base_currency_not_the_stores"] == 1


async def test_no_base_rows_means_run_the_base_crawl_first():
    store = Store()
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store, rows=[])
    assert (refused.value.outcome, refused.value.status) == ("no_base_rows", "nothing")
    assert "base-currency crawl" in refused.value.reason and store.log == []


async def test_a_delisted_product_and_a_missing_or_token_priced_variant_are_skipped_and_counted(monkeypatch):
    monkeypatch.setitem(USD_CENTS, 44101000000002, 50)  # US$0.50: a token price, not an offer (MIN_SELLABLE_PRICE)
    rows = base_rows() + [{**base_rows()[1], "base_offer_id": "o-gone", "product_key": "ext:gone",
                           "source_ref": f"https://{HOST}/products/discontinued"}]
    rows[-1]["sku_key"] = "ext:gone::canonical"
    out = await _capture(Store(), rows)
    skipped = out["checks"]["markets_capture"]["skipped"]
    assert skipped["product_js_http_404"] == 1 and skipped["no_sellable_usd_price"] == 1
    assert "v:44101000000002" not in {r["sku_key"].rsplit("::", 1)[-1] for r in out["planned"]}


async def test_too_many_products_is_capped_not_truncated():
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(Store(), max_products=1)
    assert refused.value.outcome == "crawl_capped"


def test_a_handle_comes_only_from_a_product_url():
    assert markets.handle_from_url(f"https://{HOST}/products/face-hero?variant=1") == "face-hero"
    assert markets.handle_from_url(f"https://{HOST}/collections/face") is None
    assert markets.handle_from_url(None) is None


# ================================================================== 5c. the sibling writer

def test_the_sibling_upsert_reads_its_identity_from_the_base_offer_and_never_restamps_money():
    sql = markets.SIBLING_UPSERT_SQL
    for col in ("base.sku_key", "base.product_key", "base.merchant_id", "base.offer_type", "base.is_first_party",
                "base.source_ref", "base.source_domain"):
        assert col in sql
    conflict = sql.split("ON CONFLICT", 1)[1]
    assert "market =" not in conflict.split("WHERE")[0] and "currency =" not in conflict.split("WHERE")[0]
    assert "catalog_offers.currency = EXCLUDED.currency" in conflict and "suppressed_at IS NULL" in conflict


class WriteDB:
    def __init__(self, *, dead_skus=(), retired=()):
        self.dead, self.retired, self.upserts = set(dead_skus), set(retired), []

    async def fetch_all(self, sql, values):
        assert sql == LIVE_SKU_KEYS_SQL
        return [{"sku_key": k} for k in values["sku_keys"] if k not in self.dead]

    async def fetch_val(self, sql, values):
        assert sql == markets.SIBLING_UPSERT_SQL
        self.upserts.append(values)
        return None if values["base_offer_id"] in self.retired else values["offer_id"]


async def test_the_writer_counts_what_landed_and_refuses_a_dead_sku():
    planned = (await _capture(Store()))["planned"]
    dead = planned[0]["sku_key"]
    retired = planned[1]["base_offer_id"]
    db = WriteDB(dead_skus={dead}, retired={retired})
    out = await markets.write_siblings(planned, db=db)
    assert out["refused"] == {"orphan_no_sku": sum(1 for p in planned if p["sku_key"] == dead)}
    assert out["not_written"] == [p["offer_id"] for p in planned if p["base_offer_id"] == retired]
    assert {w["offer_id"] for w in out["written"]} == {
        p["offer_id"] for p in planned if p["sku_key"] != dead and p["base_offer_id"] != retired}
    assert {(u["market"], u["currency"]) for u in db.upserts} == {("US", "USD")}


async def test_the_writer_refuses_a_sibling_that_is_not_usd():
    planned = (await _capture(Store()))["planned"]
    planned[2]["currency"] = "AUD"
    db = WriteDB()
    with pytest.raises(ValueError, match="currency_market_mismatch"):
        await markets.write_siblings(planned, db=db)
    assert db.upserts == []


class ReadbackDB:
    def __init__(self, **landed):
        self.landed = landed

    async def fetch_all(self, sql, values):
        assert sql == markets.READBACK_SQL
        base = {"product_key": "p1", "currency": "USD", "market": "US", "live": True, "content_key": "ck1",
                "source_system": markets.SOURCE_SYSTEM, "serving": True, "blocker_code": "none"}
        return [{**base, "offer_id": oid, **self.landed} for oid in values["offer_ids"]]


@pytest.mark.parametrize("landed", [{"currency": "AUD"}, {"market": "AU"}, {"live": False}])
async def test_a_sibling_that_did_not_land_usd_us_and_live_fails_the_readback(landed):
    written = [{"offer_id": "offer:shopify_markets_us:1"}]
    assert (await markets.readback(written, db=ReadbackDB()))["ok"]
    out = await markets.readback(written, db=ReadbackDB(**landed))
    assert not out["ok"] and out["problems"][0]["offer_id"] == "offer:shopify_markets_us:1"


async def test_a_republish_failure_fails_the_readback():
    out = await markets.readback([{"offer_id": "o"}], db=ReadbackDB(), republish_failed=["ck1"])
    assert not out["ok"] and "republish" in out["problems"][0]["problem"]


# ================================================================== 6. end to end, in the operator's order

class SqliteCatalog:
    """The catalog the two jobs write and read, in SQLite: every statement the lane sends is answered by
    SQL over these rows, and serving is the index's REAL verdict (_classify_product) fed the REAL
    served-region predicate (region_pricing) evaluated here."""

    def __init__(self):
        self.con = sqlite3.connect(":memory:")
        self.con.row_factory = sqlite3.Row
        self.con.executescript("""
            CREATE TABLE catalog_products (product_key text PRIMARY KEY, content_key text, brand text,
                category_path text, canonical_url text, suppressed_at text);
            CREATE TABLE catalog_skus (sku_key text PRIMARY KEY, product_key text, source_variant_id text,
                suppressed_at text, suppression_reason text);
            CREATE TABLE catalog_offers (offer_id text PRIMARY KEY, sku_key text, product_key text, merchant_id text,
                offer_type text, is_first_party int, market text NOT NULL DEFAULT 'US', currency text,
                list_price real, merchant_effective_price real, source_system text, source_ref text,
                source_domain text, offer_payload text, suppressed_at text);
            CREATE TABLE external_product_seeds (id text PRIMARY KEY, market text, price_currency text,
                attached_product_key text);
        """)
        self.republished, self.republish_sources = [], []

    def load_plan(self, plan):
        for p in plan["pdps"]:
            self.con.execute("INSERT OR REPLACE INTO catalog_products VALUES (?,?,?,?,?,NULL)",
                             (p["product_key"], p["content_key"], p["brand"], p["category_path"], p["canonical_url"]))
        for s in plan["skus"]:
            self.con.execute("INSERT OR REPLACE INTO catalog_skus VALUES (?,?,?,NULL,NULL)",
                             (s["sku_key"], s["product_key"], s["source_variant_id"]))
        for o in plan["offers"]:
            self.con.execute(  # a re-run of the crawl UPSERTS its own rows, as _OFFER_UPSERT_SQL does
                "INSERT OR REPLACE INTO catalog_offers (offer_id, sku_key, product_key, merchant_id, offer_type, is_first_party,"
                " market, currency, list_price, merchant_effective_price, source_system, source_ref, source_domain,"
                " offer_payload) VALUES (?,?,?,?,?,?,coalesce(?, 'US'),?,?,?,?,?,?,?)",
                (o["offer_id"], o["sku_key"], o["product_key"], o["merchant_id"], o["offer_type"], o["is_first_party"],
                 o.get("market"), o["currency"], o["list_price"], o["merchant_effective_price"], o["source_system"],
                 o["source_ref"], o["source_domain"], o["offer_payload"]))
        for s in plan["seeds"]:
            self.con.execute("INSERT OR REPLACE INTO external_product_seeds VALUES (?,?,?,?)",
                             (s["id"], s["market"], s["price_currency"], s["attached_product_key"]))

    def priced_for(self, content_key, regions):
        sql = (f"SELECT {has_offer_priced_for_any_region_sql('cp.product_key', regions)} "
               f"FROM catalog_products cp WHERE cp.content_key = ?")
        return any(row[0] for row in self.con.execute(sql, (content_key,)))

    def verdict(self, content_key):
        acquisition = any(r[0] for r in self.con.execute(
            f"SELECT {ips._HAS_ACQUISITION_MARKET_OFFER_EXISTS} FROM catalog_products cp WHERE cp.content_key = ?",
            (content_key,)))
        return _classify(self.priced_for(content_key, ips.serving_pricing_regions()), acquisition)

    def _scope_clause(self, sql, values):
        """The readback's storefront scope, translated clause for clause from what the real SQL says."""
        clause, args = "", []
        if ":hosts" in sql:
            clause += " AND lower(o.source_domain) IN (%s)" % ",".join("?" * len(values["hosts"]))
            args += list(values["hosts"])
        if ":lane_source_system" in sql:
            clause += " AND o.source_system = ?"
            args.append(values["lane_source_system"])
        return clause, args

    async def fetch_all(self, sql, values):
        if "offers_in_market" in sql:  # the crawl job's readback
            out = []
            clause, hosts = self._scope_clause(sql, values)
            for key in values["keys"]:
                p = self.con.execute("SELECT * FROM catalog_products WHERE product_key = ?", (key,)).fetchone()
                v = self.verdict(p["content_key"])

                def count(extra="", args=()):
                    return self.con.execute("SELECT count(*) FROM catalog_offers o WHERE o.product_key = ? AND "
                                            "o.suppressed_at IS NULL" + clause + extra, (key, *hosts, *args)).fetchone()[0]
                out.append({"product_key": key, "category_path": p["category_path"], "serving": v["serving_eligible"],
                            "pipeline_stage": v["pipeline_stage"], "blocker_code": v["blocker_code"],
                            "blocker_detail": v["blocker_detail"], "lifecycle": "published", "row_priced": True,
                            "row_image": True, "row_identity": True, "offers": count(),
                            "offers_in_currency": count(" AND o.currency = ?", (values["currency"],)),
                            "offers_in_market": count(" AND upper(o.market) = ?", (values["market"],)),
                            "content_served_region_priced": self.priced_for(p["content_key"],
                                                                            ips.serving_pricing_regions())})
            return out
        if sql == markets.CANDIDATES_SQL:
            marks = ",".join("?" * len(values["hosts"]))
            rows = self.con.execute(
                f"SELECT co.offer_id AS base_offer_id, co.product_key, co.sku_key, co.source_ref, upper(co.currency) "
                f"AS currency, upper(co.market) AS market, cp.content_key, cp.brand, s.source_variant_id "
                f"FROM catalog_offers co JOIN catalog_products cp ON cp.product_key = co.product_key "
                f"JOIN catalog_skus s ON s.sku_key = co.sku_key WHERE lower(co.source_domain) IN ({marks}) "
                f"AND co.source_system = ? AND co.suppressed_at IS NULL AND cp.suppressed_at IS NULL "
                f"AND s.suppressed_at IS NULL AND upper(co.currency) <> ? ORDER BY co.product_key, co.sku_key",
                (*values["hosts"], values["base_source_system"], values["capture_currency"])).fetchall()
            return [dict(r) for r in rows]
        if sql == LIVE_SKU_KEYS_SQL:
            marks = ",".join("?" * len(values["sku_keys"]))
            return [dict(r) for r in self.con.execute(
                f"SELECT sku_key FROM catalog_skus WHERE sku_key IN ({marks}) AND suppressed_at IS NULL "
                f"AND suppression_reason IS NULL", values["sku_keys"])]
        if sql == markets.READBACK_SQL:
            marks = ",".join("?" * len(values["offer_ids"]))
            out = []
            for r in self.con.execute(
                    f"SELECT o.offer_id, o.product_key, o.currency, o.market, (o.suppressed_at IS NULL) AS live, "
                    f"o.source_system, p.content_key FROM catalog_offers o JOIN catalog_products p "
                    f"ON p.product_key = o.product_key WHERE o.offer_id IN ({marks})", values["offer_ids"]):
                v = self.verdict(r["content_key"])
                out.append({**dict(r), "live": bool(r["live"]), "serving": v["serving_eligible"],
                            "blocker_code": v["blocker_code"], "blocker_detail": v["blocker_detail"]})
            return out
        raise AssertionError(f"unexpected statement: {sql[:120]}")

    async def fetch_val(self, sql, values):
        assert sql == markets.SIBLING_UPSERT_SQL
        base = self.con.execute(
            "SELECT o.* FROM catalog_offers o JOIN catalog_products cp ON cp.product_key = o.product_key "
            "JOIN catalog_skus s ON s.sku_key = o.sku_key WHERE o.offer_id = ? AND o.suppressed_at IS NULL "
            "AND cp.suppressed_at IS NULL AND s.suppressed_at IS NULL", (values["base_offer_id"],)).fetchone()
        if base is None:
            return None
        self.con.execute(
            "INSERT INTO catalog_offers (offer_id, sku_key, product_key, merchant_id, offer_type, is_first_party, "
            "market, currency, list_price, merchant_effective_price, source_system, source_ref, source_domain, "
            "offer_payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (offer_id) DO UPDATE SET "
            "list_price = excluded.list_price, merchant_effective_price = excluded.merchant_effective_price",
            (values["offer_id"], base["sku_key"], base["product_key"], base["merchant_id"], base["offer_type"],
             base["is_first_party"], values["market"], values["currency"], values["list_price"],
             values["merchant_effective_price"], values["source_system"], base["source_ref"], base["source_domain"],
             values["offer_payload"]))
        return values["offer_id"]

    def offers(self, where="1=1", args=()):
        return [dict(r) for r in self.con.execute(f"SELECT * FROM catalog_offers WHERE {where}", args)]


@pytest.fixture
def two_jobs(env, monkeypatch, gates_on):  # noqa: F811
    """The runbook's two jobs against one SqliteCatalog: the AU crawl's real plan is what the capture reads."""
    from services.catalog_enrichment_agent import apply as apply_mod, primary_ingestion as pi

    catalog = SqliteCatalog()
    applied = []

    async def crawl(**kw):
        batch = feed.ShopifyProductBatch(goto_records("AUD"), scanned_products=2, pages=1)
        batch.crawl_report["storefront"] = dict(GOTO_META)
        return batch
    monkeypatch.setattr(feed, "records_for_brand", crawl)

    async def apply(plan, **kw):
        applied.append((plan, kw.get("market")))
        catalog.load_plan(plan)
        return {"pdps": len(plan["pdps"]), "skus": len(plan["skus"]), "offers": len(plan["offers"])}
    monkeypatch.setattr(apply_mod, "apply_ingest_plan", apply)

    def require_apply(preflight, counts):
        plan = applied[-1][0]
        return {"status": "applied", "missing": {}, "applied": {**counts, "primary_readiness": {
            "status": "complete",
            "products": [{"product_key": p["product_key"], "canonical_url": p["canonical_url"]} for p in plan["pdps"]]}}}
    monkeypatch.setattr(pi, "require_primary_apply", require_apply)

    async def republish(content_keys, *, db, source_system=markets.SOURCE_SYSTEM):
        catalog.republished.extend(content_keys)
        catalog.republish_sources.append(source_system)
        return []
    monkeypatch.setattr(markets, "republish", republish)
    store = Store()
    monkeypatch.setattr(markets, "HTTP_TRANSPORT", httpx.MockTransport(store.handler))
    monkeypatch.setattr(markets, "crawl_politeness", Polite())
    return env, catalog, applied, store


async def test_an_aud_base_store_becomes_us_servable_only_through_its_usd_siblings(two_jobs):
    env, catalog, applied, store = two_jobs
    # --- Operator step 1: the base-currency crawl, market AU.
    dry = await pipeline.run_stage(au_job(), db=catalog)
    assert dry["status"] == "apply_due", list(env.ledger.runs.values())[-1].get("flags")
    out = await pipeline.run_stage(au_job("apply_due"), db=catalog)
    assert (out["status"], out["outcome"]) == ("done", "applied"), env.ledger.transitions[-1]
    assert "stored, not served (acquisition market)" in env.ledger.transitions[-1]["reason"]
    (plan, market), = applied
    assert market == "AU"
    base = catalog.offers()
    assert len(base) == 5 and {(o["market"], o["currency"]) for o in base} == {("AU", "AUD")}
    seeds = [dict(r) for r in catalog.con.execute("SELECT * FROM external_product_seeds")]
    assert seeds and {(s["market"], s["price_currency"]) for s in seeds} == {("US", "AUD")}
    content_keys = sorted({p["content_key"] for p in plan["pdps"]})
    for ck in content_keys:  # stored, not served: for the US, and for SG too
        assert catalog.verdict(ck)["blocker_code"] == gates.BLOCKER_NO_US_OFFER
        assert not catalog.priced_for(ck, ["US"]) and not catalog.priced_for(ck, ["US", "SG"])
    assert store.log == []  # the crawl job never touched a session

    # --- Operator step 2: the Shopify-Markets capture, market US.
    dry = await pipeline.run_stage(markets_job(), db=catalog)
    assert (dry["status"], dry["outcome"]) == ("apply_due", "clean")
    assert catalog.offers("market = 'US'") == []  # a dry run writes nothing
    done = await pipeline.run_stage(markets_job("apply_due"), db=catalog)
    assert (done["status"], done["outcome"]) == ("done", "applied"), env.ledger.transitions[-1]
    run = list(env.ledger.runs.values())[-1]
    assert run["checks"]["markets_capture"]["cart_currency"] == "USD"
    assert run["checks"]["catalog_write"] == ledger_db.CATALOG_WRITE_STARTED
    assert run["readback"]["ok"] and run["readback"]["served_content_keys"] == len(content_keys)

    siblings = catalog.offers("market = 'US'")
    assert len(siblings) == 5
    assert {(o["currency"], o["source_system"], o["source_domain"]) for o in siblings} == {
        ("USD", "shopify_markets_us_localization", HOST)}
    # Sibling, never rewrite: the AUD offers are exactly as the crawl left them.
    assert sorted(catalog.offers("market = 'AU'"), key=lambda o: o["offer_id"]) == sorted(base, key=lambda o: o["offer_id"])
    assert sorted(catalog.republished) == content_keys
    for ck in content_keys:  # serving-eligible for the US, and only through the USD siblings
        assert catalog.verdict(ck)["serving_eligible"] is True
        assert catalog.priced_for(ck, ["US"]) and not catalog.priced_for(ck, ["SG"])


async def test_the_runbook_re_runs_base_capture_base_capture_and_every_run_ends_done(two_jobs):
    """Review of #2358 (D1): the storefront's AU crawl must stay re-runnable after its USD siblings exist on
    the same host. Its readback counts the offers IT wrote there, not the capture's siblings."""
    env, catalog, applied, store = two_jobs

    async def both(make):
        assert (await pipeline.run_stage(make(), db=catalog))["status"] == "apply_due"
        out = await pipeline.run_stage(make("apply_due"), db=catalog)
        assert (out["status"], out["outcome"]) == ("done", "applied"), env.ledger.transitions[-1]

    await both(au_job)
    await both(markets_job)
    await both(au_job)        # the refresh: 5 AUD/AU base offers + 5 USD/US siblings on gotoskincare.com
    await both(markets_job)   # and the capture refreshes its siblings in place
    assert len(catalog.offers("market = 'AU'")) == 5 and len(catalog.offers("market = 'US'")) == 5


async def test_stored_au_rows_stay_unservable_when_a_flag_off_process_recomputes_them(two_jobs, monkeypatch):
    """Review of #2358 (D3): the stored AU rows, recomputed by a process with the agent-decision gate OFF."""
    env, catalog, applied, store = two_jobs
    await pipeline.run_stage(au_job(), db=catalog)
    await pipeline.run_stage(au_job("apply_due"), db=catalog)
    monkeypatch.setattr(gates, "agent_decision_gates_enabled", lambda: False)
    monkeypatch.setattr(ips, "agent_decision_gates_enabled", lambda: False)
    for ck in {p["content_key"] for plan, _market in applied for p in plan["pdps"]}:
        verdict = catalog.verdict(ck)
        assert (verdict["serving_eligible"], verdict["blocker_code"]) == (False, gates.BLOCKER_NO_US_OFFER)


@pytest.mark.parametrize("path", ["/cart.js", "/localization"])
async def test_a_robots_refusal_leaves_the_base_job_done_and_writes_no_sibling(two_jobs, monkeypatch, path):
    env, catalog, applied, store = two_jobs
    await pipeline.run_stage(au_job(), db=catalog)
    assert (await pipeline.run_stage(au_job("apply_due"), db=catalog))["status"] == "done"
    au_done = env.ledger.transitions[-1]
    monkeypatch.setattr(markets, "crawl_politeness", Polite(disallow={path}))
    out = await pipeline.run_stage(markets_job(), db=catalog)
    assert (out["status"], out["outcome"]) == ("nothing", "robots_disallowed")
    run = list(env.ledger.runs.values())[-1]
    assert run["error"].startswith(f"robots_disallowed:{path} ")
    assert run["checks"]["markets_capture"]["robots"]["disallowed"] == path
    assert catalog.offers("market = 'US'") == [] and store.log == []
    assert au_done["status"] == "done" and len(catalog.offers("market = 'AU'")) == 5  # untouched


async def test_the_capture_refusal_writes_nothing_and_records_why(two_jobs, monkeypatch):
    env, catalog, applied, store = two_jobs
    await pipeline.run_stage(au_job(), db=catalog)
    await pipeline.run_stage(au_job("apply_due"), db=catalog)
    store.honors = False  # the store ignores the localization: its cart stays AUD
    out = await pipeline.run_stage(markets_job(), db=catalog)
    assert (out["status"], out["outcome"]) == ("nothing", "us_session_unproven")
    run = list(env.ledger.runs.values())[-1]
    assert run["checks"]["markets_capture"]["cart_currency"] == "AUD" and "reports AUD" in run["error"]
    assert catalog.offers("market = 'US'") == []


async def test_a_throttled_capture_backs_off_like_a_crawl(two_jobs):
    env, catalog, applied, store = two_jobs
    await pipeline.run_stage(au_job(), db=catalog)
    await pipeline.run_stage(au_job("apply_due"), db=catalog)
    store.overrides[("GET", "/cart.js")] = _answer(429)
    out = await pipeline.run_stage(markets_job(), db=catalog)
    assert (out["status"], out["outcome"]) == ("queued", "crawl_throttled")
    assert env.ledger.transitions[-1]["next_run_at"] is not None and env.ledger.transitions[-1]["count_attempt"]


async def test_a_capture_before_its_base_crawl_is_nothing_to_do(two_jobs):
    env, catalog, applied, store = two_jobs
    out = await pipeline.run_stage(markets_job(), db=catalog)
    assert (out["status"], out["outcome"]) == ("nothing", "no_base_rows")
    assert store.log == []


async def test_a_capture_apply_whose_readback_finds_no_us_offer_fails(two_jobs, monkeypatch):
    """If the index still says no_us_offer after a USD sibling 'landed', the write is not what it claims."""
    env, catalog, applied, store = two_jobs
    await pipeline.run_stage(au_job(), db=catalog)
    await pipeline.run_stage(au_job("apply_due"), db=catalog)
    monkeypatch.setattr(catalog, "verdict", lambda ck: _classify(False))
    out = await pipeline.run_stage(markets_job("apply_due"), db=catalog)
    assert (out["status"], out["outcome"]) == ("failed", "readback_failed")
    assert "still blocks it as no_us_offer" in env.ledger.transitions[-1]["reason"]
