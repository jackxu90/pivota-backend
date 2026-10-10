"""Multi-market storefronts ADR Phase 2: the new SQL, executed on real Postgres.

tests/services/test_retailer_ingest_markets_phase2.py fakes the database. This file runs the statements
themselves against the real schema: the capture's candidate read, its sibling INSERT ... SELECT (identity
read from the base offer at write time; money never restamped on conflict), its readback, the apply's
market-declaring offer upsert, and the retailer_ingest readback for an acquisition market (scoped to the
job's storefront, with the served-region leak detector over the content_key).
"""
import json
import os
from urllib.parse import urlsplit

import pytest

URL = os.getenv("DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not URL.startswith("postgres"), reason="requires test Postgres")

PREFIX = "mkt-gate-"
HOST = "mkt-gate-goto.example"
#: A retailer listing key (ingestion: "ext:" + "retailer:" + digest), cleaned up like PREFIX rows.
RETAILER_PREFIX = "ext:retailer:" + PREFIX
#: The dialect gate runs on `pivota_dialect_check` in CI (and fails on ANY skip); local runs use *_test.
_SAFE_DB_MARKERS = ("dialect_check", "_test", "test_")

# One database is shared by every gate file, and earlier files create these tables narrower: add exactly
# the columns this file touches, idempotently (never run migrations here), and clean up rows by PREFIX.
_LIGHTWEIGHT_DDL = """
CREATE TABLE IF NOT EXISTS index_pipeline_state (content_key text PRIMARY KEY);
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS pipeline_stage text;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS blocker_code text;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS blocker_detail text;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS serving_eligible boolean;
CREATE TABLE IF NOT EXISTS product_group_members (product_group_id text, merchant_id text, platform text,
                                                  platform_product_id text);
ALTER TABLE product_group_members ADD COLUMN IF NOT EXISTS product_group_id text;
ALTER TABLE product_group_members ADD COLUMN IF NOT EXISTS merchant_id text;
ALTER TABLE product_group_members ADD COLUMN IF NOT EXISTS platform text;
ALTER TABLE product_group_members ADD COLUMN IF NOT EXISTS platform_product_id text;
ALTER TABLE catalog_products ADD COLUMN IF NOT EXISTS brand varchar(255);
ALTER TABLE catalog_products ADD COLUMN IF NOT EXISTS content_key varchar(40);
ALTER TABLE catalog_products ADD COLUMN IF NOT EXISTS category_path text;
ALTER TABLE catalog_products ADD COLUMN IF NOT EXISTS image_url text;
ALTER TABLE catalog_products ADD COLUMN IF NOT EXISTS pdp_scope varchar(64);
ALTER TABLE catalog_products ADD COLUMN IF NOT EXISTS pdp_lifecycle_stage varchar(32);
ALTER TABLE catalog_products ADD COLUMN IF NOT EXISTS suppressed_at timestamptz;
ALTER TABLE catalog_skus ADD COLUMN IF NOT EXISTS suppressed_at timestamptz;
ALTER TABLE catalog_skus ADD COLUMN IF NOT EXISTS suppression_reason text;
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS offer_type varchar(16);
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS is_first_party boolean NOT NULL DEFAULT false;
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS market varchar(8) NOT NULL DEFAULT 'US';
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS source_domain text;
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS offer_payload jsonb;
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS inventory_quantity integer;
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS estimated_best_price numeric(12, 2);
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS price_confidence numeric(5, 2);
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS suppressed_at timestamptz;
ALTER TABLE catalog_offers ADD COLUMN IF NOT EXISTS suppression_reason text;
"""


@pytest.fixture
async def db():
    import asyncpg
    from databases import Database
    from sqlalchemy import create_engine

    import db.catalog  # noqa: F401  (registers catalog_products / catalog_skus / catalog_offers on the MetaData)
    from db.database import metadata

    name = urlsplit(URL).path.rsplit("/", 1)[-1]
    if not any(marker in name for marker in _SAFE_DB_MARKERS):
        pytest.skip("synthetic catalog rows require a test database")
    engine = create_engine(URL)
    metadata.create_all(engine, checkfirst=True)
    engine.dispose()
    connection = await asyncpg.connect(URL)
    try:
        await connection.execute(_LIGHTWEIGHT_DDL)
    finally:
        await connection.close()
    database = Database(URL)
    await database.connect()
    try:
        yield database
    finally:
        for prefix in (PREFIX, RETAILER_PREFIX):
            await database.execute("DELETE FROM catalog_offers WHERE product_key LIKE :p", {"p": prefix + "%"})
            await database.execute("DELETE FROM catalog_skus WHERE product_key LIKE :p", {"p": prefix + "%"})
            await database.execute("DELETE FROM catalog_products WHERE product_key LIKE :p", {"p": prefix + "%"})
        await database.execute("DELETE FROM index_pipeline_state WHERE content_key LIKE :p", {"p": PREFIX + "%"})
        await database.disconnect()


async def _product(db, key, *, content_key, brand="Go-To"):
    await db.execute(
        "INSERT INTO catalog_products (product_key, merchant_id, platform, source_product_id, title, brand,"
        " category_path, image_url, pdp_scope, content_key, pdp_lifecycle_stage) VALUES (:k, 'agent_seed::go-to',"
        " 'external_seed', :k, 'Face Hero', :b, 'beauty/skincare/face-oil', 'https://cdn.example/i.jpg',"
        " 'multi_merchant_canonical', :ck, 'published')", {"k": key, "b": brand, "ck": content_key})
    await db.execute(
        "INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform, source_product_id, source_variant_id,"
        " title) VALUES (:s, :k, 'agent_seed::go-to', 'external_seed', :k, :k, 'Face Hero')",
        {"s": key + "::canonical", "k": key})


async def _offer(db, oid, key, *, currency="AUD", market="AU", host=HOST,
                 source_system="catalog_enrichment_agent_v1", price=45.0):
    await db.execute(
        "INSERT INTO catalog_offers (offer_id, sku_key, product_key, merchant_id, offer_type, is_first_party, market,"
        " currency, list_price, merchant_effective_price, source_system, source_ref, source_domain, offer_mode)"
        " VALUES (:o, :s, :k, 'agent_seed::go-to', 'brand_direct', TRUE, :m, :c, :p, :p, :sys, :ref, :h,"
        " 'external_referral')",
        {"o": oid, "s": key + "::canonical", "k": key, "m": market, "c": currency, "p": price, "sys": source_system,
         "ref": f"https://{host}/products/face-hero", "h": host})


async def _ips(db, content_key, *, blocker, serving):
    # No ON CONFLICT: another gate file may have created this table without the key this one declares.
    await db.execute("DELETE FROM index_pipeline_state WHERE content_key = :ck", {"ck": content_key})
    await db.execute(
        "INSERT INTO index_pipeline_state (content_key, pipeline_stage, blocker_code, serving_eligible)"
        " VALUES (:ck, 'quality_gated', :b, :s)", {"ck": content_key, "b": blocker, "s": serving})


def _planned(base_offer_id, key, *, cents=3200, market="US"):
    from services.retailer_ingest import shopify_markets as markets
    from services.region_pricing import pricing_currency_for_region
    price = cents / 100.0
    lane = markets.source_system_for(market)
    return {"offer_id": markets.sibling_offer_id(base_offer_id, market), "base_offer_id": base_offer_id,
            "product_key": key, "sku_key": key + "::canonical", "content_key": PREFIX + "ck", "market": market,
            "currency": pricing_currency_for_region(market), "availability": "in_stock", "list_price": price,
            "merchant_effective_price": price, "estimated_best_price": price, "price_confidence": 0.7,
            "source_system": lane, "offer_payload": json.dumps({"capture": lane, "price_cents": cents})}


async def test_the_candidate_read_returns_only_this_hosts_base_currency_crawl_rows(db):
    from services.retailer_ingest import shopify_markets as markets
    key = PREFIX + "p1"
    await _product(db, key, content_key=PREFIX + "ck")
    await _offer(db, PREFIX + "base", key)
    await _offer(db, PREFIX + "usd", key, currency="USD", market="US")                  # already USD
    await _offer(db, PREFIX + "other-host", key, host="mkt-gate-other.example")         # another storefront
    await _offer(db, PREFIX + "other-lane", key, source_system="us_market_capture")     # not the base crawl
    await _offer(db, PREFIX + "gone", key)
    await db.execute("UPDATE catalog_offers SET suppressed_at = NOW() WHERE offer_id = :o", {"o": PREFIX + "gone"})
    brand_store = {"source_role": "brand_official", "source": "shopify_markets"}  # every US capture's role
    rows = await markets.load_candidates({"domain": "www." + HOST, "brand": "Go-To", "options": brand_store}, db)
    assert [(r["base_offer_id"], r["currency"], r["market"], r["source_variant_id"]) for r in rows] == [
        (PREFIX + "base", "AUD", "AU", key)]
    assert await markets.load_candidates({"domain": HOST, "brand": "Sukin", "options": brand_store}, db) == []


async def test_a_sibling_takes_its_identity_from_the_base_offer_and_never_touches_it(db):
    from services.retailer_ingest import shopify_markets as markets
    key = PREFIX + "p2"
    await _product(db, key, content_key=PREFIX + "ck")
    await _offer(db, PREFIX + "base2", key)
    out = await markets.write_siblings([_planned(PREFIX + "base2", key)], db=db)
    assert len(out["written"]) == 1 and out["not_written"] == []
    sib = dict(await db.fetch_one("SELECT * FROM catalog_offers WHERE offer_id = :o",
                                  {"o": markets.sibling_offer_id(PREFIX + "base2")}))
    assert (sib["market"], sib["currency"], float(sib["list_price"])) == ("US", "USD", 32.0)
    assert (sib["merchant_id"], sib["offer_type"], sib["is_first_party"], sib["source_domain"], sib["sku_key"]) == (
        "agent_seed::go-to", "brand_direct", True, HOST, key + "::canonical")
    assert sib["source_ref"] == f"https://{HOST}/products/face-hero"
    assert sib["source_system"] == "shopify_markets_us_localization"
    # A re-run refreshes the price; the base AUD offer is exactly as the crawl wrote it.
    await markets.write_siblings([_planned(PREFIX + "base2", key, cents=3400)], db=db)
    rows = {r["offer_id"]: dict(r) for r in await db.fetch_all(
        "SELECT offer_id, market, currency, list_price FROM catalog_offers WHERE product_key = :k", {"k": key})}
    assert float(rows[markets.sibling_offer_id(PREFIX + "base2")]["list_price"]) == 34.0
    assert (rows[PREFIX + "base2"]["market"], rows[PREFIX + "base2"]["currency"],
            float(rows[PREFIX + "base2"]["list_price"])) == ("AU", "AUD", 45.0)


async def test_no_sibling_on_a_retired_base_and_a_suppressed_sibling_is_never_refreshed(db):
    from services.retailer_ingest import shopify_markets as markets
    key = PREFIX + "p3"
    await _product(db, key, content_key=PREFIX + "ck")
    await _offer(db, PREFIX + "base3", key)
    await _offer(db, PREFIX + "base3b", key, host=HOST)
    await db.execute("UPDATE catalog_offers SET suppressed_at = NOW() WHERE offer_id = :o", {"o": PREFIX + "base3"})
    retired = await markets.write_siblings([_planned(PREFIX + "base3", key)], db=db)
    assert retired["written"] == [] and len(retired["not_written"]) == 1
    await markets.write_siblings([_planned(PREFIX + "base3b", key)], db=db)
    sib = markets.sibling_offer_id(PREFIX + "base3b")
    await db.execute("UPDATE catalog_offers SET suppressed_at = NOW() WHERE offer_id = :o", {"o": sib})
    again = await markets.write_siblings([_planned(PREFIX + "base3b", key, cents=9900)], db=db)
    assert again["not_written"] == [sib]
    assert float(await db.fetch_val("SELECT list_price FROM catalog_offers WHERE offer_id = :o", {"o": sib})) == 32.0


async def test_the_capture_readback_reads_what_landed_and_the_index_verdict(db):
    from services.retailer_ingest import shopify_markets as markets
    key, ck = PREFIX + "p4", PREFIX + "ck4"
    await _product(db, key, content_key=ck)
    await _offer(db, PREFIX + "base4", key)
    wrote = await markets.write_siblings([_planned(PREFIX + "base4", key)], db=db)
    await _ips(db, ck, blocker="none", serving=True)
    out = await markets.readback(wrote["written"], db=db)
    assert out["ok"], out["problems"]
    assert out["served_content_keys"] == 1
    await _ips(db, ck, blocker="no_us_offer", serving=False)
    assert not (await markets.readback(wrote["written"], db=db))["ok"]


async def test_the_market_declaring_offer_upsert_stamps_on_insert_and_never_restamps(db):
    from services.catalog_enrichment_agent import apply as writer
    from services.catalog_enrichment_agent.ingestion import ingest_validated_jsonl
    from services.curated_brand_feed import shopify_product_to_record

    record = shopify_product_to_record(
        {"id": 41, "vendor": "Go-To", "title": "Face Hero 30ml", "handle": "face-hero", "product_type": "Face Oil",
         "body_html": "<p>A face oil for dry skin.</p>", "images": [{"src": "https://cdn.example/i.jpg"}],
         "variants": [{"id": 44101000000001, "price": "45.00", "available": True, "sku": "FH30"}]},
        domain=HOST, category_path="beauty", brand_override="Go-To", currency="AUD", source_role="brand_official",
        emit_native_variants=True)
    plan = ingest_validated_jsonl([record], market="AU")
    offer = writer._with_offer_write_defaults([dict(plan["offers"][0])])[0]
    offer.update(offer_id=PREFIX + "declared", product_key=PREFIX + "p5", sku_key=PREFIX + "p5::canonical")
    # The row the plan builds binds EXACTLY the statement's names (databases refuses an unknown key).
    await db.execute(writer._offer_sql_for(offer), offer)
    await db.execute(writer._offer_sql_for(offer), {**offer, "market": "US"})  # a collision never restamps
    assert await db.fetch_val("SELECT market FROM catalog_offers WHERE offer_id = :o", {"o": PREFIX + "declared"}) == "AU"
    legacy = {k: v for k, v in offer.items() if k != "market"}
    legacy.update(offer_id=PREFIX + "legacy")
    await db.execute(writer._offer_sql_for(legacy), legacy)
    assert await db.fetch_val("SELECT market FROM catalog_offers WHERE offer_id = :o", {"o": PREFIX + "legacy"}) == "US"


async def test_an_acquisition_readback_on_postgres(db):
    """Stored not served; scoped to the storefront; and served only on a served-region price."""
    from services.retailer_ingest.pipeline import _readback
    key, ck = PREFIX + "acq", PREFIX + "ck-acq"
    await _product(db, key, content_key=ck)
    await _offer(db, PREFIX + "acq-aud", key)
    await _offer(db, PREFIX + "acq-us-store", key, currency="USD", market="US", host="mkt-gate-us.example")
    # A shopify_markets USD sibling on THIS host (review of #2358, D1): the capture's, not the crawl's.
    await _offer(db, PREFIX + "acq-sibling", key, currency="USD", market="US",
                 source_system="shopify_markets_us_localization")
    await db.execute("UPDATE catalog_offers SET suppressed_at = NOW() WHERE offer_id = :o",
                     {"o": PREFIX + "acq-sibling"})  # suppressed here, so the leak case below stays a leak
    await _offer(db, PREFIX + "acq-sibling-live", key, currency="USD", market="US",
                 source_system="shopify_markets_us_localization", host="www." + HOST)
    await db.execute("UPDATE catalog_offers SET merchant_effective_price = 0, list_price = 0 WHERE offer_id = :o",
                     {"o": PREFIX + "acq-sibling-live"})  # live but unpriced: counted by the scope, never serving
    await _ips(db, ck, blocker="no_us_offer", serving=False)
    stored = await _readback([key], "AUD", db, market="AU", domain=HOST, planned_images={key: True})
    assert stored["ok"], stored["problems"]  # neither the other storefront's offer nor the sibling is this job's
    assert stored["rows"][0]["offers"] == 1
    assert [n["kind"] for n in stored["notes"]] == ["acquisition_not_served"]
    # Served: legitimate here, because the content_key carries a USD offer (the US store's).
    await _ips(db, ck, blocker="none", serving=True)
    assert (await _readback([key], "AUD", db, market="AU", domain=HOST, planned_images={key: True}))["ok"]
    # ...and the leak once no served-region price exists anywhere on the content_key.
    await db.execute("UPDATE catalog_offers SET suppressed_at = NOW() WHERE offer_id = :o",
                     {"o": PREFIX + "acq-us-store"})
    leak = await _readback([key], "AUD", db, market="AU", domain=HOST, planned_images={key: True})
    assert not leak["ok"] and "must never serve" in leak["problems"][0]["problem"]


# ------------------------------------------------------------------ SG (Peng 2026-10-10)

async def test_an_sg_capture_reads_a_usd_retailers_listing_rows_and_never_an_sgd_row(db):
    """The same CANDIDATES_SQL with capture_currency SGD: the USD/US base rows of a retailer's listing are
    candidates for SG (not for a US capture), an SGD row is not, and a retailer job keeps only its
    ext:retailer: listings of its vendors (Python, over what Postgres returned)."""
    from services.retailer_ingest import shopify_markets as markets
    listing, canonical = RETAILER_PREFIX + "l1", PREFIX + "c1"
    await _product(db, listing, content_key=PREFIX + "ck-l1", brand="Purito SEOUL")
    await _product(db, canonical, content_key=PREFIX + "ck-c1", brand="Purito SEOUL")
    await _offer(db, PREFIX + "l1-usd", listing, currency="USD", market="US")
    await _offer(db, PREFIX + "l1-sgd", listing, currency="SGD", market="SG")
    await _offer(db, PREFIX + "c1-usd", canonical, currency="USD", market="US")
    job = {"domain": HOST, "brand": "Purito", "options": {"vendors": ["Purito"], "market": "SG",
                                                          "source": "shopify_markets"}}
    rows = await markets.load_candidates(job, db)
    assert [(r["base_offer_id"], r["currency"], r["market"]) for r in rows] == [(PREFIX + "l1-usd", "USD", "US")]
    us_brand_job = {"domain": HOST, "brand": "Purito SEOUL",
                    "options": {"vendors": ["Purito"], "source_role": "brand_official", "source": "shopify_markets"}}
    # A US capture reads the SAME statement with USD excluded: the SGD row (any key) is its candidate instead.
    assert [r["base_offer_id"] for r in await markets.load_candidates(us_brand_job, db)] == [PREFIX + "l1-sgd"]


async def test_a_us_and_an_sg_sibling_of_one_base_offer_coexist_and_read_back(db):
    from services.retailer_ingest import shopify_markets as markets
    key, ck = PREFIX + "p6", PREFIX + "ck"
    await _product(db, key, content_key=ck)
    await _offer(db, PREFIX + "base6", key, currency="JPY", market="JP", price=980.0)
    us = await markets.write_siblings([_planned(PREFIX + "base6", key, cents=700)], db=db)
    sg = await markets.write_siblings([_planned(PREFIX + "base6", key, cents=1290, market="SG")], db=db)
    assert len(us["written"]) == len(sg["written"]) == 1
    rows = {r["offer_id"]: dict(r) for r in await db.fetch_all(
        "SELECT offer_id, market, currency, source_system, list_price, source_domain, merchant_id"
        " FROM catalog_offers WHERE product_key = :k", {"k": key})}
    sg_id, us_id = markets.sibling_offer_id(PREFIX + "base6", "SG"), markets.sibling_offer_id(PREFIX + "base6")
    assert sg_id != us_id and len(rows) == 3
    assert (rows[sg_id]["market"], rows[sg_id]["currency"], rows[sg_id]["source_system"],
            float(rows[sg_id]["list_price"])) == ("SG", "SGD", "shopify_markets_sg_localization", 12.9)
    assert (rows[us_id]["market"], rows[us_id]["currency"], float(rows[us_id]["list_price"])) == ("US", "USD", 7.0)
    assert rows[sg_id]["source_domain"] == rows[PREFIX + "base6"]["source_domain"] == HOST
    assert rows[sg_id]["merchant_id"] == rows[PREFIX + "base6"]["merchant_id"]
    # A re-capture refreshes the SG price in place; the JPY base is untouched.
    await markets.write_siblings([_planned(PREFIX + "base6", key, cents=1350, market="SG")], db=db)
    assert float(await db.fetch_val("SELECT list_price FROM catalog_offers WHERE offer_id = :o", {"o": sg_id})) == 13.5
    assert float(await db.fetch_val("SELECT list_price FROM catalog_offers WHERE offer_id = :o",
                                    {"o": PREFIX + "base6"})) == 980.0
    await _ips(db, ck, blocker="none", serving=True)
    assert (await markets.readback(sg["written"], db=db, market="SG"))["ok"]
    assert not (await markets.readback(sg["written"], db=db))["ok"]  # an SGD/SG row is not a US sibling
