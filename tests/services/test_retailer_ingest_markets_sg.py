"""source = shopify_markets for SG (Peng 2026-10-10: "Build the SG currency support").

The coverage wave of 2026-10-10 found many (brand, store) pairs for SG buyers at Shopify stores whose base
currency is not SGD but whose cart quotes SGD to SG (mostly USD- and JPY-base, nearly all of them
retailers). The capture now targets options.market
(US as before, or SG) and, for SG only, retailer stores. Every rule below has a test that fails without it:
  1. Options: SG is a capture market (brand or retailer); a US capture stays brand-only; AU/JP never; a
     multi_brand retailer cohort may be captured; require_ships_to_market is a storefront-crawl option.
  2. Ids and lanes: US ids / source_system byte-identical; SG ids in their own namespace, never a US id.
  3. The SG session proof: ships to SG -> PUT country_code=SG -> /cart.js SGD before any price -> in-session
     reads, re-checked; a cart that stays USD, a decayed session, an SGD-base store: clean refusals.
  4. Retailer identity: candidates are the cohort's ext:retailer: listing rows on this host (vendor and
     family spellings, multi_brand vendors), never another brand's or a canonical row; a retailer whose base
     rows claim a served market the store does not ship to is refused.
  5. Pipeline: SG capture refuses where this process does not serve SG; the base crawl's ships-to gate.
  6. End to end with the real producers: a USD-base retailer (US crawl -> SG capture) and a JPY-base retailer
     (JP acquisition crawl -> SG capture); readback checks SGD/SG; the US path is untouched.
Producer shapes: shopify_product_to_record(source_role="retailer") -> ingest_validated_jsonl(market=...), the
same calls records_for_brand and the lane's plan make.
"""
from __future__ import annotations

import hashlib
import json
import re
from urllib.parse import urlsplit

import httpx
import pytest

import services.agent_decision_gates as gates
from db import retailer_ingest as ledger_db
from services import curated_brand_feed as feed, storefront_currency
from services.catalog_enrichment_agent.ingestion import ingest_validated_jsonl
from services.retailer_ingest import pipeline, shopify_markets as markets
from tests.services.test_retailer_ingest_markets_phase2 import (  # noqa: F401 -- fixtures
    Polite, ReadbackDB, SqliteCatalog, base_rows, gates_on, markets_job)
from tests.services.test_retailer_ingest_pipeline import env  # noqa: F401 -- the state-machine fixture

HOST, RETAILER = "kbeauty-retailer.example.com", "K-Beauty Retailer"
USD_META = {"name": RETAILER, "myshopify_domain": "example-retailer.myshopify.com", "currency": "USD",
            "ships_to_countries": ["CA", "SG", "US"]}
JP_HOST = "jbeauty-retailer.example.com"
JPY_META = {"name": "J-Beauty Retailer", "myshopify_domain": "example-jp.myshopify.com", "currency": "JPY",
            "ships_to_countries": ["JP", "SG", "US"]}

#: handle -> (vendor, product_type, [(variant_id, {currency: minor units})]). Merchant-set SGD prices, not FX.
USD_CATALOG = {
    "purito-centella-serum": ("Purito", "Serum", [(44510000000001, {"USD": 1900, "SGD": 2690}),
                                                   (44510000000002, {"USD": 3400, "SGD": 4790})]),
    "cosrx-snail-essence": ("COSRX", "Essence", [(44520000000001, {"USD": 2500, "SGD": 3490})]),
    "laneige-lip-mask": ("Laneige", "Lip Mask", [(44530000000001, {"USD": 2400, "SGD": 3300})]),
}
#: Under ¥1,000 on purpose: detectors.detect flags ANY price >= 1000 as placeholder_product whatever the
#: currency (a BLOCK on nearly every real JPY row -- an open problem for JPY base crawls, not this test's).
JPY_CATALOG = {
    "kose-sekkisei-toner": ("KOSE", "Toner", [(44610000000001, {"JPY": 98000, "SGD": 1290})]),
}


def retailer_records(catalog, *, host=HOST, currency="USD", retailer_name=RETAILER, brand_by_vendor=None,
                     brand=None, vendors=None):
    """What records_for_brand builds for a retailer cohort: the vendor filter, then one record per product
    with the operator's brand as override (or each vendor's own spelling, multi_brand)."""
    out = []
    for i, (handle, (vendor, ptype, variants)) in enumerate(sorted(catalog.items())):
        if vendors is not None and vendor not in vendors:
            continue
        product = {"id": 8800000000 + i, "vendor": vendor, "title": f"{vendor} {handle.replace('-', ' ').title()}",
                   "handle": handle, "product_type": ptype,
                   "body_html": f"<p>A {ptype.lower()} for daily use, gentle on sensitive skin.</p>",
                   "images": [{"src": f"https://cdn.shopify.com/s/{handle}.jpg"}],
                   "variants": [{"id": vid, "price": f"{prices[currency] / 100:.2f}", "available": True,
                                 "sku": f"S{vid}"} for vid, prices in variants]}
        override = (brand_by_vendor or {}).get(vendor.casefold(), brand)
        out.append(feed.shopify_product_to_record(
            product, domain=host, category_path="beauty", brand_override=override, currency=currency,
            source_role="retailer", retailer_name=retailer_name, emit_native_variants=True))
    return out


def sg_job(status="queued", *, domain=HOST, brand="Purito", vendors=("Purito",), **options):
    return {"id": "rij_sg", "domain": domain, "brand": brand, "status": status, "attempts": 0, "max_attempts": 6,
            "options": {"vendors": list(vendors), "source": "shopify_markets", "market": "SG", **options}}


def base_job(status="queued", *, domain=HOST, brand="Purito", vendors=("Purito",), market="US", **options):
    return {"id": "rij_base", "domain": domain, "brand": brand, "status": status, "attempts": 0, "max_attempts": 6,
            "options": {"vendors": list(vendors), "market": market, "retailer_name": RETAILER, **options}}


class MarketStore:
    """A Shopify-Markets storefront in any base currency. A MULTIPART PUT to /localization naming a
    country it has a market for sets the cookie; /cart.js and /products/<h>.js then answer in the
    currency `quotes` gives that country (absent: the base currency)."""

    def __init__(self, *, meta, catalog, quotes=None, honors=True, loses_session_after=None, overrides=None):
        self.meta, self.catalog = dict(meta), catalog
        self.quotes = {"SG": "SGD", "US": "USD"} if quotes is None else quotes
        self.honors, self.loses_session_after = honors, loses_session_after
        self.overrides, self.log, self.carts, self.countries = dict(overrides or {}), [], 0, []

    def _session_currency(self, request):
        m = re.search(r"localization=([A-Z]{2})", request.headers.get("cookie", ""))
        return self.quotes.get(m.group(1), self.meta["currency"]) if m else self.meta["currency"]

    def handler(self, request):
        path = request.url.path
        self.log.append((request.method, path, request.url.query.decode()))
        if (request.method, path) in self.overrides:
            return self.overrides[(request.method, path)](request)
        currency = self._session_currency(request)
        if path == "/meta.json":
            return httpx.Response(200, json=self.meta)
        if path == "/":
            return httpx.Response(200, text="<html>store</html>", headers={"content-type": "text/html"})
        if path == "/localization" and request.method == "POST":
            body = request.content
            multipart = "multipart/form-data" in request.headers.get("content-type", "")
            m = re.search(rb'name="country_code"\r\n\r\n([A-Z]{2})\r\n', body)
            headers = {"location": "/"}
            if m:
                self.countries.append(m.group(1).decode())
            if self.honors and multipart and b'name="_method"' in body and m:
                headers["set-cookie"] = f"localization={m.group(1).decode()}; Path=/"
            return httpx.Response(302, headers=headers)
        if path == "/cart.js":
            self.carts += 1
            if self.loses_session_after is not None and self.carts > self.loses_session_after:
                currency = self.meta["currency"]
            return httpx.Response(200, json={"currency": currency, "item_count": 0})
        if path.startswith("/products/") and path.endswith(".js"):
            handle = path[len("/products/"):-3]
            if handle not in self.catalog:
                return httpx.Response(404, text="not found", headers={"content-type": "text/html"})
            _vendor, _ptype, variants = self.catalog[handle]
            return httpx.Response(200, json={"id": 1, "handle": handle, "variants": [
                {"id": vid, "price": prices[currency], "available": True} for vid, prices in variants]})
        return httpx.Response(404)

    def paths(self):
        return [(m, p) for m, p, _q in self.log]


class CandidatesDB:
    """CANDIDATES_SQL over a plan, as Postgres answers it: host, lane, live, and currency <> the capture's."""

    def __init__(self, rows):
        self.rows, self.reads = rows, []

    async def fetch_all(self, sql, values):
        assert sql == markets.CANDIDATES_SQL
        self.reads.append(values)
        return [dict(r) for r in self.rows if r["currency"] != values["capture_currency"]]


def usd_plan(**kw):
    return ingest_validated_jsonl(retailer_records(USD_CATALOG, **kw), market="US")


async def _capture(store, job=None, rows=None, *, polite=None):
    rows = base_rows(usd_plan()) if rows is None else rows
    return await markets.capture(job or sg_job(), db=CandidatesDB(rows), max_products=200,
                                 polite=polite or Polite(), transport=httpx.MockTransport(store.handler))


def usd_store(**kw):
    return MarketStore(meta=kw.pop("meta", USD_META), catalog=USD_CATALOG, **kw)


@pytest.fixture(autouse=True)
def _clean_meta_cache():
    storefront_currency.clear_cache()
    yield
    storefront_currency.clear_cache()


# ================================================================== 1. options

@pytest.mark.parametrize("role", [None, "retailer", "brand_official"])
def test_sg_is_a_capture_market_for_retailers_and_brand_stores(role):
    o = {"vendors": ["Purito"], "source": "shopify_markets", "market": "sg"}
    if role:
        o["source_role"] = role
    out = pipeline.validate_options(o)
    assert (pipeline.job_market(out), pipeline.job_currency(out)) == ("SG", "SGD")


@pytest.mark.parametrize("role", [None, "retailer"])
def test_a_us_capture_stays_brand_stores_only(role):
    """The gate is the market: the ADR's "brand stores first" phase still holds for the US."""
    o = {"vendors": ["DHC"], "source": "shopify_markets"}
    if role:
        o["source_role"] = role
    with pytest.raises(ValueError, match="retailer for market SG only"):
        pipeline.validate_options(o)
    with pytest.raises(ValueError, match="retailer for market SG only"):
        pipeline.validate_options({**o, "market": "US"})


def test_a_multi_brand_retailer_cohort_may_be_captured_for_sg_with_its_brands_map():
    o = {"vendors": ["Purito", "COSRX"], "brands": {"Purito": "Purito", "COSRX": "COSRX"}, "multi_brand": True,
         "source": "shopify_markets", "market": "SG"}
    assert pipeline.validate_options(dict(o))
    with pytest.raises(ValueError, match="multi_brand supports only a retailer"):
        pipeline.validate_options({**o, "source_role": "brand_official"})
    with pytest.raises(ValueError, match="retailer for market SG only"):
        pipeline.validate_options({**o, "market": "US"})
    with pytest.raises(ValueError, match="options.brands"):
        pipeline.validate_options({k: v for k, v in o.items() if k != "brands"})


def test_require_ships_to_market_is_a_storefront_crawl_option():
    assert pipeline.validate_options({"vendors": ["Purito"], "require_ships_to_market": True})
    with pytest.raises(ValueError, match="require_ships_to_market is only meaningful on a storefront crawl"):
        pipeline.validate_options({"vendors": ["Purito"], "source": "shopify_markets", "market": "SG",
                                   "require_ships_to_market": True})
    with pytest.raises(ValueError, match="must be bool"):
        pipeline.validate_options({"vendors": ["Purito"], "require_ships_to_market": "yes"})


def test_the_sg_capture_is_its_own_cohort_and_enqueue_accepts_it():
    from scripts.enqueue_retailer_ingest import _row_to_job
    o = {"vendors": ["Purito"], "source": "shopify_markets"}
    assert ledger_db.scope_key(HOST, "Purito", {**o, "market": "SG"}) != ledger_db.scope_key(HOST, "Purito", o)
    row = _row_to_job({"domain": HOST, "brand": "Purito", "vendors": ["Purito"],
                       "options": {"source": "shopify_markets", "market": "SG"}})
    assert row["options"]["market"] == "SG"
    base = _row_to_job({"domain": HOST, "brand": "Purito", "vendors": ["Purito"],
                        "options": {"retailer_name": RETAILER, "require_ships_to_market": True}})
    assert base["options"]["require_ships_to_market"] is True
    with pytest.raises(ValueError, match="retailer for market SG only"):
        _row_to_job({"domain": HOST, "brand": "Purito", "vendors": ["Purito"],
                     "options": {"source": "shopify_markets"}})


# ================================================================== 2. ids and lanes

def test_the_us_sibling_id_and_lane_are_byte_identical_to_before_sg():
    expected = "offer:shopify_markets_us:" + hashlib.sha256(b"offer:base:1|US").hexdigest()[:32]
    assert markets.sibling_offer_id("offer:base:1") == markets.sibling_offer_id("offer:base:1", "US") == expected
    assert markets.source_system_for("US") == markets.SOURCE_SYSTEM == "shopify_markets_us_localization"
    assert markets.offer_id_prefix_for("US") == markets.OFFER_ID_PREFIX
    assert markets.capture_market(markets_job()) == "US"


def test_an_sg_sibling_lives_in_its_own_namespace_and_never_takes_a_us_id():
    sg, us = markets.sibling_offer_id("offer:base:1", "SG"), markets.sibling_offer_id("offer:base:1", "US")
    assert sg.startswith("offer:shopify_markets_sg:") and sg != us
    assert not sg.startswith(markets.OFFER_ID_PREFIX) and not us.startswith(markets.offer_id_prefix_for("SG"))
    assert markets.source_system_for("SG") == "shopify_markets_sg_localization"
    with pytest.raises(ValueError):
        markets.capture_market(sg_job(market="AU"))


# ================================================================== 3. the SG session proof

async def test_a_proven_sg_session_prices_the_retailers_base_offers_in_sgd():
    store = usd_store()
    out = await _capture(store)
    rows = base_rows(usd_plan())
    purito = {r["base_offer_id"] for r in rows if r["brand"] == "Purito SEOUL"}
    assert {r["base_offer_id"] for r in out["planned"]} == purito and purito
    assert {(r["market"], r["currency"], r["source_system"]) for r in out["planned"]} == {
        ("SG", "SGD", "shopify_markets_sg_localization")}
    assert sorted({r["list_price"] for r in out["planned"]}) == [26.9, 47.9]  # the store's SGD, not FX
    assert all(r["offer_id"] == markets.sibling_offer_id(r["base_offer_id"], "SG") for r in out["planned"])
    ev = out["checks"]["markets_capture"]
    assert (ev["market"], ev["cart_currency"], ev["storefront"]["currency"]) == ("SG", "SGD", "USD")
    assert ev["ships_to_base_markets"] == {"US": True}
    payload = json.loads(out["planned"][0]["offer_payload"])
    assert (payload["capture"], payload["cart_currency"], payload["base_currency"]) == (
        "shopify_markets_sg_localization", "SGD", "USD")
    # The proof: PUT country_code=SG, then the cart, then the first price; never a ?country= query.
    assert store.countries == ["SG"]
    paths = store.paths()
    first_price = next(i for i, (_m, p) in enumerate(paths) if p.startswith("/products/"))
    assert paths.index(("POST", "/localization")) < paths.index(("GET", "/cart.js")) < first_price
    assert paths[-1] == ("GET", "/cart.js") and all("country" not in q for _m, _p, q in store.log)


async def test_a_store_whose_sg_market_prices_in_usd_is_a_clean_refusal_with_no_price_read():
    """Localized to SG, the cart still says USD: an SG market without local currency. No SGD offer exists."""
    store = usd_store(quotes={"SG": "USD"})
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    r = refused.value
    assert (r.outcome, r.status, r.transient) == ("sg_session_unproven", "nothing", False)
    assert "reports USD after localizing to SG" in r.reason and "confirm SGD" in r.reason
    assert not any(p.startswith("/products/") for _m, p in store.paths())


@pytest.mark.parametrize("quote", ["JPY", None])
async def test_a_cart_in_any_other_currency_is_refused(quote):
    store = usd_store(quotes={} if quote is None else {"SG": quote})
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    assert refused.value.outcome == "sg_session_unproven"


async def test_a_session_that_stops_reporting_sgd_voids_the_whole_capture(monkeypatch):
    monkeypatch.setattr(markets, "SESSION_RECHECK_EVERY", 1)
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(usd_store(loses_session_after=1))
    r = refused.value
    assert (r.outcome, r.status) == ("sg_session_lost", "failed") and "stopped reporting SGD" in r.reason


async def test_the_final_recheck_alone_catches_a_session_that_stops_reporting_sgd():
    # The default interval: a cohort under SESSION_RECHECK_EVERY products is re-checked ONLY at the end, so the
    # final re-check is the one guard against writing base-currency cents labelled SGD (review of #2553).
    assert len(USD_CATALOG) < markets.SESSION_RECHECK_EVERY
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(usd_store(loses_session_after=1))
    r = refused.value
    assert (r.outcome, r.status) == ("sg_session_lost", "failed") and "at the final re-check" in r.reason


async def test_a_store_that_does_not_ship_to_sg_is_refused_before_any_session():
    store = usd_store(meta={**USD_META, "ships_to_countries": ["US", "CA"]})
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    assert (refused.value.outcome, refused.value.status) == ("store_does_not_ship_to_sg", "nothing")
    assert store.paths() == [("GET", "/meta.json")]


async def test_an_sgd_base_store_is_not_captured():
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(usd_store(meta={**USD_META, "currency": "SGD"}))
    assert refused.value.outcome == "base_currency_is_sgd" and "market = SG" in refused.value.reason


async def test_a_robots_refusal_names_the_sg_market():
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(usd_store(), polite=Polite(disallow={"/cart.js"}))
    assert refused.value.reason.startswith("robots_disallowed:/cart.js ") and "no SG offer" in refused.value.reason


# ================================================================== 4. retailer identity

async def test_the_retailer_cohort_finds_rows_written_under_the_vendors_family_spelling():
    """Job brand "Purito"; the crawl wrote "Purito SEOUL" (RETAILER_BRAND_CANONICAL). The job's brand alone
    would miss every row; the cohort's vendor family finds them -- and no other vendor's."""
    rows = base_rows(usd_plan())
    assert {r["brand"] for r in rows} == {"Purito SEOUL", "COSRX", "Laneige"}
    found = await markets.load_candidates(sg_job(), CandidatesDB(rows))
    assert {r["brand"] for r in found} == {"Purito SEOUL"}
    assert await markets.load_candidates(sg_job(brand="Laneige", vendors=("Laneige",)), CandidatesDB(rows))


async def test_a_vendor_spelt_unlike_the_jobs_brand_is_still_the_cohorts():
    rows = base_rows(usd_plan())
    # The operator named the cohort "Cosrx Korea"; the crawl wrote the vendor's own "COSRX" (not foldable to it).
    found = await markets.load_candidates(sg_job(brand="Cosrx Korea", vendors=("COSRX",)), CandidatesDB(rows))
    assert {r["brand"] for r in found} == {"COSRX"}


async def test_a_multi_brand_cohort_prices_every_vendor_it_selects_and_not_its_label():
    o = {"multi_brand": True, "brands": {"Purito": "Purito", "COSRX": "COSRX"}}
    plan = ingest_validated_jsonl(retailer_records(
        USD_CATALOG, vendors={"Purito", "COSRX"}, brand_by_vendor={"purito": "Purito", "cosrx": "COSRX"}),
        market="US")
    rows = base_rows(plan) + [dict(r, brand="Laneige") for r in base_rows(usd_plan())[-1:]]
    job = sg_job(brand="Laneige", vendors=("Purito", "COSRX"), **o)  # a label that happens to be a brand
    found = await markets.load_candidates(job, CandidatesDB(rows))
    assert {r["brand"] for r in found} == {"Purito SEOUL", "COSRX"}
    out = await _capture(usd_store(), job, rows)
    assert {r["product_key"] for r in out["planned"]} == {r["product_key"] for r in found}


async def test_a_retailer_capture_never_prices_a_canonical_row_on_the_same_host():
    rows = base_rows(usd_plan())
    canonical = [dict(r, product_key="ext:purito-seoul:centella", base_offer_id="o-canonical")
                 for r in rows if r["brand"] == "Purito SEOUL"]
    found = await markets.load_candidates(sg_job(), CandidatesDB(rows + canonical))
    assert found and all(r["product_key"].startswith(markets.RETAILER_LISTING_KEY_PREFIX) for r in found)
    assert "o-canonical" not in {r["base_offer_id"] for r in found}


async def test_a_brand_store_capture_still_selects_by_the_jobs_brand_alone(monkeypatch):
    """brand_official selection is unchanged: rows of the job's brand, whatever their key."""
    rows = [dict(r, product_key="ext:purito:x") for r in base_rows(usd_plan()) if r["brand"] == "Purito SEOUL"]
    job = sg_job(brand="Purito SEOUL", source_role="brand_official")
    assert len(await markets.load_candidates(job, CandidatesDB(rows))) == len(rows)
    assert await markets.load_candidates(sg_job(brand="Purito", source_role="brand_official"), CandidatesDB(rows)) == []


async def test_a_retailer_whose_us_base_rows_the_store_does_not_back_is_refused():
    """The store ships to SG but not the US, yet its USD crawl (a US job) wrote US offers. Never stack SG
    siblings on that seller claim: refuse loudly, before any session."""
    store = usd_store(meta={**USD_META, "ships_to_countries": ["SG", "MY"]})
    with pytest.raises(markets.MarketsRefused) as refused:
        await _capture(store)
    r = refused.value
    assert (r.outcome, r.status) == ("base_market_not_shipped", "failed") and "['US']" in r.reason
    assert r.checks["markets_capture"]["ships_to_base_markets"] == {"US": False}
    assert ("POST", "/localization") not in store.paths()


async def test_an_acquisition_base_market_needs_no_shipping_proof():
    """A JPY store's base rows are JP (stored, never served): no seller claim to back."""
    plan = ingest_validated_jsonl(retailer_records(JPY_CATALOG, host=JP_HOST, currency="JPY",
                                                   retailer_name="J-Beauty Retailer", brand="Kosé"), market="JP")
    rows = base_rows(plan)
    assert {(r["market"], r["currency"], r["brand"]) for r in rows} == {("JP", "JPY", "Kosé")}
    store = MarketStore(meta={**JPY_META, "ships_to_countries": ["SG"]}, catalog=JPY_CATALOG)
    out = await _capture(store, sg_job(domain=JP_HOST, brand="Kosé", vendors=("KOSE",)), rows)
    assert {(r["currency"], r["list_price"]) for r in out["planned"]} == {("SGD", 12.9)}
    assert out["checks"]["markets_capture"]["ships_to_base_markets"] == {}


# ================================================================== 5. writer and readback

class WriteDB:
    def __init__(self):
        self.upserts = []

    async def fetch_all(self, sql, values):
        return [{"sku_key": k} for k in values["sku_keys"]]

    async def fetch_val(self, sql, values):
        assert sql == markets.SIBLING_UPSERT_SQL
        self.upserts.append(values)
        return values["offer_id"]


async def test_the_writer_lands_sg_siblings_and_refuses_one_mispriced():
    planned = (await _capture(usd_store()))["planned"]
    db = WriteDB()
    out = await markets.write_siblings(planned, db=db)
    assert len(out["written"]) == len(planned)
    assert {(u["market"], u["currency"], u["source_system"]) for u in db.upserts} == {
        ("SG", "SGD", "shopify_markets_sg_localization")}
    planned[0]["currency"] = "USD"
    with pytest.raises(ValueError, match="currency_market_mismatch"):
        await markets.write_siblings(planned, db=WriteDB())


async def test_an_sg_readback_wants_sgd_stamped_sg():
    written = [{"offer_id": "offer:shopify_markets_sg:1"}]
    sg = {"currency": "SGD", "market": "SG", "source_system": "shopify_markets_sg_localization"}
    assert (await markets.readback(written, db=ReadbackDB(**sg), market="SG"))["ok"]
    for landed in ({**sg, "currency": "USD"}, {**sg, "market": "US"}, {**sg, "live": False}):
        out = await markets.readback(written, db=ReadbackDB(**landed), market="SG")
        assert not out["ok"] and out["problems"][0]["offer_id"] == "offer:shopify_markets_sg:1"
    # A US readback is unchanged: an SGD/SG row is not a US sibling.
    assert not (await markets.readback(written, db=ReadbackDB(**sg)))["ok"]
    blocked = await markets.readback(written, db=ReadbackDB(**sg, serving=False, blocker_code="no_us_offer"),
                                     market="SG")
    assert not blocked["ok"] and "a SGD sibling landed" in blocked["problems"][0]["problem"]


# ================================================================== 6. the pipeline, end to end

@pytest.fixture
def sg_lane(env, monkeypatch, gates_on):  # noqa: F811
    """A base crawl then an SG capture against one SqliteCatalog, both through pipeline.run_stage, the
    base crawl's records built by the real producer from whichever store `state.store` is."""
    from services.catalog_enrichment_agent import apply as apply_mod, primary_ingestion as pi

    monkeypatch.setenv("PIVOTA_SERVING_PRICING_REGIONS", "US,SG")  # the drain's (infra/gcp/setup_scheduler.sh)
    catalog = SqliteCatalog()
    applied = []
    state = type("State", (), {})()
    state.store = usd_store()
    state.records = lambda: retailer_records(USD_CATALOG, vendors={"Purito"}, brand="Purito")

    async def crawl(**kw):
        batch = feed.ShopifyProductBatch(state.records(), scanned_products=3, pages=1)
        batch.crawl_report["storefront"] = {k: state.store.meta.get(k) for k in
                                            ("name", "myshopify_domain", "currency", "ships_to_countries")}
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
    monkeypatch.setattr(markets, "HTTP_TRANSPORT", httpx.MockTransport(lambda r: state.store.handler(r)))
    monkeypatch.setattr(markets, "crawl_politeness", Polite())
    return env, catalog, applied, state


async def _both(env, catalog, make):
    dry = await pipeline.run_stage(make(), db=catalog)
    assert dry["status"] == "apply_due", (dry, list(env.ledger.runs.values())[-1].get("flags"),
                                          env.ledger.transitions[-1])
    out = await pipeline.run_stage(make("apply_due"), db=catalog)
    assert (out["status"], out["outcome"]) == ("done", "applied"), env.ledger.transitions[-1]
    return out


async def test_a_usd_retailer_becomes_sg_servable_through_sgd_siblings(sg_lane):
    env, catalog, applied, state = sg_lane
    # --- 1. The base crawl: a US storefront job, with the ships-to gate on.
    await _both(env, catalog, lambda s="queued": base_job(s, require_ships_to_market=True))
    base = catalog.offers()
    assert base and {(o["market"], o["currency"], o["offer_type"]) for o in base} == {("US", "USD", "retailer")}
    assert state.store.log == []  # the crawl job never touched a session
    # --- 2. The SG capture.
    await _both(env, catalog, sg_job)
    run = list(env.ledger.runs.values())[-1]
    assert run["checks"]["markets_capture"]["cart_currency"] == "SGD"
    assert run["readback"]["ok"] and run["readback"]["served_content_keys"] >= 1
    assert "SGD sibling offer(s)" in env.ledger.transitions[-1]["reason"]
    # A USD retailer's key already serves through its US base offers, so the SG reason never claims "serving".
    assert "content_key(s) re-published" in env.ledger.transitions[-1]["reason"]
    assert "serving" not in env.ledger.transitions[-1]["reason"]
    siblings = catalog.offers("market = 'SG'")
    assert len(siblings) == len(base)
    assert {(o["currency"], o["source_system"], o["source_domain"]) for o in siblings} == {
        ("SGD", "shopify_markets_sg_localization", HOST)}
    # The retailer's own seller identity, read from the base offer: same merchant, same redirect offer.
    by_base = {o["offer_id"]: o for o in base}
    for sib in siblings:
        src = by_base[json.loads(sib["offer_payload"])["base_offer_id"]]
        assert (sib["merchant_id"], sib["offer_type"], sib["is_first_party"], sib["source_ref"]) == (
            src["merchant_id"], src["offer_type"], src["is_first_party"], src["source_ref"])
    assert sorted(catalog.offers("market = 'US'"), key=lambda o: o["offer_id"]) == sorted(
        base, key=lambda o: o["offer_id"])  # sibling, never rewrite
    assert set(catalog.republish_sources) == {"shopify_markets_sg_localization"}
    for ck in {p["content_key"] for plan, _m in applied for p in plan["pdps"]}:
        assert catalog.priced_for(ck, ["SG"]) and catalog.priced_for(ck, ["US"])
    # --- 3. Re-runs stay re-runnable: the base crawl does not count the SG siblings as its own.
    await _both(env, catalog, lambda s="queued": base_job(s, require_ships_to_market=True))
    await _both(env, catalog, sg_job)
    assert len(catalog.offers("market = 'SG'")) == len(base)


async def test_a_jpy_retailer_is_stored_for_jp_and_served_to_sg_only_through_its_sgd_siblings(sg_lane):
    env, catalog, applied, state = sg_lane
    state.store = MarketStore(meta=JPY_META, catalog=JPY_CATALOG)
    state.records = lambda: retailer_records(JPY_CATALOG, host=JP_HOST, currency="JPY",
                                             retailer_name="J-Beauty Retailer", brand="Kosé")
    await _both(env, catalog, lambda s="queued": base_job(s, domain=JP_HOST, brand="Kosé", vendors=("KOSE",),
                                                          market="JP", retailer_name="J-Beauty Retailer"))
    cks = {p["content_key"] for plan, _m in applied for p in plan["pdps"]}
    for ck in cks:  # stored, not served
        assert catalog.verdict(ck)["blocker_code"] == gates.BLOCKER_NO_US_OFFER
    await _both(env, catalog, lambda s="queued": sg_job(s, domain=JP_HOST, brand="Kosé", vendors=("KOSE",)))
    assert {(o["currency"], o["list_price"]) for o in catalog.offers("market = 'SG'")} == {("SGD", 12.9)}
    for ck in cks:  # servable, and priced for SG only: a US buyer still has no USD offer here
        assert catalog.verdict(ck)["serving_eligible"] is True
        assert catalog.priced_for(ck, ["SG"]) and not catalog.priced_for(ck, ["US"])


async def test_an_sg_capture_refuses_to_run_where_this_process_does_not_serve_sg(sg_lane, monkeypatch):
    env, catalog, applied, state = sg_lane
    await _both(env, catalog, base_job)
    monkeypatch.setenv("PIVOTA_SERVING_PRICING_REGIONS", "US")
    out = await pipeline.run_stage(sg_job(), db=catalog)
    assert (out["status"], out["outcome"]) == ("failed", "served_market_unconfigured")
    assert state.store.log == [] and catalog.offers("market = 'SG'") == []


async def test_a_us_retailer_capture_is_refused_before_any_request(sg_lane):
    env, catalog, applied, state = sg_lane
    out = await pipeline.run_stage(sg_job(market="US"), db=catalog)
    assert (out["status"], out["outcome"]) == ("failed", "invalid_job")
    assert state.store.log == []


async def test_an_sg_session_refusal_writes_nothing(sg_lane):
    env, catalog, applied, state = sg_lane
    await _both(env, catalog, base_job)
    state.store.quotes = {"SG": "USD"}
    out = await pipeline.run_stage(sg_job(), db=catalog)
    assert (out["status"], out["outcome"]) == ("nothing", "sg_session_unproven")
    assert catalog.offers("market = 'SG'") == []


@pytest.mark.parametrize("ships,outcome,status", [(["SG", "MY"], "store_does_not_ship_to_market", "nothing"),
                                                  (None, "ships_to_unverifiable", "failed")])
async def test_the_base_crawl_ships_to_gate_writes_nothing_for_a_store_that_does_not_ship_there(
        sg_lane, ships, outcome, status):
    env, catalog, applied, state = sg_lane
    state.store = usd_store(meta={**USD_META, "ships_to_countries": ships})
    out = await pipeline.run_stage(base_job(require_ships_to_market=True), db=catalog)
    assert (out["status"], out["outcome"]) == (status, outcome)
    assert applied == [] and catalog.offers() == []
    # Without the option the crawl behaves exactly as before: it records ships_to and goes on.
    out = await pipeline.run_stage(base_job(), db=catalog)
    assert out["status"] == "apply_due"
    assert list(env.ledger.runs.values())[-1]["checks"]["storefront"]["ships_to_market"] is (
        None if ships is None else False)


def test_the_ships_to_gate_is_not_part_of_the_cohort():
    # Re-enqueueing an open base cohort with the gate answers `exists` instead of opening a second job over the
    # same rows (review of #2553). To apply the gate to an open job, cancel it first.
    from db import retailer_ingest as ledger
    o = {"retailer_name": "K-Beauty Retailer"}
    assert (ledger.scope_key("kbeauty-retailer.example.com", "COSRX", o)
            == ledger.scope_key("kbeauty-retailer.example.com", "COSRX", {**o, "require_ships_to_market": True}))


@pytest.mark.parametrize("role,retailer", [(None, True), ("retailer", True), ("Retailer", True), ("", True),
                                           ("brand_official", False)])
def test_anything_but_a_brand_store_is_a_retailer_job(role, retailer):
    # The same test validate_options gates SG retailers on: a stored role that is neither value is never
    # allowed as a retailer and then selected as a brand store.
    options = {"source": "shopify_markets", "market": "SG"} | ({} if role is None else {"source_role": role})
    assert markets.is_retailer_job({"options": options}) is retailer
