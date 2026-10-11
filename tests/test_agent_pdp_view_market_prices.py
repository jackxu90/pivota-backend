"""agent_pdp_view.market_prices (migration 263): the per-currency price summary, and the agent
PDP route's view and buy pick for the BUYER's market.

What is pinned, and why:

  * The summary is keyed by CURRENCY, versioned, and independent of the writer's environment: a
    writer with PIVOTA_SERVING_PRICING_REGIONS unset (a one-off job) writes the same value as one
    with US,SG.
  * Legacy columns: identical with the write flag on and off (origin/main parity is
    test_agent_pdp_view_market_prices_legacy_parity.py).
  * Flag off: assemble_row emits no summary and the upsert never names the columns; flag on with
    the columns absent: the legacy upsert, so refreshes keep landing.
  * Route: the buyer market is honoured only behind AGENT_PDP_V1_MARKET_PRICES_READ and only for a
    served region (never an acquisition market); a US / silent buyer never names the columns and
    is byte-identical with every flag on; an SG buyer gets the SGD price, offers, offers_count and
    buy pick -- the same values the gateway serves; a missing column falls back per request.
  * Staleness (a flag-off write after the summary) is a property of the SQL stamp and is pinned
    on real Postgres in test_agent_pdp_view_market_prices_postgres.py.

Every row is built by the real assembler (assemble_row) from offers in the shape
fetch_offers_for_keys SELECTs, and reaches the route the way the database hands it back (jsonb as
JSON text).
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import routes.agent_pdp_v1 as agent_pdp_v1  # noqa: E402
from services import agent_pdp_view_assembler as assembler  # noqa: E402

CK = "ck_" + "e" * 32
SIG = "sig_" + "e" * 32
READ_FLAG = "AGENT_PDP_V1_MARKET_PRICES_READ"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("PIVOTA_SERVING_PRICING_REGIONS", "US,SG")
    monkeypatch.delenv(assembler.MARKET_PRICES_FLAG_ENV, raising=False)
    monkeypatch.delenv(READ_FLAG, raising=False)
    monkeypatch.delenv("AGENT_PDP_SERVING_MARKET", raising=False)
    assembler.reset_market_prices_probe()
    yield
    assembler.reset_market_prices_probe()


def _offer(n: int, *, merchant: str, currency: str, market: str, price: str,
           availability: str = "in_stock") -> Dict[str, Any]:
    """One row as fetch_offers_for_keys returns it (its SELECT list, nothing more)."""
    return {
        "offer_id": f"of_{n}", "sku_key": f"sku_{n}", "product_key": "pk_store",
        "merchant_id": merchant, "availability": availability, "currency": currency,
        "list_price": Decimal(price), "merchant_effective_price": None,
        "estimated_best_price": None, "market": market, "offer_type": "retail",
        "is_first_party": False, "merchant_name": merchant,
    }


def _products() -> List[Dict[str, Any]]:
    return [{
        "product_key": "pk_store", "merchant_id": "m_store", "platform": "shopify",
        "source_product_id": "sp_1", "title": "Barrier Cream", "description": "A cream.",
        "brand": "Example", "product_payload": {}, "pdp_lifecycle_stage": "published",
        "pivota_signature_id": SIG, "canonical_url": "https://store.example.com/p/1",
        "sync_status": "live", "product_group_id": "grp_1", "group_is_primary": True,
    }]


def _assemble(offers: List[Dict[str, Any]]) -> Dict[str, Any]:
    row = assembler.assemble_row(
        content_key=CK, products=_products(), skus=[], offers=offers, external_seed=None,
    )
    assert row is not None
    return row


def _entry(row: Dict[str, Any], currency: str) -> Optional[Dict[str, Any]]:
    return assembler.market_prices_entry(row["market_prices"], currency)


USD_ONLY = [
    _offer(1, merchant="m_store", currency="USD", market="US", price="24.00"),
    _offer(2, merchant="m_a", currency="USD", market="US", price="21.00"),
    _offer(3, merchant="m_b", currency="USD", market="US", price="22.50", availability="out_of_stock"),
]

# Six USD offers and three SGD siblings. SGD amounts are larger numbers, so the legacy
# cross-currency top-5 keeps only USD offers, and USD is the modal currency.
MIXED = [
    _offer(i, merchant=f"m_us{i}", currency="USD", market="US", price=f"{18 + i}.00")
    for i in range(6)
] + [
    _offer(10, merchant="m_sg0", currency="SGD", market="SG", price="32.00"),
    _offer(11, merchant="m_sg1", currency="SGD", market="SG", price="29.90"),
    _offer(12, merchant="m_sg2", currency="SGD", market="US", price="35.00", availability="sold_out"),
]

SGD_ONLY = [
    _offer(1, merchant="m_store", currency="SGD", market="SG", price="32.00"),
    _offer(2, merchant="m_sg1", currency="SGD", market="SG", price="30.00"),
]


# ---------------------------------------------------------------------------
# assembler
# ---------------------------------------------------------------------------


def test_flag_off_emits_no_market_prices_key_and_the_legacy_upsert() -> None:
    row = _assemble(MIXED)
    assert "market_prices" not in row
    assert assembler.upsert_sql_for_row(row) is assembler.UPSERT_SQL
    assert "market_prices" not in assembler.UPSERT_SQL
    assert "market_prices" not in assembler.row_to_upsert_params(row)


@pytest.mark.parametrize("offers", [USD_ONLY, MIXED, SGD_ONLY], ids=["usd", "usd+sgd", "sgd"])
def test_legacy_columns_are_identical_with_the_flag_on(monkeypatch, offers) -> None:
    off = _assemble(offers)
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    on = _assemble(offers)
    assert {k: v for k, v in on.items() if k != "market_prices"} == off
    params_on = assembler.row_to_upsert_params(on)
    params_off = assembler.row_to_upsert_params(off)
    assert set(params_on) - set(params_off) == {"market_prices"}
    assert {k: params_on[k] for k in params_off} == params_off


def test_summary_does_not_depend_on_the_writers_served_regions(monkeypatch) -> None:
    """P1-2: a writer with the regions unset (defaults to US) or US-only stores exactly what a
    US,SG writer stores -- the SGD entry included."""
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    reference = _assemble(MIXED)["market_prices"]
    for regions in (None, "US", "SG", "JP,AU"):
        if regions is None:
            monkeypatch.delenv("PIVOTA_SERVING_PRICING_REGIONS", raising=False)
        else:
            monkeypatch.setenv("PIVOTA_SERVING_PRICING_REGIONS", regions)
        assert _assemble(MIXED)["market_prices"] == reference
    assert set(reference["currencies"]) == {"USD", "SGD"}
    monkeypatch.delenv("PIVOTA_SERVING_PRICING_REGIONS", raising=False)
    assert set(_assemble(SGD_ONLY)["market_prices"]["currencies"]) == {"SGD"}


def test_usd_only_usd_entry_restates_the_legacy_columns(monkeypatch) -> None:
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    row = _assemble(USD_ONLY)
    assert row["market_prices"]["version"] == assembler.MARKET_PRICES_VERSION
    assert set(row["market_prices"]["currencies"]) == {"USD"}
    usd = _entry(row, "USD")
    assert Decimal(str(usd["price_min"])) == row["price_min"] == Decimal("21.00")
    assert Decimal(str(usd["price_max"])) == row["price_max"] == Decimal("24.00")
    assert usd["offer_count"] == row["offer_count"] == 3
    assert usd["offers"] == row["offers"]


def test_usd_plus_sgd_keeps_legacy_usd_and_adds_both_entries(monkeypatch) -> None:
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    row = _assemble(MIXED)

    # Legacy: modal USD, USD range, every offer counted, top-5 holds no SGD offer.
    assert row["currency"] == "USD"
    assert (row["price_min"], row["price_max"]) == (Decimal("18.00"), Decimal("23.00"))
    assert row["offer_count"] == 9
    assert {o["currency"] for o in row["offers"]} == {"USD"}

    usd, sgd = _entry(row, "USD"), _entry(row, "SGD")
    assert (usd["price_min"], usd["price_max"], usd["offer_count"]) == (18.0, 23.0, 6)
    assert usd["offers"] == row["offers"]
    # SGD: the range over every SGD offer (sold-out included, like offer_count), no conversion,
    # and its offers in the stored order: sellable first, then price.
    assert (sgd["price_min"], sgd["price_max"], sgd["offer_count"]) == (29.9, 35.0, 3)
    assert [o["merchant_id"] for o in sgd["offers"]] == ["m_sg1", "m_sg0", "m_sg2"]
    assert set(sgd["offers"][0]) == set(row["offers"][0])


def test_primary_store_sgd_sibling_ranks_first_within_sgd(monkeypatch) -> None:
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    offers = MIXED + [_offer(13, merchant="m_store", currency="SGD", market="SG", price="33.00")]
    sgd = _entry(_assemble(offers), "SGD")
    assert sgd["offers"][0]["merchant_id"] == "m_store"
    assert sgd["offers"][0]["is_primary"] is True
    assert sgd["offers"][0]["url"] == "https://store.example.com/p/1"


def test_top_n_caps_each_currency_independently(monkeypatch) -> None:
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    offers = [
        _offer(i, merchant=f"m_us{i}", currency="USD", market="US", price=f"{10 + i}.00")
        for i in range(7)
    ] + [
        _offer(20 + i, merchant=f"m_sg{i}", currency="SGD", market="SG", price=f"{40 + i}.00")
        for i in range(7)
    ]
    row = _assemble(offers)
    assert row["currency"] == "USD"  # tie 7/7 -> the higher code, exactly as before
    for currency in ("USD", "SGD"):
        entry = _entry(row, currency)
        assert entry["offer_count"] == 7
        assert len(entry["offers"]) == assembler.OFFER_TOP_N
        assert {o["currency"] for o in entry["offers"]} == {currency}


def test_currency_spelling_is_normalised_and_junk_codes_get_no_entry(monkeypatch) -> None:
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    offers = [
        _offer(1, merchant="m_a", currency=" usd", market="US", price="10.00"),
        _offer(2, merchant="m_b", currency="", market="US", price="11.00"),
        _offer(3, merchant="m_c", currency="US$", market="US", price="12.00"),
    ]
    assert set(_assemble(offers)["market_prices"]["currencies"]) == {"USD"}
    row = _assemble([])
    assert row["market_prices"] == {"version": 1, "currencies": {}}
    assert json.loads(assembler.row_to_upsert_params(row)["market_prices"]) == {"version": 1, "currencies": {}}


def test_flag_on_upsert_stamps_the_summary_with_the_rows_own_now() -> None:
    sql = assembler.UPSERT_SQL_WITH_MARKET_PRICES
    assert "CAST(:market_prices AS jsonb), NOW()," in sql
    assert "market_prices_refreshed_at = EXCLUDED.market_prices_refreshed_at" in sql
    stripped = (
        sql.replace(" market_prices, market_prices_refreshed_at,", "", 1)
        .replace(" CAST(:market_prices AS jsonb), NOW(),", "", 1)
        .replace("      market_prices = EXCLUDED.market_prices,\n", "", 1)
        .replace("      market_prices_refreshed_at = EXCLUDED.market_prices_refreshed_at,\n", "", 1)
    )
    assert stripped == assembler.UPSERT_SQL


def test_market_prices_entry_reads_only_version_1() -> None:
    summary = {"version": 1, "currencies": {"SGD": {"price_min": 1.0}}}
    assert assembler.market_prices_entry(summary, "sgd") == {"price_min": 1.0}
    assert assembler.market_prices_entry(json.dumps(summary), "SGD") == {"price_min": 1.0}
    for other in (None, "", "junk", {"version": 2, "currencies": {"SGD": {}}}, {"SG": {"currency": "SGD"}}):
        assert assembler.market_prices_entry(other, "SGD") is None
    assert assembler.market_prices_entry(summary, "USD") is None


class _WriteDb:
    def __init__(self, columns_present: bool, fail_with: Optional[Exception] = None) -> None:
        self.columns_present = columns_present
        self.fail_with = fail_with
        self.executed: List[str] = []

    async def fetch_one(self, query: str, values: Optional[Dict[str, Any]] = None):
        assert "information_schema.columns" in query
        return {"n": 2 if self.columns_present else 0}

    async def execute(self, query: str, values: Optional[Dict[str, Any]] = None):
        if self.fail_with is not None and "market_prices" in query:
            raise self.fail_with
        self.executed.append(query)


@pytest.mark.asyncio
async def test_write_flag_on_without_the_columns_writes_the_legacy_upsert(monkeypatch) -> None:
    """P2-2: the heal not run yet must not make every refresh fail (catalog_sync swallows it)."""
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    row = _assemble(MIXED)
    db = _WriteDb(columns_present=False)
    await assembler.execute_agent_pdp_view_upsert(db, row)
    assert db.executed == [assembler.UPSERT_SQL]
    assembler.reset_market_prices_probe()
    db = _WriteDb(columns_present=True)
    await assembler.execute_agent_pdp_view_upsert(db, row)
    assert db.executed == [assembler.UPSERT_SQL_WITH_MARKET_PRICES]


@pytest.mark.asyncio
async def test_a_column_dropped_after_the_probe_falls_back_on_the_write(monkeypatch) -> None:
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    missing = Exception('column "market_prices" of relation "agent_pdp_view" does not exist')
    db = _WriteDb(columns_present=True, fail_with=missing)
    await assembler.execute_agent_pdp_view_upsert(db, _assemble(MIXED))
    assert db.executed == [assembler.UPSERT_SQL]
    other = _WriteDb(columns_present=True, fail_with=RuntimeError("deadlock detected"))
    assembler.reset_market_prices_probe()
    with pytest.raises(RuntimeError):
        await assembler.execute_agent_pdp_view_upsert(other, _assemble(MIXED))


# ---------------------------------------------------------------------------
# route
# ---------------------------------------------------------------------------


def _as_stored(row: Dict[str, Any]) -> Dict[str, Any]:
    """What the route's SELECT hands back for an assembled row: the selected columns, jsonb as
    JSON text (asyncpg registers no codec), and the summary when the row has one (the fresh-stamp
    CASE is SQL, pinned on Postgres)."""
    params = assembler.row_to_upsert_params(row)
    stored = {col: params.get(col) for col in agent_pdp_v1.AGENT_PDP_VIEW_COLUMNS}
    stored["refreshed_at"] = None
    stored["pdp_renderable"] = True
    stored["market_prices"] = params.get("market_prices")
    return stored


class _FakeDb:
    def __init__(self, stored: Dict[str, Any], *, columns_present: bool = True,
                 select_fails: Optional[Exception] = None) -> None:
        self.stored = stored
        self.columns_present = columns_present
        self.select_fails = select_fails
        self.queries: List[str] = []

    async def fetch_one(self, query: str, values: Optional[Dict[str, Any]] = None):
        text = str(query)
        self.queries.append(text)
        if "information_schema.columns" in text:
            return {"n": 2 if self.columns_present else 0}
        if "agent_pdp_view" in text:
            if "market_prices" in text and self.select_fails is not None:
                raise self.select_fails
            row = dict(self.stored)
            if "market_prices" not in text:
                row.pop("market_prices", None)
            return row
        return None

    async def fetch_all(self, query: str, values: Optional[Dict[str, Any]] = None):
        return []


def _get(monkeypatch, stored: Dict[str, Any], query: str = "", **db_kwargs) -> Dict[str, Any]:
    db = _FakeDb(stored, **db_kwargs)
    monkeypatch.setattr(agent_pdp_v1, "database", db)
    app = FastAPI()
    app.include_router(agent_pdp_v1.router)
    response = TestClient(app).get(f"/api/agent/pdp/{CK}{query}")
    assert response.status_code == 200
    body = response.json()
    body["_queries"] = db.queries
    return body


def _product(body):
    canonical = next(m for m in body["modules"] if m["type"] == "canonical")
    return canonical["data"]["pdp_payload"]["product"]


def _offers_module(body):
    return next(m for m in body["modules"] if m["type"] == "offers")["data"]


def _buy_pick(body):
    return next(o for o in _offers_module(body)["offers"] if o["is_buy_pick"])


QUERIES = ["", "?serving_market=US", "?serving_market=us", "?serving_market=en-US", "?serving_market=ZZ"]


@pytest.mark.parametrize("offers", [USD_ONLY, MIXED, SGD_ONLY], ids=["usd", "usd+sgd", "sgd"])
@pytest.mark.parametrize("query", QUERIES)
def test_us_and_silent_buyers_are_byte_identical_with_every_flag_on(monkeypatch, offers, query) -> None:
    baseline = _get(monkeypatch, _as_stored(_assemble(offers)))
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    monkeypatch.setenv(READ_FLAG, "on")
    flagged = _get(monkeypatch, _as_stored(_assemble(offers)), query)
    # P2-1: a US / silent buyer never probes or names the columns.
    assert not any("market_prices" in q or "information_schema" in q for q in flagged.pop("_queries"))
    baseline.pop("_queries")
    assert flagged == baseline


@pytest.mark.parametrize("query", ["?serving_market=SG", "?serving_market=JP", "?serving_market=AU"])
def test_read_flag_off_ignores_the_buyer_market_entirely(monkeypatch, query) -> None:
    """P2-3: nothing about the route changes on merge, whatever serving_market says."""
    stored = _as_stored(_assemble(MIXED + [_offer(30, merchant="m_jp", currency="JPY", market="JP", price="3000")]))
    baseline = _get(monkeypatch, stored)
    body = _get(monkeypatch, stored, query)
    assert not any("market_prices" in q or "information_schema" in q for q in body.pop("_queries"))
    baseline.pop("_queries")
    assert body == baseline


def test_an_unserved_market_never_gets_a_foreign_buy_pick(monkeypatch) -> None:
    """P2-3: JP is priceable (JPY) but not served: a stored JPY acquisition offer must not become
    the buy pick, with the read flag on."""
    monkeypatch.setenv(READ_FLAG, "on")
    offers = [
        _offer(1, merchant="m_store", currency="USD", market="US", price="24.00"),
        _offer(2, merchant="m_jp", currency="JPY", market="JP", price="3000"),
    ]
    stored = _as_stored(_assemble(offers))
    us = _get(monkeypatch, stored)
    jp = _get(monkeypatch, stored, "?serving_market=JP")
    us.pop("_queries"), jp.pop("_queries")
    assert jp == us
    assert _buy_pick(jp)["currency"] == "USD"


def test_sg_buyer_gets_the_sgd_entry_everywhere_the_gateway_does(monkeypatch) -> None:
    """P2-4: price, currency, offers AND offers_count are the SGD entry's, as the gateway serves."""
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    monkeypatch.setenv(READ_FLAG, "on")
    row = _assemble(MIXED)
    body = _get(monkeypatch, _as_stored(row), "?serving_market=SG")
    sgd = _entry(row, "SGD")

    product = _product(body)
    assert product["currency"] == "SGD"
    assert product["price"] == {"current": {"amount": 29.9, "currency": "SGD"}}
    assert (product["price_min"], product["price_max"]) == (29.9, 35.0)
    assert "market_prices" not in product
    offers_module = _offers_module(body)
    assert offers_module["offers_count"] == body["offers_count"] == sgd["offer_count"] == 3
    assert [o["merchant_id"] for o in offers_module["offers"]] == [o["merchant_id"] for o in sgd["offers"]]
    pick = _buy_pick(body)
    assert (pick["currency"], pick["merchant_id"]) == ("SGD", "m_sg1")
    assert any("market_prices_refreshed_at = apv.refreshed_at" in q for q in body["_queries"])


def test_sg_buyer_on_a_row_with_no_sgd_entry_keeps_the_legacy_view(monkeypatch) -> None:
    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    monkeypatch.setenv(READ_FLAG, "on")
    body = _get(monkeypatch, _as_stored(_assemble(USD_ONLY)), "?serving_market=SG")
    assert _product(body)["currency"] == "USD"
    assert {o["currency"] for o in _offers_module(body)["offers"]} == {"USD"}


@pytest.mark.parametrize("summary", [None, json.dumps({"SG": {"currency": "SGD"}}), json.dumps({"version": 2})])
def test_sg_buyer_on_a_stale_unbuilt_or_unknown_summary_keeps_the_legacy_view(monkeypatch, summary) -> None:
    monkeypatch.setenv(READ_FLAG, "on")
    stored = _as_stored(_assemble(MIXED))
    stored["market_prices"] = summary  # NULL is what the fresh-stamp CASE returns for a stale one
    body = _get(monkeypatch, stored, "?serving_market=SG")
    assert _product(body)["currency"] == "USD"
    assert len(_offers_module(body)["offers"]) == assembler.OFFER_TOP_N
    assert _offers_module(body)["offers_count"] == 9


def test_sg_buyer_with_the_columns_absent_never_names_them(monkeypatch) -> None:
    monkeypatch.setenv(READ_FLAG, "on")
    body = _get(monkeypatch, _as_stored(_assemble(MIXED)), "?serving_market=SG", columns_present=False)
    assert not any("market_prices" in q and "agent_pdp_view apv" in q for q in body["_queries"])
    assert _product(body)["currency"] == "USD"


def test_a_missing_column_at_select_time_retries_the_legacy_select(monkeypatch) -> None:
    """P2-1: the probe raced a dropped / deferred heal: the request still answers, legacy."""
    monkeypatch.setenv(READ_FLAG, "on")
    missing = Exception("column apv.market_prices_refreshed_at does not exist")
    body = _get(monkeypatch, _as_stored(_assemble(MIXED)), "?serving_market=SG", select_fails=missing)
    assert _product(body)["currency"] == "USD"
    selects = [q for q in body["_queries"] if "FROM agent_pdp_view" in q]
    assert len(selects) == 2 and "market_prices" in selects[0] and "market_prices" not in selects[1]
    # ...and the next request does not try again until re-probed.
    body = _get(monkeypatch, _as_stored(_assemble(MIXED)), "?serving_market=SG", select_fails=missing)
    assert not any("market_prices" in q for q in body["_queries"])


def test_buyer_market_normalisation(monkeypatch) -> None:
    norm = agent_pdp_v1._normalize_buyer_market
    assert norm("SG") is None  # read flag off
    monkeypatch.setenv(READ_FLAG, "on")
    assert norm("sg") == "SG" and norm(" US ") == "US"
    for junk in (None, "", "en-US", "US,SG", "USA", "ZZ", "JP", "AU", 7, agent_pdp_v1.Query(default=None)):
        assert norm(junk) is None
    monkeypatch.setenv("PIVOTA_SERVING_PRICING_REGIONS", "US")
    assert norm("SG") is None


# ---------------------------------------------------------------------------
# backfill (--scope market_prices_missing)
# ---------------------------------------------------------------------------


def test_keyset_scope_never_takes_an_offset() -> None:
    import scripts.backfill_agent_pdp_view as backfill

    sql, params = backfill.build_content_key_query(scope="market_prices_missing", limit=50, offset=999, after="ck_x")
    assert "OFFSET" not in sql and "offset" not in params
    assert params == {"after": "ck_x", "limit": 50}
    # What readers ignore is exactly what the scope selects: missing, stale, other version.
    assert "market_prices IS NULL" in sql
    assert "market_prices_refreshed_at IS DISTINCT FROM refreshed_at" in sql
    assert "market_prices->>'version' IS DISTINCT FROM '1'" in sql
    sql, params = backfill.build_content_key_query(scope="all", limit=50, offset=10)
    assert params == {"limit": 50, "offset": 10}


class _BackfillDb:
    is_connected = True

    def __init__(self, keys: List[str]) -> None:
        self.keys = keys
        self.executed: List[str] = []

    async def fetch_all(self, query: str, values: Optional[Dict[str, Any]] = None):
        return [{"content_key": k} for k in self.keys]

    async def fetch_one(self, query: str, values: Optional[Dict[str, Any]] = None):
        if "information_schema.columns" in query:
            return {"n": 2}
        return None  # the served row's overlays: none, so nothing can downgrade

    async def execute(self, query: str, values: Optional[Dict[str, Any]] = None):
        self.executed.append(values["content_key"])


@pytest.mark.asyncio
async def test_a_failing_row_is_counted_the_page_continues_and_next_after_is_reported(monkeypatch) -> None:
    import argparse

    import scripts.backfill_agent_pdp_view as backfill

    monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
    keys = ["ck_a", "ck_b", "ck_c"]
    db = _BackfillDb(keys)
    monkeypatch.setattr(backfill, "database", db)

    async def build(content_key, *, refresh_source):
        if content_key == "ck_b":
            raise RuntimeError("source read failed")
        return {**_assemble(MIXED), "content_key": content_key}

    sleeps: List[float] = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(backfill, "build_agent_pdp_view_row", build)
    monkeypatch.setattr(backfill.asyncio, "sleep", fake_sleep)
    report = await backfill._drive(argparse.Namespace(
        apply=True, scope="market_prices_missing", limit=3, offset=0, after="", sleep=0.25,
    ))
    assert db.executed == ["ck_a", "ck_c"]
    assert report["outcome_counts"]["rows_failed"] == 1
    assert report["outcome_counts"]["rows_upserted"] == 2
    assert report["failed_sample"][0]["content_key"] == "ck_b"
    assert report["next_after"] == "ck_c"
    assert sleeps == [0.25, 0.25, 0.25]


@pytest.mark.asyncio
async def test_the_scope_refuses_to_run_with_the_write_flag_off() -> None:
    import argparse

    import scripts.backfill_agent_pdp_view as backfill

    with pytest.raises(SystemExit):
        await backfill._drive(argparse.Namespace(
            apply=True, scope="market_prices_missing", limit=3, offset=0, after="", sleep=0.0,
        ))
