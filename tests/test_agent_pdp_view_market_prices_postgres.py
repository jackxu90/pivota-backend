"""agent_pdp_view.market_prices on real Postgres (migration 263).

Pinned here, because only the server can answer them:

  * BEFORE the migration (the columns absent), the flag-off write -- UPSERT_SQL, unchanged --
    still inserts and updates, and the flag-ON write (execute_agent_pdp_view_upsert) lands the
    legacy row instead of failing. Prod deploys skip db/migrations/, so this is every
    environment's state until schema_guard runs.
  * Migration 263 applies, re-applies, and rolls back cleanly.
  * The flag-on upsert stamps market_prices_refreshed_at with the row's own refreshed_at, and the
    route's SELECT (market_prices_fresh_sql) serves the summary into an SG buyer's SGD view.
  * STALENESS: a later write without the summary -- the flag-off upsert, and the repair script's
    APV_OFFER_FIELDS_UPDATE_SQL -- retires the SGD offers and moves refreshed_at; the SELECT then
    returns NULL and the SG buyer falls back to the legacy view, never the retired offer.
  * The backfill's keyset scope finds the missing, stale and other-version rows, and only those.

The statements are driven verbatim from the modules. PRIVATE DATABASE, created and dropped here,
like test_agent_pdp_view_overlay_preservation_postgres.py: the real agent_pdp_view is a db.catalog
Table, and building it in the database the sibling *_postgres.py files share is the blast radius
those files warn about.
"""

from __future__ import annotations

import asyncio
import os
from decimal import Decimal
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL", "").startswith("postgres"),
    reason="needs a Postgres DATABASE_URL — production-dialect gate",
)

_DB_NAME = f"apv_market_prices_{os.getpid()}"
_MIGRATION = REPO_ROOT / "db" / "migrations" / "263_agent_pdp_view_market_prices.sql"
CK = "ck_" + "f" * 32
SIG = "sig_" + "f" * 32


def _private_url() -> str:
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(os.environ["DATABASE_URL"])
    return urlunsplit(parts._replace(path=f"/{_DB_NAME}"))


@pytest.fixture(scope="module")
def db_url():
    import psycopg2

    admin = psycopg2.connect(os.environ["DATABASE_URL"])
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{_DB_NAME}"')
        cur.execute(f'CREATE DATABASE "{_DB_NAME}"')
    admin.close()
    try:
        yield _private_url()
    finally:
        admin = psycopg2.connect(os.environ["DATABASE_URL"])
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (_DB_NAME,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{_DB_NAME}"')
        admin.close()


def _sync(url: str, *statements: str) -> None:
    from sqlalchemy import create_engine, text

    engine = create_engine(url, future=True)
    try:
        with engine.begin() as conn:
            for statement in statements:
                conn.execute(text(statement))
    finally:
        engine.dispose()


def _create_pre_migration_table(url: str) -> None:
    """The real model table as every environment has it BEFORE 263: the model now carries the
    column, so drop it to get the shipped pre-migration shape."""
    from sqlalchemy import create_engine

    import db.catalog  # noqa: F401  (registers agent_pdp_view on the shared MetaData)
    from db.database import metadata

    engine = create_engine(url, future=True)
    try:
        metadata.tables["agent_pdp_view"].drop(engine, checkfirst=True)
        metadata.tables["agent_pdp_view"].create(engine)
    finally:
        engine.dispose()
    _sync(url, "ALTER TABLE agent_pdp_view DROP COLUMN market_prices, DROP COLUMN market_prices_refreshed_at")


def _drive(url, coro_factory):
    import databases

    async def run():
        database = databases.Database(url.replace("postgresql://", "postgresql+asyncpg://"))
        await database.connect()
        try:
            return await coro_factory(database)
        finally:
            await database.disconnect()

    return asyncio.new_event_loop().run_until_complete(run())


def _offer(n, merchant, currency, market, price):
    return {
        "offer_id": f"of_{n}", "sku_key": f"sku_{n}", "product_key": "pk_store",
        "merchant_id": merchant, "availability": "in_stock", "currency": currency,
        "list_price": Decimal(price), "merchant_effective_price": None,
        "estimated_best_price": None, "market": market, "offer_type": "retail",
        "is_first_party": False, "merchant_name": merchant,
    }


OFFERS = [_offer(i, f"m_us{i}", "USD", "US", f"{18 + i}.00") for i in range(6)] + [
    _offer(10, "m_sg0", "SGD", "SG", "32.00"),
    _offer(11, "m_sg1", "SGD", "SG", "29.90"),
]


def _assembled(offers):
    from services.agent_pdp_view_assembler import assemble_row

    return assemble_row(
        content_key=CK,
        products=[{
            "product_key": "pk_store", "merchant_id": "m_store", "platform": "shopify",
            "source_product_id": "sp_1", "title": "Barrier Cream", "description": "A cream.",
            "brand": "Example", "product_payload": {}, "pdp_lifecycle_stage": "published",
            "pivota_signature_id": SIG, "canonical_url": "https://store.example.com/p/1",
            "sync_status": "live", "product_group_id": "grp_1", "group_is_primary": True,
        }],
        skus=[], offers=offers, external_seed=None,
    )


def test_market_prices_lifecycle(db_url, monkeypatch) -> None:
    from routes import agent_pdp_v1
    from services import agent_pdp_view_assembler as assembler
    import scripts.backfill_agent_pdp_view as backfill
    from scripts.repair_external_seed_offer_mainline import APV_OFFER_FIELDS_UPDATE_SQL

    monkeypatch.delenv("PIVOTA_SERVING_PRICING_REGIONS", raising=False)  # a writer with no regions
    monkeypatch.setenv("AGENT_PDP_V1_MARKET_PRICES_READ", "on")
    monkeypatch.delenv(assembler.MARKET_PRICES_FLAG_ENV, raising=False)
    assembler.reset_market_prices_probe()
    _create_pre_migration_table(db_url)

    legacy_row = _assembled(OFFERS)
    legacy_select = agent_pdp_v1.BYPASS_SELECT_BY_CONTENT_KEY_SQL
    market_select = agent_pdp_v1._MARKET_PRICES_SQL[legacy_select]

    async def before_migration(database):
        for _ in range(2):  # insert, then the conflict branch
            await database.execute(assembler.UPSERT_SQL, assembler.row_to_upsert_params(legacy_row))
        # Flag ON, columns absent: the legacy row lands; the refresh does not fail.
        monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
        flagged = _assembled(OFFERS)
        assert "market_prices" in flagged
        assert await assembler.agent_pdp_view_has_market_prices(database) is False
        await assembler.execute_agent_pdp_view_upsert(database, flagged)
        row = await database.fetch_one(legacy_select, {"id": CK})
        assert row is not None and row["currency"] == "USD"
        with pytest.raises(Exception, match="market_prices"):
            await database.fetch_one(market_select, {"id": CK})
        monkeypatch.delenv(assembler.MARKET_PRICES_FLAG_ENV, raising=False)

    _drive(db_url, before_migration)

    sql = _MIGRATION.read_text(encoding="utf-8")
    _sync(db_url, sql)
    _sync(db_url, sql)  # idempotent
    assembler.reset_market_prices_probe()

    async def sg_view(database):
        stored = agent_pdp_v1._row_to_dict(await database.fetch_one(market_select, {"id": CK}))
        return stored, agent_pdp_v1._market_view(stored, "SG")

    async def keyset(database):
        page_sql, params = backfill.build_content_key_query(
            scope="market_prices_missing", limit=10, offset=7, after="")
        return [r["content_key"] for r in await database.fetch_all(page_sql, params)]

    async def after_migration(database):
        assert await assembler.agent_pdp_view_has_market_prices(database) is True
        # Not computed yet: NULL, legacy view, and the backfill finds it.
        stored, view = await sg_view(database)
        assert stored["market_prices"] is None and view is stored
        assert await keyset(database) == [CK]

        # Flag-on write: stamped with the row's own NOW(); the SG buyer gets the SGD view.
        monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
        await assembler.execute_agent_pdp_view_upsert(database, _assembled(OFFERS))
        typed = dict(await database.fetch_one(
            "SELECT (market_prices_refreshed_at = refreshed_at) AS stamped, "
            "market_prices->>'version' AS version, "
            "(market_prices->'currencies'->'SGD'->>'price_min')::numeric AS sgd_min, "
            "jsonb_array_length(market_prices->'currencies'->'SGD'->'offers') AS sgd_offers, "
            "currency, price_min, offer_count, jsonb_array_length(offers) AS n_offers "
            "FROM agent_pdp_view WHERE content_key = :ck", {"ck": CK}))
        assert typed == {
            "stamped": True, "version": "1", "sgd_min": Decimal("29.9"), "sgd_offers": 2,
            "currency": "USD", "price_min": Decimal("18.00"), "offer_count": 8, "n_offers": 5,
        }
        _, view = await sg_view(database)
        assert (view["currency"], view["price_min"], view["offer_count"]) == ("SGD", 29.9, 2)
        assert {o["merchant_id"] for o in view["offers"]} == {"m_sg0", "m_sg1"}
        assert await keyset(database) == []

        # STALE (P1-1), path 1: the SGD offers are retired and a writer WITHOUT the flag refreshes.
        monkeypatch.delenv(assembler.MARKET_PRICES_FLAG_ENV, raising=False)
        usd_only = [o for o in OFFERS if o["currency"] == "USD"]
        await assembler.execute_agent_pdp_view_upsert(database, _assembled(usd_only))
        stored, view = await sg_view(database)
        assert stored["market_prices"] is None and view is stored
        assert all(o["currency"] == "USD" for o in view["offers"])
        assert await keyset(database) == [CK]

        # Re-stamp, then STALE path 2: the repair script's offer-field update.
        monkeypatch.setenv(assembler.MARKET_PRICES_FLAG_ENV, "on")
        await assembler.execute_agent_pdp_view_upsert(database, _assembled(OFFERS))
        assert (await sg_view(database))[1]["currency"] == "SGD"
        await database.execute(APV_OFFER_FIELDS_UPDATE_SQL, {
            "content_key": CK, "currency": "USD", "price_min": Decimal("18.00"),
            "price_max": Decimal("23.00"), "offer_count": 6, "offers": "[]",
            "refresh_source": "repair_test",
        })
        stored, view = await sg_view(database)
        assert stored["market_prices"] is None and view is stored

        # Another version is not served, and the backfill re-selects it.
        await assembler.execute_agent_pdp_view_upsert(database, _assembled(OFFERS))
        await database.execute(
            "UPDATE agent_pdp_view SET market_prices = jsonb_set(market_prices, '{version}', '2') "
            "WHERE content_key = :ck", {"ck": CK})
        assert (await sg_view(database))[0]["market_prices"] is None
        assert await keyset(database) == [CK]
        page_sql, params = backfill.build_content_key_query(
            scope="market_prices_missing", limit=10, offset=0, after=CK)
        assert await database.fetch_all(page_sql, params) == []

        # A recompute that finds nothing priced: a version-1 object with no currencies, never NULL.
        await assembler.execute_agent_pdp_view_upsert(database, _assembled([]))
        assert await database.fetch_val(
            "SELECT market_prices->'currencies' = '{}'::jsonb FROM agent_pdp_view WHERE content_key = :ck",
            {"ck": CK}) is True

    _drive(db_url, after_migration)
    assembler.reset_market_prices_probe()


def test_down_migration_drops_the_columns(db_url) -> None:
    _create_pre_migration_table(db_url)
    _sync(db_url, _MIGRATION.read_text(encoding="utf-8"))
    down = REPO_ROOT / "db" / "migrations" / "down" / "263_agent_pdp_view_market_prices_down.sql"
    _sync(db_url, down.read_text(encoding="utf-8"))

    async def check(database):
        return await database.fetch_val(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_name = 'agent_pdp_view' AND column_name LIKE 'market_prices%'")

    assert _drive(db_url, check) == 0
