"""db/agent_purchase_ledger.py on Postgres: the production dialect.

What only Postgres can show: that the self-heal builds the SAME schema as migration 263 (columns,
pg_indexes.indexdef, CHECK definitions), that concurrent writers on separate connections leave
exactly one parent, and that jsonb and timestamptz come back as Python values through asyncpg.

    DATABASE_URL=postgresql://postgres:postgres@localhost:5432/pivota_dialect_check \\
        pytest tests/test_agent_purchase_ledger_postgres.py
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
_IS_PG = DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://")

pytestmark = pytest.mark.skipif(
    not _IS_PG,
    reason="needs a Postgres DATABASE_URL; this is the production-dialect gate for migration 263",
)

_MIGRATION = Path(__file__).resolve().parent.parent / "db/migrations/263_agent_purchases.sql"
_SAFE_DB_MARKERS = ("dialect_check", "_test", "test_", "localhost/pivota_dialect")

AGENT = "agent_pg"
OWNER = "hash_pg"


def _assert_throwaway_database() -> None:
    dbname = DATABASE_URL.rsplit("/", 1)[-1].split("?")[0]
    if not any(m in dbname or m in DATABASE_URL for m in _SAFE_DB_MARKERS):
        pytest.skip(f"refusing to drop agent_purchases in database {dbname!r}: throwaway only")


@pytest.fixture(autouse=True)
async def _db():
    from db.database import database
    from db.schema_guard import ensure_required_schema_light

    _assert_throwaway_database()
    was_connected = database.is_connected
    if not was_connected:
        await database.connect()
    await database.execute("DROP TABLE IF EXISTS agent_purchases")
    # The Reap tables the parent copies from, built the way production builds them.
    await ensure_required_schema_light()
    try:
        yield
    finally:
        for table in ("agent_purchases", "reap_agentic_purchases"):
            try:
                await database.execute(f"DELETE FROM {table}")
            except Exception:  # noqa: BLE001 - a table this run never built is not a leak
                continue
        if not was_connected and database.is_connected:
            await database.disconnect()


async def _fingerprint():
    from db.database import database

    columns = [
        tuple(r) for r in await database.fetch_all(
            """
            SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
              FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = 'agent_purchases'
             ORDER BY column_name
            """
        )
    ]
    indexes = {
        r["indexname"]: " ".join(r["indexdef"].split())
        for r in await database.fetch_all(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = 'agent_purchases'"
        )
    }
    checks = sorted(
        r["def"] for r in await database.fetch_all(
            """
            SELECT pg_get_constraintdef(c.oid) AS def
              FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
             WHERE t.relname = 'agent_purchases' AND c.contype = 'c'
            """
        )
    )
    return columns, indexes, checks


async def _reap(agent_id: str = AGENT, owner: str = OWNER) -> str:
    import db.reap_agentic_ledger as reap_ledger

    purchase = await reap_ledger.create_purchase(
        buyer_ref="bref_pg", agent_id=agent_id, agent_user_ref_hash=owner,
        merchant_domain="brand.example", product_key="pk_1", quantity=1,
        currency="USD", our_price_minor=4250,
    )
    return purchase["id"]


async def test_the_self_heal_builds_the_same_schema_as_migration_263():
    from db.database import database
    from db.sql_migrations import split_statements
    import db.agent_purchase_ledger as purchases

    await database.execute("DROP TABLE IF EXISTS agent_purchases")
    for statement in split_statements(_MIGRATION.read_text(encoding="utf-8")):
        await database.execute(statement)
    from_migration = await _fingerprint()
    assert from_migration[0] and from_migration[1] and from_migration[2]

    await database.execute("DROP TABLE IF EXISTS agent_purchases")
    await purchases.ensure_agent_purchase_schema()
    assert await _fingerprint() == from_migration


async def test_the_rail_index_is_unique_as_built():
    _columns, indexes, _checks = await _fingerprint()
    assert indexes["uq_agent_purchases_rail_purchase"].startswith("CREATE UNIQUE INDEX")
    assert not indexes["idx_agent_purchases_owner"].startswith("CREATE UNIQUE INDEX")


async def test_concurrent_writers_leave_exactly_one_parent():
    from db.database import database
    import db.agent_purchase_ledger as purchases

    rp = await _reap()
    ids = await asyncio.gather(*(purchases.ensure_reap_parent(rp) for _ in range(12)))
    assert len(set(ids)) == 1 and ids[0].startswith("pp_")
    count = await database.fetch_val(
        "SELECT count(*) FROM agent_purchases WHERE rail_purchase_id = :rp", {"rp": rp}
    )
    assert count == 1


async def test_jsonb_and_timestamptz_come_back_as_python_values():
    import db.agent_purchase_ledger as purchases
    import db.reap_agentic_ledger as reap_ledger

    rp = await _reap()
    pp = await purchases.ensure_reap_parent(rp)
    parent = await purchases.get_for_owner(pp, AGENT, OWNER)
    child = await reap_ledger.get_purchase_internal(rp)
    assert parent["routing_plan"] == {
        "rail": "reap", "executor": "rail_managed", "basis": "agent_selected_rail",
    }
    assert parent["created_at"] == child["created_at"]
    assert parent["created_at"].tzinfo is not None


async def test_owner_reads_lists_and_backfill_on_postgres():
    import db.agent_purchase_ledger as purchases

    mine = [await _reap() for _ in range(3)]
    theirs = await _reap(agent_id="agent_other")
    assert await purchases.heal_reap_parents_for_owner(AGENT, OWNER) == 3
    listed = await purchases.list_for_owner(AGENT, OWNER, limit=10)
    assert sorted(p["rail_purchase_id"] for p in listed) == sorted(mine)
    assert await purchases.get_by_rail_id_for_owner("reap", theirs, AGENT, OWNER) is None
    assert await purchases.backfill_reap_parents() == 1
    assert await purchases.backfill_reap_parents() == 0
    assert await purchases.get_by_rail_id_for_owner("reap", theirs, "agent_other", OWNER)
