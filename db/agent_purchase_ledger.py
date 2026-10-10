"""The rail-neutral purchase ledger (migration 263): one `agent_purchases` row per purchase on ANY
payment rail, pointing at that rail's own purchase row.

Payment orchestration P0. Today the
only rail is Reap (`reap_agentic_purchases`, executor `rail_managed`: the rail places the order).
The next rail adds a second child table and a second value in the two CHECKs below.

── THE PARENT HAS NO STATE, ON PURPOSE ──────────────────────────────────────────────────────

The rail's own row is the state machine, and on Reap it is moved by a dozen writers: the
fenced transition, the claim/release, three sweeps, the operator resolutions. A `state` column
here would be a second copy of that answer, and this driver cannot hold a transaction across the
two writes (db/reap_agentic_ledger.py, driver properties 1-6), so the copy WOULD drift. So the
parent holds only what never changes after the purchase opens -- which rail, which executor,
whose purchase, how the rail was chosen -- and every status read goes to the child.
`services/payment_orchestration/rails.py` maps the child's state onto the shared vocabulary.

── WRITTEN AFTER THE RAIL'S OWN COMMIT, AND HEALED ──────────────────────────────────────────

`ensure_reap_parent` copies identity FROM the committed child row in one INSERT ... SELECT, so
the parent cannot disagree with the child about who owns it or when it was opened. It is called
by the Reap create route after that route's own transaction commits, best-effort and behind
`AGENT_PURCHASE_LEDGER_ENABLED`; it is never inside the rail's transaction, so a fault here can
never fail or roll back a Reap purchase. A parent that was never written is not an error: the
unified read heals it on first touch (`heal_reap_parents_for_owner`) and
`backfill_reap_parents` heals the rest. Both are idempotent on the (rail, rail_purchase_id)
unique index, so any number of concurrent healers leave exactly one row.

This module opens no database transactions (same reason as the Reap ledger:
databases==0.7.0 scopes connections by ContextVar, so a held transaction swallows concurrent
tasks' writes). Every statement is a module-level constant so the PREPARE gate
(tests/test_repo_sql_prepare_postgres.py) sees it.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any, Dict, List, Optional

from db.database import IS_POSTGRES, database

# Generic row decoders, shared with the Reap ledger rather than copied: jsonb arrives as text on
# raw SQL and SQLite timestamps arrive as text; both come out of here as Python values.
from db.reap_agentic_ledger import _decode_dt, _decode_json

AGENT_PURCHASE_LEDGER_ENABLED_ENV = "AGENT_PURCHASE_LEDGER_ENABLED"
_TRUTHY = frozenset({"1", "true", "on", "yes"})

#: The vocabularies. The CHECKs in `_CREATE_TABLE_PG` hold the same strings; a test parses them
#: back out so the two cannot disagree.
RAILS = ("reap",)
EXECUTORS = ("rail_managed",)


def is_enabled() -> bool:
    """Default OFF. An allowlist of truthy spellings, like every dial on the payment rails."""
    return (os.getenv(AGENT_PURCHASE_LEDGER_ENABLED_ENV) or "").strip().lower() in _TRUTHY


# ── schema ───────────────────────────────────────────────────────────────────────────────────
#
# db/migrations/263_agent_purchases.sql is the same DDL. Production startup runs in fast mode and
# skips db/migrations/, so db/schema_guard.ensure_required_schema_light calls
# `ensure_agent_purchase_schema` on both dialects. tests/test_agent_purchase_ledger_postgres.py
# proves the two build the same columns, indexes and CHECKs.

_CREATE_TABLE_PG = """
CREATE TABLE IF NOT EXISTS agent_purchases (
    id VARCHAR(64) PRIMARY KEY,
    rail VARCHAR(16) NOT NULL CHECK (rail IN ('reap')),
    executor VARCHAR(24) NOT NULL CHECK (executor IN ('rail_managed')),
    rail_purchase_id VARCHAR(64) NOT NULL,
    agent_id VARCHAR(128),
    agent_user_ref_hash VARCHAR(64),
    routing_plan JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""
_CREATE_TABLE_SQLITE = (
    _CREATE_TABLE_PG.replace("TIMESTAMPTZ", "TIMESTAMP")
    .replace("JSONB", "TEXT")
    .replace("now()", "CURRENT_TIMESTAMP")
)

#: One parent per rail purchase. This is the index every healer's ON CONFLICT names.
_CREATE_RAIL_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_purchases_rail_purchase
    ON agent_purchases (rail, rail_purchase_id)
"""

#: The owner's history, newest first.
_CREATE_OWNER_INDEX = """
CREATE INDEX IF NOT EXISTS idx_agent_purchases_owner
    ON agent_purchases (agent_id, agent_user_ref_hash, created_at)
"""


async def ensure_agent_purchase_schema() -> None:
    """Self-heal parity with migration 263. Idempotent; called at every startup."""
    await database.execute(_CREATE_TABLE_PG if IS_POSTGRES else _CREATE_TABLE_SQLITE)
    await database.execute(_CREATE_RAIL_INDEX)
    await database.execute(_CREATE_OWNER_INDEX)


# ── writes ───────────────────────────────────────────────────────────────────────────────────


def new_purchase_id() -> str:
    return f"pp_{uuid.uuid4().hex[:24]}"


def _routing_plan(basis: str) -> str:
    """How this purchase came to be on its rail. With one rail there is no choice to record, so
    this says who decided: the agent called the Reap route itself, or the row predates the
    ledger and was backfilled. The router writes ranked candidates here once a second rail
    exists."""
    return json.dumps(
        {"rail": "reap", "executor": "rail_managed", "basis": basis},
        separators=(",", ":"),
        sort_keys=True,
    )


#: Identity is COPIED from the committed child, never passed in: the parent cannot disagree with
#: the child about who owns the purchase or when it opened. No child row -> nothing inserted.
_INSERT_REAP_PARENT_SQL = """
    INSERT INTO agent_purchases (
        id, rail, executor, rail_purchase_id, agent_id, agent_user_ref_hash, routing_plan,
        created_at
    )
    SELECT :id, 'reap', 'rail_managed', r.id, r.agent_id, r.agent_user_ref_hash,
           CAST(:routing_plan AS JSONB), r.created_at
      FROM reap_agentic_purchases r
     WHERE r.id = :rail_purchase_id
    ON CONFLICT (rail, rail_purchase_id) DO NOTHING
"""
_INSERT_REAP_PARENT_SQL_SQLITE = _INSERT_REAP_PARENT_SQL.replace(
    "CAST(:routing_plan AS JSONB)", ":routing_plan"
)

_SELECT_PARENT_ID_SQL = """
    SELECT id FROM agent_purchases WHERE rail = :rail AND rail_purchase_id = :rail_purchase_id
"""


async def ensure_reap_parent(rail_purchase_id: str, *, basis: str = "agent_selected_rail") -> Optional[str]:
    """The parent id for a Reap purchase, writing it if absent. None when no such Reap row exists.

    Idempotent and race-safe: the INSERT is ON CONFLICT DO NOTHING on (rail, rail_purchase_id),
    and the id is read back afterwards, so a concurrent writer's row is the answer either way.
    """
    rail_purchase_id = str(rail_purchase_id or "").strip()
    if not rail_purchase_id:
        return None
    await database.execute(
        _INSERT_REAP_PARENT_SQL if IS_POSTGRES else _INSERT_REAP_PARENT_SQL_SQLITE,
        {
            "id": new_purchase_id(),
            "routing_plan": _routing_plan(basis),
            "rail_purchase_id": rail_purchase_id,
        },
    )
    row = await database.fetch_one(
        _SELECT_PARENT_ID_SQL, {"rail": "reap", "rail_purchase_id": rail_purchase_id}
    )
    return str(row["id"]) if row else None


_SELECT_REAP_MISSING_PARENTS_SQL = """
    SELECT r.id
      FROM reap_agentic_purchases r
     WHERE NOT EXISTS (
            SELECT 1 FROM agent_purchases p
             WHERE p.rail = 'reap' AND p.rail_purchase_id = r.id
           )
     ORDER BY r.created_at, r.id
     LIMIT :limit
"""

_SELECT_REAP_MISSING_PARENTS_FOR_OWNER_SQL = """
    SELECT r.id
      FROM reap_agentic_purchases r
     WHERE r.agent_id = :agent_id
       AND r.agent_user_ref_hash = :agent_user_ref_hash
       AND NOT EXISTS (
            SELECT 1 FROM agent_purchases p
             WHERE p.rail = 'reap' AND p.rail_purchase_id = r.id
           )
     ORDER BY r.created_at, r.id
     LIMIT :limit
"""


def _clamp(limit: Any, *, maximum: int) -> int:
    return max(1, min(maximum, int(limit)))


async def backfill_reap_parents(*, limit: int = 500) -> int:
    """Give up to `limit` parentless Reap purchases a parent, oldest first. Returns how many it
    examined; each of them has a parent afterwards (this call's, or a concurrent healer's).
    Run until it returns 0."""
    rows = await database.fetch_all(
        _SELECT_REAP_MISSING_PARENTS_SQL, {"limit": _clamp(limit, maximum=5000)}
    )
    for row in rows:
        await ensure_reap_parent(row["id"], basis="backfill")
    return len(rows)


async def heal_reap_parents_for_owner(
    agent_id: str, agent_user_ref_hash: str, *, limit: int = 100
) -> int:
    """The same backfill, scoped to one owner, so their history is complete the first time the
    unified list is read rather than after the next backfill run."""
    rows = await database.fetch_all(
        _SELECT_REAP_MISSING_PARENTS_FOR_OWNER_SQL,
        {
            "agent_id": agent_id,
            "agent_user_ref_hash": agent_user_ref_hash,
            "limit": _clamp(limit, maximum=500),
        },
    )
    for row in rows:
        await ensure_reap_parent(row["id"], basis="backfill")
    return len(rows)


# ── owner-scoped reads ───────────────────────────────────────────────────────────────────────
#
# BOTH OWNER CONJUNCTS IN THE SQL, as on the Reap ledger: "not yours" and "does not exist" are
# the same None from the same statement, so these reads cannot be used to probe for ids.

# Plain literals, not f-strings: the PREPARE gate (tests/test_repo_sql_prepare_postgres.py) only
# resolves a statement bound to a string LITERAL, so a composed one would ship unplanned.
_SELECT_FOR_OWNER_SQL = """
    SELECT id, rail, executor, rail_purchase_id, routing_plan, created_at
      FROM agent_purchases
     WHERE id = :id
       AND agent_id = :agent_id
       AND agent_user_ref_hash = :agent_user_ref_hash
"""

_SELECT_BY_RAIL_ID_FOR_OWNER_SQL = """
    SELECT id, rail, executor, rail_purchase_id, routing_plan, created_at
      FROM agent_purchases
     WHERE rail = :rail
       AND rail_purchase_id = :rail_purchase_id
       AND agent_id = :agent_id
       AND agent_user_ref_hash = :agent_user_ref_hash
"""

_LIST_FOR_OWNER_SQL = """
    SELECT id, rail, executor, rail_purchase_id, routing_plan, created_at
      FROM agent_purchases
     WHERE agent_id = :agent_id
       AND agent_user_ref_hash = :agent_user_ref_hash
     ORDER BY created_at DESC, id DESC
     LIMIT :limit OFFSET :offset
"""


def _parent(row: Any) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    out = dict(row)
    out["routing_plan"] = _decode_json(out.get("routing_plan"))
    out["created_at"] = _decode_dt(out.get("created_at"))
    return out


async def get_for_owner(
    purchase_id: str, agent_id: str, agent_user_ref_hash: str
) -> Optional[Dict[str, Any]]:
    return _parent(
        await database.fetch_one(
            _SELECT_FOR_OWNER_SQL,
            {"id": purchase_id, "agent_id": agent_id, "agent_user_ref_hash": agent_user_ref_hash},
        )
    )


async def get_by_rail_id_for_owner(
    rail: str, rail_purchase_id: str, agent_id: str, agent_user_ref_hash: str
) -> Optional[Dict[str, Any]]:
    return _parent(
        await database.fetch_one(
            _SELECT_BY_RAIL_ID_FOR_OWNER_SQL,
            {
                "rail": rail,
                "rail_purchase_id": rail_purchase_id,
                "agent_id": agent_id,
                "agent_user_ref_hash": agent_user_ref_hash,
            },
        )
    )


async def list_for_owner(
    agent_id: str, agent_user_ref_hash: str, *, limit: int = 20, offset: int = 0
) -> List[Dict[str, Any]]:
    rows = await database.fetch_all(
        _LIST_FOR_OWNER_SQL,
        {
            "agent_id": agent_id,
            "agent_user_ref_hash": agent_user_ref_hash,
            "limit": _clamp(limit, maximum=500),
            "offset": max(0, int(offset)),
        },
    )
    return [p for p in (_parent(r) for r in rows) if p is not None]
