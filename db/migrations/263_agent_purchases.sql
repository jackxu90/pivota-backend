-- 263: THE RAIL-NEUTRAL PURCHASE LEDGER (payment orchestration P0).
--
-- One row per purchase on ANY payment rail, pointing at that rail's own purchase row. Today the
-- only rail is Reap (reap_agentic_purchases, executor rail_managed: the rail places the order);
-- the next rail adds a value to each CHECK and its own child table.
--
-- NO STATE COLUMN, ON PURPOSE. The rail's row is the state machine and has many writers; this
-- driver cannot hold a transaction across a parent and a child write, so a copied state would
-- drift. Status is always read from the child (services/payment_orchestration/rails.py maps it
-- to the shared vocabulary). Everything here is fixed when the purchase opens.
--
-- IDENTITY IS COPIED FROM THE CHILD, never passed in: db/agent_purchase_ledger.ensure_reap_parent
-- is one INSERT ... SELECT from the committed Reap row, ON CONFLICT (rail, rail_purchase_id) DO
-- NOTHING, so any number of concurrent writers and healers leave exactly one parent.
--
-- No foreign key to the child, for the same reason reap_agentic_purchases.enrollment_id has none:
-- the child table differs per rail.
--
-- Production startup runs in fast mode and skips db/migrations/; the same DDL is in
-- db/agent_purchase_ledger._CREATE_TABLE_PG, run by db/schema_guard.ensure_required_schema_light.
-- tests/test_agent_purchase_ledger_postgres.py proves the two build the same schema.

CREATE TABLE IF NOT EXISTS agent_purchases (
    id VARCHAR(64) PRIMARY KEY,
    rail VARCHAR(16) NOT NULL CHECK (rail IN ('reap')),
    executor VARCHAR(24) NOT NULL CHECK (executor IN ('rail_managed')),
    rail_purchase_id VARCHAR(64) NOT NULL,
    agent_id VARCHAR(128),
    agent_user_ref_hash VARCHAR(64),
    routing_plan JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_purchases_rail_purchase
    ON agent_purchases (rail, rail_purchase_id);

CREATE INDEX IF NOT EXISTS idx_agent_purchases_owner
    ON agent_purchases (agent_id, agent_user_ref_hash, created_at);
