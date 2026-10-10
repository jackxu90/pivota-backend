-- Down for 263: drop the rail-neutral purchase ledger. The rail tables are untouched;
-- every row here is derivable from them again (db/agent_purchase_ledger.backfill_reap_parents).
DROP INDEX IF EXISTS idx_agent_purchases_owner;
DROP INDEX IF EXISTS uq_agent_purchases_rail_purchase;
DROP TABLE IF EXISTS agent_purchases;
