-- Older code never names these columns; the legacy columns it reads are written unchanged, so a
-- runtime rollback can leave them in place. Drop them only with the writers' flag off.
ALTER TABLE IF EXISTS agent_pdp_view
    DROP COLUMN IF EXISTS market_prices_refreshed_at,
    DROP COLUMN IF EXISTS market_prices;
