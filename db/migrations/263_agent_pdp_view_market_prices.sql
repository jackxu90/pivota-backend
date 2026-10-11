-- 263: agent_pdp_view.market_prices -- the price summary PER CURRENCY (2026-10-10).
--
-- The row-level currency / price_min / price_max are ONE currency, the modal one (ties to the
-- higher code). A product can carry offers in several served currencies (retailer_ingest
-- shopify_markets writes USD siblings beside a non-USD base offer today, and SGD siblings beside
-- USD ones once #2553 lands); one with USD and SGD offers reads as a USD product, and every
-- surface serving an SG buyer from those columns drops it. The top-5 `offers` cut also sorts raw prices across currencies,
-- so the SGD offers can be cut from the row entirely.
--
-- market_prices (written by services/agent_pdp_view_assembler.build_market_prices):
--   {"version": 1,
--    "currencies": {"USD": {"price_min": n, "price_max": n, "offer_count": n, "offers": [...]},
--                   "SGD": {...}}}
-- one entry per currency the offers are priced in, never converted, independent of any
-- process's served-region env: readers map their buyer's market to a currency.
--
-- market_prices_refreshed_at: the NOW() of the write that produced the summary, the same value
-- that write gives refreshed_at. A later write that rewrites offers and refreshed_at without the
-- summary (a writer with the flag off, an old image, a repair script) leaves the two unequal, and
-- readers then ignore the summary (agent_pdp_view_assembler.market_prices_fresh_sql) and use the
-- legacy columns, which are unchanged.
--
-- NULLABLE, no default, no index: read only off a row already found by its key, written only
-- while AGENT_PDP_VIEW_MARKET_PRICES is on. Production deploys skip db/migrations/, so
-- db/schema_guard.py adds the same columns at boot. Fill: scripts/backfill_agent_pdp_view.py
-- --scope market_prices_missing.

ALTER TABLE IF EXISTS agent_pdp_view
    ADD COLUMN IF NOT EXISTS market_prices JSONB,
    ADD COLUMN IF NOT EXISTS market_prices_refreshed_at TIMESTAMPTZ;
