"""Payment orchestration: one purchase vocabulary across payment rails.

P0 is the rail-neutral ledger (db/agent_purchase_ledger.py) and the unified read
(routes/agent_commerce_purchases.py), with Reap as the only rail.
"""
