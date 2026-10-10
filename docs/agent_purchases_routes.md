# `/agent/v2/commerce/purchases`: the rail-neutral purchase read

`routes/agent_commerce_purchases.py` implements this contract. The storage is
`db/agent_purchase_ledger.py` (migration 263); the shared state vocabulary is
`services/payment_orchestration/rails.py`.

**Payment orchestration P0.** One purchase id (`pp_…`) and one state vocabulary across payment
rails. Reap is the only rail today; more rails follow. The routes are **read-only**: they
open nothing, advance nothing and call no rail. Purchases are still created through
`POST /agent/v2/commerce/reap/purchases` (docs/reap_agentic_routes.md).

**Dark by default.** While `AGENT_PURCHASE_LEDGER_ENABLED` is not one of `1/true/on/yes`, every
request answers 404 `not_available`, whatever its shape. The dial is read per request.

## Authentication

The same two headers as the Reap routes: `X-API-Key` (the agent) and `X-Agent-User-JWT` (the
buyer). Missing buyer: 401 `agent_user_required`. Every read is scoped to the agent **and** the
buyer, in SQL; another owner's purchase and a purchase that does not exist are the same 404
`purchase_not_found`.

Errors arrive in the app-wide envelope; read the reason at `detail.error`.

## `GET /agent/v2/commerce/purchases/{purchase_id}`

`purchase_id` is a `pp_<24 hex>` id, or a Reap `rp_<24 hex>` id (a Reap purchase opened before the
ledger existed gets its `pp_` id on this first read). Any other shape is 404 `purchase_not_found`,
decided before any database read.

```json
{
  "purchase_id": "pp_…",
  "rail": "reap",
  "executor": "rail_managed",
  "rail_purchase_id": "rp_…",
  "state": "awaiting_buyer_authorization",
  "rail_state": "awaiting_approval",
  "created_at": "2026-10-10T11:00:00+00:00",
  "totals": {"currency": "USD", "quoted_total_minor": 4650, "…": "…"},
  "poll_after_seconds": 30,
  "next_action": {
    "type": "open_url",
    "kind": "approval",
    "url": "https://…",
    "expires_at": "2026-10-10T11:15:00+00:00"
  },
  "detail": {"…": "the rail's own owner-facing body, byte for byte"}
}
```

- `state`: one of `routing`, `needs_payment_method`, `locking`, `awaiting_buyer_authorization`,
  `placing`, `completed`, `failed`, `refused`, `expired`. The last four are terminal.
- `rail_state`: the rail's own word for the same state; for Reap, see docs/reap_agentic_routes.md.
- `next_action`: present only while the buyer must act on a rail-hosted page AND the rail route
  still offers a valid URL (`kind` is `card_binding` or `approval`). Absent means there is
  nowhere to send the buyer: poll again.
- `order_reference`: present on `completed`; the merchant order reference from the rail.
- `detail`: exactly what `GET /agent/v2/commerce/reap/purchases/{rp_id}` returns.
- 503 `state_unmapped`: the rail reported a state this service has no unified word for. Do not
  infer anything; retry later.
- 503 `ledger_unavailable`: the ledger could not be read. Says nothing about the purchase; retry.
- Reads do not depend on the Reap rail's own dial: like Reap's GET, a stored purchase stays
  readable while the rail is switched off.

## `GET /agent/v2/commerce/purchases?limit=N`

The caller's purchases, newest first, each in the shape above. `limit` is 1..100 (default 20);
out of range is 400 `invalid_request`, never a silent clamp. The page is all-or-nothing: if any
purchase on it is in a state with no unified word, the whole list answers 503 `state_unmapped`
rather than leaving that purchase out (a missing row would read as a purchase that does not exist).

```json
{"purchases": [{"purchase_id": "pp_…", "…": "…"}], "limit": 20}
```

## Operations

- Turn on, in this order:
  1. Backfill while the dial is still off (the table exists from the self-heal; the backfill is
     idempotent and safe beside live traffic): `python scripts/backfill_agent_purchases.py --dry-run`,
     then without `--dry-run`.
  2. Set `AGENT_PURCHASE_LEDGER_ENABLED=1`. From then on the Reap create route writes each new
     purchase's parent after its own commit (best-effort, at most 1 s; a failure or a timeout is
     logged and never affects the purchase).
  3. Run the backfill once more for purchases opened between steps 1 and 2.
  Reads also heal: a single read heals that purchase, and a list read heals that owner's newest 100.
- Turn off: unset the dial. The table stays; nothing reads or writes it.
