"""Stage 3a-ii backfill — (re)materialize agent_pdp_view rows.

Stage 3a-i (migration 085) added the denormalized agent_pdp_view table; this
script seeds and re-seeds it from catalog_products × catalog_skus ×
catalog_offers × product_group_members × external_product_seeds, plus the
evidence / enrichment / seller-trust overlays.

🚨 THIS SCRIPT USED TO DESTROY THE OVERLAYS IT WAS POINTED AT.

It inlined its own fetch→assemble→upsert, calling `assemble_row` with neither
`evidence=` nor `enrichment=` nor `seller_trust_by_id=`. Those default to None,
and UPSERT_SQL assigns EVERY column from EXCLUDED on conflict — including
`description`, `bullet_points`, `usage_scenarios`, `evidence_profile` and
`required_disclaimers`. So an `--apply` run did not merely fail to add
enrichment: it NULLed the enrichment, substantiated claims and disclaimers on
every row it touched that already had them. Pointing it at the ~138 enriched
rows stranded before the SERVE_PDP_ENRICHMENT_ON_WRITE flip would have wiped
the ~217 that were already serving.

It now delegates to `refresh_agent_pdp_view_for_content_key` /
`build_agent_pdp_view_row` — the canonical path that fetches all three overlays
— and no longer owns any assembly of its own. Do not reintroduce a local
`assemble_row` call here; see the warning on `build_agent_pdp_view_row`.

The grouping model and tiebreak ladder are documented in
services/agent_pdp_view_assembler.py.

Mock/synthetic boundary (memory: feedback_mock_data_never_to_merchant): the
assembler never synthesizes description prose; this script never backfills rows
with fabricated content. Every field originates in a primary catalog table or
external_product_seeds.seed_data (employee-authored bootstrap data — memory:
project_pivota_external_seed_bootstrap).

Usage
-----
Dry-run over the enriched cohort (what workstream 1 wants):
  python3 scripts/backfill_agent_pdp_view.py --scope enriched --limit 0

Apply it:
  python3 scripts/backfill_agent_pdp_view.py --scope enriched --limit 0 --apply

Dry-run / apply over the whole corpus, paginated:
  python3 scripts/backfill_agent_pdp_view.py --limit 200 --offset 0
  python3 scripts/backfill_agent_pdp_view.py --apply --limit 200 --offset 0

Fill agent_pdp_view.market_prices (migration 263) on the rows whose summary is
missing, stale (a later write without the flag moved refreshed_at past its
stamp) or of another version. Requires AGENT_PDP_VIEW_MARKET_PRICES=on in the
job's env (the script refuses otherwise: it would rewrite rows without the
summary). The summary is keyed by currency, so it does not depend on the job's
PIVOTA_SERVING_PRICING_REGIONS. Keyset-paged on content_key (--offset is not
used: the --apply set shrinks as it fills); pass the report's `next_after` back
as --after until `content_keys_considered` is 0. --sleep throttles the writes
for the 2-vCPU primary; a row that fails is counted and the page continues:
  AGENT_PDP_VIEW_MARKET_PRICES=on python3 scripts/backfill_agent_pdp_view.py \
      --scope market_prices_missing --limit 500 --sleep 0.05 --apply [--after <content_key>]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import database  # noqa: E402
from services.agent_pdp_view_assembler import (  # noqa: E402
    BACKFILL_REFRESH_SOURCE,
    MARKET_PRICES_VERSION,
    build_agent_pdp_view_row,
    execute_agent_pdp_view_upsert,
    market_prices_enabled,
)

logger = logging.getLogger("backfill_agent_pdp_view")

# Distinct refresh_source for the enriched-cohort pass so the audit trail can
# tell "propagated the stranded enrichment" from a generic re-seed.
ENRICHED_REFRESH_SOURCE = "backfill_enrichment_propagation"

# Every content_key, ordered so each --limit/--offset window is a disjoint slice.
_ALL_KEYS_SQL = """
    SELECT DISTINCT content_key
    FROM catalog_products
    WHERE content_key IS NOT NULL
    ORDER BY content_key ASC
"""

# content_keys carrying a product_enrichment overlay. product_enrichment is keyed
# by the enrichment domain's spelling of the catalog identity triple —
# platform_product_id there IS source_product_id on catalog_products. Getting
# that mapping wrong is what killed the on-write publish bridge; see the comment
# in refresh_agent_pdp_view_for_enrichment_write.
_ENRICHED_KEYS_SQL = """
    SELECT DISTINCT cp.content_key
    FROM product_enrichment pe
    JOIN catalog_products cp
      ON cp.merchant_id = pe.merchant_id
     AND cp.platform = pe.platform
     AND cp.source_product_id = pe.platform_product_id
    WHERE cp.content_key IS NOT NULL
    ORDER BY cp.content_key ASC
"""

# Rows whose summary readers would not use: never computed, stale (its stamp is
# not the row's refreshed_at), or another version. The negation of
# market_prices_fresh_sql's test, so this finds exactly what readers ignore.
# Keyset on content_key: the PK index serves the order and the `> :after` seek.
_MARKET_PRICES_MISSING_KEYS_SQL = f"""
    SELECT content_key
    FROM agent_pdp_view
    WHERE (market_prices IS NULL
           OR market_prices_refreshed_at IS DISTINCT FROM refreshed_at
           OR market_prices->>'version' IS DISTINCT FROM '{MARKET_PRICES_VERSION}')
      AND content_key > :after
    ORDER BY content_key ASC
"""


def build_content_key_query(
    *, scope: str, limit: int, offset: int, after: str = ""
) -> Tuple[str, Dict[str, Any]]:
    """(sql, params) for the content_key window. A pure builder so the driven
    PREPARE gate can plan every shape it emits against real Postgres — the
    assembled string lives in a function local, which the static sweep in
    tests/test_repo_sql_prepare_postgres.py cannot follow."""
    params: Dict[str, Any] = {}
    keyset = scope == "market_prices_missing"
    if keyset:
        sql = _MARKET_PRICES_MISSING_KEYS_SQL
        params["after"] = str(after or "")
    else:
        sql = _ENRICHED_KEYS_SQL if scope == "enriched" else _ALL_KEYS_SQL
    if limit > 0:
        sql += "\n        LIMIT :limit"
        params["limit"] = int(limit)
    # Keyset pages never take an OFFSET: it would skip rows the shrinking set moved up.
    if offset > 0 and not keyset:
        sql += "\n        OFFSET :offset"
        params["offset"] = int(offset)
    return sql, params


async def _fetch_content_keys(
    *, scope: str, limit: int, offset: int, after: str = ""
) -> List[str]:
    """Stable content_key window. Paged by content_key ASC so each chunk is a
    disjoint slice — no double-writes, safe to resume on partial failures.
    """
    sql, params = build_content_key_query(scope=scope, limit=limit, offset=offset, after=after)
    rows = await database.fetch_all(sql, params)
    return [r["content_key"] for r in rows or []]


# What the SERVED row currently carries. The cohort query admits a content_key
# when ANY cluster member has an overlay, but `_fetch_enrichment_for_canonical`
# publishes only the CANONICAL member's (or a brand-attested one). For a cluster
# whose overlay sits on a non-canonical, non-attested member, the assembled row
# therefore carries no overlay — and writing it would REMOVE one that is
# currently serving, published back when that member was canonical. This job
# exists to add stranded enrichment; removing any is out of its contract, so
# such keys are skipped and counted rather than written.
_CURRENT_OVERLAY_SQL = """
    SELECT (bullet_points IS NOT NULL) AS has_bullet_points,
           (usage_scenarios IS NOT NULL) AS has_usage_scenarios,
           (evidence_profile IS NOT NULL) AS has_evidence_profile
    FROM agent_pdp_view
    WHERE content_key = :ck
"""


def _would_downgrade(row: Dict[str, Any], current: Optional[Dict[str, Any]]) -> bool:
    """True when writing `row` would clear an overlay the served row still has.

    `preserve_*` is checked first: when the overlay READ failed, the UPSERT keeps
    the published value, so an empty column in the assembled row is not a removal
    and must not be reported as one.
    """
    if not current:
        return False
    if not row.get("preserve_enrichment"):
        if current.get("has_bullet_points") and not row.get("bullet_points"):
            return True
        if current.get("has_usage_scenarios") and not row.get("usage_scenarios"):
            return True
    if not row.get("preserve_evidence"):
        if current.get("has_evidence_profile") and not row.get("evidence_profile"):
            return True
    return False


def _overlay_flags(row: Dict[str, Any]) -> Dict[str, bool]:
    """Which overlays the freshly-assembled row carries. Reported in dry-run so
    the operator can see the propagation BEFORE writing, and counted on apply."""
    return {
        "bullet_points": bool(row.get("bullet_points")),
        "usage_scenarios": bool(row.get("usage_scenarios")),
        "evidence_profile": bool(row.get("evidence_profile")),
        "required_disclaimers": bool(row.get("required_disclaimers")),
    }


# ---------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------

async def _drive(args: argparse.Namespace) -> Dict[str, Any]:
    if args.scope == "market_prices_missing" and not market_prices_enabled():
        # Without the flag the assembled row carries no market_prices, the
        # upsert leaves the column NULL, and the next pass selects the same keys.
        raise SystemExit(
            "--scope market_prices_missing needs AGENT_PDP_VIEW_MARKET_PRICES=on"
        )
    if not getattr(database, "is_connected", False):
        await database.connect()

    refresh_source = (
        ENRICHED_REFRESH_SOURCE if args.scope == "enriched" else BACKFILL_REFRESH_SOURCE
    )
    content_keys = await _fetch_content_keys(
        scope=args.scope, limit=args.limit, offset=args.offset,
        after=getattr(args, "after", "") or "",
    )
    logger.info(
        "loaded %d content_keys (scope=%s limit=%d offset=%d)",
        len(content_keys), args.scope, args.limit, args.offset,
    )

    outcomes: Dict[str, int] = {
        "content_keys_considered": len(content_keys),
        "rows_failed": 0,
        "rows_assembled": 0,
        "rows_skipped_nothing_to_build": 0,
        "rows_upserted": 0,
        "rows_skipped_no_op_in_dry_run": 0,
        "rows_skipped_would_downgrade": 0,
        "with_bullet_points": 0,
        "with_usage_scenarios": 0,
        "with_evidence_profile": 0,
        "with_required_disclaimers": 0,
    }
    samples: List[Dict[str, Any]] = []
    downgrades: List[str] = []
    failures: List[Dict[str, str]] = []
    sleep_s = max(0.0, float(getattr(args, "sleep", 0.0) or 0.0))

    for ck in content_keys:
        # One row's failure must not lose the page: it is counted and sampled, and
        # next_after still moves past it.
        try:
            await _process_key(
                ck, args=args, refresh_source=refresh_source, outcomes=outcomes,
                samples=samples, downgrades=downgrades,
            )
        except Exception as exc:  # noqa: BLE001
            outcomes["rows_failed"] += 1
            if len(failures) < 20:
                failures.append({"content_key": ck, "error": str(exc)[:200]})
            logger.warning("backfill row failed content_key=%s: %s", ck, str(exc)[:200])
        if sleep_s and args.apply:
            await asyncio.sleep(sleep_s)

    return {
        "scope": args.scope,
        "refresh_source": refresh_source,
        "applied": bool(args.apply),
        "outcome_counts": outcomes,
        "samples": samples,
        "skipped_would_downgrade_sample": downgrades,
        "failed_sample": failures,
        # Keyset cursor for --scope market_prices_missing: rows this pass skipped
        # or failed stay selected, so the next pass must start after them.
        "next_after": content_keys[-1] if content_keys else None,
    }


async def _process_key(
    ck: str,
    *,
    args: argparse.Namespace,
    refresh_source: str,
    outcomes: Dict[str, int],
    samples: List[Dict[str, Any]],
    downgrades: List[str],
) -> None:
    # Assemble through the canonical read path so the evidence, enrichment
    # and seller-trust overlays are attached. Dry-run stops here; apply
    # persists exactly this row.
    row = await build_agent_pdp_view_row(ck, refresh_source=refresh_source)
    if row is None:
        outcomes["rows_skipped_nothing_to_build"] += 1
        return
    outcomes["rows_assembled"] += 1

    current = await database.fetch_one(_CURRENT_OVERLAY_SQL, {"ck": ck})
    if _would_downgrade(row, dict(current) if current else None):
        # Reported in BOTH modes, and skipped in both: a dry run that does
        # not surface this would send an operator into --apply believing the
        # run can only add.
        outcomes["rows_skipped_would_downgrade"] += 1
        if len(downgrades) < 20:
            downgrades.append(ck)
        return

    flags = _overlay_flags(row)
    for name, present in flags.items():
        if present:
            outcomes[f"with_{name}"] += 1

    if len(samples) < 5:
        samples.append({
            "content_key": ck,
            "title": row["title"],
            "brand": row["brand"],
            "offer_count": row["offer_count"],
            "variants_count": row["variants_count"],
            "primary_merchant_id": row["primary_merchant_id"],
            **flags,
        })

    if not args.apply:
        outcomes["rows_skipped_no_op_in_dry_run"] += 1
        return
    await execute_agent_pdp_view_upsert(database, row)
    outcomes["rows_upserted"] += 1


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--apply", action="store_true",
        help="Actually UPSERT agent_pdp_view rows. Default: dry-run.",
    )
    p.add_argument(
        "--scope", choices=("all", "enriched", "market_prices_missing"), default="all",
        help=(
            "'all' = every content_key (default). 'enriched' = only the "
            "content_keys carrying a product_enrichment overlay — the cohort "
            "stranded before the SERVE_PDP_ENRICHMENT_ON_WRITE flip. "
            "'market_prices_missing' = served rows whose market_prices is NULL "
            "(migration 263; needs AGENT_PDP_VIEW_MARKET_PRICES=on; page with --after)."
        ),
    )
    p.add_argument(
        "--after", default="",
        help="market_prices_missing only: start after this content_key (keyset page).",
    )
    p.add_argument(
        "--sleep", type=float, default=0.0,
        help="Seconds to sleep after each row of an --apply run, to keep the primary's CPU down.",
    )
    p.add_argument(
        "--limit", type=int, default=200,
        help="Max content_keys to process this run (0 = all). Default 200.",
    )
    p.add_argument(
        "--offset", type=int, default=0,
        help="Skip the first N content_keys. Use to paginate across chunks.",
    )
    return p.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args()
    report = asyncio.run(_drive(args))
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
