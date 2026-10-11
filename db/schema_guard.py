from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Set

from db.database import IS_POSTGRES, IS_SQLITE, database

logger = logging.getLogger(__name__)


def text(sql: str) -> str:
    """Return raw SQL for the databases adapter.

    Railway Postgres uses the `databases` driver path, where SQLAlchemy
    TextClause values can fail before DDL runs. Keep the existing guard call
    shape while ensuring startup self-heal statements execute as raw SQL.
    """
    return sql


# The column heals below are ALTERs whose only clauses are column adds that
# carry IF NOT EXISTS. Those words do not make the statement cheap: Postgres
# takes the table's ACCESS EXCLUSIVE lock BEFORE it finds the column already
# there, so run bare, every boot queued behind any open transaction on the table
# and every later reader and writer queued behind the boot (orders: every
# checkout). `_heal_add_columns` runs each ALTER only while one of its columns
# is missing, and then with a transaction-local lock_timeout, so a busy table
# costs the heal one retry next boot instead of stalling the table.
HEAL_LOCK_TIMEOUT = "500ms"

_HEAL_ALTER_RE = re.compile(
    r"ALTER\s+TABLE\s+(IF\s+EXISTS\s+)?(\w+)\s+(.*?);", re.IGNORECASE | re.DOTALL
)
_HEAL_CLAUSE_RE = re.compile(r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+(\w+)\s", re.IGNORECASE)


def _top_level_clauses(body: str) -> List[str]:
    """Split an ALTER body on the commas that separate its clauses (not the
    ones inside a type modifier, an expression or a quoted literal)."""
    clauses: List[str] = []
    depth, quoted, start = 0, False, 0
    for i, ch in enumerate(body):
        if ch == "'":
            quoted = not quoted
        elif quoted:
            continue
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            clauses.append(body[start:i])
            start = i + 1
    clauses.append(body[start:])
    return [c.strip() for c in clauses]


def guarded_add_columns(sql: str) -> List[str]:
    """Return one guarded DO block per ALTER statement in `sql`.

    Each ALTER is kept verbatim (the schema-guard-coverage gate reads this
    file's source for its ADD COLUMN clauses) and runs only while its table
    exists and at least one of the columns it adds does not. When one is
    missing the whole ALTER runs, as it did bare; the columns already there
    no-op on their IF NOT EXISTS.

    Presence is read from pg_attribute for to_regclass(<table>), which resolves
    the name through search_path exactly as the ALTER does, and reads the
    catalog without taking a lock on the table.

    `ALTER TABLE <t>` without IF EXISTS (the request-path self-heals) keeps its
    raise on a missing table: there no pg_attribute row matches, so the ALTER
    runs and fails with UndefinedTable exactly as it did bare.

    Raises ValueError for any statement that is not purely column adds, so
    the guard cannot swallow a clause that has to run on every boot.
    """
    statements: List[str] = []
    sql = sql.strip()
    if not sql.endswith(";"):
        sql += ";"
    rest = sql
    for match in _HEAL_ALTER_RE.finditer(sql):
        if_exists, table, body = match.group(1), match.group(2).lower(), match.group(3)
        # Every clause must be a column add with IF NOT EXISTS. Anything else
        # (ALTER COLUMN, ADD CONSTRAINT, DROP ...) has to run on a boot where
        # the columns exist too, which this guard would silently stop.
        columns: List[str] = []
        for clause in _top_level_clauses(re.sub(r"--[^\n]*", "", body)):
            added = _HEAL_CLAUSE_RE.match(clause + " ")
            if added is None or "$" in clause:
                raise ValueError(f"heal for {table} is not only column adds: {clause[:60]!r}")
            columns.append(added.group(1).lower())
        wanted = ", ".join(f"'{c}'" for c in columns)
        statements.append(
            f"""
            DO $heal$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM unnest(ARRAY[{wanted}]::text[]) AS wanted(column_name)
                    WHERE {"to_regclass('" + table + "') IS NOT NULL AND" if if_exists else ""}
                      NOT EXISTS (
                          SELECT 1 FROM pg_attribute
                          WHERE attrelid = to_regclass('{table}')
                            AND attname = wanted.column_name
                            AND attnum > 0
                            AND NOT attisdropped
                      )
                ) THEN
                    PERFORM set_config('lock_timeout', '{HEAL_LOCK_TIMEOUT}', true);
                    {match.group(0).strip()}
                END IF;
            END $heal$;
            """
        )
        rest = rest.replace(match.group(0), "", 1)
    if not statements or rest.strip():
        raise ValueError(f"not a column heal: {sql.strip()[:80]!r}")
    return statements


async def _heal_add_columns(sql: str) -> None:
    """Run the guarded form of a column heal, each ALTER in its own try.

    Best-effort like every heal in ensure_required_schema_light, but one that
    fails (a lock timeout on a busy table included) must not abandon the
    statements after it, and is logged so a column that keeps missing its
    heal is visible.
    """
    for statement in guarded_add_columns(sql):
        try:
            await database.execute(text(statement))
        except Exception as exc:  # noqa: BLE001
            logger.warning("schema guard: column heal deferred to next boot: %s", exc)


# The same hazard, for the heals that are not column adds: ALTER COLUMN ... TYPE,
# ALTER COLUMN ... DROP NOT NULL, and the DROP + ADD CONSTRAINT pairs that re-widen
# a CHECK. Each takes ACCESS EXCLUSIVE, and each used to run bare on every boot
# although it changes nothing after its first run. `guarded_ddl` runs one only
# while the catalog says it is still needed, and then with the same lock_timeout.


def guarded_ddl(table: str, needed: str, ddl: str) -> str:
    """Return a DO block that runs `ddl` only while `table` exists and `needed`
    (a SQL boolean over the catalog) is true, under a transaction-local
    lock_timeout.

    `ddl` is kept verbatim and may be several statements: they run in one
    transaction, so a DROP + ADD CONSTRAINT pair is never seen half-done and
    takes the table's lock once.
    """
    if "$guard$" in ddl or "$guard$" in needed:
        raise ValueError("guarded_ddl: the statement carries the block's own quote tag")
    return f"""
        DO $guard$
        BEGIN
            IF to_regclass('{table}') IS NOT NULL AND ({needed}) THEN
                PERFORM set_config('lock_timeout', '{HEAL_LOCK_TIMEOUT}', true);
                {ddl.strip()}
            END IF;
        END $guard$;
        """


def column_is_not_null(table: str, column: str) -> str:
    """True while `table`.`column` exists and is NOT NULL (its DROP NOT NULL is needed)."""
    return f"""EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = to_regclass('{table}')
                  AND attname = '{column}'
                  AND attnum > 0
                  AND NOT attisdropped
                  AND attnotnull
            )"""


def column_type_differs(table: str, columns: Sequence[str], sql_type: str) -> str:
    """True while any existing column of `columns` is not `sql_type`, spelled as
    format_type() spells it (e.g. 'character varying(128)')."""
    wanted = ", ".join(f"'{c}'" for c in columns)
    return f"""EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = to_regclass('{table}')
                  AND attname = ANY(ARRAY[{wanted}]::text[])
                  AND attnum > 0
                  AND NOT attisdropped
                  AND format_type(atttypid, atttypmod) <> '{sql_type}'
            )"""


def constraint_differs(table: str, name: str, definition: str) -> str:
    """True unless `table` holds a validated constraint `name` whose
    pg_get_constraintdef() is exactly `definition`.

    Exact, not a token match: the bare DROP + ADD this guards reset the CHECK to
    its one list on every boot, so any other list (narrower OR wider) is still a
    boot that has to run it. A definition Postgres renders differently from
    `definition` only costs the old behaviour, now bounded by the lock_timeout.
    (`convalidated` is belt and braces: pg_get_constraintdef() already appends
    NOT VALID to a check that is not validated, so the text would differ too.)
    """
    return f"""NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = to_regclass('{table}')
                  AND conname = '{name}'
                  AND convalidated
                  AND pg_get_constraintdef(oid) = $def${definition}$def$
            )"""


# Mig 232's verdict vocabulary. The heal's CHECK and the definition its guard
# compares against are both built from this one list, so they cannot drift apart.
_TIERB_VERDICTS = (
    "ELIGIBLE", "LOGIN_REQUIRED", "NOT_ACCEPTING_ORDERS", "VARIANT_GONE",
    "VARIANT_UNAVAILABLE", "PASSWORD_PAGE", "BLOCKED_UNKNOWN", "CHECKOUT_PREFILL_MISSING",
    "CHECKOUT_MARKET_MISMATCH", "NO_CARD_PAYMENT", "PRICE_DRIFT",
)


def _tierb_verdict_def(column: str) -> str:
    """pg_get_constraintdef() of mig 232's CHECK on the VARCHAR(32) `column`."""
    values = ", ".join(f"'{v}'::character varying" for v in _TIERB_VERDICTS)
    return (
        f"CHECK ((({column} IS NULL) OR (({column})::text = ANY "
        f"((ARRAY[{values}])::text[]))))"
    )


_PARTNER_SEND_LOG_TEMPLATE_DEF = (
    "CHECK ((template_id = ANY (ARRAY['settlement_monthly'::text, "
    "'settlement_skipped'::text, 'settlement_failed_notice'::text, "
    "'partner_invite'::text])))"
)


async def _heal_guarded(table: str, needed: str, ddl: str) -> None:
    """Run `guarded_ddl` in its own try, like `_heal_add_columns`: a heal that
    fails (a lock timeout on a busy table included) is retried next boot and must
    not abandon the statements after it."""
    try:
        await database.execute(text(guarded_ddl(table, needed, ddl)))
    except Exception as exc:  # noqa: BLE001
        logger.warning("schema guard: %s heal deferred to next boot: %s", table, exc)


# Mig 246's trigger, installed from the migration file itself: the statements that run on a prod
# boot are the ones reviewed in the .sql, never a copy that can drift from it. Needed while the
# column exists (the function reads it) and the trigger does not.
#
# FIRST INSTALL ONLY. The guard looks for the trigger by NAME, so an edit to 246's function or
# WHEN clause after a database has the trigger is not re-applied by a boot. Such a change ships
# as a new migration and a new guard that recognises the new definition.
PRICE_CHECK_MIGRATION = Path(__file__).resolve().parent / "migrations" / "246_catalog_offers_price_checked_at.sql"
PRICE_CHECK_TRIGGER = "trg_catalog_offers_forget_unread_price_check"
PRICE_CHECK_TRIGGER_NEEDED = f"""EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = to_regclass('catalog_offers')
                  AND attname = 'price_checked_at'
                  AND attnum > 0
                  AND NOT attisdropped
            ) AND NOT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgrelid = to_regclass('catalog_offers')
                  AND tgname = '{PRICE_CHECK_TRIGGER}'
                  AND NOT tgisinternal
            )"""
_PRICE_CHECK_TRIGGER_STATEMENTS = ("CREATE OR REPLACE FUNCTION", "DROP TRIGGER", "CREATE TRIGGER")


def price_check_trigger_ddl() -> str:
    """The function and trigger statements of migration 246, in file order, comments dropped."""
    from db.sql_migrations import split_statements

    picked = []
    for statement in split_statements(PRICE_CHECK_MIGRATION.read_text()):
        code = "\n".join(
            line for line in statement.splitlines() if not line.lstrip().startswith("--")
        ).strip().rstrip(";").strip()
        if code.upper().startswith(_PRICE_CHECK_TRIGGER_STATEMENTS):
            picked.append(code + ";")
    if len(picked) != len(_PRICE_CHECK_TRIGGER_STATEMENTS):
        raise ValueError(f"migration 246 changed shape: {len(picked)} trigger statements")
    return "\n".join(picked)


async def _heal_price_check_trigger() -> None:
    try:
        ddl = price_check_trigger_ddl()
    except Exception as exc:  # noqa: BLE001
        logger.warning("schema guard: price_checked_at trigger not installed: %s", exc)
        return
    await _heal_guarded("catalog_offers", PRICE_CHECK_TRIGGER_NEEDED, ddl)


# And for index builds. `CREATE INDEX IF NOT EXISTS` takes the table's SHARE lock
# before it looks for the name, so run bare every boot it queued behind any open
# writer of the table and blocked every later writer behind it. `_ensure_index`
# builds only while the table's schema holds no relation of that name (the test
# IF NOT EXISTS itself makes, read from pg_class without a lock), and a build that
# cannot get the lock within the lock_timeout waits for the next boot instead.
# Any other failure raises exactly as the bare statement did (a missing table
# still raises UndefinedTable), so the callers that rely on a raise keep it.
_INDEX_RE = re.compile(
    r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+(\w+)\s+ON\s+(\w+)\s*\(",
    re.IGNORECASE,
)
_LOCK_NOT_AVAILABLE = "55P03"


def guarded_index(sql: str) -> str:
    """Return the DO block that builds the index `sql` creates only while it is
    missing. `sql` must be exactly one CREATE [UNIQUE] INDEX IF NOT EXISTS."""
    match = _INDEX_RE.match(sql)
    statement = sql.strip().rstrip(";").strip()
    if match is None or ";" in statement or "$" in statement:
        raise ValueError(f"not a single CREATE INDEX IF NOT EXISTS: {sql.strip()[:80]!r}")
    name, table = match.group(1).lower(), match.group(2).lower()
    return f"""
        DO $index$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class
                WHERE relname = '{name}'
                  AND relnamespace = (
                      SELECT relnamespace FROM pg_class WHERE oid = to_regclass('{table}')
                  )
            ) THEN
                PERFORM set_config('lock_timeout', '{HEAL_LOCK_TIMEOUT}', true);
                {statement};
            END IF;
        END $index$;
        """


async def _ensure_index(sql: str) -> None:
    try:
        await database.execute(text(guarded_index(sql)))
    except Exception as exc:
        if not is_lock_timeout(exc):
            raise
        logger.warning("schema guard: index build deferred to next boot: %s", exc)


def is_lock_timeout(exc: BaseException) -> bool:
    """True when `exc` is a guarded statement giving up on its lock_timeout
    (SQLSTATE 55P03), the one failure a bare statement never had: it waited.

    A caller that memoizes its heal must not memoize past one of these, and one
    whose request used to wait for the lock should defer the heal to a later
    call rather than fail the request. Reads the driver error under a
    SQLAlchemy wrapper too."""
    for err in (exc, getattr(exc, "orig", None)):
        if err is not None and getattr(err, "sqlstate", None) == _LOCK_NOT_AVAILABLE:
            return True
    return False


def constraint_exists(table: str, name: str) -> str:
    """True while `table` holds a constraint `name` (its DROP CONSTRAINT IF EXISTS is needed)."""
    return f"""EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = to_regclass('{table}')
                  AND conname = '{name}'
            )"""


_GUARDED_BLOCK_RE = re.compile(r"^\s*DO\s+\$(heal|guard|index)\$", re.IGNORECASE)
_CREATE_TABLE_RE = re.compile(r"^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s", re.IGNORECASE)


def guarded_statements(statements: Sequence[str], *, postgres: bool = IS_POSTGRES) -> List[str]:
    """An `ensure_*` DDL list (run on the request path, once per process or
    after a failure) with every statement that locks an existing table in its
    guarded form: column-add ALTERs through `guarded_add_columns`, index builds
    through `guarded_index`. CREATE TABLE IF NOT EXISTS locks nothing that
    exists and passes through verbatim, as do blocks already guarded by hand
    (`guarded_ddl` for an ALTER COLUMN or a constraint).

    Anything else raises ValueError, so a statement added to one of these
    lists later is guarded or refused at import, never sent out bare.

    Off Postgres the list comes back unchanged: DO blocks are Postgres-only,
    and on SQLite the bare CREATE INDEX statements are what build the test
    databases' indexes (unique ones included).
    """
    if not postgres:
        return list(statements)
    out: List[str] = []
    for statement in statements:
        if _GUARDED_BLOCK_RE.match(statement) or _CREATE_TABLE_RE.match(statement):
            out.append(statement)
        elif _INDEX_RE.match(statement):
            out.append(guarded_index(statement))
        elif re.match(r"^\s*ALTER\s+TABLE\s", statement, re.IGNORECASE):
            out.extend(guarded_add_columns(statement))
        else:
            raise ValueError(f"no guarded form for: {statement.strip()[:80]!r}")
    return out


@dataclass(frozen=True)
class RequiredTableColumns:
    table: str
    columns: Set[str]


REQUIRED_SCHEMA: Sequence[RequiredTableColumns] = (
    RequiredTableColumns(
        table="orders",
        columns={
            # Buyer Vault linkage columns (used by creator/shopping agent checkout flows)
            "buyer_id",
            "intent_id",
            "agent_user_ref",
            "agent_scoped_buyer_ref",
        },
    ),
    RequiredTableColumns(
        table="merchant_psps",
        columns={
            "secret_key",
            "environment",
            "provider_config",
            "validation_status",
            "validation_error",
            "last_validated_at",
        },
    ),
    RequiredTableColumns(
        table="merchant_stores",
        columns={
            "is_primary",
            "order_writeback_status",
            "order_writeback_enabled_at",
            "order_writeback_canary_order_id",
            "order_writeback_last_canary_order_id",
            "order_writeback_last_verified_at",
            "order_writeback_last_error",
            # Store-lifecycle reconciliation — see migration 190. The job that
            # catches a missed uninstall webhook (issue #1648) cannot decide
            # what is due, or apply its two-strike rule, without these.
            "upstream_probe_at",
            "upstream_probe_status",
            "upstream_probe_http_status",
            "upstream_probe_failures",
            # Not added by migration 190 — it predates it — but the due-store
            # SELECT now reads it as the fallback connect anchor, so a table
            # created before this column existed kills the probe half silently.
            # Required here so that failure is a loud 503, not a quiet no-op.
            "created_at",
        },
    ),
    RequiredTableColumns(
        table="catalog_merchants",
        columns={
            # Merchant-wide public discovery gate — see migration 139.
            # Public crawler-facing surfaces require this TRUE plus the
            # per-content-key index_pipeline_state.serving_eligible gate.
            "indexable",
        },
    ),
    RequiredTableColumns(
        table="catalog_products",
        columns={
            # Pivota canonical PDP — see migration 071. The canonical
            # resolver (routes/pivota_canonical_routes.py) and the
            # audit URL fallback (routes/merchant_audit_routes.py)
            # both depend on these columns being present.
            "pivota_signature_id",
            "pivota_canonical_url",
            # Phase C-4 PR-D — see migration 073. Audit reports use
            # this timestamp to compute the indexing-arc phase
            # (fresh / indexing / expected_steady) for Pivota
            # canonical PDPs in merchant_view.diagnosis.
            "pivota_signature_minted_at",
            # Phase O-1 — see migration 075. The Shopify ingest path
            # (services/catalog_sync_service.py:ingest_standard_products)
            # writes merchant-supplied tags into this column. Without
            # the column the SQLAlchemy mapping in db/catalog.py errors
            # on insert. Listed here so prod deploys without separately
            # applying migrations still get the column at startup.
            "tags",
            # Phase O-2 — see migration 076. Pivota-normalized
            # taxonomy v1. Same fail-safe pattern: catalog_products
            # mapping in db/catalog.py declares them, so without these
            # columns ingest writes will error.
            "price_tier",
            "use_case_tags",
            "lifestyle_tags",
            "demographic",
            # Phase 2 / O-5 — see migration 069. Catalog sync writes
            # category_path + provenance inline; without these in both
            # schema_guard and db.catalog metadata, production sync fails.
            "category_path",
            "category_label",
            "category_confidence",
            "category_label_source",
            # Phase O-4 — see migration 077. Onboarding lifecycle
            # stage (draft/candidate/validated/published/hold/archived).
            # Recall (Phase O-5) filters on this column.
            "pdp_lifecycle_stage",
            # Phase O-5b — see migration 094. Structured fashion fields
            # + per-field provenance; the PIVOTA-Agent gateway's
            # canonicalCatalogSearch SELECT (PR #1393) materializes
            # these directly into product.fashion_meta. Missing column
            # → gateway query errors on any catalog row hit.
            "material",
            "material_source",
            "material_confidence",
            "care",
            "care_source",
            "care_confidence",
            "size_guide",
            "size_guide_source",
            "size_guide_confidence",
            # Sitemap freshness signal - see migration 138. Separate
            # from updated_at so internal row touches do not leak into
            # public sitemap lastmod values.
            "content_changed_at",
            # Source-level provenance — see migration 133. Lets new
            # ingests distinguish stores under the same merchant/platform.
            "source_domain",
        },
    ),
    RequiredTableColumns(
        table="catalog_skus",
        columns={
            "source_domain",
        },
    ),
    RequiredTableColumns(
        table="catalog_offers",
        columns={
            "suppression_reason",
            "suppressed_at",
            "source_domain",
        },
    ),
    RequiredTableColumns(
        table="external_product_seeds",
        columns={
            # ADR-009 D3 seller-of-record threading — see migration 169.
            # Seeds carry the seller-of-record (seller_ref) + seed_kind so the
            # T2 attribution chain keys conversions by SELLER. NULL = pre-A9-4
            # legacy; never assumed 'self'.
            "seller_ref",
            "seed_kind",
            # Destination liveness (migration 200). Written ONLY by a fetch that
            # reached the origin, so the readiness gate can ask "is this link still
            # there" instead of inferring it from `updated_at`, which any writer
            # bumps. NULL destination_checked_at = never verified = blocked.
            "destination_checked_at",
            "destination_http_status",
            "destination_verdict",
            "destination_failure_streak",
            # Content freshness (migration 202). Written ONLY by the success path of
            # _refresh_external_seed_by_id, so the refresh queue can order by "when did
            # we last re-read this PRICE" instead of `updated_at`, which an attach, a
            # status flip or a governance write bumps without going near the origin.
            # Two clocks: `last_crawl_attempt_at` orders the QUEUE (advances on every terminal
            # outcome, so a dead seed cannot pin the head of it); `last_crawled_at` is the
            # FRESHNESS signal (advances only on a fetch that reached the origin).
            "last_crawled_at",
            "last_crawl_attempt_at",
        },
    ),
    RequiredTableColumns(
        table="merchant_credit_balance",
        columns={
            "purchased_credits",
            "overage_pending_credits",
            "overage_charged_credits",
            "overage_blocked_until_payment",
            "overage_last_payment_intent_id",
            "overage_last_failed_at",
        },
    ),
)


async def _ensure_database_connected() -> None:
    if getattr(database, "is_connected", False):
        return
    try:
        await database.connect()
    except Exception:
        return


async def check_required_schema() -> Dict[str, List[str]]:
    """
    Returns missing columns for required tables.
    This is a read-only check (no DDL) and is safe to call in /health.
    """
    await _ensure_database_connected()

    missing: Dict[str, List[str]] = {}
    for spec in REQUIRED_SCHEMA:
        present: Set[str] = set()
        try:
            if IS_POSTGRES:
                # NOTE: use raw SQL string instead of SQLAlchemy TextClause + values.
                # Railway prod uses `databases` in a mode where TextClause does not support
                # `.values(**values)` and will raise AttributeError, which would break /health.
                rows = await database.fetch_all(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = :schema_name
                      AND table_name = :table_name
                    """,
                    {"schema_name": "public", "table_name": spec.table},
                )
                present = {str(r["column_name"]) for r in rows}  # type: ignore[index]
            elif IS_SQLITE:
                # PRAGMA table_info returns columns: cid, name, type, notnull, dflt_value, pk
                rows = await database.fetch_all(f"PRAGMA table_info({spec.table});")
                if not rows:
                    # Table absent entirely. On sqlite (dev/test only) that
                    # means the feature was never exercised locally — several
                    # required tables are created by Postgres migrations that
                    # never run on sqlite. Only column gaps on EXISTING tables
                    # are drift here; prod (Postgres) keeps the strict check.
                    continue
                present = {str(r["name"]) for r in rows}  # type: ignore[index]
            else:
                present = set()
        except Exception:
            # If we cannot introspect, treat as missing to allow callers to fail safely.
            missing[spec.table] = sorted(spec.columns)
            continue

        missing_cols = sorted([c for c in spec.columns if c not in present])
        if missing_cols:
            missing[spec.table] = missing_cols

    return missing


def _is_unique_violation(exc: BaseException) -> bool:
    """A UNIQUE violation on either driver: asyncpg's `UniqueViolationError` (SQLSTATE 23505) or
    SQLite's `IntegrityError` naming "unique", looked for through `__cause__`/`__context__`/`orig`
    too. Local rather than imported from db/reap_agentic_ledger: this module is imported by far
    more than the rail, and must not pull the rail's imports into every boot."""
    seen, candidates = set(), [exc]
    while candidates:
        current = candidates.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if getattr(current, "sqlstate", None) == "23505":
            return True
        if type(current).__name__ == "UniqueViolationError":
            return True
        if type(current).__name__ == "IntegrityError" and "unique" in str(current).lower():
            return True
        candidates.extend(
            (current.__cause__, current.__context__, getattr(current, "orig", None))
        )
    return False


def _warn_one_pending_index_missing(exc: BaseException) -> None:
    """Migration 252's index could not be built. SWALLOWED like every sibling (startup must not
    fail on it) but NOT SILENT, because this is the one index whose absence changes behaviour:
    without it a concurrent mint is no longer handed the winner, and the purchase service's
    reconcile-every-pending-row path is what keeps a buyer from being stranded.

    THE CAUSE IS NAMED ONLY WHEN IT IS KNOWN. A unique violation means a buyer_ref already holds
    two pending enrollments, and the runbook's census finds them; any other failure (a missing
    table, a lock, a permission) is reported as what it is, not blamed on duplicates. The
    exception's MESSAGE is logged: for a CREATE INDEX it is DDL text (Postgres keeps the
    offending key in `detail`, which is not read here), never a row of buyer data."""
    message = str(exc)[:300]
    if _is_unique_violation(exc):
        logger.warning(
            "schema_guard: could not create uq_reap_agentic_enrollments_one_pending: %s: %s — "
            "a buyer_ref already has two pending enrollments. Run the census in "
            "docs/runbooks/reap_agentic_purchase.md ('One pending row per buyer') and reconcile "
            "the duplicates, then restart or apply db/migrations/252 by hand",
            type(exc).__name__,
            message,
        )
        return
    logger.warning(
        "schema_guard: could not build uq_reap_agentic_enrollments_one_pending: %s: %s",
        type(exc).__name__,
        message,
    )


async def ensure_required_schema_light() -> None:
    """
    Best-effort DDL for *critical* schema dependencies.

    This is intentionally limited to fast, low-risk operations (ADD COLUMN IF NOT EXISTS).
    It exists to prevent production outages when a deploy accidentally skips migrations.
    """
    await _ensure_database_connected()
    try:
        if IS_POSTGRES:
            # mig 231: the MERCHANT PURCHASABILITY fact — the only thing that may
            # let a merchant x market row carry a BUY affordance. See
            # db/migrations/231_merchant_purchasability.sql for the incident it
            # closes (flowerbeauty.com served as purchasable with a PayPal-only
            # checkout at a price we did not hold).
            #
            # A CREATE TABLE, so the coverage gate (ADD COLUMN only) cannot see a
            # missing heal here — the same hole the mig-224 and mig-230 blocks
            # name. The catalog-parity test in
            # tests/test_merchant_purchasability_postgres.py is what closes it.
            #
            # Its OWN try, and EARLY in the branch, for the reason the mig-207
            # block below states at length: this whole IS_POSTGRES branch is one
            # try-block, so a statement that raises abandons every statement after
            # it. It fails SAFE either way — with the table missing,
            # `is_purchasable` finds no fact and answers False, which refuses the
            # purchase rather than opening it.
            try:
                await database.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS merchant_purchasability (
                            merchant_domain VARCHAR(255) NOT NULL,
                            market_country VARCHAR(2) NOT NULL
                                CONSTRAINT ck_merchant_purchasability_market
                                CHECK (market_country ~ '^[A-Z]{2}$'),
                            vantage VARCHAR(32) NOT NULL,
                            checked_at TIMESTAMPTZ,
                            verdict VARCHAR(32),
                            card_available BOOLEAN,
                            payment_methods JSONB,
                            landed_price_minor BIGINT,
                            landed_currency VARCHAR(8),
                            expected_price_minor BIGINT,
                            price_drift_minor BIGINT,
                            variant_id VARCHAR(32),
                            evidence JSONB,
                            consecutive_failures INTEGER NOT NULL DEFAULT 0,
                            positive_until TIMESTAMPTZ,
                            PRIMARY KEY (merchant_domain, market_country, vantage)
                        );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 231, second statement: the sweep's due-list index. Its OWN try —
            # an index build that raises must not cost the table above it or the
            # heals below.
            try:
                await _ensure_index(
                    """
                    CREATE INDEX IF NOT EXISTS idx_merchant_purchasability_due
                        ON merchant_purchasability (checked_at);
                    """
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 232: widen migration 228's verdict vocabulary for NO_CARD_PAYMENT and
            # PRICE_DRIFT.
            #
            # THE CREATE TABLE IN db/tierb_cart_link_eligibility_schema.py ALREADY CARRIES THE
            # WIDE LIST, and on a FRESH database that is the whole heal. It is NOT enough on a
            # database that already holds a 228-shaped table: `CREATE TABLE IF NOT EXISTS` does
            # nothing there, so the narrow CHECK survives and the first NO_CARD_PAYMENT write
            # raises. Measured on a 228-only database, which is what production is.
            #
            # The constraint names are POSTGRES'S OWN (`<table>_<column>_check`): migration 228
            # declared both CHECKs inline and unnamed. Dropped by that name and re-added under
            # it, so this and the migration leave byte-identical catalogs — compared by
            # tests/test_merchant_purchasability_postgres.py.
            #
            # IDEMPOTENT, and a no-op once widened. The DROP + ADD pair used to run bare on
            # every boot: two ACCESS EXCLUSIVE locks with no lock_timeout, a validating scan,
            # and a window with no CHECK at all. Now each pair runs in one DO block, only
            # while the constraint is not already exactly the wide one, under the 500ms
            # lock_timeout. Its own try each (inside _heal_guarded), for the reason the
            # mig-207 block below states: this branch is one try-block.
            verdicts = ", ".join(f"'{v}'" for v in _TIERB_VERDICTS)
            for column in ("verdict", "previous_verdict"):
                await _heal_guarded(
                    "tierb_cart_link_eligibility",
                    constraint_differs(
                        "tierb_cart_link_eligibility",
                        f"tierb_cart_link_eligibility_{column}_check",
                        _tierb_verdict_def(column),
                    ),
                    f"""
                    ALTER TABLE IF EXISTS tierb_cart_link_eligibility
                        DROP CONSTRAINT IF EXISTS tierb_cart_link_eligibility_{column}_check;
                    ALTER TABLE IF EXISTS tierb_cart_link_eligibility
                        ADD CONSTRAINT tierb_cart_link_eligibility_{column}_check
                        CHECK ({column} IS NULL OR {column} IN ({verdicts}));
                    """,
                )
            # mig 207: the Reap EXTERNAL AUTHORIZATION ledger + merchant descriptor
            # registry. Both tables are read on the FIRST authorization Reap sends,
            # inside a 1.6-second budget, and a missing relation there is not a 500
            # on a background job — it is a DECLINED CARD at a live checkout. Prod
            # deploys skip db/migrations/, so the tables are born here too.
            #
            # The schema-guard-coverage gate only inspects ADD COLUMN, so nothing
            # would have flagged these CREATE TABLEs; they are here because the
            # runtime read exists, not because a test demanded them. Keep this DDL
            # byte-identical to db/migrations/207_agent_card_auth_decisions.sql —
            # a self-heal that disagrees with the migration makes prod behave
            # unlike every environment where the migration ran.
            #
            # FIRST IN THE BRANCH, AND IN ITS OWN try/except. Both are deliberate,
            # and both were measured rather than guessed.
            #
            # This whole IS_POSTGRES branch is ONE try-block: the first statement
            # that raises abandons every statement after it. The mig-190 block
            # names that hazard and answers it by moving LATER in the chain, which
            # is the right trade for columns that only make /health fail closed.
            # It is the WRONG trade here — a missing relation on this path is a
            # declined card at a live checkout, inside Reap's 1.6s budget, not a
            # background 500. Reproduced on an empty database 2026-09-02: the
            # `CREATE INDEX ... ON commerce_interactions` further down carries no
            # IF EXISTS guard on its table, raises, and starved these two tables
            # completely when they sat at the end of the chain.
            #
            # So: first, so nothing upstream can starve them; and wrapped, so a
            # failure HERE cannot starve the self-heals that follow. Isolated in
            # both directions, which position alone cannot buy.
            try:
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS agent_card_auth_decisions (
                        event_id VARCHAR(128) PRIMARY KEY,
                        card_id VARCHAR(64),
                        issuer_card_ref VARCHAR(128) NOT NULL,
                        decision VARCHAR(8) NOT NULL
                            CHECK (decision IN ('APPROVE', 'DECLINE')),
                        reason VARCHAR(32),
                        reason_code VARCHAR(48) NOT NULL,
                        amount_minor BIGINT,
                        currency VARCHAR(8),
                        channel VARCHAR(16),
                        merchant_name TEXT,
                        merchant_city TEXT,
                        merchant_country VARCHAR(2),
                        mcc VARCHAR(4),
                        merchant_verified BOOLEAN NOT NULL DEFAULT FALSE,
                        latency_ms INTEGER NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                        """
                    )
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS "
                    "idx_agent_card_auth_decisions_card_decision "
                    "ON agent_card_auth_decisions (card_id, decision);"
                )
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS agent_card_merchant_descriptors (
                        id BIGSERIAL PRIMARY KEY,
                        merchant_domain VARCHAR(255) NOT NULL,
                        name_norm TEXT NOT NULL,
                        country VARCHAR(2),
                        city_norm TEXT,
                        source VARCHAR(16) NOT NULL,
                        seen_count INTEGER NOT NULL DEFAULT 1,
                        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        UNIQUE (merchant_domain, name_norm, country)
                    );
                        """
                    )
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS "
                    "idx_agent_card_merchant_descriptors_domain "
                    "ON agent_card_merchant_descriptors (merchant_domain);"
                )
                # mig 207 (F5): the composite the LIVE decision reads under its advisory lock.
                # ALTER-free and on a table born in mig 201, so it heals independently of
                # whether that migration ever ran here.
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS "
                    "idx_agent_issued_cards_issuer_ref_status "
                    "ON agent_issued_cards (issuer_card_ref, status);"
                )
            except Exception:
                # Same best-effort contract as the enclosing block. The card-rail
                # decision path is the loud one if this ever silently fails: the
                # first authorization 500s inside Reap's 1.6s budget, which Reap
                # renders as a declined card — not a silent wrong answer.
                pass
            # mig 224: the Reap AGENTIC rail's two tables — purchases and
            # enrollments. A DIFFERENT rail from the card one above: the buyer
            # enrols their OWN card on Reap's hosted page and approves each
            # purchase there, so nothing here holds money or card data.
            #
            # SECOND IN THE BRANCH, AND IN ITS OWN try/except, for the two
            # reasons the mig-207 block spells out at length. This branch is ONE
            # try-block, so a statement that raises abandons every statement
            # after it — being early means nothing downstream can starve these
            # tables, and being wrapped means a failure HERE cannot starve the
            # self-heals that follow. Both directions, which position alone
            # cannot buy.
            #
            # These are the whole storage layer for the rail, not a column on an
            # existing table: prod deploys skip db/migrations/, so without this
            # block the tables do not exist in production at all and every call
            # on the rail is an UndefinedTable 500.
            #
            # THE COVERAGE GATE CANNOT SEE THIS. tests/test_schema_guard_migration_
            # coverage.py inspects ADD COLUMN only, so a missing CREATE TABLE
            # self-heal is invisible to it — exactly the hole the mig-207 comment
            # names. tests/test_reap_agentic_ledger.py::test_self_heal_* is the
            # thing that actually checks it, on both dialects.
            #
            # THIS DDL MUST BUILD THE SAME SCHEMA AS
            # db/migrations/224_reap_agentic_ledger.sql. Not "byte-identical" —
            # that was the earlier wording here and it was a claim no test made,
            # which is worse than a weaker claim that one does. What is actually
            # enforced, by
            # tests/test_reap_agentic_ledger_postgres.py::
            # test_the_self_heal_builds_the_same_schema_as_the_migration, is that
            # a database built by this block and one built by the migration agree
            # on: every column (name, type, nullability, default, length), every
            # index's `pg_indexes.indexdef` — so UNIQUE cannot quietly become
            # non-unique — and every CHECK constraint's `pg_get_constraintdef`.
            # Formatting and comments may differ; nothing the database acts on may.
            #
            # That test exists because the reviewer's mutant proved the old prose
            # was load-bearing and unchecked: changing CREATE UNIQUE INDEX to
            # CREATE INDEX in both branches here left the entire suite green.
            try:
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_enrollments (
                        id VARCHAR(64) PRIMARY KEY,
                        buyer_ref VARCHAR(128) NOT NULL,
                        agent_id VARCHAR(128),
                        reap_enrollment_id VARCHAR(128),
                        status VARCHAR(16) NOT NULL
                            CHECK (status IN ('pending', 'active', 'dead')),
                        reap_status VARCHAR(64),
                        card_network VARCHAR(32),
                        card_last4 VARCHAR(4)
                            CHECK (card_last4 IS NULL OR length(card_last4) = 4),
                        hosted_url TEXT,
                        hosted_url_expires_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                        """
                    )
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS "
                    "idx_reap_agentic_enrollments_buyer_status "
                    "ON reap_agentic_enrollments (buyer_ref, status);"
                )
                await _ensure_index(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_reap_agentic_enrollments_reap_id "
                    "ON reap_agentic_enrollments (reap_enrollment_id) "
                    "WHERE reap_enrollment_id IS NOT NULL;"
                )
                # AT MOST ONE ACTIVE ENROLLMENT PER BUYER. With two, "which card
                # did this buyer authorize?" has no answer. The ledger's
                # mark_enrollment_active demotes the loser in the same
                # transaction; this index is what makes that non-optional.
                await _ensure_index(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_reap_agentic_enrollments_one_active "
                    "ON reap_agentic_enrollments (buyer_ref) "
                    "WHERE status = 'active';"
                )
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_purchases (
                        id VARCHAR(64) PRIMARY KEY,
                        buyer_ref VARCHAR(128) NOT NULL,
                        agent_id VARCHAR(128),
                        agent_user_ref_hash VARCHAR(64),
                        enrollment_id VARCHAR(64),
                        state VARCHAR(24) NOT NULL CHECK (state IN (
                            'resolving', 'needs_enrollment', 'quoting', 'awaiting_approval',
                            'processing', 'completed', 'failed', 'refused', 'expired'
                        )),
                        -- The clock the poll loop cannot reset: stamped at create and
                        -- only by a statement that CHANGES state. `updated_at` is
                        -- written by every claim/release/requeue, so an absolute
                        -- waiting deadline measured from it never fires.
                        state_entered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        merchant_domain VARCHAR(255),
                        product_key TEXT,
                        variant_key TEXT,
                        product_name TEXT,
                        variant_title TEXT,
                        brand TEXT,
                        category TEXT,
                        quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
                        currency VARCHAR(8),
                        our_price_minor BIGINT,
                        click_id VARCHAR(128),
                        return_url TEXT,
                        reap_product_id VARCHAR(128),
                        reap_variant_id VARCHAR(128),
                        reap_quote_id VARCHAR(128),
                        reap_quote_expires_at TIMESTAMPTZ,
                        reap_checkout_id VARCHAR(128),
                        reap_order_id VARCHAR(128),
                        quoted_total_minor BIGINT,
                        final_total_minor BIGINT,
                        shipping_minor BIGINT,
                        tax_minor BIGINT,
                        hosted_url TEXT,
                        hosted_url_expires_at TIMESTAMPTZ,
                        refusal_reason VARCHAR(64),
                        queries_tried JSONB,
                        last_error_code VARCHAR(64),
                        attempts INTEGER NOT NULL DEFAULT 0,
                        claimed_by VARCHAR(128),
                        claimed_at TIMESTAMPTZ,
                        next_poll_at TIMESTAMPTZ,
                        shipping_address JSONB,
                        buyer_email TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        terminal_at TIMESTAMPTZ
                    );
                        """
                    )
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS "
                    "idx_reap_agentic_purchases_state_poll "
                    "ON reap_agentic_purchases (state, next_poll_at);"
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS "
                    "idx_reap_agentic_purchases_buyer_created "
                    "ON reap_agentic_purchases (buyer_ref, created_at);"
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS "
                    "idx_reap_agentic_purchases_owner_created "
                    "ON reap_agentic_purchases "
                    "(agent_id, agent_user_ref_hash, created_at);"
                )
                await _ensure_index(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_reap_agentic_purchases_checkout "
                    "ON reap_agentic_purchases (reap_checkout_id) "
                    "WHERE reap_checkout_id IS NOT NULL;"
                )
            except Exception:  # noqa: BLE001
                # Best-effort like every sibling, and it must not starve what
                # follows. The rail itself is loud if this silently fails: every
                # call against it is an UndefinedTable 500 rather than a wrong
                # answer, so the failure is visible from the first request.
                pass
            # mig 255 and mig 253: the operator audit tables
            # (reap_unopened_attempt_retirements, reap_checkout_manual_resolution_audit).
            # EACH IN ITS OWN try, AFTER the mig-224 block. They used to sit inside it,
            # between the enrollments and the purchases DDL: a failure in either abandoned
            # the purchases table, and a failure in the enrollment indexes abandoned both
            # audit tables, which the operator decisions refuse to run without. Neither
            # depends on the other or on the mig-224 tables. DDL text unchanged.
            try:
                await database.execute(text("""CREATE TABLE IF NOT EXISTS reap_unopened_attempt_retirements (
    receipt_id VARCHAR(32) PRIMARY KEY,
    agent_id VARCHAR(128) NOT NULL,
    agent_user_ref_hash VARCHAR(64) NOT NULL,
    native_key VARCHAR(128) NOT NULL,
    cart_key VARCHAR(128) NOT NULL,
    native_request_hash VARCHAR(64) NOT NULL,
    cart_request_hash VARCHAR(64) NOT NULL,
    authority_sha256 VARCHAR(64) NOT NULL,
    evidence_sha256 VARCHAR(64) NOT NULL,
    operator_ref VARCHAR(128) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (agent_id, agent_user_ref_hash, native_key, cart_key),
    CHECK (native_key <> cart_key)
);
"""))
            except Exception as exc:  # noqa: BLE001
                logger.warning("schema_guard: reap_unopened_attempt_retirements (mig 255) unavailable (%s)",
                               type(exc).__name__)
            try:
                await database.execute(text("""CREATE TABLE IF NOT EXISTS reap_checkout_manual_resolution_audit (
    purchase_id VARCHAR(64) PRIMARY KEY,
    reap_checkout_id VARCHAR(128) NOT NULL,
    from_state VARCHAR(32) NOT NULL,
    resolved_state VARCHAR(32) NOT NULL,
    attribution_outcome VARCHAR(32) NOT NULL,
    operator_ref VARCHAR(128) NOT NULL,
    evidence_source VARCHAR(64) NOT NULL,
    evidence_reference VARCHAR(128) NOT NULL,
    evidence_sha256 VARCHAR(64) NOT NULL,
    expected_updated_at TIMESTAMPTZ NOT NULL,
    evidence_observed_at TIMESTAMPTZ NOT NULL,
    provider_base_url VARCHAR(255) NOT NULL,
    provider_status VARCHAR(32) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (from_state IN ('awaiting_approval','processing')),
    CHECK (resolved_state IN ('completed','failed','expired')),
    CHECK (provider_status IN ('COMPLETED','FAILED','EXPIRED')),
    CHECK (attribution_outcome IN ('edge_closed','closed_by_other_channel','not_applicable')),
    CHECK (evidence_source IN ('authenticated_reap_checkout_read','verified_reap_support_statement'))
);"""))
            except Exception as exc:  # noqa: BLE001
                logger.warning("schema_guard: reap_checkout_manual_resolution_audit (mig 253) unavailable (%s)",
                               type(exc).__name__)
            try:
                await _heal_add_columns("""
                    ALTER TABLE IF EXISTS reap_agentic_purchases
                        ADD COLUMN IF NOT EXISTS dispatch_tracking_version INTEGER,
                        ADD COLUMN IF NOT EXISTS checkout_dispatch_key VARCHAR(64),
                        ADD COLUMN IF NOT EXISTS contact_received_at TIMESTAMPTZ,
                        ADD COLUMN IF NOT EXISTS contact_purged_at TIMESTAMPTZ,
                        ADD COLUMN IF NOT EXISTS contact_revision INTEGER NOT NULL DEFAULT 0;
                """)
            except Exception:
                pass
            # mig 256: privacy clocks and durable dispatch fence, independent of enrollment indexes.
            try:
                from db.reap_continuation import ensure_continuation_schema
                await ensure_continuation_schema()
            except Exception as exc:
                logger.warning("schema_guard: Reap continuation schema unavailable (%s)", type(exc).__name__)
            # mig 257: reap_checkout_dispatch_resolution_audit, the parked-dispatch decision
            # audit, in its OWN try as well. `ensure_continuation_schema` creates it LAST, after
            # the column adds, the journal, its widening and its triggers, so any of those
            # failing skipped it. Re-issued here on its own (CREATE TABLE IF NOT EXISTS, a no-op
            # when the call above succeeded), with the module's own DDL text.
            try:
                from db.reap_continuation import _RESOLUTION_AUDIT
                await database.execute(_RESOLUTION_AUDIT)
            except Exception as exc:  # noqa: BLE001
                logger.warning("schema_guard: reap_checkout_dispatch_resolution_audit (mig 257) unavailable (%s)",
                               type(exc).__name__)
            # mig 258: the price witness (preflight quote + corroborated price change). Its own
            # try: the columns are read and written only behind dark dials, and a failure here must
            # not starve what follows. The same statement as
            # db/reap_price_witness._ADD_COLUMNS_PG, spelled here for the coverage gate
            # (tests/test_schema_guard_migration_coverage.py).
            try:
                await _heal_add_columns("""
                    ALTER TABLE IF EXISTS reap_agentic_purchases
                        ADD COLUMN IF NOT EXISTS preflight_outcome VARCHAR(16),
                        ADD COLUMN IF NOT EXISTS preflight_error_code VARCHAR(64),
                        ADD COLUMN IF NOT EXISTS preflight_checked_at TIMESTAMPTZ,
                        ADD COLUMN IF NOT EXISTS preflight_items_subtotal_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS preflight_shipping_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS preflight_tax_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS preflight_tax_included BOOLEAN,
                        ADD COLUMN IF NOT EXISTS preflight_total_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS live_unit_price_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS live_items_subtotal_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS live_quoted_total_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS live_price_stage VARCHAR(16),
                        ADD COLUMN IF NOT EXISTS price_rebound_from_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS price_rebound_to_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS price_corroboration_source VARCHAR(32),
                        ADD COLUMN IF NOT EXISTS price_corroborated_at TIMESTAMPTZ;
                """)
            except Exception:
                pass
            try:
                from db.reap_price_witness import ensure_price_witness_schema
                await ensure_price_witness_schema()
            except Exception as exc:
                logger.warning("schema_guard: Reap price witness schema unavailable (%s)", type(exc).__name__)
            # mig 252: AT MOST ONE PENDING ENROLLMENT PER BUYER.
            # db/migrations/252_reap_agentic_enrollments_one_pending.sql is the
            # same index. Two purchases of one buyer, each in 'resolving' in one
            # tick, used to mint one pending enrollment EACH (two hosted links to
            # one buyer, review of #2483 P2-1); with this index the second INSERT
            # is refused and db/reap_agentic_ledger.upsert_pending_enrollment
            # hands it the winner instead.
            #
            # ITS OWN try/except, for the reason every sibling states: CREATE
            # UNIQUE INDEX FAILS on a database that already holds two pending
            # rows for one buyer_ref, and that failure must not starve anything
            # after it. The rail stays correct without the index — the purchase
            # service reconciles EVERY pending row, oldest first — it just stops
            # being able to prevent the duplicate. The runbook has the census.
            try:
                await _ensure_index(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_reap_agentic_enrollments_one_pending "
                    "ON reap_agentic_enrollments (buyer_ref) "
                    "WHERE status = 'pending';"
                )
            except Exception as exc:  # noqa: BLE001
                _warn_one_pending_index_missing(exc)
            # mig 254: durable malformed expiry provenance, independently healed.
            try:
                await _heal_add_columns("ALTER TABLE IF EXISTS reap_agentic_enrollments ADD COLUMN IF NOT EXISTS hosted_url_expiry_invalid BOOLEAN NOT NULL DEFAULT FALSE;")
            except Exception:  # noqa: BLE001
                pass
            # mig 225: the resolver's three hint columns.
            #
            # THIS DDL MUST BUILD THE SAME SCHEMA AS
            # db/migrations/225_reap_agentic_purchase_hints.sql — not the same
            # bytes. Leading whitespace differs and no test asserts otherwise;
            # the same wording as the mig-224 block above, and for the same
            # reason. What IS enforced, by
            # tests/test_reap_agentic_ledger_postgres.py::
            # test_the_self_heal_builds_the_hint_columns_identically_to_the_
            # migration, is that the two builds agree on every column the
            # catalog reports — name, type, nullability, default, length.
            #
            # ── ITS OWN try/except, AND THAT IS THE WHOLE POINT OF THIS BLOCK ──
            #
            # It used to be the LAST statement inside the mig-224 try above, and
            # that was a real defect rather than a style question: every
            # statement in that try can raise, and one of them raises on
            # databases that exist. `CREATE UNIQUE INDEX
            # uq_reap_agentic_enrollments_one_active` FAILS on a 224-shaped
            # database that already holds two 'active' enrollments for one
            # buyer_ref — reproduced by the reviewer. The raise abandoned every
            # statement after it, so the three hint columns NEVER LANDED, and
            # because production never runs db/migrations there was no other
            # route: `create_purchase` then raised UndefinedColumnError on every
            # call, forever, on exactly the databases that were already unwell.
            #
            # A failure of the CREATE TABLEs cannot starve this, and a failure
            # HERE cannot starve the self-heals below. Both directions, which is
            # what the mig-207 and mig-212 blocks buy the same way and for the
            # same reason. Position alone buys neither.
            #
            # THIS IS ALSO THE SHAPE THE COVERAGE GATE CHECKS. The CREATE TABLEs
            # above are invisible to tests/test_schema_guard_migration_coverage.py
            # (it parses ADD COLUMN only) — an ALTER is not, so this one is gated
            # at PR time as well as by the rail's own parity test.
            #
            # THE CREATE TABLE ABOVE DELIBERATELY DOES NOT CARRY THESE THREE, and
            # this ALTER is what lands them on EVERY path. That keeps the two
            # builds identical down to ordinal position: the migration route is
            # `224 CREATE` then `225 ALTER`, and folding the columns into the
            # CREATE here would make the self-heal's route `CREATE-with-them` —
            # same column set, different order, and a needless second place to
            # keep in step. On an empty database the CREATE runs and this ALTER
            # adds three columns; on a 224-shaped database the CREATE is the
            # no-op and this ALTER is the whole of the heal; on a 225-shaped one
            # both are no-ops. All three arrivals end at one schema, which is
            # what idempotent means here.
            #
            # The statement carries `IF EXISTS` on the table name, so on a
            # database where the CREATE above genuinely failed this is a no-op
            # rather than a second error.
            #
            # DO NOT WRITE THE TWO WORDS "A-L-T-E-R T-A-B-L-E" IN PROSE ANYWHERE
            # IN THIS FILE. tests/test_schema_guard_migration_coverage.py scans
            # this source with a regex that matches those words followed by a
            # name and then captures everything up to the next `;` — a mention
            # in a comment matches first, swallows the real statement below it
            # into its body, and files these columns under a table called "if".
            # The gate then reports migration 225 as uncovered. That happened,
            # on this very comment.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS reap_agentic_purchases
                        ADD COLUMN IF NOT EXISTS accept_variant_labels JSONB,
                        ADD COLUMN IF NOT EXISTS also_accept_domains JSONB,
                        ADD COLUMN IF NOT EXISTS market_country TEXT;
                    """
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 226: the three tables the agentic purchase ROUTES read —
            # eligibility, the per-buyer opaque ref, and the idempotency keys.
            # db/migrations/226_reap_agentic_routes.sql is the same schema; that
            # file's header says what each one is for and why none of them could
            # be a column on the purchase row.
            #
            # ITS OWN try/except, NOT folded into either block above. The mig-225
            # comment records what folding cost last time: one statement in a
            # shared try raised on databases that exist, and everything after it
            # never landed. The failure modes here are not hypothetical either —
            # `CREATE UNIQUE INDEX uq_reap_agentic_buyer_refs_ref` fails on a
            # database that somehow holds two buyers with one ref, and if that
            # raise could reach the mig-224 block it would take the whole rail's
            # storage with it.
            #
            # AFTER the two blocks above rather than before them, which is the
            # opposite of the mig-207 argument and deliberate: these three tables
            # are read by a ROUTE that 404s while the dial is off, so a missing
            # relation here is a refused purchase on a dark rail, not a declined
            # card at a live checkout. The tables that ARE on the live path go
            # first; this one takes its turn.
            #
            # THIS DDL MUST BUILD THE SAME SCHEMA AS THE MIGRATION — not the same
            # bytes. What is enforced, by tests/test_agent_commerce_reap_routes_
            # postgres.py::test_the_self_heal_builds_the_same_schema_as_migration
            # _226, is that a database built from that file and one built by this
            # block agree on every column, every index's `pg_indexes.indexdef`
            # (so UNIQUE cannot quietly become non-unique), and every CHECK
            # constraint's `pg_get_constraintdef`. Prose is not a test; that one
            # is, and it was written because the reviewer's mutant on the mig-224
            # pair proved the earlier wording was load-bearing and unchecked.
            #
            # The coverage gate cannot see any of this: tests/test_schema_guard_
            # migration_coverage.py inspects one kind of statement only, and
            # every statement here creates a relation rather than a column.
            # ONE try PER STATEMENT, NOT ONE PER BLOCK. The first cut of this
            # block shared a single try across all four statements, and its own
            # comment named `CREATE UNIQUE INDEX uq_reap_agentic_buyer_refs_ref`
            # as the realistic failure — on a database that already holds two
            # buyers with one ref — while `CREATE TABLE
            # reap_agentic_purchase_keys` sat AFTER it in the same try. That is
            # exactly the mig-224/225 defect this file already carries a long
            # comment about: the raise abandons every statement after it, so the
            # keys table would never land, on precisely the databases that were
            # already unwell, and production has no other route to it.
            #
            # Per statement, every arrival converges: each one is independently
            # a no-op when its object exists, and a failure of any one cannot
            # starve the others or the self-heals below.
            try:
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_eligibility (
                        merchant_domain VARCHAR(255) NOT NULL,
                        product_key TEXT NOT NULL DEFAULT '',
                        variant_key TEXT NOT NULL DEFAULT '',
                        market_country VARCHAR(2) NOT NULL
                            CHECK (market_country ~ '^[A-Z]{2}$'),
                        enabled BOOLEAN NOT NULL DEFAULT FALSE,
                        accept_variant_labels JSONB,
                        also_accept_domains JSONB,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (merchant_domain, market_country, product_key, variant_key)
                    );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            try:
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_buyer_refs (
                        buyer_id VARCHAR(50) PRIMARY KEY,
                        reap_buyer_ref VARCHAR(128) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 227: the consent tag the buyer's enrollment hangs off.
            #
            # NOT FOLDED INTO THE CREATE ABOVE, DELIBERATELY. Folding it in
            # would make this statement dead on a fresh database — the only
            # kind CI builds — so deleting it would still pass every test while
            # leaving the columns missing on exactly the databases that already
            # hold a buyer_refs table, which is production. Kept separate, this
            # statement is the whole of the heal on EVERY arrival: the CREATE
            # builds the 226 shape and this brings it to 227, whether the table
            # was born a second ago or a month ago.
            #
            # ITS OWN try, per the rule this block already follows. The columns
            # are nullable with no default, so this cannot fail on data — but a
            # raise here must not be able to starve the keys table below, which
            # is the exact defect the comment above records.
            #
            # DO NOT WRITE THE TWO WORDS "A-L-T-E-R T-A-B-L-E" IN PROSE
            # ANYWHERE IN THIS FILE — see the mig-225 comment above for the
            # gate this breaks and how it broke it.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS reap_agentic_buyer_refs
                        ADD COLUMN IF NOT EXISTS consent_version VARCHAR(32),
                        ADD COLUMN IF NOT EXISTS consented_at TIMESTAMPTZ;
                    """
                )
            except Exception:  # noqa: BLE001
                pass
            try:
                # THE STATEMENT THAT ACTUALLY FAILS IN THE FIELD. It cannot be
                # created on a database that already holds two buyers sharing
                # one ref, and that is the whole reason it has its own try.
                await _ensure_index(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_reap_agentic_buyer_refs_ref "
                    "ON reap_agentic_buyer_refs (reap_buyer_ref);"
                )
            except Exception:  # noqa: BLE001
                pass
            try:
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_purchase_keys (
                        agent_id VARCHAR(128) NOT NULL,
                        agent_user_ref_hash VARCHAR(64) NOT NULL,
                        idempotency_key VARCHAR(128) NOT NULL,
                        purchase_id VARCHAR(64) NOT NULL,
                        request_hash VARCHAR(64) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (agent_id, agent_user_ref_hash, idempotency_key)
                    );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 229: the purchase's ITEM SOURCE — 'reap_variant' (every row
            # before this) or 'cart_link' (a Shopify cart permalink Reap quotes
            # as received), plus the URL a cart_link row carries.
            #
            # THIS DDL MUST BUILD THE SAME SCHEMA AS
            # db/migrations/229_reap_agentic_purchase_item_source.sql, CHECKs
            # included — they carry the pairing rule (a cart_link row has a URL,
            # a reap_variant row has none). Enforced through the catalog by
            # tests/test_reap_agentic_cart_link_postgres.py and by the whole-
            # table parity test in tests/test_reap_agentic_ledger_postgres.py.
            #
            # ITS OWN try/except, for the reason the mig-225 block above gives
            # at length: a raise in a sibling must not starve these columns, and
            # a raise here must not starve the heals that follow. The column
            # constraints ride on `ADD COLUMN IF NOT EXISTS`, so a second run
            # skips them with their columns rather than adding a duplicate.
            #
            # NOT folded into the CREATE TABLE above, same as mig 225: this is
            # what lands the columns on EVERY path, in migration order.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS reap_agentic_purchases
                        ADD COLUMN IF NOT EXISTS item_source TEXT NOT NULL DEFAULT 'reap_variant'
                            CONSTRAINT ck_reap_agentic_purchases_item_source
                            CHECK (item_source IN ('reap_variant', 'cart_link')),
                        ADD COLUMN IF NOT EXISTS cart_url TEXT
                            CONSTRAINT ck_reap_agentic_purchases_cart_url_pairing
                            CHECK (
                                (item_source = 'reap_variant' AND cart_url IS NULL)
                                OR (item_source = 'cart_link' AND cart_url IS NOT NULL)
                            );
                    """
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 236: the partner's settled amount and cut on an agent-share ledger row (ADR-025 D5,
            # the agent's share is taken after a channel partner's). create_all built
            # agent_share_ledger without them and never alters an existing table, so this is what
            # lands them in prod. THIS DDL MUST MATCH db/migrations/236_agent_share_after_partner.sql
            # and db/agent_share.py (pinned by tests/test_agent_share_accrual.py). Its OWN try, as
            # above: a raise here must not starve the heals around it.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS agent_share_ledger
                        ADD COLUMN IF NOT EXISTS partner_settled_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS merchant_billed_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS partner_cut_minor BIGINT;
                    """
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 229, second statement: one cart-link purchase per click. Its OWN try:
            # on a database that already holds two cart-link rows for one click the
            # build RAISES, and that must not cost the columns above or the heals below.
            try:
                await _ensure_index(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_reap_agentic_purchases_cart_link_click
                        ON reap_agentic_purchases (click_id)
                        WHERE item_source = 'cart_link';
                    """
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 233: the consent that was in force when the purchase was OPENED,
            # on the purchase row itself.
            #
            # WHY THE PURCHASE ROW AND NOT ONLY THE BUYER ROW (mig 227). WP4c
            # deletes the `reap_agentic_buyer_refs` row when a buyer identity is
            # repointed by a hosted-checkout sign-in, and that row was the sole
            # carrier of the tag. These two columns are the copy nothing deletes;
            # the refs row keeps its own pair as the LATEST consent, for re-use.
            #
            # ITS OWN try, per this block's rule. The columns are nullable with no
            # default, so this cannot fail on data — but a raise here must not be
            # able to starve the mig-230 heal below, which is the exact defect the
            # mig-225 comment above records at length.
            #
            # NOT FOLDED INTO THE mig-224 CREATE TABLE ABOVE, deliberately and for
            # the reason the mig-227 twin states: folded in, this would be dead on
            # a fresh database — the only kind CI builds — so deleting it would
            # pass every test while leaving the columns missing on exactly the
            # databases that already hold a purchases table, which is production.
            # Kept separate, this statement is the whole of the heal on EVERY
            # arrival, and it lands the columns in migration order.
            #
            # DO NOT WRITE THE TWO WORDS "A-L-T-E-R T-A-B-L-E" IN PROSE ANYWHERE
            # IN THIS FILE — see the mig-225 comment above for the gate this
            # breaks and how it broke it.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS reap_agentic_purchases
                        ADD COLUMN IF NOT EXISTS consent_version VARCHAR(32),
                        ADD COLUMN IF NOT EXISTS consented_at TIMESTAMPTZ;
                    """
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 247: the buyer's offer code (create-only), what it came to at the
            # quote, and the discount Reap applied. The ledger names all three in
            # every purchase INSERT / transition, so without them every purchase
            # write fails loudly. ITS OWN try, per this block's rule, and not
            # folded into the mig-224 CREATE above for the reason the mig-233 heal
            # states. Same "no prose A-L-T-E-R T-A-B-L-E" rule as above.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS reap_agentic_purchases
                        ADD COLUMN IF NOT EXISTS offer_code VARCHAR(128),
                        ADD COLUMN IF NOT EXISTS offer_code_outcome VARCHAR(16),
                        ADD COLUMN IF NOT EXISTS discount_minor BIGINT,
                        ADD COLUMN IF NOT EXISTS tax_included BOOLEAN;
                    """
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 249: the live variant title on the enrichment cart proof (display only; the
            # proof job writes it, PR C shows it). The table itself is created on first use by
            # db/enrichment_cart_variant_proofs.ensure_table(), which also runs this ALTER; this
            # heal covers a table created before 249. A no-op while the table does not exist.
            # ITS OWN try, per this block's rule.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS enrichment_cart_variant_proofs
                        ADD COLUMN IF NOT EXISTS variant_title TEXT;
                    """
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 237: the chargeback part of commerce_attribution_edges.refund_amount_cents.
            # Every refund and dispute write to the edge names this column
            # (services/commerce_attribution_service.py _APPLY_REFUND_TOTAL_QUERY,
            # _ATTRIBUTE_DISPUTE_QUERY), so without it those writes fail, and
            # RefundService.create_refund makes its write INSIDE its own transaction.
            # A constant default is a catalog-only change on PG11+, so this does not
            # rewrite the table. Its OWN try, per this block's rule.
            #
            # LOCKING. `ADD COLUMN IF NOT EXISTS` takes the table's ACCESS EXCLUSIVE
            # lock BEFORE it finds the column exists, so run bare it queued every
            # read of commerce_attribution_edges behind it on EVERY boot. The
            # information_schema check means the statement only runs while the
            # column is missing, and the transaction-local 500ms lock_timeout means
            # that one run gives up (and is retried next boot) instead of stalling
            # the table, and the heals after it, for the guard's whole budget.
            try:
                await database.execute(
                    text(
                        """
                        DO $$
                        BEGIN
                            IF to_regclass('public.commerce_attribution_edges') IS NOT NULL
                               AND NOT EXISTS (
                                   SELECT 1 FROM information_schema.columns
                                   WHERE table_name = 'commerce_attribution_edges'
                                     AND column_name = 'dispute_amount_cents'
                               ) THEN
                                PERFORM set_config('lock_timeout', '500ms', true);
                                ALTER TABLE IF EXISTS commerce_attribution_edges
                                    ADD COLUMN IF NOT EXISTS dispute_amount_cents BIGINT NOT NULL DEFAULT 0;
                            END IF;
                        END $$;
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 237, second statement: the migration's >= 0 CHECK. Guarded on
            # pg_constraint (as the apm_cadence_days heal is) so a boot does not
            # re-validate the table every time, and bounded by the same 500ms
            # lock_timeout as the column heal above. Its OWN try: a failure here
            # must not undo, or be mistaken for, the column heal.
            try:
                await database.execute(
                    text(
                        """
                        DO $$
                        BEGIN
                            IF to_regclass('public.commerce_attribution_edges') IS NOT NULL
                               AND EXISTS (
                                   SELECT 1 FROM information_schema.columns
                                   WHERE table_name = 'commerce_attribution_edges'
                                     AND column_name = 'dispute_amount_cents'
                               )
                               AND NOT EXISTS (
                                   SELECT 1 FROM pg_constraint
                                   WHERE conname = 'ck_commerce_attribution_edges_dispute_amount_cents'
                               ) THEN
                                PERFORM set_config('lock_timeout', '500ms', true);
                                ALTER TABLE commerce_attribution_edges
                                    ADD CONSTRAINT ck_commerce_attribution_edges_dispute_amount_cents
                                    CHECK (dispute_amount_cents >= 0);
                            END IF;
                        END $$;
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 230: the per-click attribution CLAIM for cart-link Reap purchases,
            # and the index the merchant side needs to ask "is this click one of
            # those?". THIS DDL MUST BUILD THE SAME SCHEMA AS
            # db/migrations/230_conversion_click_claims.sql; compared through the
            # catalog by tests/test_reap_agentic_cart_link_postgres.py
            # (test_the_self_heal_builds_the_230_catalog_the_migration_builds).
            #
            # A CREATE TABLE, so the coverage gate (ADD COLUMN only) cannot see a
            # missing heal here, the same hole the mig-224 block names. The parity
            # test is what catches it.
            #
            # Best-effort like every sibling, and it fails SAFE: without the table
            # the merchant side fails open (closes as before 230) and the Reap side
            # fails closed (skips its edge), so a missing heal can never double an
            # edge. The scope lookup's index is the item_source migration's partial
            # unique index, healed with that migration.
            try:
                await database.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS conversion_click_claims (
                            click_id TEXT PRIMARY KEY,
                            claimed_by TEXT NOT NULL
                                CONSTRAINT ck_conversion_click_claims_claimed_by
                                CHECK (claimed_by IN ('reap_agentic', 'merchant_order')),
                            external_order_id TEXT,
                            claimed_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 239 (ADR-025 D1): when a click id was ISSUED to an agent; NULL = legacy row
            # first written by /r. The surface_click_events model names it, so every
            # select(surface_click_events) needs it: its own try, and ADD COLUMN IF NOT EXISTS so
            # the coverage gate sees it.
            try:
                # Guarded and bounded like the dispute heals (#2289): the ALTER runs only while
                # the column is missing, and gives up after 500ms instead of queueing every read
                # of surface_click_events behind it and eating the guard's startup budget.
                await database.execute(
                    text(
                        """
                        DO $$
                        BEGIN
                            IF to_regclass('public.surface_click_events') IS NOT NULL
                               AND NOT EXISTS (
                                   SELECT 1 FROM information_schema.columns
                                   WHERE table_schema = 'public'
                                     AND table_name = 'surface_click_events'
                                     AND column_name = 'issued_at'
                               ) THEN
                                PERFORM set_config('lock_timeout', '500ms', true);
                                ALTER TABLE IF EXISTS surface_click_events
                                  ADD COLUMN IF NOT EXISTS issued_at TIMESTAMPTZ;
                            END IF;
                        END $$;
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 212: the recovery key — the join the Prove stage rests on.
            # Early and wrapped for the same reason as mig 210 below: this
            # branch is ONE try, and an unguarded CREATE INDEX further down
            # abandons everything after it on a partial database.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS merchant_tasks
                      ADD COLUMN IF NOT EXISTS recovery_key TEXT NULL;
                    """
                )
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS commerce_interactions
                      ADD COLUMN IF NOT EXISTS recovery_key VARCHAR(40) NULL;
                    """
                )
            except Exception:  # noqa: BLE001
                # Best-effort like every sibling; must not starve what follows.
                pass
            # mig 228: Tier B cart-link eligibility. Early and in its own
            # try, for the reasons the mig-207 block gives: this branch is ONE
            # try, so being early means nothing upstream can starve it and
            # being wrapped means a failure here cannot starve what follows.
            # The DDL lives in db/tierb_cart_link_eligibility_schema (one
            # definition, shared with the SQLite branch below); tests/
            # test_tierb_cart_link_eligibility_postgres.py compares the schema
            # it builds with db/migrations/228_* through the catalog.
            try:
                from db.tierb_cart_link_eligibility_schema import (
                    ensure_schema as _ensure_tierb_cart_link_eligibility,
                )

                await _ensure_tierb_cart_link_eligibility()
            except Exception:  # noqa: BLE001
                # Loud elsewhere if it fails: the job's first write is an
                # UndefinedTable error, and it exits non-zero.
                pass
            # mig 215: collector token registry. Early and wrapped like its
            # siblings: the issue routes INSERT into these tables, so a deploy
            # that skips db/migrations/ would fail every token provisioning
            # until they exist. Both tables here, both in the migration.
            try:
                await database.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS merchant_collector_tokens (
                            jti VARCHAR(64) PRIMARY KEY,
                            merchant_id VARCHAR(50) NOT NULL,
                            store_id VARCHAR(128) NOT NULL,
                            token_type VARCHAR(32) NOT NULL,
                            token_version INTEGER NOT NULL,
                            store_token_version INTEGER NOT NULL,
                            allowed_origins JSONB NULL,
                            issued_at TIMESTAMPTZ NOT NULL,
                            expires_at TIMESTAMPTZ NOT NULL,
                            revoked_at TIMESTAMPTZ NULL,
                            revoked_reason VARCHAR(64) NULL,
                            superseded_by VARCHAR(64) NULL,
                            issued_by VARCHAR(128) NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        );
                        """
                    )
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS idx_merchant_collector_tokens_store "
                    "ON merchant_collector_tokens (merchant_id, store_id);"
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS idx_merchant_collector_tokens_expiring "
                    "ON merchant_collector_tokens (expires_at) WHERE revoked_at IS NULL;"
                )
                await database.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS merchant_collector_token_policy (
                            store_id VARCHAR(128) PRIMARY KEY,
                            merchant_id VARCHAR(50) NOT NULL,
                            min_token_version INTEGER NOT NULL DEFAULT 1,
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                # Best-effort like every sibling; must not starve what follows.
                pass
            # mig 220: source + run_id on the preflight observations. `web` deploys
            # with SKIP_HEAVY_STARTUP_INIT, so db/migrations/ never runs there and
            # db/catalog.py's model is what builds the table — but only on a database
            # that does not have it YET. An existing prod table predates these two
            # columns and create_all will not alter it, so without this heal
            # `record()` fails every INSERT on an unknown column and swallows the
            # error, and shadow mode stops recording with only a dropped log line to
            # show for it. Two columns in the migration, two here.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS checkout_preflight_observations
                      ADD COLUMN IF NOT EXISTS source VARCHAR(32) NOT NULL DEFAULT 'live',
                      ADD COLUMN IF NOT EXISTS run_id VARCHAR(64) NULL;
                    """
                )
                # The index too, or an existing prod table never gets it: create_all only
                # builds indexes on a table it creates, and db/migrations/ does not run here.
                await _ensure_index(
                    """
                    CREATE INDEX IF NOT EXISTS idx_checkout_preflight_obs_run
                      ON checkout_preflight_observations (run_id);
                    """
                )
            except Exception:  # noqa: BLE001
                # Best-effort like every sibling; must not starve what follows.
                pass

            # mig 213: trust provenance on the commerce ledger. Early and
            # wrapped like mig 212: the SQLAlchemy INSERT names every modeled
            # column, so a deploy that skips db/migrations/ would fail every
            # canonical event write until these columns exist. Four columns in
            # the migration, four here.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS commerce_interaction_events
                      ADD COLUMN IF NOT EXISTS write_path VARCHAR(48) NULL,
                      ADD COLUMN IF NOT EXISTS authority VARCHAR(16) NULL,
                      ADD COLUMN IF NOT EXISTS agent_identity_confidence VARCHAR(24) NULL,
                      ADD COLUMN IF NOT EXISTS synthetic BOOLEAN NOT NULL DEFAULT FALSE;
                    """
                )
                # The mig-214 partial index is deliberately absent here: it
                # rolls out CONCURRENTLY, which this guard's transaction
                # cannot do, and nothing on the write path needs it.
            except Exception:  # noqa: BLE001
                # Best-effort like every sibling; must not starve what follows.
                pass
            # mig 216: the canonical order_ref. Early and wrapped like mig 213
            # for the same reason: the SQLAlchemy INSERT names every modeled
            # column, so a deploy that skips db/migrations/ would fail every
            # canonical event write until BOTH columns exist. Two columns in
            # the migration, two here; and unlike mig 214's index this one is
            # built normally (order_ref is new and all-NULL, so there is
            # nothing to scan), so the guard carries it too.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS commerce_interactions
                      ADD COLUMN IF NOT EXISTS order_ref VARCHAR(160) NULL;
                    """
                )
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS commerce_interaction_events
                      ADD COLUMN IF NOT EXISTS order_ref VARCHAR(160) NULL;
                    """
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS ix_commerce_interactions_order_ref "
                    "ON commerce_interactions (order_ref);"
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS ix_commerce_interaction_events_order_ref "
                    "ON commerce_interaction_events (order_ref);"
                )
                # The stitch rests on this one: without it two authorities can
                # both insert an interaction for the same canonical order and
                # neither insert raises.
                await _ensure_index(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "idx_commerce_interactions_order_ref_unique "
                    "ON commerce_interactions (merchant_id, COALESCE(store_id, ''), order_ref) "
                    "WHERE order_ref IS NOT NULL;"
                )
            except Exception:  # noqa: BLE001
                # Best-effort like every sibling; must not starve what follows.
                pass
            # mig 210: anonymous audit runs. Position and wrapping are BOTH
            # load-bearing, and neither alone is enough — measured, not assumed.
            #
            # Sitting at the end of the chain, this block NEVER RAN on a
            # partial database: `CREATE INDEX ... ON commerce_interactions`
            # ~1600 lines below has no IF EXISTS guard on its table, raises
            # UndefinedTable, and abandons every statement after it. Reproduced
            # 2026-09-03 on a database holding only merchant_audit_runs —
            # merchant_id stayed NOT NULL and merchant_claimed_at was never
            # added, i.e. the one deploy shape this self-heal exists for.
            # Wrapping alone would not have fixed it: the abort happens
            # UPSTREAM, so the block has to move too. Same answer the mig-207
            # block above reaches, for the same reason.
            #
            # Both halves must be here. A deploy that skips db/migrations/
            # would otherwise leave merchant_id NOT NULL with the column
            # present, and every anonymous insert would fail. The
            # schema-guard-coverage gate only inspects ADD COLUMN, so the
            # DROP NOT NULL is carried deliberately, and the partial index
            # with it — three statements in the migration, three here.
            try:
                await _heal_add_columns(
                    """
                    ALTER TABLE IF EXISTS merchant_audit_runs
                      ADD COLUMN IF NOT EXISTS merchant_claimed_at TIMESTAMPTZ;
                    """
                )
                # Only while merchant_id is still NOT NULL: bare, this took ACCESS
                # EXCLUSIVE on every boot with no lock_timeout.
                await _heal_guarded(
                    "merchant_audit_runs",
                    column_is_not_null("merchant_audit_runs", "merchant_id"),
                    """
                    ALTER TABLE IF EXISTS merchant_audit_runs
                      ALTER COLUMN merchant_id DROP NOT NULL;
                    """,
                )
                await _ensure_index(
                    "CREATE INDEX IF NOT EXISTS "
                    "idx_merchant_audit_runs_unclaimed "
                    "ON merchant_audit_runs (requested_at DESC) "
                    "WHERE merchant_id IS NULL;"
                )
                # mig 211 (#2020): one unclaimed funnel run per domain. NOT
                # CONCURRENTLY here — this path runs inside the guard's own
                # transaction, where CONCURRENTLY is illegal. On a database
                # that reaches this code the table is small or the index
                # already exists; the migration carries the CONCURRENTLY form
                # for the hot production table.
                await _ensure_index(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "idx_funnel_run_one_per_domain ON merchant_audit_runs "
                    "((partial_result_jsonb->'funnel'->>'domain')) "
                    "WHERE merchant_id IS NULL "
                    "AND subject_type = 'public_funnel';"
                )
            except Exception:
                # Best-effort, like every sibling. Failing here is fail-CLOSED
                # (anonymous inserts error); it must not starve what follows.
                pass
            # Universal commerce collector references (migration 204). Railway
            # fast-mode skips migrations, while SQLAlchemy SELECTs materialize
            # every modeled column; self-heal before the first event arrives.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS commerce_interactions
                  ADD COLUMN IF NOT EXISTS store_id VARCHAR(128),
                  ADD COLUMN IF NOT EXISTS cart_id VARCHAR(128),
                  ADD COLUMN IF NOT EXISTS payment_id VARCHAR(128),
                  ADD COLUMN IF NOT EXISTS visitor_id VARCHAR(128);
                """
            )
            # Only while one of the four is not VARCHAR(128) yet. Bare, this took
            # ACCESS EXCLUSIVE on the event ledger on every boot with no
            # lock_timeout; and it sat in this branch's shared try, so a failure
            # (a missing column) abandoned every heal after it.
            await _heal_guarded(
                "commerce_interactions",
                column_type_differs(
                    "commerce_interactions",
                    ("checkout_id", "order_id", "refund_id", "return_id"),
                    "character varying(128)",
                ),
                """
                ALTER TABLE IF EXISTS commerce_interactions
                  ALTER COLUMN checkout_id TYPE VARCHAR(128),
                  ALTER COLUMN order_id TYPE VARCHAR(128),
                  ALTER COLUMN refund_id TYPE VARCHAR(128),
                  ALTER COLUMN return_id TYPE VARCHAR(128);
                """,
            )
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS commerce_interaction_events
                  ADD COLUMN IF NOT EXISTS store_id VARCHAR(128),
                  ADD COLUMN IF NOT EXISTS cart_id VARCHAR(128),
                  ADD COLUMN IF NOT EXISTS payment_id VARCHAR(128),
                  ADD COLUMN IF NOT EXISTS visitor_id VARCHAR(128);
                """
            )
            for statement in (
                "CREATE INDEX IF NOT EXISTS idx_commerce_interactions_store "
                "ON commerce_interactions(merchant_id, platform, store_id)",
                "CREATE INDEX IF NOT EXISTS idx_commerce_interactions_store_cart "
                "ON commerce_interactions(merchant_id, store_id, cart_id) WHERE cart_id IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_commerce_interactions_store_payment "
                "ON commerce_interactions(merchant_id, store_id, payment_id) WHERE payment_id IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_commerce_interactions_store_session "
                "ON commerce_interactions(merchant_id, store_id, session_id) WHERE session_id IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_commerce_interaction_events_store "
                "ON commerce_interaction_events(merchant_id, platform, store_id)",
                "CREATE INDEX IF NOT EXISTS idx_commerce_interaction_events_cart "
                "ON commerce_interaction_events(merchant_id, cart_id) WHERE cart_id IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_commerce_interaction_events_payment "
                "ON commerce_interaction_events(merchant_id, payment_id) WHERE payment_id IS NOT NULL",
            ):
                await _ensure_index(statement)

            # Migration 205: external platform references are local to a
            # merchant/store. Replace only legacy global indexes; once the
            # scoped definition is present this block performs no DDL.
            await database.execute(
                text(
                    """
                    DO $$
                    DECLARE
                      ref_col TEXT;
                      idx_name TEXT;
                      idx_def TEXT;
                    BEGIN
                      FOREACH ref_col IN ARRAY ARRAY[
                        'click_id', 'quote_id', 'checkout_id',
                        'order_id', 'refund_id', 'return_id'
                      ] LOOP
                        idx_name := 'idx_commerce_interactions_' || ref_col || '_unique';
                        SELECT indexdef INTO idx_def
                          FROM pg_indexes
                         WHERE schemaname = current_schema()
                           AND indexname = idx_name;
                        IF idx_def IS NULL
                           OR position('merchant_id' IN idx_def) = 0
                           OR position('store_id' IN idx_def) = 0 THEN
                          EXECUTE format('DROP INDEX IF EXISTS %I', idx_name);
                          EXECUTE format(
                            'CREATE UNIQUE INDEX %I ON commerce_interactions '
                            || '(merchant_id, COALESCE(store_id, ''''), %I) '
                            || 'WHERE %I IS NOT NULL',
                            idx_name, ref_col, ref_col
                          );
                        END IF;
                      END LOOP;
                    END $$;
                    """
                )
            )

            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS orders
                  ADD COLUMN IF NOT EXISTS buyer_id TEXT,
                  ADD COLUMN IF NOT EXISTS intent_id TEXT,
                  ADD COLUMN IF NOT EXISTS agent_user_ref TEXT,
                  ADD COLUMN IF NOT EXISTS agent_scoped_buyer_ref TEXT;
                """
            )
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_psps
                  ADD COLUMN IF NOT EXISTS secret_key TEXT,
                  ADD COLUMN IF NOT EXISTS environment VARCHAR(20) DEFAULT 'unknown',
                  ADD COLUMN IF NOT EXISTS provider_config JSONB DEFAULT '{}'::jsonb,
                  ADD COLUMN IF NOT EXISTS validation_status VARCHAR(20) DEFAULT 'unknown',
                  ADD COLUMN IF NOT EXISTS validation_error TEXT,
                  ADD COLUMN IF NOT EXISTS last_validated_at TIMESTAMP WITH TIME ZONE;
                """
            )
            # Migration 241: merchant_psps.psp_id must satisfy the SAME regex
            # `orders.psp_id` has enforced since migration 006. Order creation
            # copies this column into orders.psp_id, so a malformed id written
            # here is a 500 the merchant only meets at their first sale. Prod
            # fast mode skips db/migrations/, so own the apply here too.
            #
            # NOT VALID: enforce every new INSERT/UPDATE without scanning the
            # existing rows. A validating ADD would abort startup on the legacy
            # malformed rows this exists to surface (see
            # scripts/audit_malformed_psp_ids.py). The IF NOT EXISTS guard keeps
            # this a no-op after the first boot -- a DROP+ADD every startup would
            # take an ACCESS EXCLUSIVE lock on a table every checkout reads.
            #
            # Its OWN try, and a 500ms lock_timeout, like the mig-237 heals. The
            # ADD still takes ACCESS EXCLUSIVE on its one run; queued bare behind
            # a long transaction it would block every reader of merchant_psps
            # while it waited. And two instances booting together can both pass
            # the pg_constraint check, so the loser's ADD raises DuplicateObject.
            # Either failure is retried next boot and must not skip the heals
            # after this one.
            try:
                await database.execute(
                    text(
                        """
                    DO $$
                    BEGIN
                      IF to_regclass('merchant_psps') IS NULL THEN
                        RETURN;
                      END IF;
                      IF NOT EXISTS (
                        SELECT 1
                          FROM pg_constraint
                         WHERE conname = 'check_merchant_psps_psp_id_format'
                           AND conrelid = to_regclass('merchant_psps')
                      ) THEN
                        PERFORM set_config('lock_timeout', '500ms', true);
                        ALTER TABLE merchant_psps
                          ADD CONSTRAINT check_merchant_psps_psp_id_format
                          CHECK (psp_id ~* '^psp_[a-z0-9]+_[a-z0-9]{12}$')
                          NOT VALID;
                      END IF;
                    END $$;
                    """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # Multi-use partner invite links (migration 171). Production fast
            # mode skips db/migrations/, so ensure the columns the invite-token
            # service reads/writes (use_count, max_uses) exist at startup —
            # otherwise list_for_partner/issue/consume 500 on the missing
            # columns and the whole invite panel breaks.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS partner_invite_tokens
                  ADD COLUMN IF NOT EXISTS use_count INTEGER NOT NULL DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS max_uses INTEGER;
                """
            )
            # Allow 'partner_invite' in partner_send_log so the invite auto-email
            # is recorded in "Recent sends" (migration 172). Prod fast mode skips
            # db/migrations/, so widen the CHECK here on startup. Without it the
            # invite send-log INSERT violates the settlement-only template CHECK
            # and is silently dropped.
            #
            # Only while the CHECK is not already exactly this one. Bare, the
            # DROP + ADD ran on every boot: two ACCESS EXCLUSIVE locks with no
            # lock_timeout, a validating scan, and a window with no CHECK at all
            # (each statement committed on its own). Now one DO block, in its own
            # try.
            await _heal_guarded(
                "partner_send_log",
                constraint_differs(
                    "partner_send_log",
                    "ck_partner_send_log_template",
                    _PARTNER_SEND_LOG_TEMPLATE_DEF,
                ),
                """
                ALTER TABLE IF EXISTS partner_send_log
                  DROP CONSTRAINT IF EXISTS ck_partner_send_log_template;
                ALTER TABLE IF EXISTS partner_send_log
                  ADD CONSTRAINT ck_partner_send_log_template CHECK (
                    template_id IN (
                      'settlement_monthly',
                      'settlement_skipped',
                      'settlement_failed_notice',
                      'partner_invite'
                    )
                  );
                """,
            )
            # NOTE ON PLACEMENT: this block is deliberately NOT next to the
            # merchant_psps ADD COLUMN statements above, which is where it
            # naturally belongs by subject. That seam is where the sibling psp_id
            # fix (20f4542c, migration 241) also inserts, and two independent
            # additions at one anchor merge as a CONFLICT whose correct resolution
            # is a UNION -- precisely the resolution someone drops a block during.
            # Order does not matter here: the statement is self-guarding on
            # to_regclass('orders') and touches nothing else in this function.
            # Migration 242: `orders.psp_used` must accept every provider this
            # code WRITES. Migration 006 froze the list at five names
            # ('stripe','adyen','checkout','paypal','braintree'); the code moved
            # on. `_resolve_active_order_psp` copies merchant_psps.provider into
            # orders.psp_used, and that column can hold 'antom'
            # (SUPPORTED_CANONICAL_PSPS) — so onboarding succeeded and then EVERY
            # order creation 500'd on the CHECK. 'protocol_deferred' is the
            # capability-gated deferred lane's sentinel and was the same defect a
            # feature flag away. Prod fast mode skips db/migrations/, so own the
            # apply here too.
            #
            # NOT VALID: enforce every new INSERT/UPDATE without scanning the
            # existing rows. The new list is a strict superset of 006's, so a
            # widen cannot fail — but on a database where 006 never ran this is
            # an ADD, and a validating one would scan years of an unconstrained
            # vocabulary and abort the boot.
            #
            # The definition guard is load-bearing: `orders` is the table every
            # checkout writes, and an unconditional DROP+ADD would take an ACCESS
            # EXCLUSIVE lock on it once per instance start. Matching on the two
            # NEW tokens (rather than the exact text) also re-widens if something
            # re-narrows the constraint.
            #
            # Its OWN try, and a 500ms lock_timeout, for the same two reasons as
            # the mig-241 block above: an unbounded wait for ACCESS EXCLUSIVE on
            # `orders` blocks every checkout behind it, and a concurrent boot can
            # make this DROP/ADD fail. Either way it is retried next boot and the
            # heals after it still run.
            try:
                await database.execute(
                    text(
                        """
                    DO $$
                    BEGIN
                      IF to_regclass('orders') IS NULL THEN
                        RETURN;
                      END IF;
                      IF EXISTS (
                        SELECT 1
                          FROM pg_constraint
                         WHERE conname = 'check_psp_used_valid_provider'
                           AND conrelid = to_regclass('orders')
                           AND pg_get_constraintdef(oid) LIKE '%antom%'
                           AND pg_get_constraintdef(oid) LIKE '%protocol_deferred%'
                      ) THEN
                        RETURN;
                      END IF;
                      PERFORM set_config('lock_timeout', '500ms', true);
                      ALTER TABLE orders
                        DROP CONSTRAINT IF EXISTS check_psp_used_valid_provider;
                      ALTER TABLE orders
                        ADD CONSTRAINT check_psp_used_valid_provider
                        CHECK (
                          psp_used IS NULL OR psp_used IN (
                            'stripe',
                            'adyen',
                            'checkout',
                            'paypal',
                            'braintree',
                            'antom',
                            'protocol_deferred'
                          )
                        )
                        NOT VALID;
                      -- Earn the validated status back where it is earnable. In
                      -- production this constraint is already convalidated=TRUE
                      -- over a clean 593-row `orders` (measured 2026-09-02), so a
                      -- bare DROP + ADD NOT VALID would DOWNGRADE a proven
                      -- invariant to a merely-enforced one. The new list is a
                      -- strict SUPERSET of the old, so nothing that passed the old
                      -- one can fail this. Swallowed: a table with genuinely bad
                      -- rows because 006 never ran keeps it NOT VALID -- still
                      -- enforcing every new INSERT/UPDATE -- and the boot
                      -- continues.
                      --
                      -- NOT bounded, and NOT a light lock. This VALIDATE runs in
                      -- the same statement as the DROP/ADD, so it scans `orders`
                      -- under the ACCESS EXCLUSIVE lock they already hold. A
                      -- statement_timeout set in here cannot help, because
                      -- Postgres arms that timer when the statement starts. That
                      -- is fine at ~593 rows, and it only runs on the one boot
                      -- that widens the constraint. On a large `orders`, move the
                      -- VALIDATE into its own statement instead.
                      BEGIN
                        ALTER TABLE orders
                          VALIDATE CONSTRAINT check_psp_used_valid_provider;
                      EXCEPTION WHEN OTHERS THEN
                        RAISE NOTICE 'check_psp_used_valid_provider left NOT VALID: %', SQLERRM;
                      END;
                    END $$;
                    """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # Merchant portal primary-store selection (migration 089).
            # Production fast mode skips db/migrations/, so keep the
            # critical column and invariant available at startup.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_stores
                  ADD COLUMN IF NOT EXISTS is_primary BOOLEAN NOT NULL DEFAULT FALSE,
                  ADD COLUMN IF NOT EXISTS order_writeback_status TEXT NOT NULL DEFAULT 'disabled',
                  ADD COLUMN IF NOT EXISTS order_writeback_enabled_at TIMESTAMPTZ NULL,
                  ADD COLUMN IF NOT EXISTS order_writeback_canary_order_id TEXT NULL,
                  ADD COLUMN IF NOT EXISTS order_writeback_last_canary_order_id TEXT NULL,
                  ADD COLUMN IF NOT EXISTS order_writeback_last_verified_at TIMESTAMPTZ NULL,
                  ADD COLUMN IF NOT EXISTS order_writeback_last_error TEXT NULL;
                """
            )
            await database.execute(
                text(
                    """
                    UPDATE merchant_stores ms
                    SET is_primary = TRUE
                    FROM (
                      SELECT store_id, merchant_id,
                             ROW_NUMBER() OVER (
                               PARTITION BY merchant_id
                               ORDER BY connected_at DESC NULLS LAST, store_id
                             ) AS primary_rank
                      FROM merchant_stores
                      WHERE LOWER(COALESCE(status, 'connected')) IN ('active', 'connected')
                    ) candidate
                    WHERE ms.store_id = candidate.store_id
                      AND candidate.primary_rank = 1
                      AND NOT EXISTS (
                        SELECT 1
                        FROM merchant_stores existing
                        WHERE existing.merchant_id = candidate.merchant_id
                          AND existing.is_primary = TRUE
                      );
                    """
                )
            )
            await database.execute(
                text(
                    """
                    UPDATE merchant_stores ms
                    SET is_primary = FALSE
                    FROM (
                      SELECT store_id,
                             ROW_NUMBER() OVER (
                               PARTITION BY merchant_id
                               ORDER BY connected_at DESC NULLS LAST, store_id
                             ) AS primary_rank
                      FROM merchant_stores
                      WHERE is_primary = TRUE
                    ) ranked
                    WHERE ms.store_id = ranked.store_id
                      AND ranked.primary_rank > 1;
                    """
                )
            )
            await _ensure_index(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uniq_merchant_stores_primary_per_merchant
                  ON merchant_stores (merchant_id)
                  WHERE is_primary = TRUE;
                """
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_merchant_stores_merchant_primary
                  ON merchant_stores (merchant_id, is_primary, connected_at DESC);
                """
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_merchant_stores_order_writeback_status
                  ON merchant_stores (platform, order_writeback_status, status);
                """
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_merchant_stores_order_writeback_canary
                  ON merchant_stores (order_writeback_canary_order_id)
                  WHERE order_writeback_canary_order_id IS NOT NULL;
                """
            )
            # PR-13 APM config columns on merchant_onboarding
            # (migration 089_merchant_onboarding_apm_config.sql).
            # That PR shipped the SQL migration + the SQLAlchemy model
            # update + the routes, but no admin-run-migration apply
            # lever, and production deploys do NOT auto-run
            # db/migrations/. As a result, after PR #494 deployed,
            # every audit run failed in `discovering` with
            # "column merchant_onboarding.apm_enabled does not exist"
            # because merchant_onboarding.select() materializes
            # apm_enabled + friends. This block self-heals on startup
            # so the outage cannot recur on any environment.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_onboarding
                  ADD COLUMN IF NOT EXISTS apm_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                  ADD COLUMN IF NOT EXISTS apm_cadence_days INTEGER NULL,
                  ADD COLUMN IF NOT EXISTS apm_scope_jsonb JSONB NULL,
                  ADD COLUMN IF NOT EXISTS apm_configured_at TIMESTAMPTZ NULL,
                  ADD COLUMN IF NOT EXISTS apm_last_run_at TIMESTAMPTZ NULL,
                  -- signup_source (migration 187), here for the same
                  -- reason the paragraph above gives.
                  --
                  -- WARNING - NO SEMICOLONS ANYWHERE IN THIS COMMENT. The
                  -- coverage gate matches an ALTER body non-greedily up to
                  -- the FIRST semicolon, so one in prose truncates the
                  -- statement and every column after it stops counting as
                  -- covered. Two slipped into the first draft of this note
                  -- and the gate went red on a change whose DDL was correct.
                  -- A third slipped into the warning about the first two.
                  -- The gate caught all three, which is the argument for it.
                  --
                  -- It DOES reach prod today, but only through a LAZY
                  -- backstop: db/merchant_onboarding.py has its own
                  -- idempotent add inside ensure_operating_mode_column(),
                  -- called at the top of create_merchant_onboarding() - i.e.
                  -- on the FIRST MERCHANT SIGNUP of a process, behind a
                  -- module-level done-flag.
                  --
                  -- That covers the column by CALL ORDER. Two read paths
                  -- escape it:
                  --   * merchant_onboarding.select() materializes every
                  --     column, and get_merchant_onboarding() sits on the
                  --     ADR-018 connection-layer path, and
                  --   * services/funnel_metrics_service.py issues a RAW
                  --     "SELECT merchant_id, signup_source, ..." from the
                  --     admin funnel-metrics router. That one never touches
                  --     create_merchant_onboarding() at all, so NO call
                  --     order saves it - a fresh process against a DB
                  --     without the column returns a hard
                  --     "column does not exist".
                  --
                  -- Honest about the strength of this fix: the whole guard
                  -- is best-effort (one try/except around the body, a 12s
                  -- startup timeout, failures downgraded to a warning), so
                  -- it CANNOT fail a deploy - which is the right trade. It
                  -- makes the column independent of call order. It does not
                  -- make it guaranteed.
                  ADD COLUMN IF NOT EXISTS signup_source VARCHAR(64);
                """
            )
            # Cadence check constraint (idempotent via DO block —
            # mirrors migration 089's pg_constraint guard).
            await database.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = 'merchant_onboarding_apm_cadence_days_chk'
                        ) THEN
                            ALTER TABLE merchant_onboarding
                                ADD CONSTRAINT merchant_onboarding_apm_cadence_days_chk
                                CHECK (
                                    apm_cadence_days IS NULL
                                    OR apm_cadence_days IN (7, 14, 30)
                                );
                        END IF;
                    END $$;
                    """
                )
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_merchant_onboarding_apm_due
                  ON merchant_onboarding (apm_last_run_at, apm_cadence_days)
                  WHERE apm_enabled = TRUE;
                """
            )
            # Migration 164: store-less brand model — operating_mode discriminator.
            # store_url NOT NULL was relaxed and operating_mode added in the same
            # migration but the migration runner is skipped in prod.  Self-heal both
            # here so the column is present before any SELECT on merchant_onboarding.
            # The DROP NOT NULL only while store_url is still NOT NULL: bare, it took
            # ACCESS EXCLUSIVE on every boot with no lock_timeout.
            await _heal_guarded(
                "merchant_onboarding",
                column_is_not_null("merchant_onboarding", "store_url"),
                """
                ALTER TABLE IF EXISTS merchant_onboarding
                  ALTER COLUMN store_url DROP NOT NULL;
                """,
            )
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_onboarding
                  ADD COLUMN IF NOT EXISTS operating_mode VARCHAR(32) NOT NULL DEFAULT 'storefront';
                """
            )
            # P0-3: DB-enforced audit idempotency (migration 144).
            # Partial UNIQUE index scoped to active stages closes the
            # check-then-insert race in POST /api/audits. The enqueue
            # path uses conflict inference because partial unique
            # indexes are not valid ON CONSTRAINT targets.
            await database.execute(
                text(
                    """
                    DO $$
                    BEGIN
                      IF EXISTS (
                        SELECT 1
                          FROM pg_indexes
                         WHERE schemaname = 'public'
                           AND indexname = 'uniq_merchant_audit_runs_active_idempotency_key'
                           AND indexdef NOT ILIKE '%(merchant_id, idempotency_key)%'
                      ) THEN
                        DROP INDEX uniq_merchant_audit_runs_active_idempotency_key;
                      END IF;
                    END $$;
                    """
                )
            )
            await _ensure_index(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS
                  uniq_merchant_audit_runs_active_idempotency_key
                  ON merchant_audit_runs (merchant_id, idempotency_key)
                  WHERE idempotency_key IS NOT NULL
                    AND stage = ANY(ARRAY[
                      'queued'::text, 'discovering'::text, 'probing'::text,
                      'scoring'::text, 'materializing'::text, 'verifying'::text
                    ]);
                """
            )
            # Q-P0-2 / Q-P1-4: cross-audit task supersession
            # (migration 092). The task_queue_service marks prior
            # pending tasks as `status='superseded'` and points
            # `superseded_by_task_id` at the newer task when a fresh
            # audit emits the same canonical action identity. Without
            # this column, supersession can't write back the pointer
            # and the merchant queue stays cluttered with stale rows.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_tasks
                  ADD COLUMN IF NOT EXISTS superseded_by_task_id UUID NULL;
                """
            )
            # Q-P1-5: executor-produced task child pointer
            # (migration 093). Lets the queue renderer group concrete
            # executor artifacts under the audit action that spawned
            # them while keeping the child rows actionable.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_tasks
                  ADD COLUMN IF NOT EXISTS parent_task_id UUID NULL;
                """
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS
                  idx_merchant_tasks_identity_pending
                  ON merchant_tasks (merchant_id, lever, title)
                  WHERE status = 'pending';
                """
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS
                  idx_merchant_tasks_superseded_by
                  ON merchant_tasks (superseded_by_task_id)
                  WHERE superseded_by_task_id IS NOT NULL;
                """
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS
                  idx_merchant_tasks_parent_task
                  ON merchant_tasks (parent_task_id)
                  WHERE parent_task_id IS NOT NULL;
                """
            )
            # Pivota canonical PDP columns (migration 071). Fast-mode
            # startup skips db/migrations/, so the schema guard owns
            # these in production. Mirrors what's already in db.catalog
            # (the SQLAlchemy model) — schema_guard is the runtime
            # safety net.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_merchants
                  ADD COLUMN IF NOT EXISTS indexable BOOLEAN NOT NULL DEFAULT TRUE;
                """
            )
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_products
                  ADD COLUMN IF NOT EXISTS pivota_signature_id TEXT,
                  ADD COLUMN IF NOT EXISTS pivota_canonical_url TEXT,
                  ADD COLUMN IF NOT EXISTS pivota_signature_minted_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS tags JSONB,
                  ADD COLUMN IF NOT EXISTS price_tier VARCHAR(16),
                  ADD COLUMN IF NOT EXISTS use_case_tags JSONB,
                  ADD COLUMN IF NOT EXISTS lifestyle_tags JSONB,
                  ADD COLUMN IF NOT EXISTS demographic VARCHAR(16),
                  ADD COLUMN IF NOT EXISTS category_path VARCHAR(255),
                  ADD COLUMN IF NOT EXISTS category_label VARCHAR(255),
                  ADD COLUMN IF NOT EXISTS category_confidence REAL,
                  ADD COLUMN IF NOT EXISTS category_label_source VARCHAR(32),
                  ADD COLUMN IF NOT EXISTS pdp_lifecycle_stage VARCHAR(16);
                """
            )
            # Sitemap freshness signal (migration 138). The canonical
            # products sitemap list orders by and returns this column,
            # while updated_at keeps its internal row-touched meaning.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_products
                  ADD COLUMN IF NOT EXISTS content_changed_at TIMESTAMP NOT NULL DEFAULT NOW();
                """
            )
            # Sitemap list pagination index (migration 175). GET
            # /api/canonical/products orders by this exact composite key
            # (and keyset cursors seek on it); without the index every
            # page sorts the whole eligible set and deep OFFSET pages
            # trip the 4s route timeout. Migration 138's single-column
            # content_changed_at index never got a schema_guard entry,
            # so prod had no index behind this sort until this one.
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_catalog_products_sitemap_keyset
                  ON catalog_products (
                    content_changed_at DESC,
                    pivota_signature_id ASC,
                    content_key ASC,
                    product_key ASC
                  )
                  WHERE pivota_signature_id LIKE 'sig_%' AND content_key IS NOT NULL;
                """
            )
            # Phase O-5b: structured fashion fields + per-field provenance
            # (migration 094_catalog_fashion_fields.sql). Production
            # fast-mode startup skips db/migrations/, so schema_guard owns
            # the apply. Without this, the PIVOTA-Agent gateway SELECT
            # (PR #1393) fails with "column does not exist" the moment
            # any fashion-tagged product is requested.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_products
                  ADD COLUMN IF NOT EXISTS material TEXT,
                  ADD COLUMN IF NOT EXISTS material_source VARCHAR(32),
                  ADD COLUMN IF NOT EXISTS material_confidence REAL,
                  ADD COLUMN IF NOT EXISTS care TEXT,
                  ADD COLUMN IF NOT EXISTS care_source VARCHAR(32),
                  ADD COLUMN IF NOT EXISTS care_confidence REAL,
                  ADD COLUMN IF NOT EXISTS size_guide JSONB,
                  ADD COLUMN IF NOT EXISTS size_guide_source VARCHAR(32),
                  ADD COLUMN IF NOT EXISTS size_guide_confidence REAL;
                """
            )
            # Store-domain provenance (migration 133). This is additive and
            # nullable; legacy rows and legacy callers remain NULL.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_products
                  ADD COLUMN IF NOT EXISTS source_domain TEXT;
                """
            )
            # Durable top-level vertical (migration 173). Railway fast-mode
            # startup skips db/migrations/, so schema_guard owns the apply.
            # catalog_sync_service's upsert NAMES this column, so without the
            # self-heal the first sync in prod crashes on "column does not
            # exist" (the PR #494/#501 apm_enabled outage shape).
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_products
                  ADD COLUMN IF NOT EXISTS resolved_vertical VARCHAR(16);
                """
            )
            # LLM attribute-extractor cache (migration 174). catalog_products is
            # SELECT *'d at audit context-build; a missing column would break the
            # read the moment the extractor flag is on. Railway skips migrations,
            # so self-heal it here.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_products
                  ADD COLUMN IF NOT EXISTS llm_attributes JSONB;
                """
            )
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_skus
                  ADD COLUMN IF NOT EXISTS source_domain TEXT;
                """
            )
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_offers
                  ADD COLUMN IF NOT EXISTS source_domain TEXT;
                """
            )
            await _ensure_index(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_products_pivota_signature
                  ON catalog_products (pivota_signature_id)
                  WHERE pivota_signature_id IS NOT NULL;
                """
            )
            # PDP / commerce-index repair PR-1: reversible offer
            # suppression primitive. Code paths filter suppressed offers,
            # so the runtime guard self-heals these additive columns in
            # environments where raw db/migrations/ are skipped.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_offers
                  ADD COLUMN IF NOT EXISTS suppression_reason TEXT NULL,
                  ADD COLUMN IF NOT EXISTS suppressed_at TIMESTAMPTZ NULL;
                """
            )
            # mig 246: when the offer's price was last READ, and the trigger that forgets it when
            # the price moves without a read. The dual-write writers stamp the column, so it has
            # to land with the deploy; the trigger comes after it (its function reads the column).
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_offers
                  ADD COLUMN IF NOT EXISTS price_checked_at TIMESTAMPTZ NULL;
                """
            )
            await _heal_price_check_trigger()
            # Phase O-5b cross-PDP coalesce: agent_pdp_view aggregates
            # material/care/size_guide from all product_group_members +
            # matched external_product_seeds. The columns mirror the
            # catalog_products fashion fields (mig 094) but live on the
            # denormalized view so the gateway gets them in one read.
            # Migration 096_agent_pdp_view_fashion_fields.sql carries the
            # same DDL for dev; schema_guard owns the prod-startup apply
            # since fast-mode skips db/migrations/.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS agent_pdp_view
                  ADD COLUMN IF NOT EXISTS material TEXT,
                  ADD COLUMN IF NOT EXISTS material_source VARCHAR(32),
                  ADD COLUMN IF NOT EXISTS material_confidence REAL,
                  ADD COLUMN IF NOT EXISTS care TEXT,
                  ADD COLUMN IF NOT EXISTS care_source VARCHAR(32),
                  ADD COLUMN IF NOT EXISTS care_confidence REAL,
                  ADD COLUMN IF NOT EXISTS size_guide JSONB,
                  ADD COLUMN IF NOT EXISTS size_guide_source VARCHAR(32),
                  ADD COLUMN IF NOT EXISTS size_guide_confidence REAL;
                """
            )
            # Phase O-4: partial index covering only the live recall
            # stages so the recall-path WHERE clauses (Phase O-5) hit
            # an index instead of a heap scan.
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_catalog_products_lifecycle_live
                  ON catalog_products (pdp_lifecycle_stage)
                  WHERE pdp_lifecycle_stage IN ('validated', 'published');
                """
            )
            # Phase D: GSC OAuth + URL submission tables (migration 074).
            # Fast-mode startup skips db/migrations/, so schema_guard
            # owns these in production. Idempotent CREATE IF NOT EXISTS
            # is safe to run on every boot.
            await database.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS gsc_oauth_tokens (
                      merchant_id        TEXT PRIMARY KEY,
                      refresh_token_enc  TEXT NOT NULL,
                      access_token_enc   TEXT NULL,
                      access_token_expires_at TIMESTAMPTZ NULL,
                      granted_scopes     TEXT[] NOT NULL,
                      granted_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      authorized_site_url TEXT NOT NULL,
                      last_refresh_ok_at TIMESTAMPTZ NULL,
                      last_refresh_error TEXT NULL,
                      revoked_at         TIMESTAMPTZ NULL
                    );
                    """
                )
            )
            await database.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS gsc_url_submissions (
                      merchant_id        TEXT NOT NULL,
                      url                TEXT NOT NULL,
                      last_status        TEXT NOT NULL,
                      last_status_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      submitted_at       TIMESTAMPTZ NULL,
                      indexed_at         TIMESTAMPTZ NULL,
                      error_message      TEXT NULL,
                      source_audit_run_id UUID NULL,
                      PRIMARY KEY (merchant_id, url)
                    );
                    """
                )
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_gsc_url_submissions_merchant_status
                  ON gsc_url_submissions (merchant_id, last_status, last_status_at DESC);
                """
            )
            # BD cold-start audit: prospect_products table (migration 075).
            # Stores discovered products from cold-target brand audits.
            # Separate from catalog_products — prospect data is tentative
            # until the brand onboards.
            await database.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS prospect_products (
                      prospect_brand    TEXT NOT NULL,
                      prospect_domain   TEXT NOT NULL,
                      url               TEXT NOT NULL,
                      title             TEXT NULL,
                      vendor            TEXT NULL,
                      product_type      TEXT NULL,
                      discovery_source  TEXT NOT NULL,
                      raw_extracted     JSONB NULL,
                      discovered_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      last_audit_run_id UUID NULL,
                      last_audited_at   TIMESTAMPTZ NULL,
                      claimed_at                 TIMESTAMPTZ NULL,
                      claimed_by_merchant_id     TEXT NULL,
                      PRIMARY KEY (prospect_domain, url)
                    );
                    """
                )
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_prospect_products_domain_discovered
                  ON prospect_products (prospect_domain, discovered_at DESC);
                """
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_prospect_products_unclaimed
                  ON prospect_products (claimed_at)
                  WHERE claimed_at IS NULL;
                """
            )
            # PR #8 settlement files: snapshot settle markers. The full
            # settlement_files table and triggers are owned by migration 131,
            # but these ADD COLUMNs are mirrored here so startup self-heal
            # covers the runtime columns used by settlement_file_service.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS settlement_snapshots
                  ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS settled_via_file_id BIGINT;
                """
            )
            # Direct/self-serve merchant credit wallet top-ups and overage
            # state (migrations 141/142). The direct audit launch path reads
            # these columns before it can safely decide hard-stop vs overage.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_credit_balance
                  ADD COLUMN IF NOT EXISTS purchased_credits BIGINT NOT NULL DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS overage_pending_credits BIGINT NOT NULL DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS overage_charged_credits BIGINT NOT NULL DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS overage_blocked_until_payment BOOLEAN NOT NULL DEFAULT FALSE,
                  ADD COLUMN IF NOT EXISTS overage_last_payment_intent_id TEXT,
                  ADD COLUMN IF NOT EXISTS overage_last_failed_at TIMESTAMPTZ;
                """
            )
            # T2-2 external-conversion representation (migration 167). These
            # columns are now in the SQLAlchemy Table model, so every runtime
            # `select(commerce_attribution_edges)` emits them — they MUST exist
            # in prod or the SELECT crashes (the PR #494/#501 apm_enabled class
            # of outage). Railway deploys skip db/migrations/, so self-heal here.
            # gross_attributed_gmv_cents predates this (migration 109) and is
            # already in prod; re-adding IF NOT EXISTS is a harmless no-op.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS commerce_attribution_edges
                  ADD COLUMN IF NOT EXISTS state TEXT,
                  ADD COLUMN IF NOT EXISTS converted_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS currency TEXT,
                  ADD COLUMN IF NOT EXISTS external_order_id TEXT,
                  ADD COLUMN IF NOT EXISTS source TEXT,
                  ADD COLUMN IF NOT EXISTS click_id TEXT,
                  ADD COLUMN IF NOT EXISTS gross_attributed_gmv_cents BIGINT;
                """
            )
            # Idempotency guard for the external-conversion closure — one edge
            # per (merchant, external Shopify order). Internal edges keep
            # external_order_id NULL (distinct under multi-column NULL rules).
            await _ensure_index(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_commerce_attribution_edges_external_order
                  ON commerce_attribution_edges (merchant_id, external_order_id);
                """
            )
            # T2-2b read_orders polling floor (migration 168): per-merchant
            # watermark so each poll only fetches new/updated Shopify orders.
            # services/external_conversion_poller.py reads/writes this table on
            # every run; Railway deploys skip db/migrations/, so self-heal here.
            await database.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS external_conversion_poll_state (
                      merchant_id        TEXT PRIMARY KEY,
                      last_polled_at     TIMESTAMPTZ,
                      last_run_at        TIMESTAMPTZ,
                      last_closed_count  INTEGER NOT NULL DEFAULT 0,
                      updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            )
            # P1.11 (migration 188): the persisted ROW-GRAIN renderability column.
            # **Railway prod skips db/migrations/ entirely**, so the migration
            # file alone would never reach production — this block is the path
            # that actually runs. Additive + nullable with NO default on
            # purpose: NULL means "never computed", which consumers must treat
            # as do-not-advertise (`IS TRUE`, never `IS NOT FALSE`). A DEFAULT
            # would erase the distinction between "computed false" and "never
            # computed", which is the one distinction keeping it fail-closed.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_products
                  ADD COLUMN IF NOT EXISTS pdp_will_render BOOLEAN,
                  ADD COLUMN IF NOT EXISTS pdp_will_render_computed_at TIMESTAMPTZ;
                """
            )
            # Partial index on the advertisable side only, keyed on
            # pivota_signature_id rather than the PK: the predicate is ~35%
            # selective, so a PK-keyed index would just be seq-scanned. The sig
            # is what the ACP lane looks rows up by.
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_catalog_products_pdp_will_render_true
                  ON catalog_products (pivota_signature_id)
                  WHERE pdp_will_render IS TRUE;
                """
            )
            # ADR-009 D3 seller-of-record threading (migration 169). external
            # seeds gain seller_ref (a catalog_merchants.merchant_id) + seed_kind
            # ('self'|'cross'); the T2-1 redirect stamps them onto the click and
            # T2-2 closure keys the conversion subject by seller_ref. Railway
            # deploys skip db/migrations/, so self-heal here (167/168 idiom).
            # Additive + nullable: NULL = pre-A9-4 legacy (never assumed 'self').
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS external_product_seeds
                  ADD COLUMN IF NOT EXISTS seller_ref TEXT,
                  ADD COLUMN IF NOT EXISTS seed_kind TEXT;
                """
            )
            # Honesty guard for seed_kind (idempotent via DO block — mirrors the
            # migration-167 state constraint). Only the two derived kinds; NULL
            # allowed for legacy/pre-backfill rows.
            await database.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = 'ck_external_product_seeds_seed_kind'
                        ) THEN
                            ALTER TABLE external_product_seeds
                                ADD CONSTRAINT ck_external_product_seeds_seed_kind
                                CHECK (seed_kind IS NULL OR seed_kind IN ('self', 'cross'));
                        END IF;
                    END $$;
                    """
                )
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_external_product_seeds_seller_ref
                  ON external_product_seeds (seller_ref)
                  WHERE seller_ref IS NOT NULL;
                """
            )
            # Destination liveness (migration 200). Railway deploys skip
            # db/migrations/, so self-heal here — and these columns are load-bearing
            # for the readiness gate the moment the sweep starts writing them.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS external_product_seeds
                  ADD COLUMN IF NOT EXISTS destination_checked_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS destination_http_status INTEGER,
                  ADD COLUMN IF NOT EXISTS destination_verdict TEXT,
                  ADD COLUMN IF NOT EXISTS destination_failure_streak INTEGER NOT NULL DEFAULT 0;
                """
            )
            await database.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = 'ck_external_product_seeds_destination_verdict'
                        ) THEN
                            ALTER TABLE external_product_seeds
                                ADD CONSTRAINT ck_external_product_seeds_destination_verdict
                                CHECK (
                                    destination_verdict IS NULL
                                    OR destination_verdict IN (
                                        'live',
                                        'live_delisted',
                                        'redirected_to_product',
                                        'redirected_off_product',
                                        'dead_404',
                                        'unverifiable'
                                    )
                                );
                        END IF;
                    END $$;
                    """
                )
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_external_product_seeds_destination_checked
                  ON external_product_seeds (destination_checked_at NULLS FIRST)
                  WHERE status = 'active';
                """
            )
            # Content freshness (migration 202). Railway deploys skip db/migrations/,
            # so self-heal here. The refresh queue orders on this column, and until it
            # exists that ORDER BY is a hard error rather than a degraded ordering --
            # `last_crawled_at` is referenced unconditionally by
            # get_external_referral_refresh_candidate_seed_ids.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS external_product_seeds
                  ADD COLUMN IF NOT EXISTS last_crawled_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS last_crawl_attempt_at TIMESTAMPTZ;
                """
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_external_product_seeds_last_crawl_attempt
                  ON external_product_seeds (last_crawl_attempt_at NULLS FIRST)
                  WHERE status = 'active';
                """
            )
            await _ensure_index(
                """
                CREATE INDEX IF NOT EXISTS idx_external_product_seeds_destination_verdict
                  ON external_product_seeds (destination_verdict)
                  WHERE destination_verdict IS NOT NULL;
                """
            )
            # Convergence P1.2 (migration 176): seller-of-record on the CANONICAL
            # product row. The audit intake door writes catalog_products with no
            # external_product_seeds row, so seller identity must live on the
            # canonical row or attribution closure stamps seller_ref_missing.
            # Same column semantics + honesty guard as the 169 seed columns.
            # Railway deploys skip db/migrations/, so self-heal here.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_products
                  ADD COLUMN IF NOT EXISTS seller_ref TEXT,
                  ADD COLUMN IF NOT EXISTS seed_kind TEXT;
                """
            )
            await database.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = 'ck_catalog_products_seed_kind'
                        ) THEN
                            ALTER TABLE catalog_products
                                ADD CONSTRAINT ck_catalog_products_seed_kind
                                CHECK (seed_kind IS NULL OR seed_kind IN ('self', 'cross'));
                        END IF;
                    END $$;
                    """
                )
            )
            # GDPR/data-privacy compliance audit trail. The compliance handlers in
            # routes/webhook_routes.py write one row per Shopify compliance webhook
            # recording that the obligation was fulfilled (or flagged needs_review),
            # not just logged. Railway deploys skip db/migrations/, so self-heal here
            # (mirrors db/migrations/170_shopify_gdpr_requests.sql).
            await database.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS shopify_gdpr_requests (
                      id              BIGSERIAL PRIMARY KEY,
                      merchant_id     TEXT,
                      shop_domain     TEXT,
                      topic           TEXT NOT NULL,
                      shopify_request JSONB,
                      status          TEXT NOT NULL DEFAULT 'received',
                      resolution      JSONB,
                      received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      resolved_at     TIMESTAMPTZ
                    );
                    """
                )
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_shopify_gdpr_requests_shop_domain "
                "ON shopify_gdpr_requests (shop_domain);"
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_shopify_gdpr_requests_merchant "
                "ON shopify_gdpr_requests (merchant_id);"
            )
            # ADR-011 intake identity contract: provenance for every
            # resolve-or-attach outcome at every intake door (services/
            # intake_identity.py). Railway deploys skip db/migrations/, so
            # self-heal here (mirrors db/migrations/177_intake_identity_events.sql).
            await database.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS intake_identity_events (
                      id                BIGSERIAL PRIMARY KEY,
                      door              TEXT NOT NULL,
                      action            TEXT NOT NULL,
                      matcher           TEXT NULL,
                      merchant_id       TEXT NULL,
                      product_key       TEXT NULL,
                      content_key       TEXT NULL,
                      product_group_id  TEXT NULL,
                      evidence          JSONB NULL,
                      created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_intake_identity_events_content_key "
                "ON intake_identity_events (content_key) WHERE content_key IS NOT NULL;"
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_intake_identity_events_door_action "
                "ON intake_identity_events (door, action, created_at);"
            )
            # ADR-011 (mig 178): GTIN as a match-attribute on catalog_products
            # (NOT folded into content_key). The resolve-or-attach primitive's
            # Tier-0 GTIN matcher keys on it; every intake door persists the
            # source barcode here. Additive + nullable — behavior byte-identical
            # until populated. Railway skips db/migrations/, so self-heal here.
            await _heal_add_columns(
                "ALTER TABLE IF EXISTS catalog_products "
                "ADD COLUMN IF NOT EXISTS gtin TEXT;"
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_catalog_products_gtin "
                "ON catalog_products (gtin) WHERE gtin IS NOT NULL;"
            )
            # ADR-010 D-2 (mig 179): identity-resolution spine — proposals,
            # append-only apply/revert events, provenance columns on
            # product_group_members, and pdp_review_tasks moved into
            # migrations. Additive only; dark until the Phase-A2 engine
            # writes here (docs/plans/adr010_d2_catalog_reconciliation_
            # at_scale.md). Railway skips db/migrations/, so self-heal here.
            await database.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS identity_resolution_proposals (
                      proposal_id          TEXT PRIMARY KEY,
                      proposal_key         TEXT NOT NULL,
                      kind                 TEXT NOT NULL,
                      strategy             TEXT NOT NULL,
                      resolver_version     TEXT NOT NULL,
                      merchant_id          TEXT NULL,
                      content_key          TEXT NULL,
                      subject_product_keys TEXT[] NOT NULL,
                      keeper_product_key   TEXT NULL,
                      member_fingerprint   TEXT NOT NULL,
                      confidence           NUMERIC NULL,
                      evidence             JSONB NULL,
                      status               TEXT NOT NULL DEFAULT 'proposed',
                      run_id               TEXT NULL,
                      decided_by           TEXT NULL,
                      created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      decided_at           TIMESTAMPTZ NULL,
                      applied_at           TIMESTAMPTZ NULL
                    );
                    """
                )
            )
            await _ensure_index(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_identity_resolution_proposals_key "
                "ON identity_resolution_proposals (proposal_key);"
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_identity_resolution_proposals_status "
                "ON identity_resolution_proposals (status, strategy);"
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_identity_resolution_proposals_content_key "
                "ON identity_resolution_proposals (content_key) "
                "WHERE content_key IS NOT NULL;"
            )
            await database.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS identity_resolution_events (
                      id           BIGSERIAL PRIMARY KEY,
                      proposal_id  TEXT NULL,
                      action       TEXT NOT NULL,
                      run_id       TEXT NOT NULL,
                      detail       JSONB NULL,
                      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_identity_resolution_events_run "
                "ON identity_resolution_events (run_id);"
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_identity_resolution_events_proposal "
                "ON identity_resolution_events (proposal_id) "
                "WHERE proposal_id IS NOT NULL;"
            )
            await _heal_add_columns(
                "ALTER TABLE IF EXISTS product_group_members "
                "ADD COLUMN IF NOT EXISTS match_tier TEXT NULL, "
                "ADD COLUMN IF NOT EXISTS confidence NUMERIC NULL, "
                "ADD COLUMN IF NOT EXISTS evidence JSONB NULL, "
                "ADD COLUMN IF NOT EXISTS resolver_version TEXT NULL, "
                "ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ NULL;"
            )
            # mig 186: product REVIEW signal (aggregateRating) on the canonical
            # record + served view. catalog_products and agent_pdp_view are both
            # SELECT-*'d at runtime, so both need the self-heal or the next
            # Table.select() crashes on prod (Railway skips db/migrations/).
            # Additive + nullable; the decision-intelligence lane reads exactly
            # rating_value / rating_count. Never touches offer pricing.
            await _heal_add_columns(
                "ALTER TABLE IF EXISTS catalog_products "
                "ADD COLUMN IF NOT EXISTS rating_value NUMERIC, "
                "ADD COLUMN IF NOT EXISTS rating_count INTEGER;"
            )
            await _heal_add_columns(
                "ALTER TABLE IF EXISTS agent_pdp_view "
                "ADD COLUMN IF NOT EXISTS rating_value NUMERIC, "
                "ADD COLUMN IF NOT EXISTS rating_count INTEGER;"
            )
            # mig 263: per-currency price summary on the served view, and the
            # stamp tying it to the write that produced it
            # (services/agent_pdp_view_assembler.build_market_prices). Nullable,
            # written only while AGENT_PDP_VIEW_MARKET_PRICES is on; readers use
            # the summary only while its stamp equals refreshed_at.
            await _heal_add_columns(
                "ALTER TABLE IF EXISTS agent_pdp_view "
                "ADD COLUMN IF NOT EXISTS market_prices JSONB, "
                "ADD COLUMN IF NOT EXISTS market_prices_refreshed_at TIMESTAMPTZ;"
            )
            # mig 181: ONE canonical URL per content_key. 474 content_keys
            # carry >1 sitemap-eligible renderable sig; every sibling serves
            # identical content under a self-referential canonical tag. The
            # sitemap and the gateway's `canonical` module must name the SAME
            # winner, and the sticky answer (seeded from live sitemap
            # incumbency) has to live somewhere both can read — here. See
            # db/migrations/181_content_canonical_election.sql for the full
            # rationale. Railway skips db/migrations/, so self-heal here.
            await database.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS content_canonical_election (
                      content_key       TEXT PRIMARY KEY,
                      canonical_sig_id  TEXT NOT NULL,
                      election_reason   TEXT NOT NULL,
                      elected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            )
            await _ensure_index(
                "CREATE INDEX IF NOT EXISTS idx_content_canonical_election_sig "
                "ON content_canonical_election (canonical_sig_id);"
            )
            # ---------------------------------------------------------------
            # schema-guard-coverage backfill (migrations 103-165).
            # Railway fast-mode skips db/migrations/, so every ADD COLUMN from
            # these already-shipped migrations is mirrored here as an idempotent,
            # additive, NULLABLE self-heal (NOT NULL dropped so it can never fail
            # on existing rows). Guarded per the schema-guard-coverage CI gate.
            # ---------------------------------------------------------------
            # mig 103: merchants
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchants
                  ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT,
                  ADD COLUMN IF NOT EXISTS subscription_id BIGINT,
                  ADD COLUMN IF NOT EXISTS current_tier TEXT DEFAULT 'free',
                  ADD COLUMN IF NOT EXISTS credits_balance BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS current_period_credit_used BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS promo_period_until TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS billing_anchor_day SMALLINT DEFAULT 1;
                """
            )
            # mig 109,128: commerce_attribution_edges — monetization + gmv-channel fields
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS commerce_attribution_edges
                  ADD COLUMN IF NOT EXISTS channel_partner_id BIGINT,
                  ADD COLUMN IF NOT EXISTS take_rate_applied_bp SMALLINT,
                  ADD COLUMN IF NOT EXISTS refund_amount_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS refunded_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS protocol_name TEXT,
                  ADD COLUMN IF NOT EXISTS gmv_channel TEXT,
                  ADD COLUMN IF NOT EXISTS third_party_platform TEXT,
                  ADD COLUMN IF NOT EXISTS third_party_platform_fee_pct NUMERIC(5,4);
                """
            )
            # mig 116: agent_payouts
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS agent_payouts
                  ADD COLUMN IF NOT EXISTS payee_type TEXT DEFAULT 'agent',
                  ADD COLUMN IF NOT EXISTS payee_id BIGINT,
                  ADD COLUMN IF NOT EXISTS comp_config_version INTEGER,
                  ADD COLUMN IF NOT EXISTS snapshot_id BIGINT,
                  ADD COLUMN IF NOT EXISTS billing_run_id BIGINT,
                  ADD COLUMN IF NOT EXISTS subsidy_cap_remaining_cents BIGINT,
                  ADD COLUMN IF NOT EXISTS clawback_amount_cents BIGINT DEFAULT 0;
                """
            )
            # mig 117: credit_reservations
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS credit_reservations
                  ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb;
                """
            )
            # mig 117: credit_ledger
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS credit_ledger
                  ADD COLUMN IF NOT EXISTS source_type TEXT;
                """
            )
            # mig 118,119,129: invoices
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS invoices
                  ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS billing_run_id BIGINT,
                  ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT,
                  ADD COLUMN IF NOT EXISTS refunded_cents BIGINT DEFAULT 0;
                """
            )
            # mig 119: billing_run_items
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS billing_run_items
                  ADD COLUMN IF NOT EXISTS voided_at TIMESTAMPTZ;
                """
            )
            # mig 119: invoice_disputes
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS invoice_disputes
                  ADD COLUMN IF NOT EXISTS disputed_line_items_jsonb JSONB DEFAULT '[]'::jsonb;
                """
            )
            # mig 124: subscription_plans
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS subscription_plans
                  ADD COLUMN IF NOT EXISTS stripe_mode TEXT;
                """
            )
            # mig 125: channel_partners
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS channel_partners
                  ADD COLUMN IF NOT EXISTS term_start_date DATE,
                  ADD COLUMN IF NOT EXISTS term_months INTEGER DEFAULT 12,
                  ADD COLUMN IF NOT EXISTS term_auto_renew BOOLEAN DEFAULT TRUE,
                  ADD COLUMN IF NOT EXISTS per_brand_tail_months INTEGER DEFAULT 36,
                  ADD COLUMN IF NOT EXISTS churn_clawback_days INTEGER DEFAULT 90,
                  ADD COLUMN IF NOT EXISTS nonpayment_clawback_days INTEGER DEFAULT 60,
                  ADD COLUMN IF NOT EXISTS per_brand_subsidy_cap_cents BIGINT,
                  ADD COLUMN IF NOT EXISTS gmv_take_rate_bp INTEGER DEFAULT 1000,
                  ADD COLUMN IF NOT EXISTS active_rate_scope TEXT DEFAULT 'B',
                  ADD COLUMN IF NOT EXISTS gmv_take_definition TEXT DEFAULT 'net',
                  ADD COLUMN IF NOT EXISTS prepaid_credits_supported BOOLEAN DEFAULT TRUE,
                  ADD COLUMN IF NOT EXISTS monthly_overage_supported BOOLEAN DEFAULT TRUE;
                """
            )
            # mig 127: monthly_brand_statements
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS monthly_brand_statements
                  ADD COLUMN IF NOT EXISTS merchant_id VARCHAR(50),
                  ADD COLUMN IF NOT EXISTS calendar_month DATE,
                  ADD COLUMN IF NOT EXISTS subscription_plan_id BIGINT,
                  ADD COLUMN IF NOT EXISTS tier_name TEXT,
                  ADD COLUMN IF NOT EXISTS subscription_revenue_usd_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS credits_consumed BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS bundled_credits_consumed BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS overage_credits BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS overage_revenue_usd_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS gmv_usd_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS gmv_personal_usd_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS gmv_third_party_usd_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS pivota_gmv_take_usd_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS total_revenue_usd_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS total_cogs_usd_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS pivota_gross_margin_usd_cents BIGINT DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'open',
                  ADD COLUMN IF NOT EXISTS frozen_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS invoiced_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS overage_invoice_id BIGINT,
                  ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb,
                  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
                  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
                """
            )
            # mig 134: partner_invite_tokens
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS partner_invite_tokens
                  ADD COLUMN IF NOT EXISTS channel_partner_id BIGINT,
                  ADD COLUMN IF NOT EXISTS token_hash TEXT,
                  ADD COLUMN IF NOT EXISTS token_prefix TEXT,
                  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active',
                  ADD COLUMN IF NOT EXISTS issued_by TEXT,
                  ADD COLUMN IF NOT EXISTS notes TEXT,
                  ADD COLUMN IF NOT EXISTS consumed_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS consumed_by_merchant_id VARCHAR(50),
                  ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS revoked_by TEXT,
                  ADD COLUMN IF NOT EXISTS revoked_reason TEXT,
                  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
                  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
                """
            )
            # mig 135,151,161: catalog_products
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_products
                  ADD COLUMN IF NOT EXISTS suppression_reason TEXT,
                  ADD COLUMN IF NOT EXISTS suppressed_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS suppression_metadata JSONB,
                  ADD COLUMN IF NOT EXISTS category_kind VARCHAR(16),
                  ADD COLUMN IF NOT EXISTS claim_state VARCHAR(16) DEFAULT 'unclaimed';
                """
            )
            # mig 135: catalog_skus
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_skus
                  ADD COLUMN IF NOT EXISTS suppression_reason TEXT,
                  ADD COLUMN IF NOT EXISTS suppressed_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS suppression_metadata JSONB;
                """
            )
            # mig 135,149: catalog_offers
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS catalog_offers
                  ADD COLUMN IF NOT EXISTS suppression_metadata JSONB,
                  ADD COLUMN IF NOT EXISTS offer_type VARCHAR(16),
                  ADD COLUMN IF NOT EXISTS market VARCHAR(8) DEFAULT 'US',
                  ADD COLUMN IF NOT EXISTS is_first_party BOOLEAN DEFAULT FALSE,
                  ADD COLUMN IF NOT EXISTS why_buy_direct TEXT;
                """
            )
            # mig 145: agent_decision_events
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS agent_decision_events
                  ADD COLUMN IF NOT EXISTS protocol VARCHAR(32) DEFAULT 'pdp_direct';
                """
            )
            # mig 145: checkout_decisions
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS checkout_decisions
                  ADD COLUMN IF NOT EXISTS protocol VARCHAR(32) DEFAULT 'pdp_direct';
                """
            )
            # mig 145: agent_decision_funnel_links
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS agent_decision_funnel_links
                  ADD COLUMN IF NOT EXISTS protocol VARCHAR(32) DEFAULT 'pdp_direct';
                """
            )
            # mig 146,158: citation_targets
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS citation_targets
                  ADD COLUMN IF NOT EXISTS merchant_brand TEXT,
                  ADD COLUMN IF NOT EXISTS merchant_host TEXT,
                  ADD COLUMN IF NOT EXISTS content_key VARCHAR(40);
                """
            )
            # mig 150: beauty_product_profiles
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS beauty_product_profiles
                  ADD COLUMN IF NOT EXISTS evidence_profile JSONB,
                  ADD COLUMN IF NOT EXISTS required_disclaimers JSONB;
                """
            )
            # mig 152,162: agent_pdp_view
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS agent_pdp_view
                  ADD COLUMN IF NOT EXISTS evidence_profile JSONB,
                  ADD COLUMN IF NOT EXISTS required_disclaimers JSONB,
                  ADD COLUMN IF NOT EXISTS bullet_points jsonb,
                  ADD COLUMN IF NOT EXISTS usage_scenarios jsonb;
                """
            )
            # mig 156: merchant_stores
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_stores
                  ADD COLUMN IF NOT EXISTS content_writeback_status TEXT DEFAULT 'disabled',
                  ADD COLUMN IF NOT EXISTS content_writeback_enabled_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS content_writeback_canary_product_id TEXT,
                  ADD COLUMN IF NOT EXISTS content_writeback_last_canary_product_id TEXT,
                  ADD COLUMN IF NOT EXISTS content_writeback_last_written_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS content_writeback_last_error TEXT;
                """
            )
            # mig 158: merchant_audit_runs
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_audit_runs
                  ADD COLUMN IF NOT EXISTS content_keys TEXT[],
                  ADD COLUMN IF NOT EXISTS content_key_basis JSONB;
                """
            )
            # mig 158: evidence_items
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS evidence_items
                  ADD COLUMN IF NOT EXISTS content_key VARCHAR(40);
                """
            )
            # mig 158: readiness_findings
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS readiness_findings
                  ADD COLUMN IF NOT EXISTS content_key VARCHAR(40);
                """
            )
            # mig 158: niche_target_outcomes
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS niche_target_outcomes
                  ADD COLUMN IF NOT EXISTS content_key VARCHAR(40);
                """
            )
            # mig 158: citation_scan_runs
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS citation_scan_runs
                  ADD COLUMN IF NOT EXISTS content_key VARCHAR(40);
                """
            )
            # mig 165: index_pipeline_state
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS index_pipeline_state
                  ADD COLUMN IF NOT EXISTS index_eligible BOOLEAN DEFAULT FALSE;
                """
            )
            # mig 190: merchant_stores upstream-probe bookkeeping. Emitted near
            # the END of the chain on purpose: these four columns are in
            # REQUIRED_SCHEMA, so if an earlier ALTER in this single try-block
            # raises, /health fails closed (503) on columns that were never
            # reached. Later position = fewer statements that can starve it.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_stores
                  ADD COLUMN IF NOT EXISTS upstream_probe_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS upstream_probe_status TEXT,
                  ADD COLUMN IF NOT EXISTS upstream_probe_http_status INTEGER,
                  ADD COLUMN IF NOT EXISTS upstream_probe_failures INTEGER NOT NULL DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
                """
            )
            # mig 196: Store Audit route evidence is read through SQLAlchemy
            # Tables at runtime. Production deploys do not run migrations, so
            # keep the column subset used by the route/evidence workers alive.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS execution_routes
                  ADD COLUMN IF NOT EXISTS last_audit_run_id UUID;
                ALTER TABLE IF EXISTS evidence_items
                  ADD COLUMN IF NOT EXISTS execution_route_id UUID,
                  ADD COLUMN IF NOT EXISTS evidence_level TEXT,
                  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
                ALTER TABLE IF EXISTS verification_runs
                  ADD COLUMN IF NOT EXISTS execution_route_id UUID;
                """
            )
            # mig 202: Reap webhook reconciliation columns on agent_issued_cards. The
            # webhook path UPDATEs these at runtime; a deploy that skipped migrations
            # would 500 on the first issuer report. reap_webhook_events itself is
            # CREATE TABLE IF NOT EXISTS in the migration and the table is only
            # touched via explicit SQL here, but the COLUMNS ride on a table born in
            # mig 201, so self-heal them the same way.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS agent_issued_cards
                  ADD COLUMN IF NOT EXISTS last_auth_at TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS auth_count INTEGER NOT NULL DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS settled_amount_minor BIGINT;
                """
            )
            # mig 207: merchant_order_sync_jobs.progress — the durable queue for
            # post-payment merchant-order sync. The TABLE is born in mig 207 and
            # also self-heals via
            # db/merchant_order_sync_jobs.ensure_merchant_order_sync_jobs_table(),
            # which every accessor calls; this heals the one column added after
            # that file's first cut, since CREATE TABLE IF NOT EXISTS is a no-op
            # on a table that already exists. ALTER ... IF EXISTS so it no-ops
            # harmlessly wherever the table has not been created yet.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS merchant_order_sync_jobs
                  ADD COLUMN IF NOT EXISTS progress TEXT;
                """
            )
            # mig 209: citation_observations.destination_rank +
            # .is_primary_destination — B3's "where did the answer send the
            # buyer?" columns. The audit deposit path INSERTs both on every
            # citation it writes, and it builds that INSERT from the SQLAlchemy
            # Table, so a deploy that skipped migrations would name a column the
            # database does not have and fail every citation write in the run.
            # db/audit_evidence.py carries the same two ALTERs in its own inline
            # backstop; this is the startup half of the pair.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS citation_observations
                  ADD COLUMN IF NOT EXISTS destination_rank INTEGER,
                  ADD COLUMN IF NOT EXISTS is_primary_destination BOOLEAN NOT NULL DEFAULT FALSE;
                """
            )
            # mig 109: commerce_attribution_edges.net_attributed_gmv_cents — STORED generated column
            # (derived from refund_amount_cents, added above). Emitted LAST so this
            # lone potential table-rewrite can't block the lightweight self-heals;
            # in prod the column already exists so IF NOT EXISTS no-ops.
            await _heal_add_columns(
                """
                ALTER TABLE IF EXISTS commerce_attribution_edges
                  ADD COLUMN IF NOT EXISTS net_attributed_gmv_cents BIGINT GENERATED ALWAYS AS ( CASE WHEN gross_attributed_gmv_cents IS NULL THEN NULL ELSE GREATEST(gross_attributed_gmv_cents - refund_amount_cents, 0) END ) STORED;
                """
            )

            return

        if IS_SQLITE:
            # mig 231: the merchant purchasability fact, SQLite twin.
            #
            # NOT byte-identical to the Postgres DDL, and the differences are the
            # ones this branch always makes: TIMESTAMPTZ -> TIMESTAMP, JSONB ->
            # TEXT (SQLite has no JSON affinity; the module encodes and decodes),
            # and the regex CHECK -> the length/upper pair SQLite can evaluate,
            # exactly as the mig-226 eligibility twin below writes it.
            #
            # SQL-ONLY, no SQLAlchemy Table, for the reason the mig-224 twin gives:
            # a `Table` would make the model the source of truth because
            # `metadata.create_all` runs BEFORE db/migrations in main.py, and then
            # the SQLite half of this rail's tests would be testing a schema that
            # exists nowhere. Its own try, so it cannot starve what follows.
            try:
                await database.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS merchant_purchasability (
                            merchant_domain VARCHAR(255) NOT NULL,
                            market_country VARCHAR(2) NOT NULL
                                CONSTRAINT ck_merchant_purchasability_market
                                CHECK (length(market_country) = 2
                                       AND market_country = upper(market_country)),
                            vantage VARCHAR(32) NOT NULL,
                            checked_at TIMESTAMP,
                            verdict VARCHAR(32),
                            card_available BOOLEAN,
                            payment_methods TEXT,
                            landed_price_minor BIGINT,
                            landed_currency VARCHAR(8),
                            expected_price_minor BIGINT,
                            price_drift_minor BIGINT,
                            variant_id VARCHAR(32),
                            evidence TEXT,
                            consecutive_failures INTEGER NOT NULL DEFAULT 0,
                            positive_until TIMESTAMP,
                            PRIMARY KEY (merchant_domain, market_country, vantage)
                        );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 231, second statement: the due-list index, SQLite twin. Its own
            # try, same reason as the Postgres half.
            try:
                await database.execute(
                    text(
                        """
                        CREATE INDEX IF NOT EXISTS idx_merchant_purchasability_due
                            ON merchant_purchasability (checked_at);
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 232, SQLite twin: THERE IS NO STATEMENT HERE, AND THAT IS THE ANSWER.
            #
            # SQLite cannot alter a CHECK in place. There is no DROP CONSTRAINT and no ADD
            # CONSTRAINT; widening one means rebuilding the table (create-new, copy, drop, rename)
            # under `PRAGMA legacy_alter_table`, and doing that here — best-effort, inside a try
            # that swallows everything, against a table another statement in this same branch may
            # have just created — risks losing the rows on a partial failure. That trade is not
            # worth taking for this table.
            #
            # IT IS SAFE TO OMIT because of where SQLite is used: dev machines and the test suite,
            # both of which build the schema from scratch. `ensure_schema` below runs the
            # `CREATE TABLE IF NOT EXISTS` in db/tierb_cart_link_eligibility_schema.py, whose
            # verdict list ALREADY carries NO_CARD_PAYMENT and PRICE_DRIFT, so every SQLite
            # database born after this change has the wide vocabulary. Only a SQLite file that
            # predates it keeps the narrow one, and the fix for that file is to delete it.
            #
            # Production is Postgres, and the Postgres branch above does heal it in place.
            # mig 228: Tier B cart-link eligibility, SQLite twin — the same
            # function as the Postgres branch; it picks the dialect's DDL.
            # SQL-only (no SQLAlchemy Table) for the reason the mig-224 block
            # below gives. Its own try, so it cannot starve what follows.
            try:
                from db.tierb_cart_link_eligibility_schema import (
                    ensure_schema as _ensure_tierb_cart_link_eligibility,
                )

                await _ensure_tierb_cart_link_eligibility()
            except Exception:  # noqa: BLE001
                pass
            # mig 224: the Reap AGENTIC rail's two tables, SQLite twin.
            #
            # THE SQLITE BRANCH HAS ONLY EVER DONE `ADD COLUMN` UNTIL NOW, and
            # that is not an oversight to copy: every other table reached from a
            # SQLite database is either in `metadata` (so create_all builds it)
            # or is created on demand by its own module's `_ensure_*_table`.
            # These two are neither. They are SQL-ONLY on purpose — a SQLAlchemy
            # `Table` would make the model the source of truth, because
            # `metadata.create_all` runs BEFORE db/migrations in main.py — so
            # without this block a dev or test SQLite database never gets them
            # at all, and the SQLite half of this rail's tests would be testing
            # a schema that exists nowhere.
            #
            # WHY IT IS NOT BYTE-IDENTICAL TO THE POSTGRES DDL, unlike the two
            # halves of the mig-224 self-heal above: `now()` is not a SQLite
            # function (CURRENT_TIMESTAMP is), and JSONB is not a SQLite type
            # name — declaring it would give the column NUMERIC affinity, which
            # is the same trap as `CAST(:x AS JSONB)`. TIMESTAMPTZ -> TIMESTAMP
            # follows the convention the `sqlite_type` map below already states.
            # Everything that carries MEANING — the CHECK vocabularies, the
            # NOT NULLs, the partial unique indexes — is identical, because
            # those are what the tests and the invariants rest on.
            #
            # Its own try/except, same contract as the Postgres siblings: a
            # failure here must not starve the ADD COLUMN heals below.
            try:
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_enrollments (
                        id VARCHAR(64) PRIMARY KEY,
                        buyer_ref VARCHAR(128) NOT NULL,
                        agent_id VARCHAR(128),
                        reap_enrollment_id VARCHAR(128),
                        status VARCHAR(16) NOT NULL
                            CHECK (status IN ('pending', 'active', 'dead')),
                        reap_status VARCHAR(64),
                        card_network VARCHAR(32),
                        card_last4 VARCHAR(4)
                            CHECK (card_last4 IS NULL OR length(card_last4) = 4),
                        hosted_url TEXT,
                        hosted_url_expires_at TIMESTAMP,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                        """
                    )
                )
                await database.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS "
                        "idx_reap_agentic_enrollments_buyer_status "
                        "ON reap_agentic_enrollments (buyer_ref, status);"
                    )
                )
                await database.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS "
                        "uq_reap_agentic_enrollments_reap_id "
                        "ON reap_agentic_enrollments (reap_enrollment_id) "
                        "WHERE reap_enrollment_id IS NOT NULL;"
                    )
                )
                await database.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS "
                        "uq_reap_agentic_enrollments_one_active "
                        "ON reap_agentic_enrollments (buyer_ref) "
                        "WHERE status = 'active';"
                    )
                )
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_purchases (
                        id VARCHAR(64) PRIMARY KEY,
                        buyer_ref VARCHAR(128) NOT NULL,
                        agent_id VARCHAR(128),
                        agent_user_ref_hash VARCHAR(64),
                        enrollment_id VARCHAR(64),
                        state VARCHAR(24) NOT NULL CHECK (state IN (
                            'resolving', 'needs_enrollment', 'quoting', 'awaiting_approval',
                            'processing', 'completed', 'failed', 'refused', 'expired'
                        )),
                        -- The clock the poll loop cannot reset: stamped at create and
                        -- only by a statement that CHANGES state. `updated_at` is
                        -- written by every claim/release/requeue, so an absolute
                        -- waiting deadline measured from it never fires.
                        state_entered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        merchant_domain VARCHAR(255),
                        product_key TEXT,
                        variant_key TEXT,
                        product_name TEXT,
                        variant_title TEXT,
                        brand TEXT,
                        category TEXT,
                        quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
                        currency VARCHAR(8),
                        our_price_minor BIGINT,
                        click_id VARCHAR(128),
                        return_url TEXT,
                        reap_product_id VARCHAR(128),
                        reap_variant_id VARCHAR(128),
                        reap_quote_id VARCHAR(128),
                        reap_quote_expires_at TIMESTAMP,
                        reap_checkout_id VARCHAR(128),
                        reap_order_id VARCHAR(128),
                        quoted_total_minor BIGINT,
                        final_total_minor BIGINT,
                        shipping_minor BIGINT,
                        tax_minor BIGINT,
                        hosted_url TEXT,
                        hosted_url_expires_at TIMESTAMP,
                        refusal_reason VARCHAR(64),
                        queries_tried TEXT,
                        last_error_code VARCHAR(64),
                        attempts INTEGER NOT NULL DEFAULT 0,
                        claimed_by VARCHAR(128),
                        claimed_at TIMESTAMP,
                        next_poll_at TIMESTAMP,
                        shipping_address TEXT,
                        buyer_email TEXT,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        terminal_at TIMESTAMP
                    );
                        """
                    )
                )
                await database.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS "
                        "idx_reap_agentic_purchases_state_poll "
                        "ON reap_agentic_purchases (state, next_poll_at);"
                    )
                )
                await database.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS "
                        "idx_reap_agentic_purchases_buyer_created "
                        "ON reap_agentic_purchases (buyer_ref, created_at);"
                    )
                )
                await database.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS "
                        "idx_reap_agentic_purchases_owner_created "
                        "ON reap_agentic_purchases "
                        "(agent_id, agent_user_ref_hash, created_at);"
                    )
                )
                await database.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS "
                        "uq_reap_agentic_purchases_checkout "
                        "ON reap_agentic_purchases (reap_checkout_id) "
                        "WHERE reap_checkout_id IS NOT NULL;"
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 255 and mig 253: the operator audit tables
            # (reap_unopened_attempt_retirements, reap_checkout_manual_resolution_audit).
            # EACH IN ITS OWN try, AFTER the mig-224 block. They used to sit inside it,
            # between the enrollments and the purchases DDL: a failure in either abandoned
            # the purchases table, and a failure in the enrollment indexes abandoned both
            # audit tables, which the operator decisions refuse to run without. Neither
            # depends on the other or on the mig-224 tables. DDL text unchanged. SQLite twin.
            try:
                await database.execute(text("""CREATE TABLE IF NOT EXISTS reap_unopened_attempt_retirements (
    receipt_id VARCHAR(32) PRIMARY KEY,
    agent_id VARCHAR(128) NOT NULL,
    agent_user_ref_hash VARCHAR(64) NOT NULL,
    native_key VARCHAR(128) NOT NULL,
    cart_key VARCHAR(128) NOT NULL,
    native_request_hash VARCHAR(64) NOT NULL,
    cart_request_hash VARCHAR(64) NOT NULL,
    authority_sha256 VARCHAR(64) NOT NULL,
    evidence_sha256 VARCHAR(64) NOT NULL,
    operator_ref VARCHAR(128) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (agent_id, agent_user_ref_hash, native_key, cart_key),
    CHECK (native_key <> cart_key)
);
"""))
            except Exception as exc:  # noqa: BLE001
                logger.warning("schema_guard: reap_unopened_attempt_retirements (mig 255) unavailable (%s)",
                               type(exc).__name__)
            try:
                await database.execute(text("""CREATE TABLE IF NOT EXISTS reap_checkout_manual_resolution_audit (
    purchase_id VARCHAR(64) PRIMARY KEY,
    reap_checkout_id VARCHAR(128) NOT NULL,
    from_state VARCHAR(32) NOT NULL,
    resolved_state VARCHAR(32) NOT NULL,
    attribution_outcome VARCHAR(32) NOT NULL,
    operator_ref VARCHAR(128) NOT NULL,
    evidence_source VARCHAR(64) NOT NULL,
    evidence_reference VARCHAR(128) NOT NULL,
    evidence_sha256 VARCHAR(64) NOT NULL,
    expected_updated_at TIMESTAMP NOT NULL,
    evidence_observed_at TIMESTAMP NOT NULL,
    provider_base_url VARCHAR(255) NOT NULL,
    provider_status VARCHAR(32) NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (from_state IN ('awaiting_approval','processing')),
    CHECK (resolved_state IN ('completed','failed','expired')),
    CHECK (provider_status IN ('COMPLETED','FAILED','EXPIRED')),
    CHECK (attribution_outcome IN ('edge_closed','closed_by_other_channel','not_applicable')),
    CHECK (evidence_source IN ('authenticated_reap_checkout_read','verified_reap_support_statement'))
);"""))
            except Exception as exc:  # noqa: BLE001
                logger.warning("schema_guard: reap_checkout_manual_resolution_audit (mig 253) unavailable (%s)",
                               type(exc).__name__)
            # mig 256: privacy clocks and durable dispatch fence, independent of enrollment indexes.
            try:
                from db.reap_continuation import ensure_continuation_schema
                await ensure_continuation_schema()
            except Exception as exc:
                logger.warning("schema_guard: Reap continuation schema unavailable (%s)", type(exc).__name__)
            # mig 257: reap_checkout_dispatch_resolution_audit, the parked-dispatch decision
            # audit, in its OWN try as well. `ensure_continuation_schema` creates it LAST, after
            # the column adds, the journal, its widening and its triggers, so any of those
            # failing skipped it. Re-issued here on its own (CREATE TABLE IF NOT EXISTS, a no-op
            # when the call above succeeded), with the module's own DDL text. SQLite twin: the
            # same TIMESTAMPTZ -> TIMESTAMP spelling the module applies.
            try:
                from db.reap_continuation import _RESOLUTION_AUDIT
                await database.execute(_RESOLUTION_AUDIT.replace('TIMESTAMPTZ', 'TIMESTAMP'))
            except Exception as exc:  # noqa: BLE001
                logger.warning("schema_guard: reap_checkout_dispatch_resolution_audit (mig 257) unavailable (%s)",
                               type(exc).__name__)
            # mig 258: the price witness (preflight quote + corroborated price change). Its own
            # try: the columns are read and written only behind dark dials, and a failure here must
            # not starve what follows.
            try:
                from db.reap_price_witness import ensure_price_witness_schema
                await ensure_price_witness_schema()
            except Exception as exc:
                logger.warning("schema_guard: Reap price witness schema unavailable (%s)", type(exc).__name__)
            # mig 252: at most one PENDING enrollment per buyer, SQLite twin of
            # the Postgres block above (same index, same reason, same own try:
            # it fails on a database that already holds two pending rows for one
            # buyer_ref, and must not starve what follows).
            try:
                await database.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS "
                        "uq_reap_agentic_enrollments_one_pending "
                        "ON reap_agentic_enrollments (buyer_ref) "
                        "WHERE status = 'pending';"
                    )
                )
            except Exception as exc:  # noqa: BLE001
                _warn_one_pending_index_missing(exc)
            # mig 254: SQLite twin; duplicate column is the idempotent no-op.
            try:
                await database.execute(text("ALTER TABLE reap_agentic_enrollments ADD COLUMN hosted_url_expiry_invalid BOOLEAN NOT NULL DEFAULT FALSE;"))
            except Exception:  # noqa: BLE001
                pass
            # mig 225: the resolver's three hint columns, SQLite twin.
            #
            # ── TWO LAYERS OF try, AND EACH ONE ANSWERS A DIFFERENT FAILURE ──
            #
            # OUTER (here): this loop is OUTSIDE the mig-224 try above, for the
            # reason the Postgres sibling spells out at length — every statement
            # in that try can raise, one of them raises on databases that exist,
            # and a raise there used to abandon these three columns forever on
            # exactly the databases that were already unwell.
            #
            # INNER (per column): SQLite's ALTER TABLE has NO `IF NOT EXISTS`
            # for ADD COLUMN and NO multi-clause ADD, so the Postgres statement
            # cannot be reused verbatim. A duplicate column raises
            # OperationalError("duplicate column name: …"), which is the no-op
            # this branch has to spell with an except — and a SHARED try would
            # make the FIRST already-present column abandon the two after it,
            # leaving a partially-healed table permanently short. Per column,
            # every run converges.
            #
            # The statement here cannot carry `IF EXISTS` on the table name
            # (SQLite has no such form), so on a database where the CREATE above
            # genuinely failed each one raises "no such table" and the inner
            # except absorbs it — the same no-op, reached by a different error.
            #
            # JSONB IS NOT A SQLITE TYPE NAME: declaring it would give the column
            # NUMERIC affinity and silently store 0 for a JSON payload, which is
            # the same trap as `CAST(:x AS JSONB)` — see property 3 in
            # db/reap_agentic_ledger.py's header. TEXT, like the queries_tried /
            # shipping_address twins above.
            try:
                for _hint_column, _hint_type in (
                    ("accept_variant_labels", "TEXT"),
                    ("also_accept_domains", "TEXT"),
                    ("market_country", "TEXT"),
                ):
                    try:
                        await database.execute(
                            text(
                                f"ALTER TABLE reap_agentic_purchases "
                                f"ADD COLUMN {_hint_column} {_hint_type};"
                            )
                        )
                    except Exception:  # noqa: BLE001
                        # Almost always "duplicate column name" — the column is
                        # already there and this run had nothing to do. Continue
                        # so the remaining columns still get their chance.
                        continue
            except Exception:  # noqa: BLE001
                pass
            # mig 226, SQLite twin: eligibility, the per-buyer opaque ref, and
            # the idempotency keys. Same argument as the mig-224 twin above for
            # why these are here at all — they are SQL-only tables, absent from
            # `metadata`, so without this block a dev or test SQLite database
            # never gets them and the SQLite arm of the route tests would be
            # testing a schema that exists nowhere.
            #
            # THREE DELIBERATE DIFFERENCES FROM THE POSTGRES DDL, and no others:
            #   now()        -> CURRENT_TIMESTAMP   (not a SQLite function)
            #   TIMESTAMPTZ  -> TIMESTAMP           (the convention the
            #                                        `sqlite_type` map below
            #                                        already states)
            #   JSONB        -> TEXT                (JSONB is not a SQLite type
            #                                        name; declaring it gives the
            #                                        column NUMERIC affinity and
            #                                        silently stores 0 for a JSON
            #                                        payload — the same trap as
            #                                        `CAST(:x AS JSONB)`)
            #
            # The `market_country` CHECK is the fourth difference and the only
            # one that is not a type name. Postgres spells it with the regex
            # operator, which SQLite does not have; the twin spells the same rule
            # as a length-and-case test. Neither is the authority — the route
            # uppercases and regex-checks the value before it is ever bound, and
            # db/reap_agentic_ledger._require_country checks it again on the way
            # into the purchase row. These CHECKs are the third line, kept
            # because a table that can hold `usa` is a table somebody will
            # eventually put `usa` in by hand.
            #
            # Everything that carries MEANING is identical: the PRIMARY KEYs, the
            # NOT NULLs, the `enabled` default of FALSE (a half-configured row is
            # not an authorization to spend), and the UNIQUE on reap_buyer_ref.
            #
            # Its own try/except, same contract as its Postgres sibling and as
            # the mig-224 twin above: a failure here must not starve the ADD
            # COLUMN heals below.
            # ONE try PER STATEMENT, NOT ONE PER BLOCK. The first cut of this
            # block shared a single try across all four statements, and its own
            # comment named `CREATE UNIQUE INDEX uq_reap_agentic_buyer_refs_ref`
            # as the realistic failure — on a database that already holds two
            # buyers with one ref — while `CREATE TABLE
            # reap_agentic_purchase_keys` sat AFTER it in the same try. That is
            # exactly the mig-224/225 defect this file already carries a long
            # comment about: the raise abandons every statement after it, so the
            # keys table would never land, on precisely the databases that were
            # already unwell, and production has no other route to it.
            #
            # Per statement, every arrival converges: each one is independently
            # a no-op when its object exists, and a failure of any one cannot
            # starve the others or the self-heals below.
            try:
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_eligibility (
                        merchant_domain VARCHAR(255) NOT NULL,
                        product_key TEXT NOT NULL DEFAULT '',
                        variant_key TEXT NOT NULL DEFAULT '',
                        market_country VARCHAR(2) NOT NULL
                            CHECK (length(market_country) = 2
                                   AND market_country = upper(market_country)),
                        enabled BOOLEAN NOT NULL DEFAULT FALSE,
                        accept_variant_labels TEXT,
                        also_accept_domains TEXT,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (merchant_domain, market_country, product_key, variant_key)
                    );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            try:
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_buyer_refs (
                        buyer_id VARCHAR(50) PRIMARY KEY,
                        reap_buyer_ref VARCHAR(128) NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 227, SQLite twin: the consent tag, one statement per column.
            #
            # PER COLUMN, for the reason the mig-225 twin above spells out:
            # SQLite's ADD COLUMN has no `IF NOT EXISTS` and no multi-clause
            # form, so a duplicate column raises and a SHARED try would let the
            # first already-present column abandon the second. Per column,
            # every run converges on the same table.
            #
            # TIMESTAMP, NOT TIMESTAMPTZ: SQLite has no such type name, and the
            # column is written with a bound aware datetime either way.
            #
            # NOT FOLDED INTO THE CREATE ABOVE — same argument as the Postgres
            # twin: folded in, it would be dead code on the only databases CI
            # builds, and its deletion would pass every test.
            try:
                for _consent_column, _consent_type in (
                    ("consent_version", "VARCHAR(32)"),
                    ("consented_at", "TIMESTAMP"),
                ):
                    try:
                        await database.execute(
                            text(
                                f"ALTER TABLE reap_agentic_buyer_refs "
                                f"ADD COLUMN {_consent_column} {_consent_type};"
                            )
                        )
                    except Exception:  # noqa: BLE001
                        # Almost always "duplicate column name" — the column is
                        # already there and this run had nothing to do.
                        continue
            except Exception:  # noqa: BLE001
                pass
            try:
                # THE STATEMENT THAT ACTUALLY FAILS IN THE FIELD. It cannot be
                # created on a database that already holds two buyers sharing
                # one ref, and that is the whole reason it has its own try.
                await database.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS "
                        "uq_reap_agentic_buyer_refs_ref "
                        "ON reap_agentic_buyer_refs (reap_buyer_ref);"
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            try:
                await database.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS reap_agentic_purchase_keys (
                        agent_id VARCHAR(128) NOT NULL,
                        agent_user_ref_hash VARCHAR(64) NOT NULL,
                        idempotency_key VARCHAR(128) NOT NULL,
                        purchase_id VARCHAR(64) NOT NULL,
                        request_hash VARCHAR(64) NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (agent_id, agent_user_ref_hash, idempotency_key)
                    );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 229: item_source + cart_url, SQLite twin.
            #
            # Same two layers of try as the mig-225 twin above, for the same
            # reasons: SQLite has no `IF NOT EXISTS` on ADD COLUMN and no
            # multi-clause ADD, so each column is its own statement and a
            # "duplicate column name" on one must not abandon the other.
            #
            # THE CHECKS ARE COLUMN CONSTRAINTS HERE TOO, byte-for-byte the
            # Postgres ones, and SQLite enforces a column CHECK that names
            # another column exactly as a table CHECK. ORDER MATTERS: item_source
            # first, because cart_url's pairing CHECK names it. Since SQLite
            # 3.37 an ADD COLUMN's CHECK is tested against the existing rows;
            # every existing row takes the DEFAULT 'reap_variant' and a NULL
            # URL, which the pairing admits.
            try:
                for _source_column, _source_decl in (
                    (
                        "item_source",
                        "TEXT NOT NULL DEFAULT 'reap_variant' "
                        "CONSTRAINT ck_reap_agentic_purchases_item_source "
                        "CHECK (item_source IN ('reap_variant', 'cart_link'))",
                    ),
                    (
                        "cart_url",
                        "TEXT "
                        "CONSTRAINT ck_reap_agentic_purchases_cart_url_pairing "
                        "CHECK ("
                        "(item_source = 'reap_variant' AND cart_url IS NULL) "
                        "OR (item_source = 'cart_link' AND cart_url IS NOT NULL))",
                    ),
                ):
                    try:
                        await database.execute(
                            text(
                                f"ALTER TABLE reap_agentic_purchases "
                                f"ADD COLUMN {_source_column} {_source_decl};"
                            )
                        )
                    except Exception:  # noqa: BLE001
                        continue
            except Exception:  # noqa: BLE001
                pass
            # mig 229, second statement, SQLite twin: one cart-link purchase per click.
            # SQLite supports the same partial unique index verbatim. Own try, same reason.
            try:
                await database.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS "
                        "uq_reap_agentic_purchases_cart_link_click "
                        "ON reap_agentic_purchases (click_id) "
                        "WHERE item_source = 'cart_link';"
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 233, SQLite twin: the consent in force when the purchase was
            # opened, one statement per column.
            #
            # PER COLUMN, for the reason the mig-225 and mig-227 twins above spell
            # out: SQLite's ADD COLUMN has no `IF NOT EXISTS` and no multi-clause
            # form, so a duplicate column raises OperationalError and a SHARED try
            # would let the first already-present column abandon the second —
            # leaving a table permanently short of `consented_at` on exactly the
            # databases a previous run half-healed. Per column, every arrival
            # converges on the same table.
            #
            # TIMESTAMP, NOT TIMESTAMPTZ: SQLite has no such type name, and the
            # column is written through `_bind_dt`, which hands SQLite the
            # server's own text format either way.
            #
            # THE COVERAGE GATE CANNOT SEE THIS ONE. The column name is an
            # f-string placeholder, and tests/test_schema_guard_migration_
            # coverage.py reads SOURCE TEXT — so deleting this loop would not turn
            # that gate red. What defends it is the runtime suite: every consent
            # assertion in tests/test_reap_agentic_ledger.py builds its schema
            # through this self-heal and fails without these columns. That is the
            # check to keep working, exactly as the mig-227 twin says of its own.
            try:
                for _purchase_consent_column, _purchase_consent_type in (
                    ("consent_version", "VARCHAR(32)"),
                    ("consented_at", "TIMESTAMP"),
                ):
                    try:
                        await database.execute(
                            text(
                                f"ALTER TABLE reap_agentic_purchases "
                                f"ADD COLUMN {_purchase_consent_column} "
                                f"{_purchase_consent_type};"
                            )
                        )
                    except Exception:  # noqa: BLE001
                        # Almost always "duplicate column name" — the column is
                        # already there and this run had nothing to do. Continue
                        # so the remaining column still gets its chance.
                        continue
            except Exception:  # noqa: BLE001
                pass
            # mig 247, SQLite twin: the offer-code columns, one statement per
            # column for the reason the mig-233 twin above states. Invisible to
            # the source-text coverage gate for the same reason; the runtime
            # suite (every offer-code assertion in the SQLite ledger/purchase
            # tests builds through this heal) is what defends it.
            try:
                for _offer_code_column, _offer_code_type in (
                    ("offer_code", "VARCHAR(128)"),
                    ("offer_code_outcome", "VARCHAR(16)"),
                    ("discount_minor", "BIGINT"),
                    ("tax_included", "BOOLEAN"),
                ):
                    try:
                        await database.execute(
                            text(
                                f"ALTER TABLE reap_agentic_purchases "
                                f"ADD COLUMN {_offer_code_column} "
                                f"{_offer_code_type};"
                            )
                        )
                    except Exception:  # noqa: BLE001
                        continue
            except Exception:  # noqa: BLE001
                pass
            # mig 230: the per-click attribution claim, SQLite twin. Same CHECK and
            # key as the Postgres statement; TIMESTAMPTZ -> TIMESTAMP and now() ->
            # CURRENT_TIMESTAMP per this branch's convention.
            try:
                await database.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS conversion_click_claims (
                            click_id TEXT PRIMARY KEY,
                            claimed_by TEXT NOT NULL
                                CONSTRAINT ck_conversion_click_claims_claimed_by
                                CHECK (claimed_by IN ('reap_agentic', 'merchant_order')),
                            external_order_id TEXT,
                            claimed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                        );
                        """
                    )
                )
            except Exception:  # noqa: BLE001
                pass
            # mig 239, SQLite twin: one column, and an existing one raises, so its own try.
            try:
                await database.execute(
                    text("ALTER TABLE surface_click_events ADD COLUMN issued_at TIMESTAMP")
                )
            except Exception:  # noqa: BLE001
                pass
            # Self-heal EVERY table in REQUIRED_SCHEMA, not a hardcoded subset:
            # /health fails closed on any missing required column, so a spec
            # entry without a matching heal here leaves a fresh sqlite dev/test
            # DB permanently unhealthy (503).
            sqlite_type = {
                ("merchant_stores", "is_primary"): "BOOLEAN DEFAULT FALSE",
                ("merchant_stores", "order_writeback_status"): "TEXT DEFAULT 'disabled'",
                # DEFAULTS MUST MATCH THE MIGRATION. A heal that disagrees with
                # the real schema is worse than no heal: it makes a self-healed
                # SQLite DB behave unlike prod for exactly the first executing
                # test that touches it. Enforced by
                # tests/test_schema_guard_sqlite_heal_defaults.py.
                #
                # Type NAMES may differ where SQLite has no equivalent
                # (TIMESTAMPTZ -> TIMESTAMP, BIGINT -> NUMERIC); SQLite's
                # dynamic typing makes that harmless. The DEFAULT VALUE may not.
                ("merchant_stores", "upstream_probe_at"): "TIMESTAMP",
                ("merchant_stores", "upstream_probe_status"): "TEXT",
                ("merchant_stores", "upstream_probe_http_status"): "INTEGER",
                ("merchant_stores", "upstream_probe_failures"): "INTEGER NOT NULL DEFAULT 0",
                ("merchant_stores", "created_at"): "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                # migration 139: BOOLEAN NOT NULL DEFAULT TRUE. This read
                # DEFAULT FALSE until 2026-08-01 — copy-pasted from the
                # is_primary line above (#1536) — which meant a self-healed
                # SQLite DB marked EVERY existing merchant non-indexable, and
                # `COALESCE(m.indexable, TRUE) IS TRUE` then dropped all of them
                # from cross-merchant recall.
                ("catalog_merchants", "indexable"): "BOOLEAN NOT NULL DEFAULT TRUE",
                ("merchant_credit_balance", "purchased_credits"): "NUMERIC DEFAULT 0",
                ("merchant_credit_balance", "overage_pending_credits"): "NUMERIC DEFAULT 0",
                ("merchant_credit_balance", "overage_charged_credits"): "NUMERIC DEFAULT 0",
                ("merchant_credit_balance", "overage_blocked_until_payment"): "BOOLEAN DEFAULT FALSE",
            }
            missing = await check_required_schema()
            for table, cols in missing.items():
                for col in sorted(cols or []):
                    try:
                        await database.execute(
                            text(
                                f"ALTER TABLE {table} ADD COLUMN {col} "
                                f"{sqlite_type.get((table, col), 'TEXT')};"
                            )
                        )
                    except Exception:
                        # Ignore duplicate-column / unsupported variations.
                        continue
            return
    except Exception:
        # Best-effort only; callers should not depend on this always succeeding.
        return
