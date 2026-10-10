"""Shape of migrations 260/261 (catalog_products.name_norm / own_name_norm; PIVOTA-Agent #2404).

The whole point of the column is that it equals what the gateway folds at query time, so the
first pin is the fold EXPRESSION, character for character, against the gateway's `identitySql`
(PIVOTA-Agent src/services/canonicalSearchQualitySql.js). The gateway is another repository, so
the expression is pinned here as the literal the two sides agreed on; a change on either side
has to change this constant, and the Postgres gate (tests/test_catalog_products_own_name_norm_
postgres.py) proves the column equals the expression on real rows.

SQLite-safe: reads files, never a database.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
# *.sql.disabled: deploys skip startup migrations (main.startup returns early in fast mode,
# SKIP_HEAVY_STARTUP_INIT=true on every Cloud Run deploy), so these two are applied by hand, in order,
# from a one-off job; the suffix also keeps them out of the boot runner in any environment that runs
# the full startup, which applies every ACTIVE *.sql file it has not ledgered, with no lock_timeout.
UP_260 = REPO / "db" / "migrations" / "260_catalog_products_own_name_norm.sql.disabled"
UP_261 = REPO / "db" / "migrations" / "261_catalog_products_name_norm_trgm_index.sql.disabled"
DOWN_260 = REPO / "db" / "migrations" / "down" / "260_catalog_products_own_name_norm_down.sql"
DOWN_261 = REPO / "db" / "migrations" / "down" / "261_catalog_products_name_norm_trgm_index_down.sql"

# PIVOTA-Agent src/services/canonicalSearchQualitySql.js `identitySql(expression)`, with the
# gateway's `coalesce(${expression}, '')` argument replaced by the function's parameter.
IDENTITY_ACCENTED = "ÀÁÂÃÄÅÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÝàáâãäåèéêëìíîïòóôõöùúûüýÿ"
IDENTITY_FOLDED = "AAAAAAEEEEIIIIOOOOOUUUUYaaaaaaeeeeiiiiooooouuuuyy"
GATEWAY_IDENTITY_SQL = (
    "trim(regexp_replace(lower(translate(regexp_replace(coalesce(expression, ''), '[·•]', '', 'g'), "
    f"'{IDENTITY_ACCENTED}', '{IDENTITY_FOLDED}')), '[^[:alnum:]]+', ' ', 'g'))"
)
# The gateway's own-name inputs (canonicalSearchQualitySql.js `ownName`) and the carrier CTE's
# (`carriesAll`: title + product_type only).
OWN_NAME_INPUTS = "concat_ws(' ', NEW.title, NEW.product_type,\n              NEW.product_payload->>'canonical_title', NEW.product_payload->>'canonical_name')"
CARRIER_INPUTS = "concat_ws(' ', NEW.title, NEW.product_type)"


def _sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_both_files_are_invisible_to_the_boot_runner_and_say_why() -> None:
    from db.sql_migrations import (
        HAND_APPLIED_MARKER, list_hand_applied_migration_files, list_migration_files, needs_autocommit,
    )

    active = {Path(f).name for f in list_migration_files(str(REPO))}
    # The schema-rebuilding test fixtures (tests/test_repo_sql_prepare_postgres.py) apply these two
    # in version order with the active files, keyed on the first-line marker: production has them, so SQL written
    # against their columns must be planned against a schema that has them. The retired .disabled
    # files carry no marker and stay out of everything.
    assert HAND_APPLIED_MARKER == "-- HAND-APPLIED ONLY"
    assert [Path(f).name for f in list_hand_applied_migration_files(str(REPO))] == [UP_260.name, UP_261.name]
    assert not (set(Path(f).name for f in list_hand_applied_migration_files(str(REPO))) & active)
    assert UP_260.exists() and UP_261.exists()
    for name in ("260_catalog_products_own_name_norm.sql", "261_catalog_products_name_norm_trgm_index.sql"):
        assert name not in active, f"{name} would be applied by any environment that runs the full startup"
        assert not (REPO / "db" / "migrations" / name).exists()
    assert not any(n.startswith(("260_", "261_")) for n in active)
    # The hand-apply still runs them the way the runner would classify them: 260 in one transaction,
    # 261 statement-at-a-time on autocommit.
    assert needs_autocommit(_sql(UP_260)) is False
    assert needs_autocommit(_sql(UP_261)) is True
    for path in (UP_260, UP_261):
        head = _sql(path).splitlines()[0]
        assert head.startswith("-- HAND-APPLIED ONLY"), path.name
    assert "lock_timeout = '3s'" in _sql(UP_260)
    assert "AFTER the backfill" in _sql(UP_261) and "indisvalid" in _sql(UP_261)


def test_the_fold_function_is_the_gateways_identity_sql_character_for_character() -> None:
    sql = _sql(UP_260)
    assert "CREATE OR REPLACE FUNCTION catalog_products_identity_fold(expression TEXT)" in sql
    body = re.search(r"AS \$\$\s*SELECT (.*?)\s*\$\$;", sql, re.DOTALL)
    assert body, "the fold function body is not a single SELECT"
    assert body.group(1).strip() == GATEWAY_IDENTITY_SQL
    # Pure, so an index on it or a generated column could use it; one definition on both sides.
    assert "IMMUTABLE" in sql and "PARALLEL SAFE" in sql
    assert len(IDENTITY_ACCENTED) == len(IDENTITY_FOLDED)  # translate() maps position by position


def test_both_columns_are_stamped_by_the_triggers_over_the_gateways_inputs() -> None:
    sql = _sql(UP_260)
    assert "ADD COLUMN IF NOT EXISTS name_norm TEXT" in sql
    assert "ADD COLUMN IF NOT EXISTS own_name_norm TEXT" in sql
    assert f"NEW.name_norm := catalog_products_identity_fold({CARRIER_INPUTS});" in sql
    assert "NEW.own_name_norm := catalog_products_identity_fold(\n    " + OWN_NAME_INPUTS + ");" in sql
    # INSERT always stamps; UPDATE stamps only when an input changed or a column is NULL, so a write
    # to an unrelated column pays no fold, a NULL row is repaired by its next write, and setting the
    # columns to NULL re-folds in one statement (the script's --refold-all).
    assert "CREATE TRIGGER trg_catalog_products_stamp_name_norm_insert\n  BEFORE INSERT ON catalog_products\n  FOR EACH ROW" in sql
    assert "CREATE TRIGGER trg_catalog_products_stamp_name_norm_update\n  BEFORE UPDATE ON catalog_products\n  FOR EACH ROW" in sql
    when = re.search(r"WHEN \((.*?)\)\s+EXECUTE FUNCTION catalog_products_stamp_name_norm\(\);", sql, re.DOTALL)
    assert when, "the UPDATE trigger needs its WHEN clause"
    clause = " ".join(when.group(1).split())
    for part in ("NEW.name_norm IS NULL", "NEW.own_name_norm IS NULL", "OLD.title IS DISTINCT FROM NEW.title",
                 "OLD.product_type IS DISTINCT FROM NEW.product_type", "OLD.product_payload IS DISTINCT FROM NEW.product_payload"):
        assert part in clause, part
    assert "BEFORE INSERT OR UPDATE" not in sql
    for name in ("trg_catalog_products_stamp_name_norm", "trg_catalog_products_stamp_name_norm_insert", "trg_catalog_products_stamp_name_norm_update"):
        assert f"DROP TRIGGER IF EXISTS {name} ON catalog_products;" in sql
    # No backfill in the migration: existing rows are stamped by the bounded, resumable, throttled
    # script (below), through this same trigger, so the file stays a short transaction.
    assert "UPDATE catalog_products" not in sql
    # No CONCURRENTLY here: this file must run in ONE transaction (function + columns + trigger). The
    # runner classifies a file onto its statement-at-a-time autocommit path on that word ANYWHERE in
    # the file, comments included.
    assert "CONCURRENTLY" not in sql
    from db.sql_migrations import needs_autocommit

    assert needs_autocommit(sql) is False
    assert needs_autocommit(_sql(UP_261)) is True


def test_the_backfill_script_is_dry_run_by_default_bounded_lock_safe_and_never_restates_the_fold() -> None:
    src = (REPO / "scripts" / "backfill_catalog_products_name_norm.py").read_text(encoding="utf-8")
    assert '"--apply"' in src and 'action="store_true"' in src  # dry-run unless asked
    for flag in ('"--batch-size"', '"--max-batches"', '"--sleep-ms"', '"--refold-all"', '"--after-key"'):
        assert flag in src, flag
    # Never queues behind a writer, never deadlocks with one: SKIP LOCKED on the pick, timeouts per batch.
    assert "FOR UPDATE SKIP LOCKED" in src
    assert "SET LOCAL lock_timeout" in src and "SET LOCAL statement_timeout" in src
    assert '"all_locked"' in src and '"stalled"' in src  # the two outcomes are told apart
    # --refold-all re-folds by setting both columns NULL in one statement (the UPDATE trigger fires).
    assert "SET name_norm = NULL, own_name_norm = NULL" in src
    # The script touches rows so the TRIGGER stamps them; the fold rule has exactly one owner, so
    # none of the fold's SQL appears here (naming the function in prose is fine; restating it is not).
    for fragment in ("regexp_replace", "translate(", "[^[:alnum:]]", IDENTITY_ACCENTED,
                     "SET name_norm = catalog_products_identity_fold", "SET own_name_norm = catalog_products_identity_fold"):
        assert fragment not in src, fragment
    assert "SET title = title" in src
    assert "name_norm IS NULL OR own_name_norm IS NULL" in src
    # It refuses to run when the triggers are absent (otherwise `SET title = title` would stamp nothing).
    assert "trg_catalog_products_stamp_name_norm_insert" in src and "trg_catalog_products_stamp_name_norm_update" in src


def test_the_index_is_its_own_concurrent_file_on_the_carrier_column() -> None:
    sql = _sql(UP_261)
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_catalog_products_name_norm_trgm" in sql
    assert "ON catalog_products USING GIN (name_norm gin_trgm_ops)" in sql
    assert "CREATE EXTENSION IF NOT EXISTS pg_trgm;" in sql
    # Index-only files are exempt from the schema-guard coverage gate, and say so.
    assert "schema-guard-exempt" in sql
    # Nothing else in a CONCURRENTLY file: the runner sends it statement-at-a-time on autocommit.
    assert "ALTER TABLE" not in sql and "UPDATE " not in sql


def test_down_files_keep_the_columns_and_drop_only_what_the_up_created() -> None:
    down = _sql(DOWN_260)
    for name in ("trg_catalog_products_stamp_name_norm_insert", "trg_catalog_products_stamp_name_norm_update"):
        assert f"DROP TRIGGER IF EXISTS {name} ON catalog_products;" in down
    assert "DROP FUNCTION IF EXISTS catalog_products_stamp_name_norm();" in down
    assert "DROP FUNCTION IF EXISTS catalog_products_identity_fold(TEXT);" in down
    assert "DROP COLUMN" not in down
    assert "DROP INDEX CONCURRENTLY IF EXISTS idx_catalog_products_name_norm_trgm;" in _sql(DOWN_261)


def test_the_migration_owns_the_columns_nothing_in_the_backend_names_them() -> None:
    # A column the shared Table names must exist before the new image serves, and the boot heal's
    # ADD COLUMN can lose its 500 ms lock race to the gateway's multi-second category reads. Nothing
    # in the backend reads these columns, so the Table and schema_guard must NOT name them, and the
    # migration carries the coverage gate's exemption marker instead of a self-heal entry.
    from db import catalog as catalog_models

    columns = {c.name for c in catalog_models.catalog_products.columns}
    assert not ({"name_norm", "own_name_norm"} & columns)
    guard = (REPO / "db" / "schema_guard.py").read_text(encoding="utf-8")
    assert "own_name_norm" not in guard
    # (another table legitimately has a `name_norm` column; the check is per catalog_products ALTER)
    assert not re.search(r"ALTER TABLE IF EXISTS catalog_products[^;]*\bname_norm\b", guard)
    from db.schema_guard import REQUIRED_SCHEMA

    required = next(t for t in REQUIRED_SCHEMA if t.table == "catalog_products").columns
    assert not ({"name_norm", "own_name_norm"} & set(required))
    sql = _sql(UP_260)
    assert "schema-guard-exempt:" in sql
    # No backend source names the columns either (the gateway is the only reader).
    for folder in ("db", "routes", "services", "scripts"):
        for path in (REPO / folder).rglob("*.py"):
            if path.name == "backfill_catalog_products_name_norm.py":
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            assert "own_name_norm" not in text, path
