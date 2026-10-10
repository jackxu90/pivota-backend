"""
Runner for ``db/migrations/*.sql``.

What this replaces: the inline runner in ``main.startup()``, which sent each
file whole through ``conn.execute(text(body))`` on an AUTOCOMMIT connection.
Measured against a clean Postgres on 2026-08-19, **6 of 219 files could never
apply** under it, each rolling back entirely because a file is one
simple-query batch:

1. ``CONCURRENTLY`` (4 files: 051, 059, 132, 135). psycopg2 sends a
   multi-statement string as ONE implicit transaction, so AUTOCOMMIT alone
   does not help — ``CREATE INDEX CONCURRENTLY cannot run inside a
   transaction block``. The ``ALTER TABLE``s that shared those files
   (``catalog_offers.suppression_*``, ``writer_audit_log``, …) never landed
   either.
2. ``text()`` bind parsing (2 files: 191_acp_checkout_sessions,
   192_acp_delegate_allowances). ``text()`` reads ``:name`` as a bind
   parameter **even inside a ``--`` comment** — both files describe a cache
   key like ``acp_complete:<session_id>:a<attempt>`` in prose and die with
   "A value is required for bind parameter 'a'".

An earlier form of that runner (before it moved to AUTOCOMMIT) called
``conn.commit()`` on a legacy SQLAlchemy 1.4 ``Connection``, which has no such
method: every file raised ``AttributeError``, and only files whose text began
with a DDL keyword survived through 1.4 legacy autocommit. That is the history
behind ``relation "webhook_events" does not exist``; it is fixed on main, and
this runner keeps it fixed by using a real transaction per file.

Migration bodies are executed on the raw DBAPI cursor (see ``_execute_raw``),
not through ``text()`` or ``exec_driver_sql`` — both of those mangle static
SQL that contains ``:name`` tokens or literal ``%``. Only the ledger INSERT
is bound.

Applied files are recorded in ``startup_sql_migrations`` so each file runs at most
once per database *through this runner*, in the same transaction as the file
itself: a file that fails leaves no ledger row and is retried on the next
boot, exactly like the old best-effort behaviour. Migration content is
unchanged.

The ledger is deliberately NOT called ``schema_migrations``: production already
has a table of that name, owned by something outside this repo — 71 rows keyed
``id`` (``001_taxonomy.sql``, ``004_look_replicator.sql``, …) with no
``filename``/``checksum`` columns. Using that name made ``CREATE TABLE IF NOT
EXISTS`` a no-op against the foreign table and every ledger INSERT then failed
inside the migration's own transaction, so **all 219 files rolled back**
(reproduced 2026-08-20).

Two independent guards came out of that, because the name alone is not enough
on a database this repo shares with other systems:

* The ledger row is recorded AFTER the migration's transaction commits, never
  inside it (:func:`_record_applied`). A ledger that is unwritable for any
  reason — a view, a missing unique constraint for ``ON CONFLICT``, an extra
  NOT NULL column, no INSERT grant — can then only cost a re-run, never a
  rollback.
* :func:`_ledger_state` decides by READING the columns we use, not by
  comparing information_schema shapes. If a relation of our name exists and we
  cannot read it, the runner REFUSES to run rather than falling back to
  "apply everything": re-running the tree against a populated database
  re-executes convergent writes — ``126_subscription_plans_allowance_rebase``
  resets ``subscription_plans.monthly_credit_allowance``, 139/146 re-tombstone
  catalog rows, ``013`` appends a routing_migration_log row per boot — and
  three files (003, 024, 027) are not re-runnable at all, because
  ``CREATE TRIGGER`` has no ``IF NOT EXISTS`` in PG15.

Note the ledger is not the only applier: ``routes/admin_run_migration_pending``
executes a migration by number and writes no ledger row, so a file applied
that way is applied once more by the next boot. Harmless for the additive
files that route targets today, but if that changes, teach it to write the
ledger row too.

Call this INSIDE the startup advisory lock (see db/startup_ddl.py) so two
instances never apply the same file concurrently.
"""
from __future__ import annotations

import glob
import hashlib
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from db.startup_ddl import is_already_exists_error

logger = logging.getLogger(__name__)

MIGRATIONS_DIRNAME = os.path.join("db", "migrations")
LEDGER_TABLE = "startup_sql_migrations"

_LEDGER_DDL = f"""
CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
    filename TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
)
"""


def migrations_dir(base_dir: Optional[str] = None) -> str:
    root = base_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root, MIGRATIONS_DIRNAME)


def list_migration_files(base_dir: Optional[str] = None) -> List[str]:
    """Active migrations, in filename order. ``*.sql.disabled`` is excluded by
    the glob — that suffix is how a migration is retired (see the header of
    002_production_tables.sql.disabled)."""
    return sorted(glob.glob(os.path.join(migrations_dir(base_dir), "*.sql")))


# A ``*.sql.disabled`` file whose FIRST line starts with this marker is not retired:
# it is schema that production gets by hand, in a one-off job, because applying it
# from the boot runner would be unsafe (260: ACCESS EXCLUSIVE on the hottest table
# behind multi-second reads; 261: an index that must follow 260's backfill). The
# boot runner still never reads it -- only `list_migration_files` decides that --
# but the test fixtures that rebuild production's schema from the migrations DO
# apply it, in version order with the active files, so SQL written against those columns is
# planned against the schema production actually has.
HAND_APPLIED_MARKER = "-- HAND-APPLIED ONLY"


def is_hand_applied_migration(path: str) -> bool:
    """True for a ``*.sql.disabled`` file that opens with HAND_APPLIED_MARKER."""
    if not path.endswith(".sql.disabled"):
        return False
    with open(path, "r", encoding="utf-8") as handle:
        return handle.readline().startswith(HAND_APPLIED_MARKER)


def list_hand_applied_migration_files(base_dir: Optional[str] = None) -> List[str]:
    """Hand-applied migrations (see HAND_APPLIED_MARKER), in filename order.
    Disjoint from `list_migration_files` by construction: a different glob."""
    candidates = sorted(glob.glob(os.path.join(migrations_dir(base_dir), "*.sql.disabled")))
    return [path for path in candidates if is_hand_applied_migration(path)]


def _checksum(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


_CONCURRENTLY = re.compile(r"\bCONCURRENTLY\b", re.IGNORECASE)


def needs_autocommit(body: str) -> bool:
    """CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction block
    (051_external_seed_text_trgm_concurrent.sql says so in its own runbook
    header). Such files are executed on an AUTOCOMMIT connection instead —
    statement-at-a-time, so a failure part-way leaves the earlier statements
    applied; every such file today is a list of IF NOT EXISTS indexes."""
    return bool(_CONCURRENTLY.search(body))


def split_statements(body: str) -> List[str]:
    """
    Split a migration into top-level statements on ``;``.

    Only used for the AUTOCOMMIT path: psycopg2 sends a multi-statement string
    as ONE simple-query batch, which Postgres wraps in an implicit transaction
    — so even with isolation_level=AUTOCOMMIT a CONCURRENTLY index fails
    unless each statement is sent on its own.

    Quote-aware: single quotes (incl. '' escapes), double-quoted identifiers,
    dollar-quoted bodies ($$ / $tag$), line comments and block comments are
    scanned so a ``;`` inside any of them does not split.
    """
    statements: List[str] = []
    buf: List[str] = []
    i = 0
    n = len(body)
    while i < n:
        ch = body[i]
        nxt = body[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":
            j = body.find("\n", i)
            j = n if j == -1 else j
            buf.append(body[i:j])
            i = j
            continue
        if ch == "/" and nxt == "*":
            j = body.find("*/", i + 2)
            j = n if j == -1 else j + 2
            buf.append(body[i:j])
            i = j
            continue
        if ch == "'" or ch == '"':
            quote = ch
            j = i + 1
            while j < n:
                if body[j] == quote:
                    if j + 1 < n and body[j + 1] == quote:
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            buf.append(body[i:j])
            i = j
            continue
        if ch == "$":
            m = re.match(r"\$[A-Za-z_0-9]*\$", body[i:])
            if m:
                tag = m.group(0)
                j = body.find(tag, i + len(tag))
                j = n if j == -1 else j + len(tag)
                buf.append(body[i:j])
                i = j
                continue
        if ch == ";":
            statements.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    statements.append("".join(buf))
    return [st for st in (raw.strip() for raw in statements) if st and not _is_only_comments(st)]


def _is_only_comments(statement: str) -> bool:
    stripped = re.sub(r"/\*.*?\*/", "", statement, flags=re.S)
    stripped = re.sub(r"--[^\n]*", "", stripped)
    return not stripped.strip()


# Ledger states.
#   OK      — the table is ours and readable; skip what it records.
#   ABSENT  — no such relation and we could not create one (e.g. the SQLite dev
#             default, whose DDL this table's definition does not support). Run
#             every file, record nothing. This is the pre-#1775 behaviour.
#   FOREIGN — a relation of our name exists but we cannot read our columns out
#             of it, i.e. it belongs to something else. REFUSE to run.
LEDGER_OK = "ok"
LEDGER_ABSENT = "absent"
LEDGER_FOREIGN = "foreign"


def _relation_exists(engine) -> bool:
    try:
        with engine.connect() as conn:
            return conn.execute(text("SELECT to_regclass(:t)"), {"t": LEDGER_TABLE}).scalar() is not None
    except Exception:  # noqa: BLE001 - non-Postgres, or we cannot tell
        return False


def _ledger_state(engine) -> Tuple[str, Dict[str, str]]:
    """(state, already-applied) — see LEDGER_* above.

    The check is a READ of the columns we actually use, not an
    information_schema shape comparison: a table can have the right column
    names and still be unusable (a view, no unique constraint for ON CONFLICT,
    an extra NOT NULL column, or no INSERT grant). Reading is also the
    capability that matters most — knowing what already ran is what keeps the
    runner from re-executing files against a populated database.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(_LEDGER_DDL))
    except Exception as exc:  # noqa: BLE001
        if not is_already_exists_error(exc):
            logger.info("SQL migrations: could not create %s (%s)", LEDGER_TABLE, exc)

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT filename, checksum FROM {LEDGER_TABLE}"))
            return LEDGER_OK, {r[0]: r[1] for r in rows}
    except Exception as exc:  # noqa: BLE001
        if _relation_exists(engine):
            logger.error(
                "SQL migrations: a table named %s exists but is not ours (%s). REFUSING to "
                "run migrations: without the ledger this runner cannot tell which files have "
                "already been applied, and re-running the tree against a populated database "
                "re-executes convergent writes (126_subscription_plans_allowance_rebase resets "
                "billing allowances; 139/146 re-tombstone catalog rows). Rename LEDGER_TABLE "
                "or hand the other table over.",
                LEDGER_TABLE,
                exc,
            )
            return LEDGER_FOREIGN, {}
        logger.warning(
            "SQL migrations: no usable %s ledger (%s) — running every file and recording "
            "nothing.",
            LEDGER_TABLE,
            exc,
        )
        return LEDGER_ABSENT, {}


def _execute_raw(conn, sql: str) -> None:
    """Run a migration body as raw SQL on the DBAPI cursor of ``conn``.

    Neither ``text()`` nor ``exec_driver_sql()`` will do:
    * ``text()`` parses ``:name`` as a bind parameter even inside a ``--``
      comment (191/192_acp_* die with "A value is required for bind parameter
      'a'" over a comment describing a cache key).
    * ``exec_driver_sql()`` hands psycopg2 an empty parameter mapping, so
      psycopg2 tries ``%``-interpolation and every migration containing a
      literal ``%`` (LIKE patterns, percentages in comments) fails with
      "dict is not a sequence" — 30+ files.

    ``cursor.execute(sql)`` with no parameters does neither.
    """
    cursor = conn.connection.cursor()
    try:
        cursor.execute(sql)
    finally:
        cursor.close()


def _ledger_insert():
    return text(
        f"INSERT INTO {LEDGER_TABLE} (filename, checksum) "
        "VALUES (:filename, :checksum) "
        "ON CONFLICT (filename) DO UPDATE SET checksum = EXCLUDED.checksum"
    )


def _record_applied(engine, name: str, digest: str) -> None:
    """Best-effort, and deliberately NOT part of the migration's transaction.

    Coupling them is what turned a ledger defect into a total outage: every
    INSERT failed inside the migration's own transaction, so every file rolled
    back. Recorded separately, the worst a broken ledger write can do is make
    the file run again next boot.
    """
    try:
        with engine.begin() as conn:
            conn.execute(_ledger_insert(), {"filename": name, "checksum": digest})
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "SQL migrations: %s applied but could not be recorded in %s (%s) — it will "
            "run again on the next boot.",
            name,
            LEDGER_TABLE,
            exc,
        )


def run_sql_migrations(engine, *, base_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Apply every not-yet-applied file in db/migrations/*.sql, one transaction
    per file. Best-effort: a failing file is logged and skipped (no ledger
    row), never raised, so a bad migration cannot block boot.

    Returns {"applied": [...], "skipped": [...], "failed": [(name, error)]}.
    """
    result: Dict[str, Any] = {"applied": [], "skipped": [], "failed": []}
    files = list_migration_files(base_dir)
    if not files:
        return result

    state, applied = _ledger_state(engine)
    if state == LEDGER_FOREIGN:
        result["aborted"] = f"{LEDGER_TABLE} exists but is not ours"
        return result
    have_ledger = state == LEDGER_OK

    for path in files:
        name = os.path.basename(path)
        with open(path, "r", encoding="utf-8") as fh:
            body = fh.read()
        digest = _checksum(body)

        if have_ledger and name in applied:
            if applied[name] != digest:
                # Content changed after being applied. Do NOT re-run: these
                # files are not written to be re-runnable. Surface it loudly.
                logger.warning(
                    "SQL migrations: %s already applied but its checksum changed "
                    "(recorded %s, on disk %s) — NOT re-running; add a new "
                    "migration instead.",
                    name,
                    applied[name][:12],
                    digest[:12],
                )
            result["skipped"].append(name)
            continue

        try:
            if needs_autocommit(body):
                autocommit_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
                with autocommit_engine.connect() as conn:
                    for statement in split_statements(body):
                        _execute_raw(conn, statement)
            else:
                # One transaction for the whole file: preserves $$ blocks and
                # keeps a partially-failing file from landing half-applied.
                with engine.begin() as conn:
                    _execute_raw(conn, body)
            if have_ledger:
                _record_applied(engine, name, digest)
            result["applied"].append(name)
            logger.info("   ✅ %s applied", name)
        except Exception as exc:  # noqa: BLE001 - best-effort, as before
            result["failed"].append((name, str(exc)))
            logger.warning("   Migration %s error (rolled back, will retry next boot): %s", name, exc)

    logger.info(
        "SQL migrations: %d applied, %d already applied, %d failed",
        len(result["applied"]),
        len(result["skipped"]),
        len(result["failed"]),
    )
    return result
