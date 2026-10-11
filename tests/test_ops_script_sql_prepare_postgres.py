"""Every statement the ops scripts send must be PLANNABLE by Postgres.

THE DEFECT THIS CLOSES. The first production `--apply` of
`scripts/remediate_unpublished_crawl_rows.py` died at PREPARE with
`could not determine data type of parameter $2`, having written nothing.
`jsonb_build_object` is variadic `"any"`, so Postgres cannot infer a bind
parameter's type from its position; the statement is rejected before it is
planned, on every row, so the runner was simply dead. Fixed in #1703 by wrapping
the params in `CAST(:x AS text)`.

WHY NOTHING CAUGHT IT — the same three-part gap, again:

* `tests/test_remediate_unpublished_crawl_rows.py` records SQL strings against a
  fake connection. A fake never asks Postgres to plan anything, so a statement
  that cannot be prepared looks identical to one that can. This is the same
  reason `tests/test_databases_textclause_params.py` exists — read its module
  docstring; the argument is verbatim the same, only the engine differs. That
  class raises inside `databases._build_query` before the driver is touched, so
  SQLite reproduces it. This one needs a real Postgres PREPARE.
* The dialect gate (`.github/workflows/postgres-dialect-gate.yml`) collects
  `tests/test_*_postgres.py`; the remediation script's test file matches no set
  it runs, so it never executed against Postgres at all.
* The regression test #1703 added is a TEXTUAL pin on those specific casts. It
  catches deleting them. It cannot catch the next statement written with the
  same flaw — which is exactly the "an assertion has to be authored against a
  defect you have already met" argument in the workflow header.

WHAT THIS FILE DOES INSTEAD. It takes the statements the scripts actually build
— captured by driving the real functions, or by calling the real query builders,
never by lifting a copy into the test — and asks Postgres to PREPARE each one. A
statement that cannot be planned fails the gate. No foreknowledge of the defect
is needed, which is the whole point.

PREPARE, NOT EXECUTE. `conn.prepare()` is Parse+Describe: it resolves every
table, column, operator and parameter type and plans the statement, then stops.
Nothing is written and no parameter VALUES are needed — which matters, because
inventing values for `INSERT INTO catalog_skus (...)` would trip NOT NULL
constraints and report a schema-fixture problem as a dialect defect. Type
inference happens at Parse and is value-independent, so PREPARE is exactly the
right knife: it sees the whole failure class and nothing else.

`:name` -> `$n` is done by compiling the SQL through SQLAlchemy's postgresql
dialect (the same compiler `databases` uses) rather than a hand-rolled regex,
so `'{}'::jsonb` stays a cast and does not become a bind named `jsonb`.

KNOWN LIMIT, stated rather than discovered later: PREPARE resolves names against
THIS database's schema, so the gate is only as truthful as the fixture. Column
TYPES are load-bearing here in a way they are not for a row-semantics test — a
stub column typed `text` where production has `jsonb` would change whether a
cast is required. The lightweight DDL below mirrors migrations 044 / 098.

🚨 THESE GATE FILES SHARE ONE DATABASE. `metadata.create_all` for tables
`db.catalog` owns; `CREATE TABLE IF NOT EXISTS` + `ADD COLUMN IF NOT EXISTS`
only for tables it does not. Never hand-roll DDL for a table `db.catalog` owns —
see the warning in tests/test_canonical_feed_tombstoned_flag_postgres.py, where
a three-column stub under a real table's name broke every sibling gate.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
_IS_PG = DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://")

pytestmark = pytest.mark.skipif(
    not _IS_PG,
    reason="needs a Postgres DATABASE_URL — production-dialect gate",
)


# ---------------------------------------------------------------------------
# schema fixture
# ---------------------------------------------------------------------------
# Only tables `db.catalog` does NOT own. Types mirror migrations 044
# (external_product_seeds) and 098 (index_pipeline_state) because PREPARE
# resolves parameter types THROUGH the column types — see KNOWN LIMIT above.
_LIGHTWEIGHT_DDL = """
-- The A9-4 checkpoint is created at RUNTIME by ensure_checkpoint_table, so it
-- is not on the shared MetaData and metadata.create_all cannot provision it.
-- The quality-snapshot repair plans against it, and an unplannable statement
-- there aborts a repair mid-run. Declared minimally for the same reason as
-- everything else in this block: PREPARE needs the shape, not the data.
--
-- `previous_value` is deliberately NOT declared. Nothing here needs it, and
-- tests/test_a9_4_barekey_guard_postgres.py omits it on purpose so that
-- ensure_checkpoint_table's own ADD COLUMN stays under test — these gate files
-- share one database, so adding it here would make that sibling's assertion
-- pass vacuously depending on file order.
CREATE TABLE IF NOT EXISTS a9_4_backfill_checkpoint (phase text, ref_id text);
ALTER TABLE a9_4_backfill_checkpoint ADD COLUMN IF NOT EXISTS observed_id text;
ALTER TABLE a9_4_backfill_checkpoint ADD COLUMN IF NOT EXISTS status text;
ALTER TABLE a9_4_backfill_checkpoint ADD COLUMN IF NOT EXISTS updated_at timestamptz;

CREATE TABLE IF NOT EXISTS external_product_seeds (id text);
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS external_product_id text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS market text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS tool text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS destination_url text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS canonical_url text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS domain text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS title text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS image_url text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS status text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS attached_product_key text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS seller_ref text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS seed_kind text;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS seed_data jsonb;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS created_at timestamptz;
ALTER TABLE external_product_seeds ADD COLUMN IF NOT EXISTS updated_at timestamptz;
-- product_reviews: keyed in the REVIEWS service's own namespace
-- (merchant|platform|id), which is why the catalog cascade never reached it.
-- scripts/dispose_sentinel_orphans.py PREPAREs against it, and metadata does
-- not register it, so the gate needs it here or those statements cannot plan.
CREATE TABLE IF NOT EXISTS product_reviews (id bigserial PRIMARY KEY);
ALTER TABLE product_reviews ADD COLUMN IF NOT EXISTS product_key text;
ALTER TABLE product_reviews ADD COLUMN IF NOT EXISTS sku_key text;
ALTER TABLE product_reviews ADD COLUMN IF NOT EXISTS merchant_id text;
ALTER TABLE product_reviews ADD COLUMN IF NOT EXISTS platform text;
ALTER TABLE product_reviews ADD COLUMN IF NOT EXISTS platform_product_id text;
ALTER TABLE product_reviews ADD COLUMN IF NOT EXISTS status text;
ALTER TABLE product_reviews ADD COLUMN IF NOT EXISTS created_at timestamptz;
CREATE TABLE IF NOT EXISTS index_pipeline_state (content_key text PRIMARY KEY);
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS pivota_signature_id text;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS merchant_id text;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS pipeline_stage text;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS blocker_code text;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS blocker_detail text;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS serving_eligible boolean;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS index_eligible boolean;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS content_quality_score real;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS quality_scored_at timestamptz;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS last_extracted_at timestamptz;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS description_length integer;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS has_image boolean;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS has_price boolean;
ALTER TABLE index_pipeline_state ADD COLUMN IF NOT EXISTS updated_at timestamptz;
CREATE TABLE IF NOT EXISTS product_group_members (product_group_id text);
ALTER TABLE product_group_members ADD COLUMN IF NOT EXISTS merchant_id text;
ALTER TABLE product_group_members ADD COLUMN IF NOT EXISTS platform text;
ALTER TABLE product_group_members ADD COLUMN IF NOT EXISTS platform_product_id text;
ALTER TABLE product_group_members ADD COLUMN IF NOT EXISTS is_primary boolean;
"""

# product_quality_snapshot is a LIVE instance of the #1651 hazard, not a
# hypothetical one. tests/test_dead_quality_component_canary_postgres.py runs
# `DROP TABLE IF EXISTS product_quality_snapshot; CREATE TABLE ... (id,
# snapshot_date, details)` — a three-column stub — and these gate files share ONE
# database. It sorts earlier, so its DROP wins outright; a plain
# CREATE TABLE IF NOT EXISTS here then inherits the narrow shape and the
# join-diagnosis scale query dies on UndefinedColumn: merchant_id.
#
# Additive ALTERs are the documented answer: they repair whatever shape the
# sibling leaves without fighting it for ownership of the CREATE. Types mirror
# db/product_quality.py. (This is also why the gate must be run against the
# SHARED database to be trusted — a local sandbox built from
# metadata.create_all has the real table and passes while CI does not.)
_PRODUCT_QUALITY_SNAPSHOT_GUARDS = """
CREATE TABLE IF NOT EXISTS product_quality_snapshot (id bigserial PRIMARY KEY);
ALTER TABLE product_quality_snapshot ADD COLUMN IF NOT EXISTS merchant_id VARCHAR(100);
ALTER TABLE product_quality_snapshot ADD COLUMN IF NOT EXISTS platform VARCHAR(50);
ALTER TABLE product_quality_snapshot ADD COLUMN IF NOT EXISTS platform_product_id VARCHAR(200);
ALTER TABLE product_quality_snapshot ADD COLUMN IF NOT EXISTS geo_code VARCHAR(16);
ALTER TABLE product_quality_snapshot ADD COLUMN IF NOT EXISTS rules_version VARCHAR(32);
ALTER TABLE product_quality_snapshot ADD COLUMN IF NOT EXISTS content_quality_score DOUBLE PRECISION;
ALTER TABLE product_quality_snapshot ADD COLUMN IF NOT EXISTS snapshot_date TIMESTAMP;
"""

# Columns the enrichment statements touch. Belt-and-braces on top of the real
# CREATE lifted below: if a sibling gate file ever lands a narrower
# `product_enrichment` stub first, `CREATE TABLE IF NOT EXISTS` silently keeps
# the narrower shape (the #1651 hazard this file's header describes), and these
# additive ALTERs are what keep the gate honest instead of red for the wrong
# reason. Types mirror db/product_enrichment.py.
_PRODUCT_ENRICHMENT_COLUMN_GUARDS = """
ALTER TABLE product_enrichment ADD COLUMN IF NOT EXISTS geo_code VARCHAR(16);
ALTER TABLE product_enrichment ADD COLUMN IF NOT EXISTS description_markdown TEXT;
ALTER TABLE product_enrichment ADD COLUMN IF NOT EXISTS bullet_points JSONB;
ALTER TABLE product_enrichment ADD COLUMN IF NOT EXISTS usage_scenarios JSONB;
"""


def _strip_sql_comments(sql: str) -> str:
    """Drop `--` line comments and `/* */` block comments. Test-side only.

    Quote-naive on purpose: it is used to CLASSIFY a statement, never to build
    one, and the statements it classifies are DDL with no string literals.
    """
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


def _agent_pdp_view_catchup_ddl() -> List[str]:
    """`ALTER TABLE agent_pdp_view ...` statements from db/migrations, in order.

    agent_pdp_view IS a db.catalog table, so `metadata.create_all` creates it,
    and db/catalog.py's Table definition now declares the full migration column
    set (evidence_profile, required_disclaimers, rating_value, rating_count,
    bullet_points, usage_scenarios) — on a genuinely fresh database these ALTERs
    are no-ops.

    They stay because the database is SHARED and reused: `create_all` is
    checkfirst-only and will not widen a table that already exists, so a
    catalog table built by an OLDER db/catalog.py keeps its narrow shape and
    statements naming the newer columns fail with UndefinedColumn against a
    fixture that looks complete.

    Additive ALTERs lifted from the migrations themselves, never hand-written —
    the 🚨 rule above forbids hand-rolling DDL for a table db.catalog owns, and
    ADD COLUMN IF NOT EXISTS is safe to replay in the shared database.
    """
    from db.sql_migrations import split_statements

    out: List[str] = []
    paths = sorted(
        (REPO_ROOT / "db" / "migrations").glob("*.sql"),
        key=lambda p: [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", p.name)],
    )
    for path in paths:
        body = path.read_text(encoding="utf-8")
        if "agent_pdp_view" not in body:
            continue
        # `split_statements`, NOT `body.split(";")`. The naive split is wrong
        # twice over, and both were live defects:
        #
        #   * it cuts on a `;` inside a `--` comment. Migration 096 line 17 has
        #     "(or absence of intent); shadowing it with" in prose, so the
        #     fragment carrying its real ALTER begins mid-comment and fails any
        #     prefix test — 096 was dropped no matter how the prefix was spelled.
        #   * the prefix test itself matched only `alter table agent_pdp_view`,
        #     dropping migration 186, which writes `ALTER TABLE IF EXISTS`.
        #
        # 186 cost `report_agent_depth_scorecard`'s APV_DEPTH_SQL a PREPARE
        # (`column "rating_value" does not exist`) on a fresh database, masked
        # because a sibling gate file that sorts earlier
        # (tests/test_citation_read_surfaces_postgres.py) patches the SHARED
        # database first. This splitter is the one the real migration runner
        # uses, so it is quote-, dollar-quote- and comment-aware.
        for statement in filter(None, (s.strip() for s in split_statements(body))):
            # Comments STRIPPED before the prefix test, because `split_statements`
            # keeps the comment block that precedes a statement attached to it —
            # migration 096's ALTER arrives as `-- Idempotent: ...\nALTER TABLE
            # IF EXISTS agent_pdp_view ...` and no `startswith` can see past that.
            # Executed unstripped: only the TEST needs the bare SQL.
            collapsed = " ".join(_strip_sql_comments(statement).split()).lower()
            if "add column" not in collapsed:
                continue
            if collapsed.startswith(("alter table agent_pdp_view",
                                    "alter table if exists agent_pdp_view")):
                # The STRIPPED text is what we emit, not the original. The
                # fixture below joins these with ";" and re-splits the blob on
                # ";" — and 096's comment block contains a prose semicolon
                # ("(or absence of intent); shadowing it with"), which that
                # re-split turns into a comment-only fragment and psycopg2
                # rejects as "can't execute an empty query". Stripping here
                # keeps the statement whole through the naive split downstream.
                out.append(" ".join(_strip_sql_comments(statement).split()))
    return out


def _product_enrichment_ddl() -> str:
    """The real `CREATE TABLE product_enrichment`, lifted from
    db/product_enrichment.py's own AST.

    That table is owned by db/product_enrichment.py, not db.catalog, so
    `metadata.create_all` above does not create it and the enrichment statements
    fail with UndefinedTable — a fixture gap reported as a defect.

    Lifted rather than hand-copied for the reason the sibling gate's header
    gives: a stub drifts from the real schema silently, and PREPARE resolves
    parameter types THROUGH column types, so a column typed `text` where
    production has `jsonb` changes whether a cast is required. This is the same
    DDL the application runs at startup.
    """
    source = (REPO_ROOT / "db" / "product_enrichment.py").read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(source, filename="product_enrichment.py")):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and "CREATE TABLE IF NOT EXISTS product_enrichment" in node.value
        ):
            return node.value
    raise AssertionError(
        "could not find the CREATE TABLE for product_enrichment in "
        "db/product_enrichment.py — it moved or was renamed. Do not paste a stub "
        "here; find the real DDL, or this gate silently plans against a fiction."
    )


@pytest.fixture(scope="module")
def prepare() -> Callable[[str], None]:
    """Yield `prepare(sql)` -> raises if Postgres cannot plan the statement.

    One asyncpg connection on a private loop for the whole module. The tests are
    sync so there is no running loop to collide with, and nothing here is
    written: `prepare` is Parse+Describe and stops.
    """
    import asyncpg
    import db.catalog  # noqa: F401  (registers the catalog tables on the shared MetaData)
    import db.products  # noqa: F401  (products_cache — the join-diagnosis report reads it)
    from sqlalchemy import create_engine, text

    from db.database import metadata

    engine = create_engine(DATABASE_URL)
    metadata.create_all(engine, checkfirst=True)
    ddl = "\n".join((
        _LIGHTWEIGHT_DDL,
        _product_enrichment_ddl(),
        _PRODUCT_ENRICHMENT_COLUMN_GUARDS,
        _PRODUCT_QUALITY_SNAPSHOT_GUARDS,
        *(f"{stmt};" for stmt in _agent_pdp_view_catchup_ddl()),
    ))
    with engine.begin() as conn:
        for stmt in filter(None, (s.strip() for s in ddl.split(";"))):
            conn.execute(text(stmt))
    engine.dispose()

    loop = asyncio.new_event_loop()
    pg = loop.run_until_complete(asyncpg.connect(DATABASE_URL.replace("+asyncpg", "")))

    def _prepare(sql: str) -> None:
        loop.run_until_complete(pg.prepare(_to_positional(sql)))

    try:
        yield _prepare
    finally:
        loop.run_until_complete(pg.close())
        loop.close()


# ---------------------------------------------------------------------------
# `:name` -> `$n`
# ---------------------------------------------------------------------------
_PYFORMAT = re.compile(r"%\((\w+)\)s")


def _to_positional(sql: str) -> str:
    """Render `:name` binds as `$n` for asyncpg.

    Compiled through SQLAlchemy's postgresql dialect — the same compiler
    `databases` drives — rather than a regex over `:\\w+`, so `'{}'::jsonb` stays
    a cast instead of becoming a bind named `jsonb`.
    """
    from sqlalchemy import text as sa_text
    from sqlalchemy.dialects import postgresql

    rendered = sa_text(sql).compile(dialect=postgresql.dialect(paramstyle="pyformat")).string
    order: List[str] = []
    for match in _PYFORMAT.finditer(rendered):
        if match.group(1) not in order:
            order.append(match.group(1))
    numbered = {name: f"${i}" for i, name in enumerate(order, start=1)}
    return _PYFORMAT.sub(lambda m: numbered[m.group(1)], rendered).replace("%%", "%")


# ---------------------------------------------------------------------------
# statement collectors
#
# Each returns [(origin, sql)]. Statements are DRIVEN out of the real functions
# or produced by the real query builders — never a copy pasted into this file.
# A lifted copy tests the copy, which is how the original defect shipped (see
# tests/test_pdp_scope_backfill_postgres.py's header on the same point).
# ---------------------------------------------------------------------------
class _Recorder:
    """A `databases.Database` stand-in that records instead of executing."""

    def __init__(self, origin: str) -> None:
        self.origin = origin
        self.statements: List[Tuple[str, str]] = []

    def _record(self, method: str, query: Any) -> None:
        self.statements.append((f"{self.origin} -> database.{method}()", str(query)))

    async def execute(self, query: Any, values: Any = None) -> None:
        self._record("execute", query)

    async def fetch_all(self, query: Any, values: Any = None) -> List[Any]:
        self._record("fetch_all", query)
        return []

    async def fetch_one(self, query: Any, values: Any = None) -> None:
        self._record("fetch_one", query)
        return None

    async def fetch_val(self, query: Any, values: Any = None) -> None:
        self._record("fetch_val", query)
        return None


def _drive(module: Any, coro_factory: Callable[[], Any], origin: str,
           **extra_patches: Any) -> List[Tuple[str, str]]:
    """Run an async entry point with the module's `database` global recording.

    try/finally restores every patched global — these are module-level names
    shared with the rest of the session.
    """
    recorder = _Recorder(origin)
    saved = {"database": module.database}
    module.database = recorder
    for name, value in extra_patches.items():
        saved[name] = getattr(module, name)
        setattr(module, name, value)
    try:
        asyncio.run(coro_factory())
    finally:
        for name, value in saved.items():
            setattr(module, name, value)
    return recorder.statements


async def _noop_recompute(content_key: str, reason: str | None = None) -> bool:
    """Stand-in for services.index_pipeline_state_service.recompute_serving_eligibility.

    It owns a different module's `database` global; this gate is scoped to the
    SQL the SCRIPT itself sends.
    """
    return True


def _collect_remediate() -> List[Tuple[str, str]]:
    """The script that broke prod. Every statement, driven through the real
    functions — the `jsonb_build_object` UPDATE among them."""
    import scripts.remediate_unpublished_crawl_rows as module

    row = {
        "seed_id": "seed_probe",
        "seed_status": "active",
        "destination_url": "https://example.com/products/probe",
        "product_key": "prod::probe",
        "content_key": "ck_probe",
        "suppression_reason": None,
        "suppressed_at": None,
        "rows_on_key": 5,
    }
    revert_row = dict(
        row,
        # 'active' is the branch that also touches external_product_seeds; the
        # 'inactive' branch is a strict subset of these statements.
        suppression_metadata={"script": module.SCRIPT_NAME, "prior_seed_status": "active"},
    )

    async def _go() -> None:
        await module._load_candidates(None)
        await module._withdraw([dict(row)])
        await module._revert([revert_row])
        await module._drive_revert(
            argparse.Namespace(host=None, apply=False, show=5, revert=True)
        )

    return _drive(
        module, _go, "remediate_unpublished_crawl_rows",
        recompute_serving_eligibility=_noop_recompute,
    )


def _collect_withdraw_catalog_rows() -> List[Tuple[str, str]]:
    """Named takedown: the per-table jsonb_build_object UPDATE with a jsonb bind,
    the CASE-on-bind + metadata-subtraction reverts, the ANY(:ids) seed writes
    and the ANY(:pks) loader — every statement it sends (13)."""
    import scripts.withdraw_catalog_rows as module

    row = {
        "product_key": "prod::probe", "content_key": "ck_probe", "title": "probe",
        "brand": "Probe", "source_system": "catalog_enrichment_agent_v1",
        "source_domain": "example.com", "suppression_reason": None, "suppressed_at": None,
        "suppression_metadata": None, "skus": 1, "offers": 1, "seeds": 1,
        "active_seeds": 1, "rows_on_key": 1,
    }
    ours = dict(row, suppressed_at="2026-01-01",
                suppression_metadata={"script": module.SCRIPT_NAME, "reason": "probe",
                                      "deactivated_seed_ids": ["seed_probe"]})

    async def _trust(*, db, product_keys, **kw):
        return len(product_keys)

    async def _go() -> None:
        await module._load_rows(["prod::probe"])
        await module._load_ours(None)
        await module._withdraw([dict(row)], "probe")
        await module._revert([ours])

    return _drive(
        module, _go, "withdraw_catalog_rows",
        recompute_serving_eligibility=_noop_recompute,
        upsert_catalog_row_trust_many=_trust,
    )


def _collect_seed_content_audit() -> List[Tuple[str, str]]:
    import scripts.run_seed_content_audit as module

    origin = "run_seed_content_audit"
    return [
        # Both branches: the default one carries `seed_data->'review_summary'->>
        # 'auditor' != :auditor_version`, a bind compared against a `->>`
        # extraction — the same family of position Postgres must infer through.
        (f"{origin}._select_sql(force=False)",
         module._select_sql(force=False, limit=10, offset=5)),
        (f"{origin}._select_sql(force=True)",
         module._select_sql(force=True, limit=10, offset=5)),
        (f"{origin}.UPDATE_SEED_SQL", module.UPDATE_SEED_SQL),
        (f"{origin}.UPDATE_CATALOG_PRODUCTS_SQL", module.UPDATE_CATALOG_PRODUCTS_SQL),
    ]


def _collect_source_pdp_content_repair() -> List[Tuple[str, str]]:
    import scripts.source_pdp_content_repair as module

    origin = "source_pdp_content_repair"
    return [
        (f"{origin}.build_candidate_query(limit=10)",
         module.build_candidate_query(limit=10)[0]),
        (f"{origin}.build_candidate_query(limit=0)",
         module.build_candidate_query(limit=0)[0]),
        (f"{origin}.UPDATE_DESCRIPTION_SQL", module.UPDATE_DESCRIPTION_SQL),
    ]


def _collect_source_pdp_offer_image_repair() -> List[Tuple[str, str]]:
    import scripts.source_pdp_offer_image_repair as module

    origin = "source_pdp_offer_image_repair"
    return [
        (f"{origin}.build_candidate_query(scoped)",
         module.build_candidate_query(limit=10, include_upstream_blockers=False)[0]),
        (f"{origin}.build_candidate_query(unscoped)",
         module.build_candidate_query(limit=0, include_upstream_blockers=True)[0]),
        (f"{origin}.EXISTING_SKU_QUERY", module.EXISTING_SKU_QUERY),
        (f"{origin}.INSERT_REPAIR_SKU_SQL", module.INSERT_REPAIR_SKU_SQL),
        (f"{origin}.INSERT_REPAIR_OFFER_SQL", module.INSERT_REPAIR_OFFER_SQL),
        (f"{origin}.UPDATE_CATALOG_IMAGE_SQL", module.UPDATE_CATALOG_IMAGE_SQL),
    ]


def _collect_backfill_agent_pdp_view() -> List[Tuple[str, str]]:
    """The enriched-cohort join is the statement that matters here. It maps
    product_enrichment.platform_product_id -> catalog_products.source_product_id;
    naming the wrong column is what killed the on-write publish bridge, and here
    it would not raise — an unplannable or empty cohort query reads as "nothing
    to backfill", which is indistinguishable from success."""
    import scripts.backfill_agent_pdp_view as module

    origin = "backfill_agent_pdp_view"
    shapes = [
        ("enriched", 0, 0), ("enriched", 10, 5), ("all", 0, 0), ("all", 10, 5),
    ]
    statements = [
        (f"{origin}.build_content_key_query(scope={scope}, limit={limit}, offset={offset})",
         module.build_content_key_query(scope=scope, limit=limit, offset=offset)[0])
        for scope, limit, offset in shapes
    ]
    # The market_prices (migration 263) keyset page: its own statement, with the :after seek.
    statements += [
        (f"{origin}.build_content_key_query(scope=market_prices_missing, limit={limit}, offset=0)",
         module.build_content_key_query(scope="market_prices_missing", limit=limit, offset=0, after="ck_")[0])
        for limit in (0, 10)
    ]
    # The downgrade guard's read. It runs once per candidate on every pass,
    # including dry runs, so an unplannable version would abort the whole job.
    statements.append((f"{origin}._CURRENT_OVERLAY_SQL", module._CURRENT_OVERLAY_SQL))
    return statements


def _collect_quality_scale_population() -> List[Tuple[str, str]]:
    """Read-only report, but its statements still have to PLAN — and three of
    them carry binds (:current_rules_version, :threshold) whose types Postgres
    must infer, which is exactly the class this gate exists for."""
    import scripts.report_quality_scale_population as module

    origin = "report_quality_scale_population"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            # _LATEST is a fragment the other four embed, but it is a valid
            # standalone SELECT and the completeness guard rightly refuses to
            # let a SQL constant escape unplanned.
            "_LATEST_SQL",
            "BY_VERSION_SQL", "STALE_AND_BLOCKED_SQL", "BANDS_SQL", "BY_PLATFORM_SQL",
        )
    ]


def _collect_us_market_capture() -> List[Tuple[str, str]]:
    """The capture script WRITES offers; an unplannable statement is an
    aborted capture mid-run."""
    import scripts.capture_us_market_offers as module

    origin = "capture_us_market_offers"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            "CANDIDATES_SQL", "OFFER_UPSERT_SQL",
            # The SKU precondition, added when this lane was found to have
            # minted 529 live orphan offers. MINT_CANONICAL_SKU_SQL is an
            # INSERT ... SELECT with an ON CONFLICT on a FOUR-column index and a
            # WHERE on the DO UPDATE, and SUPPRESSED_IDENTITY_PROBE_SQL compares
            # two columns of the same row inside `= ANY(:binds)` — both are
            # shapes Postgres can refuse to plan and SQLite types not at all.
            "MINT_CANONICAL_SKU_SQL", "SUPPRESSED_IDENTITY_PROBE_SQL",
            # The suppressed-PRODUCT refusal. Separate from the SKU probe above
            # and separately plannable: this one reads catalog_products, and the
            # lane read that table's gate column nowhere until it was found
            # writing live offers onto withdrawn products.
            "SUPPRESSED_PRODUCT_PROBE_SQL",
        )
    ]


def _collect_reconcile_catalog_offers() -> List[Tuple[str, str]]:
    """The reconciler SUPPRESSES offers table-wide. An unplannable statement here
    aborts a sweep partway through and leaves the table in a state no report
    describes.

    EVERY SQL CONSTANT THE MODULE DEFINES IS LISTED, and the completeness guard
    below re-derives that list from the module so a new constant cannot be added
    without either being driven here or being named as deliberately skipped. Three
    of these reached Postgres only through a module-local `_fetch()` wrapper until
    2026-09-08, which made them invisible to the repo-wide AST sweep in
    tests/test_repo_sql_prepare_postgres.py — the sweep follows
    `database.<accessor>(CONSTANT, ...)` and nothing else. The call sites now pass
    the constant as the accessor's first positional argument, and they are driven
    here as well.
    """
    import scripts.reconcile_catalog_offers as module

    origin = "reconcile_catalog_offers"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            "ORPHAN_SELECT_SQL", "DUPLICATE_SELECT_SQL",
            "DUPLICATE_GROUP_COUNT_SQL", "CASCADE_SELECT_SQL",
            "SUPPRESS_OFFERS_SQL",
            # The revert: its per-row decision (a CASE over two anti-joins and
            # an EXISTS on the shelf tuple, with two array binds), the UPDATE
            # that moves what the decision cleared, and the audit-row read that
            # sizes `healed_since_batch`.
            "REVERT_CANDIDATES_SQL", "REVERT_BATCH_SQL", "BATCH_AUDIT_SQL",
        )
    ]


def _collect_variant_id_provenance_stamps() -> List[Tuple[str, str]]:
    """The stamping backfill WRITES, and its UPDATE builds jsonb with `jsonb_build_object` —
    the exact variadic-`"any"` shape that made #1703's statement unplannable on every row while
    the script exited 0 having written nothing. Both constants are registered here rather than
    driven, because driving the scan against a recorder yields only the SELECT: the UPDATE is
    reached only once a page comes back non-empty."""
    import scripts.backfill_variant_id_provenance_stamps as module

    origin = "backfill_variant_id_provenance_stamps"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            "SELECT_PAGE_SQL", "STAMP_SQL",
        )
    ]


def _collect_a9_4_quality_repair() -> List[Tuple[str, str]]:
    """The repair REWRITES a merchant column, so an unplannable statement here
    aborts mid-repair and leaves the cohort half-restored — the worst of both
    states. COHORT_SQL is also the tool's entire safety argument, so it has to
    plan on the production dialect before it is ever allowed to select rows."""
    import scripts.repair_a9_4_orphaned_quality_snapshots as module

    origin = "repair_a9_4_orphaned_quality_snapshots"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            "COHORT_SQL", "REPOINT_SQL", "DONOR_ROWS_SQL",
        )
    ]


def _collect_dispose_sentinel_orphans() -> List[Tuple[str, str]]:
    """The disposition tool RE-KEYS and DELETES production rows; an unplannable
    statement is an aborted run with a partially-disposed table. Its two write
    statements and both population reads are driven here."""
    import scripts.dispose_sentinel_orphans as module

    origin = "dispose_sentinel_orphans"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            "OFFERS_SQL", "REVIEWS_SQL", "BUCKET_SQL", "REVIEW_CHILDREN_SQL",
            "OFFERS_LOCKED_SQL", "REVIEWS_LOCKED_SQL",
            "OFFERS_REKEY_SQL", "REVIEWS_DELETE_SQL",
        )
    ]


# NOT covered: scripts/verify_seller_rekey.py. Its Q2 counts against
# pdp_identity_listing / _override / _review_queue, and provisioning those here
# breaks later gate files — `test_quarantine_domain_chain_postgres` issues a
# BARE `CREATE TABLE pdp_identity_listing` (no IF NOT EXISTS), so creating it in
# this shared database makes that file fail outright, and
# `test_priced_offer_gate_postgres` declares more columns than a minimal
# declaration here would leave it. Covering the verifier therefore costs two
# sibling gates, which is a bad trade for a read-only grader. The repair script
# below IS covered: it writes.


def _collect_reattribution() -> List[Tuple[str, str]]:
    """The re-attribution script WRITES (rekey + republish), so an unplannable
    statement here is an aborted repair mid-run, not just a wrong report."""
    import scripts.reattribute_orphaned_enrichment as module

    origin = "reattribute_orphaned_enrichment"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            "ORPHANS_SQL", "H0_SQL", "H1_SQL", "H2_SQL", "H3_SQL", "H4_SQL",
            "TARGET_OCCUPIED_SQL", "REKEY_SQL",
        )
    ]


def _collect_inci_quality() -> List[Tuple[str, str]]:
    import scripts.report_inci_ingestion_quality as module

    origin = "report_inci_ingestion_quality"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            "TOTALS_SQL", "GATE_FAIL_SAMPLES_SQL", "BOILERPLATE_SQL",
            "PROVENANCE_SQL", "FRESHNESS_SQL", "REACH_SQL",
        )
    ]


def _collect_depth_scorecard() -> List[Tuple[str, str]]:
    """The holistic then-vs-now scorecard. Same argument as its siblings: a
    scoreboard whose SQL cannot be planned reports nothing, and one that
    silently errors mid-collection reports a PARTIAL picture as the whole."""
    import scripts.report_agent_depth_scorecard as module

    origin = "report_agent_depth_scorecard"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            "APV_DEPTH_SQL", "INDEX_SURFACE_SQL", "INDEX_BLOCKERS_SQL",
            "INCI_INGESTED_SQL", "INCI_CAPTURED_SQL", "ENRICHMENT_INPUT_SQL",
        )
    ]


def _collect_enrichment_join_diagnosis() -> List[Tuple[str, str]]:
    """Every constant the join-diagnosis report sends. Same reasoning as the
    baseline: a diagnosis whose SQL cannot be planned is an outage mid-ops-run,
    and one that silently returns zero reads as a conclusion."""
    import scripts.report_enrichment_join_diagnosis as module

    origin = "report_enrichment_join_diagnosis"
    return [
        (f"{origin}.{name}", getattr(module, name)) for name in (
            "CLASSIFY_SQL", "BY_MERCHANT_SQL",
            "SAMPLE_ENRICHMENT_IDS_SQL", "SAMPLE_CATALOG_IDS_SQL",
            "TREND_SQL", "COHORT_STATUS_SQL",
            "COHORT_BLOCKERS_SQL", "COHORT_BLOCKED_SAMPLES_SQL",
            "COHORT_BLOCKED_SCALE_SQL", "RESCORE_REACHABILITY_SQL",
            "ORPHANED_SERVING_SQL",
            "COHORT_KEYS_SQL", "PLAIN_PRODUCT_SQL",
        )
    ]


def _collect_enrichment_baseline() -> List[Tuple[str, str]]:
    """Read-only report, but its statements still have to PLAN. A baseline that
    dies on the first query is an outage in the middle of an ops run, and one
    that silently returns zero would be read as "nothing to backfill"."""
    import scripts.report_enrichment_propagation_baseline as module

    origin = "report_enrichment_propagation_baseline"
    statements = [
        (f"{origin}.{name}", sql) for name, sql in (
            ("IDENTITY_JOIN_SANITY_SQL", module.IDENTITY_JOIN_SANITY_SQL),
            ("COHORT_SQL", module.COHORT_SQL),
            ("STRANDED_SQL", module.STRANDED_SQL),
            ("STRANDED_BY_BRAND_SQL", module.STRANDED_BY_BRAND_SQL),
        )
    ]
    statements += [
        (f"{origin}.SOURCE_COUNTS[{i}] {label.strip()}", sql)
        for i, (label, sql) in enumerate(module.SOURCE_COUNTS)
    ]
    return statements


# module path -> collector. The completeness guard below reads this same list,
# so a script cannot be registered here and left half-covered in silence.
_COVERED_SCRIPTS: Dict[str, Callable[[], List[Tuple[str, str]]]] = {
    "scripts/remediate_unpublished_crawl_rows.py": _collect_remediate,
    "scripts/withdraw_catalog_rows.py": _collect_withdraw_catalog_rows,
    "scripts/run_seed_content_audit.py": _collect_seed_content_audit,
    "scripts/source_pdp_content_repair.py": _collect_source_pdp_content_repair,
    "scripts/source_pdp_offer_image_repair.py": _collect_source_pdp_offer_image_repair,
    "scripts/backfill_agent_pdp_view.py": _collect_backfill_agent_pdp_view,
    "scripts/report_enrichment_propagation_baseline.py": _collect_enrichment_baseline,
    "scripts/report_enrichment_join_diagnosis.py": _collect_enrichment_join_diagnosis,
    "scripts/report_agent_depth_scorecard.py": _collect_depth_scorecard,
    "scripts/report_inci_ingestion_quality.py": _collect_inci_quality,
    "scripts/reattribute_orphaned_enrichment.py": _collect_reattribution,
    "scripts/capture_us_market_offers.py": _collect_us_market_capture,
    "scripts/reconcile_catalog_offers.py": _collect_reconcile_catalog_offers,
    "scripts/dispose_sentinel_orphans.py": _collect_dispose_sentinel_orphans,
    "scripts/report_quality_scale_population.py": _collect_quality_scale_population,
    "scripts/repair_a9_4_orphaned_quality_snapshots.py": _collect_a9_4_quality_repair,
    "scripts/backfill_variant_id_provenance_stamps.py": _collect_variant_id_provenance_stamps,
}

# What each collector yields TODAY, not a slack lower bound. Guards the failure
# the workflow header calls out by name: a gate that runs and asserts nothing is
# green while testing nothing. Deleting a driven call site — or a collector that
# quietly stops driving one — drops the count below these and turns the gate red
# instead of shrinking its coverage in silence. Adding statements is fine; only
# losing them needs a deliberate edit here, with the reason.
_MIN_STATEMENTS = {
    "scripts/remediate_unpublished_crawl_rows.py": 11,
    "scripts/withdraw_catalog_rows.py": 13,
    "scripts/run_seed_content_audit.py": 4,
    "scripts/source_pdp_content_repair.py": 3,
    "scripts/source_pdp_offer_image_repair.py": 6,
    "scripts/backfill_agent_pdp_view.py": 5,
    "scripts/report_enrichment_propagation_baseline.py": 12,
    "scripts/report_enrichment_join_diagnosis.py": 13,
    "scripts/report_agent_depth_scorecard.py": 6,
    "scripts/report_inci_ingestion_quality.py": 6,
    "scripts/reattribute_orphaned_enrichment.py": 8,
    "scripts/capture_us_market_offers.py": 5,
    "scripts/reconcile_catalog_offers.py": 8,
    "scripts/dispose_sentinel_orphans.py": 8,
    "scripts/report_quality_scale_population.py": 5,
    "scripts/repair_a9_4_orphaned_quality_snapshots.py": 3,
    "scripts/backfill_variant_id_provenance_stamps.py": 2,
}


# ---------------------------------------------------------------------------
# the gate
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("script", sorted(_COVERED_SCRIPTS), ids=lambda p: Path(p).stem)
def test_every_statement_the_script_sends_can_be_planned(prepare, script: str) -> None:
    statements = _COVERED_SCRIPTS[script]()

    minimum = _MIN_STATEMENTS[script]
    assert len(statements) >= minimum, (
        f"{script} yielded {len(statements)} statement(s), expected at least "
        f"{minimum}. Either a call site was removed (update _MIN_STATEMENTS with "
        "the reason) or the collector stopped driving it — a collector that "
        "yields nothing makes this gate green without planning anything."
    )

    failures: List[str] = []
    for origin, sql in statements:
        try:
            prepare(sql)
        except Exception as exc:  # noqa: BLE001 — every planning failure is in scope
            body = "\n".join("    " + line for line in sql.strip().splitlines())
            failures.append(f"  {origin}\n    !! {type(exc).__name__}: {exc}\n{body}")

    assert not failures, (
        f"{len(failures)} statement(s) in {script} cannot be PREPAREd by Postgres.\n"
        "`IndeterminateDatatypeError: could not determine data type of parameter $n` "
        "means a bind sits where Postgres cannot infer its type from position — "
        "inside a variadic \"any\" function such as jsonb_build_object, or compared "
        "against another untyped expression. Wrap it: `CAST(:x AS text)`.\n"
        + "\n".join(failures)
    )


def test_the_gate_fails_on_an_untypeable_bind_param(prepare) -> None:
    """A gate that passes both before and after the fix is worthless.

    This is the #1703 statement with and without its casts. The bad form is the
    literal SQL that died in production; if it PREPAREs here, the harness above
    proves nothing and every green run in this file is a false negative.
    """
    fixed = (
        "UPDATE catalog_products "
        "   SET suppression_metadata = COALESCE(suppression_metadata, '{}'::jsonb) "
        "       || jsonb_build_object("
        "            'script', CAST(:script AS text), "
        "            'prior_seed_status', CAST(:prior AS text)), "
        "       updated_at = NOW() "
        " WHERE source_ref = :id AND suppressed_at IS NULL"
    )
    broken = fixed.replace("CAST(:script AS text)", ":script").replace(
        "CAST(:prior AS text)", ":prior"
    )

    prepare(fixed)  # must plan

    with pytest.raises(Exception) as caught:
        prepare(broken)
    assert "could not determine data type of parameter" in str(caught.value), (
        "expected the production failure — asyncpg IndeterminateDatatypeError — "
        f"but got {type(caught.value).__name__}: {caught.value}"
    )


def test_the_cast_renderer_does_not_eat_postgres_cast_syntax() -> None:
    """`'{}'::jsonb` must survive as a cast, not become a bind named `jsonb`.

    A regex over `:\\w+` gets this wrong, and it would get it wrong SILENTLY —
    the statement would still PREPARE, just with a parameter that production
    never sends, so the gate would be planning SQL nobody runs.
    """
    got = _to_positional(
        "UPDATE t SET meta = COALESCE(meta, '{}'::jsonb) || CAST(:v AS jsonb) "
        "WHERE k = :k AND meta->>'s' = CAST(:v AS text)"
    )
    assert "'{}'::jsonb" in got
    assert ":v" not in got and ":k" not in got
    # `:v` appears twice and must reuse one placeholder, as `databases` does.
    assert got.count("$1") == 2 and "$2" in got and "$3" not in got


# ---------------------------------------------------------------------------
# completeness guard — covers the NEXT statement, not just today's
# ---------------------------------------------------------------------------
_SQL_CONSTANT = re.compile(r"(?:SQL|QUERY)$")


def _sql_constant_text(node: ast.AST) -> str | None:
    """The statically-known text of a string constant, f-string included.

    F-STRINGS WERE INVISIBLE HERE. The scan only accepted `ast.Constant`, so a
    constant assigned from an f-string that interpolates a shared fragment
    escaped this guard entirely —
    and composing SQL from a shared fragment is the normal idiom in these
    reports. scripts/report_enrichment_join_diagnosis.py has NINE such constants
    and satisfied the "declares at least one" assertion only because one
    unrelated constant happens to be a plain string. A guard that promises "no
    SQL constant escapes" while silently skipping the most common spelling is
    the weaker-than-advertised failure this file exists to prevent.

    For an f-string, the literal text up to the FIRST interpolation is the
    signature: it is all that is knowable without executing the module, and it
    is what the head-matching below already does for `.format()` templates.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts: List[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            else:
                break  # first interpolation ends the statically-known head
        return "".join(parts) or None
    return None


def _module_level_sql_constants(path: Path) -> List[Tuple[str, str]]:
    """Module-level `NAME = "...sql..."` constants whose name ends in SQL/QUERY."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: List[Tuple[str, str]] = []
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        text = _sql_constant_text(node.value)
        if text is None:
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and _SQL_CONSTANT.search(target.id):
                found.append((target.id, text))
    return found


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


@pytest.mark.parametrize("script", sorted(_COVERED_SCRIPTS), ids=lambda p: Path(p).stem)
def test_no_sql_constant_in_a_covered_script_escapes_the_gate(script: str) -> None:
    """Adding a new SQL constant to a covered script must not silently opt out.

    Without this, the gate covers the statements someone remembered to register
    on the day they wrote it — the same "authored against a defect you have
    already met" trap the workflow header warns about, one level up.

    A constant counts as covered when it is named in a collected statement's
    origin, or when its text turns up inside one (that is how the DRIVEN
    statements and the `.format()`-rendered CANDIDATE_QUERY templates register).
    """
    path = REPO_ROOT / script
    statements = _COVERED_SCRIPTS[script]()
    origins = " ".join(origin for origin, _ in statements)
    bodies = [_normalize(sql) for _, sql in statements]

    constants = _module_level_sql_constants(path)
    assert constants, (
        f"{script} is registered as covered but declares no module-level SQL "
        "constant — either the AST scan broke or the wrong file is registered."
    )

    uncovered: List[str] = []
    for name, raw in constants:
        if name in origins:
            continue
        value = _normalize(raw)
        # A `.format()` template never appears verbatim in its rendered output,
        # so match on the fixed head that precedes the first substitution.
        head = value.split("{", 1)[0].strip() if "{" in value else value
        if head and any(head in body for body in bodies):
            continue
        uncovered.append(name)

    assert not uncovered, (
        f"{script} defines SQL constant(s) no collector in this file reaches: "
        + ", ".join(sorted(uncovered))
        + ". Add them to the collector (preferably by driving the function that "
        "sends them, so the test cannot drift from the call site) — otherwise "
        "they ship unplanned, which is exactly how #1703 reached production."
    )
