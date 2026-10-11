"""DB accessors for the retailer ingest pipeline (migration 234).

Unlike db/catalog_onboard_queue.py, nothing here swallows an error: a pipeline whose ledger write
failed silently would report a store as done that never was. Every accessor raises.

Leases, not a reaper: a claim sets `lease_until`, and an expired lease makes the job claimable
again, so a run killed mid-crawl (Cloud Run task timeout, OOM) is retried by the next tick instead
of sitting in a "processing" state nothing ever clears (the gap migration 158 left open).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional

from db.database import database

logger = logging.getLogger(__name__)

STATUSES = ("queued", "apply_due", "held", "done", "nothing", "failed", "cancelled")  # migration 234 CHECK
OPEN_STATUSES = ("queued", "apply_due", "held")
DUE_STATUSES = ("queued", "apply_due")


def _dumps(value: Any) -> Optional[str]:
    return None if value is None else json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def handle_key(value: Any) -> str:
    """How a storefront handle is compared everywhere (approval lists, the pipeline's re-file and
    exclusion matching): trimmed, slashes stripped, casefolded -- as listing_handle returns it."""
    return str(value).strip().strip("/").casefold()


def scope_key(domain: str, brand: str, options: Dict[str, Any]) -> str:
    """One open job per (host, brand, scope). Operator bookkeeping (approvals, accepted flags,
    exclusions added at review) is NOT scope: approving a job must not let a duplicate enqueue."""
    scope = {k: v for k, v in (options or {}).items()
             if k not in {"accepted_flags", "exclude_handles", "refile_to_sets", "notes",
                          # a gate on the crawl, not a different cohort: re-enqueueing an open base cohort
                          # with it must answer `exists`, not open a second job over the same rows
                          "require_ships_to_market"}}
    # options.market IS scope ((host, brand, market) is one cohort), but an absent market MEANS "US"
    # (pipeline.DEFAULT_MARKET; a literal here because the pipeline imports this module). So "US" in any
    # case is dropped and every other value upper-cased: {"market": "US"} and no market at all are one
    # key, and it is the key every job queued before the option existed already has. require_currency is
    # NOT folded the same way: jobs stored with "USD" hold keys that include it, and dropping it now would
    # let a re-enqueue of such a cohort miss its open job and duplicate it.
    market = str(scope.pop("market", None) or "").strip().upper()
    if market and market != "US":
        scope["market"] = market
    raw = json.dumps({"domain": domain.strip().lower(), "brand": brand.strip(), "scope": scope},
                     sort_keys=True, ensure_ascii=False, default=str)
    return f"rij:{domain.strip().lower()}:{hashlib.sha256(raw.encode()).hexdigest()[:24]}"


async def enqueue_job(*, domain: str, brand: str, options: Dict[str, Any], priority: int = 0,
                      source: Optional[str] = None, max_attempts: int = 6, db: Any = None) -> Optional[str]:
    """Insert a queued job; returns its id, or None when the same cohort already has an open job."""
    if not domain or not brand:
        raise ValueError("a retailer ingest job needs a domain and a brand")
    write_db = db or database
    row = await write_db.fetch_one(
        """
        INSERT INTO retailer_ingest_jobs
          (id, domain, brand, options, scope_key, priority, source, max_attempts)
        VALUES
          (:id, :domain, :brand, CAST(:options AS jsonb), :scope_key, :priority, :source, :max_attempts)
        ON CONFLICT (scope_key) WHERE status IN ('queued', 'apply_due', 'held')
        DO NOTHING
        RETURNING id
        """,
        {"id": f"rij_{uuid.uuid4().hex}", "domain": domain.strip().lower(), "brand": brand.strip(),
         "options": _dumps(options or {}), "scope_key": scope_key(domain, brand, options),
         "priority": int(priority or 0), "source": source, "max_attempts": int(max_attempts or 6)},
    )
    return row["id"] if row else None


_CLAIM_LANE_LOCK_SQL = "SELECT pg_try_advisory_xact_lock(hashtext('retailer_ingest_drain')) AS mine"

#: A job's host is compared lowercased with "www." dropped: a store's single-brand re-crawl job and
#: its multi_brand store cohort are different scope_keys but ONE host. (A literal, not an f-string:
#: tests/test_repo_sql_prepare_postgres.py PREPAREs module-level literals.)
_CLAIM_SQL = """
    WITH busy AS (
        SELECT regexp_replace(lower(domain), '^www[.]', '') AS host, status FROM retailer_ingest_jobs WHERE lease_until > NOW()
    )
    UPDATE retailer_ingest_jobs
    SET lease_until = NOW() + (CAST(:lease AS integer) * interval '1 second'), updated_at = NOW()
    WHERE id = (
        SELECT id FROM retailer_ingest_jobs
        WHERE status IN ('queued', 'apply_due')
          AND next_run_at <= NOW()
          AND (lease_until IS NULL OR lease_until < NOW())
          -- at most :max_leases stages in flight (1 = one crawl at a time, the default)
          AND (SELECT count(*) FROM busy) < CAST(:max_leases AS integer)
          -- never two crawls at one host, whatever the cohort shape of the job holding it
          AND regexp_replace(lower(domain), '^www[.]', '') NOT IN (SELECT host FROM busy)
          -- two applies at different hosts may both run: an apply's crawl and checks are read-only on
          -- the catalog, and its catalog write is serialized by catalog_write_lock, not by the claim
        -- within a priority, finish paid-for work first: a clean dry run's apply frees a cohort slot,
        -- a new dry run starts more work (2026-09-24: a clean apply waited 9h behind older dry runs)
        ORDER BY priority DESC, (status = 'apply_due') DESC, next_run_at, created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING *
"""


#: Every lane is its own execution with its own pool (DB_POOL_MAX_SIZE), and every crawl leaves from
#: the one crawl NAT: a typo must not become 30 crawls.
MAX_LEASES_CEILING = 4


def max_leases_from_env() -> int:
    """RETAILER_INGEST_MAX_LEASES: stages the lane may run at once (default 1). Overlapping */10
    executions are the lanes; this caps them. Bad values fall back to 1, never to unlimited."""
    raw = str(os.getenv("RETAILER_INGEST_MAX_LEASES", "1")).strip()
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if 1 <= value <= MAX_LEASES_CEILING:
        return value
    logger.warning("RETAILER_INGEST_MAX_LEASES=%r is not 1..%d; running one lane", raw, MAX_LEASES_CEILING)
    return 1


async def claim_due_job(*, lease_seconds: int, max_leases: int = 1, db: Any = None) -> Optional[Dict[str, Any]]:
    """Claim ONE due job, or None. Up to `max_leases` stages run at once (default 1), never two at one
    host (a shared crawl IP stays polite to each store). Two applies at different hosts may both be
    claimed: an apply re-crawls and re-checks for minutes before it writes (2026-09-25: 244-2310 s per
    apply, INCI enrichment up to 499 s of it), so only the catalog WRITE is serial, under
    catalog_write_lock. Among due jobs, higher priority first; within a priority, an apply before a
    dry run.

    Two statements in one transaction, not one: the lane lock must be held BEFORE the claim takes
    its snapshot. A single statement snapshots first and locks second, so a claim that wins the
    lock just after another commits reads the lease table from before that claim, and can put a
    second crawl on the same host. An execution that loses the lock exits idle; the next tick retries.
    This relies on READ COMMITTED (every statement takes a fresh snapshot), which is what prod runs;
    under REPEATABLE READ the snapshot would be the lock statement's and the race would return.
    """
    if not 1 <= int(max_leases) <= MAX_LEASES_CEILING:
        raise ValueError(f"max_leases must be 1..{MAX_LEASES_CEILING}")
    write_db = db or database
    async with write_db.transaction():
        lane = await write_db.fetch_one(_CLAIM_LANE_LOCK_SQL)
        if not (lane and lane["mine"]):
            return None
        row = await write_db.fetch_one(_CLAIM_SQL, {"lease": int(lease_seconds), "max_leases": int(max_leases)})
    if not row:
        return None
    job = dict(row)
    if isinstance(job.get("options"), str):
        job["options"] = json.loads(job["options"])
    return job


#: Catalog writes stay serial across lanes: an apply holds this SESSION advisory lock around its
#: catalog write (apply_ingest_plan) and nothing else, so two applies' crawls and checks overlap but
#: their writes to the shared catalog / identity index never do. A key of its own: the lane key above
#: is a transaction lock the claim takes and drops in one transaction. Plain literals (no
#: parameters): tests/test_repo_sql_prepare_postgres.py PREPAREs module-level SQL.
_CATALOG_WRITE_TRY_SQL = "SELECT pg_try_advisory_lock(hashtext('retailer_ingest_catalog_write'))"
_CATALOG_WRITE_UNLOCK_SQL = "SELECT pg_advisory_unlock(hashtext('retailer_ingest_catalog_write'))"
_LOCK_STATEMENT_TIMEOUT_S = 30.0
_LOCK_CLOSE_TIMEOUT_S = 10.0


class CatalogWriteLockBusy(Exception):
    """Another apply held the catalog write lock for the whole wait; nothing was written."""

    def __init__(self, waited_s: float):
        super().__init__(f"catalog write lock busy for {waited_s:.0f}s")
        self.waited_s = waited_s


class CatalogWriteLockUnavailable(Exception):
    """The lock connection could not be opened, or a try-lock failed, before the lock was held:
    nothing was written, so the caller retries rather than failing the job."""

    def __init__(self, cause: BaseException):
        self.error_type = type(cause).__name__
        super().__init__(f"catalog write lock unavailable: {self.error_type}")


async def _open_lock_connection() -> Any:
    """A DEDICATED raw asyncpg connection, never a pool connection: `databases` 0.7 shares one
    Connection across a task and its child tasks, so a session lock taken through the pool handle
    could be held by (or released under) queries that are not this apply's, and would stay on the
    pooled connection if an unlock were missed. Closing this connection releases the lock whatever
    happened before. Same DSN and per-connection settings as the startup DDL lock."""
    import asyncpg

    from db.startup_ddl import _asyncpg_dsn, _connect_kwargs

    dsn = _asyncpg_dsn()
    if not dsn:
        # Fail closed: an apply never writes unserialized because the lock could not be built.
        raise RuntimeError("the catalog write lock needs a Postgres DATABASE_URL")
    return await asyncio.wait_for(asyncpg.connect(dsn, **_connect_kwargs()), timeout=_LOCK_STATEMENT_TIMEOUT_S)


async def _close_lock_connection(conn: Any) -> None:
    """Close, or terminate: either ends the session, and a session's advisory locks end with it.
    A failed close is logged, never raised -- after a successful write it must not fail the job."""
    try:
        await asyncio.wait_for(conn.close(), timeout=_LOCK_CLOSE_TIMEOUT_S)
    except asyncio.CancelledError:
        conn.terminate()  # synchronous: works on the cancellation path
        raise
    except Exception as exc:  # noqa: BLE001
        conn.terminate()
        logger.warning("catalog write lock: close failed, connection terminated: %s", str(exc)[:200])


@contextlib.asynccontextmanager
async def catalog_write_lock(*, wait_s: float, poll_s: float = 5.0,
                             connect: Optional[Callable[[], Awaitable[Any]]] = None,
                             clock: Callable[[], float] = time.monotonic,
                             sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep) -> AsyncIterator[float]:
    """Hold the catalog write lock for the enclosed block; yields the seconds spent waiting for it.

    Polls pg_try_advisory_lock every `poll_s` (never a blocking pg_advisory_lock: that holds a
    statement open on the waiter for the whole wait). Not acquired within `wait_s` ->
    CatalogWriteLockBusy, and the block never runs. The lock is released on every path: unlocked
    explicitly, then the connection is closed (or terminated), which releases it even when the
    unlock did not run or failed."""
    try:
        conn = await (connect or _open_lock_connection)()
    except Exception as exc:  # not BaseException: a cancellation stays a cancellation
        raise CatalogWriteLockUnavailable(exc) from exc
    try:
        started = clock()
        while True:
            try:
                acquired = await asyncio.wait_for(conn.fetchval(_CATALOG_WRITE_TRY_SQL),
                                                  timeout=_LOCK_STATEMENT_TIMEOUT_S)
            except Exception as exc:
                raise CatalogWriteLockUnavailable(exc) from exc
            if acquired:
                break
            waited = clock() - started
            if waited >= wait_s:
                raise CatalogWriteLockBusy(waited)
            await sleep(min(poll_s, wait_s - waited))
        try:
            yield clock() - started
        finally:
            try:
                released = await asyncio.wait_for(conn.fetchval(_CATALOG_WRITE_UNLOCK_SQL),
                                                  timeout=_LOCK_CLOSE_TIMEOUT_S)
                if not released:
                    # The session lost it mid-write (its connection was dropped): the write ran
                    # unserialized for part of its length. Loud, so it is found; the close below
                    # still runs.
                    logger.error("catalog write lock was no longer held at release")
            except Exception as exc:  # noqa: BLE001 -- the close below releases it
                logger.warning("catalog write lock: unlock failed, closing the connection: %s", str(exc)[:200])
    finally:
        await _close_lock_connection(conn)


#: An unfinished APPLY run's `checks.catalog_write` says whether its catalog write can have begun:
#:   "not_started"  written by start_run: the execution died before the write (crawl, checks, lock
#:                  wait) -- nothing was written, so the apply is retried like a dry run;
#:   "started"      written by mark_write_started, and COMMITTED, before apply_ingest_plan is called --
#:                  the cohort may be partial, never re-applied blindly;
#:   absent         a run started by an image older than this marker: treated as "started".
CATALOG_WRITE_NOT_STARTED = "not_started"
CATALOG_WRITE_STARTED = "started"


async def start_run(*, job_id: str, stage: str, image_sha: Optional[str], execution: Optional[str],
                    db: Any = None) -> str:
    write_db = db or database
    run_id = f"rir_{uuid.uuid4().hex}"
    checks = {"catalog_write": CATALOG_WRITE_NOT_STARTED} if stage == "apply" else None
    await write_db.execute(
        "INSERT INTO retailer_ingest_runs (id, job_id, stage, image_sha, execution, checks) "
        "VALUES (:id, :job_id, :stage, :image_sha, :execution, CAST(:checks AS jsonb))",
        {"id": run_id, "job_id": job_id, "stage": stage, "image_sha": image_sha, "execution": execution,
         "checks": _dumps(checks)},
    )
    return run_id


_MARK_WRITE_STARTED_SQL = """
    UPDATE retailer_ingest_runs
    SET checks = COALESCE(checks, '{}'::jsonb)
                 || jsonb_build_object('catalog_write', CAST(:started AS text), 'write_started_at', NOW())
    WHERE id = :id AND finished_at IS NULL
    RETURNING id
"""


async def mark_write_started(run_id: str, *, db: Any = None) -> None:
    """Record, durably, that this apply run is about to write the catalog. The caller awaits it
    BEFORE apply_ingest_plan: a crash between the two reads as "may be partial" (safe), never as
    "nothing written" (a re-apply over a partial write). Raises when no unfinished run matched, so
    the write never starts unmarked."""
    write_db = db or database
    row = await write_db.fetch_one(_MARK_WRITE_STARTED_SQL, {"id": run_id, "started": CATALOG_WRITE_STARTED})
    if not row:
        raise RuntimeError(f"cannot mark the catalog write started: no unfinished run {run_id}")


_RECORD_RETIRE_MANIFEST_SQL = """
    UPDATE retailer_ingest_runs
    SET checks = COALESCE(checks, '{}'::jsonb)
                 || jsonb_build_object('stale_brand_retire_manifest', CAST(:manifest AS jsonb))
    WHERE id = :id AND finished_at IS NULL
    RETURNING id
"""


async def record_retire_manifest(run_id: str, manifest: Dict[str, Any], *, db: Any = None) -> None:
    """Store, durably and BEFORE the write, the reversal manifest of the old-spelling retire an apply run is
    about to do (pipeline._retire_stale_brand). `retire_superseded_brand_keys revert --ingest-run` reads it.
    Raises when no unfinished run matched, so the retire never writes without its manifest."""
    write_db = db or database
    row = await write_db.fetch_one(_RECORD_RETIRE_MANIFEST_SQL, {"id": run_id, "manifest": _dumps(manifest)})
    if not row:
        raise RuntimeError(f"cannot record the retire manifest: no unfinished run {run_id}")


async def finish_run(run_id: str, *, outcome: str, crawl: Any = None, plan: Any = None,
                     checks: Any = None, flags: Any = None, applied: Any = None,
                     readback: Any = None, error: Optional[str] = None, db: Any = None) -> None:
    write_db = db or database
    await write_db.execute(
        """
        UPDATE retailer_ingest_runs
        SET outcome = :outcome, crawl = CAST(:crawl AS jsonb), plan = CAST(:plan AS jsonb),
            checks = CAST(:checks AS jsonb), flags = CAST(:flags AS jsonb),
            applied = CAST(:applied AS jsonb), readback = CAST(:readback AS jsonb),
            error = :error, finished_at = NOW()
        WHERE id = :id
        """,
        {"id": run_id, "outcome": outcome, "crawl": _dumps(crawl), "plan": _dumps(plan),
         "checks": _dumps(checks), "flags": _dumps(flags), "applied": _dumps(applied),
         "readback": _dumps(readback), "error": (error or None) and error[:8000]},
    )


async def transition(job_id: str, *, status: str, reason: Optional[str], run_id: Optional[str],
                     next_run_at: Optional[datetime] = None, count_attempt: bool = False,
                     expected_status: Optional[str] = None, db: Any = None) -> bool:
    """Move a job to its next state and release the lease; returns whether it moved.

    `expected_status` makes the move conditional on the status the stage CLAIMED: an operator who
    cancelled the job while its crawl ran must not be overwritten by the stage's verdict (review of
    #2263: cancelled -> apply_due -> applied). `count_attempt` spends retry budget (transient
    failures only: a clean dry run advancing to apply_due does not)."""
    write_db = db or database
    guard = "AND status = :expected" if expected_status else ""
    row = await write_db.fetch_one(
        f"""
        UPDATE retailer_ingest_jobs
        SET status = :status, status_reason = :reason, last_run_id = COALESCE(:run_id, last_run_id),
            next_run_at = COALESCE(CAST(:next_run_at AS timestamptz), next_run_at),
            attempts = attempts + CASE WHEN CAST(:count AS boolean) THEN 1 ELSE 0 END,
            lease_until = NULL, updated_at = NOW()
        WHERE id = :id {guard}
        RETURNING id
        """,
        {"id": job_id, "status": status, "reason": (reason or None) and reason[:2000], "run_id": run_id,
         "next_run_at": next_run_at, "count": bool(count_attempt),
         **({"expected": expected_status} if expected_status else {})},
    )
    if not row and expected_status:
        # Superseded: the stage's verdict is dropped, but its lease must not block the lane.
        await write_db.execute("UPDATE retailer_ingest_jobs SET lease_until = NULL WHERE id = :id "
                               "AND status NOT IN ('queued', 'apply_due')", {"id": job_id})
    return bool(row)


_APPROVE_READ_SQL = "SELECT options FROM retailer_ingest_jobs WHERE id = :id AND status = 'held'"

#: Compare-and-set on the options read above: an approval computed from a stale read must not land.
_APPROVE_SQL = """
    UPDATE retailer_ingest_jobs
    SET status = 'apply_due', next_run_at = NOW(), approved_by = :by, approved_at = NOW(),
        options = options || CAST(:patch AS jsonb), status_reason = 'approved', updated_at = NOW()
    WHERE id = :id AND status = 'held' AND options = CAST(:old AS jsonb)
    RETURNING id
"""


def _as_list(value: Any) -> List[str]:
    return [v for v in value if isinstance(v, str)] if isinstance(value, list) else []


async def approve(job_id: str, *, approved_by: str, exclude_handles: List[str],
                  accepted_flags: List[str], refile_handles: Optional[List[str]] = None,
                  db: Any = None) -> bool:
    """held -> apply_due, recording who approved and which rows/flags the approval covers.
    The apply re-runs every check; only the exclusions and accepted flag keys named here change
    its verdict, so an approval cannot wave through a flag that appears later.

    `refile_handles` are bundles re-filed to the gift-set shelf instead of excluded
    (options.refile_to_sets). A handle is in at most ONE of the two lists: re-filing a handle an
    earlier approval excluded MOVES it (and the reverse), because the drain refuses a job holding
    both -- as a failed job no approval can reopen (review of #2316)."""
    refile_handles = [] if refile_handles is None else refile_handles
    for name, values in (("exclude_handles", exclude_handles), ("accepted_flags", accepted_flags),
                         ("refile_handles", refile_handles)):
        if not isinstance(values, (list, tuple)) or not all(isinstance(v, str) and v.strip() for v in values):
            raise ValueError(f"{name} must be a list of non-empty strings")
    if not str(approved_by or "").strip():
        raise ValueError("approved_by is required")
    new_excl = [v.strip() for v in exclude_handles]
    new_refile = [v.strip() for v in refile_handles]
    both = sorted({handle_key(h) for h in new_excl} & {handle_key(h) for h in new_refile})
    if both:
        raise ValueError(f"a handle cannot be both re-filed and excluded: {both}")
    write_db = db or database
    row = await write_db.fetch_one(_APPROVE_READ_SQL, {"id": job_id})
    if not row:
        return False
    options = row["options"]
    if isinstance(options, str):
        options = json.loads(options)
    options = options if isinstance(options, dict) else {}
    refile_keys, excl_keys = {handle_key(h) for h in new_refile}, {handle_key(h) for h in new_excl}
    patch = {
        "exclude_handles": [h for h in _as_list(options.get("exclude_handles")) if handle_key(h) not in refile_keys]
                           + new_excl,
        "accepted_flags": _as_list(options.get("accepted_flags")) + [v.strip() for v in accepted_flags],
    }
    refiles = [h for h in _as_list(options.get("refile_to_sets")) if handle_key(h) not in excl_keys] + new_refile
    # Only a job that names a re-file carries the key: a drain image older than the option refuses it.
    if refiles or "refile_to_sets" in options:
        patch["refile_to_sets"] = refiles
    done = await write_db.fetch_one(_APPROVE_SQL, {"id": job_id, "by": approved_by,
                                                   "patch": _dumps(patch), "old": _dumps(options)})
    return bool(done)


async def cancel(job_id: str, *, by: str, reason: str, db: Any = None) -> bool:
    write_db = db or database
    row = await write_db.fetch_one(
        "UPDATE retailer_ingest_jobs SET status='cancelled', status_reason=:r, lease_until=NULL, "
        "updated_at=NOW() WHERE id=:id AND status IN ('queued','apply_due','held') "
        # A stage is running: its verdict would race the cancel. Refuse; the caller retries later.
        "AND (lease_until IS NULL OR lease_until < NOW()) RETURNING id",
        {"id": job_id, "r": f"cancelled by {by}: {reason}"[:2000]},
    )
    return bool(row)


async def list_jobs(*, status: Optional[str] = None, limit: int = 200, db: Any = None) -> List[Dict[str, Any]]:
    read_db = db or database
    where = "WHERE status = :status" if status else ""
    rows = await read_db.fetch_all(
        f"SELECT * FROM retailer_ingest_jobs {where} ORDER BY updated_at DESC LIMIT :limit",
        {"limit": int(limit), **({"status": status} if status else {})},
    )
    return [dict(r) for r in rows]


async def job_runs(job_id: str, *, db: Any = None) -> List[Dict[str, Any]]:
    read_db = db or database
    rows = await read_db.fetch_all(
        "SELECT * FROM retailer_ingest_runs WHERE job_id = :id ORDER BY started_at DESC", {"id": job_id})
    return [dict(r) for r in rows]


_RECENT_OUTCOMES_SQL = """
    SELECT outcome FROM retailer_ingest_runs
    WHERE job_id = :id AND finished_at IS NOT NULL
    ORDER BY started_at DESC, id DESC
    LIMIT :n
"""


async def consecutive_outcomes(job_id: str, outcomes: Any, *, limit: int, db: Any = None) -> int:
    """How many of the job's most recent FINISHED runs, newest first and without a break, ended in
    one of `outcomes` (at most `limit`). Derived from the ledger itself, so it survives executions."""
    read_db = db or database
    rows = await read_db.fetch_all(_RECENT_OUTCOMES_SQL, {"id": job_id, "n": int(limit)})
    count = 0
    for row in rows:
        if row["outcome"] not in outcomes:
            break
        count += 1
    return count


async def unfinished_run(job_id: str, *, db: Any = None) -> Optional[Dict[str, Any]]:
    """The job's latest run, when it never finished: its execution was killed (timeout, OOM).
    `catalog_write` is the apply's write marker (see CATALOG_WRITE_STARTED), None when absent."""
    read_db = db or database
    row = await read_db.fetch_one(
        "SELECT id, stage, started_at, finished_at, checks ->> 'catalog_write' AS catalog_write, checks "
        "FROM retailer_ingest_runs WHERE job_id = :id "
        "ORDER BY started_at DESC LIMIT 1", {"id": job_id})
    return dict(row) if row and row["finished_at"] is None else None


async def status_counts(*, db: Any = None) -> Dict[str, int]:
    read_db = db or database
    rows = await read_db.fetch_all("SELECT status, count(*) AS n FROM retailer_ingest_jobs GROUP BY status")
    return {r["status"]: int(r["n"]) for r in rows}


async def get_job(job_id: str, *, db: Any = None) -> Optional[Dict[str, Any]]:
    read_db = db or database
    row = await read_db.fetch_one("SELECT * FROM retailer_ingest_jobs WHERE id = :id", {"id": job_id})
    return dict(row) if row else None


async def recent_runs(*, limit: int = 20, db: Any = None) -> List[Dict[str, Any]]:
    """The most recent runs across every job, with the job's domain/brand and the run's outcome."""
    read_db = db or database
    rows = await read_db.fetch_all(
        """
        SELECT r.id, r.job_id, j.domain, j.brand, r.stage, r.outcome, r.error, r.image_sha,
               r.started_at, r.finished_at
        FROM retailer_ingest_runs r JOIN retailer_ingest_jobs j ON j.id = r.job_id
        ORDER BY r.started_at DESC, r.id DESC
        LIMIT :limit
        """,
        {"limit": int(limit)},
    )
    return [dict(r) for r in rows]


def backoff_until(attempts: int, *, now: Optional[datetime] = None, base_minutes: int = 30,
                  cap_hours: int = 8) -> datetime:
    """30m, 1h, 2h, 4h, 8h, 8h... -- a throttled store is retried later, never back to back."""
    now = now or datetime.now(timezone.utc)
    minutes = min(base_minutes * (2 ** max(0, int(attempts))), cap_hours * 60)
    return now + timedelta(minutes=minutes)
