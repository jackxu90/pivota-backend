"""relgraph-sync's env as the REAL setup_scheduler.sh writes it, against a fake gcloud.

`--set-env-vars` REPLACES a Cloud Run job's whole env, so whatever this script passes IS the job's
env after a reconcile. Before 2026-09-30 the relgraph write gates, the Vertex settings and the step
budget lived only on the live job, set by hand, and any reconcile silently turned graph writes off
and broke AI review. These tests pin the reconciled env to the live job's, var for var, and pin that
staging never receives the write gates or the prod Vertex project.

The script runs end to end with the same fake gcloud as test_setup_scheduler_is_safe_to_rerun.py:
it records argv and never reaches a cloud API.
"""
import os
import subprocess
from pathlib import Path

import pytest

from tests.test_setup_scheduler_is_safe_to_rerun import (
    EXISTING_TRIGGERS,
    FAKE_GCLOUD,
    REPO,
    REVISION_JSON,
    SCRIPT,
    WEB_JSON,
)

# The live job's env on 2026-09-30 (`gcloud run jobs describe relgraph-sync`), minus
# PIVOTA_COMMIT_SHA, which every reconcile restamps from the gateway tag.
LIVE_PROD_ENV = {
    "PIVOTA_ENV": "production",
    "PIVOTA_SERVICE_NAME": "relgraph-sync",
    "DB_POOL_MAX": "3",
    "PCI_KB_DB_POOL_MAX": "1",
    "INGREDIENT_REFERENCE_DB_POOL_MAX": "1",
    "INGREDIENT_SIGNAL_DB_POOL_MAX": "1",
    "RELGRAPH_SYNC_APPLY_BUILD": "true",
    "RELGRAPH_SYNC_APPLY_REVIEW": "true",
    "RELGRAPH_SYNC_ALLOW_WRITES": "true",
    "RELGRAPH_SYNC_CONFIRM": "APPLY_RELGRAPH_SYNC_ROUTINE",
    "RELGRAPH_SYNC_STEP_TIMEOUT_MINUTES": "90",
    "RELGRAPH_SYNC_REVIEW_LIMIT": "1000",
    "RELGRAPH_SYNC_REVIEW_CONCURRENCY": "6",
    "RELGRAPH_SYNC_PRIORITIZE_UNCOVERED": "true",
    "VERTEX_AI_ENABLED": "true",
    "GOOGLE_CLOUD_PROJECT": "pivota-prod",
    "GOOGLE_CLOUD_LOCATION": "global",
    "GCE_METADATA_HOST": "metadata.google.internal",
}
WRITE_GATES = {"RELGRAPH_SYNC_APPLY_BUILD", "RELGRAPH_SYNC_APPLY_REVIEW", "RELGRAPH_SYNC_ALLOW_WRITES",
               "RELGRAPH_SYNC_CONFIRM"}
VERTEX = {"VERTEX_AI_ENABLED", "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION", "GCE_METADATA_HOST"}
GATEWAY_TAG = "b" * 40


def _reconcile(tmp_path: Path, env_name: str, overrides: dict[str, str | None], *, expect_rc: int = 0):
    fake = tmp_path / "fake-gcloud"
    fake.write_text(FAKE_GCLOUD)
    fake.chmod(0o755)
    log = tmp_path / "calls.log"
    log.touch()
    existing = tmp_path / "existing.txt"
    existing.write_text("\n".join(sorted(EXISTING_TRIGGERS)) + "\n")
    env = {
        **os.environ,
        "GCLOUD": str(fake),
        "GCLOUD_LOG": str(log),
        "EXISTING_TRIGGERS_FILE": str(existing),
        "WEB_JSON": WEB_JSON,
        "REVISION_JSON": REVISION_JSON,
        "STORE_AUDIT_UCP_REPROBE_WORKER": "true",
        "STORE_AUDIT_UCP_REPROBE_ARMED": "true",
        "STORE_AUDIT_UCP_PROBE_BACKEND_BASE_URL": "https://web-gpx4jyrubq-uw.a.run.app",
        "STORE_AUDIT_COMMERCE_REPROBE_WORKER": "true",
        "STORE_AUDIT_COMMERCE_REPROBE_ARMED": "true",
        "STORE_AUDIT_COMMERCE_PROBE_BACKEND_BASE_URL": "https://web-gpx4jyrubq-uw.a.run.app",
    }
    env.pop("RELGRAPH_SYNC_WRITES", None)
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    proc = subprocess.run(["bash", str(SCRIPT), env_name, "a" * 40, GATEWAY_TAG],
                          capture_output=True, text=True, env=env, cwd=str(REPO))
    assert proc.returncode == expect_rc, f"rc={proc.returncode}\nstderr:\n{proc.stderr[-3000:]}"
    return log.read_text().splitlines(), proc


def _relgraph_sync_env(calls: list[str]) -> dict[str, str]:
    lines = [c for c in calls if c.startswith("run jobs ") and c.split()[3] == "relgraph-sync"
             and "--set-env-vars" in c.split()]
    assert len(lines) == 1, lines
    tokens = lines[0].split()
    raw = tokens[tokens.index("--set-env-vars") + 1]
    pairs = [item.split("=", 1) for item in raw.split(",")]
    keys = [k for k, _ in pairs]
    assert len(keys) == len(set(keys)), f"duplicate env keys: {keys}"
    return dict(pairs)


def test_a_prod_reconcile_reproduces_the_live_env_var_for_var(tmp_path):
    env = _relgraph_sync_env(_reconcile(tmp_path, "prod", {})[0])
    assert env.pop("PIVOTA_COMMIT_SHA") == GATEWAY_TAG
    assert env == LIVE_PROD_ENV


def test_review_limit_rises_only_with_concurrency_and_anchor_caps_stay_default(tmp_path):
    # 1,000 sequential reviews (~7 s each) blow the step every night; the raise is only safe with
    # bounded review concurrency (PIVOTA-Agent #2335, live since 2026-10-01): measured 2026-10-10,
    # 1,000 reviews at concurrency 6 take ~27 minutes. The anchor caps stay at the image defaults.
    env = _relgraph_sync_env(_reconcile(tmp_path, "prod", {})[0])
    assert (env["RELGRAPH_SYNC_REVIEW_LIMIT"], env["RELGRAPH_SYNC_REVIEW_CONCURRENCY"]) == ("1000", "6")
    for cap in ("RELGRAPH_SYNC_LIMIT", "RELGRAPH_SYNC_SELECT_LIMIT"):
        assert cap not in env


def test_prod_can_be_reconciled_as_a_dry_run(tmp_path):
    env = _relgraph_sync_env(_reconcile(tmp_path, "prod", {"RELGRAPH_SYNC_WRITES": "false"})[0])
    assert WRITE_GATES.isdisjoint(env)
    assert VERTEX <= set(env)  # dry-run review still needs the model


def test_staging_gets_neither_write_gates_nor_the_prod_vertex_project(tmp_path):
    env = _relgraph_sync_env(_reconcile(tmp_path, "staging", {})[0])
    assert env["PIVOTA_ENV"] == "staging"
    assert WRITE_GATES.isdisjoint(env)
    assert VERTEX.isdisjoint(env)
    assert "pivota-prod" not in ",".join(f"{k}={v}" for k, v in env.items())


def test_staging_refuses_writes_before_touching_anything(tmp_path):
    calls, proc = _reconcile(tmp_path, "staging", {"RELGRAPH_SYNC_WRITES": "true"}, expect_rc=2)
    assert "refused in staging" in proc.stderr
    assert calls == []


@pytest.mark.parametrize("value", ["yes", "1", "TRUE"])
def test_a_malformed_switch_is_refused(tmp_path, value):
    calls, proc = _reconcile(tmp_path, "prod", {"RELGRAPH_SYNC_WRITES": value}, expect_rc=2)
    assert "RELGRAPH_SYNC_WRITES must be" in proc.stderr
    assert calls == []
