"""Exercise only the extracted relgraph_sync_env function; never execute infra/gcp scripts."""
import os
import re
import subprocess
from pathlib import Path

import pytest

from tests.test_relgraph_sync_iac_env import LIVE_PROD_ENV, WRITE_GATES, VERTEX, GATEWAY_TAG

SCRIPT = Path(__file__).resolve().parents[1] / "infra/gcp/setup_scheduler.sh"
PROD_ONLY = {"RELGRAPH_SYNC_REVIEW_LIMIT", "RELGRAPH_SYNC_REVIEW_CONCURRENCY", "RELGRAPH_SYNC_PRIORITIZE_UNCOVERED"}


def env_function(env_name, writes):
    source = SCRIPT.read_text()
    match = re.search(r"^relgraph_sync_env\(\)\{\n.*?^\}", source, re.M | re.S)
    assert match
    proc = subprocess.run(["bash", "-c", match.group() + "\nrelgraph_sync_env"],
        capture_output=True, text=True, check=True, env={**os.environ, "ENV": env_name,
            "PIVOTA_ENV": "production" if env_name == "prod" else "staging",
            "GATEWAY_TAG": GATEWAY_TAG, "PROJECT": "pivota-prod" if env_name == "prod" else "pivota-staging",
            "RELGRAPH_SYNC_WRITES": writes})
    pairs = [item.split("=", 1) for item in proc.stdout.split(",")]
    assert len(pairs) == len({key for key, _ in pairs}), "duplicate env keys"
    return dict(pairs)


def test_prod_target_env():
    env = env_function("prod", "true")
    assert env.pop("PIVOTA_COMMIT_SHA") == GATEWAY_TAG
    assert env == LIVE_PROD_ENV


def test_prod_grows_coverage_with_bounded_review():
    env = env_function("prod", "true")
    assert env["RELGRAPH_SYNC_PRIORITIZE_UNCOVERED"] == "true"
    assert (env["RELGRAPH_SYNC_REVIEW_LIMIT"], env["RELGRAPH_SYNC_REVIEW_CONCURRENCY"]) == ("1000", "6")
    assert env["RELGRAPH_SYNC_STEP_TIMEOUT_MINUTES"] == "90"


@pytest.mark.parametrize("env_name,writes", [("prod", "false"), ("staging", "false")])
def test_dry_run_and_staging_are_inert(env_name, writes):
    env = env_function(env_name, writes)
    assert WRITE_GATES.isdisjoint(env)
    if env_name == "prod":
        assert VERTEX <= set(env)
    else:
        assert VERTEX.isdisjoint(env)
        assert PROD_ONLY.isdisjoint(env)
        assert env["RELGRAPH_SYNC_STEP_TIMEOUT_MINUTES"] == "45"


@pytest.mark.parametrize("env_name", ["prod", "staging"])
def test_anchor_caps_stay_absent(env_name):
    env = env_function(env_name, "false")
    assert "RELGRAPH_SYNC_LIMIT" not in env
    assert "RELGRAPH_SYNC_SELECT_LIMIT" not in env
