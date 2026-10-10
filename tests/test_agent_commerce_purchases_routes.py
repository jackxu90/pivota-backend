"""routes/agent_commerce_purchases.py: the rail-neutral purchase read, on SQLite, over the REAL app.

Only the two auth dependencies are overridden. No route here may reach the network (`_no_network`).
Purchases are opened with the Reap ledger directly: these routes read, and how a Reap purchase
is created is tests/test_agent_commerce_reap_routes.py's business (including the post-commit
parent write, which is tested there).
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, Optional

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db.database import IS_POSTGRES, database  # noqa: E402
from db.schema_guard import ensure_required_schema_light  # noqa: E402
from db.buyer_vault import hash_agent_user_ref  # noqa: E402
import db.agent_purchase_ledger as purchases  # noqa: E402
import db.reap_agentic_ledger as reap_ledger  # noqa: E402
import routes.agent_commerce_purchases as unified_routes  # noqa: E402
from routes.agent_auth import get_agent_context  # noqa: E402
from routes.agent_user_auth import AgentUserContext, get_agent_user_context  # noqa: E402

from main import app  # noqa: E402

_REAL_ASYNC_CLIENT = httpx.AsyncClient

pytestmark = pytest.mark.skipif(
    IS_POSTGRES, reason="the SQLite arm of the unified purchase routes"
)

BASE = "/agent/v2/commerce/purchases"
REAP_BASE = "/agent/v2/commerce/reap"
AGENT = "agent_unified_routes"
OTHER_AGENT = "agent_unified_routes_other"
USER_REF = "user-ada"
OTHER_USER_REF = "user-grace"
EMAIL = "ada@example.test"
STREET = "900 Brannan St"


@pytest.fixture(autouse=True)
async def _db():
    if not database.is_connected:
        await database.connect()
    for table in ("agent_purchases", "reap_agentic_purchases", "reap_agentic_enrollments"):
        await database.execute(f"DROP TABLE IF EXISTS {table}")
    await ensure_required_schema_light()
    yield


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv(purchases.AGENT_PURCHASE_LEDGER_ENABLED_ENV, "1")
    # The Reap rail itself is DARK: stored purchases must stay readable through the unified route
    # exactly as they stay readable through Reap's own GET.
    monkeypatch.delenv("REAP_AGENTIC_ENABLED", raising=False)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    class _NetworkForbidden:
        def __init__(self, *a, **k):
            raise AssertionError("a read route reached the network")

    monkeypatch.setattr(httpx, "AsyncClient", _NetworkForbidden, raising=True)


class _Caller:
    agent_id = AGENT
    agent_user_ref: Optional[str] = USER_REF
    agent_name = "Unified Routes Test"
    allowed_merchants = None
    session_id = "session_unified_routes"

    def can_access_merchant(self, merchant_id: str) -> bool:
        return True


CALLER = _Caller()


@pytest.fixture
def client():
    CALLER.agent_id = AGENT
    CALLER.agent_user_ref = USER_REF

    async def _agent():
        return CALLER

    def _user():
        ref = CALLER.agent_user_ref
        return AgentUserContext(agent_user_ref=ref) if ref else None

    app.dependency_overrides[get_agent_context] = _agent
    app.dependency_overrides[get_agent_user_context] = _user

    class _Harness:
        async def get(self, url, **kwargs):
            transport = httpx.ASGITransport(app=app)
            async with _REAL_ASYNC_CLIENT(transport=transport, base_url="http://test") as http:
                return await http.get(url, **kwargs)

    try:
        yield _Harness()
    finally:
        app.dependency_overrides.pop(get_agent_context, None)
        app.dependency_overrides.pop(get_agent_user_context, None)


async def _reap(*, agent_id: str = AGENT, user_ref: str = USER_REF, state: str = "resolving",
                **columns: Any) -> str:
    purchase = await reap_ledger.create_purchase(
        buyer_ref="bref_ada",
        agent_id=agent_id,
        agent_user_ref_hash=hash_agent_user_ref(user_ref),
        merchant_domain="brand.example",
        product_key="pk_1",
        product_name="Glow Serum",
        quantity=1,
        currency="USD",
        our_price_minor=4250,
        buyer_email=EMAIL,
        shipping_address={"addressLine1": STREET, "city": "San Francisco"},
    )
    updates = {"state": state, **columns}
    assignments = ", ".join(f"{k} = :{k}" for k in updates)
    await database.execute(
        f"UPDATE reap_agentic_purchases SET {assignments} WHERE id = :id",
        {**updates, "id": purchase["id"]},
    )
    return purchase["id"]


def _error(resp) -> Optional[str]:
    """The route's reason, out of the app-wide error envelope (`detail.error`), as
    tests/test_agent_commerce_reap_routes.py reads it."""
    payload = resp.json()
    detail = payload.get("detail")
    if isinstance(detail, dict):
        return detail.get("error")
    error = payload.get("error")
    if isinstance(error, dict) and isinstance(error.get("details"), dict):
        return error["details"].get("error")
    return None


async def _parents():
    return [dict(r) for r in await database.fetch_all("SELECT * FROM agent_purchases")]


# ── mounting and the dial ────────────────────────────────────────────────────────────────────


def test_the_router_is_mounted_on_the_real_app():
    paths = {getattr(r, "path", None) for r in app.routes}
    assert {BASE, f"{BASE}/{{purchase_id}}"} <= paths


@pytest.mark.parametrize("url", [f"{BASE}/pp_anything", f"{BASE}/rp_anything", f"{BASE}/%00", BASE,
                                 f"{BASE}?limit=banana"])
async def test_a_dark_ledger_answers_the_same_404_for_every_request(client, monkeypatch, url):
    monkeypatch.delenv(purchases.AGENT_PURCHASE_LEDGER_ENABLED_ENV, raising=False)
    resp = await client.get(url)
    assert resp.status_code == 404
    assert _error(resp) == "not_available"


async def test_a_dark_ledger_writes_no_parent_even_for_a_real_purchase(client, monkeypatch):
    monkeypatch.delenv(purchases.AGENT_PURCHASE_LEDGER_ENABLED_ENV, raising=False)
    rp = await _reap()
    await client.get(f"{BASE}/{rp}")
    await client.get(BASE)
    assert await _parents() == []


@pytest.mark.parametrize("url", [f"{BASE}/pp_x", BASE])
async def test_every_route_needs_an_agent_user(client, url):
    CALLER.agent_user_ref = None
    resp = await client.get(url)
    assert resp.status_code == 401


# ── the single read ──────────────────────────────────────────────────────────────────────────


async def test_a_reap_id_heals_its_parent_and_reads_unified(client):
    rp = await _reap()
    resp = await client.get(f"{BASE}/{rp}")
    assert resp.status_code == 200
    body = resp.json()
    [parent] = await _parents()
    assert body["purchase_id"] == parent["id"] and body["purchase_id"].startswith("pp_")
    assert body["rail"] == "reap" and body["executor"] == "rail_managed"
    assert body["rail_purchase_id"] == rp
    assert body["state"] == "routing" and body["rail_state"] == "resolving"
    assert json.loads(parent["routing_plan"])["basis"] == "backfill"


async def test_the_parent_id_reads_the_same_purchase(client):
    rp = await _reap()
    by_rp = (await client.get(f"{BASE}/{rp}")).json()
    by_pp = (await client.get(f"{BASE}/{by_rp['purchase_id']}")).json()
    assert by_pp == by_rp


async def test_the_detail_is_exactly_what_the_reap_route_returns(client):
    rp = await _reap(state="quoting")
    unified = (await client.get(f"{BASE}/{rp}")).json()
    reap = (await client.get(f"{REAP_BASE}/purchases/{rp}")).json()
    assert unified["detail"] == reap
    assert unified["totals"] == reap["totals"]
    assert unified["state"] == "locking"


async def test_no_buyer_pii_or_internal_ids_reach_the_response(client):
    rp = await _reap()
    text = (await client.get(f"{BASE}/{rp}")).text
    for needle in (EMAIL, STREET, "bref_ada", hash_agent_user_ref(USER_REF), AGENT):
        assert needle not in text


@pytest.mark.parametrize("agent_id,user_ref", [(OTHER_AGENT, USER_REF), (AGENT, OTHER_USER_REF)])
async def test_another_owners_purchase_is_not_found_and_gets_no_parent(client, agent_id, user_ref):
    rp = await _reap(agent_id=agent_id, user_ref=user_ref)
    resp = await client.get(f"{BASE}/{rp}")
    assert resp.status_code == 404 and _error(resp) == "purchase_not_found"
    assert await _parents() == []

    pp = await purchases.ensure_reap_parent(rp)
    resp = await client.get(f"{BASE}/{pp}")
    assert resp.status_code == 404 and _error(resp) == "purchase_not_found"


@pytest.mark.parametrize("purchase_id", ["pp_nope", "rp_nope", "ord_123", "pp_" + "x" * 80])
async def test_unknown_or_malformed_ids_are_not_found(client, purchase_id):
    resp = await client.get(f"{BASE}/{purchase_id}")
    assert resp.status_code == 404 and _error(resp) == "purchase_not_found"


async def test_an_approval_wait_carries_one_open_url_action(client):
    rp = await _reap(
        state="awaiting_approval",
        hosted_url="https://pay.prava.space/approve/abc",
        hosted_url_expires_at="2999-01-01 00:00:00",
        reap_quote_expires_at="2999-01-01 00:00:00",
    )
    body = (await client.get(f"{BASE}/{rp}")).json()
    assert body["state"] == "awaiting_buyer_authorization"
    assert body["next_action"]["type"] == "open_url"
    assert body["next_action"]["kind"] == "approval"
    assert body["next_action"]["url"] == "https://pay.prava.space/approve/abc"
    assert body["next_action"]["expires_at"] == body["detail"]["approval_deadline"]


async def test_a_url_the_rail_route_drops_is_never_offered(client):
    rp = await _reap(
        state="awaiting_approval",
        hosted_url="https://evilprava.space/approve/steal",
        hosted_url_expires_at="2999-01-01 00:00:00",
        reap_quote_expires_at="2999-01-01 00:00:00",
    )
    resp = await client.get(f"{BASE}/{rp}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["state"] == "awaiting_buyer_authorization"
    assert "next_action" not in body
    assert "evilprava" not in json.dumps(body)


async def test_a_completed_purchase_carries_its_order_reference(client):
    rp = await _reap(state="completed", reap_order_id="ord_merchant_1", final_total_minor=4650)
    body = (await client.get(f"{BASE}/{rp}")).json()
    assert body["state"] == "completed"
    assert body["order_reference"] == "ord_merchant_1"
    assert "next_action" not in body


async def test_an_unmapped_rail_state_is_a_503_not_a_guess(client, monkeypatch):
    rp = await _reap()
    monkeypatch.setattr(unified_routes, "unified_state", lambda rail, state: None)
    resp = await client.get(f"{BASE}/{rp}")
    assert resp.status_code == 503 and _error(resp) == "state_unmapped"


# ── the list ─────────────────────────────────────────────────────────────────────────────────


async def test_the_list_heals_and_returns_only_the_owners_purchases_newest_first(client):
    older = await _reap()
    await database.execute(
        "UPDATE reap_agentic_purchases SET created_at = datetime('now', '-1 day') WHERE id = :i",
        {"i": older},
    )
    newer = await _reap(state="completed", reap_order_id="ord_2")
    await _reap(agent_id=OTHER_AGENT)
    resp = await client.get(BASE)
    assert resp.status_code == 200
    body = resp.json()
    assert [p["rail_purchase_id"] for p in body["purchases"]] == [newer, older]
    assert [p["state"] for p in body["purchases"]] == ["completed", "routing"]
    assert body["limit"] == 20
    assert len(await _parents()) == 2


@pytest.mark.parametrize("limit", ["0", "101", "banana"])
async def test_a_bad_limit_is_refused_not_clamped(client, limit):
    resp = await client.get(f"{BASE}?limit={limit}")
    assert resp.status_code == 400


async def test_a_failing_heal_never_fails_the_list(client, monkeypatch):
    rp = await _reap()
    await purchases.ensure_reap_parent(rp)

    async def _boom(*a, **k):
        raise RuntimeError("heal down")

    monkeypatch.setattr(purchases, "heal_reap_parents_for_owner", _boom)
    resp = await client.get(BASE)
    assert resp.status_code == 200
    assert [p["rail_purchase_id"] for p in resp.json()["purchases"]] == [rp]
