"""`/agent/v2/commerce/purchases`: the rail-neutral purchase read (payment orchestration P0).

One id (`pp_…`) and one state vocabulary for a purchase on any payment rail, with the rail's own
owner-facing body nested under `detail` exactly as the rail's route returns it. Reap is the only
rail today; a Reap id (`rp_…`) is accepted too, so a caller holding the id from
`POST /agent/v2/commerce/reap/purchases` can read the unified view without a lookup.

READ-ONLY, AND DARK BY DEFAULT. Nothing here creates, advances or pays for a purchase, and
nothing calls a rail. While `AGENT_PURCHASE_LEDGER_ENABLED` is off every request answers the
same 404, whatever its shape.

OWNERSHIP IS THE RAIL'S AND THE LEDGER'S, IN SQL. The parent read conjuncts on the agent and the
end user (db/agent_purchase_ledger.py), and the nested detail is read through the Reap route's own
owner-scoped `_owner_view`. "Not yours" and "does not exist" are the same 404.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

import db.agent_purchase_ledger as purchases
import db.reap_agentic_ledger as reap_ledger
import routes.agent_commerce_reap as reap_routes
import services.reap_agentic_purchase as reap_svc
from db.buyer_vault import hash_agent_user_ref
from routes.agent_auth import AgentContext, get_agent_context
from routes.agent_user_auth import AgentUserContext, get_agent_user_context
from services.payment_orchestration.rails import BUYER_ACTION_KIND, unified_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent/v2/commerce/purchases", tags=["agent-commerce-purchases"])

#: Longest id either prefix can carry (both columns are VARCHAR(64)).
_MAX_ID_CHARS = 64


def _dark() -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "not_available"})


def _owner_hash(agent_user: Optional[AgentUserContext]) -> str:
    agent_user_ref = reap_routes._require_agent_user(agent_user)
    agent_user_ref_hash = hash_agent_user_ref(agent_user_ref)
    if not agent_user_ref_hash:
        raise reap_svc.PurchaseRefused("agent_user_required")
    return agent_user_ref_hash


async def _parent_for(purchase_id: str, agent_id: str, owner: str) -> Optional[Dict[str, Any]]:
    """The owner's parent row for a `pp_` or `rp_` id, healing a Reap purchase that has none yet.

    The heal runs only after the Reap ledger's own owner-scoped read has said the purchase is
    this caller's, so it can never create a parent for somebody else's purchase.
    """
    if purchase_id.startswith("pp_"):
        return await purchases.get_for_owner(purchase_id, agent_id, owner)
    if not purchase_id.startswith("rp_"):
        return None
    parent = await purchases.get_by_rail_id_for_owner("reap", purchase_id, agent_id, owner)
    if parent is not None:
        return parent
    if await reap_ledger.get_purchase_for_owner(purchase_id, agent_id, owner) is None:
        return None
    await purchases.ensure_reap_parent(purchase_id, basis="backfill")
    return await purchases.get_by_rail_id_for_owner("reap", purchase_id, agent_id, owner)


def _unified_body(parent: Mapping[str, Any], detail: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """The rail-neutral shape. None when the rail state has no unified mapping, which the caller
    turns into a 503 rather than telling an agent something made up about the purchase."""
    rail_state = detail.get("state")
    state = unified_state(parent["rail"], rail_state)
    if state is None:
        return None
    body: Dict[str, Any] = {
        "purchase_id": parent["id"],
        "rail": parent["rail"],
        "executor": parent["executor"],
        "rail_purchase_id": parent["rail_purchase_id"],
        "state": state,
        "rail_state": rail_state,
        "created_at": parent.get("created_at"),
        "totals": detail.get("totals"),
        "poll_after_seconds": detail.get("poll_after_seconds"),
    }
    # The rail route has already vetted the hosted URL against its allowlist and its deadline and
    # dropped it when it fails; an absent `hosted_url` therefore means "nowhere to send the buyer".
    kind = BUYER_ACTION_KIND.get(state)
    if kind and detail.get("hosted_url"):
        body["next_action"] = {
            "type": "open_url",
            "kind": kind,
            "url": detail["hosted_url"],
            "expires_at": detail.get("approval_deadline") or detail.get("hosted_url_expires_at"),
        }
    if state == "completed" and detail.get("order_reference"):
        body["order_reference"] = detail["order_reference"]
    body["detail"] = dict(detail)
    return body


async def _unified_for(parent: Mapping[str, Any], agent_id: str, owner: str):
    """(body, None) on success, (None, response) when the purchase cannot be answered."""
    detail = await reap_routes._owner_view(
        purchase_id=str(parent["rail_purchase_id"]), agent_id=agent_id, agent_user_ref_hash=owner
    )
    if not detail:
        return None, reap_routes._not_found()
    body = _unified_body(parent, detail)
    if body is None:
        logger.warning(
            "agent_purchases: unmapped rail state rail=%s purchase=%s", parent["rail"], parent["id"]
        )
        return None, JSONResponse(status_code=503, content={"error": "state_unmapped"})
    return body, None


@router.get("/{purchase_id}")
async def get_purchase(
    purchase_id: str,
    context: AgentContext = Depends(get_agent_context),
    agent_user: Optional[AgentUserContext] = Depends(get_agent_user_context),
):
    if not purchases.is_enabled():
        return _dark()
    try:
        owner = _owner_hash(agent_user)
    except reap_svc.PurchaseRefused as exc:
        return reap_routes._refused(exc)

    purchase_id = str(purchase_id or "").strip()
    if not purchase_id or len(purchase_id) > _MAX_ID_CHARS:
        return reap_routes._not_found()
    agent_id = str(context.agent_id)
    parent = await _parent_for(purchase_id, agent_id, owner)
    if parent is None:
        return reap_routes._not_found()
    body, error = await _unified_for(parent, agent_id, owner)
    return error if error is not None else body


@router.get("")
async def list_purchases(
    request: Request,
    context: AgentContext = Depends(get_agent_context),
    agent_user: Optional[AgentUserContext] = Depends(get_agent_user_context),
):
    if not purchases.is_enabled():
        return _dark()
    try:
        owner = _owner_hash(agent_user)
        limit = reap_routes._limit_from(request)
    except reap_svc.PurchaseRefused as exc:
        return reap_routes._refused(exc)

    agent_id = str(context.agent_id)
    # This owner's Reap purchases that predate the ledger (or whose post-commit write failed)
    # get their parent now, so the first page is complete. A failure here only means the page
    # may miss rows the backfill will add; it never fails the read.
    try:
        await purchases.heal_reap_parents_for_owner(agent_id, owner)
    except Exception as exc:  # noqa: BLE001
        logger.warning("agent_purchases: owner heal failed error_type=%s", type(exc).__name__)

    out = []
    for parent in await purchases.list_for_owner(agent_id, owner, limit=limit):
        body, _error = await _unified_for(parent, agent_id, owner)
        if body is not None:
            out.append(body)
    return {"purchases": out, "limit": limit}
