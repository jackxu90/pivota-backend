"""The shared purchase vocabulary, and each rail's map onto it.

A rail keeps its own state machine and its own words for it. Agents reading the unified purchase
see ONE vocabulary whatever the rail, so every rail state must map to exactly one unified state.
tests/test_agent_purchase_ledger.py holds the Reap map to the Reap ledger's full state set, so a
new Reap state that is not mapped here fails the build instead of reaching an agent as `None`.

Executor `rail_managed`: the rail itself places the merchant order (Reap today;
the next rail's checkout agent likewise). With one rail there is nothing to route yet, so there is no router here:
the parent row records how its rail was chosen, and the router arrives with the second rail.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, Optional

#: The unified states, in the order a purchase normally moves through them.
UNIFIED_STATES = (
    "routing",
    "needs_payment_method",
    "locking",
    "awaiting_buyer_authorization",
    "placing",
    "completed",
    "failed",
    "refused",
    "expired",
)

UNIFIED_TERMINAL_STATES: FrozenSet[str] = frozenset({"completed", "failed", "refused", "expired"})

#: Reap's ledger states (db/reap_agentic_ledger.PURCHASE_STATES) onto the unified ones.
#:   resolving         -> routing: matching our catalog row to the rail's product
#:   needs_enrollment  -> needs_payment_method: the buyer adds a card on the rail's page
#:   quoting           -> locking: the rail prices the exact cart; nothing is charged
#:   awaiting_approval -> awaiting_buyer_authorization: the buyer approves on the rail's page
#:   processing        -> placing: the rail places the order with the merchant
REAP_STATE_MAP: Dict[str, str] = {
    "resolving": "routing",
    "needs_enrollment": "needs_payment_method",
    "quoting": "locking",
    "awaiting_approval": "awaiting_buyer_authorization",
    "processing": "placing",
    "completed": "completed",
    "failed": "failed",
    "refused": "refused",
    "expired": "expired",
}

#: rail -> (executor, state map). A purchase's executor is fixed per rail until a rail offers more
#: than one way to place an order.
RAIL_EXECUTOR: Dict[str, str] = {"reap": "rail_managed"}
_STATE_MAPS: Dict[str, Dict[str, str]] = {"reap": REAP_STATE_MAP}

#: A rail's own purchase id prefix -> rail. The unified routes accept a rail id wherever a `pp_` id is
#: accepted; this is the one place that says which rail an id belongs to.
RAIL_ID_PREFIX: Dict[str, str] = {"rp_": "reap"}

#: The unified states in which the buyer must act on a rail-hosted page, and what that page is for.
BUYER_ACTION_KIND: Dict[str, str] = {
    "needs_payment_method": "card_binding",
    "awaiting_buyer_authorization": "approval",
}


def unified_state(rail: str, rail_state: Optional[str]) -> Optional[str]:
    """The unified state for a rail's state, or None for a state this module does not know.

    None, not a guess: a caller that gets None must not tell an agent anything about the
    purchase's progress. The test above makes None unreachable for every state Reap can store.
    """
    return _STATE_MAPS.get(rail, {}).get(str(rail_state or ""))
