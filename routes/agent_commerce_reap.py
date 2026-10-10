"""The agent-facing door onto the Reap agentic purchase rail (WP4).

Routes over the machinery WP1–WP3 built. This module owns NO state and makes NO partner
call: it decides whether a purchase may be opened, builds the one `PurchaseRow` that
`services.reap_agentic_purchase.start_purchase` will trust, and reads rows back to the buyer they
belong to. Everything that talks to Reap happens later, in the poller, on another process.

── WHAT 404 MEANS HERE ──────────────────────────────────────────────────────────────────────

While the base rail is off or lacks credentials, create and list return
**404 `not_available_on_this_rail`**. A create-only pause also blocks new purchases.
Once the buyer selects Reap, unavailable or ambiguous responses never authorize another
rail, a cart-link retry, or a replacement idempotency key. Preserve the original attempt
and use authenticated, read-only recovery when its outcome is uncertain.

The stored purchase-by-ID GET and original-attempt recovery remain authenticated and
owner-scoped while disarmed. They make no provider calls. List remains base-rail gated.
Create parses its raw request after admission checks; list parses its limit after its
base gate. Authentication dependencies run first, so unauthenticated callers receive 401.
The mounted paths remain visible in `/openapi.json` regardless of admission settings.

── WHAT THIS ROUTE REFUSES TO TRUST ─────────────────────────────────────────────────────────

**The price is never the caller's.** The request names a product; it does not name a price, and
if it did this module would ignore it. `_load_catalog_row` reads OUR catalog and the number it
finds is the number `start_purchase` stores as `our_price_minor`, which is the number
`verify_quote` multiplies by the quantity and compares against Reap's subtotal, exactly, with no
tolerance. A caller-supplied price would make that whole check a comparison of the caller's claim
against itself.

**The merchant is never the caller's either.** `reap_agentic_eligibility` is an ALLOWLIST: a
domain with no enabled row for the buyer's market refuses `merchant_not_eligible`. There is no
"unknown means allowed" arm anywhere below.

**The buyer is never the caller's.** The purchase is opened for the buyer that
`buyer_identity_links` maps `(agent_id, hash(agent_user_ref))` to, and for nobody else. An agent
cannot name a buyer in the body; there is no field for it. Since WP4b that mapping is CREATED on
the first purchase when it is absent (see `_buyer_id_for`), and the important half of "never the
caller's" survives that intact: the identity is minted from randomness, never from the email or
anything else in the body, and the existing row always wins. An agent can cause a buyer to exist;
it still cannot choose WHICH buyer.

── CONSENT ──────────────────────────────────────────────────────────────────────────────────

`buyer.consent_version` is REQUIRED on the POST and its absence is `consent_required` (400),
checked after the dial and before eligibility, the catalog read, and every write. It is the
version tag of the terms the buyer accepted, stored on `reap_agentic_buyer_refs` — the row the
card enrollment hangs off — and rewritten on every purchase, so the pair
`(consent_version, consented_at)` is always the LATEST consent rather than the first.

The field exists because WP4b took a human out of the loop. Before it, a buyer arrived already
linked by a surface where they had signed in, and that sign-in WAS the consent; minting the
identity from an agent's assertion removes that step, so the door must carry the act forward.
The wording is the owner's and lives at the door. This route records which wording, and
deliberately does not adjudicate it — see `_consent_version`.

── THE THREE IDENTIFIERS, AND WHY REAP GETS THE THIRD ───────────────────────────────────────

    buyer_id                the global buyer. Joins to orders, addresses, users. NEVER leaves.
                            Minted here on an agent-only buyer's first purchase; it is then an
                            id in `shop_users`' format with NO `shop_users` row behind it, and
                            `_buyer_id_for` explains at length why creating that row from the
                            request's email would be an account-takeover primitive.
    agent_user_ref_hash     the ownership conjunct on the purchase row. NEVER leaves.
    reap_buyer_ref          opaque, per-buyer, minted here. This is `owner.id` at Reap.

`reap_agentic_buyer_refs` is the map, and it is a THIRD identifier rather than a reuse of
`buyer_agent_links.agent_scoped_buyer_ref` because an enrollment is a CARD: scoping the ref to an
agent would give one buyer two refs, two enrollments and two cards for what is one card, and
migration 224's "at most one active enrollment per buyer_ref" would hold twice instead of once.
Neither identifier appears in any response this module writes — see `_public_body`, which builds
from the ledger's allowlist rather than from the row.

── PII ──────────────────────────────────────────────────────────────────────────────────────

The buyer's email and address enter through the request body, are handed to `start_purchase`,
and are never logged, never echoed in an error, and never in a response. `PurchaseRefused.reason`
is our vocabulary and carries no request value; the partner's text never reaches a caller at all
because this module never speaks to the partner. The one exception carrying partner-adjacent
prose is `rc.ReapRequestError` out of `build_shipping_address`, whose message names MISSING KEYS
and no values — and it is mapped to a bare reason code below rather than forwarded.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Mapping, Optional, Tuple

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError, StrictInt, StrictStr, model_validator

import db.agent_purchase_ledger as agent_purchase_ledger
import db.reap_agentic_ledger as ledger
import db.reap_continuation as continuation
# THE ONE consent-tag shape rule, imported rather than re-implemented. Bound at module level
# so tests can assert BY IDENTITY that this module, services/reap_agentic_purchase and the
# ledger call the same function object — see that function's docstring for the production
# divergence three copies of the rule produced.
from db.reap_agentic_ledger import require_consent_version as consent_shape
import db.tierb_cart_link_eligibility as tierb_eligibility
import db.merchant_purchasability as purchasability
import services.reap_agentic_client as rc
import services.reap_agentic_purchase as svc
from db.buyer_vault import hash_agent_user_ref, mint_pairwise_buyer_ref
from db.commerce_attribution import surface_click_events
from db.database import IS_POSTGRES, database
from routes.agent_auth import AgentContext, get_agent_context
from routes.agent_user_auth import AgentUserContext, get_agent_user_context
from services.commerce_attribution_service import IssuedClick, issue_click, new_click_id
from services.text_normalization.display_text import clean_product_name
from services.outbound_links_service import (
    build_shopify_cart_permalink,
    extract_shopify_numeric_variant_id,
)
from services.shopify_variant_identity import (
    CART_PROOF_SCOPE_NAMED,
    clean_variant_title,
    verified_cart_variant_id,
    verified_selected_cart_variant_id,
)
# THE owner of the observed seller-of-record id (`merch_obs_<hash>`): the SAME dispatch every
# ingestion and re-key path mints with (retailer domain -> etld1 alone, else (brand, etld1)).
# Imported, never re-implemented -- see `_mirror_seller_ref`.
from services.seller_identity import resolve_seed_seller_identity
# THE ONE merchant-host canonicaliser, imported rather than re-implemented: lower case, ONE
# leading `www.` removed, and a ValueError for anything that is not a bare DNS host name —
# including a trailing dot. It is `tierb_cart_link_eligibility`'s own key function
# (`normalize_domain`) plus that one refusal, and for every bare host it agrees with
# `db.merchant_purchasability.normalize_domain`, the key of the purchasability facts. See
# `_merchant_domain_key` for why the route needs it.
from services.tierb_cart_link_merchants import canonical_merchant_domain
# Option 2 (PR C): the enrichment-row branch of the cart-link lane. The proof reader, the pure
# verifier / seller / price checks (PR A), and THE owner of the one-`www.` storefront-host rule.
import db.enrichment_cart_variant_proofs as enrichment_proofs
from services.curated_brand_feed import _same_storefront_host
from services.reap_enrichment_cart_proof import (
    ENRICHMENT_SOURCE_SYSTEM,
    PLACEHOLDER_SUFFIX as ENRICHMENT_PLACEHOLDER_SUFFIX,
    SOLE_VARIANT as ENRICHMENT_SOLE_VARIANT,
    VARIANT_INFIX as ENRICHMENT_VARIANT_INFIX,
    derive_enrichment_seller,
    enrichment_offer_price_ok,
    storefront_page,
    verify_enrichment_cart_proof,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent/v2/commerce/reap", tags=["agent-commerce-reap"])


# ── refusals ─────────────────────────────────────────────────────────────────────────────────

#: Every refusal this router can answer, and the status it answers with. `svc.PurchaseRefused` is
#: the ONE exception type on this path — the route raises it for its own refusals too, not just
#: the ones `start_purchase` raises — so each handler has one `except` and one place where a
#: reason becomes a status.
#:
#: THE DEFAULT IS 409, NOT 500 AND NOT 422. A reason this map has not heard of is still a refusal
#: we chose to make; answering 500 would page somebody for a decision the code made on purpose,
#: and answering 422 would tell the caller to edit a request that may be perfectly well-formed.
_REFUSAL_STATUS: Dict[str, int] = {
    "checkout_dispatch_unresolved": 409,
    "contact_reentry_not_required": 409,
    "resume_raced": 409,
    "resume_selection_changed": 409,
    "terminal_purchase_not_resumable": 409,
    # The rail is not here. Indistinguishable from the dial being off, deliberately: both mean
    # "unavailable", and a caller that could tell them apart would learn our configuration.
    "not_available_on_this_rail": 404,
    "rail_disabled": 404,
    "create_disabled": 404,
    "pilot_scope_invalid": 404,
    "pilot_scope_refused": 404,
    "rail_unconfigured": 404,
    # The caller is not authenticated as a buyer. 401 and not 403, because 403 would assert that
    # we know who this buyer is and are refusing them — we do not know, there is no token. It
    # also matches `get_agent_user_identity`, which already answers 401 for a token that is
    # present and bad; a token that is absent is the same failure one step earlier.
    "agent_user_required": 401,
    # The request is wrong and the caller can fix it by editing the request.
    #
    # 400 AND NOT 422, WHICH IS WHAT THE PLAN FOR THIS WORK ASKED FOR. This app CANNOT EMIT A
    # 422: `middleware/error_handler.ErrorHandlerMiddleware` normalises every 4xx JSON response
    # app-wide, and its `status_code == 422` branch rewrites the status to **400** and replaces
    # the body's details with a `validation_errors` list. A route that answered 422 would
    # therefore arrive at the caller as a 400 whose reason code had been thrown away — the worst
    # of both. Answering 400 outright keeps the status the route chose and keeps the reason where
    # the contract page says it is. See docs/reap_agentic_routes.md for the envelope the
    # middleware wraps these in.
    "invalid_request": 400,
    "invalid_address": 400,
    "invalid_return_url": 400,
    "currency_unsupported": 400,
    # An offer code Reap's field cannot carry (empty, whitespace only, over 128 characters, a
    # control character). Checked by `rc.validate_offer_code`, the one rule the service and the
    # ledger also call. A code that is well-formed but that REAP refuses is not this: the
    # purchase goes ahead without it and says so (`offer_code_outcome`).
    "invalid_offer_code": 400,
    # The buyer did not agree to anything. 400 AND NOT 401/403: the caller CAN fix this by
    # editing the request — it is a missing field, not a missing credential — and a 401 would
    # send a door that already holds a valid user token off to re-authenticate, which would
    # change nothing. It is listed apart from `invalid_request` because the door has to be able
    # to tell "your body is malformed" from "go and ask your user", which are different
    # instructions to different parts of a client.
    "consent_required": 400,
    # The request is well-formed; the world does not permit it. Editing the body will not help.
    "merchant_not_eligible": 409,
    # An operator TURNED THIS MERCHANT OFF (a merchant row exists in this market and is disabled).
    # A separate code from `merchant_not_eligible` ("nobody listed it") because a door may try the
    # merchant another way on the second -- the gateway retries Tier B on it -- and must never do
    # so on the first: "off" means off on every lane (see `_refuse_if_merchant_disabled`).
    "merchant_disabled": 409,
    # The merchant is ALLOWLISTED but holds no fresh, positive purchasability fact: nobody has
    # recently rendered its checkout from the buyer's vantage and seen a card method at our
    # price. A SEPARATE CODE from `merchant_not_eligible` on purpose — the two say different
    # things to an operator ("nobody listed this merchant" vs "this merchant is listed and we
    # cannot prove it can be paid"), and collapsing them would hide exactly the state this gate
    # exists to make visible. 409 like its neighbours: the body is fine, the world is not.
    "merchant_not_purchasable": 409,
    "buyer_unlinked": 409,
    "row_not_found": 409,
    "row_unpriced": 409,
    "price_changed": 409,
    # Tier B: two catalog spellings of the ONE chosen Shopify variant carry different usable
    # prices for this seller, and no sku was named -- no single price to commit to. 409 like
    # `row_unpriced`: the body is fine, the catalog is not.
    "row_price_ambiguous": 409,
    "row_not_shopify": 409,
    "row_variant_unverified": 409,
    "seller_identity_unverified": 409,
    "row_currency_mismatch": 409,
    # Cart-link lane, ENRICHMENT rows only (option 2, dark flag): the catalog offer's price is not
    # the price the storefront proof read live -- refused before any purchase opens.
    "row_price_stale": 409,
    # Cart-link lane, ENRICHMENT rows only: no `variant_key`, and the product has two or more real
    # skus. The lane never picks one; the caller names the sku or falls back.
    "row_variant_ambiguous": 409,
    "idempotency_conflict": 409,
    "attempt_retired": 409,
}

_DEFAULT_REFUSAL_STATUS = 409


def _refused(exc: svc.PurchaseRefused) -> JSONResponse:
    """A refusal on the wire: `{"error": "<reason>"}` and nothing else.

    NOT `HTTPException`, whose body would be `{"detail": {...}}`. The gateway team codes against
    docs/reap_agentic_routes.md and this is the shape that page promises; an envelope is a
    contract, not a formatting preference. `str(exc)` is NOT used — it carries the detail, which
    names fields and bounds and is for our logs, not for a caller.
    """
    reason = str(getattr(exc, "reason", "") or "invalid_request")
    if reason in {"create_disabled", "pilot_scope_invalid", "pilot_scope_refused"}:
        return JSONResponse(status_code=404, content={"error": "not_available_on_this_rail"})
    return JSONResponse(
        status_code=_REFUSAL_STATUS.get(reason, _DEFAULT_REFUSAL_STATUS),
        content={"error": reason},
    )


def _rail_is_live() -> bool:
    """Both halves, read at call time. `is_enabled()` is the operator's dial; `is_configured()`
    is whether we have anywhere to send a request. A rail with no credentials is as absent as one
    that is switched off, and `start_purchase` refuses both, so the door is told the same thing
    either way."""
    return svc.is_enabled() and rc.is_configured()


def _require_rail() -> None:
    if not _rail_is_live():
        raise svc.PurchaseRefused("not_available_on_this_rail")


#: Characters that must never reach a bind on this path. `Cc`/`Cf`/`Cs`/`Co`/`Cn` is the same
#: Unicode-category set `services.reap_agentic_purchase._clean_buyer_text` uses, and it is used
#: here for the opposite outcome — see `_identifier` below.
_UNPRINTABLE = frozenset({"Cc", "Cf", "Cs", "Co", "Cn"})


def _identifier(value: Any, name: str, *, max_chars: int) -> str:
    """One route-owned identifier, or `invalid_request`.

    ── WHY THIS REFUSES WHERE THE BUYER'S ADDRESS IS CLEANED ────────────────────────────────

    `_clean_buyer_text` STRIPS format characters out of the buyer's name and address and then
    refuses what is left, because a zero-width joiner pasted out of a web form is not an attack
    and refusing a whole purchase for it helps nobody. These four values are not prose. They are
    `merchant_domain`, `product_key`, `variant_key` and `idempotency_key`, and every one of them
    is matched EXACTLY against something in storage (the domain after one documented fold — see
    `_merchant_domain_key` — which is a rule both sides apply, not a repair of the input). Silently removing a character from an
    identifier does not sanitise it — it turns it into a different identifier, which then either
    matches nothing or, worse, matches something else. So these refuse.

    ── WHAT IT ACTUALLY STOPS, AND WHY ONLY THE POSTGRES ARM COULD SEE IT ───────────────────

    A single NUL byte in any of the four reached an asyncpg bind — in `_eligibility` and in
    `_replayed_purchase_id` — and asyncpg raises `CharacterNotInRepertoireError`, which is not a
    `PurchaseRefused`, so it escaped the handler's one `except` and the error middleware turned
    it into a **500**. SQLite stores NULs happily and answered a clean 409, so the SQLite arm
    could not see this at all: it is a defect that existed only on the dialect production runs.
    A 500 on a dark rail is also the one answer that tells a prober the route is really there.

    The length cap is here for the same reason the cleanliness check is: `merchant_domain` is
    `VARCHAR(255)` and `idempotency_key` is `VARCHAR(128)` in migration 226, and a value past
    those is a driver error rather than a refusal.
    """
    text = str(value or "").strip()
    if not text:
        raise svc.PurchaseRefused("invalid_request", f"{name} is required")
    if len(text) > max_chars:
        raise svc.PurchaseRefused("invalid_request", f"{name} is longer than {max_chars}")
    if any(unicodedata.category(ch) in _UNPRINTABLE for ch in text):
        # The VALUE is not named in the message or the log. It is caller-controlled, it is about
        # to be refused precisely because it contains something that does not render as itself,
        # and a log line is exactly where that matters.
        raise svc.PurchaseRefused(
            "invalid_request", f"{name} contains characters that are not printable"
        )
    return text


def _merchant_domain_key(host: str) -> str:
    """The MERCHANT this request names, as the key every allowlist and fact on this rail is
    matched under, or `invalid_request`.

    ── WHY `www.brand.com` AND `brand.com` HAVE TO BE ONE MERCHANT ──────────────────────────

    Three parties spell the same store independently and none of them agree. Shopify's
    `shop_domain` is what the sync writes into `catalog_products.source_domain`, and in
    production that is `www.Brand.com` — mixed case, with the `www.`. An operator writes the
    eligibility row by hand, and writes `brand.com`. The gateway sends the storefront host AS IT
    OBSERVED IT, which is either. Every one of those comparisons used to be EXACT, so whichever
    party disagreed with the other two got a refusal — `merchant_not_eligible` or
    `row_not_found` — that names the wrong cause and cannot be fixed from the door.

    So the rule is ONE canonical form, applied on BOTH sides of every comparison: this function
    on the request, the identical SQL expression on each stored column (see `_ELIGIBILITY_SQL`
    and `_PRODUCT_SQL`). It strips exactly ONE leading `www.`: `www.www.brand.com` is
    `www.brand.com`, and `wwwbrand.com` is a different store from `brand.com`, because a prefix
    that is not followed by a dot is part of the name.

    ── WHY THIS REFUSES RATHER THAN REDUCES ───────────────────────────────────────────────

    `https://brand.com/`, `brand.com:443`, `user@brand.com`, `brand.com.`, an IP literal or a
    single label is not a spelling of a merchant; it is a caller bug. Reducing it to a host would be a guess, and
    `_identifier` above already says what a guessed identifier turns into. Refused with the
    value NOT echoed — the canonicaliser's own `ValueError` names it, so it is not carried.

    ── THE FOLD RUNS EXACTLY ONCE, ON A SPELLING ─────────────────────────────────────────────

    It is not idempotent: `www.www.brand.com` -> `www.brand.com` -> `brand.com`. So the key this
    returns is compared only against a stored SPELLING folded by the same rule (the SQL), and it
    is never handed to a consumer that folds a SPELLING for itself: the Tier B verdict gets the
    observed host, and so does the purchase row, whose readers fold it — the resolver's
    `merchant_domain_matches`, and the attribution close (`svc._attribution_merchant_key`),
    which must key one merchant once whichever spelling bought. The one exception is
    `is_purchasable`, whose WRITER folds twice — see the handler. The CART-LINK
    lane's catalog read and URL get the observed host for a different reason: the permalink's host and the storefront
    evidence's host are compared byte-for-byte downstream (`sole_verified_cart_variant_id`,
    `cart_link_refusal`). See the handler.
    """
    try:
        return canonical_merchant_domain(host)
    except ValueError:
        raise svc.PurchaseRefused(
            "invalid_request", "merchant_domain is not a bare host name"
        ) from None


def _consent_version(value: Any) -> str:
    """The version of the terms this buyer accepted, or `consent_required`.

    ── WHY A PURCHASE IS WHERE THIS IS ASKED FOR ────────────────────────────────────────────

    Because this is the request that creates a DURABLE buyer identity and, behind it, a stored
    card at Reap. Before WP4b there was nowhere to put it: the buyer arrived already linked by a
    surface where a human had signed in, and that sign-in was the consent. Minting the identity
    from an agent's assertion removes the human from that step, so the door has to carry the act
    forward explicitly — and it has to carry it on the request that USES it, not on some earlier
    call whose result nothing here can see.

    ── THE RULE IS NOT HERE. IT IS `consent_shape`, AND SO IS THE SERVICE'S AND THE LEDGER'S ─

    `consent_shape` is `db.reap_agentic_ledger.require_consent_version`. This function is the
    mapping from its `ValueError` to this route's refusal code, and nothing else.

    IT USED TO BE A COPY OF THE RULE, one of three, and the three had drifted: this one and the
    ledger tested unicode CATEGORY membership while the service tested `str.isprintable()`, which
    is additionally False for every `Zs` except U+0020 and for U+2028/U+2029. A tag carrying a
    non-breaking space was therefore accepted HERE, written onto the buyer-ref row by
    `_record_consent`, and then refused by `start_purchase` with `consent_required` — the consent
    recorded, the purchase refused for not having one. Measured on Postgres: `buyer_refs = 1`,
    `purchases = 0`.

    ── WHAT IS DELIBERATELY NOT CHECKED, WHEREVER THE RULE LIVES ────────────────────────────

    THE VALUE IS NOT MATCHED AGAINST A LIST. There is no allowlist of known versions and there
    must not be one: the wording is the owner's, it changes without a deploy, and a backend that
    refused an unrecognised tag would reject the newest consent — the strongest one — the moment
    the door shipped it and before we had. We record what was accepted; we do not adjudicate it.
    What the record is FOR is answering "which wording was this buyer shown", and for that,
    storing an unfamiliar tag beats refusing it.

    ── WHAT A NON-STRING DOES ───────────────────────────────────────────────────────────────

    It never reaches here: `StartPurchaseRequest.buyer` types the field, so `123` / `true` / `{}`
    are `invalid_request` from pydantic before this runs. `consent_shape` refuses one too, which
    is what protects the direct callers of `start_purchase` that have no pydantic in front.
    """
    try:
        return str(consent_shape(value, required=True))
    except ValueError as exc:
        # The ledger's messages name the FIELD and the RULE and never the value — which is the
        # same courtesy `_identifier` extends, and the reason it is safe to carry one out here.
        raise svc.PurchaseRefused("consent_required", str(exc)) from None


def _require_agent_user(agent_user: Optional[AgentUserContext]) -> str:
    """The end-user ref, or a refusal. A purchase needs a BUYER — there is nobody to enroll a
    card for, nobody to own the row, and nobody to scope a later read to."""
    ref = str(getattr(agent_user, "agent_user_ref", "") or "").strip() if agent_user else ""
    if not ref:
        raise svc.PurchaseRefused("agent_user_required")
    return ref


# ── the request body ─────────────────────────────────────────────────────────────────────────


class ReapShippingAddress(BaseModel):
    """THE CLIENT'S FIELD NAMES, on purpose, and not the snake_case shape
    `routes/agent_commerce.CommerceShippingAddress` uses.

    `BuyerContact.shipping_address` is handed straight to `rc.build_shipping_address`, which
    whitelists `firstName / lastName / phone / addressLine1 / city / country` as REQUIRED and
    `addressLine2 / region / postalCode` as optional. Accepting a second spelling here would put
    a translation table between the wire and the validator — a second contract to keep in step
    with a partner's spec, and the first place a field would be silently dropped when the spec
    moves. One name per field, all the way through.

    `phone` is required because the client requires it. The brief for this work called it
    optional; the code it has to satisfy does not, and `build_shipping_address` refuses an
    incomplete address rather than sending a partial one.

    Extra keys are dropped by the model rather than refused, and then dropped AGAIN by
    `build_shipping_address` — a caller cannot widen what reaches a third party by adding keys.
    """

    firstName: Optional[str] = None
    lastName: Optional[str] = None
    phone: Optional[str] = None
    addressLine1: str = Field(..., min_length=1)
    city: str = Field(..., min_length=1)
    country: str = Field(..., min_length=2, max_length=2)
    addressLine2: Optional[str] = None
    region: Optional[str] = None
    postalCode: Optional[str] = None


class ReapBuyer(BaseModel):
    """`name` and `phone` are FALLBACKS for the address fields, never overrides.

    They exist because a caller that already holds a contact record should not have to restate
    the name in the address, and they are applied only where the address left the field empty —
    an address that names a recipient always wins, because that is the field that will be on the
    parcel. `name` splits on the LAST space: "Ada Lovelace" -> ("Ada", "Lovelace"). That is a
    guess, which is why it is a fallback and not the primary path.
    """

    email: str = Field(..., min_length=3)
    name: Optional[str] = None
    phone: Optional[str] = None
    shipping_address: ReapShippingAddress
    #: REQUIRED BY THE ROUTE, OPTIONAL IN THIS MODEL, AND THAT IS THE POINT. A pydantic
    #: `Field(...)` here would be validated before the handler's own checks and would arrive as
    #: `invalid_request` — the one code that tells a door to go and fix its JSON. A missing
    #: consent is not a malformed body; it is a missing act by a human, and it has its own code.
    #: `_consent_version` below is the real requirement, and it is a statement the mutant table
    #: can delete on its own.
    consent_version: Optional[str] = None


class StartPurchaseRequest(BaseModel):
    item_source: Literal["reap_variant", "cart_link"] = "reap_variant"
    merchant_domain: str = Field(..., min_length=1, max_length=255)
    product_key: str = Field(..., min_length=1)
    variant_key: Optional[str] = None
    quantity: StrictInt = Field(1, ge=1, le=svc.MAX_QUANTITY)
    buyer: ReapBuyer
    return_url: Optional[str] = None
    #: REQUIRED on create, every `item_source` -- by the handler (`_identifier`), not here, so a
    #: missing key and an unprintable one are the same `invalid_request`, before any SQL. One key
    #: is one attempt: it is what makes a retry of an unknown outcome (a lost 202, a 503
    #: `checkout_outcome_unknown`) the SAME purchase, and `/recover` cannot find an attempt that
    #: was opened without one.
    idempotency_key: Optional[str] = Field(default=None, max_length=128)
    #: Accepted and NOT forwarded anywhere. The click id this rail records is one WE mint (see
    #: `new_click_id` below): a caller-supplied attribution context cannot be trusted to be
    #: unique, and the purchase row has exactly one attribution column. Kept in the schema so the
    #: door can send what it already sends to the other commerce routes without a 422, and so
    #: that the day there is somewhere to put it, the field is already the one callers use.
    click_context: Optional[Dict[str, Any]] = None
    #: The buyer's own offer (coupon) code, optional, either lane. Validated by the handler
    #: through `rc.validate_offer_code` rather than by a pydantic constraint here, so a bad one
    #: answers `invalid_offer_code` instead of a generic `invalid_request`, and so the rule has
    #: one owner. Sent to Reap AS GIVEN; if Reap refuses it the purchase is re-quoted without it
    #: and `offer_code_outcome` on the purchase says `dropped_invalid` / `dropped_expired`.
    offer_code: Optional[str] = None
    # REQUIRED for a NEW attempt (the handler refuses a money-less one before any write): the
    # money the buyer was shown, bound into the original attempt and checked against the current
    # offer (`price_changed`). Neither is a client price override. Optional in the schema ONLY
    # so a money-less retry of an attempt keyed before the pair was required still reaches its
    # read-only replay (create) or lookup (recovery). Half a pair is refused here.
    expected_unit_price_minor: Optional[StrictInt] = Field(None, ge=1, le=9007199254740991)
    expected_currency: Optional[StrictStr] = Field(None, pattern=r"^[A-Z]{3}$")

    @model_validator(mode="after")
    def paired_expected_money(self):
        names = {"expected_unit_price_minor", "expected_currency"}
        present = names & self.model_fields_set
        if present and (present != names or self.expected_unit_price_minor is None or self.expected_currency is None):
            raise ValueError("expected money must be a nonnull pair")
        return self


class RecoverPurchaseRequest(StartPurchaseRequest):
    # Recovery must recognize bodies accepted before strict create admission; it cannot spend.
    quantity: int = Field(1, ge=1, le=svc.MAX_QUANTITY)


class PreparePurchaseSelectionRequest(BaseModel):
    """Resolve a selected Shopify ID without creating an attempt or buyer identity."""

    item_source: Literal["cart_link"]
    merchant_domain: StrictStr
    product_key: StrictStr
    variant_id: StrictStr
    quantity: StrictInt = Field(..., ge=1, le=svc.MAX_QUANTITY)
    market_country: StrictStr

    model_config = {"extra": "forbid"}


def _offer_code(value: Any) -> Optional[str]:
    """`rc.validate_offer_code` at the edge, as a `PurchaseRefused`. The SAME function object the
    service and the ledger call -- one rule, one function; pinned by identity in the tests."""
    try:
        return rc.validate_offer_code(value)
    except rc.ReapRequestError:
        raise svc.PurchaseRefused("invalid_offer_code")


def _buyer_address_for_client(buyer: ReapBuyer) -> Dict[str, Any]:
    """The address in the client's own vocabulary, with the two fallbacks applied."""
    address: Dict[str, Any] = {
        key: value
        for key, value in buyer.shipping_address.model_dump().items()
        if str(value or "").strip()
    }
    if not address.get("phone") and str(buyer.phone or "").strip():
        address["phone"] = str(buyer.phone).strip()
    if not address.get("firstName") and not address.get("lastName"):
        whole = str(buyer.name or "").strip()
        if whole:
            first, _, last = whole.rpartition(" ")
            # A single-word name goes in `firstName`, leaving `lastName` missing — which
            # `build_shipping_address` then refuses by name. Guessing a surname would be worse
            # than saying we do not have one.
            address["firstName"] = (first or whole).strip()
            if last and first:
                address["lastName"] = last.strip()
    return address


# ── eligibility ──────────────────────────────────────────────────────────────────────────────

#: The sentinel that distinguishes the MERCHANT row from an OVERRIDE row. `''`, not NULL: see the
#: header of db/migrations/226_reap_agentic_routes.sql for why a NULL here behaves differently on
#: the two dialects and a value does not.
#:
#: IMPORTED, NOT REDECLARED (WP6). `db.reap_agentic_ledger` owns this constant and the SET form
#: of the same predicate (`list_enabled_merchant_markets`), which the purchasability sweep uses
#: to build its population. Two spellings of "which rows are merchant rows" is how a merchant
#: this route accepts came to be one the sweep never visited.
_MERCHANT_ROW = ledger.ELIGIBILITY_MERCHANT_ROW

#: THE CURRENCY A MARKET IMPLIES. A MAP AND NOT A COLUMN, deliberately.
#:
#: The currency of a market is a fact about the world, not a per-merchant setting: two operators
#: filling in a `market_currency` column for the same country could disagree, and one of them
#: would be wrong in a way only a charge would reveal. A column also has to be RIGHT on every row
#: an operator writes by hand, and the SQL snippet in the runbook is the interface most of them
#: will use.
#:
#: IT FAILS CLOSED. A market that is not in this map refuses `row_currency_mismatch` rather than
#: skipping the check, which is the same direction every other decision on this rail takes —
#: adding a market is a one-line change here and the runbook says so. That is the right trade for
#: a list this short: the rail is domestic-only, and the set of markets Reap's agentic rail is
#: transactable in is smaller still.
#:
#: NOTE FOR WHOEVER ENABLES SG. The curated lane writes USD rows, so an SG merchant whose offers
#: are denominated in USD will refuse here. That refusal is CORRECT — an SGD storefront quoted in
#: USD is exactly the divergence this check exists for — but it will look like a bug, so the
#: runbook names it under "Before arming".
_MARKET_CURRENCY: Dict[str, str] = {
    "US": "USD",
    "CA": "CAD",
    "GB": "GBP",
    "AU": "AUD",
    "SG": "SGD",
    "HK": "HKD",
    "JP": "JPY",
    "NZ": "NZD",
}

#: THE STORED DOMAIN IS CANONICALISED IN THE SQL, not trusted to be canonical.
#:
#: `:merchant_domain` is `_merchant_domain_key`'s output. The column is folded by the SAME rule
#: — lower case, ONE leading `www.` removed — spelled as a `CASE` over `LIKE` and `substr`
#: because SQLite has no `regexp_replace`, and the text is IDENTICAL on both dialects so one
#: statement is what both gates test. `LIKE 'www.%'` has no `_` in it, so the only wildcard is
#: the trailing `%`; `lower()` runs first because Postgres' LIKE is case-sensitive. Rows are
#: operator-entered and the runbook says to write them canonical; this expression is what
#: keeps a row somebody typed as `www.Brand.com` from silently refusing every purchase. (The
#: column is read as a SPELLING and folded once — see `_merchant_domain_key` — so the one host
#: whose canonical form is not a fixed point, `www.www.<x>`, is written as observed.)
#:
#: THE PRIMARY KEY'S LEADING COLUMN IS NO LONGER USABLE FOR THIS READ, and that is accepted
#: knowingly: the table is an allowlist of tens of hand-written rows, and a scan of it costs
#: less than the second spelling rule it would take to keep the index.
_ELIGIBILITY_SQL = """
    SELECT merchant_domain, product_key, variant_key, market_country, enabled,
           accept_variant_labels, also_accept_domains
      FROM reap_agentic_eligibility
     WHERE CASE WHEN lower(merchant_domain) LIKE 'www.%'
                THEN substr(lower(merchant_domain), 5)
                ELSE lower(merchant_domain) END = :merchant_domain
       AND market_country = :market_country
       AND product_key IN (:merchant_row, :product_key)
"""


def _decode_alias_list(value: Any) -> Tuple[str, ...]:
    """jsonb on Postgres arrives as a `str`; TEXT on SQLite always does. One decode for both.

    Anything that is not a list of non-empty strings decodes to `()`. The ledger applies the real
    bounds (32 entries, 128 characters, hostname shape) and RAISES on a violation, which
    `start_purchase` turns into `invalid_request` — so a malformed operator row refuses the
    purchase rather than silently resolving without the alias it was created to supply.
    """
    if value is None:
        return ()
    if isinstance(value, (str, bytes)):
        try:
            value = json.loads(value)
        except Exception:  # noqa: BLE001
            return ()
    if not isinstance(value, list):
        return ()
    return tuple(str(item).strip() for item in value if str(item or "").strip())


class _Eligibility:
    __slots__ = ("market_country", "accept_variant_labels", "also_accept_domains")

    def __init__(
        self,
        market_country: str,
        accept_variant_labels: Tuple[str, ...],
        also_accept_domains: Tuple[str, ...],
    ) -> None:
        self.market_country = market_country
        self.accept_variant_labels = accept_variant_labels
        self.also_accept_domains = also_accept_domains


async def _refuse_if_merchant_disabled(*, merchant_domain: str, market_country: str) -> None:
    """`merchant_disabled` when an operator has a DISABLED merchant row for this merchant in this
    market -- checked on the CART-LINK lane too.

    THE CART-LINK LANE HAS ITS OWN ELIGIBILITY (the daily Tier B verdict), and before this it did
    not read `reap_agentic_eligibility` at all. So the runbook's "turn a merchant off" (an UPDATE
    to `enabled = FALSE`) stopped the variant lane and left the cart-link lane buying from the
    same merchant -- and the gateway's Tier B retry, which fires on the variant lane's refusal,
    made that the likely path rather than a corner. "Off" is a statement about the merchant, not
    about one lane. The same folded match and the same market conjunct as `_eligibility`.
    """
    rows = await database.fetch_all(
        _ELIGIBILITY_SQL,
        {
            "merchant_domain": merchant_domain,
            "market_country": market_country,
            "merchant_row": _MERCHANT_ROW,
            "product_key": _MERCHANT_ROW,
        },
    )
    if any(
        str(dict(r).get("product_key") or "") == _MERCHANT_ROW and not bool(dict(r).get("enabled"))
        for r in rows
    ):
        raise svc.PurchaseRefused(
            "merchant_disabled", "an eligibility row for this domain and market is disabled"
        )


async def _eligibility(
    *, merchant_domain: str, market_country: str, product_key: str, variant_key: Optional[str]
) -> _Eligibility:
    """The allowlist read. Refuses `merchant_not_eligible` unless an ENABLED merchant row exists
    for exactly this domain in exactly this market.

    THE MARKET IS A CONJUNCT IN THE SQL, not a comparison afterwards. A filter applied to a row
    already loaded is a filter a refactor can drop while every test that reads the returned value
    still passes — the same argument the ledger's header makes about the ownership conjunct, and
    the same stakes: this one is the only thing that keeps the rail domestic.

    An OVERRIDE row (a real `product_key`) contributes aliases and NOTHING ELSE. It cannot make
    an ineligible merchant eligible, and `enabled` is not even read from it: eligibility is a
    property of the merchant in a market, and a per-product exception to that would be a way to
    turn a merchant on without turning it on.
    """
    rows = await database.fetch_all(
        _ELIGIBILITY_SQL,
        {
            "merchant_domain": merchant_domain,
            "market_country": market_country,
            "merchant_row": _MERCHANT_ROW,
            "product_key": product_key,
        },
    )
    merchant_rows: List[Dict[str, Any]] = []
    labels: List[str] = []
    domains: List[str] = []
    for raw in rows:
        row = dict(raw)
        if str(row.get("product_key") or "") == _MERCHANT_ROW:
            merchant_rows.append(row)
            continue
        row_variant = str(row.get("variant_key") or "")
        # An override pinned to a variant applies to THAT variant only; one with a blank variant
        # applies to every variant of the product. A pinned row must not leak onto its siblings —
        # an accepted label names one object.
        if row_variant and row_variant != str(variant_key or ""):
            continue
        labels.extend(_decode_alias_list(row.get("accept_variant_labels")))
        domains.extend(_decode_alias_list(row.get("also_accept_domains")))

    # EVERY MERCHANT ROW MUST BE ENABLED, not whichever one the database returned last. Since the
    # domain is matched canonically, `www.brand.com` and `brand.com` written as two rows are two
    # merchant rows for ONE merchant — and "turning a merchant off" is an UPDATE an operator runs
    # against the spelling in front of them. A disabled twin therefore wins, which is the only
    # order-independent answer and the one a payment gate should give. (Before canonical matching
    # the same ambiguity existed for a merchant row typed with a `variant_key`, and the answer
    # was row order.) The runbook's one-off collapses such twins.
    if not merchant_rows:
        raise svc.PurchaseRefused(
            "merchant_not_eligible", "no eligibility row for this domain and market"
        )
    if not all(bool(r.get("enabled")) for r in merchant_rows):
        # NOT `merchant_not_eligible`: an operator turned this merchant off, and a door that
        # tries another lane on "not eligible" must not route around that. See `merchant_disabled`
        # in `_REFUSAL_STATUS`.
        raise svc.PurchaseRefused(
            "merchant_disabled", "an eligibility row for this domain and market is disabled"
        )
    merchant_row = merchant_rows[0]

    # Order-preserving dedupe: the ledger caps the list at 32 entries and raises past it, and two
    # override rows can legitimately name the same alias.
    return _Eligibility(
        market_country=str(merchant_row.get("market_country") or "").upper(),
        accept_variant_labels=tuple(dict.fromkeys(labels)),
        also_accept_domains=tuple(dict.fromkeys(domains)),
    )


# ── the buyer ────────────────────────────────────────────────────────────────────────────────


_BUYER_LINK_SQL = """
    SELECT buyer_id
      FROM buyer_identity_links
     WHERE agent_id = :agent_id
       AND agent_user_ref_hash = :agent_user_ref_hash
     LIMIT 1
"""


async def _linked_buyer_id(*, agent_id: str, agent_user_ref_hash: str) -> str:
    """The buyer this pair is already linked to, or `""`. A PURE READ — it mints nothing.

    Split out of `_buyer_id_for` so the replay path can record a consent against an existing
    buyer without acquiring the power to create one. See the call site: a replay must be able to
    update the consent tag, and must NOT be able to mint an identity before eligibility has been
    checked.
    """
    row = await database.fetch_one(
        _BUYER_LINK_SQL,
        {"agent_id": agent_id, "agent_user_ref_hash": agent_user_ref_hash},
    )
    return str(dict(row).get("buyer_id") or "").strip() if row else ""


async def _touch_link(*, agent_id: str, agent_user_ref_hash: str) -> None:
    """`last_seen_at` on a link we just used, the same as the prefill reader does.

    `routes/agent_checkout_intents._buyer_prefill_from_identity_link` stamps this on every read,
    and this route was the one surface that used a link without saying so — which made
    `last_seen_at` mean "last seen by the checkout-intent path" rather than "last seen", and would
    have made any dormancy sweep built on it retire buyers who purchase on this rail every day.

    BEST-EFFORT, like its twin. A bookkeeping column must not be able to refuse a purchase.
    """
    try:
        await database.execute(
            """
            UPDATE buyer_identity_links
               SET last_seen_at = CURRENT_TIMESTAMP,
                   updated_at = CURRENT_TIMESTAMP
             WHERE agent_id = :agent_id
               AND agent_user_ref_hash = :agent_user_ref_hash
            """,
            {"agent_id": agent_id, "agent_user_ref_hash": agent_user_ref_hash},
        )
    except Exception:  # noqa: BLE001
        pass


def _mint_buyer_id() -> str:
    """A new buyer id, in the id space `db/accounts.create_or_get_shop_user` mints into.

    `u_` + 16 hex characters: 18 characters into a `VARCHAR(50)` column, 64 bits of randomness,
    and indistinguishable at every reader from an id that came from a sign-up. The FORMAT is
    copied deliberately — a second spelling of "a buyer id" would be the kind of thing a later
    `startswith` somewhere reads as a type tag — but nothing else about that function is.
    """
    return f"u_{secrets.token_hex(8)}"


async def _buyer_id_for(*, agent_id: str, agent_user_ref_hash: str) -> str:
    """`(agent_id, hash(agent_user_ref))` -> `buyer_id`, minting one on the first purchase.

    ── WHAT CHANGED, AND WHOSE DECISION IT WAS ──────────────────────────────────────────────

    WP4 refused `buyer_unlinked` here, on the argument that the only writer of
    `buyer_identity_links` was `routes/buyer_api._upsert_buyer_identity_link` — a
    BUYER-AUTHENTICATED surface — so a link was something a human's sign-in created and nothing
    an agent asserted could. The consequence was that EVERY agent-only buyer was refused, which
    made the rail unarmable for the Minds door it was built for. The owner chose Option A on
    2026-09-18: mint the buyer identity here, on the first purchase, from the agent JWT's user
    reference. This function is that decision.

    ── WHAT IS MINTED, AND WHAT IS EMPHATICALLY NOT ─────────────────────────────────────────

    An OPAQUE buyer id, and NO `shop_users` row. Not "not yet" — never, on this path.

    The obvious reading of "create the buyer the base writer would" is to call
    `db.accounts.create_or_get_shop_user(buyer.email)`, and it is a HOLE. That function is
    `create_or_GET`: it looks the email up by `email_normalized` first and RETURNS THE EXISTING
    ACCOUNT when one matches. The email on this request is a string an agent put in a body. So an
    agent that asserts a stranger's address would be handed that stranger's real `buyer_id`, and
    the link written below would bind the agent's own user ref to a real human's account — after
    which `routes/agent_checkout_intents._buyer_prefill_from_identity_link` hands that agent the
    account's email and default shipping address on the next checkout intent. An account
    takeover, reachable from an agent token, with the victim's address as the prize.

    The identity minted here is AGENT-ASSERTED, not verified: the agent says "this is end user
    X", and nothing has checked that. Such an identity must not be able to collide with a
    verified one, so it is minted from randomness and never from anything the caller sent. The
    email in the body goes where it always went — to `start_purchase`, as the contact for THIS
    purchase — and is not an identity here.

    ── WHAT A ROW-LESS BUYER ID COSTS, MEASURED RATHER THAN ASSUMED ─────────────────────────

    `buyer_identity_links.buyer_id` is `VARCHAR(50)` with NO foreign key; there is no
    `REFERENCES shop_users` anywhere in the repo. The table has exactly two readers besides this
    module, and both tolerate a buyer id with no account behind it:

      * `routes/agent_checkout_intents._buyer_prefill_from_identity_link` runs
        `SELECT email FROM shop_users WHERE id = :id` inside a `try` and then reads
        `buyer_addresses` for a default, also inside one. With neither it returns `None` — which
        is precisely what it returned for this buyer YESTERDAY, when there was no link at all.
        The prefill does not regress; it stays absent.
      * `routes/buyer_api` writes the link, and writes it for a buyer that signed in.

    So the cost is a prefill nobody had. What is bought is a durable identity for the enrollment
    to hang off, which is the whole of WP4b.

    ── AND WHEN THE SAME HUMAN LATER SIGNS IN ───────────────────────────────────────────────

    `_upsert_buyer_identity_link` is a real upsert and REPOINTS the link to the signed-in
    account. That is correct and is left alone: a verified account supersedes a placeholder.
    What it costs is one re-enrollment — `reap_agentic_buyer_refs` is keyed on `buyer_id`, so
    the next purchase mints a fresh ref for the real account and Reap asks for the card once
    more. WP4 refused every such buyer forever to avoid exactly that; one re-enrollment after a
    sign-in is the cheaper half of the trade, and docs/reap_agentic_routes.md says so out loud
    so the door is not surprised by it.

    ── WHY THE INSERT CANNOT OVERWRITE ──────────────────────────────────────────────────────

    `ON CONFLICT DO NOTHING`, and then the answer is RE-READ rather than assumed. The row that
    exists wins, whoever wrote it: a concurrent first purchase, or a buyer-authenticated link
    written between our SELECT and our INSERT. `DO UPDATE` here would let this route — the one
    with the WEAKEST evidence of who the buyer is — overwrite a link a human's sign-in
    established. Returning `minted` without the re-read would be the same defect wearing a
    different hat: two concurrent first purchases would then proceed under two different buyer
    ids, one of which is in no table, and mint two enrollments for one card.
    """
    buyer_id = await _linked_buyer_id(
        agent_id=agent_id, agent_user_ref_hash=agent_user_ref_hash
    )
    if buyer_id:
        await _touch_link(agent_id=agent_id, agent_user_ref_hash=agent_user_ref_hash)
        return buyer_id

    minted = _mint_buyer_id()
    try:
        await database.execute(
            """
            INSERT INTO buyer_identity_links (
                agent_id, agent_user_ref_hash, buyer_id, created_at, updated_at, last_seen_at
            )
            VALUES (
                :agent_id, :agent_user_ref_hash, :buyer_id,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            ON CONFLICT (agent_id, agent_user_ref_hash) DO NOTHING
            """,
            {
                "agent_id": agent_id,
                "agent_user_ref_hash": agent_user_ref_hash,
                "buyer_id": minted,
            },
        )
    except Exception:  # noqa: BLE001
        # A dialect without `ON CONFLICT` raises the unique violation instead of absorbing it,
        # and the answer to both is the same one: read what is there now.
        pass

    # THE RE-READ IS NOT A VERIFICATION STEP, IT IS THE ANSWER. `minted` is never returned.
    row = await database.fetch_one(
        _BUYER_LINK_SQL,
        {"agent_id": agent_id, "agent_user_ref_hash": agent_user_ref_hash},
    )
    buyer_id = str(dict(row).get("buyer_id") or "").strip() if row else ""
    if not buyer_id:
        # The INSERT failed for a reason that was not a lost race. Fail CLOSED: continuing would
        # open a purchase — and an enrollment — for a buyer no table has heard of.
        raise svc.PurchaseRefused("buyer_unlinked", "could not establish a buyer identity")
    return buyer_id


async def _record_consent(buyer_id: str, consent_version: str) -> None:
    """The LATEST consent this buyer gave, on the row the enrollment hangs off.

    NOT WRAPPED IN A `try`, unlike almost everything else that touches this table. The writes
    around it are wrapped because their failure mode is a lost RACE, which has a correct
    recovery. This one's failure mode is a schema that is missing migration 227's columns, which
    has no recovery and must not be absorbed into a purchase that then proceeds as though the
    consent had been recorded. It surfaces as a 500 on a rail that is already live, which is what
    every other genuine driver failure on this path does.
    """
    await database.execute(
        """
        UPDATE reap_agentic_buyer_refs
           SET consent_version = :consent_version,
               consented_at = :consented_at
         WHERE buyer_id = :buyer_id
        """,
        {
            "consent_version": consent_version,
            "consented_at": _now(),
            "buyer_id": buyer_id,
        },
    )


async def _reap_buyer_ref(buyer_id: str, *, consent_version: str) -> str:
    """The opaque `owner.id` for this buyer. Insert-or-read, and race-safe by re-reading.

    Read first, because that is what almost every call does. On a miss, mint and insert; a
    concurrent caller that got there first raises a unique violation on either the primary key or
    `uq_reap_agentic_buyer_refs_ref`, and the answer to that is to READ WHAT THEY WROTE, never to
    retry the mint. Two refs for one buyer would be two enrollments for one card.

    THE CONSENT IS WRITTEN ON EVERY PATH THROUGH HERE. On the mint it rides in the same INSERT,
    so a row cannot exist without one. On every reuse it is rewritten, because the tag is the
    LATEST version this buyer accepted and a buyer who accepts v2 has not un-accepted it by
    having a row that says v1.
    """
    existing = await database.fetch_one(
        "SELECT reap_buyer_ref FROM reap_agentic_buyer_refs WHERE buyer_id = :buyer_id",
        {"buyer_id": buyer_id},
    )
    if existing:
        ref = str(dict(existing).get("reap_buyer_ref") or "").strip()
        if ref:
            await _record_consent(buyer_id, consent_version)
            return ref

    minted = mint_pairwise_buyer_ref()
    try:
        await database.execute(
            """
            INSERT INTO reap_agentic_buyer_refs (
                buyer_id, reap_buyer_ref, consent_version, consented_at
            )
            VALUES (:buyer_id, :reap_buyer_ref, :consent_version, :consented_at)
            """,
            {
                "buyer_id": buyer_id,
                "reap_buyer_ref": minted,
                "consent_version": consent_version,
                "consented_at": _now(),
            },
        )
        return minted
    except Exception:  # noqa: BLE001
        pass

    row = await database.fetch_one(
        "SELECT reap_buyer_ref FROM reap_agentic_buyer_refs WHERE buyer_id = :buyer_id",
        {"buyer_id": buyer_id},
    )
    ref = str(dict(row).get("reap_buyer_ref") or "").strip() if row else ""
    if not ref:
        # The INSERT failed for a reason that was not a lost race. Refusing is the only safe
        # answer: continuing would mean opening a purchase under a ref nothing has stored, and
        # the enrollment it creates would be unreachable from the next purchase.
        raise svc.PurchaseRefused("buyer_unlinked", "could not establish a buyer reference")
    await _record_consent(buyer_id, consent_version)
    return ref


# ── our catalog row ──────────────────────────────────────────────────────────────────────────

#: WHY THIS QUERY EXISTS RATHER THAN A CALL TO `services.pivot_query_service`.
#:
#: `_fetch_canonical_rows_for_product` / `_for_sku` read the same three tables and would give
#: every field below. They are RECALL readers: they INNER JOIN `catalog_offers`, so a product
#: that exists and has no unsuppressed offer returns nothing at all and is indistinguishable
#: from a product that does not exist — and this route has to tell `row_unpriced` from
#: `row_not_found`, because they send the door to different places. They also carry recall's
#: visibility gates (merchant `indexable`, seller status), which are about whether a row may be
#: BROWSED. Whether it may be BOUGHT is what `reap_agentic_eligibility` answers, by a human,
#: per merchant, and a second unrelated set of gates on top of that allowlist would be a second
#: place to debug a purchase that "should work".
#:
#: What IS copied verbatim is the price precedence —
#: `coalesce(merchant_effective_price, estimated_best_price, list_price)` — because a purchase
#: that priced a row differently from every other surface in the repo would be the worst kind of
#: divergence: silent, and only visible on the charge.
#:
#: `o.market` IS NOT A CONJUNCT. The offer table has a market column and filtering on it looks
#: obviously right; the SG census (market:SG -> 0 rows) is the record of what it actually does
#: to a lane that names a market. Domesticity is enforced where it is decidable — the
#: eligibility row's `market_country` against the buyer's shipping country, and the CURRENCY
#: check below — and not by a column whose population nobody has measured on this cohort.
#:
#: `o.merchant_id` IS A CONJUNCT, AND IT IS THE ONE THIS QUERY WAS MISSING.
#: `catalog_offers.merchant_id` is the OFFER SELLER, and it is NOT the same column as
#: `catalog_products.merchant_id`, which is whose catalogue the product row belongs to. The repo
#: says so structurally: `services/pivot_query_service._fetch_canonical_rows_for_sku` LEFT JOINs
#: `catalog_merchants` TWICE — once on `p.merchant_id` and once on `o.merchant_id` — precisely
#: because one sku can carry offers from several sellers.
#:
#: Without the conjunct this query was `ORDER BY price ASC LIMIT 1` across EVERY seller's offer
#: on the sku, so the cheapest one won. Two ways that goes wrong, and neither is hypothetical:
#:
#:   * a competing seller's offer at 1.00 on the same sku becomes `our_price_minor = 100`. The
#:     buyer is then sent through card enrolment, the quote comes back at the eligible merchant's
#:     real price, and `verify_quote` refuses `price_changed` — for a price change that never
#:     happened. The buyer has entered a card and bought nothing.
#:   * the same product mirrored into the crawl lane (`external_seed`) and synced by the merchant
#:     (`internal_merchant`) carries TWO offer rows with two prices, and the cheaper copy is
#:     often the stale one.
#:
#: The conjunct is `o.merchant_id = p.merchant_id`: the offer belonging to the merchant whose
#: catalogue this product is in, which is the merchant the eligibility allowlist was read under
#: (the domain is a conjunct on the product read). An eligible merchant with no offer of its OWN
#: on this sku is `row_unpriced` — not "somebody else will sell it to you".
#:
#: SUPPRESSION IS A CONJUNCT ON ALL THREE TABLES, both columns each. A suppressed row is one
#: somebody took out of circulation; selling it is the one thing that must not still work.
#:
#: `source_domain` IS CANONICALISED BY THE SAME EXPRESSION `_ELIGIBILITY_SQL` USES, character
#: for character. The Shopify sync writes Shopify's `shop_domain`, which in production is
#: `www.Brand.com`; a bare `lower()` compared that to the canonical `brand.com` the eligibility
#: row was read under and answered `row_not_found` for every product the merchant has. The
#: domain stays a CONJUNCT (see `_load_catalog_row`); only its spelling rule changed.
_PRODUCT_SQL = """
    SELECT p.product_key, p.merchant_id, p.title AS product_title, p.brand, p.category,
           p.product_type, p.platform, p.source_domain
      FROM catalog_products p
     WHERE p.product_key = :product_key
       AND CASE WHEN lower(p.source_domain) LIKE 'www.%'
                THEN substr(lower(p.source_domain), 5)
                ELSE lower(p.source_domain) END = :merchant_domain
       AND p.suppression_reason IS NULL
       AND p.suppressed_at IS NULL
"""

_SKU_BY_KEY_SQL = """
    SELECT s.sku_key, s.title AS variant_title, s.currency AS sku_currency
      FROM catalog_skus s
     WHERE s.sku_key = :variant_key
       AND s.product_key = :product_key
       AND s.suppression_reason IS NULL
       AND s.suppressed_at IS NULL
"""

_SKUS_FOR_PRODUCT_SQL = """
    SELECT s.sku_key, s.title AS variant_title, s.currency AS sku_currency
      FROM catalog_skus s
     WHERE s.product_key = :product_key
       AND s.suppression_reason IS NULL
       AND s.suppressed_at IS NULL
     ORDER BY s.sku_key
     LIMIT 2
"""

#: `CAST(... AS TEXT)` ON THE PRICE, AND IT IS NOT COSMETIC. This is raw SQL, so no SQLAlchemy
#: result processor runs: asyncpg hands a `numeric` back as a `Decimal`, and SQLite hands the same
#: column back as a **binary float**. `major_to_minor` refuses a float OUTRIGHT — deliberately,
#: because 42.50 as a float is 42.4999999999999964 and a charge that inherits that error is wrong
#: in a direction nobody chose. So without the cast this route prices correctly on Postgres and
#: refuses `row_unpriced` for every priced row on SQLite: a dialect split that no amount of
#: SQLite testing would reveal as anything but "the fixture is wrong". Text round-trips exactly on
#: both, and `Decimal(str(...))` is the parse the converter already does.
#: The ORDER BY deliberately stays on the numeric expression — ordering on the text would sort
#: '9.00' after '10.00'.
_OFFER_SQL = """
    SELECT o.currency,
           CAST(coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price)
                AS TEXT) AS price
      FROM catalog_offers o
     WHERE o.sku_key = :sku_key
       AND o.product_key = :product_key
       AND o.merchant_id = :merchant_id
       AND o.suppression_reason IS NULL
       AND o.suppressed_at IS NULL
       AND coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price) IS NOT NULL
     ORDER BY coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price) ASC
     LIMIT 1
"""


async def _load_catalog_row(
    *,
    merchant_domain: str,
    storefront_host: str,
    product_key: str,
    variant_key: Optional[str],
    market_country: str,
    accept_variant_labels: Tuple[str, ...],
    also_accept_domains: Tuple[str, ...],
) -> svc.PurchaseRow:
    """OUR row for the thing being bought. The price comes from here and from nowhere else.

    Refuses, in this order and for these reasons:
      `row_not_found`   no such product under THIS domain, or the variant is not this product's,
                        or no variant was named and the product does not have exactly one.
      `row_not_shopify` the row's intake lane is not `shopify`.
      `row_unpriced`    no usable offer, or an amount/currency this rail cannot convert exactly.

    THE DOMAIN IS A CONJUNCT, not a check afterwards. `merchant_domain` is the CANONICAL key the
    eligibility allowlist was read under; if it were not also the key the product is read under,
    a caller could name an enabled merchant and a product belonging to a different one. A product
    that exists under another domain answers `row_not_found` — the same answer as one that does
    not exist, so this route cannot be used to enumerate another merchant's catalogue.
    """
    product = await database.fetch_one(
        _PRODUCT_SQL, {"product_key": product_key, "merchant_domain": merchant_domain}
    )
    if not product:
        raise svc.PurchaseRefused("row_not_found", "no catalog product for this domain and key")
    product = dict(product)

    # THE INTAKE LANE, NOT THE STOREFRONT, and this route wants the intake lane. `external_seed`
    # rows are overwhelmingly Shopify storefronts, and the gateway normalises them for DISPLAY on
    # evidence held in a seed snapshot — evidence that is not in `catalog_*` and that this module
    # would have to re-derive to reuse. A purchase is not a display: if the row we are about to
    # charge a card against did not come through the merchant sync, we do not claim to know what
    # its storefront is, and we refuse.
    if str(product.get("platform") or "").strip().lower() != "shopify":
        raise svc.PurchaseRefused("row_not_shopify", "the catalog row's intake lane is not shopify")

    if variant_key:
        # EXACT MATCH ON sku_key, NEVER A DERIVATION. There are three live spellings of a variant
        # sku key in this repo (`::v::`, `::v:` and the sync lane's own), they collide on the
        # identity index, and a route that rebuilt a key from a product key and a variant id
        # would pick the wrong lane's row for thousands of products.
        sku_row = await database.fetch_one(
            _SKU_BY_KEY_SQL, {"variant_key": variant_key, "product_key": product_key}
        )
        if not sku_row:
            raise svc.PurchaseRefused("row_not_found", "no catalog sku for this product and key")
        sku = dict(sku_row)
    else:
        # No variant named: the product must have exactly ONE, or we do not know what is being
        # bought. `LIMIT 2` is how "exactly one" is asked without counting the whole table.
        candidates = [dict(r) for r in await database.fetch_all(
            _SKUS_FOR_PRODUCT_SQL, {"product_key": product_key}
        )]
        if len(candidates) != 1:
            raise svc.PurchaseRefused(
                "row_not_found", "no variant named and the product does not have exactly one"
            )
        sku = candidates[0]

    offer = await database.fetch_one(
        _OFFER_SQL,
        {
            "sku_key": sku["sku_key"],
            "product_key": product_key,
            # THE SELLER. `p.merchant_id`, not a caller value and not the cheapest seller on the
            # sku — see the note above `_PRODUCT_SQL`.
            "merchant_id": str(product.get("merchant_id") or ""),
        },
    )
    if not offer:
        raise svc.PurchaseRefused(
            "row_unpriced", "the eligible merchant has no usable offer of its own on this sku"
        )
    offer = dict(offer)

    currency = str(offer.get("currency") or sku.get("sku_currency") or "").strip().upper()
    if not currency:
        raise svc.PurchaseRefused("row_unpriced", "the offer names no currency")

    # THE MARKET'S CURRENCY, AND IT IS THE LAST THING BETWEEN A DOMESTIC-ONLY RAIL AND A
    # CROSS-CURRENCY CHARGE. Everything upstream has established that the buyer ships to a market
    # this merchant is enabled for; nothing yet has established that the PRICE we are about to
    # commit to is denominated in that market's money. A US-priced offer quoted for an SG buyer
    # on an SGD storefront clears every other check here and then diverges at the quote, where
    # the only vocabulary for it is `price_changed` — a refusal that names the wrong cause and
    # arrives after the buyer has entered a card.
    expected = _MARKET_CURRENCY.get(market_country)
    if expected is None or currency != expected:
        raise svc.PurchaseRefused(
            "row_currency_mismatch",
            f"the offer is priced in {currency} and {market_country} is not",
        )

    # THE RAIL'S OWN CONVERTER, which refuses rather than rounds. A price we cannot state exactly
    # in minor units is one we do not buy against: `verify_quote` compares
    # `quantity * our_price_minor` to Reap's subtotal with NO tolerance, so a rounded value here
    # is a refused purchase three states later, reported as a price change that never happened.
    price_minor = ledger.amount_minor_or_none(offer.get("price"), currency)
    if not price_minor or price_minor <= 0:
        raise svc.PurchaseRefused("row_unpriced", "the offer price is not an exact minor amount")

    product_name = str(product.get("product_title") or "").strip()
    if not product_name:
        raise svc.PurchaseRefused("row_not_found", "the catalog row has no title")

    variant_title = str(sku.get("variant_title") or "").strip() or None
    return svc.PurchaseRow(
        # THE HOST AS OBSERVED, not the canonical key. Its readers fold it themselves: the
        # resolver's `rc.merchant_domain_matches`, and the attribution close
        # (`svc._attribution_merchant_key`). See `_merchant_domain_key`.
        merchant_domain=storefront_host,
        product_key=product_key,
        variant_key=str(sku.get("sku_key") or "") or None,
        product_name=product_name,
        variant_title=variant_title,
        brand=str(product.get("brand") or "").strip() or None,
        category=str(product.get("category") or product.get("product_type") or "").strip() or None,
        our_price_minor=int(price_minor),
        currency=currency,
        market_country=market_country,
        accept_variant_labels=accept_variant_labels,
        also_accept_domains=also_accept_domains,
        # THE TWO ARE EXCLUSIVE AND `start_purchase` REFUSES THE INCONSISTENT PAIR. A sku with no
        # title (the synthetic single-variant row) is exactly the "single value axis accepted
        # without a title" case the client documents, and the ABSENCE of `variant_title` is how
        # that is carried — there is no column for the flag.
        declared_single_variant=variant_title is None,
    )


# Tier B uses the same catalog price discipline as the variant lane, but its item identity is a
# merchant-issued Shopify numeric variant id, not Reap's opaque variant. A mirrored external-seed
# SKU is synthetic (`source_variant_id == product_key`). Its operator-entered
# `attached_variant_id` is NOT proof of Shopify identity: it can be a numeric SKU. The mirror
# therefore needs a product-bound seed with a sole variant stamped from storefront `.js` evidence.
_CART_PRODUCT_SQL = """
    SELECT p.product_key, p.merchant_id, p.seller_ref, p.seed_kind, p.platform,
           p.source_ref, p.source_system, p.title AS product_title, p.brand
      FROM catalog_products p
     WHERE p.product_key = :product_key
       AND lower(p.source_domain) = :merchant_domain
       AND p.suppression_reason IS NULL
       AND p.suppressed_at IS NULL
"""
# A CALLER-NAMED sku is read by its key, exactly (review of #2453): searching the bounded list
# below for it refused a named variant that sorted past the cap on a product with many skus.
_CART_SKU_BY_KEY_SQL = """
    SELECT s.sku_key, s.source_variant_id, s.title AS variant_title, s.currency
      FROM catalog_skus s
     WHERE s.product_key = :product_key AND s.sku_key = :variant_key
       AND s.suppression_reason IS NULL AND s.suppressed_at IS NULL
"""
# EVERY live sku of the product (bounded), not `LIMIT 2`: the choice is made by `_cart_sku_choice`
# over what the skus RESOLVE to, and a mirror row routinely carries three rows for one variant.
_CART_PRODUCT_SKUS_SQL = """
    SELECT s.sku_key, s.source_variant_id, s.title AS variant_title, s.currency
      FROM catalog_skus s
     WHERE s.product_key = :product_key
       AND s.suppression_reason IS NULL AND s.suppressed_at IS NULL
     ORDER BY s.sku_key LIMIT 51
"""
#: More live skus than this is not a product a cart link can name one variant of.
_CART_MAX_SKUS = 50
_CART_SEED_VARIANT_SQL = """
    SELECT e.attached_variant_id, e.seed_data, e.destination_url, e.canonical_url
      FROM external_product_seeds e
     WHERE e.id = :seed_id AND e.status = 'active'
       AND e.attached_product_key = :product_key
       AND lower(e.domain) = :merchant_domain AND upper(e.market) = :market_country
"""
# THE MARKET'S CURRENCY, in SQL (review of #2457): an offer in another currency is not an offer for
# this buyer -- not a price to compare, not a cheaper pick. It gates on `currency`, NEVER on
# `market`, for the reason `services.region_pricing` gives: `market` is NOT NULL DEFAULT 'US' and
# the mirror writer (`external_offer_dual_write.MIRROR_OFFER_UPSERT_SQL`) never sets it, so every
# mirror offer -- an SG seed's SGD offer included -- reads 'US'. Filtering on it would refuse every
# SG mirror row and still count an SGD offer as a US one. A NULL / blank currency matches nothing.
_CART_OFFER_MARKET_CURRENCY = "upper(trim(coalesce(o.currency, ''))) = :market_currency"
# EVERY usable offer this seller has on ONE sku (bounded) -- read for a mirror's `::canonical`
# placeholder, whose offers must all AGREE before one is priced (see `_load_cart_link_item`).
_CART_ALL_OFFERS_SQL = """
    SELECT o.currency,
           CAST(coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price)
                AS TEXT) AS price
      FROM catalog_offers o
     WHERE o.product_key = :product_key AND o.sku_key = :sku_key
       AND o.merchant_id = :merchant_id
       AND o.suppression_reason IS NULL AND o.suppressed_at IS NULL
       AND coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price) IS NOT NULL
       AND lower(coalesce(o.availability, 'unknown')) NOT IN
           ('out_of_stock', 'sold_out', 'unavailable')
       AND """ + _CART_OFFER_MARKET_CURRENCY + """
     ORDER BY coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price) ASC
     LIMIT 20
"""
_CART_OFFER_SQL = """
    SELECT o.currency,
           CAST(coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price)
                AS TEXT) AS price
      FROM catalog_offers o
     WHERE o.product_key = :product_key AND o.sku_key = :sku_key
       AND o.merchant_id = :merchant_id
       AND o.suppression_reason IS NULL AND o.suppressed_at IS NULL
       AND coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price) IS NOT NULL
       AND lower(coalesce(o.availability, 'unknown')) NOT IN
           ('out_of_stock', 'sold_out', 'unavailable')
       AND """ + _CART_OFFER_MARKET_CURRENCY + """
     ORDER BY coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price) ASC
     LIMIT 1
"""


#: The display placeholder every mirror / canonical lane writes: `<product_key>::canonical`, whose
#: `source_variant_id` IS the product_key (a storage token, not a variant). It names no variant.
_CANONICAL_SKU_SUFFIX = "::canonical"
# `ext_<external id>:<shopify numeric variant>` -- the crawl lane's spelling of a mirror row's own
# variant (live: `ext_0f95730ee5ba05a6b7957ada:49819267301653`).
_EXT_VARIANT_RE = re.compile(r"^(ext_[A-Za-z0-9]+):([0-9]{1,20})\Z")


def _is_placeholder_sku(sku: Mapping[str, Any], product_key: str) -> bool:
    return (str(sku.get("sku_key") or "").endswith(_CANONICAL_SKU_SUFFIX)
            and str(sku.get("source_variant_id") or "").strip() == product_key)


def _cart_numeric_variant(source_variant_id: Any, product_key: str) -> Optional[str]:
    """The Shopify numeric variant id a sku's `source_variant_id` names, or None.

    `extract_shopify_numeric_variant_id` for the bare and `gid://` forms (it does NOT read the
    `ext_…:<n>` form), plus that form -- and only when its `ext_…` is THIS product's own external
    id (the last segment of its product_key), so another product's variant cannot be borrowed.
    """
    numeric = extract_shopify_numeric_variant_id(source_variant_id)
    if numeric:
        return numeric
    match = _EXT_VARIANT_RE.match(str(source_variant_id or "").strip())
    if match and match.group(1) == product_key.rsplit("::", 1)[-1]:
        return match.group(2)
    return None


def _cart_sku_choice(
    skus: List[Mapping[str, Any]], product_key: str
) -> Tuple[Optional[str], List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """`(numeric_variant, skus_for_it_sorted, placeholder)` for a cart link, or `row_not_found`.

    THE ONE SKU RULE for the cart-link lane (the no-key path AND the mirror check use it; review
    of the staging demo, 2026-09-29). `LIMIT 2` + "exactly one row" refused every mirror row that
    carries the `::canonical` placeholder beside its real variant sku -- 6,395 of 7,858 in prod --
    and every row where two sku lanes spell ONE Shopify variant twice (`::sku_…` -> `ext_…:<n>`,
    `::v:<n>` -> `<n>`). So:
      * the placeholder (`_is_placeholder_sku`) is set aside -- it names no variant;
      * every other sku must reduce to a Shopify numeric variant (`_cart_numeric_variant`), and
        there must be EXACTLY ONE DISTINCT numeric variant among them; all skus naming it are
        returned, lowest sku_key first, and the caller prices the first that has an offer;
      * no real sku at all -> `(None, [], placeholder)`: the placeholder is then the only row,
        and the variant can come only from the seed's storefront proof (the mirror branch).
    ACCEPT {canonical, sku_x->4981...}; {canonical, sku_x->4981..., v:4981...->4981...}.
    REFUSE {sku_a->1, sku_b->2} (two variants); a real sku that names no numeric variant.
    """
    if len(skus) > _CART_MAX_SKUS:
        raise svc.PurchaseRefused("row_not_found", "variant is ambiguous")
    placeholder = None
    real: List[Dict[str, Any]] = []
    for raw in skus:
        sku = dict(raw)
        if _is_placeholder_sku(sku, product_key):
            placeholder = sku
        else:
            real.append(sku)
    if len(real) == 1 and _cart_numeric_variant(real[0].get("source_variant_id"), product_key) is None:
        # ONE real sku that names no Shopify variant: returned as the sole candidate with no
        # variant, so the caller refuses it `row_variant_unverified` exactly as before this rule.
        return None, real, placeholder
    by_variant: Dict[str, List[Dict[str, Any]]] = {}
    for sku in real:
        numeric = _cart_numeric_variant(sku.get("source_variant_id"), product_key)
        if numeric is None:
            raise svc.PurchaseRefused("row_not_found", "variant is ambiguous")
        by_variant.setdefault(numeric, []).append(sku)
    if len(by_variant) > 1:
        raise svc.PurchaseRefused("row_not_found", "variant is ambiguous")
    if not by_variant:
        return None, [], placeholder
    ((numeric, group),) = by_variant.items()
    return numeric, sorted(group, key=lambda row: str(row.get("sku_key") or "")), placeholder


#: `scripts/mirror_external_seeds_to_catalog_products.py`'s `mirrored_brand` -- the brand the mirror
#: hands `ensure_observed_seller` to mint the row's `merch_obs_` id -- is
#: `nullif(btrim(coalesce(<these, in order>, '')), '')`. That SQL is the only owner of the order (no
#: Python helper carries it), so it is restated here ONCE and a test parses the script's SQL and
#: pins the two equal.
MIRRORED_BRAND_PATHS: Tuple[Tuple[str, ...], ...] = (
    ("snapshot", "brand"), ("brand",), ("snapshot", "vendor"), ("vendor",),
)


def mirrored_seed_brand(seed_data: Any) -> Optional[str]:
    """The mirror's `mirrored_brand` for a seed: SQL `coalesce` semantics -- the FIRST path that is
    present (not JSON null / absent) wins, even if blank, and is then trimmed; blank -> None."""
    data = seed_data if isinstance(seed_data, dict) else {}
    for path in MIRRORED_BRAND_PATHS:
        node: Any = data
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
        if node is not None:
            return str(node).strip() or None
    return None


def _mirror_seller_ref(product: Mapping[str, Any], seed: Mapping[str, Any], seed_data: Any,
                       merchant_domain: str) -> Optional[str]:
    """The seller of a MIRROR row, or None.

    `seller_ref` is NULL on every mirror row (the mirror writes `merchant_id` only), so the old
    "seller_ref must equal merchant_id" refused all of them. A NULL seller_ref is accepted when the
    row's `merchant_id` is EXACTLY the observed seller id the repo's own minting function derives
    for this storefront: `resolve_seed_seller_identity(brand, domain)` over the attached ACTIVE
    seed's domain (which `_CART_SEED_VARIANT_SQL` already pinned to `merchant_domain` and this
    product; no seed row -> None). The brand is the one the mirror minted from
    (`mirrored_seed_brand`: snapshot.brand, brand, snapshot.vendor, vendor), else the product's.
    A non-null seller_ref keeps today's rule: it must equal merchant_id. Live: brand Judydoll + judydoll.com -> merch_obs_a25cbba37ef98c52.
    """
    merchant_id = str(product.get("merchant_id") or "").strip()
    seller_ref = str(product.get("seller_ref") or "").strip()
    if seller_ref:
        return seller_ref if seller_ref == merchant_id else None
    if not merchant_id or not seed:
        return None
    # The brand the MIRROR minted this row's seller from (`mirrored_seed_brand`, the mirror's own
    # coalesce order), then the product row's copy of it (a re-key may have written that).
    for brand in (mirrored_seed_brand(seed_data), product.get("brand")):
        if not str(brand or "").strip():
            continue
        try:
            derived = resolve_seed_seller_identity(brand=str(brand), domain=merchant_domain)
        except ValueError:
            continue
        if derived.get("merchant_id") == merchant_id:
            return merchant_id
    return None


def _proof_live_variant_count(seed_data: Any) -> Optional[int]:
    """The storefront proof's `live_variant_count` (an int, exactly as the proof reads it), or None."""
    snapshot = seed_data.get("snapshot") if isinstance(seed_data, dict) else None
    proof = snapshot.get("shopify_cart_proof") if isinstance(snapshot, dict) else None
    count = proof.get("live_variant_count") if isinstance(proof, dict) else None
    return count if type(count) is int else None


# ── option 2 (PR C): ENRICHMENT rows on the cart-link lane, behind a dark flag ───────────────
#
# A `catalog_enrichment_agent_v1` row (product keys `ext:<slug>::<8hex>` and, for the retailer lane,
# `ext:retailer:<32hex>`) names a brand's own Shopify page in `canonical_url` and carries one
# `::v:` sku per variant plus the `::canonical` placeholder. It has NO seed-borne storefront proof
# and NO offer under its own seller: its offers sit under `agent_seed::…`. So none of the Shopify /
# mirror rules above apply, and the branch below is a separate path end to end:
#   * the product is found by KEY and enrichment source_system, and the POSTed host must be the
#     same storefront as its canonical_url host AND its source_domain (`_same_storefront_host`,
#     one `www.` fold; the gateway posts the storefront target's host, which may differ by `www.`);
#   * the seller is `product.merchant_id` only when `derive_enrichment_seller` re-derives it;
#   * the sku is the caller's (a live sku of this product), else the ONE real sku, else the
#     placeholder -- never a pick among two;
#   * the Shopify variant is the one `verify_enrichment_cart_proof` returns from the proof row, and
#     nothing else;
#   * the price is the listing's own offers, and must equal the proof's live price.
# REAP_AGENTIC_CART_LINK_ENRICHMENT_ENABLED off: none of this runs, not even the product read.
_CART_ENRICHMENT_PRODUCT_SQL = """
    SELECT p.product_key, p.merchant_id, p.seller_ref, p.seed_kind, p.source_system,
           p.source_domain, p.canonical_url, p.title AS product_title, p.brand
      FROM catalog_products p
     WHERE p.product_key = :product_key AND p.source_system = :source_system
       AND p.suppression_reason IS NULL
       AND p.suppressed_at IS NULL
"""
# A caller-named sku, by its key exactly, live only.
_CART_ENRICHMENT_SKU_BY_KEY_SQL = """
    SELECT s.sku_key, s.source_variant_id, s.sku_payload
      FROM catalog_skus s
     WHERE s.product_key = :product_key AND s.sku_key = :variant_key
       AND s.suppression_reason IS NULL AND s.suppressed_at IS NULL
"""
# LIMIT 3 IS EXACT, NOT A SAMPLE. The choice needs only "how many real skus: 0, 1, or 2+". The
# primary key admits one placeholder at most, so any three rows hold at least two real skus -- a
# refusal whatever the rest are -- and fewer than three rows are all of them.
_CART_ENRICHMENT_LIVE_SKUS_SQL = """
    SELECT s.sku_key, s.source_variant_id, s.sku_payload
      FROM catalog_skus s
     WHERE s.product_key = :product_key
       AND s.suppression_reason IS NULL AND s.suppressed_at IS NULL
     ORDER BY s.sku_key
     LIMIT 3
"""
# EVERY `::v:` sku of the product, SUPPRESSED ONES INCLUDED (review of #2460): the verifier lets the
# placeholder stand for the product only when the catalog knows no variant of it at all, and a
# live-only count of 0 is exactly how a MAC parent whose shade skus were suppressed buys its
# one-variant stub (the variant restates the product title, `P2000_` sku, no image -- not "Default
# Title"; services/reap_enrichment_cart_proof.py THE PLACEHOLDER). It is ALSO the no-`variant_key`
# rule's count (review of #2465): a product whose catalog knows two variants, one of them
# suppressed, is still a product with two variants.
# `substr` rather than LIKE only so the prefix is compared as a plain string (`::v:` after this
# exact product_key), with no pattern characters to escape.
_CART_ENRICHMENT_VARIANT_SKU_COUNT_SQL = """
    SELECT count(*) AS n
      FROM catalog_skus s
     WHERE s.product_key = :product_key
       AND substr(s.sku_key, 1, :variant_prefix_len) = :variant_prefix
"""
# THE LISTING'S OWN OFFERS on this sku: written by the enrichment lane (source_system), under its
# `agent_seed::` seller namespace, live, priced and not out of stock (`_CART_ALL_OFFERS_SQL`'s
# availability rule). NOT filtered on currency: an offer in another currency is the listing's price
# in the wrong money, which `enrichment_offer_price_ok` names `row_currency_mismatch` rather than
# `row_unpriced`. The source_ref's host and handle are checked in Python (`_enrichment_listing_offer`),
# exactly, by the same URL reader the verifier uses. Bounded: more than 50 is not one listing.
# `offer_id` is not read here: services/reap_price_writeback selects the rows it may write through
# this same statement, so the price it corrects is exactly the one this route reads.
_CART_ENRICHMENT_OFFERS_SQL = """
    SELECT o.offer_id, o.currency,
           CAST(coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price)
                AS TEXT) AS price,
           o.source_ref
      FROM catalog_offers o
     WHERE o.product_key = :product_key AND o.sku_key = :sku_key
       AND o.source_system = :source_system
       AND substr(o.merchant_id, 1, :seed_merchant_prefix_len) = :seed_merchant_prefix
       AND o.suppression_reason IS NULL AND o.suppressed_at IS NULL
       AND coalesce(o.merchant_effective_price, o.estimated_best_price, o.list_price) IS NOT NULL
       AND lower(coalesce(o.availability, 'unknown')) NOT IN
           ('out_of_stock', 'sold_out', 'unavailable')
     ORDER BY o.offer_id
     LIMIT 51
"""
#: More offers than this on one sku is not one listing's price.
_CART_ENRICHMENT_MAX_OFFERS = 50
#: THE LEGACY COLLAPSED KEY (review of PIVOTA-Agent#2330, defence in depth beside the gateway's
#: identical refusal): `ext:unknown::<8 hex>` was minted for products with no brand, and MANY
#: different products shared one such key -- so a proof "for" it proves whichever product was
#: written last. It names no one product and is refused before anything is read for it. The
#: distinct 16-hex successor (`ext:unknown::<16 hex>`, pivota-backend#2461) is NOT this shape.
_LEGACY_COLLAPSED_ENRICHMENT_KEY = re.compile(r"ext:unknown::[0-9a-f]{8}")
#: The enrichment lane's offer-seller namespace (`ingestion.derive_merchant_id`:
#: `agent_seed::<slug>`, `agent_seed::retailer::<host>`). It is where the listing's offers live;
#: it is NEVER the seller of record (that is `product.merchant_id`).
_ENRICHMENT_OFFER_MERCHANT_PREFIX = "agent_seed::"


def _enrichment_sku_choice(
    live_skus: List[Mapping[str, Any]], product_key: str, variant_sku_count: int,
) -> Dict[str, Any]:
    """The sku an enrichment row's cart link buys when the caller named none, or a refusal.

    `live_skus` is `_CART_ENRICHMENT_LIVE_SKUS_SQL`'s rows (at most three); `variant_sku_count` is
    `_CART_ENRICHMENT_VARIANT_SKU_COUNT_SQL`'s: EVERY `::v:` sku, SUPPRESSED ONES INCLUDED. The
    placeholder is recognised by its KEY, `<product_key>::canonical`, as the verifier recognises it
    -- never by `_is_placeholder_sku`, which compares `source_variant_id` to the product key and so
    misreads every enrichment key longer than 128 characters (the id is truncated there).
      * the catalog knows two or more variants (suppressed ones count) -> `row_variant_ambiguous`.
        A LIVE-only rule (review of #2465, P1) bought NC10 of a 3-shade MAC line whose other two
        shades were suppressed, and the 8 oz of a two-size bluemercury wash whose 16.9 oz was;
      * two or more live real skus  -> `row_variant_ambiguous`: never pick one;
      * exactly one real sku        -> it (the verifier then proves its own variant, and the
                                       caller also requires its SOLE-variant mode);
      * none                        -> the placeholder (the verifier gates it on the catalog's
                                       variant count and the storefront's one variant);
      * nothing live at all         -> `row_not_found`.
    """
    if variant_sku_count > 1:
        raise svc.PurchaseRefused("row_variant_ambiguous", "the catalog knows two or more variants")
    placeholder_key = product_key + ENRICHMENT_PLACEHOLDER_SUFFIX
    real = [dict(row) for row in live_skus if row.get("sku_key") != placeholder_key]
    if len(real) > 1:
        raise svc.PurchaseRefused("row_variant_ambiguous", "two or more skus and none named")
    if real:
        return real[0]
    for row in live_skus:
        if row.get("sku_key") == placeholder_key:
            return dict(row)
    raise svc.PurchaseRefused("row_not_found", "no live sku for this product")


def _enrichment_listing_offer(offer: Mapping[str, Any], shop_host: str, handle: str) -> bool:
    """Is this offer the enrichment LISTING's own -- its source_ref the product's storefront page?

    `storefront_page` (the verifier's URL reader) must parse it as `https://<host>/products/<handle>`,
    the host must be the same storefront as the product's (`_same_storefront_host`: one `www.`
    fold, no subdomain, suffix or lookalike) and the handle must be the product's, exactly.
    """
    page = storefront_page(offer.get("source_ref"))
    return page is not None and _same_storefront_host(shop_host, page[0]) and page[1] == handle


def _enrichment_sku_payload(sku: Mapping[str, Any]) -> Dict[str, Any]:
    """`sku_payload` as a dict ({} when absent or unreadable; the verifier refuses a malformed one).
    asyncpg and SQLite both hand JSON back as text."""
    payload = sku.get("sku_payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return {}
    return payload if isinstance(payload, dict) else {}


def _enrichment_source_handle(sku: Mapping[str, Any]) -> str:
    """The sku's own storefront handle when it is a FOLDED shade (`sku_payload.source_handle`),
    else ''."""
    handle = _enrichment_sku_payload(sku).get("source_handle")
    return handle.strip() if isinstance(handle, str) else ""


def _enrichment_variant_title(sku: Mapping[str, Any]) -> Optional[str]:
    """DISPLAY ONLY: a variant title the sku's payload carries, else None. The proof table has no
    title column and the enrichment writer puts none in `sku_payload` today, so this is None on
    every live row; it is read so the day either source carries one, the purchase says it.
    Merchant-typed text, so it passes THE cart-link title rule (`clean_variant_title`, #2462).

    TODO(option 2 PR B, #2464): once `enrichment_cart_variant_proofs` carries the storefront's own
    `variant_title` column, prefer the PROOF's title (the live storefront's words, as the mirror
    lane does) and select it in `db.enrichment_cart_variant_proofs._SELECT_PROOF_SQL`; this PR
    does not add the column."""
    return clean_variant_title(_enrichment_sku_payload(sku).get("variant_title"))


async def _load_enrichment_cart_link_item(
    *, product: Dict[str, Any], merchant_host: str, variant_key: Optional[str],
    market_country: str,
) -> Tuple[Dict[str, Any], str, str, Optional[str]]:
    """`_load_cart_link_item` for an ENRICHMENT row: the same four-tuple, or a refusal.

    Reached only with REAP_AGENTIC_CART_LINK_ENRICHMENT_ENABLED on, and only for a product whose
    source_system is the enrichment lane's. See the note above `_CART_ENRICHMENT_PRODUCT_SQL`.
    """
    product_key = str(product.get("product_key") or "")
    if _LEGACY_COLLAPSED_ENRICHMENT_KEY.fullmatch(product_key):
        raise svc.PurchaseRefused("row_not_found", "a legacy collapsed key names no one product")
    # THE STOREFRONT. The POSTed host must be this row's store: the canonical_url's host AND its
    # source_domain, each after one `www.` fold. Anything else is not this product "under this
    # domain" -- the same `row_not_found` the lane answers for a key on another domain.
    page = storefront_page(product.get("canonical_url"))
    if (page is None
            or not _same_storefront_host(merchant_host, page[0])
            or not _same_storefront_host(merchant_host, product.get("source_domain"))):
        raise svc.PurchaseRefused("row_not_found", "no catalog product for this domain and key")
    shop_host, handle = page

    # THE SELLER OF RECORD: `product.merchant_id`, and only when the repo's own minting functions
    # re-derive it from this row. Never the `agent_seed::` offer merchant, never a seed's seller_ref.
    # A non-null product seller_ref must agree with it (the mirror rule, `_mirror_seller_ref`).
    # (`merchant_id` is NOT NULL, so an underivable seller -- None -- never equals it.)
    merchant_id = product.get("merchant_id")
    seller_ref = str(product.get("seller_ref") or "").strip()
    if derive_enrichment_seller(product) != merchant_id or (seller_ref and seller_ref != merchant_id):
        raise svc.PurchaseRefused("seller_identity_unverified", "enrichment seller does not re-derive")

    # THE CATALOG'S VARIANT COUNT, suppressed skus included: the no-key rule and the verifier's
    # placeholder gate both read it, so it is read before any sku is chosen or proof is read.
    variant_prefix = product_key + ENRICHMENT_VARIANT_INFIX
    variant_sku_count = int(await database.fetch_val(
        _CART_ENRICHMENT_VARIANT_SKU_COUNT_SQL,
        {"product_key": product_key, "variant_prefix": variant_prefix,
         "variant_prefix_len": len(variant_prefix)},
    ) or 0)

    # THE SKU.
    if variant_key:
        raw_sku = await database.fetch_one(
            _CART_ENRICHMENT_SKU_BY_KEY_SQL, {"product_key": product_key, "variant_key": variant_key}
        )
        if raw_sku is None:
            raise svc.PurchaseRefused("row_not_found", "no sku for this product and key")
        sku = dict(raw_sku)
    else:
        sku = _enrichment_sku_choice(
            [dict(row) for row in await database.fetch_all(
                _CART_ENRICHMENT_LIVE_SKUS_SQL, {"product_key": product_key}
            )],
            product_key,
            variant_sku_count,
        )
        # A FOLDED SHADE IS ONE CHOICE AMONG A FAMILY (review of #2465, P2 / A7). A sku whose
        # `sku_payload.source_handle` names another handle than the canonical_url's is one shade of
        # a folded family (`curated_brand_feed` sets source_handle ONLY for folded variants): the
        # catalog holding just that one shade, and its own handle having one variant, does not make
        # the family single-variant. Nobody named it, so it is not bought -- refused before the
        # proof is read. A caller-NAMED folded shade is unaffected (the branch above).
        folded_handle = _enrichment_source_handle(sku)
        if folded_handle and folded_handle != handle:
            raise svc.PurchaseRefused("row_variant_ambiguous", "one shade of a folded family")

    # THE VARIANT: only what the verifier returns from the storefront proof for exactly this sku.
    proof = await enrichment_proofs.fetch_proof(product_key, str(sku.get("sku_key") or ""))
    proven, variant_id, reason = verify_enrichment_cart_proof(
        product, sku, proof, catalog_variant_sku_count=variant_sku_count, now=_now(),
    )
    if not proven:
        raise svc.PurchaseRefused("row_variant_unverified", f"enrichment proof refused: {reason}")
    # NOBODY NAMED A VARIANT, SO THE STOREFRONT MUST HAVE ONLY ONE (review of #2465, P1). The
    # verifier also accepts a NAMED-variant proof (the handle has several variants and this sku's
    # id is one of them) -- right when the caller named the sku, a silent pick when nobody did: the
    # catalog holding one of a storefront's two sizes is not a buyer choosing that size. (The
    # catalog-side count is enforced before the choice, in `_enrichment_sku_choice`.)
    if not variant_key and reason != ENRICHMENT_SOLE_VARIANT:
        raise svc.PurchaseRefused("row_variant_ambiguous", "the storefront has several variants")

    # THE PRICE: the listing's own offers on this sku, equal to the proof's live price, in the
    # buyer market's currency -- decided here, BEFORE any click or purchase row exists.
    offers = [dict(row) for row in await database.fetch_all(
        _CART_ENRICHMENT_OFFERS_SQL,
        {"product_key": product_key, "sku_key": sku["sku_key"],
         "source_system": ENRICHMENT_SOURCE_SYSTEM,
         "seed_merchant_prefix": _ENRICHMENT_OFFER_MERCHANT_PREFIX,
         "seed_merchant_prefix_len": len(_ENRICHMENT_OFFER_MERCHANT_PREFIX)},
    )]
    if len(offers) > _CART_ENRICHMENT_MAX_OFFERS:
        raise svc.PurchaseRefused("row_price_ambiguous", "too many offers for one listing")
    listing = [offer for offer in offers if _enrichment_listing_offer(offer, shop_host, handle)]
    market_currency = str(_MARKET_CURRENCY.get(market_country) or "").upper()
    priced, price_minor, price_reason = enrichment_offer_price_ok(listing, proof, market_currency)
    if not priced:
        raise svc.PurchaseRefused(price_reason, "enrichment offer price refused")

    return (
        {"shop_domain": merchant_host, "our_price_minor": int(price_minor),
         "currency": market_currency, "market_country": market_country,
         # Merchant-typed text: the same display rule as the mirror/Shopify path (#2467).
         "product_name": clean_product_name(product.get("product_title")),
         "product_key": product_key,
         "variant_title": _enrichment_variant_title(sku)},
        str(merchant_id),
        variant_id,
        str(product.get("seed_kind") or "").strip() or None,
    )


async def _load_cart_link_item(
    *, merchant_domain: str, product_key: str, variant_key: Optional[str],
    market_country: str,
) -> Tuple[Dict[str, Any], str, str, Optional[str]]:
    """Return server-priced cart item, seller identity, and Shopify variant, or refuse.

    The buyer/agent supplies only catalog keys. No caller URL, price, Shopify variant, or seller
    identity is accepted. Mirrored seeds need an attached product and a sole variant proven by
    storefront evidence. Numeric operator input alone cannot authorize a purchase.

    ENRICHMENT rows (option 2) take `_load_enrichment_cart_link_item` instead, and only while
    REAP_AGENTIC_CART_LINK_ENRICHMENT_ENABLED is on. Off, not one extra statement runs here.
    """
    if svc.is_cart_link_enrichment_enabled():
        enrichment = await database.fetch_one(
            _CART_ENRICHMENT_PRODUCT_SQL,
            {"product_key": product_key, "source_system": ENRICHMENT_SOURCE_SYSTEM},
        )
        if enrichment is not None:
            return await _load_enrichment_cart_link_item(
                product=dict(enrichment), merchant_host=merchant_domain,
                variant_key=variant_key, market_country=market_country,
            )
    raw = await database.fetch_one(
        _CART_PRODUCT_SQL, {"product_key": product_key, "merchant_domain": merchant_domain}
    )
    if raw is None:
        raise svc.PurchaseRefused("row_not_found", "no catalog product for this domain and key")
    product = dict(raw)
    platform = str(product.get("platform") or "").strip().lower()
    if platform not in ("shopify", "external_seed"):
        raise svc.PurchaseRefused("row_not_shopify", "cart-link product has no Shopify source")

    proof_scope: Optional[str] = None  # the mirror's storefront-proof scope; None on shopify rows
    proof_variant_title: Optional[str] = None  # the proof's live title, display only
    named: Optional[Dict[str, Any]] = None
    if variant_key:
        raw_sku = await database.fetch_one(
            _CART_SKU_BY_KEY_SQL, {"product_key": product_key, "variant_key": variant_key}
        )
        if raw_sku is None:
            raise svc.PurchaseRefused("row_not_found", "no sku for this product and key")
        named = dict(raw_sku)

    placeholder: Optional[Dict[str, Any]] = None
    seed_data: Any = None
    if platform == "shopify":
        if named is not None:
            # The caller named the sku: exactly as before -- that sku, its variant, its offer.
            candidates = [named]
            variant_id = extract_shopify_numeric_variant_id(named.get("source_variant_id"))
        else:
            skus = [dict(row) for row in await database.fetch_all(
                _CART_PRODUCT_SKUS_SQL, {"product_key": product_key}
            )]
            variant_id, candidates, _placeholder = _cart_sku_choice(skus, product_key)
        seller_ref = str(product.get("seller_ref") or product.get("merchant_id") or "").strip()
    else:
        if str(product.get("source_system") or "") != "external_product_seeds_mirror_v1":
            raise svc.PurchaseRefused("row_variant_unverified", "external seed source is unknown")
        # A mirror is a product-grain row: its sku is chosen by the SAME rule as the no-key path,
        # never by the caller. A caller-named sku must be one of the rows that rule chose.
        skus = [dict(row) for row in await database.fetch_all(
            _CART_PRODUCT_SKUS_SQL, {"product_key": product_key}
        )]
        if named is not None and not _is_placeholder_sku(named, product_key):
            sku_variant = _cart_numeric_variant(named.get("source_variant_id"), product_key)
            candidates = [named]
            placeholder = None  # A selected size never inherits a product-level price.
        else:
            sku_variant, candidates, placeholder = _cart_sku_choice(skus, product_key)
        if candidates and sku_variant is None:
            # A mirror's one real sku names no Shopify variant: not a cart this lane can prove.
            raise svc.PurchaseRefused("row_variant_unverified", "mirror sku names no Shopify variant")
        if not candidates:
            # No real variant sku: the placeholder is the only row, and the storefront proof
            # below is the only identity (unchanged from before this rule).
            candidates = [placeholder] if placeholder else []
        if not candidates:
            raise svc.PurchaseRefused("row_not_found", "variant is ambiguous")
        if named is not None:
            if named["sku_key"] not in {row["sku_key"] for row in candidates}:
                raise svc.PurchaseRefused("row_variant_unverified", "mirror variant is ambiguous")
            # The caller named one of the chosen spellings: THAT sku is priced.
            candidates = [named]
        seed = await database.fetch_one(
            _CART_SEED_VARIANT_SQL,
            {"seed_id": product.get("source_ref"), "product_key": product_key,
             "merchant_domain": merchant_domain,
             "market_country": market_country},
        )
        seed = dict(seed) if seed else {}
        seed_data = seed.get("seed_data")
        if isinstance(seed_data, str):
            try:
                seed_data = json.loads(seed_data)
            except (TypeError, ValueError):
                seed_data = None
        # Sole-variant proof, or a named-variant proof (a multi-variant product whose seed names
        # ONE variant) -- which also requires the catalog's chosen variant to be that one.
        proven = verified_cart_variant_id(
            seed_data,
            product_urls=[seed.get("canonical_url") or seed.get("destination_url")],
            shop_domain=merchant_domain,
            catalog_variant_id=sku_variant,
        )
        if named is not None and not _is_placeholder_sku(named, product_key):
            selected_proof = verified_selected_cart_variant_id(
                seed_data, product_urls=[seed.get("canonical_url") or seed.get("destination_url")],
                shop_domain=merchant_domain, catalog_variant_id=sku_variant,
            )
            proven = selected_proof or proven
        variant_id = proven.variant_id if proven else None
        proof_scope = proven.scope if proven else None
        proof_variant_title = proven.variant_title if proven else None
        # THE SEED'S STOREFRONT PROOF IS THE AUTHORITY; the catalog sku must AGREE with it. A sku
        # naming another Shopify variant than the one the storefront proved is a contradiction.
        if sku_variant is not None and variant_id != sku_variant:
            raise svc.PurchaseRefused("row_variant_unverified", "catalog variant contradicts storefront")
        # An attachment naming another id is a contradiction, even if its string is all digits.
        # Never use it as a fallback: that is the numeric-SKU wrong-cart bug in the gateway.
        attached_id = str(seed.get("attached_variant_id") or "").strip()
        if attached_id and extract_shopify_numeric_variant_id(attached_id) != variant_id:
            raise svc.PurchaseRefused("row_variant_unverified", "seed identity contradicts storefront")
        seller_ref = _mirror_seller_ref(product, seed, seed_data, merchant_domain) or ""
    if not variant_id:
        raise svc.PurchaseRefused("row_variant_unverified", "no verified Shopify numeric variant")
    if not seller_ref or seller_ref != str(product.get("merchant_id") or "").strip():
        raise svc.PurchaseRefused("seller_identity_unverified", "catalog seller identity is ambiguous")

    # THE PRICED SKU. A caller-named sku is priced itself (`candidates == [named]`). Without a
    # name, every spelling of the ONE chosen variant that this seller has a usable offer on is
    # read, and they must AGREE (review of #2453): the lowest sku_key's price is only a
    # deterministic pick when it is the same price as the others. Disagreeing spellings are a
    # price nobody can vouch for -> `row_price_ambiguous`.
    #
    # A MIRROR ROW MAY BE PRICED FROM ITS `::canonical` PLACEHOLDER (staging demo, 2026-09-29:
    # KraveBeauty 24 Carrot Retinal). The mirror writes its offer on the placeholder
    # (`external_offer_dual_write.derive_mirror_sku_key`); a real variant sku another lane added
    # often carries none. The placeholder names no variant, so its offer is the variant's price
    # ONLY while the storefront proof shows exactly ONE live variant (`live_variant_count == 1`,
    # checked here itself, not inherited from today's proof: a multi-variant, variant-scoped proof
    # must never be priced from a product-grain offer). Rules (review of #2457):
    #   * the real sku's offer WINS whenever it has one -- it is variant-scoped and fresher, and
    #     Reap's exact-subtotal check refuses a stale price at quote time; the placeholder is
    #     not read and cannot disagree with it;
    #   * only with NO usable real-sku offer is the placeholder read, EVERY usable offer this
    #     seller has on it in the market's currency, and they must agree, else
    #     `row_price_ambiguous`;
    #   * only for a mirror row with no caller-named sku; Shopify rows are unchanged.
    mirror_placeholder = placeholder if platform != "shopify" and named is None else None
    # The same gate when the caller NAMES the placeholder (review of #2457): it is priced by the
    # ordinary read below, and must not become the way around `live_variant_count == 1`.
    if (platform != "shopify" and named is not None and _is_placeholder_sku(named, product_key)
            and _proof_live_variant_count(seed_data) != 1):
        raise svc.PurchaseRefused("row_unpriced", "a placeholder prices only a sole live variant")

    def _priced(candidate: Mapping[str, Any], found: Any) -> Tuple[Dict[str, Any], Dict[str, Any], str, Optional[int]]:
        found = dict(found)
        cur = str(found.get("currency") or candidate.get("currency") or "").strip().upper()
        return dict(candidate), found, cur, ledger.amount_minor_or_none(found.get("price"), cur)

    offer_params = {"product_key": product_key, "merchant_id": seller_ref,
                    "market_currency": _MARKET_CURRENCY.get(market_country)}
    priced: List[Tuple[Dict[str, Any], Dict[str, Any], str, Optional[int]]] = []
    for candidate in candidates:
        if mirror_placeholder is not None and candidate["sku_key"] == mirror_placeholder["sku_key"]:
            continue  # the placeholder is only ever read below, every offer, behind the gate
        found = await database.fetch_one(
            _CART_OFFER_SQL, {**offer_params, "sku_key": candidate["sku_key"]}
        )
        if found is not None:
            priced.append(_priced(candidate, found))
    if (not priced and mirror_placeholder is not None
            and _proof_live_variant_count(seed_data) == 1):
        priced = [_priced(mirror_placeholder, found) for found in await database.fetch_all(
            _CART_ALL_OFFERS_SQL, {**offer_params, "sku_key": mirror_placeholder["sku_key"]}
        )]
    if len({(cur, minor) for _c, _o, cur, minor in priced}) > 1:
        raise svc.PurchaseRefused(
            "row_price_ambiguous", "the spellings of this variant carry different prices"
        )
    if not priced:
        raise svc.PurchaseRefused("row_unpriced", "seller has no usable offer on this sku")
    sku, offer, currency, price_minor = priced[0]
    if proof_scope == CART_PROOF_SCOPE_NAMED and _is_placeholder_sku(sku, product_key):
        # A NAMED-variant proof is about one variant of a multi-variant product: only that
        # variant's own sku offer is its price. The product-level `::canonical` placeholder offer
        # stands in only under a SOLE proof (one live variant), never here.
        raise svc.PurchaseRefused("row_unpriced", "no offer on the proven variant's own sku")
    if currency != _MARKET_CURRENCY.get(market_country):
        raise svc.PurchaseRefused("row_currency_mismatch", "offer currency differs from market")
    if not price_minor or price_minor <= 0:
        raise svc.PurchaseRefused("row_unpriced", "offer price is not an exact minor amount")

    # URL and click id are filled by the route after it mints a new click. These are facts only;
    # there is no URL-shaped caller input anywhere in this path.
    return (
        {"shop_domain": merchant_domain, "our_price_minor": int(price_minor),
         "currency": currency, "market_country": market_country,
         # Merchant-typed catalog text: the same display rule as the variant title after it.
         "product_name": clean_product_name(product.get("product_title")),
         "product_key": product_key,
         # DISPLAY ONLY: which variant this link buys, in the live storefront's words, so a
         # door can show "07 BURGUNDY INK" -- the buyer never picks it on this lane.
         "variant_title": proof_variant_title},
        seller_ref,
        variant_id,
        str(product.get("seed_kind") or "").strip() or None,
    )


async def _record_cart_link_click(
    *, click_id: str, cart_url: str, shop_domain: str, seller_ref: str,
    product_key: str, variant_key: Optional[str], agent_id: str,
    seed_kind: Optional[str],
) -> None:
    """Persist the click identity before a purchase can be opened; failure closes the door.

    Recorded as ISSUED (ADR-025 D1): the link is handed to Reap, not followed by a buyer, so
    click_count stays 0 until something actually clicks. `issue_click` raises on a database error,
    which is what keeps this lane fail-closed: no click row, no purchase. It drops an agent id over
    64 characters or a seller over 50, as this function did before, rather than truncating them.
    """
    await issue_click(IssuedClick(
        click_id=click_id, surface="reap_cart_link", commerce_surface="reap_cart_link",
        source_channel="agent", agent_id=agent_id, merchant_id=seller_ref,
        destination_url=cart_url, dest_domain=shop_domain,
        context={"seller_ref": seller_ref, "seed_kind": seed_kind, "item_source": "cart_link",
                 "product_key": product_key, "variant_key": variant_key},
    ))


# ── idempotency ──────────────────────────────────────────────────────────────────────────────

#: Where Reap sends the buyer's browser after a hosted page. There is no such constant in the
#: client — it validates a return URL but never supplies one — so this rail's default lives here.
#: With neither env var set it is `https://api.pivota.cc/reap/return`, the static page in
#: routes/reap_return.py (the first entry of `rc.DEFAULT_RETURN_URL_HOSTS`).
#:
#: A CODE DEFAULT, derived from the client's own host allowlist, with the env var as an override.
#: The other order (env var required, code fallback absent) is the shape that took five flags
#: down with one environment wipe; and because the value is pushed through
#: `rc.validate_return_url` on every call like any caller-supplied one, an operator who sets
#: `REAP_AGENTIC_RETURN_URL` to a host that is not ours gets a refusal, not an open redirector
#: with a payment page in front of it.
_RETURN_URL_ENV = "REAP_AGENTIC_RETURN_URL"


def _default_return_url() -> str:
    configured = (os.getenv(_RETURN_URL_ENV) or "").strip()
    if configured:
        return configured
    hosts = rc.return_url_hosts()
    return f"https://{hosts[0]}/reap/return"


def _request_hash(
    *,
    merchant_domain: str,
    product_key: str,
    variant_key: Optional[str],
    quantity: int,
    email: str,
    shipping_address: Mapping[str, Any],
    return_url: str,
    item_source: str = "reap_variant",
    offer_code: Optional[str] = None,
    expected_unit_price_minor: Optional[int] = None,
    expected_currency: Optional[str] = None,
) -> str:
    """A stable fingerprint of the fields that DECIDE this purchase.

    WHAT IS IN IT is everything a buyer would notice being different: the merchant, the product,
    the variant, how many, who it goes to and where, and where they come back to afterwards.
    Change any of those and it is a different purchase, whatever key the caller reused.

    WHAT IS NOT IN IT: `click_context` (accepted and forwarded nowhere) and the idempotency key
    itself (it is the lookup, not part of what is being compared). Also nothing the ROUTE derives
    rather than the caller supplying — the authoritative price, the buyer ref, the click id — because those are
    ours and a client retrying an identical request must hash identically even though the click
    id will differ.

    BUILT FROM THE NORMALISED VALUES, not the raw body. The domain is already the canonical
    merchant (`_merchant_domain_key`: lowercased, one `www.` removed), the keys stripped, the
    address already through `_buyer_address_for_client` with its fallbacks applied. A retry that
    differs only in the casing of a domain, or in whether it carries the `www.`, or a
    missing-then-supplied `buyer.name` that resolves to the same recipient is the SAME request and must replay; a
    retry that differs in the address is not, and must not.

    `sort_keys=True` and a fixed separator so two dicts with the same content hash the same
    whatever order they were built in — the whole value of the column depends on that.
    The cart-link lane also includes `item_source`; the legacy variant lane keeps its original
    hash shape so existing idempotency keys retain their meaning across this rollout.
    """
    facts = {
        "merchant_domain": merchant_domain,
        "product_key": product_key,
        "variant_key": variant_key or "",
        "quantity": int(quantity),
        "email": email,
        "shipping_address": {str(k): str(v) for k, v in dict(shipping_address).items()},
        "return_url": return_url,
    }
    if item_source != "reap_variant":
        facts["item_source"] = item_source
    # Only when present, for the same reason as `item_source`: a request with no code keeps the
    # hash it always had, and a retry that ADDS or CHANGES a code is a different purchase (it
    # can change the price the buyer approves), so it must not replay the first one.
    if offer_code is not None:
        facts["offer_code"] = offer_code
    # Absent pair preserves every legacy fingerprint byte-for-byte. Recovery
    # compares this original pair without current price/proof/eligibility reads.
    if expected_unit_price_minor is not None or expected_currency is not None:
        if type(expected_unit_price_minor) is not int or expected_unit_price_minor <= 0 or not isinstance(expected_currency, str) or not re.fullmatch(r"[A-Z]{3}", expected_currency):
            raise svc.PurchaseRefused("invalid_request", "expected money must be a strict pair")
        facts["expected_unit_price_minor"] = expected_unit_price_minor
        facts["expected_currency"] = expected_currency
    canonical = json.dumps(
        facts,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _replayed_purchase_id(
    *, agent_id: str, agent_user_ref_hash: str, idempotency_key: str, request_hash: str
) -> Optional[str]:
    """The purchase this key already opened, or None — and `idempotency_conflict` when the key
    was used for a DIFFERENT request.

    The conflict is raised rather than ignored because the alternative is the failure mode the
    column exists to stop: answering 202 with a purchase of something the caller did not just ask
    for. A client that derives keys from a session or a cart id WILL reuse one across two
    different bodies, and it will read the 202 as "done".
    """
    row = await database.fetch_one(
        """
        SELECT purchase_id, request_hash, created_at
          FROM reap_agentic_purchase_keys
         WHERE agent_id = :agent_id
           AND agent_user_ref_hash = :agent_user_ref_hash
           AND idempotency_key = :idempotency_key
        """,
        {
            "agent_id": agent_id,
            "agent_user_ref_hash": agent_user_ref_hash,
            "idempotency_key": idempotency_key,
        },
    )
    if not row:
        return None
    record = dict(row)
    # A durable attempt remains the same purchase beyond 24 hours. Age never authorizes a
    # second possibly charged checkout; a new intentional purchase requires a new key.
    stored_hash = str(record.get("request_hash") or "")
    if not stored_hash or stored_hash != request_hash:
        raise svc.PurchaseRefused(
            "idempotency_conflict", "this key was used for a different request"
        )
    stored = str(record.get("purchase_id") or "").strip()
    if stored.startswith(_REFUSED_KEY_PREFIX):
        # A TOMBSTONE: this key, for this exact request, was refused (or retired). The same
        # answer again for the lifetime of this attempt key.
        from services.reap_unopened_attempt import MARKER
        if MARKER.fullmatch(stored):
            raise svc.PurchaseRefused("attempt_retired", "this original attempt was permanently retired")
        reason = stored[len(_REFUSED_KEY_PREFIX):]
        if reason not in _TOMBSTONED_REFUSALS:
            # Corrupt/unknown tombstones cannot authorize another purchase lane.
            raise _PurchasePersistenceUnavailable()
        raise svc.PurchaseRefused(reason, "this key was refused for this request")
    return stored or None


#: `reap_agentic_purchase_keys.purchase_id` of a key whose request was REFUSED (not a purchase).
_REFUSED_KEY_PREFIX = "refused:"
#: The refusals that were remembered against a key. Create no longer writes one (every attempt
#: reaching eligibility carries bound money that has not passed admission), but a key
#: tombstoned before that still answers its refusal on replay and `not_found` on recovery.
_TOMBSTONED_REFUSALS = frozenset({"merchant_not_eligible"})


class _PurchasePersistenceUnavailable(Exception):
    """A keyed outcome cannot be accepted or refused authoritatively."""


# Lifetime mappings are immutable at the SQL conflict fence too, not just at lookup.
# Concurrent first requests still converge through the winner re-read/duplicate cleanup.
_WRITE_KEY_SQL = """
    INSERT INTO reap_agentic_purchase_keys (
        agent_id, agent_user_ref_hash, idempotency_key, purchase_id, request_hash
    ) VALUES (
        :agent_id, :agent_user_ref_hash, :idempotency_key, :purchase_id, :request_hash
    )
    ON CONFLICT (agent_id, agent_user_ref_hash, idempotency_key) DO NOTHING
    RETURNING purchase_id
"""

_WRITE_KEY_SQL_SQLITE = """
    INSERT INTO reap_agentic_purchase_keys (
        agent_id, agent_user_ref_hash, idempotency_key, purchase_id, request_hash
    ) VALUES (
        :agent_id, :agent_user_ref_hash, :idempotency_key, :purchase_id, :request_hash
    )
    ON CONFLICT (agent_id, agent_user_ref_hash, idempotency_key) DO NOTHING
    RETURNING purchase_id
"""


async def _write_idempotency_key(
    *, agent_id: str, agent_user_ref_hash: str, idempotency_key: str, purchase_id: str,
    request_hash: str,
) -> bool:
    """Land a new immutable key mapping. True when this insert won."""
    values = {
        "agent_id": agent_id,
        "agent_user_ref_hash": agent_user_ref_hash,
        "idempotency_key": idempotency_key,
        "purchase_id": purchase_id,
        "request_hash": request_hash,
    }
    if IS_POSTGRES:
        row = await database.fetch_one(
            _WRITE_KEY_SQL, values
        )
    else:
        row = await database.fetch_one(
            _WRITE_KEY_SQL_SQLITE, values,
        )
    return row is not None


async def _claim_idempotency_key(
    *,
    agent_id: str,
    agent_user_ref_hash: str,
    idempotency_key: str,
    purchase_id: str,
    request_hash: str,
) -> str:
    """Bind an immutable key inside the caller's purchase transaction.

    No worker can observe our purchase until both writes commit. SQL errors must
    unwind the transaction; a conflict re-read can return only a durable winner.
    """
    landed = await _write_idempotency_key(
        agent_id=agent_id, agent_user_ref_hash=agent_user_ref_hash,
        idempotency_key=idempotency_key, purchase_id=purchase_id, request_hash=request_hash,
    )
    if landed:
        return purchase_id

    # THE LOSER RE-READS, AND THE RE-READ CAN REFUSE. Two concurrent requests with one key and
    # two different bodies race here: the winner's hash lands, and this call raises
    # `idempotency_conflict` for the loser rather than handing it the winner's purchase. That is
    # the same answer the sequential case gives, which is the point — a race must not be a way to
    # get an answer the ordinary path refuses.
    winner = await _replayed_purchase_id(
        agent_id=agent_id,
        agent_user_ref_hash=agent_user_ref_hash,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    if not winner:
        raise _PurchasePersistenceUnavailable()
    return winner


async def _abandon_duplicate(purchase_id: str) -> None:
    """Scrub the uncommitted same-body loser; failed cleanup rolls back the unit."""
    changed = await ledger.transition(
        purchase_id, from_states=("resolving",), to_state="refused",
        refusal_reason="duplicate_idempotency_key",
    )
    if changed is None:
        raise _PurchasePersistenceUnavailable()


# ── the owner-facing body ────────────────────────────────────────────────────────────────────

#: Every state in which a hosted URL is a link we want a buyer to open. `resolving` and `quoting`
#: have no page yet; the terminals have one that is spent, and a completed purchase whose
#: approval link still worked would be a second charge waiting to happen.
_HOSTED_STATES = frozenset({"needs_enrollment", "awaiting_approval"})

#: Lifted out of the flat view into `totals` so each number has ONE spelling in the response.
_TOTAL_KEYS = (
    "our_price_minor",
    "quoted_total_minor",
    "final_total_minor",
    "shipping_minor",
    "tax_minor",
    # mig 247: True when `tax_minor` is already INSIDE the prices (Reap's `tax.includedInPrices`),
    # so a door summing subtotal + shipping + tax must not add it again. None until quoted.
    "tax_included",
    # mig 247: what Reap's offer-code discount took off, as evidence beside the total it
    # explains. `quoted_total_minor` is already net of it -- it is Reap's `finalAmount`.
    "discount_minor",
)


#: The price-witness columns (mig 258). Popped out of the flat view and re-emitted, nested, ONLY
#: when they apply -- see `_price_witness_fields`.
_PRICE_WITNESS_KEYS = (
    "preflight_outcome", "preflight_error_code", "preflight_checked_at",
    "preflight_items_subtotal_minor", "preflight_shipping_minor", "preflight_tax_minor",
    "preflight_tax_included", "preflight_total_minor",
    "live_unit_price_minor", "live_items_subtotal_minor", "live_quoted_total_minor",
    "live_price_stage",
    "price_rebound_from_minor", "price_rebound_to_minor", "price_corroboration_source",
    "price_corroborated_at",
)


#: The states before any approval quote exists: the only ones a preflight's totals describe.
_PREFLIGHT_VISIBLE_STATES = frozenset({"resolving", "needs_enrollment"})
#: The refusals whose cause IS the quoted item price (services/reap_agentic_purchase).
_LIVE_PRICE_CODES = frozenset({"quote_items_subtotal_mismatch", "quote_price_increased_corroborated"})


def _price_witness_fields(body: Dict[str, Any], *, state: str, currency: Any) -> None:
    """Pop the mig-258 keys from `body` and add, only when each applies:

      preflight    the buy-intent quote CONFIRMED the price (`preflight_outcome == 'ok'`) and no
                   approval quote exists yet ('resolving' / 'needs_enrollment'): its totals.
      live_price   the purchase ENDED `price_changed` BECAUSE OF the quoted item price (the
                   subtotal / corroborated-increase codes) and a live price is recorded: the
                   merchant's live unit price (null when the subtotal is not a multiple of the
                   quantity), the quoted items subtotal and total, and the stage that saw it.
      price_rebound  the purchase continued at a LOWER live unit price our own store read
                   corroborated: from / to unit prices, the corroborating source, when.

    A row the dark dials never touched has none of the three, so the body is exactly what it was.
    """
    witness = {key: body.pop(key, None) for key in _PRICE_WITNESS_KEYS}
    # ONLY BEFORE ANY LATER QUOTE: from 'quoting' on, the approval quote's own totals (`totals`)
    # supersede the witness's, so a door can never show two different confirmed totals.
    if (state in _PREFLIGHT_VISIBLE_STATES and witness["preflight_outcome"] == "ok"
            and witness["preflight_total_minor"] is not None):
        body["preflight"] = {
            "checked_at": witness["preflight_checked_at"],
            "totals": {
                "currency": currency,
                "items_subtotal_minor": witness["preflight_items_subtotal_minor"],
                "shipping_minor": witness["preflight_shipping_minor"],
                "tax_minor": witness["preflight_tax_minor"],
                "tax_included": witness["preflight_tax_included"],
                "total_minor": witness["preflight_total_minor"],
            },
        }
    # ONLY WHEN THE REFUSAL IS THE QUOTE-PRICE REFUSAL ITSELF: another `price_changed` (the
    # resolver's `_price_verdict`, a reconcile/shipping/currency failure) is not "the price is X".
    if (state == "refused" and body.get("refusal_reason") == "price_changed"
            and body.get("last_error_code") in _LIVE_PRICE_CODES
            and witness["live_items_subtotal_minor"] is not None):
        body["live_price"] = {
            "currency": currency,
            "unit_price_minor": witness["live_unit_price_minor"],
            "items_subtotal_minor": witness["live_items_subtotal_minor"],
            "quoted_total_minor": witness["live_quoted_total_minor"],
            "stage": witness["live_price_stage"],
        }
    if witness["price_rebound_to_minor"] is not None:
        body["price_rebound"] = {
            "currency": currency,
            "from_unit_price_minor": witness["price_rebound_from_minor"],
            "to_unit_price_minor": witness["price_rebound_to_minor"],
            "source": witness["price_corroboration_source"],
            "corroborated_at": witness["price_corroborated_at"],
        }


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: Any) -> Optional[datetime]:
    """A comparable UTC datetime, or None.

    THREE SHAPES ARRIVE HERE AND ONLY ONE OF THEM IS A DATETIME. asyncpg hands a `timestamptz`
    back as an AWARE datetime; SQLite hands the same column back as a **string**, because this
    module's reads of `reap_agentic_purchase_keys` are raw SQL with no result processor; and the
    ledger's own reads are already normalised to NAIVE UTC datetimes. Comparing an aware datetime
    to a naive one raises, and treating a string as "not a datetime" silently skips the
    comparison. Timestamp comparison must behave identically on both dialects.

    `ledger._decode_dt` is the repo's one parser for the string shape, and it is reused rather
    than reimplemented: a second parser would be a second opinion about what SQLite stored, and
    the ledger is the module that decided the storage format.
    """
    if isinstance(value, str):
        value = ledger._decode_dt(value)
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _iso_utc(value: datetime) -> str:
    """ISO-8601 with an explicit `+00:00` offset — the SAME spelling FastAPI's encoder gives the
    sibling timestamps in this body, so a door that parses one parses all of them.

    The `astimezone` is defensive: every value that reaches here is already UTC-aware (the
    ledger normalises both dialects), so it is a no-op today. If a non-UTC aware value ever
    arrived, the sibling column would be spelled with its own offset by FastAPI while this key
    is re-spelled `+00:00` — the route test that pins the two equal is what would notice."""
    return value.astimezone(timezone.utc).isoformat()


def approval_deadline(quote_expires_at: Any, hosted_url_expires_at: Any) -> Optional[datetime]:
    """The instant after which the buyer's approval can no longer succeed: the EARLIER of the two
    expiries the row holds, ignoring an absent one. None only when both are absent.

    MEASURED 2026-09-25 IN THE REAP SANDBOX, twice (with and without `X-Simulate-Checkout`,
    neither approved): `POST /agentic/checkouts` answers REQUIRES_ACTION with
    `nextAction.expiresAt` = created + 15 min, but the checkout flips to FAILED — not EXPIRED —
    1–10 s after the QUOTE's `expiresAt` (created + 5 min), and never passes PROCESSING. The
    hosted page's expiry is therefore ten minutes of a link that no longer works, and a deadline
    read from `hosted_url_expires_at` alone tells the buyer they have time they do not have.

    MIN, NOT "THE QUOTE": the quote can be absent (the spec does not require `expiresAt`), and a
    partner that one day kills the page before the quote is still a partner we read correctly.
    Both inputs go through `_aware`, the route's one parser for the three shapes a timestamp can
    arrive in (aware datetime, naive datetime, SQLite text). On THIS path both values are already
    aware UTC — the owner reads go through the ledger's `_normalize_row` on both dialects — so
    the naive and text branches are defence in depth for a caller that hands this function a raw
    row, not a shape the routes produce today. A comparison between an aware and a naive
    datetime raises, and a string skipped as "not a datetime" would be a deadline silently
    ignored, which is why the parser is applied rather than assumed.
    """
    candidates = [
        value
        for value in (_aware(quote_expires_at), _aware(hosted_url_expires_at))
        if value is not None
    ]
    return min(candidates) if candidates else None


def _public_body(view: Mapping[str, Any]) -> Dict[str, Any]:
    """The owner-facing shape, built FROM the ledger's allowlist and never from the row.

    `view` is already `ledger.public_purchase_view` output, so the only keys that can appear are
    `PUBLIC_PURCHASE_COLUMNS`. This function only ever REMOVES keys and adds ones it computes
    from keys already in the view; it never reads a column of the row. That is what makes "a
    buyer id cannot appear in this response" a property of the code rather than a thing the
    tests hope for — there is no expression here that could put one there.
    """
    body = {key: value for key, value in view.items()}
    state = str(body.get("state") or "")

    currency = body.get("currency")
    totals: Dict[str, Any] = {"currency": currency}
    for key in _TOTAL_KEYS:
        totals[key] = body.pop(key, None)
    body.pop("currency", None)
    body["totals"] = totals
    _price_witness_fields(body, state=state, currency=currency)

    # THE HOSTED URL IS RE-VETTED AT READ TIME, against the same allowlist that let it be stored.
    # Defence in depth, and not theatre: the value in the column came from a partner, it is the
    # one field on this row whose whole purpose is to be opened by a human, and the cost of the
    # check is a `urlparse`. A URL that no longer passes is DROPPED rather than reported — there
    # is nowhere to send the buyer, which is exactly what an absent key says.
    hosted_url = body.pop("hosted_url", None)
    hosted_expires = body.pop("hosted_url_expires_at", None)
    # THE LINK DIES AT THE APPROVAL DEADLINE, NOT AT THE PAGE'S OWN EXPIRY. On 'awaiting_approval'
    # that is the earlier of the quote's expiry and the page's (see `approval_deadline`); on
    # 'needs_enrollment' nothing has been quoted — the row re-quotes after the card is added, so
    # the quote column, always NULL there by the legal edges, is not consulted. `_now()` is the
    # same aware-UTC clock as the provider deadline; `reap_quote_expires_at` itself stays in
    # the body untouched, as the raw column it has always been.
    if state == "awaiting_approval":
        deadline = approval_deadline(body.get("reap_quote_expires_at"), hosted_expires)
    else:
        deadline = approval_deadline(None, hosted_expires)
    if (
        state in _HOSTED_STATES
        and rc.hosted_url_is_allowed(hosted_url)
        and (deadline is None or deadline > _now())
        and (state != "needs_enrollment" or deadline is not None)
    ):
        body["hosted_url"] = hosted_url
        body["hosted_url_expires_at"] = hosted_expires
    # Surfaced whether or not it has passed: a deadline in the past beside a missing `hosted_url`
    # is the honest reading of a row the poller has not yet flipped, and a door that only ever
    # saw the key on live rows would learn to treat its absence as "still open".
    if state == "awaiting_approval" and deadline is not None:
        body["approval_deadline"] = _iso_utc(deadline)

    # Reap's order id is EVIDENCE on every other state and an ANSWER on `completed`. Renamed on
    # the way out because the caller is being told "your order exists and this is its reference",
    # not handed a partner handle to look things up with.
    order_id = body.pop("reap_order_id", None)
    if state == "completed" and order_id:
        body["order_reference"] = order_id

    body["poll_after_seconds"] = svc.POLL_INTERVALS.get(state)
    return body


async def _owner_view(
    *, purchase_id: str, agent_id: str, agent_user_ref_hash: str
) -> Optional[Dict[str, Any]]:
    # Private columns are needed only to scope the legacy enrollment deadline lookup;
    # public_purchase_view must redact them before any response is built.
    row = await ledger.get_purchase_for_owner(
        purchase_id, agent_id, agent_user_ref_hash, include_private=True
    )
    if not row:
        return None
    return await _owner_public_body(row)


async def _owner_public_body(row):
    """Owner-scoped private row to a redacted response with an original-attempt deadline."""
    row = dict(row)
    if row.get("state") == "needs_enrollment":
        enrollment = None
        enrollment_id = str(row.get("enrollment_id") or "").strip()
        if enrollment_id:
            try:
                enrollment = await ledger.get_enrollment_internal(enrollment_id)
            except Exception:
                enrollment = None
        if (enrollment and enrollment.get("status") == "pending"
                and str(enrollment.get("id")) == enrollment_id
                and enrollment.get("buyer_ref") == row.get("buyer_ref")
                and enrollment.get("hosted_url") == row.get("hosted_url")
                and not enrollment.get("hosted_url_expiry_invalid")):
            deadline = svc._effective_expiry(enrollment.get("hosted_url_expires_at"), enrollment.get("created_at"))
            purchase_deadline = svc._parse_ts(row.get("hosted_url_expires_at"))
            if deadline is not None and purchase_deadline is not None:
                deadline = min(deadline, purchase_deadline)
        else:
            deadline = None
        if deadline is None:
            row.pop("hosted_url", None)
            row.pop("hosted_url_expires_at", None)
        else:
            row["hosted_url_expires_at"] = deadline
    body = _public_body(ledger.public_purchase_view(row))
    body["checkout_dispatch_state"] = continuation.dispatch_state(row)
    body["contact_reentry_required"] = (
        row.get("state") in {"resolving", "needs_enrollment", "quoting"}
        and continuation.contact_required(row)
    )
    return body


async def _record_purchase_parent(purchase_id: str) -> None:
    """Payment orchestration P0: the rail-neutral parent row (db/agent_purchase_ledger.py).

    AFTER this route's own transaction has committed, never inside it, and it never raises: the
    parent is derived entirely from the committed purchase row, so a parent that is not written
    here is written by the unified read's heal or by the backfill, and a fault in this table can
    never fail, delay or roll back a purchase. Dark unless AGENT_PURCHASE_LEDGER_ENABLED is on.
    Idempotent, so an idempotent replay of the create is harmless here too.
    """
    if not agent_purchase_ledger.is_enabled():
        return
    try:
        await agent_purchase_ledger.ensure_reap_parent(purchase_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "reap_agentic: agent_purchases parent not written purchase=%s error_type=%s",
            purchase_id,
            type(exc).__name__,
        )


def _not_found() -> JSONResponse:
    """WRONG OWNER AND NO SUCH PURCHASE ARE THE SAME ANSWER. 404, never 403: a 403 confirms the
    id exists, which turns this endpoint into an oracle for guessing other buyers' purchase ids.
    A new response object each time — `JSONResponse` is not safe to share across requests."""
    return JSONResponse(status_code=404, content={"error": "purchase_not_found"})


# ── routes ───────────────────────────────────────────────────────────────────────────────────


#: WHY THESE TWO HANDLERS TAKE A RAW `Request`.
#:
#: A parameter in the signature — `payload: Dict[str, Any] = Body(...)`, `limit: int = Query(le=100)`
#: — is validated by FastAPI BEFORE the handler body runs, and its refusal is a 400 naming the
#: field. So while the rail was dark, `POST` with a body of `[1, 2]`, or with the wrong
#: content-type, or `GET ?limit=500`, all answered 400 with a field message while every valid
#: request answered 404. That is a working probe for a feature that is supposed to be absent, and
#: it survives every test that only ever sends well-formed requests.
#:
#: Taking the raw request moves ALL parsing inside the handler, after the dial. Every shape of
#: input now gets the same 404 from a dark rail, byte for byte.
#:
#: WHAT IS STILL VISIBLE, and cannot be fixed here: `/openapi.json` lists these paths whatever the
#: dial says, because the routes are mounted at import and the schema is built from the router.
#: That is a deliberate trade — see the note on the mount in `main.py` — and it is documented on
#: the contract page rather than papered over.


@router.post("/purchases/prepare")
async def prepare_reap_purchase_selection(
    request: Request,
    context: AgentContext = Depends(get_agent_context),
    agent_user: Optional[AgentUserContext] = Depends(get_agent_user_context),
):
    """Authenticated read-only selection witness, before an original create body exists.

    A numeric selector is not a SKU key. Resolve it to exactly one existing live SKU,
    then use the create lane's proof, seller and own-offer reader with that exact key.
    No buyer, consent, idempotency, click, enrollment or purchase row is touched; no
    network fetch or provider request is made. Recovery never calls this endpoint.
    """
    try:
        _require_rail()
        if not svc.is_create_enabled() or not svc.is_cart_link_enabled():
            raise svc.PurchaseRefused("not_available_on_this_rail")
        _require_agent_user(agent_user)
        try:
            payload = await request.json()
            req = PreparePurchaseSelectionRequest.model_validate(payload)
        except (ValidationError, ValueError, TypeError):
            raise svc.PurchaseRefused("invalid_request") from None
        merchant_host = _identifier(req.merchant_domain, "merchant_domain", max_chars=255).lower()
        merchant_domain = _merchant_domain_key(merchant_host)
        product_key = _identifier(req.product_key, "product_key", max_chars=1024)
        variant_id = _identifier(req.variant_id, "variant_id", max_chars=20)
        if not re.fullmatch(r"[1-9][0-9]{0,19}", variant_id):
            raise svc.PurchaseRefused("invalid_request")
        market = purchasability.normalize_market(req.market_country)
        if market is None:
            raise svc.PurchaseRefused("invalid_request")
        agent_id = str(context.agent_id)
        svc.enforce_pilot_scope(
            agent_id=agent_id, merchant_domain=merchant_domain, market_country=market,
            product_key=product_key, quantity=req.quantity, resolved=False,
        )
        if svc.is_cart_link_enrichment_enabled():
            # PostgreSQL refuses CREATE TABLE IF NOT EXISTS in a read-only transaction even when
            # the table exists, so the proof table's self-heal runs HERE, before the snapshot. Its
            # answer is not trusted: `fetch_proof` inside still decides, never runs DDL there, and
            # a missing table or row is a missing proof (refused).
            await enrichment_proofs.ensure_table()
        async with database.transaction():
            if IS_POSTGRES:
                # One coherent catalog/proof snapshot, enforced read-only by PostgreSQL.
                await database.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            if purchasability.is_enforcement_enabled() and not await purchasability.is_purchasable(
                merchant_domain, market
            ):
                raise svc.PurchaseRefused("merchant_not_purchasable")
            await _refuse_if_merchant_disabled(merchant_domain=merchant_domain, market_country=market)
            if not await tierb_eligibility.is_cart_link_eligible(merchant_host, market):
                raise svc.PurchaseRefused("merchant_not_eligible")
            product = await database.fetch_one(
                _CART_PRODUCT_SQL, {"product_key": product_key, "merchant_domain": merchant_host}
            )
            if product is None:
                raise svc.PurchaseRefused("row_not_found")
            product = dict(product)
            if str(product.get("platform") or "").lower() not in {"shopify", "external_seed"}:
                raise svc.PurchaseRefused("row_not_shopify")
            skus = [dict(row) for row in await database.fetch_all(
                _CART_PRODUCT_SKUS_SQL.replace(
                    "s.sku_key, s.source_variant_id,", "s.sku_key, s.source_variant_id, s.merchant_id, s.platform,"
                ), {"product_key": product_key}
            )]
            if len(skus) > _CART_MAX_SKUS:
                raise svc.PurchaseRefused("row_variant_ambiguous")
            matches = [sku for sku in skus
                       if not _is_placeholder_sku(sku, product_key)
                       and _cart_numeric_variant(sku.get("source_variant_id"), product_key) == variant_id]
            if len(matches) != 1:
                raise svc.PurchaseRefused(
                    "row_variant_ambiguous" if matches else "row_variant_unverified"
                )
            variant_key = str(matches[0]["sku_key"])
            if (matches[0]["merchant_id"] != product["merchant_id"]
                    or matches[0]["platform"] != product["platform"]):
                raise svc.PurchaseRefused("row_variant_unverified")
            facts, seller, resolved_id, _seed_kind = await _load_cart_link_item(
                merchant_domain=merchant_host, product_key=product_key,
                variant_key=variant_key, market_country=market,
            )
            if resolved_id != variant_id:
                raise svc.PurchaseRefused("row_variant_unverified")
            if str(matches[0].get("currency") or "").strip().upper() != facts["currency"]:
                raise svc.PurchaseRefused("row_currency_mismatch")
            # The enabled, source-bound enrichment loader already requires every live
            # own-listing offer to agree with this SKU's fresh storefront proof in this
            # read-only snapshot. Its listing owner is distinct from the product seller
            # of record; a second query under that seller would incorrectly see no offers.
            # Mirror/Shopify rows retain their independent seller-owned agreement check.
            enrichment_priced = (
                svc.is_cart_link_enrichment_enabled()
                and product.get("source_system") == ENRICHMENT_SOURCE_SYSTEM
            )
            if not enrichment_priced:
                offers = [dict(row) for row in await database.fetch_all(
                    _CART_ALL_OFFERS_SQL.replace("LIMIT 20", "LIMIT 21"),
                    {"product_key": product_key, "sku_key": variant_key, "merchant_id": seller,
                     "market_currency": _MARKET_CURRENCY.get(market)},
                )]
                prices = {ledger.amount_minor_or_none(row.get("price"), facts["currency"]) for row in offers}
                if len(offers) > 20 or prices != {facts["our_price_minor"]}:
                    raise svc.PurchaseRefused("row_price_ambiguous")
            if not context.can_access_merchant(seller):
                raise svc.PurchaseRefused("not_available_on_this_rail")
            _require_rail()
            if not svc.is_create_enabled() or not svc.is_cart_link_enabled():
                raise svc.PurchaseRefused("not_available_on_this_rail")
            svc.enforce_pilot_scope(
                agent_id=agent_id, merchant_domain=facts["shop_domain"], market_country=market,
                product_key=product_key, quantity=req.quantity, variant_key="shopify:" + resolved_id,
                currency=facts["currency"], total_minor=facts["our_price_minor"] * req.quantity,
            )
            selection = {
                "product_key": product_key, "variant_id": resolved_id, "variant_key": variant_key,
                "merchant_domain": merchant_host, "market": market, "currency": facts["currency"],
                "unit_price_minor": facts["our_price_minor"], "quantity": req.quantity,
                "item_source": "cart_link",
            }
        return JSONResponse(status_code=200, content={"selection": selection})
    except svc.PurchaseRefused as exc:
        return _refused(exc)


@router.post("/purchases")
async def start_reap_purchase(
    request: Request,
    context: AgentContext = Depends(get_agent_context),
    agent_user: Optional[AgentUserContext] = Depends(get_agent_user_context),
):
    """Open a purchase and answer at once. MAKES NO PARTNER CALL — the poller does that."""
    try:
        _require_rail()
        if not svc.is_create_enabled():
            raise svc.PurchaseRefused("create_disabled")
        agent_user_ref = _require_agent_user(agent_user)

        try:
            payload = await request.json()
        except Exception:  # noqa: BLE001
            # A body that is not JSON at all, or an empty one. The parser's message can quote the
            # body, so it is not carried.
            raise svc.PurchaseRefused("invalid_request", "the request body is not JSON")
        if not isinstance(payload, dict):
            raise svc.PurchaseRefused("invalid_request", "the request body must be an object")

        try:
            req = StartPurchaseRequest.model_validate(payload)
        except ValidationError:
            # The pydantic error names fields AND the values that failed, and one of those fields
            # is the buyer's address. Never forwarded, never logged.
            raise svc.PurchaseRefused("invalid_request", "the request body did not validate")

        # The cart-link lane has two further dark gates. Decide them before consent or any
        # merchant/catalog read so a disabled lane is the same 404 unavailability as the base rail.
        if req.item_source == "cart_link" and not svc.is_cart_link_enabled():
            raise svc.PurchaseRefused("not_available_on_this_rail")

        # ── CONSENT, AND WHERE IT SITS IN THE ORDER ──────────────────────────────────────────
        #
        # AFTER THE DIAL, WHICH IS NOT A STYLE CHOICE. `_require_rail()` is the first statement
        # of this handler and everything about a dark rail depends on it staying first: a
        # `consent_required` answered before the dial is a 400 where every well-formed request
        # gets a 404, which is a working probe for a feature that is supposed to be absent. That
        # is the exact defect the note above these handlers records about `Body(...)` and
        # `Query(...)`, and moving this check one line up would reintroduce it. The mutant table
        # for this PR kills it in that direction specifically.
        #
        # BEFORE ELIGIBILITY, THE CATALOG READ AND EVERY WRITE, which is the other half. A buyer
        # who has not consented must not have an identity minted for them, must not have a
        # purchase opened, and must not be able to learn — by the shape of the refusal — which
        # merchants we have enabled or what is in our catalogue.
        consent_version = _consent_version(req.buyer.consent_version)
        # After consent, before anything is read or hashed: a code that cannot be sent is a
        # malformed request, and it is part of what the idempotency key is a key FOR.
        offer_code = _offer_code(req.offer_code)

        # EVERY ROUTE-OWNED IDENTIFIER THROUGH ONE CHECKPOINT, before any of them can reach a
        # bind. `.lower()` after the check rather than before: the check is about what the string
        # CONTAINS, and lowercasing cannot add or remove an unprintable character.
        merchant_host = _identifier(
            req.merchant_domain, "merchant_domain", max_chars=255
        ).lower()
        # TWO VALUES FROM ONE FIELD, and which one each consumer gets is the whole change.
        #
        # `merchant_domain` is the canonical MERCHANT: what the variant lane's eligibility and
        # catalog reads compare against (each folding its stored column by the same rule), and
        # what the idempotency hash carries (`www.` and apex are one merchant, so one purchase).
        #
        # `merchant_host` is the storefront host as the door observed it, lowercased. It goes to
        # the purchase row, whose readers fold it (the resolver's `merchant_domain_matches`
        # against Reap's own spelling of the host, and the attribution close, which keys the
        # merchant canonically); to the Tier B verdict, whose reader folds
        # it too; and to the CART-LINK lane's catalog read and URL, whose host is compared
        # byte-for-byte downstream. The purchasability lookup is the exception — see there.
        #
        # Validated here, for BOTH lanes, so a host that is not a bare host name is refused
        # before any read.
        merchant_domain = _merchant_domain_key(merchant_host)
        product_key = _identifier(req.product_key, "product_key", max_chars=1024)
        variant_key = (
            _identifier(req.variant_key, "variant_key", max_chars=1024)
            if str(req.variant_key or "").strip()
            else None
        )
        # THE ONE MARKET RULE (`utils.market_code.iso2_market`, bound as the fact store's
        # `normalize_market`): the same function the purchasability fact is keyed with, so the
        # rail can never ask the gate about a market the fact store would have spelled differently.
        # Unknown is refused here, never defaulted.
        market_country = purchasability.normalize_market(req.buyer.shipping_address.country)
        if market_country is None:
            raise svc.PurchaseRefused("invalid_address", "country must be two letters")

        agent_id = str(context.agent_id)
        agent_user_ref_hash = hash_agent_user_ref(agent_user_ref)
        if not agent_user_ref_hash:
            raise svc.PurchaseRefused("agent_user_required")

        # REQUIRED, and a blank one is as missing as an absent one: refused here, before the
        # replay lookup and before any write (see `StartPurchaseRequest.idempotency_key`).
        idempotency_key = _identifier(req.idempotency_key, "idempotency_key", max_chars=128)
        # THE RETURN URL AND THE ADDRESS ARE RESOLVED HERE, BEFORE THE REPLAY LOOKUP, because both
        # are part of what the idempotency key is a key FOR. `start_purchase` validates the URL;
        # this line only decides which one it validates.
        shipping_address = _buyer_address_for_client(req.buyer)
        buyer_email = str(req.buyer.email or "").strip()
        return_url = str(req.return_url or "").strip() or _default_return_url()

        request_hash = _request_hash(
            merchant_domain=merchant_domain,
            product_key=product_key,
            variant_key=variant_key,
            quantity=int(req.quantity),
            email=buyer_email,
            shipping_address=shipping_address,
            return_url=return_url,
            item_source=req.item_source,
            offer_code=offer_code,
            expected_unit_price_minor=req.expected_unit_price_minor,
            expected_currency=req.expected_currency,
        )

        # A body with NEITHER money field (half a pair never validated) is not a new attempt this
        # route will open. It may still be the retry of an attempt keyed before the pair was
        # required -- a door that sent the key and no money -- and refusing that retry with a 400
        # would tell the door "not created" about a purchase that exists. So it gets the
        # read-only replay below under the money-less fingerprint it was keyed with, and a 400
        # only when that finds nothing. The lookup is a read; nothing is written on this path.
        legacy_retry = req.expected_unit_price_minor is None

        # Raises `idempotency_conflict` when this key was used for a different request.
        replayed = await _replayed_purchase_id(
            agent_id=agent_id,
            agent_user_ref_hash=agent_user_ref_hash,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )
        if replayed:
            # THE SAME ID, AND THE PURCHASE'S REAL STATE. Not a hardcoded "resolving": by the
            # time a caller retries, the row may have moved, and answering with a state it is
            # no longer in would be a lie in the one field the caller polls on.
            view = await _owner_view(
                purchase_id=replayed,
                agent_id=agent_id,
                agent_user_ref_hash=agent_user_ref_hash,
            )
            if view:
                # THE CONSENT IS RECORDED ON A REPLAY TOO, and this is the whole reason
                # `_linked_buyer_id` exists as a separate read.
                #
                # The contract page, the runbook and migration 227's header all say the tag
                # is rewritten on EVERY purchase and is always the latest version the buyer
                # accepted. Returning here without writing it made that false for exactly the
                # requests a door retries — which is where a consent version most plausibly
                # changes mid-flight. The prose was right and the code was wrong; this is the
                # code catching up.
                #
                # A READ, NOT `_buyer_id_for`. A replay implies the buyer already exists, so
                # the lookup finds them; using the minting version here would hand the replay
                # path the power to CREATE an identity before eligibility has been checked,
                # and a caller could then mint buyer rows by probing merchants we never
                # enabled. Nothing is written when there is no link — there is nothing to
                # record a consent against.
                replay_buyer_id = await _linked_buyer_id(
                    agent_id=agent_id, agent_user_ref_hash=agent_user_ref_hash
                )
                # Not on a legacy (money-less) retry: that path is read-only end to end, and
                # the attempt's consent was recorded when it was opened.
                if replay_buyer_id and not legacy_retry:
                    await _record_consent(replay_buyer_id, consent_version)
                replay_body: Dict[str, Any] = {
                    "purchase_id": replayed,
                    "status": view.get("state"),
                    "poll_after_seconds": view.get("poll_after_seconds"),
                    "checkout_dispatch_state": view["checkout_dispatch_state"],
                    "contact_reentry_required": view["contact_reentry_required"],
                }
                if req.item_source == "cart_link":
                    # The same additive field the cart-link create answered with. The key's
                    # request hash covers `item_source`, so a replay is always the same lane.
                    replay_body["variant_title"] = view.get("variant_title")
                return JSONResponse(status_code=202, content=replay_body)
            # An immutable key without an owner-visible row cannot authorize a new attempt.
            return _not_found()
        if legacy_retry:
            # No attempt under this key and fingerprint: a NEW attempt, and a new attempt binds
            # the money the buyer was shown. Refused before any write.
            raise svc.PurchaseRefused(
                "invalid_request", "expected_unit_price_minor and expected_currency are required"
            )

        svc.enforce_pilot_scope(
            agent_id=agent_id, merchant_domain=merchant_domain,
            market_country=str(shipping_address.get("country") or ""),
            product_key=product_key, quantity=req.quantity, resolved=False,
        )

        # PURCHASABILITY BEFORE EITHER LANE'S ELIGIBILITY, because it is the broader refusal:
        # both allowlists say a merchant is PERMITTED, and neither says its checkout can be PAID.
        # flowerbeauty.com was allowlisted, priced a UCP cart, advertised `dev.shopify.card` (a
        # platform constant every Shopify store repeats) and rendered a checkout whose only
        # payment method was PayPal, at a price we did not hold.
        #
        # DARK BY DEFAULT. With MERCHANT_PURCHASABILITY_ENFORCE off this is not consulted at all
        # and the rail behaves exactly as it did; with it on, a merchant with no fresh positive
        # fact FROM THE BUYER VANTAGE is refused. `is_purchasable` fails CLOSED on a database
        # error, which is the right direction for a payment gate even though it is the wrong one
        # for a liveness check.
        #
        # HANDED THE CANONICAL MERCHANT, because that is what the WRITER hands its own fold. The
        # sweep builds its population with `normalize_domain(<allowlist row's domain>)` and then
        # passes that already-folded value to `record_check`, which folds it again; `is_purchasable`
        # folds its argument the same way. So the fact is keyed fold(fold(spelling)), and the read
        # matches it exactly when it is handed fold(spelling) — `merchant_domain`. For every host
        # that does not begin `www.www.` the second fold is a no-op and the distinction vanishes;
        # for the one that does, handing the observed host here would read a key no sweep writes.
        if purchasability.is_enforcement_enabled():
            if not await purchasability.is_purchasable(merchant_domain, market_country):
                raise svc.PurchaseRefused(
                    "merchant_not_purchasable",
                    "no fresh positive purchasability fact for this domain and market",
                )

        # ELIGIBILITY BEFORE THE CATALOG READ on either lane. The daily Tier B verdict is
        # distinct from the variant rail's operator allowlist; it expires after 48 hours.
        row = None
        cart_facts = None
        cart_variant = None
        cart_seller = None
        cart_seed_kind = None
        if req.item_source == "cart_link":
            # THE OBSERVED HOST, NOT THE CANONICAL MERCHANT — see `merchant_host` above. The Tier B
            # verdict is keyed canonically by its own reader, so this is the same lookup either
            # way; the catalog read, the storefront evidence and the permalink are not.
            # AN OPERATOR'S "OFF" FIRST: a disabled variant-lane row for this merchant refuses
            # this lane too, whatever the Tier B verdict says.
            await _refuse_if_merchant_disabled(
                merchant_domain=merchant_domain, market_country=market_country
            )
            if not await tierb_eligibility.is_cart_link_eligible(
                merchant_host, market_country
            ):
                raise svc.PurchaseRefused("merchant_not_eligible")
            cart_facts, cart_seller, cart_variant, cart_seed_kind = await _load_cart_link_item(
                merchant_domain=merchant_host,
                product_key=product_key,
                variant_key=variant_key,
                market_country=market_country,
            )
        else:
            # A refusal here writes nothing, the key included: every attempt that reaches here
            # carries bound money that has not passed admission yet. (A `refused:` key written before the pair was
            # required is still honoured by `_replayed_purchase_id`.)
            eligible = await _eligibility(
                merchant_domain=merchant_domain,
                market_country=market_country,
                product_key=product_key,
                variant_key=variant_key,
            )
            row = await _load_catalog_row(
                merchant_domain=merchant_domain,
                storefront_host=merchant_host,
                product_key=product_key,
                variant_key=variant_key,
                market_country=eligible.market_country,
                accept_variant_labels=eligible.accept_variant_labels,
                also_accept_domains=eligible.also_accept_domains,
            )

        facts = cart_facts if cart_facts is not None else row
        # The money the buyer was shown, against the money this offer has now. Every attempt
        # that reaches here carries the pair (`legacy_retry` above), so this always runs.
        actual_minor = facts["our_price_minor"] if isinstance(facts, dict) else facts.our_price_minor
        actual_currency = facts["currency"] if isinstance(facts, dict) else facts.currency
        if type(actual_minor) is not int or actual_minor != req.expected_unit_price_minor or actual_currency != req.expected_currency:
            raise svc.PurchaseRefused("price_changed", "the selected offer money changed")
        svc.enforce_pilot_scope(
            agent_id=agent_id, merchant_domain=(facts["shop_domain"] if isinstance(facts, dict) else facts.merchant_domain),
            market_country=(facts["market_country"] if isinstance(facts, dict) else facts.market_country),
            product_key=(facts.get("product_key") if isinstance(facts, dict) else facts.product_key),
            quantity=req.quantity,
            variant_key=("shopify:" + str(cart_variant) if cart_facts is not None else row.variant_key),
            currency=(facts["currency"] if isinstance(facts, dict) else facts.currency),
            total_minor=(facts["our_price_minor"] if isinstance(facts, dict) else facts.our_price_minor) * req.quantity,
        )

        buyer_id = await _buyer_id_for(
            agent_id=agent_id, agent_user_ref_hash=agent_user_ref_hash
        )
        buyer_ref = await _reap_buyer_ref(buyer_id, consent_version=consent_version)

        click_id = new_click_id()
        cart_link_item = None
        if cart_facts is not None:
            cart_url = build_shopify_cart_permalink(
                shop_domain=merchant_host, variant_id=cart_variant, click_id=click_id,
                quantity=int(req.quantity), country=market_country,
            )
            if cart_url is None:
                raise svc.PurchaseRefused("row_variant_unverified")
            await _record_cart_link_click(
                click_id=click_id, cart_url=cart_url, shop_domain=merchant_host,
                seller_ref=cart_seller, product_key=product_key, variant_key=variant_key,
                agent_id=agent_id, seed_kind=cart_seed_kind,
            )
            cart_link_item = svc.CartLinkItem(cart_url=cart_url, **cart_facts)

        winning_view = None
        try:
            # start_purchase makes no partner call. This short durable unit hides
            # the purchase from other connections until its immutable key lands.
            async with database.transaction():
                if not IS_POSTGRES:
                    # Reserve SQLite's writer before cart consent/catalog reads.
                    # This changes no row and prevents two deferred read snapshots
                    # from racing to upgrade their transactions into writers.
                    await database.execute(
                        "UPDATE reap_agentic_purchase_keys SET purchase_id=purchase_id WHERE 0=1"
                    )
                purchase_id = await svc.start_purchase(
                    agent_id=agent_id,
                    agent_user_ref_hash=agent_user_ref_hash,
                    buyer_ref=buyer_ref,
                    row=row,
                    cart_link=cart_link_item,
                    consent_version=consent_version,
                    # THE RETURN URL IS NOT VALIDATED BY THIS ROUTE. `start_purchase` runs
                    # `rc.validate_return_url` on whatever it is handed, builds both stage variants from
                    # it, and raises `PurchaseRefused("invalid_return_url")` — the same reason code this
                    # route would have used. A copy of that check here would be a guard that can never be
                    # the one that fires, and an unreachable guard reads as protection that does not
                    # exist. One validator, and it is the one that owns the URL — including for the
                    # default, so an operator who points `REAP_AGENTIC_RETURN_URL` at a host that is not
                    # ours gets a refusal rather than an open redirector with a payment page in front.
                    buyer=svc.BuyerContact(email=buyer_email, shipping_address=shipping_address),
                    quantity=int(req.quantity),
                    # MINTED HERE, NEVER TAKEN FROM THE REQUEST. This is the only join between a Reap
                    # checkout and the session that started it, and a caller-supplied one could collide
                    # with another caller's.
                    click_id=click_id,
                    return_url=return_url,
                    offer_code=offer_code,
                )

                winner = await _claim_idempotency_key(
                    agent_id=agent_id, agent_user_ref_hash=agent_user_ref_hash,
                    idempotency_key=idempotency_key, purchase_id=purchase_id,
                    request_hash=request_hash,
                )
                if winner != purchase_id:
                    await _abandon_duplicate(purchase_id)
                    purchase_id = winner
                    winning_view = await _owner_view(
                        purchase_id=winner, agent_id=agent_id,
                        agent_user_ref_hash=agent_user_ref_hash,
                    )
                    if winning_view is None:
                        raise _PurchasePersistenceUnavailable()
        except svc.PurchaseRefused:
            raise
        except Exception as exc:
            logger.warning("reap_agentic: atomic create unavailable error_type=%s", type(exc).__name__)
            raise _PurchasePersistenceUnavailable() from None

        logger.info(
            "reap_agentic: route opened purchase=%s merchant=%s agent=%s",
            purchase_id,
            merchant_domain,
            agent_id,
        )
        # Read committed owner-scoped facts, including a concurrent winner's actual state.
        # A successful INSERT alone cannot prove absence of a later worker dispatch.
        if winning_view is None:
            try:
                winning_view = await _owner_view(
                    purchase_id=purchase_id, agent_id=agent_id,
                    agent_user_ref_hash=agent_user_ref_hash,
                )
            except Exception:
                raise _PurchasePersistenceUnavailable() from None
        if winning_view is None:
            raise _PurchasePersistenceUnavailable()
        await _record_purchase_parent(purchase_id)
        accepted: Dict[str, Any] = {
            "purchase_id": purchase_id,
            "status": winning_view["state"],
            "poll_after_seconds": winning_view.get("poll_after_seconds"),
            "checkout_dispatch_state": winning_view["checkout_dispatch_state"],
            "contact_reentry_required": winning_view["contact_reentry_required"],
        }
        if cart_link_item is not None:
            # ADDITIVE, CART-LINK LANE ONLY, display only: the variant this link buys, in the live
            # storefront's words ("07 BURGUNDY INK") -- the buyer never picks it on this lane.
            # The stored row's `variant_title`, as GET returns it; null when the proof has none.
            accepted["variant_title"] = winning_view.get("variant_title")
        return JSONResponse(status_code=202, content=accepted)
    except _PurchasePersistenceUnavailable:
        return JSONResponse(status_code=503, content={"error": "checkout_outcome_unknown"})
    except svc.PurchaseRefused as exc:
        return _refused(exc)


async def _validate_resume_selection(req, row, merchant, host, product, variant, market):
    """Fresh admission for the SAME immutable item and price; creates no buyer/click/attempt."""
    if purchasability.is_enforcement_enabled() and not await purchasability.is_purchasable(merchant, market):
        raise svc.PurchaseRefused("merchant_not_purchasable")
    if req.item_source == "cart_link":
        if not svc.is_cart_link_enabled():
            raise svc.PurchaseRefused("not_available_on_this_rail")
        await _refuse_if_merchant_disabled(merchant_domain=merchant, market_country=market)
        if not await tierb_eligibility.is_cart_link_eligible(host, market):
            raise svc.PurchaseRefused("merchant_not_eligible")
        facts, _, selected_variant, _ = await _load_cart_link_item(
            merchant_domain=host, product_key=product, variant_key=variant, market_country=market)
        selected_key = "shopify:" + str(selected_variant)
        price, currency = facts["our_price_minor"], facts["currency"]
        # The stored cart (including attribution) survives; only compare its immutable line.
        line = svc.cart_link_line(row.get("cart_url"))
        if line is None or str(line[0]) != str(selected_variant) or line[1] != req.quantity:
            raise svc.PurchaseRefused("resume_selection_changed")
    else:
        eligible = await _eligibility(merchant_domain=merchant, market_country=market,
                                      product_key=product, variant_key=variant)
        facts = await _load_catalog_row(merchant_domain=merchant, storefront_host=host,
            product_key=product, variant_key=variant, market_country=eligible.market_country,
            accept_variant_labels=eligible.accept_variant_labels, also_accept_domains=eligible.also_accept_domains)
        selected_key, price, currency = facts.variant_key, facts.our_price_minor, facts.currency
    if (selected_key != row.get("variant_key") or product != row.get("product_key")
            or _merchant_domain_key(row.get("merchant_domain")) != merchant
            or market != row.get("market_country") or req.quantity != row.get("quantity")
            or req.item_source != row.get("item_source", "reap_variant")):
        raise svc.PurchaseRefused("resume_selection_changed")
    if type(price) is not int or price != row.get("our_price_minor") or currency != row.get("currency"):
        raise svc.PurchaseRefused("price_changed")
    svc.enforce_pilot_scope(agent_id=row["agent_id"], merchant_domain=merchant,
        market_country=market, product_key=product, quantity=req.quantity,
        variant_key=selected_key, currency=currency, total_minor=price * req.quantity)


@router.post("/purchases/{purchase_id}/resume")
async def resume_reap_purchase(purchase_id: str, request: Request,
    context: AgentContext = Depends(get_agent_context),
    agent_user: Optional[AgentUserContext] = Depends(get_agent_user_context)):
    """Explicit contact re-entry for one owner/request/attempt; never a replacement/recovery write.

    The original canonical request (including the original contact) is required. Its key already
    maps to this purchase. Terminal and legacy/unknown dispatch rows cannot be rehydrated.
    Repeated accepted submissions return the same view without renewing the contact clock.
    """
    try:
        _require_rail()
        if not svc.is_create_enabled():
            raise svc.PurchaseRefused("create_disabled")
        owner = hash_agent_user_ref(_require_agent_user(agent_user))
        agent = str(context.agent_id)
        try:
            req = StartPurchaseRequest.model_validate(await request.json())
        except (ValueError, ValidationError):
            raise svc.PurchaseRefused("invalid_request") from None
        key = _identifier(req.idempotency_key, "idempotency_key", max_chars=128)
        host = _identifier(req.merchant_domain, "merchant_domain", max_chars=255).lower()
        merchant = _merchant_domain_key(host)
        product = _identifier(req.product_key, "product_key", max_chars=1024)
        variant = _identifier(req.variant_key, "variant_key", max_chars=1024) if str(req.variant_key or "").strip() else None
        consent = _consent_version(req.buyer.consent_version)
        offer = _offer_code(req.offer_code)
        address = _buyer_address_for_client(req.buyer)
        email = str(req.buyer.email or "").strip()
        request_hash = _request_hash(merchant_domain=merchant, product_key=product, variant_key=variant,
            quantity=int(req.quantity), email=email, shipping_address=address,
            return_url=str(req.return_url or "").strip() or _default_return_url(), item_source=req.item_source,
            offer_code=offer, expected_unit_price_minor=req.expected_unit_price_minor, expected_currency=req.expected_currency)
        bound = await _replayed_purchase_id(agent_id=agent, agent_user_ref_hash=owner,
                                           idempotency_key=key, request_hash=request_hash)
        if bound != purchase_id:
            return _not_found()
        row = await ledger.get_purchase_for_owner(purchase_id, agent, owner, include_private=True)
        if row is None:
            return _not_found()
        if row["state"] in ledger.TERMINAL_STATES:
            raise svc.PurchaseRefused("terminal_purchase_not_resumable")
        if continuation.dispatch_state(row) != "not_dispatched":
            raise svc.PurchaseRefused("checkout_dispatch_unresolved")
        if not continuation.contact_required(row):
            if row.get("contact_revision", 0) > 0:
                return await _owner_public_body(row)
            raise svc.PurchaseRefused("contact_reentry_not_required")
        if row["state"] not in {"resolving", "needs_enrollment", "quoting"}:
            raise svc.PurchaseRefused("contact_reentry_not_required")
        if consent != row.get("consent_version"):
            raise svc.PurchaseRefused("consent_required")
        buyer_id = await _linked_buyer_id(agent_id=agent, agent_user_ref_hash=owner)
        linked = await database.fetch_one("SELECT reap_buyer_ref FROM reap_agentic_buyer_refs WHERE buyer_id=:buyer", {"buyer":buyer_id}) if buyer_id else None
        if linked is None or linked["reap_buyer_ref"] != row.get("buyer_ref"):
            raise svc.PurchaseRefused("buyer_unlinked")
        market = purchasability.normalize_market(req.buyer.shipping_address.country)
        await _validate_resume_selection(req, row, merchant, host, product, variant, market)
        email, address = svc._validated_buyer(svc.BuyerContact(email=email, shipping_address=address))
        restored = await continuation.restore_contact(row, agent_id=agent, owner_hash=owner,
            request_key=key, request_hash=request_hash, email=email, address=address, offer_code=offer)
        if restored is None:
            fresh = await ledger.get_purchase_for_owner(purchase_id, agent, owner, include_private=True)
            if (fresh and fresh["state"] not in ledger.TERMINAL_STATES
                    and fresh.get("contact_revision", 0) > row.get("contact_revision", 0)
                    and not continuation.contact_required(fresh)):
                return await _owner_public_body(fresh)
            raise svc.PurchaseRefused("resume_raced")
        return await _owner_public_body(restored)
    except _PurchasePersistenceUnavailable:
        return JSONResponse(status_code=503, content={"error": "checkout_outcome_unknown"})
    except svc.PurchaseRefused as exc:
        return _refused(exc)


@router.post("/purchases/recover")
async def recover_reap_purchase(
    request: Request,
    context: AgentContext = Depends(get_agent_context),
    agent_user: Optional[AgentUserContext] = Depends(get_agent_user_context),
):
    """Read-only exact-attempt lookup, available while create is disabled.

    Takes the original create body so the existing canonical fingerprint can be checked.
    Reads only: no merchant/catalog lookup, identity/consent write, provider call or key refresh.
    Missing/uncertain mapping must never be interpreted as permission to create another attempt.
    """
    try:
        agent_user_ref = _require_agent_user(agent_user)
        owner_hash = hash_agent_user_ref(agent_user_ref)
        if not owner_hash:
            raise svc.PurchaseRefused("agent_user_required")
        try:
            payload = await request.json()
            req = RecoverPurchaseRequest.model_validate(payload)
        except (ValueError, ValidationError):
            raise svc.PurchaseRefused("invalid_request", "the recovery body did not validate")
        key = _identifier(req.idempotency_key, "idempotency_key", max_chars=128)
        merchant = _merchant_domain_key(_identifier(req.merchant_domain, "merchant_domain", max_chars=255).lower())
        product = _identifier(req.product_key, "product_key", max_chars=1024)
        variant = _identifier(req.variant_key, "variant_key", max_chars=1024) if str(req.variant_key or "").strip() else None
        _consent_version(req.buyer.consent_version)  # Validation only; preserve stored consent.
        request_hash = _request_hash(
            merchant_domain=merchant, product_key=product, variant_key=variant,
            quantity=int(req.quantity), email=str(req.buyer.email or "").strip(),
            shipping_address=_buyer_address_for_client(req.buyer),
            return_url=str(req.return_url or "").strip() or _default_return_url(),
            item_source=req.item_source, offer_code=_offer_code(req.offer_code),
            expected_unit_price_minor=req.expected_unit_price_minor,
            expected_currency=req.expected_currency,
        )
        try:
            from services.reap_unopened_attempt import retired_receipt
            receipt = await retired_receipt(agent_id=str(context.agent_id), owner_hash=owner_hash,
                                            key=key, request_hash=request_hash)
        except Exception:
            raise _PurchasePersistenceUnavailable() from None
        if receipt:
            return receipt
        purchase_id = await _replayed_purchase_id(
            agent_id=str(context.agent_id), agent_user_ref_hash=owner_hash,
            idempotency_key=key, request_hash=request_hash,
        )
        if purchase_id:
            view = await _owner_view(purchase_id=purchase_id, agent_id=str(context.agent_id), agent_user_ref_hash=owner_hash)
            if view:
                return view
        return _not_found()
    except _PurchasePersistenceUnavailable:
        return JSONResponse(status_code=503, content={"error": "checkout_outcome_unknown"})
    except svc.PurchaseRefused as exc:
        # Refusal tombstones never identify an opened purchase; keep recovery a lookup.
        if exc.reason in _TOMBSTONED_REFUSALS:
            return _not_found()
        return _refused(exc)


@router.get("/purchases/{purchase_id}")
async def get_reap_purchase(
    purchase_id: str,
    context: AgentContext = Depends(get_agent_context),
    agent_user: Optional[AgentUserContext] = Depends(get_agent_user_context),
):
    try:
        # Stored owner-scoped status reads survive create disarming/credential outages.
        # This handler makes no provider call and retains both identity dependencies.
        agent_user_ref = _require_agent_user(agent_user)
        agent_user_ref_hash = hash_agent_user_ref(agent_user_ref)
        if not agent_user_ref_hash:
            raise svc.PurchaseRefused("agent_user_required")
    except svc.PurchaseRefused as exc:
        return _refused(exc)

    # BOTH CONJUNCTS, IN THE LEDGER'S SQL. Not re-implemented here and not re-checked afterwards:
    # `get_purchase_for_owner` puts `agent_id` AND `agent_user_ref_hash` in the WHERE clause, so
    # "not yours" and "does not exist" are one answer produced by one statement, and there is no
    # filter in this module for a refactor to drop.
    view = await _owner_view(
        purchase_id=str(purchase_id),
        agent_id=str(context.agent_id),
        agent_user_ref_hash=agent_user_ref_hash,
    )
    if not view:
        return _not_found()
    return view


#: The list is capped whatever the caller asks for. The ledger clamps again at 500; this is the
#: tighter of the two, because a history page is a page.
_LIST_MAX = 100


def _limit_from(request: Request) -> int:
    """`?limit=` as an int in 1..`_LIST_MAX`, or `invalid_request`.

    Hand-parsed because the dial has to be checked first — see the note above the handlers. An
    absent parameter is the default; a present one that is not a number, or is out of range, is a
    refusal rather than a silent clamp, because a caller that asked for 500 and got 20 has been
    given a page it does not know is partial.
    """
    raw = request.query_params.get("limit")
    if raw is None or str(raw).strip() == "":
        return 20
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        raise svc.PurchaseRefused("invalid_request", "limit must be an integer")
    if not (1 <= value <= _LIST_MAX):
        raise svc.PurchaseRefused("invalid_request", f"limit must be 1..{_LIST_MAX}")
    return value


@router.get("/purchases")
async def list_reap_purchases(
    request: Request,
    context: AgentContext = Depends(get_agent_context),
    agent_user: Optional[AgentUserContext] = Depends(get_agent_user_context),
):
    try:
        _require_rail()
        agent_user_ref = _require_agent_user(agent_user)
        agent_user_ref_hash = hash_agent_user_ref(agent_user_ref)
        if not agent_user_ref_hash:
            raise svc.PurchaseRefused("agent_user_required")
        # Parsed HERE, not in the signature: a `Query(le=...)` refusal is a 400 that fires before
        # the handler and told a prober the route exists while the rail was dark.
        limit = _limit_from(request)
    except svc.PurchaseRefused as exc:
        return _refused(exc)

    # THE SAME CONJUNCT AS THE SINGLE READ, and this is the read that matters more: a get that
    # leaks needs an id to be guessed first, and a list that leaks hands the whole set over.
    views = await ledger.list_purchases_for_owner(
        str(context.agent_id), agent_user_ref_hash, limit=min(int(limit), _LIST_MAX), include_private=True
    )
    return {
        "purchases": [await _owner_public_body(view) for view in views],
        "limit": min(int(limit), _LIST_MAX),
    }
