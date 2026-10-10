"""routes/agent_commerce_reap.py — the agent-facing door onto the agentic purchase rail, SQLite.

WHAT IS REAL HERE AND WHAT IS NOT, same split as tests/test_reap_agentic_purchase.py:

  REAL   the routes, the app, the ledger, the schema (built by the SELF-HEAL, because production
         skips db/migrations and the self-heal is therefore the schema that ships), the catalog
         tables (built from the repo's own `metadata`, so a column this route reads is the column
         the catalog actually has), `hash_agent_user_ref`, and every allowlist in
         `services.reap_agentic_client`.

  FAKE   the two auth dependencies, overridden per test so one file can be two agents and three
         end users; and nothing else. There is no fake for the Reap transport here because
         **these routes make no partner call at all** — `start_purchase` is documented as making
         none, and the autouse `_no_network` fixture is the control that proves it: an
         `httpx.AsyncClient` constructed anywhere under a request raises.

NEVER `TestClient`. It hangs against the asyncpg pool; `httpx.ASGITransport` is the house rule.

The Postgres arm is tests/test_agent_commerce_reap_routes_postgres.py, which is not a copy: it
re-runs the parts where the ENGINE is what is under test (migration-226 parity against the
self-heal, the numeric→text price cast, and the ownership conjunct over real SQL).

EVERY GUARD BELOW WAS MUTATED ONE AT A TIME AND CONFIRMED TO KILL AT LEAST ONE TEST; the table is
in the PR body.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.database import IS_POSTGRES, database, metadata  # noqa: E402
from db.schema_guard import ensure_required_schema_light  # noqa: E402
import db.reap_agentic_ledger as ledger  # noqa: E402
import routes.agent_commerce_reap as routes_reap  # noqa: E402
from services.shopify_variant_identity import ProvenCartVariant  # noqa: E402
import services.reap_agentic_purchase as svc  # noqa: E402
from db.buyer_vault import hash_agent_user_ref  # noqa: E402
from routes.agent_auth import get_agent_context  # noqa: E402
from routes.agent_user_auth import AgentUserContext, get_agent_user_context  # noqa: E402

#: IMPORTED AT MODULE LEVEL, NOT INSIDE A FIXTURE. `main` pulls in the billing routes, which
#: construct a Stripe client at import time, which constructs an `httpx.AsyncClient` — so an
#: import under the `_no_network` fixture trips a ban aimed at the routes rather than at the
#: import graph. Importing here also means the mounting test exercises the same app object every
#: request test does.
from main import app  # noqa: E402

#: Captured before `_no_network` can replace the attribute. The HARNESS uses this; application
#: code reaching for `httpx.AsyncClient` during a test still gets the forbidden one.
_REAL_ASYNC_CLIENT = httpx.AsyncClient

pytestmark = pytest.mark.skipif(
    IS_POSTGRES,
    reason=(
        "the SQLite arm of the agentic purchase routes; the Postgres arm is "
        "tests/test_agent_commerce_reap_routes_postgres.py"
    ),
)

BASE = "/agent/v2/commerce/reap"

AGENT = "agent_reap_routes"
OTHER_AGENT = "agent_reap_routes_other"
USER_REF = "user-ada"
OTHER_USER_REF = "user-grace"

BUYER_ID = "buy_ada"
OTHER_BUYER_ID = "buy_grace"

DOMAIN = "brand.example"
PRODUCT_KEY = "prod::m_brand::shopify::1001"
SKU_KEY = "sku::prod::m_brand::shopify::1001::v1"
SECOND_SKU_KEY = "sku::prod::m_brand::shopify::1001::v2"
MIRROR_SEED_ID = "seed_reap_cart_route_fixture"

EMAIL = "ada@example.test"
ADDRESS = {
    "firstName": "Ada",
    "lastName": "Lovelace",
    "phone": "+15550100",
    "addressLine1": "900 Brannan St",
    "city": "San Francisco",
    "country": "US",
    "postalCode": "94103",
}

#: Written out rather than derived from ADDRESS: a test that built its expectation from the same
#: dict it fed in cannot notice a field being dropped from both.
PII_STRINGS = ("ada@example.test", "900 Brannan St", "Lovelace", "+15550100")

CATALOG_TABLES = (
    "catalog_products", "catalog_skus", "catalog_offers", "catalog_merchants",
    "surface_click_events",
)
RAIL_TABLES = (
    "reap_agentic_purchase_keys",
    "reap_agentic_buyer_refs",
    "reap_agentic_eligibility",
    "reap_agentic_purchases",
    "reap_agentic_enrollments",
    "tierb_cart_link_eligibility",
)


def _error(resp) -> Optional[str]:
    """The refusal reason, out of the app-wide error envelope.

    `middleware/error_handler.ErrorHandlerMiddleware` re-wraps EVERY 4xx JSON response in the
    house shape (`status` / `error` / `metadata`) and preserves the route's own body verbatim
    under `detail`. So the route writes `{"error": "<reason>"}` and the caller reads it at
    `detail.error` — which is the field docs/reap_agentic_routes.md tells the gateway to read,
    and the one this helper asserts on. Reading `error.message` instead would also work today and
    would stop working the day the middleware gains a code hint for one of these statuses.
    """
    payload = resp.json()
    detail = payload.get("detail")
    if isinstance(detail, dict):
        return detail.get("error")
    error = payload.get("error")
    if isinstance(error, dict) and isinstance(error.get("details"), dict):
        return error["details"].get("error")
    return None


#: The consent tag every well-formed POST carries since WP4b. A version string, not prose — the
#: wording lives at the door.
CONSENT = "reap-agentic-v1"


def _buyer(**over) -> Dict[str, Any]:
    """The buyer block, with the consent tag already on it.

    A helper and not a literal at each call site because `consent_version` is REQUIRED: a test
    that overrides the buyer to say something about the ADDRESS should not accidentally also be
    asserting what happens without consent. The tests that are about consent build it explicitly
    and never come through here.
    """
    payload: Dict[str, Any] = {
        "email": EMAIL,
        "shipping_address": dict(ADDRESS),
        "consent_version": CONSENT,
    }
    payload.update(over)
    return payload


def _body(**over) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "merchant_domain": DOMAIN,
        "product_key": PRODUCT_KEY,
        "variant_key": SKU_KEY,
        "quantity": 1,
        "buyer": {
            "email": EMAIL,
            "shipping_address": dict(ADDRESS),
            "consent_version": CONSENT,
        },
        # Both REQUIRED on create: a fresh key per body (a test about replay passes its own), and
        # the money `_seed_catalog` prices the default row at.
        "idempotency_key": f"k-{uuid.uuid4().hex}",
        "expected_unit_price_minor": 4250,
        "expected_currency": "USD",
    }
    payload.update(over)
    return payload


def _legacy(body: Dict[str, Any]) -> Dict[str, Any]:
    """`body` as a door sent it before the money pair was required: the key, no money."""
    return {k: v for k, v in body.items() if not k.startswith("expected_")}


def _legacy_request_hash(body: Dict[str, Any]) -> str:
    """The fingerprint create stored for a MONEY-LESS body before the pair was required. Built by
    hand from the documented facts, not through `_request_hash`, so a change there cannot move
    it: an attempt keyed then keeps exactly this hash."""
    buyer = routes_reap.ReapBuyer.model_validate(body["buyer"])
    facts: Dict[str, Any] = {
        "merchant_domain": routes_reap._merchant_domain_key(body["merchant_domain"].lower()),
        "product_key": body["product_key"],
        "variant_key": body.get("variant_key") or "",
        "quantity": int(body.get("quantity", 1)),
        "email": str(buyer.email or "").strip(),
        "shipping_address": {str(k): str(v)
                             for k, v in routes_reap._buyer_address_for_client(buyer).items()},
        "return_url": body.get("return_url") or routes_reap._default_return_url(),
    }
    if body.get("item_source", "reap_variant") != "reap_variant":
        facts["item_source"] = body["item_source"]
    if body.get("offer_code") is not None:
        facts["offer_code"] = body["offer_code"]
    canonical = json.dumps(facts, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _plant_legacy_tombstone(
    body: Dict[str, Any], reason: str = "merchant_not_eligible"
) -> None:
    """A `refused:` key as create wrote it before the money pair was required -- under the
    money-less fingerprint of `body`, which is what a door retrying that attempt sends."""
    await database.execute(
        "INSERT INTO reap_agentic_purchase_keys (agent_id, agent_user_ref_hash, idempotency_key, "
        "purchase_id, request_hash) VALUES (:a, :h, :k, :p, :r)",
        {"a": AGENT, "h": hash_agent_user_ref(USER_REF), "k": body["idempotency_key"],
         "p": f"refused:{reason}", "r": _legacy_request_hash(body)},
    )


# ── fixtures ─────────────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
async def _db():
    """The rail's tables the way PRODUCTION builds them — the self-heal, not a test-only CREATE
    TABLE — and the catalog's tables from the repo's own `metadata`, so the columns this route
    reads are the columns the catalog really has. A hand-written catalog fixture would let a
    renamed column pass here and fail in production."""
    if not database.is_connected:
        await database.connect()
    for table in RAIL_TABLES:
        await database.execute(f"DROP TABLE IF EXISTS {table}")
    await ensure_required_schema_light()

    import sqlalchemy
    from db.accounts import shop_users
    from db.buyer_vault import buyer_identity_links
    from db.catalog import catalog_merchants, catalog_offers, catalog_products, catalog_skus
    from db.commerce_attribution import surface_click_events

    url = (os.getenv("DATABASE_URL") or "").replace("sqlite+aiosqlite://", "sqlite://")
    engine = sqlalchemy.create_engine(url)
    metadata.create_all(
        engine,
        tables=[
            catalog_products,
            catalog_skus,
            catalog_offers,
            catalog_merchants,
            surface_click_events,
            buyer_identity_links,
            # Built so the mint test can PROVE no account row is created for a minted buyer.
            # Without the table that assertion raises "no such table", which is not the same
            # statement and would go on "passing" as an error if it were ever swallowed.
            shop_users,
        ],
        checkfirst=True,
    )
    engine.dispose()

    # Other suites use the same file-backed SQLite database and require migration 044's wider
    # seed table. Only create this route's small fixture if absent, and DROP it after the test;
    # IF NOT EXISTS without teardown poisoned those suites with a permanently narrow schema.
    had_seed_table = await database.fetch_one(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'external_product_seeds'"
    ) is not None
    if not had_seed_table:
        await database.execute(
            "CREATE TABLE external_product_seeds ("
            "id TEXT PRIMARY KEY, status TEXT, domain TEXT, market TEXT, "
            "destination_url TEXT, canonical_url TEXT, attached_product_key TEXT, attached_variant_id TEXT, "
            "seed_data TEXT)"
        )

    for table in CATALOG_TABLES + ("buyer_identity_links", "shop_users"):
        await database.execute(f"DELETE FROM {table}")
    await database.execute(
        "DELETE FROM external_product_seeds WHERE id = :id", {"id": MIRROR_SEED_ID}
    )
    try:
        yield
    finally:
        await database.execute(
            "DELETE FROM external_product_seeds WHERE id = :id", {"id": MIRROR_SEED_ID}
        )
        if not had_seed_table:
            await database.execute("DROP TABLE external_product_seeds")


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.delenv("REAP_AGENTIC_CREATE_ENABLED", raising=False)
    monkeypatch.delenv("REAP_AGENTIC_PILOT_SCOPE", raising=False)
    monkeypatch.setenv("REAP_AGENTIC_ENABLED", "1")
    monkeypatch.setenv("REAP_API_BASE_URL", "https://sandbox.api.reap.global")
    monkeypatch.setenv("REAP_API_KEY", "sk_test_key")
    monkeypatch.delenv("REAP_AGENTIC_CART_LINK_ENABLED", raising=False)
    monkeypatch.delenv("REAP_RETURN_URL_HOSTS", raising=False)
    monkeypatch.delenv("REAP_AGENTIC_RETURN_URL", raising=False)
    monkeypatch.delenv("BUYER_IDENTITY_LINK_SECRET", raising=False)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """These routes make NO partner call. This is the control that proves it rather than the
    docstring asserting it: a handler that reached the wire fails instead of hanging."""

    class _NetworkForbidden:
        def __init__(self, *a, **k):
            raise AssertionError("a route reached the network; this rail's routes make no call")

    monkeypatch.setattr(httpx, "AsyncClient", _NetworkForbidden, raising=True)


class _Caller:
    """Who is calling. Mutated per test rather than re-overridden, so the override installed on
    the app is one object and a test that forgets to restore cannot leak an identity."""

    agent_id = AGENT
    agent_user_ref: Optional[str] = USER_REF

    agent_name = "Reap Routes Test"
    allowed_merchants = None
    session_id = "session_reap_routes"

    def can_access_merchant(self, merchant_id: str) -> bool:
        return True


CALLER = _Caller()


@pytest.fixture
def client(monkeypatch):
    """A real httpx client over the REAL app, with only the two auth dependencies overridden.

    `httpx.AsyncClient` is forbidden by `_no_network`, so the transport is constructed from the
    UNPATCHED class captured before that fixture replaced it — the ban is on a route reaching the
    wire, not on the test harness existing.
    """
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
        def __init__(self):
            self._app = app

        async def request(self, method, url, **kwargs):
            transport = httpx.ASGITransport(app=self._app)
            async with _REAL_ASYNC_CLIENT(
                transport=transport, base_url="http://test"
            ) as http:
                return await http.request(method, url, **kwargs)

        async def post(self, url, **kwargs):
            return await self.request("POST", url, **kwargs)

        async def get(self, url, **kwargs):
            return await self.request("GET", url, **kwargs)

    try:
        yield _Harness()
    finally:
        app.dependency_overrides.pop(get_agent_context, None)
        app.dependency_overrides.pop(get_agent_user_context, None)



# ── seeds ────────────────────────────────────────────────────────────────────────────────────


async def _seed_catalog(
    *,
    platform: str = "shopify",
    domain: str = DOMAIN,
    price: str = "42.50",
    currency: str = "USD",
    second_variant: bool = False,
    variant_title: Optional[str] = "Standard",
    priced: bool = True,
):
    await database.execute(
        """
        INSERT INTO catalog_products (product_key, merchant_id, platform, source_product_id,
                                      title, brand, category, product_type, source_domain)
        VALUES (:pk, 'm_brand', :platform, '1001', 'Standard Eau de Parfum', 'Brand',
                'fragrance', 'Fragrance', :domain)
        """,
        {"pk": PRODUCT_KEY, "platform": platform, "domain": domain},
    )
    skus = [(SKU_KEY, variant_title)] + ([(SECOND_SKU_KEY, "Large")] if second_variant else [])
    for sku_key, title in skus:
        await database.execute(
            """
            INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform,
                                      source_product_id, source_variant_id, title, currency)
            VALUES (:sk, :pk, 'm_brand', :platform, '1001', :vid, :title, :currency)
            """,
            {
                "sk": sku_key,
                "pk": PRODUCT_KEY,
                "platform": platform,
                "vid": sku_key[-2:],
                "title": title,
                "currency": currency,
            },
        )
        if priced:
            await database.execute(
                """
                INSERT INTO catalog_offers (offer_id, sku_key, product_key, merchant_id,
                                            currency, merchant_effective_price)
                VALUES (:oid, :sk, :pk, 'm_brand', :currency, :price)
                """,
                {
                    "oid": f"off_{sku_key}",
                    "sk": sku_key,
                    "pk": PRODUCT_KEY,
                    "currency": currency,
                    "price": price,
                },
            )


async def _seed_eligibility(
    *,
    domain: str = DOMAIN,
    market: str = "US",
    enabled: bool = True,
    product_key: str = "",
    variant_key: str = "",
    labels=None,
    domains=None,
):
    await database.execute(
        """
        INSERT INTO reap_agentic_eligibility (merchant_domain, product_key, variant_key,
                                              market_country, enabled,
                                              accept_variant_labels, also_accept_domains)
        VALUES (:d, :pk, :vk, :m, :enabled, :labels, :domains)
        """,
        {
            "d": domain,
            "pk": product_key,
            "vk": variant_key,
            "m": market,
            "enabled": enabled,
            "labels": json.dumps(labels) if labels else None,
            "domains": json.dumps(domains) if domains else None,
        },
    )


async def _seed_link(*, agent_id: str = AGENT, ref: str = USER_REF, buyer_id: str = BUYER_ID):
    await database.execute(
        """
        INSERT INTO buyer_identity_links (agent_id, agent_user_ref_hash, buyer_id)
        VALUES (:a, :h, :b)
        """,
        {"a": agent_id, "h": hash_agent_user_ref(ref), "b": buyer_id},
    )


async def _seed_all():
    await _seed_catalog()
    await _seed_eligibility()
    await _seed_link()


async def _seed_tierb_verdict(*, verdict: str = "ELIGIBLE", age_hours: int = 0):
    await database.execute(
        """
        INSERT INTO tierb_cart_link_eligibility
            (shop_domain, market, verdict, checked_at)
        VALUES (:domain, 'US', :verdict,
                datetime(CURRENT_TIMESTAMP, :age))
        """,
        {"domain": DOMAIN, "verdict": verdict, "age": f"-{age_hours} hours"},
    )


async def _seed_tierb_shopify_item():
    await _seed_catalog()
    await database.execute(
        "UPDATE catalog_products SET seller_ref = 'm_brand', seed_kind = 'self' "
        "WHERE product_key = :pk", {"pk": PRODUCT_KEY},
    )
    await database.execute(
        "UPDATE catalog_skus SET source_variant_id = '50041364447509' WHERE sku_key = :sk",
        {"sk": SKU_KEY},
    )


async def _reshape_sole_sku_to_placeholder() -> None:
    """Turn `_seed_catalog`'s one sku into the mirror's `<product_key>::canonical` placeholder."""
    placeholder = f"{PRODUCT_KEY}::canonical"
    await database.execute(
        "INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform, source_product_id, "
        "source_variant_id, title, currency) SELECT :new, product_key, merchant_id, platform, "
        "source_product_id, :pk, title, currency FROM catalog_skus WHERE sku_key = :old",
        {"new": placeholder, "pk": PRODUCT_KEY, "old": SKU_KEY},
    )
    await database.execute(
        "UPDATE catalog_offers SET sku_key = :new WHERE sku_key = :old",
        {"new": placeholder, "old": SKU_KEY},
    )
    await database.execute("DELETE FROM catalog_skus WHERE sku_key = :old", {"old": SKU_KEY})


async def _purchase_row(purchase_id: str) -> Dict[str, Any]:
    row = await ledger.get_purchase_internal(purchase_id)
    assert row is not None
    return row


# ── the router is mounted ────────────────────────────────────────────────────────────────────


def test_the_router_is_mounted_on_the_real_app():
    """A merged route is not a running route. This asserts the three paths exist on the app
    `main` builds — not that the module imports, and not that a router object has routes on it.
    Registration is a line in main.py that nothing else would notice the absence of."""
    paths = {getattr(route, "path", None) for route in app.routes}
    assert f"{BASE}/purchases" in paths
    assert f"{BASE}/purchases/{{purchase_id}}" in paths


def test_the_mounted_routes_carry_the_methods_the_contract_promises():
    """The path existing is not the verb existing: a POST-only mount would pass the test above
    and 405 every read."""
    methods = {}
    for route in app.routes:
        path = getattr(route, "path", None)
        if path and path.startswith(BASE):
            methods.setdefault(path, set()).update(getattr(route, "methods", set()) or set())
    assert "POST" in methods[f"{BASE}/purchases"]
    assert "GET" in methods[f"{BASE}/purchases"]
    assert "GET" in methods[f"{BASE}/purchases/{{purchase_id}}"]


# ── the dial: every route 404s while the rail is dark ────────────────────────────────────────


async def test_post_answers_404_while_the_dial_is_off(client, monkeypatch):
    monkeypatch.delenv("REAP_AGENTIC_ENABLED", raising=False)
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 404
    assert _error(resp) == "not_available_on_this_rail"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_get_recovers_stored_status_while_the_dial_is_off(client, monkeypatch):
    await _seed_all()
    created = await client.post(f"{BASE}/purchases", json=_body())
    purchase_id = created.json()["purchase_id"]

    monkeypatch.delenv("REAP_AGENTIC_ENABLED", raising=False)
    resp = await client.get(f"{BASE}/purchases/{purchase_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == purchase_id


async def test_list_answers_404_while_the_dial_is_off(client, monkeypatch):
    await _seed_all()
    await client.post(f"{BASE}/purchases", json=_body())

    monkeypatch.delenv("REAP_AGENTIC_ENABLED", raising=False)
    resp = await client.get(f"{BASE}/purchases")
    assert resp.status_code == 404
    assert _error(resp) == "not_available_on_this_rail"


async def test_an_unconfigured_client_is_as_absent_as_a_dial_that_is_off(client, monkeypatch):
    """`is_configured()` is half the gate. One unset credential must not read as an outage."""
    monkeypatch.delenv("REAP_API_KEY", raising=False)
    await _seed_all()
    for coro in (
        client.post(f"{BASE}/purchases", json=_body()),
        client.get(f"{BASE}/purchases"),
    ):
        resp = await coro
        assert resp.status_code == 404
        assert _error(resp) == "not_available_on_this_rail"

    missing = await client.get(f"{BASE}/purchases/rp_nope")
    assert missing.status_code == 404 and _error(missing) == "purchase_not_found"


async def test_cart_link_stays_dark_when_its_dial_is_off(client, monkeypatch):
    """The lane dial is the ONLY cart-link switch since the client builds Reap's published
    `externalCheckout` body (2026-09-28); off, unset, empty or misspelt is the rail's 404."""
    await _seed_tierb_shopify_item()
    await _seed_tierb_verdict()
    body = _body(item_source="cart_link")
    for dial in (None, "", "0", "ture"):
        if dial is None:
            monkeypatch.delenv("REAP_AGENTIC_CART_LINK_ENABLED", raising=False)
        else:
            monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", dial)
        response = await client.post(f"{BASE}/purchases", json=body)
        assert response.status_code == 404
        assert _error(response) == "not_available_on_this_rail"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_cart_link_needs_a_fresh_tierb_verdict_before_minting_a_buyer(client, monkeypatch):
    await _seed_tierb_shopify_item()
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    for verdict, age in ((None, 0), ("LOGIN_REQUIRED", 0), ("ELIGIBLE", 49)):
        if verdict is not None:
            await database.execute("DELETE FROM tierb_cart_link_eligibility")
            await _seed_tierb_verdict(verdict=verdict, age_hours=age)
        response = await client.post(f"{BASE}/purchases", json=_body(item_source="cart_link"))
        assert response.status_code == 409
        assert _error(response) == "merchant_not_eligible"
    assert await database.fetch_val("SELECT COUNT(*) FROM buyer_identity_links") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_cart_link_route_builds_server_priced_item_and_owned_click(client, monkeypatch):
    await _seed_tierb_shopify_item()
    await _seed_tierb_verdict()
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    # These unrecognised client fields are ignored, not used as price, item, or attribution.
    response = await client.post(f"{BASE}/purchases", json=_body(
        item_source="cart_link", our_price_minor=1, cart_url="https://evil.example/cart/1:1",
        seller_ref="attacker",
    ))
    assert response.status_code == 202, response.text
    purchase = await _purchase_row(response.json()["purchase_id"])
    assert purchase["item_source"] == "cart_link"
    assert purchase["our_price_minor"] == 4250
    assert purchase["currency"] == "USD" and purchase["market_country"] == "US"
    assert purchase["cart_url"] == (
        "https://brand.example/cart/50041364447509:1?attributes[pivota_click_id]="
        f"{purchase['click_id']}&country=US"
    )
    click = await database.fetch_one(
        "SELECT merchant_id, dest_domain, context FROM surface_click_events "
        "WHERE click_id = :click", {"click": purchase["click_id"]},
    )
    assert click is not None
    assert click["merchant_id"] == "m_brand" and click["dest_domain"] == DOMAIN
    context = click["context"]
    if isinstance(context, str):
        context = json.loads(context)
    assert context["seller_ref"] == "m_brand"
    assert EMAIL not in json.dumps(context)


async def test_cart_link_refuses_a_non_numeric_catalog_variant(client, monkeypatch):
    await _seed_catalog()  # synthetic source_variant_id='v1' is not a Shopify id
    await _seed_tierb_verdict()
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    response = await client.post(f"{BASE}/purchases", json=_body(item_source="cart_link"))
    assert response.status_code == 409
    assert _error(response) == "row_variant_unverified"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


@pytest.mark.parametrize("stamped_variants,attached_variant,attached_product,expected,canonical_url", [
    (("50041364447509",), "50041364447509", PRODUCT_KEY, 202, None),
    (("50041364447509",), None, PRODUCT_KEY, 202, None),
    ((), "50041364447509", PRODUCT_KEY, 409, None),
    (("50041364447509",), "99999999999999", PRODUCT_KEY, 409, None),
    (("50041364447509",), "50041364447509", "prod::other", 409, None),
    (("50041364447509", "50041364447510"), "50041364447509", PRODUCT_KEY, 409, None),
    (("50041364447509", "MALFORMED"), "50041364447509", PRODUCT_KEY, 409, None),
    (("50041364447509",), "50041364447509", PRODUCT_KEY, 409,
     f"https://{DOMAIN}/products/new"),
])
async def test_cart_link_mirrored_seed_requires_product_bound_storefront_variant(
    client, monkeypatch, stamped_variants, attached_variant, attached_product, expected,
    canonical_url,
):
    await _seed_catalog(platform="external_seed")
    await database.execute(
        "UPDATE catalog_products SET seller_ref = 'm_brand', seed_kind = 'self', "
        "source_system = 'external_product_seeds_mirror_v1', source_ref = :seed_id "
        "WHERE product_key = :pk", {"pk": PRODUCT_KEY, "seed_id": MIRROR_SEED_ID},
    )
    # THE MIRROR'S REAL PLACEHOLDER SHAPE (`<product_key>::canonical`, source_variant_id = the
    # product_key -- scripts/mirror_external_seeds_to_catalog_products / external_offer_dual_write):
    # the only sku, so the seed's storefront proof is the only variant identity.
    await _reshape_sole_sku_to_placeholder()
    seed_data = (
        {"snapshot": {"storefront_platform": "shopify", "variants": [
            variant if variant == "MALFORMED" else {"shopify_variant_id": variant}
            for variant in stamped_variants
        ], "shopify_cart_proof": {
            "source": "products_js_v1",
            "product_js_url": f"https://{DOMAIN}/products/test.js",
            "live_variant_count": 1,
            "variant_id": stamped_variants[0],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }}} if stamped_variants else {}
    )
    await database.execute(
        "INSERT INTO external_product_seeds "
        "(id, status, domain, market, destination_url, canonical_url, attached_product_key, "
        "attached_variant_id, seed_data) "
        "VALUES (:seed_id, 'active', :domain, 'US', :destination, :canonical, :product_key, :variant, "
        ":seed_data)",
        {"seed_id": MIRROR_SEED_ID, "domain": DOMAIN,
         "destination": f"https://{DOMAIN}/products/test", "canonical": canonical_url,
         "product_key": attached_product,
         "variant": attached_variant, "seed_data": json.dumps(seed_data)},
    )
    await _seed_tierb_verdict()
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    response = await client.post(
        f"{BASE}/purchases", json=_body(item_source="cart_link", variant_key=None))
    assert response.status_code == expected, response.text
    if expected == 202:
        purchase = await _purchase_row(response.json()["purchase_id"])
        assert "/cart/50041364447509:1?" in purchase["cart_url"]
    else:
        assert _error(response) == "row_variant_unverified"
        assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_a_dark_rail_answers_404_even_for_a_body_that_would_not_validate(client, monkeypatch):
    """The gate runs BEFORE the body is validated. A 422 here would tell a caller the route is
    there, which is the one thing 404 is for."""
    monkeypatch.delenv("REAP_AGENTIC_ENABLED", raising=False)
    resp = await client.post(f"{BASE}/purchases", json={"nonsense": True})
    assert resp.status_code == 404


# ── the buyer is required ────────────────────────────────────────────────────────────────────


async def test_every_route_refuses_401_without_an_agent_user(client):
    await _seed_all()
    CALLER.agent_user_ref = None
    for resp in (
        await client.post(f"{BASE}/purchases", json=_body()),
        await client.get(f"{BASE}/purchases"),
        await client.get(f"{BASE}/purchases/rp_nope"),
    ):
        assert resp.status_code == 401
        assert _error(resp) == "agent_user_required"


# ── WP4b: the buyer identity is minted on the first purchase ─────────────────────────────────
#
# This block replaces `test_a_missing_buyer_link_refuses_rather_than_minting_a_buyer`, which
# asserted the WP4 behaviour the owner reversed on 2026-09-18. The refusal it pinned —
# `buyer_unlinked` on an agent-only buyer — no longer exists on this route.


async def _links() -> list:
    rows = await database.fetch_all(
        "SELECT agent_id, agent_user_ref_hash, buyer_id FROM buyer_identity_links"
    )
    return [dict(r) for r in rows]


async def test_the_first_purchase_mints_exactly_one_buyer_one_link_and_one_ref(client):
    """The whole of WP4b in one assertion set: an agent-only buyer, never seen before, gets a
    purchase — and gets exactly one of each thing behind it."""
    await _seed_catalog()
    await _seed_eligibility()

    resp = await client.post(f"{BASE}/purchases", json=_body())

    assert resp.status_code == 202
    links = await _links()
    assert len(links) == 1
    assert links[0]["agent_id"] == AGENT
    assert links[0]["agent_user_ref_hash"] == hash_agent_user_ref(USER_REF)
    assert str(links[0]["buyer_id"]).strip()
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 1
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 1


async def test_the_minted_buyer_id_is_not_derived_from_the_email(client):
    """THE SECURITY PROPERTY, not a formatting preference.

    `db.accounts.create_or_get_shop_user` is create-or-GET: handed an email that already has an
    account it returns THAT account's id. If this route minted through it, an agent asserting a
    stranger's address would be handed the stranger's real buyer_id — and
    `_buyer_prefill_from_identity_link` would then hand that agent the account's email and
    default shipping address. So the minted id must contain nothing from the request, and no
    `shop_users` row may be created for it.

    ── WHY NOT `EMAIL` ─────────────────────────────────────────────────────────────────────

    The minted id is `u_` + 16 hex characters. The module's EMAIL local part is "ada", which is
    three HEX DIGITS, so `"ada" not in buyer_id` failed by pure chance (CI run 35808498872:
    'u_4f49a0007fada0e3'). Any local part spelled only from [0-9a-f] can do that. This test's
    email has a `z`, `q`, `w`, `y`, `x` and a `.` in its local part, none of which a minted id
    can contain, so the substring checks below can only fail if the id really carries the text.

    The substring checks are the weak half, though: `sha256(email)[:16]` contains no email text
    and would pass them. What catches derivation is (1) the id is not a window of any obvious
    encoding of the email, and (2) THE SAME EMAIL, sent by a different agent for a different
    end user, gets a DIFFERENT id — so the id is not a function of the email, whatever the
    encoding. Both are coincidence-proof: a random 64-bit id matches either by chance with
    probability around 2**-58.
    """
    email = "zoltan.qwyx@example.test"
    local_part = email.split("@")[0]
    body = _body(buyer=_buyer(email=email))

    await _seed_catalog()
    await _seed_eligibility()
    first = await client.post(f"{BASE}/purchases", json=body)
    assert first.status_code == 202

    buyer_id = (await _links())[0]["buyer_id"]
    # The id space, so no reader can tell a minted buyer from a signed-up one by its shape.
    assert buyer_id.startswith("u_")
    assert len(buyer_id) == 18
    int(buyer_id[2:], 16)  # hex, which is what makes the non-hex local part a sound probe

    assert email not in buyer_id
    assert local_part not in buyer_id
    assert USER_REF not in buyer_id
    assert hash_agent_user_ref(USER_REF) not in buyer_id

    # (1) Not a window of a straightforward encoding of the email or of its local part.
    digest = buyer_id[2:]
    for source in {email, email.lower(), email.upper(), local_part}:
        raw = source.encode()
        encodings = [raw.hex(), base64.b16encode(raw).decode().lower()]
        encodings += [
            hashlib.new(name, raw).hexdigest()
            for name in ("md5", "sha1", "sha224", "sha256", "sha384", "sha512", "blake2b", "blake2s")
        ]
        encodings += [base64.b64encode(raw).decode(), base64.urlsafe_b64encode(raw).decode()]
        for encoded in encodings:
            assert digest not in encoded, (source, encoded)

    # (2) Not a function of the email. Same email, a different agent and end user: a different
    # buyer. An id derived from the email alone — in any encoding — would be the same one here,
    # which is exactly the collision that would hand one agent another's buyer.
    CALLER.agent_id = OTHER_AGENT
    CALLER.agent_user_ref = OTHER_USER_REF
    second = await client.post(f"{BASE}/purchases", json=body)
    assert second.status_code == 202
    other = {l["buyer_id"] for l in await _links() if l["agent_id"] == OTHER_AGENT}
    assert len(other) == 1
    assert other != {buyer_id}

    #: No account row is created, and the prefill reader tolerates that — it returned None for
    #: this buyer yesterday, when there was no link at all, and it returns None now.
    assert await database.fetch_val(
        "SELECT COUNT(*) FROM shop_users WHERE id = :id", {"id": buyer_id}
    ) == 0


async def test_a_second_purchase_reuses_the_buyer_the_link_and_the_enrollment(client):
    """Idempotent. A second ref would be a second enrollment, which is a second card."""
    await _seed_catalog()
    await _seed_eligibility()

    first = await client.post(f"{BASE}/purchases", json=_body())
    buyer_id = (await _links())[0]["buyer_id"]
    second = await client.post(f"{BASE}/purchases", json=_body())

    assert second.status_code == 202
    assert len(await _links()) == 1
    assert (await _links())[0]["buyer_id"] == buyer_id
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 1
    a = (await _purchase_row(first.json()["purchase_id"]))["buyer_ref"]
    b = (await _purchase_row(second.json()["purchase_id"]))["buyer_ref"]
    assert a == b


async def test_a_buyer_already_linked_by_the_hosted_checkout_is_not_re_minted(client):
    """The link a HUMAN's sign-in wrote is the one that stands. A route that overwrote it would
    detach a real account from the agent session that belongs to it."""
    await _seed_all()

    resp = await client.post(f"{BASE}/purchases", json=_body())

    assert resp.status_code == 202
    links = await _links()
    assert len(links) == 1
    assert links[0]["buyer_id"] == BUYER_ID


async def test_two_agents_with_the_same_user_ref_get_two_buyers(client):
    """`agent_user_ref` is opaque and agent-scoped: "user-ada" at two agents is two people, and
    the link's key is the PAIR. One buyer for both would merge two strangers' cards."""
    await _seed_catalog()
    await _seed_eligibility()

    await client.post(f"{BASE}/purchases", json=_body())
    CALLER.agent_id = OTHER_AGENT
    await client.post(f"{BASE}/purchases", json=_body())

    links = await _links()
    assert len(links) == 2
    assert {l["agent_id"] for l in links} == {AGENT, OTHER_AGENT}
    assert len({l["buyer_id"] for l in links}) == 2
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 2


WINNER_BUYER_ID = "u_thewinner00000"


def _race_a_link_in(monkeypatch, buyer_id: str = WINNER_BUYER_ID):
    """Make a competing link appear BETWEEN the route's SELECT and its INSERT.

    ── WHY THIS SEAM AND NOT A PRE-INSERTED ROW ─────────────────────────────────────────────

    The first cut of this test seeded the link before the request. It asserted the right thing
    and proved nothing: the route's opening SELECT finds a pre-seeded row and returns on the
    spot, so the INSERT — the statement whose `ON CONFLICT` behaviour is the entire point — was
    never reached. The mutant that turns `DO NOTHING` into `DO UPDATE SET buyer_id = EXCLUDED
    .buyer_id` SURVIVED that test on both dialects, which is how the gap was found.

    The race is a write that lands after our read missed. So the competing row is inserted from
    inside `database.execute`, on the way into the route's own link INSERT — which is exactly
    that window, deterministically, on either engine.
    """
    real_execute = database.execute
    state = {"raced": False}

    async def _racing_execute(query, values=None, *args, **kwargs):
        if not state["raced"] and "INSERT INTO buyer_identity_links" in str(query):
            # Set FIRST: the seed below goes through this same patched callable.
            state["raced"] = True
            await _seed_link(buyer_id=buyer_id)
        return await real_execute(query, values, *args, **kwargs)

    monkeypatch.setattr(database, "execute", _racing_execute)
    return state


async def test_two_user_refs_at_one_agent_get_two_buyers(client):
    """THE OTHER DIRECTION, and the one that was undefended.

    `test_two_agents_with_the_same_user_ref_get_two_buyers` covers two AGENTS. This covers two END
    USERS OF ONE AGENT sending the SAME body — same `buyer.email`, same everything — and it is the
    direction where a "make the mint deterministic" refactor does real harm: a buyer id derived
    from `(agent_id, email)` collapses them onto ONE buyer, ONE `reap_buyer_ref`, and therefore
    ONE STORED CARD shared between two strangers who merely typed the same address.

    A mutant minting `"u_" + sha256(agent_id + "|" + email)[:16]` survives every other test in
    this file: the id contains no literal substring of the email, so the non-containment
    assertions above pass, and the cross-agent test passes because `agent_id` is in the hash. Only
    this test kills it.
    """
    await _seed_catalog()
    await _seed_eligibility()

    # Same agent, same body, two different end users.
    CALLER.agent_user_ref = USER_REF
    first = await client.post(f"{BASE}/purchases", json=_body())
    CALLER.agent_user_ref = OTHER_USER_REF
    second = await client.post(f"{BASE}/purchases", json=_body())

    assert first.status_code == 202
    assert second.status_code == 202
    links = await _links()
    assert len(links) == 2
    assert {l["agent_id"] for l in links} == {AGENT}, "precondition: one agent, two end users"
    assert {l["agent_user_ref_hash"] for l in links} == {
        hash_agent_user_ref(USER_REF),
        hash_agent_user_ref(OTHER_USER_REF),
    }
    assert len({l["buyer_id"] for l in links}) == 2, (
        "two end users of one agent share a buyer id — and therefore one stored card"
    )
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 2
    a = (await _purchase_row(first.json()["purchase_id"]))["buyer_ref"]
    b = (await _purchase_row(second.json()["purchase_id"]))["buyer_ref"]
    assert a != b, "two end users of one agent were enrolled against one card"


async def test_a_link_written_between_the_read_and_the_insert_wins(client, monkeypatch):
    """THE RACE, at the statement where it bites.

    Two concurrent first POSTs both SELECT nothing and both INSERT. The unique constraint on
    `(agent_id, agent_user_ref_hash)` lets exactly one land; `ON CONFLICT DO NOTHING` absorbs the
    other, and the route then RE-READS rather than returning what it minted. The winner's
    buyer_id must be what the purchase is opened under — never ours.
    """
    await _seed_catalog()
    await _seed_eligibility()
    state = _race_a_link_in(monkeypatch)

    resp = await client.post(f"{BASE}/purchases", json=_body())

    assert state["raced"], "precondition: the route never reached its link INSERT"
    assert resp.status_code == 202
    links = await _links()
    assert len(links) == 1, "the loser inserted a second link instead of yielding"
    assert links[0]["buyer_id"] == WINNER_BUYER_ID, (
        "the route overwrote a link that was written first — `DO NOTHING` became `DO UPDATE`"
    )
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 1
    row = await _purchase_row(resp.json()["purchase_id"])
    ref = await database.fetch_val(
        "SELECT reap_buyer_ref FROM reap_agentic_buyer_refs WHERE buyer_id = :b",
        {"b": WINNER_BUYER_ID},
    )
    assert row["buyer_ref"] == ref, "the purchase was opened under a ref that is not the winner's"


async def test_two_first_purchases_in_flight_leave_one_buyer_one_link_and_one_ref(client):
    """Both callers run the whole handler; only one row of each may exist afterwards."""
    import asyncio
    import contextvars

    await _seed_catalog()
    await _seed_eligibility()

    # HTTP requests own separate connections, rather than inheriting the
    # fixture's databases0.7 ContextVar and sharing nested transactions.
    tasks = [contextvars.Context().run(asyncio.create_task,
        client.post(f"{BASE}/purchases", json=_body())) for _ in range(2)]
    first, second = await asyncio.gather(*tasks)

    assert first.status_code == 202
    assert second.status_code == 202
    assert len(await _links()) == 1
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 1
    a = (await _purchase_row(first.json()["purchase_id"]))["buyer_ref"]
    b = (await _purchase_row(second.json()["purchase_id"]))["buyer_ref"]
    assert a == b


async def test_the_minted_buyer_id_is_never_in_the_response(client):
    await _seed_catalog()
    await _seed_eligibility()

    resp = await client.post(f"{BASE}/purchases", json=_body())
    buyer_id = (await _links())[0]["buyer_id"]

    assert buyer_id not in resp.text
    read = await client.get(f"{BASE}/purchases/{resp.json()['purchase_id']}")
    assert buyer_id not in read.text
    listed = await client.get(f"{BASE}/purchases")
    assert buyer_id not in listed.text


async def test_nothing_on_the_mint_path_logs_the_identity(client, caplog):
    """The same detector as `test_nothing_on_this_path_logs_the_buyer`, aimed at what WP4b added:
    the minted buyer id, the opaque ref, the ref hash and the consent tag. `_ours` excludes the
    DRIVER loggers, which echo every bind parameter at DEBUG — that is the claim being made here,
    "our code does not log the identity", not "no library ever sees it"."""
    await _seed_catalog()
    await _seed_eligibility()
    with caplog.at_level(logging.DEBUG):
        await client.post(f"{BASE}/purchases", json=_body())

    buyer_id = (await _links())[0]["buyer_id"]
    ref = await database.fetch_val("SELECT reap_buyer_ref FROM reap_agentic_buyer_refs")
    ours = [record for record in caplog.records if _ours(record)]
    # THE CONTROL, for the reason the twin above gives: an absence assertion passes when the
    # mechanism is absent too.
    assert ours, "precondition: this path logged something of ours to look at"
    assert any("reap_agentic" in record.getMessage() for record in ours)

    logged = "\n".join(record.getMessage() for record in ours)
    for secret in (buyer_id, ref, hash_agent_user_ref(USER_REF), USER_REF, CONSENT):
        assert secret not in logged


# ── WP4b: consent ────────────────────────────────────────────────────────────────────────────


async def test_a_purchase_without_consent_is_refused_before_anything_is_written(client):
    await _seed_catalog()
    await _seed_eligibility()

    resp = await client.post(
        f"{BASE}/purchases", json=_body(buyer={"email": EMAIL, "shipping_address": dict(ADDRESS)})
    )

    assert resp.status_code == 400
    assert _error(resp) == "consent_required"
    assert await _links() == []
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


@pytest.mark.parametrize("value", ["", "   ", None, "v" * 33, "v1\x00"])
async def test_a_blank_overlong_or_unprintable_consent_is_refused(client, value):
    await _seed_all()
    resp = await client.post(
        f"{BASE}/purchases", json=_body(buyer=_buyer(consent_version=value))
    )
    assert resp.status_code == 400
    assert _error(resp) == "consent_required"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_the_dial_is_checked_before_consent_so_a_dark_rail_stays_dark(client, monkeypatch):
    """THE ORDERING MUTANT'S TARGET. A consent check ahead of the dial answers 400 where every
    well-formed request answers 404 — a working probe for a rail that is meant to be absent."""
    monkeypatch.delenv("REAP_AGENTIC_ENABLED", raising=False)
    resp = await client.post(
        f"{BASE}/purchases", json=_body(buyer={"email": EMAIL, "shipping_address": dict(ADDRESS)})
    )
    assert resp.status_code == 404
    assert _error(resp) == "not_available_on_this_rail"


async def test_consent_is_stored_on_the_row_the_enrollment_hangs_off(client):
    await _seed_catalog()
    await _seed_eligibility()

    await client.post(f"{BASE}/purchases", json=_body())

    row = await database.fetch_one(
        "SELECT consent_version, consented_at FROM reap_agentic_buyer_refs"
    )
    assert dict(row)["consent_version"] == CONSENT
    assert dict(row)["consented_at"] is not None


async def test_a_later_consent_version_replaces_the_stored_one(client):
    """LATEST WINS. A buyer who accepted v2 has not un-accepted it by having a row saying v1."""
    await _seed_catalog()
    await _seed_eligibility()

    await client.post(f"{BASE}/purchases", json=_body())
    await client.post(
        f"{BASE}/purchases", json=_body(buyer=_buyer(consent_version="reap-agentic-v2"))
    )

    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 1
    assert await database.fetch_val(
        "SELECT consent_version FROM reap_agentic_buyer_refs"
    ) == "reap-agentic-v2"


async def test_consent_is_recorded_for_a_buyer_the_hosted_checkout_linked(client):
    """The already-linked path writes the consent too — it is the path that mints no buyer, and
    the one a `_record_consent` call could most easily be dropped from."""
    await _seed_all()
    await client.post(f"{BASE}/purchases", json=_body())
    assert await database.fetch_val(
        "SELECT consent_version FROM reap_agentic_buyer_refs WHERE buyer_id = :b",
        {"b": BUYER_ID},
    ) == CONSENT


async def test_a_replay_still_records_the_latest_consent(client):
    """THE CONTRACT SAYS "REWRITTEN ON EVERY PURCHASE", SO A REPLAY MUST REWRITE IT.

    A replay returns 202 before the buyer-ref code runs, so it was the one POST that answered
    successfully and left the stored tag stale — while the contract page, the runbook and
    migration 227's header all promised the opposite. A retry is exactly where a door's consent
    version plausibly changes mid-flight.
    """
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-replay"))
    assert first.status_code == 202
    before = await database.fetch_val("SELECT consented_at FROM reap_agentic_buyer_refs")

    second = await client.post(
        f"{BASE}/purchases",
        json=_body(idempotency_key="k-replay", buyer=_buyer(consent_version="v9")),
    )

    assert second.status_code == 202
    assert second.json()["purchase_id"] == first.json()["purchase_id"], (
        "precondition: this was a replay, not a new purchase"
    )
    assert await database.fetch_val("SELECT consent_version FROM reap_agentic_buyer_refs") == "v9"
    assert await database.fetch_val("SELECT consented_at FROM reap_agentic_buyer_refs") >= before


async def test_a_replay_whose_link_was_deleted_does_not_re_mint_one(client):
    """The replay path records consent through a READ, never through the minting lookup.

    ── THE STATE THIS BUILDS, AND WHY IT IS REACHABLE ───────────────────────────────────────

    An idempotency key outlives its purchase's link if the link is deleted inside the 24-hour
    window — an erasure request, or an operator cleaning up. The key row is keyed on
    `(agent_id, agent_user_ref_hash, idempotency_key)` and knows nothing about the link, so the
    replay still resolves.

    If the replay path resolved the buyer with `_buyer_id_for`, that retry would silently MINT a
    fresh identity for a buyer whose link was deliberately removed — recreating erased data, and
    doing it on a path that has not checked eligibility. `_linked_buyer_id` is a pure read, so
    there is simply nothing to record the consent against and nothing is written.

    The first cut of this test switched end users instead of deleting the link, which meant no
    replay resolved at all and the mutant it was aimed at survived it.
    """
    await _seed_catalog()
    await _seed_eligibility()
    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-x"))
    assert first.status_code == 202
    assert len(await _links()) == 1

    await database.execute("DELETE FROM buyer_identity_links")
    assert await _links() == []

    second = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-x"))

    assert second.status_code == 202
    assert second.json()["purchase_id"] == first.json()["purchase_id"], (
        "precondition: this was a replay — otherwise the mint below is not the thing under test"
    )
    assert await _links() == [], (
        "a replay re-created a buyer identity that had been deleted, on a path that has not "
        "checked eligibility"
    )


async def test_the_link_is_touched_when_it_is_reused(client):
    """`last_seen_at` means "last seen", not "last seen by the checkout-intent path".

    `_buyer_prefill_from_identity_link` stamps it on every read. This route used a link without
    saying so, which would make any dormancy sweep built on the column retire buyers who purchase
    on this rail daily."""
    await _seed_all()
    await database.execute(
        "UPDATE buyer_identity_links SET last_seen_at = NULL WHERE agent_id = :a", {"a": AGENT}
    )
    assert await database.fetch_val("SELECT last_seen_at FROM buyer_identity_links") is None

    await client.post(f"{BASE}/purchases", json=_body())

    assert await database.fetch_val("SELECT last_seen_at FROM buyer_identity_links") is not None


@pytest.mark.parametrize("value", [123, True, {}, [], 1.5])
async def test_a_non_string_consent_version_is_invalid_request_not_consent_required(
    client, value
):
    """A TYPE ERROR IS A MALFORMED BODY, AND THE CODES MEAN DIFFERENT THINGS.

    `consent_required` tells a door to go and ask its user; `invalid_request` tells it to fix its
    JSON. A non-string never reaches `_consent_version` — pydantic refuses it first, and in v2
    that includes `123` and `true`, which are NOT coerced to strings. Pinned because the contract
    page now documents this split and a reader would otherwise have to guess."""
    await _seed_all()
    resp = await client.post(
        f"{BASE}/purchases", json=_body(buyer=_buyer(consent_version=value))
    )
    assert resp.status_code == 400
    assert _error(resp) == "invalid_request"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_consent_is_not_part_of_the_idempotency_hash(client):
    """A re-consent is not a different purchase. Folding the tag into the request hash would turn
    a door that upgraded its consent version mid-retry into an `idempotency_conflict`."""
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-consent"))
    second = await client.post(
        f"{BASE}/purchases",
        json=_body(idempotency_key="k-consent", buyer=_buyer(consent_version="v2")),
    )
    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["purchase_id"] == first.json()["purchase_id"]


# ── the happy path, and where the price comes from ───────────────────────────────────────────


async def test_a_purchase_opens_and_the_response_says_so(client):
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    payload = resp.json()
    assert payload["purchase_id"].startswith("rp_")
    assert payload["status"] == "resolving"
    assert payload["poll_after_seconds"] == svc.POLL_INTERVALS["resolving"]

    row = await _purchase_row(payload["purchase_id"])
    assert row["state"] == "resolving"
    assert row["merchant_domain"] == DOMAIN
    assert row["product_key"] == PRODUCT_KEY
    assert row["variant_key"] == SKU_KEY
    assert row["variant_title"] == "Standard"
    assert row["brand"] == "Brand"
    assert row["market_country"] == "US"
    assert row["agent_id"] == AGENT
    assert row["agent_user_ref_hash"] == hash_agent_user_ref(USER_REF)


async def _agent_purchase_parents(purchase_id: str) -> list:
    return [
        dict(r) for r in await database.fetch_all(
            "SELECT * FROM agent_purchases WHERE rail = 'reap' AND rail_purchase_id = :i",
            {"i": purchase_id},
        )
    ]


async def test_an_opened_purchase_gets_its_rail_neutral_parent_when_the_ledger_is_on(
    client, monkeypatch
):
    """Payment orchestration P0: the parent is written after the create commits, copied from the
    committed row, and the 202 the agent sees is unchanged."""
    import db.agent_purchase_ledger as agent_purchase_ledger

    monkeypatch.setenv(agent_purchase_ledger.AGENT_PURCHASE_LEDGER_ENABLED_ENV, "1")
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    purchase_id = resp.json()["purchase_id"]
    assert set(resp.json()) == {
        "purchase_id", "status", "poll_after_seconds", "checkout_dispatch_state",
        "contact_reentry_required",
    }
    [parent] = await _agent_purchase_parents(purchase_id)
    assert parent["id"].startswith("pp_")
    assert parent["executor"] == "rail_managed"
    assert parent["agent_id"] == AGENT
    assert parent["agent_user_ref_hash"] == hash_agent_user_ref(USER_REF)


async def test_no_parent_is_written_while_the_ledger_is_off(client, monkeypatch):
    import db.agent_purchase_ledger as agent_purchase_ledger

    monkeypatch.delenv(agent_purchase_ledger.AGENT_PURCHASE_LEDGER_ENABLED_ENV, raising=False)
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    assert await _agent_purchase_parents(resp.json()["purchase_id"]) == []


async def test_a_failing_ledger_never_fails_or_rolls_back_the_purchase(client, monkeypatch):
    import db.agent_purchase_ledger as agent_purchase_ledger

    monkeypatch.setenv(agent_purchase_ledger.AGENT_PURCHASE_LEDGER_ENABLED_ENV, "1")

    async def _boom(*a, **k):
        raise RuntimeError("agent_purchases unavailable")

    monkeypatch.setattr(agent_purchase_ledger, "ensure_reap_parent", _boom)
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["state"] == "resolving"


async def test_a_stalled_ledger_write_cannot_hold_the_202(client, monkeypatch):
    import asyncio
    import time
    import db.agent_purchase_ledger as agent_purchase_ledger

    monkeypatch.setenv(agent_purchase_ledger.AGENT_PURCHASE_LEDGER_ENABLED_ENV, "1")
    monkeypatch.setattr(routes_reap, "_RECORD_PARENT_TIMEOUT_S", 0.05)

    async def _stall(*a, **k):
        await asyncio.sleep(30)

    monkeypatch.setattr(agent_purchase_ledger, "ensure_reap_parent", _stall)
    await _seed_all()
    started = time.monotonic()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    assert time.monotonic() - started < 5


async def test_the_price_is_our_catalogs_and_the_request_cannot_move_it(client):
    """THE CENTRAL GUARD. `verify_quote` compares `quantity * our_price_minor` against Reap's
    subtotal EXACTLY. If the caller could set that number, the comparison would be the caller's
    claim against itself and a substituted, repriced variant would sail through."""
    await _seed_all()
    resp = await client.post(
        f"{BASE}/purchases",
        json=_body(
            unit_price=1.00,
            price=1.00,
            our_price_minor=100,
            currency="EUR",
            amount_minor=100,
        ),
    )
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["our_price_minor"] == 4250
    assert row["currency"] == "USD"


async def test_the_price_survives_the_numeric_to_text_round_trip(client):
    """A price the binary-float path would mangle. 42.50 as a float is 42.4999999999999964 and
    `major_to_minor` refuses a float outright — so this is also the test that fails if the CAST
    ever comes out of the offer query."""
    await _seed_catalog(price="19.99")
    await _seed_eligibility()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body(expected_unit_price_minor=1999))
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["our_price_minor"] == 1999


async def test_a_click_id_is_minted_and_recorded(client):
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    row = await _purchase_row(resp.json()["purchase_id"])
    assert str(row["click_id"] or "").startswith("clk_")
    assert row["click_id"] not in (None, "")


async def test_two_purchases_get_different_click_ids(client):
    """A click id shared between purchases is not an attribution join, it is a collision — and
    this rail has no other join between a Reap checkout and the session that started it."""
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body())
    second = await client.post(f"{BASE}/purchases", json=_body())
    a = (await _purchase_row(first.json()["purchase_id"]))["click_id"]
    b = (await _purchase_row(second.json()["purchase_id"]))["click_id"]
    assert a != b


# ── eligibility ──────────────────────────────────────────────────────────────────────────────


async def test_a_merchant_with_no_eligibility_row_is_refused(client):
    await _seed_catalog()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "merchant_not_eligible"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_a_disabled_eligibility_row_is_refused(client):
    await _seed_catalog()
    await _seed_link()
    await _seed_eligibility(enabled=False)
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    # Its OWN code since #2425's review: an operator's "off" must be distinguishable from "never
    # listed", or a door that tries the cart-link lane on the second routes around the first.
    assert _error(resp) == "merchant_disabled"


async def test_a_buyer_in_another_market_is_refused(client):
    """DOMESTIC ONLY. The merchant is enabled — for SG. The buyer ships to US."""
    await _seed_catalog()
    await _seed_link()
    await _seed_eligibility(market="SG")
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "merchant_not_eligible"


async def test_the_market_comes_from_the_shipping_country_not_from_the_caller(client):
    """Two eligibility rows, two markets. The one that applies is the one the parcel goes to.

    The catalogue is seeded in CAD because the parcel goes to Canada: the market a purchase binds
    and the currency its price is in are the same decision, and a CAD buyer against a USD offer is
    `row_currency_mismatch` by design.
    """
    await _seed_catalog(currency="CAD")
    await _seed_link()
    await _seed_eligibility(market="US")
    await _seed_eligibility(market="CA")
    address = dict(ADDRESS, country="CA")
    resp = await client.post(
        f"{BASE}/purchases",
        json=_body(buyer=_buyer(shipping_address=address), market_country="US",
                   expected_currency="CAD"),
    )
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["market_country"] == "CA"


async def test_an_eligibility_row_for_another_domain_does_not_enable_this_one(client):
    await _seed_catalog()
    await _seed_link()
    await _seed_eligibility(domain="someone-else.example")
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "merchant_not_eligible"


async def test_per_row_aliases_reach_the_purchase_row(client):
    await _seed_catalog()
    await _seed_link()
    await _seed_eligibility()
    await _seed_eligibility(
        product_key=PRODUCT_KEY,
        labels=["Standard 50ml"],
        domains=["shop.brand.example"],
    )
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert ledger._decode_json(row["accept_variant_labels"]) == ["Standard 50ml"]
    assert ledger._decode_json(row["also_accept_domains"]) == ["shop.brand.example"]


async def test_an_override_pinned_to_another_variant_does_not_leak_onto_this_one(client):
    """An accepted label names ONE object. A variant-pinned override that applied to its siblings
    would vouch for a label nobody vouched for."""
    await _seed_catalog(second_variant=True)
    await _seed_link()
    await _seed_eligibility()
    await _seed_eligibility(
        product_key=PRODUCT_KEY, variant_key=SECOND_SKU_KEY, labels=["Large 100ml"]
    )
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["accept_variant_labels"] in (None, "null")


async def test_a_product_override_alone_cannot_make_a_merchant_eligible(client):
    """`enabled` is read from the MERCHANT row only. A per-product exception that granted
    eligibility would be a way to turn a merchant on without turning it on."""
    await _seed_catalog()
    await _seed_link()
    await _seed_eligibility(product_key=PRODUCT_KEY, labels=["Standard 50ml"])
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "merchant_not_eligible"


# ── our catalog row ──────────────────────────────────────────────────────────────────────────


async def test_a_product_that_does_not_exist_is_refused(client):
    await _seed_eligibility()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "row_not_found"


async def test_a_product_belonging_to_another_domain_answers_row_not_found(client):
    """Not `merchant_not_eligible`, and certainly not a purchase: the domain is a CONJUNCT on the
    product read, so naming an enabled merchant and somebody else's product finds nothing — the
    same answer as a key that does not exist, so this cannot enumerate another catalogue."""
    await _seed_catalog(domain="someone-else.example")
    await _seed_eligibility()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "row_not_found"


async def test_a_non_shopify_row_is_refused(client):
    await _seed_catalog(platform="external_seed")
    await _seed_eligibility()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "row_not_shopify"


async def test_an_unpriced_row_is_refused(client):
    await _seed_catalog(priced=False)
    await _seed_eligibility()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "row_unpriced"


async def test_a_variant_key_belonging_to_another_product_is_refused(client):
    await _seed_catalog()
    await _seed_eligibility()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body(variant_key="sku::other::v1"))
    assert resp.status_code == 409
    assert _error(resp) == "row_not_found"


async def test_no_variant_key_resolves_a_sole_variant(client):
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(variant_key=None))
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["variant_key"] == SKU_KEY


async def test_no_variant_key_on_a_multi_variant_product_is_refused(client):
    await _seed_catalog(second_variant=True)
    await _seed_eligibility()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body(variant_key=None))
    assert resp.status_code == 409
    assert _error(resp) == "row_not_found"


async def test_a_titleless_sole_variant_is_declared_rather_than_named(client):
    """`declared_single_variant` is carried as the ABSENCE of a variant_title — there is no column
    for the flag, and `start_purchase` refuses the inconsistent pair.

    The seed is an EMPTY title, not a NULL one: `catalog_skus.title` is `NOT NULL`, so a
    titleless variant in this catalogue is spelled `''`. That is the shape the route has to
    recognise, and a fixture that used NULL would be testing a row the catalog cannot hold.
    """
    await _seed_catalog(variant_title="")
    await _seed_eligibility()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["variant_title"] is None


# ── the buyer ref ────────────────────────────────────────────────────────────────────────────


async def test_the_buyer_ref_is_minted_once_and_reused(client):
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body())
    second = await client.post(f"{BASE}/purchases", json=_body())
    a = (await _purchase_row(first.json()["purchase_id"]))["buyer_ref"]
    b = (await _purchase_row(second.json()["purchase_id"]))["buyer_ref"]
    assert a == b
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 1


async def test_the_buyer_ref_is_not_the_buyer_id_and_not_the_agent_ref(client):
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    buyer_ref = (await _purchase_row(resp.json()["purchase_id"]))["buyer_ref"]
    assert buyer_ref not in (BUYER_ID, USER_REF, hash_agent_user_ref(USER_REF), AGENT)
    assert BUYER_ID not in buyer_ref
    assert len(buyer_ref) >= 20


async def test_two_buyers_get_two_refs(client):
    await _seed_catalog()
    await _seed_eligibility()
    await _seed_link()
    await _seed_link(ref=OTHER_USER_REF, buyer_id=OTHER_BUYER_ID)

    first = await client.post(f"{BASE}/purchases", json=_body())
    CALLER.agent_user_ref = OTHER_USER_REF
    second = await client.post(f"{BASE}/purchases", json=_body())

    a = (await _purchase_row(first.json()["purchase_id"]))["buyer_ref"]
    b = (await _purchase_row(second.json()["purchase_id"]))["buyer_ref"]
    assert a != b


async def test_one_buyer_reached_through_two_agents_keeps_one_ref(client):
    """An enrollment is a CARD. Scoping the ref to an agent would give one human two cards."""
    await _seed_catalog()
    await _seed_eligibility()
    await _seed_link()
    await _seed_link(agent_id=OTHER_AGENT, ref=OTHER_USER_REF, buyer_id=BUYER_ID)

    first = await client.post(f"{BASE}/purchases", json=_body())
    CALLER.agent_id = OTHER_AGENT
    CALLER.agent_user_ref = OTHER_USER_REF
    second = await client.post(f"{BASE}/purchases", json=_body())

    a = (await _purchase_row(first.json()["purchase_id"]))["buyer_ref"]
    b = (await _purchase_row(second.json()["purchase_id"]))["buyer_ref"]
    assert a == b


# ── the return url ───────────────────────────────────────────────────────────────────────────


async def test_a_return_url_on_a_host_that_is_not_ours_is_refused(client):
    """Reap sends a buyer's BROWSER here after they have entered a card. A caller-supplied host
    would turn this into an open redirector with a payment page in front of it."""
    await _seed_all()
    resp = await client.post(
        f"{BASE}/purchases", json=_body(return_url="https://evil.example/collect")
    )
    assert resp.status_code == 400
    assert _error(resp) == "invalid_return_url"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_a_return_url_with_userinfo_is_refused(client):
    """`https://agent.pivota.cc@evil.example/` has a HOSTNAME of evil.example."""
    await _seed_all()
    resp = await client.post(
        f"{BASE}/purchases", json=_body(return_url="https://agent.pivota.cc@evil.example/")
    )
    assert resp.status_code == 400
    assert _error(resp) == "invalid_return_url"


async def test_the_default_return_url_is_used_and_is_allowlisted(client):
    """The default is the API host's landing page (routes/reap_return.py), NOT agent.pivota.cc:
    nothing answers `/reap/return` there, so the old default sent every buyer who had just
    approved a payment to a 404. Pinned as the exact origin + path, because `startswith` on the
    host alone would pass a default that pointed at a page this backend does not serve."""
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(return_url=None))
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    stored = row["return_url"]
    assert stored == "https://api.pivota.cc/reap/return" \
        or stored.startswith("https://api.pivota.cc/reap/return?"), stored


async def test_an_operator_allowlist_still_picks_the_default_host(client, monkeypatch):
    """`REAP_RETURN_URL_HOSTS` keeps working exactly as before the default moved: its FIRST host
    is the default's host, and the shipped default list plays no part."""
    monkeypatch.setenv("REAP_RETURN_URL_HOSTS", "staging.pivota.cc,api.pivota.cc")
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(return_url=None))
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["return_url"].startswith("https://staging.pivota.cc/reap/return")


async def test_an_operator_configured_return_url_that_is_allowlisted_is_used(client, monkeypatch):
    """`REAP_AGENTIC_RETURN_URL` still overrides the code default, verbatim."""
    monkeypatch.setenv("REAP_AGENTIC_RETURN_URL", "https://agent.pivota.cc/landing?src=ops")
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(return_url=None))
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["return_url"].startswith("https://agent.pivota.cc/landing?src=ops")


async def test_an_operator_configured_return_url_is_validated_too(client, monkeypatch):
    """The env var is an override, not an exemption."""
    monkeypatch.setenv("REAP_AGENTIC_RETURN_URL", "https://evil.example/collect")
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(return_url=None))
    assert resp.status_code == 400
    assert _error(resp) == "invalid_return_url"


# ── idempotency ──────────────────────────────────────────────────────────────────────────────


async def test_the_same_idempotency_key_returns_the_same_purchase(client):
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    second = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    assert first.status_code == second.status_code == 202
    assert first.json()["purchase_id"] == second.json()["purchase_id"]
    assert await database.fetch_val(
        "SELECT COUNT(*) FROM reap_agentic_purchases WHERE state = 'resolving'"
    ) == 1


async def test_a_different_idempotency_key_starts_a_new_purchase(client):
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    second = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-2"))
    assert first.json()["purchase_id"] != second.json()["purchase_id"]


async def test_another_buyers_idempotency_key_is_not_this_buyers(client):
    """The key is scoped to the OWNERSHIP PAIR. Two end users of one agent will pick the same
    key, and one replaying into the other's purchase is a cross-buyer read."""
    await _seed_catalog()
    await _seed_eligibility()
    await _seed_link()
    await _seed_link(ref=OTHER_USER_REF, buyer_id=OTHER_BUYER_ID)

    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    CALLER.agent_user_ref = OTHER_USER_REF
    second = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    assert first.json()["purchase_id"] != second.json()["purchase_id"]


async def test_a_replay_reports_the_purchases_real_state(client):
    """Not a hardcoded 'resolving': by the time a caller retries the row may have moved, and the
    status field is the one the caller polls on."""
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    purchase_id = first.json()["purchase_id"]
    await ledger.transition(purchase_id, from_states=("resolving",), to_state="quoting")

    second = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    assert second.json()["purchase_id"] == purchase_id
    assert second.json()["status"] == "quoting"


async def test_an_old_idempotency_key_replays_the_same_purchase(client):
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    await database.execute(
        "UPDATE reap_agentic_purchase_keys SET created_at = :old",
        {"old": datetime.now(timezone.utc) - timedelta(hours=48)},
    )
    second = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    assert second.json()["purchase_id"] == first.json()["purchase_id"]


# ── ownership ────────────────────────────────────────────────────────────────────────────────


async def test_a_buyer_reads_their_own_purchase(client):
    await _seed_all()
    created = await client.post(f"{BASE}/purchases", json=_body())
    purchase_id = created.json()["purchase_id"]

    resp = await client.get(f"{BASE}/purchases/{purchase_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == purchase_id
    assert resp.json()["state"] == "resolving"


async def test_another_agent_cannot_read_this_purchase(client):
    """THE AGENT CONJUNCT. 404, not 403: a 403 confirms the id exists."""
    await _seed_all()
    created = await client.post(f"{BASE}/purchases", json=_body())
    purchase_id = created.json()["purchase_id"]

    CALLER.agent_id = OTHER_AGENT
    resp = await client.get(f"{BASE}/purchases/{purchase_id}")
    assert resp.status_code == 404
    assert _error(resp) == "purchase_not_found"


async def test_another_end_user_of_the_same_agent_cannot_read_this_purchase(client):
    """THE USER-REF CONJUNCT — the one an agent-only check would miss entirely. Same agent, same
    API key, different human."""
    await _seed_all()
    created = await client.post(f"{BASE}/purchases", json=_body())
    purchase_id = created.json()["purchase_id"]

    CALLER.agent_user_ref = OTHER_USER_REF
    resp = await client.get(f"{BASE}/purchases/{purchase_id}")
    assert resp.status_code == 404
    assert _error(resp) == "purchase_not_found"


async def test_a_purchase_that_does_not_exist_answers_the_same_as_one_that_is_not_yours(client):
    await _seed_all()
    resp = await client.get(f"{BASE}/purchases/rp_definitely_not_a_real_id")
    assert resp.status_code == 404
    assert _error(resp) == "purchase_not_found"


async def test_the_list_shows_only_this_buyers_purchases(client):
    """A get that leaks needs an id guessed first. A list that leaks hands the set over."""
    await _seed_catalog()
    await _seed_eligibility()
    await _seed_link()
    await _seed_link(ref=OTHER_USER_REF, buyer_id=OTHER_BUYER_ID)

    mine = await client.post(f"{BASE}/purchases", json=_body())
    CALLER.agent_user_ref = OTHER_USER_REF
    theirs = await client.post(f"{BASE}/purchases", json=_body())

    CALLER.agent_user_ref = USER_REF
    resp = await client.get(f"{BASE}/purchases")
    ids = [item["id"] for item in resp.json()["purchases"]]
    assert ids == [mine.json()["purchase_id"]]
    assert theirs.json()["purchase_id"] not in ids


async def test_the_list_shows_only_this_agents_purchases(client):
    await _seed_catalog()
    await _seed_eligibility()
    await _seed_link()
    await _seed_link(agent_id=OTHER_AGENT, ref=USER_REF, buyer_id=OTHER_BUYER_ID)

    mine = await client.post(f"{BASE}/purchases", json=_body())
    CALLER.agent_id = OTHER_AGENT
    theirs = await client.post(f"{BASE}/purchases", json=_body())

    CALLER.agent_id = AGENT
    resp = await client.get(f"{BASE}/purchases")
    ids = [item["id"] for item in resp.json()["purchases"]]
    assert ids == [mine.json()["purchase_id"]]
    assert theirs.json()["purchase_id"] not in ids


async def test_the_list_is_capped(client):
    await _seed_all()
    for _ in range(3):
        await client.post(f"{BASE}/purchases", json=_body())
    resp = await client.get(f"{BASE}/purchases?limit=2")
    assert resp.status_code == 200
    assert len(resp.json()["purchases"]) == 2
    assert resp.json()["limit"] == 2

    # FastAPI's own `le=` refusal, which the error envelope rewrites from 422 to 400 — the same
    # rewrite that is the reason this module's own refusals answer 400 in the first place.
    over = await client.get(f"{BASE}/purchases?limit=99999")
    assert over.status_code == 400


# ── what the read exposes, and what it must not ──────────────────────────────────────────────


async def _set(purchase_id: str, **values):
    sets = ", ".join(f"{key} = :{key}" for key in values)
    await database.execute(
        f"UPDATE reap_agentic_purchases SET {sets} WHERE id = :id",
        dict(values, id=purchase_id),
    )


async def _open(client) -> str:
    await _seed_all()
    created = await client.post(f"{BASE}/purchases", json=_body())
    assert created.status_code == 202
    return created.json()["purchase_id"]


async def test_the_read_carries_no_pii_and_no_identity(client):
    purchase_id = await _open(client)
    for resp in (
        await client.get(f"{BASE}/purchases/{purchase_id}"),
        await client.get(f"{BASE}/purchases"),
    ):
        text = resp.text
        for secret in PII_STRINGS:
            assert secret not in text
        for identity in (BUYER_ID, USER_REF, hash_agent_user_ref(USER_REF), AGENT):
            assert identity not in text


async def test_the_read_carries_no_reap_evidence_ids(client):
    """`reap_product_id`, `reap_variant_id`, `reap_quote_id` and `reap_checkout_id` are EVIDENCE.
    Handing them out would read as identity downstream, and Reap's ids change under a
    substitution."""
    purchase_id = await _open(client)
    await _set(
        purchase_id,
        reap_product_id="prd_x",
        reap_variant_id="var_x",
        reap_quote_id="q_x",
        reap_checkout_id="chk_x",
    )
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "reap_product_id" not in body
    assert "reap_variant_id" not in body
    assert "reap_quote_id" not in body
    assert "reap_checkout_id" not in body
    assert "buyer_ref" not in body
    assert "enrollment_id" not in body
    assert "click_id" not in body
    assert "return_url" not in body


async def test_the_totals_are_one_object_with_one_spelling_each(client):
    purchase_id = await _open(client)
    await _set(purchase_id, quoted_total_minor=4500, shipping_minor=100, tax_minor=150)
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert body["totals"] == {
        "currency": "USD",
        "our_price_minor": 4250,
        "quoted_total_minor": 4500,
        "final_total_minor": None,
        "shipping_minor": 100,
        "tax_minor": 150,
        "tax_included": None,
        "discount_minor": None,
    }
    for key in ("currency", "our_price_minor", "quoted_total_minor", "tax_minor"):
        assert key not in body


async def test_a_hosted_url_is_shown_while_the_buyer_needs_it(client):
    purchase_id = await _open(client)
    await ledger.transition(
        purchase_id,
        from_states=("resolving",),
        to_state="needs_enrollment",
        hosted_url="https://pay.prava.space/enroll/abc",
        hosted_url_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    purchase = await ledger.get_purchase_internal(purchase_id)
    enrollment = await ledger.upsert_pending_enrollment(buyer_ref=purchase["buyer_ref"], hosted_url=purchase["hosted_url"], hosted_url_expires_at=purchase["hosted_url_expires_at"])
    await _set(purchase_id, enrollment_id=enrollment["id"])
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert body["hosted_url"] == "https://pay.prava.space/enroll/abc"
    assert body["hosted_url_expires_at"]
    assert body["poll_after_seconds"] == svc.POLL_INTERVALS["needs_enrollment"]


async def test_a_hosted_url_on_a_host_we_do_not_vouch_for_is_never_forwarded(client):
    """DEFENCE IN DEPTH. The client refuses this URL on the way IN; this asserts the read refuses
    it too. `evilprava.space` passes a naive `endswith`, and this is a page where somebody types a
    card number."""
    purchase_id = await _open(client)
    await ledger.transition(
        purchase_id, from_states=("resolving",), to_state="needs_enrollment"
    )
    await _set(purchase_id, hosted_url="https://evilprava.space/enroll/steal")
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "hosted_url" not in body
    assert "evilprava" not in (await client.get(f"{BASE}/purchases/{purchase_id}")).text


async def test_an_http_hosted_url_is_never_forwarded(client):
    purchase_id = await _open(client)
    await ledger.transition(
        purchase_id, from_states=("resolving",), to_state="needs_enrollment"
    )
    await _set(purchase_id, hosted_url="http://pay.prava.space/enroll/abc")
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "hosted_url" not in body


async def test_an_expired_hosted_url_is_not_shown(client):
    purchase_id = await _open(client)
    await ledger.transition(
        purchase_id,
        from_states=("resolving",),
        to_state="needs_enrollment",
        hosted_url="https://pay.prava.space/enroll/abc",
        hosted_url_expires_at=datetime.now(timezone.utc) - timedelta(minutes=5),
    )
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "hosted_url" not in body
    assert "hosted_url_expires_at" not in body


# ── the approval deadline ────────────────────────────────────────────────────────────────────
#
# MEASURED 2026-09-25 in the Reap sandbox: an unapproved checkout is FAILED 1–10 s after the
# QUOTE's `expiresAt` (created + 5 min), while the hosted page's own `expiresAt` says created +
# 15 min. The buyer's real window is the earlier of the two, and the body says so explicitly.


def _t(**delta) -> datetime:
    return datetime.now(timezone.utc) + timedelta(**delta)


@pytest.mark.parametrize(
    "quote,hosted,expected",
    [
        pytest.param(_t(minutes=5), _t(minutes=15), "quote", id="quote earlier -> quote"),
        pytest.param(_t(minutes=15), _t(minutes=5), "hosted", id="page earlier -> page"),
        pytest.param(_t(minutes=5), None, "quote", id="quote only"),
        pytest.param(None, _t(minutes=15), "hosted", id="page only"),
        pytest.param(None, None, None, id="neither -> absent"),
    ],
)
def test_the_approval_deadline_is_the_earlier_of_the_two_expiries(quote, hosted, expected):
    got = routes_reap.approval_deadline(quote, hosted)
    if expected is None:
        assert got is None
    else:
        assert got == {"quote": quote, "hosted": hosted}[expected]
        assert got.tzinfo is not None, "comparable with the route's aware `_now()`"


def test_equal_expiries_give_that_instant():
    """The two clocks agreeing is not a third case."""
    same = _t(minutes=5)
    assert routes_reap.approval_deadline(same, same.replace()) == same


def test_the_deadline_reads_every_shape_a_timestamp_arrives_in():
    """SQLite hands the column back as text and a naive datetime is UTC by the ledger's rule.
    Either one skipped as "not a datetime" would be a deadline silently ignored, and an aware/
    naive comparison would raise inside `min`."""
    aware = datetime(2026, 9, 25, 12, 5, tzinfo=timezone.utc)
    assert routes_reap.approval_deadline("2026-09-25 12:05:00", aware.replace(tzinfo=None) + timedelta(minutes=10)) == aware
    assert routes_reap.approval_deadline(aware.replace(tzinfo=None), "2026-09-25 12:15:00") == aware
    assert routes_reap.approval_deadline("", None) is None


async def _awaiting_approval(purchase_id: str, *, quote, hosted) -> None:
    await ledger.transition(purchase_id, from_states=("resolving",), to_state="quoting")
    await ledger.transition(
        purchase_id,
        from_states=("quoting",),
        to_state="awaiting_approval",
        reap_quote_expires_at=quote,
        hosted_url="https://pay.prava.space/checkout/chk_7f3a",
        hosted_url_expires_at=hosted,
    )


async def test_an_awaiting_approval_row_names_the_quote_expiry_as_its_deadline(client):
    purchase_id = await _open(client)
    await _awaiting_approval(purchase_id, quote=_t(minutes=5), hosted=_t(minutes=15))
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert body["hosted_url"] == "https://pay.prava.space/checkout/chk_7f3a"
    assert body["approval_deadline"] == body["reap_quote_expires_at"], (
        "one spelling for one instant: the deadline is serialised exactly as the raw column is"
    )
    assert body["approval_deadline"] != body["hosted_url_expires_at"]
    assert body["approval_deadline"].endswith("+00:00"), "ISO-8601 with an explicit UTC offset"
    assert datetime.fromisoformat(body["approval_deadline"]).tzinfo is not None
    # both raw columns are still there, untouched
    assert body["reap_quote_expires_at"]
    assert body["hosted_url_expires_at"]


async def test_the_deadline_falls_back_to_the_page_when_nothing_was_quoted_with_an_expiry(client):
    purchase_id = await _open(client)
    await _awaiting_approval(purchase_id, quote=None, hosted=_t(minutes=15))
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert body["reap_quote_expires_at"] is None
    assert body["approval_deadline"] == body["hosted_url_expires_at"]


async def test_no_deadline_at_all_leaves_the_key_absent_and_the_link_live(client):
    purchase_id = await _open(client)
    await _awaiting_approval(purchase_id, quote=None, hosted=None)
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "approval_deadline" not in body
    assert body["hosted_url"] == "https://pay.prava.space/checkout/chk_7f3a"


async def test_a_lapsed_quote_drops_the_link_even_while_the_page_is_still_live(client):
    """THE DEFECT. The page says fifteen minutes; the checkout is FAILED at five. A link shown
    for the ten minutes in between is a link to a page that will not take the approval."""
    purchase_id = await _open(client)
    await _awaiting_approval(purchase_id, quote=_t(minutes=-1), hosted=_t(minutes=9))
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "hosted_url" not in body
    assert "hosted_url_expires_at" not in body
    assert body["state"] == "awaiting_approval"
    assert body["approval_deadline"] == body["reap_quote_expires_at"], (
        "surfaced even though it has passed: a past deadline beside a missing link is the honest "
        "reading of a row the poller has not flipped yet"
    )
    assert datetime.fromisoformat(body["approval_deadline"]) < datetime.now(timezone.utc)
    assert "prava.space" not in (await client.get(f"{BASE}/purchases/{purchase_id}")).text


async def test_a_lapsed_quote_drops_the_link_on_the_list_too(client):
    purchase_id = await _open(client)
    await _awaiting_approval(purchase_id, quote=_t(minutes=-1), hosted=_t(minutes=9))
    listed = (await client.get(f"{BASE}/purchases")).json()["purchases"]
    row = next(item for item in listed if item["id"] == purchase_id)
    assert "hosted_url" not in row
    assert row["approval_deadline"] == row["reap_quote_expires_at"]


async def test_a_live_quote_with_a_dead_page_still_drops_the_link(client):
    """CONTROL for the min rule in the other direction: the page's clock is not ignored."""
    purchase_id = await _open(client)
    await _awaiting_approval(purchase_id, quote=_t(minutes=4), hosted=_t(minutes=-1))
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "hosted_url" not in body
    assert body["approval_deadline"] < body["reap_quote_expires_at"]


async def test_the_deadline_is_an_awaiting_approval_field_only(client):
    """'needs_enrollment' has nothing quoted — the row re-quotes after the card is added — so it
    carries no approval deadline and does not consult the quote column at all."""
    purchase_id = await _open(client)
    await ledger.transition(
        purchase_id,
        from_states=("resolving",),
        to_state="needs_enrollment",
        hosted_url="https://pay.prava.space/enroll/abc",
        hosted_url_expires_at=_t(hours=1),
    )
    await _set(purchase_id, reap_quote_expires_at=ledger._bind_dt(_t(minutes=-30)))
    purchase = await ledger.get_purchase_internal(purchase_id)
    enrollment = await ledger.upsert_pending_enrollment(buyer_ref=purchase["buyer_ref"], hosted_url=purchase["hosted_url"], hosted_url_expires_at=purchase["hosted_url_expires_at"])
    await _set(purchase_id, enrollment_id=enrollment["id"])
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "approval_deadline" not in body
    assert body["hosted_url"] == "https://pay.prava.space/enroll/abc"

    await ledger.transition(purchase_id, from_states=("needs_enrollment",), to_state="quoting")
    await ledger.transition(
        purchase_id,
        from_states=("quoting",),
        to_state="awaiting_approval",
        reap_quote_expires_at=_t(minutes=5),
        hosted_url="https://pay.prava.space/checkout/chk_7f3a",
        hosted_url_expires_at=_t(minutes=15),
    )
    await ledger.transition(purchase_id, from_states=("awaiting_approval",), to_state="processing")
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "approval_deadline" not in body, "nothing left to approve on 'processing'"
    assert body["reap_quote_expires_at"], "the raw column is still reported"


async def test_a_hosted_url_is_not_shown_on_a_state_that_has_no_page(client):
    """A completed purchase whose approval link still worked would be a second charge waiting to
    happen."""
    purchase_id = await _open(client)
    await ledger.transition(
        purchase_id,
        from_states=("resolving",),
        to_state="needs_enrollment",
        hosted_url="https://pay.prava.space/enroll/abc",
    )
    await ledger.transition(purchase_id, from_states=("needs_enrollment",), to_state="quoting")
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert "hosted_url" not in body


async def test_an_order_reference_appears_only_when_the_purchase_is_complete(client):
    purchase_id = await _open(client)
    await ledger.transition(purchase_id, from_states=("resolving",), to_state="quoting")
    await ledger.transition(
        purchase_id, from_states=("quoting",), to_state="awaiting_approval", reap_order_id="ord_9"
    )
    assert "order_reference" not in (
        await client.get(f"{BASE}/purchases/{purchase_id}")
    ).json()

    await ledger.transition(
        purchase_id,
        from_states=("awaiting_approval",),
        to_state="completed",
        reap_order_id="ord_9",
        final_total_minor=4500,
    )
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert body["order_reference"] == "ord_9"
    assert body["totals"]["final_total_minor"] == 4500
    assert body["poll_after_seconds"] is None


# ── validation and PII in logs ───────────────────────────────────────────────────────────────


async def test_a_quantity_over_the_cap_is_refused(client):
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(quantity=svc.MAX_QUANTITY + 1))
    assert resp.status_code == 400
    assert _error(resp) == "invalid_request"


async def test_an_incomplete_address_is_refused_without_naming_a_value(client):
    await _seed_all()
    address = {key: value for key, value in ADDRESS.items() if key != "phone"}
    resp = await client.post(
        f"{BASE}/purchases", json=_body(buyer=_buyer(shipping_address=address))
    )
    assert resp.status_code == 400
    assert _error(resp) == "invalid_address"
    for secret in PII_STRINGS:
        assert secret not in resp.text


async def test_the_buyer_name_and_phone_are_fallbacks_not_overrides(client):
    await _seed_all()
    address = {
        key: value for key, value in ADDRESS.items() if key not in ("firstName", "lastName", "phone")
    }
    resp = await client.post(
        f"{BASE}/purchases",
        json=_body(
            buyer=_buyer(name="Ada Lovelace", phone="+15550100", shipping_address=address)
        ),
    )
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    stored = ledger._decode_json(row["shipping_address"])
    assert stored["firstName"] == "Ada"
    assert stored["lastName"] == "Lovelace"
    assert stored["phone"] == "+15550100"


async def test_an_address_that_names_a_recipient_wins_over_the_buyer_name(client):
    await _seed_all()
    resp = await client.post(
        f"{BASE}/purchases",
        json=_body(buyer=_buyer(name="Somebody Else")),
    )
    assert resp.status_code == 202
    stored = ledger._decode_json((await _purchase_row(resp.json()["purchase_id"]))["shipping_address"])
    assert stored["firstName"] == "Ada"
    assert stored["lastName"] == "Lovelace"


async def test_a_bad_email_is_refused(client):
    await _seed_all()
    resp = await client.post(
        f"{BASE}/purchases",
        json=_body(buyer=_buyer(email="not-an-email")),
    )
    assert resp.status_code == 400
    assert _error(resp) == "invalid_request"


#: Loggers this repo does not own. `aiosqlite` and `databases` log every statement WITH ITS BOUND
#: PARAMETERS at DEBUG, and on this table those parameters are the buyer's address and email — so
#: an unfiltered scan of `caplog` is a scan of the SQLite driver's debug output, not of our code.
#: That is a real property of the test harness and not of production (asyncpg does not do it, and
#: nothing runs these loggers at DEBUG in prod), and folding it into the assertion would make this
#: test permanently red for a reason nobody can fix here. The claim being made is "OUR code does
#: not log the buyer", so the detector looks at our code's records.
_DRIVER_LOGGERS = ("aiosqlite", "databases", "sqlalchemy", "httpx", "httpcore", "asyncio")


def _ours(record: logging.LogRecord) -> bool:
    return not record.name.startswith(_DRIVER_LOGGERS)


async def test_nothing_on_this_path_logs_the_buyer(client, caplog):
    await _seed_all()
    with caplog.at_level(logging.DEBUG):
        created = await client.post(f"{BASE}/purchases", json=_body())
        await client.get(f"{BASE}/purchases/{created.json()['purchase_id']}")
        await client.get(f"{BASE}/purchases")

    ours = [record for record in caplog.records if _ours(record)]
    # THE CONTROL. An absence assertion passes when the mechanism is absent too: if the filter
    # above ever matched everything, the loop below would be checking an empty string and would
    # go on passing after somebody started logging the buyer's email.
    assert ours, "precondition: this path logged something of ours to look at"
    assert any(
        "reap_agentic" in record.getMessage() for record in ours
    ), "precondition: the rail's own log line is among the records being scanned"

    logged = "\n".join(record.getMessage() for record in ours)
    for secret in PII_STRINGS:
        assert secret not in logged


async def test_an_extra_address_key_cannot_widen_what_is_stored(client):
    """The model drops it and `build_shipping_address` drops it again. A caller cannot add a field
    to what reaches a third party."""
    await _seed_all()
    address = dict(ADDRESS, ssn="000-00-0000", note="deliver to the neighbour")
    resp = await client.post(
        f"{BASE}/purchases", json=_body(buyer=_buyer(shipping_address=address))
    )
    assert resp.status_code == 202
    stored = ledger._decode_json((await _purchase_row(resp.json()["purchase_id"]))["shipping_address"])
    assert "ssn" not in stored
    assert "note" not in stored


# ── the module's own shape ───────────────────────────────────────────────────────────────────


def test_the_route_never_reads_the_unscoped_ledger_read():
    """`get_purchase_internal` is the UNSCOPED, UNREDACTED read. It is named `_internal` precisely
    because it is the one a route author reaches for, and the difference between it and
    `get_purchase_for_owner` is the whole access-control story on this rail. A grep, because the
    failure is a call that compiles."""
    source = Path(routes_reap.__file__).read_text(encoding="utf-8")
    assert "get_purchase_internal" not in source


def test_every_refusal_reason_the_module_raises_has_a_status():
    """A reason with no entry falls to the 409 default, which is a defensible answer but not a
    chosen one. This pins that every reason this module writes was actually decided on."""
    source = Path(routes_reap.__file__).read_text(encoding="utf-8")
    import re

    raised = set(re.findall(r'PurchaseRefused\(\s*"([a-z_]+)"', source))
    assert raised, "precondition: the module raises some refusals"
    assert raised <= set(routes_reap._REFUSAL_STATUS), (
        f"no status decided for: {sorted(raised - set(routes_reap._REFUSAL_STATUS))}"
    )


# ── the offer seller ─────────────────────────────────────────────────────────────────────────


async def _seed_competitor_offer(*, price: str = "1.00", merchant_id: str = "m_competitor"):
    """A SECOND SELLER's offer on the SAME sku, cheaper. `catalog_offers.merchant_id` is the offer
    seller and is a different column from `catalog_products.merchant_id`."""
    await database.execute(
        """
        INSERT INTO catalog_offers (offer_id, sku_key, product_key, merchant_id,
                                    currency, merchant_effective_price)
        VALUES (:oid, :sk, :pk, :mid, 'USD', :price)
        """,
        {
            "oid": f"off_{merchant_id}",
            "sk": SKU_KEY,
            "pk": PRODUCT_KEY,
            "mid": merchant_id,
            "price": price,
        },
    )


async def test_a_competing_sellers_cheaper_offer_is_not_our_price(client):
    """THE P0. Without `o.merchant_id = p.merchant_id` this query was `ORDER BY price ASC LIMIT 1`
    across every seller on the sku, so a competitor's 1.00 became `our_price_minor`. The buyer
    would then be sent through card enrolment and refused `price_changed` at the quote — for a
    price change that never happened."""
    await _seed_all()
    await _seed_competitor_offer(price="1.00")

    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["our_price_minor"] == 4250, (
        "the price came from a seller who is not the merchant this purchase is with"
    )


async def test_a_stale_mirror_copy_of_the_same_sku_is_not_our_price(client):
    """The other shape of the same defect: one sku carrying two offer rows because the product was
    both mirrored into the crawl lane and synced by the merchant. The cheaper copy is often the
    stale one, and `ORDER BY price ASC` picks it every time."""
    await _seed_all()
    await _seed_competitor_offer(price="28.20", merchant_id="m_brand_external_seed")

    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["our_price_minor"] == 4250


async def test_a_merchant_with_no_offer_of_its_own_is_unpriced(client):
    """Not "somebody else will sell it to you". The eligible merchant has no offer on this sku, so
    there is no price we are entitled to commit to."""
    await _seed_catalog(priced=False)
    await _seed_eligibility()
    await _seed_link()
    await _seed_competitor_offer(price="9.99")

    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "row_unpriced"


# ── the market's currency ────────────────────────────────────────────────────────────────────


async def test_an_offer_priced_in_another_currency_is_refused(client):
    """A US buyer against a EUR-priced offer. Every other check here passes; the divergence would
    surface at the quote as `price_changed`, which names the wrong cause and arrives after the
    buyer has entered a card."""
    await _seed_catalog(currency="EUR")
    await _seed_eligibility(market="US")
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409
    assert _error(resp) == "row_currency_mismatch"


async def test_a_market_the_currency_map_does_not_know_fails_closed(client):
    """Adding a market is a one-line change in `_MARKET_CURRENCY`. Until somebody makes it, the
    rail refuses rather than skipping the check — the same direction every other decision here
    takes."""
    assert "ZZ" not in routes_reap._MARKET_CURRENCY, "precondition: ZZ is not a known market"
    await _seed_catalog(currency="USD")
    await _seed_eligibility(market="ZZ")
    await _seed_link()
    address = dict(ADDRESS, country="ZZ")
    resp = await client.post(
        f"{BASE}/purchases", json=_body(buyer=_buyer(shipping_address=address))
    )
    assert resp.status_code == 409
    assert _error(resp) == "row_currency_mismatch"


async def test_a_matching_market_currency_is_accepted(client):
    """The control: an absence assertion passes when the mechanism is absent too."""
    await _seed_catalog(currency="USD")
    await _seed_eligibility(market="US")
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202


# ── unprintable characters in the route-owned identifiers ────────────────────────────────────


@pytest.mark.parametrize(
    "field,value",
    [
        ("merchant_domain", "brand.example\x00"),
        ("product_key", PRODUCT_KEY + "\x00"),
        ("variant_key", SKU_KEY + "\x00"),
        ("idempotency_key", "k-1\x00"),
    ],
)
async def test_a_nul_byte_in_an_identifier_is_refused(client, field, value):
    """REFUSED, not stripped. These four are matched EXACTLY against storage, so removing a
    character would not sanitise the value — it would turn it into a different identifier. On
    Postgres an unstripped NUL reached an asyncpg bind and raised
    `CharacterNotInRepertoireError`, which is not a `PurchaseRefused` and became a 500; see the
    Postgres arm, which is the only one that can observe that."""
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(**{field: value}))
    assert resp.status_code == 400
    assert _error(resp) == "invalid_request"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_an_overlong_identifier_is_refused_rather_than_truncated(client):
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain="a" * 300))
    assert resp.status_code == 400
    assert _error(resp) == "invalid_request"


# ── idempotency is a key PLUS the request it was used for ────────────────────────────────────


async def test_the_same_key_on_a_different_body_is_a_conflict(client):
    """A key alone answers "have I seen this key?", and every caller reads the 202 as "your
    request was carried out". A client that derives keys from a session or a cart id WILL reuse
    one across two bodies, and the old code answered 202 naming a purchase of something else."""
    await _seed_catalog(second_variant=True)
    await _seed_eligibility()
    await _seed_link()

    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    assert first.status_code == 202

    second = await client.post(
        f"{BASE}/purchases", json=_body(idempotency_key="k-1", quantity=3)
    )
    assert second.status_code == 409
    assert _error(second) == "idempotency_conflict"

    third = await client.post(
        f"{BASE}/purchases", json=_body(idempotency_key="k-1", variant_key=SECOND_SKU_KEY)
    )
    assert _error(third) == "idempotency_conflict"

    address = dict(ADDRESS, addressLine1="1 Other St")
    fourth = await client.post(
        f"{BASE}/purchases",
        json=_body(idempotency_key="k-1", buyer=_buyer(shipping_address=address)),
    )
    assert _error(fourth) == "idempotency_conflict"

    # ...and none of them opened anything.
    assert await database.fetch_val(
        "SELECT COUNT(*) FROM reap_agentic_purchases WHERE state = 'resolving'"
    ) == 1


async def test_a_cosmetically_different_but_identical_request_still_replays(client):
    """The hash is built from the NORMALISED values, so a retry that differs only in the casing of
    a domain — or supplies the recipient through `buyer.name` instead of the address, resolving to
    the same recipient — is the same request and must replay rather than conflict."""
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))

    address = {
        key: value for key, value in ADDRESS.items() if key not in ("firstName", "lastName")
    }
    second = await client.post(
        f"{BASE}/purchases",
        json=_body(
            idempotency_key="k-1",
            merchant_domain=DOMAIN.upper(),
            buyer=_buyer(name="Ada Lovelace", shipping_address=address),
        ),
    )
    assert second.status_code == 202
    assert second.json()["purchase_id"] == first.json()["purchase_id"]


async def test_the_recorded_hash_is_what_the_conflict_compares(client):
    await _seed_all()
    await client.post(f"{BASE}/purchases", json=_body(idempotency_key="k-1"))
    stored = await database.fetch_val(
        "SELECT request_hash FROM reap_agentic_purchase_keys WHERE idempotency_key = 'k-1'"
    )
    assert isinstance(stored, str) and len(stored) == 64


# ── the dark rail is not probeable through a validator ───────────────────────────────────────


async def test_a_dark_rail_answers_404_for_every_shape_of_bad_input(client, monkeypatch):
    """THE PROBE. A parameter in the handler signature is validated by FastAPI BEFORE the handler
    runs, so a malformed body used to answer 400 with a field message while every valid request
    answered 404 — a working existence oracle for a feature that is supposed to be absent. Every
    answer below must be byte-for-byte the one a well-formed request gets."""
    await _seed_all()
    monkeypatch.delenv("REAP_AGENTIC_ENABLED", raising=False)

    baseline = await client.post(f"{BASE}/purchases", json=_body())
    assert baseline.status_code == 404
    expected = _error(baseline)

    probes = [
        await client.post(f"{BASE}/purchases", json=[1, 2]),
        await client.post(f"{BASE}/purchases", json={"nonsense": True}),
        await client.post(f"{BASE}/purchases", content=b"not json at all"),
        await client.post(
            f"{BASE}/purchases",
            content=b"merchant_domain=brand.example",
            headers={"content-type": "application/x-www-form-urlencoded"},
        ),
        await client.get(f"{BASE}/purchases?limit=500"),
        await client.get(f"{BASE}/purchases?limit=not-a-number"),
    ]
    for probe in probes:
        assert probe.status_code == 404, probe.text
        assert _error(probe) == expected == "not_available_on_this_rail"


async def test_an_over_large_limit_is_refused_when_the_rail_is_live(client):
    """The control for the probe test: with the dial ON, the bound is still enforced — and as a
    refusal rather than a silent clamp, because a caller that asked for 500 and got 20 has been
    handed a page it does not know is partial."""
    await _seed_all()
    for resp in (
        await client.get(f"{BASE}/purchases?limit=500"),
        await client.get(f"{BASE}/purchases?limit=0"),
        await client.get(f"{BASE}/purchases?limit=not-a-number"),
    ):
        assert resp.status_code == 400
        assert _error(resp) == "invalid_request"


async def test_a_malformed_body_is_refused_when_the_rail_is_live(client):
    await _seed_all()
    for resp in (
        await client.post(f"{BASE}/purchases", json=[1, 2]),
        await client.post(f"{BASE}/purchases", content=b"not json at all"),
    ):
        assert resp.status_code == 400
        assert _error(resp) == "invalid_request"


# ── the self-heal survives its own worst statement ───────────────────────────────────────────


async def test_a_failing_unique_index_does_not_starve_the_keys_table():
    """ONE try PER STATEMENT. The block's own comment names `uq_reap_agentic_buyer_refs_ref` as
    the statement that fails in the field — on a database already holding two buyers with one ref
    — and `reap_agentic_purchase_keys` used to sit after it in the SAME try. Production has no
    other route to that table, so the raise would have starved it forever on exactly the databases
    that were already unwell."""
    for table in RAIL_TABLES:
        await database.execute(f"DROP TABLE IF EXISTS {table}")
    # A buyer-refs table WITHOUT its unique index, holding the rows that make the index
    # impossible. This is the shape the self-heal has to survive.
    await database.execute(
        """
        CREATE TABLE reap_agentic_buyer_refs (
            buyer_id VARCHAR(50) PRIMARY KEY,
            reap_buyer_ref VARCHAR(128) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    for buyer in ("b1", "b2"):
        await database.execute(
            "INSERT INTO reap_agentic_buyer_refs (buyer_id, reap_buyer_ref) "
            "VALUES (:b, 'collides')",
            {"b": buyer},
        )

    await ensure_required_schema_light()

    # The index could not be created — that is the precondition, not the failure.
    assert await database.fetch_val(
        "SELECT COUNT(*) FROM sqlite_master "
        "WHERE type = 'index' AND name = 'uq_reap_agentic_buyer_refs_ref'"
    ) == 0, "precondition: the unique index really is impossible on this data"

    # ...and every other statement in the block still landed.
    for table in ("reap_agentic_eligibility", "reap_agentic_purchase_keys"):
        assert await database.fetch_val(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = :t",
            {"t": table},
        ) == 1, f"{table} was starved by the failing index"


# ── mig 233: the same consent on the purchase row, in the same request ───────────────────────


async def test_the_route_writes_the_same_consent_to_both_stores_in_one_request(client):
    """THE PROPERTY MIGRATION 233 IS FOR, at the seam where it can actually go wrong.

    The route validates `buyer.consent_version` ONCE and has to hand the SAME string to two
    writers: `_reap_buyer_ref`, which records the buyer's LATEST consent, and `start_purchase`,
    which records the consent THIS purchase was opened under. A mutant that passes a different
    value to one of them — a re-read of `req.buyer.consent_version`, a literal, a stale local —
    produces two stores that disagree about an act a human performed, and every existing test
    still passes because each store is individually populated.
    """
    await _seed_catalog()
    await _seed_eligibility()

    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 202
    purchase_id = resp.json()["purchase_id"]

    on_the_buyer = await database.fetch_val(
        "SELECT consent_version FROM reap_agentic_buyer_refs"
    )
    row = await database.fetch_one(
        "SELECT consent_version, consented_at FROM reap_agentic_purchases WHERE id = :i",
        {"i": purchase_id},
    )
    on_the_purchase = dict(row)

    assert on_the_purchase["consent_version"] == CONSENT
    assert on_the_purchase["consented_at"] is not None
    assert on_the_purchase["consent_version"] == on_the_buyer, (
        "the route wrote a different consent tag to the purchase than to the buyer identity"
    )


async def test_a_refused_purchase_writes_no_consent_anywhere(client):
    """The control: `consent_required` is decided before either write, so neither store gets a
    row. Without this, the test above would pass on a build that wrote both stores from a
    request that should have been refused outright."""
    await _seed_catalog()
    await _seed_eligibility()

    resp = await client.post(
        f"{BASE}/purchases", json=_body(buyer=_buyer(consent_version=""))
    )
    assert resp.status_code == 400
    assert _error(resp) == "consent_required"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_buyer_refs") == 0


async def test_the_owner_read_hands_back_the_consent_this_purchase_was_opened_under(client):
    """IN the public view. `_public_body` is built from `PUBLIC_PURCHASE_COLUMNS` and never reads
    a column itself, so this is what the allowlist decided — and it is the end-to-end reading of
    that decision."""
    await _seed_catalog()
    await _seed_eligibility()

    purchase_id = (await client.post(f"{BASE}/purchases", json=_body())).json()["purchase_id"]

    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert body["consent_version"] == CONSENT
    assert body["consented_at"] is not None

    listed = (await client.get(f"{BASE}/purchases")).json()["purchases"]
    assert listed[0]["consent_version"] == CONSENT


async def test_a_replay_moves_the_buyers_tag_but_not_the_purchases(client):
    """THE TWO STORES MEAN DIFFERENT THINGS, and this is the request where the difference shows.

    A retry under the same idempotency key with a NEWER tag replays to the original purchase and
    rewrites the buyer's latest consent — both already tested. What 233 adds is that the PURCHASE
    keeps the tag it was actually opened under, because it is evidence rather than current state.
    A mutant that made the purchase's copy follow the buyer's would pass every test that came
    before this one.
    """
    await _seed_catalog()
    await _seed_eligibility()

    first = await client.post(
        f"{BASE}/purchases", json=_body(idempotency_key="k-233-replay")
    )
    purchase_id = first.json()["purchase_id"]

    again = await client.post(
        f"{BASE}/purchases",
        json=_body(idempotency_key="k-233-replay", buyer=_buyer(consent_version="v-later")),
    )
    assert again.json()["purchase_id"] == purchase_id

    assert await database.fetch_val(
        "SELECT consent_version FROM reap_agentic_buyer_refs"
    ) == "v-later"
    assert await database.fetch_val(
        "SELECT consent_version FROM reap_agentic_purchases WHERE id = :i", {"i": purchase_id}
    ) == CONSENT


# ── the merchant domain is matched canonically ──────────────────────────────────────────────
#
# Production's Shopify rows spell `catalog_products.source_domain` as Shopify's `shop_domain`,
# `www.Brand.com`; operators write eligibility rows as `brand.com`; the gateway sends the host
# it observed, which is either. One canonical form — lower case, ONE leading `www.` removed — on
# BOTH sides of every comparison the variant lane makes. The spellings below are chosen so each
# side can only match if IT is the side doing the folding.

WWW_PRODUCT_DOMAIN = "www.Brand.example"  # the Shopify spelling, mixed case and all
CANONICAL_DOMAIN = "brand.example"


async def _seed_www_world(*, product_domain: str = WWW_PRODUCT_DOMAIN,
                          eligibility_domain: str = CANONICAL_DOMAIN):
    await _seed_catalog(domain=product_domain)
    await _seed_eligibility(domain=eligibility_domain)


@pytest.mark.parametrize("requested", ["www.brand.example", "brand.example", "WWW.BRAND.EXAMPLE"])
async def test_every_spelling_of_one_merchant_reaches_the_same_row(client, requested):
    """Product `www.Brand.example`, eligibility `brand.example`, and three request spellings:
    all three pass eligibility and all three find the same product row at the same price. The
    purchase keeps the host as observed (lowercased): its one reader folds it itself."""
    await _seed_www_world()
    resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain=requested))
    assert resp.status_code == 202, (requested, resp.text)
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["product_key"] == PRODUCT_KEY
    assert row["variant_key"] == SKU_KEY
    assert row["our_price_minor"] == 4250
    assert row["merchant_domain"] == DOMAIN


async def test_an_eligibility_row_typed_with_the_www_still_matches(client):
    """THE ELIGIBILITY SIDE FOLDS TOO. The runbook says to write rows canonical; a row somebody
    typed as `www.Brand.example` must not silently refuse every purchase for the merchant."""
    await _seed_www_world(eligibility_domain="www.Brand.example")
    resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain="brand.example"))
    assert resp.status_code == 202, resp.text


async def test_a_www_with_no_dot_after_it_is_part_of_the_name(client):
    """`wwwbrand.example` is a different store. Only `www.` followed by the dot is a prefix."""
    await _seed_www_world(product_domain="brand.example")
    resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain="wwwbrand.example"))
    assert resp.status_code == 409
    assert _error(resp) == "merchant_not_eligible"


async def test_a_product_under_a_www_less_lookalike_is_not_this_merchants(client):
    """The product side: a catalog row under `wwwbrand.example` is not `brand.example`'s, and the
    answer is the same `row_not_found` as a product that does not exist."""
    await _seed_www_world(product_domain="wwwbrand.example")
    resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain="brand.example"))
    assert resp.status_code == 409
    assert _error(resp) == "row_not_found"


async def test_only_one_www_is_stripped_from_the_request(client):
    """`www.www.brand.example` is `www.brand.example`, not `brand.example`."""
    await _seed_www_world(product_domain="brand.example")
    resp = await client.post(
        f"{BASE}/purchases", json=_body(merchant_domain="www.www.brand.example")
    )
    assert resp.status_code == 409
    assert _error(resp) == "merchant_not_eligible"


async def test_only_one_www_is_stripped_from_either_stored_column(client):
    """The SQL folds exactly one `www.` too: a double-`www.` product and a `www.`-prefixed
    eligibility row are the merchant `www.brand.example` — and never `brand.example`."""
    await _seed_www_world(
        product_domain="www.www.brand.example", eligibility_domain="www.www.brand.example"
    )
    resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain="brand.example"))
    assert resp.status_code == 409
    assert _error(resp) == "merchant_not_eligible"

    resp = await client.post(
        f"{BASE}/purchases", json=_body(merchant_domain="www.www.brand.example")
    )
    assert resp.status_code == 202, resp.text
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["product_key"] == PRODUCT_KEY


@pytest.mark.parametrize(
    "value",
    [
        "https://brand.example/",
        "brand.example:443",
        "user@brand.example",
        "brand.example/path",
        "127.0.0.1",
        "localhost",
        "www.example",  # one label once the `www.` is gone
        "brand..example",
        "brand.example.",  # a trailing dot: the SQL fold never strips one, so it names nothing
        "brand.example..",
        "www.brand.example.",
    ],
)
async def test_a_merchant_domain_that_is_not_a_bare_host_is_refused_before_any_read(
    client, value
):
    """Not reduced to a host — refused, before eligibility, the catalog or a buyer mint. The
    value is not echoed back."""
    await _seed_catalog()
    await _seed_eligibility()
    resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain=value))
    assert resp.status_code == 400, (value, resp.text)
    assert _error(resp) == "invalid_request"
    assert value not in resp.text
    assert await database.fetch_val("SELECT COUNT(*) FROM buyer_identity_links") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


@pytest.mark.parametrize("disabled_first", [True, False], ids=["disabled_first", "enabled_first"])
@pytest.mark.parametrize(
    "disabled_spelling", ["brand.example", "www.brand.example"], ids=["bare_off", "www_off"]
)
async def test_a_disabled_twin_spelling_turns_the_merchant_off(
    client, disabled_spelling, disabled_first
):
    """Two merchant rows for one canonical merchant, one of them disabled: refused, whatever
    order the database returns them in. Turning a merchant off is an UPDATE against whichever
    spelling the operator has in front of them.

    FOUR CASES, because one is not a test of order-independence: which spelling is off x which
    was inserted first. Whatever order the engine returns the rows in — insertion order, or the
    primary key's (`brand.example` sorts before `www.brand.example`) — at least one case puts the
    ENABLED row last and one puts it first, so "the last row wins" and "the first row wins" each
    reach a 202 somewhere and die."""
    enabled_spelling = (
        "www.brand.example" if disabled_spelling == "brand.example" else "brand.example"
    )
    await _seed_catalog(domain=WWW_PRODUCT_DOMAIN)
    rows = [(disabled_spelling, False), (enabled_spelling, True)]
    for domain, enabled in (rows if disabled_first else list(reversed(rows))):
        await _seed_eligibility(domain=domain, enabled=enabled)
    for requested in ("brand.example", "www.brand.example"):
        resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain=requested))
        assert resp.status_code == 409, (requested, resp.text)
        assert _error(resp) == "merchant_disabled"


async def test_the_offer_seller_is_still_a_conjunct_under_the_www_spelling(client):
    """Canonical matching must not widen the offer read: a competitor's cheaper offer on the same
    sku is still not our price."""
    await _seed_www_world()
    await _seed_competitor_offer(price="1.00")
    resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain="www.brand.example"))
    assert resp.status_code == 202, resp.text
    row = await _purchase_row(resp.json()["purchase_id"])
    assert row["our_price_minor"] == 4250


async def test_the_202_body_is_unchanged_by_canonical_matching(client):
    """A SNAPSHOT of the accepted body: the same five keys and the same values, whichever
    spelling was sent. Only `purchase_id` varies, and only in value."""
    await _seed_www_world()
    bodies = []
    for spelling in ("www.brand.example", "brand.example"):
        resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain=spelling))
        assert resp.status_code == 202, resp.text
        bodies.append(resp.json())
    for body in bodies:
        assert body["purchase_id"].startswith("rp_")
        assert {k: v for k, v in body.items() if k != "purchase_id"} == {
            "status": "resolving",
            "poll_after_seconds": svc.POLL_INTERVALS["resolving"],
            "checkout_dispatch_state": "not_dispatched",
            "contact_reentry_required": False,
        }
        assert sorted(body) == ["checkout_dispatch_state", "contact_reentry_required", "poll_after_seconds", "purchase_id", "status"]
        assert body["checkout_dispatch_state"] == "not_dispatched"
        assert body["contact_reentry_required"] is False


async def test_a_retry_in_the_other_spelling_replays_the_same_purchase(client):
    """The idempotency hash carries the canonical merchant: `www.` and apex are one purchase."""
    await _seed_www_world()
    first = await client.post(
        f"{BASE}/purchases",
        json=_body(merchant_domain="www.brand.example", idempotency_key="k-www-apex"),
    )
    again = await client.post(
        f"{BASE}/purchases",
        json=_body(merchant_domain="Brand.Example", idempotency_key="k-www-apex"),
    )
    assert first.status_code == again.status_code == 202, again.text
    assert again.json()["purchase_id"] == first.json()["purchase_id"]


def _positive_preflight(host: str):
    from services.shopify_cart_link_preflight import PreflightResult, Verdict

    return PreflightResult(
        host=host,
        verdict=Verdict.ELIGIBLE,
        retryable=False,
        market="US",
        variant_id="50041364447509",
        variant_source="caller",
        payment_methods=("Airwallex",),
        card_available=True,
        landed_price_minor=4250,
        landed_currency="USD",
        final_status=200,
        final_host=host,
        chain=((302, f"https://{host}/cart/1:1"), (200, f"https://{host}/checkouts/cn/T")),
    )


async def _record_sweep_facts() -> int:
    """Write a positive fact for every merchant the SWEEP would visit, keyed exactly the way the
    sweep keys it (its own `load_population`), so the route is tested against the writer's key
    and not against a key this test chose."""
    import db.merchant_purchasability as facts
    import jobs.merchant_purchasability_sweep as sweep

    targets = await sweep.load_population(50)
    for target in targets:
        assert await facts.record_check(
            target.domain, target.market, _positive_preflight(target.domain)
        ) is not None
    return len(targets)


@pytest.mark.parametrize(
    "eligibility_domain, product_domain, requested",
    [
        (CANONICAL_DOMAIN, WWW_PRODUCT_DOMAIN, "WWW.BRAND.EXAMPLE"),
        # The case that tells the canonical merchant from the observed host. The sweep folds the
        # row `www.www.brand.example` to `www.brand.example` and `record_check` folds that again,
        # to `brand.example`. `is_purchasable` handed the canonical `www.brand.example` folds to
        # the same key; handed the observed `www.www.brand.example` it reads `www.brand.example`
        # and misses.
        ("www.www.brand.example", "www.www.brand.example", "www.www.brand.example"),
    ],
)
async def test_the_purchasability_lookup_is_keyed_as_the_sweep_keys_it(
    client, monkeypatch, eligibility_domain, product_domain, requested
):
    monkeypatch.setenv("MERCHANT_PURCHASABILITY_ENFORCE", "1")
    monkeypatch.delenv("MERCHANT_PURCHASABILITY_BUYER_VANTAGE", raising=False)
    await database.execute("DELETE FROM merchant_purchasability")
    try:
        await _seed_www_world(product_domain=product_domain, eligibility_domain=eligibility_domain)
        refused = await client.post(f"{BASE}/purchases", json=_body(merchant_domain=requested))
        assert refused.status_code == 409
        assert _error(refused) == "merchant_not_purchasable", "control: no fact yet"

        assert await _record_sweep_facts() == 1
        resp = await client.post(f"{BASE}/purchases", json=_body(merchant_domain=requested))
        assert resp.status_code == 202, resp.text
    finally:
        await database.execute("DELETE FROM merchant_purchasability")


async def test_the_cart_link_lane_keeps_the_host_it_was_given(client, monkeypatch):
    """NOT canonicalised, deliberately: the permalink's host and the storefront evidence's host
    are compared byte-for-byte downstream, so this lane reads the catalog under — and builds its
    URL on — the host the door observed."""
    await _seed_tierb_shopify_item()
    await database.execute(
        "UPDATE catalog_products SET source_domain = 'www.brand.example' WHERE product_key = :pk",
        {"pk": PRODUCT_KEY},
    )
    await _seed_tierb_verdict()  # keyed `brand.example`; its reader folds the `www.` itself
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(
        f"{BASE}/purchases",
        json=_body(item_source="cart_link", merchant_domain="www.brand.example"),
    )
    assert resp.status_code == 202, resp.text
    purchase = await _purchase_row(resp.json()["purchase_id"])
    assert purchase["cart_url"].startswith("https://www.brand.example/cart/50041364447509:1?")
    assert purchase["merchant_domain"] == "www.brand.example"


async def test_the_sql_fold_agrees_with_the_python_canonicaliser():
    """The two halves of one rule, run against each other on the database this suite uses. The
    expression is lifted OUT OF THE ROUTE'S OWN STATEMENTS, so a divergence between them — or
    between either and the Python canonicaliser — fails here by name."""
    import re as _re

    pattern = _re.compile(
        r"CASE WHEN lower\((?P<col>[\w.]+)\) LIKE 'www\.%'\s+THEN substr\(lower\((?P=col)\), 5\)"
        r"\s+ELSE lower\((?P=col)\) END = :merchant_domain"
    )
    found = []
    for statement in (routes_reap._ELIGIBILITY_SQL, routes_reap._PRODUCT_SQL):
        match = pattern.search(statement)
        assert match, "the canonical fold is missing from a statement the variant lane matches on"
        text = match.group(0).split(" = :merchant_domain")[0]
        found.append(" ".join(text.replace(match.group("col"), "COL").split()))
    assert found[0] == found[1], "the two statements fold the domain differently"

    expression = found[0].replace("COL", ":h")
    for host in (
        "brand.example", "www.brand.example", "WWW.Brand.Example", "www.www.brand.example",
        "wwwbrand.example", "shop.brand.example", "www-brand.example",
    ):
        folded = await database.fetch_val(f"SELECT {expression}", {"h": host})
        assert folded == routes_reap.canonical_merchant_domain(host), host


# ── offer codes (2026-09-28) ─────────────────────────────────────────────────────────────────


async def test_an_offer_code_is_forwarded_stored_and_read_back(client):
    await _seed_all()
    created = await client.post(f"{BASE}/purchases", json=_body(offer_code="peachie20"))
    assert created.status_code == 202, created.text
    purchase_id = created.json()["purchase_id"]
    assert (await _purchase_row(purchase_id))["offer_code"] == "peachie20"  # case kept
    body = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    assert body["offer_code"] == "peachie20" and body["offer_code_outcome"] is None
    assert body["totals"]["discount_minor"] is None


async def test_no_offer_code_stores_none(client):
    purchase_id = await _open(client)
    assert (await _purchase_row(purchase_id))["offer_code"] is None


@pytest.mark.parametrize("bad", ["", "   ", "x" * 129, "A\u0000B", "SAVE\n10"])
async def test_an_unsendable_offer_code_is_refused_at_the_edge_before_any_write(client, bad):
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(offer_code=bad))
    assert resp.status_code == 400
    assert _error(resp) == "invalid_offer_code"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_a_non_string_offer_code_is_a_malformed_body(client):
    await _seed_all()
    resp = await client.post(f"{BASE}/purchases", json=_body(offer_code=20))
    assert resp.status_code == 400 and _error(resp) == "invalid_request"


async def test_the_offer_code_is_part_of_what_an_idempotency_key_is_for(client):
    """A retry that adds or changes a code can change the price the buyer approves, so it is a
    different purchase -- and a retry with the SAME code replays."""
    await _seed_all()
    first = await client.post(f"{BASE}/purchases",
                              json=_body(idempotency_key="k-code", offer_code="SAVE10"))
    assert first.status_code == 202
    replay = await client.post(f"{BASE}/purchases",
                               json=_body(idempotency_key="k-code", offer_code="SAVE10"))
    assert replay.status_code == 202
    assert replay.json()["purchase_id"] == first.json()["purchase_id"]
    for other in ("SAVE20", None):
        payload = _body(idempotency_key="k-code")
        if other is not None:
            payload["offer_code"] = other
        resp = await client.post(f"{BASE}/purchases", json=payload)
        assert resp.status_code == 409 and _error(resp) == "idempotency_conflict"


def test_a_request_without_a_code_keeps_its_old_hash():
    """Existing idempotency keys keep their meaning across this rollout."""
    facts = dict(merchant_domain="m.example", product_key="p", variant_key="v", quantity=1,
                 email="a@b.co", shipping_address={"city": "X"}, return_url="https://r")
    assert routes_reap._request_hash(**facts) == routes_reap._request_hash(**facts, offer_code=None)
    assert routes_reap._request_hash(**facts) != routes_reap._request_hash(**facts, offer_code="X")


# ── merchant_disabled is a distinct answer, and it binds the cart-link lane too (B6) ──────────


@pytest.mark.parametrize("spelling", ["brand.example", "www.brand.example"])
async def test_a_disabled_variant_row_refuses_the_cart_link_lane_too(client, monkeypatch, spelling):
    """An operator's "off" (runbook: UPDATE ... SET enabled = FALSE) must not be routed around by
    the Tier B lane, whose own verdict says ELIGIBLE. Refused before any buyer or purchase write."""
    await _seed_tierb_shopify_item()
    await _seed_tierb_verdict()
    await _seed_eligibility(domain=spelling, enabled=False)
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_body(item_source="cart_link"))
    assert resp.status_code == 409 and _error(resp) == "merchant_disabled"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM buyer_identity_links") == 0


@pytest.mark.parametrize("variant_row", [None, "enabled", "other_market_disabled"])
async def test_the_cart_link_lane_is_not_refused_without_a_disabled_row(
    client, monkeypatch, variant_row
):
    """THE CONTROL, both ways: no variant row, an ENABLED one, or a disabled one in ANOTHER market
    leaves the cart-link lane to its own Tier B verdict."""
    await _seed_tierb_shopify_item()
    await _seed_tierb_verdict()
    if variant_row == "enabled":
        await _seed_eligibility(enabled=True)
    elif variant_row == "other_market_disabled":
        await _seed_eligibility(enabled=False, market="SG")
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_body(item_source="cart_link"))
    assert resp.status_code == 202, resp.text


async def test_no_variant_row_is_still_merchant_not_eligible(client):
    """The other half of the split: "never listed" keeps its old answer, which is the one a door
    may try the cart-link lane on."""
    await _seed_catalog()
    await _seed_link()
    resp = await client.post(f"{BASE}/purchases", json=_body())
    assert resp.status_code == 409 and _error(resp) == "merchant_not_eligible"


async def test_a_key_tombstoned_before_money_was_required_stays_refused_after_enabling(client):
    """G5 (gateway review of #2425), for the tombstones that still exist. A variant-lane
    merchant_not_eligible used to be remembered against its key; create no longer writes one
    (every new attempt carries bound money, and a refusal before money admission writes nothing),
    but a key tombstoned before that keeps answering its refusal -- to the money-less retry it was
    keyed for -- after the merchant is enabled, and writes nothing doing so."""
    await _seed_all()  # the operator has since enabled the merchant
    body = _body(idempotency_key="K")
    await _plant_legacy_tombstone(body)
    keys = await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys")
    retry = await client.post(f"{BASE}/purchases", json=_legacy(body))
    assert retry.status_code == 409 and _error(retry) == "merchant_not_eligible", retry.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == keys

    # CONTROLS: a NEW key opens a variant purchase now, and K with money is a different request.
    fresh = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K2"))
    assert fresh.status_code == 202, fresh.text
    other = await client.post(f"{BASE}/purchases", json=body)
    assert other.status_code == 409 and _error(other) == "idempotency_conflict"


async def test_an_old_refusal_requires_a_new_intentional_key(client):
    await _seed_all()
    body = _legacy(_body(idempotency_key="K-t"))
    await _plant_legacy_tombstone(body)
    await database.execute(
        "UPDATE reap_agentic_purchase_keys SET created_at = datetime('now', '-25 hours') "
        "WHERE idempotency_key = 'K-t'")
    resp = await client.post(f"{BASE}/purchases", json=body)
    assert _error(resp) == "merchant_not_eligible"
    resp = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-new"))
    assert resp.status_code == 202, resp.text
    again = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-new"))
    assert again.json()["purchase_id"] == resp.json()["purchase_id"]


async def test_a_variant_refusal_writes_no_key(client, monkeypatch):
    """A keyed variant-lane `merchant_not_eligible` writes nothing, the key included: the money it
    carries has not passed admission. The key stays free for the same body once the merchant is
    enabled."""
    await _seed_catalog()
    await _seed_link()

    async def forbidden(**kwargs):
        raise AssertionError("a refusal before money admission wrote a key")

    with monkeypatch.context() as patch:
        patch.setattr(routes_reap, "_write_idempotency_key", forbidden)
        resp = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-free"))
    assert resp.status_code == 409 and _error(resp) == "merchant_not_eligible", resp.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0
    await _seed_eligibility()
    resp = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-free"))
    assert resp.status_code == 202, resp.text


# ── #2425 re-review: R2 (a refused claim abandons its purchase), R3 (a stale key is replaced), R4 ──


@pytest.mark.parametrize("winner_body", ["tombstone", "other_request"])
async def test_a_claim_that_loses_to_a_refusal_abandons_the_purchase_it_opened(
    client, monkeypatch, winner_body
):
    """R2. Request A opens purchase P, and before A claims the key a CONCURRENT request lands the
    key first -- as a merchant_not_eligible tombstone for the same body, or for a different body.
    A's re-read then REFUSES; P must not be left open holding the buyer's address and email."""
    await _seed_all()
    real_write = routes_reap._write_idempotency_key

    async def _race_then_write(**kwargs):
        await database.execute(
            "INSERT INTO reap_agentic_purchase_keys (agent_id, agent_user_ref_hash, "
            "idempotency_key, purchase_id, request_hash) VALUES (:a, :h, :k, :p, :r)",
            {"a": kwargs["agent_id"], "h": kwargs["agent_user_ref_hash"],
             "k": kwargs["idempotency_key"],
             "p": "refused:merchant_not_eligible" if winner_body == "tombstone" else "rp_other",
             "r": kwargs["request_hash"] if winner_body == "tombstone" else "x" * 64},
        )
        return await real_write(**kwargs)

    monkeypatch.setattr(routes_reap, "_write_idempotency_key", _race_then_write)
    resp = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-race"))
    expected = "merchant_not_eligible" if winner_body == "tombstone" else "idempotency_conflict"
    assert resp.status_code == 409 and _error(resp) == expected, resp.text
    rows = [dict(r) for r in await database.fetch_all(
        "SELECT state, buyer_email, shipping_address FROM reap_agentic_purchases")]
    # This seam injects the competitor on the same connection: the whole failed
    # transaction rolls back. Independent real winners survive in the race tests.
    assert rows == []


async def test_a_lifetime_key_replays_without_replacement(client):
    """An aged key must retain the same purchase and request fingerprint."""
    await _seed_all()
    first = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-old"))
    assert first.status_code == 202
    await database.execute(
        "UPDATE reap_agentic_purchase_keys SET created_at = datetime('now', '-25 hours') "
        "WHERE idempotency_key = 'K-old'")
    second = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-old"))
    assert second.status_code == 202
    assert second.json()["purchase_id"] == first.json()["purchase_id"]
    third = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-old"))
    assert third.json()["purchase_id"] == second.json()["purchase_id"]
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 1
    # An immutable key is never overwritten by the upsert: a different body is still a conflict.
    fourth = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-old", quantity=2))
    assert _error(fourth) == "idempotency_conflict"


async def test_merchant_disabled_under_a_key_writes_no_key_row(client):
    """R4. Only merchant_not_eligible is remembered against a key (`_TOMBSTONED_REFUSALS`): an
    operator's "off" is answered fresh on every request, so turning the merchant back on works
    for the same key."""
    await _seed_catalog()
    await _seed_link()
    await _seed_eligibility(enabled=False)
    resp = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="K-off"))
    assert _error(resp) == "merchant_disabled"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == 0


# ── Tier B: REAL external-seed mirror rows (staging partner demo, 2026-09-29) ────────────────
#
# Shaped EXACTLY like the live row that 409'd on staging (and the same shape in prod):
# judydoll Silky Matte Lip Ink, product prod::external_seed::external_seed::ext_0f95730ee5ba05a6b7957ada.
# Unsuppressed skus: the `::canonical` placeholder (source_variant_id = the product_key), the crawl
# lane's `::sku_58ae…` (source_variant_id `ext_0f95…:49819267301653`) and -- prod only -- the variant
# promoter's `::v:49819267301653` (source_variant_id `49819267301653`): ONE Shopify variant, spelled
# twice. Offers on every sku for merch_obs_a25cbba37ef98c52, USD 13.99, in_stock. seller_ref NULL.
# The seed: active, attached to the product, judydoll.com / US, attached_variant_id NULL, and a
# seed_data shaped as scripts/backfill_shopify_variant_ids.py writes it (snapshot.brand,
# storefront_platform, variants[].shopify_variant_id, shopify_cart_proof).

LIVE_DOMAIN = "judydoll.com"
LIVE_EXT_ID = "ext_0f95730ee5ba05a6b7957ada"
LIVE_PK = f"prod::external_seed::external_seed::{LIVE_EXT_ID}"
LIVE_MERCHANT = "merch_obs_a25cbba37ef98c52"
LIVE_SEED_ID = "epsv_38ad88d436c32e24ba7c6446"
LIVE_VARIANT = "49819267301653"
LIVE_SKU_CANONICAL = f"{LIVE_PK}::canonical"
LIVE_SKU_CRAWL = f"{LIVE_PK}::sku_58ae6f8de2c8797993f2"
LIVE_SKU_PROMOTED = f"{LIVE_PK}::v:{LIVE_VARIANT}"
LIVE_HANDLE = "silky-matte-lip-ink"

#: (sku_key, source_variant_id) as they sit in the catalog.
LIVE_STAGING_SKUS = (
    (LIVE_SKU_CANONICAL, LIVE_PK),
    (LIVE_SKU_CRAWL, f"{LIVE_EXT_ID}:{LIVE_VARIANT}"),
)
LIVE_PROD_SKUS = LIVE_STAGING_SKUS + ((LIVE_SKU_PROMOTED, LIVE_VARIANT),)


def _live_seed_data(variant: str = LIVE_VARIANT, *, brand: str = "Judydoll") -> Dict[str, Any]:
    return {
        "snapshot": {
            "brand": brand,
            "title": "Silky Matte Lip Ink",
            "storefront_platform": "shopify",
            "storefront_platform_source": "products_js_v1",
            "variants": [{"title": "Default Title", "price": "13.99", "shopify_variant_id": variant}],
            "shopify_cart_proof": {
                "source": "products_js_v1",
                "product_js_url": f"https://{LIVE_DOMAIN}/products/{LIVE_HANDLE}.js",
                "live_variant_count": 1,
                "variant_id": variant,
                "checked_at": datetime.now(timezone.utc).isoformat(),
            },
        },
    }


async def _seed_live_mirror(
    *, skus=LIVE_PROD_SKUS, offers_on=None, merchant_id: str = LIVE_MERCHANT,
    seller_ref: Optional[str] = None, brand: Optional[str] = "Judydoll", seed_data=None,
):
    await database.execute(
        "INSERT INTO catalog_products (product_key, merchant_id, platform, source_product_id, title, "
        "brand, category, product_type, source_domain, source_system, source_ref, seller_ref, "
        "seed_kind) VALUES (:pk, :m, 'external_seed', :ext, 'Silky Matte Lip Ink', :brand, "
        "'makeup', 'Lipstick', :domain, 'external_product_seeds_mirror_v1', :seed, :seller, 'self')",
        {"pk": LIVE_PK, "m": merchant_id, "ext": LIVE_EXT_ID, "brand": brand,
         "domain": LIVE_DOMAIN, "seed": LIVE_SEED_ID, "seller": seller_ref},
    )
    priced = {key for key, _ in skus} if offers_on is None else set(offers_on)
    for sku_key, source_variant_id in skus:
        await database.execute(
            "INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform, "
            "source_product_id, source_variant_id, title, currency) VALUES "
            "(:sk, :pk, :m, 'external_seed', :ext, :vid, 'Silky Matte Lip Ink', 'USD')",
            {"sk": sku_key, "pk": LIVE_PK, "m": merchant_id, "ext": LIVE_EXT_ID,
             "vid": source_variant_id},
        )
        if sku_key in priced:
            await database.execute(
                "INSERT INTO catalog_offers (offer_id, sku_key, product_key, merchant_id, currency, "
                "merchant_effective_price, availability) VALUES "
                "(:oid, :sk, :pk, :m, 'USD', '13.99', 'in_stock')",
                {"oid": f"off_{sku_key[-24:]}", "sk": sku_key, "pk": LIVE_PK, "m": merchant_id},
            )
    await database.execute(
        "INSERT INTO external_product_seeds (id, status, domain, market, destination_url, "
        "canonical_url, attached_product_key, attached_variant_id, seed_data) VALUES "
        "(:id, 'active', :domain, 'US', :dest, NULL, :pk, NULL, :sd)",
        {"id": LIVE_SEED_ID, "domain": LIVE_DOMAIN,
         "dest": f"https://{LIVE_DOMAIN}/products/{LIVE_HANDLE}", "pk": LIVE_PK,
         "sd": json.dumps(seed_data if seed_data is not None else _live_seed_data())},
    )
    await database.execute(
        "INSERT INTO tierb_cart_link_eligibility (shop_domain, market, verdict, checked_at) "
        "VALUES (:d, 'US', 'ELIGIBLE', CURRENT_TIMESTAMP)", {"d": LIVE_DOMAIN},
    )


def _live_body(**over) -> Dict[str, Any]:
    # What the gateway sent (outcome cart_link_direct): the catalog keys, NO variant_key, and the
    # money the buyer was shown (the live mirror's 13.99 USD unless a test says otherwise).
    return _body(**{"item_source": "cart_link", "merchant_domain": LIVE_DOMAIN,
                    "product_key": LIVE_PK, "variant_key": None,
                    "expected_unit_price_minor": 1399, **over})


@pytest.mark.parametrize("skus", [LIVE_STAGING_SKUS, LIVE_PROD_SKUS], ids=["staging", "prod"])
async def test_a_live_shaped_mirror_row_is_bought_through_the_cart_link(client, monkeypatch, skus):
    await _seed_live_mirror(skus=skus)
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 202, resp.text
    purchase = await _purchase_row(resp.json()["purchase_id"])
    assert purchase["item_source"] == "cart_link"
    assert f"https://{LIVE_DOMAIN}/cart/{LIVE_VARIANT}:1?" in purchase["cart_url"]
    assert purchase["our_price_minor"] == 1399 and purchase["currency"] == "USD"
    click = await database.fetch_one(
        "SELECT merchant_id FROM surface_click_events WHERE click_id = :c",
        {"c": purchase["click_id"]})
    assert click["merchant_id"] == LIVE_MERCHANT


async def test_the_priced_sku_is_the_lowest_one_with_an_offer(client, monkeypatch):
    """Deterministic choice among the spellings of the ONE variant: the lowest sku_key that this
    seller has a usable offer on. Here only the promoter's `::v:` spelling is priced."""
    await _seed_live_mirror(offers_on=[LIVE_SKU_PROMOTED])
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 202, resp.text


def test_the_cart_sku_choice_rule_accepts_and_refuses_by_distinct_variant():
    choose = routes_reap._cart_sku_choice
    rows = lambda pairs: [{"sku_key": k, "source_variant_id": v} for k, v in pairs]  # noqa: E731
    # ACCEPT: the placeholder is set aside; one variant under one or two spellings.
    variant, group, placeholder = choose(rows(LIVE_STAGING_SKUS), LIVE_PK)
    assert variant == LIVE_VARIANT and [g["sku_key"] for g in group] == [LIVE_SKU_CRAWL]
    assert placeholder["sku_key"] == LIVE_SKU_CANONICAL
    variant, group, _ = choose(rows(LIVE_PROD_SKUS), LIVE_PK)
    assert variant == LIVE_VARIANT
    assert [g["sku_key"] for g in group] == sorted([LIVE_SKU_CRAWL, LIVE_SKU_PROMOTED])
    variant, group, _ = choose(rows([(LIVE_SKU_PROMOTED, f"gid://shopify/ProductVariant/{LIVE_VARIANT}")]), LIVE_PK)
    assert variant == LIVE_VARIANT
    # Only the placeholder: no variant from the catalog (the seed proof is then the only identity).
    assert choose(rows(LIVE_STAGING_SKUS[:1]), LIVE_PK) == (None, [], {"sku_key": LIVE_SKU_CANONICAL, "source_variant_id": LIVE_PK})
    # REFUSE: two different variants; another product's `ext_` id beside a real one; a placeholder
    # spelling whose id is NOT the product key is a real sku that names nothing.
    for bad in (
        LIVE_STAGING_SKUS + ((f"{LIVE_PK}::v:49819267301654", "49819267301654"),),
        LIVE_STAGING_SKUS + ((f"{LIVE_PK}::sku_other", f"ext_somethingelse:{LIVE_VARIANT}"),),
        ((LIVE_SKU_CANONICAL, "not-the-product-key"), (LIVE_SKU_CRAWL, f"{LIVE_EXT_ID}:{LIVE_VARIANT}")),
    ):
        with pytest.raises(svc.PurchaseRefused) as caught:
            choose(rows(bad), LIVE_PK)
        assert caught.value.reason == "row_not_found"


@pytest.mark.parametrize("extra", [
    ((f"{LIVE_PK}::v:49819267301654", "49819267301654"),),                 # two variants
    ((f"{LIVE_PK}::sku_other", f"ext_somethingelse:{LIVE_VARIANT}"),),     # another product's id
])
async def test_a_mirror_row_naming_two_variants_is_still_ambiguous(client, monkeypatch, extra):
    await _seed_live_mirror(skus=LIVE_STAGING_SKUS + extra)
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 409 and _error(resp) == "row_not_found"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_a_catalog_variant_that_contradicts_the_storefront_proof_is_refused(client, monkeypatch):
    """The seed's storefront proof is the authority; the catalog must AGREE with it."""
    await _seed_live_mirror(seed_data=_live_seed_data("49819267309999"))
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 409 and _error(resp) == "row_variant_unverified"


async def test_a_mirror_row_with_no_fresh_storefront_proof_is_refused(client, monkeypatch):
    stale = _live_seed_data()
    stale["snapshot"]["shopify_cart_proof"]["checked_at"] = "2026-01-01T00:00:00+00:00"
    await _seed_live_mirror(seed_data=stale)
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 409 and _error(resp) == "row_variant_unverified"


# ── option 1: a MULTI-variant product whose seed NAMES one variant (Peng, 2026-09-29) ────────
#
# The live judydoll row as it really is: 8 shades on the storefront (tests/fixtures/judydoll_*
# products_js), a seed naming 07 BURGUNDY INK (tests/fixtures/judydoll_*_seed, staging), and the
# proof scripts/backfill_shopify_variant_ids.build_cart_proof writes from exactly those two.

_FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _named_variant_seed(*, variant_overrides: Optional[Dict[str, Any]] = None,
                        page_url: Optional[str] = None, only_variant: Optional[str] = None,
                        entry_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from scripts.backfill_shopify_variant_ids import build_cart_proof
    from services.shopify_variant_identity import parse_product_js, stamp_variant_ids

    payload = json.loads((_FIXTURES / "judydoll_silky_matte_lip_ink_products_js_2026_09_29.json").read_text())
    row = json.loads((_FIXTURES / "judydoll_silky_matte_lip_ink_seed_2026_09_29.json").read_text())
    for variant in payload["variants"]:
        variant.update((variant_overrides or {}).get(str(variant["id"]), {}))
    if only_variant:
        payload["variants"] = [v for v in payload["variants"] if str(v["id"]) == only_variant]
    seed = row["seed_data"]
    seed["snapshot"]["variants"][0].update(entry_overrides or {})
    live = parse_product_js(payload)
    new_variants, _ = stamp_variant_ids(seed["snapshot"]["variants"], live)
    seed["snapshot"].update({
        "variants": new_variants, "storefront_platform": "shopify",
        "storefront_platform_source": "products_js_v1",
        "shopify_cart_proof": build_cart_proof(
            seed, new_variants, payload, live,
            js_url=f"https://{LIVE_DOMAIN}/products/{LIVE_HANDLE}.js",
            page_url=page_url or row["canonical_url"], shop_host=LIVE_DOMAIN,
            checked_at=datetime.now(timezone.utc)),
    })
    return seed


async def _seed_named_variant_mirror(*, env: str, seed_data: Dict[str, Any], **kw) -> None:
    """`_seed_live_mirror`, then the seed's two URL columns exactly as each environment has them."""
    await _seed_live_mirror(seed_data=seed_data, **kw)
    dest = f"https://{LIVE_DOMAIN}/products/{LIVE_HANDLE}?variant={LIVE_VARIANT}&utm_source=pivota&utm_medium=affiliate"
    canonical = dest if env == "prod" else f"https://{LIVE_DOMAIN}/products/{LIVE_HANDLE}"
    await database.execute(
        "UPDATE external_product_seeds SET canonical_url = :c, destination_url = :d WHERE id = :id",
        {"c": canonical, "d": dest, "id": LIVE_SEED_ID})


@pytest.mark.parametrize("env,skus", [("staging", LIVE_STAGING_SKUS), ("prod", LIVE_PROD_SKUS)])
async def test_a_multi_variant_mirror_row_is_bought_on_its_named_variant_proof(client, monkeypatch, env, skus):
    seed_data = _named_variant_seed()
    assert seed_data["snapshot"]["shopify_cart_proof"]["scope"] == "named_variant"
    assert seed_data["snapshot"]["shopify_cart_proof"]["live_variant_count"] == 8
    await _seed_named_variant_mirror(env=env, skus=skus, seed_data=seed_data)
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 202, resp.text
    purchase = await _purchase_row(resp.json()["purchase_id"])
    assert f"https://{LIVE_DOMAIN}/cart/{LIVE_VARIANT}:1?" in purchase["cart_url"]
    assert purchase["our_price_minor"] == 1399 and purchase["currency"] == "USD"
    # F1 (#2459 review): the buyer never picked the shade, so the purchase SAYS which one it buys,
    # in the live storefront's words -- on the create response, on the stored row, and on GET.
    assert resp.json()["variant_title"] == "07 BURGUNDY INK"
    assert purchase["variant_title"] == "07 BURGUNDY INK"
    got = await client.get(f"{BASE}/purchases/{resp.json()['purchase_id']}")
    assert got.status_code == 200 and got.json()["variant_title"] == "07 BURGUNDY INK"
    # additive: every field the response had before is still there
    assert {"purchase_id", "status", "poll_after_seconds"} <= set(resp.json())


async def test_a_replayed_cart_link_purchase_answers_the_same_variant_title(client, monkeypatch):
    await _seed_named_variant_mirror(env="staging", skus=LIVE_STAGING_SKUS, seed_data=_named_variant_seed())
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    body = {**_live_body(), "idempotency_key": "named-variant-replay"}
    first = await client.post(f"{BASE}/purchases", json=body)
    again = await client.post(f"{BASE}/purchases", json=body)
    assert first.status_code == again.status_code == 202, again.text
    assert again.json()["purchase_id"] == first.json()["purchase_id"]
    assert again.json()["variant_title"] == first.json()["variant_title"] == "07 BURGUNDY INK"


async def test_an_old_proofs_dirty_variant_title_reaches_nobody_dirty(client, monkeypatch):
    """The title is merchant-typed storefront text. A proof written BEFORE the display rule (a
    newline, U+202E, a zero-width space and a private-use glyph in it) is cleaned where it is
    READ, so the 202 body, the stored row and GET all carry the clean title. HTML stays text."""
    seed_data = _named_variant_seed()
    seed_data["snapshot"]["shopify_cart_proof"]["variant_title"] = (
        "\u202e07\nBURGUNDY\u200b INK\ue000 <b>")
    await _seed_named_variant_mirror(env="staging", skus=LIVE_STAGING_SKUS, seed_data=seed_data)
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 202, resp.text
    purchase = await _purchase_row(resp.json()["purchase_id"])
    got = await client.get(f"{BASE}/purchases/{resp.json()['purchase_id']}")
    assert resp.json()["variant_title"] == purchase["variant_title"] == got.json()["variant_title"] \
        == "07 BURGUNDY INK <b>"


async def test_a_dirty_catalog_product_title_is_stored_and_shown_clean(client, monkeypatch):
    """#2462 follow-up: the door composes "<product name> -- <variant title>", so an unterminated
    U+202E in the CATALOG title would reverse the clean variant title after it. The cart-link
    loader cleans the product name with the same display rule before it is stored."""
    await _seed_named_variant_mirror(env="staging", skus=LIVE_STAGING_SKUS, seed_data=_named_variant_seed())
    await database.execute("UPDATE catalog_products SET title = :t WHERE product_key = :pk",
                           {"t": "Silky Matte\u202e Lip\nInk\u200b\ue000", "pk": LIVE_PK})
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 202, resp.text
    purchase = await _purchase_row(resp.json()["purchase_id"])
    got = await client.get(f"{BASE}/purchases/{resp.json()['purchase_id']}")
    assert purchase["product_name"] == got.json()["product_name"] == "Silky Matte Lip Ink"


async def test_a_row_written_dirty_before_the_rule_is_answered_clean_everywhere(client, monkeypatch):
    """A purchase row written before #2462 still HOLDS the raw names. GET, the list and an
    idempotent replay all answer from the public view, which cleans them on the way out -- so
    they agree with a fresh 202 -- while the stored row itself is left exactly as written."""
    await _seed_named_variant_mirror(env="staging", skus=LIVE_STAGING_SKUS, seed_data=_named_variant_seed())
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    body = {**_live_body(), "idempotency_key": "dirty-old-row"}
    first = await client.post(f"{BASE}/purchases", json=body)
    assert first.status_code == 202, first.text
    purchase_id = first.json()["purchase_id"]
    dirty_title, dirty_name = "\u202e07\nBURGUNDY\u200b INK\ufeff", "\u2066Silky Matte\tLip Ink\ue000"
    await database.execute(
        "UPDATE reap_agentic_purchases SET variant_title = :v, product_name = :p WHERE id = :id",
        {"v": dirty_title, "p": dirty_name, "id": purchase_id})
    got = (await client.get(f"{BASE}/purchases/{purchase_id}")).json()
    listed = [p for p in (await client.get(f"{BASE}/purchases")).json()["purchases"]
              if p["id"] == purchase_id]
    again = await client.post(f"{BASE}/purchases", json=body)
    assert again.status_code == 202 and again.json()["purchase_id"] == purchase_id
    for view in (got, listed[0]):
        assert (view["variant_title"], view["product_name"]) == ("07 BURGUNDY INK", "Silky Matte Lip Ink")
    assert again.json()["variant_title"] == first.json()["variant_title"] == "07 BURGUNDY INK"
    row = await _purchase_row(purchase_id)
    assert (row["variant_title"], row["product_name"]) == (dirty_title, dirty_name)


async def test_a_named_variant_proof_never_prices_from_the_placeholder(client, monkeypatch):
    """#2457 review: only a SOLE proof may let the `::canonical` product-level offer stand in. On a
    multi-variant product, the placeholder's price is not the named shade's price."""
    await _seed_named_variant_mirror(env="staging", skus=LIVE_STAGING_SKUS,
                                     seed_data=_named_variant_seed(), offers_on=[LIVE_SKU_CANONICAL])
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 409 and _error(resp) == "row_unpriced", resp.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_the_named_proof_placeholder_guard_is_reached_by_a_real_producer_shape(client, monkeypatch):
    """#2459 review, F2: the shape that REACHES the guard. A seed entry whose `shopify_variant_id`
    is junk (a live writer lands unvalidated keys there; `stamp_variant_ids` never overwrites one),
    `?variant=` on its product URL, and a ONE-variant storefront: the sole proof cannot be written
    (the stamp is junk), so `build_cart_proof` writes a NAMED proof with live_variant_count 1 --
    which #2457's placeholder fallback reads as "one live variant". Only the named-scope guard
    stops the product-level placeholder offer from pricing it."""
    seed_data = _named_variant_seed(
        only_variant=LIVE_VARIANT, page_url=f"https://{LIVE_DOMAIN}/products/{LIVE_HANDLE}?variant={LIVE_VARIANT}",
        entry_overrides={"shopify_variant_id": "not-a-number"})
    proof = seed_data["snapshot"]["shopify_cart_proof"]
    assert proof["scope"] == "named_variant" and proof["live_variant_count"] == 1
    await _seed_named_variant_mirror(env="prod", skus=LIVE_STAGING_SKUS, seed_data=seed_data,
                                     offers_on=[LIVE_SKU_CANONICAL])
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 409 and _error(resp) == "row_unpriced", resp.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0
    # control: with the variant's OWN sku offered, the same row is bought at that price
    await database.execute(
        "INSERT INTO catalog_offers (offer_id, sku_key, product_key, merchant_id, currency, "
        "merchant_effective_price, availability) VALUES "
        "('off_guard_real', :sk, :pk, :m, 'USD', '13.99', 'in_stock')",
        {"sk": LIVE_SKU_CRAWL, "pk": LIVE_PK, "m": LIVE_MERCHANT})
    ok = await client.post(f"{BASE}/purchases", json=_live_body())
    assert ok.status_code == 202, ok.text


@pytest.mark.parametrize("case", ["placeholder_only", "catalog_names_a_sibling", "unavailable",
                                  "url_names_a_sibling"])
async def test_a_named_variant_proof_is_refused_unless_every_source_agrees(client, monkeypatch, case):
    sibling = "49819267170581"  # 01 PETAL INK, live on the same product
    skus, seed_data = LIVE_STAGING_SKUS, _named_variant_seed()
    if case == "placeholder_only":
        skus = LIVE_STAGING_SKUS[:1]
    elif case == "catalog_names_a_sibling":
        skus = ((LIVE_SKU_CANONICAL, LIVE_PK), (LIVE_SKU_CRAWL, f"{LIVE_EXT_ID}:{sibling}"))
    elif case == "unavailable":
        seed_data = _named_variant_seed(variant_overrides={LIVE_VARIANT: {"available": False}})
        assert seed_data["snapshot"]["shopify_cart_proof"] is None
    await _seed_named_variant_mirror(env="staging", skus=skus, seed_data=seed_data)
    if case == "url_names_a_sibling":
        await database.execute(
            "UPDATE external_product_seeds SET canonical_url = :c WHERE id = :id",
            {"c": f"https://{LIVE_DOMAIN}/products/{LIVE_HANDLE}?variant={sibling}", "id": LIVE_SEED_ID})
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 409 and _error(resp) == "row_variant_unverified", resp.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


@pytest.mark.parametrize("merchant_id,seller_ref,brand,snapshot_brand,expected", [
    # the live row: seller_ref NULL, merchant_id derived from (Judydoll, judydoll.com)
    (LIVE_MERCHANT, None, "Judydoll", "Judydoll", 202),
    # the product's brand is empty -> the seed snapshot's brand derives it
    (LIVE_MERCHANT, None, None, "Judydoll", 202),
    # a non-null seller_ref equal to merchant_id: today's rule, unchanged
    (LIVE_MERCHANT, LIVE_MERCHANT, "Judydoll", "Judydoll", 202),
    # REFUSE: merchant_id is not the id derived for this storefront
    ("merch_obs_0000000000000000", None, "Judydoll", "Judydoll", 409),
    # REFUSE: a different brand derives a different id
    (LIVE_MERCHANT, None, "Judy Doll", "Judy Doll", 409),
    # REFUSE: a non-null seller_ref that differs from merchant_id
    (LIVE_MERCHANT, "merch_obs_ffffffffffffffff", "Judydoll", "Judydoll", 409),
])
async def test_a_mirror_row_seller_is_the_observed_id_derived_from_its_storefront(
    client, monkeypatch, merchant_id, seller_ref, brand, snapshot_brand, expected,
):
    await _seed_live_mirror(merchant_id=merchant_id, seller_ref=seller_ref, brand=brand,
                            seed_data=_live_seed_data(brand=snapshot_brand))
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == expected, resp.text
    if expected == 409:
        assert _error(resp) == "seller_identity_unverified"


async def test_the_mirror_seller_rule_does_not_apply_to_shopify_rows(client, monkeypatch):
    """A `platform='shopify'` row keeps today's rule (seller_ref, else merchant_id)."""
    await _seed_tierb_shopify_item()
    await _seed_tierb_verdict()
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_body(item_source="cart_link"))
    assert resp.status_code == 202, resp.text


async def test_a_caller_named_sku_outside_the_chosen_variant_is_refused_on_a_mirror(client, monkeypatch):
    await _seed_live_mirror()
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    ok = await client.post(f"{BASE}/purchases", json={**_live_body(), "variant_key": LIVE_SKU_PROMOTED})
    assert ok.status_code == 202, ok.text
    bad = await client.post(f"{BASE}/purchases", json={**_live_body(), "variant_key": LIVE_SKU_CANONICAL})
    assert bad.status_code == 409 and _error(bad) == "row_variant_unverified"


# ── #2453 review round ──────────────────────────────────────────────────────────────────────


async def test_a_named_shopify_sku_past_the_list_cap_is_still_found(client, monkeypatch):
    """Regression probe A: the caller-named sku is read BY KEY, not searched inside the bounded
    no-key list -- a product with 60 skus and a named key that sorts last is bought (main: 202)."""
    await _seed_tierb_shopify_item()
    await _seed_tierb_verdict()
    for i in range(60):
        await database.execute(
            "INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform, source_product_id, "
            "source_variant_id, title, currency) VALUES (:sk, :pk, 'm_brand', 'shopify', '1001', :vid, "
            ":t, 'USD')",
            {"sk": f"{PRODUCT_KEY}::a{i:03d}", "pk": PRODUCT_KEY, "vid": str(40000000000000 + i),
             "t": f"Shade {i}"},
        )
    late = f"{PRODUCT_KEY}::zz_named"
    await database.execute(
        "INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform, source_product_id, "
        "source_variant_id, title, currency) VALUES (:sk, :pk, 'm_brand', 'shopify', '1001', "
        "'41111111111111', 'Late', 'USD')", {"sk": late, "pk": PRODUCT_KEY})
    await database.execute(
        "INSERT INTO catalog_offers (offer_id, sku_key, product_key, merchant_id, currency, "
        "merchant_effective_price) VALUES ('off_late', :sk, :pk, 'm_brand', 'USD', '42.50')",
        {"sk": late, "pk": PRODUCT_KEY})
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_body(item_source="cart_link", variant_key=late))
    assert resp.status_code == 202, resp.text
    purchase = await _purchase_row(resp.json()["purchase_id"])
    assert "/cart/41111111111111:1?" in purchase["cart_url"]


async def _reprice(sku_key: str, price: str) -> None:
    await database.execute(
        "UPDATE catalog_offers SET merchant_effective_price = :p WHERE sku_key = :sk",
        {"p": price, "sk": sku_key})


async def test_a_named_mirror_spelling_is_priced_itself(client, monkeypatch):
    """Probe B/I: a caller-named sku is priced from ITS offer, not the lowest sku_key's."""
    await _seed_live_mirror()
    await _reprice(LIVE_SKU_PROMOTED, "14.99")
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json={**_live_body(expected_unit_price_minor=1499),
                                                       "variant_key": LIVE_SKU_PROMOTED})
    assert resp.status_code == 202, resp.text
    assert (await _purchase_row(resp.json()["purchase_id"]))["our_price_minor"] == 1499
    named_low = await client.post(f"{BASE}/purchases", json={**_live_body(), "variant_key": LIVE_SKU_CRAWL})
    assert named_low.status_code == 202, named_low.text
    assert (await _purchase_row(named_low.json()["purchase_id"]))["our_price_minor"] == 1399


async def test_unnamed_spellings_that_disagree_on_price_are_refused(client, monkeypatch):
    await _seed_live_mirror()
    await _reprice(LIVE_SKU_PROMOTED, "14.99")
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 409 and _error(resp) == "row_price_ambiguous"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_unnamed_spellings_that_agree_on_price_are_bought(client, monkeypatch):
    """CONTROL: the same row with agreeing prices (the live shape: 13.99 everywhere)."""
    await _seed_live_mirror()
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 202, resp.text


async def test_shopify_no_key_two_distinct_variants_is_ambiguous(client, monkeypatch):
    """M13 / probe K."""
    await _seed_tierb_shopify_item()
    await _seed_tierb_verdict()
    await database.execute(
        "INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform, source_product_id, "
        "source_variant_id, title, currency) VALUES (:sk, :pk, 'm_brand', 'shopify', '1001', "
        "'50041364447510', 'Large', 'USD')", {"sk": SECOND_SKU_KEY, "pk": PRODUCT_KEY})
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_body(item_source="cart_link", variant_key=None))
    assert resp.status_code == 409 and _error(resp) == "row_not_found"


async def test_shopify_no_key_placeholder_plus_one_real_sku_is_bought(client, monkeypatch):
    """M13 / probe L: the `::canonical` placeholder beside ONE real variant is not ambiguity."""
    await _seed_tierb_shopify_item()
    await _seed_tierb_verdict()
    await database.execute(
        "INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform, source_product_id, "
        "source_variant_id, title, currency) VALUES (:sk, :pk, 'm_brand', 'shopify', '1001', :pk, "
        "'Standard Eau de Parfum', 'USD')", {"sk": f"{PRODUCT_KEY}::canonical", "pk": PRODUCT_KEY})
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_body(item_source="cart_link", variant_key=None))
    assert resp.status_code == 202, resp.text
    assert "/cart/50041364447509:1?" in (await _purchase_row(resp.json()["purchase_id"]))["cart_url"]


async def test_a_mirror_whose_only_real_sku_names_no_variant_is_unverified(client, monkeypatch):
    """M9 / probe H."""
    await _seed_live_mirror(skus=((LIVE_SKU_CANONICAL, LIVE_PK), (LIVE_SKU_CRAWL, "SKU-SILKY-01")))
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 409 and _error(resp) == "row_variant_unverified"


def test_the_sku_cap_refuses_more_than_fifty_rows():
    """M10: over `_CART_MAX_SKUS` live skus is not a product one cart link can name -- even when
    every row names the SAME variant (so the distinct-variant rule alone would accept it)."""
    def rows(n):
        spellings = [f"{LIVE_EXT_ID}:{LIVE_VARIANT}", LIVE_VARIANT, f"gid://shopify/ProductVariant/{LIVE_VARIANT}"]
        return [{"sku_key": f"{LIVE_PK}::s{i:03d}", "source_variant_id": spellings[i % 3]} for i in range(n)]
    variant, group, _ = routes_reap._cart_sku_choice(rows(routes_reap._CART_MAX_SKUS), LIVE_PK)
    assert variant == LIVE_VARIANT and len(group) == routes_reap._CART_MAX_SKUS
    with pytest.raises(svc.PurchaseRefused) as caught:
        routes_reap._cart_sku_choice(rows(routes_reap._CART_MAX_SKUS + 1), LIVE_PK)
    assert caught.value.reason == "row_not_found"


def test_the_mirrored_brand_order_is_the_mirror_scripts_own():
    """ONE order: `MIRRORED_BRAND_PATHS` must equal the `coalesce(...) AS mirrored_brand` list in
    scripts/mirror_external_seeds_to_catalog_products.py -- parsed from the SQL, not restated."""
    import re as _re
    src = (Path(__file__).resolve().parents[1]
           / "scripts" / "mirror_external_seeds_to_catalog_products.py").read_text(encoding="utf-8")
    block = src[:src.index("AS mirrored_brand")]
    block = block[block.rindex("coalesce("):]
    paths = []
    for arrow, path in _re.findall(r"eps\.seed_data\s*(#>>|->>)\s*'([^']+)'", block):
        paths.append(tuple(path.strip("{}").split(",")) if arrow == "#>>" else (path,))
    assert tuple(paths) == routes_reap.MIRRORED_BRAND_PATHS


@pytest.mark.parametrize("seed_data,expected", [
    ({"snapshot": {"brand": "A", "vendor": "B"}, "brand": "C", "vendor": "D"}, "A"),
    ({"snapshot": {"vendor": "B"}, "brand": "C", "vendor": "D"}, "C"),
    ({"snapshot": {"vendor": "B"}, "vendor": "D"}, "B"),
    ({"vendor": " D "}, "D"),
    ({"snapshot": {"brand": "  "}, "brand": "C"}, None),   # SQL coalesce: a present blank wins
    ({}, None),
])
def test_mirrored_seed_brand_is_sql_coalesce(seed_data, expected):
    assert routes_reap.mirrored_seed_brand(seed_data) == expected


async def test_a_vendor_only_mirror_seed_derives_its_seller(client, monkeypatch):
    """A seed with no brand anywhere but `snapshot.vendor` -- what the mirror minted from -- and a
    product row with no brand: the seller still derives to the row's merch_obs_ id."""
    seed = _live_seed_data()
    del seed["snapshot"]["brand"]
    seed["snapshot"]["vendor"] = "Judydoll"
    await _seed_live_mirror(brand=None, seed_data=seed)
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_live_body())
    assert resp.status_code == 202, resp.text


# ── Tier B mirror priced from its `::canonical` placeholder (staging demo, 2026-09-29) ───────
#
# The live row: KraveBeauty 24 Carrot Retinal, prod::external_seed::external_seed::ext_8026e9…,
# merchant merch_obs_a6747fa7eb42a31c (derived from KraveBeauty + kravebeauty.com). Skus: the
# `::canonical` placeholder and ONE real sku resolving to 41596313010251. The ONLY offer for the
# merchant sits on the placeholder: USD 28.00, in_stock. The seed proves one live variant.

KRAVE_DOMAIN = "kravebeauty.com"
KRAVE_EXT = "ext_8026e90301d17f1f7745b5c7"
KRAVE_PK = f"prod::external_seed::external_seed::{KRAVE_EXT}"
KRAVE_MERCHANT = "merch_obs_a6747fa7eb42a31c"
KRAVE_SEED = "epsv_krave_24_carrot_retinal"
KRAVE_VARIANT = "41596313010251"
KRAVE_PLACEHOLDER = f"{KRAVE_PK}::canonical"
KRAVE_REAL = f"{KRAVE_PK}::sku_3c1f0e9a7b2d4c6e8f01"


async def _seed_krave_mirror(*, offers, market="US", live_variant_count=1, real_sku=True):
    """`offers`: [(sku_key, merchant_id, price)] -- USD, in_stock, market 'US' (the mirror writer's
    column default) -- or [(sku_key, merchant_id, price, {column: value})] to override any of
    currency / market / availability / suppression_reason / suppressed_at. `market` is the seed's
    and the Tier B verdict's; the buyer's market is the shipping country (`_krave_body`)."""
    await database.execute(
        "INSERT INTO catalog_products (product_key, merchant_id, platform, source_product_id, title, "
        "brand, category, product_type, source_domain, source_system, source_ref, seller_ref, "
        "seed_kind) VALUES (:pk, :m, 'external_seed', :ext, '24 Carrot Retinal', 'KraveBeauty', "
        "'skincare', 'Serum', :d, 'external_product_seeds_mirror_v1', :seed, NULL, 'self')",
        {"pk": KRAVE_PK, "m": KRAVE_MERCHANT, "ext": KRAVE_EXT, "d": KRAVE_DOMAIN, "seed": KRAVE_SEED},
    )
    skus = [(KRAVE_PLACEHOLDER, KRAVE_PK)]
    skus += [(KRAVE_REAL, f"{KRAVE_EXT}:{KRAVE_VARIANT}")] if real_sku else []
    for sku_key, vid in skus:
        await database.execute(
            "INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform, source_product_id, "
            "source_variant_id, title, currency) VALUES (:sk, :pk, :m, 'external_seed', :ext, :vid, "
            "'24 Carrot Retinal', 'USD')",
            {"sk": sku_key, "pk": KRAVE_PK, "m": KRAVE_MERCHANT, "ext": KRAVE_EXT, "vid": vid},
        )
    for i, (sku_key, merchant_id, price, *extra) in enumerate(offers):
        cols = {"currency": "USD", "market": "US", "availability": "in_stock",
                "suppression_reason": None, "suppressed_at": None, **(extra[0] if extra else {})}
        await database.execute(
            "INSERT INTO catalog_offers (offer_id, sku_key, product_key, merchant_id, currency, market, "
            "merchant_effective_price, availability, suppression_reason, suppressed_at) VALUES "
            "(:oid, :sk, :pk, :m, :currency, :market, :p, :availability, :suppression_reason, "
            ":suppressed_at)",
            {"oid": f"off_krave_{i}", "sk": sku_key, "pk": KRAVE_PK, "m": merchant_id, "p": price,
             **cols},
        )
    seed = {"snapshot": {
        "brand": "KraveBeauty", "title": "24 Carrot Retinal",
        "storefront_platform": "shopify", "storefront_platform_source": "products_js_v1",
        "variants": [{"title": "Default Title", "price": "28.00", "shopify_variant_id": KRAVE_VARIANT}],
        "shopify_cart_proof": {
            "source": "products_js_v1",
            "product_js_url": f"https://{KRAVE_DOMAIN}/products/24-carrot-retinal.js",
            "live_variant_count": live_variant_count, "variant_id": KRAVE_VARIANT,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        },
    }}
    await database.execute(
        "INSERT INTO external_product_seeds (id, status, domain, market, destination_url, "
        "canonical_url, attached_product_key, attached_variant_id, seed_data) VALUES "
        "(:id, 'active', :d, :mk, :dest, NULL, :pk, NULL, :sd)",
        {"id": KRAVE_SEED, "d": KRAVE_DOMAIN, "mk": market,
         "dest": f"https://{KRAVE_DOMAIN}/products/24-carrot-retinal", "pk": KRAVE_PK,
         "sd": json.dumps(seed)},
    )
    await database.execute(
        "INSERT INTO tierb_cart_link_eligibility (shop_domain, market, verdict, checked_at) "
        "VALUES (:d, :mk, 'ELIGIBLE', CURRENT_TIMESTAMP)", {"d": KRAVE_DOMAIN, "mk": market},
    )


def _krave_body(country: str = "US", minor: Optional[int] = None) -> Dict[str, Any]:
    # The shown money: the placeholder offer's 28.00 USD, or 36.00 SGD for an SG buyer, unless
    # a test prices the row otherwise.
    sg = country == "SG"
    return _body(item_source="cart_link", merchant_domain=KRAVE_DOMAIN, product_key=KRAVE_PK,
                 variant_key=None, buyer=_buyer(shipping_address=dict(ADDRESS, country=country)),
                 expected_unit_price_minor=minor or (3600 if sg else 2800),
                 expected_currency="SGD" if sg else "USD")


_SGD_SG = {"currency": "SGD", "market": "SG"}
#: The mirror writer never sets `market`, so an SG seed's SGD offer carries the column default.
_SGD_DEFAULT_MARKET = {"currency": "SGD", "market": "US"}
_SUPPRESSED = {"suppression_reason": "stale_source", "suppressed_at": "2026-09-28 00:00:00"}


@pytest.mark.parametrize("offers,price", [
    # THE LIVE SHAPE: the real sku has no offer, the placeholder has the merchant's only one.
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00")], 2800),
    # both offered, agreeing: the real sku's offer is used (same price)
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"), (KRAVE_REAL, KRAVE_MERCHANT, "28.00")], 2800),
    # only the real sku offered: unchanged from before
    ([(KRAVE_REAL, KRAVE_MERCHANT, "28.00")], 2800),
    # two placeholder offers from this merchant that agree
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"), (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00")], 2800),
    # THE REAL SKU WINS (review of #2457): a disagreeing placeholder is never read, in either
    # direction -- the real sku's offer is variant-scoped, and Reap's subtotal check catches stale.
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"), (KRAVE_REAL, KRAVE_MERCHANT, "26.00")], 2600),
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "24.00"), (KRAVE_REAL, KRAVE_MERCHANT, "26.00")], 2600),
    # THE MARKET'S CURRENCY: the same seller's SGD placeholder offer beside the USD one is not a
    # disagreement for a US buyer -- whether it is stamped SG or carries the mirror's default 'US'.
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"),
      (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "36.00", _SGD_SG)], 2800),
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"),
      (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "36.00", _SGD_DEFAULT_MARKET)], 2800),
    # a real sku plus an SG/SGD placeholder offer
    ([(KRAVE_REAL, KRAVE_MERCHANT, "28.00"), (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "36.00", _SGD_SG)], 2800),
    # `_CART_OFFER_SQL` across currencies: the real sku's SGD offer is numerically cheaper, and is
    # still not the US buyer's price (it used to be picked, then refused `row_currency_mismatch`).
    ([(KRAVE_REAL, KRAVE_MERCHANT, "28.00"), (KRAVE_REAL, KRAVE_MERCHANT, "20.00", _SGD_SG)], 2800),
    ([(KRAVE_REAL, KRAVE_MERCHANT, "28.00"),
      (KRAVE_REAL, KRAVE_MERCHANT, "20.00", _SGD_DEFAULT_MARKET)], 2800),
    # an OUT-OF-STOCK placeholder offer at another price is not a price
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"),
      (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "30.00", {"availability": "out_of_stock"})], 2800),
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"),
      (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "30.00", {"availability": "Sold_Out"})], 2800),
    # a SUPPRESSED placeholder offer at another price is not a price (either suppression column)
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"),
      (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "30.00", _SUPPRESSED)], 2800),
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"),
      (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "30.00", {"suppression_reason": "stale_source"})], 2800),
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"),
      (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "30.00", {"suppressed_at": "2026-09-28 00:00:00"})], 2800),
])
async def test_a_mirror_row_is_priced_from_its_placeholder_offer(client, monkeypatch, offers, price):
    await _seed_krave_mirror(offers=offers)
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_krave_body(minor=price))
    assert resp.status_code == 202, resp.text
    purchase = await _purchase_row(resp.json()["purchase_id"])
    assert purchase["our_price_minor"] == price and purchase["currency"] == "USD"
    assert f"https://{KRAVE_DOMAIN}/cart/{KRAVE_VARIANT}:1?" in purchase["cart_url"]


@pytest.mark.parametrize("offers", [
    # the mirror's own shape: the SGD offer carries the column default market 'US'. Filtering on
    # `market` (not currency) would refuse every SG mirror row.
    [(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "36.00", _SGD_DEFAULT_MARKET)],
    [(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "36.00", _SGD_SG)],
    # the same seller's USD offer is not a disagreement for an SG buyer
    [(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"),
     (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "36.00", _SGD_DEFAULT_MARKET)],
    # and the real sku's cheaper USD offer is not the SG buyer's price
    [(KRAVE_REAL, KRAVE_MERCHANT, "20.00"), (KRAVE_REAL, KRAVE_MERCHANT, "36.00", _SGD_SG)],
])
async def test_an_sg_mirror_row_is_priced_in_sgd(client, monkeypatch, offers):
    await _seed_krave_mirror(offers=offers, market="SG")
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_krave_body(country="SG"))
    assert resp.status_code == 202, resp.text
    purchase = await _purchase_row(resp.json()["purchase_id"])
    assert purchase["our_price_minor"] == 3600 and purchase["currency"] == "SGD"


@pytest.mark.parametrize("offers,reason", [
    # two placeholder offers from this merchant that disagree
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"), (KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "30.00")],
     "row_price_ambiguous"),
    # the only placeholder offer is ANOTHER merchant's
    ([(KRAVE_PLACEHOLDER, "merch_obs_0000000000000000", "28.00")], "row_unpriced"),
    # the only placeholder offer is suppressed / out of stock / in another market's currency
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00", _SUPPRESSED)], "row_unpriced"),
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00", {"availability": "out_of_stock"})], "row_unpriced"),
    ([(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "36.00", _SGD_DEFAULT_MARKET)], "row_unpriced"),
    # no offer anywhere
    ([], "row_unpriced"),
])
async def test_a_mirror_row_placeholder_price_refusals(client, monkeypatch, offers, reason):
    await _seed_krave_mirror(offers=offers)
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_krave_body())
    assert resp.status_code == 409 and _error(resp) == reason, resp.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


@pytest.mark.parametrize("live_variant_count,offers,expected", [
    # control: the stubbed proof below changes nothing when the count is one
    (1, [(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00")], 202),
    # MORE THAN ONE LIVE VARIANT: the product-grain placeholder offer is never that variant's price
    (2, [(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00")], "row_unpriced"),
    (3, [(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00")], "row_unpriced"),
    # the count is an int, exactly as the proof reads it: a string "1" is not one live variant
    ("1", [(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00")], "row_unpriced"),
    # ... while the real sku's own (variant-scoped) offer still prices it
    (2, [(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00"), (KRAVE_REAL, KRAVE_MERCHANT, "26.00")], 202),
])
async def test_a_multi_variant_proof_never_prices_from_the_placeholder(
    client, monkeypatch, live_variant_count, offers, expected
):
    """FORWARD COMPAT: a variant-scoped proof of a multi-variant product (another PR) must not
    reach the placeholder's price. Today's proof refuses such a snapshot before pricing, so the
    proof reader is stubbed to accept it; the gate under test reads the snapshot itself."""
    await _seed_krave_mirror(offers=offers, live_variant_count=live_variant_count)
    # #2459 renamed the route's authority call; the stub is the SOLE-scope answer it replaced.
    monkeypatch.setattr(routes_reap, "verified_cart_variant_id",
                        lambda *a, **k: ProvenCartVariant(KRAVE_VARIANT, "sole_variant"))
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    # The real sku's own offer, when there is one, is the price shown; else the placeholder's.
    shown = 2600 if any(sku == KRAVE_REAL for sku, *_ in offers) else 2800
    resp = await client.post(f"{BASE}/purchases", json=_krave_body(minor=shown))
    if expected == 202:
        assert resp.status_code == 202, resp.text
    else:
        assert resp.status_code == 409 and _error(resp) == expected, resp.text
        assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


@pytest.mark.parametrize("live_variant_count,expected", [
    (1, 202), (2, "row_unpriced"), (3, "row_unpriced"), ("1", "row_unpriced"),
])
async def test_a_named_placeholder_meets_the_same_live_variant_gate(
    client, monkeypatch, live_variant_count, expected
):
    """NAMING the placeholder is not a way around the gate (review of #2457): a placeholder-only
    row, the caller naming `<pk>::canonical`, is priced from it only under one live variant. The
    proof reader is stubbed as in the unnamed test above."""
    await _seed_krave_mirror(offers=[(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00")],
                             live_variant_count=live_variant_count, real_sku=False)
    # #2459 renamed the route's authority call; the stub is the SOLE-scope answer it replaced.
    monkeypatch.setattr(routes_reap, "verified_cart_variant_id",
                        lambda *a, **k: ProvenCartVariant(KRAVE_VARIANT, "sole_variant"))
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases",
                             json={**_krave_body(), "variant_key": KRAVE_PLACEHOLDER})
    if expected == 202:
        assert resp.status_code == 202, resp.text
        assert (await _purchase_row(resp.json()["purchase_id"]))["our_price_minor"] == 2800
    else:
        assert resp.status_code == 409 and _error(resp) == expected, resp.text
        assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_a_named_real_mirror_sku_with_no_offer_is_not_priced_from_the_placeholder(client, monkeypatch):
    """The placeholder price is the NO-name path's; a caller who named the real sku buys at THAT
    sku's price or not at all."""
    await _seed_krave_mirror(offers=[(KRAVE_PLACEHOLDER, KRAVE_MERCHANT, "28.00")])
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json={**_krave_body(), "variant_key": KRAVE_REAL})
    assert resp.status_code == 409 and _error(resp) == "row_unpriced"


async def test_a_shopify_row_is_never_priced_from_a_placeholder(client, monkeypatch):
    """Shopify rows are unchanged: a placeholder offer does not price the chosen real sku."""
    await _seed_tierb_shopify_item()
    await _seed_tierb_verdict()
    placeholder = f"{PRODUCT_KEY}::canonical"
    await database.execute(
        "INSERT INTO catalog_skus (sku_key, product_key, merchant_id, platform, source_product_id, "
        "source_variant_id, title, currency) VALUES (:sk, :pk, 'm_brand', 'shopify', '1001', :pk, "
        "'Standard', 'USD')", {"sk": placeholder, "pk": PRODUCT_KEY})
    await database.execute(
        "UPDATE catalog_offers SET sku_key = :ph WHERE sku_key = :sk", {"ph": placeholder, "sk": SKU_KEY})
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    resp = await client.post(f"{BASE}/purchases", json=_body(item_source="cart_link", variant_key=None))
    assert resp.status_code == 409 and _error(resp) == "row_unpriced"


@pytest.mark.parametrize("missing_credentials", [False, True])
async def test_disarmed_status_read_retains_identity_checks_and_has_no_provider_work(client, monkeypatch, missing_credentials):
    await _seed_all()
    purchase_id = (await client.post(f"{BASE}/purchases", json=_body())).json()["purchase_id"]
    monkeypatch.setenv("REAP_AGENTIC_ENABLED", "0")
    if missing_credentials:
        monkeypatch.delenv("REAP_API_KEY", raising=False)
    import services.reap_agentic_purchase as purchase_svc
    import services.reap_agentic_client as reap_client
    async def forbidden(*args, **kwargs):
        raise AssertionError("stored status read attempted provider work")
    monkeypatch.setattr(purchase_svc, "advance", forbidden)
    for name in ("get_checkout", "create_checkout", "create_enrollment", "request_quote"):
        monkeypatch.setattr(reap_client, name, forbidden)
    response = await client.get(f"{BASE}/purchases/{purchase_id}")
    assert response.status_code == 200 and response.json()["id"] == purchase_id
    # A create remains dark; neither owner conjunct nor required user session is relaxed.
    assert (await client.post(f"{BASE}/purchases", json=_body())).status_code == 404
    CALLER.agent_user_ref = OTHER_USER_REF
    denied = await client.get(f"{BASE}/purchases/{purchase_id}")
    assert denied.status_code == 404 and _error(denied) == "purchase_not_found"
    CALLER.agent_user_ref = USER_REF
    CALLER.agent_id = OTHER_AGENT
    denied = await client.get(f"{BASE}/purchases/{purchase_id}")
    assert denied.status_code == 404 and _error(denied) == "purchase_not_found"
    CALLER.agent_id = AGENT
    CALLER.agent_user_ref = None
    denied = await client.get(f"{BASE}/purchases/{purchase_id}")
    assert denied.status_code == 401 and _error(denied) == "agent_user_required"






# Durable recovery never opens another checkout or refreshes identity/consent.
@pytest.mark.parametrize("state", ["awaiting_approval", "processing", "completed"])
async def test_recovery_after_25_days_is_read_only_while_create_is_paused(client, monkeypatch, state):
    await _seed_all()
    body = _body(idempotency_key="recover-old")
    first = await client.post(f"{BASE}/purchases", json=body)
    purchase_id = first.json()["purchase_id"]
    old = datetime.now(timezone.utc) - timedelta(days=25)
    await database.execute("UPDATE reap_agentic_purchase_keys SET created_at=:old", {"old": old})
    await database.execute(
        "UPDATE reap_agentic_purchases SET state=:state, reap_checkout_id=:checkout WHERE id=:id",
        {"state": state, "checkout": "synthetic-paid-checkout", "id": purchase_id})
    key_before = dict(await database.fetch_one("SELECT * FROM reap_agentic_purchase_keys"))
    purchase_before = dict(await database.fetch_one("SELECT * FROM reap_agentic_purchases WHERE id=:id", {"id": purchase_id}))
    monkeypatch.setenv("REAP_AGENTIC_ENABLED", "0")
    monkeypatch.delenv("REAP_API_KEY", raising=False)
    monkeypatch.setenv("REAP_CART_LINK_ENABLED", "0")
    async def forbidden(*args, **kwargs):
        raise AssertionError("read-only recovery attempted a write or create")
    monkeypatch.setattr(database, "execute", forbidden)
    monkeypatch.setattr(routes_reap, "_record_consent", forbidden)
    monkeypatch.setattr(routes_reap, "_buyer_id_for", forbidden)
    monkeypatch.setattr(svc, "start_purchase", forbidden)
    for _ in range(2):
        recovered = await client.post(f"{BASE}/purchases/recover", json=body)
        assert recovered.status_code == 200, recovered.text
        assert recovered.json()["id"] == purchase_id
        assert recovered.json()["state"] == state
        for pii in PII_STRINGS:
            assert pii not in recovered.text
    assert dict(await database.fetch_one("SELECT * FROM reap_agentic_purchase_keys")) == key_before
    assert dict(await database.fetch_one("SELECT * FROM reap_agentic_purchases WHERE id=:id", {"id": purchase_id})) == purchase_before
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 1


@pytest.mark.parametrize("state", ["awaiting_approval", "processing", "completed"])
async def test_create_after_25_days_replays_even_a_possibly_paid_attempt(client, monkeypatch, state):
    await _seed_all()
    body = _body(idempotency_key="create-lifetime")
    first = await client.post(f"{BASE}/purchases", json=body)
    purchase_id = first.json()["purchase_id"]
    await database.execute("UPDATE reap_agentic_purchase_keys SET created_at=:old",
                           {"old": datetime.now(timezone.utc) - timedelta(days=25)})
    await database.execute("UPDATE reap_agentic_purchases SET state=:state, reap_checkout_id=:checkout WHERE id=:id",
                           {"state": state, "checkout": "synthetic-checkout", "id": purchase_id})
    async def forbidden(*args, **kwargs):
        raise AssertionError("lifetime replay started a new purchase")
    monkeypatch.setattr(svc, "start_purchase", forbidden)
    again = await client.post(f"{BASE}/purchases", json=body)
    assert again.status_code == 202, again.text
    assert again.json()["purchase_id"] == purchase_id
    assert again.json()["status"] == state
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 1
    conflict = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="create-lifetime", quantity=2))
    assert conflict.status_code == 409 and _error(conflict) == "idempotency_conflict"


@pytest.mark.parametrize("changed", ["quantity", "address", "variant", "source", "offer", "return_url"])
async def test_recovery_requires_the_original_request_fingerprint(client, monkeypatch, changed):
    await _seed_all()
    body = _body(idempotency_key="recover-fingerprint")
    assert (await client.post(f"{BASE}/purchases", json=body)).status_code == 202
    monkeypatch.setenv("REAP_AGENTIC_ENABLED", "0")
    if changed == "quantity":
        body["quantity"] = 2
    elif changed == "address":
        body["buyer"]["shipping_address"]["addressLine1"] = "1 Other Street"
    elif changed == "variant":
        body["variant_key"] = SECOND_SKU_KEY
    elif changed == "source":
        body["item_source"] = "cart_link"
    elif changed == "offer":
        body["offer_code"] = "BUYER-CODE"
    else:
        body["return_url"] = "https://shop.example/reap/changed"
    response = await client.post(f"{BASE}/purchases/recover", json=body)
    assert response.status_code == 409 and _error(response) == "idempotency_conflict", response.text


@pytest.mark.parametrize("owner", ["agent", "buyer", "missing_user"])
async def test_recovery_keeps_the_existing_identity_and_owner_checks(client, owner):
    await _seed_all()
    body = _body(idempotency_key="recover-owner")
    assert (await client.post(f"{BASE}/purchases", json=body)).status_code == 202
    if owner == "agent":
        CALLER.agent_id = OTHER_AGENT
    elif owner == "buyer":
        CALLER.agent_user_ref = OTHER_USER_REF
    else:
        CALLER.agent_user_ref = None
    response = await client.post(f"{BASE}/purchases/recover", json=body)
    assert response.status_code == (401 if owner == "missing_user" else 404), response.text


@pytest.mark.parametrize("key", [None, "", "   "])
async def test_recovery_requires_an_opaque_attempt_key(client, key):
    response = await client.post(f"{BASE}/purchases/recover", json=_body(idempotency_key=key))
    assert response.status_code == 400, response.text


@pytest.mark.parametrize("mapping", ["missing", "tombstone", "deleted", "unowned", "unverifiable"])
async def test_recovery_missing_or_unverifiable_evidence_cannot_create(client, monkeypatch, mapping):
    body = _body(idempotency_key="recover-evidence")
    if mapping == "tombstone":
        await _seed_all()
        body = _legacy(body)  # a tombstone was only ever written for a money-less body
        await _plant_legacy_tombstone(body)
        assert _error(await client.post(f"{BASE}/purchases", json=body)) == "merchant_not_eligible"
    elif mapping != "missing":
        await _seed_all()
        assert (await client.post(f"{BASE}/purchases", json=body)).status_code == 202
        if mapping == "deleted":
            await database.execute("DELETE FROM reap_agentic_purchases")
        elif mapping == "unowned":
            await database.execute("UPDATE reap_agentic_purchases SET agent_id=:agent", {"agent": OTHER_AGENT})
        else:
            await database.execute("UPDATE reap_agentic_purchase_keys SET request_hash=''")
    monkeypatch.setenv("REAP_AGENTIC_ENABLED", "0")
    response = await client.post(f"{BASE}/purchases/recover", json=body)
    expected = 409 if mapping == "unverifiable" else 404
    assert response.status_code == expected, response.text
    if mapping == "unverifiable":
        assert _error(response) == "idempotency_conflict"
    if mapping in {"deleted", "unowned"}:
        monkeypatch.setenv("REAP_AGENTIC_ENABLED", "1")
        replay = await client.post(f"{BASE}/purchases", json=body)
        assert replay.status_code == 404, replay.text


async def test_an_aged_key_cannot_be_overwritten_at_the_sql_write_seam(client):
    await _seed_all()
    assert (await client.post(f"{BASE}/purchases", json=_body(idempotency_key="immutable-seam"))).status_code == 202
    await database.execute("UPDATE reap_agentic_purchase_keys SET created_at=:old",
                           {"old": datetime.now(timezone.utc) - timedelta(days=25)})
    before = dict(await database.fetch_one("SELECT * FROM reap_agentic_purchase_keys"))
    landed = await routes_reap._write_idempotency_key(
        agent_id=AGENT, agent_user_ref_hash=hash_agent_user_ref(USER_REF),
        idempotency_key="immutable-seam", purchase_id="replacement-forbidden", request_hash="x"*64)
    assert landed is False
    assert dict(await database.fetch_one("SELECT * FROM reap_agentic_purchase_keys")) == before


async def test_recovery_accepts_a_new_valid_consent_tag_without_rewriting_original_consent(client, monkeypatch):
    await _seed_all()
    body = _body(idempotency_key="recover-consent", buyer=_buyer(consent_version="v1"))
    purchase_id = (await client.post(f"{BASE}/purchases", json=body)).json()["purchase_id"]
    buyer_before = dict(await database.fetch_one("SELECT * FROM reap_agentic_buyer_refs"))
    original = dict(await database.fetch_one("SELECT consent_version, consented_at FROM reap_agentic_purchases WHERE id=:id", {"id": purchase_id}))
    monkeypatch.setenv("REAP_AGENTIC_ENABLED", "0")
    body["buyer"]["consent_version"] = "v2"
    response = await client.post(f"{BASE}/purchases/recover", json=body)
    assert response.status_code == 200 and response.json()["id"] == purchase_id, response.text
    assert response.json()["consent_version"] == "v1"
    assert dict(await database.fetch_one("SELECT * FROM reap_agentic_buyer_refs")) == buyer_before
    assert dict(await database.fetch_one("SELECT consent_version, consented_at FROM reap_agentic_purchases WHERE id=:id", {"id": purchase_id})) == original


async def test_recovery_does_not_silently_recompute_a_changed_default_return_url(client, monkeypatch):
    await _seed_all()
    body = _body(idempotency_key="recover-return-default")
    assert (await client.post(f"{BASE}/purchases", json=body)).status_code == 202
    monkeypatch.setenv("REAP_AGENTIC_RETURN_URL", "https://shop.example/reap/changed")
    response = await client.post(f"{BASE}/purchases/recover", json=body)
    assert response.status_code == 409 and _error(response) == "idempotency_conflict", response.text



async def _seed_atomic_request_lane(monkeypatch, lane):
    await _seed_all()
    if lane == "cart_link":
        monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
        await database.execute("UPDATE catalog_products SET seller_ref=merchant_id WHERE product_key=:p", {"p": PRODUCT_KEY})
        await database.execute("UPDATE catalog_skus SET source_variant_id='50041364447509' WHERE sku_key=:s", {"s": SKU_KEY})
        await database.execute("INSERT INTO tierb_cart_link_eligibility (shop_domain,market,verdict,checked_at,consecutive_same) VALUES (:d,'US','ELIGIBLE',CURRENT_TIMESTAMP,1)", {"d": DOMAIN})

# Durable create/key atomicity: a worker must never see a purchase without its key.
@pytest.mark.parametrize("failure", ["key_constraint", "after_purchase"])
@pytest.mark.parametrize("lane", ["reap_variant", "cart_link"])
async def test_atomic_key_failure_rolls_back_purchase_and_retry_cannot_duplicate(client, monkeypatch, failure, lane):
    await _seed_atomic_request_lane(monkeypatch, lane)
    body = _body(item_source=lane, idempotency_key="atomic-failed-insert")
    seam = "_write_idempotency_key" if failure == "key_constraint" else "_claim_idempotency_key"
    real = getattr(routes_reap, seam)

    async def unavailable(**kwargs):
        if failure == "key_constraint":
            # Actual statement failure on each engine, not a fake database response.
            await database.execute("INSERT INTO reap_agentic_purchase_keys (purchase_id) VALUES ('synthetic-key-fault')")
        raise RuntimeError("synthetic failure after purchase, before durable key")

    monkeypatch.setattr(routes_reap, seam, unavailable)
    response = await client.post(f"{BASE}/purchases", json=body)
    assert response.status_code == 503, response.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == 0
    assert (await client.post(f"{BASE}/purchases/recover", json=body)).status_code == 404
    monkeypatch.setattr(routes_reap, seam, real)
    accepted = await client.post(f"{BASE}/purchases", json=body)
    assert accepted.status_code == 202, accepted.text
    replay = await client.post(f"{BASE}/purchases", json=body)
    assert replay.json()["purchase_id"] == accepted.json()["purchase_id"]
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 1
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == 1


@pytest.mark.parametrize("lane", ["reap_variant", "cart_link"])
async def test_atomic_key_cancellation_after_purchase_rolls_back(client, monkeypatch, lane):
    import asyncio
    await _seed_atomic_request_lane(monkeypatch, lane)
    async def cancelled(**kwargs):
        raise asyncio.CancelledError()
    monkeypatch.setattr(routes_reap, "_claim_idempotency_key", cancelled)
    from starlette.requests import Request
    raw = json.dumps(_body(item_source=lane, idempotency_key="atomic-cancelled")).encode()
    async def receive():
        return {"type": "http.request", "body": raw, "more_body": False}
    request = Request({"type": "http", "method": "POST", "path": f"{BASE}/purchases", "headers": []}, receive)
    # Exercise cancellation at the actual handler. ASGITransport+middleware cannot
    # represent a cancelled response and replaces it with its own assertion.
    with pytest.raises(asyncio.CancelledError):
        await routes_reap.start_reap_purchase(request, context=CALLER, agent_user=AgentUserContext(agent_user_ref=USER_REF))
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == 0


@pytest.mark.parametrize("lane", ["reap_variant", "cart_link"])
async def test_atomic_key_worker_connection_cannot_observe_precommit_purchase(client, monkeypatch, lane):
    from databases import Database
    await _seed_atomic_request_lane(monkeypatch, lane)
    observer = Database(os.environ["DATABASE_URL"])
    await observer.connect()
    seen = []
    real = svc.start_purchase
    async def observed_start(**kwargs):
        purchase_id = await real(**kwargs)
        seen.append(await observer.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases WHERE id=:p", {"p": purchase_id}))
        return purchase_id
    monkeypatch.setattr(svc, "start_purchase", observed_start)
    try:
        response = await client.post(f"{BASE}/purchases", json=_body(item_source=lane, idempotency_key="atomic-worker-visibility"))
        assert response.status_code == 202, response.text
        assert seen == [0], "another database connection saw the purchase before its key was durable"
        assert await observer.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 1
        assert await observer.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == 1
    finally:
        await observer.disconnect()


@pytest.mark.parametrize("different_body", [False, True])
@pytest.mark.parametrize("lane", ["reap_variant", "cart_link"])
async def test_atomic_key_concurrent_requests_have_one_claimable_purchase(client, monkeypatch, different_body, lane):
    import asyncio
    import contextvars
    await _seed_atomic_request_lane(monkeypatch, lane)
    real = routes_reap._replayed_purchase_id
    arrived = 0
    both = asyncio.Event()
    async def simultaneous_miss(**kwargs):
        nonlocal arrived
        if arrived < 2:
            result = await real(**kwargs)
            assert result is None
            arrived += 1
            if arrived == 2:
                both.set()
            await asyncio.wait_for(both.wait(), timeout=5)
            return None
        return await real(**kwargs)
    monkeypatch.setattr(routes_reap, "_replayed_purchase_id", simultaneous_miss)
    # Independent HTTP requests must not inherit the fixture's existing
    # databases0.7 ContextVar connection and share a transaction stack.
    posts = [
        client.post(f"{BASE}/purchases", json=_body(item_source=lane, idempotency_key="atomic-concurrent", quantity=1)),
        client.post(f"{BASE}/purchases", json=_body(item_source=lane, idempotency_key="atomic-concurrent", quantity=2 if different_body else 1)),
    ]
    tasks = [contextvars.Context().run(asyncio.create_task, post) for post in posts]
    first, second = await asyncio.wait_for(asyncio.gather(*tasks), timeout=15)
    assert sorted([first.status_code, second.status_code]) == ([202, 409] if different_body else [202, 202]), (first.text, second.text)
    if not different_body:
        assert first.json()["purchase_id"] == second.json()["purchase_id"]
    else:
        refused = first if first.status_code == 409 else second
        assert _error(refused) == "idempotency_conflict"
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == 1
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases WHERE state='resolving'") == 1
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases WHERE state='refused' AND (buyer_email IS NOT NULL OR shipping_address IS NOT NULL)") == 0


@pytest.mark.parametrize("lane", ["reap_variant", "cart_link"])
async def test_atomic_key_duplicate_cleanup_failure_cannot_leave_claimable_orphan(client, monkeypatch, lane):
    await _seed_atomic_request_lane(monkeypatch, lane)
    body = _body(item_source=lane, idempotency_key="atomic-cleanup-fault")
    first = await client.post(f"{BASE}/purchases", json=body)
    assert first.status_code == 202, first.text
    real_lookup = routes_reap._replayed_purchase_id
    first_miss = True
    async def miss_before_concurrent_winner(**kwargs):
        nonlocal first_miss
        if first_miss:
            first_miss = False
            return None
        return await real_lookup(**kwargs)
    async def broken_cleanup(*args, **kwargs):
        # A real statement failure, after the immutable key picked the durable winner.
        await database.execute("UPDATE reap_atomic_cleanup_missing_table SET id='synthetic'")
    monkeypatch.setattr(routes_reap, "_replayed_purchase_id", miss_before_concurrent_winner)
    monkeypatch.setattr(ledger, "transition", broken_cleanup)
    retry = await client.post(f"{BASE}/purchases", json=body)
    assert retry.status_code == 503, retry.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 1
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == 1
    recovered = await client.post(f"{BASE}/purchases/recover", json=body)
    assert recovered.status_code == 200
    assert recovered.json()["id"] == first.json()["purchase_id"]


@pytest.mark.parametrize('legacy',[False,True])
@pytest.mark.parametrize('invalid',[False,True])
async def test_deadline_http_get_history_recover_have_identical_stable_handoff(client,monkeypatch,legacy,invalid):
    await _seed_all()
    request_body=_body(idempotency_key='deadline-synthetic-http')
    response=await client.post(f'{BASE}/purchases',json=request_body)
    assert response.status_code==202,response.text
    pid=response.json()['purchase_id']
    row=await ledger.get_purchase_internal(pid)
    link='https://pay.prava.space/enroll/owned_deadline_fixture'
    enrollment=await ledger.upsert_pending_enrollment(buyer_ref=row['buyer_ref'],hosted_url=link,hosted_url_expiry_invalid=invalid)
    deadline=enrollment['created_at']+timedelta(seconds=ledger.HOSTED_SESSION_SECONDS)
    await ledger.transition(pid,from_states=('resolving',),to_state='needs_enrollment',enrollment_id=enrollment['id'],hosted_url=link,hosted_url_expires_at=None if legacy else deadline)
    before=await ledger.get_purchase_internal(pid)
    original_enrollment=await ledger.get_enrollment_internal(enrollment['id'])
    monkeypatch.setenv('REAP_AGENTIC_CREATE_ENABLED','0')
    for _ in range(2):
        direct=await client.get(f'{BASE}/purchases/{pid}')
        history=await client.get(f'{BASE}/purchases')
        recovered=await client.post(f'{BASE}/purchases/recover',json=request_body)
        assert direct.status_code==history.status_code==recovered.status_code==200
        views=[direct.json(),history.json()['purchases'][0],recovered.json()]
        assert views[0]==views[1]==views[2]
        if invalid:assert not views[0].get('hosted_url') and not views[0].get('hosted_url_expires_at')
        else:assert views[0]['hosted_url']==link and views[0]['hosted_url_expires_at']
        for key in ['buyer_ref','buyer_email','shipping_address','enrollment_id','hosted_url_expiry_invalid']:assert key not in views[0]
    assert await ledger.get_purchase_internal(pid)==before
    assert await ledger.get_enrollment_internal(enrollment['id'])==original_enrollment


@pytest.mark.parametrize("flag", ["0", "false", "", "ture", "arbitrary"])
async def test_create_only_pause_precedes_buyer_writes_and_preserves_read_recovery(client, monkeypatch, flag):
    await _seed_all()
    body = _body(idempotency_key="pilot-pause-read")
    purchase_id = (await client.post(f"{BASE}/purchases", json=body)).json()["purchase_id"]
    monkeypatch.setenv("REAP_AGENTIC_CREATE_ENABLED", flag)
    async def forbidden(*args, **kwargs):
        raise AssertionError("paused create reached a buyer write")
    monkeypatch.setattr(routes_reap, "_buyer_id_for", forbidden)
    monkeypatch.setattr(routes_reap, "_record_consent", forbidden)
    response = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="pilot-new"))
    assert response.status_code == 404 and _error(response) == "not_available_on_this_rail", response.text
    assert (await client.get(f"{BASE}/purchases/{purchase_id}")).status_code == 200
    recovered = await client.post(f"{BASE}/purchases/recover", json=body)
    assert recovered.status_code == 200 and recovered.json()["id"] == purchase_id
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 1


@pytest.mark.parametrize("field,value", [
    ("agent_ids", "other-agent"), ("merchant_domains", "other.example"),
    ("markets", "CA"), ("product_keys", "other-product"), ("quantities", 2),
])
async def test_exact_pilot_scope_refuses_each_wrong_dimension_before_buyer_writes(client, monkeypatch, field, value):
    monkeypatch.setenv("PIVOTA_ENV", "staging")
    await _seed_all()
    scope = {"agent_ids": [AGENT], "merchant_domains": [DOMAIN], "markets": ["US"],
             "product_keys": [PRODUCT_KEY], "quantities": [1]}
    scope[field] = [value]
    monkeypatch.setenv("REAP_AGENTIC_PILOT_SCOPE", json.dumps(scope))
    async def forbidden(*args, **kwargs):
        raise AssertionError("outside-pilot request reached buyer writes")
    monkeypatch.setattr(routes_reap, "_buyer_id_for", forbidden)
    monkeypatch.setattr(routes_reap, "_record_consent", forbidden)
    response = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="pilot-denied"))
    assert response.status_code == 404 and _error(response) == "not_available_on_this_rail", response.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchase_keys") == 0


@pytest.mark.parametrize("configured", ["", "not-json", "null", "[]", "{}", '{"unknown":[1]}',
    '{"agent_ids":[]}', '{"agent_ids":"x"}', '{"agent_ids":[""]}', '{"agent_ids":[" x"]}',
    '{"markets":["us"]}', '{"markets":["USA"]}', '{"merchant_domains":["https://brand.example"]}',
    '{"quantities":[true]}', '{"quantities":[0]}', '{"quantities":[1.0]}',
    '{"agent_ids":["a"],"agent_ids":["b"]}'])
async def test_malformed_pilot_configuration_fails_closed(client, monkeypatch, configured):
    monkeypatch.setenv("REAP_AGENTIC_PILOT_SCOPE", configured)
    response = await client.post(f"{BASE}/purchases", json=_body())
    assert response.status_code == 404 and _error(response) == "not_available_on_this_rail", response.text
    assert svc.is_create_enabled() is False
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


async def test_matching_pilot_scope_allows_create_and_later_scope_change_preserves_existing_attempt(client, monkeypatch):
    monkeypatch.setenv("PIVOTA_ENV", "staging")
    await _seed_all()
    monkeypatch.setenv("REAP_AGENTIC_CREATE_ENABLED", "true")
    monkeypatch.setenv("REAP_AGENTIC_PILOT_SCOPE", json.dumps({
        "agent_ids": [AGENT], "merchant_domains": [DOMAIN], "markets": ["US"],
        "product_keys": [PRODUCT_KEY], "quantities": [1]}))
    body = _body(idempotency_key="pilot-allowed")
    created = await client.post(f"{BASE}/purchases", json=body)
    assert created.status_code == 202, created.text
    monkeypatch.setenv("REAP_AGENTIC_PILOT_SCOPE", json.dumps({
        "agent_ids": ["different-agent"], "merchant_domains": [DOMAIN], "markets": ["US"],
        "product_keys": [PRODUCT_KEY], "quantities": [1]}))
    replay = await client.post(f"{BASE}/purchases", json=body)
    assert replay.status_code == 202 and replay.json()["purchase_id"] == created.json()["purchase_id"]
    recovered = await client.post(f"{BASE}/purchases/recover", json=body)
    assert recovered.status_code == 200 and recovered.json()["id"] == created.json()["purchase_id"]
    monkeypatch.setenv("REAP_AGENTIC_PILOT_SCOPE", "malformed")
    assert (await client.post(f"{BASE}/purchases/recover", json=body)).status_code == 200


@pytest.mark.parametrize("missing", ["agent_ids", "merchant_domains", "markets", "product_keys", "quantities"])
async def test_a_partial_pilot_allowlist_fails_closed(client, monkeypatch, missing):
    scope = {"agent_ids": [AGENT], "merchant_domains": [DOMAIN], "markets": ["US"],
             "product_keys": [PRODUCT_KEY], "quantities": [1]}
    del scope[missing]
    monkeypatch.setenv("REAP_AGENTIC_PILOT_SCOPE", json.dumps(scope))
    response = await client.post(f"{BASE}/purchases", json=_body())
    assert response.status_code == 404 and _error(response) == "not_available_on_this_rail", response.text
    assert svc.is_create_enabled() is False
@pytest.mark.parametrize("selection,expected", [("proven", 202), ("other", 409), ("missing", 409)])
async def test_explicit_multi_variant_mirror_selection_keeps_proof_authoritative(client, monkeypatch, selection, expected):
    other_variant = "49819267301654"
    other_key = f"{LIVE_PK}::v:{other_variant}"
    await _seed_named_variant_mirror(env="prod", skus=LIVE_PROD_SKUS + ((other_key, other_variant),), seed_data=_named_variant_seed())
    monkeypatch.setenv("REAP_AGENTIC_CART_LINK_ENABLED", "1")
    selected = {"proven": LIVE_SKU_PROMOTED, "other": other_key, "missing": None}[selection]
    response = await client.post(f"{BASE}/purchases", json={**_live_body(), "variant_key": selected, "idempotency_key": "selected-variant-replay"})
    assert response.status_code == expected, response.text
    if expected == 202:
        row = await _purchase_row(response.json()["purchase_id"])
        replay = await client.post(f"{BASE}/purchases", json={**_live_body(), "variant_key": selected, "idempotency_key": "selected-variant-replay"})
        assert replay.status_code == 202 and replay.json()["purchase_id"] == response.json()["purchase_id"]
        changed = await client.post(f"{BASE}/purchases", json={**_live_body(), "variant_key": other_key, "idempotency_key": "selected-variant-replay"})
        assert changed.status_code == 409 and _error(changed) == "idempotency_conflict"
        assert f"/cart/{LIVE_VARIANT}:1?" in row["cart_url"]
        assert row["our_price_minor"] == 1399
    else:
        assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0

@pytest.mark.parametrize('choice', [0, 1])
async def test_two_selected_mirror_sizes_have_independent_proofs_and_prices(client, monkeypatch, choice):
    from scripts.backfill_shopify_variant_ids import build_selected_variant_proofs
    second='42199434526795'
    second_key=f'{KRAVE_PK}::v::{second}'
    await _seed_krave_mirror(offers=[(KRAVE_REAL, KRAVE_MERCHANT, '16.00'),(second_key, KRAVE_MERCHANT, '27.00')])
    await database.execute("INSERT INTO catalog_skus (sku_key,product_key,merchant_id,platform,source_product_id,source_variant_id,title,currency) VALUES (:sk,:pk,:m,'external_seed',:ext,:vid,'2 Pack','USD')", {'sk':second_key,'pk':KRAVE_PK,'m':KRAVE_MERCHANT,'ext':KRAVE_EXT,'vid':second})
    ids=[KRAVE_VARIANT,second]
    variants=[{'shopify_variant_id':vid,'variant_id':vid} for vid in ids]
    payload={'handle':'24-carrot-retinal','variants':[{'id':int(vid),'available':True,'title':title,'price':price} for vid,title,price in zip(ids,['1 Pack','2 Pack'],[1600,2700])]}
    seed={'snapshot':{'brand':'KraveBeauty','variants':variants,'storefront_platform':'shopify','storefront_platform_source':'products_js_v1','shopify_cart_variant_proofs':build_selected_variant_proofs(variants,payload,js_url=f'https://{KRAVE_DOMAIN}/products/24-carrot-retinal.js',checked_at=datetime.now(timezone.utc))}}
    await database.execute('UPDATE external_product_seeds SET seed_data=:data WHERE id=:id',{'data':json.dumps(seed),'id':KRAVE_SEED})
    monkeypatch.setenv('REAP_AGENTIC_CART_LINK_ENABLED','1')
    response=await client.post(f'{BASE}/purchases',json={**_krave_body(minor=[1600,2700][choice]),'variant_key':[KRAVE_REAL,second_key][choice]})
    assert response.status_code==202,response.text
    row=await _purchase_row(response.json()['purchase_id'])
    assert row['our_price_minor']==[1600,2700][choice]
    assert f'/cart/{ids[choice]}:1?' in row['cart_url']


@pytest.mark.parametrize("field,value", [("variant_keys", ["other-sku"]), ("currency", "EUR"), ("max_total_minor", 4249)])
async def test_full_pilot_resolved_scope_precedes_identity_and_consent(client, monkeypatch, field, value):
    await _seed_all()
    scope = {"agent_ids": [AGENT], "merchant_domains": [DOMAIN], "markets": ["US"],
             "product_keys": [PRODUCT_KEY], "quantities": [1], "variant_keys": [SKU_KEY],
             "currency": "USD", "max_total_minor": 4500}
    scope[field] = value
    monkeypatch.setenv("PIVOTA_ENV", "production")
    monkeypatch.setenv("REAP_AGENTIC_PILOT_SCOPE", json.dumps(scope))
    async def forbidden(*args, **kwargs):
        raise AssertionError("resolved outside-scope request reached buyer writes")
    monkeypatch.setattr(routes_reap, "_buyer_id_for", forbidden)
    monkeypatch.setattr(routes_reap, "_record_consent", forbidden)
    response = await client.post(f"{BASE}/purchases", json=_body(idempotency_key="bounded-resolved-denied"))
    assert response.status_code == 404 and _error(response) == "not_available_on_this_rail", response.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 0


@pytest.mark.parametrize("quantity", [True, 1.0, "1"])
async def test_authenticated_recovery_preserves_original_coerced_quantity_body(client, monkeypatch, quantity):
    await _seed_all()
    body = _body(quantity=1, idempotency_key="legacy-quantity-recover")
    created = await client.post(f"{BASE}/purchases", json=body)
    assert created.status_code == 202, created.text
    original = await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases")
    monkeypatch.setenv("REAP_AGENTIC_CREATE_ENABLED", "0")
    body["quantity"] = quantity
    recovered = await client.post(f"{BASE}/purchases/recover", json=body)
    assert recovered.status_code == 200 and recovered.json()["id"] == created.json()["purchase_id"], recovered.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == original


@pytest.mark.parametrize("lane", ["reap_variant", "cart_link"])
async def test_full_production_pilot_admits_resolved_primary_route_and_replays(client, monkeypatch, lane):
    await _seed_atomic_request_lane(monkeypatch, lane)
    identity = "shopify:50041364447509" if lane == "cart_link" else SKU_KEY
    scope = {"agent_ids": [AGENT], "merchant_domains": [DOMAIN], "markets": ["US"],
             "product_keys": [PRODUCT_KEY], "quantities": [1], "variant_keys": [identity],
             "currency": "USD", "max_total_minor": 4500}
    monkeypatch.setenv("PIVOTA_ENV", "production")
    monkeypatch.setenv("REAP_AGENTIC_PILOT_SCOPE", json.dumps(scope))
    body = _body(item_source=lane, merchant_domain=("www." + DOMAIN if lane == "reap_variant" else DOMAIN), idempotency_key="bounded-primary-route")
    created = await client.post(f"{BASE}/purchases", json=body)
    assert created.status_code == 202, created.text
    purchase = await ledger.get_purchase_internal(created.json()["purchase_id"])
    assert purchase["variant_key"] == identity and purchase["currency"] == "USD"
    replayed = await client.post(f"{BASE}/purchases", json=body)
    assert replayed.status_code == 202 and replayed.json()["purchase_id"] == purchase["id"], replayed.text
    assert await database.fetch_val("SELECT COUNT(*) FROM reap_agentic_purchases") == 1
