"""The legacy offer aggregation is byte-identical to origin/main's, after the market_prices change.

migration 263 refactored aggregate_offers (the offer projection and the sort key moved into
shared helpers so build_market_prices reads the same offers in the same order). Every existing
reader of agent_pdp_view's currency / price_min / price_max / offer_count / offers depends on that
refactor changing nothing. A same-branch flag-off vs flag-on comparison cannot show it -- both
sides run the refactored code -- so this compares against the PRE-CHANGE implementation, frozen
below verbatim from origin/main 70d8413e2 (comments stripped). The two helpers it calls,
coalesce_first and availability_is_known_unavailable, are not touched by the change and are
imported live.

Then the same comparison through assemble_row, with the write flag on, so the flag cannot move
a legacy column either.
"""

from __future__ import annotations

import json
import random
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import agent_pdp_view_assembler as current  # noqa: E402
from services.agent_pdp_view_assembler import coalesce_first  # noqa: E402
from services.offer_buyability import availability_is_known_unavailable  # noqa: E402

FROZEN_OFFER_TOP_N = 5


# ---- frozen: origin/main 70d8413e2 services/agent_pdp_view_assembler.py ---------------------
def frozen_normalize_offer(offer: Dict[str, Any], primary_merchant_id: Optional[str]) -> Optional[Dict[str, Any]]:
    price = coalesce_first(
        offer.get("merchant_effective_price"),
        offer.get("estimated_best_price"),
        offer.get("list_price"),
    )
    if price is None:
        return None
    try:
        price_decimal = Decimal(price)
    except Exception:
        return None
    if price_decimal <= 0:
        return None
    return {
        "merchant_id": offer.get("merchant_id"),
        "merchant_name": offer.get("merchant_name"),
        "price": float(price_decimal),
        "currency": offer.get("currency"),
        "availability": offer.get("availability"),
        "url": None,
        "is_primary": offer.get("merchant_id") == primary_merchant_id,
        "market": offer.get("market"),
        "offer_type": offer.get("offer_type"),
        "is_first_party": bool(offer.get("is_first_party")),
    }


def frozen_aggregate_offers(
    offers: List[Dict[str, Any]],
    primary_merchant_id: Optional[str],
    merchant_url_by_id: Dict[str, Optional[str]],
    seller_trust_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[Optional[str], Optional[Decimal], Optional[Decimal], int, List[Dict[str, Any]]]:
    trust_by_id = seller_trust_by_id or {}
    normalized: List[Dict[str, Any]] = []
    for o in offers:
        n = frozen_normalize_offer(o, primary_merchant_id)
        if not n:
            continue
        merchant_id = n.get("merchant_id") or ""
        n["url"] = merchant_url_by_id.get(merchant_id)
        trust = trust_by_id.get(merchant_id)
        if trust:
            n["seller_trust"] = trust
        normalized.append(n)

    if not normalized:
        return None, None, None, 0, []

    currency_counts: Dict[str, int] = {}
    for n in normalized:
        c = n.get("currency") or ""
        if c:
            currency_counts[c] = currency_counts.get(c, 0) + 1
    currency = (
        max(currency_counts.items(), key=lambda kv: (kv[1], kv[0]))[0]
        if currency_counts
        else None
    )

    prices_in_currency = [
        Decimal(str(n["price"])) for n in normalized
        if currency is None or n.get("currency") == currency
    ]
    price_min = min(prices_in_currency) if prices_in_currency else None
    price_max = max(prices_in_currency) if prices_in_currency else None

    def sort_key(o: Dict[str, Any]) -> Tuple[int, int, float, str]:
        return (
            1 if availability_is_known_unavailable(o.get("availability")) else 0,
            0 if o.get("is_primary") else 1,
            float(o.get("price") or 0.0),
            o.get("merchant_id") or "",
        )

    top = sorted(normalized, key=sort_key)[:FROZEN_OFFER_TOP_N]
    return currency, price_min, price_max, len(normalized), top
# ---- end frozen ------------------------------------------------------------------------------


def _random_offers(rnd: random.Random) -> List[Dict[str, Any]]:
    """The reviewer's differential generator: messy currencies (case, padding, blank, None),
    unparseable and non-positive prices, every availability spelling the vocabulary meets."""
    currencies = ["USD", "SGD", "usd", " USD", None, "", "KRW"]
    availabilities = ["in_stock", "out_of_stock", "sold_out", None, "unknown", "preorder"]
    prices = [None, "0", "-1", "10", "10.00", "9.99", "20", "abc", "1e2"]
    return [
        {
            "offer_id": f"of_{k}", "merchant_id": rnd.choice(["m1", "m2", "m3", None, ""]),
            "merchant_name": "x", "currency": rnd.choice(currencies),
            "availability": rnd.choice(availabilities), "list_price": rnd.choice(prices),
            "merchant_effective_price": rnd.choice([None, None, "8.5"]),
            "estimated_best_price": None, "market": rnd.choice(["US", "SG", None]),
            "offer_type": "retail", "is_first_party": rnd.random() < 0.2,
        }
        for k in range(rnd.randint(0, 9))
    ]


def test_aggregate_offers_matches_origin_main_on_5000_random_offer_sets() -> None:
    rnd = random.Random(7)
    for _ in range(5000):
        offers = _random_offers(rnd)
        urls = {"m1": "https://m1.example.com/p", "m2": None}
        trust = {"m1": {"t": 1}} if rnd.random() < 0.5 else None
        primary = rnd.choice(["m1", "m2", None])
        expected = frozen_aggregate_offers(offers, primary, urls, trust)
        got = current.aggregate_offers(offers, primary, urls, trust)
        assert json.dumps(got, default=str) == json.dumps(expected, default=str), offers
        # The summary must never raise on anything aggregate_offers accepts.
        current.build_market_prices(offers, primary, urls, trust)


def _offer(n, merchant, currency, price, availability="in_stock", market="US"):
    return {
        "offer_id": f"of_{n}", "sku_key": f"sku_{n}", "product_key": "pk_1",
        "merchant_id": merchant, "availability": availability, "currency": currency,
        "list_price": Decimal(price), "merchant_effective_price": None,
        "estimated_best_price": None, "market": market, "offer_type": "retail",
        "is_first_party": False, "merchant_name": merchant,
    }


FIXTURES = {
    "usd_only": [_offer(1, "m_store", "USD", "24.00"), _offer(2, "m_a", "USD", "21.00"),
                 _offer(3, "m_b", "USD", "22.50", availability="out_of_stock")],
    "usd_plus_sgd_siblings": [_offer(i, f"m_us{i}", "USD", f"{18 + i}.00") for i in range(6)]
    + [_offer(10, "m_store", "SGD", "32.00", market="SG"), _offer(11, "m_sg1", "SGD", "29.90", market="SG")],
    "tie_usd_sgd": [_offer(1, "m_a", "USD", "10.00"), _offer(2, "m_b", "SGD", "13.00", market="SG")],
    "sgd_only": [_offer(1, "m_store", "SGD", "32.00", market="SG")],
    "no_priced_offer": [],
}


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_assemble_row_legacy_columns_match_origin_main_with_the_flag_on(monkeypatch, name) -> None:
    monkeypatch.setenv(current.MARKET_PRICES_FLAG_ENV, "on")
    offers = FIXTURES[name]
    row = current.assemble_row(
        content_key="ck_" + "a" * 32,
        products=[{
            "product_key": "pk_1", "merchant_id": "m_store", "platform": "shopify",
            "source_product_id": "sp_1", "title": "Barrier Cream", "description": None,
            "brand": "Example", "product_payload": {}, "pdp_lifecycle_stage": "published",
            "pivota_signature_id": "sig_" + "a" * 32, "canonical_url": "https://store.example.com/p/1",
            "sync_status": "live", "product_group_id": "grp_1", "group_is_primary": True,
        }],
        skus=[], offers=offers, external_seed=None,
    )
    currency, price_min, price_max, count, top = frozen_aggregate_offers(
        offers, "m_store", {"m_store": "https://store.example.com/p/1"}, None,
    )
    assert (row["currency"], row["price_min"], row["price_max"], row["offer_count"], row["offers"]) == (
        currency, price_min, price_max, count, top or None,
    )
