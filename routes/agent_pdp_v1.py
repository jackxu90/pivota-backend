"""Agent PDP v1 read path backed by agent_pdp_view.

This endpoint is intentionally boring: one indexed SELECT against the
denormalized Stage 3a table and no fallback joins or hot-path enrichment.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Mapping, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder

from db.database import database
from services.agent_pdp_view_assembler import (
    agent_pdp_view_has_market_prices,
    is_missing_market_prices_column_error,
    mark_market_prices_columns_missing,
    market_prices_entry,
    market_prices_fresh_sql,
    normalize_taxonomy_tags,
)
from services.catalog_identity import is_content_key
from services.catalog_sync_service import pivota_canonical_pdp_url
from services.claim_safety import substantiated_claims
from services.independent_signals import independent_signals_for
from services.offer_buyability import (
    DEFAULT_SERVING_MARKET,
    annotate_offer_buyability,
    expected_currency_for_market,
)
from services.pdp_renderability import sig_pdp_will_render_sql
from services.serving_freshness import serving_freshness
from utils.market_code import iso2_market


router = APIRouter(prefix="/api/agent/pdp", tags=["agent-pdp"])
logger = logging.getLogger(__name__)


def _serving_market() -> str:
    """Market this read surface serves buyability against. US-oriented index by
    default; override with AGENT_PDP_SERVING_MARKET for a non-US deployment."""
    return (os.getenv("AGENT_PDP_SERVING_MARKET") or DEFAULT_SERVING_MARKET).strip() or DEFAULT_SERVING_MARKET


def _normalize_buyer_market(raw: Any) -> Optional[str]:
    """The buyer's market this route honours, else None (the deployment's market applies, as
    before). Only behind AGENT_PDP_V1_MARKET_PRICES_READ, and only a SERVED pricing region
    (PIVOTA_SERVING_PRICING_REGIONS): an acquisition market (JP, AU -- stored, never served) or any
    other code must not turn a stored foreign offer into a buy pick. ISO-2 parsing is the search
    routes' (routes/agent_api._normalize_serving_market_param): a locale, a list or an unpriced
    code makes no claim."""
    if not _market_prices_read_enabled():
        return None
    code = iso2_market(raw)
    if not code or not expected_currency_for_market(code):
        return None
    from services.index_pipeline_state_service import serving_pricing_regions

    return code if code in serving_pricing_regions() else None


def _needs_market_view(buyer_market: Optional[str]) -> bool:
    """A buyer priced in another currency than the deployment's: the only case that reads the
    per-currency summary. A US buyer on the US deployment never names the column."""
    if not buyer_market:
        return False
    currency = expected_currency_for_market(buyer_market)
    return bool(currency) and currency != expected_currency_for_market(_serving_market())


def _market_prices_read_enabled() -> bool:
    """Read flag for agent_pdp_view.market_prices (migration 263). Default OFF: the SELECTs do
    not name the column, so the route keeps working on a database without it."""
    return (
        (os.getenv("AGENT_PDP_V1_MARKET_PRICES_READ") or "").strip().lower()
        in {"1", "true", "yes", "on"}
    )


AGENT_PDP_VIEW_COLUMNS: Tuple[str, ...] = (
    "content_key",
    "pivota_signature_id",
    "product_group_id",
    "brand",
    "title",
    "description",
    "bullet_points",
    "usage_scenarios",
    "image_url",
    "image_urls",
    "currency",
    "price_min",
    "price_max",
    "offer_count",
    "offers",
    "variants",
    "variants_count",
    "gtin13",
    # Review ratings (migration 186): captured from schema.org storefronts and
    # mirrored onto agent_pdp_view, but never selected here — so agents could not
    # read social proof this index already held. NULL means "no review data on
    # the source page", never "zero stars"; _row_as_product only emits the
    # normalized aggregate_rating when both value and a positive count exist.
    "rating_value",
    "rating_count",
    "category_path",
    "taxonomy_tags",
    "breadcrumb",
    "pdp_lifecycle_stage",
    "sync_status",
    "primary_merchant_id",
    "refreshed_at",
    "refreshed_by_proposal_id",
    "refresh_source",
    # Evidence layer (migration 152): stored in agent_pdp_view but was dropped
    # here, so agents never saw grounded claims. Emitted (substantiated-only) in
    # _row_as_product.
    "evidence_profile",
    "required_disclaimers",
)

_SELECT_COLUMNS = ",\n      ".join(AGENT_PDP_VIEW_COLUMNS)
_SELECT_APV_COLUMNS = ",\n      ".join(f"apv.{column}" for column in AGENT_PDP_VIEW_COLUMNS)

# Is the citable `url` this route emits actually followable? BOTH of
# get_pdp_v2's gates, asked about apv's own signature — the exact sig the URL is
# built from — via the predicate the sitemap feed and the canonical election
# share (services.pdp_renderability.sig_pdp_will_render).
#
# WHY THIS IS AFFORDABLE HERE, measured rather than assumed. On prod,
# EXPLAIN (ANALYZE, BUFFERS) over several signatures: **Execution Time 0.18-0.57
# ms**, ~10 shared buffers, all index lookups — roughly 2-6% of migration 085's
# <10ms p99 target.
#
# Stated precisely, because the raw EXPLAIN output is easy to misread: those runs
# ALSO report Planning Time ~2.2 ms, and that does NOT recur per request. EXPLAIN
# re-plans by definition; the serving path does not. asyncpg prepares and caches
# statements per connection (statement_cache_size 100) and `databases` hands out
# pooled connections, so planning is paid ONCE per statement per connection and
# amortises to ~0. The honest caveat is the cold edge: the first request on a
# freshly-opened pool connection pays that ~2.2 ms once.
#
# The alternatives are both
# worse: materialising the flag onto agent_pdp_view costs a migration, assembler
# wiring, a backfill AND a staleness failure mode (renderability moves with
# index_pipeline_state and external_product_seeds, neither of which triggers an
# apv refresh) to save 0.18 ms; a second round trip — the pattern the citation
# route uses, where a 300s cache absorbs it — would cost MORE than inlining.
#
# AVAILABILITY COUPLING, stated because the cost analysis alone does not cover it.
# These reads now depend on `catalog_products` and `external_product_seeds` being
# queryable, where migration 085's stated purpose was decoupling this read from
# `catalog_products`. Concretely: an `ALTER TABLE catalog_products` takes ACCESS
# EXCLUSIVE and will now block every gated agent PDP read for its duration, which
# it did not before. Judged acceptable — those migrations are brief and this route
# already hard-depends on `index_pipeline_state` via its INNER JOIN — but it IS a
# new coupling, and the emergency bypass (which touches neither table) remains the
# escape hatch if a long migration ever makes it bite.
#
# ONLY ON THE GATED SELECTS. The emergency-bypass variants below deliberately do
# NOT carry it: they exist to serve when index_pipeline_state is the problem, and
# gate 1 of this predicate reads that very table. A flag derived from the gate the
# operator just overrode is not a weaker answer, it is a meaningless one — so the
# bypass omits the column entirely and `_row_as_product` emits null ("unknown"),
# never False.
_RENDERABLE_APV_SQL = (
    f"({sig_pdp_will_render_sql('apv.pivota_signature_id')}) AS pdp_renderable"
)
_SELECT_APV_COLUMNS_WITH_RENDERABLE = (
    f"{_SELECT_APV_COLUMNS},\n      {_RENDERABLE_APV_SQL}"
)

# ADR-007 SLICE 1: the citation read surface gates on serving_eligible today.
# When INDEX_ELIGIBLE_READ is ON the gate widens to the OFFER-FREE
# `index_eligible` floor as well (serving_eligible OR index_eligible). The two
# clause variants are substituted into the SELECTs below so that with the flag
# OFF the emitted SQL is byte-identical to the pre-ADR-007 query.
_SERVING_ELIGIBLE_CLAUSE = "ips.serving_eligible = TRUE"
_INDEX_ELIGIBLE_CLAUSE = "(ips.serving_eligible = TRUE OR ips.index_eligible = TRUE)"

SELECT_BY_CONTENT_KEY_SQL = f"""
    SELECT
      {_SELECT_APV_COLUMNS_WITH_RENDERABLE}
    FROM agent_pdp_view apv
    INNER JOIN index_pipeline_state ips ON ips.content_key = apv.content_key
    WHERE apv.content_key = :id
      AND {_SERVING_ELIGIBLE_CLAUSE}
    LIMIT 1
"""

SELECT_BY_SIGNATURE_SQL = f"""
    SELECT
      {_SELECT_APV_COLUMNS_WITH_RENDERABLE}
    FROM agent_pdp_view apv
    INNER JOIN index_pipeline_state ips ON ips.content_key = apv.content_key
    WHERE apv.pivota_signature_id = :id
      AND {_SERVING_ELIGIBLE_CLAUSE}
    LIMIT 1
"""

SELECT_BY_PRODUCT_GROUP_SQL = f"""
    SELECT
      {_SELECT_APV_COLUMNS_WITH_RENDERABLE}
    FROM agent_pdp_view apv
    INNER JOIN index_pipeline_state ips ON ips.content_key = apv.content_key
    WHERE apv.product_group_id = :id
      AND {_SERVING_ELIGIBLE_CLAUSE}
    ORDER BY
      CASE WHEN apv.pivota_signature_id IS NOT NULL THEN 0 ELSE 1 END,
      apv.content_key ASC
    LIMIT 1
"""

# index_eligible-widened variants (used only when INDEX_ELIGIBLE_READ is ON).
INDEX_SELECT_BY_CONTENT_KEY_SQL = SELECT_BY_CONTENT_KEY_SQL.replace(
    _SERVING_ELIGIBLE_CLAUSE, _INDEX_ELIGIBLE_CLAUSE
)
INDEX_SELECT_BY_SIGNATURE_SQL = SELECT_BY_SIGNATURE_SQL.replace(
    _SERVING_ELIGIBLE_CLAUSE, _INDEX_ELIGIBLE_CLAUSE
)
INDEX_SELECT_BY_PRODUCT_GROUP_SQL = SELECT_BY_PRODUCT_GROUP_SQL.replace(
    _SERVING_ELIGIBLE_CLAUSE, _INDEX_ELIGIBLE_CLAUSE
)

BYPASS_SELECT_BY_CONTENT_KEY_SQL = f"""
    SELECT
      {_SELECT_COLUMNS}
    FROM agent_pdp_view
    WHERE content_key = :id
    LIMIT 1
"""

BYPASS_SELECT_BY_SIGNATURE_SQL = f"""
    SELECT
      {_SELECT_COLUMNS}
    FROM agent_pdp_view
    WHERE pivota_signature_id = :id
    LIMIT 1
"""

BYPASS_SELECT_BY_PRODUCT_GROUP_SQL = f"""
    SELECT
      {_SELECT_COLUMNS}
    FROM agent_pdp_view
    WHERE product_group_id = :id
    ORDER BY
      CASE WHEN pivota_signature_id IS NOT NULL THEN 0 ELSE 1 END,
      content_key ASC
    LIMIT 1
"""


def _with_market_prices(sql: str) -> str:
    """The same SELECT plus the summary, appended after the last agent_pdp_view column
    (required_disclaimers) of either SELECT shape -- through market_prices_fresh_sql, so a
    summary a later flag-off write left behind reads as NULL."""
    gated = "apv.required_disclaimers,\n"
    bypass = "      required_disclaimers\n    FROM"
    if sql.count(gated) == 1:
        return sql.replace(
            gated, f"apv.required_disclaimers,\n      {market_prices_fresh_sql('apv')} AS market_prices,\n"
        )
    if sql.count(bypass) == 1:
        return sql.replace(
            bypass, f"      required_disclaimers,\n      {market_prices_fresh_sql()} AS market_prices\n    FROM"
        )
    raise RuntimeError("agent_pdp_v1 SELECT changed shape; update _with_market_prices")


# Built once at import, so a shape change fails at boot rather than per request.
_MARKET_PRICES_SQL = {
    sql: _with_market_prices(sql)
    for sql in (
        SELECT_BY_CONTENT_KEY_SQL, SELECT_BY_SIGNATURE_SQL, SELECT_BY_PRODUCT_GROUP_SQL,
        INDEX_SELECT_BY_CONTENT_KEY_SQL, INDEX_SELECT_BY_SIGNATURE_SQL,
        INDEX_SELECT_BY_PRODUCT_GROUP_SQL,
        BYPASS_SELECT_BY_CONTENT_KEY_SQL, BYPASS_SELECT_BY_SIGNATURE_SQL,
        BYPASS_SELECT_BY_PRODUCT_GROUP_SQL,
    )
}
_LEGACY_SQL = {variant: legacy for legacy, variant in _MARKET_PRICES_SQL.items()}


async def _fetch_view_row(query: str, values: Dict[str, Any]) -> Any:
    """fetch_one, and if a market_prices SELECT finds the columns missing (dropped, or the probe
    raced the heal), the legacy SELECT for this request and the legacy path until re-probed."""
    try:
        return await database.fetch_one(query, values)
    except Exception as exc:
        legacy = _LEGACY_SQL.get(query)
        if legacy is None or not is_missing_market_prices_column_error(exc):
            raise
        mark_market_prices_columns_missing()
        return await database.fetch_one(legacy, values)


def _is_pivota_signature_id(value: str) -> bool:
    return value.startswith("sig_")


def _is_product_group_id(value: str) -> bool:
    return value.startswith(("pg_", "pg:", "grp_"))


def _is_external_product_id(value: str) -> bool:
    return value.startswith("ext_")


def _strip_group_wrapper(value: str) -> str:
    """The legacy gateway emits canonical refs as ``pg:<product_group_id>``
    where the inner id is itself a ``pg_*`` / ``grp_*`` string. Strip the
    leading ``pg:`` so the SQL matches the stored product_group_id."""
    if value.startswith("pg:"):
        return value[len("pg:"):]
    return value


def _bypass_serving_eligibility() -> bool:
    return (
        (os.getenv("AGENT_PDP_V1_BYPASS_SERVING_ELIGIBILITY") or "")
        .strip()
        .lower()
        == "true"
    )


def _index_eligible_read_enabled() -> bool:
    """ADR-007 SLICE 1 read flag. When ON, the citation read gate widens to
    serving_eligible OR index_eligible. Default OFF ⇒ byte-identical to today.

    This is a real quality floor, distinct from the emergency
    AGENT_PDP_V1_BYPASS_SERVING_ELIGIBILITY full bypass."""
    return (
        (os.getenv("INDEX_ELIGIBLE_READ") or "").strip().lower()
        in {"1", "true", "yes", "on"}
    )


def _request_context(request: Request) -> Dict[str, Optional[str]]:
    request_id = (
        getattr(request.state, "request_id", None)
        or request.headers.get("x-request-id")
        or request.headers.get("x-correlation-id")
    )
    merchant_id = (
        getattr(request.state, "merchant_id", None)
        or request.headers.get("x-merchant-id")
        or request.query_params.get("merchant_id")
    )
    return {
        "request_id": str(request_id) if request_id else None,
        "merchant_id": str(merchant_id) if merchant_id else None,
    }


def _warn_serving_eligibility_bypass(request: Request, lookup_id: str) -> None:
    context = _request_context(request)
    logger.warning(
        json.dumps(
            {
                "event": "agent_pdp_v1_serving_eligibility_bypass_enabled",
                "request_id": context["request_id"],
                "merchant_id": context["merchant_id"],
                "lookup_id": lookup_id,
                "bypass_env": "AGENT_PDP_V1_BYPASS_SERVING_ELIGIBILITY",
            },
            sort_keys=True,
        ),
        extra={
            "request_id": context["request_id"],
            "merchant_id": context["merchant_id"],
            "lookup_id": lookup_id,
            "bypass_env": "AGENT_PDP_V1_BYPASS_SERVING_ELIGIBILITY",
        },
    )


def _query_for_id(
    value: str,
    *,
    bypass_serving_eligibility: bool = False,
    index_eligible_read: bool = False,
    market_prices_read: bool = False,
) -> Optional[str]:
    sql = _base_query_for_id(
        value,
        bypass_serving_eligibility=bypass_serving_eligibility,
        index_eligible_read=index_eligible_read,
    )
    if sql is not None and market_prices_read:
        return _MARKET_PRICES_SQL[sql]
    return sql


def _base_query_for_id(
    value: str,
    *,
    bypass_serving_eligibility: bool = False,
    index_eligible_read: bool = False,
) -> Optional[str]:
    # bypass (emergency, no IPS join) takes precedence over the index_eligible
    # read widening. index_eligible_read only swaps the eligibility clause on
    # the serving-gated SELECTs; flag OFF ⇒ the original serving-only SQL.
    if is_content_key(value):
        if bypass_serving_eligibility:
            return BYPASS_SELECT_BY_CONTENT_KEY_SQL
        return (
            INDEX_SELECT_BY_CONTENT_KEY_SQL
            if index_eligible_read
            else SELECT_BY_CONTENT_KEY_SQL
        )
    if _is_pivota_signature_id(value):
        if bypass_serving_eligibility:
            return BYPASS_SELECT_BY_SIGNATURE_SQL
        return (
            INDEX_SELECT_BY_SIGNATURE_SQL
            if index_eligible_read
            else SELECT_BY_SIGNATURE_SQL
        )
    if _is_product_group_id(value):
        if bypass_serving_eligibility:
            return BYPASS_SELECT_BY_PRODUCT_GROUP_SQL
        return (
            INDEX_SELECT_BY_PRODUCT_GROUP_SQL
            if index_eligible_read
            else SELECT_BY_PRODUCT_GROUP_SQL
        )
    return None


# External-product (ext_*) IDs are not stored on agent_pdp_view directly.
# They live on external_product_seeds.external_product_id; resolution
# chases attached_product_key → catalog_products.content_key → the
# agent_pdp_view PK. One extra indexed SELECT, still well inside the
# <10ms p99 budget.
EXT_RESOLVE_SQL = """
    SELECT cp.content_key
    FROM external_product_seeds eps
    JOIN catalog_products cp ON cp.product_key = eps.attached_product_key
    WHERE eps.external_product_id = :ext_id
      AND eps.status = 'active'
      AND cp.content_key IS NOT NULL
    LIMIT 1
"""


# A signature that is no longer the served row's (its content_key's canonical pick moved to another
# member -- e.g. the brand store's row now outranks a retailer listing, #2384 -- or the listing was
# re-keyed onto the brand's product) still names a live catalog row: resolve it to that row's
# content_key and serve the product, as ext_* ids already are. Run ONLY after the direct lookup
# misses, so the served path stays one indexed SELECT; idx_catalog_products_pivota_signature (071).
SIG_RESOLVE_SQL = """
    SELECT cp.content_key
    FROM catalog_products cp
    WHERE cp.pivota_signature_id = :id
      AND cp.suppressed_at IS NULL
      AND cp.content_key IS NOT NULL
    LIMIT 1
"""


def _coerce_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _coerce_list(value: Any) -> list:
    parsed = _coerce_json(value)
    if isinstance(parsed, list):
        return parsed
    return []


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        data = dict(row)
    else:
        data = dict(row)

    for key in (
        "image_urls", "offers", "variants", "taxonomy_tags", "breadcrumb",
        "bullet_points", "usage_scenarios",
    ):
        data[key] = _coerce_json(data.get(key))
    if "market_prices" in data:
        data["market_prices"] = _coerce_json(data.get("market_prices"))

    data["image_urls"] = _coerce_list(data.get("image_urls"))
    data["offers"] = _coerce_list(data.get("offers"))
    data["variants"] = _coerce_list(data.get("variants"))
    data["breadcrumb"] = _coerce_list(data.get("breadcrumb"))
    data["bullet_points"] = _coerce_list(data.get("bullet_points"))
    data["usage_scenarios"] = _coerce_list(data.get("usage_scenarios"))
    return data


def _row_as_product(row: Dict[str, Any]) -> Dict[str, Any]:
    title = str(row.get("title") or "").strip()
    image_url = row.get("image_url")
    product_id = row.get("pivota_signature_id") or row.get("content_key")

    product = dict(row)
    # Internal read model, never served as-is: _market_view applies it.
    product.pop("market_prices", None)
    product.update(
        {
            "id": product_id,
            "product_id": product_id,
            "name": title,
            "brand_name": row.get("brand"),
            "vendor": row.get("brand"),
            "main_image_url": image_url,
            "images": row.get("image_urls") or ([] if not image_url else [image_url]),
            "variants": row.get("variants") or [],
        }
    )

    # Citable canonical URL: the Pivota agent-facing PDP an agent should attribute
    # to when it grounds a recommendation on this product. The crawlable page
    # (sitemap + schema.org JSON-LD) already carries it; without it here, an agent
    # calling the API/MCP has grounded claims but no source to cite. Built from the
    # signature via the single source of truth (mirrors sitemap + JSON-LD exactly);
    # `url` follows the schema.org convention the public page uses.
    signature_id = row.get("pivota_signature_id")
    if signature_id and str(signature_id).startswith("sig_"):
        canonical_url = pivota_canonical_pdp_url(str(signature_id))
        product["pivota_canonical_url"] = canonical_url
        product["url"] = canonical_url

    # …AND WHETHER THAT URL ANSWERS 200. Emitting a citable URL with no way to
    # tell whether it renders was the honesty gap #1592/#1593 closed on the
    # canonical and citation reads; this is the same fix on the last surface that
    # still had it.
    #
    # Sized against THIS ROUTE's population, not the feed's. The feed figure —
    # 879 non-renderable of 5,887 — is the wrong denominator here, because this
    # route 404s anything that fails its eligibility gate, so most of those 879
    # were never served. Measured on prod for the rows this route actually
    # returns: **390 of 4,782 (8.2%)** on the default serving-only lane, and 489
    # of 4,881 with INDEX_ELIGIBLE_READ on. Still ~1 in 12 reads handing out a
    # dead link with no warning.
    #
    # THREE-STATE, and the third state is the point:
    #   True  — both of get_pdp_v2's gates pass; follow the link.
    #   False — it will not render; cite the CONTENT, do not emit the link.
    #   None  — UNKNOWN. Either no sig was minted (so there is no URL to
    #           characterise), or the emergency
    #           AGENT_PDP_V1_BYPASS_SERVING_ELIGIBILITY path served this row, and
    #           that path deliberately does not join index_pipeline_state — which
    #           gate 1 of the predicate reads. Reporting False there would be
    #           asserting a check we did not run against a gate the operator
    #           explicitly overrode.
    #
    # `row` genuinely lacks the key on the bypass path (the column is absent from
    # those SELECTs), so `.get` returning None IS the signal, not a default.
    raw_renderable = row.get("pdp_renderable")
    product["pdp_renderable"] = (
        None
        if raw_renderable is None or not product.get("url")
        else bool(raw_renderable)
    )

    # Serve gate: never leak the raw evidence_profile (it can carry unverified
    # claims); emit only substantiated claims + the required disclaimers.
    product.pop("evidence_profile", None)
    product.pop("required_disclaimers", None)
    product["evidence_claims"] = substantiated_claims(row.get("evidence_profile"))
    product["disclaimers"] = row.get("required_disclaimers") or []

    # Honest freshness: offers/price are baked at row assembly, so refreshed_at
    # bounds their age. Surface a staleness signal (never withhold) so agents
    # can trust the baked price or choose to re-fetch. TTL mirrors the catalog
    # sync price-fact window. See services.serving_freshness.
    freshness = serving_freshness(row.get("refreshed_at"))
    product["freshness"] = freshness
    product["is_stale"] = freshness["is_stale"]

    price_min = row.get("price_min")
    currency = row.get("currency")
    if price_min is not None:
        product["price"] = {
            "current": {
                "amount": price_min,
                **({"currency": currency} if currency else {}),
            }
        }

    # Social proof, never fabricated: the raw rating_value/rating_count columns
    # still pass through dict(row) for consumers that want them unnormalized.
    product["aggregate_rating"] = aggregate_rating_from_row(row)

    # Repair double-encoded taxonomy_tags (JSON strings inside the JSONB dict —
    # see services.agent_pdp_view_assembler._TAXONOMY_LIST_KEYS). Historical
    # rows keep the broken shape INDEFINITELY — the reconciler only re-assembles
    # on a truth-timestamp change — so this normalization is permanent, not a
    # transition shim (or run scripts/backfill_agent_pdp_view.py once).
    product["taxonomy_tags"] = normalize_taxonomy_tags(row.get("taxonomy_tags"))

    return product


def aggregate_rating_from_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize migration 186's rating columns into {value, count}, or None.

    Emitted ONLY when the source page carried both a rating value and a positive
    review count — NULL means "no review data on the source page", never "zero
    stars", and a rating with no reviews behind it is withheld rather than
    invented. The {value, count} shape is what the PDP JSON-LD builder's
    aggregateRating resolver reads first, so one normalization serves API
    agents, the citation surface, and the crawlable page alike.
    """
    rating_value = row.get("rating_value")
    rating_count = row.get("rating_count")
    try:
        if rating_value is None or rating_count is None or int(rating_count) <= 0:
            return None
        # round(2): prod rows carry float dirt (4.5999999999999996 for a source
        # page's 4.6) — emit the value the source page actually displayed.
        return {"value": round(float(rating_value), 2), "count": int(rating_count)}
    except (TypeError, ValueError):
        return None


def _market_view(row: Dict[str, Any], buyer_market: Optional[str]) -> Dict[str, Any]:
    """The row as the BUYER's market sees it.

    Only for a buyer priced in another currency than the deployment's (_needs_market_view), and
    only when the row carries a FRESH version-1 summary with an entry in that currency
    (agent_pdp_view.market_prices, migration 263; the SELECT nulls a stale one): currency,
    price_min, price_max, offer_count and offers are that currency's -- the same values the
    gateway serves from the same summary. Never converted. Any other case returns the row as is,
    so those responses are byte-identical to the route without this function.
    """
    if not _needs_market_view(buyer_market):
        return row
    currency = expected_currency_for_market(buyer_market)
    entry = market_prices_entry(row.get("market_prices"), currency)
    if entry is None:
        return row
    view = dict(row)
    view.update(
        {
            "currency": currency,
            "price_min": entry.get("price_min"),
            "price_max": entry.get("price_max"),
            "offer_count": entry.get("offer_count"),
            "offers": [o for o in (entry.get("offers") or []) if isinstance(o, dict)],
        }
    )
    return view


def _build_response(
    row: Dict[str, Any],
    independent_signals: Optional[list] = None,
    buyer_market: Optional[str] = None,
) -> Dict[str, Any]:
    row = _market_view(row, buyer_market)
    # Market-aware buyability: tag each offer domestic/cross_border + is_buy_pick
    # against the BUYER's market when the request names one it can price
    # (`serving_market`), else the deployment's, so a cross-border brand-direct offer
    # (e.g. a KRW listing) isn't presented to a US agent as a domestic same-market
    # purchase, and an SG buyer's buy pick is an SGD offer when the row has one.
    offers = annotate_offer_buyability(row.get("offers") or [], buyer_market or _serving_market())
    offer_count = row.get("offer_count")
    if offer_count is None:
        offer_count = len(offers)

    product = _row_as_product(row)
    # SEPARATION invariant: independent (non-merchant) endorsements are a DISTINCT
    # block, never merged into merchant-asserted evidence_claims.
    product["independent_signals"] = independent_signals or []

    product_group_id = row.get("product_group_id")
    canonical_data: Dict[str, Any] = {
        "product_group_id": product_group_id,
        "pdp_payload": {
            "product": product,
            "offers": offers,
            "offers_count": offer_count,
            "product_group_id": product_group_id,
            "modules": [],
            "actions": [],
        },
    }
    if product_group_id and int(offer_count or 0) > 1:
        canonical_data["canonical_scope"] = "multi_merchant_canonical"

    payload = {
        "modules": [
            {"type": "canonical", "data": canonical_data},
            {
                "type": "offers",
                "data": {
                    "offers_count": offer_count,
                    "offers": offers,
                    "product_group_id": product_group_id,
                },
            },
        ],
        "subject": {"type": "product_group", "id": product_group_id},
        "product_group_id": product_group_id,
        "offers_count": offer_count,
    }
    return jsonable_encoder(payload)


async def _respond(row: Dict[str, Any], buyer_market: Optional[str] = None) -> Dict[str, Any]:
    """Build the PDP response, enriched with credible independent signals (one
    indexed lookup on content_key; [] for the common no-citation case)."""
    signals = await independent_signals_for(str(row.get("content_key") or ""), db=database)
    return _build_response(row, independent_signals=signals, buyer_market=buyer_market)


@router.get("/{id}")
async def get_agent_pdp(
    id: str,
    request: Request,
    serving_market: Optional[str] = Query(
        default=None,
        description=(
            "The BUYER's market (ISO-2), as on the search routes. Honoured only for a served "
            "pricing region while AGENT_PDP_V1_MARKET_PRICES_READ is on: it decides the buy pick "
            "and, when the row carries a fresh per-currency summary, the price block, offers and "
            "offers_count. Anything else is ignored (the deployment's market applies)."
        ),
    ),
) -> Dict[str, Any]:
    raw_id = str(id or "")
    buyer_market = _normalize_buyer_market(serving_market)
    bypass_serving_eligibility = _bypass_serving_eligibility()
    index_eligible_read = _index_eligible_read_enabled()
    market_prices_read = _needs_market_view(buyer_market) and await agent_pdp_view_has_market_prices(database)
    if bypass_serving_eligibility:
        _warn_serving_eligibility_bypass(request, raw_id)

    # ext_*: resolve to a content_key via external_product_seeds + catalog_products,
    # then fall through to the standard content_key SELECT.
    if _is_external_product_id(raw_id):
        resolved = await database.fetch_one(EXT_RESOLVE_SQL, {"ext_id": raw_id})
        if not resolved:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="PDP not found",
            )
        resolved_content_key = str(dict(resolved).get("content_key") or "")
        query = _query_for_id(
            resolved_content_key,
            bypass_serving_eligibility=bypass_serving_eligibility,
            index_eligible_read=index_eligible_read,
            market_prices_read=market_prices_read,
        )
        if query is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="PDP not found",
            )
        row = await _fetch_view_row(query, {"id": resolved_content_key})
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="PDP not found",
            )
        return await _respond(_row_to_dict(row), buyer_market)

    lookup_id = _strip_group_wrapper(raw_id)
    query = _query_for_id(
        lookup_id,
        bypass_serving_eligibility=bypass_serving_eligibility,
        index_eligible_read=index_eligible_read,
        market_prices_read=market_prices_read,
    )
    if query is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="PDP not found",
        )

    row = await _fetch_view_row(query, {"id": lookup_id})
    if not row and _is_pivota_signature_id(lookup_id):
        resolved = await database.fetch_one(SIG_RESOLVE_SQL, {"id": lookup_id})
        resolved_content_key = str(dict(resolved).get("content_key") or "") if resolved else ""
        ck_query = _query_for_id(
            resolved_content_key,
            bypass_serving_eligibility=bypass_serving_eligibility,
            index_eligible_read=index_eligible_read,
            market_prices_read=market_prices_read,
        ) if resolved_content_key else None
        if ck_query is not None:
            row = await _fetch_view_row(ck_query, {"id": resolved_content_key})
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="PDP not found",
        )

    return await _respond(_row_to_dict(row), buyer_market)
