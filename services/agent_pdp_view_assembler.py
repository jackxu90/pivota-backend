"""Shared assembler and source-row readers for agent_pdp_view rows (Stage 3a).

The agent_pdp_view denormalized table (mig 085, Stage 3a-i) is fed from
two surfaces:

  * The one-shot backfill (Stage 3a-ii — scripts/backfill_agent_pdp_view.py)
  * The writer hook (Stage 3a-iii — services/seed_data_writer.py), which
    refreshes affected rows on every seed_data commit.

Both call the same pure-Python assembler defined here. Keeping assembly
side-effect-free (no DB writes inside assemble_row) means the writer
hook can dry-run / preview the next row state without touching prod,
and the backfill stays trivially testable. The read helpers here share
the per-content_key SELECT shape across the backfill and writer hook;
callers still own batching and persistence.
"""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from db.database import database
from services.catalog_identity import validated_source_gtin
from services.claim_safety import ensure_category_disclaimers
from services.offer_buyability import availability_is_known_unavailable
from services.source_quarantine import (
    CATALOG_PRODUCT_DOMAIN_SQL,
    build_quarantine_anti_join_sql,
)
from services.title_normalization import normalize_display_title

logger = logging.getLogger(__name__)

# Top-N offers stored per row. Schema docstring (mig 085) says <=5;
# matches the AggregateOffer behavior on the frontend.
OFFER_TOP_N = 5

# Cap variants at 50 — matches the schema comment. Tom Ford foundation
# has 40 shades, our prod p99.
VARIANT_CAP = 50

PDP_URL_PREFIX = "https://agent.pivota.cc/products/"

# Refresh-source tag the backfill writes when seeding rows from
# scratch. The Stage 3a-iii writer hook sets its own source ("writer_commit"
# or similar) so the audit trail distinguishes one-shot fills from
# incremental refreshes.
BACKFILL_REFRESH_SOURCE = "backfill_3a_ii"

# Enforce the source-quarantine overlay (mig 134) at the canonical-pick / serve
# layer. catalog_source_quarantine is OPT-IN — readers must anti-join it, and
# this serving assembler historically did NOT, so quarantines were silently
# shadow here. fetch_products_for_key is the single loader feeding pick_canonical
# (both assemble_row and the enrichment overlay fetch, via
# refresh_agent_pdp_view_for_content_key — the only APV rebuild path, shared by
# on-demand refresh AND catalog_sync). Anti-joining here keeps a quarantined
# source (e.g. a duplicate App Store review account sharing a real merchant's
# store) from winning the canonical pick and shadowing the real merchant's
# enriched PDP. Pure SQL with hardcoded column refs — no user input.
_SOURCE_QUARANTINE_ANTI_JOIN = build_quarantine_anti_join_sql(
    # The FULL domain chain, not cp.source_domain alone — see #1643 and the
    # comment on CATALOG_PRODUCT_DOMAIN_SQL. 4,007 of 14,124 prod rows have a
    # NULL source_domain; on those this predicate was blind to the quarantine
    # while the trust layer was not.
    row_domain_expr=CATALOG_PRODUCT_DOMAIN_SQL,
    row_merchant_expr="cp.merchant_id",
    row_platform_expr="cp.platform",
    row_source_system_expr="cp.source_system",
    row_source_ref_expr="cp.source_ref",
)


# ---------------------------------------------------------------------
# DB fetch helpers
# ---------------------------------------------------------------------

async def fetch_products_for_key(content_key: str, *, db: Any = None) -> List[Dict[str, Any]]:
    read_db = db or database
    rows = await read_db.fetch_all(
        f"""
        SELECT
          cp.product_key,
          cp.merchant_id,
          cp.platform,
          cp.source_product_id,
          cp.title,
          cp.description,
          cp.brand,
          cp.product_type,
          cp.category,
          cp.category_kind,
          cp.image_url,
          cp.product_payload,
          cp.tags,
          cp.price_tier,
          cp.use_case_tags,
          cp.lifestyle_tags,
          cp.demographic,
          cp.pdp_lifecycle_stage,
          cp.pivota_signature_id,
          cp.canonical_url,
          cp.sync_status,
          cp.suppressed_at,
          cp.created_at,
          cp.material,
          cp.material_source,
          cp.material_confidence,
          cp.care,
          cp.care_source,
          cp.care_confidence,
          cp.size_guide,
          cp.size_guide_source,
          cp.size_guide_confidence,
          cp.rating_value,
          cp.rating_count,
          cp.source_domain,
          -- pick_canonical's brand-store rule: the ingest pipeline's own seller verdict for this row
          EXISTS (
            SELECT 1 FROM catalog_offers o
            WHERE o.product_key = cp.product_key AND o.suppressed_at IS NULL
              AND o.offer_type = 'brand_direct'
          ) AS has_brand_direct_offer,
          pgm.product_group_id,
          pgm.is_primary AS group_is_primary
        FROM catalog_products cp
        LEFT JOIN product_group_members pgm
          ON pgm.merchant_id = cp.merchant_id
         AND pgm.platform = cp.platform
         AND pgm.platform_product_id = cp.source_product_id
        WHERE cp.content_key = :ck
        {_SOURCE_QUARANTINE_ANTI_JOIN}
        """,
        {"ck": content_key},
    )
    return [dict(r) for r in rows or []]


async def fetch_skus_for_keys(product_keys: List[str], *, db: Any = None) -> List[Dict[str, Any]]:
    if not product_keys:
        return []
    read_db = db or database
    rows = await read_db.fetch_all(
        """
        SELECT
          sku_key, product_key, merchant_id, source_variant_id, source_product_id,
          sku, barcode, title, currency, image_url, visible_attributes,
          visible_option_labels
        FROM catalog_skus
        WHERE product_key = ANY(:keys)
          AND suppressed_at IS NULL
        """,
        {"keys": product_keys},
    )
    return [dict(r) for r in rows or []]


async def fetch_offers_for_keys(product_keys: List[str], *, db: Any = None) -> List[Dict[str, Any]]:
    if not product_keys:
        return []
    read_db = db or database
    rows = await read_db.fetch_all(
        """
        SELECT
          o.offer_id, o.sku_key, o.product_key, o.merchant_id,
          o.availability, o.currency, o.list_price,
          o.merchant_effective_price, o.estimated_best_price,
          o.market, o.offer_type, o.is_first_party,
          m.merchant_name
        FROM catalog_offers o
        LEFT JOIN catalog_merchants m ON m.merchant_id = o.merchant_id
        WHERE o.product_key = ANY(:keys)
          AND o.suppressed_at IS NULL
        """,
        {"keys": product_keys},
    )
    return [dict(r) for r in rows or []]


async def fetch_external_seed_for_keys(
    product_keys: List[str],
    *,
    db: Any = None,
) -> Optional[Dict[str, Any]]:
    """First active external_product_seed attached to any product in a
    content_key cluster. The backfill uses this — it has no specific
    seed in hand, just a group of product_keys, and any one row works
    as a content fallback.

    The writer hook should NOT use this; it has a specific seed in
    hand (the one whose seed_data just changed). See
    fetch_external_seed_by_id.
    """
    if not product_keys:
        return None
    read_db = db or database
    row = await read_db.fetch_one(
        """
        SELECT id, attached_product_key, title, image_url, seed_data,
               canonical_url, destination_url
        FROM external_product_seeds
        WHERE attached_product_key = ANY(:keys)
          AND status = 'active'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        {"keys": product_keys},
    )
    return dict(row) if row else None


async def fetch_external_seed_by_id(
    seed_id: str,
    *,
    db: Any = None,
) -> Optional[Dict[str, Any]]:
    """Fetch the external seed by its specific id. Used by the writer
    hook — the triggering seed_id is in scope, and we want the
    description fallback to come from the seed whose seed_data just
    changed, not "newest active in the group" (which can be a
    different, stale row).
    """
    read_db = db or database
    row = await read_db.fetch_one(
        """
        SELECT id, attached_product_key, title, image_url, seed_data,
               canonical_url, destination_url
        FROM external_product_seeds
        WHERE id = :seed_id
        """,
        {"seed_id": seed_id},
    )
    return dict(row) if row else None


async def fetch_own_seed_copy_for_keys(
    product_keys: List[str],
    *,
    db: Any = None,
) -> Dict[str, Dict[str, Any]]:
    """Each product's OWN newest active seed: its storefront, how its description was chosen,
    whether a person reviewed it, and the brand-page sections the crawl captured
    -> {product_key: {...}}.

    Keyed per row, unlike fetch_external_seed_for_keys (one seed for the whole cluster, chosen by
    the caller), so what compose_brand_section_description serves depends only on the winner. The
    id breaks an updated_at tie, so two seeds written in one transaction never alternate.
    """
    if not product_keys:
        return {}
    read_db = db or database
    rows = await read_db.fetch_all(
        """
        SELECT DISTINCT ON (attached_product_key)
               attached_product_key,
               domain,
               canonical_url,
               coalesce(seed_data->>'seed_description_origin',
                        seed_data->'snapshot'->>'seed_description_origin', '') AS description_origin,
               (seed_data->'snapshot_quarantine'->'pivota_description_rollback_v1') IS NOT NULL
                 AS description_reviewed,
               seed_data->'pdp_details_sections' AS details_sections
        FROM external_product_seeds
        WHERE attached_product_key = ANY(:keys)
          AND status = 'active'
        ORDER BY attached_product_key, updated_at DESC, id DESC
        """,
        {"keys": product_keys},
    )
    return {str(r["attached_product_key"]): dict(r) for r in rows or []}


async def fetch_shared_section_lines(
    host: str,
    lines: List[str],
    *,
    exclude_product_keys: List[str],
    own_base_title: str,
    db: Any = None,
) -> set:
    """Of `lines`, those the page sections of at least _SHARED_LINE_MIN_OTHER_PRODUCTS other
    products on the same storefront also carry: the store's copy, not this product's -- plus every
    line past _SHARED_LINE_CHECK_MAX, which is not checked and so is treated as shared.

    The storefront is the normalised host, matched with and without "www." (seed domains are
    stored both ways). Other editions of the product itself -- its shades, "Pro Filt'r … — #265"
    beside "— #210" -- are one product, not boilerplate: they share its base title
    (_product_base_title, computed the same way in SQL) and are not counted.

    BOUNDED, because it runs inside a rebuild on the 2-vCPU primary (review #2443): at most
    _SHARED_LINE_SEED_SCAN_MAX seeds, both CTEs MATERIALIZED so each body is normalised once rather
    than once per line, and a _SHARED_LINE_TIMEOUT_MS statement_timeout on Postgres. A timeout
    raises; build_agent_pdp_view_row then appends nothing.
    """
    if not lines:
        return set()
    if not host:
        raise ValueError("fetch_shared_section_lines needs the storefront's host")
    checked, unchecked = list(lines[:_SHARED_LINE_CHECK_MAX]), set(lines[_SHARED_LINE_CHECK_MAX:])
    read_db = db or database
    query = """
        WITH seeds AS MATERIALIZED (
          SELECT s.title, s.seed_data->'pdp_details_sections' AS sections
          FROM external_product_seeds s
          WHERE s.domain = ANY(CAST(:domains AS text[]))
            AND s.status = 'active'
            AND (s.attached_product_key IS NULL OR s.attached_product_key <> ALL(:keys))
          ORDER BY s.id
          LIMIT :seed_limit
        ),
        bodies AS MATERIALIZED (
          SELECT lower(btrim(regexp_replace(coalesce(seeds.title, ''), '\\s+[—–-]\\s+.*$', ''))) AS base,
                 regexp_replace(e->>'body', '\\s+', ' ', 'g') AS body
          FROM seeds
          CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(seeds.sections) = 'array'
                 THEN seeds.sections ELSE CAST('[]' AS jsonb) END) e
        )
        SELECT l.line, count(DISTINCT b.base) AS products
        FROM unnest(CAST(:lines AS text[])) AS l(line)
        JOIN bodies b ON strpos(b.body, l.line) > 0 AND b.base <> :own_base
        GROUP BY l.line
        """
    params = {
        "domains": [host, f"www.{host}"],
        "lines": checked,
        "keys": list(exclude_product_keys),
        "own_base": own_base_title,
        "seed_limit": _SHARED_LINE_SEED_SCAN_MAX,
    }
    if hasattr(read_db, "transaction") and str(getattr(read_db, "url", "")).lower().startswith("postgres"):
        async with read_db.transaction():
            await read_db.execute(f"SET LOCAL statement_timeout = {int(_SHARED_LINE_TIMEOUT_MS)}")
            rows = await read_db.fetch_all(query, params)
    else:
        rows = await read_db.fetch_all(query, params)
    shared = {str(r["line"]) for r in rows or [] if int(r["products"] or 0) >= _SHARED_LINE_MIN_OTHER_PRODUCTS}
    return shared | unchecked


async def fetch_evidence_for_keys(
    product_keys: List[str],
    *,
    db: Any = None,
) -> Dict[str, Any]:
    """Highest-precedence agent-decision-grade evidence for a content_key cluster.

    evidence_profile lives per product_key (beauty_product_profiles), but the
    canonical record is shared across the merchant rows that resolve to one
    content_key. Per ADR-001 source precedence (brand-official > supplier >
    reseller) the canonical evidence is the brand-official one — which for the
    crawled wedge catalog is the external-seed row: the legacy 'external_seed'
    bucket OR (ADR-009) a per-brand observed seller (merch_obs_…). So prefer
    external-seed-origin evidence, then the most-recently-authored, among the
    rows that actually carry evidence.

    Returns {} when no row in the cluster has authored evidence yet (the agent
    view column stays NULL — the merchant simply isn't decision-grade yet).
    """
    if not product_keys:
        return {}
    read_db = db or database
    row = await read_db.fetch_one(
        """
        SELECT evidence_profile, required_disclaimers
        FROM beauty_product_profiles
        WHERE product_key = ANY(:keys)
          AND evidence_profile IS NOT NULL
        ORDER BY (merchant_id = 'external_seed' OR merchant_id LIKE 'merch_obs_%') DESC, updated_at DESC
        LIMIT 1
        """,
        {"keys": product_keys},
    )

    # Phase 2a: UNION the cross-vertical merchant evidence store with the beauty
    # (ingredient-mechanism) evidence so non-beauty merchant claims land on
    # agent_pdp_view.evidence_profile too. Beauty passed first → it wins dedupe +
    # review_state precedence. Best-effort; a missing store yields {}.
    from db.product_evidence import (
        fetch_product_evidence_for_keys,
        merge_disclaimer_lists,
        merge_evidence_profiles,
    )

    beauty_profile = row["evidence_profile"] if row else None
    beauty_disclaimers = row["required_disclaimers"] if row else None
    general = await fetch_product_evidence_for_keys(product_keys, db=read_db)

    merged_profile = merge_evidence_profiles(beauty_profile, general.get("evidence_profile"))
    merged_disclaimers = merge_disclaimer_lists(beauty_disclaimers, general.get("required_disclaimers"))

    if not merged_profile and not merged_disclaimers:
        return {}
    return {
        "evidence_profile": merged_profile,
        "required_disclaimers": merged_disclaimers,
    }


def coalesce_first(*values: Any) -> Any:
    """First non-empty value (string trim aware)."""
    for v in values:
        if v is None:
            continue
        if isinstance(v, str):
            stripped = v.strip()
            if stripped:
                return stripped
        else:
            return v
    return None


_RETAILER_LISTING_KEY_PREFIX = "ext:retailer:"


def _is_retailer_listing(row: Dict[str, Any]) -> bool:
    """A retailer-lane listing: one store's page for a product (ingestion's retailer_listing_identity)."""
    return str(row.get("product_key") or "").startswith(_RETAILER_LISTING_KEY_PREFIX)


# Set by annotate_served_copy: the title and description a row would SERVE as the winner, plus
# the overlay read it came from. Never persisted; assemble_row reads named fields only.
_SERVED_COPY = "_served_copy"


def _passes_serving_content_bar(row: Dict[str, Any]) -> bool:
    """This row, as the winner, would supply content the serving gate accepts, under a signature.

    The gate's own checks (index_pipeline_state_service: suppressed / not_live / no_image /
    short_description), read the way the gate reads them -- a set suppressed_at, or a non-empty
    sync_status other than 'live', refuses; the description is the one agent_pdp_view would serve,
    measured stripped -- plus a title (assemble_row builds nothing without one) and a signature: the
    winner supplies the served id and canonical URL, so a row that would take the product off
    serving, or serve it unsigned, is not a content-ready winner (review #2384). The ONE definition
    both pick_canonical rules below use.

    The served title and description come from annotate_served_copy (overlay first) when the caller
    ran it -- the serving path does. Otherwise the row's own columns are the answer, and a caller
    that loads none of them never passes, so its order is unchanged. The image is the row's own:
    see annotate_served_copy for why the seed is not consulted.
    """
    from services.index_pipeline_state_service import MIN_DESCRIPTION_LENGTH

    if row.get("suppressed_at") is not None:
        return False
    sync_status = str(row.get("sync_status") or "").strip()
    if sync_status and sync_status != "live":
        return False
    served = row.get(_SERVED_COPY) or {"title": row.get("title"), "description": row.get("description")}
    if not str(served.get("title") or "").strip():
        return False
    if len(str(served.get("description") or "").strip()) < MIN_DESCRIPTION_LENGTH:
        return False
    return bool(str(row.get("image_url") or "").strip() and row.get("pivota_signature_id"))


def _is_brand_store_row(row: Dict[str, Any]) -> bool:
    """The brand's own store's copy of the product, with content to serve.

    Evidence is what the ingest pipeline already decided about the seller: a brand_direct offer on
    this row (Tier A or B proven at ingest, e.g. saiehello.com for Saie), or a host that IS the
    brand's domain -- and never a known retailer host. A row that fails `_passes_serving_content_bar`
    never wins here: it would take the product off serving or serve it unsigned. (A url_audit seed
    needs no test here: pick_canonical ranks it last before this rule is consulted.)
    """
    if _is_retailer_listing(row):
        return False
    if not _passes_serving_content_bar(row):
        return False
    return _is_brand_owned_row(row)


def _is_brand_owned_row(row: Dict[str, Any]) -> bool:
    """The brand's own store sells this row: not a retailer listing, a host that is not a known
    retailer, and a brand_direct offer (the ingest pipeline's seller verdict) or the brand's domain."""
    if _is_retailer_listing(row):
        return False
    from services.offer_seller_identity import brand_owns_domain, host_from_url, is_known_retailer, normalize_host

    host = normalize_host(row.get("source_domain")) or host_from_url(row.get("canonical_url"))
    if not host or is_known_retailer(host):
        return False
    return bool(row.get("has_brand_direct_offer")) or brand_owns_domain(row.get("brand"), host)


# ---------------------------------------------------------------------
# Brand section copy: a thin brand-store description, filled from the brand's own page sections
# ---------------------------------------------------------------------
#
# Measured on prod 2026-09-29: 1,849 of 8,117 served brand-store products serve under 200 chars,
# mostly the store's one-line tagline (the crawl's pdp_variant_description), while the same row's
# own seed already holds the brand's Details / Overview / Benefits sections from its page (Fenty's
# accordions, Rare Beauty, Pixi, Jurlique, Ole Henriksen...). The public PDP renders those sections;
# the served description -- what agents read and the serving gate measures -- never saw them.
#
# THE PICK DOES NOT SEE THEM, deliberately: _passes_serving_content_bar judges a row by its overlay
# and its own column, so a row whose column is under MIN_DESCRIPTION_LENGTH still loses to a sibling
# that passes. The sections only lengthen whichever row already won. The GATE does see them (it reads
# agent_pdp_view.description), so a winner that was blocked short_description can start serving.

# Set by build_agent_pdp_view_row. Never persisted.
_OWN_SEED_COPY = "_own_seed_copy"            # fetch_own_seed_copy_for_keys, per row
_SHARED_SECTION_LINES = "_shared_section_lines"  # fetch_shared_section_lines, on the winner

# canonical_pdp_enrichment's own "thin enough to be worth more copy" line.
_THIN_SERVED_DESCRIPTION_CHARS = 200
_BRAND_SECTION_MAX_CHARS = 900
_COMPOSED_DESCRIPTION_MAX_CHARS = 1600
_BRAND_SECTION_MAX_COUNT = 3
_BRAND_SECTION_MIN_CHARS = 80
# A line other products of the same storefront also carry is the store's copy, not this product's
# (scripts/backfill_brand_official_descriptions.drop_shared_boilerplate, mechanism 2). Counted in
# distinct OTHER products -- title before " — " -- so the shades of one product sharing its copy
# are one product, not boilerplate.
_SHARED_LINE_MIN_OTHER_PRODUCTS = 2
# Bounds on that check (fetch_shared_section_lines): lines judged per rebuild (the rest count as
# shared, i.e. are not appended), seeds scanned per storefront, and its statement_timeout.
_SHARED_LINE_CHECK_MAX = 24
_SHARED_LINE_SEED_SCAN_MAX = 3000
_SHARED_LINE_TIMEOUT_MS = 2000
# Two sentences this alike (word-set Jaccard) say the same thing twice.
_NEAR_DUPLICATE_JACCARD = 0.8

# The crawl's tags for a description taken as-is from the brand's page. Any other origin was chosen
# by a person -- the reviewed Pivota-intel rollbacks (pivota.description.rollback.v1, which replaced
# junk and wrong-variant copy), reviewed content patches, manual text -- and is served as reviewed.
_CRAWLED_DESCRIPTION_ORIGINS = frozenset({"", "pdp_variant_description", "pdp_product_description"})

# Matched against the heading's lowercased words. Descriptive sections only: never how-to,
# ingredients, FAQ, claims / clinical results, taxonomy ("Product Type"), "About the brand", or
# "Features" (murad.com's is a concern list ending in an item number).
_BRAND_COPY_HEADING = re.compile(
    r"(?:product )?(?:details|overview|description)|(?:key |product )?benefits?|what it (?:is|does)"
    r"|why (?:we made it|we love it|you ll love it|you will love it)|highlights"
)
# Storefront tags and image alt text are captured as "sections" but are not prose.
_NON_PROSE_SECTION_KIND = re.compile(r"tags?$|media_alt")

# A line of any of these classes is not product copy and is dropped from the section. The classes
# are what prod sections and review #2443 showed sitting beside real copy under a "Details" heading.
_NOT_PRODUCT_COPY: Tuple[Tuple[str, "re.Pattern[str]"], ...] = tuple(
    (name, re.compile(pattern, re.I))
    for name, pattern in (
        ("markup", r"<\s*/?\s*[a-z!]|&[a-z]+;|&#\d+;"),
        ("link", r"https?://|www\.|\]\(|\b[a-z0-9-]+\.(?:com|us|co|net|org|io|shop|store|beauty|kr|jp|uk|au|ca|sg|de|fr)\b"),
        ("contact", r"\S@\S|@\w|\b\w+ \[?at\]? \w+ \[?dot\]? (?:com|net|org|co)\b"
                    r"|(?<!\w)\+?\(?\d{2,4}\)?[\s.-]?\d{3,4}[\s.-]?\d{3,4}(?!\w)|\b(?:call|text|email|dm) us\b"),
        ("price", r"[$€£¥₩₹]\s?\d|\d\s?(?:円|원)|\b\d[\d,.]*\s?(?:usd|eur|gbp|krw|jpy|sgd|aud|cad|myr|won|yen)\b"
                  r"|\b(?:usd|eur|gbp|krw|jpy|sgd|aud|cad|myr|rm)\s?\d"),
        ("promo", r"\d+\s?%\s?off|\b(?:percent|half) off\b|\b(?:sale|promo|coupon|discount|bogo|deal"
                  r"|buy (?:one|1|two|2)|save (?:up to )?\S*\d|limited[- ]edition|while supplies last|limited time"
                  r"|gift with purchase|free (?:shipping|gift|sample|returns?)|complimentary|with (?:every|any) (?:purchase|order)"
                  r"|deluxe (?:sample|mini)|as seen (?:on|in)|featured in)\b"),
        ("store", r"\b(?:price|sold out|in stock|out of stock|add to (?:bag|cart)|shop now|available only"
                  r"|exclusively (?:at|on)|rewards?|refer a friend|privacy policy|terms of (?:use|service)"
                  r"|log ?in|sign in|my account|gift card|store locator|contact us|subscribe|sign up"
                  r"|newsletter|klarna|afterpay|affirm|sezzle|shop pay|pay in (?:4|four)|installments?|loyalty"
                  r"|(?:earn|redeem|bonus|reward) points?|points? (?:program|per|on every))\b"),
        ("shipping", r"\b(?:shipping|ships (?:in|within|free|from|to)|free returns|returns? (?:within|policy)"
                     r"|return policy|refunds?|business days|delivery (?:in|within|times?))\b"),
        ("reviews", r"\b(?:reviews?|reviewers|rated|ratings?|verified (?:customers|buyers|purchasers)|out of 5"
                    r"|\d(?:\.\d)? stars?)\b"),
        ("cross_sell", r"\b(?:pair (?:it|this|with)|pairs (?:well|perfectly|beautifully) with"
                       r"|complete (?:the|your) (?:look|routine|regimen)|you may also|also try|try it with"
                       r"|shop (?:the|our))\b"),
        ("legal", r"^\W*(?:full )?(?:ingredients|warnings?|caution|directions)\s*:|\b(?:for external use only"
                  r"|keep out of (?:the )?reach|discontinue use|consult (?:a|your) (?:doctor|physician)"
                  r"|prop(?:osition)? 65|avoid contact with (?:the )?eyes|not been evaluated by the (?:food and drug"
                  r" administration|fda)|not intended to diagnose)\b"),
        ("ingredient_list", r"^(?=(?:[^,]*,){8})(?=.*\b(?:aqua|water|glycerin|alcohol|parfum|fragrance|dimethicone)\b)"),
        ("call_to_action", r"\b(?:learn (?:more|how)|click|tap here|find out more|tiktok|instagram|follow us"
                           r"|(?:our|the) (?:blog|journal))\b"),
    )
)
# Two first-person words: a customer review captured beside the product copy.
_FIRST_PERSON = re.compile(r"\b(?:i|i'm|i've|my|me)\b", re.I)
# Our own generated summaries sometimes sit in the sections too ("positioned for brightening",
# "…from reviewed source evidence"); they are not the brand's copy.
_SYNTHETIC_SUMMARY = re.compile(r"\bpositioned (?:as|for|around)\b|\bpositioning\b|\breviewed source", re.I)
# "…trouble spotsUnique Feature: …" -- labels the crawl ran together; split them onto their own line.
_RUN_TOGETHER_LABEL = re.compile(r"(?<=[a-z0-9.)])(?=(?:[A-Z][a-z]+ ){0,3}[A-Z][a-z]+:)")
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
# "Pro Filt'r Soft Matte Longwear Foundation — #265" -> the product, without its shade or edition.
_EDITION_SUFFIX = re.compile(r"\s+[—–-]\s+.*$")


def _norm_words(text: Any) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", str(text or "").lower()))


def _product_base_title(title: Any) -> str:
    return _EDITION_SUFFIX.sub("", str(title or "")).strip().lower()


def _section_heading(value: Any) -> str:
    """The heading's first non-empty line (the crawl sometimes doubles it: "Details\\n\\nDetails")."""
    for line in str(value or "").splitlines():
        line = line.strip().rstrip(":?+").strip()
        if line:
            return line
    return ""


def _is_label_line(line: str) -> bool:
    """A heading-like line with no sentence end, most words capitalised: "What Else You Need To
    Know", "Skin Concerns - Dryness, Dullness", a run-together list of set contents."""
    if line.rstrip().endswith((".", "!", "?")):
        return False
    words = [w for w in re.findall(r"[A-Za-z][A-Za-z'’-]*", line) if len(w) >= 3]
    return bool(words) and sum(w[0].isupper() for w in words) >= 0.75 * len(words)


# Latin lookalikes NFKC leaves alone ("frее ѕhipping" in Cyrillic): folded before classifying.
_CONFUSABLES = str.maketrans({
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x", "ѕ": "s", "і": "i", "ј": "j",
    "ԁ": "d", "һ": "h", "ӏ": "l", "ԛ": "q", "ԝ": "w", "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M",
    "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X", "Ѕ": "S", "І": "I", "Ј": "J",
    "ο": "o", "α": "a", "ε": "e", "ι": "i", "κ": "k", "ν": "v", "ρ": "p", "τ": "t", "υ": "u", "χ": "x",
})
# A footnote line: the marker a claim's fine print starts with.
_FOOTNOTE_MARKERS = ("*", "†", "‡", "§", "¹", "²", "³")


def _fold(text: str) -> str:
    return unicodedata.normalize("NFKC", text).translate(_CONFUSABLES)


def _not_product_copy(line: str) -> Optional[str]:
    """The class of non-product text the line is (_NOT_PRODUCT_COPY), or None. Judged on the line
    NFKC-normalised with Latin lookalikes folded, so "＄ 25" and "frее ѕhipping" read as written."""
    probe = _fold(line)
    for name, pattern in _NOT_PRODUCT_COPY:
        if pattern.search(probe):
            return name
    return None


def _section_lines(body: Any) -> List[str]:
    """The section's candidate lines: five words or more, not a "*Based on…" / "†" footnote, not a label
    line, not any _NOT_PRODUCT_COPY class. Nothing is judged against what is served yet."""
    lines = []
    for raw in _RUN_TOGETHER_LABEL.sub("\n", str(body or "")).splitlines():
        line = " ".join(raw.split())
        if len(re.findall(r"[A-Za-z][A-Za-z'’-]*", line)) < 5:
            continue
        if line.startswith(_FOOTNOTE_MARKERS) or _is_label_line(line) or _not_product_copy(line):
            continue
        lines.append(line)
    return lines


def _clip_at_sentence(text: str, limit: int) -> str:
    """At most `limit` chars, ending on a sentence or line; "" rather than a fragment."""
    if len(text) <= limit:
        return text
    cut = text[:limit]
    end = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "), cut.rfind("\n"))
    return cut[: end + 1].strip() if end >= limit // 3 else ""


def _brand_section_plan(
    canonical: Dict[str, Any], enrichment: Optional[Dict[str, Any]]
) -> List[Tuple[str, List[str]]]:
    """[(heading, candidate lines)] for each section compose_brand_section_description may use, in
    page order -- or [] when the rule does not apply to this winner.

    It applies only when all hold:
      * no overlay description (curated or generated copy is served as written);
      * the winner's own column is under _THIN_SERVED_DESCRIPTION_CHARS;
      * the winner's own seed took it from the brand's page (_CRAWLED_DESCRIPTION_ORIGINS) and no
        reviewed rollback replaced it;
      * that seed is on the winner's own storefront (its domain, else its canonical URL's host);
      * the winner is the brand's own store (_is_brand_owned_row) -- a retailer's sections are the
        retailer's copy.
    Sections: descriptive headings only (_BRAND_COPY_HEADING), each heading once, prose kinds only,
    none containing a script, none in a reviewer's voice or our own generated summary's.
    """
    own = canonical.get(_OWN_SEED_COPY)
    if not isinstance(own, dict):
        return []
    if coalesce_first((enrichment or {}).get("description_markdown")):
        return []
    base = coalesce_first(canonical.get("description"))
    if not isinstance(base, str) or len(base) >= _THIN_SERVED_DESCRIPTION_CHARS:
        return []
    if own.get("description_reviewed") or str(own.get("description_origin") or "") not in _CRAWLED_DESCRIPTION_ORIGINS:
        return []
    if not _is_brand_owned_row(canonical):
        return []
    from services.offer_seller_identity import host_from_url, normalize_host

    row_host = normalize_host(canonical.get("source_domain")) or host_from_url(canonical.get("canonical_url"))
    seed_host = normalize_host(own.get("domain")) or host_from_url(own.get("canonical_url"))
    if not row_host or seed_host != row_host:
        return []
    sections = _parse_jsonish(own.get("details_sections"))
    if not isinstance(sections, list):
        return []

    plan: List[Tuple[str, List[str]]] = []
    headings_used = set()
    for section in sections:
        if not isinstance(section, dict) or _NON_PROSE_SECTION_KIND.search(str(section.get("source_kind") or "")):
            continue
        heading = _section_heading(section.get("heading"))
        heading_words = _norm_words(heading)
        if heading_words in headings_used or not _BRAND_COPY_HEADING.fullmatch(heading_words):
            continue
        body = str(section.get("body") or "")
        if re.search(r"<\s*script", body, re.I):
            continue
        lines = _section_lines(body)
        text = "\n".join(lines)
        if not lines or len(_FIRST_PERSON.findall(text)) >= 2 or _SYNTHETIC_SUMMARY.search(text):
            continue
        headings_used.add(heading_words)
        plan.append((heading, lines))
    return plan


def brand_section_candidate_lines(canonical: Dict[str, Any], enrichment: Optional[Dict[str, Any]]) -> List[str]:
    """Every line compose_brand_section_description might serve for this winner, in page order --
    the lines the storefront repetition check (fetch_shared_section_lines) must look up first."""
    return list(dict.fromkeys(line for _, lines in _brand_section_plan(canonical, enrichment) for line in lines))


def _near_duplicate(words: frozenset, served: List[frozenset]) -> bool:
    return any(len(words & other) >= _NEAR_DUPLICATE_JACCARD * len(words | other) for other in served)


def compose_brand_section_description(
    canonical: Dict[str, Any],
    description: Optional[str],
    enrichment: Optional[Dict[str, Any]],
) -> Optional[str]:
    """The served description, followed by the brand's own descriptive page sections when the
    winner is a thin crawled tagline on the brand's own store row (_brand_section_plan). Otherwise
    the description, unchanged.

    Only onto the winner's own column (the plan requires it and no overlay, so assemble_row serves
    exactly that column; a seed fallback, which depends on the caller, never qualifies). Only after the
    storefront repetition check ran (build_agent_pdp_view_row sets _SHARED_SECTION_LINES); without
    it nothing is added. Then, per line: dropped if other products of the storefront carry it;
    sentences dropped when they repeat, or nearly repeat, anything already served (the tagline
    included). A section needs _BRAND_SECTION_MIN_CHARS left, is cut at a sentence to
    _BRAND_SECTION_MAX_CHARS; at most _BRAND_SECTION_MAX_COUNT sections and
    _COMPOSED_DESCRIPTION_MAX_CHARS in all.
    """
    if not isinstance(description, str):
        return description
    plan = _brand_section_plan(canonical, enrichment)
    shared = canonical.get(_SHARED_SECTION_LINES)
    if not plan or shared is None:
        return description

    served: List[frozenset] = [frozenset(_norm_words(s).split()) for s in _SENTENCE_END.split(description)]
    served = [s for s in served if s]
    blocks: List[str] = []
    total = len(description)
    for heading, lines in plan:
        if len(blocks) >= _BRAND_SECTION_MAX_COUNT:
            break
        kept_lines: List[str] = []
        kept_sets: List[frozenset] = []
        for line in lines:
            if line in shared:
                continue
            sentences = []
            for sentence in _SENTENCE_END.split(line):
                words = frozenset(_norm_words(sentence).split())
                if not words or _near_duplicate(words, served + kept_sets):
                    continue
                sentences.append(sentence)
                kept_sets.append(words)
            rest = " ".join(sentences)
            if len(re.findall(r"[A-Za-z][A-Za-z'’-]*", rest)) >= 5:
                kept_lines.append(rest)
        prose = _clip_at_sentence("\n".join(kept_lines), _BRAND_SECTION_MAX_CHARS)
        if len(prose) < _BRAND_SECTION_MIN_CHARS:
            continue
        block = f"{heading.capitalize() if heading.isupper() else heading}\n{prose}"
        if total + 2 + len(block) > _COMPOSED_DESCRIPTION_MAX_CHARS:
            break
        blocks.append(block)
        total += 2 + len(block)
        served.extend(kept_sets)
    return "\n\n".join([description, *blocks]) if blocks else description


def pick_canonical(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Deterministic canonical-row pick.

    Tiebreak ladder:
      0. platform != 'url_audit'  (a real row always beats an observed audit seed)
      0b. ONLY where the content_key also holds a retailer-lane listing: the brand's own store's
          row (`_is_brand_store_row`) first. ADR-001 precedence -- brand-official content over a
          reseller's -- which the ladder below ignored: every member is primary and signed, so the
          lowest product_key decided, and "ext:retailer:..." beat every brand slug sorting after
          "r" (measured 2026-09-26: 49 of 75 mixed content_keys served a retailer's copy while the
          brand's own store row -- westman-atelier.com, naturium.com, tomfordbeauty.com,
          saiehello.com -- sat beside it). Groups without a retailer listing are ordered exactly
          as before.
      0c. a row that passes `_passes_serving_content_bar` before one that does not. The winner
          supplies the served title, description, image, id and canonical URL, and
          index_pipeline_state gates the whole content_key on what it serves. Replayed with the
          members' real overlays over all 2,897 multi-row content_keys on prod 2026-09-28, 179
          change: 38 blocked short_description (koolseoul's title-only rows beside dodoskin/coscorea
          1-2k-char copy) and 2 blocked no_image start serving; 135 were served from a SUPPRESSED
          row (the retired Stila/Tarte/Tower 28 old-spelling rows, arencia's JP seeds, re-keyed
          simihaze variants, the suppressed cocomo.sg row) and move to the live sibling; no served
          overlay is dropped. A winner that already passes keeps rank 0, so only a content_key
          whose current winner fails the bar can change.
      1. product_group_members.is_primary = true  (multi-seller canonical)
      2. catalog_products.pivota_signature_id is set  (indexed surface)
      3. lowest product_key ASC  (stable hash-derived ordering)

    Tier 0 is a safety guardrail: a `url_audit` seed's content_key is brand+title
    (non-unique, GTIN-less), so it can COLLIDE with a real/claimed product sharing
    that content_key. agent_pdp_view is keyed by content_key, so whichever row
    wins here supplies the served title/description/image. An audit seed must
    NEVER win that over a non-audit row — else a colliding seed would overwrite a
    claimed product's served PDP. Audit seeds therefore sort strictly last; when a
    content_key has ONLY audit seeds (the common case — a competitor/arbitrary URL
    the merchant audited), it isn't served anyway (no index_pipeline_state row).
    """
    mixed_with_retailer = any(_is_retailer_listing(r) for r in rows)

    def key(r: Dict[str, Any]) -> Tuple[int, int, int, int, int, str]:
        audit_rank = 1 if r.get("platform") == "url_audit" else 0
        brand_rank = (0 if _is_brand_store_row(r) else 1) if mixed_with_retailer else 0
        content_rank = 0 if _passes_serving_content_bar(r) else 1
        primary_rank = 0 if r.get("group_is_primary") else 1
        sig_rank = 0 if r.get("pivota_signature_id") else 1
        return (audit_rank, brand_rank, content_rank, primary_rank, sig_rank, r.get("product_key") or "")

    return sorted(rows, key=key)[0]


def read_image_urls(payload: Any) -> List[str]:
    """Pull a deduped image-URL list out of product_payload JSONB. Real
    upstream shapes vary (Shopify nests under .images[].src, Wix has
    .media, our external seeds drop a flat array). Try the common ones.
    """
    if not isinstance(payload, dict):
        return []
    found: List[str] = []

    def _add(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            url = value.strip()
            if url not in found:
                found.append(url)

    for v in payload.get("image_urls") or []:
        _add(v)
    for v in payload.get("images") or []:
        if isinstance(v, str):
            _add(v)
        elif isinstance(v, dict):
            _add(v.get("src") or v.get("url"))
    if not found:
        _add(payload.get("image_url"))
    return found


def normalize_offer(offer: Dict[str, Any], primary_merchant_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """Project a catalog_offers row + merchants join down to the
    offer-shape we store in agent_pdp_view.offers JSONB. Returns None if
    the offer has no usable price.
    """
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
        "url": None,  # caller injects via merchant_url_by_id
        "is_primary": offer.get("merchant_id") == primary_merchant_id,
        # Buyability facts (market-agnostic; the serve layer decides buyable-here
        # against the request market). A brand-direct KRW offer and a US retailer
        # offer must be distinguishable so agents don't treat a cross-border
        # listing as a same-market purchase.
        "market": offer.get("market"),
        "offer_type": offer.get("offer_type"),
        "is_first_party": bool(offer.get("is_first_party")),
    }


def aggregate_offers(
    offers: List[Dict[str, Any]],
    primary_merchant_id: Optional[str],
    merchant_url_by_id: Dict[str, Optional[str]],
    seller_trust_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[
    Optional[str],
    Optional[Decimal],
    Optional[Decimal],
    int,
    List[Dict[str, Any]],
]:
    """Compute price aggregates + top-N offers. Stable ordering:
    sellable before known-unavailable, then primary merchant first, then
    price ASC, then merchant_id ASC — all applied BEFORE the top-N cut.

    `seller_trust_by_id` (W8): the outcome-derived per-merchant trust envelope,
    injected onto each offer like `url` is. Absent/None → offers carry no trust
    key (the honest empty state — no transacted outcomes, no claim).
    """
    normalized = _normalized_offers(
        offers, primary_merchant_id, merchant_url_by_id, seller_trust_by_id
    )

    if not normalized:
        return None, None, None, 0, []

    # Modal currency: dominant one across the group. Mixed-currency
    # groups (rare but real — .com vs .ca) surface the dominant code at
    # the row level; each offer keeps its own currency intact.
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

    # SELLABLE FIRST, and before the cut. (primary, price) alone let a cheaper or primary
    # out-of-stock offer lead the list and let the top-N cut drop the sellable ones. Rebuilt
    # read-only from prod on 2026-09-18: 39 of 12,643 rows reorder, 26 of them led with an
    # out-of-stock offer while a sellable one sat behind it, 13 change which offers survive the
    # cut, and on 1 (a 39-shade lipstick whose five cheapest-then-first shades were all sold
    # out) the cut had left no in-stock offer, so the served buy pick was a sold-out shade.
    #
    # ABOVE is_primary, deliberately. is_primary says whose listing supplies the page, not who
    # can sell it now; the brand's own sold-out listing ahead of an in-stock retailer is the
    # defect, not a preference (The Ordinary out of stock over ulta.com in stock at the same
    # $19; COSRX out of stock at $15 over stylekorean in stock at $6.30). The best_offer
    # companion to #2218 (PIVOTA-Agent#2240, offersToSignals) makes the same call. The primary still leads WITHIN its
    # stock group, and can only leave the stored set when it is unavailable and at least N
    # sellable offers exist — 0 rows in prod that day. No reader of these offers (backend,
    # PIVOTA-Agent, pivota-agent-ui) keys on is_primary or offers[0]; offer_count, price_min
    # and price_max still cover every offer, unavailable ones included.
    #
    # Unknown availability is sellable here: one vocabulary, OFFER_UNAVAILABLE_AVAILABILITIES,
    # shared with offers.resolve; the reasons are at the constant in services/offer_buyability.py.
    #
    # NO internal-offer exemption — offers.resolve has none either since #2221. An internal
    # offer's `availability` is what catalog_sync wrote from the gate's own stock verdict
    # (standard_variant_in_stock: `available` first, then quantity), so an untracked or
    # keep-selling variant is in_stock here and a sold-out one is not.
    top = sorted(normalized, key=_offer_sort_key)[:OFFER_TOP_N]
    return currency, price_min, price_max, len(normalized), top


def _normalized_offers(
    offers: List[Dict[str, Any]],
    primary_merchant_id: Optional[str],
    merchant_url_by_id: Dict[str, Optional[str]],
    seller_trust_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """normalize_offer over every row, with the url and seller_trust injected.
    The one projection both aggregate_offers and build_market_prices read, so
    the per-market summaries can never describe a different offer set."""
    trust_by_id = seller_trust_by_id or {}
    normalized: List[Dict[str, Any]] = []
    for o in offers:
        n = normalize_offer(o, primary_merchant_id)
        if not n:
            continue
        merchant_id = n.get("merchant_id") or ""
        n["url"] = merchant_url_by_id.get(merchant_id)
        trust = trust_by_id.get(merchant_id)
        if trust:
            n["seller_trust"] = trust
        normalized.append(n)
    return normalized


def _offer_sort_key(o: Dict[str, Any]) -> Tuple[int, int, float, str]:
    """The stored-offer order (see the comment in aggregate_offers): sellable
    first, then primary, then price ASC, then merchant_id ASC."""
    return (
        1 if availability_is_known_unavailable(o.get("availability")) else 0,
        0 if o.get("is_primary") else 1,
        float(o.get("price") or 0.0),
        o.get("merchant_id") or "",
    )


# ---------------------------------------------------------------------
# Per-currency price summary (agent_pdp_view.market_prices, migration 263)
# ---------------------------------------------------------------------
#
# WHY. The row-level currency / price_min / price_max above are ONE currency:
# the modal one, ties to the higher code. A product can carry offers in several
# served currencies (retailer_ingest shopify_markets writes USD siblings beside a
# non-USD base offer today, and SGD siblings beside USD ones once #2553 lands);
# with USD offers and SGD ones it is a USD row, and every reader that serves an
# SG buyer from those columns drops it; and the top-N cut sorts raw prices
# across currencies, so the SGD offers can be cut from `offers` altogether.
#
# THE CONTRACT (version 1):
#
#   {"version": 1,
#    "currencies": {"USD": {"price_min": 18.0, "price_max": 24.0,
#                           "offer_count": 3, "offers": [<top-N USD offers>]},
#                   "SGD": {...}}}
#
#   * One entry per currency the offers are priced in (upper-cased ISO code;
#     an offer with no well-formed code prices no entry). KEYED BY CURRENCY, and
#     every currency, so the stored value never depends on the writer's
#     environment (PIVOTA_SERVING_PRICING_REGIONS): the READER maps its buyer's
#     market to a currency (services/region_pricing) and decides what it serves.
#     Never offers.market, a DEFAULT 'US' most writers never set.
#   * No conversion: an entry only compares amounts in its own currency.
#   * offer_count counts every such offer, unavailable ones included, like the
#     row-level offer_count; `offers` is that currency's top OFFER_TOP_N in the
#     stored order (_offer_sort_key), the same offer dicts `offers` carries.
#   * {"version": 1, "currencies": {}} = computed, no priced offer.
#
# STALENESS. A writer without the flag (an old image, a one-off with no env,
# the flag turned off, scripts/repair_external_seed_offer_mainline.py's
# APV_OFFER_FIELDS_UPDATE_SQL) rewrites offers and refreshed_at but not the
# summary. So the flag-on upsert stamps market_prices_refreshed_at with the
# same NOW() as refreshed_at, and readers use the summary ONLY while the two
# are equal (MARKET_PRICES_FRESH_SQL); any later write that bumps refreshed_at
# without restamping retires it. NULL summary, NULL stamp, an unknown version:
# all read as "not computed", and readers fall back to the legacy columns.
#
# The legacy columns are computed exactly as before and never read from here.
# Written only while AGENT_PDP_VIEW_MARKET_PRICES is on and the columns exist
# (agent_pdp_view_has_market_prices): otherwise assemble_row emits no summary
# and the upsert is UPSERT_SQL byte for byte.

MARKET_PRICES_FLAG_ENV = "AGENT_PDP_VIEW_MARKET_PRICES"
MARKET_PRICES_VERSION = 1
_ISO_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


def market_prices_enabled() -> bool:
    """Write flag for agent_pdp_view.market_prices. Default OFF."""
    return (os.getenv(MARKET_PRICES_FLAG_ENV) or "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def market_prices_fresh_sql(alias: str = "") -> str:
    """The summary only while it was written by the write that set refreshed_at, else NULL.
    Every reader selects the column through this, never bare."""
    a = f"{alias}." if alias else ""
    return (
        f"CASE WHEN {a}market_prices_refreshed_at = {a}refreshed_at "
        f"AND {a}market_prices->>'version' = '{MARKET_PRICES_VERSION}' "
        f"THEN {a}market_prices END"
    )


def build_market_prices(
    offers: List[Dict[str, Any]],
    primary_merchant_id: Optional[str],
    merchant_url_by_id: Dict[str, Optional[str]],
    seller_trust_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """The per-currency price summary (contract above). Pure, and independent of
    process environment."""
    normalized = sorted(
        _normalized_offers(offers, primary_merchant_id, merchant_url_by_id, seller_trust_by_id),
        key=_offer_sort_key,
    )
    by_currency: Dict[str, List[Dict[str, Any]]] = {}
    for n in normalized:
        code = str(n.get("currency") or "").strip().upper()
        if _ISO_CURRENCY_RE.match(code):
            by_currency.setdefault(code, []).append(n)
    currencies: Dict[str, Dict[str, Any]] = {}
    for code in sorted(by_currency):
        in_currency = by_currency[code]
        prices = [Decimal(str(n["price"])) for n in in_currency]
        currencies[code] = {
            "price_min": float(min(prices)),
            "price_max": float(max(prices)),
            "offer_count": len(in_currency),
            "offers": in_currency[:OFFER_TOP_N],
        }
    return {"version": MARKET_PRICES_VERSION, "currencies": currencies}


def market_prices_entry(market_prices: Any, currency: Optional[str]) -> Optional[Dict[str, Any]]:
    """A reader's lookup: the entry for `currency` in a version-1 summary, else None."""
    if isinstance(market_prices, str):
        try:
            market_prices = json.loads(market_prices)
        except ValueError:
            return None
    if not isinstance(market_prices, dict) or market_prices.get("version") != MARKET_PRICES_VERSION:
        return None
    currencies = market_prices.get("currencies")
    code = str(currency or "").strip().upper()
    entry = currencies.get(code) if isinstance(currencies, dict) and code else None
    return entry if isinstance(entry, dict) else None


# Does agent_pdp_view have both columns? Migration 263 does not self-apply (schema_guard adds the
# columns at boot, and a heal can defer), and a writer or reader naming a missing column fails --
# for the refresh path silently, since catalog_sync swallows the error and legacy prices would stop
# refreshing. Probed once per process: a positive answer is kept, a negative one for
# _MARKET_PRICES_PROBE_RETRY_S, so a later heal is picked up without a restart.
_MARKET_PRICES_COLUMNS = ("market_prices", "market_prices_refreshed_at")
_MARKET_PRICES_PROBE_RETRY_S = 300.0
_market_prices_probe: Dict[str, Any] = {"present": None, "checked_at": 0.0}
MARKET_PRICES_COLUMNS_PROBE_SQL = """
    SELECT count(*) AS n
    FROM information_schema.columns
    WHERE table_name = 'agent_pdp_view'
      AND table_schema = ANY(current_schemas(false))
      AND column_name IN ('market_prices', 'market_prices_refreshed_at')
"""


def reset_market_prices_probe() -> None:
    _market_prices_probe.update(present=None, checked_at=0.0)


def mark_market_prices_columns_missing() -> None:
    import time

    _market_prices_probe.update(present=False, checked_at=time.monotonic())


async def agent_pdp_view_has_market_prices(db: Any = None) -> bool:
    import time

    if _market_prices_probe["present"] is True:
        return True
    now = time.monotonic()
    if (
        _market_prices_probe["present"] is False
        and now - _market_prices_probe["checked_at"] < _MARKET_PRICES_PROBE_RETRY_S
    ):
        return False
    try:
        row = await (db or database).fetch_one(MARKET_PRICES_COLUMNS_PROBE_SQL)
        present = bool(row) and int(dict(row).get("n") or 0) == len(_MARKET_PRICES_COLUMNS)
    except Exception:  # noqa: BLE001 - an unanswerable probe is "absent": the legacy path is safe
        present = False
    _market_prices_probe.update(present=present, checked_at=now)
    return present


def is_missing_market_prices_column_error(exc: BaseException) -> bool:
    """asyncpg UndefinedColumnError (42703) naming the summary columns."""
    code = getattr(exc, "sqlstate", None) or getattr(getattr(exc, "orig", None), "sqlstate", None)
    message = str(exc)
    return (code == "42703" or "does not exist" in message) and "market_prices" in message


def aggregate_variants(skus: List[Dict[str, Any]], canonical_source_product_id: Optional[str]) -> Tuple[List[Dict[str, Any]], int]:
    """Build variants[]. Drop the "singleton placeholder" SKU where
    source_variant_id == source_product_id (Path A sync inserts one such
    row per product to keep SKU joins valid even when upstream has no
    variants — not a real variant).
    """
    variants: List[Dict[str, Any]] = []
    for s in skus:
        svid = s.get("source_variant_id")
        spid = s.get("source_product_id")
        if svid and spid and str(svid) == str(spid):
            continue
        variants.append({
            "variant_id": svid,
            "sku": s.get("sku"),
            "title": s.get("title"),
            "options": s.get("visible_option_labels") or s.get("visible_attributes") or {},
            "image_url": s.get("image_url"),
            "currency": s.get("currency"),
            "merchant_id": s.get("merchant_id"),
        })

    # Deterministic ordering: SKU then variant_id. Keeps re-runs
    # byte-identical for diff-friendly admin tooling.
    variants.sort(key=lambda v: (v.get("sku") or "", str(v.get("variant_id") or "")))

    count = len(variants)
    return variants[:VARIANT_CAP], count


def pick_gtin13(skus: List[Dict[str, Any]]) -> Optional[str]:
    """Pick the canonical 14-char GTIN for the content_key group.

    Two failure modes to avoid:
      1. A barcode that is not a GTIN. validated_source_gtin owns that rule
         (GTIN-8/12/13/14, check digit, never all-zero): normalize_gtin alone
         passes 15+ digits through and zero-pads "0" into a GS1-shaped
         "00000000000000", which the catalog_products.gtin backfill would hand
         to the GLOBAL Tier-0 GTIN matcher.
      2. SKUs in the same content_key group can carry different
         barcodes (data noise, or genuine cross-merchant disagreement).
         Pick the modal value so we deterministically converge on the
         GTIN that appears most often; tiebreak by lex order so re-runs
         are byte-identical.
    """
    counts: Dict[str, int] = {}
    for s in skus:
        bar = s.get("barcode")
        if not bar:
            continue
        canon = validated_source_gtin(str(bar))
        if not canon:
            continue
        counts[canon] = counts.get(canon, 0) + 1
    if not counts:
        return None
    # Sort by count DESC, then GTIN ASC for stable tiebreak.
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def evidence_safe_product_keys(
    products: List[Dict[str, Any]], skus: List[Dict[str, Any]]
) -> List[str]:
    """Cluster members SAFE to draw agent-decision-grade evidence from.

    A no-GTIN content_key is DELIBERATELY non-unique — it collapses to brand+title
    (services/catalog_identity.py) — so a content_key cluster can collide two
    DISTINCT physical products. fetch_evidence_for_keys picks ONE member's evidence
    cluster-wide (ORDER BY external-seed-origin DESC, updated_at DESC LIMIT 1) with no
    record of which member; assemble_row then bakes it onto pick_canonical's row,
    which may be a DIFFERENT product — i.e. Brand A's substantiated claim served on
    Brand B's listing. Restrict the evidence fetch to the canonical member UNLESS
    the cluster is provably ONE product:
      - GTIN-resolved (a real barcode folded the cluster — the merge is intended);
      - a single member (nothing was merged);
      - all members share one normalized brand + title (legit multi-merchant DTC).
    Returns the product_key list to pass to fetch_evidence_for_keys.
    """
    pkeys = [p["product_key"] for p in products if p.get("product_key")]
    if len(set(pkeys)) <= 1:
        return pkeys
    if pick_gtin13(skus):
        return pkeys
    from services.catalog_identity import normalize_brand, normalize_title

    brands = {normalize_brand(p.get("brand") or "") for p in products}
    titles = {normalize_title(p.get("title") or "") for p in products}
    if len(brands) == 1 and "" not in brands and len(titles) == 1:
        return pkeys
    # Collision: only the canonical member's evidence may be attributed to the
    # served row. Scoping the fetch (rather than suppressing post-hoc) also keeps
    # the canonical product's own required_disclaimers — the floor-safety field
    # rides the same query — without ever pulling a colliding member's claims.
    canonical = pick_canonical(products)
    canon_pk = canonical.get("product_key")
    return [canon_pk] if canon_pk else []


# The list-typed members of taxonomy_tags. Their source columns are JSONB, and
# depending on driver codec a read can hand them back as JSON *strings* rather
# than decoded lists. Embedding that string and re-serializing the outer dict is
# how prod ended up serving `"tags": "[\"serum\"]"` — a JSON string inside
# JSON, which every agent-side consumer mis-parses (measured 2026-08-10:
# 33/26/26 string-typed values across a 39-row sample of the live view).
_TAXONOMY_LIST_KEYS = ("tags", "use_case_tags", "lifestyle_tags")


def _parse_jsonish(value: Any) -> Any:
    """Decode a JSON-encoded-string container; pass everything else through.

    Only strings that LOOK like JSON containers are attempted, and a failed
    parse returns the original value — a plain scalar tag is never destroyed
    and nothing is ever invented.
    """
    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:1] in ("[", "{"):
            try:
                return json.loads(stripped)
            except (ValueError, TypeError):
                return value
    return value


def normalize_taxonomy_tags(value: Any) -> Any:
    """Repair double-encoded taxonomy_tags for consumers.

    Read-side twin of the write-side coercion in build_taxonomy_tags — and
    effectively PERMANENT, not transitional: the reconciler only re-assembles
    rows whose truth timestamps moved, so a historical row with no source
    change keeps the string-encoded shape forever unless
    scripts/backfill_agent_pdp_view.py is run once. Every projection must
    therefore normalize on the way out, indefinitely. Handles the
    whole-dict-as-string case too. Never raises.
    """
    value = _parse_jsonish(value)
    if not isinstance(value, dict):
        return value
    out = dict(value)
    for key in _TAXONOMY_LIST_KEYS:
        if key in out:
            out[key] = _parse_jsonish(out[key])
    return out


def build_taxonomy_tags(canonical: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fields = {
        "price_tier": canonical.get("price_tier"),
        # Coerced at WRITE time so freshly assembled rows store real arrays —
        # see _TAXONOMY_LIST_KEYS for how the string shape got in.
        "use_case_tags": _parse_jsonish(canonical.get("use_case_tags")),
        "lifestyle_tags": _parse_jsonish(canonical.get("lifestyle_tags")),
        "demographic": canonical.get("demographic"),
        "product_type": canonical.get("product_type"),
        "category": canonical.get("category"),
        "tags": _parse_jsonish(canonical.get("tags")),
    }
    tags = {k: v for k, v in fields.items() if v not in (None, "", [], {})}
    return tags or None


def build_breadcrumb(canonical: Dict[str, Any], pdp_url: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    title = coalesce_first(canonical.get("title"))
    if not title:
        return None
    crumbs: List[Dict[str, Any]] = [
        {"position": 1, "name": "Home", "item": "https://agent.pivota.cc/"},
    ]
    category = coalesce_first(canonical.get("category"), canonical.get("product_type"))
    if category:
        crumbs.append({"position": len(crumbs) + 1, "name": category, "item": None})
    brand = coalesce_first(canonical.get("brand"))
    if brand:
        crumbs.append({"position": len(crumbs) + 1, "name": brand, "item": None})
    crumbs.append({"position": len(crumbs) + 1, "name": title, "item": pdp_url})
    return crumbs


# Source-priority ordering used by coalesce_fashion_fields. Lower index = higher
# priority. Merchant_payload always wins (it's the merchant's authoritative
# platform); merchant_authored beats LLM; LLM beats external_seed; unknown /
# legacy sources fall through to the lowest priority. Order matters — do not
# rearrange without updating the corresponding source-precedence rule in
# services/fashion_field_authoring.py (write-side guard).
_FASHION_SOURCE_PRIORITY = (
    "merchant_payload",
    "merchant_authored",
    "llm_extraction_v1",
    "external_seed",
)


def _source_rank(src: Optional[str]) -> int:
    """Lower is better. Unknown / None falls to the worst rank."""
    if not src:
        return len(_FASHION_SOURCE_PRIORITY) + 1
    try:
        return _FASHION_SOURCE_PRIORITY.index(src)
    except ValueError:
        return len(_FASHION_SOURCE_PRIORITY)


def _looks_like_text_value(v: Any) -> bool:
    """material / care must be a non-blank string. Codex review noted
    that an empty dict, list, or whitespace-only string from a corrupted
    upstream row was passing the truthiness check and could land in a
    TEXT column or surface to merchant-facing prose. Defend explicitly."""
    return isinstance(v, str) and bool(v.strip())


def _looks_like_size_guide_value(v: Any) -> bool:
    """size_guide accepts strings (wrapped as {raw: ...}) or dicts
    (structured charts). Lists, numbers, bools are rejected."""
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, dict):
        return bool(v)
    return False


def _value_is_valid_for_field(field: str, value: Any) -> bool:
    if field == "size_guide":
        return _looks_like_size_guide_value(value)
    return _looks_like_text_value(value)


def _candidates_for(
    products: List[Dict[str, Any]],
    external_seed: Optional[Dict[str, Any]],
    field: str,
) -> List[Dict[str, Any]]:
    """Build the candidate list for one fashion field across all members
    of a product_group plus the matched external_product_seed.

    Each candidate is `{value, source, confidence, product_key,
    is_primary}` with provenance preserved so the caller can record
    which source won, and so equal-rank ties break deterministically
    against the product_group primary member.

    Type-gating per field is enforced here — a corrupted upstream row
    with `material: []` or `care: {}` is dropped before it reaches the
    coalesce sort or a merchant-facing surface.
    """
    out: List[Dict[str, Any]] = []
    src_col = f"{field}_source"
    conf_col = f"{field}_confidence"
    for p in products:
        v = p.get(field)
        if not _value_is_valid_for_field(field, v):
            continue
        out.append({
            "value": v,
            "source": p.get(src_col),
            "confidence": p.get(conf_col),
            "product_key": p.get("product_key") or "",
            "is_primary": bool(p.get("group_is_primary")),
        })
    if external_seed is not None:
        seed_data = external_seed.get("seed_data") or {}
        if isinstance(seed_data, str):
            try:
                seed_data = json.loads(seed_data)
            except Exception:
                seed_data = {}
        if isinstance(seed_data, dict):
            seed_val = seed_data.get(field)
            if _value_is_valid_for_field(field, seed_val):
                # size_guide from a seed is the same plain-string shape
                # the LLM extractor and the merchant-authored path use —
                # wrap into {raw: ...} so downstream JSONB writers see a
                # consistent dict. Was previously emitted unwrapped,
                # producing inconsistent gateway shapes.
                if field == "size_guide" and isinstance(seed_val, str):
                    seed_val = {"raw": seed_val.strip()}
                out.append({
                    "value": seed_val,
                    "source": "external_seed",
                    # Seed-derived confidence isn't structured in seed_data
                    # today; treat as 1.0 to keep ranking deterministic but
                    # park it at the lowest source priority above.
                    "confidence": 1.0,
                    "product_key": "",
                    "is_primary": False,
                })
    return out


def coalesce_fashion_fields(
    products: List[Dict[str, Any]],
    external_seed: Optional[Dict[str, Any]],
) -> Dict[str, Optional[Any]]:
    """Pick the winning material / care / size_guide across the product_group.

    For each field, sort candidates by (source priority, confidence DESC,
    is_primary DESC, product_key ASC) and take the head. Each field is
    coalesced independently — the winning row for material may differ
    from the winning row for size_guide.

    Deterministic tie-breaking: equal source + confidence is resolved by
    preferring the product_group primary member, then by alphabetic
    product_key. Codex review flagged that unordered ties could flap
    between merchants on every refresh; this pins them.

    Returns a dict with `<field>`, `<field>_source`, `<field>_confidence`
    for all three fields (None when no candidate exists).
    """
    coalesced: Dict[str, Optional[Any]] = {}
    for field in ("material", "care", "size_guide"):
        candidates = _candidates_for(products, external_seed, field)
        if not candidates:
            coalesced[field] = None
            coalesced[f"{field}_source"] = None
            coalesced[f"{field}_confidence"] = None
            continue
        candidates.sort(key=lambda c: (
            _source_rank(c["source"]),
            -(c["confidence"] if isinstance(c.get("confidence"), (int, float)) else 0.0),
            # is_primary True (0) wins over False (1)
            0 if c.get("is_primary") else 1,
            c.get("product_key") or "",
        ))
        winner = candidates[0]
        coalesced[field] = winner["value"]
        coalesced[f"{field}_source"] = winner["source"]
        coalesced[f"{field}_confidence"] = winner["confidence"]
    return coalesced


def assemble_row(
    *,
    content_key: str,
    products: List[Dict[str, Any]],
    skus: List[Dict[str, Any]],
    offers: List[Dict[str, Any]],
    external_seed: Optional[Dict[str, Any]],
    evidence: Optional[Dict[str, Any]] = None,
    refresh_source: str = BACKFILL_REFRESH_SOURCE,
    enrichment: Optional[Dict[str, Any]] = None,
    seller_trust_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """Produce the agent_pdp_view row payload from raw source rows.
    Returns None when the data is too thin to be useful (no title).
    Side-effect-free: caller persists.

    `enrichment` is the Pivota product_enrichment overlay row for the canonical
    product (E2 — the enrichment publish bridge). When it carries a
    description_markdown, that curated/generated copy takes precedence over the
    raw storefront description so it reaches BOTH the served PDP and the
    serving-eligibility gate (which reads agent_pdp_view.description). Optional;
    callers that don't pass it (the maintenance scripts) keep prior behavior.
    """
    canonical = pick_canonical(products)
    # E2 publish bridge: a brand-attested title_override outranks the raw
    # storefront title (same precedence the description bridge below applies),
    # so the brand's own title reaches the served PDP. coalesce_first is
    # strip-aware, so an absent/empty override falls through.
    title = coalesce_first(
        (enrichment or {}).get("title_override"),
        canonical.get("title"),
    )
    if not title:
        return None
    # Clean storefront noise (bundle tags, legal-entity seller prefix) before
    # this title reaches the agent surface; keeps size/pack info intact.
    title = normalize_display_title(title)

    seed_data = (external_seed or {}).get("seed_data") or {}
    if isinstance(seed_data, str):
        try:
            seed_data = json.loads(seed_data)
        except Exception:
            seed_data = {}

    # E2 publish bridge: the Pivota enrichment overlay's description_markdown
    # outranks the raw storefront description (it's the compliance-checked,
    # citable copy generated for the canonical PDP). None/empty falls through —
    # coalesce_first is strip-aware, so an absent overlay can't blank the field.
    description = coalesce_first(
        (enrichment or {}).get("description_markdown"),
        canonical.get("description"),
        seed_data.get("description"),
        seed_data.get("short_description"),
    )
    description = compose_brand_section_description(canonical, description, enrichment)

    image_url = coalesce_first(
        canonical.get("image_url"),
        (external_seed or {}).get("image_url"),
        seed_data.get("image_url"),
    )

    image_urls = read_image_urls(canonical.get("product_payload"))
    if image_url and image_url not in image_urls:
        image_urls.insert(0, image_url)
    seed_images = seed_data.get("image_urls") if isinstance(seed_data, dict) else None
    if isinstance(seed_images, list):
        for u in seed_images:
            if isinstance(u, str) and u.strip() and u.strip() not in image_urls:
                image_urls.append(u.strip())

    product_group_id = coalesce_first(
        canonical.get("product_group_id"),
        *[p.get("product_group_id") for p in products],
    )

    primary_merchant_id = None
    for p in products:
        if p.get("group_is_primary"):
            primary_merchant_id = p.get("merchant_id")
            break
    if not primary_merchant_id:
        primary_merchant_id = canonical.get("merchant_id")

    merchant_url_by_id: Dict[str, Optional[str]] = {}
    for p in products:
        mid = p.get("merchant_id")
        if mid and mid not in merchant_url_by_id:
            merchant_url_by_id[mid] = p.get("canonical_url")

    currency, price_min, price_max, offer_count, top_offers = aggregate_offers(
        offers, primary_merchant_id, merchant_url_by_id, seller_trust_by_id
    )
    market_prices = (
        build_market_prices(offers, primary_merchant_id, merchant_url_by_id, seller_trust_by_id)
        if market_prices_enabled()
        else None
    )

    variants_capped, variants_count = aggregate_variants(
        skus, canonical.get("source_product_id")
    )
    gtin13 = pick_gtin13(skus)

    sig = canonical.get("pivota_signature_id")
    pdp_url = f"{PDP_URL_PREFIX}{sig}" if sig else None
    breadcrumb = build_breadcrumb(canonical, pdp_url)
    taxonomy_tags = build_taxonomy_tags(canonical)
    category_path = coalesce_first(canonical.get("category"), canonical.get("product_type"))

    fashion = coalesce_fashion_fields(products, external_seed)

    row = {
        "content_key": content_key,
        "pivota_signature_id": sig,
        "product_group_id": product_group_id,
        "brand": canonical.get("brand"),
        "title": title,
        "description": (description[:5000] if isinstance(description, str) else None),
        # Brand-attested rich content (E2 bridge) — key selling points + how-to-use.
        "bullet_points": (enrichment or {}).get("bullet_points"),
        "usage_scenarios": (enrichment or {}).get("usage_scenarios"),
        # Agent-decision-grade evidence (provenance-backed claims + required
        # disclaimers) — highest-precedence across the content_key cluster.
        # Floor-safety: a category-mandatory disclaimer (the FDA/DSHEA supplement
        # statement) rides along even when the merchant authored none, so the
        # direct PDP route serves it at parity with the search path.
        "evidence_profile": (evidence or {}).get("evidence_profile"),
        "required_disclaimers": ensure_category_disclaimers(
            (evidence or {}).get("required_disclaimers"),
            canonical.get("category_kind"),
        ),
        "image_url": image_url,
        "image_urls": image_urls or None,
        "currency": currency,
        "price_min": price_min,
        "price_max": price_max,
        "offer_count": offer_count,
        "offers": top_offers or None,
        "variants": variants_capped or None,
        "variants_count": variants_count,
        "gtin13": gtin13,
        "category_path": category_path,
        "taxonomy_tags": taxonomy_tags,
        "breadcrumb": breadcrumb,
        "pdp_lifecycle_stage": canonical.get("pdp_lifecycle_stage"),
        "sync_status": canonical.get("sync_status"),
        "primary_merchant_id": primary_merchant_id,
        "material": fashion["material"],
        "material_source": fashion["material_source"],
        "material_confidence": fashion["material_confidence"],
        "care": fashion["care"],
        "care_source": fashion["care_source"],
        "care_confidence": fashion["care_confidence"],
        "size_guide": fashion["size_guide"],
        "size_guide_source": fashion["size_guide_source"],
        "size_guide_confidence": fashion["size_guide_confidence"],
        # Review signal surfaced verbatim from the canonical (mig 186) — the
        # decision-intelligence lane consumes rating_value / rating_count. Null
        # when the canonical has no captured rating.
        "rating_value": canonical.get("rating_value"),
        "rating_count": canonical.get("rating_count"),
        "refresh_source": refresh_source,
    }
    # Only while the write flag is on: the key's presence is what selects the
    # upsert that names the column (upsert_sql_for_row).
    if market_prices is not None:
        row["market_prices"] = market_prices
    return row


UPSERT_SQL = """
    INSERT INTO agent_pdp_view (
      content_key, pivota_signature_id, product_group_id,
      brand, title, description, bullet_points, usage_scenarios,
      evidence_profile, required_disclaimers,
      image_url, image_urls,
      currency, price_min, price_max, offer_count, offers,
      variants, variants_count, gtin13,
      category_path, taxonomy_tags, breadcrumb,
      pdp_lifecycle_stage, sync_status, primary_merchant_id,
      material, material_source, material_confidence,
      care, care_source, care_confidence,
      size_guide, size_guide_source, size_guide_confidence,
      rating_value, rating_count,
      refreshed_at, refresh_source, refreshed_by_proposal_id
    ) VALUES (
      :content_key, :pivota_signature_id, :product_group_id,
      :brand, :title, :description,
      CAST(:bullet_points AS jsonb), CAST(:usage_scenarios AS jsonb),
      CAST(:evidence_profile AS jsonb), CAST(:required_disclaimers AS jsonb),
      :image_url, CAST(:image_urls AS jsonb),
      :currency, :price_min, :price_max, :offer_count, CAST(:offers AS jsonb),
      CAST(:variants AS jsonb), :variants_count, :gtin13,
      :category_path, CAST(:taxonomy_tags AS jsonb), CAST(:breadcrumb AS jsonb),
      :pdp_lifecycle_stage, :sync_status, :primary_merchant_id,
      :material, :material_source, :material_confidence,
      :care, :care_source, :care_confidence,
      CAST(:size_guide AS jsonb), :size_guide_source, :size_guide_confidence,
      :rating_value, :rating_count,
      NOW(), :refresh_source, :refreshed_by_proposal_id
    )
    ON CONFLICT (content_key) DO UPDATE SET
      pivota_signature_id = EXCLUDED.pivota_signature_id,
      product_group_id = EXCLUDED.product_group_id,
      brand = EXCLUDED.brand,
      -- Overlay-derived columns. When the overlay READ failed (as opposed to
      -- succeeding and finding nothing) the assembled row carries no values for
      -- these, so assigning from EXCLUDED would erase what is already published.
      -- Keep the current value in that case; a genuine removal still propagates,
      -- because a successful fetch that found nothing leaves the flag false.
      -- The CASTs are required: an untyped bind compared inside CASE is one of
      -- the IndeterminateDatatype shapes the prepare gate rejects.
      title = CASE WHEN CAST(:preserve_enrichment AS boolean)
                   THEN agent_pdp_view.title ELSE EXCLUDED.title END,
      description = CASE WHEN CAST(:preserve_enrichment AS boolean)
                   THEN agent_pdp_view.description ELSE EXCLUDED.description END,
      bullet_points = CASE WHEN CAST(:preserve_enrichment AS boolean)
                   THEN agent_pdp_view.bullet_points ELSE EXCLUDED.bullet_points END,
      usage_scenarios = CASE WHEN CAST(:preserve_enrichment AS boolean)
                   THEN agent_pdp_view.usage_scenarios ELSE EXCLUDED.usage_scenarios END,
      evidence_profile = CASE WHEN CAST(:preserve_evidence AS boolean)
                   THEN agent_pdp_view.evidence_profile ELSE EXCLUDED.evidence_profile END,
      required_disclaimers = CASE WHEN CAST(:preserve_evidence AS boolean)
                   THEN agent_pdp_view.required_disclaimers
                   ELSE EXCLUDED.required_disclaimers END,
      image_url = EXCLUDED.image_url,
      image_urls = EXCLUDED.image_urls,
      currency = EXCLUDED.currency,
      price_min = EXCLUDED.price_min,
      price_max = EXCLUDED.price_max,
      offer_count = EXCLUDED.offer_count,
      offers = EXCLUDED.offers,
      variants = EXCLUDED.variants,
      variants_count = EXCLUDED.variants_count,
      gtin13 = EXCLUDED.gtin13,
      category_path = EXCLUDED.category_path,
      taxonomy_tags = EXCLUDED.taxonomy_tags,
      breadcrumb = EXCLUDED.breadcrumb,
      pdp_lifecycle_stage = EXCLUDED.pdp_lifecycle_stage,
      sync_status = EXCLUDED.sync_status,
      primary_merchant_id = EXCLUDED.primary_merchant_id,
      material = EXCLUDED.material,
      material_source = EXCLUDED.material_source,
      material_confidence = EXCLUDED.material_confidence,
      care = EXCLUDED.care,
      care_source = EXCLUDED.care_source,
      care_confidence = EXCLUDED.care_confidence,
      size_guide = EXCLUDED.size_guide,
      size_guide_source = EXCLUDED.size_guide_source,
      size_guide_confidence = EXCLUDED.size_guide_confidence,
      rating_value = EXCLUDED.rating_value,
      rating_count = EXCLUDED.rating_count,
      refreshed_at = NOW(),
      refresh_source = EXCLUDED.refresh_source,
      -- STRAIGHT ASSIGNMENT, deliberately. An earlier revision COALESCEd this,
      -- on the theory that an anonymous rebuild should not erase the seed
      -- writer's attribution. That was wrong: migration 085 defines this column
      -- as "link to the proposal that TRIGGERED THE REFRESH", paired with
      -- refresh_source, "free-form provenance for refreshes that did NOT come
      -- from a proposal". The two describe THIS refresh, mutually exclusively —
      -- so clearing it when the current refresh had no proposal is the
      -- contract, not a bug. fashion_field_authoring used to set it to None
      -- explicitly for exactly that reason.
      --
      -- COALESCE also made a wrong value permanent on a field
      -- routes/agent_pdp_v1.py serves publicly: nothing anywhere performs a
      -- non-NULL -> NULL transition, so a merchant's hand edit would be
      -- attributed to an unrelated (possibly rejected) proposal forever.
      refreshed_by_proposal_id = EXCLUDED.refreshed_by_proposal_id
"""


def _upsert_sql_with_market_prices(base: str) -> str:
    """UPSERT_SQL plus market_prices and its stamp (migration 263), derived from it
    so the two can never drift in any other column. The stamp is the statement's
    own NOW(), the value refreshed_at gets in the same row write."""
    insert_anchor = "      rating_value, rating_count,\n      refreshed_at,"
    values_anchor = "      :rating_value, :rating_count,\n      NOW(),"
    update_anchor = "      rating_count = EXCLUDED.rating_count,\n"
    for anchor in (insert_anchor, values_anchor, update_anchor):
        if base.count(anchor) != 1:
            raise RuntimeError("UPSERT_SQL changed shape; update _upsert_sql_with_market_prices")
    return (
        base.replace(
            insert_anchor,
            "      rating_value, rating_count, market_prices, market_prices_refreshed_at,\n      refreshed_at,",
        )
        .replace(
            values_anchor,
            "      :rating_value, :rating_count, CAST(:market_prices AS jsonb), NOW(),\n      NOW(),",
        )
        .replace(
            update_anchor,
            update_anchor
            + "      market_prices = EXCLUDED.market_prices,\n"
            + "      market_prices_refreshed_at = EXCLUDED.market_prices_refreshed_at,\n",
        )
    )


UPSERT_SQL_WITH_MARKET_PRICES = _upsert_sql_with_market_prices(UPSERT_SQL)


def upsert_sql_for_row(row: Dict[str, Any]) -> str:
    """The upsert for an assembled row: the market_prices variant only when the
    row carries the key (AGENT_PDP_VIEW_MARKET_PRICES on), else UPSERT_SQL byte
    for byte -- which never names the column, so it runs before migration 263."""
    return UPSERT_SQL_WITH_MARKET_PRICES if "market_prices" in row else UPSERT_SQL


async def execute_agent_pdp_view_upsert(db: Any, row: Dict[str, Any]) -> None:
    """Write an assembled row. The summary is written only when the columns exist;
    otherwise -- the heal not run yet -- the row is written exactly as with the
    flag off, so the legacy prices keep refreshing instead of every refresh
    failing on the missing column."""
    if "market_prices" in row and not await agent_pdp_view_has_market_prices(db):
        row = {k: v for k, v in row.items() if k != "market_prices"}
    try:
        await db.execute(upsert_sql_for_row(row), row_to_upsert_params(row))
    except Exception as exc:
        if "market_prices" not in row or not is_missing_market_prices_column_error(exc):
            raise
        # The probe said present and the write says otherwise (dropped after the probe).
        mark_market_prices_columns_missing()
        legacy = {k: v for k, v in row.items() if k != "market_prices"}
        await db.execute(UPSERT_SQL, row_to_upsert_params(legacy))


def to_jsonb(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, default=str)


def row_to_upsert_params(row: Dict[str, Any]) -> Dict[str, Any]:
    """Project assemble_row() output to the bind-parameter dict shape
    UPSERT_SQL expects (JSONB columns → JSON-encoded strings)."""
    params = dict(row)
    params["image_urls"] = to_jsonb(row.get("image_urls"))
    params["offers"] = to_jsonb(row.get("offers"))
    params["variants"] = to_jsonb(row.get("variants"))
    params["taxonomy_tags"] = to_jsonb(row.get("taxonomy_tags"))
    params["breadcrumb"] = to_jsonb(row.get("breadcrumb"))
    params["evidence_profile"] = to_jsonb(row.get("evidence_profile"))
    params["required_disclaimers"] = to_jsonb(row.get("required_disclaimers"))
    params["bullet_points"] = to_jsonb(row.get("bullet_points"))
    params["usage_scenarios"] = to_jsonb(row.get("usage_scenarios"))
    # size_guide on catalog_products is already a JSONB value (stored as
    # dict by the LLM extractor / authoring path). coalesce_fashion_fields
    # passes it through verbatim. Re-encode for the SQLAlchemy bind. Other
    # fashion fields are plain text + float and pass through unchanged.
    params["size_guide"] = to_jsonb(row.get("size_guide"))
    if "market_prices" in row:
        # Always a version-1 object, never NULL: NULL means "not computed yet".
        mp = row.get("market_prices")
        params["market_prices"] = to_jsonb(
            mp if mp is not None else {"version": MARKET_PRICES_VERSION, "currencies": {}}
        )
    params["refreshed_by_proposal_id"] = row.get("refreshed_by_proposal_id")
    # Default FALSE, so every existing caller that builds a row without going
    # through build_agent_pdp_view_row keeps the previous overwrite semantics
    # rather than silently preserving stale overlays.
    params["preserve_enrichment"] = bool(row.get("preserve_enrichment"))
    params["preserve_evidence"] = bool(row.get("preserve_evidence"))
    return params


class _FetchFailed:
    """Sentinel: the overlay fetch ITSELF failed, as opposed to succeeding and
    finding nothing.

    Without this distinction both outcomes arrive as None, `assemble_row` builds
    a row with the overlay columns empty, and UPSERT_SQL — which assigns every
    column from EXCLUDED — writes those empties over a row that was serving real
    curated copy. A transient `get_enrichment` hiccup during one reconciler pass
    therefore strips the overlay permanently: the write stamps refreshed_at=NOW(),
    and the reconciler only re-selects keys whose truth timestamps moved PAST
    refreshed_at, so the repaired-by-the-next-sweep assumption does not hold. The
    row silently stops serving its enrichment until unrelated catalog truth
    changes.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<overlay-fetch-failed>"


FETCH_FAILED = _FetchFailed()


async def _fetch_member_overlays(
    products: List[Dict[str, Any]],
) -> Tuple[Dict[Tuple[str, str, str], Dict[str, Any]], bool]:
    """Every cluster member's product_enrichment overlay, keyed by the catalog identity triple
    (merchant_id, platform, source_product_id), geo 'default' -> (overlays, any_fetch_failed).
    Best-effort: a failed read is reported, never raised."""
    from db.product_enrichment import get_enrichments_for_products

    # ONE QUERY PER MERCHANT, not one per cluster member.
    #
    # This used to call get_enrichment() per product. Measured prod cluster sizes
    # are median 2 / p99 14 / max 45, and the largest are all merchant_id
    # 'external_seed' — i.e. exactly the cohort refresh_agent_pdp_view_for_seed
    # fires on. Since get_enrichment ALSO awaits ensure_product_enrichment_table()
    # on every call, the per-member loop multiplied both the round trips and the
    # DDL attempts by N; on a failing-DDL role the ensure never becomes sticky, so
    # a 43-member cluster cost 93 statements per rebuild, all serialized through a
    # process-global asyncio.Lock. That is now on a live merchant request path,
    # which is what makes it worth fixing rather than tolerating.
    #
    # get_enrichments_for_products is merchant-scoped, so group first: a cluster
    # can legitimately span merchants, and the single-merchant clusters that
    # dominate the tail collapse to exactly one query.
    by_merchant: Dict[str, List[Tuple[str, str]]] = {}
    for product in products:
        merchant_id = product.get("merchant_id")
        platform = product.get("platform")
        source_product_id = product.get("source_product_id")
        if not (merchant_id and platform and source_product_id):
            continue
        by_merchant.setdefault(str(merchant_id), []).append(
            (str(platform), str(source_product_id))
        )

    overlays: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    any_fetch_failed = False
    for merchant_id, keys in by_merchant.items():
        try:
            fetched = await get_enrichments_for_products(
                merchant_id, product_keys=keys
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "agent_pdp_view enrichment overlay fetch failed (best-effort): %s",
                str(exc)[:200],
            )
            any_fetch_failed = True
            continue
        for (platform, source_product_id), overlay in (fetched or {}).items():
            overlays[(merchant_id, platform, source_product_id)] = overlay
    return overlays, any_fetch_failed


def _overlay_for(product: Dict[str, Any], overlays: Dict[Tuple[str, str, str], Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    ident = (product.get("merchant_id"), product.get("platform"), product.get("source_product_id"))
    if not all(ident):
        return None
    return overlays.get((str(ident[0]), str(ident[1]), str(ident[2])))


def _brand_attested_overlay(
    products: List[Dict[str, Any]], overlays: Dict[Tuple[str, str, str], Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """The FIRST brand-attested overlay in cluster order, or None. It serves whichever row wins."""
    for product in products:
        overlay = _overlay_for(product, overlays)
        if overlay and overlay.get("updated_by_employee_id") == "brand_attestation":
            return overlay
    return None


def annotate_served_copy(
    products: List[Dict[str, Any]],
    overlays: Dict[Tuple[str, str, str], Dict[str, Any]],
    any_fetch_failed: bool,
) -> None:
    """Record on each row the title and description it would SERVE if it won the pick.

    `_passes_serving_content_bar` must judge what the gate will read -- agent_pdp_view's
    description, which assemble_row takes as coalesce(overlay description_markdown, the winner's
    description, ...) -- not the row's raw column: an executor overlay (canonical_pdp_enrichment
    writes one onto rows under 200 chars) makes a thin row serve fine, and demoting it would swap
    800 chars of overlay for a sibling's 60 (review #2423). The overlay that applies to a row as
    winner is the cluster's brand-attested overlay if there is one, else the row's own; the title
    follows assemble_row the same way (title_override first).

    DELIBERATELY NOT THE SEED. assemble_row falls back to the seed's copy and image, but which seed
    a rebuild holds depends on its caller (the seed writer passes the seed that fired it, the
    reconciler the cluster's newest), and a pick that read it would flip the served signature
    between two refreshes of unchanged rows (review #2423). Rows and their overlays are the same
    for every caller.

    NOR THE BRAND SECTIONS compose_brand_section_description appends from the winner's own seed.
    Those are per row, so they would not flip between callers; they are left out on purpose. The
    bar ranks rows by the copy each would serve without them, so a thin row never outranks a
    sibling that passes because of its page sections -- the sections only lengthen whichever row
    already won. The gate reads the composed description, so the one place this differs is a winner
    the bar judged thin that serves once its sections are appended (1 of the 353 keys the sections
    reached on the 2026-09-29 replay was blocked short_description).

    A FAILED overlay read judges every row as if it had no overlay -- the answer a successful read
    gives almost every key -- rather than waving thin rows through: the rebuild then preserves the
    published title/description, and a pick that differed from the successful read's would write
    another row's signature and image under them (review #2423). The read itself (partial or not)
    still rides on the rows for _fetch_enrichment_for_canonical, which reports FETCH_FAILED as before.
    (Where an overlay DECIDES the pick and one merchant's read fails, the preserved copy can still
    sit under the no-overlay winner's signature. Measured 2026-09-28: 0 of 2,896 multi-row
    content_keys pick differently with and without their overlays; 22 have any overlay.)
    """
    judged = {} if any_fetch_failed else overlays
    attested = _brand_attested_overlay(products, judged)
    for product in products:
        overlay = attested or _overlay_for(product, judged) or {}
        product[_SERVED_COPY] = {
            # the read itself, so the enrichment pick in the same rebuild reuses it
            "own_overlay": _overlay_for(product, overlays),
            "reads_failed": bool(any_fetch_failed),
            "title": coalesce_first(overlay.get("title_override"), product.get("title")),
            "description": coalesce_first(overlay.get("description_markdown"), product.get("description")),
        }


async def load_served_copy(products: List[Dict[str, Any]]) -> None:
    """Fetch the members' overlays once and annotate the rows (annotate_served_copy). The read
    rides on the rows, so _fetch_enrichment_for_canonical in the same rebuild does not repeat it."""
    overlays, any_fetch_failed = await _fetch_member_overlays(products)
    annotate_served_copy(products, overlays, any_fetch_failed)


def _overlays_from_annotations(
    products: List[Dict[str, Any]],
) -> Optional[Tuple[Dict[Tuple[str, str, str], Dict[str, Any]], bool]]:
    """The overlay read load_served_copy already made, recovered from the rows -- or None when
    any row is unannotated (then the caller reads them itself)."""
    if not products or any(not isinstance(p.get(_SERVED_COPY), dict) for p in products):
        return None
    overlays: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for product in products:
        own = product[_SERVED_COPY].get("own_overlay")
        ident = (product.get("merchant_id"), product.get("platform"), product.get("source_product_id"))
        if own is not None and all(ident):
            overlays[(str(ident[0]), str(ident[1]), str(ident[2]))] = own
    return overlays, any(bool(p[_SERVED_COPY].get("reads_failed")) for p in products)


async def _fetch_enrichment_for_canonical(
    products: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Fetch the product_enrichment overlay for the served PDP. Prefers a
    BRAND-ATTESTED overlay anywhere in the content_key cluster (the verified
    brand's own copy is authoritative for the entity, even if attest keyed it to
    a non-canonical member), else the canonical product's overlay. Keyed by the
    catalog identity triple (merchant_id, platform, source_product_id), geo
    'default'. Best-effort: any failure degrades to no overlay rather than
    breaking the serve-cache rebuild — agent_pdp_view is a cache, never truth.
    Rows annotated by load_served_copy carry the read already; it is reused, not repeated."""
    if not products:
        return None
    overlays, any_fetch_failed = (
        _overlays_from_annotations(products) or await _fetch_member_overlays(products)
    )

    # Finding B: walk the content_key cluster — a brand-attested overlay wins
    # (only the verified brand can create one, so it's safe + authoritative for
    # the entity, even if attest keyed it to a non-canonical seed beside a synced
    # product that wins pick_canonical), else fall back to the canonical's overlay.
    # Iteration order is unchanged, so the winner is identical to the per-member
    # version: the FIRST brand-attested overlay in cluster order.
    attested = _brand_attested_overlay(products, overlays)
    if attested is not None:
        return attested  # authoritative brand copy — wins immediately
    canonical_overlay = _overlay_for(pick_canonical(products), overlays)
    if canonical_overlay is None and any_fetch_failed:
        # We have no overlay AND at least one read errored, so "there is no
        # overlay" is not a conclusion we are entitled to draw. Say so, and let
        # the caller preserve whatever is already published.
        return FETCH_FAILED
    return canonical_overlay


async def refresh_agent_pdp_view_for_content_key(
    content_key: str,
    *,
    refresh_source: str,
    db: Any = None,
    external_seed_id: Optional[str] = None,
    refreshed_by_proposal_id: Optional[int] = None,
) -> bool:
    """Rebuild + upsert the denormalized agent_pdp_view row for a content_key.

    Canonical fetch→assemble→upsert orchestration (previously inlined in the
    backfill script, the seed writer, and fashion authoring). Pure of any
    serving-eligibility recompute — the caller owns that, so a caller that
    already recomputes right after (e.g. catalog_sync) does not double-fire.

    Returns True when a row was upserted, False when there was nothing to
    build (no catalog rows / too thin a row to be useful — no title). Raises
    on DB errors so callers can decide isolation; the catalog_sync caller
    wraps this best-effort so a stale PDP cache never breaks ingest.
    """
    read_db = db or database
    row = await build_agent_pdp_view_row(
        content_key,
        refresh_source=refresh_source,
        db=read_db,
        external_seed_id=external_seed_id,
        refreshed_by_proposal_id=refreshed_by_proposal_id,
    )
    if row is None:
        return False
    await execute_agent_pdp_view_upsert(read_db, row)
    return True


async def build_agent_pdp_view_row(
    content_key: str,
    *,
    refresh_source: str,
    db: Any = None,
    external_seed_id: Optional[str] = None,
    refreshed_by_proposal_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Fetch every source row for a content_key and assemble the view row.

    The read half of refresh_agent_pdp_view_for_content_key, split out so a
    caller can PREVIEW the next row state without writing it. Returns None when
    there is nothing to build (no catalog rows, or too thin a row to be useful —
    no title).

    NOT side-effect-free, despite reading like it. It is free of side effects on
    agent_pdp_view — nothing here writes the view — but the overlay fetch reaches
    `db.product_enrichment.get_enrichment`, which calls
    `ensure_product_enrichment_table()` and issues CREATE TABLE / ADD COLUMN /
    CREATE INDEX. A module-global flag makes that once per process, but the first
    call takes brief ACCESS EXCLUSIVE locks, and under a read-only role the DDL
    fails, the flag is never set, and it is retried on EVERY subsequent call.
    `seller_trust_bulk` has the same shape. So a "dry run" pointed at production
    still executes DDL there; say so rather than let an operator infer otherwise.

    Assembling through this function rather than calling assemble_row directly
    is what keeps the evidence / enrichment / seller-trust overlays attached. A
    caller that skips it gets a row with those fields None, and UPSERT_SQL sets
    every column from EXCLUDED — so writing such a row DELETES the enrichment,
    evidence_profile and required_disclaimers already serving on it. That is not
    hypothetical: scripts/backfill_agent_pdp_view.py did exactly this.

    `external_seed_id` selects the seed used for content fallback. Default
    (None) takes the newest active seed attached anywhere in the cluster, which
    is right for a backfill holding only a content_key. The seed_data WRITER
    passes the triggering seed's id instead: it knows which seed_data just
    changed, and "newest active in the group" can pull description text from a
    stale, unrelated seed.

    `refreshed_by_proposal_id` records which proposal caused the rebuild. Only
    the seed writer knows one; everything else leaves it None, and the UPSERT
    COALESCEs so a later anonymous refresh does not erase the attribution.
    """
    read_db = db or database
    products = await fetch_products_for_key(content_key, db=read_db)
    if not products:
        return None
    product_keys = [p["product_key"] for p in products if p.get("product_key")]
    skus = await fetch_skus_for_keys(product_keys, db=read_db)
    offers = await fetch_offers_for_keys(product_keys, db=read_db)
    external_seed = (
        await fetch_external_seed_by_id(external_seed_id, db=read_db)
        if external_seed_id
        else await fetch_external_seed_for_keys(product_keys, db=read_db)
    )
    # Each row's OWN seed, for compose_brand_section_description. Best-effort: without it the
    # description is served exactly as before.
    try:
        own_seed_copy = await fetch_own_seed_copy_for_keys(product_keys, db=read_db)
    except Exception as exc:  # noqa: BLE001
        logger.warning("agent_pdp_view own-seed copy fetch failed (best-effort): %s", str(exc)[:200])
        own_seed_copy = {}
    for product in products:
        if product.get("product_key") in own_seed_copy:
            product[_OWN_SEED_COPY] = own_seed_copy[product["product_key"]]
    # BEFORE the first pick_canonical below (evidence_safe_product_keys picks too): every pick in
    # this rebuild must judge each row by what it would serve, overlay included, and all of them
    # must see the same annotated rows. The overlays read here are reused for the enrichment pick.
    await load_served_copy(products)
    # Identity-confidence gate: a no-GTIN content_key is deliberately non-unique
    # (brand+title), so a cluster can collide DISTINCT products. Scope the evidence
    # fetch to members safe to attribute to the served row, so we never bake
    # Brand A's substantiated claim onto Brand B (see evidence_safe_product_keys).
    evidence_keys = evidence_safe_product_keys(products, skus)
    try:
        evidence = await fetch_evidence_for_keys(evidence_keys, db=read_db)
    except Exception as exc:  # noqa: BLE001 — see _FetchFailed
        logger.warning(
            "agent_pdp_view evidence fetch failed; preserving published evidence: %s",
            str(exc)[:200],
        )
        evidence = FETCH_FAILED
    # E2 publish bridge: overlay the Pivota enrichment (generated/curated
    # description_markdown) so it reaches the served PDP AND the
    # serving-eligibility gate. Best-effort; None keeps the raw description.
    enrichment = await _fetch_enrichment_for_canonical(products)
    # The storefront repetition check for compose_brand_section_description, on the winner only and
    # only when it has a line to judge. A failed read leaves _SHARED_SECTION_LINES unset, and then
    # nothing is appended: the check is what keeps store copy out, so it fails closed.
    if enrichment is not FETCH_FAILED:
        winner = pick_canonical(products)
        candidate_lines = brand_section_candidate_lines(winner, enrichment)
        from services.offer_seller_identity import normalize_host

        seed_host = normalize_host((winner.get(_OWN_SEED_COPY) or {}).get("domain")) or ""
        if candidate_lines and seed_host:  # no domain: nothing to count against, nothing appended
            try:
                winner[_SHARED_SECTION_LINES] = await fetch_shared_section_lines(
                    seed_host,
                    candidate_lines,
                    exclude_product_keys=product_keys,
                    own_base_title=_product_base_title(winner.get("title")),
                    db=read_db,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "agent_pdp_view shared section line check failed; no sections appended: %s",
                    str(exc)[:200],
                )
    # W8: attach the outcome-derived seller-trust signal to each offer's merchant.
    # Best-effort — a trust-store hiccup must never block the served PDP, and a
    # merchant with no transacted outcomes simply carries no trust key.
    seller_trust_by_id: Dict[str, Dict[str, Any]] = {}
    try:
        from services.outcome_aggregation_service import seller_trust_bulk

        seller_trust_by_id = await seller_trust_bulk(
            [o.get("merchant_id") for o in offers if o.get("merchant_id")]
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "agent_pdp_view seller_trust fetch failed (best-effort): %s",
            str(exc)[:200],
        )

    # Resolve the tri-state. A failed fetch contributes NO values and instead
    # tells the write to keep whatever is already published for those columns;
    # a successful fetch that found nothing writes empties as before, so a
    # genuine removal (an employee deleting enrichment) still propagates.
    preserve_enrichment = enrichment is FETCH_FAILED
    preserve_evidence = evidence is FETCH_FAILED
    row = assemble_row(
        content_key=content_key,
        products=products,
        skus=skus,
        offers=offers,
        external_seed=external_seed,
        evidence=None if preserve_evidence else evidence,
        refresh_source=refresh_source,
        enrichment=None if preserve_enrichment else enrichment,
        seller_trust_by_id=seller_trust_by_id,
    )
    if row is not None:
        row["preserve_enrichment"] = preserve_enrichment
        row["preserve_evidence"] = preserve_evidence
        row["refreshed_by_proposal_id"] = refreshed_by_proposal_id
    return row


# ---------------------------------------------------------------------
# Orphan reaper — agent_pdp_view rows whose content_key vanished
# ---------------------------------------------------------------------
#
# agent_pdp_view is keyed by content_key and kept fresh with UPSERT-on-content_key.
# But content_key = make_content_key(brand, title, gtin) is derived from MUTABLE
# fields: when a merchant re-syncs a product whose title or barcode changed, the
# catalog_products row is re-keyed to the NEW content_key in place — and nothing
# ever deletes the OLD content_key's agent_pdp_view row. It becomes an orphan:
# unreachable by content_key, yet it keeps its pivota_signature_id. Because sig is
# merchant-scoped and now belongs to the row under the NEW content_key, the unique
# sig index blocks the live row from materializing, AND a /products/sig_* lookup
# can resolve to the DEAD orphan instead of the live product. Reaping orphans fixes
# both: it clears the latent mis-serve and frees the sig for the live row.
#
# "Orphaned" is defined conservatively as: an agent_pdp_view row whose content_key
# has NO catalog_products row referencing it. That NOT-EXISTS guard means a
# content_key still shared by another merchant/path (the legitimate multi-seller
# case) is never reaped.

_DELETE_ORPHAN_IF_UNREFERENCED_SQL = """
    DELETE FROM agent_pdp_view av
    WHERE av.content_key = :content_key
      AND NOT EXISTS (
        SELECT 1 FROM catalog_products cp WHERE cp.content_key = :content_key
      )
"""


async def delete_agent_pdp_view_if_orphaned(content_key: str, *, db: Any = None) -> bool:
    """Delete the agent_pdp_view row for `content_key` IFF it is orphaned — no
    catalog_products row references it any more. No-op when the content_key is still
    live (shared by another merchant/path) or has no view row. Safe to call on every
    re-key; the DELETE re-checks NOT EXISTS so it can never remove a live row.

    Returns True when a row was deleted, False otherwise.
    """
    if not content_key:
        return False
    read_db = db or database
    was_orphan = await read_db.fetch_val(
        """
        SELECT EXISTS (SELECT 1 FROM agent_pdp_view WHERE content_key = :ck)
           AND NOT EXISTS (SELECT 1 FROM catalog_products WHERE content_key = :ck)
        """,
        {"ck": content_key},
    )
    if not was_orphan:
        return False
    await read_db.execute(_DELETE_ORPHAN_IF_UNREFERENCED_SQL, {"content_key": content_key})
    logger.info(
        {"event": "agent_pdp_view_orphan_reaped", "content_key": content_key}
    )
    return True


async def reap_orphaned_agent_pdp_view_rows(
    *,
    db: Any = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Sweep + delete ALL orphaned agent_pdp_view rows (content_key absent from
    catalog_products). This is the catch-all for every re-key site — not just
    catalog_sync — and the belt-and-suspenders backstop for the inline reap.

    Deletes regardless of whether the orphan still carries an evidence_profile: an
    orphan is unreachable by content_key and its evidence cannot be regenerated for
    a content_key nothing resolves to (the live product under the new content_key
    keeps its own evidence). `with_evidence` is surfaced for visibility since a
    non-zero value is unusual and worth noticing.

    Returns {"orphans", "with_evidence", "deleted", "sample"}. When dry_run, reports
    without deleting.
    """
    read_db = db or database
    limit_clause = " LIMIT :limit" if limit and int(limit) > 0 else ""
    params = {"limit": int(limit)} if limit and int(limit) > 0 else {}
    rows = await read_db.fetch_all(
        """
        SELECT av.content_key, av.pivota_signature_id, av.title,
               av.refresh_source, (av.evidence_profile IS NOT NULL) AS has_evidence
        FROM agent_pdp_view av
        WHERE NOT EXISTS (
            SELECT 1 FROM catalog_products cp WHERE cp.content_key = av.content_key
        )
        ORDER BY av.refreshed_at ASC
        """ + limit_clause,
        params,
    )
    orphans = [dict(r) for r in rows]
    with_evidence = sum(1 for r in orphans if r.get("has_evidence"))
    deleted = 0
    if not dry_run:
        # Per-row guarded delete (re-checks NOT EXISTS). Orphan counts are small and
        # this reuses the single-row invariant, so no fragile array binding.
        for r in orphans:
            await read_db.execute(
                _DELETE_ORPHAN_IF_UNREFERENCED_SQL, {"content_key": r["content_key"]}
            )
            deleted += 1
    if orphans:
        logger.info({
            "event": "agent_pdp_view_orphan_sweep",
            "orphans": len(orphans),
            "with_evidence": with_evidence,
            "deleted": deleted,
            "dry_run": dry_run,
        })
    return {
        "orphans": len(orphans),
        "with_evidence": with_evidence,
        "deleted": deleted,
        "sample": orphans[:10],
    }


def _propagate_enrichment_on_write_enabled() -> bool:
    """Canary flag for write-triggered enrichment propagation (B①).

    Default OFF. When off, an enrichment write does NOT immediately rebuild the
    served agent_pdp_view — the overlay still reaches the view on the next
    unrelated re-assembly (catalog sync, graduation, etc.), i.e. today's
    behavior. When on, an enrichment write propagates straight to the served PDP.
    """
    return (os.getenv("SERVE_PDP_ENRICHMENT_ON_WRITE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


# Distinct (reason, merchant_id, platform) combinations already warned about in
# this process. See _warn_unresolvable_once.
_UNRESOLVABLE_WARNED: set = set()
_UNRESOLVABLE_WARN_CAP = 512


def _warn_unresolvable_once(
    reason: str,
    merchant_id: str,
    platform: str,
    platform_product_id: str,
) -> None:
    """Warn at most once per (reason, merchant, platform) per process.

    ONE LINE PER WRITE WOULD SATURATE THE ALERT ON DAY ONE, defeating the point.
    The bridge is reached from paths keyed on the products_cache identity space —
    jobs/product_enrichment_worker.py walks every non-expired cached row for a
    merchant, and routes/merchant_products.py's bulk endpoint allows up to 1000 —
    while it resolves against catalog_products. A cached product that was never
    catalog-synced is therefore unresolvable as a matter of course, not as drift,
    and prod already carries 248 such rows. Per-write WARNING would emit hundreds
    of lines on an ordinary worker pass, be muted within a day, and hide the next
    real drift exactly as the silence did.

    Deduping on (reason, merchant, platform) bounds the volume while keeping the
    discriminating signal: a routine pass over one merchant's stale cache
    collapses to ONE line, whereas an id-space migration moving products out from
    under many merchants still produces one line per merchant. The SHAPE of the
    burst separates them, which the raw count never could.

    JSON, not a bare dict. This app configures no root log handler, so records
    fall through to `logging.lastResort` and a dict `msg` renders as a
    single-quoted Python repr that no JSON pipeline can parse a field out of.
    middleware/structured_logging.py:118 is the working precedent.
    """
    key = (reason, merchant_id, platform)
    if key in _UNRESOLVABLE_WARNED:
        return
    if len(_UNRESOLVABLE_WARNED) >= _UNRESOLVABLE_WARN_CAP:
        return  # 512 distinct merchant/platform pairs is already ample signal
    _UNRESOLVABLE_WARNED.add(key)
    logger.warning(json.dumps({
        "event": "enrichment_write_pdp_refresh_unresolvable",
        "reason": reason,
        "merchant_id": merchant_id,
        "platform": platform,
        # The FIRST offending id only: this line stands for every write sharing
        # this (reason, merchant, platform), not just the one that emitted it.
        "example_platform_product_id": platform_product_id,
        "note": "further occurrences for this merchant/platform are suppressed",
    }))


async def refresh_agent_pdp_view_for_enrichment_write(
    merchant_id: Optional[str],
    platform: Optional[str],
    platform_product_id: Optional[str],
    *,
    db: Any = None,
) -> bool:
    """Best-effort: rebuild the served agent_pdp_view after an enrichment WRITE.

    The E2 publish bridge (assemble_row) already overlays product_enrichment into
    agent_pdp_view at assembly time — but the only writers that re-assembled on
    write were the merchant add/edit routes. The automated enrichment pipeline +
    the employee path wrote enrichment WITHOUT re-assembling, so generated/curated
    copy didn't reach the served PDP until some unrelated re-assembly. This closes
    that gap: resolve the source product's content_key and refresh.

    Flag-gated (SERVE_PDP_ENRICHMENT_ON_WRITE, default OFF) so it's a canary, and
    fully best-effort — it never raises into the enrichment writer.

    Returns True iff a view row was rebuilt.
    """
    if not _propagate_enrichment_on_write_enabled():
        return False
    if not (merchant_id and platform and platform_product_id):
        return False
    read_db = db or database
    try:
        # The enrichment domain calls this identifier `platform_product_id`
        # (it is the param name on upsert_enrichment / get_enrichment); on
        # catalog_products the SAME value is stored as `source_product_id`.
        # The column below MUST use the catalog spelling — see
        # _fetch_enrichment_for_canonical, which builds the identical triple.
        # Shipped as `platform_product_id`, which is not a column on this
        # table, so every call raised UndefinedColumn into the best-effort
        # except below and this bridge silently never propagated anything.
        # No `AND content_key IS NOT NULL` here, deliberately. The filter used to
        # be in this predicate, which collapsed two different situations into one
        # empty result: "no product under this identity" and "the product exists
        # but has not been content-keyed yet". Those want different fixes, and the
        # warnings below can only tell them apart if the row comes back either
        # way. Safe to widen: (merchant_id, platform, source_product_id) is UNIQUE
        # (idx_catalog_products_source_identity, migration 058), so at most one
        # row matches and dropping the filter cannot change WHICH row is picked.
        row = await read_db.fetch_one(
            "SELECT content_key FROM catalog_products "
            "WHERE merchant_id = :merchant_id "
            "  AND platform = :platform "
            "  AND source_product_id = :platform_product_id "
            "LIMIT 1",
            {
                "merchant_id": str(merchant_id),
                "platform": str(platform),
                "platform_product_id": str(platform_product_id),
            },
        )
        # WHY THESE TWO WARNINGS EXIST. Both branches below mean "enrichment was
        # just written against an identity the catalog cannot resolve", and both
        # used to return False in silence. That silence is how 248 of 360
        # product_enrichment rows (69%, measured 2026-08-11) drifted into being
        # unjoinable without anyone noticing: catalog_products was re-keyed by the
        # ADR-009 / A9-4 id-space migration and by delete-and-recreate on two
        # merchants, while product_enrichment kept the old spelling. Nothing
        # re-keys the overlay when identity moves, so the only signal available is
        # at write time — here.
        #
        # The two cases are logged separately because they call for different
        # fixes. NO ROW means no product exists under this triple at all —
        # identity drift, or enrichment written for a products_cache-only product
        # that was never catalog-synced (jobs/product_enrichment_worker.py
        # iterates products_cache and can still mint these). NULL CONTENT_KEY
        # means the product exists but has not been content-keyed yet, which is a
        # sequencing issue that resolves itself once the key is minted, and needs
        # no intervention.
        #
        # WARNING, not error: the enrichment write itself succeeded and the caller
        # is best-effort. Volume is one line per unresolvable write, so a burst
        # means a migration moved ids out from under the overlay — which is
        # exactly the thing worth paging on.
        if not row:
            _warn_unresolvable_once(
                "no_catalog_row_for_identity",
                str(merchant_id), str(platform), str(platform_product_id),
            )
            return False
        # .strip() is load-bearing, not tidiness. Without it a whitespace-only
        # content_key ('   ') is TRUTHY, skips both branches, and is handed to
        # refresh_agent_pdp_view_for_content_key — which assembles and UPSERTs an
        # agent_pdp_view row keyed on whitespace. An empty string took the branch
        # below and reported "null_content_key", a lie about the column that
        # sends an operator to `WHERE content_key IS NULL` to find nothing. The
        # repo's canonical unkeyed test is `IS NULL OR btrim(...) = ''`
        # (scripts/backfill_pg_singleton.py:154); this matches it, so NULL, ''
        # and '   ' all land in one honestly-named branch.
        content_key = str(dict(row).get("content_key") or "").strip()
        if not content_key:
            _warn_unresolvable_once(
                "catalog_row_has_no_content_key",
                str(merchant_id), str(platform), str(platform_product_id),
            )
            return False
        return await refresh_agent_pdp_view_for_content_key(
            content_key,
            refresh_source="enrichment_write",
            db=read_db,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort; never break the writer
        logger.warning(
            "enrichment-write PDP refresh failed (best-effort) for %s/%s/%s: %s",
            merchant_id,
            platform,
            platform_product_id,
            str(exc)[:200],
        )
        return False
