"""Curated-brand-list feed — the CLEAN primary source for catalog coverage.

Given a curated list of brand storefront domains, enumerate their products via
Shopify's PUBLIC `/products.json` and turn each into a Path-C *validated record*
(the `{pdp, offers}` shape `ingestion.ingest_validated_record` consumes). The
brand's own storefront is the authoritative PDP, so this bypasses Gemini URL
resolution entirely — it's deterministic, cheap, and carries the brand's own
title/price/image, variant **barcode (GTIN)**, and tags. The records then ingest
as depositable canonical anchors via the existing FK-order executor.

This is the "crawl the brand before they integrate" engine for the (very common)
Shopify-hosted D2C brand. Non-Shopify/failed or capped scans raise CrawlIncomplete
so callers cannot ingest a prefix as a completed catalog. PURE-ish: this module fetches public pages + builds records; the
caller runs `ingest_validated_jsonl` + `apply_ingest_plan` (gated).
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import contextvars
import html
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote, urljoin, urlsplit

import httpx

from services.catalog_identity import validated_source_gtin
from services import storefront_currency

from services.retailer_ingest.sitemap_crawler import _looks_like_inci_list
from services import crawl_politeness, shopify_edge_pacer
from services.variant_identity import MERCHANT_ISSUED, variant_id_provenance

logger = logging.getLogger("curated_brand_feed")


def _note_response(url: str, resp: Any) -> None:
    """Feed one storefront response back to the crawl gates -- the ONE place this module does it.

    crawl_politeness paces the next request to this host (unchanged) and, given the headers, forwards
    them to any installed IP-throttle breaker and puts their throttle diagnostics on a 429/503
    backoff line (#2473). The shared Shopify-edge pacer (#2474) learns the host that ANSWERED as
    Shopify-served from its response headers, so later requests to it share the crawl IP's budget.
    Both pacer calls are no-ops unless CRAWL_SHOPIFY_EDGE_PACER_ENABLED is on."""
    headers = getattr(resp, "headers", None)
    crawl_politeness.note_response(
        url, resp.status_code,
        retry_after=headers.get("retry-after") if headers is not None else None,
        headers=headers,
    )
    shopify_edge_pacer.learn_from_response(str(getattr(resp, "url", None) or url), headers)

_UA = "PivotaCommerceIndex/1.0 (+https://pivota.cc; catalog coverage)"
_PER_PAGE = 250  # Shopify max

# Attempts per /products.json page before a crawl gives up (CrawlIncomplete). Default 3 -- the value
# every lane has always used. A large retailer (holiholic.com, koolseoul.com: 5,000-9,000 products)
# that answers 429 once mid-crawl fails the whole run at 3, and the operator's retry starts again at
# page 1: MORE requests to the same store than waiting on the throttled page. A one-off crawl may
# raise it with CURATED_CRAWL_PAGE_ATTEMPTS; crawl_politeness still doubles its hold per consecutive
# block, so extra attempts are patience, not pressure. Capped; a malformed value refuses.
_PAGE_ATTEMPTS_DEFAULT = 3
_PAGE_ATTEMPTS_MAX = 8


def _page_attempts() -> int:
    raw = os.getenv("CURATED_CRAWL_PAGE_ATTEMPTS")
    if raw is None or not raw.strip():
        return _PAGE_ATTEMPTS_DEFAULT
    try:
        value = int(raw.strip())
    except ValueError:
        raise ValueError(f"CURATED_CRAWL_PAGE_ATTEMPTS must be an integer, got {raw!r}") from None
    if not 1 <= value <= _PAGE_ATTEMPTS_MAX:
        raise ValueError(f"CURATED_CRAWL_PAGE_ATTEMPTS must be 1..{_PAGE_ATTEMPTS_MAX}, got {value}")
    return value
# Lowest variant price (in the store's currency) that counts as a real offer. Across
# the four Meitu-US feeds measured 2026-09-05 (2,108 products) exactly one variant sat
# in (0, 1.00): the $0.01 stila promo described at the variant pick below. Nothing legitimate in a beauty D2C feed is
# priced under a dollar; a floor this low cannot drop a real product.
MIN_SELLABLE_PRICE = 1.0

# Gift-with-purchase / free-sample items a store lists with a NOMINAL price, so the $1 floor above keeps them
# (measured 2026-09-24: westman-atelier.com's $26 "Blush Stick" deluxe GWP mini, tagged `gwp`). Census
# 2026-09-24, 24,523 products across 54 US stores: these EXACT tag tokens (casefolded, whole tag) mark only
# gifts -- and a SUBSTRING match is not safe: paid products carry `solo_gwp_elta_pca`,
# `rationale_4_eye_cream_gwp`, `excludefromgwp:sitewide`. Tags that LOOK related but mark sellable products and
# must never be added here: yblocklist, searchanise_ignore, gorgias_do_not_recommend, brand promo, freesample
# (Sol de Janeiro $26 mists), sample (travel sprays), free-gifts / free_gifts (perfumania $24.95 sets),
# "sets not for sale" (sokoglam $199 routines). Measured cost of this rule on the census: 0 sellable dropped.
GIFT_WITH_PURCHASE_TAGS = frozenset({
    "gwp", "filter::type_gwp", "filter::type_sample", "_free_gift", "_free_gift_sample", "checkout-sample",
    "motivator_hidden_product", "gwp:brand", "gwp:sitewide", "subscription gwp", "gwp-choice",
    "gift with purchase",
})
# Titles that mark a paid-listed gift no tag marks: an EXACT "[FREE GIFT]" / "[FREE SAMPLE]" bracket, or an
# unbracketed "FREE GIFT" / "FREE SAMPLE" followed by a dash or colon ("FREE GIFT - SkinMedica ... Sample"),
# "... Gift with Purchase", "Loyalty Reward - ...". NOT a bare leading "Free" (ezenzia sells "Free Random
# Fragrance" at $19.99), NOT "[Free Gift Set]" -- a product SOLD WITH a gift (dodoskin "[Free Gift Set] beaund
# Nmode Pro + Booster Gel", $159). A title carrying "+" is such a bundle ("[Free Gift] 1+1 SAMJIWON ... + FREE
# medicube Mask", $79) and is always kept (_GIFT_BUNDLE).
_GIFT_WITH_PURCHASE_TITLE = re.compile(
    r"^\[\s*free\s+(?:gift|sample)s?\s*\]|^free\s+(?:gift|sample)s?\s*[-–—:]|\bgift\s+with\s+purchase\b"
    r"|^loyalty\s+reward\b", re.I)
_GIFT_BUNDLE = re.compile(r"\+")
# NOT the merchant product type: `GWP`-typed items include in-stock full-size products (elizabetharden.com
# PREVAGE set $169, night capsules $99; beautybrands $44.99 hair dryer) -- measured 2026-09-25. Known gap.


def gift_with_purchase_reason(product: Dict[str, Any]) -> Optional[str]:
    """Why this Shopify product is a gift/sample rather than something a buyer can order, or None."""
    raw = product.get("tags")
    tags = raw if isinstance(raw, list) else str(raw or "").split(",")
    tokens = {str(t).strip().casefold() for t in tags if str(t).strip()}
    hit = sorted(tokens & GIFT_WITH_PURCHASE_TAGS)
    if hit:
        return f"tag:{hit[0]}"
    title = str(product.get("title") or "").strip()
    if _GIFT_BUNDLE.search(title):
        return None  # sold WITH a gift, not a gift
    m = _GIFT_WITH_PURCHASE_TITLE.search(title)
    return f"title:{m.group(0).strip()}" if m else None

# The axis a variant varies on, named the way the shop names it. Shopify reports
# a product's axes in `options`, and `option1` is a value on the FIRST of them.
# A shop with no axis at all reports the placeholder "Title" / "Default Title",
# which names nothing.
# "Color", NOT "Shade", and the difference is not cosmetic. The renderer treats
# the two names ASYMMETRICALLY when its own keyword gate does not read the
# product as cosmetic: `shade|tone|hue|undertone` falls through to
# NON_DISPLAYABLE, while `color|colour` falls through to a working `color` axis
# (both still divert a volume-looking value to a volume axis first). Measured
# across the folded bases of the six cached brand feeds, this literal alone is
# the difference between 48 and 75 of 79 products rendering a selector — the
# axis we invent should be the one the consumer accepts.
_DEFAULT_SHADE_OPTION_NAME = "Color"
_PLACEHOLDER_OPTION_NAMES = {"title", "option", "variant", "selection", "default title"}
_SHADE_AXIS_NAMES = {"shade", "color", "colour", "tone", "hue"}


def _base_option_name(product: Dict[str, Any]) -> str:
    """The shop's own name for the product's FIRST option axis, "" if it names none."""
    # Shopify `/products.json` emits `options` as a list of dicts, and it is the
    # only writer that reaches here — the string-shaped branches this used to
    # carry were unfalsifiable by any input, so they are gone rather than left
    # as coverage nobody can earn.
    options = product.get("options")
    first = options[0] if isinstance(options, list) and options else None
    name = str(first.get("name") or "").strip() if isinstance(first, dict) else ""
    if name.lower() in _PLACEHOLDER_OPTION_NAMES:
        return ""
    return name


_SIZE_LIKE_VALUE = re.compile(
    r"""(?ix)
    ^\s*(?:
        [\d.,/]+\s*(?:ml|l|g|kg|mg|oz|fl\.?\s*oz|floz|lb|ct|count|pc|pcs|pack|x)\b
      | (?:x?\s*[\d.,]+\s*(?:ml|g|oz))
      | (?:travel|mini|deluxe|jumbo|full|full\s*size|trial|sample|refill)\s*(?:size)?
      | (?:small|medium|large|x-?large|xs|s|m|l|xl|xxl|one\s*size)
    )\s*$
    """
)


def _looks_like_a_size(value: str) -> bool:
    """A quantity, a pack, or a garment/format size — never a colour."""
    return bool(_SIZE_LIKE_VALUE.match(value or ""))


def _variant_option_name(variant: Dict[str, Any], base_option_name: str) -> str:
    """The axis THIS variant varies on — per variant, because a folded product's
    variant list is not homogeneous.

    `fold_shade_listings` appends variants taken from OTHER products (the
    per-shade listings) onto a base that keeps its own `options`. On a base whose
    real axis is Size — a foundation sold in 30ml and 50ml — naming every variant
    from `options[0]` published the shades as "Size: NC15". That is not just an
    ugly label: the renderer only demands a swatch when the axis reads as a
    shade, so a mislabelled shade also rendered without one.

    A folded-in variant is on the shade axis BY CONSTRUCTION, whatever the base
    calls its own. The base's own variants really are on the base's axis, so they
    keep it — and "" when the shop names no axis, because a guess would be a
    label the merchant never wrote.
    """
    # PRESENCE, not truthiness. The fold stamps this key with the handle it took
    # the variant from, and a shade row with an empty handle stores "" — which a
    # truthiness test reads as "not folded", handing that shade the base's own
    # axis and reproducing the mislabel this function exists to prevent.
    if FOLDED_FROM_KEY in variant:
        if base_option_name.strip().lower() in _SHADE_AXIS_NAMES:
            return base_option_name
        # The fold collapses listings that differ by a TITLE SUFFIX, and a suffix
        # is not always a shade: "Fix+ - 3.4 fl oz" and "Blot Powder - Medium"
        # fold exactly like "Retro Matte Lipstick - Ruby Woo". Inventing a colour
        # axis over a quantity publishes "Color: 3.4 fl oz". When the shop has not
        # named an axis we can trust and the value reads as a size, decline —
        # naming nothing is the honest answer, and by the product-level rule the
        # whole product then serves as the bare list it was before.
        if _looks_like_a_size(str(variant.get("option1") or variant.get("title") or "")):
            return ""
        return _DEFAULT_SHADE_OPTION_NAME
    return base_option_name


def _clean_domain(domain: str) -> str:
    d = str(domain or "").strip().lower()
    d = d.replace("https://", "").replace("http://", "").rstrip("/")
    return d.split("/")[0]


def _same_storefront_host(requested: str, actual: Optional[str]) -> bool:
    """Is `actual` (a redirect chain's final host) the SAME storefront we asked?

    Only the `www.` prefix is treated as noise. A SUBDOMAIN is not: `uk.brand.com` and
    `shop.brand.com` are separate Shopify stores with their own `shop.description`, their own
    catalogue and their own currency, which is precisely the confusion a host pin exists to
    refuse. Suffix matching (`endswith(host)`) would accept both, and would additionally accept
    `evilbrand.com` for `brand.com`.
    """
    a = str(actual or "").strip().lower().rstrip(".")
    r = str(requested or "").strip().lower().rstrip(".")
    if not a or not r:
        return False
    return a.removeprefix("www.") == r.removeprefix("www.")


_ISO_CURRENCY = re.compile(r"^[A-Z]{3}$")


async def fetch_shopify_shop_locale(
    domain: str,
    *,
    timeout_s: float = 10.0,
) -> Dict[str, Optional[str]]:
    """The storefront's own currency, via the module that already reads /meta.json.

    `/products.json` carries prices but NEVER the currency they are in, so every record this
    module built was currency-less and the ingest lane stamped USD on all of them. Measured
    2026-09-06: jsmbeauty.sg prices LIP-PRESSION Glowy Tint at 3000 minor = SGD 30.00, and
    ingesting it through that lane wrote USD 30.

    DELEGATES to `services.storefront_currency.fetch_storefront_meta` rather than fetching here.
    A first draft of this function was a second, uncached reader of the same endpoint -- which is
    why a THIRD gated fetch had to be registered in this file's crawl-politeness budget. That
    module already validates the currency, caches per domain for the process lifetime, and is the
    place this knowledge belongs.

    The politeness gate is preserved by injecting the fetch: `fetch_storefront_meta` takes a
    `fetch` seam precisely so a caller can supply its own transport, so the shared gate still sees
    every request this crawl lane makes against a merchant host.

    CURRENCY ONLY, and `country` is deliberately NOT returned. `storefront_currency`'s own
    docstring records why they are different axes ("a KR/HK exporter legitimately prices in USD"),
    and measurement made the asymmetry concrete: `external_product_seeds.market` is a HARD serving
    partition -- `external_seed_search` appends `market = :market` and every serving caller passes
    DEFAULT_EXTERNAL_SEED_MARKET="US" -- so a seed stamped with the storefront's country vanishes
    from US seed search. Returning the value at all invites that mistake again.

    NEGATIVE RESULTS ARE NOT CACHED ACROSS BRANDS. `fetch_storefront_meta` caches per domain for
    the PROCESS lifetime, negatives included, and its docstring says a long-lived caller should
    clear periodically. This lane is exactly that caller (`catalog_onboard_worker` drains a queue
    with a retry budget), and a single transient timeout would otherwise pin that brand to None ->
    USD for the whole process, silently defeating the retry AND the fix. So a miss is evicted.
    """
    host = _clean_domain(domain)
    if not host:
        return {"currency": None}
    # /meta.json is a Shopify storefront endpoint: pace it on the shared Shopify-edge budget from
    # the first request (a no-op with the pacer off).
    shopify_edge_pacer.mark_shopify_host(host)

    async def _gated_fetch(url: str) -> Optional[str]:
        headers = {"User-Agent": _UA, "Accept": "application/json"}
        timeout = httpx.Timeout(timeout_s, connect=5.0)
        try:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=timeout, headers=headers
            ) as client:
                await crawl_politeness.before_request(url, user_agent=_UA, max_wait=0)
                resp = await client.get(url)
                _note_response(url, resp)
                if not _same_storefront_host(host, getattr(getattr(resp, "url", None), "host", None)):
                    return None
                if resp.status_code != 200:
                    return None
                if "application/json" not in (resp.headers.get("content-type") or ""):
                    return None
                return resp.text
        except Exception:
            return None

    meta = await storefront_currency.fetch_storefront_meta(host, fetch=_gated_fetch)
    if not isinstance(meta, dict):
        # Evict the negative so the next brand (or a retry of this one) asks again.
        storefront_currency.clear_cache()
        return {"currency": None}
    cur = str(meta.get("currency") or "").strip().upper()
    return {"currency": cur if _ISO_CURRENCY.match(cur) else None}


def shop_storefront_identity(domain: str) -> Optional[Dict[str, Any]]:
    """WHICH store the crawl read, from the /meta.json `fetch_shopify_shop_locale` just fetched (the
    per-host cache: no second request). {name, myshopify_domain, currency, ships_to_countries}, or
    None when nothing was proven for this host.

    Recorded on the crawl report so a retailer_ingest run says which Shopify store it crawled
    (`us.frankbody.com` is `letsbefrankusa`, a separate store from `frankbody.com`'s `letsbefrank`)
    and where it ships; the lane's brand-official Tier B rule reads it. Evidence only: it never
    changes what the crawl accepts or refuses, and none of it is stamped on a row (see the
    `country` note above -- `ships_to_countries` is fulfilment reach, not a market)."""
    meta = storefront_currency.cached_meta(_clean_domain(domain))
    if not isinstance(meta, dict):
        return None
    return {k: meta.get(k) for k in ("name", "myshopify_domain", "currency", "ships_to_countries")}


class CrawlIncomplete(RuntimeError):
    """A bounded scan did not establish a complete catalog; never ingest its prefix.

    next_page is diagnostic retry position, not an ingestion checkpoint. Callers retry
    the whole read before writing, so no partial batch can become a successful job.

    `reason_code` names WHY when the cause is known (see classify_transport_failure).
    `status` is deliberately NOT derived from it: every consumer gates on
    `status == "complete"`, and an unreachable host is a failed crawl like any other.
    """
    def __init__(self, message: str, *, status: str, next_page: int,
                 scanned_products: int, selected_products: int,
                 reason_code: Optional[str] = None):
        super().__init__(message)
        self.status = status
        self.next_page = next_page
        self.scanned_products = scanned_products
        self.selected_products = selected_products
        self.reason_code = reason_code

    def as_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"status": self.status, "next_page": self.next_page,
                               "scanned_products": self.scanned_products,
                               "selected_products": self.selected_products, "reason": str(self)}
        # Additive: existing readers key on status/reason and must not have to know this.
        if self.reason_code:
            out["reason_code"] = self.reason_code
        return out


#: Where to look first. A transport failure does not prove whose fault it is, so the
#: classification says which side the evidence points at and nothing stronger.
_BLAME_NEXT_STEP = {
    "host": "verify the storefront from a vantage outside the crawl subnet before suspecting "
            "this crawler",
    "client": "suspect this client's egress or TLS configuration before the storefront",
    "unknown": "the failure happened before any HTTP response; verify reachability from outside "
               "the crawl subnet before suspecting this crawler",
}

#: (needle, reason_code, explanation, blame). First match wins, so specific cases precede
#: general ones. Needles are matched against "TypeName: message", lowercased.
#:
#: BLAME IS EVIDENCE, NOT A VERDICT. `SSLV3_ALERT_HANDSHAKE_FAILURE` is what an edge returns
#: for a hostname it does not serve — and also what a server sends on a cipher-suite mismatch
#: with a hardened client. `services/official_domain_liveness.py` already records this class
#: of failure as `unverifiable` rather than a confirmed negative, after 213/286 brand hosts
#: answered with WAF challenges; this table keeps that posture.
_TRANSPORT_SIGNATURES: Tuple[Tuple[str, str, str, str], ...] = (
    # Client-side first: these say our own egress or trust store, and blaming the site here
    # is the same misdirection this classifier exists to remove, pointed the other way.
    ("unable to get local issuer", "client_trust_store",
     "this client could not build a trust chain for the host's certificate, which the error "
     "attributes to the LOCAL issuer store (a stale CA bundle in the image, or interception)",
     "client"),
    ("self-signed certificate", "client_trust_store",
     "the presented certificate is self-signed, which an intercepting proxy also produces",
     "client"),
    ("network is unreachable", "client_egress_unreachable",
     "this client's network could not route to the host at all", "client"),
    ("no route to host", "client_egress_unreachable",
     "this client's network has no route to the host", "client"),
    # Host-side.
    ("sslv3_alert_handshake_failure", "host_tls_refused",
     "the host's TLS layer rejected the handshake before any HTTP request, which is what an "
     "edge returns for a hostname it has no certificate for — and also what a server sends "
     "when no cipher suite is shared",
     "host"),
    ("tlsv1_alert_protocol_version", "tls_version_mismatch",
     "the host and this client share no TLS protocol version", "unknown"),
    ("certificate_verify_failed", "tls_untrusted",
     "the host's certificate did not verify (expired or wrongly issued at the host, or a "
     "trust-store problem here)", "unknown"),
    ("tlsv1_alert", "host_tls_refused", "the host rejected the TLS handshake", "host"),
    ("[ssl:", "tls_error", "the TLS handshake failed", "unknown"),
    ("ssl/tls", "tls_error", "the TLS handshake failed", "unknown"),
    ("_ssl.c", "tls_error", "the TLS handshake failed", "unknown"),
    ("name or service not known", "host_dns_unresolved", "the hostname does not resolve", "host"),
    ("nodename nor servname", "host_dns_unresolved", "the hostname does not resolve", "host"),
    ("no address associated with hostname", "host_dns_unresolved",
     "the hostname resolves to no address", "host"),
    ("temporary failure in name resolution", "dns_failure",
     "the hostname could not be resolved right now, which a resolver problem here also "
     "produces", "unknown"),
    ("getaddrinfo", "dns_failure", "the hostname could not be resolved", "unknown"),
    ("connection refused", "host_connection_refused",
     "the host refused the TCP connection", "host"),
    ("connection reset", "connection_reset",
     "the connection was reset after it was established, which rate limiting also produces",
     "unknown"),
    ("server disconnected", "connection_reset",
     "the host closed an established connection without responding", "unknown"),
)

#: Failures that are OURS by construction — a malformed request, an unsupported URL scheme,
#: our proxy, our own connection pool. Classifying these as anything about the host would
#: send an operator to check a site that was never contacted.
_CLIENT_SIDE_TYPES = (
    httpx.LocalProtocolError,
    httpx.UnsupportedProtocol,
    httpx.ProxyError,
    httpx.PoolTimeout,
)


def classify_transport_failure(exc: BaseException) -> Optional[Dict[str, str]]:
    """{reason_code, explanation, blame} for a transport failure, else None.

    A raw `ConnectError: [SSL: SSLV3_ALERT_HANDSHAKE_FAILURE]` on page 1 reads as a bug in
    this crawler; it usually means the storefront did not answer. Not naming that cost a day
    of diagnosis on the 2026-09-15 A'PIEU canary. But the opposite error is just as costly:
    a proxy failure or a stale CA bundle blamed on the merchant sends an operator to check a
    site that is perfectly healthy. So this reports WHERE THE EVIDENCE POINTS (`blame`), never
    a verdict, and returns None for failures that are ours by construction — those keep the
    raw error, which is the right thing to read when the bug is here.

    This NEVER changes control flow: an unreachable host is still an incomplete crawl that
    must not ingest a partial prefix, and the retry budget is untouched — a site that is down
    now can answer on the next attempt.
    """
    if isinstance(exc, _CLIENT_SIDE_TYPES):
        return None
    if not isinstance(exc, httpx.TransportError):
        return None
    text = f"{type(exc).__name__}: {exc}".lower()
    for needle, code, explanation, blame in _TRANSPORT_SIGNATURES:
        if needle in text:
            return {"reason_code": code, "explanation": explanation, "blame": blame}
    if isinstance(exc, httpx.TimeoutException):
        return {"reason_code": "timeout",
                "explanation": "no response arrived within the request timeout",
                "blame": "unknown"}
    return {"reason_code": "transport_failure",
            "explanation": "the connection attempt failed before any HTTP response",
            "blame": "unknown"}


class ShopifyProductBatch(list):
    """List-compatible result; successful results always represent an exhausted feed -- of the
    store, or (scope "collection:<handle>") of one collection only, never claimed for the store."""
    def __init__(self, products: list, *, scanned_products: int, pages: int, scope: str = "store"):
        super().__init__(products)
        self.crawl_report = {"status": "complete", "scanned_products": scanned_products,
                             "selected_products": len(products), "pages": pages}
        if scope != "store":
            self.crawl_report["scope"] = scope


#: Shopify serves /products.json pages 1..100 only: page 101 answers HTTP 400 (measured 2026-09-24 on
#: beautycarebag.com, japanwithlovestore.com, marissacollections.com, shopcgx.com). A store with more
#: than 100 * 250 products cannot reach its empty page; crawl its brand collections instead.
SHOPIFY_MAX_PAGES = 100
_COLLECTION_HANDLE = re.compile(r"[a-z0-9][a-z0-9_-]{0,254}")


def valid_collection_handle(handle: Any) -> bool:
    return isinstance(handle, str) and bool(_COLLECTION_HANDLE.fullmatch(handle))


async def fetch_shopify_products(
    domain: str,
    *,
    max_products: int = 500,
    timeout_s: float = 15.0,
    only_vendors: Optional[Sequence[str]] = None,
    max_scan_products: Optional[int] = None,
    collection: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Enumerate to exhaustion, selecting vendors before the product budget.

    `collection` pages /collections/<handle>/products.json instead of the whole store: the batch is
    complete for THAT collection only (crawl_report.scope) -- a store's brand collection can hold other
    vendors (the vendor filter still applies) and can miss some of the brand's products.

    max_scan_products bounds the whole retailer scan, independently of the selected
    max_products budget. A cap or failed page raises CrawlIncomplete, never returns a
    misleading partial success. Exhaustion is an EMPTY page, never a short one, so every
    crawl ends with one lookahead request. (Still an inference: a mid-catalog page whose every
    slot is a hidden product would also come back empty. Not observed.) Transient errors get paced attempts per page: three, unless the
    process sets CURATED_CRAWL_PAGE_ATTEMPTS (see _page_attempts).
    """
    host = _clean_domain(domain)
    if not host:
        raise ValueError("a storefront domain is required")
    scan_limit = max_scan_products if max_scan_products is not None else max_products
    if max_products < 1 or scan_limit < 1:
        raise ValueError("product and scan budgets must be positive")
    if only_vendors is not None and not any(str(v or "").strip() for v in only_vendors):
        raise ValueError("only_vendors cannot contain only blank values")
    if collection is not None and not valid_collection_handle(collection):
        raise ValueError(f"not a Shopify collection handle: {collection!r}")
    listing = f"/collections/{collection}/products.json" if collection else "/products.json"
    scope = f"collection:{collection}" if collection else "store"
    out: List[Dict[str, Any]] = []
    scanned = 0
    page = 1
    seen_pages: set = set()
    # /products.json (and a collection's) is Shopify's storefront feed: this host is Shopify-served
    # by construction, so every page of the crawl -- the first included -- is paced on the crawl IP's
    # shared Shopify-edge budget (a no-op with CRAWL_SHOPIFY_EDGE_PACER_ENABLED off).
    # MARKED BEFORE THE ANSWER, on purpose: page 1 is where pacing matters under a throttle. The cost
    # is that a non-Shopify brand domain probed here (records_for_brand tries this first; such a
    # domain returns nothing) spends one or two requests of the shared budget and stays cached as
    # Shopify for this process. That can only slow its requests, never cause a 429 (review of #2477).
    shopify_edge_pacer.mark_shopify_host(host)
    timeout = httpx.Timeout(timeout_s, connect=5.0)
    headers = {"User-Agent": _UA, "Accept": "application/json"}
    attempts = _page_attempts()

    def incomplete(reason: str, status: str = "failed",
                   reason_code: Optional[str] = None) -> CrawlIncomplete:
        return CrawlIncomplete(f"{host}: page {page}: {reason}", status=status,
                               next_page=page, scanned_products=scanned,
                               selected_products=len(out), reason_code=reason_code)

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout, headers=headers) as client:
            while True:
                if page > SHOPIFY_MAX_PAGES:
                    raise incomplete(
                        f"Shopify serves at most {SHOPIFY_MAX_PAGES} pages of {listing}; this listing is "
                        "larger -- crawl the brand's collection (options.collections) instead", "capped")
                url = f"https://{host}{listing}?limit={_PER_PAGE}&page={page}"
                for attempt in range(attempts):
                    await crawl_politeness.before_request(url, user_agent=_UA, max_wait=0)
                    try:
                        resp = await client.get(url)
                    except (httpx.TimeoutException, httpx.TransportError):
                        if attempt == attempts - 1:
                            raise
                        await asyncio.sleep(0.5 * (2 ** attempt))
                        continue
                    _note_response(url, resp)
                    actual_host = getattr(getattr(resp, "url", None), "host", None)
                    if not _same_storefront_host(host, actual_host):
                        # A regional/sibling store can have a different catalog and
                        # currency. Never pair its prices with this host's locale.
                        raise incomplete(f"storefront host changed to {actual_host or '(unknown)'}")
                    if resp.status_code not in {429, 500, 502, 503, 504} or attempt == attempts - 1:
                        break
                    await asyncio.sleep(0.5 * (2 ** attempt))
                if resp.status_code != 200:
                    raise incomplete(f"HTTP {resp.status_code}")
                if "application/json" not in (resp.headers.get("content-type") or ""):
                    raise incomplete("storefront did not return JSON")
                body = resp.json()
                if not isinstance(body, dict) or not isinstance(body.get("products"), list):
                    raise incomplete("invalid products.json envelope")
                products = body["products"]
                if not all(isinstance(p, dict) for p in products):
                    raise incomplete("products.json contains a non-object product")
                if not products:
                    return ShopifyProductBatch(out, scanned_products=scanned, pages=page, scope=scope)
                fingerprint = json.dumps(products, sort_keys=True, default=str)
                if fingerprint in seen_pages:
                    raise incomplete("repeated page; pagination did not advance")
                seen_pages.add(fingerprint)
                if scanned + len(products) > scan_limit:
                    raise incomplete(f"scan budget {scan_limit} exhausted", "capped")
                scanned += len(products)
                selected = filter_products_by_vendor(products, only_vendors)
                if len(out) + len(selected) > max_products:
                    raise incomplete(f"selected-product budget {max_products} exhausted", "capped")
                out.extend(selected)
                # A short page is NOT the end of the catalog: only an empty page is. Shopify counts
                # products hidden from the storefront toward `limit`, so it serves short pages
                # mid-catalog -- measured 2026-09-24, bluemercury.com page 1 = 249 then page 2 = 250
                # (23 pages, 5,593 products). Stopping on the first short page reported "complete"
                # after 249 of them.
                page += 1
    except CrawlIncomplete:
        raise
    except Exception as exc:
        raw = f"{type(exc).__name__}: {str(exc)[:160]}"
        classified = classify_transport_failure(exc)
        if classified is None:
            raise incomplete(raw) from exc
        # ORDER IS LOAD-BEARING: code and raw error first. The queue stores this text
        # truncated (db/catalog_onboard_queue.py), and the original error is the part a
        # reader cannot reconstruct — a classification is a reading of evidence, and a
        # wrong reading has to stay checkable against what was actually raised.
        served = ""
        if scanned:
            # The host answered before this, so "could not be reached" would be false.
            served = (f" The host served {scanned} product(s) across {page - 1} page(s) "
                      f"before this, so this is a failure mid-crawl, not an unreachable host.")
        raise incomplete(
            f"{classified['reason_code']}: {raw} — {classified['explanation']}.{served} "
            f"Next: {_BLAME_NEXT_STEP[classified['blame']]}.",
            reason_code=classified["reason_code"],
        ) from exc


async def fetch_shopify_collections(
    domain: str,
    collections: Sequence[str],
    *,
    only_vendors: Optional[Sequence[str]],
    max_products: int,
    max_scan_products: int,
) -> "ShopifyProductBatch":
    """The listed brand collections of a store too large for /products.json (> 100 pages), each crawled
    to ITS empty page, vendor-filtered, and merged by product id. Complete for those collections only:
    the report's scope names them, never the store. A collection that lists none of the vendors fails
    the crawl loudly -- a mistyped or re-purposed handle must not read as "the store carries nothing"."""
    handles = list(dict.fromkeys(str(h) for h in collections))
    if not handles or not all(valid_collection_handle(h) for h in handles):
        raise ValueError(f"collections must be Shopify collection handles: {list(collections)!r}")
    merged: Dict[Any, Dict[str, Any]] = {}
    per: Dict[str, Dict[str, Any]] = {}
    scanned = pages = 0
    for handle in handles:
        remaining = max_scan_products - scanned  # the scan budget bounds ALL collections together
        if remaining < 1:
            raise CrawlIncomplete(f"{_clean_domain(domain)}: scan budget {max_scan_products} exhausted before "
                                  f"collection {handle!r}", status="capped", next_page=0,
                                  scanned_products=scanned, selected_products=len(merged))
        try:
            batch = await fetch_shopify_products(domain, only_vendors=only_vendors, max_products=max_products,
                                                 max_scan_products=remaining, collection=handle)
        except CrawlIncomplete as exc:
            # Report the WHOLE crawl so far, not just this collection's share.
            exc.scanned_products += scanned
            exc.selected_products += len(merged)
            raise
        report = batch.crawl_report
        per[handle] = {k: report.get(k) for k in ("pages", "scanned_products", "selected_products")}
        scanned += int(report.get("scanned_products") or 0)
        pages += int(report.get("pages") or 0)
        if not batch:
            raise CrawlIncomplete(
                f"{_clean_domain(domain)}: collection {handle!r} lists no product from vendors "
                f"{list(only_vendors or [])} ({report.get('scanned_products')} scanned)",
                status="failed", next_page=0, scanned_products=scanned, selected_products=len(merged))
        for product in batch:
            merged.setdefault(product.get("id") or product.get("handle"), product)
    if len(merged) > max_products:
        raise CrawlIncomplete(f"{_clean_domain(domain)}: selected-product budget {max_products} exhausted",
                              status="capped", next_page=0, scanned_products=scanned, selected_products=len(merged))
    out = ShopifyProductBatch(list(merged.values()), scanned_products=scanned, pages=pages,
                              scope="collections:" + ",".join(handles))
    out.crawl_report["collections"] = per
    return out


def _native_shopify_id(value: Any) -> Optional[str]:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    text = str(value).strip()
    return text if len(text) <= 30 and re.fullmatch(r"[0-9]+", text) and int(text) > 0 else None


def _missing_barcode(variant: Dict[str, Any]) -> bool:
    return variant.get("barcode") is None or str(variant["barcode"]).strip() == ""



async def _fetch_missing_variant_gtins(
    product: Dict[str, Any], *, domain: str, client: httpx.AsyncClient, deadline: Optional[float] = None,
    hosts: Optional[set] = None,
) -> Tuple[Dict[str, str], int]:
    """Return only validated (native variant ID -> GTIN), never detail-page copy.

    A product recovery uses at most three HTTP requests (two same-storefront
    redirects). Check each redirect before following, including www/apex aliases,
    and never visit another region/store. Price units differ on .js, so no price,
    product fields, or new variant identities may escape this helper.
    """
    requests = 0
    host = _clean_domain(domain)
    product_id = _native_shopify_id(product.get("id"))
    handle = str(product.get("handle") or "").strip()
    vendor = _vendor_token(product.get("vendor"))
    variants = product.get("variants") or []
    source_ids = [_native_shopify_id(v.get("id")) for v in variants if isinstance(v, dict)]
    wanted = {
        _native_shopify_id(v.get("id")) for v in variants
        if isinstance(v, dict) and _missing_barcode(v)
        and variant_id_provenance(str(v.get("id") or ""), product_id=product_id, handle=handle) == MERCHANT_ISSUED
    } - {None}
    if (not host or not product_id or not handle or not vendor or not wanted
            or any(source_ids.count(vid) != 1 for vid in wanted)):
        return {}, requests
    url = f"https://{host}/products/{quote(handle, safe='')}.js"
    shopify_edge_pacer.mark_shopify_host(host)  # /products/<handle>.js is a Shopify endpoint
    try:
        for redirect in range(3):
            # A request the shared Shopify-edge budget paces waits for its turn, bounded by what is left
            # of the recovery's budget: a fixed 10s refused every product once that schedule was more
            # than 10s out, while an unbounded wait slept out a 429'ing host's growing backoff product
            # after product (~16h for 200). Any other request keeps main's 10s bound, so with the pacer
            # off this is main exactly. A refusal (CrawlPaced) propagates to
            # recover_missing_variant_gtins, which stops the batch and counts why (review of #2477).
            if hosts is not None:
                hosts.add(crawl_politeness.host_of(url))  # the redirect target too: blocks are its own
            wait = _remaining_wait(deadline) if shopify_edge_pacer.applies(url) else 10.0
            await crawl_politeness.before_request(url, user_agent=_UA, max_wait=wait)
            requests += 1
            response = await client.get(url)
            _note_response(url, response)
            if not _same_storefront_host(host, getattr(getattr(response, "url", None), "host", None)):
                return {}, requests
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                target = urlsplit(urljoin(url, location or ""))
                if (not location or redirect == 2 or target.scheme != "https"
                        or target.username or target.password or target.port not in (None, 443)
                        or not _same_storefront_host(host, target.hostname)):
                    return {}, requests
                url = target.geturl()
                continue
            if response.status_code != 200:
                return {}, requests
            detail = response.json()
            if (not isinstance(detail, dict) or _native_shopify_id(detail.get("id")) != product_id
                    or str(detail.get("handle") or "").strip() != handle
                    or _vendor_token(detail.get("vendor")) != vendor
                    or not isinstance(detail.get("variants"), list)):
                return {}, requests
            detail_variants = detail["variants"]
            if not all(isinstance(v, dict) for v in detail_variants):
                return {}, requests
            detail_ids = [_native_shopify_id(v.get("id")) for v in detail_variants]
            recovered = {}
            for variant in detail_variants:
                vid = _native_shopify_id(variant.get("id"))
                if vid not in wanted or detail_ids.count(vid) != 1:
                    continue
                gtin = validated_source_gtin(variant.get("barcode"))
                if gtin:
                    recovered[vid] = gtin
            return recovered, requests
    except crawl_politeness.CrawlPaced:
        if requests:  # refused on a redirect hop: this product did ask, so it failed like any other
            return {}, requests
        raise  # refused before asking: not this product's failure; the caller stops and counts why
    except Exception as exc:
        logger.debug("GTIN recovery refused for %s/%s: %s", host, handle, type(exc).__name__)
    return {}, requests


#: A GTIN recovery never runs longer than this (seconds). Above what the unthrottled path needs at the
#: drain's pacing (200 products x CRAWL_MIN_INTERVAL_SECONDS 4 = 800s), so it bounds only a backed-up
#: Shopify-edge schedule or a slow host; the INCI step's budget (pipeline.DRAIN_PDP_INCI_BUDGET_S) is
#: the same 900s.
GTIN_RECOVERY_BUDGET_S = 900.0
#: Stop asking a host after this many 429/503s in a row: one, as on main, where the first throttle
#: ended the batch after a single request. Asking again only feeds the throttle (2026-09-30).
GTIN_RECOVERY_STOP_AFTER_BLOCKS = 1


def _clock() -> float:
    """The recovery budget's clock -- a seam, so a test can move it without moving asyncio's."""
    return time.monotonic()


def _remaining_wait(deadline: Optional[float]) -> float:
    """The longest a paced request may wait for its turn: the rest of the budget. 0 means unbounded in
    crawl_politeness, so no deadline -> 0, and an exhausted one raises rather than going unbounded."""
    if deadline is None:
        return 0
    left = deadline - _clock()
    if left <= 0:
        raise crawl_politeness.CrawlPaced("GTIN recovery budget exhausted")
    return left


async def recover_missing_variant_gtins(
    products: List[Dict[str, Any]], *, domain: str, max_fetches: int = 100,
    budget_s: Optional[float] = GTIN_RECOVERY_BUDGET_S,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Recover selected products only, before shade folding changes product identity.

    attempted/recovered/failed/capped/paced count PRODUCTS; recovered_gtins counts variant
    barcodes and http_requests includes bounded redirect hops. The batch STOPS, and the products it
    did not attempt are counted by why -- never `failed`:
      * blocked_stopped: the host, or a host it redirected to, answered GTIN_RECOVERY_STOP_AFTER_BLOCKS
        429/503s in a row;
      * budget_stopped: `budget_s` (GTIN_RECOVERY_BUDGET_S, 900s) ran out. It applies only with the
        shared Shopify-edge pacer on, bounding each paced request's wait to what is left of it;
      * paced: the crawl gate refused to wait (a Crawl-delay longer than we honour, or main's 10s
        bound on a request the shared pacer does not gate). A failed observation
    never changes its product. Each attempt has at most three requests with the
    client's ten-second timeout, paced by the shared merchant crawl gate.
    """
    if type(max_fetches) is not int or max_fetches < 0:
        raise ValueError("max_pdp_identity_fetches must be a nonnegative integer")
    copied = [dict(p, variants=[dict(v) if isinstance(v, dict) else v for v in (p.get("variants") or [])])
              for p in products]
    candidates = [p for p in copied if any(isinstance(v, dict) and _missing_barcode(v)
                                          for v in p["variants"])]
    report = {"attempted": 0, "recovered": 0, "failed": 0, "paced": 0, "budget_stopped": 0,
              "blocked_stopped": 0,
              "capped": max(0, len(candidates) - max_fetches), "recovered_gtins": 0, "http_requests": 0}
    if not candidates or not max_fetches:
        return copied, report
    async with httpx.AsyncClient(follow_redirects=False, timeout=httpx.Timeout(10.0, connect=5.0),
                                 headers={"User-Agent": _UA, "Accept": "application/json"}) as client:
        batch = candidates[:max_fetches]
        hosts = {_clean_domain(domain)}
        # The budget bounds the shared pacer's waits; with the pacer off there is none, as on main.
        deadline = _clock() + budget_s if budget_s and shopify_edge_pacer.enabled() else None
        for index, product in enumerate(batch):
            left = len(batch) - index
            if max(crawl_politeness.consecutive_blocks(h) for h in hosts) >= GTIN_RECOVERY_STOP_AFTER_BLOCKS:
                report["blocked_stopped"] = left
                logger.warning("GTIN recovery for %s stopped: the host is throttling (429/503); %d "
                               "product(s) not attempted", domain, left)
                break
            try:
                _remaining_wait(deadline)  # raises once the budget is spent, before counting an attempt
                recovered, requests = await _fetch_missing_variant_gtins(
                    product, domain=domain, client=client, deadline=deadline, hosts=hosts)
            except crawl_politeness.CrawlPaced as exc:
                # Refused before any request for this product: it is not attempted, and neither is
                # anything after it.
                budget_spent = deadline is not None and not isinstance(exc, crawl_politeness.CrawlDelayTooLong)
                key = "budget_stopped" if budget_spent else "paced"
                report[key] = left
                logger.warning("GTIN recovery for %s stopped (%s: %s); %d product(s) not attempted",
                               domain, key, type(exc).__name__, left)
                break
            report["attempted"] += 1
            report["http_requests"] += requests
            applied = 0
            for variant in product["variants"]:
                if not isinstance(variant, dict):
                    continue
                gtin = recovered.get(_native_shopify_id(variant.get("id")))
                if gtin and _missing_barcode(variant):
                    variant["barcode"] = gtin
                    applied += 1
            report["recovered_gtins"] += applied
            report["recovered" if applied else "failed"] += 1
    return copied, report


def _first(seq: Any) -> Optional[Dict[str, Any]]:
    return seq[0] if isinstance(seq, list) and seq and isinstance(seq[0], dict) else None


# <script>/<style> INNER TEXT is code, not prose — page-builder exports
# (PageFly/GemPages) routinely embed style blocks in body_html; a naive
# tag-strip would keep the CSS soup and could auto-publish it as the brand's
# words. Strip whole blocks (and comments) BEFORE the tag pass.
_HTML_BLOCK_RE = re.compile(r"(?is)<(script|style)\b.*?</\1\s*>|<!--.*?-->")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
BODY_TEXT_MAX_LEN = 2000  # PDP prose field, not a document store


def body_html_to_text(body_html: Optional[str]) -> str:
    """Deterministic Shopify body_html → plain text: drop script/style/comment
    blocks, strip tags, unescape entities, collapse whitespace, cap at a word
    boundary. Output is the brand's own words or ''."""
    if not body_html:
        return ""
    text = _HTML_BLOCK_RE.sub(" ", str(body_html))
    text = _HTML_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > BODY_TEXT_MAX_LEN:
        cut = text[:BODY_TEXT_MAX_LEN]
        text = cut.rsplit(" ", 1)[0] if " " in cut else cut
    return text


# Brand storefronts routinely list the INCI under an "Ingredients" heading inside
# body_html. Capture it deterministically (no LLM): find the label, take the
# following text, and stop at the next section heading. Null when absent — never
# fabricated; the downstream INCI intake re-validates it parses as a real list.
_INCI_LABEL_RE = re.compile(
    r"(?is)\b(?:full\s+|all\s+|key\s+)?ingredients?\b(?:\s*list)?\s*[:\-]\s*(.{15,3000})"
)
_INCI_STOP_RE = re.compile(
    r"(?is)\b(?:how\s+to\s+use|directions|how\s+to\s+apply|usage|warnings?|caution|"
    r"about\s+the\s+brand|shipping|net\s+wt|precautions)\b"
)


def inci_from_body_html(body_html: Optional[str]) -> Optional[str]:
    """Deterministic INCI extraction from a Shopify body_html: strip tags, find an
    'Ingredients:' label, capture the following list, and cut at the next section
    heading. Returns None when there's no label or the captured text isn't a
    comma-delimited list (a single blob / prose is rejected here and again by the
    canonical INCI intake)."""
    if not body_html:
        return None
    text = _HTML_BLOCK_RE.sub(" ", str(body_html))
    text = _HTML_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    m = _INCI_LABEL_RE.search(text)
    if not m:
        return None
    tail = m.group(1).strip()
    stop = _INCI_STOP_RE.search(tail)
    if stop:
        tail = tail[: stop.start()].strip()
    tail = tail.rstrip(". ").strip()
    # A real INCI is a comma-delimited list of several ingredients, not a blurb.
    if len([p for p in tail.split(",") if p.strip()]) < 2:
        return None
    return tail or None


# --- Rendered-PDP INCI (the metafield / accordion source) ------------------
# The K-beauty cohort keeps its full INCI OUT of `/products.json` body_html — it
# lives in a Shopify product METAFIELD that the theme renders into the PDP either
# as a visible <p> inside a "Full Ingredients" popup/accordion/modal (cosrx,
# axis-y, iunik, skin1004 confirmed 2026-07-19) or as a JSON string inside a data
# island (rich-text `{"type":"text","value":"Water, ..."}`, or an escaped-HTML
# metafield string). Neither is in `/products.json`, so body_html capture
# recovered INCI for only ~4.6% of the cohort. This extractor reads the RENDERED
# PDP HTML from BOTH surfaces and recovers the list deterministically — never
# fabricated; re-validated by the canonical INCI intake before it can write.
_PDP_STRIP_RE = re.compile(r"(?is)<(script|style|template|noscript)\b.*?</\1\s*>|<!--.*?-->")
_PDP_BLOCK_BOUNDARY_RE = re.compile(r"(?is)</(p|div|li|td|section|h[1-6])>|<br\s*/?>")
# Block-level closers WITHOUT <br>, plus the <br> tag alone. Used by the join-<br>
# segmentation variant: some themes wrap ONE continuous ingredient list across
# several visual lines with <br> (even mid-ingredient-name, e.g. dasique's
# "Ethylhexyl<br/>Hydroxystearate"), so treating <br> as a break shreds the list.
# The join variant collapses <br> to a space and only breaks on real block tags.
_PDP_BLOCK_ONLY_RE = re.compile(r"(?is)</(p|div|li|td|section|h[1-6])>")
_PDP_BR_RE = re.compile(r"(?is)<br\s*/?>")
# A leading "Full Ingredients:" / "INCI —" / "[INGREDIENTS]" label sometimes shares
# the <p>/value with the list (cosrx / barr-cosmetics); strip it (incl. surrounding
# brackets/parens) so the written value is the list itself.
_PDP_INCI_LABEL_RE = re.compile(
    r"(?i)^\s*[\[(]?\s*"
    r"(?:full\s+|all\s+|key\s+|active\s+|main\s+)?"
    r"(?:ingredients?(?:\s+list)?|inci)"
    r"\s*[\])]?\s*[:\-–—]?\s*"
)
# A full INCI opens with the highest-concentration ingredient, which for an
# aqueous cosmetic is water/aqua (regulatory descending-order). Matching this is
# the FAST qualifier: it cleanly tells a full INCI from a short "key ingredients"
# highlight (which opens with an active) and rejects comma-heavy noise. But it is
# NOT required — a real full INCI can open with a non-water ingredient (a Centella
# serum whose extract outranks water; an anhydrous balm/cushion that opens with a
# wax/oil). Those qualify via `_pdp_is_full_inci`'s secondary path instead, so we
# no longer leave them null. The same regex also tests an individual comma-part for
# "is this ingredient the solvent?" (used to require water be PRESENT in the list).
_PDP_SOLVENT_OPENER_RE = re.compile(
    r"(?i)^\s*(?:purified\s+|deionized\s+|distilled\s+)?(?:aqua|water|eau)\b"
)
# JSON string literals in a data island; pre-filtered to comma-bearing strings
# that plausibly contain the solvent before the (costlier) decode+gate.
_PDP_JSON_STR_RE = re.compile(r'"((?:[^"\\]|\\.){20,8000})"')
_PDP_SOLVENT_HINT_RE = re.compile(r"(?i)(?:aqua|water|eau)")
# A full INCI lists many ingredients; a "key ingredients" highlight is short. The
# secondary (non-water-opener) qualifier requires at least this many parts so a
# short active-forward highlight can never be mistaken for a full list.
_PDP_FULL_INCI_MIN_PARTS = 10
# Shape of a real INCI opening token: a short noun phrase of chemical/botanical
# words, no sentence punctuation. Rejects a product-name/heading that a join-<br>
# pass may prepend to a list (e.g. "GLOW LAYERING FIT CUSHION (NO.17 IVORY) WATER").
_PDP_INGREDIENT_TOKEN_RE = re.compile(r"^[A-Za-z0-9 ()\-./+'&]+$")
# When several distinct full lists appear on one page they are EITHER shade variants
# of the SAME product (collapse to one) OR different products — a bundle/kit, or a
# neighbor/related-product's list carried in a recommendations JSON island (stay
# ambiguous -> None, never attribute another product's list to this page). The
# separation is tight and precision-critical: measured 2026-07-20, real shade groups
# score ingredient-set Jaccard 0.90 (misshaus cushion, min pairwise) to 0.98
# (dasique balm), while two GENUINELY DIFFERENT same-line K-beauty products that
# share a large aqueous base score only ~0.67–0.73. A 0.7 floor (the first cut of
# this fix) wrongly collapsed those neighbors and, picking the longest, PUBLISHED the
# neighbor's list. The floor is raised to 0.85 (well above the ~0.73 different-product
# ceiling, below the 0.90 shade floor) AND gated by two corroborating signals that a
# shared-base neighbor fails: identical opening ingredient and near-identical length.
_PDP_SHADE_SIM_MIN = 0.85
# Shade variants are the same formula (reordered / pigment-tail only), so their
# ingredient COUNT barely moves; a different product (toner vs serum) differs more.
_PDP_SHADE_LEN_RATIO = 1.15


def _pdp_visible_segments(page_html: str, *, join_br: bool = False) -> List[str]:
    """Block-level VISIBLE text lines: drop code/comment blocks, turn block-closers
    into newlines (so a self-contained <p> INCI is its own line), strip tags,
    unescape entities, collapse whitespace. Covers the accordion/modal/rich-text
    <div> surface.

    Two <br> modes, both run by the extractor: the default treats <br> as a line
    break (separates a product-name heading from its list — misshaus's
    `<strong>NAME</strong><br>WATER, ...`); `join_br=True` treats <br> as a space
    so a single list wrapped across many <br> lines is reassembled whole (dasique's
    `Ethylhexyl<br/>Hydroxystearate, ...`). Fragments from the break mode that are
    sub-strings of the reassembled list are dropped downstream."""
    view = _PDP_STRIP_RE.sub(" ", page_html)
    if join_br:
        view = _PDP_BR_RE.sub(" ", view)
        view = _PDP_BLOCK_ONLY_RE.sub("\n", view)
    else:
        view = _PDP_BLOCK_BOUNDARY_RE.sub("\n", view)
    view = _HTML_TAG_RE.sub(" ", view)
    view = html.unescape(view)
    out: List[str] = []
    for line in view.split("\n"):
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            out.append(line)
    return out


def _pdp_json_island_segments(page_html: str) -> List[str]:
    """Candidate lines from JSON string literals in a data island (including inside
    <script> JSON), covering the metafield-as-JSON surface: a rich-text text node
    (`{"type":"text","value":"Water, ..."}`) or an escaped-HTML metafield string
    (`"\\u003cp\\u003eWater, ...\\u003c/p\\u003e"`). Each candidate string is
    JSON-decoded, tag-stripped, and split on rich-text paragraph breaks."""
    if not _PDP_SOLVENT_HINT_RE.search(page_html):
        return []
    out: List[str] = []
    for m in _PDP_JSON_STR_RE.finditer(page_html):
        raw = m.group(1)
        if "," not in raw or not _PDP_SOLVENT_HINT_RE.search(raw):
            continue
        try:
            decoded = json.loads('"' + raw + '"')
        except Exception:  # noqa: BLE001 — a non-JSON match just isn't a candidate
            continue
        decoded = html.unescape(_HTML_TAG_RE.sub(" ", decoded))
        for chunk in re.split(r"\n\s*\n|\r", decoded):
            chunk = re.sub(r"\s+", " ", chunk).strip()
            if chunk:
                out.append(chunk)
    return out


def _pdp_ingredient_like(part: str) -> bool:
    """Does a comma-part look like a single INCI ingredient (short chemical/botanical
    noun phrase) rather than a product-name/heading or a sentence? Guards the opener
    of a non-water-opening list so a join-<br> pass can't smuggle a leading heading
    ("GLOW LAYERING FIT CUSHION (NO.17 IVORY) WATER") in as the first "ingredient"."""
    p = part.strip()
    if not p or len(p.split()) > 6:
        return False
    if not _PDP_INGREDIENT_TOKEN_RE.match(p):
        return False
    return bool(re.search(r"[A-Za-z]{3}", p))


def _pdp_is_full_inci(cand: str) -> bool:
    """A candidate that already cleared `_looks_like_inci_list` is a FULL product
    INCI when EITHER it opens with the solvent (aqueous product, INCI descending-
    order — the fast, high-precision path) OR it is unambiguously a full list that
    happens to open with a non-water ingredient: a clean ingredient opener, MANY
    ingredients, and water/aqua present somewhere as an ingredient. The secondary
    path recovers Centella serums, balms and cushions (opener is an extract/wax/oil)
    without admitting a short "key ingredients" highlight (few parts, active opener,
    typically no water)."""
    if _PDP_SOLVENT_OPENER_RE.match(cand):
        return True
    parts = [p.strip() for p in cand.split(",") if p.strip()]
    if len(parts) < _PDP_FULL_INCI_MIN_PARTS:
        return False
    if not _pdp_ingredient_like(parts[0]):
        return False
    return any(_PDP_SOLVENT_OPENER_RE.match(p) for p in parts)


def _pdp_norm_key(text: str) -> str:
    """Alphanumeric-only lowercase fingerprint of a list (for dedup / substring tests)."""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _pdp_parts(text: str) -> List[str]:
    return [p.strip() for p in text.split(",") if p.strip()]


def _pdp_solvent_opener_count(cand: str) -> int:
    """How many comma-parts are a solvent opener (water/aqua/eau at the start of the
    part). A real INCI has EXACTLY ONE solvent entry; two means two lists were
    concatenated (a kit/routine block whose per-product lists were joined across a
    bare <br>). Note the first solvent may be mid-list — a Centella serum opens with
    the extract and lists Water at #4 — so we count occurrences, not position."""
    return sum(1 for p in _pdp_parts(cand) if _PDP_SOLVENT_OPENER_RE.match(p))


def _pdp_inci_similarity(a: str, b: str) -> float:
    """Jaccard overlap of two lists' normalized ingredient SETS (order-independent —
    shade variants reorder ingredients). 1.0 = identical set, 0.0 = disjoint."""
    def _set(text: str) -> set:
        return {_pdp_norm_key(p) for p in text.split(",") if _pdp_norm_key(p)}
    sa, sb = _set(a), _set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _pdp_collapse_shades(cands: List[str]) -> Optional[str]:
    """Given >1 distinct full lists, return one ONLY when they are shade variants of
    the SAME product; else None (never guess which product's list to attribute).

    Three corroborating signals must ALL hold vs the longest (most complete) list —
    a shared-base neighbor or a bundle component fails at least one:
      * ingredient-set Jaccard >= _PDP_SHADE_SIM_MIN (near-identical formula);
      * identical opening ingredient (same #1 by concentration);
      * ingredient count within _PDP_SHADE_LEN_RATIO (a variant barely changes size).
    Comparing every candidate to the longest ref (not just the top two) means one odd
    list out of many still forces None."""
    ref = max(cands, key=len)
    ref_parts = _pdp_parts(ref)
    if not ref_parts:
        return None
    ref_opener = _pdp_norm_key(ref_parts[0])
    for cand in cands:
        if cand is ref:
            continue
        parts = _pdp_parts(cand)
        if not parts or _pdp_norm_key(parts[0]) != ref_opener:
            return None
        lo, hi = sorted((len(parts), len(ref_parts)))
        if lo == 0 or hi > lo * _PDP_SHADE_LEN_RATIO:
            return None
        if _pdp_inci_similarity(ref, cand) < _PDP_SHADE_SIM_MIN:
            return None
    return ref


def inci_from_pdp_html(page_html: Optional[str]) -> Optional[str]:
    """Deterministic INCI extraction from a RENDERED brand PDP.

    Scans the visible block text (accordion/modal/rich-text div) and the JSON data
    island (metafield rendered as a JSON string) for FULL ingredient lists. A segment
    is a candidate when it clears the strong INCI-list gate (`_looks_like_inci_list` —
    the reseller-tier safety net) AND reads as a FULL list (`_pdp_is_full_inci`) AND
    is a SINGLE list (exactly one solvent opener; two = two products concatenated).
    Then, so we never guess or fabricate:

      * exactly one distinct full INCI -> return it;
      * several that are shade variants of ONE product -> the longest (via
        `_pdp_collapse_shades`; near-identical set + opener + length);
      * several genuinely different lists (a bundle, or a neighbor/related-product's
        list in a recommendations island) -> None (never attribute another's list);
      * none -> None.

    The <br> handling is deliberately two-tier for precision. The DEFAULT visible pass
    breaks on <br>, so two products separated by a bare <br> in one block become two
    SEPARATE candidates (their ambiguity is then visible). A SECOND join-<br> pass
    reassembles a single list a theme wrapped across many <br> — but it may only
    REPAIR: a join candidate is adopted only when it strictly contains exactly ONE
    trusted (default/JSON) candidate (the <br>-shredded fragment made whole). A join
    candidate that spans TWO trusted candidates is a concatenation, not a repair, and
    is discarded — so join-<br> can never fabricate a franken list the strict passes
    didn't already see as separate.

    Whatever it returns is re-validated by
    `canonical_inci_intake.ingest_canonical_inci` before any write."""
    if not page_html:
        return None
    page_html = str(page_html)

    def _collect(segments) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for seg in segments:
            cand = _PDP_INCI_LABEL_RE.sub("", seg).strip().rstrip(". ").strip()
            if not _looks_like_inci_list(cand):
                continue
            if not _pdp_is_full_inci(cand):
                continue
            if _pdp_solvent_opener_count(cand) >= 2:
                continue  # two lists concatenated into one string -> not a single INCI
            key = _pdp_norm_key(cand)
            if key and (key not in out or len(cand) > len(out[key])):
                out[key] = cand
        return out

    # Trusted candidates come from REAL block/JSON boundaries.
    base = _collect((
        *_pdp_visible_segments(page_html),
        *_pdp_json_island_segments(page_html),
    ))
    # Reassembly candidates come from collapsing <br> to a space within a block.
    joined = _collect(_pdp_visible_segments(page_html, join_br=True))

    pool: Dict[str, str] = dict(base)
    for jkey, jcand in joined.items():
        contained = [bkey for bkey in base if bkey != jkey and bkey in jkey]
        if len(contained) >= 2:
            # The reassembled string strictly contains TWO OR MORE distinct trusted
            # lists -> it concatenated separate products (a kit/routine block), not a
            # single <br>-wrapped list. Discard it and keep the trusted lists apart so
            # the ambiguity check sees them. (Aqueous+aqueous concatenations are
            # already rejected upstream by the two-solvent-opener guard; this also
            # catches a concatenation whose second list is anhydrous.)
            continue
        # 0 or 1 trusted fragment inside: a single list the theme wrapped across <br>.
        # Adopt the reassembled whole; drop the lone shredded fragment if base saw one.
        for bkey in contained:
            pool.pop(bkey, None)
        pool[jkey] = jcand

    if not pool:
        return None
    # Same-list cleanup among trusted candidates: drop a candidate that is a contiguous
    # substring of another (e.g. a JSON-island whole vs a visible partial of the SAME
    # list). Concatenations were already rejected above, so a substring here is a
    # genuine partial of one list, never a distinct product.
    keys = list(pool)
    survivors = [
        cand
        for key, cand in pool.items()
        if not any(other != key and key in other for other in keys)
    ]
    if len(survivors) == 1:
        return survivors[0]
    return _pdp_collapse_shades(survivors)


_META_DESC_RE = re.compile(
    r"<meta\b[^>]*>",
    re.I,
)


def description_from_pdp_html(raw_html: str) -> Optional[str]:
    """Brand-authored copy from a PDP's own meta description, or None.

    WHY THIS EXISTS. `backfill_brand_official_descriptions` takes body copy from
    `/products.json` body_html, and some storefronts publish none — measured on jsmbeauty.sg
    2026-09-06: 158 of 232 products carry under 50 characters of body_html TEXT (LIP-PRESSION
    Glowy Tint's 123 characters of markup render to zero), so the whole cohort failed the 50-char
    floor. The copy is not missing from the site, only from that field: the same PDP serves 190
    characters of real prose in its meta description.

    PREFERS `name="description"`, NOT og. The og tag is frequently theme-generated; the name tag
    is the per-product SEO field a merchant fills. Measured on kyliecosmetics.com: og carried the
    STORE blurb while name carried the product's own line, so preferring og took the strictly
    worse string.

    BOILERPLATE IS NOT REJECTED HERE, and deliberately so. A theme with no per-product SEO
    description substitutes the SHOP blurb (`page_description | default: shop.description`), and
    app vendors write operational text into the name tag; both clear the 50-char floor with zero
    product information, and both are worse than staying blocked, because `is_published_ready`
    auto-publishes this lane. But neither is visible in ONE page's markup — the tell is that the
    value REPEATS across the storefront, or equals the shop's own blurb. That needs the whole
    domain, so it lives in the caller (`backfill_brand_official_descriptions.
    drop_shared_boilerplate`), against `fetch_shop_description` below.

    An earlier version of this function keyed on a PRESENT-BUT-EMPTY name tag. That is a THEME
    detail, not the substitution mechanism: Dawn-family themes wrap the name tag in
    `{% if page_description %}` and so OMIT it entirely. A 60-PDP sweep across 10 storefronts
    (2026-09-06) found the shop blurb served under an ABSENT name tag 9 times and under a
    present-but-empty one 0 times — the rule fired only on the single storefront it was derived
    from, and three parser paths (a raw `>` inside the value, a `content`-less tag, an unquoted
    value) silently disabled it even there.

    Returns None when there is nothing trustworthy — never a partial and never a guess.
    """
    if not raw_html:
        return None

    og: Optional[str] = None
    name: Optional[str] = None

    for tag in _META_DESC_RE.findall(raw_html)[:400]:
        key = ""
        for attr in ("property", "name"):
            m = re.search(rf'{attr}\s*=\s*"([^"]+)"', tag, re.I) or re.search(
                rf"{attr}\s*=\s*'([^']+)'", tag, re.I
            )
            if m:
                key = m.group(1).strip().lower()
                break
        if key not in ("og:description", "description"):
            continue
        # MATCH THE OPENING DELIMITER. A character class of both quotes ends the capture at the
        # first apostrophe inside a double-quoted attribute — measured live on kyliecosmetics.com,
        # an 88-character sentence truncated to 25 at "that's", which still cleared the 50-char
        # floor as a mid-sentence fragment. Raw apostrophes are ubiquitous in this copy.
        m = re.search(r'content\s*=\s*"([^"]*)"', tag, re.I | re.S) or re.search(
            r"content\s*=\s*'([^']*)'", tag, re.I | re.S
        )
        if not m:
            continue
        value = re.sub(r"\s+", " ", html.unescape(m.group(1))).strip()
        if key == "description":
            name = name or (value or None)
        elif og is None and value:
            og = value

    if name:
        return name
    return og


async def fetch_pdp_description(
    domain: str,
    handle: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
    timeout_s: float = 15.0,
) -> Optional[str]:
    """Fetch one brand PDP and recover its meta description.

    Deliberately the same shape as `fetch_pdp_inci` beside it, including gating BOTH branches:
    the caller-supplied-client branch is the one a batch loop uses, so gating only the standalone
    branch would leave the high-volume path unpaced. Any failure returns None — a brand site
    hiccup must never fabricate copy or raise into a backfill loop.
    """
    host = _clean_domain(domain)
    handle = str(handle or "").strip().strip("/")
    if not host or not handle:
        return None
    url = f"https://{host}/products/{handle}"
    timeout = httpx.Timeout(timeout_s, connect=5.0)
    headers = {"User-Agent": _UA, "Accept": "text/html"}
    try:
        await crawl_politeness.before_request(url, user_agent=_UA, max_wait=0)
        if client is not None:
            resp = await client.get(url)
        else:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=timeout, headers=headers
            ) as c:
                resp = await c.get(url)
        _note_response(url, resp)
        if resp.status_code != 200:
            return None
        return description_from_pdp_html(resp.text)
    except Exception as exc:  # noqa: BLE001 — a brand PDP being down must not break the batch
        logger.debug("fetch_pdp_description failed for %s/%s: %s", host, handle, str(exc)[:160])
        return None


async def fetch_shop_description(
    domain: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
    timeout_s: float = 15.0,
) -> Optional[str]:
    """The storefront's OWN blurb, from its homepage meta description, or None.

    NOT product copy — the opposite of it, and that is the point. A Shopify theme renders
    og:description as `page_description | default: shop.description`, so every product without an
    SEO description serves THIS string on its PDP: over the 50-char floor, identical across the
    storefront, and carrying no product information. Measured 2026-09-06, it reached
    `description_from_pdp_html` unchallenged on 9 of 60 PDPs across cosrx.com, mixsoon.us and
    medicube.us.

    Fetching it once per domain turns "is this the shop blurb?" from a guess about the theme's
    markup into an exact string comparison against the blurb itself — the substitution mechanism,
    not a symptom of it. One request per domain, gated like every other call in this module.
    Returns None on any failure, which simply leaves the comparison unarmed.
    """
    host = _clean_domain(domain)
    if not host:
        return None
    url = f"https://{host}/"
    timeout = httpx.Timeout(timeout_s, connect=5.0)
    headers = {"User-Agent": _UA, "Accept": "text/html"}
    try:
        await crawl_politeness.before_request(url, user_agent=_UA, max_wait=0)
        if client is not None:
            resp = await client.get(url)
        else:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=timeout, headers=headers
            ) as c:
                resp = await c.get(url)
        _note_response(url, resp)
        if resp.status_code != 200:
            return None
        return description_from_pdp_html(resp.text)
    except Exception as exc:  # noqa: BLE001 — a homepage being down must not break the batch
        logger.debug("fetch_shop_description failed for %s: %s", host, str(exc)[:160])
        return None


async def fetch_shop_description_from_meta(
    domain: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
    timeout_s: float = 15.0,
) -> Optional[str]:
    """The storefront's OWN blurb again, read from Shopify's `/meta.json` `description`.

    THE SAME STRING AS THE HOMEPAGE META, from a different door. Measured on jsmbeauty.sg
    2026-09-08: the homepage meta description and `/meta.json` `description` are the identical
    135 characters. The homepage is a 700 KB themed page that a Cloudflare-fronted store starts
    refusing after a run has fetched sixty product pages from the same egress, while the JSON
    endpoints keep answering — which is exactly the moment the description backfill asks for the
    blurb. This is the fallback for that moment, not a replacement: `fetch_shop_description`
    stays first because a theme can override `shop.description` on the homepage, and it is the
    HOMEPAGE string a PDP without its own SEO copy repeats. Returns None on any failure.

    NOT THE SAME STRING AS THE THEME RENDERS, and the caller must be told. `/meta.json` returns
    `shop.description` RAW, while the homepage meta and the PDP og tag both pass it through the
    theme (escaping, truncation, `| append: shop.name`), so the two can differ by exactly the
    filters that make the PDP comparison an EXACT match. Callers that use this to arm an
    equality test must treat the result as unverified — see `_load_shop_blurb` in
    scripts/backfill_brand_official_descriptions.py.
    """
    host = _clean_domain(domain)
    if not host:
        return None
    url = f"https://{host}/meta.json"
    shopify_edge_pacer.mark_shopify_host(host)  # a Shopify storefront endpoint
    timeout = httpx.Timeout(timeout_s, connect=5.0)
    headers = {"User-Agent": _UA, "Accept": "application/json"}
    try:
        await crawl_politeness.before_request(url, user_agent=_UA, max_wait=0)
        if client is not None:
            resp = await client.get(url)
        else:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=timeout, headers=headers
            ) as c:
                resp = await c.get(url)
        _note_response(url, resp)
        if resp.status_code != 200:
            return None
        # THE ANSWER MUST COME FROM THE HOST WE ASKED. `follow_redirects=True` with no check
        # will happily read `/meta.json` off whatever storefront the redirect chain ends on --
        # and a regional redirect (brand.com -> uk.brand.com, or an apex parked on a partner's
        # shop) lands on a DIFFERENT Shopify store with a different `shop.description`. That
        # string would then arm an equality comparison for a domain whose theme never renders
        # it: not merely useless, but the exact "non-empty blurb that matches nothing" shape
        # that switches OFF this lane's fail-closed refusal. www<->apex is the same storefront
        # and is allowed; anything else is not.
        if not _same_storefront_host(host, getattr(resp.url, "host", None)):
            logger.debug(
                "fetch_shop_description_from_meta: %s redirected off-host to %s — refusing",
                host, getattr(resp.url, "host", None),
            )
            return None
        data = resp.json()
        desc = data.get("description") if isinstance(data, dict) else None
        # A STRING OR NOTHING. `str(desc)` of a list renders `['a', 'b']` -- punctuation and all,
        # comfortably over the 50-char floor -- and a dict renders its repr. Both would be
        # published-shaped garbage rather than the shop blurb, and neither can ever equal a PDP
        # meta description, so both arrive as an unmatchable "blurb" instead of an absent one.
        if not isinstance(desc, str):
            return None
        desc = " ".join(desc.split())
        return desc or None
    except Exception as exc:  # noqa: BLE001 — same contract as fetch_shop_description
        logger.debug("fetch_shop_description_from_meta failed for %s: %s", host, str(exc)[:160])
        return None


async def fetch_pdp_inci(
    domain: str,
    handle: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
    timeout_s: float = 15.0,
) -> Optional[str]:
    """Fetch a single brand PDP and recover its INCI via `inci_from_pdp_html`.

    The polite, additive fallback for the (very common) cohort product whose
    `/products.json` body_html carries no ingredients. Returns None on any
    network/parse failure or when the PDP doesn't publish a recoverable INCI — a
    brand site hiccup must never fabricate or raise into the mint/backfill loop."""
    host = _clean_domain(domain)
    handle = str(handle or "").strip().strip("/")
    if not host or not handle:
        return None
    url = f"https://{host}/products/{handle}"
    timeout = httpx.Timeout(timeout_s, connect=5.0)
    headers = {"User-Agent": _UA, "Accept": "text/html"}
    try:
        # Gated on BOTH branches. The caller-supplied-client branch is the one the batch loop
        # uses, so gating only the standalone branch would leave the high-volume path unpaced —
        # the shape of "a guard on one path does not cover the path that bypasses it".
        await crawl_politeness.before_request(url, user_agent=_UA, max_wait=0)
        if client is not None:
            resp = await client.get(url)
        else:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=timeout, headers=headers
            ) as c:
                resp = await c.get(url)
        _note_response(url, resp)
        if resp.status_code != 200:
            return None
        return inci_from_pdp_html(resp.text)
    except Exception as exc:  # noqa: BLE001 — a brand PDP being down must not break the batch
        logger.debug("fetch_pdp_inci failed for %s/%s: %s", host, handle, str(exc)[:160])
        return None


def _to_float(value: Any) -> Optional[float]:
    """Coerce Shopify's string prices (e.g. '56.00') to float; None if absent/invalid.
    Numeric columns (catalog_offers.*_price, external_product_seeds.price_amount) reject
    strings, so the mapper must hand downstream a real number or None."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _brand_key(value: Optional[str]) -> str:
    """Alphanumeric-only comparison form of a brand/vendor label.

    Tighter than `_vendor_token` (which only casefolds) because the comparison here is
    "are these two labels the same BRAND", and the spellings that must compare equal
    differ by punctuation and spacing: `A'PIEU`/`Apieu`, `MISSHA US`/`Missha`.
    `_vendor_token` is left alone — it backs `filter_products_by_vendor`, where exact
    selection is the point and loose matching is the documented hazard.

    "&" is read as "and" before stripping: us.sandandsky.com publishes both `Sand & Sky US`
    and `Sand and Sky INT` (measured 2026-09-26), which stripped to `sandskyus` and
    `sandandskyint` and so could never be one brand under any job brand.
    """
    return "".join(c for c in str(value or "").casefold().replace("&", "and") if c.isalnum())


# A FIRST host label that names a region or a storefront role, not the store: us.sandandsky.com,
# us.frankbody.com, us.koraorganics.com (measured 2026-09-26), www./shop./store. hosts. The store's
# own name is the next label. Not stripped when the next label is a public second level
# (shop.com.sg's store label is "shop", not "com").
_HOST_PREFIX_LABELS = frozenset({"www", "shop", "store", "us", "uk", "au", "ca", "eu", "int"})
_PUBLIC_SECOND_LEVEL_LABELS = frozenset({"com", "co", "net", "org"})


def _store_host_label(domain: Optional[str]) -> str:
    """`_brand_key` of the label that names the store in `domain`, regional prefix skipped."""
    labels = _clean_domain(domain or "").split(".")
    while (len(labels) > 2 and labels[0] in _HOST_PREFIX_LABELS
           and labels[1] not in _PUBLIC_SECOND_LEVEL_LABELS):
        labels = labels[1:]
    return _brand_key(labels[0])


# MEASURED RENAMES that retailers still sell under the old and new names. In retailer mode a
# vendor is written under the operator's --brand only when the two are the SAME brand; exact
# alphanumeric equality decides that everywhere except here. Containment cannot: it would merge
# "Purito" with "Purito Seoul" (a rename -- correct) and equally "A'PIEU" with "A'PIEU Plus"
# (a distinct vendor -- tests/services/test_retailer_adversarial_acceptance.py pins that it is
# NOT overridden). So each family is listed, with the evidence that it is one brand.
#
# Measured 2026-09-18 over every vendor at eyurs.com, ohlolly.com and sokoglam.com: these are
# the ONLY cross-host spellings that exact equality leaves split.
RETAILER_BRAND_SPELLINGS = {
    # Manyo Factory renamed to ma:nyo: sokoglam "MANYO FACTORY", ohlolly "ma:nyo", eyurs "manyo".
    "manyofactory": "manyo",
    "manyo": "manyo",
    # Purito renamed to Purito Seoul: eyurs "Purito SEOUL", sokoglam "Purito Seoul", ohlolly "Purito".
    "puritoseoul": "purito",
    "purito": "purito",
    # US top-100 wave, measured 2026-09-24 (prod catalog_products + store vendors). A store's spelling
    # of a prestige brand is a different brand key otherwise -- words (Tom Ford / Tom Ford Beauty,
    # Christian Dior / Dior, Yves Saint Laurent / YSL) or ACCENTS, which _brand_key and
    # normalize_brand both keep (perfumania "Lancome" vs the catalog's "Lancôme").
    "tomford": "tomford", "tomfordbeauty": "tomford",                        # tomfordbeauty.com 145 rows
    "dior": "dior", "christiandior": "dior", "diorbeauty": "dior",           # perfumania "Christian Dior"
    "ysl": "ysl", "yslbeauty": "ysl", "yvessaintlaurent": "ysl", "yvessaintlaurentbeauty": "ysl",
    "jomalone": "jomalone", "jomalonelondon": "jomalone",                    # bluemercury "Jo Malone London"
    "lancome": "lancome", "lancôme": "lancome",
    "esteelauder": "esteelauder", "estéelauder": "esteelauder",
    "kiehls": "kiehls", "kiehlssince1851": "kiehls",
    "dolceandgabbana": "dolcegabbana", "dolcegabbana": "dolcegabbana",
    "lorealparis": "lorealparis", "loréalparis": "lorealparis",
    "tresemme": "tresemme", "tresemmé": "tresemme",
    "avene": "avene", "avène": "avene",                                      # bluemercury Avène, bigelow Avene
    "kerastase": "kerastase", "kérastase": "kerastase",
    # Measured 2026-09-24 over every vendor at 400+ Japanese/Australian beauty storefronts (JP/AU
    # coverage census): the same maker spelt with and without its accent. `_brand_key` keeps accented
    # letters (isalnum), and so does normalize_brand, so each spelling below was a separate brand
    # identity -- e.g. "Kose" at ichibanm.com (26), goodsania.com (185) vs "Kosé" at japanesetaste.com
    # (47), japanwithlovestore.com (93). Generic-word names that happen to split the same way
    # ("Elegance"/"Elégance", "Naive"/"Naïve") are deliberately NOT listed: an unrelated vendor
    # called "Elegance" would be relabelled.
    "kose": "kose", "kosé": "kose",
    "biore": "biore", "bioré": "biore",
    "curel": "curel", "curél": "curel",
    # Decorté was "Cosme Decorté" until 2020: a rename sold under both names (sasa.com "Cosme Decorte",
    # wafuu.com "COSME DECORTÉ", ichibanm.com "Decorte", decortecosmetics.com "Decorté").
    "decorte": "decorte", "decorté": "decorte", "cosmedecorte": "decorte", "cosmedecorté": "decorte",
    # sasa.com and nanamall.com shorten it to "Cle de Peau" / "Clé de Peau".
    "cledepeaubeaute": "cledepeaubeaute", "clédepeaubeauté": "cledepeaubeaute",
    "cledepeau": "cledepeaubeaute", "clédepeau": "cledepeaubeaute",
    "naturaglace": "naturaglace", "naturaglacé": "naturaglace",
    "visee": "visee", "visée": "visee",
    "fiancee": "fiancee", "fiancée": "fiancee",
    # lamourlife.com writes the Roman-numeral character "SK-Ⅱ" (U+2161), which casefolds to "ⅱ".
    "skii": "skii", "skⅱ": "skii",
    # AU: the brand's own store says "Lük Beautifood"; five US retailers say "Luk Beautifood".
    "lukbeautifood": "lukbeautifood", "lükbeautifood": "lukbeautifood",
    # wet n wild's own store (www.wetnwildbeauty.com, read 2026-09-26) files 500 of its 580 products
    # under the CORPORATE vendor "Markwins WNW" (Markwins Beauty Brands owns wet n wild; WNW = wet n
    # wild) and the other 80 under "wet n wild Beauty". Without this family a brand_official crawl
    # branded 500 products "Markwins WNW" and the domain review held the job (the store's name does
    # not start with "Markwins WNW"). Markwins' OTHER labels (e.g. "Markwins Physicians Formula") are
    # deliberately not listed: only the WNW code names this brand.
    "wetnwild": "wetnwild", "wetnwildbeauty": "wetnwild", "markwinswnw": "wetnwild",
    # ETUDE HOUSE renamed to ETUDE (2023) and retailers sell under both, measured in prod 2026-09-28: 293 retailer
    # rows "ETUDE HOUSE" (luxiface.com 129, holiholic.com 106, openthebeauty.com 28, ...) against 184 "ETUDE" /
    # "Etude" (dodoskin.com 77, moidaus.com 33, kbeautymakeup.com 24, ...) -- one product at two retailers never
    # grouped. The existing rows were relabelled FIRST (scripts/relabel_retailer_brand.py, run relabel_07651af59606,
    # 2026-09-29: 303 rows, 0 held; a re-crawl never updates a stored brand) -- this family must not reach the drain
    # while an old-spelling row is live, or intake re-keys that row into the new key's group and apply refuses it.
    "etudehouse": "etude", "etude": "etude",
    # _brand_key keeps accents (isalnum), so the accented spellings are keys of their own (review of #2440).
    "étudehouse": "etude", "étude": "etude",
    # K-beauty / US wave, measured in prod 2026-09-30 over the 13,337 rows ingested 09-23..09-29: each brand
    # below was stored under two or three spellings that differ ONLY in case (normalize_brand lowercases, so
    # every spelling of one brand already shares a content_key -- these families move no identity, and so,
    # unlike ETUDE, may reach the drain before the old rows are relabelled). The vendor's case decided the
    # display brand: dodoskin.com/holiholic.com "MISSHA" (413 retailer rows) vs misshaus.com "Missha" (149);
    # eyurs.com "ROUND LAB" vs roundlab.com "Round Lab"; skin1004.com itself writes both "SKIN1004" and
    # "Skin1004"; cocomo.sg "ANUA"/"ISNTREE"/"GENABELLE"/"MOLVANY"/"ARENCIA". "Centellian 24" (one merchant
    # row, not a crawl lane) splits by a space; the crawl lanes only ever wrote "Centellian24"/"CENTELLIAN24",
    # so that family moves no stored crawl row's identity either.
    # NOT here yet, because they DO move identity (see services/brand_relabel.py ORDER): "O HUI (오휘)" /
    # "Jung Saem Mool" (smkoreabeauty.com retailer rows -- relabel first), "APIEU" (misshaus.com brand rows --
    # the product_key moves too), "Supergoop" (supergoop.com brand rows -- no relabel tool reaches them).
    "missha": "missha", "mixsoon": "mixsoon", "roundlab": "roundlab", "anua": "anua", "arencia": "arencia",
    "skin1004": "skin1004", "medicube": "medicube", "beautyofjoseon": "beautyofjoseon",
    "theordinary": "theordinary", "isntree": "isntree", "centellian24": "centellian24",
    "makeupforever": "makeupforever", "genabelle": "genabelle", "maybelline": "maybelline",
    "bobbibrown": "bobbibrown", "molvany": "molvany", "byterry": "byterry",
    # Identity-changing, added only AFTER its rows were relabelled (services/brand_relabel.py ORDER): smkoreabeauty.com
    # wrote "Jung Saem Mool" (5 retailer rows; normalize_brand keeps the spaces, so its content_key differed from
    # jsmbeauty.sg's "JUNGSAEMMOOL"). Relabelled by scripts/relabel_retailer_brand.py --family jungsaemmool (run id
    # in this change's PR); a re-crawl now writes "JUNGSAEMMOOL" onto the relabelled key instead of the old one.
    "jungsaemmool": "jungsaemmool",
    # Same ORDER: smkoreabeauty.com wrote "O HUI (오휘)" on 20 retailer rows (_brand_key keeps the Hangul, so its key
    # differs from "O HUI"). 18 were relabelled (relabel_retailer_brand.py --family ohui, run relabel_fad2cefd5223);
    # the 2 the relabel held -- a 2-piece and a 3-piece Extreme White set sharing one GTIN, hence one content_key --
    # were withdrawn first (withdraw_catalog_rows.py, reason ohui_set_gtin_collision). Neither had a product group,
    # so a re-crawl that re-keys them cannot trip _ensure_primary_retailer_group.
    "ohui": "ohui", "ohui오휘": "ohui",
}
# The spelling each family is WRITTEN as -- in retailer AND brand-official mode, with or without
# --brand. content_key is
# built from normalize_brand(brand), which keeps punctuation ("ma:nyo" != "manyo"), so writing
# the operator's string would still split a family whenever two runs spelled --brand
# differently. The canonical spelling is the one the catalog ALREADY carries, measured in prod
# 2026-09-18: 78 brand-official rows as "Ma:nyo" (manyo.us), and "Purito SEOUL"
# (purito-seoul.com). Any other spelling would split retailer rows from those.
RETAILER_BRAND_CANONICAL = {
    "manyo": "Ma:nyo",
    "purito": "Purito SEOUL",
    # The catalog's own spelling, measured 2026-09-24 (rows): Tom Ford Beauty 145 (vs "Tom Ford" 4),
    # Dior 23, YSL 6, Jo Malone 4, Lancôme 12, Estée Lauder 14 (vs "Estee Lauder" 2), Kiehl's Since
    # 1851 10, Dolce and Gabbana 38 (vs "Dolce & Gabbana" 1), L'Oreal Paris 12. No rows yet for
    # TRESemmé, Avène, Kérastase: the brand's own spelling.
    "tomford": "Tom Ford Beauty",
    "dior": "Dior",
    "ysl": "YSL",
    "jomalone": "Jo Malone",
    "lancome": "Lancôme",
    "esteelauder": "Estée Lauder",
    "kiehls": "Kiehl's Since 1851",
    "dolcegabbana": "Dolce and Gabbana",
    "lorealparis": "L'Oreal Paris",
    "tresemme": "TRESemmé",
    "avene": "Avène",
    "kerastase": "Kérastase",
    # 2026-09-24: prod carried none of these as a brand (only two seed-mirror rows, "Biore UV" and
    # "Clé de Peau"), so each family is written as the maker's own spelling on its own storefront.
    "kose": "Kosé",
    "biore": "Bioré",
    "curel": "Curél",
    "decorte": "Decorté",
    "cledepeaubeaute": "Clé de Peau Beauté",
    "naturaglace": "Naturaglacé",
    "visee": "Visée",
    "fiancee": "Fiancée",
    "skii": "SK-II",
    # The brand's own lowercase spelling, as on wetnwildbeauty.com.
    "wetnwild": "wet n wild",
    "lukbeautifood": "Lük Beautifood",
    # The brand's own current name (etude.com). Every family member is relabelled to it (see the spellings note).
    "etude": "ETUDE",
    # 2026-09-30 wave. Rule: the spelling the catalog already carries on the rows the retailer relabel tool
    # cannot reach (brand-official / merchant rows, not ext:retailer:), so repairing the existing rows is the
    # reviewed relabel of retailer rows alone. Rows per spelling outside ext:retailer: in parentheses. It
    # agrees with the brand's own styling where that is clear (SKIN1004, The Ordinary, Beauty of Joseon).
    "missha": "Missha",                     # misshaus.com 149 (vs "MISSHA" 0; 413 retailer rows)
    "mixsoon": "Mixsoon",                   # 225 (vs "MIXSOON" 4, mixsoon.us)
    "roundlab": "Round Lab",                # roundlab.com 88 (vs "ROUND LAB" 42)
    "anua": "Anua",                         # anua.com/anua.us 86 (vs "ANUA" 6, all suppressed)
    "arencia": "Arencia",                   # 126 (vs "ARENCIA" 21)
    "skin1004": "SKIN1004",                 # skin1004.com 93 (vs "Skin1004" 48)
    "medicube": "MEDICUBE",                 # 66 (vs "Medicube" 17, medicube.us)
    "beautyofjoseon": "Beauty of Joseon",   # beautyofjoseon.com 103 (vs "BEAUTY OF JOSEON" 2, suppressed)
    "theordinary": "The Ordinary",          # 109 (vs "the ordinary" 5)
    "isntree": "Isntree",                   # isntree-global.com 58 (vs "ISNTREE" 5, suppressed)
    "centellian24": "Centellian24",         # centellian24usa.com 106 (vs "Centellian 24" 1, "CENTELLIAN24" 1)
    "makeupforever": "MAKE UP FOR EVER",    # makeupforever.sg 74 (vs "Make Up For Ever" 0; 4 retailer rows)
    "genabelle": "Genabelle",               # 31 (vs "GENABELLE" 8, suppressed)
    "maybelline": "Maybelline",             # 25 (vs "MAYBELLINE" 0; 12 retailer rows)
    "bobbibrown": "Bobbi Brown",            # 9 (vs "BOBBI BROWN" 0; 17 retailer rows)
    "molvany": "Molvany",                   # 10 (vs "MOLVANY" 4, suppressed)
    "byterry": "BY TERRY",                  # retailer rows only (11 vs "By Terry" 4): the brand's own styling
    "jungsaemmool": "JUNGSAEMMOOL",         # jsmbeauty.sg 171 (vs smkoreabeauty.com "Jung Saem Mool" 5, relabelled)
    "ohui": "O HUI",                        # 409 retailer rows (vs smkoreabeauty.com "O HUI (오휘)" 20: 18 relabelled, 2 withdrawn)
}


def _retailer_brand_family(key: str) -> Optional[str]:
    """The listed brand family for an alphanumeric brand key, or None when it is not listed."""
    return RETAILER_BRAND_SPELLINGS.get(key)


def _looks_like_a_brand_name(value: Optional[str]) -> bool:
    """Only explicit supplier-code shapes are codes; short/Unicode names are brands.

    3CE and Chinese/Korean labels are actual Meitu vendors. Length or ASCII-only
    checks cannot establish that a merchant's different label is not a brand.
    """
    raw = str(value or "").strip()
    return bool(raw) and not raw.isdecimal() and not bool(re.fullmatch(r"[A-Za-z]{1,3}-[A-Za-z]?\d{3,}", raw))


def resolve_record_brand(
    vendor: Optional[str], brand_override: Optional[str], domain: Optional[str]
) -> Tuple[str, str]:
    """Decide the brand for ONE product, returning `(brand, reason)`.

    `brand_override` is a per-DOMAIN claim by the operator; `vendor` is the storefront's
    own per-PRODUCT claim. They disagree on brand-family storefronts: misshaus.com
    publishes 125 products of which 17 carry `vendor: "APIEU"` (Able C&C owns both
    labels). Renaming those to "Missha" is not a spelling normalisation, it is an
    assertion that A'pieu's products are Missha's — and it was measured live on
    2026-09-11 doing exactly that: 15 A'pieu rows in the index branded `Missha`, which
    also made them invisible to brand-strict recall (`external_seed_brand_strict_rows: 0`
    on a search for `A'PIEU` that nonetheless returned all 15).

    So the override still wins everywhere it is a NORMALISATION, and loses where it
    would be a RELABEL:
      * no vendor              -> override           (nothing to contradict it)
      * same brand, differently spelt -> override    (`MISSHA US` -> `Missha`)
      * vendor names the STORE -> override           (`thefaceshopny` on thefaceshopny.com)
      * genuine disagreement   -> VENDOR             (`APIEU` on misshaus.com)

    Containment, not equality, decides "same brand": `missha` is a substring of
    `misshaus`. It is deliberately narrow — both sides are short brand labels, and the
    cost of a false "same" is only today's behaviour, while the cost of a false
    "different" is a wrong brand on the row. Guarded by a 3-character floor so a
    2-letter vendor cannot be a substring of half the brands in the catalogue.
    """
    v_raw = str(vendor or "").strip()
    o_raw = str(brand_override or "").strip()
    if not o_raw:
        return v_raw, "vendor_only"
    if not v_raw:
        return o_raw, "override_no_vendor"
    v, o = _brand_key(v_raw), _brand_key(o_raw)
    if not v:
        return v_raw, "vendor_disagrees"
    # Containment subsumes equality (`v == o` implies `v in o`), including below the
    # 3-char floor: two equal sub-floor keys cannot carry a 3-letter run either, so
    # they fall to `_looks_like_a_brand_name` and return the same string anyway. A
    # separate equality arm here was provably unreachable — it changed no output under
    # mutation — and is deliberately absent rather than kept as untested reassurance.
    if len(v) >= 3 and len(o) >= 3 and (v in o or o in v):
        return o_raw, "override_same_brand"
    # The same 3-char floor on the HOST label: without it the "us" of us.sandandsky.com matched
    # every vendor containing "us" -- `Sand and Sky US` by coincidence, and equally `Lush`.
    host_label = _store_host_label(domain)
    if len(host_label) >= 3 and len(v) >= 3 and (v in host_label or host_label in v):
        # The vendor field is the STORE's name, not a brand — the override is the only
        # brand claim available and is what the operator came to assert. Measured:
        # metro.com.sg publishes `vendor: "Metro Singapore Departmental Store -
        # Celebrating 69 Years in SG"`, which contains the host label and names no brand.
        return o_raw, "override_vendor_is_store"
    if not _looks_like_a_brand_name(v_raw):
        # A SUPPLIER CODE is not a brand. sukoshi.com publishes `vendor: "VC-B004"` on
        # products whose brand appears only in the title; adopting that verbatim would
        # put "VC-B004" in the brand column, which is worse than the override it
        # replaced. The override at least names a real brand.
        return o_raw, "override_vendor_is_not_a_name"
    return v_raw, "vendor_disagrees"


# Reuses PR #2158's confidence contract and distinct-path ambiguity guard. The
# source label remains enrichment_agent_v1: it is also a canonical-scope lane ID.
CATEGORY_CONFIDENCE_MERCHANT_TYPE = 0.9
CATEGORY_CONFIDENCE_EXPLICIT_TITLE = 0.8
CATEGORY_CONFIDENCE_FEED_DEFAULT = 0.3
# A leaf taken from a MEASURED (host, merchant product_type) pair. Below a merchant type
# that names its class directly (0.9): the merchant declared a shelf, the leaf was inferred
# from reading every product on it. Distinct from every other writer's value so a stored
# (label, confidence) pair can be traced to this table.
CATEGORY_CONFIDENCE_MEASURED_HOST_TYPE = 0.82  # not 0.85: the regex backfill and variant fold write 0.85

# Generic shelves are not assertions of a purchasable product class. In particular,
# the shared legacy regex maps Lip Care to balm, contradicting the measured lip oil.
_GENERIC_PRODUCT_TYPES = frozenset({
    "beauty", "cosmetics", "makeup", "make up", "skin care", "skincare",
    "face care", "hair care", "haircare", "lip care", "lip treatment", "lip treatments",
    "lip color", "lip colour",
})
_GENERIC_LIP_TYPES = frozenset({"lip care", "lip treatment", "lip treatments"})
_TOOL_NOUN_SUFFIX = re.compile(r"\bbrush(?:es)?(?:\s+#?\d{1,4})?\s*$", re.I)
# Formula names ending in an included applicator are not tool names. This is a
# noun/suffix exception, not a general pass of marketing titles through the taxonomy.
_TOOL_FORMULA_CONTEXT = re.compile(r"[+&/]|\b(?:and|with|includes?|including|for|using|built[- ]in)\b|brush[- ]on", re.I)
# ...but where the merchant TYPE is itself "<something> Brush" ("Concealer Brush", "Eyeshadow Brush,
# Cheek Brush"), the type names the tool, and "&" inside the title joins two uses of one brush
# ("double-ended shadow & liner brush"). Still refused: a formula SOLD WITH a brush ("CC undereye &
# brush", "Foundation with Brush") -- the brush is then the second thing, not the product.
_TOOL_TYPE_FORMULA_CONTEXT = re.compile(r"\b(?:with|includes?|including|for|using|built[- ]in)\b|brush[- ]on", re.I)
# ...and a title split at a joiner is a brush only when no part BEFORE the last names a product:
# "Gel Eyeliner + Angled Brush", "CC undereye & mini brush", "Concealer, Brush" are a formula and a
# brush (review of #2403).
_TOOL_TITLE_JOINER = re.compile(r"\s*(?:[+&/,]|\s-\s|\band\b|\bplus\b)\s*", re.I)
# ...and after a joiner the brush must say what it is FOR ("liner brush", "buff concealer brush",
# "cheek brush"), or "Gel Liner & Brush" / "Pomade + Angled Brush" is a formula and a bare brush.
_BRUSH_USE_WORDS = re.compile(
    r"\b(?:liner|shadow|shader|crease|smudge|smudger|blend(?:ing|er)?|buff(?:ing|er)?|detail(?:ing)?|cheek|eyes?|"
    r"lips?|brows?|face|contour(?:ing)?|sculpt(?:ing)?|highlight(?:ing|er)?|powder|foundation|concealer|"
    r"corrector|blush|bronz(?:er|ing)|kabuki|fan|stippl(?:e|ing)|complexion|setting|precision)\b", re.I)
# The leaves a makeup brush's own name may carry: its use ("concealer brush") and texture words
# ("cream foundation brush", "serum foundation brush"). Not skincare formulas ("Sunscreen Brush" is a
# sunscreen dispensed through a brush) and not nails (a "Gel Polish Brush" is not a makeup tool).
# formula nouns no leaf pattern reads on their own ("CC undereye & brush")
_BRUSH_TITLE_FORMULA_WORDS = re.compile(r"\b(?:cc|bb|creams?|serums?|oils?|balms?|sticks?|palettes?|gloss|liquids?|"
                                        r"tints?|stains?|minis?|gels?|pomades?|kohls?|pigments?|glue|wax|mousse|putty)\b", re.I)


def _is_makeup_brush_title(title: str) -> bool:
    from services.pdp_category_classifier import CATEGORY_PATTERNS
    # one space between words: a leading \s* retried across a long whitespace run is quadratic
    # (re-review of #2403: 10,000 spaces took 9.7s; the classifier fixed the same bug the same way)
    title = " ".join(title.split())
    parts = [x for x in _TOOL_TITLE_JOINER.split(title) if x.strip()]
    if not parts or not _TOOL_NOUN_SUFFIX.search(parts[-1]) or _TOOL_TYPE_FORMULA_CONTEXT.search(title):
        return False
    if len(parts) > 1 and not _BRUSH_USE_WORDS.search(_TOOL_NOUN_SUFFIX.sub(" ", parts[-1])):
        return False
    for part in parts[:-1]:
        if (_BRUSH_TITLE_FORMULA_WORDS.search(part)
                or any(pattern.search(part) for _label, _path, pattern in CATEGORY_PATTERNS)):
            return False
    named = {path for _label, path, pattern in CATEGORY_PATTERNS if pattern.search(title)}
    return all(path == "beauty/tools/brush" or path in _TEXTURE_LEAVES
               or (path.startswith("beauty/makeup/") and not path.startswith("beauty/makeup/nails/"))
               for path in named)


# Direct product nouns, not a general title classifier. These two families
# exposed swapped Stila merchant types in the 2026-09-12 source review. A hybrid
# naming both families keeps its type; shade names like "Blush" are not proof.
_EXPLICIT_PRODUCT_FAMILIES = {
    "eye_liner": re.compile(r"\beye[\s-]*liner\b", re.I),
    "cheek": re.compile(r"\b(?:cheek\s+(?:duo|stick|cream|colou?r|palette)|(?:liquid|powder|cream)\s+blush)\b", re.I),
}
_TYPE_FAMILIES = {
    "beauty/makeup/eye/eyeliner": "eye_liner",
    "beauty/makeup/face/blush": "cheek",
    "beauty/makeup/face/bronzer": "cheek",
}
_AMBIGUOUS_PRODUCT_TYPE_KEYS = frozenset({"lipglossoil", "lipglossliptint"})


# A setting spray is sprayed over the face. Measured 2026-10-10 on holiholic.com's "Setting Spray" shelf:
# it also holds Etude's "Dr. Mascara Fixer", "Bare Edge Brow Fixer", "Pang Pang Haircara Fixer" and
# A'PIEU's "Pro Curling The Black Fixer Mascara" -- fixers for lashes, brows and hair, not for a face.
_SETTING_SPRAY_LEAF = "beauty/makeup/face/setting-spray"
_SETTING_SPRAY_OTHER_AREA = re.compile(r"\b(?:mascaras?|(?:eye)?lash(?:es)?|(?:eye)?brows?|hair\w*|haircara)\b", re.I)
# A shelf that names two products by design: the title picks which one THIS product is, and only when it
# names exactly one of them and nothing else. credobeauty.com ("makeup, face, setting spray & powder") and
# unitedbeautysupply.com ("Setting Spray and Powder") shelve both; until setting spray had a leaf, the
# type read as powder alone and filed the sprays on it.
_COMBINED_SHELF_LEAVES = (frozenset({_SETTING_SPRAY_LEAF, "beauty/makeup/face/powder"}),)


def _title_contradicts_product_type(title: Optional[str], path: str) -> bool:
    if path == _SETTING_SPRAY_LEAF and _SETTING_SPRAY_OTHER_AREA.search(str(title or "")):
        return True
    claimed = _TYPE_FAMILIES.get(path)
    if not claimed:
        return False
    explicit = {family for family, pattern in _EXPLICIT_PRODUCT_FAMILIES.items()
                if pattern.search(str(title or ""))}
    return bool(explicit and claimed not in explicit)


def _pattern_matches(text: Optional[str]) -> int:
    """PR #2158 guard: first-match-wins is not evidence when multiple paths match."""
    from services.pdp_category_classifier import CATEGORY_PATTERNS
    return len({path for _label, path, pattern in CATEGORY_PATTERNS if pattern.search(str(text or ""))})


# MEASURED (host, merchant product_type) -> leaf. Every CATEGORY_PATTERNS entry matches the
# SINGULAR noun, so a merchant filing products under "Cleansers" / "Sheet Masks" / "Serums"
# resolves to nothing -- and one unresolved row blocks its whole curated cohort. This is NOT a
# plural rule: a general singularising door was drafted and reviewed, and it gave confident
# wrong leaves on hosts nobody had read ("Pads" -> toner, "Lash Serums" -> skincare serum,
# "Pet Shampoos" -> shampoo, "Masks & Peels" -> exfoliant). A merchant's shelf name means what
# THAT merchant files under it; it is evidence only where somebody read the shelf.
#
# Each entry below was read product by product on 2026-09-18 over every product on that host
# (eyurs.com 434, ohlolly.com 510, sokoglam.com 567), and every product on the shelf belonged
# to the leaf -- or named a different leaf in its title, which `_resolve_category` checks per
# product before using the entry. Shelves that were NOT one class are deliberately absent:
# eyurs "Cotton Pads" (an accessory the toner pattern catches through "pad"), "Foot Masks"
# (body care), ohlolly "Exfoliator" (body scrubs + facial peel pads), sokoglam "chemical" (an
# azelaic treatment and a tea tree stick, not one class). Re-read 2026-09-22 and also left out:
# eyurs "Exfoliators" (a peel-off mask among the exfoliants) and "Wrinkle Patch" (an acne patch
# among the wrinkle patches). sokoglam "physical" moved IN on 2026-09-23 -- see its own note.
_MEASURED_HOST_PRODUCT_TYPES = {
    "eyurs.com": {
        "sheet masks": "beauty/skincare/treat/mask",
        "masks": "beauty/skincare/treat/mask",
        "eye masks": "beauty/skincare/treat/mask",
        "wash off mask": "beauty/skincare/treat/mask",
        "moisturizers": "beauty/skincare/moisturize/cream",
        "face moisturizers": "beauty/skincare/moisturize/cream",
        "serums": "beauty/skincare/treat/serum",
        "face serums": "beauty/skincare/treat/serum",
        "ampoules": "beauty/skincare/treat/serum",
        "essences": "beauty/skincare/treat/serum",
        "cleansers": "beauty/skincare/cleanse/cleanser",
        "oil cleansers": "beauty/skincare/cleanse/cleanser",
        "gel cleansers": "beauty/skincare/cleanse/cleanser",
        "toners": "beauty/skincare/tone/toner",
        "suncream": "beauty/skincare/sun/sunscreen",
        # Added 2026-09-22 from a re-read of every product on eyurs.com (433). Both types name TWO
        # pattern families, so the evidence policy reads them as ambiguous; every product on each
        # shelf was one class: four rinse-off / clay masks, and three acne patches. (The per-product title
        # check still sets the shelf aside for "Wash Off Pack": "wash" names the cleanser leaf.)
        "masks, exfoliators": "beauty/skincare/treat/mask",
        # treat/treatment, not treat/mask: Google 5976 and Shopify file patches under Acne
        # Treatments. CATEGORY_PATTERNS now agrees (#2250), so this type usually resolves on its own
        # evidence before the shelf is read; the entry stays as the measured record of the shelf.
        "acne pimple patch": "beauty/skincare/treat/treatment",
    },
    "ohlolly.com": {
        # "Wash off mask" is AMBIGUOUS to the regexes (cleanser "wash" + mask); every product
        # on the shelf was a rinse-off mask.
        "wash off mask": "beauty/skincare/treat/mask",
        # Overnight "sleeping packs" are leave-on masks.
        "sleeping pack": "beauty/skincare/treat/mask",
        "sun care": "beauty/skincare/sun/sunscreen",
    },
    "sokoglam.com": {
        "lip balms": "beauty/makeup/lip/balm",
        # 2026-09-22, every product on sokoglam.com (564): nine blemish patches and spot treatments.
        "spot": "beauty/skincare/treat/treatment",
        # Read again 2026-09-23, once an acid/peel pad stopped reading as a toner: all six products
        # on this shelf exfoliate (an AHA-BHA-PHA pad, a peel gel, three gauze peels) except
        # NEOGEN's soothing Real Cica Pad, whose title names the toner leaf and steps the shelf
        # aside. The shelf NAME is still no evidence on its own: the neighbouring "Chemical" shelf
        # holds an azelaic treatment and a tea tree stick, and stays out.
        # KNOWN FORWARD RISK: physical exfoliation is where TOOLS land, and the title check above is
        # a membership test, so a konjac pad or a silicone applicator filed here would be claimed as
        # a topical exfoliant. sokoglam files its tools under their own "Tools" shelf today (8
        # products). A general tools veto was drafted and REJECTED in review: `_title_paths` matches
        # a bare "brush", so it refused "Clay Mask with Applicator Brush" and "Lip Balm with Brush
        # Applicator" on the shelves above -- and an unresolved row stops the whole cohort at the
        # apply gate. Re-read this shelf before trusting it again.
        "physical": "beauty/skincare/treat/exfoliant",
    },
    # Read 2026-09-24, every product on the host (5,120; USD storefront; Japanese beauty). Its types are
    # breadcrumbs ("beauty & personal care / skincare / face masks"): each names two or more pattern
    # families, so the evidence policy reads every one as ambiguous and NO beauty row resolved. Two
    # shelves were one class: 97 masks and sheet packs, and 77 sunscreens. Their titles still step the
    # shelf aside eight times: 5 UV primers/CC/concealer and 2 UV makeup-base gels name another leaf,
    # and a cleansing mud paste names the cleanser. The set detector holds three filled mask rows until
    # someone accepts them: a mask-and-cream SET, a 6-sheet pack (a false positive: "6 Pieces"), and a
    # 20-piece dry compressed sheet you soak in your own lotion (arguably an accessory, not a mask).
    # The shelves read and LEFT OUT, each for rows no title check would catch:
    #   "skincare / cleansers" (203): blotting papers, pore strips, a clay face pack.
    #   "skincare / japanese lotions" (126): toners (化粧水) AND milky emulsions (乳液), two classes.
    #   "skincare / moisturizers" (121): a rice-bran beauty oil and an overnight sleeping pack.
    #   "skincare / facial treatments" (83): serums, beauty oils, emulsions, balms, a massage gel.
    #   "bath & body / body skincare" (189): liquid bandages, an antiseptic ointment, antiperspirants.
    "japanesetaste.com": {
        "beauty & personal care / skincare / face masks": "beauty/skincare/treat/mask",
        "beauty & personal care / skincare / sun care": "beauty/skincare/sun/sunscreen",
    },
    # Read 2026-09-26, every product on the host (181; MineTan's own US store, USD; it also sells the
    # sister brand b.tan). Its types are house shelf names ("Mine Foam") that name no pattern family, so
    # MineTan's self-tanners resolved nowhere and only its bundles and a brush went live. Two shelves
    # were one class: 12 self-tan foams/mousses and 4 self-tan gels ("Hydrating Tan Gelly"). This is
    # the first BODY shelf in the table; none of the 16 titles names another body area, so the
    # non-face veto below changes nothing here. KNOWN LIMIT: that veto is face-shaped, so a future
    # "Self Tan Body Mousse" on these shelves would be set aside (unresolved, never mislabelled); and a
    # title naming no leaf ("Glow Butter") would take the shelf's leaf. Re-read before trusting again.
    # The shelves read and LEFT OUT, each mixing classes:
    #   "Mine Body" (5): a blending brush, an applicator mitt, a tan-remover foam, a body butter and a
    #     gradual tan lotion. The mitt and the butter name no leaf, so the title check could not stop them.
    #   "Mine Face" (5): facial tan mists, bronzing drops, a bronzing serum and highlighter glow drops.
    #     Titles set 4 of 5 aside (toner/serum/highlighter); the shelf would add one row, so not worth it.
    #   "Mine Bronze Babe" (2): a home spray-tan machine kit (equipment; its title names gift-set).
    #   "Mine Pro Mist" (21): salon spray-tan solutions; already resolve (to toner, via "mist") and
    #   are excluded per cohort, so they are not a shelf question.
    # b.tan's own shelves ("b.tan Foam", ...) are a different brand's products; not read for it here.
    "us.shop.minetanbodyskin.com": {
        "mine foam": "beauty/body/tanning",
        "mine tan gellies": "beauty/body/tanning",
    },
    # Read 2026-09-27, every product on the host (435; tarte's own US store). Its brow, lip-plump and
    # sponge shelves name no pattern family ("Eyebrow" is not "brow pencil"), so 13 brow products, 5
    # plumping glosses and 5 sponges/puffs resolved nowhere and the brand's re-run kept none of them.
    #   "Eyebrow" (13): pencils, gels, pomade, mousse, a detailing pen, a brow lift, a pencil-and-gel duo.
    #   "Lip Plump" (6): five plumping glosses / gloss-balms (the copy says "gloss") and one "lip & cheek
    #     shift", a two-area tint that the lip-shelf area veto below sets aside.
    #   "Face Tool" (5): three blending sponges and two powder puffs.
    # The shelves read and LEFT OUT: "Skincare" (14: body butter, rollers, a mitt, self-tan, an eye
    # treatment, face oil), "Freckle" (4 freckle stamps/sticks -- no leaf -- and a pouch), "Setting
    # Spray" (no leaf: category_path_aliases), "Eyelash Curlers" (tools/* are brushes and sponges
    # only), "Blush, Bronzer" / "Blush, Highlighter" (multi-stick and palettes, two classes each).
    "tartecosmetics.com": {
        "eyebrow": "beauty/makeup/eye/brow",
        "lip plump": "beauty/makeup/lip/gloss",
        "face tool": "beauty/tools/sponge",
    },
    # Read 2026-09-27, every product on the host (131; Stila's own US store). Its types are sometimes
    # swapped ("Blush" on an eyeliner, "Eye Liner" on a cheek duo); those rows are REFUSED by
    # `_title_contradicts_product_type` and a shelf never fills a refusal.
    #   "Brows" (2): a brow pencil and a waterproof brow colour.
    #   "Lip Care" (2): a lip balm, and "Buff & Blur Lip Enzyme Exfoliator" -- which the lip shelves'
    #     multi-use rule sets aside for its "&" (the safe side; a lip scrub would otherwise be a balm).
    #     KNOWN FORWARD RISK: a gloss moved onto this shelf ("Plumping Lip Glaze" sits on "Lip Gloss/Oil"
    #     today) names no leaf and would become a balm. Re-read before trusting the shelf again.
    # LEFT OUT: "Complexion" (8: lip & cheek sticks, a sponge, a correcting palette, a primer, a
    # foundation-and-concealer), "Lip Products" (stains, a balm and a bundle).
    "stilacosmetics.com": {
        "brows": "beauty/makeup/eye/brow",
        "lip care": "beauty/makeup/lip/balm",
    },
}


_NON_FACE_TITLE = re.compile(r"\b(?:hair|scalp|body|foot|feet|hands?|nails?|lash(?:es)?|brows?|beard)\b", re.I)


# The non-face body-area rule lives with the taxonomy so every writer shares ONE function: this
# feed, the regex backfill, the seed mirror and merchant sync (services/pdp_category_classifier.py).
from services.pdp_category_classifier import FACE_SKINCARE_LEAF as _FACE_SKINCARE_LEAF  # noqa: E402
from services.pdp_category_classifier import non_face_leaf  # noqa: E402


def _non_face_leaf(path: str, *, title: Optional[str], product_type: Optional[str]) -> Optional[str]:
    return non_face_leaf(path, title, product_type)


def _title_paths(title: Optional[str]) -> set:
    from services.pdp_category_classifier import CATEGORY_PATTERNS
    return {path for _label, path, pattern in CATEGORY_PATTERNS if pattern.search(str(title or ""))}


def _measured_host_type_leaf(*, domain: Optional[str], product_type: Optional[str], title: Optional[str]) -> Optional[str]:
    """The measured leaf for this host's shelf, unless THIS product's title names another leaf."""
    host = _clean_domain(domain or "").lower()
    host = host[4:] if host.startswith("www.") else host
    ptype = " ".join(str(product_type or "").casefold().split())
    leaf = _MEASURED_HOST_PRODUCT_TYPES.get(host, {}).get(ptype)
    if not leaf:
        return None
    named = _title_paths(title)
    # eyurs files "Pyunkang Yul Essence Toner" under "Cleansers": a shelf is evidence about the
    # shelf, and the product's own title outranks it. A title naming no leaf does not.
    if named and leaf not in named:
        return None
    # Measured shelves are FACE (or lip) care, except MineTan's two tanning shelves (whose 16 titles
    # name no other body area, so this veto is inert there). A title naming another body area is a different
    # product the patterns cannot see: measured on eyurs' own "Moisturizers" shelf, a "Hand & Nail
    # Cream" and a "Body Lotion"; a future "Argan Oil Hair Mask" on "Masks" would otherwise become a
    # facial mask, and a "Lip & Body Balm" is not only a lip balm.
    # A brow shelf's own area word is not another area: "tinted brow gel" is what that shelf holds.
    own_area = _SHELF_OWN_AREA.get(leaf)
    text = own_area.sub(" ", str(title or "")) if own_area else str(title or "")
    if _NON_FACE_TITLE.search(text):
        return None
    # A lip shelf holds lip products; one that also names the cheek or eyes is two products in one
    # (tarte's "lip & cheek shift" on its "Lip Plump" shelf). The lip title door's own area rule.
    if leaf.startswith(_LIP_LEAF_PREFIX) and (_LIP_OTHER_AREA.search(str(title or ""))
                                            or _LIP_MULTI_USE.search(str(title or ""))):
        return None  # ...or two lip products ("lip plump & liner")
    # A makeup shelf's tool or accessory is not its formula ("spoolie", "brow shaping razors" on a brow
    # shelf), and a sponge shelf holds sponges and puffs, not every face tool (gua sha, rollers, curlers).
    if leaf.startswith("beauty/makeup/") and _SHELF_TOOL_WORDS.search(str(title or "")):
        return None
    required = _SHELF_REQUIRES.get(leaf)
    if required and not required.search(str(title or "")):
        return None
    return leaf


_SHELF_OWN_AREA = {"beauty/makeup/eye/brow": re.compile(r"\b(?:eye)?brows?\b", re.I)}
_SHELF_TOOL_WORDS = re.compile(r"\b(?:spoolies?|razors?|tweezers?|scissors|stencils?|sharpeners?|brush(?:es)?|"
                               r"curlers?|cases?)\b", re.I)
_SHELF_REQUIRES = {"beauty/tools/sponge": re.compile(r"\b(?:sponges?|puffs?|blenders?)\b", re.I)}


# A MEASURED host whose products carry no usable merchant type (blank, or a house shelf such as
# "Sale" / "new") -- the title's HEAD noun, the last leaf it names, is the product. "Swipe Serum
# Foundation" is a foundation, "BeachPlease Cream Blush" a blush, "SOS Recovery Cream" a cream.
# NOT a general title classifier: only these hosts, only these types, each read product by product;
# on any other host the head noun is no evidence (PR #2158: "Powder Kiss Lipstick", "Strobe Cream").
# Read on the product NAME only: a shade or variant suffix is cut first (" in Michelle (Warm Blush)",
# " - Last Chance Shades", ": Two Colors"), so a shade word is never the head. Refused, and left
# unresolved (review of #2403, each a real or plausible title on these hosts):
#   * no leaf; two leaves ending at the same place; a head outside makeup/skincare (merch: "Blush Tee");
#   * a LIP head: the lip title door owns lip rows, with its accessory / multi-use vetoes and its
#     per-row hold (placed_by_lip_title) -- this rule never places a lip row around that hold;
#   * two products in one name: any second leaf, unless it is a texture word right before the head
#     ("Serum Foundation", "Cream Blush", "Matte Powder Blush" -- not "Foundation & Concealer");
#   * an area the head's leaf does not belong to ("Lash Primer", "Brow Powder", "Body Highlighter",
#     "Lip & Cheek Cream");
#   * sets / bundles / duos / palettes, accessories (case, sharpener, remover, bag, clip), giveaways
#     ("Free Mini ..."), and anything the lip door calls not a product (samples, cards, socks).
_MEASURED_TITLE_HOST_TYPES = {
    # Read 2026-09-27, every product (64). 45 have a blank type: the SOS skincare line, MakeWaves
    # mascara, GoGo eyeshadow, GetSet powders and blush, cream blush / bronzer / contour, SuperDew
    # highlighter; samples, merch (a claw clip, socks, a crewneck, a keychain), a gift card and sets.
    # "Foundations & Concealers" (4) holds a foundation, a concealer, its sample and a tinted SPF --
    # four titles that each name their own leaf.
    # Its lip rows (ShineOn, LipSoftie, OneLiner) are NOT placed here: lip heads are the lip door's.
    "tower28beauty.com": frozenset({"", "foundations & concealers"}),
    # Read 2026-09-27, every product (131). The blank / "Sale" / "new" / "Eye Products" rows are
    # eyeliners, liquid eyeshadows, a primer, a mascara and bundles; the "Hidden" free minis are not
    # on the list.
    "stilacosmetics.com": frozenset({"", "sale", "new", "eye products"}),
}
# Distinct from every other writer's value (see CATEGORY_CONFIDENCE_LIP_TITLE).
CATEGORY_CONFIDENCE_MEASURED_HOST_TITLE = 0.81


# The product name ends where a shade / variant / edition suffix starts.
_TITLE_VARIANT_SUFFIX = re.compile(r"\s+(?:in|-|\u2013|\u2014)\s+|\s*[(:|]")
# Texture words that name a leaf of their own but only describe the head right after them.
_TEXTURE_LEAVES = frozenset({"beauty/skincare/moisturize/cream", "beauty/skincare/treat/serum",
                             "beauty/makeup/face/powder", "beauty/skincare/moisturize/oil"})
_TITLE_AREA_WORDS = {
    "lip": re.compile(r"\blips?\b", re.I),
    "cheek": re.compile(r"\bcheeks?\b", re.I),
    "eye": re.compile(r"\b(?:eyes?|eyelids?|lids?|under[\s-]?eyes?)\b", re.I),
    "lash": re.compile(r"\b(?:eye)?lash(?:es)?\b", re.I),
    "brow": re.compile(r"\b(?:eye)?brows?\b", re.I),
    "other": re.compile(r"\b(?:body|hair|scalp|nails?|cuticles?|hands?|feet|foot|neck|decollet[ae]ge|beard|legs?)\b", re.I),
}
# the areas a head leaf may name; anything else is another product (or two)
_HEAD_AREAS = (
    ("beauty/makeup/eye/mascara", {"lash", "eye"}),
    ("beauty/makeup/eye/brow", {"brow"}),
    ("beauty/makeup/eye/", {"eye"}),
    ("beauty/makeup/face/concealer", {"eye"}),
    ("beauty/makeup/face/", set()),
    ("beauty/skincare/", {"eye"}),
)
_TITLE_ACCESSORY_OR_GIVEAWAY = re.compile(
    r"\b(?:free|gwp|brush(?:es)?|sharpeners?|applicators?|cases?|holders?|pouch(?:es)?|mirrors?|removers?|"
    r"wipes?|organi[sz]ers?|bags?|totes?|clips?|tees?|t-?shirts?|shirts?|hoodies?|crewnecks?|sweatshirts?|"
    r"hats?|caps?|refills?|wands?)\b", re.I)


def _measured_host_title_leaf(*, domain: Optional[str], product_type: Optional[str],
                              title: Optional[str]) -> Optional[str]:
    """The head-noun leaf of this product's name, on a measured title host only; else None."""
    host = _clean_domain(domain or "").lower()
    host = host[4:] if host.startswith("www.") else host
    ptype = " ".join(str(product_type or "").casefold().split())
    if ptype not in _MEASURED_TITLE_HOST_TYPES.get(host, frozenset()):
        return None
    text = " ".join(str(title or "").split())  # see _is_makeup_brush_title: no quadratic split
    if _LIP_SET.search(text) or _LIP_NOT_A_PRODUCT.search(text) or _TITLE_ACCESSORY_OR_GIVEAWAY.search(text):
        return None
    name = _TITLE_VARIANT_SUFFIX.split(text, maxsplit=1)[0]
    from services.pdp_category_classifier import CATEGORY_PATTERNS
    spans: Dict[str, Tuple[int, int]] = {}
    for _label, path, pattern in CATEGORY_PATTERNS:
        for m in pattern.finditer(name):
            if path not in spans or m.end() > spans[path][1]:
                start = m.start(1) if m.lastindex and m.start(1) >= 0 else m.start()
                spans[path] = (start, m.end())
    if not spans:
        return None
    # the leaf whose match ends last; a tie, like a named gift set, is a second leaf and refused below
    head = max(spans, key=lambda path: spans[path][1])
    if not head.startswith(("beauty/makeup/", "beauty/skincare/")) or head.startswith(_LIP_LEAF_PREFIX):
        return None
    head_start = spans[head][0]
    for path, (_start, end) in spans.items():
        if path != head and not (path in _TEXTURE_LEAVES and not name[end:head_start].strip()):
            return None
    allowed = next((areas for prefix, areas in _HEAD_AREAS if head.startswith(prefix)), None)
    if allowed is None:
        return None  # a leaf with no area row (nails, lashes...) is not this rule's to place
    # over the WHOLE title: a suffix can name another area ("Highlighter (Face & Body)")
    if any(pattern.search(text) for area, pattern in _TITLE_AREA_WORDS.items() if area not in allowed):
        return None
    return head


# A lip product's own title, where the merchant type says nothing. Measured 2026-09-23 on the
# UCP-ready Meitu retailers: k-touch.us, belleandblush.com and openthebeauty.com file lipsticks
# under a blank type, "Cosmetics", or a comma tag list ("HERA,Face,Makeup,..."), so every
# "Soft Matte Lipstick" / "Rouge Opulent Lipstick" / "Lip Liner" landed unresolved and blocked
# its whole cohort, while the same stores' mascaras resolved from a clean type.
#
# Deliberately NOT a general title classifier: only a lip leaf, only when the title names that
# ONE leaf and nothing else, and only where the type and the measured shelf left the row
# unresolved. What it refuses stays unresolved -- never a guessed leaf:
#
# OFF unless an operator asks for it on a manual run (`onboard_curated_brands.py
# --lip-title-evidence`). The queue worker, the brand-official lane, the key-retirement script and
# the repair planner all share `_resolve_category`, and none of them sets this: their output is
# byte-identical to before the door existed.
_LIP_TITLE_EVIDENCE: contextvars.ContextVar[bool] = contextvars.ContextVar("lip_title_evidence", default=False)


@contextlib.contextmanager
def lip_title_evidence():
    """Enable `_explicit_lip_title_leaf` for the calls inside this block (and tasks they start)."""
    token = _LIP_TITLE_EVIDENCE.set(True)
    try:
        yield
    finally:
        _LIP_TITLE_EVIDENCE.reset(token)


_LIP_LEAF_PREFIX = "beauty/makeup/lip/"
# Distinct from every other writer's value (0.3/0.7/0.8/0.82/0.85/0.9/0.95), so a stored row this
# door placed can be found by (category_label_source, category_confidence) without re-running it.
# Nothing thresholds on category_confidence.
CATEGORY_CONFIDENCE_LIP_TITLE = 0.78
# two things joined: "Lip & Cheek", "Lipstick & Liner", "Eye and Lip Remover", "Lip/Cheek Tint". Any
# "&", "+" or "and" refuses (a shade like "Rose & Honey" stays unresolved -- the safe side), except a
# "+" after a digit ("SPF 30+"); "/" only beside "lip", because sizes read "3.5g/0.12oz".
_LIP_MULTI_USE = re.compile(r"&|(?<!\d)\+|\band\b|\blips?\s*/|/\s*lips?\b", re.I)
# ...and any other area named ANYWHERE: "Lip Tint & Cheek", "Lip Stain for Cheeks Too", "Lip Liner Eye Pencil"
_LIP_OTHER_AREA = re.compile(r"\b(?:cheeks?|eyes?|eyelids?|face|facial|brows?|cuticles?|nails?|body|hands?|"
                             r"hair|feet|foot)\b", re.I)
# a set, kit or bundle: its own shelf (beauty/sets), whatever lip product is inside it
_LIP_SET = re.compile(r"\b(?:sets?|kits?|bundles?|packs?|combos?|duos?|trios?|quads?|twins?|palettes?|wardrobes?|"
                      r"collections?|gift|sampler|discovery|advent|vault|(?:[2-9]|\d{2,})\s*-?\s*(?:pcs?|pieces?|ea)|"
                      r"\d+\s*x|x\s*\d+)\b", re.I)
# a tool or accessory FOR a lip product, not a lip product
_LIP_ACCESSORY = re.compile(r"\b(?:brush(?:es)?|sharpeners?|applicators?|cases?|holders?|pouch(?:es)?|"
                            r"mirrors?|removers?|wipes?|cleansers?|organi[sz]ers?)\b", re.I)
# not a lip product at all: merch, toys, craft supplies, packaging, displays, samples, other
# audiences, ingestibles, fragrance. Measured by adversarial review of PR #2257.
_LIP_NOT_A_PRODUCT = re.compile(
    r"\b(?:dogs?|cats?|pets?|kids?|girls?|boys?|bab(?:y|ies)|child(?:ren)?|toys?|plush|squishy|pretend|"
    r"charms?|earrings?|jewel(?:ry|lery)|necklaces?|pendants?|candles?|lighters?|socks?|usb|power\s*banks?|"
    r"key\s*rings?|keyrings?|key\s*chains?|keychains?|lanyards?|stickers?|decals?|magnets?|ornaments?|"
    r"empty|base|beeswax|molds?|moulds?|stencils?|dispensers?|displays?|stands?|tubes?|tins?|containers?|"
    r"bottles?|cards?|testers?|samples?|swatch(?:es)?|gumm(?:y|ies)|supplements?|vitamins?|capsules?|"
    r"candy|chocolate|perfume|parfum|eau|cologne|fragrance|"
    # review of #2263 (catch-all types reach the door): merch, media, toys, trade stock
    r"posters?|enamel\s+pins?|tumblers?|mugs?|diffusers?|dolls?|barbie|squish(?:y|ies)|wall\s+art|dvd|books?|"
    r"e-?books?|labels?|mica|glitter\s+pigment|wholesale|bulk|gwp|promo(?:tional)?|starter)\b", re.I)
# "Lip Color" is a family word, not a form: "Glossy Lip Color" is a gloss, "Lip Color Balm" a balm.
_LIP_FAMILY_WORD = re.compile(r"\blip\s+colou?r\b", re.I)
_LIP_FORM_WORD = re.compile(r"\b(?:gloss|glossy|tint|stain|balm|oil|liner|pencil|crayon|butter|serum)\b", re.I)
# Merchant types that say "lip" and nothing more specific. With _GENERIC_PRODUCT_TYPES and a blank
# type, the ONLY types the door accepts: an allowlist, because a type no pattern reads ("Toys",
# "Supplements", "Gift Sets", "Eyes", "Packaging") is still the merchant saying what it is.
_LIP_AREA_TYPES = frozenset({"lip", "lips", "lip makeup", "lip make up", "lip products", "lip product"})
# The neutral types the door accepts. NOT _GENERIC_PRODUCT_TYPES: that set also holds "hair care",
# "face care" and "skin care", which say the product is something other than a lip product.
# thisisbeauty.us (2026-09-23) types all 8,367 products "Misc": a catch-all says as little as a blank.
_LIP_NEUTRAL_TYPES = frozenset({"beauty", "cosmetics", "makeup", "make up", "lip care", "lip treatment",
                                "lip treatments", "lip color", "lip colour",
                                "misc", "miscellaneous", "other", "others", "general", "default"})


def _lip_word_tokens(text: str) -> set:
    return set(re.findall(r"[a-z0-9]+", text.casefold()))


def _explicit_lip_title_leaf(*, product_type: Optional[str], title: Optional[str]) -> Optional[str]:
    """The lip leaf a title names outright, or None. The caller asks only for UNRESOLVED rows."""
    if not _LIP_TITLE_EVIDENCE.get():
        return None
    text = str(title or "")
    named = _title_paths(text)
    if len(named) != 1:
        # Zero is no evidence; two is PR #2158's ambiguity ("Powder Kiss Lipstick" names a powder).
        return None
    leaf = next(iter(named))
    if not leaf.startswith(_LIP_LEAF_PREFIX):
        return None
    if any(p.search(text) for p in (_LIP_MULTI_USE, _LIP_OTHER_AREA, _LIP_SET, _LIP_ACCESSORY,
                                     _LIP_NOT_A_PRODUCT, _NON_FACE_TITLE)):
        return None
    if leaf == "beauty/makeup/lip/lipstick" and _LIP_FAMILY_WORD.search(text) and _LIP_FORM_WORD.search(text):
        return None
    ptype = " ".join(str(product_type or "").casefold().split())
    if not ptype or ptype in _LIP_NEUTRAL_TYPES or ptype in _LIP_AREA_TYPES:
        return leaf
    if "," not in ptype:
        return None
    # A comma tag list IS the merchant's own classification. Every tag must be generic, the lip
    # area, or a word the title already says (the brand, a finish) -- so "Lip,Cheek",
    # "Makeup,Lips,Sets" and "HERA,Face,Makeup,Cushion" say something the title does not -- and at
    # least one tag must name the lip area.
    title_words = _lip_word_tokens(text)
    tags = [t.strip() for t in ptype.split(",") if t.strip()]
    lip_tag = False
    for tag in tags:
        if tag in _LIP_AREA_TYPES or _title_paths(tag) == {leaf}:
            lip_tag = True
        elif tag in _LIP_NEUTRAL_TYPES:
            continue
        elif not _lip_word_tokens(tag) or not _lip_word_tokens(tag) <= title_words:
            return None
    return leaf if lip_tag else None


# A false-lash or nail product's own title, where the merchant type says nothing. Measured 2026-09-26:
# kissusa.com types 1,395 of 1,464 products "Physical Products" (39 more blank), so its crawl kept 3
# rows although 505 titles name exactly one lash or nail leaf; its stockists shelve the same products
# as "EYELASHES", "Eye lashes", "COSMETICS - EYELASH - CLUSTERS", "NAILS", "Nail Styling",
# "COSMETICS - NAILS - PRESS ONS" -- a lash/nail shelf no pattern reads as one leaf.
#
# The lip door's shape, for the lash and nail leaves only (the lip door stays lip-only): OFF unless a
# job asks (options.lash_nail_title_evidence), fills ONLY a row everything else left unresolved, and
# only when the title names ONE leaf and that leaf is a lash or nail leaf. Every row it places is held
# per row by the detectors (placed_by_lash_nail_title) until someone accepts that row.
_LASH_NAIL_TITLE_EVIDENCE: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "lash_nail_title_evidence", default=False)


@contextlib.contextmanager
def lash_nail_title_evidence():
    """Enable `_explicit_lash_nail_title_leaf` for the calls inside this block (and tasks they start)."""
    token = _LASH_NAIL_TITLE_EVIDENCE.set(True)
    try:
        yield
    finally:
        _LASH_NAIL_TITLE_EVIDENCE.reset(token)


# Distinct from every other writer's value (see CATEGORY_CONFIDENCE_LIP_TITLE), so a stored row this door
# placed can be found without re-running it.
CATEGORY_CONFIDENCE_LASH_NAIL_TITLE = 0.76
_LASH_TITLE_LEAVES = frozenset({"beauty/makeup/eye/false-lashes"})
_NAIL_TITLE_LEAVES = frozenset({"beauty/makeup/nails/nail-polish", "beauty/makeup/nails/nail-polish-remover",
                                "beauty/makeup/nails/cuticle-oil", "beauty/makeup/nails/press-on-nails"})
# Types that say nothing about which product this is. NOT the lip door's "lip care"/"lip color" types.
# "accessories": shopstarbeauty.com shelves every KISS lash and nail there; the title must still name
# one lash/nail leaf, which no hair clip or bag does.
_LASH_NAIL_NEUTRAL_TAGS = frozenset({"beauty", "cosmetics", "cosmetic", "makeup", "make up", "misc",
                                     "miscellaneous", "other", "others", "general", "default",
                                     "physical products", "physical product", "accessories"})
# Shelf words that name the lash (or nail) area and nothing more specific than the title does.
_LASH_AREA_TAGS = frozenset({"lash", "lashes", "eyelash", "eyelashes", "eye lash", "eye lashes",
                             "false lashes", "false eyelashes", "strip lashes", "strips", "lash clusters",
                             "clusters"})
_NAIL_AREA_TAGS = frozenset({"nail", "nails", "nail care", "nail styling", "fake nails", "false nails",
                             "press ons", "press on nails", "press-on nails", "artificial nails", "manicure",
                             "pedicure", "nail polish", "nail color", "nail colour", "nail lacquer"})
# One shelf, written as a path or a list: "COSMETICS - EYELASH - CLUSTERS", "Makeup > Nails", "Nails, KISS".
_LASH_NAIL_TAG_SPLIT = re.compile(r"\s+-\s+|\s*[>,|/]\s*")
# Not a lash or nail product at all: merch, cards, stickers, toys, displays, samples, other audiences
# (the lip door's list, less "base" -- a base coat IS nail polish -- and less the lip-only words) --
# and a TOOL or ACCESSORY for one, which the lip door refuses through _LIP_ACCESSORY: "Individual Lash
# Tweezers", "Press On Nails Cuticle Pusher", "Nail Polish Rack Holder" (review of #2377). It costs
# KISS lashes that list an applicator in the box ("... | 12 Lash Clusters, With Mini Applicator"):
# they stay unresolved, the safe side.
_LASH_NAIL_NOT_A_PRODUCT = re.compile(
    r"\b(?:gift\s*cards?|e-?gift|cards?|posters?|stickers?|decals?|charms?|earrings?|jewel(?:ry|lery)|"
    r"necklaces?|pendants?|key\s*rings?|keyrings?|key\s*chains?|keychains?|lanyards?|magnets?|ornaments?|"
    r"toys?|plush|dolls?|squish(?:y|ies)|pretend|puzzles?|games?|costumes?|socks?|wall\s+art|art\s+prints?|"
    r"dogs?|cats?|pets?|candles?|tumblers?|mugs?|books?|e-?books?|dvd|"
    r"displays?|stands?|empty|testers?|samples?|swatch(?:es)?|wholesale|bulk|gwp|promo(?:tional)?|"
    r"bags?|pouch(?:es)?|cases?|organi[sz]ers?|storage|box(?:es)?|trays?|racks?|holders?|openers?|"
    r"tweezers?|applicators?|pushers?|files?|buffers?|clippers?|cutters?|brush(?:es)?|prep|"
    r"lamps?|dryers?|drills?)\b", re.I)


def _explicit_lash_nail_title_leaf(*, product_type: Optional[str], title: Optional[str]) -> Optional[str]:
    """The lash or nail leaf a title names outright, or None. The caller asks only for UNRESOLVED rows."""
    if not _LASH_NAIL_TITLE_EVIDENCE.get():
        return None
    text = str(title or "")
    named = _title_paths(text)
    if len(named) != 1:
        # Zero is no evidence; two is ambiguity ("Magnetic Eyeliner & False Eyelashes", "... Collection").
        return None
    leaf = next(iter(named))
    if leaf in _LASH_TITLE_LEAVES:
        area = _LASH_AREA_TAGS
    elif leaf in _NAIL_TITLE_LEAVES:
        area = _NAIL_AREA_TAGS
    else:
        return None
    if _LASH_NAIL_NOT_A_PRODUCT.search(text):
        return None
    ptype = " ".join(str(product_type or "").casefold().split())
    if not ptype:
        return leaf
    # Every tag of the merchant's shelf must be neutral or name THIS leaf's area: "Eye lashes" vouches for
    # a lash, never for a nail; "BROW & LASH TOOLS", "NAIL TOOLS", "Hair Care" say something else.
    tags = [t for t in _LASH_NAIL_TAG_SPLIT.split(ptype) if t]
    if tags and all(t in _LASH_NAIL_NEUTRAL_TAGS or t in area for t in tags):
        return leaf
    return None


def _resolve_category(*, product_type: Optional[str], title: Optional[str], flag_path: str,
                      domain: Optional[str] = None) -> Tuple[str, float]:
    """Resolve, then refuse a FACE skincare leaf for a product that names another body area.

    The body-area rule changes an answer ONLY where the answer is a face skincare leaf AND the
    title or merchant type names a non-face area (and not the face as well). Every other row --
    every face-titled row -- is returned exactly as before.

    A caller's own taxonomy LEAF is not second-guessed here: the repair planner passes a stored
    leaf as the flag, and a stored leaf may be challenged only with fresh merchant evidence
    (--review-existing-leaves, which passes a coarse flag instead). Curated feeds pass a coarse
    flag ("beauty"), so every face leaf the resolver derives itself is checked.
    """
    path, confidence = _resolve_category_unguarded(
        product_type=product_type, title=title, flag_path=flag_path, domain=domain)
    from services.pdp_category_classifier import CATEGORY_PATTERNS
    flag = str(flag_path or "").strip().strip("/").lower()
    if path == flag and flag in {leaf for _label, leaf, _pattern in CATEGORY_PATTERNS}:
        return path, confidence
    from services.category_path_aliases import resolve
    leaf = _non_face_leaf(resolve(path) or "", title=title, product_type=product_type)
    if leaf is None:
        return path, confidence
    if not leaf:
        return "", CATEGORY_CONFIDENCE_FEED_DEFAULT
    # The leaf now rests on the product's own area word, not on the merchant's type alone.
    return leaf, min(confidence, CATEGORY_CONFIDENCE_EXPLICIT_TITLE)


def _resolve_category_unguarded(*, product_type: Optional[str], title: Optional[str], flag_path: str,
                                domain: Optional[str] = None) -> Tuple[str, float]:
    """Evidence policy, then -- ONLY where it left the product unresolved -- a measured host shelf,
    then an explicit lip title (`_explicit_lip_title_leaf`), then an explicit lash/nail title
    (`_explicit_lash_nail_title_leaf`).

    Structural no-regression: anything the evidence policy resolves to a leaf, and every
    deliberate refusal (""), is returned untouched. The measured shelf fills only a coarse
    FALLBACK -- which includes the evidence policy's multi-pattern AMBIGUITY return: ohlolly's
    "Wash Off Mask" matches both the cleanser ("wash") and mask patterns, and the table names
    it because every product on that shelf was read. It never fills a refusal or a caller's leaf.
    """
    path, confidence = _resolve_category_by_evidence(product_type=product_type, title=title, flag_path=flag_path)
    from services.category_path_aliases import resolve
    if path == "" or resolve(path):
        return path, confidence
    fallback = str(flag_path or "").strip().strip("/").lower()
    if fallback and fallback.split("/", 1)[0] != "beauty":
        return path, confidence
    leaf = _measured_host_type_leaf(domain=domain, product_type=product_type, title=title)
    if leaf:
        return leaf, CATEGORY_CONFIDENCE_MEASURED_HOST_TYPE
    leaf = _measured_host_title_leaf(domain=domain, product_type=product_type, title=title)
    if leaf:
        return leaf, CATEGORY_CONFIDENCE_MEASURED_HOST_TITLE
    # Last, and on the same terms as the shelf: fills only what everything above left unresolved.
    leaf = _explicit_lip_title_leaf(product_type=product_type, title=title)
    if leaf:
        return leaf, CATEGORY_CONFIDENCE_LIP_TITLE
    leaf = _explicit_lash_nail_title_leaf(product_type=product_type, title=title)
    if leaf:
        return leaf, CATEGORY_CONFIDENCE_LASH_NAIL_TITLE
    return path, confidence


def _resolve_category_by_evidence(*, product_type: Optional[str], title: Optional[str], flag_path: str) -> Tuple[str, float]:
    """One category evidence policy shared by feed mapping and repair planning.

    Retain #2158's ambiguity guard, conservative marketing-title behavior and
    confidence semantics. Deliberately replace its storefront-area veto: strong
    per-product evidence can disagree with a COARSE storefront shelf (MISSHA tools
    were all labelled skincare). An explicit taxonomy leaf remains protected.
    Title evidence has only two narrow doors here: a tool noun suffix without formula
    context, and an explicit lip-oil title refining the measured generic lip shelves.
    (A third, `_explicit_lip_title_leaf`, runs after the measured shelf, on unresolved rows only.)
    """
    from services.pdp_category_classifier import CATEGORY_PATTERNS, classify
    fallback = str(flag_path or "").strip().strip("/").lower()
    if fallback and fallback.split("/", 1)[0] != "beauty":
        return flag_path, CATEGORY_CONFIDENCE_FEED_DEFAULT
    leaves = {path for _label, path, _pattern in CATEGORY_PATTERNS}

    def accept(path: str, confidence: float) -> Tuple[str, float]:
        if _title_contradicts_product_type(title, path):
            # Refuse even when the caller supplied that same leaf: conflicting
            # product evidence cannot become a resolved category by repetition.
            return "", CATEGORY_CONFIDENCE_FEED_DEFAULT
        if not path.startswith("beauty/") or (fallback in leaves and path != fallback):
            return fallback, CATEGORY_CONFIDENCE_FEED_DEFAULT
        return path, confidence

    ptype = " ".join(str(product_type or "").casefold().split())
    if re.sub(r"[^a-z0-9]", "", ptype) in _AMBIGUOUS_PRODUCT_TYPE_KEYS:
        # Shared regexes currently recognize only one half of these merchant
        # alternatives. One regex hit is not a unique class assertion.
        return "", CATEGORY_CONFIDENCE_FEED_DEFAULT
    # Exact merchant product types observed on the primary retailer feed. Lip
    # Scrub belongs to the existing lip-care leaf despite also matching the
    # generic exfoliator regex; Sun Protection names sunscreen, not an SPF claim
    # on an unrelated formula. No marketing-title or storefront substitution.
    explicit_types = {
        "lip scrub": "beauty/makeup/lip/balm",
        "sun protection": "beauty/skincare/sun/sunscreen",
        # Foot Care is a body-care treatment, not footwear or a facial peel.
        "foot care": "beauty/body/care",
    }
    if ptype in explicit_types:
        return accept(explicit_types[ptype], CATEGORY_CONFIDENCE_MERCHANT_TYPE)
    matches = _pattern_matches(product_type)
    if matches > 1:
        # "Concealer Brush" matches the concealer AND brush patterns, so the type alone is not one
        # class -- but its head noun is the tool, and a title that also ENDS in brush agrees.
        # Measured 2026-09-27 on tartecosmetics.com: 33 brushes on "Concealer Brush", "Eyeshadow
        # Brush" and "Eyeliner Brush" shelves resolved nowhere. Its "Foundation Brush" shelf also
        # holds sponges and a twist tool; their titles do not end in brush, so they stay unresolved.
        # Only a brush FOR makeup: every other family the type names must be a makeup leaf. sokoglam's
        # "Hair Brush" names haircare -- a hairbrush is not a beauty/tools/brush (makeup applicators).
        # Nail types sit under beauty/makeup/nails but a "Gel Polish Brush" is not a makeup tool.
        from services.pdp_category_classifier import CATEGORY_PATTERNS
        type_uses = {path for _label, path, pattern in CATEGORY_PATTERNS
                     if pattern.search(str(product_type or "")) and path != "beauty/tools/brush"}
        if (_TOOL_NOUN_SUFFIX.search(ptype) and _is_makeup_brush_title(str(title or ""))
                and all(path.startswith("beauty/makeup/") and not path.startswith("beauty/makeup/nails/")
                        for path in type_uses)):
            return accept("beauty/tools/brush", CATEGORY_CONFIDENCE_EXPLICIT_TITLE)
        shelf = {path for _label, path, pattern in CATEGORY_PATTERNS if pattern.search(str(product_type or ""))}
        named = _title_paths(title)
        if shelf in _COMBINED_SHELF_LEAVES and len(named) == 1 and named <= shelf:
            return accept(next(iter(named)), CATEGORY_CONFIDENCE_EXPLICIT_TITLE)
        return fallback, CATEGORY_CONFIDENCE_FEED_DEFAULT
    if ptype in _GENERIC_LIP_TYPES:
        title_hit = classify(title)
        if title_hit and title_hit[1] == "beauty/makeup/lip/oil" and _pattern_matches(title) == 1:
            return accept(title_hit[1], CATEGORY_CONFIDENCE_EXPLICIT_TITLE)
    if ptype not in _GENERIC_PRODUCT_TYPES and matches == 1:
        hit = classify(product_type)
        if hit:
            return accept(hit[1], CATEGORY_CONFIDENCE_MERCHANT_TYPE)
    # An unclassifiable multi-use label (e.g. Lip & Cheek) is not permission to
    # choose a competing category from its title.
    if matches == 0 and re.search(r"[&/]|\band\b", ptype):
        return fallback, CATEGORY_CONFIDENCE_FEED_DEFAULT
    if _TOOL_NOUN_SUFFIX.search(str(title or "")) and not _TOOL_FORMULA_CONTEXT.search(str(title or "")):
        return accept("beauty/tools/brush", CATEGORY_CONFIDENCE_EXPLICIT_TITLE)
    # Powder Kiss Liquid Lipcolour / Slim Stick and Strobe Cream are explicitly
    # not classified from their marketing titles; shallow backfill owns the residue.
    return fallback, CATEGORY_CONFIDENCE_FEED_DEFAULT


def product_category_path(*, title: Optional[str], product_type: Optional[str], fallback: str,
                          domain: Optional[str] = None) -> str:
    """Path-only wrapper for the review-only repair planner; no second classifier."""
    path = _resolve_category(product_type=product_type, title=title, flag_path=fallback, domain=domain)[0]
    # The repair planner expresses abstention as no change, never a blank-path
    # update. Fresh mapping consumes the unresolved result directly above.
    return path or fallback


def shopify_product_to_record(
    product: Dict[str, Any],
    *,
    domain: str,
    category_path: str,
    brand_override: Optional[str] = None,
    emit_variants: bool = False,
    # Separate switch for NATIVE (un-folded) multi-variant rows, so the fold lane's
    # `emit_variants=True` cannot silently start emitting variants for the rows it
    # did not fold — `--base-listings-only` runs (MAC) stay byte-identical.
    emit_native_variants: bool = False,
    currency: Optional[str] = None,
    source_role: str = "brand_official",
    retailer_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Map one Shopify `/products.json` product → a Path-C validated record
    (`{pdp, offers}`). Returns None if it lacks a title/handle (not actionable).
    The brand storefront is the authoritative PDP, so the offer is brand-direct
    and carries the variant barcode (GTIN) when present."""
    if not isinstance(product, dict):
        return None
    if source_role not in {"brand_official", "retailer"}:
        raise ValueError("source_role must be brand_official or retailer")
    host = _clean_domain(domain)
    title = str(product.get("title") or "").strip()
    handle = str(product.get("handle") or "").strip()
    if not title or not handle:
        return None
    # NOT `brand_override or vendor`: that renames across a brand boundary. See
    # `resolve_record_brand` — the override normalises spelling, it does not relabel
    # a sibling brand the storefront names itself.
    if source_role == "retailer":
        vendor = str(product.get("vendor") or "").strip()
        vendor_key = "".join(c for c in vendor.casefold() if c.isalnum())
        # The STORE's label, regional prefix skipped: the first label of us.mcobeauty.com is "us",
        # under the floor, so `MCoBeauty US` passed as maker evidence on the brand's own store --
        # 495 offers (and 323 on us.inikaorganic.com) written as retailer stock, measured 2026-09-26,
        # while dhccare.com's `DHC Care` was refused. A brand's own store runs as brand_official.
        host_label = _store_host_label(host)
        if (not _looks_like_a_brand_name(vendor) or not vendor_key
                or (len(host_label) >= 3 and host_label in vendor_key)):
            raise ValueError(f"{host}: retailer_maker_unproven: vendor {vendor!r} is not maker evidence")
        # Only exact normalized maker equivalence permits a spelling override --
        # or a MEASURED rename in RETAILER_BRAND_SPELLINGS. Brand-direct store/supplier-code
        # heuristics cannot label retailer stock.
        override_key = "".join(c for c in str(brand_override or "").casefold() if c.isalnum())
        # The VENDOR's own listed family decides its spelling -- a normalisation of the maker the
        # retailer named, never a relabel into whatever family --brand belongs to. It does not
        # depend on --brand at all, so a run without one converges too.
        vendor_family = _retailer_brand_family(vendor_key)
        if vendor_family is not None:
            brand = RETAILER_BRAND_CANONICAL[vendor_family]
        else:
            brand = brand_override if override_key == vendor_key else vendor
    else:
        brand, _brand_reason = resolve_record_brand(product.get("vendor"), brand_override, host)
        # The same one-spelling-per-family rule for brand-official stores: a manyo.us re-run with
        # --brand "Manyo" would otherwise write "Manyo" and split from its own 78 "Ma:nyo" rows.
        official_family = _retailer_brand_family("".join(c for c in str(brand or "").casefold() if c.isalnum()))
        if official_family is not None:
            brand = RETAILER_BRAND_CANONICAL[official_family]
    brand = str(brand or "").strip()
    if not brand:
        return None
    variants = product.get("variants")
    variants = variants if isinstance(variants, list) else []
    # Pick the first sellable variant — priced at or above MIN_SELLABLE_PRICE.
    # Gift-with-purchase and other $0/unpriced items have no purchasable offer —
    # drop the product entirely so it never enters the commerce index (these were
    # landing as junk PDPs/seeds, the offers_skipped noise seen onboarding kosas).
    # The floor exists because "positive" was not enough: stilacosmetics.com lists a
    # "Free Travel … (TikTok Shop)" promo at $0.01, which cleared `p > 0`, ingested
    # as a canonical anchor and served (measured 2026-09-05). A token price is a
    # promo mechanic, not an offer.
    variant = None
    price = None
    for v in variants:
        p = _to_float((v or {}).get("price"))
        if p is not None and p >= MIN_SELLABLE_PRICE:
            variant, price = v, p
            break
    if variant is None:
        return None
    image = _first(product.get("images")) or {}
    if not str(image.get("src") or "").strip():
        # No product-level image: a variant's own swatch is a real image of this
        # product and is better than publishing a row the scorer counts as
        # imageless. Only a fallback — a product image always wins.
        for _v in variants:
            _fi = _v.get("featured_image") if isinstance(_v, dict) else None
            _src = (
                str((_fi or {}).get("src") or "").strip() if isinstance(_fi, dict)
                else str((_v or {}).get("image_src") or "").strip()
            )
            if _src:
                image = {"src": _src}
                break
    # A line with multiple native variants is not one physical item, just as a
    # folded shade line is not. Never promote the first variant's GTIN to Tier-0
    # PDP identity: array order and stock/price changes must not change identity.
    # A sole native variant may provide it; all native SKU barcodes remain intact.
    barcode = (
        str(variants[0].get("barcode") or "").strip() or None
        if not product.get(FOLDED_INTO_KEY) and len(variants) == 1
        and isinstance(variants[0], dict)
        else None
    )
    # Every sellable variant: the ingest writes one SKU + offer per entry beside
    # the canonical SKU, so a folded shade line (see fold_shade_listings) keeps
    # its purchasable SKUs. Since #2120 a single-variant product emits its one
    # variant too -- the canonical SKU's source_variant_id is a storage token the
    # gateway refuses to spend against, so the merchant's own id has to ride here
    # for #2113 (SKU) and #2123 (seed) to keep it.
    sellable = [
        v for v in variants
        if isinstance(v, dict) and (_to_float(v.get("price")) or 0.0) >= MIN_SELLABLE_PRICE
    ]
    pdp_variants: List[Dict[str, Any]] = []
    # OPT-IN. Emitting variants writes one extra SKU + offer per variant downstream,
    # which changes recall fan-out, offer aggregation and INCI attachment for EVERY
    # row a caller ingests — so it never fires unless a caller asked for it.
    #
    # WHY THE FOLD IS NO LONGER THE ONLY GATE. This used to additionally require
    # `product.get(FOLDED_INTO_KEY)`, i.e. only a listing the shade-fold had just
    # BUILT could carry variants. That silently excluded every storefront that
    # publishes its shades natively, as one product with many variants — which is
    # the normal Shopify shape, not the exception. Measured on flowerbeauty.com
    # 2026-09-07: `/products.json` serves 49 products, 29 of them multi-variant,
    # carrying 185 real numeric Shopify variant ids; `fold_shade_listings` folds
    # ZERO of them (bases=0, shades=0), so the gate refused all 185 and the brand
    # ingested 49 SKUs whose `source_variant_id` was the product key. #2113 taught
    # ingestion to stop DISCARDING real variant ids; this is the other half — the
    # feed never put them in the record for it to keep.
    #
    # The two lanes admit on different rules, deliberately. A FOLDED row's variants
    # were assembled by us out of separate per-shade listings and a variant there
    # may legitimately carry a synthesised `<handle>:<i>` id (see the fallback
    # below), which is display data the shade selector needs; ingestion's own
    # provenance check decides whether it may also be sold. A NATIVE row has no
    # such excuse: its ids come straight off the merchant's own feed, so anything
    # `variant_identity` cannot positively place as merchant-issued is dropped
    # here rather than carried forward as a decoy that looks purchasable.
    native = not product.get(FOLDED_INTO_KEY)
    emit_here = emit_native_variants if native else emit_variants
    if emit_here and len(sellable) >= 1:
        seen_ids: set = set()
        base_option_name = _base_option_name(product)
        for i, v in enumerate(sellable):
            vid = str(v.get("id") or v.get("variant_id") or f"{handle}:{i}").strip()
            if vid in seen_ids:
                continue
            if native and variant_id_provenance(
                vid,
                product_id=product.get("id"),
                handle=handle,
            ) != MERCHANT_ISSUED:
                continue
            seen_ids.add(vid)
            # option1 is the merchant's own shade value and outranks a name derived
            # from the title suffix; `featured_image` is where a real Shopify variant
            # carries its swatch (`image_src` is set only by the fold).
            featured = v.get("featured_image")
            featured_src = str((featured or {}).get("src") or "").strip() if isinstance(featured, dict) else ""
            pdp_variants.append({
                "variant_id": vid,
                "sku": str(v.get("sku") or "").strip() or None,
                "barcode": str(v.get("barcode") or "").strip() or None,
                "title": str(v.get("option1") or v.get("title") or "").strip() or None,
                "option_name": _variant_option_name(v, base_option_name),
                "price": _to_float(v.get("price")),
                "in_stock": v.get("available") if isinstance(v.get("available"), bool) else None,
                "image_url": (
                    featured_src
                    or str(v.get("image_src") or "").strip()
                    or str(image.get("src") or "").strip()
                    or None
                ),
                "source_handle": str(v.get(FOLDED_FROM_KEY) or "").strip() or None,
            })
    raw_tags = product.get("tags")
    tags = (
        raw_tags
        if isinstance(raw_tags, list)
        else [t.strip() for t in str(raw_tags or "").split(",") if t.strip()]
    )
    canonical_url = f"https://{host}/products/{handle}"
    category_input_path = category_path
    category_path, category_confidence = _resolve_category(
        title=title, product_type=product.get("product_type"), flag_path=category_path, domain=domain,
    )
    from services.category_path_aliases import resolve
    category_path = resolve(category_path)
    category_resolution_status = "resolved" if category_path else "unresolved"
    return {
        "pdp": {
            "brand": brand,
            "product_name": title,
            "category_path": category_path,
            "category_confidence": category_confidence,
            "category_resolution_status": category_resolution_status,
            "category_input_path": category_input_path,
            "category_source_product_type": str(product.get("product_type") or "").strip() or None,
            # Brand-authored body copy when present (it becomes the row's
            # description and feeds the lifecycle candidate gate + taxonomy
            # extractors); product_type alone otherwise. Rows minted without
            # body copy land 'draft' and rely on the description backfill /
            # LLM enrichment lane to promote.
            "attribute_summary": (
                body_html_to_text(product.get("body_html"))
                or str(product.get("product_type") or "").strip()
            ),
            "barcode": barcode,  # real GTIN when the brand fills it — strongest deposit basis
            "source_domain": host,
            "source_role": source_role,
            "tags": tags,
            # Brand-official INCI when the storefront lists it under an Ingredients
            # heading (many don't — None then, ingest skips it). brand_official is
            # the top INCI authority tier (ADR-001) so it outranks reseller lists.
            "raw_inci": inci_from_body_html(product.get("body_html")),
            "inci_source": "reseller_listing" if source_role == "retailer" else "brand_official",
            # Shopify /products.json exposes no review aggregate — ratings stay null
            # on this lane (captured on the retailer-PDP lane instead).
            "rating_value": None,
            "rating_count": None,
            # The STOREFRONT's own currency, from /meta.json. Omitted (None) rather than
            # defaulted here: direct ingestion also refuses an unknown currency.
            "currency": currency,
            "variants": pdp_variants,
        },
        "offers": [
            {
                # The MERCHANT is the storefront, which is not always the brand: once
                # `resolve_record_brand` can keep a sibling brand's own vendor (APIEU on
                # misshaus.com), `brand` names the maker and the override names the shop.
                # Identical on every single-brand feed, where the two are the same string.
                "merchant_inferred": (
                    str(retailer_name or "").strip() or host
                    if source_role == "retailer"
                    else str(brand_override or "").strip() or brand
                ),
                # Seller identity must be host-based in retailer mode even when two
                # storefronts use the same friendly name. Official-mode IDs are untouched.
                "seller_domain": host if source_role == "retailer" else None,
                "canonical_url": canonical_url,
                "destination_url": canonical_url,
                "image_url": str(image.get("src") or "").strip(),
                "price": price,
                "in_stock": variant.get("available") if isinstance(variant.get("available"), bool) else None,
                "validated_at": "shopify_products_json",
            }
        ],
    }


# Some storefronts (maccosmetics.com, measured 2026-09-04: 1,366 of a 1,500-product
# sample) publish EVERY shade as its own single-variant product — "Retro Matte
# Lipstick - Ruby Woo" beside the base "Retro Matte Lipstick". The Path-C plan keys
# PDPs on (brand, title), so ingesting that feed as-is mints one PDP per shade:
# ~1,900 near-duplicates for one brand.
#
# `fold_shade_listings` FOLDS those shade rows into the base listing's variants
# instead of dropping them: the base keeps one PDP, and every shade becomes a
# variant of it (title = shade name, its own sku / barcode / price / image), so
# the purchasable SKUs survive. Measured on the MAC feed, every base row is
# itself a single-variant PARENT STUB (variants[0].option1 == title, sku P2000_*):
# that stub variant is replaced by the shades, never kept beside them. A base
# that already carries real variants keeps them and gains the folded shades.
#
# Titles are compared through `normalize_title` (the same normaliser
# `make_content_key` uses downstream), because the feed is not case- or
# punctuation-stable across a line: stila lists "HUGE™ Extreme Lash Mascara" beside
# "Huge™ Extreme Lash Mascara - Intense Black", and "Heaven's" beside "Heaven’s".
# Shade names may themselves contain hyphens ("Lady-Be-Good", "Brick-O-La"), so
# every " - " split point is tried, longest base first.
_SHADE_SEP = " - "
FOLDED_FROM_KEY = "_folded_from_handle"
FOLDED_INTO_KEY = "_folded_shades"
# A suffix that names a merchandising state, not a shade. tarte sells "<line> - <X>
# charm" as separate $10 accessories and stila suffixes "- Last Chance"/"- Limited
# Edition" onto whole palettes; folding those makes an accessory a "shade" of the
# product it accessorises and destroys its own PDP. Measured 2026-09-05: 9 such
# false folds across the five cached feeds, 0 legitimate shades excluded.
_NON_SHADE_SUFFIX_RE = re.compile(
    r"(?i)\b(charm|last chance|limited edition|refill|travel size|mini|set|kit|bundle|gift card|sample)\b"
)
# A shade of a product costs what the product costs. A folded row priced far from its
# base is a different item wearing a similar name.
_FOLD_PRICE_RATIO = 1.5


def _shade_bases(title: str) -> List[str]:
    """Every '<base>' a '<base> - <shade>' title could be split into, longest
    base first, so 'Lip Pencil - Brick-O-La' yields ['Lip Pencil - Brick-O',
    'Lip Pencil']. Only ' - ' (space-hyphen-space) is a separator."""
    parts = title.split(_SHADE_SEP)
    return [_SHADE_SEP.join(parts[:i]).strip() for i in range(len(parts) - 1, 0, -1)]


def _image_srcs(product: Dict[str, Any]) -> List[str]:
    """Every usable image URL on a Shopify product row, in feed order."""
    out: List[str] = []
    for img in (product or {}).get("images") or []:
        src = str((img or {}).get("src") or "").strip() if isinstance(img, dict) else str(img or "").strip()
        if src:
            out.append(src)
    return out


def _first_price(product: Dict[str, Any]) -> Optional[float]:
    for v in (product or {}).get("variants") or []:
        p = _to_float((v or {}).get("price")) if isinstance(v, dict) else None
        if p is not None and p > 0:
            return p
    return None


def _fold_refused(base: Dict[str, Any], shade: Dict[str, Any], suffix: str) -> Optional[str]:
    """Why this row must NOT be folded into that base, or None to fold."""
    if _NON_SHADE_SUFFIX_RE.search(suffix or ""):
        return "non_shade_suffix"
    bp, sp = _first_price(base), _first_price(shade)
    if bp and sp and (max(bp, sp) / min(bp, sp)) > _FOLD_PRICE_RATIO:
        return "price_mismatch"
    return None


def _is_stub_variant(product: Dict[str, Any]) -> bool:
    """A single placeholder variant that names no shade: its option/title is the
    product's own title or Shopify's 'Default Title'. MAC's P2000_ parents are
    this shape; a real single-shade product ('Ruby Woo' as option1) is not."""
    variants = (product or {}).get("variants") or []
    if len(variants) != 1:
        return False
    v = variants[0] or {}
    title = str((product or {}).get("title") or "").strip()
    label = str(v.get("option1") or v.get("title") or "").strip()
    return label in ("", "Default Title", title)


def fold_shade_listings(products: List[Dict[str, Any]]) -> "Tuple[List[Dict[str, Any]], Dict[str, Any]]":
    """Pure. Fold single-variant `<base> - <shade>` rows into the variants of
    the base row (matched through normalize_title). Returns (products, report):
    the base rows now carry the shades as variants (a stub placeholder variant
    is replaced; real variants are kept and extended), the shade rows are
    removed, order is otherwise preserved, and multi-variant rows are never
    folded — a suffixed multi-variant title is a distinct line, not a shade.
    `report` names what happened so the caller can print it: bases folded,
    shade rows folded, stub variants replaced, and every folded handle by base."""
    from services.catalog_identity import normalize_title

    by_norm: Dict[str, Dict[str, Any]] = {}
    for p in products:
        key = normalize_title(str((p or {}).get("title") or ""))
        if key and key not in by_norm:
            by_norm[key] = p
    folded_into: Dict[int, List[Dict[str, Any]]] = {}  # id(base) -> shade rows
    shade_of: Dict[int, Dict[str, Any]] = {}            # id(shade row) -> base
    refusals: List[Dict[str, str]] = []
    for p in products:
        title = str((p or {}).get("title") or "").strip()
        variants = (p or {}).get("variants") or []
        if len(variants) > 1:
            continue
        for base_title in _shade_bases(title):
            base = by_norm.get(normalize_title(base_title)) if base_title else None
            if base is None or base is p:
                continue
            suffix = title[len(base_title):].lstrip(" -").strip()
            refused = _fold_refused(base, p, suffix)
            if refused:
                refusals.append({"handle": str(p.get("handle") or ""), "title": title, "reason": refused})
                break
            folded_into.setdefault(id(base), []).append(p)
            shade_of[id(p)] = base
            break
    report: Dict[str, Any] = {"bases": 0, "shades": 0, "stubs_replaced": 0, "images_adopted": 0,
                             "folded": {}, "refused": refusals}
    out: List[Dict[str, Any]] = []
    for p in products:
        if id(p) in shade_of:
            continue
        shades = folded_into.get(id(p))
        if not shades:
            out.append(p)
            continue
        base_title = str(p.get("title") or "").strip()
        base = dict(p)
        own = [] if _is_stub_variant(p) else [
            dict(v, title=str(v.get("title") or v.get("option1") or "").strip())
            for v in (p.get("variants") or []) if isinstance(v, dict)
        ]
        if not own and (p.get("variants") or []):
            report["stubs_replaced"] += 1
        new_variants: List[Dict[str, Any]] = list(own)
        handles: List[str] = []
        for s in shades:
            shade_title = str(s.get("title") or "").strip()
            for bt in _shade_bases(shade_title):
                if normalize_title(bt) == normalize_title(base_title):
                    shade_name = shade_title[len(bt):].lstrip(" -").strip() or shade_title
                    break
            else:
                shade_name = shade_title
            sv = dict((s.get("variants") or [{}])[0] or {})
            # The shade row's OWN option1 is the merchant's shade value and wins:
            # stila's "Calligraphy Lip Stain - Last Chance Shade" carries
            # option1 "Elizabeth (Pinky Nude)", and taking the title suffix minted a
            # phantom second SKU for the same merchant code.
            own_label = str(sv.get("option1") or "").strip()
            if own_label and own_label.lower() not in ("default title",):
                shade_name = own_label
            sv["title"] = shade_name
            sv["option1"] = shade_name
            sv.setdefault("id", s.get("id"))
            img = _first(s.get("images")) or {}
            if img.get("src"):
                sv["image_src"] = str(img.get("src"))
            sv[FOLDED_FROM_KEY] = str(s.get("handle") or "")
            new_variants.append(sv)
            handles.append(str(s.get("handle") or ""))
        base["variants"] = new_variants
        base[FOLDED_INTO_KEY] = len(shades)
        # A parent stub carries no images of its own — measured on maccosmetics.com
        # 2026-09-05, 106 of 109 folded bases have an EMPTY `images` list while the
        # shade rows carry the swatches. The product row is what the quality scorer
        # reads (`_extract_main_image`), so a base left imageless forfeits the whole
        # images component: MAC scored 66.7 against a 71.4 gate and every row was
        # blocked `low_quality`. Adopt the folded shades' images when the base has
        # none; a base with its own images keeps them untouched.
        if not _image_srcs(p):
            adopted: List[Dict[str, Any]] = []
            seen_src: set = set()
            for s in shades:
                for src in _image_srcs(s):
                    if src not in seen_src:
                        seen_src.add(src)
                        adopted.append({"src": src})
            if adopted:
                base["images"] = adopted
                report["images_adopted"] += 1
        report["bases"] += 1
        report["shades"] += len(shades)
        report["folded"][str(p.get("handle") or base_title)] = handles
        out.append(base)
    return out, report


def drop_shade_listings(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compatibility name for `fold_shade_listings`: same collapse, report dropped."""
    return fold_shade_listings(products)[0]


class CurrencyNotProven(RuntimeError):
    """A storefront's own currency could not be proven, or is not the one asked for.

    Both the enumerator and direct ingestion refuse unknown currency. A blocked
    or unreadable /meta.json remains a failed observation; prices are never
    assigned USD because the source did not prove its currency.

    NEVER a conversion. This raises; it does not rewrite an amount into another
    currency. See services/region_pricing (ADR-024 commitment 5).
    """


def _vendor_token(value: Optional[str]) -> str:
    """Comparison form of a Shopify `vendor` string: casefolded, whitespace collapsed.

    Deliberately NOT `catalog_identity.normalize_title`: that one is tuned for product
    titles (it keeps hyphens because 'Anti-Aging Serum' is a real distinction) and a
    vendor field is a short label where the only variation worth absorbing is case and
    stray spacing. Anything looser would be a hazard on a multi-brand retailer feed,
    which is the whole reason this exists -- cocomo.sg lists 224 vendors, and a filter
    that matched approximately would quietly pull in a neighbour brand's products under
    the target brand's name.
    """
    return " ".join(str(value or "").split()).casefold()


def filter_products_by_vendor(
    products: List[Dict[str, Any]], vendors: "Sequence[str]"
) -> List[Dict[str, Any]]:
    """Keep only the products whose Shopify `vendor` is one of `vendors`. Pure.

    A multi-brand RETAILER feed is not a brand feed. `records_for_brand`'s existing
    `brand` argument is a brand_override -- it RENAMES every product it sees -- so
    pointing it at cocomo.sg with brand='VELY VELY' would not select VELY VELY's 24
    products, it would relabel all 1,000 of that retailer's products (MEDICUBE, ANUA,
    BEAUTY OF JOSEON, ...) as VELY VELY and deposit them as brand-official anchors.
    Selection and renaming are different operations and this is the selecting one.

    Matching is exact after `_vendor_token` normalisation. An empty/None `vendors`
    returns the list unchanged -- "no filter asked for", which is what every existing
    single-brand caller means.
    """
    wanted = {_vendor_token(v) for v in (vendors or []) if str(v or "").strip()}
    if not wanted:
        return list(products)
    return [
        p for p in products
        if isinstance(p, dict) and _vendor_token(p.get("vendor")) in wanted
    ]


class CuratedRecordBatch(list):
    """Validated records and their own scan outcome, safe across concurrent calls."""
    def __init__(self, records: list, *, crawl_report: Optional[Dict[str, Any]] = None):
        super().__init__(records)
        self.crawl_report = dict(crawl_report) if crawl_report is not None else None


async def records_for_brand(
    *,
    domain: str,
    category_path: str,
    brand: Optional[str] = None,
    collection_handles: Optional[Sequence[str]] = None,
    brand_by_vendor: Optional[Mapping[str, str]] = None,
    max_products: int = 500,
    base_listings_only: bool = False,
    # Emit the merchant's OWN variants for products that are natively multi-variant
    # (the normal Shopify shape). Off by default: it adds one SKU + one offer per
    # variant, which moves recall fan-out and offer aggregation for every row the
    # caller ingests. `base_listings_only` implies it for the rows the fold builds.
    emit_real_variants: bool = False,
    only_vendors: Optional[Sequence[str]] = None,
    require_currency: Optional[str] = None,
    source_role: str = "brand_official",
    retailer_name: Optional[str] = None,
    max_scan_products: int = 10000,
    enrich_missing_inci: bool = False,
    max_pdp_inci_fetches: int = 300,
    enrich_missing_gtin: bool = False,
    max_pdp_identity_fetches: int = 100,
    # 0.0 since the shared politeness gate owns pacing. This ad-hoc sleep predates it and now
    # STACKS on top: every INCI fetch already waits its per-host interval, so a 0.3s sleep on
    # each of 300 fetches added ~90s of pure duplication. Left as a parameter rather than deleted
    # so a caller that wants extra slack on a specific brand can still ask for it.
    pdp_delay_s: float = 0.0,
    pdp_inci_budget_s: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Fetch a curated brand's storefront and return Path-C validated records.

    When `enrich_missing_inci` is set, records whose body_html carried no INCI get
    a SECOND, polite try: fetch the product's own PDP and recover the metafield /
    accordion INCI via `fetch_pdp_inci` (the cohort keeps INCI out of body_html).
    Additive — body_html INCI stays the first try and is never overwritten here;
    the fetch is capped, delayed, and best-effort (a miss leaves raw_inci None).

    `enrich_missing_gtin` optionally recovers validated missing variant barcodes
    from identity-matched product .js responses, before folding. The product-attempt
    budget is `max_pdp_identity_fetches`; batch crawl_report.gtin_recovery records
    attempts, successes, failures, capped products and actual HTTP requests.

    `source_role="retailer"` uses storefront seller identity and reseller INCI authority,
    and refuses unproven currency. Official-mode identity stays backward compatible.

    `only_vendors` selects a subset of a MULTI-BRAND retailer feed by Shopify `vendor`
    (see `filter_products_by_vendor`) — the selecting operation, as distinct from
    `brand`, which renames. Applied BEFORE the shade fold, so a fold never matches a
    base across a brand boundary.

    `require_currency` (an ISO-4217 code) refuses the whole brand with
    `CurrencyNotProven` unless the storefront's own `/meta.json` proves that currency.
    All runs require a proven currency; this option additionally asserts the
    exact requested code. No amount is converted or assigned a default currency.
    """
    if source_role not in {"brand_official", "retailer"}:
        raise ValueError("source_role must be brand_official or retailer")
    if source_role == "retailer" and (
        not isinstance(only_vendors, (list, tuple)) or not only_vendors
        or any(not isinstance(v, str) or not v.strip() for v in only_vendors)
    ):
        raise ValueError("retailer onboarding requires explicit nonempty only_vendors maker selection")
    if not isinstance(enrich_missing_gtin, bool):
        raise ValueError("enrich_missing_gtin must be a boolean")
    if type(max_pdp_identity_fetches) is not int or max_pdp_identity_fetches < 0:
        raise ValueError("max_pdp_identity_fetches must be a nonnegative integer")
    fetch_options: Dict[str, Any] = {"max_products": max_products}
    if only_vendors is not None or source_role == "retailer":
        fetch_options.update(only_vendors=only_vendors, max_scan_products=max_scan_products)
    if collection_handles:  # NOT `collections`: that name is the stdlib module this function uses
        products = await fetch_shopify_collections(domain, collection_handles, only_vendors=only_vendors,
                                                   max_products=max_products, max_scan_products=max_scan_products)
    else:
        products = await fetch_shopify_products(domain, **fetch_options)
    crawl_report = getattr(products, "crawl_report", None)
    # Compatibility/debug only; callers must use the returned batch's own report.
    records_for_brand.last_crawl_report = crawl_report  # type: ignore[attr-defined]
    # ONCE per brand, not per product: it is one storefront-wide setting and a per-product fetch
    # would multiply outbound requests by the catalogue size against a single host.
    locale = await fetch_shopify_shop_locale(domain)
    if not _ISO_CURRENCY.fullmatch(str(locale.get("currency") or "")):
        raise CurrencyNotProven(f"{domain}: storefront currency is unproven; expected {require_currency or 'an explicit currency'}; refusing ingestion")
    if require_currency:
        expected = str(require_currency).strip().upper()
        actual = locale.get("currency")
        if actual != expected:
            # BEFORE the records are built, so a refused brand cannot half-ingest.
            raise CurrencyNotProven(
                f"{domain}: expected currency {expected}, storefront /meta.json proved "
                f"{actual or 'nothing'}. Prices cannot be claimed as {expected}. "
                f"If /meta.json was unreadable, retry: a 429 bot-check "
                f"page reads exactly like a missing file. If {actual or 'the proven value'} "
                f"is genuinely right, pass --require-currency {actual or '<code>'} instead."
            )
    if only_vendors is not None:
        # A filter that was ASKED FOR but normalises to nothing is refused, not skipped.
        # `--only-vendor "$VENDOR"` with the variable unset, or a jsonl row
        # `"only_vendors": [""]`, would otherwise pass an empty set to the filter, which
        # returns the whole feed — and the brand override then relabels every product of a
        # 224-vendor retailer as the target brand, signalled only by a "1000 -> 1000" line.
        wanted = [v for v in only_vendors if str(v or "").strip()]
        if not wanted:
            raise ValueError(
                f"{domain}: --only-vendor was given but every entry is blank "
                f"({list(only_vendors)!r}). Name the vendor, or drop the flag to ingest "
                f"the whole feed deliberately."
            )
        only_vendors = wanted
        before = (getattr(products, "crawl_report", None) or {}).get("scanned_products", len(products))
        products = filter_products_by_vendor(products, only_vendors)
        if not products:
            # LOUD, not empty. An unmatched vendor filter otherwise reports the same
            # "0 products" a non-Shopify storefront does, and the operator reads a typo
            # ("Vely Vely " with a stray character) as "this brand has nothing here".
            raise ValueError(
                f"{domain}: --only-vendor {list(only_vendors)!r} matched 0 of {before} "
                f"products. Check the spelling against the feed's own `vendor` values."
            )
        records_for_brand.last_vendor_filter_report = {  # type: ignore[attr-defined]
            "vendors": list(only_vendors), "before": before, "after": len(products),
        }
    # Gifts go BEFORE GTIN recovery and shade folding, so a gift costs no PDP fetch. Recorded, never silent.
    gifts_dropped: List[Dict[str, Any]] = []
    kept_products = []
    for p in products:
        gift = gift_with_purchase_reason(p)
        if gift:
            gifts_dropped.append({"handle": str(p.get("handle") or "")[:200], "title": str(p.get("title") or "")[:200],
                                  "reason": gift[:120]})
        else:
            kept_products.append(p)
    if gifts_dropped:
        products = kept_products
    identity_report = None
    if enrich_missing_gtin:
        products, identity_report = await recover_missing_variant_gtins(
            products, domain=domain, max_fetches=max_pdp_identity_fetches)
    # A brand-family storefront (misshaus.com: 89 MISSHA + 17 APIEU + 7 CHOGONGJIN)
    # is not visibly different from a single-brand one until something counts the
    # vendors. Computed from the SAME resolver the record builder uses, so the report
    # cannot drift from the decision it describes.
    brand_census: Dict[str, Dict[str, Any]] = {}
    for _p in products:
        if not isinstance(_p, dict):
            continue
        _v = str(_p.get("vendor") or "").strip()
        _resolved, _why = resolve_record_brand(_v, brand, domain)
        _slot = brand_census.setdefault(
            _v or "(no vendor)", {"count": 0, "resolved_brand": _resolved, "reason": _why}
        )
        _slot["count"] += 1
    records_for_brand.last_brand_census = {  # type: ignore[attr-defined]
        "brand_override": brand,
        "vendors": brand_census,
        "kept_vendor_count": sum(
            v["count"] for v in brand_census.values() if v["reason"] == "vendor_disagrees"
        ),
    }
    if base_listings_only:
        products, fold_report = fold_shade_listings(products)
        records_for_brand.last_fold_report = fold_report  # type: ignore[attr-defined]
    records: List[Dict[str, Any]] = []
    pairs: List[Dict[str, Any]] = []  # (product, record) needing a PDP INCI try
    for p in products:
        rec = shopify_product_to_record(
            # brand_by_vendor (multi-brand retailer cohorts): each vendor's OWN canonical spelling, applied
            # by the same resolve_record_brand rule a single-brand job's `brand` gets.
            p, domain=domain, category_path=category_path,
            brand_override=(brand_by_vendor or {}).get(_vendor_token(p.get("vendor")), brand),
            emit_variants=base_listings_only,
            emit_native_variants=emit_real_variants,
            currency=locale.get("currency"),
            source_role=source_role, retailer_name=retailer_name,
        )
        if not rec:
            continue
        records.append(rec)
        if enrich_missing_inci and not (rec.get("pdp") or {}).get("raw_inci"):
            handle = str((p or {}).get("handle") or "").strip()
            if handle:
                pairs.append({"handle": handle, "rec": rec})
    inci_report: Optional[Dict[str, Any]] = None
    if enrich_missing_inci and pairs:
        # A wall-clock budget as well as a count: parsing a PDP for INCI is CPU-bound and scales with the page
        # (measured 2026-09-25: 2.2 s per ~2 MB luxiface.com page), so 300 fetches ran one drain stage past its
        # 3600 s task timeout -- twice -- with nothing logged. Stop early, and say so, rather than be killed.
        timeout = httpx.Timeout(15.0, connect=5.0)
        headers = {"User-Agent": _UA, "Accept": "text/html"}
        started, attempted, found, stopped = time.monotonic(), 0, 0, False
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout, headers=headers) as client:
            for i, pair in enumerate(pairs[:max_pdp_inci_fetches]):
                if pdp_inci_budget_s is not None and time.monotonic() - started >= pdp_inci_budget_s:
                    stopped = True
                    break
                attempted += 1
                inci = await fetch_pdp_inci(domain, pair["handle"], client=client)
                if inci:
                    pair["rec"]["pdp"]["raw_inci"] = inci
                    found += 1
                if pdp_delay_s and i + 1 < min(len(pairs), max_pdp_inci_fetches):
                    await asyncio.sleep(pdp_delay_s)
        inci_report = {"candidates": len(pairs), "cap": max_pdp_inci_fetches, "attempted": attempted,
                       "found": found, "stopped_on_budget": stopped,
                       "seconds": round(time.monotonic() - started, 1)}
    # ONE spelling per brand. misshaus.com publishes both `APIEU` (16 products) and
    # `Apieu` (1); kept verbatim they are two brands to every consumer that groups by
    # the brand string, which splits a brand's catalogue for exactly the reason this
    # fix exists. Fold each `_brand_key` group onto its most common raw spelling —
    # a no-op for the override groups, whose members already share one string.
    spellings: Dict[str, "collections.Counter[str]"] = {}
    for rec in records:
        b = str((rec.get("pdp") or {}).get("brand") or "")
        if b:
            spellings.setdefault(_brand_key(b), collections.Counter())[b] += 1
    canonical = {
        k: c.most_common(1)[0][0] for k, c in spellings.items() if len(c) > 1
    }
    if canonical:
        for rec in records:
            pdp = rec.get("pdp") or {}
            b = str(pdp.get("brand") or "")
            want = canonical.get(_brand_key(b))
            if want and want != b:
                pdp["brand"] = want
        records_for_brand.last_brand_spelling_folds = canonical  # type: ignore[attr-defined]
    else:
        records_for_brand.last_brand_spelling_folds = {}  # type: ignore[attr-defined]
    report = {**crawl_report, "emitted_records": len(records),
              "gift_items_dropped": len(gifts_dropped),
              "gift_items_dropped_sample": gifts_dropped[:50]} if crawl_report is not None else None
    if report is not None and inci_report is not None:
        report["inci_enrichment"] = inci_report
    if report is not None and identity_report is not None:
        report["gtin_recovery"] = identity_report
    storefront = shop_storefront_identity(domain)
    if report is not None and storefront is not None:
        report["storefront"] = storefront
    return CuratedRecordBatch(records, crawl_report=report)
