"""One stage of one (brand, retailer) ingest job: dry run, or apply + verify.

The unattended form of what an operator did by hand on 2026-09-23 (Meitu lip wave): crawl, narrow,
plan, run the guards, read every row, apply only a clean cohort through the apply gate, then read
the written rows back. Every stage writes one `retailer_ingest_runs` row, including the ones that
end early, so "why did store X not land?" is answered by the ledger, not by log archaeology.

Policy (Peng, 2026-09-23): a dry run with zero BLOCK flags is applied automatically; any BLOCK flag
holds the job until someone approves it (optionally excluding handles or accepting flag keys).
The apply stage re-crawls and RE-RUNS every check before writing, so a store that changed between
its dry run and its apply cannot slip a new row past the review.

Applies at different hosts run in parallel lanes; only the catalog write (apply_ingest_plan) is
serial, under db.retailer_ingest.catalog_write_lock. Every run records how long each phase took
(checks.timings), so the lock wait and the write can be told apart from the crawl.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from db import retailer_ingest as ledger
from db.retailer_ingest import CatalogWriteLockBusy, CatalogWriteLockUnavailable
from services.region_pricing import ACQUISITION_MARKETS
from services.retailer_ingest import detectors
from utils.logger import logger  # prod keeps this logger's output; plain module loggers' INFO is dropped

DRY_RUN = "dry_run"
APPLY = "apply"


class _Stop(Exception):
    """End this stage early with a job transition and a recorded outcome."""

    def __init__(self, outcome: str, status: str, reason: str, *, next_run_at: Optional[datetime] = None,
                 count_attempt: bool = False, checks: Optional[Dict[str, Any]] = None):
        super().__init__(reason)
        self.outcome, self.status, self.reason = outcome, status, reason
        self.next_run_at, self.count_attempt = next_run_at, count_attempt
        self.checks = checks  # what the stage had measured when it stopped, so the run records it


_OPTION_TYPES = {
    "vendors": list, "require_currency": str, "category_path": str, "only_category": str,
    "only_resolved_category": bool, "lip_title_evidence": bool, "lash_nail_title_evidence": bool,
    "exclude_handles": list,
    # Reviewer decision (Peng 2026-09-25): a bundle of different products the store filed on a
    # single-product shelf ("Cologne & Hand Cream Duo" under fragrance/perfume) is RE-FILED to the
    # gift-set shelf, not dropped -- shoppers look for gift sets; the harm was the shelf. These handles
    # are filed under REFILE_SETS_LEAF before any check runs.
    "refile_to_sets": list,
    "accepted_flags": list,
    # Accept every same_key_other_listing flag of this job at once (COCODOR raised 89): a reviewer who has
    # read the listings left out -- each is still recorded as a flag on the run.
    "accept_listing_collisions": bool,
    "max_scan_products": int, "max_products": int,
    "max_pdp_identity_fetches": int, "max_pdp_inci_fetches": int, "retailer_name": str, "notes": str,
    # "storefront" (default: crawl the retailer's /products.json), "affiliate_feed" (the network's
    # product datafeed; services/retailer_ingest/affiliate_feed.py) -- for stores that block crawlers --
    # or "shopify_markets" (services/retailer_ingest/shopify_markets.py: USD SIBLING offers, read inside
    # a US-localized session whose /cart.js proves USD, for products a base-currency crawl of the same
    # storefront already wrote; multi-market storefronts ADR Phase 2). Since 2026-10-10 (Peng: "Build the
    # SG currency support") its options.market may be SG too: SGD siblings from an SG-localized session.
    "source": str, "feed": dict,
    # Storefront crawls only (2026-10-10): refuse the crawl (status `nothing`) unless the store's own
    # /meta.json ships_to_countries names the job's market. The crawl records ships_to on every run but
    # has never gated on it, so a USD-base retailer that does not ship to the US would land as a US seller.
    # Opt-in, so no existing cohort changes: set it on the base crawl that precedes an SG capture.
    "require_ships_to_market": bool,
    # Whose store this is: "retailer" (default) or "brand_official" (the brand's own storefront, the
    # ADR-001 canonical anchor). services.catalog_onboard_worker.normalize_curated_brand_payload owns
    # the allowed values and the retailer_name rule; enqueue runs that same normalization.
    "source_role": str,
    # One crawl for MANY brands at one retailer: the job's `brand` is only a label, every product keeps
    # its own vendor as its brand (no override), and `vendors` names every brand the cohort selects.
    # 2026-09-24: a (brand, store) cohort re-crawls the whole store twice (dry run + apply), so 16 brands
    # at perfumania.com were 32 full crawls of one store; as one multi_brand cohort they are 2.
    "multi_brand": bool,
    # A store too large for /products.json (> 100 pages of 250: page 101 answers HTTP 400) is crawled
    # through the named brand collections instead (/collections/<handle>/products.json). The vendor
    # filter still applies inside them, and the crawl is complete for THOSE collections, not the store.
    "collections": list,
    # multi_brand only, REQUIRED there: {vendor: canonical brand spelling} for every vendor. Without an
    # override each row keeps the STORE's spelling, and normalize_brand keeps punctuation, so "Dr. Jart+"
    # at one store and "Dr.Jart+" everywhere else become two brands (review of #2301).
    "brands": dict,
    # The buyer market this cohort's offers are priced FOR (ISO-3166-1 alpha-2; absent = DEFAULT_MARKET).
    # It decides the currency the store must prove (require_currency defaults to the market's) and what
    # the readback checks every offer is stamped. Multi-market storefronts ADR, Phase 1.
    "market": str,
    # A brand's own store re-run under its canonical spelling (Tarte, not "Tarte Cosmetics"): the spelling
    # the store's OLD rows were written under. After a verified apply the drain retires those old keys itself
    # (scripts/retire_superseded_brand_keys.py, the same code the CLI runs) -- only where the new key is live
    # on this store AND, when the old row serves, the new one serves too; everything else keeps its old row.
    # The dry run records how many old rows are live on the store (checks.stale_brand_retire_preview).
    "retire_stale_brand": str,
}
SOURCES = ("storefront", "affiliate_feed", "shopify_markets")
MARKETS_SOURCE = "shopify_markets"
DEFAULT_MARKET = "US"
#: The markets a job may name. Every offer this lane writes is stamped catalog_offers.market = the
#: job's market and must be priced in that market's currency (ingestion._build_offer_inserts(market=),
#: region_pricing.require_market_currency): the declared-destination semantics ADR-024 reserves for a
#: writer that declares one. normalize_curated_brand_payload accepts exactly this list from this lane.
INGEST_MARKETS = ("US", "AU", "JP", "SG")
#: The ingest markets that are SERVED, not acquired: a job's rows must read back serving-eligible. SG
#: (Peng 2026-09-29): a live serving region (PIVOTA_SERVING_PRICING_REGIONS = US,SG on web; Meitu's merchant
#: list crawled SG stores). Its rows are written like every other market's: catalog_offers.market 'SG' priced
#: in SGD, and the seed in the "US" partition (ingestion.SEED_PARTITION_MARKET) -- where SG buyers already
#: find SGD seeds by currency (external_seed_search serving currency, #2389). Only the offer stamp is new:
#: jsmbeauty.sg / makeupforever.sg, written before this, carry market 'US' with SGD (INSERT-only column).
SERVED_INGEST_MARKETS = tuple(m for m in INGEST_MARKETS if m not in ACQUISITION_MARKETS)
#: Multi-market storefronts ADR Phase 2 (approved by Peng 2026-09-26): ACQUISITION markets. A job for
#: one crawls the storefront's base currency (AUD/JPY) and stores its rows truthfully (market 'AU',
#: currency 'AUD'); they are NOT served, because serving gates on the offer's currency against the
#: served regions (index_pipeline_state has_serving_region_offer, PIVOTA_SERVING_PRICING_REGIONS = US,SG):
#: index_pipeline_state blocks a product priced only here as no_us_offer whatever
#: ENABLE_KBEAUTY_AGENT_DECISION_GATES says (_acquisition_only_priced), and the agent-decision gate does too
#: when it is on. They become servable only when a USD sibling
#: lands (a source=shopify_markets job) or when the market itself is served (the ADR's Phase 3, a config
#: change plus the seed-partition decision -- never this list alone).
#: (region_pricing.ACQUISITION_MARKETS, imported above: index_pipeline_state reads the same list.)
_ISO_ALPHA2 = re.compile(r"[A-Z]{2}")
#: Tier B (storefront_tier_b) needs the store's /meta.json name to start with the brand. A brand shorter
#: than this (letters and digits) is too common a prefix to be evidence ("ZA Cosmetics" may be anyone's),
#: so such a brand needs Tier A or a human, as before.
TIER_B_MIN_BRAND_CHARS = 3
#: The one shelf a reviewed set is re-filed to: canonical in both taxonomies (pivota-backend
#: category_path_aliases, PIVOTA-Agent beautyTaxonomy.js `gift_set`).
REFILE_SETS_LEAF = "beauty/sets/gift-set"
#: A reviewer's re-file writes this confidence, distinct from every other writer's value (0.3/0.7/0.78/
#: 0.8/0.82/0.85/0.9/0.95), so a stored row placed this way can be found without the ledger.
CATEGORY_CONFIDENCE_REVIEW_REFILE = 0.74
#: The flags a re-file answers: the reviewer decided the row IS a set, so "a set on a single-product
#: shelf" and "the title names another shelf" (its contents) are resolved. Every other flag still runs
#: -- on the row as the STORE filed it too, since the lip rules key on the store's shelf.
REFILE_RESOLVES_RULES = frozenset({"set_filed_as_single_product", "title_contradicts_category"})

_handle_key = ledger.handle_key
MAX_PDP_IDENTITY_FETCHES = 300
# PDP INCI enrichment in an unattended stage: the worker's default (300 fetches, no time limit) ran luxiface.com
# past the 3600 s task timeout twice (2.2 s CPU per ~2 MB page). The drain fetches at most this many by default
# (options.max_pdp_inci_fetches overrides, 0 = none, at most MAX_PDP_INCI_FETCHES) and stops at the budget.
DRAIN_PDP_INCI_FETCHES = 60
MAX_PDP_INCI_FETCHES = 300
DRAIN_PDP_INCI_BUDGET_S = 900
# The catalog write lock (db.retailer_ingest.catalog_write_lock): an apply that has crawled and checked
# waits at most WRITE_LOCK_WAIT_S for another apply's write to finish, polling every WRITE_LOCK_POLL_S.
# On timeout it writes NOTHING and goes back to apply_due, retried after WRITE_LOCK_BUSY_RETRY_S without
# spending an attempt (a busy lock is not the store's failure). The retry re-crawls and re-checks, as
# every apply does.
WRITE_LOCK_WAIT_S = 600
WRITE_LOCK_POLL_S = 5
WRITE_LOCK_BUSY_RETRY_S = 120
# The write must finish inside the execution: the drain passes the seconds left in its task (the same
# figure its lease is computed from), and the lock wait ends WRITE_MARGIN_S before that deadline. With
# less than that left, the apply writes nothing and goes back to apply_due (a fresh execution retries
# it with the whole task ahead of it) -- a write the task timeout kills is a partial cohort. A first
# value, not a measurement: tune it from checks.timings.write_s.
WRITE_MARGIN_S = 900
# Outcomes that end an apply before its write with nothing written, retried quietly. After
# WRITE_LOCK_STARVED_AFTER of them in a row the job is HELD (the held alert fires; approve re-queues it)
# with a WARNING: a lock stuck on a dead holder, or an apply whose crawl always eats its write margin,
# must reach a human instead of retrying unseen forever.
WRITE_LOCK_RETRY_OUTCOMES = ("write_lock_busy", "write_lock_unavailable")
WRITE_LOCK_STARVED_AFTER = 12
# Integer options where 0 is meaningful ("fetch none"); every other integer option must be >= 1.
_ZERO_ALLOWED_INT_OPTIONS = frozenset({"max_pdp_inci_fetches"})


def validate_options(options: Dict[str, Any]) -> Dict[str, Any]:
    """The ONE validator for a job's options, used by the enqueue script and again at execution
    (a row written by any other path is checked before it can crawl). Raises ValueError.

    `category_path` must be coarse (beauty, or one level under it): it is the fallback for every
    product the merchant type leaves unresolved, so a leaf here ("beauty/makeup/lip/lipstick")
    would file every untyped product in the cohort under that leaf with no review."""
    if not isinstance(options, dict):
        raise ValueError("options must be an object")
    unknown = set(options) - set(_OPTION_TYPES)
    if unknown:
        raise ValueError(f"unknown options {sorted(unknown)}")
    for key in [k for k, v in options.items() if v is None and k != "vendors"]:
        del options[key]  # a null option is an absent one (approve() merges lists into them)
    for key, value in options.items():
        want = _OPTION_TYPES[key]
        floor = 0 if key in _ZERO_ALLOWED_INT_OPTIONS else 1
        if want is int and (isinstance(value, bool) or not isinstance(value, int) or value < floor):
            raise ValueError(f"options.{key} must be a {'nonnegative' if floor == 0 else 'positive'} integer")
        if want is not int and not isinstance(value, want):
            raise ValueError(f"options.{key} must be {want.__name__}")
        if want is list and not all(isinstance(v, str) and v.strip() for v in value):
            raise ValueError(f"options.{key} must be a list of non-empty strings")
    if not options.get("vendors"):
        raise ValueError("options.vendors is required for a retailer cohort")
    source = options.get("source") or "storefront"
    if source not in SOURCES:
        raise ValueError(f"options.source must be one of {list(SOURCES)}")
    if options.get("multi_brand") and (options.get("source_role", "retailer") != "retailer"
                                       or source not in ("storefront", MARKETS_SOURCE)):
        # A brand's own store is one brand (its domain must prove it); a feed maps one retailer listing.
        # A shopify_markets capture of a multi_brand retailer cohort (2026-10-10, SG only: below) prices
        # the rows that cohort's storefront crawl wrote, selected by the same vendors and brands.
        raise ValueError("options.multi_brand supports only a retailer storefront cohort (or its "
                         "shopify_markets capture)")
    if "retire_stale_brand" in options and (options.get("source_role") != "brand_official"
                                            or source != "storefront" or options.get("multi_brand")):
        # The retire derives each old key from (old spelling, title) and matches it to THIS store's rows: a
        # brand's own storefront, one brand, crawled here -- the case the CLI was built and measured for.
        raise ValueError("options.retire_stale_brand supports only a brand_official storefront cohort")
    if "retire_stale_brand" in options and not str(options["retire_stale_brand"]).strip():
        raise ValueError("options.retire_stale_brand must name the old spelling")
    if options.get("multi_brand") or "brands" in options:
        brands = options.get("brands")
        if not options.get("multi_brand"):
            raise ValueError("options.brands is only meaningful with options.multi_brand")
        from services.curated_brand_feed import RETAILER_BRAND_CANONICAL, _retailer_brand_family, _vendor_token as fold
        if not isinstance(brands, dict) or not all(isinstance(k, str) and k.strip() and isinstance(v, str) and v.strip()
                                                   for k, v in brands.items()):
            raise ValueError("options.brands must map every vendor to its canonical brand spelling")
        missing = sorted({fold(v) for v in options["vendors"]} - {fold(k) for k in brands})
        extra = sorted({fold(k) for k in brands} - {fold(v) for v in options["vendors"]})
        if missing or extra:
            raise ValueError(f"options.brands must name exactly the vendors: missing {missing}, not a vendor {extra}")
        if len({fold(k) for k in brands}) != len(brands):
            raise ValueError("options.brands has two keys for the same vendor")
        # Retailer mode applies an override only to the SAME brand spelt differently (equal letters and
        # digits). A vendor in a measured family (RETAILER_BRAND_SPELLINGS) always writes the family's
        # canonical spelling, as it has since #2302, so for those the value may only say "no respelling"
        # (the vendor's own spelling, which operators use to list a store's vendors) or name the
        # canonical itself. Anything else would be silently overridden or ignored at crawl time --
        # refuse it here, every offender at once, instead of letting the operator think it applied.
        alnum = lambda v: "".join(c for c in str(v).casefold() if c.isalnum())
        ws = lambda v: " ".join(str(v).split())  # the collapse _crawl applies before use
        overridden = []
        for vendor, spelling in brands.items():
            family = _retailer_brand_family(alnum(vendor))
            if family is None:
                continue
            canonical = RETAILER_BRAND_CANONICAL[family]
            if ws(spelling) not in (ws(vendor), canonical):
                overridden.append(f"{vendor!r} -> {spelling!r} (family {family!r} always writes {canonical!r})")
        if overridden:
            raise ValueError("options.brands cannot respell a spelling-family vendor to a value other than the "
                             "vendor's own spelling or the family's canonical spelling: " + "; ".join(overridden))
        ignored = sorted(k for k, v in brands.items()
                         if _retailer_brand_family(alnum(k)) is None and alnum(k) != alnum(v))
        if ignored:
            raise ValueError(f"options.brands can only respell a vendor (same letters and digits); "
                             f"these would be ignored: {ignored}")
    if "collections" in options:
        from services.curated_brand_feed import valid_collection_handle
        if source != "storefront" or not options["collections"] or not all(
                valid_collection_handle(h) for h in options["collections"]):
            raise ValueError("options.collections must be Shopify collection handles on a storefront cohort")
    if int(options.get("max_pdp_inci_fetches") or 0) > MAX_PDP_INCI_FETCHES:
        raise ValueError(f"options.max_pdp_inci_fetches must be at most {MAX_PDP_INCI_FETCHES}")
    overlap = sorted({_handle_key(h) for h in options.get("refile_to_sets") or []}
                     & {_handle_key(h) for h in options.get("exclude_handles") or []})
    if overlap:
        raise ValueError(f"a handle cannot be both re-filed and excluded: {overlap}")
    if int(options.get("max_pdp_identity_fetches") or 0) > MAX_PDP_IDENTITY_FETCHES:
        # Each fetch waits CRAWL_MIN_INTERVAL_SECONDS (4s): 300 is ~20 min of one stage already.
        raise ValueError(f"options.max_pdp_identity_fetches must be at most {MAX_PDP_IDENTITY_FETCHES}")
    if source == "affiliate_feed" and options.get("source_role", "retailer") != "retailer":
        # The feed mapping is retailer-shaped: a retailer-host listing clicked through a network link.
        raise ValueError("options.source = affiliate_feed supports only source_role = retailer")
    if source == "affiliate_feed":
        from services.retailer_ingest.affiliate_feed import validate_feed_options
        validate_feed_options(options.get("feed"))
    elif "feed" in options:
        raise ValueError("options.feed is only meaningful with options.source = affiliate_feed")
    path = str(options.get("category_path") or "beauty").strip().strip("/").lower()
    if not (path == "beauty" or path.startswith("beauty/")) or path.count("/") > 1:
        raise ValueError(f"options.category_path must be coarse (beauty or beauty/<area>), got {path!r}")
    if "market" in options:
        from services.region_pricing import normalize_region
        market = normalize_region(options["market"])
        if not _ISO_ALPHA2.fullmatch(market):
            raise ValueError(f"options.market must be an ISO-3166-1 alpha-2 code, got {options['market']!r}")
        if market not in INGEST_MARKETS:
            raise ValueError(f"options.market {market} is not an ingest market yet (allowed: {list(INGEST_MARKETS)})")
        options["market"] = market  # "us" and "US" are one cohort (db.retailer_ingest.scope_key agrees)
    if "require_ships_to_market" in options and source != "storefront":
        raise ValueError("options.require_ships_to_market is only meaningful on a storefront crawl")
    if source == MARKETS_SOURCE:
        from services.retailer_ingest.shopify_markets import CAPTURE_MARKETS
        if job_market(options) not in CAPTURE_MARKETS:
            # The capture proves a session in a SERVED market (/cart.js reports its currency) and writes
            # siblings stamped with it. It is never the way to price a store for AU/JP: that is its
            # base-currency crawl.
            raise ValueError(f"options.source = shopify_markets captures the {' and '.join(CAPTURE_MARKETS)} "
                             f"markets only")
        if options.get("source_role") != "brand_official" and job_market(options) != "SG":
            # Brand stores first (Peng 2026-09-26) still holds for the US: US retailers follow later.
            # SG retailers are allowed (Peng 2026-10-10, "Build the SG currency support": nearly every SG
            # pair the coverage wave found is a retailer). The gate is the
            # MARKET, in code, not an env or a job option: an env would have to match on the enqueue runner
            # and on the drain (whose image and env have a single writer), and a job option is the operator
            # vouching for themselves. A retailer sibling copies the retailer's own seller identity from its
            # base offer (merchant_id, offer_type, offer_mode redirect), so nothing new is claimed for it.
            raise ValueError("options.source = shopify_markets supports source_role = retailer for market SG "
                             "only; a US capture is source_role = brand_official only (brand stores first, "
                             "Peng 2026-09-26; US retailers follow later)")
    elif job_market(options) in ACQUISITION_MARKETS and source != "storefront":
        # An acquisition job is the BASE-CURRENCY crawl of a storefront (ADR Phase 2, operator step 1):
        # the rows a shopify_markets capture later prices in USD. A feed has no such storefront.
        raise ValueError(f"options.market {job_market(options)} (an acquisition market) supports only "
                         f"options.source = storefront")
    # The currency is the MARKET's, never a second free choice: the /products.json crawl sees only the
    # store's base currency, so "base currency = the market's currency" is the one honest rule for it
    # (ADR section 3.3). Given, it must say the same; absent, it is derived.
    if "require_currency" in options and options["require_currency"] != job_currency(options):
        raise ValueError(f"options.require_currency {options['require_currency']!r} is not market "
                         f"{job_market(options)}'s currency {job_currency(options)}")
    return options


def job_market(options: Optional[Dict[str, Any]]) -> str:
    """The job's market: options.market, else DEFAULT_MARKET (every job queued before the option)."""
    from services.region_pricing import normalize_region
    return normalize_region((options or {}).get("market") or DEFAULT_MARKET)


def job_currency(options: Optional[Dict[str, Any]]) -> str:
    """The currency the job's store and offers must be in: its market's (services.region_pricing).
    validate_options refuses a require_currency that says anything else."""
    from services.region_pricing import pricing_currency_for_region
    return pricing_currency_for_region(job_market(options))


def job_source(options: Optional[Dict[str, Any]]) -> str:
    """The job's source: options.source, else "storefront" (validate_options refuses anything else)."""
    return (options or {}).get("source") or "storefront"


def ingest_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    """The crawl payload, normalized by the ONE payload validator with this lane's market allowlist.
    Enqueue runs exactly this, so a row the drain would refuse at crawl time is refused there too."""
    from services.catalog_onboard_worker import normalize_curated_brand_payload
    return normalize_curated_brand_payload(_feed_payload(job), markets=INGEST_MARKETS)


def _tokens(value: Any) -> List[str]:
    """Accent-folded, casefolded alphanumeric tokens: "Eco By Sonya USA" -> [eco, by, sonya, usa],
    "e.l.f." -> [e, l, f], "Karen's" -> [karen, s]."""
    import unicodedata
    text = "".join(c for c in unicodedata.normalize("NFKD", str(value or "")) if not unicodedata.combining(c))
    return "".join(c if c.isalnum() else " " for c in text.casefold()).split()


#: A /meta.json name with one of these tokens AFTER the brand is a store that SELLS the brand, never the
#: brand's own ("Sukin Stockist USA", "Sukin Beauty Warehouse", "Sukin Beauty Supply" -- "supply" is on
#: a retailer in the prod ledger, "Beauty Exchange Beauty Supply", and on no brand store). "at", "by"
#: and "from" name the store that sells it ("Sukin | Shop Sukin at Beauty Bay", "Sukin by Adore
#: Beauty", "Sukin from Adore Beauty"); "@" in the name reads as "at" (NFKC first, so the full-width
#: "＠" does too). Only the words after the brand count: a brand's own name may hold one ("Chemist
#: Confessions USA", "Eco By Sonya USA").
TIER_B_RESELLER_TOKENS = frozenset({
    "stockist", "stockists", "distributor", "distributors", "distribution", "distributions", "pharmacy",
    "pharmacies", "chemist", "chemists", "retailer", "retailers", "outlet", "outlets", "wholesale",
    "wholesaler", "wholesalers", "warehouse", "warehouses", "supply", "supplies", "at", "by", "from"})
#: A separator splits a /meta.json name into a second name ("Sukin | Beauty Bay", "Sukin - Adore
#: Beauty"), and nothing tells a retailer's name from a tagline there, so a name may split only where the
#: brand's own name does. A separator is any punctuation or math/other symbol (by Unicode category, so
#: look-alikes such as "∣", "│", "‒", "−", "・" count too, review of #2373) except TIER_B_NAME_JOINERS,
#: and except a hyphen between two letters/digits ("K-Beauty" is one word; a dash like "–" always
#: splits). Measured 2026-09-26 on the /meta.json names of the 77 stores in the prod retailer-ingest
#: ledger: 0 of the 13 brand stores has one; a retailer does ("K-Beauty Makeup | Authentic Korean ...").
TIER_B_SEPARATOR_CATEGORIES = frozenset({"Pc", "Pd", "Ps", "Pe", "Pi", "Pf", "Po", "Sk", "Sm", "So"})
#: Punctuation that joins a name rather than splitting it, when attached to a word: "Head & Shoulders",
#: "Karen's", "Kiehl‘s", "e.l.f.", "Dr. Jart+", "BondiBoost.com", "Sukin® USA". Standing alone between
#: spaces, every one but "&" is a separator ("Sukin + Beauty Bay"; "&" reads as "and", which is no
#: separator either).
TIER_B_NAME_JOINERS = frozenset("&'’‘.+!#®©")
#: Trademark marks NFKC folds into LETTERS glued to the word before ("Mario Badescu™" -> "badescutm",
#: which failed the brand prefix). Each reads as "®" instead, before any folding: attached, it joins
#: ("Sukin™ USA" is "Sukin USA"); standing alone it separates like any joiner ("Sukin ™ Beauty Bay").
#: ™ ℠; circled R and C (Ⓡ ⓡ Ⓒ ⓒ), circled italic R and C (🄬 🄫), squared R (🅁); the raised MC/MD/MR
#: marks (🅪 🅫 🅬). And ℗, which NFKC leaves alone but which is a mark like © (attached, it was a split).
TIER_B_TRADEMARK_MARKS = "™℠ⓇⓡⒸⓒ🄬🄫🅁🅪🅫🅬℗"


def _tier_b_folded(value: Any) -> str:
    """`value` as Tier B reads it: every TIER_B_TRADEMARK_MARKS as "®", then NFKC-folded."""
    import unicodedata
    text = "".join("®" if ch in TIER_B_TRADEMARK_MARKS else ch for ch in str(value or ""))
    return unicodedata.normalize("NFKC", text)


_WORD_HYPHENS = frozenset("-‐")  # NFKC folds the non-breaking U+2011 into U+2010
#: Bars that Unicode files as LETTERS (so no category catches them), separators anywhere: "ǀ" "ǁ".
TIER_B_LETTER_SEPARATORS = frozenset("ǀǁ")
#: Letters used as a dash or bar in JP/KR names ("Sukin ー 公式", "설화수 ㅣ 공식몰"), after NFKC: ー
#: (and half-width ｰ), 丨 (and ⼁), Hangul ㅡ/ㅣ (folded to U+1173/U+1175). Separators only as a
#: word of their own: inside a word they are letters ("コーセー").
TIER_B_DASH_LETTERS = "ー丨\u1173\u1175"
_STANDALONE_SEPARATOR = re.compile(
    "(?<!\\S)[" + TIER_B_DASH_LETTERS + re.escape("".join(sorted(TIER_B_NAME_JOINERS - {"&"}))) + "]+(?!\\S)")


def _tier_b_name_text(text: str) -> str:
    """`text` (already NFKC-folded) with every letter-shaped or free-standing separator rewritten as
    "|", so the name's tokens and its split points are read from the same string."""
    text = "".join(" | " if ch in TIER_B_LETTER_SEPARATORS else ch for ch in text)
    return _STANDALONE_SEPARATOR.sub("|", text)


def _name_split_points(text: str) -> set:
    """The token indices at which a separator (see TIER_B_SEPARATOR_CATEGORIES) splits `text` into two
    non-empty parts: "Sukin Naturals | Beauty Bay" -> {2}, "19/99 Beauty" -> {1}, "Sukin USA |" -> {}."""
    import unicodedata
    parts, current = [], []
    for i, ch in enumerate(text):
        separator = (unicodedata.category(ch) in TIER_B_SEPARATOR_CATEGORIES and ch not in TIER_B_NAME_JOINERS
                     and not (ch in _WORD_HYPHENS and 0 < i < len(text) - 1
                              and text[i - 1].isalnum() and text[i + 1].isalnum()))
        if separator:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    points, count = set(), 0
    for tokens in (t for t in map(_tokens, parts) if t):
        if count:
            points.add(count)
        count += len(tokens)
    return points


def storefront_tier_b(brand: str, storefront: Optional[Dict[str, Any]], market: str) -> Dict[str, Any]:
    """Tier B of the brand-official evidence ladder (multi-market storefronts ADR section 3.2): the
    store's own /meta.json name STARTS with the brand, the store ships to the job's market, and prices
    in that market's currency. {"passed": bool, <each conjunct>: bool, + what was read}.

    The name rule is a token-boundary PREFIX, not containment: containment passed "REN" in "Karen's
    Beauty Shop", "Tula" in "Tulane Pharmacy" and "e.l.f." in "Self Care Supply Co" (review of #2353).
    The brand's tokens must be the name's first tokens, the words after them must carry no reseller
    token (TIER_B_RESELLER_TOKENS: "Sukin Beauty Warehouse", "Sukin by Adore Beauty"), the name must not
    split into a second name (TIER_B_SEPARATOR_CATEGORIES: "Sukin | Beauty Bay"), and the folded brand must
    be at least TIER_B_MIN_BRAND_CHARS long. Measured positives (prod /meta.json,
    2026-09-26): "Sukin Naturals USA", "Bali Body US", "MooGoo USA", "Eco By Sonya USA", "esmi Skin",
    "MineTan USA" -- each USD, ships_to [US].

    CONJUNCTIVE on purpose: a name alone would pass a distributor's store that leads with the brand it
    resells, and a US-shipping USD store alone proves nothing about whose it is."""
    from services.region_pricing import pricing_currency_for_region_or_none
    sf = storefront if isinstance(storefront, dict) else {}
    want = _tokens(_tier_b_folded(brand))
    raw = _tier_b_name_text(_tier_b_folded(sf.get("name")).replace("@", " at "))
    name = _tokens(raw)
    starts = len("".join(want)) >= TIER_B_MIN_BRAND_CHARS and name[:len(want)] == want
    rest = name[len(want):] if starts else name
    ships = sf.get("ships_to_countries")
    expected = pricing_currency_for_region_or_none(market)
    out: Dict[str, Any] = {
        "name": sf.get("name"), "myshopify_domain": sf.get("myshopify_domain"), "currency": sf.get("currency"),
        "market": market,
        "name_starts_with_brand": starts,
        "name_has_no_reseller_token": bool(name) and not (set(rest) & TIER_B_RESELLER_TOKENS),
        "name_is_one_store_name": bool(name) and _name_split_points(raw) <= _name_split_points(
            _tier_b_name_text(_tier_b_folded(brand))),
        "ships_to_market": isinstance(ships, list) and market in ships,
        "currency_is_market_currency": bool(expected) and sf.get("currency") == expected,
    }
    out["passed"] = bool(out["name_starts_with_brand"] and out["name_has_no_reseller_token"]
                         and out["name_is_one_store_name"]
                         and out["ships_to_market"] and out["currency_is_market_currency"])
    return out


def brand_official_domain_flags(domain: str, brands: List[str], *, storefront: Optional[Dict[str, Any]] = None,
                                market: str = DEFAULT_MARKET) -> List[Dict[str, Any]]:
    """The BLOCK flags of `brand_official_domain_review` (see there); enqueue calls it with no
    storefront, where only the known-retailer refusal and Tier A can decide."""
    return brand_official_domain_review(domain, brands, storefront=storefront, market=market)[0]


def brand_official_domain_review(domain: str, brands: List[str], *, storefront: Optional[Dict[str, Any]] = None,
                                 market: str = DEFAULT_MARKET) -> tuple:
    """A brand_official cohort writes CANONICAL rows: a product's key derives from (brand, product
    name) alone -- and its brand is the product's own VENDOR, not the job's `brand` -- so the same
    product at any host lands on the same key, and the upsert re-points that key's source_domain /
    canonical_url / payload at this host and labels its INCI brand-official. Nothing downstream checks
    that the host is the brand's (the legacy-listing report and the native-variant rule look only at
    ext:retailer: keys; the brand-host guard only at the same host), and a clean dry run auto-applies.
    So the host must PROVE it is the store of EVERY brand the cohort would write (`brands`: the job's
    brand and each record's):

      * a known retailer host can never be a brand's own store (not acceptable -- fix the job);
      * Tier A: the domain's name IS that brand (offer_seller_identity.brand_owns_domain, the rule
        that types an offer brand_direct) -- us.frankbody.com for "Frank Body";
      * Tier B: the store's own /meta.json (`storefront`, from the crawl) names the brand, ships to
        the job's `market` and prices in its currency (storefront_tier_b) -- sukinnaturals.com,
        "Sukin Naturals USA", for "Sukin". Peng approved it as automatic on 2026-09-26;
      * otherwise a human accepts the flag (tartecosmetics.com for "Tarte", k18hair.com for "K18"):
        held, never auto-applied. A failed Tier B yields exactly that flag, same key.

    Returns (flags, evidence): `evidence` says which tier admitted each brand (or what Tier B read
    when none did) and is recorded on the run as checks.brand_official_evidence.
    """
    from services.catalog_enrichment_agent.apply import brand_official_label
    from services.offer_seller_identity import brand_owns_domain, is_known_retailer

    if is_known_retailer(domain):
        # Before any tier: a retailer's /meta.json may well name the brand it sells.
        refused = {"key": "brand_official_on_a_retailer", "rule": "brand_official_on_a_retailer",
                   "severity": detectors.BLOCK, "acceptable": False,
                   "detail": f"{domain} is a known retailer; source_role brand_official would overwrite "
                             f"canonical rows of {sorted(set(brands))} with this retailer's listings"}
        return [refused], {"domain": domain, "known_retailer": True}
    flags: Dict[str, Dict[str, Any]] = {}
    evidence: Dict[str, Any] = {"domain": domain, "market": market, "brands": {}}
    for brand in brands:
        # The brand's own casefolded spelling, NOT normalize_brand: that strips every non-ASCII
        # letter, so 설화수 and 헤라 would share one key and one acceptance would pass both.
        label = brand_official_label(brand)
        if brand_owns_domain(brand, domain):
            evidence["brands"].setdefault(label, {"tier": "A"})
            continue
        tier_b = storefront_tier_b(brand, storefront, market)
        if tier_b["passed"]:
            evidence["brands"].setdefault(label, {"tier": "B", **tier_b})
            continue
        evidence["brands"].setdefault(label, {"tier": None, "tier_b": tier_b})
        key = unproven_domain_flag_key(domain, label)
        flags.setdefault(key, {
            "key": key, "rule": "brand_official_domain_unproven", "severity": detectors.BLOCK,
            "detail": f"the domain name of {domain} is not the brand {brand!r}, and its /meta.json does not "
                      f"prove it for {market} (name {tier_b['name']!r} starts with the brand: "
                      f"{tier_b['name_starts_with_brand']}, no reseller word: {tier_b['name_has_no_reseller_token']}, "
                      f"no second name after a separator: {tier_b['name_is_one_store_name']}, "
                      f"ships to {market}: {tier_b['ships_to_market']}, "
                      f"{tier_b['currency']!r} is {market}'s currency: {tier_b['currency_is_market_currency']}); "
                      f"accept this key only if {domain} is {brand}'s own store (its rows become {brand}'s "
                      f"canonical rows)"})
    return list(flags.values()), evidence


def unproven_domain_flag_key(domain: str, label: str) -> str:
    """The key of the flag a human accepts to vouch that `domain` is the store of the brand `label`."""
    return f"brand_official_domain_unproven:{domain}:{label}"


def verified_brand_storefront(job: Dict[str, Any], checks: Optional[Dict[str, Any]]):
    """The storefront this run PROVED is the brand's own, and for which brands, or None.

    Read from what brand_official_domain_review recorded on this run (checks.brand_official_evidence):
    a brand is proven when Tier A or Tier B admitted it, or when the job's accepted_flags carry its
    brand_official_domain_unproven key (a human vouched). A known retailer, a job that is not
    source_role=brand_official, or evidence for another domain proves nothing. Only a proven brand's
    rows on this host get 'self' seeds (catalog_enrichment_agent.apply.VerifiedBrandStorefront), which
    lets trust serve them without a manual live-read promotion (Peng, 2026-09-29)."""
    from services.catalog_enrichment_agent.apply import VerifiedBrandStorefront, _storefront_host

    o = job.get("options") or {}
    domain = str(job.get("domain") or "")
    evidence = (checks or {}).get("brand_official_evidence")
    if o.get("source_role") != "brand_official" or not domain or not isinstance(evidence, dict):
        return None
    if evidence.get("known_retailer") or evidence.get("domain") != domain:
        return None
    accepted = set(o.get("accepted_flags") or [])
    proven = frozenset(
        label for label, seen in (evidence.get("brands") or {}).items()
        if (seen or {}).get("tier") in ("A", "B") or unproven_domain_flag_key(domain, label) in accepted
    )
    if not proven:
        return None
    return VerifiedBrandStorefront(host=_storefront_host(domain), brand_labels=proven)


def _feed_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    try:
        o = validate_options(dict(job.get("options") or {}))
    except ValueError as exc:
        raise _Stop("invalid_job", "failed", str(exc)) from exc
    if o.get("retire_stale_brand"):
        from services.curated_brand_feed import _vendor_token as fold
        if fold(o["retire_stale_brand"]) == fold(job.get("brand")):
            # The same spelling moves no key; retiring "old" keys then means retiring this run's own rows
            # (the CLI's #2173 mode, which the drain does not do). Review of #2426.
            raise _Stop("invalid_job", "failed",
                        "options.retire_stale_brand must be a different spelling from the job's brand")
    return {
        # multi_brand: no override at all, so no product can be renamed to the job's label.
        "domain": job["domain"], "brand": None if o.get("multi_brand") else job["brand"],
        "category_path": o.get("category_path") or "beauty",
        "source_role": o.get("source_role") or "retailer", "retailer_name": o.get("retailer_name"),
        "only_vendors": list(o["vendors"]),
        # The market's currency (validate_options has refused any other require_currency), and the market
        # itself, which normalize_curated_brand_payload refuses unless it is in the INGEST_MARKETS this
        # lane hands it (ingest_payload) -- a second lock on the allowlist; the curated onboard queue,
        # which stamps no market on its offers, still gets US only.
        "require_currency": job_currency(o), "market": job_market(o), "emit_real_variants": True,
        "enrich_missing_gtin": True, "max_products": int(o.get("max_products") or 200),
        "max_scan_products": int(o.get("max_scan_products") or 20000),
        "max_pdp_identity_fetches": int(o.get("max_pdp_identity_fetches") or 200),
        # 0 is a real choice (no INCI fetches), so no `or` here.
        "max_pdp_inci_fetches": (DRAIN_PDP_INCI_FETCHES if o.get("max_pdp_inci_fetches") is None
                                 else int(o["max_pdp_inci_fetches"])),
        "pdp_inci_budget_s": DRAIN_PDP_INCI_BUDGET_S,
    }


def _transient(crawl: Dict[str, Any]) -> bool:
    reason = str(crawl.get("reason") or "")
    return crawl.get("status") == "failed" and bool(
        re.search(r"HTTP (?:429|5\d\d)|[Tt]imeout|TransportError|NetworkError|ConnectError|ReadError|"
                  r"WriteError|RemoteProtocolError|ProtocolError|PoolTimeout", reason))


async def _affiliate_records(job: Dict[str, Any], payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The affiliate datafeed as records, reported like a complete crawl. A download that answers
    429/5xx or times out is TRANSIENT (backs off exactly like a throttled crawl); a feed or mapping
    that cannot be trusted fails the job with its reason."""
    import httpx

    from services.curated_brand_feed import CrawlIncomplete, ShopifyProductBatch
    from services.retailer_ingest.affiliate_feed import FeedError, feed_rows_to_records, fetch_feed_text, parse_feed

    feed = job["options"]["feed"]
    if feed["retailer_host"].strip().lower().removeprefix("www.") != job["domain"].strip().lower().removeprefix("www."):
        raise _Stop("invalid_job", "failed", "options.feed.retailer_host must be the job's domain")

    def incomplete(reason: str) -> CrawlIncomplete:
        return CrawlIncomplete(f"{job['domain']}: affiliate feed: {reason}", status="failed", next_page=0,
                               scanned_products=0, selected_products=0)
    # httpx exceptions can carry the request URL (and so the token) in str(): only their TYPE is recorded.
    try:
        text = await fetch_feed_text(feed, env=dict(os.environ))
    except (httpx.TimeoutException, httpx.TransportError) as exc:
        raise incomplete(f"{type(exc).__name__}") from exc
    except FeedError as exc:
        if re.search(r"HTTP (?:429|5\d\d)", str(exc)):
            raise incomplete(str(exc)) from exc
        raise _Stop("feed_invalid", "failed", f"affiliate feed: {exc}") from exc
    except (httpx.HTTPError, httpx.InvalidURL, UnicodeError, ValueError) as exc:
        raise _Stop("feed_invalid", "failed", f"affiliate feed download: {type(exc).__name__}") from exc
    try:
        rows = parse_feed(text, fmt=feed["format"], json_path=feed.get("json_path"))
        records = feed_rows_to_records(rows, feed, vendors=payload["only_vendors"],
                                       category_path=payload["category_path"],
                                       currency=payload["require_currency"])
    except (FeedError, ValueError, UnicodeError) as exc:
        raise _Stop("feed_invalid", "failed", f"affiliate feed: {exc}") from exc
    batch = ShopifyProductBatch(records, scanned_products=len(rows), pages=1)
    batch.crawl_report["source"] = f"affiliate_feed:{feed['network']}"
    batch.crawl_report.update(getattr(records, "stats", None) or {})
    return batch


def _throttled(job: Dict[str, Any], stage: str, reason: str, *,
               checks: Optional[Dict[str, Any]] = None) -> _Stop:
    """A 429/5xx/timeout: back off and retry the SAME stage later, spending an attempt; once the retry
    budget is spent the job fails. The host is never asked faster than crawl_politeness allows."""
    attempts = int(job.get("attempts") or 0) + 1
    if attempts >= int(job.get("max_attempts") or 6):
        return _Stop("crawl_throttled", "failed", f"retry budget spent: {reason}", count_attempt=True,
                     checks=checks)
    return _Stop("crawl_throttled", "queued" if stage == DRY_RUN else "apply_due", f"throttled, retry later: {reason}",
                 next_run_at=ledger.backoff_until(attempts - 1), count_attempt=True, checks=checks)


def _require_acquisition_is_unserved(market: str) -> None:
    """An acquisition-market job writes rows priced in a currency no served region expects. TWO locks keep
    them off the serving surface, and this job requires both: index_pipeline_state's acquisition rule
    (`_acquisition_only_priced`, enforced in every process whatever the flag says -- the lock that holds
    when some other process recomputes the row) and the agent-decision gate's no_us_offer blocker
    (ENABLE_KBEAUTY_AGENT_DECISION_GATES; index_pipeline_state recomputes it in THIS process when the
    apply materializes readiness). A job process with the gate off is misconfigured for this lane: refuse
    before any crawl rather than run on one lock."""
    if market not in ACQUISITION_MARKETS:
        return
    from services.agent_decision_gates import agent_decision_gates_enabled
    if not agent_decision_gates_enabled():
        raise _Stop("acquisition_unguarded", "failed",
                    f"market {market} is an acquisition market (rows stored, not served), but "
                    f"ENABLE_KBEAUTY_AGENT_DECISION_GATES is off in this process: only the index's "
                    f"acquisition rule would keep its {market} rows from serving. Turn the gate on for the "
                    f"job, then re-queue.")


def _require_served_market_is_served(market: str) -> None:
    """A served market's rows must read back serving-eligible, and index_pipeline_state decides that in THIS
    process from its own PIVOTA_SERVING_PRICING_REGIONS (read at import). A drain whose env lists only US
    would score every SGD row as priced for no served region and fail the job after its write -- so refuse
    before any crawl, and name the env to fix (infra/gcp/setup_scheduler.sh carries US,SG for the drain)."""
    if market not in SERVED_INGEST_MARKETS or market == DEFAULT_MARKET:
        return
    from services.index_pipeline_state_service import serving_pricing_regions
    regions = serving_pricing_regions()
    if market not in regions:
        raise _Stop("served_market_unconfigured", "failed",
                    f"market {market} is served, but this process serves only {regions} "
                    f"(PIVOTA_SERVING_PRICING_REGIONS): its {market} rows would read back unserved. Set the "
                    f"drain's env to include {market}, then re-queue.")


async def _crawl(job: Dict[str, Any], stage: str) -> List[Dict[str, Any]]:
    from services.curated_brand_feed import (CrawlIncomplete, lash_nail_title_evidence, lip_title_evidence,
                                             records_for_brand)
    import contextlib

    _require_acquisition_is_unserved(job_market(job.get("options")))
    _require_served_market_is_served(job_market(job.get("options")))
    try:
        payload = ingest_payload(job)
        if (job.get("options") or {}).get("collections"):
            payload["collection_handles"] = list(job["options"]["collections"])
        if (job.get("options") or {}).get("multi_brand"):
            payload["brand_by_vendor"] = {" ".join(k.split()).casefold(): " ".join(v.split())
                                          for k, v in job["options"]["brands"].items()}
    except ValueError as exc:  # e.g. an unknown source_role on a row written by another path
        raise _Stop("invalid_job", "failed", str(exc)) from exc
    options = job.get("options") or {}
    evidence = contextlib.ExitStack()
    if options.get("lip_title_evidence"):
        evidence.enter_context(lip_title_evidence())
    if options.get("lash_nail_title_evidence"):
        evidence.enter_context(lash_nail_title_evidence())
    try:
        with evidence:
            if (job.get("options") or {}).get("source") == "affiliate_feed":
                records = await _affiliate_records(job, payload)
            else:
                records = await records_for_brand(**{k: v for k, v in payload.items() if k != "market"})
    except CrawlIncomplete as exc:
        crawl = exc.as_dict()
        if crawl.get("status") == "capped":
            reason = str(crawl.get("reason") or "")
            advice = ("crawl the brand's collections (options.collections) -- Shopify pages stop at 100"
                      if "options.collections" in reason else
                      "raise options.max_scan_products (store scan) or options.max_products (selected rows), "
                      "whichever the reason names")
            raise _Stop("crawl_capped", "failed", f"crawl capped: {reason} -- {advice}, or cancel the job") from exc
        if _transient(crawl):
            raise _throttled(job, stage, str(crawl.get("reason"))) from exc
        raise _Stop("crawl_failed", "failed", f"crawl failed: {crawl.get('reason')}") from exc
    report = getattr(records, "crawl_report", None)
    if not isinstance(report, dict) or report.get("status") != "complete":
        raise _Stop("crawl_unproven", "failed", "crawl completeness was not proven")
    currency = payload["require_currency"]  # job_currency: the market's (validated in _feed_payload)
    for record in records:
        seen = (record.get("pdp") or {}).get("currency")
        if seen != currency:
            raise _Stop("currency_unproven", "failed", f"record currency {seen!r} is not {currency}")
    return records


#: Rows the category filter left out, recorded per run. Bounded so one huge store cannot bloat a run
#: row: at most LEFT_OUT_ROWS_CAP rows and LEFT_OUT_TYPES_CAP merchant types, each string at most
#: LEFT_OUT_STR_CAP chars. `count` and `by_reason` are always complete; the `*_truncated` flags say
#: when a list is not.
LEFT_OUT_ROWS_CAP = 300
LEFT_OUT_TYPES_CAP = 25
LEFT_OUT_STR_CAP = 200


def _ledger_safe(value: Any) -> Optional[str]:
    """A merchant string as Postgres jsonb will take it: no NUL (jsonb refuses \\u0000), no NaN
    (refused as a token), no lone surrogate (not encodable as UTF-8), bounded length. The run row must
    never fail to write over a product name: a failed finish_run leaves the run unfinished, and the
    next execution treats the stage as interrupted and spends an attempt on it."""
    if value is None or (isinstance(value, float) and value != value):
        return None
    text = str(value).replace("\x00", "").encode("utf-8", "replace").decode("utf-8")
    return text[:LEFT_OUT_STR_CAP]


def _left_out_summary(left_out: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Which rows a category filter left out and why: counts by reason and by the merchant's own
    product type (the usual culprit), plus the rows themselves up to LEFT_OUT_ROWS_CAP."""
    from collections import Counter
    types = Counter(_ledger_safe(e.get("merchant_product_type")) or "(none)" for e in left_out)
    return {
        "count": len(left_out),
        "by_reason": dict(Counter(e["reason"] for e in left_out)),
        "by_merchant_type": dict(types.most_common(LEFT_OUT_TYPES_CAP)),
        "merchant_types_truncated": len(types) > LEFT_OUT_TYPES_CAP,
        "rows": [{k: _ledger_safe(e.get(k)) for k in ("reason", "product_name", "category_path",
                                                      "merchant_product_type", "handle")}
                 for e in left_out[:LEFT_OUT_ROWS_CAP]],
        "rows_truncated": len(left_out) > LEFT_OUT_ROWS_CAP,
    }


def _refile_to_sets(records: List[Dict[str, Any]], handles: Any, checks: Dict[str, Any],
                    flags: List[Dict[str, Any]]) -> tuple:
    """File the reviewer-named bundles under REFILE_SETS_LEAF; returns (handles re-filed, each re-filed
    record AS THE STORE FILED IT, for the detectors). A named handle the crawl no longer carries blocks
    (like an unmatched exclusion) -- accept its key to go on."""
    import copy
    import scripts.onboard_curated_brands as cli
    wanted = {_handle_key(h) for h in (handles or []) if str(h).strip()}
    if not wanted:
        return set(), []
    matched, as_filed = set(), []
    for record in records:
        handle = cli._record_handle(record)
        pdp = record.get("pdp")
        if handle in wanted and isinstance(pdp, dict):
            as_filed.append(copy.deepcopy(record))
            pdp["category_path"] = REFILE_SETS_LEAF
            pdp["category_resolution_status"] = "resolved"
            pdp["category_confidence"] = CATEGORY_CONFIDENCE_REVIEW_REFILE
            matched.add(handle)
    checks["refiled_to_sets"] = sorted(matched)
    for handle in sorted(wanted - matched):
        flags.append({"key": f"refile_handle_unmatched:{handle}", "rule": "refile_handle_unmatched",
                      "severity": detectors.BLOCK, "handle": handle,
                      "detail": "an approved re-file to gift sets no longer matches any product"})
    return matched, as_filed


async def _check(job: Dict[str, Any], records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Narrow, plan and run every check. Returns the plan plus a verdict; never writes."""
    import scripts.onboard_curated_brands as cli
    from services.catalog_enrichment_agent.ingestion import ingest_validated_jsonl
    from services.catalog_enrichment_agent.primary_ingestion import inspect_primary_plan

    o = job.get("options") or {}
    checks: Dict[str, Any] = {"crawl": getattr(records, "crawl_report", None), "selected": len(records)}
    # Judged BEFORE exclude_handles and the category filter narrow the cohort: judged after, excluding some
    # flagged rows (or an only_category slice) dropped the store under the minimum and applied the rest
    # unanswered (review of #2371). Rows are flagged later, only those still in the cohort.
    store_verdict = detectors.placeholder_price_store_verdict(records)
    if store_verdict:
        checks["placeholder_price_store"] = store_verdict
    flags: List[Dict[str, Any]] = []
    market = job_market(o)
    storefront = (checks["crawl"] or {}).get("storefront") if isinstance(checks["crawl"], dict) else None
    if isinstance(storefront, dict):
        # WHICH Shopify store this crawl read (a us.<brand> host is usually a separate store from the
        # brand's home one, not an alias) and whether it ships to the job's market. Recorded, never gated.
        ships = storefront.get("ships_to_countries")
        checks["storefront"] = {
            "name": storefront.get("name"), "myshopify_domain": storefront.get("myshopify_domain"),
            "currency": storefront.get("currency"), "market": market,
            "ships_to_count": len(ships) if isinstance(ships, list) else None,
            "ships_to_market": (market in ships) if isinstance(ships, list) else None,
        }
    if o.get("require_ships_to_market"):
        # 2026-10-10 (SG capture work): the gate the recorded ships_to never was, opt-in per job. A USD-base
        # retailer's base crawl is a US job, so without it a store that does not ship to the US would land as
        # a US seller; the SG capture that follows refuses such a store too (base_market_not_shipped).
        ships = storefront.get("ships_to_countries") if isinstance(storefront, dict) else None
        if not isinstance(ships, list):
            raise _Stop("ships_to_unverifiable", "failed",
                        f"options.require_ships_to_market: {job['domain']}'s /meta.json did not state "
                        f"ships_to_countries, so shipping to {market} is unproven; nothing written", checks=checks)
        if market not in ships:
            raise _Stop("store_does_not_ship_to_market", "nothing",
                        f"options.require_ships_to_market: {job['domain']}'s /meta.json ships_to_countries "
                        f"({len(ships)} countries) does not include {market}: not a {market} seller; "
                        f"nothing written", checks=checks)
    if o.get("source_role") == "brand_official":
        # Every brand the cohort would write: the job's, and each record's own (its vendor's).
        written = [job["brand"]] + sorted({str((r.get("pdp") or {}).get("brand") or "") for r in records} - {""})
        domain_flags, checks["brand_official_evidence"] = brand_official_domain_review(
            job["domain"], written, storefront=storefront, market=market)
        flags.extend(domain_flags)

    excluded = {str(h).strip().strip("/").casefold() for h in (o.get("exclude_handles") or []) if str(h).strip()}
    if excluded:
        records, matched = cli._exclude_by_handle(records, excluded, domain=job["domain"])
        checks["excluded"] = sorted(matched)
        for handle in sorted(excluded - matched):
            flags.append({"key": f"exclude_handle_unmatched:{handle}", "rule": "exclude_handle_unmatched",
                          "severity": detectors.BLOCK, "handle": handle,
                          "detail": "an approved exclusion no longer matches any product"})
    refiled, refiled_as_filed = _refile_to_sets(records, o.get("refile_to_sets"), checks, flags)
    if o.get("only_category") or o.get("only_resolved_category"):
        selected = len(records)
        # A reviewer's re-file is kept whatever the cohort's category filter: a lip pass that re-files a
        # lip duo must not drop it as "outside beauty/makeup/lip" and still report it re-filed.
        kept_refiled = [r for r in records if cli._record_handle(r) in refiled]
        records, left_out = cli._partition_by_category(
            [r for r in records if cli._record_handle(r) not in refiled], prefix=o.get("only_category"))
        if kept_refiled:
            inside, _ = cli._partition_by_category(kept_refiled, prefix=o.get("only_category"))
            checks["refiled_kept_outside_filter"] = sorted(
                {cli._record_handle(r) for r in kept_refiled} - {cli._record_handle(r) for r in inside})
            records = records + kept_refiled
        cli._print_category_filter(selected, records, left_out, domain=job["domain"],
                                   want=cli._normalize_category_prefix(o.get("only_category")))
        checks["left_out"] = _left_out_summary(left_out)
        if not records:
            checks["kept"] = 0
            raise _Stop("nothing_to_ingest", "nothing",
                        f"no product resolves under {o.get('only_category') or 'a resolved category'}",
                        checks=checks)
    checks["kept"] = len(records)

    # Every offer is DECLARED for the job's market (catalog_offers.market) and must be priced in its
    # currency; the seed rows keep the US serving partition (ingestion.SEED_PARTITION_MARKET).
    moves_accepted = [k.split(":", 1)[1] for k in o.get("accepted_flags") or [] if k.startswith("listing_moved:")]
    plan = await cli._plan_with_current_listings(records, market=market, allow_moves=moves_accepted)
    inspection = inspect_primary_plan(plan)
    checks["plan"] = {k: inspection.get(k) for k in ("status", "reasons", "planned", "unresolved_category_count")}
    if inspection.get("reasons"):
        flags.append({"key": "plan_not_ready", "rule": "plan_not_ready", "severity": detectors.BLOCK, "acceptable": False,
                      "detail": f"primary plan {inspection.get('status')}: {inspection.get('reasons')}"})

    legacy = await cli._legacy_listing_report(plan, check=True)
    guard = await cli._brand_host_guard_report(plan, check=True)
    checks["legacy_listings"] = {k: legacy.get(k) for k in ("status", "conflict_count", "planned_listings",
                                                             "retired_owner_count")}
    checks["brand_host_guard"] = {k: guard.get(k) for k in ("status", "rows_at_risk", "planned_groups")}
    for name, report in (("legacy_listings", legacy), ("brand_host_guard", guard)):
        if report.get("status") in ("conflicts", "error"):
            flags.append({"key": name, "rule": name, "severity": detectors.BLOCK, "acceptable": False,
                          "detail": json.dumps(report, default=str)[:600]})

    row_flags = detectors.detect(records, store_verdict=store_verdict)
    if refiled:
        # Judge each re-filed row on BOTH shelves: the rules keyed on the store's shelf (lip size, lip
        # copy, the lip title door) cannot fire on the gift-set shelf, and a re-file must not hide them.
        # Per-row rules only: placeholder_price_store already flagged these rows above (a re-file changes
        # the shelf, never the price), and the re-filed handful is not a store (20 $1 sets must not read as one).
        seen = {f["key"] for f in row_flags}
        row_flags += [f for f in detectors.detect(refiled_as_filed, store_level=False) if f["key"] not in seen]
        answered = [f for f in row_flags if f.get("handle") in refiled and f.get("rule") in REFILE_RESOLVES_RULES]
        row_flags = [f for f in row_flags if f not in answered]
        checks["refile_resolved_flags"] = sorted(f["key"] for f in answered)
    # A listing the plan left out because an earlier one on this host has its title (one content key).
    collisions = plan.get("listing_collisions") or []
    checks["listing_collisions"] = len(collisions)
    checks["current_listings"] = plan.get("current_listings")
    checks["listing_moves"] = len(plan.get("listing_moves") or [])
    row_flags += detectors.listing_move_flags(plan.get("listing_moves") or [])
    if (plan.get("current_listings") or {}).get("status") in ("unchecked", "error"):
        # Without the listing each row names, a plan cannot tell a move from a first ingest.
        flags.append({"key": "current_listings_unread", "rule": "current_listings_unread",
                      "severity": detectors.BLOCK, "acceptable": False,
                      "detail": f"could not read the rows' current listings: {plan['current_listings']}"})
    row_flags += detectors.listing_collision_flags(collisions)
    # Name each row's brand on its flag: in a multi_brand cohort the reviewer must see whose row it is.
    brand_of = {detectors._handle(r): (r.get("pdp") or {}).get("brand") for r in records}
    for f in row_flags:
        if f.get("handle") and not f.get("brand"):
            f["brand"] = brand_of.get(f["handle"])
    flags.extend(row_flags)
    accepted = list(o.get("accepted_flags") or [])
    if o.get("accept_listing_collisions"):
        accepted += [f["key"] for f in flags if f.get("rule") == "same_key_other_listing"]
    blocking = detectors.blocking(flags, accepted=accepted)
    checks["flags"] = {"block": len([f for f in flags if f["severity"] == detectors.BLOCK]),
                       "info": len([f for f in flags if f["severity"] == detectors.INFO]),
                       "blocking_after_approval": len(blocking)}
    return {"plan": plan, "inspection": inspection, "checks": checks, "flags": flags, "blocking": blocking}


#: The lifecycle stages backend global recall admits (services/pivot_query_service.py: `IN (...) OR
#: pdp_lifecycle_stage IS NULL`; the filter is skipped for merchant-scoped lanes and for
#: require_signature/canonical_entities_only). NOT the agent door's rule: the gateway gates on
#: serving_eligible only. Used to annotate a readback, never to fail one.
BACKEND_RECALL_LIFECYCLE_STAGES = ("validated", "published")

#: index_pipeline_state blocker codes that mean the index REFUSED a row on its content (thin copy, no image,
#: low quality score, not a core product) -- services/index_pipeline_state_service.py. A readback notes these;
#: any other blocker on an applied row fails the job.
INDEX_CONTENT_REFUSALS = ("low_quality", "no_image", "short_description", "non_core_product")
#: services/agent_decision_gates.BLOCKER_NO_US_OFFER: the blocker an acquisition-market row is EXPECTED to
#: carry (priced, but in a currency no served region expects). Spelled here to keep that import lazy.
_NO_US_OFFER = "no_us_offer"


def _storefront_hosts(domain: Optional[str]) -> List[str]:
    """The spellings of one storefront host an offer's source_domain may carry (www. is noise)."""
    host = str(domain or "").strip().lower().rstrip(".").removeprefix("www.")
    return [host, f"www.{host}"] if host else []


async def _readback(product_keys: List[str], currency: str, db: Any,
                    planned_images: Optional[Dict[str, bool]] = None, *, market: str = DEFAULT_MARKET,
                    domain: Optional[str] = None) -> Dict[str, Any]:
    """Did what the gate says landed actually land servable? One row per applied product.

    Every live offer the job's STOREFRONT (`domain`) holds on the product must be in the job's currency
    AND stamped the job's market (catalog_offers.market): currency = market, always (multi-market
    storefronts ADR section 3.3). Scoped to the storefront because a product legitimately carries other
    storefronts' offers in other markets -- the Frank Body product holds us.frankbody.com's USD/US offer
    AND frankbody.com's AUD/AU one -- and neither job may fail over the other's. And scoped to the offers
    THIS LANE writes there (source_system = ingestion.AGENT_VERSION): a shopify_markets capture writes its
    USD/US siblings on the SAME host (services/retailer_ingest/shopify_markets.py, source_system
    shopify_markets_us_localization), as did the older scripts/capture_us_market_offers.py, so a re-run of
    the storefront's AU crawl would otherwise count them as its own and fail after every write (review of
    #2358, D1). `domain=None` counts every offer of the product (a caller with no storefront). An SG job's
    offers are stamped market 'SG' in SGD; its seeds stay in the "US" partition like every market's
    (external_product_seeds.market is a hard serving partition -- ingestion.SEED_PARTITION_MARKET).

    ACQUISITION markets (AU/JP): the rows must land STORED and NOT SERVED. A row the index blocks as
    no_us_offer -- the last rung of its ladder, reached only when every content gate passed -- with its
    own priced offer and a resolved identity is the expected outcome, noted as `acquisition_not_served`.
    A row that reads back SERVED must owe it to an offer priced for a served region on its content_key
    (another storefront's USD offer, or a shopify_markets sibling); a served acquisition row without one
    is the leak this lane must never cause, and fails the job."""
    if not product_keys:
        return {"ok": False, "reason": "no product keys to read back", "notes": [], "rows": []}
    from services.index_pipeline_state_service import _RESOLVED_PDP_SCOPES
    from services.priced_offer_sql import priced_offer_exists_sql
    from services.catalog_enrichment_agent.ingestion import AGENT_VERSION
    hosts = _storefront_hosts(domain)
    scoped = (" AND lower(o.source_domain) = ANY(:hosts) AND o.source_system = :lane_source_system"
              if hosts else "")
    acquisition = market in ACQUISITION_MARKETS
    served_priced = ""
    if acquisition:
        from services.index_pipeline_state_service import serving_pricing_regions
        from services.region_pricing import has_offer_priced_for_any_region_sql
        served_priced = (""",
               -- The leak detector: index_pipeline_state is per CONTENT_KEY, so the offer that makes a
               -- served acquisition row legitimate may hang off a sibling product of the same content_key.
               EXISTS (SELECT 1 FROM catalog_products cp WHERE cp.content_key = p.content_key
                         AND cp.suppressed_at IS NULL
                         AND """ + has_offer_priced_for_any_region_sql("cp.product_key", serving_pricing_regions())
                         + """) AS content_served_region_priced""")
    values: Dict[str, Any] = {"keys": list(product_keys), "currency": currency, "market": market,
                              "resolved_scopes": sorted(_RESOLVED_PDP_SCOPES)}
    if hosts:
        values["hosts"] = hosts
        values["lane_source_system"] = AGENT_VERSION
    rows = await db.fetch_all(
        """
        SELECT p.product_key, p.category_path, coalesce(ips.serving_eligible, false) AS serving,
               ips.pipeline_stage, ips.blocker_code, ips.blocker_detail, p.pdp_lifecycle_stage AS lifecycle,
               -- Per-PRODUCT evidence, never the IPS flags: an IPS row is per content_key, and when products
               -- share one its flags are the best-ranked sibling's (index_pipeline_state_service warns
               -- callers off exactly this), so a priced sibling could mask this row's lost price write.
               """ + priced_offer_exists_sql("p.product_key") + """ AS row_priced,
               (coalesce(p.image_url, '') <> '') AS row_image,
               (p.pdp_scope = ANY(:resolved_scopes) OR EXISTS (
                   SELECT 1 FROM product_group_members pgm WHERE pgm.merchant_id = p.merchant_id
                     AND pgm.platform = p.platform AND pgm.platform_product_id = p.source_product_id)) AS row_identity,
               (SELECT count(*) FROM catalog_offers o WHERE o.product_key = p.product_key
                  AND o.suppressed_at IS NULL""" + scoped + """) AS offers,
               (SELECT count(*) FROM catalog_offers o WHERE o.product_key = p.product_key
                  AND o.suppressed_at IS NULL""" + scoped + """ AND o.currency = :currency) AS offers_in_currency,
               (SELECT count(*) FROM catalog_offers o WHERE o.product_key = p.product_key
                  AND o.suppressed_at IS NULL""" + scoped + """ AND upper(o.market) = :market) AS offers_in_market"""
        + served_priced + """
        FROM catalog_products p LEFT JOIN index_pipeline_state ips USING (content_key)
        WHERE p.product_key = ANY(:keys)
        """,
        values,
    )
    out = [dict(r) for r in rows]
    problems, notes = [], []
    found = {r["product_key"] for r in out}
    for key in product_keys:
        if key not in found:
            problems.append({"product_key": key, "problem": "not in catalog_products"})
    for r in out:
        if not r["category_path"]:
            problems.append({"product_key": r["product_key"], "problem": "no category_path"})
        if not r["serving"]:
            # The index's own content gate refusing a thin row (measured 2026-09-24: a gift-with-purchase
            # mini at westman-atelier.com, "Blush Stick", content_quality_score 71.2 < 71.4) is the
            # system working, not a lost write: note it, keep the store applied. Every other reason --
            # suppressed, not live, no seed/extraction, unscored, no price, unresolved identity -- can
            # mean the write itself went wrong, and still fails the job.
            # The index records only the FIRST failed check, and low_quality / non_core_product sit ahead of
            # no_price and entity_unresolved: a content code can mask a lost write. So a refusal is a note only
            # when THIS product's own row shows a priced offer and a resolved identity, and an image this plan
            # wrote actually landed (apply overwrites image_url, so a missing one is ours).
            wrote_image = bool((planned_images or {}).get(r["product_key"]))
            own_row_whole = (r.get("row_priced") and r.get("row_identity")
                             and not (wrote_image and not r.get("row_image")))
            if acquisition and r.get("blocker_code") == _NO_US_OFFER and own_row_whole:
                notes.append({"product_key": r["product_key"], "kind": "acquisition_not_served",
                              "note": f"stored, not served: {market} is an acquisition market and its "
                                      f"{currency} offer is priced for no served region (no_us_offer)"})
            elif r.get("blocker_code") in INDEX_CONTENT_REFUSALS and own_row_whole:
                notes.append({"product_key": r["product_key"], "kind": "index_refused",
                              "note": f"not served: the index refused it on content ({r.get('blocker_code')}"
                                      f"{': ' + str(r.get('blocker_detail'))[:160] if r.get('blocker_detail') else ''})"})
            else:
                problems.append({"product_key": r["product_key"],
                                 "problem": f"not serving-eligible (blocker {r.get('blocker_code') or 'unknown'})"})
        elif acquisition and not r.get("content_served_region_priced"):
            problems.append({"product_key": r["product_key"],
                             "problem": f"served, but no offer on its content_key is priced for a served region: "
                                        f"a {market} acquisition row must never serve on its {currency} offer"})
        # Recorded, never a failure: the agent door (gateway) serves on serving-eligibility alone, while
        # backend global recall admits only BACKEND_RECALL_LIFECYCLE_STAGES (or NULL). Measured
        # 2026-09-24: 14 of 28 O HUI rows at buybeautykorea.com landed `candidate` (no taxonomy signal:
        # the store has no tags) and the agent door still returned them. The run says which rows
        # backend recall will not see.
        if r.get("lifecycle") is not None and r.get("lifecycle") not in BACKEND_RECALL_LIFECYCLE_STAGES:
            notes.append({"product_key": r["product_key"], "kind": "outside_backend_recall",
                          "note": f"outside backend global recall: pdp_lifecycle_stage {r.get('lifecycle')!r}"})
        if not r["offers"] or r["offers_in_currency"] != r["offers"]:
            problems.append({"product_key": r["product_key"],
                             "problem": f"offers {r['offers']}, in {currency}: {r['offers_in_currency']}"})
        if r["offers_in_market"] != r["offers"]:
            problems.append({"product_key": r["product_key"],
                             "problem": f"offers {r['offers']}, stamped market {market}: {r['offers_in_market']}"})
    return {"ok": not problems, "problems": problems, "notes": notes, "rows": out}


async def _move(job: Dict[str, Any], *, db: Any, **fields: Any) -> bool:
    """Every transition is conditional on the status this stage claimed: a job an operator
    cancelled while its crawl ran keeps its cancellation (the stage's verdict is recorded on the
    run only)."""
    moved = await ledger.transition(job["id"], expected_status=job["status"], db=db, **fields)
    if not moved:
        job["superseded"] = True
    return moved


async def run_stage(job: Dict[str, Any], *, db: Any, time_left_s: Optional[float] = None) -> Dict[str, Any]:
    """Run the job's due stage and record it. Returns {job_id, stage, outcome, status, reason}
    (+ superseded=True when an operator changed the job while the stage ran).

    `time_left_s`: seconds left in the calling execution's task when the job was claimed (None = no
    deadline). An apply starts its catalog write only with WRITE_MARGIN_S of it still left."""
    deadline = None if time_left_s is None else time.monotonic() + float(time_left_s)
    out = await _run_stage(job, db=db, deadline=deadline)
    if job.get("superseded"):
        out = {**out, "superseded": True}
    return out


@contextlib.contextmanager
def _timed(timings: Dict[str, float], key: str):
    """Record the seconds the block took under `key`, also when it raised."""
    started = time.monotonic()
    try:
        yield
    finally:
        timings[key] = round(time.monotonic() - started, 3)


def _with_timings(checks: Any, timings: Dict[str, float], stage: str) -> Any:
    """The run's checks with the phase timings in them (a stage that stopped before any check
    returned records the timings alone). An apply that ends before its write keeps the
    catalog_write=not_started evidence start_run wrote, instead of finish_run erasing it."""
    if checks is None and (timings or stage == APPLY):
        checks = {"timings": timings}
    if isinstance(checks, dict):
        checks.setdefault("timings", timings)
        if stage == APPLY:
            checks.setdefault("catalog_write", ledger.CATALOG_WRITE_NOT_STARTED)
    return checks


async def _run_stage(job: Dict[str, Any], *, db: Any, deadline: Optional[float] = None) -> Dict[str, Any]:
    stage = APPLY if job["status"] == "apply_due" else DRY_RUN
    # The previous execution was killed mid-stage (task timeout, OOM): its run never finished.
    interrupted = await ledger.unfinished_run(job["id"], db=db)
    if interrupted:
        note = "execution ended before the stage finished (task timeout or OOM)"
        # An apply whose run was never marked write-started died in its crawl, checks or lock wait:
        # nothing was written. Only the explicit "not_started" marker proves that; a run without any
        # marker (started by an older image) may have been writing.
        write_may_have_started = (interrupted["stage"] == APPLY
                                  and interrupted.get("catalog_write") != ledger.CATALOG_WRITE_NOT_STARTED)
        if interrupted["stage"] == APPLY and not write_may_have_started:
            note += "; the catalog write had not started, nothing was written"
        # Keep what the run recorded before it died -- the write marker and, for an apply that was retiring an
        # old spelling, the reversal manifest it stored before that write (ledger.record_retire_manifest).
        kept = interrupted.get("checks")
        kept = json.loads(kept) if isinstance(kept, str) else kept
        await ledger.finish_run(interrupted["id"], outcome="interrupted", checks=kept, error=note, db=db)
        if write_may_have_started:
            # It may have written part of the cohort. Never re-apply blindly.
            reason = f"the previous apply was interrupted and may be partial; review before re-queueing"
            await _move(job, status="failed", run_id=interrupted["id"], reason=reason, db=db)
            return {"job_id": job["id"], "stage": APPLY, "outcome": "interrupted", "status": "failed",
                    "reason": reason}
        attempts = int(job.get("attempts") or 0) + 1
        if attempts >= int(job.get("max_attempts") or 6):
            await _move(job, status="failed", run_id=interrupted["id"], count_attempt=True,
                        reason=f"retry budget spent: {note}", db=db)
            return {"job_id": job["id"], "stage": interrupted["stage"], "outcome": "interrupted", "status": "failed"}
        await _move(job, status=job["status"], run_id=interrupted["id"], count_attempt=True,
                    next_run_at=ledger.backoff_until(attempts - 1), reason=f"retry later: {note}", db=db)
        return {"job_id": job["id"], "stage": interrupted["stage"], "outcome": "interrupted", "status": job["status"]}
    run_id = await ledger.start_run(job_id=job["id"], stage=stage,
                                    image_sha=os.getenv("PIVOTA_COMMIT_SHA") or os.getenv("IMAGE_SHA"),
                                    execution=os.getenv("CLOUD_RUN_EXECUTION"), db=db)
    result: Dict[str, Any] = {}
    # Seconds per phase: crawl_s, check_s (every stage), write_lock_wait_s, write_s, readback_s (apply).
    timings: Dict[str, float] = {}
    try:
        if job_source(job.get("options")) == MARKETS_SOURCE:
            return await _run_markets_stage(job, run_id, stage, timings, result, db=db, deadline=deadline)
        with _timed(timings, "crawl_s"):
            records = await _crawl(job, stage)
        with _timed(timings, "check_s"):
            result = await _check(job, records)
        result["checks"]["timings"] = timings
        if stage == DRY_RUN and (job.get("options") or {}).get("retire_stale_brand"):
            result["checks"]["stale_brand_retire_preview"] = await _stale_brand_retire_preview(job, records)
        if stage == APPLY:
            result["checks"].setdefault("catalog_write", ledger.CATALOG_WRITE_NOT_STARTED)
        summary = {"crawl": result["checks"].get("crawl"), "plan": result["checks"].get("plan"),
                   "checks": result["checks"], "flags": result["flags"]}
        if result["blocking"]:
            await ledger.finish_run(run_id, outcome="held", **summary, db=db)
            await _move(job, status="held", run_id=run_id,
                                    reason=f"{len(result['blocking'])} blocking flag(s): "
                                           + ", ".join(sorted({f['rule'] for f in result['blocking']})),
                                    db=db)
            return {"job_id": job["id"], "stage": stage, "outcome": "held", "status": "held"}
        if stage == DRY_RUN:
            await ledger.finish_run(run_id, outcome="clean", **summary, db=db)
            await _move(job, status="apply_due", run_id=run_id,
                                    reason="dry run clean; apply due", next_run_at=datetime.now(timezone.utc),
                                    db=db)
            return {"job_id": job["id"], "stage": stage, "outcome": "clean", "status": "apply_due"}
        return await _apply(job, run_id, result, summary, timings, db=db, deadline=deadline, records=records)
    except _Stop as stop:
        await ledger.finish_run(run_id, outcome=stop.outcome,
                                checks=_with_timings(stop.checks or result.get("checks"), timings, stage),
                                flags=result.get("flags"), error=stop.reason, db=db)
        await _move(job, status=stop.status, run_id=run_id, reason=stop.reason,
                                next_run_at=stop.next_run_at, count_attempt=stop.count_attempt, db=db)
        return {"job_id": job["id"], "stage": stage, "outcome": stop.outcome, "status": stop.status,
                "reason": stop.reason}
    except Exception as exc:  # noqa: BLE001 -- recorded, then re-raised so the job execution fails loudly
        reason = f"{type(exc).__name__}: {exc}"
        await ledger.finish_run(run_id, outcome="error", checks=_with_timings(result.get("checks"), timings, stage),
                                error=reason, db=db)
        await _move(job, status="failed", run_id=run_id, reason=reason[:2000], db=db)
        raise


async def _write_not_started(job: Dict[str, Any], outcome: str, why: str, *, db: Any) -> _Stop:
    """The stop for an apply that ends before its catalog write, nothing written: back to apply_due
    shortly, no attempt spent -- unless this is the WRITE_LOCK_STARVED_AFTER-th such end in a row, which
    holds the job for a human (and says so at WARNING)."""
    streak = await ledger.consecutive_outcomes(job["id"], WRITE_LOCK_RETRY_OUTCOMES,
                                               limit=WRITE_LOCK_STARVED_AFTER, db=db) + 1
    if streak >= WRITE_LOCK_STARVED_AFTER:
        logger.warning("retailer_ingest: job %s (%s) ended %d applies in a row before its catalog write "
                       "(last: %s); held for review, nothing written", job["id"], job.get("domain"), streak, why)
        return _Stop("write_lock_starved", "held",
                     f"{streak} applies in a row ended before the catalog write, nothing written (last: {why}); "
                     f"check for a stuck catalog write lock or a crawl that leaves no time to write, then approve "
                     f"to retry", count_attempt=False)
    return _Stop(outcome, "apply_due", f"{why}; nothing written, retry in {WRITE_LOCK_BUSY_RETRY_S}s",
                 next_run_at=datetime.now(timezone.utc) + timedelta(seconds=WRITE_LOCK_BUSY_RETRY_S),
                 count_attempt=False)


async def _write_lock_wait_s(job: Dict[str, Any], deadline: Optional[float], timings: Dict[str, float], *,
                             db: Any) -> float:
    """How long an apply may wait for the catalog write lock: at most WRITE_LOCK_WAIT_S, and never so long
    that less than WRITE_MARGIN_S of the task is left for the write itself (then nothing is written and
    the job goes back to apply_due)."""
    waiting = time.monotonic()
    wait_s = float(WRITE_LOCK_WAIT_S)
    if deadline is not None:
        wait_s = min(wait_s, deadline - waiting - WRITE_MARGIN_S)
    if wait_s <= 0:
        timings["write_lock_wait_s"] = 0.0
        raise await _write_not_started(
            job, "write_lock_busy",
            f"no time left in this execution for the catalog write ({deadline - waiting:.0f}s left, "
            f"{WRITE_MARGIN_S}s needed)", db=db)
    return wait_s


async def _apply(job: Dict[str, Any], run_id: str, result: Dict[str, Any], summary: Dict[str, Any],
                 timings: Dict[str, float], *, db: Any, deadline: Optional[float] = None,
                 records: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    from scripts.curated_apply_gate import evaluate_apply_log
    from services.catalog_enrichment_agent.apply import apply_ingest_plan
    from services.catalog_enrichment_agent.primary_ingestion import require_primary_apply, require_primary_plan

    plan = result["plan"]
    preflight = require_primary_plan(plan)
    # The storefront this run proved is the brand's own; only its proven brands' seeds become 'self'.
    verified = verified_brand_storefront(job, result.get("checks"))
    result["checks"]["verified_storefront"] = (
        {"host": verified.host, "brands": sorted(verified.brand_labels)} if verified else None)
    wait_s = await _write_lock_wait_s(job, deadline, timings, db=db)
    waiting = time.monotonic()
    try:
        # The ONLY catalog write of the stage, and the only part serialized across lanes: the crawl and
        # checks above ran unlocked. The lock is released when this block exits, however it exits.
        async with ledger.catalog_write_lock(wait_s=wait_s, poll_s=WRITE_LOCK_POLL_S) as waited:
            timings["write_lock_wait_s"] = round(waited, 3)
            # Durable BEFORE the first catalog write: an execution killed from here on is "may be
            # partial"; one killed before it (crawl, checks, lock wait) is retried, nothing written.
            await ledger.mark_write_started(run_id, db=db)
            result["checks"]["catalog_write"] = ledger.CATALOG_WRITE_STARTED  # kept when the run finishes
            with _timed(timings, "write_s"):
                counts = await apply_ingest_plan(plan, batch_label=f"retailer_ingest:{job['id']}", db=db,
                                                 primary_readiness=True, market=job_market(job.get("options")),
                                                 verified_storefront=verified)
        report = require_primary_apply(preflight, counts)
    except CatalogWriteLockBusy as busy:
        timings["write_lock_wait_s"] = round(time.monotonic() - waiting, 3)
        raise await _write_not_started(
            job, "write_lock_busy",
            f"catalog write lock busy for {busy.waited_s:.0f}s (another apply is writing)", db=db) from busy
    except CatalogWriteLockUnavailable as unavailable:
        # Opening the lock connection or a try-lock failed: the lock was never held, nothing written.
        timings["write_lock_wait_s"] = round(time.monotonic() - waiting, 3)
        raise await _write_not_started(
            job, "write_lock_unavailable",
            f"catalog write lock unavailable ({unavailable.error_type})", db=db) from unavailable
    except ValueError as exc:
        # A refused apply may have written part of the cohort: failed, never retried blindly.
        report = getattr(exc, "report", None) or getattr(getattr(exc, "__cause__", None), "report", None)
        await ledger.finish_run(run_id, outcome="apply_refused", **summary, applied=report,
                                error=str(exc), db=db)
        await _move(job, status="failed", run_id=run_id,
                                reason=f"apply refused (may be partial): {str(exc)[:600]}", db=db)
        return {"job_id": job["id"], "stage": APPLY, "outcome": "apply_refused", "status": "failed"}

    # The same gate an operator ran on the log, fed the same line the CLI prints.
    gate = evaluate_apply_log("primary ingestion: " + json.dumps(report, default=str) + "\nJOB=pipeline RC=0",
                              domain=job["domain"])
    currency, market = job_currency(job.get("options")), job_market(job.get("options"))
    with _timed(timings, "readback_s"):
        readback = await _readback(gate.get("product_keys") or [], currency, db, market=market,
                                   domain=job["domain"],
                                   planned_images={p.get("product_key"): bool(p.get("image_url"))
                                                   for p in plan.get("pdps") or []})
    ok = bool(gate.get("ok")) and readback["ok"]
    outcome = "applied" if ok else ("gate_failed" if not gate.get("ok") else "readback_failed")
    retire: Optional[Dict[str, Any]] = None
    if ok and (job.get("options") or {}).get("retire_stale_brand"):
        # Only after a VERIFIED apply: the retire keys on the new rows being live (and serving) on this store.
        with _timed(timings, "stale_brand_retire_s"):
            retire = await _retire_stale_brand(job, run_id, records or [], result["checks"],
                                               applied_keys=gate.get("product_keys") or [], db=db,
                                               deadline=deadline)
        result["checks"]["stale_brand_retire"] = retire
    await ledger.finish_run(run_id, outcome=outcome, **summary, applied={"gate": gate}, readback=readback,
                            db=db)
    kinds = [n.get("kind") for n in readback.get("notes") or []]
    said = [f"{kinds.count(k)} {label}" for k, label in (("outside_backend_recall", "row(s) outside backend global recall"),
                                                          ("index_refused", "row(s) refused by the index content gate"),
                                                          ("acquisition_not_served", "row(s) stored, not served "
                                                                                     "(acquisition market)"))
            if kinds.count(k)]
    reason = ("; ".join(["applied and verified", *said]) if ok else
              f"{outcome}: gate {gate.get('reasons')}; readback {readback.get('problems')}")
    status = "done" if ok else "failed"
    if retire is not None:
        reason = f"{reason}; {_retire_reason(retire)}"
        if retire["outcome"] in RETIRE_FAILED_OUTCOMES:
            # The catalog apply stands (verified above); the retire did not. Failed reaches a human through the
            # drain's failed alert; nothing is re-applied, and the old rows are all still live.
            status = "failed"
    await _move(job, status=status, run_id=run_id, reason=reason, db=db)
    out = {"job_id": job["id"], "stage": APPLY, "outcome": outcome, "status": status}
    if retire is not None:
        out["stale_brand_retire"] = retire["outcome"]
    return out


#: A retire that needs a human: its error (wrote nothing), a read-back that disagreed with the plan, or a
#: committed retire whose rows' catalog_row_trust was not refreshed (they stay publicly listed).
RETIRE_FAILED_OUTCOMES = ("error", "readback_failed", "trust_not_refreshed")


def _retire_counts(p: Dict[str, Any]) -> Dict[str, int]:
    return {"cohort": len(p["cohort"]), "present": len(p["present"]), "retirable": len(p["live"]),
            "waiting_for_new_key": len(p["waiting_for_new_key"]),
            "new_not_serving": len(p.get("new_not_serving") or []), "foreign": len(p["foreign"]),
            "already_suppressed": len(p.get("already_suppressed") or []),
            **({"canonical_handovers": len(p["handovers"])} if p.get("handovers") else {})}


async def _stale_brand_retire_preview(job: Dict[str, Any], records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Dry run: how many of this store's rows are live under the old spelling (the retire's upper bound; the
    apply retires only those whose new row is live and, where the old row serves, serving). Read-only."""
    from scripts import retire_superseded_brand_keys as retire_tool
    stale = str(job["options"]["retire_stale_brand"]).strip()
    try:
        cohort = retire_tool.cohort_from_records(records, job["brand"], stale)
        p = await retire_tool.plan_for_cohort(cohort, job["domain"], job["brand"],
                                              job["options"].get("category_path") or "beauty", stale,
                                              before_rewrite=True)
    except Exception as exc:  # noqa: BLE001 -- a preview never fails the dry run; the apply re-plans anyway
        return {"stale_brand": stale, "error": f"{type(exc).__name__}: {exc}"[:500]}
    counts = _retire_counts(p)
    return {"stale_brand": stale, "cohort": counts["cohort"], "old_rows_live_on_store": counts["retirable"],
            "foreign": counts["foreign"]}


async def _retire_stale_brand(job: Dict[str, Any], run_id: str, records: List[Dict[str, Any]],
                              checks: Dict[str, Any], *, applied_keys: List[str], db: Any,
                              deadline: Optional[float]) -> Dict[str, Any]:
    """Retire the store's old-spelling keys after a verified apply: under the catalog write lock, the CLI's own
    plan (serving-guarded) on the records this stage just applied, the manifest stored on the run BEFORE the
    write, the write, then a read-back. Never raises: the outcome says what happened."""
    from scripts import retire_superseded_brand_keys as retire_tool
    stale = str(job["options"]["retire_stale_brand"]).strip()
    out: Dict[str, Any] = {"stale_brand": stale}
    wait_s = float(WRITE_LOCK_WAIT_S)
    if deadline is not None:
        wait_s = min(wait_s, deadline - time.monotonic() - RETIRE_LOCK_MARGIN_S)
        if wait_s <= 0:
            # A write the task timeout kills leaves a verified apply reading "may be partial". Not started.
            out.update(outcome="deferred", error="no time left in this execution")
            return _deferred(job, out)
    written = False
    try:
        # Plan INSIDE the lock: no other writer can suppress or restore these keys between the plan (and the
        # manifest built from it) and the write (review of #2426).
        async with ledger.catalog_write_lock(wait_s=wait_s, poll_s=WRITE_LOCK_POLL_S):
            # The keys this apply just wrote are this run's products: never an "old" row (review of #2426).
            protected = set(applied_keys)
            cohort = [c for c in retire_tool.cohort_from_records(records, job["brand"], stale)
                      if c["stale_key"] not in protected]
            p = await retire_tool.plan_for_cohort(cohort, job["domain"], job["brand"],
                                                  job["options"].get("category_path") or "beauty", stale)
            out.update(_retire_counts(p))
            if p.get("handover_error"):
                # The plan handed nothing over and kept those pairs as "new key does not serve yet"; without this
                # the status line would read like an ordinary wait, not a failed check.
                out["handover_error"] = p["handover_error"]
            prepared = retire_tool.prepare_retire(p)
            if not prepared:
                out["outcome"] = "nothing_to_retire"
                return out
            # Durable before the write, and kept in the run's final checks too (finish_run rewrites the column).
            await ledger.record_retire_manifest(run_id, prepared["manifest"], db=db)
            checks["stale_brand_retire_manifest"] = prepared["manifest"]
            out["retire_run_id"] = prepared["run_id"]
            out["counts"] = await retire_tool.write_retire(prepared)
            written = True
    except (CatalogWriteLockBusy, CatalogWriteLockUnavailable) as exc:
        out.update(outcome="deferred", error=f"catalog write lock: {type(exc).__name__}")
        return _deferred(job, out)
    except Exception as exc:  # noqa: BLE001 -- write_retire is one transaction: an error before it returns wrote nothing
        if not written:
            checks.pop("stale_brand_retire_manifest", None)  # nothing to revert; revert refuses this run anyway
            out.update(outcome="error", error=f"{type(exc).__name__}: {exc}"[:600])
            logger.warning(f"retailer_ingest_drain: stale-brand retire FAILED for {job['id']} "
                           f"({job['domain']}), nothing retired: {out['error']}")
            return out
        out["post_write_error"] = f"{type(exc).__name__}: {exc}"[:600]
    try:
        out["readback"] = await _retire_readback(p, retire_tool)
    except Exception as exc:  # noqa: BLE001 -- the write committed; an unreadable result is for a human
        out["readback"] = {"ok": False, "problems": [f"read-back raised {type(exc).__name__}: {exc}"[:300]]}
    out["outcome"] = "retired" if out["readback"]["ok"] and "post_write_error" not in out else "readback_failed"
    if out["outcome"] == "retired" and out["counts"].get("trust_problems"):
        # The tombstones are right; only their trust rows lag. Not a revert -- a re-run of the refresh.
        out["outcome"] = "trust_not_refreshed"
    return out


def _deferred(job: Dict[str, Any], out: Dict[str, Any]) -> Dict[str, Any]:
    # The job stays done (its apply verified); this line is what a deferred retire leaves besides the status.
    logger.warning(f"retailer_ingest_drain: stale-brand retire DEFERRED for {job['id']} ({job['domain']}), "
                   f"nothing retired: {out.get('error')}")
    return out


def _retire_reason(r: Dict[str, Any]) -> str:
    """The job's status line for the retire, e.g. "old spelling 'Tarte Cosmetics': retired 51 key(s)
    (retire_7983ba6a31bb); kept 19 waiting for a live new key, 1 whose new key does not serve yet"."""
    head = f"old spelling {r['stale_brand']!r}: "
    kept = [f"{r[k]} {label}" for k, label in (("waiting_for_new_key", "waiting for a live new key"),
                                                ("new_not_serving", "whose new key does not serve yet"),
                                                ("foreign", "owned by another source"))
            if r.get(k)]
    tail = f"; kept {', '.join(kept)}" if kept else ""
    if r.get("handover_error"):
        tail += (f"; the canonical handover check FAILED ({r['handover_error']}), so a pair whose old row holds its "
                 "content_key's canonical URL was kept, not handed over")
    if r["outcome"] == "retired":
        handed = r["counts"].get("canonical_handovers")
        moved = f", {handed} with the canonical URL handed to the new key" if handed else ""
        return f"{head}retired {r['counts']['products']} key(s) ({r['retire_run_id']}){moved}{tail}"
    if r["outcome"] == "nothing_to_retire":
        return f"{head}nothing to retire{tail}"
    if r["outcome"] == "deferred":
        return (f"{head}retire DEFERRED, nothing retired ({r.get('error')}); run "
                f"scripts/retire_superseded_brand_keys.py --stale-brand by hand")
    if r["outcome"] == "trust_not_refreshed":
        return (f"{head}retired {r['counts']['products']} key(s) ({r['retire_run_id']}) but their catalog_row_trust "
                f"was not refreshed: {r['counts']['trust_problems'][:3]}; their trust rows keep the old decision (a retired "
                f"row stays publicly listed, a new row the canonical URL was handed to stays unlisted) until "
                f"retire_superseded_brand_keys refresh-trust --ingest-run <this run> (or the trust backfill cron)")
    if r["outcome"] == "readback_failed":
        return (f"{head}retired {r['counts']['products']} key(s) ({r['retire_run_id']}) but the read-back "
                f"disagrees: {r['readback']['problems'][:3]}; revert with retire_superseded_brand_keys revert "
                f"--ingest-run <this run>, THEN services.catalog_offer_suppression.revert_offer_suppression("
                f"<the manifest's product_keys>) -- restored rows have no live offers until it runs -- THEN "
                f"retire_superseded_brand_keys refresh-trust --ingest-run <this run>, or their trust stays blocked "
                f"until the trust backfill cron")
    return f"{head}retire FAILED, nothing retired: {r.get('error')}"


#: Seconds of the task the retire leaves unused when it waits for the write lock: its write is small, but it
#: must not be the thing the task timeout kills.
RETIRE_LOCK_MARGIN_S = 120


async def _retire_readback(p: Dict[str, Any], retire_tool: Any) -> Dict[str, Any]:
    """Every retired old key is suppressed, and its new key is live on this store -- and serving wherever the
    old row served -- and every content_key whose canonical URL the retire handed over now elects the new key.
    Measured, not assumed: the plan was read before the write."""
    from db.database import database
    retired = p["live"]
    old = {r["product_key"]: dict(r) for r in await database.fetch_all(
        retire_tool.LIVE_ROWS_SQL, {"keys": [c["stale_key"] for c in retired]})}
    new_rows = [dict(r) for r in await database.fetch_all(
        retire_tool.LIVE_ROWS_SQL, {"keys": [c["new_key"] for c in retired]})]
    host = retire_tool._host(p["domain"])
    new_live = {r["product_key"] for r in new_rows
                if not r.get("suppression_reason") and retire_tool._host(r.get("source_domain")) == host}
    serving = await retire_tool.load_serving({}, [r for r in new_rows if r["product_key"] in new_live])
    searchable = await retire_tool.load_searchable(sorted(new_live))
    was_serving = {c["stale_key"] for c in retired} & set(p.get("serving") or [])
    was_searchable = {c["stale_key"] for c in retired} & set(p.get("searchable") or [])
    problems = []
    for c in retired:
        if not (old.get(c["stale_key"]) or {}).get("suppression_reason"):
            problems.append(f"old key still live: {c['stale_key']}")
        if c["new_key"] not in new_live:
            problems.append(f"new key not live: {c['new_key']}")
        elif c["stale_key"] in was_serving and c["new_key"] not in serving:
            problems.append(f"served before, new key not serving: {c['new_key']}")
        elif c["stale_key"] in was_searchable and c["new_key"] not in searchable:
            problems.append(f"searchable before, new key not searchable: {c['new_key']}")
    handovers = p.get("handovers") or []
    if handovers:
        elected = {r["content_key"]: r["canonical_sig_id"] for r in await database.fetch_all(
            retire_tool.ELECTIONS_SQL, {"keys": sorted({h["content_key"] for h in handovers})})}
        problems += [f"canonical not handed over: {h['content_key']} still elects {elected.get(h['content_key'])}"
                     for h in handovers if elected.get(h["content_key"]) != h["to_sig"]]
    return {"ok": not problems, "retired": len(retired), "new_live": len(new_live),
            "new_serving": len(serving), "new_searchable": len(searchable), "problems": problems[:20]}


async def _run_markets_stage(job: Dict[str, Any], run_id: str, stage: str, timings: Dict[str, float],
                             result: Dict[str, Any], *, db: Any, deadline: Optional[float] = None) -> Dict[str, Any]:
    """One stage of a source=shopify_markets job (services/retailer_ingest/shopify_markets.py).

    Same shape as a crawl job: the dry run proves the session in the job's market (US, or SG since
    2026-10-10) and reads every price, writing nothing; a clean dry run is applied automatically (Peng's
    policy); the apply RE-proves the session, re-reads every price, and only then writes the siblings
    (USD/US or SGD/SG) under the catalog write lock, republishes the touched content_keys and reads them
    back. A store that does not confirm the market's currency is a clean refusal (status `nothing`, the
    reason and the evidence on the run), never a crash; a block fails the job as unverifiable; a 429/5xx
    backs off like a throttled crawl."""
    from services.retailer_ingest import shopify_markets as markets

    o = job.get("options") or {}
    try:
        validate_options(dict(o))
    except ValueError as exc:
        raise _Stop("invalid_job", "failed", str(exc)) from exc
    market, currency = job_market(o), job_currency(o)
    # An SG sibling serves only where index_pipeline_state counts SGD as a served-region price: refuse
    # before any request to the store, not after a write whose readback would fail (a no-op for US).
    _require_served_market_is_served(market)
    try:
        with _timed(timings, "crawl_s"):
            capture = await markets.capture(job, db=db, max_products=int(o.get("max_products") or 200))
    except markets.MarketsRefused as refused:
        if refused.transient:
            raise _throttled(job, stage, refused.reason, checks=refused.checks) from refused
        raise _Stop(refused.outcome, refused.status, refused.reason, checks=refused.checks) from refused
    checks = result["checks"] = capture["checks"]
    checks["timings"] = timings
    planned = capture["planned"]
    summary = {"crawl": checks.get("markets_capture"), "plan": {"planned": len(planned)}, "checks": checks,
               "flags": []}
    if not planned:
        raise _Stop("nothing_to_capture", "nothing",
                    f"the {market} session was proven but no base offer has a sellable {currency} price "
                    f"({checks['markets_capture'].get('skipped')})", checks=checks)
    if stage == DRY_RUN:
        await ledger.finish_run(run_id, outcome="clean", **summary, db=db)
        await _move(job, status="apply_due", run_id=run_id, reason=f"{market} session proven, {len(planned)} "
                    f"{currency} sibling offer(s) planned; apply due", next_run_at=datetime.now(timezone.utc), db=db)
        return {"job_id": job["id"], "stage": stage, "outcome": "clean", "status": "apply_due"}

    checks.setdefault("catalog_write", ledger.CATALOG_WRITE_NOT_STARTED)
    wait_s = await _write_lock_wait_s(job, deadline, timings, db=db)
    waiting = time.monotonic()
    try:
        async with ledger.catalog_write_lock(wait_s=wait_s, poll_s=WRITE_LOCK_POLL_S) as waited:
            timings["write_lock_wait_s"] = round(waited, 3)
            await ledger.mark_write_started(run_id, db=db)
            checks["catalog_write"] = ledger.CATALOG_WRITE_STARTED
            with _timed(timings, "write_s"):
                wrote = await markets.write_siblings(planned, db=db)
                failed = await markets.republish(
                    sorted({r["content_key"] for r in wrote["written"] if r.get("content_key")}), db=db,
                    source_system=markets.source_system_for(market))
    except CatalogWriteLockBusy as busy:
        timings["write_lock_wait_s"] = round(time.monotonic() - waiting, 3)
        raise await _write_not_started(
            job, "write_lock_busy", f"catalog write lock busy for {busy.waited_s:.0f}s (another apply is writing)",
            db=db) from busy
    except CatalogWriteLockUnavailable as unavailable:
        timings["write_lock_wait_s"] = round(time.monotonic() - waiting, 3)
        raise await _write_not_started(
            job, "write_lock_unavailable", f"catalog write lock unavailable ({unavailable.error_type})", db=db
        ) from unavailable
    applied = {"written": len(wrote["written"]), "not_written": len(wrote["not_written"]),
               "refused": wrote["refused"], "republish_failed": len(failed)}
    with _timed(timings, "readback_s"):
        readback = await markets.readback(wrote["written"], db=db, republish_failed=failed, market=market)
    ok = readback["ok"]
    outcome = "applied" if ok else "readback_failed"
    await ledger.finish_run(run_id, outcome=outcome, **summary, applied=applied, readback=readback, db=db)
    reason = (f"applied and verified: {applied['written']} {currency} sibling offer(s), "
              + (f"{readback.get('served_content_keys', 0)} content_key(s) serving" if market == DEFAULT_MARKET
                 # A USD retailer's content_key already serves through its US base offers, so a serving
                 # count is no evidence about SG (review of #2553): the reason claims only what was checked.
                 else f"{len({r['content_key'] for r in wrote['written'] if r.get('content_key')})} "
                      f"content_key(s) re-published")
              + (f"; {applied['not_written']} not written" if applied["not_written"] else "")
              if ok else f"{outcome}: {readback.get('problems')}")
    await _move(job, status="done" if ok else "failed", run_id=run_id, reason=reason[:2000], db=db)
    return {"job_id": job["id"], "stage": APPLY, "outcome": outcome, "status": "done" if ok else "failed"}
