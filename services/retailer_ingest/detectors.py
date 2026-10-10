"""Automated checks that stand in for an operator reading every row of a dry run.

Each rule is measured, not imagined:
  * 2026-09-23 k-touch.us: "3CE - TONE UP TINT 40ml" typed `LIP TINT` by the merchant; its own
    description sells it as a tone-up cream for the complexion. Caught only by reading the row.
    -> lip_row_copy_not_about_lips, lip_row_implausible_size, title_contradicts_category.
  * 2026-09-23 k-touch.us: "3CE Soft Matte Lipstick - Warmish Move" carries a rice cleanser's
    pasted description. -> lip_row_copy_not_about_lips.
  * 2026-09-09 limecrime.com: a literal `TEST Product` priced $999,999,999.00 in /products.json.
    -> placeholder_product.
  * 2026-09-26 headandshoulders.com: a "where to buy" brand site pricing EVERY storefront variant at
    1.00 (120 of 120). The drain applied 74 PDPs / 152 offers at $1.00 unflagged: $1.00 clears the
    per-row rule, and the signal is store-wide. Measured the same day over every store this lane has
    written (186 store/currency cohorts, 39,514 offers): headandshoulders.com is the only one whose
    modal price is <= 2.00 at a modal share over 0.22, or whose <= 1.00 share is over 0.20 (next:
    timelybasket.com, 10/46 at 2.00 and 9/46 at 1.00).
    honest.com's storefront carries 125 of 317 variants at 1.00, mostly diapers. -> placeholder_price_store,
    judged over every record the crawl emitted for the job (already scoped to the job's vendors by
    records_for_brand -- not the whole store), before exclusions or a category filter narrow it.
  * Rows the opt-in lip title door placed (#2257) are a per-row BLOCK: unattended, nothing but the
    title vouches for them (review of #2263: "Lipstick Poster" typed "Misc" reaches a lip leaf).

A BLOCK flag holds the job for review; INFO never does. A flag's `key` is stable across runs of
the same cohort (rule + handle), so an approval can accept exactly the flags it looked at.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

BLOCK = "block"
INFO = "info"

_LIP_PREFIX = "beauty/makeup/lip/"
_SETS_PREFIX = "beauty/sets/"

# A lip product's own copy talks about lips. Measured 2026-09-23 on the 10 lip products applied by
# hand plus the k-touch.us tone-up cream: every real one mentions lips; the tone-up cream's copy
# ("brighten ... your complexion", "dull skin") and a Soft Matte Lipstick page carrying a rice
# CLEANSER's pasted description never do. A face-word list was tried first and held 4 of the 10 real
# lipsticks ("flatters every skin tone", "brightens the complexion") -- lipstick copy uses those words.
_LIP_WORD = re.compile(r"\b(?:lips?|lipsticks?|lip\s*(?:colou?r|tint|gloss|balm|liner|stain)|pout|mouth|smile)\b|립|입술", re.I)
_MIN_COPY_CHARS = 60  # shorter copy is not evidence either way
# Balms and scrubs are the lip leaves whose copy talks about "chapped skin" and whose tins and jars
# run 20-30 g (Vaseline Lip Therapy 20g, Sugar Lip Scrub 30g): the copy and size rules skip them.
_LIP_COLOUR_LEAVES = frozenset({"beauty/makeup/lip/lipstick", "beauty/makeup/lip/tint", "beauty/makeup/lip/gloss",
                                "beauty/makeup/lip/liner", "beauty/makeup/lip/oil"})
# Area leaves the non-face rule (#2248) files a product under on purpose; its title names the face
# leaf it was moved AWAY from ("Hand Cream" -> body/care), which is not a contradiction.
_AREA_LEAF = re.compile(r"^beauty/(?:body|haircare)/")
# A set/kit/multi-pack filed as ONE product: its own shelf is beauty/sets. A count of 1 ("1pc",
# "1ea" -- how Korean retailers label single items, or "1 x 50ml") is a single product.
_SET_TITLE = re.compile(r"\b(?:sets?|kits?|bundles?|trio|(?:[2-9]|\d{2,})\s*-?\s*(?:pcs?|pieces?|ea)|special\s+edition|"
                        r"duo\s+edition|double\s+edition|(?:[2-9]|\d{2,})\s*x\s*\d+\s*(?:ml|g))\b", re.I)
# "Set" the VERB, in a product-line name: held 2026-09-27 on stilacosmetics.com "Stay All Day® Smudge &
# Set™ Waterproof Gel Eye Liner". An ALLOWLIST, measured over 283,801 titles on 86 stores (reports/*/catalogs):
# every "X & Set" / "Set & X" verb pair the corpus uses, and "Set" + the setting product it names. Guessing which
# partners are NOT verbs failed three review rounds ("Set & Save", "Set & Tote", "Cleanse & Set"): a real set
# left held costs a reviewer one click, a single product waived goes live mis-shelved.
_VERB_BEFORE_SET = re.compile(r"\b(?:prime|mist|smooth|smudge|bake|perfect|twist|brighten|prep|shape|spray|grip|blow)"
                              r"[™®]*\s*(?:&|and)\s*$", re.I)
_VERB_AFTER_SET = re.compile(r"^\s*(?:&|and)\s*(?:protect|keep|wave|correct|stay|flow)\b", re.I)
_SETTING_NOUN = re.compile(r"^\s+(?:powder|spray|finishing|setting|fixer|translucent|loose|pressed)\b", re.I)
_HAIR_SET_NOUN = re.compile(r"^\s+(?:lotion|essence)\b", re.I)  # "Curl Set Lotion", "Quick Dry Set Lotion"
_HAIR_SET_WORD = re.compile(r"^(?:curl|curls|dry)$", re.I)
# A set, area or product word right before "Set" makes it the noun: "Starter Set", "Lip Set", "Toner Set".
_SET_WORD = re.compile(r"^(?:starter|discovery|trial|travel|holiday|mini|minis|sample|best-?sellers?|care|"
                       r"makeup|skincare|beauty|deluxe|festive|lips?|eyes?|brows?|lash(?:es)?|face|nails?|body|skin)$", re.I)
# Anything joined after the verb (beyond its own pair) is a second item: "Eyeliner & Sharpener", "Powder + Puff",
# "Eyeliner Black & Brown".
_JOIN_WORD = re.compile(r"&|\band\b|\bwith\b|\+", re.I)
# ...and never when the title also names a pack: "Prime & Set Duo", "Mini Prime & Set Pouch", "Gel Eye Liner
# 2 Pack", "Setting Powder (Pack of 2)", "Set Powder Loose Twin Pack", "Loose Powder 2ct", "2 Count", "(2 pk)",
# "Double Pack", "2 Units", "2 Bottles".
_PACK_WORD = re.compile(r"\b(?:duos?|pouch|vault|collection|gift|value|twin|double\s+pack|"
                        r"(?:[2-9]|\d{2,})\s*-?\s*(?:packs?|pk|ct|count|units?|bottles?)|pack\s+of\s+\d+|x\s*[2-9])\b", re.I)
# On the sets shelf a title may name a leaf other than the shelf only when it shows it IS a set, by STRONG
# evidence alone: a pack or count word, a free gift/refill, or products joined by "+". "&"/"and"/"with" are
# not evidence ("Hair & Body Shampoo", "Cream with Ceramides", "Face and Body Wash" are one product): a real set
# left held costs a reviewer one click, a single product waived goes live mis-shelved (review of #2402).
# ("Duo", "Trio", "Collection", "Essentials", "Routine" need no word here: the classifier files those titles to
# the gift-set leaf itself. "Quad" is one palette, not a set.)
# Measured 2026-09-27 over the rows of reports/*/catalogs the real producer files under beauty/sets/ whose
# title names another leaf -- see the PR for the table. Single products a store TYPES as a set still hold:
# koolseoul "Skincare Set": "AHC Renew Age Total Reset Cream 50ml", "Julyme ... Perfume Hair Oil 30ml" (two
# leaves, one product); image118 files a UV hoodie ("爽壁+") under gift sets.
_SET_PACK = re.compile(r"\b(?:regimen|system(?=\s*(?:$|[-–(,|]|for\b))|"
                       r"(?:sample|variety|double|value|discovery)\s+pack|gift\s+(?:box|basket)|"
                       r"(?:[2-9]|\d{2,})\s*-?\s*(?:packs?|vials))\b|\+\s*(?:free|refill)\b", re.I)
# ("N types/kinds" is a pick-one variant and "N-step" a single mask far more often than a set -- measured
# 3 sets vs 171 / 116 single-shelf rows, review of accfb44cb -- so neither is evidence.)
# "+" joining products: only with a space, "(" or a size before it -- not "Cica+ Cream", "SPF50+", "PA+++" or a
# CJK "爽壁+". ("System" is a kit only at the end or before "for ...": not "Acne Treatment System Gel".)
_PLUS_JOIN = re.compile(r"(?:(?<=\s)|(?<=\()|(?<=\dml)|(?<=\dg)|(?<=\doz))\+", re.I)
# "Hair"/"Scalp" alone name this leaf: an area word, not a product ("Hair + Body Shampoo").
_AREA_ONLY_LEAVES = frozenset({"beauty/haircare/general"})
# ...and each joined part carries its own size or count ("Toner 200ml + Lotion 200ml"): "Concealer + Foundation",
# "Sunscreen + Primer" name one hybrid product.
_PART_SIZE = re.compile(r"\d+(?:\.\d+)?\s*(?:ml|g|oz|fl\.?\s*oz|l|ea|pcs?|pads?|sheets?|ct|capsules?)\b", re.I)

# A lip product ships in a few ml/g (balm tins reach ~15-18 g); 20+ is a face or body format.
_SIZE = re.compile(r"(\d+(?:\.\d+)?)\s*(ml|g|oz|fl\.?\s*oz)\b", re.I)
_PLACEHOLDER = re.compile(r"\b(?:test\s*product|dummy|do\s+not\s+buy|placeholder|sample\s+product)\b", re.I)

# Store-level placeholder pricing (placeholder_price_store), judged over every record the crawl emitted
# for the job (before exclusions and the category filter; see placeholder_price_store_verdict). Measured 2026-09-26 (see the module docstring): every threshold in the grid min 10-30 /
# modal share 0.6-0.9 / modal ceiling 2-5 / <=1.00 share 0.2-0.4 held headandshoulders.com and nothing
# else. The values below sit between the placeholder stores and the nearest legitimate ones.
_STORE_MIN_VARIANTS = 20       # a smaller cohort is not evidence of a store-wide price
_STORE_MODAL_SHARE = 0.80      # headandshoulders.com 1.00; max legitimate store with a mode <= 2.00: 0.217
_STORE_MODAL_CEILING = 2.00    # a flat $10 / $23 store (biodance.com 0.53 at 23.00) is a real price list
_STORE_TOKEN_PRICE = 1.00      # the "see store" token price
_STORE_TOKEN_SHARE = 0.30      # honest.com 0.39-0.40; max legitimate: timelybasket.com 0.196


def _pdp(record: Any) -> Dict[str, Any]:
    pdp = record.get("pdp") if isinstance(record, dict) else None
    return pdp if isinstance(pdp, dict) else {}


def _handle(record: Dict[str, Any]) -> Optional[str]:
    from services.catalog_enrichment_agent.ingestion import listing_handle
    # The FIRST offer that names a product decides, as before listing_handle existed (a /products/ URL
    # with an empty handle still answers None rather than falling through to the next offer).
    for offer in record.get("offers") or []:
        url = str((offer or {}).get("canonical_url") or "")
        handle = listing_handle(url)
        if handle or "/products/" in url:
            return handle
    return None


# Placeholder price bounds were written in dollars (a $999,999,999 TEST product; a $1 "see store" token) and
# were applied to every currency's numbers as-is, so any JPY or KRW row priced 1,000 or more -- nearly every
# real one -- was a placeholder_product BLOCK (found 2026-10-10 building the SG capture: JPY base crawls of
# Japanese retailers could never apply unattended). The bounds now scale by the currency's ORDER OF MAGNITUDE
# against the dollar. This is a coarse unit, not FX: no price is converted, compared across currencies or
# written; a 10x margin either way still separates a token or a test row from a real price list. A currency
# not listed (or none) keeps the dollar bounds, exactly today's behaviour.
# Each value sits at or BELOW the currency's usual rate to the dollar, so a scaled upper bound holds a little
# earlier than in dollars (fails safe) and a scaled token bound never reaches a common real price point (CNY's
# 9.9 trial size sits under no token bound at 5).
_PRICE_MAGNITUDE = {
    "JPY": 100.0, "KRW": 1000.0, "HKD": 5.0, "CNY": 5.0, "TWD": 30.0, "THB": 30.0, "PHP": 50.0,
    "INR": 50.0, "IDR": 10000.0, "VND": 20000.0, "MYR": 4.0, "SEK": 10.0, "NOK": 10.0, "DKK": 5.0,
    "MXN": 15.0, "ZAR": 15.0,
}


def _currency(record: Dict[str, Any]) -> str:
    return str(_pdp(record).get("currency") or "").strip().upper()


def price_magnitude(currency: Optional[str]) -> float:
    """How many units of `currency` sit where one dollar does in the placeholder bounds (1.0 if unknown)."""
    return _PRICE_MAGNITUDE.get(str(currency or "").strip().upper(), 1.0)


def _prices(record: Dict[str, Any]) -> List[float]:
    out = []
    for offer in record.get("offers") or []:
        try:
            out.append(float((offer or {}).get("price")))
        except (TypeError, ValueError):
            continue
    return out


def _variant_prices(record: Dict[str, Any]) -> List[float]:
    """Every variant's own price (pdp.variants, one per sellable storefront variant), rounded to the
    cent; the offer prices when the record carries no variants."""
    out = []
    for variant in _pdp(record).get("variants") or []:
        try:
            out.append(round(float((variant or {}).get("price")), 2))
        except (TypeError, ValueError, AttributeError):
            continue
    return out or [round(p, 2) for p in _prices(record)]


def placeholder_price_store_verdict(population: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Does this population price its catalogue with a token? {"ceiling", "evidence"} or None.

    `population` must be every record the CRAWL emitted for the job -- before exclude_handles and before
    the category filter. Judged on the narrowed cohort instead, the verdict was released by the very
    approval it should survive (review of #2371): excluding 6 of 25 $1 rows dropped the count under the
    minimum and applied the other 19 unanswered, and an only_category lip slice of an all-$1 store kept
    10 rows and applied them at $1. It is still NOT the whole store: records_for_brand's vendor filter
    has already scoped the crawl to the job's brands."""
    population = [record for record in population or [] if _pdp(record)]
    every = [p for record in population for p in _variant_prices(record)]
    total = len(every)
    # One storefront prices in one currency (its /meta.json; the pipeline stops a job whose records disagree).
    # Should a population ever mix currencies, take the LARGEST scale present: deterministic, and it fails
    # safe -- a larger token bound holds more, never fewer.
    scale = max((price_magnitude(_currency(record)) for record in population), default=1.0)
    token_price, modal_ceiling = _STORE_TOKEN_PRICE * scale, _STORE_MODAL_CEILING * scale
    if total < _STORE_MIN_VARIANTS:
        return None
    counts: Dict[float, int] = {}
    for p in every:
        counts[p] = counts.get(p, 0) + 1
    # Ties go to the LOWER price: the placeholder is the cheap one.
    modal, modal_n = min(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    token_n = sum(1 for p in every if p <= token_price)
    ceilings = []
    if modal_n / total >= _STORE_MODAL_SHARE and modal <= modal_ceiling:
        ceilings.append(modal)
    if token_n / total >= _STORE_TOKEN_SHARE:
        ceilings.append(token_price)
    if not ceilings:
        return None
    return {"ceiling": max(ceilings),
            "evidence": (f"store-wide placeholder pricing: {modal_n}/{total} variants at {modal:.2f}, "
                         f"{token_n}/{total} at or below {token_price:.2f}")}


def placeholder_price_store_flags(records: Iterable[Dict[str, Any]],
                                  verdict: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """placeholder_price_store: a BLOCK per row of `records` (the cohort being checked) whose prices are
    ALL at or below the verdict's placeholder price -- keyed like every row flag, so an operator can accept
    a real $1 item or exclude rows. A row with any price above it is a real product carrying a
    token-priced variant, and is not held. An excluded row is not in `records`, so it gets no flag."""
    if not verdict:
        return []
    ceiling = verdict["ceiling"]
    flags = []
    for record in records or []:
        prices = _variant_prices(record) if _pdp(record) else []
        if prices and max(prices) <= ceiling:
            flags.append(_flag("placeholder_price_store", BLOCK, record,
                               f"{verdict['evidence']}; every price of this row is <= {ceiling:.2f} "
                               f"({sorted(set(prices))[:4]})"))
    return flags


def _size_units(text: str) -> List[float]:
    """Sizes normalised to ml/g (oz x 28.35 is close enough for a 20 cut)."""
    sizes = []
    for number, unit in _SIZE.findall(text or ""):
        value = float(number)
        if "oz" in unit.lower():
            value *= 28.35
        sizes.append(value)
    return sizes


def _beauty_leaves(text: str) -> set:
    from services.curated_brand_feed import _title_paths
    return {p for p in _title_paths(text) if p.startswith("beauty/")}


def _set_is_the_verb(title: str, m: "re.Match") -> bool:
    if m.group(0).lower() != "set" or _PACK_WORD.search(title):
        return False
    before, after = title[:m.start()], title[m.end():].lstrip("™®")
    words = before.split()
    prev = words[-1].strip("™®()[],.:;-–") if words else ""
    if prev and (_beauty_leaves(prev) or _SET_WORD.match(prev)):
        return False
    joins = len(_JOIN_WORD.findall(after))
    if _VERB_BEFORE_SET.search(before):              # "Smudge & Set™ Waterproof Gel Eye Liner"
        return joins == 0
    if _VERB_AFTER_SET.match(after):                 # "Set & Stay Makeup Spray": its own "&" only
        return joins == 1
    if _SETTING_NOUN.match(after) or (_HAIR_SET_NOUN.match(after) and _HAIR_SET_WORD.match(prev)):
        return joins == 0 and not before.rstrip().endswith("+")   # not "Primer + Set Spray"
    return False


def _names_a_set(title: str) -> bool:
    """_SET_TITLE, except a "set" that is the verb of a product-line name (see _VERB_BEFORE_SET)."""
    return any(not _set_is_the_verb(title, m) for m in _SET_TITLE.finditer(title))


def _joined_products(title: str) -> int:
    """How many "+"-joined parts of the title name a sized product ("Toner 200ml + Lotion 200ml" -> 2)."""
    return sum(1 for part in _PLUS_JOIN.split(title)
               if _beauty_leaves(part) - _AREA_ONLY_LEAVES and _PART_SIZE.search(part))


def _title_shows_a_set(title: str) -> bool:
    return bool(_names_a_set(title) or _SET_PACK.search(title) or _joined_products(title) >= 2)


def _flag(rule: str, severity: str, record: Dict[str, Any], detail: str) -> Dict[str, Any]:
    pdp = _pdp(record)
    handle = _handle(record)
    return {
        "key": f"{rule}:{handle or pdp.get('product_name') or '?'}",
        "rule": rule,
        "severity": severity,
        "handle": handle,
        "product_name": pdp.get("product_name") or pdp.get("title"),
        "category_path": pdp.get("category_path"),
        "merchant_product_type": pdp.get("category_source_product_type"),
        "detail": detail,
    }


_OWN = object()


def detect(records: Iterable[Dict[str, Any]], *, store_level: bool = True,
           store_verdict: Any = _OWN) -> List[Dict[str, Any]]:
    """Per-row rules over each record, then (store_level) placeholder_price_store on the rows of `records`.

    The store verdict must be judged over every record the crawl emitted for the job
    (placeholder_price_store_verdict), NOT over `records` once exclusions or a category filter narrowed
    them: the pipeline computes it before narrowing and passes it as `store_verdict`. Omitted, it is
    judged over `records` themselves -- correct only when they are the whole crawl. A caller re-checking
    rows it already judged passes store_level=False."""
    from services.curated_brand_feed import (CATEGORY_CONFIDENCE_LASH_NAIL_TITLE, CATEGORY_CONFIDENCE_LIP_TITLE,
                                             _NON_FACE_TITLE, _title_paths)

    flags: List[Dict[str, Any]] = []
    cohort: List[Dict[str, Any]] = []
    for record in records or []:
        pdp = _pdp(record)
        if not pdp:
            continue
        cohort.append(record)
        title = str(pdp.get("product_name") or pdp.get("title") or "")
        category = str(pdp.get("category_path") or "")
        copy = str(pdp.get("attribute_summary") or "")
        variant_titles = " ".join(str((v or {}).get("title") or "") for v in pdp.get("variants") or []
                                  if isinstance(v, dict))
        is_lip = category.startswith(_LIP_PREFIX)

        if is_lip:
            if (category in _LIP_COLOUR_LEAVES and len(copy.strip()) >= _MIN_COPY_CHARS
                    and not _LIP_WORD.search(copy)):
                flags.append(_flag("lip_row_copy_not_about_lips", BLOCK, record,
                                   f"filed under {category} but its {len(copy)}-char description never "
                                   f"mentions lips: {copy.strip()[:90]!r}"))
            big = [s for s in _size_units(f"{title} {variant_titles}") if s >= 20] \
                if category in _LIP_COLOUR_LEAVES else []
            if big:
                flags.append(_flag("lip_row_implausible_size", BLOCK, record,
                                   f"filed under {category} at {max(big):g} ml/g; lip formats are a few ml/g"))
            try:
                door = abs(float(pdp.get("category_confidence")) - CATEGORY_CONFIDENCE_LIP_TITLE) < 1e-6
            except (TypeError, ValueError):
                door = False
            if door:
                # Unattended, no merchant type vouches for this row, and no word list can name every
                # non-product ("Lipstick Poster"). So it holds the cohort until someone accepts this
                # row's key -- the pipeline's form of an operator reading each placed row.
                flags.append(_flag("placed_by_lip_title", BLOCK, record,
                                   "category from the product's own title (--lip-title-evidence); "
                                   "no merchant type vouches for it -- accept this row to apply it"))

        try:
            lash_nail_door = abs(float(pdp.get("category_confidence")) - CATEGORY_CONFIDENCE_LASH_NAIL_TITLE) < 1e-6
        except (TypeError, ValueError):
            lash_nail_door = False
        if lash_nail_door:
            # The lip door's rule, for its lash/nail twin: only the title vouches for this row.
            flags.append(_flag("placed_by_lash_nail_title", BLOCK, record,
                               "category from the product's own title (options.lash_nail_title_evidence); "
                               "no merchant type vouches for it -- accept this row to apply it"))

        # The title names one or more leaves and the row is filed under none of them: the merchant
        # type (or a measured shelf) and the product's own name disagree. "Essence Toner" on a
        # "Cleansers" shelf names serum AND toner -- neither is cleanser.
        named = _title_paths(title)
        # Exempt only what the non-face rule moved on purpose: an area leaf whose OWN title names that
        # area ("Hand Cream" -> body/care). A "Vitamin C Serum" typed "Hair" is still a contradiction.
        moved_by_area_rule = bool(_AREA_LEAF.match(category) and _NON_FACE_TITLE.search(title))
        # A set's title names what is IN it ("Glow Pot Eyeshadow & Brush" on beauty/sets): held 20 of
        # tartecosmetics.com's 23 title flags 2026-09-27. Only a title that shows it is a set: a single
        # product the store types "Skincare Set" is still a contradiction (_SET_PACK, _PLUS_JOIN).
        on_sets_shelf = category.startswith(_SETS_PREFIX)
        set_named_by_title = on_sets_shelf and _title_shows_a_set(title)
        if category and named and category not in named and not moved_by_area_rule and not set_named_by_title:
            flags.append(_flag("title_contradicts_category", BLOCK, record,
                               f"filed under {category}; the title names {sorted(named)}"))

        if category and not on_sets_shelf and _names_a_set(title):
            flags.append(_flag("set_filed_as_single_product", BLOCK, record,
                               f"the title names a set/multi-pack but it is filed under {category}"))

        prices = _prices(record)
        vendor = str(pdp.get("brand") or "")
        scale = price_magnitude(_currency(record))
        # The low bound also reads every VARIANT price: the feed's sellable floor (MIN_SELLABLE_PRICE = 1.0 in
        # the store's currency) drops a $0.01 promo variant in dollars, but in yen it is 1 yen, so a 1-yen
        # "free sample" variant survives into pdp.variants while the offer carries the real price (review of
        # this change, 2026-10-10). In dollars nothing changes: no variant under $1 reaches a record.
        token_variants = sorted({p for p in _variant_prices(record) if p <= 0.5 * scale})
        if (_PLACEHOLDER.search(title) or re.search(r"\bdev\b", vendor, re.I)
                or any(p <= 0.5 * scale or p >= 1000 * scale for p in prices) or token_variants):
            # Name a token VARIANT in the detail: the offer may carry only the real price, and a reviewer who
            # saw "prices [5500.0]" would accept the key -- applying exactly the 1-yen offer this rule stops.
            flags.append(_flag("placeholder_product", BLOCK, record,
                               f"looks like a test/placeholder row (prices {sorted(set(prices))[:4]}"
                               + (f"; token variant prices {token_variants[:4]}" if token_variants else "")
                               + ")"))
    if store_level:
        verdict = placeholder_price_store_verdict(cohort) if store_verdict is _OWN else store_verdict
        flags.extend(placeholder_price_store_flags(cohort, verdict))
    return flags


def listing_collision_flags(collisions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """same_key_other_listing: one flag per listing the plan left out because an earlier listing on
    the same host carries the same title, and so the same content key (ingestion.ingest_validated_jsonl).

    Leaving it out is what keeps a buyer from seeing one page and buying another; the flag says what
    was left out. INFO only when both pages look like one product listed twice -- the same prices, the
    same merchant product type AND the same image (an ad landing clone, a region copy). Otherwise BLOCK,
    and a reviewer decides: a different price or type is how a different product wearing the same title
    shows (COCODOR "Black Cherry" refill $6.99 vs diffuser $11.19; Mr. Smith full size vs sachet), and a
    different image at one price is a shade (us.mcobeauty.com lists each "Dream Liquid Dewy Blush" shade
    as its own $5.99 product). Accepting the key (or options.accept_listing_collisions) applies the cohort
    without that listing; exclude_handles on the KEPT listing makes the other one the row's listing."""
    flags = []
    for c in collisions or []:
        kept, dropped = c.get("kept") or {}, c.get("dropped") or {}
        same = (bool(kept.get("prices")) and kept.get("prices") == dropped.get("prices")
                and kept.get("product_type") == dropped.get("product_type")
                and bool(kept.get("image")) and kept.get("image") == dropped.get("image"))
        flags.append({
            "key": f"same_key_other_listing:{dropped.get('handle')}",
            "rule": "same_key_other_listing",
            "severity": INFO if same else BLOCK,
            "handle": dropped.get("handle"),
            "product_name": dropped.get("product_name"),
            "category_path": None,
            "merchant_product_type": dropped.get("product_type"),
            "detail": (f"left out: {c.get('host')} lists {dropped.get('handle')!r} (type "
                       f"{dropped.get('product_type')!r}, prices {(dropped.get('prices') or [])[:4]}, image "
                       f"{dropped.get('image')!r}) under the same title as {kept.get('handle')!r} (type "
                       f"{kept.get('product_type')!r}, prices {(kept.get('prices') or [])[:4]}, image "
                       f"{kept.get('image')!r}), which keeps {c.get('product_key')}"),
        })
    return flags


def listing_move_flags(moves: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """listing_moved: a row names a listing on this host that the crawl does not carry, so the plan held
    every record of it (ingestion.ingest_validated_jsonl never moves a row). Always BLOCK and never covered
    by options.accept_listing_collisions: accepting `listing_moved:<product_key>` is what lets the row move
    to the listing named in the detail (a renamed handle, an unpublished page); a crawl that merely dropped
    the page is answered by re-running it."""
    return [{
        "key": f"listing_moved:{m.get('product_key')}",
        "rule": "listing_moved",
        "severity": BLOCK,
        "handle": m.get("would_keep"),
        "product_name": None,
        "category_path": None,
        "merchant_product_type": None,
        "detail": (f"held: {m.get('host')} row {m.get('product_key')} names {m.get('current')!r}, which this "
                   f"crawl does not carry; accepting moves it to {m.get('would_keep')!r} "
                   f"(crawled {list(m.get('crawled') or [])[:5]})"),
    } for m in moves or []]


def blocking(flags: Iterable[Dict[str, Any]], *, accepted: Iterable[str] = ()) -> List[Dict[str, Any]]:
    """The BLOCK flags an approval has not accepted by key (cohort-level flags are never accepted)."""
    ok = set(accepted or ())
    # A cohort-level flag (plan not ready, a guard conflict) names no row: accepting its key would
    # accept every future instance of it, so it can never be accepted -- only fixed.
    return [f for f in flags if f.get("severity") == BLOCK
            and (f.get("acceptable") is False or f.get("key") not in ok)]
