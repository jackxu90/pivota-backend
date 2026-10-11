"""PDP category classifier shared by:

- scripts/backfill_pdp_category_path.py (Phase 2 — populate catalog_products.category_path)
- services/pivot_query_service.py recall path (Phase 2b — bias recall toward
  category_path matches when the query is a category alias)

Patterns ported from PIVOTA-Agent-mainline-verify/src/services/externalSeedProducts.js
BEAUTY_CATEGORY_PATTERNS, augmented with explicit taxonomy paths.

DRY rule: the patterns live HERE. Both the backfill script and the search
path import from this module. Adding/removing a pattern updates everywhere
at once.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

# A base-makeup product that carries an SPF claim is FOUNDATION, not sunscreen:
# "Foundation Broad Spectrum SPF 50+" is a foundation. The leading negative
# lookahead declines those titles here so they fall through to the Foundation
# pattern further down, which is what a3940018 set out to fix. Doing it this way
# rather than hoisting Foundation to the top of CATEGORY_PATTERNS matters:
# hoisting also lifted Foundation above Primer and Cleanser, which re-labelled
# every "foundation primer" as a foundation. Only sunscreen is narrowed here, so
# a real sunscreen is untouched.
_SUNSCREEN_RE = re.compile(
    r"^(?!.*\b(?:foundation|bb\s+cream|cc\s+cream|skin\s+tint|"
    r"cushion|concealer|primer)\b)"
    r".*\b(sunscreen|sun\s*screen|broad\s+spectrum|spf\s*\d{2,3}\+?|pa\s*\+{2,4}|"
    r"sun\s+(?:serum|fluid|cream|gel|milk|stick)|"
    r"uv\s*(?:protection|shield|defen[cs]e|lock))\b",
    re.IGNORECASE,
)

# Shared vetoes for the lash/nail leaves below. A title naming another area is not a nail product
# first. Spelled like _NON_FACE_AREAS further down -- `(?:eye\s?)?lash`, `(?:eye)?brows?` -- because a
# bare `\blash\b` never matches "Eyelash" (review of #2364: "Eyelash Top Coat" and "Gel Remover for
# Eyelash Extensions" both landed on nail leaves). The lip family is spelled out rather than `lip\w*`,
# which also caught "lipids" ("Cuticle Oil with Lipids"). PRIMER is not an area: a nail base coat
# "& Primer" is a nail product, so primer is vetoed only on the coat arm, and only when no nail word
# is present (_FACE_PRIMER).
_AREA_VETO = (r"lips?|lip(?:sticks?|gloss(?:es)?|liners?|balms?|tints?|stains?|oils?)"
              r"|(?:eye\s?)?lash(?:es)?|(?:eye)?brows?|mascara|eye\s?shadows?|eyeliners?")
# Where a title's first segment -- the phrase that names the product -- ends: a separator ("|",
# ",", " - ", "(" ...) or a preposition/conjunction. "Kiss Nail Glue for ..." -> "Kiss Nail Glue";
# "Nail Glue & Press On Nails" -> "Nail Glue". It carries no leading \s*: a pattern that re-tests
# it at every position scans each whitespace run again from every space in it (20,000 spaces took
# 2.6s).
_SEGMENT_END = r"(?:[|,(&\u2013\u2014]|-(?=\s)|\b(?:for|with|and)\b)"
# A primer is face makeup unless the title also names the nail: "Base Coat Primer" -> face primer,
# "Gel Polish Base Coat & Primer" -> nail polish.
_FACE_PRIMER = r"(?!(?!.*\b(?:nails?|gel|polish|lacquer)\b).*\bprimers?\b)"
_OUTERWEAR = r"wool|cashmere|tweed|(?:wo)?men[\u2019']?s|trench|parka|jackets?|puffer|overcoats?|outerwear"
# Outerwear likewise, but a nail finish word outranks a shade name: "essie Cashmere Matte Top Coat" is
# nail polish, "Men's Wool Top Coat" is a coat.
_NAIL_CUE = r"nails?|gel|polish|lacquer|matte|quick[-\s]?dry|glossy|shine"
_NOT_OUTERWEAR = r"(?!(?!.*\b(?:" + _NAIL_CUE + r")\b).*\b(?:" + _OUTERWEAR + r")\b)"

# (category_label, taxonomy_path, regex). Order matters — more specific
# patterns appear earlier; the first match wins.
CATEGORY_PATTERNS: List[Tuple[str, str, "re.Pattern[str]"]] = [
    # ----- Electronics: camera drones (electronics_drone sub-vertical) -----
    # First so a "self-flying camera" / "camera drone" gets an electronics path
    # instead of falling through to a beauty/fashion rule or NULL. Tokens are
    # drone-specific (no bare "camera"), so no beauty SKU is miscategorized.
    ("Camera Drone", "electronics/drones/camera-drone", re.compile(
        r"\b(drones?|quadcopters?|fpv\s+drone|uav|self[-\s]?flying\s+camera|"
        r"flying\s+camera|camera\s+drone|follow[-\s]?me\s+drone)\b",
        re.IGNORECASE)),
    # ===== Beauty DEVICE family (beauty_device_* sub-profiles). =====
    # Own beauty/devices/* subtree, distinct from beauty/tools/* (makeup
    # applicators) and topical beauty/skincare|haircare. All are BEFORE the makeup
    # "Brush" / generic "Hair Care" / "Mask" patterns so a heat/energy/light
    # device is a DEVICE, not a topical mask/brush. Each has a leading negative
    # lookahead for topical FORM nouns so a formulation that merely names a tool
    # ("hair removal cream", "led-boosting serum", "flat iron spray") falls through
    # to its real topical pattern. hair-removal + skincare FIRST because they carry
    # "hair"/"mask"/"facial" that lower topical patterns would otherwise grab.
    ("Hair Removal Device", "beauty/devices/hair-removal", re.compile(
        r"^(?!.*\b(?:cream|wax|gel|foam|lotion|spray|serum|mousse|strips?)\b)"
        r".*\b(ipl(?:\s+hair\s+removal)?|laser\s+hair\s+removal|"
        r"hair\s+removal\s+(?:device|handset|system)|epilator)\b",
        re.IGNORECASE)),
    ("Skincare Device", "beauty/devices/skincare-energy", re.compile(
        r"^(?!.*\b(?:sheet\s+mask|cream|serum|ampoule|essence|toner|spray)\b)"
        r".*\b(led\s+(?:face\s+)?mask|light\s+therapy\s+(?:mask|device)|"
        r"red\s+light\s+therapy|microcurrent|nanocurrent|"
        r"radio\s?frequency\s+(?:device|wand)|high[-\s]?frequency\s+wand|"
        r"microdermabrasion|derma\s?roller|microneedl\w*|"
        r"skin\s+tightening\s+device|facial\s+toning\s+device)\b",
        re.IGNORECASE)),
    ("Facial Cleansing Device", "beauty/devices/facial-cleansing", re.compile(
        r"^(?!.*\b(?:cream|gel|foam|oil|balm|milk|powder)\b)"
        r".*\b(facial\s+cleansing\s+brush|cleansing\s+brush|"
        r"sonic\s+(?:facial\s+)?cleanser|facial\s+cleansing\s+device)\b",
        re.IGNORECASE)),
    ("Nail Device", "beauty/devices/nail", re.compile(
        r"\b(uv[-\s/]?led\s+(?:nail\s+)?lamp|nail\s+lamp|gel\s+lamp|nail\s+dryer|"
        r"electric\s+nail\s+file|nail\s+drill)\b",
        re.IGNORECASE)),
    # ===== Lash and nail products (Peng 2026-09-26: they get leaves). =====
    # Before these, a false lash or a nail product had NO honest leaf and fell to whatever noun it
    # also carried: "Peel Off Nail Polish" -> exfoliant (peel/polish), a dip powder -> face powder,
    # "Top Coat" -> a fashion coat, "24 Nails" -> nothing. A curated crawl with only_resolved_category
    # therefore dropped them all (universalnailsupplies.com: 141 OPI products crawled, 5 planned --
    # the 5 mis-filed dip powders). The nail paths are the ones prod rows and the gateway already use
    # (PIVOTA-Agent's queryUnderstanding browses `beauty/makeup/nails/nail-polish/` and keeps removers
    # out of it); false lashes join the singular `eye/` tree where mascara and eyeliner live.
    # They sit ABOVE Brush, Cleanser, Exfoliant, Powder, Mascara and Coat so first-match-wins reaches
    # them. Each needs an explicit phrase -- never a bare "nail" ("Nailed It Cleansing Balm" stays a
    # cleanser), never a bare "polish" (a face or body polish stays an exfoliant), never a bare
    # "lash" (mascara, lash serums, lifts and curlers keep their homes). A title that also names
    # another area is declined through the shared vetoes above ("Eyelash Top Coat", "Lipstick Top
    # Coat", "Hand & Cuticle Cream" -- a hand product is hand care, as non_face_leaf says).
    # Remover BEFORE polish, and polish refuses a trailing "remover", so a remover product type
    # counts as ONE match in services/curated_brand_feed._pattern_matches, not an ambiguous two.
    ("False Lashes", "beauty/makeup/eye/false-lashes", re.compile(
        r"^(?!.*\b(?:mascara|serums?|lift(?:ing)?|curlers?|primers?|conditioners?|growth|tints?|"
        r"perm|removers?|lip\w*)\b).*\b(?:"
        r"(?:false|fake|faux|mink|magnetic|strip|individual|cluster|wispy)\s+(?:eye\s?)?lash(?:es)?"
        r"|(?:eye\s?)?lash\s+(?:clusters?|wisps?|strips?|bands?|glue|adhesive)"
        # DUO spells its strip-lash glue as one word: "Duo Brush On Striplash Adhesive".
        r"|striplash(?:es)?"
        # Bare "falsies" is also Maybelline's MASCARA line ("Falsies Surreal Extensions"), so only
        # KISS's lash spellings count.
        r"|(?:impress|kiss)\s+falsies|falsies\s+(?:press[-\s]?on|lash(?:es)?|clusters?)|falscara|wispies"
        r"|faux\s+mink"
        r")\b",
        re.IGNORECASE | re.DOTALL)),
    # DUO is a lash-adhesive brand, and its own titles do not always say "lash": "Duo: Brush-On Dark
    # Adhesive with Vitamins" (shopbeautydepot.com 2026-09-26) was a makeup brush via "Brush-On".
    # Only a title the brand HEADS counts ("Lash Extension Adhesive Duo" is a two-pack, not the brand),
    # and a DUO remover is not an adhesive.
    ("False Lashes", "beauty/makeup/eye/false-lashes", re.compile(
        r"^duo\b(?!.*\bremovers?\b).*\badhesives?\b",
        re.IGNORECASE | re.DOTALL)),
    # Two entries, one path (_pattern_matches counts distinct PATHS, so a type matching both is still
    # one match). The explicit "press-on nails" phrase wins even when the title lists its glue, file
    # or stickers ("24 Pcs Press On Nails with Glue and Mini File"); the looser arms -- false/fake
    # nails, nail tips, "24 Nails" -- decline any accessory, care or tool word. NOT gel/acrylic nails:
    # "Gel Nail Color" is polish and an "Acrylic Nails & Tips" shelf is mostly powders (review).
    # The forgiveness holds only while the NAILS are the head noun. Declined (kissusa.com 2026-09-26):
    # a product FOR them ("Kiss Nail Glue for Press On Nails", as the stricter arm below), a glue,
    # adhesive or lamp that ends the title's first segment BEFORE the phrase ("Kiss Salon X-tend LED
    # Gel Adhesive | Clear, Press On Nail Gel Glue", "Kiss Salon X-tend Pro LED Lamp | White, Soft
    # Gel Press On Nail Curing Light"), and a glue named right after the phrase unless it is
    # "included" ("Press On Nail Gel Glue 10g", "... Nails Brush-On Glue"). A glue or lamp further
    # AFTER the phrase is a listed accessory or a design name, and the phrase wins ("24 Pcs Press On
    # Nails with Glue and Mini File", "Press On Nails Jelly Glue", "... Nails Lava Lamp"), as it
    # does when the glue qualifies the nails ("Jelly Glue Press On Nails").
    ("Press-On Nails", "beauty/makeup/nails/press-on-nails", re.compile(
        r"^(?!.*\bfor\s+(?:\w+\s+){0,2}press[-\s]?on\b)"
        r"(?!(?:(?!" + _SEGMENT_END + r"|\bpress[-\s]?on\b).)*?\b(?:glue|adhesives?|lamps?)\s*" + _SEGMENT_END + r")"
        r"(?!.*\b(?:polish|lacquer|removers?|" + _AREA_VETO + r")\b).*"
        r"\bpress[-\s]?on\s+(?:nails?|manicures?)\b"
        r"(?!\s+(?:(?:gel|brush[-\s]?on|nail)\s+)?(?:glue|adhesives?|stickers?|decals?|removers?)\b"
        r"(?!\s+included\b))",
        re.IGNORECASE | re.DOTALL)),
    # KISS names its glue-on line "Press On (Fake) Glue Nails" / "Press On Soft Gel Nails" / "Press On
    # Glue Toenails" (kissusa.com 2026-09-26: ~500 titles matched nothing), so up to three of those words
    # may sit between "press on" and "(toe)nails". This arm is stricter than the one above: a product
    # FOR the line ("Nail Glue for Press On Glue Nails", "UV Lamp for Press On Soft Gel Nails") and any
    # tool, care, kit or bundle word anywhere decline it (review of #2377), so it widens the explicit
    # phrase to KISS's line names without widening what the phrase above already forgives.
    ("Press-On Nails", "beauty/makeup/nails/press-on-nails", re.compile(
        r"^(?!.*\bfor\s+(?:\w+\s+){0,2}press[-\s]?on\b)"
        r"(?!.*\b(?:polish|lacquer|removers?|(?<!no\s)glue\s+(?!(?:toe)?nails?\b)\w+|adhesives?|tabs?|coats?|oils?|"
        r"primers?|dehydrators?|lamps?|files?|buffers?|clippers?|pushers?|tweezers?|care|prep|cuticles?|"
        r"kits?|sets?|bundles?|stickers?|decals?|" + _AREA_VETO + r")\b).*"
        r"\bpress[-\s]?on\s+(?:(?:fake|false|glue|soft|gel|acrylic)\s+){1,3}(?:(?:toe)?nails?|manicures?)\b"
        r"(?!\s+(?:art\s+)?(?:glue|stickers?|decals?|removers?)\b)"
        r"|^(?!.*\bfor\s+(?:\w+\s+){0,2}press[-\s]?on\b)"
        r"(?!.*\b(?:polish|lacquer|removers?|(?<!no\s)glue|adhesives?|coats?|oils?|lamps?|files?|kits?|sets?|"
        r"bundles?|care|cuticles?|" + _AREA_VETO + r")\b).*\bpress[-\s]?on\s+toenails?\b"
        r"(?!\s+(?:art\s+)?(?:glue|stickers?|decals?|removers?)\b)",
        re.IGNORECASE | re.DOTALL)),
    # The loosest arm. Like the two above it refuses a product FOR the nails ("Nail Dehydrator for
    # Fake Nails") and the adhesive, tab, prep and curing words the stricter arm refuses ("Nail
    # Adhesive for False Nails", "Adhesive Tabs for False Nails" were press-on before this).
    ("Press-On Nails", "beauty/makeup/nails/press-on-nails", re.compile(
        r"^(?!.*\bfor\s+(?:\w+\s+){0,2}(?:false|fake|faux|artificial)\s+nails?\b)"
        r"(?!.*\b(?:polish|lacquer|coats?|powders?|liquids?|monomer|brush(?:es)?|files?|"
        r"clippers?|cutters?|drill|lamps?|stickers?|wraps?|decals?|removers?|glue(?![-\s]?on)|adhesives?|"
        r"tabs?|dehydrators?|primers?|care|cuticles?|oils?|tricks|guide|falsies|" + _AREA_VETO + r")\b).*\b(?:"
        r"(?:false|fake|faux|artificial|glue[-\s]?on|stick[-\s]?on)\s+nails?"
        r"|nail\s+tips?|\d+\s*(?:pcs?\s+)?nails"
        r")\b",
        re.IGNORECASE | re.DOTALL)),
    ("Nail Polish Remover", "beauty/makeup/nails/nail-polish-remover", re.compile(
        r"^(?!.*\b(?:makeup|" + _AREA_VETO + r")\b).*"
        r"\b(?:(?:nail\s+)?(?:polish|lacquer|varnish|enamel)|gel(?:\s+polish)?|nail)\s+removers?\b",
        re.IGNORECASE | re.DOTALL)),
    ("Cuticle Care", "beauty/makeup/nails/cuticle-oil", re.compile(
        r"^(?!.*\b(?:hands?|" + _AREA_VETO + r")\b).*"
        r"\bcuticle\s+(?:oils?|serums?|pens?|balms?|creams?|softeners?|treatments?|"
        r"revitali[sz]ers?|removers?)\b",
        re.IGNORECASE | re.DOTALL)),
    # A hair "top coat" (a gloss/colour-seal treatment) names hair or scalp and is declined here, and
    # so is an outerwear top coat ("Men's Wool Top Coat" stays a fashion coat -- see Coat below).
    # MEASURED 2026-09-26 over the 319 prod rows these patterns can touch: 110 re-classify, 109 are
    # nail products; the one miss, "Pureology Color Fanatic Top Coat 6.7 oz", names no hair word at
    # all. It stays on haircare/general because imagebeauty.com types it "Hair Color" (read from
    # the store 2026-09-26) and the product type is classified before the title -- which is also
    # why no hair-brand list is carried here.
    ("Nail Polish", "beauty/makeup/nails/nail-polish", re.compile(
        r"^(?!.*\b(?:hair|scalp|" + _AREA_VETO + r")\b).*\b(?:"
        r"nail\s+(?:polish(?:es)?|lacquers?|varnish(?:es)?|enamels?|colou?rs?|paints?)"
        r"|gel\s+(?:nail\s+)?polish(?:es)?|gel\s+(?:nail\s+)?colou?rs?(?:\s+polish(?:es)?)?"
        r"|dip(?:ping)?\s+(?:powders?|systems?|liquids?)|nail\s+dip|chrome\s+(?:nail\s+)?powders?"
        r")\b(?!\s+removers?\b)",
        re.IGNORECASE | re.DOTALL)),
    # The bare coat arm is its OWN entry (same path): only a bare "top/base coat" needs the outerwear
    # and face-primer vetoes -- on the colour arms above they blocked shade names ("essie Cashmere
    # Matte Nail Polish", "Tweed Your Heart"; review of #2364).
    ("Nail Polish", "beauty/makeup/nails/nail-polish", re.compile(
        r"^(?!.*\b(?:hair|scalp|" + _AREA_VETO + r")\b)" + _NOT_OUTERWEAR + _FACE_PRIMER +
        r".*\b(?:nail\s+)?(?:top|base)\s+coats?\b(?!\s+removers?\b)",
        re.IGNORECASE | re.DOTALL)),
    # Hair-styling tools (VODANA cross-category, beauty x 3C). Negative lookahead
    # vetoes "flat iron spray" / "blow dry primer" / "curl styler cream"; no bare
    # "brush"/"hair"/"styler"; bare "straightener" survives only under the veto;
    # "blow dry" requires "dryer"/"brush"; "flat[\s-]+iron" so "flatiron" (place
    # names) does not match.
    ("Hair Styling Tool", "beauty/devices/hair-styling", re.compile(
        r"^(?!.*\b(?:spray|serum|balm|primer|cream|gel|paste|mist|lotion|mousse|"
        r"wax|pomade|treatment|oil|protectant|pencil|mascara|essence|ampoule)\b)"
        r".*\b(flat[\s-]+iron|hair\s+straightener|straightening\s+brush|"
        r"curling\s+(?:iron|wand|brush)|hair\s+curler|hair\s+dryer|"
        r"blow[-\s]?dryer|blow[-\s]?dry\s+brush|hot\s+air\s+brush|hot\s+brush|"
        r"air\s+styler|hair\s+styler|styling\s+(?:iron|wand)|"
        r"hair\s+styling\s+tool|straightener|"
        r"hair\s+waver|wave\s+iron|deep\s+waver|beachwaver|waver)\b",
        re.IGNORECASE)),
    ("Makeup Sponge", "beauty/tools/sponge", re.compile(
        r"\b(makeup sponge|beauty sponge|sponge\s*/\s*puff|powder puff|blender sponge)\b",
        re.IGNORECASE)),
    ("Brush Pouch", "beauty/tools/brush-accessory", re.compile(
        r"\b(brush bag|brush pouch|brush case|brush holder|brush roll)\b",
        re.IGNORECASE)),
    # "Brush-On" / "Brush On" names how a GLUE or powder is applied, not a brush: a nail glue, a lash or
    # wig adhesive and a sunscreen powder all landed here (kissusa.com, unitedbeautysupply.com,
    # shopbeautydepot.com 2026-09-26). What they fall through to is narrowed too: Exfoliant declines
    # "Applies Like Polish", Gift Set declines the DUO brand, and DUO's lash glues reach False Lashes.
    ("Brush", "beauty/tools/brush", re.compile(
        r"\b(brush(?![-\s]+on\b)|makeup brush|foundation brush|powder brush|blush brush|shader brush|kabuki)\b",
        re.IGNORECASE)),
    ("Shampoo", "beauty/haircare/shampoo", re.compile(
        r"\b(shampoo|dry shampoo|clarifying shampoo)\b", re.IGNORECASE)),
    ("Conditioner", "beauty/haircare/conditioner", re.compile(
        r"\b(conditioner|deep conditioner|leave-in conditioner|leave in conditioner)\b",
        re.IGNORECASE)),
    ("Hair Styling", "beauty/haircare/styling", re.compile(
        r"\b(edge control|styling gel|hair-thickening|hair thickening|"
        r"detangling spray|hair clip|hair clips|edge styling|slick[-\s]?back|"
        r"styling essentials)\b",
        re.IGNORECASE)),
    ("Hair Care", "beauty/haircare/general", re.compile(
        r"\b(hair care|hair repair|repair bundle|maintenance crew|"
        r"detangling|leave-in|leave in|hair|scalp)\b",
        re.IGNORECASE)),
    ("Sunscreen", "beauty/skincare/sun/sunscreen", _SUNSCREEN_RE),
    ("Fragrance", "beauty/fragrance/perfume", re.compile(
        r"\b(perfume|parfum|extrait|extract|eau de parfum|eau de toilette|cologne|body spray|scent)\b|"
        r"\bfragrance\b(?![-\s]?free)\b",
        re.IGNORECASE)),
    ("Cleanser", "beauty/skincare/cleanse/cleanser", re.compile(
        r"\b(cleanser|cleansing|face wash|facial wash|"
        # A "Nail Polish Remover Wipes" type is a nail remover, not a second match; a MAKEUP remover
        # wipe is still a cleanser, so only the nail/polish remover spellings are declined.
        r"cleansing milk|cleansing foam|cleansing gel|face wipes?|cleansing wipes?|"
        # ...and "No-Wipe Top Coat" names no wipe at all.
        r"(?<!polish\sremover\s)(?<!nail\sremover\s)(?<!gel\sremover\s)(?<!no-)(?<!no\s)wipes?|wash)\b",
        re.IGNORECASE)),
    # A SETTING SPRAY is makeup, sprayed over a finished face to hold it -- not a toner, although
    # most are sold as a "mist". It was a declared gap (`beauty/makeup/setting-spray`, "no
    # setting-spray leaf") until 2026-10-10. Measured that day: ~72 drain products on 28 domains
    # left out as category_unresolved, and 23 live rows coarse or misfiled -- 6 of them on the toner
    # leaf through the bare "mist" below ("Makeup Fixing Mist", "You Mist ... Setting Spray").
    # Four segments under face/, so recall's `beauty/makeup/face/` door already reaches it.
    # Only the phrases that NAME the product: a bare "finishing spray" is a hair spray as often as
    # not ("Volumizing Finishing Spray", "Finishing Hairspray"), a bare "fixer" is unread, and
    # "makeup setting" alone names puffs as often as sprays ("Makeup Setting Puffs").
    # ABOVE Toner (and above the acid-pad arm, which must sit directly on Toner), and Toner declines
    # the same phrases, so a merchant type "Setting Mist" or "Mist & Fix" is one match in
    # services/curated_brand_feed._pattern_matches, not two.
    ("Setting Spray", "beauty/makeup/face/setting-spray", re.compile(
        r"\b(setting\s+(?:spray|mist)s?|fix(?:er|ing)?\s+(?:spray|mist)s?|mist\s*(?:&|and|\+)\s*fix|"
        r"make[\s-]?up\s+(?:setting\s+)?fix(?:er|ing)s?|setting\s+fixers?)\b",
        re.IGNORECASE)),
    # TONER GETS ITS OWN BUCKET, not a slot inside `treat/`. Two reasons, and they agree:
    #
    # 1. INDUSTRY STANDARD. Google Product Taxonomy 5976 and Shopify's standard taxonomy
    #    hb-3-2-9-17 both put `Toners & Astringents` as a DIRECT CHILD of Skin Care — a sibling of
    #    Facial Cleansers, Lotion & Moisturizer, Sunscreen, Skin Care Masks & Peels and Acne
    #    Treatments & Kits. Neither nests it under a treatments node; in both, "treatments",
    #    "masks" and "toners" are three peers.
    # 2. RECALL. PIVOTA-Agent measured it (src/services/beautyTaxonomy.js): folding toner into
    #    `treat/` puts it in one bucket with serum(520) + mask(421) + exfoliant(123), which is the
    #    broad-bucket shape behind the 2026-07-31 junk recall.
    #
    # ⚠️ THIS MATCHES THE GATEWAY ON PURPOSE. PIVOTA-Agent has declared `tone/toner` canonical
    # since 2026-08-04 and its browse leg queries `category_path LIKE 'beauty/skincare/tone/%'`,
    # while this file named `treat/toner` and nothing here could reach the 315 prod rows sitting on
    # the gateway's path. Two taxonomies over one column, each calling the other's rows corrupt.
    # Do not retarget this leaf without changing beautyTaxonomy.js in the same breath.
    # An ACID or PEELING pad is an exfoliant, not a toner. The Toner entry below claims a bare
    # "pad", which is right for the hydrating/soothing pads that dominate the shelf (Anua Heartleaf
    # 77% Toner Pad, Torriden Multi Pad, NEOGEN Real Cica Pad) but wrong for an exfoliating one --
    # measured on sokoglam's "Physical" and "Chemical" shelves, where SOME BY MI "AHA-BHA-PHA 30
    # Days Miracle Truecica Clear Pad" and IOPE "Skin Booster Ampoule Peel Pad" both read as toners.
    # This arm sits ABOVE Toner so first-match-wins reaches it, and it NARROWS nothing: a pad with
    # no acid and no peel noun still falls through to Toner exactly as before. The acid list is the
    # exfoliating acids only -- hyaluronic, azelaic and amino acids are NOT exfoliants, and a title
    # naming an acid without a pad noun is left to the entries below; `lactic` declines "lactic acid
    # BACTERIA ferment", which is a soothing ingredient. "Gauze" is deliberately NOT a noun here:
    # prod holds a "Calming Gauze Pad" (a soothing pad), and NEOGEN's exfoliating gauzes all say
    # "Bio-Peel ... Peeling" anyway. This DOES add a second path to the distinct-path count that
    # services/curated_brand_feed._pattern_matches takes over a merchant product_type: a shelf
    # literally named "BHA Pad" would read as ambiguous there. No host in the measured census files
    # products under such a type.
    ("Exfoliant", "beauty/skincare/treat/exfoliant", re.compile(
        # \A + lookahead: a title that CALLS ITSELF a toner pad keeps the toner leaf, however many
        # acids it lists ("Ji Woo Gae Cica BHA Blemish Toner Pad" is a BHA toner pad, measured in
        # prod). The lookahead is DOTALL and takes one-or-more separators, so a line break or a
        # double space inside the phrase cannot slip past it. "Toning pad" is NOT in the guard:
        # unlike "toner pad" it names no product class on its own (the corpus holds no "toning pad"
        # either way -- its one "toning" product is a lotion), and a toning pad with no acid never
        # reaches this arm. A BUNDLE naming both ("AHA Peeling Pad + Toner Pad Set") stays a toner:
        # the lookahead reads the whole title, which is the conservative answer and what main did.
        # The rest scans the title for an exfoliating acid BEFORE a pad noun (an acid named after
        # the noun -- "Clear Pad with AHA BHA" -- is left to Toner), or an explicit peel pad.
        r"\A(?!(?s:.)*\btoner[-\s]+pads?\b)(?s:.)*?"
        r"(?:\b(?:aha|bha|pha|glycolic|salicylic|mandelic|lactic(?![\s-]+acid[\s-]+bacteria))\b"
        r"[^\n]{0,60}?\bpads?\b"
        r"|\b(?:peel(?:ing)?|exfoliating|exfoliant)(?:-|[^\S\n\r])+pads?\b)",
        re.IGNORECASE)),
    ("Toner", "beauty/skincare/tone/toner", re.compile(
        r"\b(toner|tonic|(?<!setting\s)(?<!fix\s)(?<!fixer\s)(?<!fixing\s)mist(?!\s*(?:&|and|\+)\s*fix)|"
        r"pad|skin booster)\b", re.IGNORECASE)),
    # An acne / blemish patch is a TREATMENT, not a mask. Google Product Taxonomy 5976 and Shopify
    # both file it under Acne Treatments, and the measured curated shelves (eyurs "Acne Pimple
    # Patch", sokoglam "Spot") map it to treat/treatment -- so while these phrases sat in the Mask
    # pattern one product class lived on two leaves, and a title saying "pimple patch" on one of
    # those shelves named a different leaf than its shelf and stayed unresolved.
    # It sits ABOVE Mask because Mask still claims the generic plural "patches": "Pimple Patches"
    # must reach this entry first. Only a patch noun NAMED by its acne qualifier moves; eye patches,
    # lip patches and a bare "patches" stay masks.
    ("Treatment", "beauty/skincare/treat/treatment", re.compile(
        r"\b(?:pimple|spot(?:\s+cover)?|acne|blemish)\s+patch(?:es)?\b",
        re.IGNORECASE)),
    # Mask is SPLIT in two. Everything here names an unambiguous mask FORM, so
    # it wins over "essence" below: "Real Rice Essence Sheet Mask" is a mask.
    # The bare "patches" arm declines the acne-qualified plural the entry above owns. First-match
    # order alone is not enough: services/curated_brand_feed.py counts EVERY pattern that matches,
    # and a merchant product_type "Pimple Patches" hitting both would read as ambiguous there.
    ("Mask", "beauty/skincare/treat/mask", re.compile(
        r"\b(face mask|clay mask|charcoal mask|sheet mask|mask sheet|gel mask|"
        r"sleeping mask|sleep mask|wash[-\s]?off mask|under eye patch|eye patch|"
        r"patchs|(?<!\bpimple\s)(?<!\bspot\s)(?<!\bspot\scover\s)(?<!\bacne\s)(?<!\bblemish\s)patches|lip\s?patch)\b",
        re.IGNORECASE)),
    ("Exfoliant", "beauty/skincare/treat/exfoliant", re.compile(
        # A NAIL or GEL polish is a nail product; a face/body/lip polish is still an exfoliant.
        r"\b(exfoliant|exfoliating|exfoliation|peel|peeling|peeling gel|peel pads?|"
        # ...and a COLOUR polish ("Gel Color Polish") or a polish REMOVER is not one either.
        # ...and "Applies Like Polish" (KISS's brush-on nail glues) is a simile, not a polish.
        r"scrub|(?<!nail\s)(?<!gel\s)(?<!color\s)(?<!colour\s)(?<!like\s)polish(?!\s+removers?\b))\b",
        re.IGNORECASE)),
    ("Treatment", "beauty/skincare/treat/treatment", re.compile(
        r"\b(spot[-\s]?target(?:ing|ed)?|spot[-\s]?treatment|blemish|acne|"
        r"clarifying treatment|targeting gel|treatment gel|spot stickers?|"
        r"pimple stickers?|acne stickers?|azelaic acid|vitamin c duo|"
        r"retinol youth renewal|retinol treatment)\b",
        re.IGNORECASE)),
    ("Face Oil", "beauty/skincare/moisturize/oil", re.compile(
        r"\b(face oils?|facial oils?|body oil|essential oil|oil drops?)\b",
        re.IGNORECASE)),
    ("Serum", "beauty/skincare/treat/serum", re.compile(
        r"\b(serum|essence|ampoule|concentrate)\b", re.IGNORECASE)),
    # Bare `mask` LAST among the skincare-treat family, so a line-name "Mask"
    # ("Mask Fit Tone Up Essence") no longer beats the real form noun. A title
    # whose only signal is the word "mask" still lands here.
    ("Mask", "beauty/skincare/treat/mask", re.compile(
        r"\bmask\b", re.IGNORECASE)),
    ("Tanning", "beauty/body/tanning", re.compile(
        r"\b(self[-\s]?tan|self[-\s]?tanning|sunless tan|gradual tanning|gradualglow)\b",
        re.IGNORECASE)),
    # A nail "Base Coat & Primer" is nail polish (above); declining the nail cue here keeps it ONE match
    # as a merchant type in _pattern_matches (third review of #2364).
    ("Primer", "beauty/makeup/face/primer", re.compile(
        r"^(?!.*\b(?:nails?|gel|polish|lacquer)\b).*\b(primer|pore prep|pore[-\s]?filling)\b",
        re.IGNORECASE | re.DOTALL)),
    ("Concealer", "beauty/makeup/face/concealer", re.compile(
        r"\b(concealer|corrector|correcting skinstick|skinstick|skin stick|"
        r"eye brightener|bright fix)\b",
        re.IGNORECASE)),
    # Stays BELOW Primer and Cleanser: "foundation primer" is a primer and
    # "foundation cleansing balm" is a cleanser. The SPF ordering this pattern
    # used to be hoisted for is handled by _SUNSCREEN_RE's lookahead instead.
    ("Foundation", "beauty/makeup/face/foundation", re.compile(
        r"\b(foundation|bb\s+cream|cc\s+cream|skin\s+tint|tint\s+stick|"
        r"foundation\s+stick|cushion\s+foundation)\b",
        re.IGNORECASE)),
    ("Powder", "beauty/makeup/face/powder", re.compile(
        # A DIP powder is a nail colour system, not a face powder.
        # ...nor a NAIL / ACRYLIC powder, nor a "Powder Nail Color" (a nail store's dip shelf).
        r"\b((?<!dip\s)(?<!dipping\s)(?<!nail\s)(?<!acrylic\s)powder(?!\s+nail\b)|setting powder|pressed powder|loose powder|"
        r"blurring powder|finishing powder)\b",
        re.IGNORECASE)),
    # Lip Gloss before Highlighter: "Gloss Bomb Universal Lip Luminizer" contains
    # "luminizer" which the Highlighter pattern would catch — but it's a lip gloss.
    # Placing Lip Gloss here lets "gloss bomb" win before "luminizer" is seen.
    ("Lip Gloss", "beauty/makeup/lip/gloss", re.compile(
        r"\b(lip\s+gloss|gloss\s+bomb|gloss\s+luxe|gloss\s+drip|"
        r"gloss\s+stick|gloss\s+stix|lip\s+luminizer|clear\s+gloss)\b",
        re.IGNORECASE)),
    ("Highlighter", "beauty/makeup/face/highlighter", re.compile(
        r"\b(highlighter|illuminator|luminizer|luminiser|killawatt|diamond bomb|"
        r"glow drops)\b",
        re.IGNORECASE)),
    ("Blush", "beauty/makeup/face/blush", re.compile(
        r"\b(blush|cheeks out|cheek tint|flush)\b", re.IGNORECASE)),
    ("Bronzer", "beauty/makeup/face/bronzer", re.compile(
        r"\b(bronzer|contour)\b", re.IGNORECASE)),
    ("Eyeshadow", "beauty/makeup/eye/eyeshadow", re.compile(
        r"\b(eye\s?shadow|eyeshadow|eye color|eye colour)\b", re.IGNORECASE)),
    ("Eyeliner", "beauty/makeup/eye/eyeliner", re.compile(
        r"\b(eyeliner|eye liner|liquid liner|pencil liner|flypencil)\b",
        re.IGNORECASE)),
    ("Mascara", "beauty/makeup/eye/mascara", re.compile(
        r"\b(mascara)\b", re.IGNORECASE)),
    ("Brow Pencil", "beauty/makeup/eye/brow", re.compile(
        r"\b(brow pencil|eyebrow pencil|brow definer|brow sculptor|brow styler)\b",
        re.IGNORECASE)),
    ("Lip Balm", "beauty/makeup/lip/balm", re.compile(
        r"\b(lip balm|lip butter|lip treatment|lip care|lip serum|lipserum|"
        r"nightbalm|lip scrub|scrubstick)\b",
        re.IGNORECASE)),
    ("Lip Oil", "beauty/makeup/lip/oil", re.compile(
        r"\b(lip\s+oil)\b", re.IGNORECASE)),
    ("Lip Liner", "beauty/makeup/lip/liner", re.compile(
        r"\b(lip\s+liner|lip\s+pencil|pout\s+liner|precision\s+pout)\b",
        re.IGNORECASE)),
    ("Lip Tint", "beauty/makeup/lip/tint", re.compile(
        r"\b(lip\s+tint|lip\s+stain)\b", re.IGNORECASE)),
    ("Lipstick", "beauty/makeup/lip/lipstick", re.compile(
        # Narrowed: lip gloss/oil/liner/tint/stain each have their own patterns above.
        # `lip[\s-]*stick` catches "lipstick", "lip stick", "lip-stick".
        # See lipstick-recall regression 2026-05-09.
        r"\b(lip[\s-]*stick|lip\s+color|lip\s+colour|liquid\s+lip|lip\s+luxe|"
        r"lip\s+lacquer|pout\s+lip|lip\s+combo|lip\s+duo)\b",
        re.IGNORECASE)),
    ("Moisturizer", "beauty/skincare/moisturize/cream", re.compile(
        r"\b(moisturizer|moisturiser|cream|lotion|gel cream|gel-cream|"
        r"water gel|barrier cream)\b",
        re.IGNORECASE)),
    ("Body Care", "beauty/body/care", re.compile(
        r"\b(body milk|body relief|body essentials|body care|hand care|loofah)\b",
        re.IGNORECASE)),
    # ----- Phase O-5b: fashion / apparel patterns -----
    # Order: more specific (sweater, hoodie, dress) above the broad "apparel".
    # Pet apparel is included intentionally because some active fashion
    # merchants sell dog/cat clothing, and the LLM extractor's category
    # gate filters on category_path starting with 'fashion/' or 'apparel/'.
    ("Sweater", "fashion/apparel/tops/sweater", re.compile(
        r"\b(sweater|knit(?:ted)?\s+sweater|knit\s+top|cardigan|pullover|jumper)\b",
        re.IGNORECASE)),
    ("Hoodie", "fashion/apparel/tops/hoodie", re.compile(
        r"\b(hoodie|sweatshirt|zip[-\s]?up|pullover\s+hoodie)\b",
        re.IGNORECASE)),
    ("T-Shirt", "fashion/apparel/tops/tshirt", re.compile(
        r"\b(t[-\s]?shirt|tee\b|tank\s+top|long[-\s]?sleeve\s+tee|graphic\s+tee)\b",
        re.IGNORECASE)),
    ("Shirt", "fashion/apparel/tops/shirt", re.compile(
        r"\b(button[-\s]?up|button[-\s]?down|dress\s+shirt|blouse|polo\s+shirt)\b",
        re.IGNORECASE)),
    ("Dress", "fashion/apparel/dresses", re.compile(
        r"\b(dress|gown|sundress|maxi\s+dress|midi\s+dress|cocktail\s+dress)\b",
        re.IGNORECASE)),
    ("Skirt", "fashion/apparel/bottoms/skirt", re.compile(
        r"\b(skirt|mini\s+skirt|midi\s+skirt|maxi\s+skirt|pencil\s+skirt)\b",
        re.IGNORECASE)),
    ("Pants", "fashion/apparel/bottoms/pants", re.compile(
        r"\b(pants|trousers|chinos|slacks|joggers?\s+pants|joggers\b|cargo\s+pants|leggings)\b",
        re.IGNORECASE)),
    ("Jeans", "fashion/apparel/bottoms/jeans", re.compile(
        r"\b(jeans|denim|skinny\s+jeans|straight\s+leg|boot[-\s]?cut)\b",
        re.IGNORECASE)),
    ("Shorts", "fashion/apparel/bottoms/shorts", re.compile(
        r"\b(shorts|bermuda\s+shorts|denim\s+shorts|athletic\s+shorts)\b",
        re.IGNORECASE)),
    ("Coat", "fashion/apparel/outerwear/coat", re.compile(
        # A nail TOP/BASE coat is a nail product, not outerwear -- unless the title says outerwear
        # ("Men's Wool Top Coat"), which the Nail Polish pattern declines and this one keeps.
        r"\b((?<!top\s)(?<!base\s)coat|overcoat|trench\s+coat|peacoat|parka)\b"
        r"|^(?!.*\b(?:" + _NAIL_CUE + r")\b)(?=.*\b(?:" + _OUTERWEAR + r")\b).*\b(?:top|base)\s+coats?\b",
        re.IGNORECASE | re.DOTALL)),
    ("Jacket", "fashion/apparel/outerwear/jacket", re.compile(
        r"\b(jacket|blazer|bomber|denim\s+jacket|windbreaker|puffer\s+jacket)\b",
        re.IGNORECASE)),
    ("Vest", "fashion/apparel/outerwear/vest", re.compile(
        r"\b(vest|gilet|puffer\s+vest|padded\s+vest|down\s+vest)\b",
        re.IGNORECASE)),
    ("Base Layer", "fashion/apparel/base-layer", re.compile(
        r"\b(base\s+layer|baselayer|thermal\s+(?:top|bottom|underwear|set))\b",
        re.IGNORECASE)),
    ("Lingerie", "fashion/apparel/intimates/lingerie", re.compile(
        r"\b(lingerie|bra\b|panty|panties|underwear|brief|boy[-\s]?short|push[-\s]?up)\b",
        re.IGNORECASE)),
    ("Swimwear", "fashion/apparel/swimwear", re.compile(
        r"\b(swimwear|swimsuit|bikini|one[-\s]?piece\s+swim|board\s+shorts|trunks)\b",
        re.IGNORECASE)),
    ("Activewear", "fashion/apparel/activewear", re.compile(
        r"\b(activewear|sportswear|yoga\s+pants|workout\s+(?:top|tee|set))\b",
        re.IGNORECASE)),
    ("Sleepwear", "fashion/apparel/sleepwear", re.compile(
        r"\b(sleepwear|pajamas|pyjamas|nightgown|robe\b|loungewear)\b",
        re.IGNORECASE)),
    ("Shoes", "fashion/shoes", re.compile(
        r"\b(shoes\b|sneakers|loafers|heels|boots|sandals|flats\b|oxfords|mules)\b",
        re.IGNORECASE)),
    ("Bag", "fashion/accessories/bag", re.compile(
        r"\b(handbag|tote\b|backpack|crossbody|clutch|satchel|messenger\s+bag)\b",
        re.IGNORECASE)),
    ("Jewelry", "fashion/accessories/jewelry", re.compile(
        r"\b(jewelry|necklace|earring|bracelet|ring\b|pendant|brooch)\b",
        re.IGNORECASE)),
    ("Hat", "fashion/accessories/hat", re.compile(
        r"\b(hat|cap\b|beanie|fedora|baseball\s+cap|bucket\s+hat)\b",
        re.IGNORECASE)),
    ("Scarf", "fashion/accessories/scarf", re.compile(
        r"\b(scarf|shawl|wrap\b|stole\b)\b",
        re.IGNORECASE)),
    # Pet apparel — narrower-but-still-clothing for products explicitly
    # framed as pet wear (PawStyle catalog). Sits AFTER human apparel
    # patterns so a "dog sweater" matches Sweater first (the path tree
    # uses fashion/apparel/* either way, so category_kind=fashion).
    ("Pet Apparel", "fashion/apparel/pet", re.compile(
        r"\b(pet\s+(?:apparel|wear|clothing|sweater|coat|jacket|outfit|overalls?|onesies?)|"
        r"dog\s+(?:sweater|jacket|coat|hoodie|outfit|overalls?|onesies?)|"
        r"cat\s+(?:sweater|outfit|onesies?)|"
        r"\d-leg\s+(?:onesies?|base\s+layer))\b",
        re.IGNORECASE)),
    # Pet accessories — non-clothing pet gear (harness/leash/collar/etc.).
    # Distinct fashion/accessories/pet path so the catalog can tell
    # "pet apparel" from "pet accessory" without one being a parent of
    # the other.
    ("Pet Accessory", "fashion/accessories/pet", re.compile(
        r"\b((?:dog|cat|pet)\s+(?:harness|leash|collar|bandana|bow\s*tie|tag|carrier)|"
        r"tactical\s+(?:dog|cat|pet)\s+harness|"
        r"retractable\s+(?:dog|cat|pet)\s+leash)\b",
        re.IGNORECASE)),
    # Generic apparel/clothing fallback — last so specific patterns win.
    ("Apparel", "fashion/apparel/general", re.compile(
        r"\b(apparel|clothing|garment|womenswear|menswear|kidswear)\b",
        re.IGNORECASE)),
    # ----- Electronics patterns -----
    # Keyword-matchable subset only. Model-number-only products (WH-1000XM5,
    # AirPods, etc.) have no keyword signal and go to the LLM backfill path.
    ("Headphones", "electronics/audio/headphones", re.compile(
        r"\b(headphones|over[-\s]?ear\s+headphones|on[-\s]?ear\s+headphones|"
        r"wireless\s+headphones|noise[-\s]?cancell?ing\s+headphones)\b",
        re.IGNORECASE)),
    ("Earbuds", "electronics/audio/earbuds", re.compile(
        r"\b(earbuds|ear\s+buds|true\s+wireless\s+earbuds|wireless\s+earbuds|"
        r"in[-\s]?ear\s+(?:headphones|earphones))\b",
        re.IGNORECASE)),
    ("E-Reader", "electronics/ereader", re.compile(
        r"\b(e[-\s]?reader|ebook\s+reader|e[-\s]?book\s+reader)\b",
        re.IGNORECASE)),
    ("Bluetooth Speaker", "electronics/audio/speaker", re.compile(
        r"\b(bluetooth\s+speaker|wireless\s+speaker|portable\s+speaker|smart\s+speaker)\b",
        re.IGNORECASE)),
    # A "Duo" that HEADS a title and names more is the DUO lash-adhesive brand ("Duo: Brush-On Dark
    # Adhesive", "DUO Striplash Adhesive"), not a two-piece set; a bare "Duo" type is still a set.
    ("Gift Set", "beauty/sets/gift-set", re.compile(
        r"\b(skincare set|skin care set|gift set|holiday edition|routine|bundle|"
        r"essentials set|essentials|care set|(?<!^)duo|^duo$|kit|collection|set)\b",
        re.IGNORECASE)),
]


# THE ONE DEFINITION OF "THIS ROW HAS BEEN CATEGORISED".
#
# A path names a category only once it says something past the top-level domain. `beauty` is a
# NAMESPACE, not an answer to "what is this" -- and treating it as an answer is what stranded an
# entire cohort. Measured on the live index: of 50 rows returned for "eau de parfum", 16 sit on bare
# `beauty`, including the whole Ariana Grande fragrance line, Cosmic Kylie Jenner and every
# PixiPerfume. Serving drops them as `category_mismatch`, and the backfill below never revisits them
# because `category_path IS NULL` reads them as already done. A useless answer counted as an answer
# in both directions at once.
#
# It also explains why the cohort survived a taxonomy standardisation pass: `beauty` IS on the
# taxonomy, so an off-taxonomy health check counts it healthy. Nothing was watching non-leaf paths.
#
# Exported so the backfill, the serving gate and any future writer share one rule rather than each
# re-deciding what "categorised" means. PIVOTA-Agent's serving-side twin is
# `categoryPathIsCategorised` in src/server.js; the drift test below pins them to the same rule.
MIN_CATEGORISED_PATH_SEGMENTS = 2


def is_categorised_path(category_path: Optional[str]) -> bool:
    """True when the path names a category, not merely a top-level domain."""
    if not category_path:
        return False
    segments = [seg for seg in str(category_path).strip().strip("/").split("/") if seg]
    return len(segments) >= MIN_CATEGORISED_PATH_SEGMENTS


def classify(text: Optional[str]) -> Optional[Tuple[str, str]]:
    """Return (category_label, category_path) on first matching pattern, else None."""
    if not text:
        return None
    for label, path, pattern in CATEGORY_PATTERNS:
        if pattern.search(text):
            return (label, path)
    return None


# FACE skincare leaves: the four shelves a face routine is built from. Sunscreen is deliberately
# absent -- a body sunscreen IS a sunscreen, and sun/sunscreen names no body area.
FACE_SKINCARE_LEAF = re.compile(r"^beauty/skincare/(?:cleanse|tone|treat|moisturize)/")

# The body area a product names, and the leaf that area belongs to -- None where the taxonomy has
# NO honest leaf (no lash/brow-care, nail-care or men's-grooming leaf; see TAXONOMY_GAPS), so the
# row is left unresolved rather than filed under a face shelf it does not belong to.
#
# Why this exists: a pattern keeps only the noun it recognises and drops its qualifier. Measured
# on ohlolly.com / eyurs.com 2026-09-22: type "Hand Cream" -> moisturize/cream, "Eye Lash Serum"
# -> treat/serum, a "Body Wash" filed under "Cleanser" -> cleanse/cleanser; and in prod, 60 live
# seed-mirror rows such as "Peel Off Nail Polish" -> treat/exfoliant (via "peel" / "polish").
#
# Whole words only: "Handmade", "Behind", "Splash", "Brown", "Bodied", "Hairline" are not areas,
# nor is a craft word ("Hand-Picked", "Hand Poured", "Second hand"). "The Body Shop" is a brand.
_NON_FACE_AREAS = (
    ("hand", re.compile(r"(?<!second[- ])\bhand(?:s|cream|wash)?\b"
                        r"(?![- ](?:picked|made|crafted|poured|selected|blended|harvested|tied))", re.I),
     "beauty/body/care"),
    ("body", re.compile(r"\bbody\b(?!\s+shop\b)", re.I), "beauty/body/care"),
    ("foot", re.compile(r"\b(?:foot|feet)\b", re.I), "beauty/body/care"),
    ("hair", re.compile(r"\b(?:hair|scalp)\b", re.I), "beauty/haircare/general"),
    # A curl is hair. Measured in prod 2026-09-26: 9 curl creams ("Curl Cream 100ml", "The Homecurl
    # Curl-Defining Cream", "Wave Boost Curl Cream") sat on moisturize/cream via the bare "cream"
    # arm, while the Moroccanoil curl creams whose merchant said "hair" sat on haircare/general.
    # "curling mascara" is untouched: makeup is not a face SKINCARE leaf, so this rule never runs.
    ("curl", re.compile(r"\bcurl(?:s|y|ing)?\b", re.I), "beauty/haircare/general"),
    ("lash", re.compile(r"\b(?:eye\s?)?lash(?:es)?\b", re.I), None),
    ("brow", re.compile(r"\b(?:eye)?brows?\b", re.I), None),
    ("nail", re.compile(r"\b(?:nails?|cuticles?)\b", re.I), None),
    ("beard", re.compile(r"\bbeards?\b", re.I), None),
)
# A title that ALSO names the face ("Face & Body Lotion") is face care too; the old answer stands.
_FACE_WORD = re.compile(r"\b(?:face|facial)\b", re.I)
# Phrases that contain an area word but name no product area: "crow's feet" are eye wrinkles
# (measured in prod on an eye serum; Shopify titles often use a curly apostrophe), and a cleanser
# "safe for lash extensions" / "gentle on lashes" is a face cleanser.
_NOT_AN_AREA = re.compile(r"\bcrow[\u2019']?s?[\s-]+feet\b|\b(?:eye\s?)?lash[\s-]+extensions?\b"
                          r"|\b(?:safe|gentle)\s+(?:for|on)\s+(?:the\s+)?(?:eye\s?)?lash(?:es)?\b", re.I)
# Hair REMOVAL is body care, not hair care: "Hair Removal Aftercare Serum", "Ingrown Hair Serum".
_HAIR_REMOVAL = re.compile(r"\b(?:hair[\s-]+removal|ingrown[\s-]+hairs?)\b", re.I)


_AREA_LABELS = {"beauty/body/care": "Body Care", "beauty/haircare/general": "Hair Care"}


def non_face_leaf(path: Optional[str], *texts: Optional[str]) -> Optional[str]:
    """The leaf a FACE-leaf answer must become when the product names another body area.

    `texts` are the row's own words (title, merchant product_type, category). Returns None when the
    rule does not apply -- `path` is not a face skincare leaf, or no non-face area is named, or the
    face is named too -- so the caller keeps its answer unchanged. Otherwise returns that area's
    leaf, or "" (no honest leaf: the areas disagree, or the taxonomy has none for them).
    """
    if not FACE_SKINCARE_LEAF.match(path or ""):
        return None
    text = " ".join(str(v or "") for v in texts)
    if _FACE_WORD.search(text):
        return None
    text = _HAIR_REMOVAL.sub(" body ", _NOT_AN_AREA.sub(" ", text))
    areas = {name: leaf for name, pattern, leaf in _NON_FACE_AREAS if pattern.search(text)}
    if not areas:
        return None
    # A "Hand & Nail Cream" is a hand cream: nail care is part of the hand shelf.
    if "hand" in areas:
        areas.pop("nail", None)
    leaves = set(areas.values())
    if len(leaves) == 1 and None not in leaves:
        return leaves.pop()
    return ""


def resolve_path_from_row(
    *,
    category: Optional[str],
    product_type: Optional[str],
    title: Optional[str],
) -> Optional[Tuple[str, str]]:
    """Try category, product_type, title in priority order. Used by the backfill.

    A FACE skincare answer for a row that names another body area is refused, judged on ALL of the
    row's words: it becomes that area's leaf, or no answer at all where the taxonomy has no honest
    leaf (nails, lashes, brows, beards). No answer is what keeps the row out of the regex backfill:
    it is counted as unmatched and left alone, rather than re-filed under a face shelf every run.
    """
    return _guard_face_leaf(_first_hit(category, product_type, title), title, product_type, category)


def _first_hit(*candidates: Optional[str]) -> Optional[Tuple[str, str]]:
    for candidate in candidates:
        hit = classify(candidate)
        if hit is not None:
            return hit
    return None


def _guard_face_leaf(hit: Optional[Tuple[str, str]], *texts: Optional[str]) -> Optional[Tuple[str, str]]:
    """`hit` unless it is a face skincare leaf for a product whose `texts` name another body area:
    then that area's leaf, or None where the taxonomy has no honest leaf."""
    if hit is None:
        return None
    area_leaf = non_face_leaf(hit[1], *texts)
    if area_leaf is None:
        return hit
    if not area_leaf:
        return None
    return (_AREA_LABELS.get(area_leaf, hit[0]), area_leaf)


# Provenance enum values written to catalog_products.category_label_source.
# Confidence defaults are documented in
# ~/.claude/plans/let-s-build-a-full-breezy-taco.md.
CATEGORY_SOURCE_MERCHANT = "merchant_payload"
CATEGORY_SOURCE_VARIANT = "variant_aggregate"

CATEGORY_CONFIDENCE_MERCHANT = 1.0
CATEGORY_CONFIDENCE_VARIANT = 0.85


def fold_category_from_variants(
    *,
    category: Optional[str],
    product_type: Optional[str],
    title: Optional[str],
    variants: Optional[list] = None,
) -> Optional[Tuple[Tuple[str, str], str, float]]:
    """Resolve (label, path) plus provenance from product-level fields,
    falling back to variant-level fields when product-level misses.

    Variants can be StandardProductVariant objects OR plain dicts (raw
    Shopify payload). For dicts, we look at:
      - top-level keys: category / product_type / title
      - platform_metadata: category / product_type
    For StandardProductVariant objects, we look at title and
    platform_metadata.get("category") / platform_metadata.get("product_type").

    Returns ((label, path), source, confidence) or None.

    A product-level answer REFUSED by the non-face rule ends the fold: the product's own words said
    "nail polish" / "lash serum", and a variant titled "Serum 8ml" or "Top Coat" must not bring the
    face leaf (or a fashion coat) back. A variant hit is judged on the product's words as well.
    """
    raw = _first_hit(category, product_type, title)
    if raw is not None:
        hit = _guard_face_leaf(raw, title, product_type, category)
        return (hit, CATEGORY_SOURCE_MERCHANT, CATEGORY_CONFIDENCE_MERCHANT) if hit else None
    for variant in variants or []:
        v_category = _variant_field(variant, "category")
        v_product_type = _variant_field(variant, "product_type")
        v_title = _variant_field(variant, "title")
        v_raw = _first_hit(v_category, v_product_type, v_title)
        if v_raw is not None:
            v_hit = _guard_face_leaf(v_raw, title, product_type, category, v_title, v_product_type, v_category)
            return (v_hit, CATEGORY_SOURCE_VARIANT, CATEGORY_CONFIDENCE_VARIANT) if v_hit else None
    return None


async def fold_category_with_llm_fallback(
    *,
    merchant_id: Optional[str] = None,
    category: Optional[str] = None,
    product_type: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    variants: Optional[list] = None,
) -> Optional[Tuple[Tuple[str, str], str, float]]:
    """Same return shape as fold_category_from_variants, but when the
    regex+variant fallback returns None AND the LLM_CATEGORY_CLASSIFIER_ENABLED
    flag is on, fires services.category_classifier_llm.classify_via_llm
    as a final fallback. Source on hit: 'llm_category_v1'.

    Stays async so caller can `await`. Regex hits still complete
    synchronously (no LLM call) — the LLM only runs on the long tail.
    """
    regex_hit = fold_category_from_variants(
        category=category, product_type=product_type, title=title, variants=variants,
    )
    if regex_hit is not None:
        return regex_hit
    # Local import — keeps the heavy httpx+settings deps out of the
    # sync regex path. Anyone importing fold_category_from_variants
    # gets no LLM dependency.
    from services.category_classifier_llm import classify_via_llm, CATEGORY_SOURCE_LLM
    llm = await classify_via_llm(
        merchant_id=merchant_id, category=category,
        product_type=product_type, title=title, description=description,
    )
    if llm is None:
        return None
    label, path, confidence = llm
    # The LLM backfill selects `category_path IS NULL`, which is exactly where a row the regex
    # refused lands; an unguarded LLM answer would re-file a cleared lash serum as a face serum.
    # Judged on the same words as the regex (not the description, which says "face and hands").
    guarded = _guard_face_leaf((label, path), title, product_type, category)
    if guarded is None:
        return None
    return (guarded, CATEGORY_SOURCE_LLM, confidence)


def _variant_field(variant, key: str) -> Optional[str]:
    """Read a field from a variant that might be a dict OR a pydantic model."""
    if variant is None:
        return None
    # dict path (raw Shopify payload before model parsing)
    if isinstance(variant, dict):
        direct = variant.get(key)
        if direct:
            return str(direct)
        meta = variant.get("platform_metadata") or {}
        if isinstance(meta, dict):
            nested = meta.get(key)
            if nested:
                return str(nested)
        return None
    # pydantic model path (StandardProductVariant). title is always present;
    # category/product_type live in platform_metadata when Shopify carries them.
    if key == "title":
        title = getattr(variant, "title", None)
        return str(title) if title else None
    meta = getattr(variant, "platform_metadata", None) or {}
    if isinstance(meta, dict):
        nested = meta.get(key)
        if nested:
            return str(nested)
    return None


def category_path_prefix_for_query(query: Optional[str]) -> Optional[str]:
    """Used by the recall path: when the user query matches a known category,
    return a 3-segment prefix like 'beauty/makeup/lip/' so the SQL can do
    `WHERE category_path LIKE :prefix || '%'`. Returning None means the
    query does NOT match a known category and recall should fall back to
    the existing trigram text scan.

    Example: 'lipstick' → 'beauty/makeup/lip/' (matches 'beauty/makeup/lip/lipstick'
    AND 'beauty/makeup/lip/balm' so users see both lipstick and lip balm
    rows on a generic 'lipstick' search). Adjust the slice depth if a more
    precise match is desired.
    """
    hit = classify(query)
    if hit is None:
        return None
    _, path = hit
    # Slice to category-parent level (drop the final segment).
    parts = path.rsplit("/", 1)
    if len(parts) <= 1:
        return path + "/"
    return parts[0] + "/"
