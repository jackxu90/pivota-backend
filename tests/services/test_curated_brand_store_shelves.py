"""Brand-store categories for the Tarte / Stila / Tower 28 spelling re-runs (read 2026-09-27).

The held re-runs kept 290/435, 54/131 and 15/64 products: brow shelves ("Eyebrow", "Brows") name no
pattern, "Concealer Brush" names two, and Tower 28 leaves 45 of 64 types blank. Three narrow fixes,
each measured on every product of every host in the shelf table plus kissusa / impressbeauty /
falscara (11 hosts, lip and lash doors on and off): only these three stores change.
Titles are the stores' own unless marked.
"""
import pytest

from services import curated_brand_feed as feed

SHELF = feed.CATEGORY_CONFIDENCE_MEASURED_HOST_TYPE
HEAD = feed.CATEGORY_CONFIDENCE_MEASURED_HOST_TITLE
TITLE = feed.CATEGORY_CONFIDENCE_EXPLICIT_TITLE
BROW = "beauty/makeup/eye/brow"
BRUSH = "beauty/tools/brush"


def resolve(product_type, title, domain, flag_path="beauty"):
    return feed._resolve_category(product_type=product_type, title=title, flag_path=flag_path, domain=domain)


def unresolved(result):
    from services.category_path_aliases import resolve as leaf
    return not leaf(result[0])


# --- measured shelves -------------------------------------------------------------------------

@pytest.mark.parametrize("domain,ptype,title,want", [
    ("tartecosmetics.com", "Eyebrow", "busy gal BROWS tinted brow gel", BROW),
    ("tartecosmetics.com", "Eyebrow", "maneater™ emphasEYES™ high definition brow", BROW),
    ("tartecosmetics.com", "Eyebrow", "frameworker™ brow pomade", BROW),
    ("tartecosmetics.com", "Eyebrow", "maracuja juicy brow", BROW),
    ("tartecosmetics.com", "Lip Plump", "maracuja juicy lip plump", "beauty/makeup/lip/gloss"),
    ("tartecosmetics.com", "Lip Plump", "maracuja juicy shift", "beauty/makeup/lip/gloss"),
    ("tartecosmetics.com", "Face Tool", "quickie blending sponge", "beauty/tools/sponge"),
    ("tartecosmetics.com", "Face Tool", "set & go powder puff", "beauty/tools/sponge"),
    ("stilacosmetics.com", "Brows", "Stay All Day® Waterproof Brow Color", BROW),
    ("stilacosmetics.com", "Lip Care", "Heaven’s Dew™ Honey Glow Balm - Golden Sun", "beauty/makeup/lip/balm"),
])
def test_the_brand_store_shelves_resolve_their_products(domain, ptype, title, want):
    assert resolve(ptype, title, domain) == (want, SHELF)


def test_a_two_area_tint_on_a_lip_shelf_is_set_aside():
    """tarte files its "lip & cheek shift" (a lip AND cheek tint) on the Lip Plump shelf."""
    assert unresolved(resolve("Lip Plump", "lime-coded maracuja juicy lip & cheek shift", "tartecosmetics.com"))
    # the same area rule on the older lip shelf (constructed title; none on sokoglam today)
    assert unresolved(resolve("Lip Balms", "Lip & Cheek Balm", "sokoglam.com"))
    assert resolve("Lip Balms", "Phyto-Glow Lip Balm SPF 45", "sokoglam.com")[0] == "beauty/makeup/lip/balm"


def test_the_brow_word_is_exempt_only_on_a_brow_shelf():
    # On a face shelf a brow product is still another area (constructed).
    assert unresolved(resolve("Moisturizers", "Brow Growth Cream", "eyurs.com"))
    # ...and a brow shelf still refuses the other areas.
    assert unresolved(resolve("Eyebrow", "brow & body glow oil", "tartecosmetics.com"))


@pytest.mark.parametrize("ptype,title", [
    # review of #2403: a shelf's tools, and a second product, are not its formula (constructed)
    ("Eyebrow", "brow spoolie"),
    ("Eyebrow", "brow shaping razors"),
    ("Face Tool", "sculpting gua sha"),
    ("Face Tool", "Amazonian clay facial roller"),
    ("Face Tool", "eyelash curler"),
    ("Lip Plump", "maracuja juicy lip plump & liner"),
])
def test_a_shelf_does_not_take_its_tools_or_a_second_product(ptype, title):
    assert unresolved(resolve(ptype, title, "tartecosmetics.com"))


@pytest.mark.parametrize("domain", ["sephora.com", "", None, "eyurs.com"])
def test_the_brand_store_shelves_mean_nothing_elsewhere(domain):
    assert unresolved(resolve("Eyebrow", "busy gal BROWS tinted brow gel", domain))
    assert unresolved(resolve("Lip Plump", "maracuja juicy lip plump", domain))


# --- "<X> Brush" types ------------------------------------------------------------------------

@pytest.mark.parametrize("ptype,title", [
    ("Concealer Brush", "shape tape™ blur concealer brush"),
    ("Concealer Brush", "double-ended detail & buff concealer brush"),
    ("Eyeshadow Brush", "double-ended shadow & liner brush"),
    ("Eyeliner Brush", "eye definer line & smudge brush"),
    ("Foundation Brush", "the buffer™ brush"),
    ("Foundation Brush", "shape tape™ cream foundation brush"),    # a texture word is the brush's use
    ("Eyeshadow Brush, Cheek Brush", "glow pot eye & cheek brush"),
])
def test_a_brush_type_with_a_brush_title_is_a_brush(ptype, title):
    assert resolve(ptype, title, "tartecosmetics.com") == (BRUSH, TITLE)
    assert resolve(ptype, title, "unknown-store.com") == (BRUSH, TITLE)  # not host-scoped


@pytest.mark.parametrize("ptype,title", [
    ("Foundation Brush", "jumbo quickie blending sponge"),        # on tarte's brush shelf, not a brush
    ("Foundation Brush", "2-in-1 twist tool"),
    ("Concealer Brush", "face card never declines CC undereye & brush"),  # a formula sold WITH a brush
    ("Foundation Brush", "Serum Foundation with Brush"),                  # constructed
    ("Concealer Brush", "Brush-On Concealer"),                            # constructed
    ("Hair Brush", "Flex Gentle Brush"),                                  # sokoglam: a hairbrush
    # review of #2403 (constructed): a formula before any joiner, a skincare formula, a nail tool
    ("Concealer Brush", "face card never declines CC undereye & mini brush"),
    ("Eyeliner Brush", "Gel Eyeliner + Angled Brush"),
    ("Eyeshadow Brush", "Eyeshadow Palette & Blending Brush"),
    ("Concealer Brush", "Concealer, Brush"),
    ("Concealer Brush", "Concealer - Brush"),
    ("Concealer Brush", "Concealer & Brush 12"),
    ("Powder Brush", "Sunforgettable Mineral Sunscreen Brush"),
    ("Gel Polish Brush", "Gel Polish Brush"),
    ("Dip Powder Brush", "Dip Powder Brush"),
    ("Nail Polish Brush", "the buffer™ brush"),              # only the TYPE names nails
    # re-review of #2403: a formula and a bare brush
    ("Eyeliner Brush", "Gel Liner & Brush"),
    ("Eyeliner Brush", "Gel Liner + Angled Brush"),
    ("Eyeliner Brush", "Brow Pomade & Brush"),
    ("Eyeshadow Brush", "Loose Pigment & Brush"),
    ("Concealer Brush", "Concealer Plus Brush"),
    # third review: a kit whose brush names its use
    ("Eyeliner Brush", "Gel Liner & Liner Brush"),
    ("Eyebrow Brush, Eyeliner Brush", "Brow Pomade & Brow Brush"),
    ("Eyeliner Brush", "Kohl & Smudge Brush"),
    ("Eyeshadow Brush", "Loose Pigment & Shadow Brush"),
    ("Eyeshadow Brush", "Glitter Glue & Eye Brush"),
    ("Foundation Brush", "nail polish brush"),               # only the TITLE names nails
    # the type's head noun must be the tool: a two-formula type is not a brush type
    ("Concealer, Foundation", "the buffer™ brush"),
])
def test_a_brush_type_does_not_make_everything_on_it_a_brush(ptype, title):
    assert unresolved(resolve(ptype, title, "tartecosmetics.com"))


# --- head-noun title rule on measured hosts ---------------------------------------------------

@pytest.mark.parametrize("domain,ptype,title,want", [
    ("www.tower28beauty.com", "", "Swipe Serum Foundation", "beauty/makeup/face/foundation"),
    ("tower28beauty.com", "Foundations & Concealers", "Swipe Serum Concealer®", "beauty/makeup/face/concealer"),
    ("tower28beauty.com", "", "BeachPlease Cream Blush", "beauty/makeup/face/blush"),
    ("tower28beauty.com", "", "Sculptino® Cream Contour", "beauty/makeup/face/bronzer"),
    ("tower28beauty.com", "", "GetSet® Matte Powder Blush", "beauty/makeup/face/blush"),
    ("tower28beauty.com", "", "MakeWaves® Mascara", "beauty/makeup/eye/mascara"),
    ("tower28beauty.com", "", "SOS Recovery Cream", "beauty/skincare/moisturize/cream"),
    ("tower28beauty.com", "", "SOS Gel Cleanser", "beauty/skincare/cleanse/cleanser"),
    ("stilacosmetics.com", "Sale", "Stay All Day® Waterproof Liquid Eye Liner - Micro Tip", "beauty/makeup/eye/eyeliner"),
    ("stilacosmetics.com", "Eye Products", "HUGE™ Extreme Lash Mascara", "beauty/makeup/eye/mascara"),
    ("stilacosmetics.com", "new", "All About The Blur Blurring & Smoothing Primer", "beauty/makeup/face/primer"),
    ("tower28beauty.com", "", "Mini MakeWaves Mascara in Jet", "beauty/makeup/eye/mascara"),
    ("tower28beauty.com", "", "MakeWaves Mascara in Warm Blush", "beauty/makeup/eye/mascara"),  # shade cut
    ("tower28beauty.com", "", "SOS Eye Cream", "beauty/skincare/moisturize/cream"),            # constructed
])
def test_the_head_noun_names_the_product_on_a_measured_title_host(domain, ptype, title, want):
    assert resolve(ptype, title, domain) == (want, HEAD)


# Read 2026-10-10: the SOS Daily Rescue Facial Spray is tower28's only spray, and its body wash names
# the body. Both were unresolved, so the drain left them out (#2403 pinned them as refusals).
@pytest.mark.parametrize("title,want", [
    ("SOS Rescue Spray", "beauty/skincare/tone/toner"),
    ("SOS Rescue Spray - Standard Size", "beauty/skincare/tone/toner"),
    ("Travel SOS Rescue Spray", "beauty/skincare/tone/toner"),
    ("SOS Rescue + Relief Body Wash Treatment", "beauty/body/care"),
])
def test_tower28s_facial_spray_and_body_wash_resolve(title, want):
    assert resolve("", title, "tower28beauty.com") == (want, HEAD)


@pytest.mark.parametrize("domain,ptype,title,want", [
    # the host's "spray" head is never another product's: each is constructed
    ("tower28beauty.com", "", "Setting Spray", None),
    ("tower28beauty.com", "", "Makeup Setting Spray", None),
    ("tower28beauty.com", "", "Hair Spray", None),
    ("tower28beauty.com", "", "Body Spray", None),
    ("tower28beauty.com", "", "Room & Linen Spray", None),
    # ...a pattern that names something keeps its answer
    ("tower28beauty.com", "", "SunnyDays SPF 30 Sunscreen Spray", "beauty/skincare/sun/sunscreen"),
    # ...a typed shelf is not the blank type the spray head was read on
    ("tower28beauty.com", "Foundations & Concealers", "SOS Rescue Spray", None),
    # ...and the spray head is tower28's alone
    ("stilacosmetics.com", "", "Rescue Spray", None),
    # a makeup head that names the body stays refused (no body leaf for makeup)
    ("tower28beauty.com", "", "SuperDew Body Highlighter", None),
])
def test_the_spray_head_and_the_body_area_answer_stay_narrow(domain, ptype, title, want):
    got = resolve(ptype, title, domain)
    if want is None:
        assert unresolved(got)
    else:
        assert got[0] == want


@pytest.mark.parametrize("domain,ptype,title", [
    # review of #2403: the head of the whole title is not the product
    ("stilacosmetics.com", "Sale", "Convertible Color™ Dual Lip & Cheek Cream"),
    ("stilacosmetics.com", "Sale", "Stay All Day® Foundation & Concealer"),
    ("stilacosmetics.com", "Sale", "Calligraphy Lip Stain in Michelle (Warm Blush)"),
    ("stilacosmetics.com", "", "Free Mini HUGE Extreme Lash Mascara"),
    ("tower28beauty.com", "", "SOS Face & Body Wash"),        # constructed: the face is named too
    ("tower28beauty.com", "", "MakeWaves Lash Primer"),
    ("tower28beauty.com", "", "ShineOn Lip Primer"),
    ("tower28beauty.com", "", "GetSet Brow Powder"),
    ("tower28beauty.com", "", "SuperDew Body Highlighter"),
    ("tower28beauty.com", "", "ShineOn Lip Oil Case"),
    ("tower28beauty.com", "", "Eyeliner Pencil Sharpener"),
    ("tower28beauty.com", "", "Mascara Remover"),
    ("tower28beauty.com", "", "ShineOn Lip Gloss + OneLiner Lip Liner"),
    ("tower28beauty.com", "", "BeachPlease Blush Tee"),
    ("tower28beauty.com", "", "MakeWaves Mascara Tote Bag"),
    ("tower28beauty.com", "", "SOS Hair Claw Clip"),
    ("tower28beauty.com", "", "BeachPlease Blush Sweater"),   # a fashion head no accessory word names
    ("tower28beauty.com", "", "The Sunset Sweater"),          # ...and one naming no beauty leaf at all
    # re-review of #2403: another area in the suffix, a wand
    ("tower28beauty.com", "", "SuperDew Highlighter (Face & Body)"),
    ("tower28beauty.com", "", "SuperDew Highlighter - Body Edition"),
    ("tower28beauty.com", "", "BeachPlease Cream Blush (Lip + Cheek)"),
    ("tower28beauty.com", "", "MakeWaves Mascara Wand"),
    # a leaf with no area row is refused, never raised (nails, lashes)
    ("tower28beauty.com", "", "Gel Nail Polish"),
    ("stilacosmetics.com", "Sale", "Stay All Day Top Coat"),
    ("tower28beauty.com", "", "Press On Nails"),
    ("tower28beauty.com", "", "Cuticle Oil Pen"),
    ("stilacosmetics.com", "Eye Products", "Dip Powder"),
    ("tower28beauty.com", "", "SOS Serum Glow Foundation"),   # a texture word NOT right before the head
])
def test_the_head_noun_rule_refuses_another_area_accessories_merch_and_two_products(domain, ptype, title):
    assert feed._measured_host_title_leaf(domain=domain, product_type=ptype, title=title) is None
    assert unresolved(resolve(ptype, title, domain))


def test_the_head_noun_rule_leaves_lip_rows_to_the_lip_door_and_its_hold():
    title = "ShineOn Lip Gloss in Pistachio"
    assert feed._measured_host_title_leaf(domain="tower28beauty.com", product_type="", title=title) is None
    assert unresolved(resolve("", title, "tower28beauty.com"))                  # door off: as on main
    with feed.lip_title_evidence():
        assert resolve("", title, "tower28beauty.com") == ("beauty/makeup/lip/gloss",
                                                            feed.CATEGORY_CONFIDENCE_LIP_TITLE)  # held per row


@pytest.mark.parametrize("ptype,title", [
    ("", "SOS FaceGuard™ SPF 30 - Sample"),               # a sample
    ("", "MakeWaves® Mascara Set"),                       # a set
    ("", "JELL-O® X TOWER 28 Lip Jelly Lip Gloss Gift Set"),
    ("", "The Cheeky Duo"),
    ("", "Keep-It Together Keychain"),                    # merch
    ("", "$10 Gift Card"),
    ("", "SOS Rescue Spray Refill"),                      # a refill
    ("", "SOS Spray Duo"),                                # a duo
    ("", "MakeWaves Mascara Trio"),                       # constructed: a set word no gift-set pattern reads
    ("", "MakeWaves Mascara Holiday Edition"),            # constructed: the gift-set pattern, no set word
    ("Hidden", "Free Mini HUGE Extreme Lash Mascara"),    # a type nobody listed
])
def test_the_head_noun_rule_refuses_sets_samples_merch_and_unlisted_types(ptype, title):
    assert unresolved(resolve(ptype, title, "tower28beauty.com" if ptype != "Hidden" else "stilacosmetics.com"))


def test_two_leaves_ending_together_are_a_tie_not_a_head(monkeypatch):
    import re
    from services import pdp_category_classifier as clf
    patterns = [("A", "beauty/makeup/face/blush", re.compile(r"\b(glow)\b", re.I)),
                ("B", "beauty/makeup/face/highlighter", re.compile(r"\b(glow)\b", re.I))]
    monkeypatch.setattr(clf, "CATEGORY_PATTERNS", patterns)
    assert feed._measured_host_title_leaf(domain="tower28beauty.com", product_type="", title="Super Glow") is None
    monkeypatch.setattr(clf, "CATEGORY_PATTERNS", patterns[:1])
    assert feed._measured_host_title_leaf(domain="tower28beauty.com", product_type="",
                                          title="Super Glow") == "beauty/makeup/face/blush"


@pytest.mark.parametrize("domain", ["sephora.com", "tartecosmetics.com", "", None])
def test_the_head_noun_is_no_evidence_on_any_other_host(domain):
    assert unresolved(resolve("", "BeachPlease Cream Blush", domain))
    assert unresolved(resolve("", "Swipe Serum Foundation", domain))


def test_a_refusal_and_a_resolved_row_are_never_touched():
    # Stila's swapped type: a "Blush" typed eyeliner is refused, and stays refused.
    assert resolve("Blush", "Stay All Day® Chroma-Flash Liquid Eye Liner", "stilacosmetics.com")[0] == ""
    # A merchant type that names its class resolves at merchant confidence, not the head-noun rule's.
    assert resolve("Mascara", "MakeWaves® Mascara", "tower28beauty.com")[1] == feed.CATEGORY_CONFIDENCE_MERCHANT_TYPE


def test_every_head_the_rule_can_pick_has_an_area_row_or_is_refused():
    from services.pdp_category_classifier import CATEGORY_PATTERNS
    for _label, path, _pattern in CATEGORY_PATTERNS:
        if path.startswith(("beauty/makeup/", "beauty/skincare/")) and not path.startswith(feed._LIP_LEAF_PREFIX):
            rows = [areas for prefix, areas in feed._HEAD_AREAS if path.startswith(prefix)]
            assert rows or path.startswith(("beauty/makeup/nails/",)) or "lash" in path, path


def test_a_lip_shelf_keeps_a_balm_named_after_a_mirror_or_an_applicator():
    assert resolve("Lip Balms", "Mirror Glow Lip Balm", "sokoglam.com")[0] == "beauty/makeup/lip/balm"
    assert resolve("Lip Balms", "Lip Balm with Applicator", "sokoglam.com")[0] == "beauty/makeup/lip/balm"


def test_the_new_confidences_are_no_other_writers_value():
    from scripts.backfill_pdp_category_path import CONFIDENCE_REGEX_BACKFILL
    from services.pdp_category_classifier import CATEGORY_CONFIDENCE_VARIANT
    others = {CONFIDENCE_REGEX_BACKFILL, CATEGORY_CONFIDENCE_VARIANT, feed.CATEGORY_CONFIDENCE_MERCHANT_TYPE,
              feed.CATEGORY_CONFIDENCE_EXPLICIT_TITLE, feed.CATEGORY_CONFIDENCE_FEED_DEFAULT, SHELF,
              feed.CATEGORY_CONFIDENCE_LIP_TITLE, feed.CATEGORY_CONFIDENCE_LASH_NAIL_TITLE}
    assert HEAD not in others


@pytest.mark.parametrize("ptype,title,domain", [
    ("Concealer Brush", "Blush" + " " * 20000 + "Cream", "tartecosmetics.com"),
    ("", "Blush" + " " * 20000 + "Cream", "tower28beauty.com"),
    ("", " " * 20000, "stilacosmetics.com"),
])
def test_a_long_whitespace_run_resolves_in_linear_time(ptype, title, domain):
    import time
    started = time.perf_counter()
    resolve(ptype, title, domain)
    assert time.perf_counter() - started < 0.5  # quadratic took 4-36s (third review of #2403)
