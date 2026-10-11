"""Setting spray is a leaf: beauty/makeup/face/setting-spray (2026-10-10).

It was a declared gap. Measured that day: ~72 drain products on 28 domains left out as
category_unresolved (types "Setting Spray", "Setting Sprays", "Makeup Fixer", "Makeup Setting Fixer"),
and 6 live setting mists filed on the toner leaf through its bare "mist". Titles are the stores' own
unless marked.
"""
import pytest

from services import curated_brand_feed as feed
from services.category_path_aliases import ALIASES, TAXONOMY_GAPS, TAXONOMY_LEAVES, resolve as leaf
from services.pdp_category_classifier import category_path_prefix_for_query, classify

LEAF = "beauty/makeup/face/setting-spray"
TONER = "beauty/skincare/tone/toner"
POWDER = "beauty/makeup/face/powder"


def resolve(product_type, title, domain=None):
    return feed._resolve_category(product_type=product_type, title=title, flag_path="beauty", domain=domain)


def test_it_is_a_leaf_and_the_old_gap_spelling_aliases_to_it():
    assert LEAF in TAXONOMY_LEAVES
    assert "beauty/makeup/setting-spray" not in TAXONOMY_GAPS
    assert ALIASES["beauty/makeup/setting-spray"] == LEAF
    assert leaf("beauty/makeup/setting-spray") == LEAF


@pytest.mark.parametrize("title", [
    "Makeup Fixing Mist",                                  # pixibeauty, was on tone/toner
    "You Mist Makeup-Extending Setting Spray",             # fentybeauty, was on tone/toner
    "RMS Beauty Radiance Lock Setting Mist 100ml",
    "Jill Stuart Crystal Glow & Fix Mist",
    "Mist & Fix Matte Spray",
    "Fix+ Stay Over 24HR Setting Spray",
    "Mask Fit Waterproof Setting Spray",                   # was on treat/mask via "Mask"
    "3CE Makeup Fixer Mist",
    "Hera Make Up Fixer 80ml",
    "espoir Fresh Setting Fixer 100ml",
])
def test_a_setting_spray_title_names_the_leaf_not_toner(title):
    assert classify(title)[1] == LEAF


@pytest.mark.parametrize("title,want", [
    ("Hydrating Face Mist", TONER),
    ("Rose Water Mist", TONER),
    ("Setting Powder", POWDER),
    ("Makeup Setting Puffs", None),                        # constructed: puffs, not a spray
    ("Volumizing Finishing Spray", None),                  # a hair spray
    ("Finishing Hairspray", None),
    ("Hair Fixing Spray", "beauty/haircare/general"),
])
def test_the_leaf_does_not_take_mists_powders_puffs_or_hair_sprays(title, want):
    hit = classify(title)
    assert (hit[1] if hit else None) == want


@pytest.mark.parametrize("ptype,title", [
    ("Setting Spray", "Life Lock™ hydrating setting spray"),             # tartecosmetics.com
    ("Setting Sprays", "Airbrush Flawless Setting Spray"),               # bluemercury.com
    ("SETTING SPRAY", "NYX Makeup Setting Spray 2.03oz"),                # ikatehouse.com
    ("Makeup Fixer", "Espoir Fresh Setting Fixer 100ml"),                # koolseoul.com
    ("Makeup Setting Fixer", "3CE MAKEUP FIXER MIST 100ml"),             # nanasbeautyholic.com
    ("Setting Mist", "Continuous Setting Mist"),                         # constructed type
    ("Mist & Fix", "Mist & Fix Spray"),                                  # constructed type
])
def test_a_setting_spray_shelf_resolves(ptype, title):
    assert resolve(ptype, title)[0] == LEAF


@pytest.mark.parametrize("title", [
    "[ETUDE HOUSE] Dr. Mascara Fixer 6ml",
    "[ETUDE HOUSE] Bare Edge Brow Fixer",
    "[ETUDE HOUSE] Pang Pang Haircara Fixer 10ml",
    "[A’PIEU] Pro Curling The Black Fixer Mascara",
])
def test_a_lash_brow_or_hair_fixer_on_a_setting_spray_shelf_is_refused(title):
    # holiholic.com shelves these under "Setting Spray"
    assert not leaf(resolve("Setting Spray", title)[0])


@pytest.mark.parametrize("ptype,title,want", [
    ("makeup, face, setting spray & powder", "Slip Tint Undetectable Baked Setting Powder", POWDER),
    ("makeup, face, setting spray & powder", "Vital Pressed Skincare Powder", POWDER),
    ("Setting Spray and Powder", "Ruby Kisses RFS Never Touch Up Setting Spray 1.69 oz", LEAF),
    # names two leaves (gift-set reads "Set N"): the shelf cannot be split, so it stays unresolved
    ("Setting Spray and Powder", "Ruby Kisses HD Set N Forget Setting Powder -RRSP .4 oz", None),
    ("Setting Spray and Powder", "Setting Spray & Powder Duo", None),                # constructed
])
def test_a_combined_spray_and_powder_shelf_takes_the_one_its_title_names(ptype, title, want):
    got = resolve(ptype, title)[0]
    assert (leaf(got) or None) == want


def test_the_combined_shelf_rule_reads_only_that_pair():
    # any other two-product type stays the #2158 ambiguity it was
    assert not leaf(resolve("Primer & Foundation", "Blurring Primer")[0])


@pytest.mark.parametrize("query", ["setting spray", "setting mist", "makeup fixer"])
def test_a_setting_spray_query_browses_face_makeup(query):
    assert category_path_prefix_for_query(query) == "beauty/makeup/face/"
