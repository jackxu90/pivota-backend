"""The alias map, and the membership check it backs.

The defect being repaired is "somebody stored a category_path that is not in the taxonomy". Every
test here is written so that a map which could do the same thing while repairing it fails.
"""

from __future__ import annotations

import pytest

from services.category_path_aliases import (
    ALIASES,
    has_category_door,
    TAXONOMY_GAPS,
    TAXONOMY_LEAVES,
    resolve,
)


# --- the map cannot invent a category ----------------------------------------------------------


def test_every_alias_target_is_a_real_taxonomy_leaf():
    """THE LOAD-BEARING PROPERTY. A map that repairs `tone/toner` into another non-existent path
    has moved the defect, not fixed it. (Also asserted at import, so a bad edit fails collection —
    this pins that the assertion itself is real.)"""
    bad = sorted(set(ALIASES.values()) - TAXONOMY_LEAVES)
    assert not bad, bad


def test_no_alias_hides_a_path_that_is_already_correct():
    """Aliasing a real leaf away would silently re-file working products."""
    assert not sorted(p for p in ALIASES if p in TAXONOMY_LEAVES)


def test_gaps_and_aliases_are_disjoint():
    assert not (set(ALIASES) & set(TAXONOMY_GAPS))


# --- resolve ------------------------------------------------------------------------------------


def test_resolve_snaps_the_measured_near_misses():
    """The real prod cohorts, by row count."""
    assert resolve("beauty/skincare/treat/toner") == "beauty/skincare/tone/toner"
    assert resolve("beauty/makeup/lips/lip-balm") == "beauty/makeup/lip/balm"         # 12
    assert resolve("beauty/makeup/lips/lip-gloss") == "beauty/makeup/lip/gloss"       # 9
    assert resolve("beauty/body-care/deodorant") == "beauty/body/care"                # 11


def test_resolve_returns_a_real_leaf_unchanged():
    """The control for the test above: a map that returned a constant would pass it."""
    assert resolve("beauty/makeup/lip/lipstick") == "beauty/makeup/lip/lipstick"
    assert resolve("beauty/skincare/tone/toner") == "beauty/skincare/tone/toner"


def test_resolve_refuses_a_genuine_gap():
    """THE OTHER CONTROL, and the more important one. There is no honest leaf for a nail polish;
    filing it under a blush would make it findable by the WRONG query, which is louder than not
    being findable at all. These must stay off-taxonomy and stay counted."""
    for path in ("beauty/accessories/pouch", "wellness/supplements",
                 "beauty/oral-care/toothpaste", "home/fragrance/candle"):
        assert resolve(path) is None, path


def test_resolve_refuses_something_it_has_never_seen():
    assert resolve("beauty/quantum/entanglement") is None
    assert resolve("") is None
    assert resolve(None) is None


def test_resolve_normalises_the_INPUT_because_recall_cannot():
    """Recall's `LIKE` is case-sensitive and does not trim, which is exactly WHY a mis-cased path is
    unreachable — so the repair has to catch what recall cannot. What comes back is always the
    canonical leaf, never a normalised copy of the input."""
    assert resolve("Beauty/Skincare/Treat/Toner") == "beauty/skincare/tone/toner"
    assert resolve("/beauty/makeup/lips/lip-gloss/") == "beauty/makeup/lip/gloss"
    assert resolve("  beauty/skincare/treat/toner  ") == "beauty/skincare/tone/toner"


def test_a_repaired_path_is_actually_REACHABLE_by_recall():
    """The point of the whole exercise, asserted against recall's own prefix builder rather than
    against the map's opinion of itself. A repair that produced another unreachable path would
    satisfy every test above."""
    from services.pdp_category_classifier import category_path_prefix_for_query

    for query, source in (
        ("toner", "beauty/skincare/treat/toner"),
        ("lip gloss", "beauty/makeup/lips/lip-gloss"),
        ("lipstick", "beauty/makeup/lips/lip-balm"),
    ):
        prefix = category_path_prefix_for_query(query)
        assert prefix, query
        repaired = resolve(source)
        assert repaired and repaired.startswith(prefix), (
            "%r -> %r does not match recall's prefix %r for %r"
            % (source, repaired, prefix, query)
        )
        # and the control: the ORIGINAL did not
        assert not source.startswith(prefix), (
            "%r already matched %r; it was not broken" % (source, prefix)
        )


# --- the LLM classifier now checks membership ----------------------------------------------------


def test_the_llm_validator_rejects_a_shape_valid_invention_UNDER_AN_INDEXED_ROOT():
    """`_PATH_RE` accepts `beauty/skincare/treat/toner`: right casing, right separators, allowed
    root. The old validator returned it and the caller stored it. Shape is not membership.

    The rejection is scoped to roots recall actually INDEXES, which is the narrow case where a
    plausible-but-fake path is harmful: it looks classified while having no door."""
    from services.category_classifier_llm import _validate_path

    assert _validate_path("beauty/skincare/treat/toner") == "beauty/skincare/tone/toner"
    # A plausible nail path outside every leaf family. (`nails/nail-polish` is a leaf since
    # 2026-09-26 and is accepted -- see test_the_nail_leaves_are_leaves_now; a sibling under
    # `beauty/makeup/nails/` is inside that family's browse prefix, like any other family's.)
    assert _validate_path("beauty/makeup/manicure/polish") is None


def test_the_llm_validator_does_NOT_police_roots_recall_does_not_index():
    """THE REGRESSION AN EARLIER VERSION OF THIS CHANGE SHIPPED, caught by five pre-existing tests.

    The classifier serves eleven `_ALLOWED_ROOTS`; the recall taxonomy covers three
    (beauty, electronics, fashion). Requiring taxonomy membership everywhere turned a yoga mat into
    None — deleting a working classification to fix a beauty problem. `sports/yoga/mat` has no
    prefix in recall either way; that is not this function's business."""
    from services.category_classifier_llm import _validate_path

    assert _validate_path("sports/yoga/mat") == "sports/yoga/mat"
    assert _validate_path("wellness/supplements") == "wellness/supplements"
    assert _validate_path("food/snacks/bar") == "food/snacks/bar"


def test_a_path_can_be_REACHABLE_without_being_a_leaf():
    """`electronics` is the parent of the `electronics/ereader` leaf, so it is a live query prefix
    and `electronics/laptops/gaming` matches `LIKE 'electronics/%'` directly. Rejecting it for not
    being a leaf would throw away a classification that works."""
    from services.category_classifier_llm import _validate_path
    from services.category_path_aliases import has_category_door

    assert has_category_door("electronics/laptops/gaming")
    assert _validate_path("electronics/laptops/gaming") == "electronics/laptops/gaming"
    # ...and the control: `beauty` is NOT a leaf-parent, so the same shape under it has no door
    assert not has_category_door("beauty/quantum/entanglement")
    assert _validate_path("beauty/quantum/entanglement") is None


def test_has_category_door_agrees_with_recalls_own_prefix_builder():
    """Asserted against `category_path_prefix_for_query`, not against this module's opinion."""
    from services.pdp_category_classifier import category_path_prefix_for_query

    prefix = category_path_prefix_for_query("lipstick")
    assert prefix == "beauty/makeup/lip/"
    assert has_category_door("beauty/makeup/lip/lipstick")
    assert has_category_door("beauty/makeup/lip")          # ancestor: #2122 can admit it
    assert not has_category_door("beauty/makeup/lips/lip-gloss")   # sibling typo: no door


def test_the_llm_validator_still_accepts_a_real_leaf():
    """The control. A validator that rejected everything passes the test above."""
    from services.category_classifier_llm import _validate_path

    assert _validate_path("beauty/makeup/lip/lipstick") == "beauty/makeup/lip/lipstick"
    assert _validate_path("BEAUTY/MAKEUP/LIP/LIPSTICK") == "beauty/makeup/lip/lipstick"


def test_the_llm_validator_still_rejects_a_malformed_string():
    from services.category_classifier_llm import _validate_path

    assert _validate_path("not a path at all!") is None
    assert _validate_path(None) is None
    assert _validate_path(12) is None


# --- the map matches the cohort it was built for -------------------------------------------------


def test_the_map_covers_the_measured_production_cohort():
    """117 distinct off-taxonomy paths were measured on prod 2026-09-09. Each is either aliased or
    declared a gap; a path in neither would be silently left broken by a map that claims to have
    considered it. The counts are pinned so that trimming the map is a visible decision."""
    assert len(ALIASES) == 88, "alias count changed; re-measure before editing the expectation"
    assert len(TAXONOMY_GAPS) == 28
    # 2026-10-10: the `makeup/setting-spray` gap became an ALIAS of the new setting-spray leaf.
    # 2026-09-26: the three nail gaps became LEAVES (not dropped), and the `eyes/lashes` gap became
    # an ALIAS of the new false-lash leaf, together with two spellings measured since 09-09
    # (`eyes/false-lashes`, `eyes/false_lashes`) -- so 117 measured + 2 new sources.
    measured_nail_leaves = NAIL_LEAVES - {"beauty/makeup/nails/press-on-nails"}
    assert len(ALIASES) + len(TAXONOMY_GAPS) + len(measured_nail_leaves) == 117 + 2
    assert NAIL_LEAVES <= TAXONOMY_LEAVES


@pytest.mark.parametrize(
    "path,rows",
    [
        ("beauty/skincare/treat/toner", 316),
        ("beauty/skincare/sets", 49),   # a declared gap now, not an alias
        ("beauty/makeup/nails/nail-polish", 37),
        ("beauty/skincare/eye-care", 31),
    ],
)
def test_the_biggest_cohorts_are_each_decided(path, rows):
    """Named individually so the four largest cannot fall out of the map unnoticed."""
    assert path in ALIASES or path in TAXONOMY_GAPS or path in TAXONOMY_LEAVES, (path, rows)


# --- the cross-repo constraint this map violated in production ---------------------------------


def test_no_alias_collapses_a_path_the_GATEWAY_keeps_distinct():
    """THE CHECK THAT WAS MISSING, and the reason 7 path families were merged in prod 2026-09-10.

    PIVOTA-Agent's `beautyTaxonomy.js` carries three tables: canonical paths, aliases, and
    INTENTIONALLY_DISTINCT — "kept here so a future pass does not 'helpfully' collapse them". I
    diffed this map against the first two, found one conflict, and shipped. The third table is the
    one whose entire purpose is to say NO, and nothing compared against it.
    """
    from services.gateway_intentionally_distinct import INTENTIONALLY_DISTINCT

    from services.category_path_aliases import gateway_collisions

    collides = gateway_collisions(ALIASES)
    assert not collides, (
        "PIVOTA-Agent keeps these distinct; aliasing them away merges rows it relies on: %s"
        % collides
    )


def test_the_gateway_distinct_paths_are_GAPS_not_aliases_and_not_leaves():
    """Removing the aliases is not enough, and making them LEAVES is worse than leaving them.

    They are 3 segments, so recall's prefix would be their PARENT — `beauty/skincare/`,
    `beauty/makeup/` — and a "makeup set" query would browse every makeup row. Measured while
    trying it: `beauty/makeup/nails/nail-polish` and `beauty/makeup/lips/lip-gloss` both became
    "reachable", blinding the off-taxonomy invariant across two subtrees in order to make one
    cohort look fixed. So they stay off-taxonomy and stay COUNTED, which is what that invariant is
    for; closing them needs a 4-segment leaf per family, decided with the gateway."""
    from services.gateway_intentionally_distinct import INTENTIONALLY_DISTINCT

    for path in (
        "beauty/skincare/sets",
        "beauty/bodycare/sets",
        "beauty/makeup/sets",
        "beauty/skincare/eye/balm",
        "beauty/skincare/hand/cream",
        "beauty/skincare/moisturizer/balm",
        "beauty/skincare/oil",
    ):
        assert path in INTENTIONALLY_DISTINCT, path
        assert path in TAXONOMY_GAPS, "%s must be a declared gap" % path
        assert path not in ALIASES, "%s must not be merged" % path
        assert path not in TAXONOMY_LEAVES, "%s as a leaf over-broadens its parent prefix" % path
        assert resolve(path) is None, "a gap has no honest target"


def test_declaring_them_did_not_widen_any_query_prefix():
    """The control for the test above, and the measurement that rejected the leaf approach. If any
    of these ever becomes a leaf, its parent turns into a live browse prefix and whole subtrees
    stop being visible to `serving_eligible_off_taxonomy_path`."""
    from services.category_path_aliases import LEAF_PARENTS

    for parent in ("beauty/skincare", "beauty/makeup", "beauty/bodycare"):
        assert parent not in LEAF_PARENTS, (
            "%s is a query prefix; every path under it is now 'reachable' and invisible to the "
            "off-taxonomy check" % parent
        )
    assert not has_category_door("beauty/makeup/lips/lip-gloss")
    # nails/nail-polish HAS a door since 2026-09-26 -- as a 4-segment leaf of its own, whose parent
    # prefix is `beauty/makeup/nails/`, not by widening `beauty/makeup/` (asserted above).
    assert has_category_door("beauty/makeup/nails/nail-polish")


def test_the_collision_check_ACTUALLY_FIRES_on_a_colliding_map():
    """The mechanism, not the current state. Deleting the import-time assert left every other test
    in this file green, because they all only inspect a map that happens to be clean — the exact
    shape of an absence assertion that passes when the mechanism is absent too."""
    from services.category_path_aliases import gateway_collisions

    bad = dict(ALIASES)
    bad["beauty/skincare/sets"] = "beauty/sets/gift-set"
    assert gateway_collisions(bad) == ["beauty/skincare/sets"]
    # ...and the control: a path the gateway does NOT keep distinct is not flagged
    ok = dict(ALIASES)
    ok["beauty/skincare/totally-made-up"] = "beauty/sets/gift-set"
    assert gateway_collisions(ok) == []


def test_the_vendored_distinct_list_is_not_EMPTY_or_TRUNCATED():
    """An empty list makes the collision check pass on any map at all. Pinned by count and by the
    entries this repo actually collided with, so a bad regeneration is loud."""
    from services.gateway_intentionally_distinct import (
        GATEWAY_SOURCE_COMMIT,
        INTENTIONALLY_DISTINCT,
    )

    assert len(INTENTIONALLY_DISTINCT) == 16, "re-vendored? update the count deliberately"
    assert GATEWAY_SOURCE_COMMIT, "record which gateway commit this was copied from"
    for path in (
        "beauty/skincare/sets",
        "beauty/makeup/sets",
        "beauty/skincare/eye/balm",
        "beauty/skincare/hand/cream",
        "beauty/skincare/oil",
    ):
        assert path in INTENTIONALLY_DISTINCT, path


# --- the nail leaves (Peng 2026-09-26) -----------------------------------------------------------

NAIL_LEAVES = frozenset({
    "beauty/makeup/nails/nail-polish",
    "beauty/makeup/nails/cuticle-oil",
    "beauty/makeup/nails/nail-polish-remover",
    "beauty/makeup/nails/press-on-nails",
})
FALSE_LASHES = "beauty/makeup/eye/false-lashes"


def test_the_nail_leaves_are_leaves_now():
    """They were declared gaps ("no nail-colour leaf"); the classifier now carries them, so they are
    leaves, resolve to themselves, pass the LLM validator, and are neither gaps nor aliases."""
    from services.category_classifier_llm import _validate_path

    for path in NAIL_LEAVES | {FALSE_LASHES}:
        assert path in TAXONOMY_LEAVES, path
        assert path not in TAXONOMY_GAPS, path
        assert path not in ALIASES, path
        assert resolve(path) == path, path
        assert _validate_path(path) == path, path


def test_the_nail_leaves_widen_only_the_nails_prefix():
    """The control: a leaf's PARENT becomes a browse prefix. For these that is `beauty/makeup/nails`
    and nothing wider -- the over-broadening that keeps the 3-segment set paths as gaps."""
    from services.category_path_aliases import LEAF_PARENTS

    assert "beauty/makeup/nails" in LEAF_PARENTS
    assert "beauty/makeup" not in LEAF_PARENTS
    assert not has_category_door("beauty/makeup/manicure/polish")


def test_the_plural_eyes_lash_spellings_alias_to_the_false_lash_leaf():
    """`eyes/lashes` was a declared gap ("no false-lash leaf"); it and the two spellings measured
    since are the plural-`eyes/` typo of the leaf. `eyes/lash` is NOT: that cohort is mascara."""
    for path in ("beauty/makeup/eyes/lashes", "beauty/makeup/eyes/false-lashes",
                 "beauty/makeup/eyes/false_lashes"):
        assert resolve(path) == FALSE_LASHES, path
        assert path not in TAXONOMY_GAPS, path
    assert resolve("beauty/makeup/eyes/lash") == "beauty/makeup/eye/mascara"
