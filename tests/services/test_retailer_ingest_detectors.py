"""The automated stand-in for reading every row of a dry run, pinned on the rows that needed it."""
import pytest

from services import curated_brand_feed as feed
from services.retailer_ingest import detectors


def record(title, ptype, handle, *, body="<p>A lip colour.</p>", price="20.00", vendor="3CE", variants=None,
           brand="3CE", currency="USD"):
    return feed.shopify_product_to_record(
        {"id": abs(hash(handle)) % 10**9, "vendor": vendor, "title": title, "handle": handle,
         "product_type": ptype, "body_html": body, "images": [],
         "variants": variants or [{"id": abs(hash(handle + "v")) % 10**12, "price": price, "available": True}]},
        domain="k-touch.us", category_path="beauty", brand_override=brand, currency=currency,
        source_role="retailer", retailer_name="k-touch.us", emit_native_variants=True,
    )


# Copied from the live k-touch.us product page, 2026-09-23.
TONE_UP = record(
    "3CE - TONE UP TINT 40ml", "LIP TINT", "3ce-tone-up-tint-40ml",
    body="<p>Instantly Brighten and Refresh Your Complexion 3CE Tone Up Tint is a Korean tone-up cream "
         "designed to help improve the appearance of dull skin while creating a brighter complexion.</p>")


def rules(flags, severity=None):
    return {f["rule"] for f in flags if severity is None or f["severity"] == severity}


def test_the_mislabelled_tone_up_cream_is_held():
    flags = detectors.detect([TONE_UP])
    assert {"lip_row_copy_not_about_lips", "lip_row_implausible_size"} <= rules(flags, detectors.BLOCK)
    assert all(f["handle"] == "3ce-tone-up-tint-40ml" for f in flags)


@pytest.mark.parametrize("title,ptype", [
    ("3CE - Velvet Lip Tint Plush 4g", "LIP TINT"),
    ("BY TERRY | Rouge Opulent Lipstick", "Lipstick"),
    ("[O HUI] The First Geniture Lipstick", "Lipstick"),
    ("Lip Sleeping Balm 15g", "Lip Balm"),  # a balm tin under the 20 cut
])
def test_real_lip_products_raise_no_blocking_flag(title, ptype):
    assert detectors.blocking(detectors.detect([record(title, ptype, title.lower().replace(" ", "-"))])) == []


def test_a_title_that_names_another_class_contradicts_its_merchant_type():
    # The Wave 2 shape: a toner filed on a measured "Cleansers" shelf.
    rec = record("Pyunkang Yul Essence Toner", "Cleanser", "essence-toner", vendor="Pyunkang Yul",
                 brand="Pyunkang Yul")
    assert rec is not None
    assert "title_contradicts_category" in rules(detectors.detect([rec]), detectors.BLOCK)


# A $0.01 row never reaches here: the feed drops it (MIN_SELLABLE_PRICE). A priced test row does.
@pytest.mark.parametrize("kw", [dict(title="TEST Product", price="999999999.00"),
                                dict(title="Velvet Lip Tint", price="20.00", vendor="Lime Crime Dev",
                                     brand="Lime Crime Dev")])
def test_placeholder_rows_are_held(kw):
    title = kw.pop("title")
    rec = record(title, "LIP TINT", "x-" + title.lower().replace(" ", "-"), **kw)
    assert rec is not None
    assert "placeholder_product" in rules(detectors.detect([rec]), detectors.BLOCK)


def test_rows_the_lip_title_door_placed_hold_until_that_row_is_accepted():
    """Unattended, nothing but the title vouches for a door-placed row: it holds, per row."""
    with feed.lip_title_evidence():
        rec = record("3CE - Soft Matte Lipstick 3.5g", "", "soft-matte")
    flags = detectors.detect([rec])
    assert rules(flags) == {"placed_by_lip_title"}
    [flag] = detectors.blocking(flags)
    assert flag["key"] == "placed_by_lip_title:soft-matte"
    assert detectors.blocking(flags, accepted=[flag["key"]]) == []


def test_an_approval_accepts_exactly_the_flag_keys_it_names():
    flags = detectors.detect([TONE_UP])
    keys = [f["key"] for f in detectors.blocking(flags)]
    assert detectors.blocking(flags, accepted=keys[:1]) and not detectors.blocking(flags, accepted=keys)
    # keys are stable across runs of the same cohort
    assert keys == [f["key"] for f in detectors.blocking(detectors.detect([TONE_UP]))]


def test_non_records_are_skipped():
    assert detectors.detect([None, {}, {"pdp": None}, "x"]) == []


def test_a_lip_product_page_carrying_another_products_description_is_held():
    """k-touch.us, 2026-09-23: "3CE - Soft Matte Lipstick 3.5g - Warmish Move" was served with a rice
    squalane cleanser's description. Real lipstick copy ("flatters every skin tone", "brightens the
    complexion") is NOT flagged -- a face-word list held 4 of 10 real lipsticks and was replaced."""
    with feed.lip_title_evidence():  # the lip pass that placed these rows ran with the switch on
        wrong, right = _warmish_and_mood()
    flags = detectors.detect([wrong, right])
    copy_holds = {f["handle"] for f in detectors.blocking(flags) if f["rule"] == "lip_row_copy_not_about_lips"}
    assert copy_holds == {"3ce-soft-matte-lipstick-warmish-move"}


def _warmish_and_mood():
    wrong = record("3CE - Soft Matte Lipstick 3.5g - Warmish Move", "", "3ce-soft-matte-lipstick-warmish-move",
                   body="<p>Natural Skin-Clarity Boost From Korean Rice Germ Extract. Inspired by Korea's "
                        "rice bran cleansing rituals, this glow double cleanser leaves skin clear and bright.</p>")
    right = record("3CE - Mood Recipe Lip Color 3.5g", "", "3ce-mood-recipe-lip-color-3-5g",
                   body="<p>Universally flattering nude shades crafted to complement every skin tone and "
                        "brighten the complexion, with a creamy formula that glides over lips.</p>")
    return wrong, right


def test_an_absurd_price_alone_marks_a_placeholder():
    rec = record("Velvet Lip Tint Plush 4g", "LIP TINT", "priced-placeholder", price="1500.00")
    assert "placeholder_product" in rules(detectors.detect([rec]), detectors.BLOCK)



@pytest.mark.parametrize("title,ptype,body", [
    ("Pyunkang Yul Hand Cream", "Hand Cream", "<p>Rich cream for dry hands.</p>"),  # body/care on purpose
    ("Vaseline Lip Therapy Original 20g", "Lip Balm", "<p>Soothes chapped skin in a tin.</p>"),
    ("Healing Balm Tin 12g", "Lip Balm", "<p>Soothes and protects dry, chapped skin with pure petroleum jelly "
                                        "in a handy travel tin for everyday use.</p>"),
    ("Sugar Lip Scrub 30g", "Lip Scrub", "<p>Exfoliates dry, flaky skin.</p>"),
    ("Velvet Lip Tint 4g", "LIP TINT", "<p>" + "벨벳 립 틴트, 입술에 부드럽게 발리는 컬러. " * 3 + "</p>"),
])
def test_review_false_positives_do_not_hold(title, ptype, body):
    rec = record(title, ptype, title.lower().replace(" ", "-"), body=body)
    assert rec is not None
    assert detectors.blocking(detectors.detect([rec])) == [], detectors.detect([rec])


def test_a_set_filed_as_one_product_is_held():
    rec = record("[OHUI] Miracle Moisture Cleansing Oil Special Set", "Cleansing Oil", "cleansing-oil-set",
                 body="<p>A cleansing oil set.</p>")
    assert "set_filed_as_single_product" in rules(detectors.detect([rec]), detectors.BLOCK)



@pytest.mark.parametrize("title,ptype", [("Vitamin C Serum", "Hair"), ("Snail Mucin Essence", "Body Care")])
def test_an_area_leaf_is_exempt_only_when_its_own_title_names_the_area(title, ptype):
    rec = record(title, ptype, title.lower().replace(" ", "-"), body="<p>A serum.</p>")
    assert rec is not None and rec["pdp"]["category_path"]
    assert "title_contradicts_category" in rules(detectors.detect([rec]), detectors.BLOCK)



def test_a_single_unit_count_is_not_a_set():
    rec = record("3CE Velvet Lip Tint (1pc)", "LIP TINT", "one-pc", body="<p>Velvet colour for lips.</p>")
    assert "set_filed_as_single_product" not in rules(detectors.detect([rec]))
    two = record("3CE Velvet Lip Tint 2pcs", "LIP TINT", "two-pcs", body="<p>Velvet colour for lips.</p>")
    assert "set_filed_as_single_product" in rules(detectors.detect([two]))
    one_x = record("Moisture Cream 1 x 50ml", "Cream", "one-x", body="<p>A cream.</p>")
    assert "set_filed_as_single_product" not in rules(detectors.detect([one_x]))
    two_x = record("Moisture Cream 2 x 50ml", "Cream", "two-x", body="<p>A cream.</p>")
    assert "set_filed_as_single_product" in rules(detectors.detect([two_x]))


# ------------------------------------------------------------------ placeholder_price_store (2026-09-26)
# headandshoulders.com priced all 120 storefront variants at 1.00 and the drain applied 74 PDPs at $1.00:
# $1.00 clears the per-row rule. The signal is store-wide, so the rule is judged over the whole cohort.

def store(prices_per_product, *, domain="headandshoulders.com", currency="USD"):
    """One real-producer record per product, each carrying the given variant prices (>= 1.00: the feed
    drops a variant under MIN_SELLABLE_PRICE, so a sub-dollar row is built by `hand_record`)."""
    out = []
    for i, prices in enumerate(prices_per_product):
        handle = f"p{i}"
        rec = feed.shopify_product_to_record(
            {"id": 7_000_000 + i, "vendor": "Brand", "title": f"Moisture Shampoo {i}", "handle": handle,
             "product_type": "Shampoo", "body_html": "<p>A shampoo.</p>", "images": [],
             "variants": [{"id": 40_000_000_000 + i * 100 + j, "price": f"{p:.2f}", "available": True,
                           "title": f"Size {j}"} for j, p in enumerate(prices)]},
            domain=domain, category_path="beauty", brand_override="Brand", currency=currency,
            source_role="brand_official", emit_native_variants=True,
        )
        assert rec is not None and len(rec["pdp"]["variants"]) == len(prices), (prices, rec)
        out.append(rec)
    return out


def hand_record(handle, prices):
    """The producer's shape (pdp.variants + one offer at the first variant's price), for prices the
    Shopify feed would drop before detect() sees them."""
    return {"pdp": {"product_name": f"Item {handle}", "brand": "Brand", "category_path": "beauty/haircare/shampoo",
                    "variants": [{"variant_id": f"{handle}-{j}", "price": p} for j, p in enumerate(prices)]},
            "offers": [{"canonical_url": f"https://example.com/products/{handle}", "price": prices[0]}]}


def test_the_hand_record_matches_the_producers_shape():
    [real] = store([[12.0, 14.0]])
    fake = hand_record("p0", [12.0, 14.0])
    assert detectors._variant_prices(real) == detectors._variant_prices(fake) == [12.0, 14.0]
    assert detectors._handle(real) == detectors._handle(fake) == "p0"


def _varied(n, start=5.0):
    return [round(start + 0.37 * i, 2) for i in range(n)]


def _held(flags):
    return {f["handle"] for f in flags if f["rule"] == "placeholder_price_store" and f["severity"] == detectors.BLOCK}


@pytest.mark.parametrize("name,records,expect_held", [
    # HOLD: headandshoulders.com, 2026-09-26 -- 120 variants all at 1.00.
    ("120 all at 1.00", lambda: store([[1.0]] * 120), set(range(120))),
    # HOLD: honest.com's shape -- 317 variants, 125 at 1.00 (0.39 <= 1.00 share).
    ("317 with 125 at 1.00", lambda: store([[1.0]] * 125 + [[v] for v in _varied(192)]), set(range(125))),
    # HOLD: a 0.99 token (built by hand: the feed drops sub-dollar variants).
    ("50 all at 0.99", lambda: [hand_record(f"p{i}", [0.99]) for i in range(50)], set(range(50))),
    # DO NOT HOLD: under the minimum count (the per-row rule still owns <= 0.5).
    ("15 all at 1.00", lambda: store([[1.0]] * 15), set()),
    # DO NOT HOLD: a flat-priced legitimate store -- its modal price is above 2.00.
    ("200 with 190 at 10.00", lambda: store([[10.0]] * 190 + [[v] for v in _varied(10, 20.0)]), set()),
    # DO NOT HOLD: 20 of 300 at 1.00 is under the <= 1.00 share.
    ("300 with 20 at 1.00", lambda: store([[1.0]] * 20 + [[v] for v in _varied(280)]), set()),
])
def test_store_level_placeholder_pricing(name, records, expect_held):
    flags = detectors.detect(records())
    assert _held(flags) == {f"p{i}" for i in expect_held}, name


def test_only_rows_whose_every_price_is_the_placeholder_are_held():
    # 40 variants at 1.00 of 100 (0.40: the store holds) -- but 30 of them ride on real products whose
    # other variants are real prices. Those products are not held; the ten all-1.00 rows are.
    real_with_token = [[1.0, 1.0, 1.0, 24.0, 26.0, 28.0]] * 10   # 30 at 1.00, 30 real
    all_token = [[1.0]] * 10                                       # 10 at 1.00
    rest = [[v] for v in _varied(30, 30.0)]
    flags = detectors.detect(store(real_with_token + all_token + rest))
    assert _held(flags) == {f"p{i}" for i in range(10, 20)}


def test_a_store_whose_one_price_rows_are_only_variants_of_real_products_holds_no_row():
    # Every 1.00 is a variant beside a real price (0.67 at 1.00: the store holds) -- nothing is
    # placeholder-only, so no row is held.
    flags = detectors.detect(store([[1.0, 1.0, 19.0]] * 30))
    assert _held(flags) == set()


def test_the_flag_names_the_store_evidence_and_is_keyed_per_row():
    flags = [f for f in detectors.detect(store([[1.0]] * 120)) if f["rule"] == "placeholder_price_store"]
    assert flags[0]["key"] == "placeholder_price_store:p0"
    assert "120/120 variants at 1.00" in flags[0]["detail"]
    assert len({f["key"] for f in flags}) == 120
    # an operator accepts a specific real $1 item; the rest still hold
    left = detectors.blocking(flags, accepted=["placeholder_price_store:p7"])
    assert len(left) == 119 and "placeholder_price_store:p7" not in {f["key"] for f in left}


def test_a_modal_placeholder_at_two_dollars_holds_and_the_per_row_rule_is_untouched():
    recs = store([[2.0]] * 40 + [[v] for v in _varied(5, 30.0)])  # 40/45 = 0.89 at 2.00
    flags = detectors.detect(recs)
    assert _held(flags) == {f"p{i}" for i in range(40)}
    assert "placeholder_product" not in rules(flags)  # 2.00 was never a per-row placeholder


def test_a_subset_recheck_does_not_judge_the_store():
    recs = store([[1.0]] * 30)
    assert _held(detectors.detect(recs, store_level=False)) == set()
    assert len(_held(detectors.detect(recs))) == 30


@pytest.mark.parametrize("n,held", [(19, False), (20, True)])
def test_the_minimum_count_boundary(n, held):
    assert bool(_held(detectors.detect(store([[1.0]] * n)))) is held


@pytest.mark.parametrize("products,held", [
    ([[1.0] * 5] * 5, True),        # 25 variants over 5 products: the minimum counts VARIANTS
    ([[1.0] * 4] * 4 + [[1.0, 1.0, 1.0]], False),  # 19 variants over 5 products
])
def test_the_minimum_counts_variants_not_products(products, held):
    assert bool(_held(detectors.detect(store(products)))) is held


def test_a_verdict_judged_elsewhere_flags_only_the_rows_checked():
    crawl = store([[1.0]] * 25)
    verdict = detectors.placeholder_price_store_verdict(crawl)
    assert verdict and verdict["ceiling"] == 1.0
    kept = crawl[:5]  # 5 rows alone are under the minimum; the crawl's verdict still holds them
    assert _held(detectors.detect(kept)) == set()
    assert _held(detectors.detect(kept, store_verdict=verdict)) == {f"p{i}" for i in range(5)}
    assert _held(detectors.detect(crawl, store_verdict=None)) == set()


@pytest.mark.parametrize("price,token,rest,held", [
    (1.00, 30, 70, True),    # 0.30 at <= 1.00: holds
    (1.00, 29, 71, False),   # 0.29: does not
    (1.01, 30, 70, False),   # 1.01 is a price, not the token
])
def test_the_token_share_boundary(price, token, rest, held):
    flags = detectors.detect(store([[price]] * token + [[v] for v in _varied(rest)]))
    assert bool(_held(flags)) is held


def test_a_sub_dollar_modal_store_holds_its_one_dollar_rows_too():
    # 80 at 0.99 (the modal placeholder) and 20 at 1.00: both are token prices, every row holds.
    recs = [hand_record(f"p{i}", [0.99]) for i in range(80)] + [hand_record(f"p{i}", [1.0]) for i in range(80, 100)]
    assert _held(detectors.detect(recs)) == {f"p{i}" for i in range(100)}


def test_the_evidence_names_the_token_price_when_the_mode_ties():
    flags = [f for f in detectors.detect(store([[1.0]] * 25 + [[30.0]] * 25)) if f["rule"] == "placeholder_price_store"]
    assert len(flags) == 25 and "25/50 variants at 1.00" in flags[0]["detail"]


@pytest.mark.parametrize("price,modal,rest,held", [
    (2.00, 80, 20, True),    # 0.80 at 2.00: holds
    (2.00, 79, 21, False),   # 0.79: does not
    (2.01, 90, 10, False),   # a modal price over 2.00 is a real price list
])
def test_the_modal_share_and_ceiling_boundaries(price, modal, rest, held):
    flags = detectors.detect(store([[price]] * modal + [[v] for v in _varied(rest, 30.0)]))
    assert bool(_held(flags)) is held


# ------------------------------------------------------------------ set false positives (2026-09-27)
# Held on the US top-100 re-runs: stilacosmetics.com "Smudge & Set" (the verb "set" in a product-line
# name) and tartecosmetics.com gift sets whose titles name what is inside them. Every refusing case below
# is held on main and must stay held (review of #2402).

def _on(rec, category_prefix):
    assert rec is not None and (rec["pdp"]["category_path"] or "").startswith(category_prefix), rec and rec["pdp"]
    return rec


def _flags(title, ptype, category_prefix="beauty/"):
    return rules(detectors.detect([_on(record(title, ptype, title.lower().replace(" ", "-")), category_prefix)]))


@pytest.mark.parametrize("title,ptype", [
    ("Stay All Day® Smudge & Set™ Waterproof Gel Eye Liner", "Eyeliner"),   # X & Set
    ("Smooth and Set Loose Powder", "Setting Powder"),                        # X and Set
    ("ICD Set & Stay Makeup Spray 80 mL", "Face Primer"),                     # Set & X
    ("Super Hold Set and Keep Spray Primer", "Face Primer"),                  # Set and X
    ("Super Hold Set and Keep Primer", "Face Primer"),
    ("Shape and Set Brow Primer", "Face Primer"),                             # X and Set, no noun after
    # "Set" + what it is -- one per noun, with no pair to lean on
    ("Forever Set Powder", "Setting Powder"),
    ("Dewy Set Spray Primer", "Face Primer"),
    ("Curl Set Lotion Primer", "Face Primer"),
    ("Curl Set Essence Primer", "Face Primer"),
    ("tfit Translucent Set Finishing Powder 7g", "Setting Powder"),
    ("All Set Setting Powder", "Setting Powder"),
    ("Hold Set Fixer Primer", "Face Primer"),
    ("Neutral Set Translucent Powder", "Setting Powder"),
    ("Cloud Set Loose Powder – Translucent Peachy", "Setting Powder"),
    ("Cloud Set™ Loose Powder", "Setting Powder"),                           # a mark glued to "Set"
    ("Kett Set Pressed Powder", "Setting Powder"),
    ("Set Powder Brush", "Setting Powder"),              # a brush FOR setting powder
    ("Hydrating Set and Stay Spray", "Setting Powder"),
])
def test_the_verb_set_in_a_product_line_name_is_not_a_set(title, ptype):
    assert "set_filed_as_single_product" not in _flags(title, ptype), title


@pytest.mark.parametrize("title,ptype", [
    ("Hydrating Toner & Emulsion Set", "Toner"),        # "&" not beside "Set"
    ("Hydrating Toner Set + Free Gift", "Toner"),       # "+" beside "Set" is a bundle
    ("Glow Toner Set +Refill", "Toner"),
    ("Hydrating Toner + Set of Cotton Pads", "Toner"),
    ("Primer + Set Spray", "Face Primer"),              # "+" before, even with a finish noun after
    ("Bake & Set + Refill", "Setting Powder"),          # "+" after, partner not a product noun
    ("Travel Prime & Set Duo", "Face Primer"),          # the verb, but the title names a pack
    ("Mini Prime & Set Pouch", "Face Primer"),
    ("Translucent Set Finishing Powder Brush Special Set", "Setting Powder"),  # one verb, one real set
    ("Travel Kit Bake & Set Powder", "Setting Powder"),  # a real set BEFORE the verb
    ("Makeup Kit Powder", "Setting Powder"),            # only "set" can be the verb, not "kit"
    ("Glow Kit Powder", "Setting Powder"),
    ("Glow [Mini] Set Powder", "Setting Powder"),       # a bracketed set word
    ("Smudge & Set Eyeliner & Mascara", "Eyeliner"),     # a product joined after it
    ("Luxe Set & Mini Mirror", "Setting Powder"),        # a tool two words after the join
    ("Smudge & Set Eyeliner and Mini Mascara", "Eyeliner"),  # a product two words after the join
    ("Bake & Set Powder & Beauty Blender", "Setting Powder"),
    ("Set and Forget Mascara", "Setting Powder"),        # "forget" is not a verb the corpus pairs with Set
    # partners that are not verbs (review of 763dc7c40, inputs3)
    ("Vitamin C Set & Save", "Serum"),
    ("Anti-Aging Set & More", "Serum"),
    ("Summer Set & Tote", "Serum"),
    ("Spa Set & Headband", "Face Mask"),
    ("Luxury Set & Keychain", "Serum"),
    ("Cleanse & Set", "Cleanser"),
    ("Whitening Set Essence", "Essence"),               # lotion/essence only after "curl"/"dry"
    ("Moisture Set Lotion", "Lotion"),
    ("Men's Set Lotion", "Lotion"),
    ("Smudge & Set Eyeliner Black & Brown", "Eyeliner"),  # anything joined after the verb
    ("Set & Stay Spray & Brush", "Face Primer"),        # a join after the "Set & X" pair
    ("Serum Set Powder", "Setting Powder"),             # a product noun before "Set" + a setting noun
    ("Cloud Set Powder & Puff", "Setting Powder"),      # a join after "Set" + a setting noun
    ("Cloud Set Powder with Puff", "Setting Powder"),
    # a multi-pack beside the verb (review of accfb44cb)
    ("Smudge & Set Gel Eye Liner 2 Pack", "Eyeliner"),
    ("Bake & Set Setting Powder (Pack of 2)", "Setting Powder"),
    ("Kett Set Powder Loose Twin Pack", "Setting Powder"),
    ("Smooth & Set Loose Powder 2ct", "Setting Powder"),
    ("Bake & Set Powder x2", "Setting Powder"),
    ("Stay All Day Smudge & Set Waterproof Gel Eye Liner 2 Count", "Eyeliner"),  # review of d042640af
    ("Smudge & Set Gel Eye Liner 2pk", "Eyeliner"),
    ("Bake & Set Setting Powder (2 pk)", "Setting Powder"),
    ("Smooth & Set Loose Powder Double Pack", "Setting Powder"),
    ("Smooth & Set Loose Powder 2 Units", "Setting Powder"),
    ("Curl Set Lotion 200ml 2 Bottles", "Face Primer"),
    ("Lip Set & Go", "Setting Powder"),                 # an area word before "Set"
    ("Glow Toner Sticker Set &", "Toner"),              # "&" with nothing after it
    ("Glow Toner Set", "Toner"),
    ("Toner Set Essence 50ml", "Toner"),                # a product noun before "Set"
    ("Makeup Set & Brush", "Face Primer"),              # a product noun joined after it
    ("Serum Set & Cream", "Toner"),
    ("Brush Set & Bag", "Face Primer"),
    ("Sheet Mask Set and Eye Patch", "Toner"),
    ("Starter Set Lotion + Cream", "Toner"),
    # a set word before "Set" (review of bbadf0343)
    ("Starter Set Essence", "Essence"),
    ("Discovery Set Essence Toner", "Toner"),
    ("Trial Set Mist Toner", "Toner"),
    ("Hair Care Set Spray", "Hair Spray"),
    ("Mini Set Powder", "Setting Powder"),
    ("Holiday Set & Mirror Powder", "Setting Powder"),
    ("Travel Set Powder and Puff", "Setting Powder"),
    ("Glow Set Gel Cleanser", "Cleanser"),              # "gel" is not a setting noun
    # a tool joined after it
    ("Smudge & Set Eyeliner & Sharpener", "Eyeliner"),
    ("Bake & Set Powder + Puff", "Setting Powder"),
])
def test_a_real_set_beside_the_verb_exemption_is_still_held(title, ptype):
    rec = _on(record(title, ptype, title.lower().replace(" ", "-")), "beauty/")
    assert not rec["pdp"]["category_path"].startswith("beauty/sets/")
    assert "set_filed_as_single_product" in rules(detectors.detect([rec]), detectors.BLOCK), title


@pytest.mark.parametrize("title", [
    "1025 Dokdo Toner 200ml + Dokdo Lotion 200ml",      # " + "
    "Repair Cream 100ml+Essence 50ml",                  # "+" after a size
    "Hydrating Serum 30ml (+Collagen Jelly Cream 50ml)",  # "(+"
    "CC Undereye Corrector + FREE Tubing Mascara",      # "+ free"
    "Cushion Foundation 15g + Refill",                  # "+ refill"
    "Hydrating Serum 30ml + Free Pouch",                # "+ free", partner not a product noun
    "Niacinamide 20 Serum (5ea)",                       # a count (_SET_TITLE)
    "Niacinamide Serum 2 Pack",
    "Scent Exploration Perfume (10 Vials)",
    "Acne Attack Serum Trio",
    "Hydrating Serum Duo",
    "Acne Control Serum Regimen",
    "Clear Skin Serum System",
    "Spa Serum System for Acne Prone Skin",
    "Clear Serum System - Travel Size",
    "Glow Serum Sample Pack",
    "Glow Serum Double Pack",
    "Glow Serum Value Pack",
    "Glow Serum Discovery Pack",
    "Serum 30ml + Glow Cream 50ml",                     # "+" joins even a one-word part, when both are sized
    "Glow Serum Gift Box",
    "Deep Mask Variety Pack",
    "Harvest Gift Basket Body Wash Cleanser",
])
def test_a_set_on_the_sets_shelf_may_name_what_is_inside_it(title):
    assert "title_contradicts_category" not in _flags(title, "Gift Set", "beauty/sets/"), title


@pytest.mark.parametrize("title", [
    "AHC Renew Age Total Reset Cream 50ml",             # koolseoul types it "Skincare Set"
    "Rosee Sib Jang Saeng Cheon Ji Hyang Essence 45Ml",
    "Skin Setting Tone Up Sun Base SPF50+ PA+++",       # "+" of SPF/PA is not a join
    "Julyme Damage Care Perfume Hair Oil 30ml (Sunset Freesia)",  # two leaves, one product
    "Pyunkang Yul Essence Toner",
    "Hydrating Cream with Ceramides 50ml",              # "with" an ingredient
    "Plump and Glow Cream",                             # "and" between adjectives
    "Cica+ Soothing Cream",                             # "+" glued to a word
    "Sun Serum SPF50+ Moisture Cream",                  # SPF "+" between two leaf words
    "Glow Eyeshadow Quad",                              # one palette
    "Daily Moisturizer with SPF 30",                    # "with SPF" is a feature (review of bbadf0343)
    "Tinted Moisturizer with Sunscreen SPF 30",
    "Lip Balm with SPF 15",
    "1-Step Exfoliating Peel Pad",                      # a count of 1
    "Hydrating Serum 1 Pack",
    "Hair & Body Wash",                                 # one noun shared by two modifiers
    "Day & Night Cream",
    "Shampoo & Conditioner 2-in-1",                     # one product named twice
    "Toner and Essence in One",
    "Hydrating Toner and Rich Essence 2-in-1",
    "Anti-Aging System Serum",                          # "system" naming a line, not a kit
    "Acne Treatment System Gel",
    "Moisture Cream 1 x 50ml",                          # a count of 1
    "[Gift Recommendation] Layered Perfume",            # "gift" alone is not a gift set
    "COGIT Sun Block Cooling UV Protection Hoodie UPF50+ 爽壁+ 全脸防护",  # CJK "+"; not beauty at all
    # "&"/"and"/"with" are not set evidence (review of 763dc7c40): real sets joined only by them stay for a
    # reviewer, because the same words join a single product's name. Real titles from reports/*/catalogs:
    "Glow Pot Eyeshadow & Brush",                       # tartecosmetics.com set: held, one click
    "Vitamin Serum with Mini Brush",
    "Philip B Nordic Wood Hair & Body Shampoo 11.8 oz",
    "Kylie Jenner Vanilla Dew Hair & Body Mist 236ml",
    "Klorane Shampoo with Nettle For Oily Hair",
    "Dr.Groot Shampoo For Thinning & Weak Hair 180ml",
    "Bosley Hair & Scalp Conditioner",
    "YUNJAC Hydrating & Soothing Toner With Baeknyoncho Extract & Ujildu Water",
    "Momori Hair Cream for Dry & Damaged Hair",
    "Face Reality Acne Face and Body Wash",
    "[Medi-Peel] Peptide 9 Volume And Tension Tox Cream Pro",
    "Milbon Serum with Natural oil Hair Treatment",
    "Philip B Nordic Wood Hair + Body Shampoo",         # "+" beside an area word, not a product
    # review of accfb44cb: weak evidence stays for a reviewer
    "LADOR Perfumed Hair Oil 80ml (5types)",            # "N types": a pick-one variant
    "Scentlier Perfume Hand Cream 50ml (4types)",
    "Soy Nut Line 2 Types (Cream/Toner)",               # a real set, held: one click
    "Enzyme Cleansing Powder 5kinds",
    "celimax 2-Step Gel Mask 1ea",                      # "N-step": one mask
    "CNP 2- Step Greenery Calming Ampule Mask 1 Sheet",
    "Cleanse & Calm 3-Step Serum",
    "OneBase Concealer + Foundation 045",               # "+" in a hybrid's name: no size on each side
    "SUN PREP Sunscreen + Primer",
    "Serum + Glow Cream",
    "Hydrating Toner 200ml + Lotion",                   # only one side sized
    "Silk Hair 100ml + Body Shampoo 200ml",             # sized, but "Hair" alone is an area, not a product
    "Sun Serum 30ml SPF50+ Moisture Cream 50ml",        # sized both sides, but the "+" is SPF's
    "Concealer 12 + Foundation 045",                    # shade numbers are not sizes
])
def test_a_single_product_the_store_types_as_a_set_is_still_held(title):
    rec = _on(record(title, "Skincare Set", title.lower().replace(" ", "-")), "beauty/sets/")
    assert "title_contradicts_category" in rules(detectors.detect([rec]), detectors.BLOCK), title


def test_a_set_title_on_the_sets_shelf_is_not_a_set_filed_as_one_product():
    assert "set_filed_as_single_product" not in _flags("Glow Toner Set", "Gift Set", "beauty/sets/")


@pytest.mark.parametrize("word", ["Starter", "Discovery", "Trial", "Travel", "Holiday", "Mini", "Minis", "Sample",
                                  "Bestseller", "Best-Sellers", "Care", "Makeup", "Skincare",
                                  "Beauty", "Deluxe", "Festive", "Lip", "Lips", "Eye", "Eyes", "Brow", "Brows",
                                  "Lash", "Lashes", "Face", "Nail", "Nails", "Body", "Skin"])
def test_a_set_word_before_set_makes_it_the_noun(word):
    assert "set_filed_as_single_product" in _flags(f"Glow {word} Set Powder", "Setting Powder")


@pytest.mark.parametrize("tool", ["Puff", "Puffs", "Sharpener", "Mirror", "Bag", "Case", "Pouches", "Sponge", "Applicator"])
def test_a_tool_joined_after_the_verb_set_is_a_second_item(tool):
    assert "set_filed_as_single_product" in _flags(f"Bake & Set Powder + {tool}", "Setting Powder")


@pytest.mark.parametrize("verb", ["Prime", "Mist", "Smooth", "Smudge", "Bake", "Perfect", "Twist", "Brighten", "Prep",
                                  "Shape", "Spray", "Grip", "Blow"])
def test_each_corpus_verb_before_set(verb):
    assert "set_filed_as_single_product" not in _flags(f"Stay All Day {verb} & Set Gel Eye Liner", "Eyeliner"), verb


@pytest.mark.parametrize("verb", ["Protect", "Keep", "Wave", "Correct", "Stay", "Flow"])
def test_each_corpus_verb_after_set(verb):
    assert "set_filed_as_single_product" not in _flags(f"Hold Set & {verb} Primer", "Face Primer"), verb


@pytest.mark.parametrize("title,held", [("Curl Set Lotion Primer", False), ("Quick Dry Set Lotion Primer", False),
                                        ("Glow Set Lotion Primer", True), ("Curls Set Essence Primer", False)])
def test_set_lotion_is_the_verb_only_after_a_hair_word(title, held):
    assert ("set_filed_as_single_product" in _flags(title, "Face Primer")) is held, title


# --- Currency-aware placeholder bounds (2026-10-10, SG capture work: JPY base crawls of Japanese retailers) ---

def _jp(title, handle, price, currency="JPY"):
    return record(title, "Lotion", handle, body="<p>A hydrating lotion for the face.</p>", price=price,
                  vendor="KOSE", brand="KOSE", currency=currency)


@pytest.mark.parametrize("price,currency", [("1980", "JPY"), ("5500", "JPY"), ("29700", "JPY"),
                                            ("25000", "KRW"), ("189000", "KRW")])
def test_a_real_yen_or_won_price_is_not_a_placeholder(price, currency):
    # Before: any price >= 1000 in ANY currency was placeholder_product, so nearly every JPY/KRW row held.
    flags = detectors.detect([_jp("Sekkisei Lotion 200ml", "sekkisei-lotion", price, currency)], store_level=False)
    assert "placeholder_product" not in rules(flags)


@pytest.mark.parametrize("price,currency", [("999999999", "JPY"), ("40", "JPY"), ("200000000", "KRW"),
                                            ("1000", "USD"), ("1000", ""), ("1000", "XTS")])
def test_a_test_or_token_row_still_holds_in_its_currency(price, currency):
    # USD, a missing currency and an unlisted one keep the dollar bounds exactly as before.
    flags = detectors.detect([_jp("Sekkisei Lotion 200ml", "sekkisei-lotion", price, currency)], store_level=False)
    assert "placeholder_product" in rules(flags, detectors.BLOCK)


@pytest.mark.parametrize("currency,scale", [
    ("JPY", 100.0), ("jpy", 100.0), ("KRW", 1000.0), ("HKD", 5.0), ("CNY", 5.0), ("TWD", 30.0), ("THB", 30.0),
    ("PHP", 50.0), ("INR", 50.0), ("IDR", 10000.0), ("VND", 20000.0), ("MYR", 4.0), ("SEK", 10.0), ("NOK", 10.0),
    ("DKK", 5.0), ("MXN", 15.0), ("ZAR", 15.0),
    ("USD", 1.0), ("SGD", 1.0), ("AUD", 1.0), ("EUR", 1.0), ("GBP", 1.0), ("CAD", 1.0), (None, 1.0), ("", 1.0)])
def test_the_magnitude_is_a_unit_not_a_rate(currency, scale):
    assert detectors.price_magnitude(currency) == scale


def test_a_one_yen_sample_variant_beside_a_real_price_holds_the_row():
    # The feed's sellable floor is 1.0 in the store's currency: it drops a $0.01 promo, but keeps a 1-yen one.
    rec = record("Sekkisei Lotion 200ml", "Lotion", "sekkisei-lotion", body="<p>A lotion for the face.</p>",
                 vendor="KOSE", brand="KOSE", currency="JPY",
                 variants=[{"id": 44_100_000_001, "price": "5500", "available": True, "title": "200ml"},
                           {"id": 44_100_000_002, "price": "1", "available": True, "title": "Sample"}])
    assert "placeholder_product" in rules(detectors.detect([rec], store_level=False), detectors.BLOCK)


def test_a_dollar_row_is_untouched_by_the_variant_low_bound():
    rec = record("Lip Oil", "Lip Oil", "lip-oil", variants=[
        {"id": 44_100_000_011, "price": "38.00", "available": True, "title": "Rose"},
        {"id": 44_100_000_012, "price": "0.01", "available": True, "title": "Promo"}])  # the feed drops the $0.01 variant
    assert "placeholder_product" not in rules(detectors.detect([rec], store_level=False))


def test_a_yen_store_priced_at_a_token_still_holds_store_wide():
    # The headandshoulders.com shape in yen: every variant at 100 yen is a token, not a price list.
    held = _held(detectors.detect(store([[100.0]] * 40, domain="example.jp", currency="JPY")))
    assert held == {f"p{i}" for i in range(40)}


def _yen(n, start=1980):
    return [float(start + 110 * i) for i in range(n)]  # whole yen, as a JPY store prices


def test_a_real_yen_store_holds_nothing():
    flags = detectors.detect(store([[v] for v in _yen(60)], domain="example.jp", currency="JPY"))
    assert _held(flags) == set() and "placeholder_product" not in rules(flags)


def test_the_yen_token_path_alone_holds():
    # 21/60 = 0.35 at <= 100 yen (the scaled 1.00 token), the rest real and varied: no mode reaches 0.80, so only
    # the token path can hold -- it pins the scaled token price AND the scaled ceiling it appends.
    recs = store([[100.0]] * 21 + [[v] for v in _yen(39)], domain="example.jp", currency="JPY")
    assert _held(detectors.detect(recs)) == {f"p{i}" for i in range(21)}


def test_the_yen_modal_path_alone_holds():
    # 43/50 = 0.86 at 150 yen: above the 100-yen token, at or below the 200-yen modal ceiling.
    recs = store([[150.0]] * 43 + [[v] for v in _yen(7)], domain="example.jp", currency="JPY")
    assert _held(detectors.detect(recs)) == {f"p{i}" for i in range(43)}


def test_a_mixed_currency_population_takes_the_largest_scale():
    # Unreachable through the pipeline (it stops a job whose records disagree on currency), but the verdict
    # must be deterministic and fail safe: yen tokens judged on the yen scale, whatever the set order.
    recs = (store([[100.0]] * 30, domain="example.jp", currency="JPY")
            + store([[v] for v in _varied(10, 30.0)], domain="example.com", currency="USD"))
    verdict = detectors.placeholder_price_store_verdict(recs)
    assert verdict is not None and verdict["ceiling"] >= 100.0
