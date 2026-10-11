from __future__ import annotations

from sqlalchemy import (
    REAL,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.sql import expression, func

from db.database import JSONB_TYPE, metadata


catalog_merchants = Table(
    "catalog_merchants",
    metadata,
    Column("merchant_id", String(64), primary_key=True),
    Column("merchant_name", String(255), nullable=True),
    Column("primary_platform", String(64), nullable=True),
    Column("status", String(32), nullable=False, server_default="active"),
    # expression.true(), NOT the string "true": a str server_default is rendered
    # as a QUOTED literal, so create_all emits `DEFAULT 'true'` and SQLite — which
    # has no native boolean — stores the four-character string for any INSERT that
    # omits the column. `COALESCE(m.indexable, TRUE) IS TRUE`, the gate every
    # cross-merchant recall lane runs, then evaluates to FALSE and the merchant
    # silently drops out of search. expression.true() renders as the dialect's own
    # constant (1 on SQLite, true on Postgres). Same for is_first_party below.
    Column("indexable", Boolean, nullable=False, server_default=expression.true()),
    Column("source_system", String(64), nullable=True),
    Column("source_ref", String(255), nullable=True),
    Column("metadata_json", JSONB_TYPE, nullable=True),
    # PR-1b — opt-in for the auto-re-audit scheduler. 'none' | 'weekly'
    # | 'monthly'. Migration 078_catalog_merchants_audit_schedule.sql
    # adds the column with default 'none' to existing rows — as TEXT, not
    # VARCHAR(16), so that is what this declares.
    Column("audit_schedule", Text, nullable=False, server_default="none"),
    # Stage 2a (mig 084): timestamp of the merchant's most recent
    # successful Path A full sync. Used by the sweep to compare per-row
    # last_seen_in_sync_at — without it we couldn't tell "merchant
    # hasn't synced lately" from "row was deleted from upstream."
    Column("last_full_sync_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
)


catalog_products = Table(
    "catalog_products",
    metadata,
    Column("product_key", String(255), primary_key=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("platform", String(64), nullable=False, index=True),
    Column("source_product_id", String(128), nullable=False),
    Column("catalog_track", String(32), nullable=False, server_default="internal_merchant"),
    Column("truth_tier", String(32), nullable=False, server_default="primary"),
    Column("readiness_tier", String(32), nullable=False, server_default="commerce_ready"),
    Column("source_system", String(64), nullable=True),
    Column("source_ref", String(255), nullable=True),
    Column("source_domain", Text, nullable=True),
    Column("suppression_reason", Text, nullable=True),
    Column("suppressed_at", DateTime(timezone=True), nullable=True),
    Column("suppression_metadata", JSONB_TYPE, nullable=True),
    Column("title", Text, nullable=False),
    Column("description", Text, nullable=True),
    Column("brand", String(255), nullable=True),
    Column("product_type", String(255), nullable=True),
    Column("category", String(255), nullable=True),
    # Phase 2 / O-5 — hierarchical PDP taxonomy path. The sync path writes
    # these inline, so runtime metadata must match migration 069.
    Column("category_path", String(255), nullable=True),
    # Human-readable leaf label for category_path (mig 097). Also in
    # schema_guard's REQUIRED_SCHEMA for catalog_products.
    Column("category_label", String(255), nullable=True),
    Column("category_confidence", REAL, nullable=True),
    Column("category_label_source", String(32), nullable=True),
    # Durable category_kind in {skincare, haircare, supplement} (mig 151).
    # Drives claim-safety / disclaimers / serving-gate; see services.category_kind.
    Column("category_kind", String(16), nullable=True),
    # Durable top-level vertical in {beauty, fashion, electronics, other} (mig 173).
    # Resolved once at sync intake and read by both this repo and the Node serving
    # layer; see services.vertical_profiles.resolve_vertical.
    Column("resolved_vertical", String(16), nullable=True),
    # Cached LLM attribute-extractor output keyed by a source_hash (mig 174) so
    # re-audits don't re-pay the LLM. See services.llm_attribute_extractor.
    Column("llm_attributes", JSONB_TYPE, nullable=True),
    # ADR-009 seller-of-record on the CANONICAL row (mig 176, convergence P1.2):
    # records written without an external_product_seeds row (the audit intake
    # door) need seller identity here or attribution closure stamps
    # seller_ref_missing. seed_kind ∈ {'self','cross'}; NULL = legacy/underivable
    # (never assumed 'self' — ADR-009 D3 no-fallback).
    Column("seller_ref", Text, nullable=True),
    Column("seed_kind", Text, nullable=True),
    Column("canonical_url", Text, nullable=True),
    Column("image_url", Text, nullable=True),
    Column("product_payload", JSONB_TYPE, nullable=True),
    # NOT HERE ON PURPOSE: the two folded-name columns of migration 260 are read only by the
    # PIVOTA-Agent gateway. A column this Table names must exist before the new image serves, and the
    # boot heal's ADD COLUMN can lose its 500 ms lock race to the gateway's multi-second category reads
    # (see the warning at the end of this file); so the migration owns those columns, applied by hand,
    # and no backend code names them (a test pins that).
    Column("freshness_json", JSONB_TYPE, nullable=True),
    # Phase O-1 — free-form merchant-provided tags from StandardProduct.tags[]
    # (Shopify/Wix/WooCommerce sync). JSONB array; NULL on rows predating
    # mig 075. See docs/PDP_ONBOARDING_PLAYBOOK.md.
    Column("tags", JSONB_TYPE, nullable=True),
    # Phase O-2 — Pivota-normalized taxonomy v1 (mig 076). Pure
    # derivation from merchant data + heuristics; NULL when ingest
    # had no signal (Phase O-3 LabelAgent fills the long tail).
    Column("price_tier", String(16), nullable=True),
    Column("use_case_tags", JSONB_TYPE, nullable=True),
    Column("lifestyle_tags", JSONB_TYPE, nullable=True),
    Column("demographic", String(16), nullable=True),
    # Phase 6 — PDP scope dimension (mig 070). Merchant sync writes
    # these fields during ingest, so the SQLAlchemy table metadata must
    # stay aligned with the live schema.
    Column("pdp_scope", String(32), nullable=False, server_default="unverified"),
    Column("pdp_scope_source", String(32), nullable=True),
    Column("pdp_scope_set_at", DateTime(timezone=True), nullable=True),
    # Phase O-4 — onboarding lifecycle stage (mig 077). Computed at
    # ingest by every path; recall (Phase O-5) filters on
    # validated|published. See docs/PDP_ONBOARDING_PLAYBOOK.md.
    Column("pdp_lifecycle_stage", String(16), nullable=True),
    # Pivota canonical PDP — sig_<32hex> identifying the product's
    # agent.pivota.cc/products/<sig> URL (the AI-channel surface).
    # See migration 071 + services/catalog_sync_service.py:
    # make_pivota_signature_id. Nullable for now (rows predating the
    # migration get populated lazily at next sync or first audit).
    Column("pivota_signature_id", Text, nullable=True),
    Column("pivota_canonical_url", Text, nullable=True),
    # Phase C-4 PR-D — when the sig + canonical_url were minted, used
    # to compute the indexing-arc phase (fresh / indexing /
    # expected_steady) in merchant_view.diagnosis. See migration 073
    # + services/pivota_indexing_arc.py. Nullable for now because the
    # backfill sets pre-existing rows to their created_at.
    Column("pivota_signature_minted_at", DateTime(timezone=True), nullable=True),
    # Stage 1 of the PDP architecture roadmap (mig 083). Content-derived
    # product identity: same physical product across merchants/paths
    # produces the same content_key. See services/catalog_identity.py
    # + plans/rosy-mixing-bengio.md. Nullable for rows predating mig
    # 083; backfilled by scripts/backfill_content_key.py. VARCHAR(40), matching
    # migration 083 and agent_pdp_view.content_key — not TEXT.
    Column("content_key", String(40), nullable=True),
    # ADR-011 (mig 178): GS1-canonical GTIN-14 as a MATCH ATTRIBUTE on the
    # canonical identity — NOT folded into content_key. The SPU model
    # (Amazon ASIN / Dewu SPU): content_key is the merchant-agnostic
    # brand+title family key, and GTIN is a strong cross-merchant matcher
    # the resolve-or-attach primitive keys on (services/intake_identity.py)
    # so a product seen with-then-without a barcode converges on ONE
    # identity instead of fragmenting. Nullable; populated by every intake
    # door when the source carried a barcode. Legacy rows stay NULL until a
    # re-ingest / D-2 backfill.
    Column("gtin", Text, nullable=True),
    # P1 (mig 161): claim lifecycle on the SKU —
    # unclaimed | claimed | attested | substantiated. Audit-seed defaults
    # 'unclaimed'; a verified brand claim promotes to 'claimed'. Drives the
    # syndicate-after-claim gate (services/claim_state.py).
    Column("claim_state", String(16), nullable=False, server_default="unclaimed"),
    # Stage 2a (mig 084): Path A sync hygiene. Path A's _upsert_by_pk
    # sets last_seen_in_sync_at=NOW() on every write. NULL on rows from
    # non-sync paths (external_seed mirror, enrichment agent) or rows
    # predating mig 084.
    Column("last_seen_in_sync_at", DateTime(timezone=True), nullable=True),
    # Stage 2a (mig 084): sync lifecycle. 'live' (default) | 'stale' |
    # 'archived'. Sweep flips to stale when last_seen falls behind
    # catalog_merchants.last_full_sync_at by GRACE_HOURS. Recall layer
    # filters on sync_status='live'.
    Column("sync_status", String(16), nullable=False, server_default="live"),
    # Phase O-5b — structured fashion fields (mig 094). The catalog
    # sync path (services/catalog_sync_service.ingest_standard_products)
    # writes these from Shopify metafields via
    # services/fashion_field_payload_extractor.py. Without these column
    # declarations, the SQLAlchemy UPDATE fails with "Unconsumed
    # column names" — surfaced via the 2026-05-18 E2E validation run.
    Column("material", Text, nullable=True),
    Column("material_source", String(32), nullable=True),
    Column("material_confidence", REAL, nullable=True),
    Column("care", Text, nullable=True),
    Column("care_source", String(32), nullable=True),
    Column("care_confidence", REAL, nullable=True),
    Column("size_guide", JSONB_TYPE, nullable=True),
    Column("size_guide_source", String(32), nullable=True),
    Column("size_guide_confidence", REAL, nullable=True),
    # Review signal lifted from a PDP's schema.org aggregateRating (mig 186).
    # The decision-intelligence lane reads exactly these names, and
    # services/agent_pdp_view_assembler.py SELECTs cp.rating_value when it
    # builds agent_pdp_view — a schema built from this model without them
    # fails that build (swallowed as a best-effort warning).
    Column("rating_value", Numeric, nullable=True),
    Column("rating_count", Integer, nullable=True),
    # pdp_will_render / pdp_will_render_computed_at (mig 188) are DELIBERATELY
    # ABSENT, and a drift audit that "helpfully" adds them is reverting a
    # decision, not fixing an oversight. services/pdp_renderability_store.py
    # documents why: the columns are referenced only BY NAME (a
    # sa.literal_column predicate and a raw UPDATE), because adding them here
    # makes every select(catalog_products) in the repo emit them, and a deploy
    # that lands before the database grows the column turns each of those into
    # an UndefinedColumn 500. The safeguard IS their absence from this Table.
    Column("content_changed_at", DateTime, server_default=func.now(), nullable=False),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
    # mig 223. Mirror provenance lookup: the reconciler and sync_offer_for_seed
    # both locate a seed's mirror row by (source_ref, source_system). See the
    # catalog_offers twin below.
    Index(
        "idx_catalog_products_source_ref_system",
        "source_ref",
        "source_system",
        postgresql_where=Column("source_ref").isnot(None),
    ),
    Index(
        "idx_catalog_products_source_identity",
        "merchant_id",
        "platform",
        "source_product_id",
        unique=True,
    ),
    Index(
        "idx_catalog_products_pivota_signature",
        "pivota_signature_id",
        unique=True,
        postgresql_where=Column("pivota_signature_id").isnot(None),
    ),
    # Stage 1 (mig 083): NON-UNIQUE partial index. Stage 1 is the
    # visibility step — we WANT duplicates to be visible so Stage 2
    # can auto-group them. Stage 4 may tighten to UNIQUE on
    # (content_key, merchant_id) once the auto-grouper is stable.
    Index(
        "idx_catalog_products_content_key",
        "content_key",
        postgresql_where=Column("content_key").isnot(None),
    ),
    # ADR-011 (mig 178): the GTIN match-attribute lookup. Partial (most rows
    # are GTIN-less) so the resolve-or-attach primitive's Tier-0 GTIN matcher
    # is an index seek, not a scan.
    Index(
        "idx_catalog_products_gtin",
        "gtin",
        postgresql_where=Column("gtin").isnot(None),
    ),
    # Stage 2a (mig 084): partial index on the non-live tail. Most
    # recall queries filter sync_status='live' (the default); sweep
    # + admin dashboards scan stale/archived rows specifically.
    Index(
        "idx_catalog_products_sync_status_non_live",
        "sync_status",
        "last_seen_in_sync_at",
        postgresql_where=Column("sync_status") != "live",
    ),
)


catalog_skus = Table(
    "catalog_skus",
    metadata,
    Column("sku_key", String(255), primary_key=True),
    Column("product_key", String(255), nullable=False, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("platform", String(64), nullable=False, index=True),
    Column("source_product_id", String(128), nullable=False),
    Column("source_variant_id", String(128), nullable=False),
    Column("source_domain", Text, nullable=True),
    Column("suppression_reason", Text, nullable=True),
    Column("suppressed_at", DateTime(timezone=True), nullable=True),
    Column("suppression_metadata", JSONB_TYPE, nullable=True),
    Column("sku", String(128), nullable=True, index=True),
    Column("barcode", String(128), nullable=True),
    Column("title", Text, nullable=False),
    Column("currency", String(16), nullable=True),
    Column("image_url", Text, nullable=True),
    Column("visible_attributes", JSONB_TYPE, nullable=True),
    Column("visible_option_labels", JSONB_TYPE, nullable=True),
    Column("ingredient_ids", JSONB_TYPE, nullable=True),
    Column("sku_payload", JSONB_TYPE, nullable=True),
    Column("readiness_tier", String(32), nullable=False, server_default="commerce_ready"),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
    Index("idx_catalog_skus_product_key", "product_key"),
    # mig 262 (F2, 2026-10-10): intake identity's Tier-0a matches variant barcodes here
    # (services/intake_identity.barcode_lookup_sql). Partial: most SKUs carry none.
    Index(
        "idx_catalog_skus_barcode",
        "barcode",
        postgresql_where=Column("barcode").isnot(None),
    ),
    Index(
        "idx_catalog_skus_source_identity_v2",
        "merchant_id",
        "platform",
        "product_key",
        "source_variant_id",
        unique=True,
    ),
)


catalog_offers = Table(
    "catalog_offers",
    metadata,
    Column("offer_id", String(255), primary_key=True),
    Column("sku_key", String(255), nullable=False, index=True),
    Column("product_key", String(255), nullable=False, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("catalog_track", String(32), nullable=False, server_default="internal_merchant"),
    Column("truth_tier", String(32), nullable=False, server_default="primary"),
    Column("readiness_tier", String(32), nullable=False, server_default="commerce_ready"),
    Column("offer_mode", String(32), nullable=False, server_default="merchant_checkout"),
    Column("channel", String(64), nullable=False, server_default="default"),
    # Agent-decision-grade offer fields (mig 149). offer_type is
    # brand_direct | retailer | NULL ("unknown"); see services.offer_classification.
    Column("offer_type", String(16), nullable=True),
    Column("market", String(8), nullable=False, server_default="US"),
    Column("is_first_party", Boolean, nullable=False, server_default=expression.false()),
    Column("why_buy_direct", Text, nullable=True),
    Column("availability", String(32), nullable=False, server_default="unknown"),
    Column("inventory_quantity", Integer, nullable=True),
    Column("currency", String(16), nullable=True),
    Column("list_price", Numeric(12, 2), nullable=True),
    Column("merchant_effective_price", Numeric(12, 2), nullable=True),
    Column("estimated_best_price", Numeric(12, 2), nullable=True),
    Column("price_confidence", Numeric(5, 2), nullable=True),
    Column("source_system", String(64), nullable=True),
    Column("source_ref", String(255), nullable=True),
    Column("source_domain", Text, nullable=True),
    Column("offer_payload", JSONB_TYPE, nullable=True),
    Column("suppression_reason", Text, nullable=True),
    Column("suppressed_at", DateTime(timezone=True), nullable=True),
    Column("suppression_metadata", JSONB_TYPE, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
    Index("idx_catalog_offers_merchant_track", "merchant_id", "catalog_track"),
    # mig 223. The external-seed mirror reconciler joins
    # (source_ref, source_system) to find drifted / missing mirror offers.
    # Without this the reconciler's first query was a seq scan and it timed out
    # on prod, so the repair path had never run once. Partial because source_ref
    # is null on every non-mirror row.
    Index(
        "idx_catalog_offers_source_ref_system",
        "source_ref",
        "source_system",
        postgresql_where=Column("source_ref").isnot(None),
    ),
)


# KNOWN MODEL/MIGRATION DISAGREEMENT, deliberately left as-is.
# db/migrations/132 declares `applied_at TIMESTAMPTZ NOT NULL DEFAULT now()`,
# but main.py runs metadata.create_all BEFORE the migrations, so on every
# database the app has ever booted this table was created HERE, naive, and
# 132's CREATE TABLE IF NOT EXISTS has no-opped ever since. The model is what
# prod actually has; the migration is the one that never landed. Changing this
# to timezone=True would make fresh migrations-first databases disagree with
# prod rather than agree with it, so the fix belongs in a decision about which
# of the two owns this table, not in a type swap here. Migration 132 also
# indexes (writer_name, applied_at DESC); the Index below is ASC.
writer_audit_log = Table(
    "writer_audit_log",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("writer_name", Text, nullable=False),
    Column("batch_id", Text, nullable=False),
    Column("dry_run_report_hash", Text, nullable=True),
    Column("applied_rows", Integer, nullable=False, server_default="0"),
    Column("skipped_rows", Integer, nullable=False, server_default="0"),
    Column("reasons", JSONB_TYPE, nullable=True),
    Column("actor", Text, nullable=True),
    Column("applied_at", DateTime, server_default=func.now(), nullable=False),
    Index("idx_writer_audit_writer_time", "writer_name", "applied_at"),
)


catalog_inventory_snapshots = Table(
    "catalog_inventory_snapshots",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("offer_id", String(255), nullable=False, index=True),
    Column("sku_key", String(255), nullable=False, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("inventory_quantity", Integer, nullable=True),
    Column("availability", String(32), nullable=False, server_default="unknown"),
    Column("source_system", String(64), nullable=True),
    Column("source_ref", String(255), nullable=True),
    Column("observed_at", DateTime, server_default=func.now(), nullable=False),
)


catalog_price_snapshots = Table(
    "catalog_price_snapshots",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("offer_id", String(255), nullable=False, index=True),
    Column("sku_key", String(255), nullable=False, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("currency", String(16), nullable=True),
    Column("list_price", Numeric(12, 2), nullable=True),
    Column("merchant_effective_price", Numeric(12, 2), nullable=True),
    Column("estimated_best_price", Numeric(12, 2), nullable=True),
    Column("source_system", String(64), nullable=True),
    Column("source_ref", String(255), nullable=True),
    Column("observed_at", DateTime, server_default=func.now(), nullable=False),
)


catalog_field_facts = Table(
    "catalog_field_facts",
    metadata,
    Column("fact_id", String(255), primary_key=True),
    Column("entity_type", String(32), nullable=False, index=True),
    Column("entity_id", String(255), nullable=False, index=True),
    Column("field_family", String(64), nullable=False),
    Column("field_key", String(128), nullable=False),
    Column("source_system", String(64), nullable=False),
    Column("source_ref", String(255), nullable=True),
    Column("value_json", JSONB_TYPE, nullable=True),
    Column("observed_at", DateTime, nullable=False),
    Column("fresh_until", DateTime, nullable=True),
    Column("confidence", Numeric(5, 2), nullable=True),
    Column("review_state", String(32), nullable=False, server_default="observed"),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
    Index(
        "idx_catalog_field_facts_field",
        "entity_type",
        "entity_id",
        "field_family",
        "field_key",
    ),
)


catalog_sync_events = Table(
    "catalog_sync_events",
    metadata,
    Column("event_id", String(255), primary_key=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("connector", String(64), nullable=False, index=True),
    Column("event_type", String(64), nullable=False),
    Column("topic", String(128), nullable=True),
    Column("status", String(32), nullable=False, server_default="pending"),
    Column("payload_json", JSONB_TYPE, nullable=True),
    Column("source_ref", String(255), nullable=True),
    Column("error_message", Text, nullable=True),
    Column("occurred_at", DateTime, nullable=True),
    Column("processed_at", DateTime, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
    Index("idx_catalog_sync_events_pending", "merchant_id", "connector", "status"),
)


catalog_sync_jobs = Table(
    "catalog_sync_jobs",
    metadata,
    Column("job_id", String(255), primary_key=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("connector", String(64), nullable=False, index=True),
    Column("mode", String(64), nullable=False),
    Column("scope_json", JSONB_TYPE, nullable=True),
    Column("status", String(32), nullable=False, server_default="pending"),
    Column("requested_by", String(128), nullable=True),
    Column("stats_json", JSONB_TYPE, nullable=True),
    Column("error_message", Text, nullable=True),
    Column("started_at", DateTime, nullable=True),
    Column("completed_at", DateTime, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
    Index("idx_catalog_sync_jobs_status", "merchant_id", "connector", "status"),
)



catalog_payment_incentives = Table(
    "catalog_payment_incentives",
    metadata,
    Column("incentive_id", String(255), primary_key=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("incentive_type", String(64), nullable=False),
    Column("funding_source", String(64), nullable=True),
    Column("payment_method_type", String(64), nullable=True),
    Column("card_network", String(64), nullable=True),
    Column("issuer_name", String(128), nullable=True),
    Column("wallet_type", String(64), nullable=True),
    Column("installment_provider", String(64), nullable=True),
    Column("label", String(255), nullable=False),
    Column("benefit_kind", String(64), nullable=False),
    Column("benefit_value", Numeric(12, 2), nullable=True),
    Column("benefit_currency", String(16), nullable=True),
    Column("market", String(16), nullable=True),
    Column("eligibility_confidence", Numeric(5, 2), nullable=True),
    Column("source_system", String(64), nullable=True),
    Column("source_ref", String(255), nullable=True),
    Column("status", String(32), nullable=False, server_default="active"),
    Column("starts_at", DateTime, nullable=True),
    Column("ends_at", DateTime, nullable=True),
    Column("metadata_json", JSONB_TYPE, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
)


catalog_incentive_rules = Table(
    "catalog_incentive_rules",
    metadata,
    Column("rule_id", String(255), primary_key=True),
    Column("incentive_id", String(255), nullable=False, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("rule_type", String(64), nullable=False),
    Column("scope_json", JSONB_TYPE, nullable=True),
    Column("conditions_json", JSONB_TYPE, nullable=True),
    Column("schedule_json", JSONB_TYPE, nullable=True),
    Column("human_rule", Text, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
)


catalog_offer_incentive_links = Table(
    "catalog_offer_incentive_links",
    metadata,
    Column("link_id", String(255), primary_key=True),
    Column("offer_id", String(255), nullable=False, index=True),
    Column("incentive_id", String(255), nullable=False, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("relationship_type", String(64), nullable=False, server_default="eligible"),
    Column("priority", Integer, nullable=False, server_default="0"),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
)


catalog_quote_snapshots = Table(
    "catalog_quote_snapshots",
    metadata,
    Column("quote_snapshot_id", String(255), primary_key=True),
    Column("quote_id", String(255), nullable=True, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("offer_id", String(255), nullable=True, index=True),
    Column("sku_key", String(255), nullable=True, index=True),
    Column("product_key", String(255), nullable=True, index=True),
    Column("currency", String(16), nullable=True),
    Column("list_price", Numeric(12, 2), nullable=True),
    Column("merchant_effective_price", Numeric(12, 2), nullable=True),
    Column("estimated_best_price", Numeric(12, 2), nullable=True),
    Column("exact_quote_price", Numeric(12, 2), nullable=True),
    Column("incentives_json", JSONB_TYPE, nullable=True),
    Column("quote_payload_json", JSONB_TYPE, nullable=True),
    Column("expires_at", DateTime, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
)


checkout_preflight_observations = Table(
    "checkout_preflight_observations",
    metadata,
    Column("observation_id", String(64), primary_key=True),
    # timezone=True, matching TIMESTAMPTZ in db/migrations/219. They must agree, and the MODEL
    # is what decides: create_all runs BEFORE migrations, so it creates the table and the
    # migration's CREATE TABLE IF NOT EXISTS then skips — a bare DateTime here would silently
    # give production a naive column while the migration file claimed otherwise. Caught only by
    # running the dialect gate in order, where another file's create_all builds this table
    # first and a tz-aware bind then fails with "can't subtract offset-naive and offset-aware".
    Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    # WHERE the row came from, and WHICH run wrote it. Both live in the MODEL and not only in
    # db/migrations/220, because `web` deploys with SKIP_HEAVY_STARTUP_INIT and never runs the
    # migration directory at all — `metadata.create_all` above is what actually builds this table
    # on production, exactly as the `created_at` note explains. A column added to the migration
    # alone would not exist in prod, and `record()` swallows its own failures, so shadow mode
    # would stop recording with nothing but a dropped log line to show for it.
    Column("source", String(32), nullable=False, server_default="live"),
    # Nullable and untagged for live traffic; a sweep stamps every row of one pass with the same
    # value so a run that aborted part-way can be excluded from the window instead of poisoning
    # it. Without it a partial sweep is indistinguishable from a good one.
    Column("run_id", String(64), nullable=True, index=True),
    Column("mode", String(16), nullable=False),
    Column("outcome", String(16), nullable=False),
    # The decision the ENFORCING gate WOULD have made, recorded while it is not enforcing.
    # `outcome` only ever says "allow" in shadow, so this is the column the refusal rate is
    # measured from. See db/migrations/219_checkout_preflight_observations.sql.
    Column("would_block", Boolean, nullable=False),
    Column("reason", String(64), nullable=False),
    Column("merchant_id", String(64), nullable=True, index=True),
    Column("product_key", String(255), nullable=True),
    Column("sku_key", String(255), nullable=True, index=True),
    Column("offer_id", String(255), nullable=True),
    Column("live_status", String(32), nullable=True),
    Column("in_stock", Boolean, nullable=True),
    # Kept separate from `live_status` deliberately, exactly as live_offer_verification.Verdict
    # does: a check can establish STOCK without establishing PRICE, and folding the two into one
    # flag is what produced the yen-as-dollars bug there.
    Column("price_verified", Boolean, nullable=False, server_default=expression.false()),
    Column("quoted_price", Numeric(12, 2), nullable=True),
    Column("quoted_currency", String(16), nullable=True),
    Column("live_price", Numeric(12, 2), nullable=True),
    Column("live_currency", String(16), nullable=True),
    Column("latency_ms", Integer, nullable=True),
    Column("detail", JSONB_TYPE, nullable=True),
)


beauty_product_profiles = Table(
    "beauty_product_profiles",
    metadata,
    Column("product_key", String(255), primary_key=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("taxonomy_json", JSONB_TYPE, nullable=True),
    Column("concerns_json", JSONB_TYPE, nullable=True),
    Column("claims_json", JSONB_TYPE, nullable=True),
    Column("routine_phase", String(64), nullable=True),
    Column("benefits_json", JSONB_TYPE, nullable=True),
    Column("profile_payload", JSONB_TYPE, nullable=True),
    # Provenance-backed evidence + required disclaimers (mig 150). evidence_profile
    # holds {claims:[ProductClaim], review_state}; see services.claim_safety.
    Column("evidence_profile", JSONB_TYPE, nullable=True),
    Column("required_disclaimers", JSONB_TYPE, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
)


beauty_sku_ingredients = Table(
    "beauty_sku_ingredients",
    metadata,
    Column("sku_key", String(255), primary_key=True),
    Column("product_key", String(255), nullable=False, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("raw_inci", Text, nullable=True),
    Column("normalized_ingredients_json", JSONB_TYPE, nullable=True),
    Column("active_ingredients_json", JSONB_TYPE, nullable=True),
    Column("concentration_notes_json", JSONB_TYPE, nullable=True),
    Column("allergen_flags_json", JSONB_TYPE, nullable=True),
    Column("evidence_refs_json", JSONB_TYPE, nullable=True),
    Column("source_system", String(64), nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
)


beauty_usage_guides = Table(
    "beauty_usage_guides",
    metadata,
    Column("guide_id", String(255), primary_key=True),
    Column("product_key", String(255), nullable=False, index=True),
    Column("sku_key", String(255), nullable=True, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("how_to_use_text", Text, nullable=True),
    Column("steps_json", JSONB_TYPE, nullable=True),
    Column("frequency", String(64), nullable=True),
    Column("time_of_day", String(32), nullable=True),
    Column("application_order", Integer, nullable=True),
    Column("warnings_json", JSONB_TYPE, nullable=True),
    Column("evidence_refs_json", JSONB_TYPE, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
)


beauty_shades = Table(
    "beauty_shades",
    metadata,
    Column("shade_id", String(255), primary_key=True),
    Column("sku_key", String(255), nullable=False, index=True),
    Column("product_key", String(255), nullable=False, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("shade_code", String(128), nullable=True),
    Column("shade_name", String(255), nullable=False),
    Column("shade_family", String(128), nullable=True),
    Column("undertone", String(128), nullable=True),
    Column("finish", String(128), nullable=True),
    Column("swatch_refs_json", JSONB_TYPE, nullable=True),
    Column("media_refs_json", JSONB_TYPE, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
)


beauty_compatibility_rules = Table(
    "beauty_compatibility_rules",
    metadata,
    Column("compatibility_rule_id", String(255), primary_key=True),
    Column("product_key", String(255), nullable=True, index=True),
    Column("sku_key", String(255), nullable=True, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("rule_type", String(64), nullable=False),
    Column("subject_ingredients_json", JSONB_TYPE, nullable=True),
    Column("related_ingredients_json", JSONB_TYPE, nullable=True),
    Column("verdict", String(64), nullable=False),
    Column("rationale", Text, nullable=True),
    Column("evidence_refs_json", JSONB_TYPE, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
)


beauty_content_assets = Table(
    "beauty_content_assets",
    metadata,
    Column("asset_id", String(255), primary_key=True),
    Column("product_key", String(255), nullable=False, index=True),
    Column("sku_key", String(255), nullable=True, index=True),
    Column("merchant_id", String(64), nullable=False, index=True),
    Column("asset_type", String(64), nullable=False),
    Column("title", String(255), nullable=True),
    Column("url", Text, nullable=False),
    Column("thumbnail_url", Text, nullable=True),
    Column("sort_order", Integer, nullable=False, server_default="0"),
    Column("metadata_json", JSONB_TYPE, nullable=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
    Column("updated_at", DateTime, server_default=func.now(), nullable=False),
)


# Stage 3a (mig 085) — denormalized one-row-per-canonical-product view
# that powers /api/agent/pdp/{id}. Read path target: <10ms p99 SELECT.
# Backfilled by Stage 3a-ii script; refreshed by Stage 3a-iii hook in
# seed_data_writer; read by Stage 3a-iv endpoint.
# See plans/rosy-mixing-bengio.md.
agent_pdp_view = Table(
    "agent_pdp_view",
    metadata,
    Column("content_key", String(40), primary_key=True),
    Column("pivota_signature_id", String(40), nullable=True),
    Column("product_group_id", String(64), nullable=True),
    Column("brand", Text, nullable=True),
    Column("title", Text, nullable=False),
    Column("description", Text, nullable=True),
    # Brand-attested rich content (E2 bridge from product_enrichment): key
    # selling points + how-to-use, served to agents alongside title/description.
    Column("bullet_points", JSONB_TYPE, nullable=True),
    Column("usage_scenarios", JSONB_TYPE, nullable=True),
    Column("image_url", Text, nullable=True),
    Column("image_urls", JSONB_TYPE, nullable=True),
    Column("currency", String(3), nullable=True),
    Column("price_min", Numeric(12, 2), nullable=True),
    Column("price_max", Numeric(12, 2), nullable=True),
    Column("offer_count", Integer, nullable=True),
    Column("offers", JSONB_TYPE, nullable=True),
    Column("variants", JSONB_TYPE, nullable=True),
    Column("variants_count", Integer, nullable=True),
    Column("gtin13", String(14), nullable=True),
    Column("category_path", Text, nullable=True),
    Column("taxonomy_tags", JSONB_TYPE, nullable=True),
    Column("breadcrumb", JSONB_TYPE, nullable=True),
    Column("pdp_lifecycle_stage", String(16), nullable=True),
    Column("sync_status", String(16), nullable=True),
    Column("primary_merchant_id", String(64), nullable=True),
    # Phase O-5b cross-PDP coalesce: material/care/size_guide aggregated
    # from all product_group_members + matched external_product_seeds,
    # picked by source-priority ordering in services/agent_pdp_view_assembler.
    # Source enum mirrors catalog_products: merchant_payload >
    # merchant_authored > llm_extraction_v1 > external_seed.
    Column("material", Text, nullable=True),
    Column("material_source", String(32), nullable=True),
    Column("material_confidence", REAL, nullable=True),
    Column("care", Text, nullable=True),
    Column("care_source", String(32), nullable=True),
    Column("care_confidence", REAL, nullable=True),
    Column("size_guide", JSONB_TYPE, nullable=True),
    Column("size_guide_source", String(32), nullable=True),
    Column("size_guide_confidence", REAL, nullable=True),
    # Provenance-backed evidence + disclaimers mirrored from
    # beauty_product_profiles (mig 152).
    Column("evidence_profile", JSONB_TYPE, nullable=True),
    Column("required_disclaimers", JSONB_TYPE, nullable=True),
    # Review signal mirrored from catalog_products for the serve path (mig 186).
    Column("rating_value", Numeric, nullable=True),
    Column("rating_count", Integer, nullable=True),
    # Per-currency price summary (mig 263): {"version": 1, "currencies": {"USD":
    # {price_min, price_max, offer_count, offers}, ...}}, valid only while
    # market_prices_refreshed_at = refreshed_at (the write that produced it).
    Column("market_prices", JSONB_TYPE, nullable=True),
    Column("market_prices_refreshed_at", DateTime(timezone=True), nullable=True),
    Column(
        "refreshed_at", DateTime(timezone=True), server_default=func.now(), nullable=False
    ),
    Column("refreshed_by_proposal_id", BigInteger, nullable=True),
    Column("refresh_source", Text, nullable=True),
    Index(
        "idx_agent_pdp_view_pivota_signature_id",
        "pivota_signature_id",
        unique=True,
        postgresql_where=Column("pivota_signature_id").isnot(None),
    ),
    Index(
        "idx_agent_pdp_view_product_group_id",
        "product_group_id",
        postgresql_where=Column("product_group_id").isnot(None),
    ),
    Index(
        "idx_agent_pdp_view_gtin13",
        "gtin13",
        postgresql_where=Column("gtin13").isnot(None),
    ),
    Index(
        "idx_agent_pdp_view_brand",
        "brand",
        postgresql_where=Column("brand").isnot(None),
    ),
)

# --- category_taxonomy: ONE vocabulary, read by BOTH services ----------------------------------
#
# WHY A TABLE AND NOT A CONSTANT. `category_path` is written and read by two repositories over one
# database — pivota-backend's CATEGORY_PATTERNS and PIVOTA-Agent's src/services/beautyTaxonomy.js.
# Each held its own vocabulary, neither imported the other, and on 2026-09-10 they disagreed about
# where a toner lives: the gateway wrote 315 rows to `beauty/skincare/tone/toner` on purpose while
# this repo's taxonomy named `beauty/skincare/treat/toner` and could not reach any of them. This
# repo's own invariant then counted those rows as corruption. Two vocabularies over one column
# means every disagreement presents as data corruption to whichever side is reading.
#
# The rows are the shared place. Both services load and cache this table; nobody has to vendor a
# copy of the other's file, and `serving_eligible_off_taxonomy_path` can join against it directly
# instead of against one repo's opinion.
#
# SHAPE. Three kinds of row, distinguished without a type column:
#   canonical leaf   alias_of IS NULL, is_leaf = true    a path recall builds a prefix for
#   interior node    alias_of IS NULL, is_leaf = false    an ancestor; reachable only via #2122
#   alias            alias_of IS NOT NULL                 a spelling that means another path
#
# The self-FK is what stops an alias pointing at nothing, which is exactly how the 117 orphan paths
# came to exist. `alias_of` on a row whose target is itself an alias is refused by the CHECK below:
# one hop only, so resolution cannot loop or need a recursive query on a hot path.
#
# ⚠️ IN THE MODEL, not only in db/migrations/221 — `web` deploys with SKIP_HEAVY_STARTUP_INIT and
# never runs the migration directory, so `metadata.create_all` is what actually builds this on
# production. See the note on checkout_preflight_observations.created_at above.
category_taxonomy = Table(
    "category_taxonomy",
    metadata,
    Column("path", String(255), primary_key=True),
    Column("label", String(128), nullable=False),
    Column("is_leaf", Boolean, nullable=False, server_default=text("false")),
    # Self-referential: the canonical path this spelling resolves to. NULL means "this IS canonical".
    Column("alias_of", String(255), ForeignKey("category_taxonomy.path"), nullable=True),
    # Free text, because the reason a path was chosen is the part that gets lost. The toner row
    # carries the Google/Shopify citation; without it the next person re-litigates it.
    Column("note", Text, nullable=True),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    CheckConstraint(
        "alias_of IS NULL OR is_leaf = false",
        name="ck_category_taxonomy_alias_is_not_a_leaf",
    ),
    CheckConstraint(
        "alias_of IS NULL OR alias_of <> path",
        name="ck_category_taxonomy_alias_not_self",
    ),
    extend_existing=True,
)
