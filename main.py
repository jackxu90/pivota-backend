# main.py
"""
Pivota Infrastructure Main Application
FastAPI application with comprehensive dashboard and real-time metrics
"""

import asyncio
import hmac
import logging
import time
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from functools import lru_cache
import uvicorn
from services.merchant_store_service import get_merchant_active_stores, get_primary_store
from typing import Any, Dict, Optional

from fastapi import (
    FastAPI, BackgroundTasks, Depends, HTTPException, WebSocket, WebSocketDisconnect,
    Request, Response, status,
)
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from middleware.cors import TieredCORSMiddleware, resolve_trusted_origins
from middleware.rate_limiter import RateLimitMiddleware
from middleware.usage_logger import UsageLoggerMiddleware
from middleware.structured_logging import (
    StructuredLoggingMiddleware,
    install_uvicorn_access_log_redaction,
)
from middleware.error_handler import ErrorHandlerMiddleware
from middleware.ap2_security import AP2SecurityMiddleware
from middleware.security_headers import SecurityHeadersMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

# Database
from db.database import database, metadata, engine
from db.startup_ddl import StartupDdlLock, startup_ddl_lock
import db.agents  # noqa: F401  (register agents in metadata; startup() relies on create_all building it)
import db.auth_identity  # noqa: F401  (register canonical auth identity tables in metadata)
import db.pcs_tables  # noqa: F401  (register PCS v0.1 tables/constraints in metadata)
import db.id_bridge  # noqa: F401  (register id_bridge table in metadata)
import db.canonical_commerce  # noqa: F401  (register canonical commerce tables in metadata)
import db.commerce_interactions  # noqa: F401  (register canonical interaction ledger tables in metadata)
import db.merchant_collector_tokens  # noqa: F401  (register collector token registry tables in metadata)
import db.commerce_attribution  # noqa: F401  (register commerce attribution tables in metadata)
import db.agent_share  # noqa: F401  (register agent share rate + ledger tables in metadata)
import db.agent_oauth_clients  # noqa: F401  (register the provisioned OAuth client -> agent table in metadata)
import db.gmv_invoice_credits  # noqa: F401  (register the GMV invoice credit table in metadata)
import db.merchant_commerce_readiness  # noqa: F401  (register merchant commerce readiness state in metadata)
import db.surface_listing_registry  # noqa: F401  (register surface listing registry tables in metadata)
try:
    import db.merchant_portal_preferences  # noqa: F401  (register merchant portal preferences table in metadata)
except ModuleNotFoundError:
    logging.getLogger(__name__).warning(
        "db.merchant_portal_preferences is unavailable; continuing without merchant portal preferences metadata"
    )
import subprocess
import os
import secrets
from pathlib import Path
from utils.database_readiness import (
    DatabaseUnavailableError,
    connect_database_with_timeout,
    probe_database_health,
    run_database_reconnect_supervisor,
)

SERVICE_STARTED_AT = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
SERVICE_NAME = (os.getenv("PIVOTA_SERVICE_NAME") or os.getenv("SERVICE_NAME") or "pivota-backend").strip() or "pivota-backend"



def _guard_single_order_routes_py() -> None:
    """
    Governance guardrail: avoid duplicate `order_routes.py` files.

    Multiple copies across the repo create "shadowing" risk and lead to fixing
    the wrong implementation. This runs once at process startup and fails fast
    on deploy if a second copy is added.
    """
    repo_root = Path(__file__).resolve().parent
    # `.claude` holds harness-managed worktrees (full repo copies) on dev machines only —
    # scanning them makes every copy of order_routes.py look like a duplicate.
    ignored_dirs = {".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache", ".claude"}
    matches: list[str] = []

    for dirpath, dirnames, filenames in os.walk(repo_root):
        dirnames[:] = [d for d in dirnames if d not in ignored_dirs]
        if "order_routes.py" in filenames:
            matches.append(Path(dirpath, "order_routes.py").relative_to(repo_root).as_posix())

    matches.sort()
    expected = ["routes/order_routes.py"]
    if matches != expected:
        raise RuntimeError(
            "Repo governance violation: expected a single `order_routes.py` at "
            "`routes/order_routes.py`, found:\n- " + "\n- ".join(matches)
        )

# Core routers (only include what exists)
_guard_single_order_routes_py()

# AT IMPORT, before a single request can be served. `uvicorn.access` writes the
# raw request line — `get_path_with_query_string(scope)` — at INFO on every
# response, 200s and 401s alike, and infra/gcp/Dockerfile starts uvicorn with
# neither `--no-access-log` nor a `--log-config`, so on Cloud Run that line goes
# straight to Cloud Logging. `POST /webhooks/webflow/{store_id}/{url_secret}`
# authenticates with a secret IN THE PATH (Webflow does not sign a site-token
# webhook), so without this the credential is in the access line of every
# delivery.
#
# It is installed here rather than only in the startup hook because uvicorn
# imports `main:app` after it has configured logging and before it serves
# anything: an import-time install is in place for the first request under
# `uvicorn main:app`, under `uvicorn.run` below, and under `--reload`. The
# lifespan calls it again, idempotently, to cover a deployment whose own
# `--log-config` reconfigures logging in between.
#
# It cannot cover the LOAD BALANCER's `httpRequest.requestUrl`, which the
# platform records regardless of this process — that surface is the argument for
# configuring `WEBFLOW_CLIENT_SECRET`, and it is the ONLY one left.
install_uvicorn_access_log_redaction()

from routes.agent_routes import router as agent_router
from routes.agent_briefs import router as agent_briefs_router
from routes.quote_routes import router as quote_router
from routes.offer_routes import router as offer_router
from routes.ops_routes import router as ops_router
from routes.psp_routes import router as psp_router
from routes.payment_routes import router as payment_router
from routes.agent_checkout_intents import router as agent_checkout_intents_router
from routes.employee_settlement_rules import employee_router as employee_settlement_router

# Dashboard routers
from routes.dashboard_api import router as dashboard_api_router
# payment_routes already imported above, removed duplicate
from routes.demo_data_routes import router as demo_data_router
from routes.auth_routes import router as auth_router
from routes.auth import router as auth_api_router  # API auth endpoints
from routes.mcp_oauth_as import router as mcp_oauth_as_router  # MCP OAuth Authorization Server (flag-gated)
from routes.crawl_identity import router as crawl_identity_router  # Web Bot Auth key directory (404 until keyed)
from routes.agent_account import router as agent_account_router  # Agent account management
from routes.agent_commerce import router as agent_commerce_router
from routes.agent_commerce_reap import router as agent_commerce_reap_router
from routes.agent_commerce_purchases import router as agent_commerce_purchases_router
from routes.reap_return import router as reap_return_router
from routes.admin_api import router as admin_api_router
from routes.admin_partner_cohort import router as admin_partner_cohort_router
from routes.admin_partner_comms import router as admin_partner_comms_router
from routes.admin_partner_invite_tokens import router as admin_partner_invite_tokens_router
from routes.admin_partner_settlements import router as admin_partner_settlements_router
from routes.admin_partner_subsidies import router as admin_partner_subsidies_router
from routes.admin_partners import router as admin_partners_router
from routes.admin_billing_statements import router as admin_billing_statements_router
from routes.merchant_routes import router as merchant_router
from routes.merchant_onboarding_routes import router as merchant_onboarding_router
from routes.platform_onboarding_routes import router as platform_onboarding_router
from routes.merchant_dashboard_routes import router as merchant_dashboard_router  # Original with fallback - STABLE
from routes.admin_catalog_debug import router as admin_catalog_debug_router
from routes.admin_audit_backfill import router as admin_audit_backfill_router
from routes.admin_signal_scorecard import router as admin_signal_scorecard_router
from routes.admin_outcomes import router as admin_outcomes_router
from routes.merchant_analytics_routes import router as merchant_analytics_router
from routes.citation_operator_routes import router as citation_operator_router
from routes.pivot_routes import router as pivot_router
from routes.catalog_routes import router as catalog_router
from routes.merchant_api_extensions import router as merchant_api_extensions_router
from routes.direct_db_check import router as direct_db_check_router
from routes.cleanup_test_data import router as cleanup_test_data_router
from routes.manage_integrations import router as manage_integrations_router
from routes.psp_metrics import router as psp_metrics_router
from routes.wix_sync import router as wix_sync_router
from routes.fix_duplicate_stores import router as fix_duplicate_stores_router
from routes.admin_cleanup import router as admin_cleanup_router
from routes.init_orders_table import router as init_orders_router
from routes.employee_dashboard_routes import router as employee_dashboard_router
from routes.employees_security import router as employees_security_router
from routes.mcp_mgmt import router as mcp_mgmt_router
from routes.employee_missing_endpoints import router as employee_missing_router
from routes.agent_sdk_fixed import router as agent_sdk_fixed_router
from routes.employee_store_psp_fixes import router as emp_store_psp_router
from routes.fix_agents_table import router as fix_agents_router
from routes.admin_migrations import router as admin_migrations_router
from routes.admin_apply_agent_center_migration import (
    router as admin_apply_agent_center_migration_router,
)
from routes.admin_apply_citation_operator import (
    router as admin_apply_citation_operator_router,
)
from routes.agent_center_demand_test_routes import (
    router as agent_center_demand_test_router,
)
from routes.agent_center_sku_match_routes import (
    router as agent_center_sku_match_router,
)
from routes.agent_center_admin_routes import (
    router as agent_center_admin_router,
)
from routes.agent_center_bd_routes import (
    router as agent_center_bd_router,
)
from routes.channel_graph_routes import (
    router as channel_graph_router,
)
from routes.merchant_audit_routes import (
    public_share_router as audit_public_share_router,
    router as merchant_audit_router,
)
from routes.funnel_metrics_routes import (
    router as funnel_metrics_router,
)
from routes.brand_claim_routes import (
    router as brand_claim_router,
)
from routes.substantiation_admin_routes import (
    router as substantiation_admin_router,
)
from routes.audit_runs_routes import (
    router as audit_runs_router,
)
from routes.gsc_oauth_routes import (
    router as gsc_oauth_router,
)
from routes.pivota_canonical_routes import (
    router as pivota_canonical_router,
)
from routes.agent_pdp_v1 import router as agent_pdp_v1_router
from routes.agent_citation_v1 import router as agent_citation_v1_router
from routes.agent_payment_sdk import router as agent_payment_router
from routes.agent_products import router as agent_products_router
from routes.psp_overview_routes import router as psp_overview_router
from routes.admin_fix_merchant import router as admin_fix_router
from routes.simple_test_orders import router as simple_test_orders_router
from routes.migrate_employees_password import router as migrate_employees_password_router
from routes.debug_product_sync import router as debug_product_sync_router
from routes.debug_shopify_api import router as debug_shopify_api_router
from routes.merchant_store_connections import router as merchant_store_connections_router
from routes.ops_shopify_integration_routes import router as ops_shopify_integration_router
from routes.ops_pcs_reducer_routes import router as ops_pcs_reducer_router
from routes.merchant_onboarding_shopify_verify_routes import router as merchant_onboarding_shopify_verify_router
from routes.admin_simple_fix import router as admin_simple_fix_router
from routes.admin_cleanup_rebuild import router as admin_cleanup_rebuild_router
from routes.admin_cleanup_stores import router as admin_cleanup_stores_router
from routes.admin_sql_quick import router as admin_sql_router
from routes.admin_agents_debug import router as admin_agents_debug_router
from routes.agent_health import router as agent_health_router
from routes.photos import router as photos_router, start_photo_cleanup_loop
from routes.admin_usage_debug import router as admin_usage_debug_router
from routes.agent_analytics import router as agent_analytics_router
# Debug routers - only import if DEBUG_MODE is enabled
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

if DEBUG_MODE:
    from routes.agent_debug import router as agent_debug_router
    from routes.admin_debug_products import router as debug_products_router
    from routes.admin_populate_products import router as test_populate_router
from routes.shopify_routes import router as shopify_router
from routes.payment_execution_routes import router as payment_execution_router
from routes.billing_routes import router as billing_router
from routes.partner_invite_redeem import router as partner_invite_redeem_router
from routes.product_routes import router as product_router
from routes.product_routes_v2 import router as product_router_v2
from routes.product_sync import router as product_sync_router
from routes.universal_product_sync import router as universal_sync_router
from routes.sync_all_platforms import router as sync_all_router
from routes.product_sync_monitoring import router as product_monitoring_router
from routes.admin_reset_employee import router as admin_reset_employee_router
from routes.admin_cleanup_duplicates import router as admin_cleanup_duplicates_router
from routes.admin_data_consistency import router as admin_data_consistency_router
from routes.admin_merchant_canonicalize import router as admin_merchant_canonicalize_router
from routes.admin_merchant_reset import router as admin_merchant_reset_router
from routes.admin_shopify_health import router as admin_shopify_health_router
from routes.admin_amazon_oauth import router as admin_amazon_oauth_router
from routes.admin_amazon_orders import router as admin_amazon_orders_router
from routes.admin_amazon_fulfillment import router as admin_amazon_fulfillment_router
from routes.products_cache_maintenance import router as products_cache_maintenance_router
from routes.mcp_e2e_test import router as mcp_e2e_test_router
from routes.platform_connector_prefix import (
    LEGACY_MCP_PREFIX,
    PLATFORM_CONNECTORS_PREFIX,
    legacy_mcp_prefix_deprecation,
)
from routes.admin_cleanup_all_test_data import router as admin_cleanup_all_router
from routes.admin_debug_shopify_token import router as admin_debug_shopify_token_router
from routes.employee_agent_mgmt import router as employee_agent_mgmt_router
from routes.employee_products import router as employee_products_router
from routes.employee_pdp_governance import router as employee_pdp_governance_router
from routes.employee_content import router as employee_content_router
from routes.employee_kb_monitoring import router as employee_kb_monitoring_router
from routes.admin_psp_integrity import router as admin_psp_integrity_router
from routes.admin_orphan_orders import router as admin_orphan_orders_router
from routes.admin_run_migration import router as admin_run_migration_router
from routes.admin_run_migration_081 import router as admin_run_migration_081_router
from routes.admin_run_migration_082 import router as admin_run_migration_082_router
from routes.admin_run_migration_083 import router as admin_run_migration_083_router
from routes.admin_run_migration_084 import router as admin_run_migration_084_router
from routes.admin_run_migration_085 import router as admin_run_migration_085_router
from routes.admin_run_migration_089 import router as admin_run_migration_089_router
from routes.admin_run_migration_098 import router as admin_run_migration_098_router
from routes.admin_run_migration_099 import router as admin_run_migration_099_router
from routes.admin_sync_refresh_presence import router as admin_sync_refresh_presence_router
from routes.admin_fix_agents import router as admin_fix_agents_router
from routes.admin_fix_agent_metrics import router as admin_fix_agent_metrics_router
from routes.admin_fix_agent_metrics_v2 import router as admin_fix_agent_metrics_v2_router
from routes.debug_mcp_data import router as debug_mcp_data_router
from routes.admin_run_migration_008 import router as admin_run_migration_008_router
from routes.admin_governance import router as admin_governance_router
from routes.admin_run_migration_009 import router as admin_run_migration_009_router
from routes.admin_seed_test_data import router as admin_seed_test_data_router
# Phase 4 imports
from routes.payment_routing_routes import router as payment_routing_router
from routes.payment_routing_routes import employee_router as employee_payment_routing_router
from routes.protocol_routes import router as protocol_router
from routes.protocol_routes import agent_router as agent_protocol_router
from routes.protocol_routes import employee_router as employee_protocol_router
from routes.employee_routing_dashboard import router as employee_routing_dashboard_router
from routes.admin_run_migration_010 import router as admin_run_migration_010_router
from routes.admin_fix_agent_protocols import router as admin_fix_agent_protocols_router
from routes.agent_protocol_test import router as agent_protocol_test_router
# [Phase 4++] Dual-routing imports
from routes.routing_governance import router as routing_governance_router
from routes.admin_run_migration_011 import router as admin_run_migration_011_router
from routes.admin_creators import router as admin_creators_router
from routes.admin_run_migration_152 import router as admin_run_migration_152_router
from routes.admin_run_migration_164 import router as admin_run_migration_164_router
from routes.admin_run_migration_165 import router as admin_run_migration_165_router
from routes.admin_rescore_external_seed_quality import router as admin_rescore_external_seed_quality_router
from routes.admin_run_migration_pending import (
    router as admin_run_migration_pending_router,
)
from routes.admin_seed_routing_logs import router as admin_seed_routing_logs_router
from routes.admin_cleanup_routing_test_data import router as admin_cleanup_routing_router
from routes.admin_seed_agent_routing_history import router as admin_seed_agent_history_router
# [Phase 5] Agent routing control and revenue
from routes.agent_routing_api import router as agent_routing_api_router
from routes.agent_revenue_api import router as agent_revenue_api_router
from routes.admin_run_migration_012 import router as admin_run_migration_012_router
from routes.admin_run_migration_013 import router as admin_run_migration_013_router
# [Phase 5.5] Dual-sided revenue
from routes.merchant_commission_api import router as merchant_commission_api_router
# [Phase 5.6] Agent Portal settlement, protocol, integration
from routes.agent_settlement_routes import router as agent_settlement_router
from routes.merchant_payouts import router as merchant_payouts_router  # Merchant payout management
from routes.agent_integration_status import router as agent_integration_router
from routes.agent_payouts import router as agent_payouts_router  # Agent payout view
from routes.admin_payout_backfill import router as admin_payout_backfill_router  # Admin backfill for payouts/commissions
from routes.agent_bank import router as agent_bank_router  # Agent bank info
from routes.employee_finance import router as employee_finance_router  # Employee finance dashboard
from routes.employee_payouts import router as employee_payouts_router  # Employee payout actions
from routes.merchant_agent_bank import router as merchant_agent_bank_router  # Merchant view agent bank info
from routes.order_routes import router as order_router
from routes.webhook_routes import router as webhook_router
from routes.agent_api import router as agent_api_router
try:
    from routes.agent_v2 import router as agent_v2_router
except ModuleNotFoundError:
    agent_v2_router = None
    logging.getLogger(__name__).warning(
        "routes.agent_v2 is unavailable; skipping agent_v2 router registration"
    )
from routes.after_sales_cases import router as after_sales_cases_router
from routes.agent_events import router as agent_events_router
from routes.card_rail_outcomes import router as card_rail_outcomes_router
from routes.agent_cards import router as agent_cards_router
from routes.reap_webhooks import router as reap_webhooks_router
from routes.agent_management import router as agent_management_router
from routes.shopify_setup import router as shopify_setup_router
from routes.shopify_manual import router as shopify_manual_router
from routes.fulfillment_api import router as fulfillment_api_router
from routes.refund_api import router as refund_api_router
from routes.agent_docs import router as agent_docs_router
from routes.agent_recommendations import router as agent_recommendations_router
from routes.fix_orders_table import router as fix_orders_table_router
from routes.agent_metrics import router as agent_metrics_router
from routes.agent_keys import router as agent_keys_router
from routes.agent_identity_issuers import router as agent_identity_issuers_router, internal_router as agent_identity_issuers_internal_router
from routes.payment_grant_issuers import router as payment_grant_issuers_router, internal_router as payment_grant_issuers_internal_router
from routes.agent_webhooks import router as agent_webhooks_router
from routes.init_agent_key import router as init_agent_key_router
from routes.merchant_products import router as merchant_products_router
from routes.merchant_citations import router as merchant_citations_router
from routes.merchant_pdp import router as merchant_pdp_router
from routes.product_quality_routes import router as product_quality_router
from routes.product_enrichment_routes import router as product_enrichment_router
from routes.performance_optimization import router as performance_optimization_router
if DEBUG_MODE:
    from routes.debug_usage_logs import router as debug_usage_logs_router
    from routes.debug_query_analytics import router as debug_query_analytics_router
    from routes.debug_orders_agent import router as debug_orders_agent_router
from routes.agent_metrics_v1 import router as agent_metrics_v1_router
from routes.quick_index_setup import router as quick_index_setup_router
from routes.agent_shop_gateway import router as agent_shop_gateway_router
from routes.agent_internal_auth import router as agent_internal_auth_router
from routes.store_audit_probe_internal import router as store_audit_probe_internal_router
from routes.store_audit_commerce_probe_internal import router as store_audit_commerce_probe_internal_router
from routes.store_audit_ops import router as store_audit_ops_router
from routes.merchant_purchasability_ops import router as merchant_purchasability_ops_router
from routes.store_audit_public_intake import router as store_audit_public_intake_router
from routes.store_audit_public_intake import claim_router as store_audit_claim_router
from routes.store_readiness import router as store_readiness_router
from routes.agent_internal_products import router as agent_internal_products_router
from routes.subject_resolve import router as subject_resolve_router
from routes.accounts_orders_api import router as accounts_orders_router
from routes.buyer_api import router as buyer_router
from routes.merchant_risk_api import router as merchant_risk_router
from routes.shopify_products_sync_api import router as shopify_products_sync_router
from routes.platform_products_sync_api import router as platform_products_sync_router
from routes.outbound_links import router as outbound_links_router
from routes.attribution_conversions import router as attribution_conversions_router
from routes.merchant_events import router as merchant_events_router
from routes.cafe24_integration import router as cafe24_integration_router
from routes.magento_integration import router as magento_integration_router
from routes.adobe_commerce_events import router as adobe_commerce_events_router
from routes.cafe24_webhooks import router as cafe24_webhooks_router
from routes.woocommerce_webhooks import router as woocommerce_webhooks_router
from routes.bigcommerce_webhooks import router as bigcommerce_webhooks_router
from routes.wix_webhooks import router as wix_webhooks_router
from routes.squarespace_webhooks import router as squarespace_webhooks_router
from routes.webflow_webhooks import router as webflow_webhooks_router
from routes.sfcc_integration import router as sfcc_integration_router
from routes.sfcc_events import router as sfcc_events_router
from routes.prestashop_webhooks import router as prestashop_webhooks_router
from routes.shopline_integrations import router as shopline_integrations_router
from routes.shopline_family_webhooks import router as shopline_family_webhooks_router
from routes.ap2_agent_registration import router as ap2_agent_registration_router
from routes.ap2_trusted_issuers_admin import router as ap2_trusted_issuers_admin_router
from routes.external_offers import router as external_offers_router
from routes.prometheus_metrics import router as prometheus_metrics_router
from routes.employee_reviews import router as employee_reviews_router
from routes.readiness_internal import router as readiness_internal_router
from routes.buyer_reviews import router as buyer_reviews_router
from routes.questions_api import router as questions_router
from routes.reviews_invitation_issuer import router as reviews_invitation_issuer_router
from routes.reviews_invitation_shortlink import router as reviews_invitation_shortlink_router

# Service routers (only include what exists)
try:
    from routes.operations_routes import router as operations_router
    OPERATIONS_AVAILABLE = True
except ImportError:
    OPERATIONS_AVAILABLE = False

# Utils
from utils.auth import require_admin
from utils.logger import logger
from config.platform import (
    commit_sha as platform_commit_sha,
    deployment_id as platform_deployment_id,
    git_branch as platform_git_branch,
    is_production,
    platform_metadata,
    raw_environment_label,
    require_platform_env,
    service_id as platform_service_id,
    service_name as platform_service_name,
)
from config.settings import settings
from services.agent_governance import agent_governance, governance_runtime_contract

from openapi_config import get_custom_openapi_schema
from services.agent_webhook_service import (
    ensure_agent_webhook_tables,
    start_agent_webhook_retry_worker,
    stop_agent_webhook_retry_worker,
)
from services.merchant_webhook_service import (
    ensure_merchant_webhook_tables,
    start_merchant_webhook_retry_worker,
    stop_merchant_webhook_retry_worker,
)
from services.webhook_service import WebhookService

app = FastAPI(
    title="Pivota Infra Dashboard", 
    version="0.2.1-build-1762178331",  # Updated to verify Railway deployment
    description="Pivota Infrastructure API with comprehensive payment processing and agent SDK support",
    # The built-ins are OFF so the same three paths can be re-mounted below behind a guard. They
    # are not removed: /openapi.json is the full internal path list - 1,019 of them as measured on
    # 2026-08-22 - and serving that anonymously hands an attacker the map. The curated,
    # partner-facing spec is a different surface and stays public at /agent/docs/openapi.json;
    # anonymous GET /openapi.json redirects there rather than 404ing (it is the published URL).
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    # No trailing-slash redirect. Starlette's default answers `GET /reap/return/` with
    # `307 Location: https://<Host header>/reap/return`, and the Host is whatever the client sent:
    # the production edge forwards it unchanged (pivota-urlmap's default backend is this service,
    # probed 2026-09-25 with SNI api.pivota.cc + `Host: evil.example` -> a Location on
    # evil.example). Nothing depended on the redirect: 30 days of request logs show no 307 from
    # this mechanism other than the reviewer's own probes. A toggled-slash path is now a plain 404.
    # tests/test_no_host_reflecting_slash_redirect.py pins it. This knob covers app.router only:
    # a sub-app mounted with app.mount() redirects on its own Router's knob (there is none today).
    redirect_slashes=False,
)


def _should_skip_heavy_startup_init() -> bool:
    """Whether startup() should stop after the light schema guard.

    startup() runs a large amount of best-effort schema/DDL work. A deployed
    service must pass its healthcheck quickly; long-running DDL can exceed the
    retry window and roll the deploy back. Production skips it by default.

    Extracted from the body of startup() so it can be tested: inline, the
    prod/non-prod decision was reachable only by running the whole startup
    sequence, so nothing would have noticed it inverting. NOT the same rule as
    utils.startup_mode.should_skip_heavy_startup (which skips on ANY deployed
    host, staging included) — that difference is pre-existing and deliberate.
    """
    explicit = os.getenv("SKIP_HEAVY_STARTUP_INIT")
    if explicit is None:
        return is_production()
    return explicit.lower() in {"1", "true", "yes"}


@lru_cache(maxsize=1)
def _service_version_payload() -> dict:
    # commit_sha()/git_branch() keep the same fallback chain and add the
    # Cloud Run ones (PIVOTA_COMMIT_SHA / COMMIT_SHA / SOURCE_VERSION), which
    # a deploy must inject because Cloud Run publishes no git metadata.
    commit_sha = platform_commit_sha()
    branch = platform_git_branch()
    deployment_id = platform_deployment_id()

    if commit_sha is None:
        try:
            commit_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=os.path.dirname(__file__),
                stderr=subprocess.DEVNULL,
            ).decode("utf-8").strip()
        except Exception:
            commit_sha = None
    if branch is None:
        try:
            branch = subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=os.path.dirname(__file__),
                stderr=subprocess.DEVNULL,
            ).decode("utf-8").strip()
        except Exception:
            branch = None

    full_sha = commit_sha or None
    commit = full_sha[:12] if full_sha else None
    build_id = (
        os.getenv("PIVOTA_BUILD_ID")
        or platform_deployment_id()
        or os.getenv("VERCEL_GIT_COMMIT_SHA")
        or commit
        or f"local-{SERVICE_STARTED_AT}"
    )

    return {
        "service": "pivota-backend",
        "build_id": str(build_id),
        "started_at": SERVICE_STARTED_AT,
        "commit": commit,
        "full_sha": full_sha,
        "branch": branch,
        "deployment_id": deployment_id,
    }


@lru_cache(maxsize=1)
def _runtime_build_payload() -> dict:
    version = _service_version_payload()
    return {
        "service": "pivota-backend",
        "build_id": version["build_id"],
        "deployment_id": version["deployment_id"],
        "commit_sha": version["full_sha"],
        "full_sha": version["full_sha"],
        "version": version,
        "git": {
            "commit_sha": version["full_sha"],
            "branch": version["branch"],
        },
        # Key kept as "railway" because published health/build consumers read
        # it by that name; the values now resolve on either platform. The
        # "platform" block beside it is the platform-neutral successor.
        "railway": {
            "environment": raw_environment_label(),
            "deployment_id": version["deployment_id"],
            "service_id": platform_service_id(),
            "service_name": platform_service_name(),
        },
        "platform": platform_metadata(),
    }


@app.middleware("http")
async def add_service_version_headers(request: Request, call_next):
    response = await call_next(request)
    version = _service_version_payload()
    response.headers["X-Service-Build-Id"] = version["build_id"]
    if version["commit"]:
        response.headers["X-Service-Commit"] = version["commit"]
    if version["deployment_id"]:
        response.headers["X-Service-Deployment-Id"] = version["deployment_id"]
    return response


def _is_route_mounted(path: str, method: str) -> bool:
    for route in app.routes:
        route_path = getattr(route, "path", "") or ""
        methods = getattr(route, "methods", set()) or set()
        if route_path == path and method in methods:
            return True
    return False


def _guard_legacy_psp_maintenance_routes() -> None:
    forbidden_paths = {
        "/debug/insert-adyen",
        "/debug/check-psps",
        "/debug/psp/validate/{merchant_id}",
        "/admin/recover/psps",
        "/admin/fix/order-psp-associations/{merchant_id}",
        "/admin/fix-orders-psp-id",
        "/admin/debug/psp-overview-diagnosis",
        "/admin/debug/psp-metrics/{merchant_id}",
        "/admin/simulate/payments/{agent_id}",
        "/admin/simulate/payments/all",
    }
    mounted = sorted(
        route.path
        for route in app.routes
        if getattr(route, "path", None) in forbidden_paths
    )
    if mounted:
        raise RuntimeError(
            "Legacy PSP maintenance routes must not be mounted:\n- " + "\n- ".join(mounted)
        )


# Paths banned by `_guard_legacy_inmemory_auth_routes`. Module-level so tests
# assert against this set itself rather than a hand-copy that silently drifts.
LEGACY_INMEMORY_AUTH_PATHS = frozenset({
    "/auth/signup",
    "/auth/admin-token",
    "/auth/admin/users",
    "/auth/admin/users/{user_id}/approve",
    "/auth/admin/users/{user_id}/role",
})


def _guard_legacy_inmemory_auth_routes() -> None:
    """Fail startup if the removed in-memory auth fixtures are ever re-mounted.

    These `/auth/*` endpoints were backed by module-level dicts and honoured a
    caller-supplied `role`, so an anonymous caller could mint a JWT with
    `role: "admin"` -- satisfying `require_admin`, `get_current_admin`,
    `require_admin_or_key`, `ADMIN_ROLES`, and every route that checks
    `role in ["admin", "super_admin"]`. `/auth/admin-token` handed out an admin
    JWT to an anonymous GET with no credentials at all.

    `auth_router` is mounted unconditionally (no env flag, no host gate, no
    middleware), so nothing downstream would catch a re-introduction.

    This is a ban on the PATHS, not on the implementation, and it is
    exact-match (a near-miss like `/auth/signup/verify` does not trip it). A
    legitimate, authenticated, DB-backed admin user list at
    `/auth/admin/users` would therefore fail startup here -- that is
    deliberate: reclaiming one of these paths should be a conscious edit to
    this set in review, not something that happens by accident. The converse
    also holds: re-adding a route with a renamed path parameter would slip
    past, so this guard backs up the tests rather than replacing them.
    """
    mounted = sorted(
        route.path
        for route in app.routes
        if getattr(route, "path", None) in LEGACY_INMEMORY_AUTH_PATHS
    )
    if mounted:
        raise RuntimeError(
            "Legacy in-memory auth routes must not be mounted "
            "(anonymous privilege escalation):\n- " + "\n- ".join(mounted)
        )


def _settings_contract_payload() -> dict:
    rate_limit_rpm = getattr(settings, "rate_limit_rpm", None)
    reconciliation_mode_raw = str(os.getenv("SHOPIFY_DISCOUNT_RECONCILIATION_MODE", "observe") or "").strip().lower()
    reconciliation_mode = reconciliation_mode_raw if reconciliation_mode_raw in {"observe", "fail_closed"} else "observe"
    return {
        "rate_limit_rpm_present": hasattr(settings, "rate_limit_rpm"),
        "rate_limit_rpm": rate_limit_rpm,
        "rate_limit_rpm_source": "settings" if rate_limit_rpm is not None else "default",
        "shopify_discount_reconciliation_mode": reconciliation_mode,
        "shopify_discount_reconciliation_mode_source": (
            "env" if "SHOPIFY_DISCOUNT_RECONCILIATION_MODE" in os.environ else "default"
        ),
    }


def _runtime_contracts_payload() -> dict:
    return {
        "agent_governance": governance_runtime_contract(agent_governance),
        "canonical_mutating_routes": {
            "agent_v1_orders_create": {
                "path": "/agent/v1/orders/create",
                "method": "POST",
                "mounted": _is_route_mounted("/agent/v1/orders/create", "POST"),
            },
            "agent_v1_confirm_payment": {
                "path": "/agent/v1/orders/{order_id}/confirm-payment",
                "method": "POST",
                "mounted": _is_route_mounted("/agent/v1/orders/{order_id}/confirm-payment", "POST"),
            },
            "agent_v2_orders": {
                "path": "/agent/v2/orders",
                "method": "POST",
                "mounted": _is_route_mounted("/agent/v2/orders", "POST"),
            },
            "agent_v2_checkout_sessions": {
                "path": "/agent/v2/payments/checkout-sessions",
                "method": "POST",
                "mounted": _is_route_mounted("/agent/v2/payments/checkout-sessions", "POST"),
            },
        },
    }

# Override OpenAPI schema with our custom, investor-ready version
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    # Get the default schema generated by FastAPI
    from fastapi.openapi.utils import get_openapi
    default_schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        routes=app.routes,
        tags=app.openapi_tags,
        servers=app.servers,
        contact=app.contact,
        license_info=app.license_info,
    )
    
    # Get our custom schema
    custom_schema = get_custom_openapi_schema()
    
    # Merge: Use custom info/servers/security, but keep all real paths from default
    openapi_schema = {
        **default_schema,  # Start with the default (includes all real endpoints)
        "info": custom_schema["info"],  # Use our investor-ready description
        "servers": custom_schema["servers"],  # Use our server configuration
        "security": custom_schema["security"],  # Use our security schemes
        "components": {
            **default_schema.get("components", {}),
            **custom_schema.get("components", {})  # Merge components
        }
    }
    
    # Add our custom tags if not already present
    existing_tags = {tag["name"] for tag in openapi_schema.get("tags", [])}
    for tag in custom_schema.get("tags", []):
        if tag["name"] not in existing_tags:
            openapi_schema.setdefault("tags", []).append(tag)
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi


def docs_viewer_allowed(x_admin_key: Optional[str]) -> bool:
    """Whether this caller may read the FULL internal spec; True everywhere outside production.

    The spec is the full internal route list. Anonymously readable, it is a map of every path an
    attacker might probe, which is worth more to them than any single one of those paths. This
    predicate only answers "may they read it"; what a disallowed caller receives instead is
    decided by the route handler (_guarded_openapi_spec).

    Outside production this returns True: /docs is how people work, and a staging service that
    behaves differently from the one you develop against is its own hazard. `is_production()`
    fails CLOSED toward production when it cannot tell (config/platform.py), so an environment
    that forgets PIVOTA_ENV gets the guard rather than the hole.

    Compared with `hmac.compare_digest` ON BYTES. The sibling admin guards in routes/ use `!=`,
    which leaks a timing signal proportional to the shared prefix; there is no reason for a new one
    to inherit that. But the str form of compare_digest RAISES TypeError on any codepoint above
    0x7F, and ASGI decodes header values as latin-1 - so a single byte >= 0x80 in X-ADMIN-KEY once
    turned this guard into an unhandled 500: a remotely triggerable 5xx on a payments API, and -
    because `expected and ...` short-circuits - one that fired only when a key was configured, a
    free unauthenticated probe for "is an admin key mounted on this revision". Encoding both sides
    first removes both.

    It also fails closed when no key is configured at all - an empty expected value must never match
    an empty header.
    """
    if not is_production():
        return True
    expected = (os.getenv("ADMIN_API_KEY") or os.getenv("PROMOTIONS_ADMIN_KEY") or "").strip()
    supplied = (x_admin_key or "").strip()
    return bool(expected and supplied and hmac.compare_digest(
        supplied.encode("utf-8", "surrogateescape"),
        expected.encode("utf-8", "surrogateescape"),
    ))


async def require_docs_ui_enabled() -> None:
    """The HTML doc pages are not served in production at all.

    They are shells that fetch /openapi.json FROM THE BROWSER, and a browser cannot attach an
    X-ADMIN-KEY header to that fetch - so an admin-gated Swagger page in production renders
    "Failed to load API definition". Serving a page that cannot work is worse than not serving
    one: it reads as a broken API rather than a deliberate closure, and it still advertises that
    the surface exists. Admins read the spec with curl and the key.
    """
    if is_production():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")


# Every method, not just GET. Mounting GET alone left FastAPI answering 405 with `Allow: GET` on
# anything else, so one `curl -X POST` proved /docs and /redoc exist - which is the very fact
# their 404s exist to hide. /openapi.json no longer hides (its GET answers a public redirect by
# design) but keeps the uniform non-GET 404 so the method channel is identical across all three.
_DOC_ROUTE_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"]


@app.api_route(
    "/openapi.json",
    methods=_DOC_ROUTE_METHODS,
    include_in_schema=False,
)
async def _guarded_openapi_spec(request: Request) -> Response:
    if request.method not in ("GET", "HEAD"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")
    # no-store on BOTH branches: the response varies on X-ADMIN-KEY, and a shared cache that
    # stored the keyed 200 would replay the full internal map to anonymous callers - while a
    # cached redirect would hand keyed ops clients the wrong document with a clean 200.
    if docs_viewer_allowed(request.headers.get("X-ADMIN-KEY")):
        return JSONResponse(app.openapi(), headers={"Cache-Control": "no-store"})
    # Everyone else is sent to the CURATED partner spec, which is public by design. /openapi.json
    # is the URL the marketing site publishes and the conventional path agents probe first; a 404
    # here read as "no public spec" when /agent/docs/openapi.json has been public all along. The
    # redirect must be identical for a missing, empty, and wrong key - any difference is an
    # oracle for whether an admin key is mounted. 307 rather than 308: clients cache a permanent
    # redirect indefinitely, which would freeze the alias target (and this URL's keyed behavior)
    # into every cache that ever saw an anonymous response.
    return RedirectResponse(
        url=app.url_path_for("agent_openapi_spec"),
        status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        headers={"Cache-Control": "no-store"},
    )


@app.api_route(
    "/docs",
    methods=_DOC_ROUTE_METHODS,
    include_in_schema=False,
    dependencies=[Depends(require_docs_ui_enabled)],
)
async def _guarded_swagger_ui(request: Request) -> HTMLResponse:
    if request.method not in ("GET", "HEAD"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")
    return get_swagger_ui_html(openapi_url="/openapi.json", title=f"{app.title} - Swagger UI")


@app.api_route(
    "/redoc",
    methods=_DOC_ROUTE_METHODS,
    include_in_schema=False,
    dependencies=[Depends(require_docs_ui_enabled)],
)
async def _guarded_redoc(request: Request) -> HTMLResponse:
    if request.method not in ("GET", "HEAD"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")
    return get_redoc_html(openapi_url="/openapi.json", title=f"{app.title} - ReDoc")


async def startup_event():
    """
    Initialize database connections and ensure core tables exist.
    """
    # Avoid blocking Railway healthchecks indefinitely.
    try:
        if not getattr(database, "is_connected", False):
            await connect_database_with_timeout(15, db=database)
    except Exception as exc:
        logger = logging.getLogger(__name__)
        logger.warning(f"⚠️ Startup DB connect skipped/failed (continuing degraded): {exc}")
        return
    # All startup DDL below runs under a Postgres advisory lock so concurrent
    # instances (Cloud Run min-instances>1, rolling revisions) and concurrent
    # tasks serialize their CREATE ... IF NOT EXISTS against an EMPTY database.
    # See db/startup_ddl.py. No-op cost on a warm DB: one lock round-trip.
    async with startup_ddl_lock():
        try:
            # Create all tables defined in db.database (transactions, etc.)
            metadata.create_all(engine)
        except Exception as exc:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ Failed to run metadata.create_all: {exc}")

        # Ensure quote-first table exists (best-effort; does not raise).
        try:
            from db.quotes import ensure_quotes_table

            await ensure_quotes_table()
        except Exception as exc:
            logger.warning(f"⚠️ Failed to ensure quotes table: {exc}")

        # Ensure webhook audit/idempotency table exists (best-effort; does not raise)
        await WebhookService.ensure_webhook_events_table()
        await ensure_agent_webhook_tables()
        await ensure_merchant_webhook_tables()
    await start_agent_webhook_retry_worker()
    await start_merchant_webhook_retry_worker()

    # Optional: background photo TTL cleanup (non-blocking).
    try:
        start_photo_cleanup_loop()
    except Exception:
        pass

    # PR-1b: APM auto-re-audit cron. Best-effort — start_scheduler
    # logs + degrades gracefully if apscheduler is unavailable, so
    # this won't crash the API on missing dep.
    #
    # NOTE: scheduler init failures (import errors in worker modules,
    # apscheduler version mismatches, etc.) were previously swallowed
    # silently here, producing a deployment where audit endpoints
    # accepted requests but no worker tick ever drained the queue.
    # Log the exception with traceback so the boot-time failure is
    # visible in logs while preserving the best-effort contract.
    try:
        from services.audit_scheduler import start_scheduler
        await start_scheduler()
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).exception(
            "audit_scheduler boot failed (continuing degraded; "
            "audit endpoints will accept requests but no worker will "
            "drain them until this is fixed): %s",
            exc,
        )


async def shutdown_event():
    await stop_agent_webhook_retry_worker()
    await stop_merchant_webhook_retry_worker()
    # PR-1b: gracefully stop the audit scheduler.
    try:
        from services.audit_scheduler import stop_scheduler
        await stop_scheduler()
    except Exception:
        pass
    finally:
        # `finally`, not a bare next statement: stop_scheduler now contains an await (the
        # shutdown drain), so a CancelledError landing inside it would propagate past an
        # `except Exception` and skip the disconnect entirely. Cancellation still propagates
        # -- swallowing it would break cancellation semantics -- but the pool is closed first.
        #
        # GUARDED, because the reorder made THIS the disconnect that actually runs. It is
        # asyncpg's Pool.close(); if it raises, the exception escapes app_lifespan's own
        # `finally` and `await shutdown()` never runs, which uvicorn reports as a failed
        # lifespan shutdown. Previously the guarded copy in shutdown() ran first and this one
        # hit databases' idempotent no-op path, so the raise had nowhere to go.
        try:
            await database.disconnect()
        except Exception as exc:  # noqa: BLE001
            logger.error("Error closing the database pool during shutdown: %s", exc)

# CORS. Two tiers, resolved in middleware/cors.py (the module docstring is the design record):
# first-party portals + ALLOWED_ORIGINS get browser credentials; every other Origin gets a plain
# `*` with NO credentials. The previous code parsed ALLOWED_ORIGINS and then set
# allow_origin_regex=".*" with allow_credentials=True on both branches, which reflected any Origin
# with credentials - the 2026-09-25 finding against #2346.
trusted_cors_origins, trusted_cors_origin_regex = resolve_trusted_origins(
    settings.cors_origins, dev_mode=settings.dev_mode
)

# Add usage logging middleware (tracks Agent API calls)
app.add_middleware(UsageLoggerMiddleware)

# Add rate limiting middleware for agent API (env-configurable)
app.add_middleware(
    RateLimitMiddleware,
    requests_per_minute=int(getattr(settings, "rate_limit_rpm", 1000) or 1000),
)

# Add structured logging middleware (logs all requests in JSON format)
app.add_middleware(StructuredLoggingMiddleware)

# Add unified error handler middleware (formats errors + includes request id)
app.add_middleware(ErrorHandlerMiddleware)

# Security response headers on EVERY response, including the ones other middleware generate.
#
# Registered AFTER ErrorHandlerMiddleware, which in Starlette means it wraps it: add_middleware
# prepends, so the last registered runs outermost. That ordering is the point - an error response
# formatted by the handler below still leaves through here and still carries nosniff/HSTS. A
# security header that is present on 200s and missing on 500s protects the requests that matter
# least.
#
# CORSMiddleware is registered after this one and so stays outermost of all, which the comment
# there explains: error responses must keep their CORS headers.
app.add_middleware(SecurityHeadersMiddleware)

# AP2 protocol security middleware. Enforces agent-consent / signature / nonce
# headers on non-public /ap2/* routes. Inert unless ENABLE_AP2_ROUTES=true — the
# same flag that mounts the AP2 router below — so this is a no-op by default and
# never touches non-/ap2 traffic. Do NOT enable in any environment before working
# through docs/AP2_ENABLEMENT.md (agent public-key registration + the sibling
# verify_ap2_signature reconciliation).
#
# Deliberately added AFTER ErrorHandlerMiddleware, i.e. it wraps *outside* it, so
# AP2's own rejections keep their protocol-shaped body ({error, protocol, version}
# from AP2SecurityMiddleware.dispatch) instead of being reformatted by the unified
# error handler. The trade-off is those AP2 responses skip the handler's request-id
# injection; that is intentional, not an ordering bug — leave it after the error
# handler and inside CORS (added below) so CORS headers still apply.
app.add_middleware(AP2SecurityMiddleware, enabled=settings.enable_ap2_routes)

# Handle CORS preflight (OPTIONS) for any path in middleware rather than via a
# catch-all `@app.options("/{full_path:path}")` route. A catch-all OPTIONS route
# makes every path "match" by path, so non-OPTIONS requests to unknown paths
# (e.g. GET /robots.txt, GET /favicon.ico) return 405 Method Not Allowed instead
# of 404 — which crawlers report as "Blocked due to other 4xx issue" and treat
# as a site-wide crawl block. Intercepting OPTIONS before routing avoids the
# phantom match, so unknown paths get a clean 404. Registered just before CORS
# so CORSMiddleware stays outermost (real preflights with
# Access-Control-Request-Method are still handled by CORSMiddleware first).
@app.middleware("http")
async def cors_preflight_passthrough(request: Request, call_next):
    if request.method == "OPTIONS":
        # No Allow-Origin / Allow-Credentials here. This response leaves through the tiered CORS
        # middleware below, which decorates it by Origin exactly like any other response: a
        # first-party origin gets itself reflected plus credentials, anything else gets `*` and
        # no credentials. The old hand-written headers reflected ANY Origin with credentials -
        # the same finding as the middleware config, on a second path.
        return Response(
            status_code=200,
            headers={
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-API-Key, X-Request-Id, X-Buyer-Issuer-Key, Idempotency-Key",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH",
            },
        )
    return await call_next(request)


# CORS should be outermost so error responses still include headers.
app.add_middleware(
    TieredCORSMiddleware,
    trusted_origins=trusted_cors_origins,
    trusted_origin_regex=trusted_cors_origin_regex,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=[
        "Content-Type",
        "Authorization",
        "X-API-Key",
        "X-Checkout-Token",
        "X-Request-Id",
        "Idempotency-Key",
        "X-Pivota-Brief-Id",
        "X-Pivota-Brief-Schema-Version",
    ],
    expose_headers=[
        "X-Request-Id",
        "X-Total-Count",
        # Deck export (PR #1411): the portal reads billing feedback + the
        # download filename off the PPTX response cross-origin.
        "Content-Disposition",
        "X-Pivota-Billing-Mode",
        "X-Pivota-Credits-Charged",
    ],
)

# Log CORS configuration for debugging
import logging
logger = logging.getLogger(__name__)
logger.info(
    "🌐 CORS configured: credentialed origins=%s, credentialed regex=%s; every other Origin gets "
    "`*` without credentials",
    trusted_cors_origins,
    trusted_cors_origin_regex,
)

# Include available routers
app.include_router(agent_router)
app.include_router(agent_briefs_router)
app.include_router(agent_checkout_intents_router)
app.include_router(quote_router)
app.include_router(offer_router)
app.include_router(ops_router)
app.include_router(psp_router)
app.include_router(payment_router)
app.include_router(auth_router)  # New authentication system
app.include_router(mcp_oauth_as_router)  # MCP OAuth Authorization Server (returns 404 unless MCP_OAUTH_AS_ENABLED=1)
app.include_router(crawl_identity_router)  # GET /.well-known/http-message-signatures-directory (404 unless WEB_BOT_AUTH_PRIVATE_KEY)
app.include_router(auth_api_router)  # API auth endpoints (/api/auth/*)
app.include_router(admin_migrations_router)  # Admin migrations
app.include_router(admin_apply_agent_center_migration_router)  # Agent Center V1 migration apply/verify
app.include_router(admin_apply_citation_operator_router)  # Citation-operator migrations 146-148 apply/verify
app.include_router(agent_center_demand_test_router)  # Agent Center: Demand Test Agent V1 (internal pilot)
app.include_router(agent_center_sku_match_router)  # Agent Center: SKU Match Agent V1 (internal pilot)
app.include_router(agent_center_admin_router)  # Agent Center: stuck-run inspection / force-reset (admin-only)
app.include_router(agent_center_bd_router)  # Agent Center: BD external-merchant AI visibility report
app.include_router(channel_graph_router)  # BD Channel Graph: cross-merchant cited-host demand rollup (read-only)
app.include_router(merchant_audit_router)
app.include_router(funnel_metrics_router)  # Admin audit-growth-funnel conversion metrics (WS-4)  # Merchant self-service AI Commerce Readiness audit (legacy synchronous)
app.include_router(audit_public_share_router)  # Wave-3 B2: unauthenticated read-only audit shares (env-flagged)
app.include_router(brand_claim_router)  # P1: brand claim write path (claim -> verify -> brand_direct), merchant-scoped
app.include_router(substantiation_admin_router)  # P1: substantiation grading (employee-only, flag-gated)
app.include_router(audit_runs_router)  # P2.3: async audit_runs lifecycle (POST/GET/cancel/list at /api/audits)

# Operational endpoint: report APScheduler health (running? jobs registered?).
# Root logger filters app-level INFO on Railway prod, so the scheduler success
# log line never surfaces in log fetches; this endpoint provides direct state.
from routes.scheduler_health import router as scheduler_health_router
app.include_router(scheduler_health_router)
from routes.pool_health import router as pool_health_router
app.include_router(pool_health_router)  # GET /__pool_health: pool-vs-database partition during a wedge

# Admin: resume/pause allowlisted settlement crons (T7/T8 + settlement-file
# jobs) for Stage-4 promotion and rollback without a redeploy.
from routes.admin_scheduler_jobs import router as admin_scheduler_jobs_router
app.include_router(admin_scheduler_jobs_router)

# Admin: read the retailer ingest ledger, approve/cancel a held job, queue a
# cohort (migration 234). require_admin sits on the router itself.
from routes.admin_retailer_ingest import router as admin_retailer_ingest_router
app.include_router(admin_retailer_ingest_router)
from routes.admin_gmv_invoice_credits import router as admin_gmv_invoice_credits_router
app.include_router(admin_gmv_invoice_credits_router)  # GMV invoice credits: review/approve/issue (admin only)

# C1 Phase 2d — read-only trust table health (total rows, decision distribution,
# drift count, stale rows). Mirrors /__scheduler_health in naming convention.
from routes.__trust_health import router as trust_health_router
app.include_router(trust_health_router)

# ADR-012 Phase 0 — per-cohort catalog funnel + on-demand invariant checks.
from routes.__catalog_health import router as catalog_health_router
app.include_router(catalog_health_router)

# Operational endpoint family for v1.3 monetization shakeout — wraps the
# T6/T7/T9 cron tasks as synchronously-invokable HTTP endpoints so end-to-end
# shakeout scripts (scripts/shakeout/c_full_order_pipeline.py et al.) can
# drive the pipeline without waiting for the daily/monthly schedulers.
# Gated to non-production environments + a shared-secret header
# (SHAKEOUT_DEBUG_TOKEN). See routes/shakeout_debug.py for the safety model.
from routes.shakeout_debug import router as shakeout_debug_router
app.include_router(shakeout_debug_router)
app.include_router(gsc_oauth_router)  # Phase D: Google Search Console OAuth start + callback
app.include_router(pivota_canonical_router)  # Public canonical PDP resolver (sig_* → product) + sitemap list
app.include_router(agent_pdp_v1_router)  # Agent PDP v1 denormalized read path (/api/agent/pdp/*)
app.include_router(agent_citation_v1_router)  # External citation read API (/agent/v1/citation/*) — ADR-007 P0
app.include_router(agent_account_router)  # Agent account management (/agent/account/*)
app.include_router(agent_commerce_router)  # Agent v2 commerce execute contract
# Agent v2 Reap agentic purchase rail (/agent/v2/commerce/reap/*). DARK: every route on it
# answers 404 while REAP_AGENTIC_ENABLED is off or the Reap client is unconfigured, which is
# the state of production. Mounted anyway, and deliberately: a router that is only mounted
# when a dial is on is a router whose mounting is itself untested, and the dial is read per
# request so flipping it must not need a redeploy.
app.include_router(agent_commerce_reap_router)
# GET /agent/v2/commerce/purchases[/{id}]: the rail-neutral purchase read (payment orchestration
# P0). Read-only; dark (404) unless AGENT_PURCHASE_LEDGER_ENABLED, read per request.
app.include_router(agent_commerce_purchases_router)
# GET/HEAD /reap/return: the static page Reap's hosted checkout sends the buyer's browser back to
# (the rail's default returnUrl is https://api.pivota.cc/reap/return). Public and NOT dialled:
# a buyer mid-purchase must never land on a 404 because an operator turned the rail off. It
# reads nothing, writes nothing, and says nothing about the order (routes/reap_return.py).
app.include_router(reap_return_router)
app.include_router(admin_api_router)  # Admin API endpoints
app.include_router(admin_partner_cohort_router)  # Admin channel-partner cohort progress/evaluation
app.include_router(admin_partner_comms_router)  # Admin channel-partner contact, send log, send settlement email
app.include_router(admin_partner_invite_tokens_router)  # Admin channel-partner invite token issue/list/revoke
app.include_router(admin_partner_settlements_router)  # Admin channel-partner settlement list + detail
app.include_router(admin_partner_subsidies_router)  # Admin channel-partner subsidy ledger read
app.include_router(admin_partners_router)  # Admin channel-partner list + detail, subsidies POST, settlement retry
app.include_router(admin_billing_statements_router)  # Admin report-only monthly statement assembly (no charge)
app.include_router(admin_reset_employee_router)  # Admin employee password reset
app.include_router(admin_cleanup_duplicates_router)  # Admin cleanup for duplicate data
app.include_router(admin_data_consistency_router)  # Admin data consistency check and fix
app.include_router(admin_merchant_canonicalize_router)  # Admin merchant canonicalization
app.include_router(admin_merchant_reset_router)  # Admin merchant reset
app.include_router(admin_shopify_health_router)  # Admin Shopify health check
if settings.enable_amazon_sp_api:
    app.include_router(admin_amazon_oauth_router)  # Amazon SP-API OAuth (admin-only)
    app.include_router(admin_amazon_orders_router)  # Amazon SP-API orders sync (admin-only)
    app.include_router(admin_amazon_fulfillment_router)  # Amazon SP-API fulfillment feeds (admin-only)
if (
    settings.feature_readiness_audit
    or settings.feature_readiness_ucp_thin_slice
    or settings.feature_readiness_real_merchant_alpha
    or settings.feature_readiness_source_of_truth_v1
    or settings.feature_readiness_canonical_checkout_alpha
):
    app.include_router(readiness_internal_router)
    logger.info("✅ Readiness audit / UCP thin-slice router registered")
else:
    logger.info("⏭️  Readiness audit / UCP thin-slice router disabled")
app.include_router(products_cache_maintenance_router)  # Products cache maintenance
# Platform-connector test (Shopify/Wix/Woo/BigCommerce connection check).
# Canonical prefix + a WORKING legacy /mcp alias — see routes/platform_connector_prefix.
app.include_router(mcp_e2e_test_router, prefix=PLATFORM_CONNECTORS_PREFIX)
app.include_router(
    mcp_e2e_test_router,
    prefix=LEGACY_MCP_PREFIX,
    dependencies=[Depends(legacy_mcp_prefix_deprecation)],
    include_in_schema=False,
)
app.include_router(admin_cleanup_all_router)  # Admin cleanup all test data
app.include_router(admin_debug_shopify_token_router)  # Admin debug Shopify token
# Employee agent management (stable /employee/agents/* endpoints used by Employee Portal)
app.include_router(employee_agent_mgmt_router)
app.include_router(employee_settlement_router)
app.include_router(employee_content_router)
app.include_router(admin_fix_agents_router)  # Admin fix agents data
app.include_router(admin_fix_agent_metrics_router)  # Admin fix agent metrics
app.include_router(admin_fix_agent_metrics_v2_router)  # Admin fix agent metrics v2 (from orders)
app.include_router(debug_mcp_data_router)  # Debug MCP data
app.include_router(admin_run_migration_008_router)  # Run migration 008 - Agents Phase 2
app.include_router(admin_governance_router)  # Admin governance - Phase 3
app.include_router(admin_run_migration_009_router)  # Run migration 009 - Agents Phase 3
app.include_router(admin_seed_test_data_router)  # Seed test data for Phase 3 demo
app.include_router(admin_run_migration_010_router)  # Run migration 010 - Phase 4 Payment Routing
app.include_router(admin_fix_agent_protocols_router)  # Fix agent protocols - Phase 4
# Phase 4 - Payment Routing & Protocol Support
app.include_router(payment_routing_router)  # Payment routing with failover
app.include_router(employee_payment_routing_router)  # Employee payment routing monitoring
app.include_router(protocol_router)  # Protocol management (AP2, ACP, X-402)
app.include_router(agent_protocol_test_router)  # Agent protocol testing (must be before agent_protocol_router)
app.include_router(agent_protocol_router)  # Agent-specific protocol management
app.include_router(employee_protocol_router)  # Employee protocol monitoring
app.include_router(employee_routing_dashboard_router)  # Employee PSP routing dashboard

# AP2 protocol surface (agent consent grant/revoke, transactions, X-402, wallet).
# Gated by ENABLE_AP2_ROUTES (default false) so it stays off until the enablement
# checklist in docs/AP2_ENABLEMENT.md is satisfied. POST /ap2/consent/grant
# self-authenticates — it resolves agents.public_key and fails closed on an
# unknown/keyless agent or bad signature — so the router is safe to mount without
# relying on AP2SecurityMiddleware enforcement, and no route depends on the
# not-yet-reconciled verify_ap2_signature helper.
if settings.enable_ap2_routes:
    from routes.ap2_routes import router as ap2_router
    app.include_router(ap2_router)
    logger.info("✅ AP2 protocol routes registered (ENABLE_AP2_ROUTES=true)")
else:
    logger.info("⏭️  AP2 protocol routes disabled (ENABLE_AP2_ROUTES=false)")

# [Phase 4++] Dual-side routing and AP2 adapter
app.include_router(routing_governance_router)  # Routing policy management
app.include_router(admin_run_migration_011_router)  # Run migration 011 - Phase 4++ Dual Routing
app.include_router(admin_creators_router)  # Phase 3 — creator directory intake
app.include_router(admin_run_migration_152_router)  # Run migration 152 - agent_pdp_view evidence columns
app.include_router(admin_run_migration_164_router)  # Run migration 164 - merchant_onboarding.operating_mode (store-less brand)
app.include_router(admin_run_migration_165_router)  # Run migration 165 - index_pipeline_state.index_eligible (ADR-007 SLICE 1)
app.include_router(admin_rescore_external_seed_quality_router)  # In-cluster chunked external-seed quality rescore (proxy/ssh venues both unusable)
app.include_router(admin_run_migration_pending_router)  # Generic: run db/migrations/<NNN>_*.sql by number (covers 161/162/163 + future)
app.include_router(admin_seed_routing_logs_router)  # Seed test routing logs
app.include_router(admin_cleanup_routing_router)  # Cleanup routing test data
app.include_router(admin_seed_agent_history_router)  # Seed agent routing history for demo

# [Phase 5] Agent routing control and revenue
app.include_router(agent_routing_api_router)  # Agent routing policies and testing
app.include_router(agent_revenue_api_router)  # Agent revenue policies and earnings (+ Phase 5.5 expectations)
app.include_router(admin_run_migration_012_router)  # Run migrations 012a/012b - Phase 5 Revenue
app.include_router(admin_run_migration_013_router)  # Run migration 013 - Consolidate routing systems

# [Phase 5.5] Dual-sided revenue matching
app.include_router(merchant_commission_api_router)  # Merchant commission offers

# [Phase 5.6] Agent Portal - Settlement, Protocol, Integration
app.include_router(agent_settlement_router)  # Agent settlements and payouts
app.include_router(merchant_payouts_router)  # Merchant payout management
app.include_router(agent_payouts_router)  # Agent payout view
app.include_router(employee_payouts_router)  # Employee payout approval endpoints
app.include_router(admin_payout_backfill_router)  # Admin backfill for payouts/commissions
app.include_router(agent_integration_router)  # Agent integration status (aggregates existing data)
app.include_router(agent_bank_router)  # Agent bank info
app.include_router(merchant_agent_bank_router)  # Merchant view agent bank info

app.include_router(admin_psp_integrity_router)  # PSP data integrity management
app.include_router(admin_orphan_orders_router)  # Janitorial: cancel orphaned never-paid hosted-link drafts
app.include_router(admin_run_migration_router)  # Database migrations via API
app.include_router(admin_run_migration_081_router)  # Run migration 081 - Seed content lock/proposals
app.include_router(admin_run_migration_082_router)  # Run migration 082 - seed_data_proposals.status += 'no_change'
app.include_router(admin_run_migration_083_router)  # Run migration 083 - catalog_products.content_key
app.include_router(admin_run_migration_084_router)  # Run migration 084 - catalog_products sync hygiene
app.include_router(admin_run_migration_085_router)  # Run migration 085 - agent_pdp_view denormalized table
app.include_router(admin_run_migration_089_router)  # Run migration 089 - merchant_onboarding APM config (PR-13 hotfix)
app.include_router(admin_run_migration_098_router)  # Run migration 098 - index_pipeline_state
app.include_router(admin_run_migration_099_router)  # Run migration 099 - domain_extractor_baselines
app.include_router(admin_sync_refresh_presence_router)  # Stage 2a — lightweight Shopify presence refresh
app.include_router(merchant_router)  # Merchant management endpoints
app.include_router(merchant_onboarding_router)  # Merchant onboarding (Phase 2)
if settings.platform_onboarding_v2_enabled:
    app.include_router(platform_onboarding_router)  # Platform Merchant Onboarding v2
    logger.info("✅ Platform Onboarding v2 router registered successfully")
else:
    logger.info("⚠️ Platform Onboarding v2 is disabled (feature flag is false)")
app.include_router(merchant_dashboard_router)  # Merchant dashboard API
app.include_router(admin_catalog_debug_router)  # Internal catalog debug
app.include_router(admin_audit_backfill_router)  # W1 site-5 stored-report backfill
app.include_router(admin_signal_scorecard_router)  # Internal controlled-signal scorecard
app.include_router(admin_outcomes_router)  # Internal outcome-aggregation read/refresh
app.include_router(merchant_analytics_router)  # Merchant analytics (trends)
app.include_router(citation_operator_router)  # Provider-aware AI-citation operator (merchant-facing)
app.include_router(pivot_router)  # Pivot catalog query and quote APIs
app.include_router(catalog_router)  # Catalog sync and webhook ingest APIs
app.include_router(merchant_api_extensions_router)  # Extended merchant API features
app.include_router(direct_db_check_router)  # Direct DB check
app.include_router(cleanup_test_data_router)  # Cleanup test data
app.include_router(manage_integrations_router)  # Manage integrations (delete/update)
app.include_router(psp_metrics_router)  # Real PSP metrics
app.include_router(wix_sync_router)  # Wix product sync
app.include_router(fix_duplicate_stores_router)  # Fix duplicate stores
app.include_router(admin_cleanup_router)  # Admin cleanup (no auth)
app.include_router(init_orders_router)  # Orders initialization
app.include_router(employee_dashboard_router)  # Employee dashboard endpoints
app.include_router(employee_finance_router)  # Employee finance endpoints
app.include_router(employee_products_router)  # Employee products endpoints (MVP)
app.include_router(employee_pdp_governance_router)  # Employee PDP governance projection/review
app.include_router(employee_kb_monitoring_router)  # Aurora KB v0 monitoring summary
app.include_router(employees_security_router)  # Employees and security
# Platform-connector management (status / test-connection / analytics / logs).
app.include_router(mcp_mgmt_router, prefix=PLATFORM_CONNECTORS_PREFIX)
app.include_router(
    mcp_mgmt_router,
    prefix=LEGACY_MCP_PREFIX,
    dependencies=[Depends(legacy_mcp_prefix_deprecation)],
    include_in_schema=False,
)
app.include_router(employee_reviews_router)  # Employee Reviews Center (imports/governance)
app.include_router(buyer_reviews_router)  # Buyer Reviews (submission; default disabled)
app.include_router(questions_router)  # Buyer Questions (UGC)
app.include_router(reviews_invitation_issuer_router)  # Internal: mint invitation_token from paid orders
app.include_router(reviews_invitation_shortlink_router)  # Public: short links -> buyer review submission
app.include_router(employee_missing_router)  # Missing employee endpoints
app.include_router(agent_sdk_fixed_router)  # Fixed SDK-ready agent endpoints
app.include_router(emp_store_psp_router)  # Employee store/PSP connection fixes
app.include_router(fix_agents_router)  # Fix agents table schema
app.include_router(agent_payment_router)  # Agent payment SDK endpoints
app.include_router(agent_products_router)  # Agent product browsing
app.include_router(psp_overview_router)  # PSP overview and metrics
app.include_router(admin_fix_router)  # Admin fix utilities
app.include_router(merchant_store_connections_router)  # Merchant store connections (Shopify, Wix)
app.include_router(ops_shopify_integration_router)  # Internal ops: Shopify integration verify/report
app.include_router(ops_pcs_reducer_router)  # Internal ops: PCS reducer replay
app.include_router(merchant_onboarding_shopify_verify_router)  # Merchant onboarding facade: Shopify verify/report
app.include_router(admin_simple_fix_router)  # Admin simple fix
app.include_router(admin_cleanup_rebuild_router)  # Admin cleanup and rebuild
app.include_router(admin_cleanup_stores_router)  # Admin cleanup stores
app.include_router(outbound_links_router)  # Outbound links (resolve + redirect + ops config)
app.include_router(attribution_conversions_router)  # Non-custodial conversion-report / receipt-ingest API (#1482)
app.include_router(merchant_events_router)  # Platform-neutral Agent Commerce event ingestion
app.include_router(cafe24_integration_router)  # Cafe24 OAuth/manual connection and catalog adapter
app.include_router(magento_integration_router)  # Magento/Adobe Commerce native REST catalog adapter
app.include_router(adobe_commerce_events_router)  # Signed Adobe I/O order/payment/refund events
app.include_router(cafe24_webhooks_router)  # Cafe24 Data Bridge + order lifecycle events
app.include_router(woocommerce_webhooks_router)  # Signed WooCommerce order lifecycle events
app.include_router(bigcommerce_webhooks_router)  # Header-authenticated BigCommerce order lifecycle events
app.include_router(wix_webhooks_router)  # JWT-verified Wix eCom order + transaction events (static, app-level)
app.include_router(squarespace_webhooks_router)  # HMAC-signed Squarespace order notifications (OAuth-connected sites only)
app.include_router(webflow_webhooks_router)  # Webflow Ecommerce order triggers, authenticated by a per-store URL secret (+ signature when an OAuth app is configured)
app.include_router(sfcc_integration_router)  # Salesforce B2C Commerce SCAPI catalog adapter
app.include_router(sfcc_events_router)  # Signed SFCC cartridge order/cart/payment events
app.include_router(prestashop_webhooks_router)  # Signed PrestaShop module order/refund events (no native webhooks)
app.include_router(shopline_integrations_router)  # SHOPLINE / Shoplazza native REST catalog adapters
app.include_router(shopline_family_webhooks_router)  # Signed SHOPLINE / Shoplazza order lifecycle events
app.include_router(ap2_agent_registration_router)  # AP2 agent signing-key ADMIN backfill (#1442) — pilot provisioning (ADR-012 carve-out)
app.include_router(ap2_trusted_issuers_admin_router)  # AP2 trusted-issuer enrollment (#1495) — global tier + per-agent, proof-of-control
app.include_router(external_offers_router)  # External offers (fetch/cache OG/JSON-LD for external-only products)
app.include_router(simple_test_orders_router)  # Simple test orders generation
app.include_router(migrate_employees_password_router)  # Migrate employees table
app.include_router(debug_product_sync_router)  # Debug product sync
app.include_router(debug_shopify_api_router)  # Debug Shopify API
app.include_router(admin_sql_router)  # Admin SQL
app.include_router(admin_agents_debug_router)  # Admin agents debug
app.include_router(agent_health_router)  # Agent health check
app.include_router(photos_router)  # Selfie uploads + QC (presign/confirm/qc)
app.include_router(admin_usage_debug_router)  # Admin usage logs debug
app.include_router(agent_analytics_router)  # Agent analytics (funnel, queries)
if DEBUG_MODE:
    app.include_router(agent_debug_router)  # Agent debug endpoints (TEMP)
    app.include_router(debug_products_router)  # Debug products endpoints
    app.include_router(test_populate_router)  # Test data population
    logger.warning("⚠️ DEBUG MODE ENABLED - Debug endpoints are accessible!")
app.include_router(shopify_router)  # Shopify MCP integration
app.include_router(payment_execution_router)  # Payment execution (Phase 3)
app.include_router(billing_router)  # Stripe Billing monetization
app.include_router(partner_invite_redeem_router)  # Merchant onboarding partner invite redemption
# Register more specific product routes FIRST to avoid path conflicts
app.include_router(product_router_v2)  # Product management v2 (cache-based) - MUST be before product_router
app.include_router(product_sync_router)  # Product sync from platforms (legacy)
app.include_router(product_router)  # Product management - MUST be after v2 to avoid /{merchant_id} matching /v2/xxx
app.include_router(universal_sync_router)
app.include_router(sync_all_router)  # Universal product sync (new)
app.include_router(product_monitoring_router)  # Product sync monitoring and metrics
# Temporary debug endpoints commented out - v2 is stable now
app.include_router(order_router)  # Order processing
app.include_router(accounts_orders_router)  # Accounts & Orders API (customer-facing)
app.include_router(buyer_router)  # Buyer Vault API (unified buyer account)
app.include_router(webhook_router)  # Webhook handlers
app.include_router(agent_api_router)  # Agent API endpoints
if agent_v2_router is not None:
    app.include_router(agent_v2_router)  # Canonical Agent v2 middleware contract
app.include_router(subject_resolve_router)  # Stable subject resolution contract (/v1/subject/resolve)
app.include_router(after_sales_cases_router)  # After-sales Case API (refund/return_refund)
app.include_router(agent_recommendations_router)  # Agent recommendations (proxy to internal service)
app.include_router(agent_events_router)  # Agent events (click tracking etc.)
app.include_router(card_rail_outcomes_router)  # POST /agent/v1/outcomes — handoff results
app.include_router(agent_cards_router)  # POST /agent/v1/cards — Reap rail card minting (503 unless AGENT_CARD_ISSUANCE_ENABLED)
app.include_router(reap_webhooks_router)  # POST /webhooks/reap — issuer reconciliation (503 unless REAP_WEBHOOK_SECRET); POST /webhooks/reap/authorize — live external authorization (503 unless REAP_EXTERNAL_AUTH_ENABLED + REAP_AUTH_WEBHOOK_SECRET)
app.include_router(agent_shop_gateway_router)  # Agent shopping gateway (/agent/shop/v1/invoke)
app.include_router(agent_internal_auth_router)  # Internal auth introspection (/agent/internal/auth/introspect)
app.include_router(store_audit_probe_internal_router)  # Store Audit UCP worker receipt (flag + key gated)
app.include_router(store_audit_commerce_probe_internal_router)  # Store Audit commerce receipt/capability (flag + key gated)
app.include_router(store_audit_ops_router)  # Admin-only Store Audit lane diagnostics (no caller SQL; redacted)
app.include_router(merchant_purchasability_ops_router)  # Admin-only merchant purchasability facts (no caller SQL; no buyer data)
app.include_router(store_audit_public_intake_router)  # Public store-audit intake/teaser for the marketing funnel (flag gated, UCP lane only)
app.include_router(store_audit_claim_router)  # AUTHENTICATED claim of an anonymous funnel run (same flag; not under /public/*)
app.include_router(store_readiness_router)  # Merchant-triggered storefront journey: search -> PDP -> cart -> address -> checkout
app.include_router(agent_internal_products_router)  # Thin internal search primitive (/agent/internal/products/search)
app.include_router(agent_management_router)  # Agent management
app.include_router(fulfillment_api_router)  # Fulfillment tracking for agents
app.include_router(refund_api_router)  # Refund processing
app.include_router(agent_docs_router)  # Agent developer docs
app.include_router(fix_orders_table_router)  # Fix orders table structure
app.include_router(agent_metrics_router)  # Agent API metrics and monitoring
app.include_router(agent_keys_router)  # Agent API key management
app.include_router(agent_identity_issuers_router)  # Federated buyer identity: per-agent user-token issuers (portal self-serve)
app.include_router(agent_identity_issuers_internal_router)  # Internal registry the gateway polls (/agent/internal/identity-issuers)
app.include_router(payment_grant_issuers_router)  # Which PSPs may authorize money — ADMIN-only registration (Antom lane)
app.include_router(payment_grant_issuers_internal_router)  # Internal registry the gateway polls (/agent/internal/payment-issuers)
app.include_router(agent_webhooks_router)  # Agent webhook management and deliveries
app.include_router(init_agent_key_router)  # Initialize test agent key
app.include_router(merchant_products_router)  # Merchant product optimization APIs
app.include_router(merchant_citations_router)  # Merchant citation observations (get-cited proof loop, B⑤)
app.include_router(merchant_pdp_router)  # Merchant PDP status and contribution APIs
app.include_router(merchant_risk_router)  # Internal risk APIs (disputes/returns)
app.include_router(shopify_products_sync_router)  # Sync Shopify products into products_cache
app.include_router(platform_products_sync_router)  # Sync non-Shopify platform products into products_cache/catalog
app.include_router(product_enrichment_router)  # Internal product enrichment trigger
app.include_router(performance_optimization_router)  # Performance optimization
app.include_router(quick_index_setup_router)  # Quick setup (no auth)
if DEBUG_MODE:
    app.include_router(debug_usage_logs_router)  # Debug usage logs
    app.include_router(debug_query_analytics_router)  # Debug query analytics
    app.include_router(debug_orders_agent_router)  # Debug orders by agent
app.include_router(agent_metrics_v1_router)  # Stable /agent/v1/metrics aliases
app.include_router(prometheus_metrics_router)  # /metrics (Prometheus scrape)
app.include_router(shopify_setup_router)  # Shopify setup endpoints
app.include_router(shopify_manual_router)  # Shopify manual trigger endpoints
app.include_router(dashboard_api_router)  # New Dashboard API
# payment_routes_router is same as payment_router, already included above
app.include_router(demo_data_router)  # Demo data management
# [DELETED] test_data_router router registration removed (file not in Git)
app.include_router(product_quality_router)  # Internal product quality preview (Merchant Portal)

if OPERATIONS_AVAILABLE:
    app.include_router(operations_router)
    logger.info("✅ Operations router included")

_guard_legacy_psp_maintenance_routes()
_guard_legacy_inmemory_auth_routes()

@app.get("/version")
async def get_version(request: Request):
    """
    返回当前部署的版本信息（Git commit hash）
    优先使用 Railway 环境变量，本地开发时回退到 git 命令
    Redeployed with Shopify/Stripe credentials
    """
    # Build metadata injected by the platform (Railway) or by the deploy
    # (Cloud Run, which publishes none of its own).
    railway_commit = platform_commit_sha()
    railway_branch = platform_git_branch()
    # Computed once: all three return branches below (Railway, local-git,
    # unknown) published settings_contract, so gating only the Railway one
    # would just relocate the disclosure.
    settings_contract_block = (
        {"settings_contract": _settings_contract_payload()}
        if await _probe_caller_is_admin(request)
        else {}
    )
    
    if railway_commit:
        # 在 Railway 上运行
        return {
            "version": railway_commit[:8],  # 短 hash
            "full_sha": railway_commit,
            "branch": railway_branch,
            # RAILWAY_GIT_AUTHOR deliberately NOT returned: it is a named
            # individual's identity, this endpoint is public and
            # unauthenticated, and nothing consumes it (the two in-repo readers
            # take `settings_contract` and the SHA). Publishing who last
            # deployed is gratuitous.
            "environment": "production",
            "status": "healthy",
            # settings_contract is ADMIN-ONLY — see the note on /health. It
            # publishes the exact rate-limit threshold and whether Shopify
            # discount reconciliation is enforcing, neither of which an
            # anonymous caller needs. This is a deliberate NARROWING of a
            # previously public contract: the ops workflow that read it (see
            # docs/monetization/partner_settlement_promotion_runbook.md) now
            # needs an admin token.
            **settings_contract_block,
        }
    
    # 本地开发环境，尝试 git 命令
    try:
        commit = subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=os.path.dirname(__file__),
            stderr=subprocess.DEVNULL
        ).decode('utf-8').strip()
        
        commit_time = subprocess.check_output(
            ['git', 'log', '-1', '--format=%cd', '--date=iso'],
            cwd=os.path.dirname(__file__),
            stderr=subprocess.DEVNULL
        ).decode('utf-8').strip()
        
        return {
            "version": commit,
            "commit_time": commit_time,
            "environment": "local",
            "status": "healthy",
            **settings_contract_block,
        }
    except Exception as e:
        return {
            "version": "unknown",
            "error": str(e),
            "environment": "unknown",
            "status": "healthy",
            **settings_contract_block,
        }

async def startup():
    """Initialize services on startup"""
    logger.info("🚀 Starting Pivota Infrastructure Dashboard...")
    logger.info(
        "🧭 Agent runtime contract: %s",
        {
            "build": _runtime_build_payload(),
            "runtime_contracts": _runtime_contracts_payload(),
            "settings_contract": _settings_contract_payload(),
        },
    )
    
    # Initialize Sentry error tracking (optional)
    try:
        from config.sentry_config import init_sentry
        init_sentry()
    except Exception as e:
        logger.warning(f"⚠️ Sentry initialization skipped: {e}")
    
    _startup_ddl_lock = StartupDdlLock()
    try:
        logger.info("📡 Connecting to database...")
        logger.info(f"   Database URL type: {type(database.url)}")
        logger.info(f"   Database driver: {database.url.scheme if hasattr(database, 'url') else 'unknown'}")
        # Establish DB connection
        if not getattr(database, "is_connected", False):
            await connect_database_with_timeout(15, db=database)
        logger.info("✅ Database connected successfully")

        # Serialize this whole DDL phase (create_all, schema guard, SQL + inline
        # migrations) across concurrent instances/tasks — see db/startup_ddl.py.
        # Released explicitly after the migrations below and, as a safety net,
        # in the outer finally (release() is idempotent).
        await _startup_ddl_lock.acquire()
        
        # Ensure all tables exist (important for PostgreSQL)
        # Reuse db.database's sync engine: building one from database.url
        # picks up the async sqlite driver (sqlite+aiosqlite) and dies with
        # MissingGreenlet when used synchronously.
        from db.database import engine, metadata
        metadata.create_all(engine)
        logger.info("✅ All database tables verified/created")
        
        # Test the connection
        await asyncio.wait_for(database.execute("SELECT 1"), timeout=10)
        logger.info("✅ Database connection test passed")

        # Optional: background photo TTL cleanup (non-blocking).
        try:
            start_photo_cleanup_loop()
        except Exception:
            pass

        # Minimal schema guard (fast; safe for prod "fast mode").
        # Prevents outages when a deploy accidentally skips SQL migrations.
        try:
            from db.schema_guard import ensure_required_schema_light

            schema_guard_timeout = float(os.getenv("SCHEMA_GUARD_STARTUP_TIMEOUT_S", "12"))
            await asyncio.wait_for(ensure_required_schema_light(), timeout=schema_guard_timeout)
            logger.info("✅ Schema guard: critical columns ensured")
        except Exception as schema_guard_err:
            logger.warning(f"Schema guard warning: {schema_guard_err}")

        if _should_skip_heavy_startup_init():
            logger.warning(
                "🟡 Skipping heavy startup DDL/migrations for fast healthcheck. "
                "Set SKIP_HEAVY_STARTUP_INIT=false to run full startup init."
            )
            logger.info("🚀 Application startup complete (fast mode)")
            return
        
        # Run automatic migrations
        try:
            # Add password column to employees table if not exists
            await database.execute("""
                ALTER TABLE employees 
                ADD COLUMN IF NOT EXISTS password VARCHAR(255)
            """)
            logger.info("✅ Migration: password column added to employees table")
        except Exception as migration_error:
            logger.warning(f"Migration warning: {migration_error}")
        
        # Create tables if they don't exist
        logger.info("📋 Creating tables...")
        from db.merchants import merchants, kyb_documents
        from db.merchant_onboarding import merchant_onboarding
        from db.payment_router import payment_router_config
        from db.products import (
            products_cache, api_call_events, order_events, merchant_analytics
        )
        from db.product_quality import product_quality_snapshot
        from db.product_enrichment import product_enrichment
        from db.agent_ranking_log import agent_ranking_log
        from db.agent_product_events import agent_product_events
        from db.agent_ranking_log import agent_ranking_log
        from db.orders import orders
        # Accounts & Orders API: customer-facing accounts tables
        try:
            from db.accounts import (
                shop_users,
                shop_user_memberships,
                shop_login_otps,
                public_order_lookup_logs,
            )
            logger.info("✅ Accounts tables imported for metadata creation")
        except Exception as e:
            logger.warning(f"⚠️ Could not import accounts tables: {e}")
        
        # Create integration tables
        try:
            # No agents DDL here: metadata.create_all above builds agents from the db/agents.py
            # model (imported explicitly at the top of this file), so a raw CREATE TABLE IF NOT
            # EXISTS at this point never creates anything. The one that stood here described a
            # legacy table (name, company, use_case, status, request_count) that prod's table does
            # not have (probe 2026-09-24; routes written against those columns: pivota-backend#2305).

            # Fix missing columns in agents table (2024-10-30)
            logger.info("🔧 Applying database fixes for agents table...")
            try:
                await database.execute("""
                    ALTER TABLE agents ADD COLUMN IF NOT EXISTS total_gmv NUMERIC(12,2) DEFAULT 0
                """)
                await database.execute("""
                    ALTER TABLE agents ADD COLUMN IF NOT EXISTS total_requests INTEGER DEFAULT 0
                """)
                await database.execute("""
                    ALTER TABLE agents ADD COLUMN IF NOT EXISTS total_orders INTEGER DEFAULT 0
                """)
                await database.execute("""
                    ALTER TABLE agents ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMP WITH TIME ZONE
                """)
                logger.info("✅ Added missing columns to agents table")
            except Exception as e:
                logger.warning(f"⚠️ Could not add columns to agents table: {e}")
            
            # Create payments table
            await database.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    payment_id VARCHAR(100) PRIMARY KEY,
                    order_id VARCHAR(100) NOT NULL,
                    payment_intent_id VARCHAR(255) UNIQUE NOT NULL,
                    amount DECIMAL(10, 2) NOT NULL,
                    currency VARCHAR(3) NOT NULL,
                    psp_type VARCHAR(50) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    idempotency_key VARCHAR(255) UNIQUE,
                    agent_id VARCHAR(50),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE,
                    metadata JSONB
                )
            """)

            # Create agent usage logs table (for rate limit & analytics)
            await database.execute("""
                CREATE TABLE IF NOT EXISTS agent_usage_logs (
                    id SERIAL PRIMARY KEY,
                    agent_id VARCHAR(50) NOT NULL,
                    endpoint VARCHAR(255) NOT NULL,
                    method VARCHAR(10) NOT NULL,
                    merchant_id VARCHAR(50),
                    request_id VARCHAR(100) UNIQUE,
                    ip_address VARCHAR(50),
                    user_agent TEXT,
                    status_code INTEGER,
                    response_time_ms INTEGER,
                    error_message TEXT,
                    order_id VARCHAR(50),
                    order_amount NUMERIC(10,2),
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Fix request_id constraint issue (2024-10-30)
            logger.info("🔧 Fixing agent_usage_logs request_id constraint...")
            try:
                # Drop existing constraint
                await database.execute("""
                    ALTER TABLE agent_usage_logs DROP CONSTRAINT IF EXISTS agent_usage_logs_request_id_key
                """)
                # Clean up empty request_ids
                await database.execute("""
                    UPDATE agent_usage_logs SET request_id = NULL WHERE request_id = ''
                """)
                # Allow NULLs for request_id
                await database.execute("""
                    ALTER TABLE agent_usage_logs ALTER COLUMN request_id DROP NOT NULL
                """)
                # Re-add unique constraint (NULLs allowed)
                await database.execute("""
                    ALTER TABLE agent_usage_logs ADD CONSTRAINT agent_usage_logs_request_id_key UNIQUE (request_id)
                """)
                logger.info("✅ Fixed request_id constraint in agent_usage_logs")
            except Exception as e:
                logger.warning(f"⚠️ Could not fix request_id constraint: {e}")

            for statement in (
                "ALTER TABLE agent_usage_logs ADD COLUMN IF NOT EXISTS caller_id VARCHAR(128)",
                "ALTER TABLE agent_usage_logs ADD COLUMN IF NOT EXISTS source_channel VARCHAR(128)",
                "ALTER TABLE agent_usage_logs ADD COLUMN IF NOT EXISTS source_family VARCHAR(64)",
                "ALTER TABLE agent_usage_logs ADD COLUMN IF NOT EXISTS query_source VARCHAR(128)",
                "ALTER TABLE agent_usage_logs ADD COLUMN IF NOT EXISTS protocol_name VARCHAR(64)",
                "ALTER TABLE agent_usage_logs ADD COLUMN IF NOT EXISTS commerce_surface VARCHAR(64)",
                "ALTER TABLE agent_usage_logs ADD COLUMN IF NOT EXISTS llm_provider VARCHAR(64)",
                "ALTER TABLE agent_usage_logs ADD COLUMN IF NOT EXISTS llm_model VARCHAR(128)",
                "ALTER TABLE surface_click_events ADD COLUMN IF NOT EXISTS commerce_surface VARCHAR(64)",
                "ALTER TABLE surface_click_events ADD COLUMN IF NOT EXISTS source_channel VARCHAR(128)",
                "ALTER TABLE surface_click_events ADD COLUMN IF NOT EXISTS source_family VARCHAR(64)",
                "ALTER TABLE surface_click_events ADD COLUMN IF NOT EXISTS query_source VARCHAR(128)",
                "ALTER TABLE surface_click_events ADD COLUMN IF NOT EXISTS agent_id VARCHAR(64)",
                "ALTER TABLE surface_click_events ADD COLUMN IF NOT EXISTS protocol_name VARCHAR(64)",
                "ALTER TABLE surface_click_events ADD COLUMN IF NOT EXISTS llm_provider VARCHAR(64)",
                "ALTER TABLE surface_click_events ADD COLUMN IF NOT EXISTS llm_model VARCHAR(128)",
                "ALTER TABLE surface_click_events ADD COLUMN IF NOT EXISTS caller_id VARCHAR(128)",
                "ALTER TABLE commerce_attribution_edges ADD COLUMN IF NOT EXISTS commerce_surface VARCHAR(64)",
                "ALTER TABLE commerce_attribution_edges ADD COLUMN IF NOT EXISTS source_channel VARCHAR(128)",
                "ALTER TABLE commerce_attribution_edges ADD COLUMN IF NOT EXISTS source_family VARCHAR(64)",
                "ALTER TABLE commerce_attribution_edges ADD COLUMN IF NOT EXISTS query_source VARCHAR(128)",
                "ALTER TABLE commerce_attribution_edges ADD COLUMN IF NOT EXISTS agent_id VARCHAR(64)",
                "ALTER TABLE commerce_attribution_edges ADD COLUMN IF NOT EXISTS protocol_name VARCHAR(64)",
                "ALTER TABLE commerce_attribution_edges ADD COLUMN IF NOT EXISTS llm_provider VARCHAR(64)",
                "ALTER TABLE commerce_attribution_edges ADD COLUMN IF NOT EXISTS llm_model VARCHAR(128)",
                "ALTER TABLE commerce_attribution_edges ADD COLUMN IF NOT EXISTS caller_id VARCHAR(128)",
                "ALTER TABLE commerce_interactions ADD COLUMN IF NOT EXISTS commerce_surface VARCHAR(64)",
                "ALTER TABLE commerce_interactions ADD COLUMN IF NOT EXISTS source_channel VARCHAR(128)",
                "ALTER TABLE commerce_interactions ADD COLUMN IF NOT EXISTS source_family VARCHAR(64)",
                "ALTER TABLE commerce_interactions ADD COLUMN IF NOT EXISTS query_source VARCHAR(128)",
                "ALTER TABLE commerce_interactions ADD COLUMN IF NOT EXISTS agent_id VARCHAR(64)",
                "ALTER TABLE commerce_interactions ADD COLUMN IF NOT EXISTS protocol_name VARCHAR(64)",
                "ALTER TABLE commerce_interactions ADD COLUMN IF NOT EXISTS llm_provider VARCHAR(64)",
                "ALTER TABLE commerce_interactions ADD COLUMN IF NOT EXISTS llm_model VARCHAR(128)",
                "ALTER TABLE commerce_interactions ADD COLUMN IF NOT EXISTS caller_id VARCHAR(128)",
            ):
                try:
                    await database.execute(statement)
                except Exception:
                    pass
            
            # Create agent_merchants table
            await database.execute("""
                CREATE TABLE IF NOT EXISTS agent_merchants (
                    agent_id VARCHAR(50),
                    merchant_id VARCHAR(50),
                    connected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    permissions TEXT,
                    PRIMARY KEY (agent_id, merchant_id)
                )
            """)
            
            await database.execute("""
                CREATE TABLE IF NOT EXISTS merchant_stores (
                    store_id VARCHAR(50) PRIMARY KEY,
                    merchant_id VARCHAR(50) NOT NULL,
                    platform VARCHAR(50) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    domain VARCHAR(255),
                    api_key TEXT,
                    status VARCHAR(50) DEFAULT 'connected',
                    connected_at TIMESTAMP WITH TIME ZONE,
                    last_sync TIMESTAMP WITH TIME ZONE,
                    product_count INTEGER DEFAULT 0,
                    is_primary BOOLEAN DEFAULT FALSE,
                    order_writeback_status TEXT DEFAULT 'disabled',
                    order_writeback_enabled_at TIMESTAMP WITH TIME ZONE,
                    order_writeback_canary_order_id TEXT,
                    order_writeback_last_canary_order_id TEXT,
                    order_writeback_last_verified_at TIMESTAMP WITH TIME ZONE,
                    order_writeback_last_error TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Shopify OAuth anti-replay state store.
            # We store only the SHA256 hash of the `state` sent to Shopify.
            await database.execute("""
                CREATE TABLE IF NOT EXISTS shopify_oauth_states (
                    state_sha256 VARCHAR(64) PRIMARY KEY,
                    merchant_id VARCHAR(50) NOT NULL,
                    shop_domain VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    used_at TIMESTAMP WITH TIME ZONE
                )
            """)
            await database.execute(
                "CREATE INDEX IF NOT EXISTS idx_shopify_oauth_states_lookup ON shopify_oauth_states(merchant_id, shop_domain, expires_at)"
            )

            # One-time install-link tokens (no-login distribution). Tokens are signed and we persist only sha256(jti).
            await database.execute("""
                CREATE TABLE IF NOT EXISTS shopify_install_tokens (
                    jti_sha256 VARCHAR(64) PRIMARY KEY,
                    merchant_id VARCHAR(50) NOT NULL,
                    shop_domain VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    used_at TIMESTAMP WITH TIME ZONE,
                    used_request_id TEXT
                )
            """)
            await database.execute(
                "CREATE INDEX IF NOT EXISTS idx_shopify_install_tokens_lookup ON shopify_install_tokens(merchant_id, shop_domain, expires_at)"
            )
            
            await database.execute("""
                CREATE TABLE IF NOT EXISTS merchant_psps (
                    psp_id VARCHAR(50) PRIMARY KEY,
                    merchant_id VARCHAR(50) NOT NULL,
                    provider VARCHAR(50) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    api_key TEXT,
                    account_id VARCHAR(255),
                    secret_key TEXT,
                    environment VARCHAR(20) DEFAULT 'unknown',
                    provider_config JSONB DEFAULT '{}',
                    validation_status VARCHAR(20) DEFAULT 'unknown',
                    validation_error TEXT,
                    last_validated_at TIMESTAMP WITH TIME ZONE,
                    capabilities TEXT,
                    status VARCHAR(50) DEFAULT 'active',
                    connected_at TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Postgres-only backfill for pre-existing deployments; sqlite lacks
            # ADD COLUMN IF NOT EXISTS and gets these columns from the CREATE above.
            for statement in (
                "ALTER TABLE merchant_psps ADD COLUMN IF NOT EXISTS secret_key TEXT",
                "ALTER TABLE merchant_psps ADD COLUMN IF NOT EXISTS environment VARCHAR(20) DEFAULT 'unknown'",
                "ALTER TABLE merchant_psps ADD COLUMN IF NOT EXISTS provider_config JSONB DEFAULT '{}'::jsonb",
                "ALTER TABLE merchant_psps ADD COLUMN IF NOT EXISTS validation_status VARCHAR(20) DEFAULT 'unknown'",
                "ALTER TABLE merchant_psps ADD COLUMN IF NOT EXISTS validation_error TEXT",
                "ALTER TABLE merchant_psps ADD COLUMN IF NOT EXISTS last_validated_at TIMESTAMP WITH TIME ZONE",
            ):
                try:
                    await database.execute(statement)
                except Exception:
                    pass
            
            # Create indexes
            await database.execute("CREATE INDEX IF NOT EXISTS idx_merchant_stores_merchant_id ON merchant_stores(merchant_id)")
            await database.execute("CREATE INDEX IF NOT EXISTS idx_merchant_psps_merchant_id ON merchant_psps(merchant_id)")
            await database.execute("CREATE INDEX IF NOT EXISTS idx_merchant_psps_provider_status ON merchant_psps(merchant_id, provider, status)")
            
            # Create performance indexes for agent tables
            await database.execute("CREATE INDEX IF NOT EXISTS idx_agent_usage_logs_agent_id_timestamp ON agent_usage_logs(agent_id, timestamp DESC)")
            await database.execute("CREATE INDEX IF NOT EXISTS idx_agent_usage_logs_source_channel_timestamp ON agent_usage_logs(source_channel, timestamp DESC)")
            await database.execute("CREATE INDEX IF NOT EXISTS idx_agent_usage_logs_protocol_name_timestamp ON agent_usage_logs(protocol_name, timestamp DESC)")
            await database.execute("CREATE INDEX IF NOT EXISTS idx_agent_usage_logs_query_source_timestamp ON agent_usage_logs(query_source, timestamp DESC)")
            await database.execute("CREATE INDEX IF NOT EXISTS idx_agents_agent_id ON agents(agent_id)")
            
            logger.info("✅ Integration tables created/verified")
            
            # DISABLED: Auto-initialization of demo merchant (causes duplicate data)
            # Merchants should be created via onboarding flow, not auto-initialized
            
        except Exception as e:
            logger.warning(f"⚠️ Could not create integration tables: {e}")
        from db.agents import agents, agent_usage_logs
        # Ensure accounts tables are part of metadata when running create_all
        try:
            from db.accounts import (
                shop_users,
                shop_user_memberships,
                shop_login_otps,
                public_order_lookup_logs,
            )
            logger.info("✅ Accounts tables included in metadata.create_all")
        except Exception as e:
            logger.warning(f"⚠️ Could not include accounts tables in metadata.create_all: {e}")
        from db.database import metadata, engine
        metadata.create_all(engine)
        logger.info("✅ Tables created:")
        logger.info("   - Core: merchants, kyb_documents, merchant_onboarding, payment_router_config, orders")
        logger.info("   - Agents: agents, agent_usage_logs")
        logger.info("   - Cache: products_cache")
        logger.info("   - Events: api_call_events, order_events")
        logger.info("   - Analytics: merchant_analytics")
        
        # Run SQL migration files.
        # Previously this opened a NEW engine per file and called
        # conn.commit() on a legacy SQLAlchemy 1.4 Connection, which has no
        # such method: every file raised AttributeError, and only files whose
        # text began with a DDL keyword landed (1.4 legacy autocommit) while
        # comment-led files were silently ROLLED BACK — the reason a fresh
        # database had no webhook_events/users/employees. See db/sql_migrations.py.
        # Runs inside the startup advisory lock acquired above.
        logger.info("🔄 Running SQL migration files...")
        try:
            from db.sql_migrations import run_sql_migrations

            # `engine` is db.database's SYNC engine on purpose: database.url may
            # carry an async driver (sqlite+aiosqlite) that a fresh create_engine
            # here would bind sync-side (MissingGreenlet).
            run_sql_migrations(engine)
            logger.info("✅ All SQL migrations completed")
        except Exception as migration_err:
            logger.warning(f"⚠️ SQL migration warning: {migration_err}")

        # startup_event() probed webhook_events BEFORE the migration that
        # creates it had run (first boot on an empty DB), which latched webhook
        # audit persistence off for the process. Re-probe now that migrations
        # are applied.
        try:
            if await WebhookService.recheck_webhook_events_schema():
                logger.info("✅ webhook_events audit persistence enabled")
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"webhook_events re-check skipped: {exc}")
        
        # Run inline migrations for merchant_onboarding table
        logger.info("🔄 Running inline database migrations...")
        try:
            from sqlalchemy import text
            logger.info("   Checking for store_url column...")
            
            # Migration 1: Add store_url column
            check_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='merchant_onboarding' 
                AND column_name='store_url';
            """)
            result = await database.fetch_one(check_query)
            
            if not result:
                logger.info("📝 Adding store_url column to merchant_onboarding...")
                await database.execute(text("""
                    ALTER TABLE merchant_onboarding 
                    ADD COLUMN IF NOT EXISTS store_url VARCHAR(500);
                """))
                await database.execute(text("""
                    UPDATE merchant_onboarding 
                    SET store_url = COALESCE(website, 'https://placeholder.com')
                    WHERE store_url IS NULL;
                """))
                await database.execute(text("""
                    ALTER TABLE merchant_onboarding 
                    ALTER COLUMN store_url SET NOT NULL;
                """))
                logger.info("✅ store_url column added successfully")
            else:
                logger.info("✅ store_url column already exists")
            
            # Migration 2: Add auto-approval columns
            auto_approval_columns = [
                ("auto_approved", "BOOLEAN DEFAULT FALSE"),
                ("approval_confidence", "REAL DEFAULT 0.0"),
                ("full_kyb_deadline", "TIMESTAMP")
            ]
            
            for col_name, col_type in auto_approval_columns:
                check_col = text(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='merchant_onboarding' 
                    AND column_name='{col_name}';
                """)
                col_exists = await database.fetch_one(check_col)
                
                if not col_exists:
                    logger.info(f"📝 Adding {col_name} column to merchant_onboarding...")
                    await database.execute(text(f"""
                        ALTER TABLE merchant_onboarding 
                        ADD COLUMN IF NOT EXISTS {col_name} {col_type};
                    """))
                    logger.info(f"✅ {col_name} column added successfully")

            # Migration 3: MCP columns
            mcp_columns = [
                ("mcp_connected", "BOOLEAN DEFAULT FALSE"),
                ("mcp_platform", "VARCHAR(50)"),
                ("mcp_shop_domain", "VARCHAR(255)"),
                ("mcp_access_token", "TEXT")
            ]
            for col_name, col_type in mcp_columns:
                check_col = text(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='merchant_onboarding' 
                    AND column_name='{col_name}';
                """)
                col_exists = await database.fetch_one(check_col)
                if not col_exists:
                    logger.info(f"📝 Adding {col_name} column to merchant_onboarding...")
                    await database.execute(text(f"""
                        ALTER TABLE merchant_onboarding 
                        ADD COLUMN IF NOT EXISTS {col_name} {col_type};
                    """))
                    logger.info(f"✅ {col_name} column added successfully")

            # Migration 4: Ensure critical columns exist on orders table
            logger.info("   Checking orders table columns...")
            orders_columns = [
                ("shipping_address", "JSONB"),
                ("items", "JSONB"),
                ("client_secret", "TEXT")
            ]
            for col_name, col_type in orders_columns:
                check_col = text(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='orders' 
                    AND column_name='{col_name}';
                """)
                col_exists = await database.fetch_one(check_col)
                if not col_exists:
                    logger.info(f"📝 Adding {col_name} column to orders...")
                    await database.execute(text(f"""
                        ALTER TABLE orders 
                        ADD COLUMN IF NOT EXISTS {col_name} {col_type};
                    """))
                    logger.info(f"✅ {col_name} column added to orders")
            
        except Exception as migration_err:
            logger.warning(f"⚠️ Migration warning (may be already applied): {migration_err}")

        
        # Re-run the light schema guard now that ALL tables exist. The first
        # pass (above) runs before the heavy init creates the remaining
        # tables — on a fresh sqlite dev/test DB its per-column ALTERs no-op
        # against tables that don't exist yet, leaving /health failing closed
        # on missing required columns.
        try:
            from db.schema_guard import ensure_required_schema_light

            post_guard_timeout = float(os.getenv("SCHEMA_GUARD_STARTUP_TIMEOUT_S", "12"))
            await asyncio.wait_for(ensure_required_schema_light(), timeout=post_guard_timeout)
            logger.info("✅ Schema guard: post-init required columns ensured")
        except Exception as schema_guard_err:
            logger.warning(f"Schema guard (post-init) warning: {schema_guard_err}")

        # End of the startup DDL phase. The post-init schema guard above is
        # part of it: db/schema_guard.py issues 24 CREATE INDEX IF NOT EXISTS
        # statements through bare database.execute(), which is exactly as
        # non-atomic across sessions as CREATE TABLE IF NOT EXISTS — and all
        # of them share one try/except, so a racing instance would abort the
        # rest of the guard and leave /health failing closed on a column it
        # was supposed to ensure.
        await _startup_ddl_lock.release()

        # Initialize services if available
        logger.info("🔌 Initializing optional services...")
        logger.info("✅ All services initialized successfully!")
        logger.info("🚀 Application startup complete!")
        logger.info("=" * 80)
        
    except (asyncio.TimeoutError, TimeoutError) as e:
        # Database connection timeout – log but allow app to start
        logger.error("=" * 80)
        logger.error("❌ CRITICAL ERROR during startup: database connection timed out")
        logger.error(f"❌ Error type: {type(e).__name__}")
        logger.error(f"❌ Error details: {str(e)}")
        logger.error("=" * 80)
        logger.error("🟡 Continuing startup with database in DISCONNECTED state")
        logger.error("🟡 Most DB-backed endpoints will fail until the database is reachable again")
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ CRITICAL ERROR during startup: {e}")
        logger.error(f"❌ Error type: {type(e).__name__}")
        logger.error(f"❌ Error details: {str(e)}")
        logger.error("=" * 80)
        import traceback
        traceback.print_exc()
        # Do not block deploy/healthchecks; keep the service up in degraded mode.
        logger.error("🟡 Continuing startup in degraded mode (some DB-backed endpoints may fail)")
        return
    finally:
        # Safety net: the fast-mode `return` and every error path above must
        # drop the advisory lock so a sibling instance's boot is never blocked
        # by this one. Idempotent if already released.
        try:
            await _startup_ddl_lock.release()
        except Exception:  # noqa: BLE001
            pass

async def shutdown():
    """Cleanup on shutdown"""
    try:
        await database.disconnect()
        logger.info("Database disconnected")
        logger.info("🛑 Application shutdown complete")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


@asynccontextmanager
async def app_lifespan(_app: FastAPI):
    # FAIL AT BOOT, not per-request. Every prod/staging guard in this service
    # resolves through config.platform; if the platform cannot tell us which
    # environment this is (a Cloud Run revision deployed without PIVOTA_ENV),
    # the shim answers "production" so the guards stay armed — but running on
    # that guess is not acceptable, so refuse to come up at all. Local dev and
    # the test suite (no Cloud Run marker of EITHER family -- K_* for a service,
    # CLOUD_RUN_JOB/CLOUD_RUN_EXECUTION for a job -- and no RAILWAY_*) resolve
    # "development" and pass straight through.
    resolved_env = require_platform_env()
    # Again, idempotently: a deployment that hands uvicorn its own
    # `--log-config` reconfigures logging AFTER this module was imported, and
    # `dictConfig` on a named logger drops handlers. It does not drop filters,
    # so this is belt and braces rather than the load-bearing call — but the
    # channel it guards carries a live credential, and a second `addFilter` on
    # an already-installed filter is a no-op by construction.
    install_uvicorn_access_log_redaction()
    logger.info("🌍 Platform: %s", platform_metadata())
    logger.info("🌍 Resolved environment: %s", resolved_env)

    await startup_event()
    await startup()
    # Unattended DB repair. /health no longer reconnects as a side effect of
    # being polled (see utils.database_readiness.probe_database_health), and
    # ensure_database_ready only runs on login/quote/order requests — so a
    # process serving read-only traffic had no way back from a degraded
    # start. This task is the way back. It is NOT an APScheduler job on
    # purpose: during the 2026-08-05/06 incident every scheduler job was
    # wedged, and a repair mechanism must not share fate with what it repairs.
    reconnect_supervisor = asyncio.create_task(run_database_reconnect_supervisor())
    try:
        yield
    finally:
        reconnect_supervisor.cancel()
        # Suppress Exception too, not only CancelledError: anything else
        # escaping this await propagates out of the finally and SKIPS
        # shutdown()/shutdown_event() entirely (review round 20).
        with suppress(asyncio.CancelledError, Exception):
            await reconnect_supervisor
        # ORDER IS LOAD-BEARING, and it was the wrong way round.
        #
        # `shutdown()` is `database.disconnect()`, which closes the asyncpg pool.
        # `shutdown_event()` stops the webhook workers and the audit scheduler. Running the
        # disconnect FIRST meant every still-running scheduler job was cut off from the pool
        # while it was being asked to stop: `PostgresConnection.acquire` asserts
        # "DatabaseBackend is not running", so a run that had already made its external call
        # (settlement, refund) then FAILED ITS RECORDING WRITE rather than completing. Worse,
        # once audit_scheduler grew a shutdown drain, that run got seconds of extra life in
        # which to do it, and could reach its own end and be booked `ok` — a silent
        # half-completed run on the money path, where the old behaviour was a prompt cancel.
        #
        # Stopping the producers before tearing down the resource they use is the right order
        # regardless of the drain, and it also removes a pre-existing hazard: asyncpg's
        # `Pool.close()` waits for every holder to be released (it only warns at 60s), and it
        # was being called while every scheduler job still held connections and had not been
        # told to stop.
        await shutdown_event()
        await shutdown()


app.router.lifespan_context = app_lifespan

@app.get("/")
async def root():
    """Service banner, and the SECOND door onto the same honesty guarantee as
    `/health` — so it cannot be the door that stays open.

    This endpoint used to report `"status": "healthy"` and `"health": "OK"` as
    string literals, while the `db_status` field sitting beside them in the very
    same payload said `"disconnected"`. Only `db_status` was ever computed;
    "healthy" was true by construction. Anything reading the status — a monitor,
    a load balancer, a human — got a green answer from a process that could not
    serve a single database-backed request.

    That matters beyond this handler. `/health` was made observe-only and
    503-on-failure precisely so an outage cannot hide behind a green indicator;
    leaving `/` unconditionally healthy would have made that guarantee depend
    on which path the Railway dashboard happens to name. It currently names
    `/health`: a committed Railway API dump (a point-in-time snapshot, not live
    truth) shows nine services set `healthcheckPath` to `/health` and five to
    `null`, none to `/`, with the `api.pivota.cc` service among the nine. So
    this change fixes no active outage — it removes a second door that a single
    dashboard edit could have opened onto the one #1683 closed.

    Two rules hold this together:

    1. OBSERVE, NEVER REPAIR. `probe_database_health` connects nothing and
       resets nothing — if the shared `database` is not connected, that IS the
       answer. It replaces a bare `await database.execute("SELECT 1")`, which
       was a second, weaker copy of the same idiom: no timeout, so a stalled
       backend hung this handler instead of reporting on it. Request-time
       recovery stays in `ensure_database_ready` (auth/quote/order paths) and
       in the unattended supervisor. Reporting the truth does not remove the
       app's ability to heal.
    2. ONE SOURCE OF TRUTH. Every reported field below derives from `db_ok`.
       The original defect was three fields that were supposed to agree and had
       no mechanism forcing them to; a future edit must not be able to fix one
       and leave another lying.

    Connectivity only, deliberately: `/health` additionally gates on the schema
    guard, and `/` keeps the narrower contract its `db_status` field always
    described.

    Two limits, stated so they are not rediscovered during an incident:

    - THE STATUS CODE IS THE SIGNAL; the body's `status` field is not what a
      client sees on failure. `ErrorHandlerMiddleware` re-nests every >=400
      JSON body, so on 503 this payload moves under `error.details` and the
      top-level `status` reads "error". `db_status` — the only field here that
      predates the fix and was always honest — is therefore ABSENT at the top
      level on 503, where it was previously always present. No consumer of it
      is known (see the commit message's sweep), and `/health` already behaves
      this way; the status code is what survives the envelope intact.
    - A SATURATED POOL READS AS DOWN. `probe_database_health` decides from one
      `SELECT 1` under a timeout, and as `_force_mark_database_disconnected`
      notes, that cannot distinguish a dead pool from a slow or saturated one.
      A load spike on a healthy database will flip `/` to 503. That ambiguity
      is inherited from the primitive and `/health` already carries it; this
      change widens its surface to a second path rather than introducing it.
    """
    probe_timeout = _health_timeout_seconds("DB_HEALTH_PROBE_TIMEOUT_SECONDS", 3.0)

    db_error = None
    try:
        await probe_database_health(probe_timeout_seconds=probe_timeout)
        db_ok = True
    except DatabaseUnavailableError as e:
        db_ok = False
        db_error = e.error_type
        logger.error(f"Root health check DB error: {e.phase}/{e.error_type}")

    return JSONResponse(
        status_code=200 if db_ok else 503,
        content={
            "message": "Pivota Infrastructure Dashboard API",
            "version": "0.2.1-fixed",
            "status": "healthy" if db_ok else "unhealthy",
            "db_status": "connected" if db_ok else "disconnected",
            "timestamp": time.time(),
            "health": "OK" if db_ok else "UNAVAILABLE",
            "error": db_error,
        },
    )


def _health_timeout_seconds(name: str, default: float, *, min_value: float = 0.5, max_value: float = 30.0) -> float:
    raw = (os.getenv(name) or "").strip()
    try:
        value = float(raw) if raw else default
    except Exception:
        value = default
    return max(min_value, min(max_value, value))


async def _probe_caller_is_admin(request: Request) -> bool:
    """Is THIS caller an admin? Never raises `Exception`, never blocks the request.

    The public probes below (`/health`, `/version`) must keep answering for
    anonymous callers — Railway's healthcheck reads /health's status code and an
    unauthenticated internal monitor reads both — so authentication here is
    strictly ADDITIVE: it decides how much DIAGNOSTIC DETAIL to include, never
    whether the probe responds. Every failure path returns False, so a malformed
    token degrades to the public view instead of 500-ing an indicator. That
    property is the whole point: an endpoint Railway restarts the service over
    must not gain a new way to fail.
    """
    try:
        # BOTH admin credentials this repo recognises, matching
        # utils.auth.require_admin_or_key: an operator who authenticates with
        # X-ADMIN-KEY would otherwise get the redacted view with no explanation.
        admin_key = (request.headers.get("x-admin-key") or "").strip()
        if admin_key:
            expected = [
                (os.getenv("ADMIN_API_KEY") or "").strip(),
                (os.getenv("PROMOTIONS_ADMIN_KEY") or "").strip(),
            ]
            if any(
                candidate and secrets.compare_digest(admin_key, candidate)
                for candidate in expected
            ):
                return True

        header = request.headers.get("authorization") or ""
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            return False
        from utils.auth import get_current_user

        from fastapi.security import HTTPAuthorizationCredentials

        user = await get_current_user(
            HTTPAuthorizationCredentials(scheme="Bearer", credentials=token.strip())
        )
        return (user or {}).get("role") in ("admin", "super_admin")
    except Exception:
        # Deliberately Exception, not BaseException: CancelledError must still
        # propagate. get_current_user has no await points, so none is
        # deliverable inside this block anyway.
        return False


@app.get("/health")
async def health_check(request: Request):
    """
    Health check endpoint. Railway's healthcheck is understood to point here
    (`healthcheckPath=/health`), but that is a SERVICE SETTING held in Railway,
    not in this repo — nothing in the tree pins it, so re-confirm in the
    dashboard before relying on it.

    Must be fast, deterministic, and READ-ONLY:
      - OBSERVES DB state via probe_database_health — never connects, never
        resets the pool. An indicator that repairs what it measures cannot
        reveal an outage: driven on the pre-fix commit, /health answered 200
        five times out of five against a disconnected backend, having
        reconnected on the first poll. (The 12.5-hour outage of 2026-08-05/06
        is what sent us looking here, but its mechanism was never reproduced
        from this path — motivation, not established root cause.)
      - checks required schema exists (read-only)

    A disconnected backend MUST surface as 503 here. Request-time recovery
    lives in ensure_database_ready, which the auth/quote/order paths still
    call — reporting the truth does not remove the app's ability to heal.
    """
    started_at = time.time()
    db_ok = False
    missing: dict[str, list[str]] = {}
    db_error = None
    probe_timeout = _health_timeout_seconds("DB_HEALTH_PROBE_TIMEOUT_SECONDS", 3.0)
    schema_timeout = _health_timeout_seconds("DB_HEALTH_SCHEMA_TIMEOUT_SECONDS", 5.0)

    try:
        await probe_database_health(probe_timeout_seconds=probe_timeout)
        db_ok = True
    except DatabaseUnavailableError as e:
        db_error = e.error_type

    if db_ok:
        try:
            from db.schema_guard import check_required_schema

            missing = await asyncio.wait_for(check_required_schema(), timeout=schema_timeout)
        except Exception as e:
            db_ok = False
            db_error = type(e).__name__

    healthy = db_ok and not missing
    status_code = 200 if healthy else 503

    # THE VERDICT IS PUBLIC; THE DIAGNOSTICS ARE NOT.
    #
    # Everything a health check owes an anonymous caller is here: the status
    # code (all Railway's healthcheck reads), whether the DB answered, and the
    # error TYPE. What used to ride along was a recon payload: the exact
    # rate-limit threshold, whether Shopify discount reconciliation is
    # enforcing or merely observing, a mounted-route map, and on a degraded
    # response the names of missing schema columns.
    #
    # HOW MUCH EACH ONE ACTUALLY BUYS, stated honestly rather than uniformly:
    #   * shopify_discount_reconciliation_mode, runtime_contracts and the
    #     missing-column NAMES are genuinely new closures — they appear nowhere
    #     else on an anonymous response.
    #   * rate_limit_rpm WAS not, and now is. When this comment was written,
    #     `middleware/rate_limiter.py` stamped `X-RateLimit-Limit:
    #     <rate_limit_rpm>` on every `/agent/*` response whenever any x-api-key
    #     header was present — including 401s and 404s — so removing it from
    #     this body was tidiness rather than containment. That header leak has
    #     since been closed: the middleware now publishes only the limit
    #     ENFORCED on a caller who authenticated, and nothing to anyone else.
    #     Both halves are needed for the closure to hold, so if you are
    #     reinstating either, reinstate this note too.
    #
    # Build identity is deliberately still public: removing it from this body
    # while the middleware stamps X-Service-Commit on every 404 would be
    # theatre, and deploy verification legitimately needs it.
    body = {
        "status": "ok" if healthy else "unhealthy",
        "timestamp": time.time(),
        "elapsed_ms": int((time.time() - started_at) * 1000),
        "db_ok": db_ok,
        "error": db_error,
        "build": _runtime_build_payload(),
        "version": _service_version_payload(),
    }
    # UNCONDITIONAL: presence without content. An operator watching the public
    # probe still learns that schema drift EXISTS and needs a credential only to
    # read which columns — the outage signal survives, the schema map does not.
    # Emitted for admins too, so authenticating never REMOVES a field (a
    # dashboard trending this count must not break the day it gets a token).
    body["missing_columns_count"] = sum(len(v) for v in missing.values())
    if await _probe_caller_is_admin(request):
        body["missing_columns"] = missing
        body["runtime_contracts"] = _runtime_contracts_payload()
        body["settings_contract"] = _settings_contract_payload()

    return JSONResponse(status_code=status_code, content=body)

@app.get("/__build")
async def build_info():
    """
    Deployment introspection (no secrets).
    Useful to confirm which git SHA / deployment is running in prod.
    """
    return {
        **_runtime_build_payload(),
        "timestamp": time.time(),
    }

@app.get("/robots.txt", include_in_schema=False)
async def robots_txt():
    """
    Serve an explicit robots.txt. Most of this host is keyed API surface with no
    indexable content, so crawling stays disallowed - EXCEPT the discovery surface
    this host deliberately publishes anonymously: the curated partner docs and
    OpenAPI spec under /agent/docs/, the /openapi.json alias that redirects there,
    and the OAuth/JWKS metadata under /.well-known/. The old blanket Disallow made
    robots-respecting agent fetchers (Claude, GPTBot, Perplexity) refuse to read
    the exact artifacts the marketing site advertises as proof the surface is
    agent-readable. Keyed SDK clients never consult robots.txt, so nothing here
    affects real integrations either way.

    Allow lines come FIRST: Google resolves by longest-match (Allow wins), but
    Python's urllib.robotparser is first-match-in-order - this ordering satisfies
    both. See cors_preflight_passthrough for why unknown paths 404, not 405.

    PRODUCTION ONLY. Outside production docs_viewer_allowed() serves the FULL
    internal spec anonymously at /openapi.json, so the allowlist below would
    invite crawlers straight to the route map the production guard exists to
    hide. Staging keeps the blanket disallow.
    """
    if not is_production():
        return Response(
            content="User-agent: *\nDisallow: /\n",
            media_type="text/plain",
        )
    return Response(
        content=(
            "User-agent: *\n"
            "Allow: /agent/docs/\n"
            "Allow: /openapi.json\n"
            "Allow: /.well-known/\n"
            "Disallow: /\n"
        ),
        media_type="text/plain",
    )


# Catch-all OPTIONS preflight is handled by the cors_preflight_passthrough
# middleware above (not a route), so unknown paths return 404 instead of 405.

@app.get("/operations", response_class=HTMLResponse)
async def operations_dashboard():
    """Serve the operations dashboard"""
    try:
        with open("templates/operations_dashboard.html", "r") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Operations Dashboard</h1><p>Dashboard template not found</p>", status_code=404)

# Deploy-time configuration probe. ADMIN-ONLY, and presence-only.
#
# It shipped public and unauthenticated ("no auth required" was the docstring),
# which made it a free recon endpoint: it told any caller which PSP/platform
# integrations are live, whether the nightly PSP backfill runs — and it echoed
# four LITERAL values, measured on prod 2026-08-11 as
# `adyen_merchant_account: "WoopayECOM"` plus the Shopify/Wix store URLs and the
# OAuth redirect URI. Naming a real payment-processor merchant account to the
# internet is the leak; the rest is the map an attacker would otherwise have to
# guess at.
#
# The literals were never needed for the endpoint's own stated purpose — its
# `instructions` field says "if any values show NOT SET, add them in Railway",
# i.e. PRESENCE is the entire contract. So values are gone rather than masked:
# nothing here has to decide how many characters of a merchant account are safe
# to publish, and a future reader cannot "helpfully" unmask them.
# NOTE on adyen_merchant_account: config/settings.py hardcodes a non-empty
# default for it, so this field can never report "NOT SET" however the
# environment is configured — the endpoint's own instruction is structurally
# unreachable for that one setting. Pre-existing; left alone here because
# changing a settings default is a different blast radius from closing a
# public endpoint.
_CONFIG_CHECK_SETTINGS = (
    "stripe_secret_key",
    "adyen_api_key",
    "adyen_merchant_account",
    "shopify_access_token",
    "shopify_store_url",
    "shopify_client_id",
    "shopify_client_secret",
    "shopify_redirect_uri",
    "wix_api_key",
    "wix_store_url",
)


@app.get("/config-check", include_in_schema=False)
async def config_check(_admin: Dict[str, Any] = Depends(require_admin)):
    """Admin-only: is each integration env var SET? Never returns its value."""
    from config.settings import settings

    config = {
        name: ("✅ SET" if getattr(settings, name, None) else "❌ NOT SET")
        for name in _CONFIG_CHECK_SETTINGS
    }
    # Not a credential, and the version is load-bearing when diagnosing a
    # metrics discrepancy — but still admin-gated along with everything else.
    config["metrics_query_version"] = settings.metrics_query_version
    config["enable_nightly_psp_id_backfill"] = (
        "✅ ENABLED" if settings.enable_nightly_psp_id_backfill else "❌ DISABLED"
    )
    return {
        "status": "success",
        "message": "Environment variable configuration check",
        "config": config,
        "instructions": "If any values show '❌ NOT SET', add them in Railway Environment Variables and redeploy",
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))  # Railway auto-injects PORT
    keep_alive_timeout = int(
        os.getenv(
            "UVICORN_TIMEOUT_KEEP_ALIVE",
            os.getenv("UVICORN_KEEPALIVE_SECONDS", "75"),
        )
    )
    keep_alive_timeout = max(5, min(600, keep_alive_timeout))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        timeout_keep_alive=keep_alive_timeout,
    )  # reload=False for production

# Force redeploy: 1761914340
