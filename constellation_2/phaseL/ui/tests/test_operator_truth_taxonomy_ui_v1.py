from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"


def _pages() -> str:
    return PAGES.read_text(encoding="utf-8")


def _main() -> str:
    return MAIN.read_text(encoding="utf-8")


def test_operator_truth_model_defines_required_taxonomy_labels() -> None:
    pages = _pages()
    for label in [
        "Monitoring only",
        "Blocked from acting",
        "Degraded",
        "Healthy",
        "Data incomplete",
        "Review only",
    ]:
        assert label in pages
    assert "function buildOperatorTruthModel" in pages
    assert "runtimeStatus === \"BLOCKED\"" in pages
    assert "const primaryStatus = paperMode ? \"PAPER MODE\"" in pages


def test_policy_capability_blocks_are_not_david_actions() -> None:
    pages = _pages()
    assert "function operatorTruthIsPolicyCapabilityBlock" in pages
    assert "function operatorTruthIsDavidAction" in pages
    helper = pages.split("function operatorTruthIsDavidAction", 1)[1].split("function operatorTruthDavidActionSummary", 1)[0]
    assert "!operatorTruthIsPolicyCapabilityBlock(row)" in helper
    for capability in ["MANUAL_TRADE_CAPTURE_ALLOWED", "TRADE_ADVICE_ALLOWED", "BROKER_SUBMIT_TRANSMIT", "AUTONOMOUS_EXECUTION_ALLOWED"]:
        assert capability in pages


def test_today_and_system_health_share_david_action_taxonomy() -> None:
    pages = _pages()
    today_block = pages.split("async function renderCommandCenterWorkspace", 1)[1].split("function positionsCaptureSummary", 1)[0]
    health_action_block = pages.split("function renderSystemHealthOperatorActionState", 1)[1].split("function renderSystemHealthDataAvailability", 1)[0]
    assert "buildOperatorTruthModel(payload)" in today_block
    assert "renderCommandCenterStatusHeader(payload, truth)" in today_block
    assert "operatorTruthDavidActionSummary(operatorRows)" in health_action_block
    assert "No David action is currently required" in health_action_block
    assert "policy/capability" in health_action_block


def test_position_review_surface_ready_is_secondary_to_runtime_truth() -> None:
    pages = _pages()
    block = pages.split("function renderAegisPositionReviewPage", 1)[1].split("function renderPositionReviewBriefCard", 1)[0]
    assert "fetchAegisOperatorToday(routeParams)" in block
    assert "buildOperatorTruthModel(todayPayload, payload)" in block
    assert "topReadinessLabel: operatorTruth.primaryStatus" in block
    assert "Surface/canonical status is secondary to runtime truth." in block
    assert "Brief artifact:" in block


def test_research_followups_are_research_only_not_trading_actions() -> None:
    pages = _pages()
    model_block = pages.split("function buildResearchOperatorModel", 1)[1].split("function researchStateCopy", 1)[0]
    render_block = pages.split("function renderResearchOperatorPage", 1)[1].split("function renderResearchFactCard", 1)[0]
    assert "operatorTruth.actionLabel" in model_block
    assert "topReadinessLabel: operatorTruth.runtimeBlocked ? operatorTruth.primaryStatus" in model_block
    assert "No trading action or broker action is enabled from Research." in render_block
    assert "Research-only tasks; not trading actions." in render_block


def test_shell_chrome_uses_operator_truth_before_local_readiness() -> None:
    main = _main()
    assert "state.activeView?.operatorTruth" in main
    assert "truth.primaryStatus || state.activeView?.topReadinessLabel" in main
    assert "truth.primaryStatus || view.topReadinessLabel" in main
    assert "Monitoring only" in main


def test_blocker_counts_are_labeled_by_taxonomy() -> None:
    pages = _pages()
    health_block = pages.split("async function renderEngineeringDashboardWorkspace", 1)[1].split("function renderAegisTodayWorkflow", 1)[0]
    blocker_block = pages.split("function renderSystemHealthBlockers", 1)[1].split("function renderSystemHealthDegraded", 1)[0]
    assert "Operator blockers" in health_block
    assert "Fix-first items" in health_block
    assert "Runtime capability blockers" in pages
    assert "Operator blockers are distinct from runtime capability blockers, root causes, and fix-first recovery items." in blocker_block



def test_today_route_has_route_query_params_helper() -> None:
    pages = _pages()
    assert "function routeQueryParams()" in pages
    today_block = pages.split("async function renderCommandCenterWorkspace", 1)[1].split("function positionsCaptureSummary", 1)[0]
    assert "const routeParams = routeQueryParams();" in today_block
    for missing_helper in ["renderTodayReadinessPanel", "renderTodayActionInbox", "renderTodayBlockers", "renderResearchQueuePreview"]:
        assert missing_helper not in today_block
    for helper in ["renderCommandCenterPrimaryOverview(payload)", "renderCommandCenterValidationPipeline(payload)", "renderCommandCenterRunSummary(payload)", "renderCommandCenterSafetyStrip(payload)"]:
        assert helper in today_block
    assert "renderTodayActivity(payload)" not in today_block


def test_surface_ready_copy_is_not_primary_visible_content() -> None:
    pages = _pages()
    ready_block = pages.split("function renderOperatorSurfaceContractReadyBanner", 1)[1].split("function renderOperatorSurfaceContractReadyDiagnostics", 1)[0]
    assert "Surface readiness diagnostics" in ready_block
    assert "Surface checks passed" not in pages
    assert "Normal workflow may render" not in pages
    assert "Primary workflow may render" not in pages


def test_research_lab_disables_autonomous_and_labels_followups() -> None:
    pages = _pages()
    card_block = pages.split("function renderHypothesisCard", 1)[1].split("function emptyHypothesisViewSection", 1)[0]
    lab_block = pages.split("async function renderResearchLabPage", 1)[1].split("function _configFieldValue", 1)[0]
    assert "Research follow-up" in card_block
    assert "Research follow-up required; no trading, broker, or autonomous action." in card_block
    assert "Autonomous execution" in card_block
    assert "Disabled by policy" in card_block
    assert "Autonomous engine" not in card_block
    assert "Research follow-ups only; no David trading action required." in lab_block
    assert "No broker action is enabled from Research." in lab_block


def test_system_health_primary_headline_comes_from_operator_truth() -> None:
    pages = _pages()
    status_block = pages.split("function renderSystemHealthStatus", 1)[1].split("function renderSystemHealthTopIssue", 1)[0]
    assert 'const truth = buildOperatorTruthModel(verifiedRuntime, queue);' in status_block
    assert '<h2>${escapeHtml(truth.primaryStatus || "Monitoring only")}</h2>' in status_block
    assert "Technical health" in status_block
    assert "operatorPlainLabel(technicalState)" in status_block
    assert "systemHealthHeadline(state)" not in status_block
    assert "operatorPlainLabel(state)" not in status_block


def test_engineering_troubleshooting_template_renders_truth_body_before_contract_gate() -> None:
    pages = _pages()
    template_block = pages.split("function TroubleshootingTemplate", 1)[1].split("function renderContractGatedPage", 1)[0]
    assert 'surfaceId === "engineering"' in template_block
    assert 'options.bodyHtml || ""' in template_block
    assert "renderOperatorSurfaceContractState" not in template_block



def test_performance_routes_use_authoritative_paper_performance_count_path() -> None:
    pages = _pages()
    performance_block = pages.split("function renderAegisPaperPerformancePage", 1)[1].split("function renderAegisPositionReviewPage", 1)[0]
    route_block = pages.split("export async function loadRouteView", 1)[1]
    analytics_block = pages.split("function AnalyticsTemplate", 1)[1].split("function ReviewTemplate", 1)[0]
    assert 'fetchAegisPerformanceReport(routeParams)' in performance_block
    assert 'const openPositions = performanceNumber(overview.open_positions);' in performance_block
    assert 'renderPerformanceFactCard("Open positions", performancePlain(model.openPositions)' in performance_block
    assert 'case "aegis_performance":\n      return renderAegisPaperPerformancePage();' in route_block
    assert 'case "aegis_review":\n      return renderAegisPaperPerformancePage();' in route_block
    assert 'surfaceId === "performance" && row.render_allowed === true && semanticInvariantFailures(row).length === 0' in analytics_block
    assert 'options.bodyHtml || ""' in analytics_block
