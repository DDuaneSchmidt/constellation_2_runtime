from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
SHELL = REPO_ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell"
PAGES = SHELL / "pages" / "index.js"
NAV = SHELL / "navigation_schema.js"
ROUTE_METADATA = SHELL / "pages" / "route_metadata.js"
DOMAIN_CLIENT = SHELL / "domain_client" / "index.js"
MAIN = SHELL / "main.js"
SERVER = REPO_ROOT / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"
MANIFEST = REPO_ROOT / "aegis" / "modules" / "operator_portal" / "aegis.module.yaml"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_performance_nav_item_route_and_metadata_are_registered() -> None:
    nav = _read(NAV)
    metadata = _read(ROUTE_METADATA)
    main = _read(MAIN)

    assert 'id: "aegis_paper_performance"' in nav
    assert 'label: "Performance"' in nav
    assert 'route: "/aegis-paper-performance"' in nav
    assert 'truthOwner: "Portfolio results"' in nav
    operator_nav = nav.split("export const ENGINEERING_NAVIGATION_SCHEMA", 1)[0]
    assert 'id: "aegis_paper_performance"' in operator_nav
    assert '{ workspaceId: "aegis_paper_performance", routes: ["/aegis-paper-performance", "/aegis-sleeve-validation", "/aegis-sleeve-analytics"] }' in nav
    assert 'path: "/aegis-paper-performance"' in metadata
    assert 'id: "aegis_paper_performance"' in metadata
    assert '"aegis_paper_performance"' in main


def test_performance_api_route_is_read_only_and_registered() -> None:
    server = _read(SERVER)
    client = _read(DOMAIN_CLIENT)

    assert "build_paper_performance_report_v1" in server
    assert '"/api/aegis/performance/latest"' in server
    assert '"/api/aegis/performance/advisor-benchmark"' in server
    assert '"/api/aegis/performance-report"' in server
    assert "HTTPStatus.OK" in server
    assert "fetchAegisPerformanceReport" in client
    assert "fetchAegisSleeveAnalytics" in client
    assert "saveAegisAdvisorBenchmark" in client
    assert 'query("/api/aegis/performance/latest", params)' in client
    assert 'query("/api/aegis/sleeve-analytics/latest", params)' in client
    assert 'postJson("/api/aegis/performance/advisor-benchmark", payload)' in client


def test_performance_page_renders_operator_first_sections_and_states() -> None:
    pages = _read(PAGES)
    start = pages.find("function renderAegisPaperPerformancePage()")
    end = pages.find("function renderAegisPositionReviewPage()", start)
    performance_fn = pages[start:end]

    assert "function buildPerformanceOperatorModel" in performance_fn
    assert "NORMAL" in performance_fn
    assert "NO_DATA" in performance_fn
    assert "PARTIAL_DATA" in performance_fn
    assert "DEGRADED" in performance_fn
    assert "How are we doing" not in performance_fn  # Screen answers this through the headline/summary, not a slogan.
    assert "Is performance data complete?" in performance_fn
    assert "What is working and what is not working?" in performance_fn
    assert "Benchmark comparison" in performance_fn
    assert "What trends matter?" in performance_fn
    assert "Do I need to do anything?" in performance_fn
    assert "View performance evidence" in performance_fn
    assert "Total P&L" in performance_fn
    assert "Realized P&L" in performance_fn
    assert "Active Research Positions" in performance_fn
    assert "Current Mark-to-Market Research Outcome" in performance_fn
    assert "Sleeve / Strategy Performance" in performance_fn
    assert "Completeness" in performance_fn
    assert "No performance action is required" in performance_fn
    assert "Full portfolio P&L" in performance_fn
    assert "Active paper research positions referenced by the performance source." in performance_fn
    assert "Canonical P&L is not certified because current target-day marks are missing." in performance_fn
    assert "Trend status" in performance_fn
    assert "Strategy performance limited" in performance_fn
    assert "Benchmark stale" in performance_fn
    assert "Attribution exists, but contributor rankings cannot be trusted until mark coverage is complete." in performance_fn


def test_performance_page_excludes_other_workflows_and_engineering_language() -> None:
    pages = _read(PAGES)
    start = pages.find("function renderAegisPaperPerformancePage()")
    end = pages.find("function renderAegisPositionReviewPage()", start)
    performance_fn = pages[start:end]

    assert "Candidate Outcomes" not in performance_fn
    assert "Decision Quality" not in performance_fn
    assert "Sleeve Analytics" not in performance_fn
    assert "AEGIS_SLEEVE_ANALYTICS_V1" not in performance_fn
    assert "surface contract" not in performance_fn.lower()
    assert "semantic invariant" not in performance_fn.lower()
    assert "runtime truth" not in performance_fn.lower()
    assert "repair command" not in performance_fn.lower()
    assert "system health" not in performance_fn.lower()
    assert "candidate capture" not in performance_fn.lower()
    assert "research workflow" not in performance_fn.lower()
    assert "new accounting layer" not in performance_fn
    assert "rebuilt_on_request" not in performance_fn


def test_performance_renders_attribution_benchmarks_and_trends_as_summaries() -> None:
    pages = _read(PAGES)
    start = pages.find("function renderAegisPaperPerformancePage()")
    end = pages.find("function renderAegisPositionReviewPage()", start)
    performance_fn = pages[start:end]

    assert "payload.position_attribution" in performance_fn
    assert "performanceContributorModel" in performance_fn
    assert "renderPerformanceContributorList" in performance_fn
    assert "renderPerformanceBenchmarkCard(\"Aegis Paper\"" in performance_fn
    assert "renderPerformanceBenchmarkCard(\"SPY\"" in performance_fn
    assert "renderPerformanceBenchmarkCard(\"Advisor\"" in performance_fn
    assert "performanceTrendModel" in performance_fn
    assert "payload.sleeves" not in performance_fn


def test_performance_benchmark_comparison_is_read_only_on_page() -> None:
    pages = _read(PAGES)
    start = pages.find("function renderAegisPaperPerformancePage()")
    end = pages.find("function renderAegisPositionReviewPage()", start)
    performance_fn = pages[start:end]

    assert "Benchmark comparison" in performance_fn
    assert "Broad-market comparison if connected" in performance_fn
    assert "Manual advisor comparison if connected" in performance_fn
    assert "data-advisor-benchmark-form" not in performance_fn
    assert "Save advisor benchmark" not in performance_fn

def test_positions_workflow_route_remains_separate() -> None:
    pages = _read(PAGES)

    assert 'case "aegis_positions":' in pages
    assert "return renderPositionsWorkspace();" in pages
    assert 'case "aegis_positions_diagnostics":' in pages
    assert "return renderPositionsDiagnosticsWorkspace();" in pages


def test_operator_manifest_declares_performance_surface_without_accounting_layer() -> None:
    manifest = _read(MANIFEST)

    assert '"surface_id": "paper_performance"' in manifest
    assert '"route": "/aegis-paper-performance"' in manifest
    assert "paper_pnl_report_v1" in manifest
    assert "daily_paper_performance_v1" in manifest
    assert "sleeve_performance_truth_v1" in manifest
    assert "aegis_sleeve_analytics_v1" in manifest
    assert "aegis:sleeve-analytics" in manifest
    assert "candidate_lifecycle_projection_v1" in manifest
    assert "read_only_performance_reporting_over_existing_read_models_no_accounting_layer" in manifest


def test_advisor_benchmark_input_form_renders_and_posts_without_cockpit_reload() -> None:
    pages = _read(PAGES)
    main = _read(MAIN)

    assert "Advisor Benchmark Input" in pages
    assert "data-advisor-benchmark-form" in pages
    assert 'name="as_of_date"' in pages
    assert 'name="period_type"' in pages
    assert 'name="return_pct"' in pages
    assert 'name="source"' in pages
    assert 'name="notes"' in pages
    assert "saveAegisAdvisorBenchmark" in main
    assert "advisorBenchmarkForm" in main
    assert "Saved advisor benchmark" in main
    assert "field_errors" in main
    assert "advisor-benchmark-save" in main
    assert "operator-cockpit" not in main[main.find("advisorBenchmarkForm"):main.find("const candidateActionForm", main.find("advisorBenchmarkForm"))]


def test_benchmark_comparison_displays_advisor_metadata_after_save() -> None:
    pages = _read(PAGES)

    assert "row.period_type" in pages
    assert "row.as_of_date" in pages
    assert "row.notes" in pages
    assert "Manual advisor comparison if connected" in pages
    assert "Not connected" in pages



def test_sleeve_analytics_route_nav_and_metadata_are_registered() -> None:
    nav = _read(NAV)
    metadata = _read(ROUTE_METADATA)
    main = _read(MAIN)
    server = _read(SERVER)

    assert 'id: "aegis_sleeve_analytics"' in nav
    assert 'label: "Sleeve Analytics"' in nav
    assert 'route: "/aegis-sleeve-analytics"' in nav
    assert 'truthOwner: "aegis_sleeve_analytics_v1"' in nav
    assert '"/aegis-sleeve-analytics"' in nav
    assert 'path: "/aegis-sleeve-analytics"' in metadata
    assert 'id: "aegis_sleeve_analytics"' in metadata
    assert '"aegis_sleeve_analytics"' in main
    assert '"/aegis-sleeve-analytics"' in server


def test_sleeve_analytics_page_reads_canonical_endpoint_only() -> None:
    pages = _read(PAGES)
    client = _read(DOMAIN_CLIENT)
    start = pages.find("function renderAegisSleeveAnalyticsPage()")
    end = pages.find("function sleeveClosedTradeMetric", start)
    page = pages[start:end]

    assert "fetchAegisSleeveAnalytics(routeParams)" in page
    assert 'query("/api/aegis/sleeve-analytics/latest", params)' in client
    assert "fetchAegisPerformanceReport" not in page
    assert "safeList(payload.sleeves)" in page
    assert "position_attribution" not in page
    assert "paper_pnl_report_v1" not in page
    assert "sleeve_performance_truth_v1" not in page


def test_sleeve_analytics_page_renders_required_phase1_fields() -> None:
    pages = _read(PAGES)
    start = pages.find("function renderAegisSleeveAnalyticsPage()")
    end = pages.find("function sleeveClosedTradeMetric", start)
    page = pages[start:end]

    assert "Sleeve Analytics" in page
    assert "Artifact: AEGIS_SLEEVE_ANALYTICS_V1" in page
    assert "Status:" in page
    assert "As of:" in page
    assert "Total Sleeves" in page
    assert "Active Sleeves" in page
    assert "Total P&L" in page
    assert "Realized P&L" in page
    assert "Unrealized P&L" in page
    assert "Best Sleeve" in page
    assert "Worst Sleeve" in page
    assert "Mark Coverage" in page
    assert "Attribution Coverage" in page
    assert "Data Quality" in page
    assert "Sleeve Scorecard" in page
    assert "Profit Factor" in page
    assert "Current Exposure" in pages
    assert "Trade Quality" in pages


def test_sleeve_analytics_diagnostics_are_collapsed_and_closed_trade_copy_is_clear() -> None:
    pages = _read(PAGES)
    start = pages.find("function renderAegisSleeveAnalyticsPage()")
    end = pages.find("function renderBenchmarkCard", start)
    page = pages[start:end]

    assert "<details class=\"operator-disclosure\">" in page
    assert "Sleeve Analytics diagnostics" in page
    assert "Missing sleeve attribution" in page
    assert "Stale inputs" in page
    assert "Null metric reasons" in page
    assert "Closed-trade analytics unavailable until exits are recorded." in page
    assert "No closed trades" in page


def test_sleeve_analytics_page_has_no_composite_score_or_unknown_current_fixture() -> None:
    pages = _read(PAGES)
    start = pages.find("function renderAegisSleeveAnalyticsPage()")
    end = pages.find("function renderBenchmarkCard", start)
    page = pages[start:end]

    assert "Composite" not in page
    assert "0-100" not in page
    assert "UNKNOWN sleeve bucket" not in page


def test_sleeve_analytics_page_renders_silent_sleeve_evaluation() -> None:
    pages = _read(PAGES)
    start = pages.find("function renderAegisSleeveAnalyticsPage()")
    end = pages.find("function sleeveClosedTradeMetric", start)
    page = pages[start:end]

    assert "Silent Sleeve Evaluation" in page
    assert "aegis_sleeve_evaluation_v1" in page
    assert "silent_sleeve_evaluation" in page
    assert "Silent sleeve count" in page
    assert "Correctly silent" in page
    assert "Runtime failures" in page
    assert "Data-blocked sleeves" in page
    assert "Blocked" in page
    assert "Needs investigation" in page
    assert "Classification" in page
    assert "Repair action" in page
    assert "Missing inputs" in page
    assert "Run npm run aegis:sleeve-evaluation" in page


def test_operator_manifest_declares_silent_sleeve_evaluation() -> None:
    manifest = _read(MANIFEST)

    assert "aegis_sleeve_evaluation_v1" in manifest
    assert "aegis:sleeve-evaluation" in manifest
    assert "reports/aegis_sleeve_evaluation_v1/{day}/sleeve_evaluation.v1.json" in manifest
    assert "read_only_diagnostic_no_sleeve_logic_change_no_candidate_creation_no_trade_advice_no_trading_gate_change" in manifest
    assert '"surface_id": "sleeve_analytics"' in manifest


def test_sleeve_validation_route_nav_metadata_and_server_are_registered() -> None:
    nav = _read(NAV)
    metadata = _read(ROUTE_METADATA)
    main = _read(MAIN)
    server = _read(SERVER)
    manifest = _read(MANIFEST)

    assert 'id: "aegis_sleeve_validation"' in nav
    assert 'label: "Sleeve Validation"' in nav
    assert 'route: "/aegis-sleeve-validation"' in nav
    assert 'truthOwner: "sleeve_performance_truth_v1"' in nav
    assert 'path: "/aegis-sleeve-validation"' in metadata
    assert 'id: "aegis_sleeve_validation"' in metadata
    assert '"aegis_sleeve_validation"' in main
    assert '"/aegis-sleeve-validation"' in server
    assert '"surface_id": "sleeve_validation"' in manifest
    assert '"route": "/aegis-sleeve-validation"' in manifest


def test_sleeve_validation_page_renders_required_question_table_and_statuses() -> None:
    pages = _read(PAGES)
    start = pages.find("const SLEEVE_VALIDATION_FACTORIES")
    end = pages.find("function renderAegisPositionReviewPage()", start)
    page = pages[start:end]

    assert "Which sleeves are working, which are not working, and which are underpowered?" in page
    assert "Paper research positions are research observations, not investment recommendations." in page
    for column in [
        "Sleeve",
        "Factory Classification",
        "Active Positions",
        "Closed Positions",
        "Net P&L",
        "Unrealized P&L",
        "Realized P&L",
        "Win Rate",
        "Average Gain",
        "Average Loss",
        "Expected Value",
        "Profit Factor",
        "Benchmark Excess Return",
        "Max Drawdown",
        "Sample Status",
        "Evidence Status",
        "Recommended Action",
    ]:
        assert column in page
    for status in ["ZERO_SAMPLE", "UNDERPOWERED", "BUILDING_SAMPLE", "SUFFICIENT_SAMPLE"]:
        assert status in page
    for status in ["POSITIVE_EVIDENCE", "NEGATIVE_EVIDENCE", "INCONCLUSIVE", "BASELINE_FAIL", "BENCHMARK_STALE", "DATA_INCOMPLETE"]:
        assert status in page
    for action in ["CONTINUE_OBSERVATION", "INCREASE_RESEARCH_ATTENTION", "REDUCE_RESEARCH_ATTENTION", "HOSTILE_REVIEW_REQUIRED", "RETIRE_CANDIDATE", "DO_NOT_USE_FOR_CAPITAL"]:
        assert action in page
    for factory in ["ALPHA_DISCOVERY", "TECHNICAL_STRATEGY", "INVESTMENT_PROCESS", "ALLOCATION", "UNCLASSIFIED"]:
        assert factory in page
    assert "?sort=${escapeHtml(key)}" in page
    for sort_key in ["net_pnl", "expected_value", "benchmark_excess_return", "sample_status", "evidence_status"]:
        assert sort_key in page
    assert "ticker contributors are shown after sleeve validation" in page.lower()
    assert "No investable edge is claimed" in page
    assert "factory_missing" in page

