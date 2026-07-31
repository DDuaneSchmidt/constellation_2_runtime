from __future__ import annotations

import json
import re
from pathlib import Path

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server

ROOT = Path(__file__).resolve().parents[4]
TRUTH = Path("/home/node/constellation_runtime_data/truth")
ROUTE_METADATA = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js"
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
DAY = "2026-05-30"
EXPECTED_OPEN_POSITIONS = 36


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact(family: str, filename: str) -> dict:
    return _json(TRUTH / "reports" / family / DAY / filename)


def _latest_day_for_family(family: str) -> str:
    root = TRUTH / "reports" / family
    return sorted(path.name for path in root.iterdir() if path.is_dir())[-1]


def test_no_day_browser_shared_fact_routes_use_latest_fact_day_not_stale_operator_session() -> None:
    for family in [
        "aegis_canonical_operator_state_v1",
        "aegis_candidate_lifecycle_projection_v1",
        "aegis_paper_pnl_report_v1",
        "aegis_position_review_brief_v1",
    ]:
        assert server._operator_shared_fact_day_v1(None, family) == _latest_day_for_family(family)
    assert server._operator_shared_fact_day_v1("2026-05-26", "aegis_candidate_lifecycle_projection_v1") == "2026-05-26"


def test_no_day_browser_shared_fact_endpoints_do_not_use_current_operator_truth_source_day() -> None:
    server_source = _text(SERVER)
    for endpoint in [
        'if path in {"/api/aegis/positions", "/api/aegis/positions/latest"}:',
        'if path in {"/api/aegis/performance/latest", "/api/aegis/performance-report", "/api/aegis/performance-report/latest"}:',
        'if path in {"/api/aegis/position-review", "/api/aegis/position-review/latest"}:',
        'if path == "/api/aegis/operator/today":',
    ]:
        block = server_source.split(endpoint, 1)[1].split('return True', 1)[0]
        assert '_operator_shared_fact_day_v1(' in block
        assert 'current_truth.get("source_day")' not in block


def test_screen_to_source_registry_declares_open_position_owners() -> None:
    routes = _text(ROUTE_METADATA)
    assert "OPERATOR_SHARED_FACT_SOURCE_REGISTRY" in routes
    assert "open_positions" in routes
    for route in [
        "/aegis-command-center",
        "/aegis-positions",
        "/aegis-position-review",
        "/aegis-performance",
        "/aegis-paper-performance",
    ]:
        assert route in routes
    for family in [
        "aegis_candidate_lifecycle_projection_v1",
        "aegis_paper_position_ledger_v1",
        "aegis_paper_pnl_report_v1",
        "aegis_position_review_brief_v1",
    ]:
        assert family in routes


def test_authoritative_open_position_sources_agree_on_fixture_day() -> None:
    lifecycle = _artifact("aegis_candidate_lifecycle_projection_v1", "candidate_lifecycle_projection.v1.json")
    ledger = _artifact("aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json")
    pnl = _artifact("aegis_paper_pnl_report_v1", "paper_pnl_report.v1.json")
    review = _artifact("aegis_position_review_brief_v1", "position_review_brief.v1.json")
    values = {
        "candidate_lifecycle_projection.open_paper_positions.length": len(lifecycle.get("open_paper_positions") or []),
        "paper_position_ledger.open_positions.length": len(ledger.get("open_positions") or []),
        "paper_pnl_report.open_position_count": int(pnl.get("open_position_count") or 0),
        "position_review_brief.summary.open_position_count": int((review.get("summary") or {}).get("open_position_count") or 0),
    }
    assert values == {key: EXPECTED_OPEN_POSITIONS for key in values}, values


def test_rendered_screen_endpoints_resolve_open_positions_from_registered_sources() -> None:
    pages = _text(PAGES)
    server = _text(SERVER)
    assert 'fetchAegisOperatorToday(routeParams)' in pages
    assert 'fetchAegisPositions(routeParams)' in pages
    assert 'fetchAegisPositionReviewBrief(routeParams)' in pages
    assert 'fetchAegisPerformanceReport(routeParams)' in pages
    assert '_positions_lightweight_payload_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)' in server
    assert 'load_position_review_brief_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)' in server
    assert 'build_paper_performance_report_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)' in server
    assert '"/api/aegis/operator/today"' in pages or 'fetchAegisOperatorToday' in pages


def test_legacy_performance_route_cannot_render_legacy_trade_evaluation_count() -> None:
    pages = _text(PAGES)
    route_block = pages.split("export async function loadRouteView", 1)[1]
    for route_id in ["aegis_performance", "aegis_review"]:
        block = route_block.split(f'case "{route_id}":', 1)[1].split("case ", 1)[0]
        assert 'renderAegisPaperPerformancePage()' in block
        assert 'renderAegisWorkflowPage("performance")' not in block


def test_performance_template_renders_degraded_but_allowed_performance_body() -> None:
    pages = _text(PAGES)
    analytics_block = pages.split("function AnalyticsTemplate", 1)[1].split("function ReviewTemplate", 1)[0]
    assert 'surfaceId === "performance" && row.render_allowed === true' in analytics_block
    assert 'semanticInvariantFailures(row).length === 0' in analytics_block
    assert 'options.bodyHtml || ""' in analytics_block
    performance_block = pages.split("function renderAegisPaperPerformancePage", 1)[1].split("function renderAegisPositionReviewPage", 1)[0]
    assert 'renderPerformanceFactCard("Open positions", performancePlain(model.openPositions)' in performance_block
    assert 'const openPositions = performanceNumber(overview.open_positions);' in performance_block


def test_screenshot_regression_registry_has_count_fix_proof_images() -> None:
    routes = _text(ROUTE_METADATA)
    for rel_path in [
        "docs/screenshots/aegis_positions_2026-05-30-count-fix.png",
        "docs/screenshots/aegis_position_review_2026-05-30-count-fix.png",
        "docs/screenshots/aegis_today_2026-05-30-count-fix.png",
        "docs/screenshots/aegis_performance_2026-05-30-count-fix.png",
        "docs/screenshots/aegis_paper_performance_2026-05-30-count-fix.png",
        "docs/screenshots/aegis_today_2026-05-30-truth-hardening.png",
        "docs/screenshots/aegis_positions_2026-05-30-truth-hardening.png",
        "docs/screenshots/aegis_position_review_2026-05-30-truth-hardening.png",
        "docs/screenshots/aegis_performance_2026-05-30-truth-hardening.png",
        "docs/screenshots/aegis_today_2026-05-30-open-positions-browser-fix.png",
        "docs/screenshots/aegis_positions_2026-05-30-open-positions-browser-fix.png",
        "docs/screenshots/aegis_position_review_2026-05-30-open-positions-browser-fix.png",
        "docs/screenshots/aegis_performance_2026-05-30-open-positions-browser-fix.png",
    ]:
        assert rel_path in routes
        path = ROOT / rel_path
        assert path.exists(), rel_path
        assert path.stat().st_size > 10_000, rel_path


def test_registry_values_are_not_legacy_or_local_fallbacks() -> None:
    routes = _text(ROUTE_METADATA)
    assert 'forbidden_legacy_renderers' in routes
    assert 'renderAegisWorkflowPage(\\"performance\\")' in routes
    assert 'replacement_call: "renderAegisPaperPerformancePage()"' in routes
    assert 'expected_value: 36' in routes
    assert 'screenshot_regression' in routes
