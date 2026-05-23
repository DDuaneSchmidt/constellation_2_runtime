from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_primary_aegis_navigation_matches_workflow_model() -> None:
    nav = _text(NAV)

    assert 'section: "OPERATOR WORKFLOW"' in nav
    for label in ['label: "Dashboard"', 'label: "Candidates"', 'label: "Hypotheses"', 'label: "Captured Trades"', 'label: "System Health"']:
        assert label in nav.split("export const LEGACY_NAVIGATION_REFERENCE", 1)[0]
    for old_primary in [
        'label: "Opportunities"',
        'label: "Edge Lab"',
        'label: "Performance"',
        'label: "Journal"',
        'label: "EOD Queue"',
        'label: "Operator Inbox"',
        'label: "Runtime Truth"',
        'label: "Receipts / Outcomes"',
        'label: "Sleeve Performance"',
        'label: "AI Feedback / EOD-EOW Review"',
        'label: "Feature Completion Audit"',
    ]:
        assert old_primary not in nav.split("export const LEGACY_NAVIGATION_REFERENCE", 1)[0]
    primary_nav = nav.split("export function flattenNavigation", 1)[0]
    assert "Drilldown" not in primary_nav
    dashboard_block = nav.split('id: "aegis_dashboard"', 1)[1].split('id: "aegis_candidates"', 1)[0]
    candidates_block = nav.split('id: "aegis_candidates"', 1)[1].split('id: "research_pipeline"', 1)[0]
    research_block = nav.split('id: "research_pipeline"', 1)[1].split('id: "captured_trades"', 1)[0]
    health_block = nav.split('id: "system_health"', 1)[1].split("],", 1)[0]
    assert "Event Trigger Drilldown" not in dashboard_block
    assert "Receipts / Outcomes Drilldown" not in health_block
    assert "Sleeve Performance Drilldown" not in health_block
    assert "Research Lab Drilldown" not in research_block
    assert "Adaptive Intelligence Drilldown" not in candidates_block


def test_obsolete_primary_labels_are_absent_from_active_ui_copy() -> None:
    active_ui = pages_source_v1(ROOT) + "\n" + _text(NAV)

    forbidden = [
        "AEGIS Advisory",
        "Aegis Advisory",
        "full Aegis",
        "Full Aegis",
        "autonomous trading",
        "auto-submit",
        "broker required",
        "IB execution pipeline",
        "live execution",
    ]
    for phrase in forbidden:
        assert phrase not in active_ui


def test_new_operator_routes_are_exposed_by_shell_and_server() -> None:
    pages = pages_source_v1(ROOT)
    server = _text(SERVER)

    for route in ["/aegis-opportunities", "/aegis-edge-lab", "/aegis-performance", "/aegis-journal", "/aegis-today", "/aegis-review", "/aegis-research", "/aegis-history", "/aegis-operator-cockpit"]:
        assert route in pages
        assert route in server
    assert "renderAegisWorkflowPage" in pages
    assert "fetchAegisOperatorCockpit" in pages


def test_lite_ui_surfaces_runtime_truth_and_manual_workflow_guardrails() -> None:
    pages = pages_source_v1(ROOT)

    assert "REAL_RUNTIME" in pages
    assert "DEMO_ONLY" in pages
    assert "DRY_RUN_ONLY" in pages
    assert "ADVISORY_ONLY" in pages
    assert "GATE_ONLY_NO_TRANSPORT" in pages
    assert "operator-entered trades" in pages
    assert "This is not broker automation" in pages
    assert "Broker submit/transmit" in pages
    assert "Autonomous execution" in pages
    assert "Canonical Operator State" in pages
