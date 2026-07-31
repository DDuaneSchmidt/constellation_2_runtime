from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_primary_aegis_navigation_matches_ai_cio_model() -> None:
    nav = _text(NAV)

    assert 'section: "AI CIO"' in nav
    primary_nav = nav.split("export const ENGINEERING_NAVIGATION_SCHEMA", 1)[0]
    for label in ['label: "CIO Briefing"', 'label: "Capital Map"', 'label: "Portfolios"', 'label: "Research Lab"', 'label: "Opportunities"', 'label: "Retirement Simulator"', 'label: "Advisor Oversight"', 'label: "Documents"', 'label: "Carolyn"']:
        assert label in primary_nav
    for old_primary in [
        'label: "Dashboard"',
        'label: "Candidate Funnel"',
        'label: "Captured Trades"',
        'label: "Runtime Timeline"',
        'label: "Verified Runtime"',
        'label: "Repair Center"',
        'label: "Command Center"',
        'label: "Positions"',
    ]:
        assert old_primary not in primary_nav
    assert "hidden_by_default: true" in nav
    assert "available_in_engineering_mode: true" in nav


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

    for route in ["/aegis-command-center", "/aegis-positions", "/aegis-history", "/aegis-research-workspace", "/aegis-trading-desk", "/aegis-operations", "/aegis-audit-evidence", "/aegis-opportunities", "/aegis-edge-lab", "/aegis-performance", "/aegis-journal", "/aegis-today", "/aegis-review", "/aegis-research", "/aegis-operator-cockpit"]:
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
