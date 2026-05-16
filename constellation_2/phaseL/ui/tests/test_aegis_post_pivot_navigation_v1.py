from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_primary_aegis_lite_navigation_matches_post_pivot_model() -> None:
    nav = _text(NAV)

    assert 'section: "AEGIS LITE"' in nav
    assert 'label: "Aegis Lite"' in nav
    assert "Today / Operator Status" in nav
    assert "EOD Queue" in nav
    assert "Event Monitoring" in nav
    assert "Manual Trade Packets" in nav
    assert "Receipts / Outcomes" in nav
    assert "Sleeve Performance" in nav
    assert "AI Feedback / EOD-EOW Review" in nav
    assert "Research Lab" in nav
    assert "Operator Inbox" in nav
    assert 'section: "LEGACY / DEFERRED"' in nav
    assert "Legacy Runtime Diagnostics" in nav


def test_obsolete_primary_labels_are_absent_from_active_ui_copy() -> None:
    active_ui = _text(PAGES) + "\n" + _text(NAV)

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
    pages = _text(PAGES)
    server = _text(SERVER)

    for route in ["/aegis-lite", "/aegis-events", "/aegis-ai-feedback", "/research-lab", "/operator-inbox"]:
        assert route in pages
        assert route in server
    assert "renderAegisAiFeedbackPage" in pages
    assert "renderResearchLabPage" in pages
    assert "renderOperatorInboxPage" in pages


def test_lite_ui_surfaces_runtime_truth_and_manual_workflow_guardrails() -> None:
    pages = _text(PAGES)

    assert "REAL_RUNTIME" in pages
    assert "DEMO_ONLY" in pages
    assert "DRY_RUN_ONLY" in pages
    assert "ADVISORY_ONLY" in pages
    assert "GATE_ONLY_NO_TRANSPORT" in pages
    assert "operator-entered trades" in pages
    assert "This is not broker automation" in pages
    assert "Manual Trade Packets" in pages
    assert "Receipts / Outcomes" in pages
    assert "Sleeve Performance" in pages
