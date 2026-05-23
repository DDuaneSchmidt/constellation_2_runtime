from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_position_management_state_is_secondary_not_primary_dashboard_workflow() -> None:
    pages = pages_source_v1(ROOT)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]
    position_block = pages.split("function renderPositionManagementPanel", 1)[1].split("function renderCandidateReviewLedgerPanel", 1)[0]

    assert "renderPositionManagementPanel" not in today_block
    for label in [
        "Position Management",
        "Entry price",
        "Quantity",
        "Stop price",
        "Max planned loss",
        "Risk plan status",
        "Stop event status",
        "Add Risk Plan",
        "Record Stop Event",
        "Correct Risk Plan",
        "Record Final Outcome",
        "Advanced CLI fallback",
        "npm run aegis:record-position-risk-plan",
        "npm run aegis:record-stop-event",
        "npm run aegis:correct-position-event",
        "No broker execution",
        "No autonomous execution",
        "No automatic order placement",
        "No automatic stop execution",
    ]:
        assert label in position_block


def test_position_management_safe_post_endpoints_are_registered() -> None:
    server = _text(SERVER)

    for endpoint in [
        "/api/aegis/position/risk-plan",
        "/api/aegis/position/stop-event",
        "/api/aegis/position/correction",
    ]:
        assert endpoint in server
    assert "append_position_risk_plan_v1" in server
    assert "append_stop_event_v1" in server
    assert "append_position_event_correction_v1" in server
    assert "automatic_stop_execution_allowed" in server
    assert "order_routing_allowed" in server
    assert "live_trading_allowed" in server
    assert "broker_execution_allowed" in server
    assert "autonomous_execution_allowed" in server
