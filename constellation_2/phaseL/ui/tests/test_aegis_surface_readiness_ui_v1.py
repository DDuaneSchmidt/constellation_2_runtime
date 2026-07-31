from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def test_command_center_consumes_surface_readiness_before_actions():
    text = PAGES.read_text(encoding="utf-8")
    assert "function surfaceReadinessRow" in text
    assert 'surfaceActionsAllowed(payload, "command_center")' in text
    assert 'if (!surfaceActionsAllowed(payload, "command_center")) return []' in text


def test_operator_cockpit_includes_surface_readiness_payload():
    text = SERVER.read_text(encoding="utf-8")
    assert "surface_readiness" in text
    assert "build_surface_readiness_v1" in text
    assert "/api/aegis/surface-readiness/latest" in text


def test_position_review_missing_artifact_returns_unavailable_not_404():
    text = SERVER.read_text(encoding="utf-8")
    assert "aegis_position_review_unavailable" in text
    assert "Position Review unavailable for requested day" in text
    assert "POSITION_REVIEW_BRIEF_MISSING" not in text
