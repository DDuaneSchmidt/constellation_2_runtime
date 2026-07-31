from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def test_command_center_renders_scheduled_run_readiness_section() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    assert "renderCommandCenterScheduledRunReadiness" in pages
    assert 'data-testid="scheduled-run-readiness"' in pages
    assert "${renderCommandCenterScheduledRunReadiness(payload)}" in pages


def test_today_api_envelope_includes_scheduled_run_readiness() -> None:
    server = SERVER.read_text(encoding="utf-8")
    assert "scheduled_run_readiness" in server
    assert "build_scheduled_run_readiness_certificate_v1" in server
    assert "build_scheduled_run_reconciliation_v1" in server
