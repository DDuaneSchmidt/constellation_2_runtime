from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def test_command_center_renders_daily_integrity_section_from_backend_artifact() -> None:
    page = PAGES.read_text(encoding="utf-8")
    block = page.split("function renderDailyResearchIntegrityAudit", 1)[1].split("function renderOperatorDecisionDavidActions", 1)[0]
    assert "daily_research_integrity_audit_v1" in block
    assert "data-testid=\"daily-research-integrity\"" in block
    assert "DAILY INTEGRITY" in block
    assert "Daily integrity passed. No blocking research defects." in block
    assert "Daily integrity passed with warnings." in block
    assert "safeList(audit.issue_rows).slice(0, 3)" in block
    assert "aegis_daily_research_integrity_audit_v1" in block
    assert "infer" not in block.lower()


def test_operator_today_envelope_exposes_daily_integrity_artifact() -> None:
    server = SERVER.read_text(encoding="utf-8")
    assert "aegis_daily_research_integrity_audit_v1" in server
    assert "daily_research_integrity_audit.v1.json" in server
