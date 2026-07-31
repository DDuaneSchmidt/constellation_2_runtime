from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def test_portfolio_context_ui_renders_compactly_on_performance_and_exit_review() -> None:
    source = PAGES.read_text(encoding="utf-8")

    assert "function renderPortfolioContextPanel" in source
    assert "Portfolio Context" in source
    assert "Exposure Summary" in source
    assert "Sleeve Rollup" in source
    assert "Concentration Warnings" in source
    assert "Sleeve Overlap" in source
    assert "Regime Context" in source
    assert "Factor Context" in source
    assert "Context is advisory only" in source
    assert source.count("renderPortfolioContextPanel(payload)") >= 2


def test_portfolio_context_evidence_drawer_links_source_artifacts() -> None:
    source = PAGES.read_text(encoding="utf-8")
    panel = source[source.index("function renderPortfolioContextPanel"):source.index("function renderPaperTradeEvaluationWorkspace")]

    assert "renderEvidenceTrigger" in panel
    assert "portfolio_context_projection_v1" in panel
    assert "evidence_links" in panel
    assert "artifact_path" in panel
    assert "artifact_sha256" in panel
    assert "View Evidence" in panel
    assert "PARTIAL" in panel


def test_portfolio_context_backend_is_in_cockpit_payload() -> None:
    server = SERVER.read_text(encoding="utf-8")

    assert "build_portfolio_context_projection_v1" in server
    assert 'payload["portfolio_context_projection_v1"]' in server
    assert 'payload["portfolio_context_projection"]' in server
    assert 'data["portfolio_context_projection_v1"]' in server
    assert "write_portfolio_context_projection_v1" in server


def test_portfolio_context_ui_does_not_expose_automation_copy() -> None:
    source = PAGES.read_text(encoding="utf-8")
    panel = source[source.index("function renderPortfolioContextPanel"):source.index("function renderPaperTradeEvaluationWorkspace")]

    assert "does not change scoring, sizing, trade selection, exit decisions, order routing, broker submission, or autonomous execution" in panel
    assert "Execute trade" not in panel
    assert "Submit order" not in panel
    assert "Sell" not in panel
