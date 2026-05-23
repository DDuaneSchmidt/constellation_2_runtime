from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_candidate_portfolio_selection_is_not_primary_dashboard_workflow() -> None:
    pages = pages_source_v1(ROOT)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]
    candidates_block = pages.split("function renderAegisCandidatesWorkflow", 1)[1].split("function renderAegisReviewWorkflow", 1)[0]

    assert "renderCandidatePortfolioSelectionPanel" not in today_block
    assert "renderSuppressedWatchlistProjectionPanel" not in today_block
    assert "Current-Day Candidates" in candidates_block
    assert "Your task" in candidates_block
    assert "MANUAL_IB_CAPTURE_READY" in pages
    assert "NO_USER_ACTION" in pages


def test_suppressed_candidates_are_grouped_collapsed_and_plain_english() -> None:
    pages = pages_source_v1(ROOT)
    block = pages.split("function renderSuppressedWatchlistProjectionPanel", 1)[1].split("function renderManualCaptureRecordForm", 1)[0]

    assert "Suppressed / Watchlist candidates" in block
    assert "<details" in block
    assert "Reason groups" in block
    assert "Sleeve groups" in block
    assert "operator_explanation" in block
    assert "Exposure cluster" in block


def test_raw_score_is_not_primary_ui_label() -> None:
    pages = pages_source_v1(ROOT)
    selected_block = pages.split("function renderPortfolioSelectedCandidateTable", 1)[1].split("function renderCandidatePortfolioSelectionPanel", 1)[0]
    suppressed_block = pages.split("function renderSuppressedWatchlistProjectionPanel", 1)[1].split("function renderManualCaptureRecordForm", 1)[0]

    assert "Raw score" not in selected_block
    assert "Raw score" in suppressed_block
    assert suppressed_block.index("Score band") < suppressed_block.index("Show all advanced raw scores")
