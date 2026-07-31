from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGE = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"


def test_performance_page_renders_trade_evaluation_sections_and_commands() -> None:
    source = PAGE.read_text(encoding="utf-8")
    for text in [
        "P&L Summary",
        "Daily Paper Performance",
        "daily_paper_performance_v1",
        "Sleeve comparison",
        "Operator attention list",
        "stop loss near",
        "Paper PnL Summary",
        "Open Position PnL",
        "Exit Strategy panel",
        "What would make me exit?",
        "paper_pnl_report_v1",
        "exit_strategy_analysis_v1",
        "Certified mark",
        "automatic exits remain disabled",
        "Open Trades",
        "Closed Trades",
        "Needs Attention",
        "Sleeve Attribution",
        "Hypothesis Attribution",
        "Exit Review",
        "Stop / Target Status",
        "Time stop",
        "Sleeve Performance",
        "renderDailyPaperPerformanceSection",
        "renderPaperPnlReportSection",
        "renderExitStrategyAnalysisSection",
        "renderSleevePerformanceTruthSection",
        "sleeve_performance_truth_v1",
        "NOT_CANONICAL",
        "renderPaperTradeEvaluationWorkspace",
    ]:
        assert text in source
    assert "next_action_command" in source
    assert "data-aegis-command-id" in source
    assert "manual-receipt-form" in source
    assert "Receipt fields needed" in source
    assert "Save manual receipt" in source


def test_performance_trade_actions_are_in_page_command_contracts_only() -> None:
    source = PAGE.read_text(encoding="utf-8")
    trade_renderer = source[source.index("function tradeCommandButton"):source.index("function renderTradeDetailDialog")]
    assert 'data-aegis-command-action-type="IN_PAGE_DETAIL"' in trade_renderer
    assert 'href=' not in trade_renderer
    assert '/aegis-performance/' not in trade_renderer
    assert '/research-lab/' not in trade_renderer


def test_performance_page_renders_exit_review_command_contracts() -> None:
    source = PAGE.read_text(encoding="utf-8")
    for text in [
        "renderExitReviewPositionTable",
        "exitReviewCommandButton",
        "VIEW_EXIT_DECISION",
        "UPDATE_STOP_PLAN",
        "RECORD_PARTIAL_EXIT",
        "RECORD_FULL_EXIT",
        "RECORD_TRADE_OUTCOME",
        "VIEW_POSITION_DETAIL",
        "data-aegis-command-action-type=\"IN_PAGE_DETAIL\"",
    ]:
        assert text in source
    exit_renderer = source[source.index("function exitReviewCommandButton"):source.index("function renderExitReviewDetailDrawer")]
    assert "href=" not in exit_renderer
    assert "data-aegis-command-id" in exit_renderer


def test_sleeve_performance_truth_section_is_read_only_and_separate() -> None:
    source = PAGE.read_text(encoding="utf-8")
    section = source[source.index("function renderSleevePerformanceTruthTable"):source.index("function renderPaperTradeEvaluationWorkspace")]
    assert "sleeve_performance_truth_v1" in section
    assert "scorecard_ref" in section
    assert "NOT_CANONICAL" in section
    assert "Open Paper Positions" not in section
    assert "Captured Trades" not in section
    assert "RECORD_PAPER_EXIT" not in section
    assert "data-aegis-command-id" not in section
