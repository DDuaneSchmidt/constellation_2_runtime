from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"


def test_paper_promotion_recommendations_card_and_actions_visible() -> None:
    source = PAGES.read_text(encoding="utf-8")
    assert "PAPER PROMOTION RECOMMENDATIONS" in source
    assert "Paper promotion recommendation" in source
    assert "Awaiting paper-test approval" in source
    assert "paper-promotion-recommendations-card" in source
    assert "paper-promotion-recommendation-count" in source
    assert "recommendation${pending.length === 1 ? \"\" : \"s\"} awaiting decision" in source
    assert "Approve Paper Test" in source
    assert "Reject" in source
    assert "Defer" in source
    assert "Current state" in source
    assert "PAPER_PROMOTION_RECOMMENDED" in source
    assert "Shadow Validation" in source
    assert "Paper Readiness" in source
    assert "Paper setup status" in source
    assert "Candidate generation eligibility" in source
    assert "Awaiting qualifying paper candidates" in source
    assert "Eligible for future paper candidate generation" in source
    assert "Expected Sample Rate" in source
    assert "Why recommended:" in source
    assert "Paper research only. Not trade advice. No broker execution. No live trading." in source
    assert "approve trade" in source
    assert "approve broker execution" in source
    assert "approve real capital" in source
    assert "investment recommendation" in source


def test_research_page_prioritizes_paper_promotion_before_generic_followups() -> None:
    source = PAGES.read_text(encoding="utf-8")
    render_block = source.split("function renderResearchOperatorPage", 1)[1].split("function renderResearchFactCard", 1)[0]
    assert render_block.index("renderPaperPromotionRecommendationsCard(model)") < render_block.index("renderResearchPriorityStrip(model)")
    assert "Paper promotions" in render_block
    assert "paper promotion approval needed" in render_block
    assert "Research follow-ups" not in render_block
    assert "Monitoring only" not in render_block
    assert "Production" not in render_block


def test_promotion_api_is_research_only_and_declares_forbidden_execution() -> None:
    server = SERVER.read_text(encoding="utf-8")
    assert "/api/research-lab/hypothesis-proposal-promotion/latest" in server
    assert "/api/research-lab/approved-hypothesis-paper-setup/latest" in server
    assert "approved_hypothesis_paper_tracking_setup_path_v1" in server
    assert "paper_readiness_certification_path_v1" in server
    assert "paper_sleeve_blueprint_path_v1" in server
    assert "/api/research-lab/paper-promotion/action" in server
    assert "record_paper_promotion_approval_event_v1" in server
    assert '"trade_advice_allowed": False' in server
    assert '"broker_execution_allowed": False' in server
    assert '"live_trading_allowed": False' in server
    assert '"real_capital_allowed": False' in server
    assert '"automatic_paper_sleeve_creation_allowed": False' in server


def test_paper_promotion_click_handler_records_and_reports_state() -> None:
    main = MAIN.read_text(encoding="utf-8")
    page = PAGES.read_text(encoding="utf-8")
    server = SERVER.read_text(encoding="utf-8")

    assert "executePaperPromotionAction" in main
    assert "fetchApprovedHypothesisPaperSetup" in page
    assert "researchPaperPromotionRecommendations(promotionArtifacts, paperSetupArtifacts)" in page
    assert "[data-paper-promotion-action]" in main
    assert "runPaperPromotionActionElement" in main
    assert "Approved for paper research tracking." in main
    assert "Approval failed:" in main
    assert "Paper promotion rejected." in main
    assert "Paper promotion deferred." in main
    assert 'await renderRoute({ source: "paper-promotion-action" })' in main

    assert "data-paper-promotion-status" in page
    assert "paperPromotionTerminalMessage" in page
    assert "PAPER_PROMOTION_APPROVED" in page
    assert "Approved for paper research tracking." in page

    assert "approval_event_path" in server
    assert "queue_item" in server
    assert "prior_state" in server
    assert "new_state" in server
    assert "promotion_packet_hash" in server


def test_paper_promotion_failure_and_safety_copy_are_explicit() -> None:
    main = MAIN.read_text(encoding="utf-8")
    server = SERVER.read_text(encoding="utf-8")

    assert "Approval failed: missing action or hypothesis id" in main
    assert "approval endpoint returned ok=false" in main
    assert "buttons.forEach((button) => { button.disabled = false; });" in main
    assert '"trade_advice_allowed": False' in server
    assert '"broker_execution_allowed": False' in server
    assert '"live_trading_allowed": False' in server
    assert '"real_capital_allowed": False' in server
    assert '"automatic_paper_sleeve_creation_allowed": False' in server
    assert "execute a trade" in server
    assert "create broker order" in server
    assert "submit/transmit order" in server
    assert "allocate real capital" in server
    assert "enable live trading" in server
    assert "create investment advice" in server
