from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
SHELL = ROOT / "constellation_2/phaseL/ui/static/operator_shell"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
PACKAGE = ROOT / "package.json"


def test_research_review_route_nav_and_api_exist() -> None:
    route_metadata = (SHELL / "pages/route_metadata.js").read_text(encoding="utf-8")
    navigation = (SHELL / "navigation_schema.js").read_text(encoding="utf-8")
    server = SERVER.read_text(encoding="utf-8")
    package_json = PACKAGE.read_text(encoding="utf-8")

    assert 'path: "/research-lab/review"' in route_metadata
    assert 'id: "research_review"' in route_metadata
    assert 'route: "/research-lab/review"' in navigation
    assert 'label: "Queue"' in navigation
    assert 'label: "Review"' in navigation
    assert 'label: "Diagnostics"' in navigation
    assert 'routes: ["/aegis-research-workspace", "/research-lab", "/research-lab/review", "/research-lab/blocked-work"' in navigation
    assert '"/api/research-lab/review-brief/latest"' in server
    assert '"aegis:research-review-brief"' in package_json
    assert '"aegis:research-review-self-check"' in package_json


def test_research_review_page_reads_canonical_brief_api() -> None:
    domain = (SHELL / "domain_client/index.js").read_text(encoding="utf-8")
    page = (SHELL / "pages/index.js").read_text(encoding="utf-8")

    assert "function fetchResearchReviewBrief" in domain
    assert 'query("/api/research-lab/review-brief/latest"' in domain
    assert "renderResearchReviewPage" in page
    assert "fetchResearchReviewBrief()" in page or "fetchResearchReviewBrief(routeParams)" in page
    assert "renderResearchOperatorPage" in page
    assert "Research follow-up needed" in page
    assert "What has been learned?" in page
    assert "AEGIS_RESEARCH_REVIEW_BRIEF_V1" in page
    assert "Review Brief" in page
    assert "TIER_1_ACTIVE" not in page.split("function renderResearchReviewBriefCard", 1)[1].split("function renderResearchReviewPageContent", 1)[0]


def test_recommendation_ready_links_to_research_review_brief_content() -> None:
    page = (SHELL / "pages/index.js").read_text(encoding="utf-8")

    assert "Research Briefs" in page
    assert "conclusion" in page
    assert "confidence_reason" in page
    assert "key_evidence" in page
    assert "risks" in page
    assert "decision_needed" in page
    assert "hypotheses-diagnostics" in page



def test_research_review_collecting_evidence_sample_size_ui() -> None:
    page = (SHELL / "pages/index.js").read_text(encoding="utf-8")
    card = page.split("function renderResearchReviewBriefCard", 1)[1].split("function renderResearchReviewPageContent", 1)[0]
    hypothesis_card = page.split("function renderHypothesisCard", 1)[1].split("function emptyHypothesisViewSection", 1)[0]

    assert "Collecting Evidence" in page
    assert "Research is active. Aegis needs more forward-return observations before this hypothesis can qualify for paper validation." in page
    assert "Required samples" in page
    assert "Current samples" in page
    assert "Missing samples" in page
    assert "Sample count" in page
    assert "Next sample" in page
    assert "Estimated completion" in page
    assert "Trading days remaining" in page
    assert "Operator action required" in page
    assert "Paper-testing sleeve" in page
    assert "Not created yet" in page
    assert "Qualification requires more evidence" in page
    assert "Collect forward-return observations" in page
    assert "Manual Review Required" not in card
    assert "Manual IB capture guidance" not in hypothesis_card


def test_rebuilt_research_workspace_is_operator_workflow_only() -> None:
    page = (SHELL / "pages/index.js").read_text(encoding="utf-8")
    block = page.split("async function renderResearchWorkspace", 1)[1].split("async function renderOperationsWorkspace", 1)[0]

    assert "What is active?" in block
    assert "What has been learned?" in block
    assert "What is blocked?" in block
    assert "What happens next?" in block
    assert "David Action" in block
    assert "Research-only" in block
    assert "Follow-ups needing review" in block
    assert "Latest finding" in block
    assert "Research blocker" in block
    assert "No action - collecting evidence." in block
    assert "open positions" not in block.lower()
    assert "holdings" not in block.lower()
    assert "exposure" not in block.lower()
    assert "candidate qualification" not in block.lower()
    assert "candidate actionability" not in block.lower()
    assert "candidate capture" not in block.lower()
    assert "No trade, candidate, or broker action" not in block
    assert "portfolio p&l" not in block.lower()
    assert "Open Research Review" not in block
    assert "Sleeve Analytics" not in block
    assert "Performance Review" not in block


def test_research_workspace_shows_validation_engine_without_trading_actions() -> None:
    domain = (SHELL / "domain_client/index.js").read_text(encoding="utf-8")
    page = (SHELL / "pages/index.js").read_text(encoding="utf-8")
    server = SERVER.read_text(encoding="utf-8")
    block = page.split("async function renderResearchWorkspace", 1)[1].split("async function renderOperationsWorkspace", 1)[0]

    assert "function fetchResearchValidationEngine" in domain
    assert 'query("/api/research-lab/validation-engine/latest"' in domain
    assert '"/api/research-lab/validation-engine/latest"' in server
    assert "Validation status" in block
    assert "Protocol used" in block
    assert "Promotion eligibility" in block
    assert "Required metrics" in block
    assert "Source binding" in block
    assert "Bias checks" in block
    assert "Sample policy" in block
    assert "Eligible for candidate review" in block
    assert "Not eligible for candidate review" in block
    assert "No trading action or broker action is enabled from Research." in block
    assert "candidate approval" not in block.lower()
    assert "broker action" in block
