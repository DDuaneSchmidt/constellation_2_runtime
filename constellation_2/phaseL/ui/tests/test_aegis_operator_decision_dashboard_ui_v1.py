from __future__ import annotations

from pathlib import Path

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server

ROOT = Path(__file__).resolve().parents[4]
UI = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
TRUTH = Path("/home/node/constellation_runtime_data/truth")
DAY = "2026-06-01"
DOCS = [
    ROOT / "docs/aegis_operator_decision_dashboard_requirements_v1.md",
    ROOT / "docs/aegis_operator_decision_dashboard_spec_v1.md",
    ROOT / "docs/aegis_operator_decision_dashboard_design_v1.md",
    ROOT / "docs/aegis_research_daily_scorecard_design_v1.md",
    ROOT / "docs/aegis_command_center_visual_design_requirements_v1.md",
    ROOT / "docs/aegis_today_screen_spec.md",
    ROOT / "docs/aegis_operator_screen_spec.md",
    ROOT / "docs/aegis_research_ui_dumb_renderer_design_v1.md",
]


def _command_block() -> str:
    text = UI.read_text(encoding="utf-8")
    return text.split("async function renderCommandCenterWorkspace", 1)[1].split("function positionsCaptureSummary", 1)[0]


def _scorecard_block() -> str:
    text = UI.read_text(encoding="utf-8")
    return text[text.index("function commandCenterOilShockFlow"):text.index("function renderTodaySummaryGrid")]


def test_operator_decision_sections_are_above_detailed_cards() -> None:
    block = _command_block()
    normal_body = block.rsplit("const bodyHtml = `", 1)[1]
    ordered = [
        "${renderResearchDailyScorecardSection(payload)}",
        "${renderCommandCenterPrimaryOverview(payload)}",
        "${renderPaperPromotionRecommendationsCard",
        "${renderCommandCenterValidationPipeline(payload)}",
        "${renderCommandCenterRunSummary(payload)}",
        "${renderCommandCenterSafetyStrip(payload)}",
    ]
    positions = [normal_body.index(fragment) for fragment in ordered]
    assert positions == sorted(positions)
    scorecard = _scorecard_block()
    for label in ["TODAY'S RESEARCH RESULT", "DAVID ACTIONS", "GENERATED HYPOTHESIS PROGRESS", "VALIDATION PROGRESS", "CURRENT BOTTLENECK"]:
        assert label in scorecard


def test_primary_dashboard_forbidden_labels_and_vague_review_are_absent() -> None:
    scorecard = _scorecard_block()
    for forbidden in ["Production", "Monitoring only", "Trade Recommendation", "Manual Capture", "UNKNOWN", "Values incomplete"]:
        assert forbidden not in scorecard
    assert "Needs review" not in scorecard
    assert "action.exact_buttons" in scorecard


def test_oil_shock_blocker_uses_authoritative_candidate_flow_artifact() -> None:
    scorecard = _scorecard_block()
    assert "payload.oil_shock_candidate_flow_v1" in scorecard
    assert "oilFlow.exact_blocker" in scorecard
    assert "PRODUCER_MISSING" in scorecard
    assert "MISSING_DATA" not in scorecard.split("function commandCenterGeneratedProgressRows", 1)[1].split("function renderOperatorDecisionTodayResult", 1)[0]


def test_research_allocation_uses_recommendation_artifact() -> None:
    text = UI.read_text(encoding="utf-8")
    primary = text[text.index("function renderCommandCenterPrimaryOverview"):text.index("function renderCommandCenterValidationPipeline")]
    assert "payload.research_allocation_recommendation_v1" in primary
    assert "allocationRecommendationSummary.HOLD" in primary
    assert "allocationRecommendationSummary.PAUSE" in primary
    assert "No allocation decisions yet" not in primary


def test_operator_payload_attaches_decision_dashboard_sources() -> None:
    payload = server._today_build_operator_envelope_v1(TRUTH, DAY, DAY)
    for key in [
        "research_daily_scorecard",
        "operator_action_queue_v1",
        "hypothesis_workflow_state_v1",
        "generated_hypothesis_throughput_v1",
        "oil_shock_candidate_flow_v1",
        "research_quality_engine_v1",
        "hypothesis_decision_policy_v1",
        "research_allocation_recommendation_v1",
        "research_follow_through_control_v1",
        "ai_research_intelligence_summary_v1",
    ]:
        assert isinstance(payload.get(key), dict), key
    oil = payload["oil_shock_candidate_flow_v1"]
    oil_row = oil.get("oil_shock") if isinstance(oil.get("oil_shock"), dict) else oil
    assert oil_row.get("exact_blocker") == "MISSING_DATA"
    allocation = payload["research_allocation_recommendation_v1"]
    assert allocation.get("summary", {}).get("recommendation_count", 0) > 0


def test_ui_design_docs_specs_were_updated() -> None:
    for path in DOCS:
        text = path.read_text(encoding="utf-8")
        assert "operator decision dashboard" in text.lower(), path
        assert "TODAY'S RESEARCH RESULT" in text, path
        assert "GENERATED HYPOTHESIS PROGRESS" in text, path
        assert "CURRENT BOTTLENECK" in text, path
