from __future__ import annotations

from pathlib import Path

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server

ROOT = Path(__file__).resolve().parents[4]
UI = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
TRUTH = Path("/home/node/constellation_runtime_data/truth")
DAY = "2026-06-01"


def test_command_center_renders_research_scorecard_above_detailed_cards() -> None:
    text = UI.read_text(encoding="utf-8")
    assert "function renderResearchDailyScorecardSection" in text
    assert "TODAY'S RESEARCH RESULT" in text
    assert text.index("${renderResearchDailyScorecardSection(payload)}") < text.index("${renderCommandCenterPrimaryOverview(payload)}")
    assert "DAVID ACTIONS" in text
    assert "CURRENT BOTTLENECK" in text


def test_ui_uses_backend_scorecard_text_and_buttons() -> None:
    text = UI.read_text(encoding="utf-8")
    assert "scorecard.primary_message" in text
    assert "scorecard.primary_bottleneck" in text
    assert "action.exact_buttons" in text
    scorecard_fn = text[text.index("function renderResearchDailyScorecardSection"):text.index("function renderTodaySummaryGrid")]
    assert "Monitoring only" not in scorecard_fn
    assert "Production" not in scorecard_fn


def test_operator_payload_attaches_research_daily_scorecard() -> None:
    payload = server._today_build_operator_envelope_v1(TRUTH, DAY, DAY)
    scorecard = payload.get("research_daily_scorecard")
    assert isinstance(scorecard, dict)
    assert scorecard.get("schema_id") == "aegis_research_daily_scorecard"
    assert payload.get("source_paths", {}).get("research_daily_scorecard", "").endswith("research_daily_scorecard.v1.json")



def test_command_center_renders_macro_calendar_data_readiness() -> None:
    text = UI.read_text(encoding="utf-8")
    assert "function renderMacroCalendarDataReadinessSection" in text
    assert "Macro Calendar Data Readiness" in text
    assert "Macro Calendar needs a governed macro event calendar source." in text
    assert "safeList(artifact.buttons)" in text
    assert "macro-calendar-data-readiness-buttons" in text
    assert "${renderMacroCalendarDataReadinessSection(payload.macro_calendar_data_readiness_v1 || {})}" in text

def test_command_center_oil_shock_uses_candidate_flow_exact_blocker() -> None:
    text = UI.read_text(encoding="utf-8")
    block = text[text.index("function commandCenterOilShockFlow"):text.index("function renderOperatorDecisionTodayResult")]
    assert "artifact.oil_shock" in block
    assert "aegis_oil_shock_candidate_flow_v1" in block
    assert "PRODUCER_MISSING" in block
    assert "Oil Shock deterministic candidate producer is missing." in block
    assert "implement/run deterministic Oil Shock producer" in block
    assert "MISSING_DATA" not in block.split('if (blocker === "PRODUCER_MISSING"', 1)[1].split("function renderOperatorDecisionTodayResult", 1)[0]
