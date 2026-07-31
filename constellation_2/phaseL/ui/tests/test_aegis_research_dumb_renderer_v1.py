from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
CLIENT = ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def test_research_ui_uses_canonical_workflow_and_action_queue() -> None:
    page = PAGES.read_text(encoding="utf-8")
    block = page.split("async function renderResearchWorkspace", 1)[1].split("function buildResearchOperatorModel", 1)[0]
    assert "fetchHypothesisWorkflowState(routeParams)" in block
    assert "fetchOperatorActionQueue(routeParams)" in block
    assert "fetchHypothesisWorkflowReplayVerification(routeParams)" in block
    assert "fetchGeneratedHypothesisThroughput(routeParams)" in block
    assert "renderCanonicalDavidActionQueue(model)" in block
    assert "renderMacroCalendarDataReadinessSection(model.macroCalendarDataReadiness || {})" in block
    assert "renderCanonicalHypothesisStatusSummary(model)" in block
    assert "renderCanonicalHypothesisList(model)" in block
    assert "renderResearchFollowThroughSection(model)" in block
    assert "renderOilShockCandidateFlowSection(model)" in block
    assert "renderAIResearchIntelligenceSection(model)" in block
    assert "State age" in block
    assert "Generated throughput" in block
    assert "Replay verification" in block
    assert "ready for human review" not in block
    assert "Production" not in block
    assert "Monitoring only" not in block
    assert "Trade Recommendation" not in block
    assert "Manual Capture" not in block
    assert "UNKNOWN" not in block




def test_macro_calendar_data_readiness_section_renders_backend_artifact() -> None:
    page = PAGES.read_text(encoding="utf-8")
    block = page.split("function renderMacroCalendarDataReadinessSection", 1)[1].split("function renderOperatorDecisionDavidActions", 1)[0]
    assert "aegis_macro_calendar_data_readiness_v1" in block
    assert "Macro Calendar Data Readiness" in block
    assert "artifact.status" in block
    assert "artifact.macro_calendar_ready" in block
    assert "artifact.david_action_required" in block
    assert "artifact.missing_fields" in block
    assert "artifact.required_fields" in block
    assert "artifact.next_step" in block
    assert "safeList(artifact.buttons)" in block
    assert "No broker execution" in block
    assert "No live trading" in block

def test_research_quality_section_renders_backend_artifacts_only() -> None:
    page = PAGES.read_text(encoding="utf-8")
    block = page.split("function renderResearchQualitySection", 1)[1].split("function renderResearchFollowThroughSection", 1)[0]
    assert "model.quality?.hypotheses" in block
    assert "model.decisions?.decisions" in block
    assert "model.allocation?.recommendations" in block
    assert "quality_status" in block
    assert "recommendation" in block
    assert "recommended_allocation_action" in block
    assert "active_hard_gate_codes" in block
    assert "requires_david_review" in block
    assert "score" not in block.lower()
    assert "infer" not in block.lower()



def test_research_follow_through_section_renders_backend_artifacts_only() -> None:
    page = PAGES.read_text(encoding="utf-8")
    block = page.split("function renderResearchFollowThroughSection", 1)[1].split("function renderCanonicalActionCard", 1)[0]
    assert "model.followThrough?.follow_ups" in block
    assert "summary.by_type" in block
    assert "summary.by_status" in block
    assert "follow_up_type" in block
    assert "current_status" in block
    assert "blocking_what" in block
    assert "next_action" in block
    assert "review_after_days" in block
    assert "requires_david_action" in block
    assert "needs review" not in block.lower()
    assert "infer" not in block.lower()




def test_oil_shock_candidate_flow_section_renders_backend_artifact_only() -> None:
    page = PAGES.read_text(encoding="utf-8")
    block = page.split("function renderOilShockCandidateFlowSection", 1)[1].split("function renderAIResearchIntelligenceSection", 1)[0]
    assert "model.oilShockCandidateFlow" in block
    assert "model.oilShockCandidateConstruction" in block
    assert "model.generatedHypothesisGovernanceBridge" in block
    assert "model.generatedHypothesisApprovalEventLineage" in block
    assert "aegis_generated_hypothesis_approval_event_lineage_v1" in block
    assert "aegis_generated_hypothesis_governance_bridge_v1" in block
    assert "aegis_oil_shock_candidate_flow_v1" in block
    assert "aegis_oil_shock_candidate_construction_v1" in block
    assert "candidate_flow_status" in block
    assert "candidate_producer_status" in block
    assert "last_evaluation_status" in block
    assert "candidate_construction_status" in block
    assert "governance_bridge_status" in block
    assert "approval_lineage_status" in block
    assert "approval_event_found" in block
    assert "approval_event_hash" in block
    assert "governanceMissingFields" in block
    assert "sleeve_id" in block
    assert "risk_policy_id" in block
    assert "exit_policy_id" in block
    assert "missing_construction_fields" in block
    assert "candidate_count" in block
    assert "raw_signal_count" in block
    assert "blocker_code" in block
    assert "blocker_owner" in block
    assert "candidate_flow_started" in block
    assert "reason_no_candidates_generated_yet" in block
    assert "next_expected_step" in block
    assert "david_action_required" in block
    assert "reason_codes" in block
    assert "No David action required. Aegis is waiting for qualifying Oil Shock candidate conditions." in block
    assert "row.ui_message" in block
    assert "AI" not in block
    assert "infer" not in block.lower()


def test_ai_research_intelligence_section_renders_advisory_label() -> None:
    page = PAGES.read_text(encoding="utf-8")
    block = page.split("function renderAIResearchIntelligenceSection", 1)[1].split("function renderCanonicalHypothesisStatusSummary", 1)[0]
    assert "AI research analysis — advisory only." in block
    assert "model.aiIntelligence?.hypotheses" in block
    assert "aegis_ai_research_intelligence_summary_v1" in block
    assert "ai_recommendation_type" in block
    assert "root_cause_summary" in block
    assert "repair_summary" in block
    assert "duplicate_summary" in block
    assert "evidence_summary" in block
    assert "ai_confidence" in block
    assert "Deterministic Aegis artifacts remain authoritative" in block
    assert "infer" not in block.lower()


def test_buttons_are_rendered_from_backend_exact_buttons_only() -> None:
    page = PAGES.read_text(encoding="utf-8")
    assert "safeList(item.exact_buttons)" in page
    assert "data-operator-action-event" in page
    assert "blocking_what" in page
    assert "impact_area" in page
    assert "action_age_days" in page
    assert "Connect Source" not in page.split("function renderCanonicalActionCard", 1)[1].split("function renderCanonicalHypothesisStatusSummary", 1)[0]
    assert "No David action required. Aegis will continue automatically." in page


def test_client_and_server_expose_workflow_queue_and_event_log() -> None:
    client = CLIENT.read_text(encoding="utf-8")
    server = SERVER.read_text(encoding="utf-8")
    main = MAIN.read_text(encoding="utf-8")
    assert "/api/research-lab/hypothesis-workflow-state/latest" in client
    assert "/api/research-lab/operator-action-queue/latest" in client
    assert "/api/research-lab/hypothesis-workflow-replay-verification/latest" in client
    assert "/api/research-lab/generated-hypothesis-throughput/latest" in client
    assert "/api/research-lab/operator-action/event" in client
    assert "hypothesis_workflow_state_path_v1" in server
    assert "operator_action_queue_path_v1" in server
    assert "hypothesis_workflow_replay_verification_path_v1" in server
    assert "generated_hypothesis_throughput_path_v1" in server
    assert "build_research_follow_through_control_v1" in server
    assert "build_ai_research_intelligence_bundle_v1" in server
    assert "build_oil_shock_candidate_flow_v1" in server
    assert "build_oil_shock_candidate_construction_v1" in server
    assert "build_generated_hypothesis_approval_event_lineage_v1" in server
    assert "build_generated_hypothesis_governance_bridge_v1" in server
    assert "append_operator_action_event_v1" in server
    assert "appendOperatorActionEvent" in main
    assert "runCanonicalOperatorActionElement" in main



def test_command_center_renders_market_data_universe_section() -> None:
    page = PAGES.read_text(encoding="utf-8")
    block = page.split("function renderMarketDataUniverseSection", 1)[1].split("function renderResearchDailyScorecardSection", 1)[0]
    assert "market_data_universe_consistency_v1" in block
    assert "Market Data Universe" in block
    assert "overall_universe_coverage" in block
    assert "missing_required_symbol_count" in block
    assert "oil_shock_symbol_coverage" in block
    assert "paper_position_mark_coverage" in block
    assert "david_action_required" in block
    command_block = page.split("function renderResearchDailyScorecardSection", 1)[1].split("function renderTodaySummaryGrid", 1)[0]
    assert "renderDailyResearchIntegrityAudit(payload)" in command_block
    assert "renderMarketDataUniverseSection(payload)" in command_block
