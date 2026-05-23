from __future__ import annotations

from pathlib import Path

import json
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from ops.aegis.operator_action_command_contracts_v1 import command_registry_v1

from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import OpsHandler, _aegis_kernel_status_rail_view, _operator_cockpit_payload


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
DOMAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
CSS = ROOT / "constellation_2/phaseL/ui/static/aegis.css"
RESEARCH_PIPELINE = ROOT / "ops/aegis/research_lab/research_pipeline_v1.py"
MANUAL_CAPTURE = ROOT / "ops/aegis/candidate_manual_capture_v1.py"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_four_aegis_workflow_routes_are_registered() -> None:
    for route in ["/aegis-opportunities", "/aegis-edge-lab", "/aegis-runtime-timeline", "/aegis-repair-center", "/aegis-performance", "/aegis-journal"]:
        assert route in OpsHandler.SHELL_ROUTES


def test_top_level_nav_is_workflow_first() -> None:
    nav = _text(NAV)
    domains = [line.strip() for line in nav.splitlines() if line.strip().startswith("label:")]
    assert domains[:7] == ['label: "Dashboard",', 'label: "Candidates",', 'label: "Hypotheses",', 'label: "Captured Trades",', 'label: "Runtime Timeline",', 'label: "Repair Center",', 'label: "System Health",']
    for old_label in [
        'label: "EOD Queue",',
        'label: "Operator Inbox",',
        'label: "Runtime Truth",',
        'label: "Receipts / Outcomes",',
        'label: "Sleeve Performance",',
        'label: "AI Feedback / EOD-EOW Review",',
        'label: "Feature Completion Audit",',
        'label: "Evidence",',
    ]:
        assert old_label not in domains[:7]

def test_old_aegis_pages_are_aliased_into_workflows() -> None:
    pages = pages_source_v1(ROOT)
    expected_aliases = {
        '"/aegis-today": "/aegis-opportunities"',
        '"/aegis-review": "/aegis-performance"',
        '"/aegis-research": "/aegis-edge-lab"',
        '"/aegis-history": "/aegis-journal"',
        '"/aegis-lite": "/aegis-opportunities"',
        '"/operator-inbox": "/aegis-opportunities"',
        '"/aegis-events": "/aegis-journal"',
        '"/outcomes": "/aegis-journal"',
        '"/performance": "/aegis-performance"',
        '"/aegis-ai-feedback": "/aegis-journal"',
        '"/aegis-runtime-truth": "/aegis-journal"',
        '"/reports": "/aegis-journal"',
        '"/audit": "/aegis-journal"',
    }
    for alias in expected_aliases:
        assert alias in pages


def test_workflow_pages_show_required_operator_jobs() -> None:
    pages = pages_source_v1(ROOT)

    for label in [
        "System Status",
        "Today's Summary",
        "Your tasks",
        "IB capture tickets: 0",
        "Current-Day Candidates",
        "Your task",
        "NO_USER_ACTION",
        "MONITOR_ONLY",
        "MANUAL_IB_CAPTURE_READY",
        "SYSTEM_REPAIR_REQUIRED",
        "IB capture tickets",
        "System repair",
        "Execution eligible",
        "Locked non-certified",
        "Captured Trades",
        "System Diagnostics",
        "Playbook",
        "Providers tried",
        "Validation",
        "Remediation result",
        "Health Status",
        "Performance Readiness",
        "Outcome Follow-up",
        "What Changed",
        "What We Learned",
        "Advanced Metrics",
        "EOD / EOW Review Items",
        "Hypotheses",
        "Researching",
        "Ready to Start",
        "Waiting",
        "Recommendations Ready",
        "Blocked",
        "Advanced diagnostics",
        "Background research activity",
        "Timeline Feed",
        "Entity History",
        "System / Audit Details",
        "Source artifacts and hashes",
    ]:
        assert label in pages
    commands = command_registry_v1()["commands_by_id"]
    assert commands["START_RESEARCH"]["label"] == "Start Research"
    assert commands["VIEW_FINDINGS"]["label"] == "View Findings"

def test_major_workflow_cards_include_canonical_evidence_contract() -> None:
    pages = pages_source_v1(ROOT)

    assert "renderWorkflowCard" in pages
    assert "renderCanonicalEvidenceBlock" in pages
    for label in [
        "Canonical field path",
        "Source artifact path",
        "Freshness status",
        "Generated / last updated",
        "Why shown",
        "Drilldown link",
    ]:
        assert label in pages


def test_workflow_ui_reads_canonical_cockpit_api_only() -> None:
    pages = pages_source_v1(ROOT)
    domain = _text(DOMAIN)
    workflow_block = pages.split("async function renderAegisWorkflowPage", 1)[1].split("function flattenCockpitSleeves", 1)[0]

    assert 'query("/api/aegis/operator-cockpit", params)' in domain
    assert "fetchAegisOperatorCockpit" in workflow_block
    assert "fetchAegisRuntimeTruth" not in workflow_block
    assert "fetchAegisAdaptiveIntelligence" not in workflow_block
    assert "fetchAegisIntelligenceGovernance" not in workflow_block
    assert "fetchAegisLiteExecutionQueue" not in workflow_block
    assert "build_runtime_truth_kernel_v1" not in workflow_block


def test_today_page_is_decision_first_and_runtime_is_compressed() -> None:
    pages = pages_source_v1(ROOT)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]

    assert "renderDashboardSystemStatus" in today_block
    assert "renderDashboardTodaySummary" in today_block
    assert "renderDashboardAttentionRequired" in today_block
    assert "renderDashboardRecentEvents" in today_block
    assert today_block.index("renderDashboardSystemStatus") < today_block.index("renderDashboardTodaySummary")
    assert today_block.index("renderDashboardTodaySummary") < today_block.index("renderDashboardAttentionRequired")
    assert today_block.index("renderDashboardAttentionRequired") < today_block.index("renderDashboardRecentEvents")
    assert "renderOperatorAttentionQueue" not in today_block
    assert "Review Candidate" not in today_block
    assert "Needs More Evidence" not in today_block

def test_today_missing_canonical_state_has_actionable_empty_state() -> None:
    pages = pages_source_v1(ROOT)
    missing_block = pages.split("function renderMissingCanonicalTodayWorkflow", 1)[1].split("function workflowTitle", 1)[0]

    assert "Today's operator state has not been generated yet." in missing_block
    assert "The cockpit reads canonical_operator_state.v1.json. Generate the operator state to populate today's candidates, actions, and workflow." in missing_block
    assert "npm run aegis:canonical-operator-state && npm run aegis:operator-brief" in missing_block
    assert "canonical_state" in missing_block
    assert "operator_brief" in missing_block
    assert "runtime_truth" in missing_block


def test_today_empty_and_trigger_summaries_are_operator_facing() -> None:
    pages = pages_source_v1(ROOT)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]

    for label in [
        "Market data state",
        "Candidate certification",
        "Execution eligibility",
        "Current intraday candidates",
        "IB capture tickets",
        "Final certified candidates",
        "IB capture tickets: 0",
    ]:
        assert label in pages
    assert "Event Trigger Drilldown" not in today_block
    assert "No opportunities today." not in today_block

def test_opportunities_ui_renders_no_opportunity_diagnostics() -> None:
    pages = pages_source_v1(ROOT)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]
    no_opportunity_block = pages.split("function renderNoOpportunityExplanation", 1)[1].split("function renderMissingOpportunityDiagnostics", 1)[0]

    assert "renderWorkflowCandidateTable(opportunityCandidates, opportunities)" not in today_block
    assert "Summary" in no_opportunity_block
    assert "Sleeve Run Status" in no_opportunity_block
    assert "Sleeve Readiness Summary" in no_opportunity_block
    assert "Global Context" in no_opportunity_block
    assert "Global context; not necessarily required by every sleeve." in no_opportunity_block
    assert "Market Data Provider" in no_opportunity_block
    assert "Market data provider is not configured. Aegis cannot evaluate sleeve opportunities." in no_opportunity_block
    assert "Candidate generation blocked because required symbols are missing current data." in no_opportunity_block
    assert "Provider configured" in no_opportunity_block
    assert "Universe mode" in no_opportunity_block
    assert "Dataset snapshot" in no_opportunity_block
    assert "Production scan universe" in no_opportunity_block
    assert "Sleeve required symbols" in no_opportunity_block
    assert "Total requested symbols" in no_opportunity_block
    assert "Universe source" in no_opportunity_block
    assert "Requested symbols" in no_opportunity_block
    assert "Missing symbols" in no_opportunity_block
    assert "configure provider env" in no_opportunity_block
    assert "place manual CSV drop" in no_opportunity_block
    assert "Noon Preflight Status" in no_opportunity_block
    assert "Readiness status" in no_opportunity_block
    assert "READY_FULL, READY_PARTIAL, and BLOCKED" in no_opportunity_block
    assert "Rejection Reasons" in no_opportunity_block
    assert "Trigger/Event Status" in no_opportunity_block
    assert "Recommended Next Step" in no_opportunity_block
    assert "Sleeves evaluated" in no_opportunity_block
    assert "sleeves expected" in no_opportunity_block
    assert "Blocker" in no_opportunity_block
    assert "Required missing input" in no_opportunity_block
    assert "Optional missing input" in no_opportunity_block
    assert "Next action" in no_opportunity_block
    assert "Raw signals rejected" in no_opportunity_block
    assert "Candidates passed filters" in no_opportunity_block
    assert "Why zero" in no_opportunity_block
    assert "Raw signal" in no_opportunity_block
    assert "Stage" in no_opportunity_block
    assert "Explanation" in no_opportunity_block
    assert "Required next action" in no_opportunity_block
    assert "Classification" in no_opportunity_block
    assert "volatility filter" in no_opportunity_block
    assert "confidence threshold" in no_opportunity_block
    assert "regime mismatch" in no_opportunity_block
    assert "insufficient data" in no_opportunity_block
    assert "missing event packet" in no_opportunity_block

def test_opportunities_restore_first_class_operator_actions() -> None:
    pages = pages_source_v1(ROOT)
    domain = _text(DOMAIN)
    main = _text(MAIN)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]
    task_block = pages.split("function renderDashboardAttentionRequired", 1)[1].split("function renderDashboardRecentEvents", 1)[0]
    candidates_block = pages.split("function renderAegisCandidatesWorkflow", 1)[1].split("function renderAegisReviewWorkflow", 1)[0]
    action_block = pages.split("function renderCandidateActionButtons", 1)[1].split("function renderCandidateReviewDialog", 1)[0]

    for label in [
        "Your tasks",
        "IB capture tickets: 0",
        "Current-Day Candidates",
        "Your task",
        "Record IB capture complete",
        "Open Evidence",
        "Open Sleeve",
        "Open Research Context",
    ]:
        assert label in today_block or label in task_block or label in candidates_block or label in action_block
    for task_state in ["NO_USER_ACTION", "MONITOR_ONLY", "MANUAL_IB_CAPTURE_READY", "SYSTEM_REPAIR_REQUIRED"]:
        assert task_state in pages
    for forbidden in [
        "Review Candidate",
        "Record Manual Capture",
        "Needs More Evidence",
        "Record Review",
        "REVIEW_AVAILABLE",
        "human review",
        "operator decision",
    ]:
        assert forbidden not in today_block
        assert forbidden not in task_block
        assert forbidden not in candidates_block
        assert forbidden not in action_block
    assert "executeAegisOperatorCommand" in domain
    assert "/api/aegis/operator/commands" in domain
    assert "recordAegisCandidateReview" not in domain
    assert "recordAegisManualExternalCapture" not in domain
    assert "/api/aegis/candidate-review" not in domain
    assert "/api/aegis/manual-external-capture" not in domain

def test_opportunities_projection_taxonomy_separates_tasks_diagnostics_changes_and_health() -> None:
    pages = pages_source_v1(ROOT)
    attention_block = pages.split("function renderDashboardAttentionRequired", 1)[1].split("function renderDashboardRecentEvents", 1)[0]
    diagnostics_block = pages.split("function renderSystemDiagnosticsPanel", 1)[1].split("function renderWhatChangedPanel", 1)[0]
    what_changed_block = pages.split("function renderWhatChangedPanel", 1)[1].split("function renderPassiveHealthStrip", 1)[0]
    health_block = pages.split("function renderPassiveHealthStrip", 1)[1].split("function candidateAgeLabel", 1)[0]

    assert "Your tasks" in attention_block
    assert "IB capture tickets: 0" in attention_block
    assert "manual IB capture tickets and genuine system repair items" in attention_block
    assert "operatorActions" not in attention_block
    assert "Blocked sleeve" not in attention_block
    assert "Market data warning" not in attention_block
    assert "Review drilldown" not in attention_block
    assert "System Diagnostics (" in diagnostics_block
    assert "Critical diagnostics require attention" in diagnostics_block
    assert "<details ${hasCritical ? \"open\" : \"\"}>" in diagnostics_block
    assert "System-managed issue; no operator remediation available." in diagnostics_block
    assert "No system diagnostics" in diagnostics_block
    assert "What Changed is informational only" in what_changed_block
    assert "No row is a required action" in what_changed_block
    assert "operatorSnapshotHasUnresolvedHealth(payload) ? \"SYSTEM_DIAGNOSTICS_PRESENT\" : \"NO_CHANGE\"" in what_changed_block
    assert "Passive health is compact status only" in health_block
    assert "It never emits pseudo-actions" in health_block
    assert "SYSTEM_DIAGNOSTICS_PRESENT" in diagnostics_block
    assert "Projection mismatch — refresh required" in diagnostics_block
    assert "blocked_sleeve_count" in pages
    assert "data_warning_count" in pages
    assert "runtime_truth_status" in pages



def test_unsupported_candidates_render_as_research_observations_without_capture() -> None:
    pages = pages_source_v1(ROOT)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]
    observation_block = pages.split("function renderResearchObservationCandidateTable", 1)[1].split("function renderResearchObservationActions", 1)[0]
    observation_actions = pages.split("function renderResearchObservationActions", 1)[1].split("function candidateDirectionLabel", 1)[0]

    assert "active_opportunity_projection.candidates" not in today_block
    assert "active_opportunity_projection.research_observations" not in today_block
    assert "Research Observations / Needs Evidence" not in today_block
    assert "Supporting evidence incomplete" in observation_block
    assert "Missing evidence" in observation_block
    assert "Request More Evidence" in observation_actions
    assert "Watchlist" in observation_actions
    assert "Dismiss" in observation_actions
    assert "Open Evidence" in observation_actions
    assert "Record IB capture complete" not in observation_actions
    assert "Record Manual Capture" not in observation_actions
    assert "renderManualCaptureDialog" not in observation_actions

def test_eod_outcome_ledger_renders_what_happened_today_without_tasks() -> None:
    pages = pages_source_v1(ROOT)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]
    eod_block = pages.split("function renderEodOutcomeLedgerPanel", 1)[1].split("function candidateAgeLabel", 1)[0]

    assert "renderEodOutcomeLedgerPanel" not in today_block
    assert "What happened today?" in eod_block
    assert "EOD_OUTCOME_LEDGER" in eod_block
    assert "Sleeves expected" in eod_block
    assert "Unsupported observations" in eod_block
    assert "Operator eligible" in eod_block
    assert "Manual captures" in eod_block
    assert "Research-only sleeve outcome ledger" in eod_block

def test_manual_capture_modal_is_audit_only_and_not_execution() -> None:
    pages = pages_source_v1(ROOT)
    server = _text(SERVER)
    manual_capture = _text(MANUAL_CAPTURE)
    block = pages.split("function renderManualCaptureDialog", 1)[1].split("function renderCandidateEvidencePreview", 1)[0]

    for field in [
        "manually_captured",
        "quantity",
        "capture_timestamp",
        "external_execution_venue",
        "operator_notes",
        "confidence_override",
        "paper_trade_only",
        "review_decision",
    ]:
        assert field in block
    assert "Aegis did not execute this trade." in block
    assert "MANUAL_EXTERNAL_CAPTURE_RECORDED" in manual_capture
    assert "broker_execution_allowed" in server
    assert "order_routing_allowed" in server
    assert "automatic_promotion_allowed" in server
    for forbidden in [">Buy<", ">Sell<", ">Execute<", ">Submit Order<", ">Route<", ">Allocate<"]:
        assert forbidden not in block


def test_opportunities_advisory_trust_context_is_plain_english_and_fail_closed() -> None:
    pages = pages_source_v1(ROOT)
    main = _text(MAIN)
    server = _text(SERVER)
    candidate_block = pages.split("function renderWorkflowCandidateTable", 1)[1].split("function renderCandidateReviewLedgerPanel", 1)[0]

    for label in [
        "Why Should I Trust This?",
        "Should I manually capture this?",
        "Why This Candidate Exists",
        "Evidence For",
        "Evidence Against / Missing",
        "What weakens this candidate?",
        "Sleeve Health",
        "Review Window / Expiry",
        "What would invalidate this setup?",
        "Recommended Operator Action",
        "Trust classification",
        "Guidance",
        "Evidence completeness",
        "Main missing item",
        "Next action",
        "Candidate generated, but supporting research evidence is incomplete. Manual capture is not recommended until evidence projection succeeds.",
        "Evidence projection incomplete.",
        "Insufficient evidence",
    ]:
        assert label in candidate_block
    assert "READ-ONLY GOVERNANCE · NO BROKER EXECUTION · MANUAL CAPTURE ONLY" in candidate_block
    assert "kernel-safety-strip" in main
    assert '"Partial"' in server
    assert "CANDIDATE_EXISTS_EVIDENCE_PROJECTION_INCOMPLETE" in server
    assert "/api/aegis/candidates/" in server
    assert "decision-support" in server
    assert "CandidateDecisionSupportBrief" not in pages  # UI consumes the projection, not implementation jargon.
    assert "unsupported_evidence_acknowledgement" in candidate_block
    for placeholder in [
        'row.confidence || "UNKNOWN"',
        'row.evidence_quality || row.evidence_status || "evidence not summarized"',
        'row.why_now || row.why_this_trade || row.explanation || "No summary available."',
        'row.drift_state || row.expectancy_drift_status || "not reported"',
        'row.fragility_state || row.regime_fragility_status || "not reported"',
    ]:
        assert placeholder not in candidate_block


def test_opportunities_use_operator_command_and_projection_architecture() -> None:
    pages = pages_source_v1(ROOT)
    domain = _text(DOMAIN)
    server = _text(SERVER)
    candidate_block = pages.split("function renderWorkflowCandidateTable", 1)[1].split("function renderCandidateReviewLedgerPanel", 1)[0]

    for endpoint in [
        "/api/aegis/operator/state-snapshot/latest",
        "/api/aegis/operator/today",
        "/api/aegis/operator/tasks",
        "/api/aegis/operator/diagnostics",
        "/api/aegis/operator/what-changed",
        "/api/aegis/operator/health",
        "/api/aegis/opportunities",
        "/api/aegis/review-ledger",
        "/api/aegis/eod-outcomes/latest",
        "/api/aegis/eod-outcomes/by-session/",
        "/api/aegis/eod-outcomes/sleeves/",
        "/api/aegis/operator/commands",
    ]:
        assert endpoint in domain or endpoint in server
    assert "fetchAegisOperatorStateSnapshotLatest" in pages
    assert "fetchAegisOperatorToday" in pages
    assert "fetchAegisOperatorTasks" in pages
    assert "fetchAegisOperatorDiagnostics" in pages
    assert "fetchAegisOperatorWhatChanged" in pages
    assert "fetchAegisOperatorHealth" in pages
    assert "fetchAegisOpportunitiesProjection" in pages
    assert "fetchAegisReviewLedger" in pages
    assert "fetchAegisEodOutcomeLatest" in pages
    assert "executeAegisOperatorCommand" in pages
    assert "operator_task_projection" in pages
    assert "system_diagnostic_projection" in pages
    assert "what_changed_projection" in pages
    assert "passive_health_projection" in pages
    assert "candidate_decision_projection" in candidate_block
    assert "allowed_commands" in candidate_block
    assert "source_projection_fingerprint" in candidate_block
    assert "RECORD_MANUAL_EXTERNAL_CAPTURE" in candidate_block
    assert "BUY" not in candidate_block
    assert "SELL" not in candidate_block
    assert "SUBMIT_ORDER" not in candidate_block


def test_edge_lab_research_workbench_and_human_review_workspace_exist() -> None:
    pages = pages_source_v1(ROOT)
    research_block = pages.split("function renderAegisResearchWorkflow", 1)[1].split("function renderResearchLabEvidenceProjectionCard", 1)[0]

    for label in [
        "Research Workbench",
        "Open Research",
        "Generate Evidence",
        "Run Event Study",
        "Create Challenger",
        "Start Paper Trial",
        "Review Drift",
        "Compare Challengers",
        "Prepare Human Review",
        "Archive Hypothesis",
        "Human Review Workspace",
        "HumanReviewDossier",
        "ChallengerComparisonReport",
        "ExpectancyDriftReport",
        "RegimeFragilityReport",
        "SleeveStabilityReport",
        "Research Intelligence Panels",
        "Rolling expectancy",
        "Observation growth",
    ]:
        assert label in pages or label in research_block


def test_edge_lab_is_hypotheses_workspace_not_static_metadata_viewer() -> None:
    pages = pages_source_v1(ROOT)
    css = _text(CSS)
    research_block = pages.split("function renderAegisResearchWorkflow", 1)[1].split("function renderEdgeLabCommandHeader", 1)[0]

    assert 'title: "Hypotheses"' in research_block
    assert "renderHypothesesWorkspace(consolePayload, {})" in research_block
    assert 'contextHtml: ""' in research_block
    assert "hideContextRail: true" in research_block
    assert ".hypotheses-workspace" in css
    assert "Research Command Workspace" not in research_block
    assert 'title: "Research Pipeline"' not in research_block

def test_edge_lab_simplified_operator_model_hides_internal_buttons_under_advanced() -> None:
    pages = pages_source_v1(ROOT)
    research_block = pages.split("function renderAegisResearchWorkflow", 1)[1].split("function renderAegisHistoryWorkflow", 1)[0]
    details_block = pages.split("function renderEdgeLabResearchDetails", 1)[1].split("function renderPlainEnglishHypothesisTable", 1)[0]
    advanced_block = pages.split("function renderAdvancedResearchOperations", 1)[1].split("function renderHumanReviewWorkspaceCard", 1)[0]

    for label in [
        "Review Dossier",
        "Record Paper Observation",
        "Review Degrading Sleeve",
        "Investigate Blocked Evidence",
        "Decide Whether To Continue Observation",
    ]:
        assert label in research_block
    for label in [
        "Hypothesis active",
        "Evidence generated",
        "Paper trial active",
        "Drift monitored",
        "Challenger comparison ready",
        "Human review prepared",
    ]:
        assert label in research_block
    for label in [
        "Hypothesis",
        "Current Status",
        "What Aegis Has Done",
        "What Needs Attention",
        "Next Human Action",
        "Research is already active. Aegis is monitoring this hypothesis. No manual research start is needed.",
        "No action needed. Aegis continues monitoring.",
    ]:
        assert label in pages
    assert "Advanced research operations — normally not needed." in details_block or "Advanced research operations — normally not needed." in advanced_block
    for internal_action in [
        "Generate Evidence",
        "Run Event Study",
        "Create Challenger",
        "Compare Challengers",
        "Prepare Human Review",
        "Archive Hypothesis",
    ]:
        assert internal_action in advanced_block
    assert "<details" in details_block


def test_edge_lab_research_actions_are_review_only_buttons_without_execution_language() -> None:
    pages = pages_source_v1(ROOT)
    research_block = pages.split("function renderAegisResearchWorkflow", 1)[1].split("function renderAegisHistoryWorkflow", 1)[0]

    for label in [
        "Review Dossier",
        "Record Paper Observation",
        "Review Degrading Sleeve",
        "Investigate Blocked Evidence",
        "Continue Observation",
    ]:
        assert label in research_block or label in pages
    for forbidden in [">Buy<", ">Sell<", ">Execute<", ">Submit Order<", ">Route<", ">Allocate<", "order_routing_allowed.: true"]:
        assert forbidden not in research_block
    assert "READ-ONLY GOVERNANCE" in research_block
    assert "NO BROKER EXECUTION" in research_block
    assert "MANUAL REVIEW REQUIRED" in research_block


def test_opportunities_ui_shows_noon_preflight_alert_when_email_not_delivered() -> None:
    pages = pages_source_v1(ROOT)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]
    alert_block = pages.split("function renderNoonPreflightAlert", 1)[1].split("function renderNoOpportunityExplanation", 1)[0]

    assert "renderNoonPreflightAlert(opportunities)" not in today_block
    assert "Noon preflight failed and no email alert was delivered." in alert_block
    assert "Email status" in alert_block
    assert "Next action" in alert_block
    assert "Configure C2_EMAIL_SMTP_HOST" in alert_block

def test_opportunities_ui_does_not_claim_no_opportunities_without_diagnostics() -> None:
    pages = pages_source_v1(ROOT)
    missing_block = pages.split("function renderMissingOpportunityDiagnostics", 1)[1].split("function noOpportunityMessage", 1)[0]

    assert "The UI will not claim there are no opportunities until candidate diagnostics exist." in missing_block
    assert "npm run aegis:candidate-diagnostics" in missing_block
    assert "No opportunities today." not in missing_block


def test_signal_driven_workflows_use_single_page_empty_states() -> None:
    pages = pages_source_v1(ROOT)
    review_block = pages.split("function renderAegisReviewWorkflow", 1)[1].split("function renderAegisResearchWorkflow", 1)[0]
    research_block = pages.split("function renderAegisResearchWorkflow", 1)[1].split("function renderAegisHistoryWorkflow", 1)[0]

    assert "No review work yet." in review_block
    assert "Review becomes useful after candidates are approved, ignored, expired, corrected, or assigned outcomes." in review_block
    assert "Review Opportunities for candidates." in review_block
    assert "npm run aegis:update-candidate-outcomes" in review_block
    assert 'title: "Hypotheses"' in research_block
    assert "New Research Idea" in pages
    assert "renderHypothesesWorkspace" in research_block

def test_review_is_operator_intelligence_first_and_metrics_are_advanced() -> None:
    pages = pages_source_v1(ROOT)
    review_block = pages.split("function renderAegisReviewWorkflow", 1)[1].split("function renderAegisResearchWorkflow", 1)[0]

    assert 'title: "Performance Readiness"' in review_block
    assert 'title: "Outcome Follow-up"' in review_block
    assert 'title: "What Changed"' in review_block
    assert 'title: "What We Learned"' in review_block
    assert 'title: "Advanced Metrics"' in review_block
    assert "Attribution metrics are initialized but not statistically meaningful yet." in review_block
    assert "Not enough candidate outcome history yet for meaningful attribution review." in review_block
    assert "No outcome or attribution changes yet." in review_block
    assert "<details><summary>Show ${metricRows.length} advanced attribution metrics</summary>" in review_block
    assert 'title: "Attribution Changes"' not in review_block
    assert review_block.index('title: "Performance Readiness"') < review_block.index('title: "Advanced Metrics"')


def test_review_advanced_metrics_include_expected_advisory_quality_names() -> None:
    pages = pages_source_v1(ROOT)
    metric_block = pages.split("function performanceMetricRows", 1)[1].split("function workflowContextHtml", 1)[0]

    for metric in [
        "candidate_hit_rate",
        "false_positive_rate",
        "false_negative_proxy",
        "ignored_candidate_opportunity_cost",
        "realized_vs_advisory_gap",
        "recommendation_accuracy",
        "regime_specific_candidate_quality",
        "sleeve_candidate_quality",
        "traded_vs_ignored_performance",
    ]:
        assert metric in metric_block


def test_research_workflow_displays_hypotheses_before_diagnostics() -> None:
    pages = pages_source_v1(ROOT)
    research_block = pages.split("function renderAegisResearchWorkflow", 1)[1].split("function renderAegisHistoryWorkflow", 1)[0]
    workspace = pages.split("function renderHypothesesWorkspace", 1)[1].split("function researchFilterTokens", 1)[0]

    assert "mergeCanonicalResearchState(canonical.research || {}, payload.research_priorities || {})" in pages
    assert "researchAllHypothesisRows(payload)" in workspace
    assert "renderHypothesesWorkspace" in research_block
    assert 'title: "Hypotheses"' in research_block
    assert "hypothesisViewModel(payload)" in workspace
    assert "renderHypothesesSummaryCounts(viewModel)" in workspace
    assert "renderHypothesisSection(viewModel.sections[sectionId]" in workspace
    assert "Researching" in pages
    assert "Recommendations Ready" in pages
    diagnostics = pages.split("function renderHypothesesDiagnosticsPanel", 1)[1].split("function renderHypothesesWorkspace", 1)[0]
    assert '<details class="hypotheses-diagnostics"' in diagnostics
    assert "renderHypothesesDiagnosticsPanel(payload, state)" not in workspace
    assert 'title: "Research Pipeline"' not in research_block

def test_edge_lab_kanban_shows_next_command_per_hypothesis() -> None:
    pages = pages_source_v1(ROOT)
    kanban_block = pages.split("function renderResearchPipelineKanban", 1)[1].split("function renderResearchPipelineCard", 1)[0]
    card_block = pages.split("function renderResearchPipelineCard", 1)[1].split("function researchInventorySummary", 1)[0]
    css = _text(CSS)
    pipeline = _text(RESEARCH_PIPELINE)

    for label in ["Active", "Promising", "Experimental", "Watchlist", "Blocked", "Captured", "Archived"]:
        assert label in kanban_block
    assert "Tier 1 Active" in pages
    assert "Accept for Paper Trial" in pages
    assert "lower-priority items folded" in kanban_block
    assert "item.next_command" in card_block
    assert "item.alternate_commands" in card_block
    assert "operatorResearchStateLabel" in pages
    assert "Queue for testing" in pipeline
    assert "Needs clarification" in pipeline
    assert "--decision QUEUE_TEST_PLAN" in pipeline
    assert "--decision NEEDS_CLARIFICATION" in pipeline
    assert "--decision REJECTED" in pipeline
    assert "npm run aegis:build-research-plan" in pipeline
    assert "npm run aegis:run-research-test" in pipeline
    assert "npm run aegis:record-research-review" in pipeline
    assert "Evidence needed" in card_block
    assert "Allowed transitions" in card_block
    assert "Latest result" in card_block
    assert "Next required action" in card_block
    assert "edge-pipeline-card" in card_block
    assert "edge-command-block" in card_block
    assert "Advanced diagnostics" in card_block
    assert ".edge-kanban" in css
    assert "overflow-x: auto" in css
    assert "min-width: 280px" in css
    assert "background: #0d1724" in css
    assert "background:#ffffff" not in card_block


def test_research_blocked_cards_use_operator_recovery_language() -> None:
    pages = pages_source_v1(ROOT)
    css = _text(CSS)
    card_block = pages.split("function renderResearchPipelineCard", 1)[1].split("function operatorResearchStateLabel", 1)[0]
    default_card_markup = card_block.split('<details class="edge-cli-fallback">', 1)[0]

    assert "Blocked: Market data is stale" in pages
    assert "Blocked: External dataset required" in RESEARCH_PIPELINE.read_text(encoding="utf-8")
    assert "Aegis will retry automatically at the next market refresh window." in pages
    assert "Automatic retry scheduled. No operator action required." in pages
    assert "Wait for the next market session refresh. No operator action required." in pages
    assert "Waiting for data" in pages
    assert "Waiting for current intraday market data" in pages
    assert "Aegis is waiting for current intraday market data before this research can continue." in pages
    assert "Fetch market data now" in pages
    assert "Next scheduled retry" in pages
    assert "Last fetch attempt" in pages
    assert "Aegis can fetch automatically" in pages
    assert "User must upload" in pages
    assert "User must configure provider" in pages
    assert "Not currently supported" in pages
    assert "CSV with documented columns for the dataset type." in pages
    assert "Configure provider" in RESEARCH_PIPELINE.read_text(encoding="utf-8")
    assert "AEGIS_MARKET_DATA_PRIMARY_PROVIDER" in RESEARCH_PIPELINE.read_text(encoding="utf-8")
    assert "Upload dataset" in pages
    assert "research-data-acquisition-form" in pages
    assert "refresh_required_symbol_data" in pages
    assert "Needs Data" not in pages
    assert "Needs data" not in pages
    assert "Advanced diagnostics" in card_block
    assert "Raw blockers" in card_block
    assert "warningText" not in default_card_markup
    assert "item.blocker" not in default_card_markup
    assert "renderResearchOperatorBlocker" in default_card_markup
    assert "research-blocker-panel" in css
    assert "research-recovery-row" in css


def test_edge_lab_kanban_uses_safe_ui_buttons_and_modals() -> None:
    pages = pages_source_v1(ROOT)
    domain = _text(DOMAIN)
    card_block = pages.split("function renderResearchPipelineCard", 1)[1].split("function researchInventorySummary", 1)[0]

    for label in [
        "Queue Test",
        "Reject",
        "Needs Clarification",
        "Build Test Plan",
        "Run Test",
        "Needs More Evidence",
        "Accept for Paper Trial",
        "Promote to Sleeve Review",
    ]:
        assert label in pages
    assert "Queue hypothesis for testing?" in pages
    assert "Build research test plan?" in pages
    assert "Run research test?" in pages
    assert "reason" in pages
    assert "operator" in pages
    assert "data-edge-open-modal" in card_block
    assert "edge-lab-action-form" in pages
    assert "No broker execution, no autonomous execution, no sleeve mutation, no automatic approval" in pages
    for endpoint in [
        "/api/aegis/edge-lab/hypothesis/triage",
        "/api/aegis/edge-lab/hypothesis/build-plan",
        "/api/aegis/edge-lab/hypothesis/run-test",
        "/api/aegis/edge-lab/hypothesis/review",
    ]:
        assert endpoint in domain


def test_edge_lab_ui_actions_are_handled_without_broker_or_sleeve_mutation_routes() -> None:
    main = _text(MAIN)
    server = _text(SERVER)

    assert "executeEdgeLabWorkflow" in main
    assert ".edge-lab-action-form" in main
    assert "renderRoute()" in main
    assert "execute_edge_lab_workflow_action_v1" in server
    assert "aegis_edge_lab_ui_actions_v1" in server
    assert "automatic_sleeve_mutation_allowed" in server
    assert "broker_execution_allowed" in server
    assert "autonomous_execution_allowed" in server
    assert '"/api/aegis/edge-lab/hypothesis/' in server
    assert "/api/aegis/edge-lab/broker" not in server
    assert "/api/aegis/edge-lab/sleeve/mutate" not in server


def test_today_suppresses_empty_followup_warning_cards() -> None:
    pages = pages_source_v1(ROOT)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]

    assert "renderDashboardTodaySummary" in today_block
    assert "renderDashboardAttentionRequired" in today_block
    assert "IB capture tickets: 0" in pages
    assert "if (pendingOutcomes.length)" not in today_block
    assert "if (meaningfulSleeves.length)" not in today_block
    assert "if (governanceRows.length)" not in today_block
    assert "No sleeve warnings requiring review." not in today_block
    assert "INSUFFICIENT_DATA" not in today_block

def test_dashboard_uses_system_status_without_operational_right_rail() -> None:
    pages = pages_source_v1(ROOT)
    main = _text(MAIN)
    css = _text(CSS)
    today_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function renderDashboardSystemStatus", 1)[0]
    system_status_block = pages.split("function renderDashboardSystemStatus", 1)[1].split("function renderDashboardTodaySummary", 1)[0]

    assert "renderDashboardSystemStatus(payload" in today_block
    assert 'contextHtml: ""' in today_block
    assert "hideContextRail: true" in today_block
    assert "workflowContextHtml(payload)" not in today_block

    assert "dashboard-main-only" in main
    assert "view.hideContextRail === true" in main
    assert ".dashboard-main-only .shell-context" in css
    assert "display: none" in css

    assert "Dashboard System Status is the single authoritative operational status area" in system_status_block
    assert "Market data state" in system_status_block
    assert "Candidate certification state" in system_status_block
    assert "Execution eligibility" in system_status_block
    assert "Fallback / read-only state" in system_status_block
    assert "Validation explanation" in system_status_block
    assert "Retry state" in system_status_block
    assert "Next scheduled action" in system_status_block
    assert "Certification timing" in system_status_block
    assert "renderDashboardEvidenceDrawer(payload)" in system_status_block
    assert "Current Truth" not in today_block
    assert "Current Truth" not in system_status_block


def test_non_dashboard_context_keeps_safety_boundary_without_dashboard_freshness_authority() -> None:
    pages = pages_source_v1(ROOT)
    context_block = pages.split("function workflowContextHtml", 1)[1].split("function flattenCockpitSleeves", 1)[0]

    assert "Safety Boundary" in context_block
    assert "Open drilldown links" in context_block
    assert "<details>" in context_block
    assert "No broker execution" in context_block
    assert "No submit/transmit" in context_block


def test_dashboard_main_only_layout_supports_1600_by_900() -> None:
    css = _text(CSS)

    assert ".dashboard-main-only .shell-frame" in css
    assert "grid-template-columns: 310px minmax(0, 1fr);" in css
    assert ".dashboard-main-only .shell-context" in css
    assert "display: none;" in css
    assert ".dashboard-main-only .page-content" in css
    assert "grid-template-columns: repeat(2, minmax(0, 1fr));" in css
    assert ".dashboard-main-only .page-content > .panel-card:has(.dashboard-status-metrics)" in css
    assert "grid-column: 1 / -1;" in css
    assert "@media (max-width: 1040px)" in css


def test_no_broker_or_autonomous_workflow_endpoints_are_added() -> None:
    server = _text(SERVER)
    pages = pages_source_v1(ROOT)
    for route in ["/aegis-opportunities", "/aegis-edge-lab", "/aegis-performance", "/aegis-journal", "/aegis-today", "/aegis-review", "/aegis-research", "/aegis-history", "/api/aegis/operator-cockpit"]:
        post_block = server.split("def do_POST", 1)[1].split("def do_PATCH", 1)[0]
        patch_block = server.split("def do_PATCH", 1)[1]
        assert route not in post_block
        assert route not in patch_block
    assert "broker_execution_allowed === true" in pages
    assert "autonomous_execution_allowed === true" in pages


def test_aegis_shell_rail_maps_no_candidates_to_waiting_not_blocked(tmp_path: Path) -> None:
    day = "2026-05-17"
    canonical_dir = tmp_path / "reports" / "aegis_canonical_operator_state_v1" / day
    runtime_dir = tmp_path / "reports" / "aegis_runtime_truth_kernel_v1" / day
    brief_dir = tmp_path / "reports" / "aegis_operator_brief_v1" / day
    for path in [canonical_dir, runtime_dir, brief_dir]:
        path.mkdir(parents=True, exist_ok=True)
    runtime = {
        "schema_id": "aegis_runtime_truth_kernel",
        "generated_at_utc": f"{day}T12:00:00Z",
        "runtime_truth_classification": "PARTIAL_CONTEXT",
        "highest_readiness_layer": "ADVISORY_ONLY",
        "target_operating_mode": "HUMAN_APPROVED_ADVISORY_RUNTIME",
        "trade_advice_allowed": False,
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
    }
    canonical = {
        "schema_id": "aegis_canonical_operator_state",
        "day_utc": day,
        "generated_at_utc": f"{day}T12:01:00Z",
        "runtime": runtime,
        "candidates": {"awaiting_decision": [], "approved_or_traded": [], "deferred": [], "awaiting_outcome": []},
        "top_candidates": [],
        "missing_inputs": [],
        "safety": {
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
        },
    }
    (runtime_dir / "runtime_truth_kernel.v1.json").write_text(json.dumps(runtime), encoding="utf-8")
    (canonical_dir / "canonical_operator_state.v1.json").write_text(json.dumps(canonical), encoding="utf-8")
    (brief_dir / "operator_brief.v1.json").write_text(json.dumps({"generated_at_utc": f"{day}T12:02:00Z"}), encoding="utf-8")

    cockpit = _operator_cockpit_payload(tmp_path, day)
    rail = _aegis_kernel_status_rail_view(tmp_path, day)
    statuses = {row["kernel_id"]: row["status"] for row in rail["kernels"]}

    assert cockpit["status"] == "AVAILABLE"
    assert cockpit["source_status"]["runtime_truth_generated_at"] == f"{day}T12:00:00Z"
    assert "control" not in statuses
    assert statuses["state"]["label"] == "Current"
    assert statuses["advisory"]["label"] == "None"
    assert statuses["submission"]["label"] == "Disabled By Design"
    assert statuses["lifecycle"]["label"] == "No Candidates Yet"
    assert all(status["semantic"] != "blocked" for status in statuses.values())


def test_aegis_routes_request_aegis_status_rail_surface() -> None:
    main = _text(MAIN)
    server = _text(SERVER)

    assert 'railParams.surface = "aegis"' in main
    assert 'surface == "aegis"' in server


def test_journal_is_timeline_first_memory_system() -> None:
    pages = pages_source_v1(ROOT)
    journal_block = pages.split("function renderAegisHistoryWorkflow", 1)[1].split("function renderWorkflowCandidateTable", 1)[0]

    assert "Timeline Feed" in journal_block
    assert "Entity History" in journal_block
    assert "System / Audit Details" in journal_block
    assert "What happened historically" not in journal_block
    assert "Audit / Replay / Diff" not in journal_block
    assert "journal-audit-details" in pages
    assert "journal-filter-chip" in pages
    assert "Why it matters:" in pages


def test_journal_timeline_api_and_script_are_registered() -> None:
    server = _text(SERVER)
    domain = _text(DOMAIN)
    package_json = _text(ROOT / "package.json")

    assert "/api/aegis/journal-timeline" in server
    assert "fetchAegisJournalTimeline" in domain
    assert "aegis:journal-timeline" in package_json
    assert "broker_execution_allowed" in server
    assert "autonomous_execution_allowed" in server


def test_primary_operator_ui_does_not_show_current_truth_terminology() -> None:
    pages = pages_source_v1(ROOT)
    main = _text(MAIN)
    for visible_source in [pages, main]:
        assert "Current Truth" not in visible_source
        assert "Current truth" not in visible_source
        assert "current truth" not in visible_source


def test_uncapped_operator_clickthrough_harness_covers_primary_workspaces() -> None:
    harness = (ROOT / "ops/tools/aegis_operator_clickthrough_qa_v1.py").read_text(encoding="utf-8")
    for route in [
        '"/aegis-opportunities"',
        '"/research-lab"',
        '"/aegis-runtime-timeline"',
        '"/aegis-repair-center"',
        '"/aegis-candidates"',
        '"/aegis-journal"',
    ]:
        assert route in harness
    assert '"/aegis-captured-trades"' not in harness
    assert "workspaceRoot.querySelectorAll('button, a[href], summary, [data-aegis-command-id]')" in harness
    assert "controls.slice" not in harness
    assert "clicked_count" in harness
    assert "produced browser/404 state" in harness
    assert "command click produced no visible response" in harness


def test_dashboard_surfaces_downstream_certification_blocker_message() -> None:
    pages = pages_source_v1(ROOT)
    system_status_block = pages.split("function renderDashboardSystemStatus", 1)[1].split("function renderDashboardTodaySummary", 1)[0]
    assert "downstream_certification_invariant" in system_status_block
    assert "data-downstream-certification-blocker" in system_status_block
    assert "US_EQUITIES_EOD is certified, but downstream market-data readiness is still blocked." in system_status_block
