#!/usr/bin/env python3
"""
Constellation 2.0 — Phase L — Live Ops Dashboard

- Serves static UI + JSON API
- Reads canonical runtime truth artifacts
- Never talks to IB, never submits orders
- Configuration API writes are draft-governed and activation-audited
- Fail-closed: missing artifacts are surfaced with explicit error codes + file pointers
- Minimal deps: Python stdlib only
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import date, datetime, timezone, timedelta
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

_BOOTSTRAP_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_BOOTSTRAP_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_REPO_ROOT))

from constellation_2.common.operator_control_plane_v1 import (
    build_operator_home_bundle,
    build_operator_query_bundle,
)
from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.runtime_contract_v1 import (
    resolve_canonical_truth_root,
    resolve_truth_sleeves_root,
)
from ops.aegis.runtime_truth_kernel_v1 import build_runtime_truth_kernel_v1, read_runtime_state_history_summary_v1
from ops.tools.aegis_submit_enforcement_v1 import packet_currentness_v1
from ops.aegis.intelligence_common_v1 import intelligence_summaries_v1, latest_json_v1
from ops.aegis.canonical_operator_state_v1 import build_canonical_operator_state_v1, write_canonical_operator_state_v1
from ops.aegis.candidate_lifecycle_v1 import append_candidate_review_action_v1, update_candidate_outcomes_v1
from ops.aegis.position_management_v1 import (
    append_position_event_correction_v1,
    append_position_risk_plan_v1,
    append_stop_event_v1,
    build_position_management_v1,
    write_position_management_reports_v1,
)
from ops.aegis.journal.journal_event_v1 import build_journal_timeline_v1, write_journal_timeline_v1
from ops.aegis.candidate_manual_capture_v1 import append_manual_external_capture_v1
from ops.aegis.candidate_decision_support_v1 import (
    build_candidate_decision_support_payload_v1,
    find_candidate_for_decision_support_v1,
    build_candidate_decision_support_brief_v1,
)
from ops.aegis.operator_command_v1 import (
    build_operator_projections_v1,
    execute_operator_command_v1,
    operator_command_registry_v1,
)
from ops.aegis.operator_action_command_contracts_v1 import (
    command_registry_v1 as aegis_command_registry_v1,
    execute_aegis_command_v1,
    validate_command_registry_v1 as validate_aegis_command_registry_v1,
)
from ops.aegis.domain_certification_v1 import build_domain_certification_report_v1
from ops.aegis.domain_repair_orchestrator_v1 import run_domain_repair_orchestration_v1
from ops.aegis.repair_center_projection_v1 import build_repair_center_projection_v1
from ops.aegis.data_remediation_v1 import (
    classify_data_blockers_v1,
    get_remediation_attempt_v1,
    latest_remediation_v1,
    run_data_remediation_v1,
)
from ops.aegis.operator_state.canonical_operator_state_builder_v1 import (
    load_or_build_operator_state_snapshot_response_v1,
    api_envelope_v1 as operator_state_api_envelope_v1,
)
from ops.aegis.operator_state.current_operator_truth_resolver_v1 import (
    api_envelope_v1 as current_operator_truth_api_envelope_v1,
    resolve_current_operator_truth_v1,
)
from ops.aegis.operator_state.manual_capture_record_v1 import (
    append_manual_capture_record_v1,
    latest_manual_capture_record_v1,
    list_manual_capture_records_v1,
)
from ops.aegis.thesis_graph.thesis_graph_v1 import load_or_build_thesis_graph_response_v1
# Compatibility: legacy trade_candidate_projection_v1 remains in snapshots; manual-capture authority is TradeLifecycleCase -> ReadinessDomainEvaluation -> TradeTicketProjection.
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import (
    latest_paper_trade_construction_v1,
    paper_trade_construction_view_v1,
)
from ops.aegis.trade_lifecycle.trade_lifecycle_case_v1 import latest_trade_lifecycle_case_v1
from ops.aegis.trade_lifecycle.trade_case_projection_v1 import trade_case_projection_v1
from ops.aegis.trade_lifecycle.trade_ticket_projection_v1 import (
    latest_trade_ticket_projection_v1,
    trade_ticket_projection_v1,
)
from ops.aegis.universe.canonical_universe_authority_v1 import (
    canonical_universe_health_v1,
    latest_canonical_universe_authority_v1,
)
from ops.aegis.trade_lifecycle.readiness_domain_evaluation_v1 import (
    blockers_by_domain_v1,
    domain_status_map_v1,
    latest_readiness_domain_evaluations_v1,
)
from ops.aegis.eod_opportunity_outcome_report_v1 import (
    build_eod_opportunity_outcome_report_v1,
    eod_sleeve_history_v1,
    read_eod_opportunity_outcome_report_by_id_v1,
    read_eod_opportunity_outcome_report_v1,
    read_latest_eod_opportunity_outcome_report_v1,
)
from ops.aegis.candidate_review_ledger_v1 import build_candidate_review_ledger_v1, filter_candidate_review_ledger_v1, write_candidate_review_ledger_v1
from ops.aegis.research_hypothesis_classification_v1 import (
    build_research_hypothesis_classification_v1,
    write_research_hypothesis_classification_v1,
)
from ops.aegis.research_lab.research_pipeline_v1 import (
    append_research_review_decision_v1,
    append_triage_record_v1,
    build_research_pipeline_v1,
    build_research_plan_v1,
    run_research_test_v1,
    write_research_pipeline_v1,
    write_research_plan_v1,
    write_research_test_result_v1,
)
from ops.aegis.research_lab.research_lab_routes import (
    research_lab_blocked_evidence_console_v1,
    research_lab_blocked_work_console_v1,
    research_lab_console_action_failure_response_v1,
    research_lab_console_assess_hypothesis_v1,
    research_lab_console_convert_hypothesis_v1,
    research_lab_console_dossier_v1,
    research_lab_console_review_hypothesis_v1,
    research_lab_console_v1,
    research_lab_challenger_comparison_report_v1,
    research_lab_challenger_comparison_reports_v1,
    research_lab_challenger_evidence_batch_v1,
    research_lab_challenger_evidence_batches_v1,
    research_lab_challenger_evidence_completeness_v1,
    research_lab_challenger_evidence_lineage_v1,
    research_lab_challenger_blockers_v1,
    research_lab_backlog_priority_v1,
    research_lab_challenger_track_v1,
    research_lab_challenger_tracks_v1,
    research_lab_challenger_variant_v1,
    research_lab_challenger_variants_v1,
    research_lab_edge_lab_projection_v1,
    research_lab_expectancy_drift_latest_v1,
    research_lab_evidence_console_v1,
    research_lab_evidence_inventory_v1,
    research_lab_evidence_chain_v1,
    research_lab_evidence_summary_v1,
    research_lab_health_v1,
    research_lab_human_review_dossier_v1,
    research_lab_human_review_dossiers_v1,
    research_lab_human_review_decision_latest_v1,
    research_lab_human_review_decision_read_model_v1,
    research_lab_human_review_decision_v1,
    research_lab_human_review_decisions_v1,
    research_lab_human_review_dossier_decisions_v1,
    research_lab_human_review_decision_paper_trial_proposals_v1,
    research_lab_hypothesis_intake_batch_latest_v1,
    research_lab_hypothesis_intake_batch_v1,
    research_lab_hypothesis_intake_batches_v1,
    research_lab_hypothesis_intake_decision_latest_v1,
    research_lab_hypothesis_intake_decision_v1,
    research_lab_hypothesis_intake_decisions_v1,
    research_lab_hypothesis_proposal_batch_latest_v1,
    research_lab_hypothesis_proposal_batch_v1,
    research_lab_hypothesis_proposal_batches_v1,
    research_lab_hypothesis_proposal_latest_v1,
    research_lab_hypothesis_proposal_review_batch_latest_v1,
    research_lab_hypothesis_proposal_review_batch_v1,
    research_lab_hypothesis_proposal_review_batches_v1,
    research_lab_hypothesis_proposal_review_latest_v1,
    research_lab_hypothesis_proposal_review_v1,
    research_lab_hypothesis_proposal_reviews_for_proposal_v1,
    research_lab_hypothesis_proposal_reviews_v1,
    research_lab_hypothesis_proposal_v1,
    research_lab_hypothesis_queue_v1,
    research_lab_projection_health_v1,
    research_lab_rebuild_projections_v1,
    research_lab_validate_projections_v1,
    research_lab_hypothesis_proposals_v1,
    research_lab_hypothesis_proposal_assess_readiness_v1,
    research_lab_hypothesis_proposal_dossier_v1,
    research_lab_hypothesis_proposal_review_append_v1,
    research_lab_research_intake_queue_v1,
    research_lab_research_plans_console_v1,
    research_lab_integrity_latest_summary_v1,
    research_lab_integrity_report_latest_v1,
    research_lab_integrity_report_v1,
    research_lab_integrity_reports_v1,
    research_lab_operator_home_v1,
    research_lab_observation_candidate_batch_latest_v1,
    research_lab_observation_candidate_batch_v1,
    research_lab_observation_candidate_batches_v1,
    research_lab_observation_candidate_latest_v1,
    research_lab_observation_candidate_v1,
    research_lab_observation_candidates_v1,
    research_lab_observation_cluster_batch_latest_v1,
    research_lab_observation_cluster_batch_v1,
    research_lab_observation_cluster_batches_v1,
    research_lab_observation_cluster_latest_v1,
    research_lab_observation_cluster_v1,
    research_lab_observation_cluster_hypothesis_proposals_v1,
    research_lab_observation_clusters_v1,
    research_lab_paper_trial_inventory_v1,
    research_lab_paper_trial_proposal_latest_v1,
    research_lab_paper_trial_proposal_v1,
    research_lab_paper_trial_proposals_v1,
    research_lab_paper_trial_summary_v1,
    research_lab_paper_trials_console_v1,
    research_lab_regime_fragility_latest_v1,
    research_lab_research_hypothesis_latest_v1,
    research_lab_research_backlog_console_v1,
    research_lab_research_hypothesis_v1,
    research_lab_research_hypotheses_v1,
    research_lab_sleeve_review_center_console_v1,
    research_lab_sleeve_v1,
    research_lab_sleeve_comparison_v1,
    research_lab_sleeve_stability_latest_v1,
    research_lab_sleeve_stability_v1,
    research_lab_sleeves_v1,
    research_lab_status_latest_v1,
    research_lab_status_report_v1,
    research_lab_start_research_options_v1,
    research_lab_start_research_v1,
    research_lab_status_reports_v1,
    research_lab_explicit_action_placeholder_v1,
)
from ops.tools.write_aegis_operator_brief_v1 import build_operator_brief_v1, write_operator_brief_v1
from constellation_2.phaseL.ui_api import (
    STATUS_SEMANTICS,
    build_action_inventory,
    build_advisory_view,
    build_aegis_lite_execution_queue_view,
    build_aegis_lite_ui_health_view,
    build_aegis_event_monitoring_view,
    build_alerts_view,
    build_capital_accounts_view,
    build_capital_allocation_view,
    build_capital_cashflow_view,
    build_capital_flows_view,
    build_capital_history_view,
    build_capital_overview_view,
    build_capital_query_surface_v1,
    build_capital_validation_view,
    build_command_overview_view,
    build_configuration_catalog_v1,
    build_configuration_current_v1,
    build_financial_state_view,
    build_kernel_status_rail_view,
    build_kernel_status_rail_summary_view,
    build_integrity_view,
    build_operations_view,
    build_opportunity_state_view,
    build_operator_work_queue_view,
    build_orders_view,
    build_operator_workflow_summary,
    build_outcome_state_view,
    build_policy_evolution_view,
    build_positions_view,
    build_reconciliation_view,
    build_readiness_kernel_v1,
    build_refinement_state_view,
    build_sleeve_evaluation_view,
    build_system_summary_view,
    build_tax_state_view,
    build_value_state_view,
    build_workspace_view,
    create_configuration_draft_v1,
    create_reliability_fix_attempt_v1,
    create_reliability_issue_v1,
    create_reliability_issue_verification_v1,
    create_reliability_issue_work_order_v1,
    create_reliability_observation_v1,
    create_reliability_verification_v1,
    create_reliability_work_order_fix_attempt_v1,
    create_reliability_work_order_from_issue_v1,
    create_reliability_work_order_v1,
    dispatch_kernel_command,
    draft_reliability_issue_v1,
    get_reliability_fix_attempt_v1,
    get_latest_reliability_readiness_v1,
    get_operator_state,
    get_configuration_draft_v1,
    get_reliability_issue_v1,
    get_reliability_readiness_v1,
    get_reliability_verification_v1,
    get_reliability_work_order_v1,
    list_action_audit_entries,
    list_reliability_fix_attempts_v1,
    list_reliability_issue_verifications_v1,
    list_reliability_issue_work_orders_v1,
    list_reliability_next_actions_v1,
    link_reliability_issue_observation_v1,
    list_reliability_issues_v1,
    list_reliability_observations_v1,
    list_reliability_verifications_v1,
    list_reliability_work_order_fix_attempts_v1,
    list_reliability_work_orders_v1,
    record_reliability_fix_attempt_v1,
    reject_configuration_draft_v1,
    resolve_effective_capital_cashflow_inputs_v1,
    review_configuration_draft_v1,
    run_action,
    assess_reliability_readiness_v1,
    update_reliability_fix_attempt_v1,
    update_reliability_issue_v1,
    update_reliability_verification_v1,
    update_reliability_work_order_v1,
    validate_configuration_draft_v1,
    activate_configuration_draft_v1,
    verify_reliability_issue_v1,
)
from constellation_2.phaseL.ui_api.configuration_workflow_v1 import ConfigurationWorkflowApiError
from constellation_2.phaseL.ui_api.common import ADVISORY_RUNTIME_ROOT, GLOBAL_TRUTH_ROOT, SLEEVE_TRUTH_ROOT
# --------------------------
# Error codes (audit-safe)
# --------------------------

E_TRUTH_ROOT_MISSING = "TRUTH_ROOT_MISSING"
E_SUBMISSIONS_ROOT_MISSING = "SUBMISSIONS_ROOT_MISSING"
E_NO_DAYS_FOUND = "NO_DAYS_FOUND"
E_DAY_INVALID = "DAY_INVALID"
E_NO_SUBMISSIONS_FOUND = "NO_SUBMISSIONS_FOUND"
E_NO_ORDER_PLAN_PRESENT = "NO_ORDER_PLAN_PRESENT"
E_NAV_MISSING = "NAV_MISSING"
E_ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE = "ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE"

# Activity endpoints
E_ACTIVITY_DAY_NOT_RESOLVED = "ACTIVITY_DAY_NOT_RESOLVED"
E_ACTIVITY_ARTIFACT_MISSING = "ACTIVITY_ARTIFACT_MISSING"
E_ACTIVITY_ARTIFACT_UNREADABLE = "ACTIVITY_ARTIFACT_UNREADABLE"

# Submission index (day-level) — preferred for speed when present (legacy path used by this UI)
SUBMISSION_INDEX_SCHEMA_ID = "C2_SUBMISSION_INDEX_V1"
SUBMISSION_INDEX_SCHEMA_VERSION = 1
SUBMISSION_INDEX_FILENAME = "submission_index.v1.json"

# Pillars decision record (preferred submission evidence surface)
PILLARS_DECISION_SCHEMA_ID = "submission_decision_record"
PILLARS_DECISION_SCHEMA_VERSION = "v1"
PILLARS_DECISION_SUFFIX = ".submission_decision_record.v1.json"

# --------------------------
# Repo / truth roots (deterministic)
# --------------------------

THIS_FILE = Path(__file__).resolve()
# .../constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py
# parents: [server, ui, phaseL, constellation_2, <repo_root>, ...]
REPO_ROOT = THIS_FILE.parents[4]
TRUTH_ROOT = SLEEVE_TRUTH_ROOT
RUNTIME_ROOT = Path(os.environ.get("C2_RUNTIME_STATE_ROOT", "/home/node/constellation_runtime_data/runtime")).resolve()
PROJECTION_CONTRACT_VERSION = "aegis_ui_projection.v1"
PERFORMANCE_SHOWCASE_FAMILY = "aegis_performance_showcase_v1"
PERFORMANCE_SHOWCASE_HTML = "aegis_performance_showcase.v1.html"
EDGE_LAB_UI_ACTION_FAMILY = "aegis_edge_lab_ui_actions_v1"


class EdgeLabWorkflowApiError(Exception):
    def __init__(self, message: str, *, status_code: HTTPStatus = HTTPStatus.BAD_REQUEST, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details or {}


def _edge_lab_safety_fields() -> Dict[str, bool]:
    return {
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
        "human_approval_required": True,
    }


def _edge_lab_request_hash(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _edge_lab_action_id(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(f"{_utc_now_iso()}|{_edge_lab_request_hash(payload)}".encode("utf-8")).hexdigest()[:24]


def _edge_lab_day(day: Optional[str]) -> str:
    return day if isinstance(day, str) and _is_day_str(day) else date.today().isoformat()


def _edge_lab_item(payload: Dict[str, Any], hypothesis_id: str) -> Dict[str, Any]:
    for item in payload.get("items") or []:
        if str(item.get("hypothesis_id") or "") == str(hypothesis_id):
            return item
    raise EdgeLabWorkflowApiError("Hypothesis not found.", status_code=HTTPStatus.NOT_FOUND, details={"hypothesis_id": hypothesis_id})


def _edge_lab_regenerate(root: Path, day: str) -> Dict[str, Any]:
    classification = build_research_hypothesis_classification_v1(truth_root=root, day_utc=day)
    classification_paths = write_research_hypothesis_classification_v1(truth_root=root, day_utc=day, payload=classification)
    pipeline = build_research_pipeline_v1(truth_root=root, day_utc=day)
    pipeline_paths = write_research_pipeline_v1(truth_root=root, day_utc=day, payload=pipeline)
    canonical = build_canonical_operator_state_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=day)
    canonical_paths = write_canonical_operator_state_v1(truth_root=root, day_utc=day, payload=canonical)
    brief = build_operator_brief_v1(canonical=canonical, canonical_path=Path(canonical_paths["json"]))
    brief_paths = write_operator_brief_v1(truth_root=root, day_utc=day, payload=brief)
    return {
        "classification": classification_paths,
        "research_pipeline": pipeline_paths,
        "canonical_operator_state": canonical_paths,
        "operator_brief": brief_paths,
        "pipeline": pipeline,
    }


def _append_edge_lab_ui_action(root: Path, day: str, event: Dict[str, Any]) -> Path:
    out_dir = root / "reports" / EDGE_LAB_UI_ACTION_FAMILY / day
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "edge_lab_ui_actions.v1.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return path


def _edge_lab_action_response(root: Path, day: str, request_payload: Dict[str, Any], *, action_type: str, hypothesis_id: str, from_gate: str, to_gate: str, generated_artifacts: Dict[str, Any], result: Dict[str, Any], operator: str, reason: str, success: bool = True, error: str = "") -> Dict[str, Any]:
    refreshed = _edge_lab_regenerate(root, day)
    updated_item = _edge_lab_item(refreshed["pipeline"], hypothesis_id)
    event = {
        "action_id": _edge_lab_action_id(request_payload),
        "hypothesis_id": hypothesis_id,
        "from_gate": from_gate,
        "to_gate": updated_item.get("current_gate") or to_gate,
        "action_type": action_type,
        "operator": operator,
        "reason": reason,
        "timestamp_utc": _utc_now_iso(),
        "request_payload_hash": _edge_lab_request_hash(request_payload),
        "generated_artifacts": generated_artifacts,
        **_edge_lab_safety_fields(),
        "success": success,
        "error": error,
    }
    action_log = _append_edge_lab_ui_action(root, day, event)
    return {
        "ok": success,
        "day_utc": day,
        "hypothesis_id": hypothesis_id,
        "from_gate": from_gate,
        "to_gate": updated_item.get("current_gate"),
        "message": _edge_lab_message(action_type, updated_item, result),
        "hypothesis": updated_item,
        "result": result,
        "generated_artifacts": generated_artifacts,
        "edge_lab_ui_action_log": str(action_log),
        **_edge_lab_safety_fields(),
    }


def _edge_lab_message(action_type: str, item: Dict[str, Any], result: Dict[str, Any]) -> str:
    if action_type == "TRIAGE":
        gate = str(item.get("current_gate") or "")
        if gate == "TEST_PLAN":
            return "Queued for test planning."
        if gate == "REJECTED_ARCHIVED":
            return "Hypothesis rejected or archived."
        return "Hypothesis triage recorded."
    if action_type == "BUILD_PLAN":
        if str(item.get("current_gate") or "") == "TESTING":
            return "Research plan created. Ready for testing."
        return "Research plan created; gate blocker remains visible."
    if action_type == "RUN_TEST":
        status = str(result.get("test_status") or "UNKNOWN")
        if status == "DATA_NEEDED":
            blocker = str(result.get("blocker") or "")
            if blocker == "EARNINGS_EVENT_CALENDAR_REQUIRED":
                return "Cannot run test yet. External earnings calendar required."
            if blocker == "EVENT_WINDOW_OHLCV_DATA_REQUIRED":
                return "Cannot run test yet. Event-window OHLCV dataset required."
            return str(result.get("result_summary") or "Cannot run test yet. Required research data is missing.")
        if status == "TEST_NOT_IMPLEMENTED":
            return "Test runner not implemented for this event study."
        return "Research test result generated for review."
    if action_type == "REVIEW":
        return "Research review decision recorded."
    return "Edge Lab action completed."


def execute_edge_lab_workflow_action_v1(*, truth_root: Path, day_utc: str, endpoint: str, request_payload: Dict[str, Any]) -> Dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = _edge_lab_day(day_utc)
    hypothesis_id = str(request_payload.get("hypothesis_id") or "").strip()
    if not hypothesis_id:
        raise EdgeLabWorkflowApiError("hypothesis_id is required.", details={"field": "hypothesis_id"})
    operator = str(request_payload.get("operator") or "David").strip() or "David"
    reason = str(request_payload.get("reason") or "").strip()
    pipeline = build_research_pipeline_v1(truth_root=root, day_utc=day)
    item = _edge_lab_item(pipeline, hypothesis_id)
    from_gate = str(item.get("current_gate") or "UNKNOWN")

    if endpoint == "triage":
        decision = str(request_payload.get("decision") or "").strip().upper()
        if not reason:
            raise EdgeLabWorkflowApiError("reason is required for triage.")
        allowed = {
            "QUEUE_TEST_PLAN": {"from": {"INBOX", "TRIAGE"}, "to": "TEST_PLAN"},
            "REJECT": {"from": {"INBOX", "TRIAGE"}, "to": "REJECTED_ARCHIVED"},
            "ARCHIVE": {"from": {"INBOX", "TRIAGE"}, "to": "REJECTED_ARCHIVED"},
            "NEEDS_CLARIFICATION": {"from": {"INBOX"}, "to": "TRIAGE"},
        }
        spec = allowed.get(decision)
        if not spec or from_gate not in spec["from"]:
            raise EdgeLabWorkflowApiError("Invalid research pipeline transition.", status_code=HTTPStatus.CONFLICT, details={"from_gate": from_gate, "decision": decision})
        event = append_triage_record_v1(truth_root=root, day_utc=day, hypothesis_id=hypothesis_id, decision=decision, reason=reason, operator=operator)
        return _edge_lab_action_response(root, day, request_payload, action_type="TRIAGE", hypothesis_id=hypothesis_id, from_gate=from_gate, to_gate=spec["to"], generated_artifacts={"triage_event": event.get("path", "")}, result=event, operator=operator, reason=reason)

    if endpoint == "build-plan":
        if from_gate != "TEST_PLAN":
            raise EdgeLabWorkflowApiError("Invalid research pipeline transition.", status_code=HTTPStatus.CONFLICT, details={"from_gate": from_gate, "required_gate": "TEST_PLAN"})
        plan = build_research_plan_v1(truth_root=root, day_utc=day, hypothesis_id=hypothesis_id)
        paths = write_research_plan_v1(truth_root=root, day_utc=day, payload=plan)
        return _edge_lab_action_response(root, day, request_payload, action_type="BUILD_PLAN", hypothesis_id=hypothesis_id, from_gate=from_gate, to_gate="TESTING", generated_artifacts=paths, result=plan, operator=operator, reason=reason or "UI confirmed build research test plan.")

    if endpoint == "run-test":
        if from_gate != "TESTING":
            raise EdgeLabWorkflowApiError("Invalid research pipeline transition.", status_code=HTTPStatus.CONFLICT, details={"from_gate": from_gate, "required_gate": "TESTING"})
        result = run_research_test_v1(truth_root=root, day_utc=day, hypothesis_id=hypothesis_id)
        paths = write_research_test_result_v1(truth_root=root, day_utc=day, payload=result)
        return _edge_lab_action_response(root, day, request_payload, action_type="RUN_TEST", hypothesis_id=hypothesis_id, from_gate=from_gate, to_gate="RESULT_REVIEW", generated_artifacts=paths, result=result, operator=operator, reason=reason or "UI confirmed run research test.")

    if endpoint == "review":
        decision = str(request_payload.get("decision") or "").strip().upper()
        if not reason:
            raise EdgeLabWorkflowApiError("reason is required for review.")
        allowed = {
            "PAPER_TEST_CANDIDATE": "PAPER_TRIAL",
            "SLEEVE_REVIEW_CANDIDATE": "SLEEVE_REVIEW",
            "REJECTED": "REJECTED_ARCHIVED",
            "REJECT": "REJECTED_ARCHIVED",
            "ARCHIVE": "REJECTED_ARCHIVED",
            "NEEDS_MORE_EVIDENCE": "RESULT_REVIEW",
        }
        if from_gate != "RESULT_REVIEW" or decision not in allowed:
            raise EdgeLabWorkflowApiError("Invalid research pipeline transition.", status_code=HTTPStatus.CONFLICT, details={"from_gate": from_gate, "decision": decision})
        event = append_research_review_decision_v1(truth_root=root, day_utc=day, hypothesis_id=hypothesis_id, decision=decision, reason=reason, operator=operator)
        return _edge_lab_action_response(root, day, request_payload, action_type="REVIEW", hypothesis_id=hypothesis_id, from_gate=from_gate, to_gate=allowed[decision], generated_artifacts={"review_event": event.get("path", "")}, result=event, operator=operator, reason=reason)

    raise EdgeLabWorkflowApiError("Endpoint not found.", status_code=HTTPStatus.NOT_FOUND, details={"endpoint": endpoint})


def _known_truth_roots() -> List[Path]:
    roots: List[Path] = []
    for r in [TRUTH_ROOT, SLEEVE_TRUTH_ROOT]:
        if isinstance(r, Path) and r.exists() and r.is_dir():
            roots.append(r.resolve())
    uniq: List[Path] = []
    seen = set()
    for r in roots:
        s = str(r)
        if s in seen:
            continue
        seen.add(s)
        uniq.append(r)
    return uniq


def _has_orchestrator_day(truth_root: Path, day: str) -> bool:
    p = (truth_root / "reports" / "orchestrator_run_verdict_v2" / day).resolve()
    return p.exists() and p.is_dir()


def _truth_root_for_day(day: Optional[str]) -> Path:
    if not isinstance(day, str) or not _is_day_str(day):
        return TRUTH_ROOT

    roots = _known_truth_roots()
    if not roots:
        return TRUTH_ROOT

    for r in roots:
        if _has_orchestrator_day(r, day):
            return r
    return roots[0]


# Canonical surfaces (as proven on disk)
SUBMISSIONS_ROOT = (TRUTH_ROOT / "execution_evidence_v1" / "submissions").resolve()
INTENTS_ROOT = (TRUTH_ROOT / "intents_v1" / "snapshots").resolve()
GATE_VERDICT_ROOT = (TRUTH_ROOT / "reports" / "gate_stack_verdict_v1").resolve()

ACCOUNTING_NAV_ROOT = (TRUTH_ROOT / "accounting_v2" / "nav").resolve()
ACCOUNTING_ATTR_ROOT = (TRUTH_ROOT / "accounting_v2" / "attribution").resolve()
ENGINE_LINKAGE_ROOT = (TRUTH_ROOT / "engine_linkage_v1").resolve()

# Pillars roots (preferred submission evidence)
PILLARS_V1_ROOT = (TRUTH_ROOT / "pillars_v1").resolve()
PILLARS_V1R1_ROOT = (TRUTH_ROOT / "pillars_v1r1").resolve()

# Activity monitoring roots (authoritative)
INTENTS_SUMMARY_ROOT = (TRUTH_ROOT / "monitoring_v1" / "intents_summary_v1").resolve()
SUBMISSIONS_SUMMARY_ROOT = (TRUTH_ROOT / "monitoring_v1" / "submissions_summary_v1").resolve()
ACTIVITY_ROLLUP_ROOT = (TRUTH_ROOT / "monitoring_v1" / "activity_ledger_rollup_v1").resolve()

# Instance config (service provides path via env; fail-closed if missing)
def _instance_config_path() -> Path:
    # C2_INSTANCE_CONFIG is set by systemd unit; if absent, use a non-existent sentinel to surface MISSING deterministically.
    import os
    raw = os.environ.get("C2_INSTANCE_CONFIG") or ""
    p = Path(raw) if raw else (REPO_ROOT / "__MISSING_INSTANCE_CONFIG__").resolve()
    return p.resolve()

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


SERVICE_STARTED_AT_UTC = _utc_now_iso()


def _service_version() -> Optional[str]:
    import os

    env_version = (os.environ.get("C2_SERVICE_VERSION") or "").strip()
    if env_version:
        return env_version

    package_path = (REPO_ROOT / "package.json").resolve()
    payload, _ = _safe_read_json(package_path)
    if isinstance(payload, dict):
        raw_version = payload.get("version")
        if isinstance(raw_version, str) and raw_version.strip():
            return raw_version.strip()
    return None


def _safe_read_json(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"


def _mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except Exception:
        return None


def _is_day_str(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        return False


def _canonical_truth_root() -> Path:
    try:
        return resolve_canonical_truth_root().resolve()
    except Exception:
        return GLOBAL_TRUTH_ROOT.resolve()


def _runtime_truth_root() -> Path:
    try:
        return (resolve_truth_sleeves_root().resolve() / "PRIMARY" / "PAPER").resolve()
    except Exception:
        return SLEEVE_TRUTH_ROOT.resolve()


def _latest_packet_path() -> Path:
    return (
        Path("/home/node/constellation_runtime_data")
        / "exports"
        / "aegis_state"
        / "latest"
        / "chatgpt_aegis_packet.md"
    ).resolve()


def _json_error_payload(message: str) -> dict[str, Any]:
    try:
        payload = json.loads(message)
    except Exception:
        return {"message": message}
    return payload if isinstance(payload, dict) else {"message": message}


def _projection_day(raw_day: Optional[str] = None) -> str:
    if isinstance(raw_day, str) and _is_day_str(raw_day):
        return raw_day
    day_run_root = (_canonical_truth_root() / "reports" / "aegis_day_run_v1").resolve()
    days = [day for day in _list_day_dirs(day_run_root) if day <= date.today().isoformat()]
    if days:
        return days[-1]
    return date.today().isoformat()


def _operator_truth_day(raw_day: Optional[str] = None) -> str:
    if isinstance(raw_day, str) and _is_day_str(raw_day):
        return raw_day
    return date.today().isoformat()


def _enqueue_data_remediation_job_v1(*, truth_root: Path, day_utc: str, playbook_id: str, request_payload: Dict[str, Any]) -> Dict[str, Any]:
    playbook = playbook_id or "refresh_required_symbol_data"
    job_key = {"day_utc": day_utc, "playbook_id": playbook, "symbols": request_payload.get("symbols") or []}
    job_id = f"data-remediation-job:{day_utc}:{hashlib.sha256(json.dumps(job_key, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]}"
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / "aegis_data_remediation_jobs_v1" / day_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{job_id.replace(':', '_')}.json"
    market_path = Path(truth_root).expanduser().resolve() / "reports" / "aegis_market_data_v1" / day_utc / "market_data.v1.json"
    market: Dict[str, Any] = {}
    try:
        market = json.loads(market_path.read_text(encoding="utf-8")) if market_path.exists() else {}
    except Exception:
        market = {}
    now = datetime.now(timezone.utc).replace(microsecond=0)
    current_truth: Dict[str, Any] = {}
    current_day_status: Dict[str, Any] = {}
    try:
        current_truth = resolve_current_operator_truth_v1(truth_root=truth_root, day_utc=day_utc)
        current_day_status = current_truth.get("current_day_status") if isinstance(current_truth.get("current_day_status"), dict) else {}
    except Exception:
        current_truth = {}
        current_day_status = {}
    dynamic_status = {
        "last_attempt": market.get("last_attempt") if isinstance(market.get("last_attempt"), dict) else {"attempted_at_utc": str(market.get("generated_at_utc") or current_day_status.get("last_attempt_time") or ""), "provider_status": str(market.get("status") or current_day_status.get("status") or ""), "error": str(market.get("failure_reason") or current_day_status.get("blocker") or "")},
        "last_error": str(market.get("failure_reason") or current_day_status.get("blocker") or ""),
        "next_retry_utc": str(market.get("next_retry_utc") or current_day_status.get("next_retry_utc") or (now + timedelta(minutes=15)).isoformat().replace("+00:00", "Z")),
        "validation_status": str(market.get("freshness_state") or market.get("validation_status") or current_day_status.get("validation_status") or current_day_status.get("freshness_state") or market.get("operator_market_data_state") or "UNKNOWN"),
    }
    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                refreshed = {**existing, **dynamic_status, "job_already_existed": True, "job_path": str(out_path)}
                out_path.write_text(json.dumps({k: v for k, v in refreshed.items() if k not in {"job_already_existed", "job_path"}}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                return refreshed
        except Exception:
            pass
    payload = {
        "schema_id": "aegis_data_remediation_background_job",
        "schema_version": "v1",
        "job_id": job_id,
        "day_utc": day_utc,
        "playbook_id": playbook,
        "status": "QUEUED",
        "idempotency_key": hashlib.sha256(json.dumps(job_key, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest(),
        "queued_at_utc": now.isoformat().replace("+00:00", "Z"),
        **dynamic_status,
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
    }
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {**payload, "job_already_existed": False, "job_path": str(out_path)}


def _projection_day_for_report(raw_day: Optional[str], family: str) -> str:
    if isinstance(raw_day, str) and _is_day_str(raw_day):
        return raw_day
    canonical_root = _canonical_truth_root()
    candidate_roots = [(canonical_root / "reports" / family).resolve()]
    if family == "aegis_canonical_operator_state_v1":
        alt_truth = Path("/home/node/constellation_2_runtime/constellation_2/runtime/truth").resolve()
        if not str(canonical_root).startswith("/tmp/"):
            candidate_roots.extend(
                [
                    (alt_truth / "reports" / "operator_state_snapshot_v1").resolve(),
                    (alt_truth / "reports" / "portfolio_gate_candidate_report_v1").resolve(),
                ]
            )
    elif family == "operator_state_snapshot_v1":
        alt_truth = Path("/home/node/constellation_2_runtime/constellation_2/runtime/truth").resolve()
        if not str(canonical_root).startswith("/tmp/"):
            candidate_roots.append((alt_truth / "reports" / family).resolve())
    days = sorted({day for root in candidate_roots for day in _list_day_dirs(root)})
    if days:
        return days[-1]
    return _projection_day(raw_day)


def _canonical_report_path(family: str, day_utc: str, filename: str) -> Path:
    return (_canonical_truth_root() / "reports" / family / day_utc / filename).resolve()


def _read_json_dict_or_empty(path: Path) -> Dict[str, Any]:
    payload, _err = _safe_read_json(path)
    return payload if isinstance(payload, dict) else {}


def _artifact_projection_payload(
    *,
    day_utc: str,
    family: str,
    filename: str,
    label: str,
) -> Dict[str, Any]:
    path = _canonical_report_path(family, day_utc, filename)
    payload, err = _safe_read_json(path)
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "status": "FAIL" if err == "FILE_NOT_FOUND" else "DEGRADED",
            "reason_codes": [f"{label.upper()}_MISSING" if err == "FILE_NOT_FOUND" else f"{label.upper()}_UNREADABLE"],
            "day_utc": day_utc,
            "truth_root": str(_canonical_truth_root()),
            "artifact_path": str(path),
            "data": {},
        }
    return {
        "ok": True,
        "status": str(payload.get("status") or payload.get("final_status") or "PASS").strip().upper() or "PASS",
        "reason_codes": [],
        "day_utc": str(payload.get("day_utc") or day_utc),
        "truth_root": str(_canonical_truth_root()),
        "artifact_path": str(path),
        "data": payload,
    }


def _operator_cockpit_payload(truth_root: Path, day_utc: str) -> Dict[str, Any]:
    canonical_path, canonical = latest_json_v1(
        truth_root,
        "aegis_canonical_operator_state_v1",
        day_utc,
        "canonical_operator_state.v1.json",
    )
    brief_path, brief = latest_json_v1(
        truth_root,
        "aegis_operator_brief_v1",
        day_utc,
        "operator_brief.v1.json",
    )
    runtime_truth_path, runtime_truth = latest_json_v1(
        truth_root,
        "aegis_runtime_truth_kernel_v1",
        day_utc,
        "runtime_truth_kernel.v1.json",
    )
    canonical_available = bool(canonical_path and canonical)
    brief_available = bool(brief_path and brief)
    runtime_truth_available = bool(runtime_truth_path and runtime_truth)
    runtime_evaluation_path = (truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_evaluation.v1.json").resolve()
    runtime_evaluation, runtime_evaluation_error = _safe_read_json(runtime_evaluation_path)
    runtime_evaluation = runtime_evaluation if isinstance(runtime_evaluation, dict) else {}
    runtime_evaluation_hash = str(runtime_evaluation.get("deterministic_output_hash") or "")
    control_packet_freshness = packet_currentness_v1(
        runtime_root=truth_root,
        runtime_mode="",
        day_utc=day_utc,
        runtime_evaluation_hash=runtime_evaluation_hash,
        runtime_evaluation_path=str(runtime_evaluation_path),
    )
    runtime_authority = {
        "authority": "RuntimeEvaluation",
        "runtime_evaluation_hash": runtime_evaluation_hash,
        "runtime_evaluation_path": str(runtime_evaluation_path),
        "runtime_evaluation_error": runtime_evaluation_error or "",
        "trade_advice_allowed": bool(((runtime_evaluation.get("capabilities") if isinstance(runtime_evaluation.get("capabilities"), dict) else {}).get("TRADE_ADVICE_ALLOWED") or {}).get("allowed", False)),
        "manual_trade_capture_allowed": bool(((runtime_evaluation.get("capabilities") if isinstance(runtime_evaluation.get("capabilities"), dict) else {}).get("MANUAL_TRADE_CAPTURE_ALLOWED") or {}).get("allowed", False)),
        "control_packet_readiness_usage": "EXPLANATORY_ONLY",
    }
    canonical_freshness = canonical.get("freshness") if isinstance(canonical.get("freshness"), dict) else {}
    canonical_runtime_freshness = canonical_freshness.get("runtime_truth") if isinstance(canonical_freshness.get("runtime_truth"), dict) else {}
    source_status = {
        "canonical_state": "available" if canonical_available else "missing",
        "operator_brief": "available" if brief_available else "missing",
        "runtime_truth": "available" if (canonical_runtime_freshness.get("found") is True or runtime_truth_available) else "unavailable",
        "canonical_generated_at": canonical.get("generated_at_utc") if isinstance(canonical, dict) else None,
        "operator_brief_generated_at": brief.get("generated_at_utc") if isinstance(brief, dict) else None,
        "runtime_truth_generated_at": canonical_runtime_freshness.get("generated_at") or ((runtime_truth.get("generated_at_utc") or runtime_truth.get("generated_at")) if isinstance(runtime_truth, dict) else None),
    }
    if not canonical_available:
        return {
            "ok": True,
            "status": "MISSING",
            "message": "Today's operator state has not been generated yet.",
            "day_utc": day_utc,
            "generated_at_utc": None,
            "read_only": True,
            "source_read_model": "canonical_operator_state.v1.json",
            "source_status": source_status,
            "source_paths": {
                "canonical_operator_state": str(canonical_path or ""),
                "operator_brief": str(brief_path or ""),
                "runtime_truth": str(runtime_truth_path or ""),
                "runtime_evaluation": str(runtime_evaluation_path),
                "control_packet": str(control_packet_freshness.get("path") or ""),
            },
            "runtime_evaluation_authority": runtime_authority,
            "control_packet_freshness": control_packet_freshness,
            "canonical_operator_state": {},
            "current_operator_truth": resolve_current_operator_truth_v1(truth_root=truth_root, day_utc=day_utc),
            "operator_brief": brief if isinstance(brief, dict) else {},
            "runtime": {"runtime_truth_classification": "UNKNOWN", "highest_readiness_layer": "UNKNOWN"},
            "actions_required": [
                {
                    "action_id": "refresh_canonical_operator_state",
                    "type": "MISSING_CANONICAL_STATE",
                    "priority": "HIGH",
                    "title": "Generate today's operator state",
                    "reason": "The cockpit reads canonical_operator_state.v1.json. Generate the operator state to populate today's candidates, actions, and workflow.",
                    "suggested_command": "npm run aegis:canonical-operator-state && npm run aegis:operator-brief",
                    "broker_execution_allowed": False,
                    "autonomous_execution_allowed": False,
                }
            ],
            "top_candidates": [],
            "opportunities": {},
            "candidate_decisions_corrections": {},
            "sleeve_warnings": {},
            "research_priorities": {},
            "governance_approvals": {},
            "regime": {},
            "event_triggers": [],
            "no_action_now": [],
            "drilldown_links": [],
            "safety": {
                "broker_execution_allowed": False,
                "broker_submit_transmit_allowed": False,
                "autonomous_execution_allowed": False,
                "read_only": True,
            },
        }

    candidates = canonical.get("candidates") if isinstance(canonical.get("candidates"), dict) else {}
    sleeves = canonical.get("sleeves") if isinstance(canonical.get("sleeves"), dict) else {}
    research = canonical.get("research") if isinstance(canonical.get("research"), dict) else {}
    governance = canonical.get("governance") if isinstance(canonical.get("governance"), dict) else {}
    payload_status = "AVAILABLE" if brief_available else "PARTIAL"
    current_operator_truth = resolve_current_operator_truth_v1(truth_root=truth_root, day_utc=day_utc)
    current_day_status = current_operator_truth.get("current_day_status") if isinstance(current_operator_truth.get("current_day_status"), dict) else {}
    historical_fallback = current_operator_truth.get("historical_fallback") if isinstance(current_operator_truth.get("historical_fallback"), dict) else {}
    missing_inputs = canonical.get("missing_inputs") if isinstance(canonical.get("missing_inputs"), list) else []
    warnings = canonical.get("warnings") if isinstance(canonical.get("warnings"), list) else []
    if not brief_available:
        warnings = [
            {
                "warning_id": "operator_brief_missing",
                "priority": "LOW",
                "status": "MISSING",
                "message": "Operator brief is missing; showing canonical state directly.",
                "suggested_command": "npm run aegis:operator-brief",
                "source_artifact": "",
            },
            *warnings,
        ]
    if any(bool(row.get("critical")) for row in missing_inputs if isinstance(row, dict)):
        payload_status = "PARTIAL"
    payload = {
        "ok": True,
        "status": payload_status,
        "day_utc": str(canonical.get("day_utc") or day_utc),
        "generated_at_utc": canonical.get("generated_at_utc"),
        "read_only": True,
        "source_read_model": "canonical_operator_state.v1.json",
        "brief_read_model": "operator_brief.v1.json",
        "source_status": source_status,
        "source_paths": {
            "canonical_operator_state": str(canonical_path or ""),
            "operator_brief": str(brief_path or ""),
            "runtime_truth": str(canonical_runtime_freshness.get("path") or runtime_truth_path or ""),
            "runtime_evaluation": str(runtime_evaluation_path),
            "control_packet": str(control_packet_freshness.get("path") or ""),
        },
        "runtime_evaluation_authority": runtime_authority,
        "control_packet_freshness": control_packet_freshness,
        "canonical_operator_state": canonical,
        "current_operator_truth": current_operator_truth,
        "current_day_status": current_day_status,
        "historical_fallback": historical_fallback,
        "displayed_artifact_day": str(current_operator_truth.get("displayed_artifact_day") or historical_fallback.get("source_day") or day_utc),
        "operator_brief": brief if isinstance(brief, dict) else {},
        "runtime": canonical.get("runtime") if isinstance(canonical.get("runtime"), dict) else {},
        "actions_required": canonical.get("actions_required") if isinstance(canonical.get("actions_required"), list) else [],
        "top_candidates": canonical.get("top_candidates") if isinstance(canonical.get("top_candidates"), list) else [],
        "opportunities": canonical.get("opportunities") if isinstance(canonical.get("opportunities"), dict) else {},
        "candidate_decisions_corrections": candidates,
        "sleeve_warnings": sleeves,
        "research_priorities": research,
        "governance_approvals": governance,
        "regime": canonical.get("regime") if isinstance(canonical.get("regime"), dict) else {},
        "event_triggers": canonical.get("event_triggers") if isinstance(canonical.get("event_triggers"), list) else [],
        "warnings": warnings,
        "no_action_now": canonical.get("no_action_now") if isinstance(canonical.get("no_action_now"), list) else [],
        "drilldown_links": canonical.get("drilldown_index") if isinstance(canonical.get("drilldown_index"), list) else [],
        "missing_inputs": missing_inputs,
        "conflicts": canonical.get("conflicts") if isinstance(canonical.get("conflicts"), list) else [],
        "freshness": canonical.get("freshness") if isinstance(canonical.get("freshness"), dict) else {},
        "safety": {
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
            "read_only": True,
        },
    }
    operator_state_response = load_or_build_operator_state_snapshot_response_v1(
        truth_root=truth_root,
        day_utc=str(canonical.get("day_utc") or day_utc),
    )
    operator_snapshot = operator_state_response.get("data") if isinstance(operator_state_response.get("data"), dict) else {}
    if operator_snapshot:
        payload["operator_state_snapshot"] = operator_snapshot
        payload["manual_capture_candidate"] = operator_snapshot.get("manual_capture_candidate") if isinstance(operator_snapshot.get("manual_capture_candidate"), dict) else {}
        payload["suppressed_candidate_watchlist"] = operator_snapshot.get("suppressed_candidate_watchlist") if isinstance(operator_snapshot.get("suppressed_candidate_watchlist"), dict) else {}
        payload["latest_operator_run_summary"] = operator_snapshot.get("latest_run_summary") if isinstance(operator_snapshot.get("latest_run_summary"), dict) else {}
        if isinstance(payload.get("source_paths"), dict):
            payload["source_paths"]["operator_state_snapshot"] = str(operator_snapshot.get("artifact_path") or "")
    decision_support = build_candidate_decision_support_payload_v1(payload)
    payload["candidate_decision_support"] = decision_support
    support_by_id = decision_support.get("by_candidate_id") if isinstance(decision_support.get("by_candidate_id"), dict) else {}
    enriched_candidates = []
    for row in payload.get("top_candidates", []):
        if not isinstance(row, dict):
            enriched_candidates.append(row)
            continue
        candidate_id = str(row.get("candidate_id") or row.get("id") or "").strip()
        enriched = dict(row)
        if candidate_id in support_by_id:
            enriched["decision_support_brief"] = support_by_id[candidate_id]
        enriched_candidates.append(enriched)
    payload["top_candidates"] = enriched_candidates
    payload.update(build_operator_projections_v1(payload))
    # Dashboard day/mode/count fields are governed by operator_state_snapshot_v1.
    # The generic cockpit projection builder is legacy/final-EOD oriented and must
    # not overwrite successful INTRADAY_OPERATIONAL current-day state.
    if operator_snapshot:
        for projection_key in (
            "operator_today_projection",
            "active_opportunity_projection",
            "operator_task_projection",
            "system_diagnostic_projection",
            "what_changed_projection",
            "passive_health_projection",
            "review_ledger_projection",
        ):
            projected = operator_snapshot.get(projection_key)
            if isinstance(projected, dict):
                payload[projection_key] = projected
        payload["operator_state_snapshot"] = operator_snapshot
        payload["current_operator_truth"] = operator_snapshot.get("current_operator_truth") if isinstance(operator_snapshot.get("current_operator_truth"), dict) else current_operator_truth
        payload["current_day_status"] = operator_snapshot.get("current_day_status") if isinstance(operator_snapshot.get("current_day_status"), dict) else current_day_status
        payload["historical_fallback"] = operator_snapshot.get("historical_fallback") if isinstance(operator_snapshot.get("historical_fallback"), dict) else historical_fallback
        payload["displayed_artifact_day"] = str(operator_snapshot.get("displayed_artifact_day") or current_operator_truth.get("displayed_artifact_day") or day_utc)
        payload["runtime_mode"] = str(operator_snapshot.get("runtime_mode") or current_operator_truth.get("runtime_mode") or "UNKNOWN")
        for semantic_key in ("market_data_state", "candidate_certification_state", "execution_eligibility_state", "operator_state_semantics"):
            if semantic_key in operator_snapshot:
                payload[semantic_key] = operator_snapshot.get(semantic_key)
        for snapshot_key in (
            "current_day_candidate_rows",
            "current_day_candidates",
            "historical_captures",
            "latest_captured_trade_projection",
            "runtime_timeline_projection",
            "domain_certification",
        ):
            if snapshot_key in operator_snapshot:
                payload[snapshot_key] = operator_snapshot.get(snapshot_key)
    payload["eod_opportunity_outcome_report"] = build_eod_opportunity_outcome_report_v1(
        payload,
        trading_session=str(canonical.get("day_utc") or day_utc),
        generated_at=str(canonical.get("generated_at_utc") or ""),
    )
    snapshot = payload.get("operator_state_snapshot") if isinstance(payload.get("operator_state_snapshot"), dict) else {}
    if snapshot:
        snapshot_id = str(snapshot.get("snapshot_id") or "")
        source_fingerprint = str(snapshot.get("source_fingerprint") or "")
        eod_projection = {**payload["eod_opportunity_outcome_report"], "snapshot_id": snapshot_id, "source_fingerprint": source_fingerprint}
        snapshot["eod_outcome_projection"] = eod_projection
        payload["operator_state_snapshot"] = snapshot
        payload["eod_opportunity_outcome_report"] = eod_projection
        payload["snapshot_id"] = snapshot_id
        payload["source_fingerprint"] = source_fingerprint
    return payload


def _aegis_status(code: str, *, label: str | None = None, semantic: str | None = None, reason_codes: list[str] | None = None) -> Dict[str, Any]:
    normalized = str(code or "unknown").strip().lower()
    if semantic is None:
        semantic = "healthy" if normalized in {"ready", "current"} else ("warning" if normalized in {"waiting", "partial", "disabled"} else "blocked")
    return {
        "code": normalized,
        "label": label or normalized.replace("_", " ").title(),
        "semantic": semantic,
        "reason_codes": reason_codes or [],
    }


def _aegis_kernel_status_rail_view(truth_root: Path, requested_day: str | None) -> Dict[str, Any]:
    day = _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1")
    payload = _operator_cockpit_payload(truth_root, day)
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    source_status = payload.get("source_status") if isinstance(payload.get("source_status"), dict) else {}
    safety = payload.get("safety") if isinstance(payload.get("safety"), dict) else {}
    candidates = payload.get("candidate_decisions_corrections") if isinstance(payload.get("candidate_decisions_corrections"), dict) else {}
    top_candidates = payload.get("top_candidates") if isinstance(payload.get("top_candidates"), list) else []
    canonical_available = source_status.get("canonical_state") == "available"
    runtime_available = source_status.get("runtime_truth") == "available"
    brief_available = source_status.get("operator_brief") == "available"
    safety_ok = (
        safety.get("broker_execution_allowed") is False
        and safety.get("broker_submit_transmit_allowed") is False
        and safety.get("autonomous_execution_allowed") is False
    )
    runtime_class = str(runtime.get("runtime_truth_classification") or "UNKNOWN")
    current_day_rows = payload.get("current_day_candidate_rows") if isinstance(payload.get("current_day_candidate_rows"), list) else []
    today_projection = payload.get("operator_today_projection") if isinstance(payload.get("operator_today_projection"), dict) else {}
    current_candidate_count = int(today_projection.get("current_intraday_candidate_count") or today_projection.get("current_day_candidate_count") or len(current_day_rows) or 0)
    no_candidate_rows = current_candidate_count <= 0 and not top_candidates and not any(candidates.get(key) for key in ("awaiting_decision", "approved_or_traded", "deferred", "awaiting_outcome"))

    control = _aegis_status("ready", label="Ready", semantic="healthy", reason_codes=[])
    if not runtime_available or not canonical_available or not safety_ok:
        control = _aegis_status("blocked", label="Blocked", semantic="blocked", reason_codes=["RUNTIME_OR_CANONICAL_MISSING" if not runtime_available or not canonical_available else "SAFETY_INCONSISTENCY"])

    state = _aegis_status("current", label="Current", semantic="healthy", reason_codes=[])
    if not canonical_available:
        state = _aegis_status("missing", label="Missing", semantic="blocked", reason_codes=["CANONICAL_OPERATOR_STATE_MISSING"])
    elif not brief_available:
        state = _aegis_status("partial", label="Current", semantic="warning", reason_codes=["OPERATOR_BRIEF_MISSING"])

    critical_blockers = runtime.get("critical_blockers")
    if not isinstance(critical_blockers, list):
        critical_blockers = []
    candidate_blocked = runtime_class.upper() == "BLOCKED" or any(
        str(item).upper().startswith("RUNTIME_TRUTH") for item in critical_blockers if isinstance(item, str)
    )
    if not runtime_available:
        advisory = _aegis_status("blocked", label="Blocked", semantic="blocked", reason_codes=["RUNTIME_TRUTH_MISSING"])
    elif no_candidate_rows:
        advisory = _aegis_status("none", label="None", semantic="healthy", reason_codes=["NO_ADVISORY_CANDIDATES"])
    elif candidate_blocked:
        advisory = _aegis_status("blocked", label="Blocked", semantic="blocked", reason_codes=[runtime_class])
    elif runtime.get("trade_advice_allowed") is True:
        advisory = _aegis_status("ready", label="Ready", semantic="healthy", reason_codes=[])
    else:
        advisory = _aegis_status(
            "partial",
            label="Partial",
            semantic="warning",
            reason_codes=["CANDIDATE_EXISTS_EVIDENCE_PROJECTION_INCOMPLETE"],
        )

    submission = _aegis_status("disabled", label="Disabled By Design", semantic="healthy", reason_codes=["BROKER_SUBMIT_TRANSMIT_DISABLED_BY_DESIGN"])
    lifecycle = _aegis_status("ready", label="Candidates Present" if not no_candidate_rows else "No Candidates Yet", semantic="healthy", reason_codes=[] if not no_candidate_rows else ["NO_CANDIDATES_YET"])

    labels = [
        ("state", "State", state, "/aegis-journal"),
        ("advisory", "Advisory", advisory, "/aegis-opportunities"),
        ("submission", "Submission", submission, "/aegis-journal"),
        ("lifecycle", "Lifecycle", lifecycle, "/aegis-journal"),
    ]
    return {
        "ok": True,
        "query_contract_version": "aegis_status_rail.v1",
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "source": "aegis_canonical_operator_state",
        "kernels": [
            {"kernel_id": kernel_id, "label": label, "status": status, "href": href}
            for kernel_id, label, status, href in labels
        ],
    }


def _selected_intent_projection() -> Tuple[str, str, str]:
    pointer_path = (_canonical_truth_root() / "pointers" / "selected_intent_pointer.v1.json").resolve()
    pointer = _read_json_dict_or_empty(pointer_path)
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    return (
        str(pointer.get("selected_intent_id") or selected.get("intent_id") or "").strip(),
        str(pointer.get("status") or "").strip().upper(),
        str(pointer_path),
    )


def _runtime_status_projection(day_utc: Optional[str] = None) -> Dict[str, Any]:
    day = _projection_day(day_utc)
    readiness = build_readiness_kernel_v1(day)
    truth_root = Path(str(readiness.get("truth_root") or _canonical_truth_root())).resolve()
    runtime_truth = _runtime_truth_root()
    selected_intent_id, selected_intent_status, selected_pointer_path = _selected_intent_projection()
    portfolio_state = _artifact_projection_payload(
        day_utc=day,
        family="portfolio_state_v1",
        filename="portfolio_state.v1.json",
        label="portfolio_state",
    )
    portfolio_scoring = _artifact_projection_payload(
        day_utc=day,
        family="portfolio_scoring_v1",
        filename="portfolio_scoring.v1.json",
        label="portfolio_scoring",
    )
    decision_ledger = _artifact_projection_payload(
        day_utc=day,
        family="decision_ledger_v1",
        filename="decision_ledger.v1.json",
        label="decision_ledger",
    )
    missing = []
    if readiness.get("primary_ui_authority") != "aegis_control_plane_v1" or readiness.get("canonical_blocker") == "CONTROL_PLANE_MISSING":
        missing.append("AEGIS_CONTROL_PLANE_MISSING")
    if not portfolio_state.get("ok"):
        missing.extend(portfolio_state.get("reason_codes") or [])
    if not portfolio_scoring.get("ok"):
        missing.extend(portfolio_scoring.get("reason_codes") or [])
    packet_path = _latest_packet_path()

    status = "PASS"
    if "AEGIS_CONTROL_PLANE_MISSING" in missing:
        status = "FAIL"
    elif missing:
        status = "DEGRADED"

    return {
        "ok": bool(readiness.get("canonical_blocker") != "CONTROL_PLANE_MISSING"),
        "status": status,
        "reason_codes": missing,
        "projection_contract_version": PROJECTION_CONTRACT_VERSION,
        "generated_at_utc": _utc_now_iso(),
        "day_utc": day,
        "truth_root": str(truth_root),
        "runtime_truth_root": str(runtime_truth),
        "final_status": str(readiness.get("overall_status") or "UNKNOWN"),
        "canonical_phase": str(readiness.get("current_phase") or ""),
        "canonical_blocker": str(readiness.get("canonical_blocker") or ""),
        "source_integrity_status": "SUPPORTING_EVIDENCE_ONLY",
        "runtime_mode": str(readiness.get("runtime_mode") or ""),
        "production_version_status": str(readiness.get("production_version_status") or ""),
        "promoted_commit": str(readiness.get("promoted_commit") or ""),
        "submit_status": str(readiness.get("submit_status") or ""),
        "submit_canonical_blocker": str(readiness.get("submit_canonical_blocker") or ""),
        "selected_intent_id": selected_intent_id,
        "selected_intent_status": selected_intent_status,
        "portfolio_state_status": str(portfolio_state.get("status") or "UNKNOWN"),
        "portfolio_scoring_status": str(portfolio_scoring.get("status") or "UNKNOWN"),
        "decision_ledger_status": str(decision_ledger.get("status") or "UNKNOWN"),
        "operator_next_action": str(readiness.get("operator_next_action") or "").strip(),
        "artifact_paths": {
            "control_plane": str(readiness.get("primary_authority_path") or ""),
            "selected_intent_pointer": selected_pointer_path,
            "portfolio_state": str(portfolio_state.get("artifact_path") or ""),
            "portfolio_scoring": str(portfolio_scoring.get("artifact_path") or ""),
            "decision_ledger": str(decision_ledger.get("artifact_path") or ""),
            "latest_packet": str(packet_path),
        },
    }


def _merge_latest_domain_command_results_v1(payload: Dict[str, Any], day_utc: str) -> Dict[str, Any]:
    try:
        report = build_domain_certification_report_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day_utc)
    except Exception:
        return payload
    repair_actions = report.get("domain_repair_actions") if isinstance(report.get("domain_repair_actions"), list) else []
    if not repair_actions:
        return payload
    merged = dict(payload)
    timeline = dict(merged.get("runtime_timeline_projection") or {}) if isinstance(merged.get("runtime_timeline_projection"), dict) else {}
    if timeline:
        timeline["domain_repair_actions"] = repair_actions
        certification = dict(timeline.get("domain_certification") or {}) if isinstance(timeline.get("domain_certification"), dict) else {}
        certification["domain_repair_actions"] = repair_actions
        timeline["domain_certification"] = certification
        merged["runtime_timeline_projection"] = timeline
    certification_payload = dict(merged.get("domain_certification") or {}) if isinstance(merged.get("domain_certification"), dict) else {}
    if certification_payload:
        certification_payload["domain_repair_actions"] = repair_actions
        merged["domain_certification"] = certification_payload
    return merged


def _operator_projection_payload(day_utc: Optional[str] = None) -> Dict[str, Any]:
    day = _projection_day(day_utc)
    path = _canonical_report_path("aegis_operator_projection_v1", day, "operator_projection.v1.json")
    payload = _read_json_dict_or_empty(path)
    if not payload:
        return {
            "ok": False,
            "status": "MISSING",
            "day_utc": day,
            "artifact_path": str(path),
            "operator_next_action": "Run ops/tools/run_aegis_operator_projection_v1.py for the current day.",
            "authority_note": "Missing projection does not alter final readiness; aegis_day_run_ledger_v1 remains authoritative.",
        }
    payload = _merge_latest_domain_command_results_v1(payload, day)
    return {"ok": True, "artifact_path": str(path), **payload}


def _latest_packet_projection() -> Dict[str, Any]:
    path = _latest_packet_path()
    if not path.exists() or not path.is_file():
        return {
            "ok": False,
            "status": "FAIL",
            "reason_codes": ["LATEST_PACKET_MISSING"],
            "artifact_path": str(path),
            "content": "",
        }
    content = path.read_text(encoding="utf-8", errors="replace")
    return {
        "ok": True,
        "status": "PASS",
        "reason_codes": [],
        "artifact_path": str(path),
        "updated_at_utc": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "content": content,
    }


def _ui_service_authority_path(day_utc: str) -> Path:
    return _canonical_report_path("ui_service_authority_v1", day_utc, "ui_service_authority.v1.json")


def _list_day_dirs(root: Path) -> List[str]:
    if not root.exists() or not root.is_dir():
        return []
    days: List[str] = []
    for p in root.iterdir():
        if p.is_dir() and _is_day_str(p.name):
            days.append(p.name)
    days.sort()
    return days


def _union_days() -> List[str]:
    """
    Authoritative day discovery for UI.

    Includes:
    - gate_stack_verdict_v1 days (authoritative day spine)
    - intents snapshot days
    - accounting_v2 nav/attribution days
    - submissions days (if submissions root exists)
    - pillars days
    - activity monitoring days (if present)
    """
    days = set()

    roots = _known_truth_roots()
    if not roots:
        roots = [TRUTH_ROOT]
    for troot in roots:
        submissions_root = (troot / "execution_evidence_v1" / "submissions").resolve()
        intents_root = (troot / "intents_v1" / "snapshots").resolve()
        gate_root = (troot / "reports" / "gate_stack_verdict_v1").resolve()
        nav_root = (troot / "accounting_v2" / "nav").resolve()
        attr_root = (troot / "accounting_v2" / "attribution").resolve()
        pillars_v1 = (troot / "pillars_v1").resolve()
        pillars_v1r1 = (troot / "pillars_v1r1").resolve()
        intents_summary_root = (troot / "monitoring_v1" / "intents_summary_v1").resolve()
        submissions_summary_root = (troot / "monitoring_v1" / "submissions_summary_v1").resolve()
        activity_rollup_root = (troot / "monitoring_v1" / "activity_ledger_rollup_v1").resolve()

        for root in [gate_root, intents_root, nav_root, attr_root]:
            for d in _list_day_dirs(root):
                days.add(d)

        if submissions_root.exists() and submissions_root.is_dir():
            for d in _list_day_dirs(submissions_root):
                days.add(d)

        for root in [pillars_v1r1, pillars_v1]:
            for d in _list_day_dirs(root):
                days.add(d)

        for root in [intents_summary_root, submissions_summary_root, activity_rollup_root]:
            for d in _list_day_dirs(root):
                days.add(d)

    # UI safety: exclude future days (e.g. 2199-01-19 bootstrap placeholders).
    # Selectable days must not exceed today's UTC date.
    today_utc = date.today().isoformat()
    days2 = [d for d in days if isinstance(d, str) and d <= today_utc]

    return sorted(days2)



def _select_latest_day(days: List[str]) -> Optional[str]:
    return days[-1] if days else None


def _pillars_decisions_dir(day: str) -> Optional[Path]:
    d1 = (PILLARS_V1R1_ROOT / day / "decisions").resolve()
    if d1.exists() and d1.is_dir():
        return d1
    d0 = (PILLARS_V1_ROOT / day / "decisions").resolve()
    if d0.exists() and d0.is_dir():
        return d0
    return None


def _try_load_submission_index(day: str) -> Tuple[Optional[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    idx_path = (SUBMISSIONS_ROOT / day / SUBMISSION_INDEX_FILENAME).resolve()
    if not idx_path.exists():
        missing.append(str(idx_path))
        return None, missing, source_paths, source_mtimes, warnings

    obj, err = _safe_read_json(idx_path)
    source_paths.append(str(idx_path))
    mt = _mtime(idx_path)
    if mt is not None:
        source_mtimes[str(idx_path)] = mt

    if obj is None or not isinstance(obj, dict):
        warnings.append(f"SUBMISSION_INDEX_UNREADABLE:{err}")
        return None, missing, source_paths, source_mtimes, warnings

    if obj.get("schema_id") != SUBMISSION_INDEX_SCHEMA_ID or obj.get("schema_version") != SUBMISSION_INDEX_SCHEMA_VERSION:
        warnings.append("SUBMISSION_INDEX_SCHEMA_MISMATCH")
        return None, missing, source_paths, source_mtimes, warnings

    if obj.get("day_utc") != day:
        warnings.append("SUBMISSION_INDEX_DAY_MISMATCH")
        return None, missing, source_paths, source_mtimes, warnings

    if not isinstance(obj.get("items"), list):
        warnings.append("SUBMISSION_INDEX_ITEMS_MISSING_OR_INVALID")
        return None, missing, source_paths, source_mtimes, warnings

    return obj, missing, source_paths, source_mtimes, warnings


def _try_load_pillars_decisions(day: str) -> Tuple[List[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    ddir = _pillars_decisions_dir(day)
    if ddir is None:
        return [], missing, source_paths, source_mtimes, warnings

    files = sorted([p for p in ddir.iterdir() if p.is_file() and p.name.endswith(PILLARS_DECISION_SUFFIX)], key=lambda p: p.name)
    if not files:
        missing.append(str(ddir))
        warnings.append("PILLARS_DECISIONS_EMPTY")
        return [], missing, source_paths, source_mtimes, warnings

    out: List[Dict[str, Any]] = []
    for fp in files:
        obj, err = _safe_read_json(fp)
        source_paths.append(str(fp))
        mt = _mtime(fp)
        if mt is not None:
            source_mtimes[str(fp)] = mt

        if obj is None or not isinstance(obj, dict):
            warnings.append(f"PILLARS_DECISION_UNREADABLE:{fp}:{err}")
            continue

        if str(obj.get("schema_id") or "") != PILLARS_DECISION_SCHEMA_ID or str(obj.get("schema_version") or "") != PILLARS_DECISION_SCHEMA_VERSION:
            warnings.append(f"PILLARS_DECISION_SCHEMA_MISMATCH:{fp}")
            continue

        decision_id = str(obj.get("decision_id") or "").strip()
        if decision_id == "":
            warnings.append(f"PILLARS_DECISION_MISSING_DECISION_ID:{fp}")
            continue

        input_manifest = obj.get("input_manifest")
        if not isinstance(input_manifest, list):
            warnings.append(f"PILLARS_DECISION_INPUT_MANIFEST_INVALID:{fp}")
            input_manifest = []

        broker_path: Optional[str] = None
        exec_path: Optional[str] = None
        plan_path: Optional[str] = None

        for it in input_manifest:
            if not isinstance(it, dict):
                continue
            t = str(it.get("type") or "")
            p = str(it.get("path") or "")
            if t == "broker_submission_record_v2" and p:
                broker_path = p
            elif t == "execution_event_record_v1" and p:
                exec_path = p
            elif t == "order_plan_v1" and p:
                plan_path = p

        rec: Dict[str, Any] = {
            "submission_dir": None,
            "submission_id": decision_id,
            "decision": obj.get("decision"),
            "decision_status": obj.get("status"),
            "decision_reason_codes": obj.get("reason_codes"),
            "broker_submission_record": None,
            "execution_event_record": None,
            "order_plan": None,
            "missing_paths": [],
        }

        if isinstance(broker_path, str) and broker_path:
            try:
                rec["submission_dir"] = str(Path(broker_path).resolve().parent)
            except Exception:
                rec["submission_dir"] = None

        if isinstance(broker_path, str) and broker_path:
            bobj, berr = _safe_read_json(Path(broker_path))
            if bobj is None:
                rec["missing_paths"].append(broker_path)
                warnings.append(f"PILLARS_BROKER_RECORD_UNREADABLE:{berr}")
            else:
                rec["broker_submission_record"] = bobj
                source_paths.append(broker_path)
                mtb = _mtime(Path(broker_path))
                if mtb is not None:
                    source_mtimes[broker_path] = mtb

        if isinstance(exec_path, str) and exec_path:
            eobj, _eerr = _safe_read_json(Path(exec_path))
            if eobj is None:
                rec["missing_paths"].append(exec_path)
            else:
                rec["execution_event_record"] = eobj
                source_paths.append(exec_path)
                mte = _mtime(Path(exec_path))
                if mte is not None:
                    source_mtimes[exec_path] = mte

        if isinstance(plan_path, str) and plan_path:
            pobj, _perr = _safe_read_json(Path(plan_path))
            if pobj is None:
                rec["missing_paths"].append(plan_path)
            else:
                rec["order_plan"] = pobj
                source_paths.append(plan_path)
                mtp = _mtime(Path(plan_path))
                if mtp is not None:
                    source_mtimes[plan_path] = mtp

        out.append(rec)

    return out, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def _scan_submissions_for_day(day: str) -> Tuple[List[Dict[str, Any]], List[str], List[str], Dict[str, float]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}

    day_root = SUBMISSIONS_ROOT / day
    if not day_root.exists():
        missing.append(str(day_root))
        return [], missing, source_paths, source_mtimes

    idx, miss_i, sp_i, sm_i, _w_i = _try_load_submission_index(day)
    if idx is not None:
        missing.extend(miss_i)
        source_paths.extend(sp_i)
        source_mtimes.update(sm_i)

        out: List[Dict[str, Any]] = []
        for it in idx.get("items", []):
            if not isinstance(it, dict):
                continue

            paths = it.get("paths") if isinstance(it.get("paths"), dict) else {}
            subdir = paths.get("submission_dir") if isinstance(paths, dict) else None

            rec: Dict[str, Any] = {
                "submission_dir": subdir,
                "submission_id": it.get("submission_id"),
                "broker_submission_record": {
                    "schema_id": "broker_submission_record",
                    "schema_version": "v2",
                    "submission_id": it.get("submission_id"),
                    "binding_hash": it.get("binding_hash"),
                    "broker": it.get("broker"),
                    "broker_ids": it.get("broker_ids"),
                    "status": it.get("broker_status"),
                    "submitted_at_utc": it.get("submitted_at_utc"),
                },
                "execution_event_record": None,
                "order_plan": None,
                "missing_paths": [],
            }

            ex = it.get("execution") if isinstance(it.get("execution"), dict) else None
            if isinstance(ex, dict) and ex.get("status") is not None:
                rec["execution_event_record"] = {
                    "schema_id": "execution_event_record",
                    "schema_version": "v1",
                    "status": ex.get("status"),
                    "filled_qty": ex.get("filled_qty"),
                    "avg_price": ex.get("avg_price"),
                    "event_time_utc": ex.get("event_time_utc"),
                    "perm_id": ex.get("perm_id"),
                    "broker_order_id": ex.get("broker_order_id"),
                }

            op_path = paths.get("order_plan") if isinstance(paths, dict) else None
            if isinstance(op_path, str) and op_path:
                op_obj, _op_err = _safe_read_json(Path(op_path))
                if op_obj is None:
                    rec["missing_paths"].append(op_path)
                else:
                    rec["order_plan"] = op_obj
                    source_paths.append(op_path)
                    mt2 = _mtime(Path(op_path))
                    if mt2 is not None:
                        source_mtimes[op_path] = mt2

            out.append(rec)

        return out, sorted(set(missing)), sorted(set(source_paths)), source_mtimes

    pill_records, miss_p, sp_p, sm_p, _w_p = _try_load_pillars_decisions(day)
    if pill_records:
        missing.extend(miss_p)
        source_paths.extend(sp_p)
        source_mtimes.update(sm_p)
        return pill_records, sorted(set(missing)), sorted(set(source_paths)), source_mtimes

    missing.extend(miss_i)
    source_paths.extend(sp_i)
    source_mtimes.update(sm_i)
    return [], sorted(set(missing)), sorted(set(source_paths)), source_mtimes


def _load_engine_join_map_for_day(day: str) -> Tuple[Dict[str, str], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []
    subid_to_engine: Dict[str, str] = {}

    day_snap_dir = ENGINE_LINKAGE_ROOT / "snapshots" / day
    candidates: List[Path] = []
    if day_snap_dir.exists() and day_snap_dir.is_dir():
        candidates = sorted([p for p in day_snap_dir.iterdir() if p.is_file() and p.suffix == ".json"])

    to_try: List[Path] = []
    if candidates:
        to_try.extend(candidates)

    for p in to_try:
        obj, _ = _safe_read_json(p)
        if obj is None:
            continue
        source_paths.append(str(p))
        mt = _mtime(p)
        if mt is not None:
            source_mtimes[str(p)] = mt

        if isinstance(obj, dict):
            for key in ["subid_to_engine", "submission_id_to_engine", "engine_by_submission_id", "engine_by_subid"]:
                v = obj.get(key)
                if isinstance(v, dict) and v:
                    ok = True
                    tmp: Dict[str, str] = {}
                    for k2, v2 in v.items():
                        if not isinstance(k2, str) or not isinstance(v2, str):
                            ok = False
                            break
                        tmp[k2] = v2
                    if ok and tmp:
                        subid_to_engine.update(tmp)
                        return subid_to_engine, missing, source_paths, source_mtimes, warnings

    attr_path = ACCOUNTING_ATTR_ROOT / day / "engine_attribution.v2.json"
    obj, _ = _safe_read_json(attr_path)
    if obj is None:
        missing.append(str(attr_path))
    else:
        source_paths.append(str(attr_path))
        mt = _mtime(attr_path)
        if mt is not None:
            source_mtimes[str(attr_path)] = mt

    warnings.append(E_ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE)
    return {}, missing, source_paths, source_mtimes, warnings


def _count_intents_for_day(day: str) -> Tuple[int, List[str]]:
    d = (INTENTS_ROOT / day).resolve()
    if not d.exists() or not d.is_dir():
        return 0, [str(d)]
    files = sorted([p for p in d.iterdir() if p.is_file()])
    return len(files), []


def _nav_summary_for_day(day: str) -> Tuple[Optional[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    nav_path = ACCOUNTING_NAV_ROOT / day / "nav.v2.json"
    obj, _err = _safe_read_json(nav_path)
    if obj is None:
        missing.append(str(nav_path))
        warnings.append(E_NAV_MISSING)
        return None, missing, source_paths, source_mtimes, warnings

    source_paths.append(str(nav_path))
    mt = _mtime(nav_path)
    if mt is not None:
        source_mtimes[str(nav_path)] = mt

    nav_end = None
    if isinstance(obj, dict):
        nav = obj.get("nav")
        if isinstance(nav, dict) and "nav_total" in nav:
            nav_end = nav.get("nav_total")
        elif "nav_total" in obj:
            nav_end = obj.get("nav_total")

    return {"source": "accounting_v2/nav", "day_utc": day, "nav_end": nav_end}, missing, source_paths, source_mtimes, warnings


def _series_nav_points(last_n_days: int) -> Tuple[List[Dict[str, Any]], List[str], List[str], Dict[str, float], List[str]]:
    missing: List[str] = []
    source_paths: List[str] = []
    source_mtimes: Dict[str, float] = {}
    warnings: List[str] = []

    days = _union_days()
    if not days:
        warnings.append(E_NO_DAYS_FOUND)
        return [], missing, source_paths, source_mtimes, warnings

    sel = days[-last_n_days:] if last_n_days > 0 else days
    pts: List[Dict[str, Any]] = []
    for d in sel:
        nav, m, sp, sm, w = _nav_summary_for_day(d)
        missing.extend(m)
        source_paths.extend(sp)
        source_mtimes.update(sm)
        warnings.extend(w)
        if nav is None:
            continue
        pts.append({"day_utc": d, "nav_end": nav.get("nav_end"), "source": nav.get("source")})

    return pts, sorted(set(missing)), sorted(set(source_paths)), source_mtimes, sorted(set(warnings))


def _read_activity_artifact(path: Path, expected_schema_id: str, expected_day_field: str, expected_day_value: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """
    Returns (doc_or_none, errors[])
    """
    if not path.exists():
        return None, [E_ACTIVITY_ARTIFACT_MISSING]
    obj, err = _safe_read_json(path)
    if obj is None or not isinstance(obj, dict):
        return None, [f"{E_ACTIVITY_ARTIFACT_UNREADABLE}:{err}"]
    if str(obj.get("schema_id") or "") != expected_schema_id:
        return None, [f"ACTIVITY_SCHEMA_MISMATCH:{expected_schema_id}"]
    if str(obj.get(expected_day_field) or "") != expected_day_value:
        return None, [f"ACTIVITY_DAY_MISMATCH:{expected_day_value}"]
    return obj, []


def _activity_latest_day() -> Optional[str]:
    return _select_latest_day(_union_days())


def _activity_today(day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "errors": [],
        "warnings": [],
        "missing_paths": [],
        "source_paths": [],
        "intents_summary": None,
        "submissions_summary": None,
        "rollup_asof": None,
    }

    ip = (INTENTS_SUMMARY_ROOT / day / "intents_summary.v1.json").resolve()
    sp = (SUBMISSIONS_SUMMARY_ROOT / day / "submissions_summary.v1.json").resolve()
    rp = (ACTIVITY_ROLLUP_ROOT / day / "activity_ledger_rollup.v1.json").resolve()

    doc, errs = _read_activity_artifact(ip, "intents_summary", "day_utc", day)
    if doc is None:
        resp["warnings"].extend(errs)
        resp["missing_paths"].append(str(ip))
    else:
        resp["intents_summary"] = doc
        resp["source_paths"].append(str(ip))

    doc, errs = _read_activity_artifact(sp, "submissions_summary", "day_utc", day)
    if doc is None:
        resp["warnings"].extend(errs)
        resp["missing_paths"].append(str(sp))
    else:
        resp["submissions_summary"] = doc
        resp["source_paths"].append(str(sp))

    doc, errs = _read_activity_artifact(rp, "activity_ledger_rollup", "asof_day_utc", day)
    if doc is None:
        resp["warnings"].extend(errs)
        resp["missing_paths"].append(str(rp))
    else:
        resp["rollup_asof"] = doc
        resp["source_paths"].append(str(rp))

    resp["errors"] = sorted(set(resp["errors"]))
    resp["warnings"] = sorted(set(resp["warnings"]))
    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    return resp


def _day_summary(day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "errors": [],
        "warnings": [],
        "source_paths": [],
        "source_mtimes": {},
        "missing_paths": [],
        "data_freshness_max_mtime": None,
        "counts": {
            "intents": 0,
            "planned_actions": None,
            "submissions": 0,
            "fills": 0,
            "partials": 0,
            "rejects": 0,
            "errors": 0,
            "unknown_status": 0,
        },
        "by_engine": [],
        "nav": None,
    }

    if not _is_day_str(day):
        resp["ok"] = False
        resp["errors"].append(E_DAY_INVALID)
        return resp

    if not TRUTH_ROOT.exists():
        resp["ok"] = False
        resp["errors"].append(E_TRUTH_ROOT_MISSING)
        resp["missing_paths"].append(str(TRUTH_ROOT))
        return resp

    icnt, imiss = _count_intents_for_day(day)
    resp["counts"]["intents"] = icnt
    resp["missing_paths"].extend(imiss)

    if not SUBMISSIONS_ROOT.exists():
        resp["warnings"].append(E_SUBMISSIONS_ROOT_MISSING)
        resp["missing_paths"].append(str(SUBMISSIONS_ROOT))

    submissions, miss, sps, smt = _scan_submissions_for_day(day)
    resp["missing_paths"].extend(miss)
    resp["source_paths"].extend(sps)
    resp["source_mtimes"].update(smt)

    if not submissions:
        resp["warnings"].append(E_NO_SUBMISSIONS_FOUND)
    resp["counts"]["submissions"] = len(submissions)

    planned_actions = 0
    any_plan = False
    for rec in submissions:
        op = rec.get("order_plan")
        if op is None:
            continue
        any_plan = True
        if isinstance(op, dict):
            acts = op.get("actions")
            if isinstance(acts, list):
                planned_actions += len(acts)
            else:
                planned_actions += 1
        else:
            planned_actions += 1

    resp["counts"]["planned_actions"] = planned_actions if any_plan else 0
    if not any_plan:
        resp["warnings"].append(E_NO_ORDER_PLAN_PRESENT)

    subid_to_engine, miss2, sps2, smt2, warns2 = _load_engine_join_map_for_day(day)
    resp["missing_paths"].extend(miss2)
    resp["source_paths"].extend(sps2)
    resp["source_mtimes"].update(smt2)
    resp["warnings"].extend(warns2)

    by_engine: Dict[str, Dict[str, Any]] = {}

    def eng_for(submission_id: str) -> str:
        e = subid_to_engine.get(submission_id)
        return e if isinstance(e, str) and e else "unknown"

    for rec in submissions:
        subid = str(rec.get("submission_id") or rec.get("submission_dir") or "unknown")
        engine = eng_for(subid)

        if engine not in by_engine:
            by_engine[engine] = {
                "engine": engine,
                "submissions": 0,
                "fills": 0,
                "partials": 0,
                "rejects": 0,
                "errors": 0,
                "unknown_status": 0,
            }
        by_engine[engine]["submissions"] += 1

        bsr = rec.get("broker_submission_record") or {}
        status = bsr.get("status") if isinstance(bsr, dict) else None

        eer = rec.get("execution_event_record")
        if isinstance(eer, dict):
            ev_status = eer.get("status")
            if isinstance(ev_status, str):
                s = ev_status.upper()
                if "FILL" in s:
                    resp["counts"]["fills"] += 1
                    by_engine[engine]["fills"] += 1
                elif "PART" in s:
                    resp["counts"]["partials"] += 1
                    by_engine[engine]["partials"] += 1

        if isinstance(status, str):
            s2 = status.upper()
            if "REJECT" in s2:
                resp["counts"]["rejects"] += 1
                by_engine[engine]["rejects"] += 1
            elif "ERROR" in s2 or "FAIL" in s2:
                resp["counts"]["errors"] += 1
                by_engine[engine]["errors"] += 1
        else:
            resp["counts"]["unknown_status"] += 1
            by_engine[engine]["unknown_status"] += 1

    resp["by_engine"] = [by_engine[k] for k in sorted(by_engine.keys())]

    nav, miss3, sps3, smt3, warns3 = _nav_summary_for_day(day)
    resp["nav"] = nav
    resp["missing_paths"].extend(miss3)
    resp["source_paths"].extend(sps3)
    resp["source_mtimes"].update(smt3)
    resp["warnings"].extend(warns3)

    mt_values = [v for v in resp["source_mtimes"].values() if isinstance(v, (int, float))]
    resp["data_freshness_max_mtime"] = max(mt_values) if mt_values else None

    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    resp["warnings"] = sorted(set(resp["warnings"]))
    resp["errors"] = sorted(set(resp["errors"]))
    return resp


def _day_plan(day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "errors": [],
        "warnings": [],
        "source_paths": [],
        "source_mtimes": {},
        "missing_paths": [],
        "plans": [],
    }

    if not _is_day_str(day):
        resp["ok"] = False
        resp["errors"].append(E_DAY_INVALID)
        return resp

    submissions, miss, sps, smt = _scan_submissions_for_day(day)
    resp["missing_paths"].extend(miss)
    resp["source_paths"].extend(sps)
    resp["source_mtimes"].update(smt)

    plans: List[Dict[str, Any]] = []
    for rec in submissions:
        op = rec.get("order_plan")
        if op is None:
            continue
        plans.append({"submission_id": rec.get("submission_id"), "order_plan": op})

    if not plans:
        resp["warnings"].append(E_NO_ORDER_PLAN_PRESENT)
    resp["plans"] = plans

    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    return resp


def _day_submissions(day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "errors": [],
        "warnings": [],
        "source_paths": [],
        "source_mtimes": {},
        "missing_paths": [],
        "submissions": [],
        "engine_join": {"status": "unknown", "warning": E_ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE, "source_paths": []},
    }

    if not _is_day_str(day):
        resp["ok"] = False
        resp["errors"].append(E_DAY_INVALID)
        return resp

    submissions, miss, sps, smt = _scan_submissions_for_day(day)
    resp["missing_paths"].extend(miss)
    resp["source_paths"].extend(sps)
    resp["source_mtimes"].update(smt)

    if not submissions:
        resp["warnings"].append(E_NO_SUBMISSIONS_FOUND)

    subid_to_engine, miss2, sps2, smt2, warns2 = _load_engine_join_map_for_day(day)
    resp["missing_paths"].extend(miss2)
    resp["source_paths"].extend(sps2)
    resp["source_mtimes"].update(smt2)

    if subid_to_engine:
        resp["engine_join"] = {"status": "available", "warning": None, "source_paths": sps2}
    else:
        resp["warnings"].extend(warns2)

    out = []
    for rec in submissions:
        subid = str(rec.get("submission_id") or rec.get("submission_dir") or "unknown")
        engine = subid_to_engine.get(subid, "unknown")
        x = dict(rec)
        x["engine"] = engine
        out.append(x)

    resp["submissions"] = out

    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    resp["warnings"] = sorted(set(resp["warnings"]))
    resp["errors"] = sorted(set(resp["errors"]))
    return resp


def build_operational_truth_v1(truth_root: Path, day: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "day_utc": day,
        "truth_root": str(truth_root),
        "errors": [],
        "warnings": [],
        "missing_paths": [],
        "source_paths": [],
        "source_mtimes": {},
        "summary": {
            "positions_total": 0,
            "open_positions": 0,
            "orders_total": 0,
            "working_orders": 0,
            "alerts_total": 0,
            "readiness_status": "UNKNOWN",
        },
        "positions_panel": {
            "asof_utc": None,
            "snapshot_path": None,
            "pointer_path": None,
            "rows": [],
        },
        "orders_panel": {
            "rows": [],
        },
        "system_state_panel": {
            "rows": [],
        },
        "alerts_panel": {
            "rows": [],
        },
    }

    if not _is_day_str(day):
        resp["ok"] = False
        resp["errors"].append(E_DAY_INVALID)
        return resp

    def note_source(path: Path) -> None:
        p = str(path.resolve())
        resp["source_paths"].append(p)
        mt = _mtime(path)
        if mt is not None:
            resp["source_mtimes"][p] = mt

    def read_json(path: Path) -> Optional[Any]:
        obj, err = _safe_read_json(path)
        if obj is None:
            if err == "FILE_NOT_FOUND":
                resp["missing_paths"].append(str(path.resolve()))
            else:
                resp["warnings"].append(f"UNREADABLE:{path.name}:{err}")
            return None
        note_source(path)
        return obj

    def fmt_cents_to_dollars(value: Any) -> Optional[str]:
        try:
            if value is None:
                return None
            cents = int(value)
            return f"{cents / 100:.2f}"
        except Exception:
            return None

    def parse_isoish(value: Any) -> str:
        return str(value).strip() if isinstance(value, str) and str(value).strip() else ""

    def latest_ts(*values: Any) -> Optional[str]:
        items = [parse_isoish(v) for v in values if parse_isoish(v)]
        return max(items) if items else None

    def add_alert(severity: str, code: str, summary: str, artifact_path: Optional[str] = None) -> None:
        resp["alerts_panel"]["rows"].append(
            {
                "severity": severity,
                "code": code,
                "summary": summary,
                "artifact_path": artifact_path,
            }
        )

    def add_system_row(
        key: str,
        label: str,
        status: Any,
        detail: str,
        artifact_path: Optional[str],
        produced_utc: Optional[str],
    ) -> None:
        resp["system_state_panel"]["rows"].append(
            {
                "key": key,
                "label": label,
                "status": str(status or "UNKNOWN"),
                "detail": detail,
                "artifact_path": artifact_path,
                "produced_utc": produced_utc,
            }
        )

    def extract_order_terms(order_plan: Any) -> Dict[str, Any]:
        if not isinstance(order_plan, dict):
            return {}
        order_terms = order_plan.get("order_terms")
        if isinstance(order_terms, dict):
            return order_terms
        return {}

    def find_order_plan(submission_dir: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        for name in ["equity_order_plan.v1.json", "equity_order_plan.v2.json"]:
            candidate = (submission_dir / name).resolve()
            obj = read_json(candidate)
            if isinstance(obj, dict):
                return obj, str(candidate)
        return None, None

    positions_pointer_path = (truth_root / "positions_v1" / "effective_v1" / "days" / day / "positions_effective_pointer.v1.json").resolve()
    positions_snapshot_path: Optional[Path] = None
    positions_pointer = read_json(positions_pointer_path)
    if isinstance(positions_pointer, dict):
        raw_snapshot_path = (((positions_pointer.get("pointers") or {}) if isinstance(positions_pointer.get("pointers"), dict) else {}).get("snapshot_path"))
        if isinstance(raw_snapshot_path, str) and raw_snapshot_path:
            candidate = Path(raw_snapshot_path).resolve()
            snapshot_obj = read_json(candidate)
            if isinstance(snapshot_obj, dict):
                positions_snapshot_path = candidate
                resp["positions_panel"]["pointer_path"] = str(positions_pointer_path)
                resp["positions_panel"]["snapshot_path"] = str(candidate)
                resp["positions_panel"]["asof_utc"] = (((snapshot_obj.get("positions") or {}) if isinstance(snapshot_obj.get("positions"), dict) else {}).get("asof_utc"))
                items = (((snapshot_obj.get("positions") or {}) if isinstance(snapshot_obj.get("positions"), dict) else {}).get("items"))
                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        instrument = item.get("instrument") if isinstance(item.get("instrument"), dict) else {}
                        avg_price = fmt_cents_to_dollars(item.get("avg_cost_cents"))
                        row = {
                            "position_id": item.get("position_id"),
                            "engine_id": item.get("engine_id"),
                            "symbol": instrument.get("symbol"),
                            "quantity": item.get("qty"),
                            "avg_price": avg_price,
                            "status": item.get("status"),
                            "currency": instrument.get("currency"),
                            "unrealized_pnl": None,
                        }
                        resp["positions_panel"]["rows"].append(row)
    if positions_snapshot_path is None:
        for name in ["positions_snapshot.v4.json", "positions_snapshot.v2.json"]:
            candidate = (truth_root / "positions_v1" / "snapshots" / day / name).resolve()
            snapshot_obj = read_json(candidate)
            if not isinstance(snapshot_obj, dict):
                continue
            positions_snapshot_path = candidate
            resp["positions_panel"]["snapshot_path"] = str(candidate)
            positions = snapshot_obj.get("positions")
            if isinstance(positions, dict):
                resp["positions_panel"]["asof_utc"] = positions.get("asof_utc")
                items = positions.get("items")
                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        instrument = item.get("instrument") if isinstance(item.get("instrument"), dict) else {}
                        avg_price = fmt_cents_to_dollars(item.get("avg_cost_cents"))
                        resp["positions_panel"]["rows"].append(
                            {
                                "position_id": item.get("position_id"),
                                "engine_id": item.get("engine_id"),
                                "symbol": instrument.get("symbol"),
                                "quantity": item.get("qty"),
                                "avg_price": avg_price,
                                "status": item.get("status"),
                                "currency": instrument.get("currency"),
                                "unrealized_pnl": None,
                            }
                        )
            break

    stream_day_dir = (truth_root / "execution_stream_v1" / day).resolve()
    latest_stream_by_submission: Dict[str, Dict[str, Any]] = {}
    if stream_day_dir.exists() and stream_day_dir.is_dir():
        for record_path in sorted(stream_day_dir.iterdir(), key=lambda p: p.name):
            if not record_path.is_file() or record_path.suffix != ".json":
                continue
            rec = read_json(record_path)
            if not isinstance(rec, dict):
                continue
            submission_id = str(rec.get("submission_id") or "").strip()
            if not submission_id:
                continue
            ts = latest_ts(rec.get("observed_at_utc"), rec.get("event_time_utc"), rec.get("produced_utc")) or ""
            cur = latest_stream_by_submission.get(submission_id)
            cur_ts = latest_ts((cur or {}).get("_latest_ts")) or ""
            if cur is None or ts >= cur_ts:
                rec["_latest_ts"] = ts
                rec["_artifact_path"] = str(record_path.resolve())
                latest_stream_by_submission[submission_id] = rec
            reason_codes = rec.get("reason_codes") if isinstance(rec.get("reason_codes"), list) else []
            if any("ORPHAN" in str(code).upper() for code in reason_codes):
                broker_ids = rec.get("broker_ids") if isinstance(rec.get("broker_ids"), dict) else {}
                add_alert(
                    "WARNING",
                    "ORPHAN_EVENT_LINEAGE",
                    f"submission_id={submission_id} order_id={broker_ids.get('order_id')} perm_id={broker_ids.get('perm_id')}",
                    str(record_path.resolve()),
                )

    failure_path = (truth_root / "execution_stream_v1" / "failures" / day / "failure.json").resolve()
    failure_doc = read_json(failure_path)
    if isinstance(failure_doc, dict):
        add_alert(
            "ERROR",
            str(failure_doc.get("reason_code") or failure_doc.get("error_code") or "EXECUTION_STREAM_FAILURE"),
            str(failure_doc.get("summary") or failure_doc.get("details") or "execution_stream_v1 failure"),
            str(failure_path),
        )

    submissions_root = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    fill_ledger_root = (truth_root / "fill_ledger_v1" / day).resolve()
    lifecycle_authority_path = (
        truth_root
        / "reports"
        / "execution_lifecycle_authority_v1"
        / day
        / "execution_lifecycle_authority.v1.json"
    ).resolve()
    lifecycle_authority = read_json(lifecycle_authority_path)
    lifecycle_by_submission = {
        str(row.get("submission_id") or "").strip(): row
        for row in ((lifecycle_authority or {}).get("submissions") or [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }
    lineage_candidates = [
        (
            truth_root
            / "reports"
            / "trade_lineage_graph_v1"
            / day
            / "trade_lineage_graph.v1.json"
        ).resolve(),
        (
            GLOBAL_TRUTH_ROOT
            / "reports"
            / "trade_lineage_graph_v1"
            / day
            / "trade_lineage_graph.v1.json"
        ).resolve(),
    ]
    lineage_authority_path = next((path for path in lineage_candidates if path.exists()), lineage_candidates[0])
    lineage_authority = read_json(lineage_authority_path)
    lineage_by_submission = {
        str(row.get("submission_id") or "").strip(): row
        for row in ((lineage_authority or {}).get("lineages") or [])
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip()
    }
    if submissions_root.exists() and submissions_root.is_dir():
        for submission_dir in sorted([p for p in submissions_root.iterdir() if p.is_dir()], key=lambda p: p.name):
            if submission_dir.name.startswith("__"):
                continue
            submission_id = submission_dir.name
            lifecycle_row = lifecycle_by_submission.get(submission_id) or {}
            lineage_row = lineage_by_submission.get(submission_id) or {}
            broker_record_path = (submission_dir / "broker_submission_record.v2.json").resolve()
            execution_event_path = (submission_dir / "execution_event_record.v1.json").resolve()
            broker_record = read_json(broker_record_path)
            execution_event = read_json(execution_event_path)
            order_plan, order_plan_path = find_order_plan(submission_dir)
            fill_ledger_path = (fill_ledger_root / f"{submission_id}.fill_ledger.v1.json").resolve()
            fill_ledger = read_json(fill_ledger_path)
            latest_stream = latest_stream_by_submission.get(submission_id)

            broker_ids = {}
            if lifecycle_row:
                broker_ids = {
                    "order_id": lifecycle_row.get("broker_order_id"),
                    "perm_id": lifecycle_row.get("broker_perm_id"),
                }
            elif isinstance(broker_record, dict) and isinstance(broker_record.get("broker_ids"), dict):
                broker_ids = broker_record.get("broker_ids") or {}
            elif isinstance(latest_stream, dict) and isinstance(latest_stream.get("broker_ids"), dict):
                broker_ids = latest_stream.get("broker_ids") or {}

            order_terms = extract_order_terms(order_plan)
            order_status = (
                lifecycle_row.get("current_lifecycle_state")
                or (((latest_stream.get("order_state") or {}) if isinstance((latest_stream or {}).get("order_state"), dict) else {}).get("status"))
                or (execution_event.get("raw_broker_status") if isinstance(execution_event, dict) else None)
                or (broker_record.get("status") if isinstance(broker_record, dict) else None)
                or (fill_ledger.get("lifecycle_status") if isinstance(fill_ledger, dict) else None)
                or "UNKNOWN"
            )
            last_update_utc = latest_ts(
                ((latest_stream.get("observed_at_utc") if isinstance(latest_stream, dict) else None)),
                ((latest_stream.get("event_time_utc") if isinstance(latest_stream, dict) else None)),
                (execution_event.get("event_time_utc") if isinstance(execution_event, dict) else None),
                (broker_record.get("submitted_at_utc") if isinstance(broker_record, dict) else None),
                (fill_ledger.get("produced_utc") if isinstance(fill_ledger, dict) else None),
            )

            row = {
                "submission_id": submission_id,
                "symbol": order_plan.get("symbol") if isinstance(order_plan, dict) else None,
                "side": order_plan.get("action") if isinstance(order_plan, dict) else None,
                "quantity": order_plan.get("qty_shares") if isinstance(order_plan, dict) else None,
                "status": str(order_status),
                "broker_order_id": broker_ids.get("order_id"),
                "perm_id": broker_ids.get("perm_id"),
                "last_update_utc": last_update_utc,
                "order_type": order_terms.get("order_type"),
                "time_in_force": order_terms.get("time_in_force"),
                "limit_price": order_terms.get("limit_price"),
                "filled_qty": (fill_ledger.get("filled_qty") if isinstance(fill_ledger, dict) else None),
                "remaining_qty": (fill_ledger.get("remaining_qty") if isinstance(fill_ledger, dict) else None),
                "lifecycle_status": (fill_ledger.get("lifecycle_status") if isinstance(fill_ledger, dict) else None),
                "identity_state": lineage_row.get("identity_state") if lineage_row else "UNKNOWN",
                "trade_lineage_id": lineage_row.get("trade_lineage_id") if lineage_row else None,
                "artifact_paths": {
                    "submission_dir": str(submission_dir.resolve()),
                    "broker_submission_record": str(broker_record_path),
                    "execution_event_record": str(execution_event_path),
                    "order_plan": order_plan_path,
                    "fill_ledger": str(fill_ledger_path),
                    "latest_stream": latest_stream.get("_artifact_path") if isinstance(latest_stream, dict) else None,
                    "execution_lifecycle_authority": str(lifecycle_authority_path),
                    "trade_lineage_graph": str(lineage_authority_path),
                },
            }
            resp["orders_panel"]["rows"].append(row)

            latest_hash = str(latest_stream.get("canonical_json_hash") or "") if isinstance(latest_stream, dict) else ""
            if latest_hash and isinstance(execution_event, dict):
                if str(execution_event.get("upstream_hash") or "") != latest_hash:
                    add_alert(
                        "WARNING",
                        "STALE_EXECUTION_EVENT",
                        f"submission_id={submission_id} execution_event_record.v1.json does not reflect latest execution_stream observation",
                        str(execution_event_path),
                    )
            if latest_hash and isinstance(fill_ledger, dict):
                hashes = fill_ledger.get("event_hashes") if isinstance(fill_ledger.get("event_hashes"), list) else []
                if latest_hash not in [str(x) for x in hashes]:
                    add_alert(
                        "WARNING",
                        "STALE_FILL_LEDGER",
                        f"submission_id={submission_id} fill_ledger_v1 missing latest execution_stream hash",
                        str(fill_ledger_path),
                    )

    build_ref = None
    admission_ref = None
    boundary_ref = None
    ledger_ref = None
    control_plane_ref = None
    day_authority_ref = None
    session_status_ref = None
    try:
        build_ref = read_control_plane_surface_v1(domain="session", surface="target_day_build", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        admission_ref = read_control_plane_surface_v1(domain="session", surface="target_day_admission", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        boundary_ref = read_control_plane_surface_v1(domain="execution", surface="submit_boundary_status", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        ledger_ref = read_control_plane_surface_v1(domain="execution", surface="paper_session_ledger", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        control_plane_ref = read_control_plane_surface_v1(domain="execution", surface="paper_day_control_plane", truth_root=truth_root, day_utc=day)
    except Exception:
        pass
    try:
        day_authority_ref = read_control_plane_surface_v1(
            domain="execution",
            surface="paper_trading_day_authority",
            truth_root=truth_root,
            day_utc=day,
        )
    except Exception:
        pass
    try:
        session_status_ref = read_control_plane_surface_v1(domain="session", surface="session_authority_status_current", truth_root=truth_root)
    except Exception:
        pass
    build_path = build_ref.path if build_ref is not None else (truth_root / "target_day_build_v1" / f"{day}.json").resolve()
    admission_path = admission_ref.path if admission_ref is not None else (truth_root / "target_day_admission_v1" / f"{day}.json").resolve()
    boundary_path = boundary_ref.path if boundary_ref is not None else (truth_root / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json").resolve()
    ledger_path = ledger_ref.path if ledger_ref is not None else (truth_root / "reports" / "paper_session_ledger_v1" / day / "paper_session_ledger.v1.json").resolve()
    control_plane_path = control_plane_ref.path if control_plane_ref is not None else (truth_root / "reports" / "paper_day_control_plane_v1" / day / "paper_day_control_plane.v1.json").resolve()
    day_authority_path = (
        day_authority_ref.path
        if day_authority_ref is not None
        else (truth_root / "reports" / "paper_trading_day_authority_v1" / day / "paper_trading_day_authority.v1.json").resolve()
    )
    session_status_path = session_status_ref.path if session_status_ref is not None else (truth_root / "session_authority_status_v1" / "current.json").resolve()
    execution_recon_path = (truth_root / "reports" / "execution_reconciliation_v1" / day / "execution_reconciliation.v1.json").resolve()
    runtime_service_authority_path = (truth_root / "reports" / "runtime_service_authority_v1" / day / "runtime_service_authority.v1.json").resolve()
    market_data_authority_path = (truth_root / "reports" / "market_data_authority_v1" / day / "market_data_authority.v1.json").resolve()
    strategy_decision_authority_path = (truth_root / "reports" / "strategy_decision_authority_v1" / day / "strategy_decision_authority.v1.json").resolve()
    portfolio_account_authority_path = (truth_root / "reports" / "portfolio_account_authority_v1" / day / "portfolio_account_authority.v1.json").resolve()
    risk_sizing_authority_path = (truth_root / "reports" / "risk_sizing_authority_v1" / day / "risk_sizing_authority.v1.json").resolve()
    execution_mode_authority_path = (truth_root / "reports" / "execution_mode_authority_v1" / day / "execution_mode_authority.v1.json").resolve()
    trading_day_closure_authority_path = (truth_root / "reports" / "trading_day_closure_authority_v1" / day / "trading_day_closure_authority.v1.json").resolve()
    aegis_operating_contract_path = (truth_root / "reports" / "aegis_operating_contract_v1" / day / "aegis_operating_contract.v1.json").resolve()
    aegis_authority_graph_path = (truth_root / "reports" / "aegis_authority_graph_v1" / day / "aegis_authority_graph.v1.json").resolve()
    aegis_day_evidence_ledger_path = (truth_root / "reports" / "aegis_day_evidence_ledger_v1" / day / "aegis_day_evidence_ledger.v1.json").resolve()
    aegis_daily_operator_summary_path = (truth_root / "reports" / "aegis_daily_operator_summary_v1" / day / "aegis_daily_operator_summary.v1.json").resolve()
    try:
        replay_gate_ref = read_control_plane_surface_v1(domain="execution", surface="replay_certification_gate", truth_root=truth_root, day_utc=day)
    except Exception:
        replay_gate_ref = None
    try:
        replay_bundle_ref = read_control_plane_surface_v1(domain="execution", surface="replay_certification_bundle", truth_root=truth_root, day_utc=day)
    except Exception:
        replay_bundle_ref = None
    replay_gate_path = replay_gate_ref.path if replay_gate_ref is not None else (truth_root / "reports" / "replay_certification_gate_v1" / day / "replay_certification_gate.v1.json").resolve()
    replay_bundle_path = replay_bundle_ref.path if replay_bundle_ref is not None else (truth_root / "reports" / "replay_certification_bundle_v1" / day / "replay_certification_bundle.v1.json").resolve()

    build_doc = dict(build_ref.payload) if build_ref is not None else read_json(build_path)
    admission_doc = dict(admission_ref.payload) if admission_ref is not None else read_json(admission_path)
    boundary_doc = dict(boundary_ref.payload) if boundary_ref is not None else read_json(boundary_path)
    ledger_doc = dict(ledger_ref.payload) if ledger_ref is not None else read_json(ledger_path)
    control_plane_doc = dict(control_plane_ref.payload) if control_plane_ref is not None else read_json(control_plane_path)
    day_authority_doc = dict(day_authority_ref.payload) if day_authority_ref is not None else read_json(day_authority_path)
    session_status_doc = dict(session_status_ref.payload) if session_status_ref is not None else read_json(session_status_path)
    execution_recon_doc = read_json(execution_recon_path)
    runtime_service_authority_doc = read_json(runtime_service_authority_path)
    market_data_authority_doc = read_json(market_data_authority_path)
    strategy_decision_authority_doc = read_json(strategy_decision_authority_path)
    portfolio_account_authority_doc = read_json(portfolio_account_authority_path)
    risk_sizing_authority_doc = read_json(risk_sizing_authority_path)
    execution_mode_authority_doc = read_json(execution_mode_authority_path)
    trading_day_closure_authority_doc = read_json(trading_day_closure_authority_path)
    aegis_operating_contract_doc = read_json(aegis_operating_contract_path)
    aegis_authority_graph_doc = read_json(aegis_authority_graph_path)
    aegis_day_evidence_ledger_doc = read_json(aegis_day_evidence_ledger_path)
    aegis_daily_operator_summary_doc = read_json(aegis_daily_operator_summary_path)
    replay_gate_doc = dict(replay_gate_ref.payload) if replay_gate_ref is not None else None
    replay_bundle_doc = dict(replay_bundle_ref.payload) if replay_bundle_ref is not None else None

    if isinstance(build_doc, dict):
        add_system_row(
            "build",
            "Build",
            build_doc.get("build_status"),
            f"completeness={build_doc.get('completeness_result', 'n/a')} closure={build_doc.get('closure_status', 'n/a')}",
            str(build_path),
            build_doc.get("generated_utc"),
        )
    if isinstance(admission_doc, dict):
        add_system_row(
            "admission",
            "Admission",
            admission_doc.get("admission_status"),
            f"closure={admission_doc.get('closure_status', 'n/a')}",
            str(admission_path),
            admission_doc.get("generated_utc"),
        )
    if isinstance(day_authority_doc, dict):
        add_system_row(
            "day_authority",
            "Day Authority",
            day_authority_doc.get("state"),
            (
                f"submit={day_authority_doc.get('can_submit_paper_orders')} "
                f"trade_today={day_authority_doc.get('can_paper_trade_today')} "
                f"blocker={day_authority_doc.get('canonical_blocker') or '<none>'}"
            ),
            str(day_authority_path),
            day_authority_doc.get("produced_at_utc"),
        )
    if isinstance(aegis_daily_operator_summary_doc, dict):
        add_system_row(
            "aegis_daily_operator_summary",
            "Aegis Daily Summary",
            aegis_daily_operator_summary_doc.get("no_silent_day_outcome"),
            (
                f"mode={aegis_daily_operator_summary_doc.get('mode')} "
                f"run_style={aegis_daily_operator_summary_doc.get('run_style')} "
                f"dry_run={aegis_daily_operator_summary_doc.get('dry_run')} "
                f"transmitted={aegis_daily_operator_summary_doc.get('broker_orders_transmitted')} "
                f"first_blocker={(aegis_daily_operator_summary_doc.get('first_blocker') or {}).get('code', '<none>') if isinstance(aegis_daily_operator_summary_doc.get('first_blocker'), dict) else '<none>'}"
            ),
            str(aegis_daily_operator_summary_path),
            aegis_daily_operator_summary_doc.get("produced_utc"),
        )
    if isinstance(aegis_operating_contract_doc, dict):
        add_system_row(
            "aegis_operating_contract",
            "Operating Contract",
            aegis_operating_contract_doc.get("mode"),
            f"run_style={aegis_operating_contract_doc.get('run_style')} target={aegis_operating_contract_doc.get('target_sleeve')}",
            str(aegis_operating_contract_path),
            aegis_operating_contract_doc.get("produced_utc"),
        )
    if isinstance(aegis_authority_graph_doc, dict):
        graph_nodes = aegis_authority_graph_doc.get("authority_nodes") if isinstance(aegis_authority_graph_doc.get("authority_nodes"), list) else []
        graph_blockers = aegis_authority_graph_doc.get("blocking_nodes") if isinstance(aegis_authority_graph_doc.get("blocking_nodes"), list) else []
        add_system_row(
            "aegis_authority_graph",
            "Authority Graph",
            "BLOCKED" if graph_blockers else "CLEAR",
            f"nodes={len(graph_nodes)} blockers={len(graph_blockers)}",
            str(aegis_authority_graph_path),
            aegis_authority_graph_doc.get("produced_utc"),
        )
    if isinstance(aegis_day_evidence_ledger_doc, dict):
        commands = aegis_day_evidence_ledger_doc.get("commands") if isinstance(aegis_day_evidence_ledger_doc.get("commands"), list) else []
        add_system_row(
            "aegis_day_evidence_ledger",
            "Evidence Ledger",
            aegis_day_evidence_ledger_doc.get("final_daily_outcome"),
            f"commands={len(commands)} blockers={len(aegis_day_evidence_ledger_doc.get('blockers') or [])}",
            str(aegis_day_evidence_ledger_path),
            aegis_day_evidence_ledger_doc.get("finished_utc"),
        )
    if isinstance(runtime_service_authority_doc, dict):
        add_system_row(
            "runtime_service_authority",
            "Runtime Services",
            runtime_service_authority_doc.get("service_state"),
            f"mode={runtime_service_authority_doc.get('expected_run_mode')} submit_creator={runtime_service_authority_doc.get('submit_creator_available')}",
            str(runtime_service_authority_path),
            runtime_service_authority_doc.get("produced_utc"),
        )
    if isinstance(market_data_authority_doc, dict):
        market_impact = str(market_data_authority_doc.get("operator_impact") or "UNKNOWN")
        add_system_row(
            "market_data_authority",
            "Market Data",
            market_data_authority_doc.get("market_data_state"),
            f"impact={market_impact} symbols={','.join(str(x) for x in market_data_authority_doc.get('required_symbols', []))} blocker={market_data_authority_doc.get('first_blocker') or '<none>'}",
            str(market_data_authority_path),
            market_data_authority_doc.get("produced_utc"),
        )
    if isinstance(strategy_decision_authority_doc, dict):
        add_system_row(
            "strategy_decision_authority",
            "Strategy Decision",
            strategy_decision_authority_doc.get("strategy_decision_state"),
            (
                f"intent_count={strategy_decision_authority_doc.get('intent_count')} "
                f"zero_reason={strategy_decision_authority_doc.get('zero_intent_reason') or '<none>'}"
            ),
            str(strategy_decision_authority_path),
            strategy_decision_authority_doc.get("produced_utc"),
        )
    if isinstance(portfolio_account_authority_doc, dict):
        account_values = portfolio_account_authority_doc.get("account_values") if isinstance(portfolio_account_authority_doc.get("account_values"), dict) else {}
        add_system_row(
            "portfolio_account_authority",
            "Portfolio Account",
            portfolio_account_authority_doc.get("account_state"),
            (
                f"source={portfolio_account_authority_doc.get('source_type')} "
                f"cash_cents={account_values.get('cash_total_cents')} "
                f"nlv_cents={account_values.get('net_liquidation_cents')}"
            ),
            str(portfolio_account_authority_path),
            portfolio_account_authority_doc.get("produced_utc"),
        )
    if isinstance(risk_sizing_authority_doc, dict):
        final_size = risk_sizing_authority_doc.get("final_size_summary") if isinstance(risk_sizing_authority_doc.get("final_size_summary"), dict) else {}
        final_qty = final_size.get("final_quantity") if isinstance(final_size, dict) else None
        final_risk = final_size.get("final_risk_cents") if isinstance(final_size, dict) else None
        add_system_row(
            "risk_sizing_authority",
            "Risk Sizing",
            risk_sizing_authority_doc.get("risk_sizing_state"),
            (
                f"final_qty={final_qty} final_risk_cents={final_risk} "
                f"reason={risk_sizing_authority_doc.get('first_sizing_reason') or '<none>'} "
                f"blocker={risk_sizing_authority_doc.get('first_blocker') or '<none>'}"
            ),
            str(risk_sizing_authority_path),
            risk_sizing_authority_doc.get("produced_utc"),
        )
    if isinstance(execution_mode_authority_doc, dict):
        add_system_row(
            "execution_mode_authority",
            "Execution Mode",
            execution_mode_authority_doc.get("mode_state"),
            f"broker_transmit_enabled={execution_mode_authority_doc.get('broker_transmit_enabled')} ids_expected={execution_mode_authority_doc.get('broker_ids_expected')}",
            str(execution_mode_authority_path),
            execution_mode_authority_doc.get("produced_utc"),
        )
    if isinstance(trading_day_closure_authority_doc, dict):
        add_system_row(
            "trading_day_closure_authority",
            "Trading Day Closure",
            trading_day_closure_authority_doc.get("closure_state"),
            f"closure_safe={str(trading_day_closure_authority_doc.get('closure_state') == 'DRY_RUN_CLOSED').lower()} submissions={trading_day_closure_authority_doc.get('submission_count')} blocker={trading_day_closure_authority_doc.get('first_blocker') or '<none>'}",
            str(trading_day_closure_authority_path),
            trading_day_closure_authority_doc.get("produced_utc"),
        )
    if isinstance(boundary_doc, dict):
        add_system_row(
            "boundary",
            "Boundary",
            boundary_doc.get("boundary_status"),
            f"submission_authorized={boundary_doc.get('submission_authorized')}",
            str(boundary_path),
            boundary_doc.get("produced_at_utc"),
        )
    if isinstance(ledger_doc, dict):
        control_state = ledger_doc.get("control_state") if isinstance(ledger_doc.get("control_state"), dict) else {}
        add_system_row(
            "ledger",
            "Ledger",
            control_state.get("authority_status"),
            f"evidence={ledger_doc.get('evidence_status', 'n/a')} system_ready={control_state.get('system_ready')}",
            str(ledger_path),
            ledger_doc.get("evaluated_at_utc"),
        )
    if isinstance(control_plane_doc, dict):
        add_system_row(
            "control_plane",
            "Control Plane",
            control_plane_doc.get("final_start_decision"),
            str(control_plane_doc.get("human_readable_summary") or ""),
            str(control_plane_path),
            control_plane_doc.get("evaluated_at_utc"),
        )
    consistency_status = "UNKNOWN"
    consistency_detail = ""
    if isinstance(session_status_doc, dict):
        paper_projection = (
            session_status_doc.get("paper_authority_projection")
            if isinstance(session_status_doc.get("paper_authority_projection"), dict)
            else {}
        )
        add_system_row(
            "session_status",
            "Session Status",
            paper_projection.get("open_state")
            or session_status_doc.get("submission_authorization_status"),
            (
                f"authority={paper_projection.get('authority_status', 'UNKNOWN')} "
                f"degraded={paper_projection.get('degraded_mode', False)} "
                f"submission_authorized={paper_projection.get('submission_authorized', False)} "
                f"traceability={session_status_doc.get('traceability_status', 'n/a')}"
            ),
            str(session_status_path),
            session_status_doc.get("generated_utc"),
        )
        monitoring_checks = session_status_doc.get("monitoring_checks") if isinstance(session_status_doc.get("monitoring_checks"), list) else []
        for check in monitoring_checks:
            if not isinstance(check, dict):
                continue
            if str(check.get("check_name") or "") == "canonical_readiness_authority":
                consistency_status = check.get("status") or "UNKNOWN"
                consistency_detail = str(check.get("summary") or "")
                break
        if consistency_status == "UNKNOWN":
            traceability = str(session_status_doc.get("traceability_status") or "")
            if traceability.upper() == "VALID":
                consistency_status = "PASS"
                consistency_detail = "traceability_status=VALID"
            elif traceability:
                consistency_status = traceability
                consistency_detail = f"traceability_status={traceability}"
        top_blockers = session_status_doc.get("top_blocker_reason_codes") if isinstance(session_status_doc.get("top_blocker_reason_codes"), list) else []
        for code in top_blockers:
            add_alert(
                "WARNING",
                f"SESSION_AUTHORITY:{code}",
                f"session_authority_status_v1 reported {code}",
                str(session_status_path),
            )
    add_system_row(
        "consistency_gate",
        "Consistency Gate",
        consistency_status,
        consistency_detail or "No canonical consistency detail available",
        str(session_status_path),
        session_status_doc.get("generated_utc") if isinstance(session_status_doc, dict) else None,
    )

    if isinstance(execution_recon_doc, dict):
        recon_status = str(execution_recon_doc.get("status") or "UNKNOWN")
        reason_codes = execution_recon_doc.get("reason_codes") if isinstance(execution_recon_doc.get("reason_codes"), list) else []
        if recon_status.upper() != "PASS" or reason_codes:
            add_alert(
                "WARNING" if recon_status.upper() == "PASS" else "ERROR",
                "EXECUTION_RECONCILIATION",
                f"status={recon_status} reason_codes={','.join([str(x) for x in reason_codes]) or 'none'}",
                str(execution_recon_path),
            )
    if isinstance(replay_gate_doc, dict) and str(replay_gate_doc.get("status") or "").upper() != "PASS":
        add_alert(
            "ERROR",
            "REPLAY_CERTIFICATION_GATE",
            f"status={replay_gate_doc.get('status')} reason_codes={','.join([str(x) for x in (replay_gate_doc.get('reason_codes') or [])]) or 'none'}",
            str(replay_gate_path),
        )
    elif replay_gate_doc is None and replay_bundle_doc is not None:
        add_alert(
            "WARNING",
            "REPLAY_GATE_MISSING",
            "canonical replay certification gate is unavailable",
            str(replay_gate_path),
        )
    if isinstance(replay_bundle_doc, dict) and str(replay_bundle_doc.get("status") or "").upper() != "PASS":
        add_alert(
            "ERROR",
            "REPLAY_CERTIFICATION_BUNDLE",
            f"status={replay_bundle_doc.get('status')}",
            str(replay_bundle_path),
        )

    resp["positions_panel"]["rows"].sort(key=lambda row: (str(row.get("symbol") or ""), str(row.get("position_id") or "")))
    resp["orders_panel"]["rows"].sort(key=lambda row: (str(row.get("last_update_utc") or ""), str(row.get("submission_id") or "")), reverse=True)
    resp["alerts_panel"]["rows"].sort(key=lambda row: (str(row.get("severity") or ""), str(row.get("code") or ""), str(row.get("artifact_path") or "")))
    resp["system_state_panel"]["rows"].sort(key=lambda row: [
        "build",
        "admission",
        "day_authority",
        "runtime_service_authority",
        "market_data_authority",
        "strategy_decision_authority",
        "portfolio_account_authority",
        "risk_sizing_authority",
        "execution_mode_authority",
        "boundary",
        "ledger",
        "control_plane",
        "trading_day_closure_authority",
        "session_status",
        "consistency_gate",
    ].index(str(row.get("key")) if str(row.get("key")) in {
        "build",
        "admission",
        "day_authority",
        "runtime_service_authority",
        "market_data_authority",
        "strategy_decision_authority",
        "portfolio_account_authority",
        "risk_sizing_authority",
        "execution_mode_authority",
        "boundary",
        "ledger",
        "control_plane",
        "trading_day_closure_authority",
        "session_status",
        "consistency_gate",
    } else "consistency_gate"))

    positions_rows = resp["positions_panel"]["rows"]
    order_rows = resp["orders_panel"]["rows"]
    resp["summary"]["positions_total"] = len(positions_rows)
    resp["summary"]["open_positions"] = sum(1 for row in positions_rows if str(row.get("status") or "").upper() == "OPEN")
    resp["summary"]["orders_total"] = len(order_rows)
    resp["summary"]["working_orders"] = sum(1 for row in order_rows if str(row.get("status") or "").upper() not in {"FILLED", "CANCELLED", "INACTIVE"})
    resp["summary"]["alerts_total"] = len(resp["alerts_panel"]["rows"])
    readiness_rows = {str(row.get("key")): row for row in resp["system_state_panel"]["rows"]}
    day_authority_state = str((readiness_rows.get("day_authority") or {}).get("status") or "UNKNOWN")
    control_status = str((readiness_rows.get("control_plane") or {}).get("status") or "UNKNOWN")
    session_state = str((readiness_rows.get("session_status") or {}).get("status") or "UNKNOWN")
    if day_authority_state == "OPEN_READY":
        resp["summary"]["readiness_status"] = "OPEN_READY"
    elif control_status == "READY_NOW" and session_state == "AUTHORIZED":
        resp["summary"]["readiness_status"] = "READY_NOW"
    elif control_status and control_status != "UNKNOWN":
        resp["summary"]["readiness_status"] = control_status

    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["source_paths"] = sorted(set(resp["source_paths"]))
    resp["warnings"] = sorted(set(resp["warnings"]))
    resp["errors"] = sorted(set(resp["errors"]))
    return resp


def _days_list() -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "errors": [],
        "warnings": [],
        "source_paths": [],
        "source_mtimes": {},
        "missing_paths": [],
        "days": [],
        "default_day_utc": None,
    }

    if not TRUTH_ROOT.exists():
        resp["ok"] = False
        resp["errors"].append(E_TRUTH_ROOT_MISSING)
        resp["missing_paths"].append(str(TRUTH_ROOT))
        return resp

    days = _union_days()
    resp["days"] = days
    resp["default_day_utc"] = _select_latest_day(days)
    if not days:
        resp["warnings"].append(E_NO_DAYS_FOUND)

    for p in [GATE_VERDICT_ROOT, INTENTS_ROOT, ACCOUNTING_NAV_ROOT, ACCOUNTING_ATTR_ROOT, SUBMISSIONS_ROOT, PILLARS_V1R1_ROOT, PILLARS_V1_ROOT, INTENTS_SUMMARY_ROOT, SUBMISSIONS_SUMMARY_ROOT, ACTIVITY_ROLLUP_ROOT]:
        resp["source_paths"].append(str(p))
        mt = _mtime(p)
        if mt is not None:
            resp["source_mtimes"][str(p)] = mt

    resp["source_paths"] = sorted(set(resp["source_paths"]))
    return resp


def _series_nav_endpoint(qs: Dict[str, List[str]]) -> Dict[str, Any]:
    last_n = 60
    if "days" in qs:
        try:
            last_n = int(qs["days"][0])
        except Exception:
            last_n = 60
    pts, missing, sps, smt, warns = _series_nav_points(last_n)
    return {
        "ok": True,
        "generated_utc": _utc_now_iso(),
        "errors": [],
        "warnings": warns,
        "source_paths": sps,
        "source_mtimes": smt,
        "missing_paths": missing,
        "points": pts,
    }


class OpsHandler(SimpleHTTPRequestHandler):
    STATIC_DIR = (Path(__file__).resolve().parents[1] / "static").resolve()
    SHELL_ROUTES = {
        "/",
        "/aegis-opportunities",
        "/aegis-candidates",
        "/aegis-theses",
        "/aegis-runtime-timeline",
        "/aegis-repair-center",
        "/aegis-edge-lab",
        "/aegis-performance",
        "/aegis-journal",
        "/aegis-today",
        "/aegis-review",
        "/aegis-research",
        "/aegis-history",
        "/capital",
        "/capital/accounts",
        "/capital/allocation",
        "/capital/history",
        "/capital/flows",
        "/capital/cashflow",
        "/capital/validation",
        "/portfolio",
        "/performance",
        "/outcomes",
        "/sleeves",
        "/advisory",
        "/aegis-lite",
        "/aegis-operator-cockpit",
        "/aegis-events",
        "/aegis-ai-feedback",
        "/research-lab",
        "/research-lab/start",
        "/research-lab/hypotheses",
        "/research-lab/plans",
        "/research-lab/evidence",
        "/research-lab/paper-trials",
        "/research-lab/sleeve-reviews",
        "/research-lab/blocked-work",
        "/research-lab/backlog",
        "/operator-inbox",
        "/tax",
        "/policy",
        "/operations",
        "/aegis-runtime",
        "/aegis-runtime-truth",
        "/aegis-adaptive-intelligence",
        "/aegis-intelligence-governance",
        "/configuration",
        "/reliability",
        "/reliability/readiness",
        "/reliability/issues",
        "/reliability/issues/detail",
        "/reliability/observations",
        "/reliability/work-orders",
        "/reliability/work-orders/detail",
        "/reliability/verifications",
        "/reliability/ai",
        "/reliability/ai-draft",
        "/audit",
        "/reports",
        "/control",
        "/state",
        "/submission",
        "/lifecycle",
    }

    @staticmethod
    def _local_cors_origin(origin: Optional[str]) -> Optional[str]:
        if not isinstance(origin, str) or not origin.strip():
            return None
        try:
            parsed = urlparse(origin)
        except Exception:
            return None
        scheme = (parsed.scheme or "").lower()
        hostname = (parsed.hostname or "").lower()
        if scheme not in {"http", "https"}:
            return None
        if hostname not in {"127.0.0.1", "localhost"}:
            return None
        if not parsed.netloc:
            return None
        return f"{scheme}://{parsed.netloc}"

    def end_headers(self) -> None:
        # Prevent stale browser assets; dashboard is operational truth UI.
        self.send_header("Cache-Control", "no-store")
        cors_origin = self._local_cors_origin(self.headers.get("Origin"))
        if cors_origin:
            self.send_header("Access-Control-Allow-Origin", cors_origin)
            self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Vary", "Origin")
        super().end_headers()

    def _send_json(self, code: int, obj: Any) -> None:
        b = json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _send_performance_cockpit_html(self, requested_day: Optional[str]) -> bool:
        day = requested_day if isinstance(requested_day, str) and _is_day_str(requested_day) else date.today().isoformat()
        path = (
            GLOBAL_TRUTH_ROOT
            / "reports"
            / PERFORMANCE_SHOWCASE_FAMILY
            / day
            / PERFORMANCE_SHOWCASE_HTML
        ).resolve()
        allowed_root = GLOBAL_TRUTH_ROOT.resolve()
        if not (str(path).startswith(str(allowed_root) + "/") or str(path) == str(allowed_root)):
            self._send_json(HTTPStatus.FORBIDDEN, {"ok": False, "errors": ["PATH_OUTSIDE_TRUTH_ROOT"], "path": str(path)})
            return True
        if not path.exists() or not path.is_file():
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["PERFORMANCE_COCKPIT_NOT_FOUND"], "path": str(path)})
            return True
        b = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)
        return True

    @staticmethod
    def _single_query_values(qs: Dict[str, List[str]]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for key, values in (qs or {}).items():
            if not values:
                continue
            result[key] = str(values[0])
        return result

    def _send_reliability_error(self, exc: Exception) -> None:
        code = str(exc) or "RELIABILITY_REQUEST_INVALID"
        status = HTTPStatus.BAD_REQUEST
        if code.endswith("_NOT_FOUND"):
            status = HTTPStatus.NOT_FOUND
        self._send_json(
            status,
            {
                "ok": False,
                "message": code,
                "errors": [code],
            },
        )

    def _route_reliability_get(self, path: str, qs: Dict[str, List[str]]) -> bool:
        try:
            if path == "/api/reliability/observations":
                payload = list_reliability_observations_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/issues":
                payload = list_reliability_issues_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/work-orders":
                payload = list_reliability_work_orders_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/fix-attempts":
                payload = list_reliability_fix_attempts_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/verifications":
                payload = list_reliability_verifications_v1(self._single_query_values(qs))
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path == "/api/reliability/next-actions":
                payload = list_reliability_next_actions_v1()
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path.startswith("/api/reliability/issues/"):
                parts = path.strip("/").split("/")
                if len(parts) == 5 and parts[4] == "work-orders":
                    issue_id = unquote(parts[3])
                    payload = list_reliability_issue_work_orders_v1(issue_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
                if len(parts) == 5 and parts[4] == "verifications":
                    issue_id = unquote(parts[3])
                    payload = list_reliability_issue_verifications_v1(issue_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
                if len(parts) == 4:
                    issue_id = unquote(parts[3])
                    payload = get_reliability_issue_v1(issue_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/work-orders/"):
                parts = path.strip("/").split("/")
                if len(parts) == 5 and parts[4] == "fix-attempts":
                    work_order_id = unquote(parts[3])
                    payload = list_reliability_work_order_fix_attempts_v1(work_order_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
                if len(parts) == 4:
                    work_order_id = unquote(parts[3])
                    payload = get_reliability_work_order_v1(work_order_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/fix-attempts/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4:
                    fix_attempt_id = unquote(parts[3])
                    payload = get_reliability_fix_attempt_v1(fix_attempt_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/verifications/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4:
                    verification_id = unquote(parts[3])
                    payload = get_reliability_verification_v1(verification_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path == "/api/reliability/readiness/latest":
                payload = get_latest_reliability_readiness_v1()
                self._send_json(HTTPStatus.OK, payload)
                return True
            if path.startswith("/api/reliability/readiness/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "readiness":
                    assessment_id = unquote(parts[3])
                    payload = get_reliability_readiness_v1(assessment_id)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
        except ValueError as exc:
            self._send_reliability_error(exc)
            return True
        return False

    def _route_reliability_post(self, path: str) -> bool:
        try:
            if path == "/api/reliability/observations":
                body = self._read_json_body()
                payload = create_reliability_observation_v1(body)
                self._send_json(HTTPStatus.CREATED, {"ok": True, "observation": payload})
                return True
            if path == "/api/reliability/issues":
                body = self._read_json_body()
                payload = create_reliability_issue_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
            if path == "/api/reliability/work-orders":
                body = self._read_json_body()
                payload = create_reliability_work_order_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
            if path == "/api/reliability/fix-attempts":
                body = self._read_json_body()
                payload = create_reliability_fix_attempt_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
            if path == "/api/reliability/verifications":
                body = self._read_json_body()
                payload = create_reliability_verification_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
            if path.startswith("/api/reliability/issues/"):
                parts = path.strip("/").split("/")
                if len(parts) == 5 and parts[4] == "link-observation":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = link_reliability_issue_observation_v1(issue_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
                if len(parts) == 5 and parts[4] == "work-orders":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = create_reliability_issue_work_order_v1(issue_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
                if len(parts) == 5 and parts[4] == "verifications":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = create_reliability_issue_verification_v1(issue_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
                if len(parts) == 5 and parts[4] == "create-work-order":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = create_reliability_work_order_from_issue_v1(issue_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
                if len(parts) == 5 and parts[4] == "verify":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = verify_reliability_issue_v1(issue_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
            if path.startswith("/api/reliability/work-orders/"):
                parts = path.strip("/").split("/")
                if len(parts) == 5 and parts[4] == "fix-attempts":
                    work_order_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = create_reliability_work_order_fix_attempt_v1(work_order_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
                if len(parts) == 5 and parts[4] == "record-fix-attempt":
                    work_order_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = record_reliability_fix_attempt_v1(work_order_id, body)
                    self._send_json(HTTPStatus.CREATED, payload)
                    return True
            if path == "/api/reliability/ai/draft-issue":
                body = self._read_json_body()
                payload = draft_reliability_issue_v1(body)
                status = HTTPStatus.CREATED if payload.get("created") else HTTPStatus.OK
                self._send_json(status, payload)
                return True
            if path == "/api/reliability/readiness/assess":
                body = self._read_json_body()
                payload = assess_reliability_readiness_v1(body)
                self._send_json(HTTPStatus.CREATED, payload)
                return True
        except ConfigurationWorkflowApiError as exc:
            self._send_json(
                exc.status_code,
                {
                    "ok": False,
                    "message": str(exc),
                    "reason_codes": exc.reason_codes,
                    "details": exc.details,
                },
            )
            return True
        except ValueError as exc:
            self._send_reliability_error(exc)
            return True
        return False

    def _route_reliability_patch(self, path: str) -> bool:
        try:
            if path.startswith("/api/reliability/issues/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "issues":
                    issue_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = update_reliability_issue_v1(issue_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/work-orders/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "work-orders":
                    work_order_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = update_reliability_work_order_v1(work_order_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/fix-attempts/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "fix-attempts":
                    fix_attempt_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = update_reliability_fix_attempt_v1(fix_attempt_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
            if path.startswith("/api/reliability/verifications/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[2] == "verifications":
                    verification_id = unquote(parts[3])
                    body = self._read_json_body()
                    payload = update_reliability_verification_v1(verification_id, body)
                    self._send_json(HTTPStatus.OK, payload)
                    return True
        except ConfigurationWorkflowApiError as exc:
            self._send_json(
                exc.status_code,
                {
                    "ok": False,
                    "message": str(exc),
                    "reason_codes": exc.reason_codes,
                    "details": exc.details,
                },
            )
            return True
        except ValueError as exc:
            self._send_reliability_error(exc)
            return True
        return False

    def _health_payload(self) -> Dict[str, Any]:
        host = ""
        port = 0
        try:
            host = str(self.server.server_address[0])
            port = int(self.server.server_address[1])
        except Exception:
            host = "127.0.0.1"
            port = 0

        checks: Dict[str, Any] = {
            "static_dir_exists": OpsHandler.STATIC_DIR.exists(),
            "truth_root_exists": TRUTH_ROOT.exists(),
            "truth_root_is_dir": TRUTH_ROOT.is_dir(),
            "runtime_root_exists": RUNTIME_ROOT.exists(),
            "runtime_root_is_dir": RUNTIME_ROOT.is_dir(),
        }
        status = "READY" if all(bool(v) for v in checks.values()) else "DEGRADED"
        payload: Dict[str, Any] = {
            "service": "ops_dashboard",
            "status": status,
            "timestamp_utc": _utc_now_iso(),
            "process_alive": True,
            "pid": os.getpid(),
            "host": host,
            "port": port,
            "started_at_utc": SERVICE_STARTED_AT_UTC,
            "checks": checks,
            "runtime_root": str(RUNTIME_ROOT),
        }
        version = _service_version()
        if isinstance(version, str) and version:
            payload["version"] = version
        return payload

    def _route_status_payload(self) -> Dict[str, Any]:
        routes = {
            "/aegis-runtime": "/aegis-runtime" in self.SHELL_ROUTES,
            "/aegis-opportunities": "/aegis-opportunities" in self.SHELL_ROUTES,
            "/aegis-candidates": "/aegis-candidates" in self.SHELL_ROUTES,
            "/aegis-theses": "/aegis-theses" in self.SHELL_ROUTES,
            "/aegis-edge-lab": "/aegis-edge-lab" in self.SHELL_ROUTES,
            "/aegis-performance": "/aegis-performance" in self.SHELL_ROUTES,
            "/aegis-journal": "/aegis-journal" in self.SHELL_ROUTES,
            "/aegis-today": "/aegis-today" in self.SHELL_ROUTES,
            "/aegis-review": "/aegis-review" in self.SHELL_ROUTES,
            "/aegis-research": "/aegis-research" in self.SHELL_ROUTES,
            "/aegis-history": "/aegis-history" in self.SHELL_ROUTES,
            "/aegis-runtime-truth": "/aegis-runtime-truth" in self.SHELL_ROUTES,
            "/aegis-operator-cockpit": "/aegis-operator-cockpit" in self.SHELL_ROUTES,
            "/aegis-adaptive-intelligence": "/aegis-adaptive-intelligence" in self.SHELL_ROUTES,
            "/aegis-intelligence-governance": "/aegis-intelligence-governance" in self.SHELL_ROUTES,
            "/api/runtime-status": True,
            "/api/decision-ledger": True,
            "/api/portfolio-state": True,
            "/api/portfolio-scoring": True,
            "/api/latest-packet": True,
            "/api/ui-service-authority": True,
            "/api/research-lab/health": True,
            "/api/research-lab/operator-home": True,
            "/api/research-lab/console": True,
            "/api/aegis/thesis-graph": True,
            "/api/aegis/commands/registry": True,
            "/api/aegis/commands/execute": True,
            "/aegis-repair-center": "/aegis-repair-center" in self.SHELL_ROUTES,
            "/api/research-lab/start-research/options": True,
            "/api/research-lab/start-research": True,
            "/api/research-lab/hypothesis-queue": True,
            "/api/research-lab/projection-health": True,
            "/api/research-lab/research-plans": True,
            "/api/research-lab/paper-trials": True,
            "/api/research-lab/sleeve-review-center": True,
            "/api/research-lab/evidence": True,
            "/api/research-lab/blocked-evidence": True,
            "/api/research-lab/blocked-work": True,
            "/api/research-lab/research-backlog": True,
            "/api/research-lab/sleeves": True,
            "/api/research-lab/evidence-inventory": True,
            "/api/research-lab/paper-trial-inventory": True,
            "/api/research-lab/sleeve-comparison": True,
            "/api/research-lab/research-backlog-priority": True,
            "/healthz": True,
            "/healthz/aegis-lite-ui": True,
            "/readyz": True,
            "/runtime-status": True,
        }
        return {
            "status": "PASS" if all(routes.values()) else "FAIL",
            "routes": routes,
        }

    def _readyz_payload(self) -> Dict[str, Any]:
        truth_root = _canonical_truth_root()
        runtime_truth = _runtime_truth_root()
        latest_packet = _latest_packet_path()
        route_status = self._route_status_payload()
        checks = {
            "truth_root_resolved": truth_root.exists() and truth_root.is_dir(),
            "latest_packet_available": latest_packet.exists() and latest_packet.is_file(),
            "runtime_truth_available": runtime_truth.exists() and runtime_truth.is_dir(),
            "projection_contracts_loaded": (REPO_ROOT / "constellation_2" / "phaseL" / "ui_api" / "projection_contracts.py").exists(),
            "routes_registered": route_status["status"] == "PASS",
        }
        if checks["truth_root_resolved"] and checks["runtime_truth_available"] and checks["projection_contracts_loaded"] and checks["routes_registered"]:
            status = "PASS" if checks["latest_packet_available"] else "DEGRADED"
        else:
            status = "FAIL"
        return {
            "ok": status == "PASS",
            "status": status,
            "generated_at_utc": _utc_now_iso(),
            "truth_root": str(truth_root),
            "runtime_truth_root": str(runtime_truth),
            "latest_packet_path": str(latest_packet),
            "projection_contract_version": PROJECTION_CONTRACT_VERSION,
            "checks": checks,
            "route_status": route_status,
        }

    def translate_path(self, path: str) -> str:
        u = urlparse(path)
        normalized = u.path.rstrip("/") or "/"
        if normalized.startswith("/reliability/work-orders/") and normalized != "/reliability/work-orders":
            rel = "index.html"
        elif normalized.startswith("/research-lab/"):
            rel = "index.html"
        elif normalized in self.SHELL_ROUTES:
            rel = "index.html"
        else:
            rel = u.path.lstrip("/")
        if rel == "":
            rel = "index.html"
        full = (self.STATIC_DIR / rel).resolve()
        if not str(full).startswith(str(self.STATIC_DIR)):
            return str(self.STATIC_DIR / "index.html")
        return str(full)

    def _route_api(self) -> bool:
        u = urlparse(self.path)
        path = u.path
        if not path.startswith("/api/"):
            return False

        qs = parse_qs(u.query)
        raw_day = (qs.get("day") or [None])[0]
        requested_day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else None

        if path == "/api/aegis/commands/registry":
            self._send_json(HTTPStatus.OK, aegis_command_registry_v1())
            return True

        if path == "/api/aegis/commands/validate":
            self._send_json(HTTPStatus.OK, validate_aegis_command_registry_v1())
            return True

        if path == "/api/research-lab/health":
            self._send_json(HTTPStatus.OK, research_lab_health_v1())
            return True

        if path == "/api/research-lab/console":
            self._send_json(HTTPStatus.OK, research_lab_console_v1())
            return True

        if path == "/api/research-lab/operator-home":
            self._send_json(HTTPStatus.OK, research_lab_operator_home_v1())
            return True

        if path == "/api/research-lab/start-research/options":
            self._send_json(HTTPStatus.OK, research_lab_start_research_options_v1())
            return True

        if path == "/api/research-lab/hypothesis-queue":
            rebuild = str((qs.get("rebuild") or [""])[0]).lower() == "true"
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_queue_v1(status=(qs.get("status") or [None])[0], rebuild=rebuild))
            return True

        if path == "/api/research-lab/projection-health":
            self._send_json(HTTPStatus.OK, research_lab_projection_health_v1())
            return True

        if path == "/api/research-lab/research-plans":
            self._send_json(HTTPStatus.OK, research_lab_research_plans_console_v1())
            return True

        if path == "/api/research-lab/paper-trials":
            self._send_json(HTTPStatus.OK, research_lab_paper_trials_console_v1())
            return True

        if path == "/api/research-lab/sleeve-review-center":
            self._send_json(HTTPStatus.OK, research_lab_sleeve_review_center_console_v1())
            return True

        if path == "/api/research-lab/evidence":
            self._send_json(HTTPStatus.OK, research_lab_evidence_console_v1())
            return True

        if path == "/api/research-lab/blocked-work":
            self._send_json(HTTPStatus.OK, research_lab_blocked_work_console_v1())
            return True

        if path == "/api/research-lab/blocked-evidence":
            self._send_json(HTTPStatus.OK, research_lab_blocked_evidence_console_v1())
            return True

        if path == "/api/research-lab/research-backlog":
            self._send_json(HTTPStatus.OK, research_lab_research_backlog_console_v1())
            return True

        if path == "/api/research-lab/sleeves":
            self._send_json(HTTPStatus.OK, research_lab_sleeves_v1())
            return True

        if path == "/api/research-lab/evidence-inventory":
            self._send_json(HTTPStatus.OK, research_lab_evidence_inventory_v1())
            return True

        if path == "/api/research-lab/paper-trial-inventory":
            self._send_json(HTTPStatus.OK, research_lab_paper_trial_inventory_v1())
            return True

        if path == "/api/research-lab/sleeve-comparison":
            self._send_json(HTTPStatus.OK, research_lab_sleeve_comparison_v1())
            return True

        if path == "/api/research-lab/research-backlog-priority":
            self._send_json(HTTPStatus.OK, research_lab_backlog_priority_v1())
            return True

        if path == "/api/research-lab/challenger-tracks":
            self._send_json(HTTPStatus.OK, research_lab_challenger_tracks_v1())
            return True

        if path == "/api/research-lab/challenger-variants":
            self._send_json(HTTPStatus.OK, research_lab_challenger_variants_v1())
            return True

        if path == "/api/research-lab/challenger-evidence-batches":
            self._send_json(HTTPStatus.OK, research_lab_challenger_evidence_batches_v1())
            return True

        if path == "/api/research-lab/challenger-evidence-completeness":
            batch_id = (qs.get("challenger_evidence_batch_id") or [None])[0]
            self._send_json(HTTPStatus.OK, research_lab_challenger_evidence_completeness_v1(challenger_evidence_batch_id=batch_id))
            return True

        if path == "/api/research-lab/challenger-blockers":
            batch_id = (qs.get("challenger_evidence_batch_id") or [None])[0]
            self._send_json(HTTPStatus.OK, research_lab_challenger_blockers_v1(challenger_evidence_batch_id=batch_id))
            return True

        if path == "/api/research-lab/challenger-comparison-reports":
            self._send_json(HTTPStatus.OK, research_lab_challenger_comparison_reports_v1())
            return True

        if path == "/api/research-lab/human-review-dossiers":
            self._send_json(HTTPStatus.OK, research_lab_human_review_dossiers_v1())
            return True

        if path == "/api/research-lab/human-review-decisions":
            self._send_json(HTTPStatus.OK, research_lab_human_review_decisions_v1())
            return True

        if path == "/api/research-lab/human-review-decisions/latest":
            dossier_id = (qs.get("human_review_dossier_id") or [None])[0]
            self._send_json(HTTPStatus.OK, research_lab_human_review_decision_latest_v1(human_review_dossier_id=dossier_id))
            return True

        if path == "/api/research-lab/human-review-decision-read-model":
            self._send_json(HTTPStatus.OK, research_lab_human_review_decision_read_model_v1())
            return True

        if path == "/api/research-lab/integrity-reports":
            self._send_json(HTTPStatus.OK, research_lab_integrity_reports_v1())
            return True

        if path == "/api/research-lab/integrity-reports/latest":
            self._send_json(HTTPStatus.OK, research_lab_integrity_report_latest_v1())
            return True

        if path == "/api/research-lab/integrity/latest-summary":
            self._send_json(HTTPStatus.OK, research_lab_integrity_latest_summary_v1())
            return True

        if path == "/api/research-lab/status" or path == "/api/research-lab/status/latest":
            self._send_json(HTTPStatus.OK, research_lab_status_latest_v1())
            return True

        if path == "/api/research-lab/status-reports":
            self._send_json(HTTPStatus.OK, research_lab_status_reports_v1())
            return True

        if path == "/api/research-lab/paper-trial-proposals":
            self._send_json(HTTPStatus.OK, research_lab_paper_trial_proposals_v1())
            return True

        if path == "/api/research-lab/paper-trial-proposals/latest":
            self._send_json(HTTPStatus.OK, research_lab_paper_trial_proposal_latest_v1())
            return True

        if path == "/api/research-lab/observation-candidates":
            self._send_json(
                HTTPStatus.OK,
                research_lab_observation_candidates_v1(
                    observation_type=(qs.get("observation_type") or [None])[0],
                    observation_family=(qs.get("observation_family") or [None])[0],
                    severity=(qs.get("severity") or [None])[0],
                    research_status=(qs.get("research_status") or [None])[0],
                ),
            )
            return True

        if path == "/api/research-lab/observation-candidates/latest":
            self._send_json(HTTPStatus.OK, research_lab_observation_candidate_latest_v1())
            return True

        if path == "/api/research-lab/observation-candidate-batches":
            self._send_json(HTTPStatus.OK, research_lab_observation_candidate_batches_v1())
            return True

        if path == "/api/research-lab/observation-candidate-batches/latest":
            self._send_json(HTTPStatus.OK, research_lab_observation_candidate_batch_latest_v1())
            return True

        if path == "/api/research-lab/observation-clusters":
            self._send_json(
                HTTPStatus.OK,
                research_lab_observation_clusters_v1(
                    cluster_family=(qs.get("cluster_family") or [None])[0],
                    cluster_status=(qs.get("cluster_status") or [None])[0],
                    research_status=(qs.get("research_status") or [None])[0],
                ),
            )
            return True

        if path == "/api/research-lab/observation-clusters/latest":
            self._send_json(HTTPStatus.OK, research_lab_observation_cluster_latest_v1())
            return True

        if path == "/api/research-lab/observation-cluster-batches":
            self._send_json(HTTPStatus.OK, research_lab_observation_cluster_batches_v1())
            return True

        if path == "/api/research-lab/observation-cluster-batches/latest":
            self._send_json(HTTPStatus.OK, research_lab_observation_cluster_batch_latest_v1())
            return True

        if path == "/api/research-lab/hypothesis-proposals":
            self._send_json(
                HTTPStatus.OK,
                research_lab_hypothesis_proposals_v1(
                    proposal_status=(qs.get("proposal_status") or [None])[0],
                    proposal_family=(qs.get("proposal_family") or [None])[0],
                ),
            )
            return True

        if path == "/api/research-lab/hypothesis-proposals/latest":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_latest_v1())
            return True

        if path == "/api/research-lab/research-intake/queue":
            self._send_json(HTTPStatus.OK, research_lab_research_intake_queue_v1(status=(qs.get("status") or [None])[0]))
            return True

        if path == "/api/research-lab/hypothesis-proposal-batches":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_batches_v1())
            return True

        if path == "/api/research-lab/hypothesis-proposal-batches/latest":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_batch_latest_v1())
            return True

        if path == "/api/research-lab/hypothesis-proposal-reviews":
            self._send_json(
                HTTPStatus.OK,
                research_lab_hypothesis_proposal_reviews_v1(
                    review_decision=(qs.get("review_decision") or [None])[0],
                    review_status=(qs.get("review_status") or [None])[0],
                ),
            )
            return True

        if path == "/api/research-lab/hypothesis-proposal-reviews/latest":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_review_latest_v1())
            return True

        if path == "/api/research-lab/hypothesis-proposal-review-batches":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_review_batches_v1())
            return True

        if path == "/api/research-lab/hypothesis-proposal-review-batches/latest":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_review_batch_latest_v1())
            return True

        if path == "/api/research-lab/hypothesis-intake-decisions":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_intake_decisions_v1())
            return True

        if path == "/api/research-lab/hypothesis-intake-decisions/latest":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_intake_decision_latest_v1())
            return True

        if path == "/api/research-lab/hypothesis-intake-batches":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_intake_batches_v1())
            return True

        if path == "/api/research-lab/hypothesis-intake-batches/latest":
            self._send_json(HTTPStatus.OK, research_lab_hypothesis_intake_batch_latest_v1())
            return True

        if path == "/api/research-lab/research-hypotheses":
            self._send_json(HTTPStatus.OK, research_lab_research_hypotheses_v1())
            return True

        if path == "/api/research-lab/research-hypotheses/latest":
            self._send_json(HTTPStatus.OK, research_lab_research_hypothesis_latest_v1())
            return True

        if path.startswith("/api/research-lab/observation-cluster-batches/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_observation_cluster_batch_v1(observation_cluster_batch_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/observation-clusters/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 5 and parts[4] == "hypothesis-proposals":
                self._send_json(HTTPStatus.OK, research_lab_observation_cluster_hypothesis_proposals_v1(observation_cluster_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/observation-clusters/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_observation_cluster_v1(observation_cluster_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/hypothesis-proposal-batches/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_batch_v1(hypothesis_proposal_batch_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/hypothesis-proposal-review-batches/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_review_batch_v1(hypothesis_proposal_review_batch_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/hypothesis-intake-batches/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_hypothesis_intake_batch_v1(hypothesis_intake_batch_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/hypothesis-proposals/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 5 and parts[4] == "reviews":
                self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_reviews_for_proposal_v1(hypothesis_proposal_id=parts[3]))
                return True
            if len(parts) == 5 and parts[4] == "dossier":
                self._send_json(HTTPStatus.OK, research_lab_console_dossier_v1(hypothesis_proposal_id=parts[3]))
                return True
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_v1(hypothesis_proposal_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/hypothesis-proposal-reviews/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_hypothesis_proposal_review_v1(hypothesis_proposal_review_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/hypothesis-intake-decisions/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_hypothesis_intake_decision_v1(hypothesis_intake_decision_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/research-hypotheses/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_research_hypothesis_v1(research_hypothesis_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/observation-candidate-batches/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_observation_candidate_batch_v1(observation_candidate_batch_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/observation-candidates/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_observation_candidate_v1(observation_candidate_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/paper-trial-proposals/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_paper_trial_proposal_v1(paper_trial_proposal_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/status-reports/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_status_report_v1(research_os_status_report_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/integrity-reports/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_integrity_report_v1(integrity_report_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/human-review-decisions/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 5 and parts[4] == "paper-trial-proposals":
                self._send_json(HTTPStatus.OK, research_lab_human_review_decision_paper_trial_proposals_v1(human_review_decision_id=parts[3]))
                return True
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_human_review_decision_v1(human_review_decision_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/human-review-dossiers/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 5 and parts[4] == "decisions":
                self._send_json(HTTPStatus.OK, research_lab_human_review_dossier_decisions_v1(human_review_dossier_id=parts[3]))
                return True
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_human_review_dossier_v1(human_review_dossier_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/challenger-comparison-reports/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_challenger_comparison_report_v1(challenger_comparison_report_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/challenger-evidence-batches/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_challenger_evidence_batch_v1(challenger_evidence_batch_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/challenger-evidence-lineage/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_challenger_evidence_lineage_v1(challenger_evidence_batch_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/challenger-variants/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_challenger_variant_v1(challenger_variant_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/challenger-tracks/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4:
                self._send_json(HTTPStatus.OK, research_lab_challenger_track_v1(challenger_track_id=parts[3]))
                return True

        if path == "/api/research-lab/stability/expectancy-drift/latest":
            self._send_json(HTTPStatus.OK, research_lab_expectancy_drift_latest_v1())
            return True

        if path == "/api/research-lab/stability/regime-fragility/latest":
            self._send_json(HTTPStatus.OK, research_lab_regime_fragility_latest_v1())
            return True

        if path == "/api/research-lab/stability/sleeve-stability/latest":
            self._send_json(HTTPStatus.OK, research_lab_sleeve_stability_latest_v1())
            return True

        if path.startswith("/api/research-lab/sleeves/"):
            parts = [unquote(part) for part in path.split("/") if part]
            # /api/research-lab/sleeves/{sleeve_id}
            # /api/research-lab/sleeves/{sleeve_id}/evidence-chain
            # /api/research-lab/sleeves/{sleeve_id}/edge-lab-projection
            if len(parts) >= 4:
                sleeve_id = parts[3]
                if len(parts) == 4:
                    self._send_json(HTTPStatus.OK, research_lab_sleeve_v1(sleeve_id=sleeve_id))
                    return True
                if len(parts) == 5 and parts[4] == "evidence-chain":
                    self._send_json(HTTPStatus.OK, research_lab_evidence_chain_v1(sleeve_id=sleeve_id))
                    return True
                if len(parts) == 5 and parts[4] == "edge-lab-projection":
                    self._send_json(HTTPStatus.OK, research_lab_edge_lab_projection_v1(sleeve_id=sleeve_id))
                    return True
                if len(parts) == 5 and parts[4] == "stability":
                    self._send_json(HTTPStatus.OK, research_lab_sleeve_stability_v1(sleeve_id=sleeve_id))
                    return True

        if path.startswith("/api/research-lab/evidence/") and path.endswith("/summary"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 5:
                self._send_json(HTTPStatus.OK, research_lab_evidence_summary_v1(evidence_package_id=parts[3]))
                return True

        if path.startswith("/api/research-lab/paper-trials/") and path.endswith("/summary"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 5:
                self._send_json(HTTPStatus.OK, research_lab_paper_trial_summary_v1(paper_trial_id=parts[3]))
                return True

        if path.startswith("/api/reliability/"):
            if self._route_reliability_get(path, qs):
                return True

        if path == "/api/shared/status-semantics":
            self._send_json(HTTPStatus.OK, {"ok": True, "status_semantics": STATUS_SEMANTICS})
            return True

        if path == "/api/runtime-status":
            self._send_json(HTTPStatus.OK, _runtime_status_projection(requested_day))
            return True

        if path == "/api/decision-ledger":
            self._send_json(
                HTTPStatus.OK,
                _artifact_projection_payload(
                    day_utc=_projection_day(requested_day),
                    family="decision_ledger_v1",
                    filename="decision_ledger.v1.json",
                    label="decision_ledger",
                ),
            )
            return True

        if path == "/api/portfolio-state":
            self._send_json(
                HTTPStatus.OK,
                _artifact_projection_payload(
                    day_utc=_projection_day(requested_day),
                    family="portfolio_state_v1",
                    filename="portfolio_state.v1.json",
                    label="portfolio_state",
                ),
            )
            return True

        if path == "/api/portfolio-scoring":
            self._send_json(
                HTTPStatus.OK,
                _artifact_projection_payload(
                    day_utc=_projection_day(requested_day),
                    family="portfolio_scoring_v1",
                    filename="portfolio_scoring.v1.json",
                    label="portfolio_scoring",
                ),
            )
            return True

        if path == "/api/latest-packet":
            self._send_json(HTTPStatus.OK, _latest_packet_projection())
            return True

        if path == "/api/ui-service-authority":
            day = _projection_day(requested_day)
            self._send_json(
                HTTPStatus.OK,
                _artifact_projection_payload(
                    day_utc=day,
                    family="ui_service_authority_v1",
                    filename="ui_service_authority.v1.json",
                    label="ui_service_authority",
                ),
            )
            return True

        if path == "/api/shell/status-rail":
            summary_only = (qs.get("summary") or [""])[0] in {"1", "true", "TRUE", "yes", "YES"}
            surface = (qs.get("surface") or [""])[0]
            if surface == "aegis":
                requested_status_day = (qs.get("day") or [requested_day])[0]
                self._send_json(HTTPStatus.OK, _aegis_kernel_status_rail_view(GLOBAL_TRUTH_ROOT, requested_status_day))
                return True
            self._send_json(
                HTTPStatus.OK,
                build_kernel_status_rail_summary_view() if summary_only else build_kernel_status_rail_view(),
            )
            return True

        if path == "/api/work-queue":
            self._send_json(HTTPStatus.OK, build_operator_work_queue_view())
            return True

        if path.startswith("/api/workspace/"):
            workspace_id = path.rsplit("/", 1)[-1]
            payload = build_workspace_view(workspace_id)
            status_code = HTTPStatus.OK if payload.get("ok") else HTTPStatus.NOT_FOUND
            self._send_json(status_code, payload)
            return True

        if path in {"/api/system/summary", "/api/product-summary"}:
            self._send_json(HTTPStatus.OK, build_system_summary_view(requested_day))
            return True

        if path == "/api/refinement":
            self._send_json(HTTPStatus.OK, build_refinement_state_view(requested_day))
            return True

        if path == "/api/readiness-kernel":
            self._send_json(HTTPStatus.OK, build_readiness_kernel_v1(requested_day))
            return True


        if path == "/api/aegis/data-remediation/latest":
            day = _projection_day(requested_day)
            self._send_json(HTTPStatus.OK, {"ok": True, **latest_remediation_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)})
            return True

        if path.startswith("/api/aegis/data-remediation/"):
            day = _projection_day(requested_day)
            remediation_attempt_id = unquote(path.rsplit("/", 1)[-1])
            self._send_json(HTTPStatus.OK, {"ok": True, **get_remediation_attempt_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, remediation_attempt_id=remediation_attempt_id)})
            return True

        if path == "/api/aegis/data-blockers":
            day = _projection_day(requested_day)
            self._send_json(HTTPStatus.OK, {"ok": True, **classify_data_blockers_v1(truth_root=GLOBAL_TRUTH_ROOT, repo_root=REPO_ROOT, day_utc=day)})
            return True

        if path == "/api/aegis/universe/canonical-authority/latest":
            day = _projection_day(requested_day)
            canonical_root = _canonical_truth_root()
            authority = latest_canonical_universe_authority_v1(truth_root=canonical_root, day_utc=day)
            self._send_json(HTTPStatus.OK, {"ok": True, "data": authority, "errors": [] if authority else [{"code": "CANONICAL_UNIVERSE_AUTHORITY_MISSING"}], "truth_root": str(canonical_root)})
            return True

        if path == "/api/aegis/universe/health/latest":
            day = _projection_day(requested_day)
            canonical_root = _canonical_truth_root()
            self._send_json(HTTPStatus.OK, {"ok": True, "data": canonical_universe_health_v1(truth_root=canonical_root, day_utc=day), "errors": [], "truth_root": str(canonical_root)})
            return True

        if path == "/api/aegis/operator-projection":
            self._send_json(HTTPStatus.OK, _operator_projection_payload(requested_day))
            return True

        if path == "/api/aegis/repair-center":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or requested_day or _projection_day_for_report(requested_day, "operator_state_snapshot_v1"))
            self._send_json(HTTPStatus.OK, {"ok": True, "data": build_repair_center_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)})
            return True

        if path == "/api/system/actions":
            self._send_json(HTTPStatus.OK, {"ok": True, **build_action_inventory()})
            return True

        if path == "/api/system/action-audit":
            self._send_json(HTTPStatus.OK, {"ok": True, "audit_entries": list_action_audit_entries()})
            return True

        if path == "/api/operations":
            self._send_json(HTTPStatus.OK, build_operations_view(requested_day))
            return True

        if path == "/api/aegis/operator-state":
            self._send_json(HTTPStatus.OK, get_operator_state(GLOBAL_TRUTH_ROOT))
            return True

        if path == "/api/aegis/lite-execution-queue":
            self._send_json(HTTPStatus.OK, build_aegis_lite_execution_queue_view(requested_day, GLOBAL_TRUTH_ROOT))
            return True

        if path == "/api/aegis/event-monitoring":
            self._send_json(HTTPStatus.OK, build_aegis_event_monitoring_view(requested_day, GLOBAL_TRUTH_ROOT))
            return True

        if path == "/api/aegis/runtime-truth":
            day = _projection_day(requested_day)
            kernel_payload = build_runtime_truth_kernel_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            self._send_json(
                HTTPStatus.OK,
                {
                    **kernel_payload,
                    "runtime_state_history": read_runtime_state_history_summary_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                    "intelligence_summaries": intelligence_summaries_v1(GLOBAL_TRUTH_ROOT, day),
                },
            )
            return True


        if path in {"/api/aegis/operator/current-truth", "/api/aegis/operator/current-truth/latest"}:
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            self._send_json(HTTPStatus.OK, current_operator_truth_api_envelope_v1(current_truth))
            return True

        if path == "/api/aegis/operator/state-snapshot/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "operator_state_snapshot_v1"))
            response = load_or_build_operator_state_snapshot_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            if isinstance(response.get("data"), dict):
                response = {**response, "data": _merge_latest_domain_command_results_v1(response["data"], day)}
            self._send_json(HTTPStatus.OK, response)
            return True

        if path in {"/api/aegis/thesis-graph", "/api/aegis/market-theses"}:
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or requested_day or _projection_day_for_report(requested_day, "aegis_thesis_graph_projection_v1"))
            self._send_json(HTTPStatus.OK, load_or_build_thesis_graph_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day))
            return True

        if path == "/api/aegis/operator/paper-trade-construction/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "paper_trade_construction_v1"))
            construction = latest_paper_trade_construction_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            self._send_json(HTTPStatus.OK, operator_state_api_envelope_v1(paper_trade_construction_view_v1(construction), [], next_action=paper_trade_construction_view_v1(construction).get("next_required_action", "Review paper trade construction.")))
            return True

        if path.startswith("/api/aegis/operator/paper-trade-construction/"):
            construction_id = unquote(path.rsplit("/", 1)[-1])
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "paper_trade_construction_v1"))
            construction = latest_paper_trade_construction_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            status = HTTPStatus.OK if not construction_id or str(construction.get("construction_id") or "") == construction_id else HTTPStatus.OK
            payload = paper_trade_construction_view_v1(construction) if str(construction.get("construction_id") or "") == construction_id else {**paper_trade_construction_view_v1(construction), "requested_construction_id": construction_id, "lookup_status": "DEGRADED_ID_NOT_CURRENT"}
            self._send_json(status, operator_state_api_envelope_v1(payload, [], next_action=payload.get("next_required_action", "Review paper trade construction.")))
            return True

        if path == "/api/aegis/trade-cases/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "trade_lifecycle_case_v1"))
            case = latest_trade_lifecycle_case_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            self._send_json(HTTPStatus.OK, operator_state_api_envelope_v1(case, [], next_action=case.get("state_reason", "Review trade lifecycle case.")))
            return True

        if path.startswith("/api/aegis/trade-cases/"):
            case_id = unquote(path.rsplit("/", 1)[-1])
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "trade_lifecycle_case_v1"))
            case = latest_trade_lifecycle_case_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload = case if str(case.get("trade_lifecycle_case_id") or "") == case_id else {**case, "requested_trade_lifecycle_case_id": case_id, "lookup_status": "DEGRADED_ID_NOT_CURRENT"}
            self._send_json(HTTPStatus.OK, operator_state_api_envelope_v1(payload, [], next_action=payload.get("state_reason", "Review trade lifecycle case.")))
            return True

        if path == "/api/aegis/trade-case-projection/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "trade_lifecycle_case_v1"))
            case = latest_trade_lifecycle_case_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            projection = trade_case_projection_v1(case)
            self._send_json(HTTPStatus.OK, operator_state_api_envelope_v1(projection, [], next_action=projection.get("next_required_action", "Review trade case projection.")))
            return True

        if path == "/api/aegis/trade-readiness/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "readiness_domain_evaluation_v1"))
            case = latest_trade_lifecycle_case_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            evaluations = case.get("readiness_domain_evaluations") if isinstance(case.get("readiness_domain_evaluations"), list) else latest_readiness_domain_evaluations_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload = {"schema_id": "trade_readiness_domain_set", "schema_version": "v1", "trade_lifecycle_case_id": str(case.get("trade_lifecycle_case_id") or ""), "selected_exposure_intent_id": str(case.get("selected_exposure_intent_id") or ""), "source_day": day, "domain_statuses": domain_status_map_v1(evaluations), "blockers_by_domain": blockers_by_domain_v1(evaluations), "readiness_domain_evaluations": evaluations, "broker_execution_allowed": False, "live_trading_allowed": False, "order_routing_allowed": False, "capital_allocation_allowed": False}
            self._send_json(HTTPStatus.OK, operator_state_api_envelope_v1(payload, [], next_action="Review readiness domain evaluations."))
            return True

        if path.startswith("/api/aegis/trade-readiness/"):
            case_id = unquote(path.rsplit("/", 1)[-1])
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "readiness_domain_evaluation_v1"))
            case = latest_trade_lifecycle_case_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            evaluations = case.get("readiness_domain_evaluations") if isinstance(case.get("readiness_domain_evaluations"), list) else latest_readiness_domain_evaluations_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload = {"schema_id": "trade_readiness_domain_set", "schema_version": "v1", "requested_trade_lifecycle_case_id": case_id, "lookup_status": "CURRENT" if str(case.get("trade_lifecycle_case_id") or "") == case_id else "DEGRADED_ID_NOT_CURRENT", "trade_lifecycle_case_id": str(case.get("trade_lifecycle_case_id") or ""), "selected_exposure_intent_id": str(case.get("selected_exposure_intent_id") or ""), "source_day": day, "domain_statuses": domain_status_map_v1(evaluations), "blockers_by_domain": blockers_by_domain_v1(evaluations), "readiness_domain_evaluations": evaluations, "broker_execution_allowed": False, "live_trading_allowed": False, "order_routing_allowed": False, "capital_allocation_allowed": False}
            self._send_json(HTTPStatus.OK, operator_state_api_envelope_v1(payload, [], next_action="Review readiness domain evaluations."))
            return True

        if path == "/api/aegis/trade-ticket/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "trade_ticket_projection_v1"))
            projection = latest_trade_ticket_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            if not projection:
                case = latest_trade_lifecycle_case_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                projection = trade_ticket_projection_v1(case)
            self._send_json(HTTPStatus.OK, operator_state_api_envelope_v1(projection, [], next_action=projection.get("next_action", "Review trade ticket projection.")))
            return True

        if path == "/api/aegis/operator/manual-capture/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "operator_state_snapshot_v1"))
            response = load_or_build_operator_state_snapshot_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            snapshot = response.get("data") if isinstance(response.get("data"), dict) else {}
            manual = snapshot.get("trade_ticket_projection_v1") if isinstance(snapshot.get("trade_ticket_projection_v1"), dict) else {}
            if not manual:
                case = snapshot.get("trade_lifecycle_case_v1") if isinstance(snapshot.get("trade_lifecycle_case_v1"), dict) else {}
                manual = trade_ticket_projection_v1(case) if case else {}
            self._send_json(HTTPStatus.OK, operator_state_api_envelope_v1(manual, response.get("errors") if isinstance(response.get("errors"), list) else [], next_action=response.get("next_action", "Review trade lifecycle case.")))
            return True

        if path == "/api/aegis/operator/suppressed-watchlist/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "operator_state_snapshot_v1"))
            response = load_or_build_operator_state_snapshot_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            snapshot = response.get("data") if isinstance(response.get("data"), dict) else {}
            watchlist = snapshot.get("suppressed_candidate_watchlist") if isinstance(snapshot.get("suppressed_candidate_watchlist"), dict) else {}
            self._send_json(HTTPStatus.OK, operator_state_api_envelope_v1(watchlist, response.get("errors") if isinstance(response.get("errors"), list) else [], next_action=response.get("next_action", "Review suppressed watchlist.")))
            return True

        if path in {"/api/aegis/operator/manual-capture-records", "/api/aegis/operator/manual-capture-records/latest"}:
            qs = parse_qs(u.query)
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "manual_capture_record_v1"))
            selected_id = str((qs.get("selected_exposure_intent_id") or [""])[0] or "")
            payload = (
                latest_manual_capture_record_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, selected_exposure_intent_id=selected_id)
                if path.endswith("/latest")
                else list_manual_capture_records_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, selected_exposure_intent_id=selected_id)
            )
            self._send_json(HTTPStatus.OK, {
                **payload,
                "review_only": True,
                "broker_execution_allowed": False,
                "order_routing_allowed": False,
                "live_trading_allowed": False,
                "allocation_allowed": False,
                "paper_submit_created": False,
                "autonomous_execution_allowed": False,
            })
            return True

        if path == "/api/aegis/operator-cockpit":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
            self._send_json(HTTPStatus.OK, _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day))
            return True

        if path == "/api/aegis/operator/today":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
            cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
            self._send_json(HTTPStatus.OK, {"ok": True, **cockpit.get("operator_today_projection", {})})
            return True

        if path == "/api/aegis/operator/tasks":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
            cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
            self._send_json(HTTPStatus.OK, {"ok": True, **cockpit.get("operator_task_projection", {})})
            return True

        if path == "/api/aegis/operator/diagnostics":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
            cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
            self._send_json(HTTPStatus.OK, {"ok": True, **cockpit.get("system_diagnostic_projection", {})})
            return True

        if path == "/api/aegis/operator/what-changed":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
            cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
            self._send_json(HTTPStatus.OK, {"ok": True, **cockpit.get("what_changed_projection", {})})
            return True

        if path in {"/api/aegis/operator/health", "/api/aegis/operator/passive-health"}:
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
            cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
            self._send_json(HTTPStatus.OK, {"ok": True, **cockpit.get("passive_health_projection", {})})
            return True

        if path == "/api/aegis/opportunities":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
            response = load_or_build_operator_state_snapshot_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            snapshot = response.get("data") if isinstance(response.get("data"), dict) else {}
            active = snapshot.get("active_opportunity_projection") if isinstance(snapshot.get("active_opportunity_projection"), dict) else {}
            self._send_json(HTTPStatus.OK, {"ok": True, "current_truth_status": snapshot.get("current_truth_status"), **active})
            return True

        if path == "/api/aegis/review-ledger":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
            cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
            self._send_json(HTTPStatus.OK, {"ok": True, **cockpit.get("review_ledger_projection", {})})
            return True

        if path == "/api/aegis/eod-outcomes/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
            cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
            latest = cockpit.get("eod_opportunity_outcome_report", {})
            if not latest:
                latest = read_latest_eod_opportunity_outcome_report_v1(truth_root=GLOBAL_TRUTH_ROOT) or {}
            self._send_json(HTTPStatus.OK, {"ok": True, **(latest or {})})
            return True

        if path.startswith("/api/aegis/eod-outcomes/by-session/"):
            trading_session = unquote(path.rsplit("/", 1)[-1])
            report = read_eod_opportunity_outcome_report_v1(truth_root=GLOBAL_TRUTH_ROOT, trading_session=trading_session)
            if report is None:
                cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, trading_session)
                report = cockpit.get("eod_opportunity_outcome_report", {})
            self._send_json(HTTPStatus.OK, {"ok": True, **(report or {})})
            return True

        if path.startswith("/api/aegis/eod-outcomes/sleeves/") and path.endswith("/history"):
            parts = path.strip("/").split("/")
            sleeve_id = unquote(parts[-2]) if len(parts) >= 2 else ""
            self._send_json(HTTPStatus.OK, {"ok": True, **eod_sleeve_history_v1(truth_root=GLOBAL_TRUTH_ROOT, sleeve_id=sleeve_id)})
            return True

        if path.startswith("/api/aegis/eod-outcomes/"):
            report_id = unquote(path.rsplit("/", 1)[-1])
            report = read_eod_opportunity_outcome_report_by_id_v1(truth_root=GLOBAL_TRUTH_ROOT, eod_outcome_report_id=report_id)
            if report is None:
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "EOD_OUTCOME_REPORT_NOT_FOUND", "eod_outcome_report_id": report_id})
                return True
            self._send_json(HTTPStatus.OK, {"ok": True, **report})
            return True

        if path == "/api/aegis/operator/command-registry":
            self._send_json(HTTPStatus.OK, {"ok": True, **operator_command_registry_v1()})
            return True

        if path.startswith("/api/aegis/candidates/") and path.endswith("/decision-support"):
            parts = path.strip("/").split("/")
            if len(parts) == 5 and parts[0] == "api" and parts[1] == "aegis" and parts[2] == "candidates" and parts[4] == "decision-support":
                candidate_id = unquote(parts[3])
                day = _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1")
                cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
                candidate = find_candidate_for_decision_support_v1(cockpit, candidate_id)
                if not candidate:
                    self._send_json(
                        HTTPStatus.NOT_FOUND,
                        {
                            "ok": False,
                            "error": "CANDIDATE_NOT_FOUND",
                            "candidate_id": candidate_id,
                            "read_only": True,
                            "broker_execution_allowed": False,
                            "autonomous_execution_allowed": False,
                            "order_routing_allowed": False,
                        },
                    )
                    return True
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "read_only": True,
                        "brief": build_candidate_decision_support_brief_v1(candidate, cockpit),
                    },
                )
                return True

        if path.startswith("/api/aegis/candidates/") and path.endswith("/decision"):
            parts = path.strip("/").split("/")
            if len(parts) == 5 and parts[0] == "api" and parts[1] == "aegis" and parts[2] == "candidates" and parts[4] == "decision":
                candidate_id = unquote(parts[3])
                day = _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1")
                cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
                for row in cockpit.get("active_opportunity_projection", {}).get("candidates", []):
                    if isinstance(row, dict) and str(row.get("candidate_id") or row.get("id") or "") == candidate_id:
                        self._send_json(HTTPStatus.OK, {"ok": True, "read_only": True, **row.get("candidate_decision_projection", {})})
                        return True
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "CANDIDATE_NOT_FOUND", "candidate_id": candidate_id, "read_only": True})
                return True

        if path == "/api/aegis/candidate-review-ledger":
            raw_filter = (qs.get("filter") or ["all"])[0]
            day = _projection_day_for_report(requested_day, "aegis_candidate_review_ledger_v1")
            ledger_path, ledger = latest_json_v1(
                GLOBAL_TRUTH_ROOT,
                "aegis_candidate_review_ledger_v1",
                day,
                "candidate_review_ledger.v1.json",
            )
            if not ledger:
                ledger = build_candidate_review_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, filter_name=str(raw_filter or "all"))
            else:
                ledger = filter_candidate_review_ledger_v1(ledger, str(raw_filter or "all"))
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "status": "AVAILABLE" if ledger else "MISSING",
                    "ledger_path": str(ledger_path or ""),
                    **ledger,
                    "read_only": True,
                    "broker_execution_allowed": False,
                    "autonomous_execution_allowed": False,
                    "automatic_approval_allowed": False,
                },
            )
            return True

        if path == "/api/aegis/journal-timeline":
            day = _projection_day_for_report(requested_day, "aegis_journal_timeline_v1")
            timeline_path, timeline = latest_json_v1(
                GLOBAL_TRUTH_ROOT,
                "aegis_journal_timeline_v1",
                day,
                "journal_timeline.v1.json",
            )
            timeline_payload = timeline if isinstance(timeline, dict) else {}
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": bool(timeline_payload),
                    "status": "AVAILABLE" if timeline_payload else "MISSING",
                    "day_utc": str(timeline_payload.get("day_utc") or day),
                    "timeline_path": str(timeline_path or ""),
                    "timeline_summary": timeline_payload.get("timeline_summary") if isinstance(timeline_payload.get("timeline_summary"), dict) else {
                        "total_events": 0,
                        "latest_event_at": None,
                        "candidate_events": 0,
                        "edge_events": 0,
                        "sleeve_events": 0,
                        "performance_events": 0,
                        "runtime_events": 0,
                        "governance_events": 0,
                        "system_events": 0,
                    },
                    "recent_events": timeline_payload.get("recent_events") if isinstance(timeline_payload.get("recent_events"), list) else [],
                    "filters": timeline_payload.get("filters") if isinstance(timeline_payload.get("filters"), list) else ["All", "Candidates", "Edges", "Sleeves", "Performance", "Runtime", "Governance", "System"],
                    "entity_index": timeline_payload.get("entity_index") if isinstance(timeline_payload.get("entity_index"), dict) else {"candidates": [], "hypotheses": [], "sleeves": [], "regimes": []},
                    "audit_drilldowns": timeline_payload.get("audit_drilldowns") if isinstance(timeline_payload.get("audit_drilldowns"), list) else [],
                    "diagnostics": timeline_payload.get("diagnostics") if isinstance(timeline_payload.get("diagnostics"), list) else [],
                    "broker_execution_allowed": False,
                    "autonomous_execution_allowed": False,
                    "read_only": True,
                },
            )
            return True

        if path == "/api/aegis/adaptive-intelligence":
            day = _projection_day(requested_day)
            adaptive_payloads = {}
            for key, spec in {
                "regime_detection": ("regime_detection_v1", "regime_detection.v1.json"),
                "capital_allocation_intelligence": ("capital_allocation_intelligence_v1", "capital_allocation_intelligence.v1.json"),
                "failure_analysis": ("failure_analysis_v1", "failure_analysis.v1.json"),
                "research_memory_graph": ("research_memory_graph_v1", "research_memory_graph.v1.json"),
                "cross_sleeve_interaction": ("cross_sleeve_interaction_v1", "cross_sleeve_interaction.v1.json"),
                "research_queue_optimizer": ("research_queue_optimizer_v1", "research_queue_optimizer.v1.json"),
                "event_interpretation": ("event_interpretation_v1", "event_interpretation.v1.json"),
                "adaptive_governance": ("adaptive_governance_v1", "adaptive_governance.v1.json"),
                "regime_context": ("regime_context_v1", "regime_context.v1.json"),
                "sleeve_performance_analytics": ("sleeve_performance_analytics_v1", "sleeve_performance_analytics.v1.json"),
                "cross_sleeve_analysis": ("cross_sleeve_analysis_v1", "cross_sleeve_analysis.v1.json"),
            }.items():
                _payload_path, adaptive_payloads[key] = latest_json_v1(GLOBAL_TRUTH_ROOT, spec[0], day, spec[1])
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "day_utc": day,
                    "summaries": intelligence_summaries_v1(GLOBAL_TRUTH_ROOT, day),
                    "adaptive_payloads": adaptive_payloads,
                    "execution_allowed": False,
                    "broker_submit_transmit_allowed": False,
                    "autonomous_execution_allowed": False,
                    "read_only": True,
                },
            )
            return True

        if path == "/api/aegis/intelligence-governance":
            day = _projection_day(requested_day)
            kernel_path, kernel_payload = latest_json_v1(
                GLOBAL_TRUTH_ROOT,
                "aegis_intelligence_governance_kernel_v1",
                day,
                "intelligence_governance_kernel.v1.json",
            )
            recommendations_path, recommendations_payload = latest_json_v1(
                GLOBAL_TRUTH_ROOT,
                "aegis_intelligence_governance_kernel_v1",
                day,
                "intelligence_recommendations.v1.json",
            )
            ledger_path, ledger_payload = latest_json_v1(
                GLOBAL_TRUTH_ROOT,
                "aegis_intelligence_governance_kernel_v1",
                day,
                "intelligence_approval_ledger.v1.json",
            )
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "day_utc": day,
                    "kernel_path": str(kernel_path or ""),
                    "recommendations_path": str(recommendations_path or ""),
                    "approval_ledger_snapshot_path": str(ledger_path or ""),
                    "kernel": kernel_payload,
                    "recommendations": recommendations_payload.get("recommendations") or [],
                    "approval_ledger": ledger_payload,
                    "read_only": True,
                    "runtime_truth_mutation_allowed": False,
                    "broker_execution_allowed": False,
                    "autonomous_execution_allowed": False,
                },
            )
            return True

        if path == "/api/command/overview":
            self._send_json(HTTPStatus.OK, build_command_overview_view(requested_day))
            return True

        if path == "/api/operator-workflow":
            self._send_json(
                HTTPStatus.OK,
                build_operator_workflow_summary(
                    build_operations_view(requested_day),
                    build_alerts_view(requested_day),
                    build_reconciliation_view(requested_day),
                    build_positions_view(requested_day),
                    build_orders_view(requested_day),
                    build_action_inventory(),
                ),
            )
            return True

        if path == "/api/advisory":
            summary_only = (qs.get("summary") or [""])[0] in {"1", "true", "TRUE", "yes", "YES"}
            self._send_json(HTTPStatus.OK, build_advisory_view(requested_day, include_evidence=not summary_only))
            return True

        if path == "/api/policy-evolution":
            self._send_json(HTTPStatus.OK, build_policy_evolution_view(requested_day))
            return True

        if path == "/api/opportunities":
            self._send_json(HTTPStatus.OK, build_opportunity_state_view(requested_day))
            return True

        if path in {"/api/outcomes", "/api/value"}:
            self._send_json(HTTPStatus.OK, build_value_state_view(requested_day))
            return True

        if path == "/api/financial-state":
            self._send_json(HTTPStatus.OK, build_financial_state_view(requested_day))
            return True

        if path == "/api/capital":
            self._send_json(HTTPStatus.OK, build_capital_query_surface_v1())
            return True

        if path == "/api/capital/overview":
            self._send_json(HTTPStatus.OK, build_capital_overview_view())
            return True

        if path == "/api/capital/accounts":
            self._send_json(HTTPStatus.OK, build_capital_accounts_view())
            return True

        if path == "/api/capital/allocation":
            self._send_json(HTTPStatus.OK, build_capital_allocation_view())
            return True

        if path == "/api/capital/history":
            self._send_json(HTTPStatus.OK, build_capital_history_view())
            return True

        if path == "/api/capital/flows":
            self._send_json(HTTPStatus.OK, build_capital_flows_view())
            return True

        if path == "/api/capital/cashflow":
            effective = resolve_effective_capital_cashflow_inputs_v1()
            effective_values = dict(effective.get("values") or {})
            raw_scenario = (qs.get("scenario") or [effective_values.get("scenario") or "florida"])[0]
            scenario = str(raw_scenario or "florida").strip().lower()
            if not scenario:
                scenario = "florida"
            raw_include = (qs.get("include_inheritance") or [effective_values.get("include_inheritance")])[0]
            if isinstance(raw_include, bool):
                include_inheritance = raw_include
            else:
                include_inheritance = str(raw_include).strip().lower() in {"1", "true", "yes", "on"}
            raw_horizon = (qs.get("horizon_months") or [effective_values.get("horizon_months") or "24"])[0]
            horizon_months = int(effective_values.get("horizon_months") or 24)
            try:
                horizon_months = int(raw_horizon)
            except Exception:
                horizon_months = int(effective_values.get("horizon_months") or 24)
            raw_start_month = (qs.get("start_month") or [effective_values.get("start_month")])[0]
            start_month = None if raw_start_month is None else str(raw_start_month)
            self._send_json(
                HTTPStatus.OK,
                build_capital_cashflow_view(
                    scenario=scenario,
                    include_inheritance=include_inheritance,
                    horizon_months=horizon_months,
                    start_month=start_month,
                ),
            )
            return True

        if path == "/api/capital/validation":
            self._send_json(HTTPStatus.OK, build_capital_validation_view())
            return True

        if path == "/api/configuration/catalog":
            self._send_json(HTTPStatus.OK, build_configuration_catalog_v1())
            return True

        if path == "/api/configuration/current":
            self._send_json(HTTPStatus.OK, build_configuration_current_v1())
            return True

        if path.startswith("/api/configuration/drafts/"):
            parts = path.strip("/").split("/")
            if len(parts) == 4 and parts[0] == "api" and parts[1] == "configuration" and parts[2] == "drafts":
                draft_id = parts[3]
                try:
                    payload = get_configuration_draft_v1(draft_id)
                except ConfigurationWorkflowApiError as exc:
                    self._send_json(
                        exc.status_code,
                        {
                            "ok": False,
                            "message": str(exc),
                            "reason_codes": exc.reason_codes,
                            "details": exc.details,
                        },
                    )
                    return True
                self._send_json(HTTPStatus.OK, payload)
                return True

        if path == "/api/sleeves":
            self._send_json(HTTPStatus.OK, build_sleeve_evaluation_view(requested_day))
            return True

        if path == "/api/tax":
            self._send_json(HTTPStatus.OK, build_tax_state_view(requested_day))
            return True

        if path == "/api/orders":
            self._send_json(HTTPStatus.OK, build_orders_view(requested_day))
            return True

        if path == "/api/positions":
            self._send_json(HTTPStatus.OK, build_positions_view(requested_day))
            return True

        if path == "/api/reconciliation":
            self._send_json(HTTPStatus.OK, build_reconciliation_view(requested_day))
            return True

        if path == "/api/integrity":
            self._send_json(HTTPStatus.OK, build_integrity_view(requested_day))
            return True

        if path == "/api/alerts":
            self._send_json(HTTPStatus.OK, build_alerts_view(requested_day))
            return True

        if path == "/api/days":
            self._send_json(HTTPStatus.OK, _days_list())
            return True

        if path == "/api/latest_day":
            days = _union_days()
            self._send_json(HTTPStatus.OK, {"ok": True, "errors": [], "day_utc": _select_latest_day(days)})
            return True

        if path == "/api/activity/latest":
            day = _activity_latest_day()
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": True, "errors": [E_ACTIVITY_DAY_NOT_RESOLVED], "day_utc": None})
            else:
                self._send_json(HTTPStatus.OK, {"ok": True, "errors": [], "day_utc": day})
            return True

        if path == "/api/activity/today":
            day = None
            raw = (qs.get("day") or [None])[0]
            if isinstance(raw, str) and raw and _is_day_str(raw):
                day = raw
            if day is None:
                day = _activity_latest_day()
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": [E_ACTIVITY_DAY_NOT_RESOLVED], "path": path})
                return True
            self._send_json(HTTPStatus.OK, _activity_today(day))
            return True

        if path == "/api/activity/rollup":
            raw = (qs.get("asof") or [None])[0]
            asof = raw if isinstance(raw, str) and raw and _is_day_str(raw) else _activity_latest_day()
            if not asof:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": [E_ACTIVITY_DAY_NOT_RESOLVED], "path": path})
                return True
            self._send_json(HTTPStatus.OK, _activity_today(asof))
            return True

        if path == "/api/artifact":
            try:
                raw = (qs.get("path") or [None])[0]
                if not isinstance(raw, str) or not raw:
                    self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["MISSING_QUERY_PATH"], "path": None, "content": ""})
                    return True

                day_raw = (qs.get("day") or [None])[0]
                day = day_raw if isinstance(day_raw, str) and day_raw and _is_day_str(day_raw) else None
                selected_root = _truth_root_for_day(day)
                allowed_roots = _known_truth_roots() or [selected_root]
                for extra_root in [GLOBAL_TRUTH_ROOT, ADVISORY_RUNTIME_ROOT]:
                    if isinstance(extra_root, Path) and extra_root.exists() and extra_root.is_dir():
                        allowed_roots.append(extra_root.resolve())

                p = Path(raw)
                if not p.is_absolute():
                    p = (selected_root / raw).resolve()
                else:
                    p = p.resolve()

                allowed = False
                for root in allowed_roots:
                    root_s = str(root.resolve())
                    if str(p).startswith(root_s + "/") or str(p) == root_s:
                        allowed = True
                        break
                if not allowed:
                    self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["PATH_OUTSIDE_TRUTH_ROOT"], "path": str(p), "content": ""})
                    return True

                if not p.exists() or not p.is_file():
                    self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["ARTIFACT_NOT_FOUND"], "path": str(p), "content": ""})
                    return True

                data = p.read_text(encoding="utf-8", errors="replace")
                truncated = False
                if len(data) > 20000:
                    data = data[:20000] + "\n\n...TRUNCATED...\n"
                    truncated = True

                self._send_json(HTTPStatus.OK, {"ok": True, "errors": [], "path": str(p), "content": data, "truncated": truncated})
                return True
            except Exception:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["ARTIFACT_READ_FAILED"], "path": None, "content": ""})
                return True

        if path == "/api/status":
            from constellation_2.phaseL.ui.server.c3_ui_status_collector_v1 import build_c3_ui_status

            self._send_json(HTTPStatus.OK, build_c3_ui_status(TRUTH_ROOT))
            return True

        if path == "/api/operational_truth":
            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"]})
                return True
            selected_root = _truth_root_for_day(day)
            payload = build_operational_truth_v1(selected_root, day)
            payload["truth_root"] = str(selected_root)
            self._send_json(HTTPStatus.OK, payload)
            return True

        if path == "/api/attempts":
            from constellation_2.phaseL.ui.server.c2_ops_cockpit_status_v2_collector_v1 import discover_attempts, select_preferred_attempt

            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"], "attempts": []})
                return True
            selected_root = _truth_root_for_day(day)
            attempts, missing, source_paths, source_mtimes, warnings = discover_attempts(selected_root, day)
            preferred_attempt = select_preferred_attempt(selected_root, day, attempts)
            self._send_json(HTTPStatus.OK, {
                "ok": True,
                "generated_utc": _utc_now_iso(),
                "day_utc": day,
                "attempts": attempts,
                "recommended_attempt_id": preferred_attempt,
                "truth_root": str(selected_root),
                "warnings": warnings,
                "missing_paths": missing,
                "source_paths": source_paths,
                "source_mtimes": source_mtimes,
            })
            return True

        if path == "/api/status_v2":
            from constellation_2.phaseL.ui.server.c2_ops_cockpit_status_v2_collector_v1 import build_status_v2
            from constellation_2.phaseL.ui.server.c3_ui_status_collector_v1 import build_c3_ui_status

            # Consolidated payload for Ops Cockpit UI V2.
            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"]})
                return True
            selected_root = _truth_root_for_day(day)
            raw_attempt = (qs.get("attempt_id") or [None])[0]
            attempt_id = raw_attempt if isinstance(raw_attempt, str) and raw_attempt else None

            # C3 status used only as an informational truth-derived surface for some gate tiles.
            c3 = build_c3_ui_status(selected_root)
            inst = _instance_config_path()

            payload = build_status_v2(selected_root, inst, day, attempt_id, c3)
            if isinstance(payload.get("meta"), dict):
                payload["meta"]["truth_root"] = str(selected_root)
            payload["ok"] = True
            payload["errors"] = []
            self._send_json(HTTPStatus.OK, payload)
            return True

        if path == "/api/operator/home":
            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"]})
                return True
            selected_root = _truth_root_for_day(day)
            bundle = build_operator_home_bundle(repo_root=REPO_ROOT, truth_root=selected_root, day_utc=day)
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "generated_utc": _utc_now_iso(),
                    "day_utc": day,
                    "truth_root": str(selected_root),
                    "home_view": bundle["home_view"],
                    "trust_panel": bundle["trust_panel"],
                    "retrieval_manifest": bundle["retrieval_manifest"],
                },
            )
            return True

        if path == "/api/operator/query":
            raw_day = (qs.get("day") or [None])[0]
            day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else _select_latest_day(_union_days())
            query_text = (qs.get("q") or [""])[0]
            if not day:
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["DAY_NOT_RESOLVED"]})
                return True
            if not isinstance(query_text, str) or not query_text.strip():
                self._send_json(HTTPStatus.OK, {"ok": False, "errors": ["QUERY_TEXT_REQUIRED"]})
                return True
            selected_root = _truth_root_for_day(day)
            bundle = build_operator_query_bundle(
                repo_root=REPO_ROOT,
                truth_root=selected_root,
                day_utc=day,
                query_text=query_text,
            )
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "generated_utc": _utc_now_iso(),
                    "day_utc": day,
                    "truth_root": str(selected_root),
                    "query_response": bundle["query_response"],
                    "trust_panel": bundle["trust_panel"],
                    "retrieval_manifest": bundle["retrieval_manifest"],
                },
            )
            return True

        if path.startswith("/api/day/"):
            parts = path.strip("/").split("/")
            if len(parts) != 4:
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
                return True
            _, _, day, leaf = parts
            if leaf == "summary":
                self._send_json(HTTPStatus.OK, _day_summary(day))
                return True
            if leaf == "plan":
                self._send_json(HTTPStatus.OK, _day_plan(day))
                return True
            if leaf == "submissions":
                self._send_json(HTTPStatus.OK, _day_submissions(day))
                return True
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
            return True

        if path == "/api/series/nav":
            self._send_json(HTTPStatus.OK, _series_nav_endpoint(qs))
            return True

        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
        return True

    def _read_json_body(self) -> Dict[str, Any]:
        try:
            content_length = int(self.headers.get("Content-Length") or "0")
        except Exception:
            content_length = 0
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception as exc:
            raise ConfigurationWorkflowApiError(
                "Invalid JSON body.",
                status_code=400,
                reason_codes=["INVALID_JSON_BODY"],
            ) from exc
        if not isinstance(body, dict):
            raise ConfigurationWorkflowApiError(
                "Body must be an object.",
                status_code=400,
                reason_codes=["BODY_MUST_BE_OBJECT"],
            )
        return body

    def _route_configuration_post(self) -> bool:
        u = urlparse(self.path)
        path = u.path
        if path == "/api/configuration/drafts":
            try:
                body = self._read_json_body()
                payload = create_configuration_draft_v1(body)
            except ConfigurationWorkflowApiError as exc:
                self._send_json(
                    exc.status_code,
                    {
                        "ok": False,
                        "message": str(exc),
                        "reason_codes": exc.reason_codes,
                        "details": exc.details,
                    },
                )
                return True
            self._send_json(HTTPStatus.CREATED, payload)
            return True
        if not path.startswith("/api/configuration/drafts/"):
            return False
        parts = path.strip("/").split("/")
        if len(parts) != 5 or parts[0] != "api" or parts[1] != "configuration" or parts[2] != "drafts":
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
            return True
        draft_id = parts[3]
        action = parts[4]
        try:
            body = self._read_json_body()
            if action == "validate":
                payload = validate_configuration_draft_v1(draft_id)
            elif action == "review":
                payload = review_configuration_draft_v1(draft_id)
            elif action == "activate":
                payload = activate_configuration_draft_v1(draft_id)
            elif action == "reject":
                payload = reject_configuration_draft_v1(draft_id, body)
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": path})
                return True
        except ConfigurationWorkflowApiError as exc:
            self._send_json(
                exc.status_code,
                {
                    "ok": False,
                    "message": str(exc),
                    "reason_codes": exc.reason_codes,
                    "details": exc.details,
                },
            )
            return True
        self._send_json(HTTPStatus.OK, payload)
        return True

    def _route_action_post(self) -> bool:
        u = urlparse(self.path)
        path = u.path

        if path == "/api/aegis/commands/execute":
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("operational_day") or body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day_for_report(requested_day, "operator_state_snapshot_v1")

                def _repair_job_runner(job_request: Dict[str, Any]) -> Dict[str, Any]:
                    domain_id = str(job_request.get("domain_id") or "")
                    if domain_id == "US_EQUITIES_EOD":
                        orchestration = run_domain_repair_orchestration_v1(
                            truth_root=GLOBAL_TRUTH_ROOT,
                            repo_root=REPO_ROOT,
                            day_utc=day,
                            domain_id=domain_id,
                            execute=True,
                        )
                        result = next((row for row in orchestration.get("results", []) if isinstance(row, dict) and row.get("domain_id") == domain_id), {})
                        failure_detail = str((result or {}).get("failure_reason") or "")
                        for command_result in (result or {}).get("command_results", []) if isinstance((result or {}).get("command_results"), list) else []:
                            stdout_tail = str(command_result.get("stdout_tail") or "")
                            if stdout_tail:
                                try:
                                    parsed = json.loads(stdout_tail.strip())
                                    failure_detail = str(parsed.get("failure_reason") or parsed.get("message") or failure_detail)
                                except Exception:
                                    failure_detail = stdout_tail[-500:] or failure_detail
                                break
                        return {
                            "job_id": str((result or {}).get("event_id") or f"domain-repair:{day}:{domain_id}"),
                            "status": str((result or {}).get("status") or "COMPLETED"),
                            "failure_reason": str((result or {}).get("failure_reason") or ""),
                            "failure_detail": failure_detail,
                            "queued_at_utc": str((orchestration.get("lifecycle") or {}).get("generated_at_utc") or ""),
                            "next_retry_utc": "",
                            "orchestration": orchestration,
                        }
                    return _enqueue_data_remediation_job_v1(
                        truth_root=GLOBAL_TRUTH_ROOT,
                        day_utc=day,
                        playbook_id="refresh_runtime_truth",
                        request_payload={**body, "domain_id": domain_id, "source": "aegis_command_contract"},
                    )

                payload = execute_aegis_command_v1(
                    body,
                    truth_root=GLOBAL_TRUTH_ROOT,
                    repo_root=REPO_ROOT,
                    day_utc=day,
                    actor=str(body.get("requested_by") or "operator-ui"),
                    repair_job_runner=_repair_job_runner,
                )
                status = HTTPStatus.ACCEPTED if payload.get("ok") else HTTPStatus.BAD_REQUEST
                self._send_json(status, payload)
            except KeyError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "command_id": str(self._read_json_body().get("command_id") if False else ""), "result_status": "UNKNOWN_COMMAND", "user_message": str(exc), "error_message": str(exc), "broker_execution_allowed": False, "autonomous_execution_allowed": False, "trade_advice_allowed": False})
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "result_status": "COMMAND_FAILED", "user_message": "Aegis command failed.", "error_message": str(exc), "broker_execution_allowed": False, "autonomous_execution_allowed": False, "trade_advice_allowed": False})
            return True

        if path == "/api/research-lab/start-research":
            try:
                payload = research_lab_start_research_v1(self._read_json_body())
                self._send_json(HTTPStatus.CREATED, payload)
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "message": str(exc), "broker_execution_allowed": False, "live_trading_allowed": False, "automatic_promotion_allowed": False})
            except Exception as exc:
                self._send_json(HTTPStatus.CONFLICT, {"ok": False, "message": str(exc), "broker_execution_allowed": False, "live_trading_allowed": False, "automatic_promotion_allowed": False})
            return True

        if path.startswith("/api/research-lab/hypothesis-proposals/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 5 and parts[4] == "review":
                try:
                    self._send_json(HTTPStatus.CREATED, research_lab_console_review_hypothesis_v1(hypothesis_proposal_id=parts[3], payload=self._read_json_body()))
                except Exception as exc:
                    self._send_json(HTTPStatus.CONFLICT, research_lab_console_action_failure_response_v1(action="review", hypothesis_proposal_id=parts[3], error=exc))
                return True
            if len(parts) == 5 and parts[4] == "assess-readiness":
                try:
                    self._send_json(HTTPStatus.CREATED, research_lab_console_assess_hypothesis_v1(hypothesis_proposal_id=parts[3]))
                except Exception as exc:
                    self._send_json(HTTPStatus.CONFLICT, research_lab_console_action_failure_response_v1(action="assess-readiness", hypothesis_proposal_id=parts[3], error=exc))
                return True
            if len(parts) == 5 and parts[4] == "convert-to-research-plan":
                try:
                    self._send_json(HTTPStatus.CREATED, research_lab_console_convert_hypothesis_v1(hypothesis_proposal_id=parts[3], payload=self._read_json_body()))
                except Exception as exc:
                    self._send_json(HTTPStatus.CONFLICT, research_lab_console_action_failure_response_v1(action="convert-to-research-plan", hypothesis_proposal_id=parts[3], error=exc))
                return True

        if path.startswith("/api/research-lab/research-plans/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 5 and parts[4] in {"run-event-study", "run-backtest"}:
                self._send_json(HTTPStatus.ACCEPTED, research_lab_explicit_action_placeholder_v1(action=parts[4], entity_id=parts[3], payload=self._read_json_body()))
                return True

        if path.startswith("/api/research-lab/paper-trials/"):
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 5 and parts[4] in {"record-observation", "measure-due-outcomes", "review"}:
                self._send_json(HTTPStatus.ACCEPTED, research_lab_explicit_action_placeholder_v1(action=parts[4], entity_id=parts[3], payload=self._read_json_body()))
                return True

        if path == "/api/aegis/data-remediation/run":
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day(requested_day)
                playbook_id = str(body.get("playbook_id") or "")
                if playbook_id and playbook_id not in {"refresh_required_symbol_data", "refresh_runtime_truth", "rebuild_operator_projections", "mark_provider_data_needed"}:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "PLAYBOOK_NOT_APPROVED", "broker_execution_allowed": False, "autonomous_execution_allowed": False})
                    return True
                payload = _enqueue_data_remediation_job_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    playbook_id=playbook_id or "refresh_required_symbol_data",
                    request_payload=body,
                )
                self._send_json(HTTPStatus.ACCEPTED, {"ok": True, "background_job_enqueued": True, **payload, "broker_execution_allowed": False, "order_routing_allowed": False, "live_trading_allowed": False, "autonomous_execution_allowed": False})
                return True
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": "DATA_REMEDIATION_FAILED", "detail": str(exc), "broker_execution_allowed": False, "autonomous_execution_allowed": False})
                return True

        if path == "/api/aegis/operator/commands":
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1")
                cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
                result = execute_operator_command_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    repo_root=REPO_ROOT,
                    day_utc=day,
                    cockpit_payload=cockpit,
                    request_payload=body,
                )
                if result.get("ok") is True:
                    ledger_payload = build_candidate_review_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, filter_name="all")
                    ledger_paths = write_candidate_review_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=ledger_payload)
                    canonical = build_canonical_operator_state_v1(truth_root=GLOBAL_TRUTH_ROOT, repo_root=REPO_ROOT, day_utc=day)
                    canonical_paths = write_canonical_operator_state_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=canonical)
                    brief = build_operator_brief_v1(canonical=canonical, canonical_path=Path(canonical_paths["json"]))
                    brief_paths = write_operator_brief_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=brief)
                    refreshed = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
                    result = {
                        **result,
                        "operator_task_projection": refreshed.get("operator_task_projection", {}),
                        "active_opportunity_projection": refreshed.get("active_opportunity_projection", {}),
                        "review_ledger_projection": refreshed.get("review_ledger_projection", {}),
                        "canonical_operator_state": canonical_paths,
                        "candidate_review_ledger": ledger_paths,
                        "operator_brief": brief_paths,
                    }
                status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
                self._send_json(status, {
                    **result,
                    "review_only": True,
                    "broker_execution_allowed": False,
                    "order_routing_allowed": False,
                    "live_trading_allowed": False,
                    "autonomous_execution_allowed": False,
                    "automatic_approval_allowed": False,
                    "automatic_promotion_allowed": False,
                })
                return True
            except Exception as exc:
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {
                        "ok": False,
                        "message": "Operator command failed.",
                        "error": str(exc),
                        "review_only": True,
                        "broker_execution_allowed": False,
                        "order_routing_allowed": False,
                        "live_trading_allowed": False,
                        "autonomous_execution_allowed": False,
                        "automatic_approval_allowed": False,
                        "automatic_promotion_allowed": False,
                    },
                )
                return True
        if path == "/api/aegis/operator/manual-capture-records":
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
                day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "operator_state_snapshot_v1"))
                record = append_manual_capture_record_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, request_payload=body)
            except ValueError as exc:
                error_payload = _json_error_payload(str(exc))
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "ok": False,
                    **error_payload,
                    "message": error_payload.get("message") or str(exc),
                    "review_only": True,
                    "broker_execution_allowed": False,
                    "order_routing_allowed": False,
                    "live_trading_allowed": False,
                    "allocation_allowed": False,
                    "paper_submit_created": False,
                    "autonomous_execution_allowed": False,
                    "automatic_approval_allowed": False,
                })
                return True
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {
                    "ok": False,
                    "message": "Manual capture record append failed.",
                    "error": str(exc),
                    "review_only": True,
                    "broker_execution_allowed": False,
                    "order_routing_allowed": False,
                    "live_trading_allowed": False,
                    "allocation_allowed": False,
                    "paper_submit_created": False,
                    "autonomous_execution_allowed": False,
                    "automatic_approval_allowed": False,
                })
                return True
            self._send_json(HTTPStatus.OK, {
                "ok": True,
                "record": record,
                "record_id": record.get("record_id"),
                "manual_capture_record_id": record.get("manual_capture_record_id") or record.get("record_id"),
                "event_ids": record.get("event_ids") if isinstance(record.get("event_ids"), list) else [],
                "current_ticket_id": record.get("ticket_id"),
                "current_ticket_hash": (record.get("trade_ticket_lineage") or {}).get("lineage_hash") if isinstance(record.get("trade_ticket_lineage"), dict) else "",
                "operator_statement": "Manual capture record appended. Aegis did not place, route, submit, or transmit an order.",
                "manual_ib_capture_recorded": True,
                "human_review_required": False,
                "broker_execution_allowed": False,
                "order_routing_allowed": False,
                "live_trading_allowed": False,
                "allocation_allowed": False,
                "paper_submit_created": False,
                "autonomous_execution_allowed": False,
                "automatic_approval_allowed": False,
                "automatic_promotion_allowed": False,
            })
            return True


        if path in {"/api/aegis/position/risk-plan", "/api/aegis/position/stop-event", "/api/aegis/position/correction"}:
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1")
                if path.endswith("/risk-plan"):
                    event = append_position_risk_plan_v1(
                        truth_root=GLOBAL_TRUTH_ROOT,
                        day_utc=day,
                        candidate_id=str(body.get("candidate_id") or ""),
                        sleeve_id=str(body.get("sleeve_id") or ""),
                        symbol=str(body.get("symbol") or ""),
                        direction=str(body.get("direction") or ""),
                        quantity=body.get("quantity"),
                        entry_price=body.get("entry_price"),
                        entry_timestamp_utc=str(body.get("entry_timestamp_utc") or ""),
                        stop_type=str(body.get("stop_type") or ""),
                        stop_price=body.get("stop_price"),
                        target_price=body.get("target_price"),
                        time_stop_at=str(body.get("time_stop_at") or ""),
                        stop_reason=str(body.get("stop_reason") or ""),
                        operator=str(body.get("operator") or ""),
                        reason=str(body.get("reason") or ""),
                    )
                elif path.endswith("/stop-event"):
                    event = append_stop_event_v1(
                        truth_root=GLOBAL_TRUTH_ROOT,
                        day_utc=day,
                        candidate_id=str(body.get("candidate_id") or ""),
                        stop_triggered=body.get("stop_triggered"),
                        stop_price=body.get("stop_price"),
                        exit_price=body.get("exit_price"),
                        exit_timestamp_utc=str(body.get("exit_timestamp_utc") or ""),
                        exit_reason=str(body.get("exit_reason") or ""),
                        operator=str(body.get("operator") or ""),
                        reason=str(body.get("reason") or ""),
                    )
                    update_candidate_outcomes_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, candidate_id=str(body.get("candidate_id") or ""), outcome_status=str(body.get("outcome_status") or "OUTCOME_PENDING"))
                else:
                    event = append_position_event_correction_v1(
                        truth_root=GLOBAL_TRUTH_ROOT,
                        day_utc=day,
                        candidate_id=str(body.get("candidate_id") or ""),
                        field=str(body.get("field") or ""),
                        new_value=body.get("new_value"),
                        operator=str(body.get("operator") or ""),
                        reason=str(body.get("reason") or ""),
                    )
                    update_candidate_outcomes_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, candidate_id=str(body.get("candidate_id") or ""), outcome_status=str(body.get("outcome_status") or "OUTCOME_PENDING"))
                position_payload = build_position_management_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                position_paths = write_position_management_reports_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=position_payload)
                canonical = build_canonical_operator_state_v1(truth_root=GLOBAL_TRUTH_ROOT, repo_root=REPO_ROOT, day_utc=day)
                canonical_paths = write_canonical_operator_state_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=canonical)
                brief = build_operator_brief_v1(canonical=canonical, canonical_path=Path(canonical_paths["json"]))
                brief_paths = write_operator_brief_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=brief)
                journal = build_journal_timeline_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                journal_paths = write_journal_timeline_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=journal)
                candidate_id = str(event.get("candidate_id") or "")
                updated_state = next((row for row in position_payload.get("positions", []) if isinstance(row, dict) and str(row.get("candidate_id") or "") == candidate_id), {})
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "message": str(exc), "broker_execution_allowed": False, "autonomous_execution_allowed": False, "order_routing_allowed": False, "automatic_stop_execution_allowed": False})
                return True
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "message": "Position management event append failed.", "error": str(exc), "broker_execution_allowed": False, "autonomous_execution_allowed": False, "order_routing_allowed": False, "automatic_stop_execution_allowed": False})
                return True
            self._send_json(HTTPStatus.OK, {
                "ok": True,
                "event": event,
                "position_state": updated_state,
                "position_management": position_paths,
                "canonical_operator_state": canonical_paths,
                "operator_brief": brief_paths,
                "journal_timeline": journal_paths,
                "operator_statement": "Aegis tracked this position event. Aegis did not execute a broker order or stop.",
                "manual_ib_capture_recorded": True,
                "human_review_required": False,
                "broker_execution_allowed": False,
                "order_routing_allowed": False,
                "live_trading_allowed": False,
                "autonomous_execution_allowed": False,
                "automatic_stop_execution_allowed": False,
                "automatic_order_placement_allowed": False,
                "automatic_sleeve_mutation_allowed": False,
            })
            return True

        if path == "/api/aegis/candidate-review":
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1")
                event = append_candidate_review_action_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    candidate_id=str(body.get("candidate_id") or ""),
                    action=str(body.get("action") or ""),
                    operator=str(body.get("operator") or "operator-ui"),
                    operator_note=str(body.get("operator_note") or ""),
                    source="UI",
                )
                ledger_payload = build_candidate_review_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, filter_name="all")
                ledger_paths = write_candidate_review_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=ledger_payload)
                canonical = build_canonical_operator_state_v1(truth_root=GLOBAL_TRUTH_ROOT, repo_root=REPO_ROOT, day_utc=day)
                canonical_paths = write_canonical_operator_state_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=canonical)
                brief = build_operator_brief_v1(canonical=canonical, canonical_path=Path(canonical_paths["json"]))
                brief_paths = write_operator_brief_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=brief)
            except ValueError as exc:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "ok": False,
                        "message": str(exc),
                        "review_only": True,
                        "broker_execution_allowed": False,
                        "autonomous_execution_allowed": False,
                        "automatic_approval_allowed": False,
                    },
                )
                return True
            except Exception as exc:
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {
                        "ok": False,
                        "message": "Candidate review action failed.",
                        "error": str(exc),
                        "review_only": True,
                        "broker_execution_allowed": False,
                        "autonomous_execution_allowed": False,
                        "automatic_approval_allowed": False,
                    },
                )
                return True
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "event": event,
                    "canonical_operator_state": canonical_paths,
                    "candidate_review_ledger": ledger_paths,
                    "operator_brief": brief_paths,
                    "review_only": True,
                    "human_review_required": True,
                    "broker_execution_allowed": False,
                    "autonomous_execution_allowed": False,
                    "automatic_approval_allowed": False,
                },
            )
            return True
        if path == "/api/aegis/manual-external-capture":
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1")
                event = append_manual_external_capture_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    candidate_id=str(body.get("candidate_id") or ""),
                    manually_captured=body.get("manually_captured"),
                    quantity=body.get("quantity"),
                    capture_timestamp=str(body.get("capture_timestamp") or ""),
                    external_execution_venue=str(body.get("external_execution_venue") or ""),
                    operator_notes=str(body.get("operator_notes") or ""),
                    confidence_override=str(body.get("confidence_override") or ""),
                    paper_trade_only=body.get("paper_trade_only", True),
                    review_decision=str(body.get("review_decision") or "MANUAL_CAPTURE_RECORDED"),
                    operator=str(body.get("operator") or "operator-ui"),
                )
                review_action = {
                    "WATCHLIST": "watchlist",
                    "NEEDS_MORE_EVIDENCE": "needs-more-evidence",
                    "DISMISS": "dismiss",
                }.get(str(body.get("review_decision") or "").strip().upper().replace("-", "_"), "add-note")
                review_event = append_candidate_review_action_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    candidate_id=str(body.get("candidate_id") or ""),
                    action=review_action,
                    operator=str(body.get("operator") or "operator-ui"),
                    operator_note=f"Manual external capture record: {event['capture_event_id']}. Aegis did not execute this trade. {str(body.get('operator_notes') or '').strip()}".strip(),
                    source="UI_MANUAL_CAPTURE",
                )
                ledger_payload = build_candidate_review_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, filter_name="all")
                ledger_paths = write_candidate_review_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=ledger_payload)
                canonical = build_canonical_operator_state_v1(truth_root=GLOBAL_TRUTH_ROOT, repo_root=REPO_ROOT, day_utc=day)
                canonical_paths = write_canonical_operator_state_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=canonical)
                brief = build_operator_brief_v1(canonical=canonical, canonical_path=Path(canonical_paths["json"]))
                brief_paths = write_operator_brief_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=brief)
            except ValueError as exc:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "ok": False,
                        "message": str(exc),
                        "operator_statement": "Aegis did not execute this trade.",
                        "review_only": True,
                        "broker_execution_allowed": False,
                        "order_routing_allowed": False,
                        "live_trading_allowed": False,
                        "autonomous_execution_allowed": False,
                        "automatic_approval_allowed": False,
                        "automatic_promotion_allowed": False,
                    },
                )
                return True
            except Exception as exc:
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {
                        "ok": False,
                        "message": "Manual external capture recording failed.",
                        "error": str(exc),
                        "operator_statement": "Aegis did not execute this trade.",
                        "review_only": True,
                        "broker_execution_allowed": False,
                        "order_routing_allowed": False,
                        "live_trading_allowed": False,
                        "autonomous_execution_allowed": False,
                        "automatic_approval_allowed": False,
                        "automatic_promotion_allowed": False,
                    },
                )
                return True
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "event": event,
                    "review_event": review_event,
                    "canonical_operator_state": canonical_paths,
                    "candidate_review_ledger": ledger_paths,
                    "operator_brief": brief_paths,
                    "operator_statement": "Aegis did not execute this trade.",
                    "review_only": True,
                    "human_review_required": True,
                    "broker_execution_allowed": False,
                    "order_routing_allowed": False,
                    "live_trading_allowed": False,
                    "autonomous_execution_allowed": False,
                    "automatic_approval_allowed": False,
                    "automatic_promotion_allowed": False,
                },
            )
            return True
        if path.startswith("/api/aegis/edge-lab/hypothesis/"):
            endpoint = path.rsplit("/", 1)[-1]
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day_for_report(requested_day, "aegis_research_pipeline_v1")
                payload = execute_edge_lab_workflow_action_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    endpoint=endpoint,
                    request_payload=body,
                )
            except EdgeLabWorkflowApiError as exc:
                self._send_json(
                    exc.status_code,
                    {
                        "ok": False,
                        "message": str(exc),
                        "details": exc.details,
                        **_edge_lab_safety_fields(),
                    },
                )
                return True
            except Exception as exc:
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {
                        "ok": False,
                        "message": "Edge Lab workflow action failed.",
                        "error": str(exc),
                        **_edge_lab_safety_fields(),
                    },
                )
                return True
            self._send_json(HTTPStatus.OK, payload)
            return True
        if path.startswith("/api/configuration/"):
            return self._route_configuration_post()
        if path.startswith("/api/reliability/"):
            return self._route_reliability_post(path)
        if path.startswith("/api/commands/"):
            try:
                content_length = int(self.headers.get("Content-Length") or "0")
            except Exception:
                content_length = 0
            raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                body = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "result": "INVALID_JSON_BODY"})
                return True
            if not isinstance(body, dict):
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "result": "BODY_MUST_BE_OBJECT"})
                return True
            result = dispatch_kernel_command(path, body)
            self._send_json(HTTPStatus.OK, result)
            return True
        if not path.startswith("/api/actions/"):
            return False

        try:
            content_length = int(self.headers.get("Content-Length") or "0")
        except Exception:
            content_length = 0
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "result": "INVALID_JSON_BODY"})
            return True
        if not isinstance(body, dict):
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "result": "BODY_MUST_BE_OBJECT"})
            return True

        action_map = {
            "/api/actions/refresh-lifecycle": "refresh-lifecycle",
            "/api/actions/run-reconciliation": "run-reconciliation",
            "/api/actions/run-replay-check": "run-replay-check",
            "/api/actions/cancel-working-order": "cancel-working-order",
        }
        action_name = action_map.get(path)
        if action_name is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "result": "ENDPOINT_NOT_FOUND", "path": path})
            return True

        result = run_action(action_name, body)
        status_code = HTTPStatus.OK if result.get("ok") else HTTPStatus.CONFLICT
        self._send_json(status_code, result)
        return True

    def do_GET(self) -> None:
        started = time.perf_counter()
        parsed = urlparse(self.path)
        path = parsed.path
        if path in {"/health", "/healthz"}:
            self._send_json(HTTPStatus.OK, self._health_payload())
            sys.stderr.write(f"TIMING: api endpoint={path} duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
            return
        if path == "/healthz/aegis-lite-ui":
            self._send_json(HTTPStatus.OK, build_aegis_lite_ui_health_view(GLOBAL_TRUTH_ROOT))
            sys.stderr.write(f"TIMING: api endpoint=/healthz/aegis-lite-ui duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
            return
        if path == "/readyz":
            payload = self._readyz_payload()
            status_code = HTTPStatus.OK if payload["status"] in {"PASS", "DEGRADED"} else HTTPStatus.SERVICE_UNAVAILABLE
            self._send_json(status_code, payload)
            sys.stderr.write(f"TIMING: api endpoint=/readyz duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
            return
        if path == "/runtime-status":
            qs = parse_qs(parsed.query)
            raw_day = (qs.get("day") or [None])[0]
            requested_day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else None
            self._send_json(HTTPStatus.OK, _runtime_status_projection(requested_day))
            sys.stderr.write(f"TIMING: api endpoint=/runtime-status duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
            return
        if path == "/performance/cockpit.html":
            qs = parse_qs(parsed.query)
            raw_day = (qs.get("day") or [None])[0]
            requested_day = raw_day if isinstance(raw_day, str) and raw_day and _is_day_str(raw_day) else None
            self._send_performance_cockpit_html(requested_day)
            sys.stderr.write(f"TIMING: performance cockpit duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
            return
        try:
            if self._route_api():
                sys.stderr.write(f"TIMING: api endpoint={path} duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
                return
        except Exception as exc:
            if path.startswith("/api/research-lab/"):
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": False,
                        "read_only": True,
                        "error": "research_lab_route_unavailable",
                        "error_type": type(exc).__name__,
                        "error_detail": str(exc),
                        "path": path,
                    },
                )
                sys.stderr.write(f"TIMING: api endpoint={path} status=research_lab_safe_error duration_ms={(time.perf_counter() - started) * 1000:.1f}\n")
                return
            raise
        return super().do_GET()

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self) -> None:
        if self._route_action_post():
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": self.path})

    def do_PATCH(self) -> None:
        path = urlparse(self.path).path
        if path.startswith("/api/reliability/") and self._route_reliability_patch(path):
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "errors": ["ENDPOINT_NOT_FOUND"], "path": self.path})

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s - - [%s] %s\n" % (self.client_address[0], _utc_now_iso(), fmt % args))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3000)
    ns = ap.parse_args(argv)

    if not TRUTH_ROOT.exists():
        sys.stderr.write(f"ERROR: {E_TRUTH_ROOT_MISSING}: {TRUTH_ROOT}\n")

    httpd = ThreadingHTTPServer((ns.host, ns.port), OpsHandler)
    sys.stderr.write(f"OK: OPS_DASHBOARD_LISTENING http://{ns.host}:{ns.port}\n")
    sys.stderr.write(f"OK: STATIC_DIR {OpsHandler.STATIC_DIR}\n")
    sys.stderr.write(f"OK: TRUTH_ROOT {TRUTH_ROOT}\n")
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
