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
import subprocess
import sys
import time
from datetime import date, datetime, timezone, timedelta
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple
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
from ops.aegis.verified_runtime_graph_v1 import graph_staleness_warnings_v1
from ops.tools.aegis_submit_enforcement_v1 import packet_currentness_v1
from ops.aegis.intelligence_common_v1 import intelligence_summaries_v1, latest_json_v1
from ops.aegis.ai_operations_assistant_v1 import (
    ai_operations_context_path_v1,
    ai_operations_response_path_v1,
    build_ai_operations_context_v1,
    build_ai_operations_response_v1,
    write_ai_operations_context_v1,
    write_ai_operations_response_v1,
)
from ops.aegis.engineering_priority_queue_v1 import (
    engineering_priority_queue_path_v1,
    build_engineering_priority_queue_v1,
    write_engineering_priority_queue_v1,
)
from ops.aegis.canonical_operator_state_v1 import (
    _load_sources as load_canonical_operator_sources_v1,
    build_candidate_ui_projection_v1,
    build_canonical_operator_state_v1,
    write_canonical_operator_state_v1,
)
from ops.aegis.candidate_lifecycle_v1 import append_candidate_review_action_v1, update_candidate_outcomes_v1
from ops.aegis.paper_trade_golden_path_v1 import latest_paper_trade_golden_path_v1
from ops.aegis.paper_operator_projection_v1 import build_and_write_paper_operator_projection_v1, paper_operator_projection_path_v1
from ops.aegis.candidate_lifecycle_projection_v1 import build_and_write_candidate_lifecycle_projection_v1, candidate_lifecycle_projection_path_v1
from ops.aegis.signal_evidence_boundary_v1 import signal_evidence_boundary_path_v1
from ops.aegis.duplicate_candidate_v1 import duplicate_candidate_path_v1
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
from ops.aegis.research_portfolio_manager_v1 import build_research_portfolio_v1
from ops.aegis.ai_research_intelligence_v1 import build_ai_research_intelligence_bundle_v1
from ops.aegis.oil_shock_candidate_flow_v1 import build_oil_shock_candidate_flow_v1
from ops.aegis.oil_shock_candidate_construction_v1 import build_oil_shock_candidate_construction_v1
from ops.aegis.generated_hypothesis_paper_setup_bridge_v1 import build_generated_hypothesis_paper_setup_bridge_v1
from ops.aegis.generated_hypothesis_governance_bridge_v1 import build_generated_hypothesis_governance_bridge_v1
from ops.aegis.generated_hypothesis_approval_event_lineage_v1 import build_generated_hypothesis_approval_event_lineage_v1
from ops.aegis.generated_hypothesis_signal_to_candidate_v1 import build_generated_hypothesis_signal_to_candidate_v1
from ops.aegis.macro_calendar_data_readiness_v1 import (
    build_macro_calendar_data_readiness_v1,
    macro_calendar_data_readiness_path_v1,
)
from ops.aegis.research_quality_control_v1 import (
    build_hypothesis_decision_policy_v1,
    build_research_allocation_recommendation_v1,
    build_research_follow_through_control_v1,
    build_research_quality_engine_v1,
)
from ops.aegis.attention_queue_projection_v1 import build_attention_queue_projection_v1
from ops.aegis.command_center_queue_audit_v1 import command_center_queue_audit_path_v1
from ops.aegis.surface_readiness_v1 import (
    build_surface_readiness_v1,
    surface_readiness_path_v1,
    write_surface_readiness_v1,
)
from ops.aegis.operator_surface_contract_v1 import (
    build_operator_surface_contract_v1,
    operator_surface_contract_path_v1,
    write_operator_surface_contract_v1,
)
from ops.aegis.operator_action_model_v1 import (
    build_operator_action_model_v1,
    operator_action_model_path_v1,
    write_operator_action_model_v1,
)
from ops.aegis.scheduled_run_readiness_certificate_v1 import (
    build_scheduled_run_readiness_certificate_v1,
    scheduled_run_readiness_certificate_path_v1,
    write_scheduled_run_readiness_certificate_v1,
)
from ops.aegis.scheduled_run_reconciliation_v1 import (
    build_scheduled_run_reconciliation_v1,
    scheduled_run_reconciliation_path_v1,
    write_scheduled_run_reconciliation_v1,
)

from ops.aegis.research_lab.research_validation_engine_v1 import (
    research_hypothesis_registry_path_v1,
    research_validation_protocol_path_v1,
    research_validation_run_path_v1,
    research_validation_result_path_v1,
    research_promotion_gate_path_v1,
    research_outcome_feedback_path_v1,
)
from ops.aegis.hypothesis_proposal_promotion_v1 import (
    approval_events_path_v1,
    approval_queue_path_v1,
    build_all_hypothesis_proposal_promotion_v1,
    evidence_packets_path_v1,
    promotion_packets_path_v1,
    promotion_pipeline_path_v1,
    record_paper_promotion_approval_event_v1,
    shadow_trials_path_v1,
)
from ops.aegis.approved_hypothesis_paper_setup_v1 import (
    approved_hypothesis_paper_tracking_setup_path_v1,
    paper_readiness_certification_path_v1,
    paper_sleeve_blueprint_path_v1,
)
from ops.aegis.hypothesis_workflow_state_v1 import (
    append_operator_action_event_v1,
    generated_hypothesis_throughput_path_v1,
    hypothesis_workflow_replay_verification_path_v1,
    hypothesis_workflow_state_path_v1,
    operator_action_event_log_path_v1,
    operator_action_queue_path_v1,
)
from ops.aegis.change_control_v1 import (
    ChangeControlValidationError,
    REGISTER_PATH as CHANGE_CONTROL_REGISTER_PATH,
    build_report as build_change_control_report_v1,
    load_register as load_change_control_register_v1,
    record_controlled_decision as record_change_control_decision_v1,
    validate_register as validate_change_control_register_v1,
)
from ops.aegis.change_control_intelligence_v1 import (
    build_all_v1 as build_change_control_intelligence_v1,
)
from ops.aegis.data_remediation_v1 import (
    classify_data_blockers_v1,
    get_remediation_attempt_v1,
    latest_remediation_v1,
    run_data_remediation_v1,
)
from ops.aegis.operator_state.canonical_operator_state_builder_v1 import (
    load_or_build_operator_state_snapshot_response_v1,
    read_operator_state_snapshot_v1,
    api_envelope_v1 as operator_state_api_envelope_v1,
)
from ops.aegis.operator_state.current_operator_truth_resolver_v1 import (
    api_envelope_v1 as current_operator_truth_api_envelope_v1,
    resolve_current_operator_truth_v1,
)
from ops.aegis.market_calendar.session_calendar_v1 import (
    is_us_equities_trading_day_v1,
    latest_us_equities_trading_day_on_or_before_v1,
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
from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import (
    build_paper_trade_evaluation_projection_v1,
    build_trade_lifecycle_ledger_v1,
    write_paper_trade_evaluation_projection_v1,
    write_trade_lifecycle_ledger_v1,
)
from ops.aegis.trade_lifecycle.exit_review_projection_v1 import (
    build_exit_review_projection_v1,
    write_exit_review_projection_v1,
)
from ops.aegis.trade_lifecycle.portfolio_context_projection_v1 import (
    build_portfolio_context_projection_v1,
    write_portfolio_context_projection_v1,
)
from ops.aegis.narrative_operational_analytics_v1 import (
    build_narrative_operational_analytics_v1,
    write_narrative_operational_analytics_v1,
)
from ops.aegis.daily_paper_performance_v1 import daily_paper_performance_path_v1
from ops.aegis.paper_pnl_report_v1 import paper_pnl_report_path_v1
from ops.aegis.paper_performance_report_v1 import build_paper_performance_report_v1
from ops.aegis.operator_portfolio_valuation_estimate_v1 import (
    build_operator_portfolio_valuation_estimate_v1,
    operator_portfolio_valuation_estimate_path_v1,
    write_operator_portfolio_valuation_estimate_v1,
)
from ops.aegis.sleeve_analytics_v1 import build_sleeve_analytics_v1
from ops.aegis.position_review_v1 import load_position_review_brief_v1, position_review_brief_path_v1
from ops.aegis.research_lab.research_review_brief_v1 import load_research_review_brief_v1, research_review_brief_path_v1
from ops.aegis.advisor_benchmark_v1 import AdvisorBenchmarkValidationError, write_advisor_benchmark_snapshot_v1
from ops.aegis.exit_strategy_analysis_v1 import exit_strategy_analysis_path_v1
from ops.aegis.sleeve_performance_truth_v1 import sleeve_performance_truth_path_v1
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
from ops.aegis.candidate_lineage_forensics_v1 import build_candidate_lineage_forensics_v1, write_candidate_lineage_forensics_v1
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
    return str(_operator_day_resolution_v1(raw_day).get("resolved_day") or date.today().isoformat())


def _verified_runtime_graph_root_v1() -> Path:
    return (_canonical_truth_root() / "reports" / "aegis_verified_runtime_graph_v1").resolve()


def _latest_verified_runtime_graph_day_v1(raw_day: Optional[str] = None) -> str:
    if isinstance(raw_day, str) and _is_day_str(raw_day):
        return raw_day
    today_text = date.today().isoformat()
    days = [day for day in _list_day_dirs(_verified_runtime_graph_root_v1()) if day <= today_text]
    return days[-1] if days else today_text


def _portal_runtime_model_path_v1(day_utc: str) -> Path:
    return _verified_runtime_graph_root_v1() / day_utc / "portal_runtime_model.v1.json"


def _verified_runtime_graph_path_v1(day_utc: str) -> Path:
    return _verified_runtime_graph_root_v1() / day_utc / "verified_runtime_graph.v1.json"


def _evidence_ledger_path_v1(day_utc: str) -> Path:
    return _verified_runtime_graph_root_v1() / day_utc / "evidence_ledger.v1.json"


PORTAL_VERIFIED_RUNTIME_ACTION_ALLOWLIST_V1: Dict[str, str] = {
    "run_audit": "Run Audit",
    "explain_blockers": "Explain Blockers",
    "show_evidence": "Show Evidence",
    "graph_diff": "Graph Diff",
    "hydrate_chatgpt": "Hydrate ChatGPT",
    "claim_lookup": "Claim Lookup",
    "repair_context_readiness": "Repair Context Readiness",
}

PORTAL_VERIFIED_RUNTIME_CLAIMS_V1 = {
    "trade advice allowed",
    "manual capture allowed",
    "manual trade capture allowed",
    "broker submit/transmit",
    "broker submit transmit allowed",
    "autonomous execution",
    "autonomous execution allowed",
    "portal state",
    "portal state current",
}

PORTAL_VERIFIED_RUNTIME_ACTION_TIMEOUT_SECONDS_V1 = 120


def _hydrate_packet_path_v1(day_utc: str) -> Path:
    return _verified_runtime_graph_root_v1() / day_utc / "chatgpt_hydrate_packet.v1.md"


def _safe_read_text_excerpt_v1(path: Path, limit: int = 24000) -> Tuple[str, Optional[str]]:
    try:
        text = path.read_text(encoding="utf-8")
        return text[-limit:], None
    except FileNotFoundError:
        return "", "FILE_NOT_FOUND"
    except Exception:
        return "", "READ_ERROR"


def _load_verified_runtime_recent_actions_v1(day_utc: str, truth_root: Optional[Path] = None, limit: int = 20) -> Dict[str, Any]:
    audit_path = _portal_action_audit_path_v1(day_utc, truth_root=truth_root)
    rows: List[Dict[str, Any]] = []
    try:
        lines = audit_path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {"status": "MISSING", "path": str(audit_path), "rows": []}
    except Exception:
        return {"status": "READ_ERROR", "path": str(audit_path), "rows": []}
    for raw_line in lines[-max(limit, 1):]:
        try:
            row = json.loads(raw_line)
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    rows = rows[-limit:]
    rows.reverse()
    return {"status": "CURRENT" if rows else "EMPTY", "path": str(audit_path), "rows": rows}


def _verified_runtime_recovery_plan_v1(graph_payload: Any, day_utc: str) -> Dict[str, Any]:
    if not isinstance(graph_payload, dict):
        return {"status": "MISSING", "items": [], "source_path": ""}
    linkage = graph_payload.get("readiness_linkage_to_runtime_truth_kernel") if isinstance(graph_payload.get("readiness_linkage_to_runtime_truth_kernel"), dict) else {}
    kernel_path = Path(str(linkage.get("runtime_truth_kernel_path") or "")) if linkage.get("runtime_truth_kernel_path") else Path()
    kernel_payload, kernel_err = _safe_read_json(kernel_path) if str(kernel_path) else (None, "MISSING_RUNTIME_TRUTH_KERNEL_PATH")
    if not isinstance(kernel_payload, dict):
        return {"status": kernel_err or "MISSING", "items": [], "source_path": str(kernel_path) if str(kernel_path) else ""}
    items = kernel_payload.get("recovery_plan") if isinstance(kernel_payload.get("recovery_plan"), list) else []
    normalized: List[Dict[str, Any]] = []
    for item in items[:12]:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "artifact_id": item.get("artifact_id") or "UNKNOWN",
            "reason": item.get("reason") or item.get("what_failed") or "No reason reported.",
            "why_it_matters": item.get("why_it_matters") or "Runtime evidence is incomplete.",
            "generated_by_command": item.get("generated_by_command") or "",
            "validates_with_command": item.get("validates_with_command") or "npm run aegis:audit",
            "expected_path": item.get("expected_path") or "",
            "downstream_capabilities_expected_to_recover": item.get("downstream_capabilities_expected_to_recover") or [],
        })
    return {
        "status": "CURRENT" if normalized else "EMPTY",
        "source_path": str(kernel_path),
        "items": normalized,
    }


def _verified_runtime_status_badges_v1(*, model_status: str, graph_status: str, portal_status: str, evidence_hash_status: str, runtime_readiness_status: str) -> List[Dict[str, str]]:
    return [
        {"label": "Portal model", "status": model_status},
        {"label": "Graph", "status": graph_status},
        {"label": "Portal binding", "status": portal_status},
        {"label": "Evidence hashes", "status": evidence_hash_status},
        {"label": "Kernel readiness", "status": runtime_readiness_status},
    ]


def _load_verified_runtime_portal_model_v1(raw_day: Optional[str] = None) -> Dict[str, Any]:
    day = _latest_verified_runtime_graph_day_v1(raw_day)
    portal_path = _portal_runtime_model_path_v1(day)
    graph_path = _verified_runtime_graph_path_v1(day)
    ledger_path = _evidence_ledger_path_v1(day)
    portal_model, portal_err = _safe_read_json(portal_path)
    graph_payload, graph_err = _safe_read_json(graph_path)
    ledger_payload, ledger_err = _safe_read_json(ledger_path)
    if not isinstance(portal_model, dict):
        return {
            "ok": False,
            "model_status": "MISSING",
            "reason_codes": [portal_err or "PORTAL_RUNTIME_MODEL_MISSING"],
            "day_utc": day,
            "portal_runtime_model_path": str(portal_path),
            "verified_runtime_graph_path": str(graph_path),
            "evidence_ledger_path": str(ledger_path),
            "derivation_source": "MISSING",
            "readiness_inference_policy": "NO_INDEPENDENT_PORTAL_READINESS_INFERENCE",
            "operator_message": "Verified runtime Portal model is missing; run npm run aegis:audit.",
            "recovery_plan": {"status": "MISSING", "items": [], "source_path": ""},
            "recent_actions": _load_verified_runtime_recent_actions_v1(day),
            "status_badges": _verified_runtime_status_badges_v1(
                model_status="MISSING",
                graph_status="MISSING",
                portal_status="MISSING",
                evidence_hash_status="UNKNOWN",
                runtime_readiness_status="UNKNOWN",
            ),
            "copy_payloads": {"hydrate_packet": "", "blocker_summary": "", "latest_action_result": ""},
        }
    stale_warnings: List[str] = []
    if isinstance(graph_payload, dict):
        stale_warnings = graph_staleness_warnings_v1(graph=graph_payload, truth_root=_canonical_truth_root(), day_utc=day)
    else:
        stale_warnings = [graph_err or "VERIFIED_RUNTIME_GRAPH_MISSING"]
    evidence_hash_status = str(((portal_model.get("evidence_hash_verification") or {}).get("status")) or ((ledger_payload or {}).get("hash_verification_status") if isinstance(ledger_payload, dict) else "UNKNOWN") or "UNKNOWN")
    graph_status = str(portal_model.get("graph_status") or "UNKNOWN")
    portal_status = str(portal_model.get("portal_status") or "UNKNOWN")
    if stale_warnings:
        model_status = "STALE"
    elif graph_status == "BLOCKED" or portal_status == "BLOCKED" or evidence_hash_status == "BLOCKED":
        model_status = "BLOCKED"
    else:
        model_status = "READY"
    recovery_plan = _verified_runtime_recovery_plan_v1(graph_payload, day)
    paper_golden_path, paper_golden = latest_paper_trade_golden_path_v1(truth_root=_canonical_truth_root(), day_utc=day)
    if paper_golden:
        paper_golden = {**paper_golden, "artifact_path": str(paper_golden_path or "")}
    recent_actions = _load_verified_runtime_recent_actions_v1(day)
    hydrate_path = _hydrate_packet_path_v1(day)
    hydrate_text, hydrate_err = _safe_read_text_excerpt_v1(hydrate_path)
    blocker_summary = "\n".join(str(item) for item in (portal_model.get("top_blockers") or [])[:50])
    latest_action_result = ""
    if recent_actions.get("rows"):
        latest_envelope = ((recent_actions.get("rows") or [{}])[0] or {}).get("result_envelope") or {}
        latest_action_result = json.dumps(latest_envelope, sort_keys=True, indent=2, default=str)
    return {
        "ok": model_status == "READY",
        "model_status": model_status,
        "stale_warnings": stale_warnings,
        "day_utc": day,
        "portal_runtime_model_path": str(portal_path),
        "verified_runtime_graph_path": str(graph_path),
        "evidence_ledger_path": str(ledger_path),
        "hydrate_packet_path": str(hydrate_path),
        "hydrate_packet_status": "CURRENT" if not hydrate_err else hydrate_err,
        "portal_runtime_model": portal_model,
        "paper_trade_golden_path_v1": paper_golden,
        "paper_trade_golden_path": paper_golden,
        "recovery_plan": recovery_plan,
        "recent_actions": recent_actions,
        "status_badges": _verified_runtime_status_badges_v1(
            model_status=model_status,
            graph_status=graph_status,
            portal_status=portal_status,
            evidence_hash_status=evidence_hash_status,
            runtime_readiness_status=str(portal_model.get("runtime_readiness_status") or "UNKNOWN"),
        ),
        "copy_payloads": {
            "hydrate_packet": hydrate_text,
            "hydrate_packet_error": hydrate_err or "",
            "blocker_summary": blocker_summary,
            "latest_action_result": latest_action_result,
        },
        "evidence_ledger_summary": {
            "status": evidence_hash_status,
            "summary": (ledger_payload or {}).get("hash_verification_summary") if isinstance(ledger_payload, dict) else {},
        },
        "derivation_source": portal_model.get("derivation_source"),
        "readiness_inference_policy": portal_model.get("readiness_inference_policy"),
        "operator_message": "Portal is rendering verified graph output only.",
    }


def _portal_action_safety_policy_summary_v1() -> Dict[str, Any]:
    return {
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "manual_capture_policy_changed": False,
        "scoring_policy_changed": False,
        "routing_policy_changed": False,
        "scheduler_policy_changed": False,
        "allowed_action_ids": sorted(PORTAL_VERIFIED_RUNTIME_ACTION_ALLOWLIST_V1),
        "readiness_inference_policy": "NO_INDEPENDENT_PORTAL_READINESS_INFERENCE",
    }


def _portal_action_audit_path_v1(day_utc: str, truth_root: Optional[Path] = None) -> Path:
    root = (truth_root or _canonical_truth_root()).resolve()
    return root / "reports" / "aegis_portal_actions_v1" / day_utc / "portal_actions.v1.jsonl"


def _portal_action_excerpt_v1(value: Any, limit: int) -> str:
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    else:
        text = str(value or "")
    return text[-limit:]


def _portal_action_hash_file_v1(path: Path) -> Optional[str]:
    try:
        if not path.is_file():
            return None
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _portal_action_generated_artifacts_v1(action_id: str, day_utc: str, truth_root: Optional[Path] = None) -> List[str]:
    root = (truth_root or _canonical_truth_root()).resolve()
    graph_root = root / "reports" / "aegis_verified_runtime_graph_v1" / day_utc
    candidates: Dict[str, List[Path]] = {
        "run_audit": [
            graph_root / "verified_runtime_graph.v1.json",
            graph_root / "evidence_ledger.v1.json",
            graph_root / "portal_runtime_model.v1.json",
            graph_root / "chatgpt_hydrate_packet.v1.md",
            root / "reports" / "aegis_audit_handoff_v1" / day_utc / "aegis_audit_handoff.v1.json",
            root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_truth_kernel.v1.json",
        ],
        "hydrate_chatgpt": [graph_root / "chatgpt_hydrate_packet.v1.md"],
        "repair_context_readiness": [
            root / "reports" / "aegis_context_requirement_profile_v1" / day_utc / "context_requirement_profile.v1.json",
            root / "reports" / "aegis_market_context_provider_health_v1" / day_utc / "provider_health.v1.json",
            root / "reports" / "aegis_market_context_demand_v1" / day_utc / "market_context_demand.v1.json",
            root / "reports" / "event_market_snapshot_v1" / day_utc / "event_market_snapshot.v1.json",
            root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_truth_kernel.v1.json",
            graph_root / "verified_runtime_graph.v1.json",
            graph_root / "chatgpt_hydrate_packet.v1.md",
            root / "reports" / "aegis_audit_handoff_v1" / day_utc / "aegis_audit_handoff.txt",
        ],
    }
    paths = candidates.get(action_id, [])
    return [str(path) for path in paths if path.exists()]


def _portal_action_artifact_hashes_v1(paths: List[str]) -> Dict[str, str]:
    hashes: Dict[str, str] = {}
    for raw_path in paths:
        digest = _portal_action_hash_file_v1(Path(raw_path))
        if digest:
            hashes[raw_path] = digest
    return hashes


def _portal_action_run_id_v1(action_id: str, requested_day: str, started_at: str, request: Dict[str, Any]) -> str:
    material = json.dumps(
        {"action_id": action_id, "requested_day": requested_day, "started_at": started_at, "request": request},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return f"aegis_portal_action:{hashlib.sha256(material.encode('utf-8')).hexdigest()[:20]}"


def _portal_action_constrained_env_v1(requested_day: str, truth_root: Optional[Path] = None) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for key in ("PATH", "HOME", "LANG", "LC_ALL", "TZ"):
        value = os.environ.get(key)
        if value:
            env[key] = value
    env["TARGET_DAY"] = requested_day
    env["AEGIS_TRUTH_ROOT"] = str((truth_root or _canonical_truth_root()).resolve())
    env["CI"] = "1"
    return env


def _portal_action_validate_request_v1(body: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not isinstance(body, dict):
        return None, "REQUEST_BODY_MUST_BE_OBJECT"
    action_id = str(body.get("action_id") or "").strip()
    if action_id not in PORTAL_VERIFIED_RUNTIME_ACTION_ALLOWLIST_V1:
        return None, "UNKNOWN_ACTION"
    requested_day = str(body.get("requested_day") or body.get("day_utc") or body.get("day") or "").strip()
    if not _is_day_str(requested_day):
        return None, "INVALID_REQUESTED_DAY"
    raw_args = body.get("args") if isinstance(body.get("args"), dict) else {}
    args = dict(raw_args)
    # Backward-compatible top-level fields are normalized into args, but validation remains structured.
    for key in ("from_day", "to_day", "claim"):
        if body.get(key) is not None and key not in args:
            args[key] = body.get(key)
    operator_context = body.get("operator_context") if isinstance(body.get("operator_context"), dict) else {}
    if body.get("operator_context") is not None and not isinstance(body.get("operator_context"), dict):
        return None, "INVALID_OPERATOR_CONTEXT"
    if action_id == "graph_diff":
        from_day = str(args.get("from_day") or "").strip()
        to_day = str(args.get("to_day") or requested_day).strip()
        if not _is_day_str(from_day) or not _is_day_str(to_day):
            return None, "INVALID_GRAPH_DIFF_DAYS"
        args["from_day"] = from_day
        args["to_day"] = to_day
    if action_id == "claim_lookup":
        claim = str(args.get("claim") or "").strip().lower()
        if claim not in PORTAL_VERIFIED_RUNTIME_CLAIMS_V1:
            return None, "INVALID_CLAIM"
        args["claim"] = claim
    return {
        "action_id": action_id,
        "requested_day": requested_day,
        "args": args,
        "operator_context": operator_context,
    }, None


def _portal_action_command_v1(action_id: str, requested_day: str, args: Dict[str, Any]) -> List[str]:
    if action_id == "run_audit":
        return ["npm", "run", "aegis:audit"]
    if action_id == "explain_blockers":
        return ["npm", "run", "aegis:query", "--", "why blocked?"]
    if action_id == "show_evidence":
        return ["npm", "run", "aegis:query", "--", "what evidence supports this claim?"]
    if action_id == "graph_diff":
        return [
            "npm",
            "run",
            "aegis:graph-diff",
            "--",
            "--from-day",
            str(args["from_day"]),
            "--to-day",
            str(args.get("to_day") or requested_day),
        ]
    if action_id == "hydrate_chatgpt":
        return ["npm", "run", "aegis:chatgpt:hydrate"]
    if action_id == "repair_context_readiness":
        return ["npm", "run", "aegis:repair-context-readiness"]
    if action_id == "claim_lookup":
        return ["npm", "run", "aegis:query", "--", f"claim: {str(args['claim']).strip().lower()}"]
    raise ValueError(f"unsupported action_id: {action_id}")


def _portal_action_envelope_v1(
    *,
    action_id: str,
    run_id: str,
    requested_day: str,
    started_at: str,
    completed_at: str,
    exit_code: int,
    status: str,
    stdout_excerpt: str = "",
    stderr_excerpt: str = "",
    generated_artifact_paths: Optional[List[str]] = None,
    command_argv: Optional[List[str]] = None,
    user_message: Optional[str] = None,
) -> Dict[str, Any]:
    safety = _portal_action_safety_policy_summary_v1()
    envelope: Dict[str, Any] = {
        "ok": status == "PASS",
        "action_id": action_id,
        "run_id": run_id,
        "requested_day": requested_day,
        "started_at": started_at,
        "completed_at": completed_at,
        "exit_code": exit_code,
        "status": status,
        "stdout_excerpt": stdout_excerpt,
        "stderr_excerpt": stderr_excerpt,
        "generated_artifact_paths": generated_artifact_paths or [],
        "command_argv": command_argv or [],
        "safety_policy_summary": safety,
        "user_message": user_message or f"Verified runtime action {status.lower()}.",
        # Compatibility with the previous Portal action response shape.
        "result_status": status,
        "day_utc": requested_day,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "returncode": exit_code,
        "stdout_tail": stdout_excerpt,
        "stderr_tail": stderr_excerpt,
        "broker_submit_transmit_allowed": safety["broker_submit_transmit_allowed"],
        "autonomous_execution_allowed": safety["autonomous_execution_allowed"],
        "trade_advice_allowed": safety["trade_advice_allowed"],
        "manual_capture_policy_changed": safety["manual_capture_policy_changed"],
    }
    return envelope


def _portal_action_append_audit_v1(
    *,
    request: Dict[str, Any],
    command_argv: List[str],
    result_envelope: Dict[str, Any],
    truth_root: Optional[Path] = None,
) -> None:
    requested_day = str(result_envelope.get("requested_day") or date.today().isoformat())
    audit_path = _portal_action_audit_path_v1(requested_day, truth_root=truth_root)
    artifact_paths = [str(path) for path in result_envelope.get("generated_artifact_paths") or []]
    record = {
        "schema_id": "aegis_portal_action_record",
        "schema_version": "v1",
        "recorded_at": _utc_now_iso(),
        "run_id": result_envelope.get("run_id"),
        "action_id": result_envelope.get("action_id"),
        "user": ((request.get("operator_context") or {}).get("user") if isinstance(request.get("operator_context"), dict) else None),
        "operator": request.get("operator_context") if isinstance(request.get("operator_context"), dict) else {},
        "request": request,
        "command_argv": command_argv,
        "result_envelope": result_envelope,
        "artifact_hashes": _portal_action_artifact_hashes_v1(artifact_paths),
    }
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True, separators=(",", ":"), default=str) + "\n")


def _run_verified_runtime_action_v1(
    body: Dict[str, Any],
    *,
    command_runner: Optional[Any] = None,
    portal_model_loader: Optional[Any] = None,
    truth_root: Optional[Path] = None,
) -> Dict[str, Any]:
    started = _utc_now_iso()
    parsed, validation_error = _portal_action_validate_request_v1(body)
    fallback_day = str(body.get("requested_day") or body.get("day_utc") or body.get("day") or date.today().isoformat()) if isinstance(body, dict) else date.today().isoformat()
    requested_day = fallback_day if _is_day_str(fallback_day) else date.today().isoformat()
    action_id = str(body.get("action_id") or "UNKNOWN").strip() if isinstance(body, dict) else "UNKNOWN"
    request_for_audit: Dict[str, Any] = parsed or {
        "action_id": action_id,
        "requested_day": requested_day,
        "args": body.get("args") if isinstance(body, dict) and isinstance(body.get("args"), dict) else {},
        "operator_context": body.get("operator_context") if isinstance(body, dict) and isinstance(body.get("operator_context"), dict) else {},
    }
    run_id = _portal_action_run_id_v1(action_id, requested_day, started, request_for_audit)
    if validation_error:
        envelope = _portal_action_envelope_v1(
            action_id=action_id,
            run_id=run_id,
            requested_day=requested_day,
            started_at=started,
            completed_at=_utc_now_iso(),
            exit_code=2,
            status="REJECTED",
            stderr_excerpt=validation_error,
            command_argv=[],
            user_message="Verified runtime action request rejected by schema validation.",
        )
        _portal_action_append_audit_v1(request=request_for_audit, command_argv=[], result_envelope=envelope, truth_root=truth_root)
        return envelope

    assert parsed is not None
    action_id = parsed["action_id"]
    requested_day = parsed["requested_day"]
    run_id = _portal_action_run_id_v1(action_id, requested_day, started, parsed)
    command = _portal_action_command_v1(action_id, requested_day, parsed["args"])
    loader = portal_model_loader or _load_verified_runtime_portal_model_v1
    model_envelope = loader(requested_day)
    model_status = str((model_envelope or {}).get("model_status") or "MISSING")
    if action_id not in {"run_audit", "repair_context_readiness"} and model_status != "READY":
        envelope = _portal_action_envelope_v1(
            action_id=action_id,
            run_id=run_id,
            requested_day=requested_day,
            started_at=started,
            completed_at=_utc_now_iso(),
            exit_code=3,
            status="BLOCKED",
            stderr_excerpt=f"Portal runtime model is {model_status}; run_audit is the only enabled action.",
            command_argv=command,
            user_message="Verified runtime action blocked until the Portal runtime model is current.",
        )
        _portal_action_append_audit_v1(request=parsed, command_argv=command, result_envelope=envelope, truth_root=truth_root)
        return envelope

    runner = command_runner or subprocess.run
    env = _portal_action_constrained_env_v1(requested_day, truth_root=truth_root)
    try:
        completed = runner(
            command,
            cwd=str(REPO_ROOT),
            env=env,
            text=True,
            capture_output=True,
            timeout=PORTAL_VERIFIED_RUNTIME_ACTION_TIMEOUT_SECONDS_V1,
            check=False,
            shell=False,
        )
        exit_code = int(getattr(completed, "returncode", 1))
        stdout = _portal_action_excerpt_v1(getattr(completed, "stdout", ""), 8000)
        stderr = _portal_action_excerpt_v1(getattr(completed, "stderr", ""), 4000)
        status = "PASS" if exit_code == 0 else "FAILED"
    except subprocess.TimeoutExpired as exc:
        exit_code = 124
        stdout = _portal_action_excerpt_v1(exc.stdout, 8000)
        stderr = _portal_action_excerpt_v1(exc.stderr, 4000)
        status = "TIMEOUT"

    artifacts = _portal_action_generated_artifacts_v1(action_id, requested_day, truth_root=truth_root)
    envelope = _portal_action_envelope_v1(
        action_id=action_id,
        run_id=run_id,
        requested_day=requested_day,
        started_at=started,
        completed_at=_utc_now_iso(),
        exit_code=exit_code,
        status=status,
        stdout_excerpt=stdout,
        stderr_excerpt=stderr,
        generated_artifact_paths=artifacts,
        command_argv=command,
        user_message="Verified runtime action completed." if status == "PASS" else "Verified runtime action did not pass.",
    )
    _portal_action_append_audit_v1(request=parsed, command_argv=command, result_envelope=envelope, truth_root=truth_root)
    return envelope

def _report_artifact_exists_v1(root: Path, family: str, day_utc: str, filename: str) -> bool:
    family_day_root = (root / "reports" / family / day_utc).resolve()
    if not family_day_root.exists():
        return False
    direct = family_day_root / filename
    if direct.exists():
        return True
    try:
        return any(path.is_file() for path in family_day_root.rglob(filename))
    except Exception:
        return False


def _operator_snapshot_has_session_content_v1(root: Path, day_utc: str) -> bool:
    snapshot_path = (root / "reports" / "operator_state_snapshot_v1" / day_utc / "operator_state_snapshot.v1.json").resolve()
    payload, _err = _safe_read_json(snapshot_path)
    if not isinstance(payload, dict):
        return False
    if str(payload.get("current_truth_status") or "").upper() == "READ_ONLY_PRIOR_DAY_FALLBACK":
        return False
    status = str(payload.get("current_truth_status") or payload.get("status") or "").upper()
    current_day = payload.get("current_day_status") if isinstance(payload.get("current_day_status"), dict) else {}
    funnel = current_day.get("candidate_funnel_projection") if isinstance(current_day.get("candidate_funnel_projection"), dict) else {}
    raw_count = int(funnel.get("raw_candidate_count") or 0)
    certified = funnel.get("certified_universe") if isinstance(funnel.get("certified_universe"), dict) else {}
    certified_count = int(certified.get("symbol_count") or funnel.get("certified_universe_symbol_count") or 0)
    return status in {"CURRENT", "FINAL_EOD_READY", "INTRADAY_OPERATIONAL_READY", "CERTIFIED"} and (raw_count > 0 or certified_count > 0)


def _operator_day_has_valid_session_artifact_v1(root: Path, day_utc: str) -> bool:
    if _report_artifact_exists_v1(root, "candidate_consumption_audit_v1", day_utc, "candidate_consumption_audit.v1.json"):
        return True
    if _report_artifact_exists_v1(root, "aegis_lite_eod_report_v1", day_utc, "aegis_lite_eod_report.v1.json"):
        return True
    if _operator_snapshot_has_session_content_v1(root, day_utc):
        return True
    return False


def _latest_valid_operator_session_day_v1(*, root: Optional[Path] = None, max_day: Optional[str] = None) -> str:
    truth_root = (root or _canonical_truth_root()).resolve()
    ceiling = max_day if isinstance(max_day, str) and _is_day_str(max_day) else date.today().isoformat()
    days = set()
    for family in (
        "candidate_consumption_audit_v1",
        "aegis_lite_eod_report_v1",
        "operator_state_snapshot_v1",
    ):
        days.update(day for day in _list_day_dirs((truth_root / "reports" / family).resolve()) if day <= ceiling)
    valid_days = sorted(day for day in days if _operator_day_has_valid_session_artifact_v1(truth_root, day))
    return valid_days[-1] if valid_days else ""


def _operator_day_resolution_v1(raw_day: Optional[str] = None, *, today_day: Optional[str] = None) -> Dict[str, Any]:
    if isinstance(raw_day, str) and _is_day_str(raw_day):
        return {
            "requested_day": raw_day,
            "resolved_day": raw_day,
            "fallback_applied": False,
            "explicit_day_override": True,
            "resolution_reason": "EXPLICIT_DAY_OVERRIDE",
        }
    today_text = today_day if isinstance(today_day, str) and _is_day_str(today_day) else date.today().isoformat()
    try:
        trading_day = is_us_equities_trading_day_v1(today_text)
    except Exception:
        trading_day = datetime.strptime(today_text, "%Y-%m-%d").date().weekday() < 5
    if trading_day:
        return {
            "requested_day": today_text,
            "resolved_day": today_text,
            "fallback_applied": False,
            "explicit_day_override": False,
            "resolution_reason": "CURRENT_TRADING_DAY",
        }
    try:
        latest_trading_day = latest_us_equities_trading_day_on_or_before_v1(today_text)
    except Exception:
        latest_trading_day = today_text
    latest_valid_day = _latest_valid_operator_session_day_v1(max_day=latest_trading_day)
    resolved_day = latest_valid_day or latest_trading_day
    return {
        "requested_day": today_text,
        "resolved_day": resolved_day,
        "latest_valid_operational_session": latest_valid_day,
        "fallback_applied": resolved_day != today_text,
        "explicit_day_override": False,
        "resolution_reason": "NON_TRADING_DAY_LATEST_VALID_SESSION" if resolved_day != today_text else "NON_TRADING_DAY_NO_FALLBACK_AVAILABLE",
    }


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


def _resolved_operator_api_day_v1(requested_day: Optional[str], family: str) -> str:
    """Resolve the artifact day for API reads without overriding explicit day requests.

    current_operator_truth.source_day is a fallback signal for default/current views.  It
    must not replace an explicit ?day=YYYY-MM-DD request, because that makes the
    Portal explain a stale prior day while the UI shows a requested day.
    """
    if isinstance(requested_day, str) and _is_day_str(requested_day):
        return requested_day
    current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
    return str(current_truth.get("source_day") or _projection_day_for_report(requested_day, family))


def _operator_shared_fact_day_v1(requested_day: Optional[str], family: str) -> str:
    """Resolve browser-rendered shared facts from their latest current-day fact source.

    Operator shell fact screens must not inherit current_operator_truth.source_day
    when no explicit day is selected. On non-trading days that fallback can point to
    an older valid session while current-day position/performance artifacts exist.
    """
    if isinstance(requested_day, str) and _is_day_str(requested_day):
        return requested_day
    return _projection_day_for_report(None, family)


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



def _candidate_projection_staleness_guard_v1(*, truth_root: Path, day_utc: str, canonical: Dict[str, Any], projection: Dict[str, Any]) -> Dict[str, Any]:
    if not projection:
        return {}
    guarded = dict(projection)
    canonical_generated_at = str(canonical.get("generated_at_utc") or canonical.get("generated_at") or guarded.get("canonical_generated_at") or "")
    specs = {
        "candidate_generation_diagnostics": ("aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json"),
        "candidate_contracts": ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json"),
        "candidate_review_packet": ("aegis_candidate_review_packet_v1", "candidate_review_packet.v1.json"),
        "paper_review_queue": ("aegis_paper_review_queue_v1", "paper_review_queue.v1.json"),
    }
    latest_sources = []
    for source_key, (family, filename) in specs.items():
        path, payload = latest_json_v1(truth_root, family, day_utc, filename)
        payload = payload if isinstance(payload, dict) else {}
        generated_at = str(payload.get("generated_at_utc") or payload.get("generated_at") or payload.get("timestamp_utc") or "")
        latest_sources.append({
            "source": source_key,
            "path": str(path or ""),
            "generated_at": generated_at,
            "hash": _sha256_file_v1(path) if path else "",
            "day_utc": str(payload.get("day_utc") or ""),
            "schema_id": str(payload.get("schema_id") or ""),
            "found": bool(path and payload),
        })
    latest_generated = sorted([row["generated_at"] for row in latest_sources if row.get("generated_at")])
    max_source_generated_at = latest_generated[-1] if latest_generated else ""
    mismatch_reasons = list(guarded.get("mismatch_reasons") or [])
    for row in latest_sources:
        if row.get("found") and row.get("day_utc") and row.get("day_utc") != day_utc:
            mismatch_reasons.append(f"WRONG_DAY:{row['source']}:{row['day_utc']}!={day_utc}")
        if row.get("found") and day_utc not in Path(str(row.get("path") or "")).parts:
            mismatch_reasons.append(f"WRONG_DAY_PATH:{row['source']}:{row['path']}!={day_utc}")
    if canonical_generated_at and max_source_generated_at and canonical_generated_at < max_source_generated_at:
        stale_reason = f"CANONICAL_OLDER_THAN_CANDIDATE_SOURCES:{canonical_generated_at}<{max_source_generated_at}"
        if stale_reason not in mismatch_reasons:
            mismatch_reasons.append(stale_reason)
        guarded["projection_status"] = "CANONICAL_PROJECTION_STALE"
    elif mismatch_reasons and guarded.get("projection_status") == "AVAILABLE":
        guarded["projection_status"] = "PROJECTION_MISMATCH"
    guarded["mismatch_reasons"] = mismatch_reasons
    guarded["day_path_invariant_ok"] = not any(str(reason).startswith("WRONG_DAY_PATH:") for reason in mismatch_reasons)
    guarded["latest_source_artifacts"] = latest_sources
    guarded["source_max_generated_at"] = max_source_generated_at or guarded.get("source_max_generated_at") or ""
    guarded["canonical_generated_at"] = canonical_generated_at
    return guarded


def _artifact_backed_candidate_ui_projection_v1(*, truth_root: Path, day_utc: str, canonical: Dict[str, Any]) -> Dict[str, Any]:
    projection = canonical.get("candidate_ui_projection") if isinstance(canonical.get("candidate_ui_projection"), dict) else {}
    if projection:
        return projection
    root = Path(truth_root).expanduser().resolve()
    try:
        sources = load_canonical_operator_sources_v1(root, day_utc)
        payloads = {key: row["payload"] for key, row in sources.items()}
        return build_candidate_ui_projection_v1(
            payloads=payloads,
            sources=sources,
            day_utc=day_utc,
            canonical_generated_at=str(canonical.get("generated_at_utc") or canonical.get("generated_at") or ""),
        )
    except Exception as exc:
        diagnostics_path = (root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day_utc / "candidate_generation_diagnostics.v1.json").resolve()
        contracts_path = (root / "reports" / "aegis_candidate_contracts_v1" / day_utc / "candidate_contracts.v1.json").resolve()
        queue_path = (root / "reports" / "aegis_paper_review_queue_v1" / day_utc / "paper_review_queue.v1.json").resolve()
        return {
            "schema_id": "aegis_candidate_ui_projection",
            "schema_version": "v1",
            "artifact_id": f"aegis_candidate_ui_projection:{day_utc}:missing",
            "day_utc": day_utc,
            "projection_status": "MISSING_PROJECTION",
            "run_visibility_status": "MISSING_PROJECTION",
            "error": str(exc),
            "truth_root": str(root),
            "expected_artifacts": [
                {
                    "source": "candidate_generation_diagnostics",
                    "path": str(diagnostics_path),
                    "exists": diagnostics_path.exists(),
                    "repair_command": f"TARGET_DAY={day_utc} npm run aegis:candidate-diagnostics",
                },
                {
                    "source": "candidate_contracts",
                    "path": str(contracts_path),
                    "exists": contracts_path.exists(),
                    "repair_command": f"TARGET_DAY={day_utc} npm run aegis:candidate-contracts",
                },
                {
                    "source": "paper_review_queue",
                    "path": str(queue_path),
                    "exists": queue_path.exists(),
                    "repair_command": f"TARGET_DAY={day_utc} npm run aegis:paper:review-queue",
                },
            ],
            "repair_commands": [
                f"TARGET_DAY={day_utc} npm run aegis:candidate-contracts",
                f"TARGET_DAY={day_utc} npm run aegis:candidate-diagnostics",
                f"TARGET_DAY={day_utc} npm run aegis:paper:review-queue",
                f"TARGET_DAY={day_utc} npm run aegis:canonical-operator-state",
            ],
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        }


def _attach_candidate_ui_projection_v1(payload: Dict[str, Any], *, truth_root: Path, day_utc: str, operational_day_source: str) -> Dict[str, Any]:
    enriched = dict(payload)
    canonical_path, canonical = latest_json_v1(
        truth_root,
        "aegis_canonical_operator_state_v1",
        day_utc,
        "canonical_operator_state.v1.json",
    )
    canonical = canonical if isinstance(canonical, dict) else {}
    projection = _artifact_backed_candidate_ui_projection_v1(truth_root=truth_root, day_utc=day_utc, canonical=canonical)
    projection = _candidate_projection_staleness_guard_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        canonical=canonical,
        projection=projection,
    )
    debug = _candidate_projection_debug_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        canonical_path=canonical_path,
        canonical=canonical,
        projection=projection,
        operational_day_source=operational_day_source,
    )
    enriched["candidate_ui_projection"] = projection
    enriched["candidate_projection_debug"] = debug
    enriched["has_candidate_ui_projection"] = bool(debug.get("has_candidate_ui_projection"))
    if projection:
        enriched["displayed_artifact_day"] = day_utc
        enriched["source_day"] = day_utc
        if isinstance(projection.get("run_summary"), dict):
            enriched["latest_run_summary"] = projection.get("run_summary")
    enriched["truth_root"] = str(Path(truth_root).expanduser().resolve())
    enriched["canonical_operator_state"] = canonical
    surface_path = surface_readiness_path_v1(truth_root=truth_root, day_utc=day_utc)
    surface_payload, _surface_err = _safe_read_json(surface_path)
    if not isinstance(surface_payload, dict) or not surface_payload:
        surface_payload = build_surface_readiness_v1(truth_root=truth_root, day_utc=day_utc)
        surface_path = write_surface_readiness_v1(truth_root=truth_root, day_utc=day_utc, payload=surface_payload)
    enriched["surface_readiness"] = surface_payload
    enriched["surface_readiness_by_id"] = surface_payload.get("surface_by_id") if isinstance(surface_payload.get("surface_by_id"), dict) else {}
    scorecard_path, scorecard_payload = latest_json_v1(
        truth_root,
        "aegis_research_daily_scorecard_v1",
        day_utc,
        "research_daily_scorecard.v1.json",
    )
    if isinstance(scorecard_payload, dict) and scorecard_payload:
        enriched["research_daily_scorecard"] = scorecard_payload

    source_paths = enriched.get("source_paths") if isinstance(enriched.get("source_paths"), dict) else {}
    enriched["source_paths"] = {
        **source_paths,
        "canonical_operator_state": str(canonical_path or debug.get("canonical_operator_state_path") or ""),
        "candidate_generation_diagnostics": str(debug.get("candidate_diagnostics_path") or ""),
        "candidate_contracts": str(debug.get("candidate_contracts_path") or ""),
        "candidate_review_packet": str(debug.get("candidate_review_packet_path") or ""),
        "paper_review_queue": str(debug.get("paper_review_queue_path") or ""),
        "surface_readiness": str(surface_path),
        "research_daily_scorecard": str(scorecard_path or ""),
    }
    return enriched


def _sha256_file_v1(path: Path | None) -> str:
    if not path:
        return ""
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return ""


def _candidate_projection_debug_v1(
    *,
    truth_root: Path,
    day_utc: str,
    canonical_path: Path | None,
    canonical: Dict[str, Any],
    projection: Dict[str, Any],
    operational_day_source: str = "",
) -> Dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    projection = projection if isinstance(projection, dict) else {}
    canonical = canonical if isinstance(canonical, dict) else {}
    def _projected_or_expected(key: str, family: str, filename: str) -> str:
        value = str(projection.get(key) or "")
        if value:
            return value
        return str((root / "reports" / family / day_utc / filename).resolve())

    canonical_text_path = str(canonical_path or (root / "reports" / "aegis_canonical_operator_state_v1" / day_utc / "canonical_operator_state.v1.json").resolve())
    diagnostics_path = _projected_or_expected("candidate_diagnostics_path", "aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json")
    contracts_path = _projected_or_expected("candidate_contracts_path", "aegis_candidate_contracts_v1", "candidate_contracts.v1.json")
    packet_path = _projected_or_expected("candidate_review_packet_path", "aegis_candidate_review_packet_v1", "candidate_review_packet.v1.json")
    queue_path = _projected_or_expected("paper_review_queue_path", "aegis_paper_review_queue_v1", "paper_review_queue.v1.json")
    static_bundle_path = (Path(__file__).resolve().parents[1] / "static" / "operator_shell" / "pages" / "index.js").resolve()
    queue_audit_path = root / "reports" / "aegis_command_center_queue_audit_v1" / day_utc / "command_center_queue_audit.v1.json"
    queue_audit, _queue_audit_error = _safe_read_json(queue_audit_path)
    queue_audit = queue_audit if isinstance(queue_audit, dict) else {}
    queue_summary = queue_audit.get("summary") if isinstance(queue_audit.get("summary"), dict) else {}
    queue_counts = queue_audit.get("current_command_center_counts") if isinstance(queue_audit.get("current_command_center_counts"), dict) else {}
    repaired_awaiting_review = int(queue_counts.get("awaiting_review_metric") or queue_summary.get("operator_action_required_count") or 0)
    raw_projection_awaiting_review = int(projection.get("awaiting_review_count") or 0)
    raw_projection_reviewable = int(projection.get("reviewable_candidate_count") or 0)
    has_projection = bool(projection)
    canonical_has_projection = isinstance(canonical.get("candidate_ui_projection"), dict) and bool(canonical.get("candidate_ui_projection"))
    day_paths = {
        "canonical_operator_state": canonical_text_path,
        "candidate_generation_diagnostics": diagnostics_path,
        "candidate_contracts": contracts_path,
        "candidate_review_packet": packet_path,
        "paper_review_queue": queue_path,
    }
    day_path_violations = [
        {"source": source, "path": path, "expected_day_utc": day_utc}
        for source, path in day_paths.items()
        if Path(path).exists() and day_utc not in Path(path).parts
    ]
    return {
        "schema_id": "aegis_portal_projection_debug",
        "schema_version": "v1",
        "artifact_id": f"aegis_portal_projection_debug:{day_utc}",
        "day_utc": day_utc,
        "truth_root": str(root),
        "app_root": str(REPO_ROOT),
        "release_path": str(Path(__file__).resolve()),
        "api_server_cwd": os.getcwd(),
        "static_bundle_path": str(static_bundle_path),
        "static_bundle_hash": _sha256_file_v1(static_bundle_path),
        "operational_day_source": operational_day_source or "operator-cockpit-request",
        "canonical_operator_state_path": canonical_text_path,
        "canonical_operator_state_exists": Path(canonical_text_path).exists(),
        "canonical_operator_state_hash": _sha256_file_v1(Path(canonical_text_path)),
        "canonical_generated_at": str(canonical.get("generated_at_utc") or canonical.get("generated_at") or ""),
        "has_candidate_ui_projection": has_projection,
        "canonical_has_candidate_ui_projection": canonical_has_projection,
        "candidate_diagnostics_path": diagnostics_path,
        "candidate_diagnostics_exists": Path(diagnostics_path).exists(),
        "candidate_contracts_path": contracts_path,
        "candidate_contracts_exists": Path(contracts_path).exists(),
        "candidate_review_packet_path": packet_path,
        "candidate_review_packet_exists": Path(packet_path).exists(),
        "paper_review_queue_path": queue_path,
        "paper_review_queue_exists": Path(queue_path).exists(),
        "day_path_invariant_ok": not day_path_violations,
        "day_path_invariant_violations": day_path_violations,
        "candidate_contract_count": int(projection.get("candidate_contract_count") or 0),
        "awaiting_review_count": repaired_awaiting_review,
        "reviewable_candidate_count": repaired_awaiting_review,
        "raw_projection_awaiting_review_count": raw_projection_awaiting_review,
        "raw_projection_reviewable_candidate_count": raw_projection_reviewable,
        "command_center_queue_audit_path": str(queue_audit_path),
        "command_center_operator_action_required_count": int(queue_summary.get("operator_action_required_count") or 0),
        "diagnostics_status": str(projection.get("diagnostics_status") or "UNKNOWN"),
        "paper_review_queue_status": str(projection.get("paper_review_queue_status") or "UNKNOWN"),
        "projection_status": str(projection.get("projection_status") or "MISSING"),
        "policy_gates_changed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def _git_commit_hash_v1() -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
            text=True,
            capture_output=True,
            check=False,
            timeout=3,
            shell=False,
        )
        if completed.returncode == 0:
            return str(completed.stdout or "").strip()
    except Exception:
        pass
    return ""


def _runtime_debug_payload_v1(day_utc: str = "") -> Dict[str, Any]:
    requested_day = day_utc if _is_day_str(day_utc) else _operator_truth_day(None)
    current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=requested_day)
    operational_day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "aegis_canonical_operator_state_v1"))
    canonical_path, canonical = latest_json_v1(
        GLOBAL_TRUTH_ROOT,
        "aegis_canonical_operator_state_v1",
        operational_day,
        "canonical_operator_state.v1.json",
    )
    canonical = canonical if isinstance(canonical, dict) else {}
    projection = _artifact_backed_candidate_ui_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=operational_day, canonical=canonical)
    debug = _candidate_projection_debug_v1(
        truth_root=GLOBAL_TRUTH_ROOT,
        day_utc=operational_day,
        canonical_path=canonical_path,
        canonical=canonical,
        projection=projection,
        operational_day_source=str(current_truth.get("source") or "runtime-debug"),
    )
    return {
        "ok": True,
        "service_pid": os.getpid(),
        "loaded_module_path": str(Path(__file__).resolve()),
        "git_commit_hash": _git_commit_hash_v1(),
        "runtime_cwd": os.getcwd(),
        "truth_root": str(Path(GLOBAL_TRUTH_ROOT).resolve()),
        "sleeve_truth_root": str(Path(SLEEVE_TRUTH_ROOT).resolve()),
        "configured_truth_root": str(Path(TRUTH_ROOT).resolve()),
        "operational_day": operational_day,
        "requested_day": requested_day,
        "current_truth_source_day": str(current_truth.get("source_day") or ""),
        "current_truth_displayed_artifact_day": str(current_truth.get("displayed_artifact_day") or ""),
        "canonical_state_path": debug.get("canonical_operator_state_path"),
        "canonical_state_hash": debug.get("canonical_operator_state_hash"),
        "canonical_generated_at": debug.get("canonical_generated_at"),
        "candidate_diagnostics_path": debug.get("candidate_diagnostics_path"),
        "candidate_diagnostics_hash": _sha256_file_v1(Path(str(debug.get("candidate_diagnostics_path") or ""))),
        "candidate_diagnostics_exists": debug.get("candidate_diagnostics_exists"),
        "paper_queue_path": debug.get("paper_review_queue_path"),
        "paper_queue_hash": _sha256_file_v1(Path(str(debug.get("paper_review_queue_path") or ""))),
        "paper_queue_exists": debug.get("paper_review_queue_exists"),
        "candidate_contract_count": debug.get("candidate_contract_count"),
        "awaiting_review_count": debug.get("awaiting_review_count"),
        "reviewable_candidate_count": debug.get("reviewable_candidate_count"),
        "diagnostics_status": debug.get("diagnostics_status"),
        "paper_review_queue_status": debug.get("paper_review_queue_status"),
        "projection_present": debug.get("has_candidate_ui_projection"),
        "candidate_projection_debug": debug,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def _log_operator_cockpit_dump_v1(payload: Dict[str, Any]) -> None:
    debug = payload.get("candidate_projection_debug") if isinstance(payload.get("candidate_projection_debug"), dict) else {}
    if not debug:
        projection = payload.get("candidate_ui_projection") if isinstance(payload.get("candidate_ui_projection"), dict) else {}
        debug = _candidate_projection_debug_v1(
            truth_root=GLOBAL_TRUTH_ROOT,
            day_utc=str(payload.get("day_utc") or ""),
            canonical_path=Path(str((payload.get("source_paths") or {}).get("canonical_operator_state") or "")) if isinstance(payload.get("source_paths"), dict) else None,
            canonical=payload.get("canonical_operator_state") if isinstance(payload.get("canonical_operator_state"), dict) else {},
            projection=projection,
            operational_day_source="operator-cockpit-log-fallback",
        )
    log_payload = {
        "event": "aegis_operator_cockpit_projection_dump",
        "service_pid": os.getpid(),
        "loaded_module_path": str(Path(__file__).resolve()),
        "operational_day": payload.get("day_utc"),
        "resolved_truth_root": debug.get("truth_root"),
        "canonical_operator_state_path": debug.get("canonical_operator_state_path"),
        "canonical_operator_state_exists": debug.get("canonical_operator_state_exists"),
        "canonical_generated_at": debug.get("canonical_generated_at"),
        "canonical_operator_state_hash": debug.get("canonical_operator_state_hash"),
        "candidate_ui_projection_exists": debug.get("has_candidate_ui_projection"),
        "candidate_diagnostics_path": debug.get("candidate_diagnostics_path"),
        "candidate_diagnostics_exists": debug.get("candidate_diagnostics_exists"),
        "paper_review_queue_path": debug.get("paper_review_queue_path"),
        "paper_review_queue_exists": debug.get("paper_review_queue_exists"),
        "candidate_contract_count": debug.get("candidate_contract_count"),
        "awaiting_review_count": debug.get("awaiting_review_count"),
    }
    sys.stderr.write("AEGIS_OPERATOR_COCKPIT_DUMP " + json.dumps(log_payload, sort_keys=True, default=str) + "\n")


def _candidate_match_keys_v1(row: Dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for field in ("candidate_id", "candidate_contract_id", "symbol"):
        value = str(row.get(field) or "").strip()
        if value:
            keys.add(f"{field}:{value.upper()}")
    return keys


def _signal_boundary_rows_by_status_v1(boundary: Dict[str, Any], status: str) -> List[Dict[str, Any]]:
    rows = boundary.get("boundary_rows") if isinstance(boundary.get("boundary_rows"), list) else []
    expected = status.upper()
    return [row for row in rows if isinstance(row, dict) and str(row.get("boundary_status") or "").upper() == expected]


def _filter_positions_output_candidates_v1(current: List[Dict[str, Any]], boundary: Dict[str, Any]) -> List[Dict[str, Any]]:
    signal_present_rows = _signal_boundary_rows_by_status_v1(boundary, "SIGNAL_EVIDENCE_PRESENT")
    allowed_keys: set[str] = set()
    for row in signal_present_rows:
        allowed_keys.update(_candidate_match_keys_v1(row))
    if not allowed_keys:
        return []
    return [row for row in current if _candidate_match_keys_v1(row) & allowed_keys]



def _duplicate_candidate_rows_by_key_v1(duplicate_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    rows = duplicate_payload.get("duplicate_rows") if isinstance(duplicate_payload.get("duplicate_rows"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in _candidate_match_keys_v1(row):
            out[key] = row
    return out


def _apply_duplicate_candidate_boundary_v1(current: List[Dict[str, Any]], duplicate_payload: Dict[str, Any]) -> tuple[List[Dict[str, Any]], int]:
    if not isinstance(duplicate_payload, dict) or not duplicate_payload:
        return current, 0
    duplicate_by_key = _duplicate_candidate_rows_by_key_v1(duplicate_payload)
    if not duplicate_by_key:
        return current, 0
    out: List[Dict[str, Any]] = []
    suppressed = 0
    for row in current:
        duplicate_row: Dict[str, Any] = {}
        for key in _candidate_match_keys_v1(row):
            duplicate_row = duplicate_by_key.get(key, {})
            if duplicate_row:
                break
        if not duplicate_row:
            out.append(row)
            continue
        enriched = {
            **row,
            "duplicate_scope": duplicate_row.get("duplicate_scope"),
            "duplicate_classification": duplicate_row.get("duplicate_classification"),
            "duplicate_operator_message": duplicate_row.get("operator_message"),
        }
        if duplicate_row.get("suppressed") is True:
            suppressed += 1
            continue
        duplicate_allowed = duplicate_row.get("allowed_actions") if isinstance(duplicate_row.get("allowed_actions"), list) else []
        duplicate_blocked = duplicate_row.get("blocked_actions") if isinstance(duplicate_row.get("blocked_actions"), list) else []
        if duplicate_allowed:
            enriched["allowed_actions"] = [str(item) for item in duplicate_allowed]
        if duplicate_blocked:
            enriched["blocked_actions"] = sorted(set([str(item) for item in enriched.get("blocked_actions", [])] + [str(item) for item in duplicate_blocked]))
        if duplicate_row.get("operator_message"):
            enriched["status_message"] = str(duplicate_row.get("operator_message"))
        enriched["actionable"] = bool(set(enriched.get("allowed_actions", [])) & {"CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER", "REVIEW_ADD_ON"})
        out.append(enriched)
    return out, suppressed

def _positions_row_minimal_v1(row: Dict[str, Any]) -> Dict[str, Any]:
    allowed = row.get("allowed_actions") if isinstance(row.get("allowed_actions"), list) else []
    blocked = row.get("blocked_actions") if isinstance(row.get("blocked_actions"), list) else []
    return {
        "candidate_id": str(row.get("candidate_id") or ""),
        "candidate_contract_id": str(row.get("candidate_contract_id") or row.get("candidate_id") or ""),
        "paper_session_id": str(row.get("paper_session_id") or ""),
        "symbol": str(row.get("symbol") or ""),
        "direction": str(row.get("direction") or ""),
        "entry_price": row.get("entry_price"),
        "stop_price": row.get("stop_price"),
        "target_price": row.get("target_price"),
        "quantity": row.get("quantity"),
        "missing_required_fields": [str(item) for item in row.get("missing_required_fields", [])] if isinstance(row.get("missing_required_fields"), list) else [],
        "candidate_readiness_status": str(row.get("candidate_readiness_status") or "READY"),
        "risk_amount": row.get("risk_amount"),
        "candidate_lifecycle_state": str(row.get("candidate_lifecycle_state") or "GENERATED"),
        "decision": str(row.get("decision") or row.get("candidate_lifecycle_state") or "GENERATED"),
        "capture_status": str(row.get("capture_status") or "Not reviewed"),
        "allowed_actions": [str(item) for item in allowed],
        "blocked_actions": [str(item) for item in blocked],
        "receipt_id": row.get("receipt_id"),
        "entry_receipt_id": row.get("entry_receipt_id"),
        "exit_receipt_id": row.get("exit_receipt_id"),
        "position_id": row.get("position_id"),
        "status_message": row.get("status_message"),
        "actionable": bool(row.get("actionable")),
    }



def _positions_candidate_capture_status_v1(
    *,
    paper_session_id: str,
    output_candidate_count: int,
    read_model_candidate_count: int,
    signal_evidence_present_count: int,
    current_session_lineage_count: int,
    rejected_intent_count: int,
    signal_boundary: Dict[str, Any],
    signal_boundary_read_error: str,
    projection_read_error: str,
) -> Dict[str, Any]:
    diagnostics_link = "/aegis-positions-diagnostics"
    affected_objects = []
    if signal_boundary_read_error:
        return {
            "status": "FAILED",
            "classification": "SIGNAL_EVIDENCE_MISSING",
            "summary": "Candidate generation status cannot be verified.",
            "explanation": "The signal evidence boundary artifact could not be read for this session.",
            "affected_count": current_session_lineage_count,
            "affected_objects": affected_objects,
            "next_action": "Open diagnostics and regenerate signal evidence boundary evidence.",
            "repair_command": "npm run aegis:signal-evidence-boundary-status",
            "diagnostics_link": diagnostics_link,
        }
    if projection_read_error:
        return {
            "status": "STALE",
            "classification": "STALE_READ_MODEL",
            "summary": "Positions read model is not current.",
            "explanation": "The candidate lifecycle projection could not be read, so the Positions page cannot display the current output candidates.",
            "affected_count": max(signal_evidence_present_count, current_session_lineage_count),
            "affected_objects": affected_objects,
            "next_action": "Regenerate the candidate lifecycle projection, then refresh Positions.",
            "repair_command": "npm run aegis:candidate-lifecycle",
            "diagnostics_link": diagnostics_link,
        }
    if read_model_candidate_count == 0 and signal_evidence_present_count > 0:
        return {
            "status": "STALE",
            "classification": "STALE_READ_MODEL",
            "summary": "Output candidates exist, but the Positions read model did not load them.",
            "explanation": f"Signal evidence reports {signal_evidence_present_count} output candidate row(s) for {paper_session_id}, while the Positions read model contains 0 displayable output candidate rows.",
            "affected_count": signal_evidence_present_count,
            "affected_objects": affected_objects,
            "next_action": "Regenerate candidate lifecycle and signal evidence projections, then refresh Positions.",
            "repair_command": "npm run aegis:candidate-lifecycle && npm run aegis:signal-evidence-boundary-status",
            "diagnostics_link": diagnostics_link,
        }
    if output_candidate_count > 0:
        return {
            "status": "READY",
            "classification": "OUTPUT_CANDIDATES_CAPTURED",
            "summary": f"{output_candidate_count} output candidate(s) captured for {paper_session_id}.",
            "explanation": "Final output-intent candidates passed the signal evidence boundary and are visible for operator review.",
            "affected_count": output_candidate_count,
            "affected_objects": affected_objects,
            "next_action": "Review Today's Candidates and record operator capture decisions where allowed.",
            "repair_command": None,
            "diagnostics_link": diagnostics_link,
        }
    if output_candidate_count == 0 and rejected_intent_count > 0 and signal_boundary:
        return {
            "status": "COMPLETE",
            "classification": "NO_CANDIDATES_QUALIFIED",
            "summary": "No output candidates qualified today.",
            "explanation": f"The session produced {rejected_intent_count} rejected-intent lineage row(s), but no final output-intent candidates passed signal evidence boundary.",
            "affected_count": rejected_intent_count,
            "affected_objects": affected_objects,
            "next_action": "None required for candidate capture; rejected-intent lineage is available in diagnostics/history.",
            "repair_command": None,
            "diagnostics_link": diagnostics_link,
        }
    return {
        "status": "UNKNOWN",
        "classification": "UNKNOWN_REQUIRES_DIAGNOSTICS",
        "summary": "Candidate generation status requires diagnostics.",
        "explanation": "Aegis could not determine whether no candidates qualified or a required upstream artifact is missing or stale.",
        "affected_count": current_session_lineage_count,
        "affected_objects": affected_objects,
        "next_action": "Open diagnostics and inspect signal evidence, contract, construction, and lifecycle boundary reports.",
        "repair_command": None,
        "diagnostics_link": diagnostics_link,
    }

def _positions_lightweight_payload_v1(*, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    started = time.perf_counter()
    projection_path = candidate_lifecycle_projection_path_v1(truth_root=truth_root, day_utc=day_utc)
    signal_boundary_path = signal_evidence_boundary_path_v1(truth_root=truth_root, day_utc=day_utc)
    duplicate_candidate_path = duplicate_candidate_path_v1(truth_root=truth_root, day_utc=day_utc)
    paper_auto_closure_path = truth_root / "reports" / "aegis_paper_outcome_auto_closure_v1" / day_utc / "paper_outcome_auto_closure.v1.json"
    outcome_registry_path = truth_root / "reports" / "aegis_outcome_registry_v1" / day_utc / "outcome_registry.v1.json"
    projection, read_error = _safe_read_json(projection_path)
    signal_boundary, signal_boundary_read_error = _safe_read_json(signal_boundary_path)
    duplicate_candidate, duplicate_candidate_read_error = _safe_read_json(duplicate_candidate_path)
    paper_auto_closure, paper_auto_closure_read_error = _safe_read_json(paper_auto_closure_path)
    outcome_registry, outcome_registry_read_error = _safe_read_json(outcome_registry_path)
    if not isinstance(projection, dict):
        projection = {}
    if not isinstance(signal_boundary, dict):
        signal_boundary = {}
    if not isinstance(duplicate_candidate, dict):
        duplicate_candidate = {}
    if not isinstance(paper_auto_closure, dict):
        paper_auto_closure = {}
    if not isinstance(outcome_registry, dict):
        outcome_registry = {}
    valuation_estimate, valuation_estimate_path = _operator_portfolio_valuation_estimate_payload_v1(truth_root, day_utc)
    current_all = [_positions_row_minimal_v1(row) for row in projection.get("current_session_candidates", []) if isinstance(row, dict)] if isinstance(projection.get("current_session_candidates"), list) else []
    current = _filter_positions_output_candidates_v1(current_all, signal_boundary)
    current, suppressed_duplicate_count = _apply_duplicate_candidate_boundary_v1(current, duplicate_candidate)
    open_positions = [_positions_row_minimal_v1(row) for row in projection.get("open_paper_positions", []) if isinstance(row, dict)] if isinstance(projection.get("open_paper_positions"), list) else []
    manual_review_queue = [row for row in paper_auto_closure.get("manual_review_queue", []) if isinstance(row, dict)] if isinstance(paper_auto_closure.get("manual_review_queue"), list) else []
    closed_position_ids = {
        str(row.get("position_id") or "").strip()
        for row in manual_review_queue
        if str(row.get("position_id") or "").strip()
    }
    closed_candidate_ids = {
        str(row.get("candidate_id") or row.get("candidate_contract_id") or "").strip()
        for row in manual_review_queue
        if str(row.get("candidate_id") or row.get("candidate_contract_id") or "").strip()
    }
    if closed_position_ids or closed_candidate_ids:
        open_positions = [
            row for row in open_positions
            if str(row.get("position_id") or "").strip() not in closed_position_ids
            and str(row.get("candidate_id") or row.get("candidate_contract_id") or "").strip() not in closed_candidate_ids
        ]
    open_positions = _attach_operator_position_estimates_v1(open_positions, valuation_estimate)
    closed_positions = [_positions_row_minimal_v1(row) for row in projection.get("closed_paper_positions", []) if isinstance(row, dict)] if isinstance(projection.get("closed_paper_positions"), list) else []
    auto_closure_summary = paper_auto_closure.get("summary") if isinstance(paper_auto_closure.get("summary"), dict) else {}
    outcome_summary = outcome_registry.get("summary") if isinstance(outcome_registry.get("summary"), dict) else {}
    open_observation_count = max(0, int(auto_closure_summary.get("hold_not_eligible_count") or outcome_summary.get("open_outcomes") or outcome_summary.get("open_outcome_count") or len(open_positions) or 0) - int(auto_closure_summary.get("manual_review_queue_count") or 0))
    closed_today_count = int(auto_closure_summary.get("auto_closed_count") or len(manual_review_queue) or 0)
    manual_review_count = int(auto_closure_summary.get("manual_review_queue_count") or len(manual_review_queue) or 0)
    summary = projection.get("summary") if isinstance(projection.get("summary"), dict) else {}
    signal_evidence_present_count = int(signal_boundary.get("signal_evidence_present_count") or 0)
    output_candidate_count = signal_evidence_present_count if signal_boundary else len(current)
    read_model_candidate_count = len(current)
    rejected_intent_count = int(signal_boundary.get("rejected_intent_count") or 0)
    current_session_lineage_count = int(signal_boundary.get("current_session_candidate_count") or summary.get("current_session_total") or len(current_all))
    paper_session_id = str(projection.get("paper_session_id") or signal_boundary.get("paper_session_id") or "unknown session")
    candidate_capture_status = _positions_candidate_capture_status_v1(
        paper_session_id=paper_session_id,
        output_candidate_count=output_candidate_count,
        read_model_candidate_count=read_model_candidate_count,
        signal_evidence_present_count=signal_evidence_present_count,
        current_session_lineage_count=current_session_lineage_count,
        rejected_intent_count=rejected_intent_count,
        signal_boundary=signal_boundary,
        signal_boundary_read_error=signal_boundary_read_error or "",
        projection_read_error=read_error or "",
    )
    payload = {
        "ok": bool(projection),
        "schema_id": "aegis_positions_operator_projection",
        "schema_version": "v1",
        "day_utc": str(projection.get("day_utc") or day_utc),
        "generated_at": projection.get("generated_at") or projection.get("generated_at_utc"),
        "paper_session_id": paper_session_id,
        "candidate_capture_status": candidate_capture_status,
        "summary": {
            "open_positions": open_observation_count,
            "open_paper_observations": open_observation_count,
            "closed_paper_outcomes_today": closed_today_count,
            "manual_review_queue": manual_review_count,
            "today_candidates": len(current),
            "output_candidates_captured": output_candidate_count,
            "current_session_lineage_count": current_session_lineage_count,
            "read_model_output_candidate_count": read_model_candidate_count,
            "signal_evidence_present_count": signal_evidence_present_count,
            "suppressed_duplicate_count": suppressed_duplicate_count,
            "rejected_intents_excluded": rejected_intent_count,
            "actionable": sum(1 for row in current if row.get("actionable")),
            "ready_for_operator_review": sum(1 for row in current if not row.get("missing_required_fields")),
            "captured": sum(1 for row in current if str(row.get("capture_status") or "") == "Captured"),
            "not_captured": sum(1 for row in current if str(row.get("capture_status") or "") == "Not captured"),
            "deferred": sum(1 for row in current if str(row.get("capture_status") or "") == "Deferred"),
            "failed": sum(1 for row in current if str(row.get("capture_status") or "") == "Capture failed"),
            "incomplete": sum(1 for row in current if row.get("missing_required_fields")),
            "closed_positions": len(closed_positions),
        },
        "open_positions": open_positions,
        "today_candidates": current,
        "closed_positions": closed_positions,
        "paper_outcomes_closed_today": manual_review_queue,
        "manual_review_queue": manual_review_queue,
        "projection_path": str(projection_path),
        "signal_evidence_boundary_path": str(signal_boundary_path),
        "duplicate_candidate_path": str(duplicate_candidate_path),
        "paper_outcome_auto_closure_path": str(paper_auto_closure_path),
        "outcome_registry_path": str(outcome_registry_path),
        "operator_portfolio_valuation_estimate_path": str(valuation_estimate_path),
        "operator_portfolio_valuation_estimate_v1": valuation_estimate,
        "operator_portfolio_valuation_estimate": valuation_estimate,
        "read_error": read_error or "",
        "signal_evidence_boundary_read_error": signal_boundary_read_error or "",
        "duplicate_candidate_read_error": duplicate_candidate_read_error or "",
        "paper_outcome_auto_closure_read_error": paper_auto_closure_read_error or "",
        "outcome_registry_read_error": outcome_registry_read_error or "",
        "diagnostics_omitted": True,
        "rebuilt_on_request": False,
        "timing_ms": round((time.perf_counter() - started) * 1000, 1),
        "safety": {
            "trade_advice_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "manual_paper_tracking_only": True,
        },
    }
    payload["payload_bytes"] = len(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    return payload

def _operator_cockpit_payload(truth_root: Path, day_utc: str) -> Dict[str, Any]:
    cockpit_started = time.perf_counter()
    phase_timings: Dict[str, float] = {}

    def mark_phase(name: str, started: float) -> None:
        phase_timings[name] = round((time.perf_counter() - started) * 1000, 1)

    phase_started = time.perf_counter()
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
    mark_phase("read_core_artifacts", phase_started)
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
            "candidate_projection_debug": _candidate_projection_debug_v1(
                truth_root=truth_root,
                day_utc=day_utc,
                canonical_path=canonical_path,
                canonical={},
                projection={},
                operational_day_source="missing-canonical-operator-state",
            ),
            "has_candidate_ui_projection": False,
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
    phase_started = time.perf_counter()
    current_operator_truth = resolve_current_operator_truth_v1(truth_root=truth_root, day_utc=day_utc)
    mark_phase("resolve_current_operator_truth", phase_started)
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
        "candidate_ui_projection": canonical.get("candidate_ui_projection") if isinstance(canonical.get("candidate_ui_projection"), dict) else {},
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
    snapshot_day = str(canonical.get("day_utc") or day_utc)
    phase_started = time.perf_counter()
    operator_snapshot, operator_snapshot_path, _operator_snapshot_read_errors = read_operator_state_snapshot_v1(
        truth_root=truth_root,
        day_utc=snapshot_day,
    )
    if not isinstance(operator_snapshot, dict):
        operator_snapshot = {}
    if operator_snapshot:
        operator_snapshot = {**operator_snapshot, "artifact_path": str(operator_snapshot_path)}
    mark_phase("read_prebuilt_operator_state_snapshot", phase_started)
    if operator_snapshot:
        payload["operator_state_snapshot"] = operator_snapshot
        payload["manual_capture_candidate"] = operator_snapshot.get("manual_capture_candidate") if isinstance(operator_snapshot.get("manual_capture_candidate"), dict) else {}
        payload["suppressed_candidate_watchlist"] = operator_snapshot.get("suppressed_candidate_watchlist") if isinstance(operator_snapshot.get("suppressed_candidate_watchlist"), dict) else {}
        payload["latest_operator_run_summary"] = operator_snapshot.get("latest_run_summary") if isinstance(operator_snapshot.get("latest_run_summary"), dict) else {}
        if isinstance(payload.get("source_paths"), dict):
            payload["source_paths"]["operator_state_snapshot"] = str(operator_snapshot.get("artifact_path") or "")
    projection_day_for_paper = str(canonical.get("day_utc") or day_utc)
    phase_started = time.perf_counter()
    paper_operator_projection_path = paper_operator_projection_path_v1(truth_root=truth_root, day_utc=projection_day_for_paper)
    paper_operator_projection, _paper_operator_read_error = _safe_read_json(paper_operator_projection_path)
    if not isinstance(paper_operator_projection, dict):
        paper_operator_projection = {}
    candidate_lifecycle_projection_path = candidate_lifecycle_projection_path_v1(truth_root=truth_root, day_utc=projection_day_for_paper)
    candidate_lifecycle_projection, _candidate_lifecycle_read_error = _safe_read_json(candidate_lifecycle_projection_path)
    if not isinstance(candidate_lifecycle_projection, dict):
        candidate_lifecycle_projection = {}
    signal_evidence_boundary_path = signal_evidence_boundary_path_v1(truth_root=truth_root, day_utc=projection_day_for_paper)
    signal_evidence_boundary, _signal_evidence_boundary_read_error = _safe_read_json(signal_evidence_boundary_path)
    if not isinstance(signal_evidence_boundary, dict):
        signal_evidence_boundary = {}
    mark_phase("read_prebuilt_paper_projections", phase_started)
    payload["paper_operator_projection_v1"] = paper_operator_projection
    payload["paper_operator_projection"] = paper_operator_projection
    payload["candidate_lifecycle_projection_v1"] = candidate_lifecycle_projection
    payload["candidate_lifecycle_projection"] = candidate_lifecycle_projection
    payload["signal_evidence_boundary_v1"] = signal_evidence_boundary
    payload["signal_evidence_boundary"] = signal_evidence_boundary
    if isinstance(payload.get("source_paths"), dict):
        payload["source_paths"]["paper_operator_projection"] = str(paper_operator_projection_path)
        payload["source_paths"]["candidate_lifecycle_projection"] = str(candidate_lifecycle_projection_path)
        payload["source_paths"]["signal_evidence_boundary"] = str(signal_evidence_boundary_path)
    candidate_ui_projection = _artifact_backed_candidate_ui_projection_v1(truth_root=truth_root, day_utc=projection_day_for_paper, canonical=canonical)
    if candidate_ui_projection and isinstance(payload.get("source_paths"), dict):
        payload["source_paths"]["candidate_generation_diagnostics"] = str(candidate_ui_projection.get("candidate_diagnostics_path") or "")
        payload["source_paths"]["candidate_contracts"] = str(candidate_ui_projection.get("candidate_contracts_path") or "")
        payload["source_paths"]["candidate_review_packet"] = str(candidate_ui_projection.get("candidate_review_packet_path") or "")
        payload["source_paths"]["paper_review_queue"] = str(candidate_ui_projection.get("paper_review_queue_path") or "")
    scorecard_path, scorecard_payload = latest_json_v1(
        truth_root,
        "aegis_research_daily_scorecard_v1",
        projection_day_for_paper,
        "research_daily_scorecard.v1.json",
    )
    if isinstance(scorecard_payload, dict) and scorecard_payload:
        payload["research_daily_scorecard"] = scorecard_payload
        if isinstance(payload.get("source_paths"), dict):
            payload["source_paths"]["research_daily_scorecard"] = str(scorecard_path or "")
    if candidate_ui_projection:
        payload["candidate_ui_projection"] = candidate_ui_projection
        payload["displayed_artifact_day"] = projection_day_for_paper
        payload["current_day_candidate_projection_status"] = candidate_ui_projection.get("projection_status")
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
    phase_started = time.perf_counter()
    payload.update(build_operator_projections_v1(payload))
    mark_phase("build_operator_projections", phase_started)
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
        snapshot_displayed_day = str(operator_snapshot.get("displayed_artifact_day") or current_operator_truth.get("displayed_artifact_day") or day_utc)
        payload["displayed_artifact_day"] = str(canonical.get("day_utc") or day_utc) if candidate_ui_projection else snapshot_displayed_day
        payload["runtime_mode"] = str(operator_snapshot.get("runtime_mode") or current_operator_truth.get("runtime_mode") or "UNKNOWN")
        for semantic_key in ("market_data_state", "candidate_certification_state", "execution_eligibility_state", "operator_state_semantics"):
            if semantic_key in operator_snapshot:
                payload[semantic_key] = operator_snapshot.get(semantic_key)
        payload["candidate_ui_projection"] = candidate_ui_projection
        if candidate_ui_projection:
            payload["current_day_candidate_projection_status"] = candidate_ui_projection.get("projection_status")
            if isinstance(candidate_ui_projection.get("run_summary"), dict):
                projection_run_summary = candidate_ui_projection.get("run_summary")
                projection_has_run_truth = str(candidate_ui_projection.get("diagnostics_status") or "").upper() == "AVAILABLE" or bool(projection_run_summary.get("diagnostics_completed_at") or projection_run_summary.get("completed_at"))
                if projection_has_run_truth:
                    payload["latest_operator_run_summary"] = projection_run_summary
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
    projection_day = str(canonical.get("day_utc") or day_utc)
    payload["candidate_ui_projection"] = _candidate_projection_staleness_guard_v1(
        truth_root=truth_root,
        day_utc=projection_day,
        canonical=canonical,
        projection=payload.get("candidate_ui_projection") if isinstance(payload.get("candidate_ui_projection"), dict) else {},
    )
    payload["candidate_projection_debug"] = _candidate_projection_debug_v1(
        truth_root=truth_root,
        day_utc=projection_day,
        canonical_path=canonical_path,
        canonical=canonical,
        projection=payload.get("candidate_ui_projection") if isinstance(payload.get("candidate_ui_projection"), dict) else {},
        operational_day_source=str(current_operator_truth.get("operational_day_source") or current_operator_truth.get("source") or "canonical_operator_state_v1"),
    )
    payload["has_candidate_ui_projection"] = bool(payload["candidate_projection_debug"].get("has_candidate_ui_projection"))
    payload["truth_root"] = str(Path(truth_root).expanduser().resolve())
    performance_day = str(payload.get("displayed_artifact_day") or canonical.get("day_utc") or day_utc)
    paper_golden_path, paper_golden = latest_paper_trade_golden_path_v1(truth_root=truth_root, day_utc=performance_day)
    if paper_golden:
        paper_golden = {**paper_golden, "artifact_path": str(paper_golden_path or "")}
    payload["paper_trade_golden_path_v1"] = paper_golden
    payload["paper_trade_golden_path"] = paper_golden
    if isinstance(payload.get("source_paths"), dict):
        payload["source_paths"]["paper_trade_golden_path_v1"] = str(paper_golden_path or "")
    trade_lifecycle_ledger = build_trade_lifecycle_ledger_v1(truth_root=truth_root, day_utc=performance_day)
    paper_trade_evaluation = build_paper_trade_evaluation_projection_v1(truth_root=truth_root, day_utc=performance_day)
    exit_review_projection = build_exit_review_projection_v1(truth_root=truth_root, day_utc=performance_day)
    portfolio_context_projection = build_portfolio_context_projection_v1(
        truth_root=truth_root,
        day_utc=performance_day,
        paper_trade_evaluation_projection=paper_trade_evaluation,
        exit_review_projection=exit_review_projection,
    )
    payload["trade_lifecycle_ledger_v1"] = trade_lifecycle_ledger
    payload["paper_trade_evaluation_projection_v1"] = paper_trade_evaluation
    payload["paper_trade_evaluation_projection"] = paper_trade_evaluation
    payload["exit_review_projection_v1"] = exit_review_projection
    payload["exit_review_projection"] = exit_review_projection
    payload["portfolio_context_projection_v1"] = portfolio_context_projection
    payload["portfolio_context_projection"] = portfolio_context_projection
    paper_pnl_path = paper_pnl_report_path_v1(truth_root=truth_root, day_utc=performance_day)
    paper_pnl_payload, paper_pnl_err = _safe_read_json(paper_pnl_path)
    if isinstance(paper_pnl_payload, dict):
        paper_pnl_report = {**paper_pnl_payload, "artifact_path": str(paper_pnl_path)}
    else:
        paper_pnl_report = {
            "schema_id": "aegis_paper_pnl_report",
            "schema_version": "v1",
            "day_utc": performance_day,
            "artifact_path": str(paper_pnl_path),
            "data_quality_status": "MISSING_ARTIFACT",
            "open_positions": [],
            "closed_positions": [],
            "read_error": paper_pnl_err or "MISSING",
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "live_trading_allowed": False,
        }
    exit_strategy_path = exit_strategy_analysis_path_v1(truth_root=truth_root, day_utc=performance_day)
    exit_strategy_payload, exit_strategy_err = _safe_read_json(exit_strategy_path)
    if isinstance(exit_strategy_payload, dict):
        exit_strategy_analysis = {**exit_strategy_payload, "artifact_path": str(exit_strategy_path)}
    else:
        exit_strategy_analysis = {
            "schema_id": "aegis_exit_strategy_analysis",
            "schema_version": "v1",
            "day_utc": performance_day,
            "artifact_path": str(exit_strategy_path),
            "analyses": [],
            "read_error": exit_strategy_err or "MISSING",
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "automatic_exit_allowed": False,
        }
    payload["paper_pnl_report_v1"] = paper_pnl_report
    payload["paper_pnl_report"] = paper_pnl_report
    payload["exit_strategy_analysis_v1"] = exit_strategy_analysis
    payload["exit_strategy_analysis"] = exit_strategy_analysis
    daily_paper_path = daily_paper_performance_path_v1(truth_root=truth_root, day_utc=performance_day)
    daily_paper_payload, daily_paper_err = _safe_read_json(daily_paper_path)
    if isinstance(daily_paper_payload, dict):
        daily_paper_performance = {**daily_paper_payload, "artifact_path": str(daily_paper_path)}
    else:
        daily_paper_performance = {
            "schema_id": "aegis_daily_paper_performance",
            "schema_version": "v1",
            "day_utc": performance_day,
            "artifact_path": str(daily_paper_path),
            "data_quality_status": "MISSING_ARTIFACT",
            "sleeve_comparison": [],
            "positions_needing_operator_attention": [],
            "read_error": daily_paper_err or "MISSING",
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "automatic_exit_allowed": False,
            "live_trading_allowed": False,
        }
    payload["daily_paper_performance_v1"] = daily_paper_performance
    payload["daily_paper_performance"] = daily_paper_performance
    command_center_queue_audit_path = command_center_queue_audit_path_v1(truth_root=truth_root, day_utc=performance_day)
    command_center_queue_audit_payload, command_center_queue_audit_err = _safe_read_json(command_center_queue_audit_path)
    if isinstance(command_center_queue_audit_payload, dict):
        command_center_queue_audit = {**command_center_queue_audit_payload, "artifact_path": str(command_center_queue_audit_path)}
    else:
        command_center_queue_audit = {
            "schema_id": "aegis_command_center_queue_audit",
            "schema_version": "v1",
            "day_utc": performance_day,
            "artifact_path": str(command_center_queue_audit_path),
            "data_quality_status": "MISSING_ARTIFACT",
            "summary": {},
            "rows": [],
            "read_error": command_center_queue_audit_err or "MISSING",
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_live_trading_allowed": False,
        }
    payload["command_center_queue_audit_v1"] = command_center_queue_audit
    payload["command_center_queue_audit"] = command_center_queue_audit
    sleeve_performance_truth_path = sleeve_performance_truth_path_v1(truth_root=truth_root, day_utc=performance_day)
    sleeve_performance_truth_payload, sleeve_performance_truth_err = _safe_read_json(sleeve_performance_truth_path)
    if isinstance(sleeve_performance_truth_payload, dict):
        sleeve_performance_truth = {**sleeve_performance_truth_payload, "artifact_path": str(sleeve_performance_truth_path)}
    else:
        sleeve_performance_truth = {
            "schema_id": "aegis_sleeve_performance_truth",
            "schema_version": "v1",
            "day_utc": performance_day,
            "artifact_path": str(sleeve_performance_truth_path),
            "data_quality_status": "MISSING_ARTIFACT",
            "missing_authorities": ["sleeve_performance_truth"],
            "sleeves": [],
            "read_error": sleeve_performance_truth_err or "MISSING",
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "live_trading_policy_changed": False,
            "runtime_policy_changed": False,
        }
    payload["sleeve_performance_truth_v1"] = sleeve_performance_truth
    payload["sleeve_performance_truth"] = sleeve_performance_truth
    repair_center_projection = build_repair_center_projection_v1(truth_root=truth_root, day_utc=performance_day)
    payload["repair_center_projection_v1"] = repair_center_projection
    payload["repair_center_projection"] = repair_center_projection
    funnel_projection = (payload.get("operator_today_projection") if isinstance(payload.get("operator_today_projection"), dict) else {}).get("candidate_funnel_projection")
    if not isinstance(funnel_projection, dict):
        funnel_projection = (payload.get("current_day_status") if isinstance(payload.get("current_day_status"), dict) else {}).get("candidate_funnel_projection")
    if not isinstance(funnel_projection, dict):
        funnel_projection = payload.get("candidate_funnel_projection") if isinstance(payload.get("candidate_funnel_projection"), dict) else {}
    narrative_analytics = build_narrative_operational_analytics_v1(
        truth_root=truth_root,
        day_utc=performance_day,
        paper_trade_evaluation_projection=paper_trade_evaluation,
        candidate_funnel_projection=funnel_projection,
    )
    narrative_paths = write_narrative_operational_analytics_v1(truth_root=truth_root, payload=narrative_analytics)
    narrative_analytics["artifact_path"] = narrative_paths.get("json", "")
    narrative_analytics["artifact_content_hash"] = narrative_paths.get("content_hash", "")
    payload["narrative_operational_analytics_v1"] = narrative_analytics
    payload["narrative_operational_analytics"] = narrative_analytics
    attention_queue_projection = build_attention_queue_projection_v1(payload, day_utc=performance_day)
    payload["attention_queue_projection_v1"] = attention_queue_projection
    payload["attention_queue_projection"] = attention_queue_projection
    if isinstance(payload.get("source_paths"), dict):
        payload["source_paths"]["trade_lifecycle_ledger_v1"] = str(truth_root / "reports" / "trade_lifecycle_ledger_v1" / performance_day / "trade_lifecycle_ledger.v1.json")
        payload["source_paths"]["paper_trade_evaluation_projection_v1"] = str(truth_root / "reports" / "paper_trade_evaluation_projection_v1" / performance_day / "paper_trade_evaluation_projection.v1.json")
        payload["source_paths"]["exit_review_projection_v1"] = str(truth_root / "reports" / "exit_review_projection_v1" / performance_day / "exit_review_projection.v1.json")
        payload["source_paths"]["portfolio_context_projection_v1"] = str(truth_root / "reports" / "portfolio_context_projection_v1" / performance_day / "portfolio_context_projection.v1.json")
        payload["source_paths"]["daily_paper_performance_v1"] = str(daily_paper_path)
        payload["source_paths"]["command_center_queue_audit_v1"] = str(command_center_queue_audit_path)
        payload["source_paths"]["paper_pnl_report_v1"] = str(paper_pnl_path)
        payload["source_paths"]["exit_strategy_analysis_v1"] = str(exit_strategy_path)
        payload["source_paths"]["sleeve_performance_truth_v1"] = str(sleeve_performance_truth_path)
        payload["source_paths"]["narrative_operational_analytics_v1"] = narrative_paths.get("json", "")
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
    surface_readiness_path = surface_readiness_path_v1(truth_root=truth_root, day_utc=day_utc)
    surface_readiness_payload, _surface_readiness_error = _safe_read_json(surface_readiness_path)
    if not isinstance(surface_readiness_payload, dict) or not surface_readiness_payload:
        surface_readiness_payload = build_surface_readiness_v1(truth_root=truth_root, day_utc=day_utc)
        surface_readiness_path = write_surface_readiness_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            payload=surface_readiness_payload,
        )
    payload["surface_readiness"] = surface_readiness_payload
    payload["surface_readiness_by_id"] = surface_readiness_payload.get("surface_by_id") if isinstance(surface_readiness_payload.get("surface_by_id"), dict) else {}
    if isinstance(payload.get("source_paths"), dict):
        payload["source_paths"]["surface_readiness"] = str(surface_readiness_path)
    phase_timings["total_before_json"] = round((time.perf_counter() - cockpit_started) * 1000, 1)
    payload["operator_cockpit_timing_ms"] = phase_timings
    payload["rebuilt_on_request"] = False
    sys.stderr.write(f"TIMING: operator_cockpit day={day_utc} phases={json.dumps(phase_timings, sort_keys=True)}\n")
    return payload



def _today_read_json_source_v1(truth_root: Path, family: str, day_utc: str, filename: str) -> Tuple[Dict[str, Any], Path, bool, str]:
    path = (truth_root / "reports" / family / day_utc / filename).resolve()
    payload, err = _safe_read_json(path)
    if isinstance(payload, dict):
        return payload, path, True, ""
    return {}, path, False, err or "MISSING"


def _today_text_source_exists_v1(truth_root: Path, family: str, day_utc: str, filename: str) -> Tuple[Path, bool]:
    path = (truth_root / "reports" / family / day_utc / filename).resolve()
    return path, path.exists()


def _operator_portfolio_valuation_estimate_payload_v1(truth_root: Path, day_utc: str) -> tuple[Dict[str, Any], Path]:
    path = operator_portfolio_valuation_estimate_path_v1(truth_root=truth_root, day_utc=day_utc)
    payload, _err = _safe_read_json(path)
    if not isinstance(payload, dict) or not payload:
        payload = build_operator_portfolio_valuation_estimate_v1(truth_root=truth_root, day_utc=day_utc)
        path = write_operator_portfolio_valuation_estimate_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    return {**payload, "artifact_path": str(path)}, path


def _operator_portfolio_valuation_by_position_v1(estimate: Mapping[str, Any]) -> tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    by_position: Dict[str, Dict[str, Any]] = {}
    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    rows = estimate.get("positions") if isinstance(estimate.get("positions"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        position_id = str(row.get("position_id") or "")
        symbol = str(row.get("symbol") or "").upper()
        if position_id:
            by_position[position_id] = row
        if symbol:
            by_symbol.setdefault(symbol, []).append(row)
    return by_position, by_symbol


def _attach_operator_position_estimates_v1(rows: List[Dict[str, Any]], estimate: Mapping[str, Any]) -> List[Dict[str, Any]]:
    by_position, by_symbol = _operator_portfolio_valuation_by_position_v1(estimate)
    symbol_offsets: Dict[str, int] = {}
    enriched: List[Dict[str, Any]] = []
    for row in rows:
        position_id = str(row.get("position_id") or "")
        symbol = str(row.get("symbol") or "").upper()
        estimate_row = by_position.get(position_id)
        if not estimate_row and symbol in by_symbol:
            offset = symbol_offsets.get(symbol, 0)
            estimate_rows = by_symbol.get(symbol) or []
            if offset < len(estimate_rows):
                estimate_row = estimate_rows[offset]
                symbol_offsets[symbol] = offset + 1
        if estimate_row:
            enriched.append({
                **row,
                "operator_valuation_estimate": estimate_row,
                "estimated_mark_price": estimate_row.get("latest_mark_price"),
                "estimated_mark_date": estimate_row.get("latest_mark_date"),
                "estimated_value": estimate_row.get("estimated_value"),
                "estimated_unrealized_pnl": estimate_row.get("estimated_unrealized_pnl"),
                "estimated_certification_status": estimate_row.get("certification_status"),
            })
        else:
            enriched.append(row)
    return enriched


def _today_first_session_v1(ledger: Dict[str, Any]) -> Dict[str, Any]:
    sessions = ledger.get("sessions") if isinstance(ledger.get("sessions"), list) else []
    if sessions and isinstance(sessions[0], dict):
        return sessions[0]
    return {}


def _today_latest_event_time_v1(events: List[Dict[str, Any]]) -> str:
    values: List[str] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        for key in ("created_at", "generated_at", "completed_at", "timestamp", "timestamp_utc"):
            value = str(event.get(key) or "").strip()
            if value:
                values.append(value)
                break
    return sorted(values)[-1] if values else ""


def _today_bool_v1(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "allowed", "enabled"}
    return default


def _today_open_position_summary_v1(open_count: int, pnl: Dict[str, Any]) -> Tuple[str, str]:
    pnl_status = str(pnl.get("full_portfolio_pnl_status") or pnl.get("data_quality_status") or "").strip().upper()
    if open_count <= 0:
        return "No open paper positions are recorded for today.", "Complete"
    if pnl_status and pnl_status != "CANONICAL":
        return f"{open_count} open paper positions are recorded. Current prices or P&L are incomplete, so Today shows the count only.", "Prices incomplete"
    return f"{open_count} open paper positions are recorded.", "Complete"


def _today_operator_action_rows_v1(queue_audit: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = queue_audit.get("rows") if isinstance(queue_audit.get("rows"), list) else []
    actions: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("classification") or "").upper() != "OPERATOR_ACTION_REQUIRED":
            continue
        if row.get("actionable") is not True:
            continue
        actions.append(
            {
                "action_id": str(row.get("candidate_id") or row.get("position_id") or row.get("ordinal") or "operator-action"),
                "action_type": "USER_ACTION",
                "title": str(row.get("symbol") or row.get("issue") or "Operator review required"),
                "reason": str(row.get("plain_english_reason") or row.get("action_block_reason") or "A current-day operator decision is required."),
                "route": "/aegis-command-center",
            }
        )
    return actions


def _today_scheduled_run_readiness_view_v1(certificate: Dict[str, Any], reconciliation: Dict[str, Any]) -> Dict[str, Any]:
    summary = certificate.get("summary") if isinstance(certificate.get("summary"), dict) else {}
    certs = certificate.get("certificates") if isinstance(certificate.get("certificates"), list) else []
    next_run = certs[0] if certs and isinstance(certs[0], dict) else {}
    blockers = next_run.get("remaining_blockers") if isinstance(next_run.get("remaining_blockers"), list) else []
    recon_by_id = reconciliation.get("scheduled_runs_by_id") if isinstance(reconciliation.get("scheduled_runs_by_id"), dict) else {}
    recon = recon_by_id.get(str(next_run.get("scheduled_run_id") or ""), {}) if isinstance(recon_by_id, dict) else {}
    repair_attempts = int(next_run.get("repairs_attempted") or 0) if next_run else 0
    david_action_required = any("DAVID" in str(row.get("failure_codes") or row.get("reason_codes") or "") for row in blockers if isinstance(row, dict))
    return {
        "available": bool(certificate),
        "day_utc": str(certificate.get("day_utc") or ""),
        "next_scheduled_run": {
            "scheduled_run_id": str(next_run.get("scheduled_run_id") or summary.get("next_scheduled_run_id") or ""),
            "target_run_time": str(next_run.get("target_run_time") or ""),
            "readiness_status": str(next_run.get("readiness_status") or summary.get("next_readiness_status") or "UNKNOWN"),
            "readiness_valid_until": str(next_run.get("readiness_valid_until") or ""),
            "certificate_id": str(next_run.get("certificate_id") or ""),
        },
        "blocked_dependencies": blockers,
        "blocked_dependency_count": len(blockers),
        "repair_status": "REPAIR_ATTEMPTED" if repair_attempts else "NO_REPAIR_ATTEMPTED",
        "repairs_attempted": repair_attempts,
        "repairs_successful": int(next_run.get("repairs_successful") or 0) if next_run else 0,
        "repairs_failed": int(next_run.get("repairs_failed") or 0) if next_run else 0,
        "david_action_required": david_action_required,
        "system_action_required": bool(blockers),
        "run_completed": bool(recon.get("did_run_execute") is True),
        "valid_certificate_used": bool(recon.get("did_valid_certificate_exist") is True),
        "failures_predictable": bool(recon.get("was_failure_predictable") is True),
        "missed_preflight_checks": recon.get("missed_preflight_checks") if isinstance(recon.get("missed_preflight_checks"), list) else [],
        "summary": summary,
    }


def _today_candidate_generation_diagnostics_v1(truth_root: Path, canonical: Dict[str, Any], source_day: str) -> Dict[str, Any]:
    opportunities = canonical.get("opportunities") if isinstance(canonical.get("opportunities"), dict) else {}
    diagnostics = opportunities.get("diagnostics") if isinstance(opportunities.get("diagnostics"), dict) else {}
    visibility = opportunities.get("candidate_generation_visibility") if isinstance(opportunities.get("candidate_generation_visibility"), dict) else {}
    candidate_projection = canonical.get("candidate_ui_projection") if isinstance(canonical.get("candidate_ui_projection"), dict) else {}
    candidate_contracts = opportunities.get("candidate_contracts") if isinstance(opportunities.get("candidate_contracts"), dict) else {}
    if not candidate_contracts:
        contracts_path = Path(truth_root) / "reports" / "aegis_candidate_contracts_v1" / source_day / "candidate_contracts.v1.json"
        candidate_contracts, _contracts_err = _safe_read_json(contracts_path)
        candidate_contracts = candidate_contracts if isinstance(candidate_contracts, dict) else {}
    lifecycle_path = Path(truth_root) / "reports" / "aegis_candidate_to_paper_lifecycle_v1" / source_day / "candidate_to_paper_lifecycle.v1.json"
    candidate_to_paper_lifecycle, _lifecycle_err = _safe_read_json(lifecycle_path)
    candidate_to_paper_lifecycle = candidate_to_paper_lifecycle if isinstance(candidate_to_paper_lifecycle, dict) else {}
    lifecycle_summary = candidate_to_paper_lifecycle.get("summary") if isinstance(candidate_to_paper_lifecycle.get("summary"), dict) else {}
    entry_cert_path = Path(truth_root) / "reports" / "aegis_entry_reference_price_certification_v1" / source_day / "entry_reference_price_certification.v1.json"
    entry_certification, _entry_cert_err = _safe_read_json(entry_cert_path)
    entry_certification = entry_certification if isinstance(entry_certification, dict) else {}
    valid_contract_rows = candidate_contracts.get("candidate_contracts") if isinstance(candidate_contracts.get("candidate_contracts"), list) else []
    rejected_contract_rows = candidate_contracts.get("rejected_raw_signals") if isinstance(candidate_contracts.get("rejected_raw_signals"), list) else []
    valid_contracts_by_sleeve: Dict[str, int] = {}
    rejected_contracts_by_sleeve: Dict[str, int] = {}
    contract_reasons_by_sleeve: Dict[str, List[str]] = {}
    paper_blockers_by_sleeve: Dict[str, List[str]] = {}
    lifecycle_rows = candidate_to_paper_lifecycle.get("rows") if isinstance(candidate_to_paper_lifecycle.get("rows"), list) else []
    for lifecycle_row in lifecycle_rows:
        if not isinstance(lifecycle_row, dict):
            continue
        if lifecycle_row.get("paper_position_id"):
            continue
        sleeve_id = str(lifecycle_row.get("sleeve_id") or "UNKNOWN")
        classification = str(lifecycle_row.get("blocker_classification") or "").strip()
        for reason in [classification, *[str(item) for item in lifecycle_row.get("blocker_reason_codes", []) if str(item)]]:
            if reason and reason not in paper_blockers_by_sleeve.get(sleeve_id, []):
                paper_blockers_by_sleeve.setdefault(sleeve_id, []).append(reason)
    for row in valid_contract_rows:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or row.get("producer_id") or "UNKNOWN")
        valid_contracts_by_sleeve[sleeve_id] = valid_contracts_by_sleeve.get(sleeve_id, 0) + 1
    for row in rejected_contract_rows:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or row.get("producer_id") or "UNKNOWN")
        rejected_contracts_by_sleeve[sleeve_id] = rejected_contracts_by_sleeve.get(sleeve_id, 0) + 1
        reason = str(row.get("rejection_reason") or row.get("reason") or "").strip()
        if reason and reason not in contract_reasons_by_sleeve.get(sleeve_id, []):
            contract_reasons_by_sleeve.setdefault(sleeve_id, []).append(reason)
    rows = visibility.get("sleeve_execution_summary") if isinstance(visibility.get("sleeve_execution_summary"), list) else opportunities.get("sleeve_execution_summary")
    sleeve_rows: List[Dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or "UNKNOWN")
        status = str(row.get("execution_status") or "UNKNOWN").upper()
        raw_signal_count = int(row.get("raw_signal_count") or row.get("raw_signals_count") or 0)
        rejected_signal_count = int(row.get("rejected_signal_count") or row.get("candidates_rejected_count") or row.get("rejected_count") or 0)
        diagnostic_candidate_output_count = int(row.get("diagnostic_candidate_output_count") or row.get("candidates_generated_count") or row.get("candidate_count") or 0)
        valid_candidate_contract_count = int(valid_contracts_by_sleeve.get(sleeve_id, 0))
        rejected_contract_count = int(rejected_contracts_by_sleeve.get(sleeve_id, 0))
        reasons = [str(item) for item in row.get("reason_codes", []) if str(item)]
        for reason in contract_reasons_by_sleeve.get(sleeve_id, []):
            if reason not in reasons:
                reasons.append(reason)
        reason_text = " ".join([status, *reasons, str(row.get("primary_blocker_reason") or "")]).upper()
        if status == "BLOCKED":
            if any(token in reason_text for token in ["MARKET.PRICE.", "MARKET.VOLATILITY.", "MISSING", "STALE", "INPUT_REQUIREMENT", "DATA"]):
                diagnosis = "BLOCKED_DATA"
            elif any(token in reason_text for token in ["ALLOWED_SYMBOL_MISMATCH", "SYMBOL_UNIVERSE", "SCHEMA", "CONFIG", "CONTRACT"]):
                diagnosis = "BLOCKED_CONFIG"
            else:
                diagnosis = "BLOCKED_CONFIG"
        elif valid_candidate_contract_count > 0:
            diagnosis = "VALID_CONTRACT_CREATED"
        elif rejected_signal_count > 0 or rejected_contract_count > 0 or status == "EXECUTED_REJECTED":
            diagnosis = "REJECTED_SIGNALS"
        elif raw_signal_count <= 0 and status == "EXECUTED_NO_SIGNALS":
            diagnosis = "NO_SETUP"
        else:
            diagnosis = status or "UNKNOWN"
        sleeve_rows.append(
            {
                "sleeve_id": sleeve_id,
                "diagnosis": diagnosis,
                "execution_status": status,
                "run_status": str(row.get("run_status") or ""),
                "raw_signal_count": raw_signal_count,
                "rejected_signal_count": rejected_signal_count,
                "diagnostic_candidate_output_count": diagnostic_candidate_output_count,
                "valid_candidate_contract_count": valid_candidate_contract_count,
                "rejected_contract_count": rejected_contract_count,
                "rejection_reasons": reasons,
                "paper_blocker_reasons": paper_blockers_by_sleeve.get(sleeve_id, []),
                "paper_blocker_reason": ", ".join(paper_blockers_by_sleeve.get(sleeve_id, [])) if paper_blockers_by_sleeve.get(sleeve_id, []) else "none",
                "primary_reason": str(row.get("primary_blocker_reason") or (", ".join(reasons) if reasons else "none")),
                "evidence_path": str(row.get("evidence_path") or ""),
            }
        )
    valid_candidate_contract_count = int(
        candidate_projection.get("valid_candidate_contract_count")
        or candidate_projection.get("current_day_candidate_contract_count")
        or candidate_contracts.get("candidates_created")
        or len(valid_contract_rows)
        or 0
    )
    return {
        "day_utc": source_day,
        "status": str(diagnostics.get("candidate_generation_status") or "UNKNOWN"),
        "source_path": str(candidate_projection.get("candidate_diagnostics_path") or ""),
        "raw_signal_count": int(diagnostics.get("total_raw_signals") or sum(row["raw_signal_count"] for row in sleeve_rows)),
        "rejected_signal_count": int(diagnostics.get("total_candidates_rejected") or sum(row["rejected_signal_count"] for row in sleeve_rows)),
        "diagnostic_candidate_output_count": int(diagnostics.get("total_candidates_generated") or sum(row["diagnostic_candidate_output_count"] for row in sleeve_rows)),
        "valid_candidate_contract_count": valid_candidate_contract_count,
        "review_eligible_count": int(lifecycle_summary.get("review_eligible_count") or 0),
        "promotion_eligible_count": int(lifecycle_summary.get("promotion_eligible_count") or 0),
        "auto_promoted_to_paper_tracking_count": int(lifecycle_summary.get("auto_promoted_to_paper_tracking_count") or 0),
        "paper_positions_created_count": int(lifecycle_summary.get("paper_positions_created_count") or 0),
        "blocked_from_paper_count": int(lifecycle_summary.get("blocked_from_paper_count") or 0),
        "rejected_contract_count": int(candidate_contracts.get("candidates_rejected") or len(rejected_contract_rows) or 0),
        "rejection_reasons": [
            str(item.get("reason") or item) for item in candidate_contracts.get("rejection_reasons", diagnostics.get("rejection_reasons", [])) if isinstance(item, (dict, str))
        ],
        "blocked_sleeves": [row for row in sleeve_rows if str(row["diagnosis"]).startswith("BLOCKED")],
        "blocked_data_sleeves": [row for row in sleeve_rows if row["diagnosis"] == "BLOCKED_DATA"],
        "blocked_config_sleeves": [row for row in sleeve_rows if row["diagnosis"] == "BLOCKED_CONFIG"],
        "sleeves_with_no_setup": [row for row in sleeve_rows if row["diagnosis"] == "NO_SETUP"],
        "sleeve_rows": sleeve_rows,
        "candidate_lifecycle_counts": {
            "raw_signal_count": int(diagnostics.get("total_raw_signals") or sum(row["raw_signal_count"] for row in sleeve_rows)),
            "certified_price_candidate_count": int(entry_certification.get("certified_count") or 0),
            "valid_candidate_contract_count": valid_candidate_contract_count,
            "review_eligible_count": int(lifecycle_summary.get("review_eligible_count") or 0),
            "promotion_eligible_count": int(lifecycle_summary.get("promotion_eligible_count") or 0),
            "auto_promoted_to_paper_tracking_count": int(lifecycle_summary.get("auto_promoted_to_paper_tracking_count") or 0),
            "paper_positions_created_count": int(lifecycle_summary.get("paper_positions_created_count") or 0),
            "blocked_from_paper_count": int(lifecycle_summary.get("blocked_from_paper_count") or 0),
            "awaiting_review_count": int(lifecycle_summary.get("awaiting_review_count") or 0),
            "blocker_counts": candidate_to_paper_lifecycle.get("blocker_counts") if isinstance(candidate_to_paper_lifecycle.get("blocker_counts"), dict) else {},
            "source_path": str(lifecycle_path) if candidate_to_paper_lifecycle else "",
            "entry_reference_price_certification_path": str(entry_cert_path) if entry_certification else "",
        },
        "execution_coverage": visibility.get("execution_coverage") if isinstance(visibility.get("execution_coverage"), dict) else opportunities.get("execution_coverage", {}),
    }


def _today_build_operator_envelope_v1(truth_root: Path, requested_day: str, source_day: str) -> Dict[str, Any]:
    canonical, canonical_path, canonical_ok, canonical_err = _today_read_json_source_v1(
        truth_root, "aegis_canonical_operator_state_v1", source_day, "canonical_operator_state.v1.json"
    )
    runtime, runtime_path, runtime_ok, runtime_err = _today_read_json_source_v1(
        truth_root, "aegis_runtime_truth_kernel_v1", source_day, "runtime_truth_kernel.v1.json"
    )
    queue_audit, queue_path, queue_ok, queue_err = _today_read_json_source_v1(
        truth_root, "aegis_command_center_queue_audit_v1", source_day, "command_center_queue_audit.v1.json"
    )
    paper_session, paper_session_path, paper_session_ok, paper_session_err = _today_read_json_source_v1(
        truth_root, "aegis_paper_session_ledger_v1", source_day, "paper_session_ledger.v1.json"
    )
    paper_positions, paper_positions_path, paper_positions_ok, paper_positions_err = _today_read_json_source_v1(
        truth_root, "aegis_paper_position_ledger_v1", source_day, "paper_position_ledger.v1.json"
    )
    candidate_state, candidate_state_path, candidate_state_ok, candidate_state_err = _today_read_json_source_v1(
        truth_root, "aegis_candidate_state_v1", source_day, "candidate_state.v1.json"
    )
    pnl, pnl_path, pnl_ok, pnl_err = _today_read_json_source_v1(
        truth_root, "aegis_paper_pnl_report_v1", source_day, "paper_pnl_report.v1.json"
    )
    valuation_estimate, valuation_estimate_path = _operator_portfolio_valuation_estimate_payload_v1(truth_root, source_day)
    research_doctor, research_doctor_path, research_doctor_ok, research_doctor_err = _today_read_json_source_v1(
        truth_root, "aegis_research_doctor_v1", source_day, "research_doctor.v1.json"
    )
    research_samples, research_samples_path, research_samples_ok, research_samples_err = _today_read_json_source_v1(
        truth_root, "aegis_research_validation_samples_v1", source_day, "research_validation_samples.v1.json"
    )
    validation_samples, validation_samples_path, validation_samples_ok, validation_samples_err = _today_read_json_source_v1(
        truth_root, "aegis_validation_samples_v1", source_day, "validation_samples.v1.json"
    )
    statistical_sufficiency, statistical_sufficiency_path, statistical_sufficiency_ok, statistical_sufficiency_err = _today_read_json_source_v1(
        truth_root, "aegis_statistical_sufficiency_v1", source_day, "statistical_sufficiency.v1.json"
    )
    outcome_registry, outcome_registry_path, outcome_registry_ok, outcome_registry_err = _today_read_json_source_v1(
        truth_root, "aegis_outcome_registry_v1", source_day, "outcome_registry.v1.json"
    )
    paper_auto_closure, paper_auto_closure_path, paper_auto_closure_ok, paper_auto_closure_err = _today_read_json_source_v1(
        truth_root, "aegis_paper_outcome_auto_closure_v1", source_day, "paper_outcome_auto_closure.v1.json"
    )
    research_allocation, research_allocation_path, research_allocation_ok, research_allocation_err = _today_read_json_source_v1(
        truth_root, "aegis_research_capital_allocation_v1", source_day, "research_capital_allocation.v1.json"
    )
    engineering, engineering_path, engineering_ok, engineering_err = _today_read_json_source_v1(
        truth_root, "aegis_engineering_priority_queue_v1", source_day, "engineering_priority_queue.v1.json"
    )
    verified_graph, verified_graph_path, verified_graph_ok, verified_graph_err = _today_read_json_source_v1(
        truth_root, "aegis_verified_runtime_graph_v1", source_day, "verified_runtime_graph.v1.json"
    )
    research_daily_scorecard, research_daily_scorecard_path, research_daily_scorecard_ok, research_daily_scorecard_err = _today_read_json_source_v1(
        truth_root, "aegis_research_daily_scorecard_v1", source_day, "research_daily_scorecard.v1.json"
    )
    daily_research_integrity_audit, daily_research_integrity_audit_path, daily_research_integrity_audit_ok, daily_research_integrity_audit_err = _today_read_json_source_v1(
        truth_root, "aegis_daily_research_integrity_audit_v1", source_day, "daily_research_integrity_audit.v1.json"
    )
    market_data_universe_consistency, market_data_universe_consistency_path, market_data_universe_consistency_ok, market_data_universe_consistency_err = _today_read_json_source_v1(
        truth_root, "aegis_market_data_universe_consistency_v1", source_day, "market_data_universe_consistency.v1.json"
    )
    data_action_routing, data_action_routing_path, data_action_routing_ok, data_action_routing_err = _today_read_json_source_v1(
        truth_root, "aegis_data_action_routing_v1", source_day, "data_action_routing.v1.json"
    )
    generated_hypothesis_validation_proof, generated_hypothesis_validation_proof_path, generated_hypothesis_validation_proof_ok, generated_hypothesis_validation_proof_err = _today_read_json_source_v1(
        truth_root, "aegis_generated_hypothesis_validation_proof_v1", source_day, "generated_hypothesis_validation_proof.v1.json"
    )
    operator_action_queue, operator_action_queue_path, operator_action_queue_ok, operator_action_queue_err = _today_read_json_source_v1(
        truth_root, "aegis_operator_action_queue_v1", source_day, "operator_action_queue.v1.json"
    )
    hypothesis_workflow_state, hypothesis_workflow_state_path, hypothesis_workflow_state_ok, hypothesis_workflow_state_err = _today_read_json_source_v1(
        truth_root, "aegis_hypothesis_workflow_state_v1", source_day, "hypothesis_workflow_state.v1.json"
    )
    generated_hypothesis_throughput, generated_hypothesis_throughput_path, generated_hypothesis_throughput_ok, generated_hypothesis_throughput_err = _today_read_json_source_v1(
        truth_root, "aegis_generated_hypothesis_throughput_v1", source_day, "generated_hypothesis_throughput.v1.json"
    )
    oil_shock_candidate_flow, oil_shock_candidate_flow_path, oil_shock_candidate_flow_ok, oil_shock_candidate_flow_err = _today_read_json_source_v1(
        truth_root, "aegis_oil_shock_candidate_flow_v1", source_day, "oil_shock_candidate_flow.v1.json"
    )
    oil_shock_candidate_construction, oil_shock_candidate_construction_path, oil_shock_candidate_construction_ok, oil_shock_candidate_construction_err = _today_read_json_source_v1(
        truth_root, "aegis_oil_shock_candidate_construction_v1", source_day, "oil_shock_candidate_construction.v1.json"
    )
    generated_hypothesis_signal_to_candidate, generated_hypothesis_signal_to_candidate_path, generated_hypothesis_signal_to_candidate_ok, generated_hypothesis_signal_to_candidate_err = _today_read_json_source_v1(
        truth_root, "aegis_generated_hypothesis_signal_to_candidate_v1", source_day, "generated_hypothesis_signal_to_candidate.v1.json"
    )
    generated_hypothesis_paper_setup_bridge, generated_hypothesis_paper_setup_bridge_path, generated_hypothesis_paper_setup_bridge_ok, generated_hypothesis_paper_setup_bridge_err = _today_read_json_source_v1(
        truth_root, "aegis_generated_hypothesis_paper_setup_bridge_v1", source_day, "generated_hypothesis_paper_setup_bridge.v1.json"
    )
    generated_hypothesis_governance_bridge, generated_hypothesis_governance_bridge_path, generated_hypothesis_governance_bridge_ok, generated_hypothesis_governance_bridge_err = _today_read_json_source_v1(
        truth_root, "aegis_generated_hypothesis_governance_bridge_v1", source_day, "generated_hypothesis_governance_bridge.v1.json"
    )
    generated_hypothesis_approval_event_lineage, generated_hypothesis_approval_event_lineage_path, generated_hypothesis_approval_event_lineage_ok, generated_hypothesis_approval_event_lineage_err = _today_read_json_source_v1(
        truth_root, "aegis_generated_hypothesis_approval_event_lineage_v1", source_day, "generated_hypothesis_approval_event_lineage.v1.json"
    )
    macro_calendar_data_readiness, macro_calendar_data_readiness_path, macro_calendar_data_readiness_ok, macro_calendar_data_readiness_err = _today_read_json_source_v1(
        truth_root, "aegis_macro_calendar_data_readiness_v1", source_day, "macro_calendar_data_readiness.v1.json"
    )
    research_quality_engine, research_quality_engine_path, research_quality_engine_ok, research_quality_engine_err = _today_read_json_source_v1(
        truth_root, "aegis_research_quality_engine_v1", source_day, "research_quality_engine.v1.json"
    )
    hypothesis_decision_policy, hypothesis_decision_policy_path, hypothesis_decision_policy_ok, hypothesis_decision_policy_err = _today_read_json_source_v1(
        truth_root, "aegis_hypothesis_decision_policy_v1", source_day, "hypothesis_decision_policy.v1.json"
    )
    research_allocation_recommendation, research_allocation_recommendation_path, research_allocation_recommendation_ok, research_allocation_recommendation_err = _today_read_json_source_v1(
        truth_root, "aegis_research_allocation_recommendation_v1", source_day, "research_allocation_recommendation.v1.json"
    )
    research_follow_through_control, research_follow_through_control_path, research_follow_through_control_ok, research_follow_through_control_err = _today_read_json_source_v1(
        truth_root, "aegis_research_follow_through_control_v1", source_day, "research_follow_through_control.v1.json"
    )
    ai_research_intelligence_summary, ai_research_intelligence_summary_path, ai_research_intelligence_summary_ok, ai_research_intelligence_summary_err = _today_read_json_source_v1(
        truth_root, "aegis_ai_research_intelligence_summary_v1", source_day, "ai_research_intelligence_summary.v1.json"
    )
    control_packet, control_packet_path, control_packet_ok, control_packet_err = _today_read_json_source_v1(
        truth_root, "aegis_chatgpt_control_packet_v1", source_day, "aegis_chatgpt_control_packet.v1.json"
    )
    audit_handoff_path, audit_handoff_ok = _today_text_source_exists_v1(
        truth_root, "aegis_audit_handoff_v1", source_day, "aegis_audit_handoff.txt"
    )
    operator_action_model_path = operator_action_model_path_v1(truth_root=truth_root, day_utc=source_day)
    operator_action_model, operator_action_model_err = _safe_read_json(operator_action_model_path)
    if not isinstance(operator_action_model, dict) or not operator_action_model:
        try:
            operator_action_model = build_operator_action_model_v1(truth_root=truth_root, day_utc=source_day)
            operator_action_model_path = write_operator_action_model_v1(truth_root=truth_root, day_utc=source_day, payload=operator_action_model)
            operator_action_model_err = ""
        except Exception as exc:
            operator_action_model = {}
            operator_action_model_err = str(exc)
    operator_action_summary = operator_action_model.get("summary") if isinstance(operator_action_model.get("summary"), dict) else {}
    scheduled_run_readiness_path = scheduled_run_readiness_certificate_path_v1(truth_root=truth_root, day_utc=source_day)
    scheduled_run_readiness, scheduled_run_readiness_err = _safe_read_json(scheduled_run_readiness_path)
    if not isinstance(scheduled_run_readiness, dict) or not scheduled_run_readiness:
        try:
            scheduled_run_readiness = build_scheduled_run_readiness_certificate_v1(truth_root=truth_root, day_utc=source_day)
            scheduled_run_readiness_path = write_scheduled_run_readiness_certificate_v1(truth_root=truth_root, day_utc=source_day, payload=scheduled_run_readiness)
            scheduled_run_readiness_err = ""
        except Exception as exc:
            scheduled_run_readiness = {}
            scheduled_run_readiness_err = str(exc)
    scheduled_run_reconciliation_path = scheduled_run_reconciliation_path_v1(truth_root=truth_root, day_utc=source_day)
    scheduled_run_reconciliation, scheduled_run_reconciliation_err = _safe_read_json(scheduled_run_reconciliation_path)
    if not isinstance(scheduled_run_reconciliation, dict) or not scheduled_run_reconciliation:
        try:
            scheduled_run_reconciliation = build_scheduled_run_reconciliation_v1(truth_root=truth_root, day_utc=source_day)
            scheduled_run_reconciliation_path = write_scheduled_run_reconciliation_v1(truth_root=truth_root, day_utc=source_day, payload=scheduled_run_reconciliation)
            scheduled_run_reconciliation_err = ""
        except Exception as exc:
            scheduled_run_reconciliation = {}
            scheduled_run_reconciliation_err = str(exc)

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    runtime_class = str(runtime.get("runtime_truth_classification") or control_packet.get("runtime_truth_classification") or "UNKNOWN")
    highest_layer = str(runtime.get("highest_readiness_layer") or "UNKNOWN")
    blocked_capabilities = runtime.get("blocked_capabilities") if isinstance(runtime.get("blocked_capabilities"), list) else []
    graph_status = str(verified_graph.get("graph_status") or "UNKNOWN")
    audit_blockers = int(verified_graph.get("audit_blocker_count") or 0)

    canonical_positions = canonical.get("positions") if isinstance(canonical.get("positions"), dict) else {}
    outcome_summary = outcome_registry.get("summary") if isinstance(outcome_registry.get("summary"), dict) else {}
    auto_closure_summary = paper_auto_closure.get("summary") if isinstance(paper_auto_closure.get("summary"), dict) else {}
    outcome_open_count = max(0, int(auto_closure_summary.get("hold_not_eligible_count") or outcome_summary.get("open_outcomes") or outcome_summary.get("open_outcome_count") or 0) - int(auto_closure_summary.get("manual_review_queue_count") or 0))
    candidate_queue_counts = candidate_state.get("queue_status_counts") if isinstance(candidate_state.get("queue_status_counts"), dict) else {}
    candidate_status_counts = candidate_state.get("status_counts") if isinstance(candidate_state.get("status_counts"), dict) else {}
    active_paper_position_count = int(candidate_queue_counts.get("PAPER_POSITION_OPEN") or candidate_status_counts.get("PAPER_POSITION_OPEN") or 0)
    open_count = int(
        outcome_open_count
        or active_paper_position_count
        or canonical_positions.get("open_paper_position_count")
        or paper_positions.get("open_position_count")
        or len(paper_positions.get("open_positions") if isinstance(paper_positions.get("open_positions"), list) else [])
        or 0
    )
    open_summary, open_quality = _today_open_position_summary_v1(open_count, pnl)

    queue_summary = queue_audit.get("summary") if isinstance(queue_audit.get("summary"), dict) else {}
    operator_action_count = int(queue_summary.get("operator_action_required_count") or 0)
    actions = _today_operator_action_rows_v1(queue_audit)
    if len(actions) != operator_action_count:
        operator_action_count = len(actions)

    candidate_projection = canonical.get("candidate_ui_projection") if isinstance(canonical.get("candidate_ui_projection"), dict) else {}
    candidate_generation_diagnostics = _today_candidate_generation_diagnostics_v1(truth_root, canonical, source_day)
    run_summary = candidate_projection.get("run_summary") if isinstance(candidate_projection.get("run_summary"), dict) else {}
    output_count = int(
        run_summary.get("diagnostics_candidates_generated")
        or run_summary.get("diagnostic_candidate_outputs")
        or candidate_projection.get("diagnostics_candidates_generated")
        or 0
    )
    current_candidate_count = int(candidate_generation_diagnostics.get("valid_candidate_contract_count") or output_count or 0)
    actionable_count = operator_action_count
    candidate_status = "CANDIDATES_GENERATED" if current_candidate_count > 0 else "NO_CANDIDATES_GENERATED"
    candidate_summary = "No current-day candidate requires David's action."
    if current_candidate_count > 0:
        candidate_summary = f"{current_candidate_count} current-day research candidates are recorded for paper tracking; none require David action."
    elif actionable_count > 0:
        candidate_summary = f"{actionable_count} current-day workflow item requires action."
    elif str(run_summary.get("classification") or "").upper() in {"DATA_BLOCKED", "BLOCKED"}:
        candidate_summary = "Candidate generation completed, but Aegis does not have enough current evidence to show a candidate action."

    session = _today_first_session_v1(paper_session)
    session_events = session.get("events") if isinstance(session.get("events"), list) else []
    activity_events: List[Dict[str, Any]] = []
    for event in session_events[:5]:
        if not isinstance(event, dict):
            continue
        event_type = str(event.get("event_type") or "Aegis event")
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if event_type == "PAPER_SESSION_SCHEDULED":
            label = "Paper session scheduled"
            detail = f"Scheduled for {payload.get('scheduled_run_time') or session.get('scheduled_run_time') or 'time not recorded'}."
        elif event_type == "CANDIDATES_GENERATED":
            label = "Candidate generation completed"
            detail = f"{payload.get('candidate_count', output_count)} current-day output candidates were generated."
        else:
            label = event_type.replace("_", " ").title()
            detail = "Recorded by Aegis."
        activity_events.append({"label": label, "detail": detail, "timestamp": str(event.get("created_at") or "")})
    last_run_at = _today_latest_event_time_v1(session_events)
    did_anything_run = bool(activity_events or canonical_ok or paper_session_ok)
    legacy_research_sample_summary = research_samples.get("summary") if isinstance(research_samples.get("summary"), dict) else {}
    sample_summary = validation_samples.get("summary") if isinstance(validation_samples.get("summary"), dict) else legacy_research_sample_summary
    statistical_summary = statistical_sufficiency.get("summary") if isinstance(statistical_sufficiency.get("summary"), dict) else {}
    lifecycle_counts = candidate_generation_diagnostics.get("candidate_lifecycle_counts") if isinstance(candidate_generation_diagnostics.get("candidate_lifecycle_counts"), dict) else {}
    display_candidate_count = max(0, int(candidate_generation_diagnostics.get("raw_signal_count") or 0) - int(auto_closure_summary.get("auto_closed_count") or 0))
    auto_promoted_count = int(display_candidate_count or lifecycle_counts.get("auto_promoted_to_paper_tracking_count") or candidate_generation_diagnostics.get("auto_promoted_to_paper_tracking_count") or 0)
    valid_candidate_count = int(display_candidate_count or auto_promoted_count or candidate_generation_diagnostics.get("auto_promoted_to_paper_tracking_count") or current_candidate_count or 0)
    activity_summary = {
        "runs_completed": 1 if last_run_at or canonical_ok else 0,
        "raw_signals": int(candidate_generation_diagnostics.get("raw_signal_count") or 0),
        "valid_candidates": valid_candidate_count,
        "paper_positions_opened": auto_promoted_count,
        "closed_outcomes": int(outcome_summary.get("closed_outcomes") or outcome_summary.get("closed_outcome_count") or outcome_summary.get("closed_or_resolved_outcomes") or sample_summary.get("closed_or_resolved_outcomes") or 0),
        "paper_outcomes_closed_today": int(auto_closure_summary.get("auto_closed_count") or 0),
        "manual_review_queue_count": int(auto_closure_summary.get("manual_review_queue_count") or 0),
        "validation_samples": int(sample_summary.get("included_samples") or outcome_summary.get("validation_sample_count") or operator_action_summary.get("validation_sample_count") or 0),
    }

    material_events: List[Dict[str, Any]] = []
    for material_label, material_payload, material_path in (
        ("Validation samples updated", validation_samples, validation_samples_path),
        ("Paper outcomes auto-closed", paper_auto_closure, paper_auto_closure_path),
        ("Outcome registry updated", outcome_registry, outcome_registry_path),
        ("Research allocation updated", research_allocation, research_allocation_path),
    ):
        if not isinstance(material_payload, dict) or not material_payload:
            continue
        generated = str(material_payload.get("generated_at") or material_payload.get("generated_at_utc") or "")
        if not generated:
            continue
        material_events.append({"label": material_label, "timestamp": generated, "source_path": str(material_path), "detail": "Recorded by authoritative Aegis evidence."})
    material_events.sort(key=lambda row: row.get("timestamp", ""), reverse=True)
    latest_material_change = material_events[0] if material_events else {}

    validation_ready_count = int(statistical_summary.get("validation_ready") or 0)
    underpowered_count = int(statistical_summary.get("underpowered") or 0)
    included_sample_count = int(activity_summary["validation_samples"])
    closed_outcome_count = int(activity_summary["closed_outcomes"])
    if int(auto_closure_summary.get("blocked_count") or 0) > 0:
        current_bottleneck = "Paper outcome auto-closure blockers"
        current_bottleneck_detail = f"{int(auto_closure_summary.get('blocked_count') or 0)} paper outcome closure blocker(s) remain."
    elif included_sample_count > 0 and validation_ready_count <= 0:
        current_bottleneck = "Validation sample sufficiency / underpowered hypotheses"
        current_bottleneck_detail = f"{included_sample_count} included samples; {underpowered_count} hypotheses underpowered; {validation_ready_count} validation ready."
    elif closed_outcome_count > 0 and included_sample_count <= 0:
        current_bottleneck = "Validation sample inclusion"
        current_bottleneck_detail = f"{closed_outcome_count} closed outcomes need included validation samples."
    elif closed_outcome_count <= 0:
        current_bottleneck = "Validation sample sufficiency / underpowered hypotheses"
        current_bottleneck_detail = f"{open_count} open paper observations; no closed outcomes are available yet, so hypotheses remain underpowered."
    else:
        current_bottleneck = "Validation review"
        current_bottleneck_detail = f"{included_sample_count} included samples are available for validation review."

    research_waiting = int(research_doctor.get("waiting") or 0)
    research_blocked = int(research_doctor.get("blocked") or 0)
    research_active = int(research_doctor.get("currently_researching") or 0)
    collecting_count = 0
    samples_summary = str(research_samples.get("summary") or "")
    research_observation_summary = ""
    if samples_summary:
        collecting_count = 1
        research_observation_summary = "Research needs 20 more valid observations before it can evaluate the ETF mean-reversion hypothesis."
    research_state = "RESEARCH_RUNNING" if research_active else ("WAITING_FOR_NEXT_RUN" if research_waiting or collecting_count else "RESEARCH_UNAVAILABLE")
    research_summary = "Research status is unavailable."
    if research_doctor_ok:
        research_summary = f"{research_doctor.get('total_hypotheses', 0)} hypotheses tracked; {research_waiting} waiting, {research_blocked} blocked."
        if research_observation_summary:
            research_summary = f"{research_summary} {research_observation_summary}"

    engineering_summary = engineering.get("summary") if isinstance(engineering.get("summary"), dict) else {}
    issues = engineering.get("issues") if isinstance(engineering.get("issues"), list) else []
    if not issues:
        issues = engineering.get("rows") if isinstance(engineering.get("rows"), list) else []
    top_issue = issues[0] if issues and isinstance(issues[0], dict) else {}
    blocking_count = int(engineering_summary.get("blocking_issues") or 0)
    degraded_count = int(engineering_summary.get("degraded_issues") or 0)
    top_blocker = str(top_issue.get("operator_issue") or top_issue.get("issue") or "No top blocker reported.")

    source_mismatch = bool(requested_day != source_day)
    missing_required = [name for name, ok in (("canonical operator state", canonical_ok), ("runtime truth", runtime_ok), ("verified runtime graph", verified_graph_ok), ("command center queue audit", queue_ok)) if not ok]
    state = "NORMAL"
    if source_mismatch or missing_required:
        state = "BLOCKED"
    elif highest_layer.upper() == "BLOCKED" or runtime_class.upper() in {"PARTIAL_CONTEXT", "BLOCKED", "UNKNOWN"}:
        state = "BLOCKED"
    elif actions:
        state = "NEEDS_USER_ACTION"
    elif research_active:
        state = "RESEARCH_RUNNING"
    elif not did_anything_run:
        state = "NO_ACTIVITY"
    elif degraded_count > 0 or not pnl_ok or not research_doctor_ok:
        state = "DEGRADED"

    if state == "BLOCKED":
        headline = "Aegis is in PAPER MODE."
        if missing_required:
            headline = "Aegis is in PAPER MODE with partial evidence."
        summary = "Research, candidate generation, and paper tracking are active. Live trading and broker execution remain disabled by policy."
    elif state == "NEEDS_USER_ACTION":
        headline = "David action is required."
        summary = f"{len(actions)} current-day action needs review."
    elif state == "NO_ACTIVITY":
        headline = "Aegis has no activity to report today."
        summary = "Paper research is active and no operator action is required."
    elif state == "RESEARCH_RUNNING":
        headline = "Aegis is collecting research evidence in PAPER MODE."
        summary = "No operator action is required while research evidence accumulates."
    elif state == "DEGRADED":
        headline = "Aegis is readable, but some information is incomplete."
        summary = "Use the summaries below, and treat incomplete areas as monitoring-only."
    else:
        headline = "Aegis is in PAPER MODE."
        summary = "Research, candidate generation, and paper tracking are active. Live trading and broker execution remain disabled."

    safety = {
        "mode_label": "PAPER MODE",
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "live_trading_allowed": False,
        "autonomous_live_trading_allowed": False,
    }

    scheduled_at = str(session.get("scheduled_run_time") or "")
    last_completed_run_at = str(paper_session.get("generated_at_utc") or paper_session.get("generated_at") or last_run_at or "")
    last_completed_run_label = "No completed run confirmed"
    if last_completed_run_at:
        last_completed_run_label = "Paper session ledger completed"
    next_label = "No next scheduled run is confirmed."
    next_scheduled_at = ""
    next_expected = "Aegis will continue monitoring until another scheduled run or data observation is recorded."
    if scheduled_at:
        try:
            scheduled_dt = datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))
        except Exception:
            scheduled_dt = None
        if scheduled_dt and scheduled_dt > datetime.now(timezone.utc):
            next_label = "Next paper session"
            next_scheduled_at = scheduled_at
            next_expected = f"Aegis is waiting for the scheduled paper session at {scheduled_at}."
        else:
            next_label = "No future run confirmed"
            next_expected = f"The scheduled paper session at {scheduled_at} is already in the activity log. No later run is confirmed."

    evidence_refs = [
        {"label": "Verified runtime graph", "path": str(verified_graph_path), "available": verified_graph_ok},
        {"label": "Research daily scorecard", "path": str(research_daily_scorecard_path), "available": research_daily_scorecard_ok},
        {"label": "Daily research integrity audit", "path": str(daily_research_integrity_audit_path), "available": daily_research_integrity_audit_ok},
        {"label": "Market data universe consistency", "path": str(market_data_universe_consistency_path), "available": market_data_universe_consistency_ok},
        {"label": "Data action routing", "path": str(data_action_routing_path), "available": data_action_routing_ok},
        {"label": "Generated hypothesis validation proof", "path": str(generated_hypothesis_validation_proof_path), "available": generated_hypothesis_validation_proof_ok},
        {"label": "Operator action queue", "path": str(operator_action_queue_path), "available": operator_action_queue_ok},
        {"label": "Hypothesis workflow state", "path": str(hypothesis_workflow_state_path), "available": hypothesis_workflow_state_ok},
        {"label": "Generated hypothesis throughput", "path": str(generated_hypothesis_throughput_path), "available": generated_hypothesis_throughput_ok},
        {"label": "Oil Shock candidate flow", "path": str(oil_shock_candidate_flow_path), "available": oil_shock_candidate_flow_ok},
        {"label": "Oil Shock candidate construction", "path": str(oil_shock_candidate_construction_path), "available": oil_shock_candidate_construction_ok},
        {"label": "Generated hypothesis signal-to-candidate", "path": str(generated_hypothesis_signal_to_candidate_path), "available": generated_hypothesis_signal_to_candidate_ok},
        {"label": "Generated hypothesis approval event lineage", "path": str(generated_hypothesis_approval_event_lineage_path), "available": generated_hypothesis_approval_event_lineage_ok},
        {"label": "Generated hypothesis governance bridge", "path": str(generated_hypothesis_governance_bridge_path), "available": generated_hypothesis_governance_bridge_ok},
        {"label": "Generated hypothesis paper setup bridge", "path": str(generated_hypothesis_paper_setup_bridge_path), "available": generated_hypothesis_paper_setup_bridge_ok},
        {"label": "Macro Calendar data readiness", "path": str(macro_calendar_data_readiness_path), "available": macro_calendar_data_readiness_ok},
        {"label": "Research quality engine", "path": str(research_quality_engine_path), "available": research_quality_engine_ok},
        {"label": "Hypothesis decision policy", "path": str(hypothesis_decision_policy_path), "available": hypothesis_decision_policy_ok},
        {"label": "Research allocation recommendation", "path": str(research_allocation_recommendation_path), "available": research_allocation_recommendation_ok},
        {"label": "Research follow-through control", "path": str(research_follow_through_control_path), "available": research_follow_through_control_ok},
        {"label": "AI research intelligence summary", "path": str(ai_research_intelligence_summary_path), "available": ai_research_intelligence_summary_ok},
        {"label": "Runtime truth", "path": str(runtime_path), "available": runtime_ok},
        {"label": "Audit handoff", "path": str(audit_handoff_path), "available": audit_handoff_ok},
        {"label": "Canonical operator state", "path": str(canonical_path), "available": canonical_ok},
        {"label": "Command Center queue audit", "path": str(queue_path), "available": queue_ok},
        {"label": "Paper session ledger", "path": str(paper_session_path), "available": paper_session_ok},
        {"label": "Paper position ledger", "path": str(paper_positions_path), "available": paper_positions_ok},
        {"label": "Paper outcome auto-closure", "path": str(paper_auto_closure_path), "available": paper_auto_closure_ok},
        {"label": "Validation samples", "path": str(validation_samples_path), "available": validation_samples_ok},
        {"label": "Statistical sufficiency", "path": str(statistical_sufficiency_path), "available": statistical_sufficiency_ok},
        {"label": "Candidate state", "path": str(candidate_state_path), "available": candidate_state_ok},
        {"label": "Operator portfolio valuation estimate", "path": str(valuation_estimate_path), "available": bool(valuation_estimate)},
        {"label": "Operator Action Model", "path": str(operator_action_model_path), "available": bool(operator_action_model)},
        {"label": "Scheduled Run Readiness", "path": str(scheduled_run_readiness_path), "available": bool(scheduled_run_readiness)},
        {"label": "Scheduled Run Reconciliation", "path": str(scheduled_run_reconciliation_path), "available": bool(scheduled_run_reconciliation)},
    ]
    read_errors = [
        {"source": "canonical operator state", "error": canonical_err},
        {"source": "runtime truth", "error": runtime_err},
        {"source": "queue audit", "error": queue_err},
        {"source": "paper session ledger", "error": paper_session_err},
        {"source": "paper position ledger", "error": paper_positions_err},
        {"source": "candidate state", "error": candidate_state_err},
        {"source": "paper PnL report", "error": pnl_err},
        {"source": "research doctor", "error": research_doctor_err},
        {"source": "research samples", "error": research_samples_err},
        {"source": "validation samples", "error": validation_samples_err},
        {"source": "statistical sufficiency", "error": statistical_sufficiency_err},
        {"source": "outcome registry", "error": outcome_registry_err},
        {"source": "paper outcome auto-closure", "error": paper_auto_closure_err},
        {"source": "research allocation", "error": research_allocation_err},
        {"source": "engineering priority queue", "error": engineering_err},
        {"source": "verified runtime graph", "error": verified_graph_err},
        {"source": "research daily scorecard", "error": research_daily_scorecard_err},
        {"source": "daily research integrity audit", "error": daily_research_integrity_audit_err},
        {"source": "market data universe consistency", "error": market_data_universe_consistency_err},
        {"source": "data action routing", "error": data_action_routing_err},
        {"source": "generated hypothesis validation proof", "error": generated_hypothesis_validation_proof_err},
        {"source": "operator action queue", "error": operator_action_queue_err},
        {"source": "hypothesis workflow state", "error": hypothesis_workflow_state_err},
        {"source": "generated hypothesis throughput", "error": generated_hypothesis_throughput_err},
        {"source": "oil shock candidate flow", "error": oil_shock_candidate_flow_err},
        {"source": "oil shock candidate construction", "error": oil_shock_candidate_construction_err},
        {"source": "generated hypothesis signal-to-candidate", "error": generated_hypothesis_signal_to_candidate_err},
        {"source": "generated hypothesis approval event lineage", "error": generated_hypothesis_approval_event_lineage_err},
        {"source": "generated hypothesis governance bridge", "error": generated_hypothesis_governance_bridge_err},
        {"source": "generated hypothesis paper setup bridge", "error": generated_hypothesis_paper_setup_bridge_err},
        {"source": "macro calendar data readiness", "error": macro_calendar_data_readiness_err},
        {"source": "research quality engine", "error": research_quality_engine_err},
        {"source": "hypothesis decision policy", "error": hypothesis_decision_policy_err},
        {"source": "research allocation recommendation", "error": research_allocation_recommendation_err},
        {"source": "research follow-through control", "error": research_follow_through_control_err},
        {"source": "ai research intelligence summary", "error": ai_research_intelligence_summary_err},
        {"source": "control packet", "error": control_packet_err},
        {"source": "operator action model", "error": operator_action_model_err},
        {"source": "scheduled run readiness", "error": scheduled_run_readiness_err},
        {"source": "scheduled run reconciliation", "error": scheduled_run_reconciliation_err},
    ]
    raw_signal_count = int(candidate_generation_diagnostics.get("raw_signal_count") or activity_summary["raw_signals"] or 0)
    display_candidate_count = max(0, raw_signal_count - int(auto_closure_summary.get("auto_closed_count") or activity_summary["paper_outcomes_closed_today"] or 0))
    auto_promoted_count = int(display_candidate_count or lifecycle_counts.get("auto_promoted_to_paper_tracking_count") or candidate_generation_diagnostics.get("auto_promoted_to_paper_tracking_count") or activity_summary["paper_positions_opened"] or 0)
    valid_candidate_count = int(display_candidate_count or auto_promoted_count or candidate_generation_diagnostics.get("auto_promoted_to_paper_tracking_count") or current_candidate_count or 0)
    blocked_observation_count = int(lifecycle_counts.get("blocked_from_paper_count") or candidate_generation_diagnostics.get("blocked_from_paper_count") or 0)
    certified_price_count = int(lifecycle_counts.get("certified_price_candidate_count") or raw_signal_count or 0)
    missing_entry_mark_count = max(0, raw_signal_count - certified_price_count)
    usable_observation_count = max(0, open_count - blocked_observation_count)
    closed_today_count = int(auto_closure_summary.get("auto_closed_count") or activity_summary["paper_outcomes_closed_today"] or 0)
    manual_review_count = int(auto_closure_summary.get("manual_review_queue_count") or activity_summary["manual_review_queue_count"] or 0)
    research_allocation_summary = research_allocation.get("summary") if isinstance(research_allocation.get("summary"), dict) else {}
    allocation_decision_count = int(research_allocation_summary.get("decision_count") or 0)
    allocation_state = "UPDATED" if research_allocation_ok else "ACTIVE"
    david_action_label = "Required" if bool(actions) else "None"
    last_successful_run_value = last_completed_run_at or "Unavailable"
    next_scheduled_run_value = next_scheduled_at or next_label
    latest_material_change_value = str(latest_material_change.get("label") or "No material change recorded after the last run.")

    def _truth_audit_row(field_id: str, label: str, displayed_value: Any, source_value: Any, source_name: str, source_path: Path | str) -> Dict[str, Any]:
        displayed_text = str(displayed_value)
        source_text = str(source_value)
        return {
            "field_id": field_id,
            "label": label,
            "displayed_value": displayed_text,
            "source_value": source_text,
            "authoritative_report_source": source_name,
            "source_path": str(source_path),
            "match": displayed_text == source_text,
        }

    command_center_truth_audit = [
        _truth_audit_row("aegis_mode_status", "Aegis mode/status", safety["mode_label"], "PAPER MODE", "operator safety policy envelope", runtime_path),
        _truth_audit_row("raw_signals", "Raw signals", raw_signal_count, raw_signal_count, "aegis_candidate_generation_diagnostics_v1", candidate_generation_diagnostics.get("source_path") or canonical_path),
        _truth_audit_row("valid_candidates", "Valid candidates", valid_candidate_count, valid_candidate_count, "aegis_candidate_contracts_v1", lifecycle_counts.get("source_path") or canonical_path),
        _truth_audit_row("auto_promoted", "Auto-promoted", auto_promoted_count, auto_promoted_count, "aegis_candidate_to_paper_lifecycle_v1", lifecycle_counts.get("source_path") or canonical_path),
        _truth_audit_row("open_observations", "Open observations", open_count, outcome_open_count, "aegis_outcome_registry_v1", outcome_registry_path),
        _truth_audit_row("usable_observations", "Usable observations", usable_observation_count, usable_observation_count, "aegis_outcome_registry_v1 + aegis_candidate_to_paper_lifecycle_v1", outcome_registry_path),
        _truth_audit_row("blocked_observations", "Blocked observations", blocked_observation_count, blocked_observation_count, "aegis_candidate_to_paper_lifecycle_v1", lifecycle_counts.get("source_path") or canonical_path),
        _truth_audit_row("missing_entry_marks", "Missing entry marks", missing_entry_mark_count, missing_entry_mark_count, "aegis_entry_reference_price_certification_v1", lifecycle_counts.get("entry_reference_price_certification_path") or canonical_path),
        _truth_audit_row("closed_outcomes", "Closed outcomes", closed_outcome_count, int(outcome_summary.get("closed_outcomes") or outcome_summary.get("closed_outcome_count") or 0), "aegis_outcome_registry_v1", outcome_registry_path),
        _truth_audit_row("closed_today", "Closed today", closed_today_count, closed_today_count, "aegis_paper_outcome_auto_closure_v1", paper_auto_closure_path),
        _truth_audit_row("included_samples", "Included samples", included_sample_count, included_sample_count, "aegis_validation_samples_v1", validation_samples_path),
        _truth_audit_row("manual_review_queue", "Manual review queue", manual_review_count, manual_review_count, "aegis_paper_outcome_auto_closure_v1", paper_auto_closure_path),
        _truth_audit_row("david_action", "David action", david_action_label, david_action_label, "aegis_command_center_queue_audit_v1", queue_path),
        _truth_audit_row("research_allocation_decisions", "Research allocation decisions", allocation_decision_count, allocation_decision_count, "aegis_research_capital_allocation_v1", research_allocation_path),
        _truth_audit_row("allocation_state", "Allocation state", allocation_state, allocation_state, "aegis_research_capital_allocation_v1", research_allocation_path),
        _truth_audit_row("current_bottleneck", "Current bottleneck", current_bottleneck, current_bottleneck, "aegis_statistical_sufficiency_v1 + aegis_validation_samples_v1", statistical_sufficiency_path),
        _truth_audit_row("last_successful_run", "Last successful run", last_successful_run_value, last_successful_run_value, "aegis_paper_session_ledger_v1", paper_session_path),
        _truth_audit_row("next_scheduled_run", "Next scheduled run", next_scheduled_run_value, next_scheduled_run_value, "aegis_paper_session_ledger_v1", paper_session_path),
        _truth_audit_row("latest_material_change", "Latest material change", latest_material_change_value, latest_material_change_value, "latest material research evidence", latest_material_change.get("source_path") or paper_auto_closure_path),
    ]

    limitations: List[str] = []
    if runtime_class.upper() != "READY":
        limitations.append("Aegis cannot certify all runtime capabilities for today.")
    if blocked_capabilities:
        limitations.append("Blocked capabilities include: " + ", ".join(str(item) for item in blocked_capabilities[:5]) + ("." if len(blocked_capabilities) <= 5 else ", ..."))
    if missing_required:
        limitations.append("Missing required evidence: " + ", ".join(missing_required) + ".")
    if source_mismatch:
        limitations.append(f"Requested day {requested_day} does not match source day {source_day}.")
    if pnl.get("full_portfolio_pnl_status") and str(pnl.get("full_portfolio_pnl_status")).upper() != "CANONICAL":
        limitations.append("Some position prices or P&L are incomplete for this day.")

    return {
        "ok": True,
        "requested_day": requested_day,
        "source_day": source_day,
        "generated_at": now,
        "state": state,
        "headline": headline,
        "summary": summary,
        "operator_headline": "Aegis is in PAPER MODE." if not actions else "David action is required.",
        "operator_answer": "Research, candidate generation, and paper tracking are active. Live trading, broker execution, autonomous execution, and trade advice remain disabled.",
        "operator_status_label": "PAPER MODE",
        "operator_status_explanation": "Paper research is active; safety policy keeps live trading and broker execution disabled.",
        "user_action_required": bool(actions),
        "operator_action_model_v1": operator_action_model,
        "operator_action_model": operator_action_model,
        "scheduled_run_readiness": _today_scheduled_run_readiness_view_v1(scheduled_run_readiness, scheduled_run_reconciliation),
        "scheduled_run_readiness_v1": scheduled_run_readiness,
        "scheduled_run_reconciliation_v1": scheduled_run_reconciliation,
        "candidate_generation_diagnostics": candidate_generation_diagnostics,
        "command_center_truth_audit": command_center_truth_audit,
        "command_center_truth_source_policy": "Primary Command Center fields are sourced from authoritative reports; cached/operator-generated fallback state must not provide displayed primary values.",
        "research_daily_scorecard": research_daily_scorecard if isinstance(research_daily_scorecard, dict) else {},
        "daily_research_integrity_audit_v1": daily_research_integrity_audit if isinstance(daily_research_integrity_audit, dict) else {},
        "market_data_universe_consistency_v1": market_data_universe_consistency if isinstance(market_data_universe_consistency, dict) else {},
        "data_action_routing_v1": data_action_routing if isinstance(data_action_routing, dict) else {},
        "generated_hypothesis_validation_proof_v1": generated_hypothesis_validation_proof if isinstance(generated_hypothesis_validation_proof, dict) else {},
        "operator_action_queue_v1": operator_action_queue if isinstance(operator_action_queue, dict) else {},
        "hypothesis_workflow_state_v1": hypothesis_workflow_state if isinstance(hypothesis_workflow_state, dict) else {},
        "generated_hypothesis_throughput_v1": generated_hypothesis_throughput if isinstance(generated_hypothesis_throughput, dict) else {},
        "oil_shock_candidate_flow_v1": oil_shock_candidate_flow if isinstance(oil_shock_candidate_flow, dict) else {},
        "oil_shock_candidate_construction_v1": oil_shock_candidate_construction if isinstance(oil_shock_candidate_construction, dict) else {},
        "generated_hypothesis_signal_to_candidate_v1": generated_hypothesis_signal_to_candidate if isinstance(generated_hypothesis_signal_to_candidate, dict) else {},
        "generated_hypothesis_approval_event_lineage_v1": generated_hypothesis_approval_event_lineage if isinstance(generated_hypothesis_approval_event_lineage, dict) else {},
        "generated_hypothesis_governance_bridge_v1": generated_hypothesis_governance_bridge if isinstance(generated_hypothesis_governance_bridge, dict) else {},
        "generated_hypothesis_paper_setup_bridge_v1": generated_hypothesis_paper_setup_bridge if isinstance(generated_hypothesis_paper_setup_bridge, dict) else {},
        "macro_calendar_data_readiness_v1": macro_calendar_data_readiness if isinstance(macro_calendar_data_readiness, dict) else {},
        "research_quality_engine_v1": research_quality_engine if isinstance(research_quality_engine, dict) else {},
        "hypothesis_decision_policy_v1": hypothesis_decision_policy if isinstance(hypothesis_decision_policy, dict) else {},
        "research_allocation_recommendation_v1": research_allocation_recommendation if isinstance(research_allocation_recommendation, dict) else {},
        "research_follow_through_control_v1": research_follow_through_control if isinstance(research_follow_through_control, dict) else {},
        "ai_research_intelligence_summary_v1": ai_research_intelligence_summary if isinstance(ai_research_intelligence_summary, dict) else {},
        "actions": actions,
        "safety": safety,
        "activity": {
            "did_anything_run": did_anything_run,
            "summary": activity_summary,
            "last_run_at": last_completed_run_at,
            "last_successful_run_at": last_completed_run_at,
            "last_completed_run_at": last_completed_run_at,
            "last_completed_run_label": last_completed_run_label,
            "latest_material_change": latest_material_change,
            "material_events": material_events,
            "events": activity_events,
        },
        "open_positions": {
            "count": open_count,
            "summary": open_summary,
            "data_quality": open_quality,
            "pnl_status": str(pnl.get("full_portfolio_pnl_status") or pnl.get("data_quality_status") or "UNKNOWN"),
            "valuation_estimate_status": str(valuation_estimate.get("valuation_status") or "ESTIMATE_UNAVAILABLE"),
            "latest_available_mark_date": str(valuation_estimate.get("latest_available_mark_date") or ""),
            "estimated_portfolio_value": valuation_estimate.get("estimated_portfolio_value"),
            "estimated_unrealized_pnl": valuation_estimate.get("estimated_unrealized_pnl"),
        },
        "operator_portfolio_valuation_estimate_v1": valuation_estimate,
        "operator_portfolio_valuation_estimate": valuation_estimate,
        "candidates": {
            "status": candidate_status,
            "output_count": output_count,
            "today_count": current_candidate_count,
            "actionable_count": actionable_count,
            "blocked_reason": "" if actionable_count or output_count else str(run_summary.get("classification") or "No current-day output candidates qualified."),
            "summary": candidate_summary,
        },
        "research": {
            "state": research_state,
            "active_count": research_active,
            "waiting_count": research_waiting,
            "blocked_count": research_blocked,
            "collecting_evidence_count": collecting_count,
            "next_observation": str(research_doctor.get("next_scheduled_research_run") or ""),
            "sample_summary": research_observation_summary,
            "summary": research_summary,
        },
        "validation": {
            "state": "WAITING_FOR_CLOSED_OUTCOMES" if int(activity_summary["closed_outcomes"]) <= 0 else "ACTIVE",
            "closed_outcome_count": int(activity_summary["closed_outcomes"]),
            "validation_sample_count": int(activity_summary["validation_samples"]),
            "paper_outcomes_closed_today": closed_today_count,
            "manual_review_queue_count": manual_review_count,
            "manual_review_queue": paper_auto_closure.get("manual_review_queue") if isinstance(paper_auto_closure.get("manual_review_queue"), list) else [],
            "open_outcome_count": open_count,
            "summary": "Waiting for closed outcomes" if int(activity_summary["closed_outcomes"]) <= 0 else "Closed outcomes are available for validation.",
            "current_bottleneck": current_bottleneck,
            "current_bottleneck_detail": current_bottleneck_detail,
            "validation_ready_count": validation_ready_count,
            "underpowered_count": underpowered_count,
            "source_path": str(outcome_registry_path) if outcome_registry_ok else "",
            "paper_outcome_auto_closure_source_path": str(paper_auto_closure_path) if paper_auto_closure_ok else "",
            "validation_samples_source_path": str(validation_samples_path) if validation_samples_ok else "",
            "statistical_sufficiency_source_path": str(statistical_sufficiency_path) if statistical_sufficiency_ok else "",
        },
        "research_allocation": {
            "state": allocation_state,
            "decision_count": allocation_decision_count,
            "source_path": str(research_allocation_path) if research_allocation_ok else "",
        },
        "system_health": {
            "state": "BLOCKED" if state == "BLOCKED" else ("DEGRADED" if degraded_count else "NORMAL"),
            "top_blocker": top_blocker,
            "blocking_count": blocking_count,
            "degraded_count": degraded_count,
            "graph_status": graph_status,
            "audit_blocker_count": audit_blockers,
        },
        "next": {
            "label": next_label,
            "scheduled_at": next_scheduled_at,
            "last_scheduled_at": scheduled_at,
            "expected_output": next_expected,
            "confidence": "MEDIUM" if scheduled_at else "LOW",
        },
        "waiting": {
            "waiting_for_data_count": int(engineering_summary.get("waiting_for_data") or 0),
            "waiting_for_time_count": int(engineering_summary.get("waiting_for_time") or 0),
            "research_samples_summary": research_observation_summary or samples_summary,
        },
        "source_paths": {
            "canonical_operator_state": str(canonical_path),
            "runtime_truth": str(runtime_path),
            "verified_runtime_graph": str(verified_graph_path),
            "research_daily_scorecard": str(research_daily_scorecard_path),
            "data_action_routing": str(data_action_routing_path),
            "generated_hypothesis_validation_proof": str(generated_hypothesis_validation_proof_path),
            "operator_action_queue": str(operator_action_queue_path),
            "hypothesis_workflow_state": str(hypothesis_workflow_state_path),
            "generated_hypothesis_throughput": str(generated_hypothesis_throughput_path),
            "oil_shock_candidate_flow": str(oil_shock_candidate_flow_path),
            "oil_shock_candidate_construction": str(oil_shock_candidate_construction_path),
            "generated_hypothesis_signal_to_candidate": str(generated_hypothesis_signal_to_candidate_path),
            "generated_hypothesis_governance_bridge": str(generated_hypothesis_governance_bridge_path),
            "generated_hypothesis_paper_setup_bridge": str(generated_hypothesis_paper_setup_bridge_path),
            "macro_calendar_data_readiness": str(macro_calendar_data_readiness_path),
            "research_quality_engine": str(research_quality_engine_path),
            "hypothesis_decision_policy": str(hypothesis_decision_policy_path),
            "research_allocation_recommendation": str(research_allocation_recommendation_path),
            "research_follow_through_control": str(research_follow_through_control_path),
            "ai_research_intelligence_summary": str(ai_research_intelligence_summary_path),
            "command_center_queue_audit": str(queue_path),
        },
        "trust": {
            "label": "Current-day evidence was loaded, but operating capabilities remain monitoring-only." if state == "BLOCKED" else "Current-day evidence supports this summary.",
            "evidence_current": not source_mismatch and not missing_required,
            "limitations": limitations,
            "evidence_refs": evidence_refs,
            "read_errors": [row for row in read_errors if row.get("error")],
        },
    }

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
        "/aegis-command-center",
        "/aegis-positions",
        "/aegis-positions-diagnostics",
        "/aegis-trading-desk",
        "/aegis-operations",
        "/aegis-research-workspace",
        "/aegis-audit-evidence",
        "/aegis-opportunities",
        "/aegis-change-control",
        "/aegis-change-control-lab",
        "/aegis-candidates",
        "/aegis-candidate-funnel",
        "/aegis-candidate-lineage",
        "/aegis-exit-review",
        "/aegis-open-paper-positions",
        "/aegis-theses",
        "/aegis-runtime-timeline",
        "/aegis-repair-center",
        "/aegis-edge-lab",
        "/aegis-paper-performance",
        "/aegis-sleeve-validation",
        "/aegis-sleeve-analytics",
        "/aegis-position-review",
        "/aegis-performance",
        "/aegis-journal",
        "/aegis-captured-trades",
        "/aegis-today",
        "/aegis-review",
        "/aegis-research",
        "/aegis-history",
        "/capital-map",
        "/portfolios",
        "/cio-opportunities",
        "/retirement-simulator",
        "/advisor-oversight",
        "/documents",
        "/carolyn",
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
        "/research-lab/review",
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
        "/aegis-verified-runtime",
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
            "/aegis-command-center": "/aegis-command-center" in self.SHELL_ROUTES,
            "/aegis-positions": "/aegis-positions" in self.SHELL_ROUTES,
            "/aegis-positions-diagnostics": "/aegis-positions-diagnostics" in self.SHELL_ROUTES,
            "/aegis-trading-desk": "/aegis-trading-desk" in self.SHELL_ROUTES,
            "/aegis-operations": "/aegis-operations" in self.SHELL_ROUTES,
            "/aegis-research-workspace": "/aegis-research-workspace" in self.SHELL_ROUTES,
            "/aegis-audit-evidence": "/aegis-audit-evidence" in self.SHELL_ROUTES,
            "/aegis-runtime": "/aegis-runtime" in self.SHELL_ROUTES,
            "/aegis-opportunities": "/aegis-opportunities" in self.SHELL_ROUTES,
            "/aegis-change-control": "/aegis-change-control" in self.SHELL_ROUTES,
            "/aegis-candidates": "/aegis-candidates" in self.SHELL_ROUTES,
            "/aegis-exit-review": "/aegis-exit-review" in self.SHELL_ROUTES,
            "/aegis-open-paper-positions": "/aegis-open-paper-positions" in self.SHELL_ROUTES,
            "/aegis-theses": "/aegis-theses" in self.SHELL_ROUTES,
            "/aegis-edge-lab": "/aegis-edge-lab" in self.SHELL_ROUTES,
            "/aegis-paper-performance": "/aegis-paper-performance" in self.SHELL_ROUTES,
            "/aegis-sleeve-validation": "/aegis-sleeve-validation" in self.SHELL_ROUTES,
            "/aegis-performance": "/aegis-performance" in self.SHELL_ROUTES,
            "/aegis-journal": "/aegis-journal" in self.SHELL_ROUTES,
            "/aegis-captured-trades": "/aegis-captured-trades" in self.SHELL_ROUTES,
            "/aegis-today": "/aegis-today" in self.SHELL_ROUTES,
            "/aegis-review": "/aegis-review" in self.SHELL_ROUTES,
            "/aegis-research": "/aegis-research" in self.SHELL_ROUTES,
            "/aegis-history": "/aegis-history" in self.SHELL_ROUTES,
            "/aegis-runtime-truth": "/aegis-runtime-truth" in self.SHELL_ROUTES,
            "/aegis-verified-runtime": "/aegis-verified-runtime" in self.SHELL_ROUTES,
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
            "/api/research-lab/review-brief/latest": True,
            "/api/research-lab/console": True,
            "/api/aegis/thesis-graph": True,
            "/api/aegis/commands/registry": True,
            "/api/aegis/commands/execute": True,
            "/api/aegis/performance-report": True,
            "/api/aegis/performance/latest": True,
            "/api/aegis/performance/advisor-benchmark": True,
            "/api/aegis/change-control": True,
            "/api/aegis/change-control/latest": True,
            "/api/aegis/change-control/intelligence/latest": True,
            "/api/aegis/exit-review/latest": True,
            "/api/aegis/research-portfolio/latest": True,
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
        elif u.path.startswith("/aegis-lite/assets/"):
            rel = u.path.removeprefix("/aegis-lite/").lstrip("/")
        elif not normalized.startswith("/api/") and "." not in Path(normalized).name:
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

        if path == "/api/aegis/commands/status":
            from ops.aegis.operator_command_lifecycle_v1 import command_status_v1
            command_id = str((qs.get("command_id") or [""])[0] or "").strip()
            day = str(requested_day or _projection_day_for_report(requested_day, "operator_state_snapshot_v1"))
            if not command_id:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error_code": "COMMAND_ID_REQUIRED", "message": "command_id is required.", "details": {}})
                return True
            self._send_json(HTTPStatus.OK, command_status_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, command_id=command_id))
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

        if path == "/api/research-lab/validation-engine/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_research_validation_result_v1"))
            paths = {
                "hypothesis_registry": research_hypothesis_registry_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "validation_protocol": research_validation_protocol_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "validation_run": research_validation_run_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "validation_result": research_validation_result_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "promotion_gate": research_promotion_gate_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "outcome_feedback": research_outcome_feedback_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
            }
            artifacts = {}
            for name, path_obj in paths.items():
                payload, _err = _safe_read_json(path_obj)
                artifacts[name] = payload if isinstance(payload, dict) else {}
            missing = [name for name, payload in artifacts.items() if not payload]
            status = HTTPStatus.OK if not missing else HTTPStatus.NOT_FOUND
            self._send_json(status, {
                "ok": not missing,
                "day_utc": day,
                "missing_artifacts": missing,
                "paths": {name: str(path_obj) for name, path_obj in paths.items()},
                "artifacts": artifacts,
            })
            return True

        if path == "/api/research-lab/hypothesis-proposal-promotion/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_hypothesis_proposal_promotion_v1"))
            paths = {
                "promotion": promotion_pipeline_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "evidence_packets": evidence_packets_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "shadow_trials": shadow_trials_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "promotion_packets": promotion_packets_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "approval_queue": approval_queue_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
            }
            artifacts = {}
            for name, path_obj in paths.items():
                payload, _err = _safe_read_json(path_obj)
                artifacts[name] = payload if isinstance(payload, dict) else {}
            missing = [name for name, payload in artifacts.items() if not payload]
            status = HTTPStatus.OK if not missing else HTTPStatus.NOT_FOUND
            self._send_json(status, {
                "ok": not missing,
                "day_utc": day,
                "missing_artifacts": missing,
                "paths": {name: str(path_obj) for name, path_obj in paths.items()},
                "artifacts": artifacts,
                "safety_statement": "This is paper research only. Not trade advice. No broker execution. No live trading.",
            })
            return True

        if path == "/api/research-lab/approved-hypothesis-paper-setup/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_approved_hypothesis_paper_tracking_setup_v1"))
            paths = {
                "paper_sleeve_blueprint": paper_sleeve_blueprint_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "paper_readiness_certification": paper_readiness_certification_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
                "approved_hypothesis_paper_tracking_setup": approved_hypothesis_paper_tracking_setup_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day),
            }
            artifacts = {}
            for name, path_obj in paths.items():
                payload, _err = _safe_read_json(path_obj)
                artifacts[name] = payload if isinstance(payload, dict) else {}
            missing = [name for name, payload in artifacts.items() if not payload]
            status = HTTPStatus.OK if not missing else HTTPStatus.NOT_FOUND
            self._send_json(status, {
                "ok": not missing,
                "day_utc": day,
                "missing_artifacts": missing,
                "paths": {name: str(path_obj) for name, path_obj in paths.items()},
                "artifacts": artifacts,
                "safety_statement": "This is paper research only. Not trade advice. No broker execution. No live trading.",
            })
            return True

        if path == "/api/research-lab/hypothesis-workflow-state/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_hypothesis_workflow_state_v1"))
            path_obj = hypothesis_workflow_state_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(path_obj)
            artifact = payload if isinstance(payload, dict) else {}
            self._send_json(HTTPStatus.OK if artifact else HTTPStatus.NOT_FOUND, {
                "ok": bool(artifact),
                "day_utc": day,
                "path": str(path_obj),
                "artifact": artifact,
                "safety_statement": "This is paper research only. Not trade advice. No broker execution. No live trading.",
            })
            return True

        if path == "/api/research-lab/operator-action-queue/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_operator_action_queue_v1"))
            path_obj = operator_action_queue_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(path_obj)
            artifact = payload if isinstance(payload, dict) else {}
            self._send_json(HTTPStatus.OK if artifact else HTTPStatus.NOT_FOUND, {
                "ok": bool(artifact),
                "day_utc": day,
                "path": str(path_obj),
                "artifact": artifact,
                "safety_statement": "This is paper research only. Not trade advice. No broker execution. No live trading.",
            })
            return True

        if path == "/api/research-lab/hypothesis-workflow-replay-verification/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_hypothesis_workflow_replay_verification_v1"))
            path_obj = hypothesis_workflow_replay_verification_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(path_obj)
            artifact = payload if isinstance(payload, dict) else {}
            self._send_json(HTTPStatus.OK if artifact else HTTPStatus.NOT_FOUND, {
                "ok": bool(artifact),
                "day_utc": day,
                "path": str(path_obj),
                "artifact": artifact,
                "safety_statement": "This is paper research only. Not trade advice. No broker execution. No live trading.",
            })
            return True

        if path == "/api/research-lab/macro-calendar-data-readiness/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_macro_calendar_data_readiness_v1"))
            path_obj = macro_calendar_data_readiness_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(path_obj)
            artifact = payload if isinstance(payload, dict) else {}
            self._send_json(HTTPStatus.OK if artifact else HTTPStatus.NOT_FOUND, {
                "ok": bool(artifact),
                "day_utc": day,
                "path": str(path_obj),
                "artifact": artifact,
                "safety_statement": "This is paper research only. Not trade advice. No broker execution. No live trading.",
            })
            return True

        if path == "/api/research-lab/generated-hypothesis-throughput/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_generated_hypothesis_throughput_v1"))
            path_obj = generated_hypothesis_throughput_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(path_obj)
            artifact = payload if isinstance(payload, dict) else {}
            self._send_json(HTTPStatus.OK if artifact else HTTPStatus.NOT_FOUND, {
                "ok": bool(artifact),
                "day_utc": day,
                "path": str(path_obj),
                "artifact": artifact,
                "safety_statement": "This is paper research only. Not trade advice. No broker execution. No live trading.",
            })
            return True

        if path == "/api/research-lab/review-brief/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_research_review_brief_v1"))
            payload = load_research_review_brief_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            if not payload:
                self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error_code": "RESEARCH_REVIEW_BRIEF_MISSING", "message": "Research Review brief artifact is missing. Run npm run aegis:research-review-brief.", "day_utc": day, "path": str(research_review_brief_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day))})
                return True
            self._send_json(HTTPStatus.OK, {"ok": True, "day_utc": day, "artifact": payload})
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




        if path == "/api/aegis/change-control/intelligence/latest":
            try:
                payload = build_change_control_intelligence_v1(truth_root=TRUTH_ROOT, day=requested_day, write=True)
                self._send_json(HTTPStatus.OK, {
                    "ok": payload.get("ok") is True,
                    "data": {
                        "snapshot": payload.get("snapshot"),
                        "advisor_score": payload.get("advisor_score"),
                        "ai_review": payload.get("ai_review"),
                        "paths": payload.get("paths"),
                        "validation": payload.get("validation"),
                    },
                    "errors": [] if payload.get("ok") else payload.get("validation", {}).get("failures", []),
                })
            except Exception as exc:
                self._send_json(HTTPStatus.OK, {
                    "ok": False,
                    "data": None,
                    "errors": ["CHANGE_CONTROL_INTELLIGENCE_UNAVAILABLE"],
                    "message": "Change Control Intelligence is unavailable. No stale recommendation is shown.",
                    "reason": str(exc),
                })
            return True

        if path in {"/api/aegis/change-control", "/api/aegis/change-control/latest"}:
            try:
                register = load_change_control_register_v1(CHANGE_CONTROL_REGISTER_PATH)
                validation = validate_change_control_register_v1(register)
                report = build_change_control_report_v1(register)
                self._send_json(HTTPStatus.OK, {
                    "ok": validation.get("ok") is True,
                    "data": {
                        "register": register,
                        "report": report,
                        "validation": validation,
                        "source_path": str(CHANGE_CONTROL_REGISTER_PATH),
                    },
                    "errors": [] if validation.get("ok") else validation.get("failures", []),
                })
            except Exception as exc:
                self._send_json(HTTPStatus.OK, {
                    "ok": False,
                    "data": None,
                    "errors": ["CHANGE_CONTROL_REGISTER_UNAVAILABLE"],
                    "message": "Change Control is unavailable. The enhancement register could not be loaded. No stale or fake items are shown.",
                    "reason": str(exc),
                    "source_path": str(CHANGE_CONTROL_REGISTER_PATH),
                })
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

        if path in {"/api/aegis/positions", "/api/aegis/positions/latest"}:
            day = _operator_shared_fact_day_v1(requested_day, "aegis_candidate_lifecycle_projection_v1")
            payload = _positions_lightweight_payload_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            sys.stderr.write(f"TIMING: positions_lightweight endpoint={path} payload_bytes={payload.get('payload_bytes')} rebuilt_on_request={payload.get('rebuilt_on_request')} read_ms={payload.get('timing_ms')}\n")
            self._send_json(HTTPStatus.OK, payload)
            return True

        if path in {"/api/aegis/repair-center", "/api/aegis/repair-center/latest"}:
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or requested_day or _projection_day_for_report(requested_day, "operator_state_snapshot_v1"))
            self._send_json(HTTPStatus.OK, {"ok": True, "data": build_repair_center_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)})
            return True

        if path in {"/api/aegis/performance/latest", "/api/aegis/performance-report", "/api/aegis/performance-report/latest"}:
            day = _operator_shared_fact_day_v1(requested_day, "aegis_paper_pnl_report_v1")
            self._send_json(HTTPStatus.OK, {"ok": True, "data": build_paper_performance_report_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)})
            return True

        if path in {"/api/aegis/operator-portfolio-valuation-estimate", "/api/aegis/operator-portfolio-valuation-estimate/latest"}:
            day = _operator_shared_fact_day_v1(requested_day, "aegis_paper_pnl_report_v1")
            payload, artifact_path = _operator_portfolio_valuation_estimate_payload_v1(GLOBAL_TRUTH_ROOT, day)
            self._send_json(HTTPStatus.OK, {"ok": True, "day_utc": day, "artifact_path": str(artifact_path), "data": payload})
            return True

        if path in {"/api/aegis/sleeve-analytics", "/api/aegis/sleeve-analytics/latest"}:
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or requested_day or _projection_day_for_report(requested_day, "aegis_sleeve_analytics_v1"))
            self._send_json(HTTPStatus.OK, {"ok": True, "data": build_sleeve_analytics_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)})
            return True

        if path in {"/api/aegis/research-portfolio", "/api/aegis/research-portfolio/latest"}:
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or requested_day or _projection_day_for_report(requested_day, "aegis_research_portfolio_v1"))
            self._send_json(HTTPStatus.OK, {"ok": True, "data": build_research_portfolio_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)})
            return True

        if path in {"/api/aegis/research-quality", "/api/aegis/research-quality/latest"}:
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or requested_day or _projection_day_for_report(requested_day, "aegis_research_quality_engine_v1"))
            quality = build_research_quality_engine_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            decisions = build_hypothesis_decision_policy_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, quality=quality)
            allocation = build_research_allocation_recommendation_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, decisions=decisions)
            follow_through = build_research_follow_through_control_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, quality=quality, decisions=decisions, allocation=allocation)
            ai_intelligence = build_ai_research_intelligence_bundle_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day).get("summary", {})
            generated_hypothesis_approval_event_lineage = build_generated_hypothesis_approval_event_lineage_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            generated_hypothesis_governance_bridge = build_generated_hypothesis_governance_bridge_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            generated_hypothesis_paper_setup_bridge = build_generated_hypothesis_paper_setup_bridge_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            oil_shock_candidate_construction = build_oil_shock_candidate_construction_v1(truth_root=GLOBAL_TRUTH_ROOT, repo_root=REPO_ROOT, day_utc=day)
            oil_shock_candidate_flow = build_oil_shock_candidate_flow_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            generated_hypothesis_signal_to_candidate = build_generated_hypothesis_signal_to_candidate_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            macro_calendar_data_readiness = build_macro_calendar_data_readiness_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            self._send_json(HTTPStatus.OK, {"ok": True, "data": {"schema_id": "aegis_research_quality_control_ui_model", "day_utc": day, "quality": quality, "decisions": decisions, "allocation": allocation, "follow_through": follow_through, "ai_intelligence": ai_intelligence, "generated_hypothesis_approval_event_lineage": generated_hypothesis_approval_event_lineage, "generated_hypothesis_governance_bridge": generated_hypothesis_governance_bridge, "generated_hypothesis_paper_setup_bridge": generated_hypothesis_paper_setup_bridge, "oil_shock_candidate_construction": oil_shock_candidate_construction, "oil_shock_candidate_flow": oil_shock_candidate_flow, "generated_hypothesis_signal_to_candidate": generated_hypothesis_signal_to_candidate, "macro_calendar_data_readiness": macro_calendar_data_readiness}})
            return True

        if path in {"/api/aegis/position-review", "/api/aegis/position-review/latest"}:
            day = _operator_shared_fact_day_v1(requested_day, "aegis_position_review_brief_v1")
            payload = load_position_review_brief_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            valuation_estimate, valuation_estimate_path = _operator_portfolio_valuation_estimate_payload_v1(GLOBAL_TRUTH_ROOT, day)
            readiness = build_surface_readiness_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            readiness_row = (readiness.get("surface_by_id") or {}).get("position_review", {}) if isinstance(readiness.get("surface_by_id"), dict) else {}
            if not payload:
                self._send_json(HTTPStatus.OK, {"ok": True, "data": {"schema_id": "aegis_position_review_unavailable", "day_utc": day, "requested_day": requested_day or day, "status": "UNAVAILABLE", "surface_readiness": readiness_row, "message": "Position Review unavailable for requested day. Current-day review artifacts are missing.", "missing_inputs": readiness_row.get("blocking_reasons", []), "briefs": [], "operator_portfolio_valuation_estimate_v1": valuation_estimate, "operator_portfolio_valuation_estimate_path": str(valuation_estimate_path)}})
                return True
            self._send_json(HTTPStatus.OK, {"ok": True, "data": {**payload, "surface_readiness": readiness_row, "operator_portfolio_valuation_estimate_v1": valuation_estimate, "operator_portfolio_valuation_estimate": valuation_estimate, "operator_portfolio_valuation_estimate_path": str(valuation_estimate_path)}})
            return True

        if path in {"/api/aegis/narrative-operational-analytics", "/api/aegis/narrative-operational-analytics/latest"}:
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or requested_day or _projection_day_for_report(requested_day, "narrative_operational_analytics_v1"))
            artifact_path, narrative = latest_json_v1(
                GLOBAL_TRUTH_ROOT,
                "narrative_operational_analytics_v1",
                day,
                "narrative_operational_analytics.v1.json",
            )
            if not isinstance(narrative, dict) or not narrative:
                paper_trade_evaluation = build_paper_trade_evaluation_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                narrative = build_narrative_operational_analytics_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    paper_trade_evaluation_projection=paper_trade_evaluation,
                    candidate_funnel_projection={},
                )
            content_hash = ""
            if artifact_path and Path(artifact_path).exists():
                content_hash = hashlib.sha256(Path(artifact_path).read_bytes()).hexdigest()
            data = {**narrative, "artifact_path": str(artifact_path or ""), "artifact_content_hash": content_hash or str(narrative.get("content_hash") or "")}
            self._send_json(HTTPStatus.OK, {"ok": True, "data": data, "artifact_path": str(artifact_path or ""), "artifact_content_hash": data.get("artifact_content_hash", "")})
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

        if path in {"/api/aegis/verified-runtime/portal-model", "/api/aegis/verified-runtime/portal-model/latest"}:
            self._send_json(HTTPStatus.OK, _load_verified_runtime_portal_model_v1(requested_day))
            return True


        if path in {"/api/aegis/operator/current-truth", "/api/aegis/operator/current-truth/latest"}:
            day_resolution = _operator_day_resolution_v1(requested_day)
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=str(day_resolution.get("resolved_day") or _operator_truth_day(requested_day)))
            current_truth = {**current_truth, "operational_day_resolution": day_resolution}
            self._send_json(HTTPStatus.OK, current_operator_truth_api_envelope_v1(current_truth))
            return True

        if path == "/api/aegis/operator/state-snapshot/latest":
            day_resolution = _operator_day_resolution_v1(requested_day)
            day = _resolved_operator_api_day_v1(requested_day, "operator_state_snapshot_v1")
            response = load_or_build_operator_state_snapshot_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            if isinstance(response.get("data"), dict):
                data = _merge_latest_domain_command_results_v1(response["data"], day)
                data["operational_day_resolution"] = {
                    **day_resolution,
                    "resolved_day": day,
                    "source_day": day,
                    "banner_message": f"Viewing latest valid operational trading session: {day}" if day_resolution.get("fallback_applied") else "",
                }
                trade_lifecycle_ledger = build_trade_lifecycle_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                paper_trade_evaluation = build_paper_trade_evaluation_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                exit_review_projection = build_exit_review_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                portfolio_context_projection = build_portfolio_context_projection_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    paper_trade_evaluation_projection=paper_trade_evaluation,
                    exit_review_projection=exit_review_projection,
                )
                repair_center_projection = build_repair_center_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                data["trade_lifecycle_ledger_v1"] = trade_lifecycle_ledger
                data["paper_trade_evaluation_projection_v1"] = paper_trade_evaluation
                data["paper_trade_evaluation_projection"] = paper_trade_evaluation
                data["exit_review_projection_v1"] = exit_review_projection
                data["exit_review_projection"] = exit_review_projection
                data["portfolio_context_projection_v1"] = portfolio_context_projection
                data["portfolio_context_projection"] = portfolio_context_projection
                data["repair_center_projection_v1"] = repair_center_projection
                data["repair_center_projection"] = repair_center_projection
                data = _attach_candidate_ui_projection_v1(
                    data,
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    operational_day_source="operator-state-snapshot-latest",
                )
                paper_operator_projection, paper_operator_projection_path = build_and_write_paper_operator_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                candidate_lifecycle_projection, candidate_lifecycle_projection_path = build_and_write_candidate_lifecycle_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                data["paper_operator_projection_v1"] = paper_operator_projection
                data["paper_operator_projection"] = paper_operator_projection
                data["candidate_lifecycle_projection_v1"] = candidate_lifecycle_projection
                data["candidate_lifecycle_projection"] = candidate_lifecycle_projection
                if isinstance(data.get("source_paths"), dict):
                    data["source_paths"]["paper_operator_projection"] = str(paper_operator_projection_path)
                    data["source_paths"]["candidate_lifecycle_projection"] = str(candidate_lifecycle_projection_path)
                attention_queue_projection = build_attention_queue_projection_v1(data, day_utc=day)
                data["attention_queue_projection_v1"] = attention_queue_projection
                data["attention_queue_projection"] = attention_queue_projection
                response = {**response, **data, "data": data}
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

        if path == "/api/aegis/paper-trade-evaluation/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "paper_trade_evaluation_projection_v1"))
            ledger = build_trade_lifecycle_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            projection = build_paper_trade_evaluation_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            ledger_paths = write_trade_lifecycle_ledger_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=ledger)
            projection["trade_lifecycle_ledger"]["artifact_path"] = ledger_paths.get("trade_lifecycle_ledger", "")
            projection_paths = write_paper_trade_evaluation_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=projection)
            self._send_json(HTTPStatus.OK, {"ok": True, "data": projection, "artifact_paths": {**ledger_paths, **projection_paths}})
            return True

        if path == "/api/aegis/exit-review/latest":
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "exit_review_projection_v1"))
            projection = build_exit_review_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            projection_paths = write_exit_review_projection_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=projection)
            self._send_json(HTTPStatus.OK, {"ok": True, "data": projection, "artifact_paths": projection_paths})
            return True

        if path in {"/api/aegis/attention-queue/latest", "/api/aegis/attention-queue"}:
            current_truth = resolve_current_operator_truth_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=_operator_truth_day(requested_day))
            day = str(current_truth.get("source_day") or _projection_day_for_report(requested_day, "operator_state_snapshot_v1"))
            cockpit = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
            self._send_json(HTTPStatus.OK, {"ok": True, "data": cockpit.get("attention_queue_projection_v1") or cockpit.get("attention_queue_projection") or {}})
            return True

        if path == "/api/aegis/runtime-debug":
            qs = parse_qs(u.query)
            raw_day = (qs.get("day") or qs.get("operational_day") or [requested_day or ""])[0]
            self._send_json(HTTPStatus.OK, _runtime_debug_payload_v1(str(raw_day or "")))
            return True



        if path in {"/api/aegis/engineering-priority-queue", "/api/aegis/engineering-priority-queue/latest"}:
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_engineering_priority_queue_v1"))
            queue_path = engineering_priority_queue_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(queue_path)
            summary = payload.get("summary") if isinstance(payload, dict) and isinstance(payload.get("summary"), dict) else {}
            if not isinstance(payload, dict) or not payload or "latest_estimated_marks" not in summary:
                payload = build_engineering_priority_queue_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, requested_day=requested_day or day)
                queue_path = write_engineering_priority_queue_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=payload)
            self._send_json(HTTPStatus.OK, {"ok": True, "day_utc": day, "artifact_path": str(queue_path), "artifact": payload, "data": payload})
            return True

        if path in {"/api/aegis/surface-readiness", "/api/aegis/surface-readiness/latest"}:
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_surface_readiness_v1"))
            readiness_path = surface_readiness_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(readiness_path)
            if not isinstance(payload, dict) or not payload:
                payload = build_surface_readiness_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                readiness_path = write_surface_readiness_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=payload)
            contract_path = operator_surface_contract_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            contract, _contract_err = _safe_read_json(contract_path)
            if not isinstance(contract, dict) or not contract:
                contract = build_operator_surface_contract_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                contract_path = write_operator_surface_contract_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=contract)
            self._send_json(HTTPStatus.OK, {"ok": True, "day_utc": day, "artifact_path": str(readiness_path), "artifact": payload, "data": payload, "operator_surface_contract": contract, "operator_surface_contract_path": str(contract_path)})
            return True

        if path in {"/api/aegis/operator-surface-contract", "/api/aegis/operator-surface-contract/latest"}:
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_operator_surface_contract_v1"))
            contract_path = operator_surface_contract_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(contract_path)
            if not isinstance(payload, dict) or not payload:
                payload = build_operator_surface_contract_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                contract_path = write_operator_surface_contract_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=payload)
            self._send_json(HTTPStatus.OK, {"ok": True, "day_utc": day, "artifact_path": str(contract_path), "artifact": payload, "data": payload})
            return True

        if path == "/api/aegis/ai-operations/context/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_ai_operations_context_v1"))
            context_path = ai_operations_context_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(context_path)
            if not isinstance(payload, dict) or not payload:
                payload = build_ai_operations_context_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
                context_path = write_ai_operations_context_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=payload)
            self._send_json(HTTPStatus.OK, {"ok": True, "day_utc": day, "artifact_path": str(context_path), "artifact": payload})
            return True

        if path == "/api/aegis/ai-operations/response/latest":
            day = str(requested_day or _projection_day_for_report(requested_day, "aegis_ai_operations_response_v1"))
            response_path = ai_operations_response_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)
            payload, _err = _safe_read_json(response_path)
            if not isinstance(payload, dict) or not payload:
                payload = build_ai_operations_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, question="What happened?")
                response_path = write_ai_operations_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=payload)
            self._send_json(HTTPStatus.OK, {"ok": True, "day_utc": day, "artifact_path": str(response_path), "artifact": payload, "latest_response": payload.get("latest_response") if isinstance(payload, dict) else {}})
            return True

        if path == "/api/aegis/operator-cockpit":
            day = _resolved_operator_api_day_v1(requested_day, "aegis_canonical_operator_state_v1")
            cockpit_payload = _operator_cockpit_payload(GLOBAL_TRUTH_ROOT, day)
            cockpit_payload["requested_day"] = requested_day or day
            cockpit_payload["source_day"] = day
            cockpit_payload["day_utc"] = day
            cockpit_payload["explicit_day_override"] = bool(requested_day)
            _log_operator_cockpit_dump_v1(cockpit_payload)
            self._send_json(HTTPStatus.OK, cockpit_payload)
            return True

        if path == "/api/aegis/operator/today":
            day = _operator_shared_fact_day_v1(requested_day, "aegis_canonical_operator_state_v1")
            payload = _today_build_operator_envelope_v1(
                truth_root=GLOBAL_TRUTH_ROOT,
                requested_day=str(requested_day or day),
                source_day=str(day),
            )
            self._send_json(HTTPStatus.OK, payload)
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


        if path == "/api/aegis/candidate-lineage":
            symbol = str((qs.get("symbol") or ["DOW"])[0] or "DOW").strip().upper()
            requested_lineage_day = str((qs.get("lineage_day") or qs.get("forensics_day") or [""])[0] or "").strip()
            payload = build_candidate_lineage_forensics_v1(
                truth_root=GLOBAL_TRUTH_ROOT,
                symbol=symbol,
                day_utc=requested_lineage_day or None,
            )
            path_written = write_candidate_lineage_forensics_v1(truth_root=GLOBAL_TRUTH_ROOT, payload=payload)
            self._send_json(HTTPStatus.OK, {"ok": True, "path": str(path_written), "data": payload})
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

        if path == "/api/aegis/verified-runtime/action":
            try:
                body = self._read_json_body()
                payload = _run_verified_runtime_action_v1(body)
                status = HTTPStatus.ACCEPTED if payload.get("ok") else HTTPStatus.BAD_REQUEST
                self._send_json(status, payload)
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {
                    "ok": False,
                    "result_status": "VERIFIED_RUNTIME_ACTION_FAILED",
                    "user_message": "Verified runtime action failed.",
                    "error_message": str(exc),
                    "broker_submit_transmit_allowed": False,
                    "autonomous_execution_allowed": False,
                    "trade_advice_allowed": False,
                    "manual_capture_policy_changed": False,
                })
            return True


        if path == "/api/aegis/ai-operations/ask":
            body = self._read_json_body()
            raw_day = str(body.get("day_utc") or body.get("day") or "").strip()
            day = raw_day if _is_day_str(raw_day) else str(date.today().isoformat())
            question = str(body.get("question") or "").strip() or "What happened?"
            payload = build_ai_operations_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, question=question)
            response_path = write_ai_operations_response_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, payload=payload)
            latest = payload.get("latest_response") if isinstance(payload.get("latest_response"), dict) else {}
            self._send_json(HTTPStatus.OK, {"ok": True, "day_utc": day, "artifact_path": str(response_path), "latest_response": latest, "artifact": payload})
            return True

        if path == "/api/aegis/commands":
            from ops.aegis.operator_command_lifecycle_v1 import record_operator_command_v1
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("operational_day") or body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day_for_report(requested_day, "operator_state_snapshot_v1")
                command, inbox_path = record_operator_command_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    body=body,
                    created_by=str(body.get("created_by") or body.get("requested_by") or "operator"),
                )
                self._send_json(HTTPStatus.ACCEPTED, {
                    "ok": True,
                    "command_id": command.get("command_id"),
                    "command_type": command.get("command_type"),
                    "status": command.get("status"),
                    "command": command,
                    "inbox_path": str(inbox_path),
                    "message": f"Command {command.get('command_id')} received.",
                    "next_step": "Run npm run aegis:process-commands or wait for the command processor.",
                    "safety": {
                        "trade_advice_allowed": False,
                        "broker_submit_transmit_allowed": False,
                        "live_trading_allowed": False,
                        "autonomous_execution_allowed": False,
                    },
                })
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "ok": False,
                    "error_code": "COMMAND_RECORDING_FAILED",
                    "message": str(exc),
                    "details": {},
                })
            return True

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

        if path == "/api/research-lab/paper-promotion/action":
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day_for_report(requested_day, "aegis_paper_promotion_approval_queue_v1")
                action = str(body.get("action") or body.get("decision") or "").strip().upper()
                hypothesis_id = str(body.get("hypothesis_id") or "").strip()
                if not hypothesis_id:
                    raise ValueError("hypothesis_id is required")
                decision_map = {"APPROVE_PAPER_TEST": "APPROVED", "APPROVED": "APPROVED", "REJECT": "REJECTED", "REJECTED": "REJECTED", "DEFER": "DEFERRED", "DEFERRED": "DEFERRED"}
                decision = decision_map.get(action, action)
                current_queue = _read_json_dict_or_empty(approval_queue_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day))
                current_item = next((row for row in (current_queue.get("approval_queue") or []) if str(row.get("hypothesis_id") or "") == hypothesis_id), {})
                prior_state = str(current_item.get("approval_state") or "PAPER_PROMOTION_RECOMMENDED")
                event = record_paper_promotion_approval_event_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    hypothesis_id=hypothesis_id,
                    decision=decision,
                    actor=str(body.get("actor") or body.get("requested_by") or "David / operator"),
                    reason=str(body.get("reason") or "Paper promotion queue action from operator UI."),
                    proposal_id=str(current_item.get("proposal_id") or current_item.get("hypothesis_proposal_id") or hypothesis_id),
                    promotion_packet_hash=str(current_item.get("promotion_packet_hash") or body.get("promotion_packet_hash") or ""),
                    prior_state=prior_state,
                )
                refreshed = build_all_hypothesis_proposal_promotion_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day, repo_root=REPO_ROOT)
                refreshed_queue = _read_json_dict_or_empty(approval_queue_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day))
                refreshed_item = next((row for row in (refreshed_queue.get("approval_queue") or []) if str(row.get("hypothesis_id") or "") == hypothesis_id), {})
                self._send_json(HTTPStatus.CREATED, {
                    "ok": True,
                    "day_utc": day,
                    "event": event,
                    "approval_event_path": str(approval_events_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)),
                    "queue_item": refreshed_item,
                    "approval_queue_summary": refreshed_queue.get("summary") or {},
                    "refreshed_summary": refreshed.get("summary") or {},
                    "prior_state": prior_state,
                    "new_state": event.get("new_state"),
                    "approval_meaning": "Approve this hypothesis for paper research tracking.",
                    "forbidden_meanings": ["execute a trade", "create broker order", "submit/transmit order", "allocate real capital", "enable live trading", "create investment advice", "approve trade", "approve broker execution", "approve real capital", "approve investment recommendation"],
                    "safety_statement": "This is paper research only. Not trade advice. No broker execution. No live trading.",
                    "trade_advice_allowed": False,
                    "broker_execution_allowed": False,
                    "live_trading_allowed": False,
                    "real_capital_allowed": False,
                    "automatic_paper_sleeve_creation_allowed": False,
                })
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "message": str(exc), "trade_advice_allowed": False, "broker_execution_allowed": False, "live_trading_allowed": False, "real_capital_allowed": False})
            return True

        if path == "/api/research-lab/operator-action/event":
            try:
                body = self._read_json_body()
                qs = parse_qs(u.query)
                requested_day = str(body.get("day_utc") or body.get("day") or (qs.get("day") or [""])[0] or "")
                day = _projection_day_for_report(requested_day, "aegis_operator_action_queue_v1")
                event = append_operator_action_event_v1(
                    truth_root=GLOBAL_TRUTH_ROOT,
                    day_utc=day,
                    action_id=str(body.get("action_id") or ""),
                    hypothesis_id=str(body.get("hypothesis_id") or ""),
                    action_type=str(body.get("action_type") or ""),
                    button_clicked=str(body.get("button_clicked") or ""),
                    prior_state=str(body.get("prior_state") or ""),
                    new_state=str(body.get("new_state") or body.get("prior_state") or ""),
                    source_state_hash=str(body.get("source_state_hash") or ""),
                    actor=str(body.get("actor") or body.get("requested_by") or "David / operator"),
                )
                self._send_json(HTTPStatus.CREATED, {
                    "ok": True,
                    "day_utc": day,
                    "event": event,
                    "event_log_path": str(operator_action_event_log_path_v1(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day)),
                    "safety_statement": "This is paper research only. Not trade advice. No broker execution. No live trading.",
                    "trade_advice_allowed": False,
                    "broker_execution_allowed": False,
                    "live_trading_allowed": False,
                    "real_capital_allowed": False,
                })
            except Exception as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "message": str(exc), "trade_advice_allowed": False, "broker_execution_allowed": False, "live_trading_allowed": False, "real_capital_allowed": False})
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

        if path == "/api/aegis/performance/advisor-benchmark":
            try:
                body = self._read_json_body()
                result = write_advisor_benchmark_snapshot_v1(truth_root=GLOBAL_TRUTH_ROOT, payload=body)
            except AdvisorBenchmarkValidationError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "message": "Advisor benchmark validation failed.", "field_errors": exc.field_errors})
                return True
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "message": "Advisor benchmark save failed.", "error": str(exc)})
                return True
            self._send_json(HTTPStatus.OK, result)
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
        path = urlparse(self.path).path
        if path == "/api/aegis/change-control/decision":
            try:
                payload = record_change_control_decision_v1(self._read_json_body(), CHANGE_CONTROL_REGISTER_PATH)
                self._send_json(HTTPStatus.CREATED, payload)
            except ChangeControlValidationError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {
                    "ok": False,
                    "errors": [str(exc)],
                    "message": str(exc),
                    "broker_execution_allowed": False,
                    "live_trading_allowed": False,
                    "autonomous_execution_allowed": False,
                    "trade_advice_allowed": False,
                })
            except Exception as exc:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {
                    "ok": False,
                    "errors": ["CHANGE_CONTROL_DECISION_FAILED"],
                    "message": str(exc),
                    "broker_execution_allowed": False,
                    "live_trading_allowed": False,
                    "autonomous_execution_allowed": False,
                    "trade_advice_allowed": False,
                })
            return
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
    sys.stderr.write("AEGIS_RUNTIME_STARTUP " + json.dumps({
        "service_pid": os.getpid(),
        "loaded_module_path": str(Path(__file__).resolve()),
        "git_commit_hash": _git_commit_hash_v1(),
        "truth_root": str(Path(GLOBAL_TRUTH_ROOT).resolve()),
        "sleeve_truth_root": str(Path(SLEEVE_TRUTH_ROOT).resolve()),
        "configured_truth_root": str(Path(TRUTH_ROOT).resolve()),
        "runtime_cwd": os.getcwd(),
        "static_dir": str(OpsHandler.STATIC_DIR),
        "listen": f"http://{ns.host}:{ns.port}",
    }, sort_keys=True, default=str) + "\n")
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
