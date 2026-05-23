from __future__ import annotations

import glob
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from ops.aegis.evidence_event_store_v1 import rebuild_evidence_snapshot_v1
from ops.aegis.evidence_event_store_v1 import read_evidence_events_v1
from ops.aegis.pure_runtime_evaluator_v1 import evaluate_runtime, runtime_policy_bundle_v1
from ops.aegis.producer_event_bridge_v1 import emit_evidence_events_for_artifact_v1, emit_evidence_events_for_status_rows_v1
from ops.aegis.producer_contract_reports_v1 import producer_contract_coverage_report_v1, legacy_scan_only_artifacts_report_v1, source_data_manifest_v1, manual_intent_manifest_v1, render_contract_coverage_text_v1, render_blocker_states_text_v1, critical_bridge_usage_report_v1, repair_semantics_report_v1
from ops.aegis.event_append_transaction_v1 import canonical_vs_quarantine_report_v1
from ops.aegis.decision_ledger_v1 import write_decision_ledger_v1
from ops.aegis.manual_intent_v1 import manual_intent_report_v1

from ops.aegis.intelligence_common_v1 import intelligence_summaries_v1


KERNEL_VERSION = "aegis_runtime_truth_kernel.v1"
DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_OPERATING_MODE = "HUMAN_APPROVED_ADVISORY_RUNTIME"
LIVE_BROKER_TRADING_POLICY = "DISABLED_BY_DESIGN"
AUTONOMOUS_EXECUTION_POLICY = "DISABLED_BY_DESIGN"

CAPABILITIES = [
    "DATA_READY",
    "RESEARCH_READY",
    "EVENT_READY",
    "FEEDBACK_READY",
    "ALERT_GATE_PROVEN",
    "ALERT_DRY_RUN_PROVEN",
    "ALERT_TRANSPORT_PROVEN",
    "BROKER_SIMULATION_PROVEN",
    "BROKER_PAPER_LIFECYCLE_PROVEN",
    "BROKER_LIVE_LIFECYCLE_PROVEN",
    "ADVISORY_READY",
    "TRADE_ADVICE_ALLOWED",
    "MANUAL_TRADE_CAPTURE_ALLOWED",
    "PAPER_TRADE_READY",
    "LIVE_TRADE_READY",
    "AUTONOMOUS_EXECUTION_ALLOWED",
    "BROKER_SUBMIT_TRANSMIT",
    "BROKER_LIFECYCLE_PROVEN",
]


@dataclass(frozen=True)
class ArtifactSpec:
    artifact_id: str
    domain: str
    required: bool
    expected_path: str
    freshness_window_hours: int
    generated_by_command: str
    validates_with_command: str
    downstream_capabilities_blocked: tuple[str, ...]
    claim_implications: tuple[str, ...]
    evidence_fields_required: tuple[str, ...]
    day_scoped: bool = True


ARTIFACT_REGISTRY: tuple[ArtifactSpec, ...] = (
    ArtifactSpec(
        artifact_id="aegis_lite_operating_status",
        domain="report",
        required=True,
        expected_path="reports/aegis_lite_operating_status_v1/{day}/aegis_lite_operating_status.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/build_aegis_operator_status_v1.py --truth_root {truth_root} --day_utc {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("DATA_READY", "TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED"),
        claim_implications=("Aegis Lite operating status is missing or not current.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc"),
    ),
    ArtifactSpec(
        artifact_id="aegis_lite_eod_report",
        domain="report",
        required=True,
        expected_path="reports/aegis_lite_eod_report_v1/{day}/**/aegis_lite_eod_report.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --truth_root {truth_root} --day_utc {day} --manual-only --allow-not-ready-exit-zero",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("DATA_READY", "TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED"),
        claim_implications=("Aegis Lite EOD report is missing or not current.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc"),
    ),
    ArtifactSpec(
        artifact_id="operator_execution_queue",
        domain="candidate",
        required=True,
        expected_path="reports/operator_execution_queue_v1/{day}/**/operator_execution_queue.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --truth_root {truth_root} --day_utc {day} --manual-only --allow-not-ready-exit-zero",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("DATA_READY", "TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED"),
        claim_implications=("Operator execution queue is missing or not current.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc"),
    ),
    ArtifactSpec(
        artifact_id="manual_trade_packet",
        domain="candidate",
        required=True,
        expected_path="reports/manual_trade_packet_v1/{day}/**/manual_trade_packet.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --truth_root {truth_root} --day_utc {day} --manual-only --allow-not-ready-exit-zero",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED", "PAPER_TRADE_READY"),
        claim_implications=("No current manual trade packet is present.",),
        evidence_fields_required=("schema_id", "artifact_id", "date"),
    ),
    ArtifactSpec(
        artifact_id="manual_execution_receipt",
        domain="broker",
        required=False,
        expected_path="reports/manual_execution_receipt_v1/{day}/**/manual_execution_receipt.v1.json",
        freshness_window_hours=168,
        generated_by_command="python3 ops/tools/write_manual_execution_receipt_evidence_v1.py --truth_root {truth_root} --day_utc {day} --receipt_type NONE_DECLARED",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("MANUAL_TRADE_CAPTURE_ALLOWED", "PAPER_TRADE_READY"),
        claim_implications=("Manual execution receipt state has not been evaluated for today.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc", "receipt_type", "result"),
    ),
    ArtifactSpec(
        artifact_id="broker_lifecycle_proof",
        domain="broker",
        required=True,
        expected_path="reports/broker_lifecycle_proof_v1/{day}/**/broker_lifecycle_proof.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/write_broker_lifecycle_proof_v1.py --truth_root {truth_root} --day_utc {day} --lifecycle_mode SIMULATED",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("BROKER_SIMULATION_PROVEN", "BROKER_PAPER_LIFECYCLE_PROVEN", "PAPER_TRADE_READY", "MANUAL_TRADE_CAPTURE_ALLOWED"),
        claim_implications=("Broker lifecycle evidence has not been evaluated, simulated, or proven.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc", "lifecycle_mode", "result"),
    ),
    ArtifactSpec(
        artifact_id="promoted_candidate_evidence",
        domain="candidate",
        required=True,
        expected_path="reports/promoted_sleeve_library_v1/**/promoted_sleeve_library.v1.json",
        freshness_window_hours=168,
        generated_by_command="python3 ops/tools/run_candidate_to_production_promotion_v1.py --truth_root {truth_root} --day_utc {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("CANDIDATE_PROMOTION", "TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED"),
        claim_implications=("No real promoted runtime actionable candidate is proven.",),
        evidence_fields_required=("schema_id", "artifact_id"),
        day_scoped=False,
    ),
    ArtifactSpec(
        artifact_id="selected_intent_promotion",
        domain="candidate",
        required=False,
        expected_path="reports/aegis_selected_intent_promotion_v1/{day}/selected_intent_promotion.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/promote_aegis_selected_intent_v1.py --truth_root {truth_root} --day {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=(),
        claim_implications=(),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc", "status"),
    ),
    ArtifactSpec(
        artifact_id="event_monitoring_status",
        domain="event",
        required=True,
        expected_path="reports/event_monitoring_status_v1/{day}/**/event_monitoring_status.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/run_aegis_event_monitor_v1.py --truth_root {truth_root} --day_utc {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("EVENT_READY", "TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED"),
        claim_implications=("Event monitor is not proven current/enabled.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc"),
    ),
    ArtifactSpec(
        artifact_id="event_rules_registry",
        domain="event",
        required=True,
        expected_path="reports/event_rules_registry_v1/{day}/**/event_rules_registry.v1.json",
        freshness_window_hours=168,
        generated_by_command="python3 ops/tools/run_aegis_event_monitor_v1.py --truth_root {truth_root} --day_utc {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("EVENT_READY",),
        claim_implications=("Event rules registry is missing or not current.",),
        evidence_fields_required=("schema_id", "artifact_id"),
    ),
    ArtifactSpec(
        artifact_id="event_market_snapshot",
        domain="event",
        required=True,
        expected_path="reports/event_market_snapshot_v1/{day}/event_market_snapshot.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/build_event_market_snapshot_v1.py --truth_root {truth_root} --day_utc {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("DATA_READY", "EVENT_READY", "TRADE_ADVICE_ALLOWED"),
        claim_implications=("Market/event context snapshot is missing or stale.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc"),
    ),
    ArtifactSpec(
        artifact_id="event_validity_gate",
        domain="event",
        required=False,
        expected_path="reports/event_validity_gate_v1/{day}/**/event_validity_gate.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/write_event_validity_evidence_v1.py --truth_root {truth_root} --day_utc {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("EVENT_READY", "TRADE_ADVICE_ALLOWED"),
        claim_implications=("Event tactical packet validity is not proven.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc", "evaluated", "validity_status"),
    ),
    ArtifactSpec(
        artifact_id="alert_transport_proof",
        domain="alert",
        required=True,
        expected_path="reports/alert_transport_proof_v1/{day}/**/alert_transport_proof.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/write_alert_transport_proof_v1.py --truth_root {truth_root} --day_utc {day} --transport_mode GATE_ONLY",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("ALERT_GATE_PROVEN", "ALERT_DRY_RUN_PROVEN", "ALERT_TRANSPORT_PROVEN", "TRADE_ADVICE_ALLOWED"),
        claim_implications=("Alert transport evidence has not been evaluated; live email/SMS transport is not proven.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc", "transport_mode", "result"),
    ),
    ArtifactSpec(
        artifact_id="ai_feedback_review",
        domain="feedback",
        required=True,
        expected_path="reports/ai_feedback_review_v1/EOD/{day}/ai_feedback_review.v1.json",
        freshness_window_hours=168,
        generated_by_command="python3 ops/tools/build_ai_eod_feedback_review_v1.py --truth_root {truth_root} --day {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("FEEDBACK_READY",),
        claim_implications=("AI feedback is deterministic fallback only unless ai_used=true is present in evidence.",),
        evidence_fields_required=("schema_id", "artifact_id", "ai_used"),
    ),
    ArtifactSpec(
        artifact_id="research_dataset_binding",
        domain="research",
        required=True,
        expected_path="reports/research_dataset_gap_v1/{day}/research_dataset_gap.v1.json",
        freshness_window_hours=24,
        generated_by_command="python3 ops/tools/audit_research_dataset_bindings_v1.py --truth_root {truth_root} --day_utc {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("RESEARCH_READY", "TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED", "CANDIDATE_PROMOTION"),
        claim_implications=("Research dataset binding is incomplete or not current.",),
        evidence_fields_required=("schema_id", "artifact_id", "day_utc"),
    ),
    ArtifactSpec(
        artifact_id="research_task_queue",
        domain="research",
        required=True,
        expected_path="research_lab/research_task_queue_v1/{day}/index/research_task_queue.v1.json",
        freshness_window_hours=168,
        generated_by_command="python3 ops/tools/build_ai_eod_feedback_review_v1.py --truth_root {truth_root} --day {day}",
        validates_with_command="npm run aegis:audit",
        downstream_capabilities_blocked=("RESEARCH_READY", "CANDIDATE_PROMOTION"),
        claim_implications=("Research task queue is missing or not current.",),
        evidence_fields_required=("schema_id", "artifact_id"),
    ),
)


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def report_dir_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc


def snapshot_dir_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_runtime_state_snapshots_v1" / day_utc


def transitions_dir_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_runtime_state_transitions_v1" / day_utc


def invalidations_dir_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_runtime_invalidations_v1" / day_utc


def runtime_evaluation_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return report_dir_v1(truth_root=truth_root, day_utc=day_utc) / "runtime_evaluation.v1.json"


def runtime_policy_bundle_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return report_dir_v1(truth_root=truth_root, day_utc=day_utc) / "runtime_policy_bundle.v1.json"


def read_canonical_runtime_evaluation_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    return _read_json_object(runtime_evaluation_path_v1(truth_root=truth_root, day_utc=day_utc))


def read_canonical_runtime_policy_bundle_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    return _read_json_object(runtime_policy_bundle_path_v1(truth_root=truth_root, day_utc=day_utc))


def build_ledger_runtime_evaluation_v1(*, truth_root: Path, day_utc: str, generated_at_utc: str, run_id: str = "", parent_run_id: str = "") -> tuple[dict[str, Any], dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve()
    snapshot = rebuild_evidence_snapshot_v1(truth_root=root, day_utc=day_utc)
    if not run_id:
        event_hashes = [str(event.get("event_hash") or "") for event in snapshot.get("events", [])]
        run_id = "runtime-evaluation:" + _stable_hash({"day_utc": day_utc, "generated_at_utc": generated_at_utc, "event_hashes": event_hashes})[:24]
    policy = runtime_policy_bundle_v1(run_id=run_id, parent_run_id=parent_run_id, generated_at_utc=generated_at_utc, git_sha=_git_sha())
    return evaluate_runtime(day_utc, snapshot, policy), policy


def _apply_runtime_evaluation_authority_v1(payload: dict[str, Any], evaluation: dict[str, Any]) -> None:
    caps = evaluation.get("capabilities") if isinstance(evaluation.get("capabilities"), dict) else {}
    payload["runtime_truth_classification"] = str(evaluation.get("runtime_truth_classification") or "PARTIAL_CONTEXT")
    payload["highest_readiness_layer"] = evaluation.get("highest_readiness_layer")
    payload["readiness_classification"] = evaluation.get("highest_readiness_layer")
    payload["runtime_evaluation"] = evaluation
    payload["runtime_evaluation_hash"] = evaluation.get("deterministic_output_hash")
    payload["trade_advice_allowed"] = bool((caps.get("TRADE_ADVICE_ALLOWED") or {}).get("allowed", False))
    payload["manual_trade_capture_allowed"] = bool((caps.get("MANUAL_TRADE_CAPTURE_ALLOWED") or {}).get("allowed", False))
    payload["blocked_capabilities"] = sorted(cap for cap, row in caps.items() if isinstance(row, dict) and not bool(row.get("allowed", False)))
    payload["allowed_capabilities"] = sorted(cap for cap, row in caps.items() if isinstance(row, dict) and bool(row.get("allowed", False)))



def registry_payload_v1() -> list[dict[str, Any]]:
    return [asdict(spec) for spec in ARTIFACT_REGISTRY]


def build_runtime_truth_kernel_v1(
    *,
    truth_root: Path,
    day_utc: str,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or now_utc_v1()
    generated_dt = _parse_time(generated_at) or datetime.now(UTC).replace(microsecond=0)
    artifact_statuses = [_evaluate_artifact(root=root, day_utc=day_utc, generated_dt=generated_dt, spec=spec) for spec in ARTIFACT_REGISTRY]
    statuses_by_id = {row["artifact_id"]: row for row in artifact_statuses}
    runtime_truth = _runtime_truth_classification_from_artifacts(artifact_statuses)
    dependency_graph = _evaluate_capabilities(statuses_by_id)
    all_missing_stale = [row for row in artifact_statuses if row["status"] != "OK"]
    missing_stale = [row for row in all_missing_stale if _artifact_relevant_to_target_mode(row)]
    if missing_stale and runtime_truth == "REAL_RUNTIME":
        runtime_truth = "PARTIAL_CONTEXT"
    dependency_graph = _apply_runtime_truth_to_capabilities(runtime_truth, dependency_graph)
    advisory_state = _advisory_state(statuses_by_id=statuses_by_id, dependency_graph=dependency_graph, runtime_truth_classification=runtime_truth)
    readiness = _readiness_layers(runtime_truth_classification=runtime_truth, dependency_graph=dependency_graph, advisory_state=advisory_state)
    claim_guard = _claim_guard(statuses_by_id=statuses_by_id, dependency_graph=dependency_graph)
    manual_receipt_summary = _manual_execution_receipt_summary(statuses_by_id.get("manual_execution_receipt", {}))
    recovery_items = [_recovery_item(row) for row in missing_stale]
    blocked_capabilities = sorted(
        capability
        for capability, row in dependency_graph.items()
        if not bool(row.get("allowed", False)) and bool(row.get("readiness_relevant", True))
    )
    allowed_capabilities = sorted(capability for capability, row in dependency_graph.items() if bool(row.get("allowed", False)))
    policy_disabled_capabilities = sorted(
        capability
        for capability, row in dependency_graph.items()
        if str(row.get("policy_status") or "") in {"DISABLED_BY_POLICY", "OUT_OF_SCOPE"}
    )
    optional_not_required_capabilities = sorted(
        capability
        for capability, row in dependency_graph.items()
        if str(row.get("policy_status") or "") in {"OPTIONAL_NOT_REQUIRED", "NOT_REQUIRED_FOR_TARGET_MODE"}
    )
    kernel_authority = build_kernel_authority_summary_v1()
    runtime_evaluation = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    runtime_eval_caps = runtime_evaluation.get("capabilities") if isinstance(runtime_evaluation.get("capabilities"), dict) else {}
    if runtime_evaluation:
        runtime_truth = str(runtime_evaluation.get("runtime_truth_classification") or runtime_truth)
    top_trade_allowed = bool((runtime_eval_caps.get("TRADE_ADVICE_ALLOWED") or {}).get("allowed", False))
    top_manual_allowed = bool((runtime_eval_caps.get("MANUAL_TRADE_CAPTURE_ALLOWED") or {}).get("allowed", False))
    return {
        "schema_id": "aegis_runtime_truth_kernel",
        "schema_version": "v1",
        "artifact_id": "aegis_runtime_truth_kernel_v1",
        "kernel_version": KERNEL_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "truth_root": str(root),
        "target_operating_mode": TARGET_OPERATING_MODE,
        "live_broker_trading_policy": LIVE_BROKER_TRADING_POLICY,
        "autonomous_execution_policy": AUTONOMOUS_EXECUTION_POLICY,
        "broker_submit_transmit_policy": "DISABLED_BY_DESIGN",
        "runtime_truth_classification": runtime_truth,
        "highest_readiness_layer": runtime_evaluation.get("highest_readiness_layer") or ("BLOCKED" if missing_stale else "ADVISORY_READY"),
        "layers": readiness["layers"],
        "disabled_or_optional_layers": readiness["disabled_or_optional_layers"],
        "readiness_classification": runtime_evaluation.get("highest_readiness_layer") or ("BLOCKED" if missing_stale else "ADVISORY_READY"),
        "advisory_status": advisory_state["advisory_status"],
        "advisory_state": advisory_state,
        "human_approved_advisory_runtime_ready": readiness["layers"]["HUMAN_APPROVED_ADVISORY_RUNTIME_READY"],
        "operator_action_required": advisory_state["operator_action_required"],
        "operator_action_reason": advisory_state["operator_action_reason"],
        "artifact_registry": registry_payload_v1(),
        "artifact_statuses": artifact_statuses,
        "missing_or_stale_sources": missing_stale,
        "missing_or_stale_source_count": len(missing_stale),
        "all_missing_or_stale_sources": all_missing_stale,
        "allowed_capabilities": allowed_capabilities,
        "blocked_capabilities": blocked_capabilities,
        "policy_disabled_capabilities": policy_disabled_capabilities,
        "optional_not_required_capabilities": optional_not_required_capabilities,
        "manual_capture_status": manual_receipt_summary,
        "out_of_scope_capabilities": {
            capability: {
                "policy_status": row.get("policy_status"),
                "target_mode_requirement": row.get("target_mode_requirement"),
                "reason": row.get("reason"),
            }
            for capability, row in dependency_graph.items()
            if not bool(row.get("readiness_relevant", True))
        },
        "trade_advice_allowed": top_trade_allowed,
        "manual_trade_capture_allowed": top_manual_allowed,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "autonomous_execution_allowed": False,
        "dependency_graph": dependency_graph,
        "claim_guard": claim_guard,
        "do_not_claim": claim_guard["do_not_claim"],
        "claim_violation_count": claim_guard["claim_violation_count"],
        "recovery_plan": recovery_items,
        "kernel_authority": kernel_authority,
        "runtime_evaluation": runtime_evaluation,
        "runtime_evaluation_hash": runtime_evaluation.get("deterministic_output_hash"),
        "permission_authority": "RuntimeEvaluation",
        "intelligence_summaries": intelligence_summaries_v1(root, day_utc),
        "safety_assertions": {
            "no_live_broker_execution_added": True,
            "autonomous_execution_allowed": False,
            "broker_submit_required": False,
            "prefer_false_negatives": True,
            "readiness_is_evidence_backed": True,
        },
    }


def _git_sha() -> str:
    return "UNKNOWN"


def build_kernel_authority_summary_v1() -> dict[str, Any]:
    findings = _authority_drift_findings()
    return {
        "sole_authority_for": [
            "readiness",
            "capability_governance",
            "trade_advice_permission",
            "manual_capture_permission",
            "claim_governance",
            "recovery_planning",
        ],
        "legacy_readiness_logic_detected": bool(findings),
        "truth_drift_risk": "LOW" if not findings else "ELEVATED",
        "authority_audit_scope": "Aegis Runtime Truth control packet, audit handoff, runtime truth API, and runtime truth UI route.",
        "legacy_findings": findings,
    }


def write_runtime_truth_kernel_reports_v1(*, truth_root: Path, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    out_dir = report_dir_v1(truth_root=root, day_utc=str(payload["day_utc"]))
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "runtime_truth_kernel": out_dir / "runtime_truth_kernel.v1.json",
        "missing_stale_sources": out_dir / "missing_stale_sources.v1.json",
        "recovery_plan": out_dir / "recovery_plan.v1.txt",
        "readiness_dependencies": out_dir / "readiness_dependencies.v1.json",
        "runtime_evaluation": out_dir / "runtime_evaluation.v1.json",
        "runtime_policy_bundle": out_dir / "runtime_policy_bundle.v1.json",
        "producer_contract_coverage_json": out_dir / "producer_contract_coverage.v1.json",
        "producer_contract_coverage_txt": out_dir / "producer_contract_coverage.v1.txt",
        "blocker_states_json": out_dir / "blocker_states.v1.json",
        "blocker_states_txt": out_dir / "blocker_states.v1.txt",
        "legacy_scan_only_artifacts": out_dir / "legacy_scan_only_artifacts.v1.json",
        "source_data_manifest": out_dir / "source_data_manifest.v1.json",
        "manual_intent_manifest": out_dir / "manual_intent_manifest.v1.json",
        "repair_semantics_report": out_dir / "repair_semantics_report.v1.json",
        "manual_intent_report": out_dir / "manual_intent_report.v1.json",
        "critical_bridge_usage": out_dir / "critical_bridge_usage.v1.json",
        "canonical_vs_quarantine_events": out_dir / "canonical_vs_quarantine_events.v1.json",
    }
    _write_json(
        paths["missing_stale_sources"],
        {
            "schema_id": "aegis_missing_stale_sources",
            "schema_version": "v1",
            "day_utc": payload["day_utc"],
            "generated_at_utc": payload["generated_at_utc"],
            "truth_root": payload["truth_root"],
            "sources": payload["missing_or_stale_sources"],
            "missing_or_stale_source_count": payload["missing_or_stale_source_count"],
        },
    )
    paths["recovery_plan"].write_text(render_recovery_plan_v1(payload), encoding="utf-8")
    _write_json(
        paths["readiness_dependencies"],
        {
            "schema_id": "aegis_readiness_dependencies",
            "schema_version": "v1",
            "day_utc": payload["day_utc"],
            "generated_at_utc": payload["generated_at_utc"],
            "capabilities": payload["dependency_graph"],
            "layers": payload["layers"],
            "highest_readiness_layer": payload["highest_readiness_layer"],
            "blocked_capabilities": payload["blocked_capabilities"],
        },
    )
    for artifact_path in (paths["missing_stale_sources"], paths["readiness_dependencies"]):
        emit_evidence_events_for_artifact_v1(
            artifact_path=artifact_path,
            payload=_read_json_object(artifact_path),
            producer="ops.aegis.runtime_truth_kernel_v1",
            producer_version=KERNEL_VERSION,
            run_id="runtime-truth-kernel:" + str(payload["day_utc"]),
            git_sha=_git_sha(),
        )
    emit_evidence_events_for_status_rows_v1(
        truth_root=root,
        day_utc=str(payload["day_utc"]),
        artifact_statuses=list(payload.get("artifact_statuses") or []),
        generated_at_utc=str(payload.get("generated_at_utc") or ""),
        producer="ops.aegis.runtime_truth_kernel_v1.artifact_scan",
        producer_version=KERNEL_VERSION,
        git_sha=_git_sha(),
        run_id="runtime-truth-kernel:" + str(payload["day_utc"]),
    )
    runtime_evaluation, runtime_policy = build_ledger_runtime_evaluation_v1(
        truth_root=root,
        day_utc=str(payload["day_utc"]),
        generated_at_utc=str(payload["generated_at_utc"]),
    )
    _apply_runtime_evaluation_authority_v1(payload, runtime_evaluation)
    _write_json(paths["runtime_policy_bundle"], runtime_policy)
    _write_json(paths["runtime_evaluation"], runtime_evaluation)
    write_decision_ledger_v1(truth_root=root, evaluation=runtime_evaluation)
    events = read_evidence_events_v1(truth_root=root, day_utc=str(payload["day_utc"]))
    coverage_report = producer_contract_coverage_report_v1(artifact_statuses=list(payload.get("artifact_statuses") or []), events=events)
    legacy_report = legacy_scan_only_artifacts_report_v1(artifact_statuses=list(payload.get("artifact_statuses") or []), events=events)
    _write_json(paths["producer_contract_coverage_json"], coverage_report)
    paths["producer_contract_coverage_txt"].write_text(render_contract_coverage_text_v1(coverage_report), encoding="utf-8")
    _write_json(paths["blocker_states_json"], {"schema_id": "aegis_blocker_states_report", "schema_version": "v1", "day_utc": payload["day_utc"], "blocker_state": runtime_evaluation.get("blocker_state", []), "root_blockers": runtime_evaluation.get("root_blockers", [])})
    paths["blocker_states_txt"].write_text(render_blocker_states_text_v1({"blocker_state": runtime_evaluation.get("blocker_state", [])}), encoding="utf-8")
    _write_json(paths["legacy_scan_only_artifacts"], legacy_report)
    _write_json(paths["source_data_manifest"], source_data_manifest_v1(events=events))
    _write_json(paths["manual_intent_manifest"], manual_intent_manifest_v1(events=events))
    _write_json(paths["repair_semantics_report"], repair_semantics_report_v1(events=events))
    _write_json(paths["manual_intent_report"], manual_intent_report_v1(truth_root=root, day_utc=str(payload["day_utc"]), runtime_evaluation_hash=str(runtime_evaluation.get("deterministic_output_hash") or ""), generated_at_utc=str(payload.get("generated_at_utc") or "")))
    _write_json(paths["critical_bridge_usage"], critical_bridge_usage_report_v1(events=events))
    _write_json(paths["canonical_vs_quarantine_events"], canonical_vs_quarantine_report_v1(truth_root=root, day_utc=str(payload["day_utc"])))
    payload["producer_contract_coverage"] = coverage_report
    payload["legacy_scan_only_critical_producers"] = coverage_report.get("legacy_scan_only_critical_producers", [])
    _write_json(paths["runtime_truth_kernel"], payload)
    emit_evidence_events_for_artifact_v1(
        artifact_path=paths["runtime_truth_kernel"],
        payload=payload,
        producer="ops.aegis.runtime_truth_kernel_v1",
        producer_version=KERNEL_VERSION,
        run_id=str(runtime_evaluation.get("run_id") or "runtime-truth-kernel:" + str(payload["day_utc"])),
        git_sha=_git_sha(),
    )
    history_paths = write_runtime_state_history_reports_v1(truth_root=root, payload=payload)
    return {**{key: str(path) for key, path in paths.items()}, **history_paths}


def write_runtime_state_history_reports_v1(*, truth_root: Path, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    day_utc = str(payload["day_utc"])
    prior_snapshot = read_runtime_state_snapshot_v1(truth_root=root, day_utc=day_utc)
    snapshot = build_runtime_state_snapshot_v1(payload)
    snapshot_dir = snapshot_dir_v1(truth_root=root, day_utc=day_utc)
    transition_dir = transitions_dir_v1(truth_root=root, day_utc=day_utc)
    invalidation_dir = invalidations_dir_v1(truth_root=root, day_utc=day_utc)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    transition_dir.mkdir(parents=True, exist_ok=True)
    invalidation_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = snapshot_dir / "runtime_state_snapshot.v1.json"
    snapshot_immutable_path = snapshot_dir / f"runtime_state_snapshot.{snapshot['evaluation_id']}.v1.json"
    snapshot_summary_path = snapshot_dir / "runtime_state_snapshot.summary.txt"
    snapshot_summary_immutable_path = snapshot_dir / f"runtime_state_snapshot.{snapshot['evaluation_id']}.summary.txt"
    transition_path = transition_dir / "runtime_state_transitions.v1.json"
    invalidation_path = invalidation_dir / "runtime_invalidations.v1.json"

    transition_payload = build_runtime_state_transition_log_v1(prior_snapshot=prior_snapshot, current_snapshot=snapshot)
    invalidation_payload = build_runtime_invalidation_log_v1(payload)

    _write_json(snapshot_path, snapshot)
    if not snapshot_immutable_path.exists():
        _write_json(snapshot_immutable_path, snapshot)
    summary_text = render_runtime_state_snapshot_summary_v1(snapshot)
    snapshot_summary_path.write_text(summary_text, encoding="utf-8")
    if not snapshot_summary_immutable_path.exists():
        snapshot_summary_immutable_path.write_text(summary_text, encoding="utf-8")
    _write_json(transition_path, transition_payload)
    _write_json(invalidation_path, invalidation_payload)

    return {
        "runtime_state_snapshot": str(snapshot_path),
        "runtime_state_snapshot_immutable": str(snapshot_immutable_path),
        "runtime_state_snapshot_summary": str(snapshot_summary_path),
        "runtime_state_transitions": str(transition_path),
        "runtime_invalidations": str(invalidation_path),
    }


def build_runtime_state_snapshot_v1(payload: dict[str, Any]) -> dict[str, Any]:
    evidence_paths = [
        str(row.get("path") or "")
        for row in payload.get("artifact_statuses", [])
        if isinstance(row, dict) and str(row.get("path") or "")
    ]
    evidence_hashes = {
        str(row.get("artifact_id") or ""): _sha256_file(Path(str(row.get("path") or "")))
        for row in payload.get("artifact_statuses", [])
        if isinstance(row, dict) and str(row.get("path") or "")
    }
    runtime_evaluation = payload.get("runtime_evaluation") if isinstance(payload.get("runtime_evaluation"), dict) else {}
    runtime_caps = runtime_evaluation.get("capabilities") if isinstance(runtime_evaluation.get("capabilities"), dict) else {}
    dependency_rows = {capability: row for capability, row in (payload.get("dependency_graph") or {}).items() if isinstance(row, dict)}
    authority_rows = runtime_caps or dependency_rows
    dependency_statuses = {capability: bool(row.get("allowed", False)) for capability, row in authority_rows.items() if isinstance(row, dict)}
    allowed_capabilities = sorted(cap for cap, allowed in dependency_statuses.items() if allowed)
    blocked_capabilities = sorted(
        cap
        for cap, allowed in dependency_statuses.items()
        if not allowed and bool(authority_rows.get(cap, {}).get("readiness_relevant", True))
    )
    authority_layers = dict(payload.get("layers") if isinstance(payload.get("layers"), dict) else {})
    authority_layers.update(dependency_statuses)
    recovery_plan = payload.get("recovery_plan") if isinstance(payload.get("recovery_plan"), list) else []
    core = {
        "day_utc": payload.get("day_utc"),
        "generated_at": payload.get("generated_at_utc"),
        "target_operating_mode": payload.get("target_operating_mode"),
        "live_broker_trading_policy": payload.get("live_broker_trading_policy"),
        "autonomous_execution_policy": payload.get("autonomous_execution_policy"),
        "runtime_truth_classification": payload.get("runtime_truth_classification"),
        "highest_readiness_layer": payload.get("highest_readiness_layer"),
        "layers": authority_layers,
        "disabled_or_optional_layers": payload.get("disabled_or_optional_layers"),
        "artifact_statuses": payload.get("artifact_statuses"),
        "dependency_statuses": dependency_statuses,
        "blocked_capabilities": blocked_capabilities,
        "allowed_capabilities": allowed_capabilities,
        "do_not_claim": payload.get("do_not_claim"),
        "recovery_plan_summary": [
            {
                "artifact_id": row.get("artifact_id"),
                "what_failed": row.get("what_failed") or row.get("reason"),
                "repair_command": row.get("generated_by_command"),
                "validation_command": row.get("validates_with_command"),
                "blocks": row.get("downstream_capabilities_expected_to_recover") or [],
            }
            for row in recovery_plan
            if isinstance(row, dict)
        ],
        "kernel_authority": payload.get("kernel_authority") or {},
        "input_evidence_paths": evidence_paths,
        "evidence_hashes": evidence_hashes,
    }
    evaluation_id = _stable_hash(core)[:20]
    return {
        "schema_id": "aegis_runtime_state_snapshot",
        "schema_version": "v1",
        "evaluation_id": evaluation_id,
        **core,
    }


def read_runtime_state_snapshot_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any] | None:
    path = snapshot_dir_v1(truth_root=truth_root, day_utc=day_utc) / "runtime_state_snapshot.v1.json"
    payload = _read_json_object(path)
    return payload or None


def read_runtime_state_history_summary_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    snapshot_path = snapshot_dir_v1(truth_root=root, day_utc=day_utc) / "runtime_state_snapshot.v1.json"
    transition_path = transitions_dir_v1(truth_root=root, day_utc=day_utc) / "runtime_state_transitions.v1.json"
    invalidation_path = invalidations_dir_v1(truth_root=root, day_utc=day_utc) / "runtime_invalidations.v1.json"
    snapshot = _read_json_object(snapshot_path)
    transition = _read_json_object(transition_path)
    invalidation = _read_json_object(invalidation_path)
    transition_summary = transition.get("summary") if isinstance(transition.get("summary"), dict) else {}
    return {
        "latest_snapshot_id": str(snapshot.get("evaluation_id") or ""),
        "snapshot_path": str(snapshot_path),
        "transition_path": str(transition_path),
        "invalidation_path": str(invalidation_path),
        "last_transition_summary": transition_summary,
        "transition_count": int(transition.get("transition_count") or 0),
        "invalidation_count": int(invalidation.get("invalidation_count") or 0),
        "capabilities_gained_since_prior_snapshot": transition_summary.get("capabilities_gained") or [],
        "capabilities_lost_since_prior_snapshot": transition_summary.get("capabilities_lost") or [],
        "replay_command_example": f"npm run aegis:replay-state -- --truth_root {root} --day {day_utc}",
        "diff_command_example": f"npm run aegis:state-diff -- --truth_root {root} --from-day {day_utc} --to-day {day_utc}",
    }


def build_runtime_state_transition_log_v1(*, prior_snapshot: dict[str, Any] | None, current_snapshot: dict[str, Any]) -> dict[str, Any]:
    changed_at = str(current_snapshot.get("generated_at") or now_utc_v1())
    transitions: list[dict[str, Any]] = []
    if prior_snapshot:
        transitions.extend(_dict_value_transitions("readiness_layer", prior_snapshot.get("layers") or {}, current_snapshot.get("layers") or {}, changed_at))
        transitions.extend(_dict_value_transitions("capability_allowed", prior_snapshot.get("dependency_statuses") or {}, current_snapshot.get("dependency_statuses") or {}, changed_at))
        transitions.extend(_artifact_status_transitions(prior_snapshot, current_snapshot, changed_at))
        transitions.extend(_set_transitions("do_not_claim", prior_snapshot.get("do_not_claim") or [], current_snapshot.get("do_not_claim") or [], changed_at))
        transitions.extend(_set_transitions("recovery_action", _recovery_keys(prior_snapshot), _recovery_keys(current_snapshot), changed_at))
        transitions.extend(
            _dict_value_transitions(
                "truth_drift_risk",
                {"truth_drift_risk": (prior_snapshot.get("kernel_authority") or {}).get("truth_drift_risk")},
                {"truth_drift_risk": (current_snapshot.get("kernel_authority") or {}).get("truth_drift_risk")},
                changed_at,
            )
        )
    summary = summarize_runtime_state_diff_v1(prior_snapshot or {}, current_snapshot)
    return {
        "schema_id": "aegis_runtime_state_transitions",
        "schema_version": "v1",
        "day_utc": current_snapshot.get("day_utc"),
        "changed_at": changed_at,
        "from_evaluation_id": prior_snapshot.get("evaluation_id") if prior_snapshot else "",
        "to_evaluation_id": current_snapshot.get("evaluation_id"),
        "transition_count": len(transitions),
        "summary": summary,
        "transitions": transitions,
    }


def build_runtime_invalidation_log_v1(payload: dict[str, Any]) -> dict[str, Any]:
    detected_at = str(payload.get("generated_at_utc") or now_utc_v1())
    events = []
    for row in payload.get("missing_or_stale_sources", []) if isinstance(payload.get("missing_or_stale_sources"), list) else []:
        if not isinstance(row, dict):
            continue
        event_core = {
            "artifact_id": row.get("artifact_id"),
            "invalidation_type": row.get("status"),
            "detected_at": detected_at,
            "blocked_capabilities": row.get("downstream_capabilities_blocked") or [],
            "claim_implications": row.get("claim_implications") or [],
            "recovery_command": row.get("generated_by_command") or "",
            "validation_command": row.get("validates_with_command") or "",
        }
        events.append({"event_id": _stable_hash(event_core)[:20], **event_core})
    return {
        "schema_id": "aegis_runtime_invalidations",
        "schema_version": "v1",
        "day_utc": payload.get("day_utc"),
        "detected_at": detected_at,
        "invalidation_count": len(events),
        "events": events,
    }


def render_runtime_state_snapshot_summary_v1(snapshot: dict[str, Any]) -> str:
    return "\n".join(
        [
            "AEGIS RUNTIME STATE SNAPSHOT v1",
            f"evaluation_id: {snapshot.get('evaluation_id')}",
            f"day_utc: {snapshot.get('day_utc')}",
            f"generated_at: {snapshot.get('generated_at')}",
            f"runtime_truth_classification: {snapshot.get('runtime_truth_classification')}",
            f"highest_readiness_layer: {snapshot.get('highest_readiness_layer')}",
            f"allowed_capabilities: {', '.join(snapshot.get('allowed_capabilities') or []) or 'NONE'}",
            f"blocked_capabilities: {', '.join(snapshot.get('blocked_capabilities') or []) or 'NONE'}",
            f"do_not_claim_count: {len(snapshot.get('do_not_claim') or [])}",
            f"recovery_action_count: {len(snapshot.get('recovery_plan_summary') or [])}",
            f"truth_drift_risk: {(snapshot.get('kernel_authority') or {}).get('truth_drift_risk') or 'UNKNOWN'}",
            "",
        ]
    )


def render_recovery_plan_v1(payload: dict[str, Any]) -> str:
    items = payload.get("recovery_plan") if isinstance(payload.get("recovery_plan"), list) else []
    lines = [
        "AEGIS RUNTIME TRUTH RECOVERY PLAN v1",
        f"day_utc: {payload.get('day_utc')}",
        f"runtime_truth_classification: {payload.get('runtime_truth_classification')}",
        f"highest_readiness_layer: {payload.get('highest_readiness_layer')}",
        f"missing_or_stale_source_count: {payload.get('missing_or_stale_source_count')}",
        "",
    ]
    if not items:
        lines.append("No missing, stale, or invalid runtime truth artifacts were detected.")
    for index, item in enumerate(items, start=1):
        lines.extend(
            [
                f"{index}. {item.get('artifact_id')}",
                f"   What failed: {item.get('what_failed') or item.get('reason')}",
                f"   Why it matters: {item.get('why_it_matters')}",
                f"   What it blocks: {', '.join(item.get('downstream_capabilities_expected_to_recover') or []) or 'none'}",
                f"   Run: {item.get('generated_by_command')}",
                f"   Expected output: {item.get('expected_path')}",
                f"   Validate: {item.get('validates_with_command') or 'npm run aegis:audit'}",
                f"   Evidence proves recovery: {', '.join(item.get('evidence_fields_required') or []) or 'artifact status OK'}",
                f"   Claims forbidden until recovery: {', '.join(item.get('claims_forbidden_until_recovery') or []) or 'none'}",
                "",
            ]
        )
    forbidden = payload.get("do_not_claim") if isinstance(payload.get("do_not_claim"), list) else []
    lines.append("Claims remain forbidden:")
    for item in forbidden or ["NONE"]:
        lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def _evaluate_artifact(*, root: Path, day_utc: str, generated_dt: datetime, spec: ArtifactSpec) -> dict[str, Any]:
    expected = _format_command(spec.expected_path, truth_root=root, day_utc=day_utc)
    candidates = _find_candidates(root=root, pattern=expected, day_utc=day_utc, day_scoped=spec.day_scoped)
    base = {
        "artifact_id": spec.artifact_id,
        "domain": spec.domain,
        "required": spec.required,
        "expected_path": expected,
        "freshness_window_hours": spec.freshness_window_hours,
        "generated_by_command": _format_command(spec.generated_by_command, truth_root=root, day_utc=day_utc),
        "validates_with_command": _format_command(spec.validates_with_command, truth_root=root, day_utc=day_utc),
        "downstream_capabilities_blocked": list(spec.downstream_capabilities_blocked),
        "claim_implications": list(spec.claim_implications),
        "evidence_fields_required": list(spec.evidence_fields_required),
    }
    if not candidates:
        return {**base, "status": "MISSING", "reason": "Expected artifact was not found.", "path": "", "last_modified_at": ""}
    path = candidates[-1]
    last_modified = _mtime_iso(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("JSON_NOT_OBJECT")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            **base,
            "status": "INVALID",
            "reason": f"Artifact is not readable JSON object: {type(exc).__name__}",
            "path": str(path),
            "last_modified_at": last_modified,
        }
    missing_fields = [field for field in spec.evidence_fields_required if not _has_field(payload, field)]
    if missing_fields:
        return {
            **base,
            "status": "INVALID",
            "reason": "Missing required evidence fields: " + ", ".join(missing_fields),
            "path": str(path),
            "last_modified_at": last_modified,
        }
    invalid_reason = _invalid_payload_reason(payload)
    if not invalid_reason and spec.artifact_id == "alert_transport_proof" and str(payload.get("schema_id") or "") != "alert_transport_proof" and not _alert_transport_payload_proven(payload):
        invalid_reason = "Alert transport proof is present but no LIVE_EMAIL_SENT, LIVE_SMS_SENT, or LIVE_EMAIL_PROVEN evidence exists."
    if not invalid_reason and spec.artifact_id == "research_dataset_binding" and _research_dataset_binding_has_gaps(payload):
        invalid_reason = "Research dataset binding reports one or more missing datasets."
    if invalid_reason:
        return {**base, "status": "INVALID", "reason": invalid_reason, "path": str(path), "last_modified_at": last_modified}
    evidence_time = _artifact_timestamp(payload)
    if not evidence_time:
        return {
            **base,
            "status": "INVALID",
            "reason": "No explicit artifact timestamp is available; filesystem modification time is not trusted.",
            "path": str(path),
            "last_modified_at": last_modified,
        }
    parsed = _parse_time(evidence_time)
    if parsed is None:
        return {
            **base,
            "status": "INVALID",
            "reason": "No parseable artifact timestamp or filesystem modification time is available.",
            "path": str(path),
            "last_modified_at": last_modified,
        }
    age = generated_dt - parsed
    if age > timedelta(hours=spec.freshness_window_hours):
        return {
            **base,
            "status": "STALE",
            "reason": f"Artifact age {round(age.total_seconds() / 3600, 2)}h exceeds {spec.freshness_window_hours}h freshness window.",
            "path": str(path),
            "last_modified_at": last_modified,
            "artifact_timestamp": evidence_time,
        }
    return {
        **base,
        "status": "OK",
        "reason": "Artifact exists, is parseable, has required evidence fields, and is within freshness window.",
        "path": str(path),
        "last_modified_at": last_modified,
        "artifact_timestamp": evidence_time,
        "payload_runtime_truth_classification": str(payload.get("runtime_truth_classification") or ""),
    }


def _runtime_truth_classification_from_artifacts(artifact_statuses: list[dict[str, Any]]) -> str:
    classes = {str(row.get("payload_runtime_truth_classification") or "").upper() for row in artifact_statuses}
    if "DEMO_ONLY" in classes:
        return "DEMO_ONLY"
    if "DRY_RUN_ONLY" in classes:
        return "DRY_RUN_ONLY"
    return "REAL_RUNTIME"


def _artifact_relevant_to_target_mode(row: dict[str, Any]) -> bool:
    return str(row.get("artifact_id") or "") not in {"broker_lifecycle_proof", "selected_intent_promotion", "promoted_candidate_evidence"}


def _apply_runtime_truth_to_capabilities(runtime_truth: str, graph: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if runtime_truth == "REAL_RUNTIME":
        return graph
    for capability in ("TRADE_ADVICE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED"):
        row = graph.get(capability)
        if not isinstance(row, dict):
            continue
        row["allowed"] = False
        row.setdefault("missing_or_blocking_capabilities", [])
        row["reason"] = f"Runtime truth {runtime_truth} does not allow this capability."
    return graph


def _evaluate_capabilities(statuses_by_id: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    def ok(*artifact_ids: str) -> bool:
        return all(statuses_by_id.get(artifact_id, {}).get("status") == "OK" for artifact_id in artifact_ids)

    graph = {
        "DATA_READY": _cap(ok("aegis_lite_operating_status", "aegis_lite_eod_report", "operator_execution_queue", "event_market_snapshot"), ["aegis_lite_operating_status", "aegis_lite_eod_report", "operator_execution_queue", "event_market_snapshot"], statuses_by_id),
        "RESEARCH_READY": _cap(ok("research_dataset_binding", "research_task_queue"), ["research_dataset_binding", "research_task_queue"], statuses_by_id),
        "EVENT_READY": _cap(ok("event_monitoring_status", "event_rules_registry", "event_market_snapshot", "event_validity_gate"), ["event_monitoring_status", "event_rules_registry", "event_market_snapshot", "event_validity_gate"], statuses_by_id),
        "FEEDBACK_READY": _cap(ok("ai_feedback_review"), ["ai_feedback_review"], statuses_by_id),
        "ALERT_GATE_PROVEN": _cap(_alert_gate_proven(statuses_by_id.get("alert_transport_proof", {})), ["alert_transport_proof"], statuses_by_id),
        "ALERT_DRY_RUN_PROVEN": _cap(_alert_dry_run_proven(statuses_by_id.get("alert_transport_proof", {})), ["alert_transport_proof"], statuses_by_id),
        "ALERT_TRANSPORT_PROVEN": _cap(_alert_transport_proven(statuses_by_id.get("alert_transport_proof", {})), ["alert_transport_proof"], statuses_by_id),
        "BROKER_SIMULATION_PROVEN": _cap(_broker_simulation_proven(statuses_by_id.get("broker_lifecycle_proof", {})), ["broker_lifecycle_proof"], statuses_by_id),
        "BROKER_PAPER_LIFECYCLE_PROVEN": _cap(_broker_paper_lifecycle_proven(statuses_by_id.get("broker_lifecycle_proof", {})), ["broker_lifecycle_proof"], statuses_by_id),
        "BROKER_LIVE_LIFECYCLE_PROVEN": {
            "allowed": False,
            "depends_on_artifacts": ["broker_lifecycle_proof"],
            "missing_or_blocking_artifacts": [],
            "policy_status": "DISABLED_BY_POLICY",
            "target_mode_requirement": "NOT_REQUIRED_FOR_TARGET_MODE",
            "readiness_relevant": False,
            "reason": "Live broker lifecycle proof is out of scope; live broker trading is disabled by design.",
        },
    }
    graph["ALERT_DRY_RUN_PROVEN"]["policy_status"] = "OPTIONAL_NOT_REQUIRED"
    graph["ALERT_DRY_RUN_PROVEN"]["target_mode_requirement"] = "ALERT_GATE_OR_DRY_RUN"
    graph["ALERT_DRY_RUN_PROVEN"]["readiness_relevant"] = False
    graph["ALERT_TRANSPORT_PROVEN"]["policy_status"] = "OPTIONAL_NOT_REQUIRED"
    graph["ALERT_TRANSPORT_PROVEN"]["target_mode_requirement"] = "NOT_REQUIRED_FOR_TARGET_MODE"
    graph["ALERT_TRANSPORT_PROVEN"]["readiness_relevant"] = False
    if not graph["ALERT_TRANSPORT_PROVEN"]["allowed"]:
        graph["ALERT_TRANSPORT_PROVEN"]["reason"] = "Live alert transport is optional and not required for human-approved advisory runtime."
    graph["BROKER_SIMULATION_PROVEN"]["policy_status"] = "OPTIONAL_NOT_REQUIRED"
    graph["BROKER_SIMULATION_PROVEN"]["target_mode_requirement"] = "NOT_REQUIRED_FOR_TARGET_MODE"
    graph["BROKER_SIMULATION_PROVEN"]["readiness_relevant"] = False
    graph["BROKER_PAPER_LIFECYCLE_PROVEN"]["policy_status"] = "OPTIONAL_NOT_REQUIRED"
    graph["BROKER_PAPER_LIFECYCLE_PROVEN"]["target_mode_requirement"] = "NOT_REQUIRED_FOR_TARGET_MODE"
    graph["BROKER_PAPER_LIFECYCLE_PROVEN"]["readiness_relevant"] = False
    graph["BROKER_LIFECYCLE_PROVEN"] = {
        **graph["BROKER_PAPER_LIFECYCLE_PROVEN"],
        "capability_id": "BROKER_LIFECYCLE_PROVEN",
        "reason": "Backward-compatible alias for optional paper lifecycle proof; not required for target advisory mode.",
    }
    graph["ADVISORY_READY"] = {
        "capability_id": "ADVISORY_READY",
        "allowed": True,
        "depends_on_artifacts": [],
        "missing_or_blocking_artifacts": [],
        "reason": "Read-only advisory surface remains available; execution claims stay blocked by downstream capability gates.",
    }
    graph["TRADE_ADVICE_ALLOWED"] = _derived_cap(
        "TRADE_ADVICE_ALLOWED",
        graph,
        ["DATA_READY", "RESEARCH_READY", "EVENT_READY", "FEEDBACK_READY"],
        ["manual_trade_packet", "promoted_candidate_evidence"],
        statuses_by_id,
    )
    graph["TRADE_ADVICE_ALLOWED"]["advisory_mode_semantics"] = {
        "target_operating_mode": TARGET_OPERATING_MODE,
        "requires_live_alert_transport": False,
        "requires_broker_lifecycle_proof": False,
        "requires_autonomous_execution": False,
        "broker_execution_disabled": True,
        "disclaimer": "Trade advice is advisory only; a human must review and execute outside Aegis.",
    }
    if graph["TRADE_ADVICE_ALLOWED"]["allowed"] and _event_validity_no_event_packet(statuses_by_id.get("event_validity_gate", {})):
        graph["TRADE_ADVICE_ALLOWED"]["allowed"] = False
        graph["TRADE_ADVICE_ALLOWED"]["missing_or_blocking_artifacts"] = [
            *graph["TRADE_ADVICE_ALLOWED"].get("missing_or_blocking_artifacts", []),
            "event_validity_gate:NO_EVENT_PACKET",
        ]
        graph["TRADE_ADVICE_ALLOWED"]["reason"] = "Event validity was evaluated and no actionable event packet exists."
    if graph["TRADE_ADVICE_ALLOWED"]["allowed"] and _advisory_candidate_count(statuses_by_id) <= 0:
        graph["TRADE_ADVICE_ALLOWED"]["allowed"] = False
        graph["TRADE_ADVICE_ALLOWED"]["missing_or_blocking_artifacts"] = [
            *graph["TRADE_ADVICE_ALLOWED"].get("missing_or_blocking_artifacts", []),
            "manual_trade_packet:NO_ACTIONABLE_CANDIDATES",
        ]
        graph["TRADE_ADVICE_ALLOWED"]["reason"] = "No valid actionable advisory candidates are present."
    if graph["TRADE_ADVICE_ALLOWED"]["allowed"] and _advisory_candidates_have_risk_blockers(statuses_by_id):
        graph["TRADE_ADVICE_ALLOWED"]["allowed"] = False
        graph["TRADE_ADVICE_ALLOWED"]["missing_or_blocking_artifacts"] = [
            *graph["TRADE_ADVICE_ALLOWED"].get("missing_or_blocking_artifacts", []),
            "manual_trade_packet:RISK_BLOCKERS_PRESENT",
        ]
        graph["TRADE_ADVICE_ALLOWED"]["reason"] = "Advisory candidates are present but risk blockers were emitted."
    graph["MANUAL_TRADE_CAPTURE_ALLOWED"] = _derived_cap(
        "MANUAL_TRADE_CAPTURE_ALLOWED",
        graph,
        ["DATA_READY", "RESEARCH_READY", "EVENT_READY", "FEEDBACK_READY"],
        ["manual_execution_receipt"],
        statuses_by_id,
    )
    if graph["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"] and not _manual_capture_infrastructure_available():
        graph["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"] = False
        graph["MANUAL_TRADE_CAPTURE_ALLOWED"]["missing_or_blocking_artifacts"] = [
            *graph["MANUAL_TRADE_CAPTURE_ALLOWED"].get("missing_or_blocking_artifacts", []),
            "manual_capture_cli",
        ]
        graph["MANUAL_TRADE_CAPTURE_ALLOWED"]["reason"] = "Manual capture CLI is missing; receipt journaling is unavailable."
    graph["MANUAL_TRADE_CAPTURE_ALLOWED"]["manual_capture_semantics"] = {
        "capability_type": "JOURNALING_AUDIT_ONLY",
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
        "broker_execution_allowed": False,
        "requires_trade_advice_allowed": False,
        "requires_broker_lifecycle_proof": False,
        "requires_live_alert_transport": False,
        "disclaimer": "Manual capture records operator-entered fills only; Aegis does not submit, transmit, or place orders.",
    }
    graph["PAPER_TRADE_READY"] = _derived_cap(
        "PAPER_TRADE_READY",
        graph,
        ["TRADE_ADVICE_ALLOWED", "BROKER_PAPER_LIFECYCLE_PROVEN"],
        [],
        statuses_by_id,
    )
    graph["PAPER_TRADE_READY"]["policy_status"] = "OPTIONAL_NOT_REQUIRED"
    graph["PAPER_TRADE_READY"]["target_mode_requirement"] = "NOT_REQUIRED_FOR_TARGET_MODE"
    graph["PAPER_TRADE_READY"]["readiness_relevant"] = False
    if not graph["PAPER_TRADE_READY"]["allowed"]:
        graph["PAPER_TRADE_READY"]["reason"] = "Paper broker readiness is optional and not required for human-approved advisory runtime."
    graph["LIVE_TRADE_READY"] = {
        "capability_id": "LIVE_TRADE_READY",
        "allowed": False,
        "depends_on_capabilities": ["PAPER_TRADE_READY"],
        "depends_on_artifacts": [],
        "missing_or_blocking_artifacts": [],
        "policy_status": "DISABLED_BY_POLICY",
        "target_mode_requirement": "NOT_REQUIRED_FOR_TARGET_MODE",
        "readiness_relevant": False,
        "reason": "Live broker trading is disabled by design and is not an intended Aegis operating mode.",
    }
    graph["AUTONOMOUS_EXECUTION_ALLOWED"] = _policy_disabled_capability(
        "AUTONOMOUS_EXECUTION_ALLOWED",
        "Autonomous execution is disabled by design; Aegis requires human approval and external execution.",
    )
    graph["BROKER_SUBMIT_TRANSMIT"] = _policy_disabled_capability(
        "BROKER_SUBMIT_TRANSMIT",
        "Broker submit/transmit is out of scope; Aegis records manual receipts only.",
    )
    return {capability: graph[capability] for capability in CAPABILITIES}


def _cap(allowed: bool, artifact_ids: list[str], statuses_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    blockers = [artifact_id for artifact_id in artifact_ids if statuses_by_id.get(artifact_id, {}).get("status") != "OK"]
    return {
        "allowed": bool(allowed),
        "depends_on_artifacts": artifact_ids,
        "missing_or_blocking_artifacts": blockers,
        "reason": "All artifact dependencies are OK." if allowed else "One or more artifact dependencies are missing, stale, or invalid.",
    }


def _derived_cap(
    capability_id: str,
    graph: dict[str, dict[str, Any]],
    capability_dependencies: list[str],
    artifact_dependencies: list[str],
    statuses_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    missing_caps = [cap for cap in capability_dependencies if not bool(graph.get(cap, {}).get("allowed"))]
    missing_artifacts = [artifact_id for artifact_id in artifact_dependencies if statuses_by_id.get(artifact_id, {}).get("status") != "OK"]
    allowed = not missing_caps and not missing_artifacts
    return {
        "capability_id": capability_id,
        "allowed": allowed,
        "depends_on_capabilities": capability_dependencies,
        "depends_on_artifacts": artifact_dependencies,
        "missing_or_blocking_capabilities": missing_caps,
        "missing_or_blocking_artifacts": missing_artifacts,
        "reason": "All capability and artifact dependencies are OK." if allowed else "One or more capability or artifact dependencies are blocked.",
    }


def _policy_disabled_capability(capability_id: str, reason: str) -> dict[str, Any]:
    return {
        "capability_id": capability_id,
        "allowed": False,
        "depends_on_capabilities": [],
        "depends_on_artifacts": [],
        "missing_or_blocking_capabilities": [],
        "missing_or_blocking_artifacts": [],
        "policy_status": "DISABLED_BY_POLICY",
        "target_mode_requirement": "NOT_REQUIRED_FOR_TARGET_MODE",
        "readiness_relevant": False,
        "reason": reason,
    }


def _manual_capture_infrastructure_available() -> bool:
    return (REPO_ROOT / "ops/tools/capture_manual_trade_receipt_v1.py").exists()


def _advisory_state(
    *,
    statuses_by_id: dict[str, dict[str, Any]],
    dependency_graph: dict[str, dict[str, Any]],
    runtime_truth_classification: str,
) -> dict[str, Any]:
    stale_or_missing = [
        artifact_id
        for artifact_id, row in statuses_by_id.items()
        if row.get("status") != "OK" and _artifact_relevant_to_target_mode(row)
    ]
    if runtime_truth_classification != "REAL_RUNTIME" or stale_or_missing:
        return {
            "advisory_status": "ADVISORY_BLOCKED_BY_STALE_EVIDENCE",
            "human_approved_advisory_runtime_ready": False,
            "operator_action_required": True,
            "operator_action_reason": "Refresh missing, stale, or invalid target-mode evidence before relying on advisory runtime.",
            "required_for_trade_advice": _trade_advice_requirements(),
            "candidate_count": 0,
        }
    if not all(bool(dependency_graph.get(capability, {}).get("allowed", False)) for capability in ("DATA_READY", "RESEARCH_READY", "EVENT_READY", "FEEDBACK_READY")):
        return {
            "advisory_status": "ADVISORY_NOT_EVALUATED",
            "human_approved_advisory_runtime_ready": False,
            "operator_action_required": True,
            "operator_action_reason": "Core data, research, event, or feedback readiness has not passed.",
            "required_for_trade_advice": _trade_advice_requirements(),
            "candidate_count": 0,
        }
    if _event_validity_no_event_packet(statuses_by_id.get("event_validity_gate", {})):
        return {
            "advisory_status": "NO_ACTIONABLE_CANDIDATES",
            "human_approved_advisory_runtime_ready": True,
            "operator_action_required": False,
            "operator_action_reason": "Event validity was evaluated and no actionable event packet exists today.",
            "required_for_trade_advice": _trade_advice_requirements(),
            "candidate_count": 0,
        }
    candidate_count = _advisory_candidate_count(statuses_by_id)
    if candidate_count > 0 and _advisory_candidates_have_risk_blockers(statuses_by_id):
        return {
            "advisory_status": "ADVISORY_BLOCKED_BY_RISK",
            "human_approved_advisory_runtime_ready": False,
            "operator_action_required": True,
            "operator_action_reason": "Advisory candidates exist, but one or more risk blockers are present.",
            "required_for_trade_advice": _trade_advice_requirements(),
            "candidate_count": candidate_count,
        }
    if candidate_count > 0:
        return {
            "advisory_status": "ADVISORY_CANDIDATES_AVAILABLE",
            "human_approved_advisory_runtime_ready": True,
            "operator_action_required": True,
            "operator_action_reason": "Review advisory candidates manually; Aegis will not submit or transmit broker orders.",
            "required_for_trade_advice": _trade_advice_requirements(),
            "candidate_count": candidate_count,
        }
    return {
        "advisory_status": "NO_ACTIONABLE_CANDIDATES",
        "human_approved_advisory_runtime_ready": True,
        "operator_action_required": False,
        "operator_action_reason": "Advisory evidence is current and no actionable candidates were emitted.",
        "required_for_trade_advice": _trade_advice_requirements(),
        "candidate_count": 0,
    }


def _trade_advice_requirements() -> list[str]:
    return [
        "runtime_truth_classification=REAL_RUNTIME",
        "DATA_READY, RESEARCH_READY, EVENT_READY, and FEEDBACK_READY",
        "valid candidate/advisory evidence",
        "risk guard evidence passes",
        "event/candidate context when required",
        "human approval and do-not-execute disclaimer present",
        "broker execution disabled by design",
        "manual capture available for externally executed fills",
    ]


def _advisory_candidate_count(statuses_by_id: dict[str, dict[str, Any]]) -> int:
    return len(_advisory_candidate_rows(statuses_by_id))


def _advisory_candidate_rows(statuses_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    packet_row = statuses_by_id.get("manual_trade_packet", {})
    if packet_row.get("status") != "OK":
        return []
    payload = _read_json_object(Path(str(packet_row.get("path") or "")))
    for key in ("current_actionable_items", "actionable_items", "candidates", "trade_candidates", "manual_trade_candidates", "trades"):
        value = payload.get(key)
        if isinstance(value, list) and value:
            return [row for row in value if isinstance(row, dict)]
    queue = payload.get("execution_queue")
    if isinstance(queue, list) and queue:
        return [row for row in queue if isinstance(row, dict)]
    return []


def _advisory_candidates_have_risk_blockers(statuses_by_id: dict[str, dict[str, Any]]) -> bool:
    for row in _advisory_candidate_rows(statuses_by_id):
        blockers = row.get("do_not_trade_blockers")
        if isinstance(blockers, list) and blockers:
            return True
        risk_status = str(row.get("risk_status") or row.get("risk_guard_status") or "").strip().upper()
        if risk_status in {"FAIL", "FAILED", "BLOCKED", "INVALID"}:
            return True
    return False


def _readiness_layers(*, runtime_truth_classification: str, dependency_graph: dict[str, dict[str, Any]], advisory_state: dict[str, Any]) -> dict[str, Any]:
    layers = {
        "REAL_RUNTIME_READY": runtime_truth_classification == "REAL_RUNTIME",
        "DATA_READY": bool(dependency_graph["DATA_READY"]["allowed"]),
        "RESEARCH_READY": bool(dependency_graph["RESEARCH_READY"]["allowed"]),
        "EVENT_READY": bool(dependency_graph["EVENT_READY"]["allowed"]),
        "FEEDBACK_READY": bool(dependency_graph["FEEDBACK_READY"]["allowed"]),
        "ALERT_GATE_PROVEN": bool(dependency_graph["ALERT_GATE_PROVEN"]["allowed"]),
        "ALERT_DRY_RUN_PROVEN": bool(dependency_graph["ALERT_DRY_RUN_PROVEN"]["allowed"]),
        "ALERT_TRANSPORT_PROVEN": bool(dependency_graph["ALERT_TRANSPORT_PROVEN"]["allowed"]),
        "BROKER_SIMULATION_PROVEN": bool(dependency_graph["BROKER_SIMULATION_PROVEN"]["allowed"]),
        "BROKER_PAPER_LIFECYCLE_PROVEN": bool(dependency_graph["BROKER_PAPER_LIFECYCLE_PROVEN"]["allowed"]),
        "BROKER_LIVE_LIFECYCLE_PROVEN": bool(dependency_graph["BROKER_LIVE_LIFECYCLE_PROVEN"]["allowed"]),
        "ADVISORY_READY": bool(dependency_graph["ADVISORY_READY"]["allowed"]),
        "TRADE_ADVICE_ALLOWED": bool(dependency_graph["TRADE_ADVICE_ALLOWED"]["allowed"]),
        "MANUAL_CAPTURE_CAPABILITY_READY": bool(dependency_graph["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"]),
        "MANUAL_TRADE_CAPTURE_ALLOWED": bool(dependency_graph["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"]),
        "HUMAN_APPROVED_ADVISORY_RUNTIME_READY": bool(advisory_state.get("human_approved_advisory_runtime_ready", False))
        and runtime_truth_classification == "REAL_RUNTIME"
        and bool(dependency_graph["ADVISORY_READY"]["allowed"])
        and bool(dependency_graph["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"]),
        "PAPER_TRADE_READY": bool(dependency_graph["PAPER_TRADE_READY"]["allowed"]),
        "LIVE_TRADE_READY": bool(dependency_graph["LIVE_TRADE_READY"]["allowed"]),
        "AUTONOMOUS_EXECUTION_ALLOWED": False,
        "BROKER_SUBMIT_TRANSMIT": False,
    }
    order = [
        ("HUMAN_APPROVED_ADVISORY_RUNTIME_READY", "HUMAN_APPROVED_ADVISORY_RUNTIME_READY"),
        ("MANUAL_TRADE_CAPTURE_ALLOWED", "MANUAL_TRADE_CAPTURE_ALLOWED"),
        ("TRADE_ADVICE_ALLOWED", "TRADE_ADVICE_ALLOWED"),
        ("ADVISORY_READY", "ADVISORY_ONLY"),
    ]
    highest = "BLOCKED"
    for key, label in order:
        if layers.get(key):
            highest = label
            break
    if runtime_truth_classification != "REAL_RUNTIME" and highest not in {"BLOCKED", "ADVISORY_ONLY"}:
        highest = "ADVISORY_ONLY"
    return {
        "highest_readiness_layer": highest,
        "layers": layers,
        "target_operating_mode": TARGET_OPERATING_MODE,
        "disabled_or_optional_layers": {
            "PAPER_TRADE_READY": "OPTIONAL_NOT_REQUIRED",
            "LIVE_TRADE_READY": "DISABLED_BY_POLICY",
            "AUTONOMOUS_EXECUTION_ALLOWED": "DISABLED_BY_POLICY",
            "BROKER_SUBMIT_TRANSMIT": "DISABLED_BY_POLICY",
        },
    }


def _claim_guard(*, statuses_by_id: dict[str, dict[str, Any]], dependency_graph: dict[str, dict[str, Any]]) -> dict[str, Any]:
    do_not_claim = set()
    supported_claims = [
        "Aegis Runtime Truth Kernel is read-only and deterministic.",
        "Target operating mode is human-approved advisory runtime.",
        "Live broker trading and autonomous execution are disabled by design.",
    ]
    unsupported = []
    if not dependency_graph["FEEDBACK_READY"]["allowed"] or not _payload_field_true(statuses_by_id.get("ai_feedback_review", {}), "ai_used"):
        do_not_claim.add("AI feedback is deterministic fallback only unless ai_used=true is present in evidence.")
    if not dependency_graph["ALERT_TRANSPORT_PROVEN"]["allowed"]:
        do_not_claim.add("Live email/SMS transport is not proven; alert gate or dry-run proof is not live delivery.")
    if not dependency_graph["BROKER_PAPER_LIFECYCLE_PROVEN"]["allowed"]:
        do_not_claim.add("Do not claim paper broker lifecycle readiness unless real paper evidence exists.")
    do_not_claim.add("Do not claim broker submit/transmit; broker execution is disabled by policy and out of scope.")
    do_not_claim.add("Do not claim autonomous execution; autonomous execution is disabled by policy.")
    if statuses_by_id.get("promoted_candidate_evidence", {}).get("status") != "OK":
        do_not_claim.add("No real promoted runtime actionable candidate is proven unless promotion evidence exists.")
    if statuses_by_id.get("research_dataset_binding", {}).get("status") != "OK":
        do_not_claim.add("Research dataset binding must be current.")
    if not dependency_graph["TRADE_ADVICE_ALLOWED"]["allowed"]:
        do_not_claim.add("Trade advice remains forbidden until every runtime truth dependency is evidence-backed and current.")
    if not dependency_graph["MANUAL_TRADE_CAPTURE_ALLOWED"]["allowed"]:
        do_not_claim.add("Manual trade capture remains forbidden until journaling receipt evidence and advisory-mode runtime dependencies pass.")
    for claim in sorted(do_not_claim):
        unsupported.append({"claim": claim, "reason": "Required evidence is missing, stale, invalid, or not explicitly true."})
    return {
        "do_not_claim": sorted(do_not_claim),
        "claim_violation_count": len(unsupported),
        "unsupported_claims_detected": unsupported,
        "supported_claims": supported_claims,
    }


def _recovery_item(row: dict[str, Any]) -> dict[str, Any]:
    claim_implications = list(row["claim_implications"])
    return {
        "artifact_id": row["artifact_id"],
        "what_failed": f"{row['status']}: {row['reason']}",
        "reason": f"{row['status']}: {row['reason']}",
        "why_it_matters": "; ".join(claim_implications) or "This artifact is required evidence for deterministic runtime truth.",
        "generated_by_command": row["generated_by_command"],
        "expected_path": row["expected_path"],
        "validates_with_command": row["validates_with_command"],
        "evidence_fields_required": row["evidence_fields_required"],
        "downstream_capabilities_expected_to_recover": row["downstream_capabilities_blocked"],
        "claim_implications": claim_implications,
        "claims_forbidden_until_recovery": claim_implications,
    }


def summarize_runtime_state_diff_v1(from_snapshot: dict[str, Any], to_snapshot: dict[str, Any]) -> dict[str, Any]:
    from_allowed = set(from_snapshot.get("allowed_capabilities") or [])
    to_allowed = set(to_snapshot.get("allowed_capabilities") or [])
    from_claims = set(from_snapshot.get("do_not_claim") or [])
    to_claims = set(to_snapshot.get("do_not_claim") or [])
    from_artifacts = _artifact_status_map(from_snapshot)
    to_artifacts = _artifact_status_map(to_snapshot)
    fixed = []
    degraded = []
    for artifact_id in sorted(set(from_artifacts) | set(to_artifacts)):
        old = from_artifacts.get(artifact_id)
        new = to_artifacts.get(artifact_id)
        if old == new:
            continue
        if new == "OK" and old != "OK":
            fixed.append(artifact_id)
        elif old == "OK" and new != "OK":
            degraded.append(artifact_id)
    from_recovery = len(from_snapshot.get("recovery_plan_summary") or [])
    to_recovery = len(to_snapshot.get("recovery_plan_summary") or [])
    return {
        "capabilities_gained": sorted(to_allowed - from_allowed),
        "capabilities_lost": sorted(from_allowed - to_allowed),
        "artifacts_fixed": fixed,
        "artifacts_degraded": degraded,
        "claims_newly_allowed": sorted(from_claims - to_claims),
        "claims_newly_forbidden": sorted(to_claims - from_claims),
        "recovery_burden_from": from_recovery,
        "recovery_burden_to": to_recovery,
        "recovery_burden_delta": to_recovery - from_recovery,
    }


def _dict_value_transitions(change_type: str, old: dict[str, Any], new: dict[str, Any], changed_at: str) -> list[dict[str, Any]]:
    rows = []
    for key in sorted(set(old) | set(new)):
        if old.get(key) == new.get(key):
            continue
        rows.append(_transition(change_type, key, old.get(key), new.get(key), changed_at))
    return rows


def _artifact_status_transitions(prior_snapshot: dict[str, Any], current_snapshot: dict[str, Any], changed_at: str) -> list[dict[str, Any]]:
    prior = _artifact_status_map(prior_snapshot)
    current = _artifact_status_map(current_snapshot)
    rows = []
    current_rows = {
        str(row.get("artifact_id") or ""): row
        for row in current_snapshot.get("artifact_statuses", [])
        if isinstance(row, dict)
    }
    for artifact_id in sorted(set(prior) | set(current)):
        if prior.get(artifact_id) == current.get(artifact_id):
            continue
        evidence = current_rows.get(artifact_id, {})
        rows.append(
            _transition(
                "artifact_status",
                artifact_id,
                prior.get(artifact_id),
                current.get(artifact_id),
                changed_at,
                evidence_path=str(evidence.get("path") or evidence.get("expected_path") or ""),
                downstream_impact=list(evidence.get("downstream_capabilities_blocked") or []),
            )
        )
    return rows


def _set_transitions(change_type: str, old_values: list[Any], new_values: list[Any], changed_at: str) -> list[dict[str, Any]]:
    old = {str(value) for value in old_values if str(value)}
    new = {str(value) for value in new_values if str(value)}
    rows = []
    for value in sorted(new - old):
        rows.append(_transition(change_type, value, False, True, changed_at))
    for value in sorted(old - new):
        rows.append(_transition(change_type, value, True, False, changed_at))
    return rows


def _transition(
    change_type: str,
    key: str,
    from_value: Any,
    to_value: Any,
    changed_at: str,
    *,
    evidence_path: str = "",
    downstream_impact: list[str] | None = None,
) -> dict[str, Any]:
    core = {"change_type": change_type, "key": key, "from_value": from_value, "to_value": to_value, "changed_at": changed_at}
    return {
        "transition_id": _stable_hash(core)[:20],
        **core,
        "reason": f"{change_type}:{key} changed from {from_value!r} to {to_value!r}",
        "evidence_path": evidence_path,
        "downstream_impact": downstream_impact or ([key] if change_type == "capability_allowed" else []),
    }


def _artifact_status_map(snapshot: dict[str, Any]) -> dict[str, str]:
    return {
        str(row.get("artifact_id") or ""): str(row.get("status") or "")
        for row in snapshot.get("artifact_statuses", [])
        if isinstance(row, dict) and str(row.get("artifact_id") or "")
    }


def _recovery_keys(snapshot: dict[str, Any]) -> list[str]:
    return [
        f"{row.get('artifact_id')}:{row.get('repair_command')}"
        for row in snapshot.get("recovery_plan_summary", [])
        if isinstance(row, dict)
    ]


def _find_candidates(*, root: Path, pattern: str, day_utc: str, day_scoped: bool = True) -> list[Path]:
    full_pattern = str((root / pattern).resolve()) if not Path(pattern).is_absolute() else pattern
    paths = [Path(path).resolve() for path in glob.glob(full_pattern, recursive=True)]
    files = [path for path in paths if path.is_file()]
    matched = []
    for path in files:
        if not day_scoped:
            matched.append(path)
            continue
        payload = _read_json_object(path)
        if not payload or _matches_day(payload=payload, path=path, day_utc=day_utc):
            matched.append(path)
    return sorted(matched)


def _matches_day(*, payload: dict[str, Any], path: Path, day_utc: str) -> bool:
    for key in ("day_utc", "date", "period_end", "target_day"):
        if str(payload.get(key) or "") == day_utc:
            return True
    return day_utc in path.parts


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256_file(path: Path) -> str:
    try:
        h = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return ""


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _artifact_timestamp(payload: dict[str, Any]) -> str:
    for key in ("generated_at_utc", "generated_at", "timestamp_utc", "created_at_utc", "updated_at", "produced_utc"):
        value = str(payload.get(key) or "")
        if value:
            return value
    return ""


def _parse_time(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _mtime_iso(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except OSError:
        return ""


def _has_field(payload: dict[str, Any], dotted: str) -> bool:
    current: Any = payload
    for part in dotted.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    if current is None:
        return False
    if isinstance(current, str) and not current.strip():
        return False
    return True


def _invalid_payload_reason(payload: dict[str, Any]) -> str:
    artifact_id = str(payload.get("artifact_id") or "")
    schema_id = str(payload.get("schema_id") or "")
    if artifact_id == "event_market_snapshot_v1" or schema_id == "event_market_snapshot":
        stale = str(payload.get("stale_data_status") or "").strip().upper()
        if stale and stale != "FRESH":
            reason_codes = payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else []
            suffix = f": {', '.join(str(code) for code in reason_codes[:6])}" if reason_codes else ""
            return f"Event market snapshot self-reports non-fresh context: {stale}{suffix}"
    if artifact_id == "event_validity_gate_v1" or schema_id == "event_validity_gate":
        validity = str(payload.get("validity_status") or payload.get("gate_status") or "").strip().upper()
        if validity in {"INVALID", "STALE", "FAILED", "FAIL", "BLOCKED"}:
            return f"Event validity evidence self-reports non-passing status: {validity}"
        if validity == "VALID" and (not bool(payload.get("event_packet_present", False)) or not str(payload.get("event_packet_path") or "").strip()):
            return "Event validity evidence cannot be VALID without an event packet path."
        if validity == "NO_EVENT_PACKET" and not bool(payload.get("evaluated", False)):
            return "NO_EVENT_PACKET evidence must explicitly set evaluated=true."
    if artifact_id == "alert_transport_proof_v1" or schema_id == "alert_transport_proof":
        result = str(payload.get("result") or "").strip().upper()
        mode = str(payload.get("transport_mode") or "").strip().upper()
        if result in {"FAILED", "FAIL"}:
            return f"Alert transport evidence self-reports failure: {result}"
        if mode == "LIVE" and result == "LIVE_CONFIRMED" and not bool(payload.get("delivery_confirmed", False)):
            return "LIVE_CONFIRMED requires delivery_confirmed=true."
        if mode in {"GATE_ONLY", "NONE"} and bool(payload.get("delivery_attempted", False)):
            return "GATE_ONLY/NONE alert evidence cannot claim delivery_attempted=true."
    if artifact_id == "broker_lifecycle_proof_v1" or schema_id == "broker_lifecycle_proof":
        result = str(payload.get("result") or "").strip().upper()
        mode = str(payload.get("lifecycle_mode") or "").strip().upper()
        if result in {"FAILED", "FAIL"}:
            return f"Broker lifecycle evidence self-reports failure: {result}"
        if mode == "SIMULATED" and any(bool(payload.get(field, False)) for field in ("broker_connected", "order_submitted", "order_acknowledged", "order_filled")):
            return "SIMULATED broker lifecycle proof cannot claim broker connection, submission, acknowledgement, or fill."
        if mode == "PAPER" and result == "PAPER_LIFECYCLE_CONFIRMED" and not (bool(payload.get("broker_connected", False)) and bool(payload.get("order_submitted", False)) and bool(payload.get("order_acknowledged", False))):
            return "PAPER_LIFECYCLE_CONFIRMED requires broker_connected, order_submitted, and order_acknowledged."
        if mode == "LIVE":
            return "Live broker lifecycle proof is not accepted by Aegis Lite."
    if artifact_id == "manual_execution_receipt_v1" or schema_id == "manual_execution_receipt":
        result = str(payload.get("result") or "").strip().upper()
        receipt_type = str(payload.get("receipt_type") or "").strip().upper()
        if result in {"INVALID", "MISSING", "FAILED", "FAIL", "MANUAL_FILL_RECEIPT_INVALID"}:
            return f"Manual execution receipt evidence self-reports non-passing status: {result}"
        if bool(payload.get("broker_submission_by_aegis", False)):
            return "Manual execution receipt cannot claim broker_submission_by_aegis=true."
        if bool(payload.get("autonomous_execution", False)):
            return "Manual execution receipt cannot claim autonomous_execution=true."
        if receipt_type == "NONE_DECLARED":
            if not bool(payload.get("operator_declared_no_manual_execution", False)):
                return "NONE_DECLARED receipt requires operator_declared_no_manual_execution=true."
            if bool(payload.get("manual_fill_present", False)) or bool(payload.get("fill_details_present", False)):
                return "NONE_DECLARED receipt cannot include manual fill evidence."
        if receipt_type == "MANUAL_FILL_RECORDED":
            if not (bool(payload.get("manual_fill_present", False)) and bool(payload.get("fill_details_present", False))):
                return "MANUAL_FILL_RECORDED requires manual_fill_present=true and fill_details_present=true."
            if not payload.get("trade_ids"):
                return "MANUAL_FILL_RECORDED requires at least one trade_id."
            if result != "MANUAL_FILL_RECEIPT_VALID":
                return "MANUAL_FILL_RECORDED requires result=MANUAL_FILL_RECEIPT_VALID."
    status = str(payload.get("status") or payload.get("final_status") or payload.get("gate_status") or "").strip().upper()
    if status in {"FAIL", "FAILED", "BLOCKED", "INVALID"}:
        return f"Artifact self-reports non-passing status: {status}"
    if bool(payload.get("canonical_eod_state_mutated", False)):
        return "Artifact reports canonical EOD state mutation from a non-authoritative path."
    return ""


def _alert_transport_proven(row: dict[str, Any]) -> bool:
    if row.get("status") != "OK":
        return False
    payload = _read_json_object(Path(str(row.get("path") or "")))
    return _alert_transport_payload_proven(payload)


def _alert_transport_payload_proven(payload: dict[str, Any]) -> bool:
    if str(payload.get("schema_id") or "") == "alert_transport_proof":
        return (
            str(payload.get("transport_mode") or "").upper() == "LIVE"
            and str(payload.get("result") or "").upper() == "LIVE_CONFIRMED"
            and bool(payload.get("delivery_confirmed", False))
            and not bool(payload.get("dry_run", False))
        )
    attempts = payload.get("alert_attempts") if isinstance(payload.get("alert_attempts"), list) else []
    statuses = {str(attempt.get("delivery_status") or "").upper() for attempt in attempts if isinstance(attempt, dict)}
    return "LIVE_EMAIL_SENT" in statuses or "LIVE_SMS_SENT" in statuses or "LIVE_EMAIL_PROVEN" in statuses


def _alert_gate_proven(row: dict[str, Any]) -> bool:
    if row.get("status") != "OK":
        return False
    payload = _read_json_object(Path(str(row.get("path") or "")))
    if str(payload.get("schema_id") or "") != "alert_transport_proof":
        return _alert_transport_payload_proven(payload)
    return bool(payload.get("evaluated", False)) and str(payload.get("result") or "").upper() in {
        "GATE_ONLY_NO_TRANSPORT",
        "DRY_RUN_CONFIRMED",
        "LIVE_CONFIRMED",
        "NOT_CONFIGURED",
    }


def _alert_dry_run_proven(row: dict[str, Any]) -> bool:
    if row.get("status") != "OK":
        return False
    payload = _read_json_object(Path(str(row.get("path") or "")))
    return (
        str(payload.get("schema_id") or "") == "alert_transport_proof"
        and str(payload.get("transport_mode") or "").upper() == "DRY_RUN"
        and str(payload.get("result") or "").upper() == "DRY_RUN_CONFIRMED"
        and bool(payload.get("delivery_confirmed", False))
        and bool(payload.get("dry_run", False))
    )


def _broker_simulation_proven(row: dict[str, Any]) -> bool:
    if row.get("status") != "OK":
        return False
    payload = _read_json_object(Path(str(row.get("path") or "")))
    return (
        str(payload.get("schema_id") or "") == "broker_lifecycle_proof"
        and str(payload.get("lifecycle_mode") or "").upper() == "SIMULATED"
        and str(payload.get("result") or "").upper() == "SIMULATED_CONFIRMED"
        and bool(payload.get("evaluated", False))
    )


def _broker_paper_lifecycle_proven(row: dict[str, Any]) -> bool:
    if row.get("status") != "OK":
        return False
    payload = _read_json_object(Path(str(row.get("path") or "")))
    if str(payload.get("schema_id") or "") == "broker_lifecycle_proof":
        return (
            str(payload.get("lifecycle_mode") or "").upper() == "PAPER"
            and str(payload.get("result") or "").upper() == "PAPER_LIFECYCLE_CONFIRMED"
            and str(payload.get("account_type") or "").lower() == "paper"
            and bool(payload.get("broker_connected", False))
            and bool(payload.get("order_submitted", False))
            and bool(payload.get("order_acknowledged", False))
        )
    return str(payload.get("status") or "").upper() in {"OK", "PASS"}


def _event_validity_no_event_packet(row: dict[str, Any]) -> bool:
    if row.get("status") != "OK":
        return False
    payload = _read_json_object(Path(str(row.get("path") or "")))
    return str(payload.get("validity_status") or "").upper() == "NO_EVENT_PACKET"


def _manual_execution_receipt_summary(row: dict[str, Any]) -> dict[str, Any]:
    if row.get("status") != "OK":
        return {
            "status": row.get("status") or "MISSING",
            "result": "",
            "manual_trade_receipt_count": 0,
            "last_receipt_id": None,
            "manual_capture_semantics": "JOURNALING_AUDIT_ONLY",
        }
    payload = _read_json_object(Path(str(row.get("path") or "")))
    return {
        "status": "OK",
        "result": str(payload.get("result") or ""),
        "receipt_type": str(payload.get("receipt_type") or ""),
        "manual_fill_present": bool(payload.get("manual_fill_present", False)),
        "fill_details_present": bool(payload.get("fill_details_present", False)),
        "manual_trade_receipt_count": int(payload.get("manual_trade_receipt_count") or len(payload.get("trade_ids") or [])),
        "last_receipt_id": payload.get("last_receipt_id"),
        "validation_status": payload.get("validation_status") or "VALID",
        "broker_submission_by_aegis": bool(payload.get("broker_submission_by_aegis", False)),
        "autonomous_execution": bool(payload.get("autonomous_execution", False)),
        "manual_capture_semantics": "JOURNALING_AUDIT_ONLY",
    }


def _payload_field_true(row: dict[str, Any], field: str) -> bool:
    if row.get("status") != "OK":
        return False
    payload = _read_json_object(Path(str(row.get("path") or "")))
    return bool(payload.get(field, False))


def _research_dataset_binding_has_gaps(payload: dict[str, Any]) -> bool:
    gaps = payload.get("dataset_gaps") if isinstance(payload.get("dataset_gaps"), list) else []
    for row in gaps:
        if not isinstance(row, dict):
            continue
        if str(row.get("current_status") or "").upper() in {"MISSING", "STALE", "INVALID", "UNBOUND"}:
            return True
    return False


def _format_command(template: str, *, truth_root: Path, day_utc: str) -> str:
    return template.format(truth_root=str(truth_root), day=day_utc)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _authority_drift_findings() -> list[dict[str, str]]:
    checks = {
        "constellation_2/common/aegis_chatgpt_control_packet_v1.py": [
            "def _runtime_truth_classification(",
            "def _trade_advice_gate(",
            "def _do_not_claim(",
            "trade_gate = _trade_advice_gate(",
            "do_not_claim = _do_not_claim(",
        ],
        "ops/tools/build_aegis_audit_handoff_v1.py": [
            "do_not_claim = [str(item) for item in packet.get(\"do_not_claim\"",
            "readiness = packet.get(\"readiness_state\")",
        ],
    }
    findings: list[dict[str, str]] = []
    for relpath, markers in checks.items():
        path = REPO_ROOT / relpath
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for marker in markers:
            if marker in text:
                findings.append({"path": relpath, "marker": marker, "reason": "legacy readiness or claim logic marker detected"})
    return findings
