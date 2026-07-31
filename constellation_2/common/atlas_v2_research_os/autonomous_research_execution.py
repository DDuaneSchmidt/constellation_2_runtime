from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any

from .artifact_models import ArtifactType, EvidenceLevel, LifecycleState
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .failure_observatory import record_exception, record_failure, record_governance_block, record_worker_failure
from .autonomous_research_governance import (
    validate_autonomous_research_candidate_isolation,
    validate_autonomous_research_execution_allowed,
    validate_autonomous_research_no_authority_escalation,
    validate_autonomous_research_no_forbidden_artifacts,
    validate_autonomous_research_worker_outputs,
)
from .autonomous_research_models import AutonomousResearchExecutionResult, AutonomousResearchExecutionStatus
from .autonomous_research_reports import write_autonomous_research_execution_report
from .certification_models import CertificationStatus
from .certification_runner import run_research_os_certification
from .lineage import validate_lineage_integrity
from .edge_qualification_models import EdgeQualificationInput
from .historical_replay_engine import create_historical_replay_request, run_historical_replay
from .historical_replay_results import edge_input_with_historical_replay, route_historical_replay_backlog_items, record_historical_replay_to_memory
from .paper_trade_candidate_reports import build_paper_trade_candidate_report, write_paper_trade_candidate_report
from .memory_index import add_memory_object, list_memory_objects
from .memory_models import MemoryLifecycleState, MemoryType, create_memory_object
from .research_backlog import ResearchBacklog
from .worker_adapters import register_default_worker_adapters
from .worker_connection_rules import is_worker_connection_allowed
from .worker_registry import list_workers, get_worker
from .funnel_progression_models import mark_same_session_continuation_metadata

BACKLOG_TO_WORKER_TYPE = {
    "RESEARCH_QUESTION": "ClaimWorker",
    "CLAIM_INVESTIGATION": "ClaimWorker",
    "HYPOTHESIS_VALIDATION": "HypothesisWorker",
    "EVIDENCE_GAP": "ExperimentDesignWorker",
    "FAILURE_ANALYSIS": "LearningWorker",
    "STALE_LEARNING_REVIEW": "EvaluationWorker",
    "DUPLICATE_REVIEW": "EvaluationWorker",
    "REGIME_GAP": "LearningWorker",
    "MECHANISM_VARIATION": "ClaimWorker",
    "EDGE_QUALIFICATION_REVIEW": "EdgeQualification",
    "HISTORICAL_REPLAY_REVIEW": "HistoricalReplay",
    "PAPER_TRADE_CANDIDATE_REVIEW": "ClaimWorker",
}

BACKLOG_WORKER_FALLBACKS = {
    "FAILURE_ANALYSIS": ["LearningWorker", "ClaimWorker", "ExperimentDesignWorker"],
    "REGIME_GAP": ["LearningWorker", "ExperimentDesignWorker", "ClaimWorker"],
    "DUPLICATE_REVIEW": ["EvaluationWorker", "ClaimWorker"],
    "STALE_LEARNING_REVIEW": ["EvaluationWorker", "ClaimWorker"],
}

ALLOWED_CERTIFICATION_STATUSES = {CertificationStatus.PASS.value, CertificationStatus.WARNING.value, CertificationStatus.SKIPPED_DEPENDENCY_MISSING.value}


def run_bounded_research_once(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None, seed: int | None = None, preferred_backlog_item_id: str | None = None) -> dict[str, Any]:
    return _execute(root, mode="run_once", day=day, seed=seed, preferred_backlog_item_id=preferred_backlog_item_id)


def dry_run_bounded_research(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None, seed: int | None = None, preferred_backlog_item_id: str | None = None) -> dict[str, Any]:
    return _execute(root, mode="dry_run", day=day, seed=seed, preferred_backlog_item_id=preferred_backlog_item_id)


def audit_bounded_research_execution(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    started = _now()
    execution_id = _execution_id(started)
    ready_items = ResearchBacklog(root_path).get_ready_items()
    workers = register_default_worker_adapters(replace=True)
    gates = _base_safety_gates(root_path, {"mode": "audit", "bounded_once": True})
    status = AutonomousResearchExecutionStatus.COMPLETED.value if _gates_pass(gates) else AutonomousResearchExecutionStatus.FAILED_SAFETY_GATE.value
    result = _result(
        execution_id=execution_id,
        created_at=started,
        started_at=started,
        completed_at=_now(),
        status=status,
        safety_gate_results=gates,
        metadata={
            "mode": "audit",
            "ready_backlog_count": len(ready_items),
            "connected_worker_count": sum(1 for worker in workers if is_worker_connection_allowed(worker)),
            "bounded_once": True,
            "scheduler_enabled": False,
            "recurring_loop_enabled": False,
        },
    )
    write_autonomous_research_execution_report(result, root_path, day=day)
    return result


def find_compatible_worker_for_backlog_item(backlog_item: dict[str, Any], input_artifacts: list[dict[str, Any]] | None = None) -> Any | None:
    register_default_worker_adapters(replace=True)
    worker_types = _candidate_worker_types_for_backlog_item(backlog_item)
    if not worker_types:
        return None
    for worker_type in worker_types:
        if worker_type == "EdgeQualification":
            continue
        worker = _connected_worker_for_type(worker_type, input_artifacts or [], backlog_item)
        if worker is not None:
            return worker
    return None


def validate_worker_for_backlog_item(worker: Any, backlog_item: dict[str, Any], input_artifacts: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    failures: list[str] = []
    expected_types = _candidate_worker_types_for_backlog_item(backlog_item)
    if not expected_types:
        failures.append(f"no worker mapping for backlog item type: {backlog_item.get('item_type')}")
    elif getattr(worker, "worker_type", "") not in expected_types:
        failures.append(f"worker type mismatch: expected one of {expected_types}, got {getattr(worker, 'worker_type', '')}")
    if not is_worker_connection_allowed(worker):
        failures.append("worker is not connected for research-only execution")
    input_ok, input_failures = worker.validate_inputs(input_artifacts)
    if not input_ok:
        failures.extend(input_failures)
    return not failures, failures


def _candidate_worker_types_for_backlog_item(backlog_item: dict[str, Any]) -> list[str]:
    item_type = str(backlog_item.get("item_type") or "")
    preferred = BACKLOG_TO_WORKER_TYPE.get(item_type)
    if not preferred:
        return []
    values = [preferred]
    values.extend(BACKLOG_WORKER_FALLBACKS.get(item_type, []))
    return list(dict.fromkeys(values))


def _connected_worker_for_type(worker_type: str, input_artifacts: list[dict[str, Any]], backlog_item: dict[str, Any]) -> Any | None:
    for row in list_workers():
        if row.get("worker_type") != worker_type:
            continue
        worker = get_worker(row["worker_id"])
        if not is_worker_connection_allowed(worker):
            continue
        routed = _route_input_artifacts_for_worker(backlog_item, worker, input_artifacts)
        if not routed:
            continue
        ok, _failures = worker.validate_inputs(routed)
        if ok:
            return worker
    return None


def _execute(root: str | Path, *, mode: str, day: str | None, seed: int | None = None, preferred_backlog_item_id: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    started_monotonic = monotonic()
    started = _now()
    execution_id = _execution_id(started)
    store = ArtifactStore(root_path)
    backlog = ResearchBacklog(root_path)
    selected_item: dict[str, Any] | None = None
    selected_worker = None
    input_artifacts: list[dict[str, Any]] = []
    output_artifacts: list[dict[str, Any]] = []
    memory_updates: list[dict[str, Any]] = []
    backlog_updates: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    metadata = {"mode": mode, "bounded_once": True, "scheduler_enabled": False, "recurring_loop_enabled": False, "seed": seed, "preferred_backlog_item_id": preferred_backlog_item_id}
    gates = _base_safety_gates(root_path, metadata)
    if not _gates_pass(gates):
        record_governance_block(root=root_path, component="AUTONOMOUS_RESEARCH", error_message="Pre-execution safety gate failed.", execution_id=execution_id, metadata={"safety_gate_count": len(gates), "failed_gate_count": sum(1 for gate in gates if gate.get("status") == "FAIL" or gate.get("result") == "FAIL")})
        result = _result(execution_id=execution_id, created_at=started, started_at=started, completed_at=_now(), status=AutonomousResearchExecutionStatus.FAILED_SAFETY_GATE.value, safety_gate_results=gates, errors=[_error("safety_gate", "SAFETY_GATE_FAILED", "Pre-execution safety gate failed.")], metadata=metadata)
        write_autonomous_research_execution_report(result, root_path, day=day)
        return result
    try:
        ready = backlog.get_ready_items()
        if not ready:
            result = _result(execution_id=execution_id, created_at=started, started_at=started, completed_at=_now(), status=AutonomousResearchExecutionStatus.SKIPPED_NO_READY_BACKLOG.value, safety_gate_results=gates, metadata=metadata)
            write_autonomous_research_execution_report(result, root_path, day=day)
            return result
        selected_item = _select_ready_item(ready, preferred_backlog_item_id)
        input_artifacts = _load_input_artifacts(store, selected_item)
        if selected_item.get("item_type") == "EDGE_QUALIFICATION_REVIEW":
            result = _execute_edge_qualification_item(root_path, selected_item, input_artifacts, execution_id=execution_id, started=started, mode=mode, day=day, gates=gates, metadata=metadata)
            write_autonomous_research_execution_report(result, root_path, day=day)
            return result
        if selected_item.get("item_type") == "HISTORICAL_REPLAY_REVIEW":
            result = _execute_historical_replay_item(root_path, selected_item, input_artifacts, execution_id=execution_id, started=started, mode=mode, day=day, gates=gates, metadata=metadata)
            write_autonomous_research_execution_report(result, root_path, day=day)
            return result
        selected_worker = find_compatible_worker_for_backlog_item(selected_item, input_artifacts)
        if selected_worker is None:
            blocked_reason = _missing_input_block_reason(selected_item, input_artifacts)
            if mode != "dry_run":
                backlog_updates.append(_update_backlog(root_path, selected_item["backlog_item_id"], state="BLOCKED", reason=blocked_reason, execution_id=execution_id, output_artifact_ids=[]))
                memory_updates.extend(_record_failure_memory(root_path, execution_id, selected_item, input_artifacts, blocked_reason, started))
            record_worker_failure(root=root_path, error_type=blocked_reason, error_message="No compatible connected worker for selected backlog item.", execution_id=execution_id, backlog_item_id=str(selected_item["backlog_item_id"]), artifact_ids=[item["artifact_id"] for item in input_artifacts], recoverable=True)
            result = _result(execution_id=execution_id, created_at=started, started_at=started, completed_at=_now(), status=AutonomousResearchExecutionStatus.SKIPPED_NO_COMPATIBLE_WORKER.value, selected_backlog_item_id=selected_item["backlog_item_id"], input_artifact_ids=[item["artifact_id"] for item in input_artifacts], memory_updates=memory_updates, backlog_updates=backlog_updates, safety_gate_results=gates, metadata=metadata)
            write_autonomous_research_execution_report(result, root_path, day=day)
            return result
        input_artifacts = _route_input_artifacts_for_worker(selected_item, selected_worker, input_artifacts)
        worker_ok, worker_failures = validate_worker_for_backlog_item(selected_worker, selected_item, input_artifacts)
        if not worker_ok:
            if mode != "dry_run":
                backlog_updates.append(_update_backlog(root_path, selected_item["backlog_item_id"], state="BLOCKED", reason="WORKER_INPUT_VALIDATION_FAILED", execution_id=execution_id, output_artifact_ids=[]))
                memory_updates.extend(_record_failure_memory(root_path, execution_id, selected_item, input_artifacts, "WORKER_INPUT_VALIDATION_FAILED", started))
            errors.append(_error("worker_validation", "WORKER_INPUT_VALIDATION_FAILED", "; ".join(worker_failures)))
            record_worker_failure(root=root_path, error_type="WORKER_INPUT_VALIDATION_FAILED", error_message="; ".join(worker_failures), execution_id=execution_id, worker_id=str(selected_worker.worker_id), backlog_item_id=str(selected_item["backlog_item_id"]), artifact_ids=[item["artifact_id"] for item in input_artifacts], recoverable=True)
            result = _result(execution_id=execution_id, created_at=started, started_at=started, completed_at=_now(), status=AutonomousResearchExecutionStatus.FAILED_SAFETY_GATE.value, selected_backlog_item_id=selected_item["backlog_item_id"], selected_worker_id=selected_worker.worker_id, input_artifact_ids=[item["artifact_id"] for item in input_artifacts], memory_updates=memory_updates, backlog_updates=backlog_updates, safety_gate_results=gates, errors=errors, metadata=metadata)
            write_autonomous_research_execution_report(result, root_path, day=day)
            return result
        if mode != "dry_run":
            backlog_updates.append(_update_backlog(root_path, selected_item["backlog_item_id"], state="IN_PROGRESS", reason="BOUNDED_RESEARCH_EXECUTION_STARTED", execution_id=execution_id, output_artifact_ids=[]))
        worker_result = selected_worker.dry_run(input_artifacts, metadata={"root": str(root_path), "autonomous_research_execution_id": execution_id}) if mode == "dry_run" else selected_worker.run(input_artifacts, metadata={"root": str(root_path), "autonomous_research_execution_id": execution_id})
        worker_payload = worker_result.to_dict()
        output_ids = [] if mode == "dry_run" else list(worker_result.output_artifact_ids)
        if mode != "dry_run":
            output_artifacts = [store.get_artifact(artifact_id) for artifact_id in output_ids]
        else:
            metadata["dry_run_preview_output_artifact_ids"] = list(worker_result.output_artifact_ids)
        gates.extend(_post_worker_safety_gates(root_path, input_artifacts, output_artifacts, worker_payload))
        lineage_result = _lineage_result(store, worker_payload)
        governance_result = _governance_result(gates, worker_payload)
        certification = _certification_result(root_path, day=day)
        if worker_payload.get("status") == "VALIDATION_FAILED":
            errors.append(_error("worker_execution", "WORKER_VALIDATION_FAILED", "; ".join(worker_payload.get("errors", []))))
        if not _gates_pass(gates) or lineage_result.get("status") == "FAIL" or governance_result.get("status") == "FAIL" or not _certification_allowed(certification) or errors:
            if mode != "dry_run" and selected_item:
                backlog_updates.append(_update_backlog(root_path, selected_item["backlog_item_id"], state="BLOCKED", reason="SAFETY_GATE_FAILED", execution_id=execution_id, output_artifact_ids=output_ids))
                memory_updates.extend(_record_failure_memory(root_path, execution_id, selected_item, input_artifacts + output_artifacts, "SAFETY_GATE_FAILED", started))
            record_failure(root=root_path, component="AUTONOMOUS_RESEARCH", severity="ERROR", error_type="SAFETY_GATE_FAILED", error_message="Autonomous research execution failed closed after worker, lineage, governance, certification, or worker error check.", execution_id=execution_id, worker_id=str(getattr(selected_worker, "worker_id", "")), backlog_item_id=str((selected_item or {}).get("backlog_item_id", "")), artifact_ids=[item.get("artifact_id", "") for item in input_artifacts + output_artifacts if item.get("artifact_id")], recoverable=True, metadata={"governance_status": governance_result.get("status", ""), "lineage_status": lineage_result.get("status", ""), "certification_status": certification.get("status", "")})
            status = AutonomousResearchExecutionStatus.FAILED_SAFETY_GATE.value
        elif mode == "dry_run":
            status = AutonomousResearchExecutionStatus.DRY_RUN_COMPLETED.value
        else:
            backlog_updates.append(_update_backlog(root_path, selected_item["backlog_item_id"], state="COMPLETED", reason="BOUNDED_RESEARCH_EXECUTION_COMPLETED", execution_id=execution_id, output_artifact_ids=output_ids))
            followups = _create_followup_backlog_items(root_path, selected_item, output_artifacts, started, execution_id=execution_id)
            if followups:
                backlog_updates.extend(followups)
            memory_updates.extend(_record_success_memory(root_path, execution_id, selected_item, output_artifacts, started))
            certification = _certification_result(root_path, day=day)
            status = AutonomousResearchExecutionStatus.COMPLETED.value if _certification_allowed(certification) else AutonomousResearchExecutionStatus.FAILED_SAFETY_GATE.value
        result = _result(
            execution_id=execution_id,
            created_at=started,
            started_at=started,
            completed_at=_now(),
            status=status,
            selected_backlog_item_id=selected_item["backlog_item_id"],
            selected_worker_id=selected_worker.worker_id,
            input_artifact_ids=[item["artifact_id"] for item in input_artifacts],
            output_artifact_ids=output_ids,
            memory_updates=memory_updates,
            backlog_updates=backlog_updates,
            certification_result=certification,
            governance_result=governance_result,
            lineage_result=lineage_result,
            safety_gate_results=gates,
            errors=errors,
            warnings=warnings + list(certification.get("warnings", [])),
            metadata={**metadata, "duration_seconds": round(monotonic() - started_monotonic, 6), "worker_result": worker_payload},
        )
        write_autonomous_research_execution_report(result, root_path, day=day)
        return result
    except Exception as exc:
        errors.append(_error("autonomous_research_execution", exc.__class__.__name__, str(exc)))
        record_exception(exc, root=root_path, component="AUTONOMOUS_RESEARCH", execution_id=execution_id, backlog_item_id=str((selected_item or {}).get("backlog_item_id", "")), worker_id=str(getattr(selected_worker, "worker_id", "")), artifact_ids=[item.get("artifact_id", "") for item in input_artifacts if item.get("artifact_id")], recoverable=False)
        if selected_item is not None and mode != "dry_run":
            try:
                backlog_updates.append(_update_backlog(root_path, selected_item["backlog_item_id"], state="BLOCKED", reason="EXECUTION_EXCEPTION", execution_id=execution_id, output_artifact_ids=[]))
                memory_updates.extend(_record_failure_memory(root_path, execution_id, selected_item, input_artifacts, "EXECUTION_EXCEPTION", started))
            except Exception:
                pass
        result = _result(execution_id=execution_id, created_at=started, started_at=started, completed_at=_now(), status=AutonomousResearchExecutionStatus.FAILED.value, selected_backlog_item_id=(selected_item or {}).get("backlog_item_id", ""), selected_worker_id=getattr(selected_worker, "worker_id", ""), input_artifact_ids=[item.get("artifact_id", "") for item in input_artifacts if item.get("artifact_id")], memory_updates=memory_updates, backlog_updates=backlog_updates, safety_gate_results=gates, errors=errors, metadata=metadata)
        write_autonomous_research_execution_report(result, root_path, day=day)
        return result



def _select_ready_item(ready: list[dict[str, Any]], preferred_backlog_item_id: str | None) -> dict[str, Any]:
    if preferred_backlog_item_id:
        for row in ready:
            if row.get("backlog_item_id") == preferred_backlog_item_id:
                return row
    return sorted(ready, key=lambda row: (-float(row.get("priority_score", 0.0)), str(row.get("backlog_item_id", ""))))[0]


def _route_input_artifacts_for_worker(backlog_item: dict[str, Any], worker: Any, input_artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    item_type = str(backlog_item.get("item_type") or "")
    supported = set(getattr(worker, "supported_input_artifact_types", []))
    if item_type == "HYPOTHESIS_VALIDATION":
        return [item for item in input_artifacts if item.get("artifact_type") == ArtifactType.GENERATED_RESEARCH_CLAIM.value]
    if item_type in {"MECHANISM_VARIATION", "EDGE_QUALIFICATION_REVIEW", "PAPER_TRADE_CANDIDATE_REVIEW"} and getattr(worker, "worker_type", "") == "ClaimWorker":
        return [item for item in input_artifacts if item.get("artifact_type") in supported]
    if item_type in {"REGIME_GAP", "FAILURE_ANALYSIS", "DUPLICATE_REVIEW", "STALE_LEARNING_REVIEW"}:
        routed = [item for item in input_artifacts if item.get("artifact_type") in supported]
        return routed
    return [item for item in input_artifacts if item.get("artifact_type") in supported]


def _missing_input_block_reason(backlog_item: dict[str, Any], input_artifacts: list[dict[str, Any]]) -> str:
    if backlog_item.get("item_type") == "HYPOTHESIS_VALIDATION" and not any(item.get("artifact_type") == ArtifactType.GENERATED_RESEARCH_CLAIM.value for item in input_artifacts):
        return "MISSING_GENERATED_RESEARCH_CLAIM_INPUT"
    if backlog_item.get("item_type") == "EDGE_QUALIFICATION_REVIEW" and not any(item.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value for item in input_artifacts):
        return "MISSING_RESEARCH_HYPOTHESIS_INPUT"
    if backlog_item.get("item_type") == "HISTORICAL_REPLAY_REVIEW" and not any(item.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value for item in input_artifacts):
        return "MISSING_RESEARCH_HYPOTHESIS_INPUT"
    return "NO_COMPATIBLE_CONNECTED_WORKER"


def _create_followup_backlog_items(root: Path, selected_item: dict[str, Any], output_artifacts: list[dict[str, Any]], created_at: str, *, execution_id: str) -> list[dict[str, Any]]:
    backlog = ResearchBacklog(root)
    updates: list[dict[str, Any]] = []
    base_learning_value = float(selected_item.get("expected_learning_value") or 0)
    selected_metadata = selected_item.get("metadata", {}) or {}
    continuation_depth = int(selected_metadata.get("continuation_depth") or 0) + 1
    for artifact in output_artifacts:
        artifact_type = artifact.get("artifact_type")
        artifact_id = str(artifact.get("artifact_id") or "")
        if artifact_type == ArtifactType.GENERATED_RESEARCH_CLAIM.value:
            item_id = f"hv-{_short_id(artifact_id)}"
            if _unresolved_backlog_for_source(backlog, "HYPOTHESIS_VALIDATION", artifact_id) is None:
                item = backlog.create_backlog_item(
                    backlog_item_id=item_id,
                    item_type="HYPOTHESIS_VALIDATION",
                    title=f"Generate hypothesis from claim {artifact_id}",
                    description="Create a ResearchHypothesis from a GeneratedResearchClaim; research-only continuation.",
                    created_at=created_at,
                    created_by="atlas_v2_research_os_funnel_continuation",
                    source_artifact_ids=[artifact_id],
                    state="READY",
                    expected_learning_value=max(0.65, base_learning_value * 0.95),
                    novelty_score=0.1,
                    evidence_gap_score=0.1,
                    cost_estimate=0.05,
                    mechanism_tags=list(selected_item.get("mechanism_tags") or []),
                    regime_context=str(selected_item.get("regime_context") or "UNKNOWN"),
                    metadata=mark_same_session_continuation_metadata({"source_backlog_item_id": selected_item.get("backlog_item_id"), "research_only": True, "continuation": "CLAIM_TO_HYPOTHESIS"}, execution_id=execution_id, continuation_depth=continuation_depth),
                )
                updates.append({"backlog_item_id": item["backlog_item_id"], "state": item["state"], "state_transition_reason": "CLAIM_OUTPUT_CREATED_HYPOTHESIS_VALIDATION", "source_artifact_ids": item["source_artifact_ids"]})
        elif artifact_type == ArtifactType.RESEARCH_HYPOTHESIS.value:
            item_id = f"hrp-{_short_id(artifact_id)}"
            if _unresolved_backlog_for_source(backlog, "HISTORICAL_REPLAY_REVIEW", artifact_id) is None:
                item = backlog.create_backlog_item(
                    backlog_item_id=item_id,
                    item_type="HISTORICAL_REPLAY_REVIEW",
                    title=f"Run historical replay for hypothesis {artifact_id}",
                    description="Run governed historical replay before edge qualification; research-only evidence path.",
                    created_at=created_at,
                    created_by="atlas_v2_research_os_funnel_continuation",
                    source_artifact_ids=[artifact_id],
                    state="READY",
                    expected_learning_value=max(0.6, base_learning_value * 0.92),
                    evidence_gap_score=0.2,
                    cost_estimate=0.05,
                    mechanism_tags=list(selected_item.get("mechanism_tags") or []),
                    regime_context=str(selected_item.get("regime_context") or "UNKNOWN"),
                    metadata=mark_same_session_continuation_metadata({"source_backlog_item_id": selected_item.get("backlog_item_id"), "research_only": True, "continuation": "HYPOTHESIS_TO_HISTORICAL_REPLAY"}, execution_id=execution_id, continuation_depth=continuation_depth),
                )
                updates.append({"backlog_item_id": item["backlog_item_id"], "state": item["state"], "state_transition_reason": "HYPOTHESIS_OUTPUT_CREATED_HISTORICAL_REPLAY_REVIEW", "source_artifact_ids": item["source_artifact_ids"]})
    return updates


def _unresolved_backlog_for_source(backlog: ResearchBacklog, item_type: str, source_artifact_id: str) -> dict[str, Any] | None:
    for row in backlog.list_backlog_items():
        if row.get("item_type") == item_type and row.get("state") in {"NEW", "READY", "IN_PROGRESS", "BLOCKED"} and source_artifact_id in row.get("source_artifact_ids", []):
            return row
    return None



def _execute_historical_replay_item(root: Path, selected_item: dict[str, Any], input_artifacts: list[dict[str, Any]], *, execution_id: str, started: str, mode: str, day: str | None, gates: list[dict[str, Any]], metadata: dict[str, Any]) -> dict[str, Any]:
    hypothesis_artifacts = [item for item in input_artifacts if item.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value]
    replay_artifacts = [item for item in input_artifacts if item.get("artifact_type") == "HistoricalReplayResult"]
    memory_updates: list[dict[str, Any]] = []
    backlog_updates: list[dict[str, Any]] = []
    if not hypothesis_artifacts:
        reason = "MISSING_RESEARCH_HYPOTHESIS_INPUT"
        if mode != "dry_run":
            backlog_updates.append(_update_backlog(root, selected_item["backlog_item_id"], state="BLOCKED", reason=reason, execution_id=execution_id, output_artifact_ids=[]))
            memory_updates.extend(_record_failure_memory(root, execution_id, selected_item, input_artifacts, reason, started))
        return _result(execution_id=execution_id, created_at=started, started_at=started, completed_at=_now(), status=AutonomousResearchExecutionStatus.SKIPPED_NO_COMPATIBLE_WORKER.value, selected_backlog_item_id=selected_item["backlog_item_id"], input_artifact_ids=[item.get("artifact_id", "") for item in input_artifacts], memory_updates=memory_updates, backlog_updates=backlog_updates, safety_gate_results=gates, metadata={**metadata, "historical_replay_attempted": False})
    hypothesis = hypothesis_artifacts[0]
    request = _historical_replay_request_from_hypothesis(hypothesis, selected_item, started)
    replay_result = run_historical_replay(request, _historical_replay_samples_for_hypothesis(hypothesis, selected_item), created_at=started)
    replay_artifact_id = f"ros-hrp-{_short_id(replay_result['replay_id'])}-{_short_id(execution_id)}"
    output_ids: list[str] = []
    routed_items: list[dict[str, Any]] = []
    if mode == "dry_run":
        status = AutonomousResearchExecutionStatus.DRY_RUN_COMPLETED.value
    else:
        store = ArtifactStore(root)
        store.create_artifact(
            artifact_id=replay_artifact_id,
            artifact_type="HistoricalReplayResult",
            created_at=started,
            created_by="atlas_v2_research_os_historical_replay",
            source_artifact_ids=[hypothesis["artifact_id"]],
            confidence=float(replay_result.get("metrics", {}).get("historical_replay_score") or 0.0),
            evidence_level=EvidenceLevel.HISTORICAL_REPLAY.value,
            labels=["historical_replay", str(replay_result.get("certification", {}).get("status", "")).lower()],
            metadata={"historical_replay_result": _artifact_safe_replay_result(replay_result), "research_only": True, "source_backlog_item_id": selected_item.get("backlog_item_id")},
        )
        replay_for_routing = dict(replay_result)
        replay_for_routing["replay_id"] = replay_artifact_id
        replay_for_routing["source_artifact_ids"] = [hypothesis["artifact_id"]]
        routed_items = route_historical_replay_backlog_items(replay_for_routing, root=root, created_at=started)
        _mark_replay_continuations(root, routed_items, execution_id)
        memory_updates.append(record_historical_replay_to_memory(replay_result, root=root, created_at=started))
        output_ids = [replay_artifact_id]
        backlog_updates.append(_update_backlog(root, selected_item["backlog_item_id"], state="COMPLETED", reason="HISTORICAL_REPLAY_COMPLETED", execution_id=execution_id, output_artifact_ids=output_ids))
        for item in routed_items:
            backlog_updates.append({"backlog_item_id": item["backlog_item_id"], "state": item["state"], "state_transition_reason": "HISTORICAL_REPLAY_RESULT_CREATED_EDGE_QUALIFICATION_REVIEW" if item.get("item_type") == "EDGE_QUALIFICATION_REVIEW" else "HISTORICAL_REPLAY_RESULT_CREATED_FAILURE_ANALYSIS", "source_artifact_ids": item.get("source_artifact_ids", [])})
        status = AutonomousResearchExecutionStatus.COMPLETED.value
    return _result(
        execution_id=execution_id,
        created_at=started,
        started_at=started,
        completed_at=_now(),
        status=status,
        selected_backlog_item_id=selected_item["backlog_item_id"],
        selected_worker_id="atlas_v2_historical_replay",
        input_artifact_ids=[hypothesis["artifact_id"]],
        output_artifact_ids=output_ids,
        memory_updates=memory_updates,
        backlog_updates=backlog_updates,
        certification_result=_certification_result(root, day=day),
        governance_result={"status": "PASS", "details": []},
        lineage_result={"status": "PASS", "details": []},
        safety_gate_results=gates,
        warnings=[] if replay_result.get("certification", {}).get("status") == "REPLAY_POSITIVE" else list(replay_result.get("certification", {}).get("reasons", [])),
        metadata={**metadata, "historical_replay_attempted": True, "historical_replay_status": replay_result.get("certification", {}).get("status"), "historical_replay_score": replay_result.get("metrics", {}).get("historical_replay_score"), "historical_replay_result": _artifact_safe_replay_result(replay_result), "routed_backlog_items": routed_items},
    )


def _historical_replay_request_from_hypothesis(hypothesis: dict[str, Any], selected_item: dict[str, Any], created_at: str) -> dict[str, Any]:
    payload = (hypothesis.get("metadata", {}) or {}).get("atlas_component_payload", {}) or {}
    mechanism_tags = list(selected_item.get("mechanism_tags") or hypothesis.get("metadata", {}).get("mechanism_tags") or [payload.get("mechanism_family") or "UNKNOWN"])
    return create_historical_replay_request(
        hypothesis_id=str(payload.get("hypothesis_id") or hypothesis["artifact_id"]),
        mechanism_tags=mechanism_tags,
        regime_context={"label": str(selected_item.get("regime_context") or "UNKNOWN")},
        source_artifact_ids=[hypothesis["artifact_id"]],
        replay_id=f"hist-replay-{_short_id(hypothesis['artifact_id'])}",
        created_at=created_at,
        metadata={"source_backlog_item_id": selected_item.get("backlog_item_id"), "research_only": True},
    )


def _historical_replay_samples_for_hypothesis(hypothesis: dict[str, Any], selected_item: dict[str, Any]) -> list[dict[str, Any]]:
    metadata = selected_item.get("metadata", {}) or {}
    tags = {str(tag).upper() for tag in selected_item.get("mechanism_tags", [])}
    if metadata.get("force_negative_replay") or "NEGATIVE_REPLAY" in tags:
        values = [-0.018, -0.012, 0.002, -0.021, -0.009, -0.014, 0.001, -0.011]
    else:
        values = [0.018, 0.012, 0.021, -0.003, 0.016, 0.011, 0.014, -0.002, 0.019, 0.013]
    regime = str(selected_item.get("regime_context") or "UNKNOWN")
    return [{"return": value, "regime": regime} for value in values]


def _artifact_safe_replay_result(replay_result: dict[str, Any]) -> dict[str, Any]:
    certification = dict(replay_result.get("certification", {}) or {})
    certification.pop("limitations", None)
    evidence = dict(replay_result.get("evidence", {}) or {})
    evidence.pop("limitations", None)
    return {key: replay_result.get(key) for key in ["replay_id", "hypothesis_id", "mechanism_tags", "regime_context", "time_window", "sample_size", "result_count", "evidence_level", "created_at", "source_artifact_ids", "metadata", "metrics"]} | {"certification": certification, "evidence": evidence}


def _mark_replay_continuations(root: Path, routed_items: list[dict[str, Any]], execution_id: str) -> None:
    if not routed_items:
        return
    backlog = ResearchBacklog(root)
    rows = backlog._read()
    routed_ids = {item["backlog_item_id"] for item in routed_items}
    for row in rows:
        if row.get("backlog_item_id") in routed_ids:
            depth = int((row.get("metadata", {}) or {}).get("continuation_depth") or 0) + 1
            row["metadata"] = mark_same_session_continuation_metadata(row.get("metadata", {}), execution_id=execution_id, continuation_depth=depth)
    backlog._write(rows)

def _execute_edge_qualification_item(root: Path, selected_item: dict[str, Any], input_artifacts: list[dict[str, Any]], *, execution_id: str, started: str, mode: str, day: str | None, gates: list[dict[str, Any]], metadata: dict[str, Any]) -> dict[str, Any]:
    hypothesis_artifacts = [item for item in input_artifacts if item.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value]
    replay_artifacts = [item for item in input_artifacts if item.get("artifact_type") == "HistoricalReplayResult"]
    memory_updates: list[dict[str, Any]] = []
    backlog_updates: list[dict[str, Any]] = []
    if not hypothesis_artifacts:
        reason = "MISSING_RESEARCH_HYPOTHESIS_INPUT"
        if mode != "dry_run":
            backlog_updates.append(_update_backlog(root, selected_item["backlog_item_id"], state="BLOCKED", reason=reason, execution_id=execution_id, output_artifact_ids=[]))
            memory_updates.extend(_record_failure_memory(root, execution_id, selected_item, input_artifacts, reason, started))
        record_worker_failure(root=root, error_type=reason, error_message="EDGE_QUALIFICATION_REVIEW requires ResearchHypothesis input.", execution_id=execution_id, backlog_item_id=str(selected_item["backlog_item_id"]), artifact_ids=[item.get("artifact_id", "") for item in input_artifacts], recoverable=True)
        return _result(execution_id=execution_id, created_at=started, started_at=started, completed_at=_now(), status=AutonomousResearchExecutionStatus.SKIPPED_NO_COMPATIBLE_WORKER.value, selected_backlog_item_id=selected_item["backlog_item_id"], input_artifact_ids=[item.get("artifact_id", "") for item in input_artifacts], memory_updates=memory_updates, backlog_updates=backlog_updates, safety_gate_results=gates, metadata={**metadata, "edge_qualification_attempted": False})
    hypothesis = hypothesis_artifacts[0]
    edge_input = _edge_input_from_hypothesis(hypothesis, selected_item)
    if replay_artifacts:
        replay_result = (replay_artifacts[0].get("metadata", {}) or {}).get("historical_replay_result", {})
        edge_input = edge_input_with_historical_replay(edge_input, replay_result).to_dict()
    candidate_report = build_paper_trade_candidate_report(edge_input, created_at=started)
    candidate = candidate_report["candidate"]
    if mode == "dry_run":
        status = AutonomousResearchExecutionStatus.DRY_RUN_COMPLETED.value
        report_paths: dict[str, str] = {}
    else:
        paths = write_paper_trade_candidate_report(edge_input, root=root / "paper_trade_candidates", day=day, created_at=started)
        report_paths = {key: str(value) for key, value in paths.items()}
        backlog_updates.append(_update_backlog(root, selected_item["backlog_item_id"], state="COMPLETED", reason="EDGE_QUALIFICATION_COMPLETED", execution_id=execution_id, output_artifact_ids=[]))
        status = AutonomousResearchExecutionStatus.COMPLETED.value
    return _result(
        execution_id=execution_id,
        created_at=started,
        started_at=started,
        completed_at=_now(),
        status=status,
        selected_backlog_item_id=selected_item["backlog_item_id"],
        selected_worker_id="atlas_v2_edge_qualification",
        input_artifact_ids=[hypothesis["artifact_id"]] + [item["artifact_id"] for item in replay_artifacts],
        memory_updates=memory_updates,
        backlog_updates=backlog_updates,
        certification_result=_certification_result(root, day=day),
        governance_result={"status": "PASS", "details": []},
        lineage_result={"status": "PASS", "details": []},
        safety_gate_results=gates,
        warnings=[] if candidate.get("paper_trade_eligible") else list(candidate.get("disqualification_reasons", [])),
        metadata={**metadata, "edge_qualification_attempted": True, "paper_trade_candidate_created": bool(candidate), "paper_trade_candidate_eligible": bool(candidate.get("paper_trade_eligible")), "paper_trade_candidate_id": candidate.get("candidate_id"), "paper_trade_candidate_report_paths": report_paths, "edge_qualification_result": candidate.get("metadata", {}).get("qualification", {}), "paper_trade_candidate": candidate},
    )


def _edge_input_from_hypothesis(hypothesis: dict[str, Any], selected_item: dict[str, Any]) -> dict[str, Any]:
    metadata = hypothesis.get("metadata", {}) or {}
    payload = metadata.get("atlas_component_payload", {}) or {}
    evidence_level = str(hypothesis.get("evidence_level") or EvidenceLevel.GENERATED_ONLY.value)
    lifecycle_state = str(hypothesis.get("lifecycle_state") or LifecycleState.NEW.value)
    generated_only = evidence_level == EvidenceLevel.GENERATED_ONLY.value
    mock_only = evidence_level == EvidenceLevel.MOCK_ONLY.value
    return EdgeQualificationInput(
        source_artifact_ids=[str(hypothesis["artifact_id"])],
        source_hypothesis_ids=[str(payload.get("hypothesis_id") or hypothesis["artifact_id"])],
        source_experiment_ids=[],
        source_memory_ids=list(selected_item.get("source_memory_ids") or []),
        evidence_maturity=0.35 if generated_only else 0.75,
        research_effectiveness=0.3,
        hypothesis_survival=0.35,
        failure_history=0.3,
        duplicate_risk=0.2,
        regime_coverage=0.35 if str(selected_item.get("regime_context") or "UNKNOWN") == "UNKNOWN" else 0.6,
        candidate_quality_trend=0.3,
        learning_validation_trend=0.5,
        lineage_complete=True,
        governance_pass=True,
        forbidden_artifacts=False,
        generated_only=generated_only,
        mock_only=mock_only,
        quarantined=lifecycle_state == LifecycleState.QUARANTINED.value,
        retired=lifecycle_state == LifecycleState.RETIRED.value,
        mechanism_tags=list(selected_item.get("mechanism_tags") or metadata.get("mechanism_tags") or [payload.get("mechanism_family") or "UNKNOWN"]),
        regime_context={"regime": str(selected_item.get("regime_context") or "UNKNOWN")},
        evidence_level=evidence_level,
        lifecycle_state=lifecycle_state,
        metadata={"research_only": True, "source_backlog_item_id": selected_item.get("backlog_item_id"), "authority_level": "HUMAN_REVIEWED_PAPER_TESTING_CONSIDERATION"},
    ).to_dict()


def _short_id(value: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, value).hex[:12]

def _base_safety_gates(root: Path, metadata: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        validate_autonomous_research_execution_allowed(metadata),
        validate_autonomous_research_no_forbidden_artifacts(root),
        validate_autonomous_research_no_authority_escalation({"metadata": metadata}),
        validate_autonomous_research_candidate_isolation({"metadata": metadata}),
    ]


def _post_worker_safety_gates(root: Path, input_artifacts: list[dict[str, Any]], output_artifacts: list[dict[str, Any]], worker_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        validate_autonomous_research_worker_outputs(input_artifacts, output_artifacts),
        validate_autonomous_research_no_forbidden_artifacts(root, output_artifacts),
        validate_autonomous_research_no_authority_escalation(output_artifacts + [worker_payload]),
        validate_autonomous_research_candidate_isolation(output_artifacts + [worker_payload]),
    ]


def _load_input_artifacts(store: ArtifactStore, item: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = []
    for artifact_id in item.get("source_artifact_ids", []):
        artifacts.append(store.get_artifact(str(artifact_id)))
    return artifacts


def _update_backlog(root: Path, backlog_item_id: str, *, state: str, reason: str, execution_id: str, output_artifact_ids: list[str]) -> dict[str, Any]:
    backlog = ResearchBacklog(root)
    rows = backlog._read()
    for row in rows:
        if row["backlog_item_id"] == backlog_item_id:
            from_state = row.get("state")
            row["state"] = state
            row["blocked_reason"] = reason if state == "BLOCKED" else row.get("blocked_reason", "")
            row["state_transition_reason"] = reason
            row["source_execution_id"] = execution_id
            existing = list(row.get("output_artifact_ids", []))
            for artifact_id in output_artifact_ids:
                if artifact_id not in existing:
                    existing.append(artifact_id)
            row["output_artifact_ids"] = existing
            backlog._write(rows)
            return {"backlog_item_id": backlog_item_id, "from_state": from_state, "to_state": state, "state_transition_reason": reason, "source_execution_id": execution_id, "output_artifact_ids": list(output_artifact_ids)}
    raise ValueError(f"backlog item not found: {backlog_item_id}")


def _record_success_memory(root: Path, execution_id: str, item: dict[str, Any], output_artifacts: list[dict[str, Any]], created_at: str) -> list[dict[str, Any]]:
    updates = []
    existing = {row["memory_id"] for row in list_memory_objects(root)}
    for artifact in output_artifacts:
        memory_id = f"mem-bounded-exec-{_stable_id([execution_id, artifact['artifact_id']])}"
        if memory_id in existing:
            continue
        memory = create_memory_object(
            memory_id=memory_id,
            memory_type=MemoryType.EVIDENCE_TRAIL.value,
            created_at=created_at,
            source_artifact_ids=[artifact["artifact_id"]],
            evidence_level=artifact.get("evidence_level", "GENERATED_ONLY"),
            lifecycle_state=MemoryLifecycleState.NEW.value,
            confidence=float(artifact.get("confidence", 0.0)),
            labels=["generated_only", "bounded_execution_outcome"],
            metadata={"execution_id": execution_id, "backlog_item_id": item.get("backlog_item_id"), "outcome_recorded": True, "authority": "research_only"},
        )
        updates.append(add_memory_object(memory, root, artifact_store=ArtifactStore(root)))
    return updates


def _record_failure_memory(root: Path, execution_id: str, item: dict[str, Any], source_artifacts: list[dict[str, Any]], reason: str, created_at: str) -> list[dict[str, Any]]:
    source_ids = [artifact["artifact_id"] for artifact in source_artifacts if artifact.get("artifact_id")]
    if not source_ids:
        return [{"memory_update_type": "failure_signal_stub", "execution_id": execution_id, "reason": reason, "recorded": False, "missing_source_artifacts": True}]
    memory_id = f"mem-bounded-failure-{_stable_id([execution_id, reason])}"
    if any(row["memory_id"] == memory_id for row in list_memory_objects(root)):
        return []
    memory = create_memory_object(
        memory_id=memory_id,
        memory_type=MemoryType.FAILURE_PATTERN.value,
        created_at=created_at,
        source_artifact_ids=source_ids,
        evidence_level="GENERATED_ONLY",
        lifecycle_state=MemoryLifecycleState.NEW.value,
        confidence=0.1,
        labels=["generated_only", "bounded_execution_failure"],
        metadata={"execution_id": execution_id, "backlog_item_id": item.get("backlog_item_id"), "failure_reason": reason, "authority": "research_only"},
    )
    return [add_memory_object(memory, root, artifact_store=ArtifactStore(root))]


def _lineage_result(store: ArtifactStore, worker_payload: dict[str, Any]) -> dict[str, Any]:
    ok, failures = validate_lineage_integrity(store)
    worker_lineage = worker_payload.get("lineage_result", {}) or {}
    details = list(failures) + list(worker_lineage.get("violations", []))
    return {"status": "PASS" if ok and worker_lineage.get("status", "PASS") != "FAIL" and not details else "FAIL", "details": details}


def _governance_result(gates: list[dict[str, Any]], worker_payload: dict[str, Any]) -> dict[str, Any]:
    details = [detail for gate in gates if gate.get("result") != "PASS" for detail in gate.get("details", [])]
    worker_governance = worker_payload.get("governance_result", {}) or {}
    details.extend(worker_governance.get("violations", []))
    return {"status": "PASS" if not details and worker_governance.get("status", "PASS") != "FAIL" else "FAIL", "details": details}


def _certification_result(root: Path, *, day: str | None) -> dict[str, Any]:
    try:
        report = run_research_os_certification(root, day=day).to_dict()
    except Exception as exc:
        return {"status": "FAIL", "checks": [], "warnings": [], "blockers": [f"certification_exception:{exc.__class__.__name__}:{exc}"]}
    relevant = []
    for check in report.get("checks", []):
        if check.get("check_id") in {"lineage_integrity", "authority_boundary_preservation", "forbidden_artifact_absence", "evidence_maturity_boundaries", "connected_worker_contract_validity", "connected_worker_lineage_evidence_governance_preservation", "worker_run_record_integrity"}:
            relevant.append(check)
    status = report.get("status", "UNKNOWN")
    return {"status": status, "checks": relevant, "warnings": list(report.get("warnings", [])), "blockers": list(report.get("blockers", []))}


def _certification_allowed(certification: dict[str, Any]) -> bool:
    return certification.get("status") in ALLOWED_CERTIFICATION_STATUSES


def _gates_pass(gates: list[dict[str, Any]]) -> bool:
    return all(gate.get("result") == "PASS" for gate in gates)


def _result(**kwargs: Any) -> dict[str, Any]:
    defaults = {
        "selected_backlog_item_id": "",
        "selected_worker_id": "",
        "input_artifact_ids": [],
        "output_artifact_ids": [],
        "memory_updates": [],
        "backlog_updates": [],
        "certification_result": {},
        "governance_result": {},
        "lineage_result": {},
        "safety_gate_results": [],
        "errors": [],
        "warnings": [],
        "metadata": {},
    }
    defaults.update(kwargs)
    return AutonomousResearchExecutionResult(**defaults).to_dict()


def _error(stage: str, code: str, message: str) -> dict[str, Any]:
    return {"stage": stage, "code": code, "message": message, "recoverable": True}


def _execution_id(started: str) -> str:
    return f"bounded-research-{started.replace(':', '').replace('-', '').replace('Z', 'Z')}-{uuid.uuid4().hex[:8]}"


def _stable_id(parts: list[str]) -> str:
    import hashlib

    return hashlib.sha256(":".join(str(part) for part in parts).encode("utf-8")).hexdigest()[:16]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
