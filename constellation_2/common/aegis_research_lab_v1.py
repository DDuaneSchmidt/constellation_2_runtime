from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATHS = {
    "research_inbox_item": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_inbox_item.v1.schema.json",
    "research_hypothesis": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_hypothesis.v1.schema.json",
    "research_program": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_program.v1.schema.json",
    "research_evidence_packet": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_evidence_packet.v1.schema.json",
    "research_conclusion": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_conclusion.v1.schema.json",
    "research_failure_archetype": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_failure_archetype.v1.schema.json",
    "research_to_lite_promotion": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_to_lite_promotion.v1.schema.json",
    "research_architecture_integrity_review": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_architecture_integrity_review.v1.schema.json",
    "research_lab_index": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_lab_index.v1.schema.json",
    "edge_taxonomy": "governance/04_DATA/SCHEMAS/C2/REPORTS/edge_taxonomy.v1.schema.json",
    "hypothesis_registry": "governance/04_DATA/SCHEMAS/C2/REPORTS/hypothesis_registry.v1.schema.json",
    "research_task_queue": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_task_queue.v1.schema.json",
    "research_result_ledger": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_result_ledger.v1.schema.json",
    "research_knowledge_graph": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_knowledge_graph.v1.schema.json",
    "experiment_result": "governance/04_DATA/SCHEMAS/C2/REPORTS/experiment_result.v1.schema.json",
    "research_experiment_result": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_experiment_result.v1.schema.json",
    "hypothesis_test_plan": "governance/04_DATA/SCHEMAS/C2/REPORTS/hypothesis_test_plan.v1.schema.json",
    "hypothesis_progress_report": "governance/04_DATA/SCHEMAS/C2/REPORTS/hypothesis_progress_report.v1.schema.json",
    "research_lab_awareness_report": "governance/04_DATA/SCHEMAS/C2/REPORTS/research_lab_awareness_report.v1.schema.json",
    "promotion_review": "governance/04_DATA/SCHEMAS/C2/REPORTS/promotion_review.v1.schema.json",
    "promoted_sleeve_library": "governance/04_DATA/SCHEMAS/C2/REPORTS/promoted_sleeve_library.v1.schema.json",
    "manual_trade_packet": "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_trade_packet.v1.schema.json",
    "manual_execution_receipt": "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_execution_receipt.v1.schema.json",
    "outcome_ledger": "governance/04_DATA/SCHEMAS/C2/REPORTS/outcome_ledger.v1.schema.json",
}
RESEARCH_INBOX_SOURCES = {"CHATGPT", "MANUAL", "LITE_FEEDBACK", "MARKET_OBSERVATION", "TRADE_REVIEW"}
RESEARCH_INBOX_TRIAGE_STATUSES = {"NEW", "TRIAGED", "CONVERTED_TO_HYPOTHESIS", "REJECTED", "ARCHIVED"}
RESEARCH_PROGRAM_STATUSES = {"ACTIVE", "PAUSED", "COMPLETE", "ARCHIVED"}
RESEARCH_CONCLUSION_STATUSES = {"ACTIVE", "SUPERSEDED", "CONTRADICTED", "ARCHIVED"}
RESEARCH_FAILURE_ARCHETYPE_STATUSES = {"ACTIVE", "SUPERSEDED", "ARCHIVED"}
RESEARCH_HYPOTHESIS_STATUSES = {
    "IDEA",
    "UNDER_INVESTIGATION",
    "BACKTESTING",
    "REPLAY_REVIEW",
    "OBSERVED_IN_MARKET",
    "VALIDATED_RESEARCH",
    "PROMOTION_CANDIDATE",
    "APPROVED_FOR_LITE",
    "REJECTED",
    "ARCHIVED",
}
RESEARCH_HYPOTHESIS_SOURCES = {"CHATGPT_SEED", "MANUAL", "RESEARCH_LAB"}
RESEARCH_TASK_TYPES_V1 = {
    "DEFINITION_CHECK",
    "DATA_AVAILABILITY_CHECK",
    "REPLAY_ANALYSIS",
    "BACKTEST",
    "FORWARD_OBSERVATION",
    "FAILURE_MODE_REVIEW",
    "EXPECTANCY_REVIEW",
    "PROMOTION_REVIEW",
}
RESEARCH_TASK_STATUSES_V1 = {"QUEUED", "RUNNING", "COMPLETED", "BLOCKED", "REJECTED"}
RESEARCH_RESULT_STATUSES_V1 = {
    "INSUFFICIENT_DATA",
    "SUPPORTS_HYPOTHESIS",
    "WEAK_SUPPORT",
    "CONTRADICTS_HYPOTHESIS",
    "INVALIDATED",
    "NEEDS_MORE_RESEARCH",
}
RESEARCH_PROMOTION_RECOMMENDATIONS_V1 = {"NONE", "CONTINUE_RESEARCH", "PROMOTION_CANDIDATE", "REJECT", "ARCHIVE"}
RESEARCH_PRIORITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "NORMAL": 2, "LOW": 3}
RESEARCH_METHODOLOGY_VERSION_V1 = "aegis_research_offline_executor.v1"
RESEARCH_CODE_VERSION_UNKNOWN = "repo_commit_unavailable"
HYPOTHESIS_LIFECYCLE_STATES = {
    "proposed",
    "definition_ready",
    "duplicate_checked",
    "exploratory_testing",
    "regime_testing",
    "robustness_testing",
    "friction_testing",
    "out_of_sample_testing",
    "failure_mode_review",
    "edge_overlap_review",
    "validated_candidate",
    "promotion_review",
    "promoted",
    "rejected",
    "insufficient_data",
    "regime_dependent",
    "duplicate_of_existing_edge",
    "retired",
}
PROMOTION_STATES_LC = {"not_ready", "candidate", "approved_for_lite_review", "approved_for_lite_implementation", "rejected", "deferred"}
TASK_TYPES = {
    "definition_check",
    "duplicate_review",
    "exploratory_test",
    "regime_test",
    "robustness_test",
    "friction_test",
    "out_of_sample_test",
    "failure_review",
    "edge_overlap_review",
    "promotion_review",
    "retest",
    "sleeve_failure_review",
    "anomaly_review",
    "retirement_review",
    "event_failure_review",
    "event_success_review",
    "event_stale_entry_review",
    "event_false_positive_review",
    "event_overlap_review",
}
TASK_STATUSES = {"open", "completed", "blocked", "failed"}
RESULT_STATUSES = {
    "insufficient_data",
    "weak",
    "promising",
    "regime_dependent",
    "overlapping",
    "duplicate",
    "rejected",
    "validated_candidate",
}
TEST_STAGES = [
    "definition_check",
    "duplicate_overlap_check",
    "exploratory_backtest",
    "regime_segmentation",
    "robustness_check",
    "transaction_friction_check",
    "out_of_sample_check",
    "failure_mode_review",
    "edge_overlap_review",
    "promotion_review",
]
TERMINAL_TEST_STATES = {
    "rejected",
    "insufficient_data",
    "regime_dependent",
    "validated_candidate",
    "duplicate_of_existing_edge",
    "promoted",
    "retired",
}
STAGE_TASK_TYPES = {
    "definition_check": "definition_check",
    "duplicate_overlap_check": "duplicate_review",
    "exploratory_backtest": "exploratory_test",
    "regime_segmentation": "regime_test",
    "robustness_check": "robustness_test",
    "transaction_friction_check": "friction_test",
    "out_of_sample_check": "out_of_sample_test",
    "failure_mode_review": "failure_review",
    "edge_overlap_review": "edge_overlap_review",
    "promotion_review": "promotion_review",
}
STAGE_LIFECYCLE_STATES = {
    "definition_check": "definition_ready",
    "duplicate_overlap_check": "duplicate_checked",
    "exploratory_backtest": "exploratory_testing",
    "regime_segmentation": "regime_testing",
    "robustness_check": "robustness_testing",
    "transaction_friction_check": "friction_testing",
    "out_of_sample_check": "out_of_sample_testing",
    "failure_mode_review": "failure_mode_review",
    "edge_overlap_review": "edge_overlap_review",
    "promotion_review": "promotion_review",
}
RESEARCH_TYPES = {
    "SLEEVE",
    "EDGE",
    "REGIME",
    "GOVERNANCE",
    "OVERLAP",
    "STOP_LOGIC",
    "RISK_SIZING",
    "BEHAVIORAL_STATE",
}
RESEARCH_STATUSES = {"DRAFT", "UNDER_REVIEW", "VALIDATED_RESEARCH", "REJECTED", "ARCHIVED"}
PROMOTION_COMPONENT_TYPES = {
    "SLEEVE",
    "EDGE_CLUSTER_RULE",
    "REGIME_SIGNAL",
    "GOVERNANCE_RULE",
    "STOP_LOGIC",
    "RISK_RULE",
    "REPORT_FIELD",
}
PROMOTION_STATUSES = {
    "NOT_READY",
    "CANDIDATE",
    "APPROVED_FOR_LITE_REVIEW",
    "APPROVED_FOR_LITE_IMPLEMENTATION",
    "REJECTED",
    "DEFERRED",
}


def research_artifact_path_v1(*, truth_root: Path, artifact_id: str, day_utc: str, research_id: str) -> Path:
    filename = f"{artifact_id.replace('_v1', '')}.v1.json"
    return Path(truth_root).resolve() / "research_lab" / artifact_id / day_utc / _safe_id(research_id) / filename


def validate_research_lab_artifact_v1(payload: dict[str, Any]) -> None:
    schema_id = str(payload.get("schema_id") or "")
    relpath = SCHEMA_RELPATHS.get(schema_id)
    if not relpath:
        raise ValueError(f"UNSUPPORTED_AEGIS_RESEARCH_LAB_SCHEMA:{schema_id}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, relpath)


def write_research_lab_artifact_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    validate_research_lab_artifact_v1(payload)
    schema_id = str(payload.get("schema_id") or "")
    identifier = str(
        payload.get("inbox_id")
        or payload.get("program_id")
        or payload.get("conclusion_id")
        or payload.get("failure_archetype_id")
        or payload.get("review_id")
        or payload.get("research_id")
        or payload.get("promotion_id")
        or payload.get("taxonomy_id")
        or payload.get("hypothesis_id")
        or payload.get("experiment_id")
        or payload.get("packet_id")
        or payload.get("receipt_id")
        or ("awareness" if schema_id == "research_lab_awareness_report" else "")
        or "index"
    )
    path = research_artifact_path_v1(
        truth_root=truth_root,
        artifact_id=str(payload["artifact_id"]),
        day_utc=day_utc,
        research_id=identifier,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_hypothesis_record_v1(
    *,
    hypothesis_id: str,
    title: str,
    behavioral_thesis: str,
    edge_family: str,
    market_regime: str,
    trigger_conditions: str,
    expected_outcome: str,
    failure_modes: str | list[str],
    instrument_universe: list[str] | str | None = None,
    time_horizon: str = "",
    related_hypotheses: list[str] | None = None,
    overlap_tags: list[str] | None = None,
    source_type: str = "manual",
    source_reference: str = "",
    priority: str = "normal",
    lifecycle_state: str = "proposed",
    created_at: str = "",
    updated_at: str = "",
    last_tested_at: str = "",
    test_count: int = 0,
    latest_result_status: str = "",
    promotion_status: str = "not_ready",
    notes: str = "",
) -> dict[str, Any]:
    return {
        "hypothesis_id": hypothesis_id,
        "title": title,
        "behavioral_thesis": behavioral_thesis,
        "edge_family": edge_family,
        "market_regime": market_regime,
        "trigger_conditions": trigger_conditions,
        "expected_outcome": expected_outcome,
        "failure_modes": _strings(failure_modes),
        "instrument_universe": _strings(instrument_universe),
        "time_horizon": time_horizon,
        "related_hypotheses": _strings(related_hypotheses),
        "overlap_tags": _strings(overlap_tags),
        "source_type": source_type,
        "source_reference": source_reference,
        "priority": priority,
        "lifecycle_state": _enum_lc(lifecycle_state, HYPOTHESIS_LIFECYCLE_STATES, "lifecycle_state"),
        "created_at": created_at,
        "updated_at": updated_at or created_at,
        "last_tested_at": last_tested_at,
        "test_count": int(test_count),
        "latest_result_status": latest_result_status,
        "promotion_status": _enum_lc(promotion_status, PROMOTION_STATES_LC, "promotion_status"),
        "notes": notes,
    }


def build_hypothesis_registry_v1(*, generated_at_utc: str, hypotheses: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_id": "hypothesis_registry",
        "schema_version": "v1",
        "artifact_id": "hypothesis_registry_v1",
        "generated_at_utc": generated_at_utc,
        "hypotheses": hypotheses,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "automatic_trade_generation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def deterministic_research_id_v1(*parts: Any, prefix: str) -> str:
    material = "|".join(str(part or "").strip() for part in parts)
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
    return f"{_safe_id(prefix)}-{digest}"


def build_research_inbox_item_v1(
    *,
    inbox_id: str,
    created_at_utc: str,
    source: str,
    raw_text: str,
    tags: list[str] | str | None = None,
    related_symbols: list[str] | str | None = None,
    related_edge_family: str = "",
    suggested_program: str = "",
    triage_status: str = "NEW",
    converted_hypothesis_id: str = "",
    notes: str = "",
) -> dict[str, Any]:
    status = _enum(triage_status, RESEARCH_INBOX_TRIAGE_STATUSES, "triage_status")
    if status == "CONVERTED_TO_HYPOTHESIS" and not str(converted_hypothesis_id or "").strip():
        raise ValueError("CONVERTED_INBOX_REQUIRES_CONVERTED_HYPOTHESIS_ID")
    payload = {
        "schema_id": "research_inbox_item",
        "schema_version": "v1",
        "artifact_id": "research_inbox_item_v1",
        "inbox_id": inbox_id,
        "created_at_utc": created_at_utc,
        "source": _enum(source, RESEARCH_INBOX_SOURCES, "source"),
        "raw_text": raw_text,
        "tags": _strings(tags),
        "related_symbols": _strings(related_symbols),
        "related_edge_family": related_edge_family,
        "suggested_program": suggested_program,
        "triage_status": status,
        "converted_hypothesis_id": converted_hypothesis_id,
        "notes": notes,
        "research_lab_only": True,
        "can_authorize_research_execution": False,
        "runtime_mutation_allowed": False,
        "trade_authorization_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def convert_inbox_item_to_research_hypothesis_v1(
    *,
    inbox_item: dict[str, Any],
    hypothesis_id: str,
    created_at_utc: str,
    title: str,
    hypothesis_summary: str,
    market_thesis: str = "",
    edge_family: str = "",
    behavioral_state: str = "",
    expected_regime: str = "",
    expected_direction: str = "",
    expected_holding_period: str = "",
    instruments: list[str] | str | None = None,
    rationale: str = "",
    expected_behavior: str = "",
    failure_conditions: list[str] | str | None = None,
    invalidation_conditions: list[str] | str | None = None,
    related_sleeves: list[str] | str | None = None,
    related_research_refs: list[str] | str | None = None,
    confidence_level: str = "LOW",
) -> dict[str, Any]:
    if str(inbox_item.get("schema_id") or "") != "research_inbox_item":
        raise ValueError("INBOX_CONVERSION_REQUIRES_RESEARCH_INBOX_ITEM")
    hypothesis = build_research_hypothesis_v1(
        hypothesis_id=hypothesis_id,
        created_at_utc=created_at_utc,
        title=title,
        hypothesis_summary=hypothesis_summary,
        market_thesis=market_thesis,
        edge_family=edge_family or str(inbox_item.get("related_edge_family") or ""),
        behavioral_state=behavioral_state,
        expected_regime=expected_regime,
        expected_direction=expected_direction,
        expected_holding_period=expected_holding_period,
        instruments=instruments or inbox_item.get("related_symbols"),
        rationale=rationale,
        expected_behavior=expected_behavior,
        failure_conditions=failure_conditions,
        invalidation_conditions=invalidation_conditions,
        related_sleeves=related_sleeves,
        related_research_refs=[*_strings(related_research_refs), f"research_inbox_item.v1:{inbox_item.get('inbox_id')}"],
        confidence_level=confidence_level,
        status="IDEA",
        source="MANUAL" if str(inbox_item.get("source") or "") != "CHATGPT" else "CHATGPT_SEED",
        notes=f"Explicitly converted from research_inbox_item.v1:{inbox_item.get('inbox_id')}",
    )
    converted = dict(inbox_item)
    converted["triage_status"] = "CONVERTED_TO_HYPOTHESIS"
    converted["converted_hypothesis_id"] = hypothesis_id
    converted["canonical_json_hash"] = None
    converted["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(converted)
    return {"inbox_item": converted, "hypothesis": hypothesis}


def build_research_hypothesis_v1(
    *,
    hypothesis_id: str,
    created_at_utc: str,
    title: str,
    hypothesis_summary: str,
    market_thesis: str = "",
    edge_family: str = "",
    behavioral_state: str = "",
    expected_regime: str = "",
    expected_direction: str = "",
    expected_holding_period: str = "",
    instruments: list[str] | str | None = None,
    rationale: str = "",
    expected_behavior: str = "",
    failure_conditions: list[str] | str | None = None,
    invalidation_conditions: list[str] | str | None = None,
    related_sleeves: list[str] | str | None = None,
    related_research_refs: list[str] | str | None = None,
    confidence_level: str = "LOW",
    status: str = "IDEA",
    source: str = "MANUAL",
    notes: str = "",
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_hypothesis",
        "schema_version": "v1",
        "artifact_id": "research_hypothesis_v1",
        "hypothesis_id": hypothesis_id,
        "created_at_utc": created_at_utc,
        "title": title,
        "hypothesis_summary": hypothesis_summary,
        "market_thesis": market_thesis,
        "edge_family": edge_family,
        "behavioral_state": behavioral_state,
        "expected_regime": expected_regime,
        "expected_direction": expected_direction,
        "expected_holding_period": expected_holding_period,
        "instruments": _strings(instruments),
        "rationale": rationale,
        "expected_behavior": expected_behavior,
        "failure_conditions": _strings(failure_conditions),
        "invalidation_conditions": _strings(invalidation_conditions),
        "related_sleeves": _strings(related_sleeves),
        "related_research_refs": _strings(related_research_refs),
        "confidence_level": confidence_level,
        "status": _enum(status, RESEARCH_HYPOTHESIS_STATUSES, "status"),
        "source": _enum(source, RESEARCH_HYPOTHESIS_SOURCES, "source"),
        "notes": notes,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "trade_authorization_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_program_v1(
    *,
    program_id: str,
    title: str,
    thesis: str,
    status: str = "ACTIVE",
    related_hypotheses: list[str] | str | None = None,
    edge_families: list[str] | str | None = None,
    regimes: list[str] | str | None = None,
    instruments: list[str] | str | None = None,
    current_open_questions: list[str] | str | None = None,
    summary_findings: list[str] | str | None = None,
    research_priority: str = "NORMAL",
    owner_notes: str = "",
    created_at_utc: str,
    updated_at_utc: str = "",
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_program",
        "schema_version": "v1",
        "artifact_id": "research_program_v1",
        "program_id": program_id,
        "title": title,
        "thesis": thesis,
        "status": _enum(status, RESEARCH_PROGRAM_STATUSES, "status"),
        "related_hypotheses": _strings(related_hypotheses),
        "edge_families": _strings(edge_families),
        "regimes": _strings(regimes),
        "instruments": _strings(instruments),
        "current_open_questions": _strings(current_open_questions),
        "summary_findings": _strings(summary_findings),
        "research_priority": research_priority,
        "owner_notes": owner_notes,
        "created_at_utc": created_at_utc,
        "updated_at_utc": updated_at_utc or created_at_utc,
        "research_lab_only": True,
        "can_authorize_promotion": False,
        "runtime_mutation_allowed": False,
        "trade_authorization_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_queue_task_v1(
    *,
    task_id: str,
    hypothesis_id: str,
    task_type: str,
    priority: str = "NORMAL",
    status: str = "QUEUED",
    required_inputs: list[str] | str | None = None,
    output_artifact_refs: list[str] | str | None = None,
    blocker_reason_codes: list[str] | str | None = None,
    created_at_utc: str,
    updated_at_utc: str = "",
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "hypothesis_id": hypothesis_id,
        "task_type": _enum(task_type, RESEARCH_TASK_TYPES_V1, "task_type"),
        "priority": priority,
        "status": _enum(status, RESEARCH_TASK_STATUSES_V1, "status"),
        "required_inputs": _strings(required_inputs),
        "output_artifact_refs": _strings(output_artifact_refs),
        "blocker_reason_codes": _strings(blocker_reason_codes),
        "created_at_utc": created_at_utc,
        "updated_at_utc": updated_at_utc or created_at_utc,
    }


def build_research_result_v1(
    *,
    result_id: str,
    hypothesis_id: str,
    task_id: str,
    result_status: str,
    evidence_refs: list[str] | str | None = None,
    conclusion_summary: str = "",
    confidence_before: str = "",
    confidence_after: str = "",
    confidence_change: str = "",
    metrics_summary: dict[str, Any] | None = None,
    failure_mode_notes: str = "",
    invalidation_notes: str = "",
    next_recommended_task: str = "",
    promotion_recommendation: str = "NONE",
    created_at_utc: str = "",
    data_snapshot_refs: list[str] | str | None = None,
    methodology_version: str = RESEARCH_METHODOLOGY_VERSION_V1,
    code_version: str = RESEARCH_CODE_VERSION_UNKNOWN,
    artifact_lineage: list[dict[str, Any]] | None = None,
    reason_codes: list[str] | str | None = None,
    reproducibility_notes: str = "",
) -> dict[str, Any]:
    return {
        "result_id": result_id,
        "hypothesis_id": hypothesis_id,
        "task_id": task_id,
        "result_status": _enum(result_status, RESEARCH_RESULT_STATUSES_V1, "result_status"),
        "evidence_refs": _strings(evidence_refs),
        "conclusion_summary": conclusion_summary,
        "confidence_before": confidence_before,
        "confidence_after": confidence_after,
        "confidence_change": confidence_change,
        "metrics_summary": dict(metrics_summary or {}),
        "failure_mode_notes": failure_mode_notes,
        "invalidation_notes": invalidation_notes,
        "next_recommended_task": next_recommended_task,
        "promotion_recommendation": _enum(promotion_recommendation, RESEARCH_PROMOTION_RECOMMENDATIONS_V1, "promotion_recommendation"),
        "created_at_utc": created_at_utc,
        "data_snapshot_refs": _strings(data_snapshot_refs),
        "methodology_version": methodology_version,
        "code_version": code_version,
        "artifact_lineage": _objects(artifact_lineage or []),
        "reason_codes": _strings(reason_codes),
        "reproducibility_notes": reproducibility_notes,
        "immutable_artifact": True,
    }


def build_research_result_ledger_v1(*, generated_at_utc: str, results: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_results = sorted(
        results,
        key=lambda row: (
            str(row.get("created_at_utc") or ""),
            str(row.get("hypothesis_id") or ""),
            str(row.get("task_id") or ""),
            str(row.get("result_id") or ""),
        ),
    )
    payload = {
        "schema_id": "research_result_ledger",
        "schema_version": "v1",
        "artifact_id": "research_result_ledger_v1",
        "generated_at_utc": generated_at_utc,
        "results": sorted_results,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "trade_authorization_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def validate_research_conclusion_lineage_v1(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_id") or "") != "research_conclusion":
        raise ValueError("CONCLUSION_LINEAGE_REQUIRES_RESEARCH_CONCLUSION")
    if not _strings(payload.get("supporting_evidence_refs")):
        raise ValueError("RESEARCH_CONCLUSION_REQUIRES_EVIDENCE_REFS")
    if not _strings(payload.get("result_ledger_refs")):
        raise ValueError("RESEARCH_CONCLUSION_REQUIRES_RESULT_LEDGER_REFS")
    if not _strings(payload.get("reason_codes")):
        raise ValueError("RESEARCH_CONCLUSION_REQUIRES_REASON_CODES")
    if not str(payload.get("methodology_version") or "").strip():
        raise ValueError("RESEARCH_CONCLUSION_REQUIRES_METHODOLOGY_VERSION")
    if not str(payload.get("code_version") or "").strip():
        raise ValueError("RESEARCH_CONCLUSION_REQUIRES_CODE_VERSION")
    if not str(payload.get("reproducibility_notes") or "").strip():
        raise ValueError("RESEARCH_CONCLUSION_REQUIRES_REPRODUCIBILITY_NOTES")


def build_research_conclusion_v1(
    *,
    conclusion_id: str,
    hypothesis_id: str,
    created_at_utc: str,
    conclusion_summary: str,
    conclusion_status: str = "ACTIVE",
    supporting_evidence_refs: list[str] | str | None = None,
    contradictory_evidence_refs: list[str] | str | None = None,
    result_ledger_refs: list[str] | str | None = None,
    confidence_level: str = "LOW",
    confidence_trend: str = "FLAT",
    regime_specificity: str = "",
    edge_family: str = "",
    operational_implications: list[str] | str | None = None,
    known_failure_modes: list[str] | str | None = None,
    limitations: list[str] | str | None = None,
    invalidation_conditions: list[str] | str | None = None,
    reproducibility_notes: str = "",
    methodology_version: str = RESEARCH_METHODOLOGY_VERSION_V1,
    data_snapshot_refs: list[str] | str | None = None,
    code_version: str = RESEARCH_CODE_VERSION_UNKNOWN,
    created_by: str = "research_lab",
    reviewed_by: str = "",
    program_id: str = "",
    supersedes_conclusion_ids: list[str] | str | None = None,
    superseded_by_conclusion_id: str = "",
    artifact_lineage: list[dict[str, Any]] | None = None,
    reason_codes: list[str] | str | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_conclusion",
        "schema_version": "v1",
        "artifact_id": "research_conclusion_v1",
        "conclusion_id": conclusion_id,
        "hypothesis_id": hypothesis_id,
        "program_id": program_id,
        "created_at_utc": created_at_utc,
        "conclusion_summary": conclusion_summary,
        "conclusion_status": _enum(conclusion_status, RESEARCH_CONCLUSION_STATUSES, "conclusion_status"),
        "supporting_evidence_refs": _strings(supporting_evidence_refs),
        "contradictory_evidence_refs": _strings(contradictory_evidence_refs),
        "result_ledger_refs": _strings(result_ledger_refs),
        "confidence_level": confidence_level,
        "confidence_trend": confidence_trend,
        "regime_specificity": regime_specificity,
        "edge_family": edge_family,
        "operational_implications": _strings(operational_implications),
        "known_failure_modes": _strings(known_failure_modes),
        "limitations": _strings(limitations),
        "invalidation_conditions": _strings(invalidation_conditions),
        "reproducibility_notes": reproducibility_notes,
        "methodology_version": methodology_version,
        "data_snapshot_refs": _strings(data_snapshot_refs),
        "code_version": code_version,
        "created_by": created_by,
        "reviewed_by": reviewed_by,
        "supersedes_conclusion_ids": _strings(supersedes_conclusion_ids),
        "superseded_by_conclusion_id": superseded_by_conclusion_id,
        "artifact_lineage": _objects(artifact_lineage or []),
        "reason_codes": _strings(reason_codes),
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "trade_authorization_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "immutable_artifact": True,
        "canonical_json_hash": None,
    }
    validate_research_conclusion_lineage_v1(payload)
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def supersede_research_conclusion_v1(*, prior_conclusion: dict[str, Any], new_conclusion: dict[str, Any]) -> dict[str, Any]:
    if str(prior_conclusion.get("schema_id") or "") != "research_conclusion":
        raise ValueError("SUPERSESSION_REQUIRES_PRIOR_RESEARCH_CONCLUSION")
    if str(new_conclusion.get("schema_id") or "") != "research_conclusion":
        raise ValueError("SUPERSESSION_REQUIRES_NEW_RESEARCH_CONCLUSION")
    prior_id = str(prior_conclusion.get("conclusion_id") or "")
    new_id = str(new_conclusion.get("conclusion_id") or "")
    updated_prior = dict(prior_conclusion)
    updated_prior["conclusion_status"] = "SUPERSEDED"
    updated_prior["superseded_by_conclusion_id"] = new_id
    updated_prior["canonical_json_hash"] = None
    updated_prior["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(updated_prior)
    updated_new = dict(new_conclusion)
    supersedes = _strings(updated_new.get("supersedes_conclusion_ids"))
    if prior_id and prior_id not in supersedes:
        supersedes.append(prior_id)
    updated_new["supersedes_conclusion_ids"] = sorted(set(supersedes))
    updated_new["canonical_json_hash"] = None
    updated_new["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(updated_new)
    return {"prior_conclusion": updated_prior, "new_conclusion": updated_new}


def build_research_failure_archetype_v1(
    *,
    failure_archetype_id: str,
    title: str,
    description: str,
    related_hypotheses: list[str] | str | None = None,
    related_conclusions: list[str] | str | None = None,
    related_edge_families: list[str] | str | None = None,
    related_regimes: list[str] | str | None = None,
    failure_conditions: list[str] | str | None = None,
    observable_warning_signs: list[str] | str | None = None,
    example_evidence_refs: list[str] | str | None = None,
    governance_implications: list[str] | str | None = None,
    stop_risk_implications: list[str] | str | None = None,
    created_at_utc: str,
    status: str = "ACTIVE",
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_failure_archetype",
        "schema_version": "v1",
        "artifact_id": "research_failure_archetype_v1",
        "failure_archetype_id": failure_archetype_id,
        "title": title,
        "description": description,
        "related_hypotheses": _strings(related_hypotheses),
        "related_conclusions": _strings(related_conclusions),
        "related_edge_families": _strings(related_edge_families),
        "related_regimes": _strings(related_regimes),
        "failure_conditions": _strings(failure_conditions),
        "observable_warning_signs": _strings(observable_warning_signs),
        "example_evidence_refs": _strings(example_evidence_refs),
        "governance_implications": _strings(governance_implications),
        "stop_risk_implications": _strings(stop_risk_implications),
        "created_at_utc": created_at_utc,
        "status": _enum(status, RESEARCH_FAILURE_ARCHETYPE_STATUSES, "status"),
        "advisory_research_memory_only": True,
        "governance_rule_created": False,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "trade_authorization_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_knowledge_graph_v1(
    *,
    generated_at_utc: str,
    nodes: list[dict[str, Any]] | None = None,
    edges: list[dict[str, Any]] | None = None,
    hypothesis_refs: list[str] | str | None = None,
    edge_family_refs: list[str] | str | None = None,
    regime_refs: list[str] | str | None = None,
    sleeve_refs: list[str] | str | None = None,
    evidence_refs: list[str] | str | None = None,
    failure_mode_refs: list[str] | str | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_knowledge_graph",
        "schema_version": "v1",
        "artifact_id": "research_knowledge_graph_v1",
        "generated_at_utc": generated_at_utc,
        "nodes": _objects(nodes or []),
        "edges": _objects(edges or []),
        "hypothesis_refs": _strings(hypothesis_refs),
        "edge_family_refs": _strings(edge_family_refs),
        "regime_refs": _strings(regime_refs),
        "sleeve_refs": _strings(sleeve_refs),
        "evidence_refs": _strings(evidence_refs),
        "failure_mode_refs": _strings(failure_mode_refs),
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "trade_authorization_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "non_authoritative_scaffold": True,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def reject_invalidated_hypothesis_for_promotion_v1(hypothesis: dict[str, Any], result_ledger: dict[str, Any]) -> None:
    hypothesis_id = str(hypothesis.get("hypothesis_id") or "")
    invalidated = any(
        str(row.get("hypothesis_id") or "") == hypothesis_id
        and str(row.get("result_status") or "") in {"INVALIDATED", "CONTRADICTS_HYPOTHESIS"}
        for row in _objects(result_ledger.get("results"))
    )
    if invalidated and str(hypothesis.get("status") or "") in {"PROMOTION_CANDIDATE", "APPROVED_FOR_LITE", "VALIDATED_RESEARCH"}:
        raise ValueError(f"INVALIDATED_HYPOTHESIS_CANNOT_BE_PROMOTED:{hypothesis_id}")


def validate_research_lifecycle_transition_v1(
    *,
    from_status: str,
    to_status: str,
    evidence_refs: list[str] | list[dict[str, Any]] | None = None,
    result_ledger_refs: list[str] | list[dict[str, Any]] | None = None,
    promotion_artifact: dict[str, Any] | None = None,
    approved_by_human: bool = False,
    confidence_before: str = "LOW",
    confidence_after: str = "LOW",
) -> dict[str, Any]:
    source = _enum(from_status, RESEARCH_HYPOTHESIS_STATUSES, "from_status")
    target = _enum(to_status, RESEARCH_HYPOTHESIS_STATUSES, "to_status")
    evidence = _strings(evidence_refs)
    result_refs = _strings(result_ledger_refs)
    reason_codes: list[str] = [f"TRANSITION:{source}->{target}"]
    if source in {"REJECTED", "ARCHIVED"} and target in {"PROMOTION_CANDIDATE", "APPROVED_FOR_LITE"}:
        raise ValueError(f"{source}_HYPOTHESIS_CANNOT_BE_PROMOTED")
    if source == "APPROVED_FOR_LITE" and target != "APPROVED_FOR_LITE":
        raise ValueError("APPROVED_FOR_LITE_TRANSITION_REQUIRES_SUPERSESSION_NOT_MUTATION")
    if target == "VALIDATED_RESEARCH" and not evidence:
        raise ValueError("IDEA_CANNOT_BECOME_VALIDATED_RESEARCH_WITHOUT_EVIDENCE_REFS")
    if target == "PROMOTION_CANDIDATE":
        if source != "VALIDATED_RESEARCH":
            raise ValueError("PROMOTION_CANDIDATE_REQUIRES_VALIDATED_RESEARCH_SOURCE")
        if not result_refs:
            raise ValueError("PROMOTION_CANDIDATE_REQUIRES_RESULT_LEDGER_SUPPORT")
    if target == "APPROVED_FOR_LITE":
        if not promotion_artifact or str(promotion_artifact.get("schema_id") or "") != "research_to_lite_promotion":
            raise ValueError("APPROVED_FOR_LITE_REQUIRES_PROMOTION_ARTIFACT")
        if not approved_by_human and not bool(promotion_artifact.get("approved_by_human")):
            raise ValueError("APPROVED_FOR_LITE_REQUIRES_HUMAN_APPROVAL")
        if str(promotion_artifact.get("promotion_status") or "") != "APPROVED_FOR_LITE_IMPLEMENTATION":
            raise ValueError("APPROVED_FOR_LITE_REQUIRES_IMPLEMENTATION_APPROVAL_STATUS")
    if abs(_confidence_rank(confidence_after) - _confidence_rank(confidence_before)) > 1:
        raise ValueError("CONFIDENCE_CHANGE_OUT_OF_BOUNDS")
    return {"allowed": True, "reason_codes": reason_codes}


def apply_research_result_to_hypothesis_v1(
    *,
    hypothesis: dict[str, Any],
    result: dict[str, Any],
    evidence_packet_refs: list[str] | list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    hypothesis_id = str(hypothesis.get("hypothesis_id") or "")
    if str(result.get("hypothesis_id") or "") != hypothesis_id:
        raise ValueError(f"RESEARCH_RESULT_HYPOTHESIS_MISMATCH:{hypothesis_id}:{result.get('hypothesis_id')}")
    evidence_refs = _strings(evidence_packet_refs) or _strings(result.get("evidence_refs"))
    before_status = str(hypothesis.get("status") or "IDEA")
    if before_status in {"REJECTED", "ARCHIVED"}:
        raise ValueError(f"{before_status}_HYPOTHESIS_CANNOT_BE_PROMOTED")
    if before_status == "APPROVED_FOR_LITE":
        raise ValueError("APPROVED_FOR_LITE_REQUIRES_PROMOTION_ARTIFACT_NOT_RESULT_LEDGER")
    result_status = _enum(str(result.get("result_status") or ""), RESEARCH_RESULT_STATUSES_V1, "result_status")
    recommendation = _enum(str(result.get("promotion_recommendation") or "NONE"), RESEARCH_PROMOTION_RECOMMENDATIONS_V1, "promotion_recommendation")
    if before_status == "REJECTED" and recommendation == "PROMOTION_CANDIDATE":
        raise ValueError("REJECTED_HYPOTHESIS_CANNOT_BE_PROMOTED")
    if result_status in {"INVALIDATED", "CONTRADICTS_HYPOTHESIS"} and recommendation == "PROMOTION_CANDIDATE":
        raise ValueError("INVALIDATED_HYPOTHESIS_CANNOT_BE_PROMOTION_CANDIDATE")
    before_confidence = str(hypothesis.get("confidence_level") or result.get("confidence_before") or "LOW")
    after_confidence = _bounded_confidence_after(
        before_confidence=before_confidence,
        requested_after=str(result.get("confidence_after") or ""),
        result_status=result_status,
    )
    reason_codes: list[str] = [f"RESULT_STATUS:{result_status}"]
    if result_status == "INVALIDATED":
        next_status = "REJECTED"
        next_task = ""
        reason_codes.append("HYPOTHESIS_INVALIDATED")
    elif result_status == "CONTRADICTS_HYPOTHESIS":
        next_status = "REJECTED"
        next_task = "FAILURE_MODE_REVIEW"
        reason_codes.append("CONTRADICTORY_EVIDENCE")
    elif result_status == "SUPPORTS_HYPOTHESIS":
        if not evidence_refs:
            raise ValueError("VALIDATED_RESEARCH_REQUIRES_EVIDENCE_REFS")
        next_status = "VALIDATED_RESEARCH" if recommendation == "PROMOTION_CANDIDATE" else "UNDER_INVESTIGATION"
        next_task = "PROMOTION_REVIEW" if recommendation == "PROMOTION_CANDIDATE" else str(result.get("next_recommended_task") or "EXPECTANCY_REVIEW")
        reason_codes.append("EVIDENCE_SUPPORTS_HYPOTHESIS")
    elif result_status == "WEAK_SUPPORT":
        next_status = "UNDER_INVESTIGATION"
        next_task = "EXPECTANCY_REVIEW"
        reason_codes.append("WEAK_SUPPORT_NEEDS_MORE_RESEARCH")
    else:
        next_status = "UNDER_INVESTIGATION"
        next_task = str(result.get("next_recommended_task") or "DATA_AVAILABILITY_CHECK")
        reason_codes.append("MORE_RESEARCH_REQUIRED")
    if next_status == "APPROVED_FOR_LITE":
        raise ValueError("DIRECT_LITE_APPROVAL_FORBIDDEN")
    validate_research_lifecycle_transition_v1(
        from_status=before_status,
        to_status=next_status,
        evidence_refs=evidence_refs,
        result_ledger_refs=[str(result.get("result_id") or "")] if next_status == "PROMOTION_CANDIDATE" else [],
        confidence_before=before_confidence,
        confidence_after=after_confidence,
    )
    updated = dict(hypothesis)
    updated["status"] = next_status
    updated["confidence_level"] = after_confidence
    notes = str(updated.get("notes") or "")
    transition_note = f"research_result={result.get('result_id')}; status {before_status}->{next_status}; confidence {before_confidence}->{after_confidence}"
    updated["notes"] = f"{notes}\n{transition_note}".strip()
    updated["canonical_json_hash"] = None
    updated["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(updated)
    return {
        "hypothesis": updated,
        "updated_hypothesis": updated,
        "previous_status": before_status,
        "updated_status": next_status,
        "confidence_before": before_confidence,
        "confidence_after": after_confidence,
        "confidence_change": _confidence_change(before_confidence, after_confidence),
        "transition_reason_codes": reason_codes,
        "next_recommended_task": next_task,
        "lifecycle_notes": transition_note,
    }


def build_research_knowledge_graph_from_artifacts_v1(
    *,
    generated_at_utc: str,
    hypotheses: list[dict[str, Any]] | None = None,
    evidence_packets: list[dict[str, Any]] | None = None,
    result_ledgers: list[dict[str, Any]] | None = None,
    promotions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    hypothesis_refs: list[str] = []
    edge_family_refs: list[str] = []
    regime_refs: list[str] = []
    sleeve_refs: list[str] = []
    evidence_refs: list[str] = []
    failure_mode_refs: list[str] = []
    for hypothesis in _objects(hypotheses or []):
        hid = str(hypothesis.get("hypothesis_id") or "")
        if not hid:
            continue
        hypothesis_refs.append(hid)
        nodes.append({"node_id": hid, "node_type": "research_hypothesis", "status": str(hypothesis.get("status") or "")})
        edge_family_refs.extend(_strings(hypothesis.get("edge_family")))
        regime_refs.extend(_strings(hypothesis.get("expected_regime")))
        sleeve_refs.extend(_strings(hypothesis.get("related_sleeves")))
        failure_mode_refs.extend(_strings(hypothesis.get("failure_conditions")))
    for evidence in _objects(evidence_packets or []):
        eid = str(evidence.get("research_id") or "")
        hid = str(evidence.get("hypothesis_id") or "")
        if eid:
            evidence_refs.append(eid)
            nodes.append({"node_id": eid, "node_type": "research_evidence_packet", "status": str(evidence.get("research_status") or "")})
        if eid and hid:
            edges.append({"from": hid, "to": eid, "edge_type": "HAS_EVIDENCE"})
    for ledger in _objects(result_ledgers or []):
        for result in _objects(ledger.get("results")):
            rid = str(result.get("result_id") or "")
            hid = str(result.get("hypothesis_id") or "")
            if rid:
                nodes.append({"node_id": rid, "node_type": "research_result", "status": str(result.get("result_status") or "")})
            if rid and hid:
                edges.append({"from": hid, "to": rid, "edge_type": "HAS_RESULT"})
    for promotion in _objects(promotions or []):
        pid = str(promotion.get("promotion_id") or "")
        hid = str(promotion.get("hypothesis_id") or "")
        if pid:
            nodes.append({"node_id": pid, "node_type": "research_to_lite_promotion", "status": str(promotion.get("promotion_status") or "")})
        if pid and hid:
            edges.append({"from": hid, "to": pid, "edge_type": "HAS_PROMOTION_REVIEW"})
    return build_research_knowledge_graph_v1(
        generated_at_utc=generated_at_utc,
        nodes=nodes,
        edges=edges,
        hypothesis_refs=sorted(set(hypothesis_refs)),
        edge_family_refs=sorted(set(edge_family_refs)),
        regime_refs=sorted(set(regime_refs)),
        sleeve_refs=sorted(set(sleeve_refs)),
        evidence_refs=sorted(set(evidence_refs)),
        failure_mode_refs=sorted(set(failure_mode_refs)),
    )


def taxonomy_warnings_for_research_hypotheses_v1(
    *,
    taxonomy: dict[str, Any],
    hypotheses: list[dict[str, Any]],
    programs: list[dict[str, Any]] | None = None,
) -> list[str]:
    warnings = list(taxonomy.get("duplicate_term_warnings") or [])
    for section in ("market_theses", "edge_families", "edge_clusters", "trade_expressions"):
        seen_terms: dict[str, int] = {}
        for item in _strings(taxonomy.get(section)):
            key = _norm(item)
            seen_terms[key] = seen_terms.get(key, 0) + 1
        for key, count in seen_terms.items():
            if count > 1:
                warnings.append(f"DUPLICATE_TAXONOMY_LABEL:{section}:{key}")
    edge_families = {_norm(item) for item in _strings(taxonomy.get("edge_families"))}
    edge_clusters = {_norm(item) for item in _strings(taxonomy.get("edge_clusters"))}
    deprecated = {_norm(item) for item in _strings(taxonomy.get("deprecated_terms"))}
    seen_titles: dict[str, str] = {}
    thesis_by_edge: dict[str, set[str]] = {}
    program_hypothesis_refs: dict[str, list[str]] = {}
    for program in _objects(programs or []):
        for hid in _strings(program.get("related_hypotheses")):
            program_hypothesis_refs.setdefault(hid, []).append(str(program.get("program_id") or ""))
    for hypothesis in _objects(hypotheses):
        hid = str(hypothesis.get("hypothesis_id") or "")
        title_key = _norm(str(hypothesis.get("title") or ""))
        edge_family = _norm(str(hypothesis.get("edge_family") or ""))
        thesis_key = _norm(str(hypothesis.get("market_thesis") or ""))
        if title_key and title_key in seen_titles:
            warnings.append(f"DUPLICATE_HYPOTHESIS_TITLE:{title_key}:{seen_titles[title_key]}:{hid}")
        elif title_key:
            seen_titles[title_key] = hid
        if edge_family and thesis_key:
            thesis_by_edge.setdefault(edge_family, set()).add(thesis_key)
        if edge_family and edge_family not in edge_families:
            warnings.append(f"UNMAPPED_EDGE_FAMILY:{edge_family}:{hid}")
        if edge_family in deprecated:
            warnings.append(f"DEPRECATED_EDGE_FAMILY:{edge_family}:{hid}")
        for ref in _strings(hypothesis.get("related_research_refs")):
            if ref.upper().startswith("EDGE_CLUSTER:") and _norm(ref.split(":", 1)[1]) not in edge_clusters:
                warnings.append(f"UNMAPPED_EDGE_CLUSTER:{ref}:{hid}")
        if len(program_hypothesis_refs.get(hid, [])) > 1:
            warnings.append(f"AMBIGUOUS_PROGRAM_ASSIGNMENT:{hid}:{','.join(sorted(program_hypothesis_refs[hid]))}")
    for edge_family, thesis_labels in thesis_by_edge.items():
        if len(thesis_labels) > 1:
            warnings.append(f"CONFLICTING_THESIS_LABELS:{edge_family}:{len(thesis_labels)}")
    if len({str(item.get("edge_family") or "") for item in _objects(hypotheses) if item.get("edge_family")}) > max(10, len(edge_families) * 2):
        warnings.append("CONCEPT_EXPLOSION_WARNING:EDGE_FAMILY_COUNT_EXCEEDS_TAXONOMY_EXPECTATION")
    return sorted(set(warnings))


def build_hypothesis_test_plan_v1(
    *,
    hypothesis_id: str,
    current_stage: str = "definition_check",
    completed_stages: list[str] | None = None,
    failed_stages: list[str] | None = None,
    disproof_conditions: list[str] | None = None,
    validation_conditions: list[str] | None = None,
    minimum_sample_count: int = 30,
    minimum_regime_coverage: list[str] | None = None,
    required_robustness_checks: list[str] | None = None,
    required_overlap_checks: list[str] | None = None,
    required_out_of_sample_checks: list[str] | None = None,
    next_required_task: str = "",
    terminal_state_allowed: list[str] | None = None,
) -> dict[str, Any]:
    completed = _stage_list(completed_stages)
    current = _enum_lc(current_stage, set(TEST_STAGES), "current_stage")
    next_task = next_required_task or _task_type_for_stage(current)
    payload = {
        "schema_id": "hypothesis_test_plan",
        "schema_version": "v1",
        "artifact_id": "hypothesis_test_plan_v1",
        "hypothesis_id": hypothesis_id,
        "required_test_stages": list(TEST_STAGES),
        "current_stage": current,
        "completed_stages": completed,
        "failed_stages": _stage_list(failed_stages),
        "disproof_conditions": _strings(disproof_conditions)
        or [
            "expectancy <= 0 after friction",
            "sample_count below minimum",
            "fails out-of-sample",
            "duplicate of existing sleeve with no incremental value",
        ],
        "validation_conditions": _strings(validation_conditions)
        or [
            "positive expectancy after friction",
            "minimum sample count met",
            "out-of-sample support present",
            "failure modes and edge overlap reviewed",
        ],
        "minimum_sample_count": int(minimum_sample_count),
        "minimum_regime_coverage": _strings(minimum_regime_coverage) or ["trend", "chop", "panic", "compression", "expansion", "fragility"],
        "required_robustness_checks": _strings(required_robustness_checks) or ["threshold_variation", "window_variation", "symbol_variation"],
        "required_overlap_checks": _strings(required_overlap_checks) or ["hypothesis_overlap", "sleeve_overlap", "edge_family_overlap"],
        "required_out_of_sample_checks": _strings(required_out_of_sample_checks) or ["holdout_period"],
        "next_required_task": next_task,
        "terminal_state_allowed": [_enum_lc(item, TERMINAL_TEST_STATES, "terminal_state_allowed") for item in (_strings(terminal_state_allowed) or sorted(TERMINAL_TEST_STATES))],
        "research_lab_only": True,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def register_hypothesis_v1(
    *,
    registry: dict[str, Any],
    task_queue: dict[str, Any] | None = None,
    title: str,
    edge_family: str,
    behavioral_thesis: str,
    market_regime: str,
    trigger_conditions: str,
    expected_outcome: str,
    failure_modes: str | list[str],
    created_at_utc: str,
    instrument_universe: list[str] | str | None = None,
    time_horizon: str = "",
    overlap_tags: list[str] | None = None,
    source_type: str = "manual",
    source_reference: str = "",
    priority: str = "normal",
    notes: str = "",
) -> dict[str, Any]:
    existing = _objects(registry.get("hypotheses"))
    hypothesis_id = _hypothesis_id(title=title, edge_family=edge_family, created_at_utc=created_at_utc, ordinal=len(existing) + 1)
    warnings = duplicate_warnings_for_hypothesis_v1(
        existing,
        edge_family=edge_family,
        market_regime=market_regime,
        trigger_conditions=trigger_conditions,
        behavioral_thesis=behavioral_thesis,
        expected_outcome=expected_outcome,
        overlap_tags=overlap_tags or [],
    )
    record = build_hypothesis_record_v1(
        hypothesis_id=hypothesis_id,
        title=title,
        behavioral_thesis=behavioral_thesis,
        edge_family=edge_family,
        market_regime=market_regime,
        trigger_conditions=trigger_conditions,
        expected_outcome=expected_outcome,
        failure_modes=failure_modes,
        instrument_universe=instrument_universe,
        time_horizon=time_horizon,
        related_hypotheses=warnings["related_hypothesis_ids"],
        overlap_tags=overlap_tags,
        source_type=source_type,
        source_reference=source_reference,
        priority=priority,
        lifecycle_state="proposed",
        created_at=created_at_utc,
        updated_at=created_at_utc,
        notes=notes,
    )
    task = build_research_task_v1(
        task_id=f"task:{hypothesis_id}:definition_check",
        task_type="definition_check",
        hypothesis_id=hypothesis_id,
        source_trigger="manual_hypothesis_added",
        priority=priority,
        requested_action="Run hypothesis definition_check stage.",
        created_at=created_at_utc,
        output_expected="experiment_result.v1",
    )
    plan = build_hypothesis_test_plan_v1(hypothesis_id=hypothesis_id)
    new_registry = build_hypothesis_registry_v1(
        generated_at_utc=created_at_utc,
        hypotheses=[*existing, record],
    )
    existing_tasks = _objects((task_queue or {}).get("tasks"))
    queue = build_research_task_queue_v1(generated_at_utc=created_at_utc, tasks=[*existing_tasks, task])
    return {
        "hypothesis": record,
        "registry": new_registry,
        "task_queue": queue,
        "test_plan": plan,
        "duplicate_warning": bool(warnings["related_hypothesis_ids"]),
        **warnings,
    }


def duplicate_warnings_for_hypothesis_v1(
    hypotheses: list[dict[str, Any]],
    *,
    edge_family: str,
    market_regime: str,
    trigger_conditions: str,
    behavioral_thesis: str,
    expected_outcome: str,
    overlap_tags: list[str],
) -> dict[str, Any]:
    related: list[str] = []
    reasons: list[str] = []
    new_tags = {_norm(tag) for tag in overlap_tags}
    new_words = _keyword_set(f"{trigger_conditions} {behavioral_thesis} {expected_outcome}")
    for row in hypotheses:
        row_reasons: list[str] = []
        if _norm(str(row.get("edge_family") or "")) == _norm(edge_family):
            row_reasons.append("EDGE_FAMILY_MATCH")
        if _norm(str(row.get("market_regime") or "")) == _norm(market_regime):
            row_reasons.append("REGIME_MATCH")
        if new_tags and new_tags.intersection({_norm(tag) for tag in _strings(row.get("overlap_tags"))}):
            row_reasons.append("OVERLAP_TAG_MATCH")
        existing_words = _keyword_set(f"{row.get('trigger_conditions')} {row.get('behavioral_thesis')} {row.get('expected_outcome')}")
        if _jaccard(new_words, existing_words) >= 0.35:
            row_reasons.append("THESIS_TRIGGER_OUTCOME_SIMILAR")
        if len(row_reasons) >= 2:
            related.append(str(row.get("hypothesis_id") or ""))
            reasons.extend(row_reasons)
    return {
        "related_hypothesis_ids": sorted({item for item in related if item}),
        "overlap_reason_codes": sorted(set(reasons)),
    }


def build_research_task_v1(
    *,
    task_id: str,
    task_type: str,
    hypothesis_id: str,
    source_trigger: str,
    priority: str,
    requested_action: str,
    created_at: str,
    status: str = "open",
    owner: str = "research_lab",
    started_at: str = "",
    completed_at: str = "",
    blocking_reason: str = "",
    output_expected: str = "experiment_result.v1",
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "task_type": _enum_lc(task_type, TASK_TYPES, "task_type"),
        "hypothesis_id": hypothesis_id,
        "source_trigger": source_trigger,
        "priority": priority,
        "requested_action": requested_action,
        "status": _enum_lc(status, TASK_STATUSES, "status"),
        "created_at": created_at,
        "started_at": started_at,
        "completed_at": completed_at,
        "owner": owner,
        "blocking_reason": blocking_reason,
        "output_expected": output_expected,
    }


def build_research_task_queue_v1(*, generated_at_utc: str, tasks: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_tasks = sorted(
        tasks,
        key=lambda row: (
            RESEARCH_PRIORITY_ORDER.get(str(row.get("priority") or "").strip().upper(), 9),
            str(row.get("created_at_utc") or row.get("created_at") or ""),
            str(row.get("task_id") or ""),
        ),
    )
    payload = {
        "schema_id": "research_task_queue",
        "schema_version": "v1",
        "artifact_id": "research_task_queue_v1",
        "generated_at_utc": generated_at_utc,
        "tasks": sorted_tasks,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "trade_authorization_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_experiment_result_v1(
    *,
    experiment_id: str,
    hypothesis_id: str,
    task_id: str,
    test_stage: str,
    dataset_used: str,
    instrument_universe: list[str] | str | None = None,
    test_window: str,
    trigger_definition: str,
    outcome_definition: str,
    sample_count: int,
    expectancy: str,
    win_rate: str,
    avg_return: str = "",
    median_return: str = "",
    max_drawdown: str = "",
    volatility: str = "",
    friction_adjusted_result: str = "",
    regime_dependency: str,
    robustness_notes: str,
    out_of_sample_result: str = "",
    overlap_with_existing_sleeves: str,
    result_status: str,
    recommendation: str,
    next_action: str,
    created_at: str,
) -> dict[str, Any]:
    payload = {
        "schema_id": "experiment_result",
        "schema_version": "v1",
        "artifact_id": "experiment_result_v1",
        "experiment_id": experiment_id,
        "hypothesis_id": hypothesis_id,
        "task_id": task_id,
        "test_stage": _enum_lc(test_stage, set(TEST_STAGES), "test_stage"),
        "dataset_used": dataset_used,
        "instrument_universe": _strings(instrument_universe),
        "test_window": test_window,
        "trigger_definition": trigger_definition,
        "outcome_definition": outcome_definition,
        "sample_count": int(sample_count),
        "expectancy": expectancy,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "median_return": median_return,
        "max_drawdown": max_drawdown,
        "volatility": volatility,
        "friction_adjusted_result": friction_adjusted_result,
        "regime_dependency": regime_dependency,
        "robustness_notes": robustness_notes,
        "out_of_sample_result": out_of_sample_result,
        "overlap_with_existing_sleeves": overlap_with_existing_sleeves,
        "result_status": _enum_lc(result_status, RESULT_STATUSES, "result_status"),
        "recommendation": recommendation,
        "next_action": next_action,
        "created_at": created_at,
        "research_lab_only": True,
        "executable_trade_created": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_experiment_result_v1(
    *,
    experiment_id: str,
    hypothesis_id: str,
    task_id: str,
    dataset_used: str,
    test_window: str,
    trigger_definition: str,
    outcome_definition: str,
    sample_count: int,
    expectancy: str,
    win_rate: str,
    drawdown: str = "",
    regime_dependency: str,
    robustness_notes: str,
    overlap_with_existing_sleeves: str,
    result_status: str,
    recommendation: str,
    next_action: str,
    completed_stage: str,
    completed_at_utc: str,
    instrument_universe: list[str] | str | None = None,
    avg_return: str = "",
    median_return: str = "",
    volatility: str = "",
    friction_adjusted_result: str = "",
    out_of_sample_result: str = "",
) -> dict[str, Any]:
    return build_experiment_result_v1(
        experiment_id=experiment_id,
        hypothesis_id=hypothesis_id,
        task_id=task_id,
        test_stage=completed_stage,
        dataset_used=dataset_used,
        instrument_universe=instrument_universe,
        test_window=test_window,
        trigger_definition=trigger_definition,
        outcome_definition=outcome_definition,
        sample_count=sample_count,
        expectancy=expectancy,
        win_rate=win_rate,
        avg_return=avg_return,
        median_return=median_return,
        max_drawdown=drawdown,
        volatility=volatility,
        friction_adjusted_result=friction_adjusted_result,
        regime_dependency=regime_dependency,
        robustness_notes=robustness_notes,
        out_of_sample_result=out_of_sample_result,
        overlap_with_existing_sleeves=overlap_with_existing_sleeves,
        result_status=result_status,
        recommendation=recommendation,
        next_action=next_action,
        created_at=completed_at_utc,
    )


def apply_experiment_result_v1(
    *,
    registry: dict[str, Any],
    task_queue: dict[str, Any],
    test_plan: dict[str, Any],
    result: dict[str, Any],
    updated_at_utc: str,
) -> dict[str, Any]:
    hypothesis_id = str(result["hypothesis_id"])
    result_status = str(result["result_status"])
    completed_stage = str(result.get("test_stage") or result.get("completed_stage") or "")
    current_stage = str(test_plan.get("current_stage") or "")
    if completed_stage != current_stage:
        raise ValueError(f"EXPERIMENT_RESULT_STAGE_MISMATCH:expected={current_stage}:actual={completed_stage}")
    prior_completed = _stage_list(test_plan.get("completed_stages"))
    completed_with_result = _stage_list([*prior_completed, completed_stage])
    if result_status == "validated_candidate" and (completed_stage != "promotion_review" or set(completed_with_result) != set(TEST_STAGES)):
        raise ValueError("VALIDATED_CANDIDATE_REQUIRES_COMPLETED_TEST_PLAN")
    hypotheses = []
    for row in _objects(registry.get("hypotheses")):
        if str(row.get("hypothesis_id")) != hypothesis_id:
            hypotheses.append(row)
            continue
        updated = dict(row)
        updated["updated_at"] = updated_at_utc
        updated["last_tested_at"] = updated_at_utc
        updated["test_count"] = int(updated.get("test_count") or 0) + 1
        updated["latest_result_status"] = result_status
        updated["lifecycle_state"] = _lifecycle_for_result(result_status, next_stage_hint=_next_stage(completed_with_result))
        updated["promotion_status"] = "candidate" if result_status == "validated_candidate" else str(updated.get("promotion_status") or "not_ready")
        hypotheses.append(updated)
    completed = completed_with_result
    failed = _stage_list(test_plan.get("failed_stages"))
    if result_status in {"weak", "rejected", "insufficient_data"}:
        failed = _stage_list([*failed, completed_stage])
    terminal = _terminal_for_result(result_status)
    next_stage = "" if terminal else _next_stage(completed)
    new_plan = dict(test_plan)
    new_plan["completed_stages"] = completed
    new_plan["failed_stages"] = failed
    new_plan["current_stage"] = next_stage or completed_stage
    new_plan["next_required_task"] = "" if terminal else _task_type_for_stage(next_stage)
    new_plan["canonical_json_hash"] = None
    new_plan["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(new_plan)
    tasks: list[dict[str, Any]] = []
    for task in _objects(task_queue.get("tasks")):
        updated_task = dict(task)
        if str(task.get("task_id")) == str(result.get("task_id")):
            updated_task["status"] = "completed"
            updated_task["completed_at"] = updated_at_utc
        tasks.append(updated_task)
    if next_stage:
        next_task_type = _task_type_for_stage(next_stage)
        tasks.append(
            build_research_task_v1(
                task_id=f"task:{hypothesis_id}:{next_task_type}",
                task_type=next_task_type,
                hypothesis_id=hypothesis_id,
                source_trigger="test_plan_next_stage",
                priority="normal",
                requested_action=f"Run hypothesis test stage {next_stage}.",
                created_at=updated_at_utc,
            )
        )
    new_registry = build_hypothesis_registry_v1(generated_at_utc=updated_at_utc, hypotheses=hypotheses)
    new_queue = build_research_task_queue_v1(generated_at_utc=updated_at_utc, tasks=tasks)
    return {"registry": new_registry, "task_queue": new_queue, "test_plan": new_plan, "terminal_state": terminal}


def build_research_lab_awareness_report_v1(
    *,
    generated_at_utc: str,
    registry: dict[str, Any],
    task_queue: dict[str, Any],
    experiment_results: list[dict[str, Any]],
) -> dict[str, Any]:
    hypotheses = _objects(registry.get("hypotheses"))
    tasks = _objects(task_queue.get("tasks"))
    payload = {
        "schema_id": "research_lab_awareness_report",
        "schema_version": "v1",
        "artifact_id": "research_lab_awareness_report_v1",
        "generated_at_utc": generated_at_utc,
        "new_hypotheses": [row for row in hypotheses if row.get("lifecycle_state") == "proposed"],
        "queued_tasks": [row for row in tasks if row.get("status") == "open"],
        "completed_experiments": experiment_results,
        "blocked_tasks": [row for row in tasks if row.get("status") in {"blocked", "failed"}],
        "duplicate_warnings": [
            {"hypothesis_id": row.get("hypothesis_id"), "related_hypotheses": row.get("related_hypotheses")}
            for row in hypotheses
            if row.get("related_hypotheses")
        ],
        "promotion_candidates": [row for row in hypotheses if row.get("promotion_status") == "candidate"],
        "rejected_ideas": [row for row in hypotheses if row.get("lifecycle_state") == "rejected"],
        "promising_ideas": [row for row in hypotheses if row.get("latest_result_status") == "promising"],
        "stale_ideas_needing_retest": [row for row in hypotheses if row.get("latest_result_status") == "promising" and not row.get("last_tested_at")],
        "sleeve_failure_reviews": [row for row in tasks if row.get("task_type") == "sleeve_failure_review"],
        "research_lab_only": True,
        "executable_trade_created": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_promotion_review_v1(
    *,
    promotion_id: str,
    hypothesis_id: str,
    generated_at_utc: str,
    test_plan: dict[str, Any],
    friction_adjusted_result: str,
    out_of_sample_support: str,
    regime_notes: str,
    failure_mode_notes: str,
    edge_overlap_review: str,
    approved_by_human: bool,
    approval_reason_codes: list[str] | None = None,
    rejection_reason_codes: list[str] | None = None,
) -> dict[str, Any]:
    completed = set(_strings(test_plan.get("completed_stages")))
    plan_complete = all(stage in completed for stage in TEST_STAGES)
    eligible = (
        plan_complete
        and _evidence_positive(friction_adjusted_result)
        and str(out_of_sample_support).strip().lower() not in {"", "none", "missing", "not_computed"}
        and bool(str(regime_notes).strip())
        and bool(str(failure_mode_notes).strip())
        and bool(str(edge_overlap_review).strip())
        and bool(approved_by_human)
    )
    payload = {
        "schema_id": "promotion_review",
        "schema_version": "v1",
        "artifact_id": "promotion_review_v1",
        "promotion_id": promotion_id,
        "hypothesis_id": hypothesis_id,
        "generated_at_utc": generated_at_utc,
        "completed_test_plan": plan_complete,
        "friction_adjusted_result": friction_adjusted_result,
        "out_of_sample_support": out_of_sample_support,
        "regime_notes": regime_notes,
        "failure_mode_notes": failure_mode_notes,
        "edge_overlap_review": edge_overlap_review,
        "approved_by_human": bool(approved_by_human),
        "eligible_for_promoted_sleeve_library": eligible,
        "approval_reason_codes": _strings(approval_reason_codes),
        "rejection_reason_codes": _strings(rejection_reason_codes),
        "research_lab_only": True,
        "automatic_promotion_allowed": False,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_hypothesis_progress_report_v1(
    *,
    generated_at_utc: str,
    hypothesis: dict[str, Any],
    test_plan: dict[str, Any],
) -> dict[str, Any]:
    blockers = []
    if str(hypothesis.get("lifecycle_state")) not in {"promoted", "rejected", "retired"} and test_plan.get("next_required_task"):
        blockers.append("TEST_PLAN_NOT_TERMINAL")
    payload = {
        "schema_id": "hypothesis_progress_report",
        "schema_version": "v1",
        "artifact_id": "hypothesis_progress_report_v1",
        "generated_at_utc": generated_at_utc,
        "hypothesis_id": str(hypothesis.get("hypothesis_id") or ""),
        "title": str(hypothesis.get("title") or ""),
        "lifecycle_state": str(hypothesis.get("lifecycle_state") or ""),
        "current_test_stage": str(test_plan.get("current_stage") or ""),
        "completed_stages": _strings(test_plan.get("completed_stages")),
        "next_required_task": str(test_plan.get("next_required_task") or ""),
        "blockers": blockers,
        "why_not_done_yet": "Hypothesis test plan has not reached a terminal state." if blockers else "Terminal state reached.",
        "what_would_disprove_it": _strings(test_plan.get("disproof_conditions")),
        "what_would_validate_it": _strings(test_plan.get("validation_conditions")),
        "research_lab_only": True,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_promoted_sleeve_library_v1(*, generated_at_utc: str, sleeves: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = [_promoted_sleeve(row) for row in sleeves]
    payload = {
        "schema_id": "promoted_sleeve_library",
        "schema_version": "v1",
        "artifact_id": "promoted_sleeve_library_v1",
        "generated_at_utc": generated_at_utc,
        "promoted_sleeves": normalized,
        "sleeves": normalized,
        "only_promoted_sleeves_allowed": True,
        "research_lab_artifacts_directly_executable": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_manual_trade_packet_v1(
    *,
    packet_id: str,
    run_id: str,
    date: str,
    generated_at_utc: str,
    regime_state: str,
    trade_candidates: list[dict[str, Any]],
    promoted_sleeve_library: dict[str, Any] | None = None,
) -> dict[str, Any]:
    promoted_sources = _promoted_sleeve_sources(promoted_sleeve_library)
    candidates = [
        _manual_trade_candidate(
            row,
            run_id=run_id,
            date=date,
            regime_state=regime_state,
            promoted_sources=promoted_sources,
        )
        for row in trade_candidates
    ]
    payload = {
        "schema_id": "manual_trade_packet",
        "schema_version": "v1",
        "artifact_id": "manual_trade_packet_v1",
        "packet_id": packet_id,
        "run_id": run_id,
        "date": date,
        "generated_at_utc": generated_at_utc,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_status": "DEFERRED",
        "trade_candidates": candidates,
        "all_candidates_traceable_to_promoted_sleeves": all(bool(row.get("sleeve_id") and row.get("source_hypothesis_id")) for row in candidates),
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_manual_execution_receipt_v1(
    *,
    receipt_id: str,
    recommended_trade_id: str,
    actual_symbol: str,
    actual_side: str,
    actual_quantity: int,
    order_type: str,
    fill_price: str,
    fill_timestamp: str,
    stop_order_entered: bool,
    stop_price: str,
    operator_notes: str,
    deviations_from_recommendation: list[str] | None = None,
    deviations_from_aegis_recommendation: list[str] | None = None,
    source_packet_type: str = "EOD_MANUAL_PACKET",
    alert_id: str = "",
    source_packet_id: str = "",
    event_id: str = "",
    event_run_id: str = "",
    fill_timestamp_utc: str = "",
    deviation_from_entry_reference_price: str = "",
    fill_before_valid_until: bool | None = None,
    max_entry_slippage_respected: bool | None = None,
) -> dict[str, Any]:
    deviations = _strings(deviations_from_recommendation) or _strings(deviations_from_aegis_recommendation)
    payload = {
        "schema_id": "manual_execution_receipt",
        "schema_version": "v1",
        "artifact_id": "manual_execution_receipt_v1",
        "receipt_id": receipt_id,
        "recommended_trade_id": recommended_trade_id,
        "actual_symbol": actual_symbol.upper(),
        "actual_side": actual_side.upper(),
        "actual_quantity": int(actual_quantity),
        "order_type": order_type.upper(),
        "fill_price": fill_price,
        "fill_timestamp": fill_timestamp,
        "stop_order_entered": bool(stop_order_entered),
        "stop_price": stop_price,
        "operator_notes": operator_notes,
        "deviations_from_recommendation": deviations,
        "deviations_from_aegis_recommendation": deviations,
        "source_packet_type": _enum(str(source_packet_type or "EOD_MANUAL_PACKET"), {"EOD_MANUAL_PACKET", "EVENT_TACTICAL_PACKET"}, "source_packet_type"),
        "alert_id": alert_id,
        "source_packet_id": source_packet_id or recommended_trade_id,
        "event_id": event_id,
        "event_run_id": event_run_id,
        "fill_timestamp_utc": fill_timestamp_utc or fill_timestamp,
        "deviation_from_entry_reference_price": deviation_from_entry_reference_price,
        "fill_before_valid_until": bool(fill_before_valid_until) if fill_before_valid_until is not None else False,
        "max_entry_slippage_respected": bool(max_entry_slippage_respected) if max_entry_slippage_respected is not None else False,
        "manual_observation_only": True,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_outcome_ledger_v1(*, generated_at_utc: str, outcome_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_id": "outcome_ledger",
        "schema_version": "v1",
        "artifact_id": "outcome_ledger_v1",
        "generated_at_utc": generated_at_utc,
        "outcomes": [_outcome_row(row) for row in outcome_rows],
        "feeds_research_lab": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_learning_tasks_from_outcomes_v1(
    *,
    generated_at_utc: str,
    outcome_ledger: dict[str, Any],
    existing_tasks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    tasks = list(existing_tasks or [])
    for row in _objects(outcome_ledger.get("outcomes")):
        trade_id = str(row.get("trade_id") or "")
        hypothesis_id = str(row.get("hypothesis_id") or "")
        if not trade_id or not hypothesis_id:
            continue
        status = str(row.get("outcome_status") or "").lower()
        failure = str(row.get("failure_reason") or "").lower()
        deviation = str(row.get("operator_deviation") or "").lower()
        source_packet_type = str(row.get("source_packet_type") or "").upper()
        if source_packet_type == "EVENT_TACTICAL_PACKET":
            event_type = str(row.get("event_type") or "event").lower()
            if status in {"loss", "stopped_out", "underperformed"}:
                tasks.append(
                    build_research_task_v1(
                        task_id=f"task:{hypothesis_id}:event_failure_review:{_safe_id(trade_id)}",
                        task_type="event_failure_review",
                        hypothesis_id=hypothesis_id,
                        source_trigger="outcome_ledger_event_failure",
                        priority="high",
                        requested_action=f"Review failed event tactical outcome {trade_id} ({event_type}).",
                        created_at=generated_at_utc,
                        output_expected="experiment_result.v1",
                    )
                )
            if status in {"win", "profitable", "outperformed"}:
                tasks.append(
                    build_research_task_v1(
                        task_id=f"task:{hypothesis_id}:event_success_review:{_safe_id(trade_id)}",
                        task_type="event_success_review",
                        hypothesis_id=hypothesis_id,
                        source_trigger="outcome_ledger_event_success",
                        priority="normal",
                        requested_action=f"Review successful event tactical outcome {trade_id} ({event_type}).",
                        created_at=generated_at_utc,
                        output_expected="experiment_result.v1",
                    )
                )
            if not bool(row.get("valid_until_respected", False)):
                tasks.append(
                    build_research_task_v1(
                        task_id=f"task:{hypothesis_id}:event_stale_entry_review:{_safe_id(trade_id)}",
                        task_type="event_stale_entry_review",
                        hypothesis_id=hypothesis_id,
                        source_trigger="outcome_ledger_event_stale_entry",
                        priority="high",
                        requested_action=f"Review event entry timing validity for {trade_id}.",
                        created_at=generated_at_utc,
                        output_expected="experiment_result.v1",
                    )
                )
            if not bool(row.get("entry_slippage_respected", False)):
                tasks.append(
                    build_research_task_v1(
                        task_id=f"task:{hypothesis_id}:event_false_positive_review:{_safe_id(trade_id)}",
                        task_type="event_false_positive_review",
                        hypothesis_id=hypothesis_id,
                        source_trigger="outcome_ledger_event_slippage_or_false_positive",
                        priority="normal",
                        requested_action=f"Review event validity/slippage quality for {trade_id}.",
                        created_at=generated_at_utc,
                        output_expected="experiment_result.v1",
                    )
                )
            if "overlap" in failure or "correlation" in failure:
                tasks.append(
                    build_research_task_v1(
                        task_id=f"task:{hypothesis_id}:event_overlap_review:{_safe_id(trade_id)}",
                        task_type="event_overlap_review",
                        hypothesis_id=hypothesis_id,
                        source_trigger="outcome_ledger_event_overlap",
                        priority="normal",
                        requested_action=f"Review event overlap behavior for {trade_id}.",
                        created_at=generated_at_utc,
                        output_expected="experiment_result.v1",
                    )
                )
        if status in {"loss", "stopped_out", "underperformed"}:
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:sleeve_failure_review:{_safe_id(trade_id)}",
                    task_type="sleeve_failure_review",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_sleeve_underperformance",
                    priority="high",
                    requested_action=f"Review failed or underperforming manual trade outcome {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
        if "delay" in deviation or "delay" in failure:
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:friction_test:{_safe_id(trade_id)}",
                    task_type="friction_test",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_manual_delay",
                    priority="normal",
                    requested_action=f"Test manual execution friction impact for trade {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
        if "overlap" in failure or "correlation" in failure:
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:edge_overlap_review:{_safe_id(trade_id)}",
                    task_type="edge_overlap_review",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_correlated_loss",
                    priority="high",
                    requested_action=f"Review edge overlap and correlated loss behavior for trade {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
        if "regime" in failure:
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:regime_test:{_safe_id(trade_id)}",
                    task_type="regime_test",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_regime_dependency",
                    priority="normal",
                    requested_action=f"Retest regime dependency for trade {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
        if "missed" in failure or status == "missed_opportunity":
            tasks.append(
                build_research_task_v1(
                    task_id=f"task:{hypothesis_id}:anomaly_review:{_safe_id(trade_id)}",
                    task_type="anomaly_review",
                    hypothesis_id=hypothesis_id,
                    source_trigger="outcome_ledger_missed_opportunity",
                    priority="normal",
                    requested_action=f"Investigate missed opportunity around trade {trade_id}.",
                    created_at=generated_at_utc,
                    output_expected="experiment_result.v1",
                )
            )
    return build_research_task_queue_v1(generated_at_utc=generated_at_utc, tasks=tasks)


def build_research_evidence_packet_v1(
    *,
    research_id: str,
    hypothesis_id: str,
    title: str,
    hypothesis_description: str,
    research_type: str,
    created_at_utc: str,
    source_data_summary: str,
    replay_window: str,
    instruments_tested: list[str],
    regimes_tested: list[str],
    edge_family: str,
    expected_holding_period: str,
    methodology_summary: str,
    metrics_summary: dict[str, Any],
    expectancy_summary: str,
    drawdown_summary: str,
    MAE_MFE_summary: str,
    failure_modes: list[str],
    known_limitations: list[str],
    reproducibility_notes: str,
    artifact_lineage: list[dict[str, Any]],
    research_status: str,
    task_id: str = "",
    data_snapshot_refs: list[str] | str | None = None,
    methodology_version: str = RESEARCH_METHODOLOGY_VERSION_V1,
    code_version: str = RESEARCH_CODE_VERSION_UNKNOWN,
    reason_codes: list[str] | str | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_evidence_packet",
        "schema_version": "v1",
        "artifact_id": "research_evidence_packet_v1",
        "research_id": research_id,
        "hypothesis_id": hypothesis_id,
        "task_id": task_id,
        "title": title,
        "hypothesis_description": hypothesis_description,
        "research_type": _enum(research_type, RESEARCH_TYPES, "research_type"),
        "created_at_utc": created_at_utc,
        "source_data_summary": source_data_summary,
        "replay_window": replay_window,
        "instruments_tested": _strings(instruments_tested),
        "regimes_tested": _strings(regimes_tested),
        "edge_family": edge_family,
        "expected_holding_period": expected_holding_period,
        "methodology_summary": methodology_summary,
        "methodology_version": methodology_version,
        "metrics_summary": metrics_summary,
        "expectancy_summary": expectancy_summary,
        "drawdown_summary": drawdown_summary,
        "MAE_MFE_summary": MAE_MFE_summary,
        "failure_modes": _strings(failure_modes),
        "known_limitations": _strings(known_limitations),
        "reproducibility_notes": reproducibility_notes,
        "artifact_lineage": artifact_lineage,
        "data_snapshot_refs": _strings(data_snapshot_refs),
        "code_version": code_version,
        "reason_codes": _strings(reason_codes),
        "research_status": _enum(research_status, RESEARCH_STATUSES, "research_status"),
        "research_lab_only": True,
        "execution_authority_granted": False,
        "runtime_authorized": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "automatic_lite_promotion_allowed": False,
        "immutable_artifact": True,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def validate_research_promotion_lineage_v1(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_id") or "") != "research_to_lite_promotion":
        raise ValueError("PROMOTION_LINEAGE_REQUIRES_RESEARCH_TO_LITE_PROMOTION")
    if not _objects(payload.get("evidence_packet_refs")):
        raise ValueError("PROMOTION_REQUIRES_EVIDENCE_PACKET_REFS")
    if not _objects(payload.get("result_ledger_refs")):
        raise ValueError("PROMOTION_REQUIRES_RESULT_LEDGER_REFS")
    if not str(payload.get("methodology_version") or "").strip():
        raise ValueError("PROMOTION_REQUIRES_METHODOLOGY_VERSION")
    if not str(payload.get("code_version") or "").strip():
        raise ValueError("PROMOTION_REQUIRES_CODE_VERSION")
    if not _strings(payload.get("reason_codes")):
        raise ValueError("PROMOTION_REQUIRES_REASON_CODES")


def build_research_to_lite_promotion_v1(
    *,
    promotion_id: str,
    research_id: str,
    hypothesis_id: str,
    proposed_lite_component_type: str,
    promotion_status: str,
    evidence_packet_refs: list[dict[str, Any]],
    validation_summary: str,
    regime_evidence: str,
    expectancy_evidence: str,
    failure_mode_review: str,
    governance_compatibility_review: str,
    risk_contract_review: str,
    stop_logic_review: str,
    manual_execution_compatibility: str,
    operator_clarity_review: str,
    implementation_notes: str,
    required_tests: list[str],
    approval_reason_codes: list[str],
    rejection_reason_codes: list[str],
    approved_by_human: bool,
    created_at_utc: str,
    reviewed_at_utc: str = "",
    source_research_status: str = "VALIDATED_RESEARCH",
    result_ledger_refs: list[dict[str, Any]] | None = None,
    research_hypothesis: dict[str, Any] | None = None,
    data_snapshot_refs: list[str] | str | None = None,
    methodology_version: str = RESEARCH_METHODOLOGY_VERSION_V1,
    code_version: str = RESEARCH_CODE_VERSION_UNKNOWN,
    artifact_lineage: list[dict[str, Any]] | None = None,
    reason_codes: list[str] | str | None = None,
    reproducibility_notes: str = "",
) -> dict[str, Any]:
    status = _enum(promotion_status, PROMOTION_STATUSES, "promotion_status")
    source_status = _enum(source_research_status, RESEARCH_STATUSES, "source_research_status")
    if not evidence_packet_refs:
        raise ValueError("PROMOTION_REQUIRES_EVIDENCE_PACKET_REFS")
    if not result_ledger_refs:
        raise ValueError("PROMOTION_REQUIRES_RESULT_LEDGER_REFS")
    if research_hypothesis is not None:
        hypothesis_status = str(research_hypothesis.get("status") or "")
        if hypothesis_status in {"REJECTED", "ARCHIVED"}:
            raise ValueError(f"{hypothesis_status}_HYPOTHESIS_CANNOT_BE_PROMOTED")
        if hypothesis_status != "VALIDATED_RESEARCH" and status in {"CANDIDATE", "APPROVED_FOR_LITE_REVIEW", "APPROVED_FOR_LITE_IMPLEMENTATION"}:
            raise ValueError("PROMOTION_REQUIRES_RESEARCH_HYPOTHESIS_VALIDATED_RESEARCH")
    if status in {"APPROVED_FOR_LITE_REVIEW", "APPROVED_FOR_LITE_IMPLEMENTATION"} and source_status != "VALIDATED_RESEARCH":
        raise ValueError(f"PROMOTION_REQUIRES_VALIDATED_RESEARCH:source_status={source_status}")
    if status == "APPROVED_FOR_LITE_IMPLEMENTATION" and not approved_by_human:
        raise ValueError("APPROVED_FOR_LITE_IMPLEMENTATION_REQUIRES_APPROVED_BY_HUMAN")
    payload = {
        "schema_id": "research_to_lite_promotion",
        "schema_version": "v1",
        "artifact_id": "research_to_lite_promotion_v1",
        "promotion_id": promotion_id,
        "research_id": research_id,
        "hypothesis_id": hypothesis_id,
        "proposed_lite_component_type": _enum(
            proposed_lite_component_type,
            PROMOTION_COMPONENT_TYPES,
            "proposed_lite_component_type",
        ),
        "promotion_status": status,
        "evidence_packet_refs": evidence_packet_refs,
        "result_ledger_refs": result_ledger_refs,
        "validation_summary": validation_summary,
        "regime_evidence": regime_evidence,
        "expectancy_evidence": expectancy_evidence,
        "failure_mode_review": failure_mode_review,
        "governance_compatibility_review": governance_compatibility_review,
        "risk_contract_review": risk_contract_review,
        "stop_logic_review": stop_logic_review,
        "manual_execution_compatibility": manual_execution_compatibility,
        "operator_clarity_review": operator_clarity_review,
        "implementation_notes": implementation_notes,
        "required_tests": _strings(required_tests),
        "approval_reason_codes": _strings(approval_reason_codes),
        "rejection_reason_codes": _strings(rejection_reason_codes),
        "data_snapshot_refs": _strings(data_snapshot_refs),
        "methodology_version": methodology_version,
        "code_version": code_version,
        "artifact_lineage": _objects(artifact_lineage or []),
        "reason_codes": _strings(reason_codes) or _strings(approval_reason_codes) or _strings(rejection_reason_codes),
        "reproducibility_notes": reproducibility_notes,
        "approved_by_human": bool(approved_by_human),
        "created_at_utc": created_at_utc,
        "reviewed_at_utc": reviewed_at_utc,
        "source_research_status": source_status,
        "eligible_for_lite_implementation": status == "APPROVED_FOR_LITE_IMPLEMENTATION" and bool(approved_by_human),
        "advisory_governance_evidence_only": True,
        "runtime_mutation_allowed": False,
        "automatic_lite_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    validate_research_promotion_lineage_v1(payload)
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_lab_index_v1(
    *,
    generated_at_utc: str,
    research_items: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "schema_id": "research_lab_index",
        "schema_version": "v1",
        "artifact_id": "research_lab_index_v1",
        "generated_at_utc": generated_at_utc,
        "research_items": [_research_index_item(row) for row in research_items],
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_edge_taxonomy_v1(
    *,
    taxonomy_id: str,
    generated_at_utc: str,
    market_theses: list[str],
    edge_families: list[str],
    edge_clusters: list[str],
    trade_expressions: list[str],
    deprecated_terms: list[str],
    naming_rules: list[str],
) -> dict[str, Any]:
    duplicate_warnings = _duplicate_term_warnings(
        market_theses=market_theses,
        edge_families=edge_families,
        edge_clusters=edge_clusters,
        trade_expressions=trade_expressions,
        deprecated_terms=deprecated_terms,
    )
    payload = {
        "schema_id": "edge_taxonomy",
        "schema_version": "v1",
        "artifact_id": "edge_taxonomy_v1",
        "taxonomy_id": taxonomy_id,
        "generated_at_utc": generated_at_utc,
        "market_theses": _strings(market_theses),
        "edge_families": _strings(edge_families),
        "edge_clusters": _strings(edge_clusters),
        "trade_expressions": _strings(trade_expressions),
        "deprecated_terms": _strings(deprecated_terms),
        "naming_rules": _strings(naming_rules),
        "duplicate_term_warnings": duplicate_warnings,
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_research_architecture_integrity_review_v1(
    *,
    review_id: str,
    generated_at_utc: str,
    hypotheses: list[dict[str, Any]] | None = None,
    task_queues: list[dict[str, Any]] | None = None,
    evidence_packets: list[dict[str, Any]] | None = None,
    result_ledgers: list[dict[str, Any]] | None = None,
    conclusions: list[dict[str, Any]] | None = None,
    promotions: list[dict[str, Any]] | None = None,
    knowledge_graphs: list[dict[str, Any]] | None = None,
    taxonomy: dict[str, Any] | None = None,
    legacy_registries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    findings: list[dict[str, str]] = []
    blockers: list[str] = []
    warnings: list[str] = []
    hypothesis_by_id = {str(row.get("hypothesis_id") or ""): row for row in _objects(hypotheses or [])}
    for registry in _objects(legacy_registries or []):
        if _objects(registry.get("hypotheses")):
            warnings.append("LEGACY_HYPOTHESIS_REGISTRY_PRESENT_COMPATIBILITY_ONLY")
            findings.append({"severity": "WARN", "code": "LEGACY_HYPOTHESIS_REGISTRY_PRESENT", "detail": "hypothesis_registry.v1 must not be canonical for new work."})
    if taxonomy:
        for warning in taxonomy_warnings_for_research_hypotheses_v1(taxonomy=taxonomy, hypotheses=list(hypothesis_by_id.values())):
            warnings.append(warning)
            findings.append({"severity": "WARN", "code": "TAXONOMY_WARNING", "detail": warning})
    for packet in _objects(evidence_packets or []):
        if packet.get("runtime_mutation_allowed") is not False or packet.get("broker_submit_required") is not False:
            blockers.append("UNSAFE_EVIDENCE_RUNTIME_OR_BROKER_FLAG")
        for field in ("hypothesis_id", "created_at_utc", "artifact_lineage", "methodology_version", "code_version", "reproducibility_notes"):
            if not packet.get(field):
                blockers.append(f"EVIDENCE_MISSING_LINEAGE:{field}:{packet.get('research_id')}")
    for ledger in _objects(result_ledgers or []):
        for result in _objects(ledger.get("results")):
            for field in ("hypothesis_id", "task_id", "created_at_utc", "methodology_version", "code_version", "artifact_lineage", "reason_codes", "reproducibility_notes"):
                if not result.get(field):
                    blockers.append(f"RESULT_MISSING_LINEAGE:{field}:{result.get('result_id')}")
            if str(result.get("result_status") or "") in {"INVALIDATED", "CONTRADICTS_HYPOTHESIS"} and str(result.get("promotion_recommendation") or "") == "PROMOTION_CANDIDATE":
                blockers.append(f"INVALIDATED_RESULT_RECOMMENDS_PROMOTION:{result.get('result_id')}")
    for conclusion in _objects(conclusions or []):
        try:
            validate_research_conclusion_lineage_v1(conclusion)
        except ValueError as exc:
            blockers.append(str(exc))
        if conclusion.get("runtime_mutation_allowed") is not False:
            blockers.append(f"CONCLUSION_RUNTIME_MUTATION_ALLOWED:{conclusion.get('conclusion_id')}")
    for promotion in _objects(promotions or []):
        try:
            validate_research_promotion_lineage_v1(promotion)
        except ValueError as exc:
            blockers.append(str(exc))
        hyp = hypothesis_by_id.get(str(promotion.get("hypothesis_id") or ""))
        if hyp and str(hyp.get("status") or "") in {"REJECTED", "ARCHIVED"}:
            blockers.append(f"PROMOTION_REFERENCES_NON_PROMOTABLE_HYPOTHESIS:{hyp.get('hypothesis_id')}")
        if promotion.get("runtime_mutation_allowed") is not False or promotion.get("broker_submit_required") is not False:
            blockers.append(f"PROMOTION_RUNTIME_OR_BROKER_COUPLING:{promotion.get('promotion_id')}")
    for graph in _objects(knowledge_graphs or []):
        if graph.get("non_authoritative_scaffold") is not True:
            blockers.append("KNOWLEDGE_GRAPH_NOT_MARKED_NON_AUTHORITATIVE")
        if graph.get("runtime_mutation_allowed") is not False:
            blockers.append("KNOWLEDGE_GRAPH_RUNTIME_MUTATION_ALLOWED")
    for queue in _objects(task_queues or []):
        for task in _objects(queue.get("tasks")):
            if str(task.get("status") or "") == "QUEUED" and not _strings(task.get("required_inputs")):
                warnings.append(f"QUEUED_TASK_WITHOUT_REQUIRED_INPUTS:{task.get('task_id')}")
    for code in sorted(set(blockers)):
        findings.append({"severity": "BLOCKER", "code": code.split(":", 1)[0], "detail": code})
    payload = {
        "schema_id": "research_architecture_integrity_review",
        "schema_version": "v1",
        "artifact_id": "research_architecture_integrity_review_v1",
        "review_id": review_id,
        "generated_at_utc": generated_at_utc,
        "review_status": "FAIL" if blockers else ("WARN" if warnings else "PASS"),
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "findings": findings,
        "checked_artifact_counts": {
            "hypotheses": len(_objects(hypotheses or [])),
            "task_queues": len(_objects(task_queues or [])),
            "evidence_packets": len(_objects(evidence_packets or [])),
            "result_ledgers": len(_objects(result_ledgers or [])),
            "conclusions": len(_objects(conclusions or [])),
            "promotions": len(_objects(promotions or [])),
            "knowledge_graphs": len(_objects(knowledge_graphs or [])),
            "legacy_registries": len(_objects(legacy_registries or [])),
        },
        "research_lab_only": True,
        "runtime_mutation_allowed": False,
        "trade_authorization_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _research_index_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "research_id": str(row.get("research_id") or ""),
        "title": str(row.get("title") or ""),
        "research_type": _enum(str(row.get("research_type") or "EDGE"), RESEARCH_TYPES, "research_type"),
        "research_status": _enum(str(row.get("research_status") or "DRAFT"), RESEARCH_STATUSES, "research_status"),
        "latest_evidence_packet": str(row.get("latest_evidence_packet") or ""),
        "latest_promotion_status": _enum(
            str(row.get("latest_promotion_status") or "NOT_READY"),
            PROMOTION_STATUSES,
            "latest_promotion_status",
        ),
        "related_edge_family": str(row.get("related_edge_family") or ""),
        "related_sleeves": _strings(row.get("related_sleeves")),
        "archived": bool(row.get("archived", False)),
        "operator_notes": str(row.get("operator_notes") or ""),
    }


def _duplicate_term_warnings(
    *,
    market_theses: list[str],
    edge_families: list[str],
    edge_clusters: list[str],
    trade_expressions: list[str],
    deprecated_terms: list[str],
) -> list[str]:
    sections = {
        "market_theses": _strings(market_theses),
        "edge_families": _strings(edge_families),
        "edge_clusters": _strings(edge_clusters),
        "trade_expressions": _strings(trade_expressions),
    }
    deprecated = {_norm(term) for term in deprecated_terms}
    seen: dict[str, list[str]] = {}
    for section, values in sections.items():
        for value in values:
            seen.setdefault(_norm(value), []).append(section)
    warnings: list[str] = []
    for term, locations in sorted(seen.items()):
        if len(locations) > 1:
            warnings.append(f"DUPLICATE_TERM:{term}:{','.join(sorted(locations))}")
        if term in deprecated:
            warnings.append(f"DEPRECATED_TERM_USED:{term}")
    return warnings


def _enum(value: str, allowed: set[str], field_name: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise ValueError(f"INVALID_{field_name.upper()}:{normalized}")
    return normalized


def _enum_lc(value: str, allowed: set[str], field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in allowed:
        raise ValueError(f"INVALID_{field_name.upper()}:{normalized}")
    return normalized


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    item = str(value).strip()
    return [item] if item else []


def _objects(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _confidence_rank(value: str) -> int:
    order = {"NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}
    return order.get(str(value or "").strip().upper(), 1)


def _confidence_label(rank: int) -> str:
    labels = {0: "NONE", 1: "LOW", 2: "MEDIUM", 3: "HIGH"}
    return labels[max(0, min(3, int(rank)))]


def _bounded_confidence_after(*, before_confidence: str, requested_after: str, result_status: str) -> str:
    before = _confidence_rank(before_confidence)
    requested = str(requested_after or "").strip().upper()
    if requested in {"NONE", "LOW", "MEDIUM", "HIGH"}:
        target = _confidence_rank(requested)
    elif result_status == "SUPPORTS_HYPOTHESIS":
        target = before + 1
    elif result_status in {"WEAK_SUPPORT", "NEEDS_MORE_RESEARCH", "INSUFFICIENT_DATA"}:
        target = before
    else:
        target = before - 1
    return _confidence_label(max(before - 1, min(before + 1, target)))


def _confidence_change(before: str, after: str) -> str:
    delta = _confidence_rank(after) - _confidence_rank(before)
    if delta > 0:
        return f"+{delta}"
    return str(delta)


def _norm(value: str) -> str:
    return "_".join(str(value).strip().upper().replace("-", "_").split())


def _safe_id(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    return cleaned or "research_lab"


def _hypothesis_id(*, title: str, edge_family: str, created_at_utc: str, ordinal: int) -> str:
    slug = _safe_id(f"{edge_family}-{title}")[:48].strip("_-.").lower()
    day = "".join(ch for ch in str(created_at_utc)[:10] if ch.isdigit()) or "00000000"
    return f"hyp-{day}-{ordinal:04d}-{slug or 'hypothesis'}"


def _keyword_set(value: str) -> set[str]:
    stop = {"the", "a", "an", "and", "or", "when", "with", "over", "next", "into", "from", "for"}
    words = {
        "".join(ch for ch in token.lower() if ch.isalnum())
        for token in str(value or "").replace("+", " ").replace("/", " ").split()
    }
    return {word for word in words if len(word) >= 4 and word not in stop}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / len(left.union(right))


def _stage_list(value: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in _strings(value):
        stage = _enum_lc(item, set(TEST_STAGES), "test_stage")
        if stage in seen:
            continue
        seen.add(stage)
        out.append(stage)
    return out


def _task_type_for_stage(stage: str) -> str:
    stage = str(stage or "")
    return STAGE_TASK_TYPES.get(stage, "retest")


def _next_stage(completed: list[str]) -> str:
    for stage in TEST_STAGES:
        if stage not in completed:
            return stage
    return ""


def _lifecycle_for_result(result_status: str, *, next_stage_hint: str = "") -> str:
    if result_status == "validated_candidate":
        return "validated_candidate"
    if result_status in {"weak", "rejected"}:
        return "rejected"
    if result_status == "insufficient_data":
        return "insufficient_data"
    if result_status == "regime_dependent":
        return "regime_dependent"
    if result_status in {"overlapping", "duplicate"}:
        return "duplicate_of_existing_edge"
    return STAGE_LIFECYCLE_STATES.get(next_stage_hint, "exploratory_testing")


def _terminal_for_result(result_status: str) -> str:
    mapping = {
        "weak": "rejected",
        "rejected": "rejected",
        "insufficient_data": "insufficient_data",
        "regime_dependent": "regime_dependent",
        "overlapping": "duplicate_of_existing_edge",
        "duplicate": "duplicate_of_existing_edge",
    }
    return mapping.get(result_status, "")


def _promoted_sleeve(row: dict[str, Any]) -> dict[str, Any]:
    required = [
        "sleeve_id",
        "source_hypothesis_id",
        "edge_family",
        "behavioral_thesis",
        "regime_fit",
        "instrument_universe",
        "entry_logic",
        "exit_logic",
        "stop_logic",
        "sizing_logic",
        "invalidation_logic",
        "known_failure_modes",
        "overlap_tags",
        "promotion_evidence_path",
        "production_status",
        "created_at",
        "updated_at",
    ]
    missing = [field for field in required if not row.get(field)]
    if missing:
        raise ValueError(f"PROMOTED_SLEEVE_MISSING_REQUIRED_FIELDS:{','.join(missing)}")
    if str(row.get("promotion_status") or "promoted").lower() != "promoted":
        raise ValueError("SLEEVE_LIBRARY_REQUIRES_PROMOTED_SLEEVES")
    return {
        "sleeve_id": str(row["sleeve_id"]),
        "source_hypothesis_id": str(row["source_hypothesis_id"]),
        "edge_family": str(row["edge_family"]),
        "behavioral_thesis": str(row["behavioral_thesis"]),
        "regime_fit": _strings(row["regime_fit"]),
        "instrument_universe": _strings(row["instrument_universe"]),
        "entry_logic": str(row["entry_logic"]),
        "exit_logic": str(row["exit_logic"]),
        "stop_logic": str(row["stop_logic"]),
        "sizing_logic": str(row["sizing_logic"]),
        "invalidation_logic": str(row["invalidation_logic"]),
        "known_failure_modes": _strings(row["known_failure_modes"]),
        "overlap_tags": _strings(row["overlap_tags"]),
        "promotion_evidence_path": str(row["promotion_evidence_path"]),
        "research_hypothesis_id": str(row.get("research_hypothesis_id") or row["source_hypothesis_id"]),
        "promotion_status": "promoted",
        "sleeve_name": str(row.get("sleeve_name") or row["sleeve_id"]),
        "approved_by_human": bool(row.get("approved_by_human", True)),
        "approved_for_lite_implementation": bool(row.get("approved_for_lite_implementation", True)),
        "approved_edge_families": _strings(row.get("approved_edge_families") or [row["edge_family"]]),
        "approved_trade_classes": _strings(row.get("approved_trade_classes") or row.get("instrument_universe")),
        "expected_regimes": _strings(row.get("expected_regimes") or row.get("regime_fit")),
        "operational_constraints": _strings(row.get("operational_constraints")),
        "archived": bool(row.get("archived", False)),
        "human_approval_status": str(row.get("human_approval_status") or "approved"),
        "implementation_status": str(row.get("implementation_status") or "approved"),
        "production_status": str(row["production_status"]),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _manual_trade_candidate(
    row: dict[str, Any],
    *,
    run_id: str,
    date: str,
    regime_state: str,
    promoted_sources: dict[str, str],
) -> dict[str, Any]:
    required = [
        "sleeve_id",
        "source_hypothesis_id",
        "symbol",
        "side",
        "instrument_type",
        "entry_reference_price",
        "quantity_or_sizing_guidance",
        "stop_price",
        "stop_logic",
        "risk_per_trade",
    ]
    missing = [field for field in required if row.get(field) is None or row.get(field) == "" or row.get(field) == []]
    sleeve_id = str(row.get("sleeve_id") or "")
    source_hypothesis_id = str(row.get("source_hypothesis_id") or row.get("hypothesis_id") or "")
    source_blockers: list[str] = []
    if not promoted_sources:
        source_blockers.append("PROMOTED_SLEEVE_LIBRARY_MISSING")
    elif promoted_sources.get(sleeve_id) != source_hypothesis_id:
        source_blockers.append("UNPROMOTED_SLEEVE_SOURCE")
    actionable = not missing and not source_blockers
    checklist = _strings(row.get("manual_execution_checklist")) or [
        "Review status and blockers before acting.",
        "Enter the position manually in IB paper.",
        "Immediately enter the protective stop.",
        "Confirm the stop is accepted.",
        "Record the manual execution receipt.",
    ]
    return {
        "run_id": run_id,
        "date": date,
        "recommended_trade_id": str(row.get("recommended_trade_id") or row.get("candidate_id") or _safe_id(f"{run_id}-{row.get('sleeve_id')}-{row.get('symbol')}")),
        "sleeve_id": sleeve_id,
        "source_hypothesis_id": source_hypothesis_id,
        "symbol": str(row.get("symbol") or "").upper(),
        "side": str(row.get("side") or row.get("direction") or "").upper(),
        "instrument_type": str(row.get("instrument_type") or ""),
        "entry_reference_price": str(row.get("entry_reference_price") or ""),
        "order_type_suggestion": str(row.get("order_type_suggestion") or "MANUAL_LIMIT_OR_MARKET_BY_OPERATOR"),
        "quantity_or_sizing_guidance": str(row.get("quantity_or_sizing_guidance") or ""),
        "stop_price": str(row.get("stop_price") or ""),
        "stop_logic": str(row.get("stop_logic") or ""),
        "risk_per_trade": str(row.get("risk_per_trade") or ""),
        "edge_family": str(row.get("edge_family") or ""),
        "regime_state": str(row.get("regime_state") or regime_state),
        "confidence": str(row.get("confidence") or ""),
        "inclusion_reason": str(row.get("inclusion_reason") or ""),
        "exclusion_reason": str(row.get("exclusion_reason") or ("MISSING_REQUIRED_MANUAL_TRADE_FIELDS:" + ",".join(missing) if missing else "")),
        "governance_notes": str(row.get("governance_notes") or ""),
        "edge_overlap_result": str(row.get("edge_overlap_result") or ""),
        "manual_execution_checklist": checklist,
        "actionable": actionable,
        "do_not_trade_blockers": [f"MISSING_{field.upper()}" for field in missing] + source_blockers,
    }


def _outcome_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_id": str(row.get("trade_id") or row.get("recommended_trade_id") or ""),
        "alert_id": str(row.get("alert_id") or ""),
        "source_packet_type": str(row.get("source_packet_type") or "EOD_MANUAL_PACKET"),
        "event_id": str(row.get("event_id") or ""),
        "event_type": str(row.get("event_type") or ""),
        "alert_gate_status": str(row.get("alert_gate_status") or ""),
        "execution_sensitivity": str(row.get("execution_sensitivity") or ""),
        "validity_gate_status": str(row.get("validity_gate_status") or ""),
        "valid_until_respected": bool(row.get("valid_until_respected", False)),
        "entry_slippage_respected": bool(row.get("entry_slippage_respected", False)),
        "operator_action_taken": str(row.get("operator_action_taken") or ""),
        "sleeve_id": str(row.get("sleeve_id") or ""),
        "hypothesis_id": str(row.get("hypothesis_id") or row.get("source_hypothesis_id") or ""),
        "recommended_entry": str(row.get("recommended_entry") or ""),
        "actual_entry": str(row.get("actual_entry") or ""),
        "recommended_stop": str(row.get("recommended_stop") or ""),
        "actual_stop": str(row.get("actual_stop") or ""),
        "exit_price": str(row.get("exit_price") or ""),
        "return_pct": str(row.get("return_pct") or ""),
        "risk_adjusted_return": str(row.get("risk_adjusted_return") or ""),
        "max_adverse_excursion": str(row.get("max_adverse_excursion") or ""),
        "max_favorable_excursion": str(row.get("max_favorable_excursion") or ""),
        "outcome_status": str(row.get("outcome_status") or row.get("actual_trade_outcome") or ""),
        "failure_reason": str(row.get("failure_reason") or ""),
        "operator_deviation": str(row.get("operator_deviation") or ""),
        "notes": str(row.get("notes") or ""),
        "sleeve_attribution": str(row.get("sleeve_attribution") or ""),
        "edge_overlap_attribution": str(row.get("edge_overlap_attribution") or ""),
    }


def _promoted_sleeve_sources(promoted_sleeve_library: dict[str, Any] | None) -> dict[str, str]:
    if not promoted_sleeve_library:
        return {}
    sources: dict[str, str] = {}
    for row in _objects(promoted_sleeve_library.get("sleeves")):
        if str(row.get("promotion_status") or "").lower() != "promoted":
            continue
        sleeve_id = str(row.get("sleeve_id") or "")
        source_hypothesis_id = str(row.get("source_hypothesis_id") or row.get("research_hypothesis_id") or "")
        if sleeve_id and source_hypothesis_id:
            sources[sleeve_id] = source_hypothesis_id
    return sources


def _evidence_positive(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    negative_terms = {"negative", "not_computed", "missing", "none", "failed", "fail", "weak", "insufficient"}
    if any(term in normalized for term in negative_terms):
        return False
    positive_terms = {"positive", "pass", "passed", "supported", "profitable", "favorable"}
    return any(term in normalized for term in positive_terms)
