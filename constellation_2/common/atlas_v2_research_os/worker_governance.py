from __future__ import annotations

from typing import Any

from .artifact_models import EvidenceLevel, FORBIDDEN_ARTIFACT_TYPES
from .governance import GovernanceError, validate_artifact_allowed
from .worker_models import WorkerGovernanceResult, WorkerLineageResult

EVIDENCE_RANK = {
    EvidenceLevel.GENERATED_ONLY.value: 0,
    EvidenceLevel.MOCK_ONLY.value: 1,
    EvidenceLevel.HISTORICAL_REPLAY.value: 2,
    EvidenceLevel.PAPER_FORWARD_OBSERVATION.value: 3,
    EvidenceLevel.OPERATOR_APPROVED.value: 4,
    EvidenceLevel.EXTERNALLY_VALIDATED.value: 5,
}

AUTHORITY_METADATA_FLAGS = {
    "authorizes_candidate_use",
    "authorizes_capital_use",
    "candidate_generation_authority",
    "capital_authority",
    "trading_authority",
    "can_influence_candidate_generation",
}


def validate_worker_output_governance(input_artifacts: list[dict[str, Any]], output_artifacts: list[dict[str, Any]]) -> WorkerGovernanceResult:
    violations: list[str] = []
    warnings: list[str] = []
    for output in output_artifacts:
        artifact_id = str(output.get("artifact_id", "<missing>"))
        artifact_type = str(output.get("artifact_type", ""))
        if artifact_type in FORBIDDEN_ARTIFACT_TYPES:
            violations.append(f"{artifact_id}: forbidden artifact type {artifact_type}")
        try:
            validate_artifact_allowed(output)
        except GovernanceError as exc:
            violations.append(f"{artifact_id}: {exc}")
        if _escalates_evidence(input_artifacts, output):
            violations.append(f"{artifact_id}: worker output cannot escalate evidence maturity")
        if _marks_generated_learning_external(output):
            violations.append(f"{artifact_id}: generated learning cannot be externally validated")
        if _authorizes_candidate_or_capital_use(output):
            violations.append(f"{artifact_id}: worker output cannot authorize candidate/capital use")
    return WorkerGovernanceResult(status="PASS" if not violations else "FAIL", violations=violations, warnings=warnings)


def validate_worker_lineage(input_artifacts: list[dict[str, Any]], output_artifacts: list[dict[str, Any]]) -> WorkerLineageResult:
    input_ids = [str(item.get("artifact_id", "")) for item in input_artifacts if item.get("artifact_id")]
    output_ids = [str(item.get("artifact_id", "")) for item in output_artifacts if item.get("artifact_id")]
    violations: list[str] = []
    if output_artifacts and not input_ids:
        violations.append("non-root worker output requires input artifact lineage")
    for output in output_artifacts:
        artifact_id = str(output.get("artifact_id", "<missing>"))
        source_ids = [str(item) for item in output.get("source_artifact_ids", [])]
        if input_ids and not source_ids:
            violations.append(f"{artifact_id}: output must preserve source_artifact_ids")
        missing = sorted(set(input_ids) - set(source_ids))
        if missing:
            violations.append(f"{artifact_id}: output lineage missing input ids {missing}")
    return WorkerLineageResult(status="PASS" if not violations else "FAIL", input_artifact_ids=input_ids, output_artifact_ids=output_ids, violations=violations)


def audit_worker_outputs(input_artifacts: list[dict[str, Any]], output_artifacts: list[dict[str, Any]]) -> tuple[WorkerGovernanceResult, WorkerLineageResult]:
    return validate_worker_output_governance(input_artifacts, output_artifacts), validate_worker_lineage(input_artifacts, output_artifacts)


def _escalates_evidence(input_artifacts: list[dict[str, Any]], output: dict[str, Any]) -> bool:
    if not input_artifacts:
        return False
    source_levels = [str(item.get("evidence_level", EvidenceLevel.GENERATED_ONLY.value)) for item in input_artifacts]
    strongest_source = max(EVIDENCE_RANK.get(level, -1) for level in source_levels)
    output_rank = EVIDENCE_RANK.get(str(output.get("evidence_level", EvidenceLevel.GENERATED_ONLY.value)), -1)
    return output_rank > strongest_source


def _marks_generated_learning_external(output: dict[str, Any]) -> bool:
    artifact_type = str(output.get("artifact_type", ""))
    evidence = str(output.get("evidence_level", ""))
    labels = {str(label).lower() for label in output.get("labels", [])}
    metadata = output.get("metadata", {}) or {}
    source_levels = {str(item) for item in metadata.get("source_evidence_levels", [])}
    generated_marker = "generated" in labels or "generated_only" in labels or EvidenceLevel.GENERATED_ONLY.value in source_levels
    return artifact_type == "LearningEstimate" and generated_marker and evidence == EvidenceLevel.EXTERNALLY_VALIDATED.value


def _authorizes_candidate_or_capital_use(output: dict[str, Any]) -> bool:
    metadata = output.get("metadata", {}) or {}
    if any(metadata.get(flag) is True for flag in AUTHORITY_METADATA_FLAGS):
        return True
    text = str(output).lower()
    forbidden_phrases = ["authorize candidate", "authorize capital", "candidate use authorized", "capital use authorized"]
    return any(phrase in text for phrase in forbidden_phrases)


def validate_worker_connection(worker: Any) -> WorkerGovernanceResult:
    from .worker_connection_rules import is_side_effect_allowed, is_worker_connection_allowed

    violations: list[str] = []
    warnings: list[str] = []
    if not is_worker_connection_allowed(worker):
        violations.append(f"{getattr(worker, 'worker_id', '<missing>')}: worker connection is not allowed")
    for output_type in list(getattr(worker, "supported_output_artifact_types", []) or []):
        if not is_output_type_allowed_for_connection(output_type):
            violations.append(f"{getattr(worker, 'worker_id', '<missing>')}: output artifact type not allowed: {output_type}")
    for effect in ["generate_research_artifact", "artifact_store_write", "write_worker_run_record"]:
        if not is_side_effect_allowed(effect):
            violations.append(f"{getattr(worker, 'worker_id', '<missing>')}: side effect not allowed: {effect}")
    return WorkerGovernanceResult(status="PASS" if not violations else "FAIL", violations=violations, warnings=warnings)


def validate_worker_execution(worker: Any, result: Any) -> WorkerGovernanceResult:
    payload = result.to_dict() if hasattr(result, "to_dict") else dict(result)
    violations: list[str] = []
    connection = validate_worker_connection(worker)
    violations.extend(connection.violations)
    if payload.get("governance_result", {}).get("status") == "FAIL":
        violations.extend(payload.get("governance_result", {}).get("violations", []) or ["worker execution governance failed"])
    if payload.get("lineage_result", {}).get("status") == "FAIL" and payload.get("output_artifact_ids"):
        violations.extend(payload.get("lineage_result", {}).get("violations", []) or ["worker execution lineage failed"])
    if str(payload.get("status")) == "COMPLETED" and not payload.get("output_artifact_ids"):
        violations.append("completed connected worker execution must produce output artifacts")
    return WorkerGovernanceResult(status="PASS" if not violations else "FAIL", violations=violations, warnings=list(connection.warnings))


def validate_connected_worker_outputs(output_artifacts: list[dict[str, Any]], input_artifacts: list[dict[str, Any]] | None = None) -> WorkerGovernanceResult:
    violations: list[str] = []
    for output in output_artifacts:
        if not is_output_type_allowed_for_connection(str(output.get("artifact_type", ""))):
            violations.append(f"{output.get('artifact_id', '<missing>')}: connected worker output type is not allowed")
    governance_result, lineage_result = audit_worker_outputs(input_artifacts or [], output_artifacts)
    violations.extend(governance_result.violations)
    violations.extend(lineage_result.violations)
    return WorkerGovernanceResult(status="PASS" if not violations else "FAIL", violations=violations, warnings=list(governance_result.warnings))


def validate_connected_worker_no_authority_escalation(payloads: list[dict[str, Any]] | dict[str, Any]) -> WorkerGovernanceResult:
    rows = payloads if isinstance(payloads, list) else [payloads]
    violations: list[str] = []
    forbidden_markers = [
        "promote candidate",
        "authorize capital",
        "recommend trade",
        "deploy sleeve",
        "construct portfolio",
        "size position",
        "broker execution",
        "live trading",
    ]
    for row in rows:
        metadata = row.get("metadata", {}) if isinstance(row, dict) else {}
        if isinstance(metadata, dict) and any(metadata.get(flag) is True for flag in AUTHORITY_METADATA_FLAGS):
            violations.append(f"{row.get('artifact_id', '<missing>')}: authority metadata flag set")
        text = str(row).lower().replace("_", " ").replace("-", " ")
        for marker in forbidden_markers:
            if marker in text:
                violations.append(f"{row.get('artifact_id', '<missing>')}: authority escalation marker present: {marker}")
                break
    return WorkerGovernanceResult(status="PASS" if not violations else "FAIL", violations=violations, warnings=[])


def is_output_type_allowed_for_connection(artifact_type: str) -> bool:
    from .worker_connection_rules import is_output_artifact_allowed

    return is_output_artifact_allowed(artifact_type)
