from __future__ import annotations

from typing import Any

from .artifact_models import ArtifactType, FORBIDDEN_ARTIFACT_TYPES
from .worker_models import WorkerRunStatus

ALLOWED_CONNECTED_OUTPUT_ARTIFACT_TYPES = {
    ArtifactType.GENERATED_RESEARCH_CLAIM.value,
    ArtifactType.RESEARCH_HYPOTHESIS.value,
    ArtifactType.CHEAP_EXPERIMENT_SPEC.value,
    ArtifactType.EXPERIMENT_RESULT.value,
    ArtifactType.EXPERIENCE_EVENT.value,
    ArtifactType.LEARNING_ESTIMATE.value,
    ArtifactType.LEARNING_ESTIMATE_EVALUATION.value,
    ArtifactType.ATTENTION_SIGNAL.value,
}

FORBIDDEN_CONNECTED_OUTPUT_ARTIFACT_TYPES = set(FORBIDDEN_ARTIFACT_TYPES)

ALLOWED_RESEARCH_SIDE_EFFECTS = {
    "generate_research_artifact",
    "artifact_store_write",
    "update_memory",
    "update_backlog",
    "update_candidate_quality_measurement",
    "write_worker_run_record",
    "write_worker_connection_report",
    "write_worker_execution_report",
}

FORBIDDEN_SIDE_EFFECT_MARKERS = {
    "promote_candidate",
    "authorize_capital",
    "recommend_trade",
    "deploy_sleeve",
    "construct_portfolio",
    "size_position",
    "broker_execution",
    "live_trading",
    "order_routing",
}

CONNECTED_STATUSES = {
    WorkerRunStatus.CONNECTED_READ_ONLY.value,
    WorkerRunStatus.CONNECTED_RESEARCH_ONLY.value,
}


def artifact_type_value(artifact_or_type: Any) -> str:
    if isinstance(artifact_or_type, str):
        return artifact_or_type
    if isinstance(artifact_or_type, dict):
        return str(artifact_or_type.get("artifact_type") or artifact_or_type.get("type") or "")
    value = getattr(artifact_or_type, "artifact_type", None) or getattr(artifact_or_type, "value", None)
    return str(value or "")


def is_output_artifact_allowed(artifact_or_type: Any) -> bool:
    artifact_type = artifact_type_value(artifact_or_type)
    if not artifact_type:
        return False
    if artifact_type in FORBIDDEN_CONNECTED_OUTPUT_ARTIFACT_TYPES:
        return False
    return artifact_type in ALLOWED_CONNECTED_OUTPUT_ARTIFACT_TYPES


def is_side_effect_allowed(side_effect: Any) -> bool:
    name = str(side_effect or "").strip()
    if not name:
        return False
    normalized = name.lower().replace(" ", "_").replace("-", "_")
    if normalized in FORBIDDEN_SIDE_EFFECT_MARKERS:
        return False
    if any(marker in normalized for marker in FORBIDDEN_SIDE_EFFECT_MARKERS):
        return False
    return normalized in ALLOWED_RESEARCH_SIDE_EFFECTS


def is_worker_connection_allowed(worker: Any, *, side_effects: list[str] | None = None) -> bool:
    if worker is None:
        return False
    status = str(getattr(worker, "adapter_status", ""))
    if status not in CONNECTED_STATUSES:
        return False
    outputs = list(getattr(worker, "supported_output_artifact_types", []) or [])
    if not outputs or any(not is_output_artifact_allowed(item) for item in outputs):
        return False
    for effect in side_effects or ["generate_research_artifact", "artifact_store_write", "write_worker_run_record"]:
        if not is_side_effect_allowed(effect):
            return False
    text = " ".join([str(getattr(worker, "worker_id", "")), str(getattr(worker, "source_component", ""))]).lower()
    if any(marker in text for marker in FORBIDDEN_SIDE_EFFECT_MARKERS):
        return False
    return True
