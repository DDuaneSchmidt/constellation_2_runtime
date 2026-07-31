from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class WorkerType(str, Enum):
    CLAIM = "ClaimWorker"
    HYPOTHESIS = "HypothesisWorker"
    EXPERIMENT_DESIGN = "ExperimentDesignWorker"
    EXPERIMENT_EXECUTION = "ExperimentExecutionWorker"
    LEARNING = "LearningWorker"
    EVALUATION = "EvaluationWorker"
    ATTENTION = "AttentionWorker"
    MEMORY_CURATOR = "MemoryCuratorWorker"


class WorkerRunStatus(str, Enum):
    NOT_CONNECTED = "NOT_CONNECTED"
    DESIGN_READY = "DESIGN_READY"
    CONNECTED_READ_ONLY = "CONNECTED_READ_ONLY"
    CONNECTED_RESEARCH_ONLY = "CONNECTED_RESEARCH_ONLY"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    DRY_RUN = "DRY_RUN"
    COMPLETED = "COMPLETED"


@dataclass(frozen=True)
class WorkerGovernanceResult:
    status: str
    violations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WorkerLineageResult:
    status: str
    input_artifact_ids: list[str] = field(default_factory=list)
    output_artifact_ids: list[str] = field(default_factory=list)
    violations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WorkerRunResult:
    worker_run_id: str
    worker_id: str
    started_at: str
    completed_at: str
    status: str
    input_artifact_ids: list[str] = field(default_factory=list)
    output_artifact_ids: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    governance_result: dict[str, Any] = field(default_factory=dict)
    lineage_result: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WorkerAdapterMetadata:
    worker_id: str
    worker_type: str
    adapter_status: str
    supported_input_artifact_types: list[str]
    supported_output_artifact_types: list[str]
    source_component: str
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
