from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class AutonomousResearchExecutionStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    DRY_RUN_COMPLETED = "DRY_RUN_COMPLETED"
    FAILED = "FAILED"
    FAILED_SAFETY_GATE = "FAILED_SAFETY_GATE"
    SKIPPED_NO_READY_BACKLOG = "SKIPPED_NO_READY_BACKLOG"
    SKIPPED_NO_COMPATIBLE_WORKER = "SKIPPED_NO_COMPATIBLE_WORKER"
    SKIPPED_DEPENDENCY_MISSING = "SKIPPED_DEPENDENCY_MISSING"


@dataclass(frozen=True)
class AutonomousResearchSafetyGateResult:
    gate_id: str
    result: str
    details: list[str] = field(default_factory=list)
    evidence_refs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AutonomousResearchExecutionStep:
    step_id: str
    status: str
    started_at: str
    completed_at: str
    details: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AutonomousResearchExecutionRun:
    execution_id: str
    created_at: str
    started_at: str
    completed_at: str
    status: str
    selected_backlog_item_id: str = ""
    selected_worker_id: str = ""
    input_artifact_ids: list[str] = field(default_factory=list)
    output_artifact_ids: list[str] = field(default_factory=list)
    memory_updates: list[dict[str, Any]] = field(default_factory=list)
    backlog_updates: list[dict[str, Any]] = field(default_factory=list)
    certification_result: dict[str, Any] = field(default_factory=dict)
    governance_result: dict[str, Any] = field(default_factory=dict)
    lineage_result: dict[str, Any] = field(default_factory=dict)
    safety_gate_results: list[dict[str, Any]] = field(default_factory=list)
    steps: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AutonomousResearchExecutionResult:
    execution_id: str
    created_at: str
    started_at: str
    completed_at: str
    status: str
    selected_backlog_item_id: str = ""
    selected_worker_id: str = ""
    input_artifact_ids: list[str] = field(default_factory=list)
    output_artifact_ids: list[str] = field(default_factory=list)
    memory_updates: list[dict[str, Any]] = field(default_factory=list)
    backlog_updates: list[dict[str, Any]] = field(default_factory=list)
    certification_result: dict[str, Any] = field(default_factory=dict)
    governance_result: dict[str, Any] = field(default_factory=dict)
    lineage_result: dict[str, Any] = field(default_factory=dict)
    safety_gate_results: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
