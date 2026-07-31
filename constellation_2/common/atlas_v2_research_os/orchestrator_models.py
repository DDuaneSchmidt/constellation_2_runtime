from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class OrchestratorStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    FAILED_SAFETY_GATE = "FAILED_SAFETY_GATE"
    SKIPPED_ALREADY_RUNNING = "SKIPPED_ALREADY_RUNNING"
    DRY_RUN_COMPLETED = "DRY_RUN_COMPLETED"
    AUDIT_ONLY_COMPLETED = "AUDIT_ONLY_COMPLETED"


class TriggerType(str, Enum):
    EVENT_DRIVEN = "EVENT_DRIVEN"
    SCHEDULED = "SCHEDULED"
    MANUAL = "MANUAL"
    CONDITION_WATCH = "CONDITION_WATCH"


class SafetyGateId(str, Enum):
    LABEL_INTEGRITY = "label_integrity"
    AUTHORITY_BOUNDARY = "authority_boundary"
    FORBIDDEN_ARTIFACT_AUDIT = "forbidden_artifact_audit"
    LINEAGE_INTEGRITY = "lineage_integrity"
    EVIDENCE_LEVEL_VALIDATION = "evidence_level_validation"
    CANDIDATE_CAPITAL_ISOLATION = "candidate_capital_isolation"


TERMINAL_STATUSES = {
    OrchestratorStatus.COMPLETED.value,
    OrchestratorStatus.FAILED.value,
    OrchestratorStatus.FAILED_SAFETY_GATE.value,
    OrchestratorStatus.SKIPPED_ALREADY_RUNNING.value,
    OrchestratorStatus.DRY_RUN_COMPLETED.value,
    OrchestratorStatus.AUDIT_ONLY_COMPLETED.value,
}

FORBIDDEN_ARTIFACT_MARKERS = {
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "SleeveDeployment",
    "ProductionCandidatePromotion",
    "PortfolioRecommendation",
    "PositionSizing",
}


@dataclass(frozen=True)
class SafetyGateResult:
    gate_id: str
    gate_version: str = "atlas_v2_research_os_orchestrator_gate_v1"
    result: str = "PASS"
    evidence_refs: list[str] = field(default_factory=list)
    details: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LineageValidationResult:
    validator_version: str = "atlas_v2_research_os_lineage_preflight_v1"
    result: str = "PASS"
    checked_artifacts: list[str] = field(default_factory=list)
    parent_refs: list[str] = field(default_factory=list)
    hash_validation_details: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrchestratorError:
    stage: str
    code: str
    message: str
    recoverable: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrchestratorRunRecord:
    run_id: str
    started_at: str
    completed_at: str | None
    status: str
    trigger_type: str
    trigger_source: dict[str, Any]
    selected_backlog_items: list[dict[str, Any]] = field(default_factory=list)
    workers_invoked: list[dict[str, Any]] = field(default_factory=list)
    artifacts_created: list[dict[str, Any]] = field(default_factory=list)
    safety_gate_results: list[dict[str, Any]] = field(default_factory=list)
    lineage_validation_result: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    skip_reason: str | None = None
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
