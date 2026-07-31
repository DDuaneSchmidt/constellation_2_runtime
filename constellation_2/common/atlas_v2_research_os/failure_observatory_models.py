from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class FailureSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    GOVERNANCE_BLOCK = "GOVERNANCE_BLOCK"
    CERTIFICATION_BLOCK = "CERTIFICATION_BLOCK"
    FORBIDDEN_ARTIFACT_ATTEMPT = "FORBIDDEN_ARTIFACT_ATTEMPT"


class FailureComponent(str, Enum):
    SCHEDULER = "SCHEDULER"
    ORCHESTRATOR = "ORCHESTRATOR"
    AUTONOMOUS_RESEARCH = "AUTONOMOUS_RESEARCH"
    WORKER = "WORKER"
    CERTIFICATION = "CERTIFICATION"
    GOVERNANCE = "GOVERNANCE"
    LINEAGE = "LINEAGE"
    MEMORY = "MEMORY"
    CANDIDATE_QUALITY = "CANDIDATE_QUALITY"
    PAPER_TRADE_CANDIDATE = "PAPER_TRADE_CANDIDATE"
    PAPER_TRADING_QUEUE = "PAPER_TRADING_QUEUE"
    PAPER_TRADE_OUTCOME = "PAPER_TRADE_OUTCOME"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ResearchOSFailureRecord:
    failure_id: str
    timestamp: str
    component: str
    severity: str
    error_type: str
    error_message: str
    stack_trace: str = ""
    run_id: str = ""
    execution_id: str = ""
    worker_run_id: str = ""
    worker_id: str = ""
    backlog_item_id: str = ""
    artifact_ids: list[str] = field(default_factory=list)
    certification_id: str = ""
    scheduler_trigger_id: str = ""
    recoverable: bool = False
    resolved: bool = False
    resolution_notes: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FailureTrend:
    component: str
    severity: str
    error_type: str
    count: int
    failure_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FailureSummary:
    day: str
    total_failures: int
    critical_failures: int
    governance_blocks: int
    certification_blocks: int
    worker_failures: int
    scheduler_failures: int
    repeated_failure_types: list[dict[str, Any]] = field(default_factory=list)
    unresolved_failures: list[dict[str, Any]] = field(default_factory=list)
    resolved_failures: list[dict[str, Any]] = field(default_factory=list)
    top_components_by_failure_count: list[dict[str, Any]] = field(default_factory=list)
    recommended_human_review_items: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FailureRegistry:
    schema_id: str
    schema_version: str
    root: str
    failure_count: int
    unresolved_count: int
    latest_failure_id: str = ""
    failures: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
