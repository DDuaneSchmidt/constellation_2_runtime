from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class SchedulerTriggerType(str, Enum):
    MANUAL = "MANUAL"
    HOURLY = "HOURLY"
    NEW_BACKLOG_ITEM = "NEW_BACKLOG_ITEM"
    NEW_MEMORY_SIGNAL = "NEW_MEMORY_SIGNAL"
    NEW_FAILURE_PATTERN = "NEW_FAILURE_PATTERN"
    CANDIDATE_QUALITY_REGRESSION = "CANDIDATE_QUALITY_REGRESSION"
    RESEARCH_OS_CERTIFICATION_FAILURE = "RESEARCH_OS_CERTIFICATION_FAILURE"


class SchedulerTriggerStatus(str, Enum):
    TRIGGER_ACCEPTED = "TRIGGER_ACCEPTED"
    TRIGGER_SKIPPED = "TRIGGER_SKIPPED"
    TRIGGER_BLOCKED = "TRIGGER_BLOCKED"
    EXECUTION_STARTED = "EXECUTION_STARTED"
    EXECUTION_COMPLETED = "EXECUTION_COMPLETED"


@dataclass(frozen=True)
class SchedulerTrigger:
    trigger_id: str
    trigger_type: str
    trigger_key: str
    created_at: str
    status: str
    source: dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SchedulerExecutionRecord:
    scheduler_run_id: str
    trigger_id: str
    trigger_type: str
    started_at: str
    completed_at: str
    status: str
    bounded_execution_id: str = ""
    bounded_execution_status: str = ""
    lock_id: str = ""
    governance_result: dict[str, Any] = field(default_factory=dict)
    certification_result: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
