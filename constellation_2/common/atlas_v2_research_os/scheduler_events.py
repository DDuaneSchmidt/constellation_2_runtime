from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from .scheduler_models import SchedulerTrigger, SchedulerTriggerStatus, SchedulerTriggerType

SUPPORTED_TRIGGER_TYPES = {item.value for item in SchedulerTriggerType}
EVENT_DRIVEN_TRIGGER_TYPES = {
    SchedulerTriggerType.NEW_BACKLOG_ITEM.value,
    SchedulerTriggerType.NEW_MEMORY_SIGNAL.value,
    SchedulerTriggerType.NEW_FAILURE_PATTERN.value,
    SchedulerTriggerType.CANDIDATE_QUALITY_REGRESSION.value,
    SchedulerTriggerType.RESEARCH_OS_CERTIFICATION_FAILURE.value,
}


def create_scheduler_trigger(trigger_type: str, *, source: dict[str, Any] | None = None, created_at: str | None = None, status: str = SchedulerTriggerStatus.TRIGGER_ACCEPTED.value, reason: str = "", metadata: dict[str, Any] | None = None) -> SchedulerTrigger:
    if trigger_type not in SUPPORTED_TRIGGER_TYPES:
        raise ValueError(f"unsupported scheduler trigger type: {trigger_type}")
    source_payload = dict(source or {})
    created = created_at or now_utc()
    trigger_key = build_trigger_key(trigger_type, source_payload, created)
    trigger_id = f"scheduler-trigger-{stable_id([trigger_type, trigger_key, created])}"
    return SchedulerTrigger(trigger_id=trigger_id, trigger_type=trigger_type, trigger_key=trigger_key, created_at=created, status=status, source=source_payload, reason=reason, metadata=dict(metadata or {}))


def build_trigger_key(trigger_type: str, source: dict[str, Any] | None = None, created_at: str | None = None) -> str:
    source_payload = dict(source or {})
    if trigger_type == SchedulerTriggerType.HOURLY.value:
        timestamp = created_at or source_payload.get("created_at") or now_utc()
        hour_bucket = str(timestamp)[:13]
        return f"HOURLY:{hour_bucket}"
    if trigger_type == SchedulerTriggerType.MANUAL.value:
        manual_key = source_payload.get("request_id") or source_payload.get("operator_id") or created_at or now_utc()
        return f"MANUAL:{manual_key}"
    for field in ("event_id", "backlog_item_id", "memory_id", "failure_pattern_id", "certification_id", "artifact_id"):
        if source_payload.get(field):
            return f"{trigger_type}:{field}:{source_payload[field]}"
    return f"{trigger_type}:{stable_id([json.dumps(source_payload, sort_keys=True)])}"


def is_event_driven_trigger(trigger_type: str) -> bool:
    return trigger_type in EVENT_DRIVEN_TRIGGER_TYPES


def is_supported_trigger(trigger_type: str) -> bool:
    return trigger_type in SUPPORTED_TRIGGER_TYPES


def stable_id(parts: list[Any]) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update(str(part).encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()[:24]


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
