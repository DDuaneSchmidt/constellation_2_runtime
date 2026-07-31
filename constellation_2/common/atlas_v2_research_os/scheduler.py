from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .failure_observatory import record_governance_block, record_scheduler_failure
from .autonomous_research_execution import run_bounded_research_once
from .scheduler_events import create_scheduler_trigger, stable_id
from .scheduler_governance import validate_scheduler_governance, validate_scheduler_result_governed
from .scheduler_locking import DEFAULT_STALE_AFTER_SECONDS, scheduler_lock
from .scheduler_models import SchedulerExecutionRecord, SchedulerTriggerStatus, SchedulerTriggerType
from .scheduler_reports import append_scheduler_execution, append_scheduler_trigger, has_duplicate_trigger, write_scheduler_report


def run_scheduler_trigger(trigger_type: str = SchedulerTriggerType.MANUAL.value, *, root: str | Path = DEFAULT_STORE_ROOT, source: dict[str, Any] | None = None, day: str | None = None, stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS) -> dict[str, Any]:
    root_path = Path(root)
    created_at = scheduler_timestamp(source=source, day=day)
    trigger = create_scheduler_trigger(trigger_type, source=source, created_at=created_at)
    trigger_payload = trigger.to_dict()
    governance = validate_scheduler_governance(trigger_payload, {"execution_function": "run_bounded_research_once", "continuous_loop": False, "daemon": False})
    if governance["status"] != "PASS":
        blocked = {**trigger_payload, "status": SchedulerTriggerStatus.TRIGGER_BLOCKED.value, "reason": "GOVERNANCE_FAILED", "metadata": {**trigger_payload.get("metadata", {}), "governance_result": governance}}
        append_scheduler_trigger(blocked, root_path)
        record_governance_block(root=root_path, component="SCHEDULER", error_message="Scheduler trigger blocked by governance.", scheduler_trigger_id=trigger.trigger_id, metadata={"governance_status": governance.get("status", ""), "detail_count": len(governance.get("details", []))})
        return {"status": SchedulerTriggerStatus.TRIGGER_BLOCKED.value, "trigger": blocked, "governance_result": governance}
    if has_duplicate_trigger(root_path, trigger.trigger_key):
        skipped = {**trigger_payload, "status": SchedulerTriggerStatus.TRIGGER_SKIPPED.value, "reason": "DUPLICATE_TRIGGER"}
        append_scheduler_trigger(skipped, root_path)
        return {"status": SchedulerTriggerStatus.TRIGGER_SKIPPED.value, "trigger": skipped, "governance_result": governance}
    append_scheduler_trigger(trigger_payload, root_path)
    scheduler_run_id = f"scheduler-run-{stable_id([trigger.trigger_id, created_at])}"
    with scheduler_lock(root_path, owner_id=scheduler_run_id, trigger_id=trigger.trigger_id, stale_after_seconds=stale_after_seconds) as lock_result:
        if not lock_result.acquired:
            skipped = {**trigger_payload, "status": SchedulerTriggerStatus.TRIGGER_SKIPPED.value, "reason": "LOCK_CONFLICT", "metadata": {**trigger_payload.get("metadata", {}), "lock_result": lock_result.to_dict()}}
            append_scheduler_trigger(skipped, root_path)
            return {"status": SchedulerTriggerStatus.TRIGGER_SKIPPED.value, "trigger": skipped, "governance_result": governance, "lock_result": lock_result.to_dict()}
        started = created_at if day else now_utc()
        started_trigger = {**trigger_payload, "status": SchedulerTriggerStatus.EXECUTION_STARTED.value, "metadata": {**trigger_payload.get("metadata", {}), "scheduler_run_id": scheduler_run_id, "lock_id": lock_result.lock_id}}
        append_scheduler_trigger(started_trigger, root_path)
        errors: list[str] = []
        warnings: list[str] = []
        execution_result: dict[str, Any] = {}
        try:
            execution_result = run_bounded_research_once(root_path, day=day)
        except Exception as exc:
            errors.append(f"{exc.__class__.__name__}: {exc}")
            record_scheduler_failure(root=root_path, error_type=exc.__class__.__name__, error_message=str(exc), scheduler_trigger_id=trigger.trigger_id, recoverable=False)
        completed = now_utc()
        result_payload = {"status": SchedulerTriggerStatus.EXECUTION_COMPLETED.value, "execution_result": execution_result}
        result_gate = validate_scheduler_result_governed(result_payload)
        if result_gate["result"] != "PASS":
            errors.extend(result_gate.get("details", []))
            record_governance_block(root=root_path, component="SCHEDULER", error_message="Scheduler result governance blocked execution record.", scheduler_trigger_id=trigger.trigger_id, metadata={"result_gate": result_gate.get("result", ""), "detail_count": len(result_gate.get("details", []))})
        status = SchedulerTriggerStatus.EXECUTION_COMPLETED.value if not errors else SchedulerTriggerStatus.TRIGGER_BLOCKED.value
        record = SchedulerExecutionRecord(
            scheduler_run_id=scheduler_run_id,
            trigger_id=trigger.trigger_id,
            trigger_type=trigger.trigger_type,
            started_at=started,
            completed_at=completed,
            status=status,
            bounded_execution_id=str(execution_result.get("execution_id") or ""),
            bounded_execution_status=str(execution_result.get("status") or ""),
            lock_id=lock_result.lock_id,
            governance_result={"scheduler_governance": governance, "result_governance": result_gate},
            certification_result=dict(execution_result.get("certification_result") or {}),
            errors=errors,
            warnings=warnings + list(execution_result.get("warnings", [])),
            metadata={"trigger_key": trigger.trigger_key, "trigger_source": dict(source or {}), "research_only": True, "execution_function": "run_bounded_research_once"},
        ).to_dict()
        append_scheduler_execution(record, root_path)
        completed_trigger = {**trigger_payload, "status": SchedulerTriggerStatus.EXECUTION_COMPLETED.value, "reason": "EXECUTION_RECORDED", "metadata": {**trigger_payload.get("metadata", {}), "scheduler_run_id": scheduler_run_id, "bounded_execution_id": record["bounded_execution_id"]}}
        append_scheduler_trigger(completed_trigger, root_path)
        write_scheduler_report(root_path, day=day)
        return {"status": record["status"], "trigger": completed_trigger, "execution": record, "execution_result": execution_result, "governance_result": governance, "lock_result": lock_result.to_dict()}


def run_hourly_scheduler_once(*, root: str | Path = DEFAULT_STORE_ROOT, source: dict[str, Any] | None = None, day: str | None = None) -> dict[str, Any]:
    return run_scheduler_trigger(SchedulerTriggerType.HOURLY.value, root=root, source=source, day=day)


def run_event_scheduler_once(trigger_type: str, *, root: str | Path = DEFAULT_STORE_ROOT, source: dict[str, Any] | None = None, day: str | None = None) -> dict[str, Any]:
    if trigger_type == SchedulerTriggerType.MANUAL.value or trigger_type == SchedulerTriggerType.HOURLY.value:
        raise ValueError("event scheduler requires an event-driven trigger type")
    return run_scheduler_trigger(trigger_type, root=root, source=source, day=day)


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def scheduler_timestamp(*, source: dict[str, Any] | None = None, day: str | None = None) -> str:
    source_created_at = (source or {}).get("created_at")
    if isinstance(source_created_at, str) and source_created_at:
        return source_created_at
    if day:
        return f"{day}T00:00:00Z"
    return now_utc()
