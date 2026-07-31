from __future__ import annotations

import json
import traceback
import uuid
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .failure_observatory_governance import validate_failure_observatory_no_authority_escalation
from .failure_observatory_models import FailureComponent, FailureRegistry, FailureSeverity, FailureTrend, ResearchOSFailureRecord

FAILURES_DIRNAME = "failures"
REGISTRY_NAME = "failure_registry.jsonl"
INDEX_NAME = "failure_index.json"
LATEST_NAME = "latest.json"


def failures_root(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = Path(root) / FAILURES_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def failure_registry_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = failures_root(root) / REGISTRY_NAME
    path.touch(exist_ok=True)
    return path


def failure_index_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = failures_root(root) / INDEX_NAME
    if not path.exists():
        _write_json(path, FailureRegistry("atlas_v2_research_os_failure_registry_v1", "v1", str(Path(root)), 0, 0).to_dict())
    return path


def record_failure(
    *,
    component: str = FailureComponent.UNKNOWN.value,
    severity: str = FailureSeverity.ERROR.value,
    error_type: str,
    error_message: str,
    root: str | Path = DEFAULT_STORE_ROOT,
    timestamp: str | None = None,
    stack_trace: str = "",
    run_id: str = "",
    execution_id: str = "",
    worker_run_id: str = "",
    worker_id: str = "",
    backlog_item_id: str = "",
    artifact_ids: list[str] | None = None,
    certification_id: str = "",
    scheduler_trigger_id: str = "",
    recoverable: bool = False,
    resolved: bool = False,
    resolution_notes: str = "",
    metadata: dict[str, Any] | None = None,
    failure_id: str | None = None,
) -> dict[str, Any]:
    _validate_component(component)
    _validate_severity(severity)
    authority = validate_failure_observatory_no_authority_escalation(metadata or {})
    if authority["status"] != "PASS":
        raise ValueError(f"failure metadata attempts authority escalation: {authority['failures']}")
    record = ResearchOSFailureRecord(
        failure_id=failure_id or _failure_id(component, error_type),
        timestamp=timestamp or _now(),
        component=component,
        severity=severity,
        error_type=str(error_type),
        error_message=str(error_message),
        stack_trace=str(stack_trace or ""),
        run_id=str(run_id or ""),
        execution_id=str(execution_id or ""),
        worker_run_id=str(worker_run_id or ""),
        worker_id=str(worker_id or ""),
        backlog_item_id=str(backlog_item_id or ""),
        artifact_ids=list(artifact_ids or []),
        certification_id=str(certification_id or ""),
        scheduler_trigger_id=str(scheduler_trigger_id or ""),
        recoverable=bool(recoverable),
        resolved=bool(resolved),
        resolution_notes=str(resolution_notes or ""),
        metadata=dict(metadata or {}),
    ).to_dict()
    with failure_registry_path(root).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\n")
    _rewrite_index(root)
    _write_json(failures_root(root) / LATEST_NAME, record)
    return record


def record_exception(
    exc: BaseException,
    *,
    component: str = FailureComponent.UNKNOWN.value,
    severity: str = FailureSeverity.ERROR.value,
    root: str | Path = DEFAULT_STORE_ROOT,
    **kwargs: Any,
) -> dict[str, Any]:
    return record_failure(
        root=root,
        component=component,
        severity=severity,
        error_type=exc.__class__.__name__,
        error_message=str(exc),
        stack_trace="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        **kwargs,
    )


def record_governance_block(*, error_message: str, root: str | Path = DEFAULT_STORE_ROOT, component: str = FailureComponent.GOVERNANCE.value, **kwargs: Any) -> dict[str, Any]:
    return record_failure(root=root, component=component, severity=FailureSeverity.GOVERNANCE_BLOCK.value, error_type="GOVERNANCE_BLOCK", error_message=error_message, recoverable=True, **kwargs)


def record_certification_block(*, error_message: str, root: str | Path = DEFAULT_STORE_ROOT, **kwargs: Any) -> dict[str, Any]:
    return record_failure(root=root, component=FailureComponent.CERTIFICATION.value, severity=FailureSeverity.CERTIFICATION_BLOCK.value, error_type="CERTIFICATION_BLOCK", error_message=error_message, recoverable=True, **kwargs)


def record_worker_failure(*, error_type: str, error_message: str, root: str | Path = DEFAULT_STORE_ROOT, severity: str = FailureSeverity.ERROR.value, **kwargs: Any) -> dict[str, Any]:
    return record_failure(root=root, component=FailureComponent.WORKER.value, severity=severity, error_type=error_type, error_message=error_message, **kwargs)


def record_scheduler_failure(*, error_type: str, error_message: str, root: str | Path = DEFAULT_STORE_ROOT, severity: str = FailureSeverity.ERROR.value, **kwargs: Any) -> dict[str, Any]:
    return record_failure(root=root, component=FailureComponent.SCHEDULER.value, severity=severity, error_type=error_type, error_message=error_message, **kwargs)


def list_failures(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    path = failure_registry_path(root)
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def list_unresolved_failures(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return [row for row in list_failures(root) if not row.get("resolved")]


def list_failures_by_component(component: str, root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return [row for row in list_failures(root) if row.get("component") == component]


def list_failures_by_severity(severity: str, root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return [row for row in list_failures(root) if row.get("severity") == severity]


def get_repeated_failures(root: str | Path = DEFAULT_STORE_ROOT, min_count: int = 2) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in list_failures(root):
        grouped[(str(row.get("component") or ""), str(row.get("severity") or ""), str(row.get("error_type") or ""))].append(row)
    trends = [
        FailureTrend(component=component, severity=severity, error_type=error_type, count=len(rows), failure_ids=[str(row["failure_id"]) for row in rows]).to_dict()
        for (component, severity, error_type), rows in grouped.items()
        if len(rows) >= min_count
    ]
    return sorted(trends, key=lambda row: (-int(row["count"]), row["component"], row["error_type"]))


def mark_failure_resolved(failure_id: str, *, resolution_notes: str, root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    rows = list_failures(root)
    updated: dict[str, Any] | None = None
    for row in rows:
        if row.get("failure_id") == failure_id:
            row["resolved"] = True
            row["resolution_notes"] = resolution_notes
            updated = row
            break
    if updated is None:
        raise KeyError(f"failure not found: {failure_id}")
    path = failure_registry_path(root)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
    _rewrite_index(root)
    _write_json(failures_root(root) / LATEST_NAME, updated)
    return updated


def _rewrite_index(root: str | Path) -> None:
    rows = list_failures(root)
    latest = rows[-1] if rows else {}
    index = FailureRegistry(
        schema_id="atlas_v2_research_os_failure_registry_v1",
        schema_version="v1",
        root=str(Path(root)),
        failure_count=len(rows),
        unresolved_count=sum(1 for row in rows if not row.get("resolved")),
        latest_failure_id=str(latest.get("failure_id") or ""),
        failures=[
            {
                "failure_id": row["failure_id"],
                "timestamp": row["timestamp"],
                "day": _day_from_timestamp(str(row["timestamp"])),
                "component": row["component"],
                "severity": row["severity"],
                "error_type": row["error_type"],
                "resolved": row["resolved"],
            }
            for row in rows
        ],
    ).to_dict()
    _write_json(failure_index_path(root), index)


def _validate_component(component: str) -> None:
    if component not in {item.value for item in FailureComponent}:
        raise ValueError(f"invalid failure component: {component}")


def _validate_severity(severity: str) -> None:
    if severity not in {item.value for item in FailureSeverity}:
        raise ValueError(f"invalid failure severity: {severity}")


def _failure_id(component: str, error_type: str) -> str:
    return f"failure-{date.today().isoformat()}-{component.lower()}-{_safe_id(error_type)}-{uuid.uuid4().hex[:12]}"


def _safe_id(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value)).strip("-")[:80] or "unknown"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _day_from_timestamp(value: str) -> str:
    if len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
        return value[:10]
    return date.today().isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
