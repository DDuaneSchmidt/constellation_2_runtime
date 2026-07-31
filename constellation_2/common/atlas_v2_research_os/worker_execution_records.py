from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .failure_observatory import record_worker_failure

WORKER_RUNS_DIRNAME = "worker_runs"
WORKER_RUN_INDEX_NAME = "worker_run_index.json"


@dataclass(frozen=True)
class WorkerExecutionRecord:
    worker_run_id: str
    worker_type: str
    started_at: str
    completed_at: str
    status: str
    input_artifact_ids: list[str] = field(default_factory=list)
    output_artifact_ids: list[str] = field(default_factory=list)
    lineage_result: dict[str, Any] = field(default_factory=dict)
    governance_result: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def worker_runs_root(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = Path(root) / WORKER_RUNS_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def worker_run_index_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = worker_runs_root(root) / WORKER_RUN_INDEX_NAME
    if not path.exists():
        _write_json(path, {"worker_runs": []})
    return path


def write_worker_execution_record(record: WorkerExecutionRecord | dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    payload = record.to_dict() if isinstance(record, WorkerExecutionRecord) else dict(record)
    ok, failures = validate_worker_execution_record(payload)
    if not ok:
        record_worker_failure(root=root, error_type="WORKER_EXECUTION_RECORD_INVALID", error_message="; ".join(failures), worker_run_id=str(payload.get("worker_run_id") or ""), worker_id=str(payload.get("worker_id") or ""), artifact_ids=list(payload.get("output_artifact_ids") or []), recoverable=True)
        raise ValueError(f"invalid worker execution record: {failures}")
    day = _day_from_timestamp(str(payload["started_at"]))
    out_dir = worker_runs_root(root) / day
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"worker_run_{_safe_id(str(payload['worker_run_id']))}.json"
    _write_json(path, payload)
    _append_index(root, payload, path)
    return {"record": path, "index": worker_run_index_path(root)}


def read_worker_run_index(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    return json.loads(worker_run_index_path(root).read_text(encoding="utf-8"))


def validate_worker_execution_record(record: dict[str, Any]) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for field_name in (
        "worker_run_id",
        "worker_type",
        "started_at",
        "completed_at",
        "status",
        "input_artifact_ids",
        "output_artifact_ids",
        "lineage_result",
        "governance_result",
        "errors",
        "warnings",
        "metadata",
    ):
        if field_name not in record:
            failures.append(f"missing field: {field_name}")
    for list_field in ("input_artifact_ids", "output_artifact_ids", "errors", "warnings"):
        if list_field in record and not isinstance(record[list_field], list):
            failures.append(f"{list_field} must be a list")
    for dict_field in ("lineage_result", "governance_result", "metadata"):
        if dict_field in record and not isinstance(record[dict_field], dict):
            failures.append(f"{dict_field} must be an object")
    if record.get("governance_result", {}).get("status") == "FAIL" and record.get("status") == "COMPLETED":
        failures.append("completed worker run cannot have failing governance_result")
    if record.get("lineage_result", {}).get("status") == "FAIL" and record.get("output_artifact_ids"):
        failures.append("worker run with outputs cannot have failing lineage_result")
    return not failures, failures


def validate_worker_run_record_integrity(root: str | Path = DEFAULT_STORE_ROOT) -> tuple[bool, list[str]]:
    failures: list[str] = []
    index = read_worker_run_index(root)
    rows = index.get("worker_runs")
    if not isinstance(rows, list):
        return False, ["worker_run_index.json worker_runs must be a list"]
    seen: set[str] = set()
    for row in rows:
        run_id = str(row.get("worker_run_id") or "")
        if not run_id:
            failures.append("index row missing worker_run_id")
            continue
        if run_id in seen:
            failures.append(f"duplicate worker_run_id in index: {run_id}")
        seen.add(run_id)
        path = Path(row.get("path") or "")
        if not path.is_absolute():
            path = Path(root) / path
        if not path.exists():
            failures.append(f"worker run record missing: {run_id}")
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            failures.append(f"worker run record unreadable {run_id}: {exc}")
            continue
        ok, record_failures = validate_worker_execution_record(payload)
        if not ok:
            failures.extend(f"{run_id}: {failure}" for failure in record_failures)
    return not failures, failures


def build_worker_execution_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    day_value = day or date.today().isoformat()
    rows = []
    root_path = Path(root)
    index = read_worker_run_index(root_path)
    for row in index.get("worker_runs", []):
        if str(row.get("day")) == day_value:
            rows.append(row)
    counts_by_worker: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    governance_failures = 0
    lineage_failures = 0
    blocked_outputs = 0
    for row in rows:
        worker_type = str(row.get("worker_type") or "UNKNOWN")
        status = str(row.get("status") or "UNKNOWN")
        counts_by_worker[worker_type] = counts_by_worker.get(worker_type, 0) + 1
        status_counts[status] = status_counts.get(status, 0) + 1
        if row.get("governance_status") == "FAIL":
            governance_failures += 1
        if row.get("lineage_status") == "FAIL":
            lineage_failures += 1
        blocked_outputs += int(row.get("blocked_output_count") or 0)
    return {
        "schema_id": "atlas_v2_research_os_worker_execution_report_v1",
        "schema_version": "v1",
        "day": day_value,
        "worker_execution_counts": counts_by_worker,
        "status_counts": status_counts,
        "governance_failures": governance_failures,
        "lineage_failures": lineage_failures,
        "blocked_outputs": blocked_outputs,
        "forbidden_artifact_attempts": 0,
        "worker_runs": rows,
    }


def write_worker_execution_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    day_value = day or date.today().isoformat()
    report = build_worker_execution_report(root, day=day_value)
    out_dir = worker_runs_root(root) / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "worker_execution_report.json"
    md_path = out_dir / "worker_execution_summary.md"
    _write_json(json_path, report)
    md_path.write_text(render_worker_execution_summary(report), encoding="utf-8")
    return {"json": json_path, "summary": md_path}


def render_worker_execution_summary(report: dict[str, Any]) -> str:
    return "\n".join([
        "# Atlas V2 Research OS Worker Execution Report",
        "",
        f"Day: {report['day']}",
        f"Worker execution counts: {json.dumps(report['worker_execution_counts'], sort_keys=True)}",
        f"Status counts: {json.dumps(report['status_counts'], sort_keys=True)}",
        f"Governance failures: {report['governance_failures']}",
        f"Lineage failures: {report['lineage_failures']}",
        f"Blocked outputs: {report['blocked_outputs']}",
        f"Forbidden artifact attempts: {report['forbidden_artifact_attempts']}",
        "",
    ])


def record_from_worker_result(result: Any, *, worker_type: str, metadata: dict[str, Any] | None = None) -> WorkerExecutionRecord:
    payload = result.to_dict() if hasattr(result, "to_dict") else dict(result)
    run_metadata = dict(payload.get("metadata", {}))
    run_metadata.update(metadata or {})
    return WorkerExecutionRecord(
        worker_run_id=str(payload.get("worker_run_id") or ""),
        worker_type=worker_type,
        started_at=str(payload.get("started_at") or _now()),
        completed_at=str(payload.get("completed_at") or _now()),
        status=str(payload.get("status") or ""),
        input_artifact_ids=list(payload.get("input_artifact_ids") or []),
        output_artifact_ids=list(payload.get("output_artifact_ids") or []),
        lineage_result=dict(payload.get("lineage_result") or {}),
        governance_result=dict(payload.get("governance_result") or {}),
        errors=list(payload.get("errors") or []),
        warnings=list(payload.get("warnings") or []),
        metadata=run_metadata,
    )


def _append_index(root: str | Path, payload: dict[str, Any], path: Path) -> None:
    index_path = worker_run_index_path(root)
    index = json.loads(index_path.read_text(encoding="utf-8"))
    rows = [row for row in index.get("worker_runs", []) if row.get("worker_run_id") != payload["worker_run_id"]]
    rows.append({
        "worker_run_id": payload["worker_run_id"],
        "worker_type": payload["worker_type"],
        "status": payload["status"],
        "day": _day_from_timestamp(str(payload["started_at"])),
        "started_at": payload["started_at"],
        "completed_at": payload["completed_at"],
        "input_artifact_ids": list(payload.get("input_artifact_ids", [])),
        "output_artifact_ids": list(payload.get("output_artifact_ids", [])),
        "governance_status": payload.get("governance_result", {}).get("status", "UNKNOWN"),
        "lineage_status": payload.get("lineage_result", {}).get("status", "UNKNOWN"),
        "blocked_output_count": len(payload.get("metadata", {}).get("blocked_outputs", [])),
        "path": str(path.relative_to(Path(root))),
    })
    _write_json(index_path, {"worker_runs": rows})


def _safe_id(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)[:180]


def _day_from_timestamp(value: str) -> str:
    if len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
        return value[:10]
    return date.today().isoformat()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
