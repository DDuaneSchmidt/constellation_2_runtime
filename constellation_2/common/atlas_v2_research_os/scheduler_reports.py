from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

SCHEDULER_DIRNAME = "scheduler"
TRIGGER_LEDGER_NAME = "scheduler_trigger_ledger.json"
EXECUTION_LEDGER_NAME = "scheduler_execution_ledger.json"


def scheduler_root(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = Path(root) / SCHEDULER_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def trigger_ledger_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = scheduler_root(root) / TRIGGER_LEDGER_NAME
    if not path.exists():
        write_json(path, {"triggers": []})
    return path


def execution_ledger_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = scheduler_root(root) / EXECUTION_LEDGER_NAME
    if not path.exists():
        write_json(path, {"executions": []})
    return path


def append_scheduler_trigger(trigger: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    path = trigger_ledger_path(root)
    ledger = json.loads(path.read_text(encoding="utf-8"))
    rows = [row for row in ledger.get("triggers", []) if row.get("trigger_id") != trigger.get("trigger_id")]
    rows.append(dict(trigger))
    write_json(path, {"triggers": rows})
    write_scheduler_report(root, day=day_from_timestamp(str(trigger.get("created_at") or now_utc())))
    return {"trigger_ledger": path}


def append_scheduler_execution(record: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    path = execution_ledger_path(root)
    ledger = json.loads(path.read_text(encoding="utf-8"))
    rows = [row for row in ledger.get("executions", []) if row.get("scheduler_run_id") != record.get("scheduler_run_id")]
    rows.append(dict(record))
    write_json(path, {"executions": rows})
    write_scheduler_report(root, day=day_from_timestamp(str(record.get("started_at") or now_utc())))
    return {"execution_ledger": path}


def read_scheduler_triggers(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return json.loads(trigger_ledger_path(root).read_text(encoding="utf-8")).get("triggers", [])


def read_scheduler_executions(root: str | Path = DEFAULT_STORE_ROOT) -> list[dict[str, Any]]:
    return json.loads(execution_ledger_path(root).read_text(encoding="utf-8")).get("executions", [])


def has_duplicate_trigger(root: str | Path, trigger_key: str) -> bool:
    return any(row.get("trigger_key") == trigger_key and row.get("status") in {"TRIGGER_ACCEPTED", "EXECUTION_STARTED", "EXECUTION_COMPLETED"} for row in read_scheduler_triggers(root))


def build_scheduler_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    day_value = day or date.today().isoformat()
    triggers = [row for row in read_scheduler_triggers(root) if day_from_timestamp(str(row.get("created_at") or "")) == day_value]
    executions = [row for row in read_scheduler_executions(root) if day_from_timestamp(str(row.get("started_at") or "")) == day_value]
    trigger_counts: dict[str, int] = {}
    execution_counts: dict[str, int] = {}
    for row in triggers:
        trigger_counts[str(row.get("status") or "UNKNOWN")] = trigger_counts.get(str(row.get("status") or "UNKNOWN"), 0) + 1
    for row in executions:
        execution_counts[str(row.get("status") or "UNKNOWN")] = execution_counts.get(str(row.get("status") or "UNKNOWN"), 0) + 1
    lock_conflicts = sum(1 for row in triggers if row.get("reason") == "LOCK_CONFLICT")
    failures = sum(1 for row in executions if row.get("status") not in {"EXECUTION_COMPLETED"})
    skips = sum(1 for row in triggers if row.get("status") == "TRIGGER_SKIPPED")
    return {
        "schema_id": "atlas_v2_research_os_scheduler_report_v1",
        "schema_version": "v1",
        "day": day_value,
        "trigger_counts": trigger_counts,
        "execution_counts": execution_counts,
        "triggers": triggers,
        "executions": executions,
        "skips": skips,
        "failures": failures,
        "lock_conflicts": lock_conflicts,
        "limitations": ["Research OS scheduler invokes one bounded research execution per accepted trigger and has no trading, broker, capital, sleeve, portfolio, position sizing, recommendation, or candidate promotion authority."],
    }


def write_scheduler_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    day_value = day or date.today().isoformat()
    report = build_scheduler_report(root, day=day_value)
    out_dir = scheduler_root(root) / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "scheduler_report.v1.json"
    md_path = out_dir / "scheduler_summary.md"
    latest_json = scheduler_root(root) / "latest.json"
    latest_md = scheduler_root(root) / "latest_summary.md"
    write_json(json_path, report)
    summary = render_scheduler_summary(report)
    md_path.write_text(summary, encoding="utf-8")
    write_json(latest_json, report)
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_scheduler_summary(report: dict[str, Any]) -> str:
    return "\n".join([
        "# Atlas V2 Research OS Scheduler Report",
        "",
        f"Day: {report['day']}",
        f"Trigger counts: {json.dumps(report['trigger_counts'], sort_keys=True)}",
        f"Execution counts: {json.dumps(report['execution_counts'], sort_keys=True)}",
        f"Skips: {report['skips']}",
        f"Failures: {report['failures']}",
        f"Lock conflicts: {report['lock_conflicts']}",
        "Authority: research-only bounded execution scheduling.",
        "",
    ])


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def day_from_timestamp(value: str) -> str:
    if not value:
        return date.today().isoformat()
    return value[:10]


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
