from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from constellation_2.aegis_truth.evidence_event_v1 import utc_now_iso
from constellation_2.aegis_truth.evidence_ledger_v1 import read_events
from constellation_2.aegis_truth.projection_writer_v1 import atomic_write_json

SEVERITY_RANK = {"INFO": 0, "WARN": 1, "ERROR": 2, "CRITICAL": 3}
FINAL_STATUS_RANK = {"READY": 0, "DEGRADED": 1, "UNKNOWN": 2, "BLOCKED": 3, "FORBIDDEN": 4}


def read_day_events(*, truth_root: str | Path, target_day: str) -> list[dict[str, Any]]:
    return read_events(truth_root=truth_root, target_day=target_day)


def event_sort_key(index_event: tuple[int, dict[str, Any]]) -> tuple[str, int]:
    index, event = index_event
    return str(event.get("observed_at_utc", "")), index


def latest_event(events: list[dict[str, Any]], event_types: set[str] | None = None) -> dict[str, Any] | None:
    candidates = [event for event in events if event_types is None or event.get("event_type") in event_types]
    if not candidates:
        return None
    return max(enumerate(candidates), key=event_sort_key)[1]


def latest_matching(events: list[dict[str, Any]], predicate) -> dict[str, Any] | None:
    candidates = [event for event in events if predicate(event)]
    if not candidates:
        return None
    return max(enumerate(candidates), key=event_sort_key)[1]


def events_after(events: list[dict[str, Any]], anchor: dict[str, Any], predicate) -> list[dict[str, Any]]:
    try:
        anchor_index = next(i for i, event in enumerate(events) if event.get("event_id") == anchor.get("event_id"))
    except StopIteration:
        anchor_index = -1
    anchor_at = str(anchor.get("observed_at_utc", ""))
    out: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        observed_at = str(event.get("observed_at_utc", ""))
        if observed_at < anchor_at or (observed_at == anchor_at and index <= anchor_index):
            continue
        if predicate(event):
            out.append(event)
    return out


def parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def age_seconds(observed_at_utc: str | None, *, now: datetime | None = None) -> int | None:
    observed = parse_utc(observed_at_utc)
    if observed is None:
        return None
    current = now or datetime.now(UTC)
    return max(0, int((current - observed).total_seconds()))


def evidence_event_ids(events: list[dict[str, Any]]) -> list[str]:
    return [str(event["event_id"]) for event in events if event.get("event_id")]


def evidence_paths(events: list[dict[str, Any]], extra_paths: list[str | Path | None] | None = None) -> list[str]:
    paths = {str(event["evidence_path"]) for event in events if event.get("evidence_path")}
    for path in extra_paths or []:
        if path:
            paths.add(str(path))
    return sorted(paths)


def report_path(truth_root: str | Path, report_name: str, target_day: str, filename: str) -> Path:
    return Path(truth_root) / "reports" / report_name / target_day / filename


def write_report(truth_root: str | Path, report_name: str, target_day: str, filename: str, payload: dict[str, Any]) -> Path:
    return atomic_write_json(report_path(truth_root, report_name, target_day, filename), payload)


def base_state(*, schema_version: str, target_day: str, environment: str, status: str, blocker: str | None, severity: str, owner: str, next_action: str, events: list[dict[str, Any]], extra_paths: list[str | Path | None] | None = None) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "target_day": target_day,
        "environment": environment,
        "status": status,
        "blocker": blocker,
        "severity": severity,
        "owner": owner,
        "next_action": next_action,
        "evidence_event_ids": evidence_event_ids(events),
        "evidence_paths": evidence_paths(events, extra_paths),
        "generated_at_utc": utc_now_iso(),
    }


def read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def json_files_under(path: Path) -> list[Path]:
    if not path.exists():
        return []
    if path.is_file() and path.suffix == ".json":
        return [path]
    if path.is_dir():
        return sorted(path.rglob("*.json"))
    return []


def highest_severity(states: list[dict[str, Any]], default: str = "INFO") -> str:
    if not states:
        return default
    return max((str(state.get("severity", default)) for state in states), key=lambda value: SEVERITY_RANK.get(value, 0))
