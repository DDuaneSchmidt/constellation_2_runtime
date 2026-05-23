from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
from typing import Any

from ops.aegis.evidence_event_v1 import finalize_evidence_event_v1, validate_evidence_event_v1
from ops.aegis.runtime_evaluation_v1 import stable_json_bytes_v1


EVENT_FAMILY = "aegis_evidence_events_v1"
EVENTS_FILENAME = "events.jsonl"


def evidence_events_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "events" / EVENT_FAMILY / day_utc / EVENTS_FILENAME


def read_evidence_events_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = evidence_events_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Malformed event JSON at line {index}") from exc
        if not isinstance(event, dict):
            raise ValueError(f"Malformed event object at line {index}")
        validate_evidence_event_v1(event)
        if event.get("day_utc") != day_utc:
            raise ValueError(f"Wrong-day event at line {index}: {event.get('day_utc')}")
        expected_previous = events[-1]["event_hash"] if events else ""
        if event.get("previous_event_hash") != expected_previous:
            raise ValueError(f"Evidence event hash chain broken at line {index}")
        events.append(event)
    return events


def append_evidence_event_v1(*, truth_root: Path, day_utc: str, event: dict[str, Any]) -> dict[str, Any]:
    if str(event.get("day_utc") or "") != day_utc:
        raise ValueError("Cannot append wrong-day evidence event")
    path = evidence_events_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            existing = read_evidence_events_v1(truth_root=truth_root, day_utc=day_utc)
            previous_hash = existing[-1]["event_hash"] if existing else ""
            finalized = finalize_evidence_event_v1(event, previous_event_hash=previous_hash)
            line = stable_json_bytes_v1(finalized) + b"\n"
            fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            try:
                os.write(fd, line)
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    return finalized


def rebuild_evidence_snapshot_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    events = read_evidence_events_v1(truth_root=truth_root, day_utc=day_utc)
    by_schema: dict[str, list[dict[str, Any]]] = {}
    rejected_hashes: set[str] = set()
    tampered_hashes: set[str] = set()
    for event in events:
        event_type = str(event.get("event_type") or "")
        schema_id = str(event.get("schema_id") or "")
        if event_type == "EvidenceRejected":
            rejected_hashes.update(str(value) for value in (event.get("input_hashes") or {}).values())
        if event_type == "ArtifactTampered":
            tampered_hashes.update(str(value) for value in (event.get("input_hashes") or {}).values())
        if schema_id and event_type in {"EvidenceProduced", "EvidenceValidated", "EvidenceRejected", "EvidenceExpired", "ArtifactTampered"}:
            by_schema.setdefault(schema_id, []).append(event)
    latest_by_schema = {
        schema_id: sorted(rows, key=lambda row: (str(row.get("created_at_utc") or ""), str(row.get("event_id") or "")))[-1]
        for schema_id, rows in sorted(by_schema.items())
        if rows
    }
    return {
        "schema_id": "aegis_evidence_snapshot",
        "schema_version": "v1",
        "day_utc": day_utc,
        "events": events,
        "latest_by_schema_id": latest_by_schema,
        "rejected_hashes": sorted(rejected_hashes),
        "tampered_hashes": sorted(tampered_hashes),
        "hash_chain_valid": True,
    }
