from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable

from constellation_2.aegis_truth.evidence_event_v1 import build_event, canonical_json, validate_event

DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
LEDGER_RELATIVE = Path("events/aegis_evidence_ledger_v1")


def ledger_day_dir(truth_root: str | Path, target_day: str) -> Path:
    return Path(truth_root) / LEDGER_RELATIVE / target_day


def ledger_path(truth_root: str | Path, target_day: str) -> Path:
    return ledger_day_dir(truth_root, target_day) / "events.jsonl"


def append_event(event: dict[str, Any], *, truth_root: str | Path = DEFAULT_TRUTH_ROOT) -> Path:
    validate_event(event)
    path = ledger_path(truth_root, event["target_day"])
    path.parent.mkdir(parents=True, exist_ok=True)
    line = canonical_json(event) + "\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    fd = os.open(path, flags, 0o644)
    try:
        os.write(fd, line.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_dir(path.parent)
    return path


def append_invalid_producer_event(
    *,
    producer: str,
    target_day: str,
    environment: str,
    error: str,
    owner: str = "aegis_truth",
    truth_root: str | Path = DEFAULT_TRUTH_ROOT,
) -> dict[str, Any]:
    event = build_event(
        event_type="PRODUCER_OUTPUT_INVALID",
        producer="aegis.evidence_ledger_v1",
        target_day=target_day,
        environment=environment,
        status="UNKNOWN",
        blocker="producer output invalid or missing",
        owner=owner,
        severity="ERROR",
        payload={"producer": producer, "error": error},
        next_action="Inspect producer output and rerun the producer.",
    )
    append_event(event, truth_root=truth_root)
    return event


def read_events(*, truth_root: str | Path = DEFAULT_TRUTH_ROOT, target_day: str) -> list[dict[str, Any]]:
    path = ledger_path(truth_root, target_day)
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                event = json.loads(stripped)
                validate_event(event)
                events.append(event)
            except Exception as exc:
                events.append(
                    build_event(
                        event_type="LEDGER_EVENT_INVALID",
                        producer="aegis.evidence_ledger_v1",
                        target_day=target_day,
                        environment="UNKNOWN",
                        status="UNKNOWN",
                        blocker="ledger contains invalid event",
                        owner="aegis_truth",
                        severity="ERROR",
                        payload={"ledger_path": str(path), "line_no": line_no, "error": f"{type(exc).__name__}: {exc}"},
                        next_action="Quarantine invalid ledger line and restore from durable evidence if necessary.",
                        evidence_path=str(path),
                    )
                )
    return events


def iter_events(*, truth_root: str | Path = DEFAULT_TRUTH_ROOT, target_day: str) -> Iterable[dict[str, Any]]:
    yield from read_events(truth_root=truth_root, target_day=target_day)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
