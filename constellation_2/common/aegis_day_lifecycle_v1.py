from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "aegis_day_lifecycle"
SCHEMA_VERSION = "v1"

LIFECYCLE_STATES = {
    "PREV_DAY_CLOSED",
    "BOD_PENDING",
    "BOD_PREPARED",
    "PRE_OPEN_BLOCKED",
    "PRE_OPEN_READY",
    "PAPER_READY",
    "TRADING_ACTIVE",
    "EOD_PENDING",
    "EOD_COMPLETE",
}


def utc_now_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def lifecycle_day_dir_v1(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "aegis_day_lifecycle_v1"
        / str(day_utc).strip()
    ).resolve()


def lifecycle_state_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return lifecycle_day_dir_v1(truth_root=truth_root, day_utc=day_utc) / "aegis_day_lifecycle.v1.json"


def lifecycle_events_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return lifecycle_day_dir_v1(truth_root=truth_root, day_utc=day_utc) / "aegis_day_lifecycle_events.v1.jsonl"


def read_lifecycle_state_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = lifecycle_state_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_lifecycle_transition_v1(
    *,
    truth_root: Path,
    day_utc: str,
    state: str,
    producer: str,
    blocker: str = "",
    reason: str = "",
    evidence_paths: list[str] | None = None,
    produced_utc: str | None = None,
) -> Path:
    normalized_state = str(state).strip().upper()
    if normalized_state not in LIFECYCLE_STATES:
        raise ValueError(f"invalid Aegis lifecycle state: {state}")

    now = produced_utc or utc_now_iso_v1()
    previous = read_lifecycle_state_v1(truth_root=truth_root, day_utc=day_utc)
    previous_state = str(previous.get("state") or "").strip().upper()
    transition = {
        "timestamp_utc": now,
        "from_state": previous_state,
        "to_state": normalized_state,
        "blocker": str(blocker or "").strip(),
        "reason": str(reason or "").strip(),
        "producer": str(producer or "").strip(),
        "evidence_paths": list(evidence_paths or []),
    }
    transitions = previous.get("transitions") if isinstance(previous.get("transitions"), list) else []
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": str(day_utc).strip(),
        "state": normalized_state,
        "blocking_reason": str(blocker or "").strip(),
        "reason": str(reason or "").strip(),
        "updated_at_utc": now,
        "producer": str(producer or "").strip(),
        "evidence_paths": list(evidence_paths or []),
        "transitions": [*transitions, transition],
    }
    path = lifecycle_state_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")
    events = lifecycle_events_path_v1(truth_root=truth_root, day_utc=day_utc)
    with events.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(transition, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n")
    return path
