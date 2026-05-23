from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA_ID = "trade_lifecycle_event"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "trade_lifecycle_event_v1"
REPORT_FILENAME = "trade_lifecycle_event.v1.jsonl"

ALLOWED_EVENTS = {
    "CASE_OPENED",
    "CONSTRUCTION_ATTACHED",
    "CONSTRUCTION_BLOCKED",
    "CAPTURE_READY",
    "MANUAL_CAPTURE_RECORDED",
    "SKIPPED",
    "EXPIRED",
    "CLOSED",
}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def trade_lifecycle_event_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def append_trade_lifecycle_event_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    trade_lifecycle_case_id: str,
    event_type: str,
    state: str = "",
    state_reason: str = "",
    paper_trade_construction_id: str = "",
    manual_capture_record_id: str = "",
    blocker_codes: list[str] | None = None,
    source_artifacts: list[dict[str, Any]] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    if event_type not in ALLOWED_EVENTS:
        raise ValueError(f"event_type must be one of {sorted(ALLOWED_EVENTS)}")
    now = generated_at_utc or _now()
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "event_id": "",
        "trade_lifecycle_case_id": trade_lifecycle_case_id,
        "event_type": event_type,
        "source_day": str(day_utc),
        "state": state,
        "state_reason": state_reason,
        "paper_trade_construction_id": paper_trade_construction_id,
        "manual_capture_record_id": manual_capture_record_id,
        "blocker_codes": list(blocker_codes or []),
        "source_artifacts": list(source_artifacts or []),
        "created_at": now,
        "append_only": True,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "order_routing_allowed": False,
        "capital_allocation_allowed": False,
        "paper_submit_created": False,
    }
    payload["event_id"] = f"trade-lifecycle-event:{_stable_hash(payload)[:24]}"
    payload["artifact_id"] = f"trade_lifecycle_event_v1:{day_utc}:{payload['event_id'].rsplit(':', 1)[-1]}"
    payload["immutable_hash"] = _stable_hash({**payload, "immutable_hash": ""})
    path = trade_lifecycle_event_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    return {**payload, "path": str(path)}


def list_trade_lifecycle_events_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    trade_lifecycle_case_id: str = "",
) -> list[dict[str, Any]]:
    path = trade_lifecycle_event_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        if trade_lifecycle_case_id and str(row.get("trade_lifecycle_case_id") or "") != trade_lifecycle_case_id:
            continue
        rows.append(row)
    return rows
