from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1

REPORT_FAMILY = "aegis_paper_session_ledger_v1"
REPORT_FILENAME = "paper_session_ledger.v1.json"
DEFAULT_SESSION_TIME = "09:50"
DEFAULT_SESSION_TIMEZONE = "America/New_York"
EVENT_STATUSES = {
    "PAPER_SESSION_SCHEDULED": "SCHEDULED",
    "PAPER_SESSION_STARTED": "STARTED",
    "CANDIDATES_GENERATED": "CANDIDATES_GENERATED",
    "PAPER_OPEN_AUTHORIZED": "AUTHORIZED",
    "PAPER_TRADES_CONSTRUCTED": "CONSTRUCTED",
    "CANDIDATE_SKIPPED": "CONSTRUCTED",
    "PAPER_RECEIPT_RECORDED": "CONSTRUCTED",
    "PAPER_TRADE_COMMAND_RECEIVED": "CONSTRUCTED",
    "PAPER_TRADE_COMMAND_REJECTED": "CONSTRUCTED",
    "PAPER_TRADE_COMMAND_EXECUTED": "CONSTRUCTED",
    "PAPER_TRADE_COMMAND_FAILED": "CONSTRUCTED",
    "PAPER_SESSION_CLOSED": "CLOSED",
    "PAPER_SESSION_FAILED": "FAILED",
    "PAPER_SESSION_RECONSTRUCTED": "CANDIDATES_GENERATED",
}


def paper_session_ledger_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_hhmm(value: str) -> tuple[int, int]:
    text = str(value or "").strip()
    if not text:
        text = DEFAULT_SESSION_TIME
    if ":" in text:
        hh, mm = text.split(":", 1)
    else:
        hh, mm = text[:2], text[2:4]
    hour = int(hh)
    minute = int(mm)
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"BAD_PAPER_SESSION_TIME:{value}")
    return hour, minute


def _schedule_config(root: Path) -> dict[str, Any]:
    for rel in ("config/aegis_paper_session_schedule.v1.json", "config/paper_session_schedule.v1.json"):
        path = root / rel
        if path.exists():
            payload = _read_json(path)
            if payload:
                return payload
    return {}


def scheduled_run_time_v1(*, truth_root: Path | str, day_utc: str) -> tuple[str, str, str]:
    root = Path(truth_root).expanduser().resolve()
    config = _schedule_config(root)
    timezone_name = str(os.environ.get("AEGIS_PAPER_SESSION_TIMEZONE") or config.get("session_timezone") or DEFAULT_SESSION_TIMEZONE)
    tz = ZoneInfo(timezone_name)
    explicit = str(os.environ.get("AEGIS_PAPER_SCHEDULED_RUN_TIME") or config.get("scheduled_run_time") or "").strip()
    if explicit:
        dt = datetime.fromisoformat(explicit.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return dt.astimezone(tz).replace(second=0, microsecond=0).isoformat(), timezone_name, "configured_scheduled_run_time"
    hhmm = str(os.environ.get("AEGIS_PAPER_SCHEDULED_HHMM") or config.get("scheduled_hhmm") or DEFAULT_SESSION_TIME)
    hour, minute = _parse_hhmm(hhmm)
    day = datetime.strptime(str(day_utc), "%Y-%m-%d").date()
    dt = datetime.combine(day, time(hour=hour, minute=minute), tzinfo=tz)
    return dt.isoformat(), timezone_name, "default_schedule" if not config and not os.environ.get("AEGIS_PAPER_SCHEDULED_HHMM") else "configured_scheduled_hhmm"


def paper_session_id_from_scheduled_time_v1(*, day_utc: str, scheduled_run_time: str, session_timezone: str = DEFAULT_SESSION_TIMEZONE) -> str:
    tz = ZoneInfo(session_timezone or DEFAULT_SESSION_TIMEZONE)
    dt = datetime.fromisoformat(str(scheduled_run_time).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    local = dt.astimezone(tz)
    return f"PAPER-{day_utc}-{local.strftime('%H%M')}"


def fallback_paper_session_id_from_generated_at_v1(*, day_utc: str, generated_at_utc: str) -> tuple[str, str]:
    tz = ZoneInfo(DEFAULT_SESSION_TIMEZONE)
    text = str(generated_at_utc or now_utc_v1()).strip()
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
    except Exception:
        dt = datetime.now(UTC).replace(microsecond=0)
    return f"PAPER-{day_utc}-{dt.astimezone(tz).strftime('%H%M')}", dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _event_id(event: Mapping[str, Any]) -> str:
    material = json.dumps({k: event.get(k) for k in ("event_type", "paper_session_id", "created_at", "source_tool", "source_artifact_path", "payload")}, sort_keys=True, separators=(",", ":"))
    return "paper-session-event:" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def _base_ledger(*, truth_root: Path | str, day_utc: str, generated_at: str | None = None) -> dict[str, Any]:
    generated = generated_at or now_utc_v1()
    scheduled, timezone_name, source = scheduled_run_time_v1(truth_root=truth_root, day_utc=day_utc)
    session_id = paper_session_id_from_scheduled_time_v1(day_utc=day_utc, scheduled_run_time=scheduled, session_timezone=timezone_name)
    event = {
        "event_type": "PAPER_SESSION_SCHEDULED",
        "paper_session_id": session_id,
        "created_at": generated,
        "source_tool": "ops.aegis.paper_session_ledger_v1.resolve_scheduled_paper_session_v1",
        "source_artifact_path": "",
        "payload": {"scheduled_run_time": scheduled, "session_timezone": timezone_name, "derivation_source": source},
    }
    event["event_id"] = _event_id(event)
    return {
        "schema_id": "aegis_paper_session_ledger",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at": generated,
        "generated_at_utc": generated,
        "sessions": [
            {
                "paper_session_id": session_id,
                "scheduled_run_time": scheduled,
                "session_timezone": timezone_name,
                "execution_started_at": None,
                "execution_completed_at": None,
                "canonicalized_at": None,
                "candidate_generated_at": None,
                "status": "SCHEDULED",
                "retry_count": 0,
                "reconstruction_count": 0,
                "events": [event],
            }
        ],
    }


def read_paper_session_ledger_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    path = paper_session_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    payload = _read_json(path)
    return payload if payload else _base_ledger(truth_root=truth_root, day_utc=day_utc)


def write_paper_session_ledger_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    ledger = payload or read_paper_session_ledger_v1(truth_root=root, day_utc=day_utc)
    return write_json_v1(paper_session_ledger_path_v1(truth_root=root, day_utc=day_utc), ledger)


def resolve_scheduled_paper_session_v1(*, truth_root: Path | str, day_utc: str, write: bool = True) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    ledger = read_paper_session_ledger_v1(truth_root=root, day_utc=day_utc)
    sessions = ledger.get("sessions") if isinstance(ledger.get("sessions"), list) else []
    if not sessions:
        ledger = _base_ledger(truth_root=root, day_utc=day_utc)
        sessions = ledger["sessions"]
    session = sessions[0]
    if write:
        write_paper_session_ledger_v1(truth_root=root, day_utc=day_utc, payload=ledger)
    return dict(session)


def append_paper_session_event_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    event_type: str,
    source_tool: str,
    source_artifact_path: str = "",
    payload: dict[str, Any] | None = None,
    created_at: str | None = None,
) -> tuple[dict[str, Any], Path]:
    root = Path(truth_root).expanduser().resolve()
    ledger = read_paper_session_ledger_v1(truth_root=root, day_utc=day_utc)
    session = resolve_scheduled_paper_session_v1(truth_root=root, day_utc=day_utc, write=False)
    sessions = ledger.get("sessions") if isinstance(ledger.get("sessions"), list) else []
    if not sessions:
        ledger = _base_ledger(truth_root=root, day_utc=day_utc)
        sessions = ledger["sessions"]
    session_row = sessions[0]
    session_id = str(session_row.get("paper_session_id") or session.get("paper_session_id") or "")
    created = created_at or now_utc_v1()
    event = {
        "event_type": str(event_type),
        "paper_session_id": session_id,
        "created_at": created,
        "source_tool": str(source_tool),
        "source_artifact_path": str(source_artifact_path or ""),
        "payload": payload or {},
    }
    event["event_id"] = _event_id(event)
    events = session_row.get("events") if isinstance(session_row.get("events"), list) else []
    if not any(isinstance(row, Mapping) and row.get("event_id") == event["event_id"] for row in events):
        events.append(event)
    session_row["events"] = events
    session_row["status"] = EVENT_STATUSES.get(str(event_type), str(session_row.get("status") or "SCHEDULED"))
    if event_type == "PAPER_SESSION_STARTED" and not session_row.get("execution_started_at"):
        session_row["execution_started_at"] = created
    if event_type == "CANDIDATES_GENERATED":
        session_row["candidate_generated_at"] = str((payload or {}).get("generated_at") or created)
        session_row["canonicalized_at"] = str((payload or {}).get("canonicalized_at") or created)
    if event_type == "PAPER_OPEN_AUTHORIZED" and not session_row.get("execution_started_at"):
        session_row["execution_started_at"] = created
    if event_type == "PAPER_TRADES_CONSTRUCTED":
        session_row["execution_completed_at"] = created
        session_row["canonicalized_at"] = str((payload or {}).get("canonicalized_at") or created)
    if event_type == "PAPER_SESSION_CLOSED":
        session_row["execution_completed_at"] = created
    if event_type == "PAPER_SESSION_RECONSTRUCTED":
        session_row["reconstruction_count"] = int(session_row.get("reconstruction_count") or 0) + 1
        session_row["retry_count"] = int(session_row.get("retry_count") or 0) + 1
        session_row["canonicalized_at"] = str((payload or {}).get("canonicalized_at") or created)
        session_row["latest_reconstruction_reason"] = str((payload or {}).get("reconstruction_reason") or "unspecified")
    ledger["generated_at"] = created
    ledger["generated_at_utc"] = created
    ledger["sessions"] = sessions
    path = write_paper_session_ledger_v1(truth_root=root, day_utc=day_utc, payload=ledger)
    return ledger, path


def ensure_payload_paper_session_v1(
    payload: dict[str, Any],
    *,
    truth_root: Path | str,
    day_utc: str,
    generated_at_utc: str = "",
    allow_generated_at_fallback: bool = True,
) -> dict[str, Any]:
    out = dict(payload or {})
    try:
        session = resolve_scheduled_paper_session_v1(truth_root=truth_root, day_utc=day_utc)
        out["paper_session_id"] = str(session.get("paper_session_id") or "")
        out["scheduled_run_time"] = str(session.get("scheduled_run_time") or "")
        out["session_timezone"] = str(session.get("session_timezone") or DEFAULT_SESSION_TIMEZONE)
        out["paper_session_id_derivation_source"] = "scheduled_run_time"
        out["run_timestamp_utc"] = str(out.get("run_timestamp_utc") or generated_at_utc or out.get("generated_at_utc") or now_utc_v1())
        return out
    except Exception:
        if not allow_generated_at_fallback:
            raise
    session_id, normalized = fallback_paper_session_id_from_generated_at_v1(day_utc=day_utc, generated_at_utc=generated_at_utc or str(out.get("generated_at_utc") or ""))
    out["paper_session_id"] = session_id
    out["run_timestamp_utc"] = str(out.get("run_timestamp_utc") or normalized)
    out["paper_session_id_derivation_source"] = "generated_at_fallback"
    return out
