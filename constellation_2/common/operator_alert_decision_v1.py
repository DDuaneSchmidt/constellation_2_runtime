from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[2]
ORCHESTRATOR_TIMER_PATH = (REPO_ROOT / "ops" / "systemd" / "user" / "c2-paper-day-orchestrator.timer").resolve()
ON_CALENDAR_RE = re.compile(r"^(?:[A-Za-z,-]+\s+)?(?P<hour>\d{1,2}):(?P<minute>\d{2})\s+(?P<tz>\S+)$")
BOD_GRACE_MINUTES = 5
MARKET_CLOSE_HOUR = 16
PASS_STATES = {"PASS", "READY", "READY_NOW", "COMPLETED", "SUCCESS", "HEALTHY", "CURRENT"}
FAIL_STATES = {"FAILED", "UNKNOWN_FAILURE", "ERROR", "ABORTED"}
BLOCKED_STATES = {"BLOCKED"}
PENDING_STATES = {"PENDING", "RUNNING", "STARTING"}
PASS_HEARTBEATS = {"PASS", "OK", "HEALTHY"}
TRANSITION_HASH_FIELDS = (
    "day_utc",
    "phase",
    "state",
    "severity",
    "first_failure",
    "blocked_stage",
    "orchestrator_started",
    "next_expected_event",
    "raw_state",
    "raw_heartbeat_status",
)


def _canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


@lru_cache(maxsize=1)
def _load_bod_schedule() -> Tuple[int, int, str]:
    for raw_line in ORCHESTRATOR_TIMER_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line.startswith("OnCalendar="):
            continue
        match = ON_CALENDAR_RE.match(line.split("=", 1)[1].strip())
        if not match:
            break
        return int(match.group("hour")), int(match.group("minute")), str(match.group("tz"))
    raise RuntimeError(f"INVALID_ORCHESTRATOR_TIMER_ONCALENDAR: {ORCHESTRATOR_TIMER_PATH}")


def bod_time_label_v1() -> str:
    hour, minute, tz_name = _load_bod_schedule()
    tz_label = "ET" if tz_name == "America/New_York" else tz_name
    return f"{hour:02d}:{minute:02d} {tz_label}"


def _resolve_now_local(now: Optional[datetime]) -> datetime:
    _, _, tz_name = _load_bod_schedule()
    tz = ZoneInfo(tz_name)
    if now is None:
        return datetime.now(tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=tz)
    return now.astimezone(tz)


def _target_day(day_utc: str) -> date:
    return date.fromisoformat(str(day_utc).strip())


def bod_datetime_for_day_v1(day_utc: str) -> datetime:
    hour, minute, tz_name = _load_bod_schedule()
    day = _target_day(day_utc)
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ZoneInfo(tz_name))


def bod_grace_deadline_for_day_v1(day_utc: str) -> datetime:
    return bod_datetime_for_day_v1(day_utc) + timedelta(minutes=BOD_GRACE_MINUTES)


def _session_close_for_day(day_utc: str) -> datetime:
    _, _, tz_name = _load_bod_schedule()
    day = _target_day(day_utc)
    return datetime(day.year, day.month, day.day, MARKET_CLOSE_HOUR, 0, tzinfo=ZoneInfo(tz_name))


def _phase_for_day(day_utc: str, now: Optional[datetime]) -> str:
    local_now = _resolve_now_local(now)
    target_day = _target_day(day_utc)
    if local_now.date() < target_day:
        return "PRE_OPEN"
    if local_now.date() > target_day:
        return "POST_CLOSE"
    bod_time = bod_datetime_for_day_v1(day_utc)
    grace_deadline = bod_grace_deadline_for_day_v1(day_utc)
    close_time = _session_close_for_day(day_utc)
    if local_now < bod_time:
        return "PRE_OPEN"
    if local_now < grace_deadline:
        return "BOD_WINDOW"
    if local_now < close_time:
        return "ACTIVE_SESSION"
    return "POST_CLOSE"


def _raw_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_bool(value: Any) -> Optional[bool]:
    if value is True:
        return True
    if value is False:
        return False
    text = _raw_text(value).upper()
    if text in {"TRUE", "YES", "STARTED", "ACTIVE"}:
        return True
    if text in {"FALSE", "NO", "NOT_STARTED"}:
        return False
    return None


def _orchestrator_status(raw_value: Any, state: str) -> str:
    normalized = _normalize_bool(raw_value)
    if state == "PRE_OPEN_WAITING":
        return "NOT_EXPECTED"
    if normalized is True:
        return "STARTED"
    if normalized is False:
        return "NOT_STARTED"
    return "UNKNOWN"


def _effective_blocked_stage(raw_blocked_stage: str, state: str) -> str:
    if state == "PRE_OPEN_WAITING":
        return "WAITING_FOR_BOD"
    if state == "STARTING":
        return "WAITING_FOR_STARTUP_EVIDENCE"
    if state == "BOD_DUE_NOT_STARTED":
        return "WAITING_FOR_ORCHESTRATOR_START"
    return raw_blocked_stage or state or "UNKNOWN"


def _effective_first_failure(raw_first_failure: str, state: str) -> str:
    if state in {"PRE_OPEN_WAITING", "STARTING", "BOD_DUE_NOT_STARTED", "STARTED_HEALTHY"}:
        return "N/A"
    return raw_first_failure or "UNKNOWN"


def _derive_state(
    *,
    raw_state: str,
    raw_heartbeat: str,
    raw_orchestrator_started: Any,
    phase: str,
) -> str:
    started = _normalize_bool(raw_orchestrator_started)
    if raw_state in FAIL_STATES:
        return "FAILED"
    if raw_state in BLOCKED_STATES:
        return "BLOCKED"
    if started is True:
        if raw_state in PENDING_STATES or raw_heartbeat in {"PENDING", "UNKNOWN", "N/A", ""}:
            return "STARTING"
        if raw_state in PASS_STATES and raw_heartbeat in PASS_HEARTBEATS:
            return "STARTED_HEALTHY"
        return "STARTED_DEGRADED"
    if phase == "PRE_OPEN":
        return "PRE_OPEN_WAITING"
    if phase == "BOD_WINDOW":
        return "STARTING"
    if phase == "ACTIVE_SESSION":
        return "BOD_DUE_NOT_STARTED"
    return "UNKNOWN"


def _severity_for_state(state: str, phase: str) -> str:
    if state in {"FAILED"}:
        return "CRITICAL"
    if state in {"BLOCKED", "BOD_DUE_NOT_STARTED", "STARTED_DEGRADED"}:
        return "ERROR"
    if state == "UNKNOWN" and phase in {"ACTIVE_SESSION", "POST_CLOSE"}:
        return "ERROR"
    if state == "UNKNOWN":
        return "WARN"
    return "INFO"


def _channel_policy_for_state(state: str, phase: str) -> Dict[str, bool]:
    notify_email = False
    notify_desktop = False
    notify_log = True
    if state in {"BLOCKED", "FAILED", "STARTED_DEGRADED", "BOD_DUE_NOT_STARTED"}:
        notify_email = True
        notify_desktop = True
    elif state == "UNKNOWN" and phase in {"ACTIVE_SESSION", "POST_CLOSE"}:
        notify_email = True
        notify_desktop = True
    return {
        "notify_email": notify_email,
        "notify_desktop": notify_desktop,
        "notify_log": notify_log,
    }


def _next_expected(day_utc: str, state: str) -> Tuple[str, Optional[str]]:
    bod_time = bod_datetime_for_day_v1(day_utc)
    grace_deadline = bod_grace_deadline_for_day_v1(day_utc)
    if state == "PRE_OPEN_WAITING":
        return "BOD_TIMER_FIRE", bod_time.isoformat()
    if state == "STARTING":
        return "STARTUP_EVIDENCE", grace_deadline.isoformat()
    if state == "BOD_DUE_NOT_STARTED":
        return "STARTUP_EVIDENCE", grace_deadline.isoformat()
    if state in {"STARTED_HEALTHY", "STARTED_DEGRADED"}:
        return "ENGINE_HEARTBEAT", None
    if state in {"BLOCKED", "FAILED"}:
        return "OPERATOR_INTERVENTION", None
    return "OPERATOR_REVIEW", None


def _operator_message(
    *,
    day_utc: str,
    state: str,
    severity: str,
    blocked_stage: str,
    first_failure: str,
) -> str:
    bod_label = bod_time_label_v1()
    if state == "PRE_OPEN_WAITING":
        return f"Waiting for BOD ({bod_label}). Orchestrator has not started because it is not expected yet."
    if state == "STARTING":
        return (
            f"BOD reached at {bod_label}. Waiting for startup evidence inside the "
            f"{BOD_GRACE_MINUTES}-minute grace window."
        )
    if state == "BOD_DUE_NOT_STARTED":
        return (
            f"BOD passed at {bod_label} and startup has not been observed after the "
            f"{BOD_GRACE_MINUTES}-minute grace window."
        )
    if state == "STARTED_HEALTHY":
        return "Startup observed and the system is healthy."
    if state == "STARTED_DEGRADED":
        return "Startup observed, but the system is degraded. Review heartbeat and failure evidence."
    if state == "BLOCKED":
        return f"Trading day start is blocked at {blocked_stage}. First failure: {first_failure}."
    if state == "FAILED":
        return f"Trading day start failed. First failure: {first_failure}."
    return (
        f"Alert state is unresolved with severity {severity}. "
        f"Blocked stage={blocked_stage or 'UNKNOWN'} first failure={first_failure}."
    )


def _summary(day_utc: str, severity: str, state: str) -> str:
    return f"Constellation: {severity}: {state} ({day_utc})"


def _body(
    *,
    day_utc: str,
    phase: str,
    state: str,
    severity: str,
    operator_message: str,
    first_failure: str,
    blocked_stage: str,
    orchestrator_started: str,
    next_expected_event: str,
    next_expected_time_et: Optional[str],
) -> str:
    lines = [
        f"{state} ({day_utc})",
        f"Phase: {phase}",
        f"Severity: {severity}",
        operator_message,
        f"First failure: {first_failure}",
        f"Blocked stage: {blocked_stage}",
        f"Orchestrator: {orchestrator_started}",
        f"Next expected event: {next_expected_event}",
    ]
    if next_expected_time_et:
        lines.append(f"Next expected time ET: {next_expected_time_et}")
    return "\n".join(lines)


def build_operator_alert_decision_object_v1(
    *,
    day_utc: str,
    trading_day_state: Optional[Mapping[str, Any]],
    blocked_day: Optional[Mapping[str, Any]],
    authority_source: str,
    trading_day_state_path: str,
    blocked_day_path: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    trading_doc = dict(trading_day_state or {})
    blocked_doc = dict(blocked_day or {})
    raw_state = _raw_text(trading_doc.get("state")).upper()
    raw_heartbeat = _raw_text(trading_doc.get("heartbeat_status")).upper()
    raw_first_failure = _raw_text(trading_doc.get("first_failing_prerequisite"))
    raw_blocked_stage = _raw_text(blocked_doc.get("blocked_stage")).upper()
    phase = _phase_for_day(day_utc, now)
    normalized_state = _derive_state(
        raw_state=raw_state,
        raw_heartbeat=raw_heartbeat,
        raw_orchestrator_started=trading_doc.get("orchestrator_started"),
        phase=phase,
    )
    severity = _severity_for_state(normalized_state, phase)
    policy = _channel_policy_for_state(normalized_state, phase)
    orchestrator_started = _orchestrator_status(trading_doc.get("orchestrator_started"), normalized_state)
    blocked_stage = _effective_blocked_stage(raw_blocked_stage, normalized_state)
    first_failure = _effective_first_failure(raw_first_failure, normalized_state)
    next_expected_event, next_expected_time_et = _next_expected(day_utc, normalized_state)
    operator_message = _operator_message(
        day_utc=day_utc,
        state=normalized_state,
        severity=severity,
        blocked_stage=blocked_stage,
        first_failure=first_failure,
    )
    payload = {
        "schema_id": "operator_alert_decision",
        "schema_version": "v1",
        "day": day_utc,
        "day_utc": day_utc,
        "phase": phase,
        "state": normalized_state,
        "severity": severity,
        "notify_email": bool(policy["notify_email"]),
        "notify_desktop": bool(policy["notify_desktop"]),
        "notify_log": bool(policy["notify_log"]),
        "alert_required": any(bool(policy[name]) for name in ("notify_email", "notify_desktop", "notify_log")),
        "operator_message": operator_message,
        "first_failure": first_failure,
        "blocked_stage": blocked_stage,
        "orchestrator_started": orchestrator_started,
        "next_expected_event": next_expected_event,
        "next_expected_time_et": next_expected_time_et,
        "authority_source": authority_source,
        "trading_day_state_path": trading_day_state_path,
        "blocked_day_path": blocked_day_path,
        "trading_day_state": trading_doc,
        "blocked_day": blocked_doc,
        "evidence": {
            "raw_state": raw_state or "UNKNOWN",
            "raw_heartbeat_status": raw_heartbeat or "UNKNOWN",
            "raw_blocked_stage": raw_blocked_stage or "UNKNOWN",
            "raw_orchestrator_started": trading_doc.get("orchestrator_started"),
            "first_failing_prerequisite": raw_first_failure or "",
            "authority_source": authority_source,
        },
    }
    signature = {
        field_name: payload.get(field_name) or payload["evidence"].get(field_name)
        for field_name in TRANSITION_HASH_FIELDS
    }
    dedupe_key = hashlib.sha256(_canonical_json_bytes(signature)).hexdigest()
    payload["dedupe_key"] = dedupe_key
    payload["alert_key"] = dedupe_key
    payload["summary"] = _summary(day_utc, severity, normalized_state)
    payload["body"] = _body(
        day_utc=day_utc,
        phase=phase,
        state=normalized_state,
        severity=severity,
        operator_message=operator_message,
        first_failure=first_failure,
        blocked_stage=blocked_stage,
        orchestrator_started=orchestrator_started,
        next_expected_event=next_expected_event,
        next_expected_time_et=next_expected_time_et,
    )
    return payload
