from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from constellation_2.common.operator_alert_decision_v1 import build_operator_alert_decision_object_v1
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root


STATE_ROOT = (Path.home() / ".local/state/constellation_2").resolve()
RECEIPTS_ROOT = (STATE_ROOT / "operator_alert_receipts_v1").resolve()
GLOBAL_TRUTH_ROOT = resolve_canonical_truth_root()
REPO_ROOT = Path(__file__).resolve().parents[2]
ORCHESTRATOR_TIMER_PATH = (REPO_ROOT / "ops" / "systemd" / "user" / "c2-paper-day-orchestrator.timer").resolve()
_ON_CALENDAR_RE = re.compile(r"^(?:[A-Za-z,-]+\s+)?(?P<hour>\d{1,2}):(?P<minute>\d{2})\s+(?P<tz>\S+)$")


def _canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_bytes(payload)
    os.replace(tmp, path)


def _safe_load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


@lru_cache(maxsize=1)
def _load_bod_schedule() -> Tuple[int, int, str]:
    for raw_line in ORCHESTRATOR_TIMER_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line.startswith("OnCalendar="):
            continue
        match = _ON_CALENDAR_RE.match(line.split("=", 1)[1].strip())
        if not match:
            break
        return (
            int(match.group("hour")),
            int(match.group("minute")),
            str(match.group("tz")),
        )
    raise RuntimeError(f"INVALID_ORCHESTRATOR_TIMER_ONCALENDAR: {ORCHESTRATOR_TIMER_PATH}")


def _bod_time_label() -> str:
    hour, minute, tz_name = _load_bod_schedule()
    tz_label = "ET" if tz_name == "America/New_York" else tz_name
    return f"{hour:02d}:{minute:02d} {tz_label}"


def _bod_datetime_for_day(day_utc: str) -> datetime:
    hour, minute, tz_name = _load_bod_schedule()
    target_day = date.fromisoformat(str(day_utc).strip())
    return datetime(target_day.year, target_day.month, target_day.day, hour, minute, tzinfo=ZoneInfo(tz_name))


def _resolve_now_local(now: Optional[datetime]) -> datetime:
    _, _, tz_name = _load_bod_schedule()
    tz = ZoneInfo(tz_name)
    if now is None:
        return datetime.now(tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=tz)
    return now.astimezone(tz)


def _is_pre_bod(*, day_utc: str, now: Optional[datetime]) -> bool:
    return _resolve_now_local(now) < _bod_datetime_for_day(day_utc)


def _phase_semantics_enabled(*, day_utc: str, now: Optional[datetime]) -> bool:
    return str(day_utc).strip() == _resolve_now_local(now).date().isoformat()


def _semantic_overlay(
    *,
    day_utc: str,
    trading_day_state: Dict[str, Any],
    blocked_day: Dict[str, Any],
    state: str,
    heartbeat_status: str,
    first_failure: str,
    blocked_stage: str,
    orchestrator_started: Any,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    state_doc = dict(trading_day_state)
    state_doc.update(
        {
            "day_utc": day_utc,
            "state": state,
            "heartbeat_status": heartbeat_status,
            "first_failing_prerequisite": first_failure,
            "orchestrator_started": orchestrator_started,
        }
    )
    state_doc.setdefault("produced_utc", "")
    blocked_doc = dict(blocked_day)
    blocked_doc.update(
        {
            "day_utc": day_utc,
            "blocked_stage": blocked_stage,
            "failing_service": "c2-paper-day-orchestrator.timer",
            "failing_script": "ops/systemd/user/c2-paper-day-orchestrator.timer",
        }
    )
    blocked_doc.setdefault("evidence_paths", [str(ORCHESTRATOR_TIMER_PATH)])
    return state_doc, blocked_doc


def _semantic_status(*, pre_bod: bool, trading_day_state: Dict[str, Any]) -> str:
    if pre_bod:
        return "PRE_OPEN"
    if trading_day_state.get("orchestrator_started") is True:
        return "STARTED"
    return "PENDING"


def _execution_context() -> Dict[str, Any]:
    invocation_id = str(os.environ.get("INVOCATION_ID") or "").strip()
    journal_stream = str(os.environ.get("JOURNAL_STREAM") or "").strip()
    systemd_context_present = bool(invocation_id or journal_stream)
    return {
        "authoritative_invocation": bool(systemd_context_present),
        "invocation_source": "SYSTEMD_SERVICE" if systemd_context_present else "DIRECT_SHELL_OR_REPLAY",
        "systemd_invocation_id": invocation_id or None,
        "journal_stream_present": bool(journal_stream),
    }


def _trading_day_state_path(day_utc: str, truth_root: Path = GLOBAL_TRUTH_ROOT) -> Path:
    return (truth_root / "reports" / "trading_day_state_v1" / day_utc / "trading_day_state.v1.json").resolve()


def _blocked_day_path(day_utc: str, truth_root: Path = GLOBAL_TRUTH_ROOT) -> Path:
    return (truth_root / "reports" / "day_start_blocked_v1" / day_utc / "day_start_blocked.v1.json").resolve()


def _trading_day_state_machine_path(day_utc: str, truth_root: Path = GLOBAL_TRUTH_ROOT) -> Path:
    return (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").resolve()


def _receipt_path(day_utc: str) -> Path:
    return (RECEIPTS_ROOT / f"{day_utc}.json").resolve()


def _log_path(day_utc: str, truth_root: Path = GLOBAL_TRUTH_ROOT) -> Path:
    return (
        truth_root / "reports" / "operator_alert_log_v1" / day_utc / "operator_alert_log.v1.jsonl"
    ).resolve()


def _latest_pointer_path(truth_root: Path = GLOBAL_TRUTH_ROOT) -> Path:
    return (truth_root / "reports" / "operator_alert_log_v1" / "latest_pointer.v1.json").resolve()


def _rollup_path(day_utc: str, truth_root: Path = GLOBAL_TRUTH_ROOT) -> Path:
    return (
        truth_root / "reports" / "operator_alert_rollup_v1" / day_utc / "operator_alert_rollup.v1.json"
    ).resolve()


def _is_authoritative_doc(doc: Dict[str, Any]) -> bool:
    provenance = doc.get("provenance")
    if isinstance(provenance, dict):
        return bool(provenance.get("authoritative_write") is True)
    return False


def _blocked_stage_from_state_machine(doc: Dict[str, Any]) -> str:
    transitions = doc.get("state_transitions")
    if isinstance(transitions, list):
        for row in transitions:
            if not isinstance(row, dict):
                continue
            to_state = str(row.get("to_state") or "").strip().upper()
            if to_state in {"SUPPORTING_REGEN_BLOCKED", "SESSION_AUTHORITY_DENIED", "DAY_OPEN_BLOCKED"}:
                return to_state
        for row in reversed(transitions):
            if isinstance(row, dict):
                to_state = str(row.get("to_state") or "").strip().upper()
                if to_state:
                    return to_state
    return str(doc.get("final_start_decision") or "UNKNOWN").strip().upper() or "UNKNOWN"


def _legacy_surfaces_from_state_machine(day_utc: str, state_machine_path: Path) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    state_machine = _safe_load_json(state_machine_path)
    if not state_machine:
        return {}, {}

    final_start_decision = str(state_machine.get("final_start_decision") or "").strip().upper()
    first_true_blocker = state_machine.get("first_true_blocker")
    if not isinstance(first_true_blocker, dict):
        first_true_blocker = {}
    blocker_code = ""
    if final_start_decision in {"BLOCKED_VALID", "BLOCKED_BY_DEFECT"}:
        blocker_code = str(
            first_true_blocker.get("first_true_blocker_code") or final_start_decision or "UNKNOWN"
        ).strip()
    blocker_path = str(first_true_blocker.get("first_true_blocker_artifact_path") or str(state_machine_path)).strip()
    blocked_stage = _blocked_stage_from_state_machine(state_machine)
    alertable = final_start_decision in {"BLOCKED_VALID", "BLOCKED_BY_DEFECT"}
    trading_day_state = {
        "schema_id": "trading_day_state_legacy_projection",
        "schema_version": "v1",
        "day_utc": day_utc,
        "state": "BLOCKED" if alertable else "PASS",
        "heartbeat_status": "FAIL" if alertable else "PASS",
        "first_failing_prerequisite": blocker_code,
        "first_failing_path": blocker_path,
        "orchestrator_started": False,
        "produced_utc": str(state_machine.get("evaluated_at_utc") or ""),
        "producer": {"repo": "constellation", "module": "ops/tools/run_trading_day_state_machine_v1.py"},
        "evidence_paths": [
            item
            for item in [
                str(state_machine_path),
                blocker_path,
                str(
                    ((state_machine.get("supporting_daily_control_refs") or {}).get("trading_day_execution_control_plane_path") or "")
                ).strip(),
            ]
            if item
        ],
        "provenance": {"authoritative_write": True, "derived_from": "trading_day_state_machine_v1"},
    }
    blocked_day = {
        "schema_id": "day_start_blocked_legacy_projection",
        "schema_version": "v1",
        "day_utc": day_utc,
        "blocked_stage": blocked_stage,
        "failing_service": "trading-day-state-machine",
        "failing_script": "ops/tools/run_trading_day_state_machine_v1.py",
        "producer": {"repo": "constellation", "module": "ops/tools/run_trading_day_state_machine_v1.py"},
        "evidence_paths": [path for path in [str(state_machine_path), blocker_path] if path],
        "provenance": {"authoritative_write": True, "derived_from": "trading_day_state_machine_v1"},
    }
    return trading_day_state, blocked_day


def _alert_key(trading_day_state: Dict[str, Any], blocked_day: Dict[str, Any]) -> str:
    key_obj = {
        "day_utc": str(trading_day_state.get("day_utc") or ""),
        "state": str(trading_day_state.get("state") or ""),
        "heartbeat_status": str(trading_day_state.get("heartbeat_status") or ""),
        "first_failing_prerequisite": str(trading_day_state.get("first_failing_prerequisite") or ""),
        "blocked_stage": str(blocked_day.get("blocked_stage") or ""),
    }
    return hashlib.sha256(_canonical_json_bytes(key_obj)).hexdigest()


def _build_message(
    trading_day_state: Dict[str, Any],
    blocked_day: Dict[str, Any],
    *,
    semantic_status: Optional[str] = None,
) -> Tuple[str, str]:
    day_utc = str(trading_day_state.get("day_utc") or "UNKNOWN")
    state = str(trading_day_state.get("state") or "UNKNOWN")
    heartbeat = str(trading_day_state.get("heartbeat_status") or "UNKNOWN")
    first_fail = str(trading_day_state.get("first_failing_prerequisite") or "UNKNOWN")
    blocked_stage = str(blocked_day.get("blocked_stage") or trading_day_state.get("state") or "UNKNOWN")
    if semantic_status == "PRE_OPEN":
        summary = f"Constellation: PRE_OPEN ({day_utc})"
        body = (
            f"PRE_OPEN ({day_utc})\n"
            f"Waiting for BOD ({_bod_time_label()})\n"
            "Orchestrator: NOT STARTED (EXPECTED)"
        )
        return summary, body
    if semantic_status == "PENDING":
        summary = f"Constellation: ALERT: PENDING ({day_utc})"
        body = (
            f"PENDING ({day_utc})\n"
            f"BOD passed ({_bod_time_label()})\n"
            "Orchestrator: NOT STARTED"
        )
        return summary, body
    if semantic_status == "STARTED" and state == "PASS":
        summary = f"Constellation: STARTED ({day_utc})"
        body = (
            f"STARTED ({day_utc})\n"
            f"Heartbeat: {heartbeat}\n"
            "Orchestrator: STARTED"
        )
        return summary, body
    orchestrator_started = "yes" if trading_day_state.get("orchestrator_started") is True else "no"
    summary = f"Constellation trading day alert: {day_utc} {state}"
    body = (
        f"day={day_utc} state={state} heartbeat={heartbeat} "
        f"first_failure={first_fail} blocked_stage={blocked_stage} orchestrator_started={orchestrator_started}"
    )
    return summary, body


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _alert_severity(decision: Dict[str, Any]) -> str:
    return str(decision.get("severity") or "INFO").upper()


def _transport_configuration_notes() -> List[str]:
    notes: List[str] = []
    email_configured = all(
        str(os.environ.get(name) or "").strip()
        for name in (
            "C2_EMAIL_SMTP_HOST",
            "C2_EMAIL_SMTP_PORT",
            "C2_EMAIL_USERNAME",
            "C2_EMAIL_PASSWORD",
            "C2_EMAIL_FROM",
            "C2_EMAIL_TO",
        )
    )
    sms_configured = bool(str(os.environ.get("C2_SMS_WEBHOOK_URL") or "").strip())
    if not email_configured:
        notes.append("EMAIL_CHANNEL_NOT_CONFIGURED")
    if not sms_configured:
        notes.append("SMS_CHANNEL_NOT_CONFIGURED")
    return notes


def _reason_with_transport_notes(base_reason: str) -> str:
    parts = [str(base_reason or "").strip()] if str(base_reason or "").strip() else []
    parts.extend(_transport_configuration_notes())
    return ";".join(parts)


def _related_artifact_paths(decision: Dict[str, Any]) -> List[str]:
    doc = decision.get("trading_day_state") if isinstance(decision.get("trading_day_state"), dict) else {}
    blocked = decision.get("blocked_day") if isinstance(decision.get("blocked_day"), dict) else {}
    items: List[str] = []
    for value in (
        decision.get("trading_day_state_path"),
        decision.get("blocked_day_path"),
        doc.get("first_failing_path"),
    ):
        text = str(value or "").strip()
        if text:
            items.append(text)
    for field in ("evidence_paths",):
        raw = doc.get(field)
        if isinstance(raw, list):
            items.extend(str(item).strip() for item in raw if str(item).strip())
        raw = blocked.get(field)
        if isinstance(raw, list):
            items.extend(str(item).strip() for item in raw if str(item).strip())
    return sorted(set(items))


def _validate_log_row(row: Dict[str, Any]) -> None:
    required_text_fields = (
        "schema_id",
        "schema_version",
        "alert_key",
        "day_utc",
        "produced_utc",
        "severity",
        "summary",
        "body",
        "channel",
        "reason",
        "source_service",
        "source_script",
        "source_authority",
        "trading_day_state_path",
        "producer",
    )
    for field in required_text_fields:
        value = row.get(field)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"OPERATOR_ALERT_LOG_ROW_MALFORMED: {field}")
    for field in ("emitted", "dedup_suppressed"):
        if not isinstance(row.get(field), bool):
            raise RuntimeError(f"OPERATOR_ALERT_LOG_ROW_MALFORMED: {field}")
    related = row.get("related_artifact_paths")
    if not isinstance(related, list) or any(not isinstance(item, str) or not item.strip() for item in related):
        raise RuntimeError("OPERATOR_ALERT_LOG_ROW_MALFORMED: related_artifact_paths")


def _append_jsonl(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        os.write(fd, payload)
    finally:
        os.close(fd)


def _build_log_row(decision: Dict[str, Any]) -> Dict[str, Any]:
    doc = decision.get("trading_day_state") if isinstance(decision.get("trading_day_state"), dict) else {}
    blocked = decision.get("blocked_day") if isinstance(decision.get("blocked_day"), dict) else {}
    blocked_producer = blocked.get("producer") if isinstance(blocked.get("producer"), dict) else {}
    state_producer = doc.get("producer") if isinstance(doc.get("producer"), dict) else {}
    source_script = str(blocked.get("failing_script") or state_producer.get("module") or "UNKNOWN").strip()
    source_service = str(blocked.get("failing_service") or "UNKNOWN").strip()
    producer = str(blocked_producer.get("module") or state_producer.get("module") or __name__).strip()
    row = {
        "schema_id": "operator_alert_log",
        "schema_version": "v1",
        "alert_key": str(decision.get("alert_key") or "").strip(),
        "day_utc": str(decision.get("day_utc") or "").strip(),
        "produced_utc": _now_utc_iso(),
        "severity": _alert_severity(decision),
        "summary": str(decision.get("summary") or "").strip(),
        "body": str(decision.get("body") or "").strip(),
        "channel": str(decision.get("channel") or "none").strip(),
        "emitted": bool(decision.get("emitted")),
        "dedup_suppressed": bool(decision.get("dedup_suppressed")),
        "reason": _reason_with_transport_notes(str(decision.get("reason") or "").strip()),
        "source_service": source_service,
        "source_script": source_script,
        "source_authority": str(decision.get("authority_source") or "UNKNOWN").strip(),
        "trading_day_state_path": str(decision.get("trading_day_state_path") or "").strip(),
        "related_artifact_paths": _related_artifact_paths(decision),
        "producer": producer,
    }
    _validate_log_row(row)
    return row


def _update_rollup_and_pointer(*, row: Dict[str, Any], truth_root: Path) -> None:
    day_utc = str(row["day_utc"])
    log_path = _log_path(day_utc, truth_root)
    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    docs = [json.loads(line) for line in lines]
    if not docs:
        raise RuntimeError(f"OPERATOR_ALERT_LOG_EMPTY_AFTER_APPEND: {log_path}")
    emitted_count = sum(1 for item in docs if bool(item.get("emitted")))
    dedup_count = sum(1 for item in docs if bool(item.get("dedup_suppressed")))
    severity_counts: Dict[str, int] = {}
    for item in docs:
        severity = str(item.get("severity") or "UNKNOWN")
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
    latest = docs[-1]
    rollup = {
        "schema_id": "operator_alert_rollup",
        "schema_version": "v1",
        "day_utc": day_utc,
        "log_path": str(log_path),
        "alert_count": len(docs),
        "emitted_count": emitted_count,
        "dedup_suppressed_count": dedup_count,
        "severity_counts": severity_counts,
        "latest_alert_key": str(latest.get("alert_key") or ""),
        "latest_produced_utc": str(latest.get("produced_utc") or ""),
        "latest_summary": str(latest.get("summary") or ""),
        "latest_reason": str(latest.get("reason") or ""),
    }
    _atomic_write(_rollup_path(day_utc, truth_root), _canonical_json_bytes(rollup))
    pointer = {
        "schema_id": "operator_alert_log_latest_pointer",
        "schema_version": "v1",
        "day_utc": day_utc,
        "log_path": str(log_path),
        "rollup_path": str(_rollup_path(day_utc, truth_root)),
        "latest_alert_key": str(latest.get("alert_key") or ""),
        "latest_produced_utc": str(latest.get("produced_utc") or ""),
        "latest_summary": str(latest.get("summary") or ""),
    }
    _atomic_write(_latest_pointer_path(truth_root), _canonical_json_bytes(pointer))


def _persist_operator_alert_event(*, decision: Dict[str, Any], truth_root: Path) -> Dict[str, Any]:
    row = _build_log_row(decision)
    log_path = _log_path(str(decision.get("day_utc") or ""), truth_root)
    _append_jsonl(log_path, _canonical_json_bytes(row))
    _update_rollup_and_pointer(row=row, truth_root=truth_root)
    decision["operator_alert_log_path"] = str(log_path)
    decision["operator_alert_rollup_path"] = str(_rollup_path(str(decision.get("day_utc") or ""), truth_root))
    decision["operator_alert_latest_pointer_path"] = str(_latest_pointer_path(truth_root))
    return decision


def build_operator_alert_decision(
    *,
    day_utc: str,
    truth_root: Path = GLOBAL_TRUTH_ROOT,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    trading_day_state_path = _trading_day_state_path(day_utc, truth_root)
    blocked_day_path = _blocked_day_path(day_utc, truth_root)
    trading_day_state = _safe_load_json(trading_day_state_path)
    blocked_day_raw = _safe_load_json(blocked_day_path)
    blocked_day = blocked_day_raw if _is_authoritative_doc(blocked_day_raw) else {}
    authority_source = "legacy_day_start_bridge_v1"
    if not trading_day_state:
        state_machine_path = _trading_day_state_machine_path(day_utc, truth_root)
        trading_day_state, blocked_day = _legacy_surfaces_from_state_machine(day_utc, state_machine_path)
        if trading_day_state:
            trading_day_state_path = state_machine_path
            blocked_day_path = state_machine_path
            authority_source = "trading_day_state_machine_v1_projection"
        else:
            authority_source = "missing_authoritative_startup_surface"
    decision = build_operator_alert_decision_object_v1(
        day_utc=day_utc,
        trading_day_state=trading_day_state,
        blocked_day=blocked_day,
        authority_source=authority_source,
        trading_day_state_path=str(trading_day_state_path),
        blocked_day_path=str(blocked_day_path),
        now=now,
    )
    decision["semantic_status"] = str(decision.get("state") or "")
    decision["receipt_path"] = str(_receipt_path(day_utc))
    if decision["state"] == "PRE_OPEN_WAITING":
        decision["reason"] = "PRE_OPEN_WAITING_FOR_BOD"
    elif decision["state"] == "BOD_DUE_NOT_STARTED":
        decision["reason"] = "ORCHESTRATOR_NOT_STARTED_AFTER_BOD_GRACE"
    elif decision["notify_email"] or decision["notify_desktop"]:
        decision["reason"] = "ACTIONABLE_ALERT_STATE"
    else:
        decision["reason"] = "STATE_NOT_ACTIONABLE"
    return decision


def _load_receipt(path: Path) -> Dict[str, Any]:
    return _safe_load_json(path)


def _write_receipt(*, decision: Dict[str, Any], channel: str) -> Path:
    path = Path(str(decision["receipt_path"])).resolve()
    payload = {
        "day_utc": str(decision.get("day_utc") or ""),
        "alert_key": str(decision.get("dedupe_key") or decision.get("alert_key") or ""),
        "phase": str(decision.get("phase") or ""),
        "state": str(decision.get("state") or ""),
        "severity": str(decision.get("severity") or ""),
        "notify_email": bool(decision.get("notify_email")),
        "notify_desktop": bool(decision.get("notify_desktop")),
        "notify_log": bool(decision.get("notify_log")),
        "first_failing_prerequisite": str(decision.get("first_failure") or ""),
        "blocked_stage": str(decision.get("blocked_stage") or ""),
        "trading_day_state_path": str(decision.get("trading_day_state_path") or ""),
        "blocked_day_path": str(decision.get("blocked_day_path") or ""),
        "channel": channel,
        "source_produced_utc": str((decision.get("trading_day_state") or {}).get("produced_utc") or ""),
    }
    _atomic_write(path, _canonical_json_bytes(payload))
    return path


def emit_operator_alert_for_day(
    *,
    day_utc: str,
    truth_root: Path = GLOBAL_TRUTH_ROOT,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    ctx = _execution_context()
    decision = build_operator_alert_decision(day_utc=day_utc, truth_root=truth_root, now=now)
    decision["execution_context"] = ctx
    if not ctx["authoritative_invocation"]:
        decision["emitted"] = False
        decision["dedup_suppressed"] = False
        decision["channel"] = None
        decision["reason"] = "NONAUTHORITATIVE_INVOCATION"
        return decision
    if not (bool(decision.get("notify_desktop")) or bool(decision.get("notify_log"))):
        decision["emitted"] = False
        decision["dedup_suppressed"] = False
        decision["channel"] = None
        return decision
    receipt = _load_receipt(Path(str(decision["receipt_path"])))
    if str(receipt.get("alert_key") or "") == str(decision.get("dedupe_key") or decision.get("alert_key") or ""):
        decision["emitted"] = False
        decision["dedup_suppressed"] = True
        decision["channel"] = str(receipt.get("channel") or "")
        decision["reason"] = "DEDUP_SUPPRESSED"
        return decision

    summary = str(decision.get("summary") or "Constellation trading day alert")
    body = str(decision.get("body") or "")
    emitted = False
    channel = ""
    notify_send = shutil.which("notify-send")
    if notify_send and bool(decision.get("notify_desktop")):
        rc = subprocess.run(
            [notify_send, "--app-name=Constellation", summary, body],
            text=True,
            capture_output=True,
            check=False,
        ).returncode
        if rc == 0:
            emitted = True
            channel = "notify-send"
    if not emitted and bool(decision.get("notify_log")):
        logger_bin = shutil.which("logger")
        if logger_bin:
            rc = subprocess.run(
                [logger_bin, "-t", "constellation-operator-alert", f"{summary} | {body}"],
                text=True,
                capture_output=True,
                check=False,
            ).returncode
            if rc == 0:
                emitted = True
                channel = "logger"

    decision["emitted"] = emitted
    decision["dedup_suppressed"] = False
    decision["channel"] = channel or None
    if emitted or bool(decision.get("notify_log")):
        receipt_path = _write_receipt(decision=decision, channel=channel or "durable_log")
        decision["receipt_path"] = str(receipt_path)
    if not emitted and not channel:
        decision["reason"] = "ALERT_EMISSION_FAILED"
    return _persist_operator_alert_event(decision=decision, truth_root=truth_root)
