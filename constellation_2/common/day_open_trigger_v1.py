from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from zoneinfo import ZoneInfo

from constellation_2.common.day_open_window_v1 import build_day_open_window_v1
from constellation_2.common.day_open_policy_v1 import (
    INITIAL_TRIGGER_KIND,
    LATE_TRIGGER_KIND,
    NO_TRIGGER_KIND,
    build_day_open_policy_snapshot,
    normalize_day_open_environment,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_day_open_attempt_path,
    resolve_day_open_trigger_path,
    resolve_paper_session_authority_path,
    resolve_paper_session_ledger_path,
)
from constellation_2.common.paper_session_authority_v1 import read_paper_session_authority_ref_v1
from constellation_2.common.session_authority_monitor_v1 import resolve_session_authority_status_path
from constellation_2.common.session_authority_v1 import (
    resolve_active_session_path,
    resolve_target_day_admission_path,
)


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/day_open_trigger.v1.schema.json"


def _load_existing_trigger(*, truth_root: Path, day_utc: str) -> Dict[str, Any] | None:
    trigger_path = resolve_day_open_trigger_path(truth_root=truth_root, day_utc=day_utc)
    if not trigger_path.exists() or not trigger_path.is_file():
        return None
    payload = read_json_object_v1(trigger_path)
    return payload if isinstance(payload, dict) else None


def _load_attempt(*, truth_root: Path, day_utc: str) -> Dict[str, Any] | None:
    attempt_path = resolve_day_open_attempt_path(truth_root=truth_root, day_utc=day_utc)
    if not attempt_path.exists() or not attempt_path.is_file():
        return None
    payload = read_json_object_v1(attempt_path)
    return payload if isinstance(payload, dict) else None


def _trigger_entry(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "trigger_sequence": int(payload.get("trigger_sequence") or 0),
        "trigger_kind": str(payload.get("trigger_kind") or NO_TRIGGER_KIND).strip().upper() or NO_TRIGGER_KIND,
        "trigger_status": str(payload.get("trigger_status") or "").strip().upper(),
        "trigger_reason_code": str(payload.get("trigger_reason_code") or "").strip(),
        "emitted_at_utc": str(payload.get("emitted_at_utc") or "").strip(),
        "consumed": bool(payload.get("consumed") is True),
        "consumed_at_utc": str(payload.get("consumed_at_utc") or "").strip(),
        "dedupe_key": str(payload.get("dedupe_key") or "").strip(),
    }


def _trigger_history(existing: Dict[str, Any] | None) -> list[Dict[str, Any]]:
    if not isinstance(existing, dict):
        return []
    history = existing.get("trigger_history")
    rows = [dict(row) for row in history if isinstance(row, dict)] if isinstance(history, list) else []
    rows.sort(key=lambda row: int(row.get("trigger_sequence") or 0))
    return rows


def _append_prior_trigger(history: list[Dict[str, Any]], existing: Dict[str, Any] | None) -> list[Dict[str, Any]]:
    if not isinstance(existing, dict):
        return history
    sequence = int(existing.get("trigger_sequence") or 0)
    status = str(existing.get("trigger_status") or "").strip().upper()
    if sequence <= 0 or not status:
        return history
    if any(int(row.get("trigger_sequence") or 0) == sequence for row in history):
        return history
    history.append(_trigger_entry(existing))
    history.sort(key=lambda row: int(row.get("trigger_sequence") or 0))
    return history


def _is_terminal_attempt(payload: Dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    return str(payload.get("final_classification") or "").strip().upper() in {
        "OPEN_SUCCEEDED",
        "OPEN_FAILED",
        "OPEN_MISSED",
    }


def _kind_for_window(window_status: str) -> str:
    return INITIAL_TRIGGER_KIND if window_status == "OPEN_WINDOW" else NO_TRIGGER_KIND


def build_day_open_trigger_payload(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    environment: str = "PAPER",
    now_utc: datetime | None = None,
) -> Dict[str, Any]:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    normalized_environment = normalize_day_open_environment(environment)
    paper_session_authority_ref = (
        read_paper_session_authority_ref_v1(truth_root=root, day_utc=day_utc)
        if normalized_environment == "PAPER"
        else None
    )
    paper_session_authority_payload = dict(paper_session_authority_ref.payload) if paper_session_authority_ref is not None else {}
    active_session = (
        read_json_object_v1(resolve_active_session_path(truth_root=root))
        if normalized_environment != "PAPER"
        else {}
    )
    admission = (
        read_json_object_v1(resolve_target_day_admission_path(truth_root=root, target_day=day_utc))
        if normalized_environment != "PAPER"
        else {}
    )
    session_status = (
        read_json_object_v1(resolve_session_authority_status_path(truth_root=root))
        if normalized_environment != "PAPER"
        else {}
    )
    ledger = (
        read_json_object_v1(resolve_paper_session_ledger_path(truth_root=root, day_utc=day_utc))
        if normalized_environment != "PAPER"
        else {}
    )
    existing = _load_existing_trigger(truth_root=root, day_utc=day_utc)
    attempt = _load_attempt(truth_root=root, day_utc=day_utc)
    attempt_path = resolve_day_open_attempt_path(truth_root=root, day_utc=day_utc)
    window = build_day_open_window_v1(repo_root=repo_root, day_utc=day_utc, now_utc=now_utc)
    policy = build_day_open_policy_snapshot(
        environment=normalized_environment,
        window_status=window.window_status,
        trigger_payload=existing,
        attempt_payload=attempt,
        attempt_path=attempt_path,
    )
    control_state = ledger.get("control_state") if isinstance(ledger.get("control_state"), dict) else {}

    admission_status = str(admission.get("admission_status") or "").strip().upper()
    active_day = str(active_session.get("active_day") or "").strip()
    rollover_status = str(active_session.get("rollover_status") or "").strip().upper()
    ledger_authority_status = (
        str(paper_session_authority_payload.get("authority_status") or "").strip().upper()
        if normalized_environment == "PAPER"
        else str(
            control_state.get("authority_status")
            or ledger.get("authority_status")
            or ""
        ).strip().upper()
    )
    paper_open_allowed = (
        bool(paper_session_authority_payload.get("paper_open_allowed") is True)
        if normalized_environment == "PAPER"
        else ledger_authority_status == "GRANTED"
    )

    emitted_at_utc = str((existing or {}).get("emitted_at_utc") or "").strip()
    trigger_sequence = int((existing or {}).get("trigger_sequence") or 0) if existing else 0
    consumed = bool((existing or {}).get("consumed") is True)
    consumed_at_utc = str((existing or {}).get("consumed_at_utc") or "").strip()
    consumed_by = dict((existing or {}).get("consumed_by") or {}) if existing else {}
    existing_status = str((existing or {}).get("trigger_status") or "").strip().upper()
    trigger_history = _trigger_history(existing)

    trigger_status = "SUPPRESSED_AUTHORITY_NOT_GRANTED"
    trigger_reason_code = "OPEN_TRIGGER_AUTHORITY_NOT_GRANTED"
    trigger_kind = "NO_TRIGGER"
    if existing and existing_status == "EMITTED" and not consumed:
        trigger_status = "EMITTED"
        trigger_reason_code = str(existing.get("trigger_reason_code") or "OPEN_TRIGGER_ALREADY_EMITTED").strip()
        trigger_kind = str(existing.get("trigger_kind") or INITIAL_TRIGGER_KIND).strip() or INITIAL_TRIGGER_KIND
    elif normalized_environment != "PAPER" and bool(policy.get("successful_open_already_recorded") is True):
        trigger_status = "SUPPRESSED_TERMINAL_ATTEMPT"
        trigger_reason_code = str(policy.get("terminal_reason_code") or "OPEN_ALREADY_SUCCEEDED_FOR_DAY")
    elif normalized_environment != "PAPER" and bool(policy.get("late_open_consumed") is True):
        trigger_status = "SUPPRESSED_TERMINAL_ATTEMPT"
        trigger_reason_code = str(policy.get("terminal_reason_code") or "PAPER_LATE_OPEN_ALREADY_CONSUMED_FOR_DAY")
    elif normalized_environment != "PAPER" and _is_terminal_attempt(attempt) and not bool(policy.get("late_open_available") is True):
        trigger_status = "SUPPRESSED_TERMINAL_ATTEMPT"
        trigger_reason_code = str(policy.get("terminal_reason_code") or "OPEN_TRIGGER_TERMINAL_ATTEMPT_ALREADY_RECORDED")
    elif normalized_environment != "PAPER" and window.window_status == "PRE_OPEN":
        trigger_status = "SUPPRESSED_PRE_OPEN"
        trigger_reason_code = "OPEN_TRIGGER_PRE_OPEN"
    elif normalized_environment != "PAPER" and window.window_status == "POST_OPEN_WINDOW":
        trigger_status = "SUPPRESSED_OUTSIDE_OPEN_WINDOW"
        trigger_reason_code = "OPEN_TRIGGER_OPEN_WINDOW_EXPIRED"
    elif normalized_environment != "PAPER" and admission_status != "ADMIT":
        trigger_status = "SUPPRESSED_ADMISSION_NOT_ADMIT"
        trigger_reason_code = "OPEN_TRIGGER_ADMISSION_NOT_ADMIT"
    elif normalized_environment != "PAPER" and (active_day != day_utc or rollover_status != "ACTIVE_SESSION_CONFIRMED"):
        trigger_status = "SUPPRESSED_ACTIVE_SESSION_NOT_BOUND"
        trigger_reason_code = "OPEN_TRIGGER_ACTIVE_SESSION_NOT_BOUND"
    elif normalized_environment == "PAPER" and (ledger_authority_status != "GRANTED" or not paper_open_allowed):
        trigger_status = "SUPPRESSED_AUTHORITY_NOT_GRANTED"
        trigger_reason_code = "OPEN_TRIGGER_AUTHORITY_NOT_GRANTED"
    elif normalized_environment != "PAPER" and ledger_authority_status != "GRANTED":
        trigger_status = "SUPPRESSED_AUTHORITY_NOT_GRANTED"
        trigger_reason_code = "OPEN_TRIGGER_AUTHORITY_NOT_GRANTED"
    elif normalized_environment != "PAPER" and consumed and not bool(policy.get("late_open_available") is True):
        trigger_status = "SUPPRESSED_ALREADY_CONSUMED"
        trigger_reason_code = "OPEN_TRIGGER_ALREADY_CONSUMED"
        trigger_kind = str((existing or {}).get("trigger_kind") or INITIAL_TRIGGER_KIND).strip() or INITIAL_TRIGGER_KIND
    else:
        trigger_status = "EMITTED"
        if normalized_environment == "PAPER":
            trigger_history = _append_prior_trigger(trigger_history, existing)
            trigger_sequence = max(trigger_sequence, 0) + 1
            trigger_reason_code = (
                "OPEN_TRIGGER_EMITTED_PAPER_READY_REPEAT"
                if trigger_sequence > 1
                else "OPEN_TRIGGER_EMITTED_PAPER_READY"
            )
            trigger_kind = INITIAL_TRIGGER_KIND
        else:
            late_grant_reentry = existing_status in {
                "SUPPRESSED_AUTHORITY_NOT_GRANTED",
                "SUPPRESSED_ACTIVE_SESSION_NOT_BOUND",
            }
            late_open_trigger = bool(policy.get("late_open_available") is True) and (
                late_grant_reentry or bool(policy.get("initial_open_consumed") is True)
            )
            trigger_reason_code = "OPEN_TRIGGER_EMITTED_LATE_OPEN" if late_open_trigger else "OPEN_TRIGGER_EMITTED"
            trigger_kind = LATE_TRIGGER_KIND if late_open_trigger else _kind_for_window(window.window_status)
            trigger_sequence = max(trigger_sequence, 1 if trigger_kind == INITIAL_TRIGGER_KIND else 2)
        consumed = False
        consumed_at_utc = ""
        consumed_by = {}
        emitted_at_utc = now_utc_iso_v1()

    if trigger_kind == NO_TRIGGER_KIND and trigger_status == "EMITTED":
        trigger_kind = INITIAL_TRIGGER_KIND
    dedupe_key = (
        f"{day_utc}:{trigger_kind}:{trigger_sequence}"
        if normalized_environment == "PAPER"
        else f"{day_utc}:{trigger_kind}:{window.bod_time_et}"
    )

    return {
        "schema_id": "day_open_trigger",
        "schema_version": "v1",
        "day_utc": str(day_utc).strip(),
        "trigger_status": trigger_status,
        "emitted_at_utc": emitted_at_utc,
        "trigger_reason_code": trigger_reason_code,
        "trigger_kind": trigger_kind,
        "environment": normalized_environment,
        "open_policy": policy,
        "source_authority_refs": {
            "active_session_v1": str(resolve_active_session_path(truth_root=root)),
            "target_day_admission_v1": str(resolve_target_day_admission_path(truth_root=root, target_day=day_utc)),
            "session_authority_status_v1": str(resolve_session_authority_status_path(truth_root=root)),
            "paper_session_ledger_v1": str(resolve_paper_session_ledger_path(truth_root=root, day_utc=day_utc)),
            "paper_session_authority_v1": str(resolve_paper_session_authority_path(truth_root=root, day_utc=day_utc)),
        },
        "open_window_status": window.window_status,
        "open_window": {
            "timezone": window.timezone,
            "bod_time_et": window.bod_time_et,
            "cutoff_time_et": window.cutoff_time_et,
            "bod_time_utc": window.bod_time_utc,
            "cutoff_time_utc": window.cutoff_time_utc,
            "source_timer_path": window.source_timer_path,
            "cutoff_timer_path": window.cutoff_timer_path,
        },
        "dedupe_key": dedupe_key,
        "trigger_sequence": trigger_sequence,
        "trigger_history": trigger_history,
        "consumed": consumed,
        "consumed_at_utc": consumed_at_utc,
        "consumed_by": consumed_by,
        "expires_at_utc": window.cutoff_time_utc,
        "producer": producer_block_v1(module="constellation_2/common/day_open_trigger_v1.py"),
    }


def write_day_open_trigger_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_day_open_trigger_path(
            truth_root=root,
            day_utc=str(payload.get("day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("emitted_at_utc", "consumed_at_utc"),
    )


def consume_day_open_trigger_v1(
    *,
    truth_root: Path,
    day_utc: str,
    actor_name: str,
    actor_path: str,
    environment: str = "PAPER",
) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    payload = build_day_open_trigger_payload(
        repo_root=Path(__file__).resolve().parents[2],
        truth_root=root,
        day_utc=day_utc,
        environment=environment,
    )
    payload["consumed"] = True
    payload["consumed_at_utc"] = now_utc_iso_v1()
    payload["consumed_by"] = {
        "actor_name": str(actor_name).strip(),
        "actor_path": str(actor_path).strip(),
    }
    return write_day_open_trigger_v1(truth_root=root, payload=payload)
