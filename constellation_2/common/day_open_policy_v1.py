from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List


INITIAL_TRIGGER_KIND = "INITIAL_BOD_TRIGGER"
LATE_TRIGGER_KIND = "LATE_OPEN_TRIGGER"
NO_TRIGGER_KIND = "NO_TRIGGER"
PAPER_POLICY_MODE = "PAPER_READY_WHEN_GRANTED_UNBOUNDED_SAME_DAY"
STRICT_POLICY_MODE = "STRICT_SINGLE_OPEN_WINDOW"
_TERMINAL_CLASSIFICATIONS = {"OPEN_SUCCEEDED", "OPEN_FAILED", "OPEN_MISSED"}
_CONSUMED_CLASSIFICATIONS = _TERMINAL_CLASSIFICATIONS | {"OPEN_ATTEMPTED"}


def normalize_day_open_environment(environment: str) -> str:
    value = str(environment or "").strip().upper()
    return value if value else "PAPER"


def policy_mode_for_environment(environment: str) -> str:
    return PAPER_POLICY_MODE if normalize_day_open_environment(environment) == "PAPER" else STRICT_POLICY_MODE


def same_day_open_cap_enforced_for_environment(environment: str) -> bool:
    return policy_mode_for_environment(environment) == STRICT_POLICY_MODE


def time_window_enforced_for_environment(environment: str) -> bool:
    return policy_mode_for_environment(environment) == STRICT_POLICY_MODE


def _entry_from_attempt(payload: Dict[str, Any], *, attempt_path: Path) -> Dict[str, Any]:
    return {
        "attempt_sequence": int(payload.get("attempt_sequence") or 0),
        "trigger_kind": str(payload.get("trigger_kind") or NO_TRIGGER_KIND).strip().upper() or NO_TRIGGER_KIND,
        "final_classification": str(payload.get("final_classification") or "").strip().upper(),
        "result_code": str(payload.get("result_code") or "").strip(),
        "attempt_ref": str(attempt_path.resolve()),
    }


def _history_entries(payload: Dict[str, Any] | None, *, attempt_path: Path) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    entries: List[Dict[str, Any]] = []
    history = payload.get("attempt_history")
    if isinstance(history, list):
        for row in history:
            if not isinstance(row, dict):
                continue
            entries.append(
                {
                    "attempt_sequence": int(row.get("attempt_sequence") or 0),
                    "trigger_kind": str(row.get("trigger_kind") or NO_TRIGGER_KIND).strip().upper() or NO_TRIGGER_KIND,
                    "final_classification": str(row.get("final_classification") or "").strip().upper(),
                    "result_code": str(row.get("result_code") or "").strip(),
                    "attempt_ref": str(row.get("attempt_ref") or str(attempt_path.resolve())).strip(),
                }
            )
    current_classification = str(payload.get("final_classification") or "").strip().upper()
    if current_classification:
        entries.append(_entry_from_attempt(payload, attempt_path=attempt_path))
    entries.sort(key=lambda item: (int(item.get("attempt_sequence") or 0), str(item.get("trigger_kind") or "")))
    return entries


def _first_entry(entries: List[Dict[str, Any]], *, trigger_kind: str, classifications: set[str]) -> Dict[str, Any] | None:
    for entry in entries:
        if str(entry.get("trigger_kind") or "").strip().upper() != trigger_kind:
            continue
        if str(entry.get("final_classification") or "").strip().upper() in classifications:
            return entry
    return None


def build_day_open_policy_snapshot(
    *,
    environment: str,
    window_status: str,
    trigger_payload: Dict[str, Any] | None,
    attempt_payload: Dict[str, Any] | None,
    attempt_path: Path,
) -> Dict[str, Any]:
    env = normalize_day_open_environment(environment)
    policy_mode = policy_mode_for_environment(env)
    entries = _history_entries(attempt_payload, attempt_path=attempt_path)
    successful_entry = _first_entry(entries, trigger_kind=INITIAL_TRIGGER_KIND, classifications={"OPEN_SUCCEEDED"})
    if successful_entry is None:
        successful_entry = _first_entry(entries, trigger_kind=LATE_TRIGGER_KIND, classifications={"OPEN_SUCCEEDED"})
    initial_terminal_entry = _first_entry(entries, trigger_kind=INITIAL_TRIGGER_KIND, classifications={"OPEN_FAILED", "OPEN_MISSED"})
    late_consumed_entry = _first_entry(entries, trigger_kind=LATE_TRIGGER_KIND, classifications=_CONSUMED_CLASSIFICATIONS)

    prior_trigger_status = str((trigger_payload or {}).get("trigger_status") or "").strip().upper()
    prior_trigger_kind = str((trigger_payload or {}).get("trigger_kind") or NO_TRIGGER_KIND).strip().upper() or NO_TRIGGER_KIND
    prior_trigger_sequence = int((trigger_payload or {}).get("trigger_sequence") or 0)
    late_grant_reentry_eligible = prior_trigger_status in {
        "SUPPRESSED_AUTHORITY_NOT_GRANTED",
        "SUPPRESSED_ACTIVE_SESSION_NOT_BOUND",
    }

    successful_open_already_recorded = successful_entry is not None
    prior_success_ref = str((successful_entry or {}).get("attempt_ref") or "").strip()
    same_day_cap_enforced = same_day_open_cap_enforced_for_environment(env)
    time_window_enforced = time_window_enforced_for_environment(env)
    late_open_consumed = late_consumed_entry is not None if same_day_cap_enforced else False

    late_open_available = False
    if same_day_cap_enforced and not successful_open_already_recorded and not late_open_consumed:
        if window_status == "OPEN_WINDOW":
            late_open_available = initial_terminal_entry is not None or late_grant_reentry_eligible

    open_terminal = False
    terminal_reason_code = ""
    if same_day_cap_enforced:
        if successful_open_already_recorded:
            open_terminal = True
            terminal_reason_code = "OPEN_ALREADY_SUCCEEDED_FOR_DAY"
        elif late_open_consumed:
            open_terminal = True
            terminal_reason_code = "PAPER_LATE_OPEN_ALREADY_CONSUMED_FOR_DAY"
        elif initial_terminal_entry is not None:
            open_terminal = True
            terminal_reason_code = "STRICT_SINGLE_OPEN_WINDOW_ALREADY_CONSUMED"
        elif time_window_enforced and window_status == "POST_OPEN_WINDOW" and not late_open_available:
            open_terminal = True
            terminal_reason_code = "OPEN_TRIGGER_OPEN_WINDOW_EXPIRED"

    policy_status = "INITIAL_OPEN_ONLY"
    if policy_mode == PAPER_POLICY_MODE:
        policy_status = (
            "PAPER_REPEAT_OPEN_ALLOWED_WHEN_GRANTED"
            if successful_open_already_recorded
            else "PAPER_OPEN_AVAILABLE_WHEN_GRANTED"
        )
    elif successful_open_already_recorded:
        policy_status = "OPEN_ALREADY_SUCCEEDED_FOR_DAY"
    elif late_open_available:
        policy_status = "LATE_OPEN_AVAILABLE"
    elif late_open_consumed:
        policy_status = "LATE_OPEN_CONSUMED"
    elif open_terminal and terminal_reason_code == "OPEN_TRIGGER_OPEN_WINDOW_EXPIRED":
        policy_status = "STRICT_OPEN_TERMINAL"

    return {
        "policy_mode": policy_mode,
        "environment": env,
        "max_successful_opens_per_day": 0 if policy_mode == PAPER_POLICY_MODE else 1,
        "max_late_open_attempts_per_day": 0,
        "same_day_open_cap_enforced": same_day_cap_enforced,
        "time_window_enforced": time_window_enforced,
        "successful_open_already_recorded": successful_open_already_recorded,
        "prior_success_ref": prior_success_ref,
        "initial_open_consumed": initial_terminal_entry is not None
        or _first_entry(entries, trigger_kind=INITIAL_TRIGGER_KIND, classifications={"OPEN_ATTEMPTED", "OPEN_SUCCEEDED"}) is not None
        or prior_trigger_sequence > 0,
        "late_open_available": late_open_available,
        "late_open_consumed": late_open_consumed,
        "late_open_consumed_ref": str((late_consumed_entry or {}).get("attempt_ref") or "").strip(),
        "open_terminal": open_terminal,
        "terminal_reason_code": terminal_reason_code,
        "policy_status": policy_status,
        "attempt_count": len(entries),
        "latest_attempt_kind": str((entries[-1] or {}).get("trigger_kind") or NO_TRIGGER_KIND).strip() if entries else NO_TRIGGER_KIND,
        "latest_attempt_result": str((entries[-1] or {}).get("final_classification") or "").strip() if entries else "",
        "prior_trigger_kind": prior_trigger_kind,
        "prior_trigger_status": prior_trigger_status,
    }
