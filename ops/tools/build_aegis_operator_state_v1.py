#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root

STATE_WAITING = "WAITING_FOR_INTENT"
STATE_SELECTED = "SELECTED_INTENT_REVIEW_REQUIRED"
STATE_ATTENTION = "ATTENTION_REQUIRED"
STATE_STALE = "STALE"
STATE_ERROR = "ERROR"

ALERT_STATES = {STATE_SELECTED, STATE_ATTENTION, STATE_STALE, STATE_ERROR}


def _now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _now_iso() -> str:
    return _now_utc().isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ValueError(f"MISSING_FILE:{path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"MALFORMED_JSON:{path}:{type(exc).__name__}") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _read_jsonl_objects(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        raise ValueError(f"MISSING_FILE:{path}")
    rows: list[dict[str, Any]] = []
    for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception as exc:
            raise ValueError(f"MALFORMED_JSONL:{path}:line={idx}:{type(exc).__name__}") from exc
        if not isinstance(obj, dict):
            raise ValueError(f"JSONL_TOP_LEVEL_NOT_OBJECT:{path}:line={idx}")
        rows.append(obj)
    if not rows:
        raise ValueError(f"EMPTY_JSONL:{path}")
    return rows


def _parse_utc(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def aegis_operator_state_path(*, truth_root: Path) -> Path:
    return Path(truth_root).resolve() / "control_plane" / "aegis_operator_state.v1.json"


def latest_scan_pointer_path(*, truth_root: Path) -> Path:
    return Path(truth_root).resolve() / "pointers" / "latest_scan_cycle_pointer.v1.json"


def selected_intent_pointer_path(*, truth_root: Path) -> Path:
    return Path(truth_root).resolve() / "pointers" / "selected_intent_pointer.v1.json"


def scan_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "scan_ledger_v1" / f"{day_utc}.scan_ledger.v1.jsonl"


def _cycle_artifact_path(latest_pointer: dict[str, Any], filename: str) -> Path:
    root = Path(str(latest_pointer.get("artifact_root") or "")).expanduser()
    if not root.is_absolute():
        root = root.resolve()
    return root / filename


def _selected_intent_id(*, selected_pointer: dict[str, Any], operator_status: dict[str, Any], arbitration: dict[str, Any]) -> str:
    for value in (
        operator_status.get("selected_intent_id"),
        selected_pointer.get("selected_intent_id"),
        (selected_pointer.get("selected_intent") if isinstance(selected_pointer.get("selected_intent"), dict) else {}).get("intent_id"),
        (arbitration.get("selected_intent") if isinstance(arbitration.get("selected_intent"), dict) else {}).get("intent_id"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return ""


def build_aegis_operator_alert_state_v1(
    *,
    state: str,
    latest_cycle_id: str,
    selected_intent_id: str,
    previous_alert_key: str = "",
) -> dict[str, Any]:
    alert_key = f"{state}|{latest_cycle_id}|{selected_intent_id}"
    alertable = state in ALERT_STATES
    duplicate = alertable and bool(previous_alert_key) and previous_alert_key == alert_key
    return {
        "alertable": alertable,
        "should_alert": alertable and not duplicate,
        "suppressed_duplicate": duplicate,
        "alert_key": alert_key if alertable else "",
        "browser_notification_supported": True,
        "desktop_notification_enabled": False,
        "webhook_enabled": False,
        "webhook_configured": False,
    }


def _recommended_action(state: str, canonical_blocker: str) -> str:
    if state == STATE_ERROR:
        return "Repair missing or malformed Aegis operator source artifacts, then rebuild derived operator state."
    if state == STATE_SELECTED:
        return "Review the selected intent before any separate execution workflow."
    if state == STATE_ATTENTION:
        if canonical_blocker:
            return f"Resolve attention condition: {canonical_blocker}."
        return "Review readiness counts, pointer alignment, and execution safety flags."
    if state == STATE_STALE:
        return "Run the scanner loop and rebuild Aegis operator state."
    return "Continue monitoring; no selected intent is awaiting review."


def _error_payload(
    *,
    truth_root: Path,
    errors: list[str],
    max_age_seconds: int,
    expected_ready_baseline: int,
    updated_at_utc: str,
) -> dict[str, Any]:
    latest_path = latest_scan_pointer_path(truth_root=truth_root)
    selected_path = selected_intent_pointer_path(truth_root=truth_root)
    payload = {
        "schema_id": "aegis_operator_state",
        "schema_version": "v1",
        "state": STATE_ERROR,
        "state_reason": "; ".join(errors) if errors else "required artifact missing or malformed",
        "recommended_operator_action": _recommended_action(STATE_ERROR, ""),
        "latest_cycle_id": "",
        "latest_cycle_age_seconds": None,
        "readiness_counts": {"ready": 0, "blocked": 0, "unknown": 0, "disabled": 0},
        "sleeves_by_status": {},
        "arbitration_status": "UNKNOWN",
        "selected_intent_id": "",
        "canonical_blocker": "Aegis operator source artifact unavailable",
        "top_blockers": [],
        "submit_enabled": None,
        "execution_path_touched": None,
        "latest_scan_pointer_path": str(latest_path),
        "selected_intent_pointer_path": str(selected_path),
        "operator_status_path": "",
        "readiness_summary_path": "",
        "arbitration_result_path": "",
        "ledger_path": "",
        "max_age_seconds": max_age_seconds,
        "expected_ready_baseline": expected_ready_baseline,
        "updated_at_utc": updated_at_utc,
    }
    payload["alert_state"] = build_aegis_operator_alert_state_v1(
        state=payload["state"],
        latest_cycle_id=payload["latest_cycle_id"],
        selected_intent_id=payload["selected_intent_id"],
    )
    return payload


def build_aegis_operator_state_v1(
    *,
    truth_root: Path,
    max_age_seconds: int = 300,
    expected_ready_baseline: int = 5,
    now_utc: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    now = (now_utc or _now_utc()).astimezone(UTC).replace(microsecond=0)
    updated_at_utc = now.isoformat().replace("+00:00", "Z")

    latest_path = latest_scan_pointer_path(truth_root=root)
    selected_path = selected_intent_pointer_path(truth_root=root)
    errors: list[str] = []

    latest_pointer: dict[str, Any] = {}
    selected_pointer: dict[str, Any] = {}
    try:
        latest_pointer = _read_json_object(latest_path)
    except ValueError as exc:
        errors.append(str(exc))
    try:
        selected_pointer = _read_json_object(selected_path)
    except ValueError as exc:
        errors.append(str(exc))

    latest_cycle_id = str(latest_pointer.get("cycle_id") or "").strip()
    day_utc = str(latest_pointer.get("day_utc") or "").strip()
    operator_path_raw = str(latest_pointer.get("operator_status_path") or "").strip()
    operator_path = Path(operator_path_raw).expanduser().resolve() if operator_path_raw else _cycle_artifact_path(latest_pointer, "operator_status.v1.json")
    readiness_path = _cycle_artifact_path(latest_pointer, "operator_readiness_summary.v1.json")
    arbitration_path = _cycle_artifact_path(latest_pointer, "arbitration_result.v1.json")
    ledger_path = scan_ledger_path(truth_root=root, day_utc=day_utc) if day_utc else root / "reports" / "scan_ledger_v1" / "UNKNOWN.scan_ledger.v1.jsonl"

    operator_status: dict[str, Any] = {}
    readiness_summary: dict[str, Any] = {}
    arbitration: dict[str, Any] = {}
    ledger_rows: list[dict[str, Any]] = []

    if not errors:
        try:
            operator_status = _read_json_object(operator_path)
        except ValueError as exc:
            errors.append(str(exc))
        if operator_status:
            readiness_path_text = str(operator_status.get("operator_readiness_summary_path") or "").strip()
            if readiness_path_text:
                readiness_path = Path(readiness_path_text).expanduser().resolve()
        try:
            readiness_summary = _read_json_object(readiness_path)
        except ValueError as exc:
            errors.append(str(exc))
        try:
            arbitration = _read_json_object(arbitration_path)
        except ValueError as exc:
            errors.append(str(exc))
        try:
            ledger_rows = _read_jsonl_objects(ledger_path)
        except ValueError as exc:
            errors.append(str(exc))

    if errors:
        payload = _error_payload(
            truth_root=root,
            errors=errors,
            max_age_seconds=max_age_seconds,
            expected_ready_baseline=expected_ready_baseline,
            updated_at_utc=updated_at_utc,
        )
        payload["latest_cycle_id"] = latest_cycle_id
        payload["operator_status_path"] = str(operator_path) if str(operator_path) else ""
        payload["readiness_summary_path"] = str(readiness_path) if str(readiness_path) else ""
        payload["arbitration_result_path"] = str(arbitration_path) if str(arbitration_path) else ""
        payload["ledger_path"] = str(ledger_path) if str(ledger_path) else ""
        payload["alert_state"] = build_aegis_operator_alert_state_v1(
            state=payload["state"],
            latest_cycle_id=payload["latest_cycle_id"],
            selected_intent_id=payload["selected_intent_id"],
        )
        if write:
            _write_json(aegis_operator_state_path(truth_root=root), payload)
        return payload

    completed = _parse_utc(operator_status.get("completed_at_utc")) or _parse_utc(latest_pointer.get("updated_at_utc"))
    latest_cycle_age_seconds = int((now - completed).total_seconds()) if completed is not None else None

    readiness_counts = {
        "ready": _coerce_int(readiness_summary.get("ready_count")),
        "blocked": _coerce_int(readiness_summary.get("blocked_count")),
        "unknown": _coerce_int(readiness_summary.get("unknown_count")),
        "disabled": _coerce_int(readiness_summary.get("disabled_count")),
    }
    sleeves_by_status = readiness_summary.get("sleeves_by_status") if isinstance(readiness_summary.get("sleeves_by_status"), dict) else {}
    top_blockers = readiness_summary.get("top_blockers") if isinstance(readiness_summary.get("top_blockers"), list) else []
    selected_intent_id = _selected_intent_id(selected_pointer=selected_pointer, operator_status=operator_status, arbitration=arbitration)
    selected_intent_present = bool(selected_intent_id) or bool(operator_status.get("selected_intent_present"))
    submit_enabled = bool(operator_status.get("submit_enabled")) if operator_status.get("submit_enabled") is not None else None
    execution_path_touched = bool(operator_status.get("execution_path_touched")) if operator_status.get("execution_path_touched") is not None else None
    selected_cycle_id = str(selected_pointer.get("cycle_id") or "").strip()
    pointer_mismatch = bool(selected_cycle_id and latest_cycle_id and selected_cycle_id != latest_cycle_id)
    ledger_latest_cycle = str((ledger_rows[-1] if ledger_rows else {}).get("cycle_id") or "").strip()
    ledger_stopped = bool(ledger_latest_cycle and latest_cycle_id and ledger_latest_cycle != latest_cycle_id)

    canonical_blocker = str(
        operator_status.get("canonical_blocker")
        or latest_pointer.get("canonical_blocker")
        or arbitration.get("canonical_blocker")
        or ""
    ).strip()

    attention_reasons: list[str] = []
    if readiness_counts["blocked"] > 0:
        attention_reasons.append("blocked_count > 0")
    if readiness_counts["unknown"] > 0:
        attention_reasons.append("unknown_count > 0")
    if readiness_counts["ready"] < int(expected_ready_baseline):
        attention_reasons.append("ready_count below expected baseline")
    if pointer_mismatch:
        attention_reasons.append("selected/latest pointer mismatch")
    if submit_enabled is not False:
        attention_reasons.append("submit_enabled != false")
    if execution_path_touched is not False:
        attention_reasons.append("execution_path_touched != false")

    stale_reasons: list[str] = []
    if latest_cycle_age_seconds is None:
        stale_reasons.append("latest cycle timestamp unavailable")
    elif latest_cycle_age_seconds > int(max_age_seconds):
        stale_reasons.append("latest cycle age exceeds max_age_seconds")
    if ledger_stopped:
        stale_reasons.append("scan ledger latest row does not match latest pointer")

    if selected_intent_present:
        state = STATE_SELECTED
        state_reason = "selected intent is present"
    elif attention_reasons:
        state = STATE_ATTENTION
        state_reason = "; ".join(attention_reasons)
    elif stale_reasons:
        state = STATE_STALE
        state_reason = "; ".join(stale_reasons)
    else:
        state = STATE_WAITING
        state_reason = "healthy scanner state with no selected intent"

    if not canonical_blocker and top_blockers:
        first = top_blockers[0] if isinstance(top_blockers[0], dict) else {}
        canonical_blocker = str(first.get("blocker") or "").strip()

    payload = {
        "schema_id": "aegis_operator_state",
        "schema_version": "v1",
        "state": state,
        "state_reason": state_reason,
        "recommended_operator_action": _recommended_action(state, canonical_blocker),
        "latest_cycle_id": latest_cycle_id,
        "latest_cycle_age_seconds": latest_cycle_age_seconds,
        "readiness_counts": readiness_counts,
        "sleeves_by_status": sleeves_by_status,
        "arbitration_status": str(arbitration.get("status") or "UNKNOWN"),
        "selected_intent_id": selected_intent_id,
        "canonical_blocker": canonical_blocker,
        "top_blockers": top_blockers[:5],
        "submit_enabled": submit_enabled,
        "execution_path_touched": execution_path_touched,
        "latest_scan_pointer_path": str(latest_path),
        "selected_intent_pointer_path": str(selected_path),
        "operator_status_path": str(operator_path),
        "readiness_summary_path": str(readiness_path),
        "arbitration_result_path": str(arbitration_path),
        "ledger_path": str(ledger_path),
        "max_age_seconds": int(max_age_seconds),
        "expected_ready_baseline": int(expected_ready_baseline),
        "updated_at_utc": updated_at_utc,
    }
    payload["alert_state"] = build_aegis_operator_alert_state_v1(
        state=state,
        latest_cycle_id=latest_cycle_id,
        selected_intent_id=selected_intent_id,
    )
    if write:
        _write_json(aegis_operator_state_path(truth_root=root), payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_operator_state_v1")
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--max_age_seconds", type=int, default=300)
    parser.add_argument("--expected_ready_baseline", type=int, default=5)
    parser.add_argument("--no_write", action="store_true")
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root).expanduser().resolve() if str(args.truth_root or "").strip() else resolve_canonical_truth_root()
    payload = build_aegis_operator_state_v1(
        truth_root=truth_root,
        max_age_seconds=int(args.max_age_seconds),
        expected_ready_baseline=int(args.expected_ready_baseline),
        write=not bool(args.no_write),
    )
    print(
        json.dumps(
            {
                "state": payload["state"],
                "recommended_operator_action": payload["recommended_operator_action"],
                "latest_cycle_id": payload["latest_cycle_id"],
                "path": str(aegis_operator_state_path(truth_root=truth_root)),
            },
            sort_keys=True,
        )
    )
    return 0 if payload["state"] in {STATE_WAITING, STATE_SELECTED, STATE_ATTENTION, STATE_STALE} else 2


if __name__ == "__main__":
    raise SystemExit(main())
