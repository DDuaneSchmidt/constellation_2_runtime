from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"INVALID_JSON_OBJECT:{path}")
    return payload


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _latest_submission_record(submissions_day_dir: Path) -> Path | None:
    if not submissions_day_dir.exists() or not submissions_day_dir.is_dir():
        return None
    candidates = [
        path.resolve()
        for path in submissions_day_dir.glob("*/broker_submission_record.v2.json")
        if path.exists() and path.is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def classify_paper_submit_mode_status_v1(*, execution_root: Path, day_utc: str) -> dict[str, Any]:
    execution_root = Path(execution_root).resolve()
    submissions_day_dir = (execution_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    broker_record_path = _latest_submission_record(submissions_day_dir)
    if broker_record_path is None:
        return {
            "submit_mode_status": "NO_SUBMIT_ATTEMPT",
            "dry_run_policy": "UNKNOWN",
            "broker_transmit_enabled": None,
            "broker_order_transmitted": False,
            "missing_broker_ids_blocker": False,
            "missing_broker_ids_diagnostic": False,
            "broker_submission_record_path": "",
            "broker_submit_attempt_path": "",
            "submission_id": "",
            "reason_codes": [],
            "diagnostic_codes": [],
        }

    broker_record = _read_json(broker_record_path)
    submit_attempt_path = (broker_record_path.parent / "broker_submit_attempt_v1.json").resolve()
    submit_attempt: dict[str, Any] = {}
    if submit_attempt_path.exists() and submit_attempt_path.is_file():
        try:
            submit_attempt = _read_json(submit_attempt_path)
        except Exception:
            submit_attempt = {}

    broker_ids = broker_record.get("broker_ids") if isinstance(broker_record.get("broker_ids"), dict) else {}
    order_id = _coerce_positive_int(broker_ids.get("order_id"))
    perm_id = _coerce_positive_int(broker_ids.get("perm_id"))
    error = broker_record.get("error") if isinstance(broker_record.get("error"), dict) else {}
    error_code = str(error.get("code") or "").strip().upper()
    dry_run = bool(submit_attempt.get("dry_run") is True or error_code == "DRY_RUN_NO_BROKER_ID")
    has_broker_ids = bool(order_id is not None or perm_id is not None)
    diagnostic_codes: list[str] = []
    reason_codes: list[str] = []

    if order_id is None:
        diagnostic_codes.append("BROKER_ORDER_ID_MISSING")
    if perm_id is None:
        diagnostic_codes.append("BROKER_PERM_ID_MISSING")

    if dry_run:
        return {
            "submit_mode_status": "DRY_RUN_COMPLETE",
            "dry_run_policy": "YES",
            "broker_transmit_enabled": False,
            "broker_order_transmitted": False,
            "missing_broker_ids_blocker": False,
            "missing_broker_ids_diagnostic": bool(diagnostic_codes),
            "broker_submission_record_path": str(broker_record_path),
            "broker_submit_attempt_path": str(submit_attempt_path) if submit_attempt_path.exists() else "",
            "submission_id": str(broker_record.get("submission_id") or broker_record_path.parent.name).strip(),
            "reason_codes": ["DRY_RUN_SUBMIT_ATTEMPT"],
            "diagnostic_codes": diagnostic_codes,
        }

    if has_broker_ids:
        status = "ACKNOWLEDGED" if perm_id is not None else "SUBMITTED"
        return {
            "submit_mode_status": status,
            "dry_run_policy": "NO",
            "broker_transmit_enabled": True,
            "broker_order_transmitted": True,
            "missing_broker_ids_blocker": False,
            "missing_broker_ids_diagnostic": False,
            "broker_submission_record_path": str(broker_record_path),
            "broker_submit_attempt_path": str(submit_attempt_path) if submit_attempt_path.exists() else "",
            "submission_id": str(broker_record.get("submission_id") or broker_record_path.parent.name).strip(),
            "reason_codes": [],
            "diagnostic_codes": [],
        }

    reason_codes.extend(diagnostic_codes)
    return {
        "submit_mode_status": "DEGRADED",
        "dry_run_policy": "NO",
        "broker_transmit_enabled": True,
        "broker_order_transmitted": False,
        "missing_broker_ids_blocker": True,
        "missing_broker_ids_diagnostic": False,
        "broker_submission_record_path": str(broker_record_path),
        "broker_submit_attempt_path": str(submit_attempt_path) if submit_attempt_path.exists() else "",
        "submission_id": str(broker_record.get("submission_id") or broker_record_path.parent.name).strip(),
        "reason_codes": reason_codes,
        "diagnostic_codes": [],
    }
