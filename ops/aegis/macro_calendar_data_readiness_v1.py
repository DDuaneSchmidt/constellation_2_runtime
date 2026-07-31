from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.hypothesis_proposal_promotion_v1 import SAFETY, SAFETY_STATEMENT
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1

FAMILY = "aegis_macro_calendar_data_readiness_v1"
FILENAME = "macro_calendar_data_readiness.v1.json"
SOURCE_FAMILY = "aegis_macro_calendar_source_v1"
SOURCE_FILENAME = "macro_calendar_source.v1.json"

REQUIRED_FIELDS = [
    "event_name",
    "event_type",
    "release_datetime",
    "actual",
    "consensus",
    "prior",
    "importance",
    "affected_assets",
    "source",
    "source_timestamp",
    "timezone",
    "data_quality_status",
]

SUPPORTED_EVENT_TYPES = [
    "CPI",
    "FOMC",
    "NFP",
    "GDP",
    "Retail Sales",
    "ISM",
    "PPI",
    "Jobless Claims",
    "Other",
]

BUTTONS = ["Connect Source", "Upload Dataset", "Mark Not Available", "Defer"]

SAFETY_FLAGS = {
    **SAFETY,
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_shadow_validation_pass_forced": True,
    "no_macro_event_fabrication": True,
    "safety_gates_changed": False,
}


def macro_calendar_data_readiness_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root) / "reports" / FAMILY / str(day_utc) / FILENAME


def macro_calendar_source_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root) / "reports" / SOURCE_FAMILY / str(day_utc) / SOURCE_FILENAME


def build_macro_calendar_data_readiness_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    source_path = macro_calendar_source_path_v1(truth_root=truth_root, day_utc=day_utc)
    computed_at = f"{day_utc}T00:00:00Z"
    source_payload = read_json_v1(source_path) if source_path.exists() else {}
    events = _event_rows(source_payload)
    row_validation = [_validate_event(row, index) for index, row in enumerate(events)]
    missing_fields = sorted({field for row in row_validation for field in row["missing_fields"]}, key=REQUIRED_FIELDS.index)
    invalid_count = sum(1 for row in row_validation if row["status"] != "VALID")
    valid_count = sum(1 for row in row_validation if row["status"] == "VALID")

    if not source_path.exists():
        status = "NEEDS_SOURCE"
        message = "Macro Calendar needs a governed macro event calendar source."
        next_step = "Connect Source, Upload Dataset, Mark Not Available, or Defer."
        david_action = True
        buttons = list(BUTTONS)
    elif missing_fields or invalid_count or not events:
        status = "SOURCE_INCOMPLETE"
        message = "Macro Calendar source exists but required fields are missing or invalid."
        next_step = "Complete the governed macro calendar source, then rerun data readiness."
        david_action = True
        buttons = list(BUTTONS)
        if not events and "event_name" not in missing_fields:
            missing_fields = list(REQUIRED_FIELDS)
    else:
        status = "READY"
        message = "Macro Calendar governed source is ready for automatic shadow validation."
        next_step = "rerun shadow validation using governed macro calendar source"
        david_action = False
        buttons = []

    payload: dict[str, Any] = {
        "schema_id": "aegis_macro_calendar_data_readiness_v1",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": str(day_utc),
        "computed_at_utc": computed_at,
        "status": status,
        "macro_calendar_ready": status == "READY",
        "david_action_required": david_action,
        "buttons": buttons,
        "missing_fields": missing_fields,
        "required_fields": list(REQUIRED_FIELDS),
        "supported_event_types": list(SUPPORTED_EVENT_TYPES),
        "event_count": len(events),
        "valid_event_count": valid_count,
        "invalid_event_count": invalid_count,
        "source_status": "FOUND" if source_path.exists() else "MISSING",
        "source_artifact_path": str(source_path),
        "source_artifact_hash": _sha(source_path),
        "row_validation": row_validation,
        "next_step": next_step,
        "message": message,
        "downstream_effects": {
            "workflow_state": "READY_FOR_SHADOW_TRIAL" if status == "READY" else "NEEDS_DATA",
            "operator_action_queue": "PROVIDE_DATA_SOURCE" if david_action else "NONE",
            "shadow_validation": "eligible_to_rerun" if status == "READY" else "blocked_by_macro_calendar_data_readiness",
            "shadow_validation_pass_forced": False,
        },
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY_FLAGS),
        **SAFETY_FLAGS,
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def write_macro_calendar_data_readiness_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_macro_calendar_data_readiness_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(macro_calendar_data_readiness_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _event_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    for key in ("events", "macro_events", "rows", "data"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _validate_event(row: Mapping[str, Any], index: int) -> dict[str, Any]:
    missing: list[str] = []
    for field in REQUIRED_FIELDS:
        if not _has_value(row.get(field)):
            missing.append(field)
    event_type = str(row.get("event_type") or "")
    if event_type and event_type not in SUPPORTED_EVENT_TYPES and "event_type" not in missing:
        missing.append("event_type")
    if not _parseable_timestamp(row.get("release_datetime")) and "release_datetime" not in missing:
        missing.append("release_datetime")
    if not _parseable_timestamp(row.get("source_timestamp")) and "source_timestamp" not in missing:
        missing.append("source_timestamp")
    assets = row.get("affected_assets")
    if isinstance(assets, list):
        if not [item for item in assets if str(item).strip()]:
            missing.append("affected_assets")
    elif isinstance(assets, str):
        if not [item for item in assets.replace(";", ",").split(",") if item.strip()] and "affected_assets" not in missing:
            missing.append("affected_assets")
    return {
        "row_index": index,
        "status": "VALID" if not missing else "INVALID",
        "missing_fields": sorted(set(missing), key=REQUIRED_FIELDS.index),
        "event_name": str(row.get("event_name") or ""),
        "event_type": event_type,
    }


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return bool(value)
    return True


def _parseable_timestamp(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _sha(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else ""
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()
