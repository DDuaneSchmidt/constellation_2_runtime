from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1


REPORT_FAMILY = "aegis_breadth_drop_validation_v1"
MANUAL_DROP_ROOT = Path("/home/node/constellation_runtime_data/manual_drops/market_context")
REPAIR_COMMAND = "npm run aegis:repair-context-readiness"


def expected_breadth_drop_path_v1(*, day_utc: str) -> Path:
    return MANUAL_DROP_ROOT / day_utc / "breadth.csv"


def breadth_drop_validation_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "breadth_drop_validation.v1.json"


def write_breadth_drop_validation_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    path = breadth_drop_validation_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return {"json": str(path)}


def build_breadth_drop_validation_v1(*, truth_root: Path, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    generated_at = generated_at_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    path = expected_breadth_drop_path_v1(day_utc=day_utc)
    file_found = path.exists()
    schema_valid = False
    certification_status = "BLOCKED"
    failure_reason = ""
    parsed_row: dict[str, Any] = {}
    checked_columns = ["day_utc", "advance_decline_delta", "breadth_down_pct", "source", "timestamp_utc"]

    if not file_found:
        failure_reason = f"BREADTH_DROP_MISSING: expected file at {path}"
    else:
        try:
            rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
        except Exception as exc:
            rows = []
            failure_reason = f"BREADTH_DROP_INVALID_CSV: {exc}"
        if not failure_reason:
            header = rows[0].keys() if rows else []
            missing_columns = [column for column in checked_columns if column not in header]
            if missing_columns:
                failure_reason = f"BREADTH_DROP_SCHEMA_INVALID: missing columns {', '.join(missing_columns)}"
            elif len(rows) != 1:
                failure_reason = f"BREADTH_DROP_ROW_COUNT_INVALID: expected 1 row, found {len(rows)}"
            else:
                row = rows[0]
                schema_valid = True
                parsed_row = {
                    "day_utc": str(row.get("day_utc") or "").strip(),
                    "advance_decline_delta": row.get("advance_decline_delta"),
                    "breadth_down_pct": row.get("breadth_down_pct"),
                    "source": str(row.get("source") or "").strip(),
                    "timestamp_utc": str(row.get("timestamp_utc") or "").strip(),
                }
                failure_reason = _validate_breadth_row(day_utc=day_utc, row=parsed_row)
                if not failure_reason:
                    certification_status = "CERTIFIED"
    payload = {
        "schema_id": "aegis_breadth_drop_validation",
        "schema_version": "v1",
        "artifact_id": "aegis_breadth_drop_validation_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "expected_path": str(path),
        "file_found": file_found,
        "schema_valid": schema_valid,
        "certification_status": certification_status,
        "failure_reason": failure_reason,
        "parsed_row": parsed_row,
        "file_hash": _sha256_file(path) if file_found else "",
        "next_operator_action": "" if certification_status == "CERTIFIED" else f"Provide {path.name} at {path} and rerun {REPAIR_COMMAND}",
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _validate_breadth_row(*, day_utc: str, row: dict[str, Any]) -> str:
    row_day = str(row.get("day_utc") or "").strip()
    if row_day != day_utc:
        return f"BREADTH_DROP_DAY_MISMATCH: day_utc {row_day or 'MISSING'} != {day_utc}"
    advance_decline_delta = _to_number(row.get("advance_decline_delta"))
    if advance_decline_delta is None:
        return "BREADTH_DROP_ADVANCE_DECLINE_DELTA_INVALID: advance_decline_delta must be numeric"
    breadth_down_pct = _to_number(row.get("breadth_down_pct"))
    if breadth_down_pct is None:
        return "BREADTH_DROP_BREADTH_DOWN_PCT_INVALID: breadth_down_pct must be numeric"
    if breadth_down_pct < 0 or breadth_down_pct > 100:
        return f"BREADTH_DROP_BREADTH_DOWN_PCT_RANGE_INVALID: {breadth_down_pct} must be between 0 and 100"
    if not str(row.get("timestamp_utc") or "").strip():
        return "BREADTH_DROP_TIMESTAMP_MISSING: timestamp_utc is required"
    if not str(row.get("source") or "").strip():
        return "BREADTH_DROP_SOURCE_MISSING: source is required"
    return ""


def _to_number(value: Any) -> float | int | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return int(number) if number.is_integer() else number


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
