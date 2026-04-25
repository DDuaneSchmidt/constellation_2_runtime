from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    read_validated_surface_v1,
    resolve_market_calendar_record_v1,
    sha256_file_v1,
)
from constellation_2.common.session_authority_v1 import resolve_session_authority_target_day_v1
from constellation_2.phaseJ.tools import market_calendar_ingest_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
MARKET_CALENDAR_COVERAGE_STATUS_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/market_calendar_coverage_status.v1.schema.json"
)
MARKET_CALENDAR_COVERAGE_STATUS_ARTIFACT_FAMILY = "market_calendar_coverage_status_v1"
MARKET_CALENDAR_COVERAGE_OWNER = "market_calendar_coverage_authority_v1"
DEFAULT_SOURCE_ROOT = (
    REPO_ROOT / "constellation_2" / "phaseJ" / "source_data" / "market_calendar_source_v1"
).resolve()
DEFAULT_REQUIRED_OFFSET_CALENDAR_DAYS = 1
DEFAULT_POLICY_BUFFER_CALENDAR_DAYS = 3

COVERAGE_STATUS_HEALTHY = "HEALTHY"
COVERAGE_STATUS_WARNING = "WARNING"
COVERAGE_STATUS_BLOCKED = "BLOCKED"

SEVERITY_INFO = "INFO"
SEVERITY_WARNING = "WARNING"
SEVERITY_CRITICAL = "CRITICAL"

REASON_SOURCE_NOT_EXTENDED = "SOURCE_NOT_EXTENDED"
REASON_RUNTIME_NOT_REFRESHED = "RUNTIME_NOT_REFRESHED"
REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH = "SOURCE_AND_RUNTIME_RANGE_MISMATCH"
REASON_COVERAGE_BELOW_POLICY_BUFFER = "COVERAGE_BELOW_POLICY_BUFFER"
REASON_MANIFEST_MISSING = "MANIFEST_MISSING"
REASON_SCHEMA_INVALID = "SCHEMA_INVALID"
REASON_INGEST_REFRESH_FAILED = "INGEST_REFRESH_FAILED"

ACTION_NONE = "NONE"
ACTION_EXTEND_GOVERNED_SOURCE = "EXTEND_GOVERNED_SOURCE"
ACTION_REFRESH_RUNTIME_FROM_SOURCE = "REFRESH_RUNTIME_FROM_SOURCE"
ACTION_REPAIR_MANIFEST_OR_SCHEMA = "REPAIR_MANIFEST_OR_SCHEMA"
ACTION_INSPECT_INGEST_REFRESH_FAILURE = "INSPECT_INGEST_REFRESH_FAILURE"


@dataclass(frozen=True)
class MarketCalendarCoverageRefV1:
    path: Path
    payload: Dict[str, Any]
    sha256: str


def _utc_now(now: datetime | None = None) -> str:
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _blank_ref(path: str = "") -> Dict[str, str]:
    return {"artifact_path": str(path or ""), "artifact_sha256": ""}


def _surface_ref_dict(ref: SurfaceRefV1 | MarketCalendarCoverageRefV1 | None) -> Dict[str, str]:
    if ref is None:
        return _blank_ref()
    return {"artifact_path": str(ref.path), "artifact_sha256": str(ref.sha256)}


def _parse_day(day_text: str) -> str:
    return date.fromisoformat(str(day_text).strip()).isoformat()


def _day_plus(day_text: str, offset_days: int) -> str:
    return (date.fromisoformat(_parse_day(day_text)) + timedelta(days=int(offset_days))).isoformat()


def _read_json_object(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"JSON_PARSE_ERROR:{path}:{type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return payload


def _parse_bool(value: str) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"BAD_BOOL:{value!r}")


def _artifact_ref_from_path(path: Path) -> Dict[str, str]:
    if not path.exists() or not path.is_file():
        return _blank_ref(str(path))
    return {"artifact_path": str(path), "artifact_sha256": sha256_file_v1(path)}


def resolve_market_calendar_coverage_status_path(*, truth_root: Path) -> Path:
    return (Path(truth_root).resolve() / MARKET_CALENDAR_COVERAGE_STATUS_ARTIFACT_FAMILY / "current.json").resolve()


def default_market_calendar_source_root_v1(*, repo_root: Path | None = None) -> Path:
    root = Path(repo_root or REPO_ROOT).resolve()
    return (root / "constellation_2" / "phaseJ" / "source_data" / "market_calendar_source_v1").resolve()


def resolve_market_calendar_required_target_day_v1(
    *,
    required_target_day: str | None = None,
    now: datetime | None = None,
    minimum_required_offset_calendar_days: int = DEFAULT_REQUIRED_OFFSET_CALENDAR_DAYS,
) -> str:
    explicit = str(required_target_day or "").strip()
    if explicit:
        return _parse_day(explicit)
    base_day = resolve_session_authority_target_day_v1(now=now)
    return _day_plus(base_day, max(int(minimum_required_offset_calendar_days), 0))


def _validated_source_rows(source_path: Path, expected_hash: str) -> Dict[str, Any]:
    actual_hash = sha256_file_v1(source_path)
    if expected_hash and actual_hash != expected_hash:
        raise ValueError(f"SOURCE_HASH_MISMATCH:{source_path}")
    with source_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
    if not rows:
        raise ValueError(f"SOURCE_EMPTY:{source_path}")
    if "day_utc" not in reader.fieldnames or "is_trading_session" not in reader.fieldnames:
        raise ValueError(f"SOURCE_COLUMNS_INVALID:{source_path}")
    days: List[str] = []
    for row in rows:
        day_utc = _parse_day(str(row.get("day_utc") or "").strip())
        _parse_bool(str(row.get("is_trading_session") or "").strip())
        days.append(day_utc)
    ordered = sorted(days)
    if len(ordered) != len(set(ordered)):
        raise ValueError(f"SOURCE_DUPLICATE_DAY:{source_path}")
    return {
        "source_path": str(source_path),
        "source_hash": actual_hash,
        "coverage_start": ordered[0],
        "coverage_end": ordered[-1],
        "days": ordered,
    }


def _inspect_source_bundle(*, source_root: Path) -> Dict[str, Any]:
    manifest_path = (Path(source_root).resolve() / "dataset_manifest.json").resolve()
    result: Dict[str, Any] = {
        "manifest_path": manifest_path,
        "manifest_ref": _artifact_ref_from_path(manifest_path),
        "reason_codes": [],
        "exchange": "",
        "dataset_version": "",
        "source_name": "",
        "coverage_start": "",
        "coverage_end": "",
        "days": set(),
        "files": [],
    }
    if not manifest_path.exists() or not manifest_path.is_file():
        result["reason_codes"].append(REASON_MANIFEST_MISSING)
        return result
    try:
        manifest = _read_json_object(manifest_path)
    except ValueError:
        result["reason_codes"].append(REASON_SCHEMA_INVALID)
        return result
    result["manifest"] = manifest
    exchange = str(manifest.get("exchange") or "").strip().upper()
    schema_version = str(manifest.get("schema_version") or "").strip()
    source_name = str(manifest.get("dataset_id") or manifest.get("schema_id") or "").strip()
    source_files = manifest.get("source_files")
    source_hashes = manifest.get("source_file_hashes")
    effective_range = manifest.get("effective_date_range")
    if (
        not exchange
        or not schema_version
        or not source_name
        or not isinstance(source_files, list)
        or not source_files
        or not isinstance(source_hashes, dict)
        or not isinstance(effective_range, dict)
    ):
        result["reason_codes"].append(REASON_SCHEMA_INVALID)
        return result
    result["exchange"] = exchange
    result["dataset_version"] = schema_version
    result["source_name"] = source_name
    all_days: set[str] = set()
    file_rows: List[Dict[str, Any]] = []
    for item in source_files:
        relpath = str(item or "").strip()
        if not relpath:
            result["reason_codes"].append(REASON_SCHEMA_INVALID)
            return result
        file_path = (Path(source_root).resolve() / relpath).resolve()
        expected_hash = str(source_hashes.get(relpath) or "").strip()
        if not file_path.exists() or not file_path.is_file() or not expected_hash:
            result["reason_codes"].append(REASON_SCHEMA_INVALID)
            return result
        try:
            file_state = _validated_source_rows(file_path, expected_hash)
        except ValueError:
            result["reason_codes"].append(REASON_SCHEMA_INVALID)
            return result
        file_state["relative_path"] = relpath
        file_rows.append(file_state)
        all_days.update(file_state["days"])
    if not all_days:
        result["reason_codes"].append(REASON_SCHEMA_INVALID)
        return result
    coverage_start = min(all_days)
    coverage_end = max(all_days)
    manifest_start = str(effective_range.get("start") or "").strip()
    manifest_end = str(effective_range.get("end") or "").strip()
    if not manifest_start or not manifest_end or manifest_start != coverage_start or manifest_end != coverage_end:
        result["reason_codes"].append(REASON_SCHEMA_INVALID)
        return result
    result["coverage_start"] = coverage_start
    result["coverage_end"] = coverage_end
    result["days"] = set(all_days)
    result["files"] = sorted(file_rows, key=lambda row: (row["coverage_start"], row["relative_path"]))
    return result


def _inspect_runtime_calendar(*, truth_root: Path) -> Dict[str, Any]:
    manifest_path = (Path(truth_root).resolve() / "market_calendar_v1" / "dataset_manifest.json").resolve()
    result: Dict[str, Any] = {
        "manifest_path": manifest_path,
        "manifest_ref": _artifact_ref_from_path(manifest_path),
        "reason_codes": [],
        "coverage_start": "",
        "coverage_end": "",
        "dataset_version": "",
        "files": [],
    }
    if not manifest_path.exists() or not manifest_path.is_file():
        result["reason_codes"].append(REASON_MANIFEST_MISSING)
        return result
    try:
        manifest = _read_json_object(manifest_path)
    except ValueError:
        result["reason_codes"].append(REASON_SCHEMA_INVALID)
        return result
    result["manifest"] = manifest
    date_range = manifest.get("date_range")
    files = manifest.get("files")
    dataset_version = str(manifest.get("dataset_version") or "").strip()
    if not isinstance(date_range, dict) or not isinstance(files, list) or not dataset_version:
        result["reason_codes"].append(REASON_SCHEMA_INVALID)
        return result
    coverage_start = str(date_range.get("start") or "").strip()
    coverage_end = str(date_range.get("end") or "").strip()
    if not coverage_start or not coverage_end:
        result["reason_codes"].append(REASON_SCHEMA_INVALID)
        return result
    result["coverage_start"] = coverage_start
    result["coverage_end"] = coverage_end
    result["dataset_version"] = dataset_version
    result["files"] = [dict(item) for item in files if isinstance(item, dict)]
    return result


def _runtime_has_day(*, truth_root: Path, day_utc: str) -> bool:
    resolved = resolve_market_calendar_record_v1(truth_root=truth_root, day_utc=day_utc)
    return str(resolved.get("status") or "").strip().upper() == "OK"


def _policy_object(
    *,
    base_session_authority_day: str,
    required_target_day: str,
    buffer_calendar_days: int,
    minimum_required_offset_calendar_days: int,
) -> Dict[str, Any]:
    effective_buffer = max(int(buffer_calendar_days), int(minimum_required_offset_calendar_days), 0)
    return {
        "policy_id": "MARKET_CALENDAR_FORWARD_COVERAGE_POLICY_V1",
        "policy_mode": "SESSION_AUTHORITY_DAY_PLUS_CALENDAR_BUFFER",
        "base_session_authority_day": base_session_authority_day,
        "minimum_required_offset_calendar_days": max(int(minimum_required_offset_calendar_days), 0),
        "buffer_calendar_days": effective_buffer,
        "warning_target_day": _day_plus(base_session_authority_day, effective_buffer),
    }


def _reason_codes_unique(reason_codes: Iterable[str]) -> List[str]:
    unique_reasons: List[str] = []
    for reason in reason_codes:
        code = str(reason or "").strip()
        if code and code not in unique_reasons:
            unique_reasons.append(code)
    return unique_reasons


def _source_runtime_status_details(
    *,
    source_state: Mapping[str, Any],
    runtime_state: Mapping[str, Any],
    required_target_day: str,
    warning_target_day: str,
) -> Dict[str, Any]:
    source_reason_codes = _reason_codes_unique(source_state.get("reason_codes") or [])
    runtime_reason_codes = _reason_codes_unique(runtime_state.get("reason_codes") or [])
    source_days = set(source_state.get("days") or set())
    source_required_target_day_covered = required_target_day in source_days
    source_warning_target_day_covered = warning_target_day in source_days
    runtime_truth_root = Path(str(runtime_state.get("truth_root") or "")).resolve()
    runtime_required_target_day_covered = False
    runtime_warning_target_day_covered = False
    if not runtime_reason_codes and str(runtime_state.get("truth_root") or "").strip():
        runtime_required_target_day_covered = _runtime_has_day(
            truth_root=runtime_truth_root,
            day_utc=required_target_day,
        )
        runtime_warning_target_day_covered = _runtime_has_day(
            truth_root=runtime_truth_root,
            day_utc=warning_target_day,
        )
    source_status = COVERAGE_STATUS_HEALTHY
    if REASON_MANIFEST_MISSING in source_reason_codes or REASON_SCHEMA_INVALID in source_reason_codes:
        source_status = COVERAGE_STATUS_BLOCKED
    elif not source_required_target_day_covered:
        source_status = COVERAGE_STATUS_BLOCKED
    elif not source_warning_target_day_covered:
        source_status = COVERAGE_STATUS_WARNING

    runtime_status = COVERAGE_STATUS_HEALTHY
    runtime_start = str(runtime_state.get("coverage_start") or "").strip()
    runtime_end = str(runtime_state.get("coverage_end") or "").strip()
    source_start = str(source_state.get("coverage_start") or "").strip()
    source_end = str(source_state.get("coverage_end") or "").strip()
    if REASON_MANIFEST_MISSING in runtime_reason_codes or REASON_SCHEMA_INVALID in runtime_reason_codes:
        runtime_status = COVERAGE_STATUS_BLOCKED
    elif not runtime_required_target_day_covered:
        runtime_status = COVERAGE_STATUS_BLOCKED
    elif source_start and source_end and runtime_start and runtime_end and (source_start != runtime_start or source_end != runtime_end):
        runtime_status = COVERAGE_STATUS_WARNING
    elif not runtime_warning_target_day_covered:
        runtime_status = COVERAGE_STATUS_WARNING

    return {
        "source_status": source_status,
        "runtime_status": runtime_status,
        "source_required_target_day_covered": source_required_target_day_covered,
        "runtime_required_target_day_covered": runtime_required_target_day_covered,
        "source_warning_target_day_covered": source_warning_target_day_covered,
        "runtime_warning_target_day_covered": runtime_warning_target_day_covered,
    }


def _operator_action_code(reason_codes: Iterable[str]) -> str:
    codes = _reason_codes_unique(reason_codes)
    if not codes:
        return ACTION_NONE
    if REASON_INGEST_REFRESH_FAILED in codes:
        return ACTION_INSPECT_INGEST_REFRESH_FAILURE
    if REASON_MANIFEST_MISSING in codes or REASON_SCHEMA_INVALID in codes:
        return ACTION_REPAIR_MANIFEST_OR_SCHEMA
    if REASON_SOURCE_NOT_EXTENDED in codes or REASON_COVERAGE_BELOW_POLICY_BUFFER in codes:
        return ACTION_EXTEND_GOVERNED_SOURCE
    if REASON_RUNTIME_NOT_REFRESHED in codes or REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH in codes:
        return ACTION_REFRESH_RUNTIME_FROM_SOURCE
    return ACTION_REPAIR_MANIFEST_OR_SCHEMA


def _coverage_summary_text(
    *,
    required_target_day: str,
    warning_target_day: str,
    source_coverage_start: str,
    source_coverage_end: str,
    runtime_coverage_start: str,
    runtime_coverage_end: str,
    source_required_target_day_covered: bool,
    runtime_required_target_day_covered: bool,
    source_warning_target_day_covered: bool,
    runtime_warning_target_day_covered: bool,
) -> str:
    return (
        "required_target_day="
        f"{required_target_day} "
        f"warning_target_day={warning_target_day} "
        f"source_range={source_coverage_start or 'NONE'}..{source_coverage_end or 'NONE'} "
        f"runtime_range={runtime_coverage_start or 'NONE'}..{runtime_coverage_end or 'NONE'} "
        f"source_required_day={'YES' if source_required_target_day_covered else 'NO'} "
        f"runtime_required_day={'YES' if runtime_required_target_day_covered else 'NO'} "
        f"source_buffer_day={'YES' if source_warning_target_day_covered else 'NO'} "
        f"runtime_buffer_day={'YES' if runtime_warning_target_day_covered else 'NO'}"
    )


def _coverage_summary(
    *,
    source_state: Mapping[str, Any],
    runtime_state: Mapping[str, Any],
    required_target_day: str,
    warning_target_day: str,
) -> Dict[str, Any]:
    reasons: List[str] = []
    if REASON_MANIFEST_MISSING in source_state.get("reason_codes", []):
        reasons.append(REASON_MANIFEST_MISSING)
    elif REASON_SCHEMA_INVALID in source_state.get("reason_codes", []):
        reasons.append(REASON_SCHEMA_INVALID)
    elif required_target_day not in set(source_state.get("days") or set()):
        reasons.append(REASON_SOURCE_NOT_EXTENDED)
    if not reasons:
        if REASON_MANIFEST_MISSING in runtime_state.get("reason_codes", []):
            reasons.extend([REASON_MANIFEST_MISSING, REASON_RUNTIME_NOT_REFRESHED])
        elif REASON_SCHEMA_INVALID in runtime_state.get("reason_codes", []):
            reasons.append(REASON_SCHEMA_INVALID)
        elif not _runtime_has_day(truth_root=Path(runtime_state["truth_root"]), day_utc=required_target_day):
            reasons.append(REASON_RUNTIME_NOT_REFRESHED)
    source_start = str(source_state.get("coverage_start") or "").strip()
    source_end = str(source_state.get("coverage_end") or "").strip()
    runtime_start = str(runtime_state.get("coverage_start") or "").strip()
    runtime_end = str(runtime_state.get("coverage_end") or "").strip()
    if source_start and source_end and runtime_start and runtime_end and (source_start != runtime_start or source_end != runtime_end):
        reasons.append(REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH)
    if not reasons:
        source_days = set(source_state.get("days") or set())
        runtime_warning_ok = _runtime_has_day(truth_root=Path(runtime_state["truth_root"]), day_utc=warning_target_day)
        if warning_target_day not in source_days or not runtime_warning_ok:
            reasons.append(REASON_COVERAGE_BELOW_POLICY_BUFFER)
    unique_reasons = _reason_codes_unique(reasons)
    if any(reason in {REASON_SOURCE_NOT_EXTENDED, REASON_RUNTIME_NOT_REFRESHED, REASON_MANIFEST_MISSING, REASON_SCHEMA_INVALID, REASON_INGEST_REFRESH_FAILED} for reason in unique_reasons):
        return {"coverage_status": COVERAGE_STATUS_BLOCKED, "severity": SEVERITY_CRITICAL, "reason_codes": unique_reasons}
    if unique_reasons:
        return {"coverage_status": COVERAGE_STATUS_WARNING, "severity": SEVERITY_WARNING, "reason_codes": unique_reasons}
    return {"coverage_status": COVERAGE_STATUS_HEALTHY, "severity": SEVERITY_INFO, "reason_codes": []}


def _recommended_action(reason_codes: Iterable[str]) -> str:
    codes = [str(code).strip() for code in reason_codes if str(code).strip()]
    if not codes:
        return "No operator action required."
    if REASON_SOURCE_NOT_EXTENDED in codes:
        return "Extend the governed market-calendar source dataset through the required target day, then rerun market-calendar coverage authority and Session Authority."
    if REASON_RUNTIME_NOT_REFRESHED in codes:
        return "Run the approved market-calendar coverage REFRESH path to ingest governed source rows into canonical runtime truth."
    if REASON_COVERAGE_BELOW_POLICY_BUFFER in codes:
        return "Extend governed source and runtime market-calendar coverage to restore the forward buffer before the next rollover window."
    if REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH in codes:
        return "Inspect source versus runtime coverage ranges and refresh canonical runtime truth if the source is ahead."
    if REASON_MANIFEST_MISSING in codes:
        return "Restore the missing market-calendar manifest or rerun the approved market-calendar ingest flow."
    if REASON_SCHEMA_INVALID in codes:
        return "Repair the market-calendar source or runtime manifest shape before attempting refresh."
    if REASON_INGEST_REFRESH_FAILED in codes:
        return "Inspect the failed market-calendar ingest run, correct the append-only source file sequence, and rerun refresh."
    return "Inspect market-calendar coverage authority status and rerun after resolving the blocker."


def build_market_calendar_coverage_status_payload_v1(
    *,
    truth_root: Path,
    source_root: Path | None = None,
    required_target_day: str | None = None,
    buffer_calendar_days: int = DEFAULT_POLICY_BUFFER_CALENDAR_DAYS,
    minimum_required_offset_calendar_days: int = DEFAULT_REQUIRED_OFFSET_CALENDAR_DAYS,
    now: datetime | None = None,
    refresh_actions: Iterable[Mapping[str, Any]] | None = None,
    refresh_failed: bool = False,
) -> Dict[str, Any]:
    root = Path(truth_root).resolve()
    source = Path(source_root or default_market_calendar_source_root_v1()).resolve()
    base_session_authority_day = resolve_session_authority_target_day_v1(now=now)
    normalized_required_target_day = resolve_market_calendar_required_target_day_v1(
        required_target_day=required_target_day,
        now=now,
        minimum_required_offset_calendar_days=minimum_required_offset_calendar_days,
    )
    policy = _policy_object(
        base_session_authority_day=base_session_authority_day,
        required_target_day=normalized_required_target_day,
        buffer_calendar_days=buffer_calendar_days,
        minimum_required_offset_calendar_days=minimum_required_offset_calendar_days,
    )
    source_state = _inspect_source_bundle(source_root=source)
    runtime_state = _inspect_runtime_calendar(truth_root=root)
    runtime_state["truth_root"] = str(root)
    status_details = _source_runtime_status_details(
        source_state=source_state,
        runtime_state=runtime_state,
        required_target_day=normalized_required_target_day,
        warning_target_day=str(policy.get("warning_target_day") or ""),
    )
    summary = _coverage_summary(
        source_state=source_state,
        runtime_state=runtime_state,
        required_target_day=normalized_required_target_day,
        warning_target_day=str(policy.get("warning_target_day") or ""),
    )
    reason_codes = list(summary["reason_codes"])
    if refresh_failed and REASON_INGEST_REFRESH_FAILED not in reason_codes:
        reason_codes.append(REASON_INGEST_REFRESH_FAILED)
        summary = {
            "coverage_status": COVERAGE_STATUS_BLOCKED,
            "severity": SEVERITY_CRITICAL,
            "reason_codes": reason_codes,
        }
    operator_action_code = _operator_action_code(reason_codes)
    warning_target_day = str(policy.get("warning_target_day") or "")
    source_coverage_start = str(source_state.get("coverage_start") or "")
    source_coverage_end = str(source_state.get("coverage_end") or "")
    runtime_coverage_start = str(runtime_state.get("coverage_start") or "")
    runtime_coverage_end = str(runtime_state.get("coverage_end") or "")
    return {
        "schema_id": "market_calendar_coverage_status",
        "schema_version": "v1",
        "generated_utc": _utc_now(now),
        "source_coverage_start": source_coverage_start,
        "source_coverage_end": source_coverage_end,
        "runtime_coverage_start": runtime_coverage_start,
        "runtime_coverage_end": runtime_coverage_end,
        "required_target_day": normalized_required_target_day,
        "required_forward_coverage_policy": policy,
        "warning_target_day": warning_target_day,
        "source_status": str(status_details["source_status"]),
        "runtime_status": str(status_details["runtime_status"]),
        "source_required_target_day_covered": bool(status_details["source_required_target_day_covered"]),
        "runtime_required_target_day_covered": bool(status_details["runtime_required_target_day_covered"]),
        "source_warning_target_day_covered": bool(status_details["source_warning_target_day_covered"]),
        "runtime_warning_target_day_covered": bool(status_details["runtime_warning_target_day_covered"]),
        "coverage_status": str(summary["coverage_status"]),
        "reason_codes": reason_codes,
        "severity": str(summary["severity"]),
        "operator_action_code": operator_action_code,
        "recommended_action": _recommended_action(reason_codes),
        "status_summary": _coverage_summary_text(
            required_target_day=normalized_required_target_day,
            warning_target_day=warning_target_day,
            source_coverage_start=source_coverage_start,
            source_coverage_end=source_coverage_end,
            runtime_coverage_start=runtime_coverage_start,
            runtime_coverage_end=runtime_coverage_end,
            source_required_target_day_covered=bool(status_details["source_required_target_day_covered"]),
            runtime_required_target_day_covered=bool(status_details["runtime_required_target_day_covered"]),
            source_warning_target_day_covered=bool(status_details["source_warning_target_day_covered"]),
            runtime_warning_target_day_covered=bool(status_details["runtime_warning_target_day_covered"]),
        ),
        "source_manifest_ref": dict(source_state.get("manifest_ref") or _blank_ref(str(source_state.get("manifest_path") or ""))),
        "runtime_manifest_ref": dict(runtime_state.get("manifest_ref") or _blank_ref(str(runtime_state.get("manifest_path") or ""))),
        "source_root": str(source),
        "truth_root": str(root),
        "writer_owner": MARKET_CALENDAR_COVERAGE_OWNER,
        "refresh_actions": [dict(item) for item in (refresh_actions or []) if isinstance(item, Mapping)],
    }


def write_market_calendar_coverage_status_v1(
    *,
    truth_root: Path,
    payload: Mapping[str, Any],
) -> MarketCalendarCoverageRefV1:
    ref = atomic_write_validated_json_v1(
        path=resolve_market_calendar_coverage_status_path(truth_root=truth_root),
        payload=dict(payload),
        schema_relpath=MARKET_CALENDAR_COVERAGE_STATUS_SCHEMA_RELPATH,
    )
    return MarketCalendarCoverageRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def read_market_calendar_coverage_status_ref_v1(*, truth_root: Path) -> MarketCalendarCoverageRefV1:
    ref = read_control_plane_surface_v1(
        domain="session",
        surface="market_calendar_coverage_status_current",
        truth_root=Path(truth_root).resolve(),
    )
    return MarketCalendarCoverageRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def render_market_calendar_coverage_status_summary_v1(payload: Mapping[str, Any]) -> str:
    return (
        "MARKET_CALENDAR_COVERAGE_STATUS "
        f"severity={str(payload.get('severity') or '').strip()} "
        f"coverage_status={str(payload.get('coverage_status') or '').strip()} "
        f"source_status={str(payload.get('source_status') or '').strip()} "
        f"runtime_status={str(payload.get('runtime_status') or '').strip()} "
        f"required_target_day={str(payload.get('required_target_day') or 'NONE').strip() or 'NONE'} "
        f"warning_target_day={str(payload.get('warning_target_day') or 'NONE').strip() or 'NONE'} "
        f"source_range={str(payload.get('source_coverage_start') or 'NONE').strip() or 'NONE'}..{str(payload.get('source_coverage_end') or 'NONE').strip() or 'NONE'} "
        f"runtime_range={str(payload.get('runtime_coverage_start') or 'NONE').strip() or 'NONE'}..{str(payload.get('runtime_coverage_end') or 'NONE').strip() or 'NONE'} "
        f"source_required_day={'YES' if bool(payload.get('source_required_target_day_covered')) else 'NO'} "
        f"runtime_required_day={'YES' if bool(payload.get('runtime_required_target_day_covered')) else 'NO'} "
        f"source_buffer_day={'YES' if bool(payload.get('source_warning_target_day_covered')) else 'NO'} "
        f"runtime_buffer_day={'YES' if bool(payload.get('runtime_warning_target_day_covered')) else 'NO'} "
        f"reason_codes={','.join(payload.get('reason_codes') or []) or 'NONE'} "
        f"operator_action_code={str(payload.get('operator_action_code') or ACTION_NONE).strip()} "
        f"action={json.dumps(str(payload.get('recommended_action') or '').strip())}"
    )


def render_market_calendar_source_coverage_summary_v1(payload: Mapping[str, Any]) -> str:
    return (
        "MARKET_CALENDAR_SOURCE_COVERAGE_STATUS "
        f"severity={str(payload.get('severity') or '').strip()} "
        f"source_status={str(payload.get('source_status') or '').strip()} "
        f"required_target_day={str(payload.get('required_target_day') or 'NONE').strip() or 'NONE'} "
        f"warning_target_day={str(payload.get('warning_target_day') or 'NONE').strip() or 'NONE'} "
        f"source_range={str(payload.get('source_coverage_start') or 'NONE').strip() or 'NONE'}..{str(payload.get('source_coverage_end') or 'NONE').strip() or 'NONE'} "
        f"source_required_day={'YES' if bool(payload.get('source_required_target_day_covered')) else 'NO'} "
        f"source_buffer_day={'YES' if bool(payload.get('source_warning_target_day_covered')) else 'NO'} "
        f"reason_codes={','.join(payload.get('reason_codes') or []) or 'NONE'} "
        f"operator_action_code={str(payload.get('operator_action_code') or ACTION_NONE).strip()} "
        f"action={json.dumps(str(payload.get('recommended_action') or '').strip())}"
    )


def _refresh_action_row(
    *,
    source_file: str,
    source_hash: str,
    coverage_start: str,
    coverage_end: str,
    return_code: int,
) -> Dict[str, Any]:
    return {
        "source_file": str(source_file).strip(),
        "source_hash": str(source_hash).strip(),
        "coverage_start": str(coverage_start).strip(),
        "coverage_end": str(coverage_end).strip(),
        "return_code": int(return_code),
        "producer": "constellation_2/phaseJ/tools/market_calendar_ingest_v1.py",
    }


def refresh_market_calendar_coverage_v1(
    *,
    truth_root: Path,
    source_root: Path | None = None,
    required_target_day: str | None = None,
    buffer_calendar_days: int = DEFAULT_POLICY_BUFFER_CALENDAR_DAYS,
    minimum_required_offset_calendar_days: int = DEFAULT_REQUIRED_OFFSET_CALENDAR_DAYS,
    now: datetime | None = None,
) -> Dict[str, Any]:
    root = Path(truth_root).resolve()
    source = Path(source_root or default_market_calendar_source_root_v1()).resolve()
    source_state = _inspect_source_bundle(source_root=source)
    runtime_state = _inspect_runtime_calendar(truth_root=root)
    refresh_actions: List[Dict[str, Any]] = []
    refresh_failed = False
    normalized_required_target_day = resolve_market_calendar_required_target_day_v1(
        required_target_day=required_target_day,
        now=now,
        minimum_required_offset_calendar_days=minimum_required_offset_calendar_days,
    )
    if REASON_MANIFEST_MISSING in source_state.get("reason_codes", []) or REASON_SCHEMA_INVALID in source_state.get("reason_codes", []):
        return build_market_calendar_coverage_status_payload_v1(
            truth_root=root,
            source_root=source,
            required_target_day=normalized_required_target_day,
            buffer_calendar_days=buffer_calendar_days,
            minimum_required_offset_calendar_days=minimum_required_offset_calendar_days,
            now=now,
            refresh_actions=refresh_actions,
            refresh_failed=False,
        )
    source_days = set(source_state.get("days") or set())
    if normalized_required_target_day not in source_days:
        return build_market_calendar_coverage_status_payload_v1(
            truth_root=root,
            source_root=source,
            required_target_day=normalized_required_target_day,
            buffer_calendar_days=buffer_calendar_days,
            minimum_required_offset_calendar_days=minimum_required_offset_calendar_days,
            now=now,
            refresh_actions=refresh_actions,
            refresh_failed=False,
        )
    runtime_end = str(runtime_state.get("coverage_end") or "").strip()
    if _runtime_has_day(truth_root=root, day_utc=normalized_required_target_day):
        return build_market_calendar_coverage_status_payload_v1(
            truth_root=root,
            source_root=source,
            required_target_day=normalized_required_target_day,
            buffer_calendar_days=buffer_calendar_days,
            minimum_required_offset_calendar_days=minimum_required_offset_calendar_days,
            now=now,
            refresh_actions=refresh_actions,
            refresh_failed=False,
        )
    candidate_files = []
    overlapping_files = []
    for file_state in source_state.get("files", []):
        start_day = str(file_state.get("coverage_start") or "").strip()
        end_day = str(file_state.get("coverage_end") or "").strip()
        if not runtime_end:
            candidate_files.append(file_state)
            continue
        if start_day > runtime_end:
            candidate_files.append(file_state)
        elif end_day > runtime_end:
            overlapping_files.append(file_state)
    if overlapping_files:
        refresh_failed = True
    else:
        for file_state in candidate_files:
            source_file = str(file_state.get("relative_path") or "").strip()
            source_hash = str(file_state.get("source_hash") or "").strip()
            csv_path = Path(str(file_state.get("source_path") or "")).resolve()
            run_utc = _utc_now(now)
            try:
                return_code = market_calendar_ingest_v1.main(
                    [
                        "--dataset_version",
                        str(source_state.get("dataset_version") or "v1"),
                        "--run_utc",
                        run_utc,
                        "--exchange",
                        str(source_state.get("exchange") or "NYSE"),
                        "--csv",
                        str(csv_path),
                        "--source_name",
                        str(source_state.get("source_name") or "market_calendar_source_dataset_v1"),
                        "--source_hash",
                        source_hash,
                        "--truth_root",
                        str(root),
                    ]
                )
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 1
                return_code = int(code)
            refresh_actions.append(
                _refresh_action_row(
                    source_file=source_file,
                    source_hash=source_hash,
                    coverage_start=str(file_state.get("coverage_start") or ""),
                    coverage_end=str(file_state.get("coverage_end") or ""),
                    return_code=int(return_code),
                )
            )
            if int(return_code) != 0:
                refresh_failed = True
                break
    return build_market_calendar_coverage_status_payload_v1(
        truth_root=root,
        source_root=source,
        required_target_day=normalized_required_target_day,
        buffer_calendar_days=buffer_calendar_days,
        minimum_required_offset_calendar_days=minimum_required_offset_calendar_days,
        now=now,
        refresh_actions=refresh_actions,
        refresh_failed=refresh_failed,
    )
