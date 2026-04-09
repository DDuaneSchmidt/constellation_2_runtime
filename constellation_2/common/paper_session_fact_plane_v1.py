from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_intents_day_completeness_path,
    resolve_paper_day_control_plane_path,
    resolve_paper_session_evidence_manifest_path,
    resolve_paper_session_ledger_path,
    resolve_paper_session_kernel_path,
    resolve_paper_trading_posture_path,
    resolve_sleeve_rollup_path,
    resolve_startup_materialization_path,
    resolve_submit_boundary_status_path,
    resolve_startup_proof_validation_path,
    resolve_trading_day_control_plane_path,
    resolve_trading_day_execution_control_plane_path,
    resolve_trading_day_intent_generation_path,
    resolve_trading_day_state_machine_path,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
NON_AUTHORITY_SCOPE = "NON_AUTHORITY_FACT"
REPO_ROLE_FILENAME = "repo_role.v1.json"

STARTUP_MATERIALIZATION_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization.v1.schema.json"
)
INTENTS_DAY_COMPLETENESS_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/intents_day_completeness.v1.schema.json"
)
TRADING_DAY_INTENT_GENERATION_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_intent_generation.v1.schema.json"
)
PAPER_TRADING_POSTURE_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trading_posture.v1.schema.json"
)
SUBMIT_BOUNDARY_STATUS_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json"
)
SLEEVE_ROLLUP_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_rollup.v1.schema.json"
)
PAPER_SESSION_EVIDENCE_MANIFEST_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_evidence_manifest.v1.schema.json"
)
PAPER_SESSION_KERNEL_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_kernel.v1.schema.json"
)
PAPER_SESSION_LEDGER_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_ledger.v1.schema.json"
)
PAPER_DAY_CONTROL_PLANE_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_day_control_plane.v1.schema.json"
)
TRADING_DAY_CONTROL_PLANE_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_control_plane.v1.schema.json"
)
TRADING_DAY_EXECUTION_CONTROL_PLANE_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_execution_control_plane.v1.schema.json"
)
TRADING_DAY_STATE_MACHINE_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_state_machine.v1.schema.json"
)
STARTUP_PROOF_VALIDATION_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/startup_proof_validation.v1.schema.json"
)
TRADE_SUBMIT_READINESS_STATUS_SCHEMA_RELPATH_V1 = (
    "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json"
)

_SUPPORTED_IDENTITY_FILE_SETS: tuple[tuple[str, ...], ...] = (
    ("equity_order_plan.v2.json", "mapping_ledger_record.v2.json", "binding_record.v2.json"),
    ("equity_order_plan.v1.json", "mapping_ledger_record.v2.json", "binding_record.v2.json"),
    ("order_plan.v1.json", "mapping_ledger_record.v1.json", "binding_record.v1.json"),
)


@dataclass(frozen=True)
class SurfaceRefV1:
    path: Path
    payload: Dict[str, Any]
    sha256: str


def parse_day_utc_v1(day_utc: str) -> str:
    day = str(day_utc or "").strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {day_utc!r}")
    return day


def canonical_paper_session_id_v1(day_utc: str) -> str:
    day = parse_day_utc_v1(day_utc)
    return f"paper_session:{day}:PAPER"


def repo_git_sha_v1() -> str:
    authoritative_root = resolve_authoritative_repo_root_v1()
    try:
        return subprocess.check_output(
            ["/usr/bin/git", "rev-parse", "HEAD"],
            cwd=str(authoritative_root),
            text=True,
        ).strip()
    except Exception:
        return "UNKNOWN"


def resolve_authoritative_repo_root_v1(repo_root: Path | None = None) -> Path:
    root = Path(repo_root or REPO_ROOT).resolve()
    marker_path = (root / REPO_ROLE_FILENAME).resolve()
    if marker_path.exists() and marker_path.is_file():
        try:
            marker = read_json_object_v1(marker_path)
        except ValueError:
            marker = {}
        authoritative_text = str(marker.get("authoritative_repo_root") or "").strip()
        if authoritative_text:
            authoritative_root = Path(authoritative_text).expanduser().resolve()
            if authoritative_root.is_absolute():
                return authoritative_root
    return root


def resolve_fact_plane_truth_root_v1(truth_root: str | Path | None = None) -> Path:
    if truth_root is None or not str(truth_root).strip():
        return DEFAULT_TRUTH_ROOT
    root = Path(str(truth_root)).expanduser().resolve()
    if not root.is_absolute():
        raise ValueError(f"FACT_PLANE_TRUTH_ROOT_NOT_ABSOLUTE: {root}")
    return root


def now_utc_iso_v1() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json_object_v1(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ValueError(f"MISSING_FILE:path={path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"JSON_PARSE_ERROR:path={path}:err={type(exc).__name__}:{exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def sha256_file_v1(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic_write_validated_json_v1(*, path: Path, payload: Dict[str, Any], schema_relpath: str) -> SurfaceRefV1:
    validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
    raw = canonical_json_bytes_v1(payload) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_bytes(raw)
    os.replace(str(tmp), str(path))
    return SurfaceRefV1(path=path, payload=payload, sha256=hashlib.sha256(raw).hexdigest())


def _normalize_semantic_noop_value_v1(value: Any, *, volatile_field_names: set[str]) -> Any:
    if isinstance(value, dict):
        return {
            str(key): (
                None
                if str(key) in volatile_field_names
                else _normalize_semantic_noop_value_v1(item, volatile_field_names=volatile_field_names)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _normalize_semantic_noop_value_v1(item, volatile_field_names=volatile_field_names)
            for item in value
        ]
    return value


def atomic_write_idempotent_validated_json_v1(
    *,
    path: Path,
    payload: Dict[str, Any],
    schema_relpath: str,
    volatile_field_names: Iterable[str],
) -> SurfaceRefV1:
    validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
    normalized_volatile_names = {
        str(field_name).strip()
        for field_name in volatile_field_names
        if str(field_name).strip()
    }
    if path.exists() and path.is_file():
        try:
            existing_payload = read_json_object_v1(path)
            validate_against_repo_schema_v1(existing_payload, REPO_ROOT, schema_relpath)
        except Exception:
            existing_payload = None
        if isinstance(existing_payload, dict):
            existing_normalized = _normalize_semantic_noop_value_v1(
                existing_payload,
                volatile_field_names=normalized_volatile_names,
            )
            candidate_normalized = _normalize_semantic_noop_value_v1(
                payload,
                volatile_field_names=normalized_volatile_names,
            )
            if existing_normalized == candidate_normalized:
                raw = canonical_json_bytes_v1(existing_payload) + b"\n"
                return SurfaceRefV1(
                    path=path,
                    payload=existing_payload,
                    sha256=hashlib.sha256(raw).hexdigest(),
                )
    return atomic_write_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=schema_relpath,
    )


def read_validated_surface_v1(*, path: Path, schema_relpath: str) -> SurfaceRefV1:
    payload = read_json_object_v1(path)
    validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
    raw = canonical_json_bytes_v1(payload) + b"\n"
    return SurfaceRefV1(path=path, payload=payload, sha256=hashlib.sha256(raw).hexdigest())


def phasec_root_v1(*, truth_root: Path, day_utc: str) -> Path:
    return (Path(truth_root).resolve() / "phaseC_preflight_v1" / parse_day_utc_v1(day_utc)).resolve()


def supported_identity_dir_v1(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    for required_set in _SUPPORTED_IDENTITY_FILE_SETS:
        if all((path / name).exists() and (path / name).is_file() for name in required_set):
            return True
    return False


def discover_phasec_identity_dirs_v1(*, truth_root: Path, day_utc: str) -> List[Path]:
    root = phasec_root_v1(truth_root=truth_root, day_utc=day_utc)
    if not root.exists() or not root.is_dir():
        return []
    latest_pointer = (root / "latest_active_attempt.v1.json").resolve()
    if latest_pointer.exists() and latest_pointer.is_file():
        try:
            obj = read_json_object_v1(latest_pointer)
        except Exception:
            obj = {}
        attempt_dir = Path(str(obj.get("attempt_dir") or "").strip()).resolve() if str(obj.get("attempt_dir") or "").strip() else None
        if attempt_dir is not None and attempt_dir.exists() and attempt_dir.is_dir():
            out = [child.resolve() for child in sorted(attempt_dir.iterdir()) if child.is_dir() and supported_identity_dir_v1(child)]
            if out:
                return out
    out: List[Path] = []
    for child in sorted(root.iterdir()):
        if child.is_dir() and supported_identity_dir_v1(child):
            out.append(child.resolve())
        if child.is_dir() and child.name.startswith("attempt_"):
            out.extend(grand.resolve() for grand in sorted(child.iterdir()) if grand.is_dir() and supported_identity_dir_v1(grand))
    deduped: List[Path] = []
    seen: set[str] = set()
    for path in out:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def collect_intent_files_v1(*, truth_root: Path, day_utc: str) -> List[Path]:
    intents_dir = (Path(truth_root).resolve() / "intents_v1" / "snapshots" / parse_day_utc_v1(day_utc)).resolve()
    if not intents_dir.exists() or not intents_dir.is_dir():
        return []
    return sorted(
        path.resolve()
        for path in intents_dir.iterdir()
        if path.is_file() and path.name.endswith(".json") and path.name != "no_intents_day.v1.json"
    )


def resolve_market_calendar_record_v1(*, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    day = parse_day_utc_v1(day_utc)
    cal_root = (Path(truth_root).resolve() / "market_calendar_v1").resolve()
    manifest_path = (cal_root / "dataset_manifest.json").resolve()
    if not manifest_path.exists() or not manifest_path.is_file():
        return {
            "status": "MISSING",
            "reason_code": "MARKET_CALENDAR_MANIFEST_MISSING",
            "manifest_path": manifest_path,
            "year_path": None,
            "record": None,
        }
    try:
        manifest = read_json_object_v1(manifest_path)
    except ValueError:
        return {
            "status": "MALFORMED",
            "reason_code": "MARKET_CALENDAR_MANIFEST_MALFORMED",
            "manifest_path": manifest_path,
            "year_path": None,
            "record": None,
        }
    files = manifest.get("files")
    if not isinstance(files, list):
        return {
            "status": "MALFORMED",
            "reason_code": "MARKET_CALENDAR_FILES_LIST_MISSING",
            "manifest_path": manifest_path,
            "year_path": None,
            "record": None,
        }
    year_path: Path | None = None
    for entry in files:
        if not isinstance(entry, dict):
            continue
        if int(entry.get("year") or -1) != int(day[0:4]):
            continue
        rel = str(entry.get("file") or "").strip()
        if not rel:
            continue
        year_path = (cal_root / rel).resolve()
        break
    if year_path is None or not year_path.exists() or not year_path.is_file():
        return {
            "status": "MISSING",
            "reason_code": "MARKET_CALENDAR_YEAR_FILE_MISSING",
            "manifest_path": manifest_path,
            "year_path": year_path,
            "record": None,
        }
    try:
        for line in year_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            if str(row.get("day_utc") or "").strip() != day:
                continue
            value = row.get("is_trading_session")
            if not isinstance(value, bool):
                return {
                    "status": "MALFORMED",
                    "reason_code": "MARKET_CALENDAR_DAY_RECORD_MALFORMED",
                    "manifest_path": manifest_path,
                    "year_path": year_path,
                    "record": None,
                }
            return {
                "status": "OK",
                "reason_code": "MARKET_CALENDAR_DAY_RESOLVED",
                "manifest_path": manifest_path,
                "year_path": year_path,
                "record": row,
            }
    except Exception:
        return {
            "status": "MALFORMED",
            "reason_code": "MARKET_CALENDAR_YEAR_FILE_MALFORMED",
            "manifest_path": manifest_path,
            "year_path": year_path,
            "record": None,
        }
    return {
        "status": "MISSING",
        "reason_code": "MARKET_CALENDAR_DAY_MISSING",
        "manifest_path": manifest_path,
        "year_path": year_path,
        "record": None,
    }


def build_fact_dependency_row_v1(
    *,
    logical_name: str,
    absolute_path: Path | None,
    status: str,
    reason_codes: Iterable[str],
    day_utc: str,
) -> Dict[str, Any]:
    path_value = "" if absolute_path is None else str(Path(absolute_path).resolve())
    sha256 = ""
    if absolute_path is not None and absolute_path.exists() and absolute_path.is_file():
        sha256 = sha256_file_v1(absolute_path)
    return {
        "logical_name": str(logical_name),
        "absolute_path": path_value,
        "sha256": sha256,
        "day_utc": parse_day_utc_v1(day_utc),
        "status": str(status),
        "reason_codes": sorted({str(code).strip() for code in reason_codes if str(code).strip()}),
    }


def build_source_dependency_row_v1(
    *,
    logical_name: str,
    absolute_path: Path | None,
    status: str,
    reason_codes: Iterable[str],
    producer: str,
    day_utc: str,
) -> Dict[str, Any]:
    row = build_fact_dependency_row_v1(
        logical_name=logical_name,
        absolute_path=absolute_path,
        status=status,
        reason_codes=reason_codes,
        day_utc=day_utc,
    )
    row["producer"] = str(producer)
    return row


def validate_startup_materialization_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, STARTUP_MATERIALIZATION_SCHEMA_RELPATH_V1)


def validate_intents_day_completeness_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, INTENTS_DAY_COMPLETENESS_SCHEMA_RELPATH_V1)


def validate_paper_trading_posture_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_TRADING_POSTURE_SCHEMA_RELPATH_V1)


def validate_submit_boundary_status_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, SUBMIT_BOUNDARY_STATUS_SCHEMA_RELPATH_V1)


def validate_sleeve_rollup_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, SLEEVE_ROLLUP_SCHEMA_RELPATH_V1)


def validate_paper_session_evidence_manifest_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_EVIDENCE_MANIFEST_SCHEMA_RELPATH_V1)


def validate_paper_session_kernel_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_KERNEL_SCHEMA_RELPATH_V1)


def validate_paper_session_ledger_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_SESSION_LEDGER_SCHEMA_RELPATH_V1)


def validate_paper_day_control_plane_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, PAPER_DAY_CONTROL_PLANE_SCHEMA_RELPATH_V1)


def validate_trading_day_control_plane_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, TRADING_DAY_CONTROL_PLANE_SCHEMA_RELPATH_V1)


def validate_trading_day_execution_control_plane_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, TRADING_DAY_EXECUTION_CONTROL_PLANE_SCHEMA_RELPATH_V1)


def validate_trading_day_state_machine_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, TRADING_DAY_STATE_MACHINE_SCHEMA_RELPATH_V1)


def validate_trading_day_intent_generation_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, TRADING_DAY_INTENT_GENERATION_SCHEMA_RELPATH_V1)


def validate_startup_proof_validation_obj_v1(obj: Dict[str, Any]) -> None:
    validate_against_repo_schema_v1(obj, REPO_ROOT, STARTUP_PROOF_VALIDATION_SCHEMA_RELPATH_V1)


def read_startup_materialization_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_startup_materialization_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=STARTUP_MATERIALIZATION_SCHEMA_RELPATH_V1,
    )


def read_intents_day_completeness_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_intents_day_completeness_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=INTENTS_DAY_COMPLETENESS_SCHEMA_RELPATH_V1,
    )


def read_trading_day_intent_generation_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_trading_day_intent_generation_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=TRADING_DAY_INTENT_GENERATION_SCHEMA_RELPATH_V1,
    )


def read_paper_trading_posture_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_paper_trading_posture_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=PAPER_TRADING_POSTURE_SCHEMA_RELPATH_V1,
    )


def read_submit_boundary_status_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=SUBMIT_BOUNDARY_STATUS_SCHEMA_RELPATH_V1,
    )


def read_sleeve_rollup_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_sleeve_rollup_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=SLEEVE_ROLLUP_SCHEMA_RELPATH_V1,
    )


def read_paper_session_evidence_manifest_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_paper_session_evidence_manifest_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=PAPER_SESSION_EVIDENCE_MANIFEST_SCHEMA_RELPATH_V1,
    )


def read_paper_session_kernel_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_paper_session_kernel_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=PAPER_SESSION_KERNEL_SCHEMA_RELPATH_V1,
    )


def read_paper_session_ledger_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=PAPER_SESSION_LEDGER_SCHEMA_RELPATH_V1,
    )


def read_paper_day_control_plane_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_paper_day_control_plane_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=PAPER_DAY_CONTROL_PLANE_SCHEMA_RELPATH_V1,
    )


def read_trading_day_control_plane_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_trading_day_control_plane_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=TRADING_DAY_CONTROL_PLANE_SCHEMA_RELPATH_V1,
    )


def read_trading_day_execution_control_plane_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_trading_day_execution_control_plane_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=TRADING_DAY_EXECUTION_CONTROL_PLANE_SCHEMA_RELPATH_V1,
    )


def read_trading_day_state_machine_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=TRADING_DAY_STATE_MACHINE_SCHEMA_RELPATH_V1,
    )


def read_startup_proof_validation_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    return read_validated_surface_v1(
        path=resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day_utc),
        schema_relpath=STARTUP_PROOF_VALIDATION_SCHEMA_RELPATH_V1,
    )


def read_trade_submit_readiness_for_day_v1(
    *,
    truth_root: Path,
    day_utc: str,
    ib_account: str,
    environment: str = "PAPER",
) -> SurfaceRefV1:
    day = parse_day_utc_v1(day_utc)
    env = str(environment or "").strip().upper()
    account = str(ib_account or "").strip()
    history_path = (Path(truth_root).resolve() / "trade_submit_readiness_c2_v1" / "_history" / env / account / day / "status.json").resolve()
    current_path = (Path(truth_root).resolve() / "trade_submit_readiness_c2_v1" / env / account / "status.json").resolve()
    target = history_path if history_path.exists() and history_path.is_file() else current_path
    ref = read_validated_surface_v1(path=target, schema_relpath=TRADE_SUBMIT_READINESS_STATUS_SCHEMA_RELPATH_V1)
    payload = ref.payload
    if str(payload.get("environment") or "").strip().upper() != env:
        raise ValueError(f"TRADE_SUBMIT_READINESS_ENVIRONMENT_MISMATCH:path={target}")
    if str(payload.get("ib_account") or "").strip() != account:
        raise ValueError(f"TRADE_SUBMIT_READINESS_ACCOUNT_MISMATCH:path={target}")
    if str(payload.get("day_utc") or "").strip() != day:
        raise ValueError(f"TRADE_SUBMIT_READINESS_DAY_MISMATCH:path={target}")
    provenance = payload.get("provenance")
    truth_root_value = str(provenance.get("truth_root") or "").strip() if isinstance(provenance, dict) else ""
    if truth_root_value and truth_root_value != str(Path(truth_root).resolve()):
        raise ValueError(f"TRADE_SUBMIT_READINESS_TRUTH_ROOT_MISMATCH:path={target}")
    return ref


def producer_block_v1(*, module: str, git_sha: str | None = None) -> Dict[str, Any]:
    authoritative_root = resolve_authoritative_repo_root_v1()
    return {
        "repo": authoritative_root.name,
        "module": str(module),
        "git_sha": str(git_sha or repo_git_sha_v1()),
    }
