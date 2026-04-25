from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    parse_day_utc_v1,
    read_paper_day_control_plane_ref_v1,
    read_paper_session_ledger_ref_v1,
    read_submit_boundary_status_ref_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_authority_v1 import (
    read_paper_session_authority_ref_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_session_authority_path,
)
from constellation_2.common.execution_identity_binding_v1 import (
    RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISSING,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING,
    RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING,
    RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION,
    RC_EXECUTION_IDENTITY_SLEEVE_MISSING,
    RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED,
)
from constellation_2.common.market_calendar_coverage_authority_v1 import (
    COVERAGE_STATUS_BLOCKED,
    COVERAGE_STATUS_WARNING,
    REASON_COVERAGE_BELOW_POLICY_BUFFER,
    REASON_INGEST_REFRESH_FAILED,
    REASON_MANIFEST_MISSING,
    REASON_RUNTIME_NOT_REFRESHED,
    REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH,
    REASON_SOURCE_NOT_EXTENDED,
    build_market_calendar_coverage_status_payload_v1,
    read_market_calendar_coverage_status_ref_v1,
    resolve_market_calendar_coverage_status_path,
)
from constellation_2.common.constitutional_runtime_v1 import (
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_COMPLETE,
    CLOSURE_STATE_DEGRADED,
    CLOSURE_STATE_OPEN,
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_machine_blocker_envelope_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.operator_semantic_classifier_v1 import (
    classify_operator_semantic_status_v1,
)
from constellation_2.common.session_authority_v1 import (
    ACTIVE_SESSION_SCHEMA_RELPATH,
    TARGET_DAY_ADMISSION_SCHEMA_RELPATH,
    TARGET_DAY_BUILD_SCHEMA_RELPATH,
    read_active_session_ref_v1,
    read_target_day_admission_ref_v1,
    read_target_day_build_ref_v1,
    resolve_active_session_path,
    resolve_session_authority_target_day_v1,
)
from constellation_2.common.subsystem_authority_v1 import (
    RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS,
    RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS,
    RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN,
    RC_EXECUTION_ROOT_MODE_MISSING,
    RC_EXECUTION_ROOT_PATH_MISMATCH,
    RC_EXECUTION_ROOT_PATH_UNRESOLVED,
    RC_EXECUTION_ROOT_SLEEVE_ID_MISSING,
    RC_EXECUTION_ROOT_AUTHORITY_AMBIGUOUS,
    SUBSYSTEM_ID_OPERATOR_SUMMARY_AUTHORITY,
    build_operator_summary_dossier_v1,
    resolve_operator_summary_dossier_path,
    resolve_subsystem_authority_manifest_path,
)

SOURCE_ROOT = Path(__file__).resolve().parents[2]

SESSION_AUTHORITY_STATUS_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/session_authority_status.v1.schema.json"
)
SESSION_AUTHORITY_ALERT_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/session_authority_alert.v1.schema.json"
)
SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY = "session_authority_status_v1"
SESSION_AUTHORITY_ALERT_ARTIFACT_FAMILY = "session_authority_alert_v1"
SESSION_AUTHORITY_MONITOR_OWNER = "session_authority_monitor_v1"
ENVIRONMENT_PAPER = "PAPER"

STATUS_SEVERITY_INFO = "INFO"
STATUS_SEVERITY_WARNING = "WARNING"
STATUS_SEVERITY_ERROR = "ERROR"
STATUS_SEVERITY_CRITICAL = "CRITICAL"

ALERT_STATUS_ALERT = "ALERT"
ALERT_STATUS_DEDUPED = "DEDUPED"
ALERT_STATUS_ESCALATED = "ESCALATED"
ALERT_STATUS_CLEAR = "CLEAR"
ALERT_STATUS_HEALTHY = "HEALTHY"

ROLLOVER_STATUS_WITHHELD = "ROLLOVER_WITHHELD"
BLOCKING_CODE_TRACEABILITY_BROKEN = "TRACEABILITY_BROKEN"
BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING = "SESSION_AUTHORITY_ARTIFACT_MISSING"
BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_INVALID = "SESSION_AUTHORITY_ARTIFACT_INVALID"
BLOCKING_CODE_ROLLOVER_WITHHELD = "ROLLOVER_WITHHELD"
BLOCKING_CODE_INTENT_INPUT_CONVERGENCE_BLOCKED = "PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_BLOCKED"
BLOCKING_CODE_STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_BLOCKED = "STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_BLOCKED"
BLOCKING_CODE_READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED = "READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED"
RC_SESSION_AUTHORITY_STATUS_STALE = "SESSION_AUTHORITY_STATUS_STALE"
RC_SESSION_AUTHORITY_CANONICAL_MISMATCH = "SESSION_AUTHORITY_CANONICAL_MISMATCH"

_SESSION_AUTHORITY_NOT_YET_MATERIALIZED_REASON_CODES = {
    BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING,
    "TARGET_DAY_ARTIFACT_MISSING",
}

_SESSION_AUTHORITY_UPSTREAM_PREREQUISITE_REASON_CODES = {
    BLOCKING_CODE_ROLLOVER_WITHHELD,
    BLOCKING_CODE_INTENT_INPUT_CONVERGENCE_BLOCKED,
    BLOCKING_CODE_STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_BLOCKED,
    REASON_SOURCE_NOT_EXTENDED,
    REASON_RUNTIME_NOT_REFRESHED,
    REASON_COVERAGE_BELOW_POLICY_BUFFER,
    REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH,
    REASON_MANIFEST_MISSING,
    REASON_INGEST_REFRESH_FAILED,
}

_CRITICAL_REASON_CODES = {
    "HIDDEN_DEPENDENCY_DETECTED",
    "WRONG_AUTHORITY_PATH",
    "SCHEMA_INVALID",
    "PARTIAL_BUILD",
    REASON_RUNTIME_NOT_REFRESHED,
    REASON_MANIFEST_MISSING,
    REASON_INGEST_REFRESH_FAILED,
    BLOCKING_CODE_TRACEABILITY_BROKEN,
    BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_INVALID,
    BLOCKING_CODE_READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED,
    RC_EXECUTION_ROOT_AUTHORITY_AMBIGUOUS,
    RC_EXECUTION_ROOT_SLEEVE_ID_MISSING,
    RC_EXECUTION_ROOT_MODE_MISSING,
    RC_EXECUTION_ROOT_PATH_UNRESOLVED,
    RC_EXECUTION_ROOT_PATH_MISMATCH,
    RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN,
    RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS,
    RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_SLEEVE_MISSING,
    RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING,
    RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISSING,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING,
    RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS,
    RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH,
    RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH,
    RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION,
    RC_SESSION_AUTHORITY_STATUS_STALE,
    RC_SESSION_AUTHORITY_CANONICAL_MISMATCH,
}
_WARNING_REASON_CODES = {
    REASON_SOURCE_NOT_EXTENDED,
    REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH,
    REASON_COVERAGE_BELOW_POLICY_BUFFER,
    "TARGET_DAY_ARTIFACT_MISSING",
    "TARGET_DAY_DATE_MISMATCH",
    "STALE_ARTIFACT",
    "PROVENANCE_MISSING",
    "REQUIRED_GATE_FAIL",
    "ADMISSION_RULE_BLOCKED",
    BLOCKING_CODE_ROLLOVER_WITHHELD,
    BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING,
    BLOCKING_CODE_INTENT_INPUT_CONVERGENCE_BLOCKED,
    BLOCKING_CODE_STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_BLOCKED,
}
AUTHORITY_LEVEL_DERIVED = "derived"
SESSION_AUTHORITY_STATUS_DERIVED_FROM_PAPER = (
    "paper_session_authority_v1",
    "active_session_v1",
    "target_day_admission_v1",
    "target_day_build_v1",
    "market_calendar_coverage_status_v1",
    "operator_summary_dossier_v1",
)
SESSION_AUTHORITY_STATUS_DERIVED_FROM_LIVE = (
    "submit_boundary_status_v1",
    "paper_session_ledger_v1",
    "paper_day_control_plane_v1",
    "active_session_v1",
    "target_day_admission_v1",
    "target_day_build_v1",
    "market_calendar_coverage_status_v1",
)


@dataclass(frozen=True)
class MonitorRefV1:
    path: Path
    payload: Dict[str, Any]
    sha256: str


@dataclass(frozen=True)
class CanonicalReadinessSurfaceV1:
    surface_name: str
    path: Path
    sha256: str
    produced_utc: str
    produced_dt: datetime | None
    submission_authorized: bool
    status_value: str
    reason_codes: tuple[str, ...]
    paper_open_allowed: bool | None = None
    degraded_mode: bool | None = None


def _utc_now(now: datetime | None = None) -> str:
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc(text: str) -> datetime | None:
    value = str(text or "").strip()
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _json_sha256(payload: Mapping[str, Any]) -> str:
    raw = (json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _ref_dict_from_surface(ref: SurfaceRefV1 | MonitorRefV1 | None) -> Dict[str, str]:
    if ref is None:
        return {"artifact_path": "", "artifact_sha256": ""}
    return {"artifact_path": str(ref.path), "artifact_sha256": str(ref.sha256)}


def _blank_ref_dict(path: str = "") -> Dict[str, str]:
    return {"artifact_path": str(path or ""), "artifact_sha256": ""}


def _ordered_codes(values: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        code = str(value).strip()
        if code and code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


def _derived_from_for_environment(environment: str) -> List[str]:
    normalized_environment = str(environment or "").strip().upper()
    if normalized_environment == ENVIRONMENT_PAPER:
        return list(SESSION_AUTHORITY_STATUS_DERIVED_FROM_PAPER)
    return list(SESSION_AUTHORITY_STATUS_DERIVED_FROM_LIVE)


def _advisory_severity_for_reason_code_v1(reason_code: str) -> str:
    code = str(reason_code or "").strip()
    if not code:
        return STATUS_SEVERITY_INFO
    if code in _CRITICAL_REASON_CODES:
        return STATUS_SEVERITY_CRITICAL
    if code in _WARNING_REASON_CODES:
        return STATUS_SEVERITY_WARNING
    return STATUS_SEVERITY_INFO


def _paper_advisory_reason_details_v1(
    *,
    advisory_reason_codes: Iterable[str],
    advisory_checks: Iterable[Mapping[str, Any]],
) -> List[Dict[str, str]]:
    checks = [row for row in advisory_checks if isinstance(row, Mapping)]
    details: List[Dict[str, str]] = []
    seen_codes: set[str] = set()
    for code in _ordered_codes(advisory_reason_codes):
        if code in seen_codes:
            continue
        seen_codes.add(code)
        matching_row = next(
            (
                row
                for row in checks
                if str(row.get("reason_code") or "").strip() == code
            ),
            {},
        )
        declared_severity = str(matching_row.get("severity") or "").strip().upper()
        severity = (
            declared_severity
            if declared_severity in {
                STATUS_SEVERITY_INFO,
                STATUS_SEVERITY_WARNING,
                STATUS_SEVERITY_CRITICAL,
            }
            else _advisory_severity_for_reason_code_v1(code)
        )
        details.append(
            {
                "reason_code": code,
                "severity": severity,
                "check_id": str(matching_row.get("check_id") or "").strip(),
                "summary": str(matching_row.get("summary") or "").strip(),
                "artifact_path": str(matching_row.get("artifact_path") or "").strip(),
            }
        )
    return details


def _highest_advisory_severity_v1(advisory_details: Iterable[Mapping[str, Any]]) -> str:
    seen = {
        str(row.get("severity") or "").strip().upper()
        for row in advisory_details
        if isinstance(row, Mapping)
    }
    if STATUS_SEVERITY_CRITICAL in seen:
        return STATUS_SEVERITY_CRITICAL
    if STATUS_SEVERITY_WARNING in seen:
        return STATUS_SEVERITY_WARNING
    if STATUS_SEVERITY_INFO in seen:
        return STATUS_SEVERITY_INFO
    return "NONE"


def _paper_authority_projection_defaults_v1(
    *,
    environment: str,
    authority_artifact_path: str = "",
    authority_status: str = "UNKNOWN",
    paper_open_allowed: bool = False,
    degraded_mode: bool = False,
    submission_authorized: bool = False,
    blocker_reason_codes: Iterable[str] = (),
    advisory_reason_codes: Iterable[str] = (),
    advisory_reason_details: Iterable[Mapping[str, Any]] = (),
    first_blocker_summary: str = "",
) -> Dict[str, Any]:
    normalized_environment = str(environment or "").strip().upper()
    blockers = _ordered_codes(blocker_reason_codes)
    advisories = _ordered_codes(advisory_reason_codes)
    advisory_details = _paper_advisory_reason_details_v1(
        advisory_reason_codes=advisories,
        advisory_checks=advisory_reason_details,
    )
    normalized_status = str(authority_status or "").strip().upper()
    if normalized_status not in {"GRANTED", "DENIED", "UNKNOWN"}:
        normalized_status = "UNKNOWN"
    if normalized_environment == ENVIRONMENT_PAPER:
        if normalized_status == "UNKNOWN":
            normalized_status = "DENIED"
        open_state = "OPEN_ALLOWED" if bool(paper_open_allowed) else "OPEN_BLOCKED"
    else:
        normalized_status = "UNKNOWN"
        open_state = "UNKNOWN"
    return {
        "contract_used": normalized_environment == ENVIRONMENT_PAPER,
        "authority_owner": "paper_session_authority_v1" if normalized_environment == ENVIRONMENT_PAPER else "",
        "authority_artifact_ref": _blank_ref_dict(authority_artifact_path),
        "authority_status": normalized_status,
        "paper_open_allowed": bool(paper_open_allowed),
        "open_state": open_state,
        "degraded_mode": bool(degraded_mode),
        "submission_authorized": bool(submission_authorized),
        "blocker_count": len(blockers),
        "advisory_count": len(advisories),
        "blocker_reason_codes": blockers,
        "advisory_reason_codes": advisories,
        "advisory_reason_details": advisory_details,
        "highest_advisory_severity": _highest_advisory_severity_v1(advisory_details),
        "first_blocker_code": blockers[0] if blockers else "",
        "first_blocker_summary": str(first_blocker_summary or "").strip(),
        "first_advisory_code": advisories[0] if advisories else "",
    }


def _paper_authority_projection_from_ref_v1(
    *,
    ref: SurfaceRefV1,
    environment: str,
) -> Dict[str, Any]:
    payload = dict(ref.payload)
    blocker_reason_codes = _ordered_codes(payload.get("blocking_reason_codes") or [])
    advisory_check_rows = [
        row
        for row in (payload.get("advisory_checks") or [])
        if isinstance(row, Mapping)
    ]
    advisory_reason_codes = _ordered_codes(
        str(row.get("reason_code") or "").strip()
        for row in advisory_check_rows
    )
    blocking_reason_details = [
        row
        for row in (payload.get("blocking_reason_details") or [])
        if isinstance(row, Mapping)
    ]
    first_blocker_summary = ""
    if blocker_reason_codes:
        first_blocker_code = blocker_reason_codes[0]
        for detail in blocking_reason_details:
            if str(detail.get("reason_code") or "").strip() == first_blocker_code:
                first_blocker_summary = str(detail.get("summary") or "").strip()
                break
    authority_status = str(payload.get("authority_status") or "UNKNOWN").strip().upper()
    paper_open_allowed = bool(payload.get("paper_open_allowed") is True)
    degraded_mode = bool(payload.get("degraded_mode") is True)
    submission_authorized = bool(payload.get("submission_authorized") is True)
    projection = _paper_authority_projection_defaults_v1(
        environment=environment,
        authority_artifact_path=str(ref.path),
        authority_status=authority_status,
        paper_open_allowed=paper_open_allowed,
        degraded_mode=degraded_mode,
        submission_authorized=submission_authorized,
        blocker_reason_codes=blocker_reason_codes,
        advisory_reason_codes=advisory_reason_codes,
        advisory_reason_details=advisory_check_rows,
        first_blocker_summary=first_blocker_summary,
    )
    projection["authority_artifact_ref"] = _ref_dict_from_surface(ref)
    return projection


def _build_paper_authority_projection_v1(
    *,
    truth_root: Path,
    day_utc: str,
    environment: str,
    checks: List[Dict[str, Any]],
) -> Dict[str, Any]:
    normalized_environment = str(environment or "").strip().upper()
    if normalized_environment != ENVIRONMENT_PAPER:
        return _paper_authority_projection_defaults_v1(environment=normalized_environment)
    authority_path = (
        resolve_paper_session_authority_path(truth_root=truth_root, day_utc=day_utc)
        if str(day_utc or "").strip()
        else Path()
    )
    authority_path_text = str(authority_path) if str(day_utc or "").strip() else ""
    if not str(day_utc or "").strip():
        summary = "PAPER authority projection is unavailable because no target day could be resolved."
        projection = _paper_authority_projection_defaults_v1(
            environment=normalized_environment,
            authority_artifact_path=authority_path_text,
            authority_status="DENIED",
            paper_open_allowed=False,
            blocker_reason_codes=[BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING],
            first_blocker_summary=summary,
        )
        checks.append(
            _check_row(
                check_name="paper_session_authority_projection",
                status="FAIL",
                severity=STATUS_SEVERITY_CRITICAL,
                reason_code=BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING,
                summary=summary,
                artifact_ref=projection["authority_artifact_ref"],
            )
        )
        return projection
    try:
        authority_ref = read_paper_session_authority_ref_v1(
            truth_root=truth_root,
            day_utc=day_utc,
        )
    except Exception as exc:
        reason_code = (
            BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_INVALID
            if authority_path.exists()
            else BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING
        )
        summary = (
            "Canonical PAPER authority artifact is unavailable or invalid "
            f"for day={day_utc}: {type(exc).__name__}."
        )
        projection = _paper_authority_projection_defaults_v1(
            environment=normalized_environment,
            authority_artifact_path=authority_path_text,
            authority_status="DENIED",
            paper_open_allowed=False,
            blocker_reason_codes=[reason_code],
            first_blocker_summary=summary,
        )
        checks.append(
            _check_row(
                check_name="paper_session_authority_projection",
                status="FAIL",
                severity=STATUS_SEVERITY_CRITICAL,
                reason_code=reason_code,
                summary=summary,
                artifact_ref=projection["authority_artifact_ref"],
            )
        )
        return projection

    projection = _paper_authority_projection_from_ref_v1(
        ref=authority_ref,
        environment=normalized_environment,
    )
    checks.append(
        _check_row(
            check_name="paper_session_authority_projection",
            status="PASS",
            severity=STATUS_SEVERITY_INFO,
            reason_code="",
            summary="Operator PAPER authority projection is derived from canonical paper_session_authority_v1.",
            artifact_ref=projection["authority_artifact_ref"],
            details={
                "authority_status": projection.get("authority_status"),
                "paper_open_allowed": projection.get("paper_open_allowed"),
                "degraded_mode": projection.get("degraded_mode"),
                "submission_authorized": projection.get("submission_authorized"),
                "blocker_reason_codes": list(projection.get("blocker_reason_codes") or []),
                "advisory_reason_codes": list(projection.get("advisory_reason_codes") or []),
                "advisory_reason_details": [
                    dict(row)
                    for row in (projection.get("advisory_reason_details") or [])
                    if isinstance(row, Mapping)
                ],
                "highest_advisory_severity": str(
                    projection.get("highest_advisory_severity") or "NONE"
                ).strip().upper(),
            },
        )
    )
    return projection


def _artifact_ref_from_path(path: Path) -> Dict[str, str]:
    if not path.exists() or not path.is_file():
        return _blank_ref_dict(str(path))
    return {"artifact_path": str(path), "artifact_sha256": sha256_file_v1(path)}


def _constitutional_ref_row(
    *,
    artifact_id: str,
    ref_dict: Mapping[str, Any] | None,
    artifact_class: str,
    finality_state: str = FINALITY_PROVISIONAL,
) -> Dict[str, Any] | None:
    path_text = str((ref_dict or {}).get("artifact_path") or "").strip()
    sha256 = str((ref_dict or {}).get("artifact_sha256") or "").strip()
    if not path_text or not sha256:
        return None
    return {
        "artifact_id": str(artifact_id).strip(),
        "path": path_text,
        "sha256": sha256,
        "artifact_class": str(artifact_class).strip(),
        "finality_state": str(finality_state).strip(),
    }


def _validate_governed_dependency_v1(*, artifact_id: str, ref_row: Mapping[str, Any] | None) -> str:
    if not isinstance(ref_row, Mapping):
        return ""
    path_text = str(ref_row.get("path") or "").strip()
    if not path_text:
        return ""
    try:
        dependency_payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
        if not isinstance(dependency_payload, Mapping):
            raise ValueError("TOP_LEVEL_NOT_OBJECT")
        validate_governed_artifact_payload_v1(
            repo_root=SOURCE_ROOT,
            artifact_id=artifact_id,
            payload=dependency_payload,
        )
    except Exception:
        return f"INVALID_GOVERNED_DEPENDENCY:{artifact_id}"
    return ""


def _session_authority_closure_state_v1(
    *,
    payload: Mapping[str, Any],
    blocking_codes: Iterable[str],
    missing_dependency_artifacts: Iterable[str],
) -> str:
    codes = [str(code).strip() for code in blocking_codes if str(code).strip()]
    missing = [str(dep).strip() for dep in missing_dependency_artifacts if str(dep).strip()]
    semantic_status = str(payload.get("semantic_status") or "").strip().upper()
    status_severity = str(payload.get("status_severity") or "").strip().upper()
    submission_authorized = bool(payload.get("submission_authorized") is True)
    if missing:
        return CLOSURE_STATE_BLOCKED
    if semantic_status == "NOT_YET_MATERIALIZED":
        return CLOSURE_STATE_OPEN
    if submission_authorized and status_severity == STATUS_SEVERITY_INFO and not codes:
        return CLOSURE_STATE_COMPLETE
    if status_severity == STATUS_SEVERITY_WARNING and codes:
        return CLOSURE_STATE_DEGRADED
    return CLOSURE_STATE_BLOCKED


def _finalize_session_authority_payload_v1(
    *,
    contract: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    normalized = _normalize_session_authority_status_semantics_v1(payload)
    constitutional_dependency_refs: List[Dict[str, Any]] = []
    active_session_dependency_ref = _constitutional_ref_row(
        artifact_id="active_session_v1",
        ref_dict=normalized.get("active_session_ref") if isinstance(normalized.get("active_session_ref"), Mapping) else None,
        artifact_class="compiled_state",
    )
    if active_session_dependency_ref is not None:
        constitutional_dependency_refs.append(active_session_dependency_ref)
    target_day_admission_dependency_ref = _constitutional_ref_row(
        artifact_id="target_day_admission_v1",
        ref_dict=normalized.get("target_day_admission_ref") if isinstance(normalized.get("target_day_admission_ref"), Mapping) else None,
        artifact_class="admission_result",
    )
    if target_day_admission_dependency_ref is not None:
        constitutional_dependency_refs.append(target_day_admission_dependency_ref)
    build_dependency_ref = _constitutional_ref_row(
        artifact_id="target_day_build_v1",
        ref_dict=normalized.get("target_day_build_ref") if isinstance(normalized.get("target_day_build_ref"), Mapping) else None,
        artifact_class="admission_result",
    )
    if build_dependency_ref is not None:
        constitutional_dependency_refs.append(build_dependency_ref)
    operator_summary_dependency_ref = _constitutional_ref_row(
        artifact_id="operator_summary_dossier_v1",
        ref_dict=normalized.get("operator_summary_dossier_ref") if isinstance(normalized.get("operator_summary_dossier_ref"), Mapping) else None,
        artifact_class="admission_result",
    )
    if operator_summary_dependency_ref is not None:
        constitutional_dependency_refs.append(operator_summary_dependency_ref)
    paper_authority_projection = (
        normalized.get("paper_authority_projection")
        if isinstance(normalized.get("paper_authority_projection"), Mapping)
        else {}
    )
    paper_authority_ref_dict = None
    if isinstance(paper_authority_projection.get("authority_artifact_ref"), Mapping):
        authority_path_text = str(
            (paper_authority_projection.get("authority_artifact_ref") or {}).get("artifact_path")
            or ""
        ).strip()
        if authority_path_text:
            paper_authority_ref_dict = _artifact_ref_from_path(Path(authority_path_text))
    paper_authority_dependency_ref = _constitutional_ref_row(
        artifact_id="paper_session_authority_v1",
        ref_dict=paper_authority_ref_dict,
        artifact_class="admission_result",
    )
    if paper_authority_dependency_ref is not None:
        constitutional_dependency_refs.append(paper_authority_dependency_ref)
    missing_dependency_artifacts: List[str] = []
    if active_session_dependency_ref is None:
        missing_dependency_artifacts.append("active_session_v1")
    if target_day_admission_dependency_ref is None:
        missing_dependency_artifacts.append("target_day_admission_v1")
    if build_dependency_ref is None:
        missing_dependency_artifacts.append("target_day_build_v1")
    if operator_summary_dependency_ref is None:
        missing_dependency_artifacts.append("operator_summary_dossier_v1")
    if paper_authority_dependency_ref is None:
        missing_dependency_artifacts.append("paper_session_authority_v1")
    dependency_validation_codes = [
        code
        for code in (
            _validate_governed_dependency_v1(artifact_id="active_session_v1", ref_row=active_session_dependency_ref),
            _validate_governed_dependency_v1(artifact_id="target_day_admission_v1", ref_row=target_day_admission_dependency_ref),
            _validate_governed_dependency_v1(artifact_id="operator_summary_dossier_v1", ref_row=operator_summary_dependency_ref),
        )
        if code
    ]
    declared_dependency_artifacts = [
        str(item).strip()
        for item in (contract.get("required_upstream_dependencies") or [])
        if str(item).strip()
    ]
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=_session_authority_closure_state_v1(
            payload=normalized,
            blocking_codes=[*(normalized.get("top_blocker_reason_codes") or []), *dependency_validation_codes],
            missing_dependency_artifacts=missing_dependency_artifacts,
        ),
        reason_codes=[*(normalized.get("top_blocker_reason_codes") or []), *dependency_validation_codes],
        first_blocker_code=str(normalized.get("first_real_blocker_code") or "").strip(),
        missing_dependency_artifacts=missing_dependency_artifacts,
    )
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type=SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY,
        declared_dependency_artifacts=declared_dependency_artifacts,
        dependency_refs=constitutional_dependency_refs,
    )
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type=SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY,
        producer_id="constellation_2.common.session_authority_monitor_v1",
        generated_at_utc=str(normalized.get("generated_utc") or "").strip(),
        effective_at_utc=str(normalized.get("generated_utc") or "").strip(),
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=constitutional_dependency_refs,
        policy_snapshot_refs=[],
        code_version="",
        run_id=(
            "session_authority_status:"
            f"{str(normalized.get('target_day') or normalized.get('active_day') or normalized.get('next_target_day') or '').strip()}:"
            f"{str(normalized.get('environment') or '').strip()}"
        ),
    )
    normalized.update(blocker_envelope)
    normalized["constitutional_dependency_declaration"] = constitutional_dependency_declaration
    normalized["constitutional_lineage"] = constitutional_lineage
    return normalized


def _status_comparison_day(payload: Mapping[str, Any]) -> str:
    for key in ("target_day", "blocked_target_day", "next_target_day", "active_day"):
        day_utc = str(payload.get(key) or "").strip()
        if day_utc:
            return day_utc
    return ""


def _status_current_matches_surface(
    *,
    status_payload: Mapping[str, Any],
    surface: CanonicalReadinessSurfaceV1,
) -> bool:
    current_authorized = bool(status_payload.get("submission_authorized") is True)
    current_status = str(status_payload.get("submission_authorization_status") or "UNKNOWN").strip().upper() or "UNKNOWN"
    if current_authorized != surface.submission_authorized:
        return False
    if surface.surface_name == "paper_session_authority_v1":
        projection = (
            status_payload.get("paper_authority_projection")
            if isinstance(status_payload.get("paper_authority_projection"), Mapping)
            else {}
        )
        projection_status = str(projection.get("authority_status") or "UNKNOWN").strip().upper() or "UNKNOWN"
        if projection_status != surface.status_value:
            return False
        if bool(projection.get("paper_open_allowed") is True) != bool(surface.paper_open_allowed is True):
            return False
        if bool(projection.get("degraded_mode") is True) != bool(surface.degraded_mode is True):
            return False
    if surface.surface_name == "submit_boundary_status_v1":
        if surface.submission_authorized and current_status != "AUTHORIZED":
            return False
        if not surface.submission_authorized and current_status == "AUTHORIZED":
            return False
    return True


def _canonical_surface_detail(surface: CanonicalReadinessSurfaceV1) -> Dict[str, Any]:
    return {
        "surface_name": surface.surface_name,
        "artifact_path": str(surface.path),
        "artifact_sha256": str(surface.sha256),
        "produced_utc": surface.produced_utc,
        "status_value": surface.status_value,
        "submission_authorized": surface.submission_authorized,
        "reason_codes": list(surface.reason_codes),
        "paper_open_allowed": surface.paper_open_allowed,
        "degraded_mode": surface.degraded_mode,
    }


def _read_canonical_readiness_surfaces_v1(
    *,
    truth_root: Path,
    day_utc: str,
    environment: str,
) -> List[CanonicalReadinessSurfaceV1]:
    if not str(day_utc or "").strip():
        return []

    surfaces: List[CanonicalReadinessSurfaceV1] = []
    if str(environment or "").strip().upper() == ENVIRONMENT_PAPER:
        try:
            paper_authority_ref = read_paper_session_authority_ref_v1(
                truth_root=truth_root,
                day_utc=day_utc,
            )
            paper_authority_payload = dict(paper_authority_ref.payload)
            surfaces.append(
                CanonicalReadinessSurfaceV1(
                    surface_name="paper_session_authority_v1",
                    path=paper_authority_ref.path,
                    sha256=paper_authority_ref.sha256,
                    produced_utc=str(paper_authority_payload.get("produced_utc") or "").strip(),
                    produced_dt=_parse_utc(str(paper_authority_payload.get("produced_utc") or "").strip()),
                    submission_authorized=bool(paper_authority_payload.get("submission_authorized") is True),
                    status_value=str(paper_authority_payload.get("authority_status") or "UNKNOWN").strip().upper() or "UNKNOWN",
                    reason_codes=tuple(
                        str(code).strip()
                        for code in (paper_authority_payload.get("blocking_reason_codes") or [])
                        if str(code).strip()
                    ),
                    paper_open_allowed=bool(paper_authority_payload.get("paper_open_allowed") is True),
                    degraded_mode=bool(paper_authority_payload.get("degraded_mode") is True),
                )
            )
        except Exception:
            pass
        return surfaces

    try:
        boundary_ref = read_submit_boundary_status_ref_v1(truth_root=truth_root, day_utc=day_utc)
        boundary_payload = dict(boundary_ref.payload)
        surfaces.append(
            CanonicalReadinessSurfaceV1(
                surface_name="submit_boundary_status_v1",
                path=boundary_ref.path,
                sha256=boundary_ref.sha256,
                produced_utc=str(boundary_payload.get("produced_at_utc") or "").strip(),
                produced_dt=_parse_utc(str(boundary_payload.get("produced_at_utc") or "").strip()),
                submission_authorized=bool(boundary_payload.get("submission_authorized") is True),
                status_value=str(boundary_payload.get("boundary_status") or "UNKNOWN").strip().upper() or "UNKNOWN",
                reason_codes=tuple(
                    str(code).strip()
                    for code in (boundary_payload.get("blocking_codes") or [])
                    if str(code).strip()
                ),
            )
        )
    except Exception:
        pass

    try:
        ledger_ref = read_paper_session_ledger_ref_v1(truth_root=truth_root, day_utc=day_utc)
        ledger_payload = dict(ledger_ref.payload)
        control_state = ledger_payload.get("control_state") or {}
        surfaces.append(
            CanonicalReadinessSurfaceV1(
                surface_name="paper_session_ledger_v1",
                path=ledger_ref.path,
                sha256=ledger_ref.sha256,
                produced_utc=str(ledger_payload.get("evaluated_at_utc") or "").strip(),
                produced_dt=_parse_utc(str(ledger_payload.get("evaluated_at_utc") or "").strip()),
                submission_authorized=bool(control_state.get("submission_authorized") is True),
                status_value=str(control_state.get("authority_status") or "UNKNOWN").strip().upper() or "UNKNOWN",
                reason_codes=tuple(
                    str(code).strip()
                    for code in (control_state.get("blocking_codes") or [])
                    if str(code).strip()
                ),
            )
        )
    except Exception:
        pass

    try:
        control_plane_ref = read_paper_day_control_plane_ref_v1(truth_root=truth_root, day_utc=day_utc)
        control_plane_payload = dict(control_plane_ref.payload)
        authority_result = control_plane_payload.get("authority_result") or {}
        final_start_decision = str(control_plane_payload.get("final_start_decision") or "UNKNOWN").strip().upper() or "UNKNOWN"
        ledger_authority_status = str(authority_result.get("ledger_authority_status") or "UNKNOWN").strip().upper() or "UNKNOWN"
        surfaces.append(
            CanonicalReadinessSurfaceV1(
                surface_name="paper_day_control_plane_v1",
                path=control_plane_ref.path,
                sha256=control_plane_ref.sha256,
                produced_utc=str(control_plane_payload.get("evaluated_at_utc") or "").strip(),
                produced_dt=_parse_utc(str(control_plane_payload.get("evaluated_at_utc") or "").strip()),
                submission_authorized=(final_start_decision == "READY_NOW" and ledger_authority_status == "GRANTED"),
                status_value=final_start_decision,
                reason_codes=tuple(
                    str(code).strip()
                    for code in (control_plane_payload.get("blocking_codes") or [])
                    if str(code).strip()
                ),
            )
        )
    except Exception:
        pass

    return surfaces


def _invalidate_session_authority_status_payload_v1(
    *,
    payload: Mapping[str, Any],
    reason_code: str,
    summary: str,
    comparison_day: str,
    surfaces: Iterable[CanonicalReadinessSurfaceV1],
    now: datetime | None,
    artifact_path_hint: str = "",
) -> Dict[str, Any]:
    updated = dict(payload)
    generated_utc = _utc_now(now)
    checks = [dict(row) for row in (updated.get("monitoring_checks") or []) if isinstance(row, Mapping)]
    top_reason_codes = [
        str(code).strip()
        for code in (updated.get("top_blocker_reason_codes") or [])
        if str(code).strip()
    ]
    if reason_code not in top_reason_codes:
        top_reason_codes.insert(0, reason_code)

    canonical_surfaces = list(surfaces)
    artifact_ref = (
        {
            "artifact_path": str(canonical_surfaces[0].path),
            "artifact_sha256": str(canonical_surfaces[0].sha256),
        }
        if canonical_surfaces
        else _blank_ref_dict(str(artifact_path_hint or ""))
    )
    checks.append(
        _check_row(
            check_name="canonical_readiness_authority",
            status="FAIL",
            severity=STATUS_SEVERITY_CRITICAL,
            reason_code=reason_code,
            summary=summary,
            artifact_ref=artifact_ref,
            details={
                "comparison_day": comparison_day,
                "canonical_surfaces": [_canonical_surface_detail(surface) for surface in canonical_surfaces],
            },
        )
    )

    updated["generated_utc"] = generated_utc
    updated["submission_authorization_status"] = "UNKNOWN"
    updated["submission_authorized"] = False
    updated["first_real_blocker_code"] = reason_code
    updated["first_real_blocker_summary"] = summary
    updated["top_blocker_reason_codes"] = top_reason_codes
    updated["status_severity"] = STATUS_SEVERITY_CRITICAL
    updated["monitoring_checks"] = checks
    updated["required_operator_action"] = _recommended_action(top_reason_codes, severity=STATUS_SEVERITY_CRITICAL)
    updated["recommended_operator_action"] = updated["required_operator_action"]
    return updated


def _enforce_session_authority_truth_v1(
    *,
    payload: Mapping[str, Any],
    truth_root: Path,
    now: datetime | None = None,
    invalidate_on_stale: bool,
) -> Dict[str, Any]:
    comparison_day = _status_comparison_day(payload)
    environment = str(payload.get("environment") or "").strip().upper()
    if not comparison_day:
        return dict(payload)

    surfaces = _read_canonical_readiness_surfaces_v1(
        truth_root=truth_root,
        day_utc=comparison_day,
        environment=environment,
    )
    if not surfaces:
        if environment == ENVIRONMENT_PAPER:
            authority_path = resolve_paper_session_authority_path(
                truth_root=truth_root,
                day_utc=comparison_day,
            )
            missing_reason_code = (
                BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_INVALID
                if authority_path.exists()
                else BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING
            )
            return _invalidate_session_authority_status_payload_v1(
                payload=payload,
                reason_code=missing_reason_code,
                summary=(
                    "Canonical paper_session_authority_v1 artifact is missing or invalid "
                    f"for day={comparison_day}."
                ),
                comparison_day=comparison_day,
                surfaces=[],
                now=now,
                artifact_path_hint=str(authority_path),
            )
        return dict(payload)

    latest_surface = max(
        surfaces,
        key=lambda surface: surface.produced_dt or datetime.min.replace(tzinfo=UTC),
    )
    latest_canonical_dt = latest_surface.produced_dt
    current_generated_dt = _parse_utc(str(payload.get("generated_utc") or "").strip())

    if invalidate_on_stale and current_generated_dt is not None and latest_canonical_dt is not None and latest_canonical_dt > current_generated_dt:
        return _invalidate_session_authority_status_payload_v1(
            payload=payload,
            reason_code=RC_SESSION_AUTHORITY_STATUS_STALE,
            summary=(
                "session_authority_status_v1/current.json is older than newer canonical readiness artifacts "
                f"for day={comparison_day}."
            ),
            comparison_day=comparison_day,
            surfaces=surfaces,
            now=now,
        )

    authorized_values = {surface.submission_authorized for surface in surfaces}
    if len(authorized_values) > 1:
        return _invalidate_session_authority_status_payload_v1(
            payload=payload,
            reason_code=RC_SESSION_AUTHORITY_CANONICAL_MISMATCH,
            summary=(
                "Canonical readiness artifacts disagree on submission authorization "
                f"for day={comparison_day}."
            ),
            comparison_day=comparison_day,
            surfaces=surfaces,
            now=now,
        )

    mismatched_surfaces = [
        surface
        for surface in surfaces
        if not _status_current_matches_surface(status_payload=payload, surface=surface)
    ]
    if mismatched_surfaces:
        return _invalidate_session_authority_status_payload_v1(
            payload=payload,
            reason_code=RC_SESSION_AUTHORITY_CANONICAL_MISMATCH,
            summary=(
                "session_authority_status_v1/current.json contradicts canonical readiness artifacts "
                f"for day={comparison_day}."
            ),
            comparison_day=comparison_day,
            surfaces=mismatched_surfaces,
            now=now,
        )

    updated = dict(payload)
    checks = [dict(row) for row in (updated.get("monitoring_checks") or []) if isinstance(row, Mapping)]
    checks.append(
        _check_row(
            check_name="canonical_readiness_authority",
            status="PASS",
            severity=STATUS_SEVERITY_INFO,
            reason_code="",
            summary="session_authority_status_v1 matches the latest canonical readiness artifacts for the same day.",
            artifact_ref={
                "artifact_path": str(surfaces[0].path),
                "artifact_sha256": str(surfaces[0].sha256),
            },
            details={
                "comparison_day": comparison_day,
                "canonical_surfaces": [_canonical_surface_detail(surface) for surface in surfaces],
            },
        )
    )
    updated["monitoring_checks"] = checks
    return updated


def resolve_session_authority_status_path(*, truth_root: Path) -> Path:
    return (Path(truth_root).resolve() / SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY / "current.json").resolve()


def resolve_session_authority_status_day_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY
        / parse_day_utc_v1(day_utc)
        / "session_authority_status.v1.json"
    ).resolve()


def resolve_session_authority_alert_path(*, truth_root: Path) -> Path:
    return (Path(truth_root).resolve() / SESSION_AUTHORITY_ALERT_ARTIFACT_FAMILY / "current.json").resolve()


def _status_history_day(payload: Mapping[str, Any]) -> str:
    comparison_day = str(_status_comparison_day(payload) or "").strip()
    if not comparison_day:
        return ""
    return parse_day_utc_v1(comparison_day)


def _session_authority_audit_fields(payload: Mapping[str, Any]) -> Dict[str, Any]:
    derived_from_values = (
        [
            str(item).strip()
            for item in (payload.get("derived_from") or [])
            if str(item).strip()
        ]
        if isinstance(payload.get("derived_from"), list)
        else []
    )
    if not derived_from_values:
        derived_from_values = _derived_from_for_environment(str(payload.get("environment") or ""))
    return {
        "is_canonical": False,
        "authority_level": AUTHORITY_LEVEL_DERIVED,
        "derived_from": derived_from_values,
    }


def _normalize_session_authority_status_semantics_v1(payload: Mapping[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)
    normalized_environment = str(normalized.get("environment") or "").strip().upper()
    projection_payload = (
        normalized.get("paper_authority_projection")
        if isinstance(normalized.get("paper_authority_projection"), Mapping)
        else {}
    )
    normalized_projection = _paper_authority_projection_defaults_v1(
        environment=normalized_environment,
        authority_artifact_path=str(
            (projection_payload.get("authority_artifact_ref") or {}).get("artifact_path")
            if isinstance(projection_payload.get("authority_artifact_ref"), Mapping)
            else ""
        ),
        authority_status=str(projection_payload.get("authority_status") or "UNKNOWN"),
        paper_open_allowed=bool(projection_payload.get("paper_open_allowed") is True),
        degraded_mode=bool(projection_payload.get("degraded_mode") is True),
        submission_authorized=bool(projection_payload.get("submission_authorized") is True),
        blocker_reason_codes=_ordered_codes(projection_payload.get("blocker_reason_codes") or []),
        advisory_reason_codes=_ordered_codes(projection_payload.get("advisory_reason_codes") or []),
        advisory_reason_details=[
            row
            for row in (projection_payload.get("advisory_reason_details") or [])
            if isinstance(row, Mapping)
        ],
        first_blocker_summary=str(projection_payload.get("first_blocker_summary") or ""),
    )
    normalized_projection["authority_artifact_ref"] = {
        "artifact_path": str(
            (projection_payload.get("authority_artifact_ref") or {}).get("artifact_path")
            if isinstance(projection_payload.get("authority_artifact_ref"), Mapping)
            else normalized_projection["authority_artifact_ref"]["artifact_path"]
        ),
        "artifact_sha256": str(
            (projection_payload.get("authority_artifact_ref") or {}).get("artifact_sha256")
            if isinstance(projection_payload.get("authority_artifact_ref"), Mapping)
            else normalized_projection["authority_artifact_ref"]["artifact_sha256"]
        ),
    }
    normalized["paper_authority_projection"] = normalized_projection
    submission_authorization_status = str(normalized.get("submission_authorization_status") or "UNKNOWN").strip().upper()
    submission_authorized = bool(normalized.get("submission_authorized") is True)
    target_day_admission_status = str(normalized.get("target_day_admission_status") or "").strip().upper()
    status_severity = str(normalized.get("status_severity") or "").strip().upper()
    has_fail_checks = any(
        str(row.get("status") or "").strip().upper() == "FAIL"
        for row in (normalized.get("monitoring_checks") or [])
        if isinstance(row, Mapping)
    )
    if submission_authorization_status == "AUTHORIZED" and submission_authorized:
        normalized["first_real_blocker_code"] = ""
        normalized["first_real_blocker_summary"] = ""
    if (
        submission_authorization_status == "AUTHORIZED"
        and submission_authorized
        and target_day_admission_status == "ADMIT"
        and status_severity == STATUS_SEVERITY_INFO
        and not has_fail_checks
    ):
        normalized["top_blocker_reason_codes"] = []
        if str(normalized.get("rollover_status") or "").strip().upper() != ROLLOVER_STATUS_WITHHELD:
            normalized["blocked_reason_codes"] = []
    normalized["semantic_status"] = _session_authority_semantic_status_v1(normalized)
    normalized.update(_session_authority_audit_fields(normalized))
    return normalized


def _session_authority_semantic_status_v1(payload: Mapping[str, Any]) -> str:
    top_reason_codes = {
        str(code).strip()
        for code in (payload.get("top_blocker_reason_codes") or [])
        if str(code).strip()
    }
    monitoring_reason_codes = {
        str(row.get("reason_code") or "").strip()
        for row in (payload.get("monitoring_checks") or [])
        if isinstance(row, Mapping) and str(row.get("reason_code") or "").strip()
    }
    reason_codes = top_reason_codes | monitoring_reason_codes
    submission_authorization_status = str(payload.get("submission_authorization_status") or "UNKNOWN").strip().upper()
    submission_authorized = bool(payload.get("submission_authorized") is True)
    target_day_admission_status = str(payload.get("target_day_admission_status") or "").strip().upper()
    target_day_build_status = str(payload.get("target_day_build_status") or "").strip().upper()
    status_severity = str(payload.get("status_severity") or "").strip().upper()
    rollover_status = str(payload.get("rollover_status") or "").strip().upper()
    traceability_status = str(payload.get("traceability_status") or "").strip().upper()
    hidden_dependency_check_result = str(payload.get("hidden_dependency_check_result") or "").strip().upper()
    ambiguity_state = payload.get("ambiguity_state") if isinstance(payload.get("ambiguity_state"), Mapping) else {}
    ambiguity_status = str(ambiguity_state.get("status") or "").strip().upper()
    has_fail_checks = any(
        str(row.get("status") or "").strip().upper() == "FAIL"
        for row in (payload.get("monitoring_checks") or [])
        if isinstance(row, Mapping)
    )
    required_facts_present = not bool(
        reason_codes & _SESSION_AUTHORITY_NOT_YET_MATERIALIZED_REASON_CODES
    )
    fully_observed_and_confirmed = (
        submission_authorization_status == "AUTHORIZED"
        and submission_authorized
        and target_day_admission_status == "ADMIT"
        and target_day_build_status == "COMPLETE"
        and traceability_status == "VALID"
        and hidden_dependency_check_result == "PASS"
        and ambiguity_status == "CLEAR"
        and status_severity == STATUS_SEVERITY_INFO
        and not has_fail_checks
    )
    blocked_by_upstream_prerequisite = bool(
        reason_codes & _SESSION_AUTHORITY_UPSTREAM_PREREQUISITE_REASON_CODES
    ) or (
        required_facts_present
        and (
            rollover_status == ROLLOVER_STATUS_WITHHELD
            or target_day_admission_status == "BLOCKED"
            or target_day_build_status == "BLOCKED"
        )
    )
    return classify_operator_semantic_status_v1(
        required_facts_present=required_facts_present,
        blocked_by_upstream_prerequisite=blocked_by_upstream_prerequisite,
        pending_propagation=False,
        materialized_failure=(
            required_facts_present
            and not blocked_by_upstream_prerequisite
            and not fully_observed_and_confirmed
        ),
        fully_observed_and_confirmed=fully_observed_and_confirmed,
    )


def read_session_authority_status_ref_v1(*, truth_root: Path) -> MonitorRefV1:
    ref = read_control_plane_surface_v1(
        domain="session",
        surface="session_authority_status_current",
        truth_root=Path(truth_root).resolve(),
    )
    enforced_payload = _enforce_session_authority_truth_v1(
        payload=ref.payload,
        truth_root=Path(truth_root).resolve(),
        invalidate_on_stale=True,
    )
    if enforced_payload != ref.payload:
        enforced_ref = write_session_authority_status_v1(
            truth_root=Path(truth_root).resolve(),
            payload=enforced_payload,
        )
        return enforced_ref
    return MonitorRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def read_session_authority_alert_ref_v1(*, truth_root: Path) -> MonitorRefV1:
    ref = read_control_plane_surface_v1(
        domain="session",
        surface="session_authority_alert_current",
        truth_root=Path(truth_root).resolve(),
    )
    return MonitorRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def _path_is_canonical(path: Path, truth_root: Path) -> bool:
    try:
        path.resolve().relative_to(Path(truth_root).resolve())
    except ValueError:
        return False
    return True


def _check_row(
    *,
    check_name: str,
    status: str,
    severity: str,
    reason_code: str,
    summary: str,
    artifact_ref: Mapping[str, Any] | None = None,
    details: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    return {
        "check_name": str(check_name).strip(),
        "status": str(status).strip().upper(),
        "severity": str(severity).strip().upper(),
        "reason_code": str(reason_code).strip(),
        "summary": str(summary).strip(),
        "artifact_ref": {
            "artifact_path": str((artifact_ref or {}).get("artifact_path") or ""),
            "artifact_sha256": str((artifact_ref or {}).get("artifact_sha256") or ""),
        },
        "details": dict(details or {}),
    }


def _collect_top_reason_codes(
    *,
    build_payload: Mapping[str, Any] | None,
    admission_payload: Mapping[str, Any] | None,
    active_payload: Mapping[str, Any] | None,
    coverage_payload: Mapping[str, Any] | None,
    checks: Iterable[Mapping[str, Any]],
) -> List[str]:
    codes: List[str] = []
    if isinstance(coverage_payload, Mapping):
        codes.extend(
            str(code).strip()
            for code in (coverage_payload.get("reason_codes") or [])
            if str(code).strip()
        )
    if isinstance(active_payload, Mapping):
        codes.extend(str(code).strip() for code in (active_payload.get("blocked_reason_codes") or []) if str(code).strip())
        rollover_reason_code = str(active_payload.get("rollover_reason_code") or "").strip()
        if rollover_reason_code:
            codes.append(rollover_reason_code)
    if isinstance(admission_payload, Mapping):
        codes.extend(str(code).strip() for code in (admission_payload.get("blocking_reason_codes") or []) if str(code).strip())
    if isinstance(build_payload, Mapping):
        codes.extend(
            str(row.get("blocker_code") or "").strip()
            for row in (build_payload.get("blocker_chain") or [])
            if isinstance(row, Mapping) and str(row.get("blocker_code") or "").strip()
        )
    for row in checks:
        code = str(row.get("reason_code") or "").strip()
        if code:
            codes.append(code)
    seen: set[str] = set()
    ordered: List[str] = []
    for code in codes:
        if code and code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


def _recommended_action(reason_codes: Iterable[str], *, severity: str) -> str:
    codes = [str(code).strip() for code in reason_codes if str(code).strip()]
    if severity == STATUS_SEVERITY_INFO:
        return "No operator action required."
    if REASON_SOURCE_NOT_EXTENDED in codes:
        return "Extend the governed market-calendar source dataset through the required target day, then rerun market-calendar coverage authority and Session Authority."
    if REASON_RUNTIME_NOT_REFRESHED in codes:
        return "Run the approved market-calendar coverage REFRESH path to ingest governed source rows into canonical runtime truth."
    if REASON_COVERAGE_BELOW_POLICY_BUFFER in codes:
        return "Restore forward market-calendar coverage buffer before the next rollover window."
    if REASON_SOURCE_AND_RUNTIME_RANGE_MISMATCH in codes:
        return "Inspect source versus runtime market-calendar ranges and refresh canonical runtime truth if the source is ahead."
    if REASON_MANIFEST_MISSING in codes:
        return "Restore the missing market-calendar manifest or rerun the approved market-calendar ingest flow."
    if RC_EXECUTION_ROOT_AUTHORITY_AMBIGUOUS in codes:
        return "Resolve execution-root governance so Phase C, authorization, readiness, and execution evidence agree on one canonical authority root."
    if RC_EXECUTION_ROOT_SLEEVE_ID_MISSING in codes:
        return "Restore the active PAPER sleeve binding so sleeve_execution_root_v1 can compute the canonical execution root."
    if RC_EXECUTION_ROOT_MODE_MISSING in codes:
        return "Restore the active PAPER sleeve mode so sleeve_execution_root_v1 can compute the canonical execution root."
    if RC_EXECUTION_ROOT_PATH_UNRESOLVED in codes:
        return "Materialize or restore the canonical sleeve execution root path before trusting execution readiness."
    if RC_EXECUTION_ROOT_PATH_MISMATCH in codes:
        return "Correct the runtime/submit path so it matches truth/sleeves/<sleeve_id>/<mode>/ before trusting execution readiness."
    if RC_EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN in codes:
        return "Remove the active global/shared execution-root reference and rerun subsystem authority materialization."
    if RC_EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS in codes:
        return "Resolve whether sleeve-registry gateway profile or runtime defaults are authoritative before trusting execution readiness."
    if RC_EXECUTION_IDENTITY_SLEEVE_MISSING in codes:
        return "Restore the governed sleeve_id so execution identity binding can resolve one canonical submit identity."
    if RC_EXECUTION_IDENTITY_ENVIRONMENT_MISSING in codes:
        return "Restore the governed environment binding so execution identity resolution can fail closed deterministically."
    if RC_EXECUTION_IDENTITY_SLEEVE_UNREGISTERED in codes:
        return "Register or reactivate the governed sleeve/environment binding before trusting any submit-capable runtime."
    if RC_EXECUTION_IDENTITY_ACCOUNT_MISSING in codes:
        return "Restore the governed IB account binding for the active sleeve before any submit-capable runtime is trusted."
    if RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING in codes:
        return "Restore the governed orders client ID for the active sleeve before any submit-capable runtime is trusted."
    if RC_EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS in codes:
        return "Resolve duplicate governed account candidates so one sleeve/environment binds to exactly one IB account."
    if RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS in codes:
        return "Resolve duplicate governed orders client IDs so one sleeve/environment binds to exactly one submit client."
    if RC_EXECUTION_IDENTITY_ACCOUNT_MISMATCH in codes:
        return "Correct the runtime submit account so it exactly matches the governed sleeve execution identity."
    if RC_EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH in codes:
        return "Correct the runtime orders client ID so it exactly matches the governed sleeve execution identity."
    if RC_EXECUTION_IDENTITY_FORBIDDEN_COMBINATION in codes:
        return "Remove the forbidden sleeve/account/client combination and restore the governed execution identity binding."
    if RC_ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS in codes:
        return "Resolve whether engine, account, or sleeve governance owns trading policy before trusting submission eligibility."
    if "STALE_ARTIFACT" in codes:
        return "Regenerate stale canonical target-day artifacts, then rerun Session Authority."
    if RC_SESSION_AUTHORITY_STATUS_STALE in codes:
        return "Regenerate session_authority_status_v1/current.json because newer canonical readiness artifacts exist for the same target day."
    if RC_SESSION_AUTHORITY_CANONICAL_MISMATCH in codes:
        return "Inspect canonical readiness artifacts for the same target day (including paper_session_authority_v1 for PAPER mode), then rerun Session Authority after the contradiction is resolved."
    if BLOCKING_CODE_INTENT_INPUT_CONVERGENCE_BLOCKED in codes:
        return "Run the owned PAPER startup intent-input convergence step and resolve the first missing sleeve-side input before rerunning startup authorization convergence."
    if BLOCKING_CODE_STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_BLOCKED in codes:
        return "Run the owned startup-materialization input convergence step and resolve the first missing canonical startup input before rerunning startup materialization."
    if BLOCKING_CODE_READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED in codes:
        return "Repair the readiness bootstrap runtime interpreter or ibapi environment, then rerun session readiness refresh."
    if "HIDDEN_DEPENDENCY_DETECTED" in codes:
        return "Resolve undeclared execution dependencies before trusting the blocked target day."
    if "WRONG_AUTHORITY_PATH" in codes:
        return "Correct authority-path drift before allowing activation."
    if BLOCKING_CODE_TRACEABILITY_BROKEN in codes:
        return "Repair active-session traceability before trusting activation state."
    if "TARGET_DAY_ARTIFACT_MISSING" in codes or BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING in codes:
        return "Materialize the missing canonical target-day artifacts and rerun Session Authority."
    return "Inspect the binding build and admission artifacts, then rerun Session Authority after the blocker is resolved."


def _severity_from_reason_codes(
    *,
    reason_codes: Iterable[str],
    active_day: str,
    admission_status: str,
    rollover_status: str,
    coverage_severity: str = STATUS_SEVERITY_INFO,
) -> str:
    codes = {str(code).strip() for code in reason_codes if str(code).strip()}
    coverage_level = str(coverage_severity or STATUS_SEVERITY_INFO).strip().upper()
    if coverage_level == STATUS_SEVERITY_CRITICAL:
        return STATUS_SEVERITY_CRITICAL
    if not active_day and (codes or str(admission_status).strip().upper() != "ADMIT"):
        return STATUS_SEVERITY_CRITICAL
    if codes & _CRITICAL_REASON_CODES:
        return STATUS_SEVERITY_CRITICAL
    if str(rollover_status).strip().upper() == ROLLOVER_STATUS_WITHHELD:
        return STATUS_SEVERITY_WARNING if active_day else STATUS_SEVERITY_CRITICAL
    if str(admission_status).strip().upper() != "ADMIT":
        return STATUS_SEVERITY_WARNING
    if coverage_level == STATUS_SEVERITY_WARNING:
        return STATUS_SEVERITY_WARNING
    if codes & _WARNING_REASON_CODES:
        return STATUS_SEVERITY_WARNING
    return STATUS_SEVERITY_INFO


def _withheld_seconds(*, generated_utc: str, now: datetime | None) -> int:
    started = _parse_utc(generated_utc)
    if started is None:
        return 0
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return max(0, int((current.astimezone(UTC) - started).total_seconds()))


def _validate_ref_path(
    *,
    label: str,
    path_text: str,
    truth_root: Path,
    checks: List[Dict[str, Any]],
) -> Path | None:
    path = Path(str(path_text or "").strip()).resolve() if str(path_text or "").strip() else None
    if path is None:
        checks.append(
            _check_row(
                check_name=f"{label}_ref_present",
                status="FAIL",
                severity=STATUS_SEVERITY_CRITICAL,
                reason_code=BLOCKING_CODE_TRACEABILITY_BROKEN,
                summary=f"{label} reference is missing from active_session_v1.",
                artifact_ref=_blank_ref_dict(),
            )
        )
        return None
    if not _path_is_canonical(path, truth_root):
        checks.append(
            _check_row(
                check_name=f"{label}_ref_canonical",
                status="FAIL",
                severity=STATUS_SEVERITY_CRITICAL,
                reason_code="WRONG_AUTHORITY_PATH",
                summary=f"{label} reference is not under the canonical truth root.",
                artifact_ref=_blank_ref_dict(str(path)),
            )
        )
        return None
    return path


def build_session_authority_status_payload_v1(
    *,
    truth_root: Path,
    environment: str,
    now: datetime | None = None,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(
        Path(__file__).resolve().parents[2],
        SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY,
        "constellation_2.common.session_authority_monitor_v1",
    )
    root = Path(truth_root).resolve()
    normalized_environment = str(environment).strip().upper()
    generated_utc = _utc_now(now)
    active_path = resolve_active_session_path(truth_root=root)
    checks: List[Dict[str, Any]] = []

    active_ref: SurfaceRefV1 | None = None
    admission_ref: SurfaceRefV1 | None = None
    build_ref: SurfaceRefV1 | None = None

    active_day = ""
    next_target_day = ""
    target_day = ""
    rollover_status = ""
    rollover_reason_code = ""
    blocked_target_day = ""
    blocked_reason_codes: List[str] = []
    target_day_admission_status = ""
    target_day_build_status = ""
    closure_status = ""
    hidden_dependency_check_status = ""
    traceability_status = "BROKEN"
    active_session_generated_utc = ""
    target_day_admission_generated_utc = ""
    target_day_build_generated_utc = ""
    market_calendar_coverage_status = "UNKNOWN"
    market_calendar_coverage_severity = STATUS_SEVERITY_INFO
    market_calendar_required_target_day = ""
    market_calendar_warning_target_day = ""
    market_calendar_source_coverage_end = ""
    market_calendar_runtime_coverage_end = ""
    market_calendar_source_status = "UNKNOWN"
    market_calendar_runtime_status = "UNKNOWN"
    market_calendar_source_covers_required_target_day = False
    market_calendar_runtime_covers_required_target_day = False
    market_calendar_operator_action_code = "NONE"
    market_calendar_coverage_ref = _blank_ref_dict(str(resolve_market_calendar_coverage_status_path(truth_root=root)))
    coverage_payload: Dict[str, Any] | None = None
    submission_authorization_status = "UNKNOWN"
    submission_authorized = False
    first_real_blocker_code = ""
    first_real_blocker_summary = ""
    operator_summary_authority_owner = (
        "paper_session_authority_v1"
        if normalized_environment == ENVIRONMENT_PAPER
        else "session_authority_status_v1"
    )
    operator_summary_authority_manifest_path = resolve_subsystem_authority_manifest_path(
        truth_root=root,
        subsystem_id=SUBSYSTEM_ID_OPERATOR_SUMMARY_AUTHORITY,
    )
    operator_summary_authority_manifest_ref = _artifact_ref_from_path(operator_summary_authority_manifest_path)
    operator_summary_dossier_ref = _blank_ref_dict()
    advisory_only_signals: List[Dict[str, Any]] = []
    ambiguity_state: Dict[str, Any] = {
        "status": "CLEAR",
        "summary": "",
        "reason_codes": [],
        "conflicting_authorities": [],
    }
    required_operator_action = ""
    paper_authority_projection = _paper_authority_projection_defaults_v1(
        environment=normalized_environment
    )

    try:
        active_ref = read_active_session_ref_v1(truth_root=root)
        active_payload = dict(active_ref.payload)
        active_day = str(active_payload.get("active_day") or "").strip()
        next_target_day = str(active_payload.get("next_target_day") or "").strip()
        target_day = str(active_payload.get("target_day") or "").strip()
        rollover_status = str(active_payload.get("rollover_status") or "").strip()
        rollover_reason_code = str(active_payload.get("rollover_reason_code") or "").strip()
        blocked_target_day = str(active_payload.get("blocked_target_day") or "").strip()
        blocked_reason_codes = [
            str(code).strip()
            for code in (active_payload.get("blocked_reason_codes") or [])
            if str(code).strip()
        ]
        target_day_admission_status = str(active_payload.get("target_day_admission_status") or "").strip().upper()
        active_session_generated_utc = str(active_payload.get("generated_utc") or "").strip()
        checks.append(
            _check_row(
                check_name="active_session_readable",
                status="PASS",
                severity=STATUS_SEVERITY_INFO,
                reason_code="",
                summary="active_session_v1/current.json is structurally readable.",
                artifact_ref=_ref_dict_from_surface(active_ref),
                details={"generated_utc": active_session_generated_utc},
            )
        )
    except Exception as exc:
        checks.append(
            _check_row(
                check_name="active_session_readable",
                status="FAIL",
                severity=STATUS_SEVERITY_CRITICAL,
                reason_code=BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_INVALID if active_path.exists() else BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING,
                summary=f"active_session_v1/current.json is unavailable or invalid: {type(exc).__name__}.",
                artifact_ref=_blank_ref_dict(str(active_path)),
            )
        )
        top_codes = _collect_top_reason_codes(
            build_payload=None,
            admission_payload=None,
            active_payload=None,
            coverage_payload=None,
            checks=checks,
        )
        severity = _severity_from_reason_codes(
            reason_codes=top_codes,
            active_day="",
            admission_status="BLOCKED",
            rollover_status=ROLLOVER_STATUS_WITHHELD,
            coverage_severity=STATUS_SEVERITY_INFO,
        )
        return _finalize_session_authority_payload_v1(
            contract=contract,
            payload={
            "schema_id": "session_authority_status",
            "schema_version": "v1",
            "generated_utc": generated_utc,
                "environment": normalized_environment,
            "target_day": "",
            "active_day": "",
            "next_target_day": "",
            "blocked_target_day": "",
            "rollover_status": "UNAVAILABLE",
            "rollover_reason_code": BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING,
            "target_day_admission_status": "BLOCKED",
            "target_day_build_status": "BLOCKED",
            "closure_status": "OPEN",
            "hidden_dependency_check_result": "FAIL",
            "traceability_status": "BROKEN",
            "active_session_generated_utc": "",
            "target_day_admission_generated_utc": "",
            "target_day_build_generated_utc": "",
            "active_session_ref": _blank_ref_dict(str(active_path)),
            "target_day_admission_ref": _blank_ref_dict(),
            "target_day_build_ref": _blank_ref_dict(),
            "market_calendar_coverage_status": market_calendar_coverage_status,
            "market_calendar_coverage_severity": market_calendar_coverage_severity,
            "market_calendar_required_target_day": market_calendar_required_target_day,
            "market_calendar_warning_target_day": market_calendar_warning_target_day,
            "market_calendar_source_coverage_end": market_calendar_source_coverage_end,
            "market_calendar_runtime_coverage_end": market_calendar_runtime_coverage_end,
            "market_calendar_source_status": market_calendar_source_status,
            "market_calendar_runtime_status": market_calendar_runtime_status,
            "market_calendar_source_covers_required_target_day": market_calendar_source_covers_required_target_day,
            "market_calendar_runtime_covers_required_target_day": market_calendar_runtime_covers_required_target_day,
            "market_calendar_operator_action_code": market_calendar_operator_action_code,
            "market_calendar_coverage_ref": market_calendar_coverage_ref,
            "top_blocker_reason_codes": top_codes,
            "blocked_reason_codes": [],
            "rollover_withheld_seconds": 0,
            "submission_authorization_status": submission_authorization_status,
            "submission_authorized": submission_authorized,
            "first_real_blocker_code": first_real_blocker_code,
            "first_real_blocker_summary": first_real_blocker_summary,
                "operator_summary_authority_owner": operator_summary_authority_owner,
                "operator_summary_authority_manifest_ref": operator_summary_authority_manifest_ref,
                "operator_summary_dossier_ref": operator_summary_dossier_ref,
                "paper_authority_projection": paper_authority_projection,
                "advisory_only_signals": advisory_only_signals,
                "ambiguity_state": ambiguity_state,
                "status_severity": severity,
                "monitoring_checks": checks,
                "required_operator_action": _recommended_action(top_codes, severity=severity),
                "recommended_operator_action": _recommended_action(top_codes, severity=severity),
                "is_canonical": False,
                "authority_level": AUTHORITY_LEVEL_DERIVED,
                "derived_from": _derived_from_for_environment(normalized_environment),
        })

    active_payload = dict(active_ref.payload)
    admission_path = _validate_ref_path(
        label="target_day_admission",
        path_text=str(active_payload.get("target_day_admission_ref") or ""),
        truth_root=root,
        checks=checks,
    )
    build_ref_payload = active_payload.get("target_day_build_ref")
    build_path_text = ""
    build_sha_expected = ""
    if isinstance(build_ref_payload, Mapping):
        build_path_text = str(build_ref_payload.get("artifact_path") or "").strip()
        build_sha_expected = str(build_ref_payload.get("artifact_sha256") or "").strip()
    build_path = _validate_ref_path(
        label="target_day_build",
        path_text=build_path_text,
        truth_root=root,
        checks=checks,
    )

    if admission_path is not None:
        try:
            admission_ref = read_target_day_admission_ref_v1(truth_root=root, target_day=str(target_day or blocked_target_day or next_target_day or active_day or "").strip())
            target_day_admission_generated_utc = str(admission_ref.payload.get("generated_utc") or "").strip()
            target_day_admission_status = str(admission_ref.payload.get("admission_status") or "").strip().upper()
            checks.append(
                _check_row(
                    check_name="target_day_admission_readable",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="target_day_admission_v1 reference is structurally readable.",
                    artifact_ref=_ref_dict_from_surface(admission_ref),
                    details={"generated_utc": target_day_admission_generated_utc},
                )
            )
        except Exception as exc:
            checks.append(
                _check_row(
                    check_name="target_day_admission_readable",
                    status="FAIL",
                    severity=STATUS_SEVERITY_CRITICAL,
                    reason_code=BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_INVALID if admission_path.exists() else BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING,
                    summary=f"target_day_admission_v1 reference is unavailable or invalid: {type(exc).__name__}.",
                    artifact_ref=_blank_ref_dict(str(admission_path)),
                )
            )

    if build_path is not None:
        try:
            build_ref = read_target_day_build_ref_v1(truth_root=root, target_day=str(target_day or blocked_target_day or next_target_day or active_day or "").strip())
            target_day_build_generated_utc = str(build_ref.payload.get("generated_utc") or "").strip()
            target_day_build_status = str(build_ref.payload.get("build_status") or "").strip().upper()
            closure_status = str(build_ref.payload.get("closure_status") or "").strip().upper()
            hidden_dependency_check_status = str(
                (build_ref.payload.get("hidden_dependency_check_result") or {}).get("status") or ""
            ).strip().upper()
            checks.append(
                _check_row(
                    check_name="target_day_build_readable",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="target_day_build_v1 reference is structurally readable.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                    details={"generated_utc": target_day_build_generated_utc},
                )
            )
            actual_sha = str(build_ref.sha256)
            if build_sha_expected and actual_sha != build_sha_expected:
                checks.append(
                    _check_row(
                        check_name="target_day_build_ref_matches_active_session",
                        status="FAIL",
                        severity=STATUS_SEVERITY_CRITICAL,
                        reason_code=BLOCKING_CODE_TRACEABILITY_BROKEN,
                        summary="active_session_v1 build reference sha256 does not match the referenced build artifact.",
                        artifact_ref=_ref_dict_from_surface(build_ref),
                        details={"expected_sha256": build_sha_expected, "observed_sha256": actual_sha},
                    )
                )
            else:
                checks.append(
                    _check_row(
                        check_name="target_day_build_ref_matches_active_session",
                        status="PASS",
                        severity=STATUS_SEVERITY_INFO,
                        reason_code="",
                        summary="active_session_v1 build reference matches the referenced build artifact.",
                        artifact_ref=_ref_dict_from_surface(build_ref),
                    )
                )
        except Exception as exc:
            checks.append(
                _check_row(
                    check_name="target_day_build_readable",
                    status="FAIL",
                    severity=STATUS_SEVERITY_CRITICAL,
                    reason_code=BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_INVALID if build_path.exists() else BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING,
                    summary=f"target_day_build_v1 reference is unavailable or invalid: {type(exc).__name__}.",
                    artifact_ref=_blank_ref_dict(str(build_path)),
                )
            )

    if admission_ref is not None and build_ref is not None:
        admission_build_ref = admission_ref.payload.get("build_ref")
        build_path_from_admission = ""
        if isinstance(admission_build_ref, Mapping):
            build_path_from_admission = str(admission_build_ref.get("artifact_path") or "").strip()
        if build_path_from_admission and build_path_from_admission != str(build_ref.path):
            checks.append(
                _check_row(
                    check_name="admission_build_chain_consistent",
                    status="FAIL",
                    severity=STATUS_SEVERITY_CRITICAL,
                    reason_code=BLOCKING_CODE_TRACEABILITY_BROKEN,
                    summary="target_day_admission_v1 does not point to the build artifact referenced by active_session_v1.",
                    artifact_ref=_ref_dict_from_surface(admission_ref),
                    details={"admission_build_path": build_path_from_admission, "active_build_path": str(build_ref.path)},
                )
            )
        else:
            checks.append(
                _check_row(
                    check_name="admission_build_chain_consistent",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="active_session_v1, target_day_admission_v1, and target_day_build_v1 form a valid traceability chain.",
                    artifact_ref=_ref_dict_from_surface(admission_ref),
                )
            )
            traceability_status = "VALID"

    coverage_target_day = next_target_day or blocked_target_day or target_day or active_day
    try:
        coverage_ref = read_market_calendar_coverage_status_ref_v1(truth_root=root)
        coverage_payload = dict(coverage_ref.payload)
        market_calendar_coverage_ref = _ref_dict_from_surface(coverage_ref)
        checks.append(
            _check_row(
                check_name="market_calendar_coverage_status_readable",
                status="PASS",
                severity=STATUS_SEVERITY_INFO,
                reason_code="",
                summary="market_calendar_coverage_status_v1 is structurally readable.",
                artifact_ref=market_calendar_coverage_ref,
                details={"required_target_day": str(coverage_payload.get("required_target_day") or "").strip()},
            )
        )
    except Exception:
        coverage_payload = build_market_calendar_coverage_status_payload_v1(
            truth_root=root,
            required_target_day=coverage_target_day or None,
            now=now,
        )
        checks.append(
            _check_row(
                check_name="market_calendar_coverage_status_readable",
                status="PASS",
                severity=STATUS_SEVERITY_WARNING if str(coverage_payload.get("severity") or "").strip().upper() != STATUS_SEVERITY_INFO else STATUS_SEVERITY_INFO,
                reason_code=str((coverage_payload.get("reason_codes") or [""])[0] or "").strip(),
                summary="market_calendar_coverage_status_v1 was derived read-only because no persisted coverage status artifact was available.",
                artifact_ref=market_calendar_coverage_ref,
                details={"required_target_day": str(coverage_payload.get("required_target_day") or "").strip()},
            )
        )
    market_calendar_coverage_status = str(coverage_payload.get("coverage_status") or "UNKNOWN").strip().upper()
    market_calendar_coverage_severity = str(coverage_payload.get("severity") or STATUS_SEVERITY_INFO).strip().upper()
    market_calendar_required_target_day = str(coverage_payload.get("required_target_day") or "").strip()
    market_calendar_warning_target_day = str(coverage_payload.get("warning_target_day") or "").strip()
    market_calendar_source_coverage_end = str(coverage_payload.get("source_coverage_end") or "").strip()
    market_calendar_runtime_coverage_end = str(coverage_payload.get("runtime_coverage_end") or "").strip()
    market_calendar_source_status = str(coverage_payload.get("source_status") or "UNKNOWN").strip().upper()
    market_calendar_runtime_status = str(coverage_payload.get("runtime_status") or "UNKNOWN").strip().upper()
    market_calendar_source_covers_required_target_day = bool(coverage_payload.get("source_required_target_day_covered"))
    market_calendar_runtime_covers_required_target_day = bool(coverage_payload.get("runtime_required_target_day_covered"))
    market_calendar_operator_action_code = str(coverage_payload.get("operator_action_code") or "NONE").strip().upper()
    coverage_reason_codes = [
        str(code).strip()
        for code in (coverage_payload.get("reason_codes") or [])
        if str(code).strip()
    ]
    if market_calendar_coverage_status == COVERAGE_STATUS_BLOCKED:
        checks.append(
            _check_row(
                check_name="market_calendar_forward_coverage",
                status="FAIL",
                severity=market_calendar_coverage_severity,
                reason_code=coverage_reason_codes[0] if coverage_reason_codes else REASON_SOURCE_NOT_EXTENDED,
                summary="Market-calendar forward coverage is blocking target-day readiness.",
                artifact_ref=market_calendar_coverage_ref,
                details={
                    "required_target_day": market_calendar_required_target_day,
                    "warning_target_day": market_calendar_warning_target_day,
                    "source_coverage_end": market_calendar_source_coverage_end,
                    "runtime_coverage_end": market_calendar_runtime_coverage_end,
                    "source_status": market_calendar_source_status,
                    "runtime_status": market_calendar_runtime_status,
                    "source_required_target_day_covered": market_calendar_source_covers_required_target_day,
                    "runtime_required_target_day_covered": market_calendar_runtime_covers_required_target_day,
                    "operator_action_code": market_calendar_operator_action_code,
                    "reason_codes": coverage_reason_codes,
                },
            )
        )
    elif market_calendar_coverage_status == COVERAGE_STATUS_WARNING:
        checks.append(
            _check_row(
                check_name="market_calendar_forward_coverage",
                status="FAIL",
                severity=market_calendar_coverage_severity,
                reason_code=coverage_reason_codes[0] if coverage_reason_codes else REASON_COVERAGE_BELOW_POLICY_BUFFER,
                summary="Market-calendar forward coverage is below the governed policy buffer.",
                artifact_ref=market_calendar_coverage_ref,
                details={
                    "required_target_day": market_calendar_required_target_day,
                    "warning_target_day": market_calendar_warning_target_day,
                    "source_coverage_end": market_calendar_source_coverage_end,
                    "runtime_coverage_end": market_calendar_runtime_coverage_end,
                    "source_status": market_calendar_source_status,
                    "runtime_status": market_calendar_runtime_status,
                    "source_required_target_day_covered": market_calendar_source_covers_required_target_day,
                    "runtime_required_target_day_covered": market_calendar_runtime_covers_required_target_day,
                    "operator_action_code": market_calendar_operator_action_code,
                    "reason_codes": coverage_reason_codes,
                },
            )
        )
    else:
        checks.append(
            _check_row(
                check_name="market_calendar_forward_coverage",
                status="PASS",
                severity=STATUS_SEVERITY_INFO,
                reason_code="",
                summary="Market-calendar forward coverage satisfies the governed policy buffer.",
                artifact_ref=market_calendar_coverage_ref,
                details={
                    "required_target_day": market_calendar_required_target_day,
                    "warning_target_day": market_calendar_warning_target_day,
                    "source_coverage_end": market_calendar_source_coverage_end,
                    "runtime_coverage_end": market_calendar_runtime_coverage_end,
                    "source_status": market_calendar_source_status,
                    "runtime_status": market_calendar_runtime_status,
                    "source_required_target_day_covered": market_calendar_source_covers_required_target_day,
                    "runtime_required_target_day_covered": market_calendar_runtime_covers_required_target_day,
                    "operator_action_code": market_calendar_operator_action_code,
                },
            )
        )

    if build_ref is not None:
        build_payload = dict(build_ref.payload)
        closure_status = str(build_payload.get("closure_status") or "").strip().upper()
        hidden_dependency_check_status = str(
            (build_payload.get("hidden_dependency_check_result") or {}).get("status") or ""
        ).strip().upper()
        target_day_build_status = str(build_payload.get("build_status") or "").strip().upper()
        required_rows = [
            row
            for row in (build_payload.get("artifact_results") or [])
            if isinstance(row, Mapping) and bool(row.get("required", True))
        ]
        by_artifact_id = {
            str(row.get("artifact_id") or "").strip(): row
            for row in required_rows
            if str(row.get("artifact_id") or "").strip()
        }
        stale_rows = [row for row in required_rows if str(row.get("freshness_status") or "").strip().upper() != "CURRENT"]
        provenance_missing_rows = [
            row
            for row in required_rows
            if bool((row.get("provenance_summary") or {}).get("required", False))
            and not bool((row.get("provenance_summary") or {}).get("present", False))
        ]
        source_not_extended_rows = [
            row
            for row in required_rows
            if str(row.get("blocking_reason_code") or "").strip() == REASON_SOURCE_NOT_EXTENDED
        ]
        if stale_rows:
            checks.append(
                _check_row(
                    check_name="required_artifact_freshness",
                    status="FAIL",
                    severity=STATUS_SEVERITY_WARNING,
                    reason_code="STALE_ARTIFACT",
                    summary="One or more required target-day artifacts are stale.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                    details={"artifacts": [str(row.get("artifact_id") or "") for row in stale_rows]},
                )
            )
        else:
            checks.append(
                _check_row(
                    check_name="required_artifact_freshness",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="Required target-day artifacts satisfy freshness checks.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                )
            )
        if provenance_missing_rows:
            checks.append(
                _check_row(
                    check_name="required_artifact_provenance",
                    status="FAIL",
                    severity=STATUS_SEVERITY_WARNING,
                    reason_code="PROVENANCE_MISSING",
                    summary="One or more required target-day artifacts are missing required provenance.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                    details={"artifacts": [str(row.get("artifact_id") or "") for row in provenance_missing_rows]},
                )
            )
        else:
            checks.append(
                _check_row(
                    check_name="required_artifact_provenance",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="Required target-day artifacts carry required provenance.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                )
            )
        if source_not_extended_rows:
            checks.append(
                _check_row(
                    check_name="source_extension_state",
                    status="FAIL",
                    severity=STATUS_SEVERITY_WARNING,
                    reason_code=REASON_SOURCE_NOT_EXTENDED,
                    summary="Canonical source coverage does not yet extend to the blocked target day.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                    details={"artifacts": [str(row.get("artifact_id") or "") for row in source_not_extended_rows]},
                )
            )
        else:
            checks.append(
                _check_row(
                    check_name="source_extension_state",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="Canonical source coverage extends to the current target day requirements.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                )
            )
        intent_convergence_row = by_artifact_id.get("paper_startup_intent_input_convergence_v1")
        if isinstance(intent_convergence_row, Mapping) and str(intent_convergence_row.get("result_status") or "").strip().upper() != "PASS":
            checks.append(
                _check_row(
                    check_name="paper_startup_intent_input_convergence",
                    status="FAIL",
                    severity=STATUS_SEVERITY_WARNING,
                    reason_code=BLOCKING_CODE_INTENT_INPUT_CONVERGENCE_BLOCKED,
                    summary="PAPER startup intent-input convergence is blocked before intent generation.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                    details={"artifact_id": "paper_startup_intent_input_convergence_v1"},
                )
            )
        elif isinstance(intent_convergence_row, Mapping):
            checks.append(
                _check_row(
                    check_name="paper_startup_intent_input_convergence",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="PAPER startup intent-input convergence succeeded.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                )
            )
        startup_input_convergence_row = by_artifact_id.get("startup_materialization_input_convergence_v1")
        if isinstance(startup_input_convergence_row, Mapping) and str(startup_input_convergence_row.get("result_status") or "").strip().upper() != "PASS":
            checks.append(
                _check_row(
                    check_name="startup_materialization_input_convergence",
                    status="FAIL",
                    severity=STATUS_SEVERITY_WARNING,
                    reason_code=BLOCKING_CODE_STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_BLOCKED,
                    summary="Canonical startup-materialization input convergence is blocked.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                    details={"artifact_id": "startup_materialization_input_convergence_v1"},
                )
            )
        elif isinstance(startup_input_convergence_row, Mapping):
            checks.append(
                _check_row(
                    check_name="startup_materialization_input_convergence",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="Canonical startup-materialization input convergence succeeded.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                )
            )
        readiness_source_ref = None
        for row in (build_payload.get("source_refs") or []):
            if isinstance(row, Mapping) and str(row.get("script") or "").strip() == "ops/tools/run_session_readiness_refresh_v1.py":
                readiness_source_ref = row
                break
        if isinstance(readiness_source_ref, Mapping):
            stdout_text = str(readiness_source_ref.get("stdout") or "").strip()
            try:
                readiness_payload = json.loads(stdout_text) if stdout_text else {}
            except json.JSONDecodeError:
                readiness_payload = {}
            readiness_results = readiness_payload.get("results") if isinstance(readiness_payload, Mapping) else {}
            broker_bootstrap = readiness_results.get("broker_events_bootstrap") if isinstance(readiness_results, Mapping) else {}
            broker_reason_codes = [
                str(code).strip()
                for code in (broker_bootstrap.get("reason_codes") or [])
                if str(code).strip()
            ] if isinstance(broker_bootstrap, Mapping) else []
            if BLOCKING_CODE_READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED in broker_reason_codes:
                checks.append(
                    _check_row(
                        check_name="readiness_bootstrap_runtime",
                        status="FAIL",
                        severity=STATUS_SEVERITY_CRITICAL,
                        reason_code=BLOCKING_CODE_READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED,
                        summary="Readiness bootstrap could not import ibapi under the selected runtime interpreter.",
                        artifact_ref=_ref_dict_from_surface(build_ref),
                        details={"reason_codes": broker_reason_codes},
                    )
                )
            elif isinstance(broker_bootstrap, Mapping) and int(broker_bootstrap.get("returncode") or 0) == 0:
                checks.append(
                    _check_row(
                        check_name="readiness_bootstrap_runtime",
                        status="PASS",
                        severity=STATUS_SEVERITY_INFO,
                        reason_code="",
                        summary="Readiness bootstrap runtime imported broker dependencies successfully.",
                        artifact_ref=_ref_dict_from_surface(build_ref),
                    )
                )
        if hidden_dependency_check_status != "PASS":
            checks.append(
                _check_row(
                    check_name="hidden_dependency_check",
                    status="FAIL",
                    severity=STATUS_SEVERITY_CRITICAL,
                    reason_code=str((build_payload.get("hidden_dependency_check_result") or {}).get("blocking_reason_code") or "HIDDEN_DEPENDENCY_DETECTED").strip(),
                    summary="target_day_build_v1 detected undeclared execution dependencies or partial build state.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                    details=dict(build_payload.get("hidden_dependency_check_result") or {}),
                )
            )
        else:
            checks.append(
                _check_row(
                    check_name="hidden_dependency_check",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="target_day_build_v1 hidden dependency check passed.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                )
            )
        if closure_status != "CLOSED":
            checks.append(
                _check_row(
                    check_name="build_closure_status",
                    status="FAIL",
                    severity=STATUS_SEVERITY_WARNING,
                    reason_code="ADMISSION_RULE_BLOCKED",
                    summary="target_day_build_v1 is not closed for activation.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                    details={
                        "build_status": target_day_build_status,
                        "closure_status": closure_status,
                        "hidden_dependency_check_result": hidden_dependency_check_status,
                    },
                )
            )
        else:
            checks.append(
                _check_row(
                    check_name="build_closure_status",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="target_day_build_v1 closure requirements are satisfied.",
                    artifact_ref=_ref_dict_from_surface(build_ref),
                )
            )

    if admission_ref is not None:
        admission_payload = dict(admission_ref.payload)
        blocking_reason_codes = [
            str(code).strip()
            for code in (admission_payload.get("blocking_reason_codes") or [])
            if str(code).strip()
        ]
        if target_day_admission_status != "ADMIT":
            checks.append(
                _check_row(
                    check_name="target_day_admission_state",
                    status="FAIL",
                    severity=STATUS_SEVERITY_WARNING,
                    reason_code=blocking_reason_codes[0] if blocking_reason_codes else "ADMISSION_RULE_BLOCKED",
                    summary="target_day_admission_v1 is blocking activation.",
                    artifact_ref=_ref_dict_from_surface(admission_ref),
                    details={"blocking_reason_codes": blocking_reason_codes},
                )
            )
        else:
            checks.append(
                _check_row(
                    check_name="target_day_admission_state",
                    status="PASS",
                    severity=STATUS_SEVERITY_INFO,
                    reason_code="",
                    summary="target_day_admission_v1 admits the target day.",
                    artifact_ref=_ref_dict_from_surface(admission_ref),
                )
            )

    if rollover_status.upper() == ROLLOVER_STATUS_WITHHELD:
        checks.append(
            _check_row(
                check_name="rollover_state",
                status="FAIL",
                severity=STATUS_SEVERITY_WARNING if active_day else STATUS_SEVERITY_CRITICAL,
                reason_code=rollover_reason_code or BLOCKING_CODE_ROLLOVER_WITHHELD,
                summary="Session Authority is withholding rollover to the blocked target day.",
                artifact_ref=_ref_dict_from_surface(active_ref),
                details={"blocked_target_day": blocked_target_day, "blocked_reason_codes": blocked_reason_codes},
            )
        )
    else:
        checks.append(
            _check_row(
                check_name="rollover_state",
                status="PASS",
                severity=STATUS_SEVERITY_INFO,
                reason_code="",
                summary="Session Authority rollover state is healthy.",
                artifact_ref=_ref_dict_from_surface(active_ref),
            )
        )

    summary_day = (
        blocked_target_day
        or active_day
        or target_day
        or next_target_day
        or resolve_session_authority_target_day_v1()
    )
    paper_authority_projection = _build_paper_authority_projection_v1(
        truth_root=root,
        day_utc=summary_day,
        environment=normalized_environment,
        checks=checks,
    )
    operator_summary_dossier_path = resolve_operator_summary_dossier_path(truth_root=root, day_utc=summary_day)
    operator_summary_dossier = build_operator_summary_dossier_v1(
        repo_root=Path(__file__).resolve().parents[2],
        truth_root=root,
        day_utc=summary_day,
        environment=normalized_environment,
    )
    operator_summary_state = str(operator_summary_dossier.get("current_state") or "").strip().upper()
    operator_summary_first_blocker_code = str((operator_summary_dossier.get("first_blocker") or {}).get("reason_code") or "").strip()
    operator_summary_first_blocker_summary = str((operator_summary_dossier.get("first_blocker") or {}).get("summary") or "").strip()
    if normalized_environment == ENVIRONMENT_PAPER:
        submission_authorized = bool(paper_authority_projection.get("submission_authorized") is True)
        submission_authorization_status = "AUTHORIZED" if submission_authorized else "DENIED"
        first_real_blocker_code = str(paper_authority_projection.get("first_blocker_code") or "").strip()
        first_real_blocker_summary = str(paper_authority_projection.get("first_blocker_summary") or "").strip()
        if not first_real_blocker_code:
            first_real_blocker_code = operator_summary_first_blocker_code
            first_real_blocker_summary = operator_summary_first_blocker_summary
    else:
        submission_authorization_status = str(
            operator_summary_dossier.get("submission_authorization_status") or "UNKNOWN"
        ).strip().upper() or "UNKNOWN"
        submission_authorized = bool(operator_summary_dossier.get("submission_authorized") is True)
        first_real_blocker_code = operator_summary_first_blocker_code
        first_real_blocker_summary = operator_summary_first_blocker_summary
    advisory_only_signals = [dict(row) for row in (operator_summary_dossier.get("advisory_only_signals") or []) if isinstance(row, dict)]
    ambiguity_state = dict(operator_summary_dossier.get("ambiguity_state") or ambiguity_state)
    required_operator_action = (
        ""
        if normalized_environment == ENVIRONMENT_PAPER
        else str(operator_summary_dossier.get("recommended_operator_action") or "").strip()
    )
    operator_summary_dossier_ref = _artifact_ref_from_path(operator_summary_dossier_path)
    if operator_summary_state == "BLOCKED":
        ambiguity_codes = [
            str(code).strip()
            for code in (ambiguity_state.get("reason_codes") or [])
            if str(code).strip()
        ]
        checks.append(
            _check_row(
                check_name="operator_summary_authority_plane",
                status="FAIL",
                severity=STATUS_SEVERITY_WARNING
                if normalized_environment == ENVIRONMENT_PAPER
                else STATUS_SEVERITY_CRITICAL,
                reason_code=first_real_blocker_code or (ambiguity_codes[0] if ambiguity_codes else RC_EXECUTION_ROOT_AUTHORITY_AMBIGUOUS),
                summary=(
                    "Subsystem authority ambiguity is recorded as advisory context; PAPER open authority comes from canonical paper_session_authority_v1."
                    if str(ambiguity_state.get("status") or "").strip() == "AMBIGUOUS_AUTHORITY"
                    else "A subsystem authority condition is recorded as advisory context; PAPER open authority comes from canonical paper_session_authority_v1."
                ),
                artifact_ref=operator_summary_authority_manifest_ref,
                details={
                    "operator_summary_day": summary_day,
                    "operator_summary_state": operator_summary_state,
                    "ambiguity_state": ambiguity_state,
                    "operator_summary_dossier_ref": operator_summary_dossier_ref,
                },
            )
        )
    else:
        checks.append(
            _check_row(
                check_name="operator_summary_authority_plane",
                status="PASS",
                severity=STATUS_SEVERITY_INFO,
                reason_code="",
                summary="Operator-summary precedence and subsystem authority checks are internally consistent.",
                artifact_ref=operator_summary_authority_manifest_ref,
                details={"operator_summary_day": summary_day, "submission_authorization_status": submission_authorization_status},
            )
        )
    if submission_authorization_status != "AUTHORIZED":
        checks.append(
            _check_row(
                check_name="submission_authorization_state",
                status="FAIL",
                severity=(
                    STATUS_SEVERITY_CRITICAL
                    if normalized_environment == ENVIRONMENT_PAPER
                    and not bool(paper_authority_projection.get("paper_open_allowed") is True)
                    else STATUS_SEVERITY_WARNING
                ),
                reason_code=first_real_blocker_code or "SUBMISSION_AUTHORIZATION_NOT_GRANTED",
                summary="Submit-boundary authorization is not currently granted.",
                artifact_ref=operator_summary_dossier_ref,
                details={
                    "submission_authorization_status": submission_authorization_status,
                    "paper_authority_projection": dict(paper_authority_projection),
                },
            )
        )
    else:
        checks.append(
            _check_row(
                check_name="submission_authorization_state",
                status="PASS",
                severity=STATUS_SEVERITY_INFO,
                reason_code="",
                summary="Submit-boundary authorization is currently granted.",
                artifact_ref=operator_summary_dossier_ref,
                details={"submission_authorization_status": submission_authorization_status},
            )
        )

    top_reason_codes = _collect_top_reason_codes(
        build_payload=build_ref.payload if build_ref is not None else None,
        admission_payload=admission_ref.payload if admission_ref is not None else None,
        active_payload=active_ref.payload if active_ref is not None else None,
        coverage_payload=coverage_payload,
        checks=checks,
    )
    for code in [
        first_real_blocker_code,
        *[str(item).strip() for item in (ambiguity_state.get("reason_codes") or []) if str(item).strip()],
    ]:
        if code and code not in top_reason_codes:
            top_reason_codes.append(code)
    if (
        normalized_environment == ENVIRONMENT_PAPER
        and not top_reason_codes
        and bool(paper_authority_projection.get("degraded_mode") is True)
    ):
        advisory_reason_code = str(paper_authority_projection.get("first_advisory_code") or "").strip()
        if advisory_reason_code:
            top_reason_codes.append(advisory_reason_code)
    if any(
        str(row.get("reason_code") or "").strip()
        in {
            BLOCKING_CODE_TRACEABILITY_BROKEN,
            BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_MISSING,
            BLOCKING_CODE_SESSION_AUTHORITY_ARTIFACT_INVALID,
            "WRONG_AUTHORITY_PATH",
        }
        for row in checks
    ):
        traceability_status = "BROKEN"
    severity = _severity_from_reason_codes(
        reason_codes=top_reason_codes,
        active_day=active_day,
        admission_status=target_day_admission_status or "BLOCKED",
        rollover_status=rollover_status,
        coverage_severity=market_calendar_coverage_severity,
    )
    if normalized_environment == ENVIRONMENT_PAPER:
        if not bool(paper_authority_projection.get("paper_open_allowed") is True):
            severity = STATUS_SEVERITY_CRITICAL
        elif bool(paper_authority_projection.get("degraded_mode") is True) and severity == STATUS_SEVERITY_INFO:
            severity = STATUS_SEVERITY_WARNING
    payload = {
        "schema_id": "session_authority_status",
        "schema_version": "v1",
        "generated_utc": generated_utc,
        "environment": normalized_environment,
        "target_day": target_day,
        "active_day": active_day,
        "next_target_day": next_target_day,
        "blocked_target_day": blocked_target_day,
        "rollover_status": rollover_status or "UNKNOWN",
        "rollover_reason_code": rollover_reason_code,
        "target_day_admission_status": target_day_admission_status or "BLOCKED",
        "target_day_build_status": target_day_build_status or "BLOCKED",
        "closure_status": closure_status or "OPEN",
        "hidden_dependency_check_result": hidden_dependency_check_status or "FAIL",
        "traceability_status": traceability_status,
        "active_session_generated_utc": active_session_generated_utc,
        "target_day_admission_generated_utc": target_day_admission_generated_utc,
        "target_day_build_generated_utc": target_day_build_generated_utc,
        "active_session_ref": _ref_dict_from_surface(active_ref),
        "target_day_admission_ref": _ref_dict_from_surface(admission_ref) if admission_ref is not None else _blank_ref_dict(str(admission_path or "")),
        "target_day_build_ref": _ref_dict_from_surface(build_ref) if build_ref is not None else _blank_ref_dict(str(build_path or "")),
        "market_calendar_coverage_status": market_calendar_coverage_status,
        "market_calendar_coverage_severity": market_calendar_coverage_severity,
        "market_calendar_required_target_day": market_calendar_required_target_day,
        "market_calendar_warning_target_day": market_calendar_warning_target_day,
        "market_calendar_source_coverage_end": market_calendar_source_coverage_end,
        "market_calendar_runtime_coverage_end": market_calendar_runtime_coverage_end,
        "market_calendar_source_status": market_calendar_source_status,
        "market_calendar_runtime_status": market_calendar_runtime_status,
        "market_calendar_source_covers_required_target_day": market_calendar_source_covers_required_target_day,
        "market_calendar_runtime_covers_required_target_day": market_calendar_runtime_covers_required_target_day,
        "market_calendar_operator_action_code": market_calendar_operator_action_code,
        "market_calendar_coverage_ref": market_calendar_coverage_ref,
        "top_blocker_reason_codes": top_reason_codes,
        "blocked_reason_codes": blocked_reason_codes,
        "rollover_withheld_seconds": _withheld_seconds(generated_utc=active_session_generated_utc, now=now)
        if rollover_status.upper() == ROLLOVER_STATUS_WITHHELD
        else 0,
        "submission_authorization_status": submission_authorization_status,
        "submission_authorized": submission_authorized,
        "first_real_blocker_code": first_real_blocker_code,
        "first_real_blocker_summary": first_real_blocker_summary,
        "operator_summary_authority_owner": operator_summary_authority_owner,
        "operator_summary_authority_manifest_ref": operator_summary_authority_manifest_ref,
        "operator_summary_dossier_ref": operator_summary_dossier_ref,
        "paper_authority_projection": paper_authority_projection,
        "advisory_only_signals": advisory_only_signals,
        "ambiguity_state": ambiguity_state,
        "semantic_status": "",
        "status_severity": severity,
        "monitoring_checks": checks,
        "required_operator_action": required_operator_action or _recommended_action(top_reason_codes, severity=severity),
        "recommended_operator_action": required_operator_action or _recommended_action(top_reason_codes, severity=severity),
        "is_canonical": False,
        "authority_level": AUTHORITY_LEVEL_DERIVED,
        "derived_from": _derived_from_for_environment(normalized_environment),
    }
    enforced = _enforce_session_authority_truth_v1(
        payload=payload,
        truth_root=root,
        now=now,
        invalidate_on_stale=False,
    )
    return _finalize_session_authority_payload_v1(contract=contract, payload=enforced)


def write_session_authority_status_v1(*, truth_root: Path, payload: Mapping[str, Any]) -> MonitorRefV1:
    root = Path(truth_root).resolve()
    contract = assert_constitutional_writer_allowed_v1(
        Path(__file__).resolve().parents[2],
        SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY,
        "constellation_2.common.session_authority_monitor_v1",
    )
    normalized_payload = _normalize_session_authority_status_semantics_v1(payload)
    finalized_payload = _finalize_session_authority_payload_v1(
        contract=contract,
        payload=normalized_payload,
    )
    validate_governed_artifact_payload_v1(
        repo_root=Path(__file__).resolve().parents[2],
        artifact_id=SESSION_AUTHORITY_STATUS_ARTIFACT_FAMILY,
        payload=finalized_payload,
        required_finality_states=["provisional", "finalized", "corrected"],
    )
    history_day = _status_history_day(finalized_payload)
    if history_day:
        atomic_write_validated_json_v1(
            path=resolve_session_authority_status_day_path(truth_root=root, day_utc=history_day),
            payload=dict(finalized_payload),
            schema_relpath=SESSION_AUTHORITY_STATUS_SCHEMA_RELPATH,
        )
    ref = atomic_write_validated_json_v1(
        path=resolve_session_authority_status_path(truth_root=root),
        payload=dict(finalized_payload),
        schema_relpath=SESSION_AUTHORITY_STATUS_SCHEMA_RELPATH,
    )
    return MonitorRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def derive_session_authority_alert_payload_v1(
    *,
    truth_root: Path,
    environment: str,
    status_ref: MonitorRefV1,
    prior_alert_ref: MonitorRefV1 | None = None,
) -> Dict[str, Any]:
    status_payload = dict(status_ref.payload)
    severity = str(status_payload.get("status_severity") or STATUS_SEVERITY_CRITICAL).strip().upper()
    reason_codes = [
        str(code).strip()
        for code in (status_payload.get("top_blocker_reason_codes") or [])
        if str(code).strip()
    ]
    blocked_target_day = str(status_payload.get("blocked_target_day") or "").strip()
    active_day = str(status_payload.get("active_day") or "").strip()
    target_day = str(status_payload.get("target_day") or "").strip()
    rollover_status = str(status_payload.get("rollover_status") or "").strip()
    alert_seed = {
        "severity": severity,
        "blocked_target_day": blocked_target_day,
        "reason_codes": reason_codes,
        "rollover_status": rollover_status,
        "target_day_admission_status": str(status_payload.get("target_day_admission_status") or "").strip(),
        "traceability_status": str(status_payload.get("traceability_status") or "").strip(),
        "market_calendar_coverage_status": str(status_payload.get("market_calendar_coverage_status") or "").strip(),
        "market_calendar_required_target_day": str(status_payload.get("market_calendar_required_target_day") or "").strip(),
    }
    dedupe_key = hashlib.sha256(
        (json.dumps(alert_seed, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    ).hexdigest()
    prior_payload = dict(prior_alert_ref.payload) if prior_alert_ref is not None else {}
    prior_status = str(prior_payload.get("alert_status") or "").strip().upper()
    prior_dedupe_key = str(prior_payload.get("dedupe_key") or "").strip()
    generated_utc = _utc_now()
    target_day = str(status_payload.get("target_day") or "").strip() or active_day or blocked_target_day
    prior_target_day = (
        str(prior_payload.get("target_day") or "").strip()
        or str(prior_payload.get("active_day") or "").strip()
        or str(prior_payload.get("blocked_target_day") or "").strip()
    )
    same_warning_occurrence = bool(prior_dedupe_key and prior_dedupe_key == dedupe_key and prior_target_day == target_day)
    prior_repeat_count = int(prior_payload.get("repeat_count") or 0) if same_warning_occurrence else 0
    repeat_count = prior_repeat_count + 1 if severity != STATUS_SEVERITY_INFO else 0
    first_seen_utc = (
        str(prior_payload.get("first_seen_utc") or "").strip() if same_warning_occurrence and prior_repeat_count > 0 else generated_utc
    ) if repeat_count > 0 else ""
    last_seen_utc = generated_utc if repeat_count > 0 else ""

    if severity == STATUS_SEVERITY_INFO:
        if prior_status in {ALERT_STATUS_ALERT, ALERT_STATUS_DEDUPED, ALERT_STATUS_ESCALATED} and prior_dedupe_key:
            alert_status = ALERT_STATUS_CLEAR
            recovery_of_dedupe_key = prior_dedupe_key
        else:
            alert_status = ALERT_STATUS_HEALTHY
            recovery_of_dedupe_key = ""
    elif repeat_count >= 3:
        severity = STATUS_SEVERITY_ERROR
        alert_status = ALERT_STATUS_ESCALATED
        recovery_of_dedupe_key = ""
    elif same_warning_occurrence:
        alert_status = ALERT_STATUS_DEDUPED
        recovery_of_dedupe_key = ""
    else:
        alert_status = ALERT_STATUS_ALERT
        recovery_of_dedupe_key = ""

    summary = (
        f"Session Authority {alert_status}: "
        f"severity={severity} active_day={active_day or 'NONE'} "
        f"target_day={target_day or 'NONE'} "
        f"blocked_target_day={blocked_target_day or 'NONE'} "
        f"rollover_status={rollover_status or 'UNKNOWN'} "
        f"reason_codes={','.join(reason_codes) if reason_codes else 'NONE'} "
        f"repeat_count={repeat_count}"
    )
    return {
        "schema_id": "session_authority_alert",
        "schema_version": "v1",
        "generated_utc": generated_utc,
        "alert_status": alert_status,
        "severity": severity,
        "environment": str(environment).strip().upper(),
        "active_day": active_day,
        "target_day": target_day,
        "blocked_target_day": blocked_target_day,
        "alert_reason_codes": reason_codes,
        "alert_summary": summary,
        "recommended_operator_action": str(status_payload.get("recommended_operator_action") or "").strip(),
        "dedupe_key": dedupe_key,
        "previous_dedupe_key": prior_dedupe_key,
        "recovery_of_dedupe_key": recovery_of_dedupe_key,
        "repeat_count": repeat_count,
        "first_seen_utc": first_seen_utc,
        "last_seen_utc": last_seen_utc,
        "source_status_ref": _ref_dict_from_surface(status_ref),
        "source_active_session_ref": dict(status_payload.get("active_session_ref") or _blank_ref_dict()),
        "source_admission_ref": dict(status_payload.get("target_day_admission_ref") or _blank_ref_dict()),
        "source_build_ref": dict(status_payload.get("target_day_build_ref") or _blank_ref_dict()),
        "source_market_calendar_coverage_ref": dict(status_payload.get("market_calendar_coverage_ref") or _blank_ref_dict()),
    }


def write_session_authority_alert_v1(*, truth_root: Path, payload: Mapping[str, Any]) -> MonitorRefV1:
    ref = atomic_write_validated_json_v1(
        path=resolve_session_authority_alert_path(truth_root=truth_root),
        payload=dict(payload),
        schema_relpath=SESSION_AUTHORITY_ALERT_SCHEMA_RELPATH,
    )
    return MonitorRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def render_session_authority_status_summary_v1(payload: Mapping[str, Any]) -> str:
    paper_projection = (
        payload.get("paper_authority_projection")
        if isinstance(payload.get("paper_authority_projection"), Mapping)
        else {}
    )
    return (
        "SESSION_AUTHORITY_STATUS "
        f"semantic_status={str(payload.get('semantic_status') or '').strip()} "
        f"severity={str(payload.get('status_severity') or '').strip()} "
        f"environment={str(payload.get('environment') or '').strip()} "
        f"active_day={str(payload.get('active_day') or 'NONE').strip() or 'NONE'} "
        f"next_target_day={str(payload.get('next_target_day') or 'NONE').strip() or 'NONE'} "
        f"rollover_status={str(payload.get('rollover_status') or 'UNKNOWN').strip() or 'UNKNOWN'} "
        f"rollover_reason_code={str(payload.get('rollover_reason_code') or 'NONE').strip() or 'NONE'} "
        f"admission={str(payload.get('target_day_admission_status') or 'UNKNOWN').strip() or 'UNKNOWN'} "
        f"build={str(payload.get('target_day_build_status') or 'UNKNOWN').strip() or 'UNKNOWN'} "
        f"closure={str(payload.get('closure_status') or 'UNKNOWN').strip() or 'UNKNOWN'} "
        f"hidden_dependency={str(payload.get('hidden_dependency_check_result') or 'UNKNOWN').strip() or 'UNKNOWN'} "
        f"market_calendar_coverage={str(payload.get('market_calendar_coverage_status') or 'UNKNOWN').strip() or 'UNKNOWN'} "
        f"market_calendar_required_target_day={str(payload.get('market_calendar_required_target_day') or 'NONE').strip() or 'NONE'} "
        f"market_calendar_warning_target_day={str(payload.get('market_calendar_warning_target_day') or 'NONE').strip() or 'NONE'} "
        f"market_calendar_source_status={str(payload.get('market_calendar_source_status') or 'UNKNOWN').strip() or 'UNKNOWN'} "
        f"market_calendar_runtime_status={str(payload.get('market_calendar_runtime_status') or 'UNKNOWN').strip() or 'UNKNOWN'} "
        f"market_calendar_operator_action_code={str(payload.get('market_calendar_operator_action_code') or 'NONE').strip() or 'NONE'} "
        f"paper_authority_status={str(paper_projection.get('authority_status') or 'UNKNOWN').strip() or 'UNKNOWN'} "
        f"paper_open_allowed={bool(paper_projection.get('paper_open_allowed') is True)} "
        f"paper_degraded_mode={bool(paper_projection.get('degraded_mode') is True)} "
        f"paper_submission_authorized={bool(paper_projection.get('submission_authorized') is True)} "
        f"paper_blockers={','.join(paper_projection.get('blocker_reason_codes') or []) or 'NONE'} "
        f"paper_advisories={','.join(paper_projection.get('advisory_reason_codes') or []) or 'NONE'} "
        f"blockers={','.join(payload.get('top_blocker_reason_codes') or []) or 'NONE'} "
        f"action={json.dumps(str(payload.get('recommended_operator_action') or '').strip())}"
    )


def render_session_authority_alert_summary_v1(payload: Mapping[str, Any]) -> str:
    return (
        "SESSION_AUTHORITY_ALERT "
        f"alert_status={str(payload.get('alert_status') or '').strip()} "
        f"severity={str(payload.get('severity') or '').strip()} "
        f"target_day={str(payload.get('target_day') or 'NONE').strip() or 'NONE'} "
        f"active_day={str(payload.get('active_day') or 'NONE').strip() or 'NONE'} "
        f"blocked_target_day={str(payload.get('blocked_target_day') or 'NONE').strip() or 'NONE'} "
        f"reason_codes={','.join(payload.get('alert_reason_codes') or []) or 'NONE'} "
        f"repeat_count={int(payload.get('repeat_count') or 0)} "
        f"first_seen_utc={str(payload.get('first_seen_utc') or 'NONE').strip() or 'NONE'} "
        f"last_seen_utc={str(payload.get('last_seen_utc') or 'NONE').strip() or 'NONE'} "
        f"dedupe_key={str(payload.get('dedupe_key') or '').strip()} "
        f"action={json.dumps(str(payload.get('recommended_operator_action') or '').strip())}"
    )
