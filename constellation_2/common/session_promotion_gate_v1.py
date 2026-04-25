from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from constellation_2.common.attempt_history_v1 import (
    build_attempt_id_v1,
    resolve_day_attempt_artifact_path_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_idempotent_validated_json_v1,
    atomic_write_validated_json_v1,
    read_validated_surface_v1,
)


SESSION_PROMOTION_DECISION_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/session_promotion_decision.v1.schema.json"
)
SESSION_PROMOTION_DECISION_FAMILY = "session_promotion_decision_v1"
SESSION_PROMOTION_DECISION_FILENAME = "session_promotion_decision.v1.json"
PROMOTION_STATE_PROMOTED = "PROMOTED"
PROMOTION_STATE_BLOCKED = "BLOCKED"
PROMOTION_STATE_FAILED_VALIDATION = "FAILED_VALIDATION"


@dataclass(frozen=True)
class SessionPromotionDecisionRefV1:
    path: Path
    payload: Dict[str, Any]
    sha256: str


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_day(day_utc: str) -> str:
    return date.fromisoformat(str(day_utc or "").strip()).isoformat()


def _artifact_ref_from_surface(path: Path | None, sha256: str = "") -> Dict[str, str]:
    return {
        "artifact_path": str(path) if path is not None else "",
        "artifact_sha256": str(sha256 or "").strip(),
    }


def _normalize_codes(values: Iterable[str]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def resolve_session_promotion_decision_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / SESSION_PROMOTION_DECISION_FAMILY
        / str(day_utc).strip()
        / SESSION_PROMOTION_DECISION_FILENAME
    ).resolve()


def resolve_session_promotion_decision_attempt_path_v1(
    *,
    truth_root: Path,
    day_utc: str,
    attempt_id: str,
) -> Path:
    family_root = (Path(truth_root).resolve() / "reports" / SESSION_PROMOTION_DECISION_FAMILY).resolve()
    return resolve_day_attempt_artifact_path_v1(
        family_root=family_root,
        day_utc=day_utc,
        attempt_id=attempt_id,
        filename=SESSION_PROMOTION_DECISION_FILENAME,
    )


def read_session_promotion_decision_ref_v1(
    *,
    truth_root: Path,
    target_day: str,
) -> SessionPromotionDecisionRefV1:
    path = resolve_session_promotion_decision_path_v1(truth_root=truth_root, day_utc=target_day)
    ref = read_validated_surface_v1(path=path, schema_relpath=SESSION_PROMOTION_DECISION_SCHEMA_RELPATH)
    return SessionPromotionDecisionRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def derive_session_promotion_decision_payload_v1(
    *,
    truth_root: Path,
    target_day: str,
    pre_open_bundle_ref: Mapping[str, Any] | None,
    target_day_admission_ref: Mapping[str, Any] | None,
    prior_active_session_ref: Mapping[str, Any] | None = None,
    owner_tool: str,
    allow_pre_open_incomplete_if_admitted: bool = False,
    rules_version: str = "session_promotion_gate_v1",
) -> Dict[str, Any]:
    normalized_target_day = _normalize_day(target_day)
    candidate_artifacts = [
        {
            "artifact_id": "active_session_v1",
            "artifact_path": str((Path(truth_root).resolve() / "active_session_v1" / "current.json").resolve()),
        }
    ]
    promoted_artifacts: list[dict[str, str]] = []
    blocked_reason_codes: list[str] = []
    validation_failures: list[str] = []

    pre_open_payload = dict(getattr(pre_open_bundle_ref, "payload", {}) or {})
    pre_open_path = getattr(pre_open_bundle_ref, "path", None)
    pre_open_sha256 = getattr(pre_open_bundle_ref, "sha256", "")
    admission_payload = dict(getattr(target_day_admission_ref, "payload", {}) or {})
    admission_path = getattr(target_day_admission_ref, "path", None)
    admission_sha256 = getattr(target_day_admission_ref, "sha256", "")
    prior_payload = dict(getattr(prior_active_session_ref, "payload", {}) or {})
    prior_path = getattr(prior_active_session_ref, "path", None)
    prior_sha256 = getattr(prior_active_session_ref, "sha256", "")

    pre_open_materialization_state = str(pre_open_payload.get("materialization_state") or "").strip().upper()
    pre_open_completion_state = str(pre_open_payload.get("completion_state") or "").strip().upper()
    admission_status = str(admission_payload.get("admission_status") or "").strip().upper()

    enforce_pre_open_gate = not bool(allow_pre_open_incomplete_if_admitted)

    if enforce_pre_open_gate:
        if not pre_open_payload:
            validation_failures.append("PRE_OPEN_BUNDLE_UNAVAILABLE")
        elif str(pre_open_payload.get("target_day") or "").strip() != normalized_target_day:
            validation_failures.append("PRE_OPEN_BUNDLE_TARGET_DAY_MISMATCH")

    if not admission_payload:
        validation_failures.append("TARGET_DAY_ADMISSION_UNAVAILABLE")
    elif str(admission_payload.get("target_day") or "").strip() != normalized_target_day:
        validation_failures.append("TARGET_DAY_ADMISSION_TARGET_DAY_MISMATCH")

    if validation_failures:
        promotion_state = PROMOTION_STATE_FAILED_VALIDATION
        blocked_reason_codes.extend(validation_failures)
    else:
        if enforce_pre_open_gate and (pre_open_materialization_state != "COMPLETE" or pre_open_completion_state != "COMPLETE"):
            blocked_reason_codes.extend(pre_open_payload.get("blocking_reason_codes") or [])
            if not blocked_reason_codes:
                blocked_reason_codes.append("PRE_OPEN_BUNDLE_INCOMPLETE")
        if admission_status != "ADMIT":
            blocked_reason_codes.extend(admission_payload.get("blocking_reason_codes") or [])
            if not (admission_payload.get("blocking_reason_codes") or []):
                blocked_reason_codes.append("TARGET_DAY_ADMISSION_BLOCKED")
        if blocked_reason_codes:
            promotion_state = PROMOTION_STATE_BLOCKED
        else:
            promotion_state = PROMOTION_STATE_PROMOTED
            promoted_artifacts = list(candidate_artifacts)

    return {
        "schema_id": "session_promotion_decision",
        "schema_version": "v1",
        "decided_at_utc": _utc_now(),
        "target_day": normalized_target_day,
        "owner_tool": str(owner_tool).strip(),
        "rules_version": str(rules_version).strip() or "session_promotion_gate_v1",
        "pre_open_bundle_ref": _artifact_ref_from_surface(pre_open_path, pre_open_sha256),
        "target_day_admission_ref": _artifact_ref_from_surface(admission_path, admission_sha256),
        "prior_current_state": {
            "active_session_ref": _artifact_ref_from_surface(prior_path, prior_sha256),
            "active_day": str(prior_payload.get("active_day") or "").strip(),
            "target_day_admission_status": str(prior_payload.get("target_day_admission_status") or "").strip().upper(),
        },
        "pre_open_materialization_state": pre_open_materialization_state,
        "pre_open_completion_state": pre_open_completion_state,
        "target_day_admission_status": admission_status,
        "promotion_state": promotion_state,
        "candidate_artifacts": candidate_artifacts,
        "promoted_artifacts": promoted_artifacts,
        "blocked_reason_codes": _normalize_codes(blocked_reason_codes),
    }


def write_session_promotion_decision_v1(
    *,
    truth_root: Path,
    payload: Dict[str, Any],
) -> SessionPromotionDecisionRefV1:
    target_day = str(payload.get("target_day") or "").strip()
    attempt_id = build_attempt_id_v1(payload=payload)
    atomic_write_validated_json_v1(
        path=resolve_session_promotion_decision_attempt_path_v1(
            truth_root=truth_root,
            day_utc=target_day,
            attempt_id=attempt_id,
        ),
        payload=payload,
        schema_relpath=SESSION_PROMOTION_DECISION_SCHEMA_RELPATH,
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_session_promotion_decision_path_v1(
            truth_root=truth_root,
            day_utc=target_day,
        ),
        payload=payload,
        schema_relpath=SESSION_PROMOTION_DECISION_SCHEMA_RELPATH,
        volatile_field_names=("decided_at_utc",),
    )
    return SessionPromotionDecisionRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)
