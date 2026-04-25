from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    read_validated_surface_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_session_authority_path,
)


SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_authority.v1.schema.json"


def _normalize_codes(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return sorted({str(item).strip() for item in values if str(item).strip()})


def _validate_semantics_v1(payload: Mapping[str, Any]) -> None:
    authority_status = str(payload.get("authority_status") or "").strip().upper()
    paper_open_allowed = bool(payload.get("paper_open_allowed") is True)
    degraded_mode = bool(payload.get("degraded_mode") is True)
    blocking_reason_codes = _normalize_codes(payload.get("blocking_reason_codes"))
    advisory_checks = payload.get("advisory_checks") if isinstance(payload.get("advisory_checks"), list) else []
    safety_checks = payload.get("safety_checks") if isinstance(payload.get("safety_checks"), list) else []

    if authority_status not in {"GRANTED", "DENIED"}:
        raise ValueError("PAPER_SESSION_AUTHORITY_INVALID_STATUS")
    if paper_open_allowed != (authority_status == "GRANTED"):
        raise ValueError("PAPER_SESSION_AUTHORITY_OPEN_ALLOWED_STATUS_MISMATCH")
    if authority_status == "GRANTED" and blocking_reason_codes:
        raise ValueError("PAPER_SESSION_AUTHORITY_GRANTED_WITH_BLOCKERS")
    if authority_status == "DENIED" and not blocking_reason_codes:
        raise ValueError("PAPER_SESSION_AUTHORITY_DENIED_WITHOUT_BLOCKER")
    if degraded_mode and authority_status != "GRANTED":
        raise ValueError("PAPER_SESSION_AUTHORITY_DEGRADED_REQUIRES_GRANTED")
    if degraded_mode and not advisory_checks:
        raise ValueError("PAPER_SESSION_AUTHORITY_DEGRADED_REQUIRES_ADVISORY")
    if not degraded_mode and authority_status == "GRANTED" and advisory_checks and payload.get("paper_open_allowed") is True:
        raise ValueError("PAPER_SESSION_AUTHORITY_GRANTED_WITH_ADVISORY_REQUIRES_DEGRADED")
    if not safety_checks:
        raise ValueError("PAPER_SESSION_AUTHORITY_SAFETY_CHECKS_REQUIRED")


def write_paper_session_authority_v1(*, truth_root: Path, payload: Mapping[str, Any]) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    obj = dict(payload)
    _validate_semantics_v1(obj)
    day_utc = str(obj.get("day_utc") or "").strip()
    if not day_utc:
        raise ValueError("PAPER_SESSION_AUTHORITY_DAY_REQUIRED")
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_paper_session_authority_path(truth_root=root, day_utc=day_utc),
        payload=obj,
        schema_relpath=SCHEMA_RELPATH_V1,
        volatile_field_names=("produced_utc",),
    )


def read_paper_session_authority_ref_v1(*, truth_root: Path, day_utc: str) -> SurfaceRefV1:
    ref = read_validated_surface_v1(
        path=resolve_paper_session_authority_path(
            truth_root=resolve_fact_plane_truth_root_v1(truth_root),
            day_utc=day_utc,
        ),
        schema_relpath=SCHEMA_RELPATH_V1,
    )
    _validate_semantics_v1(ref.payload)
    return ref
