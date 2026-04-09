from __future__ import annotations

import hashlib
from typing import Any, Mapping

from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1


ALLOWED_BLOCKER_FAMILIES_V1 = (
    "SUCCESS_VERIFICATION",
    "PROOF_INVALID",
    "DEPLOYMENT_BLOCK",
    "DAY_START_BLOCK",
)


def _require_nonempty_string(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(code)
    return text


def normalize_recurrence_fingerprint_input_v1(
    *,
    blocker_family: str,
    blocker_code: str,
    authority_source: str,
    stage_id: str,
) -> dict[str, str]:
    family = _require_nonempty_string(
        blocker_family,
        "RECURRENCE_FINGERPRINT_BLOCKER_FAMILY_MISSING",
    ).upper()
    if family not in ALLOWED_BLOCKER_FAMILIES_V1:
        raise ValueError(f"RECURRENCE_FINGERPRINT_BLOCKER_FAMILY_INVALID:{family}")
    return {
        "blocker_family": family,
        "blocker_code": _require_nonempty_string(
            blocker_code,
            "RECURRENCE_FINGERPRINT_BLOCKER_CODE_MISSING",
        ).upper(),
        "authority_source": _require_nonempty_string(
            authority_source,
            "RECURRENCE_FINGERPRINT_AUTHORITY_SOURCE_MISSING",
        ),
        "stage_id": _require_nonempty_string(
            stage_id,
            "RECURRENCE_FINGERPRINT_STAGE_ID_MISSING",
        ).upper(),
    }


def recurrence_fingerprint_v1(
    *,
    blocker_family: str,
    blocker_code: str,
    authority_source: str,
    stage_id: str,
) -> str:
    normalized = normalize_recurrence_fingerprint_input_v1(
        blocker_family=blocker_family,
        blocker_code=blocker_code,
        authority_source=authority_source,
        stage_id=stage_id,
    )
    digest = hashlib.sha256(canonical_json_bytes_v1(normalized)).hexdigest()
    return f"recurrence:{digest[:24]}"


def build_recurrence_fingerprint_record_v1(
    *,
    blocker_family: str,
    blocker_code: str,
    authority_source: str,
    stage_id: str,
) -> dict[str, str]:
    normalized = normalize_recurrence_fingerprint_input_v1(
        blocker_family=blocker_family,
        blocker_code=blocker_code,
        authority_source=authority_source,
        stage_id=stage_id,
    )
    return {
        **normalized,
        "recurrence_fingerprint": recurrence_fingerprint_v1(**normalized),
    }


def recurrence_fingerprint_from_mapping_v1(payload: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(payload, Mapping):
        raise ValueError("RECURRENCE_FINGERPRINT_PAYLOAD_INVALID")
    return build_recurrence_fingerprint_record_v1(
        blocker_family=str(payload.get("blocker_family") or "").strip(),
        blocker_code=str(payload.get("blocker_code") or "").strip(),
        authority_source=str(payload.get("authority_source") or "").strip(),
        stage_id=str(payload.get("stage_id") or "").strip(),
    )
