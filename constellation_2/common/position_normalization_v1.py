from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    repo_git_sha_v1,
)


POSITION_NORMALIZATION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/position_normalization.v1.schema.json"

ORIGIN_NATIVE = "NATIVE"
ORIGIN_IMPORTED = "IMPORTED"
RISK_BASIS_NATIVE = "R_NATIVE"
RISK_BASIS_SYNTHETIC = "R_SYNTHETIC"
RISK_BASIS_UNMANAGED = "UNMANAGED"
NORMALIZATION_NORMALIZED = "NORMALIZED"
NORMALIZATION_BLOCKED = "BLOCKED"
SCORING_INCLUDED = "INCLUDED"
SCORING_EXCLUDED_DEFAULT = "EXCLUDED_BY_DEFAULT"
SCORING_EXCLUDED_PERMANENT = "EXCLUDED_PERMANENT"
MANAGEMENT_READY = "READY"
MANAGEMENT_BLOCKED = "BLOCKED"


def resolve_position_normalization_path(*, truth_root: Path, day_utc: str, position_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "positions_v1"
        / "normalization_v1"
        / parse_day_utc_v1(day_utc)
        / str(position_id).strip()
        / "position_normalization.v1.json"
    ).resolve()


def _decimal_or_zero(value: Any) -> Decimal:
    raw = str(value or "").strip()
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized.quantize(Decimal("1")), "f")
    return format(normalized, "f")


def _price_text(value: Any) -> str:
    numeric = _decimal_or_zero(value)
    return "" if numeric <= 0 else _decimal_text(numeric)


def _normalize_provenance_refs(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        logical_name = str(row.get("logical_name") or "").strip()
        artifact_path = str(row.get("artifact_path") or "").strip()
        artifact_sha256 = str(row.get("artifact_sha256") or "").strip()
        if not logical_name and not artifact_path:
            continue
        key = (logical_name, artifact_path)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "logical_name": logical_name,
                "artifact_path": artifact_path,
                "artifact_sha256": artifact_sha256,
            }
        )
    return normalized


def derive_position_normalization_payload_v1(
    *,
    day_utc: str,
    position_id: str,
    origin: str,
    entry_price: Any,
    initial_stop_price: Any,
    initial_r_value: Any,
    reference_entry_source: str = "",
    synthetic_stop_source: str = "",
    scoring_eligibility: str = "",
    provenance_refs: Iterable[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    normalized_day = parse_day_utc_v1(day_utc)
    normalized_origin = str(origin or "").strip().upper()
    if normalized_origin not in {ORIGIN_NATIVE, ORIGIN_IMPORTED}:
        raise ValueError(f"UNSUPPORTED_POSITION_ORIGIN:{origin}")

    entry_price_text = _price_text(entry_price)
    initial_stop_price_text = _price_text(initial_stop_price)
    initial_r_decimal = _decimal_or_zero(initial_r_value)
    initial_r_text = "" if initial_r_decimal <= 0 else _decimal_text(initial_r_decimal)
    entry_source = str(reference_entry_source or "").strip()
    stop_source = str(synthetic_stop_source or "").strip()
    reason_codes: List[str] = []

    if not entry_price_text:
        reason_codes.append("ENTRY_REFERENCE_MISSING")
    if normalized_origin == ORIGIN_NATIVE:
        if not entry_source:
            entry_source = "CONSTELLATION_ENTRY"
        if not stop_source:
            stop_source = "CONSTELLATION_PROTECTION"
        if not initial_stop_price_text:
            reason_codes.append("NATIVE_INITIAL_STOP_MISSING")
        if not initial_r_text:
            reason_codes.append("NATIVE_INITIAL_R_MISSING")
        risk_basis = RISK_BASIS_NATIVE if not reason_codes else RISK_BASIS_UNMANAGED
        default_scoring = SCORING_INCLUDED
    else:
        if not entry_source:
            reason_codes.append("IMPORTED_REFERENCE_ENTRY_SOURCE_MISSING")
        if not stop_source:
            reason_codes.append("IMPORTED_SYNTHETIC_STOP_SOURCE_MISSING")
        if not initial_stop_price_text:
            reason_codes.append("IMPORTED_SYNTHETIC_STOP_MISSING")
        if not initial_r_text:
            reason_codes.append("IMPORTED_SYNTHETIC_R_MISSING")
        risk_basis = RISK_BASIS_SYNTHETIC if not reason_codes else RISK_BASIS_UNMANAGED
        default_scoring = SCORING_EXCLUDED_DEFAULT

    normalized_scoring = str(scoring_eligibility or default_scoring).strip().upper()
    if normalized_scoring not in {SCORING_INCLUDED, SCORING_EXCLUDED_DEFAULT, SCORING_EXCLUDED_PERMANENT}:
        raise ValueError(f"UNSUPPORTED_SCORING_ELIGIBILITY:{scoring_eligibility}")

    normalization_status = NORMALIZATION_NORMALIZED if not reason_codes else NORMALIZATION_BLOCKED
    management_eligibility = MANAGEMENT_READY if normalization_status == NORMALIZATION_NORMALIZED else MANAGEMENT_BLOCKED
    return {
        "schema_id": "position_normalization",
        "schema_version": "v1",
        "day_utc": normalized_day,
        "position_id": str(position_id).strip(),
        "origin": normalized_origin,
        "normalization_status": normalization_status,
        "risk_basis": risk_basis,
        "entry_reference": {
            "source": entry_source,
            "price": entry_price_text,
        },
        "initial_stop_reference": {
            "source": stop_source,
            "price": initial_stop_price_text,
        },
        "initial_r_value": initial_r_text,
        "reference_entry_source": entry_source,
        "synthetic_stop_source": stop_source,
        "scoring_eligibility": normalized_scoring,
        "exit_management_eligibility": management_eligibility,
        "reason_codes": reason_codes,
        "synthetic_risk_labeled": risk_basis == RISK_BASIS_SYNTHETIC,
        "produced_at_utc": now_utc_iso_v1(),
        "producer": producer_block_v1(
            module="constellation_2/common/position_normalization_v1.py",
            git_sha=repo_git_sha_v1(),
        ),
        "provenance_refs": _normalize_provenance_refs(provenance_refs),
    }


def write_position_normalization_v1(*, truth_root: Path, payload: Mapping[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_position_normalization_path(
            truth_root=truth_root,
            day_utc=str(payload.get("day_utc") or ""),
            position_id=str(payload.get("position_id") or ""),
        ),
        payload=dict(payload),
        schema_relpath=POSITION_NORMALIZATION_SCHEMA_RELPATH,
        volatile_field_names=("produced_at_utc",),
    )
