from __future__ import annotations

from datetime import datetime, timezone
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


TRADE_RESULT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/trade_result.v1.schema.json"


def resolve_trade_result_path(*, truth_root: Path, day_utc: str, position_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "positions_v1"
        / "trade_result_v1"
        / parse_day_utc_v1(day_utc)
        / str(position_id).strip()
        / "trade_result.v1.json"
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


def _holding_time_seconds(entry_time_utc: str, exit_time_utc: str) -> int:
    if not str(entry_time_utc or "").strip() or not str(exit_time_utc or "").strip():
        return 0
    entry = datetime.fromisoformat(str(entry_time_utc).replace("Z", "+00:00"))
    exit_ = datetime.fromisoformat(str(exit_time_utc).replace("Z", "+00:00"))
    if entry.tzinfo is None:
        entry = entry.replace(tzinfo=timezone.utc)
    if exit_.tzinfo is None:
        exit_ = exit_.replace(tzinfo=timezone.utc)
    seconds = int((exit_ - entry).total_seconds())
    return seconds if seconds > 0 else 0


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


def derive_trade_result_payload_v1(
    *,
    day_utc: str,
    position_id: str,
    normalization_payload: Mapping[str, Any],
    exit_decision_payload: Mapping[str, Any],
    exit_price: Any,
    entry_time_utc: str = "",
    exit_time_utc: str = "",
    mfe_r: Any = "",
    mae_r: Any = "",
    provenance_refs: Iterable[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    side = str(exit_decision_payload.get("side") or "").strip().upper()
    entry_price = _decimal_or_zero((normalization_payload.get("entry_reference") or {}).get("price"))
    exit_price_decimal = _decimal_or_zero(exit_price)
    initial_r_value = _decimal_or_zero(normalization_payload.get("initial_r_value"))
    realized_r = Decimal("0")
    if initial_r_value > 0 and entry_price > 0 and exit_price_decimal > 0:
        if side == "SHORT":
            realized_r = (entry_price - exit_price_decimal) / initial_r_value
        else:
            realized_r = (exit_price_decimal - entry_price) / initial_r_value
    scoring_eligibility = str(normalization_payload.get("scoring_eligibility") or "").strip().upper()
    analytics_eligibility = (
        "INCLUDED"
        if scoring_eligibility == "INCLUDED"
        and str(normalization_payload.get("origin") or "").strip().upper() == "NATIVE"
        and str(normalization_payload.get("risk_basis") or "").strip().upper() == "R_NATIVE"
        else "EXCLUDED_BY_DEFAULT"
    )
    return {
        "schema_id": "trade_result",
        "schema_version": "v1",
        "day_utc": parse_day_utc_v1(day_utc),
        "position_id": str(position_id).strip(),
        "decision_id": str(exit_decision_payload.get("decision_id") or "").strip(),
        "origin": str(normalization_payload.get("origin") or "").strip(),
        "risk_basis": str(normalization_payload.get("risk_basis") or "").strip(),
        "entry_price": _decimal_text(entry_price) if entry_price > 0 else "",
        "initial_stop_price": str((normalization_payload.get("initial_stop_reference") or {}).get("price") or "").strip(),
        "exit_price": _decimal_text(exit_price_decimal) if exit_price_decimal > 0 else "",
        "realized_r": _decimal_text(realized_r),
        "mfe_r": str(mfe_r or "").strip(),
        "mae_r": str(mae_r or "").strip(),
        "holding_time_seconds": _holding_time_seconds(entry_time_utc, exit_time_utc),
        "scoring_eligibility": scoring_eligibility,
        "analytics_eligibility": analytics_eligibility,
        "synthetic_risk_labeled": str(normalization_payload.get("risk_basis") or "").strip().upper() == "R_SYNTHETIC",
        "reason_codes": list(exit_decision_payload.get("reason_codes") or []),
        "produced_at_utc": now_utc_iso_v1(),
        "producer": producer_block_v1(
            module="constellation_2/common/trade_result_closure_v1.py",
            git_sha=repo_git_sha_v1(),
        ),
        "provenance_refs": _normalize_provenance_refs(provenance_refs),
    }


def write_trade_result_v1(*, truth_root: Path, payload: Mapping[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_trade_result_path(
            truth_root=truth_root,
            day_utc=str(payload.get("day_utc") or ""),
            position_id=str(payload.get("position_id") or ""),
        ),
        payload=dict(payload),
        schema_relpath=TRADE_RESULT_SCHEMA_RELPATH,
        volatile_field_names=("produced_at_utc",),
    )
