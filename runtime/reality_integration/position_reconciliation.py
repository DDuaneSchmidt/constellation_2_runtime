from __future__ import annotations

from typing import Any

from .types import PositionMismatch


def _normalize_positions(records: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[tuple[str, str], dict[str, Any]]:
    normalized: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        account_id = str(record.get("account_id") or "")
        symbol = str(record.get("symbol") or record.get("position_id") or "")
        normalized[(account_id, symbol)] = dict(record)
    return normalized


def reconcile_positions(
    internal_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    external_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    thresholds: dict[str, Any] | None = None,
) -> tuple[PositionMismatch, ...]:
    thresholds = thresholds or {}
    staleness_seconds = thresholds.get("position_staleness_seconds")
    internal = _normalize_positions(internal_records)
    external = _normalize_positions(external_records)
    mismatches: list[PositionMismatch] = []
    for key in sorted(set(internal) | set(external)):
        internal_record = internal.get(key)
        external_record = external.get(key)
        account_id, symbol = key
        if not account_id:
            mismatches.append(
                PositionMismatch(
                    account_id="UNKNOWN",
                    symbol=symbol,
                    internal_quantity=internal_record.get("quantity") if internal_record else None,
                    external_quantity=external_record.get("quantity") if external_record else None,
                    delta_quantity=None,
                    mismatch_type="account_mapping_error",
                    severity="material",
                    explanation="account_id missing on one or both position records",
                )
            )
            continue
        if internal_record is None:
            mismatches.append(
                PositionMismatch(
                    account_id=account_id,
                    symbol=symbol,
                    internal_quantity=None,
                    external_quantity=float(external_record.get("quantity", 0)),
                    delta_quantity=float(external_record.get("quantity", 0)),
                    mismatch_type="missing_internal",
                    severity="material",
                    explanation="external position exists without internal position truth",
                )
            )
            continue
        if external_record is None:
            mismatches.append(
                PositionMismatch(
                    account_id=account_id,
                    symbol=symbol,
                    internal_quantity=float(internal_record.get("quantity", 0)),
                    external_quantity=None,
                    delta_quantity=float(internal_record.get("quantity", 0)),
                    mismatch_type="missing_external",
                    severity="material",
                    explanation="internal position exists without external broker evidence",
                )
            )
            continue
        internal_quantity = float(internal_record.get("quantity", 0))
        external_quantity = float(external_record.get("quantity", 0))
        if internal_quantity != external_quantity:
            mismatches.append(
                PositionMismatch(
                    account_id=account_id,
                    symbol=symbol,
                    internal_quantity=internal_quantity,
                    external_quantity=external_quantity,
                    delta_quantity=external_quantity - internal_quantity,
                    mismatch_type="quantity_mismatch",
                    severity="material",
                    explanation="internal and external quantities differ",
                )
            )
            continue
        if staleness_seconds is not None:
            internal_timestamp = internal_record.get("captured_at") or internal_record.get("timestamp")
            external_timestamp = external_record.get("captured_at") or external_record.get("timestamp")
            if internal_timestamp and external_timestamp and internal_timestamp != external_timestamp:
                mismatches.append(
                    PositionMismatch(
                        account_id=account_id,
                        symbol=symbol,
                        internal_quantity=internal_quantity,
                        external_quantity=external_quantity,
                        delta_quantity=0.0,
                        mismatch_type="stale_internal",
                        severity="minor",
                        explanation="position quantities match but timestamps differ",
                    )
                )
    return tuple(mismatches)
