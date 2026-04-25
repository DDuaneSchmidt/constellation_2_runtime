from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .types import ValuationMismatch


def _parse_ts(value: str | None) -> float | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _normalize(records: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[tuple[str, str], dict[str, Any]]:
    normalized: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        normalized[(str(record.get("account_id") or ""), str(record.get("symbol") or ""))] = dict(record)
    return normalized


def reconcile_valuations(
    internal_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    external_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    thresholds: dict[str, Any],
) -> tuple[ValuationMismatch, ...]:
    material_delta = float(thresholds["valuation_material_delta"])
    staleness_seconds = float(thresholds["valuation_staleness_seconds"])
    timestamp_tolerance = float(thresholds.get("valuation_timestamp_tolerance_seconds", 0.0))
    internal = _normalize(internal_records)
    external = _normalize(external_records)
    mismatches: list[ValuationMismatch] = []
    for key in sorted(set(internal) | set(external)):
        left = internal.get(key)
        right = external.get(key)
        account_id, symbol = key
        if left is None or right is None:
            mismatches.append(
                ValuationMismatch(
                    account_id=account_id or "UNKNOWN",
                    symbol=symbol,
                    internal_value=left.get("value") if left else None,
                    external_value=right.get("value") if right else None,
                    delta_value=None,
                    mismatch_type="mapping_error",
                    severity="material",
                    explanation="valuation record missing on one side",
                )
            )
            continue
        left_value = float(left.get("value", 0.0))
        right_value = float(right.get("value", 0.0))
        delta_value = right_value - left_value
        if abs(delta_value) >= material_delta:
            mismatches.append(
                ValuationMismatch(
                    account_id=account_id,
                    symbol=symbol,
                    internal_value=left_value,
                    external_value=right_value,
                    delta_value=delta_value,
                    mismatch_type="value_mismatch",
                    severity="material",
                    explanation="valuation delta exceeds configured material threshold",
                )
            )
            continue
        left_ts = _parse_ts(left.get("captured_at") or left.get("timestamp"))
        right_ts = _parse_ts(right.get("captured_at") or right.get("timestamp"))
        if left_ts is not None and right_ts is not None:
            if abs(left_ts - right_ts) > staleness_seconds:
                mismatches.append(
                    ValuationMismatch(
                        account_id=account_id,
                        symbol=symbol,
                        internal_value=left_value,
                        external_value=right_value,
                        delta_value=delta_value,
                        mismatch_type="stale_valuation",
                        severity="material",
                        explanation="valuation timestamp staleness exceeds configured threshold",
                    )
                )
            elif abs(left_ts - right_ts) > timestamp_tolerance:
                mismatches.append(
                    ValuationMismatch(
                        account_id=account_id,
                        symbol=symbol,
                        internal_value=left_value,
                        external_value=right_value,
                        delta_value=delta_value,
                        mismatch_type="timestamp_mismatch",
                        severity="minor",
                        explanation="valuation timestamps differ without a material value delta",
                    )
                )
    return tuple(mismatches)
