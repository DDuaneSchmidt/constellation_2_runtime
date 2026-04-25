from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .types import ExecutionMismatch


def _parse_ts(value: str | None) -> float | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _normalize_executions(records: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for record in records:
        key = str(record.get("execution_key") or f"{record.get('position_id', 'unknown')}::{record.get('action', 'UNKNOWN')}::{record.get('captured_at', record.get('timestamp', 'unknown'))}")
        normalized[key] = dict(record)
    return normalized


def reconcile_executions(
    internal_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    external_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    thresholds: dict[str, Any] | None = None,
) -> tuple[ExecutionMismatch, ...]:
    thresholds = thresholds or {}
    fee_tolerance = float(thresholds.get("execution_fee_tolerance", 0.0))
    timestamp_tolerance = float(thresholds.get("execution_timestamp_tolerance_seconds", 0.0))
    internal = _normalize_executions(internal_records)
    external = _normalize_executions(external_records)
    mismatches: list[ExecutionMismatch] = []
    for key in sorted(set(internal) | set(external)):
        left = internal.get(key)
        right = external.get(key)
        if left is None:
            mismatches.append(
                ExecutionMismatch(
                    execution_key=key,
                    internal_execution=None,
                    external_execution=right,
                    mismatch_type="missing_fill",
                    severity="critical" if float(right.get("filled_qty", right.get("quantity", 0))) else "material",
                    explanation="external fill exists without internal execution truth",
                )
            )
            continue
        if right is None:
            mismatches.append(
                ExecutionMismatch(
                    execution_key=key,
                    internal_execution=left,
                    external_execution=None,
                    mismatch_type="extra_fill",
                    severity="critical" if float(left.get("filled_qty", left.get("quantity", 0))) else "material",
                    explanation="internal execution exists without external fill evidence",
                )
            )
            continue
        left_qty = float(left.get("filled_qty", left.get("quantity", 0)))
        right_qty = float(right.get("filled_qty", right.get("quantity", 0)))
        if left_qty != right_qty:
            mismatches.append(
                ExecutionMismatch(
                    execution_key=key,
                    internal_execution=left,
                    external_execution=right,
                    mismatch_type="quantity_mismatch",
                    severity="critical",
                    explanation="execution quantity differs and changes live position truth",
                )
            )
            continue
        if left.get("side") and right.get("side") and left.get("side") != right.get("side"):
            mismatches.append(
                ExecutionMismatch(
                    execution_key=key,
                    internal_execution=left,
                    external_execution=right,
                    mismatch_type="side_mismatch",
                    severity="critical",
                    explanation="execution side differs",
                )
            )
            continue
        if left.get("price") is not None and right.get("price") is not None and float(left["price"]) != float(right["price"]):
            mismatches.append(
                ExecutionMismatch(
                    execution_key=key,
                    internal_execution=left,
                    external_execution=right,
                    mismatch_type="price_mismatch",
                    severity="material",
                    explanation="execution price differs",
                )
            )
            continue
        if left.get("fee") is not None and right.get("fee") is not None and abs(float(left["fee"]) - float(right["fee"])) > fee_tolerance:
            mismatches.append(
                ExecutionMismatch(
                    execution_key=key,
                    internal_execution=left,
                    external_execution=right,
                    mismatch_type="fee_mismatch",
                    severity="minor",
                    explanation="execution fee differs",
                )
            )
            continue
        left_ts = _parse_ts(left.get("timestamp") or left.get("captured_at"))
        right_ts = _parse_ts(right.get("timestamp") or right.get("captured_at"))
        if left_ts is not None and right_ts is not None and abs(left_ts - right_ts) > timestamp_tolerance:
            mismatches.append(
                ExecutionMismatch(
                    execution_key=key,
                    internal_execution=left,
                    external_execution=right,
                    mismatch_type="timestamp_mismatch",
                    severity="minor",
                    explanation="execution timestamps differ beyond the allowed tolerance",
                )
            )
    return tuple(mismatches)
