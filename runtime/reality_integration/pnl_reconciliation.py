from __future__ import annotations

from typing import Any

from .types import PnLMismatch


def _normalize(records: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[tuple[str, str], dict[str, Any]]:
    normalized: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        key = (str(record.get("account_id") or ""), str(record.get("symbol_or_scope") or record.get("scope") or ""))
        normalized[key] = dict(record)
    return normalized


def reconcile_pnl(
    internal_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    external_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    thresholds: dict[str, Any],
) -> tuple[PnLMismatch, ...]:
    material_delta = float(thresholds["pnl_material_delta"])
    internal = _normalize(internal_records)
    external = _normalize(external_records)
    mismatches: list[PnLMismatch] = []
    for key in sorted(set(internal) | set(external)):
        left = internal.get(key)
        right = external.get(key)
        account_id, symbol_or_scope = key
        if left is None or right is None:
            mismatches.append(
                PnLMismatch(
                    account_id=account_id or "UNKNOWN",
                    symbol_or_scope=symbol_or_scope,
                    internal_pnl=left.get("pnl") if left else None,
                    external_pnl=right.get("pnl") if right else None,
                    delta_pnl=None,
                    mismatch_type="missing_pnl_record",
                    severity="material",
                    explanation="PnL record missing on one side of reconciliation",
                )
            )
            continue
        left_value = float(left.get("pnl", 0.0))
        right_value = float(right.get("pnl", 0.0))
        delta = right_value - left_value
        if abs(delta) < material_delta:
            continue
        pnl_type = str(left.get("pnl_type") or right.get("pnl_type") or "")
        mismatch_type = {
            "realized": "realized_pnl_mismatch",
            "unrealized": "unrealized_pnl_mismatch",
            "fees": "fee_pnl_mismatch",
        }.get(pnl_type, "pnl_mismatch")
        mismatches.append(
            PnLMismatch(
                account_id=account_id or "UNKNOWN",
                symbol_or_scope=symbol_or_scope,
                internal_pnl=left_value,
                external_pnl=right_value,
                delta_pnl=delta,
                mismatch_type=mismatch_type,
                severity="material",
                explanation="PnL delta exceeds configured threshold",
            )
        )
    return tuple(mismatches)
