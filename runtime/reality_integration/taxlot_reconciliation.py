from __future__ import annotations

from typing import Any

from .types import TaxLotMismatch


def _normalize_taxlots(records: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[tuple[str, str, str], dict[str, Any]]:
    normalized: dict[tuple[str, str, str], dict[str, Any]] = {}
    for record in records:
        account_id = str(record.get("account_id") or "")
        symbol = str(record.get("symbol") or "")
        lot_key = str(record.get("lot_key") or "")
        normalized[(account_id, symbol, lot_key)] = dict(record)
    return normalized


def reconcile_taxlots(
    internal_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    external_records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    thresholds: dict[str, Any] | None = None,
) -> tuple[TaxLotMismatch, ...]:
    thresholds = thresholds or {}
    basis_tolerance = float(thresholds.get("taxlot_basis_tolerance", 0.0))
    internal = _normalize_taxlots(internal_records)
    external = _normalize_taxlots(external_records)
    mismatches: list[TaxLotMismatch] = []
    for key in sorted(set(internal) | set(external)):
        internal_record = internal.get(key)
        external_record = external.get(key)
        account_id, symbol, lot_key = key
        if not lot_key:
            mismatches.append(
                TaxLotMismatch(
                    account_id=account_id or "UNKNOWN",
                    symbol=symbol,
                    lot_key="UNKNOWN",
                    internal_basis=internal_record.get("basis") if internal_record else None,
                    external_basis=external_record.get("basis") if external_record else None,
                    internal_quantity=internal_record.get("quantity") if internal_record else None,
                    external_quantity=external_record.get("quantity") if external_record else None,
                    mismatch_type="lot_identity_ambiguous",
                    severity="material",
                    explanation="lot identity is ambiguous and cannot be silently resolved",
                )
            )
            continue
        if internal_record is None or external_record is None:
            mismatches.append(
                TaxLotMismatch(
                    account_id=account_id or "UNKNOWN",
                    symbol=symbol,
                    lot_key=lot_key,
                    internal_basis=internal_record.get("basis") if internal_record else None,
                    external_basis=external_record.get("basis") if external_record else None,
                    internal_quantity=internal_record.get("quantity") if internal_record else None,
                    external_quantity=external_record.get("quantity") if external_record else None,
                    mismatch_type="missing_lot",
                    severity="material",
                    explanation="tax lot missing on one side of reconciliation",
                )
            )
            continue
        internal_basis = float(internal_record.get("basis", 0.0))
        external_basis = float(external_record.get("basis", 0.0))
        internal_quantity = float(internal_record.get("quantity", 0.0))
        external_quantity = float(external_record.get("quantity", 0.0))
        if abs(internal_basis - external_basis) > basis_tolerance:
            mismatches.append(
                TaxLotMismatch(
                    account_id=account_id,
                    symbol=symbol,
                    lot_key=lot_key,
                    internal_basis=internal_basis,
                    external_basis=external_basis,
                    internal_quantity=internal_quantity,
                    external_quantity=external_quantity,
                    mismatch_type="basis_mismatch",
                    severity="material",
                    explanation="tax lot basis differs beyond allowed tolerance",
                )
            )
            continue
        if internal_quantity != external_quantity:
            mismatches.append(
                TaxLotMismatch(
                    account_id=account_id,
                    symbol=symbol,
                    lot_key=lot_key,
                    internal_basis=internal_basis,
                    external_basis=external_basis,
                    internal_quantity=internal_quantity,
                    external_quantity=external_quantity,
                    mismatch_type="quantity_mismatch",
                    severity="material",
                    explanation="tax lot quantity differs",
                )
            )
    return tuple(mismatches)
