from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from .reconciliation_matcher_v1 import LOW_CONFIDENCE
from .schema_v1 import MISMATCH_TYPES


DEFAULT_PRICE_SLIPPAGE_BPS_THRESHOLD = Decimal("50")
DEFAULT_COMMISSION_ABS_THRESHOLD = Decimal("1.00")
DEFAULT_CASH_ABS_THRESHOLD = Decimal("5.00")
DEFAULT_POSITION_ABS_THRESHOLD = Decimal("0.0001")


def _to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _first_text(*values: Any, default: str = "") -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return default


def _severity_rank(severity: str) -> int:
    if severity == "HIGH":
        return 3
    if severity == "MEDIUM":
        return 2
    return 1


def _status_from_mismatches(mismatches: list[dict[str, Any]], *, all_expected_matched_deterministically: bool) -> str:
    has_high = any(m.get("severity") == "HIGH" for m in mismatches)
    has_medium = any(m.get("severity") == "MEDIUM" for m in mismatches)
    if has_high:
        return "FAIL"
    if has_medium:
        return "WARN"
    return "PASS" if all_expected_matched_deterministically else "WARN"


def _append_mismatch(
    mismatches: list[dict[str, Any]],
    *,
    mismatch_type: str,
    severity: str,
    symbol: str,
    account_id: str = "",
    aegis_attempt_id: str = "",
    ib_order_id: str = "",
    ib_perm_id: str = "",
    description: str,
    evidence_paths: list[str] | None = None,
    deterministic_rule: str,
) -> None:
    if mismatch_type not in MISMATCH_TYPES:
        raise RuntimeError(f"UNKNOWN_MISMATCH_TYPE:{mismatch_type}")
    if severity not in {"LOW", "MEDIUM", "HIGH"}:
        raise RuntimeError(f"UNKNOWN_MISMATCH_SEVERITY:{severity}")
    mismatch_id = f"MISMATCH-{len(mismatches) + 1:04d}"
    mismatches.append(
        {
            "mismatch_id": mismatch_id,
            "type": mismatch_type,
            "severity": severity,
            "symbol": symbol or "UNKNOWN",
            "account_id": account_id or "",
            "aegis_attempt_id": aegis_attempt_id or "",
            "ib_order_id": ib_order_id or "",
            "ib_perm_id": ib_perm_id or "",
            "description": description,
            "evidence_paths": sorted({p for p in (evidence_paths or []) if str(p).strip()}),
            "deterministic_rule": deterministic_rule,
        }
    )


def _expected_cash_rows(expected_activity: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = expected_activity.get("expected_cash_balances")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, Mapping):
            continue
        out.append(
            {
                "account_id": _first_text(row.get("account_id"), default="UNKNOWN_ACCOUNT"),
                "currency": _first_text(row.get("currency"), default="USD").upper(),
                "cash_balance": str(_to_decimal(row.get("cash_balance"))),
            }
        )
    return out


def _expected_position_rows(expected_activity: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = expected_activity.get("expected_positions")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, Mapping):
            continue
        out.append(
            {
                "account_id": _first_text(row.get("account_id"), default="UNKNOWN_ACCOUNT"),
                "symbol": _first_text(row.get("symbol"), default="UNKNOWN").upper(),
                "asset_class": _first_text(row.get("asset_class"), default="UNKNOWN").upper(),
                "quantity": str(_to_decimal(row.get("quantity"))),
            }
        )
    return out


def classify_mismatches_v1(
    *,
    day_utc: str,
    expected_activity: Mapping[str, Any],
    normalized_ib_trades: Mapping[str, Any],
    normalized_ib_positions: Mapping[str, Any] | None,
    normalized_ib_cash: Mapping[str, Any] | None,
    match_result: Mapping[str, Any],
    price_slippage_bps_threshold: Decimal = DEFAULT_PRICE_SLIPPAGE_BPS_THRESHOLD,
    commission_abs_threshold: Decimal = DEFAULT_COMMISSION_ABS_THRESHOLD,
    cash_abs_threshold: Decimal = DEFAULT_CASH_ABS_THRESHOLD,
    position_abs_threshold: Decimal = DEFAULT_POSITION_ABS_THRESHOLD,
) -> dict[str, Any]:
    mismatches: list[dict[str, Any]] = []
    unmatched_expected = match_result.get("unmatched_expected") if isinstance(match_result.get("unmatched_expected"), list) else []
    unmatched_ib_trades = match_result.get("unmatched_ib_trades") if isinstance(match_result.get("unmatched_ib_trades"), list) else []
    matched_items = match_result.get("matched_items") if isinstance(match_result.get("matched_items"), list) else []

    # 1) unexpected and manual IB trades.
    for trade in unmatched_ib_trades:
        if not isinstance(trade, Mapping):
            continue
        symbol = _first_text(trade.get("symbol"), default="UNKNOWN")
        ib_order_id = _first_text(trade.get("ib_order_id"), default="")
        ib_perm_id = _first_text(trade.get("ib_perm_id"), default="")
        description = f"IB trade was observed but no deterministic Aegis expected match exists for {symbol}."
        _append_mismatch(
            mismatches,
            mismatch_type="UNEXPECTED_IB_TRADE",
            severity="HIGH",
            symbol=symbol,
            account_id=_first_text(trade.get("account_id"), default=""),
            ib_order_id=ib_order_id,
            ib_perm_id=ib_perm_id,
            description=description,
            evidence_paths=[_first_text(trade.get("source_report_path"), default="")],
            deterministic_rule="UNMATCHED_IB_TRADE",
        )

        submission_refs = set()
        raw_ref = trade.get("raw_ref")
        if isinstance(raw_ref, Mapping):
            attrs = raw_ref.get("attributes")
            if isinstance(attrs, Mapping):
                for key in ("orderRef", "order_ref", "submission_id", "submissionId"):
                    value = _first_text(attrs.get(key), default="")
                    if value:
                        submission_refs.add(value)
        if not submission_refs:
            _append_mismatch(
                mismatches,
                mismatch_type="MANUAL_TRADE_DETECTED",
                severity="HIGH",
                symbol=symbol,
                account_id=_first_text(trade.get("account_id"), default=""),
                ib_order_id=ib_order_id,
                ib_perm_id=ib_perm_id,
                description=f"Unexpected IB trade {symbol} has no Aegis submission reference and appears manual/untracked.",
                evidence_paths=[],
                deterministic_rule="MANUAL_TRADE_HEURISTIC",
            )

    # 2) missing expected trades.
    for expected in unmatched_expected:
        if not isinstance(expected, Mapping):
            continue
        _append_mismatch(
            mismatches,
            mismatch_type="MISSING_IB_TRADE",
            severity="HIGH",
            symbol=_first_text(expected.get("symbol"), default="UNKNOWN"),
            aegis_attempt_id=_first_text(expected.get("attempt_id"), default=""),
            ib_order_id=_first_text(expected.get("broker_order_id"), default=""),
            ib_perm_id=_first_text(expected.get("broker_perm_id"), default=""),
            description=(
                "Aegis expected execution did not appear in normalized IB trades for the reconciliation day."
            ),
            evidence_paths=[],
            deterministic_rule="EXPECTED_EXECUTION_UNMATCHED",
        )

    # 3) duplicate exact IB trades.
    seen_signatures: dict[tuple[str, str, str, str, str, str], int] = {}
    ib_rows = normalized_ib_trades.get("trades") if isinstance(normalized_ib_trades.get("trades"), list) else []
    for trade in ib_rows:
        if not isinstance(trade, Mapping):
            continue
        signature = (
            _first_text(trade.get("ib_order_id"), default="0"),
            _first_text(trade.get("ib_perm_id"), default="0"),
            _first_text(trade.get("symbol"), default=""),
            _first_text(trade.get("side"), default=""),
            str(_to_decimal(trade.get("quantity"))),
            _first_text(trade.get("trade_time_utc"), default=""),
        )
        seen_signatures[signature] = seen_signatures.get(signature, 0) + 1
    for signature, count in sorted(seen_signatures.items()):
        if count <= 1:
            continue
        _append_mismatch(
            mismatches,
            mismatch_type="DUPLICATE_ORDER_DETECTED",
            severity="HIGH",
            symbol=signature[2] or "UNKNOWN",
            ib_order_id=signature[0],
            ib_perm_id=signature[1],
            description=f"Duplicate IB trade signature detected {count} times.",
            evidence_paths=[],
            deterministic_rule="IB_DUPLICATE_SIGNATURE",
        )

    # 4) matched row checks.
    for row in matched_items:
        if not isinstance(row, Mapping):
            continue
        symbol = _first_text(row.get("symbol"), default="UNKNOWN")
        expected_qty = _to_decimal(row.get("expected_quantity"))
        ib_qty = _to_decimal(row.get("ib_quantity"))
        expected_price = _to_decimal(row.get("expected_price"))
        ib_price = _to_decimal(row.get("ib_price"))
        broker_order_id = _first_text(row.get("broker_order_id"), default="")
        broker_perm_id = _first_text(row.get("broker_perm_id"), default="")
        ib_order_id = _first_text(row.get("ib_order_id"), default="")
        ib_perm_id = _first_text(row.get("ib_perm_id"), default="")
        attempt_id = _first_text(row.get("aegis_attempt_id"), default="")
        confidence = _first_text(row.get("confidence"), default="HIGH")

        if broker_order_id not in {"", "0"} and ib_order_id not in {"", "0"} and broker_order_id != ib_order_id:
            _append_mismatch(
                mismatches,
                mismatch_type="ORDER_ID_MISMATCH",
                severity="MEDIUM",
                symbol=symbol,
                aegis_attempt_id=attempt_id,
                ib_order_id=ib_order_id,
                ib_perm_id=ib_perm_id,
                description=f"Matched execution has broker_order_id={broker_order_id} but IB reported ib_order_id={ib_order_id}.",
                evidence_paths=[],
                deterministic_rule=_first_text(row.get("match_rule"), default="ID_COMPARE"),
            )

        if broker_perm_id not in {"", "0"} and ib_perm_id not in {"", "0"} and broker_perm_id != ib_perm_id:
            _append_mismatch(
                mismatches,
                mismatch_type="PERM_ID_MISMATCH",
                severity="MEDIUM",
                symbol=symbol,
                aegis_attempt_id=attempt_id,
                ib_order_id=ib_order_id,
                ib_perm_id=ib_perm_id,
                description=f"Matched execution has broker_perm_id={broker_perm_id} but IB reported ib_perm_id={ib_perm_id}.",
                evidence_paths=[],
                deterministic_rule=_first_text(row.get("match_rule"), default="ID_COMPARE"),
            )

        if expected_qty != ib_qty:
            mismatch_type = "PARTIAL_FILL_MISMATCH" if ib_qty < expected_qty else "QUANTITY_MISMATCH"
            _append_mismatch(
                mismatches,
                mismatch_type=mismatch_type,
                severity="MEDIUM",
                symbol=symbol,
                aegis_attempt_id=attempt_id,
                ib_order_id=ib_order_id,
                ib_perm_id=ib_perm_id,
                description=f"Expected quantity {expected_qty} differs from IB quantity {ib_qty}.",
                evidence_paths=[],
                deterministic_rule="QTY_COMPARE",
            )

        if expected_price > 0 and ib_price > 0:
            bps = abs((ib_price - expected_price) / expected_price) * Decimal("10000")
            if bps > price_slippage_bps_threshold:
                _append_mismatch(
                    mismatches,
                    mismatch_type="PRICE_SLIPPAGE_EXCEEDED",
                    severity="MEDIUM",
                    symbol=symbol,
                    aegis_attempt_id=attempt_id,
                    ib_order_id=ib_order_id,
                    ib_perm_id=ib_perm_id,
                    description=(
                        f"Price slippage {bps:.2f} bps exceeds threshold {price_slippage_bps_threshold} bps."
                    ),
                    evidence_paths=[],
                    deterministic_rule="PRICE_BPS_THRESHOLD",
                )

        expected_commission = _first_text(row.get("expected_commission"), default="")
        ib_commission = _first_text(row.get("ib_commission"), default="")
        if expected_commission and ib_commission:
            if abs(_to_decimal(ib_commission) - _to_decimal(expected_commission)) > commission_abs_threshold:
                _append_mismatch(
                    mismatches,
                    mismatch_type="COMMISSION_MISMATCH",
                    severity="MEDIUM",
                    symbol=symbol,
                    aegis_attempt_id=attempt_id,
                    ib_order_id=ib_order_id,
                    ib_perm_id=ib_perm_id,
                    description="Commission delta exceeded configured absolute threshold.",
                    evidence_paths=[],
                    deterministic_rule="COMMISSION_THRESHOLD",
                )

        if confidence == LOW_CONFIDENCE:
            _append_mismatch(
                mismatches,
                mismatch_type="LOW_CONFIDENCE_MATCH_ONLY",
                severity="MEDIUM",
                symbol=symbol,
                aegis_attempt_id=attempt_id,
                ib_order_id=ib_order_id,
                ib_perm_id=ib_perm_id,
                description="Matched only via fallback symbol/side/qty/time window heuristic.",
                evidence_paths=[],
                deterministic_rule="FALLBACK_SYMBOL_SIDE_QTY_TIME_WINDOW",
            )

    # 5) position mismatch checks.
    expected_positions = _expected_position_rows(expected_activity)
    ib_positions_raw = normalized_ib_positions.get("positions") if isinstance((normalized_ib_positions or {}).get("positions"), list) else []
    ib_positions: dict[tuple[str, str, str], Decimal] = {}
    for row in ib_positions_raw:
        if not isinstance(row, Mapping):
            continue
        key = (
            _first_text(row.get("account_id"), default="UNKNOWN_ACCOUNT"),
            _first_text(row.get("symbol"), default="UNKNOWN").upper(),
            _first_text(row.get("asset_class"), default="UNKNOWN").upper(),
        )
        ib_positions[key] = _to_decimal(row.get("quantity"))

    for expected in expected_positions:
        key = (expected["account_id"], expected["symbol"], expected["asset_class"])
        ib_qty = ib_positions.get(key)
        if ib_qty is None:
            _append_mismatch(
                mismatches,
                mismatch_type="POSITION_MISMATCH",
                severity="HIGH",
                symbol=expected["symbol"],
                account_id=expected["account_id"],
                description="Expected position not found in normalized IB positions snapshot.",
                evidence_paths=[],
                deterministic_rule="POSITION_KEY_LOOKUP",
            )
            continue
        delta = abs(ib_qty - _to_decimal(expected["quantity"]))
        if delta > position_abs_threshold:
            _append_mismatch(
                mismatches,
                mismatch_type="POSITION_MISMATCH",
                severity="HIGH",
                symbol=expected["symbol"],
                account_id=expected["account_id"],
                description=f"Position quantity delta {delta} exceeded threshold {position_abs_threshold}.",
                evidence_paths=[],
                deterministic_rule="POSITION_QUANTITY_THRESHOLD",
            )

    # 6) cash mismatch checks.
    expected_cash = _expected_cash_rows(expected_activity)
    ib_cash_raw = normalized_ib_cash.get("balances") if isinstance((normalized_ib_cash or {}).get("balances"), list) else []
    ib_cash: dict[tuple[str, str], Decimal] = {}
    for row in ib_cash_raw:
        if not isinstance(row, Mapping):
            continue
        key = (
            _first_text(row.get("account_id"), default="UNKNOWN_ACCOUNT"),
            _first_text(row.get("currency"), default="USD").upper(),
        )
        ib_cash[key] = _to_decimal(row.get("cash_balance"))

    for expected in expected_cash:
        key = (expected["account_id"], expected["currency"])
        ib_value = ib_cash.get(key)
        if ib_value is None:
            _append_mismatch(
                mismatches,
                mismatch_type="CASH_BALANCE_MISMATCH",
                severity="HIGH",
                symbol="CASH",
                account_id=expected["account_id"],
                description="Expected cash balance key missing from normalized IB cash snapshot.",
                evidence_paths=[],
                deterministic_rule="CASH_KEY_LOOKUP",
            )
            continue
        delta = abs(ib_value - _to_decimal(expected["cash_balance"]))
        if delta > cash_abs_threshold:
            severity = "HIGH"
        elif delta > 0:
            severity = "LOW"
        else:
            severity = ""
        if severity:
            _append_mismatch(
                mismatches,
                mismatch_type="CASH_BALANCE_MISMATCH",
                severity=severity,
                symbol="CASH",
                account_id=expected["account_id"],
                description=f"Cash balance delta {delta} detected for {expected['currency']}.",
                evidence_paths=[],
                deterministic_rule="CASH_ABS_THRESHOLD",
            )

    # 7) optional pnl mismatch.
    expected_pnl = expected_activity.get("expected_pnl")
    ib_pnl = expected_activity.get("ib_reported_pnl")
    if expected_pnl is not None and ib_pnl is not None:
        delta = abs(_to_decimal(expected_pnl) - _to_decimal(ib_pnl))
        if delta > Decimal("0.01"):
            _append_mismatch(
                mismatches,
                mismatch_type="PNL_MISMATCH",
                severity="MEDIUM",
                symbol="PNL",
                description="Expected and IB PnL values differ above tolerance.",
                evidence_paths=[],
                deterministic_rule="PNL_DELTA_THRESHOLD",
            )

    # Deterministic ordering.
    mismatches.sort(
        key=lambda row: (
            -_severity_rank(_first_text(row.get("severity"), default="LOW")),
            _first_text(row.get("type"), default=""),
            _first_text(row.get("symbol"), default=""),
            _first_text(row.get("aegis_attempt_id"), default=""),
            _first_text(row.get("ib_order_id"), default=""),
            _first_text(row.get("ib_perm_id"), default=""),
        )
    )
    for idx, row in enumerate(mismatches, start=1):
        row["mismatch_id"] = f"MISMATCH-{idx:04d}"

    all_expected_matched_deterministically = not unmatched_expected and all(
        _first_text(item.get("confidence"), default="HIGH") == "HIGH" for item in matched_items if isinstance(item, Mapping)
    )
    status = _status_from_mismatches(mismatches, all_expected_matched_deterministically=all_expected_matched_deterministically)

    summary = {
        "total_expected_executions": len(unmatched_expected) + len(matched_items),
        "matched_count": len(matched_items),
        "unmatched_expected_count": len(unmatched_expected),
        "unexpected_ib_trade_count": len([m for m in mismatches if m["type"] == "UNEXPECTED_IB_TRADE"]),
        "high_mismatch_count": len([m for m in mismatches if m["severity"] == "HIGH"]),
        "medium_mismatch_count": len([m for m in mismatches if m["severity"] == "MEDIUM"]),
        "low_mismatch_count": len([m for m in mismatches if m["severity"] == "LOW"]),
        "all_expected_matched_deterministically": all_expected_matched_deterministically,
    }

    return {
        "day_utc": day_utc,
        "status": status,
        "mismatches": mismatches,
        "summary": summary,
    }


def resolve_reconciliation_status_v1(mismatches: list[Mapping[str, Any]], *, all_expected_matched_deterministically: bool) -> str:
    typed = [dict(item) for item in mismatches]
    return _status_from_mismatches(typed, all_expected_matched_deterministically=all_expected_matched_deterministically)
