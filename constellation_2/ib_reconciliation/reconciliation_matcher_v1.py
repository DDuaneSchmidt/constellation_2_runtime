from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


RULE_PERM_ID_EXACT = "PERM_ID_EXACT"
RULE_ORDER_ID_EXACT = "ORDER_ID_EXACT"
RULE_SUBMISSION_ID_REFERENCE = "SUBMISSION_ID_REFERENCE"
RULE_FALLBACK_SYMBOL_SIDE_QTY_TIME_WINDOW = "FALLBACK_SYMBOL_SIDE_QTY_TIME_WINDOW"

HIGH_CONFIDENCE = "HIGH"
LOW_CONFIDENCE = "LOW"

FALLBACK_TIME_WINDOW_MINUTES = 30


def _to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _normalized_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "0"
    if text.lower() in {"none", "null"}:
        return "0"
    return text


def _is_nonzero_id(value: str) -> bool:
    if str(value).strip() in {"", "0"}:
        return False
    try:
        return int(str(value)) != 0
    except ValueError:
        return True


def _first_text(*values: Any, default: str = "") -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return default


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _trade_submission_refs(trade: Mapping[str, Any]) -> set[str]:
    refs: set[str] = set()
    raw_ref = trade.get("raw_ref")
    if not isinstance(raw_ref, Mapping):
        return refs
    attrs = raw_ref.get("attributes")
    if isinstance(attrs, Mapping):
        for key in ("orderRef", "order_ref", "submission_id", "submissionId", "clientOrderId", "clOrdID"):
            text = _first_text(attrs.get(key), default="")
            if text:
                refs.add(text)
    for key in ("submission_id", "submission_ref", "order_ref"):
        text = _first_text(raw_ref.get(key), default="")
        if text:
            refs.add(text)
    return refs


def _build_expected_execution_rows(expected_activity: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    fills = expected_activity.get("expected_fills")
    if isinstance(fills, list):
        for row in fills:
            if not isinstance(row, Mapping):
                continue
            rows.append(
                {
                    "attempt_id": _first_text(row.get("attempt_id"), default="UNKNOWN_ATTEMPT"),
                    "submission_id": _first_text(row.get("submission_id"), default="UNKNOWN_SUBMISSION"),
                    "symbol": _first_text(row.get("symbol"), default="UNKNOWN").upper(),
                    "side": _first_text(row.get("side"), default="UNKNOWN").upper(),
                    "quantity": str(_to_decimal(row.get("quantity"))),
                    "expected_price": _first_text(row.get("expected_price"), default=""),
                    "broker_order_id": _normalized_id(row.get("broker_order_id")),
                    "broker_perm_id": _normalized_id(row.get("broker_perm_id")),
                    "expected_time_utc": _first_text(row.get("expected_time_utc"), default=""),
                    "origin": "expected_fill",
                }
            )

    fill_submission_ids = {row["submission_id"] for row in rows}
    orders = expected_activity.get("expected_orders")
    if isinstance(orders, list):
        for row in orders:
            if not isinstance(row, Mapping):
                continue
            submission_id = _first_text(row.get("submission_id"), default="UNKNOWN_SUBMISSION")
            status = _first_text(row.get("status"), default="UNKNOWN").upper()
            if submission_id in fill_submission_ids:
                continue
            if status not in {"FILLED", "PARTIALLY_FILLED", "SUBMITTED", "ACKNOWLEDGED"}:
                continue
            rows.append(
                {
                    "attempt_id": _first_text(row.get("attempt_id"), default="UNKNOWN_ATTEMPT"),
                    "submission_id": submission_id,
                    "symbol": _first_text(row.get("symbol"), default="UNKNOWN").upper(),
                    "side": _first_text(row.get("side"), default="UNKNOWN").upper(),
                    "quantity": str(_to_decimal(row.get("quantity"))),
                    "expected_price": "",
                    "broker_order_id": _normalized_id(row.get("broker_order_id")),
                    "broker_perm_id": _normalized_id(row.get("broker_perm_id")),
                    "expected_time_utc": _first_text(row.get("submitted_at_utc"), default=""),
                    "origin": "expected_order_execution_status",
                }
            )
    rows.sort(key=lambda row: (row["attempt_id"], row["submission_id"], row["symbol"], row["side"], row["quantity"], row["origin"]))
    return rows


def _trade_sort_key(row: Mapping[str, Any], index: int) -> tuple[str, str, str, str, str, str, int]:
    return (
        _first_text(row.get("trade_time_utc"), default=""),
        _first_text(row.get("symbol"), default=""),
        _first_text(row.get("side"), default=""),
        _normalized_id(row.get("ib_perm_id")),
        _normalized_id(row.get("ib_order_id")),
        _first_text(row.get("quantity"), default=""),
        index,
    )


def match_expected_to_ib_v1(expected_activity: Mapping[str, Any], normalized_ib_trades: Mapping[str, Any]) -> dict[str, Any]:
    trades_raw = normalized_ib_trades.get("trades")
    trades: list[dict[str, Any]] = [dict(row) for row in trades_raw if isinstance(row, Mapping)] if isinstance(trades_raw, list) else []
    sorted_indices = sorted(range(len(trades)), key=lambda i: _trade_sort_key(trades[i], i))
    expected_rows = _build_expected_execution_rows(expected_activity)

    unmatched_trade_indices = set(sorted_indices)
    matched_items: list[dict[str, Any]] = []
    unmatched_expected: list[dict[str, Any]] = []
    match_seq = 1

    def select_candidate(expected: Mapping[str, Any], rule: str) -> int | None:
        candidates: list[int] = []
        for idx in sorted_indices:
            if idx not in unmatched_trade_indices:
                continue
            trade = trades[idx]
            trade_perm = _normalized_id(trade.get("ib_perm_id"))
            trade_order = _normalized_id(trade.get("ib_order_id"))
            expected_perm = _normalized_id(expected.get("broker_perm_id"))
            expected_order = _normalized_id(expected.get("broker_order_id"))

            if rule == RULE_PERM_ID_EXACT:
                if _is_nonzero_id(expected_perm) and _is_nonzero_id(trade_perm) and expected_perm == trade_perm:
                    candidates.append(idx)
                continue

            if rule == RULE_ORDER_ID_EXACT:
                if _is_nonzero_id(expected_order) and _is_nonzero_id(trade_order) and expected_order == trade_order:
                    candidates.append(idx)
                continue

            if rule == RULE_SUBMISSION_ID_REFERENCE:
                submission_id = _first_text(expected.get("submission_id"), default="")
                if submission_id and submission_id in _trade_submission_refs(trade):
                    candidates.append(idx)
                continue

            if rule == RULE_FALLBACK_SYMBOL_SIDE_QTY_TIME_WINDOW:
                if _first_text(expected.get("symbol")).upper() != _first_text(trade.get("symbol")).upper():
                    continue
                if _first_text(expected.get("side")).upper() != _first_text(trade.get("side")).upper():
                    continue
                if _to_decimal(expected.get("quantity")) != _to_decimal(trade.get("quantity")):
                    continue
                expected_time = _parse_utc(expected.get("expected_time_utc"))
                trade_time = _parse_utc(trade.get("trade_time_utc"))
                if expected_time is None or trade_time is None:
                    continue
                delta_minutes = abs((trade_time - expected_time).total_seconds()) / 60.0
                if delta_minutes <= FALLBACK_TIME_WINDOW_MINUTES:
                    candidates.append(idx)
                continue

        if not candidates:
            return None
        return sorted(candidates, key=lambda i: _trade_sort_key(trades[i], i))[0]

    for expected in expected_rows:
        matched_idx: int | None = None
        matched_rule = ""
        for rule in (
            RULE_PERM_ID_EXACT,
            RULE_ORDER_ID_EXACT,
            RULE_SUBMISSION_ID_REFERENCE,
            RULE_FALLBACK_SYMBOL_SIDE_QTY_TIME_WINDOW,
        ):
            candidate = select_candidate(expected, rule)
            if candidate is not None:
                matched_idx = candidate
                matched_rule = rule
                break

        if matched_idx is None:
            unmatched_expected.append(dict(expected))
            continue

        unmatched_trade_indices.remove(matched_idx)
        matched_trade = trades[matched_idx]
        confidence = LOW_CONFIDENCE if matched_rule == RULE_FALLBACK_SYMBOL_SIDE_QTY_TIME_WINDOW else HIGH_CONFIDENCE
        matched_items.append(
            {
                "match_id": f"MATCH-{match_seq:04d}",
                "match_rule": matched_rule,
                "confidence": confidence,
                "aegis_attempt_id": _first_text(expected.get("attempt_id"), default=""),
                "submission_id": _first_text(expected.get("submission_id"), default=""),
                "symbol": _first_text(expected.get("symbol"), default=""),
                "side": _first_text(expected.get("side"), default=""),
                "expected_quantity": str(_to_decimal(expected.get("quantity"))),
                "ib_quantity": str(_to_decimal(matched_trade.get("quantity"))),
                "expected_price": _first_text(expected.get("expected_price"), default=""),
                "ib_price": _first_text(matched_trade.get("price"), default=""),
                "expected_commission": _first_text(expected.get("expected_commission"), default=""),
                "ib_commission": _first_text(matched_trade.get("commission"), default=""),
                "broker_order_id": _normalized_id(expected.get("broker_order_id")),
                "broker_perm_id": _normalized_id(expected.get("broker_perm_id")),
                "ib_order_id": _normalized_id(matched_trade.get("ib_order_id")),
                "ib_perm_id": _normalized_id(matched_trade.get("ib_perm_id")),
                "ib_trade_time_utc": _first_text(matched_trade.get("trade_time_utc"), default=""),
                "ib_raw_ref": matched_trade.get("raw_ref"),
            }
        )
        match_seq += 1

    unmatched_ib_trades = [trades[idx] for idx in sorted(unmatched_trade_indices, key=lambda i: _trade_sort_key(trades[i], i))]

    return {
        "matched_items": matched_items,
        "unmatched_expected": unmatched_expected,
        "unmatched_ib_trades": unmatched_ib_trades,
    }
