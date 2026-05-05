from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
import json
import os
from typing import Any, Iterable


ORDER_CREATED = "ORDER_CREATED"
SUBMITTED_TO_BROKER = "SUBMITTED_TO_BROKER"
BROKER_ACKED = "BROKER_ACKED"
PARTIALLY_FILLED = "PARTIALLY_FILLED"
FILLED = "FILLED"
CANCELLED = "CANCELLED"
REJECTED = "REJECTED"
EXPIRED = "EXPIRED"
POSITION_RECONCILED = "POSITION_RECONCILED"
CASH_NAV_RECONCILED = "CASH_NAV_RECONCILED"
RISK_RECOMPUTED = "RISK_RECOMPUTED"
EOD_RECONCILED = "EOD_RECONCILED"
POST_TRADE_BLOCKED = "POST_TRADE_BLOCKED"

TERMINAL_CANCELLED = {"CANCELLED", "APICANCELLED", "API_CANCELLED"}
TERMINAL_REJECTED = {"REJECTED", "INACTIVE"}
TERMINAL_EXPIRED = {"EXPIRED"}
ACK_STATUSES = {"SUBMITTED", "PRESUBMITTED", "PENDINGSUBMIT", "API_PENDING", "ACKED"}


@dataclass(frozen=True)
class PostTradeLifecycleResultV1:
    lifecycle_report_path: Path
    reconciliation_report_path: Path
    order_lifecycle_paths: tuple[Path, ...]
    lifecycle_report: dict[str, Any]
    reconciliation_report: dict[str, Any]


def materialize_post_trade_lifecycle_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path,
    evaluation_utc: str | None = None,
    max_broker_event_age_seconds: int = 900,
) -> PostTradeLifecycleResultV1:
    day = _require_day(day_utc)
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve()
    evaluated = _iso(_parse_time(evaluation_utc) if evaluation_utc else datetime.now(UTC))

    submissions = _load_order_records(execution_root=execution_root, day_utc=day)
    broker_events = _load_broker_events(truth_root=truth_root, execution_root=execution_root, day_utc=day)
    normalized = [_normalize_broker_event(row) for row in broker_events]
    normalized = [row for row in normalized if row.get("event_type")]

    lifecycle_rows: list[dict[str, Any]] = []
    order_paths: list[Path] = []
    blockers: list[dict[str, Any]] = []
    for order in submissions:
        lifecycle = _evaluate_order(
            order=order,
            events=normalized,
            truth_root=truth_root,
            execution_root=execution_root,
            day_utc=day,
            evaluation_utc=evaluated,
            max_broker_event_age_seconds=max_broker_event_age_seconds,
        )
        path = execution_root / "order_lifecycle_v1" / day / f"{lifecycle['intent_id']}.order_lifecycle.v1.json"
        _atomic_write_json(path, lifecycle)
        order_paths.append(path)
        lifecycle_rows.append(lifecycle)
        if lifecycle.get("status") == "BLOCKED":
            blockers.append(
                {
                    "intent_id": lifecycle.get("intent_id"),
                    "blocker": lifecycle.get("first_blocker"),
                    "failed_field": lifecycle.get("failed_field"),
                    "operator_next_action": lifecycle.get("operator_next_action"),
                }
            )

    if not submissions:
        overall_status = "PASS"
        final_state = "NO_ORDERS"
        first_blocker = ""
        next_action = "No Aegis PAPER submission artifacts exist for this day."
    elif blockers:
        overall_status = "BLOCKED"
        final_state = POST_TRADE_BLOCKED
        first_blocker = str(blockers[0].get("blocker") or "POST_TRADE_BLOCKED")
        next_action = str(blockers[0].get("operator_next_action") or "Inspect broker evidence and rerun post-trade lifecycle.")
    elif any(row.get("final_state") in {BROKER_ACKED, PARTIALLY_FILLED, SUBMITTED_TO_BROKER} for row in lifecycle_rows):
        overall_status = "OPEN"
        final_state = "OPEN"
        first_blocker = ""
        next_action = "Continue observing broker order status, execution, position, account, NAV, and risk evidence."
    else:
        overall_status = "PASS"
        final_state = "EOD_RECONCILED" if any(row.get("final_state") == EOD_RECONCILED for row in lifecycle_rows) else "TERMINAL_RECONCILED"
        first_blocker = ""
        next_action = "No post-trade lifecycle blocker remains."

    lifecycle_report = {
        "schema_version": "aegis_post_trade_lifecycle.v1",
        "day_utc": day,
        "status": overall_status,
        "final_state": final_state,
        "first_blocker": first_blocker,
        "operator_next_action": next_action,
        "produced_at_utc": evaluated,
        "producer": "post_trade_lifecycle_v1",
        "truth_root": str(truth_root),
        "execution_root": str(execution_root),
        "order_count": len(lifecycle_rows),
        "broker_event_count": len(normalized),
        "orders": lifecycle_rows,
        "blockers": blockers,
        "evidence_paths": _evidence_paths(truth_root=truth_root, execution_root=execution_root, day_utc=day),
    }
    reconciliation_report = {
        "schema_version": "aegis_post_trade_reconciliation.v1",
        "day_utc": day,
        "status": "BLOCKED" if blockers else overall_status,
        "first_blocker": first_blocker,
        "operator_next_action": next_action,
        "produced_at_utc": evaluated,
        "producer": "post_trade_lifecycle_v1",
        "reconciliations": [
            {
                "intent_id": row.get("intent_id"),
                "final_state": row.get("final_state"),
                "filled_quantity": row.get("filled_quantity"),
                "position_reconciled": row.get("position_reconciled"),
                "cash_nav_reconciled": row.get("cash_nav_reconciled"),
                "risk_recomputed": row.get("risk_recomputed"),
                "blocker": row.get("first_blocker"),
            }
            for row in lifecycle_rows
        ],
        "blockers": blockers,
    }
    lifecycle_path = truth_root / "reports" / "post_trade_lifecycle_v1" / day / "post_trade_lifecycle.v1.json"
    reconciliation_path = truth_root / "reports" / "post_trade_reconciliation_v1" / day / "post_trade_reconciliation.v1.json"
    _atomic_write_json(lifecycle_path, lifecycle_report)
    _atomic_write_json(reconciliation_path, reconciliation_report)
    return PostTradeLifecycleResultV1(
        lifecycle_report_path=lifecycle_path,
        reconciliation_report_path=reconciliation_path,
        order_lifecycle_paths=tuple(order_paths),
        lifecycle_report=lifecycle_report,
        reconciliation_report=reconciliation_report,
    )


def _evaluate_order(
    *,
    order: dict[str, Any],
    events: list[dict[str, Any]],
    truth_root: Path,
    execution_root: Path,
    day_utc: str,
    evaluation_utc: str,
    max_broker_event_age_seconds: int,
) -> dict[str, Any]:
    matched = [event for event in events if _event_matches_order(event, order)]
    statuses = [event for event in matched if event["event_type"] == "ORDER_STATUS"]
    fills = [event for event in matched if event["event_type"] == "FILL"]
    opens = [event for event in matched if event["event_type"] == "ORDER_OPEN"]
    latest_status = statuses[-1] if statuses else {}
    status_text = str(latest_status.get("status") or order.get("broker_status") or "").strip().upper()
    expected_qty = _positive_decimal(order.get("quantity"), latest_status.get("total_quantity"), latest_status.get("filled"), latest_status.get("remaining"))
    filled_qty = sum(_number(event.get("filled_quantity") or event.get("quantity") or event.get("shares")) for event in fills)
    status_filled_qty = _number(latest_status.get("filled_quantity") or latest_status.get("filled"))
    remaining_qty = _number(latest_status.get("remaining_quantity") or latest_status.get("remaining"))
    state_path = [ORDER_CREATED]
    first_blocker = ""
    failed_field = ""
    expected_value: Any = ""
    actual_value: Any = ""
    operator_next_action = "Continue observing governed broker post-trade evidence."

    if _has_broker_identity(order):
        state_path.append(SUBMITTED_TO_BROKER)
    else:
        return _blocked_lifecycle(order, state_path, "BROKER_ORDER_ID_MISSING", "broker_ids.order_id/perm_id", "broker order id or permId", None, "Inspect submit evidence; broker state is required before lifecycle can advance.", matched)

    stale = _stale_event_blocker(matched, evaluation_utc, max_broker_event_age_seconds)
    if stale:
        return _blocked_lifecycle(order, state_path, "STALE_BROKER_EVENT", "broker_events.latest_received_utc", f"<= {max_broker_event_age_seconds}s old", stale, "Refresh broker observer/fact spine evidence, then rerun post-trade lifecycle.", matched)

    if not opens and not statuses:
        return _blocked_lifecycle(order, state_path, "BROKER_ACK_MISSING", "broker_order_status", "openOrder or orderStatus broker evidence", "MISSING", "Wait for or refresh broker order status events; do not infer broker acknowledgement.", matched)

    state_path.append(BROKER_ACKED)
    if status_text in TERMINAL_REJECTED:
        return _terminal_lifecycle(order, matched, state_path + [REJECTED], REJECTED, status_text, _reject_reason(latest_status))
    if status_text in TERMINAL_CANCELLED:
        return _terminal_lifecycle(order, matched, state_path + [CANCELLED], CANCELLED, status_text, _reject_reason(latest_status))
    if status_text in TERMINAL_EXPIRED:
        return _terminal_lifecycle(order, matched, state_path + [EXPIRED], EXPIRED, status_text, _reject_reason(latest_status))

    if status_text == "FILLED" and not fills:
        return _blocked_lifecycle(order, state_path, "FILL_EVIDENCE_MISSING", "broker_execution_fill_events", "execDetails/fill evidence", "MISSING", "Refresh broker execution/fill evidence; never infer fills from order status alone.", matched)

    if fills:
        if status_text == "FILLED" and expected_qty > 0 and filled_qty < expected_qty:
            return _blocked_lifecycle(order, state_path, "RECONCILIATION_BLOCKED", "filled_quantity", expected_qty, filled_qty, "Resolve broker fill/status quantity mismatch before downstream reconciliation.", matched)
        if expected_qty > 0 and filled_qty < expected_qty:
            state_path.append(PARTIALLY_FILLED)
            return _open_lifecycle(order, matched, state_path, PARTIALLY_FILLED, filled_qty)
        state_path.append(FILLED)
    elif status_filled_qty > 0 and remaining_qty > 0:
        return _blocked_lifecycle(order, state_path, "FILL_EVIDENCE_MISSING", "broker_execution_fill_events", "execDetails/fill evidence for partial fill", "MISSING", "Refresh broker execution/fill evidence; never infer partial fills from status alone.", matched)
    else:
        return _open_lifecycle(order, matched, state_path, BROKER_ACKED, filled_qty)

    if status_filled_qty and abs(status_filled_qty - filled_qty) > 0:
        return _blocked_lifecycle(order, state_path, "RECONCILIATION_BLOCKED", "filled_quantity", status_filled_qty, filled_qty, "Resolve broker fill/status quantity mismatch before downstream reconciliation.", matched)

    position_result = _reconcile_position(order=order, events=events, filled_qty=filled_qty)
    if not position_result["ok"]:
        return _blocked_lifecycle(order, state_path, position_result["blocker"], "broker_positions.quantity", position_result["expected"], position_result["actual"], "Refresh broker positions and rerun post-trade lifecycle.", matched)
    state_path.append(POSITION_RECONCILED)

    cash_nav_result = _reconcile_cash_nav(truth_root=truth_root, execution_root=execution_root, day_utc=day_utc)
    if not cash_nav_result["ok"]:
        return _blocked_lifecycle(order, state_path, cash_nav_result["blocker"], cash_nav_result["failed_field"], cash_nav_result["expected"], cash_nav_result["actual"], "Refresh broker account summary, accounting NAV, and cash ledger, then rerun post-trade lifecycle.", matched)
    state_path.append(CASH_NAV_RECONCILED)

    risk_result = _risk_recomputed(execution_root=execution_root, day_utc=day_utc)
    if not risk_result["ok"]:
        return _blocked_lifecycle(order, state_path, "RISK_RECOMPUTE_MISSING", "risk_budget_supply.status", "PASS", risk_result["actual"], "Run governed risk/exposure recomputation after fill reconciliation.", matched)
    state_path.extend([RISK_RECOMPUTED, EOD_RECONCILED])

    return {
        **_base_lifecycle(order, matched),
        "status": "PASS",
        "final_state": EOD_RECONCILED,
        "state_path": state_path,
        "filled_quantity": float(filled_qty),
        "position_reconciled": True,
        "cash_nav_reconciled": True,
        "risk_recomputed": True,
        "first_blocker": first_blocker,
        "failed_field": failed_field,
        "expected_value": expected_value,
        "actual_value": actual_value,
        "operator_next_action": operator_next_action,
    }


def _blocked_lifecycle(order: dict[str, Any], state_path: list[str], blocker: str, failed_field: str, expected: Any, actual: Any, next_action: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        **_base_lifecycle(order, events),
        "status": "BLOCKED",
        "final_state": POST_TRADE_BLOCKED,
        "state_path": state_path + [POST_TRADE_BLOCKED],
        "filled_quantity": sum(float(_number(event.get("filled_quantity") or event.get("quantity") or event.get("shares"))) for event in events if event.get("event_type") == "FILL"),
        "position_reconciled": False,
        "cash_nav_reconciled": False,
        "risk_recomputed": False,
        "first_blocker": blocker,
        "failed_field": failed_field,
        "expected_value": expected,
        "actual_value": actual,
        "operator_next_action": next_action,
    }


def _open_lifecycle(order: dict[str, Any], events: list[dict[str, Any]], state_path: list[str], final_state: str, filled_qty: float) -> dict[str, Any]:
    return {
        **_base_lifecycle(order, events),
        "status": "OPEN",
        "final_state": final_state,
        "state_path": state_path,
        "filled_quantity": float(filled_qty),
        "position_reconciled": False,
        "cash_nav_reconciled": False,
        "risk_recomputed": False,
        "first_blocker": "",
        "failed_field": "",
        "expected_value": "",
        "actual_value": "",
        "operator_next_action": "Order remains open; continue observing broker status and fill events.",
    }


def _terminal_lifecycle(order: dict[str, Any], events: list[dict[str, Any]], state_path: list[str], final_state: str, broker_status: str, reason: str) -> dict[str, Any]:
    return {
        **_base_lifecycle(order, events),
        "status": "PASS",
        "final_state": final_state,
        "state_path": state_path,
        "broker_terminal_status": broker_status,
        "broker_reason": reason,
        "filled_quantity": 0.0,
        "position_reconciled": False,
        "cash_nav_reconciled": False,
        "risk_recomputed": False,
        "first_blocker": "",
        "failed_field": "",
        "expected_value": "",
        "actual_value": "",
        "operator_next_action": "Terminal broker state recorded; no fill reconciliation is required unless broker later emits fill evidence.",
    }


def _base_lifecycle(order: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "order_lifecycle.v1",
        "day_utc": order["day_utc"],
        "intent_id": order["intent_id"],
        "submission_id": order.get("submission_id", ""),
        "broker_order_id": order.get("broker_order_id"),
        "client_id": order.get("client_id"),
        "perm_id": order.get("perm_id"),
        "symbol": order.get("symbol", ""),
        "side": order.get("side", ""),
        "expected_quantity": order.get("quantity"),
        "broker_state_authoritative": True,
        "matched_broker_event_count": len(events),
        "matched_broker_events": events,
        "source_artifact": order.get("source_artifact", ""),
    }


def _load_order_records(*, execution_root: Path, day_utc: str) -> list[dict[str, Any]]:
    root = execution_root / "execution_evidence_v1" / "submissions" / day_utc
    orders: list[dict[str, Any]] = []
    for path in sorted(root.glob("*/broker_submission_record.v2.json")):
        payload = _read_json(path)
        submission_id = str(payload.get("submission_id") or path.parent.name).strip()
        broker_ids = payload.get("broker_ids") if isinstance(payload.get("broker_ids"), dict) else {}
        order = {
            "day_utc": day_utc,
            "intent_id": str(payload.get("intent_id") or payload.get("intent_hash") or payload.get("source_intent_id") or submission_id).strip(),
            "submission_id": submission_id,
            "broker_order_id": _first(payload.get("broker_order_id"), payload.get("order_id"), broker_ids.get("order_id")),
            "client_id": _first(payload.get("client_id"), broker_ids.get("client_id")),
            "perm_id": _first(payload.get("perm_id"), broker_ids.get("perm_id")),
            "broker_status": str(payload.get("status") or "").strip(),
            "symbol": str(payload.get("symbol") or _nested(payload, "contract", "symbol") or "").strip(),
            "side": str(payload.get("side") or payload.get("action") or "").strip().upper(),
            "quantity": _number(payload.get("quantity") or payload.get("total_quantity") or payload.get("totalQuantity")),
            "source_artifact": str(path.resolve()),
        }
        package = _read_json_optional(execution_root / "execution_package_v1" / day_utc / submission_id / "execution_package.v1.json")
        if package:
            order["intent_id"] = str(package.get("intent_id") or package.get("intent_hash") or order["intent_id"]).strip()
            order["symbol"] = order["symbol"] or str(package.get("symbol") or _nested(package, "order", "symbol") or "").strip()
            order["side"] = order["side"] or str(package.get("side") or _nested(package, "order", "action") or "").strip().upper()
            order["quantity"] = order["quantity"] or _number(package.get("quantity") or _nested(package, "order", "quantity"))
        orders.append(order)
    return orders


def _load_broker_events(*, truth_root: Path, execution_root: Path, day_utc: str) -> list[dict[str, Any]]:
    paths = [
        execution_root / "broker_fact_spine_v1" / "fact_ledger" / day_utc / "observed_order_fact.v1.jsonl",
        execution_root / "broker_fact_spine_v1" / "fact_ledger" / day_utc / "observed_order_status_fact.v1.jsonl",
        execution_root / "broker_fact_spine_v1" / "fact_ledger" / day_utc / "observed_fill_fact.v1.jsonl",
        execution_root / "broker_fact_spine_v1" / "fact_ledger" / day_utc / "observed_position_fact.v1.jsonl",
        execution_root / "execution_evidence_v1" / "broker_events" / day_utc / "broker_event_log.v1.jsonl",
        truth_root / "execution_evidence_v1" / "broker_events" / day_utc / "broker_event_log.v1.jsonl",
    ]
    rows: list[dict[str, Any]] = []
    for path in paths:
        for row in _read_jsonl(path):
            row["_source_path"] = str(path.resolve())
            rows.append(row)
    rows.sort(key=lambda row: str(row.get("observed_utc") or row.get("received_utc") or row.get("event_utc") or ""))
    return rows


def _normalize_broker_event(row: dict[str, Any]) -> dict[str, Any]:
    flat = _flat_fields(row)
    raw_type = str(row.get("source_event_type") or row.get("event_type") or row.get("schema_id") or "").strip()
    raw_upper = raw_type.upper()
    event_type = ""
    if raw_upper in {"OPENORDER", "OBSERVED_ORDER_FACT"} or raw_type == "observed_order_fact":
        event_type = "ORDER_OPEN"
    elif raw_upper in {"ORDERSTATUS", "OBSERVED_ORDER_STATUS_FACT"} or raw_type == "observed_order_status_fact":
        event_type = "ORDER_STATUS"
    elif raw_upper in {"EXECDETAILS", "OBSERVED_FILL_FACT"} or raw_type == "observed_fill_fact":
        event_type = "FILL"
    elif raw_upper in {"POSITION", "OBSERVED_POSITION_FACT"} or raw_type == "observed_position_fact":
        event_type = "POSITION"
    return {
        "event_type": event_type,
        "source_event_type": raw_type,
        "received_utc": str(row.get("observed_utc") or row.get("received_utc") or row.get("event_utc") or flat.get("receivedutc") or "").strip(),
        "order_id": _first(flat.get("orderid"), flat.get("brokerorderid")),
        "client_id": _first(flat.get("clientid")),
        "perm_id": _first(flat.get("permid"), flat.get("permanentid")),
        "exec_id": _first(flat.get("execid"), flat.get("executionid")),
        "status": str(_first(flat.get("status"), flat.get("orderstate")) or "").strip(),
        "filled_quantity": _number(_first(flat.get("filled"), flat.get("filledquantity"), flat.get("shares"), flat.get("quantity"))),
        "remaining_quantity": _number(_first(flat.get("remaining"), flat.get("remainingquantity"))),
        "total_quantity": _number(_first(flat.get("totalquantity"), flat.get("quantity"))),
        "price": _number(_first(flat.get("price"), flat.get("avgfillprice"), flat.get("lastfillprice"))),
        "symbol": str(_first(flat.get("symbol"), flat.get("localsymbol")) or "").strip(),
        "side": str(_first(flat.get("side"), flat.get("action")) or "").strip().upper(),
        "position_quantity": _number(_first(flat.get("position"), flat.get("positionquantity"))),
        "account": str(_first(flat.get("account"), flat.get("accountid")) or "").strip(),
        "reason": str(_first(flat.get("whyheld"), flat.get("reason"), flat.get("message")) or "").strip(),
        "source_path": str(row.get("_source_path") or ""),
    }


def _flat_fields(value: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(value, dict):
        args = _nested(value, "ib_fields", "args")
        if isinstance(args, list):
            for item in args:
                raw = item.get("value") if isinstance(item, dict) else item
                if isinstance(raw, str) and "=" in raw:
                    key, val = raw.split("=", 1)
                    out[_norm_key(key)] = val
        for key, item in value.items():
            if key in {"ib_fields"}:
                continue
            out.update(_flat_fields(item, prefix + _norm_key(key)))
            if not isinstance(item, (dict, list)):
                out[_norm_key(key)] = item
    elif isinstance(value, list):
        for item in value:
            out.update(_flat_fields(item, prefix))
    return out


def _event_matches_order(event: dict[str, Any], order: dict[str, Any]) -> bool:
    for event_key, order_key in (("order_id", "broker_order_id"), ("perm_id", "perm_id")):
        if _id_text(event.get(event_key)) and _id_text(event.get(event_key)) == _id_text(order.get(order_key)):
            return True
    client_match = _id_text(event.get("client_id")) and _id_text(event.get("client_id")) == _id_text(order.get("client_id"))
    symbol_match = event.get("symbol") and order.get("symbol") and str(event["symbol"]).upper() == str(order["symbol"]).upper()
    return bool(client_match and symbol_match)


def _has_broker_identity(order: dict[str, Any]) -> bool:
    return bool(_id_text(order.get("broker_order_id")) or _id_text(order.get("perm_id")))


def _stale_event_blocker(events: list[dict[str, Any]], evaluation_utc: str, max_age: int) -> str:
    if not events:
        return ""
    latest_text = max((str(event.get("received_utc") or "") for event in events), default="")
    try:
        latest = _parse_time(latest_text)
        evaluated = _parse_time(evaluation_utc)
    except Exception:
        return latest_text or "UNKNOWN"
    if evaluated - latest > timedelta(seconds=max_age):
        return latest_text
    return ""


def _reconcile_position(*, order: dict[str, Any], events: list[dict[str, Any]], filled_qty: float) -> dict[str, Any]:
    symbol = str(order.get("symbol") or "").upper()
    positions = [event for event in events if event.get("event_type") == "POSITION" and str(event.get("symbol") or "").upper() == symbol]
    if not positions:
        return {"ok": False, "blocker": "POSITION_RECONCILIATION_FAILED", "expected": _signed_quantity(order, filled_qty), "actual": "MISSING"}
    actual = float(positions[-1].get("position_quantity") or 0)
    expected = _signed_quantity(order, filled_qty)
    if abs(actual - expected) > 0.0001:
        return {"ok": False, "blocker": "POSITION_RECONCILIATION_FAILED", "expected": expected, "actual": actual}
    return {"ok": True}


def _reconcile_cash_nav(*, truth_root: Path, execution_root: Path, day_utc: str) -> dict[str, Any]:
    nav = _read_json_optional(execution_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json") or _read_json_optional(truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json") or {}
    nav_body = nav.get("nav") if isinstance(nav.get("nav"), dict) else nav
    nav_total = _number(nav_body.get("nav_total_cents") or nav_body.get("nav_total"))
    cash_total = _number(nav_body.get("cash_total_cents") or nav_body.get("cash_total"))
    if nav_total <= 0:
        return {"ok": False, "blocker": "CASH_NAV_RECONCILIATION_FAILED", "failed_field": "nav.nav_total", "expected": "> 0", "actual": nav_total}
    broker = _read_json_optional(execution_root / "reports" / "broker_supply_v1" / day_utc / "broker_supply.v1.json") or {}
    values = broker.get("account_values") if isinstance(broker.get("account_values"), dict) else {}
    broker_cash = _number(values.get("total_cash_value_cents"))
    if broker_cash > 0 and abs(broker_cash - cash_total) > 100:
        return {"ok": False, "blocker": "CASH_NAV_RECONCILIATION_FAILED", "failed_field": "cash_total_cents", "expected": broker_cash, "actual": cash_total}
    return {"ok": True}


def _risk_recomputed(*, execution_root: Path, day_utc: str) -> dict[str, Any]:
    candidates = [
        execution_root / "reports" / "risk_budget_supply_v1" / day_utc / "risk_budget_supply.v1.json",
        execution_root / "reports" / "exposure_reconciliation_v1" / day_utc / "exposure_reconciliation.v1.json",
        execution_root / "reports" / "exposure_reconciliation_v2" / day_utc / "exposure_reconciliation.v2.json",
    ]
    for path in candidates:
        payload = _read_json_optional(path)
        if payload and str(payload.get("status") or payload.get("final_status") or "").upper() in {"PASS", "OK", "READY"}:
            return {"ok": True}
    return {"ok": False, "actual": "MISSING_OR_NOT_PASS"}


def _evidence_paths(*, truth_root: Path, execution_root: Path, day_utc: str) -> list[str]:
    return [
        str(execution_root / "execution_evidence_v1" / "submissions" / day_utc),
        str(execution_root / "broker_fact_spine_v1" / "fact_ledger" / day_utc),
        str(execution_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json"),
        str(execution_root / "reports" / "risk_budget_supply_v1" / day_utc / "risk_budget_supply.v1.json"),
        str(truth_root / "reports" / "post_trade_lifecycle_v1" / day_utc / "post_trade_lifecycle.v1.json"),
    ]


def _read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return data


def _read_json_optional(path: Path) -> dict[str, Any] | None:
    try:
        if path.exists() and path.is_file():
            return _read_json(path)
    except Exception:
        return None
    return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except Exception:
            continue
        if isinstance(data, dict):
            rows.append(data)
    return rows


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _nested(data: Any, *keys: str) -> Any:
    cur = data
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _number(value: Any) -> float:
    try:
        if isinstance(value, bool) or value is None or value == "":
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _positive_decimal(*values: Any) -> float:
    for value in values:
        number = _number(value)
        if number > 0:
            return number
    return 0.0


def _signed_quantity(order: dict[str, Any], quantity: float) -> float:
    side = str(order.get("side") or "").upper()
    if side in {"SELL", "SLD"}:
        return -float(quantity)
    return float(quantity)


def _id_text(value: Any) -> str:
    text = str(value or "").strip()
    if text in {"0", "0.0", "None", "null"}:
        return ""
    return text


def _norm_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _reject_reason(status_event: dict[str, Any]) -> str:
    return str(status_event.get("reason") or status_event.get("status") or "").strip()


def _require_day(value: str) -> str:
    text = str(value or "").strip()
    datetime.fromisoformat(text)
    if len(text) != 10:
        raise ValueError("day_utc must be YYYY-MM-DD")
    return text


def _parse_time(value: str | None) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("UTC timestamp missing")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
