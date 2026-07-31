from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from ops.aegis.market_data.symbol_alias_registry_v1 import normalize_market_symbol_v1


LEDGER_SCHEMA_ID = "trade_lifecycle_ledger"
PROJECTION_SCHEMA_ID = "paper_trade_evaluation_projection"
SCHEMA_VERSION = "v1"
LEDGER_FAMILY = "trade_lifecycle_ledger_v1"
PROJECTION_FAMILY = "paper_trade_evaluation_projection_v1"

SAFETY_FLAGS = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _artifact_hash(path: Path, payload: Mapping[str, Any] | None = None) -> str:
    if isinstance(payload, Mapping):
        for key in ("content_hash", "history_hash", "evidence_hash", "outcome_hash", "projection_hash", "source_fingerprint"):
            value = str(payload.get(key) or "")
            if value:
                return value
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return stable_hash_v1(payload or {})


def _day_from_path(path: Path, family: str) -> str:
    parts = list(path.parts)
    try:
        index = parts.index(family)
        return parts[index + 1]
    except (ValueError, IndexError):
        return ""


def _family_files(root: Path, family: str, filename: str) -> Iterable[Path]:
    base = root / "reports" / family
    if not base.exists():
        return []
    return sorted(base.glob(f"**/{filename}"))


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _coerce_quantity(value: Any) -> float | None:
    number = _coerce_float(value)
    if number is None:
        return None
    return abs(number)


def _trade_id_from(row: Mapping[str, Any]) -> str:
    for key in (
        "trade_id",
        "ticket_id",
        "capture_ticket_id",
        "manual_receipt_id",
        "receipt_id",
        "recommended_trade_id",
        "selected_exposure_intent_id",
        "intent_id",
        "candidate_id",
        "capture_record_id",
    ):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    symbol = str(row.get("symbol") or "").upper()
    day = str(row.get("day_utc") or row.get("trading_session") or row.get("operational_day") or "")
    if symbol and day:
        return f"trade:{day}:{symbol}"
    return ""


def _event_time(row: Mapping[str, Any]) -> str:
    for key in (
        "event_time",
        "captured_at_utc",
        "fill_time",
        "execution_time",
        "timestamp",
        "exit_time",
        "entry_time",
        "created_at_utc",
        "generated_at_utc",
        "generated_at",
        "produced_at_utc",
        "day_utc",
    ):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _normalize_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"BUY", "LONG"}:
        return "BUY"
    if side in {"SELL", "SHORT"}:
        return "SELL"
    return side


def _signed_multiplier(side: str) -> int:
    return -1 if _normalize_side(side) in {"SELL", "SHORT"} else 1


def _ledger_row(
    *,
    event_type: str,
    source_path: Path,
    source_payload: Mapping[str, Any],
    source_day: str,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    merged = {**dict(source_payload), **dict(overrides or {})}
    trade_id = _trade_id_from(merged)
    symbol = str(merged.get("symbol") or merged.get("ticker") or "").upper()
    row = {
        "trade_lifecycle_id": "",
        "trade_id": trade_id,
        "symbol": symbol,
        "side": _normalize_side(merged.get("side") or merged.get("direction")),
        "quantity": merged.get("quantity") or merged.get("actual_quantity") or merged.get("suggested_quantity"),
        "sleeve_id": str(merged.get("sleeve_id") or merged.get("sleeve") or ""),
        "hypothesis_id": str(merged.get("hypothesis_id") or merged.get("source_hypothesis_id") or ""),
        "sector": str(merged.get("sector") or merged.get("gics_sector") or merged.get("market_sector") or ""),
        "setup_type": str(merged.get("setup_type") or merged.get("strategy_setup") or ""),
        "asset_class": str(merged.get("asset_class") or merged.get("instrument_asset_class") or ""),
        "factor_tags": merged.get("factor_tags") or merged.get("factors") or [],
        "regime_dependency": merged.get("regime_dependency") or merged.get("regime_dependencies") or merged.get("regime") or [],
        "candidate_id": str(merged.get("candidate_id") or ""),
        "recommendation_id": str(merged.get("recommendation_id") or merged.get("recommended_trade_id") or ""),
        "capture_ticket_id": str(merged.get("capture_ticket_id") or merged.get("ticket_id") or ""),
        "manual_receipt_id": str(merged.get("manual_receipt_id") or merged.get("receipt_id") or ""),
        "event_type": event_type,
        "event_time": _event_time(merged),
        "operational_day": str(merged.get("day_utc") or source_day),
        "source_artifact": str(source_path),
        "content_hash": _artifact_hash(source_path, source_payload),
        "entry_price": merged.get("entry_price") or merged.get("actual_fill") or merged.get("fill_price") or merged.get("price") or merged.get("recommended_entry"),
        "exit_price": merged.get("exit_price"),
        "current_mark": merged.get("current_mark"),
        "stop_price": merged.get("stop_price") or merged.get("current_stop") or merged.get("current_stop_price"),
        "target_price": merged.get("target_price"),
        "time_stop_at": merged.get("time_stop_at"),
        "holding_days": merged.get("holding_days") or merged.get("holding_time_days") or merged.get("days_held"),
        "return_pct": merged.get("return_pct"),
        "realized_pnl": merged.get("realized_pnl"),
        "unrealized_pnl": merged.get("unrealized_pnl"),
        "max_favorable_excursion": merged.get("max_favorable_excursion"),
        "max_adverse_excursion": merged.get("max_adverse_excursion"),
        "lifecycle_status": str(merged.get("lifecycle_status") or merged.get("outcome_status") or merged.get("status") or ""),
    }
    row["trade_lifecycle_id"] = f"{LEDGER_FAMILY}:{stable_hash_v1({k: v for k, v in row.items() if k != 'trade_lifecycle_id'})[:24]}"
    return row


def _manual_capture_rows(root: Path, day_utc: str, *, lookback_days: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    base = root / "reports" / "manual_capture_record_v1"
    if not base.exists():
        return rows
    for path in sorted(base.glob("*/manual_capture_record.v1.jsonl"))[-lookback_days:]:
        source_day = path.parent.name
        for record in _read_jsonl(path):
            status = str(record.get("capture_status") or "").lower()
            if status and status not in {"captured_manually", "partial"}:
                continue
            rows.append(_ledger_row(event_type="MANUAL_CAPTURE_RECORDED", source_path=path, source_payload=record, source_day=source_day))
    return rows


def _manual_trade_receipt_rows(root: Path, *, lookback_days: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    base = root / "manual_trade_receipts"
    if not base.exists():
        return rows
    day_dirs = sorted([path for path in base.iterdir() if path.is_dir()])[-lookback_days:]
    for day_dir in day_dirs:
        for path in sorted(day_dir.glob("*.manual_trade_receipt.v1.json")):
            receipt = _read_json(path)
            if not receipt or str(receipt.get("validation_status") or "").upper() != "VALID":
                continue
            rows.append(_ledger_row(event_type="MANUAL_RECEIPT_RECORDED", source_path=path, source_payload=receipt, source_day=day_dir.name))
    return rows


def _final_eod_artifact_path(root: Path, day_utc: str) -> Path:
    pointer = root / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.v1.json"
    pointer_payload = _read_json(pointer)
    current = str(pointer_payload.get("current_artifact_path") or "")
    if current:
        path = Path(current).expanduser()
        if path.exists():
            return path
    return pointer


def latest_certified_marks_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve()
    path = _final_eod_artifact_path(root, str(day_utc))
    payload = _read_json(path)
    candidates: list[Any] = []
    for key in ("normalized_records", "rows", "records", "symbol_rows", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    symbols = payload.get("symbols")
    if isinstance(symbols, dict):
        candidates.extend(symbols.values())
    elif isinstance(symbols, list):
        candidates.extend(symbols)
    marks: dict[str, dict[str, Any]] = {}
    for item in candidates:
        if not isinstance(item, Mapping):
            continue
        symbol = normalize_market_symbol_v1(item.get("canonical_symbol") or item.get("symbol") or item.get("ticker") or "")
        close = _coerce_float(item.get("close") or item.get("last") or item.get("current_mark"))
        if not symbol or close is None:
            continue
        marks[symbol] = {
            "symbol": symbol,
            "current_mark": close,
            "trading_day": str(item.get("date") or item.get("market_session_date") or payload.get("day_utc") or day_utc),
            "source_artifact": str(path),
            "content_hash": _artifact_hash(path, payload),
            "provider": str(item.get("provider") or payload.get("source") or ""),
        }
    return marks


def build_trade_lifecycle_ledger_v1(*, truth_root: Path | str, day_utc: str, lookback_days: int = 120) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    rows: list[dict[str, Any]] = []
    source_artifacts: list[str] = []

    for path in _family_files(root, "captured_ticket_history_v1", "captured_ticket_history.v1.json"):
        payload = _read_json(path)
        if not payload:
            continue
        source_day = _day_from_path(path, "captured_ticket_history_v1")
        source_artifacts.append(str(path))
        rows.append(_ledger_row(event_type="CAPTURE_TICKET_CREATED", source_path=path, source_payload=payload, source_day=source_day))
        if payload.get("fill_price") or payload.get("quantity"):
            rows.append(_ledger_row(event_type="OPEN_POSITION_ESTABLISHED", source_path=path, source_payload=payload, source_day=source_day))

    manual_rows = _manual_capture_rows(root, day_utc, lookback_days=lookback_days)
    rows.extend(manual_rows)
    receipt_rows = _manual_trade_receipt_rows(root, lookback_days=lookback_days)
    rows.extend(receipt_rows)

    for path in _family_files(root, "manual_execution_receipt_v1", "manual_execution_receipt.v1.json"):
        payload = _read_json(path)
        if not payload:
            continue
        source_day = _day_from_path(path, "manual_execution_receipt_v1")
        source_artifacts.append(str(path))
        if payload.get("manual_fill_present") is True or payload.get("fill_details_present") is True:
            rows.append(_ledger_row(event_type="MANUAL_RECEIPT_RECORDED", source_path=path, source_payload=payload, source_day=source_day))

    for path in _family_files(root, "trade_outcome_v1", "trade_outcome.v1.json"):
        payload = _read_json(path)
        if not payload:
            continue
        source_day = _day_from_path(path, "trade_outcome_v1")
        source_artifacts.append(str(path))
        rows.append(_ledger_row(event_type="OUTCOME_FINALIZED", source_path=path, source_payload=payload, source_day=source_day))

    for path in _family_files(root, "sleeve_performance_report_v1", "sleeve_performance_report.v1.json"):
        payload = _read_json(path)
        if not payload:
            continue
        source_day = _day_from_path(path, "sleeve_performance_report_v1")
        source_artifacts.append(str(path))
        for row in payload.get("trade_lifecycle_rows") or []:
            if not isinstance(row, Mapping):
                continue
            status = str(row.get("lifecycle_status") or "")
            event_type = "RECOMMENDATION_CREATED"
            if status == "EXECUTED_OPEN":
                event_type = "OPEN_POSITION_ESTABLISHED"
            elif status == "EXECUTED_CLOSED":
                event_type = "OUTCOME_FINALIZED"
            elif status == "MISSING_RECEIPT":
                event_type = "CAPTURE_TICKET_CREATED"
            rows.append(_ledger_row(event_type=event_type, source_path=path, source_payload=payload, source_day=source_day, overrides=row))

    rows = [row for row in rows if row.get("trade_id") and row.get("symbol")]
    rows = sorted(rows, key=lambda row: (str(row.get("event_time") or ""), str(row.get("trade_id") or ""), str(row.get("event_type") or ""), str(row.get("trade_lifecycle_id") or "")))
    payload = {
        "schema_id": LEDGER_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": str(day_utc),
        "generated_at_utc": utc_now_v1(),
        "row_count": len(rows),
        "rows": rows,
        "source_artifacts": sorted(set(source_artifacts + [str(row.get("source_artifact") or "") for row in rows if row.get("source_artifact")])),
        **SAFETY_FLAGS,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def _trade_shell(trade_id: str) -> dict[str, Any]:
    return {
        "trade_id": trade_id,
        "symbol": "",
        "side": "",
        "quantity": None,
        "sleeve_id": "",
        "hypothesis_id": "",
        "sector": "",
        "setup_type": "",
        "asset_class": "",
        "factor_tags": [],
        "regime_dependency": [],
        "candidate_id": "",
        "recommendation_id": "",
        "capture_ticket_id": "",
        "manual_receipt_id": "",
        "entry_price": None,
        "exit_price": None,
        "current_mark": None,
        "current_stop": None,
        "target_price": None,
        "time_stop_at": "",
        "holding_days": None,
        "realized_pnl": None,
        "unrealized_pnl": None,
        "total_pnl": None,
        "return_pct": None,
        "MFE": None,
        "MAE": None,
        "status": "NOT_EVALUABLE",
        "evaluation_summary": "NOT_EVALUABLE",
        "evidence_status": "MISSING_ATTRIBUTION",
        "next_action": "Reconcile trade state",
        "next_action_command": "RECONCILE_TRADE_STATE",
        "source_artifacts": [],
        "lifecycle_events": [],
    }


def _merge_trade_event(trade: dict[str, Any], row: Mapping[str, Any]) -> None:
    trade["lifecycle_events"].append(dict(row))
    trade["source_artifacts"] = sorted(set([*trade.get("source_artifacts", []), str(row.get("source_artifact") or "")]))
    for key in ("symbol", "side", "sleeve_id", "hypothesis_id", "sector", "setup_type", "asset_class", "candidate_id", "recommendation_id", "capture_ticket_id", "manual_receipt_id"):
        if not trade.get(key) and row.get(key):
            trade[key] = row.get(key)
    for key in ("factor_tags", "regime_dependency"):
        if not trade.get(key) and row.get(key):
            trade[key] = row.get(key)
    quantity = _coerce_quantity(row.get("quantity"))
    if trade.get("quantity") is None and quantity is not None:
        trade["quantity"] = quantity
    entry = _coerce_float(row.get("entry_price"))
    if trade.get("entry_price") is None and entry is not None:
        trade["entry_price"] = entry
    exit_price = _coerce_float(row.get("exit_price"))
    if trade.get("exit_price") is None and exit_price is not None:
        trade["exit_price"] = exit_price
    current_mark = _coerce_float(row.get("current_mark"))
    if trade.get("current_mark") is None and current_mark is not None:
        trade["current_mark"] = current_mark
    stop_price = _coerce_float(row.get("current_stop") or row.get("stop_price") or row.get("current_stop_price"))
    if trade.get("current_stop") is None and stop_price is not None:
        trade["current_stop"] = stop_price
    target_price = _coerce_float(row.get("target_price"))
    if trade.get("target_price") is None and target_price is not None:
        trade["target_price"] = target_price
    if not trade.get("time_stop_at") and row.get("time_stop_at"):
        trade["time_stop_at"] = row.get("time_stop_at")
    holding_days = _coerce_float(row.get("holding_days"))
    if trade.get("holding_days") is None and holding_days is not None:
        trade["holding_days"] = holding_days
    for source, target in (("realized_pnl", "realized_pnl"), ("unrealized_pnl", "unrealized_pnl"), ("return_pct", "return_pct"), ("max_favorable_excursion", "MFE"), ("max_adverse_excursion", "MAE")):
        number = _coerce_float(row.get(source))
        if trade.get(target) is None and number is not None:
            trade[target] = number


def _finalize_trade(trade: dict[str, Any], marks: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    events = {str(row.get("event_type") or "") for row in trade.get("lifecycle_events", [])}
    symbol = str(trade.get("symbol") or "").upper()
    entry = _coerce_float(trade.get("entry_price"))
    quantity = _coerce_quantity(trade.get("quantity"))
    side = _normalize_side(trade.get("side"))
    if trade.get("exit_price") is None and any(event == "OUTCOME_FINALIZED" for event in events):
        for row in trade.get("lifecycle_events", []):
            exit_price = _coerce_float(row.get("exit_price"))
            if exit_price is not None:
                trade["exit_price"] = exit_price
                break
    if trade.get("current_mark") is None and symbol in marks:
        trade["current_mark"] = marks[symbol].get("current_mark")
        trade["mark_source_artifact"] = marks[symbol].get("source_artifact")
        trade["mark_content_hash"] = marks[symbol].get("content_hash")
    exit_price = _coerce_float(trade.get("exit_price"))
    mark = _coerce_float(trade.get("current_mark"))
    if exit_price is not None and entry is not None and quantity is not None:
        trade["realized_pnl"] = (exit_price - entry) * quantity * _signed_multiplier(side)
        trade["return_pct"] = ((exit_price - entry) / entry) * 100 * _signed_multiplier(side) if entry else None
    if exit_price is None and mark is not None and entry is not None and quantity is not None:
        trade["unrealized_pnl"] = (mark - entry) * quantity * _signed_multiplier(side)
        trade["return_pct"] = ((mark - entry) / entry) * 100 * _signed_multiplier(side) if entry else None
    realized = _coerce_float(trade.get("realized_pnl"))
    unrealized = _coerce_float(trade.get("unrealized_pnl"))
    trade["total_pnl"] = (realized or 0.0) + (unrealized or 0.0) if realized is not None or unrealized is not None else None

    if "NO_MANUAL_EXECUTION_DECLARED" in events and not symbol:
        trade["status"] = "NOT_EVALUABLE"
        trade["evaluation_summary"] = "NOT_EVALUABLE"
        trade["evidence_status"] = "COMPLETE"
        trade["next_action"] = "No manual execution was declared."
        trade["next_action_command"] = "VIEW_TRADE_DETAIL"
    elif "OUTCOME_FINALIZED" in events or exit_price is not None:
        trade["status"] = "CLOSED"
        if trade.get("return_pct") is None:
            trade["evaluation_summary"] = "INCONCLUSIVE"
            trade["evidence_status"] = "MISSING_EXIT"
        else:
            trade["evaluation_summary"] = "WORKED" if float(trade["return_pct"]) > 0 else "FAILED"
            trade["evidence_status"] = "COMPLETE"
        trade["next_action"] = "Review trade outcome."
        trade["next_action_command"] = "REVIEW_TRADE_OUTCOME"
    elif "OPEN_POSITION_ESTABLISHED" in events or "MANUAL_CAPTURE_RECORDED" in events or "MANUAL_RECEIPT_RECORDED" in events:
        trade["status"] = "OPEN"
        trade["evaluation_summary"] = "STILL_OPEN"
        if not trade.get("manual_receipt_id") and "MANUAL_RECEIPT_RECORDED" not in events:
            trade["evidence_status"] = "MISSING_RECEIPT"
            trade["next_action"] = "Add the manual IB receipt."
            trade["next_action_command"] = "ADD_MANUAL_RECEIPT"
        elif mark is None:
            trade["evidence_status"] = "MISSING_MARKET_DATA"
            trade["next_action"] = "Reconcile trade state with certified market data."
            trade["next_action_command"] = "RECONCILE_TRADE_STATE"
        else:
            trade["evidence_status"] = "MISSING_EXIT"
            trade["next_action"] = "Record exit when the trade is closed."
            trade["next_action_command"] = "RECORD_TRADE_EXIT"
    else:
        trade["status"] = "NEEDS_ATTENTION"
        trade["evaluation_summary"] = "NOT_EVALUABLE"
        trade["evidence_status"] = "MISSING_RECEIPT"
        trade["next_action"] = "Add receipt or reconcile the trade state."
        trade["next_action_command"] = "ADD_MANUAL_RECEIPT"

    if not trade.get("sleeve_id") or not trade.get("hypothesis_id"):
        if trade["evidence_status"] == "COMPLETE":
            trade["evidence_status"] = "MISSING_ATTRIBUTION"
            trade["evaluation_summary"] = "INCONCLUSIVE"
    trade["lifecycle_event_count"] = len(trade.get("lifecycle_events", []))
    return trade


def build_paper_trade_evaluation_projection_v1(*, truth_root: Path | str, day_utc: str, lookback_days: int = 120) -> dict[str, Any]:
    ledger = build_trade_lifecycle_ledger_v1(truth_root=truth_root, day_utc=day_utc, lookback_days=lookback_days)
    marks = latest_certified_marks_v1(truth_root=truth_root, day_utc=day_utc)
    trades: dict[str, dict[str, Any]] = {}
    for row in ledger.get("rows") or []:
        if not isinstance(row, Mapping):
            continue
        trade_id = str(row.get("trade_id") or "")
        if not trade_id:
            continue
        trade = trades.setdefault(trade_id, _trade_shell(trade_id))
        _merge_trade_event(trade, row)
    finalized = [_finalize_trade(trade, marks) for trade in trades.values()]
    finalized = sorted(finalized, key=lambda trade: (str(trade.get("status") or ""), str(trade.get("symbol") or ""), str(trade.get("trade_id") or "")))
    open_trades = [trade for trade in finalized if trade.get("status") == "OPEN"]
    closed_trades = [trade for trade in finalized if trade.get("status") == "CLOSED"]
    missing_evidence_trades = [trade for trade in finalized if str(trade.get("evidence_status") or "") not in {"COMPLETE"}]

    def _sum(key: str, rows: list[dict[str, Any]]) -> float:
        return round(sum(_coerce_float(row.get(key)) or 0.0 for row in rows), 6)

    sleeve_attribution: dict[str, dict[str, Any]] = {}
    hypothesis_attribution: dict[str, dict[str, Any]] = {}
    for trade in finalized:
        for key, store in (("sleeve_id", sleeve_attribution), ("hypothesis_id", hypothesis_attribution)):
            value = str(trade.get(key) or "UNATTRIBUTED")
            bucket = store.setdefault(value, {"id": value, "trade_count": 0, "realized_pnl": 0.0, "unrealized_pnl": 0.0, "total_pnl": 0.0})
            bucket["trade_count"] += 1
            bucket["realized_pnl"] += _coerce_float(trade.get("realized_pnl")) or 0.0
            bucket["unrealized_pnl"] += _coerce_float(trade.get("unrealized_pnl")) or 0.0
            bucket["total_pnl"] += _coerce_float(trade.get("total_pnl")) or 0.0
    for store in (sleeve_attribution, hypothesis_attribution):
        for row in store.values():
            for key in ("realized_pnl", "unrealized_pnl", "total_pnl"):
                row[key] = round(float(row.get(key) or 0.0), 6)

    projection = {
        "schema_id": PROJECTION_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": str(day_utc),
        "generated_at_utc": utc_now_v1(),
        "trade_count": len(finalized),
        "open_trade_count": len(open_trades),
        "closed_trade_count": len(closed_trades),
        "missing_evidence_count": len(missing_evidence_trades),
        "realized_pnl": _sum("realized_pnl", finalized),
        "unrealized_pnl": _sum("unrealized_pnl", finalized),
        "total_pnl": _sum("total_pnl", finalized),
        "return_pct": None,
        "open_trades": open_trades,
        "closed_trades": closed_trades,
        "missing_evidence_trades": missing_evidence_trades,
        "all_trades": finalized,
        "sleeve_attribution": sorted(sleeve_attribution.values(), key=lambda row: str(row.get("id") or "")),
        "hypothesis_attribution": sorted(hypothesis_attribution.values(), key=lambda row: str(row.get("id") or "")),
        "trade_lifecycle_ledger": {
            "schema_id": ledger.get("schema_id"),
            "schema_version": ledger.get("schema_version"),
            "row_count": ledger.get("row_count"),
            "content_hash": ledger.get("content_hash"),
            "artifact_path": "",
        },
        "source_artifacts": sorted(set(ledger.get("source_artifacts") or [])),
        **SAFETY_FLAGS,
    }
    projection["content_hash"] = stable_hash_v1({**projection, "generated_at_utc": "", "content_hash": ""})
    return projection


def trade_lifecycle_ledger_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / LEDGER_FAMILY / str(day_utc) / "trade_lifecycle_ledger.v1.json"


def paper_trade_evaluation_projection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / PROJECTION_FAMILY / str(day_utc) / "paper_trade_evaluation_projection.v1.json"


def write_trade_lifecycle_ledger_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> dict[str, str]:
    path = trade_lifecycle_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"trade_lifecycle_ledger": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}


def write_paper_trade_evaluation_projection_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> dict[str, str]:
    path = paper_trade_evaluation_projection_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"paper_trade_evaluation_projection": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}

