from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.market_calendar.session_calendar_v1 import is_us_equities_trading_day_v1
from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, paper_position_ledger_path_v1

REPORT_FAMILY = "aegis_operator_portfolio_valuation_estimate_v1"
REPORT_FILENAME = "operator_portfolio_valuation_estimate.v1.json"

SAFETY = {
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_execution_allowed": False,
    "autonomous_live_trading_allowed": False,
    "canonical_pnl_allowed": False,
    "read_only": True,
}


def operator_portfolio_valuation_estimate_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_operator_portfolio_valuation_estimate_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)[:10]
    ledger_path = paper_position_ledger_path_v1(truth_root=root, day_utc=day)
    ledger = _read_json(ledger_path)
    if not isinstance(ledger, dict) or not ledger:
        ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=day)
    market_path = root / "reports" / "market_data_intraday_operational_v1" / day / "market_data_intraday_operational.v1.json"
    market = _read_json(market_path)
    if not isinstance(market, dict):
        market = {}
    target_is_trading_day = is_us_equities_trading_day_v1(day)
    market_rows = market.get("symbols") if isinstance(market.get("symbols"), Mapping) else {}
    open_positions = [row for row in ledger.get("open_positions", []) if isinstance(row, Mapping)] if isinstance(ledger.get("open_positions"), list) else []
    rows: list[dict[str, Any]] = []
    estimated_value = Decimal("0")
    estimated_unrealized = Decimal("0")
    marked_count = 0
    sessions: list[str] = []
    for position in open_positions:
        estimate = _position_estimate(position, market_rows)
        rows.append(estimate)
        if estimate["estimate_available"]:
            marked_count += 1
            sessions.append(str(estimate.get("latest_mark_date") or ""))
            estimated_value += _decimal(estimate.get("estimated_value")) or Decimal("0")
            estimated_unrealized += _decimal(estimate.get("estimated_unrealized_pnl")) or Decimal("0")
    latest_session = max([item for item in sessions if item], default="")
    open_count = len(open_positions)
    missing_count = max(open_count - marked_count, 0)
    if marked_count == 0:
        valuation_status = "ESTIMATE_UNAVAILABLE"
        estimate_reason = "ESTIMATE_UNAVAILABLE"
    else:
        valuation_status = "ESTIMATED_NOT_CERTIFIED_FOR_TARGET_DAY"
        estimate_reason = "NON_TRADING_DAY_PRIOR_SESSION_MARKS" if not target_is_trading_day else "TARGET_DAY_MARKS_NOT_CERTIFIED"
    coverage_pct = round((marked_count / open_count) * 100, 4) if open_count else 100.0
    source_paths = {
        "paper_position_ledger_v1": str(ledger_path),
        "market_data_intraday_operational_v1": str(market_path),
        "market_calendar_session_calendar_v1": "ops/aegis/market_calendar/session_calendar_v1.py",
    }
    payload = {
        "schema_id": "aegis_operator_portfolio_valuation_estimate",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "target_day": day,
        "day_utc": day,
        "target_is_trading_day": bool(target_is_trading_day),
        "latest_available_mark_date": latest_session,
        "latest_available_market_session": latest_session,
        "valuation_status": valuation_status,
        "estimate_reason": estimate_reason,
        "estimated_portfolio_value": _decimal_text(estimated_value) if marked_count else "",
        "estimated_unrealized_pnl": _decimal_text(estimated_unrealized) if marked_count else "",
        "estimated_mark_coverage_pct": coverage_pct,
        "open_position_count": open_count,
        "marked_position_count": marked_count,
        "missing_estimate_count": missing_count,
        "positions": rows,
        "source_artifact_paths": source_paths,
        "source_hashes": {name: _file_hash(Path(path)) for name, path in source_paths.items() if path and not path.startswith("ops/")},
        "rules": {
            "estimate_is_read_only": True,
            "estimate_never_replaces_canonical_pnl": True,
            "estimate_uses_latest_available_marks_only": True,
            "target_day_certification_unchanged": True,
        },
        "labels": ["Estimated", "Not certified for target day", "Latest available marks"],
        "generated_at_utc": _now(),
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_operator_portfolio_valuation_estimate_v1(
    *, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None
) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_operator_portfolio_valuation_estimate_v1(truth_root=root, day_utc=day_utc))
    path = operator_portfolio_valuation_estimate_path_v1(truth_root=root, day_utc=str(day_utc)[:10])
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def _position_estimate(position: Mapping[str, Any], market_rows: Mapping[str, Any]) -> dict[str, Any]:
    symbol = str(position.get("symbol") or "").upper()
    market = market_rows.get(symbol) if isinstance(market_rows.get(symbol), Mapping) else {}
    latest_mark = _decimal(_first(market.get("last_price"), market.get("close"), market.get("price")))
    qty = _decimal(position.get("quantity"))
    entry = _decimal(position.get("entry_price"))
    side = str(position.get("side") or position.get("direction") or "BUY").upper()
    session = str(market.get("market_session_date") or market.get("trading_day") or market.get("day_utc") or "")[:10]
    estimate_available = latest_mark is not None and qty is not None and entry is not None
    value = qty * latest_mark if estimate_available else None
    if estimate_available and side in {"SELL", "SHORT"}:
        unrealized = (entry - latest_mark) * qty
    elif estimate_available:
        unrealized = (latest_mark - entry) * qty
    else:
        unrealized = None
    return {
        "position_id": str(position.get("position_id") or ""),
        "candidate_id": str(position.get("candidate_id") or ""),
        "symbol": symbol,
        "quantity": _decimal_text(qty) if qty is not None else "",
        "entry_price": _decimal_text(entry) if entry is not None else "",
        "latest_mark_price": _decimal_text(latest_mark) if latest_mark is not None else "",
        "latest_mark_date": session,
        "latest_market_session": session,
        "latest_mark_source": str(market.get("source") or market.get("provider") or ""),
        "latest_mark_source_path": str(market.get("source_url_or_path") or ""),
        "latest_mark_source_hash": str(market.get("source_hash") or ""),
        "latest_mark_freshness_status": str(market.get("freshness_status") or "UNAVAILABLE"),
        "latest_mark_mode": str(market.get("market_data_mode") or "UNAVAILABLE"),
        "estimate_available": bool(estimate_available),
        "estimated_value": _decimal_text(value) if value is not None else "",
        "estimated_unrealized_pnl": _decimal_text(unrealized) if unrealized is not None else "",
        "certification_status": "NOT_CERTIFIED_FOR_TARGET_DAY",
        "canonical_pnl_allowed": False,
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal | None) -> str:
    if value is None:
        return ""
    normalized = value.quantize(Decimal("0.0001")).normalize()
    return format(normalized, "f")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
