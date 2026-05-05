from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import os
from typing import Any


@dataclass(frozen=True)
class SleeveFillQualityResultV1:
    report_path: Path
    report: dict[str, Any]


def materialize_sleeve_fill_quality_v1(*, day_utc: str, truth_root: Path, evaluation_utc: str | None = None, scheduled_run: bool = False) -> SleeveFillQualityResultV1:
    day = _require_day(day_utc)
    truth_root = Path(truth_root).resolve()
    produced_at = _now(evaluation_utc)
    trade_path = truth_root / "reports" / "sleeve_trade_fact_v1" / day / "sleeve_trade_fact.v1.json"
    market_path = truth_root / "reports" / "market_data_authority_v1" / day / "market_data_authority.v1.json"
    trade_report = _read_json_optional(trade_path) or {}
    market = _read_json_optional(market_path) or {}
    blockers: list[dict[str, Any]] = []
    if market and str(market.get("status") or market.get("market_data_state") or "").upper() not in {"PASS", "READY", "OK"}:
        blockers.append(_blocker("STALE_OR_BLOCKED_MARKET_DATA", "market_data_authority.status", "PASS/READY", market.get("status") or market.get("market_data_state"), str(market_path), "Refresh governed market data before fill quality measurement."))
    if str(trade_report.get("status") or "").upper() == "NO_COMPLETED_TRADES":
        status = "NO_COMPLETED_TRADES"
        rows: list[dict[str, Any]] = []
    elif str(trade_report.get("status") or "").upper() != "PASS":
        blockers.append(_blocker("SLEEVE_TRADE_FACT_NOT_PASS", "sleeve_trade_fact_v1.status", "PASS", trade_report.get("status") or "MISSING", str(trade_path), "Run and clear sleeve trade facts before fill quality."))
        status = "BLOCKED"
        rows = []
    else:
        rows = [_quality_row(row, truth_root=truth_root, day_utc=day) for row in trade_report.get("trade_facts", []) if isinstance(row, dict)]
        status = "BLOCKED" if blockers else "PASS"
    report = {
        "schema_version": "sleeve_fill_quality.v1",
        "day_utc": day,
        "status": status,
        "first_blocker": blockers[0]["blocker_code"] if blockers else "",
        "produced_at_utc": produced_at,
        "producer": "sleeve_fill_quality_v1",
        "scheduled_run": bool(scheduled_run),
        "fills": rows,
        "sleeves": _aggregate(rows),
        "blockers": blockers,
        "evidence_paths": [str(trade_path), str(market_path)],
    }
    out = truth_root / "reports" / "sleeve_fill_quality_v1" / day / "sleeve_fill_quality.v1.json"
    _write_json(out, report)
    return SleeveFillQualityResultV1(report_path=out, report=report)


def _quality_row(fact: dict[str, Any], *, truth_root: Path, day_utc: str) -> dict[str, Any]:
    fill_price = _number(fact.get("fill_price"))
    snapshot, snapshot_path = _decision_snapshot(truth_root=truth_root, day_utc=day_utc, intent_id=_text(fact.get("intent_id")))
    decision_price = _snapshot_decision_price(snapshot)
    if decision_price <= 0:
        decision_price = _first_number(fact, "decision_price", "reference_price", "expected_fill_price", "limit_price")
    fees = _number(fact.get("fees"))
    notional = _number(fact.get("gross_notional"))
    if decision_price <= 0:
        slippage = None
        verdict = "UNKNOWN_REFERENCE_PRICE_MISSING"
    else:
        raw = fill_price - decision_price
        side = _text(fact.get("side")).upper()
        slippage = round(raw if side in {"BUY", "BOT"} else -raw, 6)
        verdict = "PASS" if slippage <= 0 else "MEASURED_SLIPPAGE"
    return {
        "intent_id": _text(fact.get("intent_id")),
        "sleeve_id": _text(fact.get("sleeve_id")),
        "broker_exec_id": _text(fact.get("broker_exec_id")),
        "decision_price": decision_price if decision_price > 0 else None,
        "decision_price_source": snapshot.get("source") if snapshot else ("TRADE_FACT_REFERENCE_FIELD" if decision_price > 0 else "NONE"),
        "decision_price_snapshot_path": str(snapshot_path) if snapshot_path else "",
        "fill_price": fill_price,
        "slippage": slippage,
        "spread_at_decision": fact.get("spread_at_decision"),
        "fee_drag": round(fees / notional, 8) if notional > 0 else None,
        "partial_fill_quality": "COMPLETE_FILL" if _number(fact.get("quantity")) <= 0 or _number(fact.get("filled_quantity")) >= _number(fact.get("quantity")) else "PARTIAL_FILL",
        "fill_quality_verdict": verdict,
    }


def _aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_text(row.get("sleeve_id")) or "UNKNOWN", []).append(row)
    out = []
    for sleeve_id, items in sorted(grouped.items()):
        measured = [_number(row.get("slippage")) for row in items if row.get("slippage") is not None]
        out.append(
            {
                "sleeve_id": sleeve_id,
                "fill_count": len(items),
                "measured_fill_count": len(measured),
                "average_slippage": round(sum(measured) / len(measured), 6) if measured else None,
                "fill_quality_verdict": "UNKNOWN_REFERENCE_PRICE_MISSING" if len(measured) < len(items) else "PASS",
            }
        )
    return out


def _first_number(payload: dict[str, Any], *keys: str) -> float:
    for key in keys:
        number = _number(payload.get(key))
        if number > 0:
            return number
    return 0.0


def _decision_snapshot(*, truth_root: Path, day_utc: str, intent_id: str) -> tuple[dict[str, Any], Path | None]:
    if not intent_id:
        return {}, None
    path = truth_root / "reports" / "decision_price_snapshot_v1" / day_utc / f"{intent_id}.json"
    payload = _read_json_optional(path)
    if payload and str(payload.get("status") or "").upper() == "PASS":
        return payload, path
    return {}, None


def _snapshot_decision_price(snapshot: dict[str, Any]) -> float:
    if not snapshot:
        return 0.0
    mid = _number(snapshot.get("mid"))
    if mid > 0:
        return mid
    last = _number(snapshot.get("last"))
    if last > 0:
        return last
    return 0.0


def _blocker(code: str, field: str, expected: Any, actual: Any, path: str, action: str) -> dict[str, Any]:
    return {"blocker_code": code, "failed_field": field, "expected_value": expected, "actual_value": actual, "artifact_path": path, "operator_next_action": action}


def _read_json_optional(path: Path) -> dict[str, Any] | None:
    try:
        if path.exists() and path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
    except Exception:
        return None
    return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _number(value: Any) -> float:
    try:
        if value in (None, "") or isinstance(value, bool):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _text(value: Any) -> str:
    return str(value or "").strip()


def _require_day(value: str) -> str:
    text = _text(value)
    datetime.fromisoformat(text)
    if len(text) != 10:
        raise ValueError("day_utc must be YYYY-MM-DD")
    return text


def _now(value: str | None) -> str:
    return _text(value) if value else datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
