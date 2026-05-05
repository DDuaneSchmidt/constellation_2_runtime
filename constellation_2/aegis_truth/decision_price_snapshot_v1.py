from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import os
from typing import Any


@dataclass(frozen=True)
class DecisionPriceSnapshotResultV1:
    report_paths: tuple[Path, ...]
    summary_path: Path
    summary: dict[str, Any]


def materialize_decision_price_snapshot_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    evaluation_utc: str | None = None,
    scheduled_run: bool = False,
) -> DecisionPriceSnapshotResultV1:
    day = _require_day(day_utc)
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root else truth_root
    produced_at = _now(evaluation_utc)
    intents = _decision_intents(truth_root=truth_root, execution_root=execution_root, day_utc=day)
    paths: list[Path] = []
    blockers: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    if not intents:
        blockers.append(_blocker("DECISION_INTENT_MISSING", "selected_intent/structure_decisions", "at least one intent", "MISSING", "", "Run intent arbitration or structure decision before decision price capture."))
    for intent in intents:
        snapshot = _build_snapshot(day_utc=day, truth_root=truth_root, execution_root=execution_root, intent=intent, produced_at=produced_at, scheduled_run=scheduled_run)
        out = truth_root / "reports" / "decision_price_snapshot_v1" / day / f"{snapshot['intent_id']}.json"
        _write_json(out, snapshot)
        paths.append(out)
        snapshots.append(snapshot)
        if snapshot["status"] != "PASS":
            blockers.append(_blocker(snapshot["first_blocker"], "price_source", "broker-backed or market-data-backed quote", "MISSING", str(out), "Refresh governed market data/options chain and rerun decision price snapshot."))
    summary = {
        "schema_version": "decision_price_snapshot_summary.v1",
        "day_utc": day,
        "status": "BLOCKED" if blockers else "PASS",
        "first_blocker": blockers[0]["blocker_code"] if blockers else "",
        "produced_at_utc": produced_at,
        "producer": "decision_price_snapshot_v1",
        "scheduled_run": bool(scheduled_run),
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
        "blockers": blockers,
        "truth_root": str(truth_root),
        "execution_root": str(execution_root),
    }
    summary_path = truth_root / "reports" / "decision_price_snapshot_v1" / day / "decision_price_snapshot_summary.v1.json"
    _write_json(summary_path, summary)
    return DecisionPriceSnapshotResultV1(report_paths=tuple(paths), summary_path=summary_path, summary=summary)


def _build_snapshot(*, day_utc: str, truth_root: Path, execution_root: Path, intent: dict[str, Any], produced_at: str, scheduled_run: bool) -> dict[str, Any]:
    intent_id = _text(intent.get("intent_id") or intent.get("selected_intent_id") or intent.get("intent_hash") or "UNKNOWN_INTENT")
    sleeve_id = _text(intent.get("sleeve_id") or intent.get("engine_id") or intent.get("strategy_id"))
    symbol = _text(intent.get("symbol") or intent.get("underlying") or intent.get("root_symbol")).upper()
    contract_key = _text(intent.get("contract_key") or intent.get("selected_contract_key"))
    local_symbol = _text(intent.get("local_symbol") or intent.get("localSymbol"))
    quote = _find_quote(truth_root=truth_root, execution_root=execution_root, day_utc=day_utc, symbol=symbol, contract_key=contract_key, local_symbol=local_symbol)
    bid = quote.get("bid")
    ask = quote.get("ask")
    last = quote.get("last")
    mid = _mid(bid, ask)
    status = "PASS" if quote.get("source_artifact") and any(_number(value) > 0 for value in (bid, ask, last, mid)) else "BLOCKED"
    first_blocker = "" if status == "PASS" else "DECISION_PRICE_SOURCE_MISSING"
    return {
        "schema_version": "decision_price_snapshot.v1",
        "day_utc": day_utc,
        "status": status,
        "first_blocker": first_blocker,
        "symbol": symbol,
        "bid": bid,
        "ask": ask,
        "last": last,
        "mid": mid,
        "timestamp": quote.get("timestamp") or produced_at,
        "source": quote.get("source") or "NONE",
        "source_artifact": quote.get("source_artifact") or "",
        "intent_id": intent_id,
        "sleeve_id": sleeve_id,
        "contract_key": contract_key,
        "local_symbol": local_symbol,
        "produced_at_utc": produced_at,
        "producer": "decision_price_snapshot_v1",
        "scheduled_run": bool(scheduled_run),
    }


def _decision_intents(*, truth_root: Path, execution_root: Path, day_utc: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in [
        truth_root / "reports" / "intent_arbitration_v1" / day_utc / "selected_intent_pointer.v1.json",
        execution_root / "reports" / "intent_arbitration_v1" / day_utc / "selected_intent_pointer.v1.json",
        truth_root / "reports" / "intent_arbitration_v1" / day_utc / "intent_arbitration.v1.json",
        execution_root / "reports" / "intent_arbitration_v1" / day_utc / "intent_arbitration.v1.json",
        truth_root / "reports" / "structure_decision_supply_v1" / day_utc / "structure_decision_supply.v1.json",
        execution_root / "reports" / "structure_decision_supply_v1" / day_utc / "structure_decision_supply.v1.json",
    ]:
        payload = _read_json_optional(path)
        if not payload:
            continue
        rows.extend(_extract_intents(payload, path))
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        intent_id = _text(row.get("intent_id") or row.get("selected_intent_id") or row.get("intent_hash"))
        if intent_id:
            by_id.setdefault(intent_id, row)
    return list(by_id.values())


def _extract_intents(payload: dict[str, Any], path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    selected = payload.get("selected_intent") if isinstance(payload.get("selected_intent"), dict) else {}
    if selected:
        merged = dict(selected)
        for key in ("selected_intent_id", "intent_id", "sleeve_id", "engine_id", "strategy_id", "symbol", "underlying"):
            if key in payload and key not in merged:
                merged[key] = payload.get(key)
        merged["_source_path"] = str(path)
        out.append(merged)
    for key in ("active_intents", "structure_decisions", "candidate_intents"):
        values = payload.get(key)
        if isinstance(values, list):
            for item in values:
                if isinstance(item, dict):
                    row = dict(item)
                    row["_source_path"] = str(path)
                    out.append(row)
    return out


def _find_quote(*, truth_root: Path, execution_root: Path, day_utc: str, symbol: str, contract_key: str, local_symbol: str) -> dict[str, Any]:
    roots = [truth_root, execution_root]
    market_quote = _find_market_snapshot(roots=roots, day_utc=day_utc, symbol=symbol)
    option_quote = _find_options_quote(roots=roots, day_utc=day_utc, symbol=symbol, contract_key=contract_key, local_symbol=local_symbol)
    if contract_key or local_symbol:
        return option_quote or market_quote or {}
    return market_quote or option_quote or {}


def _find_market_snapshot(*, roots: list[Path], day_utc: str, symbol: str) -> dict[str, Any] | None:
    candidates: list[tuple[str, Path, dict[str, Any]]] = []
    for root in roots:
        for path in sorted((root / "market_data_snapshot_v1" / "snapshots" / day_utc).glob("*.json")):
            payload = _read_json_optional(path)
            if not payload:
                continue
            payload_symbol = _text(payload.get("symbol") or payload.get("underlying") or payload.get("root_symbol")).upper()
            if symbol and payload_symbol and payload_symbol != symbol:
                continue
            candidates.append((_timestamp(payload), path, payload))
    for _ts, path, payload in sorted(candidates, reverse=True):
        bid = _first_number(payload, "bid", "bid_price")
        ask = _first_number(payload, "ask", "ask_price")
        last = _first_number(payload, "last", "last_price", "mark", "close")
        if bid or ask or last:
            return {"bid": bid, "ask": ask, "last": last, "timestamp": _timestamp(payload), "source": "MARKET_DATA_SNAPSHOT", "source_artifact": str(path)}
    return None


def _find_options_quote(*, roots: list[Path], day_utc: str, symbol: str, contract_key: str, local_symbol: str) -> dict[str, Any] | None:
    candidates: list[tuple[str, Path, dict[str, Any]]] = []
    for root in roots:
        for path in sorted((root / "options_chain_snapshot_v1" / day_utc).rglob("options_chain_snapshot.v1.json")):
            payload = _read_json_optional(path)
            if not payload:
                continue
            candidates.append((_timestamp(payload), path, payload))
    for _ts, path, payload in sorted(candidates, reverse=True):
        contracts = payload.get("contracts") if isinstance(payload.get("contracts"), list) else []
        for contract in contracts:
            if not isinstance(contract, dict):
                continue
            contract_local = _text(contract.get("local_symbol") or contract.get("localSymbol") or _nested(contract, "ib", "localSymbol"))
            if contract_key and _text(contract.get("contract_key")) != contract_key:
                continue
            if local_symbol and contract_local and contract_local != local_symbol:
                continue
            if not contract_key and not local_symbol:
                continue
            bid = _first_number(contract, "bid", "bid_price")
            ask = _first_number(contract, "ask", "ask_price")
            last = _first_number(contract, "last", "last_price", "mark", "close")
            if bid or ask or last:
                return {"bid": bid, "ask": ask, "last": last, "timestamp": _timestamp(payload), "source": "OPTIONS_CHAIN_SNAPSHOT", "source_artifact": str(path)}
        underlying = payload.get("underlying") if isinstance(payload.get("underlying"), dict) else {}
        underlying_symbol = _text(underlying.get("symbol") or payload.get("symbol")).upper()
        if symbol and underlying_symbol == symbol:
            last = _first_number(underlying, "spot_price", "last", "last_price")
            if last:
                return {"bid": None, "ask": None, "last": last, "timestamp": _text(underlying.get("spot_as_of_utc") or _timestamp(payload)), "source": "OPTIONS_CHAIN_UNDERLYING_SPOT", "source_artifact": str(path)}
    return None


def _blocker(code: str, field: str, expected: Any, actual: Any, path: str, action: str) -> dict[str, Any]:
    return {"blocker_code": code, "failed_field": field, "expected_value": expected, "actual_value": actual, "artifact_path": path, "operator_next_action": action}


def _mid(bid: Any, ask: Any) -> float | None:
    bid_n = _number(bid)
    ask_n = _number(ask)
    if bid_n > 0 and ask_n > 0:
        return round((bid_n + ask_n) / 2.0, 8)
    return None


def _first_number(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            value = value.get("value") or value.get("amount")
        number = _number(value)
        if number > 0:
            return number
    return None


def _timestamp(payload: dict[str, Any]) -> str:
    return _text(payload.get("timestamp") or payload.get("produced_at_utc") or payload.get("captured_at_utc") or payload.get("as_of_utc") or payload.get("snapshot_time_utc"))


def _nested(payload: dict[str, Any], *keys: str) -> Any:
    cur: Any = payload
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


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
