from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import hashlib
import json
import os
from typing import Any


TERMINAL_NO_PNL_STATES = {"CANCELLED", "REJECTED", "EXPIRED"}
COMPLETED_FILL_STATES = {"FILLED", "EOD_RECONCILED", "POSITION_RECONCILED", "CASH_NAV_RECONCILED", "RISK_RECOMPUTED"}


@dataclass(frozen=True)
class SleeveTradeFactResultV1:
    report_path: Path
    report: dict[str, Any]


def materialize_sleeve_trade_fact_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path | None = None,
    evaluation_utc: str | None = None,
    scheduled_run: bool = False,
) -> SleeveTradeFactResultV1:
    day = _require_day(day_utc)
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve() if execution_root else truth_root
    produced_at = _now(evaluation_utc)

    lifecycle_path = truth_root / "reports" / "post_trade_lifecycle_v1" / day / "post_trade_lifecycle.v1.json"
    fallback_lifecycle_path = execution_root / "reports" / "post_trade_lifecycle_v1" / day / "post_trade_lifecycle.v1.json"
    lifecycle = _read_json_optional(lifecycle_path) or _read_json_optional(fallback_lifecycle_path) or {}
    orders = lifecycle.get("orders") if isinstance(lifecycle.get("orders"), list) else []
    selected_pointer, selected_pointer_path = _load_first_existing(
        truth_root / "reports" / "intent_arbitration_v1" / day / "selected_intent_pointer.v1.json",
        execution_root / "reports" / "intent_arbitration_v1" / day / "selected_intent_pointer.v1.json",
    )
    selected_intents = _selected_intent_index(selected_pointer)
    auth_index = _authorization_index(truth_root=truth_root, execution_root=execution_root, day_utc=day)
    intent_index = _intent_index(truth_root=truth_root, execution_root=execution_root, day_utc=day)

    trade_facts: list[dict[str, Any]] = []
    lifecycle_records: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    no_trade_reason = "POST_TRADE_LIFECYCLE_MISSING" if not lifecycle else ""
    seen_exec_ids: set[str] = set()

    for order in orders:
        if not isinstance(order, dict):
            continue
        intent_id = _text(order.get("intent_id"))
        if not intent_id:
            blockers.append(_blocker("MISSING_INTENT_ID", "orders[].intent_id", "non-empty", "", str(lifecycle_path), "Repair submit/order lifecycle attribution; no sleeve fact can be created without intent_id."))
            continue
        intent = _merge_dicts(intent_index.get(intent_id), selected_intents.get(intent_id))
        sleeve_id = _text(order.get("sleeve_id") or intent.get("sleeve_id"))
        if not sleeve_id:
            blockers.append(_blocker("MISSING_SLEEVE_ID", f"intent:{intent_id}.sleeve_id", "non-empty", "", str(lifecycle_path), "Materialize intent attribution with sleeve_id before measuring sleeve effectiveness."))
            continue
        final_state = _text(order.get("final_state")).upper()
        lifecycle_record = {
            "day_utc": day,
            "intent_id": intent_id,
            "sleeve_id": sleeve_id,
            "lifecycle_state": final_state,
            "broker_order_id": order.get("broker_order_id"),
            "broker_perm_id": order.get("perm_id"),
            "filled_quantity": _number(order.get("filled_quantity")),
            "produces_realized_pnl": False,
        }
        lifecycle_records.append(lifecycle_record)
        if final_state in TERMINAL_NO_PNL_STATES:
            continue
        fills = _fill_events(order)
        if final_state in COMPLETED_FILL_STATES and not fills:
            blockers.append(_blocker("BROKER_FILL_EVIDENCE_MISSING", f"intent:{intent_id}.matched_broker_events", "at least one FILL event", "MISSING", str(lifecycle_path), "Refresh broker execution/fill events; do not infer fills from order status."))
            continue
        for fill in fills:
            exec_id = _text(fill.get("exec_id") or fill.get("broker_exec_id") or fill.get("execution_id"))
            if not exec_id:
                blockers.append(_blocker("BROKER_EXEC_ID_MISSING", f"intent:{intent_id}.fill.exec_id", "non-empty", "", str(lifecycle_path), "Refresh broker execution evidence with execId before building immutable trade facts."))
                continue
            dedupe_key = f"{intent_id}:{exec_id}"
            if dedupe_key in seen_exec_ids:
                continue
            seen_exec_ids.add(dedupe_key)
            fill_price = _number(fill.get("price") or fill.get("fill_price"))
            filled_quantity = _number(fill.get("filled_quantity") or fill.get("quantity") or fill.get("shares"))
            if fill_price <= 0:
                blockers.append(_blocker("FILL_PRICE_MISSING", f"intent:{intent_id}.fill.price", "> 0 broker price", fill.get("price"), str(lifecycle_path), "Refresh broker fill evidence; no fill price may be inferred."))
                continue
            if filled_quantity <= 0:
                blockers.append(_blocker("FILL_QUANTITY_MISSING", f"intent:{intent_id}.fill.quantity", "> 0 broker quantity", filled_quantity, str(lifecycle_path), "Refresh broker fill evidence with filled quantity."))
                continue
            side = _side(order, fill)
            fees = _number(fill.get("fees") or fill.get("commission"))
            gross_notional = round(abs(filled_quantity * fill_price), 6)
            signed_cash = -gross_notional if side in {"BUY", "BOT"} else gross_notional
            net_cash_effect = round(signed_cash - fees, 6)
            submit_artifact_path = _text(order.get("source_artifact"))
            auth_artifact_path = _text(auth_index.get(intent_id, {}).get("_source_path"))
            intent_artifact_path = _text(intent.get("_source_path"))
            source_paths = sorted({p for p in [_text(fill.get("source_path")), _text(order.get("source_artifact"))] if p})
            fact_core = {
                "day_utc": day,
                "intent_id": intent_id,
                "sleeve_id": sleeve_id,
                "strategy_id": _text(intent.get("strategy_id") or intent.get("strategy") or sleeve_id),
                "engine_id": _text(intent.get("engine_id") or intent.get("engine") or sleeve_id),
                "symbol": _text(order.get("symbol") or fill.get("symbol") or intent.get("symbol") or intent.get("underlying")),
                "asset_class": _text(intent.get("asset_class") or intent.get("asset_type") or "UNKNOWN"),
                "broker_order_id": order.get("broker_order_id"),
                "broker_perm_id": order.get("perm_id"),
                "broker_exec_id": exec_id,
                "side": side,
                "quantity": _number(order.get("expected_quantity") or order.get("quantity")),
                "filled_quantity": filled_quantity,
                "fill_price": fill_price,
                "fill_time_utc": _text(fill.get("received_utc") or fill.get("observed_utc") or fill.get("event_time_utc")),
                "fees": fees,
                "gross_notional": gross_notional,
                "net_cash_effect": net_cash_effect,
                "lifecycle_state": final_state,
                "decision_price": _first_number(intent, auth_index.get(intent_id, {}), "decision_price", "reference_price", "expected_fill_price", "limit_price"),
                "reference_price": _first_number(intent, auth_index.get(intent_id, {}), "reference_price", "decision_price", "expected_fill_price", "limit_price"),
                "spread_at_decision": _first_value(intent, auth_index.get(intent_id, {}), "spread_at_decision", "bid_ask_spread", "spread"),
                "submit_artifact_path": submit_artifact_path,
                "authorization_artifact_path": auth_artifact_path,
                "intent_artifact_path": intent_artifact_path,
                "source_broker_event_paths": source_paths,
            }
            fact_core["evidence_sha256"] = _sha256_json(fact_core)
            trade_facts.append(fact_core)
            lifecycle_record["produces_realized_pnl"] = True

    status = "PASS"
    first_blocker = ""
    if blockers:
        status = "BLOCKED"
        first_blocker = blockers[0]["blocker_code"]
    elif not trade_facts:
        status = "NO_COMPLETED_TRADES"
        first_blocker = no_trade_reason

    report = {
        "schema_version": "sleeve_trade_fact.v1",
        "day_utc": day,
        "status": status,
        "first_blocker": first_blocker,
        "produced_at_utc": produced_at,
        "producer": "sleeve_trade_fact_v1",
        "scheduled_run": bool(scheduled_run),
        "truth_root": str(truth_root),
        "execution_root": str(execution_root),
        "trade_fact_count": len(trade_facts),
        "no_trade_reason": no_trade_reason if status == "NO_COMPLETED_TRADES" else "",
        "lifecycle_record_count": len(lifecycle_records),
        "trade_facts": trade_facts,
        "lifecycle_records": lifecycle_records,
        "blockers": blockers,
        "evidence_paths": [str(path) for path in [lifecycle_path, selected_pointer_path] if path],
    }
    out_path = truth_root / "reports" / "sleeve_trade_fact_v1" / day / "sleeve_trade_fact.v1.json"
    _write_json(out_path, report)
    return SleeveTradeFactResultV1(report_path=out_path, report=report)


def _fill_events(order: dict[str, Any]) -> list[dict[str, Any]]:
    rows = order.get("matched_broker_events") if isinstance(order.get("matched_broker_events"), list) else []
    out = [row for row in rows if isinstance(row, dict) and _text(row.get("event_type")).upper() == "FILL"]
    out.sort(key=lambda row: (_text(row.get("received_utc") or row.get("observed_utc")), _text(row.get("exec_id") or row.get("execution_id"))))
    return out


def _authorization_index(*, truth_root: Path, execution_root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    roots = [
        truth_root / "reports" / "authorization_supply_v1" / day_utc,
        execution_root / "reports" / "authorization_supply_v1" / day_utc,
        truth_root / "authorization_artifacts_v1" / day_utc,
        execution_root / "authorization_artifacts_v1" / day_utc,
        truth_root / "reports" / "authorization_artifacts_v1" / day_utc,
        execution_root / "reports" / "authorization_artifacts_v1" / day_utc,
    ]
    for root in roots:
        for path in _json_files(root):
            payload = _read_json_optional(path)
            for row in _walk_dicts(payload):
                intent_id = _text(row.get("intent_id") or row.get("selected_intent_id") or row.get("intent_hash"))
                if intent_id:
                    indexed = dict(row)
                    indexed["_source_path"] = str(path)
                    index.setdefault(intent_id, indexed)
    return index


def _intent_index(*, truth_root: Path, execution_root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    roots = [
        truth_root / "intents_v1" / "snapshots" / day_utc,
        execution_root / "intents_v1" / "snapshots" / day_utc,
        truth_root / "reports" / "intent_arbitration_v1" / day_utc,
        execution_root / "reports" / "intent_arbitration_v1" / day_utc,
    ]
    for root in roots:
        for path in _json_files(root):
            payload = _read_json_optional(path)
            for row in _walk_dicts(payload):
                intent_id = _text(row.get("intent_id") or row.get("selected_intent_id") or row.get("intent_hash") or row.get("id"))
                if intent_id:
                    indexed = dict(row)
                    indexed["_source_path"] = str(path)
                    index.setdefault(intent_id, indexed)
    return index


def _selected_intent_index(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    selected = payload.get("selected_intent") if isinstance(payload.get("selected_intent"), dict) else payload
    intent_id = _text(selected.get("intent_id") or selected.get("selected_intent_id") or selected.get("intent_hash") or payload.get("selected_intent_id"))
    if not intent_id:
        return {}
    merged = dict(selected)
    for key in ("sleeve_id", "engine_id", "strategy_id", "symbol", "underlying", "asset_class"):
        if key in payload and key not in merged:
            merged[key] = payload.get(key)
    return {intent_id: merged}


def _load_first_existing(*paths: Path) -> tuple[dict[str, Any], Path | None]:
    for path in paths:
        payload = _read_json_optional(path)
        if payload is not None:
            return payload, path
    return {}, None


def _blocker(code: str, field: str, expected: Any, actual: Any, path: str, action: str) -> dict[str, Any]:
    return {
        "blocker_code": code,
        "failed_field": field,
        "expected_value": expected,
        "actual_value": actual,
        "artifact_path": path,
        "operator_next_action": action,
    }


def _side(order: dict[str, Any], fill: dict[str, Any]) -> str:
    side = _text(fill.get("side") or order.get("side")).upper()
    return {"SLD": "SELL", "BOT": "BUY"}.get(side, side)


def _merge_dicts(*rows: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for row in rows:
        if isinstance(row, dict):
            out.update(row)
    return out


def _first_number(payload_a: dict[str, Any], payload_b: dict[str, Any], *keys: str) -> float | None:
    for payload in (payload_a, payload_b):
        if not isinstance(payload, dict):
            continue
        for key in keys:
            number = _number(payload.get(key))
            if number > 0:
                return number
    return None


def _first_value(payload_a: dict[str, Any], payload_b: dict[str, Any], *keys: str) -> Any:
    for payload in (payload_a, payload_b):
        if not isinstance(payload, dict):
            continue
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return value
    return None


def _walk_dicts(obj: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        rows.append(obj)
        for value in obj.values():
            rows.extend(_walk_dicts(value))
    elif isinstance(obj, list):
        for value in obj:
            rows.extend(_walk_dicts(value))
    return rows


def _json_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    if root.is_file() and root.suffix == ".json":
        return [root]
    return sorted(path for path in root.rglob("*.json") if path.is_file())


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


def _sha256_json(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


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
    if value:
        return _text(value)
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
