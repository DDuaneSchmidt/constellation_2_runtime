#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_structure_selection_v1 import (
    STRUCTURE_STATUS_SELECTED,
    canonical_structure_decision_json_v1,
    select_structure_for_candidate_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod

SCHEMA_VERSION = "structure_decision_supply.v1"
POLICY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json"
ALLOWED_BLOCKERS = {
    "ACTIVE_INTENT_MISSING",
    "MARKET_OPEN_DATA_MISSING",
    "OPTIONS_SNAPSHOT_MISSING",
    "RISK_BUDGET_SUPPLY_BLOCKED",
    "NO_ELIGIBLE_OPTION_STRUCTURE",
    "STRUCTURE_POLICY_MISSING",
    "STRUCTURE_DECISION_VALIDATION_FAILED",
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def structure_decision_supply_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "structure_decision_supply_v1" / day_utc / "structure_decision_supply.v1.json").resolve()


def _intent_files(ctx: bod.BodContext) -> list[Path]:
    root = ctx.execution_root / "intents_v1" / "snapshots" / ctx.day_utc
    if not root.exists() or not root.is_dir():
        return []
    return sorted(path.resolve() for path in root.glob("*.json") if path.is_file())


def _intent_id(path: Path, payload: dict[str, Any]) -> str:
    return str(payload.get("intent_id") or payload.get("intent_hash") or path.name.split(".", 1)[0]).strip()


def _intent_hash(path: Path, payload: dict[str, Any]) -> str:
    return str(payload.get("intent_hash") or payload.get("canonical_json_hash") or path.name.split(".", 1)[0]).strip()


def _symbol(payload: dict[str, Any]) -> str:
    underlying = payload.get("underlying") if isinstance(payload.get("underlying"), dict) else {}
    return str(payload.get("instrument") or payload.get("symbol") or underlying.get("symbol") or "").strip().upper()


def _engine_id(payload: dict[str, Any]) -> str:
    engine = payload.get("engine") if isinstance(payload.get("engine"), dict) else {}
    return str(engine.get("engine_id") or payload.get("engine_id") or "").strip()


def _active_intents(ctx: bod.BodContext) -> list[tuple[Path, dict[str, Any]]]:
    rows: list[tuple[Path, dict[str, Any]]] = []
    for path in _intent_files(ctx):
        payload = _read_json(path)
        if not payload:
            continue
        if str(payload.get("day_utc") or ctx.day_utc).strip() != ctx.day_utc:
            continue
        rows.append((path, payload))
    return rows


def _risk_budget_path(ctx: bod.BodContext) -> Path:
    return ctx.truth_root / "reports" / "risk_budget_supply_v1" / ctx.day_utc / "risk_budget_supply.v1.json"


def _market_open_gate_path(ctx: bod.BodContext) -> Path:
    return ctx.truth_root / "reports" / "market_open_data_gate_v1" / ctx.day_utc / "market_open_data_gate.v1.json"


def _dec(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _policy_for_intent(intent: dict[str, Any]) -> dict[str, Any]:
    policy = _read_json(POLICY_PATH)
    engine_id = _engine_id(intent)
    for row in policy.get("engine_policies") or []:
        if isinstance(row, dict) and str(row.get("engine_id") or "").strip() == engine_id:
            return row
    return {}


def _snapshot_from_gate(ctx: bod.BodContext) -> tuple[Path | None, Path | None, dict[str, Any], dict[str, Any]]:
    gate = _read_json(_market_open_gate_path(ctx))
    if str(gate.get("status") or "").strip().upper() != "PASS":
        return None, None, {}, {}
    snapshot_path = Path(str(gate.get("snapshot_path") or "")).resolve()
    cert_path = Path(str(gate.get("freshness_certificate_path") or "")).resolve()
    if not snapshot_path.exists() or not snapshot_path.is_file():
        return None, cert_path, {}, _read_json(cert_path)
    expected = (ctx.execution_root / "options_chain_snapshot_v1" / ctx.day_utc).resolve()
    if not str(snapshot_path).startswith(str(expected)):
        return None, cert_path, {}, _read_json(cert_path)
    return snapshot_path, cert_path, _read_json(snapshot_path), _read_json(cert_path)


def _contract_price(row: dict[str, Any], key: str) -> Decimal | None:
    value = _dec(row.get(key))
    if value is None:
        quote = row.get("quote") if isinstance(row.get("quote"), dict) else {}
        value = _dec(quote.get(key))
    return value


def _contract_ok(row: dict[str, Any], *, right: str, max_spread: Decimal) -> bool:
    if str(row.get("right") or "").strip().upper() != right:
        return False
    bid = _contract_price(row, "bid")
    ask = _contract_price(row, "ask")
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return False
    return (ask - bid) <= max_spread


def _leg_from_contract(row: dict[str, Any], *, action: str, price_field: str) -> dict[str, Any]:
    ib = row.get("ib") if isinstance(row.get("ib"), dict) else {}
    price = _contract_price(row, price_field)
    return {
        "action": action,
        "right": str(row.get("right") or "").strip().upper(),
        "strike": str(row.get("strike") or "").strip(),
        "expiry_utc": str(row.get("expiry_utc") or "").strip(),
        "bid": str(row.get("bid") or ""),
        "ask": str(row.get("ask") or ""),
        "pricing_field_used": price_field,
        "pricing_value": str(price) if price is not None else "",
        "contract_key": str(row.get("contract_key") or ""),
        "ib_conId": ib.get("conId"),
        "ib_localSymbol": ib.get("localSymbol"),
        "exchange": ib.get("exchange", "SMART"),
        "ratio": 1,
    }


def _contract_identity(row: dict[str, Any]) -> tuple[str, str]:
    ib = row.get("ib") if isinstance(row.get("ib"), dict) else {}
    return str(row.get("contract_key") or "").strip(), str(ib.get("conId") or row.get("ib_conId") or "").strip()


def _snapshot_contract_index(snapshot: dict[str, Any]) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for row in snapshot.get("contracts") or []:
        if isinstance(row, dict):
            out.add(_contract_identity(row))
    return out


def _selected_legs_exist_in_snapshot(selected: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    index = _snapshot_contract_index(snapshot)
    for leg in selected.get("legs") or []:
        if not isinstance(leg, dict):
            return False
        key = str(leg.get("contract_key") or "").strip()
        conid = str(leg.get("ib_conId") or "").strip()
        if (key, conid) not in index:
            return False
    return True


def _expiry_days_from_snapshot(expiry_utc: str, snapshot: dict[str, Any]) -> int | None:
    as_of_text = str(snapshot.get("as_of_utc") or "").strip()
    if not expiry_utc or not as_of_text:
        return None
    try:
        as_of_dt = datetime.fromisoformat(as_of_text.replace("Z", "+00:00"))
        expiry_dt = datetime.fromisoformat(expiry_utc.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (expiry_dt.date() - as_of_dt.date()).days


def _near_itm_same_week_disallowed(selected: dict[str, Any], policy: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    template = policy.get("options_template") if isinstance(policy.get("options_template"), dict) else {}
    selection = template.get("selection_policy") if isinstance(template.get("selection_policy"), dict) else {}
    if selection.get("allow_near_itm_same_week") is True:
        return False
    underlying = snapshot.get("underlying") if isinstance(snapshot.get("underlying"), dict) else {}
    spot = _dec(underlying.get("spot_price"))
    if spot is None or spot <= 0:
        return False
    expiry_days = _expiry_days_from_snapshot(str(selected.get("expiry_utc") or ""), snapshot)
    if expiry_days is None or expiry_days < 0 or expiry_days > 7:
        return False
    for leg in selected.get("legs") or []:
        if not isinstance(leg, dict) or str(leg.get("action") or "").strip().upper() != "SELL":
            continue
        strike = _dec(leg.get("strike"))
        right = str(leg.get("right") or "").strip().upper()
        if right == "PUT" and strike is not None and strike >= spot:
            return True
        if right == "CALL" and strike is not None and strike <= spot:
            return True
    return False


def _selection_policy(policy: dict[str, Any]) -> dict[str, Any]:
    template = policy.get("options_template") if isinstance(policy.get("options_template"), dict) else {}
    selection = template.get("selection_policy") if isinstance(template.get("selection_policy"), dict) else {}
    return selection if isinstance(selection, dict) else {}


def _target_width_points(policy: dict[str, Any]) -> Decimal | None:
    selection = _selection_policy(policy)
    width_policy = selection.get("width_policy") if isinstance(selection.get("width_policy"), dict) else {}
    return _dec(width_policy.get("width_points"))


def _selected_width_points(selected: dict[str, Any]) -> Decimal | None:
    width = _dec(selected.get("width_points"))
    if width is not None:
        return width
    strikes: list[Decimal] = []
    for leg in selected.get("legs") or []:
        if not isinstance(leg, dict):
            continue
        strike = _dec(leg.get("strike"))
        if strike is not None:
            strikes.append(strike)
    if len(strikes) != 2:
        return None
    return abs(strikes[0] - strikes[1])


def _selected_width_matches_policy(selected: dict[str, Any], policy: dict[str, Any]) -> bool:
    target = _target_width_points(policy)
    actual = _selected_width_points(selected)
    return bool(target is not None and actual is not None and actual == target and actual > 0)


def _clearly_otm_structure(selected: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    underlying = snapshot.get("underlying") if isinstance(snapshot.get("underlying"), dict) else {}
    spot = _dec(underlying.get("spot_price"))
    width = _selected_width_points(selected)
    if spot is None or spot <= 0 or width is None or width <= 0:
        return False
    for leg in selected.get("legs") or []:
        if not isinstance(leg, dict) or str(leg.get("action") or "").strip().upper() != "SELL":
            continue
        strike = _dec(leg.get("strike"))
        right = str(leg.get("right") or "").strip().upper()
        if strike is None:
            return False
        if right == "PUT":
            return strike <= spot - width
        if right == "CALL":
            return strike >= spot + width
        return False
    return False


def _selected_legs_liquid(selected: dict[str, Any], snapshot: dict[str, Any], policy: dict[str, Any]) -> bool:
    selection = _selection_policy(policy)
    liquidity = selection.get("liquidity_policy") if isinstance(selection.get("liquidity_policy"), dict) else {}
    max_spread = _dec(liquidity.get("max_bid_ask_spread")) or Decimal("0.10")
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for row in snapshot.get("contracts") or []:
        if isinstance(row, dict):
            by_identity[_contract_identity(row)] = row
    for leg in selected.get("legs") or []:
        if not isinstance(leg, dict):
            return False
        key = str(leg.get("contract_key") or "").strip()
        conid = str(leg.get("ib_conId") or "").strip()
        row = by_identity.get((key, conid))
        if row is None:
            return False
        bid = _contract_price(row, "bid")
        ask = _contract_price(row, "ask")
        if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
            return False
        if ask - bid > max_spread:
            return False
    return True


def _selected_structure_guard_blocker(selected: dict[str, Any], policy: dict[str, Any], snapshot: dict[str, Any]) -> tuple[str, str]:
    if not _selected_legs_exist_in_snapshot(selected, snapshot):
        return "STRUCTURE_DECISION_VALIDATION_FAILED", "Selected option legs are absent from the latest accepted market-open snapshot; rerun market-open gate and structure selection."
    if not _selected_width_matches_policy(selected, policy):
        return "NO_ELIGIBLE_OPTION_STRUCTURE", "Selected spread width does not match the governed moderate width policy."
    if not _clearly_otm_structure(selected, snapshot):
        return "NO_ELIGIBLE_OPTION_STRUCTURE", "Selected structure is not clearly OTM by at least one governed spread width."
    if not _selected_legs_liquid(selected, snapshot, policy):
        return "NO_ELIGIBLE_OPTION_STRUCTURE", "Selected option legs do not satisfy governed bid/ask liquidity limits."
    if _near_itm_same_week_disallowed(selected, policy, snapshot):
        return "NO_ELIGIBLE_OPTION_STRUCTURE", "Near/ITM same-week structures are disallowed unless explicitly policy-approved and broker preview-valid."
    return "", ""


def _intent_budget(risk_budget: dict[str, Any], intent_id: str) -> dict[str, Any]:
    for row in risk_budget.get("intent_budgets") or []:
        if isinstance(row, dict) and str(row.get("intent_id") or "").strip() == intent_id:
            return row
    return {}


def _select_vertical_put_credit_spread(
    *,
    intent: dict[str, Any],
    policy: dict[str, Any],
    snapshot: dict[str, Any],
    risk_budget: dict[str, Any],
    intent_id: str,
) -> tuple[dict[str, Any], str]:
    template = policy.get("options_template") if isinstance(policy.get("options_template"), dict) else {}
    strategy = template.get("strategy") if isinstance(template.get("strategy"), dict) else {}
    risk = template.get("risk") if isinstance(template.get("risk"), dict) else {}
    selection = _selection_policy(policy)
    liquidity = selection.get("liquidity_policy") if isinstance(selection.get("liquidity_policy"), dict) else {}
    target_width = _target_width_points(policy)
    option = intent.get("option") if isinstance(intent.get("option"), dict) else {}
    right = str(strategy.get("right") or option.get("structure") or "").strip().upper()
    direction = str(strategy.get("direction") or "").strip().upper()
    if right != "PUT" or direction != "CREDIT":
        return {}, "STRUCTURE_POLICY_MISSING"
    max_spread = _dec(liquidity.get("max_bid_ask_spread")) or Decimal("0.10")
    max_contracts = int(risk.get("max_contracts") or 1)
    multiplier = int(risk.get("multiplier") or 100)
    policy_max_risk_cents = int((_dec(risk.get("max_risk_usd")) or Decimal("0")) * 100)
    budget = _intent_budget(risk_budget, intent_id)
    allowed_risk_cents = int(budget.get("allowed_risk_cents") or 0)
    positive_caps = [value for value in (policy_max_risk_cents, allowed_risk_cents) if value > 0]
    if not positive_caps:
        return {}, "RISK_BUDGET_SUPPLY_BLOCKED"
    max_risk_cents = min(positive_caps)
    contracts = [
        row for row in snapshot.get("contracts") or []
        if isinstance(row, dict) and _contract_ok(row, right=right, max_spread=max_spread)
    ]
    by_expiry: dict[str, list[dict[str, Any]]] = {}
    for row in contracts:
        by_expiry.setdefault(str(row.get("expiry_utc") or ""), []).append(row)
    candidates: list[dict[str, Any]] = []
    for expiry, rows in by_expiry.items():
        ordered = sorted(rows, key=lambda item: _dec(item.get("strike")) or Decimal("0"))
        for sell in ordered:
            sell_strike = _dec(sell.get("strike"))
            sell_bid = _contract_price(sell, "bid")
            if sell_strike is None or sell_bid is None:
                continue
            for buy in ordered:
                buy_strike = _dec(buy.get("strike"))
                buy_ask = _contract_price(buy, "ask")
                if buy_strike is None or buy_ask is None or buy_strike >= sell_strike:
                    continue
                width = sell_strike - buy_strike
                if target_width is None or width != target_width:
                    continue
                credit = sell_bid - buy_ask
                if credit <= 0:
                    continue
                max_loss_cents = int(((width - credit) * multiplier * 100).to_integral_value())
                if max_loss_cents <= 0 or max_loss_cents > max_risk_cents:
                    continue
                candidates.append(
                    {
                        "expiry_utc": expiry,
                        "width_points": str(width),
                        "net_credit": str(credit),
                        "max_loss_cents": max_loss_cents,
                        "legs": [
                            _leg_from_contract(sell, action="SELL", price_field="bid"),
                            _leg_from_contract(buy, action="BUY", price_field="ask"),
                        ],
                    }
                )
    if not candidates:
        return {}, "NO_ELIGIBLE_OPTION_STRUCTURE"
    candidates.sort(key=lambda item: (int(item["max_loss_cents"]), Decimal(str(item["width_points"])), Decimal(str(item["net_credit"]))))
    selected = candidates[0]
    selected["quantity_basis"] = {
        "max_contracts_policy": max_contracts,
        "allowed_risk_cents": allowed_risk_cents,
        "policy_max_risk_cents": policy_max_risk_cents,
        "selected_contracts": min(max_contracts, max(1, max_risk_cents // int(selected["max_loss_cents"]))),
    }
    return selected, ""


def _candidate_from_intent(intent_path: Path, intent: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    option = intent.get("option") if isinstance(intent.get("option"), dict) else {}
    direction = str(option.get("direction") or intent.get("direction") or "").strip().upper()
    if direction == "SELL":
        direction = "SHORT"
    underlying = snapshot.get("underlying") if isinstance(snapshot.get("underlying"), dict) else {}
    return {
        "candidate_id": _intent_id(intent_path, intent),
        "timestamp": str(snapshot.get("as_of_utc") or intent.get("created_at_utc") or ""),
        "sleeve_id": _engine_id(intent) or "UNKNOWN",
        "symbol": _symbol(intent),
        "direction": direction or "UNKNOWN",
        "current_regime": "TRENDING",
        "confidence": "UNKNOWN",
        "expected_holding_days": intent.get("expected_holding_days"),
        "raw_signal_payload": {"intent_path": str(intent_path), "intent_id": _intent_id(intent_path, intent)},
        "underlying_spot": underlying.get("spot_price"),
    }


def _blocked(ctx: bod.BodContext, blocker: str, action: str, **sections: Any) -> dict[str, Any]:
    if blocker not in ALLOWED_BLOCKERS:
        blocker = "STRUCTURE_DECISION_VALIDATION_FAILED"
    return {
        "schema_id": "structure_decision_supply",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": _now_iso(),
        "status": "BLOCKED",
        "canonical_blocker": blocker,
        "active_intents": sections.get("active_intents", []),
        "market_open_data": sections.get("market_open_data", {}),
        "risk_budget_input": sections.get("risk_budget_input", {}),
        "structure_policy": sections.get("structure_policy", {}),
        "structure_decisions": sections.get("structure_decisions", []),
        "structure_export": {"usable_for_authorization_supply": False, "decision_count": 0, "decisions": []},
        "operator_next_action": action,
    }


def build_structure_decision_supply_v1(ctx: bod.BodContext) -> dict[str, Any]:
    intents = _active_intents(ctx)
    active_rows = [
        {
            "intent_id": _intent_id(path, payload),
            "intent_hash": _intent_hash(path, payload),
            "intent_path": str(path),
            "instrument": _symbol(payload),
            "engine_id": _engine_id(payload),
            "requires_defined_risk": True,
        }
        for path, payload in intents
    ]
    if not intents:
        return _blocked(ctx, "ACTIVE_INTENT_MISSING", "Generate current-day active intent before structure selection.", active_intents=[])
    risk_path = _risk_budget_path(ctx)
    risk_budget = _read_json(risk_path)
    risk_status = str(risk_budget.get("status") or "").strip().upper()
    risk_input = {"path": str(risk_path), "status": risk_status or "MISSING", "canonical_blocker": str(risk_budget.get("canonical_blocker") or "")}
    if risk_status != "PASS":
        return _blocked(ctx, "RISK_BUDGET_SUPPLY_BLOCKED", "Resolve Risk Budget Supply before structure selection.", active_intents=active_rows, risk_budget_input=risk_input)
    snapshot_path, cert_path, snapshot, _cert = _snapshot_from_gate(ctx)
    market_open_data = {
        "market_open_data_gate_path": str(_market_open_gate_path(ctx)),
        "snapshot_path": str(snapshot_path or ""),
        "freshness_certificate_path": str(cert_path or ""),
        "status": "PASS" if snapshot else "MISSING",
    }
    if snapshot_path is None or not snapshot:
        return _blocked(ctx, "MARKET_OPEN_DATA_MISSING", "Run market-open data gate and produce current-day quote-complete options snapshot.", active_intents=active_rows, risk_budget_input=risk_input, market_open_data=market_open_data)
    decisions: list[dict[str, Any]] = []
    for intent_path, intent in intents:
        intent_id = _intent_id(intent_path, intent)
        policy = _policy_for_intent(intent)
        if not policy:
            return _blocked(ctx, "STRUCTURE_POLICY_MISSING", "Add governed exposure-to-options policy for the active intent engine.", active_intents=active_rows, risk_budget_input=risk_input, market_open_data=market_open_data)
        selected, select_blocker = _select_vertical_put_credit_spread(intent=intent, policy=policy, snapshot=snapshot, risk_budget=risk_budget, intent_id=intent_id)
        if select_blocker:
            return _blocked(ctx, select_blocker, "No current-day option legs satisfy policy, quote, and risk constraints; rerun after richer option chain capture or adjust governed policy.", active_intents=active_rows, risk_budget_input=risk_input, market_open_data=market_open_data, structure_policy={"path": str(POLICY_PATH), "engine_id": _engine_id(intent)})
        guard_blocker, guard_action = _selected_structure_guard_blocker(selected, policy, snapshot)
        if guard_blocker:
            return _blocked(ctx, guard_blocker, guard_action, active_intents=active_rows, risk_budget_input=risk_input, market_open_data=market_open_data, structure_policy={"path": str(POLICY_PATH), "engine_id": _engine_id(intent)})
        structure_input = _candidate_from_intent(intent_path, intent, snapshot)
        base_decision = select_structure_for_candidate_v1(structure_input)
        if base_decision.get("structure_status") != STRUCTURE_STATUS_SELECTED:
            return _blocked(ctx, "NO_ELIGIBLE_OPTION_STRUCTURE", str(base_decision.get("structure_reason") or "Structure selection rejected active intent."), active_intents=active_rows, risk_budget_input=risk_input, market_open_data=market_open_data, structure_decisions=[base_decision])
        decision = {
            **base_decision,
            "day_utc": ctx.day_utc,
            "intent_id": intent_id,
            "intent_hash": _intent_hash(intent_path, intent),
            "intent_path": str(intent_path),
            "selected_structure": "VERTICAL_SPREAD",
            "option_structure": {
                "selected_structure": "VERTICAL_SPREAD",
                "strategy_direction": "CREDIT",
                "right": "PUT",
                "legs": selected["legs"],
                "net_credit": selected["net_credit"],
                "width_points": selected["width_points"],
                "max_loss_cents": selected["max_loss_cents"],
                "quantity_basis": selected["quantity_basis"],
            },
            "pricing_inputs": {
                "snapshot_path": str(snapshot_path),
                "freshness_certificate_path": str(cert_path or ""),
                "snapshot_as_of_utc": str(snapshot.get("as_of_utc") or ""),
                "data_mode": "DELAYED" if (snapshot.get("provenance") or {}).get("market_data_type") in {3, 4} else "LIVE_OR_UNKNOWN",
            },
            "risk_bounds": {
                "allowed_risk_cents": selected["quantity_basis"]["allowed_risk_cents"],
                "policy_max_risk_cents": selected["quantity_basis"]["policy_max_risk_cents"],
                "max_loss_cents": selected["max_loss_cents"],
            },
        }
        decision["structure_decision_hash"] = hashlib.sha256(
            canonical_structure_decision_json_v1(decision).encode("utf-8")
        ).hexdigest()
        decisions.append(decision)
    return {
        "schema_id": "structure_decision_supply",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": _now_iso(),
        "status": "PASS",
        "canonical_blocker": "",
        "active_intents": active_rows,
        "market_open_data": market_open_data,
        "risk_budget_input": risk_input,
        "structure_policy": {"path": str(POLICY_PATH), "status": "PRESENT"},
        "structure_decisions": decisions,
        "structure_export": {
            "usable_for_authorization_supply": True,
            "decision_count": len(decisions),
            "decisions": [
                {
                    "intent_id": row["intent_id"],
                    "intent_hash": row["intent_hash"],
                    "selected_structure": row["selected_structure"],
                    "legs": row["option_structure"]["legs"],
                    "max_loss_cents": row["option_structure"]["max_loss_cents"],
                }
                for row in decisions
            ],
        },
        "operator_next_action": "",
    }


def run_structure_decision_supply_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_structure_decision_supply_v1(ctx)
    path = structure_decision_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_structure_decision_supply_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    path, payload = run_structure_decision_supply_v1(day_utc, str(args.environment or "PAPER").strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": str(path)}, sort_keys=True))
    return 0 if payload.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
