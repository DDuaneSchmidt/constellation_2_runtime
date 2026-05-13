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
from ops.tools.run_intent_arbitration_v1 import intent_arbitration_path, selected_intent_pointer_path

SCHEMA_VERSION = "structure_decision_supply.v1"
POLICY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json"
EQUITY_POLICY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_EQUITY_STRUCTURE_POLICY_V1.json"
ENGINE_REGISTRY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "ENGINE_MODEL_REGISTRY_V1.json"
ALLOWED_BLOCKERS = {
    "ACTIVE_INTENT_MISSING",
    "MARKET_OPEN_DATA_MISSING",
    "OPTIONS_SNAPSHOT_MISSING",
    "RISK_BUDGET_SUPPLY_BLOCKED",
    "NO_ELIGIBLE_OPTION_STRUCTURE",
    "STRUCTURE_POLICY_MISSING",
    "EQUITY_STRUCTURE_POLICY_MISSING",
    "EQUITY_STRUCTURE_POLICY_INVALID",
    "STRUCTURE_DECISION_VALIDATION_FAILED",
    "INTENT_ARBITRATION_MISSING",
    "NO_EXECUTABLE_INTENT",
    "ALLOWED_SYMBOL_MISMATCH",
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
    pointer_path = selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    if pointer_path.exists() and pointer_path.is_file():
        pointer = _read_json(pointer_path)
        if str(pointer.get("status") or "").strip().upper() != "SELECTED":
            return []
        selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
        intent_path = str(selected.get("intent_path") or "").strip()
        if intent_path:
            path = Path(intent_path).expanduser().resolve()
            return [path] if path.exists() and path.is_file() else []
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


def _exposure_type(payload: dict[str, Any]) -> str:
    return str(payload.get("exposure_type") or "").strip().upper()


def _target_notional_pct(payload: dict[str, Any]) -> str:
    return str(payload.get("target_notional_pct") or "").strip()


def _max_risk_pct(payload: dict[str, Any]) -> str:
    constraints = payload.get("constraints") if isinstance(payload.get("constraints"), dict) else {}
    return str(constraints.get("max_risk_pct") or "").strip()


def _allowed_symbols_for_engine(engine_id: str) -> list[str]:
    registry = _read_json(ENGINE_REGISTRY_PATH)
    for row in registry.get("engines") if isinstance(registry.get("engines"), list) else []:
        if not isinstance(row, dict) or str(row.get("engine_id") or "").strip() != engine_id:
            continue
        raw = row.get("allowed_symbols") if isinstance(row.get("allowed_symbols"), list) else []
        return [str(symbol).strip().upper() for symbol in raw if str(symbol).strip()]
    return []


def _bump_rejection(diagnostics: dict[str, Any], reason: str, count: int = 1) -> None:
    rejected = diagnostics.setdefault("rejected_by_reason", {})
    rejected[reason] = int(rejected.get(reason) or 0) + count


def _empty_structure_diagnostics(
    *,
    intent_id: str = "",
    selected_symbol: str = "",
    allowed_symbols: list[str] | None = None,
    option_chain_snapshot_path: str = "",
) -> dict[str, Any]:
    return {
        "intent_id": intent_id,
        "selected_symbol": selected_symbol,
        "allowed_symbols": sorted({str(symbol).strip().upper() for symbol in (allowed_symbols or []) if str(symbol).strip()}),
        "option_chain_snapshot_path": option_chain_snapshot_path,
        "candidates_seen": 0,
        "candidates_eligible": 0,
        "rejected_by_reason": {},
    }


def _pointer_diagnostics(pointer: dict[str, Any]) -> dict[str, Any]:
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    intent_id = str(selected.get("intent_id") or "").strip()
    selected_symbol = str(selected.get("symbol") or "").strip().upper()
    fallback_allowed_symbols: list[str] = []
    matched_allowed_symbols: list[str] = []
    for ref_key in ("source_rollup_path", "source_arbitration_path"):
        ref_path = Path(str(pointer.get(ref_key) or "")).expanduser()
        payload = _read_json(ref_path)
        outcomes = payload.get("sleeve_outcomes") if isinstance(payload.get("sleeve_outcomes"), list) else payload.get("outcomes")
        for outcome in outcomes if isinstance(outcomes, list) else []:
            if not isinstance(outcome, dict):
                continue
            raw_allowed = outcome.get("allowed_symbols") if isinstance(outcome.get("allowed_symbols"), list) else outcome.get("registry_allowed_symbols")
            if isinstance(raw_allowed, list):
                fallback_allowed_symbols.extend(str(symbol).strip().upper() for symbol in raw_allowed if str(symbol).strip())
            rejected = outcome.get("rejected_intents") if isinstance(outcome.get("rejected_intents"), list) else []
            if rejected and isinstance(rejected[0], dict):
                if not selected_symbol:
                    selected_symbol = str(rejected[0].get("symbol") or "").strip().upper()
                if not intent_id:
                    intent_id = str(rejected[0].get("intent_id") or "").strip()
                if isinstance(raw_allowed, list):
                    matched_allowed_symbols.extend(str(symbol).strip().upper() for symbol in raw_allowed if str(symbol).strip())
    diagnostics = _empty_structure_diagnostics(
        intent_id=intent_id,
        selected_symbol=selected_symbol,
        allowed_symbols=matched_allowed_symbols or fallback_allowed_symbols,
    )
    blocker = str(pointer.get("canonical_blocker") or pointer.get("status") or "").strip()
    if blocker:
        _bump_rejection(diagnostics, blocker)
    return diagnostics


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


def _equity_policy_for_intent(intent: dict[str, Any]) -> dict[str, Any]:
    policy = _read_json(EQUITY_POLICY_PATH)
    engine_id = _engine_id(intent)
    for row in policy.get("engine_policies") or []:
        if isinstance(row, dict) and str(row.get("engine_id") or "").strip() == engine_id:
            return row
    return {}


def _validate_equity_policy(intent: dict[str, Any], policy: dict[str, Any]) -> tuple[bool, str]:
    requirements = policy.get("exposure_requirements") if isinstance(policy.get("exposure_requirements"), dict) else {}
    template = policy.get("structure_template") if isinstance(policy.get("structure_template"), dict) else {}
    safety = template.get("safety") if isinstance(template.get("safety"), dict) else {}
    symbol = _symbol(intent)
    target_notional_pct = _target_notional_pct(intent)
    max_risk_pct = _max_risk_pct(intent)
    allowed_symbols = [
        str(item).strip().upper()
        for item in requirements.get("allowed_symbols") or []
        if str(item).strip()
    ]
    allowed_notional = [
        str(item).strip()
        for item in requirements.get("allowed_target_notional_pct") or []
        if str(item).strip()
    ]
    max_risk_pct_max = _dec(requirements.get("max_risk_pct_max"))
    intent_max_risk = _dec(max_risk_pct)
    if str(requirements.get("exposure_type") or "").strip().upper() != "LONG_EQUITY":
        return False, "Equity structure policy must explicitly govern LONG_EQUITY exposure."
    if symbol not in allowed_symbols:
        return False, "Equity structure policy does not allow the selected symbol."
    if target_notional_pct not in allowed_notional:
        return False, "Equity structure policy does not allow the selected target_notional_pct."
    if max_risk_pct_max is not None:
        if intent_max_risk is None:
            return False, "Equity structure policy requires numeric constraints.max_risk_pct."
        if intent_max_risk <= 0 or intent_max_risk > max_risk_pct_max:
            return False, "Equity structure policy does not allow the selected max_risk_pct."
    if str(template.get("structure_type") or "").strip().upper() != "EQUITY_SPOT":
        return False, "Equity structure policy must emit EQUITY_SPOT."
    if str(template.get("order_intent_type") or "").strip().upper() != "EQUITY_BUY":
        return False, "LONG_EQUITY policy must emit EQUITY_BUY order intent evidence."
    if safety.get("execution_authority_granted") is not False:
        return False, "Equity structure policy must not grant execution authority."
    if safety.get("order_submission_attempted") is not False:
        return False, "Equity structure policy must not mark order submission attempted."
    if safety.get("trading_behavior_changed") is not False:
        return False, "Equity structure policy must not change trading behavior."
    return True, ""


def _build_equity_spot_decision(
    *,
    ctx: bod.BodContext,
    intent_path: Path,
    intent: dict[str, Any],
    policy: dict[str, Any],
    risk_budget: dict[str, Any],
    market_open_data: dict[str, Any],
    intent_id: str,
) -> dict[str, Any]:
    template = policy.get("structure_template") if isinstance(policy.get("structure_template"), dict) else {}
    safety = template.get("safety") if isinstance(template.get("safety"), dict) else {}
    constraints = intent.get("constraints") if isinstance(intent.get("constraints"), dict) else {}
    diagnostics = {
        "intent_id": intent_id,
        "selected_symbol": _symbol(intent),
        "engine_id": _engine_id(intent),
        "exposure_type": _exposure_type(intent),
        "structure_type": "EQUITY_SPOT",
        "equity_structure_policy_path": str(EQUITY_POLICY_PATH),
        "usable_for_authorization_supply": False,
        "market_open_data_gate_path": str(market_open_data.get("market_open_data_gate_path") or ""),
        "options_policy_queried": False,
        "execution_authority_granted": False,
        "order_submission_attempted": False,
        "trading_behavior_changed": False,
    }
    decision = {
        "day_utc": ctx.day_utc,
        "intent_id": intent_id,
        "intent_hash": _intent_hash(intent_path, intent),
        "intent_path": str(intent_path),
        "selected_structure": "EQUITY_SPOT",
        "structure_type": "EQUITY_SPOT",
        "symbol": _symbol(intent),
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": _target_notional_pct(intent),
        "order_intent_type": "EQUITY_BUY",
        "execution_authority_granted": False,
        "order_submission_attempted": False,
        "trading_behavior_changed": False,
        "equity_structure_policy": {
            "path": str(EQUITY_POLICY_PATH),
            "engine_id": _engine_id(intent),
            "status": "PRESENT",
            "structure_type": "EQUITY_SPOT",
        },
        "pricing_inputs": {
            "snapshot_path": str(market_open_data.get("snapshot_path") or ""),
            "freshness_certificate_path": str(market_open_data.get("freshness_certificate_path") or ""),
            "data_mode": "MARKET_OPEN_GATE_SUPPLIED",
        },
        "risk_bounds": {
            "target_notional_pct": _target_notional_pct(intent),
            "max_risk_pct": str(constraints.get("max_risk_pct") or ""),
            "risk_budget_status": str(risk_budget.get("status") or ""),
        },
        "safety": {
            "execution_authority_granted": bool(safety.get("execution_authority_granted")),
            "order_submission_attempted": bool(safety.get("order_submission_attempted")),
            "trading_behavior_changed": bool(safety.get("trading_behavior_changed")),
            "requires_submit_boundary": safety.get("requires_submit_boundary") is True,
        },
        "structure_diagnostics": diagnostics,
    }
    decision["structure_decision_hash"] = hashlib.sha256(
        canonical_structure_decision_json_v1(decision).encode("utf-8")
    ).hexdigest()
    return decision


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


def _nearest_miss_row(
    *,
    expiry: str,
    sell: dict[str, Any],
    buy: dict[str, Any],
    width: Decimal,
    credit: Decimal,
    max_loss_cents: int,
    max_risk_cents: int,
    multiplier: int,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    sell_strike = _dec(sell.get("strike")) or Decimal("0")
    buy_strike = _dec(buy.get("strike")) or Decimal("0")
    additional_credit_needed = Decimal(max_loss_cents - max_risk_cents) / Decimal(multiplier * 100)
    underlying = snapshot.get("underlying") if isinstance(snapshot.get("underlying"), dict) else {}
    spot = _dec(underlying.get("spot_price"))
    clearly_otm = bool(spot is not None and spot > 0 and sell_strike <= spot - width)
    return {
        "failed_rule": "MAX_LOSS_EXCEEDS_RISK",
        "expiry_utc": expiry,
        "sell_strike": str(sell_strike),
        "buy_strike": str(buy_strike),
        "width_points": str(width),
        "net_credit": str(credit),
        "max_loss_cents": int(max_loss_cents),
        "effective_max_risk_cents": int(max_risk_cents),
        "max_loss_excess_cents": int(max_loss_cents - max_risk_cents),
        "additional_credit_needed": str(additional_credit_needed),
        "clearly_otm": clearly_otm,
        "distance_from_threshold": {
            "threshold": "effective_max_risk_cents",
            "actual": int(max_loss_cents),
            "limit": int(max_risk_cents),
            "excess_cents": int(max_loss_cents - max_risk_cents),
        },
    }


def _trim_nearest_misses(rows: list[dict[str, Any]], *, clearly_otm_only: bool = False, limit: int = 10) -> list[dict[str, Any]]:
    filtered = [row for row in rows if not clearly_otm_only or row.get("clearly_otm") is True]
    filtered.sort(
        key=lambda row: (
            int(row.get("max_loss_excess_cents") or 0),
            Decimal(str(row.get("sell_strike") or "0")),
            Decimal(str(row.get("buy_strike") or "0")),
        )
    )
    return filtered[:limit]


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


def _structure_dte_coverage(snapshot: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    selection = _selection_policy(policy)
    expiry_policy = selection.get("expiry_policy") if isinstance(selection.get("expiry_policy"), dict) else {}
    policy_dte_min = int(expiry_policy.get("target_dte_min") or 0)
    policy_dte_max = int(expiry_policy.get("target_dte_max") or 0)
    derived = snapshot.get("derived") if isinstance(snapshot.get("derived"), dict) else {}
    derivation_policy = derived.get("derivation_policy") if isinstance(derived.get("derivation_policy"), dict) else {}
    raw_coverage = derivation_policy.get("dte_window_policy") if isinstance(derivation_policy.get("dte_window_policy"), dict) else {}
    if raw_coverage:
        return {
            "policy_dte_min": int(raw_coverage.get("policy_dte_min") or policy_dte_min),
            "policy_dte_max": int(raw_coverage.get("policy_dte_max") or policy_dte_max),
            "max_expiries_to_capture": int(raw_coverage.get("max_expiries_to_capture") or 0),
            "expiries_available": list(raw_coverage.get("expiries_available") or []),
            "expiries_evaluated": list(raw_coverage.get("expiries_evaluated") or []),
            "expiries_omitted": list(raw_coverage.get("expiries_omitted") or []),
            "omission_reason": str(raw_coverage.get("omission_reason") or ""),
        }
    evaluated = sorted(
        {
            str(row.get("expiry_utc") or "").strip()
            for row in snapshot.get("contracts") or []
            if isinstance(row, dict) and str(row.get("expiry_utc") or "").strip()
        }
    )
    return {
        "policy_dte_min": policy_dte_min,
        "policy_dte_max": policy_dte_max,
        "max_expiries_to_capture": 0,
        "expiries_available": [],
        "expiries_evaluated": [{"expiry_utc": expiry} for expiry in evaluated],
        "expiries_omitted": [],
        "omission_reason": "LEGACY_SNAPSHOT_WITHOUT_DTE_COVERAGE_DIAGNOSTICS",
    }


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
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    engine_id = _engine_id(intent)
    diagnostics = _empty_structure_diagnostics(
        intent_id=intent_id,
        selected_symbol=_symbol(intent),
        allowed_symbols=_allowed_symbols_for_engine(engine_id),
    )
    diagnostics["dte_coverage"] = _structure_dte_coverage(snapshot, policy)
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
        _bump_rejection(diagnostics, "STRUCTURE_POLICY_MISSING")
        return {}, "STRUCTURE_POLICY_MISSING", diagnostics
    max_spread = _dec(liquidity.get("max_bid_ask_spread")) or Decimal("0.10")
    max_contracts = int(risk.get("max_contracts") or 1)
    multiplier = int(risk.get("multiplier") or 100)
    policy_max_risk_cents = int((_dec(risk.get("max_risk_usd")) or Decimal("0")) * 100)
    budget = _intent_budget(risk_budget, intent_id)
    allowed_risk_cents = int(budget.get("allowed_risk_cents") or 0)
    positive_caps = [value for value in (policy_max_risk_cents, allowed_risk_cents) if value > 0]
    if not positive_caps:
        _bump_rejection(diagnostics, "RISK_BUDGET_UNAVAILABLE")
        return {}, "RISK_BUDGET_SUPPLY_BLOCKED", diagnostics
    max_risk_cents = min(positive_caps)
    contracts = [
        row for row in snapshot.get("contracts") or []
        if isinstance(row, dict) and _contract_ok(row, right=right, max_spread=max_spread)
    ]
    right_contract_count = len([row for row in snapshot.get("contracts") or [] if isinstance(row, dict) and str(row.get("right") or "").strip().upper() == right])
    if not contracts and right_contract_count:
        _bump_rejection(diagnostics, "NO_LIQUID_CONTRACTS", right_contract_count)
    by_expiry: dict[str, list[dict[str, Any]]] = {}
    for row in contracts:
        by_expiry.setdefault(str(row.get("expiry_utc") or ""), []).append(row)
    candidates: list[dict[str, Any]] = []
    max_loss_nearest_misses: list[dict[str, Any]] = []
    for expiry, rows in by_expiry.items():
        ordered = sorted(rows, key=lambda item: _dec(item.get("strike")) or Decimal("0"))
        for sell in ordered:
            sell_strike = _dec(sell.get("strike"))
            sell_bid = _contract_price(sell, "bid")
            for buy in ordered:
                buy_strike = _dec(buy.get("strike"))
                buy_ask = _contract_price(buy, "ask")
                if sell_strike is None or sell_bid is None or buy_strike is None or buy_ask is None:
                    _bump_rejection(diagnostics, "MISSING_STRIKE_OR_PRICE")
                    continue
                if buy_strike >= sell_strike:
                    continue
                diagnostics["candidates_seen"] = int(diagnostics["candidates_seen"]) + 1
                width = sell_strike - buy_strike
                if target_width is None or width != target_width:
                    _bump_rejection(diagnostics, "WIDTH_POLICY_MISMATCH")
                    continue
                credit = sell_bid - buy_ask
                if credit <= 0:
                    _bump_rejection(diagnostics, "NON_POSITIVE_CREDIT")
                    continue
                max_loss_cents = int(((width - credit) * multiplier * 100).to_integral_value())
                if max_loss_cents <= 0:
                    _bump_rejection(diagnostics, "INVALID_MAX_LOSS")
                    continue
                if max_loss_cents > max_risk_cents:
                    _bump_rejection(diagnostics, "MAX_LOSS_EXCEEDS_RISK")
                    max_loss_nearest_misses.append(
                        _nearest_miss_row(
                            expiry=expiry,
                            sell=sell,
                            buy=buy,
                            width=width,
                            credit=credit,
                            max_loss_cents=max_loss_cents,
                            max_risk_cents=max_risk_cents,
                            multiplier=multiplier,
                            snapshot=snapshot,
                        )
                    )
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
    diagnostics["candidates_eligible"] = len(candidates)
    diagnostics["nearest_miss_diagnostics"] = {
        "top_by_max_loss_excess": _trim_nearest_misses(max_loss_nearest_misses),
        "top_clearly_otm_by_max_loss_excess": _trim_nearest_misses(max_loss_nearest_misses, clearly_otm_only=True),
    }
    if not candidates:
        return {}, "NO_ELIGIBLE_OPTION_STRUCTURE", diagnostics
    candidates.sort(key=lambda item: (int(item["max_loss_cents"]), Decimal(str(item["width_points"])), Decimal(str(item["net_credit"]))))
    selected = candidates[0]
    selected["quantity_basis"] = {
        "max_contracts_policy": max_contracts,
        "allowed_risk_cents": allowed_risk_cents,
        "policy_max_risk_cents": policy_max_risk_cents,
        "selected_contracts": min(max_contracts, max(1, max_risk_cents // int(selected["max_loss_cents"]))),
    }
    return selected, "", diagnostics


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
        "structure_diagnostics": sections.get("structure_diagnostics", []),
        "structure_decisions": sections.get("structure_decisions", []),
        "structure_export": {"usable_for_authorization_supply": False, "decision_count": 0, "decisions": []},
        "operator_next_action": action,
    }


def build_structure_decision_supply_v1(ctx: bod.BodContext) -> dict[str, Any]:
    arbitration_path = intent_arbitration_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    pointer_path = selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    if not arbitration_path.exists() and not pointer_path.exists():
        return _blocked(
            ctx,
            "INTENT_ARBITRATION_MISSING",
            "Run sleeve evaluation and intent arbitration before structure selection.",
            active_intents=[],
        )
    if pointer_path.exists() and pointer_path.is_file():
        pointer = _read_json(pointer_path)
        if str(pointer.get("status") or "").strip().upper() != "SELECTED":
            return _blocked(
                ctx,
                str(pointer.get("canonical_blocker") or pointer.get("status") or "NO_EXECUTABLE_INTENT"),
                "Resolve intent arbitration before structure selection.",
                active_intents=[],
                structure_diagnostics=[_pointer_diagnostics(pointer)],
            )
    intents = _active_intents(ctx)
    active_rows = [
        {
            "intent_id": _intent_id(path, payload),
            "intent_hash": _intent_hash(path, payload),
            "intent_path": str(path),
            "instrument": _symbol(payload),
            "engine_id": _engine_id(payload),
            "exposure_type": _exposure_type(payload),
            "requires_defined_risk": _exposure_type(payload) != "LONG_EQUITY",
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
    policy_paths: list[dict[str, Any]] = []
    for intent_path, intent in intents:
        intent_id = _intent_id(intent_path, intent)
        if _exposure_type(intent) == "LONG_EQUITY":
            policy = _equity_policy_for_intent(intent)
            if not policy:
                return _blocked(
                    ctx,
                    "EQUITY_STRUCTURE_POLICY_MISSING",
                    "Add governed equity structure policy for the active LONG_EQUITY intent engine.",
                    active_intents=active_rows,
                    risk_budget_input=risk_input,
                    market_open_data=market_open_data,
                    structure_policy={"path": str(EQUITY_POLICY_PATH), "engine_id": _engine_id(intent), "status": "MISSING"},
                )
            valid_policy, policy_error = _validate_equity_policy(intent, policy)
            if not valid_policy:
                return _blocked(
                    ctx,
                    "EQUITY_STRUCTURE_POLICY_INVALID",
                    policy_error,
                    active_intents=active_rows,
                    risk_budget_input=risk_input,
                    market_open_data=market_open_data,
                    structure_policy={"path": str(EQUITY_POLICY_PATH), "engine_id": _engine_id(intent), "status": "INVALID"},
                )
            decisions.append(
                _build_equity_spot_decision(
                    ctx=ctx,
                    intent_path=intent_path,
                    intent=intent,
                    policy=policy,
                    risk_budget=risk_budget,
                    market_open_data=market_open_data,
                    intent_id=intent_id,
                )
            )
            policy_paths.append(
                {"path": str(EQUITY_POLICY_PATH), "engine_id": _engine_id(intent), "policy_type": "EQUITY_STRUCTURE", "status": "PRESENT"}
            )
            continue
        policy = _policy_for_intent(intent)
        if not policy:
            return _blocked(ctx, "STRUCTURE_POLICY_MISSING", "Add governed exposure-to-options policy for the active intent engine.", active_intents=active_rows, risk_budget_input=risk_input, market_open_data=market_open_data)
        policy_paths.append(
            {"path": str(POLICY_PATH), "engine_id": _engine_id(intent), "policy_type": "OPTIONS_DEFINED_RISK", "status": "PRESENT"}
        )
        selected, select_blocker, diagnostics = _select_vertical_put_credit_spread(intent=intent, policy=policy, snapshot=snapshot, risk_budget=risk_budget, intent_id=intent_id)
        diagnostics["option_chain_snapshot_path"] = str(snapshot_path)
        if select_blocker:
            return _blocked(ctx, select_blocker, "No current-day option legs satisfy policy, quote, and risk constraints; rerun after richer option chain capture or adjust governed policy.", active_intents=active_rows, risk_budget_input=risk_input, market_open_data=market_open_data, structure_policy={"path": str(POLICY_PATH), "engine_id": _engine_id(intent)}, structure_diagnostics=[diagnostics])
        guard_blocker, guard_action = _selected_structure_guard_blocker(selected, policy, snapshot)
        if guard_blocker:
            _bump_rejection(diagnostics, guard_blocker)
            return _blocked(ctx, guard_blocker, guard_action, active_intents=active_rows, risk_budget_input=risk_input, market_open_data=market_open_data, structure_policy={"path": str(POLICY_PATH), "engine_id": _engine_id(intent)}, structure_diagnostics=[diagnostics])
        structure_input = _candidate_from_intent(intent_path, intent, snapshot)
        base_decision = select_structure_for_candidate_v1(structure_input)
        if base_decision.get("structure_status") != STRUCTURE_STATUS_SELECTED:
            _bump_rejection(diagnostics, "BASE_STRUCTURE_SELECTOR_REJECTED")
            return _blocked(ctx, "NO_ELIGIBLE_OPTION_STRUCTURE", str(base_decision.get("structure_reason") or "Structure selection rejected active intent."), active_intents=active_rows, risk_budget_input=risk_input, market_open_data=market_open_data, structure_decisions=[base_decision], structure_diagnostics=[diagnostics])
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
            "structure_diagnostics": diagnostics,
        }
        decision["structure_decision_hash"] = hashlib.sha256(
            canonical_structure_decision_json_v1(decision).encode("utf-8")
        ).hexdigest()
        decisions.append(decision)
    export_decisions: list[dict[str, Any]] = []
    authorization_usable = True
    for row in decisions:
        if row.get("selected_structure") == "EQUITY_SPOT":
            authorization_usable = False
            export_decisions.append(
                {
                    "intent_id": row["intent_id"],
                    "intent_hash": row["intent_hash"],
                    "selected_structure": "EQUITY_SPOT",
                    "structure_type": "EQUITY_SPOT",
                    "symbol": row["symbol"],
                    "exposure_type": "LONG_EQUITY",
                    "target_notional_pct": row["target_notional_pct"],
                    "order_intent_type": "EQUITY_BUY",
                    "execution_authority_granted": False,
                    "order_submission_attempted": False,
                    "trading_behavior_changed": False,
                }
            )
            continue
        export_decisions.append(
            {
                "intent_id": row["intent_id"],
                "intent_hash": row["intent_hash"],
                "selected_structure": row["selected_structure"],
                "legs": row["option_structure"]["legs"],
                "max_loss_cents": row["option_structure"]["max_loss_cents"],
            }
        )
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
        "structure_policy": {"status": "PRESENT", "policies": policy_paths},
        "structure_diagnostics": [row["structure_diagnostics"] for row in decisions if isinstance(row.get("structure_diagnostics"), dict)],
        "structure_decisions": decisions,
        "structure_export": {
            "usable_for_authorization_supply": authorization_usable,
            "decision_count": len(decisions),
            "decisions": export_decisions,
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
