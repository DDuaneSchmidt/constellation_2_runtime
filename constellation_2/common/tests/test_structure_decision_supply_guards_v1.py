from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.run_structure_decision_supply_v1 import (  # noqa: E402
    _near_itm_same_week_disallowed,
    _select_vertical_put_credit_spread,
    _selected_legs_exist_in_snapshot,
    _selected_structure_guard_blocker,
)
from ops.tools import run_structure_decision_supply_v1 as structure_supply  # noqa: E402
from ops.tools.run_aegis_bod_prepare_v1 import BodContext  # noqa: E402


def _snapshot() -> dict:
    return {
        "as_of_utc": "2026-04-29T14:30:00Z",
        "underlying": {"spot_price": "110.00"},
        "contracts": [
            {"contract_key": "SPY-20260430-P-108", "strike": "108.00", "right": "PUT", "bid": "2.00", "ask": "2.05", "ib": {"conId": 849302108}},
            {"contract_key": "SPY-20260430-P-105", "strike": "105.00", "right": "PUT", "bid": "1.00", "ask": "1.05", "ib": {"conId": 849302105}},
            {"contract_key": "SPY-20260430-P-103", "strike": "103.00", "right": "PUT", "bid": "0.50", "ask": "0.55", "ib": {"conId": 826251103}},
            {"contract_key": "SPY-20260430-P-100", "strike": "100.00", "right": "PUT", "bid": "0.40", "ask": "0.45", "ib": {"conId": 826251100}},
        ],
    }


def _policy() -> dict:
    return {
        "options_template": {
            "selection_policy": {
                "width_policy": {"width_points": "5.00"},
                "liquidity_policy": {"max_bid_ask_spread": "0.10"},
            }
        }
    }


def _selection_policy(max_risk_usd: str = "350.00") -> dict:
    return {
        "options_template": {
            "strategy": {"right": "PUT", "direction": "CREDIT"},
            "risk": {"max_risk_usd": max_risk_usd, "max_contracts": 1, "multiplier": 100},
            "selection_policy": {
                "expiry_policy": {"mode": "DTE_WINDOW", "target_dte_min": 1, "target_dte_max": 7},
                "width_policy": {"width_points": "5.00"},
                "liquidity_policy": {"max_bid_ask_spread": "0.10"},
            },
        }
    }


def _risk_budget(allowed_risk_cents: int = 100000) -> dict:
    return {
        "intent_budgets": [
            {"intent_id": "intent-1", "allowed_risk_cents": allowed_risk_cents},
        ]
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _ctx(tmp_path: Path) -> BodContext:
    root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    return BodContext(
        day_utc="2026-05-12",
        environment="PAPER",
        truth_root=root,
        execution_root=root,
        runtime_root=tmp_path / "runtime",
        operator_input_root=tmp_path / "operator",
        ib_account="DU1234567",
    )


def _equity_intent() -> dict:
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "day_utc": "2026-05-12",
        "intent_id": "c2_trend_eq_spy_2026-05-12_v1",
        "intent_hash": "intenthash",
        "exposure_type": "LONG_EQUITY",
        "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "mode": "PAPER", "suite": "C2_HYBRID_V1"},
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "target_notional_pct": "0.01",
        "constraints": {"max_risk_pct": "0.01", "stop_loss_bps": 1000},
    }


def _cross_asset_qqq_intent(*, target_notional_pct: str = "0.10", max_risk_pct: str = "0.02", symbol: str = "QQQ") -> dict:
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "day_utc": "2026-05-12",
        "intent_id": "c2_cross_asset_trend_qqq_2026-05-12_v1",
        "intent_hash": "crossassetintenthash",
        "exposure_type": "LONG_EQUITY",
        "engine": {"engine_id": "C2_CROSS_ASSET_TREND_V1", "mode": "PAPER", "suite": "C2_HYBRID_V1"},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "target_notional_pct": target_notional_pct,
        "constraints": {"max_risk_pct": max_risk_pct},
        "risk_class": "CROSS_ASSET_TREND",
    }


def _equity_policy() -> dict:
    return {
        "schema_id": "c2_equity_structure_policy",
        "schema_version": "v1",
        "engine_policies": [
            {
                "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                "exposure_requirements": {
                    "exposure_type": "LONG_EQUITY",
                    "required_engine_suite": "C2_HYBRID_V1",
                    "allowed_target_notional_pct": ["0.01"],
                    "allowed_risk_class": ["TREND"],
                    "allowed_symbols": ["SPY"],
                },
                "structure_template": {
                    "structure_type": "EQUITY_SPOT",
                    "order_intent_type": "EQUITY_BUY",
                    "allowed_action": "BUY",
                    "order_terms": {
                        "order_type": "LIMIT",
                        "time_in_force": "DAY",
                        "reference_price_source": "MARKET_OPEN_DATA_GATE",
                    },
                    "safety": {
                        "execution_authority_granted": False,
                        "order_submission_attempted": False,
                        "trading_behavior_changed": False,
                        "requires_submit_boundary": True,
                    },
                },
            },
            {
                "engine_id": "C2_CROSS_ASSET_TREND_V1",
                "exposure_requirements": {
                    "exposure_type": "LONG_EQUITY",
                    "required_engine_suite": "C2_HYBRID_V1",
                    "allowed_target_notional_pct": ["0.10"],
                    "allowed_risk_class": ["CROSS_ASSET_TREND"],
                    "allowed_symbols": ["QQQ"],
                    "max_risk_pct_max": "0.02",
                },
                "structure_template": {
                    "structure_type": "EQUITY_SPOT",
                    "order_intent_type": "EQUITY_BUY",
                    "allowed_action": "BUY",
                    "order_terms": {
                        "order_type": "LIMIT",
                        "time_in_force": "DAY",
                        "reference_price_source": "MARKET_OPEN_DATA_GATE",
                    },
                    "safety": {
                        "execution_authority_granted": False,
                        "order_submission_attempted": False,
                        "trading_behavior_changed": False,
                        "requires_submit_boundary": True,
                    },
                },
            }
        ],
    }


def _write_equity_structure_inputs(ctx: BodContext, intent_payload: dict | None = None) -> Path:
    intent_payload = intent_payload or _equity_intent()
    intent_id = str(intent_payload.get("intent_id") or "intent").strip()
    symbol = structure_supply._symbol(intent_payload)
    intent_path = ctx.execution_root / "intents_v1" / "snapshots" / ctx.day_utc / "intent.exposure_intent.v1.json"
    _write_json(intent_path, intent_payload)
    _write_json(
        ctx.truth_root / "pointers" / "selected_intent_pointer.v1.json",
        {
            "status": "SELECTED",
            "selected_intent": {
                "intent_id": intent_id,
                "symbol": symbol,
                "intent_path": str(intent_path),
            },
        },
    )
    _write_json(
        ctx.truth_root / "reports" / "risk_budget_supply_v1" / ctx.day_utc / "risk_budget_supply.v1.json",
        {"status": "PASS", "intent_budgets": [{"intent_id": intent_id, "allowed_risk_cents": 100000}]},
    )
    snapshot_path = ctx.execution_root / "options_chain_snapshot_v1" / ctx.day_utc / "capture" / "options_chain_snapshot.v1.json"
    cert_path = ctx.execution_root / "options_chain_snapshot_v1" / ctx.day_utc / "capture" / "freshness_certificate.v1.json"
    _write_json(snapshot_path, {"as_of_utc": "2026-05-12T19:31:00Z", "underlying": {"symbol": symbol, "spot_price": "620.00"}, "contracts": []})
    _write_json(cert_path, {"status": "PASS"})
    _write_json(
        ctx.truth_root / "reports" / "market_open_data_gate_v1" / ctx.day_utc / "market_open_data_gate.v1.json",
        {"status": "PASS", "snapshot_path": str(snapshot_path), "freshness_certificate_path": str(cert_path)},
    )
    return intent_path


def _selected(sell: str = "105", buy: str = "100") -> dict:
    return {
        "expiry_utc": "2026-04-30T00:00:00Z",
        "width_points": str(abs(int(sell) - int(buy))),
        "legs": [
            {"action": "SELL", "right": "PUT", "strike": f"{sell}.00", "contract_key": f"SPY-20260430-P-{sell}", "ib_conId": int(f"849302{sell}") if sell in {"105", "108"} else int(f"826251{sell}")},
            {"action": "BUY", "right": "PUT", "strike": f"{buy}.00", "contract_key": f"SPY-20260430-P-{buy}", "ib_conId": int(f"826251{buy}")},
        ],
    }


def test_selected_legs_must_exist_in_latest_snapshot() -> None:
    selected = {
        "legs": [
            {"contract_key": "SPY-20260430-P-105", "ib_conId": 849302105},
            {"contract_key": "SPY-20260430-P-100", "ib_conId": 826251100},
        ]
    }

    assert _selected_legs_exist_in_snapshot(selected, _snapshot()) is True


def test_selection_rejects_absent_legs() -> None:
    selected = {"legs": [{"contract_key": "SPY-20260430-P-692", "ib_conId": 123}]}

    assert _selected_legs_exist_in_snapshot(selected, _snapshot()) is False


def test_near_itm_same_week_spread_requires_explicit_policy_approval() -> None:
    selected = {
        "expiry_utc": "2026-04-30T00:00:00Z",
        "legs": [{"action": "SELL", "right": "PUT", "strike": "110.00"}],
    }

    assert _near_itm_same_week_disallowed(selected, {"options_template": {"selection_policy": {}}}, _snapshot()) is True


def test_policy_can_explicitly_allow_near_itm_same_week_spread() -> None:
    selected = {
        "expiry_utc": "2026-04-30T00:00:00Z",
        "legs": [{"action": "SELL", "right": "PUT", "strike": "110.00"}],
    }
    policy = {"options_template": {"selection_policy": {"allow_near_itm_same_week": True}}}

    assert _near_itm_same_week_disallowed(selected, policy, _snapshot()) is False


def test_valid_otm_moderate_width_structure_passes_candidate_guard() -> None:
    blocker, _action = _selected_structure_guard_blocker(_selected(), _policy(), _snapshot())

    assert blocker == ""


def test_near_itm_structure_fails_candidate_guard() -> None:
    blocker, _action = _selected_structure_guard_blocker(_selected("108", "103"), _policy(), _snapshot())

    assert blocker == "NO_ELIGIBLE_OPTION_STRUCTURE"


def test_minimal_width_structure_fails_candidate_guard() -> None:
    blocker, _action = _selected_structure_guard_blocker(_selected("103", "100"), _policy(), _snapshot())

    assert blocker == "NO_ELIGIBLE_OPTION_STRUCTURE"


def test_illiquid_structure_fails_candidate_guard() -> None:
    snapshot = _snapshot()
    snapshot["contracts"][1]["ask"] = "1.30"

    blocker, _action = _selected_structure_guard_blocker(_selected(), _policy(), snapshot)

    assert blocker == "NO_ELIGIBLE_OPTION_STRUCTURE"


def test_defined_risk_policy_allows_max_loss_up_to_350_dollars() -> None:
    snapshot = {
        "as_of_utc": "2026-04-29T14:30:00Z",
        "underlying": {"spot_price": "110.00"},
        "contracts": [
            {"contract_key": "SPY-20260430-P-100", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "100.00", "right": "PUT", "bid": "0.45", "ask": "0.50", "ib": {"conId": 826251100}},
            {"contract_key": "SPY-20260430-P-105", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "105.00", "right": "PUT", "bid": "2.00", "ask": "2.05", "ib": {"conId": 849302105}},
        ],
    }

    selected, blocker, diagnostics = _select_vertical_put_credit_spread(
        intent={"engine": {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1"}, "underlying": {"symbol": "SPY"}, "option": {"structure": "PUT"}},
        policy=_selection_policy("350.00"),
        snapshot=snapshot,
        risk_budget=_risk_budget(),
        intent_id="intent-1",
    )

    assert blocker == ""
    assert selected["max_loss_cents"] == 35000
    assert selected["quantity_basis"]["policy_max_risk_cents"] == 35000
    assert diagnostics["candidates_eligible"] == 1


def test_defined_risk_policy_blocks_max_loss_above_350_dollars() -> None:
    snapshot = {
        "as_of_utc": "2026-04-29T14:30:00Z",
        "underlying": {"spot_price": "110.00"},
        "contracts": [
            {"contract_key": "SPY-20260430-P-100", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "100.00", "right": "PUT", "bid": "0.45", "ask": "0.51", "ib": {"conId": 826251100}},
            {"contract_key": "SPY-20260430-P-105", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "105.00", "right": "PUT", "bid": "2.00", "ask": "2.05", "ib": {"conId": 849302105}},
        ],
    }

    selected, blocker, diagnostics = _select_vertical_put_credit_spread(
        intent={"engine": {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1"}, "underlying": {"symbol": "SPY"}, "option": {"structure": "PUT"}},
        policy=_selection_policy("350.00"),
        snapshot=snapshot,
        risk_budget=_risk_budget(),
        intent_id="intent-1",
    )

    assert selected == {}
    assert blocker == "NO_ELIGIBLE_OPTION_STRUCTURE"
    assert diagnostics["rejected_by_reason"]["MAX_LOSS_EXCEEDS_RISK"] == 1
    assert diagnostics["nearest_miss_diagnostics"]["top_by_max_loss_excess"][0]["max_loss_cents"] == 35100
    assert diagnostics["nearest_miss_diagnostics"]["top_by_max_loss_excess"][0]["max_loss_excess_cents"] == 100


def test_width_otm_and_credit_rules_remain_unchanged_after_max_loss_increase() -> None:
    assert _selected_structure_guard_blocker(_selected("103", "100"), _policy(), _snapshot())[0] == "NO_ELIGIBLE_OPTION_STRUCTURE"
    assert _selected_structure_guard_blocker(_selected("108", "103"), _policy(), _snapshot())[0] == "NO_ELIGIBLE_OPTION_STRUCTURE"

    snapshot = {
        "as_of_utc": "2026-04-29T14:30:00Z",
        "underlying": {"spot_price": "110.00"},
        "contracts": [
            {"contract_key": "SPY-20260430-P-100", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "100.00", "right": "PUT", "bid": "2.00", "ask": "2.05", "ib": {"conId": 826251100}},
            {"contract_key": "SPY-20260430-P-105", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "105.00", "right": "PUT", "bid": "1.00", "ask": "1.05", "ib": {"conId": 849302105}},
        ],
    }
    selected, blocker, diagnostics = _select_vertical_put_credit_spread(
        intent={"engine": {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1"}, "underlying": {"symbol": "SPY"}, "option": {"structure": "PUT"}},
        policy=_selection_policy("350.00"),
        snapshot=snapshot,
        risk_budget=_risk_budget(),
        intent_id="intent-1",
    )

    assert selected == {}
    assert blocker == "NO_ELIGIBLE_OPTION_STRUCTURE"
    assert diagnostics["rejected_by_reason"]["NON_POSITIVE_CREDIT"] == 1


def test_structure_diagnostics_include_nearest_misses_when_no_candidate_is_eligible() -> None:
    snapshot = {
        "as_of_utc": "2026-04-29T14:30:00Z",
        "underlying": {"spot_price": "110.00"},
        "derived": {
            "derivation_policy": {
                "dte_window_policy": {
                    "policy_dte_min": 1,
                    "policy_dte_max": 7,
                    "max_expiries_to_capture": 7,
                    "expiries_available": [{"dte": 1, "expiry_yyyymmdd": "20260430"}],
                    "expiries_evaluated": [{"dte": 1, "expiry_yyyymmdd": "20260430"}],
                    "expiries_omitted": [{"dte": 2, "expiry_yyyymmdd": "20260501", "reason": "CAPTURE_TIME_BUDGET_EXHAUSTED"}],
                    "omission_reason": "CAPTURE_TIME_BUDGET_EXHAUSTED",
                }
            }
        },
        "contracts": [
            {"contract_key": "SPY-20260430-P-98", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "98.00", "right": "PUT", "bid": "0.04", "ask": "0.05", "ib": {"conId": 826251098}},
            {"contract_key": "SPY-20260430-P-100", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "100.00", "right": "PUT", "bid": "0.09", "ask": "0.10", "ib": {"conId": 826251100}},
            {"contract_key": "SPY-20260430-P-103", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "103.00", "right": "PUT", "bid": "0.40", "ask": "0.45", "ib": {"conId": 826251103}},
            {"contract_key": "SPY-20260430-P-105", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "105.00", "right": "PUT", "bid": "1.00", "ask": "1.05", "ib": {"conId": 849302105}},
            {"contract_key": "SPY-20260430-P-108", "expiry_utc": "2026-04-30T00:00:00Z", "strike": "108.00", "right": "PUT", "bid": "2.00", "ask": "2.05", "ib": {"conId": 849302108}},
        ],
    }
    policy = {
        "options_template": {
            "strategy": {"right": "PUT", "direction": "CREDIT"},
            "risk": {"max_risk_usd": "250.00", "max_contracts": 1, "multiplier": 100},
            "selection_policy": {
                "expiry_policy": {"mode": "DTE_WINDOW", "target_dte_min": 1, "target_dte_max": 7},
                "width_policy": {"width_points": "5.00"},
                "liquidity_policy": {"max_bid_ask_spread": "0.10"},
            },
        }
    }
    risk_budget = {
        "intent_budgets": [
            {"intent_id": "intent-1", "allowed_risk_cents": 100000},
        ]
    }

    selected, blocker, diagnostics = _select_vertical_put_credit_spread(
        intent={"engine": {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1"}, "underlying": {"symbol": "SPY"}, "option": {"structure": "PUT"}},
        policy=policy,
        snapshot=snapshot,
        risk_budget=risk_budget,
        intent_id="intent-1",
    )

    assert selected == {}
    assert blocker == "NO_ELIGIBLE_OPTION_STRUCTURE"
    assert diagnostics["rejected_by_reason"]["MAX_LOSS_EXCEEDS_RISK"] == 3
    assert diagnostics["nearest_miss_diagnostics"]["top_by_max_loss_excess"][0]["sell_strike"] == "108.00"
    assert diagnostics["nearest_miss_diagnostics"]["top_by_max_loss_excess"][0]["max_loss_excess_cents"] == 9500
    assert diagnostics["nearest_miss_diagnostics"]["top_clearly_otm_by_max_loss_excess"][0]["sell_strike"] == "105.00"
    assert diagnostics["dte_coverage"]["expiries_evaluated"] == [{"dte": 1, "expiry_yyyymmdd": "20260430"}]
    assert diagnostics["dte_coverage"]["expiries_omitted"] == [
        {"dte": 2, "expiry_yyyymmdd": "20260501", "reason": "CAPTURE_TIME_BUDGET_EXHAUSTED"}
    ]


def test_long_equity_intent_does_not_query_options_policy(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_equity_structure_inputs(ctx)
    equity_policy_path = tmp_path / "governance" / "C2_EQUITY_STRUCTURE_POLICY_V1.json"
    _write_json(equity_policy_path, _equity_policy())
    monkeypatch.setattr(structure_supply, "EQUITY_POLICY_PATH", equity_policy_path)
    monkeypatch.setattr(structure_supply, "POLICY_PATH", tmp_path / "missing-options-policy.json")

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "PASS"
    assert payload["structure_decisions"][0]["selected_structure"] == "EQUITY_SPOT"
    assert payload["structure_decisions"][0]["structure_diagnostics"]["options_policy_queried"] is False


def test_missing_equity_policy_fails_closed(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_equity_structure_inputs(ctx)
    monkeypatch.setattr(structure_supply, "EQUITY_POLICY_PATH", tmp_path / "missing-equity-policy.json")

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "EQUITY_STRUCTURE_POLICY_MISSING"
    assert payload["structure_export"]["usable_for_authorization_supply"] is False


def test_valid_equity_policy_emits_non_authoritative_equity_spot_structure(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_equity_structure_inputs(ctx)
    equity_policy_path = tmp_path / "governance" / "C2_EQUITY_STRUCTURE_POLICY_V1.json"
    _write_json(equity_policy_path, _equity_policy())
    monkeypatch.setattr(structure_supply, "EQUITY_POLICY_PATH", equity_policy_path)

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert payload["active_intents"][0]["requires_defined_risk"] is False
    assert payload["active_intents"][0]["exposure_type"] == "LONG_EQUITY"
    decision = payload["structure_decisions"][0]
    assert decision["structure_type"] == "EQUITY_SPOT"
    assert decision["symbol"] == "SPY"
    assert decision["exposure_type"] == "LONG_EQUITY"
    assert decision["target_notional_pct"] == "0.01"
    assert decision["order_intent_type"] == "EQUITY_BUY"
    assert decision["execution_authority_granted"] is False
    assert decision["order_submission_attempted"] is False
    assert decision["trading_behavior_changed"] is False
    assert payload["structure_export"]["usable_for_authorization_supply"] is False
    export = payload["structure_export"]["decisions"][0]
    assert export["selected_structure"] == "EQUITY_SPOT"
    assert export["execution_authority_granted"] is False
    assert export["order_submission_attempted"] is False
    assert export["trading_behavior_changed"] is False


def test_cross_asset_qqq_long_equity_policy_emits_non_authoritative_equity_spot(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_equity_structure_inputs(ctx, _cross_asset_qqq_intent())
    equity_policy_path = tmp_path / "governance" / "C2_EQUITY_STRUCTURE_POLICY_V1.json"
    _write_json(equity_policy_path, _equity_policy())
    monkeypatch.setattr(structure_supply, "EQUITY_POLICY_PATH", equity_policy_path)

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "PASS"
    assert payload["canonical_blocker"] == ""
    assert payload["active_intents"][0]["engine_id"] == "C2_CROSS_ASSET_TREND_V1"
    decision = payload["structure_decisions"][0]
    assert decision["structure_type"] == "EQUITY_SPOT"
    assert decision["symbol"] == "QQQ"
    assert decision["exposure_type"] == "LONG_EQUITY"
    assert decision["target_notional_pct"] == "0.10"
    assert decision["risk_bounds"]["max_risk_pct"] == "0.02"
    assert decision["execution_authority_granted"] is False
    assert decision["order_submission_attempted"] is False
    assert decision["trading_behavior_changed"] is False
    assert decision["structure_diagnostics"]["usable_for_authorization_supply"] is False
    assert payload["structure_export"]["usable_for_authorization_supply"] is False
    export = payload["structure_export"]["decisions"][0]
    assert export["execution_authority_granted"] is False
    assert export["order_submission_attempted"] is False
    assert export["trading_behavior_changed"] is False


def test_cross_asset_unsupported_symbol_fails_closed(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_equity_structure_inputs(ctx, _cross_asset_qqq_intent(symbol="SPY"))
    equity_policy_path = tmp_path / "governance" / "C2_EQUITY_STRUCTURE_POLICY_V1.json"
    _write_json(equity_policy_path, _equity_policy())
    monkeypatch.setattr(structure_supply, "EQUITY_POLICY_PATH", equity_policy_path)

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "EQUITY_STRUCTURE_POLICY_INVALID"
    assert payload["structure_export"]["usable_for_authorization_supply"] is False


def test_cross_asset_target_notional_above_policy_fails_closed(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_equity_structure_inputs(ctx, _cross_asset_qqq_intent(target_notional_pct="0.11"))
    equity_policy_path = tmp_path / "governance" / "C2_EQUITY_STRUCTURE_POLICY_V1.json"
    _write_json(equity_policy_path, _equity_policy())
    monkeypatch.setattr(structure_supply, "EQUITY_POLICY_PATH", equity_policy_path)

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "EQUITY_STRUCTURE_POLICY_INVALID"
    assert payload["structure_export"]["usable_for_authorization_supply"] is False


def test_cross_asset_max_risk_above_policy_fails_closed(monkeypatch, tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _write_equity_structure_inputs(ctx, _cross_asset_qqq_intent(max_risk_pct="0.020001"))
    equity_policy_path = tmp_path / "governance" / "C2_EQUITY_STRUCTURE_POLICY_V1.json"
    _write_json(equity_policy_path, _equity_policy())
    monkeypatch.setattr(structure_supply, "EQUITY_POLICY_PATH", equity_policy_path)

    payload = structure_supply.build_structure_decision_supply_v1(ctx)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "EQUITY_STRUCTURE_POLICY_INVALID"
    assert payload["structure_export"]["usable_for_authorization_supply"] is False
