from __future__ import annotations

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
