from __future__ import annotations

V1_CONSTRUCTION_POLICY = {
    "portfolio_mode": "research_backtest_only",
    "factor_weights": "fixed_non_optimized",
    "rebalance_policy": "explicit_calendar_policy_required_before_backtest",
    "real_capital_position_sizing": False,
    "recommendations": False,
}


def construction_policy() -> dict[str, object]:
    return dict(V1_CONSTRUCTION_POLICY)

