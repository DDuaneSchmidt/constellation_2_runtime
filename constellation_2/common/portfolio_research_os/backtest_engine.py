from __future__ import annotations

VALIDATION_RULES = [
    "No survivorship bias: use point-in-time eligible universe or document missing point-in-time data.",
    "No lookahead bias: features, holdings, and prices must be available at decision time.",
    "No future index membership leakage: index constituents must be timestamped or avoided.",
    "No optimized factor weights in V1: fixed transparent weights only.",
    "No cherry-picked benchmark: compare against the full locked benchmark stack.",
    "No single-metric victory: evaluate return, drawdown, volatility, turnover, and consistency.",
    "Walk-forward validation: use rolling or expanding windows with a gap/embargo where needed.",
]


def validation_rules() -> list[str]:
    return list(VALIDATION_RULES)


def backtest_readiness() -> dict[str, str]:
    return {
        "classification": "DATA_REQUIRED",
        "historical_backtest": "VALIDATION_NOT_STARTED",
        "walk_forward_validation": "VALIDATION_NOT_STARTED",
        "notes": "Backtest requires point-in-time universe, prices, corporate actions, benchmark series, and explicit rebalance calendar.",
    }

