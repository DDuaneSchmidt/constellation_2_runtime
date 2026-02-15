from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any

from constellation_2.phaseK_struct.lib.k_struct_common_v1 import (
    BasicStats,
    RET_Q,
    compound_nav_path,
    max_drawdown,
    cagr_from_nav_path,
    annualized_vol,
    sharpe_annualized,
    empirical_quantile,
    mean,
    std_sample,
)


def run_cluster_shock(daily_returns: List[Decimal]) -> Dict[str, Any]:
    """
    Structural shock: force 30 consecutive synthetic underperformance days.
    Definition:
      If vol (sample std) exists:
        shock_return = -2 * std(daily_returns)
      Else:
        shock_return = -0.01  (1% daily)  [fail-closed flag]
    We append 30 shock days to the end of the series and recompute stats.
    """
    flags: List[str] = []
    if len(daily_returns) >= 2:
        vol_d = std_sample(daily_returns)
        shock = (-Decimal("2") * vol_d).quantize(RET_Q, rounding=ROUND_HALF_UP)
    else:
        shock = Decimal("-0.01000000")
        flags.append("INSUFFICIENT_HISTORY_FOR_VOL_USED_FALLBACK_SHOCK")

    shocked = list(daily_returns) + [shock] * 30
    nav = compound_nav_path(shocked)
    dd, dd_dur = max_drawdown(nav)
    stats = BasicStats(
        n=len(shocked),
        cagr=cagr_from_nav_path(nav),
        vol_ann=annualized_vol(shocked),
        sharpe=sharpe_annualized(shocked),
        max_dd=dd,
        max_dd_duration_days=dd_dur,
        tail_95=empirical_quantile(shocked, Decimal("0.05")),
        tail_99=empirical_quantile(shocked, Decimal("0.01")),
    )
    return {
        "test_id": "K_STRUCT_CORRELATION_CLUSTER_SHOCK_V1",
        "definition": {
            "shock_days": 30,
            "shock_return": str(shock),
            "rule": "shock_return = -2*std(daily_returns); if std unavailable, shock_return=-0.01 (flagged)",
        },
        "flags": flags,
        "results": {
            "n": stats.n,
            "cagr": None if stats.cagr is None else f"{stats.cagr:.8f}",
            "vol_ann": None if stats.vol_ann is None else f"{stats.vol_ann:.8f}",
            "sharpe": None if stats.sharpe is None else f"{stats.sharpe:.8f}",
            "max_dd": None if stats.max_dd is None else f"{stats.max_dd:.6f}",
            "max_dd_duration_days": stats.max_dd_duration_days,
            "tail_95": None if stats.tail_95 is None else f"{stats.tail_95:.8f}",
            "tail_99": None if stats.tail_99 is None else f"{stats.tail_99:.8f}",
        },
    }
