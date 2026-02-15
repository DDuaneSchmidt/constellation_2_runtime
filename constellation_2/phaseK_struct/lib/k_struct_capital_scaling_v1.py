from __future__ import annotations

from decimal import Decimal
from typing import List, Dict, Any

from constellation_2.phaseK_struct.lib.k_struct_common_v1 import (
    BasicStats,
    compound_nav_path,
    max_drawdown,
    cagr_from_nav_path,
    annualized_vol,
    sharpe_annualized,
    empirical_quantile,
    RET_Q,
)


def _scale_returns_linear(daily: List[Decimal], k: Decimal) -> List[Decimal]:
    # Structural proxy: scaling capital should not change return series absent liquidity impact.
    # We keep returns unchanged and only report invariants.
    # If you later model liquidity impact, that belongs in a separate harness.
    return list(daily)


def run_capital_scaling_suite(daily_returns: List[Decimal]) -> Dict[str, Any]:
    scales = [Decimal("0.5"), Decimal("2.0"), Decimal("5.0")]
    rows: List[Dict[str, Any]] = []
    base = list(daily_returns)

    for s in scales:
        adj = _scale_returns_linear(base, s)
        nav = compound_nav_path(adj)
        dd, dd_dur = max_drawdown(nav)
        stats = BasicStats(
            n=len(adj),
            cagr=cagr_from_nav_path(nav),
            vol_ann=annualized_vol(adj),
            sharpe=sharpe_annualized(adj),
            max_dd=dd,
            max_dd_duration_days=dd_dur,
            tail_95=empirical_quantile(adj, Decimal("0.05")),
            tail_99=empirical_quantile(adj, Decimal("0.01")),
        )
        rows.append(
            {
                "scale": str(s),
                "invariance_model": "returns unchanged (structural check); liquidity impact not modeled here",
                "n": stats.n,
                "cagr": None if stats.cagr is None else f"{stats.cagr:.8f}",
                "vol_ann": None if stats.vol_ann is None else f"{stats.vol_ann:.8f}",
                "sharpe": None if stats.sharpe is None else f"{stats.sharpe:.8f}",
                "max_dd": None if stats.max_dd is None else f"{stats.max_dd:.6f}",
                "max_dd_duration_days": stats.max_dd_duration_days,
            }
        )

    return {
        "test_id": "K_STRUCT_CAPITAL_SCALING_INVARIANCE_V1",
        "definition": {
            "note": "This suite asserts structural invariance absent liquidity modeling; capacity limits require execution+liquidity truth not present yet.",
        },
        "results": rows,
    }
