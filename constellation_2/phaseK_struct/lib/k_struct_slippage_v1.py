from __future__ import annotations

from dataclasses import dataclass
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
)


@dataclass(frozen=True)
class SlippageCase:
    name: str
    multiplier: Decimal


def _apply_slippage_overlay(daily: List[Decimal], mult: Decimal) -> List[Decimal]:
    """
    Deterministic structural slippage overlay.
    We do NOT claim realism; we test fragility.

    Definition (audit-grade):
      adjusted_return = r - abs(r) * (mult - 1)
    so mult=1 => unchanged, mult=2 => subtract abs(r), mult=3 => subtract 2*abs(r).
    """
    if mult < 1:
        raise ValueError("SLIPPAGE_MULT_LT_1_FORBIDDEN")
    k = (mult - Decimal("1"))
    out: List[Decimal] = []
    for r in daily:
        adj = (r - (abs(r) * k)).quantize(RET_Q, rounding=ROUND_HALF_UP)
        out.append(adj)
    return out


def run_slippage_suite(daily_returns: List[Decimal]) -> Dict[str, Any]:
    cases = [
        SlippageCase("slippage_x1", Decimal("1.0")),
        SlippageCase("slippage_x2", Decimal("2.0")),
        SlippageCase("slippage_x3", Decimal("3.0")),
    ]

    rows: List[Dict[str, Any]] = []
    for c in cases:
        adj = _apply_slippage_overlay(daily_returns, c.multiplier)
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
                "case": c.name,
                "multiplier": str(c.multiplier),
                "n": stats.n,
                "cagr": None if stats.cagr is None else f"{stats.cagr:.8f}",
                "vol_ann": None if stats.vol_ann is None else f"{stats.vol_ann:.8f}",
                "sharpe": None if stats.sharpe is None else f"{stats.sharpe:.8f}",
                "max_dd": None if stats.max_dd is None else f"{stats.max_dd:.6f}",
                "max_dd_duration_days": stats.max_dd_duration_days,
                "tail_95": None if stats.tail_95 is None else f"{stats.tail_95:.8f}",
                "tail_99": None if stats.tail_99 is None else f"{stats.tail_99:.8f}",
            }
        )

    return {
        "test_id": "K_STRUCT_SLIPPAGE_STRESS_V1",
        "definition": {
            "adjusted_return": "r - abs(r) * (multiplier - 1)",
            "cases": [{"name": r["case"], "multiplier": r["multiplier"]} for r in rows],
        },
        "results": rows,
    }
