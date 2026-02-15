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
    seeded_rng,
)


@dataclass(frozen=True)
class PerturbCase:
    name: str
    return_scale: Decimal      # multiply returns
    vol_scale: Decimal         # multiply deviation from mean
    noise_std: Decimal         # additive noise std (proxy) as absolute return units


def _apply_perturb(daily: List[Decimal], seed: str, rc: PerturbCase) -> List[Decimal]:
    """
    Structural proxy for parameter robustness without modifying engines.
    Definition:
      r0 = r * return_scale
      m = mean(r0)
      r1 = m + (r0 - m) * vol_scale
      r2 = r1 + noise, where noise ~ Uniform(-a, a), a = noise_std * sqrt(3)
    Deterministic via seeded RNG.
    """
    if not daily:
        return []
    # Mean computed deterministically
    m = sum(daily) / Decimal(len(daily))
    rng = seeded_rng(seed + "|" + rc.name)

    # Convert noise_std to uniform half-width a using Var(U[-a,a])=a^2/3
    a = (rc.noise_std * Decimal("3").sqrt())

    out: List[Decimal] = []
    for r in daily:
        r0 = (r * rc.return_scale)
        r1 = (m + (r0 - m) * rc.vol_scale)
        # deterministic uniform noise
        u = Decimal(str(rng.uniform(-1.0, 1.0)))
        noise = (u * a)
        r2 = (r1 + noise).quantize(RET_Q, rounding=ROUND_HALF_UP)
        out.append(r2)
    return out


def run_perturbation_suite(daily_returns: List[Decimal], seed_material: str) -> Dict[str, Any]:
    cases = [
        PerturbCase("base_identity", Decimal("1.0"), Decimal("1.0"), Decimal("0.0")),
        PerturbCase("lookback_wider_proxy", Decimal("0.90"), Decimal("1.10"), Decimal("0.0")),
        PerturbCase("lookback_tighter_proxy", Decimal("1.10"), Decimal("0.90"), Decimal("0.0")),
        PerturbCase("threshold_stricter_proxy", Decimal("0.90"), Decimal("1.00"), Decimal("0.0")),
        PerturbCase("threshold_looser_proxy", Decimal("1.10"), Decimal("1.00"), Decimal("0.0")),
        PerturbCase("noise_plus_10bp", Decimal("1.0"), Decimal("1.0"), Decimal("0.0010")),  # 10bp = 0.10%
    ]

    rows: List[Dict[str, Any]] = []
    for c in cases:
        adj = _apply_perturb(daily_returns, seed_material, c)
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
                "return_scale": str(c.return_scale),
                "vol_scale": str(c.vol_scale),
                "noise_std": str(c.noise_std),
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
        "test_id": "K_STRUCT_PERTURBATION_PROXY_V1",
        "definition": {
            "intent": "structural sensitivity proxy; does not modify engines; deterministic transform of return series",
            "transform": [
                "r0 = r * return_scale",
                "m = mean(r0)",
                "r1 = m + (r0 - m) * vol_scale",
                "r2 = r1 + noise; noise ~ Uniform(-a,a) where a = noise_std*sqrt(3)",
            ],
        },
        "results": rows,
    }
