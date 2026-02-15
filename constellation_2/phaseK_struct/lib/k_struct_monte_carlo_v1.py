from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any

from constellation_2.phaseK_struct.lib.k_struct_common_v1 import (
    RET_Q,
    DD_Q,
    compound_nav_path,
    max_drawdown,
    cagr_from_nav_path,
    seeded_rng,
    empirical_quantile,
)


def _bootstrap_path(daily_empirical: List[Decimal], steps: int, rng_seed: str) -> List[Decimal]:
    rng = seeded_rng(rng_seed)
    if not daily_empirical:
        return [Decimal("0")] * steps
    out: List[Decimal] = []
    n = len(daily_empirical)
    for _ in range(steps):
        idx = rng.randrange(0, n)
        out.append(daily_empirical[idx])
    return out


def run_monte_carlo_structural(
    daily_returns: List[Decimal],
    seed_material: str,
    paths: int = 10_000,
    years: int = 5,
) -> Dict[str, Any]:
    """
    Deterministic bootstrap Monte Carlo on empirical daily returns.
    NOTE: with small samples, this tests only structural response to the observed distribution.
    """
    steps = int(252 * years)

    ruin_count = 0
    dd20_count = 0
    cagr_lt_5_count = 0
    cagr_gt_12_count = 0

    worst_nav_end: Decimal | None = None
    worst_dd: Decimal | None = None

    for i in range(paths):
        seq = _bootstrap_path(daily_returns, steps, f"{seed_material}|mc|{years}y|{i}")
        nav = compound_nav_path(seq)
        dd, _dur = max_drawdown(nav)
        cagr = cagr_from_nav_path(nav)

        end = nav[-1]
        if end <= 0:
            ruin_count += 1
        if dd is not None and dd <= Decimal("-0.20"):
            dd20_count += 1
        if cagr is not None and cagr < Decimal("0.05"):
            cagr_lt_5_count += 1
        if cagr is not None and cagr > Decimal("0.12"):
            cagr_gt_12_count += 1

        if worst_nav_end is None or end < worst_nav_end:
            worst_nav_end = end
        if dd is not None:
            if worst_dd is None or dd < worst_dd:
                worst_dd = dd

    def _p(k: int) -> str:
        return f"{(Decimal(k) / Decimal(paths)).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)}"

    return {
        "test_id": "K_STRUCT_MONTE_CARLO_BOOTSTRAP_V1",
        "definition": {
            "paths": paths,
            "years": years,
            "steps": steps,
            "method": "bootstrap sample with replacement from empirical daily returns; seeded; deterministic",
        },
        "results": {
            "p_ruin_nav_le_0": _p(ruin_count),
            "p_max_dd_gt_20pct": _p(dd20_count),
            "p_cagr_lt_5pct": _p(cagr_lt_5_count),
            "p_cagr_gt_12pct": _p(cagr_gt_12_count),
            "worst_nav_end": None if worst_nav_end is None else str(worst_nav_end.quantize(RET_Q, rounding=ROUND_HALF_UP)),
            "worst_max_dd": None if worst_dd is None else str(worst_dd.quantize(DD_Q, rounding=ROUND_HALF_UP)),
        },
    }
