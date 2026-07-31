# Aegis Research Program Grouping Review v1

Review date: `2026-06-01`
Evidence sources:

- `/home/node/constellation_runtime_data/truth/reports/aegis_research_capital_allocation_v1/2026-06-01/research_capital_allocation.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_research_portfolio_v1/2026-06-01/research_portfolio.v1.json`

This review recommends mapping edits only. It makes no automatic mapping, allocation, retirement, or candidate-generation changes.

## Program Reviews

| Program | Label | Distinct alpha source | Coherence | Duplicate risk | Recommended mapping action |
|---|---:|---|---|---|---|
| `PROGRAM_TREND_PERSISTENCE_V1` | `WELL_DEFINED` with `NEEDS_SPLIT` watch | Yes: time-series/cross-sectional trend persistence | Two linked hypotheses are coherent under trend persistence. | Moderate: cross-asset trend and large-cap equity momentum can overlap in equity index exposure. | Keep grouped for now; add subprogram tags `EQUITY_MOMENTUM` and `CROSS_ASSET_TREND` if evidence grows. |
| `PROGRAM_DEFENSIVE_CONVEXITY_V1` | `NEEDS_RENAME` | Potentially yes: convex hedge timing and payoff asymmetry | Thesis and sleeve are coherent but too portfolio-protection oriented for alpha comparison. | Low | Rename candidate: `Defensive Tail Hedge Timing`; define whether success is standalone return or portfolio drawdown offset. |
| `PROGRAM_EQUITY_MEAN_REVERSION_V1` | `WELL_DEFINED` | Yes: short-horizon overextension reversal | Coherent one-thesis/one-hypothesis/one-sleeve program. | Low | Keep mapping unchanged; improve parameter metadata. |
| `PROGRAM_EVENT_DISLOCATION_V1` | `TOO_BROAD` / `NEEDS_SPLIT` | Yes, but currently mixes multiple event behaviors | Hypothesis combines follow-through and reversal/repricing. | Medium: can overlap trend or mean reversion depending branch. | Proposed split: `EVENT_FOLLOW_THROUGH` and `EVENT_REVERSAL_REPRICING`, with deterministic event/regime branch. |
| `PROGRAM_RELATIVE_VALUE_SPREAD_V1` | `NEEDS_REFINEMENT` | Yes: spread convergence | Coherent but missing pair/fair-value specificity. | Low to medium if pairs include broad risk-on/risk-off proxies already used in cross-asset trend. | Keep mapping unchanged; define pair universe and fair-value model before adding more sleeves. |
| `PROGRAM_SIMULATION_CONTROL_V1` | `TOO_NARROW` / `NEEDS_RENAME` | No investment alpha source; workflow-control source only | Coherent as control infrastructure, incoherent as alpha research program. | Not duplicative; categorically different. | Mark as `CONTROL_PROGRAM`, exclude from alpha grouping comparisons and research-capital alpha recommendations. |
| `PROGRAM_VOLATILITY_RISK_PREMIUM_V1` | `WELL_DEFINED` | Yes: defined-risk volatility premium | Coherent one-thesis/one-hypothesis/one-sleeve program. | Low | Keep mapping unchanged; require option-structure and volatility-regime metadata. |

## Proposed Mapping Changes

No deterministic mapping changes are applied.

Recommended future changes requiring explicit approval:

1. Split `PROGRAM_EVENT_DISLOCATION_V1` into event follow-through and event reversal/repricing branches if source artifacts can classify event type and expected direction.
2. Add a non-alpha category to `PROGRAM_SIMULATION_CONTROL_V1` so it remains operationally useful but stops competing with investment hypotheses.
3. Add subprogram metadata inside `PROGRAM_TREND_PERSISTENCE_V1` if equity and cross-asset results begin diverging materially.

## Current Grouping Risk

The main grouping risk is not too many programs; it is that three programs are under-specified enough to make future validation attribution ambiguous: event dislocation, defensive convexity, and relative-value spread convergence.
