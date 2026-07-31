# Aegis Thesis / Hypothesis Quality Review v1

Review date: `2026-06-01`
Evidence sources:

- `/home/node/constellation_runtime_data/truth/reports/aegis_research_portfolio_v1/2026-06-01/research_portfolio.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_statistical_sufficiency_v1/2026-06-01/statistical_sufficiency.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_research_capital_allocation_v1/2026-06-01/research_capital_allocation.v1.json`

This review recommends edits only. It does not delete, retire, promote, or change any candidate-generation behavior.

## Summary

- Theses reviewed: `7`
- Hypotheses reviewed: `8`
- Validation status: all `8` hypotheses are `UNDERPOWERED` with `0` usable closed samples.
- Main quality gap: most legacy-inferred hypotheses have good names but need source-declared formal claims, market universe, signal definitions, holding windows, and invalidation criteria before they should be treated as fully specified research truth.

## Quality Label Definitions

- `STRONG`: clear, testable, falsifiable, explicit enough for validation.
- `ACCEPTABLE`: usable, but needs modest tightening.
- `NEEDS_REFINEMENT`: directionally useful but missing key validation fields.
- `TOO_BROAD`: claim spans too many mechanisms or markets for one hypothesis.
- `DUPLICATIVE`: overlaps another hypothesis enough to impair attribution.
- `NOT_TESTABLE`: cannot produce objective validation samples as written.

## Thesis Reviews

| Thesis | Label | Assessment | Recommended edit |
|---|---:|---|---|
| `THESIS_TREND_PERSISTENCE_V1` | `ACCEPTABLE` | Clear broad thesis with two coherent implementations: equity momentum and cross-asset trend. It is broad by design, but the child hypotheses are separable. | Keep as program-level thesis; explicitly define trend horizons and cost assumptions in child hypotheses. |
| `THESIS_DEFENSIVE_CONVEXITY_V1` | `NEEDS_REFINEMENT` | Clear portfolio purpose, but the edge source is under-specified: protection value, convex payoff, and timing trigger are mixed. | Split claim into trigger quality, hedge payoff asymmetry, and portfolio drawdown-offset metrics. |
| `THESIS_EQUITY_MEAN_REVERSION_V1` | `ACCEPTABLE` | Clear short-horizon reversion thesis with explicit liquidity/regime filter language. | Add observation window and threshold definitions for overextension and normalization. |
| `THESIS_EVENT_DISLOCATION_V1` | `NEEDS_REFINEMENT` | Plausible but broad: follow-through and reversal are opposite behaviors and should not be one undifferentiated thesis unless regime-conditioned. | Define separate follow-through versus reversal hypotheses or add deterministic branch conditions. |
| `THESIS_RELATIVE_VALUE_SPREAD_V1` | `NEEDS_REFINEMENT` | Clear relative-value idea, but the spread universe and fair-value model are not explicit. | Declare pair universe, spread calculation, z-score/window, and convergence horizon. |
| `THESIS_SIMULATION_CONTROL_V1` | `NOT_TESTABLE` | Useful as workflow control, but not an investment thesis and should not compete with alpha hypotheses. | Keep as system-control thesis with zero alpha validation expectation; exclude from alpha maturity scoring. |
| `THESIS_VOLATILITY_RISK_PREMIUM_V1` | `ACCEPTABLE` | Clear source of possible edge and regime dependency. | Add exact implied/realized volatility proxy, option structure constraints, and drawdown/invalidation thresholds. |

## Hypothesis Reviews

| Hypothesis | Thesis | Label | Testable | Falsifiable | Universe explicit | Signal explicit | Window explicit | Invalidation defined | Duplicative | Suitable for validation | Recommended edit |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1` | `THESIS_TREND_PERSISTENCE_V1` | `STRONG` | Yes | Yes | Partial | Partial | Yes | Partial | No | Yes | Source-declare large-cap universe, momentum calculation, rebalance cadence, costs, and minimum forward holding period. |
| `HYP_CROSS_ASSET_TREND_PERSISTENCE_V1` | `THESIS_TREND_PERSISTENCE_V1` | `ACCEPTABLE` | Yes | Yes | Partial | Partial | Partial | Partial | Partly | Yes | Define asset universe and trend lookback; specify how it differs from equity momentum beyond asset breadth. |
| `HYP_DEFENSIVE_TAIL_CONVEXITY_V1` | `THESIS_DEFENSIVE_CONVEXITY_V1` | `NEEDS_REFINEMENT` | Partial | Partial | Partial | No | No | No | No | Not yet | Define trigger signal, hedge instrument, payoff metric, expected behavior during risk-off regimes, and cost drag limit. |
| `HYP_DEFINED_RISK_VOL_PREMIUM_V1` | `THESIS_VOLATILITY_RISK_PREMIUM_V1` | `ACCEPTABLE` | Yes | Yes | Partial | Partial | Partial | Partial | No | Yes after option-specific fields | Add explicit option structure, expiry window, IV/RV proxy, profit target/loss limit, and regime exclusions. |
| `HYP_EQUITY_SHORT_HORIZON_MEAN_REVERSION_V1` | `THESIS_EQUITY_MEAN_REVERSION_V1` | `ACCEPTABLE` | Yes | Yes | Partial | Partial | Partial | Partial | No | Yes | Declare z-score or overextension threshold, liquidity filter, holding period, and trend-filter invalidation rule. |
| `HYP_EVENT_DISLOCATION_REPRICING_V1` | `THESIS_EVENT_DISLOCATION_V1` | `TOO_BROAD` | Partial | Partial | Partial | Partial | No | No | No | Needs split | Split into `event_follow_through` and `event_reversal` or add deterministic branch rules by event type/regime. |
| `HYP_MARKET_NEUTRAL_SPREAD_CONVERGENCE_V1` | `THESIS_RELATIVE_VALUE_SPREAD_V1` | `NEEDS_REFINEMENT` | Partial | Yes | Partial | Partial | Partial | Partial | No | Not yet | Source-declare pairs, hedge ratio, spread metric, entry z-score, exit z-score, max holding period, and broken-spread criteria. |
| `HYP_INTENT_SIMULATOR_CONTROL_V1` | `THESIS_SIMULATION_CONTROL_V1` | `NOT_TESTABLE` | No for alpha | Yes for workflow | Yes | Yes for control | N/A | Yes for control | No | Workflow-only | Mark as control hypothesis; exclude from alpha validation and research-capital comparison except as plumbing health evidence. |

## Cross-Hypothesis Findings

1. `HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1` is the highest-quality current investment hypothesis because it includes a horizon in the name and has paper evidence.
2. `HYP_CROSS_ASSET_TREND_PERSISTENCE_V1` is coherent but partly duplicative with equity momentum; the distinction should be asset-universe breadth and cross-asset confirmation.
3. `HYP_EVENT_DISLOCATION_REPRICING_V1` is the broadest investment hypothesis and should be split or made regime-conditional before validation claims are interpreted.
4. `HYP_INTENT_SIMULATOR_CONTROL_V1` is valuable operationally but should not be scored as an alpha hypothesis.
5. All hypotheses remain underpowered because no paper positions have matured into closed, statistically usable validation outcomes.
