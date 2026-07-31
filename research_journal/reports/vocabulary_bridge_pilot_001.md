# Vocabulary Bridge Pilot 001

Date: 2026-06-05

Status: ANALYSIS_ONLY

## Scope

Evaluate the 10 `VOCABULARY_BRIDGE_DIAGNOSTIC` candidates using an evaluation-only vocabulary bridge overlay.

This pilot measures:

- samples retained
- validation delta
- candidate delta
- how many of the 10 become evaluable
- how many become confirmed
- how many remain blocked

This report does not implement a bridge, change replay behavior, relax production filters, change validation, change qualification, promote candidates, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.

## Inputs Reviewed

- `research_journal/reports/fixable_validation_limitations_table.csv`
- `research_journal/reports/fixable_validation_limitations_plan_001.md`
- `research_journal/reports/vocabulary_bridge_simulation_001.md`
- `research_journal/reports/vocabulary_bridge_simulation_table.csv`
- `research_journal/reports/vocabulary_bridge_effectiveness_001.md`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/validation_vocabulary_bridge/bridge_result.json`

## Bridge Overlay Applied

Allowed evaluation-only overlay:

| Source regime | Validator regime | Mapping type | Use |
| --- | --- | --- | --- |
| `CHOP` | `RANGE_BOUND` | `PARTIAL_MATCH` | diagnostic-only sample-retention test |
| `TREND` | `TRENDING` | `EXACT_MATCH` | alias normalization |
| exact validator labels | same label | `EXACT_MATCH` | no semantic expansion |
| `UNKNOWN` | `UNKNOWN` | `EXACT_MATCH` | no expansion; unresolved regime remains unresolved |

Important boundary:

`UNKNOWN` is not bridged to `CHOP`, `RANGE_BOUND`, `LOW_VOLATILITY`, `TRENDING`, or any permissive substitute. It is an unresolved or unspecified regime, not a market-state approximation.

## Candidate Set

The 10 `VOCABULARY_BRIDGE_DIAGNOSTIC` candidates all materialize as `REJECT_FOR_NOW` preview rows with `UNKNOWN` regime.

| Candidate ID | Mechanism | Regime | Timeframe | Symbols | Baseline final score | Baseline sample size | Bridge action |
| --- | --- | --- | --- | --- | ---: | ---: | --- |
| `ptc_backtest_final_009acc9a8738e8f0` | MEAN_REVERSION | UNKNOWN | 30m | DIA, QQQ | 0.682689 | 1540 | exact `UNKNOWN -> UNKNOWN`; no expansion |
| `ptc_backtest_final_0201292a9823e314` | MEAN_REVERSION | UNKNOWN | 15m | AMD, BAC, GOOGL, MSFT, NFLX, TSLA | 0.677634 | 1540 | exact `UNKNOWN -> UNKNOWN`; no expansion |
| `ptc_backtest_final_040dea316c6d1917` | VWAP_OR_AVERAGE_RECLAIM | UNKNOWN | 30m | AAPL, AMD, GOOGL, JPM, NFLX, NVDA | 0.658838 | 374 | exact `UNKNOWN -> UNKNOWN`; no expansion |
| `ptc_backtest_final_045abee318c2560a` | LIQUIDITY_SWEEP | UNKNOWN | 30m | AAPL, AMD, AMZN, JPM, META, NVDA | 0.674959 | 285 | exact `UNKNOWN -> UNKNOWN`; no expansion |
| `ptc_backtest_final_04a687b081d95eae` | VWAP_OR_AVERAGE_RECLAIM | UNKNOWN | 30m | IWM, QQQ | 0.663712 | 374 | exact `UNKNOWN -> UNKNOWN`; no expansion |
| `ptc_backtest_final_0ee5a48c813c158d` | BREAKOUT | UNKNOWN | 30m | DIA, QQQ, SPY | 0.672996 | 811 | exact `UNKNOWN -> UNKNOWN`; no expansion |
| `ptc_backtest_final_111ba2c238976aea` | MEAN_REVERSION | UNKNOWN | 1h | DBC, TLT, USO | 0.680519 | 1540 | exact `UNKNOWN -> UNKNOWN`; no expansion |
| `ptc_backtest_final_132bed719ce20d48` | LIQUIDITY_SWEEP | UNKNOWN | 1h | IWM, SPY | 0.676962 | 285 | exact `UNKNOWN -> UNKNOWN`; no expansion |
| `ptc_backtest_final_17d8e91072182cdf` | REVERSAL | UNKNOWN | 1h | AAPL, AMZN, BAC, JPM, META, MSFT, TSLA | 0.673655 | 707 | exact `UNKNOWN -> UNKNOWN`; no expansion |
| `ptc_backtest_final_1996c1c3f3a598e8` | TREND_CONTINUATION | UNKNOWN | 15m | TLT, USO, XLE | 0.675060 | 3120 | exact `UNKNOWN -> UNKNOWN`; no expansion |

## Pilot Measurement

### Samples Retained

| Measure | Count |
| --- | ---: |
| baseline materialized sample count across 10 | 11656 |
| additional samples retained by bridge overlay | 0 |
| candidates with additional retained samples | 0 |

Reason:

The previous measured bridge benefit came from `CHOP -> RANGE_BOUND`. These 10 candidates are `UNKNOWN` regime rows. The evaluation-only bridge does not translate `UNKNOWN` into any market regime, so it cannot recover additional samples.

### Validation Delta

| Measure | Count |
| --- | ---: |
| baseline `REJECT_FOR_NOW` candidates | 10 |
| candidates moving to evaluable because of bridge | 0 |
| candidates moving to confirmed because of bridge | 0 |
| validation classifications changed | 0 |

Reason:

The bridge overlay did not alter sample retention and did not provide a new validator-compatible regime. The candidates remain failed due to low final score and unresolved validation/evidence limitations, not because a known `CHOP` label failed to match a known `RANGE_BOUND` validator label.

### Candidate Delta

| Measure | Count |
| --- | ---: |
| candidate state changes | 0 |
| qualification changes | 0 |
| replay changes | 0 |
| candidates reclassified by this report | 0 |

This report records only diagnostic interpretation. It does not change any candidate or validation state.

## Outcome Counts

How many of the 10 become:

| Outcome | Count |
| --- | ---: |
| evaluable due to bridge overlay | 0 |
| confirmed due to bridge overlay | 0 |
| still blocked / unchanged | 10 |

## Interpretation

The 10-candidate `VOCABULARY_BRIDGE_DIAGNOSTIC` pilot falsifies broad bridge expansion for `UNKNOWN`-regime candidates.

The bridge is useful for a specific known mismatch:

- candidate-side `CHOP`
- validator-side `RANGE_BOUND`
- diagnostic-only `PARTIAL_MATCH`

It is not useful for `UNKNOWN` rows because `UNKNOWN` is not a semantic neighbor. It means the candidate regime is unresolved, unspecified, or insufficiently attributed. Bridging `UNKNOWN` into a concrete market state would create false-positive risk and should remain disallowed.

## Comparison To Prior Bridge Simulation

Prior measured CHOP bridge simulation:

- affected candidates: 5
- additional samples retained: 396
- candidates evaluable: 3
- candidates confirmed: 2
- candidates still blocked: 3

Current 10-candidate `UNKNOWN` pilot:

- affected candidates: 10
- additional samples retained: 0
- candidates evaluable because of bridge: 0
- candidates confirmed: 0
- candidates still blocked: 10

Conclusion:

The bridge should stay narrow. `CHOP -> RANGE_BOUND` remains a useful diagnostic overlay. `UNKNOWN` should not be bridged.

## Recommended Next Evidence Step

For these 10 candidates, the right next step is not a vocabulary bridge. It is source-regime clarification:

1. recover or infer the candidate-side regime from source observations where possible
2. distinguish truly unknown regime from missing lineage
3. run side-by-side exact-regime and diagnostic-bridge counts only after a concrete source regime exists
4. keep every result labeled as analysis-only unless separately validated

## Authority Boundary

This pilot is evaluation-only. It does not implement a bridge, change replay behavior, relax filters, change validation, change qualification, promote candidates, change governance, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, or alter runtime truth.
