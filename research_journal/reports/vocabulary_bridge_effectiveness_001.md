# Vocabulary Bridge Effectiveness 001

Date: 2026-06-05
Status: ANALYSIS_ONLY

## Scope

This report measures the observed effect of the validation vocabulary bridge overlay using existing artifacts only. It does not implement a bridge, rerun production validation, relax filters, change candidate state, change qualification, or create validation authority.

Inputs reviewed:

- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `research_journal/reports/vocabulary_bridge_simulation_001.md`
- `research_journal/reports/vocabulary_bridge_simulation_table.csv`
- `research_journal/design/validation_vocabulary_bridge_v0_1.md`
- `research_journal/reports/regime_filter_root_cause_table.csv`

Bridge overlay evaluated:

- Primary: `E_HIGHEST_CONFIDENCE_CROSSWALK`, effectively `CHOP -> RANGE_BOUND` for the affected candidates.
- Reference alternatives: `CHOP -> LOW_VOLATILITY`, `CHOP -> RANGE_BOUND OR LOW_VOLATILITY`, and `TREND -> TRENDING`.

## Baseline

Current direct validation reviewed 8 candidates:

| metric | value |
| --- | ---: |
| candidates_reviewed | 8 |
| direct_replays_run | 8 |
| baseline_confirmed | 2 |
| baseline_insufficient_data | 6 |
| affected_CHOP_candidates | 5 |
| affected_CHOP_confirmed | 0 |
| affected_CHOP_blocked | 5 |
| affected_CHOP_block_rate | 100% |

For the five affected `CHOP` candidates, baseline surviving samples after exact regime filtering were all zero.

## Bridge-Enabled Evaluation

Using the highest-confidence bridge overlay (`CHOP -> RANGE_BOUND`):

| metric | value |
| --- | ---: |
| additional_samples_retained | 396 |
| candidates_with_any_sample_improvement | 5 |
| additional_candidates_evaluable | 3 |
| additional_candidates_confirmed | 2 |
| remaining_blocked_candidates | 3 |
| affected_CHOP_block_rate_after_bridge | 60% |
| validation_block_rate_reduction | 40 percentage points |
| bridge_confidence | 0.75 |

`additional_candidates_evaluable` is counted as candidates with bridge-retained samples at or above the minimum sample threshold path used by the simulation. Five candidates gained samples, but only three gained enough samples to be meaningfully evaluated; two of those became simulated `CONFIRMED`.

## Candidate-Level Effects

| candidate_id | baseline | bridge_samples | bridge_outcome | effect |
| --- | --- | ---: | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | `INSUFFICIENT_DATA`, 0 samples | 146 | `CONFIRMED` | material improvement |
| `ptc_backtest_final_3a4ac24107c77136` | `INSUFFICIENT_DATA`, 0 samples | 146 | `CONFIRMED` | material improvement |
| `ptc_backtest_final_d5931b24bd391113` | `INSUFFICIENT_DATA`, 0 samples | 49 | `INSUFFICIENT_DATA` / `BACKTEST_WEAK` | sample recovery only |
| `ptc_backtest_final_7d839944a8a4070a` | `INSUFFICIENT_DATA`, 0 samples | 27 | `INSUFFICIENT_DATA` | below sample threshold |
| `ptc_backtest_final_b23c6756bfb3263a` | `INSUFFICIENT_DATA`, 0 samples | 28 | `INSUFFICIENT_DATA` | below sample threshold |

## Overlay Comparison

| overlay | added samples | confirmed candidates | retained value | risk |
| --- | ---: | ---: | --- | --- |
| `CHOP -> RANGE_BOUND` | 396 | 2 | high | medium |
| `CHOP -> LOW_VOLATILITY` | 2 | 0 | negligible | high |
| `CHOP -> RANGE_BOUND OR LOW_VOLATILITY` | 398 | 2 | no candidate-level gain over `RANGE_BOUND` | higher than needed |
| `TREND -> TRENDING` | 0 | 0 | irrelevant to current affected set | low |
| highest-confidence crosswalk | 396 | 2 | high | medium |

## False Positive Risk

False positive risk: `MEDIUM`.

Reasons:

- The bridge row is a `PARTIAL_MATCH`, not an exact semantic match.
- `CHOP` is candidate-side and underdefined; `RANGE_BOUND` is a daily proxy classifier state.
- Daily `RANGE_BOUND` cannot prove intraday chop, two-way whipsaw behavior, or event-window behavior.
- One restored candidate has an intraday-plus-daily event-reaction requirement and remains blocked despite 49 retained samples.
- Expanding to `LOW_VOLATILITY` adds only 2 samples and no confirmed candidates, while increasing semantic dilution.

Mitigating evidence:

- `CHOP -> RANGE_BOUND` adds 396 samples.
- Two candidates become simulated `CONFIRMED`.
- The bridge reduces the affected-candidate block rate from 100% to 60%.
- The result is falsifiable because three candidates remain blocked rather than being blindly passed.

## Bridge Confidence

Bridge confidence: `0.75`.

This comes from the design bridge row for `CHOP -> RANGE_BOUND`: a generated-only design confidence, not a statistical probability. The observed effectiveness supports keeping this confidence in the moderate-high range for diagnostic use, but it does not justify treating the mapping as exact validation truth.

## Decision

Decision: `REFINE`.

Rationale:

- `KEEP` alone is too weak because the bridge is materially useful and should be shaped into a stricter diagnostic contract.
- `EXPAND` is not supported because `LOW_VOLATILITY` and `UNKNOWN` do not add confirmed candidates and increase false-positive risk.
- `RETIRE` is not supported because the bridge recovers 396 samples and confirms 2 affected candidates in simulation.
- `REFINE` is the right action: preserve `CHOP -> RANGE_BOUND` as a diagnostic bridge, prevent silent promotion to validation authority, and require explicit labels for bridge-derived evaluations.

## Recommended Refinements

1. Keep `CHOP -> RANGE_BOUND` as the only supported CHOP bridge overlay for now.
2. Do not include `LOW_VOLATILITY` in the default bridge.
3. Do not use `UNKNOWN` as a CHOP substitute.
4. Label bridge-derived results as approximation evidence, not exact validation evidence.
5. Require separate intraday/event validation for event-reaction or intraday-timed CHOP candidates.
6. Track both sample-retention improvement and candidate-level outcome improvement; sample recovery alone is not confirmation.

## Final Output

`REFINE`

## Authority Boundary

This report is diagnostic only. It does not implement a vocabulary bridge, change replay behavior, relax validation filters, validate candidates, change qualification, promote candidates, recommend trades, allocate capital, authorize broker execution, size positions, place paper trades, or alter runtime governance.
