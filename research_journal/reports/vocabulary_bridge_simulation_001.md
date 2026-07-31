# Vocabulary Bridge Simulation 001

Date: 2026-06-05

## Scope

This is an offline simulation to prove or falsify whether a regime vocabulary bridge would materially improve direct validation outcomes. It does not implement a bridge and does not change replay, qualification, candidate state, or governance.

Inputs:

- `research_journal/reports/validation_regime_mapping_study_001.md`
- `research_journal/reports/regime_vocabulary_crosswalk.csv`
- `research_journal/reports/candidate_replay_trace_001.md`
- `research_journal/reports/regime_filter_root_cause_001.md`
- `research_journal/reports/regime_filter_sensitivity_001.md`

CSV companion: `research_journal/reports/vocabulary_bridge_simulation_table.csv`.

## Method

The simulation rebuilt direct-validation samples offline from existing local data and deterministic trigger logic. It then applied candidate-regime-to-validator-regime mappings only in memory, computed surviving samples, ran offline replay metrics for those samples, and classified the result using the same direct-validation outcome style.

Mappings tested:

- Mapping A: `CHOP -> RANGE_BOUND`
- Mapping B: `CHOP -> LOW_VOLATILITY`
- Mapping C: `CHOP -> RANGE_BOUND OR LOW_VOLATILITY`
- Mapping D: `TREND -> TRENDING`
- Mapping E: highest-confidence crosswalk mapping, which is effectively `CHOP -> RANGE_BOUND` for the affected candidates and `TREND/TRENDING -> TRENDING` where applicable.

## Summary Results

| mapping | additional surviving samples | additional candidates validated | candidates with any improvement | sample-sufficiency unblocked candidates | estimated validation block-rate reduction |
|---|---:|---:|---:|---:|---:|
| A: `CHOP -> RANGE_BOUND` | 396 | 2 | 5 | 3 | 40 percentage points |
| B: `CHOP -> LOW_VOLATILITY` | 2 | 0 | 2 | 0 | 0 percentage points |
| C: `CHOP -> RANGE_BOUND OR LOW_VOLATILITY` | 398 | 2 | 5 | 3 | 40 percentage points |
| D: `TREND -> TRENDING` | 0 | 0 | 0 | 0 | 0 percentage points |
| E: highest-confidence crosswalk | 396 | 2 | 5 | 3 | 40 percentage points |

Baseline affected-candidate validation block rate was 5/5, or 100 percent. Under mappings A, C, or E, two candidates move from `INSUFFICIENT_DATA` to `CONFIRMED`, reducing the validation block rate to 3/5, or 60 percent. That is a 40 percentage point reduction.

## Candidate Outcomes

### `ptc_backtest_final_469607b8340421b7`

Current: `CHOP`, 1049 trigger samples, 0 surviving samples, `INSUFFICIENT_DATA`.

- Mapping A: 146 samples survive; simulated outcome `CONFIRMED`; `EXACT IMPROVEMENT`.
- Mapping B: 1 sample survives; simulated outcome `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.
- Mapping C: 147 samples survive; simulated outcome `CONFIRMED`; `EXACT IMPROVEMENT`.
- Mapping D: no applicable mapping; `NO IMPROVEMENT`.
- Mapping E: 146 samples survive; simulated outcome `CONFIRMED`; `EXACT IMPROVEMENT`.

### `ptc_backtest_final_3a4ac24107c77136`

Current: `CHOP`, 1049 trigger samples, 0 surviving samples, `INSUFFICIENT_DATA`.

- Mapping A: 146 samples survive; simulated outcome `CONFIRMED`; `EXACT IMPROVEMENT`.
- Mapping B: 1 sample survives; simulated outcome `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.
- Mapping C: 147 samples survive; simulated outcome `CONFIRMED`; `EXACT IMPROVEMENT`.
- Mapping D: no applicable mapping; `NO IMPROVEMENT`.
- Mapping E: 146 samples survive; simulated outcome `CONFIRMED`; `EXACT IMPROVEMENT`.

### `ptc_backtest_final_d5931b24bd391113`

Current: `CHOP`, 503 trigger samples, 0 surviving samples, `INSUFFICIENT_DATA`.

- Mapping A: 49 samples survive; simulated backtest is `BACKTEST_WEAK`; direct outcome remains `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.
- Mapping B: 0 samples survive; `NO IMPROVEMENT`.
- Mapping C: 49 samples survive; simulated backtest is `BACKTEST_WEAK`; direct outcome remains `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.
- Mapping D: no applicable mapping; `NO IMPROVEMENT`.
- Mapping E: 49 samples survive; simulated backtest is `BACKTEST_WEAK`; direct outcome remains `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.

This candidate also has an intraday-plus-daily requirement, so a vocabulary bridge alone cannot resolve the full validation mismatch.

### `ptc_backtest_final_7d839944a8a4070a`

Current: `CHOP`, 176 trigger samples, 0 surviving samples, `INSUFFICIENT_DATA`.

- Mapping A: 27 samples survive; direct outcome remains `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.
- Mapping B: 0 samples survive; `NO IMPROVEMENT`.
- Mapping C: 27 samples survive; direct outcome remains `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.
- Mapping D: no applicable mapping; `NO IMPROVEMENT`.
- Mapping E: 27 samples survive; direct outcome remains `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.

The bridge restores some samples but not enough to clear the minimum sample threshold.

### `ptc_backtest_final_b23c6756bfb3263a`

Current: `CHOP`, 626 trigger samples, 0 surviving samples, `INSUFFICIENT_DATA`.

- Mapping A: 28 samples survive; direct outcome remains `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.
- Mapping B: 0 samples survive; `NO IMPROVEMENT`.
- Mapping C: 28 samples survive; direct outcome remains `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.
- Mapping D: no applicable mapping; `NO IMPROVEMENT`.
- Mapping E: 28 samples survive; direct outcome remains `INSUFFICIENT_DATA`; `PARTIAL IMPROVEMENT`.

The bridge restores some samples but not enough to clear the minimum sample threshold.

## Conclusion

A regime vocabulary bridge would materially improve direct validation, but only partially.

The evidence supports Mapping A or Mapping E as materially useful because they add 396 surviving samples and convert two of five affected candidates from `INSUFFICIENT_DATA` to simulated `CONFIRMED`. Mapping C adds two extra low-volatility samples but does not improve candidate-level outcomes beyond Mapping A. Mapping B is effectively not useful. Mapping D is irrelevant to the current affected candidates because their candidate regime is `CHOP`, not `TREND`.

Falsification boundary: a bridge alone does not solve all five candidates. Three remain blocked after the best mapping family: one has weak simulated evidence and an intraday-plus-daily requirement, and two remain below the minimum sample threshold. Therefore a bridge is material for reducing attrition, but insufficient as a standalone remediation.

## Authority Boundary

This simulation is diagnostic only. It does not implement a vocabulary bridge, change replay behavior, relax production filters, promote candidates, change qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
