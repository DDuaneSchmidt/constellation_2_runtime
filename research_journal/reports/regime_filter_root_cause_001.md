# Regime Filter Root Cause 001

Date: 2026-06-05

## Scope

This report diagnoses the five direct-validation candidates with trigger samples greater than zero and final replay samples equal to zero. The analysis is offline only and does not change production replay behavior.

Source artifacts:

- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/candidate_data_validation_plan/latest.json`
- `constellation_2/common/atlas_v2_research_os/candidate_backtests.py`

CSV companion: `research_journal/reports/regime_filter_root_cause_table.csv`.

## Root Cause

The direct validation regime gate uses exact allowed-regime matching:

`triggered_sample.regime in allowed_regimes`

For the five affected candidates, `allowed_regimes=[CHOP]`. The daily proxy replay regime classifier emits only:

- `HIGH_VOLATILITY`
- `TRENDING`
- `RANGE_BOUND`
- `LOW_VOLATILITY`
- `UNKNOWN`

It does not emit `CHOP`. Therefore an exact `CHOP` condition is impossible under the current daily proxy replay regime vocabulary, even when trigger samples exist.

## Candidate Findings

| candidate_id | symbols | timeframe | triggers | observed triggered-sample regimes | exact CHOP pass | first failed condition | classification |
|---|---|---|---:|---|---:|---|---|
| `ptc_backtest_final_469607b8340421b7` | DIA, QQQ, SPY | daily bars plus optional intraday confirmation | 1049 | HIGH_VOLATILITY:40; LOW_VOLATILITY:1; RANGE_BOUND:146; TRENDING:801; UNKNOWN:61 | 0 | No triggered sample had `CHOP` | REGIME_LABEL_MISMATCH |
| `ptc_backtest_final_3a4ac24107c77136` | DIA, QQQ, SPY | daily bars plus optional intraday confirmation | 1049 | HIGH_VOLATILITY:40; LOW_VOLATILITY:1; RANGE_BOUND:146; TRENDING:801; UNKNOWN:61 | 0 | No triggered sample had `CHOP` | REGIME_LABEL_MISMATCH |
| `ptc_backtest_final_d5931b24bd391113` | AMZN, BAC, META, MSFT, NFLX, TSLA | intraday plus daily confirmation | 503 | HIGH_VOLATILITY:254; RANGE_BOUND:49; TRENDING:174; UNKNOWN:26 | 0 | No triggered sample had `CHOP` | TIMEFRAME_REGIME_MISMATCH |
| `ptc_backtest_final_7d839944a8a4070a` | TLT, USO | daily bars plus optional intraday confirmation | 176 | HIGH_VOLATILITY:41; RANGE_BOUND:27; TRENDING:97; UNKNOWN:11 | 0 | No triggered sample had `CHOP` | REGIME_LABEL_MISMATCH |
| `ptc_backtest_final_b23c6756bfb3263a` | BAC, GOOGL, META, MSFT, NFLX, TSLA | daily bars plus optional intraday confirmation | 626 | HIGH_VOLATILITY:323; RANGE_BOUND:28; TRENDING:246; UNKNOWN:29 | 0 | No triggered sample had `CHOP` | REGIME_LABEL_MISMATCH |

## Interpretation

The filter is too strict for the current daily proxy vocabulary because it treats candidate-level `CHOP` as an exact replay-regime label. The closest daily proxy labels are likely `RANGE_BOUND`, `LOW_VOLATILITY`, and possibly `UNKNOWN`, but that compatibility is not currently declared or validated before replay.

The event-reaction candidate, `ptc_backtest_final_d5931b24bd391113`, also has a true timeframe issue because its required timeframe is `intraday plus daily confirmation`. Daily replay can expose the label mismatch, but it cannot certify intraday event-regime evidence.

## Candidate Remediation Notes

- `ptc_backtest_final_469607b8340421b7`: add CHOP-to-daily-regime compatibility diagnostics before replay; do not relax production filters yet.
- `ptc_backtest_final_3a4ac24107c77136`: same as above; identical symbol set and triggered-regime distribution.
- `ptc_backtest_final_d5931b24bd391113`: separate intraday-required validation from daily proxy replay and require event/intraday metadata.
- `ptc_backtest_final_7d839944a8a4070a`: add CHOP compatibility diagnostics for TLT/USO daily triggered samples.
- `ptc_backtest_final_b23c6756bfb3263a`: add CHOP compatibility diagnostics for the equity basket; do not treat exact-label rejection as candidate failure.

## Authority Boundary

This report is diagnostic only. It does not change replay behavior, relax production filters, promote candidates, change qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
