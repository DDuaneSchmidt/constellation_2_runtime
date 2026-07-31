# Direct Validation Pipeline Map 001

Date: 2026-06-05

## Scope

This map documents the current Atlas v2 research OS direct candidate data validation path. It is descriptive only and does not change the pipeline.

Primary implementation references:

- `constellation_2/common/atlas_v2_research_os/direct_candidate_data_validation.py`
- `constellation_2/common/atlas_v2_research_os/candidate_backtests.py`

## Inputs

Direct validation reads:

- `focused_observation_campaign/latest.json`: candidate list, mechanism, regime, proxy backtest metrics.
- `candidate_data_validation_plan/latest.json`: required symbols or universe, timeframe, minimum lookback, needed indicators, validation steps.
- `candidate_symbol_attribution/latest.json`: resolved candidate symbols, universe symbols, source observation ids, candidate timeframes, attribution method and confidence.
- Local market data CSV files discovered from the repository/report root.

Checkpoint:

- Current run reviewed 8 candidates.
- Current coverage tracker reports 8/8 full candidate coverage and 100 percent symbol coverage.

## Symbol Attribution

The validator resolves symbols from candidate, plan, and attribution fields:

- `candidate_symbol`
- `symbol`
- `symbols`
- `universe_symbols`
- `candidate_symbols`
- `candidate_universe_symbols`

Checkpoint:

- Current run attributed symbols for 8/8 candidates.
- Zero-sample candidates all have symbols attributed and local data present.

## Data Loading

For each resolved symbol, direct validation discovers local market data and ranks files by requested timeframe, with daily as fallback. CSVs are normalized to OHLCV rows.

Checkpoint:

- Current run has direct data for 8/8 candidates.
- The five zero-sample candidates have local daily data for every audited symbol.

## Trigger Generation

The candidate backtest path computes daily features and applies a deterministic mechanism trigger:

- BREAKOUT: close above prior 20-day high.
- MEAN_REVERSION: close at least 1 percent below 20-day average.
- REVERSAL: three-day down streak followed by close above open.
- EVENT_REACTION: large positive daily move versus 20-day volatility.

Checkpoint for zero-sample candidates:

- `ptc_backtest_final_469607b8340421b7`: 1049 trigger samples.
- `ptc_backtest_final_3a4ac24107c77136`: 1049 trigger samples.
- `ptc_backtest_final_d5931b24bd391113`: 503 trigger samples.
- `ptc_backtest_final_7d839944a8a4070a`: 176 trigger samples.
- `ptc_backtest_final_b23c6756bfb3263a`: 626 trigger samples.

## Regime Filtering

Each trigger sample receives a daily-bar regime from the current classifier:

- `HIGH_VOLATILITY`
- `TRENDING`
- `RANGE_BOUND`
- `LOW_VOLATILITY`
- `UNKNOWN`

Candidate plans pass allowed regimes using the candidate regime label, such as `CHOP` or `TRENDING`. If the candidate regime is not `UNKNOWN`, the sample survives only when the computed daily-bar regime exactly matches an allowed regime.

Checkpoint for zero-sample candidates:

- All five zero-sample candidates have zero regime survivors.
- The observed fatal stage is regime filtering.
- The likely semantic issue is that candidate `CHOP` is not currently translated to daily-bar `RANGE_BOUND` or another compatible regime label.

## Quality Filtering

There is no separate pre-replay quality-filter stage in the current direct validation path. Sample-size and metric sufficiency are applied during classification after samples are formed.

Checkpoint:

- For the five zero-sample candidates, quality-filter surviving count is recorded diagnostically as zero because no samples survive regime filtering.

## Evidence Filtering

There is no separate pre-replay evidence-filter stage in the current direct validation path. Evidence sufficiency is represented by missing-data messages, sample-size checks, and classification.

Checkpoint:

- For the five zero-sample candidates, evidence-filter surviving count is recorded diagnostically as zero because no samples survive regime filtering.

## Universe Aggregation

The current implementation runs each resolved symbol independently and chooses the best direct run. It does not require all-symbol confirmation, and it does not combine symbol samples into a universe-level sample set.

Checkpoint:

- Individual symbols for all five zero-sample candidates also have zero regime survivors.
- Aggregation is not the immediate sample-loss cause.
- Aggregation/reporting is diagnostically lossy because only the selected symbol result is surfaced in the direct validation report.

## Classification

After samples are formed, historical replay metrics are computed and classified:

- `BACKTEST_SUPPORTED` if sample size and replay metrics pass thresholds.
- `BACKTEST_WEAK` if enough samples exist but support is weak.
- `INSUFFICIENT_DATA` if samples are fewer than 30 or required data is missing.
- `SPEC_TOO_AMBIGUOUS` if no deterministic trigger exists.

Checkpoint:

- Five zero-sample candidates classify as `INSUFFICIENT_DATA`.
- Two direct-validation candidates classify as `CONFIRMED`.
- One candidate has nonzero samples but remains `INSUFFICIENT_DATA` in the direct validation report.

## Report Writing

The direct validation report writes:

- `reports/atlas_v2_research_os/direct_candidate_data_validation/YYYY-MM-DD/direct_candidate_data_validation_report.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/YYYY-MM-DD/direct_candidate_data_validation_summary.md`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest_summary.md`

Checkpoint:

- Current latest report exists and contains an authority boundary prohibiting live trading, broker execution, capital allocation, position sizing, portfolio construction, automatic paper placement, and candidate production promotion.

## Authority Boundary

This design map is documentation only. It does not implement replay changes, promote candidates, override replay, override qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
