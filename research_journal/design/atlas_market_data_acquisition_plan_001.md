# Atlas Market Data Acquisition Plan 001

Status: PLANNING ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: executable acquisition plan to raise Atlas candidate validation data coverage. This document does not download data, implement ingestion, alter replay, alter candidates, alter qualification, alter governance, allocate capital, recommend trades, size positions, place paper trades, broker execution, or promote candidates.

## Objective

Reach at least `75%` direct candidate validation coverage for the current Atlas candidate universe.

The measurable target for this plan is candidate-symbol coverage:

```text
covered candidate-symbol pairs / required candidate-symbol pairs >= 75%
```

This is the most practical near-term target because several candidates are universe-level and require multiple symbols. Full-candidate coverage is stricter and will require additional symbols beyond the first high-leverage batch.

## Inputs Reviewed

- `reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md`
- `reports/atlas_v2_research_os/market_data_readiness/required_market_data_manifest.csv`
- `reports/atlas_v2_research_os/candidate_data_validation_plan/latest_summary.md`

Current baseline:

- candidate_count: `8`
- candidate_symbol_count: `15`
- candidate-symbol pairs: `32`
- symbols with local direct data: `1` (`SPY`)
- current candidate-symbol coverage: `2/32 = 6.25%`
- current unique-symbol coverage: `1/15 = 6.67%`
- candidate validation block rate: `100%`

## Required Symbols

Current candidate universe symbols:

| Symbol | Candidates Blocked | Priority | Reason |
| --- | ---: | ---: | --- |
| `BAC` | 3 | 1 | Appears in three equity/event candidate universes. |
| `DIA` | 3 | 2 | Completes Dow/ETF breakout and mean-reversion candidates with QQQ. |
| `META` | 3 | 3 | Appears in three equity/event candidate universes. |
| `MSFT` | 3 | 4 | Appears in three equity/event candidate universes. |
| `QQQ` | 3 | 5 | Completes ETF candidate set with DIA and existing SPY. |
| `TSLA` | 3 | 6 | Appears in three equity/event candidate universes. |
| `AMZN` | 2 | 7 | Required to reach 75% candidate-symbol coverage. |
| `NFLX` | 2 | 8 | Required to reach 75% candidate-symbol coverage. |
| `TLT` | 2 | 9 | Required for rates/commodity ETF candidates. |
| `USO` | 2 | 10 | Required for commodity/oil ETF candidates. |
| `AAPL` | 1 | 11 | Completes one equity candidate with AMZN/BAC/JPM/META/MSFT/TSLA. |
| `DBC` | 1 | 12 | Completes one commodity ETF candidate with TLT/USO. |
| `GOOGL` | 1 | 13 | Completes one equity candidate with BAC/META/MSFT/NFLX/TSLA. |
| `JPM` | 1 | 14 | Completes one equity candidate with AAPL/AMZN/BAC/META/MSFT/TSLA. |
| `SPY` | 2 | Existing | Daily data already present; intraday still missing. |

## Required Frequencies

### Daily Bars

Required for all candidate symbols.

Expected fields:

- date
- open
- high
- low
- close
- volume
- adjusted close or adjusted OHLC fields where available
- split/dividend adjustment metadata where available

Expected paths:

```text
data/cache/{SYMBOL}_tiingo_adjusted_daily.csv
```

Daily bars are the first acquisition layer because every candidate validation plan requires daily direct replay context.

### Intraday Bars

Required for candidates with event timing, opening range, breakout confirmation, or intraday rule validation.

Manifest frequencies:

- `1h`: AAPL, AMZN, BAC, JPM, META, MSFT, TSLA
- `30m`: AMZN, BAC, DBC, DIA, GOOGL, META, MSFT, NFLX, QQQ, SPY, TLT, TSLA, USO
- `15m`: TLT, USO
- `5m`: DIA, QQQ, SPY

Expected paths:

```text
data/historical/{SYMBOL}_1h.csv
data/historical/{SYMBOL}_30m.csv
data/historical/{SYMBOL}_15m.csv
data/historical/{SYMBOL}_5m.csv
```

Intraday data should follow daily data. It is needed for stronger validation, but daily coverage alone unlocks the first direct-data replay audit pass.

## Required History Depth

Target date range from the manifest:

```text
2021-06-05 through 2026-06-05
```

Minimum depth:

- `5 years preferred`
- `2 years minimum` only for provisional paper-forward validation context

Recommended acquisition depth:

- Daily: full 5-year window for every required symbol.
- Intraday: full 5-year window where provider coverage and storage cost are acceptable; otherwise acquire at least 2 years and mark the gap explicitly.

## Storage Estimates

These estimates are planning approximations only. Actual size depends on provider fields, adjustment metadata, timestamp format, and compression.

Assumptions:

- Trading days per 5 years: about `1,260`
- 6.5 trading hours per regular session
- 1h bars: about `7` bars/day, `8,820` rows/symbol
- 30m bars: about `13` bars/day, `16,380` rows/symbol
- 15m bars: about `26` bars/day, `32,760` rows/symbol
- 5m bars: about `78` bars/day, `98,280` rows/symbol
- CSV row size estimate: `120-180 bytes` for OHLCV plus timestamp; use `160 bytes` planning estimate

Approximate uncompressed CSV sizes:

| Frequency | Rows / Symbol | Size / Symbol |
| --- | ---: | ---: |
| `1d` | 1,260 | 0.2 MB |
| `1h` | 8,820 | 1.4 MB |
| `30m` | 16,380 | 2.6 MB |
| `15m` | 32,760 | 5.2 MB |
| `5m` | 98,280 | 15.7 MB |

Priority batch storage estimate:

| Batch | Symbols | Daily | Intraday Required | Approx Size |
| --- | ---: | ---: | ---: | ---: |
| Batch 1 | 6 | 1.2 MB | 6 required intraday files per listed frequency set | about 35-55 MB |
| Batch 2 | 2 | 0.4 MB | AMZN 1h/30m, NFLX 30m | about 7-10 MB |
| Batch 3 | 4 | 0.8 MB | TLT/USO 15m/30m, AAPL/JPM 1h | about 18-25 MB |
| Batch 4 | 2 | 0.4 MB | DBC/GOOGL 30m | about 5-6 MB |

Full current universe estimate:

- Daily only, missing 14 symbols: about `2.8 MB`
- All manifest intraday missing files: about `65-95 MB`
- Total uncompressed planning estimate: about `70-100 MB`

Storage is not the limiting factor. Provider availability, timestamp normalization, adjustment consistency, and validation are the real constraints.

## Coverage Projections

Coverage uses candidate-symbol pairs, current total `32`.

Existing local coverage:

- `SPY`: 2 candidate-symbol pairs
- baseline: `2/32 = 6.25%`

Acquisition sequence:

| Step | Add Symbol | New Covered Pairs | Cumulative Pairs | Candidate-Symbol Coverage | Unique-Symbol Coverage |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | existing `SPY` | 2 | 2 | 6.25% | 6.67% |
| 1 | `BAC` | 3 | 5 | 15.63% | 13.33% |
| 2 | `DIA` | 3 | 8 | 25.00% | 20.00% |
| 3 | `META` | 3 | 11 | 34.38% | 26.67% |
| 4 | `MSFT` | 3 | 14 | 43.75% | 33.33% |
| 5 | `QQQ` | 3 | 17 | 53.13% | 40.00% |
| 6 | `TSLA` | 3 | 20 | 62.50% | 46.67% |
| 7 | `AMZN` | 2 | 22 | 68.75% | 53.33% |
| 8 | `NFLX` | 2 | 24 | 75.00% | 60.00% |

Conclusion:

- The expected first six symbols are correct as the highest-blocker group.
- They do not reach 75% candidate-symbol coverage by themselves.
- `AMZN` and `NFLX` should be included in the executable 75% plan.

Full-candidate coverage projection:

| Acquisition Set | Fully Covered Candidates | Full-Candidate Coverage |
| --- | ---: | ---: |
| Existing `SPY` only | 0/8 | 0.00% |
| Add `DIA`, `QQQ` | 3/8 | 37.50% |
| Add first six symbols | 3/8 | 37.50% |
| Add `AMZN`, `NFLX` | 3/8 | 37.50% |
| Add `TLT`, `USO` | 4/8 | 50.00% |
| Add `DBC` | 5/8 | 62.50% |
| Add `GOOGL` | 6/8 | 75.00% |

If the target is full-candidate coverage rather than candidate-symbol coverage, the plan must extend through `GOOGL` and include `TLT`, `USO`, and `DBC`.

## Acquisition Phases

### Phase 1: Highest-Blocker Daily Data

Goal: reach 62.5% candidate-symbol coverage and complete ETF candidates requiring DIA/QQQ/SPY.

Symbols:

- `BAC`
- `DIA`
- `META`
- `MSFT`
- `QQQ`
- `TSLA`

Required files:

```text
data/cache/BAC_tiingo_adjusted_daily.csv
data/cache/DIA_tiingo_adjusted_daily.csv
data/cache/META_tiingo_adjusted_daily.csv
data/cache/MSFT_tiingo_adjusted_daily.csv
data/cache/QQQ_tiingo_adjusted_daily.csv
data/cache/TSLA_tiingo_adjusted_daily.csv
```

Expected validation effect:

- Candidate-symbol coverage rises from `6.25%` to `62.50%`.
- Full-candidate coverage rises from `0%` to `37.50%` once `DIA` and `QQQ` are present.
- Direct validation should still remain blocked for equity/event universes that require AMZN/NFLX/AAPL/JPM/GOOGL.

### Phase 2: 75% Candidate-Symbol Coverage Completion

Goal: reach exactly 75% candidate-symbol coverage.

Symbols:

- `AMZN`
- `NFLX`

Required files:

```text
data/cache/AMZN_tiingo_adjusted_daily.csv
data/cache/NFLX_tiingo_adjusted_daily.csv
```

Expected validation effect:

- Candidate-symbol coverage rises from `62.50%` to `75.00%`.
- Equity/event candidates remain partially blocked until AAPL/JPM/GOOGL are acquired.

### Phase 3: Rates/Commodity ETF Candidate Completion

Goal: unlock TLT/USO candidates and move toward full-candidate coverage.

Symbols:

- `TLT`
- `USO`
- `DBC`

Required files:

```text
data/cache/TLT_tiingo_adjusted_daily.csv
data/cache/USO_tiingo_adjusted_daily.csv
data/cache/DBC_tiingo_adjusted_daily.csv
```

Expected validation effect:

- `ptc_backtest_final_7d839944a8a4070a` becomes fully covered when `TLT` and `USO` are present.
- `ptc_backtest_final_624fdd85668e2c08` becomes fully covered when `DBC`, `TLT`, and `USO` are present.
- Full-candidate coverage can reach `62.50%`.

### Phase 4: Remaining Equity Universe Completion

Goal: reach at least 75% full-candidate coverage.

Symbols:

- `AAPL`
- `GOOGL`
- `JPM`

Minimum required for 75% full-candidate coverage:

- `GOOGL` after Phase 2 completes `ptc_backtest_final_b23c6756bfb3263a`.

Full completion:

- `AAPL` and `JPM` complete `ptc_backtest_final_4df2e8e80685a054`.

Required files:

```text
data/cache/AAPL_tiingo_adjusted_daily.csv
data/cache/GOOGL_tiingo_adjusted_daily.csv
data/cache/JPM_tiingo_adjusted_daily.csv
```

## Intraday Follow-Up Plan

After daily coverage reaches the 75% candidate-symbol target, acquire intraday data in this order:

1. `DIA`, `QQQ`, `SPY` at `5m` and `30m` for ETF breakout/opening-range style validation.
2. `BAC`, `META`, `MSFT`, `TSLA`, `AMZN` at `1h` and `30m` for equity/event validation.
3. `NFLX`, `GOOGL`, `DBC` at `30m`.
4. `TLT`, `USO` at `15m` and `30m`.
5. `AAPL`, `JPM` at `1h`.

Intraday validation should not be treated as live execution support. It is historical research data only.

## Validation Estimates

After each acquisition phase, run read-only validation checks:

1. File presence check against manifest.
2. CSV schema check: timestamp/date, OHLCV, adjusted fields where required.
3. Date coverage check: first date <= `2021-06-05`, last date near current available market date.
4. Row-count reasonableness:
   - daily: about `1,200-1,300` rows for five years
   - 1h: about `8,000-9,000` rows
   - 30m: about `15,000-17,000` rows
   - 15m: about `30,000-34,000` rows
   - 5m: about `90,000-100,000` rows
5. Gap check for missing sessions or duplicated timestamps.
6. Adjustment consistency check for daily files.
7. Candidate validation rerun in audit/report mode only.

Expected validation progression:

| Phase | Candidate-Symbol Coverage | Full-Candidate Coverage | Expected Block Rate |
| --- | ---: | ---: | ---: |
| Baseline | 6.25% | 0.00% | 100% |
| Phase 1 daily | 62.50% | 37.50% | lower, but still materially blocked |
| Phase 2 daily | 75.00% | 37.50% | lower, still blocked for incomplete universes |
| Phase 3 daily | 84.38% | 62.50% | materially improved |
| Phase 4 daily minimum with `GOOGL` | 87.50% | 75.00% | target full-candidate coverage reached |
| Phase 4 daily complete | 93.75% | 87.50% | only remaining gaps are intraday and any rule-specific data |

## Acceptance Criteria

For 75% candidate-symbol coverage:

- Local daily CSVs exist for `BAC`, `DIA`, `META`, `MSFT`, `QQQ`, `TSLA`, `AMZN`, and `NFLX`.
- Files cover `2021-06-05` through current available market date.
- CSV schemas pass OHLCV validation.
- Candidate-symbol coverage report shows at least `24/32 = 75%`.
- No candidate, replay, qualification, governance, broker, capital, or production state is mutated by the acquisition validation.

For 75% full-candidate coverage:

- Complete Phase 1, Phase 2, Phase 3, and acquire at least `GOOGL`.
- Full candidate coverage report shows at least `6/8 = 75%`.
- Remaining incomplete candidate universes are explicitly listed.

## Risks And Controls

| Risk | Control |
| --- | --- |
| Split-adjustment mismatch | Prefer adjusted daily source with split/dividend fields; validate against existing SPY format. |
| Intraday gaps | Run timestamp gap reports before using intraday files in validation. |
| Provider field mismatch | Normalize only after schema audit; do not silently coerce missing OHLCV. |
| False sense of validation | Label all outputs historical data validation only. |
| Candidate influence creep | Keep acquisition and validation reports advisory; no candidate state mutation. |
| Replay authority creep | Data availability may enable replay audit, but does not override replay outcomes. |

## No-Download Execution Checklist For Next Session

This plan intentionally does not download data. A future acquisition session should:

1. Confirm provider and licensing rules.
2. Confirm target directory layout.
3. Acquire Phase 1 daily files.
4. Run schema/date/row-count validation.
5. Update coverage audit.
6. Acquire Phase 2 daily files.
7. Re-run coverage audit and direct candidate validation in report-only mode.
8. Stop if authority-boundary checks fail.

## Authority Boundary

This document has no authority to:

- download data
- implement ingestion
- change replay
- change candidates
- change qualification
- change governance
- write memory automatically
- recommend trades
- allocate capital
- size positions
- construct portfolios
- place paper trades
- broker execution
- promote candidates
- authorize live trading

It is a planning artifact only.

