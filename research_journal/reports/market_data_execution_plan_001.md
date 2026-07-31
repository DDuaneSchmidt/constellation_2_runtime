# Market Data Execution Plan 001

Status: EXECUTION PLAN ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: plan the minimum market data acquisition needed to reach `75%` Atlas candidate-symbol coverage. This report does not download data, implement ingestion, change candidates, change replay, change qualification, change governance, change production systems, recommend trades, allocate capital, size positions, place paper trades, broker execution, or promote candidates.

## Goal

Acquire the minimum dataset required to move from current coverage to:

```text
candidate-symbol coverage >= 75%
```

Current denominator:

```text
32 candidate-symbol pairs
```

Current coverage:

```text
SPY covers 2 pairs
2 / 32 = 6.25% candidate-symbol coverage
1 / 15 = 6.67% unique-symbol coverage
```

Required target:

```text
24 / 32 candidate-symbol pairs covered
```

Minimum acquisition set:

```text
DIA, QQQ, BAC, META, MSFT, TSLA, AMZN, NFLX
```

This set adds `22` candidate-symbol pairs. With existing `SPY`, total covered pairs become `24/32 = 75.00%`.

## Source Availability Inventory

### Local Availability

| Symbol | Local Daily Data | Current Local Path | Status |
| --- | --- | --- | --- |
| `SPY` | Yes | `data/cache/SPY_tiingo_adjusted_daily.csv` | Existing baseline |
| `DIA` | No | `data/cache/DIA_tiingo_adjusted_daily.csv` | Required |
| `QQQ` | No | `data/cache/QQQ_tiingo_adjusted_daily.csv` | Required |
| `BAC` | No | `data/cache/BAC_tiingo_adjusted_daily.csv` | Required |
| `META` | No | `data/cache/META_tiingo_adjusted_daily.csv` | Required |
| `MSFT` | No | `data/cache/MSFT_tiingo_adjusted_daily.csv` | Required |
| `TSLA` | No | `data/cache/TSLA_tiingo_adjusted_daily.csv` | Required |
| `AMZN` | No | `data/cache/AMZN_tiingo_adjusted_daily.csv` | Required |
| `NFLX` | No | `data/cache/NFLX_tiingo_adjusted_daily.csv` | Required |

### Expected Source Type

The local naming convention and existing `SPY` file imply adjusted daily OHLCV from the same provider style:

```text
data/cache/{SYMBOL}_tiingo_adjusted_daily.csv
```

The minimum acquisition should use one consistent adjusted daily source for all eight symbols. Mixed providers should be avoided for the first pass because provider differences can create adjustment, volume, timestamp, and corporate-action inconsistencies.

### Source Readiness

All eight symbols are actively traded U.S. listed equities or ETFs with ordinary daily OHLCV availability from mainstream historical market data providers. The execution risk is not symbol availability; it is:

- provider credential/licensing readiness
- field schema consistency
- adjustment convention consistency
- date coverage through the current available market date
- preserving source lineage and checksums

No source has been queried or downloaded by this plan.

## Minimum Dataset

### Required Symbols

Priority order:

1. `DIA`
2. `QQQ`
3. `BAC`
4. `META`
5. `MSFT`
6. `TSLA`
7. `AMZN`
8. `NFLX`

### Required Frequency

Minimum required frequency for the 75% candidate-symbol coverage target:

```text
1d adjusted OHLCV
```

Intraday data remains required for stronger rule-specific validation, but it is not required to reach the 75% candidate-symbol coverage threshold defined in the roadmap.

### Required History Depth

Minimum target:

```text
2021-06-05 through current available market date
```

Preferred:

```text
5 full years, matching the required market data manifest
```

Acceptance rule:

- `first_date <= 2021-06-05`
- `last_date` within one completed trading session of the acquisition date
- row count approximately `1,200-1,300` daily bars per symbol for the 5-year window

## Ingestion Format

### Daily CSV Contract

Each acquired file should match the existing SPY adjusted daily convention as closely as possible.

Required path:

```text
data/cache/{SYMBOL}_tiingo_adjusted_daily.csv
```

Required columns:

```text
date
open
high
low
close
volume
```

Preferred columns:

```text
adjOpen
adjHigh
adjLow
adjClose
adjVolume
divCash
splitFactor
```

Acceptable alternate adjusted column:

```text
adj_close
```

Only acceptable if the validation report explicitly records the schema difference and normalizes it before any replay audit consumes it.

### Timestamp Format

Preferred:

```text
YYYY-MM-DDT00:00:00.000Z
```

Acceptable:

```text
YYYY-MM-DD
```

Validation must normalize dates before coverage scoring. It must not infer intraday timestamps from daily bars.

### Source Metadata

Each acquired dataset should have a companion metadata record before it is considered valid:

```text
symbol
provider
dataset_type
frequency
source_url_or_provider_endpoint
acquired_at
requested_start_date
requested_end_date
first_date
last_date
row_count
file_path
sha256
adjustment_mode
known_limitations
```

Recommended metadata path:

```text
data/cache/{SYMBOL}_tiingo_adjusted_daily.metadata.json
```

This metadata is an ingestion-control artifact only. It must not mutate candidate, replay, qualification, or governance state.

## Validation Checks

### File Presence

For each required symbol:

- expected path exists
- file is non-empty
- file extension is `.csv`
- companion metadata exists or is queued for creation

### Schema

Required checks:

- `date` column exists
- `open`, `high`, `low`, `close`, `volume` columns exist
- adjusted fields exist or adjustment mode is explicitly marked
- no unknown provider schema is silently accepted

### Date Coverage

Required checks:

- first date is on or before `2021-06-05`
- last date is near current available completed market date
- dates are strictly increasing
- no duplicate dates
- no future dates

### OHLCV Integrity

Required checks:

- `high >= open`
- `high >= close`
- `high >= low`
- `low <= open`
- `low <= close`
- `volume >= 0`
- no null OHLC values
- no negative prices

### Adjustment Integrity

Required checks:

- split factor is positive when present
- dividend field is numeric when present
- adjusted close is positive when present
- adjustment fields are internally consistent enough for research replay

### Row Count Reasonableness

Expected:

```text
about 1,200-1,300 rows per symbol
```

Warnings:

- `< 1,000` rows: insufficient or partial history
- `> 1,500` rows: check for duplicates or non-trading-day bars

### Coverage Recalculation

After files pass validation, recompute:

- unique-symbol coverage
- candidate-symbol-pair coverage
- full-candidate coverage
- validation block rate due to symbol availability

This recalculation should be report-only.

## Storage Estimate

Daily adjusted OHLCV estimate:

| Item | Estimate |
| --- | ---: |
| rows per symbol | `~1,260` |
| row size | `120-180 bytes` |
| uncompressed CSV per symbol | `~0.15-0.25 MB` |
| eight symbols | `~1.2-2.0 MB` |
| eight metadata files | `<0.1 MB` |
| total minimum dataset | `<2.5 MB` |

Storage is not a constraint for the minimum dataset.

Deferred intraday storage is larger and should not be part of this minimum 75% execution target.

## Coverage Gain By Priority Order

Coverage denominator:

```text
32 candidate-symbol pairs
```

| Step | Add Symbol | New Pairs | Cumulative Covered Pairs | Candidate-Symbol Coverage | Unique-Symbol Coverage |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | existing `SPY` | 2 | 2 | 6.25% | 6.67% |
| 1 | `DIA` | 3 | 5 | 15.63% | 13.33% |
| 2 | `QQQ` | 3 | 8 | 25.00% | 20.00% |
| 3 | `BAC` | 3 | 11 | 34.38% | 26.67% |
| 4 | `META` | 3 | 14 | 43.75% | 33.33% |
| 5 | `MSFT` | 3 | 17 | 53.13% | 40.00% |
| 6 | `TSLA` | 3 | 20 | 62.50% | 46.67% |
| 7 | `AMZN` | 2 | 22 | 68.75% | 53.33% |
| 8 | `NFLX` | 2 | 24 | 75.00% | 60.00% |

## Candidate Unlock Estimate

| Acquisition Point | Newly Fully Covered Candidate Universes | Cumulative Full Candidates | Expected Validation Effect |
| --- | ---: | ---: | --- |
| `DIA` only | 0 | 0/8 | Partial ETF coverage; QQQ still blocks DIA/QQQ/SPY universes. |
| `DIA` + `QQQ` | 3 | 3/8 | Unlocks the top ETF cluster: two BREAKOUT/CHOP candidates and one MEAN_REVERSION/TRENDING comparator. |
| Add `BAC`, `META`, `MSFT`, `TSLA` | 0 | 3/8 | Strong equity cluster progress, but AMZN/NFLX/GOOGL/AAPL/JPM still block full single-stock universes. |
| Add `AMZN`, `NFLX` | 1 | 4/8 | Reaches 75% candidate-symbol coverage and fully covers the AMZN/BAC/META/MSFT/NFLX/TSLA event-reaction candidate. |

Expected validation gain:

- symbol availability block rate can fall from `100%` to roughly `50%` by full-candidate coverage.
- candidate-symbol coverage reaches the requested `75%`.
- direct replay may still produce insufficient samples if trigger/regime filters are sparse.
- intraday mismatch remains unresolved for candidates whose source timeframe is `5m`, `30m`, or `1h`.

## Deferred Intraday Requirements

The following intraday files are required later for rule-specific validation but are not part of the minimum 75% daily dataset:

| Symbol | Deferred Frequencies |
| --- | --- |
| `DIA` | `30m`, `5m` |
| `QQQ` | `30m`, `5m` |
| `BAC` | `1h`, `30m` |
| `META` | `1h`, `30m` |
| `MSFT` | `1h`, `30m` |
| `TSLA` | `1h`, `30m` |
| `AMZN` | `1h`, `30m` |
| `NFLX` | `30m` |

Intraday acquisition should follow only after daily files pass validation and coverage reporting confirms the 75% candidate-symbol target.

## Execution Checklist For A Future Data Pull

This checklist is intentionally non-executed in this session.

1. Confirm provider credentials and licensing for historical daily OHLCV.
2. Confirm target paths under `data/cache/`.
3. Pull daily adjusted OHLCV for `DIA`.
4. Validate `DIA` file and metadata.
5. Repeat for `QQQ`, `BAC`, `META`, `MSFT`, `TSLA`, `AMZN`, `NFLX`.
6. Recompute coverage after each symbol.
7. Stop if schema or adjustment mode diverges from the SPY baseline without explicit metadata.
8. Produce a report-only coverage update.
9. Run direct validation only in report/audit mode.
10. Confirm no candidate, replay, qualification, governance, or production state changed.

## Acceptance Criteria

The minimum acquisition is complete only when:

- all eight required daily files exist
- all eight files pass schema checks
- all eight files pass OHLCV integrity checks
- all eight files cover the required date range
- coverage report shows `24/32 = 75.00%` candidate-symbol coverage
- no candidate state changes were made
- no replay state changes were made
- no qualification state changes were made
- no production changes were made

## Non-Actions

This execution plan does not:

- download data
- call a provider API
- write market data files
- normalize market data
- run replay
- change candidate status
- change qualification status
- change governance state
- change production state

## Authority Boundary

This plan has no authority to:

- recommend trades
- allocate capital
- size positions
- construct portfolios
- place paper trades
- broker execution
- authorize live trading
- promote candidates
- override replay
- override qualification
- override governance

It is an execution plan for future human-approved market data acquisition only.

