# Intraday Dependency Inventory 001

Date: 2026-06-05
Status: ANALYSIS_ONLY

## Purpose

Inventory all intraday requirements found in the current research and runtime evidence reviewed for Atlas/Aegis candidate validation.

This report is documentation only. It does not acquire data, implement replay, change validation logic, qualify candidates, promote candidates, alter governance, authorize paper placement, recommend trades, allocate capital, size positions, or authorize broker execution.

## Source Scope

Primary sources reviewed:

- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/market_data_readiness/latest.json`
- `reports/atlas_v2_research_os/market_data_readiness/required_market_data_manifest.csv`
- `reports/atlas_v2_research_os/candidate_symbol_attribution/latest.json`
- `research_journal/design/atlas_market_data_acquisition_plan_001.md`
- `research_journal/design/atlas_direct_replay_data_strategy_001.md`
- `research_journal/design/atlas_search_space_mapping_001.md`
- `research_journal/reports/event_metadata_requirements_001.md`
- `research_journal/reports/direct_validation_feasibility_study_001.md`
- `constellation_2/common/atlas_v2_research_os/market_data_readiness.py`
- `constellation_2/common/atlas_v2_research_os/mechanism_search_space.py`
- Aegis operational tests covering intraday market data, trading-day readiness, and broker supply.

## Executive Inventory

Current candidate validation requires intraday data for all 8 reviewed direct-validation candidates.

Current market-data readiness summary:

- candidates reviewed: `8`
- candidates ready for direct validation: `0`
- candidates blocked: `8`
- required symbols: `15`
- symbols with daily data: `10`
- symbols missing daily data: `5`
- intraday symbols required: `15`
- intraday symbols missing: `15`
- safe local importer exists: `false`

The required intraday timeframes are:

- `1h`
- `30m`
- `15m`
- `5m`

Required historical window:

- start: `2021-06-05`
- end: `2026-06-05`
- preferred depth: 5 years
- provisional minimum: 2 years for paper-forward research context only

## Candidate-Level Intraday Requirements

| candidate_id | mechanism | regime | intraday timeframe | symbols | current blocker |
| --- | --- | --- | --- | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | `BREAKOUT` | `CHOP` | `30m` | DIA, QQQ, SPY | missing 30m CSVs for DIA, QQQ, SPY |
| `ptc_backtest_final_3a4ac24107c77136` | `BREAKOUT` | `CHOP` | `5m` | DIA, QQQ, SPY | missing 5m CSVs for DIA, QQQ, SPY |
| `ptc_backtest_final_624fdd85668e2c08` | `MEAN_REVERSION` | `TRENDING` | `30m` | DBC, TLT, USO | missing daily CSVs and 30m CSVs for DBC, TLT, USO |
| `ptc_backtest_final_854ad10b904e1ae9` | `MEAN_REVERSION` | `TRENDING` | `30m` | DIA, QQQ | missing 30m CSVs for DIA, QQQ |
| `ptc_backtest_final_d5931b24bd391113` | `EVENT_REACTION` | `CHOP` | `30m` | AMZN, BAC, META, MSFT, NFLX, TSLA | missing 30m CSVs and event metadata |
| `ptc_backtest_final_4df2e8e80685a054` | `REVERSAL` | `TRENDING` | `1h` | AAPL, AMZN, BAC, JPM, META, MSFT, TSLA | missing daily CSVs for AAPL/JPM and 1h CSVs for all symbols |
| `ptc_backtest_final_7d839944a8a4070a` | `BREAKOUT` | `CHOP` | `15m` | TLT, USO | missing daily CSVs and 15m CSVs for TLT, USO |
| `ptc_backtest_final_b23c6756bfb3263a` | `BREAKOUT` | `CHOP` | `30m` | BAC, GOOGL, META, MSFT, NFLX, TSLA | missing 30m CSVs for all symbols |

## Symbol-Timeframe Inventory

Required intraday CSVs from the current manifest:

| symbol | required intraday timeframes | required candidates | current status |
| --- | --- | --- | --- |
| AAPL | `1h` | `ptc_backtest_final_4df2e8e80685a054` | missing |
| AMZN | `1h`, `30m` | `ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_d5931b24bd391113` | missing |
| BAC | `1h`, `30m` | `ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_b23c6756bfb3263a`, `ptc_backtest_final_d5931b24bd391113` | missing |
| DBC | `30m` | `ptc_backtest_final_624fdd85668e2c08` | missing |
| DIA | `30m`, `5m` | `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_854ad10b904e1ae9` | missing |
| GOOGL | `30m` | `ptc_backtest_final_b23c6756bfb3263a` | missing |
| JPM | `1h` | `ptc_backtest_final_4df2e8e80685a054` | missing |
| META | `1h`, `30m` | `ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_b23c6756bfb3263a`, `ptc_backtest_final_d5931b24bd391113` | missing |
| MSFT | `1h`, `30m` | `ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_b23c6756bfb3263a`, `ptc_backtest_final_d5931b24bd391113` | missing |
| NFLX | `30m` | `ptc_backtest_final_b23c6756bfb3263a`, `ptc_backtest_final_d5931b24bd391113` | missing |
| QQQ | `30m`, `5m` | `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_854ad10b904e1ae9` | missing |
| SPY | `30m`, `5m` | `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136` | missing |
| TLT | `15m`, `30m` | `ptc_backtest_final_624fdd85668e2c08`, `ptc_backtest_final_7d839944a8a4070a` | missing |
| TSLA | `1h`, `30m` | `ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_b23c6756bfb3263a`, `ptc_backtest_final_d5931b24bd391113` | missing |
| USO | `15m`, `30m` | `ptc_backtest_final_624fdd85668e2c08`, `ptc_backtest_final_7d839944a8a4070a` | missing |

Expected file layout:

```text
data/historical/{SYMBOL}_1h.csv
data/historical/{SYMBOL}_30m.csv
data/historical/{SYMBOL}_15m.csv
data/historical/{SYMBOL}_5m.csv
```

The readiness code also searches fallback paths:

```text
data/cache/{SYMBOL}_{TIMEFRAME}.csv
data/historical/{SYMBOL}_{TIMEFRAME}.csv
data/{SYMBOL}_{TIMEFRAME}.csv
```

## Timeframe-Specific Requirements

### `1h`

Required symbols:

- AAPL
- AMZN
- BAC
- JPM
- META
- MSFT
- TSLA

Primary candidate use:

- reversal validation
- higher intraday context
- equity basket validation
- trend/reversal structure checks

Minimum row-count expectation for 5 years:

- about `8,000-9,000` rows per symbol

### `30m`

Required symbols:

- AMZN
- BAC
- DBC
- DIA
- GOOGL
- META
- MSFT
- NFLX
- QQQ
- SPY
- TLT
- TSLA
- USO

Primary candidate use:

- breakout confirmation
- mean-reversion intraday confirmation
- event-window reaction alignment
- session/event replay
- ETF and single-stock basket validation

Minimum row-count expectation for 5 years:

- about `15,000-17,000` rows per symbol

### `15m`

Required symbols:

- TLT
- USO

Primary candidate use:

- faster breakout validation
- rates/commodity ETF intraday pattern validation
- opening/midday/afternoon mechanism support in broader search-space design

Minimum row-count expectation for 5 years:

- about `30,000-34,000` rows per symbol

### `5m`

Required symbols:

- DIA
- QQQ
- SPY

Primary candidate use:

- high-frequency breakout confirmation
- opening range
- VWAP/average reclaim
- liquidity sweep
- precise trigger reconstruction

Minimum row-count expectation for 5 years:

- about `90,000-100,000` rows per symbol

## Mechanism-Level Intraday Requirements

From direct replay strategy and mechanism search-space design:

| mechanism | intraday requirement | preferred/observed timeframes | additional dependencies |
| --- | --- | --- | --- |
| `BREAKOUT` | intraday and daily OHLCV, volume, session boundaries | `15m`, `1h`, `1d`; current candidates use `5m`, `15m`, `30m` | range high/low, close location, volume, accepted boundary |
| `MEAN_REVERSION` | daily and intraday OHLCV, volatility/regime labels | `5m`, `15m`, `1d`; current candidates use `30m` | rolling mean, distance from mean, momentum slope, volume |
| `OPENING_RANGE` | 5m/15m bars and session calendar | `1m`, `5m`, `15m` | opening range high/low, gap, opening volume, session open time |
| `SESSION_TIMING` | intraday bars, session labels, holiday/half-day calendar | `5m`, `15m`, `session` | session time, range progression, volume curve |
| `VWAP_OR_AVERAGE_RECLAIM` | intraday OHLCV/volume and VWAP calculation support | `5m`, `15m`, `1h` | VWAP or moving average, price acceptance, volume |
| `VOLATILITY_EXPANSION` | daily and intraday range/ATR/realized volatility | `15m`, `1h`, `1d` | realized volatility, true range, participation expansion |
| `LIQUIDITY_SWEEP` | 5m/15m bars at minimum; quote/trade data later | `1m`, `5m`, `15m` | swing levels, wick rejection, close location, volume |
| `TREND_CONTINUATION` | daily and intraday trend context | `15m`, `1h`, `1d` | trend structure, retracement depth, moving average, volume |
| `REVERSAL` | intraday and daily OHLCV, predeclared reference levels | `5m`, `15m`, `1h`; current candidate uses `1h` | failed break, structure reversal, momentum slope, volume |
| `EVENT_REACTION` | event calendar plus timestamped intraday price data | `5m`, `15m`, `1d`; current candidate requires `30m` | event timestamp, initial reaction, volatility, volume, source lineage |

## Event-Reaction Specific Intraday Requirements

For `ptc_backtest_final_d5931b24bd391113`, daily data is not enough. Required intraday/event-aware replay pieces:

- 30m bars for AMZN, BAC, META, MSFT, NFLX, and TSLA.
- Timestamped event metadata for each symbol over the same tested range.
- Event timestamp in UTC and America/New_York.
- Session bucket: pre-market, regular session, after-hours, weekend/holiday, or unknown.
- Source publication timestamp.
- Effective reaction anchor timestamp if different from publication time.
- First eligible 30m bar open after the event.
- Reaction window start and end.
- Pre-event baseline window.
- Post-event reaction window.
- Event eligibility filter.
- Event-type stratification.
- Non-event control windows.
- Exclusion reasons for date-only, ambiguous, duplicate, or non-symbol-specific events.

Daily OHLCV cannot prove:

- whether the move began before or after the event timestamp
- whether the move occurred inside the intended 30m window
- whether pre-market/after-hours events map to same day or next session
- whether the move is event reaction rather than ordinary daily volatility
- whether `CHOP` regime was known before the reaction

## Dataset Validation Requirements

Every intraday dataset must pass these checks before it is usable for validation:

- symbol exists in symbol master
- timestamp is timezone-normalized
- timestamp has explicit session date
- session labels match trading calendar
- no duplicate bars for `(symbol, timeframe, timestamp)`
- OHLCV fields exist
- OHLC invariants hold
- no negative prices or volume
- raw versus adjusted price mode is explicit
- corporate-action adjustment method is documented
- gaps, half-days, holidays, and early closes are detected
- bar count is reasonable for timeframe and date range
- source lineage and provider are recorded
- raw source files remain distinguishable from normalized replay-ready tables

## Trading Calendar And Session Dependencies

Intraday validation depends on a trading calendar with:

- regular session open and close
- pre-market and after-hours classification where used
- holidays
- half-days
- timezone rules for America/New_York
- session labels such as opening, midday, closing, and after-hours

These are required for:

- opening range
- session timing
- event-window boundaries
- pre-market/after-hours event alignment
- same-session versus next-session reaction classification
- intraday gap detection

## Regime Dependencies

Intraday validation also depends on timestamped or bar-aligned regime labels:

- trend state
- volatility state
- liquidity state
- breadth or benchmark context where applicable
- gap state
- event-window state

For `CHOP`, current daily validation can only approximate through `RANGE_BOUND` or related labels. Intraday chop claims require intraday regime definitions and cannot be proven from daily bars alone.

## Operational Aegis Intraday Dependencies

Separate from research validation, Aegis operational intraday readiness has its own requirements.

### Intraday Market Data Mode

Existing tests require:

- `AEGIS_MARKET_DATA_MODE=INTRADAY_OPERATIONAL`
- `--market-data-mode INTRADAY_OPERATIONAL`
- provisional intraday data may be visible for candidate generation
- provisional intraday data is labeled `NON_CERTIFIED`
- provisional intraday data is usable for current sleeve candidate generation
- provisional intraday data is not usable for execution candidate generation
- final EOD certification remains separate and pending

Configured intraday refresh times in tests:

- 09:35 America/New_York
- 11:55 America/New_York
- 15:45 America/New_York
- 16:10 America/New_York

Final EOD certification is separate:

- 16:30 America/New_York
- 17:00 America/New_York
- 18:00 America/New_York
- 08:15 America/New_York

### Trading-Day Readiness

Intraday submit-ready mode requires:

- current day target
- regular session open
- same-day broker event log
- same-day options snapshot
- live account truth

Future target days use pre-open build mode and do not permit future broker-event evidence.

After-hours closure does not require market-open quotes and does not permit submit by mode.

### Broker Supply

Intraday target-day broker supply requires same-day broker event log. A prior-day broker event log may be carried forward for future-day pre-open build, but it is not sufficient for intraday submit readiness.

If only prior-day broker event evidence is present during intraday mode, broker supply blocks with:

```text
BROKER_EVENT_LOG_MISSING
```

## Acquisition Priority

Existing acquisition plan recommends daily coverage first, then intraday in this order:

1. DIA, QQQ, SPY at `5m` and `30m` for ETF breakout/opening-range style validation.
2. BAC, META, MSFT, TSLA, AMZN at `1h` and `30m` for equity/event validation.
3. NFLX, GOOGL, DBC at `30m`.
4. TLT, USO at `15m` and `30m`.
5. AAPL, JPM at `1h`.

This order targets the current candidate queue, not live execution.

## Current Blocking Summary

Research validation blockers:

- all 15 intraday-required symbols are missing intraday files
- 5 symbols are still missing daily files in the market-data readiness report
- event-reaction validation lacks timestamped event metadata
- `CHOP` validation lacks intraday regime semantics and vocabulary alignment
- no safe local importer exists in the market-data readiness report

Operational blockers:

- runtime truth has recently been `PARTIAL_CONTEXT` / `BLOCKED` in verified graph and truth-kernel outputs
- operational intraday readiness requires same-day broker/options/account evidence
- provisional intraday market data is not execution-certified

## Non-Requirements

Intraday data availability does not by itself provide:

- replay authority
- qualification authority
- candidate authority
- governance authority
- paper placement authority
- trade advice
- broker execution authority
- capital allocation authority
- position sizing authority
- live execution readiness

## Authority Boundary

This inventory is analysis-only. It does not implement data ingestion, download market data, create event metadata, run replay, change validation, qualify candidates, promote candidates, create paper observations, modify governance, recommend trades, allocate capital, authorize broker execution, size positions, or alter safety gates.

