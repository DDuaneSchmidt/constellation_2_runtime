# Atlas Direct Replay Data Strategy 001

Date: 2026-06-05
Status: Design only

Scope: defines a direct replay data strategy for Atlas candidate validation. This document does not implement software, download data, create integrations, alter replay state, change qualification, create candidates, approve paper-forward activity, recommend trades, allocate capital, size positions, or authorize broker execution.

## Purpose

Atlas currently has a recurring evidence gap: replay and backtest support often depends on broad proxy data, especially SPY daily bars, while many mechanisms are candidate-specific and intraday by design.

Direct replay data should replace proxy inference with source data that matches:

- candidate symbol
- mechanism
- timeframe
- regime
- session
- event context
- replay timestamp discipline

The goal is candidate validation coverage, not trading execution.

## Current Evidence Problem

Current reports show:

- final qualification evaluated 600 candidates
- 600 of 600 candidates carried a proxy penalty
- 180 candidates carried intraday/daily mismatch penalties
- 156 candidates carried sample-size penalties
- only 110 of 228 backtest-supported candidates passed final qualification
- final ranking states the biggest remaining risk is dependence on SPY daily proxy evidence until candidate-specific data is supplied

Direct replay data should target those blockers first.

## What Datasets Are Required?

### Required Core Datasets

| Dataset | Required Fields | Purpose |
| --- | --- | --- |
| Daily adjusted OHLCV | date, open, high, low, close, adjusted close, volume, split/dividend adjustments | Long-horizon baseline replay, regime labeling, gap context, daily mechanisms. |
| Intraday OHLCV | timestamp, open, high, low, close, volume, session flag | Opening range, VWAP, session timing, intraday breakout, reversal, liquidity sweep, volatility expansion. |
| Symbol master | ticker, exchange, asset type, active dates, corporate action mapping | Prevents symbol drift, delist/survivorship bias, and invalid candidate-symbol references. |
| Corporate actions | splits, dividends, symbol changes, effective dates | Ensures replay uses point-in-time adjusted or explicitly raw prices. |
| Trading calendar | session open/close, holidays, half-days, timezone | Required for opening range, session timing, and event-window boundaries. |
| Regime labels | timestamp/date, trend, volatility, liquidity, breadth, gap state | Allows mechanism evaluation by context instead of undifferentiated samples. |
| Event calendar | event type, timestamp, expected/actual when available, source lineage | Required for event reaction and macro/catalyst-sensitive mechanisms. |
| Benchmark/sector references | SPY, QQQ, IWM, DIA, sector ETFs, rates/commodity proxies | Relative strength, beta control, and proxy-dependence diagnostics. |

### Optional Later Datasets

| Dataset | Why Later |
| --- | --- |
| NBBO or quote data | Useful for true liquidity-sweep validation, but heavy and not required for first direct replay coverage. |
| Trade prints | Useful for microstructure studies, but expensive and storage-heavy. |
| Fundamental/event surprise datasets | Needed for richer event reaction tests, but can follow timestamped event calendar basics. |
| News or filing text | Useful for catalyst classification, but higher lineage and licensing burden. |

## What Symbols Appear Most Often?

The structured observation expansion artifacts use a 20-symbol universe. In the 5,000-observation profile, the most frequent symbols are:

| Symbol | Count |
| --- | ---: |
| SPY | 273 |
| BAC | 273 |
| AMD | 273 |
| USO | 273 |
| GOOGL | 273 |
| NFLX | 273 |
| NVDA | 273 |
| IWM | 272 |
| TLT | 272 |
| AAPL | 272 |
| TSLA | 228 |
| QQQ | 228 |
| DIA | 228 |
| DBC | 228 |
| XLE | 227 |
| META | 227 |
| MSFT | 227 |
| JPM | 227 |
| GLD | 227 |
| AMZN | 226 |

Design implication: direct replay V0.1 should prioritize this 20-symbol core universe before expanding.

Suggested symbol tiers:

| Tier | Symbols | Purpose |
| --- | --- | --- |
| Tier 1 index/ETF core | SPY, QQQ, IWM, DIA | Benchmark, regime, broad-market, and high-coverage replay. |
| Tier 2 sector/macro ETFs | XLE, TLT, GLD, USO, DBC | Event, rates, commodity, volatility, and sector context. |
| Tier 3 mega-cap equities | AAPL, MSFT, NVDA, AMZN, GOOGL, META, TSLA, NFLX, AMD | Candidate-specific equity mechanism validation. |
| Tier 4 liquid financials | JPM, BAC | Bank/financial sector behavior and liquidity-sensitive tests. |

## What Timeframes Appear Most Often?

The observation expansion profile is intentionally balanced:

| Timeframe | Count In 5,000 Profile |
| --- | ---: |
| 5m | 1,251 |
| 15m | 1,251 |
| 30m | 1,250 |
| 1h | 1,248 |

Current backtests also use daily adjusted OHLCV as a local proxy. Direct replay should therefore support:

- `1d` for long-horizon replay, gaps, broad regimes, and baseline controls
- `1h` for higher intraday context
- `30m` for session phase and event-window behavior
- `15m` for opening/midday/afternoon mechanisms
- `5m` for opening range, VWAP reclaim, liquidity sweep, and precise trigger reconstruction

Design priority:

1. Daily adjusted OHLCV for the full 20-symbol universe.
2. 15m and 30m intraday for all 20 symbols.
3. 5m intraday for the highest-frequency mechanisms and top candidate symbols.
4. 1h derived from intraday bars or stored directly if provider lineage requires it.

## Mechanisms To Cover

Observation expansion is balanced across 10 mechanisms:

| Mechanism | Count In 5,000 Profile | Direct Data Need |
| --- | ---: | --- |
| BREAKOUT | 500 | intraday and daily OHLCV, volume, session boundaries |
| MEAN_REVERSION | 500 | daily and intraday OHLCV, volatility/regime labels |
| OPENING_RANGE | 500 | 5m/15m bars, session calendar |
| SESSION_TIMING | 500 | intraday bars, session labels, holiday/half-day calendar |
| VWAP_OR_AVERAGE_RECLAIM | 500 | intraday OHLCV/volume, VWAP calculation support |
| VOLATILITY_EXPANSION | 500 | daily and intraday range/ATR/realized volatility |
| LIQUIDITY_SWEEP | 500 | 5m/15m bars at minimum; later quote/trade data |
| TREND_CONTINUATION | 500 | daily and intraday trend context |
| REVERSAL | 500 | intraday and daily OHLCV, predeclared reference levels |
| EVENT_REACTION | 500 | event calendar plus timestamped price data |

## What Minimum History Is Needed?

Minimum history depends on mechanism and timeframe.

| Use Case | Minimum Useful History | Preferred History |
| --- | ---: | ---: |
| Daily mechanism replay | 5 years | 10+ years |
| Regime labeling | 5 years | 10+ years |
| Intraday 1h/30m replay | 2 years | 5 years |
| Intraday 15m replay | 2 years | 5 years |
| Intraday 5m replay | 1 year | 3-5 years |
| Event reaction | 3 years of events | 5+ years of events |
| Candidate-specific backtest support | 100+ samples or 2 years | 250+ samples or 5 years |

Minimum sample rules:

- exploratory replay: at least 30 independent samples
- candidate validation: at least 100 independent samples where feasible
- robust mechanism-family validation: at least 250 independent samples across regimes
- event mechanisms: fewer samples may be acceptable only if event metadata is strong and controls are explicit

## What Storage Requirements Exist?

Design assumptions:

- 20-symbol V0.1 universe
- 5 years intraday history preferred
- OHLCV bars stored columnar, partitioned by symbol/timeframe/year
- raw source files retained separately from normalized replay tables

Approximate storage:

| Dataset | Estimated Size For 20 Symbols | Notes |
| --- | ---: | --- |
| Daily OHLCV, 10+ years | < 100 MB | Small; include all core symbols and benchmarks. |
| 1h bars, 5 years | < 1 GB | Can be derived from lower intraday bars if lineage is preserved. |
| 30m bars, 5 years | 1-2 GB | Useful middle layer for session/event replay. |
| 15m bars, 5 years | 2-4 GB | Good V0.1 intraday coverage baseline. |
| 5m bars, 3-5 years | 5-15 GB | Core for opening range, VWAP, sweep, precise triggers. |
| Event calendar | < 1 GB | Usually small, but lineage metadata matters. |
| Raw + normalized + indexes | 2x-3x normalized size | Keep raw immutable source plus normalized replay-ready tables. |

Storage layout recommendation:

```text
data/atlas_direct_replay/
  raw/{provider}/{dataset}/{symbol}/{year}/...
  normalized/bars/{timeframe}/symbol={SYMBOL}/year={YYYY}/...
  normalized/events/event_type={TYPE}/year={YYYY}/...
  manifests/{dataset_version}.json
  validation/{dataset_version}/...
```

No implementation is authorized by this document.

## What Validation Requirements Exist?

### Dataset Validation

Required checks:

- symbol exists in symbol master for every row
- timestamps are timezone-normalized
- session labels match trading calendar
- no duplicate bars for `(symbol, timeframe, timestamp)`
- no negative prices or volume
- OHLC invariants hold: high >= open/close/low and low <= open/close/high
- adjusted and raw price modes are explicitly labeled
- corporate-action adjustment method is documented
- missing bars are counted by symbol, session, and timeframe
- source provider and extraction timestamp are recorded
- checksum or content hash exists for raw and normalized partitions

### Replay Validation

Required checks:

- replay uses only data available at or before the evaluated timestamp
- mechanism trigger can be reconstructed from stored fields
- regime label is point-in-time safe
- session and event windows are deterministic
- candidate symbol is not silently replaced by proxy symbol
- proxy fallback, if any, is explicitly marked and penalized
- minimum sample threshold is computed before eligibility claims

### Coverage Validation

Required coverage fields:

```text
symbol
timeframe
start_timestamp
end_timestamp
bar_count
expected_bar_count
coverage_ratio
missing_session_count
corporate_action_coverage
event_calendar_coverage
validation_status
known_limitations
```

Validation bands:

| Coverage Ratio | Status |
| ---: | --- |
| `>= 0.98` | COMPLETE |
| `0.95-0.979` | USABLE_WITH_GAPS |
| `0.85-0.949` | RESEARCH_ONLY_LIMITED |
| `< 0.85` | INSUFFICIENT |

## Candidate Validation Unlock Levels

Coverage unlocks should be based on candidate population share that can be tested with candidate-specific data, correct timeframe, sufficient history, and validated lineage.

### 25% Candidate Validation Unlock

Required coverage:

- 20 core symbols have daily adjusted OHLCV for 10+ years
- top 4 symbols by candidate demand have 5m/15m/30m intraday for 2+ years
- SPY, QQQ, IWM, DIA have 5m/15m/30m intraday for 3+ years
- trading calendar and symbol master are complete
- replay engine can reject proxy substitution when candidate-specific data is missing

Likely unlocks:

- daily mechanisms
- benchmark-relative checks
- early direct validation of top candidate symbols
- some breakout, mean reversion, trend continuation, and reversal candidates

Still blocked:

- broad intraday mechanism validation
- event reaction validation without event calendar
- liquidity sweep precision beyond bar-level approximation

### 50% Candidate Validation Unlock

Required coverage:

- 20 core symbols have 15m/30m/1h intraday for 3+ years
- top 10 symbols have 5m intraday for 2+ years
- event calendar covers scheduled macro and major earnings dates for 3+ years
- regime labels are generated from validated point-in-time data
- minimum 100-sample feasibility can be computed per mechanism/regime

Likely unlocks:

- most trend, breakout, reversal, VWAP/average reclaim, and volatility expansion candidates
- first useful event-reaction validation
- better separation between candidate-specific evidence and benchmark proxy behavior

Still blocked:

- long-horizon intraday robustness
- lower-frequency symbols with sparse samples
- quote-level liquidity sweep tests

### 75% Candidate Validation Unlock

Required coverage:

- all 20 core symbols have 5m/15m/30m/1h intraday for 5 years
- daily adjusted OHLCV and corporate actions cover 10+ years
- event calendar has source lineage and timestamp discipline
- sector/macro ETFs are available for benchmark and relative context
- validation manifests show `COMPLETE` or `USABLE_WITH_GAPS` for most partitions
- mechanism-level sample independence checks exist as validation criteria

Likely unlocks:

- most current Atlas mechanism families
- stronger paper-forward candidate filtering
- direct replay for top, near-threshold, and rejected-monitor candidates
- regime-conditioned replay across CHOP, TRENDING, HIGH_VOLATILITY, LOW_VOLATILITY, and UNKNOWN cleanup

Still blocked:

- true microstructure claims
- quote/trade-level liquidity sweeps
- rare event regimes with limited event count

### 90% Candidate Validation Unlock

Required coverage:

- 20 core symbols plus expansion candidates have 5m/15m/30m/1h intraday for 5+ years
- selected high-priority symbols have quote or trade data for liquidity-sensitive studies
- complete symbol master with delisting and corporate-action lineage
- event calendar includes scheduled and major unscheduled catalyst tagging
- validation manifests are versioned and replay reproducible
- direct replay can measure proxy-vs-candidate divergence explicitly
- coverage reports are linked to every candidate validation result

Likely unlocks:

- nearly all non-microstructure Atlas candidates
- robust candidate-specific replay and backtest comparison
- direct validation for rejected candidates worth monitoring
- high-confidence detection of proxy dependence, timeframe mismatch, and sample-size insufficiency

Still blocked:

- claims requiring unavailable proprietary feeds
- claims depending on order book dynamics without quote depth
- claims whose mechanism is not objectively reconstructable

## Direct Replay Coverage Formula

Suggested coverage score per candidate:

```text
DirectReplayCoverage(candidate) =
  0.30 * SymbolCoverage
+ 0.25 * TimeframeCoverage
+ 0.20 * HistoryDepthCoverage
+ 0.15 * MechanismFieldCoverage
+ 0.10 * ValidationLineageCoverage
```

Candidate is directly replayable when:

```text
DirectReplayCoverage >= 0.85
and minimum_sample_count_met == true
and proxy_substitution_required == false
and validation_status in {COMPLETE, USABLE_WITH_GAPS}
```

Candidate remains proxy-limited when:

```text
proxy_substitution_required == true
or TimeframeCoverage < required_mechanism_timeframe
or minimum_sample_count_met == false
```

## Prioritization

Data acquisition priority should be:

1. symbol master, trading calendar, daily adjusted OHLCV for 20 symbols
2. 15m/30m intraday for 20 symbols
3. 5m intraday for Tier 1 and Tier 3 high-frequency symbols
4. event calendar for macro, earnings, and major catalysts
5. full 5m history for all 20 symbols
6. quote/trade data only for specific liquidity-sweep research questions

This order targets the current biggest blockers: proxy dependency, intraday/daily mismatch, insufficient sample size, and weak event/source lineage.

## Non-Goals

- No implementation.
- No downloads.
- No integrations.
- No vendor selection.
- No live data feed.
- No broker execution.
- No trading recommendation.
- No capital allocation.
- No position sizing.
- No candidate promotion.
- No replay or qualification state changes.

## Conclusion

Direct replay data should be built around candidate-specific symbol and timeframe coverage, not a larger proxy backtest.

The first useful milestone is not maximum data breadth. It is enough validated direct coverage to remove proxy penalties from the highest-value candidate families and to prove which mechanisms survive candidate-specific replay.
