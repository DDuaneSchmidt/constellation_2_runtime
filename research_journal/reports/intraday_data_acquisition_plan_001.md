# Intraday Data Acquisition Plan 001

Date: 2026-06-05

Status: ANALYSIS_ONLY

## Scope

Prepare minimum intraday data requirements for the 4 `HIGH_VALUE_INTRADAY` candidates.

Inputs reviewed:

- `research_journal/reports/intraday_event_evidence_readiness_pack_001.md`
- `reports/atlas_v2_research_os/candidate_validation_queue_001.md`
- `research_journal/reports/insufficient_data_root_cause_analysis_001.md`

This plan does not download data, implement ingestion, run replay, change validation, change qualification, change candidate state, change governance, create paper observations, recommend trades, allocate capital, authorize broker execution, or size positions.

## Planning Assumptions

Date range:

- start date: `2021-06-07`
- end date: `2026-06-05`

Reason:

The current market-data acquisition artifacts use a five-year daily range ending 2026-06-05. The intraday readiness pack estimates rows over the same five-year planning horizon.

Required common fields:

- `timestamp`
- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `bar_interval`
- `timezone`
- `session`
- `adjustment_mode`
- `source`
- `source_version` or `source_hash`

Common quality requirements:

- timestamps normalized to exchange time and UTC-compatible
- no duplicate `(symbol, timestamp, bar_interval)` rows
- no negative OHLCV values
- `high >= max(open, close)` and `low <= min(open, close)`
- explicit regular-session versus extended-hours policy
- explicit split/dividend adjustment mode
- symbol-level row counts reported before replay use

## Candidate Requirements

| Candidate ID | Symbols | Timeframe | Start Date | End Date | Required Bar Interval | Estimated Rows | Minimum Viable Dataset | Validation Target | Expected Candidate Impact |
| --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | DIA, QQQ, SPY | `30m` | 2021-06-07 | 2026-06-05 | 30-minute OHLCV | 45,000-51,000 | 30m bars for all 3 symbols, regular-session labeled, with enough coverage to reconstruct the attributed breakout/chop trigger and report post-filter samples by symbol. | Test whether the top-ranked `BREAKOUT / CHOP / 30m` candidate survives direct intraday ETF-universe replay instead of daily proxy filtering. | HIGH: resolves priority 1 queue candidate and tests whether the strongest SPY-proxy result survives direct DIA/QQQ/SPY evidence. |
| `ptc_backtest_final_3a4ac24107c77136` | DIA, QQQ, SPY | `5m` | 2021-06-07 | 2026-06-05 | 5-minute OHLCV | 270,000-300,000 | 5m bars for all 3 symbols, regular-session labeled, with enough coverage to reconstruct the attributed breakout/chop trigger without daily-bar proxy substitution. | Test the same high-leverage ETF breakout/chop cluster at 5m granularity and compare sample survival against the 30m sibling candidate. | HIGH: resolves priority 2 queue candidate and tests timeframe sensitivity using the same DIA/QQQ/SPY symbol cluster. |
| `ptc_backtest_final_854ad10b904e1ae9` | DIA, QQQ | `30m` | 2021-06-07 | 2026-06-05 | 30-minute OHLCV | 30,000-34,000 | 30m bars for DIA and QQQ, regular-session labeled, with enough coverage to test mean-reversion in TRENDING regimes and produce symbol-level plus aggregate sample counts. | Convert the `MEAN_REVERSION / TRENDING / 30m` comparator from daily proxy evidence into direct intraday ETF evidence. | HIGH: reuses the DIA/QQQ acquisition lane while testing a different mechanism/regime pair from the top breakout candidates. |
| `ptc_backtest_final_b23c6756bfb3263a` | BAC, GOOGL, META, MSFT, NFLX, TSLA | `30m` | 2021-06-07 | 2026-06-05 | 30-minute OHLCV | 90,000-102,000 | 30m bars for all 6 symbols, regular-session labeled, with full symbol-universe coverage and per-symbol contribution reporting before aggregate candidate interpretation. | Test whether the single-stock `BREAKOUT / CHOP / 30m` candidate produces enough direct intraday samples after trigger and regime filtering. | MEDIUM-HIGH: full daily coverage now exists, but daily proxy replay still produced zero usable samples; intraday data is the minimum evidence needed to separate real failure from proxy/replay limitation. |

## Per-Candidate Details

### `ptc_backtest_final_469607b8340421b7`

| Field | Requirement |
| --- | --- |
| candidate_id | `ptc_backtest_final_469607b8340421b7` |
| symbols | DIA, QQQ, SPY |
| timeframe | `30m` |
| start/end date | `2021-06-07` to `2026-06-05` |
| required bar interval | 30-minute OHLCV |
| estimated rows | 45,000-51,000 total |
| required fields | timestamp, symbol, open, high, low, close, volume, bar_interval, timezone, session, adjustment_mode, source, source_version/source_hash |
| minimum viable dataset | Complete 30m regular-session OHLCV for DIA, QQQ, and SPY with lineage and no duplicate bars. |
| validation target | Directly test 30m breakout behavior in CHOP context and report raw bars, trigger events, regime-matched events, and final scored samples. |
| expected candidate impact | High-value unblock for the top campaign and robust candidate; determines whether daily proxy support survives direct intraday ETF replay. |

Root-cause basis:

- daily coverage exists
- direct replay attempted
- direct result still had sample size 0 after 125 triggered samples were filtered
- daily data alone does not resolve the candidate

### `ptc_backtest_final_3a4ac24107c77136`

| Field | Requirement |
| --- | --- |
| candidate_id | `ptc_backtest_final_3a4ac24107c77136` |
| symbols | DIA, QQQ, SPY |
| timeframe | `5m` |
| start/end date | `2021-06-07` to `2026-06-05` |
| required bar interval | 5-minute OHLCV |
| estimated rows | 270,000-300,000 total |
| required fields | timestamp, symbol, open, high, low, close, volume, bar_interval, timezone, session, adjustment_mode, source, source_version/source_hash |
| minimum viable dataset | Complete 5m regular-session OHLCV for DIA, QQQ, and SPY with lineage and no duplicate bars. |
| validation target | Directly test 5m breakout behavior in CHOP context and compare sample survival to the 30m DIA/QQQ/SPY sibling candidate. |
| expected candidate impact | High-value unblock for priority 2; tests whether the proxy-supported thesis is timeframe-sensitive or an artifact of daily proxy logic. |

Root-cause basis:

- daily coverage exists
- direct replay attempted
- direct result still had sample size 0 after 125 triggered samples were filtered
- daily-bar proxy cannot represent a 5m breakout mechanism cleanly

### `ptc_backtest_final_854ad10b904e1ae9`

| Field | Requirement |
| --- | --- |
| candidate_id | `ptc_backtest_final_854ad10b904e1ae9` |
| symbols | DIA, QQQ |
| timeframe | `30m` |
| start/end date | `2021-06-07` to `2026-06-05` |
| required bar interval | 30-minute OHLCV |
| estimated rows | 30,000-34,000 total |
| required fields | timestamp, symbol, open, high, low, close, volume, bar_interval, timezone, session, adjustment_mode, source, source_version/source_hash |
| minimum viable dataset | Complete 30m regular-session OHLCV for DIA and QQQ with lineage and no duplicate bars. |
| validation target | Test mean-reversion behavior in TRENDING regimes on the attributed ETF universe using direct intraday bars. |
| expected candidate impact | High-value comparator; reuses the DIA/QQQ acquisition set while testing a non-breakout mechanism and a non-CHOP regime. |

Root-cause basis:

- queue ranked it priority 3 by validation value
- readiness pack classified it `HIGH_VALUE_INTRADAY`
- same DIA/QQQ acquisition lane supports priorities 1 and 2

### `ptc_backtest_final_b23c6756bfb3263a`

| Field | Requirement |
| --- | --- |
| candidate_id | `ptc_backtest_final_b23c6756bfb3263a` |
| symbols | BAC, GOOGL, META, MSFT, NFLX, TSLA |
| timeframe | `30m` |
| start/end date | `2021-06-07` to `2026-06-05` |
| required bar interval | 30-minute OHLCV |
| estimated rows | 90,000-102,000 total |
| required fields | timestamp, symbol, open, high, low, close, volume, bar_interval, timezone, session, adjustment_mode, source, source_version/source_hash |
| minimum viable dataset | Complete 30m regular-session OHLCV for BAC, GOOGL, META, MSFT, NFLX, and TSLA with lineage, no duplicate bars, and per-symbol completeness reports. |
| validation target | Test 30m breakout behavior in CHOP context across the full single-stock universe and report symbol-level plus aggregate sample survival. |
| expected candidate impact | Medium-high value; daily coverage is complete but direct validation remained insufficient, so intraday data is needed to distinguish candidate weakness from replay/proxy limitation. |

Root-cause basis:

- full daily coverage exists after GOOGL unlock
- direct replay attempted
- direct result still had sample size 0 after 98 triggered samples were filtered
- universe aggregation must be preserved because evidence spans six stocks

## Acquisition Priority

| Priority | Dataset | Candidates Unblocked | Rationale |
| ---: | --- | --- | --- |
| 1 | DIA, QQQ, SPY at `30m` | `469607...` and partially `854ad...` through DIA/QQQ overlap | Small ETF set, highest campaign priority, 30m row count is moderate. |
| 2 | DIA, QQQ, SPY at `5m` | `3a4ac...` | Same symbols as priority 1, but much higher row count; validates timeframe sensitivity. |
| 3 | DIA, QQQ at `30m` completeness check | `854ad...` | If priority 1 provides DIA/QQQ 30m, only candidate-specific validation packaging remains. |
| 4 | BAC, GOOGL, META, MSFT, NFLX, TSLA at `30m` | `b23c...` | Higher breadth and aggregation complexity; high value after ETF lane. |

## Minimum Viable Dataset Definition

A dataset is minimum viable only if it includes:

1. all symbols for the candidate
2. the exact required bar interval
3. the full planning date range or an explicit coverage exception ledger
4. OHLCV fields plus timestamp, timezone, session, adjustment mode, and source lineage
5. duplicate-bar and missing-bar checks
6. per-symbol row counts
7. enough retained post-filter samples to evaluate the candidate, with insufficient sample size reported as a validation limitation rather than candidate failure

## Validation Targets

The intraday acquisition should answer only evidence questions:

- Do direct intraday bars produce trigger events?
- Do trigger events survive regime filtering?
- Are post-filter samples sufficient by symbol and aggregate?
- Does direct intraday behavior agree with or contradict the daily proxy result?
- Is the issue candidate weakness, proxy mismatch, regime vocabulary, trigger design, or missing data?

## Non-Actions

No downloads.
No implementation.
No replay run.
No validation classification change.
No qualification change.
No candidate promotion.
No paper-forward state change.
No governance change.
No trading recommendation.
No capital allocation.
No broker execution.
No position sizing.

## Authority Boundary

This plan is a requirements artifact only. Intraday data, if later acquired, must remain evidence-only until separate governed validation, qualification, and authority checks are completed.
