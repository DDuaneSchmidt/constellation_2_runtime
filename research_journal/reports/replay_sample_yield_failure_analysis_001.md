# Replay Sample Yield Failure Analysis 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: diagnostic comparison of global replay sample yield versus candidate-specific direct validation sample yield. No replay behavior changes, no implementation, no downloads, no candidate changes, no qualification changes, no governance changes, no paper-forward changes, no trading authority, no broker authority, no capital allocation, and no position sizing.

## Inputs

- `reports/atlas_v2_research_os/replay_sample_yield/2026-06-05/replay_sample_yield.json`
- `reports/atlas_v2_research_os/replay_sample_yield/2026-06-05/replay_sample_yield_summary.md`
- `reports/atlas_v2_research_os/candidate_backtests/latest.json`
- `reports/atlas_v2_research_os/candidate_backtests/latest_summary.md`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `research_journal/reports/replay_yield_discrepancy_001.md`
- `research_journal/reports/zero_sample_root_cause_study_001.md`
- `research_journal/reports/zero_sample_root_cause_table.csv`

## Result

Global replay sample yield is healthy because it measures aggregate sample retention across a selected proxy-backtest cohort.

Candidate-specific direct validation yield is poor because it applies candidate-specific symbol, timeframe, regime, universe, and event assumptions that are not represented in the global headline metric.

The mismatch is a scope and filter mismatch, not a contradiction.

## Global Yield

Source: `reports/atlas_v2_research_os/replay_sample_yield/2026-06-05/replay_sample_yield.json`

| Measure | Value |
| --- | ---: |
| raw candidate count | 12 |
| trigger sample count | 13,833 |
| post-filter sample count | 12,300 |
| replay sample yield | 88.9178% |
| zero-sample candidate count | 0 |
| low-sample candidate count | 0 |
| low-sample threshold | 30 |
| regime-filter attrition | 1,533 |
| regime-filter attrition share | 11.0822% |
| invalidation attrition | 0 |
| other/unclassified attrition | 0 |

Global candidate rows:

| Candidate | Mechanism | Trigger Samples | Post-Filter Samples | Yield | Regime Filtered | Classification |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `ptc_f492d1bebb6bd47f` | `MEAN_REVERSION` | 1,540 | 170 | 11.0390% | 1,370 | `BACKTEST_SUPPORTED` |
| `ptc_3d46c2d50e5fe58b` | `BREAKOUT` | 811 | 648 | 79.9014% | 163 | `BACKTEST_WEAK` |
| `ptc_7f054744c90194c8` | `VWAP_OR_AVERAGE_RECLAIM` | 374 | 374 | 100.0000% | 0 | `BACKTEST_SUPPORTED` |
| `ptc_fc801836c012a96d` | `EVENT_REACTION` | 515 | 515 | 100.0000% | 0 | `BACKTEST_WEAK` |
| `ptc_b57c4cfe8ed94fff` | `SESSION_TIMING` | 2,542 | 2,542 | 100.0000% | 0 | `BACKTEST_WEAK` |
| `ptc_ffad13e2dbe09619` | `REVERSAL` | 707 | 707 | 100.0000% | 0 | `BACKTEST_SUPPORTED` |
| `ptc_9112fdddb80509f1` | `TREND_CONTINUATION` | 3,120 | 3,120 | 100.0000% | 0 | `BACKTEST_SUPPORTED` |
| `ptc_b97beffb99f141ae` | `OPENING_RANGE` | 764 | 764 | 100.0000% | 0 | `BACKTEST_WEAK` |
| `ptc_695a47fe74f8e36d` | `MEAN_REVERSION` | 1,540 | 1,540 | 100.0000% | 0 | `BACKTEST_SUPPORTED` |
| `ptc_78687792cbb592e8` | `LIQUIDITY_SWEEP` | 285 | 285 | 100.0000% | 0 | `BACKTEST_SUPPORTED` |
| `ptc_0614c2bf7a7c39b0` | `VOLATILITY_EXPANSION` | 824 | 824 | 100.0000% | 0 | `BACKTEST_SUPPORTED` |
| `ptc_2c34d7d9d627b7e7` | `BREAKOUT` | 811 | 811 | 100.0000% | 0 | `BACKTEST_SUPPORTED` |

Global cohort observations:

- 10 of 12 global rows retain 100% of trigger samples after filtering.
- The global headline is sample-weighted, not candidate-failure-weighted.
- The weakest global row still has 170 post-filter samples, above the 30-sample low-sample threshold.
- The global cohort has no zero-sample candidates.

## Candidate-Specific Yield

Source: `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`

| Candidate | Mechanism | Regime | Timeframe | Symbols | Direct Classification | Direct Samples | Regime-Filtered Trigger Samples |
| --- | --- | --- | --- | --- | --- | ---: | ---: |
| `ptc_backtest_final_469607b8340421b7` | `BREAKOUT` | `CHOP` | `30m` | DIA, QQQ, SPY | `INSUFFICIENT_DATA` | 0 | 125 |
| `ptc_backtest_final_3a4ac24107c77136` | `BREAKOUT` | `CHOP` | `5m` | DIA, QQQ, SPY | `INSUFFICIENT_DATA` | 0 | 125 |
| `ptc_backtest_final_624fdd85668e2c08` | `MEAN_REVERSION` | `TRENDING` | `30m` | DBC, TLT, USO | `INSUFFICIENT_DATA` | 36 | 312 |
| `ptc_backtest_final_854ad10b904e1ae9` | `MEAN_REVERSION` | `TRENDING` | `30m` | DIA, QQQ | `CONFIRMED` | 48 | 292 |
| `ptc_backtest_final_d5931b24bd391113` | `EVENT_REACTION` | `CHOP` | `30m` | AMZN, BAC, META, MSFT, NFLX, TSLA | `INSUFFICIENT_DATA` | 0 | 85 |
| `ptc_backtest_final_4df2e8e80685a054` | `REVERSAL` | `TRENDING` | `1h` | AAPL, AMZN, BAC, JPM, META, MSFT, TSLA | `CONFIRMED` | 62 | 86 |
| `ptc_backtest_final_7d839944a8a4070a` | `BREAKOUT` | `CHOP` | `15m` | TLT, USO | `INSUFFICIENT_DATA` | 0 | 71 |
| `ptc_backtest_final_b23c6756bfb3263a` | `BREAKOUT` | `CHOP` | `30m` | BAC, GOOGL, META, MSFT, NFLX, TSLA | `INSUFFICIENT_DATA` | 0 | 98 |

Candidate-specific direct validation observations:

- 8 of 8 direct validation rows ran direct replay.
- 6 of 8 rows remain `INSUFFICIENT_DATA`.
- 5 of 8 rows have zero post-filter samples.
- All 5 zero-sample rows are `CHOP` regime candidates.
- All 5 zero-sample rows have intraday-attributed timeframes but were represented by deterministic daily-bar proxy triggers.
- All 8 rows carry warnings that the candidate plan lacks symbol or universe and that generic observation text was converted into deterministic daily-bar proxy triggers.

## Filter Differences

| Filter Or Gate | Global Replay Cohort | Candidate-Specific Direct Validation | Yield Effect |
| --- | --- | --- | --- |
| Cohort selection | Already-selected `candidate_backtests/latest.json` cohort with 12 replay rows | Direct-validation queue with 8 candidate-specific records | Global excludes the candidate queue's zero-sample failure profile. |
| Symbol handling | Uses SPY adjusted daily data as local proxy when candidate plan lacks symbol/universe | Resolves candidate-attributed symbols, but direct result still reports one tested symbol | Candidate rows expose universe mismatch and single-symbol representation. |
| Timeframe handling | Daily proxy backtest over generic triggers | Candidates are attributed to `30m`, `5m`, `15m`, or `1h` timeframes | Intraday claims suffer when represented by daily proxy triggers. |
| Trigger construction | Generic observation text mapped to deterministic daily-bar proxy triggers | Same generic trigger construction, but applied to more specific candidate universe/regime claims | Candidate-specific constraints can erase every triggered sample. |
| Regime filter | Removes 1,533 of 13,833 global trigger samples, 11.0822% aggregate | Removes all triggered samples in 5 zero-sample candidates: 125, 125, 85, 71, and 98 | Local CHOP rows collapse to zero even though global attrition looks modest. |
| Minimum sample threshold | No global candidate falls below threshold 30 | Zero-sample rows fail `minimum sample size 30 not met` | Threshold failure appears only in candidate-specific direct rows. |
| Event metadata | Global EVENT_REACTION row has 515/515 sample retention using proxy trigger | EVENT_REQUIRED direct row has 0 samples and lacks event metadata | Event-aware alignment is absent locally. |
| Universe aggregation | Not validated as full attributed universe evidence | Multi-symbol candidates still report one tested symbol in direct result | Candidate-specific evidence can be incomplete even when replay runs. |

## Assumption Differences

| Assumption | Global Replay | Candidate-Specific Direct Validation |
| --- | --- | --- |
| Proxy adequacy | SPY daily proxy is acceptable for research-only backtest sample-yield measurement. | SPY or single-symbol daily proxy is insufficient for final candidate-specific validation. |
| Daily bars | Daily bars can produce a replay/backtest sample and sample-yield metric. | Daily bars cannot fully validate intraday entry/exit, event timing, or candidate timeframe claims. |
| Candidate identity | Generic `ptc_*` replay rows measure mechanism-level proxy behavior. | `ptc_backtest_final_*` rows require candidate-specific symbol, timeframe, regime, and universe alignment. |
| Regime vocabulary | Global row survives if enough proxy samples remain after broad regime filtering. | Candidate row fails if exact candidate regime filtering removes every trigger. |
| Event reaction | Event reaction can be represented by a generic proxy trigger in the global cohort. | Event reaction requires timestamped event metadata and 30m event-to-bar alignment. |
| Sample health | Healthy aggregate sample retention implies replay machinery can retain proxy samples. | Healthy candidate evidence requires enough post-filter samples for the actual candidate class. |

## Candidate Classes That Suffer Most

### By Zero-Sample Count

| Class | Zero-Sample Candidates | Candidate Count | Evidence |
| --- | ---: | ---: | --- |
| `BREAKOUT` / `CHOP` | 4 | 4 | `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_7d839944a8a4070a`, `ptc_backtest_final_b23c6756bfb3263a` |
| `EVENT_REACTION` / `CHOP` | 1 | 1 | `ptc_backtest_final_d5931b24bd391113` |
| `MEAN_REVERSION` / `TRENDING` | 0 | 2 | Samples remain but one row is still insufficient at 36 samples. |
| `REVERSAL` / `TRENDING` | 0 | 1 | 62 samples and confirmed. |

Note: The `EVENT_REACTION` / `CHOP` row overlaps with the broader CHOP failure pattern but has an additional event-metadata dependency.

### By Regime

| Regime | Candidate Rows | Zero-Sample Rows | Zero-Sample Rate |
| --- | ---: | ---: | ---: |
| `CHOP` | 5 | 5 | 100.00% |
| `TRENDING` | 3 | 0 | 0.00% |

### By Timeframe

| Timeframe | Candidate Rows | Zero-Sample Rows | Zero-Sample Rate |
| --- | ---: | ---: | ---: |
| `30m` | 5 | 3 | 60.00% |
| `5m` | 1 | 1 | 100.00% |
| `15m` | 1 | 1 | 100.00% |
| `1h` | 1 | 0 | 0.00% |

### By Mechanism

| Mechanism | Candidate Rows | Zero-Sample Rows | Suffering Pattern |
| --- | ---: | ---: | --- |
| `BREAKOUT` | 4 | 4 | Most severe; every local breakout candidate is zero-sample after CHOP/regime filtering. |
| `EVENT_REACTION` | 1 | 1 | Severe; zero-sample plus missing event metadata. |
| `MEAN_REVERSION` | 2 | 0 | Partial; one insufficient row at 36 samples and one confirmed row at 48 samples. |
| `REVERSAL` | 1 | 0 | Least affected in this queue; confirmed with 62 samples. |

## Failure Chain

The local zero-sample chain is:

```text
candidate-specific intraday/regime claim
-> daily proxy CSV load
-> generic observation text converted to daily-bar proxy trigger
-> trigger samples exist
-> exact candidate regime constraints applied
-> all triggered samples filtered
-> post-filter sample_size = 0
-> minimum sample size 30 not met
-> INSUFFICIENT_DATA
```

For the event-reaction candidate, the chain adds:

```text
EVENT_REACTION claim
-> no timestamped event metadata
-> no event-to-30m-bar alignment
-> generic daily-bar proxy trigger substitutes for event trigger
-> all triggered samples filtered
```

## Answer To Questions

### Which filters differ?

- Global replay uses broad proxy replay filters over the selected backtest cohort.
- Candidate-specific direct validation applies candidate regime constraints to attributed candidate records.
- Local zero-sample rows are dominated by candidate regime filtering: all triggered samples are removed in the affected rows.
- Candidate-specific validation also applies minimum sample threshold failure after filtering.
- Event-reaction validation requires metadata/event-alignment filters that are absent from the global proxy sample-yield metric.

### Which assumptions differ?

- Global assumes proxy daily replay is sufficient to measure research sample retention.
- Candidate-specific validation assumes symbol, universe, timeframe, regime, and event metadata matter.
- Global assumes aggregate sample retention is a valid cohort health metric.
- Candidate-specific validation requires per-candidate post-filter sample adequacy.
- Global tolerates generic observation-to-trigger conversion as proxy evidence.
- Candidate-specific validation exposes that generic trigger conversion does not preserve intraday/event candidate semantics.

### Which candidate classes suffer most?

Most affected:

1. `BREAKOUT` / `CHOP`: 4 of 4 local rows are zero-sample.
2. `EVENT_REACTION` / `CHOP`: 1 of 1 local rows is zero-sample and event metadata is missing.
3. Intraday-attributed CHOP candidates: 5 of 5 local CHOP rows are zero-sample.
4. `5m` and `15m` intraday candidates: 100% zero-sample in the current local queue, though each has only one row.
5. `30m` candidates: 3 of 5 are zero-sample, with additional event/replay alignment weaknesses.

Least affected in this local queue:

- `REVERSAL` / `TRENDING`: confirmed with 62 direct samples.
- `MEAN_REVERSION` / `TRENDING`: nonzero direct samples in both rows, though one remains insufficient at 36 samples.

## Authority Boundary

This report is diagnostic only. It does not implement replay changes, relax filters, download data, acquire intraday bars, add event metadata, promote candidates, qualify candidates, change governance, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, or alter safety gates.
