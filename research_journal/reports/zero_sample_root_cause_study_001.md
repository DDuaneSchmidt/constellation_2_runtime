# Zero Sample Root Cause Study 001

Created: 2026-06-05T19:58:15Z

Status: GENERATED_ONLY

Scope: diagnostic study of fully covered Atlas candidates that still produce zero post-filter direct replay samples. This report does not modify candidate state, replay state, qualification state, governance state, paper-forward state, memory, or runtime truth.

Authority boundary: no candidate promotion, replay override, qualification override, governance change, trading recommendation, capital authority, broker execution, position sizing, portfolio construction, or automatic paper trade placement.

## Inputs Reviewed

- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/candidate_symbol_attribution/latest.json`
- `reports/atlas_v2_research_os/market_data_coverage_tracker/latest.json`
- `research_journal/reports/insufficient_data_root_cause_table.csv`

## Population

- fully covered but `INSUFFICIENT_DATA` zero-sample candidates: `4`
- inclusion rule: current coverage status is `FULL_COVERAGE`, direct validation classification is `INSUFFICIENT_DATA`, direct replay was attempted, and direct result sample size is `0`.
- excluded: partially covered candidate `ptc_backtest_final_4df2e8e80685a054`, because it produced a nonzero supported direct result on `BAC` but remains insufficient due to incomplete universe coverage.

## Main Finding

The zero-sample failures are not caused by absent daily CSVs. The validator loads daily data and generates trigger candidates, but the candidate regime/filter stage removes every triggered sample. All four cases also carry warnings that generic observation text was converted into a deterministic daily-bar proxy trigger, while the attributed candidate timeframes are intraday. This points to a combined daily-proxy/timeframe/replay-logic issue, with a separate event-metadata gap for the event-reaction candidate.

## Summary Table

| Candidate ID | Symbols | Timeframe | Raw Samples | Trigger Samples | Post-Filter Samples | Root Cause Classes | Minimum Remediation |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | DIA; QQQ; SPY | `30m` | 1256 | 125 | 0 | FILTER_TOO_STRICT; DAILY_PROXY_MISMATCH; TIMEFRAME_MISMATCH; REPLAY_LOGIC_GAP; UNIVERSE_AGGREGATION_ISSUE | Use 30m intraday bars for the attributed universe and inspect/relax or redesign regime filtering; aggregate replay evidence across all attributed symbols instead of one symbol. |
| `ptc_backtest_final_3a4ac24107c77136` | DIA; QQQ; SPY | `5m` | 1256 | 125 | 0 | FILTER_TOO_STRICT; DAILY_PROXY_MISMATCH; TIMEFRAME_MISMATCH; REPLAY_LOGIC_GAP; UNIVERSE_AGGREGATION_ISSUE | Use 5m intraday bars for the attributed universe and inspect/relax or redesign regime filtering; aggregate replay evidence across all attributed symbols instead of one symbol. |
| `ptc_backtest_final_d5931b24bd391113` | AMZN; BAC; META; MSFT; NFLX; TSLA | `30m` | 1256 | 85 | 0 | EVENT_METADATA_REQUIRED; DAILY_PROXY_MISMATCH; TIMEFRAME_MISMATCH; REPLAY_LOGIC_GAP; UNIVERSE_AGGREGATION_ISSUE | Add event metadata and event-aware replay trigger; then validate with 30m intraday data across AMZN/BAC/META/MSFT/NFLX/TSLA. |
| `ptc_backtest_final_b23c6756bfb3263a` | BAC; GOOGL; META; MSFT; NFLX; TSLA | `30m` | 1256 | 98 | 0 | FILTER_TOO_STRICT; DAILY_PROXY_MISMATCH; TIMEFRAME_MISMATCH; REPLAY_LOGIC_GAP; UNIVERSE_AGGREGATION_ISSUE | Use 30m intraday bars for the attributed universe and inspect/relax or redesign regime filtering; aggregate replay evidence across all attributed symbols instead of one symbol. |

## Candidate Detail

### `ptc_backtest_final_469607b8340421b7`

- symbols: `DIA; QQQ; SPY`
- timeframe: `30m`
- replay attempted: `yes`
- tested symbol reported by validator: `DIA`
- raw sample count: `1256` from `data/cache/DIA_tiingo_adjusted_daily.csv`
- trigger sample count: `125`
- post-filter sample count: `0`
- filter stages applied: daily CSV load; symbol/proxy fallback; deterministic daily-bar proxy trigger; candidate regime constraints; minimum sample size check
- attrition by stage: 1256->125 (1131 removed before/at trigger stage); 125->0 (125 removed by regime/filter stage); post-filter minimum sample check 0<30
- root cause classification: `FILTER_TOO_STRICT; DAILY_PROXY_MISMATCH; TIMEFRAME_MISMATCH; REPLAY_LOGIC_GAP; UNIVERSE_AGGREGATION_ISSUE`
- root cause: Daily proxy trigger created candidate events, but the regime/filter stage removed every triggered sample. The requested intraday timeframe is not being directly tested, and the validator reports a single tested symbol rather than full-universe aggregation.
- minimum remediation: Use 30m intraday bars for the attributed universe and inspect/relax or redesign regime filtering; aggregate replay evidence across all attributed symbols instead of one symbol.
- validator warnings: candidate plan has no symbol or universe; used SPY adjusted daily data as local proxy | generic observation text converted into deterministic daily-bar proxy trigger | 125 triggered samples filtered by candidate regime constraints

### `ptc_backtest_final_3a4ac24107c77136`

- symbols: `DIA; QQQ; SPY`
- timeframe: `5m`
- replay attempted: `yes`
- tested symbol reported by validator: `DIA`
- raw sample count: `1256` from `data/cache/DIA_tiingo_adjusted_daily.csv`
- trigger sample count: `125`
- post-filter sample count: `0`
- filter stages applied: daily CSV load; symbol/proxy fallback; deterministic daily-bar proxy trigger; candidate regime constraints; minimum sample size check
- attrition by stage: 1256->125 (1131 removed before/at trigger stage); 125->0 (125 removed by regime/filter stage); post-filter minimum sample check 0<30
- root cause classification: `FILTER_TOO_STRICT; DAILY_PROXY_MISMATCH; TIMEFRAME_MISMATCH; REPLAY_LOGIC_GAP; UNIVERSE_AGGREGATION_ISSUE`
- root cause: Daily proxy trigger created candidate events, but the regime/filter stage removed every triggered sample. The requested intraday timeframe is not being directly tested, and the validator reports a single tested symbol rather than full-universe aggregation.
- minimum remediation: Use 5m intraday bars for the attributed universe and inspect/relax or redesign regime filtering; aggregate replay evidence across all attributed symbols instead of one symbol.
- validator warnings: candidate plan has no symbol or universe; used SPY adjusted daily data as local proxy | generic observation text converted into deterministic daily-bar proxy trigger | 125 triggered samples filtered by candidate regime constraints

### `ptc_backtest_final_d5931b24bd391113`

- symbols: `AMZN; BAC; META; MSFT; NFLX; TSLA`
- timeframe: `30m`
- replay attempted: `yes`
- tested symbol reported by validator: `AMZN`
- raw sample count: `1256` from `data/cache/AMZN_tiingo_adjusted_daily.csv`
- trigger sample count: `85`
- post-filter sample count: `0`
- filter stages applied: daily CSV load; symbol/proxy fallback; deterministic daily-bar proxy trigger; candidate regime constraints; minimum sample size check
- attrition by stage: 1256->85 (1171 removed before/at trigger stage); 85->0 (85 removed by regime/filter stage); post-filter minimum sample check 0<30
- root cause classification: `EVENT_METADATA_REQUIRED; DAILY_PROXY_MISMATCH; TIMEFRAME_MISMATCH; REPLAY_LOGIC_GAP; UNIVERSE_AGGREGATION_ISSUE`
- root cause: Event-reaction mechanism is represented by a generic daily-bar proxy with no event calendar/news/earnings timing. All triggered samples were removed by regime/filter logic, leaving zero post-filter observations.
- minimum remediation: Add event metadata and event-aware replay trigger; then validate with 30m intraday data across AMZN/BAC/META/MSFT/NFLX/TSLA.
- validator warnings: candidate plan has no symbol or universe; used SPY adjusted daily data as local proxy | generic observation text converted into deterministic daily-bar proxy trigger | 85 triggered samples filtered by candidate regime constraints

### `ptc_backtest_final_b23c6756bfb3263a`

- symbols: `BAC; GOOGL; META; MSFT; NFLX; TSLA`
- timeframe: `30m`
- replay attempted: `yes`
- tested symbol reported by validator: `BAC`
- raw sample count: `1256` from `data/cache/BAC_tiingo_adjusted_daily.csv`
- trigger sample count: `98`
- post-filter sample count: `0`
- filter stages applied: daily CSV load; symbol/proxy fallback; deterministic daily-bar proxy trigger; candidate regime constraints; minimum sample size check
- attrition by stage: 1256->98 (1158 removed before/at trigger stage); 98->0 (98 removed by regime/filter stage); post-filter minimum sample check 0<30
- root cause classification: `FILTER_TOO_STRICT; DAILY_PROXY_MISMATCH; TIMEFRAME_MISMATCH; REPLAY_LOGIC_GAP; UNIVERSE_AGGREGATION_ISSUE`
- root cause: Daily proxy trigger created candidate events, but the regime/filter stage removed every triggered sample. The requested intraday timeframe is not being directly tested, and the validator reports a single tested symbol rather than full-universe aggregation.
- minimum remediation: Use 30m intraday bars for the attributed universe and inspect/relax or redesign regime filtering; aggregate replay evidence across all attributed symbols instead of one symbol.
- validator warnings: candidate plan has no symbol or universe; used SPY adjusted daily data as local proxy | generic observation text converted into deterministic daily-bar proxy trigger | 98 triggered samples filtered by candidate regime constraints

## Root Cause Classification Notes

- `TRIGGER_TOO_STRICT`: not selected as primary for these cases because the validator did report triggered samples before filtering.
- `FILTER_TOO_STRICT`: applies to non-event candidates because regime constraints removed all triggered samples.
- `DAILY_PROXY_MISMATCH`: applies to all four cases because the validator used deterministic daily-bar proxy triggers for intraday-attributed candidates.
- `UNIVERSE_AGGREGATION_ISSUE`: applies because direct results report one tested symbol even when candidates are attributed to multi-symbol universes.
- `TIMEFRAME_MISMATCH`: applies because candidate timeframes are `5m` or `30m`, while available evidence is daily proxy replay.
- `EVENT_METADATA_REQUIRED`: applies to the event-reaction candidate; no event calendar/news/earnings timing appears in the direct validation artifact.
- `REPLAY_LOGIC_GAP`: applies because full daily coverage plus nonzero trigger counts should produce explainable stage-level attrition, but current output only exposes warning strings and final sample size.
- `UNKNOWN`: not used; the artifacts provide enough evidence to classify the dominant failure modes.

## Minimum Remediation Order

1. Add stage-level replay telemetry: raw rows loaded, trigger construction count, regime filter count, final eligible sample count, and per-symbol aggregation counts.
2. Validate intraday candidates with matching intraday bars instead of daily proxy triggers: `5m` for `ptc_backtest_final_3a4ac24107c77136`, `30m` for the other three.
3. Review regime filters for full-coverage candidates where all triggered samples are removed.
4. Add event metadata and event-aware trigger construction for `ptc_backtest_final_d5931b24bd391113`.
5. Aggregate direct replay evidence across all attributed symbols rather than reporting one symbol as representative of the universe.

No candidate promotion, replay override, qualification override, governance change, trading recommendation, capital authority, broker execution, position sizing, portfolio construction, automatic paper trade placement, or automatic memory write was added.
