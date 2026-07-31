# Replay Attrition Remediation Plan 001

Created: 2026-06-05
Status: plan only

Scope: remediation plan based on the zero-sample / insufficient-data root-cause study. This report does not implement remediation, acquire data, change replay logic, change candidate state, change ranking, change paper-forward state, promote candidates, override qualification, alter governance, recommend trades, allocate capital, size positions, or authorize broker execution.

## Inputs

- `research_journal/reports/insufficient_data_root_cause_analysis_001.md`
- `research_journal/reports/insufficient_data_root_cause_table.csv`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest_summary.md`
- `reports/atlas_v2_research_os/market_data_acquisition/next_unlock_validation_delta_001.md`
- `research_journal/reports/research_debt_post_75pct_data_update_001.md`

## Current State

- candidates reviewed: `8`
- confirmed: `1`
- insufficient data: `7`
- direct replays run: `6`
- zero post-filter direct samples: `4` candidates
- no direct replay due to missing local data: `2` candidates
- partial-universe block despite supportive partial direct replay: `1` candidate

The zero-sample cases are concentrated in candidates where local daily data exists but daily proxy replay is standing in for intraday or event-specific mechanics. The remaining non-zero-sample blockers are mostly coverage completion issues.

## Root Causes

### Missing Symbol Coverage

- issue: Required local direct CSVs are absent for part or all of the attributed universe. This prevents replay from running at all for two candidates and prevents one candidate from graduating despite a supportive partial direct replay.
- affected candidates: `ptc_backtest_final_624fdd85668e2c08`, `ptc_backtest_final_7d839944a8a4070a`, `ptc_backtest_final_4df2e8e80685a054`.
- expected validation gain: High. Completing `AAPL`, `JPM`, `TLT`, `USO`, and `DBC` daily coverage can directly unblock first-pass validation for `3` of `7` remaining insufficient-data candidates. `ptc_backtest_final_4df2e8e80685a054` is especially high-leverage because its partial direct replay already showed `BACKTEST_SUPPORTED` with sample size `62`.
- implementation complexity: Low to medium. Daily adjusted OHLCV acquisition/import is already represented in the existing data acquisition workflow and does not require replay-engine changes.
- risk: Low authority risk if handled as historical data staging only. Data-quality risk is moderate because symbols must be adjusted, date-aligned, schema-validated, and marked as historical rather than live trading data.
- recommended action: Stage and validate daily adjusted files for `AAPL`, `JPM`, `TLT`, `USO`, and `DBC`, then rerun direct validation in analysis mode only. Prioritize `AAPL` + `JPM` first because it completes a candidate that already has supportive partial direct evidence; prioritize `TLT` + `USO` second because it unblocks one no-coverage candidate and partially supports another; add `DBC` third to complete the commodity/rates mean-reversion candidate.

### Daily-vs-Intraday Timeframe Mismatch

- issue: Candidates attributed to `5m`, `15m`, `30m`, or `1h` mechanisms are being tested with daily proxy logic. Daily data can prove basic symbol coverage, but it cannot directly validate intraday breakout, reversal, or mean-reversion rules.
- affected candidates: `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_b23c6756bfb3263a`, `ptc_backtest_final_d5931b24bd391113`, `ptc_backtest_final_624fdd85668e2c08`, `ptc_backtest_final_7d839944a8a4070a`, `ptc_backtest_final_4df2e8e80685a054`.
- expected validation gain: Very high but staged. Intraday coverage is required for direct mechanism validation across all `7` insufficient-data candidates. Immediate zero-sample unblock potential is highest for the already daily-covered zero-sample set: `469607...`, `3a4ac...`, and `b23c...`.
- implementation complexity: Medium to high. Intraday historical data acquisition is broader, provider-dependent, and timeframe-specific; replay must also consume the correct bar interval.
- risk: Low authority risk if data remains historical and analysis-only. Data risk is higher than daily data because intraday gaps, split adjustments, session handling, extended-hours policy, and provider limitations can distort samples.
- recommended action: Define a historical intraday data manifest and validation gate before import. Start with the smallest high-impact set: `DIA`, `QQQ`, and `SPY` at `30m` and `5m` for the two top breakout candidates; then add `BAC`, `GOOGL`, `META`, `MSFT`, `NFLX`, and `TSLA` at `30m` for the single-stock breakout candidate. Keep intraday data staging separate from any paper-forward or trading workflow.

### Over-Strict Trigger/Regime Filtering

- issue: Daily proxy replay generates trigger candidates, then filters all usable samples out under candidate regime constraints. The zero-sample candidates show triggered samples before filtering: `125`, `125`, `85`, and `98`, but post-filter sample size becomes `0`.
- affected candidates: `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_d5931b24bd391113`, `ptc_backtest_final_b23c6756bfb3263a`.
- expected validation gain: High. A diagnostics-only replay explaining why each triggered sample is removed can unblock understanding for all `4` zero-sample cases and may reveal whether the issue is regime labeling, trigger translation, symbol selection, or a legitimate null result.
- implementation complexity: Medium. Requires replay instrumentation and a per-filter attrition ledger, but does not require changing the candidate rules or relaxing thresholds.
- risk: Low authority risk if implemented as diagnostics-only. Methodology risk is high if the next step relaxes filters without preserving a strict baseline; that would create false confirmations.
- recommended action: Add a read-only attrition report before changing filters. For each candidate, count samples at stages: raw bars, trigger events, regime-matched events, entry-valid events, exit-valid events, and final scored samples. Do not loosen any filter until the attrition ledger proves which filter is eliminating all samples.

### Generic Daily-Bar Proxy Trigger Translation

- issue: Validator warnings say generic observation text was converted into deterministic daily-bar proxy triggers. That translation is probably too coarse for intraday breakout/event/reversal mechanics and can create sample attrition or false proxy support.
- affected candidates: `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_d5931b24bd391113`, `ptc_backtest_final_b23c6756bfb3263a`, and partly `ptc_backtest_final_4df2e8e80685a054`.
- expected validation gain: Medium to high. Better mechanism-specific trigger definitions can convert the replay from proxy approximation into candidate-specific evidence, but it may also correctly weaken candidates.
- implementation complexity: Medium to high. Requires mapping mechanisms to explicit trigger templates: breakout/chop, reversal/trending, mean-reversion/trending, and event-reaction/chop.
- risk: Medium authority risk if trigger changes are mistaken for qualification changes. Methodology risk is high unless old and new triggers are reported side by side.
- recommended action: Create a versioned replay-trigger specification and run old-vs-new comparison reports before any validation classification changes. Treat trigger refinement as experimental until the comparison shows stable, explainable sample recovery.

### Missing Event Metadata

- issue: The event-reaction candidate has full daily symbol coverage, but no event calendar, earnings/news timestamp, or event-aware replay trigger. Daily price bars alone cannot validate an event-reaction mechanism.
- affected candidates: `ptc_backtest_final_d5931b24bd391113`.
- expected validation gain: Medium for candidate count, high for that candidate's validity. It can unblock `1` candidate, but it is essential because this root cause is mechanism-specific and cannot be solved by daily data alone.
- implementation complexity: High. Requires event source selection, timestamp normalization, symbol binding, market-session alignment, and event-aware replay windows.
- risk: Medium authority risk if event data is treated as a signal source rather than historical validation context. Data risk is high because wrong timestamps or after-hours handling can invert conclusions.
- recommended action: Keep the event-reaction candidate blocked from stronger validation claims until event metadata exists. Build an event metadata fixture first, then test event-window replay in a diagnostic report before changing validation status.

### Universe Aggregation Limitation

- issue: Direct replay reports one symbol result per candidate even when the candidate is universe-level. That can leave partial evidence over-weighted or under-weighted and can hide symbol-level heterogeneity.
- affected candidates: `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_d5931b24bd391113`, `ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_b23c6756bfb3263a`, and confirmed comparator `ptc_backtest_final_854ad10b904e1ae9`.
- expected validation gain: Medium. It may not immediately create samples, but it improves confidence and prevents single-symbol evidence from standing in for a multi-symbol candidate.
- implementation complexity: Medium. Requires per-symbol replay rows, aggregation policy, and candidate-level classification rules.
- risk: Low authority risk if it is read-only reporting. Methodology risk is moderate because aggregation choices can change candidate interpretation.
- recommended action: Add per-symbol replay output and classify candidate-level evidence using explicit aggregation rules: all-symbol pass, quorum pass, best-symbol exploratory, and insufficient universe coverage. Do not use aggregation to promote candidates automatically.

### Threshold and Minimum-Sample Interpretation

- issue: Zero-sample candidates fail the minimum sample size target after filtering. The threshold itself is not the primary root cause, but the current output collapses different causes into `minimum sample size not met`.
- affected candidates: `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_d5931b24bd391113`, `ptc_backtest_final_b23c6756bfb3263a`.
- expected validation gain: Low to medium. Better classification of sample-size failure improves triage but should not by itself confirm candidates.
- implementation complexity: Low. It is mostly report taxonomy: distinguish `NO_TRIGGER_EVENTS`, `REGIME_FILTER_ZEROED`, `ENTRY_EXIT_RULE_ZEROED`, `MISSING_INTRADAY_DATA`, and `MISSING_EVENT_METADATA`.
- risk: Low authority risk. Main risk is semantic drift if new reason codes are interpreted as softer validation failures.
- recommended action: Add more precise insufficiency reason codes while preserving the existing `INSUFFICIENT_DATA` classification. This is a triage improvement, not a validation improvement.

## Ranked Actions

Ranking criteria, in order: highest validation unblock potential, lowest authority risk, lowest implementation complexity.

| Rank | Action | Root cause addressed | Affected candidates | Expected validation gain | Complexity | Authority risk | Recommended action |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 1 | Complete remaining daily symbol coverage for partial/no-coverage candidates | Missing symbol coverage | `4df2e8e80685a054`, `7d839944a8a4070a`, `624fdd85668e2c08` | High: can unblock first-pass direct validation for `3` candidates; strongest immediate candidate is `4df2e8e80685a054` because partial direct replay is already supportive | Low-medium | Low | Stage historical daily adjusted files for `AAPL`, `JPM`, `TLT`, `USO`, `DBC`; validate schema/date coverage; rerun direct validation in analysis mode |
| 2 | Add zero-sample attrition ledger before changing replay rules | Over-strict trigger/regime filtering | `469607b8340421b7`, `3a4ac24107c77136`, `d5931b24bd391113`, `b23c6756bfb3263a` | High: explains all `4` zero-post-filter failures and identifies the exact filter stage to remediate | Medium | Low | Build diagnostics-only counts from raw bars to final scored samples; do not relax filters in this step |
| 3 | Add precise insufficiency reason codes | Threshold and minimum-sample interpretation | Same `4` zero-sample candidates, plus future insufficient-data cases | Medium: improves triage and prevents sample-size failures from hiding root causes | Low | Low | Report `REGIME_FILTER_ZEROED`, `MISSING_INTRADAY_DATA`, `MISSING_EVENT_METADATA`, and related codes while preserving `INSUFFICIENT_DATA` |
| 4 | Stage targeted intraday data for highest-ranked zero-sample breakout candidates | Daily-vs-intraday mismatch | `469607b8340421b7`, `3a4ac24107c77136` | High: directly tests the top DIA/QQQ/SPY breakout cluster at `30m` and `5m` | Medium-high | Low | Stage historical `DIA`, `QQQ`, `SPY` intraday bars for `30m` and `5m`; validate coverage before replay |
| 5 | Stage targeted intraday data for single-stock breakout candidate | Daily-vs-intraday mismatch | `b23c6756bfb3263a` | Medium-high: can unblock one full-coverage zero-sample candidate across six equities | High | Low | Stage `30m` historical bars for `BAC`, `GOOGL`, `META`, `MSFT`, `NFLX`, `TSLA`; keep analysis-only |
| 6 | Add per-symbol replay rows and explicit aggregation policy | Universe aggregation limitation | `469607b8340421b7`, `3a4ac24107c77136`, `d5931b24bd391113`, `4df2e8e80685a054`, `b23c6756bfb3263a`, `854ad10b904e1ae9` | Medium: improves evidence quality and prevents one-symbol overclaiming | Medium | Low | Report per-symbol direct results and classify aggregate evidence without automatic promotion |
| 7 | Version mechanism-specific trigger templates | Generic daily-bar proxy trigger translation | `469607b8340421b7`, `3a4ac24107c77136`, `d5931b24bd391113`, `b23c6756bfb3263a`, `4df2e8e80685a054` | Medium-high: may recover legitimate samples or correctly weaken proxy candidates | Medium-high | Medium | Define old-vs-new trigger comparison artifacts before any classification changes |
| 8 | Build event metadata fixture and event-window replay diagnostic | Missing event metadata | `d5931b24bd391113` | Medium: unblocks one mechanism-specific candidate; high validity impact for event-reaction research | High | Medium | Keep candidate blocked; add event calendar/news/earnings metadata fixture and event-aware diagnostic replay |

## Recommended Sequence

1. Complete daily data coverage for `AAPL`, `JPM`, `TLT`, `USO`, and `DBC`.
2. Add read-only zero-sample attrition ledger and precise insufficiency reason codes.
3. Rerun direct validation in analysis mode and separate coverage failures from filter-zeroed failures.
4. Stage targeted intraday data for the DIA/QQQ/SPY breakout cluster.
5. Add per-symbol replay rows and aggregation rules before interpreting universe-level candidates.
6. Only after diagnostics are stable, evaluate mechanism-specific trigger templates.
7. Treat event-reaction remediation as a separate lane requiring event metadata and event-window replay.

## Do Not Do Yet

- Do not relax filters just to produce samples.
- Do not convert zero-sample candidates to confirmed candidates.
- Do not change final ranking.
- Do not change paper-forward state.
- Do not create candidate promotions or paper trades.
- Do not treat daily data as full validation for intraday or event-specific mechanisms.
- Do not let one-symbol direct replay stand in for a full universe-level conclusion without explicit aggregation policy.

## Authority Boundary

This plan is diagnostic and sequencing guidance only. It does not implement remediation, mutate runtime truth, promote candidates, change ranking, change paper-forward state, recommend trades, allocate capital, size positions, authorize broker execution, or change governance state.
