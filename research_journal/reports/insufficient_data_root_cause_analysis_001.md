# Insufficient Data Root Cause Analysis 001

Created: 2026-06-05T19:55:09Z

Status: GENERATED_ONLY

Scope: analysis of the seven remaining `INSUFFICIENT_DATA` direct-candidate validation cases after the 75%+ candidate-symbol data acquisition and the subsequent GOOGL unlock. This report is diagnostic only.

Authority boundary: no candidate promotion, replay override, qualification override, governance change, trading recommendation, capital authority, broker execution, position sizing, portfolio construction, automatic paper trade placement, or automatic memory write.

## Inputs Reviewed

- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/market_data_acquisition/validation_delta_report_001.md`
- `reports/atlas_v2_research_os/market_data_acquisition/next_unlock_validation_delta_001.md`
- `reports/atlas_v2_research_os/candidate_symbol_attribution/latest.json`
- `reports/atlas_v2_research_os/market_data_coverage_tracker/latest.json`
- `research_journal/reports/confirmed_candidate_evidence_review_001.md`
- `reports/atlas_v2_research_os/candidate_data_validation_plan/latest.json`

## Current State

- direct validation candidates reviewed: `8`
- confirmed: `1`
- remaining insufficient data: `7`
- current unique-symbol coverage: `66.67%` (`10/15` candidate symbols)
- current full candidate coverage: `5/8`
- current validation block rate: `37.5%`
- current missing symbols: `AAPL`, `DBC`, `JPM`, `TLT`, `USO`

## Findings

Daily symbol acquisition removed the first layer of blockage for several candidates, but it did not make daily proxy replay equivalent to the candidates' attributed intraday rules. Four remaining candidates now have full daily coverage but still fail because the validator records zero usable samples after trigger/regime filtering or because event timing is absent. Three candidates still have missing-symbol coverage gaps.

Root-cause counts:

- missing symbols: `3` candidates
- insufficient sample size after trigger/regime filtering: `4` candidates
- timeframe or daily-vs-intraday mismatch: `7` candidates
- missing event metadata: `1` candidate
- replay logic limitation: `4` candidates
- qualification threshold: `0` candidates as primary cause
- universe aggregation issue: `5` candidates

## Candidate Table

| Candidate ID | Symbols | Timeframe | Coverage | Replay Attempted | Root Cause | Minimum Next Action | Daily Resolves? | Intraday Required? |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | DIA; QQQ; SPY | `30m` | FULL_COVERAGE daily symbols 3/3; no 30m intraday coverage evidenced | yes | insufficient sample size after filtering; daily-vs-intraday mismatch; replay logic limitation; universe aggregation issue | Acquire/attach DIA, QQQ, and SPY 30m intraday bars or refine the direct replay trigger/regime filter so the 30m breakout claim is tested directly across the attributed universe. | no | yes |
| `ptc_backtest_final_3a4ac24107c77136` | DIA; QQQ; SPY | `5m` | FULL_COVERAGE daily symbols 3/3; no 5m intraday coverage evidenced | yes | insufficient sample size after filtering; daily-vs-intraday mismatch; replay logic limitation; universe aggregation issue | Acquire/attach DIA, QQQ, and SPY 5m intraday bars or rewrite the direct replay test so the 5m breakout mechanism is not represented by a daily-bar proxy. | no | yes |
| `ptc_backtest_final_624fdd85668e2c08` | DBC; TLT; USO | `30m` | NO_COVERAGE daily symbols 0/3 | no | missing symbols; daily-vs-intraday mismatch remains after daily acquisition | Acquire DBC, TLT, and USO adjusted daily files first; add 30m intraday bars for rule-specific validation after daily replay can run. | yes, for first-pass direct replay coverage | yes, for mechanism/timeframe validation |
| `ptc_backtest_final_d5931b24bd391113` | AMZN; BAC; META; MSFT; NFLX; TSLA | `30m` | FULL_COVERAGE daily symbols 6/6; no event metadata or 30m event-timing coverage evidenced | yes | missing event metadata; insufficient sample size after filtering; daily-vs-intraday mismatch; replay logic limitation; universe aggregation issue | Attach event calendar/earnings/news metadata and 30m intraday data for the equity universe; implement an event-aware replay trigger instead of generic daily-bar proxy logic. | no | yes, plus event metadata |
| `ptc_backtest_final_4df2e8e80685a054` | AAPL; AMZN; BAC; JPM; META; MSFT; TSLA | `1h` | PARTIAL_COVERAGE daily symbols 5/7 | yes | missing symbols; universe aggregation issue; daily-vs-intraday mismatch remains after daily acquisition | Acquire AAPL and JPM adjusted daily files to complete universe coverage; then add 1h intraday files for rule-specific reversal validation. | yes, likely for direct coverage/classification if validation logic accepts daily replay | yes, for mechanism/timeframe validation |
| `ptc_backtest_final_7d839944a8a4070a` | TLT; USO | `15m` | NO_COVERAGE daily symbols 0/2 | no | missing symbols; daily-vs-intraday mismatch remains after daily acquisition | Acquire TLT and USO adjusted daily files first; add 15m intraday bars for rule-specific breakout validation. | yes, for first-pass direct replay coverage | yes, for mechanism/timeframe validation |
| `ptc_backtest_final_b23c6756bfb3263a` | BAC; GOOGL; META; MSFT; NFLX; TSLA | `30m` | FULL_COVERAGE daily symbols 6/6 after GOOGL unlock; no 30m intraday coverage evidenced | yes | insufficient sample size after filtering; daily-vs-intraday mismatch; replay logic limitation; universe aggregation issue | Acquire/attach 30m intraday data for BAC, GOOGL, META, MSFT, NFLX, and TSLA or refine the breakout trigger/regime filter across the full universe. | no; daily coverage is already complete | yes |

## Candidate Details

### `ptc_backtest_final_469607b8340421b7`

- mechanism/regime: `BREAKOUT` / `CHOP`
- attributed symbols: `DIA; QQQ; SPY`
- timeframe: `30m`
- full coverage status: FULL_COVERAGE daily symbols 3/3; no 30m intraday coverage evidenced
- missing data if any: No missing daily symbols; missing rule-specific 30m intraday data/trigger evidence
- replay attempted: `yes`
- reason direct validation remains insufficient: Daily proxy replay ran on DIA but produced sample_size=0 after trigger/regime filtering; direct_result says minimum sample size 30 not met after 125 triggered samples were filtered. This is not a symbol absence problem anymore.
- minimum next action: Acquire/attach DIA, QQQ, and SPY 30m intraday bars or refine the direct replay trigger/regime filter so the 30m breakout claim is tested directly across the attributed universe.
- daily data can resolve it: no
- intraday data required: yes

### `ptc_backtest_final_3a4ac24107c77136`

- mechanism/regime: `BREAKOUT` / `CHOP`
- attributed symbols: `DIA; QQQ; SPY`
- timeframe: `5m`
- full coverage status: FULL_COVERAGE daily symbols 3/3; no 5m intraday coverage evidenced
- missing data if any: No missing daily symbols; missing rule-specific 5m intraday data/trigger evidence
- replay attempted: `yes`
- reason direct validation remains insufficient: Daily proxy replay ran on DIA but produced sample_size=0 after trigger/regime filtering; direct_result says minimum sample size 30 not met after 125 triggered samples were filtered.
- minimum next action: Acquire/attach DIA, QQQ, and SPY 5m intraday bars or rewrite the direct replay test so the 5m breakout mechanism is not represented by a daily-bar proxy.
- daily data can resolve it: no
- intraday data required: yes

### `ptc_backtest_final_624fdd85668e2c08`

- mechanism/regime: `MEAN_REVERSION` / `TRENDING`
- attributed symbols: `DBC; TLT; USO`
- timeframe: `30m`
- full coverage status: NO_COVERAGE daily symbols 0/3
- missing data if any: DBC; TLT; USO daily and 30m files missing
- replay attempted: `no`
- reason direct validation remains insufficient: No local direct CSV data found for resolved symbols, so direct replay did not run.
- minimum next action: Acquire DBC, TLT, and USO adjusted daily files first; add 30m intraday bars for rule-specific validation after daily replay can run.
- daily data can resolve it: yes, for first-pass direct replay coverage
- intraday data required: yes, for mechanism/timeframe validation

### `ptc_backtest_final_d5931b24bd391113`

- mechanism/regime: `EVENT_REACTION` / `CHOP`
- attributed symbols: `AMZN; BAC; META; MSFT; NFLX; TSLA`
- timeframe: `30m`
- full coverage status: FULL_COVERAGE daily symbols 6/6; no event metadata or 30m event-timing coverage evidenced
- missing data if any: No missing daily symbols; missing event metadata and rule-specific 30m event timing data
- replay attempted: `yes`
- reason direct validation remains insufficient: Daily proxy replay ran on AMZN but produced sample_size=0 after trigger/regime filtering; 85 triggered samples were filtered. Event-reaction mechanism is unlikely to be resolved by generic daily-bar proxy alone.
- minimum next action: Attach event calendar/earnings/news metadata and 30m intraday data for the equity universe; implement an event-aware replay trigger instead of generic daily-bar proxy logic.
- daily data can resolve it: no
- intraday data required: yes, plus event metadata

### `ptc_backtest_final_4df2e8e80685a054`

- mechanism/regime: `REVERSAL` / `TRENDING`
- attributed symbols: `AAPL; AMZN; BAC; JPM; META; MSFT; TSLA`
- timeframe: `1h`
- full coverage status: PARTIAL_COVERAGE daily symbols 5/7
- missing data if any: AAPL; JPM daily and 1h files missing
- replay attempted: `yes`
- reason direct validation remains insufficient: Partial direct data only. Direct replay produced BACKTEST_SUPPORTED on BAC with sample_size=62, expectancy=0.004254, and profit_factor=1.547891, but classification remains INSUFFICIENT_DATA because attributed universe coverage is incomplete.
- minimum next action: Acquire AAPL and JPM adjusted daily files to complete universe coverage; then add 1h intraday files for rule-specific reversal validation.
- daily data can resolve it: yes, likely for direct coverage/classification if validation logic accepts daily replay
- intraday data required: yes, for mechanism/timeframe validation

### `ptc_backtest_final_7d839944a8a4070a`

- mechanism/regime: `BREAKOUT` / `CHOP`
- attributed symbols: `TLT; USO`
- timeframe: `15m`
- full coverage status: NO_COVERAGE daily symbols 0/2
- missing data if any: TLT; USO daily and 15m files missing
- replay attempted: `no`
- reason direct validation remains insufficient: No local direct CSV data found for resolved symbols, so direct replay did not run.
- minimum next action: Acquire TLT and USO adjusted daily files first; add 15m intraday bars for rule-specific breakout validation.
- daily data can resolve it: yes, for first-pass direct replay coverage
- intraday data required: yes, for mechanism/timeframe validation

### `ptc_backtest_final_b23c6756bfb3263a`

- mechanism/regime: `BREAKOUT` / `CHOP`
- attributed symbols: `BAC; GOOGL; META; MSFT; NFLX; TSLA`
- timeframe: `30m`
- full coverage status: FULL_COVERAGE daily symbols 6/6 after GOOGL unlock; no 30m intraday coverage evidenced
- missing data if any: No missing daily symbols; missing rule-specific 30m intraday data/trigger evidence
- replay attempted: `yes`
- reason direct validation remains insufficient: GOOGL completed daily coverage, but direct validation remained unchanged. Daily proxy replay ran on BAC and produced sample_size=0 after trigger/regime filtering; 98 triggered samples were filtered.
- minimum next action: Acquire/attach 30m intraday data for BAC, GOOGL, META, MSFT, NFLX, and TSLA or refine the breakout trigger/regime filter across the full universe.
- daily data can resolve it: no; daily coverage is already complete
- intraday data required: yes

## Prioritized Remediation List

### 1. Daily-Data Resolvable

- `ptc_backtest_final_4df2e8e80685a054`: acquire `AAPL` and `JPM` adjusted daily files. This candidate already showed `BACKTEST_SUPPORTED` on `BAC`, but remains blocked by partial universe coverage.
- `ptc_backtest_final_7d839944a8a4070a`: acquire `TLT` and `USO` adjusted daily files to allow first-pass direct replay.
- `ptc_backtest_final_624fdd85668e2c08`: acquire `DBC`, `TLT`, and `USO` adjusted daily files to allow first-pass direct replay.

### 2. Intraday-Required

- `ptc_backtest_final_469607b8340421b7`: needs `30m` validation for `DIA`, `QQQ`, and `SPY`; daily replay produced zero post-filter samples.
- `ptc_backtest_final_3a4ac24107c77136`: needs `5m` validation for `DIA`, `QQQ`, and `SPY`; daily replay produced zero post-filter samples.
- `ptc_backtest_final_b23c6756bfb3263a`: needs `30m` validation for `BAC`, `GOOGL`, `META`, `MSFT`, `NFLX`, and `TSLA`; daily coverage is complete but zero samples survived filtering.
- `ptc_backtest_final_4df2e8e80685a054`: after daily `AAPL` and `JPM`, add `1h` validation for rule-specific reversal testing.
- `ptc_backtest_final_7d839944a8a4070a`: after daily `TLT` and `USO`, add `15m` validation for rule-specific breakout testing.
- `ptc_backtest_final_624fdd85668e2c08`: after daily `DBC`, `TLT`, and `USO`, add `30m` validation for rule-specific mean-reversion testing.

### 3. Event-Metadata-Required

- `ptc_backtest_final_d5931b24bd391113`: event-reaction candidate has full daily symbol coverage but needs event calendar/news/earnings timing plus `30m` intraday data and an event-aware replay trigger.

### 4. Replay-Logic-Required

- Full-coverage candidates with zero post-filter daily samples need replay logic review: `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_d5931b24bd391113`, and `ptc_backtest_final_b23c6756bfb3263a`.
- Common validator warnings indicate generic observation text was converted into deterministic daily-bar proxy triggers, then filtered by regime constraints. That can erase samples even when local data exists.
- Direct result output also reports one symbol per candidate, so universe-level aggregation remains a limitation for candidates whose evidence should span multiple attributed symbols.

### 5. Should Remain Blocked

- `ptc_backtest_final_d5931b24bd391113` should remain blocked from stronger validation claims until event metadata exists; daily-only evidence is not enough for an event-reaction mechanism.
- `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_3a4ac24107c77136`, and `ptc_backtest_final_b23c6756bfb3263a` should remain blocked from confirmation until intraday or replay-trigger changes produce enough direct samples.
- `ptc_backtest_final_624fdd85668e2c08`, `ptc_backtest_final_4df2e8e80685a054`, and `ptc_backtest_final_7d839944a8a4070a` should remain blocked until missing-symbol coverage is complete.

## Conclusion

The remaining insufficiencies are not one uniform data gap. The highest-leverage next daily acquisitions are `AAPL`, `JPM`, `TLT`, `USO`, and `DBC`; these can reduce pure coverage blocks. Separately, the already-covered candidates need intraday data, event metadata, and replay-trigger refinement because daily proxy replay is producing zero usable direct samples after filtering.

No candidate promotion, replay override, qualification override, governance change, trading recommendation, capital authority, broker execution, position sizing, portfolio construction, automatic paper trade placement, or automatic memory write was added.
