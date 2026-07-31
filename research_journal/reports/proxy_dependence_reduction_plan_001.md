# Proxy Dependence Reduction Plan 001

Date: 2026-06-05

Status: ANALYSIS_ONLY

## Scope

Reduce the remaining 600 proxy-dependent candidate problem by identifying where proxy dependence is real, where it is stale after data acquisition, and what minimum work is needed to reclassify evidence as direct.

This report uses existing artifacts only. It does not change production behavior, replay behavior, candidate state, qualification state, governance, paper-forward state, memory, trading, capital, broker execution, or position sizing.

## Sources Reviewed

- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/candidate_data_validation_plan/latest.json`
- `reports/atlas_v2_research_os/market_data_coverage_tracker/latest_summary.md`
- `reports/atlas_v2_research_os/market_data_import/latest_coverage.json`
- `reports/atlas_v2_research_os/manual_intraday_data_sourcing_pack/latest.json`
- `reports/atlas_v2_research_os/market_data_acquisition/next_unlock_validation_delta_001.md`
- `research_journal/design/atlas_direct_replay_data_strategy_001.md`
- `research_journal/reports/candidate_funnel_attrition_analysis_001.md`

## Current Proxy Debt

Proxy debt count: 600.

Evidence:

- final qualification evaluated 600 candidates
- proxy penalty count is 600
- final ranking reports `proxy_dependency: 600`
- the main remaining evidence weakness says all selected candidates still depend on SPY daily proxy evidence until candidate-specific data is supplied

Primary proxy source used:

- local SPY adjusted daily data
- deterministic daily-bar proxy trigger conversion from generic observation text

Core limitation:

SPY daily proxy evidence is not candidate-specific evidence. It can support screening, but it cannot by itself validate symbol-specific, universe-specific, intraday, event, or regime-sensitive candidate claims.

## Direct Symbol Availability

Current focused-candidate direct symbol coverage is now complete.

Market data coverage tracker:

- candidate symbol coverage: 15 of 15 required symbols
- full candidate coverage: 8 of 8 candidates
- coverage percent: 100.0%
- missing symbols: none
- validation block rate from missing daily symbols: 0.0%

This means the old blocker "candidate currently lacks direct symbol/universe data and uses SPY daily proxy evidence" is stale for the 8 focused candidates at the direct daily symbol-availability layer.

It is not fully stale at the evidence layer because:

- all 8 current focused validations still carry stale SPY proxy warnings
- all 8 still use generic observation text converted into deterministic daily-bar proxy triggers
- 5 CHOP candidates still lose all samples after regime filtering
- intraday/event mechanisms still need data and replay support beyond daily bars

## Candidate-Level Plan

| Candidate | Mechanism | Regime | Attributed symbols | Proxy source used | Direct data now exists? | Current direct result | Proxy dependence stale? | Minimum work to reclassify as direct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | BREAKOUT | CHOP | DIA, QQQ, SPY | SPY adjusted daily proxy plus generic daily trigger | Yes, daily coverage exists | `INSUFFICIENT_DATA`, sample size 0 | Partially stale | Clear stale no-symbol proxy warning, add direct-source lineage, then resolve CHOP regime filtering and acquire/use 30m intraday data before stronger direct evidence. |
| `ptc_backtest_final_3a4ac24107c77136` | BREAKOUT | CHOP | DIA, QQQ, SPY | SPY adjusted daily proxy plus generic daily trigger | Yes, daily coverage exists | `INSUFFICIENT_DATA`, sample size 0 | Partially stale | Clear stale no-symbol proxy warning, add direct-source lineage, then resolve CHOP regime filtering and acquire/use 5m intraday data before stronger direct evidence. |
| `ptc_backtest_final_624fdd85668e2c08` | MEAN_REVERSION | TRENDING | DBC, TLT, USO | SPY adjusted daily proxy plus generic daily trigger | Yes, daily coverage exists | `BACKTEST_WEAK`, sample size 36 | Stale for symbol availability, not stale for evidence quality | Reclassify source as direct daily replay, preserve weak outcome, remove stale no-symbol warning, and keep candidate weak unless direct metrics improve. |
| `ptc_backtest_final_854ad10b904e1ae9` | MEAN_REVERSION | TRENDING | DIA, QQQ | SPY adjusted daily proxy plus generic daily trigger | Yes, daily coverage exists | `CONFIRMED`, sample size 48 | Mostly stale | Reclassify as direct daily evidence after lineage/report cleanup; keep generic trigger caveat until mechanism-specific trigger templates exist. |
| `ptc_backtest_final_d5931b24bd391113` | EVENT_REACTION | CHOP | AMZN, BAC, META, MSFT, NFLX, TSLA | SPY adjusted daily proxy plus generic daily trigger | Yes, daily coverage exists | `INSUFFICIENT_DATA`, sample size 0 | Partially stale | Daily symbol proxy is stale, but event/intraday proxy dependence remains real. Requires event metadata, event-window replay, CHOP bridge/diagnostic labeling, and intraday/event timing support. |
| `ptc_backtest_final_4df2e8e80685a054` | REVERSAL | TRENDING | AAPL, AMZN, BAC, JPM, META, MSFT, TSLA | SPY adjusted daily proxy plus generic daily trigger | Yes, daily coverage exists | `CONFIRMED`, sample size 62 | Mostly stale | Reclassify as direct daily evidence after lineage/report cleanup; keep 1h/intraday caveat if the claim requires intraday reversal timing. |
| `ptc_backtest_final_7d839944a8a4070a` | BREAKOUT | CHOP | TLT, USO | SPY adjusted daily proxy plus generic daily trigger | Yes, daily coverage exists | `INSUFFICIENT_DATA`, sample size 0 | Partially stale | Clear stale no-symbol proxy warning, add direct-source lineage, then resolve CHOP regime filtering and acquire/use 15m intraday data if the breakout claim remains intraday. |
| `ptc_backtest_final_b23c6756bfb3263a` | BREAKOUT | CHOP | BAC, GOOGL, META, MSFT, NFLX, TSLA | SPY adjusted daily proxy plus generic daily trigger | Yes, daily coverage exists | `INSUFFICIENT_DATA`, sample size 0 | Partially stale | Clear stale no-symbol proxy warning, add direct-source lineage, then resolve CHOP regime filtering and acquire/use 30m intraday data before stronger direct evidence. |

## Candidates Resolvable Now

Resolvable now at the evidence-label/reporting layer: 2.

These already have direct daily data, direct replay ran, and direct validation is confirmed:

- `ptc_backtest_final_854ad10b904e1ae9`
- `ptc_backtest_final_4df2e8e80685a054`

Minimum work:

1. Remove stale "candidate plan has no symbol or universe; used SPY adjusted daily data as local proxy" language from direct evidence reports when `resolved_symbols` exist and local direct data was used.
2. Attach explicit direct data lineage: symbol list, file path or data artifact id, timeframe, date range, schema validation, and replay run id.
3. Preserve remaining caveats separately: generic trigger conversion, daily-vs-intraday limits, and sample-size limits.
4. Reclassify the evidence source from proxy-backed to direct-daily-backed, without changing candidate approval, qualification, or paper-forward state.

Resolvable now at the direct-data-run layer, but not as positive evidence: 1.

- `ptc_backtest_final_624fdd85668e2c08`

Minimum work:

1. Reclassify source from SPY proxy to direct daily replay.
2. Preserve outcome as weak: direct result is `BACKTEST_WEAK`, sample size 36, expectancy -0.000523, profit factor 0.943765.
3. Do not treat proxy-debt cleanup as candidate confirmation.

## Candidates Requiring Intraday/Event Data

Focused candidates requiring intraday/event data before stronger direct evidence: 6.

| Candidate | Required data | Reason |
| --- | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | DIA, QQQ, SPY 30m intraday | 30m breakout claim; daily data cannot prove intraday breakout behavior. |
| `ptc_backtest_final_3a4ac24107c77136` | DIA, QQQ, SPY 5m intraday | 5m breakout claim; manual intraday sourcing pack already flags DIA/QQQ/SPY 5m and 30m as required. |
| `ptc_backtest_final_d5931b24bd391113` | Event metadata plus intraday/event-window replay | EVENT_REACTION cannot be validated by daily bars alone. |
| `ptc_backtest_final_7d839944a8a4070a` | TLT, USO 15m intraday if claim remains intraday | 15m breakout claim; daily bars are only a coarse proxy. |
| `ptc_backtest_final_b23c6756bfb3263a` | BAC, GOOGL, META, MSFT, NFLX, TSLA 30m intraday | 30m breakout claim; daily bars are only a coarse proxy. |
| `ptc_backtest_final_4df2e8e80685a054` | 1h intraday if stronger reversal-timing evidence is required | Direct daily evidence is confirmed, but 1h reversal timing remains a stricter evidence requirement. |

The first two are the smallest explicit intraday acquisition lane because the manual intraday sourcing pack requires 6 files for DIA, QQQ, and SPY at 5m and 30m.

## Candidates Requiring Replay Logic Changes

Focused candidates requiring replay/reporting logic changes: 8.

All 8 focused candidates require at least stale-warning and evidence-lineage repair because direct data now exists but the validation warnings still say the plan has no symbol or universe and used SPY proxy evidence.

CHOP/regime replay logic changes are required for 5 candidates:

- `ptc_backtest_final_469607b8340421b7`
- `ptc_backtest_final_3a4ac24107c77136`
- `ptc_backtest_final_d5931b24bd391113`
- `ptc_backtest_final_7d839944a8a4070a`
- `ptc_backtest_final_b23c6756bfb3263a`

Reason:

Each has trigger samples but zero final samples after regime filtering. The daily proxy classifier emits `RANGE_BOUND`, not `CHOP`, so exact `CHOP` filtering can zero out otherwise triggered samples.

Minimum replay logic work:

1. Add an attrition ledger that records raw bars, trigger events, regime-matched events, entry-valid events, exit-valid events, and final scored samples.
2. Separate stale source warnings from live evidence warnings.
3. Record direct data lineage independently of candidate plan history.
4. Add a diagnostic-only regime translation namespace for `CHOP -> RANGE_BOUND`; do not make it validation authority.
5. Keep direct baseline and bridge-derived results side by side.
6. Add mechanism-specific trigger templates so generic daily-bar proxy triggers stop standing in for intraday or event mechanisms.

## 600-Candidate Reduction Strategy

The full 600-candidate proxy debt cannot be eliminated by changing the 8 focused candidates alone. The 8 focused candidates are the proof-of-process lane.

### Phase 1: Remove stale proxy labels from focused direct evidence

Target count:

- 8 focused candidates

Expected debt reduction:

- 2 candidates can move from proxy-dependent confirmed evidence to direct-daily-backed confirmed evidence.
- 1 candidate can move from proxy-dependent evidence to direct-daily-backed weak evidence.
- 5 candidates remain blocked by CHOP/regime, intraday, event, or sample-size issues.

### Phase 2: Expand direct attribution to ranked candidates

Target count:

- top 20 robust candidates first
- then 110 eligible candidates
- then the remaining evaluated candidates only if they remain worth evaluating

Minimum work:

- persist symbol attribution into the candidate plan/evidence object
- require direct data availability check before qualification score claims
- rerun direct replay in analysis mode only
- classify evidence as one of:
  - `DIRECT_DAILY_CONFIRMED`
  - `DIRECT_DAILY_WEAK`
  - `DIRECT_DAILY_INSUFFICIENT`
  - `DIRECT_INTRADAY_REQUIRED`
  - `EVENT_DATA_REQUIRED`
  - `PROXY_ONLY_STALE`
  - `PROXY_ONLY_ACTIVE`

### Phase 3: Retire or quarantine unresolved proxy-only candidates

Target count:

- remaining proxy-only candidates after direct attribution and data checks

Minimum work:

- mark candidates with no recoverable symbol lineage as unresolved proxy debt
- do not spend intraday/event acquisition on low-rank, duplicate, fragile, or weak candidates
- preserve them as research debt or failure-pattern examples, not direct evidence

## Output Metrics

| Metric | Count | Notes |
| --- | ---: | --- |
| proxy debt count | 600 | All evaluated candidates carried proxy penalty. |
| focused candidates with direct daily symbol coverage now | 8 | 8 of 8 focused candidates have full symbol coverage. |
| candidates resolvable now as direct confirmed evidence | 2 | `854ad...`, `4df2...`. |
| candidates resolvable now as direct weak/negative evidence | 1 | `624f...`; direct replay ran with weak result. |
| candidates requiring intraday/event data | 6 | Five for intraday-strength evidence plus the event-reaction candidate; `4df2...` only if stricter 1h timing evidence is required. |
| candidates requiring replay/reporting logic changes | 8 | All focused candidates need stale-warning/lineage cleanup; 5 need CHOP/regime handling. |
| CHOP candidates requiring regime attrition repair | 5 | All current zero-sample focused failures are CHOP candidates. |
| currently confirmed direct validations | 2 | Direct validation confirms 2 of 8. |
| current direct validation insufficient data | 6 | Six remain not reclassifiable as positive direct evidence. |

## Direct Answers

### Which candidates are proxy-dependent?

At qualification scale, all 600 evaluated candidates are proxy-dependent because all 600 carry proxy penalty.

At focused-candidate scale, all 8 focused candidates still carry proxy warnings and proxy penalty, even though direct daily symbol coverage now exists.

### Is proxy dependence stale after data acquisition?

Partially.

Stale:

- the claim that focused candidates lack direct symbol/universe data
- the claim that focused candidates can only use SPY because direct symbols are unavailable

Not stale:

- generic daily-bar proxy trigger dependence
- daily-vs-intraday approximation
- missing event metadata for event-reaction validation
- CHOP/RANGE_BOUND vocabulary mismatch
- replay/reporting inability to cleanly label direct versus proxy-derived evidence

### Minimum work to reclassify evidence as direct

Minimum non-production work:

1. Persist candidate symbol attribution into validation plans and direct validation reports.
2. Attach local direct data lineage to each direct replay.
3. Clear stale SPY proxy warnings when direct data is actually used.
4. Keep generic trigger, intraday, event, and vocabulary caveats as separate limitations.
5. Re-run analysis-only direct validation after the reporting repair.
6. Reclassify only the evidence source, not candidate authority or production state.

## Final Output

- proxy debt count: 600
- candidates resolvable now: 2 direct confirmed, plus 1 direct weak evidence cleanup
- candidates requiring intraday/event data: 6 focused candidates
- candidates requiring replay logic changes: 8 focused candidates, with 5 requiring CHOP/regime attrition repair

## Authority Boundary

This plan is diagnostic only. It does not implement replay changes, acquire data, change validation classifications, change candidate state, alter qualification, alter governance, create paper positions, recommend trades, allocate capital, authorize broker execution, size positions, or modify production workflows.
