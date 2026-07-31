# Validation Fidelity Master Plan 001

Date: 2026-06-05
Status: ANALYSIS_ONLY
Authority: GENERATED_ONLY

## Authority Boundaries

This plan does not implement remediation. It does not change replay logic, qualification scoring, candidate status, governance, trading authority, broker execution, capital authority, position sizing, or paper placement. All expected gains below are planning estimates or diagnostic simulation results, not production validation outcomes.

## Inputs Reviewed

- `replay_limitation_ledger_001.md`
- `replay_limitation_concentration_001.md`
- `replay_sample_yield_failure_analysis_001.md`
- `candidate_quality_audit_001.md`
- `vocabulary_bridge_effectiveness_001.md`
- `intraday_event_evidence_readiness_pack_001.md`
- `sample_size_recheck_program_001.md`
- `candidate_quality_scoreboard_002.md`

## 1. Current Validation State

Atlas validation fidelity has improved at the infrastructure level, but candidate-specific validation still has material evidence gaps.

| Measure | Current state |
|---|---:|
| Total candidates in scoreboard | 600 |
| Final eligible candidates | 110 |
| Final qualification failures | 490 |
| Paper-forward-ready candidates | 58 |
| Focused direct validations | 8 |
| Focused confirmed candidates | 2 |
| Focused insufficient-data candidates | 6 |
| Focused zero post-filter sample candidates | 5 |
| Replay-limited ledger rows | 21 |
| Global replay sample yield | 88.9178% |
| Proxy-dependent candidates | 600 |
| Intraday/daily mismatch penalties | 180 |
| Sample-size or missing-evidence penalties | 156 |

The central distinction is that global replay yield is healthy, while candidate-specific replay fidelity is not. The strongest failure pattern is not missing daily CSV coverage; it is the mismatch between candidate hypotheses, required timeframes, proxy data, regime vocabulary, and evidence requirements.

## 2. Main Validation Failure Classes

| Failure class | Evidence | Candidate impact | Interpretation |
|---|---:|---:|---|
| Replay logic limitation | Replay limitation ledger | 11 ledger rows | Candidate-specific replay does not expose enough stage-level explanation for why support is lost. |
| Regime filter / vocabulary mismatch | Replay ledger and vocabulary bridge study | 4 primary ledger rows; 5 CHOP affected candidates | Exact candidate-regime labels do not consistently map to emitted validation regimes. |
| Proxy dependence | Scoreboard and quality audit | 600 candidates | All candidates remain exposed to proxy penalties; this is a broad fidelity suppressor. |
| Intraday requirements | Concentration study and readiness pack | 180 penalty rows; 4 high-value intraday candidates; 3 deferred intraday candidates | Many hypotheses are intraday-attributed but validated through daily proxies. |
| Event metadata requirements | Readiness pack and quality audit | 1 focused event candidate; 2 materialized event flags | Event-reaction hypotheses require event records, not only price bars. |
| Sample-size limitation | Sample-size recheck program | 14 recheck candidates; 2 ready for recheck | Some failures may become evaluable with modest additional samples, but most are replay-limited. |
| Qualification threshold failures | Scoreboard and candidate quality audit | 490 failures; 216 backtest-weak floor | Some failures reflect poor candidates, while a large share reflects evidence-readiness debt. |

## 3. Candidate Impact By Failure Class

### Replay Limitations

The replay limitation ledger contains 21 replay-limited rows:

| Replay class | Count | Candidate impact |
|---|---:|---|
| REPLAY_LOGIC | 11 | `ptc_backtest_final_00ddd14158f5530b`, `ptc_backtest_final_02630f234d5edbab`, `ptc_backtest_final_05e4c8563adf27d6`, `ptc_backtest_final_0df2fa30ffcbc127`, `ptc_backtest_final_0e47e54d68df23b2`, `ptc_backtest_final_0ea60d559ba9d153`, `ptc_backtest_final_11943bdfd5c2bfee`, `ptc_backtest_final_14143f615c830af7`, `ptc_backtest_final_152adfbd23003578`, `ptc_backtest_final_158689d9e7ef68a5`, `ptc_backtest_final_199c01b1a6e78de1` |
| REGIME_FILTER | 4 | `ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_7d839944a8a4070a`, `ptc_backtest_final_b23c6756bfb3263a` |
| LOW_SAMPLE_DENSITY | 3 | `ptc_backtest_final_0a75270e4aced70b`, `ptc_backtest_final_133dce459bccef18`, `ptc_backtest_final_167bf746a7e16482` |
| PROXY_MISMATCH | 2 | `ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_624fdd85668e2c08` |
| EVENT_METADATA | 1 | `ptc_backtest_final_d5931b24bd391113` |

Primary need: preserve baseline replay behavior while adding enough diagnostic lineage to distinguish true rejections from evidence-path failures.

### Vocabulary Mismatch

Five focused direct-validation candidates are affected by CHOP vocabulary mismatch. Baseline direct validation leaves all five blocked or insufficient. Diagnostic simulation of the highest-confidence bridge, `CHOP -> RANGE_BOUND`, retained 396 additional samples, improved five candidates, made three candidates evaluable, and simulated two additional confirmations.

This is a material fidelity opportunity, but it carries medium false-positive risk because `CHOP` is only a partial semantic match to `RANGE_BOUND`. Any bridge should remain evaluation-only until validated against independent evidence.

### Intraday Requirements

The readiness pack identifies four high-value intraday candidates:

- `ptc_backtest_final_469607b8340421b7`
- `ptc_backtest_final_3a4ac24107c77136`
- `ptc_backtest_final_854ad10b904e1ae9`
- `ptc_backtest_final_b23c6756bfb3263a`

Three additional candidates are deferred because daily prerequisites or coverage quality need cleanup before intraday work is meaningful:

- `ptc_backtest_final_624fdd85668e2c08`
- `ptc_backtest_final_4df2e8e80685a054`
- `ptc_backtest_final_7d839944a8a4070a`

The main fidelity gap is timeframe mismatch: intraday-attributed hypotheses are being evaluated through deterministic daily-bar proxy triggers.

### Event Metadata Requirements

`ptc_backtest_final_d5931b24bd391113` is the focused event-required candidate. It is an event-reaction / CHOP candidate with zero baseline samples and separate event metadata needs. Price bars alone cannot validate the event trigger semantics.

Expected impact is narrower than vocabulary or intraday work, but this path is necessary to avoid false rejection of event-reaction hypotheses.

### Sample-Size Limitations

The sample-size recheck program identifies 14 candidates:

| Class | Count | Impact |
|---|---:|---|
| READY_FOR_RECHECK | 2 | `ptc_backtest_final_0a75270e4aced70b`, `ptc_backtest_final_167bf746a7e16482` |
| MORE_DATA_REQUIRED | 1 | `ptc_backtest_final_133dce459bccef18` |
| REPLAY_LIMITATION | 11 | Requires mechanism-specific replay attribution before recheck is meaningful. |

Two candidates are near the 30-sample minimum at 26/30 and need four additional post-filter samples each. These are the clearest low-complexity insufficient-data reduction opportunities.

### Proxy Dependence

Proxy dependence is universal across the 600-candidate scoreboard. It also appears as a concrete replay limitation for two ledger candidates. This does not mean every candidate is invalid; it means validation confidence is systematically capped until direct evidence lineage improves.

Priority should be on evidence lineage, symbol-level attribution, and explicit proxy labels in reports, not on changing replay outcomes.

### Qualification Threshold Failures

There are 490 final qualification failures. The candidate quality audit splits materialized failures into 28 true rejections and 28 validation limitations. The scoreboard also reports a 216-candidate backtest-weak floor.

The correct interpretation is mixed: some candidates are poor, but many failures are not yet clean rejections because evidence fidelity is insufficient.

## 4. Highest-Value Remediation Paths

These paths are ranked for validation fidelity, not for candidate promotion.

| Rank | Path | Validation unblock potential | Complexity | Authority risk | Expected evidence gain |
|---:|---|---|---|---|---|
| 1 | Add stage-level replay telemetry and precise insufficiency reason codes | High across 21 replay-limited rows | Medium | Low if read-only | Separates true rejection from replay-path failure. |
| 2 | Keep evaluating the diagnostic vocabulary bridge with side-by-side baseline results | High for 5 CHOP candidates | Low-Medium | Medium | 396 simulated retained samples; 3 evaluable; 2 simulated confirmations. |
| 3 | Improve proxy lineage, symbol-level evidence summaries, and universe aggregation diagnostics | Very high across 600 proxy-dependent candidates | Medium | Low | Reduces ambiguity without changing replay outcomes. |
| 4 | Build a matched-timeframe intraday evidence plan for high-value candidates | High for 4 high-value and 3 deferred intraday candidates | Medium-High | Low while evidence-only | Tests whether daily proxy failures are timeframe artifacts. |
| 5 | Run sample-size recheck and event-readiness tracks as bounded evidence programs | Medium | Low for sample recheck; High for event metadata | Low-Medium | Two near-threshold candidates can become evaluable; one event candidate can become properly testable. |

## 5. Expected Confirmed-Candidate Gain

Measured production-confirmed gain from this plan is zero because this report makes no production changes.

Diagnostic and planning estimates:

| Source | Expected gain type | Estimate |
|---|---|---:|
| Vocabulary bridge simulation | Additional simulated confirmed candidates | 2 |
| Vocabulary bridge simulation | Additional evaluable candidates | 3 |
| Sample-size recheck | Candidates that could move from insufficient-data to evaluable | 2 |
| Intraday evidence readiness | Candidates that could become properly testable | 4 high-value; 3 deferred |
| Event metadata readiness | Event candidates that could become properly testable | 1 |

Conservative conclusion: the only quantified simulated confirmed-candidate gain is +2 from the vocabulary bridge overlay. All other paths should be counted as evidence gain or evaluability gain until validated.

## 6. Expected Insufficient-Data Reduction

Near-term insufficient-data reduction is most defensible in two areas:

| Path | Insufficient-data reduction estimate | Basis |
|---|---:|---|
| Vocabulary bridge diagnostics | 2 to 3 candidates | Five affected CHOP candidates improve; two simulate confirmed and three become evaluable in the bridge analysis. |
| Sample-size recheck | 2 candidates | Two candidates are at 26/30 and ready for recheck. |
| Intraday matched-timeframe evidence | 0 to 4 candidates initially | Four high-value candidates need matched intraday evidence before resolution can be claimed. |
| Event metadata | 0 to 1 candidate | One event-reaction candidate requires event records. |

Planning estimate: 4 to 6 candidates have the clearest path from insufficient-data toward evaluability, but confirmed status should not be projected without new evidence.

## 7. Complexity / Risk Ranking

| Rank | Action family | Complexity | Risk | Notes |
|---:|---|---|---|---|
| 1 | Reporting telemetry and insufficiency reason codes | Medium | Low | Read-only and improves diagnosis across replay-limited cases. |
| 2 | Sample-size recheck for near-threshold candidates | Low-Medium | Low | Bounded and evidence-only. |
| 3 | Vocabulary bridge evaluation overlay | Low-Medium | Medium | Material simulated benefit, but partial semantic mapping can create false positives. |
| 4 | Proxy lineage and symbol-level diagnostics | Medium | Low | Broadly useful and does not require replay changes. |
| 5 | Intraday evidence readiness | Medium-High | Low if evidence-only | High value, but data volume and timeframe consistency are costly. |
| 6 | Event metadata readiness | High | Medium | Narrow candidate count but required for event-reaction validity. |

## 8. Top 5 Validation Fidelity Actions

1. Add read-only stage-level replay telemetry with sample-count checkpoints and precise insufficiency reason codes.
2. Continue diagnostic-only vocabulary bridge evaluation, prioritizing `CHOP -> RANGE_BOUND` while preserving exact baseline results.
3. Add proxy lineage, symbol-level evidence summaries, and universe aggregation diagnostics to validation reports.
4. Prepare matched-timeframe intraday evidence plans for the high-value intraday cluster before judging those candidates.
5. Run bounded sample-size recheck for the two near-threshold candidates and define event metadata requirements for the event-reaction candidate.

## Final Authority Boundary

This master plan is non-authoritative. It does not promote candidates, override replay, relax qualification thresholds, change governance, recommend trades, authorize capital, connect brokers, define position sizing, or place paper orders. Any future remediation must be explicitly requested and separately verified.
