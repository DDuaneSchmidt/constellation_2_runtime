# Candidate Quality Scoreboard 002

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: validation-focused anomaly-quality scoreboard. This report uses existing artifacts only. It makes no implementation changes, does not rerun replay, does not alter validation, does not change qualification, does not modify candidate state, does not change governance, does not recommend trades, does not allocate capital, does not size positions, does not authorize broker execution, and does not place paper trades.

## Question

Is Atlas improving anomaly quality or just reducing infrastructure friction?

Short answer: `IMPROVING_INFRASTRUCTURE_FRICTION_MORE_THAN PROVEN_ANOMALY_QUALITY`.

Atlas is improving validation infrastructure and removing friction: daily symbol coverage is complete for the focused direct-validation set, direct replay runs for 8/8 focused candidates, and 2 candidates are directly confirmed. But durable anomaly-quality evidence remains early: 6/8 focused candidates are still insufficient-data, 600/600 evaluated candidates still carry proxy dependence, 372/600 remain replay-gap candidates, 490/600 fail final qualification, and only 2 paper-forward outcomes exist.

## Inputs Reviewed

- `research_journal/reports/candidate_survival_scorecard_001.md`
- `research_journal/reports/qualification_failure_analysis_001.md`
- `research_journal/reports/proxy_dependence_reduction_plan_001.md`
- `research_journal/reports/vocabulary_bridge_effectiveness_001.md`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest_summary.md`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/paper_forward_outcomes/latest.json`

## Count Definitions

The Atlas artifacts expose different candidate populations:

- `total candidates`: 600 candidates evaluated by backtest-aware final qualification.
- `materialized candidates`: 120 candidate-level rows visible in final ranking artifacts: 20 top robust candidates plus 100 excluded-candidate preview rows. The 8 focused campaign candidates are a subset of the top/campaign preview, not an additional 8.
- `focused direct-validation candidates`: 8 campaign candidates with direct validation rows.

## Scoreboard

| Metric | Count | Source | Validation-Quality Reading |
| --- | ---: | --- | --- |
| Total candidates | 600 | backtest-aware final qualification | Large candidate universe; not quality proof by itself. |
| Materialized candidate-level rows | 120 | final candidate ranking | Enough for preview diagnostics, not full-population candidate-level auditing. |
| Final eligible candidates | 110 | backtest-aware final qualification | Intermediate quality gate; 18.3% of 600. |
| Paper-forward-ready candidates | 58 | final candidate ranking | Human-review observation candidates; not outcome evidence. |
| Focused direct-validation candidates | 8 | direct candidate data validation | Current strongest validation-quality sample. |
| Confirmed candidates | 2 | direct candidate data validation | Direct evidence improvement; 25.0% of focused validation set. |
| Weakened candidates | 0 | direct candidate data validation | No direct `WEAKENED` classifications yet. |
| Insufficient-data candidates | 6 | direct candidate data validation | Main focused validation blocker; 75.0% of focused set. |
| True rejections / major redesign candidates | 216 floor | qualification failure analysis | `BACKTEST_WEAK` is the clearest true-rejection or major-redesign class. |
| Final qualification failures | 490 | qualification failure analysis | Mixed poor candidates and poor validation; not all are true rejections. |
| Validation limitations | 600 proxy penalties; 180 intraday/daily mismatch; 156 sample-size/missing-evidence penalties | qualification failure analysis | Validation fidelity remains the dominant quality limiter. |
| Evidence gaps | 156 insufficient-data / missing-evidence class; 6 focused direct insufficient-data cases | qualification and direct validation | Candidate quality cannot be cleanly separated from evidence insufficiency yet. |
| Vocabulary gaps | 5 affected CHOP candidates; 5 baseline blocked; 396 bridge-recoverable samples | vocabulary bridge effectiveness | Major semantic validation gap; bridge helps but remains approximate. |
| Intraday-required candidates | 6 focused candidates requiring intraday/event data for stronger evidence | proxy dependence reduction plan | Daily data coverage reduces friction but does not prove intraday anomalies. |
| Event-required candidates | 1 focused candidate | proxy dependence reduction plan; event metadata requirements | Event reaction cannot be validated with daily bars alone. |
| Paper-forward outcomes | 2 total: 1 survived, 1 weakened | paper-forward outcomes | Outcome-quality evidence remains too small for durable anomaly-quality claims. |

## Validation Ratios

| Ratio | Value | Interpretation |
| --- | ---: | --- |
| Final eligible / total candidates | 18.3% | Candidate filter is selective, but still proxy-penalized. |
| Paper-forward-ready / total candidates | 9.7% | Narrower observation-readiness surface. |
| Focused direct validations / total candidates | 1.3% | Only a small subset has direct validation rows. |
| Confirmed / focused direct validations | 25.0% | Real quality signal, but sample is small. |
| Insufficient-data / focused direct validations | 75.0% | Validation still dominates the direct sample. |
| Backtest-supported final pass rate | 48.25% | 110 of 228 supported candidates survive final qualification. |
| Final qualification failure rate | 81.7% | 490 of 600 fail at the 0.70 threshold. |
| Proxy dependence rate | 100.0% | All 600 evaluated candidates carry proxy penalty. |
| Replay-gap rate | 62.0% | 372 of 600 are `BACKTEST_WEAK` or `INSUFFICIENT_DATA`. |
| Paper-forward survivor rate among recorded outcomes | 50.0% | 1 of 2, too small to generalize. |

## Validation Quality Signals

### Positive Signals

- Direct validation now runs for all 8 focused candidates; missing CSV count is `0`.
- Direct validation confirms 2 candidates:
  - `ptc_backtest_final_854ad10b904e1ae9`
  - `ptc_backtest_final_4df2e8e80685a054`
- The confirmed direct results are not simply volume: they compare proxy and direct metrics and preserve direct sample sizes.
- Vocabulary bridge simulation shows a plausible route to recover validation signal in CHOP candidates: `396` additional samples, `5` candidates with sample improvement, `3` additional candidates evaluable, and `2` simulated confirmations.
- The qualification failure analysis separates fixable validation gaps from likely true weaknesses rather than treating all failures as bad candidates.

### Negative Or Unresolved Signals

- 6 of 8 focused direct validations are still `INSUFFICIENT_DATA`.
- 5 CHOP candidates remain baseline blocked under exact vocabulary matching.
- The vocabulary bridge has `MEDIUM` false-positive risk and cannot be treated as exact validation.
- 600 of 600 candidates remain proxy-dependent at qualification scale.
- 180 candidates carry intraday/daily mismatch penalties.
- 156 candidates carry sample-size, insufficient-data, or missing-evidence penalties.
- 216 candidates are `BACKTEST_WEAK`, the clearest true-rejection or major-redesign class.
- 490 candidates fail final qualification, but the causes are mixed.
- Paper-forward outcome evidence remains 2 records total.

## Infrastructure Friction Versus Anomaly Quality

| Dimension | Improving? | Evidence | Quality Interpretation |
| --- | --- | --- | --- |
| Daily data availability | YES | Focused direct validation has `missing_csvs=[]`; market coverage reached full focused coverage in prior reports. | Infrastructure friction reduced. |
| Direct replay execution | YES | Direct replays run for `8/8` focused candidates. | Infrastructure friction reduced. |
| Direct confirmation | PARTIAL | `2/8` focused candidates confirmed. | Early anomaly-quality improvement, not yet broad. |
| Direct rejection quality | NOT YET | `0` direct `WEAKENED`; 6 insufficient-data. | Direct validation is not yet cleanly rejecting weak anomalies. |
| Proxy retirement | MOSTLY NO | Proxy debt remains `600`; 2 confirmed candidates are resolvable at evidence-label layer. | Infrastructure progress has not propagated to qualification-scale evidence. |
| Replay gap reduction | NOT YET | `372/600` replay gaps remain. | Quality bottleneck remains. |
| Vocabulary gap reduction | DIAGNOSTIC ONLY | Bridge simulation recovers samples but is not implemented or authoritative. | Potential quality improvement, still approximation evidence. |
| Intraday/event evidence | NOT YET | 6 focused candidates need intraday/event data; 1 needs event metadata. | Infrastructure friction remains for stricter validation. |
| Paper-forward outcomes | NOT ENOUGH | 2 outcomes total. | Outcome-quality proof is underpowered. |

## True Rejection Readout

Atlas is beginning to separate true rejections from validation limitations, but the separation is incomplete.

Evidence supporting true rejection or major redesign:

- `216` candidates classified `BACKTEST_WEAK`.
- `348` candidates have fragile drawdown weakness in qualification analysis.
- One focused direct candidate, `ptc_backtest_final_624fdd85668e2c08`, has direct daily replay evidence that is weak: direct expectancy `-0.000523`, profit factor `0.943765`, sample size `36`.

Evidence that many apparent failures are still validation limitations:

- `600` proxy penalties.
- `180` intraday/daily mismatch penalties.
- `156` insufficient-data/sample-size/missing-evidence penalties.
- `5` CHOP candidates zeroed by vocabulary mismatch under exact matching.
- `6` focused candidates remain direct `INSUFFICIENT_DATA`, not confirmed weak or rejected.

Conclusion: Atlas has a credible rejection signal for some candidates, but many failures are still poor-validation cases rather than proven poor anomalies.

## Scoreboard 001 -> 002 Change

| Area | Scoreboard 001 Read | Scoreboard 002 Read |
| --- | --- | --- |
| Main question | Candidate survival and volume-to-quality funnel. | Validation quality, not volume. |
| Confirmed candidates | 2 direct confirmations. | Still 2; quality proof has not expanded. |
| Insufficient-data candidates | 6 focused direct cases. | Still 6; now framed as validation limitation. |
| Proxy dependence | Known as all-candidate risk. | Central quality blocker: 600/600. |
| Vocabulary gap | Not central. | Central CHOP validation blocker with bridge simulation evidence. |
| True rejection | Mixed in final qualification. | Backtest-weak class treated as true-rejection floor, not all 490 failures. |
| Paper-forward outcomes | 2 outcomes, 1 survived. | Still underpowered for durable anomaly-quality claims. |

## Answer

Atlas is improving infrastructure friction more clearly than it is proving anomaly quality.

The strongest improvement is validation plumbing:

- direct data exists for all 8 focused candidates;
- direct replay runs for all 8;
- missing daily CSVs are no longer the focused blocker;
- proxy warnings can now be partially retired for some focused candidates;
- vocabulary bridge diagnostics show a path to recover samples.

The anomaly-quality improvement is real but narrow:

- 2 focused candidates are directly confirmed;
- 1 focused candidate has weak direct evidence;
- 216 broader candidates are likely true rejection or major redesign candidates through `BACKTEST_WEAK`;
- 58 candidates are paper-forward-ready, but that is still observation readiness, not outcome success.

The durable quality claim remains underpowered:

- 6/8 focused candidates are still insufficient-data;
- 600/600 candidates remain proxy-dependent in qualification;
- 372/600 remain replay-gap candidates;
- 490/600 fail final qualification;
- paper-forward outcomes are only 2 total.

Final judgment:

```text
Atlas is not merely producing volume anymore; it is reducing validation friction and beginning to surface direct anomaly-quality signal.
But the dominant measured improvement is infrastructure friction reduction, not yet broad durable anomaly-quality improvement.
```

## Next Scoreboard Requirements

The next scoreboard should track:

- direct-daily-confirmed candidates after stale proxy labels are cleaned;
- bridge-derived candidates separately from exact-validation candidates;
- intraday-confirmed candidates separately from daily-confirmed candidates;
- event-replay-ready candidates separately from event-confirmed candidates;
- true direct `WEAKENED` or rejected candidates;
- paper-forward outcomes linked to the exact candidate ids in the current campaign;
- qualification-scale proxy penalty reduction after evidence-source reclassification.

## Authority Boundary

This scorecard is analysis only. It does not implement software, modify Atlas or Aegis behavior, modify manifests, alter verified runtime truth, promote candidates, reject candidates, override replay, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, write memory automatically, or place paper trades.
