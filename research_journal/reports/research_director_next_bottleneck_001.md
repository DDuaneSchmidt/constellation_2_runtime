# Research Director Next Bottleneck 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: non-authoritative Research Director work recommendation. No workflow authority, no production integration, no backlog mutation, no candidate changes, no replay changes, no qualification changes, no governance changes, no paper-forward changes, no memory writes, no trade recommendation, no capital authority, no broker execution, and no position sizing.

## Question

What should Research Director recommend as the next non-authoritative workstream?

Short answer: prioritize work that converts the current post-data-coverage plateau into candidate-specific evidence and explicit blocker attribution.

## Inputs Reviewed

- `research_journal/reports/research_director_service_hardening_001.md`
- `research_journal/reports/research_debt_trend_001.md`
- `research_journal/reports/vocabulary_bridge_effectiveness_001.md`
- `research_journal/reports/intraday_dependency_inventory_001.md`
- `research_journal/reports/event_metadata_requirements_001.md`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest_summary.md`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest_summary.md`
- `reports/atlas_v2_research_os/research_adversary_corpus/corpus_summary.md`
- `reports/atlas_v2_research_os/research_adversary_evaluation/latest/research_adversary_evaluation_summary.md`
- `reports/atlas_v2_research_os/candidate_review/latest_summary.md`

## Compared Bottlenecks

| Bottleneck | Current Evidence | Research Director Read |
| --- | --- | --- |
| Qualification failures | `490/600` candidates below final score `0.7`; `110/228` backtest-supported candidates pass final qualification. | High-mass suppressor, but needs decomposition into repairable evidence gaps versus true rejection. |
| Proxy dependence | `600/600` final-ranking candidates carry proxy dependence; final ranking says selected candidates still depend on SPY daily proxy evidence until candidate-specific data is supplied. | Largest unchanged debt class; not solvable by another summary report alone. |
| Replay gaps | `372/600` candidates are `BACKTEST_WEAK` or `INSUFFICIENT_DATA`; sample-size penalty count `156`; intraday/daily mismatch penalty count `180`. | Directly blocks candidate-quality interpretation and feeds qualification failures. |
| Vocabulary bridge | `CHOP -> RANGE_BOUND` simulation recovers `396` samples, improves `5` candidates, makes `3` candidates evaluable, and simulates `2` additional confirmations. | High near-term diagnostic value, but bridge evidence is approximate and false-positive risk is `MEDIUM`. |
| Intraday/event requirements | All `8` focused direct-validation candidates require intraday data; `15` intraday-required symbols missing; event-reaction candidate needs 30m bars plus timestamped event metadata. | Highest candidate-specific evidence gain, but complexity is high. |
| Human review gap | `150` adversary reviews, `750` assumptions, `1,070` constraints, `600` falsification tests; `0` evaluated cases; recommendation `HOLD`. | Important for service hardening, but lower immediate candidate-quality impact than evidence-production blockers. |

## Top 3 Recommendations

| Rank | Recommended Workstream | Expected Information Gain | Expected Candidate-Quality Impact | Implementation Complexity | Authority Risk |
| ---: | --- | --- | --- | --- | --- |
| 1 | Qualification Failure Attribution Ledger | Very high | High | Medium | Low |
| 2 | Targeted Intraday/Event Evidence Readiness Pack | Very high | Very high | High | Low-medium |
| 3 | Vocabulary Bridge Diagnostic Refinement | High | Medium-high | Medium | Medium |

## 1. Qualification Failure Attribution Ledger

Recommendation: create a non-authoritative ledger that decomposes `490` qualification failures, `372` replay gaps, `600` proxy penalties, `180` intraday/daily mismatch penalties, and `156` sample-size penalties into explicit next-state categories:

- evidence repair
- intraday/event requirement
- vocabulary bridge candidate
- true weak replay
- proxy-only limitation
- human review needed
- reject-for-now

Why this ranks first:

- It has the best information-gain-to-complexity ratio.
- It prevents Research Director from confusing high-volume debt with one uniform bottleneck.
- It turns qualification failures into actionable research classes without changing qualification.
- It can identify whether intraday/event work, vocabulary bridge work, or human review should receive the next scarce effort.

Expected information gain: `Very high`.

Expected candidate-quality impact: `High`, because it separates candidates that need better evidence from candidates that are probably weak.

Implementation complexity: `Medium`, because it is a read-only attribution/reporting layer over existing qualification, replay, final ranking, and validation outputs.

Authority risk: `Low`, if the ledger preserves current classifications and does not modify qualification, ranking, candidate state, or paper-forward state.

Non-authoritative output expected:

- A ranked table of failure categories by candidate count.
- Candidate examples for each category.
- A recommended evidence path per category.
- Explicit "no classification change" language.

## 2. Targeted Intraday/Event Evidence Readiness Pack

Recommendation: create a read-only readiness pack for targeted intraday/event evidence, focused first on the current campaign candidates rather than the whole search space.

Primary targets:

- DIA, QQQ, SPY at `5m` and `30m` for the top BREAKOUT/CHOP candidates.
- AMZN, BAC, META, MSFT, NFLX, TSLA at `30m` plus timestamped event metadata for the EVENT_REACTION candidate.
- AAPL, AMZN, BAC, JPM, META, MSFT, TSLA at `1h` for the REVERSAL candidate.
- TLT, USO at `15m` and DBC/TLT/USO at `30m` for remaining rates/commodity ETF candidates.

Why this ranks second:

- Proxy dependence cannot be retired until candidate-specific evidence exists.
- All focused direct-validation candidates require intraday data.
- Event-reaction validation cannot advance with daily data alone.
- This is the most direct route to candidate-quality improvement, but it is more complex than the qualification ledger.

Expected information gain: `Very high`.

Expected candidate-quality impact: `Very high`, because it targets the main proxy, replay, and qualification suppressors.

Implementation complexity: `High`, due to intraday data quality, timestamp normalization, session calendar, event metadata, symbol-level replay, and validation alignment requirements.

Authority risk: `Low-medium`, if kept as historical data/readiness planning only. Risk rises if anyone treats readiness as validation, qualification, paper-forward approval, or trading authority.

Non-authoritative output expected:

- Symbol-timeframe manifest.
- Event metadata completeness checklist.
- Per-candidate evidence readiness matrix.
- Validation blockers by missing intraday/event component.
- Explicit "data readiness is not replay/qualification authority" language.

## 3. Vocabulary Bridge Diagnostic Refinement

Recommendation: refine the `CHOP -> RANGE_BOUND` bridge as a diagnostic-only overlay with candidate-level labels, false-positive controls, and separation from exact validation.

Why this ranks third:

- It has observed diagnostic lift: `396` additional retained samples, `5` affected candidates with sample improvement, `3` additional candidates evaluable, and `2` simulated confirmations.
- It directly attacks the current zero-sample `CHOP` vocabulary mismatch.
- It is faster than full intraday/event readiness, but less authoritative and more semantically risky.

Expected information gain: `High`.

Expected candidate-quality impact: `Medium-high`, because it may distinguish vocabulary mismatch from true candidate weakness for CHOP candidates.

Implementation complexity: `Medium`, because the crosswalk and simulations already exist, but diagnostic labels and false-positive controls need tightening.

Authority risk: `Medium`, because a bridge can be misread as a relaxed validation filter. The output must label bridge-derived results as approximation evidence, not exact validation evidence.

Non-authoritative output expected:

- Bridge-derived versus exact-filter comparison.
- Candidate-level sample retention changes.
- False-positive risk notes per candidate.
- Explicit exclusion of `LOW_VOLATILITY` and `UNKNOWN` from default CHOP bridge.
- Explicit "no replay behavior change" and "no validation authority" language.

## Why The Other Workstreams Do Not Rank Top 3

### Proxy Dependence As A Standalone Workstream

Proxy dependence is the largest debt class at `600/600`, but as a standalone label it is too broad. The actionable forms are:

- qualification failure attribution
- targeted intraday/event evidence readiness
- candidate-specific replay and symbol-level evidence

Therefore proxy dependence is treated as the main reason for the top two workstreams rather than a separate top-three item.

### Replay Gaps As A Standalone Workstream

Replay gaps are large at `372/600`, but they overlap heavily with qualification failures, proxy dependence, intraday/daily mismatch, and sample-size penalties. The first useful step is attribution, then targeted evidence readiness or vocabulary bridge refinement.

### Human Review Gap

The human review gap is real: `150` adversary reviews and `0` evaluated cases. It is important for Research Director service hardening and false-positive control, but it is less likely to improve near-term candidate quality than qualification attribution, intraday/event evidence, or vocabulary bridge diagnostics.

Recommended next placement: rank human-review scoring as the next service-hardening workstream after the top-three candidate-evidence bottlenecks.

## Scoring Rationale

| Workstream | Information Gain | Candidate-Quality Impact | Complexity | Authority Risk | Ranking Rationale |
| --- | --- | --- | --- | --- | --- |
| Qualification Failure Attribution Ledger | Very high | High | Medium | Low | Best first step after data coverage because it decomposes unchanged high-mass debt before choosing expensive remediation. |
| Targeted Intraday/Event Evidence Readiness Pack | Very high | Very high | High | Low-medium | Most direct path to retiring proxy dependence and intraday/event suppressors, but requires more infrastructure planning. |
| Vocabulary Bridge Diagnostic Refinement | High | Medium-high | Medium | Medium | Highest near-term diagnostic lift for CHOP zero-sample cases, but approximate bridge semantics need guardrails. |
| Human Review Gap Scoring | Medium-high | Medium | Medium | Low | Needed for service calibration, but less direct candidate-quality impact. |
| Proxy Dependence Standalone Review | Medium | High | Low | Low | Too broad unless decomposed into evidence requirements. |
| Replay Gaps Standalone Review | Medium | High | Low-medium | Low | Too overlapping unless tied to qualification attribution and candidate-specific evidence. |

## Final Recommendation

Research Director's next non-authoritative work recommendation is:

1. `Qualification Failure Attribution Ledger`
2. `Targeted Intraday/Event Evidence Readiness Pack`
3. `Vocabulary Bridge Diagnostic Refinement`

Decision status: `GENERATED_ONLY`.

This is a recommendation for human/evaluation review only. It does not authorize workflow execution, backlog mutation, candidate changes, replay changes, qualification changes, governance changes, paper-forward actions, memory writes, trading, capital allocation, broker execution, or position sizing.

## Authority Boundary

This report has no workflow authority. It is generated-only research guidance and must not be consumed as operational truth, candidate truth, replay truth, qualification truth, governance truth, paper-forward authority, trading authority, capital authority, broker authority, or position-sizing authority.
