# Research Hours Saved Estimate 001

Objective: estimate research effort that would have been avoided if Research Adversary reviews had been run earlier against candidate, failure, and paper-forward research items.

Scope: retrospective estimate only. This report does not modify candidates, paper-forward state, replay, qualification, governance, production pipelines, broker execution, live trading, capital allocation, or position sizing.

Inputs reviewed:
- `research_journal/reports/research_adversary_candidate_retrospective_001.md`
- `research_journal/evaluation_sets/research_adversary/historical_failure_eval_set_001.md`
- `research_journal/evaluation_sets/research_adversary/historical_failure_eval_set_001.json`
- `reports/atlas_v2_research_os/research_adversary_evaluation/latest/research_adversary_evaluation_summary.md`
- `reports/atlas_v2_research_os/research_adversary_corpus/reviews/*/research_adversary_review_summary.md`

Authority boundary:
- This is a research-process estimate, not a measured production metric.
- It does not kill, reject, promote, demote, qualify, disqualify, or paper-place any candidate.
- It does not alter historical failure records or adversary review artifacts.
- It does not authorize trade recommendations, capital recommendations, broker execution, live trading, position sizing, replay override, qualification override, or governance override.

## Estimation Method

The estimate asks whether an earlier adversary review would likely have:
- killed the item earlier
- reduced investigation scope
- accelerated rejection

Usefulness levels:
- LOW: would mostly add wording or reminders; estimated savings 0.5-1.5 hours.
- MEDIUM: would narrow review scope or force a missing check; estimated savings 2-4 hours.
- HIGH: would likely stop or materially redirect work before a larger review loop; estimated savings 4-8 hours.

Important caveat: the current Research Adversary evaluation summary reports `cases_evaluated: 0`, `reviewer_usefulness_score: 0.0`, and `estimated_research_hours_saved: 0`. Therefore this report is an estimated opportunity-cost model, not a scored validation result.

## Summary Metrics

- estimated_hours_saved: 115 hours
- estimated_hours_saved_range: 90-140 hours
- estimated_rejection_speed_improvement: median 1 review cycle faster; high-impact cases 2-3 review cycles faster
- cases_reviewed: 33
- high_usefulness_cases: 21
- medium_usefulness_cases: 10
- low_usefulness_cases: 2

## Candidate Retrospective Cases

| Case | Would adversary have killed it earlier? | Reduced investigation scope? | Accelerated rejection? | Estimate | estimated_hours_saved | estimated_rejection_speed_improvement |
| --- | --- | --- | --- | --- | ---: | --- |
| `ptc_f492d1bebb6bd47f` MEAN_REVERSION TRENDING | No | Yes | Partly | HIGH | 3.0 | 1 review cycle |
| `ptc_3d46c2d50e5fe58b` BREAKOUT TRENDING | No | Yes | Partly | HIGH | 3.0 | 1 review cycle |
| `ptc_7f054744c90194c8` VWAP_OR_AVERAGE_RECLAIM UNKNOWN | Possibly | Yes | Yes | HIGH | 4.0 | 1-2 review cycles |
| `ptc_fc801836c012a96d` EVENT_REACTION UNKNOWN | Possibly | Yes | Yes | HIGH | 4.0 | 1-2 review cycles |
| `ptc_b57c4cfe8ed94fff` SESSION_TIMING UNKNOWN | No | Yes | Partly | MEDIUM | 2.0 | 1 review cycle |
| `ptc_ffad13e2dbe09619` REVERSAL UNKNOWN | Possibly | Yes | Yes | HIGH | 3.5 | 1-2 review cycles |
| `ptc_9112fdddb80509f1` TREND_CONTINUATION UNKNOWN | Possibly | Yes | Yes | HIGH | 3.5 | 1-2 review cycles |
| `ptc_b97beffb99f141ae` OPENING_RANGE UNKNOWN | No | Yes | Partly | MEDIUM | 2.5 | 1 review cycle |
| `ptc_695a47fe74f8e36d` MEAN_REVERSION UNKNOWN | Possibly | Yes | Yes | HIGH | 3.0 | 1 review cycle |
| `ptc_78687792cbb592e8` LIQUIDITY_SWEEP UNKNOWN | Possibly | Yes | Yes | HIGH | 4.0 | 1-2 review cycles |
| `ptc_0614c2bf7a7c39b0` VOLATILITY_EXPANSION UNKNOWN | Possibly | Yes | Yes | HIGH | 4.0 | 1-2 review cycles |
| `ptc_2c34d7d9d627b7e7` BREAKOUT UNKNOWN | Possibly | Yes | Yes | HIGH | 3.0 | 1 review cycle |
| `ptc_6fe5fd4fe1dba60c` below-threshold MEAN_REVERSION | Yes | Yes | Yes | HIGH | 4.0 | 2 review cycles |

Candidate subtotal:
- estimated_hours_saved: 43.5 hours
- estimated_rejection_speed_improvement: 1 review cycle median; 2 cycles for the below-threshold candidate

Main avoided effort:
- arguing from replay-positive narrative before checking proxy dependency
- reviewing UNKNOWN-regime candidates without first requiring regime repair
- treating overlapping mechanisms as independent paper-forward opportunities
- spending paper-forward queue attention on under-specified observation rules

## Historical Failure Cases

| Case | Known failure pattern | Would adversary have killed it earlier? | Reduced investigation scope? | Accelerated rejection? | Estimate | estimated_hours_saved | estimated_rejection_speed_improvement |
| --- | --- | --- | --- | --- | --- | ---: | --- |
| `FAIL_0004` | Architecture maturity mistaken for evidence maturity | Yes | Yes | Yes | HIGH | 5.0 | 2 review cycles |
| `FAIL_0005` | Paper workflow progress mistaken for runtime readiness | Yes | Yes | Yes | HIGH | 6.0 | 2-3 review cycles |
| `FAIL_0006` | Observation accumulation mistaken for outcome evidence | Yes | Yes | Yes | HIGH | 5.0 | 2 review cycles |
| `FAIL_0007` | Candidate volume treated as quality | Possibly | Yes | Yes | HIGH | 4.0 | 1-2 review cycles |
| `FAIL_0008` | Standalone technical-indicator claims failed fixture review | Yes | Yes | Yes | HIGH | 6.0 | 2-3 review cycles |
| `FAIL_0009` | Active sleeves did not produce distributed candidate flow | No | Yes | Partly | MEDIUM | 3.0 | 1 review cycle |
| `FAIL_0010` | Research artifacts existed but no hypotheses were capital-review ready | Yes | Yes | Yes | HIGH | 6.0 | 2-3 review cycles |
| `FAIL_0011` | Legacy compatibility could not be retired by architecture decision | Possibly | Yes | Yes | HIGH | 4.0 | 1-2 review cycles |
| `FAIL_0012` | Macro readiness gaps could not be repaired by workflow expansion | Yes | Yes | Yes | HIGH | 6.0 | 2-3 review cycles |
| `FAIL_0013` | Clean audit state mistaken for mature understanding | Possibly | Yes | Yes | HIGH | 4.0 | 1-2 review cycles |
| `FAIL_0014` | Low sleeve production misdiagnosed as poor sleeve quality | No | Yes | Partly | MEDIUM | 3.0 | 1 review cycle |
| `FAIL_0015` | Recurring integrity warnings treated as noise | Possibly | Yes | Yes | HIGH | 4.0 | 1-2 review cycles |
| `FAIL_0016` | Standalone indicator evidence did not support durable factory claims | Yes | Yes | Yes | HIGH | 6.0 | 2-3 review cycles |
| `failure-demo-opening-range` | Opening-range memory had regime mismatch warning | Possibly | Yes | Yes | MEDIUM | 2.0 | 1 review cycle |
| `failure-2026-06-04-worker-no-compatible-connected-worker-c5b07de5e574` | Selected backlog item had no compatible worker | Yes | Yes | Yes | LOW | 1.0 | same-cycle rejection |
| `failure-2026-06-05-autonomous_research-artifactstoreerror-64d655a35e43` | Referenced artifact was missing | Yes | Yes | Yes | MEDIUM | 1.5 | same-cycle rejection |
| `failure-2026-06-05-autonomous_research-artifactstoreerror-c54059bb338d` | Selected backlog input artifact was unavailable | Yes | Yes | Yes | MEDIUM | 1.5 | same-cycle rejection |
| `failure-2026-06-05-autonomous_research-safety-gate-failed-794b177f0415` | Partial gate pass treated as enough despite certification failure | Yes | Yes | Yes | MEDIUM | 2.0 | same-cycle rejection |
| `failure-2026-06-05-certification-certification-block-0d8cd9475db6` | Reduced blocker count mistaken for certification pass | Yes | Yes | Yes | MEDIUM | 2.0 | same-cycle rejection |

Historical failure subtotal:
- estimated_hours_saved: 72.0 hours
- estimated_rejection_speed_improvement: 1-2 review cycles median; same-cycle rejection for simple missing-worker and missing-artifact cases

Main avoided effort:
- repeated architecture-versus-evidence debates
- interpreting paper or audit progress as authority readiness
- over-investigating standalone indicator claims before relationship-specific validation
- diagnosing sleeve quality before checking no-signal, data, conversion, and certification constraints
- retrying work with missing artifacts or missing worker compatibility

## Adversary Review Artifact Cases

Existing adversary review corpus artifacts are useful as templates and coverage scaffolding, but current scoring does not yet prove measured savings.

| Case | Would adversary have killed it earlier? | Reduced investigation scope? | Accelerated rejection? | Estimate | estimated_hours_saved | estimated_rejection_speed_improvement |
| --- | --- | --- | --- | --- | ---: | --- |
| Generated observation reviews `001-050` | No, not by themselves | Yes, for assumptions and constraints | Partly | MEDIUM | included in case estimates | 1 review cycle when tied to a human label |
| Generated claim reviews `051-100` | Possibly, for weak claims | Yes | Yes, if mapped to known failures | MEDIUM | included in case estimates | 1-2 review cycles |
| Generated hypothesis reviews `101-150` | Possibly, for under-specified hypotheses | Yes | Yes, if mapped to known failures | MEDIUM | included in case estimates | 1-2 review cycles |
| Current adversary evaluation summary | No | No | No | LOW | 0.0 | none |

Reason: the corpus is generated-only and not human-scored. Its practical savings come only when a reviewer uses the generated assumptions, constraints, and falsification prompts to stop or narrow a concrete case.

## Highest-Leverage Avoided Work

1. Standalone technical-indicator and Technical Strategy Factory claims.
   - Estimated saved effort: 12 hours across `FAIL_0008` and `FAIL_0016`.
   - Why: Research Adversary would have asked for relationship-specific support, baseline separation, fixture robustness, and regime context before broad claims consumed deeper review time.

2. Authority-readiness confusion.
   - Estimated saved effort: 18 hours across `FAIL_0005`, `FAIL_0010`, and `FAIL_0012`.
   - Why: Research Adversary would have separated paper progress, artifact richness, and workflow expansion from authority readiness.

3. Candidate paper-forward queue narrowing.
   - Estimated saved effort: 43.5 hours across 13 candidate cases.
   - Why: Earlier critique would have exposed proxy dependency, UNKNOWN regime labels, mechanism duplication, weak observation rules, and below-threshold eligibility.

4. Missing-artifact and missing-worker failures.
   - Estimated saved effort: 4 hours across three operational cases.
   - Why: These should have failed fast during input and capability checks.

## Final Estimate

Estimated hours saved: 115 hours.

Estimated rejection speed improvement:
- median: 1 review cycle faster
- high-impact research narrative cases: 2-3 review cycles faster
- missing-input and missing-worker cases: same-cycle rejection

Confidence: MEDIUM.

Rationale: the historical failure labels are clear, and the candidate retrospective identifies concrete assumptions Research Adversary should have challenged. Confidence is not HIGH because the existing adversary evaluation summary has not yet completed human-scored case evaluation.

## Recommended Measurement Upgrade

To turn this estimate into a measured metric:
- score each historical failure case as EXACT, PARTIAL, MISS, or FALSE
- require a human reviewer to mark whether the adversary output would have changed the review path
- record actual time spent per rejected or narrowed case
- separate hours saved from hours added by adversary-review overhead
- update `estimated_hours_saved` only after human scoring

This recommendation is process-only and does not change any candidate, failure, replay, qualification, governance, paper-forward, or production artifact.
