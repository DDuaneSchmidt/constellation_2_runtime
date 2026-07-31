# Research Adversary Operational Readiness 001

Date: 2026-06-05

Status: analysis-only

Decision: `LIMITED_OPERATIONAL_USE`

## Purpose

Determine whether Research Adversary is ready to move from evaluation-only use into the standard Atlas research workflow without expanding authority.

This review uses existing evaluation results, taxonomy results, vocabulary bridge results, and governance audits. It does not implement workflow integration, change candidate state, alter replay behavior, alter qualification, change governance, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.

## Inputs Reviewed

- `reports/atlas_v2_research_os/research_adversary_evaluation/latest/research_adversary_evaluation_summary.md`
- `reports/atlas_v2_research_os/research_adversary_validation/failure_recall_study_001.md`
- `research_journal/reports/assumption_recall_study_001.md`
- `research_journal/reports/research_hours_saved_estimate_001.md`
- `research_journal/reports/research_adversary_candidate_retrospective_001.md`
- `research_journal/reports/taxonomy_recall_results_001.md`
- `research_journal/reports/taxonomy_effectiveness_study_001.md`
- `research_journal/reports/adversary_taxonomy_impact_study_001.md`
- `research_journal/reports/vocabulary_bridge_effectiveness_001.md`
- `reports/atlas_v2_research_os/validation_vocabulary_bridge/bridge_result.md`
- `research_journal/reports/research_adversary_governance_audit_001.md`

## Readiness Summary

Research Adversary is not ready for full operational use. It is ready for limited operational use as a generated-only, human-reviewed critique step inside the standard Atlas research workflow.

The strongest evidence for use is that taxonomy-enhanced recall materially improves failure, assumption, and constraint recall while preserving authority boundaries. The strongest reason not to grant full operational use is that the original adversary evaluation still reports zero human-scored cases, zero measured research hours saved, and `NEEDS_HUMAN_REVIEW`.

Recommended operational position:

- allow Research Adversary as a standard pre-review critique artifact;
- require human review before any downstream interpretation;
- prohibit any automatic gating, promotion, rejection, replay effect, qualification effect, governance effect, memory write, or paper-forward action;
- keep taxonomy and vocabulary bridge outputs labeled as generated-only diagnostic overlays.

## Scorecard

Scale: 1 low, 5 high. For risk categories, higher means more risk or cost.

| Dimension | Score | Evidence | Readiness Interpretation |
| --- | ---: | --- | --- |
| Usefulness | 4 | Candidate retrospective rated most candidate-family reviews HIGH for surfacing proxy dependency, weak regimes, mechanism duplication, sample insufficiency, and falsification gaps. Taxonomy recall improved failure recall from 23.7% to 86.8%. | Useful enough for standard pre-review critique. |
| False Positive Risk | 3 | Taxonomy recall reported 0 false positives on the frozen mapping, but ambiguity remained material: 5 ambiguous cases and 6 multi-category cases. Vocabulary bridge effectiveness rated bridge false-positive risk MEDIUM for partial `CHOP -> RANGE_BOUND` semantics. | Acceptable only with human review and explicit weak-analogy labels. |
| Maintenance Cost | 3 | The adversary already has tests and generated-only contracts, but taxonomy precedence, ambiguity handling, wording cleanup, and scorer maintenance remain required. | Moderate recurring maintenance; manageable for limited use. |
| Research Hours Saved | 4 | Estimated opportunity is 115 hours across 33 retrospective cases, with 90-140 hour range. Current measured evaluation still reports 0 hours saved because human-scored operational measurement has not run. | Strong estimated value, not yet fully measured. |
| Failure Recall | 4 | Baseline failure recall was 23.7%; taxonomy-enhanced recall was 86.8% on the frozen historical set, with 14 direct hits, 5 partial hits, and 0 misses. | Strong with taxonomy; weak without taxonomy. |
| Assumption Recall | 3 | Baseline weighted assumption recall was 39.1%, exact found rate 3.1%, useful assumption rate 75.0%. Taxonomy-enhanced assumption recall was reported at 81.2%. | Good when taxonomy-enhanced, but exact baseline recall is weak. |
| Governance Risk | 2 | Governance audit found no executable authority leakage and 31 focused tests passed, but warned about approval-like status wording and lifecycle-like terms. All reviewed outputs preserve generated-only boundaries. | Low-to-moderate risk if wording is tightened and consumers honor boundaries. |

## Evidence Review

### Evaluation Results

The current evaluation summary is conservative:

- cases evaluated: 0
- evaluation status: `NEEDS_HUMAN_REVIEW`
- reviewer usefulness score: 0.0
- estimated research hours saved: 0
- known failures tested: 13
- missed known failure modes: 0
- authority violation count: 0
- recommendation: `HOLD`

This prevents a `FULL_OPERATIONAL_USE` recommendation. The zero scored cases mean measured human utility has not yet been established in the ordinary workflow.

The separate failure recall study is more informative:

- failures evaluated: 19
- direct hits: 0
- partial hits: 9
- misses: 10
- weighted failure recall: 23.7%

Baseline Research Adversary is directionally useful but too generic when it lacks Atlas-specific taxonomy support.

### Taxonomy Results

Taxonomy integration materially changes the readiness picture:

- baseline failure recall: 23.7%
- taxonomy-enhanced failure recall: 86.8%
- delta: +63.1 percentage points
- direct hits: 14
- partial hits: 5
- misses: 0
- assumption recall: 81.2%
- constraint recall: 86.8%
- false positives: 0
- authority violations: 0
- output status: `GENERATED_ONLY`

This supports limited operational use if Research Adversary runs with taxonomy lookup and if ambiguous categories remain human-review hints rather than decisions.

Residual taxonomy concerns:

- ambiguous taxonomy cases: 5
- multi-category cases needing reviewer confirmation: 6
- precedence rules still need refinement
- secondary categories must not become automatic decisions

### Vocabulary Bridge Results

The vocabulary bridge is not part of Research Adversary authority, but it is relevant to readiness because it shows whether generated-only diagnostic overlays can reveal validation bottlenecks without modifying replay.

Observed bridge result:

- candidates reviewed: 8
- exact matches: 3
- partial matches: 5
- unmappable: 0
- unknown: 0
- status: `GENERATED_ONLY`

Bridge simulation and effectiveness reports found:

- `CHOP -> RANGE_BOUND` recovered 396 surviving samples.
- 5 affected candidates improved at the sample level.
- 3 candidates became sample-sufficient.
- 2 candidates became simulated `CONFIRMED`.
- affected-candidate block rate dropped from 100% to 60%.
- false positive risk remained MEDIUM because `CHOP -> RANGE_BOUND` is partial, not exact.

Interpretation: generated-only overlays can materially improve diagnosis, but their outputs must stay clearly separated from validation truth. This favors limited operational use rather than full operational use.

### Governance Audits

The governance audit recommendation was `PASS_WITH_WARNINGS`.

Positive controls:

- no executable authority leakage found
- generated-only review artifacts
- explicit false boundaries for trade, capital, sizing, candidate promotion, replay override, qualification override, governance override, automatic memory writes, and production pipeline integration
- focused tests: 31 passed

Warnings:

- `APPROVED_FOR_EXPERIMENT_DESIGN` has approval-like wording
- `RETIRE_CANDIDATE_CAPABILITY` and `RETIRE` have lifecycle-like wording
- generated markdown includes promotion/approval-adjacent language that could confuse downstream consumers

Interpretation: governance risk is acceptable for generated-only limited use, but wording/status cleanup should precede broader automation or integration.

## Recommendation

Recommendation: `LIMITED_OPERATIONAL_USE`

Research Adversary should move out of pure evaluation only for one bounded role: a standard generated-only pre-review critique artifact in Atlas research workflow.

Allowed limited use:

- attach adversary critique to research observations, claims, hypotheses, candidate retrospectives, and failure investigations;
- include taxonomy categories, confidence, ambiguity, related historical failure patterns, assumptions, constraints, and falsification prompts;
- allow humans to use the critique to narrow research questions and request follow-up evidence;
- use vocabulary bridge and taxonomy outputs only as diagnostic context.

Not allowed:

- automatic candidate promotion, demotion, rejection, qualification, or disqualification;
- replay override, replay relaxation, or replay-result mutation;
- qualification override or edge-gate mutation;
- governance override or policy change;
- automatic paper-forward admission or paper placement;
- automatic memory writes;
- trading recommendation, broker execution, capital allocation, portfolio construction, or position sizing.

## Conditions For Full Operational Use

Do not move to `FULL_OPERATIONAL_USE` until all of the following are true:

1. Human-scored workflow cases are evaluated, not just retrospective or frozen mapping studies.
2. Measured research hours saved is positive after subtracting review overhead.
3. False positive rate remains at or below 30% on live workflow cases.
4. Taxonomy precedence rules reduce ambiguity without hiding secondary categories.
5. Approval-like and lifecycle-like wording is removed or clearly renamed.
6. Authority-boundary checks remain zero-violation under generated outputs and markdown summaries.
7. Consumers are tested to ignore adversary outputs as authority and treat them only as critique evidence.

## Authority Boundary

This report is analysis-only.

No authority expansion is granted.
No production integration is implemented.
No candidate state is changed.
No replay behavior is changed.
No qualification behavior is changed.
No governance policy is changed.
No paper-forward state is changed.
No memory write is authorized.
No trade recommendation is made.
No broker execution is authorized.
No capital allocation is authorized.
No position sizing is authorized.
No automatic paper placement is authorized.

Research Adversary output remains generated-only and human-reviewed.
