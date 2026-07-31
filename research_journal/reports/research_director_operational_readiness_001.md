# Research Director Operational Readiness Audit 001

Date: 2026-06-05

Status: ANALYSIS_ONLY

Decision: EVALUATION_SERVICE

Authority: No authority expansion. This audit does not recommend replay, candidate, qualification, governance, memory, paper-trading, capital, or broker authority.

## Question

Determine whether Research Director should remain design/evaluation only or move into a permanent service role.

Allowed recommendations:

- REMAIN_DESIGN_ONLY
- EVALUATION_SERVICE
- OPERATIONAL_SERVICE

## Inputs Reviewed

- `research_journal/reports/research_director_evaluation_001.md`
- `research_journal/reports/research_director_prediction_accuracy_001.md`
- `research_journal/reports/research_director_bottleneck_selection_test_001.md`
- `research_journal/reports/research_director_retrospective_001.md`
- `research_journal/reports/research_director_retrospective_scoring_001.md`
- `research_journal/reports/research_hours_saved_estimate_001.md`
- `research_journal/design/research_director_v0_1_architecture_001.md`
- `research_journal/design/research_director_v0_1_evaluation_framework_001.md`
- `research_journal/design/research_director_v0_1_scoring_model_001.md`
- `constellation_2/common/atlas_v2_research_os/research_director_evaluation.py`

## Current Authority Boundary

Research Director is currently generated-only and evaluation-only.

The implementation and reports describe a read-only evaluator that consumes observations, claims, hypotheses, failures, research debt, and adversary reviews, then emits rankings and evaluation reports.

It does not currently have authority to:

- modify candidates
- modify replay
- modify validation or qualification
- write memory
- alter governance
- control trading, paper trading, capital, broker, or execution systems

That boundary should remain unchanged.

## Metric Review

### Bottleneck Prediction Accuracy

Observed evidence is strong enough for permanent evaluation, but not strong enough for operational authority.

The main evaluation report shows the required target order:

1. DATA_COVERAGE
2. REPLAY_ATTRITION
3. VOCABULARY_MISMATCH

The actual top three Research Director priorities matched that order exactly:

1. Data Coverage
2. Replay Attrition
3. Vocabulary Mismatch

The prediction accuracy audit reports:

- broad bottleneck detection: 4/4
- broad detection accuracy: 100%
- precise prediction accuracy: 2.5/4, or 62.5%
- verdict: PASS_WITH_LIMITATIONS

Interpretation:

Research Director is good at identifying the broad area of research blockage. It is less proven at precise diagnosis before root-cause details are exposed.

### Prioritization Quality

Prioritization quality is high for hard-blocker ordering.

Evidence:

- Data Coverage was ranked first in the main evaluation.
- Replay Attrition and Vocabulary Mismatch were ranked second and third.
- Lower-value work did not appear above the three target bottlenecks in the main evaluation.
- The bottleneck selection test selected Data Coverage as the primary Atlas bottleneck.
- Retrospective scoring also put Data Coverage first after tie-breaking against Failure Taxonomy.

Limitations:

- Several evaluations are retrospective.
- Failure Taxonomy was a near tie in the bottleneck selection and retrospective scoring reports.
- The system has not yet accumulated enough prospective cycles to prove stable prioritization quality under fresh unknowns.

Interpretation:

Research Director appears useful as a prioritization evaluator. It should not yet be treated as an operational planner.

### Research Hours Saved

The best direct Research Director estimate is 19-40 research hours saved, from the prediction accuracy audit.

That estimate comes from avoided delay across:

- Research Adversary support: 4-8 hours
- Data Coverage: 8-16 hours
- Replay Attrition: 4-10 hours
- Regime Vocabulary Mismatch: 3-6 hours

The audit also estimates 3-6 research or validation cycles of counterfactual delay avoided.

The separate research-hours-saved report estimates 115 hours of opportunity value for Research Adversary review support. That figure is useful context, but it should not be counted as directly measured Research Director savings because the report itself says current adversary evaluation telemetry recorded zero evaluated cases and zero measured saved hours.

Interpretation:

Research Director has plausible medium-confidence labor-saving value, but measured value is not yet strong enough for operational service status.

### False Prioritization Rate

Measured top-three false prioritization rate in the main evaluation: 0/3.

Reason:

- all top-three priorities matched target bottlenecks
- the highest-ranked lower-value work appeared at rank 4

Measured primary-bottleneck false prioritization in the bottleneck selection test: 0/1.

Residual false prioritization risk: MEDIUM.

Reasons:

- evidence is retrospective
- sample size is small
- precise prediction accuracy is 62.5%, not near-perfect
- Failure Taxonomy was close enough to Data Coverage to show plausible ranking sensitivity
- no prospective human-scored false-priority dataset exists yet

Interpretation:

The currently observed false prioritization rate is low, but confidence in that rate is limited.

## Readiness Classification

### REMAIN_DESIGN_ONLY

Not recommended.

Research Director has moved beyond pure design. There is an implemented offline evaluator, a repeatable scoring model, generated reports, and retrospective evidence that it correctly ranks major Atlas bottlenecks.

Remaining design-only would underuse a tool that is already useful for non-authoritative evaluation.

### EVALUATION_SERVICE

Recommended.

Research Director is ready to become a permanent evaluation service if the service is constrained to:

- read-only inputs
- generated reports only
- no autonomous action
- no state mutation outside research/evaluation artifacts
- no candidate, replay, validation, qualification, governance, memory, or trading authority
- explicit measurement of predictions against realized bottlenecks

The service should act as a standing evaluator that periodically answers:

- what is the current research bottleneck?
- what work is likely to remove the most uncertainty?
- which priorities were later confirmed, partially confirmed, missed, or false?
- how many research hours were plausibly saved after human review?

### OPERATIONAL_SERVICE

Not recommended.

Operational service status would imply stronger readiness than the evidence supports.

Blocking gaps:

- insufficient prospective evaluation cycles
- no stable false-prioritization baseline
- no human-scored hours-saved ledger
- no operational service acceptance threshold
- no evidence that precise bottleneck diagnosis is reliable enough for autonomous routing
- authority boundary is explicitly evaluation-only

## Required Gates Before Operational Service

Research Director should not advance beyond evaluation service until it has completed a prospective shadow period.

Minimum gates:

- at least 3-5 prospective research cycles
- every prediction classified as EXACT, PARTIAL, MISS, or FALSE
- measured false prioritization rate
- measured reviewer overhead
- measured research hours saved after human scoring
- explicit comparison against a no-Director baseline
- unchanged authority boundary during evaluation

Operational service should remain unavailable unless those gates show high precision, low false prioritization, and repeatable time savings.

## Recommendation

Recommendation: EVALUATION_SERVICE

Research Director should move from design/evaluation artifacts into a permanent evaluation service.

It should not become an operational service.

The correct next role is a standing, non-authoritative evaluator that produces recurring bottleneck, priority, uncertainty, and research-debt reports while measuring its own prediction accuracy over time.

Final classification:

EVALUATION_SERVICE
