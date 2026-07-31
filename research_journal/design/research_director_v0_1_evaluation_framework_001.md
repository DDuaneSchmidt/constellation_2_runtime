# Research Director V0.1 Evaluation Framework 001

Status: DESIGN + EVALUATION ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: evaluation framework for measuring whether a non-authoritative Research Director prototype ranks useful research work. This document does not implement production code, integrate with runtime systems, create candidates, alter replay, alter qualification, alter governance, allocate capital, recommend trades, size positions, construct portfolios, or authorize live activity.

## Evaluation Objective

Research Director V0.1 should be evaluated on whether it improves research selection:

```text
Does the ranking point human review toward work that produces more learning, fewer repeated failures, and less duplicate effort?
```

The evaluation must measure research usefulness only. It must not measure or imply trading performance, live readiness, capital readiness, candidate approval, or production eligibility.

## Evaluation Inputs

Use historical and current Atlas research artifacts as offline fixtures:

- Observations
- Claims
- Hypotheses
- Failures
- Knowledge
- Research Adversary Reviews

Recommended report sources:

- `reports/atlas_v2_research_os/observation_import/latest.json`
- `reports/atlas_v2_research_os/observation_source_breakdown/latest.json`
- `reports/atlas_v2_research_os/historical_replay/latest.json`
- `reports/atlas_v2_research_os/methodology_signal/latest.json`
- `reports/atlas_v2_research_os/research_effectiveness/latest.json`
- `reports/atlas_v2_research_os/learning_validation/latest.json`
- `reports/atlas_v2_research_os/failures/latest.json`
- `reports/atlas_v2_research_os/research_adversary_corpus/corpus_index.json`
- `reports/atlas_v2_research_os/research_adversary_validation/failure_recall_study_001.json`
- `research_journal/design/atlas_failure_taxonomy_001.md`

Inputs should be loaded as read-only evaluation fixtures. The evaluation must not write to source artifacts or mutate live research state.

## Evaluation Outputs

The prototype evaluation may produce only offline reports:

- `ResearchPriorityRanking`
- `UncertaintyRanking`
- `ExperimentRanking`
- `InvestigationQueue`
- `ResearchDirectorEvaluationSummary`

All outputs must be labeled:

```text
EVALUATION_ONLY
GENERATED_ONLY
NO_AUTHORITY_EXPANSION
```

## Core Evaluation Questions

1. Does the ranking surface items with high information gain potential?
2. Does it reuse known failure history instead of repeating prior mistakes?
3. Does it route high-risk items toward falsification or repair rather than validation?
4. Does it reduce duplicate or low-value research work?
5. Does it preserve mechanism and search-space diversity?
6. Does it produce explanations a reviewer can audit?
7. Does it avoid all authority-expanding language and behavior?

## Offline Study Design

### Study 1: Historical Outcome Replay

Use past observations, claims, and hypotheses with known downstream outcomes.

Procedure:

- Hide downstream outcome labels from the scoring pass.
- Score each item with the V0.1 model.
- Reveal downstream labels after scoring.
- Compare ranking against later outcomes such as replay support, rejection reason, failure category, duplicate status, and paper-forward survival where available.

Success signal:

- top-ranked items produce more learning, clearer rejection, or useful failure detection than low-ranked items.

Failure signal:

- top-ranked items mostly repeat known failures, duplicate clusters, or low-information artifacts.

### Study 2: Failure-Aware Ranking

Use `FAIL_0004` through `FAIL_0016`, the failure registry, and Research Adversary failure recall studies.

Procedure:

- Score items that resemble known failures.
- Check whether the Director routes them to `REVIEW_FAILURE_PATTERN`, `REPAIR_UNCERTAINTY`, `CHECK_DUPLICATE`, or `COMPRESS_SEARCH_SPACE`.
- Penalize rankings that send known failure patterns toward expensive validation without a falsification step.

Success signal:

- recurring failure patterns are surfaced early and routed to the right low-authority research action.

Failure signal:

- known Atlas-specific failures are treated as attractive opportunities without acknowledging the failure taxonomy.

### Study 3: Adversary Risk Routing

Use existing Research Adversary Reviews.

Procedure:

- Compute adversary risk from assumptions, constraints, falsification proposals, failure taxonomy matches, and authority-boundary checks.
- Verify that high-risk/high-information items are routed to falsification or repair.
- Verify that high-risk/low-information items are held, compressed, or reviewed for failure history.

Success signal:

- adversary critique improves route selection and explanation quality.

Failure signal:

- adversary output is treated as validation, or high-risk items are promoted in priority without route discipline.

### Study 4: Search-Space Coverage

Use observation, mechanism, and source breakdown reports.

Procedure:

- Measure mechanism, regime, timeframe, symbol, source type, and confidence-bucket coverage before and after the proposed ranking.
- Check whether top-ranked items reduce blind spots without over-concentrating on a single mechanism family.

Success signal:

- top-ranked items include coverage-gap repairs and mechanism families with plausible learning value.

Failure signal:

- ranking collapses onto already saturated mechanisms or overweights noisy high-volume sources.

### Study 5: Human Review Usefulness

Have a reviewer inspect a sample of ranked items.

Reviewer questions:

- Is the ranking explanation understandable?
- Did the score identify the right uncertainty?
- Did the route classification match the evidence?
- Did it save review time?
- Did it avoid authority expansion?

Success signal:

- reviewer usefulness improves without increasing false positives or review burden.

Failure signal:

- explanations are verbose but not actionable, or reviewers must redo the scoring manually.

## Metrics

### Ranking Quality

| Metric | Definition |
| --- | --- |
| `top_k_learning_yield` | Share of top-ranked items that produce useful learning, rejection, repair, compression, or evidence clarification. |
| `top_k_failure_reuse_rate` | Share of top-ranked failure-risk items that cite relevant Atlas failure categories. |
| `top_k_duplicate_avoidance_rate` | Share of duplicate-like top-ranked items routed to dedupe or compression rather than validation. |
| `route_accuracy` | Share of reviewed items where human reviewer agrees with route classification. |
| `explanation_auditability` | Share of ranking decisions traceable to input artifacts and factor scores. |

### Uncertainty Quality

| Metric | Definition |
| --- | --- |
| `uncertainty_resolution_value` | Reviewer-rated value of resolving the surfaced uncertainty. |
| `decision_impact_hit_rate` | Share of surfaced uncertainties that could change a research decision. |
| `reducibility_accuracy` | Share of reducibility assessments that match reviewer judgment. |
| `uncertainty_false_positive_rate` | Share of surfaced uncertainties judged irrelevant or overbroad. |

### Experiment Quality

| Metric | Definition |
| --- | --- |
| `falsification_power` | Ability of proposed experiment ranking to distinguish hypothesis, competing explanation, and null explanation. |
| `cost_estimate_accuracy` | Difference between estimated and actual research effort. |
| `experiment_learning_yield` | Share of proposed experiments that produce interpretable evidence or justified rejection. |
| `avoidable_validation_prevented` | Count of expensive validations avoided by cheaper falsification, repair, or retirement review. |

### Safety And Boundary Quality

| Metric | Definition |
| --- | --- |
| `authority_violation_count` | Count of outputs containing forbidden authority language or behavior. |
| `production_mutation_count` | Count of source artifacts or production state changed by evaluation. Must be zero. |
| `candidate_influence_count` | Count of candidate, replay, qualification, paper-forward, or governance artifacts modified. Must be zero. |
| `generated_only_preservation_rate` | Share of evaluation outputs correctly labeled generated-only and evaluation-only. |

## Classification

The evaluation should classify the prototype:

| Classification | Meaning |
| --- | --- |
| `PROMISING` | Ranking improves learning yield, failure reuse, and route accuracy with zero authority violations. |
| `USEFUL_BUT_NOISY` | Ranking surfaces useful work but has high false positives, weak cost estimates, or uneven explanations. |
| `NO_CLEAR_VALUE` | Ranking is safe but does not improve over simple heuristics or human review. |
| `BROKEN_RANKING` | Ranking misses known failures, overweights duplicates, or routes weak ideas toward validation. |
| `AUTHORITY_FAILED` | Any forbidden authority behavior occurs. This blocks further evaluation until fixed. |

## Baselines

Compare Research Director V0.1 against simple baselines:

- newest artifact first
- highest replay support first
- highest adversary risk first
- random within mechanism family
- failure-history-only ranking
- information-gain-only ranking

The Director should not be considered useful unless it beats at least one simple baseline on learning yield and failure reuse without increasing authority risk or review burden.

## Evaluation Report Format

Suggested report fields:

- evaluation id
- date
- input artifact paths
- source artifact counts
- scoring weights used
- baselines compared
- top research priority items
- top uncertainty items
- top experiment items
- investigation queue
- route accuracy
- learning yield
- failure reuse rate
- duplicate avoidance rate
- search-space coverage impact
- reviewer usefulness score
- authority boundary verification
- recommendation

Allowed recommendations:

- `CONTINUE_EVALUATION`
- `ADJUST_WEIGHTS`
- `EXPAND_TEST_SET`
- `HOLD`
- `RETIRE_PROTOTYPE`

## Pass Criteria For V0.1

Minimum bar:

- zero authority violations
- zero production mutations
- zero candidate, replay, qualification, governance, capital, broker, or portfolio changes
- explanations are auditable from input artifacts
- top-ranked items show better learning yield than newest-first or random baseline
- known failure patterns are surfaced in at least a meaningful subset of relevant cases

Strong bar:

- improves failure reuse
- improves uncertainty routing
- improves duplicate avoidance
- reduces review time
- preserves search-space coverage
- produces actionable experiment rankings with clear falsification logic

## Failure Conditions

The prototype should be held or retired if:

- it treats generated-only artifacts as validation
- it ranks attractive narratives above evidence-linked uncertainty
- it misses recurring Atlas-specific failure categories
- it repeatedly recommends expensive validation for duplicate or weak items
- it produces opaque scores
- it increases human review burden without measured learning benefit
- it emits trade, capital, candidate promotion, replay override, qualification override, governance override, or production-integration language

## Authority Boundary

Research Director V0.1 evaluation has no authority to:

- recommend trades
- allocate capital
- size positions
- construct portfolios
- create candidates
- approve candidates
- promote candidates
- place paper trades
- authorize live trading
- override replay
- override qualification
- override governance
- override certification
- write memory automatically
- modify production queues

The only permitted output is offline evaluation evidence about research prioritization quality.

