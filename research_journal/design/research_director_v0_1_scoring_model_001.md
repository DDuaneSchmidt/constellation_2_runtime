# Research Director V0.1 Scoring Model 001

Status: DESIGN + EVALUATION ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: non-authoritative Research Director prototype design for ranking research work. This document does not implement production code, alter runtime truth, modify Aegis behavior, create candidates, approve candidates, approve paper placement, alter replay, alter qualification, alter governance, allocate capital, recommend trades, size positions, construct portfolios, or authorize live activity.

## Purpose

Research Director V0.1 should answer a narrow question:

```text
Which research item is most worth investigating next, and why?
```

It should rank research effort, not trading opportunity. A high Research Director score means only that an item may produce useful learning per unit of research effort. It does not mean the item is correct, profitable, eligible, validated, or approved.

## Inputs

V0.1 may evaluate typed, lineage-preserving research artifacts:

- Observations
- Claims
- Hypotheses
- Failures
- Knowledge
- Research Adversary Reviews

Each input should preserve:

- source artifact ids
- evidence level
- mechanism tags
- regime context
- timestamp or review date
- known uncertainty
- known failure links
- forbidden uses

Generated-only artifacts may contribute to ranking only as weak research signals. They must not be treated as validation.

## Outputs

The prototype may produce four non-authoritative rankings:

- `ResearchPriorityRanking`
- `UncertaintyRanking`
- `ExperimentRanking`
- `InvestigationQueue`

These outputs are advisory research artifacts only. They may suggest what a human should inspect next, but they must not mutate backlog, replay, qualification, candidate, paper-forward, memory, governance, broker, capital, or portfolio systems.

## Ranking Unit

The ranking unit should be a `ResearchItem`.

Suggested fields:

| Field | Meaning |
| --- | --- |
| `research_item_id` | Stable id for the ranked item. |
| `source_type` | Observation, claim, hypothesis, failure, knowledge, or adversary review. |
| `source_artifact_ids` | All artifacts used to score the item. |
| `mechanism_tags` | Mechanisms implicated by the item. |
| `regime_context` | Known, unknown, or conflicting regime scope. |
| `uncertainty_ids` | Linked uncertainty objects or inferred uncertainty categories. |
| `failure_categories` | Atlas failure taxonomy matches. |
| `proposed_research_action` | Review, replay design, data repair, falsification design, dedupe check, retirement review, or hold. |
| `score_breakdown` | Transparent factor scores and reasons. |
| `authority_boundary` | Explicit forbidden uses. |

## Scoring Factors

Each factor should be scored from `0.0` to `1.0`, where higher usually means higher research priority unless stated otherwise.

### Information Gain Potential

Definition: expected reduction in decision-relevant uncertainty if the item is investigated.

High score signals:

- resolving the item would change whether a mechanism family is explored, repaired, compressed, or retired
- the item tests a recurring uncertainty rather than producing more narrative
- the result would clarify multiple linked claims or hypotheses

Low score signals:

- outcome would not change any future research path
- item is already answered by prior knowledge
- investigation would only add duplicate prose

Suggested scoring:

| Score | Interpretation |
| --- | --- |
| `1.0` | Resolves a high-impact uncertainty across a mechanism family. |
| `0.7` | Clarifies a candidate hypothesis cluster or recurring failure pattern. |
| `0.4` | Improves local understanding but has limited reuse. |
| `0.1` | Mostly redundant or low decision impact. |

### Failure History

Definition: relevance and severity of prior Atlas failures connected to the item.

This factor should increase priority for failure analysis, falsification, repair, or retirement review. It should not increase confidence in the item.

High score signals:

- direct match to Atlas failure taxonomy categories
- repeated unresolved failure pattern
- prior failure likely explains current artifact weakness

Low score signals:

- no known failure link
- weak analogy to unrelated failure
- prior failure was already resolved or scoped out

Suggested scoring:

| Score | Interpretation |
| --- | --- |
| `1.0` | Direct unresolved recurring failure match. |
| `0.7` | Strong related failure pattern. |
| `0.4` | Possible historical analogy. |
| `0.0` | No meaningful failure history. |

### Research Cost

Definition: expected research effort required to reduce uncertainty.

Research cost is a penalty. Lower cost should increase priority when information gain is comparable.

Suggested normalized cost score:

| Cost score | Interpretation |
| --- | --- |
| `1.0` | Cheap review or existing report synthesis. |
| `0.7` | Small replay, data audit, or adversary review. |
| `0.4` | Larger backtest design, source reconstruction, or manual review. |
| `0.1` | Expensive data acquisition, long paper-forward wait, or broad redesign. |

### Novelty

Definition: whether the item adds non-duplicate search-space coverage or a materially new mechanism/context combination.

High score signals:

- new mechanism-context pair
- new source type with measured promise
- non-overlapping evidence path

Low score signals:

- duplicate cluster
- restated hypothesis with same source lineage
- novelty is only wording, not evidence or mechanism structure

Novelty must be balanced against failure history. A novel idea with high adversary risk may deserve a cheap falsification experiment, not expensive validation.

### Search Space Coverage

Definition: value of the item for balancing Atlas research across mechanisms, regimes, source types, timeframes, and failure categories.

High score signals:

- covers under-tested mechanism family
- closes a coverage gap identified by source breakdown, observation diversity, or methodology evaluation
- tests a mechanism with broad downstream reuse

Low score signals:

- over-concentrated in already saturated mechanism family
- adds another variant to a duplicate cluster
- only improves a narrow artifact with no reusable learning

### Adversary Risk Score

Definition: severity of risks surfaced by Research Adversary reviews, including assumptions, constraints, falsification gaps, duplicate risk, and Atlas failure taxonomy matches.

This factor is not a simple penalty. It should route the item:

- high risk plus high information gain: prioritize falsification or repair
- high risk plus low information gain: hold or retire
- low risk plus high information gain: prioritize experiment design
- low risk plus low information gain: deprioritize as low-value

Suggested components:

- assumption count and severity
- constraint count and severity
- falsification test quality
- known failure category matches
- authority violation count
- false positive risk
- reviewer usefulness score where available

Any authority violation should force the item into `HOLD_FOR_BOUNDARY_REVIEW`, regardless of other score components.

## Composite Scores

V0.1 should calculate separate scores for different ranking outputs rather than one universal number.

### Research Priority Score

Used for `ResearchPriorityRanking`.

```text
research_priority_score =
  0.30 * information_gain_potential
+ 0.20 * search_space_coverage
+ 0.15 * novelty
+ 0.15 * failure_history
+ 0.10 * research_cost_score
+ 0.10 * adversary_routing_value
```

`adversary_routing_value` is high when adversary output makes the next research action clearer. It is not high merely because the idea is attractive.

### Uncertainty Priority Score

Used for `UncertaintyRanking`.

```text
uncertainty_priority_score =
  0.40 * information_gain_potential
+ 0.25 * decision_impact
+ 0.15 * reducibility
+ 0.10 * failure_history
+ 0.10 * research_cost_score
```

The uncertainty ranking should prioritize uncertainties that can change a decision, not uncertainties that are merely interesting.

### Experiment Priority Score

Used for `ExperimentRanking`.

```text
experiment_priority_score =
  0.35 * information_gain_potential
+ 0.20 * falsification_power
+ 0.15 * research_cost_score
+ 0.10 * search_space_coverage
+ 0.10 * failure_history
+ 0.10 * novelty
```

`falsification_power` measures whether the proposed experiment can distinguish the hypothesis from competing explanations or null explanations.

### Investigation Queue Score

Used for `InvestigationQueue`.

```text
investigation_queue_score =
  max(
    research_priority_score,
    uncertainty_priority_score,
    experiment_priority_score
  )
```

Queue ordering should also include route classification.

## Route Classification

Each ranked item should receive one route:

| Route | Meaning |
| --- | --- |
| `INVESTIGATE` | Human review likely produces useful next-step clarity. |
| `DESIGN_EXPERIMENT` | Item needs a bounded falsification or information-gain test. |
| `REPAIR_UNCERTAINTY` | Lineage, data, regime, mechanism, or context uncertainty blocks interpretation. |
| `CHECK_DUPLICATE` | Possible duplicate or cluster overlap should be reviewed first. |
| `REVIEW_FAILURE_PATTERN` | Historical failure match may explain the current artifact. |
| `COMPRESS_SEARCH_SPACE` | Item may help retire, merge, or narrow a research branch. |
| `HOLD_LOW_VALUE` | Expected learning does not justify work now. |
| `HOLD_FOR_BOUNDARY_REVIEW` | Authority-boundary or forbidden-language issue requires human review. |

No route implies candidate approval, replay approval, qualification approval, paper-forward approval, capital allocation, or trading action.

## Score Explanation Requirements

Every ranking item must include:

- top three reasons it ranked where it did
- strongest evidence supporting investigation
- strongest evidence against investigation
- most important uncertainty
- known failure taxonomy matches
- Research Adversary risk summary
- estimated research cost
- expected information gain
- forbidden uses

Opaque scores should be rejected. If a score cannot be explained from evidence-linked inputs, it should be marked `UNSCORED_INSUFFICIENT_LINEAGE`.

## Example Interpretations

### High Priority, High Risk

An observation cluster has a new mechanism-context pair, strong coverage value, and a Research Adversary review that flags proxy dependency and mechanism mismatch.

Interpretation: prioritize a cheap falsification or source-specific data repair. Do not treat the risk as a rejection by itself.

### Low Priority, High Failure History

A hypothesis repeats a prior technical-indicator claim that failed for baseline recoverability and lacks new data.

Interpretation: route to `REVIEW_FAILURE_PATTERN` or `COMPRESS_SEARCH_SPACE`, not validation.

### High Uncertainty, Low Reducibility

A candidate-like hypothesis depends on unavailable macro context that cannot be reconstructed.

Interpretation: hold or scope the claim down. Do not spend expensive validation effort until reducibility improves.

## Authority Boundary

Research Director V0.1 scoring has no authority to:

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

Its only allowed use is evaluation-only research prioritization.

