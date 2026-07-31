# AI Research Scientist Recommendation 001

Date: 2026-06-05
Status: Design recommendation only

Scope: final recommendation on whether Atlas should add an AI Research Scientist layer. This document does not implement code, create production workers, change Atlas behavior, write runtime truth, create candidates, change ranking, change paper-forward state, recommend trades, allocate capital, size positions, or authorize broker execution.

## Decision Question

Should Atlas add an AI Research Scientist layer that moves from observations to anomaly narratives, mechanism proposals, competing explanations, experiment proposals, and belief update proposals?

The answer is not based on whether the idea is interesting. It is based on whether the layer can measurably improve:

```text
hypothesis quality
replay support rate
candidate conversion
backtest support
paper-forward survival
failure reduction
```

## Recommendation Summary

Build a V0.1 only as a shadow-mode, generated-only, proposal-producing research layer.

Do not build an autonomous research scientist. Do not connect it to candidate promotion, replay status, qualification status, paper-forward state, governance state, trading, capital, sizing, or portfolio systems. Do not let it write accepted memory or lifecycle state.

The V0.1 purpose is narrow:

- detect candidate research anomalies
- propose mechanisms with explicit uncertainty
- propose competing explanations, including null/noise alternatives
- propose falsifiable experiments
- propose scoped belief updates for review
- reduce repeated research failure
- improve evidence routing and replay experiment quality

## Why Not Reject

Rejection would be reasonable if Atlas lacked evidence labels, memory design, lifecycle discipline, or candidate-quality metrics. Atlas already has those design primitives. A constrained AI layer can be evaluated as another research worker without granting authority.

The strongest reason not to reject is the current research bottleneck: Atlas repeatedly faces ambiguous replay failures, proxy dependence, zero-sample attrition, missing-data triage, and mechanism taxonomy ambiguity. A disciplined AI layer may help generate better competing explanations and experiments faster than hand-authored review alone.

## Why Not Build Immediately

Immediate production build is not justified.

Reasons:

- most AI theories will be plausible nonsense
- current Atlas evidence still includes proxy and direct-replay gaps
- generated-only artifacts can contaminate memory if acceptance gates are weak
- the layer has not yet beaten deterministic triage
- authority leakage would be expensive
- paper-forward and runtime truth must remain separate

Atlas should not spend engineering effort on production integration until a narrow V0.1 proves measurable lift against a baseline.

## Why Build V0.1

V0.1 is justified if and only if it is a measurement experiment, not a productized authority path.

Expected V0.1 benefits:

- better anomaly triage
- more explicit null explanations
- more falsifiable experiment proposals
- lower duplicate/reopened-idea waste
- clearer replay attrition diagnostics
- stronger belief update discipline

Expected V0.1 outputs:

```text
AnomalyNarrative
MechanismProposal
CompetingExplanation
ExperimentProposal
BeliefUpdateProposal
```

All V0.1 outputs must be `GENERATED_ONLY` and carry forbidden-use labels.

## Required V0.1 Constraints

V0.1 must be:

- shadow-mode only
- read-only against runtime truth, candidate state, replay state, qualification state, paper state, governance state, and memory acceptance
- generated-only by default
- append-only for audit
- evaluated against a deterministic or human baseline
- blocked from candidate and paper systems
- blocked from trade, capital, sizing, allocation, and broker language
- required to produce competing explanations
- required to produce falsification criteria
- required to include authority boundaries

V0.1 must not emit:

```text
TradeRecommendation
CapitalRecommendation
PositionSizing
PortfolioAllocation
CandidatePromotion
ReplayOverride
QualificationOverride
GovernanceOverride
```

## Minimum V0.1 Acceptance Test

Create a fixed evaluation set from prior Atlas observations and replay outcomes.

Suggested test set:

- zero-sample replay attrition cases
- confirmed direct-data candidate case
- insufficient-data root-cause cases
- proxy-dependent ranked candidate cases
- event-reaction missing-metadata case
- retired or duplicated mechanism examples
- negative controls with no real anomaly

Run three arms:

1. no-AI baseline
2. deterministic template triage
3. AI Research Scientist V0.1 proposals

Measure:

| Metric | Required result |
| --- | --- |
| forbidden artifact rate | `0` |
| authority contamination | `0` |
| duplicate rate | lower than or equal to baseline |
| hypothesis quality | higher falsifiability and clearer assumptions |
| replay support rate | higher share of proposed experiments reaching usable replay |
| backtest support | higher support only after leakage and overfit controls |
| candidate conversion | higher only if evidence maturity does not deteriorate |
| paper-forward survival | no claim until enough forward evidence exists |
| failure reduction | lower repeated failure count |
| human rejection rate | acceptable and explained |

If V0.1 does not improve at least one core metric without worsening safety, evidence maturity, or duplicate rate, it should be retired.

## Implementation Boundary For Future Work

Future implementation, if separately approved, should start with schemas and offline reports only:

- `AnomalyNarrative.v0_1`
- `MechanismProposal.v0_1`
- `CompetingExplanation.v0_1`
- `ExperimentProposal.v0_1`
- `BeliefUpdateProposal.v0_1`
- `ai_research_scientist_evaluation.v0_1`

Do not start with a UI, autonomous worker, candidate-factory integration, memory writer, or paper-forward hook. Start with offline artifacts and a baseline evaluation.

## Go / No-Go Gates

Proceed from design to V0.1 only when:

- artifact schemas include authority boundaries and forbidden-use fields
- forbidden-output scan exists
- duplicate and retired-knowledge checks are defined
- evaluation dataset is fixed
- baseline is defined
- quarantine behavior is defined
- no candidate or paper state write path exists

Stop or retire V0.1 when:

- any forbidden output is emitted into a consumable artifact
- generated-only text becomes accepted evidence
- candidate or paper state changes because of AI output
- replay support rate does not improve
- repeated failures do not fall
- reviewers cannot distinguish useful proposals from plausible narrative

## Final Decision

BUILD_V0_1
