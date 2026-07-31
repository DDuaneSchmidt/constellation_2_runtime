# AI Research Scientist Design Review 001

Date: 2026-06-05
Status: Design review only

Scope: evaluates whether Atlas should add an AI Research Scientist layer. This document does not implement production code, workers, schemas, runtime modules, candidate promotion, replay override, qualification override, governance override, paper-forward state, trade recommendation, capital recommendation, position sizing, portfolio allocation, or broker execution.

Reviewed workflow:

```text
Observation
-> AI notices anomaly
-> AI proposes mechanism
-> AI proposes competing explanations
-> AI proposes experiments
-> AI updates beliefs
```

Review standard: most ideas are wrong, most patterns are noise, most explanations are post-hoc, and most AI-generated theories are plausible nonsense. The burden of proof is on the design.

## Existing Atlas Fit

Atlas already has the key primitives needed to host this layer safely:

- evidence maturity levels: `GENERATED_ONLY`, `MOCK_ONLY`, `HISTORICAL_REPLAY`, `PAPER_FORWARD_OBSERVATION`, `EXTERNALLY_VALIDATED`
- lifecycle states: `SUPPORTED`, `STALE`, `WEAKENED`, `FALSIFIED`, `RETIRED`, `REOPENED`, `QUARANTINED`
- candidate-quality metrics: candidate conversion, rejection-rate reduction, evidence maturity delta, hypothesis survival, portfolio-scoring rejection reduction, repeated failure reduction
- memory objects: mechanism, claim cluster, hypothesis cluster, experiment cluster, failure pattern, regime context, evidence trail, learning node, retired knowledge, reopened knowledge
- hard separation between research evidence, operator disposition, runtime truth, candidate state, paper state, and capital/trading authority

The AI Research Scientist should therefore not be a new authority plane. It should be a research-worker layer that produces structured, low-authority artifacts consumed by existing evaluation and memory systems after gates pass.

## Value Proposition

The layer is worth considering only if it increases the rate at which Atlas turns observations into falsifiable experiments and reduces repeated low-quality research.

Potential value:

- notice anomalies that deterministic dashboards surface but do not contextualize
- propose mechanisms in a reusable structure rather than prose-only notes
- force competing explanations before the system commits to a mechanism story
- propose cheap experiments with explicit falsification criteria
- maintain belief deltas as proposals, not truth
- reduce duplicate hypothesis creation by linking to memory and retired knowledge
- improve zero-sample and insufficient-data triage by asking why replay support failed

Non-value:

- producing more plausible theories
- explaining every anomaly
- creating narrative confidence
- accelerating candidate promotion
- bypassing direct replay
- turning generated prose into memory truth

The layer earns existence only through measured improvement in:

```text
hypothesis quality
replay support rate
candidate conversion
backtest support
paper-forward survival
failure reduction
```

## Proposed V0.1 Shape

V0.1 should be a shadow-mode research worker that reads observations, evidence records, replay summaries, failure patterns, and memory objects. It emits only the allowed AI artifacts:

```text
AnomalyNarrative
MechanismProposal
CompetingExplanation
ExperimentProposal
BeliefUpdateProposal
```

Each artifact must be `GENERATED_ONLY` unless a later non-AI process attaches stronger evidence. The AI must never emit:

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

V0.1 should not write lifecycle state. It may write proposals to a quarantine-capable research inbox only after label, lineage, duplicate, and forbidden-output checks pass.

## Artifact Roles

### AnomalyNarrative

Purpose: describe what looks unusual and why it may be worth investigation.

Required fields:

- observation refs and hashes
- anomaly type
- baseline comparison
- estimated novelty
- known data limitations
- why this may be noise
- forbidden interpretations

Hard rule: an anomaly narrative is not evidence of edge.

### MechanismProposal

Purpose: propose a possible causal or behavioral mechanism.

Required fields:

- mechanism tag
- instrument scope
- timeframe scope
- regime scope
- causal chain
- assumptions
- required evidence
- disconfirming evidence
- memory links
- duplicate/retired-knowledge check result

Hard rule: a mechanism proposal is a hypothesis seed, not a supported hypothesis.

### CompetingExplanation

Purpose: force plausible nulls and alternatives into the same artifact family as the favored theory.

Required fields:

- target anomaly or mechanism proposal
- explanation class, such as data artifact, regime drift, survivorship, liquidity, calendar effect, overfit trigger, proxy mismatch, or random noise
- evidence that would support it
- evidence that would weaken it
- cheap test
- priority

Hard rule: every mechanism proposal must have at least two competing explanations, including one null/noise explanation.

### ExperimentProposal

Purpose: propose bounded tests that can falsify or reduce uncertainty.

Required fields:

- linked mechanism and competing explanations
- experiment design
- input data requirements
- fixed window or replay set
- pass criteria
- fail criteria
- minimum sample requirement
- leakage controls
- expected information gain
- cost and risk
- authority boundary

Hard rule: experiments must be designed to kill weak ideas, not rescue them.

### BeliefUpdateProposal

Purpose: propose a scoped confidence update after evidence arrives.

Required fields:

- prior belief object refs
- evidence refs and hashes
- update direction: increase, decrease, unchanged, split-by-regime, retire, reopen, quarantine recommendation
- magnitude class: small, medium, large
- rationale
- contradictions
- uncertainty after update
- required human or lifecycle-controller review

Hard rule: this is a proposal. Only a lifecycle controller can change research lifecycle state.

## Belief Update Framework

Beliefs must be scoped and evidence-weighted. A single scalar confidence is not enough.

Required belief dimensions:

- mechanism plausibility
- evidence maturity
- replay support
- regime specificity
- data quality
- duplicate/novelty status
- failure-pattern exposure
- candidate-quality relevance

Suggested update discipline:

| Evidence event | Allowed belief update | Forbidden interpretation |
| --- | --- | --- |
| Generated mechanism only | seed uncertainty | support |
| Competing explanations added | lower narrative confidence | proof of rigor |
| Historical replay support | raise replay-support dimension only | forward performance |
| Zero-sample replay | raise attrition/failure concern | mechanism falsification unless test was valid |
| Paper-forward survival | raise forward-observation dimension | live readiness |
| Contradictory evidence | weaken or split scope | erase prior support |
| Broken lineage | quarantine | partial credit |

Belief updates must preserve uncertainty. If evidence is ambiguous, the correct update is often no change plus a better experiment.

## Competing Explanations Requirement

The design should assume that the favored explanation is usually wrong. Every anomaly and mechanism proposal must include competing explanations before any experiment is accepted.

Minimum competing explanation set:

- null/noise explanation
- data or source artifact explanation
- regime/context explanation
- proxy or universe mismatch explanation when candidate or replay evidence is involved
- overfit or multiple-testing explanation when backtest support is involved

The AI must assign each explanation a falsification route. Explanations without falsification routes should be marked as narrative-only and not consumed by experiment selection.

## Memory Integration

The AI layer should read memory before proposing new work and write only proposal artifacts after gates pass.

Read requirements:

- claim clusters for duplicates
- hypothesis clusters for near-duplicates
- experiment clusters for repeated designs
- failure patterns for known traps
- retired knowledge for reopening checks
- regime contexts for scope matching
- evidence trails for lineage validation

Write requirements:

- proposals are append-only
- every proposal links to parent observations and memory refs
- generated artifacts remain `GENERATED_ONLY`
- no memory record becomes `SUPPORTED` from AI text alone
- duplicate and retired-knowledge checks must run before backlog use

Quarantine triggers:

- missing parent refs
- no competing explanations
- forbidden output language
- unsupported certainty
- duplicate laundering
- authority leakage
- source hallucination

## Explainability Requirements

Every artifact must make its reasoning inspectable without relying on hidden model confidence.

Required explanation fields:

- source observations used
- source observations ignored
- assumptions
- uncertainty
- competing explanations
- why the preferred mechanism may be wrong
- what evidence would falsify it
- data requirements
- authority boundary
- generated-only label

The AI must not present fluent prose as explanation. The useful explanation is the chain from observation to mechanism to alternatives to experiment to falsification criteria.

## Evaluation Framework

The AI Research Scientist is not accepted because it sounds intelligent. It is accepted only if measured against a no-AI or simpler-baseline process.

Primary evaluation:

- fixed replay set with AI disabled vs AI proposals available as labeled research input
- same observation set
- same candidate factory version
- same governance and scoring contracts
- same replay windows
- same memory snapshot

Core metrics:

| Metric | Acceptance direction |
| --- | --- |
| hypothesis quality | higher pre-registered experiment pass/fail clarity and lower duplicate rate |
| replay support rate | higher share of proposed experiments reaching historical replay with non-null result |
| candidate conversion | higher valid-candidate conversion only when evidence maturity does not deteriorate |
| backtest support | higher backtest-supported rate after leakage and overfit controls |
| paper-forward survival | higher survival through predeclared observation windows |
| failure reduction | lower repeated failure count and fewer forbidden artifacts |

Required controls:

- duplicate penalty
- generated-only label integrity
- memory leakage audit
- authority boundary audit
- human review sample
- adversarial review of plausible nonsense
- negative controls where no real anomaly should exist

Minimum go/no-go:

- no forbidden artifact output in the evaluation set
- measurable reduction in repeated failures
- no deterioration in evidence maturity
- at least one metric improves beyond a fixed baseline with declared confidence
- no candidate promotion or paper-forward state mutation from the AI layer

## Design Verdict

The design is plausible only as a bounded research scientist assistant, not as an autonomous scientist and not as a candidate authority. Its first job is not to be creative. Its first job is to reduce avoidable false explanations by forcing alternatives, experiments, and belief discipline.

The layer should be rejected if it cannot beat a simpler template-based research triage process on replay support, failure reduction, or hypothesis quality.
