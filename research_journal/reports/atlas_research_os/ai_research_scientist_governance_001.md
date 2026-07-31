# AI Research Scientist Governance 001

Date: 2026-06-05
Status: Governance design only

Scope: defines governance requirements for a possible Atlas AI Research Scientist layer. This document does not implement code, register modules, create schemas, change runtime truth, write memory records, create candidates, change ranking, change paper-forward state, recommend trades, allocate capital, size positions, or authorize broker execution.

## Governance Principle

The AI Research Scientist is a proposal generator, not a truth producer. It can suggest what to investigate, what might explain an anomaly, and what experiment might reduce uncertainty. It cannot decide what is true, what is promoted, what is paper-forward, or what has authority.

Core invariant:

```text
No consumer invents truth. Consumers query verified truth.
```

## Allowed Outputs

Only the following AI artifacts are allowed:

```text
AnomalyNarrative
MechanismProposal
CompetingExplanation
ExperimentProposal
BeliefUpdateProposal
```

All allowed outputs must default to:

```text
evidence_maturity_level: GENERATED_ONLY
artifact_consumability: AUDIT_ONLY or CONSUMABLE_AFTER_GATES
authority: NONE
```

No allowed artifact may contain fields for orders, capital, sizing, allocation, candidate approval, replay status override, qualification override, or governance override.

## Forbidden Outputs

The AI layer must never emit:

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

Forbidden semantic equivalents are also prohibited. Examples:

- "buy", "sell", "short", "enter", "exit", or "hold" as advice
- "allocate", "increase exposure", "size", or "notional"
- "promote candidate", "approve for paper", or "ready for trade"
- "override replay", "override qualification", or "ignore governance"

Any violation requires quarantine of the artifact and an evaluation failure flag.

## Authority Boundaries

The AI layer may read:

- observations
- research reports
- replay summaries
- candidate validation summaries
- failure patterns
- memory objects
- evidence records
- runtime truth references
- verified graph references

The AI layer may propose:

- anomaly narratives
- mechanism hypotheses
- competing explanations
- experiment designs
- belief update proposals

The AI layer may not write:

- candidate state
- candidate ranking
- replay result state
- qualification result state
- governance state
- paper-forward state
- paper position state
- trade artifacts
- capital artifacts
- portfolio artifacts
- runtime truth
- verified graph
- lifecycle transitions

Only approved non-AI controllers may write lifecycle transitions, memory acceptance, candidate state, paper state, governance state, or runtime truth.

## Gate Sequence

Before any AI artifact becomes consumable by research tooling, it must pass these gates:

1. Schema gate: artifact type and required fields are present.
2. Forbidden-output gate: no forbidden artifact or semantic equivalent.
3. Lineage gate: parent observations, source artifacts, hashes, and runtime references are present.
4. Evidence-label gate: generated outputs remain `GENERATED_ONLY`.
5. Duplicate gate: claim, mechanism, hypothesis, and experiment duplicates are checked.
6. Retired-knowledge gate: retired or falsified ideas require reopening justification.
7. Competing-explanation gate: mechanism proposals have sufficient alternatives.
8. Experiment-quality gate: experiment proposals have pass/fail criteria and data requirements.
9. Authority-boundary gate: no state mutation or approval implication.
10. Evaluation-routing gate: artifact is routed to shadow evaluation, human review, or quarantine.

Gate failure result:

```text
artifact_consumability: QUARANTINED
allowed_uses: audit only
```

## Belief Update Governance

`BeliefUpdateProposal` is not a lifecycle transition.

Allowed belief proposal directions:

```text
INCREASE_WITH_SCOPE
DECREASE_WITH_SCOPE
UNCHANGED
SPLIT_BY_REGIME
REOPEN_RECOMMENDED
RETIRE_RECOMMENDED
QUARANTINE_RECOMMENDED
```

Forbidden belief actions:

```text
SET_SUPPORTED
SET_CONFIRMED
PROMOTE
APPROVE
CAPITAL_READY
TRADE_READY
PAPER_READY
```

Belief update proposals must state:

- prior scope
- new evidence
- competing explanations affected
- contradiction handling
- uncertainty after update
- whether a lifecycle controller is required

Generated-only evidence cannot increase empirical support. It can only increase backlog interest or uncertainty clarity.

## Memory Integration Governance

AI artifacts can enter memory only as labeled proposals. They cannot become accepted memory without a non-AI acceptance step.

Memory write policy:

| Object | AI can create directly? | Required path |
| --- | ---: | --- |
| `MechanismProposal` | yes, as proposal | gated generated artifact |
| `CompetingExplanation` | yes, as proposal | gated generated artifact |
| `ExperimentProposal` | yes, as proposal | gated generated artifact |
| `BeliefUpdateProposal` | yes, as proposal | lifecycle-controller review |
| `LearningNode` | no | evidence-backed synthesis after review |
| `FailurePattern` | no | failure analysis and lineage review |
| `RetiredKnowledge` | no | lifecycle-controller transition |
| `ReopenedKnowledge` | no | explicit reopening workflow |

Memory acceptance must preserve:

- source artifact ids and hashes
- generated-only origin
- evidence maturity
- allowed uses
- forbidden uses
- contradictions
- duplicate status
- review status

## Explainability Governance

The AI must expose enough structure for a reviewer to reject it quickly.

Required explainability fields:

- source refs
- ignored relevant refs, when known
- mechanism chain
- assumptions
- competing explanations
- falsification criteria
- proposed experiment
- data requirements
- uncertainty
- authority boundary
- forbidden interpretations

The AI must not use hidden confidence as evidence. Model confidence may be logged for diagnostics, but it is not an evidence field.

## Evaluation Governance

The layer cannot be enabled beyond shadow mode until it is evaluated against a baseline.

Baseline options:

- human-authored research triage
- template-based deterministic triage
- no-AI backlog routing

Required evaluation design:

- fixed observation set
- fixed memory snapshot
- fixed candidate factory version
- fixed replay windows
- fixed governance state
- fixed scoring rules
- AI outputs labeled and separated from baseline

Required metrics:

```text
hypothesis_quality_delta
replay_support_rate_delta
candidate_conversion_delta
backtest_support_delta
paper_forward_survival_delta
repeated_failure_reduction_delta
forbidden_artifact_rate
duplicate_rate
quarantine_rate
human_rejection_rate
```

Acceptance criteria for V0.1 continuation:

- forbidden artifact rate is `0`
- authority contamination count is `0`
- duplicate rate falls or stays flat
- repeated failures fall versus baseline
- at least one core quality metric improves without deterioration in evidence maturity
- reviewer sample confirms proposals are falsifiable, not just plausible

Failure criteria:

- any forbidden output reaches a consumable artifact
- any generated-only output is treated as supported evidence
- any candidate or paper state changes because of AI output
- failure reduction is not measurable
- replay support does not improve over baseline

## Runtime Truth Boundary

The AI layer must always include runtime truth and verified graph references in its lineage metadata when those references are available. It must not infer readiness from code, design docs, memory, or model output.

Runtime truth can block use of an AI artifact even when the artifact is internally well-formed.

## Human Review Requirements

Human review is required for:

- accepting a mechanism proposal into prioritized backlog
- reopening retired or falsified knowledge
- allowing a belief update proposal to influence lifecycle state
- interpreting ambiguous experiment outcomes
- any future movement from shadow mode to limited research mode

Human review is not allowed to convert AI text into empirical evidence without supporting artifacts.

## Audit Requirements

Every AI run should emit a future `WorkerRunRecord` with:

- worker id and version
- prompt/template id and hash
- model id, if applicable
- input artifact manifest and hashes
- output artifact ids and hashes
- gate results
- quarantine results
- forbidden-output scan result
- duplicate check result
- memory check result
- runtime truth ref
- verified graph ref

Audit logs must be append-only. Failed runs are retained for failure analysis.

## Governance Verdict

The AI Research Scientist layer is governable only if it is treated as a constrained adversarial research worker. It must be structurally unable to create authority-bearing artifacts. Its outputs must be labeled, gated, auditable, and measured against a baseline before they are allowed to influence backlog priority or memory acceptance.
