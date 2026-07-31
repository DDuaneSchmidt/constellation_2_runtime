# Research Director v0.1 Architecture 001

Status: DESIGN ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: future architecture documentation for a Research Director v0.1 layer. This document does not implement software, change runtime truth, modify Aegis behavior, create candidates, approve candidates, approve paper positions, allocate capital, provide trade advice, override replay, override qualification, override governance, or authorize live activity.

Runtime truth note: this design was written after reading the current verified runtime graph for 2026-06-05 at `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-05/verified_runtime_graph.v1.json`. The graph status is `BLOCKED`, the active mode is `HUMAN_REVIEWED_PAPER_MODE`, runtime truth classification is `PARTIAL_CONTEXT`, and blocked capabilities include `RESEARCH_READY`, `TRADE_ADVICE_ALLOWED`, `MANUAL_TRADE_CAPTURE_ALLOWED`, and `AUTONOMOUS_EXECUTION_ALLOWED`. If this design is ever implemented, the implementation must query verified truth artifacts. No consumer may infer readiness from this document, code shape, journal text, or generated output.

## Core Thesis

Research Director v0.1 should be the prioritization brain for the research system, not an authority engine.

Its job is to decide what uncertainty, hypothesis, experiment, or research portfolio item deserves attention next. It should compress the research search space by ranking work according to uncertainty severity, expected information gain, paper-forward learning value, historical failure patterns, adversary critique, and research cost.

It must not convert research priority into candidate approval, paper-forward approval, replay acceptance, qualification acceptance, governance acceptance, or capital action. Those remain separate authority surfaces governed by their own verified truth, policies, and human gates.

## Responsibilities

Research Director v0.1 owns research prioritization and triage:

- uncertainty prioritization
- experiment prioritization
- research allocation
- hypothesis triage
- research portfolio management

These responsibilities mean it may rank, route, pause, escalate for review, or recommend further research. They do not mean it may approve downstream operational state.

## Inputs

Research Director v0.1 should consume only typed, lineage-preserving research objects.

Primary inputs:

- observations
- claims
- hypotheses
- failures
- knowledge
- adversary reviews

Each input should carry:

- source artifact path or object id
- source type
- evidence level
- timestamp
- lineage hash when available
- current stage
- allowed uses
- forbidden uses
- unresolved uncertainty references

Input interpretation rules:

- Observations are evidence candidates, not conclusions.
- Claims are assertions awaiting mechanism and support.
- Hypotheses are testable research objects, not candidate permissions.
- Failures are negative knowledge and should raise priority for fast rejection of similar work.
- Knowledge records are reusable constraints, priors, and retired lessons.
- Adversary reviews are critique inputs, not veto authority by themselves.

## Outputs

Research Director v0.1 should emit rankings and research work recommendations only.

Required outputs:

- research priority rankings
- uncertainty rankings
- experiment rankings

Suggested output object families:

- `ResearchPriorityRanking`
- `UncertaintyRanking`
- `ExperimentRanking`
- `HypothesisTriageRecommendation`
- `ResearchAllocationRecommendation`
- `PortfolioAttentionMap`

Each output should include:

- ranking id
- ranked object ids
- rank score
- score components
- evidence basis
- uncertainty basis
- adversary review basis when present
- estimated research hours
- expected information gain
- expected rejection value
- expected paper-forward learning value
- stale or duplicate work flags
- blocked uses
- review due date

Allowed recommendation labels:

- `PRIORITIZE_NOW`
- `CHEAP_TEST_FIRST`
- `REQUIRE_ADVERSARY_REVIEW`
- `REQUIRE_UNCERTAINTY_REPAIR`
- `REQUIRE_EXPERIMENT_DESIGN`
- `REQUIRE_FAILURE_COMPARISON`
- `WATCH_FOR_PAPER_FORWARD_EVIDENCE`
- `DEPRIORITIZE_DUPLICATE`
- `DEPRIORITIZE_LOW_INFORMATION_GAIN`
- `PAUSE_PENDING_INPUTS`
- `TRIAGE_FOR_REJECTION`

Forbidden recommendation labels:

- `APPROVE_CANDIDATE`
- `APPROVE_PAPER`
- `APPROVE_REPLAY`
- `APPROVE_QUALIFICATION`
- `APPROVE_GOVERNANCE`
- `ALLOCATE_CAPITAL`
- `SIZE_POSITION`
- `ROUTE_ORDER`

## Explicitly Forbidden Authority

Research Director v0.1 must not perform:

- candidate approval
- replay override
- qualification override
- governance override
- capital decisions

It also must not:

- provide trade advice
- create broker execution instructions
- modify paper position state
- certify runtime readiness
- certify research readiness
- mutate the verified runtime graph
- treat its own rankings as evidence that downstream gates passed

## Ranking Model

The initial design should favor transparent ranking over opaque optimization.

Suggested score components:

| Component | Meaning |
| --- | --- |
| `uncertainty_severity` | How strongly unresolved uncertainty affects current research decisions. |
| `information_gain` | Expected learning if the work is completed. |
| `rejection_value` | Expected benefit of killing weak work early. |
| `paper_forward_learning_value` | Expected improvement in paper-forward survival understanding. |
| `candidate_conversion_relevance` | Whether resolving the item improves future valid candidate input quality. |
| `failure_similarity` | Similarity to known historical failures or negative knowledge. |
| `adversary_pressure` | Strength of critique, null explanation, or constraint raised by adversary review. |
| `research_cost` | Estimated hours or operational effort required. |
| `duplicate_penalty` | Penalty for near-duplicate mechanisms, claims, or experiments. |
| `staleness_penalty` | Penalty for old, unrefreshed, or context-expired evidence. |

Recommended ranking posture:

`priority = information_gain + rejection_value + paper_forward_learning_value + candidate_conversion_relevance + failure_similarity + adversary_pressure - research_cost - duplicate_penalty - staleness_penalty`

The formula is intentionally descriptive, not binding implementation logic. Future implementation should calibrate weights against measured outcomes and keep every score component inspectable.

## Triage Policy

Research Director v0.1 should triage hypotheses into research attention states:

| State | Meaning |
| --- | --- |
| `ACTIVE_PRIORITY` | Work is high-value and should receive near-term research attention. |
| `CHEAP_FALSIFICATION` | Work should receive the cheapest decisive rejection test first. |
| `UNCERTAINTY_BLOCKED` | Work cannot advance until a named uncertainty is repaired or scoped. |
| `ADVERSARY_BLOCKED` | Work has unresolved adversary critique that materially changes expected value. |
| `DUPLICATE_OR_STALE` | Work is redundant, stale, or already covered by stronger knowledge. |
| `PORTFOLIO_HOLD` | Work is coherent but lower priority than current alternatives. |
| `REJECTION_REVIEW` | Work appears weak enough to send to human/system rejection review. |

No triage state authorizes candidate progression, paper-forward progression, replay acceptance, qualification acceptance, governance acceptance, or capital activity.

## Research Allocation Policy

Research allocation means research attention, not capital.

Research Director v0.1 may recommend:

- researcher time allocation
- review queue ordering
- experiment queue ordering
- adversary review ordering
- uncertainty repair ordering
- portfolio attention balance across mechanism families

It must not recommend:

- capital allocation
- position sizing
- exposure limits
- broker routing
- live trading
- manual trade capture

## Portfolio Management

Research Director v0.1 should manage the research portfolio as a set of learning obligations.

Portfolio views should include:

- active hypotheses by mechanism family
- unresolved uncertainties by severity
- experiments by expected information gain
- failures mapped to current work
- stale claims requiring refresh
- duplicate mechanisms requiring compression
- paper-forward observations awaiting survival evidence
- research debt by owner or stage

Portfolio management should optimize for:

- fewer redundant hypotheses
- faster weak-hypothesis rejection
- better coverage of high-uncertainty mechanisms
- more measured paper-forward learning
- lower research hours per useful conclusion

## Success Metrics

Research Director v0.1 should be evaluated by research-process outcomes:

| Metric | Definition |
| --- | --- |
| `candidate_conversion` | Share of research items that eventually become valid candidate inputs after separate evidence and authority gates. This metric is observational and never grants approval. |
| `paper_forward_survival` | Share of paper-forward observations that remain valid through predeclared review windows. |
| `hypothesis_rejection_speed` | Median elapsed time or research hours from hypothesis intake to justified rejection. |
| `research_hours_saved` | Estimated or measured hours avoided through dedupe, early rejection, failure reuse, or uncertainty-first routing. |

Supporting metrics:

- uncertainty closure rate
- experiment information gain per hour
- adversary critique resolution rate
- duplicate hypothesis compression rate
- stale claim retirement rate
- portfolio concentration risk by mechanism family

## Boundary Tests

Any future implementation should fail validation if:

- a ranking output is consumed as candidate approval
- a ranking output is consumed as replay approval
- a ranking output is consumed as qualification approval
- a ranking output is consumed as governance approval
- a ranking output is consumed as capital instruction
- the Director infers readiness from code, manifests, or journal text
- the Director writes to runtime truth instead of querying verified truth
- the Director mutates candidate, paper, broker, governance, qualification, or capital state

## v0.1 Architecture Shape

Future implementation should be a read-only research orchestration layer:

1. Read verified research inputs and verified runtime truth references.
2. Normalize observations, claims, hypotheses, failures, knowledge, and adversary reviews into comparable research work items.
3. Attach uncertainty objects and known-failure similarities.
4. Score expected information gain, rejection value, paper-forward learning value, and research cost.
5. Emit ranked research, uncertainty, and experiment outputs.
6. Expose outputs to research consumers with explicit forbidden-use metadata.

The Director should be downstream of truth-producing systems and upstream of research queues. It should never sit in the authority path for candidate approval, replay acceptance, qualification, governance, or capital.

## Operating Principle

The Research Director should spend scarce research attention where it can most quickly convert uncertainty into knowledge. A fast, well-grounded rejection is a successful outcome when it saves research hours and protects paper-forward work from weak hypotheses.
