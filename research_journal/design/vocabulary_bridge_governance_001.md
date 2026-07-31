# Vocabulary Bridge Governance 001

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: GENERATED_ONLY

## Purpose

Define safe boundaries for using the validation vocabulary bridge.

The bridge maps candidate-facing regime labels to validation-facing regime labels as compatibility metadata only. It is not replay logic, qualification logic, candidate logic, or governance authority.

## Source Context

This governance note is scoped to:

- `research_journal/design/validation_vocabulary_bridge_v0_1.md`
- `research_journal/design/validation_vocabulary_alignment_001.md`
- `research_journal/reports/vocabulary_bridge_simulation_001.md`
- `research_journal/reports/validation_regime_mapping_study_001.md`
- `research_journal/reports/direct_validation_feasibility_study_001.md`

## Core Rule

```text
Original candidate vocabulary remains source truth.
Bridge output is compatibility metadata.
Bridge output may explain validation friction.
Bridge output must not change replay, qualification, candidate state, or governance state.
```

## Allowed Uses

### Diagnostic Use

Allowed diagnostic uses:

- identify candidate-regime labels that the validation classifier cannot emit
- distinguish vocabulary mismatch from missing market data
- explain why triggered samples were filtered by exact-regime matching
- classify bridge outcomes as `EXACT_MATCH`, `PARTIAL_MATCH`, `UNMAPPABLE`, or `UNKNOWN`
- highlight candidate metadata defects, such as mechanism labels placed in regime fields
- produce reviewer-facing blocker labels, such as `REGIME_VOCABULARY_MISMATCH`

Diagnostic output must preserve:

- original candidate regime
- candidate id
- mechanism
- validation regime target, if any
- mapping type
- confidence
- rationale
- ambiguity

### Evaluation Use

Allowed evaluation uses:

- offline simulations
- sensitivity studies
- sample-survival analysis
- estimation of validation attrition caused by vocabulary mismatch
- comparison of candidate outcomes under alternative mappings
- prioritization of future design work
- falsification studies showing what the bridge does not solve

Evaluation use must be clearly labeled:

- `OFFLINE_ONLY`
- `SIMULATION_ONLY`
- `NO_REPLAY_AUTHORITY`
- `NO_QUALIFICATION_AUTHORITY`

Evaluation results may say that a bridge would have changed a simulated outcome. They must not say that a candidate is validated, qualified, paper-ready, promotion-ready, or governance-ready.

### Reporting Use

Allowed reporting uses:

- summary tables
- candidate blocker reports
- research-journal design reports
- crosswalk documentation
- operator-facing explanation of why direct validation is blocked
- audit trail for future bridge design

Reporting use may include confidence, mapping type, and rationale. Reporting use must not hide the original label or collapse partial mappings into an apparent pass.

## Forbidden Uses

### Replay Overrides

Forbidden:

- replacing candidate regime filters during production replay
- allowing samples through a replay gate because a bridge row exists
- changing `allowed_regimes` at replay time
- treating `PARTIAL_MATCH` as equivalent to exact validator output
- using bridge confidence as a sample weight
- combining multiple partial mappings into a hidden pass condition
- rerunning direct validation with bridge mappings and labeling it as canonical validation

### Qualification Overrides

Forbidden:

- treating bridge-mapped samples as qualification evidence
- converting `INSUFFICIENT_DATA` to supported/confirmed because bridge simulation improves sample count
- satisfying minimum sample-size criteria through bridge metadata alone
- bypassing event metadata, intraday bars, source lineage, or statistical sufficiency
- using bridge rows to clear validation maturity gates

### Candidate Promotion

Forbidden:

- promoting a candidate because it has an `EXACT_MATCH`
- moving a candidate to paper observation because bridge output exists
- creating paper positions or paper observations from bridge output
- changing candidate state, queue status, rank, or priority as an authority action
- treating simulated bridge outcomes as real candidate outcomes

### Governance Decisions

Forbidden:

- approving governance changes from bridge output
- changing validated truth
- changing runtime truth
- changing safety gates
- changing Aegis module manifests as an authority consequence of this design alone
- using bridge output to authorize trade advice, broker execution, real capital allocation, or position sizing

## Confidence Thresholds

Confidence is a design-review score, not statistical probability.

| confidence band | interpretation | allowed use | forbidden use |
| --- | --- | --- | --- |
| `>= 0.90` | high-confidence exact or unmappable classification | diagnostic/reporting label; candidate-review explanation | replay pass, qualification pass, candidate promotion |
| `0.70-0.89` | strong partial compatibility | offline evaluation and sensitivity analysis | canonical replay substitution |
| `0.50-0.69` | weak or ambiguous compatibility | backlog triage and reviewer attention | sample inclusion or validation support |
| `0.25-0.49` | low-confidence overlap | warning only | any validation inference |
| `< 0.25` | unknown or missing evidence | block as unknown/unmapped | any positive inference |

Minimum rule:

```text
No confidence threshold grants authority.
```

Even a `0.95` `EXACT_MATCH` is only a vocabulary compatibility finding. It does not prove that the candidate has sufficient samples, correct data, valid event metadata, intraday coverage, or replay support.

## Mapping-Type Handling

### `EXACT_MATCH`

Allowed handling:

- report as label-compatible
- include in diagnostic summaries
- use as a candidate for future governed implementation design

Required caveat:

- exact vocabulary match still inherits the validation classifier's definition

Forbidden handling:

- treating exact vocabulary match as replay success
- treating exact vocabulary match as qualification evidence

### `PARTIAL_MATCH`

Allowed handling:

- report as approximation only
- include ambiguity and rationale
- run offline sensitivity studies
- rank design priorities
- request human review before any future implementation proposal

Required caveats:

- partial matches must preserve both source and target labels
- partial matches must not be symmetric by default
- partial matches must not be collapsed into an exact pass
- partial matches must remain separate from real replay outcomes

Handling by confidence:

- `>= 0.70`: may be used for offline evaluation.
- `0.50-0.69`: may be used for triage only.
- `< 0.50`: warning only.

For current bridge rows:

- `CHOP -> RANGE_BOUND`, confidence `0.75`: offline evaluation allowed; no replay authority.
- `CHOP -> LOW_VOLATILITY`, confidence `0.45`: warning/diagnostic only.
- `TREND -> TRENDING`, confidence `0.70`: alias evaluation allowed; no replay authority.

### `UNMAPPABLE`

Allowed handling:

- report as a blocker
- identify metadata defects
- route to candidate-spec cleanup
- separate from missing data

Required result:

- fail closed for bridge compatibility

Examples:

- `BREAKOUT` as a regime is unmappable because it is a mechanism.
- `MEAN_REVERSION` as a regime is unmappable because it is a mechanism.
- `REVERSAL` as a regime is unmappable because it is a mechanism.
- `EVENT_REACTION` as a regime is unmappable because it requires event metadata, not regime translation.

Forbidden handling:

- mapping mechanism labels to regimes without a separate governed candidate-regime definition
- treating unmappable as unconstrained
- replacing unmappable with `UNKNOWN` to avoid a blocker

### `UNKNOWN`

Allowed handling:

- report as unresolved
- request source metadata
- keep candidate in diagnostic backlog
- distinguish from data insufficiency

Required result:

- fail closed for bridge compatibility

Forbidden handling:

- using `UNKNOWN` as a permissive pass-through
- treating `UNKNOWN` as evidence of `CHOP`
- treating missing regime as unconstrained unless the validation plan explicitly declares unconstrained-regime testing

## Partial-Match Controls

Every partial match must include:

- source candidate regime
- target validation regime
- confidence
- rationale
- ambiguity
- source artifact or design basis
- statement that it has no replay or qualification authority

Every partial-match report must answer:

- what evidence the mapping preserves
- what meaning the mapping loses
- what data or metadata is still required
- what failure mode is most likely

Partial matches must be directional. For example:

```text
CHOP -> RANGE_BOUND may be useful as daily approximation.
RANGE_BOUND -> CHOP is not automatically valid.
```

## Unmappable Handling

Unmappable rows are not errors to smooth over. They are auditable blockers.

Required report fields:

- candidate id, if available
- original value
- field where value appeared
- reason unmappable
- suggested remediation owner
- whether the value appears to be a mechanism, typo, future regime, or missing metadata

Suggested classifications:

- `MECHANISM_IN_REGIME_FIELD`
- `UNSUPPORTED_REGIME`
- `MISSING_CANDIDATE_REGIME`
- `FUTURE_REGIME_NOT_DEFINED`
- `EVENT_METADATA_REQUIRED`

## Governance Review Triggers

Open a governance/design review if any of these occur:

- a partial match is proposed for canonical replay
- a bridge row is proposed to change candidate state
- a bridge confidence value is changed after outcome review
- a new candidate regime appears more than once
- a mechanism label appears in a regime field
- `UNKNOWN` mappings are repeatedly used in reports
- bridge output is cited as validation evidence
- bridge output conflicts with verified runtime truth

## Safe Report Language

Allowed language:

- "vocabulary-compatible"
- "partial vocabulary approximation"
- "diagnostic bridge result"
- "simulation-only result"
- "requires governed implementation before replay use"
- "does not grant validation authority"

Forbidden language:

- "validated by bridge"
- "qualified by bridge"
- "replay passed after bridge"
- "bridge-approved"
- "paper-ready"
- "promotion-ready"
- "governance-ready"
- "trade-ready"

## Retirement Criteria

Retire this governance design when any of the following becomes true:

- candidate and validation systems share a single governed regime vocabulary
- a versioned bridge implementation exists with schema, tests, manifests, and audit coverage
- the validation classifier emits all candidate regimes directly, including a tested `CHOP` definition
- event-reaction candidates move to an event-aware validation path where regime bridging is no longer a primary blocker
- intraday regime modeling supersedes daily approximation for `CHOP`
- bridge outputs repeatedly cause false readiness claims
- verified runtime truth or Aegis governance introduces a stricter boundary that supersedes this document

Retirement must preserve a record of:

- final bridge rows
- reason for retirement
- replacement authority, if any
- open risks
- candidates or reports that previously referenced bridge output

## Authority Boundary

This document is design-only and `GENERATED_ONLY`.

It does not implement a vocabulary bridge, modify replay, modify validation, override qualification, promote candidates, alter governance, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, or alter safety gates.

