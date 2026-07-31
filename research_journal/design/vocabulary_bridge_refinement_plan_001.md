# Vocabulary Bridge Refinement Plan 001

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: GENERATED_ONLY

## Scope

Refine safe usage of the validation vocabulary bridge without broadening unsafe mappings.

Inputs reviewed:

- `research_journal/reports/vocabulary_bridge_effectiveness_001.md`
- `research_journal/reports/chop_semantics_audit_001.md`
- `research_journal/design/validation_vocabulary_bridge_v0_1.md`
- `research_journal/design/vocabulary_bridge_governance_001.md`

This plan creates no implementation, no replay change, no validation change, no qualification change, no candidate promotion, no governance change, no paper-forward authorization, and no runtime authority.

## Refinement Decision

Decision: `REFINE_NARROWLY`

The bridge should keep one useful diagnostic partial mapping:

```text
CHOP -> RANGE_BOUND
mapping_type: PARTIAL_MATCH
confidence: 0.75
allowed_use: diagnostic and offline sensitivity only
```

The bridge must not expand `CHOP` into `LOW_VOLATILITY`, `UNKNOWN`, or combined permissive alternatives. The evidence shows that `CHOP -> RANGE_BOUND` can recover blocked samples and explain vocabulary mismatch, while broader mappings add false-positive risk without meaningful candidate-level improvement.

## Allowed Mappings

Allowed mappings are compatibility metadata only. They do not create replay, validation, qualification, or promotion authority.

| candidate label | validation label | mapping_type | confidence | allowed use | required caveat |
| --- | --- | --- | ---: | --- | --- |
| `TRENDING` | `TRENDING` | `EXACT_MATCH` | 0.95 | diagnostic label compatibility | Exact label match still inherits the daily validation classifier definition. |
| `RANGE_BOUND` | `RANGE_BOUND` | `EXACT_MATCH` | 0.90 | diagnostic label compatibility | Exact label match is not sample sufficiency or candidate confirmation. |
| `LOW_VOLATILITY` | `LOW_VOLATILITY` | `EXACT_MATCH` | 0.90 | diagnostic label compatibility | Low volatility remains a volatility state, not evidence of chop. |
| `HIGH_VOLATILITY` | `HIGH_VOLATILITY` | `EXACT_MATCH` | 0.90 | diagnostic label compatibility | High volatility can contain multiple mechanisms and directional states. |
| `TREND` | `TRENDING` | `PARTIAL_MATCH` | 0.70 | offline alias sensitivity only | `TREND` may describe direction, strength, or mechanism rather than regime. |
| `CHOP` | `RANGE_BOUND` | `PARTIAL_MATCH` | 0.75 | diagnostic-only partial mapping and offline sensitivity only | `RANGE_BOUND` is a daily approximation; it does not prove intraday chop. |

Required fields for every allowed mapping:

- original candidate label
- mapped validation label
- mapping type
- confidence
- rationale
- ambiguity
- allowed use
- explicit no-authority statement

## Rejected Mappings

Rejected mappings must not be used as validation substitutes, replay substitutes, sample-inclusion authority, or candidate-promotion evidence.

| candidate label | proposed validation label | required handling | reason |
| --- | --- | --- | --- |
| `CHOP` | `LOW_VOLATILITY` | reject for validation | `LOW_VOLATILITY` is a volatility magnitude state; `CHOP` is a sideways/low-directional context and can be high-volatility whipsaw. |
| `CHOP` | `UNKNOWN` | reject for validation | `UNKNOWN` is unresolved metadata, not positive evidence of `CHOP`. |
| `CHOP` | `RANGE_BOUND OR LOW_VOLATILITY` | reject as default bridge | Adds no candidate-level confirmation beyond `RANGE_BOUND` and increases semantic dilution. |
| `BREAKOUT` | any regime | `UNMAPPABLE`, fail closed | `BREAKOUT` is a mechanism, not a regime. |
| `MEAN_REVERSION` | any regime | `UNMAPPABLE`, fail closed | `MEAN_REVERSION` is a mechanism, not a regime. |
| `REVERSAL` | any regime | `UNMAPPABLE`, fail closed | `REVERSAL` is a transition mechanism, not a stable regime. |
| `EVENT_REACTION` | any regime | `UNMAPPABLE`, fail closed | Event reaction requires event metadata and timing, not regime translation. |
| missing label | any positive regime | `UNKNOWN`, fail closed | Missing metadata cannot be inferred safely. |
| unrecognized label | any positive regime | `UNKNOWN`, fail closed | Future regime, typo, free text, and malformed metadata must remain unresolved until reviewed. |

## Confidence Thresholds

Confidence is a design-review score. It is not statistical probability and must not be used as a sample weight.

| confidence band | interpretation | allowed use | forbidden use |
| --- | --- | --- | --- |
| `>= 0.90` | high-confidence exact compatibility or high-confidence unmappable classification | diagnostic reporting and review explanation | replay pass, qualification pass, candidate promotion |
| `0.70-0.89` | strong partial compatibility | offline sensitivity and vocabulary-friction analysis | canonical replay substitution or direct validation authority |
| `0.50-0.69` | weak compatibility or unresolved semantics | backlog triage and reviewer attention | sample inclusion or validation support |
| `0.25-0.49` | low-confidence overlap | warning only | any validation inference |
| `< 0.25` | missing, unknown, or unsupported | fail closed | any positive inference |

Minimum operating rule:

```text
No confidence threshold grants authority.
```

Even a high-confidence exact mapping only says labels are vocabulary-compatible. It does not prove sample sufficiency, source lineage, intraday coverage, event metadata, statistical support, candidate maturity, or paper-forward readiness.

## Candidate Impact

Observed bridge-effectiveness impact for the affected `CHOP` candidates:

| metric | baseline | with diagnostic `CHOP -> RANGE_BOUND` overlay | delta |
| --- | ---: | ---: | ---: |
| affected `CHOP` candidates | 5 | 5 | 0 |
| affected `CHOP` candidates blocked | 5 | 3 | -2 |
| affected `CHOP` block rate | 100% | 60% | -40 percentage points |
| additional samples retained | 0 | 396 | +396 |
| candidates with any sample improvement | 0 | 5 | +5 |
| additional candidates evaluable | 0 | 3 | +3 |
| simulated additional confirmations | 0 | 2 | +2 |

Candidate-level impact from the diagnostic overlay:

| candidate_id | baseline status | bridge-retained samples | diagnostic bridge outcome | impact classification |
| --- | --- | ---: | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | `INSUFFICIENT_DATA` | 146 | simulated `CONFIRMED` | material diagnostic improvement |
| `ptc_backtest_final_3a4ac24107c77136` | `INSUFFICIENT_DATA` | 146 | simulated `CONFIRMED` | material diagnostic improvement |
| `ptc_backtest_final_d5931b24bd391113` | `INSUFFICIENT_DATA` | 49 | `INSUFFICIENT_DATA` / `BACKTEST_WEAK` | sample recovery only |
| `ptc_backtest_final_7d839944a8a4070a` | `INSUFFICIENT_DATA` | 27 | `INSUFFICIENT_DATA` | below sample threshold |
| `ptc_backtest_final_b23c6756bfb3263a` | `INSUFFICIENT_DATA` | 28 | `INSUFFICIENT_DATA` | below sample threshold |

Interpretation:

- `CHOP -> RANGE_BOUND` is worth preserving because it materially separates vocabulary mismatch from true zero-sample failure.
- The mapping is not enough to promote all affected candidates; three remain blocked or weak.
- The two simulated confirmations are counterfactual diagnostic results, not canonical direct validations.
- `LOW_VOLATILITY` adds only negligible sample value and no confirmed candidates, so it should remain rejected for validation.
- `UNKNOWN` should remain a blocker/review state, not a bridge target.

## Governance Boundaries

Allowed:

- diagnostic bridge reports
- offline sensitivity studies
- sample-retention analysis
- candidate-level vocabulary-friction explanations
- reviewer-facing blocker labels
- design backlog prioritization

Forbidden:

- changing `allowed_regimes` during production replay
- treating `PARTIAL_MATCH` as exact validation
- treating bridge-retained samples as canonical validation samples
- using bridge confidence as a replay weight
- combining partial mappings into a hidden permissive pass condition
- converting `INSUFFICIENT_DATA` into `CONFIRMED` from bridge output alone
- moving candidates to paper-forward observation from bridge output alone
- replacing event metadata, intraday data, source lineage, or sample-size requirements
- replacing `UNMAPPABLE` with `UNKNOWN` to avoid a blocker
- using `UNKNOWN` as evidence of `CHOP`

Safe report language:

- `diagnostic bridge result`
- `partial vocabulary approximation`
- `simulation-only result`
- `vocabulary-friction reduction`
- `requires governed implementation before replay use`
- `does not grant validation authority`

Forbidden report language:

- `validated by bridge`
- `qualified by bridge`
- `bridge-approved`
- `replay passed after bridge`
- `paper-ready because of bridge`
- `promotion-ready because of bridge`
- `governance-ready because of bridge`

## Fail-Closed Handling

The bridge must fail closed when a label is `UNMAPPABLE`, missing, unrecognized, or only resolvable through `UNKNOWN`.

Fail-closed means:

- preserve the original label
- emit mapping type `UNMAPPABLE` or `UNKNOWN`
- provide a reason
- do not emit a positive validation target
- do not include samples because of the bridge
- route the case to metadata cleanup, event validation, intraday validation, or candidate-spec review

Specific required behavior:

```text
UNMAPPABLE -> no bridge target, no replay authority, no validation authority.
UNKNOWN -> unresolved, no positive inference, no permissive pass-through.
CHOP -> UNKNOWN -> rejected.
CHOP -> LOW_VOLATILITY -> rejected for validation.
CHOP -> RANGE_BOUND -> diagnostic-only PARTIAL_MATCH.
```

## Retirement Criteria

Retire or replace this refinement plan when any of the following is true:

- candidate and validation artifacts share one governed regime vocabulary
- the validation classifier emits `CHOP` with a tested definition
- an intraday regime model supersedes daily `RANGE_BOUND` approximation for `CHOP`
- event-reaction candidates move to an event-aware validation path where regime bridging is no longer the blocker
- a versioned bridge implementation exists with schema, tests, manifests, audit coverage, and explicit authority limits
- bridge outputs repeatedly cause reviewers to make false readiness claims
- `CHOP -> RANGE_BOUND` stops improving sample-retention or candidate-level diagnostics in repeated studies
- `CHOP -> RANGE_BOUND` produces materially worse false-positive evidence than exact validation or intraday validation
- verified runtime truth or Aegis governance introduces stricter vocabulary rules that supersede this design

Retirement record should preserve:

- final mapping table
- final confidence values
- affected candidate ids
- observed sample-retention effects
- observed false-positive concerns
- reason for retirement
- replacement authority, if any

## Final Plan

Use the bridge narrowly:

```text
Keep CHOP -> RANGE_BOUND as diagnostic-only PARTIAL_MATCH at confidence 0.75.
Reject CHOP -> LOW_VOLATILITY for validation.
Reject CHOP -> UNKNOWN for validation.
Require confidence and mapping_type in every bridge result.
Fail closed on UNMAPPABLE and UNKNOWN.
Do not treat any bridge output as canonical validation authority.
```

## Authority Boundary

This document is design-only and `GENERATED_ONLY`. It does not implement a vocabulary bridge, modify replay, modify validation, override qualification, promote candidates, alter governance, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, alter safety gates, or modify Aegis runtime truth.
