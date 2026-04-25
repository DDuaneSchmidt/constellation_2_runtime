# Tax Scope And Truth Model

Contract ID: `C2_TAX_SCOPE_AND_TRUTH_MODEL_V1`

## Purpose

Define the canonical tax scope model, the observed-to-accepted truth flow, and the single-writer
rules for the tax domain foundation.

## Scope Types

The tax domain recognizes these explicit scope types:

- `operator_scope`
- `legal_owner_scope`
- `filing_scope`
- `wash_enforcement_scope`
- `advisory_scope`

Every governed tax artifact must bind the applicable scope ids explicitly. Scope inference is
forbidden.

## Truth Stages

Tax truth moves only through these stages:

- `observed`
- `candidate`
- `accepted`
- `provisional`
- `rejected`
- `corrected`
- `restricted_use`

Observed tax events are not accepted tax truth.

## Single-Writer Rule

Each tax truth family must have exactly one writer boundary for a given scope and identity.
Multiple active writers for the same fact family and scope are forbidden.

## Fact Acceptance Gate

The tax domain must normalize observed events into explicit candidates and evaluate them through a
fact-acceptance gate before any accepted fact is emitted.

The gate must:

- validate scope bindings
- validate required fields for the fact family
- assign stable machine-readable reason codes
- emit explicit acceptance, provisional, restricted-use, or rejection decisions

No observed event may become accepted truth implicitly.

## Accepted Fact Immutability

Accepted tax facts are append-only immutable records.

- prior accepted facts must never be mutated in place
- acceptance state must be durable
- accepted facts must remain queryable at the decision-time boundary used to build a snapshot or a
  decision

## Correction Lineage

Corrections are additive lineage events.

- the original accepted fact must remain preserved
- the correcting accepted fact must be a new record
- the correction artifact must reference the superseded fact id
- the correction artifact must record the correction reason
- where deterministically knowable, the correction artifact should identify affected snapshot ids
  and decision ids

## Imported / Adopted Positions

Imported or adopted positions must carry explicit confidence and restriction state.

- unknown basis must disable optimization
- unknown holding period must disable ST/LT optimization
- reconstructed basis must remain policy-governed and may be preview-only or review-only
- imported positions must never silently become full-optimization eligible

## Corporate Action Extension Seam

Corporate action handling is reserved as an extension seam in this pass.

- observed corporate action events may be recorded
- unresolved corporate action impact must surface explicit restriction and block reason codes
- no snapshot or decision may assume corrected basis or holding period where corporate action
  impact is unresolved

