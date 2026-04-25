# Investor Intent Contract

Contract ID: `ADVISORY_INVESTOR_INTENT_CONTRACT_V1`

## Purpose

Define `InvestorIntent` as the authoritative advisory record for what an investor wants, independent of market state, broker state, recommendation text, or execution semantics.

## Scope

This contract governs the advisory-domain authority for investor-directed goals, preferences, constraints, prohibitions, operating mode, and approval posture.

## Ownership

The authoritative owner is the future advisory intent compiler/storage boundary under `constellation_2/common/advisory/`.

## Canonical Inputs

- user-declared goals
- risk preferences
- liquidity requirements
- time horizon
- account-role preferences
- prohibited exposures
- concentration preferences
- automation preferences
- approval preferences

## Canonical Outputs

One immutable `InvestorIntent` record carrying at minimum:

- `record_id`
- `intent_id`
- `household_id`
- `contract_version`
- `created_at`
- `goals`
- `constraints`
- `operating_mode`
- `approval_preferences`
- `unresolved_fields`
- `completeness_status`

## Allowed Writers

- the governed investor-intent writer boundary only

## Forbidden Writers

- recommendation producers
- planning snapshot writers
- advisory UI read models
- decision-plan builders
- promotion-plane writers
- trading-core writers

## Invariants

- immutable after write
- explicit version and contract version required
- no market data inputs
- no broker state inputs
- no position-state inputs
- no inferred fields without an explicit governed compilation path
- same canonical inputs must reproduce the same canonical record bytes modulo timestamp fields explicitly declared variable

## Failure States

- contradictory goals
- missing risk choice
- missing liquidity requirement
- missing operating-mode choice
- unsupported enum value
- unresolved ambiguity

Failure handling:

- record may be stored as incomplete when governance permits
- no downstream `Policy` compile may proceed when the intent is non-compilable

## Validity Model

Allowed validity tiers:

- `VALID_ADVISORY_ONLY`
- `INVALID_REMEDIABLE`
- `INVALID_HARD_STOP`

`VALID_EXECUTION_ELIGIBLE` is not valid at this layer.

## Lineage Requirements

Every record must carry:

- `record_id`
- `contract_version`
- `timestamp_basis`
- `household_id`
- `actor_or_source`
- `validity_tier`

This record has no advisory parent.

## Replay Expectations

- replay must rebuild the same intent content from the same canonical user inputs
- missing optional fields must remain explicitly missing, not silently imputed

## Projection Relationship

- projections may summarize investor intent for UI or reports
- projections must never add unique business facts absent from the authoritative record

## Operator Intervention Rules

- manual intervention may only occur by producing a new governed investor-intent record or an explicit manual override record
- prior intent history must remain immutable

