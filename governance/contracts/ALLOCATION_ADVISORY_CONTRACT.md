# Allocation Advisory Contract

Contract ID: `ADVISORY_ALLOCATION_ADVISORY_CONTRACT_V1`

## Purpose

Define value-based allocation advice as an advisory-only layer between governed current-state authority and downstream actionability.

## Scope

This contract governs only:

- target allocation
- current value allocation
- drift
- advisory reason codes

It does not authorize execution shaping by itself.

## Canonical Inputs

- exactly one valid parent `Policy`
- exactly one parent `HouseholdSnapshot`
- a governed value basis valid for value-based allocation

## Canonical Outputs

`PortfolioIntent` may carry allocation-advisory surfaces only when all of the following are explicit and distinct:

- target allocation
- current value allocation
- drift
- advisory reason codes

These surfaces must not be conflated.

## Advisory-Only Rule

Allocation advice remains advisory-only until execution shaping is explicitly applied after actionability.

At this layer:

- execution-sized trade quantities are forbidden
- broker-order semantics are forbidden
- downstream promotion may only consume explicit advisory outputs

## Value-Basis Rule

Current allocation must be computed from governed value basis only.

Value-based allocation advice must not be computed from shares alone unless a separately governed degraded mode exists and is explicitly labeled as degraded.

## Completeness Rule

Allocation advice must fail closed when:

- valuation basis is absent
- valuation basis is incomplete
- required lineage is broken
- target allocation policy is incomplete or invalid

## Determinism Rule

Same frozen policy, snapshot, and value basis inputs must yield the same:

- target allocation
- current allocation
- drift
- `PortfolioIntent` identity

Ordering, rounding, and comparison rules must be explicit and stable.

## Non-Scope

This contract does not authorize:

- optimization engines
- tax-aware rebalancing
- Monte Carlo
- hidden actionability thresholds
- execution shaping
