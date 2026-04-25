# Advisory Actionability Gate Contract

Contract ID: `ADVISORY_ACTIONABILITY_GATE_CONTRACT_V1`

## Purpose

Define actionability as a pure advisory gate over governed allocation drift.

## Scope

This contract governs only whether allocation advice should be promoted now.

It does not own portfolio construction or execution shaping.

## Canonical Inputs

- exactly one parent `Policy`
- exactly one parent `HouseholdSnapshot`
- exactly one parent `PortfolioIntent`
- governed value-basis validity state

## Allowed Evaluation Factors

The actionability gate may evaluate only:

- drift magnitude
- valuation-basis validity
- policy-owned rebalance thresholds
- policy-owned minimum trade thresholds
- lineage validity
- stale or superseded state when explicitly governed

## Allowed Outcomes

The gate outcomes are limited to:

- `promote`
- `no_action`
- `blocked`

## Purity Rule

The actionability gate must not:

- recompute allocation
- infer missing valuation inputs
- hide valuation defects behind defaults
- construct executable payloads
- become a second policy engine outside `Policy`

## Fail-Closed Rule

If valuation basis or allocation lineage cannot be proven, the gate outcome must be `blocked`.

If drift is below governed materiality thresholds, the outcome must be `no_action`.

Only governed, material, lineage-valid drift may yield `promote`.

## Determinism Rule

Same frozen inputs and same policy thresholds must yield the same actionability result and the same decision identity.
