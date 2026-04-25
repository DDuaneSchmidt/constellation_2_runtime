# Advisory Value Basis Authority Contract

Contract ID: `ADVISORY_VALUE_BASIS_AUTHORITY_CONTRACT_V1`

## Purpose

Define the smallest governed value-basis authority that may support value-based allocation advice in the advisory kernel.

## Scope

This contract governs only:

- the valuation basis used by advisory
- the governed marks or prices frozen into advisory current-state authority
- valuation completeness and freshness status
- valuation lineage and deterministic identity

It does not create a generalized pricing platform.

## Ownership

`HouseholdSnapshot` remains the sole downstream current-state artifact for advisory.

Value basis must therefore be owned as a governed extension of snapshot authority, not as a separate advisory-side live lookup.

## Canonical Questions

The value-basis layer must answer exactly:

- what valuation mode is in effect
- what governed marks or prices are in scope
- what effective valuation timestamp applies
- whether valuation is complete enough for value-based allocation advice

## Required Semantics

Any governed value basis used by advisory must carry at minimum:

- explicit `valuation_mode`
- explicit mark or price scope
- explicit effective valuation timestamp
- explicit completeness status
- explicit freshness status
- explicit source lineage refs
- explicit reason codes when blocked or incomplete
- deterministic identity derived from frozen inputs

## Fail-Closed Rule

If value-based allocation is requested and the governed value basis is missing, incomplete, stale beyond policy, lineage-broken, or otherwise invalid:

- value-based allocation advice must not be emitted
- actionability must return blocked
- execution shaping must not begin

## Fallback Prohibition

Silent fallback from value-based allocation to pseudo-value or share-count allocation is forbidden.

If a degraded non-value mode is ever introduced, it must be:

- explicitly modeled
- explicitly labeled
- non-equivalent to governed value-based allocation
- blocked from value-based actionability or execution unless separately governed

## Determinism Rule

The same frozen snapshot inputs, same governed marks or prices, and same builder version must produce the same value-basis identity and the same valuation status.

Any governed mark or price change must change the value-basis identity deterministically.

## Non-Scope

This contract does not authorize:

- tax logic
- live market-data reads during downstream advisory
- generalized marking infrastructure
- multi-source price arbitration frameworks
- optimization engines
- prediction or stochastic modeling
