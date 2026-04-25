# Advisory To Trading Promotion Boundary

Contract ID: `ADVISORY_TO_TRADING_PROMOTION_BOUNDARY_V1`

## Purpose

Define the sole future boundary by which advisory authority may create trading-core lineage.

## Scope

This contract governs the seam between advisory authority and the existing trading core.

## Ownership

The authoritative owner is the future `PromotionRecord` boundary under `constellation_2/common/advisor_bridge/`.

## Canonical Inputs

- exactly one parent `PortfolioIntent`
- execution mode
- account scope
- freshness requirements
- approval requirements
- operational readiness evidence

## Canonical Outputs

- exactly one `PromotionRecord`
- optional linkage into trading `Snapshot` only when promotion is execution-eligible and all boundary conditions pass

## Allowed Writers

- the `PromotionRecord` writer boundary only

## Forbidden Writers

- recommendation producers
- legacy fragmented promotion writers acting as final authority
- UI or dashboard components
- trading submit boundaries creating advisory promotion records retroactively

## Invariants

- this boundary is the only advisory-to-trading promotion authority under the new model
- `PortfolioIntent` is the only future advisory parent eligible for promotion
- blocked scope must remain explicit
- promotion does not produce broker actions
- trading scope must be a subset of promoted eligible scope

## Failure States

- portfolio intent stale
- approval missing
- readiness missing
- account scope mismatch
- duplicate promotion risk

Failure handling:

- write a blocked `PromotionRecord`
- do not emit trading lineage

## Validity Model

- boundary results use the governed four-tier validity vocabulary carried by `PromotionRecord`

## Lineage Requirements

- advisory parent lineage must be explicit
- created trading lineage, when present, must point to the resulting trading `Snapshot`

## Replay Expectations

- replay must reproduce the same promotion decision and the same resulting trading linkage when inputs and rules are unchanged

## Projection Relationship

- promotion readiness dashboards and operator views are derived only

## Operator Intervention Rules

- approvals, denials, and scope changes require explicit intervention records
- no silent promotion of blocked scope

## Mandatory Recommendation-Authority Demotion

Under this contract set, the following are not advisory promotion authorities:

- `official_recommendation_set_v1`
- recommendation-led `decision_plan_v1`
- `promotion_candidate_v1`
- `promotion_review_v1`
- `promotion_manual_review_v1`
- `promotion_gate_result_v1`

If compatibility requires them to remain temporarily, they are deprecated-derived or migration-shim surfaces only.

