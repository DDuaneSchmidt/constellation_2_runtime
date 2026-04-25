# Advisory Domain Lineage Contract

Contract ID: `ADVISORY_DOMAIN_LINEAGE_CONTRACT_V1`

## Purpose

Define the canonical lineage model for advisory authority and its handoff into the trading core.

## Scope

This contract governs lineage across:

- `InvestorIntent`
- `Policy`
- `HouseholdSnapshot`
- `PortfolioIntent`
- `PromotionRecord`

and their linkage into the existing trading authority chain.

## Ownership

Ownership is shared by governance and all future advisory authority writers, but each object retains its own single writer boundary.

## Canonical Inputs

- authoritative advisory records
- authoritative trading records
- assumption manifests
- freshness attestations
- manual override records where applicable

## Canonical Outputs

The advisory lineage must reconstruct:

`InvestorIntent -> Policy -> HouseholdSnapshot -> PortfolioIntent -> PromotionRecord -> Trading Snapshot -> PortfolioDecision -> ExecutionAction -> BrokerEvent -> PositionState`

## Allowed Writers

- authoritative record writers for the advisory and trading objects only

## Forbidden Writers

- UI layers
- dashboard collectors
- recommendation prose generators
- read-model materializers
- analytics/reporting writers acting as lineage owners

## Invariants

- every authoritative record must carry a stable record ID
- parent lineage IDs must be explicit
- contract version required on every authoritative record
- lineage must be queryable without UI-specific logic
- projections must not become hidden lineage owners

## Failure States

- missing parent lineage
- incompatible contract versions
- ambiguous promotion-to-trading linkage
- replay mismatch

Failure handling:

- fail closed
- emit explicit remediation or discrepancy artifacts

## Validity Model

All advisory authorities use:

- `VALID_EXECUTION_ELIGIBLE`
- `VALID_ADVISORY_ONLY`
- `INVALID_REMEDIABLE`
- `INVALID_HARD_STOP`

## Lineage Requirements

Every authoritative advisory record must carry:

- `record_id`
- `parent_record_id` or `parent_record_ids`
- `contract_version`
- `compiler_or_model_version` when applicable
- `timestamp_basis`
- `household_id`
- `account_scope` when applicable
- `actor_or_source`
- `validity_tier`
- `assumption_manifest_refs` when applicable

## Replay Expectations

- replay must reconstruct equivalent advisory lineage and the same promotion-to-trading joins for the same canonical inputs

## Projection Relationship

- decision-chain reports, recommendation summaries, and dashboards may reference lineage
- they must never define lineage

## Operator Intervention Rules

- manual overrides, exclusions, approvals, denials, and classification changes must be first-class lineage records

## Legacy Surface Status Under This Contract Set

The following legacy advisory artifacts are non-authoritative under this contract set:

- `official_recommendation_set_v1`
- recommendation-led `decision_plan_v1` as advisory authority
- `planning_snapshot_v1` as household authority
- fragmented `promotion_*` artifacts as final promotion authority

They may remain only as compatibility, migration, replay, or historical surfaces until runtime cutover completes.

