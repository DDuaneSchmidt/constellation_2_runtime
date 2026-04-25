# Policy Contract

Contract ID: `ADVISORY_POLICY_CONTRACT_V1`

## Purpose

Define `Policy` as the compiled enforceable advisory authority derived from `InvestorIntent` plus governed assumption manifests and suitability rules.

## Scope

This contract governs advisory policy compilation only. It does not govern trading policy projections, broker policy, or post-entry trade-state policy.

## Ownership

The authoritative owner is the future advisory policy compiler under `constellation_2/common/advisory/`.

## Canonical Inputs

- exactly one parent `InvestorIntent`
- approved assumption manifests
- suitability compiler version
- account regime rules

## Canonical Outputs

One immutable `Policy` record carrying at minimum:

- `record_id`
- `policy_id`
- `parent_intent_id`
- `contract_version`
- `compiler_version`
- `policy_fingerprint`
- `effective_at`
- `liquidity_floor`
- `concentration_caps`
- `asset_universe_rules`
- `prohibited_exposures`
- `account_treatment_rules`
- `rebalance_philosophy`
- `automation_permissions`
- `assumption_manifest_refs`
- `completeness_status`

## Allowed Writers

- the governed policy compiler boundary only

## Forbidden Writers

- recommendation producers
- UI/read-model writers
- portfolio construction writers
- trading-core writers
- broker adapters

## Invariants

- exactly one parent `InvestorIntent`
- same parent intent plus same assumption manifests plus same compiler version must produce the same policy
- policy records must bind an explicit semantic fingerprint of the compiled rule set
- no silent inference
- no market-aware optimization logic
- no broker or order semantics
- no mutable post-hoc edits

## Failure States

- parent intent missing
- non-compilable intent
- missing assumption manifest
- suitability unresolved
- unsupported operating-mode conflict

Failure handling:

- no downstream `HouseholdSnapshot` or `PortfolioIntent` authority may claim policy lineage if policy compile failed
- remediation must be explicit

## Validity Model

Allowed validity tiers:

- `VALID_ADVISORY_ONLY`
- `INVALID_REMEDIABLE`
- `INVALID_HARD_STOP`

## Lineage Requirements

Every record must carry:

- `record_id`
- `parent_record_id`
- `contract_version`
- `compiler_version`
- `timestamp_basis`
- `household_id`
- `actor_or_source`
- `validity_tier`
- `assumption_manifest_refs`

## Replay Expectations

- replay from the same intent, manifests, and compiler version must reproduce the same canonical policy output

## Projection Relationship

- policy summaries or UI cards are derived only
- no projection may silently extend or relax policy

## Operator Intervention Rules

- manual changes require a new policy record or an explicit manual override artifact with lineage to the parent intent and assumption basis
