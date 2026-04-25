# Portfolio Intent Contract

Contract ID: `ADVISORY_PORTFOLIO_INTENT_CONTRACT_V1`

## Purpose

Define `PortfolioIntent` as the authoritative portfolio-target record produced from `Policy` and `HouseholdSnapshot`.

## Scope

This contract governs portfolio-target authority only. It must not encode broker instructions, order semantics, or narrative recommendation text.

## Ownership

The authoritative owner is the future portfolio-intent compiler under `constellation_2/common/advisory/`.

## Canonical Inputs

- exactly one parent `Policy`
- exactly one parent `HouseholdSnapshot`
- approved construction model version
- classification manifest versions

## Canonical Outputs

One immutable `PortfolioIntent` record carrying at minimum:

- `record_id`
- `portfolio_intent_id`
- `parent_policy_id`
- `parent_household_snapshot_id`
- `contract_version`
- `model_version`
- `target_allocation_intent`
- `required_directional_adjustments`
- `constrained_deviations_accepted`
- `blocked_conditions`
- `unresolved_conflicts`
- `automation_eligible_scope`
- `rationale_codes`
- `validity_tier`

## Allowed Writers

- the governed portfolio-intent compiler only

## Forbidden Writers

- recommendation producers
- `decision_plan_v1` builders acting as authority
- UI/read-model writers
- broker adapters
- execution boundaries

## Invariants

- exactly one parent `Policy`
- exactly one parent `HouseholdSnapshot`
- same parents plus same model versions must produce the same output
- target math must be internally consistent
- no broker or order semantics
- no recommendation prose
- blocked conditions and unresolved conflicts must remain explicit

## Failure States

- no feasible compliant target
- policy conflict
- classification gap
- unapproved model version

Failure handling:

- output may be blocked or unresolved
- promotion eligibility must remain false when validity does not permit execution

## Validity Model

Allowed validity tiers:

- `VALID_EXECUTION_ELIGIBLE`
- `VALID_ADVISORY_ONLY`
- `INVALID_REMEDIABLE`
- `INVALID_HARD_STOP`

## Lineage Requirements

Every record must carry:

- `record_id`
- `parent_record_ids`
- `contract_version`
- `model_version`
- `timestamp_basis`
- `household_id`
- `account_scope`
- `actor_or_source`
- `validity_tier`
- `classification_manifest_refs`

## Replay Expectations

- replay from the same parents and same model version must reproduce the same portfolio intent

## Projection Relationship

- recommendation summaries, dashboards, and prose are derived only

## Operator Intervention Rules

- manual overrides must produce explicit override records or new authoritative portfolio intents, never mutate history

## Legacy Surface Note

`official_recommendation_set_v1` and recommendation-led `decision_plan_v1` are not portfolio-target authorities under this contract set. They may remain only as migration or derived compatibility surfaces.

