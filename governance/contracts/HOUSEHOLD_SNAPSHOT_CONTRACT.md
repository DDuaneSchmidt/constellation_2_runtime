# Household Snapshot Contract

Contract ID: `ADVISORY_HOUSEHOLD_SNAPSHOT_CONTRACT_V1`

## Purpose

Define `HouseholdSnapshot` as the frozen advisory household fact record for one advisory cycle.

## Scope

This contract governs advisory household state capture only. It does not replace trading `PositionState`.

## Ownership

The authoritative owner is the future household snapshot builder under `constellation_2/common/advisory/`.

## Canonical Inputs

- account registry snapshot
- verified positions snapshots
- verified cash snapshots
- external holdings inputs when modeled
- classification manifests
- freshness attestations
- optional linked `Policy`

Marks and prices are not in scope for this contract version.

If future advisory logic requires them, they must be introduced by explicit contract update with governed freshness semantics.

## Canonical Outputs

One immutable `HouseholdSnapshot` record carrying at minimum:

- `record_id`
- `snapshot_id`
- `household_id`
- `policy_ref`
- `contract_version`
- `scope_declaration`
- `account_registry_snapshot`
- `investable_assets_summary`
- `liquidity_summary`
- `holdings_by_account`
- `holdings_by_asset_class`
- `holdings_by_tax_treatment`
- `verified_components`
- `unverified_components`
- `completeness_flags`
- `freshness_attestations`
- `validity_tier`
- explicit `effective_at`
- explicit completeness / freshness / reconciliation status
- explicit source lineage refs for verified-core inputs

## Allowed Writers

- the governed household snapshot builder only

## Forbidden Writers

- `planning_snapshot_v1` compatibility writers
- recommendation producers
- UI/read-model writers
- promotion-plane writers
- trading-core writers that mutate advisory state

## Invariants

- frozen after write
- explicit scope and coverage required
- explicit verified versus unverified separation
- freshness bounds required
- missing or stale verified-core coverage must fail closed before a usable snapshot is written
- cannot overwrite or redefine trading `PositionState`
- cannot become a continuously mutating household bag
- downstream advisory must not reread live positions, cash, or marks after snapshot creation

## Failure States

- stale account data
- missing account coverage
- unknown classification
- unverified balances
- incomplete external holdings

Failure handling:

- fail closed on missing verified-core positions or cash
- fail closed on stale verified-core inputs
- unverified auxiliary components may degrade validity only when verified-core coverage remains complete and current
- no downstream `PortfolioIntent` may claim execution eligibility if validity is insufficient

## Validity Model

Allowed validity tiers:

- `VALID_EXECUTION_ELIGIBLE`
- `VALID_ADVISORY_ONLY`
- `INVALID_REMEDIABLE`
- `INVALID_HARD_STOP`

## Lineage Requirements

Every record must carry:

- `record_id`
- `contract_version`
- `timestamp_basis`
- `household_id`
- `policy_ref` when present
- `account_scope`
- `actor_or_source`
- `validity_tier`
- `freshness_attestation_refs`

## Replay Expectations

- replay from the same upstream account, balance, classification, and position inputs must reproduce the same household snapshot

## Run Evidence

- every snapshot-kernel run must emit a run envelope, including blocked runs
- the run envelope must link the validation decision and the resulting `HouseholdSnapshot` when one is written

## Projection Relationship

- summary views, household timeline views, and other narratives are derived only

## Operator Intervention Rules

- exclusions, classification overrides, and manual household overrides must be first-class artifacts with lineage
- no silent edits of prior snapshots are allowed

## Migration Note

`planning_snapshot_v1` is a legacy mixed surface proven in discovery. Under this contract set it is not the canonical household authority and may survive only as a migration or compatibility surface until consumers move to `HouseholdSnapshot`.
