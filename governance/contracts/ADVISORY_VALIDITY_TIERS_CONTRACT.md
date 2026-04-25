# Advisory Validity Tiers Contract

Contract ID: `ADVISORY_VALIDITY_TIERS_CONTRACT_V1`

## Purpose

Define one fixed validity vocabulary for the advisory domain.

## Scope

This contract applies to all future authoritative advisory records and any derived outputs that summarize their validity.

## Ownership

Governance owns the vocabulary. Advisory authorities must consume it without inventing alternative degraded-state taxonomies.

## Canonical Inputs

- advisory record validation results
- completeness checks
- freshness checks
- coverage checks
- approval checks where applicable

## Canonical Outputs

The only permitted advisory validity tiers are:

- `VALID_EXECUTION_ELIGIBLE`
- `VALID_ADVISORY_ONLY`
- `INVALID_REMEDIABLE`
- `INVALID_HARD_STOP`

## Allowed Writers

- authoritative advisory writers may assign one of the four governed tiers

## Forbidden Writers

- UI/read-model layers inventing custom validity states
- recommendation writers inventing independent confidence or degraded-state vocabularies

## Invariants

- no advisory authority may invent a fifth validity tier
- every authoritative advisory record must carry exactly one validity tier
- derived outputs may summarize but must not reinterpret validity into contradictory authority claims

## Failure States

- missing validity tier
- unsupported validity value
- conflicting validity assignment across derived surfaces

Failure handling:

- fail closed on invalid assignments
- preserve authoritative tier as the only truth

## Validity Model

This document is the validity model.

## Lineage Requirements

- validity tier must be stored on the authoritative record itself
- derived views must reference the authoritative source record when presenting validity

## Replay Expectations

- replay must reproduce the same validity tier assignment from the same governing inputs

## Projection Relationship

- UI badges, alert severities, and dashboard labels are derived
- they must not replace the governed validity tier

## Operator Intervention Rules

- manual validity overrides are forbidden unless governance later defines an explicit override record contract

