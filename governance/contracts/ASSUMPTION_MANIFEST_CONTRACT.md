# Assumption Manifest Contract

Contract ID: `ADVISORY_ASSUMPTION_MANIFEST_CONTRACT_V1`

## Purpose

Define governed assumption manifests used by advisory policy compilation and, where required, household or portfolio compilation.

## Scope

This contract governs the assumption-manifest record family only.

## Ownership

The authoritative owner is the future advisory assumption-manifest writer under governance-controlled advisory authority.

## Canonical Inputs

- approved planning assumptions
- effective dates
- suitability basis
- modeled external-state assumptions where governance permits them

## Canonical Outputs

One immutable assumption-manifest record carrying at minimum:

- `record_id`
- `assumption_manifest_id`
- `manifest_version`
- `manifest_type`
- `contract_version`
- `created_at`
- `effective_at`
- `compatible_compiler_versions`
- `assumption_payload`
- `status`

## Allowed Writers

- the governed assumption-manifest writer only

## Forbidden Writers

- recommendation producers
- UI layers
- household snapshot writers inventing assumptions ad hoc
- portfolio-intent writers inventing assumptions ad hoc

## Invariants

- immutable after write
- explicit version and scope required
- assumptions must be explicit, not implied
- compiler compatibility must be explicit
- policy compilation must reference specific manifest IDs and versions

## Failure States

- missing assumption manifest
- incomplete assumption set
- unsupported assumption scope

Failure handling:

- policy compile must fail closed when required assumptions are absent

## Validity Model

Assumption manifests may be:

- `VALID_ADVISORY_ONLY`
- `INVALID_REMEDIABLE`
- `INVALID_HARD_STOP`

## Lineage Requirements

- record ID
- contract version
- timestamp basis
- actor or source
- validity tier

## Replay Expectations

- replay must use the exact same manifest versions referenced by advisory authority records

## Projection Relationship

- explanatory summaries may describe assumptions
- they must not substitute for the manifest itself

## Operator Intervention Rules

- manual assumption changes require a new manifest record, never a mutation of an old one
