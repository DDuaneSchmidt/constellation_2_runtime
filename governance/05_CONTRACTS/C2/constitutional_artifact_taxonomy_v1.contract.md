# constitutional_artifact_taxonomy_v1

This contract governs the practical artifact taxonomy used by the constitutional runtime layer.

## Governing registry

- `governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json` is the active constitutional artifact contract registry on the paper-day route
- registry rows are governed by `governance/04_DATA/SCHEMAS/C2/RUNTIME/constitutional_artifact_authority_registry.v1.schema.json`

## Required contract fields

Every governed artifact contract row must declare:
- `artifact_id`
- `artifact_class`
- `authoritative_writer`
- `authoritative_domain`
- `authoritative_root_type`
- `schema_relpath`
- `root_policy`
- `authoritative_path_pattern`
- `legal_consumers`
- `required_upstream_dependencies`
- `initial_finality_state`
- `allowed_finality_states`
- `append_only_audit`
- `immutable_policy_snapshot_required`
- `frozen_input_bundle_required`
- `dependency_law`
- `materialization_policy`

## Root policy meanings

- `canonical_only`
  - artifact exists only in canonical truth
- `execution_only`
  - artifact exists only in execution truth
- `mirrored`
  - one authoritative root plus one declared bridge for mirrors
- `read_only_projection`
  - non-authoritative view/projection surface only

## Materialization policy meanings

- `produce_only`
  - only the authoritative writer may emit the artifact
- `consume_only`
  - the surface is read-only and cannot self-materialize
- `may_materialize_if_missing`
  - only the declared owned materializer may repair absence
- `must_block_if_missing`
  - consumer must fail closed when absent

## Finality semantics

- `provisional`
  - not yet closed or still open to correction
- `finalized`
  - authoritative closed artifact for its effective time
- `corrected`
  - authoritative replacement that explicitly references the prior artifact
- `superseded`
  - prior artifact retained for lineage but no longer current
- `archived`
  - retained for lineage and replay, not current

## Dependency declaration law

- every dependency consumed in constitutional runtime adoption must appear in the governing contract row
- every dependency ref emitted in adopted artifacts must be present in the declared dependency set
- observation of undeclared artifacts is a contract breach

## Read-model law

- read models must use `surface_kind=projection` or `surface_kind=composition`
- read models must not claim `authoritative_writer`
- read models must not claim `authoritative_root_type`
- read models must not claim governed finality or canonical writer status

## Current adoption scope

This pass constitutionally adopts:
- `economic_state_build_v1`
- `execution_build_v1`
- UI projection metadata validation

Other runtime surfaces remain valid but are outside the first constitutional adoption wave.
