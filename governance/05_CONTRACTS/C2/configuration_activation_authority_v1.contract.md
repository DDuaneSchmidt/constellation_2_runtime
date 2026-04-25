# configuration_activation_authority_v1

`configuration_activation_authority_v1` is the governance-first activation family for runtime configuration change control.

Canonical configuration activation outputs:
- `/home/node/constellation_runtime_data/truth/configuration_policy_snapshot_v1/<POLICY_SNAPSHOT_ID>/configuration_policy_snapshot.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/configuration_validation_result_v1/<VALIDATION_RESULT_ID>/configuration_validation_result.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/configuration_compile_result_v1/<COMPILE_RESULT_ID>/configuration_compile_result.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/configuration_review_diff_v1/<REVIEW_DIFF_ID>/configuration_review_diff.v1.json`
- `/home/node/constellation_runtime_data/truth/compiled_active_config_v1/<COMPILED_CONFIG_ID>/compiled_active_config.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/configuration_activation_transaction_v1/<ACTIVATION_TRANSACTION_ID>/configuration_activation_transaction.v1.json`
- `/home/node/constellation_runtime_data/truth/configuration_state_v1/current.json`

Canonical writer ownership:
- every artifact in this family is owned by `configuration_activation_authority_v1`
- no other family may publish or advance `configuration_state_v1/current.json`

Schema ownership and versioning:
- this family is governed by `governance/04_DATA/SCHEMAS/C2/RUNTIME/*.v1.schema.json`
- every artifact payload MUST declare `schema_id=<artifact>.v1`
- every artifact payload MUST declare `schema_version=v1`
- breaking payload changes require:
  - a new schema file
  - a new contract file
  - a new manifest registration
  - a new artifact-authority registry row
- in-place schema mutation is forbidden once adopted

Lineage and dependency sidecars:
- every artifact in this family MUST carry `constitutional_dependency_declaration`
- every artifact in this family MUST carry `constitutional_lineage`
- `constitutional_dependency_declaration` MUST validate against:
  - `governance/04_DATA/SCHEMAS/C2/RUNTIME/artifact_dependency_declaration.v1.schema.json`
- `constitutional_lineage` MUST validate against:
  - `governance/04_DATA/SCHEMAS/C2/RUNTIME/governed_artifact_lineage.v1.schema.json`
- `configuration_activation_transaction.v1` MUST also carry `frozen_input_bundle`
- `frozen_input_bundle` MUST validate against:
  - `governance/04_DATA/SCHEMAS/C2/RUNTIME/frozen_decision_input_bundle.v1.schema.json`
- every bound artifact ref in this family MUST carry exact path and sha256

Required governance utility evidence:
- `governance/00_MANIFEST.yaml`
- `governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json`
- `governance/05_CONTRACTS/C2/constitutional_runtime_architecture_v1.contract.md`
- `governance/05_CONTRACTS/C2/constitutional_artifact_taxonomy_v1.contract.md`
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/constitutional_artifact_authority_registry.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/artifact_dependency_declaration.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/governed_artifact_lineage.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/frozen_decision_input_bundle.v1.schema.json`

Lifecycle:
1. `validate`
   - consume only `configuration_policy_snapshot.v1`
   - fail closed when any required governance utility is missing, unreadable, or hash-unproven
   - fail closed when artifact-writing conventions required by this contract family cannot be proven
2. `compile`
   - require `configuration_validation_result.v1.validation_status = PASS`
   - produce `compiled_active_config.v1`
   - emit `configuration_compile_result.v1`
   - runtime MUST still remain on the previously active compiled configuration until activation succeeds
3. `review diff`
   - require candidate `compiled_active_config.v1`
   - if `configuration_state_v1/current.json` is absent, review kind MUST be `FIRST_ACTIVATION`
   - if `configuration_state_v1/current.json` exists, review kind MUST be `SUPERSEDING_ACTIVATION`
   - diff output must compare candidate compiled refs against the currently active compiled refs only
4. `activate`
   - require:
     - validation pass
     - compile success
     - review diff readiness
     - candidate compiled config readability
   - fail closed if any prerequisite artifact is missing, unreadable, schema-invalid, or hash-mismatched
   - emit `configuration_activation_transaction.v1`
5. `publish current state`
   - require `configuration_activation_transaction.v1.activation_status = PROMOTED`
   - `configuration_state_v1/current.json` is the only published current-state surface in this family
   - `configuration_state_v1/current.json` MUST point to:
     - the active `configuration_policy_snapshot.v1`
     - the active `compiled_active_config.v1`
     - the governing `configuration_activation_transaction.v1`

Runtime consumption law:
- runtime components MUST consume only the `compiled_active_config.v1` referenced by `configuration_state_v1/current.json`
- runtime components MUST NOT consume raw `configuration_policy_snapshot.v1` directly
- runtime components MUST fail closed if `configuration_state_v1/current.json` is missing, invalid, or points to an unreadable compiled configuration

Fail-closed requirements:
- missing governance utility evidence blocks validation
- missing lineage/dependency sidecars block every family artifact
- missing candidate compiled configuration blocks review and activation
- missing or invalid prior current state blocks superseding review diff
- blocked or failed activation MUST NOT advance `configuration_state_v1/current.json`
- partial publication is forbidden:
  - no current-state write without a promoted activation transaction
  - no activation transaction claiming promotion without a readable compiled config

Future implementation gates:
- runtime writer implementation gate
  - runtime writer code may be added only after this contract family, schemas, manifest registrations, and artifact-authority registry rows are merged unchanged
- focused activation test gate
  - no `/api/configuration` surface or UI work may begin until focused tests prove:
    - first activation
    - superseding activation
    - `configuration_state_v1/current.json` path/sha256 binding
    - required lineage/dependency sidecars on all written artifacts
