# compiled_active_config_v1

`compiled_active_config_v1` is the only runtime-consumable configuration artifact in the configuration activation family.

Canonical output:
- `/home/node/constellation_runtime_data/truth/compiled_active_config_v1/<COMPILED_CONFIG_ID>/compiled_active_config.v1.json`

Canonical writer owner:
- `configuration_activation_authority_v1`

Rules:
- it MUST derive only from:
  - one exact `configuration_policy_snapshot_v1`
  - one exact passing `configuration_validation_result_v1`
- it MUST publish deterministic compiled configuration refs ordered for runtime consumption
- runtime components MUST consume this artifact only through the ref published by `configuration_state_v1/current.json`
- runtime components MUST NOT consume raw `configuration_policy_snapshot_v1` directly
- it MUST carry `constitutional_dependency_declaration` and `constitutional_lineage`
