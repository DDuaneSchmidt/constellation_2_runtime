# configuration_compile_result_v1

`configuration_compile_result_v1` is the audit result for compiling a validated configuration snapshot into `compiled_active_config_v1`.

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/configuration_compile_result_v1/<COMPILE_RESULT_ID>/configuration_compile_result.v1.json`

Canonical writer owner:
- `configuration_activation_authority_v1`

Rules:
- it MUST bind one exact `configuration_policy_snapshot_v1`
- it MUST bind one exact `configuration_validation_result_v1`
- it MUST bind one exact `compiled_active_config_v1`
- `compile_status` MUST be `COMPILED` only when the referenced compiled artifact is readable and hash-stable
- activation MUST block when this artifact is missing or not `COMPILED`
