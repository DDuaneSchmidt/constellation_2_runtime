# configuration_validation_result_v1

`configuration_validation_result_v1` is the fail-closed validation artifact for a candidate `configuration_policy_snapshot_v1`.

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/configuration_validation_result_v1/<VALIDATION_RESULT_ID>/configuration_validation_result.v1.json`

Canonical writer owner:
- `configuration_activation_authority_v1`

Rules:
- it MUST bind one exact `configuration_policy_snapshot_v1`
- it MUST verify the governance utility set required by `configuration_activation_authority_v1`
- it MUST verify that lineage and dependency sidecar conventions are available and writable by contract
- if any governance utility or artifact-writing convention cannot be proven, `validation_status` MUST be `FAIL`
- compile and activation steps MUST block unless `validation_status = PASS`
