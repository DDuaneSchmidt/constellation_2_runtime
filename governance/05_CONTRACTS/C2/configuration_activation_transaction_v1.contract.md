# configuration_activation_transaction_v1

`configuration_activation_transaction_v1` is the canonical activation boundary between candidate compiled configuration artifacts and `configuration_state_v1/current.json`.

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/configuration_activation_transaction_v1/<ACTIVATION_TRANSACTION_ID>/configuration_activation_transaction.v1.json`

Canonical writer owner:
- `configuration_activation_authority_v1`

Rules:
- it MUST bind:
  - one exact `configuration_policy_snapshot_v1`
  - one exact `configuration_validation_result_v1`
  - one exact `configuration_compile_result_v1`
  - one exact `configuration_review_diff_v1`
  - one exact `compiled_active_config_v1`
- it MUST record whether the activation is:
  - `FIRST_ACTIVATION`
  - `SUPERSEDING_ACTIVATION`
- it MUST carry `frozen_input_bundle`
- `activation_status` MUST be one of:
  - `PROMOTED`
  - `BLOCKED`
  - `FAILED_VALIDATION`
- `configuration_state_v1/current.json` MUST NOT advance unless this artifact is `PROMOTED`
