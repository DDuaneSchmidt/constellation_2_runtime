# configuration_state_v1

`configuration_state_v1` is the single published current-state truth object for the active runtime configuration activation family.

Canonical output:
- `/home/node/constellation_runtime_data/truth/configuration_state_v1/current.json`

Canonical writer owner:
- `configuration_activation_authority_v1`

Rules:
- it MUST point to:
  - one exact active `configuration_policy_snapshot_v1`
  - one exact active `compiled_active_config_v1`
  - one exact governing `configuration_activation_transaction_v1`
- it MAY additionally point to the bound validation, compile, and review artifacts for traceability
- it MUST carry `constitutional_dependency_declaration` and `constitutional_lineage`
- it MUST identify prior current state when the activation supersedes an existing active configuration
- it MUST be the only current-state artifact advanced by the configuration activation family
- blocked or failed activation transactions MUST NOT mutate this artifact
