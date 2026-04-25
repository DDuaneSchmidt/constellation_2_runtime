# release_current_v1

`release_current_v1` is the reducer-owned canonical runtime authority current for the active release/runtime context.

Status:
- `CANONICAL_RUNTIME_AUTHORITY_ACTIVE`
- required by runtime authority helper resolution
- required by startup preflight before runtime identity load

Canonical output:
- `/home/node/constellation_runtime_data/truth/release_current_v1/current.json`

Reducer owner:
- `truth_kernel_release_current_reducer_v1`

Purpose:
- bind the active release manifest, active runtime contract, and active configuration state into one replayable reducer-owned current
- provide the canonical runtime authority fields consumed by runtime/bootstrap/orchestrator/systemd authority paths
- carry runtime authority fields required for path and identity derivation, including:
  `allowed_truth_roots` and `primary_execution_identity_ref`

Allowed inputs:
- semantic surface `release.release_manifest_active`
- semantic surface `release.active_runtime_contract`
- semantic surface `policy.configuration_state_current`

Schema:
- `governance/04_DATA/SCHEMAS/C2/RUNTIME/release_current.v1.schema.json`

Required lineage:
- `release_current_id`
- `reducer_owner`
- `activation_state`
- `generated_at_utc`
- `release_manifest_ref`
- `active_runtime_contract_ref`
- `configuration_state_ref`
- `compiled_active_config_ref`
- `configuration_activation_transaction_ref`
- `lineage.producer_module`
- `lineage.code_version`
- `lineage.replay_input_refs`
- `lineage.shadow_only_reason_code`

Fail-closed rules:
- reducer MUST fail if any required input surface is missing or invalid
- reducer MUST fail if `release_manifest_active.release_id` and `active_runtime_contract.release_id` differ
- reducer MUST fail if `release_manifest_active.git_sha` and `active_runtime_contract.git_sha` differ
- reducer MUST fail if `release_manifest_active.release_root` and `active_runtime_contract.release_root` differ
- reducer MUST fail if `active_runtime_contract.allowed_truth_roots` is missing, not a list of at least two entries, or omits either `canonical_truth_root` or `truth_sleeves_root`
- reducer MUST fail if `active_runtime_contract.primary_execution_identity_ref` is missing or invalid
- reducer MUST fail if `active_runtime_contract.primary_execution_identity_ref.authority_owner != execution_identity_binding_v1`
- reducer MUST fail if `configuration_state_current.status != ACTIVE`
- reducer MUST fail if `configuration_state_current` does not expose both `compiled_active_config_ref` and `configuration_activation_transaction_ref`
- reducer MUST fail if an explicit target truth root does not equal `active_runtime_contract.canonical_truth_root`
- runtime helper resolution MUST fail closed if `release_current_v1/current.json` is missing
- runtime helper resolution MUST fail closed if `release_current_v1/current.json` exists but is invalid

Write prohibition:
- no code outside `truth_kernel_release_current_reducer_v1` may write `release_current_v1/current.json`
- non-reducer writes are prohibited across runtime/bootstrap/orchestrator/systemd paths

Authority boundary:
- `release_current_v1/current.json` is the canonical runtime authority surface
- `release.release_manifest_active` is compatibility/reducer-input only
- `release.active_runtime_contract` is compatibility/reducer-input only
- `configuration_state_v1/current.json` remains canonical only for the configuration-activation family

Dual-truth prohibition:
- runtime/bootstrap/orchestrator/systemd control decisions MUST NOT treat
  `release.release_manifest_active` or `release.active_runtime_contract` as co-equal primary authority
- compatibility reads of those legacy surfaces are allowed only for:
  reducer inputs, parity validation, explicit proof/diagnostic outputs, and stamping metadata
