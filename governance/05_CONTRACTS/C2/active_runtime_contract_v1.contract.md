# C2 Active Runtime Contract V1

## Authority Status

- `active_runtime_contract.v1.json` is a compatibility artifact and reducer-input source.
- It is not the canonical runtime authority current.
- Canonical runtime authority is `release_current_v1/current.json`.

## Authority

- `/home/node/constellation` remains the development and governance authority.
- `/home/node/constellation_active` is the active execution root for validated release-root runs.
- `/home/node/constellation_runtime_data` is the canonical runtime-data root.

PROOF: `governance/05_CONTRACTS/C2/release_root_activation_v1.contract.md`

## Artifact

- The active runtime model MUST be written to:
  `/home/node/constellation_runtime_data/runtime_contract_v1/active_runtime_contract.v1.json`
- The artifact MUST validate against:
  `governance/04_DATA/SCHEMAS/C2/RUNTIME/active_runtime_contract.v1.schema.json`
- Compatibility scope for this artifact:
  reducer inputs, parity validation/comparison, and explicit proof/diagnostic evidence.

## Required Fields

- `schema_id`
- `schema_version`
- `release_id`
- `git_sha`
- `authoritative_repo_root`
- `release_root`
- `runtime_data_root`
- `canonical_truth_root`
- `truth_sleeves_root`
- `pointer_index_family`
- `allowed_truth_roots`
- `provenance_mode`
- `runtime_environment`
- `primary_execution_identity_ref`
- `generated_at_utc`
- `status`

## Runtime Model

- `authoritative_repo_root` MUST identify the governed design authority for the active runtime.
- The current authoritative repo root is `/home/node/constellation`.
- `release_root` MUST equal the resolved target of `/home/node/constellation_active`.
- `runtime_data_root` MUST equal `/home/node/constellation_runtime_data`.
- `canonical_truth_root` MUST equal `/home/node/constellation_runtime_data/truth`.
- `truth_sleeves_root` MUST equal `/home/node/constellation_runtime_data/truth_sleeves`.
- `pointer_index_family` MUST select exactly one canonical pointer family for the active model.
- The current canonical pointer family is `run_pointer_v1`.
- `provenance_mode` MUST be `release_manifest`.
- `runtime_environment` is part of the active runtime identity and MUST reflect the governed execution mode used by the canonical startup seam.
- The current canonical startup seam environment is `PAPER`.
- `primary_execution_identity_ref` MUST reference the governed execution identity binding authority instead of copying broker host, port, or client identifiers into the runtime contract.
- `primary_execution_identity_ref.authority_owner` MUST be `execution_identity_binding_v1`.
- `primary_execution_identity_ref.sleeve_id` MUST identify the governed startup sleeve used by the canonical observer/control seam.

## Consumption

- Runtime/bootstrap/orchestrator/systemd primary authority MUST resolve via release-current-required helper paths.
- Post-admission pointer producers and consumers MUST use the same `pointer_index_family` declared by the active runtime authority model.
- Release provenance for release-root execution MUST come from governed authority surfaces, not from a live `.git` worktree.
- Canonical startup wrappers MUST emit auditable startup identity evidence after authority resolution and before handing off to runtime behavior.

## Non-Authority Restrictions

- Runtime/bootstrap/orchestrator/systemd control decisions MUST NOT treat `active_runtime_contract.v1.json` as co-equal primary authority.
- Missing `release_current_v1/current.json` MUST fail closed and MUST NOT trigger fallback authority from this artifact.
- Direct reads are allowed only when explicitly scoped as:
  compatibility validation/comparison, reducer-input consumption, or proof/diagnostic evidence.
