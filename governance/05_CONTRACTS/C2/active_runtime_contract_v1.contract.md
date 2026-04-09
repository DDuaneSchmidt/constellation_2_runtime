# C2 Active Runtime Contract V1

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

## Required Fields

- `schema_id`
- `schema_version`
- `release_id`
- `git_sha`
- `release_root`
- `runtime_data_root`
- `canonical_truth_root`
- `truth_sleeves_root`
- `pointer_index_family`
- `allowed_truth_roots`
- `provenance_mode`
- `generated_at_utc`
- `status`

## Runtime Model

- `release_root` MUST equal the resolved target of `/home/node/constellation_active`.
- `runtime_data_root` MUST equal `/home/node/constellation_runtime_data`.
- `canonical_truth_root` MUST equal `/home/node/constellation_runtime_data/truth`.
- `truth_sleeves_root` MUST equal `/home/node/constellation_runtime_data/truth_sleeves`.
- `pointer_index_family` MUST select exactly one canonical pointer family for the active model.
- The current canonical pointer family is `run_pointer_v1`.
- `provenance_mode` MUST be `release_manifest`.

## Consumption

- Post-admission runtime components MUST resolve runtime roots from the active runtime contract instead of deriving them from `REPO_ROOT/constellation_2/runtime`.
- Post-admission pointer producers and consumers MUST use the same `pointer_index_family` declared by the active runtime contract.
- Release provenance for release-root execution MUST come from the active release manifest, not from a live `.git` worktree.
