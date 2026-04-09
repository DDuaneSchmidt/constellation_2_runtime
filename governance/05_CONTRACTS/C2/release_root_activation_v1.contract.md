# C2 Release Root Activation V1

## Authority

- `/home/node/constellation` is the authoritative development and governance root.
- `/home/node/constellation_2_runtime` is a legacy deployed/runtime copy and is not canonical design authority.

PROOF: `governance/02_REGISTRIES/REPO_AUTHORITY_V1.json`

## Release Assembly

- Release assembly MUST create immutable release roots under `/home/node/constellation_releases/<release_id>`.
- Release assembly MUST fail closed when the authoritative worktree is dirty.
- Each release root MUST contain a `release_manifest.v1.json`.
- The release manifest MUST record:
  - `release_id`
  - `git_sha`
  - `source_root`
  - `release_root`
  - `included_files`
  - `included_file_hashes`
  - `generated_at_utc`

## Activation

- Activation MUST update `/home/node/constellation_active` to the selected release root atomically or fail closed.
- Activation MUST ensure `/home/node/constellation_runtime_data` exists.
- Activation MUST NOT write runtime data into a release root.
- Activation MUST write the active runtime contract for the selected release or fail closed.
- Activation MUST perform post-activation verification before it is considered valid.
- If post-activation verification fails, activation MUST revert the active pointer or fail closed.
- Activation MUST write an `activation_receipt.v1.json`.
- The activation receipt MUST record:
  - `release_id`
  - `git_sha`
  - `prior_release_id`
  - `active_pointer_path`
  - `runtime_data_root`
  - `services_reloaded`
  - `parity_verified`
  - `generated_at_utc`
  - `status`

## Execution Model

- Validated execution paths in this architecture MUST resolve code through `/home/node/constellation_active`.
- Runtime artifacts for validated execution paths MUST resolve through `/home/node/constellation_runtime_data`.
- Legacy `/home/node/constellation_2_runtime` remains parallel compatibility infrastructure until separately retired by proof.
- Touched validated paper-day services and launchers must not resolve directly through
  `/home/node/constellation_2_runtime`.
