# control_plane_validator_architecture_v1

This contract governs the Bundle 4 v2 validation spine for the live control plane.

Validator families:
- shared kernel: `control_plane_validation_kernel_v1`
- boundary validator: `control_plane_boundary_validator_v1`
- family validators:
  - `day_activation_family_validator_v1`
  - `global_context_family_validator_v1`
  - `session_authority_family_validator_v1`
  - `execution_build_family_validator_v1`
- startup-chain validator/certifier: `startup_chain_certification_v1`

Validator input law:
- validators may consume only canonical control truth, canonical execution truth, explicit bounded test roots, and the explicit candidate path for execution-build validation
- validators MUST reject forbidden repo-local or legacy roots
- validators MUST NOT mutate truth
- validators MUST NOT repair missing artifacts
- validators MUST NOT invoke the writer they validate

Deterministic output law:
- JSON output MUST be deterministic
- required top-level fields:
  - `validator_id`
  - `validator_version`
  - `ok`
  - `errors`
  - `warnings`
  - `invariants_checked`
  - `validated_artifacts`
  - `validated_current_surfaces`
  - `forbidden_path_hits`
- exit `0` only when `ok == true`
- exit non-zero when `ok == false`

Failure taxonomy:
- scope resolution failure
- forbidden-root usage
- derived-surface-as-authority usage
- missing artifact
- schema invalid
- lineage/reference mismatch
- status/invariant failure
- stage-gating failure

Evidence law:
- validators MUST emit exact artifact paths and hashes for validated artifacts when readable
- current-state surfaces MUST be reported separately from immutable artifacts
- forbidden paths MUST be surfaced explicitly in `forbidden_path_hits`

Versioning:
- backward-incompatible validator output changes require a new validator version
- validator output MAY add new invariant identifiers without renaming the validator
