# live_control_plane_authority_inventory_v1

This contract is the canonical live authority inventory for the control-plane startup path.

Canonical truth roots:
- canonical control truth root: `/home/node/constellation_runtime_data/truth`
- canonical execution truth root: `/home/node/constellation_runtime_data/truth_sleeves/<SLEEVE>/<MODE>`

Authoritative immutable artifacts on the canonical startup chain:
- `day_activation_build_v1`
- `day_activation_package_v1`
- `global_context_build_v1`
- `global_context_package_v1`
- `target_day_build_v1`
- `target_day_admission_v1`
- `session_promotion_decision_v1`
- `execution_build_v1`
- `execution_package_v1`
- `control_stage_day_admitted_v1`
- `control_stage_context_admitted_v1`
- `control_stage_session_admitted_v1`
- `control_stage_execution_build_admitted_v1`
- `control_stage_day_certification_v1`
- `control_stage_context_certification_v1`
- `control_stage_session_certification_v1`
- `control_stage_execution_build_certification_v1`
- `startup_chain_certification_v1`

Authoritative current-state surfaces:
- `active_session_v1/current.json`
- `configuration_state_v1/current.json` (canonical only for configuration-activation family)
- `release_current_v1/current.json` (canonical runtime/release authority stream)

Compatibility/runtime legacy surfaces (non-primary):
- `release.active_runtime_contract`
- `release.release_manifest_active`

Derived non-authoritative surfaces:
- `session_authority_status_v1/current.json`
- `session_authority_alert_v1/current.json`
- `reports/startup_proof_validation_v1/<DAY>/startup_proof_validation.v1.json`

Forbidden legacy or repo-local live truth surfaces:
- `/home/node/constellation/constellation_2/runtime/truth`
- `/home/node/constellation/constellation_2/runtime/truth_sleeves`
- `/home/node/constellation_2_runtime`
- any path below those roots

Boundary rules:
- live control-plane validators and stage-admission writers MUST load only from the canonical truth roots above
- repo-local or legacy truth roots MUST fail closed immediately when presented as authority inputs or write targets
- derived surfaces MAY be read for operator display only
- derived surfaces MUST NOT be consumed as authority inputs for validation, admission, promotion, or certification
- current-state surfaces outside the canonical control truth root are forbidden

Release/runtime authority lock:
- `release_current_v1/current.json` is canonical runtime authority.
- Runtime/bootstrap/orchestrator/systemd paths MUST NOT use `release.active_runtime_contract` or `release.release_manifest_active` as primary truth.
- Missing/invalid `release_current_v1/current.json` MUST fail closed.

## Remaining Legacy Read Scope Table

| Location | Legacy Read Surface | Classification | Scope Justification |
| --- | --- | --- | --- |
| `constellation_2/common/release_current_v1.py` | `release.release_manifest_active`, `release.active_runtime_contract`, `policy.configuration_state_current` | `REDUCER_INPUT_REQUIRED` | Reducer input set required to materialize canonical `release_current_v1/current.json`. |
| `constellation_2/common/runtime_contract_v1.py` (`_load_active_runtime_contract_ref_or_fail`, `_resolve_release_provenance_from_old_surfaces`) | `release.active_runtime_contract`, `release.release_manifest_active`, `policy.configuration_state_current` | `COMPATIBILITY_ONLY_TEMPORARY` | Compatibility parity/shadow validation and provenance compatibility path; no missing-`release_current` fallback authority. |
| `constellation_2/common/runtime_authority_bridge_v1.py` | `resolve_release_provenance()` | `COMPATIBILITY_ONLY_TEMPORARY` | Legacy comparison path for mismatch logging versus release-current-derived authority. |
| `constellation_2/common/runtime_identity_bridge_v1.py` | `resolve_release_provenance()` | `COMPATIBILITY_ONLY_TEMPORARY` | Legacy comparison/stability field parity while bridge publishes canonical identity shape. |
| `constellation_2/common/runtime_identity_v1.py` | `resolve_release_provenance()` | `COMPATIBILITY_ONLY_TEMPORARY` | Legacy-compatible identity snapshot field population; authority roots/identity are bridge-backed. |
| `constellation_2/common/deployment_state_machine_v1.py` (`load_active_runtime_contract_if_present`) | direct `active_runtime_contract.v1.json` path | `PROOF_DIAGNOSTIC_ALLOWED` | Post-activation verification evidence; not primary runtime authority selection. |
| `ops/tools/run_deployment_state_machine_v1.py` | `active_runtime_contract` status/path fields in emitted report | `PROOF_DIAGNOSTIC_ALLOWED` | Deployment evidence/reporting surface, not authority input for runtime decisions. |
| `ops/tools/run_recurrence_kill_gate_v1.py` | `load_active_runtime_contract_or_fail()` + direct `release_manifest.v1.json` read from active root | `PROOF_DIAGNOSTIC_ALLOWED` | Identity/proof consistency checks in recurrence evidence payload. |
| `constellation_2/common/fresh_day_admission_v1.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | `release_id`/`git_sha` metadata stamping in output payload; admission decisions come from semantic inputs + authority snapshot. |
| `constellation_2/common/next_day_readiness_probe_v1.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | `release_id`/`git_sha` stamping in readiness artifact. |
| `constellation_2/common/capability_state_v1.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | metadata stamping with explicit fallback to source snapshot. |
| `ops/tools/run_paper_session_admission_v1.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | release metadata written into execution outcome context; not a control-decision authority input. |
| `ops/tools/run_global_kill_switch_v1.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | `git_sha` metadata in emitted gate artifact. |
| `ops/tools/run_gate_stack_verdict_v1.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | `git_sha` metadata stamping only. |
| `ops/tools/run_feed_attestation_gate_v1.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | `git_sha` metadata stamping only. |
| `ops/tools/run_heartbeat_gate_v1.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | `git_sha` metadata stamping only. |
| `ops/tools/run_liquidity_slippage_gate_v1.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | `git_sha` metadata stamping only. |
| `ops/tools/run_operator_daily_gate_v3.py` | `resolve_release_provenance()` | `STAMPING_ONLY_ALLOWED` | `git_sha` metadata stamping only. |
