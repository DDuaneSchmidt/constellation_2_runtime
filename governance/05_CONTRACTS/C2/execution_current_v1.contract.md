# execution_current_v1

`execution_current_v1` is the planned reducer-owned canonical current for active execution truth after explicit cutover.

Status in this pass:
- `UNIMPLEMENTED`
- cutover boundary required before activation

Planned reducer owner:
- `truth_kernel_execution_current_reducer_v1`

Purpose:
- converge active execution posture, deployment/runtime identity, ledger/execution state, and execution-level blockers into one reducer-owned current

Allowed inputs after implementation:
- semantic surface `release.active_runtime_contract`
- semantic surface `platform.deployment_state_machine`
- semantic surface `execution.paper_session_ledger`
- semantic surface `execution.startup_proof_validation`
- semantic surface `execution.trading_day_state_machine`
- semantic surface `execution.execution_reconciliation`
- additional governed execution surfaces only if explicitly admitted by governance

Schema:
- reducer-owned current with:
  - reducer identity
  - activation state
  - runtime/deployment identity tuple
  - execution state refs
  - blocker state
  - replay input lineage

Fail-closed rules:
- missing deployment/runtime identity, missing required execution refs, or contradictory execution inputs MUST fail closed

Write prohibition:
- no non-reducer writer may publish `execution_current_v1/current.json`

Dual-truth prohibition:
- existing execution reports and projections remain the live execution-facing surfaces in this phase
- `execution_current_v1` MUST NOT be activated until an explicit governed cutover boundary retires or bridges the overlapping live execution surfaces
