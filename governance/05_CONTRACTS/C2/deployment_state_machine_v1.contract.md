---
id: C2_DEPLOYMENT_STATE_MACHINE_V1
title: "C2 Deployment State Machine Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth+systemd
scope: constellation_2_0_deployment_state_machine
---

# C2 Deployment State Machine Contract (V1)

## Purpose

This contract defines the single canonical deployment truth object for the validated paper-day
execution path.

`deployment_state_machine_v1` is the only authoritative answer to:

- whether the authoritative source is clean enough for immutable release build
- what release is active
- what live service path is actually executing
- whether the active release contains the required startup-stack files
- whether runtime-copy drift still exists in the touched validated path
- whether deployment is ready, blocked, or active

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_deployment_state_machine_v1.py`
- Canonical artifact path:
  - `/home/node/constellation_runtime_data/truth/reports/deployment_state_machine_v1/<DAY>/deployment_state_machine.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json`

## Required inputs

- authoritative repo git state under `/home/node/constellation`
- active release manifest under `/home/node/constellation_active`
- active runtime contract under `/home/node/constellation_runtime_data/runtime_contract_v1/`
- installed paper-day service fragment for `c2-paper-day-orchestrator.service`
- authoritative service source at `ops/systemd/user/c2-paper-day-orchestrator.service`

## Authority classification

- This surface is the sole top-level deployment truth owner for the touched validated execution
  path.
- Release manifests, activation receipts, and active runtime contracts remain supporting
  deployment artifacts.
- Runtime-copy source files under `/home/node/constellation_2_runtime` are diagnostic only for
  drift detection and are not deployment authority.

## Allowed final decisions

- `DEPLOY_READY`
- `DEPLOY_BLOCKED_VALID`
- `DEPLOY_BLOCKED_BY_DEFECT`
- `DEPLOY_ACTIVE`

## Fail-closed rules

- Release build must fail closed if the authoritative worktree is dirty.
- Activation must fail closed unless parity and post-activation verification both pass.
- `DEPLOY_ACTIVE` is allowed only when:
  - the active release contains the required startup-stack files
  - the active runtime contract matches the active release
  - the installed paper-day service resolves execution through `/home/node/constellation_active`
  - no touched validated execution path depends on `/home/node/constellation_2_runtime`

## Required startup stack

At minimum, the deployment state machine must verify the active release contains:

- `ops/tools/run_trading_day_intent_generation_v1.py`
- `ops/tools/run_startup_materialization_inputs_prep_v1.py`
- `ops/tools/run_phasec_risk_inputs_prep_v1.py`
- `ops/tools/run_trading_day_state_machine_v1.py`
- `governance/05_CONTRACTS/C2/trading_day_state_machine_v1.contract.md`
- `constellation_2/common/paper_session_ledger_v1.py`
- `governance/05_CONTRACTS/C2/paper_session_ledger_v1.contract.md`

## Execution model

- Validated paper-day execution for the touched service must resolve through
  `/home/node/constellation_active`.
- `/home/node/constellation_2_runtime` must not remain the touched validated execution root.
- Runtime-copy service and launcher files may be inspected for drift but are not deployment
  authority.
