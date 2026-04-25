---
id: C2_OPERATOR_RUNTIME_CONTROL_KERNEL_BOUNDARY_CONTRACT_V1
title: "Constellation 2.0 Operator/Runtime Control Kernel Boundary"
status: DRAFT
version: 1
created_utc: 2026-04-13
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_runtime_control_kernel
---

# Operator/Runtime Control Kernel Boundary

This kernel governs one concern only:

- whether governed PAPER trade-submit entry consequence is currently allowed or blocked

The minimal runtime-control chain is:

- kill-switch authority input
- trade-submit-readiness authority input
- `RuntimeControlDecision`
- `RuntimeControlRecord`
- `RuntimeControlRunEnvelope`

## Canonical authority surface

- `RuntimeControlRecord` is the sole operator/runtime-control authority artifact for this kernel version.
- `RuntimeControlDecision` is a pure gate only.
- `RuntimeControlRunEnvelope` is mandatory evidence for every evaluation run.

## In-scope evidence

- canonical kill-switch truth:
  `/home/node/constellation_runtime_data/truth/risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json`
- governed sleeve-scoped readiness truth:
  `/home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/<mode>/trade_submit_readiness_c2_v1/_history/<mode>/<ib_account>/<DAY>/status.json`

## Out of scope for this first kernel version

- session-authority status projections
- operator dashboards
- capability-state projections
- operator-gate-verdict surfaces
- scheduler/orchestration redesign
- activation/session-admission semantics that are already enforced elsewhere in the submit path

These surfaces may remain as upstream context or read-only projections, but they are not the runtime-control authority for submit consequence.

## Mandatory rules

- runtime consequence gates must consult the runtime-control kernel path, not raw kill-switch plus readiness checks inline
- contradictory or missing control evidence must fail closed
- repeated identical control evidence must not create contradictory second truth
- blocked and duplicate evaluations must still emit `RuntimeControlRunEnvelope`
- `operator_gate_verdict` surfaces remain non-authoritative and must not be revived as runtime-control authority
- no broker, submission, lifecycle, snapshot, or advisory redesign is allowed through this kernel

