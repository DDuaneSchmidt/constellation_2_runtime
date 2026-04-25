---
id: C2_EXECUTION_ROOT_AUTHORITY_CONTRACT_V1
title: "C2 Execution Root Authority Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_execution
---

# C2 Execution Root Authority Contract V1

## Purpose

This contract governs one concern only:

- who owns live PAPER execution-root authority
- how the canonical execution root path is computed
- and what must hard-block execution when that authority cannot be proven

## Canonical owner

The canonical execution-root authority owner is:

- `sleeve_execution_root_v1`

## Canonical root basis

The canonical execution root path model is sleeve-partitioned truth only:

- `truth/sleeves/<sleeve_id>/<mode>/...`

For active governed PAPER execution, the resolved execution root path MUST be:

- `/home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/<mode>`

Required identity fields:

- `sleeve_id`
- `mode`
- `execution_root_path`

## Scope

Execution authority is sleeve-scoped. No active submission or execution path may use a shared or global execution root as the governing owner for:

- `phaseC_preflight_v1`
- `trade_submit_readiness_c2_v1`
- `execution_evidence_v1`
- any resolved `phasec_out_dir`
- any resolved submit/evidence family path bound to activity-producing execution

## Precedence

Precedence is explicit and fail-closed:

1. `sleeve_execution_root_v1` and `truth_partitioning_by_sleeve_v1` define the canonical execution root.
2. `truth/sleeves/<sleeve_id>/<mode>/...` overrides any older global/shared execution-root references.
3. Older global/shared execution-root references may remain only as legacy or historical documentation.
4. No runtime component may warn-and-continue, infer a global fallback root, or silently choose between sleeve and global execution-root candidates.

## Invalidations

The execution-root authority is invalid and MUST hard-block / fail closed on any of:

- missing `sleeve_id`
- missing `mode`
- unresolved sleeve execution root path
- any contradictory global/shared execution-root reference presented as active authority
- any mismatch between the computed canonical sleeve execution root and the runtime or submit path under evaluation

Stable blocker codes:

- `EXECUTION_ROOT_SLEEVE_ID_MISSING`
- `EXECUTION_ROOT_MODE_MISSING`
- `EXECUTION_ROOT_PATH_UNRESOLVED`
- `EXECUTION_ROOT_PATH_MISMATCH`
- `EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN`

## Required behavior

Submission and execution-runtime code MUST:

- deterministically compute the canonical execution root from governed sleeve identity plus mode
- reject any resolved path outside the canonical sleeve execution root family
- reject any global/shared execution-root reference for active execution authority
- block before any broker transmit behavior when execution-root authority is not proven

Global/shared execution-root ownership is explicitly non-canonical for active governed PAPER execution.

## Proof basis

- `governance/05_CONTRACTS/C2/truth_partitioning_by_sleeve_v1.contract.md`
- `governance/05_CONTRACTS/C2/multi_account_topology_v1.contract.md`
- `governance/05_CONTRACTS/C2/active_runtime_contract_v1.contract.md`
- `governance/03_CONTRACTS/C2_TRUE_EVIDENCE_SPINE_V2.md`
- `governance/05_CONTRACTS/C2/trade_submit_readiness_c2_v1.contract.md`
- `governance/05_CONTRACTS/C2/startup_materialization_v1.contract.md`

## Non-authoritative references

The following are non-authoritative for active execution-root ownership:

- any active global/shared execution root such as `/home/node/constellation_runtime_data/truth/...` used as the governing owner for execution families
- proof-only fixture roots under `/tmp/...`
- historical documentation that still records older global execution-root examples

Those references are legacy or diagnostic only unless a future governed contract explicitly replaces this contract.
