---
id: C2_EXECUTION_ROOT_ISOLATION_POLICY_CONTRACT_V1
title: "C2 Execution Root Isolation Policy Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_execution_isolation
---

# C2 Execution Root Isolation Policy Contract V1

## Purpose

This contract governs one concern only:

- whether active governed PAPER execution may use global truth references for execution families
- and which owner decides that isolation boundary

## Canonical owner

The canonical owner of execution-root isolation policy is:

- `constellation_2/common/sleeve_execution_root_v1.py`

## Canonical policy

Active governed PAPER execution families must resolve under sleeve truth only:

- `/home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/<mode>/...`

For the active PRIMARY PAPER sleeve this means:

- `/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER/...`

The following active execution families are sleeve-scoped:

- `phaseC_preflight_v1`
- `trade_submit_readiness_c2_v1`
- `execution_evidence_v1`
- any resolved `phasec_out_dir`

## Forbidden policy

The following must hard-block before any broker transmit behavior:

- any active `phasec_out_dir` under canonical global truth
- any active execution-family reference under `/home/node/constellation_runtime_data/truth/...`
- any mismatch between the resolved sleeve execution root and the candidate path under evaluation

## Stable blocker codes

- `EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN`
- `EXECUTION_ROOT_PATH_MISMATCH`
- `EXECUTION_ROOT_PATH_UNRESOLVED`
- `EXECUTION_ROOT_SLEEVE_ID_MISSING`
- `EXECUTION_ROOT_MODE_MISSING`

## Required producer alignment

Active Phase C writers and readers for governed PAPER execution must:

- read day inputs from their governed input owners
- write Phase C execution artifacts under the canonical sleeve execution root
- discover same-day active attempts from the canonical sleeve execution root

## Proof basis

- `governance/05_CONTRACTS/C2/execution_root_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/truth_partitioning_by_sleeve_v1.contract.md`
- `governance/05_CONTRACTS/C2/startup_materialization_v1.contract.md`
- `governance/05_CONTRACTS/C2/trade_submit_readiness_c2_v1.contract.md`
- `constellation_2/common/sleeve_execution_root_v1.py`
- `constellation_2/common/paper_execution_authority_v1.py`
