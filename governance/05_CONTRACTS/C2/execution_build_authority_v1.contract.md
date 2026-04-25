---
id: C2_EXECUTION_BUILD_AUTHORITY_V1
title: "C2 Execution Build Authority Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_pre_submit_executable_closure
---

# Purpose

This contract defines the canonical pre-submit closure owner for executable PAPER entry.

A raw Phase C candidate is only a governed candidate. It is not executable authority.

Execution Build Authority owns exactly four things:

- resolve the governed dependency manifest for one operation type
- evaluate the complete dependency graph for one specific candidate
- invoke already-owned producers in proven canonical stage order where allowed
- emit `execution_build.v1.json` and, only when closure is complete, `execution_package.v1.json`

Execution Build Authority does not own business truth. It compiles, materializes through existing owners, and seals readiness.

# Inputs

- `operation_type`
- `candidate_path`
- `day_utc`
- canonical truth root
- sleeve execution truth root

# Outputs

Canonical build artifact:

- `truth/reports/execution_build_v1/<DAY>/<submission_id>/execution_build.v1.json`

Canonical sealed package artifact:

- `truth_sleeves/<sleeve_id>/<mode>/execution_package_v1/<DAY>/<submission_id>/execution_package.v1.json`

# Role boundaries

- truth owners own truth artifacts
- Execution Build Authority evaluates and materializes owned prerequisites only
- Submit Boundary validates package integrity plus fast-moving invariants and sends only
- operator surfaces summarize only
- orchestrators coordinate only

# Required behavior

Execution Build Authority must classify every dependency node as one of:

- `PRESENT`
- `MISSING`
- `STALE`
- `FAILED`
- `BLOCKED_BY_UPSTREAM`
- `UNOWNED`

It must emit:

- `closure_status`
- `first_real_blocker`
- `blocking_chain[]`
- `materializable_now[]`
- `unowned_dependencies[]`
- `dependency_results[]`

# Seal rule

A sealed execution package may exist only when all manifest `seal_requires` dependencies are `PRESENT`.

If closure is not complete, package emission is forbidden.

# Legacy path rule

Raw-candidate submit is compatibility only. The normative live path is:

- raw candidate -> Execution Build Authority -> sealed execution package -> Submit Boundary
