---
id: C2_RELEASE_BASELINE_READINESS_V1
title: "C2 Release Baseline Readiness Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_release_baseline
---

# C2 Release Baseline Readiness Contract v1

## Purpose

Define the minimum governed proof required before the touched Bundle 3-6 runtime path may be treated as baseline-ready for release or operator start.

## Baseline-ready definition

`baseline ready` means all of the following are true:

- the authoritative repo root is `/home/node/constellation`
- the authoritative worktree is `CLEAN`
- active-path contracts, tools, validators, and reporting/readiness surfaces are canonical-root compliant
- the required independent validator set passes
- the required trust/readiness surfaces are present and valid

If any proof is missing, the baseline is not ready.

## Required validator pass set

The release/readiness gate MUST require passing results for:

- `configuration_activation_family_validator_v1`
- `control_plane_boundary_v1`
- `day_activation_family_v1`
- `global_context_family_v1`
- `session_authority_family_v1`
- `execution_build_family_v1`
- `control_plane_trust_surface_validator_v1`
- `runtime_state_readiness_validator_v1`

## Canonical-root compliance law

Active-path surfaces MUST resolve only through:

- `/home/node/constellation_runtime_data/truth`
- `/home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/<mode>`

The following are forbidden on active paths:

- `constellation_2/runtime/truth`
- `constellation_2/runtime/truth_sleeves`
- `/home/node/constellation/constellation_2/runtime/truth`
- `/home/node/constellation/constellation_2/runtime/truth_sleeves`
- `/home/node/constellation_2_runtime/constellation_2/runtime/truth`
- `/home/node/constellation_2_runtime/constellation_2/runtime/truth_sleeves`

## Fail-closed semantics

The release/readiness gate MUST return `ok=false` when:

- the authoritative worktree is dirty
- a forbidden-root hit exists on an active path
- a required validator result is missing or failed
- a required trust/readiness surface is missing or invalid
- a governing reference required by this contract cannot be proven

## Required proof outputs

The release/readiness gate MUST emit deterministic machine-readable output including:

- `gate_id`
- `ok`
- `baseline_status`
- `canonical_root_status`
- `validator_statuses`
- `forbidden_root_hits`
- `blocking_errors`
- `governing_refs`
- `readiness_summary`
- `authority_label`

## Non-goals

- no advisory redesign
- no control-plane semantics rewrite
- no strategy or business-logic expansion
