---
id: C2_SLEEVE_EDGE_ALLOCATOR_INTEGRATION_V1
title: "C2 Sleeve Edge Allocator Integration Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-15
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_control_stack
---

# C2 Sleeve Edge Allocator Integration Contract v1

## Purpose

Define the narrow allocator boundary for sleeve qualification consumption.

## Canonical consumer

- `ops/tools/run_capital_authority_allocation_day_v1.py`

## Allowed allocator inputs

- `qualification_state`
- `edge_band`
- `execution_health_band`
- `sample_sufficiency_band`
- `drift_band`
- `reason_codes`

## Prohibited allocator inputs

- raw trade sets
- factual metric internals
- readiness score
- composite merit score
- confidence multiplier
- regime score

## Required behavior

- allocator action must map from governed qualification outputs through explicit policy
- allocator must fail closed when qualification evidence is missing or invalid
- allocator must not recompute sleeve metrics internally
- allocator must consume already-published snapshots only
- allocator must not publish or restate sleeve measurement snapshots in the normal control path
- allocator must lock consumed snapshots to the governed `policy_version` and fail closed on mismatch
- allocator must also lock consumed snapshots to the governed compatible `calculation_version` set and fail closed on mismatch
- allocator must validate the sleeve-edge policy registry structure before using it in the control path
- allocator must validate snapshot integrity before control-path use
- allocator must fail closed on malformed snapshot payloads, missing bound fact ledgers, sha mismatches, or fact-input-hash mismatches
- allocator must require explicit canonical sequence provenance from `ops/tools/run_c2_paper_day_orchestrator_v2.py` before consuming sleeve-edge control truth
- missing or non-canonical sequence provenance must fail closed

## Versioning rule

Any change to the allocator-readable sleeve qualification surface requires an explicit contract version bump.
