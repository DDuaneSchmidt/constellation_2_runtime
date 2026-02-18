---
id: C2_MODEL_GOVERNANCE_BUNDLE_V1
title: "Model Governance Bundle v1 (Active Models + Params + Effective Mapping)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - model-governance
  - parameters
  - approvals
  - audit-grade
---

# Model Governance Bundle v1

## 1. Objective

Provide a single governed, immutable artifact per day that answers:

- “What models/engines were active?”
- “What parameter sets were in effect?”
- “What governed registries were used?”

## 2. Path

- `constellation_2/runtime/truth/pillars_v1/<DAY>/bundles/model_governance_bundle.v1.json`

## 3. Required fields

- `day_utc`
- `produced_utc` (deterministic day-scoped)
- `producer` (repo/module/git_sha)
- `status` in {`OK`, `DEGRADED`, `FAIL`}
- `reason_codes`
- `input_manifest`

## 4. Required governance references

Must include, at minimum:
- engine model registry reference (path + sha256)
- engine runner references for active engines (path + sha256)
- any allocation/risk registry references used for the day (if applicable)

## 5. Effective mapping requirement

Must include a stable mapping:
- engine_id -> runner_sha256_actual
- engine_id -> activation_status
- engine_id -> notes/reason_codes if blocked

## 6. Non-negotiable

This bundle must be immutable and day-keyed.
It must never include template day keys.
