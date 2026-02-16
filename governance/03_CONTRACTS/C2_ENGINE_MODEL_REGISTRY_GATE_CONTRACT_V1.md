---
id: C2_ENGINE_MODEL_REGISTRY_GATE_CONTRACT_V1
title: "Engine Model Registry Gate Contract V1"
status: CANONICAL
version: 1
created_utc: 2026-02-16
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - engine-registry
  - code-lock
  - sha256
  - fail-closed
  - audit-grade
---

# Engine Model Registry Gate Contract V1

## Objective

Provide an audit-grade, deterministic, fail-closed authorization gate ensuring that only approved engine runner code executes.

---

## Canonical Enforcement (FAIL-CLOSED)

The gate MUST fail if:

1. Registry JSON fails governed schema validation.
2. Any ACTIVE engine:
   - Has activation_status != ACTIVE.
   - Has engine_runner_sha256 that does not match the sha256 of the runner file.
   - Has a missing runner file.

This is the authoritative **code-lock guarantee**.

---

## Git SHA Fields (Audit-Only)

The fields:

- approved_git_sha
- current_git_sha (HEAD)

are recorded for audit transparency only.

They MUST NOT cause gate failure.

Rationale:

When the registry itself is version-controlled in the same repository, requiring:

    approved_git_sha == HEAD

creates a logical fixed-point paradox, because any approval commit changes HEAD.

Code integrity is enforced by runner sha256, not HEAD equality.

---

## Required Output

Immutable report:

constellation_2/runtime/truth/reports/engine_model_registry_gate_v1/<DAY>/engine_model_registry_gate.v1.json


Report MUST include:

- input manifest with sha256 of:
  - registry file
  - each runner file
- per-engine expected vs actual sha256
- approved_git_sha
- current_git_sha
- reason_codes

