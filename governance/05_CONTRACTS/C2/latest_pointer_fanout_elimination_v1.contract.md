---
id: C2_LATEST_POINTER_FANOUT_ELIMINATION_V1
title: "C2 Latest Pointer Fan-Out Elimination Contract v1 (Single Run Pointer)"
status: DRAFT
version: V1
created_utc: 2026-02-20
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - determinism
  - audit_grade
  - fail_closed
  - pointer_policy
  - latest_json
  - surface_area_minimization
  - single_source
---

# C2 Latest Pointer Fan-Out Elimination Contract v1

## 1. Objective

Eliminate mutable pointer fan-out by enforcing **exactly one** `latest.json` pointer under the authoritative truth root.

This reduces:
- split-brain risk (multiple “latest” answers),
- crash-mid-write ambiguity (partial pointer updates),
- audit surface area.

Default stance: **delete-first, fail-closed**.

## 2. Authority

- Repo root: `/home/node/constellation_2_runtime`
- Authoritative truth root: `constellation_2/runtime/truth/**`
- Governance authority: `governance/**`

## 3. Definitions

- **Pointer file:** `latest.json` (or any future “latest” alias).
- **Run pointer:** the only allowed mutable pointer under truth root. It points to the most recent *day-keyed* final verdict artifact.

## 4. Non-negotiable invariants

### 4.1 Single run pointer (hard)
Exactly one `latest.json` may exist under:

`constellation_2/runtime/truth/**`

The only allowed path is:

`constellation_2/runtime/truth/latest.json`

Any other `latest.json` anywhere under truth root is forbidden.

### 4.2 Run pointer semantics
The run pointer MUST reference **only** the final verdict artifact:

`constellation_2/runtime/truth/reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json`

It MUST NOT point to:
- accounting,
- positions,
- monitoring,
- reconciliation,
- any non-final “intermediate” surface.

### 4.3 Readiness and gates are day-keyed (unchanged)
No readiness decision may consume `latest.json`. Readiness MUST be computed from explicit `DAY` paths only.

### 4.4 Fail-closed enforcement
If the run pointer is missing or invalid:
- Consumers MUST treat state as FAIL/CLOSED for any operation that requires final verdict.

## 5. Implementation requirements

### 5.1 Writers
All writers under truth root MUST be day-keyed only.
They MUST NOT write `latest.json` anywhere under truth root, except the run pointer writer (if implemented).

### 5.2 Preflight enforcement
A governed preflight MUST fail if:
- any forbidden `latest.json` exists under truth root,
- the run pointer is missing (if required by policy),
- or the run pointer does not reference `gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json`.

End of contract.
