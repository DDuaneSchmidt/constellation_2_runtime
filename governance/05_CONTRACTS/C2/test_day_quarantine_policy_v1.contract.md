---
id: C2_TEST_DAY_QUARANTINE_POLICY_V1
title: "Test Day Quarantine Policy v1 (Prevent Future-Day Truth Contamination)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - quarantine
  - test-days
  - audit-grade
  - fail-closed
  - immutability
  - operational-safety
---

# Test Day Quarantine Policy v1

## 1. Objective

Prevent future-dated testing from contaminating operational readiness gating and immutable truth outputs.

This policy preserves:
- immutability,
- audit trail integrity,
- reproducibility,
- litigation survivability,

by **refusing** to create new future-day truth artifacts under operational writers.

## 2. Definitions

- `day_utc` is a UTC day key in format `YYYY-MM-DD`.
- `today_utc` is the current date in UTC as observed at runtime.

A day key is **future** if:
- `day_utc > today_utc` (lexicographic compare is valid for YYYY-MM-DD)

## 3. Policy

### 3.1 Prohibition: Future-day operational truth writes

Operational truth writers MUST refuse to run when `day_utc` is future.

This is a hard fail-closed rule:
- exit non-zero,
- produce no new truth output for that day key.

Rationale:
- Writing future-dated immutable artifacts causes irreversible collisions, breaks readiness logic,
  and creates audit ambiguity.

### 3.2 Existing future artifacts

Existing future-dated artifacts produced before this policy remain immutable historical facts and MUST NOT be deleted.

This policy does not modify history; it prevents recurrence.

### 3.3 Optional explicit quarantine registry

A governed registry MAY list day keys that are explicitly quarantined even if not future
(e.g., bad test days).

Registry (if used):
- `governance/02_REGISTRIES/TEST_DAY_KEY_QUARANTINE_V1.json`

## 4. Required enforcement points (v1 scope)

At minimum, the following operational writers MUST enforce this rule before writing:

- `ops/tools/run_pipeline_manifest_v1.py`
- `ops/tools/run_operator_gate_verdict_v1.py`
- `ops/tools/run_regime_snapshot_v2.py`

## 5. Auditor note

This policy is the formal remediation for:
- immutable overwrite collisions caused by test runs on calendar future days.

It is designed to survive hostile review.
