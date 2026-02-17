---
id: C2_ECONOMIC_TRUTH_AVAILABILITY_CERTIFICATE_V1
title: "Constellation 2.0 — Economic Truth Availability Certificate v1 (Bundle Gate File)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - certificate
  - gate_file
  - audit_grade
  - fail_closed
  - deterministic
---

# Economic Truth Availability Certificate v1 — Contract

## 1. Objective
Emit a minimal day-scoped certificate stating bundle completeness.

This file is the single “gate file” other systems can depend on.

## 2. Canonical output path
- `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/certificates/<DAY>/economic_truth_availability_certificate.v1.json`

## 3. Required checks (must be explicit)
The certificate MUST state:

- required inputs present (accounting nav for day)
- required outputs produced (snapshot, ledger, window pack)
- schemas validated (all outputs validated against governed schemas)
- immutability honored (no overwrite occurred; if existed identical bytes, recorded as such)

## 4. Required fields
Top-level required:
- `schema_id` = `"C2_ECONOMIC_TRUTH_AVAILABILITY_CERTIFICATE_V1"`
- `schema_version` = `1`
- `day_utc`
- `ready` (boolean)
- `checks` array of objects:
  - `check_id` (string)
  - `pass` (boolean)
  - `details` (string)
  - `evidence_paths` (array of strings)

- `missing_artifacts` (array of paths; empty if ready=true)
- `input_manifest` (sha256 entries for all input files used to decide readiness)
- `produced_utc`, `producer`, `canonical_json_hash`

## 5. Fail-closed semantics
- If `ready=false`, the certificate writer MUST exit non-zero.
- The certificate MUST still be written (immutable) even when not ready.

## 6. Schema requirement
The certificate MUST validate against:
- `governance/04_DATA/SCHEMAS/C2/MONITORING/economic_truth_availability_certificate.v1.schema.json`
