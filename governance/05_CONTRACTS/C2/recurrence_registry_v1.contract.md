---
id: C2_RECURRENCE_REGISTRY_V1
title: "C2 Recurrence Registry Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_recurrence_registry
---

# C2 Recurrence Registry Contract (V1)

## Purpose

This contract defines the canonical lifecycle registry keyed by recurrence fingerprint.

## Truth owner

- Canonical writer:
  - `constellation_2/common/recurrence_registry_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/recurrence_registry_v1/recurrence_registry.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/recurrence_registry.v1.schema.json`

## Lifecycle statuses

- `OPEN`
  - the recurrence family is active, unresolved, or the proof attempt itself is invalid
- `CONTAINED`
  - the run is contained or mitigated, but elimination proof is not yet sufficient
- `ELIMINATED`
  - recurrence-safe proof passed for the family under the current live-path/day-attempt evidence model

## Update rules

- `RECURRENCE_SAFE` updates the family to `ELIMINATED`
- `MITIGATED_NOT_RECURRENCE_SAFE` updates the family to `CONTAINED`
- `INVALID_PROOF` updates the family to:
  - `CONTAINED` only for `SUCCESS_VERIFICATION`
  - otherwise `OPEN`
- Reappearance of a previously eliminated family must reopen or recontain it deterministically.

## Fail-closed rules

- Registry reads and writes must be schema validated and atomic.
- Malformed entries or fingerprint mismatches must fail closed.
- Fingerprint keys must match their stored normalized family record exactly.

