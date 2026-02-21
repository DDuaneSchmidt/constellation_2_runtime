---
id: C2_SPINE_EXCLUSIVITY_CONTRACT_V1
title: "C2 Spine Exclusivity (Day-Scoped) Contract V1"
status: DRAFT
version: V1
created_utc: 2026-02-20
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - determinism
  - fail_closed
  - audit_grade
  - spine_exclusivity
  - versioning
---

# Spine Exclusivity Contract V1

## Objective

Eliminate split-brain caused by parallel versioned truth spines (e.g., `accounting_v1` + `accounting_v2`, `monitoring_v1` + `monitoring_v2`) by enforcing:

- a single **active version** per spine-family
- enforcement keyed by the **run day** (from `constellation_2/runtime/truth/latest.json`)
- FAIL-CLOSED if multiple versions are populated for the run day

This contract does **not** require deleting historical directories. It enforces day-scoped exclusivity for the authoritative run day.

## Authority

- Repo root: `/home/node/constellation_2_runtime`
- Truth root: `constellation_2/runtime/truth`
- Run pointer (single mutable pointer): `constellation_2/runtime/truth/latest.json`
- Registry of spine authority: `governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json`

## Invariants

### I1. Single run pointer
Only one `latest.json` may exist under truth root:
- Allowed: `constellation_2/runtime/truth/latest.json`
- Forbidden: any other `latest.json` anywhere under truth root

### I2. Day-scoped spine exclusivity
Let `DAY = latest.json.day_utc`. For each spine-family declared in `C2_SPINE_AUTHORITY_V1` with `exclusive=true` and `enforce_from_day_utc <= DAY`:

Exactly one version may have any day-keyed artifacts present for `DAY`.

If more than one version has day-keyed artifacts present for `DAY`, the system MUST fail closed.

### I3. Enforcement mechanism
`ops/governance/preflight.sh` MUST enforce I2 by invoking:
- `ops/governance/preflight_enforce_spine_exclusivity_v1.sh`

Any operator workflow claiming readiness MUST call `ops/governance/preflight.sh` first.

## Notes

This contract intentionally allows both v1 and v2 directories to exist historically. It prohibits ambiguous "which version is authoritative" for the run day.
