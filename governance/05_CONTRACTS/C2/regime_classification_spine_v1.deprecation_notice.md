---
id: C2_REGIME_CLASSIFICATION_SPINE_V1_DEPRECATION_NOTICE
title: "Deprecation Notice: Regime Classification Spine v1"
status: ACTIVE
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - regime
  - deprecation
  - audit-grade
  - immutability
  - governance
---

# Deprecation Notice — Regime Classification Spine v1

## 1. Statement of record

The runtime truth artifact family:

- `constellation_2/runtime/truth/monitoring_v1/regime_snapshot_v1/**`

is hereby designated **NON-AUTHORITATIVE** for regime classification decisions.

This notice does **not** modify or overwrite any immutable truth artifacts. It only defines authority.

## 2. Reason

A noncanonical prototype writer produced at least one v1 day artifact that does not satisfy the v1 design requirements for:

- complete governed-truth evidence,
- full contract rule set enforcement,
- broker manifest conditional logic,
- capital risk envelope logic.

Because immutable truth must not be overwritten, the correct remediation is to:

- preserve v1 artifacts as historical facts,
- designate v2 as authoritative for regime classification.

## 3. Replacement authority

The authoritative regime classification spine is:

- **Regime Classification Spine v2**
- Output root:
  - `constellation_2/runtime/truth/monitoring_v1/regime_snapshot_v2/**`

Pipeline manifest and operator gate MUST enforce v2 going forward.

## 4. Auditor guidance

When reconstructing regime classification for any day:

- Ignore v1 regime snapshots
- Use v2 regime snapshots, governed schemas, and governed contracts only

This maintains immutability and provides a clear audit trail.
