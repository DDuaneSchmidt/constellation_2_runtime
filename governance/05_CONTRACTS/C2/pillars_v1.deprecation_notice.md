---
id: C2_PILLARS_V1_DEPRECATION_NOTICE
title: "Deprecation Notice — Pillars v1 (Superseded by Pillars v1r1)"
status: ACTIVE
version: 1
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - deprecation
  - pillars
  - submission-evidence
  - audit-proof
  - fail-closed
---

# Deprecation Notice — Pillars v1

## 1. Summary

`pillars_v1` is deprecated and superseded by `pillars_v1r1`.

This notice preserves history: existing immutable pillars_v1 artifacts remain facts, but all operational and audit surfaces MUST prefer pillars_v1r1.

## 2. Deprecated surface

- `constellation_2/runtime/truth/pillars_v1/<DAY>/...`

## 3. Canonical replacement

- `constellation_2/runtime/truth/pillars_v1r1/<DAY>/...`

## 4. Policy

### 4.1 Read policy
Consumers MUST prefer `pillars_v1r1`. `pillars_v1` is fallback-only for historical days where `pillars_v1r1` is absent.

### 4.2 Write policy
Operational writers MUST write only to `pillars_v1r1` and MUST NOT write new artifacts to `pillars_v1`.

## 5. Auditor note
This deprecation reduces ambiguity by establishing a single canonical pillars surface.

End of notice.
