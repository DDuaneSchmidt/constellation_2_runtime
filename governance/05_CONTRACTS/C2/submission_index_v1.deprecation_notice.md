---
id: C2_SUBMISSION_INDEX_V1_DEPRECATION_NOTICE
title: "Deprecation Notice — Submission Index v1 (Legacy Submission Evidence Surface)"
status: ACTIVE
version: 1
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - deprecation
  - submission-evidence
  - execution-evidence
  - audit-proof
  - pillars
  - fail-closed
---

# Deprecation Notice — Submission Index v1

## 1. Summary

`submission_index.v1.json` is a **legacy** submission evidence surface.

It is deprecated in favor of **Pillars decision records**, which collapse multi-file execution evidence into atomic, audit-friendly decision artifacts.

This notice does **not** delete history. Existing immutable submission index artifacts remain valid historical facts.

## 2. Legacy surface (deprecated)

The deprecated artifact is:

- `constellation_2/runtime/truth/execution_evidence_v1/submission_index/<DAY>/submission_index.v1.json`

The associated legacy writer remains available only for backward compatibility.

## 3. Replacement surface (canonical)

The canonical submission evidence surface is:

- `constellation_2/runtime/truth/pillars_v1r1/<DAY>/decisions/*.submission_decision_record.v1.json`

If `pillars_v1r1` is not present for a day, `pillars_v1/<DAY>/decisions/...` is the fallback.

## 4. Policy

### 4.1 Read policy (preferred order)

Consumers MUST prefer submission evidence in this order:

1) Pillars decisions (`pillars_v1r1`, then `pillars_v1`)
2) Legacy submission index (`submission_index.v1.json`) only if pillars evidence is absent

### 4.2 Write policy (off by default)

Operational automation MUST NOT generate submission_index by default.

If legacy submission index output is required for a backward-compatibility consumer, it must be explicitly enabled via an operator-controlled switch.

(Example: supervisor flag `--write_submission_index YES`.)

### 4.3 Readiness policy

Readiness MUST NOT require submission_index when pillars decisions exist.

Canonical readiness surfaces are:
- `pipeline_manifest_v2`
- `operator_gate_verdict_v2`

## 5. Auditor note

This deprecation reduces audit surface and cognitive overhead by making the atomic decision record the unit of evidence.

This change is designed to preserve:
- immutability
- provenance
- fail-closed behavior
- replayability

End of notice.
