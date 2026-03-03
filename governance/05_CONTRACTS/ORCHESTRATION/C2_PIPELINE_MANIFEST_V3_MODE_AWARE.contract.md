---
id: C2_PIPELINE_MANIFEST_V3_MODE_AWARE
title: "Constellation 2.0 — Pipeline Manifest V3 (Mode-Aware, Attempt-Derived, Non-Bricking)"
status: DRAFT
version: 1
created_utc: 2026-03-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - pipeline_manifest
  - mode_aware
  - attempt_scoped
  - pointer_index
  - determinism
  - non_bricking
---

# Pipeline Manifest V3 (Mode-Aware, Attempt-Derived)

## Objective
Provide a daily pipeline manifest that is:
- Mode-aware (PAPER vs LIVE)
- Activity-aware (no-intent/no-trade days do not brick)
- Derived from actual attempt execution (no static “wish list”)
- Attempt-scoped + pointer-promoted (no overwrites)

## Inputs (authoritative)
Pipeline manifest v3 MUST be derived from:
- `orchestrator_attempt_manifest.v2.json` produced by Orchestrator V2

It MUST NOT require presence of legacy v1/v2 pipeline artifacts.

## Output path (attempt-scoped)
Writer MUST emit:

`constellation_2/runtime/truth/reports/pipeline_manifest_v3/<day_utc>/attempts/<attempt_id>/pipeline_manifest.v3.json`

## Canonical pointer index (append-only)
Writer MUST append to:

`constellation_2/runtime/truth/reports/pipeline_manifest_v3/<day_utc>/canonical_pointer_index.v1.jsonl`

Pointer entries MUST be mode-partitioned (mode field required). Canonical heads are derived by consumers:
- Display head: highest pointer_seq per mode
- Authority head: highest pointer_seq per mode where authoritative=true and status=PASS

No directory scans are allowed.

## Stage classification (source of truth)
Manifest stage rows MUST be constructed from the attempt manifest stage list:
- effective_required
- effective_blocking
- executed / skipped
- status (OK|SKIP|FAIL)

If a stage is not executed because it is optional or activity-gated, it MUST NOT be treated as missing.

## Status semantics
Pipeline manifest v3 status is:
- PASS: no failures among stages with `effective_required=true`
- DEGRADED: no required failures, but one or more non-required failures OR activity=false (no activity day)
- FAIL: one or more required failures (effective_required=true) but no safety breach classification
- ABORTED: one or more blocking failures (effective_blocking=true and FAIL)

The writer MUST complete and write the manifest even for FAIL.

The writer MUST exit non-zero only on fatal corruption (missing/invalid attempt manifest, schema validation failure, write failure).

## Non-bricking guarantee
V3 MUST NOT:
- Mark “no activity day” as blocking failure
- Require legacy paths (intents dir, oms dir, allocation dir) unless the attempt manifest indicates the stage was required and executed

## Determinism
- `produced_utc` in the v3 manifest MUST be day-scoped: `<DAY>T00:00:00Z`
- Hashes in the manifest MUST be computed deterministically from referenced files (sha256 of attempt manifest; sha256 of stage outputs if present)
