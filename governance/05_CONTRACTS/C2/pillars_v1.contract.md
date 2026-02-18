---
id: C2_PILLARS_V1
title: "C2 Pillars v1 (Ledger + Decisions + Daily State + Root Anchor)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - pillars
  - truth
  - audit-grade
  - deterministic
  - fail-closed
  - replay
---

# C2 Pillars v1

## 1. Objective

Introduce an institutional-grade canonical truth infrastructure that:
- reduces truth sprawl,
- prevents split-truth ambiguity,
- strengthens determinism and audit closure,
- supports replay and hostile review,

while remaining compatible with existing C2 truth spines.

This contract defines a new **canonical pillar subtree** under runtime truth, additive to existing spines.

## 2. Canonical pillar root

For each UTC day key `<DAY>`:

- `constellation_2/runtime/truth/pillars_v1/<DAY>/`

## 3. Required pillar artifacts (authoritative)

### 3.1 Inputs Frozen (determinism anchor)
Path:
- `.../pillars_v1/<DAY>/inputs_frozen.v1.json`

Purpose:
- Hash-anchors all inputs-of-record needed to reproduce the day’s downstream outputs.

Properties:
- Immutable write (fail if exists with different bytes).
- Must include `input_manifest` (type/path/sha256) for all referenced inputs.

### 3.2 Event Ledger (append-only, day-scoped)
Path:
- `.../pillars_v1/<DAY>/event_ledger.v1.jsonl`

Purpose:
- Append-only event stream for day-level events relevant to audit and replay.

Properties:
- Immutable once sealed by Root Anchor (see 3.5).
- Each line is one canonical JSON event record with a stable event id.

### 3.3 Submission Decision Records (atomic, per decision)
Path:
- `.../pillars_v1/<DAY>/decisions/<DECISION_ID>.submission_decision_record.v1.json`

Purpose:
- One atomic, self-contained decision record per submission attempt.
- Replaces multi-file interpretation of “why a submission happened.”

Properties:
- Immutable write (fail if exists with different bytes).
- Must include decision verdict + reason codes + hashes of key upstream inputs.

### 3.4 Daily Execution State (single canonical daily state)
Path:
- `.../pillars_v1/<DAY>/daily_execution_state.v1.json`

Purpose:
- Single authoritative daily record summarizing:
  - positions snapshot reference,
  - exposures reference,
  - NAV reference,
  - reconciliation summary,
  - gate outcomes summary,
  - counts (intents/submissions/acks/fills/rejects).

Properties:
- Immutable write (fail if exists with different bytes).
- Must include `input_manifest` referencing all upstream artifacts used.

### 3.5 Day Root Anchor (tamper-evident closure)
Path:
- `.../pillars_v1/<DAY>/day_root_anchor.v1.json`

Purpose:
- “Close the books” for the day by hashing:
  - event ledger file sha256,
  - set of decision record sha256s,
  - daily execution state sha256,
  - inputs frozen sha256.

Properties:
- Immutable write.
- Once written, the day is considered sealed.

## 4. Bundles (attached, not new pillars)

Bundles are written under:

- `.../pillars_v1/<DAY>/bundles/`

They are governed, immutable artifacts that strengthen the pillars without creating new independent truth domains:
- `determinism_bundle.v1.json`
- `model_governance_bundle.v1.json`
- `execution_quality_bundle.v1.json`

Bundle contracts define exact contents and invariants.

## 5. Compatibility requirements

- Existing spines remain valid.
- Pillars v1 may be produced by compiling existing truth spines (bootstrap mode) or by native writers.
- No existing path is removed by this contract.

## 6. Non-negotiable rules (institutional)

- No template day keys (e.g., `YYYY-MM-DD`) may exist under runtime truth.
- Future-day operational truth writes are prohibited per C2_TEST_DAY_QUARANTINE_POLICY_V1.
- `latest.json` pointers are non-authoritative convenience only; pillars must be day-keyed.
