---
doc_kind: contract
doc_id: C2_ATTEMPT_SCOPED_WRITE_POLICY_WITH_CANONICAL_POINTERS_V3
title: "Constellation 2.0 — Attempt-Scoped Write Policy + Deterministic Canonical Pointers (Authority Stabilization + Full Attempt Spine)"
version: 3
status: draft
repo_root_authoritative: /home/node/constellation_2_runtime
scope:
  - "Attempt-scoped immutable artifacts"
  - "Deterministic canonical pointer index (append-only)"
  - "Dual canonical heads (display vs authority)"
  - "Strict PAPER vs LIVE semantics"
  - "Authority stabilization via gate_stack_verdict_v1"
non_goals:
  - "No legacy overwrites"
  - "No mutable latest.json"
  - "No lexicographic head derivation"
---

# REQUIRED MODE (AI)

MODE: Institutional Audit Surface + Deterministic Systems Engineering + Adversarial Pointer Integrity Review

STANDARD:
- Missing proof = automatic FAIL
- No filesystem ordering authority
- No wall-clock timestamps in canonical artifacts
- Immutable does NOT equal authoritative

# HARD CONSTRAINTS

1) NO placeholders in runnable files.
2) NO heredocs.
3) NO base64.
4) All repo paths must be PROVEN via ls/test/rg.
5) All writes must be atomic (write temp → fsync file → fsync dir → rename).
6) No consumer may derive canonical by directory scanning.
7) Immutable does NOT equal authoritative.
8) Repo root is fixed: /home/node/constellation_2_runtime

# AUTHORITY SURFACE (MANDATORY)

## A) Final authority artifact

The single final decision authority surface is:

- `constellation_2/runtime/truth/reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json`

Any system making trading / risk / kill-switch decisions MUST derive authority from this artifact (directly or via the authority pointer below).

## B) Orchestrator MUST produce gate_stack_verdict_v1 every run

For every orchestrator run (PAPER or LIVE), the orchestrator MUST run the producer for `gate_stack_verdict_v1` for the run day.
If the verdict artifact is missing, authority MUST NOT advance.

STOP GATE:
If the orchestrator cannot produce `gate_stack_verdict_v1` deterministically for the run day → FAIL.

# RUN POINTER V2 (REPLACES latest.json)

## Files

- `constellation_2/runtime/truth/run_pointer_v2/authority_head_pointer.v1.json`
- `constellation_2/runtime/truth/run_pointer_v2/display_head_pointer.v1.json`
- `constellation_2/runtime/truth/run_pointer_v2/attempt_registry.v1.jsonl`

## Semantics

### 1) authority_head_pointer (AUTHORITY)

- points_to MUST reference `reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json`
- produced_utc MUST equal `<DAY>T00:00:00Z`
- May be atomically overwritten (pointer is coordination metadata, not immutable truth)
- MUST include attempt_id
- MUST only advance when:
  - the gate_stack_verdict file exists, AND
  - in LIVE: verdict status == PASS
  - in PAPER: verdict status in (PASS, OK_WITH_SOFT_FAILS) for display, but authority advances only on PASS

### 2) display_head_pointer (DISPLAY)

- UI-only pointer
- May advance regardless of PASS/FAIL
- Trading/risk code MUST NOT read display head

### 3) attempt_registry (ATTEMPTS)

Append-only JSONL. Each entry includes:
- attempt_seq (monotonic integer, allocated atomically)
- attempt_id (derived)
- git_sha
- orchestrator_config_hash
- policy_hashes
- mode (PAPER/LIVE)
- created_utc (observational only)

STOP GATE:
If attempt_seq cannot be atomically monotonic → FAIL.

# ATTEMPT ARTIFACT MODEL (REQUIRED FOR ALL STAGES)

For each artifact family:

## Attempt artifacts (immutable)

`.../<artifact_family>/<DAY>/attempts/<ATTEMPT_ID>/<artifact>.json`

Rules:
- Immutable
- Never overwritten
- Always atomic publish
- Schema validated at write-time

## Canonical pointer index (append-only, no scanning)

`.../<artifact_family>/<DAY>/canonical_pointer_index.v1.jsonl`

Each entry:
{
  "pointer_seq": <monotonic>,
  "attempt_id": "...",
  "status": "PASS|FAIL|OK_WITH_SOFT_FAILS",
  "authoritative": true|false,
  "policy_hash": "...",
  "produced_utc": "<DAY>T00:00:00Z"
}

Head derivation:
- display head: highest pointer_seq (any status)
- authority head: highest pointer_seq where (authoritative=true AND status==PASS)

STOP GATE:
If any consumer derives canonical via filesystem ordering → FAIL.

# PRODUCED_UTC POLICY

- Canonical artifacts: produced_utc = `<DAY>T00:00:00Z` (deterministic)
- Attempt artifacts: produced_utc = attempt created_utc (observational)

If any canonical artifact embeds wall-clock time → FAIL.

# LEGACY COMPATIBILITY (NO OVERWRITES)

Legacy canonical paths MUST NEVER be overwritten.
If a legacy file exists with differing bytes:
- write a de-authorization tombstone `legacy_authority_status.v1.json` adjacent to it
- consumers MUST NOT treat the legacy file as authoritative
- authority comes from pointer system

# REPLAY CERTIFICATION (ATTEMPT-SCOPED)

Replay certification MUST hash:
- attempt_registry entry (attempt_id)
- authority_head_pointer and display_head_pointer
- canonical_pointer_index.v1.jsonl for families touched
- attempt artifacts produced by this attempt

Replay MUST NOT require artifacts from subsystems not executed in this attempt.

# PAPER VS LIVE

## PAPER
- Soft failures do not brick the run.
- Display head may advance.
- Authority head advances only on PASS.

## LIVE
- All required stages must PASS.
- Authority head must advance.
- Any soft fail is treated as FAIL.

# ACCEPTANCE CRITERIA

A) Attempt allocation atomic + monotonic.
B) Canonical head derived from pointer index, not filesystem ordering.
C) Authority head and display head are separate.
D) Trading/risk reads authority head only.
E) produced_utc deterministic for canonical artifacts.
F) Replay hashes only actually-produced artifacts for the attempt.
G) No overwrite of legacy artifacts occurs.
H) All steps proven via ls/test/rg + sha256 evidence.
