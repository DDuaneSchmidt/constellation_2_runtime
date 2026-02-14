---
id: C2_E_EXECUTION_EVIDENCE_TRUTH_SPINE_V1
title: "Prerequisite E — Execution Evidence Truth Spine (PhaseD → runtime/truth)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
truth_root: constellation_2/runtime/truth
---

# 1. Purpose

Phase D produces audit-grade broker-boundary evidence, but it is written to:
- a caller-provided out_dir (ephemeral), and
- `constellation_2/phaseD/outputs/submissions/<submission_id>/` (not canonical runtime truth).

Bundle F (Accounting) and Bundle G (Allocation) require a **canonical, immutable truth surface** under:
`constellation_2/runtime/truth/`

This spine bridges Phase D evidence into canonical truth, preserving:
- immutability,
- determinism,
- schema validation,
- hostile audit replayability.

# 2. Scope and Non-Claims

## 2.1 In scope

- Discover Phase D submission directories and materialize a canonical truth mirror
- Immutable, day-scoped truth outputs under runtime truth root
- Schema validation against existing C2 schemas:
  - broker_submission_record.v2.schema.json
  - execution_event_record.v1.schema.json
  - veto_record.v1.schema.json
- Deterministic directory naming and ordering
- Pointer-only latest file
- Failure semantics (fail-closed vs degraded)
- Acceptance tests

## 2.2 Out of scope

- Broker correctness
- P&L computation
- Trade classification beyond what Phase D outputs provide
- Any mutation of Phase D original evidence

# 3. Inputs (Hash-Bound)

Primary input source (Phase D outputs):

- `constellation_2/phaseD/outputs/submissions/<submission_id>/`
  containing one of:
  - `broker_submission_record.v2.json` (always on non-veto paths)
  - optionally `execution_event_record.v1.json` (success path)
  - OR `veto_record.v1.json` (veto path)

This spine must treat Phase D outputs as immutable inputs and must record sha256 for every file consumed.

# 4. Outputs (Canonical Truth)

Truth root:
`constellation_2/runtime/truth/execution_evidence_v1/`

Day-scoped outputs:

- `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY_UTC>/<submission_id>/broker_submission_record.v2.json`
- `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY_UTC>/<submission_id>/execution_event_record.v1.json` (if present)
- `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY_UTC>/<submission_id>/veto_record.v1.json` (if present)

Pointer-only latest:
- `constellation_2/runtime/truth/execution_evidence_v1/latest.json`

Failure artifact:
- `constellation_2/runtime/truth/execution_evidence_v1/failures/<DAY_UTC>/failure.json`

Immutability rules:
- No overwrite; identical bytes may be skipped; mismatched bytes hard-fail.

# 5. Day Key Derivation (Deterministic)

Each Phase D submission directory must map to a `DAY_UTC` deterministically.

The source of truth for day key is:
- `observed_at_utc` in `veto_record` (veto path), OR
- the timestamp field in `broker_submission_record` or `execution_event_record` as defined by schema, OR
- if no timestamp exists, the spine fails closed.

The exact timestamp field names must be proven from schema before implementation.

# 6. Failure Semantics

Status codes:

- `OK`
- `DEGRADED_MISSING_EXECUTION_EVENT` (submission record present but no execution event)
- `FAIL_CORRUPT_INPUTS`
- `FAIL_SCHEMA_VIOLATION`
- `FAIL_ATTEMPTED_REWRITE`

Rules:
- Missing Phase D directory or unreadable JSON → FAIL_CORRUPT_INPUTS
- Schema validation failure → FAIL_SCHEMA_VIOLATION
- Attempted rewrite → FAIL_ATTEMPTED_REWRITE
- Missing execution event record on a submission is DEGRADED (allowed) but must be explicit.

In FAIL states:
- write failure artifact
- do not update latest pointer
- exit non-zero

# 7. Reconstruction Guarantee

Given:
- the canonical truth mirror outputs
- embedded input manifest (paths + sha256)
- producing git sha

An auditor can verify:
- Phase D source bytes match recorded hashes
- canonical truth bytes match recorded hashes
- replay produces identical output bytes

# 8. Acceptance Tests

1) Determinism: same Phase D inputs → identical canonical outputs
2) Immutability: attempted rewrite fails closed
3) Schema validation: invalid Phase D file fails closed
4) Day key derivation: stable mapping from timestamp fields
5) Latest pointer references only immutable outputs by sha256

# 9. Dependency Contract

Bundle F may consume:
- execution_event_record truth (fills/lifecycle evidence)
- broker_submission_record truth (submissions)

But only from the canonical runtime truth paths defined here, not from Phase D ephemeral out_dir.
