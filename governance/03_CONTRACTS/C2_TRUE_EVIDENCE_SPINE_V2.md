---
id: C2_TRUE_EVIDENCE_SPINE_V2
title: "Constellation 2.0 True Evidence Spine v2 (Canonical Broker-Truth, Audit-Proof)"
status: ACTIVE
version: 2
created_utc: 2026-02-16
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Constellation 2.0 — True Evidence Spine v2 (Canonical)

## 1. Objective (non-negotiable)

This contract defines the **single canonical broker-truth evidence spine** for Constellation 2.0 paper trading.

If an order is *attempted*, the system MUST be able to prove:

- exactly what was authorized upstream (intent → preflight → OMS → allocation),
- exactly what was requested at the broker boundary,
- exactly what the broker reported as the resulting lifecycle (accepted/rejected/filled/cancelled),
- and exactly what immutable truth artifacts were produced.

**No silent failure points. No parallel evidence surfaces treated as authoritative.**

The authoritative output surface is runtime truth:

- `constellation_2/runtime/truth/`

Any output outside runtime truth is **non-authoritative** and MUST NOT be used to assert broker activity in audits.

## 2. Canonical truth root and directories

### 2.1 Canonical root
- `constellation_2/runtime/truth/execution_evidence_v1/`

### 2.2 Required directory set
The following directories MUST exist (writers may create them as needed):

- `execution_evidence_v1/submissions/{DAY_UTC}/{SUBMISSION_ID}/`
- `execution_evidence_v1/manifests/{DAY_UTC}/`
- `execution_evidence_v1/failures/{DAY_UTC}/`

### 2.3 Day key
- `{DAY_UTC}` MUST be `YYYY-MM-DD` (UTC day key).
- A submission MUST be recorded under the UTC day key of its **submission attempt timestamp**.

## 3. Submission directory (atomic unit of evidence)

Each `{SUBMISSION_ID}` directory under a day represents exactly one submission attempt (including vetoed and rejected attempts when a broker call was attempted or evaluated).

### 3.1 Required files in each submission directory

Each submission directory MUST contain these files (exact filenames):

1) `order_plan.v1.json`  
   - The normalized order plan to be submitted (the canonical plan used at the broker boundary).

2) `binding_record.v1.json`  
   - The deterministic binding between upstream authorization and the submission attempt.

3) `broker_submission_record.v2.json`  
   - The broker-boundary record: what was requested and what the broker returned (IDs/status/errors).

4) `execution_event_record.v1.json`  
   - The broker lifecycle evidence record: events observed for this submission (submitted, accepted, rejected, filled, etc.).

5) `mapping_ledger_record.v1.json`  
   - The deterministic mapping from upstream identifiers to broker identifiers (and any required translation artifacts).

If any required file is missing, the evidence spine for that day is **INVALID**.

### 3.2 Required lineage guarantees (binding must be auditable)

The evidence spine MUST be able to reconstruct the full lineage:

- Intent artifact identity (hash + path)
- Preflight decision identity (hash + path)
- OMS decision identity (hash + path)
- Allocation identity (hash + path)
- Risk gate evaluations that resulted in PASS/VETO (if applicable)

This contract does not impose a specific JSON field layout inside each file; it imposes the audit requirement that the binding and submission records, taken together, must contain enough information to deterministically reconstruct lineage.

If lineage cannot be reconstructed for a submission, the spine is **NON-COMPLIANT**.

### 3.3 IB paper routing requirement (fail-closed)

For broker `IB` in paper mode:

- The broker submission record MUST prove the routed account is an IB paper account (DU*).
- If the routed account is missing or not DU*, the submission MUST be treated as INVALID and MUST FAIL-CLOSED.

## 4. Immutability and producer lock (audit-grade)

### 4.1 No overwrites (strict)
Evidence artifacts are immutable:

- If a file path already exists, a writer MUST NOT overwrite it.
- Any attempt to overwrite MUST hard-fail.

### 4.2 Atomic write requirement
Writers MUST write artifacts atomically (write temp → fsync → rename).

The artifact contents MUST be complete on disk or not present at all.

### 4.3 Producer lock
For a given `{DAY_UTC}` and `{SUBMISSION_ID}`, the producing `git_sha` is locked:

- If a writer detects an existing submission directory with evidence artifacts produced by a different `git_sha`, the writer MUST hard-fail.

## 5. Deterministic identification (submission id)

### 5.1 Submission id must be deterministic
`{SUBMISSION_ID}` MUST be derived deterministically from the immutable identity of the upstream authorization and normalized order plan, such that:

- a re-run of the same day with the same upstream authorization produces the same `{SUBMISSION_ID}`,
- and different authorizations produce different ids.

### 5.2 No "latest" ambiguity
A submission directory MUST be discoverable by deterministic enumeration of the day directory.

## 6. Manifests and failures (day-level completeness)

### 6.1 Day manifests
A day manifest MUST exist under:

- `execution_evidence_v1/manifests/{DAY_UTC}/`

The manifest MUST provide a deterministic summary of:

- the set of submission ids discovered under `submissions/{DAY_UTC}/`,
- counts by broker status (attempted/accepted/rejected/fill states),
- and completeness assertions (e.g., required files present per submission).

The manifest file naming convention is governed by the submission runner contract (C2_PHASED_SUBMISSION_RUNNER_V1).

### 6.2 Failure artifact
If evidence generation fails for a day, a failure artifact MUST be written under:

- `execution_evidence_v1/failures/{DAY_UTC}/failure.json`

The failure artifact MUST include:

- produced_utc
- git_sha
- reason_code
- human-readable summary
- the stage that failed (enumerated)
- and a deterministic pointer to any partial artifacts that exist

Failures MUST be FAIL-CLOSED.

## 7. latest.json pointer policy (non-authoritative unless validated)

The file:

- `execution_evidence_v1/latest.json`

MAY exist as a convenience pointer, but:

- It MUST be treated as **non-authoritative** in audits.
- Any consumer that uses `latest.json` MUST validate that it points to a real `{DAY_UTC}` directory and that the day manifest exists and is internally consistent.

If validation fails, consumers MUST FAIL-CLOSED.

## 8. Compliance checklist (hostile review)

A day is evidence-compliant only if:

- `submissions/{DAY_UTC}/` exists,
- each submission directory contains all required files (§3.1),
- lineage can be reconstructed (§3.2),
- IB paper routing is provable (§3.3),
- day manifest exists and enumerates submission ids (§6.1),
- and no overwrites occurred (§4.1).

Any violation renders the evidence spine NON-COMPLIANT.

## 9. Change control

This contract is canonical. Changes require:

- governance update,
- explicit version bump,
- and a deterministic migration plan for any existing truth artifacts.

No silent behavioral changes are permitted.
