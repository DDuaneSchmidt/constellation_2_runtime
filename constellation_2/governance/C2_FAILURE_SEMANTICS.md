---
id: C2_FAILURE_SEMANTICS_V1
title: Constellation 2.0 Failure Semantics
version: 1
status: DRAFT
type: hostile_review_pack
created: 2026-02-13
authority_level: ROOT_SUPPORT
---

# C2 Failure Semantics

## 1. Purpose

This document defines how C2 categorizes, records, and enforces failures.

Failures are not implementation details — they are part of the contract.

C2 is considered safe only if all failures are:
- deterministic
- classified
- fail-closed
- recorded immutably

---

## 2. Failure Categories

### 2.1 Schema Failure
Occurs when an artifact fails JSON Schema validation.

Required behavior:
- Emit VetoRecord
- Include schema name and validation error summary
- Do NOT proceed to next boundary

Reason codes typically:
- C2_DETERMINISM_CANONICALIZATION_FAILED

---

### 2.2 Invariant Violation
Occurs when business rule invariant fails.

Examples:
- Undefined risk
- Missing exit policy
- Stale snapshot

Required behavior:
- Emit VetoRecord
- Include boundary and invariant name
- No broker call

---

### 2.3 Determinism Failure
Occurs when replay yields different output.

Examples:
- Unstable ordering
- Floating-point drift
- Missing tie-break rule

Required behavior:
- HARD FAIL
- No artifact emitted beyond failure point

---

### 2.4 Freshness Failure
Occurs when snapshot expired or mismatched.

Required behavior:
- Emit VetoRecord
- Include snapshot hash and certificate hash

---

### 2.5 Binding Integrity Failure
Occurs when:
- Upstream hash mismatch
- Missing binding pointer
- Digest mismatch

Required behavior:
- Emit VetoRecord
- No submission attempt

---

## 3. Boundary Enforcement Rules

### INTENT boundary
- Validate schema
- Validate options-only constraint
- Validate exit policy presence

### MAPPING boundary
- Validate freshness certificate
- Validate selection determinism
- Validate defined-risk proof

### SUBMIT boundary
- Revalidate everything
- Write BindingRecord BEFORE broker call
- Fail-closed on any discrepancy

---

## 4. No Silent Recovery Policy

C2 explicitly forbids:
- Implicit default injection
- Silent correction of malformed fields
- Auto-adjust of missing risk fields
- Automatic downgrade of structure

All violations must produce VetoRecord or HARD FAIL.

---

## 5. Non-Claims

This document does not define:
- Performance degradation semantics
- Risk budgeting logic
- Broker retry logic
- Network fault tolerance

It defines only structural enforcement semantics.
