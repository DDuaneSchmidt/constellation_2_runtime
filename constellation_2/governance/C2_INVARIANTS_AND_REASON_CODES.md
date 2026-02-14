---
id: C2_INVARIANTS_AND_REASON_CODES_V1
title: Constellation 2.0 Invariants and Reason Codes (Fail-Closed)
version: 1
status: DRAFT
type: invariants_and_reason_codes
created: 2026-02-13
authority_level: ROOT_SUPPORT
---

# C2 Invariants and Reason Codes (Fail-Closed)

## 1. Purpose

This document defines:
- The mandatory machine-checkable invariants for C2
- The canonical reason codes emitted in `VetoRecord`
- The minimum evidence fields required for hostile review

All enforcement boundaries MUST use this list.

---

## 2. Reason Code Format

Reason codes are stable identifiers.

Rules:
- Must start with `C2_`
- Must be uppercase snake case
- Must not be reused for different meanings
- Must be suitable for downstream aggregation

---

## 3. Mandatory Invariants (Bundle A)

### C2_OPTIONS_ONLY_VIOLATION
**Invariant:** Intent/Plan MUST be options-only. No STK orders.  
**Detect:** Any leg has asset class != OPT OR any order kind implies equity spot.  
**Boundary:** INTENT, MAPPING, SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_DEFINED_RISK_REQUIRED
**Invariant:** Entry MUST be defined-risk. No naked short options.  
**Detect:** Any short option leg without a corresponding long leg bounding risk.  
**Boundary:** MAPPING, SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_EXIT_POLICY_REQUIRED
**Invariant:** Every intent MUST carry an exit policy.  
**Detect:** Missing exit policy OR policy missing required fields.  
**Boundary:** INTENT, MAPPING, SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_BINDING_HASH_MISMATCH
**Invariant:** Binding chain must be intact: Intent → Plan → Binding → BrokerSubmissionRecord.  
**Detect:** Recomputed canonical hash mismatch OR missing upstream hash pointer.  
**Boundary:** MAPPING, SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_FRESHNESS_CERT_INVALID_OR_EXPIRED
**Invariant:** Market snapshot freshness must be enforced.  
**Detect:** Missing FreshnessCertificate OR expired OR snapshot hash mismatch.  
**Boundary:** MAPPING, SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_MAPPING_FAIL_CLOSED_REQUIRED
**Invariant:** Mapper must emit either a valid OrderPlan or a VetoRecord, never partial output.  
**Detect:** Attempt to proceed with missing chain inputs, missing expiry, missing strikes, or ambiguous selection.  
**Boundary:** MAPPING  
**Fail behavior:** VETO (mandatory)

---

### C2_SUBMIT_FAIL_CLOSED_REQUIRED
**Invariant:** Submitter must revalidate and emit either BrokerSubmissionRecord or VetoRecord.  
**Detect:** Any broker call attempted without binding record written first, or without full revalidation.  
**Boundary:** SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_SINGLE_WRITER_VIOLATION
**Invariant:** Truth artifacts are immutable and single-writer.  
**Detect:** Same artifact_id or logical pointer written twice with different canonical hash.  
**Boundary:** ALL  
**Fail behavior:** HARD FAIL

---

### C2_DETERMINISM_CANONICALIZATION_FAILED
**Invariant:** Canonical JSON + hashing must succeed deterministically.  
**Detect:** Canonicalization failure or schema violation prevents canonical form creation.  
**Boundary:** ALL  
**Fail behavior:** VETO (mandatory)

---

### C2_NONDETERMINISTIC_SELECTION_RULE
**Invariant:** Strike/expiry selection must be fully deterministic.  
**Detect:** Any selection depends on unordered iteration, random choice, or missing tie-break rules.  
**Boundary:** MAPPING  
**Fail behavior:** VETO (mandatory)

---

### C2_PRICE_DETERMINISM_FAILED
**Invariant:** Limit price derivation must be deterministic.  
**Detect:** Missing tick size policy, missing rounding rule, or ambiguous price source.  
**Boundary:** MAPPING, SUBMIT  
**Fail behavior:** VETO (mandatory)

---

## 4. Required VetoRecord Evidence Fields (Hostile Review Minimum)

Every `VetoRecord` MUST include:

- `reason_code` (from this document)
- `reason_detail` (human-readable, concise)
- `boundary` (INTENT | MAPPING | SUBMIT | OTHER)
- `observed_at_utc`
- `inputs`:
  - `intent_hash` (if present)
  - `plan_hash` (if present)
  - `chain_snapshot_hash` (if present)
  - `freshness_cert_hash` (if present)
- `pointers`:
  - file path(s) or object reference(s) sufficient to locate the evidence

If any required field is missing:
→ HARD FAIL (cannot emit malformed veto)

---

## 5. Explicit Non-Claims

This document does not claim:
- coverage of every possible options structure (future expansions expected)
- correctness of liquidity proxies
- correctness of broker contract mapping details beyond what is explicitly defined in schemas
