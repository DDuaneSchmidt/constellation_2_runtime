---
id: C2_INVARIANTS_AND_REASON_CODES_V2
title: Constellation 2.0 Invariants and Reason Codes (Fail-Closed)
version: 2
status: DRAFT
type: invariants_and_reason_codes
created: 2026-02-14
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

## 3. Mandatory Invariants (Bundle A/B/C)

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

## 4. Mandatory Invariants (Phase D: Paper Broker Integration + Execution Lifecycle)

### C2_BROKER_ENV_NOT_PAPER
**Invariant:** Phase D broker interaction MUST be PAPER only. LIVE is forbidden.  
**Detect:** Any broker environment != PAPER at submit boundary OR adapter constructed without explicit PAPER mode.  
**Boundary:** SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_BROKER_ADAPTER_NOT_AVAILABLE
**Invariant:** No broker call may occur unless a deterministic adapter implementation is available and enabled.  
**Detect:** Selected adapter backend requires missing dependency OR adapter cannot be constructed deterministically.  
**Boundary:** SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_WHATIF_REQUIRED
**Invariant:** A WhatIf (margin/risk) precheck MUST be executed before any broker submission attempt.  
**Detect:** Submit path proceeds without a WhatIf result bound to the decision.  
**Boundary:** SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_RISK_BUDGET_SCHEMA_INVALID
**Invariant:** RiskBudget MUST validate against schema prior to enforcement.  
**Detect:** RiskBudget missing/invalid/unparseable OR schema validation fails.  
**Boundary:** SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_RISK_BUDGET_EXCEEDED
**Invariant:** WhatIf projected margin/notional impact MUST not exceed RiskBudget caps.  
**Detect:** WhatIf projected margin or notional > allowed budget (portfolio or per-engine).  
**Boundary:** SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_IDEMPOTENCY_DUPLICATE_SUBMISSION
**Invariant:** Duplicate submission attempts for the same binding_hash MUST be blocked.  
**Detect:** A submission_id derived from binding_hash already exists in the evidence store.  
**Boundary:** SUBMIT  
**Fail behavior:** HARD FAIL

---

### C2_BROKER_CALL_WITHOUT_BINDING
**Invariant:** No broker interaction may occur unless BindingRecord is already written immutably.  
**Detect:** Adapter.submit_order invoked before BindingRecord exists on disk in the submission out_dir.  
**Boundary:** SUBMIT  
**Fail behavior:** VETO (mandatory)

---

### C2_BROKER_SUBMISSION_RECORD_REQUIRED
**Invariant:** Any broker submission attempt MUST emit a BrokerSubmissionRecord OR a VetoRecord. No silent exits.  
**Detect:** Adapter invoked but no BrokerSubmissionRecord written; or submit path returns without a governed outcome artifact.  
**Boundary:** SUBMIT  
**Fail behavior:** HARD FAIL

---

### C2_EXECUTION_EVENT_REQUIRED
**Invariant:** Any broker submission attempt that yields broker identifiers MUST emit an ExecutionEventRecord capturing the initial status transition.  
**Detect:** BrokerSubmissionRecord written with broker_ids present but no ExecutionEventRecord written.  
**Boundary:** SUBMIT  
**Fail behavior:** HARD FAIL

---

## 5. Required VetoRecord Evidence Fields (Hostile Review Minimum)

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

## 6. Explicit Non-Claims

This document does not claim:
- coverage of every possible options structure (future expansions expected)
- correctness of liquidity proxies
- correctness of broker contract mapping details beyond what is explicitly defined in schemas
- correctness of broker-reported statuses beyond capture and binding
