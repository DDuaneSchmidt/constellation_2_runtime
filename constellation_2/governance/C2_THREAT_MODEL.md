---
id: C2_THREAT_MODEL_V1
title: Constellation 2.0 Threat Model
version: 1
status: DRAFT
type: hostile_review_pack
created: 2026-02-13
authority_level: ROOT_SUPPORT
---

# C2 Threat Model

## 1. Purpose

This document identifies structural risks and adversarial scenarios relevant to C2.

It does not assume benevolent operators.
It assumes hostile review conditions.

---

## 2. Assets to Protect

- Capital (paper or live)
- Structural determinism
- Evidence chain integrity
- Market data freshness enforcement
- Binding integrity between intent and submission

---

## 3. Threat Actors

1. Operator error
2. Code defects
3. Market data inconsistency
4. Broker API anomalies
5. Non-deterministic runtime behavior
6. Malicious mutation of truth artifacts

---

## 4. Structural Threats

### 4.1 Undefined Risk Entry
Threat:
- Naked short option accidentally allowed

Mitigation:
- Schema enforcement
- Risk proof required in OrderPlan
- Invariant C2_DEFINED_RISK_REQUIRED

---

### 4.2 Stale Market Data
Threat:
- Mapping uses expired snapshot

Mitigation:
- FreshnessCertificate required
- Hard expiration window
- Invariant C2_FRESHNESS_CERT_INVALID_OR_EXPIRED

---

### 4.3 Non-Deterministic Strike Selection
Threat:
- Unordered contract list produces different result

Mitigation:
- Deterministic tie-break rules
- Explicit selection trace in MappingLedgerRecord
- Invariant C2_NONDETERMINISTIC_SELECTION_RULE

---

### 4.4 Hash Chain Tampering
Threat:
- Upstream artifact altered after mapping

Mitigation:
- Canonical JSON hashing
- BindingRecord enforcement
- Invariant C2_BINDING_HASH_MISMATCH

---

### 4.5 Silent Broker Submission
Threat:
- Order sent without recording binding record

Mitigation:
- Submit boundary requires BindingRecord BEFORE broker call
- Invariant C2_SUBMIT_FAIL_CLOSED_REQUIRED

---

## 5. Residual Risks (Non-Structural)

C2 does not eliminate:

- Broker-side outages
- Market halts
- Latency differences
- Fill slippage
- Regime misclassification

C2 claims only structural correctness and fail-closed enforcement.

---

## 6. Threat Model Boundary

C2 protects structure and evidence integrity.

C2 does not attempt:
- Behavioral finance defense
- Strategy alpha defense
- Capital optimization

---

## 7. Hostile Review Claim

Under hostile review, C2 must demonstrate:

- Deterministic replay yields identical OrderPlan
- Every failed attempt produces VetoRecord
- No broker submission occurs without BindingRecord
- No undefined-risk entry possible

If any of these are violated,
C2 fails structural safety.
