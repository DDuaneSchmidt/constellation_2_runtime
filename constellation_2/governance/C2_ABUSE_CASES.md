---
id: C2_ABUSE_CASES_V1
title: Constellation 2.0 Abuse Cases and Fail-Closed Demonstrations
version: 1
status: DRAFT
type: hostile_review_pack
created: 2026-02-13
authority_level: ROOT_SUPPORT
---

# C2 Abuse Cases

## 1. Purpose

This document enumerates plausible misuse and failure scenarios
and describes how C2 responds in a fail-closed manner.

---

## 2. Abuse Case: Attempt Naked Short Option

Scenario:
- Intent specifies structure VERTICAL_SPREAD
- Mapper selects only one short leg (long leg missing)

Expected behavior:
- Invariant C2_DEFINED_RISK_REQUIRED triggers
- VetoRecord emitted
- No OrderPlan produced
- No BindingRecord produced
- No BrokerSubmissionRecord produced

PASS criteria:
- Evidence chain ends at VetoRecord

---

## 3. Abuse Case: Missing Exit Policy

Scenario:
- Intent omits exit_policy

Expected behavior:
- Invariant C2_EXIT_POLICY_REQUIRED triggers at INTENT boundary
- VetoRecord emitted

PASS criteria:
- No OrderPlan exists

---

## 4. Abuse Case: Expired Snapshot

Scenario:
- FreshnessCertificate valid_until_utc < now

Expected behavior:
- Invariant C2_FRESHNESS_CERT_INVALID_OR_EXPIRED triggers
- VetoRecord emitted at MAPPING boundary

PASS criteria:
- No BindingRecord created

---

## 5. Abuse Case: Hash Tampering

Scenario:
- OrderPlan hash modified before submission

Expected behavior:
- BindingRecord generation fails
- C2_BINDING_HASH_MISMATCH triggers
- VetoRecord emitted

PASS criteria:
- No broker call attempted

---

## 6. Abuse Case: Non-Deterministic Selection

Scenario:
- Contract list order changes across runs
- Mapper chooses different strikes

Expected behavior:
- Deterministic replay mismatch detected
- C2_NONDETERMINISTIC_SELECTION_RULE triggers
- HARD FAIL

PASS criteria:
- Replay inconsistency flagged before submission

---

## 7. Abuse Case: Silent Broker Submit

Scenario:
- Implementation attempts broker call before writing BindingRecord

Expected behavior:
- C2_SUBMIT_FAIL_CLOSED_REQUIRED triggers
- System aborts

PASS criteria:
- No broker IDs recorded

---

## 8. Explicit Non-Claims

This document does not claim:

- Immunity from economic loss
- Immunity from exchange-level outages
- Immunity from broker execution anomalies

It demonstrates structural fail-closed enforcement only.
