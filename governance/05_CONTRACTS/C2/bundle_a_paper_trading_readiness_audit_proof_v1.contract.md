id: C2_BUNDLE_A_PAPER_TRADING_READINESS_AUDIT_PROOF_V1
title: "Bundle A Paper Trading Readiness Audit Proof V1 (Institutional, Fail-Closed)"
status: DRAFT
version: V1
created_utc: 2026-02-16
last_reviewed: 2026-02-18
owner: CONSTELLATION
authority: governance+git+runtime_truth
domain: OPS
tags:
  - paper-trading
  - audit-proof
  - hostile-review
  - fail-closed
  - determinism
  - provenance
  - reconciliation
  - submission-evidence
---

# Bundle A — Paper Trading Readiness Audit Proof V1

## 1. Objective

Define the minimum immutable runtime truth artifacts, schemas, and fail-closed gates required to declare a trading day **READY** for paper trading under hostile institutional review (quants + risk committee).

No claim of readiness is valid unless the requirements in this contract are satisfied using only governed runtime truth artifacts.

---

## 2. Definitions

- **DAY**: UTC day key in `YYYY-MM-DD` format. All artifacts MUST be keyed by DAY.
- **Runtime truth**: immutable outputs under `constellation_2/runtime/truth/**`.
- **READY**: a day state where all required artifacts exist, lineage is intact, determinism is demonstrated, execution mode is real IB paper, and reconciliation passes.
- **SIMULATED**: any execution evidence not sourced from live broker truth (including any marker like `SYNTH_*`).
- **REAL_IB_PAPER**: execution evidence sourced from Interactive Brokers paper account broker truth (orders/status/fills), normalized deterministically with raw broker payload digests captured.

---

## 3. Non-negotiable invariants (MUST)

For DAY to be READY, all MUST hold:

### I1. Completeness (no gaps)
All required Bundle A artifacts exist for DAY at their canonical paths.

### I2. No "latest.json" inputs
No readiness decision may consume `latest.json` pointers or “most recent” aliases. Readiness is keyed only by explicit DAY paths.

### I3. Hash lineage (audit DAG)
Each required artifact MUST:
- include its own canonical sha256 (over canonical JSON form), and
- reference upstream artifact sha256 values (directly or via an input manifest sha256).

### I4. Determinism (replay proof)
Re-running the same DAY with identical inputs MUST produce byte-identical canonical JSON for all required artifacts (sha256 stable).

### I5. Fail-closed gating
If any required artifact is missing, malformed, mismatched by sha256, or inconsistent, readiness MUST fail CLOSED (nonzero exit + failure truth artifact).

### I6. Execution realism boundary (no simulation leakage)
READY requires `REAL_IB_PAPER_ONLY`.

Any SIMULATED execution evidence for DAY forbids READY.

### I7. Reconciliation required
Broker truth vs internal truth reconciliation MUST be `PASS`.

`DEGRADED` is forbidden for READY.

---

## 4. Required Bundle A truth artifacts (MUST)

All paths are relative to repo root.

---

### A) Intents day rollup

constellation_2/runtime/truth/intents_v1/day_rollup/DAY/intents_day_rollup.v1.json


Purpose: prove all engines executed and enumerate intent hashes (including explicit zero-intent engines).

---

### B) Submission evidence (canonicalized daily ledger)

Submission evidence for DAY MUST be satisfied by **one** of the following canonical surfaces:

#### B1 (legacy): Submission index

constellation_2/runtime/truth/execution_evidence_v1/submission_index/DAY/submission_index.v1.json


#### B2 (preferred): Pillars decision records

constellation_2/runtime/truth/pillars_v1r1/DAY/decisions/*.submission_decision_record.v1.json


- At least one decision record MUST exist if submissions exist.
- Decision records MUST be compiled from broker submission evidence.
- Decision records MUST encode execution realism boundary.

#### B3 (no submissions case)

If there are **no submission directories** under:

constellation_2/runtime/truth/execution_evidence_v1/submissions/DAY/


then submission evidence is considered satisfied for DAY.

Purpose:
- provide a canonical ledger for all submissions with lineage to broker outcome,
- classify execution realism (`REAL_IB_PAPER_ONLY`),
- reduce audit surface by collapsing multi-file submission evidence into atomic decision records.

---

### C) Reconciliation report (broker vs internal)

constellation_2/runtime/truth/reports/reconciliation_report_v2/DAY/reconciliation_report.v2.json


Purpose: prove internal positions/accounting agree with broker truth.

Verdict MUST be `PASS` for READY.

---

### D) Pipeline manifest (end-to-end inventory, v2)

constellation_2/runtime/truth/reports/pipeline_manifest_v2/DAY/pipeline_manifest.v2.json


Purpose:
- enumerate every required spine artifact and sha256 in a single committee-friendly artifact,
- collapse submission evidence dependency onto pillars-aware surface.

---

### E) Operator gate verdict (Bundle A readiness, v2)

constellation_2/runtime/truth/reports/operator_gate_verdict_v2/DAY/operator_gate_verdict.v2.json


Purpose:
- fail-closed readiness verdict,
- explicitly evaluate submission evidence via B1 or B2,
- enumerate missing artifacts and sha mismatches,
- provide committee-readable READY boolean.

---

## 5. Forbidden conditions (MUST fail readiness)

F1. Any missing required artifact path for DAY.  
F2. Any submission evidence indicating mode != `REAL_IB_PAPER`  
  (either via submission index records OR via pillars decision evidence).  
F3. Any `SYNTH_` marker anywhere under `execution_evidence_v1/submissions/DAY/**`.  
F4. Reconciliation verdict != `PASS`.  
F5. Operator gate verdict v2 `ready != true` or `exit_code != 0`.  
F6. Any readiness computation using `latest.json`.  

---

## 6. Committee-grade audit procedure (MUST be possible)

Given DAY, an auditor must be able to:

1) Verify existence of A–E artifacts for DAY.  
2) Verify pipeline manifest v2 enumerates A–E with sha256.  
3) Verify operator gate verdict v2 is `ready=true` and `exit_code=0`.  
4) Verify submission evidence indicates only `REAL_IB_PAPER`
   (submission index records OR pillars decisions compiled from broker submission evidence).  
5) Verify reconciliation report verdict is `PASS`.  
6) Verify determinism by replaying DAY and confirming identical sha256.  

---

## 7. Enforcement

Any operator workflow, automation, or service claiming paper-trading readiness MUST fail CLOSED if this contract is not satisfied.

Submission index is no longer a mandatory surface if pillars decision records exist.

End of contract.
