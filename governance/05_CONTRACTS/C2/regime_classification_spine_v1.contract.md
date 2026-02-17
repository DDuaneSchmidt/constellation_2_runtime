---
id: C2_REGIME_CLASSIFICATION_SPINE_V1
title: "Regime Classification Spine v1 (Deterministic, Fail-Closed, Audit-Proof)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - regime
  - risk
  - monitoring
  - audit-grade
  - immutable
  - deterministic
  - fail-closed
---

# Regime Classification Spine v1

## 1. Objective

Produce a **day-scoped** immutable regime snapshot that:

- is derived **only** from governed internal truth artifacts,
- is **deterministic**, **schema-validated**, and **fail-closed**,
- is written as an **immutable truth artifact** (refuse overwrite unless identical),
- is suitable for hostile institutional review, risk committees, and external auditors,
- integrates into:
  - pipeline manifest (structural readiness),
  - operator gate verdict (paper-trading readiness).

The regime snapshot is an **audit trail** artifact, not an alpha model.

## 2. Output (Immutable Truth)

### 2.1 Output path

Writer MUST produce exactly one day-scoped immutable JSON artifact:

- `constellation_2/runtime/truth/monitoring_v1/regime_snapshot_v1/<DAY>/regime_snapshot.v1.json`

Where `<DAY>` is `YYYY-MM-DD` in UTC day-key convention.

### 2.2 Output schema

The output MUST validate against governed schema:

- `governance/04_DATA/SCHEMAS/C2/MONITORING/regime_snapshot.v1.schema.json`

### 2.3 Canonicalization and hashing

The writer MUST:

- emit **canonical JSON** bytes: sorted keys, separators `(",", ":")`, and **one trailing newline**,
- include `snapshot_sha256` computed as:
  - `sha256(canonical_json_without_snapshot_sha256_field)`,
  - where the field is set to `null` for hashing.
- include `producer.git_sha`,
- include `input_manifest` with `sha256` for every input used.

**No floats are permitted** anywhere in the output (Decimal strings only).

## 3. Inputs (Governed Truth Only)

The writer MUST read the following governed truth artifacts:

### 3.1 Required: Accounting NAV presence (NAV existence proof)

- `constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json`

Fail-closed if missing.

Rationale:
- This is the authoritative accounting NAV artifact for the day’s economic state.
- It is required to prove that regime classification is anchored to the accounting spine.

### 3.2 Required: Economic drawdown snapshot (drawdown_pct authority)

- `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_snapshot/<DAY>/nav_snapshot.v1.json`

The writer MUST extract:

- `drawdown_pct` as a **Decimal string** with 6 decimals.

Fail-closed if missing or if `drawdown_pct` is missing/empty/non-string.

Rationale:
- Accounting nav may not contain drawdown history fields (they may be `null` during bootstrap).
- The economic nav drawdown spine is the governed authority for `drawdown_pct` under `C2_DRAWDOWN_CONVENTION_V1`.

### 3.3 Required: Engine Risk Budget Ledger

- `constellation_2/runtime/truth/risk_v1/engine_budget/<DAY>/engine_risk_budget_ledger.v1.json`

Fail-closed if missing.

### 3.4 Optional but governed inputs (used when present)

If present, the writer MUST read and include in input_manifest:

- Capital Risk Envelope:
  - `constellation_2/runtime/truth/reports/capital_risk_envelope_v1/<DAY>/capital_risk_envelope.v1.json`

- Broker Day Manifest (required only when submissions exist):
  - `constellation_2/runtime/truth/execution_evidence_v1/broker_events/<DAY>/broker_event_day_manifest.v1.json`

- Execution evidence submissions day directory (presence implies submissions exist):
  - `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/`

Optional future extensibility inputs MAY be added in later versions only if:
- governed schemas exist, and
- the contract is updated and registered in governance manifest.

## 4. Deterministic Regime Labels (v1)

Exactly four labels exist:

- `NORMAL`
- `HIGH_RISK`
- `STRESS`
- `CRASH`

The writer MUST output:

- `regime_label`
- `risk_multiplier` (Decimal string with 2 decimals)
- `blocking` (boolean)
- `reason_codes` (deterministic list)

## 5. Deterministic Rule Set (MUST)

### 5.1 Threshold conventions

- `drawdown_pct` is negative when underwater (per `C2_DRAWDOWN_CONVENTION_V1`).
- Comparisons MUST be exact decimal comparisons (no floats).
- Thresholds are fixed and audited:

  - `-0.050000`
  - `-0.100000`
  - `-0.150000`

### 5.2 Rule ordering

Rules MUST be applied in priority order (first match wins):

1) **CRASH**
2) **STRESS**
3) **HIGH_RISK**
4) **NORMAL**

### 5.3 CRASH (highest severity)

Regime MUST be `CRASH` if ANY of the following conditions hold:

- `drawdown_pct <= -0.150000`
- **Severe envelope failure** (definition below)
- **Broker truth missing during submissions** (definition below)

CRASH outputs:
- `risk_multiplier = "0.25"`
- `blocking = true`

### 5.4 STRESS

Regime MUST be `STRESS` if ANY of the following conditions hold (and CRASH did not trigger):

- `drawdown_pct <= -0.100000`
- Capital risk envelope status is `FAIL` or `DEGRADED` (i.e., not `PASS`)

STRESS outputs:
- `risk_multiplier = "0.50"`
- `blocking = true`

### 5.5 HIGH_RISK

Regime MUST be `HIGH_RISK` if ANY of the following conditions hold (and CRASH/STRESS did not trigger):

- `drawdown_pct <= -0.050000`
- Broker day manifest status is `DEGRADED` or `FAIL` (when submissions exist)

HIGH_RISK outputs:
- `risk_multiplier = "0.75"`
- `blocking = false`

### 5.6 NORMAL

Regime MUST be `NORMAL` otherwise.

NORMAL outputs:
- `risk_multiplier = "1.00"`
- `blocking = false`

## 6. Definitions (Deterministic)

### 6.1 “Broker truth missing during submissions”

Let:

- `submissions_present := execution_evidence_v1/submissions/<DAY>/ exists AND contains at least one subdirectory`

If `submissions_present` is true, then broker truth is REQUIRED.

Broker truth is considered **missing** if ANY of:

- broker day manifest file does not exist at:
  - `execution_evidence_v1/broker_events/<DAY>/broker_event_day_manifest.v1.json`
- broker day manifest `status` is not exactly `OK`

If broker truth is missing under `submissions_present`, the regime MUST be `CRASH`.

### 6.2 “Severe envelope failure”

If capital risk envelope file is present, define:

- `envelope_status := capital_risk_envelope.status` (enum: PASS/FAIL/DEGRADED)

A **severe envelope failure** is true if:

- `envelope_status == "FAIL"` AND (
  - `checks.nav_present == false` OR
  - `checks.drawdown_present == false` OR
  - `checks.positions_present == false` OR
  - any of `reason_codes` contains substring `FAILCLOSED`
)

If severe envelope failure is true, the regime MUST be `CRASH`.

Rationale:
- A FAIL that is explicitly fail-closed (missing required economic inputs or provability failures)
  is treated as a “system integrity risk” state equivalent to crash-level risk management.

## 7. Fail-Closed Requirements (Hard)

The regime writer MUST exit non-zero if ANY of:

- required accounting NAV file missing,
- required economic nav drawdown snapshot missing,
- required `drawdown_pct` missing/empty/invalid,
- required engine risk budget ledger missing,
- schema validation fails,
- attempted overwrite of immutable truth with different bytes.

The writer MUST refuse to proceed if any required input is missing.

## 8. Integration Requirements

### 8.1 Pipeline manifest integration (Bundle A)

Pipeline manifest MUST include stage:

- `REGIME_CLASSIFICATION`

Pipeline manifest MUST:

- FAIL if the regime snapshot is missing,
- FAIL if regime snapshot status != OK,
- FAIL if regime snapshot `blocking == true`,
- propagate regime reason codes upward.

### 8.2 Operator gate integration

Operator gate MUST enforce:

- regime snapshot exists,
- regime snapshot status == OK,
- if `blocking == true` then operator gate `ready=false`.

Risk multiplier MUST be surfaced for downstream compression extensibility by embedding it deterministically in operator gate check details (schema-safe).

## 9. Versioning and Change Control

Any change to:

- thresholds,
- rule ordering,
- input sources,
- output fields,
- reason code meanings,

requires:

- a new contract version,
- a new schema version if output format changes,
- registration updates in `governance/00_MANIFEST.yaml` and `governance/00_INDEX.md`.

