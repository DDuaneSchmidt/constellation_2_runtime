---
id: C2_REGIME_CLASSIFICATION_SPINE_V2
title: "Regime Classification Spine v2 (Authoritative, Deterministic, Fail-Closed, Audit-Proof)"
status: DRAFT
version: 2
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

# Regime Classification Spine v2

## 1. Authority

This v2 spine is the authoritative regime classifier.

v1 regime snapshots are **non-authoritative** per:
- `governance/05_CONTRACTS/C2/regime_classification_spine_v1.deprecation_notice.md`

## 2. Output (Immutable Truth)

Writer MUST produce exactly one day-scoped immutable JSON artifact:

- `constellation_2/runtime/truth/monitoring_v1/regime_snapshot_v2/<DAY>/regime_snapshot.v2.json`

Schema:
- `governance/04_DATA/SCHEMAS/C2/MONITORING/regime_snapshot.v2.schema.json`

Canonical JSON:
- sorted keys, compact separators, single trailing newline.
No floats permitted (Decimal strings only).

## 3. Inputs (Governed Truth Only)

REQUIRED:
1) Accounting NAV presence proof:
   - `constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json`
2) Economic drawdown authority:
   - `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_snapshot/<DAY>/nav_snapshot.v1.json`
   - field: `drawdown_pct` (Decimal string, 6dp)
3) Engine Risk Budget Ledger:
   - `constellation_2/runtime/truth/risk_v1/engine_budget/<DAY>/engine_risk_budget_ledger.v1.json`

OPTIONAL BUT GOVERNED (used when present):
4) Capital Risk Envelope:
   - `constellation_2/runtime/truth/reports/capital_risk_envelope_v1/<DAY>/capital_risk_envelope.v1.json`
5) Execution evidence submissions directory:
   - `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/`
6) Broker Day Manifest (required if submissions exist):
   - `constellation_2/runtime/truth/execution_evidence_v1/broker_events/<DAY>/broker_event_day_manifest.v1.json`

## 4. Regime labels (v2)

- NORMAL
- HIGH_RISK
- STRESS
- CRASH

Each regime defines:
- risk_multiplier (Decimal string, 2dp)
- blocking boolean
- reason_codes (deterministic, sorted)

## 5. Deterministic rule set (priority order)

Comparisons are exact decimal comparisons (no floats).

Thresholds:
- -0.050000
- -0.100000
- -0.150000

Order:
1) CRASH
2) STRESS
3) HIGH_RISK
4) NORMAL

### 5.1 CRASH
Trigger if ANY:
- drawdown_pct <= -0.150000
- severe envelope failure
- broker truth missing during submissions

CRASH outputs:
- risk_multiplier = "0.25"
- blocking = true

### 5.2 STRESS
Trigger if ANY (and CRASH did not trigger):
- drawdown_pct <= -0.100000
- capital envelope status != PASS (when present)

STRESS outputs:
- risk_multiplier = "0.50"
- blocking = true

### 5.3 HIGH_RISK
Trigger if ANY (and CRASH/STRESS did not trigger):
- drawdown_pct <= -0.050000
- broker manifest status in {DEGRADED, FAIL} when submissions exist

HIGH_RISK outputs:
- risk_multiplier = "0.75"
- blocking = false

### 5.4 NORMAL
Otherwise:
- risk_multiplier = "1.00"
- blocking = false

## 6. Deterministic definitions

### 6.1 submissions_present
True iff:
- `execution_evidence_v1/submissions/<DAY>/` exists AND has >=1 subdirectory

### 6.2 broker truth missing during submissions
If submissions_present is true, broker manifest is required.
Missing if ANY:
- broker manifest file missing
- broker manifest status != OK

### 6.3 severe envelope failure
If capital envelope present:
- status == FAIL AND (
  - checks.nav_present == false OR
  - checks.drawdown_present == false OR
  - checks.positions_present == false OR
  - any reason_code contains substring "FAILCLOSED"
)

## 7. Fail-closed requirements

Writer MUST exit non-zero if any required input is missing, or drawdown_pct missing, or schema invalid, or immutable overwrite attempted.

Writer MUST refuse overwrite unless identical bytes.

## 8. Integration requirements

Pipeline manifest:
- stage_id: REGIME_CLASSIFICATION
- must point to v2 artifact
- FAIL if missing, status!=OK, or blocking==true

Operator gate:
- requires v2 snapshot exists
- status==OK
- if blocking==true -> ready=false
- risk_multiplier surfaced in details for future compression

