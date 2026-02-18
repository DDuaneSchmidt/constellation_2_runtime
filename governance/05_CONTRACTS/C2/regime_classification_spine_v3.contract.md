---
id: C2_REGIME_CLASSIFICATION_SPINE_V3_CONTRACT
title: "Regime Classification Spine v3 (Forward-Only, Envelope v2 Aware)"
status: CANONICAL
version: 3
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - monitoring
  - regime
  - audit-grade
  - deterministic
  - fail-closed
  - safe-idle
  - forward-only
---

# Regime Classification Spine v3 Contract

## 0. Motivation (Forward-Only)

Regime v2 references `capital_risk_envelope_v1`, which may be permanently FAIL for a day due to earlier missing-input runs.
v3 is a forward-only regime artifact that references `capital_risk_envelope_v2` for readiness decisions.

v3 MUST NOT overwrite v2 regime artifacts.

## 1. Output (immutable truth)

Writes an immutable artifact at:

- `constellation_2/runtime/truth/monitoring_v1/regime_snapshot_v3/<DAY>/regime_snapshot.v3.json`

Conforms to:

- `governance/04_DATA/SCHEMAS/C2/MONITORING/regime_snapshot.v3.schema.json`

## 2. Inputs (authoritative truth)

Required inputs:

1) Accounting NAV:
- `constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json`

2) Economic NAV drawdown snapshot:
- `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_snapshot/<DAY>/nav_snapshot.v1.json`

3) Engine risk budget ledger:
- `constellation_2/runtime/truth/risk_v1/engine_budget/<DAY>/engine_risk_budget_ledger.v1.json`

4) Capital risk envelope v2:
- `constellation_2/runtime/truth/reports/capital_risk_envelope_v2/<DAY>/capital_risk_envelope.v2.json`

5) Broker manifest (required only if submissions present):
- `constellation_2/runtime/truth/execution_evidence_v1/broker_events/<DAY>/broker_event_day_manifest.v1.json`

## 3. Policy: operational day invariant

The producer MUST refuse future-day writes using `enforce_operational_day_key_invariant_v1`.

## 4. SAFE_IDLE behavior

If `submissions_present == false`:
- `broker_manifest_required == false`
- Missing broker manifest MUST NOT cause CRASH.

If `capital_risk_envelope_v2.status == PASS` and drawdown is not in CRASH thresholds:
- regime should normally be `NORMAL` with `blocking == false`.

## 5. Deterministic produced_utc

Producer uses a deterministic timestamp for day:
- `produced_utc = <DAY>T23:59:59Z`

## 6. Blocking rule

`blocking` MUST be true if and only if:
- regime_label is CRASH, OR
- broker truth missing while submissions are present, OR
- capital risk envelope v2 severe failure indicates missing critical inputs

## 7. Audit requirements

Artifact MUST include:
- sha256 self-hash field `snapshot_sha256`
- input_manifest with sha256 for all required inputs (or explicit *_missing sentinels)
- explicit evidence fields reflecting submissions/broker requirement and envelope v2 status
