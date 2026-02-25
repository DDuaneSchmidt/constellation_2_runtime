---
id: C2_SYSTEMIC_RISK_GATE_CONTRACT_V3
title: "Systemic Risk Gate Contract v3 (Fail-Closed Root Verdict with Day-0 Bootstrap Exception)"
status: CANONICAL
version: 3
created_utc: 2026-02-23
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - risk
  - systemic
  - gate
  - audit-grade
  - deterministic
  - fail-closed
  - day0-bootstrap
---

# Systemic Risk Gate Contract v3

## 0. Purpose

Systemic Risk Gate v3 produces the root-level systemic verdict for a given day.

- It is deterministic and audit-proof.
- It is fail-closed by default.
- It is forward-only and must not rewrite immutable day-keyed artifacts.

## 1. Output (immutable truth)

The gate writes the immutable report artifact at:

- `constellation_2/runtime/truth/reports/systemic_risk_gate_v3/<DAY>/systemic_risk_gate.v3.json`

The artifact MUST conform to:

- `governance/04_DATA/SCHEMAS/C2/RISK/systemic_risk_gate.v3.schema.json`

## 2. Inputs (strict mode, fail-closed)

In strict mode, the gate evaluates systemic safety using these day-scoped truth surfaces:

- Regime snapshot v2:
  - `constellation_2/runtime/truth/monitoring_v1/regime_snapshot_v2/<DAY>/regime_snapshot.v2.json`

- Engine correlation matrix v1:
  - `constellation_2/runtime/truth/monitoring_v1/engine_correlation_matrix/<DAY>/engine_correlation_matrix.v1.json`

- Global kill switch state v1:
  - `constellation_2/runtime/truth/risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json`

- Intents day directory (cluster exposure proxy):
  - `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/`

- Stress drift sentinel v1 (and optional operator override):
  - `constellation_2/runtime/truth/monitoring_v2/stress_drift_sentinel_v1/<DAY>/stress_drift_sentinel.v1.json`
  - if escalation is recommended, operator override must exist:
    - `constellation_2/runtime/truth/reports/operator_stress_override_v1/<DAY>/operator_stress_override.v1.json`

If any required input is missing or invalid in strict mode, the gate MUST fail-closed.

## 3. Day-0 Bootstrap Window definition (governed)

A day is in **Day-0 Bootstrap Window** iff:

- `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/` is missing, OR
- it exists but contains **zero submission directories**.

This definition is the ONLY allowed bootstrap trigger for this contract.

## 4. Day-0 Bootstrap exception (governed, deterministic, scoped)

If **bootstrap window is TRUE** for the day AND systemic monitoring inputs are missing:

- Gate status MUST be `OK`
- `reason_codes` MUST include:
  - `DAY0_BOOTSTRAP_SYSTEMIC_INPUTS_MISSING_ALLOWED`

The output MUST remain schema-valid and deterministic:

- `regime_ok = true`
- `correlation_ok = true`
- `kill_switch_ok = true`
- `cluster_exposure_metrics.total_intents = 0`
- `cluster_exposure_metrics.intents_by_engine = []`
- `shock_model_output` must be present and schema-valid, deterministic zeros:
  - `max_pairwise = "0.000000"`
  - `threshold_max_pairwise = "0.75"` (or the gate default if fixed)
  - `flagged_pairs_count = 0`
  - `flagged_pairs = []`

Schema compliance requirement:

- Output MUST still conform to
  `governance/04_DATA/SCHEMAS/C2/RISK/systemic_risk_gate.v3.schema.json`
- No schema bypass is permitted.
- All required fields MUST be present; values MUST be deterministic.

Scope boundary:

- This exception applies ONLY when bootstrap window is TRUE.
- Once any submission directory exists for that day, behavior MUST revert to strict fail-closed evaluation.

## 5. Audit requirements

The output MUST include:

- `producer` (repo, module, git_sha)
- `input_manifest` with at least one entry
- `gate_sha256` = sha256 of canonical JSON excluding the `gate_sha256` field

If Day-0 bootstrap exception is used, reason_codes MUST include the Day-0 reason code in §4.
