---
id: C2_BUNDLED_C_EXPOSURE_CONVERGENCE_LIFECYCLE_CONTRACT_V1
title: "Bundled C: Exposure Convergence + Lifecycle Integrity + Global Kill Switch (Contract V1)"
status: DRAFT
version: V1
created_utc: 2026-02-16
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - bundled_c
  - exposure
  - reconciliation
  - delta_orders
  - lifecycle
  - kill_switch
  - audit_grade
  - deterministic
  - fail_closed
  - single_writer
---

# Bundled C Contract V1

## 1. Objective

This contract defines a deterministic, audit-proof control-plane spine that:

1) Computes **Target Exposure** from governed intents for a given `DAY`.
2) Computes **Actual Exposure constraints** from governed positions/lifecycle truth for the same `DAY`.
3) Produces a deterministic **Delta Order Plan** to converge actual → target, including explicit closures when target decreases.
4) Enforces **Lifecycle Integrity** (no illegal state transitions).
5) Integrates a **Global Kill Switch** that can force target exposure to zero (flatten) and blocks new entries at the Phase D submission boundary.

No component governed by this contract may assume missing data is safe. Default stance is **FAIL-CLOSED**.

## 2. Authority and scope

- Repository root: `/home/node/constellation_2_runtime`
- Runtime truth root (immutable authority): `constellation_2/runtime/truth/**`
- Governance authority: `governance/**` (schemas + this contract)
- This contract governs **Bundled C** writers:
  - `ops/tools/run_global_kill_switch_v1.py`
  - `ops/tools/run_exposure_reconciliation_v1.py`
  - `ops/tools/run_lifecycle_ledger_v1.py`
- This contract also governs enforcement in Phase D:
  - `constellation_2/phaseD/lib/submit_boundary_paper_v1.py`

## 3. Non-negotiable invariants

### 3.1 Immutability
- Writers MUST refuse overwrite for any output file path.
- Writers MUST refuse “append” semantics to governed JSON artifacts.
- If an output exists:
  - If bytes identical to would-be output: MAY print `OK: EXISTS_IDENTICAL` and exit 0.
  - Otherwise: MUST hard fail with `REFUSE_OVERWRITE_EXISTING_FILE`.

### 3.2 Determinism
- Outputs MUST be deterministic given identical inputs.
- Any non-deterministic source (wall clock, unordered iteration) is forbidden.
- Canonical JSON must be stable:
  - sorted keys
  - no floats (see 3.3)
  - newline at end of file

### 3.3 No floats
- Governed outputs MUST NOT contain JSON floats.
- Fractions MUST be encoded as **decimal strings**.
- Currency quantities MUST be expressed as integer cents where used.

### 3.4 Schema validation
- Every output artifact MUST validate against its governed schema (listed in manifest).
- Every input artifact MUST be schema-validated when a governed schema exists.
- Schema validation failure MUST cause FAIL-CLOSED behavior and must be recorded via reason codes.

### 3.5 Explainability and lineage
- Every output artifact MUST include:
  - `producer` metadata (repo/module/git_sha)
  - `input_manifest` entries with sha256 for all inputs used
  - reason codes sufficient to explain FAIL/DEGRADED states
  - a self-sha field computed from canonical JSON with the self-sha field excluded

## 4. Inputs (by DAY)

Bundled C operates on an explicit `DAY` key formatted `YYYY-MM-DD`.

Inputs (all day-keyed; `latest.json` is forbidden for readiness decisions):

### 4.1 Intents (target exposure)
- Directory: `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/`
- Files: `*.exposure_intent.v1.json`
- Must validate against: `constellation_2/schemas/exposure_intent.v1.schema.json`

### 4.2 Positions (actual exposure constraints)
- Pointer: `constellation_2/runtime/truth/positions_v1/effective_v1/days/<DAY>/positions_effective_pointer.v1.json`
- Selected snapshot path is taken from pointer; snapshot must validate against its governed schema.

### 4.3 Lifecycle snapshot (existing, informational)
- `constellation_2/runtime/truth/position_lifecycle_v1/snapshots/<DAY>/position_lifecycle_snapshot.v1.json`

### 4.4 NAV (optional but constrains numeric convergence)
- `constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json`

### 4.5 Gates used by kill switch
- Operator gate verdict:
  - `constellation_2/runtime/truth/reports/operator_gate_verdict_v1/<DAY>/operator_gate_verdict.v1.json`
- Capital risk envelope:
  - `constellation_2/runtime/truth/reports/capital_risk_envelope_v1/<DAY>/capital_risk_envelope.v1.json`
- Reconciliation report v2 (broker evidence compatibility):
  - `constellation_2/runtime/truth/reports/reconciliation_report_v2/<DAY>/reconciliation_report.v2.json`

## 5. Outputs (by DAY)

### 5.1 Kill switch state (always emitted; fail-closed default)
- `constellation_2/runtime/truth/risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json`
- If required gate inputs are missing, kill switch MUST default to `ACTIVE`.

### 5.2 Exposure reconciliation report
- `constellation_2/runtime/truth/reports/exposure_reconciliation_report_v1/<DAY>/exposure_reconciliation_report.v1.json`
- MUST include target exposure per `(engine_id, underlying)` and actual exposure constraints.
- If actual exposure cannot be computed in compatible units, status MUST be `FAIL` or `DEGRADED` and delta plan mode MUST be reduce-only or flatten-only (see 5.3).

### 5.3 Delta order plan
- `constellation_2/runtime/truth/reports/delta_order_plan_v1/<DAY>/delta_order_plan.v1.json`
- MUST be deterministic.
- MUST set `mode`:
  - `NORMAL` only when inputs are sufficient to compute safe convergence.
  - `REDUCE_ONLY` when increasing exposure is not provably safe.
  - `FLATTEN_ONLY` when kill switch is ACTIVE or when actual exposure is unknown for any open positions.

### 5.4 Lifecycle ledger
- `constellation_2/runtime/truth/position_lifecycle_v1/ledger/<DAY>/position_lifecycle_ledger.v1.json`
- MUST enforce legal transitions only.
- MUST emit explicit “closing requested” directives when target decreases below current holdings (where provable).
- Any illegal transition MUST cause `status=FAIL` and must be recorded.

## 6. Phase D enforcement (hard boundary)

Phase D submission boundary MUST enforce kill switch:

- If kill switch state is missing for `DAY`, treat as `ACTIVE`.
- If state is `ACTIVE`, Phase D MUST emit a `veto_record.v1.json` with:
  - boundary = `SUBMIT`
  - reason_code = `C2_KILL_SWITCH_ACTIVE`
  - reason_detail includes the kill state file path and sha256
- Phase D MUST exit with code `2` (veto) without broker calls.

## 7. Required reason codes (non-exhaustive)

### Kill switch
- `C2_KILL_SWITCH_ACTIVE`
- `C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS`
- `C2_KILL_SWITCH_INPUT_SCHEMA_INVALID`

### Exposure reconciliation / delta plan
- `C2_EXPOSURE_INPUTS_MISSING_FAILCLOSED`
- `C2_EXPOSURE_ACTUAL_UNKNOWN_FAILCLOSED`
- `C2_EXPOSURE_NAV_MISSING_REDUCE_ONLY`
- `C2_EXPOSURE_KILL_SWITCH_FORCES_FLATTEN`

### Lifecycle ledger
- `C2_LIFECYCLE_ILLEGAL_TRANSITION_FAILCLOSED`
- `C2_LIFECYCLE_INPUTS_MISSING_FAILCLOSED`

## 8. Enforcement integration requirements

- Pipeline manifest MUST add blocking stages for Bundled C.
- Operator gate verdict MUST require Bundled C artifacts to exist and pass required checks.
- Any claim of readiness MUST fail closed if Bundled C artifacts are missing or FAIL.
