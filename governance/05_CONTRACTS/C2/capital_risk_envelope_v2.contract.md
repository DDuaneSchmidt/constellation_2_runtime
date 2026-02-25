---
id: C2_CAPITAL_RISK_ENVELOPE_CONTRACT_V2
title: "Capital-at-Risk Envelope Contract V2 (Forward-Only Readiness)"
status: CANONICAL
version: 2
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - risk
  - capital-at-risk
  - envelope
  - drawdown
  - audit-grade
  - deterministic
  - fail-closed
  - safe-idle
  - forward-only
  - day0-bootstrap
---

# Capital-at-Risk Envelope Contract V2

## 0. Motivation (Forward-Only)

This contract exists to provide a forward-only readiness artifact when earlier immutable
day-keyed envelope artifacts are permanently FAIL due to missing-input runs.

V2 MUST NOT overwrite any existing v1 artifacts. It writes to a versioned path and schema.

## 1. Objective

Same as v1: deterministic, audit-proof, fail-closed enforcement of portfolio capital-at-risk,
computed from day-scoped truth inputs, prior to PhaseD submission.

Additionally:

- V2 MUST support **SAFE_IDLE**: if positions snapshot contains zero items and all inputs are present,
  enforcement MUST PASS with portfolio_capital_at_risk_cents = 0.

- V2 MUST support **Day-0 Bootstrap Window** (governed exception): allow PASS with explicit zero-risk
  envelope when the allocation summary is missing AND there are no submissions yet for that day.

## 2. Inputs (authoritative truth)

Required day-scoped truth inputs (strict mode):

1) Allocation summary truth:
- `constellation_2/runtime/truth/allocation_v1/summary/<DAY>/summary.json`

2) Positions snapshot truth (prefer v3 else v2):
- `constellation_2/runtime/truth/positions_v1/snapshots/<DAY>/positions_snapshot.v3.json`
- `constellation_2/runtime/truth/positions_v1/snapshots/<DAY>/positions_snapshot.v2.json`

3) Accounting NAV truth:
- `constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json`

4) Drawdown convention contract (canonical):
- `governance/05_CONTRACTS/C2/drawdown_convention_v1.contract.md`

## 2.1 Day-0 Bootstrap Window definition (governed)

A day is in **Day-0 Bootstrap Window** iff:

- `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/` is missing, OR
- it exists but contains **zero submission directories**.

This definition is the ONLY allowed bootstrap trigger for this contract.

## 2.2 Day-0 Bootstrap exception (governed, conservative, deterministic)

If **bootstrap window is TRUE** for the input day AND the allocation summary is missing:

- Gate result MUST be **PASS**
- `reason_codes` MUST include:
  - `DAY0_BOOTSTRAP_ALLOC_SUMMARY_MISSING_ALLOWED`

The envelope MUST be conservative and explicit (deterministic zeros):

- `allowed_capital_at_risk_cents = 0`
- `portfolio_capital_at_risk_cents = 0`
- `headroom_cents = 0`
- `nav_total_cents = 0`
- `nav_total = 0`

Checks flags MUST reflect:

- `allocation_summary_present = false`
- `nav_present = false`
- `positions_present = false`
- `drawdown_present = false`
- `positions_all_have_max_loss = true` (vacuously true under zero positions and zero-risk enforcement)
- `portfolio_within_envelope = true` ONLY because portfolio risk is explicitly zero.

Schema compliance requirement:

- Output MUST still conform to
  `governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json`
- No schema bypass is permitted.
- All required fields MUST be present; values MUST be deterministic.

Scope boundary:

- This exception applies ONLY when bootstrap window is TRUE.
- Once any submission directory exists for that day, behavior MUST revert to strict fail-closed.

## 3. Output (immutable truth)

V2 writes an immutable report artifact at:

- `constellation_2/runtime/truth/reports/capital_risk_envelope_v2/<OUT_DAY>/capital_risk_envelope.v2.json`

The artifact MUST conform to:

- `governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json`

## 4. Computation

Identical to v1 (strict mode):

- Use Decimal arithmetic.
- `BASE_ENVELOPE_PCT = 0.020000`
- `nav_total_cents = nav_total * 100`
- Compute drawdown multiplier per `drawdown_convention_v1.contract.md`.
- `allowed_capital_at_risk_cents = floor(nav_total_cents * BASE_ENVELOPE_PCT * m)`
- `portfolio_capital_at_risk_cents = sum(max_loss_cents for OPEN positions)`

Fail-closed conditions (strict mode) are unchanged:
- missing required inputs
- invalid schemas
- drawdown missing/null
- any OPEN position missing max_loss_cents

Day-0 bootstrap exception is defined in §2.2 and does not alter strict mode.

## 5. SAFE_IDLE rule (new)

If positions snapshot is present and schema-valid and:

- `positions.items` is an empty list

Then:

- `portfolio_capital_at_risk_cents = 0`
- `positions_all_have_max_loss = true`
- If drawdown present and allocation + nav present and schema-valid:
  - status MUST be PASS
  - headroom_cents = allowed_capital_at_risk_cents

## 6. Audit fields

Same as v1, with the addition:

- The report MUST record the v2 schema version and contract pointer in input_manifest.
- If Day-0 bootstrap exception is used, reason_codes MUST include the Day-0 reason code in §2.2.
