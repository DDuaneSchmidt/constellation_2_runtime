---
id: C2_CAPITAL_RISK_ENVELOPE_CONTRACT_V1
title: "Capital-at-Risk Envelope Contract V1"
status: CANONICAL
version: 1
created_utc: 2026-02-16
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
---

# Capital-at-Risk Envelope Contract V1

## 1. Objective

Define a single audit-proof, deterministic, fail-closed rule for **capital-at-risk** constraints
enforced **before PhaseD submission**.

This contract defines:

- how **portfolio capital-at-risk** is computed from positions truth,
- how the **allowed envelope** is computed from NAV and drawdown,
- explicit fail-closed conditions and required audit fields.

## 2. Inputs (authoritative truth)

Required day-scoped truth inputs:

1) Allocation summary truth:
- `constellation_2/runtime/truth/allocation_v1/summary/<DAY>/summary.json`

2) Positions snapshot truth:
- Prefer `positions_snapshot.v3.json` if present; else `positions_snapshot.v2.json`:
- `constellation_2/runtime/truth/positions_v1/snapshots/<DAY>/positions_snapshot.v3.json`

3) Accounting NAV truth:
- `constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json`

4) Drawdown convention contract (canonical):
- `governance/05_CONTRACTS/C2/drawdown_convention_v1.contract.md`

## 3. Definitions (units)

### 3.1 Currency units

- Position-level risk is expressed in **cents** via `max_loss_cents` (integer).
- NAV `nav_total` is expressed in whole **currency units** (integer, e.g., USD dollars).
- For envelope enforcement, NAV is converted to cents:
  - `nav_total_cents = nav_total * 100`

### 3.2 Position capital-at-risk

For each position in positions snapshot truth:

- A position contributes to capital-at-risk if and only if:
  - `status == "OPEN"` and
  - `max_loss_cents` is an integer `>= 0`

Then:

- `position_capital_at_risk_cents = max_loss_cents`

If any OPEN position has `max_loss_cents == null` or missing:
- enforcement MUST FAIL-CLOSED (cannot compute risk).

## 4. Portfolio capital-at-risk (deterministic)

- `portfolio_capital_at_risk_cents = sum(position_capital_at_risk_cents for all OPEN positions)`

Order of iteration must not matter:
- Inputs must be processed in a deterministic order (sort by `position_id`).

## 5. Allowed envelope (drawdown-scaled)

### 5.1 Base budget (canonical constant)

Define a single base envelope fraction of NAV:

- `BASE_ENVELOPE_PCT = 0.020000` (2.0000% of NAV)

This is the maximum allowed portfolio capital-at-risk when `drawdown_pct >= 0.000000` (multiplier 1.00).

### 5.2 Drawdown multiplier

Apply the canonical drawdown multiplier table from:
- `drawdown_convention_v1.contract.md`

Let `m` be the multiplier in {1.00, 0.75, 0.50, 0.25} computed using contract rules.

### 5.3 Allowed envelope formula

Use Decimal arithmetic (no floats):

- `allowed_capital_at_risk_cents = floor(nav_total_cents * BASE_ENVELOPE_PCT * m)`

Where:
- `nav_total_cents` is integer,
- `BASE_ENVELOPE_PCT` is Decimal quantized to 6 dp,
- `m` is Decimal quantized to 2 dp.

If drawdown is missing/null at enforcement time:
- enforcement MUST FAIL-CLOSED.

## 6. Gate decision

The gate MUST produce an immutable report artifact and then:

- PASS if `portfolio_capital_at_risk_cents <= allowed_capital_at_risk_cents`
- FAIL otherwise

Additionally FAIL-CLOSED if any required input is missing or invalid (including unknown units).

## 7. Required audit fields

Any artifact claiming this enforcement MUST include:

- exact input pointers + sha256 for:
  - allocation summary, positions snapshot, nav, drawdown contract
- `nav_total`, `nav_total_cents`
- `peak_nav`, `drawdown_abs`, `drawdown_pct` (as stored in nav history)
- multiplier table used (exact ordering + numeric values)
- `BASE_ENVELOPE_PCT`
- `allowed_capital_at_risk_cents`
- `portfolio_capital_at_risk_cents`
- per-position breakdown (position_id, engine_id, market_exposure_type, max_loss_cents, included)
