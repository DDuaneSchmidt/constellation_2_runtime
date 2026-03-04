---
id: C2_ACCOUNTING_NAV_V2_CONDITIONAL_INPUTS_V1
title: "Constellation 2.0 — Accounting NAV v2 Conditional Inputs Policy V1"
status: DRAFT
version: 1
created_utc: 2026-03-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - nav
  - accounting_v2
  - conditional_inputs
  - broker_marks
  - positions
  - cash_ledger
  - fail_closed
---

# Accounting NAV v2 Conditional Inputs Policy V1

## Objective
Define strict, deterministic rules for what inputs are required to compute NAV v2.

## Output path
`constellation_2/runtime/truth/accounting_v2/nav/<day_utc>/nav.v2.json`

## Required inputs (always)
NAV v2 MUST require:
- `cash_ledger_v1/snapshots/<day_utc>/cash_ledger_snapshot.v1.json`
- `positions_v1/snapshots/<day_utc>/positions_snapshot.v2.json`

If either required input is missing:
- FAIL closed for normal operation
- BOOTSTRAP is permitted only for explicitly governed genesis/anchor days

## Conditional input: broker marks
Broker marks are REQUIRED only when positions exist.

Broker marks path:
`market_data_snapshot_v1/broker_marks_v1/<day_utc>/broker_marks.v1.json`

Policy:
- If positions list is empty: broker marks MUST be treated as optional and NAV MUST be computed using cash only (gross_positions_value=0).
- If positions list is non-empty: broker marks MUST be required. Missing marks MUST fail closed (or bootstrap only for explicit genesis/anchor days).

## Deterministic semantics
NAV definition:
- nav_total = cash_total + gross_positions_value
- gross_positions_value = 0 when no positions exist

## Prohibited behavior
- Treating broker marks as required when there are zero positions is prohibited.
- Producing BOOTSTRAP NAV for normal funded baseline days is prohibited.

## Evidence requirements
NAV v2 MUST include:
- input_manifest entries referencing required inputs and sha256
- conditional marks input manifest when marks are present or required
