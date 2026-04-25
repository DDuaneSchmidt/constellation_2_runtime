---
id: C2_PAPER_CAPITAL_SEED_V1
title: "C2 Paper Capital Seed Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-14
owner: Constellation
authority: governance+git
scope: constellation_2_paper_capital_seed
---

# C2 Paper Capital Seed Contract (V1)

## Purpose

This contract defines the governed paper-capital seed input used to materialize
future-day paper operator statements without manual artifact edits.

It exists so paper NAV bootstrap capital is explicit, auditable, deterministic,
and not limited to a hardcoded 100k seed.

## Truth owner

- Truth owner: Constellation governance
- Governing policy:
  - `governance/02_REGISTRIES/C2_PAPER_CAPITAL_SEED_POLICY_V1.json`
- Canonical seed input writer:
  - `ops/tools/ensure_paper_capital_seed_v1.py`
- Canonical seed input path:
  - `constellation_2/operator_inputs/paper_capital_seed_v1/<DAY>/paper_capital_seed.v1.json`
- Canonical operator statement writer:
  - `ops/tools/ensure_cash_ledger_operator_statement_v1.py`
- Canonical operator statement path:
  - `constellation_2/operator_inputs/cash_ledger_operator_statements/<DAY>/operator_statement.v1.json`

## Required meaning

The seed input must identify:

- `day_utc`
- `environment`
- `ib_account`
- `currency`
- `seed_mode`
- `cash_total`
- `nlv_total`
- deterministic provenance back to the governing policy file
- stable explanatory `notes[]`

`cash_total` and `nlv_total` are decimal USD strings with two fractional digits.

## Allowed modes

- `EXPLICIT_USD`

Legacy operator-statement bootstrap modes remain supported separately:

- `ZERO`
- `SEED_100K`

## Fail-closed rules

- Missing paper-capital-seed input must fail closed when operator-statement generation requests governed seed mode.
- Invalid day integrity must fail closed.
- Invalid decimal encoding, negative values, out-of-range values, currency mismatch, or mismatched `cash_total`/`nlv_total` must fail closed.
- Existing day-keyed seed inputs and operator statements must not be overwritten by the canonical writers.
- Same-day no-overwrite behavior must remain in force for already-materialized operator statements.

## Determinism requirements

- Seed input and operator statement generation must use deterministic JSON serialization.
- `observed_at_utc` for the derived operator statement must be `YYYY-MM-DDT00:00:00Z`.
- Given the same governing policy and same seed input, operator statement bytes must be reproducible.

## Generation flow

1. `ensure_paper_capital_seed_v1.py` writes the governed day-scoped seed input.
2. `ensure_cash_ledger_operator_statement_v1.py --mode GOVERNED_SEED` reads that seed input and materializes the operator statement.
3. `run_cash_ledger_snapshot_day_v1` reads the operator statement and produces the cash-ledger snapshot.
4. `run_accounting_nav_v2_day_v1` derives NAV from the cash-ledger snapshot.
5. `run_c2_capital_risk_envelope_gate_v2` derives the allowed capital-at-risk envelope from NAV.

## What must not change

- No manual edits to runtime truth artifacts.
- No bypass of cash-ledger, NAV, or capital-risk-envelope logic.
- No silent overwrite of existing day-keyed operator statements.
- Legacy `ZERO` and `SEED_100K` behavior must remain supported for compatibility.
