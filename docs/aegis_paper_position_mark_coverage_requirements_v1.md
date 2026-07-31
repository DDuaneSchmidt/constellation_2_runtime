# Aegis Paper Position Mark Coverage Requirements v1

## Intent

Repair paper-position mark coverage so open paper positions cannot appear fully covered when their symbols are absent from current market data.

## Authoritative Position Universe

For paper-position mark coverage, the authoritative universe is `aegis_paper_position_ledger_v1.open_positions[].symbol`.

Do not use only broad/context market data, stale coverage artifacts, candidate-only universes, or static index/ETF universes as the position mark universe.

## Required Behavior

- Market data demand includes every open paper position symbol plus existing broad/context symbols.
- Market data coverage reads the real day-scoped paper position ledger.
- If the ledger is missing or stale, coverage status is `INVALID` and includes `STALE_OR_MISSING_LEDGER_INPUT`.
- Missing or stale marks do not pass as 100% coverage.
- Every open position receives a mark status: `MARK_AVAILABLE`, `MISSING_MARK`, `MISSING_MARK_TIMESTAMP`, `SYMBOL_NOT_REQUESTED`, `SYMBOL_NOT_IN_MARKET_DATA`, `MARK_CERTIFICATION_FAILED`, `STALE_MARK`, or `POSITION_EXCLUDED_WITH_REASON`.
- Evidence lineage continues to fail closed when marks are incomplete.

## Safety

This package is read-only for market data demand, coverage, mark certification, evidence lineage, and repair reporting. It does not authorize broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, strategy changes, forced exits, or validation-rule changes.
