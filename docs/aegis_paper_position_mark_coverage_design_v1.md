# Aegis Paper Position Mark Coverage Design v1

## Design

The repair separates three universes:

- Position universe: `aegis_paper_position_ledger_v1.open_positions[].symbol`.
- Demand universe: open position symbols plus governed research/context symbols.
- Returned market data universe: symbols present in `aegis_market_data_v1.symbols`.

Coverage is computed from the position universe outward. This prevents broad/context-only market data from creating a false green state for paper positions.

## Flow

1. Build or read the paper position ledger.
2. Build market data demand from candidate contracts, sleeve contracts, symbol map, and open paper position symbols.
3. Refresh market data for the demand universe.
4. Build market data coverage from the current ledger and returned market data.
5. Build mark coverage and evidence lineage.
6. Emit `aegis_paper_position_mark_coverage_repair_v1` with the investigation fields.

## Failure Semantics

Missing ledger input invalidates coverage. Missing marks block mark coverage. Evidence lineage retains the existing fail-closed integrity threshold.

## Safety

All artifacts are read-only and preserve disabled broker execution, live trading, trade advice, autonomous execution, real-capital allocation, order management, and safety gates.
