# Aegis Paper Position Mark Coverage Spec v1

## Artifacts

- `aegis_market_data_demand_v1`: includes open paper position symbols as required market data demand when the paper ledger exists.
- `aegis_market_data_coverage_v1`: computes position mark coverage from the current paper position ledger.
- `aegis_mark_coverage_report_v1`: summarizes marked versus unmarked open positions.
- `aegis_evidence_lineage_integrity_v1`: fails closed when mark coverage is below policy threshold.
- `aegis_paper_position_mark_coverage_repair_v1`: read-only investigation and repair summary artifact.

## Coverage Rules

`aegis_market_data_coverage_v1.open_position_count` must equal the count of `aegis_paper_position_ledger_v1.open_positions` when the ledger is available for the target day.

If the ledger is unavailable or stale for the target day:

- `status`: `INVALID`
- `coverage_status`: `INVALID`
- `invalid_reason_codes`: includes `STALE_OR_MISSING_LEDGER_INPUT`
- `mark_coverage_by_position_pct`: `0.0`

## Per-Position Mark Status

Each open paper position is classified as:

- `MARK_AVAILABLE`: current session price, timestamp, and certification checks pass.
- `MISSING_MARK`: no usable price value.
- `MISSING_MARK_TIMESTAMP`: price exists without a usable timestamp.
- `SYMBOL_NOT_REQUESTED`: open position symbol was not in demand/requested symbols.
- `SYMBOL_NOT_IN_MARKET_DATA`: requested symbol was absent from canonical market data.
- `MARK_CERTIFICATION_FAILED`: certification predicate failed after other checks.
- `STALE_MARK`: mark is stale or for the wrong session.
- `POSITION_EXCLUDED_WITH_REASON`: position is excluded only with an explicit reason code.

## Success Criteria

For a target day, the repair is successful when demand includes all open position symbols, coverage reports the true open position count, missing symbols are listed with reason codes, and evidence lineage either passes with full marks or fails closed with legitimate blockers.
