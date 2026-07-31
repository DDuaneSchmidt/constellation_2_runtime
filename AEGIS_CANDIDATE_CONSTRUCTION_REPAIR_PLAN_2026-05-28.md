# Aegis Candidate Construction Repair Plan - 2026-05-28

## Runtime Facts

For `PAPER-2026-05-28-0950`:

- Captured candidates: 25
- Ready for operator review: 2
- Incomplete / construction blocked: 23
- Constructed in `paper_trade_construction_v1`: `SPY`, `QQQ`
- Missing `planned_stop`: 23
- Missing `quantity`: 20
- Market-data coverage blocked symbols: 24
- Coverage issue group: `YAHOO_CHART / PROVIDER_NO_DATA`

## Why did YAHOO_CHART return PROVIDER_NO_DATA for 24 symbols?

The label is misleading if read literally.

Runtime evidence shows Yahoo raw chart requests for blocked symbols such as `AMT` succeeded and produced current intraday chart payloads. Example `AMT` raw data has:

- `chart.error`: null
- `result_len`: 1
- `regularMarketPrice`: 188.04
- `timestamp_len`: 322
- `dataGranularity`: 1m
- `range`: 1d

`aegis_market_data_coverage_v1` still marks `AMT` as missing because:

- `market_data_status`: CURRENT
- `data_registry_status`: CURRENT
- `market_data_inputs_status`: MISSING
- `provider_attempt`: SUCCESS / normalized_status CERTIFIED

So the practical failure is not Yahoo returning no raw data. The failure is that current Yahoo raw/provider data was not bound into `market_data_inputs_v1` for the 24 candidate-contract symbols.

## Is the provider symbol format wrong?

Probably not for the observed symbols.

Evidence:

- Yahoo chart raw files exist for symbols such as `AAL`, `AMD`, `AMT`, `ARM`, `BANC`, `CCL`, `CSCO`.
- The raw `AMT` payload uses provider symbol `AMT` and contains valid NYSE metadata and price data.
- The coverage rows show provider attempts with `accepted_reason: Accepted Yahoo chart current-session intraday snapshot`.

Repair should focus on binding accepted provider output into market-data inputs, not symbol aliasing, unless a per-symbol raw file is missing.

## Is the provider request window wrong?

Probably not for the successful raw chart fetches.

Evidence:

- Raw Yahoo chart data uses `range: 1d`, `dataGranularity: 1m`.
- Current-session timestamps are present.
- Market session date is `2026-05-28` in coverage rows.
- Provider attempts were accepted as current-session intraday snapshots.

The request window appears sufficient to fetch prices. The failed stage is downstream input normalization/binding.

## Is market data stale or missing?

For the incomplete candidate symbols, market data is missing at `market_data_inputs_v1`, not necessarily missing at raw provider output.

Evidence:

- `market_data_inputs_v1` has only 11 input records.
- `SPY` has a valid `market.price.SPY` input record.
- `AMT` has raw Yahoo data and current registry status, but no `market.price.AMT` input record.
- `aegis_market_data_coverage_v1` therefore reports the symbol as missing.

## Are candidates being generated before market data is available?

The broader 25-candidate queue appears to include rows beyond the constructed/reviewable set.

Evidence:

- `aegis_candidate_contracts_v1` contains only 2 valid candidate contracts: `SPY`, `QQQ`.
- `aegis_candidate_review_packet_v1` contains only 2 review candidates.
- `paper_trade_construction_v1` contains only 2 constructed paper trades.
- `aegis_paper_review_queue_v1` contains 25 current-session rows.
- The extra 23 rows have no matching construction record and no active review packet row.

This indicates the queue/lifecycle layer is carrying raw or historical/current-session rows that did not pass the construction path.

## Is stop construction dependent on unavailable intraday data?

Yes, by current artifact behavior.

The 23 incomplete rows lack constructed trade rows, so no stop construction output exists for them. The most direct upstream blocker is market-data input binding: without valid input records, construction cannot reliably derive stops.

The construction artifact should explicitly record this as skipped/blocked per candidate instead of omitting those rows.

## Is quantity sizing skipped because stop is missing?

Likely yes.

Evidence:

- 20 rows are missing both `planned_stop` and `quantity`.
- 3 existing open rows have quantity from receipt/position lineage but still lack planned stop.
- Quantity sizing typically depends on risk per share, which depends on entry minus stop.

Repair stop construction first; quantity sizing should follow once stop/risk-per-share exists.

## Should incomplete candidates remain visible or move to diagnostics?

They should remain visible in Today’s Candidates but not actionable.

Required behavior:

- Keep all 25 current-session candidates visible.
- Show incomplete rows as `INCOMPLETE_CANDIDATE` or `CONSTRUCTION_BLOCKED`.
- Show missing fields and plain-English reason.
- Show Details only.
- Move raw construction diagnostics, provider internals, and source-path detail to `/aegis-positions-diagnostics` or a candidate construction diagnostics surface.

## Repair Plan

1. Normalize construction status reporting.

   `paper_trade_construction_v1` should emit one row per captured/current-session candidate, either constructed or blocked/skipped. Omitted candidates make the UI and diagnostics ambiguous.

2. Repair market-data input binding for candidate-contract symbols.

   The current blocker is not raw Yahoo availability for at least observed symbols. It is missing `market_data_inputs_v1` records for the 24 candidate-contract symbols.

   Candidate command:

   ```bash
   TARGET_DAY=2026-05-28 npm run aegis:repair-input-contracts
   ```

3. Rebuild candidate readiness repair.

   ```bash
   TARGET_DAY=2026-05-28 npm run aegis:repair-candidate-readiness
   ```

4. Re-run construction/open path only after input records exist.

   ```bash
   TARGET_DAY=2026-05-28 npm run aegis:paper-open
   ```

5. Add or wire the construction status report.

   Required artifact:

   ```text
   truth/reports/aegis_candidate_construction_report_v1/<day>/candidate_construction_report.v1.json
   ```

6. Add or wire the construction status command.

   Required command:

   ```bash
   npm run aegis:candidate-construction-status
   ```

7. Block operator review until construction readiness passes.

   The Positions page should continue to show `25 captured`, `2 ready for review`, and `23 construction blocked`, with Details-only controls for blocked candidates.

## Most Likely Code Paths

- Raw/candidate capture and queue: `ops.aegis.human_reviewed_paper_mode_v1.build_paper_review_queue_v1`
- Market data coverage: `ops.aegis.market_data_coverage_v1.build_market_data_coverage_v1`
- Market data input binding: `ops/tools/build_aegis_market_data_inputs_v1.py`
- Construction: `ops.aegis.trade_lifecycle.paper_trade_construction_v1.build_and_write_paper_trade_construction_v1`
- Paper open orchestration: `ops/tools/run_aegis_paper_open_v1.py`

## Safety Boundary

No construction behavior was changed as part of this report. Broker submit/transmit, live trading, autonomous execution, and trade advice remain disabled by design.
