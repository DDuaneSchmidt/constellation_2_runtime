# Aegis Candidate Readiness Investigation - 2026-05-28

## Summary

Runtime truth for `PAPER-2026-05-28-0950` shows:

- Current-session candidates captured: 25
- Ready for operator review: 2
- Incomplete: 23
- Missing `planned_stop`: 23
- Missing `quantity`: 20

The incomplete rows are not fully constructed paper-trade candidates. They are current-session queue/lifecycle rows without matching constructed trade output.

## Source Artifacts Reviewed

- `/home/node/constellation_runtime_data/truth/reports/aegis_candidate_lifecycle_projection_v1/2026-05-28/candidate_lifecycle_projection.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/paper_trade_construction_v1/2026-05-28/paper_trade_construction.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_candidate_review_packet_v1/2026-05-28/candidate_review_packet.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_paper_review_queue_v1/2026-05-28/paper_review_queue.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_market_data_coverage_v1/2026-05-28/market_data_coverage.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_candidate_readiness_repair_v1/2026-05-28/candidate_readiness_repair.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_candidate_contracts_v1/2026-05-28/candidate_contracts.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_selected_intent_promotion_v1/2026-05-28/selected_intent_promotion.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/intent_arbitration_v1/2026-05-28/intent_arbitration.v1.json`

## Findings

### 1. Are raw signals being promoted too early?

Likely yes, at the queue/lifecycle display boundary.

Evidence:

- `aegis_candidate_contracts_v1` contains only 2 valid candidate contracts: `SPY` and `QQQ`.
- `paper_trade_construction_v1` contains only 2 constructed paper trades: `SPY` and `QQQ`.
- `aegis_candidate_review_packet_v1` contains only 2 review candidates.
- `aegis_paper_review_queue_v1` contains 25 current-session rows shown by lifecycle/Positions.
- The 23 incomplete lifecycle rows have `construction_present=false` and `active_review_present=false`.

Interpretation:

The UI-visible current-session list is broader than the canonical constructed/reviewable set. The queue includes current-day rows that are not present in the active review packet and not present in constructed trade output. They should remain visible as captured/raw/incomplete context, but they must not become operator-reviewable.

### 2. Is stop construction failing?

For the 23 incomplete rows, stop construction is not producing output.

Evidence:

- Missing `planned_stop`: 23 rows.
- `paper_trade_construction_v1.constructed_paper_trades` has only 2 rows.
- `paper_trade_construction_v1.skipped_candidates` has 0 rows.
- No constructed/skipped construction record exists for the 23 incomplete rows.

Interpretation:

This is not currently represented as explicit per-candidate stop-construction failure. The construction artifact simply omits those candidates instead of recording them as skipped/rejected with reasons.

### 3. Is quantity sizing failing?

For 20 of the 23 incomplete rows, quantity sizing is not producing output.

Evidence:

- Missing `quantity`: 20 rows.
- The 3 already-open legacy paper positions `AAL`, `AMD`, and `AMDL` have quantity from receipts/ledger but still lack planned stop.
- The remaining 20 incomplete candidate rows lack both `planned_stop` and `quantity`.

Interpretation:

Quantity sizing appears coupled to construction output. When construction does not emit a row, quantity is absent. Existing open position rows may have receipt quantity, so they are incomplete by readiness fields but are not pre-capture actionable candidates.

### 4. Are missing fields caused by market data gaps?

Market data gaps are a primary blocker for the incomplete set.

Evidence:

`aegis_market_data_coverage_v1` reports:

- Status: `BLOCKED`
- Missing symbols: 24
- Provider/cause: `YAHOO_CHART` / `PROVIDER_NO_DATA`
- Repair action: `TARGET_DAY=2026-05-28 npm run aegis:repair-input-contracts`

The missing symbols include the incomplete current-session symbols such as `AAL`, `AMD`, `AMDL`, `AMT`, `ARM`, `BANC`, `BDX`, `BKSY`, `BMY`, `BNS`, `BOXX`, `BTSG`, `BURL`, `CACC`, `CART`, `CAVA`, `CCL`, `CHTR`, `CIFR`, `CMCSA`, `COF`, `CRDO`, and `CSCO`.

`aegis_candidate_readiness_repair_v1` also reports a partial run and notes stale intent artifacts for the broad trend-equity symbol set.

### 5. Which source artifact is responsible?

There are two separate responsibilities:

- Display source responsible for exposing incomplete rows: `aegis_paper_review_queue_v1/2026-05-28/paper_review_queue.v1.json`
- Construction source responsible for missing readiness fields: `paper_trade_construction_v1/2026-05-28/paper_trade_construction.v1.json`

The queue has 25 current-session rows. The construction artifact has only 2 constructed rows and 0 skipped rows. The lifecycle projection correctly derives the incomplete state from that mismatch.

### 6. What code path should repair the missing fields?

The likely repair path is:

1. Repair market-data/input coverage:

```bash
TARGET_DAY=2026-05-28 npm run aegis:repair-input-contracts
```

2. Repair candidate readiness pipeline:

```bash
TARGET_DAY=2026-05-28 npm run aegis:repair-candidate-readiness
```

3. Re-run the paper open/construction path if needed:

```bash
TARGET_DAY=2026-05-28 npm run aegis:paper-open
```

Relevant producer code paths:

- `ops.aegis.human_reviewed_paper_mode_v1.build_candidate_review_packet_v1`
- `ops.aegis.human_reviewed_paper_mode_v1.build_paper_review_queue_v1`
- `ops.tools.run_aegis_paper_open_v1`
- `ops.aegis.trade_lifecycle.paper_trade_construction_v1.build_and_write_paper_trade_construction_v1`

## Current Safe Behavior

The current readiness gate prevents incomplete rows from being actionable:

- Incomplete pre-capture rows are marked `INCOMPLETE_CANDIDATE`.
- Incomplete rows expose Details only.
- Confirm Captured, Mark Not Captured, and Defer are not enabled for incomplete candidates.
- Current-session candidates remain visible throughout the session.

## Recommendation Before Construction Changes

Do not change construction behavior until the pipeline contract is clarified:

- `paper_trade_construction_v1` should either construct every reviewable candidate or emit an explicit skipped/rejected row for every candidate it declines to construct.
- `aegis_paper_review_queue_v1` should distinguish raw/captured queue rows from construction-ready review rows.
- Candidate readiness diagnostics should make missing stop construction, missing quantity sizing, and missing market data explicit per candidate.
