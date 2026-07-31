# Aegis Entry Reference Price Certification Repair v1

## 1. Current rejected candidate count

Target reproduction day: `2026-06-01`.

`TARGET_DAY=2026-06-01 npm run aegis:candidate-diagnostics` currently reports:

- `total_raw_signals = 20`
- `valid_candidate_contracts = 0`
- `rejected_candidate_contracts = 20`
- all 20 rejected candidate rows have high-level reason `ENTRY_REFERENCE_PRICE_UNCERTIFIED`

## 2. Sleeves affected

- `C2_TREND_EQ_PRIMARY_V1`: 18 rejected raw signals
- `C2_VOL_INCOME_DEFINED_RISK_V1`: 1 rejected raw signal
- `C2_CROSS_ASSET_TREND_V1`: 1 rejected raw signal

## 3. Symbols affected

Affected symbols are: `AAL`, `AAPL`, `AMD`, `AMDL`, `AMT`, `APTV`, `ARM`, `BAC`, `BANC`, `BKSY`, `BNS`, `BOXX`, `BTSG`, `BURL`, `CACC`, `CCL`, `CRDO`, `CSCO`, `GLD`, `QQQ`.

## 4. Source of entry reference price

Current valid price evidence exists in `market_data_inputs_v1` and current market rows exist in `aegis_market_data_v1`. Examples from 2026-06-01:

- `market.price.BTSG` has a valid `market_data_inputs_v1` row with value, symbol, source vendor, timestamp, and target day.
- `market.price.GLD` has a valid `market_data_inputs_v1` row with value, symbol, source vendor, timestamp, and target day.
- `market.price.QQQ` has a valid `market_data_inputs_v1` row with value, symbol, source vendor, timestamp, and target day.

## 5. Why certification failed

The rejection is not caused by missing market prices. Prices are fetched and valid in `market_data_inputs_v1`.

Root cause: `signal_evidence_graph_v1` certifies the entry reference price using `data_registry_v1.market_data_validation_status`, but current `data_registry_v1` price rows have `market_data_validation_status = null` and `value = null`. The corresponding `market_data_inputs_v1` rows are valid and contain values. Because no canonical entry-reference certification artifact bridges that valid input evidence into candidate-contract gating, the graph marks price evidence as `EVIDENCE_NOT_CERTIFIED` and candidate contracts reject it.

## 6. Required certification evidence

Every entry reference price certification row must include:

- `symbol`
- `normalized_symbol`
- `price`
- `price_timestamp`
- `market_session`
- `source`
- `source_artifact`
- `source_hash`
- `certification_status`
- `certification_reason_codes`
- `freshness_window_seconds`
- `generated_at`
- `day_utc`

Allowed statuses:

- `CERTIFIED`
- `UNCERTIFIED_MISSING_PRICE`
- `UNCERTIFIED_MISSING_SOURCE`
- `UNCERTIFIED_STALE_PRICE`
- `UNCERTIFIED_BAD_SYMBOL`
- `UNCERTIFIED_BAD_SCHEMA`
- `UNCERTIFIED_SOURCE_NOT_ALLOWED`
- `UNCERTIFIED_MARKET_CLOSED`
- `UNCERTIFIED_UNKNOWN`

## 7. Repair plan

1. Add `aegis_entry_reference_price_certification_v1` as a deterministic artifact built from current raw signals plus `market_data_inputs_v1`/market-data artifacts.
2. Add a tool wrapper and npm command.
3. Update `signal_evidence_graph_v1` to consume the certification artifact for entry reference price evidence before candidate-contract gating.
4. Update candidate contract rejection rows with detailed certification status/reason codes.
5. Add self-checks that enforce no candidate contract passes with uncertified entry price evidence.
6. Wire certification production before `signal-evidence-graph`/`candidate-contracts` in the input-contract repair path and candidate diagnostics path.

## 8. Safety constraints

- Do not invent prices.
- Do not use UI-only values.
- Do not weaken stale price, bad symbol, missing source, or source artifact checks.
- Do not force candidates.
- Do not enable trade advice, manual capture, broker execution, or autonomous execution.
- Candidate contracts may pass only when entry reference price status is `CERTIFIED`.
