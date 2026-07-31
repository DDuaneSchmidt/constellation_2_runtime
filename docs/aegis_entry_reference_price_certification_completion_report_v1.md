# Aegis Entry Reference Price Certification Completion Report V1

## 1. Root Cause

For TARGET_DAY=2026-06-01, raw signals had current price evidence available through `market_data_inputs_v1`, but the candidate gate only saw the older signal evidence graph certification path. That path required data-registry validation fields that were absent on the current price rows, so valid market input prices were classified as `ENTRY_REFERENCE_PRICE_UNCERTIFIED`.

This was a producer-ordering/evidence-contract gap, not a reason to weaken the price gate.

## 2. Files Changed

- `ops/aegis/entry_reference_price_certification_v1.py`
- `ops/tools/build_aegis_entry_reference_price_certification_v1.py`
- `ops/aegis/entry_reference_price_certification_self_check_v1.py`
- `ops/tools/run_aegis_entry_reference_price_certification_self_check_v1.py`
- `ops/aegis/signal_evidence_graph_v1.py`
- `ops/aegis/candidate_contracts_v1.py`
- `ops/aegis/candidate_generation_diagnostics_v1.py`
- `ops/aegis/real_signal_death_report_v1.py`
- `ops/aegis/candidate_generation_visibility_v1.py`
- `ops/tools/write_aegis_signal_evidence_graph_v1.py`
- `ops/tools/write_aegis_candidate_contracts_v1.py`
- `ops/tools/write_aegis_candidate_generation_diagnostics_v1.py`
- `ops/tools/repair_aegis_input_contracts_v1.py`
- `constellation_2/common/tests/test_candidate_contracts_v1.py`
- `aegis/modules/operator_portal/aegis.module.yaml`
- `package.json`

## 3. Certification Contract

New artifact:

`reports/aegis_entry_reference_price_certification_v1/<day_utc>/entry_reference_price_certification.v1.json`

Each row includes symbol, normalized symbol, price, price timestamp, market session, source, source artifact, source hash, certification status, reason codes, freshness window, generated timestamp, and day.

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

Candidate contracts still pass only when entry price certification status is `CERTIFIED`.

## 4. Before / After Rejection Counts

Before repair on TARGET_DAY=2026-06-01:

- raw signals: 20
- valid candidate contracts: 0
- rejected candidate contracts: 20
- dominant rejection: `ENTRY_REFERENCE_PRICE_UNCERTIFIED`

After repair and rerun on TARGET_DAY=2026-06-01:

- raw signals: 21
- certified entry reference prices: 21
- uncertified entry reference prices: 0
- valid candidate contracts: 19
- rejected candidate contracts: 2
- remaining contract rejection: `INSTRUMENT_TYPE_NOT_GOVERNED`

The raw-signal count changed from 20 to 21 because the full repaired target-day pipeline regenerated sleeve evaluation and produced one additional mean-reversion raw signal.

## 5. Remaining Rejection Reasons

The remaining rejected candidate contracts are not price-certification failures:

- `C2_MEAN_REVERSION_EQ_V1` / `IMO`: `INSTRUMENT_TYPE_NOT_GOVERNED`, entry price `CERTIFIED`
- `C2_VOL_INCOME_DEFINED_RISK_V1` / `GLD`: `INSTRUMENT_TYPE_NOT_GOVERNED`, entry price `CERTIFIED`

Sleeve-level candidate generation after repair:

- `C2_CROSS_ASSET_TREND_V1`: 1 raw signal, 1 candidate contract
- `C2_TREND_EQ_PRIMARY_V1`: 18 raw signals, 18 candidate contracts
- `C2_MEAN_REVERSION_EQ_V1`: 1 raw signal, 0 candidate contracts, governed instrument rejection
- `C2_VOL_INCOME_DEFINED_RISK_V1`: 1 raw signal, 0 candidate contracts, governed instrument rejection
- `C2_DEFENSIVE_TAIL_V1`: no setup
- `C2_EVENT_DISLOCATION_V1`: no setup
- `C2_MARKET_NEUTRAL_SPREAD_V1`: no setup

## 6. Tests Run

- `python3 -m py_compile` on touched Python files
- `python3 -m pytest constellation_2/common/tests/test_candidate_contracts_v1.py constellation_2/common/tests/test_signal_evidence_graph_v1.py`
- `python3 -m pytest constellation_2/common/tests/test_candidate_generation_visibility_v1.py constellation_2/common/tests/test_candidate_contracts_v1.py`
- `TARGET_DAY=2026-06-01 npm run aegis:repair-input-contracts`
- `TARGET_DAY=2026-06-01 npm run aegis:entry-reference-price-certification`
- `TARGET_DAY=2026-06-01 npm run aegis:entry-reference-price-certification-self-check`
- `TARGET_DAY=2026-06-01 npm run aegis:candidate-diagnostics`
- `npm run aegis:operator-action-model`

Final audit was run after the target-day repair sequence and passed verified graph strict mode with `audit_blocker_count=0`.

## 7. Audit Result

`npm run aegis:audit` passed during the repaired target-day sequence. Verified runtime graph strict mode reported:

- `graph_status`: `READY`
- `audit_blocker_count`: `0`

## 8. Safety Confirmation

The repair does not enable or imply:

- trade advice
- manual trade capture
- broker execution
- autonomous execution
- automatic candidate approval
- weakened stale-price checks
- weakened invalid-symbol checks
- synthetic or UI-only pricing

Artifacts continue to emit explicit safety fields with trading/execution permissions false.

## 9. Recommended Next Blocker

The next blocker is no longer entry reference price certification. The next candidate-contract issue is instrument governance for non-long-equity implementations, currently visible as `INSTRUMENT_TYPE_NOT_GOVERNED` for `C2_MEAN_REVERSION_EQ_V1` (`IMO`) and `C2_VOL_INCOME_DEFINED_RISK_V1` (`GLD`).
