# Aegis Candidate Generation Blocker Repair - 2026-06-01

## 1. Original blocker table

Target run: `TARGET_DAY=2026-06-01 npm run aegis:candidate-diagnostics`.

Initial reproduction blocked before diagnostics because sleeve input contracts were stale against the current market-data hash:

| Sleeve | Original blocker | Initial evidence |
| --- | --- | --- |
| C2_TREND_EQ_PRIMARY_V1 | ALLOWED_SYMBOL_MISMATCH | Prior/current artifacts showed the trend sleeve using a deprecated registry symbol basis (`SPY`) while governed dynamic universe resolution expected a broad canonical universe. |
| C2_VOL_INCOME_DEFINED_RISK_V1 | SLEEVE_INPUT_REQUIREMENT_BLOCKED / market.volatility.VIX | Prior/current artifacts showed `market.volatility.VIX` missing/stale or contract lineage stale before running `aegis:repair-input-contracts`. |

The first rerun produced `STALE_SLEEVE_INPUT_CONTRACT_MARKET_DATA_HASH`, so repair was required before diagnosing sleeve behavior.

## 2. Root cause for C2_TREND_EQ_PRIMARY_V1

The allowed-symbol gate itself was not the problem. It correctly rejects output symbols outside the canonical universe.

Root cause was a demand/contract basis mismatch:

- `aegis_sleeve_input_contracts_v1` resolved `C2_TREND_EQ_PRIMARY_V1` from the canonical dynamic universe and required current market data for the first 100 governed symbols.
- `aegis_symbol_map_v1` and market-data demand were still seeded from deprecated `ENGINE_MODEL_REGISTRY_V1.allowed_symbols` plus static sleeve context, so the repair pipeline did not request the same dynamic symbol set required by the sleeve contract.
- That produced false input blockage and earlier allowed-symbol mismatch symptoms.

Repair: market-data demand now uses the canonical sleeve universe resolver for enabled sleeves when a target day/truth root is available, caps dynamic universes to their governed target count, and preserves registry fallback only for ungoverned or unresolved sleeves.

## 3. Root cause for C2_VOL_INCOME_DEFINED_RISK_V1

`market.volatility.VIX` was a stale/missing input-contract artifact condition, not a missing architecture issue.

After `TARGET_DAY=2026-06-01 npm run aegis:repair-input-contracts`:

- `market.volatility.VIX` is present and current in `market_data_inputs_v1`.
- VIX is non-blocking under the active `HUMAN_REVIEWED_PAPER_MODE` context profile.
- The sleeve readiness row is `READY`, with no `blocking_inputs` or `warning_inputs`.

## 4. Repairs made

Files changed:

- `ops/aegis/market_data/symbol_map_v1.py`
- `constellation_2/common/tests/test_aegis_data_registry_sleeve_readiness_v1.py`
- `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`
- `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`
- `constellation_2/phaseL/ui/tests/test_aegis_operator_action_model_ui_v1.py`
- `aegis/modules/operator_portal/aegis.module.yaml`
- `docs/aegis_candidate_generation_blocker_repair_2026_06_01.md`

Implementation summary:

- Added canonical sleeve symbol resolution to runtime symbol-map/market-data demand.
- Added per-sleeve symbol resolution metadata to the symbol-map artifact.
- Preserved strict allowed-symbol validation; invalid symbols still block.
- Kept VIX governed by the active context requirement profile; no VIX gate was weakened.
- Updated Command Center candidate-generation labels to distinguish `NO_SETUP`, `REJECTED_SIGNALS`, `BLOCKED_DATA`, `BLOCKED_CONFIG`, and `VALID_CONTRACT_CREATED`.

## 5. Tests added/updated

Added focused coverage for canonical dynamic sleeve universe demand:

- `test_symbol_map_uses_canonical_dynamic_sleeve_universe_for_market_data_demand`

Relevant checks run:

- `python3 -m py_compile ops/aegis/market_data/symbol_map_v1.py constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`
- `python3 -m pytest constellation_2/common/tests/test_aegis_data_registry_sleeve_readiness_v1.py::test_symbol_map_covers_all_sleeve_required_symbols constellation_2/common/tests/test_aegis_data_registry_sleeve_readiness_v1.py::test_symbol_map_uses_canonical_dynamic_sleeve_universe_for_market_data_demand constellation_2/common/tests/test_aegis_data_registry_sleeve_readiness_v1.py::test_runtime_universe_includes_real_raw_signal_symbols constellation_2/phaseL/ui/tests/test_aegis_operator_action_model_ui_v1.py -q`
- `python3 -m pytest constellation_2/common/tests/test_aegis_market_data_inputs_v1.py::test_valid_vix_clears_volatility_sleeve_readiness constellation_2/common/tests/test_aegis_market_data_inputs_v1.py::test_required_missing_input_still_blocks constellation_2/common/tests/test_aegis_market_data_inputs_v1.py::test_market_data_inputs_missing_vix_is_missing -q`
- `python3 -m pytest constellation_2/common/tests/test_aegis_candidate_generation_diagnostics_v1.py::test_candidate_diagnostics_reports_canonical_vix_missing_explanation -q`
- `python3 -m pytest constellation_2/common/tests/test_allowed_symbol_source_authority_v1.py::test_unexpected_symbol_in_output_blocks_against_canonical_universe -q`

Note: the full `test_allowed_symbol_source_authority_v1.py` file has two pre-existing/deprecated-fallback expectation failures unrelated to this repair; the strict invalid-symbol gate test passes.

## 6. Before/after diagnostics

Before repair:

- `TARGET_DAY=2026-06-01 npm run aegis:candidate-diagnostics` exited with `STALE_SLEEVE_INPUT_CONTRACT_MARKET_DATA_HASH`.
- After first contract repair but before this code repair, diagnostics were `PARTIAL_RUN`: 4 of 7 sleeves ran; 3 were blocked by sleeve input requirements.
- `C2_TREND_EQ_PRIMARY_V1` was blocked by missing required market inputs from the canonical dynamic universe.
- `C2_VOL_INCOME_DEFINED_RISK_V1` had stale/missing VIX evidence before regeneration.

After repair and regeneration:

- `aegis_symbol_map_v1`: `required_symbol_count=206`, `canonical_sleeve_symbol_resolution_used=true`, `requested_symbols_source=canonical_sleeve_required_symbols+real_raw_signal_registry`.
- `aegis_market_data_coverage_v1`: `status=READY`, `coverage_pct=100.0`, `required_symbol_count=205`, `certified_symbol_count=205`.
- `aegis_sleeve_readiness_v1`: `ready=7`, `blocked=0`.
- `aegis_candidate_generation_diagnostics_v1`: `candidate_generation_status=RAN`, `operator_interpretation=NORMAL_NO_SIGNAL`, `total_sleeves_run=7`, `total_raw_signals=20`, `valid_candidate_contracts=0`, `rejected_candidate_contracts=20`.

Target sleeve after-state:

| Sleeve | Run state | Raw signals | Candidate contracts | Current reason |
| --- | ---: | ---: | ---: | --- |
| C2_TREND_EQ_PRIMARY_V1 | RAN / READY | 18 | 0 valid, 18 rejected | Signals reached candidate-contract gates and were rejected; no ALLOWED_SYMBOL_MISMATCH or input blockage remains. Candidate-contract rejection reason: `ENTRY_REFERENCE_PRICE_UNCERTIFIED`. |
| C2_VOL_INCOME_DEFINED_RISK_V1 | RAN / READY | 1 | 0 valid, 1 rejected | VIX is current and non-blocking; signal reached promotion/contract gates and was rejected, not blocked by VIX. |

## 7. Remaining candidate blockers, if any

No sleeve remains blocked from running candidate generation for the target day. The remaining zero-candidate result is a gate outcome:

- 20 raw signals existed.
- 0 valid candidate contracts were created.
- 20 candidate contracts were rejected.
- Rejections include `ENTRY_REFERENCE_PRICE_UNCERTIFIED`, confirming uncertified entry price gates still reject candidates.

The input-contract reconciliation artifact still reports the optional simulator row as missing a contract, but it is marked `ACTIVE_SIMULATOR_NOT_PRESTART_REQUIRED,OPTIONAL_SIMULATION` and `blocking_current_day_valid_candidates=false`.

## 8. Safety confirmation

Confirmed unchanged:

- `trade_advice_allowed=false`
- `broker_execution_allowed=false`
- `autonomous_execution_allowed=false`
- No broker execution was added.
- No manual capture was enabled.
- No candidate was forced.
- No stale-market-data, invalid-symbol, or uncertified-entry-price gate was weakened.
