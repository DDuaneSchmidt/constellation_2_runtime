# Aegis Entry Reference Price Certification Consumption Completion Report v1

Completion date: 2026-06-01

## 1. Root Cause

The prior candidate-promotion bottleneck was caused by certified current price evidence not being generated and consumed deterministically before candidate-contract gating in every build path. Source prices existed in `market_data_inputs_v1`, but candidate promotion could see entry-reference evidence as uncertified when the certification bridge was absent or bypassed.

The current 2026-06-01 diagnostics path already had the certification builder wired before diagnostics, so the original `20 raw signals / 0 valid candidate contracts / ENTRY_REFERENCE_PRICE_UNCERTIFIED` failure no longer reproduced. This repair hardened the remaining direct-build path and standardized rejection detail codes so future uncertified price failures are diagnosable.

## 2. Files Changed

- `ops/aegis/entry_reference_price_certification_v1.py`
- `ops/aegis/entry_reference_price_certification_self_check_v1.py`
- `ops/aegis/candidate_contracts_v1.py`
- `constellation_2/common/tests/test_candidate_contracts_v1.py`
- `aegis/modules/operator_portal/aegis.module.yaml`
- `docs/aegis_entry_reference_price_certification_consumption_repair_v1.md`
- `docs/aegis_entry_reference_price_certification_consumption_completion_report_v1.md`

## 3. Certification Contract

Entry reference price certification rows include:

- `symbol`
- `normalized_symbol`
- `price`
- `price_timestamp`
- `market_session`
- `source`
- `source_artifact`
- `source_hash`
- `day_utc`
- `freshness_window_seconds`
- `certification_status`
- `certification_reason_codes`
- `generated_at`

Allowed certification statuses:

- `CERTIFIED`
- `UNCERTIFIED_MISSING_PRICE`
- `UNCERTIFIED_MISSING_SOURCE`
- `UNCERTIFIED_STALE_PRICE`
- `UNCERTIFIED_BAD_SYMBOL`
- `UNCERTIFIED_BAD_SCHEMA`
- `UNCERTIFIED_SOURCE_NOT_ALLOWED`
- `UNCERTIFIED_MARKET_CLOSED`
- `UNCERTIFIED_UNKNOWN`

Candidate contracts may pass only when `entry_reference_price_certification_status = CERTIFIED`.

## 4. Before / After Diagnostics

Before objective baseline from the evidence review:

- Raw signals: 20
- Valid candidate contracts: 0
- Dominant rejection: `ENTRY_REFERENCE_PRICE_UNCERTIFIED`

Current validated 2026-06-01 result:

- Raw signals: 21
- Valid candidate contracts: 19
- Rejected candidate contracts: 2
- `ENTRY_REFERENCE_PRICE_UNCERTIFIED`: 0
- Certified entry price rows: 21

## 5. Before / After Rejection Counts

| Rejection class | Before objective baseline | After validation |
|---|---:|---:|
| `ENTRY_REFERENCE_PRICE_UNCERTIFIED` | 20 | 0 |
| `INSTRUMENT_TYPE_NOT_GOVERNED` | not primary bottleneck | 2 |

Remaining rejected symbols:

| Symbol | Sleeve | Rejection | Entry price status |
|---|---|---|---|
| `IMO` | `C2_MEAN_REVERSION_EQ_V1` | `INSTRUMENT_TYPE_NOT_GOVERNED` | `CERTIFIED` |
| `GLD` | `C2_VOL_INCOME_DEFINED_RISK_V1` | `INSTRUMENT_TYPE_NOT_GOVERNED` | `CERTIFIED` |

## 6. Remaining Uncertified Reasons

No 2026-06-01 candidate rejection remains due to uncertified entry reference price.

Remaining candidate rejections are non-price candidate-field/governance failures:

- missing `candidate.direction`
- missing `candidate.instrument_type`
- missing `candidate.governance_status`

## 7. Tests Run

- `python3 -m py_compile` on touched Python files: pass
- `python3 -m pytest constellation_2/common/tests/test_candidate_contracts_v1.py`: `18 passed`
- `TARGET_DAY=2026-06-01 npm run aegis:repair-input-contracts`: pass
- `TARGET_DAY=2026-06-01 npm run aegis:entry-reference-price-certification`: pass, `21` certified rows
- `TARGET_DAY=2026-06-01 npm run aegis:entry-reference-price-certification-self-check`: pass
- `TARGET_DAY=2026-06-01 npm run aegis:candidate-diagnostics`: pass, `19` valid / `2` rejected
- `npm run aegis:operator-action-model`: pass
- `TARGET_DAY=2026-06-01 npm run aegis:audit`: pass

Known unrelated validation failure:

- `python3 -m pytest constellation_2/phaseL/ui/tests/test_aegis_candidate_ui_projection_v1.py` fails on pre-existing UI text expectations around paper workflow controls (`Confirm Captured`, `Mark Not Captured`, `Defer`, `View / Correct Capture`, `Record Exit`). This repair did not modify those UI workflow controls.

## 8. Audit Result

Final audit for `TARGET_DAY=2026-06-01` completed with the verified runtime graph `READY` and no audit blockers.

Runtime truth remains conservative:

- `trade_advice_allowed: false`
- `manual_trade_capture_allowed: false`
- `broker_submit_required: false`
- `autonomous_execution_allowed: false`

## 9. Safety Confirmation

Safety gates were not weakened:

- stale prices do not certify
- missing prices do not certify
- missing source artifact/hash cannot pass self-check as certified
- unsupported price sources do not certify
- symbol mismatches do not certify
- UI display prices are not certification input
- candidate creation from synthetic prices is not allowed
- trade advice remains disabled
- manual capture remains disabled
- broker execution remains disabled
- autonomous execution remains disabled

## 10. Recommended Next Bottleneck

The next bottleneck is non-price candidate evidence fulfillment for non-Trend sleeves, especially:

- `C2_MEAN_REVERSION_EQ_V1`
- `C2_VOL_INCOME_DEFINED_RISK_V1`

Those sleeves produced rejected contracts with certified prices but missing candidate direction, instrument type, and governance status. The next repair should focus on candidate intent field completeness and governance attribution, not entry-reference price certification.
