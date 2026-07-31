# Aegis Entry Reference Price Certification Consumption Repair v1

Review and repair date: 2026-06-01

Scope: repair and harden entry reference price certification consumption before candidate contract gating. This does not weaken gates, force candidates, invent prices, or enable trade advice, manual capture, broker execution, autonomous execution, automatic approval, or live trading behavior.

## Phase 1 Reproduction

Command run:

```bash
TARGET_DAY=2026-06-01 npm run aegis:candidate-diagnostics
```

Current reproduced output after existing certification builder was present:

| Metric | Value |
|---|---:|
| Raw signals | 21 |
| Valid candidate contracts | 19 |
| Rejected candidate contracts | 2 |
| Total candidates generated | 19 |
| Total candidates rejected | 2 |

Raw signals by sleeve:

| Sleeve | Raw signals |
|---|---:|
| `C2_TREND_EQ_PRIMARY_V1` | 18 |
| `C2_CROSS_ASSET_TREND_V1` | 1 |
| `C2_MEAN_REVERSION_EQ_V1` | 1 |
| `C2_VOL_INCOME_DEFINED_RISK_V1` | 1 |

Rejected candidate contracts after reproduction:

| Symbol | Sleeve | Rejection | Entry price certification status | Detail |
|---|---|---|---|---|
| `IMO` | `C2_MEAN_REVERSION_EQ_V1` | `INSTRUMENT_TYPE_NOT_GOVERNED` | `CERTIFIED` | Missing `candidate.direction`, `candidate.instrument_type`, `candidate.governance_status` |
| `GLD` | `C2_VOL_INCOME_DEFINED_RISK_V1` | `INSTRUMENT_TYPE_NOT_GOVERNED` | `CERTIFIED` | Missing `candidate.direction`, `candidate.instrument_type`, `candidate.governance_status` |

Current affected symbols with certified price evidence: `QQQ`, `IMO`, `AAL`, `AAPL`, `AMD`, `AMDL`, `AMT`, `APTV`, `ARM`, `BAC`, `BANC`, `BKSY`, `BNS`, `BOXX`, `BTSG`, `BURL`, `CACC`, `CCL`, `CRDO`, `CSCO`, `GLD`.

Current result: the original `20 raw signals / 0 valid candidate contracts / ENTRY_REFERENCE_PRICE_UNCERTIFIED` failure no longer reproduces in the rendered artifact path. The current candidate gate consumes certified price evidence for all 21 raw signals.

## Phase 2 Price Certification Flow

Observed path:

1. Raw signals are collected from `sleeve_evaluation_kernel_v1` output intents.
2. `ops.aegis.entry_reference_price_certification_v1` builds `aegis_entry_reference_price_certification_v1` from raw signals plus `market_data_inputs_v1` and `aegis_data_registry_v1`.
3. `ops.aegis.signal_evidence_graph_v1` consumes the certification artifact and attaches entry-reference fields to each signal evidence row.
4. `ops.aegis.candidate_contracts_v1` consumes the signal evidence graph and allows candidate contracts only when required candidate fields are present and the entry price edge is certified.
5. `ops.aegis.candidate_generation_diagnostics_v1` projects candidate contract results and rejection details to the operator-visible diagnostics path.
6. Candidate Generation By Sleeve / Command Center consume the diagnostics projection and rejected candidate visibility rows.

Root cause of the prior failure:

- Price values were available in `market_data_inputs_v1`, but candidate-contract gating previously did not reliably consume a deterministic entry-reference certification artifact before graph/gate construction.
- When the certification bridge was missing or not consumed, price evidence appeared as `EVIDENCE_NOT_CERTIFIED`, producing `ENTRY_REFERENCE_PRICE_UNCERTIFIED` even though source price rows existed.
- The current code already added the certification builder and diagnostics wrapper ordering. This repair hardens the remaining bypass: direct candidate-contract builds now regenerate and consume the certification artifact before building the graph.

Current confirmed non-causes for 2026-06-01:

| Potential cause | Finding |
|---|---|
| Price not generated | Not current cause. `market_data_inputs_v1` has rows for all 21 raw-signal symbols. |
| Price generated but not attached | Not current cause. Candidate contracts include entry reference price fields. |
| Price attached but not certified | Not current cause. All 21 certification rows are `CERTIFIED`. |
| Certification generated after candidate gate | Repaired/hardened. Candidate diagnostics and direct candidate-contract builds generate certification before graph/gate. |
| Certification artifact path mismatch | Not current cause. Candidate evidence paths include `aegis_entry_reference_price_certification_v1`. |
| Source hash missing | Not current cause. Certified rows carry `source_hash`. Self-check now fails if certified rows lack source artifact/hash. |
| Stale price timestamp | Not current cause. Rows use current 2026-06-01 market session timestamps. |
| Source not allowed | Not current cause. Sources are allowed, including `YAHOO_CHART` and `LOCAL_CACHE`. |
| Symbol normalization mismatch | Not current cause. Normalized symbols match. |
| Schema/version mismatch | Not current cause. Self-check now rejects unknown certification statuses and unknown detail codes. |
| Candidate gate expecting wrong field name | Not current cause for price; remaining rejected contracts are missing direction/instrument/governance fields. |

## Phase 3 Certification Contract

Each certification row now follows this operator-facing contract:

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

Canonical detail reason codes for uncertified price evidence:

- `PRICE_MISSING`
- `PRICE_TIMESTAMP_MISSING`
- `PRICE_STALE`
- `PRICE_SOURCE_MISSING`
- `PRICE_SOURCE_NOT_ALLOWED`
- `SOURCE_ARTIFACT_MISSING`
- `SOURCE_HASH_MISSING`
- `SYMBOL_NORMALIZATION_FAILED`
- `MARKET_SESSION_UNSUPPORTED`
- `CERTIFICATION_ARTIFACT_MISSING`
- `CERTIFICATION_SCHEMA_MISMATCH`

Candidate contracts may pass only when `entry_reference_price_certification_status = CERTIFIED`.

## Phase 4 Implementation Summary

Existing files repaired rather than duplicated:

- `ops/aegis/entry_reference_price_certification_v1.py`
- `ops/aegis/entry_reference_price_certification_self_check_v1.py`
- `ops/aegis/candidate_contracts_v1.py`
- `constellation_2/common/tests/test_candidate_contracts_v1.py`
- `aegis/modules/operator_portal/aegis.module.yaml`

Key repair:

- `build_candidate_contracts_v1` now regenerates and writes `aegis_entry_reference_price_certification_v1` before building `signal_evidence_graph_v1` and candidate contracts. This closes the direct-build bypass where a caller could construct candidate contracts without first producing the certification artifact.

## Phase 5 Operator Detail Reasons

High-level entry-price rejections remain unchanged:

- `ENTRY_REFERENCE_PRICE_MISSING`
- `ENTRY_REFERENCE_PRICE_STALE`
- `ENTRY_REFERENCE_PRICE_UNCERTIFIED`
- `ENTRY_REFERENCE_PRICE_SYMBOL_MISMATCH`

Detail reason codes now use canonical operator codes instead of producer-specific internal strings. Candidate Generation By Sleeve already projects `detail_reason_codes`, `entry_reference_price_certification_status`, `entry_reference_price_timestamp_utc`, `entry_reference_price_source_path`, and safe-repair availability from diagnostics.

## Current 2026-06-01 Result

After repair validation:

| Metric | Before repair objective baseline | Current after repair |
|---|---:|---:|
| Raw signals | 20 reported in prior review | 21 |
| Valid candidate contracts | 0 reported in prior review | 19 |
| Rejected candidate contracts | 20 reported in prior review | 2 |
| `ENTRY_REFERENCE_PRICE_UNCERTIFIED` rejections | 20 reported in prior review | 0 |
| Certified entry price rows | not consumed reliably | 21 |

The remaining two rejected contracts are not price-certification failures. They are rejected because direction, instrument type, and governance status are missing.

## Safety Confirmation

Safety gates were not weakened:

- stale prices do not certify
- missing prices do not certify
- missing source artifact/hash cannot pass self-check as certified
- unsupported price source does not certify
- symbol mismatch does not certify
- UI display prices are not certification input
- candidate creation from synthetic prices is not allowed
- broker execution remains disabled
- autonomous execution remains disabled
- trade advice remains disabled
- manual capture remains disabled

## Central Answer

For the current 2026-06-01 run, candidate contracts are no longer being rejected because Aegis failed to generate, attach, or consume certified entry price evidence. The current price evidence is generated, attached, consumed, and audited before candidate gating.

Remaining rejected contracts are rejected for non-price candidate-field/governance reasons. The next bottleneck is candidate evidence fulfillment for `candidate.direction`, `candidate.instrument_type`, and `candidate.governance_status` on non-Trend sleeves, especially `C2_MEAN_REVERSION_EQ_V1` and `C2_VOL_INCOME_DEFINED_RISK_V1`.
