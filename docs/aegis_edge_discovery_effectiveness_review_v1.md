# Aegis Edge Discovery Effectiveness Review v1

Review date: 2026-06-01

Target evidence window: last 30 trading days ending 2026-06-01, excluding weekends and Memorial Day 2026-05-25.

This is an evidence review only. It does not propose a new framework, registry, orchestration layer, readiness system, trading behavior, broker behavior, or safety change.

## Executive Finding

Aegis is finding raw opportunities, but the current bottleneck is not proven market edge validation. The bottleneck is candidate promotion and evidence certification.

Observed funnel totals from available artifacts in the 30-trading-day window:

| Funnel stage | Observed count | Interpretation |
|---|---:|---|
| Raw signals | 227 | Discovery is active, concentrated in Trend and Event Dislocation. |
| Valid candidate contracts | 40 | Conversion exists but is concentrated almost entirely in Trend. |
| Paper positions | 36 | Paper exposure exists, almost entirely from Trend-originated candidates. |
| Outcomes | 36 | Outcomes exist, but all are still open in the latest outcome registry. |
| Included validation samples | 0 | No closed/resolved outcomes have matured into validation samples. |

Answer to the central question: Aegis is discovering some useful-looking opportunities, especially in `C2_TREND_EQ_PRIMARY_V1`, but it has not yet validated them. Several other sleeves either produce noisy raw signals that fail candidate gates or are blocked/dormant due to missing inputs. The latest 2026-06-01 run is blocked by evidence certification, specifically `ENTRY_REFERENCE_PRICE_UNCERTIFIED`, not by lack of raw signals.

## Evidence Sources

Primary artifacts reviewed:

| Artifact | Path pattern | Coverage in 30-day window |
|---|---|---:|
| Candidate diagnostics | `/home/node/constellation_runtime_data/truth/reports/aegis_candidate_generation_diagnostics_v1/<day>/candidate_generation_diagnostics.v1.json` | 10 / 30 trading days |
| Candidate contracts | `/home/node/constellation_runtime_data/truth/reports/aegis_candidate_contracts_v1/<day>/candidate_contracts.v1.json` | 5 / 30 trading days |
| Paper position ledger | `/home/node/constellation_runtime_data/truth/reports/aegis_paper_position_ledger_v1/<day>/paper_position_ledger.v1.json` | latest snapshot used for current paper lineage |
| Outcome registry | `/home/node/constellation_runtime_data/truth/reports/aegis_outcome_registry_v1/2026-06-01/outcome_registry.v1.json` | latest snapshot |
| Validation samples | `/home/node/constellation_runtime_data/truth/reports/aegis_validation_samples_v1/2026-06-01/validation_samples.v1.json` | latest snapshot |
| Verified graph | `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-05-30/verified_runtime_graph.v1.json` | `READY`, `audit_blocker_count=0` |

Coverage limitation: diagnostics artifacts are missing for 20 of 30 trading days. Missing artifacts are not counted as zero discovery. The counts below are observed counts from existing evidence, not a claim that unobserved days had no activity.

## Discovery Funnel by Sleeve

Definitions:

- Raw signals: observed raw signal count from candidate diagnostics.
- Candidate intents: observed `candidate_count` in sleeve diagnostics.
- Candidate contracts: valid contracts in `aegis_candidate_contracts_v1`.
- Promotion rate: `candidate_contracts / raw_signals`.
- Rejection rate: `1 - candidate_contracts / raw_signals`; `n/a` when raw signals are zero.
- Validation samples: included validation samples only; excluded open-position samples are not counted as validation evidence.

| Sleeve | Raw signals | Candidate intents | Candidate contracts | Promotion rate | Rejection rate | Paper positions | Outcomes | Validation samples | Classification | Primary failure evidence |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| `C2_TREND_EQ_PRIMARY_V1` | 118 | 18 | 37 | 31.4% | 68.6% | 34 | 34 | 0 | Productive but unvalidated | `PORTFOLIO_GATE_SUPPRESSED`, `RAW_SIGNAL_NOT_PROMOTED`, `ENTRY_REFERENCE_PRICE_UNCERTIFIED` |
| `C2_CROSS_ASSET_TREND_V1` | 7 | 3 | 3 | 42.9% | 57.1% | 2 | 2 | 0 | Productive but underpowered | `SELECTED_INTENT_PROMOTION_CONTRACT_FAILED`, `ENTRY_REFERENCE_PRICE_UNCERTIFIED` |
| `C2_EVENT_DISLOCATION_V1` | 93 | 0 | 0 | 0.0% | 100.0% | 0 | 0 | 0 | Noisy / blocked | `PORTFOLIO_GATE_SUPPRESSED`, `RAW_SIGNAL_NOT_PROMOTED`, `ENTRY_REFERENCE_PRICE_UNCERTIFIED` |
| `C2_MEAN_REVERSION_EQ_V1` | 5 | 0 | 0 | 0.0% | 100.0% | 0 | 0 | 0 | Underpowered | `RAW_SIGNAL_NOT_PROMOTED`, `NON_CERTIFIED_CANDIDATE_SNAPSHOT` |
| `C2_VOL_INCOME_DEFINED_RISK_V1` | 3 | 0 | 0 | 0.0% | 100.0% | 0 | 0 | 0 | Blocked / underpowered | `SLEEVE_INPUT_REQUIREMENT_BLOCKED`, `ENTRY_REFERENCE_PRICE_UNCERTIFIED` |
| `C2_MARKET_NEUTRAL_SPREAD_V1` | 1 | 0 | 0 | 0.0% | 100.0% | 0 | 0 | 0 | Blocked / dormant | `PRODUCER_NONZERO_RC`, `MARKET_DATA_SHA_MISMATCH` |
| `C2_DEFENSIVE_TAIL_V1` | 0 | 0 | 0 | n/a | n/a | 0 | 0 | 0 | Blocked / dormant | `MISSING_REQUIRED_INPUTS`, `SLEEVE_INPUT_REQUIREMENT_BLOCKED` |

## Rejection Analysis

Top observed rejection reasons:

| Rank | Reason | Count | Classification | Finding |
|---:|---|---:|---|---|
| 1 | `PORTFOLIO_GATE_SUPPRESSED` | 150 | Candidate-quality gate / possible over-restrictive gate | Most common suppression; concentrated in Trend and Event Dislocation. Needs calibration review before assuming bad opportunities. |
| 2 | `RAW_SIGNAL_NOT_PROMOTED` | 62 | Promotion/evidence path issue | Raw signals exist but do not become candidate contracts. Latest run shows this alongside operator-review promotion state. |
| 3 | `ENTRY_REFERENCE_PRICE_UNCERTIFIED` | 42 | Legitimate safety rejection caused by evidence certification gap | Prices exist, but entry reference price certification is not accepted by candidate-contract gating. This is evidence quality, not discovery absence. |
| 4 | `NON_CERTIFIED_CANDIDATE_SNAPSHOT` | 5 | Missing evidence / certification | Candidate snapshot evidence is not certified enough for promotion. |
| 5 | `SELECTED_INTENT_NOT_PROMOTED_TO_OPPORTUNITY` | 4 | Promotion gate | Selected intent did not become an opportunity. |
| 6 | `SELECTED_INTENT_PROMOTION_CONTRACT_FAILED` | 2 | Promotion/evidence path issue | Cross-asset selected intents failed contract promotion. |
| 7 | `INSTRUMENT_TYPE_NOT_GOVERNED` | 2 | Legitimate safety / governance rejection | Candidate instrument type lacks governance support. |
| 8 | `PORTFOLIO_GATE_SIGNAL_ONLY` | 1 | Candidate-quality gate | Signal observed but intentionally not promoted. |
| 9 | `PROMOTED_TO_OPERATOR_REVIEW` | 1 | Semantic mismatch / promotion state ambiguity | Appears as a rejection reason despite promotion wording; treat as evidence semantics issue. |

Sleeve-level blocker reasons also appeared:

| Reason | Affected sleeves | Classification |
|---|---|---|
| `SLEEVE_INPUT_REQUIREMENT_BLOCKED` | all sleeves at least once; strongest in Vol Income | Stale or missing input dependency |
| `MISSING_REQUIRED_INPUTS` | Defensive Tail | Missing evidence / stale configuration |
| `MARKET_DATA_SHA_MISMATCH` | Cross Asset, Market Neutral | Stale data / source hash mismatch |
| `PRODUCER_NONZERO_RC` | Market Neutral | Producer failure |

## Discovery Yield

| Sleeve | Discovery Yield | Validation Yield | Research Yield |
|---|---:|---:|---:|
| `C2_CROSS_ASSET_TREND_V1` | 42.9% | 0.0% | 0.0% |
| `C2_TREND_EQ_PRIMARY_V1` | 31.4% | 0.0% | 0.0% |
| `C2_EVENT_DISLOCATION_V1` | 0.0% | n/a | 0.0% |
| `C2_MARKET_NEUTRAL_SPREAD_V1` | 0.0% | n/a | 0.0% |
| `C2_MEAN_REVERSION_EQ_V1` | 0.0% | n/a | 0.0% |
| `C2_VOL_INCOME_DEFINED_RISK_V1` | 0.0% | n/a | 0.0% |
| `C2_DEFENSIVE_TAIL_V1` | n/a | n/a | n/a |

`Research Yield = validated_hypotheses / raw_signals`. The latest research portfolio shows `validated_hypotheses = 0`, so research yield is currently zero for every sleeve with observed raw signals.

## Sleeve Ranking

| Question | Sleeve | Evidence |
|---|---|---|
| Most productive sleeve | `C2_TREND_EQ_PRIMARY_V1` | 118 raw signals, 37 candidate contracts, 34 paper positions, 34 open outcomes. |
| Highest signal volume | `C2_TREND_EQ_PRIMARY_V1` | 118 raw signals, ahead of Event Dislocation at 93. |
| Highest candidate conversion | `C2_CROSS_ASSET_TREND_V1` | 3 contracts from 7 raw signals, 42.9% observed promotion rate. |
| Highest validation yield | None | No included validation samples exist. |
| Most blocked sleeve | `C2_DEFENSIVE_TAIL_V1` | 0 raw signals and repeated `MISSING_REQUIRED_INPUTS` / `SLEEVE_INPUT_REQUIREMENT_BLOCKED`. |
| Most dormant sleeve | `C2_DEFENSIVE_TAIL_V1` | No observed raw signals and no paper/outcome/validation evidence. |
| Most promising underpowered sleeve | `C2_CROSS_ASSET_TREND_V1` | Best conversion rate but only 7 observed raw signals and 2 paper positions. |
| Noisiest sleeve | `C2_EVENT_DISLOCATION_V1` | 93 raw signals, 0 candidate contracts, 0 paper positions. |

## Bottleneck Determination

The primary bottleneck is evidence quality and candidate promotion, not raw discovery.

Evidence:

- Raw signal volume exists: 227 observed raw signals.
- Candidate conversion is uneven: 40 valid contracts, concentrated in Trend and Cross Asset.
- Latest run had 20 raw signals, 0 valid contracts, and 20 `ENTRY_REFERENCE_PRICE_UNCERTIFIED` rejections.
- `docs/aegis_entry_reference_price_certification_repair_v1.md` states that market prices exist in `market_data_inputs_v1`; the failure is that candidate-contract gating does not accept the needed entry-reference certification evidence.
- Validation has not started in a statistically meaningful way: 36 outcomes exist, but they are open; validation samples are excluded as `OPEN_POSITION_NOT_RESOLVED`.

This means Aegis is not yet proving that discovered opportunities are good or bad. It is mostly proving that raw signals are being filtered or blocked before validation.

## Single Highest ROI Improvement

Single improvement: finish the entry reference price certification path so valid current price evidence can certify candidate contracts without weakening the gate.

This is not a new architecture recommendation. It is a targeted repair to the existing candidate-quality evidence path.

Evidence:

- Latest run: 20 raw signals, 0 valid candidate contracts, 20 `ENTRY_REFERENCE_PRICE_UNCERTIFIED` rejections.
- Affected latest-run sleeves: Trend Equity Primary, Cross Asset Trend, Vol Income Defined Risk.
- Existing repair note says prices are present in `market_data_inputs_v1`; the failure is certification linkage into candidate-contract gating.
- This reason is the third most frequent observed rejection reason overall and is the dominant current-run reason.

Expected impact:

- Immediate improvement to `candidate_contracts / raw_signals` on runs where prices are already available but uncertified.
- Most direct impact on Trend, Cross Asset, and Vol Income.
- More paper positions can enter the outcome pipeline, which is required before validation samples and validated hypotheses can increase.

Implementation complexity: medium. The artifact exists, but the evidence graph / candidate-contract gate must consume it consistently. Safety constraint: keep `ENTRY_REFERENCE_PRICE_UNCERTIFIED` as a hard rejection when certification is truly missing, stale, bad-symbol, bad-source, or market-closed.

## Answers to Success Criteria

1. Which sleeves are actually producing opportunities?
   `C2_TREND_EQ_PRIMARY_V1` is producing the most usable opportunities. `C2_CROSS_ASSET_TREND_V1` is producing a small number with the best observed conversion rate.

2. Which sleeves are failing?
   `C2_EVENT_DISLOCATION_V1` is producing many raw signals but no candidate contracts. `C2_DEFENSIVE_TAIL_V1` is dormant/blocked. `C2_VOL_INCOME_DEFINED_RISK_V1`, `C2_MEAN_REVERSION_EQ_V1`, and `C2_MARKET_NEUTRAL_SPREAD_V1` are underpowered or blocked.

3. Why are they failing?
   The dominant causes are portfolio-gate suppression, raw-signal promotion failure, uncertified entry reference prices, missing sleeve inputs, and non-certified candidate snapshots.

4. Is the bottleneck discovery, candidate generation, validation, or evidence quality?
   The bottleneck is evidence quality plus candidate promotion. Validation is also blocked downstream because current outcomes are still open, but validation cannot improve until more candidate contracts safely reach paper outcomes and close.

5. What single change would most improve edge discovery effectiveness?
   Fix entry reference price certification consumption in candidate-contract gating. That is the highest-ROI improvement because it directly addresses the current 20-signal / 0-contract failure without weakening safety gates.

## Validation

Pre-review validation:

- `TARGET_DAY=2026-05-30 npm run aegis:audit`
- Result: passed.
- Verified graph: `READY`.
- Audit blocker count: `0`.

No runtime behavior, trading logic, broker execution, autonomous execution, canonical artifacts, registries, or orchestration layers were changed by this review.
