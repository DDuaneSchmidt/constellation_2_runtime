# Replay Yield Discrepancy 001

Date: 2026-06-05

Question: Why does global replay appear healthy while candidate replay collapses?

## Short Answer

Global replay appears healthy because the global metric is an aggregate sample-retention metric over an already-selected replay/backtest cohort. It reports that most triggered proxy samples survive filters across the cohort.

Candidate-specific replay collapses when the candidate's own constraints, data availability, symbol specificity, and admission gates are applied. The global average hides that collapse because high-sample candidates with yield `1.0` dominate the aggregate, while weak candidates are exposed only in per-candidate rows or later qualification/admission artifacts.

The discrepancy is therefore not a contradiction. It is a scope mismatch:

- Global replay yield answers: "Across this replay cohort, how many trigger samples survived filters?"
- Candidate-specific replay yield answers: "Did this candidate retain enough relevant, candidate-specific evidence after its own filters and gates?"
- Runtime candidate health answers: "Did today's runtime candidate flow produce valid, contract-compliant, admissible candidates?"

These are different questions.

## Evidence

### Runtime Truth Boundary

The verified runtime graph for 2026-06-05 reports:

- `graph_status`: `BLOCKED`
- `runtime_readiness_status`: `BLOCKED`
- `active_mode`: `HUMAN_REVIEWED_PAPER_MODE`
- `active_mode_readiness_status`: `BLOCKED`

The same graph marks `atlas_v2_research_os_historical_replay_validation_v1` as `READY`, but its behavior contract says historical replay generates `HISTORICAL_REPLAY` evidence only. It cannot authorize trading, broker execution, capital allocation, candidate promotion, production promotion, portfolio construction, position sizing, trade recommendations, automatic paper placement, or bypass paper testing.

Interpretation: replay capability readiness is not candidate admission readiness.

Source:

- `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-05/verified_runtime_graph.v1.json`

### Global Replay Sample Yield

`reports/atlas_v2_research_os/replay_sample_yield/latest.json` reports:

- `raw_candidate_count`: `12`
- `trigger_sample_count`: `13833`
- `post_filter_sample_count`: `12300`
- `replay_sample_yield`: `0.889178`
- `zero_sample_candidate_count`: `0`
- `low_sample_candidate_count`: `0`
- `regime_filter` attrition: `1533` samples, `0.110822` of trigger samples

This looks healthy at the aggregate level: 12,300 of 13,833 trigger samples survive.

Source:

- `reports/atlas_v2_research_os/replay_sample_yield/latest.json`

### Candidate-Specific Yield

The candidate rows are uneven:

| Candidate | Mechanism | Classification | Trigger Samples | Post-Filter Samples | Candidate Yield | Main Warning |
|---|---:|---:|---:|---:|---:|---|
| `ptc_f492d1bebb6bd47f` | `MEAN_REVERSION` | `BACKTEST_SUPPORTED` | 1540 | 170 | 0.11039 | 1370 triggered samples filtered by regime constraints |
| `ptc_3d46c2d50e5fe58b` | `BREAKOUT` | `BACKTEST_WEAK` | 811 | 648 | 0.799014 | 163 triggered samples filtered by regime constraints |
| Most other rows | mixed | mixed | mixed | same as trigger | 1.0 | proxy/generic-trigger limitations |

The global `0.889178` value is not representative of the weakest candidate. It is a weighted aggregate by sample count, not a guarantee that each candidate survives its own filters well.

Source:

- `reports/atlas_v2_research_os/replay_sample_yield/latest.json`

### Replay/Backtest Support Is Proxy-Based

Candidate backtests report:

- `candidates_tested`: `12`
- `candidates_supported`: `8`
- `candidates_weak`: `4`
- `candidates_insufficient_data`: `0`

But the report also states key limitations:

- candidate plans do not specify a tradable symbol or universe
- SPY is used only as a local proxy series
- daily adjusted OHLCV cannot fully test intraday mechanisms such as opening range, session timing, or VWAP behavior
- no transaction cost, slippage, broker, capital, or position-sizing assumptions are applied
- generic observation conditions were mapped to deterministic daily-bar proxy triggers

Interpretation: replay/backtest support is not candidate-specific market proof. It is research-only proxy evidence.

Source:

- `reports/atlas_v2_research_os/candidate_backtests/latest.json`
- `reports/atlas_v2_research_os/candidate_backtests/latest_summary.md`

### Final Qualification Suppresses Many Candidates

Backtest-aware final qualification reports:

- `candidates_evaluated`: `600`
- `backtest_supported_candidates`: `228`
- `final_eligible_candidates`: `110`
- `backtest_supported_final_passes`: `110`
- `backtest_supported_pass_rate`: `0.482456`
- `preliminary_eligible_candidates`: `0`

Suppressors:

- `proxy_penalty_count`: `600`
- `intraday/daily mismatch penalty count`: `180`
- `sample-size penalty count`: `156`
- main disqualification reasons include `final_score below 0.7`, `BACKTEST_WEAK`, and `INSUFFICIENT_DATA`

Interpretation: even when replay/backtest is supportive, final qualification penalizes proxy evidence and daily/intraday mismatch. Candidate-specific survival is much lower than aggregate replay sample survival.

Source:

- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest_summary.md`

### Runtime Candidate Flow Collapses Separately

The 2026-06-05 runtime diagnostics show a harsher admission-layer collapse:

- `total_candidates_generated`: `0`
- `total_candidates_rejected`: `43`
- `valid_candidate_contracts`: `0`
- `rejected_candidate_contracts`: `0`
- `rejection_stage_counts.CANDIDATE_CONVERSION`: `42`
- `zero_candidate_explanation`: `No candidates found because required data was missing/stale.`
- `selected_intent_promotion.promoted_candidate_count`: `0`

Several rejected raw signal rows have `candidate_gate_failed: true`. Candidate diagnostics also show candidate reasons such as `PORTFOLIO_GATE_SUPPRESSED; NON_CERTIFIED_CANDIDATE_SNAPSHOT`.

Interpretation: runtime candidates are not merely losing replay samples. They are failing candidate conversion, contract validity, certification, and portfolio/admission gates.

Source:

- `/home/node/constellation_runtime_data/truth/reports/aegis_candidate_generation_diagnostics_v1/2026-06-05/candidate_generation_diagnostics.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_research_daily_scorecard_v1/2026-06-05/research_daily_scorecard.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_sleeve_throughput_diagnostics_v1/2026-06-05/sleeve_throughput_diagnostics.v1.json`

## Root Causes

1. Aggregation masks candidate attrition.

   The global replay yield is `post_filter_sample_count / trigger_sample_count` across the cohort. It is sample-weighted. Candidates with many retained samples and yield `1.0` make the global number look healthy even when a specific candidate retains only `170 / 1540 = 0.11039`.

2. Global replay uses an already-selected research cohort.

   `replay_sample_yield` reads `candidate_backtests/latest.json`, not the full runtime candidate funnel. It does not include today's rejected raw signals, failed candidate contracts, missing/stale data blockers, or portfolio gate suppression.

3. Candidate-specific replay applies candidate-specific filters.

   The collapsed candidate row is mostly explained by regime constraints: `1370` of `1540` trigger samples were filtered. That is not visible from the aggregate headline unless the candidate rows are inspected.

4. Replay support is proxy support, not candidate-specific proof.

   The candidate backtest artifacts repeatedly state that candidate plans lack tradable symbol/universe specificity and use SPY daily bars as a local proxy. That can make global replay look orderly while candidate-specific validity remains weak for intraday or symbol-specific claims.

5. Final qualification penalizes exactly the evidence weakness that global replay tolerates.

   Backtest-aware qualification applies proxy penalties, intraday/daily mismatch penalties, and sample-size penalties. As a result, only `110` of `228` backtest-supported candidates pass final qualification.

6. Runtime candidate health is gated by contracts and data freshness.

   On 2026-06-05, runtime candidate generation had `0` generated candidates, `43` rejected candidates, and `0` valid candidate contracts. This is not contradicted by Atlas research replay health because those artifacts measure different layers.

## Conclusion

Global replay appears healthy because it measures aggregate sample retention in a research replay/backtest cohort. Candidate replay collapses because candidate-specific filters, missing symbol/universe specificity, proxy penalties, intraday/daily mismatch, sample-size penalties, and runtime candidate gates remove or downgrade evidence at the candidate layer.

The safe reading is:

- `0.889178` global replay yield means the replay machinery can retain many proxy samples across selected candidates.
- It does not mean every candidate has robust candidate-specific evidence.
- It does not mean runtime candidate conversion is healthy.
- It does not authorize paper placement, promotion, capital, or trading.

Consumers should not use global replay yield as candidate truth. They should query candidate-level replay rows, final qualification, candidate contracts, candidate generation diagnostics, and the verified runtime graph.
