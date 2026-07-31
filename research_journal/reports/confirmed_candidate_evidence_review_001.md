# Confirmed Candidate Evidence Review 001

Date: 2026-06-05
Status: Evidence review only

Scope: review the newly confirmed Atlas candidate `ptc_backtest_final_854ad10b904e1ae9` without expanding authority. This report does not promote the candidate, change ranking, change paper-forward state, alter replay state, alter qualification, recommend trades, allocate capital, size positions, authorize broker execution, or modify Aegis runtime truth.

## Inputs

- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest_summary.md`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/focused_observation_campaign/latest.json`
- `reports/atlas_v2_research_os/paper_forward_approval_checklist/latest.json`
- `reports/atlas_v2_research_os/candidate_symbol_attribution/latest.json`
- `reports/atlas_v2_research_os/market_data_coverage_tracker/latest.json`
- `reports/atlas_v2_research_os/historical_intraday_download/latest.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_runtime_truth_kernel_v1/2026-06-05/runtime_truth_kernel.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-05/verified_runtime_graph.v1.json`

## Candidate

- candidate id: `ptc_backtest_final_854ad10b904e1ae9`
- mechanism / regime: `MEAN_REVERSION` / `TRENDING`
- attributed universe: `DIA`, `QQQ`
- attributed timeframe: `30m`
- symbol attribution method: `DIRECT_OBSERVATION_LINEAGE`
- symbol attribution confidence: `0.9`
- campaign rank: `4`
- final ranking rank: `14`
- final score: `0.718305`

## Direct Replay Result

Direct candidate data validation classifies the candidate as `CONFIRMED`.

Direct replay fields:

- direct replay ran: `true`
- data exists locally: `true`
- direct result classification: `BACKTEST_SUPPORTED`
- direct result symbol: `QQQ`
- direct result sample size: `48`
- direct result expectancy: `0.004035`
- direct result profit factor: `1.471767`
- direct result max drawdown: `-0.133278`
- missing CSVs for this candidate: none

Proxy comparison:

- proxy expectancy: `0.00362`
- direct expectancy: `0.004035`
- proxy profit factor: `1.627834`
- direct profit factor: `1.471767`
- proxy sample size: `170`
- direct sample size: `48`
- proxy max drawdown: `-0.122772`
- direct max drawdown: `-0.133278`

## Evidence Supporting Confirmation

- `direct_candidate_data_validation/latest.json` marks the candidate `CONFIRMED`, with `direct_replay_ran=true`, `data_exists_locally=true`, and direct result `BACKTEST_SUPPORTED`.
- `direct_candidate_data_validation/latest_summary.md` reports focused validation counts moving to `CONFIRMED: 1`, with this candidate as the confirmed item and `weakened: 0`.
- `market_data_coverage_tracker/latest.json` reports `FULL_COVERAGE` for the candidate's attributed universe, with `DIA` and `QQQ` available and no missing symbols.
- `candidate_symbol_attribution/latest.json` attributes `DIA` and `QQQ` by `DIRECT_OBSERVATION_LINEAGE`, marks the candidate direct-replay eligible, and explicitly denies prose-only symbol inference.
- `final_candidate_ranking/latest.json` already classified the candidate as `READY_FOR_PAPER_FORWARD_OBSERVATION`, `BACKTEST_SUPPORTED`, and `final_eligible=true`, with sample-size and drawdown filters passed.
- The direct replay result preserves positive expectancy and `BACKTEST_SUPPORTED` classification after moving away from proxy-only support.

## Evidence Weakening Confirmation

- Direct replay sample size is `48`, materially smaller than the proxy sample size of `170`.
- Direct profit factor is lower than proxy profit factor: `1.471767` versus `1.627834`.
- Direct max drawdown is worse than proxy max drawdown: `-0.133278` versus `-0.122772`.
- Direct replay result shown in validation is for `QQQ`; the attributed universe is `DIA, QQQ`, so the reviewed confirmation is not strong evidence that both symbols contribute equally.
- The candidate still carries warnings that the plan had no symbol or universe, used SPY adjusted daily data as a local proxy, converted generic observation text into a deterministic daily-bar proxy trigger, and filtered triggered samples by regime constraints.
- `historical_intraday_download/latest.json` shows `DIA_30m.csv` and `QQQ_30m.csv` as `PLANNED_NOT_FETCHED` in dry-run mode; the direct validation relies on daily data plus optional intraday confirmation rather than completed 30m intraday replay.
- `paper_forward_approval_checklist/latest.json` still lists direct-data replay and measurement items as `PENDING`, so checklist state has not been reconciled with the newer direct validation artifact.
- The verified Aegis runtime graph does not contain this Atlas candidate as a promoted runtime truth node.

## Remaining Assumptions

- The direct validation artifact is the authoritative Atlas research artifact for the new `CONFIRMED` classification, but it is not itself Aegis promotion evidence.
- The `QQQ` direct replay result is treated as sufficient for the current Atlas confirmation label, while broader universe-level confidence still depends on understanding the `DIA` contribution.
- Daily-bar replay is treated as valid first-pass direct evidence, despite the candidate's attributed `30m` timeframe requiring separate intraday confirmation for stronger mechanism validation.
- The deterministic daily-bar proxy trigger remains an acceptable approximation for this review's confirmation scope.
- The approval checklist may be stale relative to direct validation, but this report does not reconcile or mutate that state.
- Confirmation is interpreted as research/direct-data validation confirmation only, not paper-forward execution readiness or runtime readiness.

## Taxonomy Risks

- `MEAN_REVERSION` under `TRENDING` regime can blur reversal, pullback, and trend-continuation taxonomy if entry/exit definitions remain generic.
- Proxy-trigger lineage can cause the candidate to inherit SPY daily behavior even after direct-data validation, especially if the direct replay logic remains based on deterministic translation from generic observation text.
- Universe-level attribution may hide symbol-specific heterogeneity between `DIA` and `QQQ`.
- A `CONFIRMED` validation label can be mistaken for promotion, trade advice, or paper placement readiness unless authority boundaries remain explicit.
- The lower direct sample size increases false-confirmation risk and may overstate mechanism stability.

## Paper-Forward Readiness Assessment

Assessment: evidence supports human review for paper-forward observation planning, but does not support any automatic paper-forward state change.

The candidate has a newly confirmed direct-data validation result, full current daily coverage for its attributed symbols, and prior `READY_FOR_PAPER_FORWARD_OBSERVATION` research classification. However, Aegis runtime truth for 2026-06-05 remains `PARTIAL_CONTEXT` with highest readiness layer `BLOCKED`; trade advice, manual trade capture, and autonomous execution are false. The verified runtime graph is also `BLOCKED`, with `trade_advice_allowed=false` and broker submit/transmit disabled by design.

Therefore, this candidate is paper-forward-reviewable as research evidence only. It is not paper-forward state-approved by this report, not promoted, not ranked differently, and not authorized for paper placement, trade recommendation, position sizing, capital allocation, live trading, or broker execution.

## Authority Boundary

Explicit statement: no promotion authority.

This review has no authority to promote `ptc_backtest_final_854ad10b904e1ae9`, change its ranking, change paper-forward state, create a paper position, recommend a trade, allocate capital, size a position, authorize live trading, or authorize broker execution.
