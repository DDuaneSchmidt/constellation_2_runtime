# Direct Validation Feasibility Study 001

Date: 2026-06-05
Status: ANALYSIS_ONLY

## Purpose

Determine whether the current direct-validation design can validate `CHOP` candidates without intraday replay.

This report does not change validation logic, replay logic, candidate state, qualification, governance, paper placement, capital allocation, trade advice, broker execution, or runtime authority.

## Runtime Truth Boundary

Required pre-work was attempted with `npm run aegis:audit`. The audit did not complete cleanly. It stopped during `aegis:candidate-to-paper-self-check` with `NON_DETERMINISTIC_OUTPUT`.

Latest verified runtime graph reviewed:

- `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-05/verified_runtime_graph.v1.json`

Graph state reviewed:

- `active_mode`: `HUMAN_REVIEWED_PAPER_MODE`
- `active_mode_readiness_status`: `BLOCKED`
- `graph_status`: `BLOCKED`
- `runtime_readiness_status`: `BLOCKED`
- runtime truth linkage: `PARTIAL_CONTEXT`, highest readiness layer `BLOCKED`

The graph explicitly includes do-not-claim constraints: do not claim readiness from code or manifests alone, do not claim trade advice unless the runtime truth kernel allows it, and do not claim a real promoted runtime actionable candidate unless promotion evidence exists.

Therefore this feasibility study is evidence classification only. It is not validation authority.

## Evidence Reviewed

- `research_journal/design/direct_validation_pipeline_map_001.md`
- `research_journal/reports/validation_regime_mapping_study_001.md`
- `research_journal/reports/regime_filter_root_cause_001.md`
- `research_journal/reports/event_metadata_requirements_001.md`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/paper_forward_approval_checklist/latest.json`
- `constellation_2/common/atlas_v2_research_os/direct_candidate_data_validation.py`
- `constellation_2/common/atlas_v2_research_os/candidate_backtests.py`

## Current Direct-Validation Design

Current direct validation uses local OHLCV CSVs, normalizes them to rows with `date`, `open`, `high`, `low`, `close`, and `volume`, then runs the generic candidate backtest path.

The backtest path computes daily-bar features and uses deterministic proxy triggers:

- `BREAKOUT`: close above prior 20-day high.
- `MEAN_REVERSION`: close below 20-day average by at least 1 percent.
- `REVERSAL`: three-day down streak followed by close above open.
- `EVENT_REACTION`: large positive daily move versus 20-day volatility.

The regime classifier emits only:

- `HIGH_VOLATILITY`
- `TRENDING`
- `RANGE_BOUND`
- `LOW_VOLATILITY`
- `UNKNOWN`

It does not emit `CHOP`.

The regime gate uses exact matching:

```text
triggered_sample.regime in allowed_regimes
```

For a candidate labeled `CHOP`, direct validation sets `allowed_regimes=[CHOP]`. Because the daily classifier never emits `CHOP`, every triggered CHOP sample is filtered out under current design.

## Current Direct-Validation Results

Latest direct-validation report reviewed 8 candidates:

- `CONFIRMED`: 2
- `INSUFFICIENT_DATA`: 6
- direct data exists locally: 8
- direct replays run: 8
- missing CSVs: 0

All five `CHOP` candidates remain `INSUFFICIENT_DATA` with `sample_size=0` after trigger/regime filtering:

| candidate_id | mechanism | required timeframe | symbols | direct-validation result | immediate blocker |
| --- | --- | --- | --- | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | `BREAKOUT` | daily plus optional 30m confirmation | DIA, QQQ, SPY | `INSUFFICIENT_DATA`, sample 0 | `CHOP` exact-label mismatch |
| `ptc_backtest_final_3a4ac24107c77136` | `BREAKOUT` | daily plus optional 5m confirmation | DIA, QQQ, SPY | `INSUFFICIENT_DATA`, sample 0 | `CHOP` exact-label mismatch |
| `ptc_backtest_final_d5931b24bd391113` | `EVENT_REACTION` | intraday plus daily confirmation, 30m | AMZN, BAC, META, MSFT, NFLX, TSLA | `INSUFFICIENT_DATA`, sample 0 | `CHOP` mismatch plus missing event/intraday validation layer |
| `ptc_backtest_final_7d839944a8a4070a` | `BREAKOUT` | daily plus optional 15m confirmation | TLT, USO | `INSUFFICIENT_DATA`, sample 0 | `CHOP` exact-label mismatch |
| `ptc_backtest_final_b23c6756bfb3263a` | `BREAKOUT` | daily plus optional 30m confirmation | BAC, GOOGL, META, MSFT, NFLX, TSLA | `INSUFFICIENT_DATA`, sample 0 | `CHOP` exact-label mismatch |

Paper-forward approval checklist also keeps these CHOP candidates pending on direct-data replay, measurable observation entry/exit conditions, explicit invalidation condition, minimum sample-size acceptance, and human paper-forward confirmation.

## Questions

### Can daily data ever validate CHOP?

Not under the current validation design.

Daily data may contain range-bound behavior, but the current replay classifier does not emit `CHOP`. Since the gate requires exact regime equality, `CHOP` candidates cannot accumulate validation samples in the current path.

Daily data could validate a future daily-bar candidate whose regime is declared in the validation vocabulary, for example `RANGE_BOUND`, if governance explicitly maps that to the candidate hypothesis. That is not the current design and cannot be inferred silently.

### Can daily data approximate CHOP?

Yes, but only as approximation evidence.

The closest daily proxies are `RANGE_BOUND`, possibly `LOW_VOLATILITY`, and in some diagnostic contexts `UNKNOWN`. Existing regime mapping work classifies `CHOP` to those labels as a partial, ambiguous mapping, not an exact mapping.

Daily approximation can support:

- feasibility triage
- sample-loss diagnostics
- rough comparison of range-bound daily bars
- deciding whether intraday/event replay is worth funding

Daily approximation cannot support:

- a validated `CHOP` claim under exact-label governance
- event-timed entry/exit validation
- 5m, 15m, or 30m execution-window claims
- production promotion or paper-forward approval by itself

### Is intraday mandatory?

Intraday is mandatory for candidates whose hypothesis depends on intraday timing, event windows, or intraday confirmation.

For the current CHOP set:

- `ptc_backtest_final_d5931b24bd391113` is impossible to validate without intraday replay because it is an `EVENT_REACTION` candidate with 30m event timing. Daily bars cannot prove whether the move occurred after the event, inside the intended window, or as ordinary daily volatility.
- The four `BREAKOUT / CHOP` candidates have daily trigger proxies, so intraday is not conceptually mandatory for a daily-only approximation. However, their current candidate metadata includes optional or candidate-specific intraday timeframes. Any claim about 5m, 15m, or 30m confirmation requires intraday replay.

So the precise answer is: intraday is not mandatory to approximate daily range-bound CHOP behavior, but it is mandatory to validate intraday CHOP claims and mandatory for the event-reaction CHOP candidate.

### Is event metadata mandatory?

Event metadata is mandatory for `EVENT_REACTION` validation.

For `ptc_backtest_final_d5931b24bd391113`, required metadata includes timestamped symbol-specific events, event-to-30m-bar alignment, event eligibility filters, reaction windows, event-type stratification, and non-event control comparison. Daily OHLCV cannot reconstruct those facts.

Event metadata is not mandatory for non-event `BREAKOUT / CHOP` approximation, unless the candidate thesis relies on a catalyst or event-defined entry window.

### Which candidates are impossible under current validation design?

Impossible to validate as `CHOP` under current exact-label daily design:

- `ptc_backtest_final_469607b8340421b7`
- `ptc_backtest_final_3a4ac24107c77136`
- `ptc_backtest_final_d5931b24bd391113`
- `ptc_backtest_final_7d839944a8a4070a`
- `ptc_backtest_final_b23c6756bfb3263a`

Impossible for a stronger reason:

- `ptc_backtest_final_d5931b24bd391113` is also impossible as an event-reaction validation without timestamped event metadata and 30m replay.

Not impossible in principle, but not valid under current design:

- The four `BREAKOUT / CHOP` candidates could become testable if governance adds an explicit `CHOP` compatibility rule, such as a predeclared `CHOP -> RANGE_BOUND` mapping, and if the candidate claim is constrained to daily-bar approximation rather than intraday confirmation.

## Feasibility Conclusion

Direct validation cannot currently validate CHOP candidates without intraday replay because the current daily regime vocabulary never emits `CHOP`, and the current gate requires exact regime matching.

Daily data can approximate CHOP only after an explicit, governed vocabulary bridge is defined. That bridge would need to label the result as daily CHOP approximation, not full CHOP validation.

Intraday replay is mandatory for any candidate whose edge depends on 5m, 15m, 30m, opening/session timing, or event reaction. Event metadata is mandatory for event-reaction candidates.

Under current validation design, all five current CHOP candidates are impossible to validate as CHOP. The event-reaction CHOP candidate is additionally impossible without event metadata and intraday event-aware replay.

## Authority Boundary

This report is analysis-only. It does not implement mappings, relax filters, validate candidates, change qualification, promote candidates, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, or alter safety gates.

