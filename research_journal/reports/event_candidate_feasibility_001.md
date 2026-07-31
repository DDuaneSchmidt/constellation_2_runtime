# Event Candidate Feasibility 001

Date: 2026-06-05
Status: ANALYSIS_ONLY

## Scope

Determine whether the single `EVENT_REQUIRED` candidate is worth pursuing.

Focus candidate:

| Field | Value |
| --- | --- |
| candidate_id | `ptc_backtest_final_d5931b24bd391113` |
| mechanism / regime | `EVENT_REACTION` / `CHOP` |
| timeframe | `30m` |
| symbols | `AMZN`, `BAC`, `META`, `MSFT`, `NFLX`, `TSLA` |
| current state | `EVENT_REQUIRED` / direct `INSUFFICIENT_DATA` |
| campaign rank | 5 |
| final robust rank | 25 |
| validation queue priority | 7 of 8 |

Inputs reviewed:

- `reports/atlas_v2_research_os/candidate_validation_queue_001.md`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/family_paper_forward_observation/latest.json`
- `research_journal/reports/event_metadata_requirements_001.md`
- `research_journal/reports/event_evidence_acquisition_plan_001.md`
- `research_journal/reports/intraday_event_evidence_readiness_pack_001.md`
- `research_journal/reports/candidate_validation_economics_001.md`
- `research_journal/reports/candidate_quality_decision_gate_001.md`
- `research_journal/reports/candidate_quality_scoreboard_002.md`

This report does not acquire event metadata, acquire intraday data, run replay, alter validation, change candidate state, change qualification, change governance, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, or modify runtime truth.

## Current Evidence

Proxy evidence is positive but not validation-grade:

| Metric | Value |
| --- | ---: |
| proxy classification | `BACKTEST_SUPPORTED` |
| proxy sample size | 96 |
| proxy expectancy | 0.003415 |
| proxy profit factor | 1.557274 |
| proxy final score | 0.714075 |
| replay/backtest consistency | 0.914509 |
| final ranking classification | `READY_FOR_PAPER_FORWARD_OBSERVATION` |

Direct validation is blocked:

| Metric | Value |
| --- | ---: |
| direct classification | `INSUFFICIENT_DATA` |
| direct sample size | 0 |
| triggered samples filtered | 85 in direct validation; 419 in final-ranking warnings |
| direct expectancy | none |
| direct profit factor | none |
| direct max drawdown | none |

The direct validation gap is not simply missing daily CSVs. The candidate needs event-aware validation: timestamped event metadata, 30m bars, event-to-bar alignment, event eligibility rules, and non-event controls.

## Expected Validation Gain

Expected validation gain: `MEDIUM`.

Positive validation gain drivers:

- It is the only focused `EVENT_REQUIRED` candidate, so resolving it tests a different evidence lane than normal intraday rule validation.
- It covers a distinct mechanism: `EVENT_REACTION`, which is not validated by ordinary daily proxy replay.
- It has proxy support: sample size 96, profit factor 1.557274, and final score 0.714075.
- It shares several symbols with the single-stock intraday cluster, especially `BAC`, `META`, `MSFT`, `NFLX`, and `TSLA`.
- A successful event-aware replay would clarify whether Atlas can validate event reactions at all, not just this one candidate.

Limits on validation gain:

- Current direct replay produced zero usable samples.
- Daily data cannot establish event ordering, reaction-window membership, or whether the move followed the event rather than preceded it.
- The candidate is priority 7 of 8 in the validation queue, below simpler and higher-gain intraday candidates.
- The candidate has lower robust rank than the top breakout/chop and mean-reversion/trending validation targets.
- Any positive result would still require careful separation from broad market moves, earnings drift, and symbol-specific news selection bias.

Net: pursuing this candidate now is unlikely to produce the fastest validation throughput improvement, but it could unlock an important future event-validation capability.

## Event Metadata Complexity

Event metadata complexity: `HIGH`.

Minimum evidence required:

- 30m OHLCV for `AMZN`, `BAC`, `META`, `MSFT`, `NFLX`, and `TSLA`.
- About 90,000-102,000 total 30m bars across the six-symbol five-year range estimated in the readiness pack.
- Timestamped event metadata for the same symbols and date range.
- Event type, source, source timestamp, session bucket, first eligible 30m bar, and source lineage for every validation-eligible event.
- Exclusion handling for date-only events, unknown timestamps, duplicate catalysts, broad market events, and events not known before the reaction window.
- Event counts and sample counts by symbol and event type.
- Non-event control windows.

Complexity drivers:

- Event records are more semantically fragile than bar data.
- Different event types need different timestamp and materiality rules.
- Earnings releases, earnings calls, guidance, analyst actions, regulatory events, and unscheduled news cannot all be treated as one interchangeable event class.
- After-hours and pre-market events require explicit session alignment.
- `BAC` can be affected by macro and rates context, but the candidate is single-stock event reaction; broad context cannot replace symbol-specific event evidence.
- `CHOP` must remain predeclared or bar-aligned; it cannot be inferred after seeing event outcomes.

## Expected Candidate Value

Expected candidate value: `MEDIUM`.

Value arguments for keeping the candidate alive:

- It represents mechanism diversity. Retiring it would leave the focused validation queue more dependent on breakout, mean-reversion, and reversal mechanisms.
- It can test whether Atlas can handle event-timed hypotheses rather than only price-pattern hypotheses.
- It has nontrivial proxy support and survived final qualification.
- It may become more valuable if event metadata infrastructure is needed for multiple future candidates.

Value arguments against immediate pursuit:

- The candidate is not top-ranked by validation priority.
- It is a single candidate, so the initial event-metadata build has low immediate candidate-count leverage.
- Existing direct validation provides no usable samples.
- Proxy support may be inflated by daily proxy mechanics and generic observation triggers.
- Pursuing it before the high-value intraday candidates may delay faster evidence gains.

The candidate is strategically interesting, but the current single-candidate payoff is not high enough to justify a full event metadata lane ahead of simpler high-value intraday validation work.

## Maintenance Cost

Maintenance cost: `HIGH`.

Ongoing maintenance would include:

- keeping event metadata source lineage reconstructable
- refreshing event calendars and timestamped news records
- deduplicating multiple reports about the same catalyst
- handling timestamp corrections and provider differences
- maintaining event type taxonomies
- preserving session bucket and timezone logic
- maintaining event-to-30m-bar alignment rules
- recording excluded events and exclusion reasons
- comparing event windows against non-event controls
- preventing event metadata from becoming hidden validation authority

Maintenance risk is materially higher than for ordinary OHLCV-only intraday validation. The largest risk is not storage volume; it is semantic drift, where event definitions, timestamp quality, or event selection rules quietly change the validation population.

## Decision Matrix

| Dimension | Rating | Reading |
| --- | --- | --- |
| expected validation gain | `MEDIUM` | Useful if event validation becomes a reusable lane; slower than high-priority intraday work for immediate validation throughput. |
| event metadata complexity | `HIGH` | Requires six-symbol 30m data plus timestamped, source-linked event metadata and event-aware replay rules. |
| expected candidate value | `MEDIUM` | Mechanism diversity and proxy support are real, but it is only one lower-priority focused candidate. |
| maintenance cost | `HIGH` | Event taxonomy, timestamp lineage, deduplication, session handling, and non-event controls create ongoing cost. |
| authority risk | `MEDIUM-HIGH` | Evidence design is manageable, but risk becomes high if event metadata is treated as validation authority. |

## Output Decision

```text
DEFER
```

Justification:

`ptc_backtest_final_d5931b24bd391113` should not be retired, because it has proxy support, final eligibility, mechanism diversity, and could become a useful test case for event-aware validation. But it should not be the next pursuit target because the expected validation gain is only medium while event metadata complexity and maintenance cost are high.

The right sequencing is:

1. Pursue higher-value intraday candidates first, especially candidates with high validation gain and no event metadata dependency.
2. Keep this event candidate in `EVENT_REQUIRED`.
3. Revisit pursuit only when event metadata infrastructure can support more than this single candidate, or when the single-stock 30m data cluster is already being acquired for other candidates.

## Reopen Criteria

Move from `DEFER` to `PURSUE` only if at least one of the following becomes true:

- timestamped event metadata is already available for all six symbols over the target date range
- 30m bars for all six symbols are already available from another validation lane
- at least three event-reaction candidates can share the same event metadata dataset
- a human reviewer explicitly prioritizes event-reaction mechanism diversity over near-term validation throughput
- a small manually reviewed event sample shows that eligible event counts are likely to exceed the minimum sample threshold after filtering

## Retirement Criteria

Move from `DEFER` to `RETIRE` if any of the following becomes true:

- event metadata remains unavailable or date-only after a bounded sourcing review
- eligible event samples remain below the minimum threshold after event-aware filtering
- non-event controls explain the apparent proxy edge
- source lineage cannot distinguish event reaction from ordinary volatility
- event metadata maintenance would serve only this one candidate with no reusable research value
- future direct/event-aware replay weakens the candidate after adequate evidence exists

## Authority Boundary

This feasibility report is analysis only. It does not implement event metadata ingestion, acquire data, run replay, modify validation, change candidate state, change qualification, alter governance, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, approve production integration, write memory automatically, or modify verified runtime truth.
