# Event Metadata Requirements 001

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: research-only, offline-only

## Purpose

Define the event metadata needed before event-reaction candidates can be validated beyond daily proxy replay.

Focus candidate:

- `ptc_backtest_final_d5931b24bd391113`

This report does not implement ingestion, replay, schema changes, candidate promotion, paper placement, qualification, governance, capital allocation, trade advice, broker execution, or memory writes.

## Source Context

Local evidence reviewed:

- `research_journal/reports/insufficient_data_root_cause_analysis_001.md`
- `reports/atlas_v2_research_os/candidate_validation_queue_001.md`
- `reports/atlas_v2_research_os/market_data_readiness/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- latest verified runtime graph: `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-05/verified_runtime_graph.v1.json`

Current verified runtime state is blocked. This report must not be used to infer runtime readiness.

## Candidate Summary

| Field | Value |
| --- | --- |
| candidate id | `ptc_backtest_final_d5931b24bd391113` |
| mechanism | `EVENT_REACTION` |
| regime | `CHOP` |
| timeframe | `30m` |
| candidate symbols | `AMZN`, `BAC`, `META`, `MSFT`, `NFLX`, `TSLA` |
| current daily coverage | full daily symbol coverage reported for 6/6 symbols |
| current validation state | insufficient |
| known blocker | no event metadata or 30m event-timing coverage evidenced |

The existing root-cause report says daily proxy replay ran on `AMZN` but produced `sample_size=0` after trigger/regime filtering, with 85 triggered samples filtered. That makes the candidate a metadata and replay-alignment problem, not a missing-daily-symbol problem.

## Required Event Types

The minimum event catalog for this candidate must include scheduled, timestamped events that plausibly create single-stock event reactions:

- earnings releases
- earnings calls or management guidance updates
- same-day guidance revisions, warnings, or preannouncements
- major company-specific news events with timestamped release time
- analyst actions only when timestamped and material enough to plausibly drive a 30m reaction
- regulatory, litigation, product, executive, M&A, credit, or capital-return events when timestamped and symbol-specific

For `BAC`, macro-sensitive banking events may be secondary context, but they are not enough unless the event can be tied to `BAC` reaction timing. Broad market, index, or macro events should be tracked as context, not as the primary event, unless the candidate is explicitly reframed away from single-stock event reaction.

## Candidate Symbols

Required symbol universe:

- `AMZN`
- `BAC`
- `META`
- `MSFT`
- `NFLX`
- `TSLA`

Validation must not rely on one symbol as a proxy for the whole candidate. Each symbol needs event metadata and aligned 30m market bars for the same tested date range.

## Event Timestamps Needed

Each event must have a reconstructable timestamp precise enough to align with 30m bars:

- event timestamp in UTC
- local exchange timestamp in `America/New_York`
- release session bucket: pre-market, regular session, after-hours, weekend/holiday, unknown
- source publication timestamp
- effective reaction anchor timestamp, if different from source publication time
- first eligible 30m bar open after the event
- embargo or scheduled release time when available
- confirmation whether the timestamp was known before the reaction window

Timestamp precision requirements:

- scheduled earnings and macro-linked events: minute-level timestamp preferred; session bucket is acceptable only for coarse triage, not validation
- unscheduled news: source timestamp must be minute-level or better
- unknown or date-only events: allowed for backlog triage only; not valid for event-reaction replay

## Minimum Metadata Fields

Each event record needs:

- `event_id`
- `symbol`
- `event_type`
- `event_timestamp_utc`
- `event_timestamp_et`
- `session_bucket`
- `source_name`
- `source_url_or_artifact_id`
- `source_published_at_utc`
- `source_retrieved_at_utc`
- `headline_or_short_description`
- `materiality_class`
- `scheduled_event`
- `known_before_reaction_window`
- `reaction_window_start_utc`
- `reaction_window_end_utc`
- `aligned_bar_timeframe`
- `aligned_first_bar_start_utc`
- `aligned_first_bar_end_utc`
- `surprise_direction`, when available
- `expected_value`, when available
- `actual_value`, when available
- `revision_or_guidance_flag`
- `duplicate_event_group_id`, when events refer to the same catalyst
- `source_hash`
- `metadata_completeness_status`

For earnings events, add:

- fiscal quarter
- report timing: before open, during session, after close
- EPS expected and actual, when available
- revenue expected and actual, when available
- guidance change direction, when available
- call start time, if the reaction window is tied to the call rather than the release

## Replay Alignment Requirements

Replay must bind events to 30m bars before evaluating the candidate:

- use only events with reconstructable timestamps
- align every event to the first eligible 30m bar after the event timestamp
- exclude after-hours events from same-session regular-hours reaction unless the first eligible bar is explicitly defined
- define pre-event baseline window before computing reaction behavior
- define post-event reaction window in 30m bars
- separate event-day reaction from next-day drift
- preserve `CHOP` regime labeling as a predeclared input, not an after-the-fact filter
- deduplicate multiple headlines that refer to one catalyst
- prevent broad market moves from being counted as symbol-specific event reaction without symbol-specific event evidence
- record excluded events and exclusion reasons

Minimum replay outputs needed:

- event count by symbol
- eligible event count after metadata filters
- excluded event count by reason
- sample size by symbol
- sample size by event type
- expectancy and hit rate by event type
- symbol-level contribution, not only aggregate result
- sensitivity to reaction-window length
- comparison against non-event control windows

## Daily Data Validation Boundary

Daily data alone cannot validate `ptc_backtest_final_d5931b24bd391113` as an event-reaction candidate.

Daily data can help with:

- first-pass symbol coverage
- broad sanity checks
- next-day drift after known events
- coarse event-day versus non-event-day comparison

Daily data cannot prove:

- whether the move began before or after the event timestamp
- whether the reaction occurred inside the intended 30m window
- whether a pre-market or after-hours event should map to the same day or next session
- whether the move is event reaction rather than ordinary daily volatility
- whether the `CHOP` regime was known before the event reaction

For this candidate, daily coverage is already not the limiting factor. The limiting factors are missing event metadata, missing 30m event-timing data, and event-aware replay logic.

## Intraday And Event-Aware Replay Requirement

Intraday/event-aware replay is mandatory for validation of this candidate.

Minimum required replay layer:

- 30m bars for `AMZN`, `BAC`, `META`, `MSFT`, `NFLX`, and `TSLA`
- timestamped event metadata for each symbol
- deterministic event-to-bar alignment
- event eligibility filter
- event-type stratification
- predeclared reaction window
- predeclared regime filter
- non-event control comparison

Without those pieces, the candidate may remain a research backlog item or generated-only hypothesis, but it should not be described as validated, qualified, paper-ready, or governance-ready.

## Acceptance Rule

`ptc_backtest_final_d5931b24bd391113` can move from event-metadata-blocked to event-replay-ready only when:

- all six symbols have 30m bar coverage over the tested range
- all six symbols have timestamped event metadata for the same range
- event records include the minimum metadata fields above
- replay can align each eligible event to a 30m bar without date-only inference
- replay reports sample counts by symbol and event type
- daily-only proxy results are explicitly separated from event-aware replay results
- source lineage is reconstructable from event metadata to replay output

## Authority Boundary

This report has no authority to:

- create or promote candidates
- approve replay, qualification, certification, or governance
- allocate capital or size positions
- recommend trades
- execute through a broker
- write memory automatically

Consumers must query verified truth and must not infer readiness from this report.
