# Event Evidence Acquisition Plan 001

Date: 2026-06-05

Status: `GENERATED_ONLY`

Scope: minimum event metadata dataset definition for the current `EVENT_REQUIRED` candidate. No implementation, no downloads, no ingestion, no replay changes, no schema changes, no candidate changes, no qualification changes, no governance changes, no paper-forward changes, no trading authority, no broker authority, no capital allocation, and no position sizing.

## Inputs

- `research_journal/reports/event_metadata_requirements_001.md`
- `research_journal/reports/intraday_event_evidence_readiness_pack_001.md`

## Event Candidate

| Field | Value |
| --- | --- |
| event candidate id | `ptc_backtest_final_d5931b24bd391113` |
| mechanism | `EVENT_REACTION` |
| regime | `CHOP` |
| required timeframe | `30m` |
| symbols | `AMZN`, `BAC`, `META`, `MSFT`, `NFLX`, `TSLA` |
| current status | `EVENT_REQUIRED` |
| daily-data boundary | Daily data cannot validate event timing, reaction-window membership, pre/post-event ordering, or whether the move was event-specific. |

## Minimum Event Types Required

| Event Type | Required For MVP | Inclusion Rule |
| --- | --- | --- |
| Earnings releases | yes | Include when the release has a timestamp or reconstructable report timing. |
| Earnings calls or management guidance updates | yes | Include when call time or guidance timestamp can be tied to a 30m reaction window. |
| Guidance revisions, warnings, or preannouncements | yes | Include when same-day timestamp and symbol specificity are available. |
| Major company-specific news | yes | Include when release timestamp is minute-level or better and symbol-specific. |
| Analyst actions | conditional | Include only when timestamped and material enough to plausibly drive a 30m reaction. |
| Regulatory, litigation, product, executive, M&A, credit, or capital-return events | conditional | Include when timestamped, symbol-specific, and plausibly reaction-driving. |
| Broad market, index, or macro events | context only | Track as secondary context only unless the candidate is reframed away from single-stock event reaction. |

For `BAC`, macro-sensitive banking events may be context, but they are not primary event evidence unless the timestamp can be tied to `BAC` reaction timing.

## Symbols

The minimum event metadata dataset must cover every candidate symbol:

| Symbol | Event Metadata Required | 30m Intraday Bars Required |
| --- | --- | --- |
| `AMZN` | yes | yes |
| `BAC` | yes | yes |
| `META` | yes | yes |
| `MSFT` | yes | yes |
| `NFLX` | yes | yes |
| `TSLA` | yes | yes |

Validation must not rely on one symbol as a proxy for the full candidate.

## Timestamp Precision

| Event Class | Minimum Precision | Validation Use |
| --- | --- | --- |
| Scheduled earnings releases | minute-level preferred; report timing bucket acceptable only for acquisition triage | Event replay requires alignment to first eligible 30m bar. |
| Earnings calls | minute-level call start time preferred | Required when reaction window is tied to call commentary rather than release timestamp. |
| Guidance updates or preannouncements | minute-level or better | Required for event-window replay. |
| Unscheduled company-specific news | minute-level or better | Required for event-window replay. |
| Analyst actions | minute-level or better | Required for event-window replay if included. |
| Date-only events | insufficient for validation | Allowed only for backlog triage, not event-reaction replay. |
| Unknown timestamp events | insufficient for validation | Must be excluded from validation-aligned event samples. |

Every validation-eligible event needs:

- event timestamp in UTC
- event timestamp in `America/New_York`
- release session bucket: pre-market, regular session, after-hours, weekend/holiday, or unknown
- source publication timestamp
- first eligible 30m bar after the event
- confirmation whether the event was known before the reaction window

## Required Metadata Fields

Minimum event record:

| Field | Required | Notes |
| --- | --- | --- |
| `event_id` | yes | Stable event identifier. |
| `symbol` | yes | One of `AMZN`, `BAC`, `META`, `MSFT`, `NFLX`, `TSLA`. |
| `event_type` | yes | Earnings, guidance, news, analyst action, regulatory, litigation, product, executive, M&A, credit, capital return, or context. |
| `event_timestamp_utc` | yes | Validation-eligible records need reconstructable timestamp. |
| `event_timestamp_et` | yes | America/New_York timestamp. |
| `session_bucket` | yes | Pre-market, regular session, after-hours, weekend/holiday, or unknown. |
| `source_name` | yes | Provider, exchange, company IR, news source, or local artifact. |
| `source_url_or_artifact_id` | yes | URL or local artifact identifier. |
| `source_published_at_utc` | yes | Publication timestamp. |
| `source_retrieved_at_utc` | yes | Retrieval timestamp or local artifact timestamp. |
| `headline_or_short_description` | yes | Compact event description. |
| `materiality_class` | yes | Minimum categories: high, medium, low, unknown. |
| `scheduled_event` | yes | Boolean. |
| `known_before_reaction_window` | yes | Boolean or unknown; unknown is not validation-ready. |
| `reaction_window_start_utc` | yes | Derived from alignment rule. |
| `reaction_window_end_utc` | yes | Derived from alignment rule. |
| `aligned_bar_timeframe` | yes | Must be `30m` for this candidate. |
| `aligned_first_bar_start_utc` | yes | First eligible 30m bar start after event. |
| `aligned_first_bar_end_utc` | yes | First eligible 30m bar end after event. |
| `surprise_direction` | when available | For earnings, guidance, and analyst actions. |
| `expected_value` | when available | For earnings or metric-driven events. |
| `actual_value` | when available | For earnings or metric-driven events. |
| `revision_or_guidance_flag` | yes | Boolean. |
| `duplicate_event_group_id` | yes | Required to deduplicate multiple headlines about one catalyst. |
| `source_hash` | yes | Reconstructable source lineage. |
| `metadata_completeness_status` | yes | Minimum values: `VALIDATION_READY`, `TRIAGE_ONLY`, `EXCLUDED`. |

Additional earnings fields:

| Field | Required For Earnings | Notes |
| --- | --- | --- |
| `fiscal_quarter` | yes | Quarter associated with release. |
| `report_timing` | yes | Before open, during session, after close, or unknown. |
| `eps_expected` | when available | Expected EPS. |
| `eps_actual` | when available | Actual EPS. |
| `revenue_expected` | when available | Expected revenue. |
| `revenue_actual` | when available | Actual revenue. |
| `guidance_change_direction` | when available | Up, down, unchanged, mixed, unknown. |
| `call_start_time_utc` | when call-reaction window is used | Required when replay anchors to the call. |

## Source Options

Planning options only:

| Source Option | Use | Minimum Lineage Requirement | Current Action |
| --- | --- | --- | --- |
| Company investor-relations releases | Earnings, guidance, management updates | Source name, URL or local artifact id, publication timestamp, source hash | no downloads |
| Earnings calendar/provider records | Scheduled earnings dates and timing buckets | Provider name, retrieval timestamp, source hash | no downloads |
| Newswire or financial news timestamped items | Major company-specific news, preannouncements, regulatory/litigation/product/executive/M&A events | Source name, publication timestamp, source URL/artifact id, source hash | no downloads |
| Analyst action feeds or reports | Analyst upgrades/downgrades/price-target actions | Timestamp, firm/source, materiality label, source hash | no downloads |
| SEC/company filing timestamps | Regulatory, capital-return, M&A, credit, executive events | Filing timestamp, accession/artifact id, source hash | no downloads |
| Local research artifacts | Existing local event notes, if any | Artifact path, artifact timestamp, source hash | no ingestion |

Date-only sources are acquisition-triage sources only and are not validation-ready for this event-reaction candidate.

## Validation Alignment Rules

Minimum rules before event evidence can support validation:

1. Use only events with reconstructable timestamps.
2. Convert event timestamps to UTC and `America/New_York`.
3. Assign a session bucket before bar alignment.
4. Align every eligible event to the first eligible `30m` bar after the event timestamp.
5. Exclude date-only events from validation samples.
6. Exclude unknown-timestamp events from validation samples.
7. Exclude after-hours events from same-session regular-hours reaction unless the first eligible bar is explicitly defined.
8. Define the pre-event baseline window before computing reaction behavior.
9. Define the post-event reaction window in `30m` bars.
10. Separate event-day reaction from next-day drift.
11. Preserve `CHOP` regime labeling as a predeclared or bar-aligned input.
12. Deduplicate multiple headlines that refer to the same catalyst.
13. Prevent broad market moves from being counted as symbol-specific event reactions without symbol-specific event evidence.
14. Record excluded events and exclusion reasons.
15. Report event count, eligible event count, excluded count, sample size, and contribution by symbol and event type.
16. Compare event windows against non-event control windows.

## Intraday Dependency

Event metadata alone is not sufficient. The candidate also requires:

| Dependency | Minimum Requirement |
| --- | --- |
| Market bars | `30m` OHLCV for `AMZN`, `BAC`, `META`, `MSFT`, `NFLX`, and `TSLA`. |
| Expected bar count | 90,000-102,000 total 30m bars across the six symbols for the five-year range estimated in the readiness pack. |
| Timestamp handling | Timezone-normalized bars with explicit session date. |
| Session handling | Regular session, pre-market, after-hours, weekend/holiday, half-day, and early-close treatment where applicable. |
| Data quality | No duplicate bars for `(symbol, timeframe, timestamp)`; OHLCV fields present; OHLC invariants pass; no negative prices or volume. |
| Adjustment lineage | Raw versus adjusted mode documented; corporate-action method explicit. |
| Source lineage | Provider or local artifact lineage preserved from raw source through normalized validation-ready table. |

Daily data remains useful only for coarse coverage, broad sanity checks, next-day drift review, and event-day versus non-event-day comparison. It cannot validate the 30m event reaction claim.

## Minimum Viable Dataset

The minimum viable event evidence dataset is:

- 30m OHLCV coverage for all six symbols over the tested range.
- Timestamped event metadata for all six symbols over the same range.
- Event records include all required metadata fields listed above.
- Validation-eligible events have minute-level or reconstructable timestamps.
- Date-only and unknown-timestamp events are marked `TRIAGE_ONLY` or `EXCLUDED`.
- Each eligible event is aligned to a first eligible 30m bar.
- Exclusion reasons are recorded.
- Duplicate catalysts are grouped.
- Source lineage is reconstructable from event record to source artifact.

## Expected Validation Gain

| Measure | Value |
| --- | --- |
| Expected validation gain | MEDIUM |
| Basis | The readiness pack classifies `ptc_backtest_final_d5931b24bd391113` as `EVENT_REQUIRED` with medium expected validation gain. |
| Reason | Full daily coverage exists in later root-cause state, but daily proxy replay produced zero usable post-filter samples and cannot validate event timing. |
| Validation unlocked | Event-aware 30m replay readiness, if all metadata and intraday dependencies are satisfied. |
| Validation not unlocked | Candidate promotion, qualification, governance approval, paper-ready status, trading authority, broker execution, capital allocation, or position sizing. |

## Acceptance Boundary

The dataset is event-replay-ready only when:

- all six symbols have 30m bar coverage over the tested range
- all six symbols have timestamped event metadata for the same range
- each validation-eligible event can align to a 30m bar without date-only inference
- event records include required metadata fields
- source lineage is reconstructable
- daily-only proxy results remain separated from event-aware replay results

## Authority Boundary

This plan does not implement ingestion, download events, download bars, run replay, alter validation, promote candidates, qualify candidates, change governance, create paper observations, recommend trades, allocate capital, authorize broker execution, size positions, or write memory automatically.
