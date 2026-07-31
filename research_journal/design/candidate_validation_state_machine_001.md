# Candidate Validation State Machine 001

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: evaluation-only, offline-only

## Purpose

Formalize candidate validation states for direct candidate validation review.

The state machine separates:

- attribution readiness
- data availability
- replay feasibility
- evaluation sufficiency
- validation outcome
- unresolved external requirements
- hard blockers

This design is intended to prevent ambiguous validation language such as treating a candidate with local data but zero surviving samples as simply "not validated" without naming the failed state boundary.

This document does not implement a state machine, change replay behavior, change validation behavior, promote candidates, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.

## State List

Required states:

- `UNATTRIBUTED`
- `ATTRIBUTED`
- `DATA_AVAILABLE`
- `REPLAYABLE`
- `EVALUABLE`
- `CONFIRMED`
- `WEAKENED`
- `INSUFFICIENT_DATA`
- `INTRADAY_REQUIRED`
- `EVENT_REQUIRED`
- `BLOCKED`

## State Categories

Progressive states:

- `UNATTRIBUTED`
- `ATTRIBUTED`
- `DATA_AVAILABLE`
- `REPLAYABLE`
- `EVALUABLE`

Outcome states:

- `CONFIRMED`
- `WEAKENED`
- `INSUFFICIENT_DATA`

Requirement states:

- `INTRADAY_REQUIRED`
- `EVENT_REQUIRED`

Blocker state:

- `BLOCKED`

Requirement states are not positive or negative validation outcomes. They identify missing validation prerequisites that must be satisfied before replay can be interpreted.

## Allowed Transition Summary

| From | Allowed next states |
| --- | --- |
| `UNATTRIBUTED` | `ATTRIBUTED`, `BLOCKED` |
| `ATTRIBUTED` | `DATA_AVAILABLE`, `INTRADAY_REQUIRED`, `EVENT_REQUIRED`, `INSUFFICIENT_DATA`, `BLOCKED` |
| `DATA_AVAILABLE` | `REPLAYABLE`, `INTRADAY_REQUIRED`, `EVENT_REQUIRED`, `INSUFFICIENT_DATA`, `BLOCKED` |
| `REPLAYABLE` | `EVALUABLE`, `INTRADAY_REQUIRED`, `EVENT_REQUIRED`, `INSUFFICIENT_DATA`, `BLOCKED` |
| `EVALUABLE` | `CONFIRMED`, `WEAKENED`, `INSUFFICIENT_DATA`, `BLOCKED` |
| `INTRADAY_REQUIRED` | `DATA_AVAILABLE`, `REPLAYABLE`, `BLOCKED` |
| `EVENT_REQUIRED` | `DATA_AVAILABLE`, `REPLAYABLE`, `BLOCKED` |
| `INSUFFICIENT_DATA` | `ATTRIBUTED`, `DATA_AVAILABLE`, `REPLAYABLE`, `INTRADAY_REQUIRED`, `EVENT_REQUIRED`, `BLOCKED` |
| `CONFIRMED` | `WEAKENED`, `INSUFFICIENT_DATA`, `BLOCKED` |
| `WEAKENED` | `CONFIRMED`, `INSUFFICIENT_DATA`, `BLOCKED` |
| `BLOCKED` | `UNATTRIBUTED`, `ATTRIBUTED`, `DATA_AVAILABLE`, `REPLAYABLE`, `EVALUABLE` |

No state transition grants trading, capital, broker, paper placement, production promotion, qualification, governance, or readiness authority.

## State Definitions

### `UNATTRIBUTED`

Entry conditions:

- Candidate exists in a validation input set.
- No validated symbol, universe, source observation lineage, or required instrument context is available.
- Any symbols present are missing, ambiguous, contradictory, or not traceable to the candidate source.

Exit conditions:

- Candidate receives traceable symbol or universe attribution.
- Attribution confidence and method are recorded.
- If attribution cannot be resolved, candidate exits to `BLOCKED`.

Required evidence:

- candidate id
- source artifact or observation id
- attempted attribution fields
- reason attribution is missing or ambiguous

Forbidden transitions:

- direct transition to `DATA_AVAILABLE`
- direct transition to `REPLAYABLE`
- direct transition to `EVALUABLE`
- direct transition to `CONFIRMED`
- direct transition to `WEAKENED`
- direct transition to `INTRADAY_REQUIRED` or `EVENT_REQUIRED` without first identifying the candidate context

### `ATTRIBUTED`

Entry conditions:

- Candidate has resolved symbol, universe, or instrument attribution.
- Attribution source and confidence are recorded.
- Candidate mechanism and declared regime/timeframe are available or explicitly marked unknown.

Exit conditions:

- Required local data exists for the attributed symbol or universe, allowing transition to `DATA_AVAILABLE`.
- Required data type is missing, allowing transition to `INTRADAY_REQUIRED`, `EVENT_REQUIRED`, or `INSUFFICIENT_DATA`.
- Attribution is later found invalid, allowing transition to `BLOCKED`.

Required evidence:

- resolved symbol or universe
- attribution method
- attribution confidence
- source observation lineage
- mechanism
- regime and timeframe fields, including unknown markers when applicable

Forbidden transitions:

- direct transition to `CONFIRMED`
- direct transition to `WEAKENED`
- direct transition to `EVALUABLE`
- direct transition to `REPLAYABLE` without data availability evidence
- transition to `DATA_AVAILABLE` using proxy data without marking the proxy boundary

### `DATA_AVAILABLE`

Entry conditions:

- Required local market data exists for at least one attributed symbol or the declared universe.
- Data is loadable and normalized into the required bar schema or event schema.
- Missing-data notes, proxy fallback, and timeframe mismatch notes are recorded.

Exit conditions:

- Deterministic replay trigger can be generated, allowing transition to `REPLAYABLE`.
- Required intraday bars are unavailable, allowing transition to `INTRADAY_REQUIRED`.
- Required event metadata is unavailable, allowing transition to `EVENT_REQUIRED`.
- Data exists but is insufficient, stale, non-reconstructable, or mismatched, allowing transition to `INSUFFICIENT_DATA` or `BLOCKED`.

Required evidence:

- data file or source reference
- symbol coverage
- timeframe coverage
- lookback coverage
- schema normalization status
- proxy or fallback status
- missing-data notes

Forbidden transitions:

- direct transition to `CONFIRMED`
- direct transition to `WEAKENED`
- transition to `EVALUABLE` without replay samples
- transition to `REPLAYABLE` when trigger generation is undefined
- treating daily proxy data as intraday validation without explicit `INTRADAY_REQUIRED` handling

### `REPLAYABLE`

Entry conditions:

- Candidate has attribution and data availability.
- Mechanism has a deterministic replay trigger.
- Trigger generation can run over the available data.
- Regime and timeframe constraints are either compatible, explicitly bridged, or explicitly unconstrained.

Exit conditions:

- Replay produces surviving samples and metrics, allowing transition to `EVALUABLE`.
- Trigger samples exist but filters remove too many samples, allowing transition to `INSUFFICIENT_DATA`.
- Replay requires intraday data, allowing transition to `INTRADAY_REQUIRED`.
- Replay requires event metadata, allowing transition to `EVENT_REQUIRED`.
- Replay cannot run because of an invalid spec, incompatible data, or missing verified prerequisite, allowing transition to `BLOCKED`.

Required evidence:

- trigger definition
- trigger sample count
- regime filter outcome
- timeframe compatibility outcome
- replay run status
- warnings
- missing-data messages

Forbidden transitions:

- direct transition to `CONFIRMED` without metric evaluation
- direct transition to `WEAKENED` without metric evaluation
- transition to `EVALUABLE` with zero surviving samples
- silent transition to `INSUFFICIENT_DATA` when the real cause is unmappable vocabulary, intraday requirement, or event requirement

### `EVALUABLE`

Entry conditions:

- Replay ran successfully.
- Surviving sample count meets the minimum sample threshold.
- Required metrics are computed.
- Missing-data and warning fields do not invalidate metric interpretation.

Exit conditions:

- Metrics meet confirmation thresholds, allowing transition to `CONFIRMED`.
- Metrics fail support thresholds while still being interpretable, allowing transition to `WEAKENED`.
- Metrics are not interpretable after review, allowing transition to `INSUFFICIENT_DATA`.
- Evaluation is invalidated by authority, lineage, or evidence failure, allowing transition to `BLOCKED`.

Required evidence:

- final sample count
- expectancy
- profit factor
- maximum drawdown
- minimum sample threshold
- classification thresholds
- direct versus proxy comparison when available
- validation warnings

Forbidden transitions:

- transition to `CONFIRMED` on proxy metrics alone
- transition to `CONFIRMED` with insufficient surviving samples
- transition to `WEAKENED` when metrics are missing
- transition to any trading, capital, paper placement, or production state

### `CONFIRMED`

Entry conditions:

- Candidate was `EVALUABLE`.
- Direct validation metrics meet confirmation thresholds.
- Required sample size and data requirements are satisfied.
- No unresolved blocker invalidates the result.

Exit conditions:

- Later direct evidence weakens the candidate, allowing transition to `WEAKENED`.
- Later data or lineage review invalidates sufficiency, allowing transition to `INSUFFICIENT_DATA`.
- Governance, authority, artifact integrity, or verified-truth failure invalidates use of the result, allowing transition to `BLOCKED`.

Required evidence:

- direct validation result
- computed metrics
- sample count
- threshold comparison
- data and attribution lineage
- authority boundary statement

Forbidden transitions:

- transition from `UNATTRIBUTED`
- transition from `ATTRIBUTED`
- transition from `DATA_AVAILABLE`
- transition from `REPLAYABLE` without `EVALUABLE`
- treating `CONFIRMED` as candidate promotion, qualification, governance approval, trade recommendation, paper-trade authorization, or capital authority

### `WEAKENED`

Entry conditions:

- Candidate was `EVALUABLE`.
- Direct validation metrics are interpretable but fail support thresholds.
- Evidence indicates the candidate hypothesis is weaker than proxy or prior support implied.

Exit conditions:

- Later sufficient direct evidence meets confirmation thresholds, allowing transition to `CONFIRMED`.
- Later review determines evidence is not interpretable, allowing transition to `INSUFFICIENT_DATA`.
- Governance, authority, artifact integrity, or verified-truth failure invalidates use of the result, allowing transition to `BLOCKED`.

Required evidence:

- direct validation result
- computed metrics
- sample count
- threshold comparison
- proxy versus direct comparison when available
- weakening rationale

Forbidden transitions:

- transition from `UNATTRIBUTED`
- transition from `ATTRIBUTED`
- transition from `DATA_AVAILABLE`
- transition from `REPLAYABLE` without `EVALUABLE`
- treating `WEAKENED` as falsification unless the falsification criteria are separately defined and met

### `INSUFFICIENT_DATA`

Entry conditions:

- Candidate has some validation context, but evidence is not enough for an interpretable direct result.
- Typical causes include missing local data, too few surviving samples, missing metrics, invalid proxy-only support, stale data, or non-reconstructable evidence.
- This state may occur after attribution, data loading, replay, or evaluation.

Exit conditions:

- Attribution is repaired, allowing transition to `ATTRIBUTED`.
- Required data is supplied, allowing transition to `DATA_AVAILABLE`.
- Replay prerequisites are supplied, allowing transition to `REPLAYABLE`.
- Intraday requirement is identified, allowing transition to `INTRADAY_REQUIRED`.
- Event metadata requirement is identified, allowing transition to `EVENT_REQUIRED`.
- A hard blocker is discovered, allowing transition to `BLOCKED`.

Required evidence:

- insufficiency reason
- minimum required evidence
- observed evidence
- failed threshold or missing field
- next needed artifact or data type
- distinction between missing data, zero surviving samples, and unmappable validation vocabulary

Forbidden transitions:

- direct transition to `CONFIRMED`
- direct transition to `WEAKENED`
- use as a generic bucket when `INTRADAY_REQUIRED`, `EVENT_REQUIRED`, or `BLOCKED` is more precise
- treating proxy support as enough to exit to an outcome state

### `INTRADAY_REQUIRED`

Entry conditions:

- Candidate validation requires intraday bars, session windows, opening-range behavior, event-window behavior, or same-day sequence evidence.
- Available daily data cannot answer the mechanism or timeframe requirement.
- The intraday requirement is explicit in the candidate plan, mechanism definition, or validation trace.

Exit conditions:

- Required intraday data becomes available and loadable, allowing transition to `DATA_AVAILABLE` or `REPLAYABLE`.
- Requirement is formally rescoped to daily-compatible validation with explicit caveats, allowing transition to `DATA_AVAILABLE`.
- Required intraday source cannot be obtained or verified, allowing transition to `BLOCKED`.

Required evidence:

- required intraday timeframe
- mechanism reason intraday data is needed
- available data timeframe
- missing intraday source or file
- timestamp and session requirements

Forbidden transitions:

- direct transition to `CONFIRMED`
- direct transition to `WEAKENED`
- treating daily proxy replay as sufficient intraday validation
- silently collapsing into `INSUFFICIENT_DATA` without preserving the intraday requirement

### `EVENT_REQUIRED`

Entry conditions:

- Candidate validation depends on event timestamps, event type, event source, news context, earnings context, macro release context, or other event metadata.
- Available OHLCV data alone cannot distinguish event reaction from ordinary volatility or trend behavior.
- Event requirement is explicit in the candidate mechanism, plan, or validation trace.

Exit conditions:

- Required event metadata becomes available and traceable, allowing transition to `DATA_AVAILABLE` or `REPLAYABLE`.
- Mechanism is formally rescoped to a non-event validation with explicit caveats, allowing transition to `DATA_AVAILABLE`.
- Event source cannot be obtained or verified, allowing transition to `BLOCKED`.

Required evidence:

- event type
- event timestamp or window
- event source
- affected symbols
- required matching or control condition
- reason OHLCV-only replay is insufficient

Forbidden transitions:

- direct transition to `CONFIRMED`
- direct transition to `WEAKENED`
- interpreting large price movement as event evidence without event metadata
- silently collapsing into `INSUFFICIENT_DATA` without preserving the event requirement

### `BLOCKED`

Entry conditions:

- Validation cannot proceed because a required source, artifact, authority boundary, verified-truth dependency, data integrity condition, or governance prerequisite is unavailable or failed.
- The blocker is not merely low sample size; it prevents valid validation work.

Exit conditions:

- Blocker is resolved and the candidate re-enters the earliest valid state supported by evidence.
- If attribution was never established, exit to `UNATTRIBUTED`.
- If attribution exists but data is missing, exit to `ATTRIBUTED`.
- If data exists but replay prerequisites remain unresolved, exit to `DATA_AVAILABLE` or `REPLAYABLE` as supported by evidence.

Required evidence:

- blocker code or reason
- blocked prerequisite
- source artifact or dependency
- resolution condition
- authority boundary affected, if any
- evidence showing why another state would be misleading

Forbidden transitions:

- direct transition to `CONFIRMED`
- direct transition to `WEAKENED`
- using `BLOCKED` for ordinary weak metrics
- using `BLOCKED` for ordinary sample insufficiency
- exiting `BLOCKED` without naming the resolved prerequisite and target state evidence

## Terminal Outcome Rules

`CONFIRMED` and `WEAKENED` are validation outcomes only after `EVALUABLE`.

`INSUFFICIENT_DATA` is not a negative result. It means the evidence cannot support an interpretable validation outcome.

`INTRADAY_REQUIRED` and `EVENT_REQUIRED` are prerequisite states. They should be preferred over `INSUFFICIENT_DATA` when the missing evidence type is known.

`BLOCKED` is reserved for hard blockers. It should not be used as a synonym for insufficient samples.

## Evidence Precedence

When multiple states appear plausible, choose the earliest precise state in this order:

1. `BLOCKED`, if a hard prerequisite, governance, authority, artifact integrity, or verified-truth dependency prevents validation.
2. `UNATTRIBUTED`, if candidate symbol or universe lineage is unresolved.
3. `INTRADAY_REQUIRED`, if intraday data is explicitly required and unavailable.
4. `EVENT_REQUIRED`, if event metadata is explicitly required and unavailable.
5. `INSUFFICIENT_DATA`, if evidence exists but is below threshold or not interpretable.
6. `EVALUABLE`, if replay metrics and sample sufficiency are present.
7. `CONFIRMED` or `WEAKENED`, if evaluable metrics cross outcome thresholds.

This precedence prevents consumers from inventing readiness from later-stage artifacts while earlier prerequisites remain unresolved.

## Current Direct Validation Mapping

Observed current direct validation cases map as follows:

| Observed condition | State |
| --- | --- |
| no resolved symbol or universe | `UNATTRIBUTED` |
| resolved symbols with no local data | `ATTRIBUTED` or `INSUFFICIENT_DATA`, depending on whether the missing data requirement is specific |
| local data exists but no deterministic trigger | `DATA_AVAILABLE` or `BLOCKED`, depending on whether the missing trigger is a design gap or invalid spec |
| trigger samples exist but regime vocabulary eliminates all samples | `INSUFFICIENT_DATA`, with required vocabulary-compatibility reason |
| daily data exists but candidate requires intraday confirmation | `INTRADAY_REQUIRED` |
| event reaction candidate lacks event metadata | `EVENT_REQUIRED` |
| replay samples and metrics exist but sample count is too low | `INSUFFICIENT_DATA` |
| replay samples and metrics satisfy support thresholds | `CONFIRMED` |
| replay samples and metrics fail support thresholds | `WEAKENED` |

## Forbidden Global Transitions

The following transitions are forbidden for every state:

- any transition to live trading authority
- any transition to broker execution authority
- any transition to capital allocation authority
- any transition to position sizing authority
- any transition to automatic paper trade placement
- any transition to candidate production promotion
- any transition that treats proxy replay as direct confirmation
- any transition that infers readiness from code shape, generated text, or journal narrative instead of verified evidence
- any transition that hides missing intraday or event requirements inside generic `INSUFFICIENT_DATA`

## Authority Boundary

This document is design only. It does not implement validation states, modify Aegis behavior, modify manifests, alter verified runtime truth, promote candidates, override replay, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.

Any future implementation must follow Aegis runtime truth rules, update affected manifests and tests, and pass audit before being treated as runtime behavior.
