# Research Retirement Future Design 001

Status: DESIGN ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: future architecture documentation for research retirement in Atlas. This document does not implement software, alter runtime truth, modify Aegis behavior, create candidates, approve paper positions, allocate capital, provide trade advice, or authorize live activity.

Runtime truth note: if this design is ever implemented, it must query verified runtime truth. No consumer may infer readiness from this document, code shape, journal text, or generated output.

## Core Thesis

Atlas must value negative knowledge. A useful Research Scientist can produce hypotheses, but a mature research system must retire weak, duplicate, stale, falsified, or economically unattractive ideas before they consume repeated attention.

Research Retirement is a first-class architecture concern. It prevents search-space bloat, repeated false positives, and the quiet accumulation of research debt.

## Retirement Objects

Future retirement should operate on typed objects.

Potential retirement targets:

- claim
- hypothesis
- mechanism
- experiment design
- replay task
- backtest task
- source
- evidence pattern
- uncertainty assumption
- research pipeline

Each retirement record should include:

- retired object id
- object type
- retirement reason
- retirement scope
- evidence basis
- uncertainty basis
- affected regimes
- affected mechanism families
- duplicate links
- reopen conditions
- forbidden uses
- review due date, if any

## Retirement Reasons

Suggested reason codes:

- `DUPLICATE`
- `STALE`
- `FALSIFIED`
- `WEAK_REPLAY_SUPPORT`
- `WEAK_BACKTEST_SUPPORT`
- `PAPER_FORWARD_FAILURE`
- `SOURCE_UNRELIABLE`
- `LINEAGE_BROKEN`
- `UNCERTAINTY_UNRESOLVED`
- `LOW_INFORMATION_GAIN`
- `HIGH_RESEARCH_COST`
- `REGIME_TOO_NARROW`
- `FALSE_POSITIVE_PATTERN`
- `SUPERSEDED_BY_STRONGER_KNOWLEDGE`

Retirement must be scoped. A mechanism may be retired for one regime or source family without being globally retired.

## Reopening Policy

Retired work should be reopenable only through explicit evidence.

Reopening requirements:

- reference to the retirement record
- material difference from the retired scope
- new evidence or new regime context
- explanation of why prior failure may not apply
- bounded research budget
- new uncertainty object
- review due date

Reopening creates a new research question. It does not restore prior confidence.

## Relationship To Search-Space Compression

Retirement should feed search-space compression by removing or suppressing:

- duplicate wording variants
- unsupported parameter variants
- repeatedly weak mechanism variants
- stale source-derived claims
- high-cost low-information-gain branches
- hypotheses with unresolved structural uncertainty

Compression must preserve lineage to the retired objects so future workers can understand why an area was narrowed.

## Future Metrics

Research Retirement should own or influence these metrics:

| Metric | Retirement interpretation |
| --- | --- |
| Candidate conversion rate | Should improve when weak inputs are retired before candidate consideration. |
| Replay support rate | Should improve as duplicate and low-quality hypotheses are retired earlier. |
| Backtest support rate | Should improve when replay-weak work stops before backtest cost. |
| Paper-forward survival | Should improve when paper-forward work receives less false positive input. |
| Hypothesis rejection speed | Primary retirement speed metric. |
| False positive elimination rate | Primary retirement quality metric. |
| Research hours saved | Measures avoided repeated investigation of retired work. |
| Information gain per research hour | Should rise as low-gain branches are removed. |
| Research debt reduction | Primary retirement debt metric. |
| Search-space compression | Measures how much future research space retirement removes or suppresses. |

## Audit Requirements

Every retirement should be auditable:

- what was retired
- why it was retired
- what evidence supported retirement
- what uses are prohibited
- what scope remains untested
- what would justify reopening
- who or what made the decision
- when the decision expires or requires review

## Operating Principle

Research Retirement should make "do not spend more time here" a durable, evidence-linked research output. Avoided work is a form of learning when the reason is explicit and reviewable.

