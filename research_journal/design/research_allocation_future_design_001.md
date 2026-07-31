# Research Allocation Future Design 001

Status: DESIGN ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: future architecture documentation for research allocation in Atlas. This document does not implement software, alter runtime truth, modify Aegis behavior, create candidates, approve paper positions, allocate capital, provide trade advice, or authorize live activity.

Runtime truth note: if this design is ever implemented, it must query verified runtime truth. No consumer may infer readiness from this document, code shape, journal text, or generated output.

## Core Thesis

Atlas should treat research attention as scarce. The Research Scientist can generate and inspect hypotheses, but the long-term system needs an explicit allocation function that decides where research hours should be spent.

Research Allocation is not capital allocation. It allocates research effort, review bandwidth, compute budget, and validation priority. It must never imply trade authority or runtime readiness.

## Allocation Unit

A future allocation decision should assign bounded research effort to a typed work item.

Potential allocation units:

- mechanism family
- hypothesis cluster
- replay task
- backtest task
- uncertainty repair task
- retirement review
- failure-pattern review
- source-quality review
- search-space compression task

Each allocation should define:

- work item id
- allowed research activity
- maximum research hours
- required evidence output
- expected information gain
- opportunity cost
- stop condition
- review owner
- forbidden uses

## Allocation Inputs

Future allocation should consider:

- evidence level
- uncertainty severity
- prior duplicate history
- retirement proximity
- expected information gain
- estimated research hours
- downstream validation cost
- source reliability
- mechanism novelty
- regime relevance
- strategic coverage gaps
- research debt impact

Inputs must be evidence-linked. Generated attractiveness alone should not justify allocation.

## Allocation Classes

Suggested allocation classes:

| Class | Meaning |
| --- | --- |
| `EXPLORE` | Low-cost investigation to clarify a weak or new idea. |
| `VALIDATE` | Evidence-building work for a hypothesis with sufficient initial support. |
| `REPAIR` | Work to fix lineage, uncertainty, source, or schema gaps. |
| `RETIRE` | Work intended to decide whether an object should be retired. |
| `COMPRESS` | Work that reduces duplicate or low-value search space. |
| `HOLD` | No current work because evidence, regime, or priority is insufficient. |
| `REJECT` | No further work unless a material difference appears. |

## Scoring Model

A future allocation score should be transparent and auditable.

Indicative scoring components:

- `expected_information_gain`
- `evidence_gap_importance`
- `uncertainty_resolution_value`
- `research_cost`
- `duplication_penalty`
- `retirement_risk`
- `source_quality`
- `regime_relevance`
- `strategic_coverage_value`
- `false_positive_risk`

The score should explain the decision. It should not be an opaque priority number.

## Future Metrics

Research Allocation should own or consume these metrics:

| Metric | Allocation interpretation |
| --- | --- |
| Candidate conversion rate | Measures how often allocated research produces valid candidate inputs. |
| Replay support rate | Measures whether replay allocations are being directed to plausible hypotheses. |
| Backtest support rate | Measures whether higher-cost validation work is being selected well. |
| Paper-forward survival | Measures whether allocated research produces hypotheses that survive future observation. |
| Hypothesis rejection speed | Measures how efficiently allocation identifies work that should stop. |
| False positive elimination rate | Measures how well allocation prevents expensive validation of weak ideas. |
| Research hours saved | Measures effort avoided by choosing dedupe, retirement, or rejection. |
| Information gain per research hour | Primary allocation efficiency metric. |
| Research debt reduction | Measures whether allocated work lowers stale or unresolved obligations. |
| Search-space compression | Measures how much allocation narrows future work without losing useful scope. |

## Stop Conditions

Every allocation should define stop conditions before work begins.

Examples:

- replay fails minimum support
- uncertainty object remains unresolved after budget
- duplicate prior work is found
- source lineage is insufficient
- hypothesis becomes regime-inapplicable
- cost exceeds expected information gain
- retirement threshold is met

## Operating Principle

Research Allocation should spend research hours where they buy the most durable reduction in uncertainty. The best allocation may be rejection, retirement, or compression rather than more experimentation.

