# Uncertainty Objects Future Design 001

Status: DESIGN ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: future architecture documentation for uncertainty objects in Atlas. This document does not implement software, alter runtime truth, modify Aegis behavior, create candidates, approve paper positions, allocate capital, provide trade advice, or authorize live activity.

Runtime truth note: if this design is ever implemented, it must query verified runtime truth. No consumer may infer readiness from this document, code shape, journal text, or generated output.

## Core Thesis

Atlas should not treat uncertainty as missing prose. It should model uncertainty as a first-class object that can be routed, measured, reduced, retired, or used to stop work.

The Research Scientist can describe uncertainty inside a report. The future Atlas center of gravity should require structured uncertainty objects before research is allocated, replayed, backtested, or advanced.

## Object Definition

An `UncertaintyObject` should represent a specific unresolved question that affects research confidence, cost, or admissibility.

Required future fields:

- uncertainty id
- linked object id
- linked object type
- uncertainty type
- uncertainty statement
- affected claim or mechanism
- severity
- reducibility
- expected information gain if resolved
- estimated research hours to resolve
- evidence needed
- current evidence
- decision impact
- allowed uses while unresolved
- forbidden uses while unresolved
- review due date

## Uncertainty Types

Suggested types:

- `SOURCE_LINEAGE`
- `DATA_QUALITY`
- `TIMESTAMP_DISCIPLINE`
- `REGIME_SCOPE`
- `MECHANISM_CLARITY`
- `DUPLICATE_RISK`
- `SAMPLE_SIZE`
- `REPLAY_VALIDITY`
- `BACKTEST_VALIDITY`
- `PAPER_FORWARD_VALIDITY`
- `EXTERNAL_VALIDITY`
- `FALSE_POSITIVE_RISK`
- `ECONOMIC_VALUE`
- `IMPLEMENTATION_GAP`
- `AUTHORITY_BOUNDARY`

## Severity And Reducibility

Severity should describe decision impact:

| Severity | Meaning |
| --- | --- |
| `LOW` | Does not block current research stage. |
| `MEDIUM` | Allows limited work but blocks advancement. |
| `HIGH` | Blocks expensive validation or paper-forward progression. |
| `CRITICAL` | Blocks consumption except audit or repair. |

Reducibility should describe whether research can lower the uncertainty:

| Reducibility | Meaning |
| --- | --- |
| `REDUCIBLE_NOW` | Existing data or review can resolve it. |
| `REDUCIBLE_LATER` | Requires future observations or unavailable context. |
| `PARTIALLY_REDUCIBLE` | Can be narrowed but not removed. |
| `IRREDUCIBLE` | Must be accepted, scoped, or used as a reason to stop. |

## Decision Impact

Uncertainty objects should influence research decisions explicitly.

Possible impacts:

- allow cheap exploration
- require replay before further work
- require backtest before paper-forward observation
- block progression
- trigger source repair
- trigger duplicate search
- trigger retirement review
- reduce allocation priority
- increase required sample size
- restrict regime scope

## Relationship To Research Economics

Each uncertainty object should estimate:

- research hours required to reduce it
- expected information gain
- probability that resolution changes the decision
- downstream cost avoided if resolved early

This allows Atlas to reject uncertainty repair when the cost is higher than the expected learning value.

## Future Metrics

Uncertainty Objects should support these metrics:

| Metric | Uncertainty interpretation |
| --- | --- |
| Candidate conversion rate | Tracks whether resolving key uncertainty improves valid candidate input quality. |
| Replay support rate | Measures whether replay-facing uncertainty predicts replay survival. |
| Backtest support rate | Measures whether backtest-facing uncertainty predicts backtest survival. |
| Paper-forward survival | Measures whether unresolved uncertainty predicts future observation failure. |
| Hypothesis rejection speed | Measures whether uncertainty blocks weak ideas early. |
| False positive elimination rate | Measures whether uncertainty flags catch attractive but unsupported work. |
| Research hours saved | Measures avoided validation caused by early uncertainty detection. |
| Information gain per research hour | Measures learning gained from uncertainty reduction work. |
| Research debt reduction | Measures closure of unresolved uncertainty inventory. |
| Search-space compression | Measures how uncertainty constraints narrow future hypothesis branches. |

## Operating Principle

Uncertainty should be routed like work, not buried in narrative. A hypothesis with unresolved high-impact uncertainty should not advance merely because its story is attractive.

