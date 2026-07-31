# Research Director v0.1 Future Design 001

Status: DESIGN ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: future architecture documentation for a Research Director layer in Atlas. This document does not implement software, alter runtime truth, modify Aegis behavior, create candidates, approve paper positions, allocate capital, provide trade advice, or authorize live activity.

Runtime truth note: if this design is ever implemented, it must query verified runtime truth. No consumer may infer readiness from this document, code shape, journal text, or generated output.

## Core Thesis

The Research Scientist role is useful for hypothesis work, experiment design, evidence review, and localized research execution. The long-term Atlas center of gravity should move above individual scientific workers toward a research operating system governed by:

- Research Director
- Meta-Research Director
- Research Allocation
- Research Retirement
- Uncertainty Objects
- Search-Space Compression
- Research Economics

The Research Director is the future orchestration layer that decides what research should happen next, what should wait, what should be rejected early, and what evidence threshold is required before a hypothesis can advance. It does not replace the Research Scientist. It constrains, sequences, and audits research labor so Atlas spends fewer cycles rediscovering weak ideas.

## Role Definition

The Research Director should own research prioritization, sequencing, and escalation policy across hypothesis families.

Responsibilities:

- maintain a ranked research agenda
- select which hypotheses deserve replay, backtest, paper-forward observation, or retirement review
- enforce evidence prerequisites before progression
- detect duplicate or near-duplicate research work
- identify blocked research flows and route them to repair or retirement
- require uncertainty objects before a hypothesis receives further work
- measure whether research activity produces knowledge, not merely artifacts

Non-responsibilities:

- broker execution
- capital allocation
- position sizing
- trade recommendation
- runtime readiness declaration
- overriding verified runtime truth
- promoting generated claims into validated evidence

## Future Inputs

The future Research Director should consume only typed, lineage-preserving objects.

Candidate inputs:

- hypothesis records
- mechanism records
- replay results
- backtest summaries
- paper-forward observations
- uncertainty objects
- retired knowledge records
- failure patterns
- research-cost estimates
- search-space compression maps
- verified runtime truth references

Every input must carry source lineage, evidence level, timestamp discipline, and allowed-use boundaries.

## Future Outputs

The Research Director should emit research decisions, not trading decisions.

Allowed future outputs:

- `RESEARCH_NEXT`
- `REPLAY_REQUIRED`
- `BACKTEST_REQUIRED`
- `PAPER_FORWARD_REQUIRED`
- `REJECT_FOR_DUPLICATION`
- `REJECT_FOR_WEAK_EVIDENCE`
- `RETIREMENT_REVIEW_REQUIRED`
- `UNCERTAINTY_REPAIR_REQUIRED`
- `HOLD_FOR_REGIME_CONTEXT`
- `DO_NOT_ADVANCE`

Each output should include:

- decision id
- referenced hypothesis or mechanism
- evidence basis
- uncertainty basis
- expected information gain
- expected research cost
- blocked or forbidden uses
- review due date

## Evidence Progression Policy

The Research Director should enforce staged progression:

1. Generated or sourced claim.
2. Mechanism framing.
3. Uncertainty object creation.
4. Historical replay support.
5. Backtest support when appropriate.
6. Paper-forward survival when authorized by future policy.
7. Candidate consideration only if separately implemented and verified.

No stage should imply the next stage automatically. Advancement requires explicit evidence and an explicit research decision.

## Future Metrics

The Research Director should own these metrics:

| Metric | Definition |
| --- | --- |
| Candidate conversion rate | Share of research items that become valid candidate inputs after all required evidence gates. |
| Replay support rate | Share of replayed hypotheses that retain support after historical replay. |
| Backtest support rate | Share of backtested hypotheses that retain support after reproducible backtest review. |
| Paper-forward survival | Share of paper-forward observations that remain valid after predeclared review windows. |
| Hypothesis rejection speed | Median time or research hours from hypothesis intake to justified rejection. |
| False positive elimination rate | Share of initially attractive hypotheses rejected before expensive validation. |
| Research hours saved | Estimated hours avoided through dedupe, retirement, prior knowledge, or early rejection. |
| Information gain per research hour | Evidence-weighted learning produced per hour spent. |
| Research debt reduction | Net decrease in stale, duplicate, weak, or unresolved research obligations. |
| Search-space compression | Reduction in candidate mechanism space after applying evidence, retirement, and uncertainty constraints. |

## Operating Principle

The Research Director should optimize for justified learning per unit of research effort. It should prefer a fast, well-evidenced rejection over a slow accumulation of ambiguous artifacts.

