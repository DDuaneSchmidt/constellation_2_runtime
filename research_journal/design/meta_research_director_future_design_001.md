# Meta-Research Director Future Design 001

Status: DESIGN ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: future architecture documentation for a Meta-Research Director layer in Atlas. This document does not implement software, alter runtime truth, modify Aegis behavior, create candidates, approve paper positions, allocate capital, provide trade advice, or authorize live activity.

Runtime truth note: if this design is ever implemented, it must query verified runtime truth. No consumer may infer readiness from this document, code shape, journal text, or generated output.

## Core Thesis

The Research Scientist is useful for doing research. The Research Director is useful for directing research. The Meta-Research Director should evaluate whether the research process itself is improving.

The long-term Atlas center of gravity should therefore include a layer that studies the quality, cost, failure modes, and learning rate of the research system, not only the quality of individual hypotheses.

## Role Definition

The Meta-Research Director should govern the research process as an object of study.

Responsibilities:

- measure whether Research Director decisions improve learning outcomes
- detect systematic false positive sources
- detect systematic false negative risks
- evaluate whether evidence gates are too loose, too strict, or misordered
- compare research pipelines by information gain and cost
- identify where research labor is being wasted
- recommend changes to research policy for future implementation review

Non-responsibilities:

- selecting trades
- authorizing candidates
- allocating capital
- mutating runtime readiness
- overriding human approvals
- replacing verified runtime truth

## Meta-Research Objects

Future meta-research should treat the research process as a measurable workflow.

Suggested objects:

- `ResearchPipeline`
- `DecisionPolicy`
- `EvidenceGate`
- `FailureModeCluster`
- `ResearchCostModel`
- `LearningYieldRecord`
- `DedupeEffectivenessRecord`
- `RetirementEffectivenessRecord`
- `SearchSpaceCompressionRecord`

Each object should preserve lineage to the lower-level research decisions it evaluates.

## Evaluation Questions

The Meta-Research Director should answer questions such as:

- Which hypothesis families consume the most research time per accepted candidate?
- Which sources produce the highest false positive rate?
- Which evidence gates eliminate weak ideas earliest?
- Which replay methods produce support that later survives paper-forward review?
- Which retired ideas keep reappearing under new wording?
- Which uncertainty fields best predict later failure?
- Which search-space compression methods preserve useful ideas while eliminating noise?

## Future Metrics

The Meta-Research Director should evaluate all research metrics at process level:

| Metric | Meta-research use |
| --- | --- |
| Candidate conversion rate | Detect whether conversion is improving because input quality improved or because gates weakened. |
| Replay support rate | Measure replay gate selectivity and source quality. |
| Backtest support rate | Compare replay findings against backtest survival. |
| Paper-forward survival | Measure whether earlier evidence actually predicts future observation survival. |
| Hypothesis rejection speed | Track whether rejection is becoming faster without increasing missed-useful-idea risk. |
| False positive elimination rate | Identify which policies eliminate attractive but unsupported hypotheses. |
| Research hours saved | Attribute saved effort to dedupe, retirement, allocation, or search compression. |
| Information gain per research hour | Compare research pipelines by learning yield. |
| Research debt reduction | Measure whether process changes reduce unresolved stale work. |
| Search-space compression | Evaluate whether compression removes redundant search paths without collapsing diversity. |

## Policy Feedback Loop

Future meta-research should produce policy recommendations, not direct operational changes.

Allowed future recommendations:

- tighten or loosen a gate
- reorder evidence stages
- raise retirement thresholds
- change replay sample requirements
- change uncertainty fields
- prioritize a mechanism family
- deprioritize a source
- expand or narrow search-space compression rules

Every recommendation should remain design or governance input until separately reviewed, implemented, tested, declared, and verified.

## Operating Principle

The Meta-Research Director should optimize the research process for durable learning. Its core question is not "which hypothesis is attractive?" but "which research process produces reliable knowledge fastest with the least avoidable waste?"

