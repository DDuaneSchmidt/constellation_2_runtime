# Research Economics Future Design 001

Status: DESIGN ONLY

Implementation posture: NOT IMPLEMENTED

Authority posture: NO AUTHORITY EXPANSION

Date: 2026-06-05

Scope: future architecture documentation for research economics in Atlas. This document does not implement software, alter runtime truth, modify Aegis behavior, create candidates, approve paper positions, allocate capital, provide trade advice, or authorize live activity.

Runtime truth note: if this design is ever implemented, it must query verified runtime truth. No consumer may infer readiness from this document, code shape, journal text, or generated output.

## Core Thesis

Atlas should measure research as an economic activity. The Research Scientist creates useful work products, but the long-term system must know whether each hour of research produces enough learning to justify its cost.

Research Economics is not portfolio economics and not trading economics. It measures the cost and yield of research activity only.

## Economic Unit

A future research economics layer should treat each research action as an investment of scarce effort.

Tracked units:

- human review hours
- machine processing time
- replay cost
- backtest cost
- source acquisition or cleaning cost
- uncertainty repair cost
- opportunity cost of delayed work
- research debt carrying cost

Each unit should be linked to resulting evidence, rejection, retirement, compression, or unresolved debt.

## Value Measures

Research value should include positive and negative learning.

Potential value outputs:

- validated candidate input
- rejected hypothesis
- retired mechanism branch
- resolved uncertainty
- discovered failure pattern
- compressed search space
- improved source-quality estimate
- reduced duplicate work
- better evidence gate calibration

Negative results can have high value when they prevent repeated expensive work.

## Cost Model

A future cost model should estimate:

- expected research hours
- actual research hours
- evidence-stage cost
- duplicate-search cost
- uncertainty-resolution cost
- retirement-review cost
- cost of delayed decision
- cost of false positive continuation
- cost of stale unresolved research

Cost estimates should be revised after work completes so future allocation improves.

## Information Gain

Information gain should measure reduction in decision uncertainty, not volume of generated text.

Indicative formula:

```text
information_gain_per_research_hour =
  evidence_weighted_decision_uncertainty_reduction / research_hours_spent
```

The future implementation should define evidence weights explicitly. Generated claims, mock evidence, replay results, backtests, and paper-forward observations must not receive the same weight.

## Research Debt

Research debt is unresolved or poorly structured research work that will consume future attention.

Examples:

- stale hypotheses
- duplicate hypothesis clusters
- unresolved uncertainty objects
- ambiguous mechanism definitions
- weak but unretired ideas
- source lineage gaps
- unreviewed replay or backtest outputs
- repeated paper-forward failures without retirement

Debt reduction should be measured as a first-class benefit.

## Future Metrics

Research Economics should define the accounting frame for all future metrics:

| Metric | Economic meaning |
| --- | --- |
| Candidate conversion rate | Yield of research inputs into valid candidate inputs. |
| Replay support rate | Yield of replay spending into supported hypotheses. |
| Backtest support rate | Yield of backtest spending into supported hypotheses. |
| Paper-forward survival | Yield of observation spending into surviving research knowledge. |
| Hypothesis rejection speed | Time-to-stop cost control. |
| False positive elimination rate | Avoided downstream validation cost. |
| Research hours saved | Direct avoided labor from dedupe, retirement, and compression. |
| Information gain per research hour | Primary research productivity metric. |
| Research debt reduction | Balance-sheet improvement in unresolved research obligations. |
| Search-space compression | Reduction in future research option cost. |

## Search-Space Compression Economics

Search-space compression should be valued by its effect on future research cost.

Compression has value when it:

- removes duplicate hypothesis variants
- suppresses retired mechanism branches
- narrows parameter ranges through evidence
- reduces source families that produce false positives
- clarifies which regimes are out of scope
- prevents repeated low-information experiments

Compression is harmful when it removes useful diversity without evidence. The future system should track both saved research hours and later reopened opportunities.

## Operating Principle

Research Economics should make the cost of curiosity visible. Atlas should still explore, but exploration should be measured by learning yield, debt reduction, and avoided waste.

