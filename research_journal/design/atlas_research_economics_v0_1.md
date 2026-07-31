# Atlas Research Economics V0.1

Date: 2026-06-05
Status: Design only

Scope: defines a V0.1 economics frame for measuring Atlas research productivity. This document does not implement software, change runtime truth, create candidates, approve paper-forward activity, recommend trades, allocate capital, size positions, authorize broker execution, or expand governance authority.

Runtime posture: research economics is measurement-only. Any future implementation must query verified runtime truth and preserve existing authority boundaries.

## Purpose

Atlas should measure research as an economic activity. Candidate count matters, but it can be a weak proxy for value. A smaller number of decisive rejections, retired dead ends, or clarified constraints may save more research time than a larger number of generated candidates.

The committee's recurring metric, `research_hours_saved`, should become a first-class output of the research system.

## Core Accounting Unit

The base unit is a `research_action`.

Examples:

- observation review
- claim generation
- hypothesis review
- replay run
- backtest run
- adversary review
- human candidate review
- failure-pattern analysis
- retirement decision
- data-quality repair

Each action should eventually record:

```text
action_id
source_artifact_ids
research_stage
mechanism
regime
cost_estimate
actual_cost
yield_outputs
information_gain
debt_created
debt_retired
authority_boundary
```

## Research Cost

Research Cost measures the total effort consumed to produce a research outcome.

It should include human and machine costs, plus downstream correction cost when the output creates ambiguity or debt.

Formula:

```text
ResearchCost(a) =
  HumanHours(a) * HumanHourWeight
+ MachineRuntimeHours(a) * MachineHourWeight
+ DataPrepHours(a) * DataPrepWeight
+ ReviewCorrectionHours(a) * CorrectionWeight
+ OpportunityDelayHours(a) * DelayWeight
+ GovernanceReviewHours(a) * GovernanceWeight
+ DebtCreatedCost(a)
```

V0.1 simplified formula:

```text
ResearchCost(a) =
  human_hours
+ 0.25 * machine_hours
+ data_prep_hours
+ reviewer_correction_hours
+ debt_created_hours
```

Interpretation:

- Low cost is not automatically good. Cheap work that creates debt can be expensive.
- Expensive work can be justified if it produces decisive information gain or retires high-cost branches.
- Cost must be measured in research units, not capital or trading units.

## Research Yield

Research Yield measures the useful research output produced by an action.

Yield includes positive and negative learning. A rejected hypothesis can have high yield if it prevents repeated validation attempts.

Yield components:

```text
ResearchYield(a) =
  EvidenceValue(a)
+ RejectionValue(a)
+ RetirementValue(a)
+ FailurePatternValue(a)
+ SearchSpaceCompressionValue(a)
+ DebtReductionValue(a)
+ ReuseValue(a)
- FalsePositiveContinuationCost(a)
- AmbiguityCost(a)
```

V0.1 component definitions:

- `EvidenceValue`: value of stronger evidence, weighted by evidence level and lineage quality.
- `RejectionValue`: avoided future cost from stopping weak hypotheses early.
- `RetirementValue`: avoided future cost from closing stale branches.
- `FailurePatternValue`: value from discovering a reusable failure mode.
- `SearchSpaceCompressionValue`: value from narrowing future experiments.
- `DebtReductionValue`: value from resolving unresolved assumptions, constraints, or lineage gaps.
- `ReuseValue`: value of artifacts that can support multiple future reviews.

Simplified scoring formula:

```text
ResearchYield(a) =
  2.0 * decisive_rejections
+ 2.0 * retired_failure_patterns
+ 1.5 * validated_constraints
+ 1.5 * resolved_assumptions
+ 1.0 * reusable_evidence_artifacts
+ 1.0 * useful_candidate_reviews
- 2.0 * false_positive_continuations
- 1.0 * ambiguous_outputs
```

All terms are research value units, not money.

## Research ROI

Research ROI measures value created per unit cost.

Formula:

```text
ResearchROI(a) =
  ResearchYield(a) / max(ResearchCost(a), minimum_cost_floor)
```

Portfolio-level formula:

```text
ResearchROI(period) =
  sum(ResearchYield(actions_in_period)) /
  max(sum(ResearchCost(actions_in_period)), minimum_cost_floor)
```

Decision bands:

| ROI | Interpretation |
| ---: | --- |
| `>= 2.0` | High-yield research; likely worth repeating if authority boundaries remain clean. |
| `1.0-1.99` | Productive research; continue measurement. |
| `0.5-0.99` | Marginal; improve targeting or reduce cost. |
| `< 0.5` | Poor yield; consider retirement, redesign, or stricter intake. |

ROI must not be interpreted as trading return, expected profit, or capital efficiency.

## Research Hours Saved

Research Hours Saved measures avoided future work.

This may matter more than candidate count because avoiding repeated dead ends increases total system capacity.

Formula:

```text
ResearchHoursSaved(a) =
  AvoidedDuplicateReviewHours(a)
+ AvoidedReplayHours(a)
+ AvoidedBacktestHours(a)
+ AvoidedPaperObservationHours(a)
+ AvoidedDataRepairHours(a)
+ AvoidedGovernanceReviewHours(a)
- NewReviewOverheadHours(a)
- FalseRejectionReopenCost(a)
```

For adversary-assisted review:

```text
ResearchHoursSaved =
  baseline_review_hours
- adversary_assisted_review_hours
- adversary_review_overhead_hours
- correction_hours
```

For rejection or retirement:

```text
ResearchHoursSaved =
  expected_future_cost_if_kept_open
- actual_rejection_or_retirement_cost
- reopen_risk_cost
```

Interpretation:

- A negative result can save many hours.
- A generated artifact that requires heavy correction can have negative hours saved.
- Hours saved should be discounted when the rejection is weak or likely to be reopened.

## Information Gain

Information Gain measures reduction in research uncertainty.

It should reward decisive learning, not output volume.

Formula:

```text
InformationGain(a) =
  PriorUncertainty(a)
- PosteriorUncertainty(a)
```

Evidence-weighted version:

```text
WeightedInformationGain(a) =
  (PriorUncertainty(a) - PosteriorUncertainty(a))
* EvidenceWeight(a)
* LineageQuality(a)
* DecisionUsefulness(a)
```

Information gain per hour:

```text
InformationGainPerHour(a) =
  WeightedInformationGain(a) /
  max(ResearchCostHours(a), minimum_cost_floor)
```

Suggested evidence weights:

| Evidence Level | Weight |
| --- | ---: |
| generated-only critique | 0.10 |
| structured observation | 0.20 |
| replay result | 0.45 |
| backtest result | 0.60 |
| human-reviewed failure analysis | 0.70 |
| paper-forward outcome | 0.85 |
| independently replicated result | 1.00 |

Information gain examples:

- High: a falsification test rejects a broad mechanism branch.
- High: a failure pattern explains many candidate rejections.
- Medium: a backtest narrows a candidate family but leaves proxy risk.
- Low: a generated summary restates known risks without resolving uncertainty.

## Hypothesis Rejection Speed

Hypothesis Rejection Speed measures how quickly Atlas stops spending effort on weak hypotheses.

Base formula:

```text
HypothesisRejectionSpeed(h) =
  rejection_timestamp - hypothesis_intake_timestamp
```

Quality-adjusted version:

```text
QualityAdjustedRejectionSpeed(h) =
  HypothesisRejectionSpeed(h) /
  max(RejectionQualityScore(h), quality_floor)
```

Where:

```text
RejectionQualityScore =
  0.30 * evidence_specificity
+ 0.25 * falsification_quality
+ 0.20 * failure_mode_specificity
+ 0.15 * reviewer_confidence
+ 0.10 * authority_boundary_cleanliness
```

Speed improvement:

```text
RejectionSpeedImprovement =
  (baseline_median_rejection_time - measured_median_rejection_time) /
  baseline_median_rejection_time
```

Guardrail:

Fast rejection is useful only when rejection quality remains high. Premature rejection creates research debt and should be penalized.

## Research Debt

Research Debt is unresolved research work that will consume future attention or distort future decisions.

Debt sources:

- stale hypotheses
- duplicate mechanism branches
- ambiguous candidate definitions
- unresolved assumptions
- unverified constraints
- source lineage gaps
- proxy-only evidence
- weak regime labels
- incomplete replay or backtest interpretation
- unresolved failure warnings
- generated summaries that sound stronger than their evidence

Debt stock formula:

```text
ResearchDebtStock(t) =
  PriorDebtStock
+ DebtCreated(t)
- DebtRetired(t)
- DebtResolved(t)
+ DebtInterest(t)
```

Debt interest:

```text
DebtInterest(t) =
  sum(open_debt_item_age_days * recurrence_risk * future_cost_weight)
```

Debt-adjusted ROI:

```text
DebtAdjustedResearchROI =
  (ResearchYield + DebtRetiredValue - DebtCreatedCost) /
  max(ResearchCost, minimum_cost_floor)
```

Debt severity bands:

| Debt Item | Severity |
| --- | --- |
| authority ambiguity | HIGH |
| proxy evidence used as candidate-specific evidence | HIGH |
| stale high-confidence hypothesis | HIGH |
| missing source lineage | HIGH |
| duplicate hypothesis cluster | MEDIUM |
| weak regime label | MEDIUM |
| unresolved warning | MEDIUM |
| cosmetic report inconsistency | LOW |

## Research Yield Funnel

Research economics should treat the funnel as learning stages, not only candidate production.

```text
observations
  -> claims
  -> hypotheses
  -> replay
  -> backtest
  -> paper-forward observation
  -> outcome evidence
  -> retained knowledge or retirement
```

At each stage, yield can be:

- promote to stronger evidence review
- reject
- retire
- merge duplicate
- split ambiguous cluster
- repair data constraint
- generate failure pattern
- defer with explicit debt tag

Candidate count alone misses most of this yield.

## V0.1 Dashboard Metrics

Minimum dashboard:

| Metric | Formula |
| --- | --- |
| `research_hours_saved` | avoided_future_hours - new_review_overhead - correction_hours |
| `research_cost` | human_hours + 0.25 * machine_hours + data_prep_hours + correction_hours + debt_created_hours |
| `research_yield` | weighted useful outputs - false_positive_continuation_cost - ambiguity_cost |
| `research_roi` | research_yield / research_cost |
| `information_gain_per_hour` | weighted_information_gain / research_cost_hours |
| `hypothesis_rejection_speed` | rejection_timestamp - intake_timestamp |
| `quality_adjusted_rejection_speed` | rejection_speed / rejection_quality_score |
| `research_debt_stock` | prior_debt + created - retired - resolved + interest |
| `debt_adjusted_research_roi` | (yield + debt_retired_value - debt_created_cost) / cost |

## Measurement Rules

- Negative learning counts as yield when it prevents repeated work.
- A candidate is not automatically yield unless it survives evidence checks.
- A generated report is not yield unless it changes review decisions, reduces uncertainty, or saves time.
- Research hours saved must include correction overhead.
- Debt created by ambiguity must be charged against the producing action.
- Authority boundary cleanliness is required before any metric can be trusted.

## Open Questions

- What is the baseline review duration for each artifact class?
- How should human review quality be calibrated across reviewers?
- What discount should be applied to generated-only outputs?
- How should reopened retired hypotheses be charged back to earlier rejection decisions?
- Which research debt items deserve automatic aging alerts?
- How much search-space compression is valuable before exploration diversity is harmed?

## Conclusion

Atlas Research Economics V0.1 should make the cost of curiosity visible.

The primary question is not how many candidates were produced. The primary question is how much uncertainty was reduced, how much future work was avoided, and how much research debt was retired per hour spent.
