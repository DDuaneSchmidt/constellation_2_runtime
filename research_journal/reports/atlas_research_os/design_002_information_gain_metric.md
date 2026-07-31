# Atlas Research OS Design 002: Information Gain Metric

Date: 2026-06-04
Status: Design only

Scope: defines a proposed utility metric for future Research OS task selection. It does not implement a priority engine, worker, scheduler, candidate generator, promotion path, or capital allocator.

Runtime posture: the verified runtime graph for 2026-06-04 is `BLOCKED`. The metric below is a design target only and must not be used as runtime truth.

## Objective

Atlas should prefer research tasks that are likely to reduce uncertainty, prevent repeated failures, improve regime coverage, and improve future candidate-factory measurement while penalizing cost and duplication.

The metric estimates expected information gain, not expected profit.

## Proposed Formula

For a task `t`:

```text
IG(t) =
  Wn * Novelty(t)
+ Wu * UncertaintyReduction(t)
+ Wf * FailurePatternReduction(t)
+ Wr * RegimeCoverageImprovement(t)
+ Wq * CandidateQualityRelevance(t)
- Wc * CostPenalty(t)
- Wd * DuplicatePenalty(t)
```

Each term is normalized to `[0, 1]`. Weights must be declared in the future governance configuration and versioned with the scoring run. A task is eligible only if all safety, lineage, and authority checks pass; a high score cannot override blocked runtime truth.

## Terms

### Novelty

Measures whether the task explores a mechanism, regime, instrument scope, feature, failure mode, or claim cluster that is not already covered.

Inputs:
- distance from existing claim and hypothesis clusters
- mechanism tag coverage
- source diversity
- prior duplicate detections

High novelty does not imply truth. It only means the task may teach Atlas something new.

### Uncertainty Reduction

Measures expected reduction in uncertainty around a claim, hypothesis, experiment, or failure pattern.

Inputs:
- current confidence interval or qualitative uncertainty
- expected evidence level after completion
- availability of decisive falsification criteria
- number of unresolved assumptions addressed

Tasks with clear pass/fail outcomes score higher than tasks that only produce more ambiguous prose.

### Failure-Pattern Reduction

Measures whether the task reduces the chance of repeating known failures.

Inputs:
- linked failure patterns
- recurrence count
- cost of repeated failure
- specificity of proposed diagnostic
- ability to convert unknown blockers into named blockers

This term rewards negative knowledge when it makes future work safer and less wasteful.

### Regime Coverage Improvement

Measures whether the task expands evidence across missing or underrepresented regimes.

Inputs:
- market regime labels
- time window coverage
- instrument and sector coverage
- event type coverage
- missing data or source gaps

Regime coverage is useful only when source lineage is explicit. A task cannot claim regime improvement from stale or unverified data.

### Candidate-Quality Relevance

Measures whether learning could later improve candidate-factory measurement.

Inputs:
- link to candidate rejection reasons
- link to paper-forward observation outcomes
- ability to explain why candidates fail or survive
- relevance to portfolio-scoring rejection
- relationship to evidence maturity

This is measurement relevance only. It does not authorize candidate promotion, paper setup, sleeve mutation, trade advice, or capital allocation.

### Cost Penalty

Measures expected resource cost and operational risk.

Inputs:
- compute time
- data acquisition burden
- operator attention required
- lineage validation effort
- opportunity cost
- risk of forbidden artifact leakage

Tasks with high safety or lineage complexity must pay a higher cost penalty unless they produce unusually decisive learning.

### Duplicate Penalty

Measures similarity to prior work, retired knowledge, stale knowledge, failed experiments, or already-open backlog items.

Inputs:
- semantic similarity to existing memory objects
- shared experiment design hash
- same mechanism/regime/instrument tuple
- repeated failure pattern
- prior retired or falsified status

Duplicate tasks may still be useful if they reopen a materially different regime or repair a prior flawed test. Otherwise the penalty should dominate.

## Eligibility Gates

Before scoring can be consumed by future automation, a task must pass:

- evidence-level label validation
- lineage parent validation
- duplicate and retired-knowledge check
- forbidden artifact check
- runtime truth and verified graph check
- worker authority check

If any gate fails, the task is ineligible regardless of `IG(t)`.

## Interpretation

Recommended bands:

| Score | Meaning | Expected action |
| --- | --- | --- |
| `0.75-1.00` | High expected learning value | Consider for next bounded research run if gates pass. |
| `0.50-0.74` | Useful but not urgent | Queue behind higher-value or lower-cost tasks. |
| `0.25-0.49` | Weak value | Keep only if cheap, clarifying, or operator-requested. |
| `0.00-0.24` | Low value or duplicate | Defer, merge, retire, or require reopening justification. |

The score is advisory. It is not a readiness certificate.
