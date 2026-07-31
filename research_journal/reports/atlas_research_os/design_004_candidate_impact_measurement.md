# Atlas Research OS Design 004: Candidate Impact Measurement

Date: 2026-06-04
Status: Design only

Scope: defines how future Atlas Research OS analytics should measure whether learning improves the candidate factory. This document does not implement candidate promotion, candidate construction, sleeve mutation, paper setup, trading, or capital authorization.

Runtime posture: the verified runtime graph for 2026-06-04 is `BLOCKED`. Current Research OS design must not infer readiness or authorization from code.

## Measurement Principle

Candidate impact measurement is retrospective and observational. It asks whether learning improved the quality, maturity, and efficiency of research-to-candidate flow.

It must not:
- create candidates
- promote candidates
- approve paper positions
- alter sleeve policy
- route orders
- allocate capital
- imply trade advice

## Metrics

### Candidate Conversion Rate

Definition:

```text
candidate_conversion_rate =
  valid_candidates_created / eligible_research_outputs
```

Use:
- measure whether supported learning eventually leads to valid candidate artifacts through existing approved systems
- compare by mechanism, evidence level, regime, and worker lineage

Guardrail: conversion is not success by itself. A high conversion rate with weak outcomes may indicate overpromotion.

### Rejection-Rate Reduction

Definition:

```text
rejection_rate_reduction =
  baseline_rejection_rate - post_learning_rejection_rate
```

Use:
- track whether learning reduces avoidable candidate rejections
- segment by rejection reason, such as missing data, policy mismatch, duplicate, weak evidence, or invalid contract

Guardrail: rejection reduction is useful only if downstream evidence quality does not deteriorate.

### Evidence Maturity Improvement

Definition:

```text
evidence_maturity_delta =
  maturity_score_after_learning - maturity_score_before_learning
```

Suggested maturity order:

```text
GENERATED_ONLY < MOCK_ONLY < HISTORICAL_REPLAY < PAPER_FORWARD_OBSERVATION < EXTERNALLY_VALIDATED < OPERATOR_APPROVED
```

Guardrail: maturity score must preserve the hard rule that `OPERATOR_APPROVED` is not capital approval.

### Hypothesis Survival Improvement

Definition:

```text
hypothesis_survival_improvement =
  post_learning_survival_rate - baseline_survival_rate
```

Survival means the hypothesis remains supported or usefully under observation through predeclared review windows. It does not mean profitable, tradable, or capital-ready.

Measure by:
- survival after duplicate check
- survival after historical replay
- survival after paper-forward observation
- survival after outcome review
- survival after regime shift

### Portfolio-Scoring Rejection Reduction

Definition:

```text
portfolio_scoring_rejection_reduction =
  baseline_portfolio_score_rejection_rate
  - post_learning_portfolio_score_rejection_rate
```

Use:
- detect whether learning improves fit with portfolio scoring constraints
- identify repeated reasons that otherwise block candidate consideration

Guardrail: this is about measuring rejection reasons. It does not authorize portfolio allocation.

### Repeated Failure Reduction

Definition:

```text
repeated_failure_reduction =
  baseline_repeated_failure_count
  - post_learning_repeated_failure_count
```

Failure examples:
- duplicate claim creation
- stale data dependency
- unsupported hypothesis type
- missing mark source
- unknown deterministic blocker
- repeated policy mismatch
- mock/generated evidence leakage

Use:
- prove that Research OS memory and retirement reduce wasted work
- prioritize worker and data-source repairs

## Attribution Model

Each measurement must link the observed candidate-factory change to:

- learning artifact id
- source evidence ids and hashes
- evidence level before and after
- worker or operator lineage
- mechanism and regime labels
- candidate rejection or survival records
- time window
- confounders and competing explanations

Attribution should be conservative. If multiple changes occurred at once, the metric should report partial or uncertain attribution rather than claiming causality.

## Reporting

Future reports should include:

- baseline window
- post-learning window
- metric value and confidence
- sample size
- excluded artifacts
- known biases
- forbidden interpretation

Every report must state: candidate impact measurement is not candidate promotion and is not capital authorization.
