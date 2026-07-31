# IMPLEMENTATION_DECISION_003

## Purpose

Record the official factory decision for IMP_003 — Turtle Breakout based solely on generated evidence and review artifacts.

This document does not certify CLAIM_001.

## References

```text
CLAIM_001
IMP_003
BACKTEST_003
RESULT_REVIEW_003
HOSTILE_REVIEW_003
```

## Evidence Summary

Primary findings:

- RESULT_REVIEW_003 classified the implementation result as UNFAVORABLE.
- HOSTILE_REVIEW_003 classified the hostile verdict as FAILS.
- IMP_003 reduced volatility, maximum drawdown, and ulcer index versus SPY buy-and-hold.
- IMP_003 did not match SPY buy-and-hold total return, CAGR, Sharpe, or Sortino.
- IMP_003 did not beat the 50/50 SPY/cash baseline on total return, CAGR, Sharpe, Sortino, or ulcer index.
- Validation-period evidence was unfavorable.

Strengths:

- Full-period maximum drawdown improved versus SPY buy-and-hold.
- Full-period maximum drawdown was slightly better than the 50/50 SPY/cash baseline.
- Random timing comparison was favorable on CAGR and Sharpe.
- Stress tests remained positive in absolute return terms.
- The implementation is rules-based and auditable.

Weaknesses:

- Full-period return and risk-adjusted performance lagged both registered baselines.
- Validation-period CAGR, Sharpe, Sortino, and ulcer index lagged both registered baselines.
- Validation-period maximum drawdown was worse than the 50/50 SPY/cash baseline.
- Lower exposure remains a major explanation for drawdown reduction.
- Trade count was materially higher than prior implementations.
- Higher costs and execution delay degraded already weak evidence.
- Evidence is limited to one instrument and one implementation.

Stress-test behavior:

- 20 bps and 50 bps round-trip cost tests remained positive but materially weaker.
- 1-day execution delay remained positive but weakened CAGR, Sharpe, and maximum drawdown versus the base case.
- 40-day and 70-day breakout perturbations were directionally similar and stronger than the base case.
- 10-day low exit reduced drawdown but lowered return.
- 30-day low exit worsened drawdown and Sharpe.

Validation behavior:

- Validation-period return remained positive.
- Validation-period maximum drawdown improved versus buy-and-hold.
- Validation-period CAGR, Sharpe, Sortino, and ulcer index lagged both buy-and-hold and 50/50 SPY/cash.
- Validation-period drawdown was worse than the 50/50 SPY/cash baseline.

## Evidence Strength

Evidence strength: MODERATE

Justification:

The evidence includes a full-period backtest, registered baselines, random timing comparison, development and validation period review, subperiod review, cost stress, execution-delay stress, and parameter perturbation review. Evidence strength is MODERATE because the evidence package is complete and reproducible for this implementation. Evidence strength is not STRONG because hostile review failed, validation evidence was unfavorable, and the run is limited to SPY and one implementation.

## Deployability

Deployability: LIMITED

Justification:

HOSTILE_REVIEW_003 states that the rules are simple and auditable, but trade count is high relative to prior implementations, return capture is weak, validation is unfavorable, and tax and operational burdens remain unresolved. Deployability is not PRACTICAL because the implementation failed hostile review and did not beat the 50/50 exposure baseline on key full-period or validation metrics.

## Official Decision

Official decision: INSUFFICIENT_EVIDENCE

Justification:

IMP_003 beat randomized timing on CAGR and Sharpe and reduced drawdown versus buy-and-hold, but RESULT_REVIEW_003 was UNFAVORABLE and HOSTILE_REVIEW_003 was FAILS. The implementation did not survive baseline and validation review. The decision is not WEAK_SUPPORT because hostile review failed. The decision is not FALSIFIED because this is one SPY implementation run and does not by itself establish that all reasonable Turtle-style breakout implementations or CLAIM_001 fail.

## Claim Impact

Claim impact: WEAK_NEGATIVE

Justification:

HOSTILE_REVIEW_003 records weak negative claim impact. This means IMP_003 adds negative implementation-level evidence to the CLAIM_001 ledger. It does not falsify CLAIM_001 and does not override prior implementation evidence.

IMP_003 does not determine the status of CLAIM_001.

## Open Questions

Why IMP_003 diverged from IMP_001 and IMP_002:

- IMP_003 used a shorter 55-day entry breakout and 20-day low exit, with materially higher trade count than prior completed implementations.
- The evidence suggests more whipsaw and weaker return capture than the slower implementations, but this document does not introduce new analysis beyond the review artifacts.

Whether SPY is an appropriate Turtle test vehicle:

- Evidence is limited to SPY.
- Turtle-style breakout logic may require additional markets or broader futures-style universes to evaluate the implementation family, but CLAIM_001 certification cannot rely on this single SPY run.

Whether the implementation structure caused failure:

- Parameter perturbations were directionally positive but varied materially.
- The 40-day and 70-day breakout perturbations were stronger than the base case.
- Exit perturbations showed sensitivity, including worse drawdown and Sharpe under the 30-day low exit.

What future evidence is required:

- IMP_004 — Dual Momentum.
- Independent replication if IMP_003 remains decision-relevant.
- Review of whether Turtle-style breakout should be retested in a broader or more appropriate universe as a separate implementation.
- Preservation of IMP_003 negative findings in the factory knowledge base.

## Required Next Actions

1. Update the CLAIM_001 status ledger without certifying CLAIM_001.
2. Launch IMP_004 — Dual Momentum as the next remaining planned implementation.
3. Preserve IMP_003 negative findings in the factory knowledge base.
4. Do not certify CLAIM_001 from IMP_003.

## Decision Status

IMPLEMENTATION_DECISION_003: INSUFFICIENT_EVIDENCE
