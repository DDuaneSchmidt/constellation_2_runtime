# IMPLEMENTATION_DECISION_002

## Purpose

Record the official factory decision for IMP_002 — 52-Week Breakout based only on existing evidence and review artifacts.

This document does not certify CLAIM_001.

## References

```text
CLAIM_001
IMP_002
BACKTEST_002
RESULT_REVIEW_002
HOSTILE_REVIEW_002
```

## Evidence Summary

Primary findings:

- RESULT_REVIEW_002 classified the implementation result as MIXED.
- HOSTILE_REVIEW_002 classified the hostile verdict as SURVIVES_WITH_CONCERNS.
- IMP_002 reduced volatility, maximum drawdown, and ulcer index versus SPY buy-and-hold.
- IMP_002 slightly improved full-period Sharpe versus SPY buy-and-hold and 50/50 SPY/cash.
- IMP_002 did not match SPY buy-and-hold or 50/50 SPY/cash on full-period CAGR.
- Validation-period CAGR and Sharpe lagged both registered baselines.

Strengths:

- Full-period maximum drawdown improved materially versus SPY buy-and-hold.
- Full-period maximum drawdown and ulcer index improved versus the 50/50 SPY/cash baseline.
- Random timing comparison was favorable on CAGR and Sharpe.
- Higher cost, execution delay, and breakout lookback perturbation reviews did not erase the evidence.
- The implementation is simple, auditable, and rules-based.

Weaknesses:

- Return capture is lower than SPY buy-and-hold.
- Full-period CAGR is lower than the 50/50 SPY/cash baseline.
- Validation-period CAGR and Sharpe lag both registered baselines.
- Validation-period drawdown is not better than the 50/50 SPY/cash baseline.
- Lower exposure remains a major explanatory candidate.
- Exit-rule perturbation behavior is uneven.
- Evidence is limited to one instrument and one implementation.
- Random max-drawdown percentile reporting requires direction-convention documentation before use.

Stress-test behavior:

- 20 bps and 50 bps round-trip cost tests remained positive but degraded.
- 1-day execution delay remained broadly similar to the base case.
- 200-day and 300-day breakout perturbations remained directionally similar.
- 50DMA exit perturbation was materially weaker.
- 150DMA exit perturbation was stronger than the base case.

Validation behavior:

- Validation-period return remained positive.
- Validation-period maximum drawdown improved versus buy-and-hold.
- Validation-period CAGR and Sharpe lagged both buy-and-hold and 50/50 SPY/cash.
- Validation-period drawdown was similar to the 50/50 SPY/cash baseline rather than better.

## Evidence Strength

Evidence strength: MODERATE

Justification:

The evidence includes a full-period backtest, registered baselines, random timing comparison, development and validation period review, subperiod review, cost stress, execution-delay stress, breakout perturbation review, and exit perturbation review. Hostile review found that the implementation survives with concerns. Evidence is not STRONG because validation-period performance lags both registered baselines, lower exposure remains a major explanatory candidate, exit-rule sensitivity is uneven, and the evidence is limited to SPY and one run.

## Deployability

Deployability: PRACTICAL

Justification:

HOSTILE_REVIEW_002 states that the rules are simple, auditable, and low-frequency enough for practical execution. Deployability is not EXCELLENT because return give-up, tax context, operational controls, paper validation, and exposure-baseline concerns remain unresolved.

## Official Decision

Official decision: WEAK_SUPPORT

Justification:

IMP_002 produced favorable evidence on random timing comparison, drawdown reduction versus buy-and-hold, higher-cost stress survival, execution-delay stress survival, and breakout lookback perturbation behavior. The decision is not SUPPORTED because RESULT_REVIEW_002 is MIXED, HOSTILE_REVIEW_002 is SURVIVES_WITH_CONCERNS, validation-period CAGR and Sharpe lag both registered baselines, full-period CAGR lags the 50/50 exposure baseline, and evidence remains limited to one instrument and one implementation.

## Claim Impact

Claim impact: WEAK_POSITIVE

Justification:

HOSTILE_REVIEW_002 records weak positive claim impact. This means IMP_002 provides limited implementation-level evidence relevant to CLAIM_001, not claim certification.

IMP_002 does not determine the status of CLAIM_001.

## Open Questions

Remaining concerns:

- Whether the return reduction versus buy-and-hold and 50/50 SPY/cash is acceptable for any intended use case.
- Whether the evidence is explained primarily by reduced exposure rather than breakout timing.
- Whether exit-rule sensitivity should reduce confidence in this implementation.
- Whether the random max-drawdown percentile field requires direction-convention documentation.
- Whether similar evidence appears outside SPY.
- Whether paper validation confirms the operational behavior.
- Whether taxes, turnover, execution, and operator workflow remain practical in the intended account context.

Required future evidence:

- Independent replication of BACKTEST_002.
- Paper validation before deployment review.
- Review or documentation of the random max-drawdown percentile convention.
- Tax and turnover burden review.
- Additional implementations under CLAIM_001.

Required future implementations:

- IMP_003 — Turtle Breakout
- IMP_004 — Dual Momentum

## Required Next Actions

1. Update the CLAIM_001 status ledger without certifying CLAIM_001.
2. Launch IMP_003 — Turtle Breakout as the next evidence source for CLAIM_001.
3. Launch IMP_004 — Dual Momentum as a future evidence source for CLAIM_001.
4. Preserve IMP_002 positive and negative findings in the factory knowledge base.
5. Do not certify CLAIM_001 from IMP_002 alone.

## Decision Status

IMPLEMENTATION_DECISION_002: WEAK_SUPPORT
