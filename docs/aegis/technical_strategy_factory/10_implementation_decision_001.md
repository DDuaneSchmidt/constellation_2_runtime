# IMPLEMENTATION_DECISION_001

## Purpose

Record the official factory decision for IMP_001 — SPY 200DMA Trend Following based on generated evidence and review artifacts.

This document does not certify CLAIM_001.

## References

```text
CLAIM_001
IMP_001
BACKTEST_001
RESULT_REVIEW_001
HOSTILE_REVIEW_001
```

## Evidence Summary

Primary findings:

- RESULT_REVIEW_001 classified the implementation result as MIXED.
- HOSTILE_REVIEW_001 classified the hostile verdict as SURVIVES_WITH_CONCERNS.
- IMP_001 reduced volatility and drawdown materially versus SPY buy-and-hold.
- IMP_001 improved Sharpe versus SPY buy-and-hold and 50/50 SPY/cash.
- IMP_001 did not match SPY buy-and-hold full-period or validation-period CAGR.

Strengths:

- Full-period max drawdown improved versus SPY buy-and-hold.
- Random timing comparison was favorable on CAGR and Sharpe.
- Higher cost, execution delay, and nearby parameter perturbation reviews did not erase the evidence.
- The implementation is simple, auditable, and rules-based.

Weaknesses:

- Return capture is lower than SPY buy-and-hold.
- Validation-period CAGR materially lagged SPY buy-and-hold.
- Validation-period drawdown and ulcer profile were worse than the 50/50 SPY/cash baseline.
- Evidence is limited to one instrument and one implementation.
- Random max-drawdown percentile reporting requires review or direction-convention documentation.

Stress-test behavior:

- 20 bps and 50 bps round-trip cost tests remained positive but degraded.
- 1-day execution delay remained broadly similar to the base case.
- 180DMA and 220DMA perturbations remained directionally similar; 180DMA was weaker.

Validation behavior:

- Validation-period CAGR and Sharpe remained positive.
- Validation-period Sharpe was slightly above buy-and-hold.
- Validation-period CAGR lagged buy-and-hold.
- Validation-period lower-exposure comparison remained a concern.

## Evidence Strength

Evidence strength: MODERATE

Justification:

The evidence includes a full-period backtest, registered baselines, random timing comparison, development and validation period review, subperiod review, cost stress, execution-delay stress, and parameter perturbation review. Hostile review found that the implementation survives with concerns. Evidence is not STRONG because it is limited to SPY, one implementation, one run, and unresolved random max-drawdown percentile interpretation.

## Deployability

Deployability: PRACTICAL

Justification:

HOSTILE_REVIEW_001 states that the rules are simple, auditable, and low-turnover enough for practical deployment review. Deployability is not EXCELLENT because tax burden, brokerage execution, operator workflow, paper validation, and return tradeoff remain unresolved.

## Official Decision

Official decision: WEAK_SUPPORT

Justification:

IMP_001 produced favorable evidence on drawdown reduction, risk-adjusted return, random timing comparison, and stress survival. The decision is not SUPPORTED because the result review is MIXED, hostile review is SURVIVES_WITH_CONCERNS, validation-period CAGR lags buy-and-hold, and evidence is limited to a single implementation and single instrument.

## Claim Impact

Claim impact: WEAK_POSITIVE

Justification:

HOSTILE_REVIEW_001 records weak positive claim impact. This means IMP_001 provides limited implementation-level evidence relevant to CLAIM_001, not claim certification.

IMP_001 does not determine the status of CLAIM_001.

## Open Questions

Remaining concerns:

- Whether the return reduction versus buy-and-hold is acceptable for any intended use case.
- Whether the random max-drawdown percentile field requires repair or direction-convention documentation.
- Whether similar evidence appears outside SPY.
- Whether paper validation confirms the operational behavior.
- Whether taxes, turnover, execution, and operator workflow remain practical in the intended account context.

Required future evidence:

- Independent replication of BACKTEST_001.
- Paper validation before deployment review.
- Review or correction of the random max-drawdown percentile interpretation.
- Tax and turnover burden review.
- Additional implementations under CLAIM_001.

Required future implementations:

- IMP_002 — 52-Week Breakout
- IMP_003 — Turtle Breakout
- IMP_004 — Dual Momentum

## Required Next Actions

1. Launch IMP_002 — 52-Week Breakout as the next evidence source for CLAIM_001.
2. Launch IMP_003 — Turtle Breakout as a future evidence source for CLAIM_001.
3. Launch IMP_004 — Dual Momentum as a future evidence source for CLAIM_001.
4. Preserve IMP_001 negative and positive findings in the factory knowledge base.
5. Do not certify CLAIM_001 from IMP_001 alone.

## Decision Status

IMPLEMENTATION_DECISION_001: WEAK_SUPPORT
