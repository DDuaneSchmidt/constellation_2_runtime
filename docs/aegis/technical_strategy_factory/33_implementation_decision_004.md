# IMPLEMENTATION_DECISION_004

## Purpose

Record the official factory decision for IMP_004 — Dual Momentum based solely on generated evidence and review artifacts.

This document does not certify CLAIM_001.

## References

```text
CLAIM_001
IMP_004
BACKTEST_004
RESULT_REVIEW_004
HOSTILE_REVIEW_004
```

## Evidence Summary

Primary findings:

- RESULT_REVIEW_004 classified the implementation result as FAVORABLE.
- HOSTILE_REVIEW_004 classified the hostile verdict as SURVIVES_WITH_CONCERNS.
- IMP_004 exceeded SPY buy-and-hold on full-period total return, CAGR, Sharpe, Sortino, maximum drawdown, and ulcer index.
- IMP_004 exceeded the 50/50 SPY/cash baseline on full-period total return, CAGR, Sharpe, Sortino, and ulcer index.
- Validation-period evidence was mixed.

Strengths:

- Full-period evidence was favorable versus both registered baselines.
- Random timing comparison was favorable on CAGR and Sharpe.
- Higher cost and execution-delay stress tests remained close to the base case.
- Registered lookback and evaluation-frequency perturbations remained directionally positive.
- Rules are simple, monthly, auditable, and low turnover.

Weaknesses:

- Validation-period CAGR and Sharpe lagged buy-and-hold.
- Validation-period Sharpe lagged the 50/50 SPY/cash baseline.
- Validation-period drawdown was worse than the 50/50 SPY/cash baseline.
- Full-period maximum drawdown was slightly worse than the 50/50 SPY/cash baseline.
- Subperiod weaknesses were visible in 2020 and 2022.
- Evidence is limited to SPY and tests only the absolute momentum component using SPY and cash.

Stress-test behavior:

- 20 bps and 50 bps round-trip cost tests remained close to the base case.
- 1-day execution delay remained close to the base case.
- 9-month lookback perturbation was weaker but directionally positive.
- 15-month lookback perturbation was stronger than the base case.
- Weekly and quarterly evaluation perturbations remained positive but weaker than the base case.

Validation behavior:

- Validation-period return remained positive.
- Validation-period CAGR exceeded the 50/50 SPY/cash baseline.
- Validation-period CAGR lagged buy-and-hold.
- Validation-period Sharpe lagged both registered baselines.
- Validation-period drawdown was worse than the 50/50 SPY/cash baseline.

## Evidence Strength

Evidence strength: STRONG

Justification:

The evidence includes a full-period backtest, registered baselines, random timing comparison, development and validation period review, subperiod review, cost stress, execution-delay stress, lookback perturbation review, and evaluation-frequency perturbation review. Evidence strength is STRONG because RESULT_REVIEW_004 was FAVORABLE, HOSTILE_REVIEW_004 found that the implementation survives with concerns, random timing comparison was favorable, and stress tests remained directionally positive. Evidence strength is not EXCEPTIONAL because validation-period evidence was mixed, the run is limited to SPY, and paper validation and independent replication remain open.

## Deployability

Deployability: PRACTICAL

Justification:

HOSTILE_REVIEW_004 states that the rules are simple, monthly, auditable, and low turnover. Deployability is not EXCELLENT because paper validation, tax review, operational controls, and acceptance of validation-period baseline tradeoffs remain unresolved.

## Official Decision

Official decision: WEAK_SUPPORT

Justification:

IMP_004 produced favorable full-period evidence versus both registered baselines, materially beat random timing on CAGR and Sharpe, and survived higher costs, execution delay, and perturbation review. The decision is not SUPPORTED because HOSTILE_REVIEW_004 is SURVIVES_WITH_CONCERNS, validation-period Sharpe lagged both baselines, validation-period drawdown lagged the 50/50 baseline, and evidence is limited to one SPY implementation.

## Claim Impact

Claim impact: WEAK_POSITIVE

Justification:

HOSTILE_REVIEW_004 records weak positive claim impact. This means IMP_004 adds favorable implementation-level evidence to the CLAIM_001 ledger. It does not certify CLAIM_001.

IMP_004 does not determine the status of CLAIM_001.

## Open Questions

Remaining concerns:

- Whether validation-period underperformance versus buy-and-hold on CAGR and Sharpe is acceptable for any intended use case.
- Whether validation-period drawdown weakness versus the 50/50 SPY/cash baseline limits implementation support.
- Whether the result generalizes beyond SPY.
- Whether the absolute-momentum-only pilot is sufficient representation of dual momentum.
- Whether the random max-drawdown percentile field requires direction-convention documentation.

Required future evidence:

- Independent replication of BACKTEST_004.
- Paper validation before deployment review.
- Tax and operational burden review.
- Claim-level synthesis across IMP_001, IMP_002, IMP_003, and IMP_004.

Areas needing replication:

- Monthly 12-month absolute momentum on SPY using an independent implementation.
- Data-source replication using an independent adjusted OHLC source.
- Broader dual momentum variants if the factory chooses to test beyond the SPY-only pilot.

## Required Next Actions

1. Create CLAIM_001_STATUS_UPDATE_003.
2. Perform the first full claim-level review using IMP_001, IMP_002, IMP_003, and IMP_004.
3. Preserve IMP_004 supporting and cautionary findings in the factory knowledge base.
4. Do not certify CLAIM_001 from IMP_004 alone.

## Decision Status

IMPLEMENTATION_DECISION_004: WEAK_SUPPORT
