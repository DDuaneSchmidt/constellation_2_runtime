# AEGIS Claim Validation Standard

## Purpose

This document defines what evidence is required before a technical claim may be supported.

## Mandatory Evidence Categories

- Backtest
- Baseline comparison
- Walk-forward validation
- Hostile review
- Replication
- Paper validation
- Exploitability review

## Baseline Comparison

Baseline comparison must include:

- Passive baseline
- Risk/exposure-adjusted baseline
- Technical baseline where applicable
- Null/randomized baseline

## Hostile Review

Hostile review must include:

- Higher costs
- Execution delay
- Parameter perturbation
- Subperiod stress
- Regime stress
- Turnover/tax burden
- Hidden beta or factor exposure

## Evidence Strength Scale

- `VERY_WEAK`
- `WEAK`
- `MODERATE`
- `STRONG`
- `EXCEPTIONAL`

## Deployability Scale

- `POOR`
- `LIMITED`
- `PRACTICAL`
- `EXCELLENT`

## Support Standard

A claim may become `SUPPORTED` only if:

- Multiple reasonable implementations survive.
- Evidence is reproducible.
- Evidence survives hostile review.
- Evidence is not explained solely by baseline exposure or randomness.
- Deployment is practical enough for the intended use case.
- Known failure modes are documented.

## Falsification Standard

A claim may become `FALSIFIED` only if:

- Multiple reasonable implementations fail.
- Failures occur across relevant environments.
- No implementation survives hostile review.
- No deployable evidence remains.

## Caution

One successful implementation may weakly support an implementation, but it does not by itself support the broader claim.
