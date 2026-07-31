# Aegis Outcome Validation Maturity Requirements v1

## Purpose

Convert paper-trading evidence into deterministic validation proof at the hypothesis level. The chain is: Hypothesis -> Candidate -> Sleeve -> Paper Position -> Outcome -> Validation Sample -> Statistical Result -> Hypothesis State.

## Non-Goals

This work does not implement autonomous broker execution, tax harvesting, real-capital promotion, Oak Harvest replacement, or another allocation engine. Open unrealized P&L must not be treated as statistical proof.

## Outcome Lifecycle

Every paper position must have one outcome state:
- `OPEN`
- `CLOSED_WIN`
- `CLOSED_LOSS`
- `CLOSED_FLAT`
- `EXPIRED`
- `INVALIDATED`
- `UNKNOWN_BLOCKED`

`UNKNOWN_BLOCKED` is allowed only when required evidence is missing and must include blocker reason codes.

## Validation Sample Contract

Resolved outcomes become validation samples when lineage and exit/return evidence are complete. Open positions are represented as excluded samples, not proof. Allowed sample states are `INCLUDED`, `EXCLUDED_OPEN_POSITION`, `EXCLUDED_MISSING_EXIT`, `EXCLUDED_INVALIDATED`, `EXCLUDED_BAD_LINEAGE`, `EXCLUDED_BAD_MARK`, and `EXCLUDED_OTHER`.

## Hypothesis Outcome Ledger

Every hypothesis in the research portfolio must have a ledger row with candidate count, paper position count, open/closed counts, win/loss/flat counts, expectancy, sample counts, and validation readiness state. Confidence cannot be calculated from open trades alone.

## Statistical Sufficiency

Sufficiency is deterministic and threshold-based. Initial conservative defaults are explicit and versioned. Closed or otherwise resolved usable samples are required for validation-ready, validated, or disproven states.

## Audit Requirements

Daily self-check must verify outcome coverage for every paper position, validation sample lineage, closed trade return evidence, blocker reasons, hypothesis ledger coverage, sufficiency reason codes, and transition evidence.
