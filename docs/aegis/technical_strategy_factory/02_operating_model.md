# AEGIS Technical Strategy Factory Operating Model

## Purpose

This document defines how research moves through the AEGIS Technical Strategy Factory.

## Core Objects

Claim: statement about market behavior.

Implementation: specific rules attempting to exploit a claim.

Evidence: test results, hostile reviews, paper validation, and audit records.

Decision: support, weak support, insufficient evidence, falsification, or retirement.

Knowledge: accumulated supported and negative findings.

## Lifecycle States

- `PROPOSED`
- `UNDER_INVESTIGATION`
- `WEAKLY_SUPPORTED`
- `SUPPORTED`
- `INSUFFICIENT_EVIDENCE`
- `FALSIFIED`
- `RETIRED`

## Research Flow

1. Claim proposed.
2. Implementation specified.
3. Test plan pre-registered.
4. Evidence collected.
5. Hostile review conducted.
6. Decision recorded.
7. Knowledge updated.

## Mandatory Implementation Rule

No implementation may support a claim without a frozen implementation specification and pre-registered test plan.

## Negative Knowledge Requirements

Every rejected implementation or claim must record:

- Why it was tested.
- What failed.
- Why it was rejected.
- What remains unknown.
- Whether related future research is blocked, allowed, or deferred.

## Capital Allocation Boundary

This factory does not allocate capital. It may only determine whether evidence is sufficient for possible review by an allocation process.
