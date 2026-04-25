---
id: C2_SLEEVE_EDGE_METRIC_V1
title: "C2 Sleeve Edge Metric Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-15
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_control_stack
---

# C2 Sleeve Edge Metric Contract v1

## Purpose

Define deterministic factual sleeve metrics calculated from the governed fact ledger only.

## Required output families

- native factual performance
- adopted factual management performance
- execution factual performance
- data sufficiency and validity
- explicit unknown-attribution impact count

## Required constraints

- no confidence multiplier
- no trust float
- no allocation merit score
- no allocator policy embedded in metric calculation
- no regime logic in the control path
- `UNKNOWN_ATTRIBUTION` rows must remain excluded from both native and adopted metric families
- unknown-attribution impact must be surfaced as an explicit count and must not be hidden inside generic invalidity
- count-based unknown-attribution governance is permitted in v1 because the count is explicit
- fraction-based unknown-attribution governance must not be introduced until its denominator is an explicit governed metric surface with a compatible versioned contract

## Unsupported metrics

Metrics that are not grounded in governed truth must publish explicit unavailable semantics with reason codes.

This applies to:

- per-risk expectancy when capital-at-risk truth is not bound
- budget utilization when sleeve budget truth is not bound
- slippage drag when decision-price truth is not bound
- capital efficiency when no governed denominator is bound

## Audit expectations

- metric calculation version must be explicit
- window definition must be explicit
- invalidity reasons must be explicit
- unavailable metrics must publish explicit unavailable semantics rather than invented values
- frozen snapshots may also publish a compact top-level `unavailable_metrics[]` summary for audit readability only
- additive unavailable-metric visibility must not change qualification semantics or allocator behavior

## Versioning rule

Any change to metric definitions, windows, or unavailable semantics requires an explicit version bump.
