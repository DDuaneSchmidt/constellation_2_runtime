---
id: C2_SLEEVE_EDGE_QUALIFICATION_V1
title: "C2 Sleeve Edge Qualification Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-15
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_control_stack
---

# C2 Sleeve Edge Qualification Contract v1

## Purpose

Define the canonical deterministic policy layer that converts factual sleeve metrics into governed bands, states, and reason codes.

## Inputs

- factual sleeve metrics only
- governed threshold policy only

## Outputs

- `edge_band`
- `execution_health_band`
- `sample_sufficiency_band`
- `drift_band`
- `qualification_state`
- `reason_codes`

## Prohibited couplings

- no capital allocation decision
- no direct headroom sizing
- no recomputation of raw facts
- no discretionary override language

## Required fail-closed behavior

- `MEASUREMENT_INVALID` when required factual inputs are incomplete or contradictory
- `INSUFFICIENT_DATA` when facts are valid but sample sufficiency is below policy minimum
- `UNKNOWN_ATTRIBUTION` exclusions must propagate through explicit reason codes and must not be silently ignored
- unknown-attribution tolerance must be governed by `C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1.json` and enforced deterministically by count
- every non-`QUALIFIED` state must emit explicit reason codes

## Versioning rule

Thresholds and state mappings are governed by `C2_SLEEVE_EDGE_QUALIFICATION_POLICY_V1.json`.

Any change to threshold meaning or state semantics requires a policy version bump.
