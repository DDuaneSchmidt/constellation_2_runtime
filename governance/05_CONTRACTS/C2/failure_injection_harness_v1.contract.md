---
id: C2_FAILURE_INJECTION_HARNESS_CONTRACT_V1
title: "C2 Failure Injection Harness Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-02-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Failure Injection Harness Contract (V1)

## Purpose

Provide **empirical proof scaffolding** for:
- gate precedence correctness
- fail-closed behavior under stressed conditions
- reproducible stress scenario reporting

## Output

The harness MUST write immutable reports to:

- `constellation_2/runtime/truth/reports/failure_injection_harness_v1/<DAY>/<SCENARIO>/failure_injection_harness.v1.json`

## Non-mutation constraint

The harness MUST NOT rewrite production truth.

If full sandbox injection is required, production tools must first support an explicit `TRUTH_ROOT` override.

## Minimum scenarios

The harness must support deterministic scenario identifiers, including at minimum:
- CORR_SPIKE_080
- MARK_LAG_300S
- NAV_MISSING
- ATTRIBUTION_MISSING
- REPLAY_DRIFT
