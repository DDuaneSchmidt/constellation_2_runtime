---
id: C2_GATE_HIERARCHY_CONTRACT_V1
title: "C2 Gate Hierarchy & Precedence Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-02-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Gate Hierarchy & Precedence Contract (V1)

## Purpose

Define an **institutional, deterministic enforcement lattice** for Constellation 2.0.

This contract eliminates:
- ambiguous gate precedence
- soft/strict confusion
- silent override paths
- gate duplication regressions

## Gate classes

Gates are classified into one of four classes:

1. **CLASS1_SYSTEM_HARD_STOP**
   - Blocks all submissions.
   - Cannot be overridden by operator action.

2. **CLASS2_RISK_HARD_STOP**
   - Blocks submissions until resolved.
   - May be overridden **only** if the specific gate’s governed override artifact exists and is valid.

3. **CLASS3_CONTROLLED_DEGRADATION**
   - Allows reduced operation.
   - Must produce explicit DEGRADED state and reason codes.

4. **CLASS4_ADVISORY**
   - Non-blocking; informational.

## Precedence

Evaluation precedence is strict:

CLASS1 > CLASS2 > CLASS3 > CLASS4

Within-class ordering is deterministic and must be stable under replay.

## Registry authority

The authoritative registry is:

- `governance/02_REGISTRIES/GATE_HIERARCHY_V1.json`

This registry defines:
- which gates are in scope
- each gate’s class
- whether the gate is required and/or blocking
- the artifact path template and pass criteria

## Consolidated verdict

A single consolidated verdict MUST be emitted for each day under:

- `constellation_2/runtime/truth/reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json`

If any required gate is not passing, the consolidated verdict must be FAIL.

## Fail-closed rules

- Missing required gate artifact => FAIL
- Parse error in required gate artifact => FAIL
- Any class1 fail => FAIL

## Non-negotiable purity

Gate evaluation must be pure:
- no mutation of input truth
- no rewrite of governed artifacts
- immutable output only
