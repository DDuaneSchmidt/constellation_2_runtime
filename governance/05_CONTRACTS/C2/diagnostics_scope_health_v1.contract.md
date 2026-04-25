---
id: C2_DIAGNOSTICS_SCOPE_HEALTH_CONTRACT_V1
title: "C2 Diagnostics Scope Health Contract v1"
status: DRAFT
version: 1
created_utc: 2026-03-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Diagnostics Scope Health Contract v1

## Purpose

Make diagnostics and operator state scope-aware in PAPER mode.

Authority split is explicit:

- Execution authority: sleeve truth (`truth_sleeves/<sleeve_id>/PAPER`)
- Monitoring authority: global truth (`/home/node/constellation_runtime_data/truth` monitoring and control-panel input surfaces)

## Required scope outputs

`constellation_runtime_state.v1.json` MUST expose three distinct status domains:

1. `sleeve_execution_health`
2. `system_monitoring_health`
3. `overall`

`overall` MUST be derived from explicit rules, not inferred by collapsing global freshness into execution authority.

## PAPER semantics

When sleeve execution is PASS and monitoring is degraded/stale:

- sleeve execution health remains PASS (authoritative in scope)
- system monitoring health is DEGRADED or FAIL
- overall status is `PARTIALLY_PROVEN`

## Freshness policy binding

Diagnostics freshness MUST be evaluated against:

- `governance/02_REGISTRIES/C2_DIAGNOSTICS_FRESHNESS_POLICY_V1.json`

The runtime state builder MUST fail closed if this policy is missing or invalid.

## Required freshness semantics

At minimum, diagnostics freshness policy MUST define expectations for:

- `paper_readiness`
- `lifecycle_monitor`
- `capital_authority_allocation`
- control-panel/system-snapshot inputs used by AI control panel entry flow

## Orchestrator dependency expectation

PAPER orchestration should produce or attempt production of monitoring surfaces (lifecycle monitor and paper readiness) in sleeve flow, but failures remain non-blocking to execution authority unless safety invariants are breached.

## Non-goals

- This contract does not redefine strategy or broker execution logic.
- This contract does not replace gate-stack or safety-breach enforcement.
