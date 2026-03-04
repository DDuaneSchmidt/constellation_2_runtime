---
id: C2_ORCHESTRATOR_ABORT_POLICY_V1
title: "Constellation 2.0 — Orchestrator ABORT Policy V1 (Meaningful Safety Breaches Only)"
status: DRAFT
version: 1
created_utc: 2026-03-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - orchestrator
  - abort_policy
  - fail_closed
  - safety_breach
  - activity_gating
  - non_bricking
---

# Orchestrator ABORT Policy V1

## Objective
Prevent “fragile orchestrator” outcomes by restricting ABORT to true safety breaches.

## ABORT definition
ABORTED means: "do not operate / do not trade" due to safety breach.

Orchestrator MUST exit non-zero only when ABORTED.

## Allowed ABORT causes (safety breaches)
ABORT is permitted only for:
- IB account mismatch or topology violation (not DUO847203 in single-account mode)
- Global kill switch ACTIVE (explicit safety stop)
- Integrity corruption (invalid pointer index, lock failure, schema corruption of authoritative artifacts)
- Explicit operator safety stop directives

## Disallowed ABORT causes
Orchestrator MUST NOT ABORT for:
- No-activity days
- Missing optional stages
- Missing conditional market context when no positions exist
- Stages that are activity-gated and not required for the day

## Activity gating rule
Stages that require market context MUST be activity-gated:
- If activity=false → stage is not required and must not be blocking
- If activity=true → stage may become required and blocking per its contract

## Verdict output guarantee
Even on FAIL/DEGRADED outcomes, orchestrator MUST write:
- attempt manifest
- run verdict
- pipeline manifest v3 and compat manifests where applicable
