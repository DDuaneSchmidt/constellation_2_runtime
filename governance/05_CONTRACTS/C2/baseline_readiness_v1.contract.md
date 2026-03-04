---
id: C2_BASELINE_READINESS_V1
title: "Constellation 2.0 — Baseline Readiness Certification V1 (Pre-Open Non-Activity Run)"
status: DRAFT
version: 1
created_utc: 2026-03-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - readiness
  - baseline
  - pre_open
  - nav
  - non_bricking
  - certification
---

# Baseline Readiness Certification V1

## Objective
Define the minimum deterministic pass criteria to declare Constellation ready for the trading day before market open.

Baseline run means:
- No intents required
- No executions expected
- No positions required
- Must produce economic truth (NAV) from operator cash seed

## Baseline prerequisites (required)
Before baseline run:
- operator statement exists for day_utc
- submissions day directory exists or is created by orchestrator

## Baseline outputs (required)
Baseline run MUST produce:
- positions snapshot v2
- cash ledger snapshot v1
- accounting NAV v2 with:
  - status == ACTIVE
  - nav_total > 0
- orchestrator run verdict v2 with status != ABORTED

## Baseline allowed verdict status
Baseline may return:
- PASS
- DEGRADED (common; NO_ACTIVITY_DAY)
- FAIL (if non-safety required stage fails)

Baseline MUST NOT return ABORTED unless a true safety breach per C2_ORCHESTRATOR_ABORT_POLICY_V1.

## Baseline prohibited states
- NAV status == BOOTSTRAP for funded baseline day
- nav_total <= 0 for funded baseline day
- account mismatch
