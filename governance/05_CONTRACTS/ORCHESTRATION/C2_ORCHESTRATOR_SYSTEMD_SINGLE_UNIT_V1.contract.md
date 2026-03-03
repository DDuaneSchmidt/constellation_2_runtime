---
id: C2_ORCHESTRATOR_SYSTEMD_SINGLE_UNIT_V1
title: "Constellation 2.0 — Orchestrator Systemd Single-Unit Policy (No A/B Instances)"
status: DRAFT
version: 1
created_utc: 2026-03-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - orchestrator
  - systemd
  - single_account
  - single_unit
  - sleeves
  - intents
---

# Orchestrator Systemd Single-Unit Policy (V1)

## Objective
Eliminate confusing multi-instance orchestrator scheduling (A/B) now that sleeves generate symbol-scoped intents.

## Policy
1) There MUST be exactly one scheduled PAPER day orchestrator unit:
   - `c2-paper-day-orchestrator.service`
   - (optionally) `c2-paper-day-orchestrator.timer`

2) All “instance” orchestrator units MUST NOT exist and MUST NOT be enabled:
   - `c2-paper-day-orchestrator-A.service` / `.timer`
   - Any `c2-paper-day-orchestrator-B-*.service` / `.timer`

3) Symbol fanout is expressed ONLY through sleeve intent generation, not systemd fanout.

4) Single-account topology remains enforced:
   - Any orchestrator unit MUST run with `--ib_account DUO847203` in single-account mode.

## Rationale
- Removes operational ambiguity (“Which orchestrator is authoritative?”).
- Prevents reintroduction of multi-account assumptions via systemd configuration drift.
- Aligns control plane: **orchestrator runs once, sleeves decide activity**.
