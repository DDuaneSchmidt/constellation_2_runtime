---
id: C2_PAPER_SESSION_PRODUCER_ATTEMPTS_V1
title: "C2 Paper Session Producer Attempts Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-07
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_admission
---

# C2 Paper Session Producer Attempts Contract v1

## Purpose

This contract defines the governed evidence artifact recording producer attempts executed under paper-session admission control.

## Required meaning

Producer-attempt evidence must record:

- which governed producers were attempted
- deterministic attempt order
- dependency keys covered by each producer
- command/entrypoint used
- pass/fail/partial result
- produced output paths
- stdout/stderr evidence

## Authority boundary

Producer-attempt evidence is subordinate to closure.

It records producer execution under admission control but does not itself authorize execution.

## Canonical runtime instance path

`constellation_2/runtime/truth/reports/paper_session_producer_attempts_v1/<DAY>/paper_session_producer_attempts.v1.json`

## Fail-closed rules

If admission attempts governed producers but no producer-attempt artifact is emitted, the admission result is invalid and must fail closed.

Producer-attempt evidence must not suppress remaining blockers.
