---
id: C2_PAPER_SESSION_BLOCKER_LEDGER_V1
title: "C2 Paper Session Blocker Ledger Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_admission
---

# C2 Paper Session Blocker Ledger Contract v1

## Purpose

This contract defines the complete blocker ledger emitted by paper-session closure.

## Required meaning

The blocker ledger must enumerate every closure blocker discovered in the declared graph, including:

- missing dependencies
- schema failures
- freshness failures
- ambiguity failures
- readiness failures
- authority failures

## Canonical runtime instance path

`constellation_2/runtime/truth/reports/paper_session_blocker_ledger_v1/<DAY>/paper_session_blocker_ledger.v1.json`

## Fail-closed rules

If closure fails and no blocker ledger is emitted, the session state is invalid and must fail closed.
