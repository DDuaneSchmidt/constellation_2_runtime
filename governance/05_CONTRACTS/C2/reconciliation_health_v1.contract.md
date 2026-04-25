---
id: C2_RECONCILIATION_HEALTH_CONTRACT_V1
title: "C2 Reconciliation Health Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_trade_reconciliation_health
---

# C2 Reconciliation Health Contract V1

## Canonical owner

- `reconciliation_health.v1`

## Purpose

`reconciliation_health.v1` governs whether an incorporated Core 2 trade-state object is trusted, degraded, or blocked for downstream reliance.

## States

- `TRUSTED`
- `DEGRADED`
- `BLOCKED`

## Trusted

Core 2 may report `TRUSTED` only when all of the following hold:

- Core 1 health is not blocked
- trade identity is resolved without ambiguity
- ownership classification is not ambiguous
- no position/order/fill contradiction remains unresolved
- freshness remains inside the trusted window
- sufficient broker evidence exists for the current incorporated state

## Degraded

Core 2 must report `DEGRADED` when current truth can be materialized but one or more non-fatal weaknesses remain, including:

- Core 1 is degraded but not blocked
- position evidence is absent and current state relies on fills/orders only
- freshness is outside the trusted window but inside the blocked window
- protective-order visibility is incomplete
- prior/current state change is explainable but incomplete

## Blocked

Core 2 must report `BLOCKED` when any blocker remains unresolved, including at minimum:

- upstream observation stale
- replay window unresolved
- sequence uncertainty too high
- insufficient broker evidence for relied-on current truth
- trade identity unresolved
- ownership ambiguous
- account mismatch
- instrument identity unresolved
- lineage unresolved
- position/order/fill inconsistency
- conflicting incorporated fills
- orphan-order conflict
- flat/open contradiction
- duplicate unresolved lineage
- foreign/manual activity suspected for would-be governed state
- unowned live position
- mixed ownership evidence
- current truth too stale for downstream action use
- last successful reconciliation outside allowed window

## Freshness rules

Governed freshness thresholds for v1:

- trusted window: age of latest relied-on broker fact <= 120 seconds
- degraded window: age > 120 seconds and <= 600 seconds
- blocked window: age > 600 seconds

Freshness is measured as `evaluation_utc - latest_observed_utc`.

## Downstream fail-closed rule

If `reconciliation_health.v1.current_state == BLOCKED`, downstream consumers must treat the trade state as fail-closed for action reliance.

This contract provides trust posture only. It does not authorize actions.

