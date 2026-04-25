---
id: C2_RECONCILED_TRADE_DESCRIPTION_CONTRACT_V1
title: "C2 Reconciled Trade Description Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_trade_description
---

# C2 Reconciled Trade Description Contract V1

## Canonical owner

- `reconciled_trade_description.v1`

## Purpose

This artifact is a derived-only descriptive read model over `incorporated_broker_trade_state.v1`.

## Derived-only rule

`reconciled_trade_description.v1` must derive from:

- `incorporated_broker_trade_state.v1`
- governed descriptive rules

It must not derive directly from raw broker evidence except through those governed incorporated-state fields.

## Required descriptive domains

At minimum the description layer must provide:

- lifecycle descriptive status
- protection descriptive status
- reconciliation descriptive status
- downstream posture of `SAFE`, `DEGRADED`, or `BLOCKED`

## Non-ownership

This artifact must not become a truth owner.

It must not:

- authorize actions
- decide stop movement
- decide close or reduce policy
- transmit broker instructions

