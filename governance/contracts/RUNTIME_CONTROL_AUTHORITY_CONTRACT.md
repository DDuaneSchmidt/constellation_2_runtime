---
id: C2_RUNTIME_CONTROL_AUTHORITY_CONTRACT_V1
title: "Constellation 2.0 Runtime Control Authority Contract"
status: DRAFT
version: 1
created_utc: 2026-04-13
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_runtime_control_authority
---

# Runtime Control Authority Contract

## Purpose

This contract defines the smallest governed runtime-control truth needed by the existing PAPER submit consequence gate.

## Authority split

- `RuntimeControlRecord` is the long-lived allow/block truth.
- `RuntimeControlDecision` is a pure evaluation artifact and is not the long-lived truth.
- `RuntimeControlRunEnvelope` is immutable audit evidence and is not the allow/block truth.

## Allowed control states

The first kernel version allows only:

- `ALLOW`
- `BLOCKED`

## Allowed decision outcomes

- `allow`
- `blocked`
- `duplicate`
- `no_action`

`no_action` is reserved for repo-compatible future use and is not required by the first implementation.

## Required fields

`RuntimeControlRecord` must include:

- stable record identity
- scope (`capability_scope`, `day_utc`, `environment`, `ib_account`, `sleeve_id`)
- current control state
- effective time
- kill-switch state and `allow_entries`
- readiness state and `ok`
- evidence lineage refs
- deterministic evidence fingerprint
- reason codes

## Evidence semantics

- kill-switch truth is authoritative only when the canonical artifact is present and authority-consistent
- readiness truth is authoritative only when the governed history artifact is present, schema-valid, account-valid, environment-valid, day-valid, and provenance-bound to the explicit sleeve execution root
- contradictory control evidence must fail closed

## Consequence-gate rule

The hard PAPER submit consequence gate may proceed only when the runtime-control kernel returns or confirms a `RuntimeControlRecord` whose `control_state == "ALLOW"`.

## Non-authoritative surfaces

The following must remain non-authoritative for runtime allow/block truth:

- `operator_gate_verdict`
- `session_authority_status`
- `session_authority_alert`
- `paper_day_control_plane`
- `trading_day_control_plane`
- `capability_state`

