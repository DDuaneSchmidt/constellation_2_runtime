---
id: C2_STEP_RESULT_ENVELOPE_V1
title: "C2 Step Result Envelope Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Step Result Envelope Contract (V1)

## Purpose

This contract defines the canonical governed result envelope for control-plane workflow steps.

The envelope standardizes the shape returned by authority, readiness, preflight, and repair steps.

## Required meaning

A governed step result envelope must distinguish:

- step id
- status
- artifact refs
- counts
- warnings
- blocking defects
- diagnostics
- schema refs

## Fail-closed rule

Control-plane steps must not return ambiguous raw values whose meaning depends on caller assumptions.

If a step cannot express its outcome through this envelope shape, the step must fail closed.

## Non-claims

This contract governs the return envelope shape only.

It does not require standalone runtime materialization for every envelope instance.
