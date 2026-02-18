---
id: C2_EXECUTION_QUALITY_BUNDLE_V1
title: "Execution Quality Bundle v1 (Slippage + Latency + Fill Quality)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - execution
  - slippage
  - latency
  - fill-quality
  - realism
---

# Execution Quality Bundle v1

## 1. Objective

Provide a single governed, immutable artifact per day that quantifies execution realism.

This addresses hostile assumptions:
- slippage underestimated
- fills unrealistic
- latency ignored

## 2. Path

- `constellation_2/runtime/truth/pillars_v1/<DAY>/bundles/execution_quality_bundle.v1.json`

## 3. Required fields

- `day_utc`
- `produced_utc` (deterministic day-scoped)
- `producer` (repo/module/git_sha)
- `status` in {`OK`, `DEGRADED`, `FAIL`}
- `reason_codes`
- `input_manifest`

## 4. v1 minimum metrics (allowed to be DEGRADED if missing inputs)

If broker event log exists:
- counts: submissions, acks, fills, cancels, rejects (as available)
- latency: submission->ack (if timestamps present), ack->fill (if timestamps present)

If timestamps are missing, status must be DEGRADED with reason codes.

## 5. Non-negotiable

This bundle must never be used as an input to submission gating (informational).
It must be immutable and day-keyed.
