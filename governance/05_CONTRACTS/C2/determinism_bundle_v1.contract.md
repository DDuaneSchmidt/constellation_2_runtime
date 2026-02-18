---
id: C2_DETERMINISM_BUNDLE_V1
title: "Determinism Bundle v1 (Inputs Freeze + Data Integrity + Replay Proof)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - determinism
  - replay
  - data-integrity
  - audit-grade
---

# Determinism Bundle v1

## 1. Objective

Provide a single governed, immutable artifact per day that answers:

- “What inputs were frozen?”
- “Was input data integrity acceptable?”
- “Can this day be replayed to the same canonical daily state hash?”

## 2. Path

- `constellation_2/runtime/truth/pillars_v1/<DAY>/bundles/determinism_bundle.v1.json`

## 3. Required fields

- `day_utc`
- `produced_utc` (deterministic day-scoped; must not require wall clock)
- `producer` (repo/module/git_sha)
- `status` in {`OK`, `DEGRADED`, `FAIL`}
- `reason_codes` (stable enumerations)
- `input_manifest` (type/path/sha256)

## 4. Determinism anchors

Must include hashes for:
- inputs_frozen.v1.json sha256
- daily_execution_state.v1.json sha256
- event_ledger.v1.jsonl sha256 (if present)

## 5. Data integrity summary (v1 minimum)

Must include, at minimum:
- presence/absence of required market data snapshot inputs for the day
- any detected missing/empty directories referenced as inputs

This bundle MUST fail-closed if any referenced input is missing.

## 6. Replay proof (v1 phased)

v1 permits two modes:

- `replay_proof_mode: "NOT_IMPLEMENTED"` (allowed for early paper, status must be DEGRADED if replay proof is missing)
- `replay_proof_mode: "HASH_MATCH"` (preferred)
  - must include a replay output hash and prove it equals the canonical daily_execution_state hash.

## 7. Non-negotiable

This bundle must be day-keyed and immutable.
It must never be written for a future day (C2_TEST_DAY_QUARANTINE_POLICY_V1).
