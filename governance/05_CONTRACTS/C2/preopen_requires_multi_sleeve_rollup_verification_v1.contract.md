---
id: C2_PREOPEN_REQUIRES_MULTI_SLEEVE_ROLLUP_VERIFICATION_CONTRACT_V1
title: "C2 Preopen Requires Multi-Sleeve Rollup Verification Contract v1"
status: DRAFT
version: 1
created_utc: 2026-03-04
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Preopen Requires Multi-Sleeve Rollup Verification Contract v1

## Purpose

Ensure trading cannot begin unless the multi-sleeve global rollup pointer chain is valid and verifies the current day’s sleeve verdicts.

## Required preopen behavior

Preopen preflight MUST run a rollup verification step that fails closed if:

- The rollup pointer index does not exist  
  truth/reports/sleeve_rollup_v1/<DAY>/canonical_pointer_index.v1.jsonl

- The pointer index last entry cannot be parsed as JSON

- The pointer entry points_to file does not exist

- points_to_sha256 does not match the rollup artifact bytes

- The rollup JSON is missing required fields:
  schema_id  
  day_utc  
  status  
  sleeves

- Any sleeve has verdict_status in:
  ABORTED  
  FAIL

## Canonical verifier entrypoint

The repo MUST provide the deterministic verifier script:

ops/run/c2_verify_multi_sleeve_rollup_v1.sh

This verifier MUST:

- compute DAY_UTC deterministically
- resolve rollup pointer entry
- validate pointer index format
- validate artifact sha256
- validate rollup schema
- fail closed on invalid sleeves

## Non-claims

This contract does not define sleeve verdict schema.
It defines **preopen gating requirements only**.
