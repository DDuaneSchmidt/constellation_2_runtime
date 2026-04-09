---
id: C2_RECURRENCE_FINGERPRINT_V1
title: "C2 Recurrence Fingerprint Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_recurrence_fingerprint
---

# C2 Recurrence Fingerprint Contract (V1)

## Purpose

This contract defines the deterministic fingerprint used to classify a recurrence family or a
success-verification family without overfitting daily incidental details.

## Inputs

The canonical fingerprint input model is:

- `blocker_family`
- `blocker_code`
- `authority_source`
- `stage_id`

## Required rules

- Fingerprints must be deterministic and stable across reruns from the same family.
- Fingerprints must not include timestamps, free-form messages, or other ephemeral values.
- `release_id` and `git_sha` are intentionally deferred from the fingerprint input because they
  are audit evidence, not family identity.
- Allowed blocker families for V1 are:
  - `SUCCESS_VERIFICATION`
  - `PROOF_INVALID`
  - `DEPLOYMENT_BLOCK`
  - `DAY_START_BLOCK`

## Canonical implementation

- Canonical helper:
  - `constellation_2/common/recurrence_fingerprint_v1.py`

