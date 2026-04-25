---
id: C2_TRADE_INSTANCE_IDENTITY_V1
title: "C2 Trade Instance Identity Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_phasec
---

# Purpose

This contract defines what makes two same-day Phase C attempts the same governed trade instance versus a new governed trade instance.

# Canonical owner

- Canonical writer: `ops/tools/run_phasec_identity_materializer_day_v1.py`
- Canonical record: `execution_identity_record.v1.json`

# Identity basis

For PAPER Phase C equity flows, `trade_instance_id` is derived deterministically from:

- `day_utc`
- `attempt_id`
- `sleeve_id`
- `environment`
- `intent_id`
- `intent_hash`

This means:

- replay of the same governed attempt yields the same `trade_instance_id`
- a new governed same-day attempt yields a new `trade_instance_id`
- a new attempt with unchanged plan is classified as `NEW_INSTANCE_SAME_PLAN`

# Classification meanings

- `SAME_INSTANCE_REPLAY`: same `trade_instance_id`
- `NEW_INSTANCE_SAME_PLAN`: different `trade_instance_id`, same `plan_hash`
- `NEW_INSTANCE_NEW_PLAN`: different `trade_instance_id`, different `plan_hash`
- `FIRST_INSTANCE`: no prior active instance for the same intent/day scope

# Prohibitions

- `attempt_id` alone is not the broker submission identity.
- Deleting prior evidence, mutating old bindings, or using randomness to force a new instance is forbidden.

