---
id: C2_SUBMISSION_IDENTITY_V1
title: "C2 Submission Identity Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_submission_identity
---

# Purpose

This contract defines the canonical governed submission identity used by Phase D execution evidence.

# Canonical formula

`submission_id = sha256(canonical_json({authority_owner, identity_type, intent_id, plan_hash, trade_instance_id}))`

The canonical implementation lives in `constellation_2/common/execution_identity_authority_v1.py`.

# Inputs

- `intent_id`: from governed intent lineage
- `plan_hash`: canonical hash of the normalized order plan
- `trade_instance_id`: governed Phase C execution-instance identity

# Required meanings

- Same `intent_id + plan_hash + trade_instance_id` => same `submission_id`
- Same `intent_id + plan_hash` with different valid `trade_instance_id` => different `submission_id`
- Exact replay must still collide on the same submission directory and be blocked by idempotency

# Downstream trust surfaces

Downstream systems should trust:

- `binding_record.v2.json` for `trade_instance_id` and `submission_id`
- `execution_identity_record.v1.json` for audited identity provenance and duplicate classification
- Phase D submission evidence directories keyed by `submission_id`

# Compatibility

Legacy records without execution identity fields may continue to use binding-hash submission identity only as a backward-compatibility mode.

