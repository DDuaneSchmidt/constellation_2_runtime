---
id: C2_EXECUTION_IDENTITY_AUTHORITY_V1
title: "C2 Execution Identity Authority Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_execution_identity
---

# Purpose

This contract defines the canonical authority chain for governed execution identity in Phase C and Phase D.

The system distinguishes three identities:

- `intent_id`: strategic decision identity owned by the source intent and carried into the normalized order plan.
- `trade_instance_id`: governed identity of one valid execution instance for a same-day Phase C attempt.
- `submission_id`: deterministic broker-submission identity derived from `intent_id + plan_hash + trade_instance_id`.

# Canonical owners

- `intent_id` owner: source intent artifact (`equity_intent.v1.json` or equivalent lineage-bearing source).
- `trade_instance_id` owner: `ops/tools/run_phasec_identity_materializer_day_v1.py`.
- `submission_id` derivation owner: `constellation_2/common/execution_identity_authority_v1.py`, materialized by `constellation_2/phaseC/tools/c2_submit_preflight_offline_v2.py` and consumed by `constellation_2/phaseD/lib/submit_boundary_paper_v4.py`.

# Canonical artifact

Phase C must emit:

- `truth_sleeves/<sleeve_id>/<mode>/phaseC_preflight_v1/<DAY>/attempt_<ATTEMPT_ID>/<INTENT_HASH>/execution_identity_record.v1.json`

This artifact is the governed audit surface for execution identity.

# Determinism rules

- The same `intent_id`, `plan_hash`, and `trade_instance_id` MUST yield the same `submission_id`.
- A new governed same-day execution instance MUST use a different `trade_instance_id`.
- A different `trade_instance_id` MAY yield a new `submission_id` even when `intent_id` and `plan_hash` are unchanged.
- Randomness and eval-time churn are forbidden as the primary execution identity mechanism.

# Trade-instance authority basis

The Phase C attempt lifecycle is the canonical same-day execution-instance owner for PAPER execution.

A Phase C attempt becomes a governed trade instance only when:

- the owned Phase C materializer creates the attempt under the canonical sleeve execution root,
- the attempt produces a supported identity output set,
- and the attempt becomes the active same-day attempt.

Superseded attempts remain immutable history and do not mutate or delete prior submission evidence.

# Fail-closed rules

- Missing `intent_id` when execution identity authority is active must fail closed.
- Missing or malformed `trade_instance_id` or `submission_id` in an active execution-identity path must fail closed.
- If `execution_identity_record.v1.json` exists and disagrees with binding or plan identity, consumers must fail closed.
- Legacy Phase C records without execution identity fields may only use legacy binding-hash submission identity for backward compatibility.

