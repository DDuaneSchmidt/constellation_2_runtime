# trade_readiness_decision_v1 Contract

## Purpose

`trade_readiness_decision_v1` is the canonical operator-facing and submit-gate readiness decision for trade placement.

It collapses multi-artifact evidence into one deterministic YES/NO decision without removing existing upstream protections.

## Writer

- `ops/tools/run_trade_readiness_reducer_v1.py`

## Canonical Path

- `{canonical_truth_root}/reports/trade_readiness_decision_v1/{day_utc}/trade_readiness_decision.v1.json`

## Required Semantics

- `decision` is `YES` or `NO`.
- `submit_allowed` can only be `true` when `decision=YES` and no canonical blocker remains.
- Missing or stale required evidence must fail closed.
- Blocker selection must be deterministic using fixed `blocker_priority_order`.
- Existing gates remain evidence inputs; this artifact owns the final operator-facing readiness decision.

## Required Fields

- `schema_version`
- `environment`
- `day_utc`
- `intent_hash`
- `decision`
- `submit_allowed`
- `canonical_gate`
- `canonical_blocker`
- `canonical_reason`
- `ordered_gate_results`
- `all_blockers`
- `evidence_artifacts`
- `evidence_hashes`
- `decision_timestamp_utc`
- `decision_writer`
- `truth_root`
- `policy_version`
- `blocker_priority_order`

## Consumer Policy

- Missing or invalid `trade_readiness_decision_v1` is fail-closed for submit authorization.
- Consumers must not infer readiness from legacy independent authorities when this decision is `NO`.
