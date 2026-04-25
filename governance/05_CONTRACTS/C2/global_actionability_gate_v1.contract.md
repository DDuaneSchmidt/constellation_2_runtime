---
id: C2_GLOBAL_ACTIONABILITY_GATE_CONTRACT_V1
title: "C2 Global Actionability Gate Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_actionability_gate
---

# C2 Global Actionability Gate Contract V1

## Purpose

This gate is the first Core 3 layer.

It answers only:

- whether autonomous post-entry action evaluation is permitted for a governed trade identity right now

## Allowed inputs

The gate may read only governed Core 2 outputs:

- `trade_identity.v1`
- `incorporated_broker_trade_state.v1`
- `reconciled_trade_description.v1`
- `reconciliation_health.v1`
- `reconciliation_provenance.v1`

## Gate verdicts

- `ACTIONABLE`
- `DEGRADED_REVIEW_REQUIRED`
- `BLOCKED`

## `ACTIONABLE`

`ACTIONABLE` is permitted only when all of the following hold:

- `trade_identity.v1.blocker_state == CLEAR`
- `trade_identity.v1.ambiguity_state == NONE`
- `trade_identity.v1.ownership_classification == CONSTELLATION_OWNED`
- `reconciliation_health.v1.current_state == TRUSTED`
- `reconciliation_health.v1.freshness_status == FRESH`
- `reconciliation_health.v1.downstream_action_posture == SAFE`
- `reconciled_trade_description.v1.downstream_posture == SAFE`

## `DEGRADED_REVIEW_REQUIRED`

`DEGRADED_REVIEW_REQUIRED` means Core 3 may emit only hold-safe and review-required posture.

This state is required when:

- `reconciliation_health.v1.current_state == DEGRADED`
- or `reconciliation_health.v1.freshness_status == STALE`
- or `reconciliation_health.v1.downstream_action_posture == DEGRADED`
- or `reconciled_trade_description.v1.downstream_posture == DEGRADED`
- or `trade_identity.v1.ownership_classification == FOREIGN_MANUAL`

## `BLOCKED`

`BLOCKED` is required when any blocker remains unresolved, including:

- `reconciliation_health.v1.current_state == BLOCKED`
- `reconciliation_health.v1.freshness_status == TOO_STALE`
- `reconciliation_health.v1.insufficient_evidence_status == INSUFFICIENT`
- `reconciliation_health.v1.ambiguity_status == AMBIGUOUS`
- `trade_identity.v1.blocker_state == BLOCKED`
- `trade_identity.v1.ambiguity_state != NONE`
- `trade_identity.v1.ownership_classification == AMBIGUOUS_OWNERSHIP`
- `trade_identity.v1.ownership_classification == INSUFFICIENT_EVIDENCE`
- `reconciled_trade_description.v1.reconciliation_descriptive_status in {BLOCKED, AMBIGUOUS}`
- `reconciled_trade_description.v1.downstream_posture == BLOCKED`

## Blocker and degradation taxonomy

The gate must preserve explicit reason codes at minimum for:

- `UPSTREAM_TRADE_TRUTH_BLOCKED`
- `STALE_TRADE_TRUTH`
- `OWNERSHIP_AMBIGUOUS`
- `FOREIGN_MANUAL_CLASSIFICATION`
- `RECONCILIATION_DEGRADED`
- `INSUFFICIENT_EVIDENCE_FOR_AUTONOMY`
- `TRADE_IDENTITY_BLOCKED`
- `DESCRIPTION_BLOCKED_FOR_DOWNSTREAM_ACTION`

## First-reason rule

The gate must emit one explicit first blocker or first degraded reason using deterministic precedence:

1. blocked reconciliation or blocked downstream posture
2. identity blocker or ambiguity
3. ownership classification blocker
4. insufficient evidence
5. stale truth
6. degraded reconciliation

## Downstream rule

Later Core 3 stages must obey gate output.

- `BLOCKED` forbids rich autonomous action posture.
- `DEGRADED_REVIEW_REQUIRED` permits only hold-safe or operator-review posture.
- `ACTIONABLE` permits candidate generation and final action resolution.
