---
id: C2_OPERATOR_PROJECTION_REGISTRY_V1
title: "C2 Operator Projection Registry Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-12
owner: Constellation
authority: governance+git
scope: constellation_2_operator_projection_registry
---

# C2 Operator Projection Registry Contract (V1)

## Purpose

This registry names the governed operator projections and binds each one to its
declared source authority and entity scope.

## Projection Registry

| contract_id | source_authority | entity_scope |
| --- | --- | --- |
| `engine_readiness_projection` | `session_authority_status_v1`, `replay_certification_gate_v1`, `ib_api_handshake_latest_pointer_v1`, `runtime_truth_integrity_result_v1` | `system` |
| `order_lifecycle_projection` | `execution_evidence_v1`, `fill_ledger_v1` | `orders` |
| `position_state_projection` | `positions_snapshot_v2` | `positions` |
| `reconciliation_status_projection` | `reconciliation_report_v3`, `execution_reconciliation_v1` | `reconciliation` |
| `alert_projection` | `alerts_projection_v1` | `alerts` |
| `advisory_context_projection` | `official_recommendation_set_v1`, `advisor_trade_intent_proposal_v1` | `advisory` |

## Registry Notes

- `alert_projection` may fall back to `session_authority_alert_v1` and `current_system_projection_v1` only when the canonical `alerts_projection_v1` artifact is missing at runtime.
- This registry names projection contracts only. Workspace composition contracts are not projection entries.
- A projection must not silently expand its `source_authority` beyond this registry without a governance update.
