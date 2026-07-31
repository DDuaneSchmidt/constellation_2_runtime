# Aegis Generated Hypothesis Approval Event Lineage Design v1

## Data Flow

1. Read Oil Shock approval candidates from:
   - `approval_events.v1.jsonl`
   - `approval_queue.v1.json`
   - `operator_action_queue.v1.json`
   - `promotion_packets.v1.json`
   - `hypothesis_workflow_state.v1.json`
   - paper setup and blueprint artifacts
2. Select an immutable approval event only when it is target-day aligned, schema-compatible, approved, and hashed.
3. Emit lineage status and source hashes.
4. Governance bridge consumes the lineage artifact for `approval_event` and `approval_event_hash`.
5. Paper setup bridge consumes governance bridge.
6. Oil Shock candidate construction consumes paper setup bridge.

## Fail-Closed Behavior

No downstream bridge may infer approval from recommendation state. `PAPER_PROMOTION_RECOMMENDED` remains a queue state, not approval.

## Non-Goals

This design does not reconstruct missing operator actions, fabricate approval, force candidates, bypass candidate contracts, or change trading/safety policy.

## Historical Recovery

The live 2026-06-02 Oil Shock queue can be regenerated into `PAPER_PROMOTION_RECOMMENDED` while the immutable approval event remains in a previous day approval JSONL. The repair layer treats the immutable event as lineage evidence when it is not newer than the target day and matches the generated hypothesis. This is lineage normalization, not approval creation.

## Operator Log Normalization

An operator action event may be normalized into a canonical approval-event shape only when it is already an immutable record of `APPROVE_PAPER_TEST` and state transition to `PAPER_PROMOTION_APPROVED`. The original source path and hash remain the audit authority.

## Discovery Order

Discovery is ordered fail-closed: current approval JSONL, current operator action event log, historical approval/operator event logs with `source_target_day <= target_day`, queue latest event with hash, then legacy paper setup/blueprint hashes. Queue state alone remains `APPROVAL_QUEUE_STATE_ONLY_NO_EVENT`. The artifact records all searched source paths and hashes so downstream bridges consume lineage rather than infer approval readiness.

