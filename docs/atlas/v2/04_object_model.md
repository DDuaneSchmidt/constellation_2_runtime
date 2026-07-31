# Atlas V2 Object Model

Canonical schema: `governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_core_objects.v1.schema.json`.

Core objects:

- AttentionDecision
- Prediction
- Outcome
- Regret
- CalibrationRecord
- BehaviorChange
- BeliefUpdate
- ExperienceEvent

Ledger rules:

- Every object carries `object_type`, a stable id field, `created_at`, and `transition_history`.
- Ledgers are JSONL files named by object type.
- Updates are recorded by appending a new object version.
- State transitions append to `transition_history`; prior versions remain in the ledger.
- Rejected alternatives are part of AttentionDecision and must not be dropped.
- Unknown, failed, pending, matched, and mismatched are separate states.
