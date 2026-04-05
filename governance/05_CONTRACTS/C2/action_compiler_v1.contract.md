# action_compiler_v1

Defines the deterministic action compiler for Execution Layer V1.

Inputs:
- `planning_snapshot_v1`
- `official_recommendation_set_v1`
- `action_policy_pack_v1`

Outputs:
- `action_intent_v1`
- `decision_action_v1`
- `blocked_action_v1`

Compiler invariants:
- deterministic for identical inputs
- no fallback defaults
- no financial planning math generation
- no runtime truth writes
- fail closed on ambiguous recommendation mapping
