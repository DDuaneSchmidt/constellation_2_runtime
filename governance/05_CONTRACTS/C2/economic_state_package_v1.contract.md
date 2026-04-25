
---
contract_id: C2_ECONOMIC_STATE_PACKAGE_V1
owner: governance
status: active
schema_id: c2_contract
schema_version: v1
---

# Economic State Package v1

A sealed economic package is the first-class artifact proving economic completeness for one governed context.

## Required fields

- `day_utc`
- `sleeve_id`
- `mode`
- `account_id`
- `operation_type`
- `context_hash`
- refs and hashes for required economic nodes
- `build_ref`
- `manifest_ref`
- `sealed=true`
- `sealed_utc`
- reproducible `package_hash`

## Rules

- no package when `economic_state_build.v1.closure_status != COMPLETE`
- package hash is deterministic from context, manifest ref, and required node hashes
- package is sleeve-scoped and day-scoped
