# promotion_gate_result_v1

Purpose: governed promotion-plane gate result describing whether a reviewed promotion candidate is ready, rejected, or blocked while remaining non-executable.

Producer and boundary
- Producer: `ops/tools/run_promotion_gate_v1.py`.
- Boundary: promotion-plane review gate after candidate, review, and manual-review artifacts already exist.
- Write path: `<advisor_runtime_root>/<MODE>/promotion_gate_result_v1/<DAY>/promotion_gate_result.v1.json`.

Required fields
- `schema_id = promotion_gate_result`
- `schema_version = v1`
- `produced_utc`
- `run_id`
- `gate_result_id`
- `candidate_id`
- `review_id`
- `manual_review_id`
- `gate_status`
- `reason_codes[]`
- `source_artifact_refs[]`
- `selection_basis`
- `notes[]`

Runtime invariants
- `gate_status` must be one of `promotion_ready`, `promotion_rejected`, or `promotion_blocked`.
- Candidate, review, and manual-review identifiers must all be present and internally consistent.
- The artifact records governance state only; it is not an execution authorization.

Fail-closed behavior
- Missing required upstream promotion artifacts is a hard failure.
- Invalid gate status or malformed identifiers is a hard failure.
- Promotion readiness here must not be interpreted as broker-executable authority.

Invalid conditions
- Missing required fields.
- Extra schema-undeclared fields.
- Empty `reason_codes`, `source_artifact_refs`, or `selection_basis` when the runtime emits them.

Determinism
- Same candidate, review, and manual-review inputs must yield the same gate result, reason codes, and source references.
- Promotion gating must not depend on live broker connectivity or mutable trading state.
