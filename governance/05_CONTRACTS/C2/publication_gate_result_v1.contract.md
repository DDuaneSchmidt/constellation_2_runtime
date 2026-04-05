# publication_gate_result_v1

Purpose: governed publication decision emitted by the advisor-kernel publication boundary for a single artifact.

Producer and boundary
- Producer: `ops/tools/run_publication_gate_v1.py`.
- Boundary: publication consumer gate between kernel outputs and publishable advisor-sidecar artifacts.
- Write path: `<advisor_runtime_root>/<MODE>/publication_gate_result_v1/<DAY>/publication_gate_result.v1.json`.

Required fields
- `schema_id = publication_gate_result`
- `schema_version = v1`
- `authority_class`
- `support_status`
- `produced_utc`
- `run_id`
- `artifact_family`
- `artifact_ref`
- `publication_status`
- `publication_class`
- `reason_codes[]`

Runtime inputs
- candidate artifact JSON
- semantic reconciliation report JSON
- authority registry artifact at `<advisor_runtime_root>/<MODE>/reports/authority_registry_v1/<DAY>/authority_registry.v1.json`

Enforced invariants
- The gate must resolve an authority-registry row for `artifact_family = <artifact schema_id>_v1`.
- The registry row `authority_class` must equal the artifact `authority_class`.
- The registry row must have `publication_required = true`.
- Output must remain schema-valid with no extra bootstrap or registry-debug fields.

Fail-closed behavior
- Missing authority registry file is a hard failure.
- Missing artifact-family row is a hard failure.
- Authority-class mismatch is a hard failure.
- `publication_required = false` is a hard failure.
- Invalid semantic report or artifact payload is a hard failure.

Invalid conditions
- Missing any required field.
- Extra fields outside schema.
- Unsupported `authority_class` or `support_status` enum values.
- `artifact_ref` or `artifact_family` that does not match the evaluated artifact.

Determinism
- Same artifact JSON, same semantic report JSON, and same authority registry row must yield the same publication result and reason codes.
- Replay or publication re-checks must not depend on network, broker state, or mutable latest pointers.
