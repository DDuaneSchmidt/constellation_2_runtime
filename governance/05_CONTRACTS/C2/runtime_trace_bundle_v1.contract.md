# runtime_trace_bundle_v1

Purpose: operator-grade deterministic trace bundle for one submission across the proven runtime path.

Producer and write path
- Producer: `ops/tools/run_runtime_trace_bundle_v1.py`.
- Write path: `<truth_root>/reports/runtime_trace_bundle_v1/<DAY>/<submission_id>.runtime_trace_bundle.v1.json`.

Required fields
- `schema_id = C2_RUNTIME_TRACE_BUNDLE_V1`
- `schema_version = 1`
- `produced_utc`
- `day_utc`
- `source_truth_root`
- `submission_id`
- `artifacts[]`
- `canonical_json_hash`

Artifact coverage
- allocation
- authorization
- submission bundle files
- execution stream records matching the submission id
- fill ledger artifact

Runtime invariants
- `artifacts[]` are sorted deterministically by stage, artifact type, and path before emission.
- Every listed artifact must exist and include a canonical sha256 digest.
- Submission-scoped execution stream and fill ledger entries must match the requested `submission_id`.

Fail-closed behavior
- Missing submission bundle directory is a hard failure.
- Missing required lineage artifacts is a hard failure.
- No broker calls or mutable live-state fetches are allowed while building the trace bundle.

Invalid conditions
- Missing required fields.
- Extra schema-undeclared fields.
- Artifact entries missing `stage`, `artifact_type`, `path`, or `sha256`.
- Stage values outside the governed enum set.

Determinism
- Same truth root, same day, and same submission id must yield the same ordered artifact list and digests.
- Trace generation must be read-only against authoritative truth.
