# authority_registry_v1

Purpose: canonical registry describing which plane owns each advisor-sidecar artifact family and whether publication, promotion, and replay are expected.

Truth owner
- Decision/governance plane definition emitted into advisor runtime reports.
- Producer: `ops/tools/run_authority_registry_v1.py`.
- Write path: `<advisor_runtime_root>/<MODE>/reports/authority_registry_v1/<DAY>/authority_registry.v1.json`.

Required fields
- `schema_id = authority_registry`
- `schema_version = v1`
- `produced_utc`
- `run_id`
- `rows[]` with:
  - `artifact_family`
  - `owner_plane`
  - `write_root`
  - `authority_class`
  - `publication_required`
  - `promotion_required`
  - `replay_expected`
  - `downstream_consumers[]`
  - `notes[]`

Runtime invariants
- `rows` are schema-valid and sorted deterministically by `artifact_family` before emission.
- `owner_plane` must be one of `truth_plane`, `decision_plane`, `promotion_plane`, `view_plane`.
- `authority_class` in each row is the runtime authority class consumers must match against the emitted artifact.
- `publication_required` and `promotion_required` describe consumer gates, not UI hints.

Fail-closed behavior
- Runtime consumers must treat the registry as authoritative when they enforce publication or promotion boundaries.
- If the registry file is missing, if a row for the target artifact family is missing, or if the row authority class does not match the artifact authority class, the consumer must fail closed.
- A row with `publication_required = false` means publication must be blocked for that artifact family.

Invalid conditions
- Missing required top-level fields or row fields.
- Extra schema-undeclared fields.
- Duplicate or ambiguous rows for the same `artifact_family`.
- Row values that contradict the emitting runtime boundary.

Determinism
- Same runtime code and same artifact-family definitions must yield the same row set and ordering.
- Replay must not reinterpret registry meaning; it must consume the emitted artifact exactly as published.
