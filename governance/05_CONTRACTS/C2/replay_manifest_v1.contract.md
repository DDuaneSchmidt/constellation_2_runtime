# replay_manifest_v1

Purpose: deterministic offline replay proof for a previously successful authoritative runtime day and submission.

Producer and write path
- Producer: `ops/tools/run_runtime_replay_day_v1.py`.
- Write path: `<replay_truth_root>/reports/replay_manifest_v1/<DAY>/<submission_id>.replay_manifest.v1.json`.

Required fields
- `schema_id = C2_REPLAY_MANIFEST_V1`
- `schema_version = 1`
- `produced_utc`
- `day_utc`
- `source_truth_root`
- `replay_truth_root`
- `submission_id`
- `status`
- `reason_codes[]`
- `tool_runs[]`
- `comparisons[]`
- `canonical_json_hash`

Replay scope
- Replay is offline-only.
- Replay consumes stored authoritative artifacts under `source_truth_root` and writes copied/replayed artifacts under `replay_truth_root`.
- Replay compares only existing governed artifact roles:
  - `allocation`
  - `authorization_dir`
  - `submission_dir`
  - `execution_stream_dir`
  - `fill_ledger`

Fail-closed behavior
- Replay must not require IB host, IB port, or IB client id.
- Replay must not invoke paper-submit or any live broker path.
- Missing required source artifacts is a hard failure.
- Any non-identical comparison status other than `identical` is a hard failure.

Invalid conditions
- Missing required fields or invalid submission-id format.
- Comparison objects outside the allowed artifact-role/status enums.
- Manifest content that references live execution or network-only steps.

Determinism
- Same source truth, same replay truth layout, same day, and same submission id must produce the same manifest content and comparison outcomes.
- `tool_runs` in replay mode are limited to offline replay steps; they must not vary with network availability.
