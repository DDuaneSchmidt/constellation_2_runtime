# next_day_readiness_probe_v1

`next_day_readiness_probe_v1` is a non-executing forecast artifact.

It projects next-day readiness from current evidence and deterministic prerequisites only.

Rules:
- it must never fabricate target-day evidence
- it must never execute trading actions
- it may return `READY`, `BLOCKED`, or `UNKNOWN`
- `READY` is allowed only when the required target-day prerequisites are already materialized and healthy
- `UNKNOWN` is required when target-day prerequisites are not yet materialized and cannot be honestly proved
