# session_authority_alert_v1

`session_authority_alert_v1` is the derived alert surface for Session Authority monitoring.

Canonical output:
- `/home/node/constellation_runtime_data/truth/session_authority_alert_v1/current.json`

Canonical writer:
- `ops/tools/run_session_authority_alert_v1.py`

Rules:
- this surface is derived-only and non-authoritative
- it must consume `session_authority_status_v1` and may refresh status immediately before alert derivation
- it must emit:
  - `alert_status`
  - `severity`
  - `alert_reason_codes`
  - `dedupe_key`
  - source refs for status, active session, admission, and build
  - source ref for market-calendar coverage status when available
  - recommended operator action
- duplicate alert storms are forbidden
- unchanged blocker family for the same blocked target day must preserve the same `dedupe_key`
- unchanged market-calendar coverage blocker family for the same required target day must preserve the same `dedupe_key`
- a recovered healthy state after a prior alert must emit `alert_status=CLEAR`
- healthy steady state without prior alert must emit `alert_status=HEALTHY`
- missing or invalid source status must fail closed
- alert reason codes must stay aligned with `governance/02_REGISTRIES/C2_SESSION_AUTHORITY_BLOCKER_REASON_REGISTRY_V1.json`
- alert derivation must preserve the operator action guidance computed from market-calendar source-vs-runtime coverage state
- alert derivation must include startup-input convergence and readiness-bootstrap blocker families when those failures are present in the status surface
