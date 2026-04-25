# market_calendar_coverage_status_v1

`market_calendar_coverage_status_v1` is the canonical operator-visible status surface for market-calendar forward coverage.

Canonical output:
- `/home/node/constellation_runtime_data/truth/market_calendar_coverage_status_v1/current.json`

Canonical writer:
- `ops/tools/run_market_calendar_coverage_authority_v1.py`

Rules:
- it must expose source and runtime coverage ranges
- it must expose the required target day and the governed forward-coverage policy
- it must expose the warning target day derived from the governed forward-coverage policy
- it must expose first-class source coverage state and runtime coverage state
- it must expose whether source and runtime each cover:
  - the required target day
  - the warning target day
- it must expose an operator action code that answers whether the next step is source extension, runtime refresh, manifest/schema repair, or ingest-failure inspection
- severity is governed as:
  - `INFO` when coverage is healthy for the required target day and policy buffer
  - `WARNING` when coverage remains above the required target day but falls below the warning buffer or ranges diverge
  - `CRITICAL` when the required target day is uncovered, manifests are missing, schema is invalid, or refresh failed
- stable reason codes are governed by `governance/02_REGISTRIES/C2_SESSION_AUTHORITY_BLOCKER_REASON_REGISTRY_V1.json`
- this artifact is non-authoritative for active-day ownership but must feed Session Authority monitoring and alerting
