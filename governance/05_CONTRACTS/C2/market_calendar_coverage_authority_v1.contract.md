# market_calendar_coverage_authority_v1

`market_calendar_coverage_authority_v1` is the owned upstream prevention authority for market-calendar forward coverage.

Canonical writer:
- `ops/tools/run_market_calendar_coverage_authority_v1.py`

Canonical output:
- `/home/node/constellation_runtime_data/truth/market_calendar_coverage_status_v1/current.json`

Rules:
- it must inspect the governed source bundle under `constellation_2/phaseJ/source_data/market_calendar_source_v1`
- it must inspect canonical runtime truth under `/home/node/constellation_runtime_data/truth/market_calendar_v1`
- the governed source maintenance floor is:
  - coverage through at least the full currently published exchange year
  - coverage through the next exchange year as well when that year is officially published and approved in the repo source workflow
- it must govern forward coverage with:
  - `minimum_required_offset_calendar_days=1`
  - `buffer_calendar_days=3`
- the forward-coverage runtime policy is not a substitute for the governed source maintenance floor
- the recurring standalone coverage runner under `ops/run/c2_market_calendar_coverage_authority_v1.sh` must default to `CHECK`, not `WRITE` or `REFRESH`
- runtime refresh remains explicit and operator-invoked through the approved producer flow
- `CHECK` mode must not write runtime truth
- `WRITE` mode may write only `market_calendar_coverage_status_v1/current.json`
- `REFRESH` mode must extend canonical runtime truth only through `constellation_2/phaseJ/tools/market_calendar_ingest_v1.py`
- it must fail closed if the governed source does not cover the required target day
- it must never declare healthy coverage when the required target day is absent from source or runtime truth
- it must explicitly distinguish governed-source coverage state from canonical-runtime coverage state
- it must emit an operator action code that distinguishes:
  - source extension required
  - runtime refresh required
  - manifest/schema repair required
  - ingest refresh failure inspection required
- it is upstream prevention only; Session Authority remains the sole active-day owner
