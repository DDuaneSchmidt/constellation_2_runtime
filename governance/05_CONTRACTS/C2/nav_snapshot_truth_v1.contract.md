---
id: C2_NAV_SNAPSHOT_TRUTH_V1
title: "Constellation 2.0 — NAV Snapshot Truth v1 (Economic NAV + Drawdown Core)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - nav
  - drawdown
  - snapshot
  - audit_grade
  - deterministic
  - fail_closed
---

# NAV Snapshot Truth v1 — Contract

## 1. Objective
Produce a day-scoped immutable truth artifact that contains the canonical economic NAV state for day `<DAY>`:

- `end_nav` (decimal string)
- `peak_nav_to_date` (decimal string)
- `drawdown_pct` (decimal string, 6dp, negative underwater)

This artifact is the core input for:
- NAV history ledger
- windowed drawdown pack
- availability certificate
- validator

## 2. Canonical output path
- `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_snapshot/<DAY>/nav_snapshot.v1.json`

## 3. Canonical upstream input (required)
- `constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json`

Required extracted field:
- `nav.nav_total` (MUST be integer >= 0)

## 4. Deterministic computation rules
### 4.1 end_nav
- `end_nav = str(nav.nav_total)` (base-10, no commas, no exponent)
- Fail-closed if `nav_total` is missing, non-integer, or < 0.

### 4.2 peak_nav_to_date and drawdown_pct
These MUST be computed per `C2_DRAWDOWN_CONVENTION_V1`:

- Maintain a prior rolling peak `peak_nav_{t-1}` from NAV Snapshot Truth history.
- Compute:
  - `peak_nav_to_date = max(end_nav, peak_nav_{t-1})`
  - `drawdown_pct = (end_nav - peak_nav_to_date) / peak_nav_to_date`

Constraints:
- `peak_nav_to_date` MUST be > 0 (fail-closed otherwise).
- Arithmetic MUST use Decimal.
- `drawdown_pct` MUST be quantized to 6 decimals using ROUND_HALF_UP.
- Stored `drawdown_pct` MUST be a decimal string with exactly 6 digits after decimal.

Genesis rule (first observation day):
- If no prior NAV snapshot exists, then:
  - `peak_nav_to_date = end_nav`
  - `drawdown_pct = "0.000000"`

## 5. Required metadata (audit)
Top-level required fields:
- `schema_id` = `"C2_NAV_SNAPSHOT_TRUTH_V1"`
- `schema_version` = `1`
- `day_utc` = `"YYYY-MM-DD"`
- `end_nav` (decimal string)
- `peak_nav_to_date` (decimal string)
- `drawdown_pct` (decimal string, 6dp)
- `input_manifest` (array of objects):
  - each entry includes: `type`, `path`, `sha256`
- `produced_utc` (UTC ISO8601 Z, no micros)
- `producer` object:
  - `git_sha` (string)
  - `module` (string)
  - `repo` (string, fixed `"constellation_2_runtime"`)
- `canonical_json_hash` (sha256 hex over canonical JSON with this field null)

## 6. Immutability
- If the output file exists:
  - If bytes are identical → treat as OK and do not rewrite.
  - If bytes differ → FAIL-CLOSED (refuse overwrite).

## 7. Float prohibition
Floats are forbidden anywhere in the artifact.

## 8. Schema requirement
The produced artifact MUST validate against:
- `governance/04_DATA/SCHEMAS/C2/MONITORING/nav_snapshot.v1.schema.json`
