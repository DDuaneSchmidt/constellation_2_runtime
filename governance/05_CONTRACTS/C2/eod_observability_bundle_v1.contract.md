---
id: C2_EOD_OBSERVABILITY_BUNDLE_V1
title: "C2 EOD Observability Bundle (Certificate + SLO Sentinel) — V1"
status: DRAFT
version: 1
created_utc: 2026-02-25
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 EOD Observability Bundle (V1)

## 1. Purpose

This contract makes EOD completion **observable, auditable, and fail-closed** without relying on manual log inspection.

It introduces:

1) **EOD Run Certificate V1** (authoritative PASS/FAIL for the EOD job).
2) **EOD SLO Sentinel V1** (periodic evaluator that detects missing/late/fail and emits a governed monitoring artifact).

The UI remains read-only and truth-derived: it surfaces these artifacts via `/api/status` warnings and evidence links.

## 2. Truth locations (authoritative)

### 2.1 EOD Run Certificate V1

Path:
- `constellation_2/runtime/truth/reports/eod_run_certificate_v1/<DAY_UTC>/eod_run_certificate.v1.json`

Schema:
- `governance/04_DATA/SCHEMAS/C2/REPORTS/eod_run_certificate.v1.schema.json`

Rules:
- Exactly one certificate per day_utc.
- `status=PASS` only if all required outputs are present.
- Any failure (including crash/exit) must still emit a certificate with `status=FAIL`.

### 2.2 EOD SLO Sentinel V1

Path:
- `constellation_2/runtime/truth/monitoring_v1/eod_slo_sentinel_v1/<DAY_UTC>/eod_slo_sentinel.v1.json`

Schema:
- `governance/04_DATA/SCHEMAS/C2/MONITORING/eod_slo_sentinel.v1.schema.json`

Rules:
- The sentinel evaluates expected EOD completion for the same `DAY_UTC` used by the EOD job.
- The sentinel must classify:
  - `OK` (certificate exists and PASS)
  - `CERT_FAIL` (certificate exists and FAIL)
  - `CERT_MISSING` (certificate missing and before deadline)
  - `LATE` (certificate missing and after deadline)

## 3. Deadline (V1)

EOD timer executes at 00:30 UTC and targets `DAY_UTC = yesterday (UTC)`.

For V1 the sentinel deadline is:
- `deadline_utc = <today_utc> 01:30:00Z` (one hour after the timer schedule)

If certificate is missing after deadline → `state=LATE` and `status=FAIL`.

## 4. UI surface rule

The Phase L Ops Dashboard must remain truth-only.

The `/api/status` response must surface EOD observability by:
- adding warning codes for missing/late/fail, and
- adding the certificate/sentinel paths to `source_paths` and/or `missing_paths` so evidence can be opened.

No UI may read systemd/journal directly.

## 5. Fail-closed semantics

This bundle is **observability**. It does not itself block trading.

However, operational policy may treat `state in {CERT_FAIL, LATE}` as incident-worthy.

## 6. Non-claims

- No claims about profitability, alpha, or execution quality beyond artifact presence and explicit statuses.
- No guarantee that EOD PASS implies intraday readiness; the operator gate stack remains the canonical readiness surface.
