---
id: C2_OPERATOR_DAY_AUTHORITY_SUMMARY_V1
title: "Operator Day Authority Summary V1"
status: DRAFT
version: 1
created_utc: 2026-04-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Operator Day Authority Summary V1

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/operator_day_authority_summary_v1/<DAY>/operator_day_authority_summary.v1.json`

Rules:
- this artifact is the canonical day-scoped binding operator summary
- canonical writer: `ops/tools/run_operator_day_authority_summary_v1.py`
- canonical helper: `constellation_2/common/operator_day_authority_summary_v1.py`
- canonical writer output must set `binding_classification = BINDING_AUTHORITY_SUMMARY`
- it must summarize binding authority surfaces only:
  - `active_session_v1/current.json`
  - `target_day_admission_v1/<DAY>.json`
  - `session_authority_status_v1/current.json`
  - `paper_session_ledger_v1/<DAY>`
  - `trading_day_state_machine_v1/<DAY>`
  - `day_open_trigger_v1/<DAY>`
  - `day_open_attempt_v1/<DAY>`
  - `execution_reconciliation_v1/<DAY>` when present
- it must not derive admission truth independently
- it must classify the day as exactly one of:
  - `PRE_OPEN_READY`
  - `READY_NOT_STARTED`
  - `OPEN_WAITING_FOR_AUTHORITY`
  - `PAPER_OPEN_AVAILABLE`
  - `OPEN_TRIGGER_EMITTED`
  - `OPEN_ATTEMPTED`
  - `OPEN_SUCCEEDED`
  - `LATE_OPEN_AVAILABLE`
  - `LATE_OPEN_EXHAUSTED`
  - `OPEN_MISSED`
  - `OPEN_FAILED`
  - `STARTING`
  - `STARTED`
  - `STARTED_AND_PROGRESSED`
  - `BLOCKED`
  - `FAILED`
- it must include the machine-readable open-policy snapshot from binding day-open truth so operators can see:
  - whether a successful open already occurred today
  - whether same-day caps are enforced
  - whether time windows are enforced
  - whether the current environment is terminal for further opens
- for `environment = PAPER`, when readiness and authority are currently granted, the operator message must say paper trading is allowed now and must not suppress availability solely because of prior same-day paper success or clock time
- operator-facing tooling must prefer this surface over legacy `operator_summary_v1`
