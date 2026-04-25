---
id: C2_DAY_OPEN_TRIGGER_V1
title: "Day Open Trigger V1"
status: DRAFT
version: 1
created_utc: 2026-04-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Day Open Trigger V1

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/day_open_trigger_v1/<DAY>/day_open_trigger.v1.json`

Rules:
- this artifact is the single governed authority-to-open trigger for the day
- canonical writer: `ops/tools/run_day_open_trigger_v1.py`
- canonical helper: `constellation_2/common/day_open_trigger_v1.py`
- policy helper: `constellation_2/common/day_open_policy_v1.py`
- it may emit `trigger_status = EMITTED` only when:
  - `target_day_admission_v1.admission_status == ADMIT`
  - `active_session_v1/current.json` is bound to the same day with `rollover_status = ACTIVE_SESSION_CONFIRMED`
  - `paper_session_ledger_v1` reports `authority_status = GRANTED`
- for non-paper environments, the day is inside the governed open window
- for non-paper environments, no terminal open-attempt artifact already exists
- for non-paper environments, no prior emitted trigger for the same dedupe key has already been consumed
- it must fail closed when any required authority input is missing, stale, contradictory, or ambiguous
- it must record whether the trigger is the initial BOD trigger or a bounded late-grant re-entry trigger when that distinction is policy-relevant
- for `environment = PAPER`, allowed trigger kinds are:
  - repeated same-day `INITIAL_BOD_TRIGGER` emissions keyed by monotonic `trigger_sequence`
- in paper mode, the trigger owner must not suppress emission solely because:
  - a successful open is already recorded for the day
  - a prior same-day paper trigger was already consumed
  - the clock is outside the BOD open window
- in paper mode, each newly emitted paper trigger must preserve prior trigger rows in `trigger_history`
- the trigger payload must embed a machine-readable open-policy snapshot including:
  - `successful_open_already_recorded`
  - `same_day_open_cap_enforced`
  - `time_window_enforced`
  - `open_terminal`
  - `terminal_reason_code`
- it must remain machine-readable, deduped, and auditable
- the trigger itself does not imply that any open command executed; that is owned by `day_open_attempt_v1`
