---
id: C2_DAY_OPEN_POLICY_V1
title: "Day Open Policy V1"
status: DRAFT
version: 1
created_utc: 2026-04-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Day Open Policy V1

Rules:
- canonical helper: `constellation_2/common/day_open_policy_v1.py`
- this helper is the single reusable policy definition for governed open-attempt eligibility
- for `environment = PAPER`, policy mode is `PAPER_READY_WHEN_GRANTED_UNBOUNDED_SAME_DAY`
- paper policy must enforce:
  - if binding admission, active-session rollover, and ledger authority are currently granted, paper open is policy-allowed now
  - no paper same-day success cap
  - no paper late-open cap
  - no paper time-of-day window suppression
  - readiness and authority remain mandatory; paper still fails closed when those are not granted
- for non-paper environments, the helper must preserve strict single-window behavior and must not loosen production semantics in this pass
- trigger and attempt owners must embed a machine-readable policy snapshot so operator and audit surfaces can report:
  - whether a successful open already exists
  - whether same-day caps are enforced
  - whether time windows are enforced
  - whether the current environment remains terminal for open purposes
