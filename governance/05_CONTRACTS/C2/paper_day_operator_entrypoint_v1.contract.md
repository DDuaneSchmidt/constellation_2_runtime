---
id: C2_PAPER_DAY_OPERATOR_ENTRYPOINT_V1
title: "C2 Paper Day Operator Entrypoint Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_admission
---

# C2 Paper Day Operator Entrypoint Contract v1

## Purpose

This contract defines the single operator-facing paper-day control boundary.

## Required behavior

The operator entrypoint must:

- route canonical morning operations through `ops/tools/run_paper_session_bootstrap_v1.py`
- run canonical pre-open materialization before Session Authority admission is relied on
- delegate target-day ownership to `session_authority_v1`
- rely on Session Authority to apply the explicit startup promotion gate before `active_session_v1/current.json` may advance
- consume `active_session_v1/current.json` for the active day
- stop before `run_day_open_attempt_v1.py` whenever bootstrap, pre-open, promotion, or Session Authority blocks morning startup
- fail closed when `target_day_admission_v1` is `BLOCKED`
- start execution only for the admitted `active_day`
- route day-open execution through `ops/tools/run_day_open_attempt_v1.py`
- rely on `ops/tools/run_c2_paper_day_orchestrator_v2.py` as the single canonical owner of sleeve-edge publication before allocation ordering

Operator stop-surface rule:

- the operator-facing morning surface must report whether the canonical stop came from:
  - `pre_open_bundle_v1`
  - `session_promotion_decision_v1`
  - `target_day_admission_v1`
- the first canonical artifact path to inspect must be included in the operator-facing failure summary
- the operator-facing morning surface must also report the earliest failing owned prerequisite, its owner tool, its artifact path, and the rule: fix that prerequisite, then rerun the canonical entrypoint
- blocked morning output must explicitly identify deeper tools that must not be run manually while startup remains blocked

## Subordinate day-readiness automation

A repo-owned operator or scheduled preparation flow may run through `ops/tools/run_tomorrow_paper_startup_prep_v1.py` before the canonical morning entrypoint.

That automation owner may only:

- resolve the target day
- refresh known refreshable prerequisites through canonical owners
- rerun bootstrap and classify the resulting readiness state for operator use

That automation owner may not:

- override bootstrap, `pre_open_bundle_v1`, `session_promotion_decision_v1`, or Session Authority truth
- bypass the canonical morning entrypoint
- run deeper day-open or orchestration stages manually

## Monopoly rule

Direct execution paths that bypass Session Authority must be removed from authority or must fail closed.

## Canonical authority owner

`ops/tools/run_session_authority_v1.py`

## Operator shell consumer

`ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh`

## Canonical morning operations tool

`ops/tools/run_paper_session_bootstrap_v1.py`

## Runtime route

`ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh`
-> `ops/tools/run_paper_session_bootstrap_v1.py`
-> `ops/tools/run_pre_open_materializer_v1.py`
-> `ops/tools/run_session_authority_v1.py`
-> `ops/tools/run_day_open_attempt_v1.py`
-> `ops/tools/run_c2_multi_sleeve_orchestrator_v1.py`
-> `ops/tools/run_c2_paper_day_orchestrator_v2.py`

## Automation layering

`ops/tools/run_day_authority_decision_v1.py`
-> `ops/tools/run_tomorrow_paper_startup_prep_v1.py`
-> `ops/tools/run_paper_session_bootstrap_v1.py`
-> `ops/tools/run_pre_open_materializer_v1.py`
-> `ops/tools/run_session_authority_v1.py`

This layering is subordinate to the canonical morning route above and must never replace it.
