---
id: C2_EXECUTION_JOURNAL_V1
title: "C2 Execution Journal Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_execution_journal
---

# C2 Execution Journal Contract (V1)

## Purpose

This contract defines the bounded canonical operational chronology for the touched Constellation
deployment and paper-day startup path.

`execution_journal_v1` is authoritative only for chronology.

It is not a control plane, not a readiness owner, not a free-form log, and not a generic event
bus.

When canonical runtime lifecycle provenance has already been established upstream,
`execution_journal_v1` may carry a narrow optional `runtime_lifecycle_ref` block that references
the governed runtime identity and startup/lifecycle start receipts without changing journal
identity ownership.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer family:
  - source-side emitters under authoritative runners
  - `ops/tools/run_execution_journal_v1.py` as transitional reconciler/backfill
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/execution_journal_v1/<DAY>/execution_journal.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_journal.v1.schema.json`
- Canonical event type registry:
  - `governance/02_REGISTRIES/C2_EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_V1.json`

## Authority classification

- This surface is the single authoritative chronology surface for the touched operational path.
- It must never override `deployment_state_machine_v1`, `trading_day_state_machine_v1`, or
  `paper_session_ledger_v1`.
- It records governed operational events emitted from those authorities and their immediate
  supporting surfaces.

## Identity tuple

Every journal and every event must bind exactly one identity tuple:

- `day_utc`
- `day_attempt_id`
- `pipeline_run_id`
- `release_id`
- `git_sha`

Cross-identity contamination is forbidden.

## Event model

Every event must include:

- `schema_version`
- `day_utc`
- `day_attempt_id`
- `pipeline_run_id`
- `release_id`
- `git_sha`
- `event_seq`
- `event_key`
- `event_type`
- `event_source`
- `generated_at_utc`
- `status`
- `payload`

## Allowed event types

Allowed event types, emitters, and payload families are governed only through:

- `governance/02_REGISTRIES/C2_EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_V1.json`
- `governance/05_CONTRACTS/C2/execution_journal_event_type_registry_v1.contract.md`

## Fail-closed rules

- The journal must be append-only.
- Mutation of prior events is forbidden.
- `event_seq` must be strictly monotonic and contiguous.
- Duplicate `event_seq` or duplicate `event_key` must fail closed.
- Missing identity fields, invalid event types, invalid event sources, invalid payload shapes, or
  cross-identity contamination must fail closed.
- The full journal must validate against schema before every write.

## Determinism requirements

- The journal file must use deterministic canonical JSON serialization.
- Appends must be atomic.
- Repeated writes from the same authoritative source state must be idempotent.
