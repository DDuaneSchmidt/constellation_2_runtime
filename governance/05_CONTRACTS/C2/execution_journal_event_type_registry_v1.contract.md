---
id: C2_EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_V1
title: "C2 Execution Journal Event Type Registry Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_execution_journal_event_type_registry
---

# C2 Execution Journal Event Type Registry Contract (V1)

## Purpose

This contract governs the bounded event taxonomy for `execution_journal_v1`.

It exists so event types, allowed emitters, and payload families cannot drift silently in code.

## Canonical registry

- Canonical registry file:
  - `governance/02_REGISTRIES/C2_EXECUTION_JOURNAL_EVENT_TYPE_REGISTRY_V1.json`
- Governing writer/validator:
  - `constellation_2/common/execution_journal_v1.py`

## Required registry fields

Every governed event type entry must define:

- `event_type`
- `payload_family`
- `semantics`
- `allowed_event_sources`
- `required_payload_fields`
- `event_key_payload_fields`
- `duplicate_policy`
- `multiple_per_day_attempt`

## Initial governed event types

- `DEPLOYMENT_ACTIVATED`
- `DEPLOYMENT_BLOCKED`
- `STARTUP_MATERIALIZATION_COMPLETED`
- `STARTUP_PROOF_VALIDATION_COMPLETED`
- `LEDGER_AUTHORITY_RECORDED`
- `STATE_MACHINE_DECISION_RECORDED`
- `SUBMISSION_AUTHORIZATION_RECORDED`
- `STAGE_DURATION_RECORDED`
- `SYSTEM_CONTRADICTION_DETECTED`

## Change control

- Unknown event types must fail closed.
- Event sources not listed for a governed event type must fail closed.
- Required payload fields must be enforced through the registry, not by ad hoc runner-local rules.
- Registry evolution requires governance update in git before any new event type may be emitted.
