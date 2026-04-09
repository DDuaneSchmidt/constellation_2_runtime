---
id: C2_PAPER_OPEN_READINESS_V1
title: "C2 Paper-Open Readiness Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Paper-Open Readiness Contract (V1)

## Purpose

This contract defines the governed paper-open readiness artifact used to record whether paper-open is allowed based on governed subsystem readiness evidence and the canonical paper-session ledger.

This phase introduces only the shared paper-open readiness artifact model.

It does not execute paper-open, switch runtime mode, or activate production behavior.

## Required report meaning

A paper-open readiness artifact must explicitly identify:

- readiness id
- environment name
- subsystem readiness refs
- unresolved blockers
- unresolved semantic gaps
- overall status
- paper-open-allowed flag
- rationale
- recorded-at timestamp

## Non-authoritative semantics

Paper-open readiness is governed readiness truth only.

Paper-open readiness must not be treated as runtime trading truth or lifecycle activation truth.

Paper-open readiness is a derived-only downstream view.

Paper-open readiness informs operator presentation only after the paper-session ledger is evaluated.

Within the Testing Evidence Plane, this artifact is the only allowed source of `paper_open_allowed = true | false`.

Logs, raw test output, and runtime code paths must not independently determine paper-open readiness state.

If the paper-session ledger denies or is unavailable, paper-open readiness must fail closed and must not emit `READY`.

## Canonical runtime instance path

When materialized as a governed runtime-truth report, the canonical path pattern is:

`constellation_2/runtime/truth/reports/<artifact_family_v1>/<DAY>/<artifact>.v1.json`

For this artifact family, the concrete path is:

`constellation_2/runtime/truth/reports/paper_open_readiness_v1/<DAY>/paper_open_readiness.v1.json`

## Fail-closed rules

If subsystem readiness refs, unresolved blockers, unresolved semantic gaps, overall status, or rationale is missing or contradictory, the artifact must fail closed.

If paper-open-allowed is true while overall status is not ready or unresolved blockers remain, the artifact must fail closed.

`unresolved_semantic_gaps` must include only gaps that remain required for paper-open evaluation under current governed dependency definitions.

Explicitly excluded dependencies must not block `paper_open_allowed`.

## Non-claims

This contract does not open paper trading, bypass the paper-day runbook, or authorize broker transmission.
