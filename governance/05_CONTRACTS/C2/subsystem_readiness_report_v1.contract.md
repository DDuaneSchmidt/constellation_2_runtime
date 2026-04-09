---
id: C2_SUBSYSTEM_READINESS_REPORT_V1
title: "C2 Subsystem Readiness Report Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Subsystem Readiness Report Contract (V1)

## Purpose

This contract defines the governed subsystem-readiness report used to summarize readiness evidence for explicitly named subsystems.

This phase introduces only the shared readiness-report artifact model.

It does not authorize paper-open execution or production activation.

## Minimum explicit subsystem coverage

At minimum, governed readiness reporting must be able to represent:

- governance
- lifecycle
- runtime_truth
- bond
- advisory
- signal
- legacy_holdings
- operator_session_replay

## Required report meaning

A subsystem-readiness report must explicitly identify:

- readiness id
- subsystem name
- status
- required result refs
- blocking issues
- semantic gaps
- approved-for-next-gate flag
- recorded-at timestamp

## Non-authoritative semantics

A subsystem-readiness report is governed readiness truth only.

It must not be treated as runtime decision authority, release authority, or registry-selected active-production authority.

Subsystem readiness must be expressed through this governed artifact family, not inferred from raw logs, raw pytest output, or ad hoc runtime code paths.

## Canonical runtime instance path

When materialized as a governed runtime-truth report, the canonical path pattern is:

`constellation_2/runtime/truth/reports/<artifact_family_v1>/<DAY>/<artifact>.v1.json`

For this artifact family, the concrete path is:

`constellation_2/runtime/truth/reports/subsystem_readiness_report_v1/<DAY>/subsystem_readiness_report.v1.json`

## Fail-closed rules

If subsystem identity, readiness status, required result refs, blocking issues, or semantic gaps are missing or ambiguous, the report must fail closed.

If approved-for-next-gate is asserted while blocking issues or semantic gaps remain unresolved, the report must fail closed.

Explicitly excluded dependencies must not be represented as semantic gaps in this artifact family.

In this phase, `legacy_holdings` is not a required upstream dependency for Advisory readiness and must not appear as an Advisory-readiness semantic gap on that basis.

## Non-claims

This contract does not open paper trading, perform approval, or mutate lifecycle state.
