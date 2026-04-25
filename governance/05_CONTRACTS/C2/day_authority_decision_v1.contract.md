---
id: C2_DAY_AUTHORITY_DECISION_V1
title: "C2 Day Authority Decision Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Day Authority Decision Contract (V1)

## Purpose

This contract defines a canonical governed validation artifact for trading-day preflight evidence.

The artifact is emitted from same-day control-plane validation before orchestration starts.

It is validation-only.

It does not activate production, publish authority, or independently gate execution.

## Artifact classification

`day_authority_decision_v1` is a `SAME_DAY_RUNTIME_ARTIFACT`.

It is not a stable day-definition artifact and it is not valid for full-year precomputation.

## Canonical producer path

Canonical writer:

- `ops/tools/run_day_authority_decision_v1.py`

Owned upstream source:

- `ops/tools/run_session_readiness_refresh_v1.py`

`run_session_readiness_refresh_v1.py` must materialize enough same-day validation state for the day-authority decision to be written even when readiness is blocked.

Session Authority may invoke the explicit writer after readiness refresh so admission never treats the artifact as missing when the real state is simply blocked.

## Non-authority rule

Lifecycle artifacts and governed registry state remain the only authoritative path for activation.

No component may treat this artifact as a substitute authority signal.

## Required validation meaning

A day authority decision must include:

- trading day
- decision state
- heartbeat
- first failure
- stage
- orchestrator started flag
- blocking class
- exact blocking evidence
- exact missing or invalid prerequisite refs
- authority attestation refs used
- registry, mapping, and schema compatibility status
- emitted-at timestamp
- run metadata

## Required upstream inputs

The artifact depends on same-day validation inputs, including:

- startup materialization result
- IB API handshake result
- global gate refresh result
- primary scoped readiness summary
- registry, mapping, and schema compatibility checks from the authority kernel

Those inputs are current-day runtime validation surfaces, not full-year stable definitions.

## Validation reporting rule

If required schemas, mappings, authority attestations, or current-day prerequisite artifacts are missing or incompatible, the artifact must record that validation state explicitly.

Runtime components may consume that state for diagnostics and evidence only.

Because the artifact depends on current-day validation state, precomputing it for all eligible 2026 trading days is invalid.

## Canonical runtime instance path

When materialized as a governed runtime-truth report, the canonical path pattern is:

`constellation_2/runtime/truth/reports/<artifact_family_v1>/<DAY>/<artifact>.v1.json`

For this artifact family, the concrete path is:

`constellation_2/runtime/truth/reports/day_authority_decision_v1/<DAY>/day_authority_decision.v1.json`
