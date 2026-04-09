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

The artifact may be emitted by the authority kernel before orchestration starts.

It is validation-only.

It does not activate production, publish authority, or independently gate execution.

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

## Validation reporting rule

If required schemas, mappings, authority attestations, or current-day prerequisite artifacts are missing or incompatible, the artifact must record that validation state explicitly.

Runtime components may consume that state for diagnostics and evidence only.

## Canonical runtime instance path

When materialized as a governed runtime-truth report, the canonical path pattern is:

`constellation_2/runtime/truth/reports/<artifact_family_v1>/<DAY>/<artifact>.v1.json`

For this artifact family, the concrete path is:

`constellation_2/runtime/truth/reports/day_authority_decision_v1/<DAY>/day_authority_decision.v1.json`
