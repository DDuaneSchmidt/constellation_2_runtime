---
id: C2_CERTIFIED_OPERATIONAL_READINESS_V1
title: "C2 Certified Operational Readiness Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_operational_readiness
---

# C2 Certified Operational Readiness Contract v1

## Purpose

Define the smallest certified operational timeline/readiness surface required for release and incident triage in Bundle 7.

## Surface class

- this surface is a deterministic, non-mutating read model
- it is current-state only in Bundle 7; no separate durable artifact family is required
- authority labeling MUST be explicit and must not exceed its evidence basis

## Allowed upstream inputs

Only the following inputs are allowed:

- `deployment_state_machine_v1`
- required Bundle 7 validator outputs
- `control_plane_operator_status_v1`
- `transition_timeline_projection_v1`
- governing refs embedded in those surfaces

Raw runtime files outside certified truth and validator outputs are forbidden inputs.

## Required output fields

At minimum the surface MUST expose:

- `authority_label`
- `baseline_status`
- `deployment_status`
- `current_operator_status`
- `chain_certification_status`
- `latest_blocked_transition`
- `latest_transition_rows`
- `validator_failures`
- `forbidden_root_hits`
- `blocking_errors`
- `governing_refs`
- `readiness_summary`

## Blocked-readiness semantics

- if any required validator or deployment proof is missing, the surface MUST report blocked or unknown explicitly
- blocked state MUST preserve the failure taxonomy from validator and deployment outputs
- the surface MUST not invent currentness, certification, or causal meaning beyond its inputs
