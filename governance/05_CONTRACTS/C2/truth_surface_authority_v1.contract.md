---
id: C2_TRUTH_SURFACE_AUTHORITY_CONTRACT_V1
title: "C2 Truth Surface Authority Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-02-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Truth Surface Authority Contract (V1)

## Purpose

Guarantee **single authoritative ownership** for truth surfaces and prevent ambiguous multi-version truth.

## Authoritative registry

The authoritative registry is:

- `governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json`

It defines:
- each surface family
- which version is ACTIVE
- whether multiple versions are mutually exclusive per-day

## Hard stop

If an exclusive surface has multiple versions present for the same day, the system must FAIL-CLOSED.

The enforcement gate produces:

- `constellation_2/runtime/truth/reports/truth_surface_authority_gate_v1/<DAY>/truth_surface_authority_gate.v1.json`

## Migration rule

Deprecated versions may exist historically but must not be concurrently produced for the same day when an ACTIVE version exists.

## Atomicity

All authoritative truth artifacts must be written atomically and immutably.
