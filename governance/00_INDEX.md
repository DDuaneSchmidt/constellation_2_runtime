---
id: GOVERNANCE_INDEX_C2_V1
title: "Constellation 2.0 Governance Index"
status: ACTIVE
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Constellation 2.0 Governance Index

This repository is the constitutional root of **Constellation 2.0**.

## Authority model

- **Governance** (this folder) defines contracts, registries, and change control.
- **Git** defines exact versioned behavior of runtime code and governance.
- **Runtime truth** under `constellation_2/runtime/truth/` is the authoritative immutable output surface.

## Canonical runtime truth root

- `constellation_2/runtime/truth`

## Document classes

### A) C2 Bundle Contracts (Design Authority)
- `governance/01_CONTRACTS/C2/`

### B) Registries (Enumerations / reason codes / constants)
- `governance/02_REGISTRIES/`

### C) Operator docs and runbooks (non-authoritative guidance)
- `docs/` (informational; not a contract unless also registered in the manifest)

## Bundle F + G (Accounting + Allocation)

The following C2 Bundle F/G documents are introduced under `docs/c2/` and are tracked in the governance manifest for traceability:

- `docs/c2/F_ACCOUNTING_SPINE_V1.md`
- `docs/c2/F_ACCOUNTING_SCHEMA_V1.md`
- `docs/c2/F_ACCOUNTING_RECONSTRUCTION_GUARANTEE_V1.md`
- `docs/c2/F_MARKING_POLICY_CONSERVATIVE_V1.md`
- `docs/c2/G_ALLOCATION_SPINE_V1.md`
- `docs/c2/G_THROTTLE_RULES_V1.md`
- `docs/c2/G_RISK_BUDGET_CONTRACT_V1.md`
- `docs/c2/G_REASON_CODES_V1.md`
- `docs/c2/RUNBOOK_FG_PHASE_V1.md`

NOTE: In this repo, docs under `docs/` are treated as governed artifacts **only when explicitly listed** in `governance/00_MANIFEST.yaml`.

## Governed data schemas

These JSON schemas are governance-controlled and must be explicitly listed in `governance/00_MANIFEST.yaml` to be treated as governed artifacts:

- `governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json`
