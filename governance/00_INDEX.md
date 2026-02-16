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

### B) C2 Canonical Contracts (Runtime + Audit Authority)
- `governance/05_CONTRACTS/C2/`

### C) Registries (Enumerations / reason codes / constants)
- `governance/02_REGISTRIES/`

### D) Operator docs and runbooks (non-authoritative guidance)
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

## Governed canonical contracts

These canonical contracts are governance-controlled and must be explicitly listed in `governance/00_MANIFEST.yaml` to be treated as governed artifacts:

- `governance/05_CONTRACTS/C2/drawdown_convention_v1.contract.md`

## Governed data schemas

These JSON schemas are governance-controlled and must be explicitly listed in `governance/00_MANIFEST.yaml` to be treated as governed artifacts:

### Existing governed schemas
- `governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_data_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_calendar.v1.schema.json`

### Phase J — Monitoring schemas (new governed outputs)
- `governance/04_DATA/SCHEMAS/C2/MONITORING/portfolio_nav_series.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_metrics.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_daily_returns.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_correlation_matrix.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/stress_replay_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/degradation_sentinel.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/capital_efficiency.v1.schema.json`

### Bundle 4 — Reports schemas (new governed outputs)
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_daily_gate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v1.schema.json`


### Bundle H-0 — OMS decision truth schema (new governed output)
- `governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/oms_decision.v1.schema.json`
