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
- `governance/05_CONTRACTS/C2/capital_risk_envelope_v1.contract.md`
- `governance/05_CONTRACTS/C2/bundle_a_paper_trading_readiness_audit_proof_v1.contract.md`
- `governance/05_CONTRACTS/C2/bundle_b_risk_blindspot_elimination_v1.contract.md`
- `governance/05_CONTRACTS/C2/bundled_c_exposure_convergence_lifecycle_v1.contract.md`
- `governance/05_CONTRACTS/C2/economic_nav_drawdown_truth_spine_bundle_v1.contract.md`
- `governance/05_CONTRACTS/C2/nav_snapshot_truth_v1.contract.md`
- `governance/05_CONTRACTS/C2/nav_history_ledger_v1.contract.md`
- `governance/05_CONTRACTS/C2/drawdown_window_pack_v1.contract.md`
- `governance/05_CONTRACTS/C2/economic_truth_availability_certificate_v1.contract.md`
- `governance/05_CONTRACTS/C2/regime_classification_spine_v1.contract.md`
- `governance/05_CONTRACTS/C2/regime_classification_spine_v1.deprecation_notice.md`
- `governance/05_CONTRACTS/C2/regime_classification_spine_v2.contract.md`

## Governed data schemas

These JSON schemas are governance-controlled and must be explicitly listed in `governance/00_MANIFEST.yaml` to be treated as governed artifacts:

### Existing governed schemas
- `governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_data_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_calendar.v1.schema.json`

### Engine activity schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/oms_decision.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/intents_day_rollup.v1.schema.json`

### Phase J — Monitoring schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/MONITORING/portfolio_nav_series.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_metrics.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_daily_returns.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/engine_correlation_matrix.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/stress_replay_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/degradation_sentinel.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/capital_efficiency.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/nav_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/nav_history_ledger.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/drawdown_window_pack.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/economic_truth_availability_certificate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/regime_snapshot.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/MONITORING/regime_snapshot.v2.schema.json`

### Reports schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_daily_gate.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_gate_verdict.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v2.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/pipeline_manifest.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/exposure_reconciliation_report.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/delta_order_plan.v1.schema.json`

### Execution evidence schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/submission_index.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_event_raw.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_event_day_manifest.v1.schema.json`

### Risk schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/RISK/engine_model_registry.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RISK/engine_risk_budget_ledger.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json`

### Positions schemas (governed outputs)
- `governance/04_DATA/SCHEMAS/C2/POSITIONS/position_lifecycle_ledger.v1.schema.json`

- governance/03_CONTRACTS/C2_TRUE_EVIDENCE_SPINE_V2.md — True Evidence Spine v2 (broker-truth contract)
- governance/03_CONTRACTS/C2_PHASED_SUBMISSION_RUNNER_V1.md — Phase D Submission Runner v1 (audit-grade submission entrypoint)
