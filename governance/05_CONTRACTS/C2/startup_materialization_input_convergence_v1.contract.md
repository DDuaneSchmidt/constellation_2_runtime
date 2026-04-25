---
id: C2_STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_V1
title: "Startup Materialization Input Convergence V1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Startup Materialization Input Convergence V1

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/startup_materialization_input_convergence_v1/<DAY>/startup_materialization_input_convergence.v1.json`

Rules:
- this artifact is the owned upstream convergence surface for canonical startup-materialization prerequisites
- it must run before `startup_materialization_v1`
- it must materialize and verify the required canonical input graph for startup-materialization prep, including:
  - operator cash-ledger statement for the requested day
  - `cash_ledger_v1/snapshots/<DAY>/cash_ledger_snapshot.v1.json`
  - `accounting_v2/nav/<DAY>/nav.v2.json`
  - `startup_materialization_inputs_prep_v1`
- `convergence_status=SUCCESS` requires all required inputs to be present and target-day aligned, and `startup_materialization_inputs_prep_v1` must evaluate `PASS`
- this surface is upstream fact only and must not claim readiness or admission authority
- `startup_materialization_v1` and Session Authority target-day build must consume this artifact as a required prerequisite
