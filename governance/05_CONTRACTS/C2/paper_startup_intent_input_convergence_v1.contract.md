---
id: C2_PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_V1
title: "Paper Startup Intent Input Convergence V1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Paper Startup Intent Input Convergence V1

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/paper_startup_intent_input_convergence_v1/<DAY>/paper_startup_intent_input_convergence.v1.json`

Rules:
- this artifact is the owned upstream convergence surface for PRIMARY/PAPER startup intent inputs
- it must materialize and verify the required sleeve-side defensive-tail input graph before PAPER startup authorization invokes `trading_day_intent_generation_v1`
- the owned convergence set must include:
  - `positions_v1/snapshots/<DAY>/positions_snapshot.v2.json`
  - `monitoring_v1/regime_snapshot_v2/<DAY>/regime_snapshot.v2.json`
  - defensive-tail bridge outputs under the primary sleeve truth root:
    - `positions_snapshot_v2/snapshots/<DAY>/positions_snapshot.v2.json`
    - `accounting_v1/nav/<DAY>/nav_snapshot.v1.json`
    - `market_data_snapshot_v1/snapshots/<DAY>/SPY.market_data_snapshot.v1.json`
- canonical runtime truth stores the convergence status artifact; governed sleeve truth stores the produced sleeve-side inputs
- `convergence_status=SUCCESS` requires every required input artifact to be present and bound to the requested target day
- this surface must fail closed and emit blocker chain rows in causal order
- PAPER startup authorization convergence must consume this artifact as a required prerequisite
