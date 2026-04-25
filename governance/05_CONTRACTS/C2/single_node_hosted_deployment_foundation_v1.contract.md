---
id: C2_SINGLE_NODE_HOSTED_DEPLOYMENT_FOUNDATION_V1
title: "Single-Node Hosted Deployment Foundation V1"
status: DRAFT
version: 1
created_utc: 2026-04-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Single-Node Hosted Deployment Foundation V1

Canonical output:
- `/home/node/constellation_runtime_data/single_node_hosted_preflight_v1/<PREFLIGHT_ID>/single_node_hosted_preflight.v1.json`

Canonical hosted seam in this pass:
- `ops/systemd/user/c2-paper-day-orchestrator.service`
- `ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh`
- `ops/systemd/user/c2-execution-observer.service`
- `ops/run/c2_execution_observer_v1.sh`

Rules:
- this contract governs the minimal deterministic hosted launch foundation for a single node only
- it does not replace runtime identity, runtime lifecycle, execution identity, execution-root authority, reconciliation authority, or evaluation-governance authority
- canonical hosted launch remains systemd-driven on one node; container, compose, and multi-node orchestration are out of scope
- hosted launch must fail closed before lifecycle admission when hosted preflight does not pass
- hosted preflight must prove:
  - authoritative repo-root alignment through active runtime identity
  - canonical service unit and wrapper presence for the touched seam
  - startup/lifecycle tool presence
  - release/deployment tool presence
  - interpreter resolution for the touched seam
  - required runtime directories exist
  - required Python modules for the touched seam are importable in the selected interpreter
- hosted preflight must emit append-only evidence under runtime data when runtime identity resolution succeeds
- hosted preflight must not silently resolve the touched seam through `/home/node/constellation_2_runtime`
- interpreter selection order for the touched seam is:
  - `PYTHON_BIN_OVERRIDE` when explicitly provided
  - `/home/node/constellation/.venv_c2/bin/python` when present
  - `python3`
- if the resolved interpreter path falls under `/home/node/constellation_2_runtime`, hosted preflight must block launch
- canonical startup and restart safety remain governed by:
  - `active_runtime_contract.v1`
  - `runtime_startup_identity.v1`
  - `runtime_lifecycle_v1`
- the hosted deployment foundation may reference existing release-root build/activation and deployment-state tooling, but this pass does not change live business semantics or bind proposal-only evaluation control into execution
- remaining repo-wide launcher normalization, supervisor cleanup, release-root direct-service cutover, and secrets/bootstrap packaging remain future work
