---
id: EXECUTION_READINESS_GATE_V1
title: "Constellation 2.0 - Execution Readiness Gate V1"
status: ACTIVE
version: 1
created_utc: 2026-04-08T03:12:30Z
owner: Constellation
authority: governance+git+runtime_data
scope: constellation_2_0
tags:
  - execution_readiness
  - runtime_contract
  - runtime_data
  - legality_gate
---

# Objective

Define one top-level machine-readable legality gate that answers whether a paper-day run is allowed before runtime execution.

# Output Artifact

- Canonical report path:
  - `/home/node/constellation_runtime_data/truth/reports/execution_readiness_gate_v1/<DAY>/execution_readiness_gate.v1.json`

# Required Inputs For Current Paper-Day Scope

- Active runtime contract:
  - `/home/node/constellation_runtime_data/runtime_contract_v1/active_runtime_contract.v1.json`
- Runtime-data readiness preflight report for `day_utc`
- Active runtime contract family compatibility report for `day_utc`
- Governed market calendar source-data manifest:
  - `constellation_2/phaseJ/source_data/market_calendar_source_v1/dataset_manifest.json`
- Runtime-data seed receipt for `market_calendar_v1`
- Current downstream family result for the same release/runtime/data scope

# Decision Rule

- `run_allowed=true` only if all required checks pass and `missing_count=0`
- otherwise `run_allowed=false`
- `blocking_reasons` must be explicit and machine-readable

# Current Required Checks

1. active release pointer resolves
2. active runtime contract exists and `status=ACTIVE`
3. runtime-data readiness preflight passes
4. active runtime contract family compatibility passes
5. governed `market_calendar_v1` source-data bundle exists and includes exact `day_utc`
6. latest PASS runtime-data seed receipt for `market_calendar_v1` exists
7. downstream family validation for the current release/runtime/data scope is green
8. no other required artifact for `day_utc` is missing

# Boundaries

- The gate is declarative only in this contract pass.
- Service wiring is not modified here.
- The gate must fail closed on missing or malformed required inputs.
