---
id: C2_V2_READINESS_DEPENDENCY_CONTRACT_V1
title: "C2 V2 Readiness Dependency Contract v1"
status: DRAFT
version: 1
created_utc: 2026-03-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 V2 Readiness Dependency Contract v1

## Purpose

Define governed readiness dependencies for PAPER promotion review in v2 architecture, without collapsing execution authority and readiness authority.

This contract is governance-first and does not directly change orchestrator behavior.

## Authority split (non-negotiable)

- Execution authority (PAPER): sleeve truth execution outputs.
- Monitoring authority: global/system monitoring truth.
- Readiness authority: governed readiness dependency and promotion policy surfaces.
- Promotion decision framing: derived from readiness authority only.

## Dependency definitions (v2 readiness scope)

The following dependencies are governed via:

- `governance/02_REGISTRIES/C2_LIFECYCLE_DEPENDENCY_CLASSIFICATION_V1.json`

Current governed dependencies:

1. `position_lifecycle_v2`
2. `exit_obligations_v1`
3. `exposure_reconciliation_v2`

## Required dependency fields

For each readiness dependency, governance MUST define:

- `dependency_name` (`dep_id`)
- `classification` (`REQUIRED_FOR_EXECUTION|REQUIRED_FOR_READINESS|OPTIONAL_MONITORING`)
- `required_scope` (`execution|readiness|monitoring`)
- `producer_responsibility` (orchestrator stage or external producer ownership)
- `production_path` (authoritative artifact path)
- `freshness_expectation` (policy binding)
- `blocking_effect` (execution blocking, readiness blocking, monitoring-only)
- `attestation_or_evidence_path` (proof location)

## Current proven v2 state (as of 2026-03-09)

- Dependencies above are classified `REQUIRED_FOR_READINESS`.
- Runtime lifecycle diagnosis reports `lifecycle_cause_class=NOT_SCHEDULED`.
- Orchestrator v2 currently runs monitor surfaces but does not schedule legacy lifecycle producers.

## Producer model options

### Model A: Orchestrator-integrated readiness producers

- Add non-blocking v2 readiness stages that produce required lifecycle artifacts.
- Stage outputs become same-run readiness evidence.
- Failure effect: readiness blocking only unless separately classified as execution/safety scope.

### Model B: External scheduled producers + governed attestation

- Keep producers external to orchestrator v2 run path.
- Require governed attestation surface proving required dependency production and freshness.
- Failure effect: readiness blocking only unless separately classified as execution/safety scope.

## Recommendation (repo-evidence aligned)

Recommended target model: **Model B first**, then optionally migrate to Model A.

Rationale:

- preserves current v2 execution semantics and avoids unauthorized orchestration expansion
- keeps authority boundaries explicit (execution PASS may coexist with readiness block)
- allows governed dependency completeness via attestation before stage integration decision

## Contractual blocking rules

- `REQUIRED_FOR_READINESS` missing/stale/fail dependencies MUST block readiness.
- `REQUIRED_FOR_READINESS` failures MUST NOT invalidate execution PASS by scope collapse.
- `REQUIRED_FOR_EXECUTION` dependencies, if defined in future, may block execution scope only through explicit governance.

## Required artifacts and bindings

- Dependency classification registry:
  - `governance/02_REGISTRIES/C2_LIFECYCLE_DEPENDENCY_CLASSIFICATION_V1.json`
- Readiness policy:
  - `governance/02_REGISTRIES/C2_SLEEVE_LIVE_READINESS_POLICY_V1.json`
- Freshness policy:
  - `governance/02_REGISTRIES/C2_DIAGNOSTICS_FRESHNESS_POLICY_V1.json`
- Readiness output:
  - `constellation_2/runtime/truth_sleeves/<sleeve_id>/PAPER/readiness_v1/sleeve_live_readiness_v1/<DAY_UTC>/sleeve_live_readiness.v1.json`

## Future change control

Before any v2 orchestrator producer-stage additions, governance MUST first update:

1. this contract
2. lifecycle dependency classification registry
3. readiness contract/policy binding as needed

No automatic producer-model switch is permitted without explicit governance update.
