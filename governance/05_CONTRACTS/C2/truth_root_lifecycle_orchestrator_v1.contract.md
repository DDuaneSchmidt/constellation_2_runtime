---
id: C2_TRUTH_ROOT_LIFECYCLE_ORCHESTRATOR_V1
title: "Truth-Root Lifecycle Orchestrator v1 Contract"
status: ACTIVE
owner: Constellation
last_updated_utc: 2026-04-21T00:00:00Z
---

# Truth-Root Lifecycle Orchestrator v1

## Canonical authority

- Canonical lifecycle orchestration owner: `ops/tools/run_gate_authority_plane_v1.py` via `constellation_2/common/truth_lifecycle_orchestrator_v1.py`.
- Canonical phase graph registry: `governance/02_REGISTRIES/C2_TRUTH_LIFECYCLE_PHASE_GRAPH_V1.json`.
- Canonical lifecycle run ownership root: `/home/node/constellation_runtime_data/truth`.

## Phase graph

Required ordered phases:

1. `GENESIS_BOOTSTRAP`
2. `PRIOR_DAY_CLOSE`
3. `DAY_ADMISSION`
4. `CONTEXT_AUTHORITY`
5. `GATE_INPUTS`
6. `GATE_PRODUCTION`
7. `GATE_AGGREGATION`
8. `AUTHORIZATION`
9. `EXECUTION_READINESS`
10. `EXECUTION`
11. `POST_EXECUTION_RECONCILIATION`
12. `DAY_CLOSE`

All phase execution order is registry-defined and deterministic. Phases after the first blocking phase are recorded as `SKIPPED` with upstream blocker linkage.

## Uniform blocker contract

Each phase result must include:

- `day_utc`
- `phase_id`
- `status` (`PASS|FAIL|BLOCKED|SKIPPED|BOOTSTRAP`)
- `first_blocker_code`
- `first_blocker_artifact_path`
- `upstream_dependency_id`
- `upstream_dependency_artifact_path`
- `reason_codes`
- `truth_root`
- `produced_utc`
- `bootstrap_applied`
- `bootstrap_reason`

Schema authority:

- `governance/04_DATA/SCHEMAS/C2/REPORTS/truth_lifecycle_phase_result.v1.schema.json`

## Day run ledger contract

Every lifecycle run writes a canonical day ledger:

- path family: `reports/truth_day_run_ledger_v1/<DAY>/truth_day_run_ledger.v1.json`
- schema: `governance/04_DATA/SCHEMAS/C2/REPORTS/truth_day_run_ledger.v1.schema.json`

The ledger records:

- phase execution outcomes
- producer invocations
- first blocker chain root
- bootstrap usage
- readiness progression flags (`structural_readiness_reached`, `business_no_trade`, `execution_readiness_reached`, `execution_reached`, `day_close_reached`)

## Genesis bootstrap semantics

`GENESIS_BOOTSTRAP` is allowed only when prior-day economic continuity is absent. It must:

- invoke canonical producer path (`run_economic_state_authority_v1.py`) rather than fabricating artifacts
- record `bootstrap_applied=true` and explicit `bootstrap_reason`
- preserve fail-closed semantics if bootstrap cannot produce a complete prior-day economic state chain

Once prior-day continuity exists, bootstrap must not be applied.

## Truth ownership rule

- Lifecycle decisions are truth-root owned.
- Sleeve-scoped side effects must not be treated as truth-root phase completion evidence.
- Authorization, gate aggregation, lifecycle phase results, and day run ledger are truth-root artifacts.
