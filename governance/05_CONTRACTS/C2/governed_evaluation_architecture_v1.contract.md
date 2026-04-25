---
id: C2_GOVERNED_EVALUATION_ARCHITECTURE_V1
title: "C2 Governed Evaluation Architecture v1"
status: DRAFT
version: 1
created_utc: 2026-04-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_governed_weekly_evaluation
---

# C2 Governed Evaluation Architecture v1

## Purpose

Define the governed measurement, validity, evaluation, governance-action, and scorecard architecture for weekly sleeve and portfolio judgment.

## Scope

- sleeve-level weekly edge measurement and judgment
- portfolio-level weekly performance and risk judgment
- explicit benchmark policy by sleeve role
- explicit `no_conclusion` outcomes when evidence is not sufficient to judge

## Non-goals

- UI-first scoring
- direct control action from raw metrics
- scorecard-owned metric computation
- silent fallback from missing evidence to synthetic PASS or FAIL

## Layer 1: Measurement truth authorities

Measurement truth is limited to measured facts and measured summaries.

Adopted v1 measurement truth surfaces:

- `sleeve_performance_truth_v1`
- `portfolio_performance_truth_v1`

These surfaces may bind existing legacy sleeve-edge measurement sources, but they must publish the frozen upstream references explicitly and must not mix governance action into the measurement truth output.

## Layer 2: Validity / evidence compilers

Validity must exist before judgment.

Required validity states:

- `insufficient_sample`
- `execution_contaminated`
- `benchmark_not_applicable`
- `benchmark_invalid`
- `data_incomplete`
- `provisional`
- `valid`

Adopted v1 validity compilers:

- `sleeve_validity_state_v1`
- `portfolio_validity_state_v1`

## Layer 3: Evaluation compilers

Evaluation consumes measurement truth plus validity state and produces governed compiled states.

Required separation rules:

- no action logic in evaluation compilers
- no hidden raw metric recomputation in scorecards
- weak or invalid evidence must remain explicit

Adopted v1 evaluation compilers:

- `sleeve_evaluation_state_v1`
- `portfolio_evaluation_state_v1`

## Layer 4: Governance policy engine

Governance action is a policy-backed interpretation step.

Required action outputs:

- `continue`
- `watch`
- `reduce`
- `pause`
- `retire`
- `no_conclusion`

Required rules:

- raw metrics must not directly drive governance action
- validity states may force `no_conclusion`
- execution contamination must block aggressive action
- benchmark policy must be explicit by sleeve role

Adopted v1 governance action compilers:

- `sleeve_governance_action_state_v1`
- `portfolio_governance_action_state_v1`

## Runtime control linkage

Runtime control may consume governed evaluation only through canonical governance action artifacts.

Required control-linkage rules:

- runtime control seams must not read raw evaluation metrics directly
- runtime control seams must not read `weekly_scorecard_view_v1`
- action-to-control behavior must come from governed policy
- missing or invalid governance action artifacts must fail safe
- `no_conclusion` must map to explicit conservative runtime behavior rather than implicit continue

First adopted v1 runtime control seam:

- `ops/tools/run_capital_authority_allocation_day_v1.py`

## Layer 5: Operator scorecards

Operator scorecards are derived-only read models.

Allowed scorecard behavior:

- display summary labels
- display weekly grades as presentation only
- show `no_conclusion` prominently

Forbidden scorecard behavior:

- computing canonical pnl, drawdown, benchmark return, expectancy, or action state
- claiming canonical authority
- bypassing validity states

Adopted v1 scorecard surface:

- `weekly_scorecard_view_v1`

## Benchmark policy by sleeve role

The v1 policy registry assigns explicit benchmark interpretation by sleeve role:

- `directional_equity` => external symbol benchmark
- `defensive` => cash baseline
- `neutral_absolute_return` => cash baseline
- `diversifier` => absolute-threshold / portfolio-contribution interpretation

Every sleeve must have an explicit role assignment before benchmark-backed judgment is considered valid.

## No-conclusion rules

`no_conclusion` is first-class.

It is the required outcome when:

- sample evidence is insufficient
- execution is contaminated
- required data is incomplete
- benchmark evidence is required but invalid
- portfolio or sleeve evidence remains provisional

## Scorecard restrictions

- scorecards must read canonical measurement, validity, evaluation, and action artifacts
- scorecards must not become a second truth path
- display grades are derived presentation only

## Invariants

- scorecards do not compute canonical truth
- validity exists before judgment
- evaluation is separate from governance action
- action is policy-backed
- benchmark policy is explicit by sleeve role
- `no_conclusion` remains reachable and mandatory when evidence is weak
- fail-closed behavior remains in force for missing required governed upstreams

## Audit lineage requirements

- every adopted governed evaluation artifact must publish exact upstream references
- every governance action artifact must bind the effective evaluation policy snapshot
- every derived scorecard row must point back to canonical evaluation and action artifacts
