---
id: C2_EVALUATION_AUTHORITY_FOUNDATION_V1
title: "C2 Evaluation Authority Foundation v1"
status: DRAFT
version: 1
created_utc: 2026-04-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_evaluation_authority_foundation
---

# C2 Evaluation Authority Foundation v1

## Purpose

Define the evidence-first authority split for Constellation evaluation governance without collapsing measured truth, explanatory truth, policy truth, governance judgment, and operative control into one artifact family.

## Authority Surfaces

### Evidence Assembly Authority

Canonical owner:

- `evaluation_input_manifest_v1`

Responsibilities:

- freeze the exact downstream evaluation input set
- reference canonical source evidence rather than duplicate payloads
- bind the effective evaluation policy snapshot
- publish a frozen input bundle for downstream authorities

### Evaluation Policy Authority

Canonical owner:

- `evaluation_policy_snapshot_v1`

Responsibilities:

- publish the effective policy snapshot used by downstream authorities
- separate scoring/sufficiency/freshness/control-interpretation policy from measured outputs

### Outcome Attribution Authority

Canonical owner:

- `outcome_attribution_snapshot_v1`

Responsibilities:

- publish explanatory decomposition of realized sleeve outcomes
- emit confidence and residual/unexplained state
- remain explanatory only

### Sleeve Edge Measurement Authority

Canonical owner:

- `sleeve_edge_measurement_snapshot_v1`

Responsibilities:

- publish measured edge/stability/confidence outputs
- reference frozen evaluation inputs and attribution outputs
- remain separate from governance action and operative control

### Allocation Governance Authority

Canonical owner:

- `allocation_governance_snapshot_v1`

Responsibilities:

- compare measured sleeve state with current allocation-control posture
- emit recommendation-only governance judgments
- not directly mutate live capital allocation

### Operative Control Authority

Canonical owner:

- `sleeve_operative_control_state_v1`

Responsibilities:

- publish proposed or effective sleeve control state
- carry `effective_from`, supersession, and override linkage when available
- remain separate from measurement truth

## Non-goals

- direct strategy, execution, portfolio, or broker behavior changes
- replacing execution/session identity ownership
- dashboard-first truth creation
- implicit action from raw metrics without governed policy

## Separation Rules

- source evidence truth must remain upstream of evaluation assembly
- attribution must not own control state
- measurement must not own live control state
- allocation governance must remain recommendation-only unless a separate control adoption explicitly binds it
- operative control must not rewrite measured truth

## Input Spine

The minimal sleeve-scoped authority chain is expected to reuse canonical evidence such as:

- `day_open_attempt_v1`
- `execution_reconciliation_v1`
- `reconciliation_report_v3`
- `sleeve_edge_fact_ledger_v1`
- `sleeve_edge_snapshot_v1`
- `capital_authority_allocation_v1`

Optional supporting refs may include:

- `sleeve_governance_action_state_v1`
- `operator_intervention_state_v1`

## Override / Control Interpretation

- operator override remains separately owned by `operator_intervention_state_v1`
- operative control may reference override state, but must not absorb override ownership
- proposal-only control state is valid in v1 when live binding is not yet proven safe

## Fail-Closed Requirements

- malformed or missing required upstream evidence must block downstream authority materialization
- policy snapshots must be explicit and governed
- downstream authorities must not restitch their own private input bundles outside `evaluation_input_manifest_v1`

## Audit Lineage Requirements

- every authority output must publish exact upstream refs
- every judgment/control artifact must bind the exact policy snapshot used
- runtime lifecycle provenance must remain referenced through upstream evidence artifacts rather than duplicated wholesale
