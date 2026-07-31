# Aegis Operator Surface Contract Requirements

## Purpose

Create a single canonical contract that all operator-facing surfaces consume.

The contract eliminates page-specific interpretations of:

* readiness
* blocked states
* degraded states
* unavailable states
* historical states
* actionability
* empty states

No operator surface should invent its own truth.

## Core Architecture

Current:

```text
Artifacts
-> Surface Readiness
-> Semantic Invariants
-> UI
```

Target:

```text
Artifacts
-> Surface Readiness
-> Semantic Invariants
-> Operator Surface Contract
-> UI
```

The UI consumes only the Operator Surface Contract.

## Canonical Artifact

Create:

```text
aegis_operator_surface_contract_v1
```

Artifact path:

```text
truth/reports/aegis_operator_surface_contract_v1/<day>/operator_surface_contract.v1.json
```

Commands:

```bash
npm run aegis:operator-surface-contract
npm run aegis:operator-surface-contract-self-check
```

## Required Surfaces

Contract rows are required for:

```text
command_center
engineering
positions
performance
position_review
sleeve_analytics
research
ask_aegis
```

No operator surface may render without a contract row.

## Contract Fields

Every surface row must contain:

```text
surface_id
requested_day
source_day
context_day
status
render_allowed
actions_allowed
primary_message
reason
impact
next_step
metrics_allowed
diagnostics_allowed
ask_aegis_prompt
evidence_refs
artifact_refs
surface_readiness_status
semantic_invariant_status
generated_at
```

## Allowed Status Values

```text
READY
DEGRADED
BLOCKED
UNAVAILABLE
HISTORICAL
INCONSISTENT
```

No surface may invent custom status values.

## Rendering Rules

### READY

Show normal workflow.

### DEGRADED

Show:

```text
Primary Message
Reason
Impact
Next Step
```

Workflow may render only if:

```text
actions_allowed=true
```

### BLOCKED

Show:

```text
Primary Message
Reason
Impact
Next Step
Ask Aegis
```

Hide primary workflow actions.

### UNAVAILABLE

Show:

```text
Unavailable
Reason
Impact
Next Step
```

Do not render workflow content.

### HISTORICAL

Show:

```text
Viewing historical operational session.
```

Include:

```text
requested_day
source_day
```

### INCONSISTENT

Show:

```text
Surface consistency check failed.
```

Hide workflow.

## Action Rules

If:

```text
actions_allowed=false
```

The UI must not render:

```text
capture
approve
reject
execute
submit
record entry
mark captured
```

even if underlying artifacts exist.

## Metric Rules

If:

```text
metrics_allowed=false
```

The UI must not render analytics cards.

Instead show:

```text
Analytics unavailable.
```

with:

```text
Reason
Impact
```

## Diagnostics Rules

If:

```text
diagnostics_allowed=false
```

Diagnostics must not appear.

If true, diagnostics remain collapsed by default.

## Ask Aegis Integration

Every contract row must provide:

```text
ask_aegis_prompt
```

Examples:

Command Center:

```text
Why are there no actionable items today?
```

Performance:

```text
Why are analytics unavailable?
```

Engineering:

```text
Why is runtime blocked?
```

Research:

```text
Why is this hypothesis waiting?
```

Ask Aegis becomes surface-aware through the contract.

## Surface Ownership

### Command Center

Primary Message:

```text
What requires operator attention?
```

### Engineering

Primary Message:

```text
What is broken and how do I repair it?
```

### Positions

Primary Message:

```text
What positions are active today?
```

### Performance

Primary Message:

```text
How did the portfolio perform?
```

### Position Review

Primary Message:

```text
What matters most for this position?
```

### Sleeve Analytics

Primary Message:

```text
How are sleeves performing?
```

### Research

Primary Message:

```text
What is being validated?
```

## Empty State Rules

Empty states must come from the contract.

Pages may not implement local empty-state logic.

Forbidden:

```text
if open_positions == 0:
   show custom message
```

Required:

```text
show contract.primary_message
show contract.reason
show contract.next_step
```

## Consistency Rules

The contract must consume:

```text
aegis_surface_readiness_v1
aegis_semantic_invariants_v1
```

No UI page may bypass them.

## Self Check

Add:

```bash
npm run aegis:operator-surface-contract-self-check
```

Fail if:

* required surface missing
* status invalid
* actions_allowed contradicts readiness
* metrics rendered while metrics_allowed=false
* blocked surface exposes actions
* inconsistent surface renders workflow
* page-specific empty-state logic detected

## Golden Scenario Requirements

2026-05-29:

```text
Canonical trading day
```

must render READY surfaces correctly.

2026-05-30:

```text
Fail-closed day
```

must render blocked/degraded/unavailable states correctly.

## Acceptance Criteria

The operator should never need to ask:

```text
Why is this page different?
```

All surfaces should explain:

```text
status
reason
impact
next_step
```

the same way.

## Governance Rule

Operator surfaces may not define their own truth.

All operator-facing behavior must be derived from:

```text
aegis_operator_surface_contract_v1
```

Update:

```text
aegis/modules/operator_portal/aegis.module.yaml
```

to reference this document as the authority for all operator surface rendering.
