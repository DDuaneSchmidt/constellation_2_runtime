# Aegis Semantic Invariants Requirements

## Purpose

Semantic Invariants prevent Aegis operator pages from rendering internally contradictory analytics even when the requested-day artifacts exist and Surface Readiness passes.

Surface Readiness answers:

```text
Is this the right day?
Are required artifacts present?
Are actions allowed?
```

Semantic Invariants answer:

```text
Does the rendered content make sense?
```

## Core Architecture

```text
Canonical Artifacts
    -> Surface Readiness Gate
    -> Semantic Invariant Gate
    -> Surface Read Model
    -> Golden Scenario Browser Tests
    -> UI
```

No operator-facing page may locally invent semantic consistency. Pages consume the invariant status emitted by the canonical gate.

## Required Artifact

```text
truth/reports/aegis_semantic_invariants_v1/<day>/semantic_invariants.v1.json
```

Each invariant result must include:

```text
surface_id
invariant_id
status: PASS | FAIL | WARNING
blocking: true | false
reason
field_values
source_artifacts
source_artifact_hashes
generated_at
```

## Required Commands

```bash
npm run aegis:semantic-invariants
npm run aegis:semantic-invariants-self-check
```

## Initial Coverage

Phase 1 covers:

* Performance
* Sleeve Analytics

## Required Invariants

The gate must fail blocking checks when any of these occur:

```text
total_sleeves = 0 AND attribution_coverage = 100%
total_sleeves = 0 AND mark_coverage = 100%
open_positions unavailable AND mark_coverage reported as complete
open_positions unavailable AND attribution_coverage reported as complete
data_quality_status = NOT_CANONICAL AND page renders complete analytics
status = NOT_CANONICAL AND total_pnl appears canonical
missing_marks > 0 AND mark_coverage = 100%
missing_marks unavailable AND mark_coverage = 100%
total_paper_pnl unavailable AND full_portfolio_pnl_status = CANONICAL
```

Non-canonical data is allowed only when it is presented as non-canonical, partial, blocked, degraded, or unavailable. Non-canonical data must not be rendered as complete analytics.

## Surface Readiness Integration

`aegis_surface_readiness_v1` must consume semantic invariant results.

If a blocking semantic invariant fails for a surface, Surface Readiness must force:

```text
surface_status = BLOCKED | DEGRADED | UNAVAILABLE
render_allowed = true only for blocked/unavailable explanation
actions_allowed = false
```

## UI Requirements

Performance and Sleeve Analytics pages must consume semantic invariant status.

If a blocking invariant fails, the UI must show:

```text
Performance Analytics Unavailable
Reason: <failed invariant>
```

or:

```text
Sleeve Analytics Unavailable
Reason: <failed invariant>
```

The UI must not render contradictory metric cards when a blocking invariant fails. Raw invariant evidence must remain collapsed.

## Golden Scenario Regression Pack

Create locked scenario checks:

```text
2026-05-29 = canonical performance day
2026-05-30 = non-trading / fail-closed day
```

Required commands:

```bash
npm run aegis:golden-scenarios
npm run aegis:golden-scenarios-self-check
```

For 2026-05-29, verify:

```text
Performance canonical when marks are present
Sleeve Analytics canonical
No UNKNOWN sleeve bucket
No stale actions
Ask Aegis grounded
```

For 2026-05-30, verify:

```text
No stale actionable rows
Command Center fails closed
Performance/Sleeve Analytics blocked or degraded cleanly
No contradictory metrics
Ask Aegis grounded to 2026-05-30
```

## Tests

Tests must prove:

* semantic invariant detects `total_sleeves=0 AND attribution_coverage=100%`
* semantic invariant detects `NOT_CANONICAL + complete analytics`
* surface readiness reflects semantic invariant failure
* Performance UI shows unavailable/degraded state instead of contradictory cards
* Golden scenario 2026-05-29 passes canonical expectations
* Golden scenario 2026-05-30 passes fail-closed expectations

## Governance Rule

No operator-facing analytics surface may present internally contradictory states. If the semantic invariant gate blocks a surface, the UI must fail closed with a clear reason and collapsed evidence.

Semantic invariants do not change candidate generation, sleeve thresholds, research logic, broker policy, execution policy, or safety gates.
