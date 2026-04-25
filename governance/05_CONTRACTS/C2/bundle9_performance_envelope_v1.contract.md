# bundle9_performance_envelope_v1

This contract governs the minimum Bundle 9 performance envelope for first-wave bounded obligation pipelines.

Measurement law:
- only phase-boundary timings are legal
- measured phases are exactly:
  - `resolve_inputs`
  - `evaluate`
  - `persist_outputs`
  - `project_emit`
  - `measure_phase_boundaries`
- results MUST be reported as deterministic milliseconds

Authoritative fixture scale:
- `fixture_small`
  - deterministic local fixture truth
  - no live network access
  - no broad filesystem discovery beyond the pipeline mode contract

Budget profiles:

## `contract_default`
- `paper_day_orchestrator_v2:normal`
  - hard total: `60000ms`
  - soft total: `5000ms`
- `paper_day_orchestrator_v2:exact_ref_replay`
  - hard total: `1500ms`
  - soft total: `500ms`
- `paper_day_orchestrator_v2:bounded_recompute`
  - hard total: `2500ms`
  - soft total: `1000ms`
- `c2_ops_cockpit_status_v2_collector_v1:normal`
  - hard total: `2000ms`
  - soft total: `800ms`

## `strict_validation`
- validation-only profile used to prove fail-closed budget enforcement in Bundle 9 tests and CLI smoke checks
- all hard totals are `1ms`
- all soft totals are `1ms`

Enforcement law:
- a hard-threshold breach MUST block the pipeline result
- a soft-threshold breach MUST preserve the pipeline result but emit an explicit warning
- budget comparison MUST be reported in the deterministic proof output

Replay budgeting:
- `exact_ref_replay` is budgeted separately from `normal`
- `bounded_recompute` is budgeted separately from `normal`
- `forensic_replay` is not budgeted for first-wave targets unless the target explicitly ratifies that mode
