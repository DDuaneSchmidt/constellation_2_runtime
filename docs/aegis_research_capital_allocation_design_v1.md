# Aegis Research Capital Allocation Design v1

## Design

The layer consumes existing research portfolio, thesis/hypothesis registries, hypothesis states, sleeve analytics, candidate diagnostics, outcome registry, hypothesis outcome ledger, validation samples, statistical sufficiency, evidence lineage integrity, scorecards, and condition audits when present. It emits read-only research-priority artifacts.

## Modules

- `research_program_registry_v1`: groups theses/hypotheses/sleeves into research programs.
- `research_capital_scoring_v1`: deterministic component scoring.
- `research_allocation_decisions_v1`: recommendation decisions and unit deltas.
- `research_allocation_event_log_v1`: daily immutable event rows.
- `research_capital_allocation_self_check_v1`: audit gate.

## Operator Visibility

Extend the existing Research Portfolio panel with Research Capital Allocation rows. The browser reads canonical artifacts; it does not score or infer allocation.

## Failure Modes

Missing program mappings, duplicate program IDs, missing score components, recommendations without reason codes, missing source artifacts, outcome proof overclaim, retired/disproven increase, data-blocked increase, non-deterministic rerun, black-box AI in final recommendation.

## Test Plan

Tests cover registry generation, hypothesis/sleeve mapping, score components, recommendation behavior, insufficient outcome handling, reason-code requirements, no increase for retired/disproven programs, deterministic rerun stability, self-check success/failure, and UI data shape.
