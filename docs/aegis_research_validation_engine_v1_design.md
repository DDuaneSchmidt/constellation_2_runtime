# Aegis Research Validation Engine V1 Design

## Architecture

```text
Research hypotheses
  -> Hypothesis Registry
  -> Validation Protocol Registry
  -> Validation Run Builder
  -> Deterministic Result Evaluator
  -> Promotion Gate
  -> Outcome Feedback
  -> Research UI summary
```

## Storage Locations

All V1 outputs are day-scoped truth artifacts under:

```text
truth/reports/aegis_research_*_v1/<day>/...
```

The implementation lives in:

```text
ops/aegis/research_lab/research_validation_engine_v1.py
ops/tools/build_aegis_research_validation_engine_v1.py
ops/tools/run_aegis_research_validation_engine_self_check_v1.py
```

## Artifact Generation Flow

1. Build hypothesis registry from current Research Lab/doctor artifacts.
2. Build deterministic protocol registry.
3. Bind each hypothesis to a protocol.
4. Build reproducible validation runs from available source artifacts.
5. Evaluate deterministic metrics into validation results.
6. Evaluate promotion gate eligibility.
7. Emit outcome feedback shell without rewriting historical validation.

## Protocol Registry Design

Protocol definitions are data records, not code-only behavior. Each protocol declares required data, minimum sample size, metrics, thresholds, exclusion rules, and lookahead guardrails.

## Validation Run Flow

A validation run captures exact hypothesis and protocol versions, source artifact paths and hashes, data window, symbols/events tested, excluded observations, and code version.

## Result Generation Flow

Results are derived from deterministic metrics. Freeform summaries are generated after status selection and cannot override status.

## Relationship to Research UI

Research UI may show validation status, protocol used, sample count, promotion eligibility, and plain-English summary. It must not show trading recommendations or candidate approval buttons.

## Relationship to Candidates

The promotion gate emits only candidate-review eligibility. It does not alter candidate generation behavior in V1. Candidate systems must treat non-SUPPORTED hypotheses as ineligible for hypothesis-derived candidate consideration.

## Relationship to Performance Attribution

Outcome feedback creates the future link needed for hypothesis-level attribution, but it does not change certified P&L or portfolio performance artifacts.

## Preventing Unvalidated Promotion

The promotion gate is fail-closed:

- only SUPPORTED results are eligible for candidate review,
- every other status blocks promotion,
- supported status still does not mean tradeable,
- all safety gates remain disabled.

## Avoiding AI-Certification Risk

AI is not used to compute validation status. The engine records `ai_certification_allowed=false` and treats summaries as explanatory only.
