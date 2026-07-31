# Aegis Research Quality Engine Requirements v1

## Purpose

The Research Quality Engine scores evidence quality for active or generated hypotheses. It improves research focus, edge discovery, edge validation, and retirement discipline without creating trade advice, broker execution, live trading, or real-capital allocation.

## Scope

- Compute deterministic quality grades for each hypothesis.
- Apply hard gates before downstream policy decisions.
- Emit auditable backend artifacts for UI rendering.
- Preserve the invariant that consumers query verified truth and do not invent truth.

## Required Inputs

- Hypothesis registry.
- Research portfolio.
- Candidate, paper position, validation sample, outcome, and statistical sufficiency artifacts.
- Existing allocation artifacts when present.

## Required Output

Artifact: `aegis_research_quality_engine_v1`.

For each hypothesis, the engine must grade:

- `economic_rationale`
- `data_quality`
- `implementation_completeness`
- `sample_production`
- `validation_evidence`
- `robustness`

Allowed grade values: `PASS`, `WATCH`, `FAIL`, `UNDERPOWERED`, `BLOCKED`, `NOT_APPLICABLE`.

Each grade must include `grade`, `reason_codes`, `source_artifact_paths`, `source_artifact_hashes`, `sample_counts`, `confidence_level`, and `scoring_version`.

## Hard Gates

The engine must compute hard gates that override softer scoring:

- `NO_DATA_SOURCE` -> `BLOCKED`
- `NO_EXIT_POLICY` -> `BLOCKED`
- `NO_PAPER_PATH` -> `BLOCKED`
- `DUPLICATE_HYPOTHESIS` -> `REJECT`
- `INCLUDED_SAMPLES_BELOW_MINIMUM` -> `UNDERPOWERED`
- `NO_CANDIDATE_FLOW` -> `WATCH` or `BLOCKED` depending on age
- `NO_OBSERVATION_FLOW` -> `WATCH` or `BLOCKED` depending on age
- `STALE_OR_MISSING_EVIDENCE` -> `BLOCKED`
- `NONDETERMINISTIC_OUTCOME_RETURNS` -> `BLOCKED`

No hypothesis may receive `READY_FOR_CAPITAL_REVIEW` downstream while any hard gate is active.

## Auditability

Every artifact must include scoring version, input artifact hashes, input generation time, computed time, deterministic rerun id, source artifact paths, reason codes, and explicit safety flags:

- `no_broker_execution: true`
- `no_trade_advice: true`
- `no_live_trading: true`
- `no_real_capital: true`

## Determinism

For the same `TARGET_DAY` and identical inputs, the engine must produce the same grades, hard gates, and content hash, excluding generated timestamp fields from the hash.
