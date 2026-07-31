# Aegis Research Quality Engine Spec v1

## Artifact Contract

Artifact id: `aegis_research_quality_engine_v1`

Canonical path:

`reports/aegis_research_quality_engine_v1/{day}/research_quality_engine.v1.json`

## Top-Level Fields

- `schema_id`: `aegis_research_quality_engine`
- `schema_version`: `v1`
- `artifact_id`: `aegis_research_quality_engine_v1`
- `day_utc`
- `scoring_version`
- `input_artifact_hashes`
- `input_generated_at_utc`
- `computed_at_utc`
- `deterministic_rerun_id`
- `source_artifact_paths`
- `hypotheses`
- `summary`
- `content_hash`
- safety flags set to true for no broker execution, no trade advice, no live trading, and no real capital

## Hypothesis Row

Each row must include:

- `hypothesis_id`
- `name`
- `thesis_id`
- `quality_status`
- `grades`
- `hard_gates`
- `active_hard_gate_codes`
- `reason_codes`
- `source_artifact_paths`
- `source_artifact_hashes`
- `sample_counts`
- `confidence_level`

## Grade Object

Each grade object must include:

- `grade`
- `reason_codes`
- `source_artifact_paths`
- `source_artifact_hashes`
- `sample_counts`
- `confidence_level`
- `scoring_version`

## Quality Status

The quality status is deterministic:

- `BLOCKED` when any blocking hard gate is active.
- `REJECT` when a duplicate hard gate is active.
- `UNDERPOWERED` when included samples are below minimum but evidence is producing.
- `PASS` when all required quality dimensions pass and no hard gate is active.
- `WATCH` otherwise.

## Safety

The artifact is research scoring only. It must not contain order instructions, broker commands, live-trading enablement, trade sizing, or real-capital allocation.
## Outcome Performance Metrics Integration

The quality engine directly consumes `aegis_outcome_performance_metrics_v1`. Each hypothesis row includes an `outcome_performance_metrics` snapshot and sample counts for closed outcomes, included validation samples, excluded validation samples, and distance to sufficiency.

Validation evidence follows sufficiency-preserving rules:

- No included validation samples keeps validation evidence `UNDERPOWERED` or `NOT_APPLICABLE`.
- Included samples below the minimum may add early outcome direction, but confidence remains `LOW` and quality status remains `UNDERPOWERED`.
- Poor underpowered outcomes may add watch/decrease/redesign-review reason codes.
- Poor underpowered outcomes do not automatically retire a hypothesis.
- Positive underpowered outcomes do not make a hypothesis ready for capital review.
- Sufficient poor outcome performance prevents a quality `PASS` and may produce `WATCH` or redesign-oriented evidence.
- Sufficient strong outcome performance can support `PASS` only when validation, robustness, sufficiency, and hard gates all allow it.
