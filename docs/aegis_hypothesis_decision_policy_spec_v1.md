# Aegis Hypothesis Decision Policy Spec v1

## Artifact Contract

Artifact id: `aegis_hypothesis_decision_policy_v1`

Canonical path:

`reports/aegis_hypothesis_decision_policy_v1/{day}/hypothesis_decision_policy.v1.json`

## Top-Level Fields

- `schema_id`: `aegis_hypothesis_decision_policy`
- `schema_version`: `v1`
- `artifact_id`: `aegis_hypothesis_decision_policy_v1`
- `day_utc`
- `scoring_version`
- `decision_policy_version`
- `input_artifact_hashes`
- `input_generated_at_utc`
- `computed_at_utc`
- `deterministic_rerun_id`
- `source_artifact_paths`
- `decisions`
- `summary`
- `content_hash`
- safety flags

## Decision Row

Each row must include:

- `hypothesis_id`
- `name`
- `recommendation`
- `quality_status`
- `active_hard_gates`
- `reason_codes`
- `confidence_level`
- `source_artifact_paths`
- `input_artifact_hashes`
- `requires_david_review`

## Policy Precedence

1. Blocking hard gates.
2. Duplicate or invalid hypothesis gates.
3. Data and implementation deficiencies.
4. Underpowered but producing evidence.
5. Strong sample production and improving validation.
6. Statistical sufficiency and validation pass.

The policy must fail closed for unknown or missing quality artifacts.
## Outcome Performance Metrics Integration

The decision policy consumes outcome-performance-informed quality rows and carries the metric snapshot forward in each decision row.

Policy rules:

- Underpowered hypotheses producing samples resolve to `CONTINUE` or `WATCH` unless poor early outcomes justify `DECREASE_ATTENTION`.
- Underpowered poor outcomes do not default to `RETIRE_RECOMMENDED`.
- Underpowered positive outcomes do not trigger `READY_FOR_CAPITAL_REVIEW`.
- Sufficient poor outcomes may trigger `RETIRE_RECOMMENDED` or `REDESIGN` when hard gates and explicit policy basis allow.
- Sufficient strong outcomes may allow `READY_FOR_CAPITAL_REVIEW` only when all hard gates pass.
- Candidate-heavy sample-poor hypotheses remain underpowered and are not rewarded from candidate flow alone.
