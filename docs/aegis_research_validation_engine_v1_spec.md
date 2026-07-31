# Aegis Research Validation Engine V1 Specification

## Artifact Families

```text
aegis_research_hypothesis_registry_v1
aegis_research_validation_protocol_v1
aegis_research_validation_run_v1
aegis_research_validation_result_v1
aegis_research_promotion_gate_v1
aegis_research_outcome_feedback_v1
```

## Paths

```text
truth/reports/aegis_research_hypothesis_registry_v1/<day>/research_hypothesis_registry.v1.json
truth/reports/aegis_research_validation_protocol_v1/<day>/research_validation_protocol.v1.json
truth/reports/aegis_research_validation_run_v1/<day>/research_validation_run.v1.json
truth/reports/aegis_research_validation_result_v1/<day>/research_validation_result.v1.json
truth/reports/aegis_research_promotion_gate_v1/<day>/research_promotion_gate.v1.json
truth/reports/aegis_research_outcome_feedback_v1/<day>/research_outcome_feedback.v1.json
```

## ID Rules

- hypothesis_id: stable research identifier supplied by Research Lab or generated as `rh-<hash>`.
- hypothesis_version: semantic version string, starting with `v1`.
- protocol_id: deterministic protocol identifier.
- protocol_version: semantic version string, starting with `v1`.
- run_id: `rvr-<12-char-hash>` derived from day, hypothesis, protocol, versions, and source hashes.

## aegis_research_hypothesis_registry_v1

Required row fields:

```json
{
  "hypothesis_id": "...",
  "hypothesis_version": "v1",
  "claim": "...",
  "hypothesis_type": "EVENT_WINDOW|MEAN_REVERSION|POST_SIGNAL_SURVIVAL|UNKNOWN",
  "asset_universe": [],
  "expected_behavior": "...",
  "created_by": "SYSTEM|OPERATOR|IMPORTED|UNKNOWN",
  "created_at": "...",
  "status": "CAPTURED|ACTIVE|READY_FOR_VALIDATION|VALIDATION_RUNNING|VALIDATED_SUPPORTED|VALIDATED_DISPROVEN|RETIRED",
  "source": "...",
  "linked_research_items": []
}
```

## aegis_research_validation_protocol_v1

Required protocol fields:

```json
{
  "protocol_id": "MEAN_REVERSION_FORWARD_RETURN_V1",
  "protocol_version": "v1",
  "required_data": [],
  "minimum_sample_size": 20,
  "metrics": [],
  "support_thresholds": {},
  "disproof_thresholds": {},
  "exclusion_rules": [],
  "lookahead_guardrails": [],
  "output_fields": []
}
```

V1 protocols:

- EVENT_WINDOW_RETURN_V1
- MEAN_REVERSION_FORWARD_RETURN_V1
- POST_SIGNAL_SURVIVAL_V1

## aegis_research_validation_run_v1

Required run fields:

```json
{
  "run_id": "...",
  "hypothesis_id": "...",
  "hypothesis_version": "v1",
  "protocol_id": "...",
  "protocol_version": "v1",
  "data_window": {},
  "symbols_tested": [],
  "events_tested": 0,
  "excluded_observations": [],
  "source_artifacts": [],
  "code_version": "...",
  "run_timestamp": "..."
}
```

## aegis_research_validation_result_v1

Required result fields:

```json
{
  "result_id": "...",
  "run_id": "...",
  "hypothesis_id": "...",
  "hypothesis_version": "v1",
  "protocol_id": "...",
  "protocol_version": "v1",
  "validation_status": "NOT_READY|UNDER_SAMPLED|INCONCLUSIVE|SUPPORTED|DISPROVEN",
  "sample_count": 0,
  "hit_rate": null,
  "average_forward_return": null,
  "median_forward_return": null,
  "benchmark_relative_return": null,
  "drawdown_or_adverse_excursion": null,
  "limitations": [],
  "plain_english_summary": "...",
  "source_artifacts": []
}
```

AI/freeform summary fields cannot change `validation_status`.

## aegis_research_promotion_gate_v1

Required row fields:

```json
{
  "hypothesis_id": "...",
  "hypothesis_version": "v1",
  "latest_result_id": "...",
  "validation_status": "...",
  "eligible_for_candidate_review": false,
  "promotion_gate_status": "ELIGIBLE_FOR_CANDIDATE_REVIEW|BLOCKED_NOT_SUPPORTED",
  "blocked_reason": "...",
  "tradeable": false,
  "trade_advice_allowed": false
}
```

Rules:

- SUPPORTED -> eligible_for_candidate_review=true.
- NOT_READY, UNDER_SAMPLED, INCONCLUSIVE, DISPROVEN -> eligible_for_candidate_review=false.
- No status permits trade execution or trade advice.

## aegis_research_outcome_feedback_v1

Required fields:

```json
{
  "hypothesis_id": "...",
  "candidate_id": "...",
  "position_id": "...",
  "realized_outcome": "...",
  "out_of_sample_result": "...",
  "degradation_flag": false,
  "retirement_recommendation": "..."
}
```

Outcome feedback is append-only with respect to validation. It must not rewrite original validation results.

## Status Transitions

Validation state machine:

```text
NOT_READY -> UNDER_SAMPLED -> INCONCLUSIVE | SUPPORTED | DISPROVEN
NOT_READY -> DISPROVEN when required evidence directly contradicts the claim
SUPPORTED -> outcome feedback may later recommend retirement, but original result remains immutable
```

## Closure and Retirement Rules

A hypothesis can be retired when:

- latest validation is DISPROVEN, or
- out-of-sample outcome feedback indicates degradation, or
- operator/research policy explicitly retires it.

Retirement must reference source evidence and must not delete historical results.
