# Aegis Research Validation Engine V1 Rework Specification

## Required Artifact Extensions

The existing artifact families remain:

```text
aegis_research_hypothesis_registry_v1
aegis_research_validation_protocol_v1
aegis_research_validation_run_v1
aegis_research_validation_result_v1
aegis_research_promotion_gate_v1
aegis_research_outcome_feedback_v1
```

The rework adds stricter fields to the existing V1 artifacts and introduces enforcement reports. This is a V1 hardening pass, not a new product family.

## Promotion Gate Enforcement Contract

Any candidate-generation path that consumes hypothesis-derived research must first resolve:

```json
{
  "hypothesis_id": "...",
  "hypothesis_version": "v1",
  "latest_result_id": "...",
  "validation_status": "SUPPORTED",
  "eligible_for_candidate_review": true,
  "promotion_gate_status": "ELIGIBLE_FOR_CANDIDATE_REVIEW",
  "candidate_review_only": true,
  "tradeable": false,
  "trade_advice_allowed": false
}
```

If no matching gate row exists, the candidate input must be rejected with:

```text
REJECTED_RESEARCH_VALIDATION_MISSING
```

If the latest gate row is not eligible, the candidate input must be rejected with:

```text
REJECTED_RESEARCH_NOT_SUPPORTED
```

If the gate row is stale, wrong-day, wrong-version, or mismatched to the candidate's hypothesis lineage, the candidate input must be rejected with:

```text
REJECTED_RESEARCH_GATE_MISMATCH
```

## Candidate Boundary Fields

Any candidate or candidate-intent record derived from research must include:

```json
{
  "research_validation_required": true,
  "hypothesis_id": "...",
  "hypothesis_version": "v1",
  "research_validation_result_id": "...",
  "research_promotion_gate_status": "...",
  "research_promotion_gate_hash": "...",
  "research_validation_enforcement_status": "PASS|REJECTED_RESEARCH_VALIDATION_MISSING|REJECTED_RESEARCH_NOT_SUPPORTED|REJECTED_RESEARCH_GATE_MISMATCH|NOT_RESEARCH_DERIVED"
}
```

Non-research-derived candidates must explicitly state:

```text
NOT_RESEARCH_DERIVED
```

They may not silently omit the research validation fields if they carry hypothesis lineage.

## Protocol-Specific Evaluator Contract

Each protocol must bind to exactly one evaluator:

```json
{
  "protocol_id": "MEAN_REVERSION_FORWARD_RETURN_V1",
  "protocol_version": "v1",
  "evaluator_id": "mean_reversion_forward_return_evaluator_v1",
  "evaluator_version": "v1",
  "required_metrics": [],
  "required_source_types": [],
  "required_bias_checks": [],
  "support_thresholds": {},
  "disproof_thresholds": {}
}
```

No generic evaluator may emit `SUPPORTED` or `DISPROVEN`.

Generic/shared helpers may compute common statistics, but the final validation status must be emitted by the protocol-specific evaluator.

## Protocol-Specific Metric Requirements

### EVENT_WINDOW_RETURN_V1

Required metrics:

- `sample_count`,
- `hit_rate`,
- `average_forward_return`,
- `benchmark_relative_return`,
- `event_window_return`,
- `adverse_excursion`.

Required source types:

- event calendar,
- event timestamp,
- event-window close,
- benchmark close.

Required checks:

- event timestamp exists,
- event window starts after event timestamp,
- benchmark-relative return exists,
- event rows are deduplicated,
- sample universe is predeclared.

`SUPPORTED` requires all support thresholds to pass.

`DISPROVEN` requires disproof thresholds to pass or direct contradiction evidence.

### MEAN_REVERSION_FORWARD_RETURN_V1

Required metrics:

- `sample_count`,
- `hit_rate`,
- `average_forward_return`,
- `median_forward_return`,
- `drawdown_or_adverse_excursion`,
- `trigger_return`,
- `forward_return_1d`.

Required source types:

- governed trigger event,
- event close,
- forward close,
- symbol universe declaration.

Required checks:

- forward close day is after event day,
- trigger event was known before forward close,
- excluded samples include explicit reasons,
- duplicate trigger events are collapsed or excluded,
- symbol belongs to the hypothesis universe.

### POST_SIGNAL_SURVIVAL_V1

Required metrics:

- `sample_count`,
- `hit_rate`,
- `average_forward_return`,
- `drawdown_or_adverse_excursion`,
- `survival_horizon`,
- `post_signal_return`.

Required source types:

- signal timestamp,
- signal evidence,
- post-signal marks,
- survival horizon definition.

Required checks:

- signal timestamp precedes all post-signal observations,
- signal evidence belongs to hypothesis,
- duplicate signals are excluded,
- survival horizon is deterministic and versioned.

## Artifact Binding Status

Validation run rows must include:

```json
{
  "artifact_binding_status": "PASS|FAIL|PARTIAL",
  "artifact_binding_failures": [],
  "hypothesis_binding": {
    "hypothesis_id": "...",
    "source_hypothesis_id": "...",
    "binding_status": "MATCH|ALLOWED_SHARED_SOURCE|MISMATCH|MISSING",
    "reason": "..."
  }
}
```

Rules:

- A hypothesis-specific artifact must match `hypothesis_id`.
- A shared artifact must declare `allowed_shared_source=true` and identify the binding fields used for filtering.
- A mismatched artifact makes the result `NOT_READY`, unless direct disproof evidence is proven.

## Reproducibility Fields

Validation run rows must include:

```json
{
  "code_version": "...",
  "code_commit": "...",
  "evaluator_id": "...",
  "evaluator_version": "v1",
  "evaluator_hash": "...",
  "protocol_hash": "...",
  "input_context_hash": "...",
  "source_artifact_hashes": {},
  "configuration_hash": "...",
  "deterministic_seed": null
}
```

If `code_commit` is unavailable, the run must include an explicit `code_reference_status`:

```text
UNAVAILABLE_BLOCKS_VALIDATION
```

and the result may not be `SUPPORTED`.

## Bias Control Status

Validation result rows must include:

```json
{
  "bias_control_status": "PASS|FAIL|PARTIAL",
  "bias_controls": [
    {
      "control_id": "LOOKAHEAD_GUARDRAIL",
      "status": "PASS|FAIL|PARTIAL",
      "evidence": "...",
      "blocking": true
    }
  ]
}
```

Minimum required controls:

- `LOOKAHEAD_GUARDRAIL`,
- `SURVIVORSHIP_GUARDRAIL`,
- `SELECTION_BIAS_GUARDRAIL`,
- `DUPLICATE_EVENT_GUARDRAIL`,
- `STALE_DATA_GUARDRAIL`.

Any blocking control with status `FAIL` or `PARTIAL` prevents `SUPPORTED`.

## Sample-Size Policy

Protocol rows must include:

```json
{
  "sample_size_policy": {
    "minimum_sample_size": 20,
    "confidence_target": "...",
    "effect_size_assumption": "...",
    "minimum_positive_events": 0,
    "minimum_distinct_dates": 0,
    "minimum_distinct_symbols": 0,
    "policy_rationale": "..."
  }
}
```

Result rows must include:

```json
{
  "sample_size_status": "PASS|UNDER_SAMPLED|FAIL",
  "sample_size_reason": "...",
  "sample_count": 0,
  "required_sample_count": 20,
  "distinct_symbol_count": 0,
  "distinct_date_count": 0
}
```

`SUPPORTED` requires `sample_size_status=PASS`.

## Result Status Rules

The result evaluator must apply this order:

1. Missing protocol or evaluator -> `NOT_READY`.
2. Failed artifact binding -> `NOT_READY`.
3. Failed required metric availability -> `NOT_READY`.
4. Failed blocking bias control -> `NOT_READY` or `DISPROVEN` only if direct contradiction evidence exists.
5. Under-sampled evidence -> `UNDER_SAMPLED`.
6. Protocol-specific disproof thresholds pass -> `DISPROVEN`.
7. Protocol-specific support thresholds pass -> `SUPPORTED`.
8. Otherwise -> `INCONCLUSIVE`.

## Self-Check Additions

`npm run aegis:research-validation-engine-self-check` must fail if:

- any research-derived candidate lacks promotion-gate evidence,
- any non-`SUPPORTED` hypothesis is eligible for candidate review,
- any `SUPPORTED` result lacks required protocol metrics,
- any `SUPPORTED` result lacks artifact binding `PASS`,
- any `SUPPORTED` result lacks bias control `PASS`,
- any `SUPPORTED` result lacks sample-size `PASS`,
- any result lacks immutable reproducibility fields,
- any protocol uses the generic evaluator to emit final status,
- any protocol-required metric is listed but not evaluated,
- any source artifact has empty hash or missing path,
- any safety gate is enabled.

## Hostile Audit Success Criteria

The rework passes hostile audit only if:

- no remaining P0 issues exist,
- candidate-generation bypass is closed by tests and artifact evidence,
- a malformed sample artifact cannot produce `SUPPORTED`,
- missing benchmark-relative return blocks `EVENT_WINDOW_RETURN_V1`,
- wrong-hypothesis samples are rejected,
- stale or future-dated samples are rejected,
- every `SUPPORTED` result is reproducible from immutable references,
- UI states why zero or more hypotheses are eligible,
- hostile audit score is at least `85/100`.
