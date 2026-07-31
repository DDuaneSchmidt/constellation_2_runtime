# Aegis Operator Action Model Spec v1

## Artifact Contract

Path:

`reports/aegis_operator_action_model_v1/<day_utc>/operator_action_model.v1.json`

Required top-level fields:

- `schema_id`: `aegis_operator_action_model`
- `schema_version`: `v1`
- `artifact_id`: `aegis_operator_action_model_v1`
- `day_utc`
- `generated_at`
- `summary`
- `capability_matrix`
- `capabilities_by_id`
- `source_artifacts`
- `source_hashes`
- `safety`

## Summary Contract

`summary` must include:

- `top_level_summary`
- `explanatory_sentence`
- `aegis_can_operate`
- `david_action_required`
- `blocked_capability_count`
- `waiting_capability_count`
- `disabled_by_policy_count`
- `active_capability_count`
- `complete_capability_count`

Current expected summary when trade advice is blocked but monitoring is active:

- `top_level_summary`: `Monitoring only. No David action required.` or equivalent if no real human task exists.
- `explanatory_sentence`: `Aegis is blocked from trade advice/manual capture, but not blocked from monitoring.`

## Capability Row Contract

Each row must include:

```json
{
  "capability_id": "TRADE_RECOMMENDATION",
  "status": "BLOCKED",
  "status_label": "Blocked",
  "reason_codes": ["TRADE_ADVICE_DISABLED_BY_RUNTIME_TRUTH"],
  "human_readable_reason": "Trade recommendations are blocked because runtime truth does not allow trade advice.",
  "source_artifacts": ["/abs/path/runtime_truth_kernel.v1.json"],
  "source_hashes": {"/abs/path/runtime_truth_kernel.v1.json": "..."},
  "next_expected_event": "Refresh runtime truth dependencies before advisory workflows can be considered.",
  "david_action_required": false
}
```

## Deterministic Status Rules

- `CANDIDATE_GENERATION` is `COMPLETE` when current-day candidate diagnostics/candidate projection proves the run completed, even when zero candidates were created.
- `PAPER_MONITORING` is `ACTIVE` when open paper positions exist and the paper position ledger or outcome registry is current.
- `OUTCOME_REALIZATION` is `WAITING` when open positions exist but no positions are closed or outcome-resolved.
- `HYPOTHESIS_VALIDATION` is `WAITING` when statistical sufficiency reports zero usable samples or underpowered hypotheses.
- `TRADE_RECOMMENDATION` is `BLOCKED` whenever `trade_advice_allowed=false`. It cannot be `READY` unless runtime truth explicitly allows trade advice.
- `MANUAL_TRADE_CAPTURE` is `NOT_APPLICABLE` when no eligible manual trade packet exists. It is `BLOCKED` when an eligible manual packet exists but `manual_trade_capture_allowed=false`. It cannot be `READY` without an eligible packet and runtime permission.
- `BROKER_EXECUTION` is `DISABLED_BY_POLICY` unless runtime truth explicitly changes policy. Aegis objectives keep broker execution out of scope.
- `DAVID_ACTION` is `ACTIVE` only when command-center queue evidence reports a real operator action. Policy blocks, waiting states, and disabled broker execution are not David actions.

## Source Hashing

The builder must hash source files directly. Missing source paths may appear in `source_artifacts` only when the row status explains missing evidence.

## Determinism

The artifact must be stable for stable inputs. `generated_at` is derived from source artifact timestamps when possible, not from local wall-clock time.

## Audit Integration

Add commands:

- `npm run aegis:operator-action-model`
- `npm run aegis:operator-action-model-self-check`

Both must run inside `npm run aegis:audit` after runtime/control/canonical/candidate/outcome artifacts exist and before verified graph strict validation.
