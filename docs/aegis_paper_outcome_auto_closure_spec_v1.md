# Aegis Paper Outcome Auto-Closure Spec v1

Date: 2026-06-01

## Artifact

`aegis_paper_outcome_auto_closure_v1`

Output path:

`reports/aegis_paper_outcome_auto_closure_v1/<day_utc>/paper_outcome_auto_closure.v1.json`

## Inputs

- `aegis_paper_position_ledger_v1`
- `aegis_exit_recommendations_v1`
- `aegis_runtime_truth_kernel_v1`
- exit policy registry embedded in exit recommendations
- certified paper ledger mark source metadata

## Output Fields

Top-level fields:

- `schema_id`
- `schema_version`
- `artifact_id`
- `day_utc`
- `generated_at`
- `rows`
- `manual_review_queue`
- `summary`
- `source_artifacts`
- `source_hashes`
- safety flags

Each row contains:

- `position_id`
- `candidate_id`
- `symbol`
- `sleeve_id`
- `hypothesis_id`
- `thesis_id`
- `auto_closure_state`
- `auto_closure_reason_codes`
- `exit_recommendation`
- `exit_trigger`
- `trigger_timestamp`
- `outcome_timestamp`
- `entry_mark`
- `entry_price_source_artifact`
- `entry_price_source_hash`
- `exit_mark`
- `exit_price_source_artifact`
- `exit_price_source_hash`
- `exit_price_timestamp`
- `realized_return`
- `deterministic_return_formula_version`
- `rerun_stability_key`
- safety flags

Manual review queue rows contain:

- `symbol`
- `sleeve`
- `hypothesis`
- `paper_entry_price`
- `paper_exit_price`
- `paper_return`
- `exit_trigger`
- `exit_timestamp`
- `plain_english_reason`
- `not_trade_advice_statement`

## State Rules

`AUTO_CLOSED_PAPER_OUTCOME` means the paper-research outcome is closed for validation evidence only.

`AUTO_CLOSURE_BLOCKED` means the position had a non-HOLD exit recommendation but required evidence was missing, uncertified, stale, policy-mismatched, or unsafe.

`AUTO_CLOSURE_NOT_ELIGIBLE` means the position did not qualify for auto-closure, typically because the recommendation was HOLD or no exit evaluation matched the open position.

## Outcome Registry Contract

The outcome registry may treat an `AUTO_CLOSED_PAPER_OUTCOME` row as closed evidence when the row provides evidence-bound entry mark, certified exit mark, trigger timestamp, outcome timestamp, and deterministic realized return.

Outcome rows created from auto-closure must remain distinguishable from operator-entered paper exits and broker/live exits.

## Validation Sample Contract

Closed auto-closure outcomes are eligible for validation samples under the same validation rules as other resolved outcomes. Open HOLD positions remain excluded as open positions.

## Safety Contract

The artifact must always emit:

- `paper_only: true`
- `trade_advice_allowed: false`
- `broker_execution_allowed: false`
- `broker_submit_transmit_allowed: false`
- `live_trading_allowed: false`
- `autonomous_execution_allowed: false`
- `automatic_real_world_position_management_allowed: false`

No auto-closure row can be interpreted as a broker instruction, trade recommendation, live order, or autonomous account action.
