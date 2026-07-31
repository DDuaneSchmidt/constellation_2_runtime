# Outcome Follow-Through Refresh Protocol 001

Objective: define a repeatable read-only protocol for rechecking whether open paper positions have moved into deterministic close state under existing rules.

Scope: existing AEGIS artifacts only. This protocol changes no rules, sleeves, candidates, governance, schemas, runtime truth logic, architecture, trade advice, position management, or capital allocation.

Primary baseline: Open Paper Position Outcome Follow-Through Review 001, using the validated 2026-06-03 evidence day.

## 1. Purpose

The purpose of this protocol is to make outcome follow-through repeatable without adding mechanisms or changing behavior. The refresh answers one question:

Did any previously open paper position move from `HOLD` / `NO_EXIT_RULE_TRIGGERED` into a deterministic close state under existing AEGIS rules?

The protocol exists because the latest follow-through review found 51 open paper positions across the five high-priority sleeves, 0 deterministic open-position close triggers, and 10 open positions in zero-sample sleeves that can become valuable first validation samples only after existing close conditions fire.

This protocol must not be used to recommend exits, change exits, create candidates, alter sleeve behavior, or infer readiness beyond verified artifacts.

## 2. Trigger For Running The Refresh

Run this refresh when one of these read-only evidence events occurs:

- A new paper position ledger is available for the target day.
- A new exit recommendations artifact is available for the target day.
- A new paper outcome auto-closure artifact is available for the target day.
- Outcome validation, validation samples, sleeve evidence certification, or generated-hypothesis outcome maturity monitor artifacts have been regenerated.
- A scheduled research review needs to know whether outcome maturity improved through existing deterministic closure.

Do not run this refresh because of market opinion, discretionary position views, candidate interest, sleeve preference, or capital review.

## 3. Inputs Required

Required artifacts:

- `aegis_paper_position_ledger_v1/<TARGET_DAY>/paper_position_ledger.v1.json`
- `aegis_exit_recommendations_v1/<TARGET_DAY>/exit_recommendations.v1.json`
- `aegis_paper_outcome_auto_closure_v1/<TARGET_DAY>/paper_outcome_auto_closure.v1.json`
- `aegis_outcome_registry_v1/<TARGET_DAY>/outcome_registry.v1.json`
- `aegis_validation_samples_v1/<TARGET_DAY>/validation_samples.v1.json`
- `aegis_sleeve_performance_truth_v1/<TARGET_DAY>/sleeve_performance_truth.v1.json`
- `aegis_sleeve_evidence_certification_v1/<TARGET_DAY>/sleeve_evidence_certification.v1.json`
- `aegis_generated_hypothesis_outcome_maturity_monitor_v1/<TARGET_DAY>/generated_hypothesis_outcome_maturity_monitor.v1.json`, if present

Reference reports:

- `research_journal/reports/open_paper_position_outcome_follow_through_review_001.md`
- `research_journal/reports/outcome_maturity_acceleration_review_001.md`
- `research_journal/reports/c2_trend_eq_outcome_review_001.md`

Validation should use the pinned review day requested by the journal unless the user explicitly requests another target day.

## 4. Positions/Sleeves To Watch

Primary watch population:

- All 51 open positions from Open Paper Position Outcome Follow-Through Review 001.
- All `C2_TREND_EQ_PRIMARY_V1` open trend positions, because trend had 41 of 51 open positions and all 12 closed samples on 2026-06-03.
- Zero-sample sleeves with open inventory:
  - `C2_CROSS_ASSET_TREND_V1`
  - `C2_MEAN_REVERSION_EQ_V1`
  - `C2_VOL_INCOME_DEFINED_RISK_V1`
  - `C2_OIL_SHOCK_REVERSAL_V1`
- `C2_OIL_SHOCK_REVERSAL_V1` USO, position `paper-position:candidate_contract_a3dd44d21952f8098131aa04`, as the first-sample watch item identified by the latest review.

High follow-through watchlist rows, if still present in current artifacts:

- CSCO
- APTV
- BURL
- CACC
- AMDL
- BMY

These symbols are watch targets only because the existing report ranked them by artifact fields such as absolute `return_pct` and holding-period days while preserving `HOLD`. They are not trade recommendations, exit recommendations, or position management advice.

## 5. Fields To Compare

Compare by `position_id` first, then by `candidate_id` only if needed for artifact linkage.

Position state fields:

- `position_id`
- `candidate_id`
- `sleeve_id`
- `symbol`
- `current_state`
- `current_status`
- `entry_time`
- `exit_time`
- `entry_price`
- `exit_price`
- `mark_price`
- `current_certified_mark`
- `mark_certification_status`
- `mark_freshness_status`

Exit recommendation fields:

- `exit_recommendation`
- `reason_codes`
- `exit_trigger`, where present
- `holding_period`
- `return_pct`
- `current_mark`
- `unrealized_pnl`
- `confidence`
- `record_exit_command`

Auto-closure and outcome fields:

- `auto_closure_state`
- `auto_closure_reason_codes`
- `lifecycle_state`
- `outcome_timestamp`
- `exit_mark`
- `exit_mark_certification_status`
- `exit_price_timestamp`
- `outcome_id`
- `outcome_state`
- `outcome_readiness_status`
- `remaining_blocker`

Validation fields:

- validation sample inclusion or exclusion state
- sample `position_id`
- sample `sleeve_id`
- sample outcome state
- `included_samples`
- `excluded_samples`
- `closed_or_resolved_outcomes`
- sleeve `closed_position_count`
- sleeve `sample_status`
- sleeve `evidence_status`

## 6. Classification Outcomes

Assign exactly one classification per watched position.

`STILL_HOLD`

Use when the position remains open and the current exit recommendation remains `HOLD` with no deterministic close trigger. This includes `NO_EXIT_RULE_TRIGGERED` and auto-closure states such as `AUTO_CLOSURE_NOT_ELIGIBLE` caused by `HOLD_RECOMMENDATION`.

`DETERMINISTIC_CLOSE_TRIGGERED`

Use when existing artifacts show a deterministic close state for a watched open position, such as an exit recommendation other than `HOLD`, a stop-loss trigger, a take-profit trigger, another existing close trigger, or auto-closure eligibility under existing rules. This classification is not an instruction to trade or manage a position.

`NEW_VALIDATION_SAMPLE`

Use only when the watched position has moved into the validation sample set as an included closed or resolved outcome under existing validation artifacts.

`BLOCKED_BY_DATA_OR_AUTHORITY`

Use when the refresh cannot determine closure or sample status because required mark, authority, source, lineage, certification, or artifact data is missing, stale, uncertified, mismatched, or blocked by verified runtime evidence.

`NOT_ASSESSABLE`

Use when the watched position cannot be linked across required artifacts by `position_id` or candidate lineage, or when a required artifact is absent and the blocker is not specific enough to classify as data or authority.

## 7. What Constitutes New Usable Validation Sample

A new usable validation sample requires all of the following:

- The paper position is closed or resolved in existing outcome artifacts.
- The close is produced or recognized by existing deterministic outcome and validation rules.
- The position appears in validation samples as included, not merely present as an excluded open sample.
- The sample has usable sleeve attribution.
- The sample has required candidate or position lineage.
- Required mark, exit price, and certification evidence are present.
- The sample does not depend on any rule change, manual override, new candidate, new sleeve behavior, or architecture change.

Mark-to-market movement alone is not a usable validation sample. `HOLD` with unrealized gain or loss is not a usable validation sample. A deterministic close trigger without validation inclusion is not yet a new usable validation sample.

## 8. What To Report

Each refresh report should include:

- Target day and artifact paths used.
- Total watched positions found, missing, and not assessable.
- Count by classification:
  - `STILL_HOLD`
  - `DETERMINISTIC_CLOSE_TRIGGERED`
  - `NEW_VALIDATION_SAMPLE`
  - `BLOCKED_BY_DATA_OR_AUTHORITY`
  - `NOT_ASSESSABLE`
- Changes from the baseline 51 open positions.
- List of any positions newly classified as `DETERMINISTIC_CLOSE_TRIGGERED`.
- List of any positions newly classified as `NEW_VALIDATION_SAMPLE`.
- Sleeve-level yield, especially whether closures remain concentrated in `C2_TREND_EQ_PRIMARY_V1` or begin distributing into zero-sample sleeves.
- Status of `C2_OIL_SHOCK_REVERSAL_V1` USO first-sample watch item.
- Status of high follow-through watchlist symbols if supported by current artifacts.
- Any data, authority, lineage, or certification blockers.
- Explicit statement that the report contains no trade advice, exit advice, candidate changes, sleeve changes, rule changes, or allocation decisions.

The recommended evidence action should remain narrow: regenerate and compare existing artifacts until open positions either remain `HOLD`, become deterministic close candidates under existing rules, or become included validation samples.

## 9. Validation Commands

Run the standard research-journal and pinned-day graph checks:

```bash
npm run aegis:research-journal-validate
pytest -q constellation_2/common/tests/test_aegis_research_journal_v0.py
env TARGET_DAY=2026-06-03 npm run aegis:verified-graph -- --strict
env TARGET_DAY=2026-06-03 npm run aegis:chatgpt:hydrate
```

If current-day audit fails because of unrelated 2026-06-04 runtime blockers, report it separately and do not broaden the protocol scope.
