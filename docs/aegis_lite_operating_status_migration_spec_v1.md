# Aegis Lite Operating Status Migration Spec v1

## Artifact Mapping

### Lite Artifact: aegis_lite_operating_status

Path:
`reports/aegis_lite_operating_status_v1/<day>/aegis_lite_operating_status.v1.json`

Producer:
`python3 ops/tools/build_aegis_operator_status_v1.py --truth_root <truth_root> --day_utc <day>`

Primary fields used:
- `schema_id`
- `artifact_id`
- `day_utc`
- `generated_at_utc`
- `readiness_classification`
- `current_blockers`
- `lite_eod_latest_report_path`
- `lite_eod_latest_queue_path`
- `broker_mode`
- `manual_execution_only`
- `broker_submit_required`
- `release_repo_match_status`
- `legacy_paper_timers_status`

Runtime capabilities supported:
- `DATA_READY`
- downstream `TRADE_ADVICE_ALLOWED`, still disabled by policy
- downstream `MANUAL_TRADE_CAPTURE_ALLOWED`, still blocked/disabled

### Lite Artifact: aegis_lite_eod_report

Path:
`reports/aegis_lite_eod_report_v1/<day>/**/aegis_lite_eod_report.v1.json`

Producer:
`python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --truth_root <truth_root> --day_utc <day> --manual-only --allow-not-ready-exit-zero`

Primary fields used:
- `schema_id`
- `artifact_id`
- `day_utc`
- `generated_at_utc`
- `run_id`
- `report_status`
- `manual_execution_status`
- `readiness_classification`
- `selected_trade_candidates`
- `do_not_trade_blockers`
- `warnings`
- `eod_outcome_status`
- `eod_input_contract`
- `source_artifact_lineage`

Runtime capabilities supported:
- `DATA_READY`
- EOD advisory-mode readiness
- downstream safety claims that advisory/manual capture remain blocked unless all evidence passes

## Strategic Replacement Artifacts

### aegis_strategic_operating_status_v1

Path:
`reports/aegis_strategic_operating_status_v1/<day>/strategic_operating_status.v1.json`

Schema fields:
- `schema_id`
- `schema_version`
- `artifact_id`
- `day_utc`
- `generated_at_utc`
- `strategic_system_of_record`
- `legacy_compatibility_layer`
- `operating_status`
- `run_summary`
- `candidate_generation_result`
- `paper_monitoring_result`
- `outcome_validation_state`
- `research_allocation_state`
- `operator_action_state`
- `policy_state`
- `source_artifacts`
- `source_artifact_hashes`
- `canonical_json_hash`

### aegis_research_eod_summary_v1

Path:
`reports/aegis_research_eod_summary_v1/<day>/research_eod_summary.v1.json`

Schema fields:
- `schema_id`
- `schema_version`
- `artifact_id`
- `day_utc`
- `generated_at_utc`
- `strategic_system_of_record`
- `eod_status`
- `candidate_summary`
- `research_summary`
- `outcome_summary`
- `validation_summary`
- `paper_monitoring_summary`
- `operator_action_summary`
- `policy_state`
- `source_artifacts`
- `source_artifact_hashes`
- `canonical_json_hash`

## Equivalence Artifact

Path:
`reports/aegis_lite_migration_equivalence_v1/<day>/lite_migration_equivalence.v1.json`

Each comparison row contains:
- `semantic_id`
- `lite_source`
- `strategic_source`
- `equivalent`
- `mismatch_reason`
- `blocking_capability`
- `migration_safe`

Overall migration is safe only when all blocking rows have `migration_safe=true`.

## Runtime Truth Migration Rule

If equivalence passes, `DATA_READY` may replace:
- `aegis_lite_operating_status` with `aegis_strategic_operating_status`
- `aegis_lite_eod_report` with `aegis_research_eod_summary`

Lite artifacts must remain in the artifact registry as legacy compatibility evidence and must not block runtime readiness for these two migrated responsibilities.
