# Aegis Scheduled Run Readiness Spec v1

## Scope

This spec defines read-only scheduled-run readiness artifacts and validation logic. It does not define a scheduler and does not execute trading, broker, live, manual capture, or autonomous workflows.

## Registry Contract

Artifact family: `aegis_scheduled_run_registry_v1`

Filename: `scheduled_run_registry.v1.json`

Top-level fields:

- `schema_id`: `aegis_scheduled_run_registry`
- `schema_version`: `v1`
- `artifact_id`: `aegis_scheduled_run_registry_v1`
- `day_utc`
- `generated_at`
- `scheduled_runs`
- `scheduled_runs_by_id`
- `source_artifacts`
- `source_hashes`

Initial IDs:

- `CANDIDATE_GENERATION_0950`
- `MIDDAY_MONITORING`
- `EOD_OUTCOME_UPDATE`
- `RESEARCH_ALLOCATION`
- `VALIDATION_MATURITY`
- `OPERATOR_DASHBOARD_REFRESH`

## Dependency Manifest Contract

Artifact family: `aegis_scheduled_run_dependency_manifest_v1`

Filename: `dependency_manifest.v1.json`

Top-level fields:

- `schema_id`: `aegis_scheduled_run_dependency_manifest`
- `schema_version`: `v1`
- `artifact_id`: `aegis_scheduled_run_dependency_manifest_v1`
- `day_utc`
- `generated_at`
- `dependencies`
- `dependencies_by_run_id`
- `source_artifacts`
- `source_hashes`

Dependency status values:

- `READY`
- `MISSING`
- `STALE`
- `SCHEMA_MISMATCH`
- `NO_SETUP_NOT_APPLICABLE`
- `MARKET_NOT_READY`

Historical target-day validation uses expected day, artifact availability, schema/version, and source hashes. Wall-clock certificate expiry is represented on the certificate and must not be confused with target-day artifact readiness during replay.

## Readiness Certificate Contract

Artifact family: `aegis_scheduled_run_readiness_certificate_v1`

Filename: `readiness_certificate.v1.json`

Top-level fields:

- `schema_id`: `aegis_scheduled_run_readiness_certificate`
- `schema_version`: `v1`
- `artifact_id`: `aegis_scheduled_run_readiness_certificate_v1`
- `day_utc`
- `generated_at`
- `certificates`
- `certificates_by_run_id`
- `summary`
- `source_artifacts`
- `source_hashes`

Each certificate includes:

- `certificate_id`
- `scheduled_run_id`
- `target_run_time`
- `readiness_status`
- `readiness_valid_until`
- `dependencies_checked`
- `dependencies_ready`
- `dependencies_blocked`
- `repairs_attempted`
- `repairs_successful`
- `repairs_failed`
- `remaining_blockers`
- `source_artifacts`
- `source_hashes`
- `generated_at`
- `certificate_hash`

`CERTIFIED_READY` and `REPAIRED_CERTIFIED_READY` require zero blocking dependencies.

## Safe Repair Contract

Artifact family: `aegis_scheduled_run_safe_repair_v1`

Filename: `safe_repair.v1.json`

Every repair row includes:

- `repair_id`
- `dependency_id`
- `command`
- `before_status`
- `after_status`
- `result`
- `reason_codes`
- `source_hashes`

Allowed repair results:

- `SUCCESS`
- `FAILED`
- `SKIPPED`
- `FORBIDDEN`
- `NO_REPAIR_AVAILABLE`

Forbidden command classes must be blocked before execution and recorded as `FORBIDDEN`.

## Reconciliation Contract

Artifact family: `aegis_scheduled_run_reconciliation_v1`

Filename: `scheduled_run_reconciliation.v1.json`

Each run reconciliation includes:

- `scheduled_run_id`
- `did_run_execute`
- `did_valid_certificate_exist`
- `run_result`
- `failed_reason`
- `was_failure_predictable`
- `missed_preflight_checks`
- `new_failure_class`
- `recommended_prevention`

## Self-Check Contract

Artifact family: `aegis_scheduled_run_readiness_self_check_v1`

Filename: `self_check.v1.json`

Failure codes:

- `SCHEDULED_RUN_MISSING_DEPENDENCIES`
- `DEPENDENCY_MISSING_PRODUCER`
- `CERTIFICATE_MISSING_HASH`
- `CERTIFIED_READY_WITH_BLOCKERS`
- `EXPIRED_CERTIFICATE_USED`
- `REPAIR_MISSING_BEFORE_AFTER`
- `RUN_FAILED_WITHOUT_RECONCILIATION`
- `NON_DETERMINISTIC_OUTPUT`

The self-check must write deterministic output and return non-zero through its CLI when failures exist.
