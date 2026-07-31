# Aegis Scheduled Run Readiness Requirements v1

## Objective

Create a durable readiness certification system for scheduled Aegis workflows. Before a scheduled run starts, Aegis must be able to answer whether the run is certified ready. After the run finishes, Aegis must be able to answer whether any failure was predictable before the run started.

This system does not replace cron, npm scripts, systemd timers, or existing orchestration. It certifies readiness for scheduled runs and records predictable failure classes.

## Initial Scheduled Runs

- `CANDIDATE_GENERATION_0950`: 09:50 candidate generation.
- `MIDDAY_MONITORING`: midday monitoring.
- `EOD_OUTCOME_UPDATE`: end-of-day outcome update.
- `RESEARCH_ALLOCATION`: research allocation.
- `VALIDATION_MATURITY`: validation maturity.
- `OPERATOR_DASHBOARD_REFRESH`: operator dashboard refresh.

## Architecture Requirements

The system must implement:

```text
Scheduled Run Registry
-> Dependency Manifest
-> Freshness Validator
-> Safe Repair Executor
-> Readiness Certificate
-> Scheduled Run Execution Check
-> Post-Run Reconciliation
```

## Artifact Requirements

### Scheduled Run Registry

Output:

```text
reports/aegis_scheduled_run_registry_v1/<day_utc>/scheduled_run_registry.v1.json
```

Each run must include:

- `scheduled_run_id`
- `run_name`
- `target_time_local`
- `target_time_utc`
- `run_type`
- `expected_command`
- `required_capabilities`
- `required_dependencies`
- `blocking_policy`
- `repair_policy`
- `owner`
- `enabled`

### Dependency Manifest

Output:

```text
reports/aegis_scheduled_run_dependency_manifest_v1/<day_utc>/dependency_manifest.v1.json
```

Each dependency must include:

- `dependency_id`
- `scheduled_run_id`
- `required_artifact`
- `producer_command`
- `freshness_window_minutes`
- `current_artifact_path`
- `expected_day_utc`
- `required_schema_version`
- `repair_command`
- `is_blocking`
- `failure_codes`

The manifest must include dependencies for market data, VIX input, sleeve input contracts, allowed symbol universe, candidate diagnostics inputs, canonical candidate state, operator action model, outcome registry, validation samples, statistical sufficiency, research allocation artifacts, and portal runtime model.

### Readiness Certificate

Output:

```text
reports/aegis_scheduled_run_readiness_certificate_v1/<day_utc>/readiness_certificate.v1.json
```

Allowed statuses:

- `CERTIFIED_READY`
- `REPAIRED_CERTIFIED_READY`
- `BLOCKED`
- `EXPIRED`
- `NOT_APPLICABLE`
- `MARKET_NOT_READY`

Each certificate must include source artifacts and source hashes. A `CERTIFIED_READY` certificate must never contain blocking dependencies.

### Safe Repair Log

Safe repair may rebuild known read-only artifacts in dependency order. Repair must never alter trading policy or bypass validation/safety gates.

Allowed repairs:

- refresh stale input contracts
- regenerate market data input hashes
- regenerate canonical candidate state
- regenerate operator action model
- regenerate portal runtime model
- regenerate current-day projections
- rerun known artifact builders in dependency order

Forbidden repairs:

- expanding symbol universe without evidence
- weakening validation gates
- bypassing stale data checks
- bypassing uncertified price checks
- forcing candidates
- enabling trade advice
- enabling manual capture
- enabling broker execution
- changing live trading policy

### Post-Run Reconciliation

Output:

```text
reports/aegis_scheduled_run_reconciliation_v1/<day_utc>/scheduled_run_reconciliation.v1.json
```

For each scheduled run, reconciliation must answer whether the run executed, whether a valid certificate existed, whether failure was predictable, which preflight checks were missed, and what prevention is recommended.

## 09:50 Candidate Run Requirements

Before the 09:50 candidate run, Aegis must certify or explicitly block:

- sleeve input contracts fresh
- market data hashes fresh
- required VIX input available if needed
- allowed symbol universe valid
- candidate diagnostics dependencies available
- operator action model can explain expected state

If not certified, Command Center must show which sleeve or dependency is blocked, why, whether safe repair was attempted, whether David action is required, and expected impact on the scheduled run.

## Operator UI Requirements

Update existing Command Center with a Scheduled Run Readiness section. It must show:

- next scheduled run
- readiness status
- certificate status
- readiness valid until
- blocked dependencies
- repair status
- David action required
- AI/system action required
- post-run completion state
- valid certificate used
- predictable failure state
- missed preflight checks

Do not create a separate dashboard unless the existing Command Center cannot safely render this information.

## Safety Requirements

- Do not weaken runtime truth kernel.
- Do not bypass verified graph.
- Do not enable trade advice.
- Do not enable manual capture.
- Do not enable broker execution.
- Do not create live-trading behavior.
- Do not force candidate generation.
- Do not treat no-setup as failure.

## Completion Requirements

The work is complete only when docs, artifacts, commands, self-check, tests, Command Center data shape, audit integration, and completion report exist and validation passes.
