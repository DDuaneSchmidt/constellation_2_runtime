# Aegis Scheduled Run Readiness Design v1

## Design Summary

Scheduled-run readiness is implemented as a read-only artifact layer. It builds a registry of scheduled workflows, expands that registry into dependency checks, optionally applies constrained safe repairs, emits readiness certificates, and reconciles observed run output after the fact.

The layer does not schedule jobs. It is invoked by npm scripts and audit. Existing cron, npm scripts, and orchestration remain the run initiators.

## Modules

- `ops/aegis/scheduled_run_registry_v1.py`: deterministic run registry.
- `ops/aegis/scheduled_run_dependency_manifest_v1.py`: dependency expansion and artifact checks.
- `ops/aegis/scheduled_run_safe_repair_v1.py`: constrained repair executor and repair log.
- `ops/aegis/scheduled_run_readiness_certificate_v1.py`: certificate generation.
- `ops/aegis/scheduled_run_reconciliation_v1.py`: post-run reconciliation.
- `ops/aegis/scheduled_run_readiness_self_check_v1.py`: deterministic validation.

CLI wrappers live in `ops/tools`.

## Dependency Strategy

Dependencies are artifact-backed and target-day scoped. For replayed historical days, freshness means the artifact belongs to the expected day and has the expected schema/version where applicable. Wall-clock expiration is recorded on certificates and checked separately so historical audits do not fail solely because the replay is later than the original run.

## Readiness Status Strategy

Readiness status is computed per run:

- Missing blocking dependencies produce `BLOCKED`.
- Market dependencies that are not available because the market is not ready may produce `MARKET_NOT_READY`.
- All blocking dependencies ready produces `CERTIFIED_READY`.
- All blocking dependencies ready after successful repair produces `REPAIRED_CERTIFIED_READY`.
- Disabled runs produce `NOT_APPLICABLE`.
- A certificate whose valid-until time is before its generated time is `EXPIRED` and cannot be treated as valid.

## Safe Repair Strategy

Safe repair is conservative. It only invokes known read-only artifact builders or records a no-repair reason. It explicitly refuses command strings that imply broker execution, live trading, manual capture enablement, stale-data bypass, validation gate weakening, forced candidates, or policy changes.

## 09:50 Candidate Run

The 09:50 run uses dependencies for market data, VIX/context, sleeve readiness/input contracts, allowed symbol universe evidence, candidate diagnostics inputs, canonical candidate state, and operator action model. If blocked, the Command Center can show dependency IDs, reasons, repair attempts, David action, and expected run impact from the certificate and reconciliation artifacts.

## UI Integration

The existing Command Center reads the operator-today envelope. The backend adds a `scheduled_run_readiness` object containing:

- next scheduled run
- readiness summary
- certificate details
- blocked dependencies
- repair summary
- David/system action flags
- reconciliation summary

The frontend renders this in the Command Center without creating a new dashboard.

## Audit Integration

`npm run aegis:audit` runs scheduled-run readiness and scheduled-run readiness self-check before strict verified graph. This does not replace audit; it adds readiness artifacts as preflight evidence.

## Safety Boundaries

The system does not change runtime truth, verified graph semantics, candidate generation rules, sleeve logic, broker execution, manual capture, trade advice, live trading policy, or autonomous execution. No repair can force a candidate or weaken a gate.
