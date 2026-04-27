# Runtime Supervisor

This directory defines the repo-native runtime lifecycle controller for local Constellation services.

## Single Source Of Process Truth

`ops/runtime/supervisor.py` is the only writer of runtime process truth files:

- `runtime/process_state/service_status.json`
- `runtime/process_state/supervisor_state.json`
- `runtime/process_state/last_start_report.json`

UI code and scripts may read these files but must not create competing process state artifacts.

## Manifest

`runtime_manifest.yaml` defines required services. Initial required service:

- `ops_dashboard`
  - host: `127.0.0.1`
  - port: `8787`
  - entrypoint: `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`
  - health: `http://127.0.0.1:8787/health`

## Commands

From repo root:

- `python3 ops/runtime/supervisor.py start`
- `python3 ops/runtime/supervisor.py stop`
- `python3 ops/runtime/supervisor.py restart`
- `python3 ops/runtime/supervisor.py status`
- `python3 ops/runtime/supervisor.py logs`
- `python3 ops/runtime/healthcheck.py`

## Runtime Outputs

The supervisor ensures these paths exist:

- `runtime/process_state/`
- `runtime/logs/`

Service PIDs are stored in `runtime/process_state/<service>.pid` and used for stop/restart ownership checks.

## Manual Trading Morning Flow

Paper trading readiness is manual and operator-driven. No scheduler, timer, or cron job auto-runs readiness.

Run from repo root:

- `npm run app:restart`
- `npm run trading:prepare`
- `npm run trading:preflight`

`trading:prepare` writes `runtime/process_state/trading_readiness_decision.json`.
`trading:preflight` fails closed if the decision is missing, expired, wrong-day, non-PAPER, non-GO, or missing required gate evidence.
