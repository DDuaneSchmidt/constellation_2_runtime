# C2 Runtime Environment Boundary V1

## Runtime Authority

- `/home/node/constellation` is the only authoritative runtime code root for direct operational entrypoints.
- `/home/node/constellation_runtime_data/truth` is the only authoritative canonical truth root for day-start and operator-state control artifacts.
- `/home/node/constellation_active` remains release provenance only for the active runtime contract until retired from direct service launch.
- `/home/node/constellation_2_runtime` is a legacy runtime copy and must not be used as live authority for direct service launch, operator alerts, or day-start control writes.

## Canonical Entrypoints

- `ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh`
- `ops/run/c2_session_authority_monitor_v1.sh`
- `ops/tools/run_trading_day_control_plane_v1.py`
- `ops/tools/run_trading_day_execution_control_plane_v1.py`
- `ops/tools/run_trading_day_state_machine_v1.py`
- `ops/tools/run_submit_boundary_status_v1.py`
- `ops/tools/run_session_authority_alert_v1.py`
- `ops/tools/run_c2_global_monitoring_refresh_v1.py`
- `ops/run/c2_supervisor_paper_v2.py`

All of these entrypoints MUST execute from `/home/node/constellation` and MUST fail closed when launched from any other repo root.

## Canonical Truth Contract

- Policy-critical day-start readers and writers default to `/home/node/constellation_runtime_data/truth`.
- Silent fallback to repo-local `constellation_2/runtime/truth` is forbidden.
- Explicit alternate truth roots are allowed only for bounded test or repair invocations that pass `--truth_root` directly.

## Alert Authority

- `constellation_2/common/operator_alert_v1.py` is authoritative for operator alert derivation in this repo.
- `governance/05_CONTRACTS/C2/operator_alert_decision_v1.contract.md` is authoritative for normalized alert phase, state, severity, dedupe, and channel policy semantics.
- When canonical `trading_day_state_v1` and `day_start_blocked_v1` bridge artifacts are absent, operator alerts MUST derive from `reports/trading_day_state_machine_v1/<day>/trading_day_state_machine.v1.json`.
- State-machine fallback MUST be labeled explicitly and MUST not degrade to `UNKNOWN` solely because legacy bridge artifacts are missing.
- Delivery layers MUST honor normalized `notify_*` fields from the decision object and MUST NOT reinterpret raw heartbeat or raw state to decide email delivery.

## Legacy / Shadow Entrypoints

The following paths are legacy or shadow and must not be used as runtime authority:

- `/home/node/constellation_2_runtime/ops/tools/run_c2_global_monitoring_refresh_v1.py`
- `/home/node/constellation_2_runtime/ops/run/c2_supervisor_paper_v2.py`
- `/home/node/constellation_2_runtime/ops/run/c2_execution_observer_v1.sh`
- Any systemd unit or wrapper that sets `WorkingDirectory=/home/node/constellation_2_runtime`
- Any systemd unit or wrapper that launches `/home/node/constellation_active/...` for direct day-start authority

## Canonical Recompute Procedure

The authoritative stale-artifact repair path is:

- `ops/tools/run_trading_day_start_recompute_v1.py --day_utc <YYYY-MM-DD> --truth_root /home/node/constellation_runtime_data/truth`

This procedure MUST run, in order:

1. `run_trading_day_control_plane_v1.py`
2. `run_trading_day_execution_control_plane_v1.py`
3. `run_trading_day_state_machine_v1.py`

The sequence is safe to rerun for the same day because each step rewrites the same canonical same-day artifact path.
