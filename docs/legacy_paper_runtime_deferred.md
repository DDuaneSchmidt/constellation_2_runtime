# Legacy PAPER Runtime Deferred

Legacy autonomous PAPER timers no longer define the operating model:

- `c2-paper-day-orchestrator.timer`
- `aegis-paper-ready-kernel-v1.timer`
- `aegis-paper-ready-kernel-v1-after-orchestrator.timer`
- `c2-paper-auto-repair-controller.timer`
- `c2-paper-auto-repair-eod-final.timer`

Their checked-in timer files are marked `LEGACY_PAPER_AUTOMATION_DEFERRED`. Their services require:

```ini
ConditionEnvironment=AEGIS_ENABLE_LEGACY_PAPER_AUTOMATION=1
```

This preserves the units for audit and rollback while preventing them from being the default manual PAPER operating path. Aegis Lite EOD is primary.

Runtime proof is written to `legacy_paper_runtime_status.v1`. The UI should treat old readiness graphs, paper-ready kernel outputs, submit-boundary orchestration, and legacy operator state as diagnostic only.

## IB Gateway Startup After Lite Pivot

Current Aegis Lite operation is broker-agnostic:

- `broker_mode=MANUAL_ONLY`
- `ib_automation_status=DEFERRED`
- `broker_required_for_runtime=false`
- `broker_submit_required=false`

IB Gateway startup paths are classified as:

| Path | Classification | Default Lite status |
| --- | --- | --- |
| `aegis-lite-eod-report-v1.timer` / `.service` | `ACTIVE` | Runs Lite EOD only; does not launch IB Gateway or submit orders. |
| `c2-ib-gateway.service` | `DEFERRED_AFTER_PIVOT` / `SAFE_TO_DISABLE` | Replaced by a deprecated non-owning marker; disabled in active user systemd. |
| `ib-gateway.service` | `REQUIRED_FOR_MANUAL_MODE` only when the operator explicitly launches Gateway | Disabled by default; not part of Lite runtime. |
| `c2-ib-liveness.timer` / `.service` | `DEFERRED_AFTER_PIVOT` | Masked; no automatic reconnect/liveness loop. |
| `constellation-ib-gateway-observer.service` | `LEGACY_UNUSED` | Disabled; diagnostic observer only. |
| `constellation-ib-market-observer.service` | `LEGACY_UNUSED` | Disabled; diagnostic observer only. |
| `c2-execution-observer.service` and `ops/run/c2_execution_observer_v1.sh` | `DEFERRED_AFTER_PIVOT` / `SAFE_TO_DISABLE` | Disabled in active user systemd; checked-in service requires `AEGIS_ENABLE_LEGACY_BROKER_OBSERVATION=1`; not a Lite dependency. |
| Legacy PAPER submit/orchestrator tools such as `run_aegis_paper_submit_v1.py`, `run_aegis_paper_auto_v1.py`, and `run_paper_submit_smoke_test_v1.py` | `DEFERRED_AFTER_PIVOT` | Must not be used by Lite EOD/event/Research/performance paths. |

Lite EOD, event awareness, Research Lab, manual receipt/outcome handling, and sleeve performance reporting must complete without IB Gateway active. If a future phase wants broker integration again, it must explicitly define operator approval, Gateway launch ownership, reconnect behavior, submit authority, transmit controls, and fill lifecycle authority.
