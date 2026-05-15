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
