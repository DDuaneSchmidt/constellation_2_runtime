# Event-Driven Aegis Research Loop V1

## Scope
This V1 introduces evidence-triggered research events for:
- `TRADING_DAY_CLOSED`
- `SANDBOX_RESULT_COMPLETED`

Research automation remains sandbox-only.
No live trading, no paper enablement, no risk/capital mutation, and no Aegis Core policy mutation are allowed.

## Future Scheduler Note (not enabled in this task)
After market close, a future scheduler can call:

```bash
python3 ops/tools/run_research_event_trading_day_closed_v1.py --day_utc <trading_day>
python3 ops/tools/run_research_event_sweep_v1.py
```

This task does not install or enable cron/systemd timers.
