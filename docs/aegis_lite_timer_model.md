# Aegis Lite Timer Model

Primary timer:

- `ops/systemd/user/aegis-lite-eod-report-v1.timer`
- `ops/systemd/user/aegis-lite-eod-report-v1.service`
- target: `15:50 America/New_York`
- calendar: `OnCalendar=Mon..Fri *-*-* 15:50:00 America/New_York`

Service command:

```bash
/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh run_aegis_lite_eod_pipeline_v1 --day_utc @today_utc@ --environment PAPER --manual-only --allow-not-ready-exit-zero
```

Purpose: generate the current Aegis Lite EOD report, operator queue, edge clusters, overlap review, and operating status artifact.

The timer is manual-execution only. It has no broker submit, transmit automation, fill lifecycle automation, or IB order routing responsibility.

The systemd calendar wakes on weekdays at 15:50 ET. The producer also reads governed `market_calendar_v1/NYSE/<year>.jsonl`; if the day is not a NYSE trading session or the calendar is missing, it writes an advisory/not-ready Lite report with no executable queue items.

The 15:50 ET EOD run is the single canonical daily Aegis Lite decision run. Event awareness runs are non-canonical and must not overwrite this state.
