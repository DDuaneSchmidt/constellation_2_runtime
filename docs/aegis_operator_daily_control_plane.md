# Aegis Operator Daily Control Plane

Date: 2026-05-15

This is the daily operator command surface for the pivoted Aegis Lite + Research Lab system. It is manual-only, broker-independent, and advisory until a current non-demo promoted executable candidate is present.

## Current Model

- Aegis Lite EOD is canonical at 15:50 ET.
- Event monitoring is non-canonical and cannot overwrite EOD state.
- Event monitoring may run automatically during market hours only after the operator explicitly enables the user timer.
- Trades are entered manually in IB paper by David.
- Receipts and outcomes are recorded through operator CLIs, not IB automation.
- Research Lab remains offline and non-authoritative.
- Alert transport is currently `GATE_ONLY_NO_TRANSPORT`; real email/SMS delivery is not implemented.

## Daily Command Path

Pre-market Research review:

```bash
python3 ops/tools/list_research_hypotheses_v1.py --truth_root <research_truth_root> --day_utc <YYYY-MM-DD>
python3 ops/tools/research_architecture_integrity_review_v1.py --truth_root <research_truth_root>
python3 ops/tools/audit_research_dataset_bindings_v1.py --truth_root <research_truth_root> --day_utc <YYYY-MM-DD>
```

Intraday event monitor, when a governed market snapshot is available:

```bash
python3 ops/tools/run_aegis_event_monitor_v1.py --truth_root <truth_root> --day_utc <YYYY-MM-DD> --market_snapshot_json <snapshot.json>
```

Fail-closed scheduled/manual event monitor:

```bash
python3 ops/tools/run_aegis_event_monitor_v1.py --truth_root <truth_root>
```

Enable disabled-by-default market-hours event monitoring:

```bash
mkdir -p ~/.config/systemd/user
cp ops/systemd/user/aegis-event-monitor-v1.service ~/.config/systemd/user/
cp ops/systemd/user/aegis-event-monitor-v1.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now aegis-event-monitor-v1.timer
```

Disable or inspect it:

```bash
systemctl --user disable --now aegis-event-monitor-v1.timer
systemctl --user status aegis-event-monitor-v1.timer --no-pager
systemctl --user list-timers aegis-event-monitor-v1.timer --all
```

Canonical near-close Lite EOD:

```bash
python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --day_utc <YYYY-MM-DD> --truth_root <truth_root> --environment PAPER --manual-only --allow-not-ready-exit-zero
```

After manual IB paper entry:

```bash
python3 ops/tools/record_manual_execution_receipt_v1.py --truth_root <truth_root> --source_packet_id <recommended_trade_id> --symbol <SYMBOL> --side <BUY|SELL> --quantity <N> --fill_price <PRICE> --fill_timestamp_utc <UTC> --stop_entered <yes|no> --stop_price <PRICE> --notes "<operator note>"
```

After exit/outcome is known:

```bash
python3 ops/tools/record_trade_outcome_v1.py --truth_root <truth_root> --trade_id <recommended_trade_id_or_receipt_id> --exit_price <PRICE> --exit_timestamp_utc <UTC> --outcome_status <WIN|LOSS|SCRATCH|STOPPED_OUT> --notes "<operator note>"
```

Performance review:

```bash
python3 ops/tools/build_and_print_sleeve_performance_report_v1.py --truth_root <truth_root> --day <YYYY-MM-DD>
```

EOD sleeve feedback review:

```bash
python3 ops/tools/build_eod_sleeve_review_v1.py --truth_root <truth_root> --day <YYYY-MM-DD>
python3 ops/tools/build_eod_sleeve_review_v1.py --truth_root <truth_root> --day <YYYY-MM-DD> --write_research_tasks
```

Weekly sleeve feedback review:

```bash
python3 ops/tools/build_eow_sleeve_review_v1.py --truth_root <truth_root> --week_ending <YYYY-MM-DD>
python3 ops/tools/build_eow_sleeve_review_v1.py --truth_root <truth_root> --week_ending <YYYY-MM-DD> --write_research_tasks
```

AI feedback review with Evidence Gate:

```bash
python3 ops/tools/build_ai_eod_feedback_review_v1.py --truth_root <truth_root> --day <YYYY-MM-DD>
python3 ops/tools/build_ai_eow_feedback_review_v1.py --truth_root <truth_root> --week_ending <YYYY-MM-DD>
```

The AI feedback commands currently use deterministic fallback only. They may write offline Research tasks when the Evidence Gate reaches at least `WEAK_SIGNAL`; they cannot create trades, promote/demote sleeves, mutate production logic, or touch broker paths.

Operator status summary:

```bash
python3 ops/tools/build_aegis_operator_status_v1.py --truth_root <truth_root> --day_utc <YYYY-MM-DD>
```

Optional advisor comparison:

```bash
python3 ops/tools/build_advisor_benchmark_v1.py --truth_root <truth_root> --benchmark_name "<name>" --period_start <YYYY-MM-DD> --period_end <YYYY-MM-DD> --gross_return <pct> --fee_rate <pct> --aegis_return <pct>
```

## Status Artifact

`aegis_operator_status.v1` is the daily one-place status artifact. It reports:

- last EOD run when present
- current promoted/actionable manual candidates
- manual packets needing action
- missing receipt count
- missing outcome count
- sleeve performance report status
- EOD/EOW sleeve review artifacts when generated
- event monitor status
- event monitor enabled/disabled status
- last event monitor run
- next scheduled event monitor run when systemd reports one
- last event data freshness status
- last triggered and blocked event counts
- Research dataset blockers
- next operator action

The status artifact is advisory and never creates trades, sleeves, broker actions, or Research promotions.

## Demo And Dry-Run Guardrails

- `DEMO_ONLY` packets are not actionable.
- `DRY_RUN_ONLY` packets are not actionable.
- A real manual paper smoke requires `runtime_truth_classification=REAL_RUNTIME`.
- Proof roots under `/tmp` are evidence only and must not be treated as current runtime truth.

## Remaining Operator Gaps

- No dashboard form exists yet for receipt or outcome entry.
- No dedicated sleeve performance dashboard panel is proven.
- No real email/SMS trade alert transport is proven.
- Research dataset bindings remain incomplete for real price, volatility, breadth, macro event, and regime history testing.
- Current runtime remains blocked for real paper trading until a non-demo promoted executable candidate appears in current runtime truth.
