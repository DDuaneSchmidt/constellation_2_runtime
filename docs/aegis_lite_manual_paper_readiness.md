# Aegis Lite Manual Paper Readiness

Audit/update date: 2026-05-15

This report covers the pivoted Aegis Lite + Research Lab manual-paper workflow only. It does not authorize production activation, broker submit, IB transmit, fill automation, or autonomous execution.

Status legend:
- `READY`: implemented and proven for the manual-paper workflow.
- `READY_WITH_MANUAL_STEPS`: implemented, but requires explicit operator/release/manual steps before use.
- `PRESENT_UNPROVEN`: exists but lacks current end-to-end proof.
- `BLOCKED`: must be fixed or activated before real supervised manual paper trading.
- `MISSING`: no usable implementation found.

## Summary

Current readiness for real manual paper trading today: `BLOCKED`.

Reason:
- Repo timer/source code, active user systemd, and current runtime operating status now use the canonical 09:50 UTC and 14:50 UTC sleeve runs time.
- Current runtime queue/report exists, but no promoted executable candidate is present.
- Current runtime is `ADVISORY_ONLY`, with no actionable manual trade packet or executable operator queue item.
- P0 offline proof has validated the promoted-sleeve-only queue and dry lifecycle mechanics under `/tmp`, but that proof is not a live runtime trade authorization.

Safe dry-run/operator validation readiness: `READY_WITH_MANUAL_STEPS`.

## P0 Operator Readiness Status

Status date: 2026-05-15

### What To Run Daily

Pre-market / morning:
```bash
python3 ops/tools/list_research_hypotheses_v1.py --truth_root /path/to/offline_research_truth --day_utc YYYY-MM-DD
python3 ops/tools/research_architecture_integrity_review_v1.py --truth_root /path/to/offline_research_truth
```

Near close:
```bash
python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --day_utc YYYY-MM-DD --truth_root /path/to/truth --environment PAPER --manual-only --allow-not-ready-exit-zero
```

After manual action or dry receipt:
```bash
python3 ops/tools/record_manual_execution_receipt_v1.py --truth_root /path/to/truth --source_packet_id <recommended_trade_id> --symbol <SYMBOL> --side <BUY|SELL> --quantity <N> --fill_price <PRICE> --fill_timestamp_utc <UTC> --stop_entered <yes|no> --stop_price <PRICE> --notes "operator note"
python3 ops/tools/record_trade_outcome_v1.py --truth_root /path/to/truth --trade_id <recommended_trade_id_or_receipt_id> --exit_price <PRICE> --exit_timestamp_utc <UTC> --outcome_status <WIN|LOSS|SCRATCH|OPEN|STOPPED_OUT> --notes "operator note"
python3 ops/tools/build_sleeve_performance_report_v1.py --truth_root /path/to/truth --day YYYY-MM-DD
python3 ops/tools/build_and_print_sleeve_performance_report_v1.py --truth_root /path/to/truth --day YYYY-MM-DD
python3 ops/tools/build_aegis_operator_status_v1.py --truth_root /path/to/truth --day_utc YYYY-MM-DD
```

### Ready

- Canonical Lite EOD timer/status alignment is 09:50 UTC and 14:50 UTC.
- Release/repo match is currently reported as `MATCH`.
- Lite runtime status carries `broker_mode=MANUAL_ONLY`, `ib_automation_status=DEFERRED`, and `broker_required_for_runtime=false`.
- Lite EOD fails closed when candidate input or promoted sleeve library is missing.
- Offline P0 proof generated one promoted executable queue item from a human-approved promoted sleeve library.
- Offline P0 proof rejected one unpromoted Research candidate with `SLEEVE_NOT_APPROVED_FOR_LITE_OPERATION`.
- Offline P0 proof generated a complete `manual_trade_packet.v1`.
- Offline P0 proof generated `manual_execution_receipt.v1`, `outcome_ledger.v1`, and `sleeve_performance_report.v1`.
- Sleeve performance report generated offline Research follow-up task recommendations without mutating Lite runtime.

### Blocked

- Current runtime truth remains `ADVISORY_ONLY`.
- Current runtime `operator_execution_queue.v1` is empty.
- Current runtime `manual_trade_packet.v1` has no trade candidates.
- No real current promoted non-demo executable candidate has been generated in runtime truth.
- A real supervised IB paper trade has not been executed or measured.

### Manual-Only

- IB paper order entry is manual.
- IB Gateway is not launched by the Lite runtime. If the operator wants Gateway open for manual paper entry, it must be started explicitly outside the Lite scheduler.
- Protective stop entry is manual.
- Receipt recording is manual.
- Outcome/exit evidence is manual unless separately supplied by governed non-IB data.
- Research feedback from performance remains offline and non-authoritative.

### Unproven

- Actual SMS/email delivery is unproven.
- Event alert output is `MESSAGE_BODY_DRY_RUN_ONLY`: ledger and message-body artifacts only.
- Dedicated UI panels for receipt status, outcome ledger, sleeve performance report, and Research Lab are unproven. Receipt/outcome/performance/status CLIs now exist.
- Real Research dataset bindings for price, volatility, breadth, macro events, and regime labels remain incomplete.

### P0 Offline Proof Artifacts

Safe proof root:
`/tmp/aegis_p0_offline_proof_20260515`

Generated artifacts:
- Candidate input: `/tmp/aegis_p0_offline_proof_20260515/inputs/candidate_input.v1.json`
- Promoted sleeve library: `/tmp/aegis_p0_offline_proof_20260515/inputs/promoted_sleeve_library.v1.json`
- EOD report: `/tmp/aegis_p0_offline_proof_20260515/reports/aegis_lite_eod_report_v1/2026-05-15/p0_promoted_queue_proof/aegis_lite_eod_report.v1.json`
- Operator queue: `/tmp/aegis_p0_offline_proof_20260515/reports/operator_execution_queue_v1/2026-05-15/p0_promoted_queue_proof/operator_execution_queue.v1.json`
- Manual trade packet: `/tmp/aegis_p0_offline_proof_20260515/reports/manual_trade_packet_v1/2026-05-15/p0_promoted_queue_proof/manual_trade_packet.v1.json`
- Manual receipt: `/tmp/aegis_p0_offline_proof_20260515/research_lab/manual_execution_receipt_v1/2026-05-15/p0_receipt_001/manual_execution_receipt.v1.json`
- Outcome ledger: `/tmp/aegis_p0_offline_proof_20260515/research_lab/outcome_ledger_v1/2026-05-15/index/outcome_ledger.v1.json`
- Sleeve performance report: `/tmp/aegis_p0_offline_proof_20260515/reports/sleeve_performance_report_v1/2026-05-15/sleeve_performance_report.v1.json`

P0 proof result:
- `manual_execution_status=READY_FOR_MANUAL_ENTRY`
- `readiness_classification=READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING`
- `broker_submit_required=false`
- `manual_execution_only=true`
- `transmit_automation_required=false`
- `ib_automation_required=false`
- sleeve report recommended 2 offline Research follow-up tasks and did not write Research queue or mutate Lite runtime.

## Timer Status

Status: `READY`

Repo/configured intent:
- `ops/systemd/user/aegis-lite-eod-report-v1.timer` uses `OnCalendar=*-*-* 09:50:00 UTC` and `OnCalendar=*-*-* 14:50:00 UTC`.
- `constellation_2/common/aegis_lite_operating_status_v1.py` now treats 09:50 UTC and 14:50 UTC as the canonical expected Lite EOD time.
- `docs/aegis_lite_timer_model.md` documents 09:50 UTC and 14:50 UTC.

Active runtime evidence:
- `systemctl --user status aegis-lite-eod-report-v1.timer` reports the active user timer at 09:50 UTC and 14:50 UTC.
- Current runtime `aegis_lite_operating_status.v1` reports `target_times_utc=[09:50,14:50]`.
- Current runtime `release_repo_match_status=MATCH`.

## EOD Manual Trade Packet

Status: `READY_WITH_MANUAL_STEPS`

Implemented:
- `manual_trade_packet.v1` schema exists.
- The EOD pipeline now writes `reports/manual_trade_packet_v1/<day>/<run_id>/manual_trade_packet.v1.json`.
- Packet candidates include run/date, sleeve id, source hypothesis id, symbol, side, instrument type, entry reference, order suggestion, quantity/sizing guidance, stop price/logic, risk, confidence, inclusion/exclusion reason, governance notes, edge-overlap result, checklist, actionability, and blockers.

Fail-closed behavior:
- Missing required entry, stop, risk, sizing, symbol, sleeve, or source-hypothesis fields makes the packet candidate `actionable=false`.
- Missing promoted sleeve/source mapping adds do-not-trade blockers.

Remaining proof gap:
- Current runtime packet generation is proven, but the packet currently has no trade candidates because promoted executable candidates are missing.

## Promoted Sleeve Flow

Status: `READY_WITH_MANUAL_STEPS`

Implemented path:
`research_hypothesis.v1 -> research_to_lite_promotion.v1 -> promoted_sleeve_library.v1 -> Aegis Lite EOD evaluation -> operator_execution_queue.v1 + manual_trade_packet.v1`

Confirmed controls:
- Research artifacts are offline/advisory and cannot directly create trades.
- `research_to_lite_promotion.v1` requires evidence refs, result ledger refs, lineage, and human approval for Lite implementation.
- EOD candidate filtering accepts only promoted sleeves with human approval and Lite implementation approval.
- Manual trade packet actionability requires the candidate sleeve/source hypothesis to match the promoted sleeve library.

Remaining proof gap:
- P0 offline proof validated promoted-sleeve-only filtering and one ready queue item under `/tmp`.
- Current runtime remains advisory and has no executable queue item. A real promoted sleeve candidate has not yet been proven end-to-end in runtime truth.

## Event Monitoring And Alert Transport

Status: `READY_WITH_MANUAL_STEPS`

Implemented:
- `event_rules_registry.v1`
- `event_monitoring_status.v1`
- `event_awareness_ledger.v1`
- `tactical_review_gate.v1`
- `event_tactical_packet.v1`
- `event_validity_gate.v1`
- `trade_capture_alert_gate.v1`
- `trade_capture_alert_ledger.v1`
- read-only UI/API surface at `/aegis-events` and `/api/aegis/event-monitoring`
- disabled-by-default market-hours timer/service:
  - `ops/systemd/user/aegis-event-monitor-v1.timer`
  - `ops/systemd/user/aegis-event-monitor-v1.service`

Alert safety:
- Email/SMS is allowed only when the event validity gate is `PASS`, the source packet is complete, sensitivity is not `EXTREME`, and enough time remains before `valid_until`.
- Blocked/expired/invalid/duplicate packets write no-alert ledger entries.

Transport finding:
- The event monitor and trade capture alert CLI write gate/ledger artifacts and message bodies.
- They do not currently send real email/SMS.
- Operator-facing transport status is `GATE_ONLY_NO_TRANSPORT`.
- Delivery rows may record `DRY_RUN_MESSAGE_BODY_ONLY` when a message body would have been eligible.
- Later enablement should wire a configured transport behind `trade_capture_alert_gate.v1` only, preserving the gate as send authority.

Scheduling finding:
- Event monitoring is source-configured to run every 15 minutes during regular U.S. market hours when explicitly enabled by the operator.
- The timer is not enabled by default and must be installed/enabled with `systemctl --user enable --now aegis-event-monitor-v1.timer`.
- If market data is missing or stale, scheduled runs write blocked status/ledger artifacts and create no actionable packets.
- Active UI route availability still depends on deploying an active release that includes `/aegis-events`; do not treat the repo route as active runtime until verified on `127.0.0.1:8787`.

## Manual Execution Receipt

Status: `READY`

Implemented:
- `manual_execution_receipt.v1` schema and builder support EOD manual packet and event tactical packet sources.
- Receipt fields include alert id, source packet id, event id/run id, fill timestamp, fill price, quantity, order type, stop entered, stop price, operator notes, deviation from recommendation, valid-until compliance, and max-slippage compliance.
- Receipt is observational only and requires no broker submit.
- Operator command:
  `python3 ops/tools/record_manual_execution_receipt_v1.py --truth_root <path> --source_packet_id <recommended_trade_id> --symbol <SYMBOL> --side <BUY|SELL> --quantity <N> --fill_price <PRICE> --fill_timestamp_utc <UTC> --stop_entered <yes|no> --stop_price <PRICE> --notes "<note>"`

Manual step:
- Operator still records the receipt outside a dedicated Lite UI form, but no JSON editing is required.

## Outcome Ledger

Status: `READY_WITH_MANUAL_STEPS`

Implemented:
- `trade_outcome_attribution.v1` records recommendation vs actual entry/exit, realized/unrealized PnL, model forward returns, MAE/MFE, operator slippage, skipped trade outcome, governance adjustment effect, sleeve signal quality, implementation quality, and operator execution quality.
- `outcome_ledger.v1` records generic outcome rows and can include alert id, event attribution, source packet type, execution sensitivity, validity-window compliance, slippage compliance, operator action, and alert usefulness.
- `sleeve_performance_report.v1` joins manual packets, receipts, outcomes, optional attributions, promoted sleeve lineage, event packets, and alert ledgers into the canonical Lite paper-performance report.
- Outcome rows can generate offline Research Lab follow-up tasks without Lite mutation.
- Operator command:
  `python3 ops/tools/record_trade_outcome_v1.py --truth_root <path> --trade_id <recommended_trade_id_or_receipt_id> --exit_price <PRICE> --exit_timestamp_utc <UTC> --outcome_status <status> --notes "<note>"`
- Operator performance summary command:
  `python3 ops/tools/build_and_print_sleeve_performance_report_v1.py --truth_root <path> --day <YYYY-MM-DD>`

Manual/no-IB mode:
- Manual execution receipt and manually supplied outcome/market data are the source of truth when IB is not integrated.

Remaining proof gap:
- There is no fully automated non-IB Lite mark/exit updater. Performance measurement depends on operator/manual outcome inputs.
- No dedicated Lite UI surface for `sleeve_performance_report.v1` is proven yet; use `python3 ops/tools/build_sleeve_performance_report_v1.py --truth_root <path> --day <YYYY-MM-DD>`.

## UI / Operator Workflow

| Surface | Status | Notes |
| --- | --- | --- |
| Aegis Lite execution queue | `READY` | `/aegis-lite` reads current Lite report/status/queue and fails closed. |
| EOD manual trade packet view | `PRESENT_UNPROVEN` | Packet artifact is now produced by repo pipeline, but UI primarily renders execution queue cards. |
| Event rules / monitor / ledger | `READY_WITH_MANUAL_STEPS` | `/aegis-events` and `/api/aegis/event-monitoring` expose rules, monitor status, ledger, packet detail, and alert status from artifacts. |
| Trade capture alert ledger | `READY_WITH_MANUAL_STEPS` | UI/API can read alert ledgers, but real email/SMS transport remains unimplemented. |
| Sleeve scores/rankings | `READY_WITH_MANUAL_STEPS` | `sleeve_performance_report.v1` now aggregates sleeve counts, returns, win rate, stop-hit rate, MAE/MFE, regime/event/alert performance; no Lite UI panel is proven. |
| Sleeve percent returns | `READY_WITH_MANUAL_STEPS` | `sleeve_performance_report.v1` computes sleeve total/average percent returns from closed outcome rows; missing receipts/outcomes are not treated as zero return. |
| Manual receipts needed/status | `READY_WITH_MANUAL_STEPS` | Receipt CLI exists and `aegis_operator_status.v1` can report missing receipt counts from the sleeve performance report; no Lite UI form/status panel is proven. |
| Outcome ledger | `READY_WITH_MANUAL_STEPS` | Outcome CLI exists and `sleeve_performance_report.v1` reads outcome ledger rows; no dedicated Lite outcome-ledger UI is proven. |
| Research Lab hypotheses/results | `MISSING` | Research Lab is CLI/JSON only. |
| Promotion candidates | `MISSING` | No Research-to-Lite promotion UI found. |

## Research Dataset Status

| Dataset | Status | Missing contract |
| --- | --- | --- |
| Price data | `PRESENT_UNPROVEN` | Research runner accepts fixture metrics; no direct `research_price_dataset_binding.v1` to governed market history is present. |
| Volatility data | `MISSING` | Need a Research Lab dataset contract for VIX/realized volatility/ATR inputs. |
| Breadth data | `MISSING` | Need a breadth dataset contract for participation, advance/decline, new highs/lows, sector breadth. |
| Macro event calendar | `MISSING` | Need a macro event calendar contract for CPI/Fed/event timestamps and surprise fields. |
| Regime labels | `PRESENT_UNPROVEN` | Evidence stores regimes, but no direct Research Lab regime-label binding is present. |
| Outcome data | `READY` | `ingest_trade_outcome_attribution_to_research_v1.py` imports Lite outcomes into Research learning. |
| Sleeve performance data | `READY_WITH_MANUAL_STEPS` | `sleeve_performance_report.v1` provides canonical Lite paper-performance evidence; Research Lab still lacks a dedicated dataset binding beyond explicit report/task handoff. |

## Daily Research Cadence

Status: `READY_WITH_MANUAL_STEPS`

Documentation:
- `docs/aegis_daily_research_cadence.md` defines the daily Aegis Research cadence.
- The documented cadence covers pre-market Research review, intraday event awareness, the 09:50 UTC and 14:50 UTC Lite sleeve runs, after-close manual receipt/outcome updates, the offline Research Lab daily run, and weekly review.

Command/status coverage:

| Cadence step | Status | Existing command support | Blocked or manual-only notes |
| --- | --- | --- | --- |
| Pre-market hypothesis/task review | `READY_WITH_MANUAL_STEPS` | `list_research_hypotheses_v1.py`, `research_lab_register_hypothesis_v1.py`, `research_architecture_integrity_review_v1.py` | Review is CLI/JSON; no Research Lab UI is proven. |
| Prior outcome review | `READY_WITH_MANUAL_STEPS` | Outcome/trade-outcome tools exist; Research import uses `ingest_trade_outcome_attribution_to_research_v1.py` | Missing receipts/outcomes require operator evidence. |
| Intraday event awareness | `PRESENT_UNPROVEN` | `run_event_awareness_v1.py`, `run_event_tactical_review_v1.py`, `run_event_validity_gate_v1.py`, `run_trade_capture_alert_gate_v1.py` | Event UI and real email/SMS transport are not proven. |
| Near-close canonical Lite EOD | `READY_WITH_MANUAL_STEPS` | `run_aegis_lite_eod_pipeline_v1.py` | Repo, active systemd, and runtime status agree on 09:50 UTC and 14:50 UTC; current output remains advisory because promoted executable candidates are missing. |
| Manual execution receipt update | `READY_WITH_MANUAL_STEPS` | Receipt schema/builder exist | Operator entry is manual; no Lite receipt UI form is proven. |
| Outcome ledger update | `READY_WITH_MANUAL_STEPS` | `run_outcome_attribution_v1.py`, `run_trade_outcome_v1.py`, `ingest_trade_outcome_attribution_to_research_v1.py` | Performance measurement depends on manual receipt/outcome data without IB integration. |
| Research Lab daily run | `READY_WITH_MANUAL_STEPS` | `run_research_lab_task_queue_v1.py` | Real dataset bindings remain incomplete; legacy `run_research_lab_v1.py` is compatibility-only. |
| Awareness report | `PRESENT_UNPROVEN` | Legacy/status report concepts exist | New canonical Research Lab daily awareness report is not proven. |
| Weekly sleeve/promotion review | `READY_WITH_MANUAL_STEPS` | Sleeve performance/evaluation, promotion CLI, and advisor benchmark CLI exist | Decisions remain human-only; no dedicated UI is proven. |

Scheduling:
- No new scheduling automation was added.
- The Research Lab daily run should remain an explicit offline command until real usage proves the cadence and dataset bindings.

## Remaining Blockers

1. Real promoted non-demo candidate path must be proven before first manual paper trade.
2. Current runtime must produce a non-empty actionable manual trade packet and operator execution queue.
3. Operator UI lacks manual receipt, event alert ledger, trade capture alert ledger, Research Lab, and promotion-candidate views.
4. Real paper-trade receipt/outcome/sleeve-performance lifecycle has not been proven from an actual IB paper fill.

## Manual Paper Smoke Readiness Answer

Aegis Lite is not ready for a real supervised IB paper smoke from current runtime truth today.

It is mechanically ready for supervised dry-run/operator validation with manual steps. The P0 offline proof shows the existing architecture can produce a promoted executable queue, manual packet, receipt, outcome ledger, sleeve performance report, and offline Research feedback without broker automation.

Before the first real supervised IB paper trade:
- confirm the 09:50 UTC and 14:50 UTC timer/status remains aligned,
- regenerate current Lite EOD artifacts with real promoted candidate input,
- prove a real promoted candidate can produce a complete manual trade packet and operator queue,
- keep maximum trade count at 1/day,
- require human supervision for entry, stop entry, receipt recording, and outcome attribution.
