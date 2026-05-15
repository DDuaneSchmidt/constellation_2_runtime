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
- Repo timer/source code, active user systemd, and current runtime operating status now use the canonical 15:50 ET EOD time.
- Current runtime queue/report exists, but no promoted executable candidate is present.
- Current runtime is `ADVISORY_ONLY`, with no actionable manual trade packet or executable operator queue item.

Safe dry-run/operator validation readiness: `READY_WITH_MANUAL_STEPS`.

## Timer Status

Status: `READY`

Repo/configured intent:
- `ops/systemd/user/aegis-lite-eod-report-v1.timer` uses `OnCalendar=Mon..Fri *-*-* 15:50:00 America/New_York`.
- `constellation_2/common/aegis_lite_operating_status_v1.py` now treats 15:50 ET as the canonical expected Lite EOD time.
- `docs/aegis_lite_timer_model.md` documents 15:50 ET.

Active runtime evidence:
- `systemctl --user status aegis-lite-eod-report-v1.timer` reports the active user timer at 15:50 ET.
- Current runtime `aegis_lite_operating_status.v1` reports `target_time_et=15:50`.
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
- Current runtime executable queue is demo/dry-run. A real promoted sleeve candidate has not yet been proven end-to-end in active runtime.

## Event Alert Transport

Status: `PRESENT_UNPROVEN`

Implemented:
- `event_awareness_ledger.v1`
- `event_tactical_packet.v1`
- `event_validity_gate.v1`
- `trade_capture_alert_gate.v1`
- `trade_capture_alert_ledger.v1`

Alert safety:
- Email/SMS is allowed only when the event validity gate is `PASS`, the source packet is complete, sensitivity is not `EXTREME`, and enough time remains before `valid_until`.
- Blocked/expired/invalid/duplicate packets write no-alert ledger entries.

Transport finding:
- The trade capture alert CLI writes gate/ledger artifacts and message bodies.
- It does not currently send real email/SMS.
- Later enablement should wire a configured transport behind `trade_capture_alert_gate.v1` only, preserving the gate as send authority.

## Manual Execution Receipt

Status: `READY`

Implemented:
- `manual_execution_receipt.v1` schema and builder support EOD manual packet and event tactical packet sources.
- Receipt fields include alert id, source packet id, event id/run id, fill timestamp, fill price, quantity, order type, stop entered, stop price, operator notes, deviation from recommendation, valid-until compliance, and max-slippage compliance.
- Receipt is observational only and requires no broker submit.

Manual step:
- Operator still records the receipt outside a dedicated Lite UI form.

## Outcome Ledger

Status: `READY_WITH_MANUAL_STEPS`

Implemented:
- `trade_outcome_attribution.v1` records recommendation vs actual entry/exit, realized/unrealized PnL, model forward returns, MAE/MFE, operator slippage, skipped trade outcome, governance adjustment effect, sleeve signal quality, implementation quality, and operator execution quality.
- `outcome_ledger.v1` records generic outcome rows and can include alert id, event attribution, source packet type, execution sensitivity, validity-window compliance, slippage compliance, operator action, and alert usefulness.
- `sleeve_performance_report.v1` joins manual packets, receipts, outcomes, optional attributions, promoted sleeve lineage, event packets, and alert ledgers into the canonical Lite paper-performance report.
- Outcome rows can generate offline Research Lab follow-up tasks without Lite mutation.

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
| Event alert status | `MISSING` | No dedicated UI surface for event awareness ledger/tactical packets. |
| Trade capture alert ledger | `MISSING` | CLI/JSON only. |
| Sleeve scores/rankings | `READY_WITH_MANUAL_STEPS` | `sleeve_performance_report.v1` now aggregates sleeve counts, returns, win rate, stop-hit rate, MAE/MFE, regime/event/alert performance; no Lite UI panel is proven. |
| Sleeve percent returns | `READY_WITH_MANUAL_STEPS` | `sleeve_performance_report.v1` computes sleeve total/average percent returns from closed outcome rows; missing receipts/outcomes are not treated as zero return. |
| Manual receipts needed/status | `MISSING` | Receipt artifact exists; no Lite UI form/status panel found. |
| Outcome ledger | `READY_WITH_MANUAL_STEPS` | `sleeve_performance_report.v1` reads outcome ledger rows; no dedicated Lite outcome-ledger UI is proven. |
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
- The documented cadence covers pre-market Research review, intraday event awareness, the 15:50 ET Lite EOD run, after-close manual receipt/outcome updates, the offline Research Lab daily run, and weekly review.

Command/status coverage:

| Cadence step | Status | Existing command support | Blocked or manual-only notes |
| --- | --- | --- | --- |
| Pre-market hypothesis/task review | `READY_WITH_MANUAL_STEPS` | `list_research_hypotheses_v1.py`, `research_lab_register_hypothesis_v1.py`, `research_architecture_integrity_review_v1.py` | Review is CLI/JSON; no Research Lab UI is proven. |
| Prior outcome review | `READY_WITH_MANUAL_STEPS` | Outcome/trade-outcome tools exist; Research import uses `ingest_trade_outcome_attribution_to_research_v1.py` | Missing receipts/outcomes require operator evidence. |
| Intraday event awareness | `PRESENT_UNPROVEN` | `run_event_awareness_v1.py`, `run_event_tactical_review_v1.py`, `run_event_validity_gate_v1.py`, `run_trade_capture_alert_gate_v1.py` | Event UI and real email/SMS transport are not proven. |
| Near-close canonical Lite EOD | `READY_WITH_MANUAL_STEPS` | `run_aegis_lite_eod_pipeline_v1.py` | Repo, active systemd, and runtime status agree on 15:50 ET; current output remains advisory because promoted executable candidates are missing. |
| Manual execution receipt update | `READY_WITH_MANUAL_STEPS` | Receipt schema/builder exist | Operator entry is manual; no Lite receipt UI form is proven. |
| Outcome ledger update | `READY_WITH_MANUAL_STEPS` | `run_outcome_attribution_v1.py`, `run_trade_outcome_v1.py`, `ingest_trade_outcome_attribution_to_research_v1.py` | Performance measurement depends on manual receipt/outcome data without IB integration. |
| Research Lab daily run | `READY_WITH_MANUAL_STEPS` | `run_research_lab_task_queue_v1.py` | Real dataset bindings remain incomplete; legacy `run_research_lab_v1.py` is compatibility-only. |
| Awareness report | `PRESENT_UNPROVEN` | Legacy/status report concepts exist | New canonical Research Lab daily awareness report is not proven. |
| Weekly sleeve/promotion review | `PRESENT_UNPROVEN` | Sleeve performance/evaluation and promotion manual review tools exist | Advisor benchmark comparison is missing; decisions remain human-only. |

Scheduling:
- No new scheduling automation was added.
- The Research Lab daily run should remain an explicit offline command until real usage proves the cadence and dataset bindings.

## Remaining Blockers

1. Real promoted non-demo candidate path must be proven before first manual paper trade.
2. Current runtime must produce a non-empty actionable manual trade packet and operator execution queue.
3. Operator UI lacks manual receipt, event alert ledger, trade capture alert ledger, Research Lab, and promotion-candidate views.
4. Real paper-trade receipt/outcome/sleeve-performance lifecycle has not been proven from an actual IB paper fill.

## Readiness Answer

Aegis Lite is not ready for real manual paper trading today.

It is ready for supervised dry-run/operator validation with manual steps, using the existing report/queue and no broker automation.

Before the first real supervised IB paper trade:
- confirm the 15:50 ET timer/status remains aligned,
- regenerate current Lite EOD artifacts with real promoted candidate input,
- prove a real promoted candidate can produce a complete manual trade packet and operator queue,
- keep maximum trade count at 1/day,
- require human supervision for entry, stop entry, receipt recording, and outcome attribution.
