# Aegis Lite + Research Lab High-Level Functions

Date: 2026-05-15

This is the operator-facing functionality list after the Aegis Lite manual-paper pivot. It separates what is implemented, proven, designed, scaffolded, missing, and obsolete after the pivot. It does not authorize production activation, broker submit, IB transmit, fill automation, or automatic promotion.

## Verification Notes

- Repo timer intent: `ops/systemd/user/aegis-lite-eod-report-v1.timer` uses `15:50 America/New_York`.
- Active user systemd and current runtime operating status report `aegis-lite-eod-report-v1.timer` at `15:50 America/New_York`.
- Actual email/SMS transport for trade capture alerts is not proven. The current alert path writes gate/ledger artifacts and message bodies with `WOULD_SEND` / `NOT_SENT` delivery status.
- `sleeve_performance_report.v1` is implemented and tested as an offline artifact/CLI report. No dedicated Lite UI panel is proven.
- Daily Research cadence is documented and command-supported in pieces. It is not scheduled automation.
- Research Lab can run deterministic offline tasks using explicit inputs and optional fixture metrics. Full real dataset bindings for price, volatility, breadth, macro events, and regime labels are incomplete.
- Legacy AI hypothesis batch intake exists for `edge_hypothesis.v1`; it is not the canonical new `research_hypothesis.v1` lifecycle and does not generate ideas by itself.

## Aegis Lite Claim Review

| # | Original claim | Accuracy | Current classification | Corrected operator-facing language |
| --- | --- | --- | --- | --- |
| 1 | Performs one canonical governed EOD run near market close. | `ACCURATE` | `PROVEN_RUNTIME_TIMER` | Repo, active user systemd, and runtime operating status define one canonical Lite EOD run at 15:50 ET. Current output is advisory because no promoted executable candidates are present. |
| 2 | Evaluates promoted sleeves against market regime, volatility, breadth, event, and behavioral conditions. | `NEEDS_REWORDING` | `PARTIAL` | Aegis Lite filters for promoted sleeves and consumes regime/data/governance/candidate context. Volatility, breadth, event, and behavioral context exist as artifact fields or adjacent layers, but full real dataset binding is not proven. |
| 3 | Produces a complete manual trade packet for operator execution. | `ACCURATE` | `PROVEN_REPO` | The Lite EOD pipeline writes `manual_trade_packet.v1`; candidates fail closed when entry, stop, risk, sizing, sleeve, or source-hypothesis lineage is missing. Active runtime regeneration is still needed to prove current deployed output. |
| 4 | Supports event-awareness functionality for unusual market conditions. | `ACCURATE` | `PROVEN_REPO` | Event awareness can create event ledgers, tactical packets, validity gates, and alert gates in offline/manual-only form. |
| 5 | Event-awareness runs are non-canonical and cannot overwrite EOD state. | `ACCURATE` | `PROVEN_REPO` | Event artifacts are non-canonical and carry `canonical_eod_state_mutated=false`; tests cover separation from EOD/broker paths. |
| 6 | Supports trade-capture alert gating for valid manually executable trades, but confirm whether actual email/SMS delivery is implemented or only scaffolded. | `NEEDS_REWORDING` | `GATE_PROVEN_TRANSPORT_NOT_IMPLEMENTED` | Trade capture alert gating is implemented; real email/SMS sending is not proven. Current outputs are gate/ledger/message-body artifacts, not actual delivered notifications. |
| 7 | Trades are manually entered into IB by the operator. | `ACCURATE` | `DESIGNED_OPERATIONAL_RULE` | Aegis Lite is manual execution only. The system does not submit or transmit IB orders. |
| 8 | Records manual execution receipts, fills, slippage, stop behavior, and outcome attribution. | `NEEDS_REWORDING` | `READY_WITH_MANUAL_STEPS` | Schemas/builders support manual receipts, fill details, slippage, stop status, outcome ledgers, and attribution. The operator must provide/manual-record the evidence; no IB fill import is active. |
| 9 | Measures sleeve performance, event performance, trade performance, regime performance, and alert effectiveness. | `PARTIAL` | `PROVEN_ARTIFACT_UI_MISSING` | `sleeve_performance_report.v1` joins packet/receipt/outcome/attribution/event/alert evidence and computes sleeve, trade, regime, event, and alert-attributed metrics. It depends on supplied manual outcomes and has no proven dedicated UI. |
| 10 | Compares recommendations, manual executions, and resulting outcomes to improve future evaluation. | `ACCURATE` | `PROVEN_WITH_MANUAL_STEPS` | The performance report compares recommendation vs receipt vs outcome and recommends offline Research Lab follow-up tasks. It does not mutate Research queues automatically. |
| 11 | Is deterministic and fail-closed. | `ACCURATE` | `PROVEN_REPO` | Lite EOD, manual packet, event gates, alert gates, and performance report are deterministic and classify missing/invalid evidence as blocked or missing instead of inventing readiness. |
| 12 | Is designed for behavioral regime awareness, volatility-event opportunism, tactical decision support, and manual execution. | `ACCURATE_WITH_SCOPE` | `DESIGNED_PARTIAL_DATA_BINDING` | The design supports behavioral/regime/event-aware manual decision support. Real dataset bindings and live operational proof remain incomplete. |

## Corrected Aegis Lite Function List

1. Aegis Lite is the manual-paper operating path for governed EOD trade recommendations.
2. Repo, active user systemd, and current runtime status agree on one canonical EOD run at 15:50 ET.
3. Lite evaluates only promoted sleeve candidates; unpromoted Research ideas cannot directly enter the operational queue.
4. Lite writes an EOD report, operator execution queue, manual trade packet, edge cluster, overlap review, and operating status.
5. Manual trade packets are complete only when entry, side, symbol, sizing, stop, risk, sleeve, and source-hypothesis lineage are present.
6. Event awareness is non-canonical and cannot overwrite EOD state.
7. Event tactical packets must pass validity and alert gates before they can be considered interrupt-worthy.
8. Trade capture alert gate/ledger/message bodies exist; actual SMS/email delivery is not proven.
9. The operator manually enters any IB paper trade and immediately records stop/receipt evidence.
10. Aegis can measure recommendation vs manual receipt vs outcome when the operator supplies receipt/outcome data.
11. `sleeve_performance_report.v1` provides the canonical offline sleeve/trade/event/regime/alert-attributed performance report.
12. Lite remains deterministic, manual-only, non-broker, and fail-closed.

## Research Lab Claim Review

| # | Original claim | Accuracy | Current classification | Corrected operator-facing language |
| --- | --- | --- | --- | --- |
| 1 | Offline behavioral market research system used to discover, test, reject, validate, and promote hypotheses. | `NEEDS_REWORDING` | `IMPLEMENTED_PARTIAL` | Research Lab is an offline system for capturing, testing, preserving, rejecting, validating, and reviewing hypotheses. It does not autonomously discover or promote operational sleeves. |
| 2 | Supports manual hypothesis capture and registration. | `ACCURATE` | `PROVEN_REPO` | Manual intake/registration CLIs and durable hypothesis/task artifacts exist. |
| 3 | Supports AI-assisted hypothesis generation/expansion only if actually implemented; otherwise classify as designed/scaffolded/missing. | `PARTIAL` | `LEGACY_OR_SCAFFOLDED` | Legacy AI hypothesis batch intake validates AI-provided JSON into `edge_hypothesis.v1`. Canonical `research_hypothesis.v1` ingestion supports explicit seed items. No autonomous AI idea generation is proven. |
| 4 | Every hypothesis enters a governed research lifecycle. | `NEEDS_REWORDING` | `PROVEN_FOR_CANONICAL_PATH` | Canonical `research_hypothesis.v1` has lifecycle states and transition checks. Legacy hypothesis paths remain compatibility-only and should not be treated as the new source of truth. |
| 5 | Performs historical testing, event analysis, volatility-event research, behavioral regime analysis, edge-overlap analysis, and failure-mode analysis, to the extent datasets are bound. | `PARTIAL` | `EXECUTOR_PROVEN_DATASETS_INCOMPLETE` | The deterministic offline executor can process queued task types and write evidence/results using explicit required inputs and optional fixture metrics. Full real historical/event/volatility/breadth/regime dataset bindings are not complete. |
| 6 | Can reject, classify, or validate hypotheses, but cannot directly create trades or bypass governance. | `ACCURATE` | `PROVEN_REPO` | Research results can update hypothesis status and recommendation fields, but Research artifacts remain offline and non-executable. |
| 7 | Only promoted hypotheses become production-eligible sleeves inside Aegis Lite. | `NEEDS_REWORDING` | `PROVEN_BOUNDARY` | Only human-approved, implementation-approved promoted sleeve library entries can feed Lite candidates. Promotion review is separate from Research validation. |
| 8 | Learns from trade outcomes, sleeve failures, event results, alert quality, slippage, and regime performance if outcome-ledger learning is implemented. | `PARTIAL` | `IMPLEMENTED_WITH_MANUAL_HANDOFF` | Lite outcome attribution can be ingested into Research as evidence/results/tasks. The sleeve performance report recommends follow-up tasks but does not automatically enqueue them. |
| 9 | Remains offline and non-executable. | `ACCURATE` | `PROVEN_REPO` | Research Lab has no broker authority, no Lite runtime mutation authority, and no trade authorization authority. |

## Corrected Research Lab Function List

1. Research Lab is the offline evidence and hypothesis system behind Aegis.
2. It captures raw/manual/seeded ideas and canonical `research_hypothesis.v1` artifacts.
3. It queues offline research work through `research_task_queue.v1`; hypotheses do not execute themselves.
4. It writes evidence packets, result ledgers, conclusion/failure memory, and generated/index-only knowledge graph artifacts.
5. It preserves failed, rejected, invalidated, and contradicted hypotheses.
6. It can run deterministic offline task execution with explicit inputs and optional fixture metrics.
7. It does not yet have complete governed dataset bindings for real price, volatility, breadth, macro-event, and regime-history testing.
8. It can ingest Lite outcome attribution into Research learning without mutating Lite.
9. It can recommend or create offline Research follow-up work from outcome evidence, depending on the command path.
10. Promotion to Lite requires separate human-approved promotion and promoted sleeve library evidence.
11. Research artifacts cannot create trades, authorize allocation, submit orders, or bypass governance.

## Current Capability Classes

### Proven / Implemented

- Manual-only Lite EOD producer in repo.
- Promoted sleeve filtering.
- `manual_trade_packet.v1` generation and fail-closed validation.
- Event awareness, tactical packet, validity gate, trade capture alert gate, and alert ledger artifacts.
- Manual execution receipt schema/builder.
- Outcome ledger and trade outcome attribution artifacts.
- `sleeve_performance_report.v1` offline report and CLI.
- Research hypothesis/task/evidence/result lifecycle artifacts.
- Offline Research task runner with explicit inputs/fixture metrics.
- Lite outcome attribution ingestion into Research learning.

### Designed / Scaffolded / Partial

- Current actionable promoted-candidate proof. Timer/runtime alignment is 15:50, but current runtime remains advisory with no executable queue item.
- Actual operator UI for performance report, receipts, event alerts, Research hypotheses/results, and promotion candidates.
- AI-assisted Research intake into the new canonical hypothesis path. Legacy AI batch intake exists; canonical generation is not proven.
- Real Research dataset bindings for price, volatility, breadth, macro event calendar, and regime labels.
- Advisor benchmark comparison.
- Daily Research cadence. It is documented and command-supported in pieces, not scheduled automation.

### Missing

- Proven SMS/email transport for trade capture alerts.
- Dedicated Lite UI panel for `sleeve_performance_report.v1`.
- Lite UI/manual form for execution receipts.
- Complete non-IB automated mark/exit updater.
- Fee-adjusted advisor benchmark comparison.
- Full real dataset contracts for Research testing.

### Obsolete After Pivot

- Legacy autonomous PAPER timers and broker-submit lifecycle timers as an operating model.
- Legacy `run_research_lab_v1.py` / `run_aegis_research_lab_v1.py` as the default new Research path.
- Legacy `hypothesis_registry.v1` as the canonical new hypothesis source.
- Any Research artifact treated as directly executable.

## Do Not Claim Yet

- Do not claim Aegis is ready for real manual IB paper trading today.
- Do not claim the current 15:50 ET EOD output is actionable; current runtime remains advisory until a real promoted executable candidate exists.
- Do not claim Aegis sends real SMS/email trade alerts.
- Do not claim event awareness creates trades.
- Do not claim Research Lab has fully bound historical price/volatility/breadth/macro/regime datasets.
- Do not claim AI autonomously discovers, validates, or promotes strategies.
- Do not claim the sleeve performance report has a dedicated UI.
- Do not claim missing receipts or missing outcomes are measurable returns.
- Do not claim advisor benchmark comparison exists.
- Do not claim anything in Research can authorize trades, allocations, promoted sleeves, broker submit, or IB transmit.
