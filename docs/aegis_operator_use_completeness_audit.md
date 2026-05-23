# Aegis Operator-Use Completeness Audit

Audit date: 2026-05-15

Scope: Aegis Lite, Research Lab, Event Awareness, Trade Capture Alerts, Manual Trade Packet, Manual Execution Receipt, Outcome Ledger, Sleeve Performance Report, Promotion Boundary, Operator Inbox, and Advisor Benchmark comparison.

This is an operator-use audit, not a schema/code existence audit. The question is whether David can complete each workflow without inspecting raw JSON, guessing command sequences, or accidentally treating dry-run/advisory artifacts as tradable.

Important basis note: the current working tree includes the new event-monitoring implementation (`run_aegis_event_monitor_v1.py`, `/aegis-events`, `event_rules_registry.v1`, `event_monitoring_status.v1`) but those files are not yet committed in this repo state.

## Workflow Classification

| # | Workflow | Classification | Operator-use verdict |
| --- | --- | --- | --- |
| 1 | Daily Aegis Lite EOD workflow | READY_WITH_MANUAL_STEPS | The canonical EOD path, 09:50 UTC and 14:50 UTC timer docs/status, manual packet generation, queue UI, and fail-closed behavior exist. Current runtime still has no real promoted executable candidate, so the workflow is usable for advisory/no-trade days but not yet proven for a real current trade. |
| 2 | Event monitoring workflow | PARTIAL | Event rules registry, monitor CLI, status artifact, ledger, and read-only `/aegis-events` surface exist in the working tree. Runtime scheduling, live market snapshot bindings, and operator proof on real data are not proven. |
| 3 | Trade capture alert workflow | DESIGNED_NOT_IMPLEMENTED | Alert gate and ledger are implemented, but actual email/SMS transport is not wired. The current output is message-body/dry-run evidence only, not an operator interruption channel. |
| 4 | Manual paper trade capture workflow | PARTIAL | Aegis Lite can produce a manual queue and manual trade packet. David can manually enter IB paper trades from the queue only when a current non-demo promoted executable candidate exists. That real runtime candidate path remains unproven. |
| 5 | Manual receipt entry workflow | READY_WITH_MANUAL_STEPS | `record_manual_execution_receipt_v1.py` records fills/stops/notes from flags and writes `manual_execution_receipt.v1` without broker access. It is CLI-only; no dashboard form is proven. |
| 6 | Outcome ledger workflow | READY_WITH_MANUAL_STEPS | `record_trade_outcome_v1.py` converts a receipt plus exit/outcome details into `outcome_ledger.v1`, calculating return and preserving sleeve/event lineage. It is CLI-only; no dashboard form is proven. |
| 7 | Sleeve performance review workflow | READY_WITH_MANUAL_STEPS | `build_and_print_sleeve_performance_report_v1.py` builds `sleeve_performance_report.v1` and prints an operator-readable summary. No operator UI panel is proven. Missing receipts/outcomes are not treated as zero return. |
| 8 | Research hypothesis capture workflow | READY_WITH_MANUAL_STEPS | Canonical `research_hypothesis.v1` intake exists via CLI, and Operator Inbox can capture pre-research ideas. It is not UI-first, but it is usable with explicit commands and safe roots. |
| 9 | Research test/review workflow | PARTIAL | Task queue, executor, evidence/result ledger, transition logic, and docs exist. Dataset bindings for real price/volatility/breadth/macro/regime testing remain incomplete, so real hypothesis testing is not yet operator-complete. |
| 10 | Promotion-to-sleeve workflow | READY_WITH_MANUAL_STEPS | `promote_validated_hypothesis_to_sleeve_v1.py` can install a human-approved promotion artifact into `promoted_sleeve_library.v1`. It remains CLI/artifact-based and does not create trades. |
| 11 | Operator Inbox workflow | READY_WITH_MANUAL_STEPS | Capture/review/promote-to-idea/report CLIs and docs exist. It is safe and low-friction for CLI use, but no UI panel exists. It cannot create tasks/trades/sleeves directly. |
| 12 | Advisor benchmark comparison workflow | READY_WITH_MANUAL_STEPS | `build_advisor_benchmark_v1.py` records advisor gross return, fee drag, net return, Aegis return, and the difference. It is artifact/CLI-only and not yet integrated into the Lite UI. |

## Incomplete Functionality Table

| Area | Severity | Operator impact | Recommended fix | Blocks manual paper trading? | Blocks hostile audit 90+? |
| --- | --- | --- | --- | --- | --- |
| No current real promoted executable candidate | CRITICAL | David cannot prove tomorrow's real EOD manual-trade workflow from current runtime truth. | Create/prove one non-demo human-approved promoted sleeve candidate through the canonical EOD path, or keep classification advisory-only. | Yes | Yes |
| Manual receipt entry is CLI-only | MEDIUM | David can avoid JSON, but still needs terminal commands after entering IB manually. | Add a dashboard form later; keep CLI as the current safe operator path. | No | Yes |
| Outcome ledger entry is CLI-only | MEDIUM | David can avoid hand-built JSON, but outcome entry still requires a command. | Add a dashboard form and missing-outcome work queue later. | No | Yes |
| Trade alert transport missing | HIGH | Aegis cannot actually interrupt David; actionable event packets must be manually checked. | Either wire explicit dry-run/live email transport behind alert gate or keep all surfaces labeled `MESSAGE_BODY_DRY_RUN_ONLY`. | No | Yes |
| Event monitor lacks live data binding proof | HIGH | Event rules may be correct but cannot be trusted on live day conditions. | Define and prove the market snapshot producer contracts for price, volatility, breadth, macro-event, and regime inputs. | No for EOD-only paper trading | Yes |
| Event monitor not scheduled/operationalized | MEDIUM | David must remember to run it manually, reducing practical value. | Add documented manual cadence first; schedule only after data bindings and alert transport are proven. | No | Yes |
| Sleeve performance has no UI panel | MEDIUM | David must inspect report JSON or command output to review sleeve performance. | Add read-only sleeve performance panel or include summary in Aegis Lite UI. | No | Yes |
| Research datasets incomplete | HIGH | Research Lab can process fixtures/tasks but cannot reliably validate real hypotheses. | Bind minimum price, volatility, breadth, macro calendar, regime labels, and outcome datasets. | No | Yes |
| Research testing remains command-heavy | MEDIUM | David can run it, but cannot easily see task backlog, blockers, or results. | Add operator read model/report for task queue, blockers, latest evidence, and result ledger. | No | Yes |
| Promotion-to-sleeve remains artifact-heavy | MEDIUM | A safe command exists, but David still needs to supply an approved promotion artifact. | Add a review checklist/report surface before routine promotions. | Yes for non-demo candidate creation | Yes |
| Advisor benchmark comparison is CLI-only | LOW | David can record the comparison, but it is not in the performance UI. | Add advisor benchmark section to the sleeve performance report/UI later. | No | No |
| Daily cadence docs mention old event commands | LOW | Operator may choose legacy event awareness commands instead of the new monitor. | Update cadence docs to prefer `run_aegis_event_monitor_v1.py`. | No | Yes |
| Dry-run/demo artifacts could be confused with real | HIGH | Operator may mistake proof artifacts for current tradable candidates. | Add stronger `DEMO_ONLY`, `DRY_RUN_ONLY`, and root/path warnings in UI/report summaries. | Yes if any demo queue is visible near trading | Yes |
| Hidden EOD candidate rules remain partly code/docs split | MEDIUM | David cannot fully inspect why candidates were selected or blocked from UI alone. | Surface candidate inclusion/exclusion, promoted sleeve match, blockers, and edge-overlap rules in the operator queue. | No | Yes |
| Hidden Research executor/data assumptions | MEDIUM | Research outcomes may look more mature than their data basis. | Add dataset availability/status to every result and Research daily report. | No | Yes |

## Workflow Findings

### 1. Daily Aegis Lite EOD Workflow

Status: READY_WITH_MANUAL_STEPS

What works:
- `run_aegis_lite_eod_pipeline_v1.py` writes EOD report, operator queue, edge cluster, overlap review, and manual trade packet.
- `/aegis-lite` and `/api/aegis/lite-execution-queue` render the current Lite queue fail-closed.
- Docs and status now describe 09:50 UTC and 14:50 UTC as canonical.

Incomplete from David's perspective:
- Current runtime has no real promoted executable queue item.
- The daily command is still operator-run unless the active systemd timer is trusted and observed.
- EOD packet details are not a first-class UI page separate from the queue.

Manual paper blocker: only if David expects to trade today. Advisory/no-trade operation is usable.

### 2. Event Monitoring Workflow

Status: PARTIAL

What works:
- `event_rules_registry.v1` makes event rules visible.
- `run_aegis_event_monitor_v1.py` requires explicit `--truth_root`.
- `event_monitoring_status.v1`, `event_awareness_ledger.v1`, tactical review gates, validity gates, alert gates, and read-only `/aegis-events` surfaces exist in the working tree.

Incomplete:
- No live market snapshot binding is proven.
- No schedule/cadence proof exists.
- Event monitor readiness is not yet represented in the main Lite readiness surface.

Manual paper blocker: no, as long as event flow is not relied upon for first EOD paper trade.

### 3. Trade Capture Alert Workflow

Status: DESIGNED_NOT_IMPLEMENTED

What works:
- Gate logic prevents alerts without validity `PASS`, complete packet fields, non-`EXTREME` sensitivity, and enough time remaining.
- Duplicate suppression and no-alert reasons are recorded.

Incomplete:
- No real email/SMS sender is wired to the trade capture alert gate.
- Current operator status must remain `MESSAGE_BODY_DRY_RUN_ONLY`.
- Older docs still mention `WOULD_SEND` in places and should be updated for consistency.

Manual paper blocker: no for EOD queue use; yes for relying on intraday event interruption.

### 4. Manual Paper Trade Capture Workflow

Status: PARTIAL

What works:
- Manual queue cards show symbol, side, quantity, entry, stop, risk, sleeve, edge cluster, blockers, and manual checklist when the EOD queue has candidates.
- Broker automation is deferred.

Incomplete:
- There is no current runtime proof with a non-demo executable promoted candidate.
- A UI distinction between current runtime candidates and `/tmp` proof artifacts must remain obvious.

Manual paper blocker: yes, until one current non-demo promoted candidate is generated or David explicitly performs a no-trade smoke.

### 5. Manual Receipt Entry Workflow

Status: READY_WITH_MANUAL_STEPS

What works:
- `manual_execution_receipt.v1` supports EOD packet, event packet, alert id, fill timestamp, fill price, quantity, stop details, valid-until compliance, slippage compliance, and operator notes.
- `record_manual_execution_receipt_v1.py` records the receipt with explicit `--truth_root` and validates that the source packet/trade exists.

Incomplete:
- The current operator path is a CLI, not a dashboard form.

Manual paper blocker: yes for any actual manual paper trade that needs valid measurement.

### 6. Outcome Ledger Workflow

Status: READY_WITH_MANUAL_STEPS

What works:
- `outcome_ledger.v1` exists and supports event/alert/source packet attribution.
- Outcome rows can feed Research Lab learning tasks.
- `record_trade_outcome_v1.py` records exits/outcomes from a receipt, calculates return, and preserves sleeve/event lineage.

Incomplete:
- No UI shows missing outcomes as an operator work queue.

Manual paper blocker: not for entering one trade, but yes for proving the full lifecycle.

### 7. Sleeve Performance Review Workflow

Status: READY_WITH_MANUAL_STEPS

What works:
- `build_sleeve_performance_report_v1.py` joins packets, receipts, outcomes, trade outcome attributions, promoted sleeve library, event packets, and alert ledgers.
- `build_and_print_sleeve_performance_report_v1.py` prints a concise operator-readable summary with sleeve return, trade counts, missing receipts/outcomes, slippage, stop behavior, and event/alert attribution.
- Missing receipt/outcome states are classified instead of treated as zero return.
- Research feedback is recommended/offline only.

Incomplete:
- The report is CLI/JSON artifact-first.
- No operator UI surface is proven for sleeve returns, missing receipts, missing outcomes, stop behavior, or alert usefulness.

Manual paper blocker: no for first trade; yes for routine review quality.

### 8. Research Hypothesis Capture Workflow

Status: READY_WITH_MANUAL_STEPS

What works:
- `ingest_research_hypotheses_v1.py` and `research_lab_register_hypothesis_v1.py` support explicit seed/manual hypothesis capture.
- `research_hypothesis.v1` is canonical for new work.
- Operator Inbox can capture casual ideas before Research.

Incomplete:
- No UI list/intake page is proven.
- AI-assisted generation is not autonomous and should not be advertised beyond explicit seed ingestion.

Manual paper blocker: no.

### 9. Research Test/Review Workflow

Status: PARTIAL

What works:
- `research_task_queue.v1` controls work.
- Offline executor writes evidence/result artifacts and can update hypothesis lifecycle.
- Failed/invalidated hypotheses are preserved.

Incomplete:
- Real dataset bindings are incomplete for price, volatility, breadth, macro events, regimes, and outcome data.
- Operator cannot easily inspect open tasks, blockers, stage, or result quality in a UI.
- Legacy runners and legacy hypothesis concepts remain visible and can confuse usage.

Manual paper blocker: no, unless Research is needed to create the first real promoted sleeve.

### 10. Promotion-To-Sleeve Workflow

Status: READY_WITH_MANUAL_STEPS

What works:
- `research_to_lite_promotion.v1` requires evidence/result lineage and human approval.
- `promoted_sleeve_library.v1` is the Lite boundary.
- Lite EOD filters to promoted sleeves.
- `promote_validated_hypothesis_to_sleeve_v1.py` installs only approved promotion-review or research-to-Lite promotion artifacts into the promoted sleeve library.

Incomplete:
- There is no clean operator approval UI.
- Building/updating the promoted sleeve library remains artifact/CLI oriented.
- A current non-demo promoted candidate has not been proven through runtime truth.

Manual paper blocker: yes for creating real executable candidate flow.

### 11. Operator Inbox Workflow

Status: READY_WITH_MANUAL_STEPS

What works:
- `aegis_operator_inbox_capture_v1.py` and `aegis_operator_inbox_review_v1.py` support capture, review, archive, reject, promote-to-idea, and reporting.
- Inbox items cannot create tasks, trades, or sleeves directly.

Incomplete:
- CLI-only.
- No inbox widget on the operator dashboard.
- Promotion-to-hypothesis depends on existing Research intake/ids and is not a one-screen guided flow.

Manual paper blocker: no.

### 12. Advisor Benchmark Comparison Workflow

Status: READY_WITH_MANUAL_STEPS

What works:
- Some generic benchmark and performance showcase code exists.
- `build_advisor_benchmark_v1.py` creates a fee-adjusted advisor benchmark artifact with Aegis return, advisor net return, difference, and fee drag.

Incomplete:
- No dedicated UI surface is present.
- The benchmark is not yet joined into `sleeve_performance_report.v1`.

Manual paper blocker: no.

## Hidden Business Rules And JSON-Only Risks

Hidden or partly hidden:
- EOD candidate selection and actionability logic are not fully visible in UI.
- Promotion acceptance criteria are documented and tested but not operator-guided.
- Research executor data sufficiency and result-quality logic are not easy to inspect from an operator surface.
- Sleeve performance feedback recommendation rules are in report logic, not a simple operator rules page.

JSON-only or CLI-only:
- Manual receipt entry is CLI-only.
- Outcome ledger creation is CLI-only.
- Promotion review/library updates.
- Research task review.
- Operator Inbox.
- Sleeve performance report review.
- Advisor benchmark comparison is CLI-only.

Dry-run/demo risks:
- `/tmp` P0 proof artifacts are valuable evidence but must never be displayed as current runtime candidates.
- Event alert output can look like a real alert body; labels must stay `MESSAGE_BODY_DRY_RUN_ONLY` until transport is real.
- Demo promoted sleeve libraries must remain visibly separate from current runtime truth.

## Top 10 Button-Up Priorities

1. Prove one current non-demo promoted executable candidate through canonical EOD runtime truth.
2. Add a single operator readiness/status page that shows EOD, event monitor, alert transport, receipts due, outcomes due, and sleeve report status.
3. Add read-only sleeve performance UI from `sleeve_performance_report.v1`.
4. Add dashboard forms for receipt and outcome entry; keep the CLIs as the safe fallback.
5. Add promotion review/operator approval UI for `research_to_lite_promotion.v1 -> promoted_sleeve_library.v1`.
6. Bind minimum real Research datasets: price, volatility, breadth, macro event calendar, regime labels, and outcome data.
7. Update daily cadence docs to prefer `run_aegis_event_monitor_v1.py` and mark older event commands as compatibility/manual primitives.
8. Either implement real gated email transport or keep all alert surfaces labeled `MESSAGE_BODY_DRY_RUN_ONLY`.
9. Integrate advisor benchmark output into the sleeve performance report/UI.
10. Add a Research Lab operator surface for open tasks, blockers, latest evidence, and result ledger summaries.

## Bottom Line

Aegis Lite + Research Lab is architecturally coherent and mostly fail-closed, but it is not yet fully operator-complete. The largest remaining gap is not safety; it is operator usability and proof. David can run advisory/EOD/report workflows with manual steps, but the full paper-trade loop still needs a non-demo promoted candidate plus receipt/outcome entry surfaces before it should be considered smooth enough for routine supervised paper trading.
