# Aegis Lite + Research Lab Functionality Footprint

Audit date: 2026-05-15

Scope: current repo plus read-only runtime evidence. No production activation, broker/IB automation, or trading paths were touched.

Status legend:
- `PROVEN`: code/schema exists and is covered by focused tests or current runtime artifacts.
- `PRESENT_UNPROVEN`: code/schema/artifact exists, but production-like proof, runtime artifact, UI exposure, or complete integration is missing.
- `DESIGNED_NOT_IMPLEMENTED`: documented or partially modeled, but no usable producer/UI/integration exists.
- `MISSING`: no equivalent implementation found.
- `OBSOLETE_AFTER_PIVOT`: legacy autonomous PAPER/runtime surface that should not define Aegis Lite after the manual EOD pivot.

Important caveat: the working repo is dirty and includes uncommitted Research/Event/Lite changes. Runtime active release evidence is therefore more important than repo-head intent where they disagree.

## Executive Finding

The Aegis Lite + Research Lab ecosystem now exists as a manual-only, non-IB-automated architecture in substantial form, but it is not fully complete.

The Lite EOD spine, operator execution queue, promoted-sleeve filtering, event tactical packet, event validity gate, trade capture alert gate, receipt/outcome artifacts, and offline Research Lab hypothesis/task/result loop are present. The current runtime also has a current Lite EOD report and operator queue.

The largest footprint gaps are:
- Current runtime is `ADVISORY_ONLY` because no real promoted executable candidate input is present.
- Current runtime queue/manual packet are empty, not a real promoted live-quality candidate proof.
- Event alerting writes alert gate/ledger artifacts but does not actually send email/SMS through a configured transport.
- Research Lab has no dedicated operator UI.
- Research dataset binding is fixture/instrument-level only, not real price/volatility/breadth/macro/regime data binding.
- Advisor benchmark / fee-adjusted comparison is missing.

## Aegis Lite Footprint

| Area | Status | Evidence | Notes |
| --- | --- | --- | --- |
| Repo-head EOD timer target | `PROVEN` | `ops/systemd/user/aegis-lite-eod-report-v1.timer` has `OnCalendar=*-*-* 09:50:00 UTC` and `OnCalendar=*-*-* 14:50:00 UTC`; `docs/aegis_lite_timer_model.md` says 09:50 UTC and 14:50 UTC. | Repo intent is 09:50 UTC and 14:50 UTC. |
| Active runtime EOD timer target | `PROVEN` | `systemctl --user status aegis-lite-eod-report-v1.timer` reports active timer at `09:50 UTC and 14:50 UTC`; current `aegis_lite_operating_status.v1` reports `target_times_utc=[09:50,14:50]`. | Repo, active systemd, and runtime status agree. No evidence of 15:55. |
| One official EOD run/day | `PRESENT_UNPROVEN` | Timer is a single systemd timer/service; service runs `run_current_release_tool_v1.sh run_aegis_lite_eod_pipeline_v1 ... --manual-only`. | Schedule is aligned, but full daily production-like proof still requires a promoted executable candidate. |
| Current Lite EOD report | `PROVEN` | `/home/node/constellation_runtime_data/truth/reports/aegis_lite_eod_report_v1/2026-05-15/.../aegis_lite_eod_report.v1.json`. | Current report is `ADVISORY_ONLY` / `NOT_READY` with missing candidate and promoted sleeve blockers. |
| Current operator execution queue | `PROVEN` | `/home/node/constellation_runtime_data/truth/reports/operator_execution_queue_v1/2026-05-15/.../operator_execution_queue.v1.json`. | Current queue exists, is manual-only/no broker, and has `execution_queue=[]`. |
| Lite EOD producer | `PROVEN` | `ops/tools/run_aegis_lite_eod_pipeline_v1.py`. | Writes overlap review, edge cluster, operator queue, promoted candidate set, EOD report, release integrity, legacy runtime status, and operating status. |
| Promoted-sleeve filtering | `PROVEN` | `filter_promoted_sleeve_candidates_v1` in `ops/tools/run_aegis_lite_eod_pipeline_v1.py`; tests in `test_aegis_lite_eod_v1.py`. | Unpromoted candidates are rejected; no promoted candidates produce advisory/not-ready payload. |
| `manual_trade_packet.v1` schema | `PROVEN` | `governance/04_DATA/SCHEMAS/C2/REPORTS/manual_trade_packet.v1.schema.json`. | Requires symbol, side, entry reference, sizing, stop, risk, manual-only, broker-submit false. |
| `manual_trade_packet.v1` builder/tests | `PROVEN` | `build_manual_trade_packet_v1` in `constellation_2/common/aegis_research_lab_v1.py`; tests in `test_aegis_research_lab_v1.py` and `test_research_hypothesis_architecture_v1.py`. | Blocks incomplete/unpromoted candidates. |
| Official EOD `manual_trade_packet.v1` generation command/output | `PROVEN` | `ops/tools/run_aegis_lite_eod_pipeline_v1.py` writes `reports/manual_trade_packet_v1/<day>/<run_id>/manual_trade_packet.v1.json`; current runtime packet exists. | Current packet is empty because no promoted executable candidates are present. |
| Operator execution queue schema | `PROVEN` | `operator_execution_queue.v1.schema.json`. | Requires manual execution recipe, required orders, stop-required flag, queue status, reason codes, broker-submit false. |
| Queue fail-closed behavior | `PROVEN` | `build_operator_execution_queue_v1` checks unsupported structure, quantity, symbol, direction, instrument, entry, stop, risk, executable status. | Missing entry/stop/risk/quantity blocks readiness. |
| Edge clustering | `PROVEN` | `build_edge_cluster_v1`; current runtime `edge_cluster.v1` exists. | Deterministic grouping by thesis/edge/regime/direction/risk tags. Quality is basic but usable. |
| Sleeve edge overlap review | `PROVEN` | `build_sleeve_edge_overlap_review_v1`; current runtime overlap review exists. | Identifies distinct edges, duplicate thesis, correlated exposure, concentration warnings. |
| Event awareness ledger | `PROVEN` | `event_awareness_ledger.v1.schema.json`, `build_event_awareness_ledger_v1`, `run_event_awareness_v1.py`, focused tests. | No current runtime event ledgers found under canonical runtime truth. |
| Event triggers | `PRESENT_UNPROVEN` | Enum supports `PANIC_EXHAUSTION`, `RECOVERY_FAILURE`, `FAILED_BREAKOUT`, `BREADTH_COLLAPSE`, `DEFENSIVE_ROTATION`, `VOLATILITY_SPIKE`, `MACRO_EVENT_REACTION`. | CLI records explicit operator/event input; no automated detector from market data is implemented. |
| Event tactical packet | `PROVEN` | `event_tactical_packet.v1.schema.json`, `build_event_tactical_packet_v1`, `run_event_tactical_review_v1.py`, tests. | Non-canonical, manual-only, no broker/transmit/runtime mutation. |
| Event validity gate | `PROVEN` | `event_validity_gate.v1.schema.json`, `build_event_validity_gate_v1`, `run_event_validity_gate_v1.py`, tests. | Blocks missing valid_until, stop, slippage, risk, sizing, stale packets, EXTREME sensitivity, and price outside slippage. |
| Trade capture alert gate | `PROVEN` | `trade_capture_alert_gate.v1.schema.json`, `build_trade_capture_alert_gate_v1`, `run_trade_capture_alert_gate_v1.py`, tests. | Allows email/SMS only for validity `PASS`, complete packet, non-EXTREME sensitivity, and enough time remaining. |
| Trade capture alert ledger | `PROVEN` | `trade_capture_alert_ledger.v1.schema.json`, `build_trade_capture_alert_ledger_v1`, tests and `/tmp` smoke. | Records allowed and blocked attempts, duplicate SMS suppression, no-alert reasons. |
| Actual email/SMS transport | `DESIGNED_NOT_IMPLEMENTED` | Alert gate builds SMS/email bodies and records `WOULD_SEND`; no send transport is wired in `run_trade_capture_alert_gate_v1.py`. | Existing generic operator alert infrastructure has email/SMS config concepts, but trade capture alert v1 does not call it. |
| Alerts only for manually executable trades | `PROVEN` | Alert gate requires event validity `PASS` plus entry, sizing, stop/stop logic, risk, slippage, valid_until, sensitivity timing. | This is proven at the gate level, not through live transport. |
| Manual operator decision | `PROVEN` | `build_manual_operator_decision_v1`, schema, tests. | Observational only, broker-submit false. |
| Manual execution event | `PROVEN` | `build_manual_execution_event_v1`, schema, tests. | Records actual entry/stop details; optional IB ids are observational. |
| Manual execution receipt | `PROVEN` | `manual_execution_receipt.v1.schema.json`, `build_manual_execution_receipt_v1`, tests. | Supports `source_packet_type=EOD_MANUAL_PACKET` or `EVENT_TACTICAL_PACKET`, plus alert/event/fill/slippage/valid_until fields. |
| Operator records exit | `PRESENT_UNPROVEN` | Outcome ledger and trade outcome attribution support actual exit/PnL fields; manual receipt schema records fill/stop, not full exit lifecycle. | Exit capture without IB is modeled through outcome rows, not a dedicated receipt command. |
| `trade_outcome_attribution.v1` | `PROVEN` | `trade_outcome_attribution.v1.schema.json`, `build_trade_outcome_attribution_v1`, tests. | Measures recommendation vs actual entry/exit, model forward returns, realized/unrealized PnL, MAE/MFE, slippage, skipped trade outcome, governance/operator quality. |
| `outcome_ledger.v1` | `PROVEN` | `outcome_ledger.v1.schema.json`, `build_outcome_ledger_v1`, tests. | Generic row schema; event/alert usefulness fields are builder-level conventions, not tightly enforced by schema. |
| Performance measurement without IB | `PRESENT_UNPROVEN` | Manual feedback builders accept manual market outcomes/decisions and do not require IB. | Works with provided outcome inputs; no proven automated non-IB price mark/outcome updater for Lite candidates. |
| Missed/ignored trade tracking | `PROVEN` | Manual decisions marked skipped/watchlist/rejected produce `skipped_trade_outcome=PENDING_FORWARD_ANALYSIS`; Research learning tasks can be generated from missed opportunity rows. | Requires later market outcome input. |
| Sleeve performance control | `PROVEN` | `run_sleeve_performance_control_v1.py`, `sleeve_performance_control.v1.schema.json`, `test_sleeve_performance_control_v1.py`. | Consumes existing attribution/outcome/edge/scorecard/regime/missed/risk artifacts and blocks allocation when evidence is missing. |
| Sleeve scores/rankings | `PRESENT_UNPROVEN` | `sleeve_performance_control` consumes `weekly_scorecard_view_v1`; UI has separate sleeve readiness/grade surfaces. | Lite report has `sleeve_performance_summary` field but EOD pipeline only passes input through; no native Lite ranking calculation found. |
| Percentage returns | `PRESENT_UNPROVEN` | Legacy `trade_outcome_v1` examples contain `return_pct`; `trade_outcome_attribution.v1` stores PnL strings and forward returns. | No canonical Lite percentage return/ranking report found. |
| Regime performance | `PRESENT_UNPROVEN` | Sleeve performance control consumes `regime_confidence_v1`; Research evidence/result metrics include regime expectancy conventions. | No proven Lite sleeve-by-regime performance table. |
| Edge-overlap performance | `PRESENT_UNPROVEN` | Edge clusters and overlap review exist; outcome rows include `edge_overlap_attribution`. | No dedicated aggregate edge-overlap performance report/UI found. |
| Advisor benchmark comparison | `MISSING` | Search found no fee-adjusted advisor-return comparison implementation. | No fee-based advisor benchmark view or artifact found. |

## Operator UI Footprint

| Surface | Status | Evidence | Notes |
| --- | --- | --- | --- |
| `/aegis-lite` route / execution queue page | `PROVEN` | `aegis_lite_execution_queue_read_model.py`; `pages/index.js` renders "Aegis Lite Execution Queue"; UI degraded-mode tests pass. | Reads current Lite operating status/report/queue, not stale operator state. |
| EOD packet/report view | `PRESENT_UNPROVEN` | `/aegis-lite` displays report path, generated time, readiness, queue cards. | It does not render the full EOD report as a standalone report view. |
| Event alerts view | `MISSING` | No UI route/read model for `event_awareness_ledger`, `event_tactical_packet`, or `trade_capture_alert_ledger` found. | Event/alert artifacts are CLI/read-only JSON surfaces today. |
| Sleeve score/ranking view | `PRESENT_UNPROVEN` | Existing operator shell has sleeve readiness/grade and sleeve evaluation routes; not clearly tied to Lite queue/report. | Not a Lite-native sleeve score/ranking view. |
| Sleeve return view | `PRESENT_UNPROVEN` | Operator shell outcome/value surfaces exist; no Lite-specific sleeve return table found. | Percentage return by sleeve remains incomplete. |
| Manual receipt status view | `MISSING` | No UI route/read model found for `manual_execution_receipt.v1`. | Operator can inspect JSON only. |
| Outcome ledger view | `PRESENT_UNPROVEN` | Operator shell has generic outcomes route; no direct `outcome_ledger.v1` or `trade_outcome_attribution.v1` Lite page found. | Not sufficient for complete Lite outcome review. |
| Advisor benchmark comparison view | `MISSING` | No matching UI or backend implementation found. | Missing artifact and UI. |
| Release mismatch warning in Lite UI | `PROVEN` | UI read model downgrades to `ADVISORY_ONLY` and blocks executable cards on mismatch; tests cover this. | Current runtime release status reports `MATCH`. |
| Degraded missing-artifact handling | `PROVEN` | UI read model catches missing/malformed report/queue/status and writes `aegis_ui_runtime_status.v1`; tests cover missing/malformed queue/report. | Prevents 502-style failures for missing Lite artifacts. |

## Research Lab Footprint

| Area | Status | Evidence | Notes |
| --- | --- | --- | --- |
| Canonical `research_hypothesis.v1` | `PROVEN` | Schema, `build_research_hypothesis_v1`, `ingest_research_hypotheses_v1.py`, tests. | Durable first-class hypothesis object for new work. |
| Legacy `hypothesis_registry.v1` | `OBSOLETE_AFTER_PIVOT` | `research_lab_register_hypothesis_v1.py`, `build_hypothesis_registry_v1`, docs say compatibility only. | Still functional and has duplicate/test-plan behavior, but should not be canonical for new work. |
| Manual hypothesis intake | `PROVEN` | `ingest_research_hypotheses_v1.py` writes explicit seed rows/text to `research_hypothesis.v1`. | Does not infer from chat memory. |
| Legacy registry intake with duplicate warnings | `PRESENT_UNPROVEN` | `research_lab_register_hypothesis_v1.py` outputs `duplicate_warning`, related ids, overlap reason codes, test plan, task queue. | Useful compatibility path, but conflicts with canonical model if treated as source of truth. |
| Hypothesis id | `PROVEN` | New ingest assigns deterministic-looking `rh-<date>-<ordinal>-<slug>` unless provided; legacy registry assigns `hyp-...`. | Two id styles exist due old/new coexistence. |
| Lifecycle states | `PROVEN` | `research_hypothesis.v1` supports uppercase states; legacy registry supports lowercase lifecycle states. | Mixed state semantics remain a concept-risk. |
| Research inbox items | `PROVEN` | `research_inbox_item.v1` schema and builder; tests cover explicit conversion. | Raw ideas are not hypotheses and cannot execute/promote. |
| Research programs | `PROVEN` | `research_program.v1` schema and builder. | Organizes only; non-authoritative. |
| Research task queue | `PROVEN` | `research_task_queue.v1`, `build_research_task_queue_v1`, `run_research_lab_task_queue_v1.py`, tests. | New queue uses uppercase task types and deterministic ordering. |
| Test plan | `OBSOLETE_AFTER_PIVOT` | `hypothesis_test_plan.v1` and legacy runner remain. | New design uses task queue/evidence/result, not legacy test-plan as canonical. |
| Offline deterministic runner | `PROVEN` | `run_research_lab_task_queue_v1.py` processes queued tasks into evidence, result ledger, updated hypothesis, and next task. | Uses explicit hypothesis artifacts and optional fixture metrics. |
| Legacy Research runner | `OBSOLETE_AFTER_PIVOT` | `run_research_lab_v1.py` emits placeholder `insufficient_data` experiment results from legacy registry/test plan. | Should not be used for new work. |
| Automatic next-stage task generation | `PROVEN` | New task runner adds `next_recommended_task`; tests cover queue advancement. | Basic deterministic transitions only. |
| Evidence packets | `PROVEN` | `research_evidence_packet.v1`, builder, runner output, tests. | Captures metrics, methodology, lineage, data refs, limitations. |
| Result ledger | `PROVEN` | `research_result_ledger.v1`, builder, runner output, tests. | Preserves failed/invalidated hypotheses. |
| Research conclusions | `PROVEN` | `research_conclusion.v1` schema/builder/tests require evidence and result refs; supersession preserves old conclusion. | No automated conclusion writer from runner found. |
| Failure archetypes | `PROVEN` | `research_failure_archetype.v1` schema/builder/tests. | Advisory memory only, no governance mutation. |
| Knowledge graph | `PRESENT_UNPROVEN` | `research_knowledge_graph.v1` schema/builder; docs say generated/index-only. | Scaffold/index exists; no advanced graph logic and no runtime authority. |
| Taxonomy discipline | `PROVEN` | `edge_taxonomy.v1`, taxonomy warning helpers, integrity review tests. | Warnings for duplicate/deprecated/conflicting concepts; not a complete semantic ontology. |
| Architecture integrity review | `PROVEN` | `research_architecture_integrity_review_v1.py`, schema, tests. | Detects missing lineage, unsafe promotion, legacy canonical drift, broker/runtime coupling. |
| Promotion gate | `PROVEN` | `research_to_lite_promotion.v1`, `build_research_to_lite_promotion_v1`, tests. | Requires evidence refs, result ledger refs, validated research status, lineage, human approval for implementation. |
| Promoted sleeve library boundary | `PROVEN` | `promoted_sleeve_library.v1`, promoted-sleeve filtering in EOD pipeline. | Lite consumes only approved promoted sleeves/candidates. |
| No automatic production influence | `PROVEN` | Research artifacts carry `runtime_mutation_allowed=false`, `broker_submit_required=false`, `automatic_lite_promotion_allowed=false`; tests cover no Lite mutation. | Boundary is strong at artifact/build level. |
| Lite feedback ingestion | `PROVEN` | `ingest_trade_outcome_attribution_to_research_v1.py`, `build_learning_tasks_from_outcomes_v1`, tests. | Creates research result/evidence/follow-up tasks without Lite mutation. |
| Aegis-generated hypothesis source: sleeve failure | `PROVEN` | Outcome-ledger learning task source `outcome_ledger_sleeve_underperformance`. | Creates follow-up tasks, not new full hypotheses. |
| Aegis-generated hypothesis source: missed opportunity | `PROVEN` | Outcome-ledger source `outcome_ledger_missed_opportunity`. | Follow-up task path exists. |
| Aegis-generated hypothesis source: event anomaly | `PROVEN` | Event failure/success/stale/false-positive/overlap review task sources exist. | Follow-up task path exists. |
| Aegis-generated hypothesis source: regime drift | `PROVEN` | Outcome-ledger source `outcome_ledger_regime_dependency`. | Follow-up task path exists. |
| Aegis-generated hypothesis source: edge-overlap failure | `PROVEN` | Outcome-ledger source `outcome_ledger_correlated_loss` and event overlap review. | Follow-up task path exists. |
| Aegis-generated hypothesis source: outcome anomaly | `PROVEN` | Outcome-ledger `anomaly_review` path for missed opportunity/anomalies. | Follow-up task path exists. |
| Automatic creation of new hypotheses from Aegis outcomes | `DESIGNED_NOT_IMPLEMENTED` | Feedback creates result/evidence/task/inbox where appropriate, not full hypotheses. | This is likely correct safety posture; explicit conversion should remain required. |
| Dataset binding: price data | `PRESENT_UNPROVEN` | Broader repo has market data snapshot systems; Research runner only uses optional `--fixture_json` and hypothesis instruments. | No direct Research Lab binding to governed price history found. |
| Dataset binding: volatility data | `MISSING` | Research runner metrics can include volatility-like fields only through fixture metrics. | No Research Lab volatility dataset adapter found. |
| Dataset binding: breadth data | `MISSING` | No Research Lab breadth dataset adapter found. | Needed for breadth hypotheses. |
| Dataset binding: macro event calendar | `MISSING` | No Research Lab macro calendar binding found. | Needed for CPI/Fed/event hypotheses. |
| Dataset binding: regime labels | `PRESENT_UNPROVEN` | Research evidence stores regimes; other repo regime artifacts exist. | No direct Research Lab regime-label dataset adapter found. |
| Dataset binding: outcome data | `PROVEN` | `ingest_trade_outcome_attribution_to_research_v1.py` imports Lite outcome attribution. | Operational outcomes can feed research. |
| Experiment/result artifact | `PROVEN` | New `research_result_ledger.v1`; legacy `research_experiment_result.v1` and `experiment_result.v1` also exist. | New result ledger is canonical; old experiment artifacts are compatibility/legacy. |
| Stage advancement / no skipped stages | `PRESENT_UNPROVEN` | New runner advances via `next_recommended_task`; legacy test plan has stage logic. | No full formal multi-stage lifecycle proof across all stages found. |
| Insufficient data behavior | `PROVEN` | New runner returns `INSUFFICIENT_DATA` when no instruments/sample; legacy runner returns placeholder insufficient data. | New behavior is deterministic but data binding is shallow. |
| Research UI: hypothesis list | `MISSING` | No Research Lab UI route/read model found. | CLI/JSON only. |
| Research UI: test plan/stage | `MISSING` | No Research Lab UI route/read model found. | CLI/JSON only. |
| Research UI: experiment/results | `MISSING` | No Research Lab UI route/read model found. | CLI/JSON only. |
| Research UI: blockers/duplicates | `MISSING` | No Research Lab UI route/read model found. | CLI/JSON only. |
| Research UI: promotion candidates | `MISSING` | No Research Lab UI route/read model found. | CLI/JSON only. |
| Research UI: awareness report | `MISSING` | Legacy awareness report artifact exists, but no UI route found. | CLI/JSON only. |

## Missing Functionality Table

| Missing / Incomplete Item | Classification | Why It Matters |
| --- | --- | --- |
| Real promoted non-demo executable candidate proof | `PRESENT_UNPROVEN` | Current runtime queue and manual packet are empty because no promoted executable candidate input is present. |
| Actual email/SMS delivery for trade capture alerts | `DESIGNED_NOT_IMPLEMENTED` | Alert gate/ledger are proven, but transport is not wired. |
| Event awareness automated market detector | `DESIGNED_NOT_IMPLEMENTED` | Event CLI accepts explicit event data; no deterministic detector from live/intraday market conditions found. |
| Event/alert UI view | `MISSING` | Operator cannot review event alert ledger/tactical packets from UI. |
| Manual receipt UI | `MISSING` | Operator cannot record/review receipt from UI. |
| Lite outcome ledger UI | `PRESENT_UNPROVEN` | Generic outcomes exist, but no Lite outcome-ledger page found. |
| Advisor benchmark / fee-adjusted return comparison | `MISSING` | No artifact, producer, or UI found. |
| Research Lab UI | `MISSING` | Hypotheses, tasks, results, blockers, duplicates, promotions are CLI/JSON only. |
| Research price/volatility/breadth/macro dataset adapters | `MISSING` | Needed before real hypothesis testing for the seeded strategy ideas. |
| Lite-native sleeve percentage return/ranking/regime/edge-overlap performance report | `PRESENT_UNPROVEN` | Related performance controls exist, but no complete Lite operator-facing performance scorecard found. |

## Obsolete Automation Items After Pivot

| Item | Status | Evidence |
| --- | --- | --- |
| `c2-paper-day-orchestrator.timer` | `OBSOLETE_AFTER_PIVOT` | Runtime operating status marks DEFERRED; systemd list shows no NEXT trigger. |
| `aegis-paper-ready-kernel-v1.timer` | `OBSOLETE_AFTER_PIVOT` | Runtime operating status marks DEFERRED. |
| `aegis-paper-ready-kernel-v1-after-orchestrator.timer` | `OBSOLETE_AFTER_PIVOT` | Runtime operating status marks DEFERRED. |
| `c2-paper-auto-repair-controller.timer` | `OBSOLETE_AFTER_PIVOT` | Runtime operating status marks DEFERRED; systemd list shows no NEXT trigger. |
| `c2-paper-auto-repair-eod-final.timer` | `OBSOLETE_AFTER_PIVOT` | Runtime operating status marks DEFERRED. |
| Legacy `run_research_lab_v1.py` placeholder runner | `OBSOLETE_AFTER_PIVOT` | Uses legacy registry/test plan and emits placeholder insufficient-data experiment results. New runner is `run_research_lab_task_queue_v1.py`. |
| Legacy `hypothesis_registry.v1` as canonical source | `OBSOLETE_AFTER_PIVOT` | Docs and integrity review treat it as compatibility only; `research_hypothesis.v1` is canonical for new work. |

## Recommended Next Implementation Priorities

1. Produce one real supervised paper-trading candidate path through `promoted_sleeve_library.v1`, still manual-only.
2. Regenerate current Lite EOD artifacts with a non-empty actionable `operator_execution_queue.v1` and `manual_trade_packet.v1`.
3. Verify the `/aegis-lite` UI renders that current queue without stale/demo confusion.
4. Add Research Lab dataset adapters for price history, volatility/VIX, breadth, macro events, regime labels, and outcome joins.
5. Add Research Lab UI read models for hypotheses, task queues, results, blockers, duplicate warnings, and promotion candidates.
6. Add Lite UI pages or panels for event alerts, manual receipts, and Lite outcome attribution.
7. Wire trade capture alert delivery to an explicit configured email/SMS transport, keeping the current alert gate as the only send authority.
8. Add advisor benchmark and fee-adjusted comparison artifact only after Lite outcome/return measurement is reliable.

## Explicit Answer

Does the full Aegis Lite + Research Lab ecosystem now exist without IB automation?

Partially.

The core manual-only ecosystem exists without IB automation: Lite EOD report/queue, promoted-sleeve boundary, event tactical/validity/alert gates, manual receipt/outcome artifacts, and offline Research Lab hypothesis/task/evidence/result/promotion boundaries are present and mostly proven.

The full ecosystem does not yet exist end-to-end because current runtime has no real promoted executable queue or manual packet candidate, actual alert transport is not wired, Research dataset bindings are incomplete, Research UI is missing, and advisor benchmark comparison is missing.
