# Aegis Performance Capability Audit

## Executive Summary

This audit found substantial existing performance-related functionality. Aegis already has an event-sourced simulated paper position ledger, derived paper P&L report, daily paper performance report, sleeve performance truth read model, candidate lifecycle projection, benchmark/showcase support, advisor benchmark CLI artifact, diagnostics, and UI consumers.

The safest next implementation step is **Option A: expose existing performance data in a new or improved UI**, using the already-declared read models. Do not build a new performance accounting layer. The primary gap is not raw accounting; it is operator-facing integration, benchmark/advisor joins, and consistent authority boundaries.

Runtime truth context from `npm run aegis:audit` on 2026-05-28: `runtime_truth_classification=PARTIAL_CONTEXT`, `highest_readiness_layer=BLOCKED`, verified runtime graph `graph_status=READY`. Trade advice, broker submit/transmit, autonomous execution, and live trading remain disabled or blocked.

## Existing Performance Architecture

The strongest current chain is:

`paper_trade_receipts / paper_position_events -> paper_position_ledger -> paper_pnl_report -> daily_paper_performance -> UI`

Sleeve rollup chain:

`paper_position_ledger + paper_position_events + paper_pnl_report + exit_recommendations + candidate review artifacts + sleeve scorecards -> sleeve_performance_truth -> UI`

Candidate/operator lifecycle chain:

`candidate_review_packet + paper_review_queue + candidate_decision_ledger + entry/exit receipts + command inbox/results + paper_position_ledger -> candidate_lifecycle_projection -> UI`

There is also a separate portfolio/evaluation architecture under `constellation_2/common/governed_evaluation_v1.py` and `aegis_performance_showcase_v1.py` for portfolio NAV, SPY benchmark curves, drawdown, and evaluation state. This should not be confused with simulated paper-position P&L.

## Existing Data Sources

| Name | Path | Purpose | Persisted or derived | Authority role | UI/API consumer |
|---|---|---:|---|---|---|
| Paper position events | `reports/aegis_paper_position_events_v1/{day}/paper_position_events.v1.jsonl`; producer `ops/aegis/paper_position_ledger_v1.py` | Append-only simulated paper open/close/invalidate events | Persisted JSONL | Source event stream for simulated paper positions | Consumed by ledger builder |
| Paper position ledger | `reports/aegis_paper_position_ledger_v1/{day}/paper_position_ledger.v1.json`; `ops/aegis/paper_position_ledger_v1.py` | Open/closed/legacy/invalidated simulated paper positions, marks, realized/unrealized P&L | Derived from events and receipt bootstrap | Canonical for simulated paper position state | `/aegis-positions`, `/aegis-open-paper-positions`, diagnostics, performance pages |
| Legacy paper trade receipts | `reports/aegis_paper_trade_receipts_v1/{day}/paper_trade_receipts.v1.json` | Legacy simulated paper receipts | Persisted operator/report artifact | Bootstrap input, legacy classified | Ledger bootstrap and mismatch diagnostics |
| Entry receipts | `reports/aegis_paper_entry_receipts_v1/{day}/paper_entry_receipts.v1.json` | Durable entry receipt evidence | Persisted | Lifecycle input | Candidate and positions UI through lifecycle projection |
| Exit receipts | `reports/aegis_paper_exit_receipts_v1/{day}/paper_exit_receipts.v1.json` | Durable exit receipt evidence | Persisted | Lifecycle input | Candidate and positions UI through lifecycle projection |
| Candidate decision ledger | `reports/aegis_candidate_decision_ledger_v1/{day}/candidate_decision_ledger.v1.json` | Approve/reject/defer decisions | Persisted ledger | Lifecycle source | Candidate lifecycle projection |
| Command inbox/results | `reports/aegis_command_inbox_v1/{day}/command_inbox.v1.json`, `reports/aegis_command_results_v1/{day}/command_results.v1.json` | Durable UI command workflow status | Persisted | Operator-command audit trail | `/api/aegis/commands`, `/api/aegis/operator/state-snapshot/latest` |
| Candidate lifecycle projection | `reports/aegis_candidate_lifecycle_projection_v1/{day}/candidate_lifecycle_projection.v1.json`; `ops/aegis/candidate_lifecycle_projection_v1.py` | Current candidates, open/closed positions, carry-forward, legacy context, allowed actions | Derived read model | Reporting/UI output | Candidate page, positions page, positions diagnostics |
| Paper P&L report | `reports/aegis_paper_pnl_report_v1/{day}/paper_pnl_report.v1.json`; `ops/aegis/paper_pnl_report_v1.py` | Open/closed P&L, P&L by sleeve/symbol, source hashes | Derived | Reporting output from paper ledger | `/aegis-performance` |
| Daily paper performance | `reports/aegis_daily_paper_performance_v1/{day}/daily_paper_performance.v1.json`; `ops/aegis/daily_paper_performance_v1.py` | Daily paper summary, best/worst, attention list, sleeve comparison | Derived | Reporting output | `/aegis-history`, `/aegis-performance` |
| Sleeve performance truth | `reports/aegis_sleeve_performance_truth_v1/{day}/sleeve_performance_truth.v1.json`; `ops/aegis/sleeve_performance_truth_v1.py` | Read-only sleeve rollup from existing authorities | Derived | Canonical read model over paper/sleeve authorities | `/aegis-performance`; declared in operator portal manifest |
| Exit recommendations | `reports/aegis_exit_recommendations_v1/{day}/exit_recommendations.v1.json` | Human-review exit recommendation layer | Derived | Recommendation read model, not closure authority | Daily paper performance and sleeve truth |
| Exit strategy analysis | `reports/aegis_exit_strategy_analysis_v1/{day}/exit_strategy_analysis.v1.json` | Stop/target/time-stop analysis | Derived | Reporting output | `/aegis-performance` |
| Performance showcase | `reports/aegis_performance_showcase_v1/{day}/aegis_performance_showcase.v1.html/json`; `constellation_2/common/aegis_performance_showcase_v1.py` | NAV equity curve, SPY benchmark curve, drawdown, paper orders | Derived | Display-only performance cockpit | `/performance`, `/performance/cockpit.html` |
| Monthly performance summary | `reports/monthly_performance_summary_v1/{month}/monthly_performance_summary.v1.json`; `ops/tools/run_monthly_performance_summary_v1.py` | Monthly NAV return, P&L, drawdown, trade count | Derived from NAV/fill ledgers | Monthly portfolio reporting | CLI/report consumer |
| Performance projection | `reports/performance_projection_v1/{day}/performance_projection.v1.json`; `constellation_2/common/performance_projection_v1.py` | Runtime stage timing, not P&L | Derived | Operational performance only | UI/report diagnostics |
| Advisor benchmark | `reports/advisor_benchmark_v1/{period_end}/{benchmark}/advisor_benchmark.v1.json`; `ops/tools/build_advisor_benchmark_v1.py` | Advisor gross/fee/net return vs Aegis return | Persisted CLI artifact | Reporting input/output, not integrated | CLI only per existing docs |
| Governed portfolio evaluation | `constellation_2/common/governed_evaluation_v1.py`; schemas under `governance/04_DATA/SCHEMAS/C2/EVALUATION/` | Portfolio/sleeve performance truth, validity, evaluation, benchmark state | Derived governed artifacts | Emerging portfolio/sleeve evaluation authority | Tests and downstream evaluation read models |

## Existing Metrics

| Metric | Exists | Where calculated | Formula/input | Output/UI/tests |
|---|---:|---|---|---|
| Open P&L / unrealized P&L | Yes, simulated paper | `paper_position_ledger_v1._mark_position`, `paper_pnl_report_v1` | `(mark - entry) * qty * sign`, only when mark is certified/current | `unrealized_pnl`; visible in `/aegis-performance`; tests include daily paper and sleeve truth |
| Closed/realized P&L | Yes | `paper_position_ledger_v1._apply_close_event` | Receipt `realized_pnl` if present, else `(exit - entry) * qty * sign` | `realized_pnl`; visible in paper P&L, daily performance, sleeve truth |
| Total paper P&L | Yes | `paper_pnl_report_v1`, `sleeve_performance_truth_v1`, `daily_paper_performance_v1` | realized + unrealized when unrealized canonical, else `NOT_CANONICAL` | UI and reports |
| Position return % | Partial | Exit strategy/performance surfaces; not in `paper_position_ledger` core rows | Some exit analysis uses return/distance percentages; paper ledger stores P&L not return % | Visible in exit/performance UI, not a single canonical paper field |
| Portfolio return % | Yes, separate NAV path | `aegis_performance_showcase_v1`, `monthly_performance_summary_v1`, `governed_evaluation_v1` | NAV current/start or daily portfolio rows | `/performance` cockpit and governed evaluation artifacts |
| Daily return | Yes, governed/evaluation path | `governed_evaluation_v1`, NAV/evaluation inputs | portfolio daily return from upstream row; benchmark daily return from close/current over previous close | Governed evaluation reports/tests |
| YTD return | Not found as canonical Aegis paper metric | No direct canonical paper YTD calculation found | Would require NAV/history window | Missing/incomplete |
| Since-inception return | Partial | `aegis_performance_showcase_v1` cumulative NAV return from first valid NAV point | `(current/start - 1) * 100` | `/performance` cockpit; limited by valid NAV points |
| Win rate | Yes | `sleeve_performance_truth_v1`, `governed_evaluation_v1`, sleeve reports/analytics | wins / (wins + losses) or winning trades / trade count | Visible in sleeve/performance review; tested |
| Average gain/loss | Yes outside paper ledger | `governed_evaluation_v1._trade_outcome_truth_from_fact_ledger`, `sleeve_performance_report` docs | avg winning/losing net P&L or outcome return | Report/test coverage; not central daily paper artifact |
| Best winner / worst loser | Yes | `daily_paper_performance_v1._best_worst_positions` | Rank open unrealized and closed realized P&L | `best_paper_positions`, `worst_paper_positions`; UI test references |
| Open vs closed performance | Yes | Paper P&L, daily performance, sleeve truth | Open unrealized separated from closed realized | UI separates open/closed |
| Position duration | Yes, sleeve level | `sleeve_performance_truth_v1._hold_time_days` | exit_time - entry_time in days | `average_hold_time`; tested indirectly |
| Candidate approval outcome | Yes | `candidate_lifecycle_projection_v1`, `sleeve_performance_truth_v1` | decision ledger state and approved/candidate counts | Candidate/positions UI |
| Rejected/deferred candidate outcome | Yes | `candidate_lifecycle_projection_v1` | `decision in REJECTED/DEFERRED` maps lifecycle state and actions | Candidate UI and diagnostics |

## Benchmark Support

Existing support:

- SPY benchmark curve exists in `aegis_performance_showcase_v1.py`, reading local `market_data_snapshot_v1/SPY/2026.jsonl` when available.
- Governed benchmark policy exists in `governance/02_REGISTRIES/C2_GOVERNED_EVALUATION_POLICY_V1.json`; benchmark role policy maps some roles to external symbol `SPY`.
- Governed benchmark daily return exists in `constellation_2/common/governed_evaluation_v1.py`, using current and previous market-data snapshot closes.
- Portfolio benchmark-relative state exists in governed evaluation: outperforming, underperforming, at benchmark, or not enough evidence.
- Benchmark gaps are explicitly surfaced as missing/invalid/partial data gaps. SPY missing rows are not fabricated.

Not found or incomplete:

- No general custom benchmark UI.
- No advisor-vs-SPY joined report.
- No dedicated stale benchmark diagnostics page found, although missing/invalid benchmark data gaps and reason codes exist.
- No paper-position-level alpha calculation found. Benchmark comparison is portfolio/evaluation/showcase-oriented.

## Advisor Support

Existing support:

- `ops/tools/build_advisor_benchmark_v1.py` records `benchmark_name`, `period_start`, `period_end`, `gross_return`, `fee_rate`, `net_return`, `aegis_return`, `difference`, `fee_drag`, `notes`, and canonical hash.
- Documentation in `docs/aegis_operator_use_completeness_audit.md` says advisor benchmark comparison is ready with manual steps but CLI-only.

Missing or incomplete:

- No quarterly/YTD advisor return UI found.
- No advisor-vs-SPY comparison artifact found.
- No source receipt attachment model for advisor returns beyond freeform notes and canonical artifact hash.
- Advisor benchmark is not integrated into `daily_paper_performance`, `sleeve_performance_truth`, `sleeve_performance_report`, or the Lite UI.

## Sleeve / Segmentation Support

Existing support:

- Sleeve identity and policy registries exist: `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`, canonical sleeve governance docs, and schemas.
- Paper P&L derives sleeve assignment from row `sleeve_id`, candidate lineage, source receipt, or exit recommendation candidate context.
- `sleeve_performance_truth_v1` computes per-sleeve open/closed counts, paper trade count, open exposure, realized P&L, unrealized P&L status/value, win/loss counts, win rate, average hold time, recommendation-followed rate, candidate count, approval rate, scorecard ref, missing authorities, and data-quality status.
- Allocation support exists through capital/risk allocation artifacts and `aegis_performance_showcase_v1._capital_allocations`.
- Separate sleeve tooling exists for performance analytics, attribution, challenger, readiness, input contracts, edge measurement, and evaluation kernels.

Not found or incomplete:

- No evidence of core/growth/dividend/speculative/cash grouping as a first-class paper performance segmentation in current read models.
- Sleeve-level benchmark is not fully joined into paper/daily performance.
- Sleeve-level return series is called out as missing by `aegis_performance_showcase_v1` unless a sleeve-level series exists.

## Ledger and Receipt Flow

Lifecycle map:

1. Candidate generated
   - Tables/files/services: `aegis_candidate_review_packet_v1`, `aegis_candidate_generation_diagnostics_v1`, `paper_review_queue_v1`.
   - Routes: Candidate UI, `/api/aegis/operator/state-snapshot/latest`.
   - Durable: generated artifacts in truth root.
   - UI: Candidate page and diagnostics.

2. Decision made
   - Files/services: `aegis_candidate_decision_ledger_v1`, `POST /api/aegis/commands`.
   - Receipt/audit ID: command ID and candidate decision event.
   - Durable: yes.
   - UI: Candidate lifecycle state, allowed actions.

3. Receipt created
   - Files/services: `aegis_paper_entry_receipts_v1`, `aegis_paper_exit_receipts_v1`, legacy `aegis_paper_trade_receipts_v1`, `manual_execution_receipt_v1`.
   - Routes: `/api/aegis/commands`; performance page manual receipt form.
   - Durable: yes.
   - UI: Candidate/positions/performance pages display receipt state.

4. Paper position opened/closed
   - Files/services: `ops/aegis/paper_position_ledger_v1.py` converts receipts/events to `PAPER_POSITION_OPENED` and `PAPER_POSITION_CLOSED`.
   - Receipt/audit ID: `position_id`, event ID, source receipt.
   - Durable: event JSONL and ledger JSON.
   - UI: positions and performance pages.

5. Ledger/event written
   - Files/services: `paper_position_events.v1.jsonl`, `paper_position_ledger.v1.json`.
   - Durable: yes.
   - UI: positions diagnostics, history, performance.

6. Diagnostics/reporting updated
   - Files/services: `candidate_lifecycle_projection`, `paper_pnl_report`, `daily_paper_performance`, `sleeve_performance_truth`, runtime truth kernel, verified runtime graph.
   - Routes: `/aegis-positions`, `/aegis-positions-diagnostics`, `/aegis-performance`, `/performance`, `/api/aegis/runtime-debug`.
   - Durable: report artifacts plus runtime graph/audit artifacts.

## Diagnostics

Relevant diagnostics/endpoints/pages found:

- `/aegis-positions`: open positions, today candidates, closed positions.
- `/aegis-positions-diagnostics`: lifecycle projection, carry-forward context, command status, session details, engineering metadata.
- `/aegis-open-paper-positions`: canonical open simulated paper positions.
- `/aegis-performance`: paper P&L, daily performance, sleeve performance truth, exit strategy, trade evaluation.
- `/performance` and `/performance/cockpit.html`: performance showcase cockpit.
- `/api/aegis/runtime-debug`: PID/module/truth root/artifact paths/hashes/projection presence.
- `/api/aegis/operator/state-snapshot/latest`: attaches candidate lifecycle projection and source paths.
- `/api/aegis/commands`, `/api/aegis/commands/status`, `/api/aegis/commands/validate`, `/api/aegis/commands/registry`.
- Candidate diagnostics command/report: `npm run aegis:candidate-diagnostics`, `candidate_generation_diagnostics.v1.json`.
- Paper ledger mismatch diagnostics: `paper_position_ledger_mismatches`, missing `PAPER_POSITION_OPENED` event for receipts.
- Stale/missing mark diagnostics: `mark_freshness_status`, `mark_certification_status`, `missing_mark_position_ids`, `PARTIAL_UNREALIZED_NOT_CANONICAL`, `DEGRADED_STALE_MARKS`.
- Runtime diagnostics: `runtime_truth_kernel`, `verified_runtime_graph`, `mode_readiness`, `missing_stale_sources`, `readiness_dependencies`.
- Performance architecture review: `aegis_performance_architecture_review_v1` lists duplicate sources, stale legacy logic, missing linkages, and P&L attribution quality.

## Tests

Relevant test files identified:

- `constellation_2/common/tests/test_aegis_daily_paper_performance_v1.py`: daily paper performance and safety flags.
- `constellation_2/common/tests/test_aegis_sleeve_performance_truth_v1.py`: sleeve truth read model, canonical/partial status, source authority behavior.
- `constellation_2/common/tests/test_aegis_performance_architecture_review_v1.py`: architecture review inventory and duplicate-risk reporting.
- `constellation_2/common/tests/test_aegis_paper_pnl_exit_strategy_v1.py`: paper P&L and exit strategy behavior.
- `constellation_2/common/tests/test_aegis_paper_lifecycle_reconciliation_v1.py`: paper lifecycle reconciliation.
- `constellation_2/common/tests/test_aegis_candidate_review_workflow_v1.py`: candidate review workflow.
- `constellation_2/common/tests/test_aegis_paper_session_ledger_v1.py`: paper session ledger.
- `constellation_2/common/tests/test_position_lifecycle_state_v1.py`: position lifecycle state.
- `constellation_2/common/tests/test_aegis_position_management_v1.py`: position management safety workflow.
- `constellation_2/common/tests/test_performance_projection_v1.py`: operational timing projection.
- `constellation_2/common/tests/test_monthly_performance_summary_v1.py`: monthly NAV performance summary.
- `constellation_2/common/tests/test_aegis_performance_showcase_v1.py`: performance showcase, NAV/benchmark/data gaps.
- `constellation_2/common/tests/test_performance_intelligence_advisory_v1.py`: performance intelligence/advisory boundaries.
- `constellation_2/phaseL/ui/tests/test_aegis_performance_trade_evaluation_ui_v1.py`: UI renders daily paper performance, paper P&L, exit strategy, sleeve performance truth.
- `constellation_2/phaseL/ui/tests/test_performance_cockpit_route_v1.py`: `/performance` cockpit route wiring.
- `constellation_2/phaseL/ui/tests/test_aegis_position_management_ui_v1.py`: position-management UI safety and endpoints.
- `constellation_2/phaseL/ui/tests/test_aegis_paper_operator_projection_ui_v1.py`: paper operator projection UI.
- `research_lab/tests/test_performance_metrics.py`, `research_lab/tests/test_forward_returns.py`, `research_lab/tests/test_sleeve_comparison.py`: research/backtest performance metrics, not runtime Aegis paper accounting.

## Already Exists

| Capability | Location | Confidence |
|---|---|---:|
| Event-sourced simulated paper position ledger | `ops/aegis/paper_position_ledger_v1.py` | High |
| Open/closed paper position state | `paper_position_ledger.v1.json` | High |
| Realized paper P&L | `paper_position_ledger_v1._apply_close_event`, `paper_pnl_report_v1` | High |
| Certified-mark unrealized paper P&L | `paper_position_ledger_v1._mark_position` | High |
| Total paper P&L with fail-closed `NOT_CANONICAL` | `paper_pnl_report_v1.py` | High |
| P&L by sleeve/symbol | `paper_pnl_report_v1.py` | High |
| Daily paper performance summary | `daily_paper_performance_v1.py` | High |
| Best/worst paper positions | `daily_paper_performance_v1.py` | High |
| Operator attention flags for stale marks, stop/target/time/exposure | `daily_paper_performance_v1.py` | High |
| Sleeve performance truth read model | `sleeve_performance_truth_v1.py` | High |
| Sleeve win rate, hold time, approval rate | `sleeve_performance_truth_v1.py` | High |
| Candidate lifecycle projection | `candidate_lifecycle_projection_v1.py` | High |
| Rejected/deferred/skipped candidate visibility | `candidate_lifecycle_projection_v1.py`, candidate diagnostics | High |
| SPY benchmark curve | `aegis_performance_showcase_v1.py` | Medium |
| Governed benchmark daily return | `governed_evaluation_v1.py` | Medium |
| Advisor benchmark CLI artifact | `ops/tools/build_advisor_benchmark_v1.py` | High |
| Performance UI consumers | `operator_shell/pages/index.js`, `run_ops_dashboard_v1.py` | High |

## Missing / Incomplete

| Capability | Recommended minimal addition | Dependency |
|---|---|---|
| Advisor benchmark in UI/report | Add a read-only advisor benchmark card/table that reads existing `advisor_benchmark_v1` artifacts | Existing advisor benchmark CLI artifact |
| Advisor-vs-SPY comparison | Extend advisor benchmark read model to include optional SPY benchmark artifact ref and comparison | Existing SPY market-data snapshots/benchmark curve |
| YTD paper/portfolio return | Add read-only derived window metric from existing NAV history or daily paper performance history | Canonical history source selection |
| Position return % in paper ledger/report | Add derived display field in paper P&L read model, not ledger authority | Certified marks and entry/exit data |
| Sleeve-level return series | Build from canonical sleeve NAV/P&L history, not UI aggregation | Sleeve performance truth history or NAV by sleeve |
| Stale benchmark diagnostics page | Surface existing benchmark reason codes/data gaps in diagnostics UI | Governed evaluation/showcase data gaps |
| Source receipt for advisor return | Add optional source path/hash/receipt fields to advisor benchmark artifact | Advisor benchmark schema/tool update |
| Core/growth/dividend/speculative/cash groups | Add mapping layer only if policy registry defines groups | Sleeve/portfolio policy registry |

## Duplication Risks

- P&L exists in multiple domains: simulated paper ledger, paper P&L report, sleeve realized P&L, monthly NAV summary, governed evaluation, and historical outcome attribution. UI must label the source authority and avoid mixing simulated paper and broker/account NAV.
- `performance_projection_v1` is operational timing, not trading performance. It should not be used for P&L UI.
- Exit surfaces overlap: `exit_recommendations`, `exit_strategy_analysis`, `exit_decision_v1`, and `exit_review_projection`.
- Sleeve ranking/quality appears in sleeve performance truth, sleeve performance analytics, scorecards, and research reports. Existing architecture review recommends routing rankings through scorecard rollups.
- Advisor benchmark is standalone; adding a parallel advisor schema without consuming the existing artifact would duplicate functionality.

## Recommended Next Step

Choose **Option A: expose existing performance data in a new UI or improve the existing performance UI**.

Reason: Aegis already has a paper performance accounting/read-model chain and UI wiring. The lowest-risk path is to consume existing artifacts: `daily_paper_performance_v1`, `paper_pnl_report_v1`, `sleeve_performance_truth_v1`, `candidate_lifecycle_projection_v1`, and optional `advisor_benchmark_v1`.

Do not choose Option D. There is no evidence that the existing architecture cannot support performance reporting. The evidence shows the opposite: the architecture exists, with known gaps around UI integration, benchmark/advisor joining, and history windows.

## Exact Files Reviewed

- `package.json`
- `aegis/modules/operator_portal/aegis.module.yaml`
- `aegis/modules/runtime_truth_kernel/aegis.module.yaml`
- `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-05-28/verified_runtime_graph.v1.json`
- `ops/aegis/paper_position_ledger_v1.py`
- `ops/aegis/paper_pnl_report_v1.py`
- `ops/aegis/daily_paper_performance_v1.py`
- `ops/aegis/sleeve_performance_truth_v1.py`
- `ops/aegis/candidate_lifecycle_projection_v1.py`
- `ops/tools/build_aegis_paper_pnl_report_v1.py`
- `ops/tools/build_aegis_daily_paper_performance_v1.py`
- `ops/tools/build_aegis_sleeve_performance_truth_v1.py`
- `ops/tools/build_aegis_performance_architecture_review_v1.py`
- `ops/tools/build_advisor_benchmark_v1.py`
- `ops/tools/run_performance_projection_v1.py`
- `ops/tools/run_monthly_performance_summary_v1.py`
- `constellation_2/common/performance_projection_v1.py`
- `constellation_2/common/aegis_performance_showcase_v1.py`
- `constellation_2/common/governed_evaluation_v1.py`
- `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`
- `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`
- `constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js`
- `constellation_2/phaseL/ui/tests/test_performance_cockpit_route_v1.py`
- `constellation_2/phaseL/ui/tests/test_aegis_performance_trade_evaluation_ui_v1.py`
- `constellation_2/phaseL/ui/tests/test_aegis_position_management_ui_v1.py`
- `docs/aegis_sleeve_performance_lifecycle.md`
- `docs/aegis_operator_use_completeness_audit.md`
- `docs/aegis_lite_manual_paper_readiness.md`
- `governance/02_REGISTRIES/C2_GOVERNED_EVALUATION_POLICY_V1.json`
- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_performance_showcase.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/monthly_performance_summary.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/performance_projection.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/sleeve_performance_truth.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/EVALUATION/portfolio_performance_truth.v1.schema.json`

## Suggested Follow-Up Codex Prompt

Implement Option A only: expose existing Aegis performance data without adding a new accounting layer. Read `AEGIS_PERFORMANCE_CAPABILITY_AUDIT.md`, run `npm run aegis:audit`, read the latest `verified_runtime_graph.v1.json`, then update the existing performance UI to consume `daily_paper_performance_v1`, `paper_pnl_report_v1`, `sleeve_performance_truth_v1`, `candidate_lifecycle_projection_v1`, and optionally existing `advisor_benchmark_v1` artifacts. Do not invent P&L, returns, benchmark values, or readiness. Update `aegis/modules/**/aegis.module.yaml` for any UI/capability changes and run the relevant tests plus `npm run aegis:audit`.
