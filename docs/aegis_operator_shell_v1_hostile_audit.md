# Aegis Operator Shell V1 Hostile Audit

Audit date: 2026-05-30  
Scope: Operator Shell V1 freeze screenshots and current 2026-05-30 runtime truth.  
Posture: adversarial review only; no implementation changes made.

## 1. Executive Summary

Operator Shell V1 is significantly more trustworthy than the pre-rebuild UI. The frozen screens are workflow-first, mostly plain-English, and no longer expose stale candidate action buttons or raw contract state as the primary experience. A non-engineer can answer the core daily questions on the major screens: what is open, what is blocked, whether action is required, and what happens next.

The shell is not operationally perfect. The runtime truth remains `PARTIAL_CONTEXT` with highest readiness `BLOCKED`, and the shell is therefore only trustworthy if the operator accepts that Aegis is currently a read-only monitoring surface. The screenshots mostly communicate this, but several visible details still weaken trust: top chrome sometimes says operational timestamps are loading, Today says no action is required while System Health says review the recovery plan only, and the UI implementation remains heavily concentrated in a very large page file.

No P0 safety or stale-action issue was found in the frozen visible output. The correct recommendation is **Freeze V1 with caveats**: usable for daily paper-mode monitoring, not for execution readiness, and not yet a maintainable long-term UI foundation without follow-up cleanup.

## 2. Grades

| Category | Grade | Rationale |
|---|---:|---|
| Overall | 82 | Product experience is now coherent enough for V1 freeze with caveats; technical debt and runtime-blocked state prevent a stronger grade. |
| Functional | 86 | The six primary screens answer their intended operator questions and respect boundaries. Some action semantics and loading/status copy remain confusing. |
| Technical | 73 | Route/navigation tests pass, but the shell still depends on a 21,626-line `pages/index.js` and a large 8,024-line server file. This is a high maintainability and regression risk. |
| Safety / Trust | 89 | Safety gates remain disabled, portal smoke confirms current-day evidence, and screenshots fail closed. Trust is reduced by persistent runtime `PARTIAL_CONTEXT` / `BLOCKED`. |
| Maintainability | 68 | The product direction is good, but the implementation is still monolithic and coupled to backend operational vocabulary. Screenshot regression coverage is not yet strong enough. |

## 3. Validation Run

Commands run during this audit:

```bash
TARGET_DAY=2026-05-30 npm run aegis:audit
TARGET_DAY=2026-05-30 npm run aegis:portal-smoke
pytest constellation_2/phaseL/ui/tests/test_aegis_engineering_dashboard_product_ui_v1.py constellation_2/phaseL/ui/tests/test_aegis_operator_workspace_navigation_v1.py -k 'engineering_route or engineering_drawer or command_center_is_default or actual_engineering or system_health'
pytest constellation_2/phaseL/ui/tests/test_aegis_research_review_ui_v1.py constellation_2/phaseL/ui/tests/test_aegis_paper_performance_ui_v1.py
```

Results:

* `aegis:audit`: completed. Verified graph `READY`, audit blocker count `0`; runtime truth remains `PARTIAL_CONTEXT`, highest readiness `BLOCKED`.
* `aegis:portal-smoke`: passed for `2026-05-30`; day path invariant true; requested/source day both `2026-05-30`; policy gates unchanged.
* Engineering/navigation selected tests: `7 passed`.
* Research/performance selected tests: `22 passed`.

Key evidence paths:

* `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-05-30/verified_runtime_graph.v1.json`
* `/home/node/constellation_runtime_data/truth/reports/aegis_runtime_truth_kernel_v1/2026-05-30/runtime_truth_kernel.v1.json`
* `/home/node/constellation_runtime_data/truth/reports/aegis_audit_handoff_v1/2026-05-30/aegis_audit_handoff.txt`
* `/home/node/constellation_runtime_data/truth/reports/aegis_candidate_state_v1/2026-05-30/candidate_state.v1.json`
* `/home/node/constellation_runtime_data/truth/reports/aegis_canonical_operator_state_v1/2026-05-30/canonical_operator_state.v1.json`

## 4. Top 10 Strengths

1. **Current-day fail-closed behavior is visible.** Today says Aegis is monitoring only and shows no action buttons.
2. **Candidate stale-row risk is materially reduced.** Candidates shows `0` actionable candidates and explicitly excludes `37` older or carry-forward rows.
3. **Positions and Candidates are now cleanly separated.** Positions owns holdings/exposure; Candidates owns actionability/evaluation.
4. **Performance no longer implies complete P&L.** It labels full portfolio P&L incomplete and emphasizes missing marks.
5. **System Health explains the graph/runtime split.** It says graph validation passed but runtime verification is incomplete.
6. **Research is more operator-readable.** It distinguishes research follow-ups from active evidence collection.
7. **Navigation labels are mostly operator-facing.** Prior backend subtitles such as `daily_operator_workspace` are gone from the frozen screenshots.
8. **Safety policy is consistently visible.** The UI and audit output show broker/live/autonomous execution remain disabled.
9. **Portal smoke confirms day-source consistency.** The current run reports requested day and source day both `2026-05-30`.
10. **Screen boundaries are now understandable.** Each primary screen has a distinct job rather than mixing diagnostics into every page.

## 5. Top 10 Risks

1. **Runtime truth remains blocked.** Verified graph `READY` is not the same as operational runtime readiness; V1 is only trustworthy as a monitoring shell.
2. **Persistent “Operational timestamps loading” appears in multiple screenshots.** This weakens trust in day/time clarity, especially on Today, Positions, and System Health.
3. **Action language is still slightly inconsistent.** Today says no action required; System Health says review recovery plan only; Research says follow-ups need review.
4. **The UI is technically monolithic.** `constellation_2/phaseL/ui/static/operator_shell/pages/index.js` is 21,626 lines.
5. **Server coupling is also heavy.** `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py` is 8,024 lines.
6. **Screenshot acceptance is not fully automated.** The freeze relies on manual screenshot inspection and targeted tests, not complete visible-text regression coverage.
7. **Some backend language remains visible.** Examples include `Runtime BLOCKED`, `BLOCKED`, and `Recovery plan required`; acceptable for System Health but still not fully operator-native.
8. **Positions can overstate clarity of sleeve assignment.** It shows `Sleeves 1` while many holdings are present; if attribution is incomplete, the visible explanation is not strong enough.
9. **Performance still shows numeric partial values.** Even with caveats, `$0.00` can anchor operator interpretation.
10. **Legacy code risk remains high.** The old shell was repeatedly patched; without stronger route/component isolation, regressions can reappear.

## 6. P0 Issues

No P0 issue found in the frozen screenshots or non-mutating validation.

Criteria checked:

* No stale prior-day actionable candidate rows were visible.
* No capture, submit, broker, or execution actions were visible.
* Safety gates remained disabled.
* Command Center did not present actionability while runtime evidence was incomplete.
* Portal smoke reported requested/source day consistency for `2026-05-30`.

## 7. P1 Issues

### P1-1: Operational timestamp loading remains visible

Evidence: OCR from Today, Positions, and System Health freeze screenshots shows `Operational timestamps loading` in the top chrome.

Impact: This undermines the shell’s trust goal. The body may show correct timestamps, but the global chrome implies time context is unresolved.

Recommendation: Fix before broad operator rollout. The timestamp component should either show a resolved timestamp or be omitted from frozen/current-day screens.

### P1-2: Operator action semantics are not perfectly unified

Evidence:

* Today: `No action required. Aegis is monitoring only.`
* System Health: `Review recovery plan only.`
* Research: `2 research follow-ups need review.`

Impact: These are not direct contradictions, but they require the operator to infer the difference between “David action,” “review only,” and “research follow-up.”

Recommendation: Define one shell-wide action taxonomy: `No action`, `Optional review`, `Required review`, `Repair needed`, `Waiting`.

### P1-3: Runtime remains blocked while shell is being frozen

Evidence: `runtime_truth_classification: PARTIAL_CONTEXT`; `highest_readiness_layer: BLOCKED`; verified graph `READY`.

Impact: The shell can freeze as a monitoring UI, but it cannot be advertised as operationally healthy. Current wording mostly handles this, but future readers may over-trust the `READY` graph concept.

Recommendation: V1 freeze language should explicitly say “monitoring shell freeze,” not “Aegis operational readiness freeze.”

### P1-4: Monolithic UI implementation creates high regression risk

Evidence:

* `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`: 21,626 lines.
* `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`: 8,024 lines.

Impact: Even if the visible shell is acceptable, small edits can reintroduce legacy render paths. This is the largest technical blocker to confidence.

Recommendation: V1 may freeze, but V1.1 should split page renderers and operator-ready data mapping into smaller tested modules.

### P1-5: Automated screenshot/visible-text regression coverage is incomplete

Evidence: Targeted tests pass, but the audit still relied on manual OCR/visual review for primary evidence. Some tests are source/string oriented rather than end-to-end visible operator truth.

Impact: Future changes can pass tests while visually regressing operator trust.

Recommendation: Add Playwright or equivalent visible-text screenshot checks for all six frozen screens before broad use.

## 8. P2/P3 Backlog

### P2

* Improve System Health language: replace remaining uppercase `BLOCKED` emphasis with a calmer operator phrase where possible.
* Clarify Positions `Sleeves 1` and attribution completeness in a single line.
* Reduce repeated `View Review` / `Details` actions in Positions rows or make the primary action clearer.
* Performance should visually separate certified partial values from unavailable full metrics even more strongly.
* Research should show exactly which two follow-ups need review with clearer row-level action wording.
* Candidates has dense wording around the blocker table; it is improved but still word-heavy.
* Navigation still exposes implementation-ish route structure indirectly: System Health lives behind prior Engineering/ops route history.
* Add accessibility pass for keyboard navigation, contrast, and table density.

### P3

* Improve OCR/readability of narrow table columns.
* Standardize capitalization for `Monitoring only`, `No action required`, and `Blocked`.
* Make all “next scheduled run” language visually consistent across Today and Candidates.
* Add a small “last refreshed” statement per screen if the global timestamp component remains.

## 9. Functional Audit by Screen

### Today / Command Center

Grade: 88

What works:

* Clearly answers “Is Aegis okay today?” with `No action required. Aegis is monitoring only.`
* Explains why: runtime evidence is incomplete.
* Shows open positions, candidates, waiting research, blocked runtime evidence, last run, and next run.
* Does not show capture or execution buttons.

Hostile findings:

* `Operational timestamps loading` in the chrome conflicts with the page’s mission to clarify today.
* The paper session scheduled text is visually dense and could be mistaken for an upcoming run unless the reader reaches the Next Run card.
* It says no action required while System Health says recovery-plan review only; this distinction needs taxonomy hardening.

Severity: P1 for timestamp loading; P2 for action taxonomy and schedule clarity.

### Positions

Grade: 84

What works:

* Answers “What do we currently own?” with 36 open paper positions.
* Shows entry notional, largest exposure, mark coverage, and incomplete price warning.
* Candidate workflow is excluded.

Hostile findings:

* Global timestamp loading remains visible.
* `Sleeves 1` is potentially confusing given many holdings and previous attribution issues.
* Repeated row actions create density and could distract from ownership review.
* Current value and P&L are unavailable, so the screen is useful for inventory but not valuation.

Severity: P1 for timestamp loading; P2 for sleeve clarity and row density.

### Candidates

Grade: 88

What works:

* Clearly answers no candidate action today.
* Explains candidate evaluation is blocked by missing market data.
* Separates prior rows: 37 older/carry-forward rows excluded and not actionable today.
* Excludes positions/holdings/P&L.

Hostile findings:

* “Candidate generation blocked” and “Candidate evaluation blocked” are understandable but still system-centric.
* Blocker table remains dense and partially repetitive.
* No future candidate evaluation is confirmed; that is honest, but it limits operational usefulness.

Severity: P2.

### Research

Grade: 82

What works:

* Shows investigations, active research, findings, blockers, and next research step.
* Above the fold shows follow-ups needing review, latest finding, and a research blocker.
* Active evidence collection says no action is required.

Hostile findings:

* Research still has the most complex action story: 2 follow-ups require review while active rows do not.
* “Review only the follow-up rows” is clear, but the exact workflow for completing that review is not obvious from the screenshot.
* Findings are visible, but confidence is `Not reported`, which weakens trust in research conclusions.

Severity: P1/P2 depending rollout; no P0.

### Performance

Grade: 80

What works:

* Strongly labels performance as partial.
* Makes missing marks and incomplete full portfolio P&L visible.
* Keeps system health/candidate/research workflows out.
* Benchmark stale status is visible.

Hostile findings:

* `$0.00` appears in multiple places and can still be misread despite caveats.
* Attribution coverage exists, but rankings are explicitly not trustworthy; this is honest but reduces page utility.
* The screen answers “is data complete?” better than “how are we doing?” because marks are missing.

Severity: P2.

### System Health

Grade: 83

What works:

* Clearly owns operational health and repair/recovery concerns.
* Explains graph validation healthy vs runtime readiness blocked.
* Shows safety mode protected.
* Does not mix portfolio performance or candidate details.

Hostile findings:

* `Operational timestamps loading` is visible.
* `Runtime BLOCKED`, `BLOCKED`, and `Recovery plan required` are still more engineering-like than operator-native.
* It says missing or stale sources: 0 while runtime verification is incomplete; the page explains this somewhat, but it is still cognitively hard.
* It references recovery plan but does not make the destination/action fully tangible in the visible screenshot.

Severity: P1 for timestamp loading and runtime/blocker clarity; P2 for recovery-plan discoverability.

## 10. Technical Audit by Layer

### Routing and Navigation

Status: Pass with caveats.

Evidence: Targeted route/navigation tests passed. Frozen screenshots show operator-facing nav subtitles rather than backend identifiers.

Risks:

* Some historical route names remain likely under the hood, such as System Health using prior Engineering route lineage.
* Route correctness is only as strong as the selected tests; full visible browser click coverage for every route should be automated.

### State Handling

Status: Improved but not fully normalized.

The shell handles monitoring-only, no-action, blocked, partial, and incomplete states in visible operator language. The weakness is taxonomy consistency: Today, Research, and System Health express different shades of “action” using different labels.

### Data Mapping

Status: Adequate for V1 monitoring.

Portal smoke confirms day path invariants and current-day artifacts for `2026-05-30`. Candidates correctly excludes prior rows. Performance and Positions clearly label missing marks.

Risk: The mapping logic remains complex and partly browser/server-renderer local. A stale artifact regression is less likely than before, but still possible without broader visible-text regression tests.

### Legacy Code Risk

Status: High risk.

`pages/index.js` at 21,626 lines is the strongest technical argument against a clean freeze. The shell may look right now, but a file of that size is difficult to reason about and invites accidental reactivation of legacy render paths.

### Component Duplication

Status: Medium risk.

Cards and summary patterns are visually consistent enough, but they appear hand-built per screen rather than guaranteed by a small shared component system. Duplication increases future inconsistency risk.

### Backend Coupling

Status: Medium/high risk.

The UI no longer leads with surface contract/invariant vocabulary, but still reflects runtime kernel concepts (`BLOCKED`, graph/runtime split, recovery plan). Some coupling is inevitable for System Health, but it should be isolated there.

### Tests

Status: Supporting, not conclusive.

Passing tests:

* Engineering/navigation selected tests: 7 passed.
* Research/performance selected tests: 22 passed.
* Portal smoke passed.

Gaps:

* Not enough full-page screenshot regression coverage.
* Not enough visible-text assertions against all frozen screens.
* Some existing tests inspect source strings, which can pass while product experience regresses.
* Known broader navigation/header test debt remains around contextual refresh labeling in previous runs.

### Performance / Loading Behavior

Status: Needs attention.

The visible text `Operational timestamps loading` suggests either a loading state persisted into capture or the global timestamp component is unreliable. Even if harmless, it damages operator trust.

### Accessibility / Readability

Status: Not proven.

The shell is visually cleaner than before, but dense tables remain. No evidence was reviewed for keyboard navigation, screen-reader labels, or contrast. This is a V1.1 requirement before broader use.

### Stale Artifact / Stale Date Risk

Status: Lower than before, not eliminated.

Portal smoke confirms current-day state and Candidates excludes stale prior rows. However, technical implementation complexity means stale-date regressions remain a test coverage risk.

## 11. Safety / Trust Assessment

Safety gates remain disabled:

* `trade_advice_allowed=false`
* `broker_execution_allowed=false`
* `broker_submit_transmit_allowed=false`
* `live_trading_allowed=false`
* `autonomous_live_trading_allowed=false`

Evidence from audit output:

* Live broker trading policy: disabled by design.
* Autonomous execution policy: disabled by design.
* Trade advice allowed: false.
* Manual trade capture allowed: false in control packet reason path.
* Portal smoke: policy gates unchanged.

No frozen screenshot implies broker submit/transmit, live trading, autonomous trading, or trade advice is enabled. The shell is safe as a read-only monitoring/operator-review surface.

Trust caveat: runtime truth remains `PARTIAL_CONTEXT` / `BLOCKED`. V1 must be described as a shell freeze, not as a runtime health clearance.

## 12. Test and Validation Gaps

1. Full screenshot regression suite for all six V1 screens.
2. Visible-text assertions for absence of stale prior-day identifiers on every current-day route.
3. Route click regression over every nav item with day preservation.
4. Negative test for persistent global loading indicators.
5. Contract/current-day test that compares screenshot-visible counts to API payloads for all screens.
6. Accessibility tests for keyboard navigation and table interaction.
7. Performance tests for large position tables.
8. Test proving no legacy render path can appear below or alongside rebuilt V1 content.
9. Ask Aegis end-to-end grounding checks from the rebuilt screens.
10. Test proving broader shell action taxonomy stays consistent across Today, Research, and System Health.

## 13. Final Recommendation

**Freeze V1 with caveats.**

Operator Shell V1 is good enough to freeze as a daily paper-mode monitoring shell. It should not be marketed or treated as evidence that Aegis is fully operationally ready, because runtime truth remains blocked and performance/position valuation is incomplete for the frozen day.

Freeze conditions:

* No execution, broker, live-trading, or autonomous claims.
* State clearly that V1 is monitoring-only when runtime evidence is incomplete.
* Track P1 issues immediately after freeze, especially global timestamp loading, action taxonomy, and monolithic implementation risk.

Do not roll back the rebuilt shell. The product direction is a clear improvement. Do not broaden use until P1 issues are scheduled and screenshot regression coverage is added.
