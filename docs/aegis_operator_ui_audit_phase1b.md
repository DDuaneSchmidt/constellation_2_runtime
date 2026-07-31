# Aegis Operator UI Audit - Phase 1B

## Scope

This audit reviews visible browser output and current UI code for the operator shell. It does not redesign or implement anything.

Evidence captured under:

```text
aegis_ui_phase1b_audit/
```

Key source files inspected:

* `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`
* `constellation_2/phaseL/ui/static/operator_shell/main.js`
* `constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js`
* `constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js`
* `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`

Runtime context from audit:

* verified graph: READY
* runtime truth: PARTIAL_CONTEXT / BLOCKED
* safety gates: disabled by policy

## Summary Findings

The current shell has useful backend safety data but still fails as an operator product. It leads with contracts, artifacts, diagnostics, and status fragments instead of answering daily workflow questions.

Top problems:

1. Several screens begin with backend architecture text rather than operator answers.
2. Actionability remains semantically unclear: some rows say actionable while global actions are disabled.
3. Evidence and raw contracts appear too early.
4. Multiple screens duplicate the same contract banner and Ask Aegis controls.
5. Legacy routes still expose stale/current-day mixed content, including a 2026-05-26 candidate in the operator cockpit legacy route.
6. Empty states are often technical rather than operator-readable.
7. Runtime health, engineering, and evidence screens overlap heavily.
8. Candidate screens are split across too many routes.
9. Performance/Sleeve Analytics show degraded contract states but still expose internal terms.
10. The UI architecture is coupled to many raw artifact endpoints and page-specific renderers.

## Screen Audits

### Command Center

Evidence: `aegis_ui_phase1b_audit/command_center.workspace.txt`

* Current purpose: daily operator workspace.
* Observed purpose: mixed contract banner, metrics, action queue, candidates, positions, diagnostics.
* Works: shows open position count, awaiting review count, attention rows, current day/source day.
* Confusing: first visible text is contract/state language; `Needs Attention 2` conflicts with actions-disabled governance; `Mode Readiness UNKNOWN` is not explained.
* Duplicated elsewhere: positions table duplicates Positions; diagnostics duplicates System Health; Ask Aegis duplicates all pages.
* Operator value: high-level counts, open positions, attention queue if correctly classified.
* No operator value: raw contract evidence note, repeated diagnostics, internal source labels.
* Missing information: did a run occur, next scheduled run, whether no candidates is normal or blocked.
* Empty-state problems: no crisp “No operator action required” story when no candidate action exists.
* Status-state problems: “Surface checks passed” plus runtime BLOCKED elsewhere creates trust ambiguity.
* Hierarchy problems: contract banner precedes actual daily summary.
* Coupling issues: `renderCommandCenterWorkspace` assembles many backend projections directly in browser.
* Severity: P1.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Top contract banner | REPLACE | Backend-centric; not the daily answer. |
| Metric strip | IMPROVE | Useful counts but needs plain labels and next-run context. |
| Attention Queue | IMPROVE | Useful only if true user actions; current wording still mixes blockers. |
| Candidate Review table | IMPROVE | Needs candidate-pipeline ownership, not primary daily clutter. |
| Open Positions preview | KEEP | Useful summary, but keep compact. |
| Monitor/Safe to Ignore | IMPROVE | Useful separation, but too much detail for first screen. |
| Diagnostics details | KEEP | Only if collapsed and secondary. |
| Ask Aegis button | IMPROVE | Should be secondary after core status. |

### Positions

Evidence: `aegis_ui_phase1b_audit/positions.workspace.txt`

* Current purpose: open positions, today's candidates, closed positions.
* Observed purpose: positions page plus candidate capture and review workflow mixed in.
* Works: open position table is visible; 36 open positions are clear; actions mostly say no operator action required.
* Confusing: candidates belong to Candidate Pipeline, not Positions; mark/P&L fields show `n/a`; “Surface checks passed” is not useful to a non-engineer.
* Duplicated elsewhere: candidate capture duplicates Command Center/Candidate Pipeline; View Review duplicates Position Review.
* Operator value: open positions list, symbol, entry, mark, P&L, status.
* No operator value: contract banner, candidate confirmation panel on Positions.
* Missing information: mark coverage summary, sleeve attribution health, what to monitor next.
* Empty-state problems: not observed for open positions, but current pattern would show technical contract state first.
* Status-state problems: positions surface READY while runtime truth globally BLOCKED; needs plain scope explanation.
* Hierarchy problems: operator state banner pushes the positions answer down.
* Coupling issues: `renderPositionsWorkspace` pulls positions plus candidates plus contracts.
* Severity: P1.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Open Positions table | KEEP | Core operator value. |
| Position action links | IMPROVE | Details/review okay; avoid action-looking controls. |
| Candidate capture panel | REMOVE | Belongs to Candidate Pipeline. |
| Today's Candidates table | REMOVE | Belongs to Candidate Pipeline. |
| Contract banner | REPLACE | Backend framing. |
| Diagnostics link | KEEP | Secondary only. |

### Position Review

Evidence: `aegis_ui_phase1b_audit/position_review.workspace.txt`

* Current purpose: audited position review briefs.
* Observed purpose: AI artifact/status shell plus review cards.
* Works: shows 36 briefs, unsupported claims 0, forbidden language 0, context hash; review artifact exists.
* Confusing: starts with contract banner; “browser reads endpoint and does not generate AI content” is engineering proof, not operator value.
* Duplicated elsewhere: position row links and position health can be surfaced from Positions.
* Operator value: thesis summary, health/status, risk/confirmation if visible in cards.
* No operator value: artifact implementation statements as primary copy.
* Missing information: quick answer of which positions need attention; summary by improving/stable/deteriorating.
* Empty-state problems: previous shell had npm-command empty state; current must avoid returning to that.
* Status-state problems: CANONICAL status dominates before “what matters.”
* Hierarchy problems: implementation proof comes before position insight.
* Coupling issues: `renderAegisPositionReviewPage` directly renders artifact internals.
* Severity: P2.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Review brief cards | KEEP | Core page value. |
| Position health/thesis status | KEEP | If prominent. |
| Artifact/status summary | IMPROVE | Move to secondary. |
| Context hash/source details | KEEP | Diagnostics only. |
| Contract banner | REPLACE | Not operator-first. |

### Research Workspace

Evidence: `aegis_ui_phase1b_audit/research_workspace.workspace.txt`

* Current purpose: research workspace landing page.
* Observed purpose: navigation hub for research queue/review/diagnostics/performance links.
* Works: has clear links to queue, review, diagnostics.
* Confusing: duplicates Research Lab and Research Review; does not clearly answer what is running now.
* Duplicated elsewhere: Research Lab and Research Review.
* Operator value: entry point if simplified.
* No operator value: broad link list without state hierarchy.
* Missing information: current research state, next expected observation, user action requirement.
* Empty-state problems: no clear no-research state.
* Status-state problems: surface READY but not tied to research progress.
* Hierarchy problems: link hub over operational answer.
* Coupling issues: separate from Research Lab renderer and route group.
* Severity: P2.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Research Review CTA | KEEP | Useful when briefs exist. |
| Queue/Diagnostics links | KEEP | Secondary navigation. |
| Contract banner | REPLACE | Not a research answer. |
| Generic link grid | IMPROVE | Needs state-led grouping. |

### Research Lab

Evidence: `aegis_ui_phase1b_audit/research_lab.workspace.txt`

* Current purpose: hypotheses and research execution monitoring.
* Observed purpose: dense hypothesis dashboard.
* Works: shows total hypotheses, collecting evidence count, waiting rows, recommendation count.
* Confusing: “No visible hypothesis” repeated; ready/waiting labels do not explain next expected progress; NVIDIA card appears under Recommendations Ready 0 context.
* Duplicated elsewhere: Research Workspace and Research Review.
* Operator value: hypothesis counts, collecting evidence status.
* No operator value: repeated empty group labels.
* Missing information: per-hypothesis next sample time and whether operator action is required near top.
* Empty-state problems: “No visible hypothesis” is not a useful empty state.
* Status-state problems: waiting/researching/ready taxonomy is not mapped cleanly to operator states.
* Hierarchy problems: counts compete with hypothesis cards.
* Coupling issues: `renderResearchLabPage` mixes console, intake, review, and validation UI.
* Severity: P1.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Hypothesis counts | KEEP | Useful if translated. |
| Collecting Evidence rows | KEEP | Core autonomous validation value. |
| Empty group cards | REMOVE | Noise. |
| Hypothesis cards | IMPROVE | Need plain next-step hierarchy. |
| Diagnostics | KEEP | Collapsed. |

### Research Review

Evidence: `aegis_ui_phase1b_audit/research_review.workspace.txt`

* Current purpose: operator-readable research briefs.
* Observed purpose: artifact-focused review brief page.
* Works: shows two briefs and meaningful evidence/risk fields.
* Confusing: `RECOMMENDATION_READY` appears as raw state; decision language still implies manual review before validation.
* Duplicated elsewhere: Research Lab recommendation summary.
* Operator value: conclusions, confidence, evidence, risks.
* No operator value: artifact name as hero content.
* Missing information: whether qualified for paper validation, paper-testing sleeve state, whether action required.
* Empty-state problems: not observed; should avoid raw empty fallback.
* Status-state problems: recommendation-ready conflicts with autonomous validation workflow language.
* Hierarchy problems: artifact/status before conclusion value.
* Coupling issues: direct artifact renderer.
* Severity: P2.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Brief cards | KEEP | Valuable. |
| Evidence/risk lists | KEEP | Operator-readable. |
| Artifact label | REMOVE | Belongs to evidence detail. |
| Raw status labels | REPLACE | Translate to validation state. |

### Runtime Timeline

Evidence: `aegis_ui_phase1b_audit/runtime_timeline.workspace.txt`

* Current purpose: timed runs, retries, pipeline-stage visibility.
* Observed purpose: operational run history and domain certification status.
* Works: shows operational day, last update, ETA, missed morning run.
* Confusing: dense with domain certification rows; “Repair required Capture recommendations” unclear.
* Duplicated elsewhere: System Health/Engineering.
* Operator value: last run, missed run, next ETA.
* No operator value: long domain detail on first view.
* Missing information: exact “what happens next” in plain language.
* Empty-state problems: not observed.
* Status-state problems: needs attention and no required sleeve blocked can coexist without impact explanation.
* Hierarchy problems: domain detail too prominent.
* Coupling issues: generic `renderAegisWorkflowPage("runtime_timeline")` route.
* Severity: P2.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Operational day/last update/ETA | KEEP | High value. |
| Missed run alert | KEEP | High value if repair is clear. |
| Domain certification rows | IMPROVE | Summarize first, detail later. |
| Repair Center links | KEEP | Secondary. |

### Performance

Evidence: `aegis_ui_phase1b_audit/performance.workspace.txt`

* Current purpose: paper performance analytics.
* Observed purpose: contract-gated degraded state with raw contract details visible early.
* Works: does not render contradictory metric cards; says degraded due noncanonical P&L.
* Confusing: begins with `Operator Surface Contract` and `OPERATOR_SURFACE_CONTRACT`; metrics allowed while DEGRADED is unclear.
* Duplicated elsewhere: Sleeve Analytics and Position P&L.
* Operator value: reason for analytics unavailability/degradation.
* No operator value: raw JSON contract in primary content.
* Missing information: concise portfolio P&L availability answer; whether there are positions; how to recover.
* Empty-state problems: technical degraded state instead of performance unavailable story.
* Status-state problems: metrics allowed under degraded needs stricter product meaning.
* Hierarchy problems: raw contract dominates.
* Coupling issues: analytics template driven by operator surface contract rather than product screen needs.
* Severity: P1.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Degraded warning | KEEP | Necessary. |
| Raw contract panel | REMOVE | Evidence-only. |
| Metrics allowed/actions disabled labels | REPLACE | Translate to operator language. |
| Ask Aegis | IMPROVE | Secondary. |

### Sleeve Analytics

Evidence: `aegis_ui_phase1b_audit/sleeve_analytics.workspace.txt`

* Current purpose: sleeve analytics.
* Observed purpose: degraded contract state and raw contract details.
* Works: avoids pretending analytics are canonical.
* Confusing: raw `PARTIAL`, contract JSON, and backend vocabulary lead the page.
* Duplicated elsewhere: Performance.
* Operator value: knowing sleeve analytics are degraded.
* No operator value: raw contract JSON as primary content.
* Missing information: which sleeves are affected, whether results are usable.
* Empty-state problems: no clean “sleeve analytics unavailable/degraded” product state.
* Status-state problems: `metrics allowed` under degraded is ambiguous.
* Hierarchy problems: no sleeve operator answer before contract detail.
* Coupling issues: driven by generic contract template.
* Severity: P1.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Degraded warning | KEEP | Needed. |
| Raw contract | REMOVE | Evidence-only. |
| Sleeve scorecard | KEEP only when reliable | Should be absent/secondary when degraded. |
| Ask Aegis | IMPROVE | Secondary. |

### Engineering / System Health

Evidence: `aegis_ui_phase1b_audit/engineering.workspace.txt`

* Current purpose: troubleshooting cockpit.
* Observed purpose: contract-gated engineering surface with raw contract JSON first.
* Works: says runtime truth is blocked and actions are disabled.
* Confusing: raw contract and backend vocabulary dominate; top issue/repair workflow is not first.
* Duplicated elsewhere: Runtime Timeline, Evidence Detail.
* Operator value: runtime truth blocked reason.
* No operator value: raw contract JSON in primary content.
* Missing information: top human-readable issue, cause, impact, next verification.
* Empty-state problems: not observed.
* Status-state problems: graph READY elsewhere vs runtime blocked requires better plain explanation.
* Hierarchy problems: contract state over repair workflow.
* Coupling issues: template/contract-first instead of problem-first.
* Severity: P1.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Runtime blocked message | KEEP | Important. |
| Raw contract | REMOVE | Evidence-only. |
| Ask Aegis issue prompt | IMPROVE | Useful after top issue. |
| Top issue repair workflow | REPLACE current | Needs to lead. |

### Candidate Pipeline (`/aegis-candidates`)

Evidence: `aegis_ui_phase1b_audit/candidate_pipeline.workspace.txt`

* Current purpose: candidate workflow details.
* Observed purpose: broken page showing `payload is not defined`.
* Works: nothing in current visible output.
* Confusing: raw JavaScript failure is the whole page.
* Duplicated elsewhere: Command Center/Positions candidate panels.
* Operator value: none until fixed.
* No operator value: entire current output.
* Missing information: all candidate pipeline information.
* Empty-state problems: unhandled exception instead of safe state.
* Status-state problems: no state model applied.
* Hierarchy problems: failure only.
* Coupling issues: legacy renderer bug.
* Severity: P0.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Current page body | REMOVE | Broken visible output. |
| Candidate pipeline concept | KEEP | Needed as one of seven rebuilt screens. |

### Candidate Funnel

Evidence: `aegis_ui_phase1b_audit/candidate_funnel.workspace.txt`

* Current purpose: raw candidates through coverage/exclusion/promotion/capture tickets.
* Observed purpose: diagnostic funnel with many zero counts.
* Works: explains no raw candidates available and shows funnel counts.
* Confusing: technical terms and “evidence available” repeated.
* Duplicated elsewhere: Candidate Pipeline and Evidence Detail.
* Operator value: low for daily operator; high for diagnostics.
* No operator value: narrative attribution placeholder.
* Missing information: whether user action is required.
* Empty-state problems: “NONE_AVAILABLE” code leads the state.
* Status-state problems: technical empty state.
* Hierarchy problems: raw funnel over operator question.
* Coupling issues: diagnostic artifact renderer.
* Severity: P2.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Funnel counts | KEEP in diagnostics | Useful evidence. |
| Technical empty code | REPLACE | Plain state needed. |
| Narrative placeholder | REMOVE | No value. |

### Evidence / Audit Detail

Evidence: `aegis_ui_phase1b_audit/evidence_audit_detail.workspace.txt`

* Current purpose: verified runtime evidence and claim guard.
* Observed purpose: raw evidence graph/status page.
* Works: provides graph status, paths, safety policy statements.
* Confusing: too technical for primary screens, acceptable for evidence detail.
* Duplicated elsewhere: Engineering/System Health.
* Operator value: evidence verification and safety guard proof.
* No operator value: excessive raw path detail unless user is troubleshooting.
* Missing information: grouped evidence by operator screen.
* Empty-state problems: not observed.
* Status-state problems: active mode BLOCKED vs human-approved runtime ready needs plain explanation.
* Hierarchy problems: evidence path density high.
* Coupling issues: direct artifact/path rendering.
* Severity: P2.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Verified graph status | KEEP | Evidence-detail core. |
| Safety policy statements | KEEP | Important. |
| Raw paths | IMPROVE | Collapse/group. |
| Claim guard | KEEP | Useful for audit. |

### Operator Cockpit Legacy

Evidence: `aegis_ui_phase1b_audit/operator_cockpit_legacy.workspace.txt`

* Current purpose: legacy consolidated cockpit.
* Observed purpose: stale/wrong-session legacy operator action surface.
* Works: exposes legacy data for investigation.
* Confusing: shows `AVAILABLE Today` while surfacing a 2026-05-26 SPY candidate needing review.
* Duplicated elsewhere: Command Center.
* Operator value: none as a primary operator screen.
* No operator value: stale candidate action content.
* Missing information: current-day boundary warning.
* Empty-state problems: none; worse, it shows stale content as live.
* Status-state problems: `AVAILABLE Today` contradicts stale candidate evidence.
* Hierarchy problems: stale action first.
* Coupling issues: legacy cockpit bypasses rebuilt product semantics.
* Severity: P0.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Entire legacy cockpit as operator route | REMOVE | Stale/wrong-day action exposure. |
| Underlying diagnostics | KEEP only in Evidence Detail | For engineering forensics. |

### Exit Review

Evidence: `aegis_ui_phase1b_audit/exit_review.workspace.txt`

* Current purpose: exit review decision visibility.
* Observed purpose: advisory exit summary and portfolio context.
* Works: shows exit summary, no submit/transmit language, evidence available.
* Confusing: one open position vs Positions showing 36; unclear scope.
* Duplicated elsewhere: Positions and Position Review.
* Operator value: exit monitoring if scoped accurately.
* No operator value: generic advisory disclaimers repeated.
* Missing information: why only one position is in exit review.
* Empty-state problems: not observed.
* Status-state problems: scope mismatch with open positions.
* Hierarchy problems: summary counts without scope explanation.
* Coupling issues: combines multiple exit/projection surfaces.
* Severity: P1.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Exit summary | IMPROVE | Useful but scope unclear. |
| Portfolio context | IMPROVE | Needs consistency with positions. |
| Evidence statements | KEEP secondary | Good safety boundary. |

### Open Paper Positions

Evidence: `aegis_ui_phase1b_audit/open_paper_positions.workspace.txt`

* Current purpose: canonical simulated paper positions.
* Observed purpose: open positions table with missing ledger warning and action buttons.
* Works: displays 36 open rows.
* Confusing: says `PAPER_POSITION_LEDGER MISSING` while showing 36 positions; action buttons include `Record Exit` despite governance action concerns.
* Duplicated elsewhere: Positions.
* Operator value: raw open position detail.
* No operator value: candidate contract IDs in primary table.
* Missing information: source explanation for rows if ledger is missing.
* Empty-state problems: not observed.
* Status-state problems: missing ledger plus rows is contradictory.
* Hierarchy problems: raw IDs dominate.
* Coupling issues: legacy workflow renderer bypasses new Positions product rules.
* Severity: P1.

Components:

| Component | Classification | Reason |
| --- | --- | --- |
| Open rows | KEEP via Positions | Valuable data. |
| Raw IDs | REMOVE from primary | Evidence-only. |
| Record Exit buttons | IMPROVE/RESTRICT | Need actionability gate and scope. |
| Missing ledger banner | IMPROVE | Must explain source of displayed rows. |

## Cross-Screen Technical Coupling Findings

* `pages/index.js` remains the central oversized renderer and mixes product decisions with artifact interpretation.
* `main.js` still owns broad shell behavior including navigation, drawers, Ask Aegis, commands, and form workflows.
* `route_metadata.js` and `navigation_schema.js` expose too many Aegis routes for the desired seven-screen rebuild.
* `run_ops_dashboard_v1.py` hosts many raw and semi-operator endpoints; the new seven endpoint boundary is not yet cleanly implemented.
* Current UI uses both operator-ready concepts and raw backend governance vocabulary simultaneously.

## Overall Severity Ranking

| Severity | Screen | Reason |
| --- | --- | --- |
| P0 | Candidate Pipeline | Visible JavaScript error: `payload is not defined`. |
| P0 | Operator Cockpit Legacy | Shows 2026-05-26 candidate as today/actionable context. |
| P1 | Command Center | Does not answer daily workflow cleanly; action/status ambiguity. |
| P1 | Positions | Mixes positions and candidate workflow; missing mark clarity. |
| P1 | Performance | Contract/internal degraded state leads page. |
| P1 | Sleeve Analytics | Contract/internal degraded state leads page. |
| P1 | Engineering | Contract/raw evidence leads repair workflow. |
| P1 | Research Lab | State taxonomy and empty groups confuse autonomous progress. |
| P1 | Exit Review | Scope mismatch with open positions. |
| P1 | Open Paper Positions | Missing ledger plus displayed rows is contradictory. |
| P2 | Position Review | Useful content but artifact/proof language leads. |
| P2 | Research Workspace | Mostly link hub; state answer missing. |
| P2 | Research Review | Useful briefs, but artifact/raw state language remains. |
| P2 | Runtime Timeline | Useful timing, too much domain detail first. |
| P2 | Evidence / Audit Detail | Valuable but too raw; acceptable only as detail page. |

## Recommendation

Do not patch individual screens.

First implementation slice remains:

1. Build a new Today / Command Center screen from `/api/aegis/operator/today` or a wrapped operator-ready endpoint.
2. Remove legacy `Operator Cockpit` from primary operator navigation.
3. Route `/aegis-candidates` away from the broken renderer or mark it unavailable until rebuilt.
4. Accept the Today screen only after screenshot review proves a non-engineer can answer:
   * Is Aegis okay today?
   * Did anything run?
   * Are there positions?
   * Are there candidates?
   * Is anything blocked?
   * Do I need to do anything?
   * What happens next?

No other screen should be rebuilt until Today / Command Center is screenshot-approved.

