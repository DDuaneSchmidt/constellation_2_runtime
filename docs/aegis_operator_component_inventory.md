# Aegis Operator Component Inventory - Phase 1C

## Scope

This inventory evaluates visible operator-facing UI components from current browser output and current code. It does not redesign, implement, or mock anything.

Visible browser evidence is under:

```text
aegis_ui_phase1b_audit/
```

Primary code evidence:

* `constellation_2/phaseL/ui/static/operator_shell/main.js`
* `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`
* `constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js`
* `constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js`
* `constellation_2/phaseL/ui/static/operator_shell/aegis_components/index.js`
* `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`

Runtime baseline observed before this inventory:

* verified graph: READY
* runtime truth: PARTIAL_CONTEXT / BLOCKED
* safety gates: disabled by policy

## Classification Key

* `KEEP`: valuable component concept, usable with minor placement/wording discipline.
* `IMPROVE`: useful concept but current presentation increases confusion or density.
* `REPLACE`: current component should be replaced by a different operator-facing expression of the same underlying need.
* `REMOVE`: no meaningful operator value in primary UI; may remain in diagnostics/evidence if useful.

## Component Inventory

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Left Navigation
Screen(s): All operator shell screens
Purpose: Navigate between operator and engineering surfaces.
Current Data Source: `navigation_schema.js`, `route_metadata.js`.
Current State Dependencies: current URL, workspace/nav route mapping, engineering drawer state.

Operator Question Answered: Where can I go?

Operator Value: HIGH

Classification: IMPROVE

Reasoning: Navigation is essential, but the current nav exposes too many engineering and legacy routes, including duplicated Performance/Research paths and legacy cockpit surfaces. It improves access but increases confusion.

Evidence: `navigation_schema.js`; visible in all screenshots; Phase 1B found duplicate paths and legacy route exposure.

Dependencies: route registry, `main.js` active nav logic, `SHELL_ROUTES` in `run_ops_dashboard_v1.py`.

Risks of Removal: Operator loses discoverability and route access.

Possible Improvement Opportunities: Analysis only: reduce primary nav to seven screens, move legacy/engineering-only routes behind Evidence/System Health.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Header / Topbar
Screen(s): All operator shell screens
Purpose: Show brand, runtime mode, readiness, timestamps.
Current Data Source: `main.js` state and runtime fetches.
Current State Dependencies: runtime status, top readiness, environment/timestamp fields.

Operator Question Answered: What environment and readiness context am I viewing?

Operator Value: MEDIUM

Classification: IMPROVE

Reasoning: A persistent header is useful, but visible output often shows `UNKNOWN` or generic readiness. It does not answer daily workflow questions and can conflict with page-specific blocked/degraded states.

Evidence: captured pages show header text like `UNKNOWN`, `Runtime Mode Production`, `Readiness READY`.

Dependencies: `renderTopBar`, `renderHeaderOperationalTimestamps`, runtime status endpoints.

Risks of Removal: Loss of global context.

Possible Improvement Opportunities: Analysis only: show requested day, source day, and safe/monitoring mode in plain language.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Global Readiness Status Pill
Screen(s): All operator shell screens
Purpose: Display high-level readiness.
Current Data Source: runtime status and view-level readiness fields.
Current State Dependencies: top readiness state, runtime kernel, page render metadata.

Operator Question Answered: Is Aegis ready?

Operator Value: MEDIUM

Classification: REPLACE

Reasoning: Current `READY` pills can coexist with runtime truth BLOCKED or page DEGRADED states, causing trust failures. The concept matters, but the visible form is too coarse.

Evidence: topbar `READY` appears while audit says runtime truth BLOCKED; Performance/Engineering pages show DEGRADED.

Dependencies: `renderStatusPill`, `main.js`, runtime truth.

Risks of Removal: Loss of quick health signal.

Possible Improvement Opportunities: Analysis only: replace with plain scoped statement: “Paper monitoring readable; runtime restricted.”

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Global Status Rail / Kernel Rail
Screen(s): All operator shell screens
Purpose: Show status links and runtime kernel surface indicators.
Current Data Source: `main.js`, status rail state, route metadata.
Current State Dependencies: runtime state, route mapping.

Operator Question Answered: What subsystems have status concerns?

Operator Value: LOW

Classification: IMPROVE

Reasoning: Can help engineers, but it is architecture-heavy and duplicates System Health. It increases visual density for operators.

Evidence: code `renderKernelRail`, status rail links; visible shell side/status areas.

Dependencies: runtime status model, route registry.

Risks of Removal: Engineers lose quick cross-surface status access.

Possible Improvement Opportunities: Analysis only: move to System Health or Evidence Detail.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Search / Command Palette
Screen(s): Global shell
Purpose: Search routes and commands.
Current Data Source: `main.js`, route entries.
Current State Dependencies: route registry.

Operator Question Answered: How do I find a screen?

Operator Value: MEDIUM

Classification: KEEP

Reasoning: Useful for navigation, but should not substitute for a clear primary workflow.

Evidence: `renderPalette`, sidebar search in `main.js`, visible shell search controls.

Dependencies: route registry, palette entries.

Risks of Removal: Reduced discoverability for infrequent routes.

Possible Improvement Opportunities: Analysis only: restrict search results to operator-approved routes first.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Context Rail
Screen(s): Many screens
Purpose: Show evidence/context after workspace content loads.
Current Data Source: page render `contextHtml`, trust panel, drawer state.
Current State Dependencies: selected route/page.

Operator Question Answered: What evidence backs this screen?

Operator Value: LOW

Classification: REPLACE

Reasoning: Evidence is valuable, but the rail encourages raw architecture content beside workflow pages. It should not be primary.

Evidence: `contextRailContent`, `renderTrustPanel`, phase screenshots include contract/evidence context.

Dependencies: page renderers, evidence drawer logic.

Risks of Removal: Loss of always-visible provenance for engineers.

Possible Improvement Opportunities: Analysis only: make Evidence / Audit Detail the primary location for raw context.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Evidence Drawer
Screen(s): Global; Evidence buttons across screens
Purpose: Show detailed evidence payloads.
Current Data Source: `data-evidence-*` attributes and evidence payloads.
Current State Dependencies: evidence button presence.

Operator Question Answered: What evidence supports this item?

Operator Value: MEDIUM

Classification: KEEP

Reasoning: Useful when evidence exists and the operator intentionally opens it. It should not appear or be promoted without evidence.

Evidence: `openEvidenceDrawer`, `evidenceDrawer`, `renderEvidenceRefs`.

Dependencies: evidence payloads, drawer event handling.

Risks of Removal: Loss of auditability.

Possible Improvement Opportunities: Analysis only: only show when current item has evidence and label it “View Evidence.”

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Ask Aegis Global Button / Panel
Screen(s): Command Center, Positions, Performance, Position Review, Research, Engineering, global handlers
Purpose: Ask AI assistant operational questions.
Current Data Source: `aegis_ai_operations_response_v1`, `/api/aegis/ai-operations/ask`, context artifacts.
Current State Dependencies: AI operations context, selected prompt/object.

Operator Question Answered: Why is this happening?

Operator Value: MEDIUM

Classification: IMPROVE

Reasoning: Valuable if grounded, but currently appears before the core status is clear on multiple pages and duplicates itself.

Evidence: visible “Ask Aegis” on many captured screens; `renderAskAegisGroundedResponse`, `askAegisAiOperations`.

Dependencies: AI operations context, response self-check, source artifacts.

Risks of Removal: Operator loses explanatory assistant.

Possible Improvement Opportunities: Analysis only: demote until the page answers core workflow questions.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Surface Contract Banner
Screen(s): Command Center, Positions, Performance, Position Review, Research, Engineering, Sleeve Analytics
Purpose: Show contract status, requested/source/context day, render/action/metric flags.
Current Data Source: `aegis_operator_surface_contract_v1`.
Current State Dependencies: surface contract row, semantic invariants.

Operator Question Answered: Mostly none for non-engineers; partially “is this page allowed to render?”

Operator Value: LOW

Classification: REPLACE

Reasoning: It prevents stale rendering in theory, but as visible content it leaks backend architecture. It often leads the page and pushes operator answers down.

Evidence: captured texts start with “Operator Surface Contract”, “Surface checks passed”, “Raw contract”.

Dependencies: surface readiness, semantic invariants, operator surface contract.

Risks of Removal: If removed from backend, safety risk. If removed from primary UI only, low risk.

Possible Improvement Opportunities: Analysis only: translate to a one-line visible operator state; move raw contract to Evidence / Audit Detail.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Raw Contract JSON Details
Screen(s): Performance, Sleeve Analytics, Engineering
Purpose: Expose raw surface contract row.
Current Data Source: `aegis_operator_surface_contract_v1`.
Current State Dependencies: contract row.

Operator Question Answered: None for normal operation.

Operator Value: NONE

Classification: REMOVE

Reasoning: Engineering evidence only. It increases confusion and makes the UI feel like a diagnostics dump.

Evidence: visible text includes “Raw contract {…}” on Performance, Sleeve Analytics, Engineering.

Dependencies: contract renderer.

Risks of Removal: None if retained in Evidence / Audit Detail.

Possible Improvement Opportunities: Analysis only: collapse under “Technical evidence.”

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Command Center Metric Strip
Screen(s): Command Center
Purpose: Show Open Positions, Unrealized P&L, Awaiting Review, Needs Attention, Mode Readiness.
Current Data Source: operator cockpit, canonical operator state, queue audit, P&L artifacts.
Current State Dependencies: day/source state, queue audit, position/P&L projections.

Operator Question Answered: What needs attention and what is open?

Operator Value: HIGH

Classification: IMPROVE

Reasoning: The right idea, but current values include `Unrealized P&L n/a`, `Needs Attention 2`, `Mode Readiness UNKNOWN`, which do not explain impact.

Evidence: `command_center.workspace.txt`.

Dependencies: `renderCommandMetricStrip`, operator cockpit payloads.

Risks of Removal: Loses daily at-a-glance summary.

Possible Improvement Opportunities: Analysis only: use plain states and hide unavailable metrics unless explained.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Command Center Attention Queue
Screen(s): Command Center
Purpose: List true operator-actionable blockers.
Current Data Source: command center queue audit and operator cockpit action rows.
Current State Dependencies: classification/actionability rules.

Operator Question Answered: Does David need to do anything?

Operator Value: HIGH

Classification: IMPROVE

Reasoning: High-value concept. Current output still shows actionable count while global actions are disabled, so the semantics need tightening.

Evidence: `command_center.workspace.txt`: “2 actionable”.

Dependencies: queue audit, action classification.

Risks of Removal: Operator loses action queue.

Possible Improvement Opportunities: Analysis only: separate David actions from system repair and waiting.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Command Center Candidate Review Table
Screen(s): Command Center
Purpose: Show candidates awaiting operator review.
Current Data Source: command center candidate projection.
Current State Dependencies: candidate readiness, duplicate policy, day/session boundary.

Operator Question Answered: Are any candidates awaiting David?

Operator Value: MEDIUM

Classification: IMPROVE

Reasoning: Useful when non-empty and current-day. It duplicates Candidate Pipeline and should remain a concise preview.

Evidence: `command_center.workspace.txt`: “0 awaiting review”.

Dependencies: candidate projection, queue audit.

Risks of Removal: Command Center loses candidate preview.

Possible Improvement Opportunities: Analysis only: show only count and one-line state unless action required.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Command Center Open Positions Preview
Screen(s): Command Center
Purpose: Preview open positions.
Current Data Source: paper position ledger/read model.
Current State Dependencies: positions read model, marks.

Operator Question Answered: Are there open positions?

Operator Value: HIGH

Classification: KEEP

Reasoning: Open position count and preview are core daily operator information.

Evidence: `command_center.workspace.txt`, `positions.workspace.txt`.

Dependencies: paper position ledger, P&L/marks.

Risks of Removal: Daily screen becomes blind to open exposure.

Possible Improvement Opportunities: Analysis only: show top-level count and link to Positions.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Command Center Monitor / Safe to Ignore Section
Screen(s): Command Center
Purpose: Separate non-action rows from action queue.
Current Data Source: queue audit classifications.
Current State Dependencies: row classification.

Operator Question Answered: What can I safely ignore?

Operator Value: MEDIUM

Classification: IMPROVE

Reasoning: Useful concept, but should be summarized to avoid clutter.

Evidence: `command_center.workspace.txt` and queue audit text.

Dependencies: command center queue audit.

Risks of Removal: Non-action rows may be confused with missing data.

Possible Improvement Opportunities: Analysis only: show collapsed count and reason buckets.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Command Center Diagnostics Details
Screen(s): Command Center
Purpose: Show queue audit paths and old incorrect counts.
Current Data Source: `aegis_command_center_queue_audit_v1`.
Current State Dependencies: queue audit artifact.

Operator Question Answered: Why does the queue look this way?

Operator Value: LOW

Classification: KEEP

Reasoning: Useful only as collapsed diagnostics. Not primary operator value.

Evidence: `command_center.workspace.txt` diagnostics section.

Dependencies: queue audit artifact.

Risks of Removal: Less auditability.

Possible Improvement Opportunities: Analysis only: move raw path/count detail to Evidence / Audit Detail.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Positions Open Positions Table
Screen(s): Positions, Open Paper Positions, Command Center preview
Purpose: Show open paper positions.
Current Data Source: paper position ledger, positions read model.
Current State Dependencies: open positions, marks, P&L, sleeve attribution.

Operator Question Answered: What is open?

Operator Value: HIGH

Classification: KEEP

Reasoning: One of the most valuable components. Current version includes useful symbol/entry/mark/qty/P&L/status data.

Evidence: `positions.workspace.txt`: 36 governed open positions.

Dependencies: paper position ledger, market marks, P&L report.

Risks of Removal: Operator loses exposure visibility.

Possible Improvement Opportunities: Analysis only: clarify unavailable mark/P&L fields and hide raw IDs.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Positions Candidate Capture Panel
Screen(s): Positions
Purpose: Show candidate capture confirmation summary.
Current Data Source: positions read model and candidate capture status.
Current State Dependencies: candidate evidence/readiness/session state.

Operator Question Answered: Are there candidate capture actions?

Operator Value: LOW on Positions; MEDIUM on Candidate Pipeline

Classification: REMOVE from Positions

Reasoning: It mixes candidate workflow into the Positions page, diluting the “what is open?” purpose.

Evidence: `positions.workspace.txt` shows Today's Candidates and capture confirmation after open positions.

Dependencies: candidate state, signal evidence boundary, paper session.

Risks of Removal: Candidate state may be less visible unless Candidate Pipeline exists.

Possible Improvement Opportunities: Analysis only: move to Candidate Pipeline and keep a small candidate count on Command Center.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Positions Today's Candidates Table
Screen(s): Positions
Purpose: List current-day candidates.
Current Data Source: positions endpoint candidate rows.
Current State Dependencies: candidate readiness/actionability.

Operator Question Answered: Are there candidates?

Operator Value: LOW on Positions; HIGH on Candidate Pipeline

Classification: REMOVE from Positions

Reasoning: It belongs on Candidate Pipeline. On Positions it creates page-purpose confusion.

Evidence: `positions.workspace.txt`: “Today's Candidates”.

Dependencies: candidate pipeline artifacts.

Risks of Removal: None if Candidate Pipeline owns it.

Possible Improvement Opportunities: Analysis only: provide link/count from Positions if needed.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Position Review Cards
Screen(s): Position Review
Purpose: Explain each position’s thesis/health/evidence.
Current Data Source: `aegis_position_review_brief_v1`, context/score artifacts.
Current State Dependencies: position review artifacts, context hashes.

Operator Question Answered: What matters most for this position?

Operator Value: HIGH

Classification: KEEP

Reasoning: This is the core value of Position Review, assuming cards lead with health, thesis, risk, and monitoring.

Evidence: `position_review.workspace.txt`: 36 review briefs, unsupported claims 0.

Dependencies: position review context, score, brief artifacts.

Risks of Removal: Loss of AI-assisted position understanding.

Possible Improvement Opportunities: Analysis only: summarize positions needing attention before long card list.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Position Review Artifact Summary
Screen(s): Position Review
Purpose: Show CANONICAL status, generated time, context hash, unsupported claims count.
Current Data Source: position review artifact metadata.
Current State Dependencies: brief artifact, context hash.

Operator Question Answered: Can I audit the brief?

Operator Value: MEDIUM

Classification: IMPROVE

Reasoning: Useful for trust, but currently too prominent and implementation-oriented.

Evidence: `position_review.workspace.txt`.

Dependencies: position review artifacts.

Risks of Removal: Reduced audit clarity.

Possible Improvement Opportunities: Analysis only: move below summary or into Evidence Detail.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Candidate Pipeline Page Body
Screen(s): `/aegis-candidates`
Purpose: Intended candidate workflow page.
Current Data Source: broken renderer.
Current State Dependencies: undefined payload.

Operator Question Answered: None.

Operator Value: NONE

Classification: REMOVE

Reasoning: Visible output is only `payload is not defined`.

Evidence: `candidate_pipeline.workspace.txt`.

Dependencies: legacy `renderAegisWorkflowPage("candidates")` path.

Risks of Removal: None; current visible component is broken.

Possible Improvement Opportunities: Analysis only: replace later with Candidate Pipeline screen defined in spec.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Candidate Funnel Counts
Screen(s): Candidate Funnel
Purpose: Show raw candidates through promotion/capture ticket funnel.
Current Data Source: candidate consumption/funnel artifacts.
Current State Dependencies: candidate audit artifacts.

Operator Question Answered: Why did candidates not promote?

Operator Value: MEDIUM for diagnostics, LOW for daily operator

Classification: KEEP in Evidence / Candidate Pipeline diagnostics

Reasoning: Valuable for investigation but too technical as a primary screen.

Evidence: `candidate_funnel.workspace.txt`: raw candidates, covered universe, excluded counts.

Dependencies: candidate consumption audit, coverage artifacts.

Risks of Removal: Loses candidate forensics.

Possible Improvement Opportunities: Analysis only: summarize reasons first; raw counts later.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Candidate Funnel Narrative Attribution Placeholder
Screen(s): Candidate Funnel
Purpose: Explain what happened and why.
Current Data Source: narrative attribution artifact.
Current State Dependencies: narrative availability.

Operator Question Answered: Intended to answer why; currently no.

Operator Value: NONE

Classification: REMOVE

Reasoning: Visible text says no evidence-backed narrative is available yet; this is placeholder noise.

Evidence: `candidate_funnel.workspace.txt`.

Dependencies: narrative attribution artifact.

Risks of Removal: None until real content exists.

Possible Improvement Opportunities: Analysis only: render only when narrative exists.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Research Workspace Link Hub
Screen(s): Research Workspace
Purpose: Link to Research Review, Queue, Diagnostics, Sleeve Analytics, Performance Review.
Current Data Source: static renderer and research routes.
Current State Dependencies: route availability.

Operator Question Answered: Where can I go for research?

Operator Value: MEDIUM

Classification: IMPROVE

Reasoning: Useful as navigation, but does not answer what is being validated now.

Evidence: `research_workspace.workspace.txt`.

Dependencies: research route group.

Risks of Removal: Reduced discoverability.

Possible Improvement Opportunities: Analysis only: lead with active research state, links secondary.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Research Hypothesis Summary Counts
Screen(s): Research Lab
Purpose: Show total hypotheses, recommendation-ready, collecting evidence, etc.
Current Data Source: research console/doctor/review artifacts.
Current State Dependencies: research artifacts and validation state.

Operator Question Answered: What is research doing?

Operator Value: HIGH

Classification: IMPROVE

Reasoning: Valuable but currently mixed with repeated empty group text and unclear state terms.

Evidence: `research_lab.workspace.txt`: Total hypotheses 6, Recommendations Ready 0, Collecting Evidence 2.

Dependencies: research console, hypothesis validation, research review.

Risks of Removal: Research becomes opaque.

Possible Improvement Opportunities: Analysis only: map to operator states like Research Running / Collecting Evidence.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Research Hypothesis Cards
Screen(s): Research Lab
Purpose: Show per-hypothesis state and description.
Current Data Source: research console artifacts.
Current State Dependencies: hypothesis registry, research state, validation sample state.

Operator Question Answered: What is this hypothesis doing?

Operator Value: HIGH

Classification: IMPROVE

Reasoning: Important, but visible states like waiting/ready lack clear “what happens next” and sample timing.

Evidence: `research_lab.workspace.txt`.

Dependencies: hypothesis registry, research validation samples.

Risks of Removal: Loss of research transparency.

Possible Improvement Opportunities: Analysis only: show next expected observation and operator-action-required status.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Research Empty Group Cards
Screen(s): Research Lab
Purpose: Indicate no visible hypothesis in a group.
Current Data Source: research grouping logic.
Current State Dependencies: group counts.

Operator Question Answered: None meaningfully.

Operator Value: NONE

Classification: REMOVE

Reasoning: Repeated “No visible hypothesis” increases clutter.

Evidence: `research_lab.workspace.txt`.

Dependencies: research group renderer.

Risks of Removal: None if counts remain.

Possible Improvement Opportunities: Analysis only: suppress empty groups or show one concise empty summary.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Research Review Brief Cards
Screen(s): Research Review
Purpose: Show conclusions, confidence, why it matters, evidence, risks, decision needed.
Current Data Source: `aegis_research_review_brief_v1`.
Current State Dependencies: review brief artifacts.

Operator Question Answered: What did research find?

Operator Value: HIGH

Classification: KEEP

Reasoning: One of the more operator-readable components, though raw statuses remain.

Evidence: `research_review.workspace.txt`.

Dependencies: research review brief artifact.

Risks of Removal: Loss of research findings.

Possible Improvement Opportunities: Analysis only: replace raw `RECOMMENDATION_READY` with validation workflow state.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Research Review Artifact Header
Screen(s): Research Review
Purpose: Show artifact ID, status, counts, as-of.
Current Data Source: research review artifact metadata.
Current State Dependencies: research review brief artifact.

Operator Question Answered: Is there a research review artifact?

Operator Value: LOW

Classification: IMPROVE

Reasoning: Useful for audit but too artifact-focused as main header.

Evidence: `research_review.workspace.txt`: `Artifact AEGIS_RESEARCH_REVIEW_BRIEF_V1`.

Dependencies: research review artifact.

Risks of Removal: Less audit transparency.

Possible Improvement Opportunities: Analysis only: move artifact ID to Evidence Detail.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Runtime Timeline Operational Summary
Screen(s): Runtime Timeline
Purpose: Show operational day, last update, certification ETA, missed runs.
Current Data Source: runtime timeline projection.
Current State Dependencies: schedule/run artifacts.

Operator Question Answered: Did anything run and what happens next?

Operator Value: HIGH

Classification: KEEP

Reasoning: Strong operator value. Timing is one of the questions the shell must answer.

Evidence: `runtime_timeline.workspace.txt`: operational day, last update, certification ETA, needs attention.

Dependencies: runtime timeline projection, schedule artifacts.

Risks of Removal: Operator loses run cadence visibility.

Possible Improvement Opportunities: Analysis only: pull summary into Today screen.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Runtime Timeline Domain Certification Rows
Screen(s): Runtime Timeline
Purpose: Show per-domain certification status.
Current Data Source: domain certification/runtime projection.
Current State Dependencies: domain certification artifacts.

Operator Question Answered: What data domains are delayed/degraded?

Operator Value: MEDIUM

Classification: IMPROVE

Reasoning: Useful for engineers/operators when debugging, but too dense for first view.

Evidence: `runtime_timeline.workspace.txt`: US_EQUITIES_EOD DELAYED, intraday/volatility/rates DEGRADED.

Dependencies: domain certification artifacts.

Risks of Removal: Less detail on data readiness.

Possible Improvement Opportunities: Analysis only: summarize domain issues first, detail collapsed.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Performance Degraded Banner
Screen(s): Performance
Purpose: State that performance input is degraded/noncanonical.
Current Data Source: surface contract, semantic invariants, P&L report.
Current State Dependencies: performance artifacts and invariants.

Operator Question Answered: Can I trust performance analytics?

Operator Value: HIGH

Classification: IMPROVE

Reasoning: The warning is necessary, but current wording is contract/internal and says metrics allowed, which confuses trust.

Evidence: `performance.workspace.txt`.

Dependencies: paper P&L report, surface contract, semantic invariants.

Risks of Removal: Analytics may look trustworthy when they are not.

Possible Improvement Opportunities: Analysis only: translate to “Performance analytics unavailable/degraded because…”

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Performance Raw Contract Panel
Screen(s): Performance
Purpose: Show raw contract details.
Current Data Source: operator surface contract.
Current State Dependencies: contract row.

Operator Question Answered: None for daily use.

Operator Value: NONE

Classification: REMOVE

Reasoning: Engineering value only; primary UI noise.

Evidence: `performance.workspace.txt`: “Raw contract”.

Dependencies: surface contract.

Risks of Removal: None if evidence detail retains it.

Possible Improvement Opportunities: Analysis only: collapse under Evidence / Audit Detail.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Sleeve Analytics Degraded Banner
Screen(s): Sleeve Analytics
Purpose: State sleeve analytics are partial/degraded.
Current Data Source: sleeve analytics artifact, surface contract, semantic invariants.
Current State Dependencies: sleeve analytics status.

Operator Question Answered: Can I trust sleeve analytics?

Operator Value: HIGH

Classification: IMPROVE

Reasoning: Needed, but current display leads with contract vocabulary and raw JSON.

Evidence: `sleeve_analytics.workspace.txt`.

Dependencies: `aegis_sleeve_analytics_v1`, surface contract.

Risks of Removal: Sleeve analytics could appear falsely canonical.

Possible Improvement Opportunities: Analysis only: plain-language impact and affected sleeve summary.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Sleeve Analytics Raw Contract Panel
Screen(s): Sleeve Analytics
Purpose: Show surface contract internals.
Current Data Source: operator surface contract.
Current State Dependencies: contract row.

Operator Question Answered: None.

Operator Value: NONE

Classification: REMOVE

Reasoning: Architecture noise.

Evidence: `sleeve_analytics.workspace.txt`.

Dependencies: surface contract.

Risks of Removal: None if retained in Evidence Detail.

Possible Improvement Opportunities: Analysis only: hide by default.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Engineering Runtime Blocked Message
Screen(s): Engineering
Purpose: Explain runtime truth is blocked.
Current Data Source: runtime truth kernel and engineering priority queue/surface contract.
Current State Dependencies: runtime truth classification.

Operator Question Answered: What is broken?

Operator Value: HIGH

Classification: KEEP

Reasoning: Important system health signal.

Evidence: `engineering.workspace.txt`: runtime truth blocked.

Dependencies: runtime truth kernel, engineering priority queue.

Risks of Removal: Operators lose blocker visibility.

Possible Improvement Opportunities: Analysis only: make it the plain top issue without raw contract framing.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Engineering Raw Contract Panel
Screen(s): Engineering
Purpose: Show raw engineering surface contract.
Current Data Source: operator surface contract.
Current State Dependencies: contract row.

Operator Question Answered: None for David.

Operator Value: NONE

Classification: REMOVE

Reasoning: Engineering metadata leaks into the page before repair workflow.

Evidence: `engineering.workspace.txt`.

Dependencies: surface contract.

Risks of Removal: None if Evidence Detail keeps it.

Possible Improvement Opportunities: Analysis only: collapse as raw evidence.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Evidence Verified Graph Panels
Screen(s): Evidence / Audit Detail
Purpose: Show verified graph, evidence ledger, portal runtime model paths.
Current Data Source: verified runtime graph artifacts.
Current State Dependencies: verified graph generation.

Operator Question Answered: What evidence supports Aegis state?

Operator Value: MEDIUM

Classification: KEEP

Reasoning: Appropriate for Evidence / Audit Detail, not primary workflow screens.

Evidence: `evidence_audit_detail.workspace.txt`.

Dependencies: verified runtime graph, evidence ledger.

Risks of Removal: Loss of audit trail.

Possible Improvement Opportunities: Analysis only: group by screen and collapse paths.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Evidence Safety Policy Statements
Screen(s): Evidence / Audit Detail
Purpose: Show disabled policy gates.
Current Data Source: runtime truth/control packet.
Current State Dependencies: safety gates.

Operator Question Answered: Can Aegis act or trade?

Operator Value: HIGH

Classification: KEEP

Reasoning: Operators need assurance that broker/live/autonomous execution is disabled.

Evidence: `evidence_audit_detail.workspace.txt`, audit output.

Dependencies: runtime truth kernel, control packet.

Risks of Removal: Reduced safety transparency.

Possible Improvement Opportunities: Analysis only: summarize visibly on Today, detail here.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Operator Cockpit Legacy Candidate Action Surface
Screen(s): Operator Cockpit Legacy
Purpose: Legacy consolidated cockpit actions.
Current Data Source: legacy operator cockpit/canonical operator state.
Current State Dependencies: old projections.

Operator Question Answered: Intended “what requires action,” but currently untrustworthy.

Operator Value: NONE

Classification: REMOVE

Reasoning: Visible output shows a 2026-05-26 SPY candidate in a “Today” context. This is actively harmful.

Evidence: `operator_cockpit_legacy.workspace.txt`.

Dependencies: legacy cockpit endpoint/projection.

Risks of Removal: Low; keeps stale action route out of operator workflow.

Possible Improvement Opportunities: Analysis only: if retained, engineering-only forensic route with explicit stale warning.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Exit Review Summary
Screen(s): Exit Review
Purpose: Show exit recommendations/blocks.
Current Data Source: exit review projection and portfolio context.
Current State Dependencies: open positions, exit rules.

Operator Question Answered: Are any exits being monitored?

Operator Value: MEDIUM

Classification: IMPROVE

Reasoning: Useful concept, but scope mismatch with Positions makes it hard to trust.

Evidence: `exit_review.workspace.txt`: one open position vs Positions 36.

Dependencies: exit review projection, position context.

Risks of Removal: Loss of exit monitoring visibility.

Possible Improvement Opportunities: Analysis only: explain scope and integrate into Positions/Position Review.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Open Paper Positions Legacy Table
Screen(s): Open Paper Positions
Purpose: Show simulated open paper positions.
Current Data Source: legacy paper position renderer/read model.
Current State Dependencies: paper ledger, exit recommendation data.

Operator Question Answered: What paper positions exist?

Operator Value: MEDIUM

Classification: REPLACE

Reasoning: Shows useful rows but also says ledger missing, exposes raw IDs, and includes `Record Exit` actions.

Evidence: `open_paper_positions.workspace.txt`.

Dependencies: paper position ledger, exit review.

Risks of Removal: Redundant with Positions, but data should be preserved through Positions screen.

Possible Improvement Opportunities: Analysis only: merge value into rebuilt Positions.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Status Pills
Screen(s): All pages
Purpose: Mark states like READY, DEGRADED, HOLD, BLOCK, CANONICAL.
Current Data Source: renderer state fields.
Current State Dependencies: varied per screen.

Operator Question Answered: What state is this item in?

Operator Value: MEDIUM

Classification: IMPROVE

Reasoning: Useful visual affordance, but raw labels leak internal states and can conflict across page levels.

Evidence: many captured screens and `renderStatusPill`.

Dependencies: `renderStatusPill`, state mappings.

Risks of Removal: Lower scanability.

Possible Improvement Opportunities: Analysis only: restrict to operator state model labels.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Metric Cards
Screen(s): Command Center, Positions, Performance, Position Review, Research Review, many legacy pages
Purpose: Display count/value summaries.
Current Data Source: many artifacts and read models.
Current State Dependencies: screen data availability.

Operator Question Answered: How many / how much?

Operator Value: HIGH when curated; LOW when unavailable/internal

Classification: IMPROVE

Reasoning: Good primitive, overused. Unavailable or raw metric cards create confusion.

Evidence: many captured texts; `renderMetricCard`.

Dependencies: artifact counts, P&L, state summaries.

Risks of Removal: Loss of dashboard scanability.

Possible Improvement Opportunities: Analysis only: use only for values that answer screen questions.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Simple Tables
Screen(s): Positions, Command Center, Candidate Funnel, Open Paper Positions, many pages
Purpose: Row-based data display.
Current Data Source: varied.
Current State Dependencies: row lists.

Operator Question Answered: What items exist?

Operator Value: HIGH when scoped; LOW when raw/diagnostic

Classification: KEEP

Reasoning: Tables are necessary for positions, candidates, and tasks. Current issue is scope and raw fields, not table primitive.

Evidence: `renderSimpleTable`, visible tables.

Dependencies: row data, column renderers.

Risks of Removal: Loss of item visibility.

Possible Improvement Opportunities: Analysis only: use fewer columns and hide raw IDs.

--------------------------------------------------
COMPONENT
--------------------------------------------------

Name: Source Evidence / Trust Panels
Screen(s): Many legacy pages, Evidence Detail
Purpose: Show source refs and trust/provenance.
Current Data Source: source_refs, trust panel data.
Current State Dependencies: artifact source references.

Operator Question Answered: Why can I trust this?

Operator Value: MEDIUM in Evidence Detail; LOW in primary screens

Classification: IMPROVE

Reasoning: Important for auditability, but overexposed in primary workflow.

Evidence: `renderTrustPanel`, `renderSourceRefCard`, captured evidence sections.

Dependencies: source refs.

Risks of Removal: Lower auditability.

Possible Improvement Opportunities: Analysis only: consolidate under Evidence / Audit Detail.

## Engineering Concepts Leaking Into Operator UI

| Concept | Where it appears | Why it exists | Operators benefit? | Severity |
| --- | --- | --- | --- | --- |
| Surface Contracts | Command Center, Positions, Performance, Sleeve Analytics, Engineering | Safety/render gate | Mostly no; engineering value only when debugging | P1 |
| Semantic Invariants | Performance, Sleeve Analytics, contract banners | Prevent contradictory analytics | Indirectly yes, but raw term no | P1 |
| Internal Readiness States | Header, Evidence Detail, Engineering, legacy cockpit | Runtime truth and graph state | Only after translation | P1 |
| Governance Architecture | Evidence Detail, contract banners, control packet language | Auditability and safety proof | Evidence page yes; primary pages no | P2 |
| Engineering Metadata | Raw contract JSON, artifact paths, context hashes, source mtimes | Debugging/provenance | Engineering value only | P1 |
| Runtime Truth Kernel Labels | Evidence Detail, Engineering, topbar concepts | Authority for readiness | Needs translation | P1 |
| Candidate Boundary Terms | Candidate Funnel, Positions candidate panels | Pipeline diagnostics | Mostly diagnostics-only | P2 |
| Artifact IDs | Research Review, Position Review, Evidence | Audit trace | Evidence page yes; primary hero no | P2 |
| Raw Error Text | Candidate Pipeline `payload is not defined` | Unhandled renderer failure | No | P0 |
| Legacy Current Truth Projection | Operator Cockpit Legacy | Old consolidated cockpit | Harmful due stale candidate exposure | P0 |

## Duplication Analysis

### Duplicate Components

* Ask Aegis appears on nearly every surface.
* Contract banner appears on many pages.
* Evidence/diagnostics panels appear on most pages.
* Position tables appear on Positions, Command Center, Open Paper Positions.
* Candidate summaries appear on Command Center, Positions, Candidate Pipeline, Candidate Funnel, Operator Cockpit Legacy.
* Research status appears on Research Workspace, Research Lab, Research Review.
* Performance health appears on Performance and Sleeve Analytics, with System Health overlap.

### Duplicate Information

* Requested/source day appears in contract banners and evidence pages.
* Runtime blocked/degraded state appears in Engineering, Evidence Detail, Header, and Audit outputs.
* Open position count appears in Command Center, Positions, Position Review, Open Paper Positions.
* Research brief counts appear in Research Workspace, Research Lab, Research Review.
* Safety gate disabled language appears in Evidence Detail, audit handoff, exit review, and several advisory disclaimers.

### Duplicate Workflows

* Candidate review: Command Center, Positions, Candidate Pipeline, Candidate Funnel, Operator Cockpit Legacy.
* Research review: Research Workspace, Research Lab, Research Review.
* Position review: Positions links and Position Review page.
* Runtime repair/health: Engineering, Runtime Timeline, Evidence Detail.
* Performance: Performance, Sleeve Analytics, Position P&L columns.

### Duplicate Status Indicators

* Header readiness.
* Surface contract status.
* Runtime truth status.
* Verified graph status.
* Data quality status.
* Artifact canonical status.
* Status pills inside tables.

### Duplicate Navigation Paths

* Performance has `/aegis-paper-performance`, `/aegis-performance`, `/performance`, Sleeve Analytics, Position Review under same group.
* Research has `/aegis-research-workspace`, `/research-lab`, `/research-lab/review`, `/research-lab/blocked-work`, plus related diagnostics.
* Engineering has `/aegis-opportunities`, `/aegis-operator-cockpit`, `/aegis-verified-runtime`, `/aegis-runtime-timeline`, and many diagnostic routes.
* Positions has `/aegis-positions`, `/aegis-open-paper-positions`, `/aegis-positions-diagnostics`.

## Top 20 Components To Preserve

1. Open Positions table.
2. Command Center open positions preview.
3. Command Center metric strip concept.
4. Operator Attention Queue concept.
5. Runtime Timeline operational day/last update/ETA.
6. Research hypothesis summary counts.
7. Research hypothesis cards.
8. Research Review brief cards.
9. Position Review brief cards.
10. Position health / thesis status content.
11. Evidence safety policy statements.
12. Verified graph status in Evidence / Audit Detail.
13. Performance degraded warning concept.
14. Sleeve analytics degraded warning concept.
15. Engineering runtime blocked message.
16. Candidate funnel counts as diagnostics.
17. Evidence drawer when evidence exists.
18. Source evidence/trust panels in Evidence Detail.
19. Metric card primitive when values are meaningful.
20. Table primitive for scoped item lists.

## Top 20 Components To Eliminate Or Replace

1. Operator Cockpit Legacy candidate action surface.
2. Candidate Pipeline current body showing `payload is not defined`.
3. Raw contract JSON panels in primary pages.
4. Surface Contract banner as first visible content.
5. Research empty group cards saying “No visible hypothesis.”
6. Candidate capture panel on Positions.
7. Today's Candidates table on Positions.
8. Open Paper Positions raw ID/action-heavy legacy table as primary route.
9. Performance raw contract panel.
10. Sleeve Analytics raw contract panel.
11. Engineering raw contract panel.
12. Candidate Funnel narrative placeholder with no content.
13. Artifact ID hero on Research Review.
14. Implementation-proof language on Position Review.
15. Generic `READY` topbar state when runtime/page state is blocked/degraded.
16. Raw artifact paths in primary screen cards.
17. Duplicate Ask Aegis prominence before core status.
18. Domain certification rows before runtime summary.
19. Legacy route groups that duplicate rebuilt screen purpose.
20. Raw internal status labels in primary UI.

## Final Assessment

The current operator UI contains valuable pieces, especially open positions, runtime timing, research cards, review briefs, and safety evidence. The dominant failure is not lack of data; it is component placement and vocabulary. Too many components expose backend architecture before operator questions are answered.

The most valuable components should be preserved as data/functionality but moved into a workflow-first shell. The least valuable components are mostly raw architecture, duplicate legacy pages, and diagnostic artifacts rendered as primary product UI.

