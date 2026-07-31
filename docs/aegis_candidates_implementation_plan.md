# Aegis Candidates Implementation Plan

## Mission

Prove the rebuilt Candidates screen can be built from existing Aegis data and backend systems before implementation begins.

This is an implementation plan only. It does not authorize UI code changes, route changes, backend logic changes, candidate-generation changes, trading changes, paper execution changes, or canonical artifact changes.

Product authority:

* `docs/aegis_candidates_screen_spec.md`
* `docs/aegis_positions_candidates_boundary.md`
* `docs/aegis_today_screen_spec.md`

Primary question:

```text
What might we own next?
```

## Current State

### Current Screen(s)

* `/aegis-candidates`
* `/aegis-candidate-funnel`
* candidate sections currently embedded in `/aegis-positions`
* candidate summaries currently embedded in Today / Command Center
* legacy operator cockpit candidate card surfaces

### Current Components

* `renderAegisCandidatesWorkflow()` in `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`.
* Candidate lifecycle summary cards.
* Actionable Candidates table.
* Current Session Candidates table.
* Open Paper Positions table inside Candidates.
* Carry-forward / Lifecycle Context disclosure.
* Skipped / Expired candidates section.
* Paper Rehearsal Golden Path section.
* Candidate Funnel diagnostic page.
* Candidate capture panels currently embedded in Positions.
* Candidate UI projection/debug panels in operator cockpit workflows.

### Current Problems

* Candidate information is scattered across Candidates, Positions, Candidate Funnel, Command Center, and legacy cockpit pages.
* `/aegis-candidates` currently uses broad operator state snapshots rather than a candidate-only operator-ready envelope.
* The current Candidates page includes Open Paper Positions, which violates the Candidates product boundary.
* It exposes raw lifecycle and paper session language before answering whether any candidate is actionable.
* It includes diagnostic/projection-source details as primary content.
* Some legacy candidate surfaces expose npm repair commands, raw artifact paths, and internal lifecycle labels as operator-facing content.
* Historical candidate carry-forward and open position context can appear next to current-session candidates without a clear actionability boundary.

### Current Coupling Issues

* `renderAegisWorkflowPage("candidates")` fetches the operator state snapshot rather than a candidate-specific endpoint.
* `renderAegisCandidatesWorkflow()` reads dashboard paper projection, candidate lifecycle projection, sessions, open positions, carry-forward context, and paper rehearsal sections in one renderer.
* Candidate detail/render helpers are shared with legacy cockpit and diagnostics flows.
* Candidate rows and candidate capture status are also built inside `_positions_lightweight_payload_v1()`.
* Candidate Funnel has its own diagnostic body, creating another candidate route with overlapping purpose.

### Current Operator Confusion

* Operators cannot tell quickly whether candidates are actionable, non-actionable, waiting, or merely diagnostic.
* Open paper positions on Candidates make the screen look like a holdings page.
* Internal terms such as lifecycle projection, paper rehearsal, skipped, expired, canonical projection, and command state appear before plain candidate status.
* Candidate capture workflow appearing on Positions makes it unclear where candidate action belongs.
* Candidate Funnel duplicates diagnostic counts without becoming the main candidate answer.

## Target State

### Screen Purpose

Show current-day candidate opportunity, actionability, qualification/readiness, blockers, and next evaluation time.

The rebuilt screen must not display open holdings, P&L, portfolio exposure, position performance, or position review details as primary content.

### Operator Questions Answered

* Are there candidates?
* Are any candidates actionable?
* Which candidates are non-actionable?
* Why are candidates not actionable?
* What candidate universe was evaluated?
* Did candidate generation run or is it waiting?
* What happens next?
* Do I need to do anything?

### Components Required

* Candidate Status Summary.
* Actionable Candidate Queue.
* Candidate Universe Summary.
* Non-Actionable Candidate List.
* Qualification and Readiness Columns.
* Candidate Blocker Summary.
* Next Candidate Evaluation.
* Candidate Detail Drawer.
* Candidate Evidence Drawer, collapsed by default.

### Components Reused

| Component | Source | Reuse plan | Reason |
| --- | --- | --- | --- |
| Candidate lifecycle row data | `candidate_lifecycle_projection_v1` via existing API paths | Reuse data, not current layout | It preserves current-session candidate identity and state. |
| Candidate actionability helpers | `candidateLifecycleAllowedActions()` and queue audit concepts | Improve | Useful only when gated to current-day actionable rows. |
| Candidate table primitive | current table helpers | Reuse | Tables are appropriate if compact and sorted by actionability. |
| Candidate detail disclosure | lifecycle detail sections | Improve | Details should be row-driven and collapsed. |
| Candidate Funnel counts | Candidate Funnel diagnostics | Reuse as secondary summary/diagnostics | Useful for explaining no-candidate outcomes. |
| Boundary reports | signal/contract/construction/duplicate reports | Reuse | They explain why candidates are missing or non-actionable. |

### Components Bypassed

| Component | Source | Bypass plan | Reason |
| --- | --- | --- | --- |
| Open Paper Positions section on Candidates | `renderAegisCandidatesWorkflow()` | Do not render in rebuilt Candidates | Open holdings belong on Positions. |
| Paper Rehearsal Golden Path | `renderPaperTradeGoldenPathSection()` | Keep out of primary Candidates | Rehearsal is diagnostic/evidence, not candidate actionability. |
| Canonical projection source details | current Candidates details | Move to collapsed evidence/diagnostics | Raw source paths are not primary operator content. |
| Refresh Candidate Projection button | current Candidates | Do not show as primary workflow | Operator should see next state and evidence, not npm/repair mechanics. |
| Legacy cockpit candidate cards | `renderCockpitCandidateCards()` | Bypass | Tall cards are not the desired candidate workflow. |
| Candidate workflow content on Positions | `renderPositionsWorkspace()` | Remove from Positions and route to Candidates | Boundary violation. |

### Components Removed From Candidates

* Open Paper Positions table.
* Portfolio exposure summaries.
* Position P&L.
* Position Review details/prose.
* Paper rehearsal as primary content.
* Raw projection source paths as primary content.
* Raw npm commands as primary operator messages.

## Data Mapping

| Visible field | Data source | Current source location | Transformation required | Missing source | Reliability level |
| --- | --- | --- | --- | --- | --- |
| Requested day | route query / operator day | route params and API request | Preserve exact requested day | None | High if explicit day preserved |
| Source day | candidate artifacts / surface readiness | candidate lifecycle, signal boundary, queue audit | Compare with requested day; non-current rows cannot be actionable | None | High when gated |
| Paper session id | candidate lifecycle / paper session ledger | `candidate_lifecycle_projection_v1.paper_session_id` | Display only as secondary context | None | Medium-High |
| Candidate universe count | lifecycle projection / diagnostics | `current_session_candidates`, candidate diagnostics | Count current-session candidate universe | Diagnostics may be missing on quiet days | Medium |
| Output-intent candidate count | signal evidence boundary / lifecycle | `signal_evidence_present_count`, filtered output rows | Count final output-intent candidates only | Signal boundary may be missing | Medium-High |
| Actionable candidate count | queue audit / lifecycle allowed actions | queue audit OPERATOR_ACTION_REQUIRED, lifecycle allowed actions | Count only current-day rows with actionability gates passed | Queue audit can be missing | Medium |
| Non-actionable candidate count | lifecycle + boundary reports | lifecycle rows minus actionable rows | Classify by blocker/duplicate/readiness reason | Some row reasons may be partial | Medium |
| Candidate id | lifecycle row | `candidate_id` | Display in details, not primary if symbol is clearer | Some older rows may use contract id | Medium |
| Symbol | lifecycle / signal evidence | row `symbol` | Primary row label | None | High |
| Sleeve/strategy | lifecycle / contract / signal | row sleeve/strategy fields | Display as candidate source; not exposure | May be missing for rejected rows | Medium |
| Direction | lifecycle / contract | row `direction` | Display if available | Some diagnostics rows may omit | Medium |
| Planned entry | construction/contract/lifecycle | row planned entry / contract fields | Show as candidate plan, not actual position entry | Missing until constructed | Medium |
| Planned stop | construction/contract/lifecycle | row planned stop / contract fields | Show as readiness field | Missing until constructed | Medium |
| Quantity | construction/contract | row quantity fields | Show only if constructed | Missing until constructed | Medium |
| Qualification status | candidate readiness / lifecycle | readiness fields, lifecycle state | Translate to ready, blocked, waiting, rejected | May require multiple boundaries | Medium |
| Readiness status | candidate readiness report | candidate readiness fields | Plain language status | Readiness artifact may be missing | Medium |
| Signal evidence outcome | signal evidence boundary | `aegis_signal_evidence_boundary_v1` | Map to present, missing, rejected lineage, review-only | None if boundary exists | Medium-High |
| Contract outcome | contract generation boundary | `aegis_contract_generation_boundary_v1` | Map to valid, blocked, rejected, not eligible | Boundary may be missing | Medium |
| Construction outcome | construction boundary | `aegis_construction_input_boundary_v1` | Map to constructed, blocked, rejected, not eligible | Boundary may be missing | Medium |
| Duplicate status | duplicate candidate report | `aegis_duplicate_candidate_v1` | Suppress or explain repeated candidates | Duplicate report may be missing | Medium |
| Market data status | market data inputs/coverage | market data inputs and coverage reports | Current/stale/missing candidate mark status | Coverage may be partial | Medium |
| Blocker reason | readiness + boundary reports | candidate state and boundary reports | Choose most specific human-readable reason | Some rows may have multiple blockers | Medium |
| Next evaluation time | run history / runtime timeline | run history, scheduler/timeline artifacts | Display next candidate generation/evaluation time | Scheduler data may be missing | Medium |
| Rejected lineage count | signal evidence boundary / lifecycle | rejected intent count | Summary only; diagnostics/details below | None if boundary exists | Medium |
| Evidence refs | source artifacts and hashes | boundary paths, lifecycle paths, queue audit | Collapse by default | Some source refs missing | Medium |
| Candidate action controls | queue audit + readiness gates | candidate lifecycle allowed actions, queue audit | Render only if current-day and actionability proven | None | High only when all gates pass |

## State Matrix

### NORMAL

Visible message:

```text
Candidate evaluation is current for the selected day.
```

Components shown:

* Candidate Status Summary
* Candidate Universe Summary
* Actionable Candidate Queue if non-empty
* Non-Actionable Candidate List if non-empty
* Next Candidate Evaluation
* Evidence collapsed

Components hidden:

* Open holdings
* Position P&L
* Portfolio exposure
* Position Review content
* Paper rehearsal diagnostics as primary content

Operator expectation:

The operator can see whether any candidate might become owned next and why.

### NO_ACTIVITY

Visible message:

```text
No candidates qualified for review today.
```

or:

```text
Candidate generation has not run yet. Waiting for the next scheduled run.
```

Components shown:

* Candidate Status Summary
* Candidate Universe Summary if generation ran
* Next Candidate Evaluation
* Reason no candidate is actionable

Components hidden:

* Action buttons
* Open positions
* P&L/exposure

Operator expectation:

No candidate action is needed; the page explains whether that is because nothing ran or nothing qualified.

### BLOCKED

Visible message:

```text
Candidate workflow is unavailable because current-day candidate evidence is missing, stale, or inconsistent.
```

Components shown:

* Blocked state summary
* Reason
* Impact
* Next step
* Evidence collapsed

Components hidden:

* Actionable Candidate Queue
* Candidate action buttons
* Candidate tables that imply current actionability
* Open holdings/P&L/exposure

Operator expectation:

Do not act on candidates until current-day candidate evidence is repaired.

### DEGRADED

Visible message:

```text
Candidate summary is visible, but some readiness evidence is incomplete.
```

Components shown:

* Candidate Status Summary
* Candidate Universe Summary
* Non-actionable rows with known reasons
* Only candidate actions whose row-level gates are complete
* Evidence collapsed

Components hidden:

* Actions for rows with partial evidence
* Raw diagnostics as primary content
* Position data

Operator expectation:

Some candidate information is useful, but actionability is strict and limited.

### NEEDS_USER_ACTION

Visible message:

```text
One or more candidates are ready for operator review.
```

Components shown:

* Actionable Candidate Queue
* Candidate row reason
* Allowed row actions
* Non-actionable summary below
* Evidence collapsed

Components hidden:

* Open holdings/P&L/exposure
* Action controls for non-actionable rows

Operator expectation:

Review only the listed actionable candidate rows. Non-actionable rows are explanatory.

### MANUAL_IB_CAPTURE_READY

Visible message:

```text
Manual paper capture is ready for the listed candidate.
```

Components shown:

* Current-day candidate row
* Symbol/sleeve/direction/planned entry/stop/quantity if available
* Confirm Captured / Mark Not Captured / Defer only when gates allow
* Plain reason why capture is ready

Components hidden:

* Position holdings table
* Position P&L/exposure
* Broker/live/autonomous actions
* Capture controls for stale, duplicate-suppressed, already captured, or non-current candidates

Operator expectation:

This is the only screen that may show candidate capture actions, and only for proven current-day actionable candidates.

## Screenshot Acceptance

A non-engineer must be able to answer:

### Are there candidates?

Acceptance:

* Candidate count/universe appears near the top.
* The page does not begin with raw lifecycle, contract, or source-path language.

### Are any actionable?

Acceptance:

* Actionable count is visible.
* Actionable rows are separated from non-actionable rows.
* If none are actionable, that is stated plainly.

### Why are they not actionable?

Acceptance:

* Blocker/reason summary is visible in operator language.
* Rejected, duplicate-suppressed, missing evidence, waiting, and blocked candidates are distinguishable.

### What happens next?

Acceptance:

* Next scheduled candidate generation/evaluation or waiting reason is visible.
* Past events are not mislabeled as next events.

### Do I need to do anything?

Acceptance:

* Candidate actions appear only for current-day, readiness-approved, non-suppressed actionable rows.
* No holdings, P&L, exposure, or Position Review details appear.

## Dependencies

### API Dependencies

* Existing operator snapshot endpoints can provide current candidate lifecycle data but are too broad for the final screen.
* Existing `/api/aegis/positions` currently contains candidate rows, but it should not be the final candidate API without extraction.
* Candidate-specific data exists in candidate lifecycle projection, signal evidence boundary, contract generation boundary, construction boundary, duplicate candidate report, market data inputs, queue audit, and run history.
* Candidate screen can be built from existing artifacts with a candidate-only operator envelope; no candidate generation logic needs to change.

### Existing Reusable Components

* Table primitives.
* Compact metric cards, used sparingly.
* Details disclosure/evidence drawer.
* Candidate lifecycle row helpers, after product-language translation.
* Queue audit classification concepts.
* Day/source gating patterns from Today.

### Existing Problematic Components

* `renderAegisCandidatesWorkflow()` as currently structured.
* Open Paper Positions section inside Candidates.
* Paper Rehearsal Golden Path as primary content.
* Candidate Funnel as a separate diagnostic-heavy answer.
* Candidate projection debug panel with raw source paths.
* Candidate workflow content inside Positions.
* Legacy cockpit candidate cards.

### Risks

* Existing candidate evidence is spread across many artifacts; the implementation must choose a deterministic precedence order for blocker reasons.
* Some days legitimately have no candidates; the UI must distinguish no-run, no-qualified, blocked, and degraded.
* Duplicate/open-position policy can mention existing positions, but must not import holdings or P&L into Candidates.
* Candidate actionability must remain stricter than candidate visibility.
* Historical/carry-forward candidates must not appear as current actionable rows.

## Boundary Violations To Fix During Implementation

| Current behavior | Why it is wrong | Owning screen |
| --- | --- | --- |
| Candidates renders Open Paper Positions. | Open holdings are current ownership, not possible future ownership. | Positions. |
| Candidate workflow appears on Positions. | Candidate review/capture does not answer what is owned. | Candidates. |
| Candidate summaries appear in Command Center, Positions, Candidate Pipeline, Candidate Funnel, and legacy cockpit. | Duplicate candidate workflows reduce trust. | Candidates owns details; Today owns summary only. |
| Candidate Funnel exposes raw diagnostics as a separate primary candidate answer. | Diagnostics should support the candidate answer, not replace it. | Candidates diagnostics/evidence. |
| Candidate page leads with lifecycle/projection vocabulary. | Operators need actionability and blockers first. | Candidates should translate lifecycle into operator language. |
| Candidates displays paper rehearsal golden path. | Rehearsal is evidence/diagnostic, not current candidate workflow. | Evidence/System Health or collapsed diagnostics. |
| Candidate actions can be implied by lifecycle state alone. | Actionability requires current-day, readiness, duplicate, and governance gates. | Candidates with strict gate. |
| Open-position duplicate blockers risk showing holdings detail. | Candidates may cite conflict, not render ownership/P&L. | Candidates for blocker; Positions for ownership. |

## Implementation Order Estimate

1. Define the operator-ready candidates envelope from existing candidate artifacts without changing producers.
2. Establish deterministic blocker/actionability precedence from queue audit, readiness, duplicate, signal, contract, construction, and market data evidence.
3. Build a new isolated Candidates renderer against that envelope.
4. Render Candidate Status Summary, Actionable Candidate Queue, Non-Actionable List, Blocker Summary, Next Evaluation, and collapsed Evidence.
5. Exclude open holdings, P&L, exposure, and Position Review content.
6. Verify screenshot against `docs/aegis_candidates_screen_spec.md`.
7. Add tests forbidding open position/P&L/exposure content and stale/wrong-day action buttons on Candidates.

## Feasibility Conclusion

The Candidates rebuild is feasible from existing backend systems. The required candidate data already exists in candidate lifecycle, signal evidence, contract generation, construction, duplicate classification, market data, queue audit, and run-history artifacts. The main implementation risk is not missing candidate evidence; it is extracting a candidate-only operator envelope and preventing legacy Positions/Open Paper Positions/Rehearsal content from leaking into the primary Candidates screen.
