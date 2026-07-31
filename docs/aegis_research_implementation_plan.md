# Aegis Research Screen Implementation Plan

## Scope

This is an implementation plan only. It does not authorize code changes.

The rebuilt Research screen must use existing backend evidence where possible and avoid changing research logic, trading logic, sleeve logic, candidate generation, canonical artifacts, broker policy, execution policy, or safety gates.

## Current State

Current screens:

* Research Workspace
* Research Lab
* Research Review
* Research Lab subroutes such as hypotheses, plans, evidence, blocked work, and diagnostics

Current components:

* Research Workspace Link Hub
* Research Lab Status Summary
* Hypothesis Cards
* Hypothesis Group Sections
* Research Review Brief Cards
* Research Review Artifact Header
* Diagnostics/details disclosures

Current problems:

* Research Workspace behaves mostly as a link hub rather than an answer surface.
* Research Lab and Research Review overlap on recommendation/finding state.
* Research status labels are not consistently mapped to operator-facing states.
* Next expected research progress is not consistently visible.
* Empty states do not clearly distinguish no research, waiting for evidence, blocked research, or findings ready.
* Artifact identifiers and internal state names can appear before the operator understands the research state.

Current coupling issues:

* Research landing, Lab, and Review use separate render paths and vocabulary.
* Research Review is discoverable as a route but not necessarily as the primary findings section.
* Research state is assembled from multiple artifacts without a single operator-ready envelope.
* Diagnostics are sometimes close to primary content rather than strictly secondary.

Current operator confusion:

* The operator cannot quickly tell whether research is running, collecting evidence, blocked, complete, or awaiting user action.
* “Recommendation Ready” can imply manual review or action even when autonomous validation should continue.
* Research findings, validation progress, and queue monitoring are separated across screens.

## Target State

Screen purpose:

```text
Show what Aegis is investigating, what is active, what has been learned, what is blocked, what happens next, and whether the operator needs to do anything.
```

Operator questions answered:

* What research exists?
* What is currently active?
* What has been learned?
* What is blocked?
* What happens next?
* Do I need to do anything?

Components required:

* Research Summary
* Active Research
* Findings Ready
* Research Blockers
* Next Research Step
* Collapsed Evidence

Components reused:

* existing shell navigation and route chrome
* shared cards/tables/details primitives if they support low-density operator language
* Research Lab hypothesis cards as source structure, simplified
* Research Review brief cards as findings content, simplified
* existing diagnostics details as collapsed evidence

Components bypassed:

* Research Workspace Link Hub as primary content
* artifact header as primary content
* generic route cards before research status
* raw diagnostics panels above findings or active research

Components removed from primary Research:

* route-only hub content
* raw artifact IDs as headings
* pipeline labels that are not translated
* duplicate recommendation status cards
* manual-review language unless actual operator action is required

## Proposed Operator API Boundary

Preferred endpoint:

```text
/api/aegis/operator/research
```

Purpose:

Return an operator-ready Research summary that can be rendered without page-local interpretation of raw research artifacts.

Suggested top-level fields:

```json
{
  "day": "2026-05-30",
  "source_day": "2026-05-30",
  "status": "NORMAL|NO_RESEARCH|RESEARCH_RUNNING|RESEARCH_BLOCKED|RESEARCH_COMPLETE|NEEDS_USER_ACTION",
  "summary": {
    "active_investigations": 0,
    "active_hypotheses": 0,
    "collecting_evidence": 0,
    "findings_ready": 0,
    "blocked": 0,
    "operator_actions_required": 0
  },
  "operator_message": "",
  "next_step": {},
  "active_research": [],
  "findings_ready": [],
  "blockers": [],
  "evidence": {}
}
```

Existing backend systems can supply this endpoint. No new research behavior is required for the screen rebuild.

## Data Mapping

| Visible field | Existing source | Current source location | Transformation required | Missing source | Reliability |
| --- | --- | --- | --- | --- | --- |
| Active investigations | research console / research doctor | Research Lab API/artifacts | map raw states to active count | none known | Medium |
| Active hypotheses | hypothesis registry / research doctor | research console payload | filter active/non-archived | none known | Medium |
| Collecting evidence | research validation samples / qualification | `aegis_research_validation_samples_v1`, qualification | map sample state to operator label | none known | High when artifact present |
| Findings ready | research review brief | `aegis_research_review_brief_v1` | count operator-readable briefs | none known | High when artifact present |
| Blocked research | research doctor / qualification / self-check | research doctor, qualification | classify waiting on data/time/system/operator | may need normalized blocker type | Medium |
| Operator action required | research review brief / doctor | allowed actions and decision state | count only true user decisions | may need explicit user-action flag | Medium |
| Next research step | research doctor / validation samples | doctor and samples artifacts | choose next run/sample/qualification action | scheduler detail may be incomplete | Medium |
| Learned finding summary | research review brief | brief artifact | hide raw artifact jargon, show conclusion/confidence/evidence | none known | High |
| Evidence progress | validation samples | sample artifact | format current/required/missing samples | none known | High |
| Source freshness | surface readiness / artifact metadata | readiness/contract/brief metadata | summarize as trust statement | none known | Medium |

## State Matrix

### NORMAL

Visible message:

```text
Research is active. Aegis is monitoring investigations and collecting evidence.
```

Components shown:

* Research Summary
* Active Research
* Findings Ready if any
* Research Blockers if any
* Next Research Step
* Collapsed Evidence

Components hidden:

* route hub as primary content
* raw artifact headers
* candidate/position/performance workflows

Operator expectation:

Monitor. No action unless explicitly shown.

### NO_RESEARCH

Visible message:

```text
No active research investigations are recorded today.
```

Components shown:

* Research Summary
* empty Active Research
* Next Research Step if known
* Collapsed Evidence

Components hidden:

* empty placeholder groups that do not answer an operator question
* findings/blockers unless present

Operator expectation:

No research action required.

### RESEARCH_RUNNING

Visible message:

```text
Research is running or collecting evidence.
```

Components shown:

* Research Summary
* Active Research
* evidence progress
* next sample/run timing

Components hidden:

* manual review prompts unless a user decision is actually required

Operator expectation:

Aegis is working or waiting for evidence. No user action unless stated.

### RESEARCH_BLOCKED

Visible message:

```text
Research cannot progress because required evidence, source data, or runner support is unavailable.
```

Components shown:

* Research Summary
* Research Blockers
* Next Step
* Collapsed Evidence

Components hidden:

* findings as current truth if briefs are missing/blocking
* candidate or trade actions

Operator expectation:

Use System Health or follow the research blocker next step.

### RESEARCH_COMPLETE

Visible message:

```text
Research findings are ready.
```

Components shown:

* Research Summary
* Findings Ready
* conclusion/confidence/evidence/risk
* allowed research-only actions

Components hidden:

* candidate capture
* position actions
* broker/trading controls

Operator expectation:

Review the finding and choose a research-only action if needed.

### NEEDS_USER_ACTION

Visible message:

```text
Research needs an operator decision.
```

Components shown:

* Research Summary
* Action Required
* relevant finding or blocked hypothesis
* allowed research-only actions

Components hidden:

* system-only blockers from the user-action queue
* candidate, position, or trading actions

Operator expectation:

Take a research decision: review, monitor, dismiss, archive, or request more research.

## Component Plan

### Research Summary

Source: research doctor, research console, review brief, validation samples.

Keep / Improve / Replace / New: NEW

Reason: current screens lack one first answer.

### Active Research

Source: Research Lab hypothesis rows and validation samples.

Keep / Improve / Replace / New: IMPROVE

Reason: current cards contain useful data but need operator-first state, timing, and action clarity.

### Findings Ready

Source: research review brief.

Keep / Improve / Replace / New: IMPROVE

Reason: brief cards are valuable but should appear as findings, not artifact-first content.

### Research Blockers

Source: research doctor, qualification, validation self-check.

Keep / Improve / Replace / New: NEW

Reason: blockers need clear owner and next step.

### Next Research Step

Source: research doctor, validation samples, scheduler state.

Keep / Improve / Replace / New: NEW

Reason: “what happens next” was a repeated audit gap.

### Collapsed Evidence

Source: existing diagnostics/source refs.

Keep / Improve / Replace / New: IMPROVE

Reason: evidence should support trust without dominating the screen.

## API / Data Dependencies

Primary dependencies:

* research console payload
* research doctor artifact
* research review brief artifact
* hypothesis qualification artifact
* research validation samples artifact
* paper-testing sleeve artifact as validation context
* operator surface readiness / operator state for source-day and unavailable-state gating

Reusable UI dependencies:

* shell navigation
* dark theme
* shared status/card/table/details primitives
* existing Research Lab card components after simplification
* existing Research Review card data mapping after simplification

Problematic components to bypass:

* Research Workspace Link Hub as first screen
* artifact headers as primary content
* duplicated recommendation-ready summaries
* generic empty groups
* raw diagnostics before operator summary

## Implementation Order

1. Build `/api/aegis/operator/research` as an operator-ready envelope from existing research artifacts.
2. Add unit tests for state mapping from existing research evidence to operator states.
3. Replace Research landing primary content with Research Summary and Active Research.
4. Integrate Findings Ready from existing review brief data.
5. Add Research Blockers and Next Research Step sections.
6. Move diagnostics/source artifacts into collapsed Evidence.
7. Capture screenshot for 2026-05-30 and compare against spec.
8. Add visible-text UI tests after screenshot acceptance.

## Risks

* Existing research state may be split across multiple payloads with inconsistent labels.
* Some next-run timing may not exist; the UI must say no next step is confirmed rather than guessing.
* Current Research Review content may expose artifact names as primary copy; must be demoted.
* Autonomous validation and manual review language can conflict; operator action must be counted only when real user input is required.
* Research must not imply candidate actionability, trading readiness, or performance conclusions.

## Boundary Violations To Remove During Implementation

| Current behavior | Why it is wrong | Owning screen |
| --- | --- | --- |
| Research Workspace primarily shows navigation links. | Does not answer active research state. | Research should own summary; navigation remains secondary. |
| Research Review artifact ID appears as a primary hero. | Artifact identity is audit detail, not operator answer. | Evidence/Audit detail. |
| Recommendation-ready summaries duplicate Research Lab state. | Multiple places answer same research status differently. | Research Findings section owns it. |
| Generic manual-review language appears when autonomous validation should continue. | Implies user gate where no user action is required. | Research state model must distinguish collecting evidence vs needs user action. |
| Research cards can include raw lifecycle/state labels. | Internal labels do not answer operator questions. | Collapsed evidence/details. |

## Screenshot Acceptance Checklist

A screenshot passes only if a non-engineer can answer:

* What research exists?
* What is currently active?
* What has been learned?
* What is blocked?
* What happens next?
* Do I need to do anything?

Visible proof required:

* first visible content is Research Summary, not a link hub
* active research is plainly labeled
* findings ready are shown only with conclusion/confidence/evidence
* blockers explain reason and next step
* no candidate capture, positions, holdings, exposure, P&L, or performance dashboard appears
* diagnostics/evidence are collapsed by default

## Non-Implementation Note

This plan does not modify UI code, routes, backend logic, research logic, candidate generation, canonical artifacts, or safety gates.
