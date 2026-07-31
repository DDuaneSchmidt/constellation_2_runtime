# Aegis System Health Implementation Plan

## Scope

This is an implementation plan only. It does not authorize UI, route, backend, trading, sleeve, candidate, research, canonical artifact, or safety-gate changes.

The rebuilt System Health screen must use existing operational evidence where possible and avoid becoming a raw artifact browser.

## Current State

Current screens/routes:

* Engineering Dashboard
* Runtime Timeline
* Verified Runtime / Runtime Truth routes
* Evidence / Audit Detail
* repair center / diagnostic routes

Current components:

* runtime blocked message
* fix-first queue
* operator action queue
* active blockers
* degraded dependencies
* run health cards
* raw contract panels
* runtime timeline domain rows
* diagnostics/evidence drawers

Current problems:

* System health concepts are scattered across Engineering, Runtime Timeline, Evidence, header status, and audit outputs.
* Raw contract/runtime terminology appears before the operator answer.
* Repair, verification, and diagnosis can blur together.
* System repair rows can be mislabeled as user action.
* Performance/data quality issues can leak into analytics pages instead of pointing to System Health for repair.

Current coupling issues:

* Engineering Dashboard currently consumes priority queue, runtime truth, surface contract, and audit outputs.
* Runtime Timeline provides useful dependency detail but is too dense for the first health view.
* Evidence Detail owns raw artifacts but some raw evidence leaks into operational pages.

Current operator confusion:

* Operators may see graph READY while runtime truth is BLOCKED without a plain explanation.
* `PARTIAL_CONTEXT`, `BLOCKED`, and readiness layer labels are too internal if not translated.
* It can be unclear whether David must act or whether Aegis is waiting on system/data/time.

## Target State

Screen purpose:

```text
Show whether Aegis can operate, what is blocked or degraded, and what must happen next.
```

Operator questions answered:

* Can Aegis operate?
* Is required data available?
* What is degraded?
* What is blocked?
* What dependencies are unhealthy?
* What must be repaired?

Components required:

* Operating Status
* Fix First
* Active Blockers
* Degraded Dependencies
* Data Availability
* Run and Dependency Health
* Collapsed Evidence

Components reused:

* existing shell navigation and route chrome
* engineering priority queue data
* runtime truth kernel summary
* verified graph status
* audit handoff
* domain certification/runtime timeline data summarized
* supervisor/route health if already available

Components bypassed:

* raw contract panel as primary content
* raw runtime truth JSON as primary content
* runtime timeline as first view
* performance analytics content
* candidate/research/position workflow content

Components removed from primary System Health:

* raw artifact paths except collapsed evidence
* raw `Surface Contract` / `Semantic Invariant` labels as headings
* P&L/performance cards
* candidate action queues
* research findings cards

## Proposed Operator API Boundary

Preferred endpoint:

```text
/api/aegis/operator/health
```

Purpose:

Return an operator-ready health summary from existing operational artifacts.

Suggested top-level fields:

```json
{
  "ok": true,
  "requested_day": "2026-05-30",
  "source_day": "2026-05-30",
  "state": "HEALTHY|DEGRADED|BLOCKED|REPAIR_NEEDED",
  "headline": "Aegis is blocked for one or more workflows.",
  "summary": "Graph validation passed, but runtime readiness is blocked because required capability gates remain closed.",
  "can_operate": false,
  "monitoring_only": true,
  "safety": {
    "trade_advice_allowed": false,
    "broker_execution_allowed": false,
    "broker_submit_transmit_allowed": false,
    "live_trading_allowed": false,
    "autonomous_live_trading_allowed": false
  },
  "fix_first": [],
  "blockers": [],
  "degraded_dependencies": [],
  "data_availability": [],
  "run_health": [],
  "user_actions_required": [],
  "system_repairs": [],
  "verify_only": [],
  "evidence_available": true,
  "evidence_refs": []
}
```

## Data Mapping

| Visible field | Existing source | Current source location | Transformation required | Missing source | Reliability |
| --- | --- | --- | --- | --- | --- |
| Overall health | runtime truth + verified graph | runtime truth kernel, verified graph | translate graph/runtime distinction | none known | High |
| Can operate | mode readiness / runtime truth | mode readiness, audit handoff | map capability gates to plain state | none known | High |
| Safety mode | control packet / audit | audit handoff/control packet | show disabled policies plainly | none known | High |
| Top issue | engineering priority queue | `aegis_engineering_priority_queue_v1` | problem/cause/impact/repair/verify | none known | High if queue current |
| Active blockers | runtime blocker states / priority queue | runtime truth kernel, priority queue | include only true blockers | none known | High |
| Degraded dependencies | domain certification / priority queue | runtime timeline/domain cert | summarize delayed/degraded domains | may need normalized endpoint | Medium |
| Data availability | runtime source manifest / surface readiness | runtime truth, surface readiness | translate missing/stale data | none known | High |
| Run health | supervisor / runtime timeline | runtime manifest, runtime timeline | summarize last/next/success/failure | scheduler detail may be incomplete | Medium |
| Repair status | engineering priority queue / repair semantics | priority queue, repair report | distinguish repair/verify/recovery | none known | High |
| Recovery plan | runtime truth recovery plan | recovery plan text | name/link plan, not raw dump | none known | High |
| Evidence | audit handoff / verified graph | audit outputs | collapsed references | none known | High |

## State Matrix

### HEALTHY

Visible message:

```text
Aegis is operational for the requested day.
```

Components shown: Operating Status, Data Availability, Run and Dependency Health, Collapsed Evidence.

Components hidden: Fix First if empty, Active Blockers if empty, raw diagnostics.

Operator expectation: No repair needed.

### DEGRADED

Visible message:

```text
Aegis is operating with degraded dependencies.
```

Components shown: Operating Status, Degraded Dependencies, Data Availability, Run and Dependency Health, Collapsed Evidence.

Components hidden: repair command buttons unless a real repair exists, performance/candidate/research workflows.

Operator expectation: Monitor the stated limitations.

### BLOCKED

Visible message:

```text
Aegis is blocked for one or more workflows.
```

Components shown: Operating Status, Fix First, Active Blockers, Data Availability, Run and Dependency Health, Collapsed Evidence.

Components hidden: analytics dashboards, candidate actionability, research findings, raw contract primary content.

Operator expectation: Read top issue, then repair or verify according to the row.

### REPAIR_NEEDED

Visible message:

```text
A repair or recovery plan is required.
```

Components shown: Fix First, repair status, real repair command if available, recovery plan if no direct repair exists, verification command.

Components hidden: verify-only command as repair, system-side rows counted as David action.

Operator expectation: Follow the named recovery path or inspect Evidence Detail.

## Screenshot Acceptance

A non-engineer must be able to answer:

* Can Aegis operate?
* What is broken?
* What is degraded?
* What should happen next?

Visible proof required:

* one plain operating status headline
* graph/runtime contradiction explained when present
* top blocker or all-clear visible
* repair and verification separated
* data availability summarized
* raw evidence collapsed

## Dependencies

API dependencies:

* `/api/aegis/operator/health` preferred
* existing engineering priority queue API/source
* runtime truth kernel
* verified runtime graph
* audit handoff
* domain certification/runtime timeline
* supervisor/route health where available

Reusable components:

* shell navigation
* dark theme
* summary cards
* compact issue rows
* collapsed evidence details

Problematic components:

* raw contract panels
* raw runtime JSON views
* long runtime timeline as primary page
* performance analytics cards in health context

Risks:

* Some recovery plans may be text-only and need summarization.
* Existing Engineering tests may assert old labels.
* Runtime graph READY plus runtime truth BLOCKED must be explained consistently.
* Repair command semantics must not regress into verify-only commands labeled as repairs.

## Implementation Order

1. Define operator-ready health envelope from existing runtime, audit, and priority queue evidence.
2. Add state mapping tests for HEALTHY, DEGRADED, BLOCKED, REPAIR_NEEDED.
3. Replace Engineering/System Health primary content with Operating Status and Fix First.
4. Add Active Blockers and Degraded Dependencies sections.
5. Add Data Availability and Run/Dependency Health summaries.
6. Move raw runtime/contract details into collapsed Evidence.
7. Capture screenshot and review against spec before expanding tests.
8. Add visible-text tests after screenshot acceptance.

## Boundary Violations To Remove During Implementation

| Current behavior | Why it is wrong | Owning screen |
| --- | --- | --- |
| Raw contract panel appears on Engineering/System Health primary view. | Engineering metadata before operator answer. | Evidence / Audit Detail. |
| Runtime timeline rows appear as dense first-view content. | Too detailed for the primary health question. | System Health summary first; detail collapsed or Evidence. |
| Performance analytics health appears as raw invariant status. | Performance page owns analytics impact; System Health owns repair cause. | Performance/System Health boundary. |
| Verify-only commands appear like repair commands. | Misleads operator about action. | System Health must separate diagnosis, repair, verification. |
| System repair rows counted as David action. | Inflates user action count. | System Health action typing. |

## Non-Implementation Note

This plan does not modify UI code, routes, backend logic, trading logic, sleeve logic, research logic, candidate generation, canonical artifacts, or safety gates.
