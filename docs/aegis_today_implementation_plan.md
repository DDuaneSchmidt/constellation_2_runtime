# Aegis Today / Command Center Implementation Plan

## Scope

This is a planning document only.

It does not authorize UI implementation, route changes, backend logic changes, candidate workflow changes, paper execution changes, or canonical artifact changes. Its purpose is to prove that the rebuilt Today / Command Center screen can be built from existing Aegis backend evidence before writing code.

Authority chain:

```text
docs/aegis_operator_jobs.md
docs/aegis_operator_screen_spec.md
docs/aegis_operator_state_model.md
docs/aegis_today_screen_spec.md
docs/aegis_today_implementation_plan.md
```

Runtime truth observed before writing this plan for `TARGET_DAY=2026-05-30`:

```text
verified_runtime_graph: READY
audit_blocker_count: 0
runtime_truth_classification: PARTIAL_CONTEXT
highest_readiness_layer: BLOCKED
trade_advice_allowed: false
broker_execution_allowed: false
broker_submit_transmit_allowed: false
live_trading_allowed: false
autonomous_live_trading_allowed: false
```

## Implementation Goal

Build a new Today / Command Center screen that answers:

1. Is Aegis okay today?
2. What happened today?
3. What is currently open?
4. What is waiting?
5. What is blocked?
6. Do I need to do anything?
7. What happens next?
8. When is the next scheduled run?
9. Can Aegis act, or is it monitoring only?
10. Why should I trust today's state?

The screen should be driven by a simplified operator-ready API:

```text
/api/aegis/operator/today
```

The endpoint may consume existing governance artifacts, but the visible UI must not expose backend governance vocabulary as primary content.

## Current Components Reused

| Component | Source | Decision | Reason |
|---|---|---:|---|
| Route shell and navigation frame | `constellation_2/phaseL/ui/static/operator_shell` | Improve | The shell already provides routing, navigation, day parameters, and shared page mounting. It should be reused after visual route behavior is verified by screenshot. |
| `/api/aegis/operator/today` route name | `run_ops_dashboard_v1.py` | Improve | The endpoint already exists and matches the desired boundary name, but its payload is currently a cockpit projection rather than the rebuilt operator-ready envelope. |
| `fetchAegisOperatorToday` client function | `domain_client/index.js` | Improve | The client function already points at the preferred endpoint. It can remain if the payload contract changes behind it. |
| Metric card primitive | Current Command Center and Performance UI | Improve | Compact status cards are useful if they answer operator questions and do not lead with contract or invariant language. |
| Attention queue concept | Command Center queue audit and Command Center UI | Improve | The concept has operator value, but it must show only true user actions and must not include already-captured, monitor-only, stale, or diagnostics-only rows. |
| Open positions summary concept | Positions and Performance sources | Keep | The Today page needs only high-level open position count and quality, not detailed position review. |
| Runtime timeline / run health concept | Runtime Timeline and engineering artifacts | Improve | Useful for "what happened" and "what happens next," but must be translated from engineering state into plain language. |
| Research progress summary concept | Research Lab / validation artifacts | Improve | Useful as a high-level "what is being validated" summary. Detailed hypothesis cards belong on Research. |
| Safety mode statement | Audit handoff and control packet | Keep | The Today screen must clearly say Aegis is monitoring-only when safety gates prohibit action. |
| Trust/evidence link | Verified runtime graph, runtime truth, audit handoff | Improve | Evidence should be available after the plain-English state is clear. It must not dominate the first view. |
| Ask Aegis entry point | AI Operations Assistant | Improve | Useful as a supporting action after core status is clear. It must not become the primary workflow. |

## Current Components Bypassed

| Component | Source | Decision | Reason |
|---|---|---:|---|
| Existing `renderCommandCenterWorkspace` full page body | `pages/index.js` | Replace | It assembles many projections directly in the browser and has repeatedly allowed architecture/state content to dominate operator workflow. |
| Legacy Operator Cockpit primary body | Current cockpit rendering paths | Replace | Prior audits observed stale or wrong-session rows such as historical paper sessions appearing in current-day context. |
| Surface Contract primary banner | Contract-driven refresh code | Replace | Backend contract status may inform the endpoint, but raw contract state must not be the visible Today product. |
| Semantic invariant / readiness panels as primary content | Surface readiness / invariant UI | Replace | These are backend safety gates, not operator workflow sections. |
| Candidate workflow detail rows | Candidate pipeline pages and current command body | Bypass | Today needs a high-level candidate summary only. Detailed readiness, construction, and rejected-lineage belong in Candidate Pipeline or diagnostics. |
| Position Review details | Position Review screen | Bypass | Today must not show thesis, brief, or review content. It may link to Position Review if open positions exist. |
| Full open positions table | Positions screen | Bypass | Today needs summary only. Detailed tables belong on Positions. |
| Research card grid | Research / Research Lab screens | Bypass | Today needs count/state summary only. Detailed hypothesis cards belong on Research. |
| Engineering diagnostics and raw artifact paths | Engineering and Evidence screens | Bypass | Today should show "blocked because..." and link to System Health/Evidence, not expose raw paths. |
| Empty placeholder groups | Multiple legacy pages | Remove | Empty groups lower trust. Empty state must be a single explicit operator message. |
| Raw npm command instructions as primary content | Legacy diagnostics states | Remove | Commands may appear in Evidence/System Health, not as Today's operator-facing primary message. |

## New Components Required

| Component | Source | Decision | Reason |
|---|---|---:|---|
| Today status statement | New Today renderer | New | First visible message must answer whether Aegis is okay today. |
| David action statement | New Today renderer | New | Must say "No David action required" or list exact user actions. |
| Safety mode statement | Control packet / audit handoff | New | Must state whether Aegis can act or is monitoring-only. |
| Today activity digest | Run history / runtime timeline / canonical state | New | Must summarize what ran, what did not run, and any misses. |
| Open positions summary | Paper position ledger / positions read model | New | Must show count and data-quality status without detailed position review. |
| Candidate summary | Candidate diagnostics / queue audit / canonical operator state | New | Must show whether candidates exist and whether any are actionable. |
| Research summary | Research doctor / validation samples / qualification | New | Must summarize autonomous research/validation state. |
| Waiting / next scheduled activity | Runtime timeline / scheduler / paper session ledger | New | Must answer what happens next and when. |
| System health one-liner | Runtime truth / engineering priority queue | New | Must translate blockers/degraded state into operator language. |
| Trust statement | Verified runtime graph / runtime truth / evidence refs | New | Must explain why today's summary is trustworthy or why it is blocked. |
| Known limitations summary | Runtime truth / surface readiness / semantic invariants | New | Must state what is unavailable without cluttering the main screen. |

## API Contract For `/api/aegis/operator/today`

The rebuilt endpoint should return one operator-ready envelope. It should not return raw cockpit projections as the top-level UI contract.

```json
{
  "ok": true,
  "requested_day": "2026-05-30",
  "source_day": "2026-05-30",
  "generated_at": "2026-05-30T16:55:34Z",
  "state": "BLOCKED",
  "headline": "Aegis is monitoring only today.",
  "summary": "Current-day evidence is available, but runtime readiness blocks trade advice and paper trade creation.",
  "user_action_required": false,
  "actions": [],
  "safety": {
    "mode_label": "Monitoring only",
    "trade_advice_allowed": false,
    "broker_execution_allowed": false,
    "broker_submit_transmit_allowed": false,
    "live_trading_allowed": false,
    "autonomous_live_trading_allowed": false
  },
  "activity": {
    "did_anything_run": true,
    "last_run_at": null,
    "last_successful_run_at": null,
    "events": []
  },
  "open_positions": {
    "count": 0,
    "summary": "No open paper positions are recorded for the requested day.",
    "data_quality": "UNKNOWN"
  },
  "candidates": {
    "status": "NO_ACTIONABLE_CANDIDATES",
    "output_count": 0,
    "actionable_count": 0,
    "blocked_reason": null
  },
  "research": {
    "state": "RESEARCH_UNAVAILABLE",
    "active_count": 0,
    "collecting_evidence_count": 0,
    "next_observation": null
  },
  "system_health": {
    "state": "BLOCKED",
    "top_blocker": "Runtime truth is PARTIAL_CONTEXT.",
    "degraded_count": 0
  },
  "next": {
    "label": "Next scheduled Aegis activity",
    "scheduled_at": null,
    "expected_output": null,
    "confidence": "LOW"
  },
  "trust": {
    "label": "Verified graph is ready; runtime truth is blocked.",
    "evidence_current": true,
    "limitations": [
      "Runtime truth classification is PARTIAL_CONTEXT."
    ],
    "evidence_refs": []
  }
}
```

Allowed `state` values must come from `docs/aegis_operator_state_model.md`:

```text
NORMAL
NO_ACTIVITY
WAITING_FOR_NEXT_RUN
BLOCKED
DEGRADED
NEEDS_USER_ACTION
MANUAL_IB_CAPTURE_READY
RESEARCH_RUNNING
RESEARCH_UNAVAILABLE
PERFORMANCE_UNAVAILABLE
```

### Contract Rules

1. `requested_day`, `source_day`, and any source-specific day must be present.
2. If `requested_day != source_day`, `state` must not be `NORMAL`.
3. If current-day evidence is missing or wrong-day, `actions` must be empty.
4. `user_action_required=true` is allowed only when at least one `actions[]` row has `action_type=USER_ACTION`.
5. Candidate action controls are not part of the Today endpoint. Today may link to Candidates only when candidate actionability is proven.
6. Trade advice, broker execution, live trading, autonomous live trading, and submit/transmit flags must remain false unless future governance explicitly changes them.
7. The response should contain operator language first and evidence references second.

## Data Mapping From Existing Backend Systems

| Field | Existing source | Transformation required | Missing source |
|---|---|---|---|
| `requested_day` | Request query `day`, `TARGET_DAY` fallback | Normalize to `YYYY-MM-DD`. | None. |
| `source_day` | `resolve_current_operator_truth_v1`, canonical operator state day | Preserve explicit mismatch; do not silently relabel historical data as current. | None, but current endpoint can fall back and must be hardened during implementation. |
| `generated_at` | Endpoint build time and source artifact timestamps | Use endpoint time plus evidence timestamps in `trust.evidence_refs`. | None. |
| `state` | Runtime truth, surface readiness, semantic invariants, queue audit, candidate/position/research status | Apply deterministic priority order from state matrix. | None if upstream artifacts exist; blocked state if not. |
| `headline` | Derived from `state`, safety gates, top blocker, action count | Translate to one plain-English sentence. | Copy rules need implementation. |
| `summary` | Derived from runtime truth, activity, candidates, positions, research | Translate into non-engineering summary. | Copy rules need implementation. |
| `user_action_required` | Command Center queue audit / operator action queue | True only for `USER_ACTION` rows. | None. |
| `actions[]` | `aegis_command_center_queue_audit_v1`, engineering priority queue action rows | Filter to true David actions; map system repair and verify-only rows out of user-action list. | None, but action-type semantics must be enforced. |
| `safety.trade_advice_allowed` | Audit handoff / control packet / runtime truth | Direct boolean. | None. |
| `safety.broker_execution_allowed` | Audit handoff / control packet / safety policy | Direct boolean; if absent, fail closed false. | None. |
| `safety.broker_submit_transmit_allowed` | Audit handoff / control packet / safety policy | Direct boolean; if absent, fail closed false. | None. |
| `safety.live_trading_allowed` | Audit handoff / control packet / safety policy | Direct boolean; if absent, fail closed false. | None. |
| `safety.autonomous_live_trading_allowed` | Audit handoff / control packet / safety policy | Direct boolean; if absent, fail closed false. | None. |
| `activity.did_anything_run` | Run history, runtime timeline, paper session ledger, audit handoff | True if any current-day scheduled or producer run is recorded. | Unified run-history summary may be incomplete; endpoint should degrade if unavailable. |
| `activity.last_run_at` | Runtime timeline / run history | Pick latest current-day run timestamp. | May require aggregation from multiple artifacts. |
| `activity.last_successful_run_at` | Runtime timeline / run history / verified graph generation | Pick latest successful current-day runtime build or scheduled run. | May require aggregation from multiple artifacts. |
| `activity.events[]` | Runtime timeline, candidate diagnostics, research status, performance generation | Reduce to 3-5 operator-relevant events. | None if run history exists; otherwise degrade. |
| `open_positions.count` | Paper position ledger / positions read model | Count open positions for requested day only. | None if ledger current; blocked if unavailable. |
| `open_positions.summary` | Paper ledger, marks, P&L report, sleeve attribution | Plain-language summary; no detailed rows. | None. |
| `open_positions.data_quality` | Mark coverage, sleeve attribution coverage, P&L status | Translate to `GOOD`, `PARTIAL`, `BLOCKED`, or `UNKNOWN`. | None. |
| `candidates.status` | Candidate state, candidate diagnostics, queue audit, paper review queue, duplicate policy | Summarize output candidate/actionable state only. | None if current-day candidate state exists; blocked if missing. |
| `candidates.output_count` | Candidate diagnostics / canonical operator state | Count current-day output-intent candidates only. | None. |
| `candidates.actionable_count` | Queue audit / candidate readiness / duplicate classification | Count only operator-actionable current-day rows. | None. |
| `candidates.blocked_reason` | Candidate state / queue audit / surface readiness | Plain-language reason if candidate summary is blocked/unavailable. | None. |
| `research.state` | Research doctor, hypothesis validation, research validation samples | Map to state model, e.g. `RESEARCH_RUNNING`, `RESEARCH_UNAVAILABLE`, `WAITING_FOR_NEXT_RUN`. | None if research doctor artifacts exist. |
| `research.active_count` | Research doctor / hypothesis registry | Count actively researching/validating hypotheses. | None. |
| `research.collecting_evidence_count` | Research validation samples / qualification | Count hypotheses collecting evidence. | None. |
| `research.next_observation` | Research validation samples | Show next expected sample in plain language. | May be null outside market/session windows. |
| `system_health.state` | Runtime truth / engineering priority queue / verified graph | Map to `NORMAL`, `DEGRADED`, or `BLOCKED`. | None. |
| `system_health.top_blocker` | Engineering priority queue / runtime truth | Select highest-priority current-day blocker and translate. | None if queue exists; otherwise use runtime truth reason. |
| `system_health.degraded_count` | Engineering priority queue | Count `DEGRADED` issues. | None. |
| `next.label` | Runtime timeline / scheduler / paper session ledger | Plain-language next activity label. | Unified source may be weak; degrade with "next run not confirmed." |
| `next.scheduled_at` | Scheduler / paper session ledger / runtime timeline | Select next current-day or next market-session timestamp. | May be missing; state should say unknown, not invent. |
| `next.expected_output` | Scheduler/run definitions | Translate expected output. | May require static mapping from known scheduled jobs. |
| `next.confidence` | Source freshness and scheduler certainty | `HIGH`, `MEDIUM`, `LOW`. | None. |
| `trust.label` | Verified graph, runtime truth, audit handoff | Translate graph and runtime state into one sentence. | None. |
| `trust.evidence_current` | Artifact days/hashes/freshness | True only when source days match requested day. | None. |
| `trust.limitations[]` | Runtime truth, surface readiness, semantic invariants | Plain-language limitations. | None. |
| `trust.evidence_refs[]` | Verified graph, audit handoff, runtime truth, source artifacts | Include only secondary evidence references. | None. |

## State Handling Matrix

| State | Trigger | Today behavior | Actions |
|---|---|---|---|
| `BLOCKED` | Current-day summary cannot be trusted; requested/source day mismatch; required current-day sources missing; semantic contradiction; runtime truth blocks critical read model | Show one blocked message, reason, impact, next step, and evidence link. Do not show workflow action buttons. | Empty. |
| `DEGRADED` | Summary is usable but a non-critical source is missing, stale, or partial | Show usable facts first and explicitly list limitations. | Only true user actions if action source is current and safe. |
| `NEEDS_USER_ACTION` | One or more true David actions exist in current-day queue audit | Show attention queue before summaries. | Only `USER_ACTION` rows; no stale or system-only actions. |
| `MANUAL_IB_CAPTURE_READY` | Current-day output candidate is ready for manual IB capture and capture is allowed by governance | Show exact candidate count and link to candidate workflow. | Manual capture actions only on the candidate workflow, not Today. |
| `WAITING_FOR_NEXT_RUN` | No user action; system is waiting for scheduled time/data | Show what is waiting and next expected time. | Empty. |
| `NO_ACTIVITY` | Current-day sources are valid but nothing has run or qualified yet | Show no-activity message and next scheduled activity. | Empty. |
| `NORMAL` | Current-day sources are valid, no blockers, no user action required, monitoring is healthy | Show concise status, open positions/candidates/research summary, next activity, trust statement. | Empty unless a true user action appears. |
| `RESEARCH_RUNNING` | Research is actively running or collecting evidence while Today is otherwise readable | Show research summary and next evidence expectation. | Empty unless research has true user action. |
| `RESEARCH_UNAVAILABLE` | Research artifacts are unavailable but Today can still summarize other areas | Show research unavailable in the research summary, not as a full-page failure unless research is the requested focus. | Empty. |
| `PERFORMANCE_UNAVAILABLE` | Performance/P&L artifacts are unavailable but core Today state is readable | Show performance unavailable in summary. Do not show metric cards. | Empty. |

State priority:

```text
BLOCKED
NEEDS_USER_ACTION
MANUAL_IB_CAPTURE_READY
DEGRADED
RESEARCH_RUNNING
WAITING_FOR_NEXT_RUN
NO_ACTIVITY
NORMAL
```

`RESEARCH_UNAVAILABLE` and `PERFORMANCE_UNAVAILABLE` are section-level states unless they invalidate the entire Today summary.

## Screenshot Acceptance Checklist

Before any implementation is accepted, a browser screenshot for Today / Command Center must prove:

1. The first visible message answers whether Aegis is okay today.
2. The first viewport shows whether David action is required.
3. The first viewport shows whether Aegis can act or is monitoring-only.
4. The first viewport shows open positions count or a clear unavailable state.
5. The first viewport shows candidate action summary or a clear unavailable state.
6. The first viewport shows what is waiting or what happens next.
7. The screen shows the next scheduled activity if known, or says it is not confirmed.
8. The screen shows why today's state is trustworthy or why it is blocked.
9. If no user action exists, no action queue is rendered.
10. If actions are blocked, no capture/review/action buttons are visible.
11. No stale prior-day paper session appears in primary content.
12. No raw contract JSON, semantic invariant IDs, or surface readiness vocabulary appears in primary content.
13. No Position Review details appear on Today.
14. No detailed candidate workflow rows appear on Today.
15. No raw npm command appears as the primary operator message.
16. Ask Aegis is visible only as a supporting action after status is clear.
17. Evidence or diagnostics are collapsed and secondary.
18. A non-engineer can answer: "Is Aegis okay today, do I need to do anything, and what happens next?"

## Risks

1. The existing `/api/aegis/operator/today` endpoint currently returns an operator cockpit projection, not the proposed operator-ready envelope.
2. Existing UI renderers may concatenate new template content with old legacy bodies if not bypassed deliberately.
3. The next scheduled activity source may be fragmented across runtime timeline, scheduler, and paper session artifacts.
4. User-action classification can regress if system repair, diagnostics, or verify-only rows are counted as David actions.
5. Current-day versus source-day fallback behavior must be explicit; silent fallback would recreate the prior trust failure.
6. Runtime truth can be `PARTIAL_CONTEXT` while graph status is `READY`; copy must distinguish graph validity from runtime capability.
7. Performance or position data can be useful but non-canonical; Today must summarize limitations without showing contradictory metrics.
8. Ask Aegis can become too prominent and obscure core status.
9. Screenshots may reveal legacy text that tests miss; visible browser output must remain authoritative.

## Dependencies

Required existing backend sources:

* `aegis_runtime_truth_kernel_v1`
* `aegis_verified_runtime_graph_v1`
* `aegis_audit_handoff_v1`
* `aegis_chatgpt_control_packet_v1`
* `aegis_canonical_operator_state_v1`
* `aegis_command_center_queue_audit_v1`
* `aegis_paper_session_ledger_v1`
* paper position ledger/read model
* candidate state and candidate generation diagnostics
* research doctor / hypothesis validation / validation samples
* engineering priority queue
* surface readiness and semantic invariants as backend safety inputs

Required frontend sources:

* operator shell route frame
* day-aware domain client
* compact card/table primitives after simplification
* evidence/diagnostics disclosure primitive
* Ask Aegis trigger as secondary action

## Estimated Implementation Order

1. Freeze this plan and screenshot acceptance checklist.
2. Add golden fixtures for the `/api/aegis/operator/today` envelope using known days:
   * `2026-05-29`: canonical trading day.
   * `2026-05-30`: current fail-closed/monitoring day.
3. Wrap or replace the existing `/api/aegis/operator/today` payload so it returns the proposed operator-ready envelope.
4. Add endpoint tests for day/source consistency, safety gates, action filtering, and state priority.
5. Build a new isolated Today renderer that consumes only the operator-ready envelope.
6. Route Today / Command Center to the new renderer while keeping legacy Command Center rendering unreachable for current-day operator pages.
7. Capture screenshot for `2026-05-30` before adding broad tests.
8. Iterate copy and hierarchy only until the screenshot answers the required operator questions.
9. Add visible-text browser tests based on accepted screenshots.
10. Add regression tests forbidding stale prior-day sessions, raw contract vocabulary, raw commands, detailed candidate workflow, and Position Review content on Today.
11. Run audit, portal smoke, and safety-gate checks.

## Feasibility Conclusion

The Today / Command Center rebuild is feasible from existing backend systems, provided the implementation introduces one operator-ready aggregation envelope at `/api/aegis/operator/today` and deliberately bypasses legacy page render paths.

The largest implementation gap is not missing evidence. It is translation and prioritization:

```text
existing evidence -> operator-ready state -> screenshot-approved visible content
```

No trading, broker, sleeve, candidate generation, research, or paper execution logic needs to change for the first Today rebuild slice.
