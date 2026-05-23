# Aegis Research Pipeline Operator UX v1

## 1. UX Audit

Current research UX exposes implementation seams instead of operator work. The primary navigation names systems such as Edge Lab, Hypothesis Queue, Research Plans, Evidence, Paper Trials, Sleeve Reviews, and Blocked Work. Those are useful implementation buckets, but they force the operator to infer lifecycle state from backend structure.

Problems found:
- Research work is split across multiple apparent apps.
- Backend terms compete with the actual operator question: what needs attention now?
- Creation asks for event family, priority, and notes before the operator has expressed the idea.
- Pipeline columns are engine gates rather than lifecycle language.
- Diagnostics and fallback commands are visible too early.

## 2. Information Architecture

Primary navigation becomes:

1. Dashboard
2. Research Pipeline
3. Captured Trades
4. Evidence
5. System Health

Edge Lab remains an internal engine and evidence producer. Hypothesis Queue, Research Plans, Paper Trials, Sleeve Reviews, and Blocked Work become filtered views or advanced diagnostics inside Research Pipeline.

## 3. Navigation Redesign

Research Pipeline is the one operator workspace for research lifecycle management. The route can continue to use existing `/research-lab` infrastructure during migration, but the displayed model is the lifecycle pipeline. Legacy deep links remain available for diagnostics and backward compatibility.

## 4. Research Pipeline Wireframe

```text
Research Pipeline
Ideas, validation, paper trials, completed research, and blocked work in one operator workflow.

What needs your attention
[ Oil Shock Reversal ] Waiting for operator review        [ Review now ]
[ Gap Fill Study     ] Waiting on market data             [ View blocker ]

New Research Idea
What are you investigating?
[ text area ]
Symbols optional
[ SPY QQQ XLE ]
[ Advanced research settings ]
[ Start Research ]

Pipeline
| Ideas | Researching | Validating | Paper Trial | Ready | Captured | Blocked | Archived |
| card  | card        | card       | card        | card  | card     | card    | card     |

Advanced diagnostics
[collapsed: evidence ids, internal gates, CLI fallback, lineage/debugging]
```

## 5. Hypothesis Card

Each card shows operator-facing fields only by default:

- title
- symbols
- strategy/sleeve if available
- confidence
- status
- last update
- next required action
- warnings
- owner
- evidence count
- paper trial status

Internal fields such as engine gates, raw evidence chains, commands, drift, fragility, challenger comparisons, and lineage stay in Advanced diagnostics.

## 6. Attention Dashboard

The top section is always `What needs your attention`. It includes only items requiring operator action:

- blocked hypotheses
- stale evidence
- failed trials
- manual review items
- capture-ready opportunities
- approvals needed
- items waiting on operator decision

Each row must include an exact reason, next action, and one-click navigation.

## 7. Lifecycle State Machine

Formal operator states:

```text
IDEA -> RESEARCHING -> VALIDATING -> PAPER_TRIAL -> READY -> CAPTURE_READY -> CAPTURED
                 \                                           \
                  -> BLOCKED ------------------------------------> ARCHIVED
```

Allowed states:
- IDEA
- RESEARCHING
- VALIDATING
- PAPER_TRIAL
- READY
- CAPTURE_READY
- CAPTURED
- BLOCKED
- ARCHIVED

State rules:
- One visible lifecycle state per hypothesis.
- Blocked and review-required states feed the attention engine.
- Lifecycle state never authorizes broker submit, autonomous execution, sleeve mutation, or trade advice.
- Technical gate state remains available only in Advanced diagnostics.

## 8. Hypothesis Detail Page

```text
Hypothesis title
Status | Confidence | Next action

Summary
Latest findings
Current paper trial
Candidate activity
Capture history
Charts / metrics

Right sidebar
Warnings
Operator tasks
Evidence freshness
Last runtime update

Bottom
Advanced diagnostics [collapsed]
Technical evidence [collapsed]
Lineage/debugging [collapsed]
```

## 9. Migration Strategy

Phase 1: Build Research Pipeline shell and rename primary navigation.
Phase 2: Map existing Edge Lab/research artifacts into operator lifecycle states.
Phase 3: Move subsystem pages into filtered pipeline views and advanced diagnostics.
Phase 4: Add hypothesis detail page with task/sidebar/evidence sections.
Phase 5: Retire subsystem-first language once telemetry confirms operators no longer use the old entry points.

## 10. Technical Risk Assessment

Low risk:
- Navigation label changes.
- Collapsing advanced diagnostics.
- Mapping existing gates into operator lifecycle labels.

Medium risk:
- Reclassifying legacy research records into lifecycle states can expose ambiguous states. Mitigation: keep raw gate in diagnostics.
- Attention ranking may over-surface noisy blocked records. Mitigation: sort by operator-required action and stale/failed severity.

Forbidden changes:
- No broker execution controls.
- No autonomous execution.
- No sleeve mutation from research UI.
- No trade advice promotion from research lifecycle state.
