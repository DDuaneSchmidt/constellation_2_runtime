# Aegis Change Control Intelligence Layer V1 Requirements

## Purpose

The Change Control Intelligence Layer helps David decide what Change Control work to address next using frozen evidence, deterministic scoring, and advisory explanation.

It does not own Change Control truth. It explains and prioritizes existing governed records.

## Scope

V1 covers Change Control records, dependencies, blockers, parent/child rollups, validation gaps, audit status, portal smoke status, and runtime truth status.

## Non-Goals

* No automatic approve, reject, defer, prioritize, validate, close, or implement actions.
* No record mutation.
* No Kanban/Jira replacement.
* No trading, broker, live, autonomous, sleeve, candidate-generation, or canonical trading artifact changes.

## Operator Problems Solved

* Which Change Control item should David look at first?
* Why is a parent record blocked?
* Which child or blocker unlocks the most work?
* What decision note or Codex prompt should be drafted next?
* Is the recommendation current enough to trust?

## Governance Rules

The governed Change Control register remains the source of truth. The intelligence layer may only read it and produce derived advisory artifacts.

## AI Authority Boundaries

AI may explain, summarize, recommend, and draft decision notes.

AI must not approve, reject, defer, prioritize, validate, close, implement, assign, mutate, or override evidence rules.

## Deterministic Advisor Requirements

The deterministic advisor must compute stable scores from the evidence snapshot. The same snapshot must produce the same ranking. AI narrative must not change the score.

## Evidence Snapshot Requirements

The snapshot freezes the facts reviewed by the advisor and AI reviewer, including register hash, open P0/P1 items, blocked parents, required children, validation gaps, stale status, and source evidence references.

## Auditability Requirements

Every advisor score and AI review must link back to the exact snapshot ID/hash and source register hash.

## Safety Requirements

The layer must prove safety gates remain disabled and must not interact with trading, broker, live, autonomous, sleeve, candidate generation, or canonical trading artifacts.

## Human Decision Workflow

The output is advisory. David must still use the governed Change Control decision workflow for approve, reject, defer, prioritize, or notes. Closure still requires validation evidence.
