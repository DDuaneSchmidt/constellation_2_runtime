# Aegis Surface Readiness Requirements

## Purpose

Define a central Surface Readiness Gate that controls whether each operator surface may render and whether it may expose action buttons. The goal is to prevent stale, wrong-day, non-canonical, unavailable, or blocked evidence from appearing as current operator truth.

## Core Architecture

```text
Canonical Artifacts
    -> Surface Readiness Gate
    -> Surface Read Model
    -> UI
```

The UI must consume `aegis_surface_readiness_v1` before rendering operator actions. No operator-facing page may decide locally whether stale or wrong-day actions are allowed.

## Required Artifact

```text
truth/reports/aegis_surface_readiness_v1/<day>/surface_readiness.v1.json
```

## Required Surfaces

* `command_center`
* `engineering`
* `positions`
* `performance`
* `position_review`
* `sleeve_analytics`
* `research`
* `ask_aegis`

## Surface Row Contract

Each surface row must include:

* `surface_id`
* `requested_day`
* `source_day`
* `artifact_days`
* `context_day`
* `render_allowed`
* `actions_allowed`
* `surface_status`
* `blocking_reasons`
* `warning_reasons`
* `source_artifacts`
* `source_artifact_hashes`
* `generated_at`

Allowed `surface_status` values:

* `READY`
* `DEGRADED`
* `BLOCKED`
* `UNAVAILABLE`
* `HISTORICAL`
* `INCONSISTENT`

## Rules

1. If `requested_day != source_day`, the surface must be `HISTORICAL` or `INCONSISTENT`, and the UI must show a day mismatch warning.
2. If required artifacts are missing, `render_allowed` may be true only for a blocked/unavailable state, and `actions_allowed` must be false.
3. If artifacts are stale or wrong-day, `actions_allowed=false`, and no capture/review/action buttons may render.
4. If Ask Aegis `context_day != requested_day`, Ask Aegis must refuse or degrade; no stale answer may be presented as current.
5. If `render_allowed=false`, the UI must show a canonical unavailable state.
6. If `actions_allowed=false`, all operator action buttons must be hidden or disabled.

## Surface Semantics

### Command Center

Command Center may render a blocked or unavailable state when candidate artifacts are missing or stale, but candidate actions may appear only when the readiness row has `actions_allowed=true` and the candidate row itself proves requested-day/source-day/paper-session-day agreement.

### Ask Aegis

Ask Aegis must include `requested_day`, `source_day`, and `context_day` in its response contract. If context day differs from requested day, the response must degrade or refuse.

### Position Review

Position Review must not show prior-day briefs for a requested current day. Missing current-day context, score, or brief artifacts must produce a canonical unavailable state.

### Sleeve Analytics

If Sleeve Analytics is `NOT_CANONICAL`, `data_quality_status` must not be `PASS`. Surface readiness must expose the missing, stale, or non-canonical reason.

### Engineering

Engineering must use surface readiness to summarize blocked/degraded surfaces. It may render when other surfaces are blocked because it is the repair/troubleshooting surface.

## Governance Rule

No operator-facing surface may expose action buttons unless its row in `aegis_surface_readiness_v1` allows actions. Consumers query verified truth through the Surface Readiness Gate rather than inventing truth locally.

## Safety Invariants

This gate does not enable trade advice, broker execution, broker submit/transmit, live trading, or autonomous live trading.
