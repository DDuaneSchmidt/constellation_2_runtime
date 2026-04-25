---
id: C2_OPERATOR_UI_INVARIANTS_V1
title: "C2 Operator UI Invariants Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-12
owner: Constellation
authority: governance+git
scope: constellation_2_operator_ui_invariants
---

# C2 Operator UI Invariants Contract (V1)

## Purpose

This contract locks the operator UI architecture so projection, composition, and
UI-shell responsibilities cannot silently drift.

## Projection Rules

- A projection is the only allowed UI-facing layer that reads governed runtime artifacts directly.
- A projection must not call another builder surface.
- A projection must expose source facts independently and must not collapse distinct upstream states into one synthetic readiness verdict.
- A projection must preserve explicit unknown, stale, degraded, reconstructed, and fail-closed conditions.
- A projection must carry:
  - `surface_kind`
  - `contract_id`
  - `contract_version`
  - `truth_state`
  - `data_condition`
  - `source_authority`
  - `provenance_refs`
  - `degradation_codes`

## Composition Rules

- A composition may call projections.
- A composition may add cross-projection linkage, operator summaries, blocking lists, and workflow-oriented arrangement.
- A composition must not override projection-owned truth.
- A composition must not replace projection-owned source authority.
- A composition must identify itself with its own `contract_id`, `contract_version`, and `surface_kind="composition"`.
- A composition may attach lightweight `_projection_metadata` describing the projection contract it is composed from.

## Semantic Rules

- `truth_state` and `data_condition` are mandatory on every top-level operator endpoint surface.
- Legacy `freshness_state="healthy"` may remain only for backward compatibility.
- Any surface that exposes `freshness_state="healthy"` must also expose `data_condition="fresh"`.
- New operator surfaces must not use `healthy` as a semantic substitute for `data_condition`.
- Advisory context must not be visually or semantically conflated with Operations truth.

## Metadata Requirements

- Every operator endpoint surface must return exactly one surface payload.
- Every important row or item must preserve stable identity where available.
- Every important row or item must preserve truth-state and provenance where available.
- When an upstream artifact omits a required semantic, the UI read layer must emit `UNKNOWN` or `[]` rather than infer or omit silently.
- Validation must be attached under `_metadata_validation`.
- Validation must not throw yet; it must fail visibly via metadata.

## Forbidden Behaviors

- Recomputing canonical truth in the UI layer.
- Hiding missing or degraded inputs behind healthy summaries.
- Returning both projection and composition payloads from one endpoint.
- Letting a projection call another builder.
- Letting a composition rewrite projection `truth_state`.
- Letting a composition rewrite projection `source_authority`.
- Removing existing compatibility fields without an explicit migration contract.

## Endpoint Invariants

The operator shell endpoints audited under this contract are:

- `/api/orders`
- `/api/positions`
- `/api/reconciliation`
- `/api/alerts`
- `/api/operations`

Each must return a single top-level surface that includes:

- `surface_kind`
- `contract_id`
- `contract_version`
- `truth_state`
- `data_condition`
- `source_authority`
- `provenance_refs`

## Validation Posture

- Targeted operator-shell tests must pass before this contract is considered satisfied.
- Repo-wide pytest failures outside the operator UI boundary do not authorize weakening these invariants.
