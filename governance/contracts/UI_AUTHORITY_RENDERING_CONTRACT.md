---
id: C2_UI_AUTHORITY_RENDERING_CONTRACT_V1
title: "C2 UI Authority Rendering Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-13
owner: Constellation
authority: governance+git
scope: constellation_2_ui_authority_rendering
---

# C2 UI Authority Rendering Contract (V1)

## Purpose

This contract fixes the minimum rendering and response contracts for the
kernel-aligned operator shell.

## Workspace Query Contract

Every kernel workspace query must return:

- `workspace_id`
- `workspace_label`
- `kernel_status`
- `authority_artifacts`
- `decision_artifacts`
- `run_envelopes`
- `lineage_chain`
- `derived_summaries`
- `command`
- `supersession`

`kernel_status` must come from backend-owned authoritative summary logic.

## Command Response Contract

Every operator-shell command response must return:

- `command_id`
- `ok`
- `outcome`
- `reason_codes`
- `run_envelope_ref`
- `result_artifact_ref` when one exists
- `related_artifact_refs`

## Rendering Primitive Contract

The shell must implement shared primitives for:

- authority artifact cards
- command panels
- run envelope panels
- lineage chains
- supersession banners
- derived summary cards

These primitives must be reused across all five kernel workspaces.

## Supersession Contract

- A superseded artifact must render an explicit banner.
- Unsafe actions bound to a superseded artifact must be disabled or refused.
- The banner must expose the current authority artifact id when known.

## Derived Summary Contract

- Derived summaries are display-only.
- Derived summaries must carry explicit derived labeling.
- Derived summaries must not be the primary card in a kernel workspace.

## Root Work Queue Contract

The `/` route must render an operator work queue only.

It may summarize only:

- blocked kernels
- missing kernels
- stale kernels
- the next valid action link into a kernel workspace

It must not become a generic metrics dashboard.
