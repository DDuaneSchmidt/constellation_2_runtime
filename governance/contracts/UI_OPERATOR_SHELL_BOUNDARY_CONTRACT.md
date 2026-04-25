---
id: C2_UI_OPERATOR_SHELL_BOUNDARY_CONTRACT_V1
title: "C2 UI Operator Shell Boundary Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-13
owner: Constellation
authority: governance+git
scope: constellation_2_operator_shell
---

# C2 UI Operator Shell Boundary Contract (V1)

## Purpose

This contract fixes the operator UI as one shell over the kernel authority
chains already governed elsewhere in this repo.

The UI is a rendering and command-binding surface only.

## Required Shell Shape

- exactly one operator shell
- exactly five kernel workspaces:
  - `/control`
  - `/state`
  - `/advisory`
  - `/submission`
  - `/lifecycle`
- exactly one root operator work queue at `/`

## Query / Command Separation

- Read endpoints may aggregate authoritative artifacts for display.
- Read endpoints must not mint new authority.
- Command endpoints must be explicit and named.
- Command endpoints must not expose arbitrary artifact editing.
- Command responses must surface the resulting run envelope id and resulting
  authority artifact id when one exists.

## Artifact-Bound Action Rules

- UI mutation requests must bind to explicit artifact ids when the target kernel
  already has an upstream authority artifact.
- The UI must not mutate against floating "latest" state alone.
- Superseded or stale artifact bindings must fail closed.
- Refused commands must return explicit reason codes.

## UI Non-Authority Rules

- The browser must not compute advisory truth.
- The browser must not compute snapshot truth.
- The browser must not compute runtime-control truth.
- The browser must not compute submission truth.
- The browser must not compute lifecycle truth.
- The browser must not recreate business eligibility rules already owned by
  backend kernels.

## Rendering Rules

- Authority artifacts must render as the dominant truth surface.
- Decisions and gates must render distinctly from authority artifacts.
- Run envelopes must render as evidence, not as authority.
- Derived summaries and projections must render with explicit derived labeling.
- A derived card must never be visually confusable with an authority artifact.

## Command Scope

The operator shell may expose only these kernel-aligned command paths:

- evaluate/update runtime control
- run snapshot kernel
- run advisory kernel
- promote advisory decision if allowed
- submit authorized execution if allowed
- refresh lifecycle truth from evidence sources

## Forbidden Expansions

- generic dashboard inference
- browser-side workflow engines
- browser-side policy engines
- projection-led mutation paths
- alternate command paths around kernel runners
- backend redesign of advisory, snapshot, execution submission, execution
  lifecycle, or broker behavior
