---
id: C2_EXECUTION_DEPENDENCY_MANIFEST_FRESH_PAPER_ENTRY_V1
title: "C2 Execution Dependency Manifest Fresh Paper Entry V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_fresh_paper_entry_dependencies
---

# Purpose

This contract governs the dependency manifest for operation type `fresh_paper_entry_v1`.

The manifest is runtime-readable and registered at:

- `governance/02_REGISTRIES/C2_EXECUTION_BUILD_MANIFESTS_V1.json`

# Canonical stages

- `CANDIDATE_IDENTITY`
- `CONTROL_BASELINE`
- `PORTFOLIO_BASELINE`
- `CAPITAL_AUTHORITY`
- `ENTRY_PERMISSION`
- `BROKER_READINESS`
- `SEAL`

# Required dependency families

Candidate identity:

- attempt state
- submit preflight decision
- binding record
- execution identity record
- selected order plan

Control baseline:

- canonical authority head
- authorization gate verdict

Portfolio baseline:

- positions snapshot
- accounting nav
- allocation summary
- correlation envelope gate

Capital authority:

- exposure net
- capital risk envelope
- capital authority allocation
- engine activity authorization

Entry permission:

- global kill switch state
- economic health gate verdict is advisory only

Broker readiness:

- IB API handshake
- trade submit readiness

# Mixed-root rule

The manifest may reference both canonical truth and sleeve execution truth when governance proves different owners for different dependencies. Execution Build Authority must not flatten those owners into one root heuristic.
