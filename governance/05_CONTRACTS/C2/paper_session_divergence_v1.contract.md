---
id: C2_PAPER_SESSION_DIVERGENCE_V1
title: "C2 Paper Session Divergence Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_admission
---

# C2 Paper Session Divergence Contract v1

## Purpose

This contract defines the governed divergence artifact emitted when execution attempts to use undeclared dependencies or drifts outside the admitted envelope.

## Required meaning

The divergence artifact must identify:

- session identity
- certificate reference
- envelope reference
- dependency key
- accessed path
- reason code
- detected time

## Canonical runtime instance path

`constellation_2/runtime/truth/reports/paper_session_divergence_v1/<DAY>/paper_session_divergence.v1.json`

## Fail-closed rules

Undeclared dependency access, envelope drift, or contract-violating runtime access must emit divergence and fail closed.
