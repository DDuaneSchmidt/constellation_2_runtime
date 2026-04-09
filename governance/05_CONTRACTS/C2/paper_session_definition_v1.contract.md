---
id: C2_PAPER_SESSION_DEFINITION_V1
title: "C2 Paper Session Definition Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_admission
---

# C2 Paper Session Definition Contract v1

## Purpose

This contract defines the governed paper-session definition artifact used to declare the exact dependency scope for a paper-trading session before execution.

## Required meaning

A paper-session definition must declare:

- session identity
- target day
- mode
- operator entrypoint
- execution entrypoint
- required artifact families
- required dependency keys
- dependency source registries
- required vs optional dependency branches

## Authority boundary

This artifact is the governed source of dependency intent for session admission.

It is not itself an admission decision and it does not execute the session.

## Canonical runtime instance path

`constellation_2/runtime/truth/reports/paper_session_definition_v1/<DAY>/paper_session_definition.v1.json`

## Fail-closed rules

If a required dependency family, dependency key, entrypoint, or source registry is omitted or ambiguous, the definition must fail closed.

Undeclared runtime discovery is prohibited.
