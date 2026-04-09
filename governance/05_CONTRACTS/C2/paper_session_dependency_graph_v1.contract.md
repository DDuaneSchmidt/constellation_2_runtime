---
id: C2_PAPER_SESSION_DEPENDENCY_GRAPH_V1
title: "C2 Paper Session Dependency Graph Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_admission
---

# C2 Paper Session Dependency Graph Contract v1

## Purpose

This contract defines the governed transitive dependency graph for a specific paper session.

## Required meaning

The graph must capture:

- graph identity
- session identity
- node set
- edge set
- dependency keys
- admitted sleeve scope
- graph fingerprint

## Authority boundary

This graph is the authoritative dependency expansion for session closure and admission.

The orchestrator must not synthesize dependencies outside this graph.

## Canonical runtime instance path

`constellation_2/runtime/truth/reports/paper_session_dependency_graph_v1/<DAY>/paper_session_dependency_graph.v1.json`

## Fail-closed rules

If the graph omits a required dependency, has duplicate node identities, or cannot produce a stable graph fingerprint, it must fail closed.
