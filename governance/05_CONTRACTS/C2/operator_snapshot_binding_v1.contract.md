---
id: C2_OPERATOR_SNAPSHOT_BINDING_CONTRACT_V1
title: "C2 Operator Snapshot Binding Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_operator_snapshot_binding
---

# C2 Operator Snapshot Binding Contract V1

## Purpose

Core 5 must never summarize floating latest state.

It must first bind one exact cross-core snapshot that names:

- Core 1 health reference
- Core 1 audit reference set
- Core 2 trade truth and provenance references
- Core 3 action authority and provenance references
- Core 4 submit-boundary reference set
- exact Core 5 rule-version set

## Binding rules

- binding is immutable once materialized
- binding identity must derive only from exact bound refs plus exact rule versions
- the same bound refs plus the same rule versions must produce the same binding artifact
- no lower-core ref may be replaced implicitly by a later artifact

## Coherence rules

At minimum the binding must record whether:

- trade identity matches across Core 2 and Core 3
- environment, sleeve, and account context remain coherent across bound artifacts
- Core 2 provenance points to the exact bound Core 1 health ref
- Core 3 upstream Core 2 refs match the exact bound Core 2 refs
- Core 4 boundary day and environment match the summarized trade snapshot scope

## Missing, incompatible, superseded, and incomplete handling

The binding artifact must explicitly record:

- required lower-core artifact missing
- lower-core snapshot incoherent
- lower-core snapshot superseded when explicitly supplied or otherwise proven by bound metadata
- lower-core provenance incomplete
- rollup rule version missing

Core 5 must not replace these conditions with cleaner language.

## Fail-transparent rule

If coherent summary is not possible, Core 5 must still materialize the binding with explicit degraded or incomplete status and drill-down refs.

Core 5 must not fabricate a coherent snapshot.
