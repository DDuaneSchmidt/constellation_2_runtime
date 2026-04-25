---
id: C2_ACTION_DECISION_PROVENANCE_CONTRACT_V1
title: "C2 Action Decision Provenance Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_action_provenance
---

# C2 Action Decision Provenance Contract V1

## Purpose

Decision provenance is a first-class Core 3 artifact.

It exists so every final action posture is explainable, replayable, and audit-grade.

## Required provenance fields

At minimum `action_decision_provenance.v1` must record:

- Core 2 input artifact refs used
- Core 2 fields read
- candidate actions generated
- rejected candidates and reasons
- blocker rules fired
- conflict rule applied
- final posture rationale
- policy and rule versions used
- prior-state comparison when available

## Required rationale questions

Provenance must answer:

- why an action became `REQUIRED`
- why an action remained only `ALLOWED`
- why an action became `FORBIDDEN`
- why an action became `BLOCKED`
- why `REVIEW_REQUIRED` replaced autonomous action

## Replay rule

Given identical Core 2 inputs and identical policy versions, the recorded provenance must be sufficient to replay the same gate verdict, candidate set, and final authority outcome.
