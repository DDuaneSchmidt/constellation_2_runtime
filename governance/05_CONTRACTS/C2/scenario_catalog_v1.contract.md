---
id: C2_SCENARIO_CATALOG_V1
title: "C2 Scenario Catalog Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-06
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Scenario Catalog Contract (V1)

## Purpose

This contract defines the governed scenario-catalog artifact used to record explicit testing scenarios and their expected governed outcomes.

This phase introduces only the shared scenario-catalog artifact model.

It does not execute scenarios, activate production, or change decision logic.

## Required catalog meaning

A scenario-catalog artifact must explicitly identify:

- catalog id
- scenario id
- domain scope
- scenario type
- purpose
- input artifact refs
- policy refs
- expected outputs
- expected action classes
- expected precedence notes
- created-at timestamp
- status

## Non-authoritative semantics

The scenario catalog is testing-evidence truth only.

The scenario catalog must not be treated as runtime decision authority or lifecycle activation authority.

## Canonical runtime instance path

When materialized as a governed runtime-truth report, the canonical path pattern is:

`constellation_2/runtime/truth/reports/<artifact_family_v1>/<DAY>/<artifact>.v1.json`

For this artifact family, the concrete path is:

`constellation_2/runtime/truth/reports/scenario_catalog_v1/<DAY>/scenario_catalog.v1.json`

## Fail-closed rules

If scenario identity, expected outputs, expected action classes, or required source references are missing or ambiguous, the artifact must fail closed.

If the scenario catalog is used to bypass governed runtime truth or governed release truth, the system must fail closed.

## Non-claims

This contract does not define scenario execution, replay execution, or promotion approval.
