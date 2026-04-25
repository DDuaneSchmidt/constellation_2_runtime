---
id: C2_WEEKLY_SCORECARD_VIEW_V1
title: "C2 Weekly Scorecard View Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-18
owner: Constellation
authority: governance+git+derived_read_model
scope: constellation_2_0_governed_weekly_evaluation
---

# C2 Weekly Scorecard View Contract v1

## Purpose

Define the derived weekly operator scorecard view for governed sleeve and portfolio evaluation.

## Canonical role

`weekly_scorecard_view_v1` is a derived read model only.

It is not a canonical authority artifact.

## Allowed inputs

- `sleeve_performance_truth_v1`
- `sleeve_validity_state_v1`
- `sleeve_evaluation_state_v1`
- `sleeve_governance_action_state_v1`
- `portfolio_performance_truth_v1`
- `portfolio_validity_state_v1`
- `portfolio_evaluation_state_v1`
- `portfolio_governance_action_state_v1`

## Required restrictions

- must not compute canonical pnl independently
- must not compute drawdown independently
- must not compute benchmark returns independently
- must not compute expectancy independently
- must not decide governance action independently
- must not claim governed artifact authority

## Required derived outputs

- sleeve summary rows
- portfolio summary row
- presentation-only grade or label fields
- explicit `no_conclusion` visibility
- canonical source references for every row

## Fail-closed rule

If required canonical evaluation or action artifacts are missing, the scorecard must surface missing-source visibility rather than inventing a conclusion.
