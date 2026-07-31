# Aegis Hypothesis Workflow State Design V1

## Design

The workflow state engine reads all upstream hypothesis artifacts, normalizes them by `hypothesis_id`, and applies deterministic precedence rules to emit one canonical row per hypothesis.

## Inputs

Inputs include research portfolio, generated proposals, evidence packets, shadow trials, promotion packets, approval events, paper setup status, paper observations, outcomes, validation samples, statistical sufficiency, research allocation, and blocker-bearing artifacts.

## Precedence

State selection is intentionally conservative. Data blockers and stale artifacts prevent optimistic ready states. Operator approval events override pending promotion recommendations. Paper tracking blockers override readiness. Statistical sufficiency is only set from explicit sufficiency evidence.

## Consumer Contract

The Research UI must treat this report as a read model. It may sort, filter, and render, but it must not derive workflow state, invent next action, or create buttons not present in the backend action queue.
