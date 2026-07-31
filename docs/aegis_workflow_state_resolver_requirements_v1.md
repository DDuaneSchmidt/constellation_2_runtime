# Aegis Workflow State Resolver Requirements v1

## Purpose

`aegis_workflow_state_resolver_v1` is the canonical rule artifact that explains how hypothesis workflow states are selected. Workflow aggregation may collect facts, but it must not hide state precedence inside UI code or implicit branches.

## Required Rules

- `NEEDS_DATA` overrides ready states when required evidence or datasets are missing.
- `PAPER_PROMOTION_APPROVED` overrides `PAPER_PROMOTION_RECOMMENDED`.
- `PAPER_TRACKING_BLOCKED` overrides `PAPER_TRACKING_READY`.
- `STATISTICALLY_SUFFICIENT` overrides underpowered states.
- `RETIRE_RECOMMENDED` and `CAPITAL_REVIEW_RECOMMENDED` only appear after sufficient evidence.
- Stale artifacts cannot override newer authoritative inputs.

## Required Outputs

- Explicit ordered resolver rules.
- Rule ids suitable for recording on each hypothesis row.
- Stale input policy.
- Deterministic content hash.
- Paper-research-only safety statement.

## Non-Goals

This resolver must not enable broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, or automatic real-world position management.
