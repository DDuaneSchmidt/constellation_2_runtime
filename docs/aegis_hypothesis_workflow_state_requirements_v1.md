# Aegis Hypothesis Workflow State Requirements V1

## Scope

Define one canonical workflow state engine for Aegis hypotheses. The engine is research-only and must not enable broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, or automatic real-world position management.

## Requirements

1. Produce `aegis_hypothesis_workflow_state_v1` as the authoritative hypothesis state report.
2. Aggregate seeded active hypotheses and generated proposals into one state model.
3. Consume only verified artifacts and source hashes; UI consumers must not infer state.
4. Include every required record field: identifiers, display name, source type, current/prior state, next action, allowed actions, reasons, blockers, source paths, source hashes, generated timestamps, computed timestamp, stale rejection flag, and transition history.
5. Enforce explicit state precedence:
   - blocked or `NEEDS_DATA` states override ready states
   - `PAPER_PROMOTION_APPROVED` overrides `PAPER_PROMOTION_RECOMMENDED`
   - `PAPER_TRACKING_BLOCKED` overrides `PAPER_TRACKING_READY`
   - `STATISTICALLY_SUFFICIENT` overrides underpowered evidence states
   - retirement and capital review states only appear after sufficient evidence
   - stale intermediate artifacts cannot override newer authoritative inputs
6. Keep reruns deterministic for identical target day and inputs.

## Phase 1 Acceptance

For `TARGET_DAY=2026-06-01`, Oil Shock must not regress to `PAPER_PROMOTION_RECOMMENDED` after approval, Macro Calendar must be `NEEDS_DATA` with a concrete data-source action, and seeded hypotheses must appear in the hypothesis list without vague David action requests.
