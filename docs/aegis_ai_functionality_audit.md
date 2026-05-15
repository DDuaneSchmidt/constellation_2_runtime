# Aegis AI Functionality Audit

Date: 2026-05-15

Scope: current Aegis Lite / Aegis Research direction after the pivot away from autonomous IB paper execution. This document audits what Aegis can do as deterministic governed software, what is only designed, and what is intentionally forbidden.

## Self-Healing / Auto-Repair Functionality

Self-healing in Aegis is narrowly defined. It may repair or rebuild safe derived artifacts through the owning producer path, enqueue offline Research Lab tasks, and recommend operator next actions. It must fail closed.

Self-healing must never submit trades, bypass governance, auto-promote hypotheses, change strategy logic, mutate finalized EOD decisions, clear safety gates by hand, or replace missing evidence with fake PASS artifacts.

### Capability Classification

| # | Capability | Classification | Current audit |
|---|---|---|---|
| 1 | Detect stale/missing artifacts | PROVEN | Existing readiness kernels, stale-artifact guards, schema validation, and Research/Lite tests detect missing or stale evidence and fail closed. |
| 2 | Identify canonical blockers | PROVEN | PAPER-ready/day-control surfaces record first canonical blockers and preserve upstream/downstream blocker distinction. |
| 3 | Recommend operator next actions | PROVEN | Existing readiness/kernel stages expose `next_action`/repair guidance, and Lite UI/report docs present operator checklist and blockers. |
| 4 | Rebuild safe derived artifacts | PRESENT_UNPROVEN | Owner-producer refresh paths exist for some derived evidence. The safe rule is valid, but coverage is uneven and not a general autonomous repair engine. |
| 5 | Re-run failed offline Research Lab tasks | DESIGNED_NOT_IMPLEMENTED | Research queues and runners process open tasks. Automatic failed-task retry/requeue policy is not implemented as a proven loop. |
| 6 | Preserve queues instead of overwriting them | PROVEN | Research intake preserves existing `research_task_queue.v1`; regression tests cover this. |
| 7 | Detect duplicate/overlap hypotheses | PROVEN | Hypothesis intake emits related hypothesis IDs and overlap reason codes using edge family, regime, tags, and thesis/trigger/outcome similarity. |
| 8 | Detect skipped research stages | PROVEN | `apply_experiment_result_v1` rejects results whose stage does not match the current `hypothesis_test_plan.v1` stage. |
| 9 | Block invalid trade packets | PROVEN | `manual_trade_packet.v1` marks candidates non-actionable unless promoted sleeve source, entry, stop, risk, sizing, symbol, side, and instrument are complete. |
| 10 | Block stale event alerts | PRESENT_UNPROVEN | Event-awareness designs describe stale alert blocking. Treat as unproven unless the active release includes the event validity gate tests and deployment. |
| 11 | Create offline learning tasks from outcomes | PROVEN | `outcome_ledger.v1` can generate offline Research Lab tasks for sleeve failures, friction, overlap, regime dependency, and missed opportunities. |
| 12 | Recover from failed EOD/manual packet generation | DESIGNED_NOT_IMPLEMENTED | The system can fail closed and report blockers. Automatic recovery/rebuild of the full EOD/manual packet is not a proven capability. |
| 13 | Recover from missing manual execution receipts | MISSING | Missing receipts require operator capture or explicit import. Aegis can warn/diagnose, not reconstruct trustworthy manual execution evidence. |
| 14 | Recover from missing outcome ledger data | MISSING | Missing outcome data requires market/account/operator evidence. Aegis can enqueue research or mark attribution unavailable, but cannot safely invent outcomes. |
| 15 | Detect data-source failure | PROVEN | Market-data gates and Research Lab placeholder results detect unavailable data and fail closed to missing/insufficient data states. |
| 16 | Degrade safely to `NO_TRADE` / `NOT_READY` | PROVEN | Lite and readiness surfaces are fail-closed. Missing required evidence produces advisory, blocked, `NOT_READY`, or non-actionable status rather than execution. |

### What Aegis Can Self-Heal Today

Aegis can safely self-heal only in limited, governed cases:

- Preserve and extend Research Lab queues without overwriting existing tasks.
- Rebuild specific safe derived artifacts when an owning producer explicitly supports refresh and the rebuild does not fabricate source evidence.
- Recompute deterministic Research Lab derived reports such as awareness reports from existing research artifacts.
- Enqueue offline learning tasks from outcome evidence.
- Re-run open Research Lab tasks through the offline runner when a valid research root and queue exist.

These actions do not create trades, do not promote sleeves, and do not alter final production decisions.

### What Aegis Can Only Diagnose

Aegis can diagnose but not automatically fix:

- Missing operator/manual execution receipts.
- Missing or incomplete outcome ledger data.
- Missing source market data or unavailable research datasets.
- Failed EOD/manual packet generation when the failure is caused by missing source evidence.
- Stale or missing governance inputs that require an owner producer, operator statement, or human approval.
- Unsupported trade structures or incomplete entry/stop/risk/sizing data.

Diagnosis should produce explicit blockers, reason codes, artifact paths, and operator next actions.

### What Requires Operator Action

Operator action is required for:

- Manual IB paper entry.
- Manual execution receipts.
- Operator decisions such as entered, skipped, modified, watchlist, or rejected.
- Missing protective-stop confirmation.
- Human approval for promotion.
- Binding or importing external datasets when Research Lab has `insufficient_data`.
- Supplying operator statements or other intentionally manual governance evidence.

### Intentionally Forbidden Self-Healing

The following are `UNSAFE_NOT_ALLOWED`:

- Clearing or bypassing governance gates.
- Faking PASS evidence.
- Creating market-data snapshots without the owning producer and data source.
- Creating manual execution receipts without operator evidence.
- Changing strategy logic to make a failing hypothesis pass.
- Auto-promoting a hypothesis or sleeve.
- Submitting trades or enabling transmit.
- Mutating finalized EOD decisions.
- Rewriting outcome ledger facts to improve performance attribution.

### Explicit Safety Answers

Can Aegis ever self-heal by changing strategy logic?

No. Strategy logic changes require source review, tests, commit, governed build, and activation. Self-healing may only diagnose strategy failure or enqueue offline research tasks.

Can Aegis ever self-heal by promoting sleeves?

No. Promotion requires completed research protocol evidence and human/operator approval. Research artifacts and self-healing tasks cannot automatically place a sleeve into the promoted sleeve library.

Can Aegis ever self-heal by submitting trades?

No. Autonomous broker execution is deferred. Aegis Lite is manual-execution-first and `broker_submit_required=false`.

Can Aegis ever self-heal by bypassing gates?

No. Gates are fail-closed. Self-healing may rebuild safe derived artifacts through the owning path, but it cannot bypass data, governance, risk, stop, promotion, or report-completeness gates.

## Summary

Aegis currently has proven diagnostic and fail-closed behavior, proven Research Lab queue preservation, proven duplicate/stage/trade-packet safety checks, and proven offline learning-task generation. It does not have a broad autonomous repair system, and that is intentional. The allowed self-healing surface is limited to deterministic rebuilds of safe derived evidence and offline research queue work. Anything that changes source evidence, strategy behavior, promotion status, trade execution, or finalized decisions is forbidden.
