# Aegis Governance Post-Pivot Alignment

Date: 2026-05-15

## Purpose

This document aligns governance language after the Aegis Lite pivot. It does not activate production, broker automation, IB Gateway, submit boundaries, fill lifecycle automation, or autonomous execution.

Audit scope covered markdown governance and operator documentation under `governance/` and `docs/` for references to autonomous execution, broker-required runtime, IB-dependent operation, continuous orchestration, selected-intent execution, execution observers, auto-submit/fill lifecycle, AI production mutation, automated promotion/demotion, and old PAPER runtime assumptions.

## Current Operational Model

Aegis Lite is the current operator-facing manual-paper architecture:

- broker-independent runtime
- advisory-only operation
- one canonical EOD run near market close
- manual execution by the operator
- manual execution receipts as the source of truth when broker integration is absent
- deterministic artifact production
- fail-closed readiness semantics
- no autonomous broker submit
- no IB transmit automation
- no automatic fill lifecycle
- no production mutation from AI, Research, or event monitoring

The canonical post-pivot flow is:

`promoted_sleeve_library.v1 -> Aegis Lite EOD -> manual_trade_packet.v1 / operator_execution_queue.v1 -> manual_execution_receipt.v1 -> outcome_ledger.v1 -> sleeve_performance_report.v1 -> Research feedback`

## Current Research Model

Research Lab is offline-only and non-authoritative for runtime trading.

Research may:

- capture hypotheses
- queue offline research tasks
- produce evidence packets
- write result ledgers
- preserve conclusions and failure archetypes
- recommend Research follow-up work
- prepare human-reviewed promotion artifacts

Research may not:

- create trades
- mutate Aegis Lite runtime
- bypass promotion gates
- create promoted sleeves without human approval
- authorize broker submit
- authorize IB transmit

Operator Inbox is separate from Research Lab. It captures raw ideas and reminders only. Inbox items cannot create Research tasks, sleeves, trades, or production state without explicit review and conversion.

## Current AI Model

AI is bounded by governance gates. AI may:

- summarize
- classify
- recommend
- explain failure modes
- suggest hypothesis refinements
- create or recommend offline Research tasks when the Evidence Gate and Research Task Gate allow it

AI may not:

- create trades
- mutate production logic
- change thresholds
- bypass gates
- promote sleeves automatically
- demote sleeves automatically
- authorize allocations
- submit orders
- infer hidden conversational state as runtime truth

The AI Feedback Engine is deterministic fallback in current repo state. It records `ai_used=false` and `deterministic_fallback_used=true` unless a separately governed AI runtime is introduced later.

## Evidence Gate And Research Task Gate

The Evidence Gate determines whether outcome evidence is clean enough for AI-assisted review.

Evidence Gate sample semantics:

- `NO_EVIDENCE`: no trade evidence
- `OBSERVATION_ONLY`: 1-2 trades; no automatic Research task creation
- `WEAK_SIGNAL`: 3-9 trades; low-priority offline Research task may be created
- `REVIEWABLE_PATTERN`: 10-19 trades; normal offline Research task may be created
- `STRONGER_PATTERN`: 20+ trades; higher-priority offline Research task may be created

Missing receipts, missing outcomes, stale data, missing labels, or missing price/outcome evidence downgrade confidence and may block task creation or strong conclusions.

The Research Task Gate may create only offline Research Lab tasks. It cannot change Lite, create trades, or promote/demote sleeves.

## Event Monitoring Governance

Event monitoring is non-canonical and advisory-only.

Event monitoring may:

- read versioned event rules
- evaluate market snapshots
- write event monitor status
- write event awareness ledger entries
- create non-canonical tactical packets
- run validity and alert gates
- produce operator-visible alert candidates
- feed later offline Research learning

Event monitoring may not:

- submit trades
- touch IB/broker paths
- mutate canonical EOD state
- overwrite manual trade packets
- promote Research artifacts
- create production sleeves
- hide business rules in code-only logic
- alert for demo/dry-run packets as actionable

Event rules must be visible and versioned through `event_rules_registry_v1`. Missing, disabled, stale, demo-only, dry-run-only, research-only, or invalid event packets must fail closed.

Current alert transport status is `GATE_ONLY_NO_TRANSPORT` unless a future explicit transport proof says otherwise.

## Manual Execution Receipts

Without broker integration, `manual_execution_receipt.v1` is the operator-entered source of truth for fills, quantity, stop entry, stop price, timing, notes, and deviations from recommendation.

`outcome_ledger.v1` and `sleeve_performance_report.v1` must treat missing receipts and missing outcomes as missing evidence, not as zero-return trades.

## Legacy / Deferred After Pivot

The following families remain useful only as historical, diagnostic, or future-governed references unless explicitly reactivated by a new governance phase.

| Reference family | Classification | Examples | Current interpretation |
| --- | --- | --- | --- |
| Aegis Lite pivot/current manual model | `ACTIVE_CURRENT` | `docs/aegis_lite_pivot.md`, `docs/aegis_lite_operational_spine.md`, `docs/aegis_lite_timer_model.md`, `docs/aegis_lite_manual_paper_readiness.md` | Current operating model. |
| Event monitoring and rules | `ACTIVE_CURRENT` | `docs/aegis_event_monitoring_v1.md`, `docs/aegis_event_rules_registry.md`, `docs/aegis_lite_event_awareness.md` | Non-canonical advisory event layer. |
| Research Lab offline lifecycle | `ACTIVE_CURRENT` | `docs/aegis_research_lab.md`, `docs/research_to_lite_architecture_adr.md`, `docs/research_task_queue.md`, `docs/research_result_ledger.md` | Offline research and promotion-review model. |
| Operator Inbox | `ACTIVE_CURRENT` | `docs/aegis_operator_inbox.md` | Lightweight idea capture only. |
| AI feedback and evidence gate | `ACTIVE_CURRENT` | `docs/aegis_ai_feedback_engine.md`, `docs/aegis_sleeve_review_feedback_loop.md` | Evidence-gated deterministic feedback and Research task recommendations. |
| Legacy autonomous PAPER runtime | `DEPRECATED_AFTER_PIVOT` | `docs/legacy_paper_runtime_deferred.md`, old PAPER runtime references in `governance/00_INDEX.md` | Must not define current Lite operations. |
| Broker submit baselines and submit-boundary contracts | `LEGACY_INTERNAL_ONLY` | `governance/06_BASELINES/C2/aegis_paper_submit_baseline_v1.md`, `governance/contracts/EXECUTION_KERNEL_BOUNDARY_CONTRACT.md`, `governance/contracts/EXECUTION_SUBMISSION_CONTRACT.md` | Historical/internal evidence for old execution architecture. Not current Lite authority. |
| Selected-intent execution and arbitration plans | `LEGACY_INTERNAL_ONLY` | `docs/candidate_centric_phase1_observability.md`, `docs/paper_multi_intent_execution_plan_v1.md`, `docs/shadow_candidate_arbitration_v1.md` | Diagnostic or pre-pivot planning references. Not current manual Lite workflow. |
| Execution observers and fill lifecycle contracts | `LEGACY_INTERNAL_ONLY` | `governance/03_CONTRACTS/C2/EXECUTION_EVIDENCE/execution_observer_spine_v1.md`, `governance/03_CONTRACTS/C2/EXECUTION_EVIDENCE/fill_ledger_spine_v1.md`, `governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md` | Deferred broker-era surfaces. Must not run as default Lite runtime. |
| IB Gateway/TWS market data or reconciliation contracts | `SAFE_OPTIONAL_FUTURE` | `governance/03_RUNTIME/IB_RECONCILIATION_LOOP_CONTRACT_V1.md`, `governance/05_CONTRACTS/C2/ib_historical_market_data_snapshot_downloader_v1.contract.md` | Optional future or manual/reconciliation tooling only; not a Lite runtime dependency. |
| Paper smoke tests and old readiness runbooks | `DEPRECATED_AFTER_PIVOT` | `governance/05_CONTRACTS/C2/paper_submit_smoke_test_v1.contract.md`, `governance/05_CONTRACTS/C2/paper_day_readiness_runbook_v1.contract.md` | Historical or future-reactivation references; not current readiness. |
| Obsolete UI/operator copy implying full Aegis or broker execution | `REMOVE_IF_SAFE` | primary UI labels formerly resembling full runtime/advisory surfaces | Should be removed from primary UI; diagnostic routes may remain under Legacy / Deferred. |

## Governance Deltas From Pre-Pivot Architecture

1. The current product is no longer autonomous IB paper execution. It is Aegis Lite tactical intelligence and manual execution support.
2. Broker availability is no longer a runtime prerequisite for Lite EOD, event monitoring, Research Lab, sleeve performance, or AI feedback.
3. Canonical EOD owns official daily Lite decision state. Event monitoring is non-canonical and cannot overwrite it.
4. Research Lab is advisory and offline. Promotion to Lite requires human approval and promoted-sleeve implementation evidence.
5. Operator Inbox is capture-only and cannot directly create Research tasks or sleeves.
6. AI is bounded to interpretation and offline Research task suggestions behind gates.
7. Manual receipts and outcome artifacts replace broker fills as source truth in the broker-independent manual-paper workflow.
8. Legacy PAPER orchestration, selected-intent execution, submit boundaries, execution observers, and fill lifecycle surfaces are deferred/internal unless a future governance phase reactivates them.

## Required Governance Language Going Forward

Any new governance, UI, or operator documentation should state:

- Aegis Lite is advisory-only and broker-independent.
- Trades are manually entered by the operator.
- No broker submit or IB transmit automation is part of current Lite.
- Event monitoring is advisory and non-canonical.
- Event rules must be visible and versioned.
- Demo and dry-run data cannot be actionable.
- Manual execution receipts are source truth without broker integration.
- Missing receipt/outcome data is missing evidence, not zero performance.
- AI can summarize/classify/recommend/offline-task, but cannot mutate production, trade, promote, or demote.
- Research Lab outputs are not runtime authority.
- Operator Inbox outputs are not Research or trading authority.

## Remaining Governance Gaps

1. `governance/00_INDEX.md` still indexes many pre-pivot execution and PAPER runtime contracts without a top-level post-pivot warning banner.
2. Historical submit-boundary, execution-observer, fill-ledger, and selected-intent contracts remain valuable but can be mistaken for active Lite authority if read out of context.
3. Some old readiness and smoke-test documents still describe broker-submit proof paths as readiness evidence. They need explicit legacy/deferred headers before future audits.
4. A future governed broker reactivation plan does not yet exist. This is acceptable; current governance should not imply broker automation is scheduled.
5. Research Lab dataset bindings remain incomplete, so Research governance should continue to distinguish evidence capture from proven large-sample validation.
6. AI feedback currently uses deterministic fallback; governance should not claim LLM operation until a runtime, model, prompt, audit, and evidence boundary are added.

## No Production Change

This alignment document is documentation only. It does not change trading logic, Research Lab logic, broker paths, IB startup, systemd activation, runtime truth, promotion status, or production configuration.
