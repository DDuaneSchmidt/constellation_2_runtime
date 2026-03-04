---
id: C2_ECONOMIC_TRUTH_SPINE_V1
title: "Constellation 2.0 — Economic Truth Spine V1 (Cash→Positions→NAV→Allocation→Risk)"
status: DRAFT
version: 1
created_utc: 2026-03-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - economic_truth
  - nav
  - cash_ledger
  - positions
  - allocation
  - risk_gates
  - determinism
  - fail_closed
---

# Economic Truth Spine V1

## Objective
Define the canonical, deterministic, audit-grade chain that transforms:
- Operator authority (cash funding)
- Broker authority (positions, executions, marks)

into:
- Accounting NAV (economic truth)
- Allocation truth
- Risk enforcement truth
- Orchestrator run verdict truth

The spine MUST support unattended daily operation in PAPER and LIVE.

## Canonical repo root
All paths are relative to repo root:
`/home/node/constellation_2_runtime`

## Single-account topology invariant (hard stop)
All sleeves/engines and all daily runs MUST use a single IB account:
- PAPER account_id: `DUO847203`

Any other account id anywhere is a safety breach and MUST ABORT.

## Day key convention
All daily artifacts use `day_utc` keys in `YYYY-MM-DD` format.

## Spine layers (authoritative order)

### Layer 0 — Operator authority (required)
**Operator statement** MUST exist before baseline/trading run:
`constellation_2/operator_inputs/cash_ledger_operator_statements/<day_utc>/operator_statement.v1.json`

Required fields:
- account_id == DUO847203
- currency
- cash_total (non-zero for funded baseline)
- nlv_total
- observed_at_utc == `<day_utc>T00:00:00Z`

### Layer 1 — Canonical state snapshots (required)
**Positions snapshot** MUST exist:
`constellation_2/runtime/truth/positions_v1/snapshots/<day_utc>/positions_snapshot.v2.json`

**Cash ledger snapshot** MUST exist:
`constellation_2/runtime/truth/cash_ledger_v1/snapshots/<day_utc>/cash_ledger_snapshot.v1.json`

Cash ledger snapshot MUST:
- be derived from the operator statement (input_manifest reference required)
- contain `snapshot.cash_total_cents` and `snapshot.currency`
- preserve account_id == DUO847203

**Execution submissions directory invariant** MUST hold:
`constellation_2/runtime/truth/execution_evidence_v1/submissions/<day_utc>/` MUST exist (can be empty)

### Layer 2 — Transaction truth (conditional)
**Fill ledger** MAY exist; if executions occur it MUST exist:
`constellation_2/runtime/truth/fill_ledger_v1/<day_utc>/fill_ledger.v1.json`

Missing submissions directory MUST NOT be treated as “missing input”; it is an invariant and must be created by the orchestrator.

### Layer 3 — Valuation truth (conditional)
**Broker marks** are REQUIRED IFF positions exist:
`constellation_2/runtime/truth/market_data_snapshot_v1/broker_marks_v1/<day_utc>/broker_marks.v1.json`

If positions are empty, broker marks MUST be treated as optional and MUST NOT force NAV bootstrap.

### Layer 4 — Accounting NAV (required)
**Accounting NAV v2** MUST exist:
`constellation_2/runtime/truth/accounting_v2/nav/<day_utc>/nav.v2.json`

NAV v2 MUST be computed from:
- cash ledger snapshot (required)
- positions snapshot (required)
- broker marks (required iff positions exist)

NAV v2 MUST satisfy:
- status == ACTIVE for funded baseline days
- nav.nav_total > 0 for funded baseline days
- input_manifest contains referenced file paths and sha256 where present

BOOTSTRAP NAV is permitted only for explicit genesis/anchor days governed outside normal daily operation.

### Layer 5 — Allocation truth (conditional but enforced before trading)
If intents are to be emitted or trades are to be executed, **allocation summary** MUST exist:
`constellation_2/runtime/truth/allocation_v1/summary/<day_utc>/summary.json`

Allocation MUST NOT be computed without NAV v2 ACTIVE and > 0.

### Layer 6 — Risk enforcement truth (conditional)
Risk gates MUST consume the spine and MUST NOT precede it.

When activity is present (intents/submissions/fills), gates may be required and blocking per policy.

When activity is not present, gates that depend on market context MUST be activity-gated and MUST NOT ABORT baseline.

### Layer 7 — Orchestrator verdict truth (required)
Orchestrator V2 MUST:
- produce an attempt manifest
- produce a run verdict artifact
- produce pipeline manifests/pointer indices
- ABORT only for true safety breaches (account mismatch/topology breach, kill switch, integrity corruption)

## Non-bricking guarantee
- No-activity days MUST be survivable and produce a verdict (PASS/DEGRADED/FAIL) without ABORT unless safety breach.
- Missing optional conditional artifacts MUST NOT brick the run.
