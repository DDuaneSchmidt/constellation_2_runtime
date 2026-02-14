---
id: C2_ARCHITECTURE_ONE_PAGE_V3
title: Constellation 2.0 One-Page Architecture + File Manifest
version: 3
status: DRAFT
type: architecture_spec
created: 2026-02-14
authority_level: ROOT_SUPPORT
---

# Constellation 2.0 One-Page Architecture

## 1. Textual Architecture Diagram (One Page)

Legend:
- Solid arrows are deterministic data flow.
- Brackets `[]` are immutable truth artifacts (JSON + schema + canonical hash).
- Parentheses `()` are processes (tools) with fail-closed behavior.
- A broker call is permitted ONLY in Phase D and ONLY in PAPER mode.

Diagram:

(Engine: Options-only 7-suite)
        |
        | emits
        v
   [OptionsIntent v2]  -----------------------------+
        |                                          |
        | reads                                   | validates
        v                                          v
(Market Data Capture)                         (Intent Validator)
        |                                          |
        | produces                                 | if fail
        v                                          v
[OptionsChainSnapshot v1]                   [VetoRecord v1]
        |
        | binds (snapshot_hash + time window)
        v
[FreshnessCertificate v1]
        |
        | inputs: intent + chain + freshness
        v
(Mapper: Intent → Plan)
        |
        | produces
        v
   [OrderPlan v1]
        |
        | records mapping decision trace
        v
[MappingLedgerRecord v1]
        |
        | binds to broker payload digest BEFORE any broker call
        v
[BindingRecord v1]
        |
        | Phase C offline submit boundary (NO BROKER CALLS)
        v
(Submit Preflight: Offline)
        |
        | if allowed
        v
[SubmitPreflightDecision v1 (ALLOW)]
        |
        | Phase D submit boundary (PAPER broker only)
        | - revalidates all invariants
        | - enforces RiskBudget via WhatIf
        | - blocks duplicates (idempotency)
        | - writes immutable evidence before/after broker boundary
        v
(Submitter: PAPER Broker Adapter Boundary)
        |
        | ALWAYS produces one of:
        |   - SUCCESS: [BrokerSubmissionRecord v2] + [ExecutionEventRecord v1] (initial)
        |   - BLOCK:   [VetoRecord v1]
        v
[BrokerSubmissionRecord v2]  ---> [ExecutionEventRecord v1] ---> (Lifecycle Ingest) ---> [PositionLifecycle v1]

Blocked path (any boundary):
- If any validation fails at INTENT / MAPPING / SUBMIT (offline or Phase D):
  → emit [VetoRecord v1]
  → do not proceed downstream
  → no broker call

Key enforcement points:
- Options-only invariant enforced at INTENT + SUBMIT
- Defined-risk invariant enforced at MAPPING + SUBMIT
- Freshness enforced at MAPPING + SUBMIT
- Binding chain enforced across all evidence outputs
- Single-writer immutability enforced globally
- Phase D broker calls are PAPER-only and must be behind a deterministic adapter interface boundary

---

## 2. Design Pack File Manifest (All Governed Files)

### Root
- `constellation_2/DESIGN_PACK_INDEX.md` — Design Pack index and reading order

### Governance (Contracts + Hostile Review Pack)
- `constellation_2/governance/C2_EXECUTION_CONTRACT.md` — authoritative execution contract
- `constellation_2/governance/C2_DETERMINISM_STANDARD.md` — canonical JSON + hashing standard
- `constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md` — invariant list + reason codes
- `constellation_2/governance/C2_SCHEMA_REGISTRY.md` — schema inventory + enforcement rule
- `constellation_2/governance/C2_THREAT_MODEL.md` — threat model for hostile review
- `constellation_2/governance/C2_FAILURE_SEMANTICS.md` — failure taxonomy and enforcement semantics
- `constellation_2/governance/C2_AUDIT_EVIDENCE_CHAIN.md` — chain traceability rules
- `constellation_2/governance/C2_ABUSE_CASES.md` — abuse cases and fail-closed expectations
- `constellation_2/governance/C2_REGRESSION_TEST_PLAN.md` — regression test plan for structural guarantees

### Schemas (Bundle A/B/C + Phase C + Phase D Extensions)
- `constellation_2/schemas/options_intent.v2.schema.json` — OptionsIntent v2
- `constellation_2/schemas/order_plan.v1.schema.json` — OrderPlan v1
- `constellation_2/schemas/broker_submission_record.v2.schema.json` — BrokerSubmissionRecord v2
- `constellation_2/schemas/position_lifecycle.v1.schema.json` — PositionLifecycle v1
- `constellation_2/schemas/veto_record.v1.schema.json` — VetoRecord v1
- `constellation_2/schemas/freshness_certificate.v1.schema.json` — FreshnessCertificate v1
- `constellation_2/schemas/options_chain_snapshot.v1.schema.json` — OptionsChainSnapshot v1
- `constellation_2/schemas/mapping_ledger_record.v1.schema.json` — MappingLedgerRecord v1
- `constellation_2/schemas/binding_record.v1.schema.json` — BindingRecord v1
- `constellation_2/schemas/submit_preflight_decision.v1.schema.json` — SubmitPreflightDecision v1
- `constellation_2/schemas/risk_budget.v1.schema.json` — RiskBudget v1
- `constellation_2/schemas/execution_event_record.v1.schema.json` — ExecutionEventRecord v1

### Acceptance
- `constellation_2/acceptance/C2_ACCEPTANCE_CHECKLIST.md` — deterministic acceptance checklist

### Specs
- `constellation_2/specs/C2_ARCHITECTURE_ONE_PAGE.md` — this document

### Phase B (Options Market Data Truth Spine Implementation)
- `constellation_2/phaseB/` — offline chain snapshot + freshness certificate builder

### Phase C (Offline Mapping + Submit Preflight + Evidence Writer)
- `constellation_2/phaseC/` — offline mapping + submit preflight boundary (no broker calls)

### Phase D (Paper Broker Integration + Execution Lifecycle Truth Spine)
- `constellation_2/phaseD/` — paper broker adapter boundary + whatif gate + idempotent submission + lifecycle ingestion

---

## 3. Explicit Non-Claims

This architecture does NOT claim:
- profitability, alpha, Sharpe
- correct market forecasts
- broker uptime
- fill quality or slippage targets

It claims only:
- deterministic structure
- fail-closed enforcement
- immutable evidence chain suitable for hostile review
- Phase D PAPER broker boundary behind a deterministic adapter interface
