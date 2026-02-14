---
id: C2_ARCHITECTURE_ONE_PAGE_V2
title: Constellation 2.0 One-Page Architecture + File Manifest
version: 2
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
        | binds to broker payload digest BEFORE broker call
        v
[BindingRecord v1]
        |
        | OFFLINE submit boundary (no broker call)
        | evaluates invariants + hash bindings + freshness
        v
(Submit Preflight: Offline)
        |
        | if allowed
        v
[SubmitPreflightDecision v1 (ALLOW)]
        |
        | if later executing (outside Phase C)
        v
(Submitter: Broker Call)  ---> [BrokerSubmissionRecord v2] ---> [PositionLifecycle v1]


Blocked path (any boundary):
- If any validation fails at INTENT / MAPPING / SUBMIT PREFLIGHT:
  → emit [VetoRecord v1]
  → do not proceed downstream
  → no broker call

Key enforcement points:
- Options-only invariant enforced at INTENT + SUBMIT PREFLIGHT
- Defined-risk invariant enforced at MAPPING + SUBMIT PREFLIGHT
- Freshness enforced at MAPPING + SUBMIT PREFLIGHT
- Binding chain enforced across all evidence outputs
- Single-writer immutability enforced globally

---

## 2. Design Pack File Manifest (All New Files)

### Root
- `constellation_2/DESIGN_PACK_INDEX.md` — Design Pack index and reading order

### Governance (Contracts + Hostile Review Pack)
- `constellation_2/governance/C2_EXECUTION_CONTRACT.md` — authoritative execution contract (A/B/C)
- `constellation_2/governance/C2_DETERMINISM_STANDARD.md` — canonical JSON + hashing standard
- `constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md` — invariant list + reason codes
- `constellation_2/governance/C2_SCHEMA_REGISTRY.md` — schema inventory + enforcement rule
- `constellation_2/governance/C2_THREAT_MODEL.md` — threat model for hostile review
- `constellation_2/governance/C2_FAILURE_SEMANTICS.md` — failure taxonomy and enforcement semantics
- `constellation_2/governance/C2_AUDIT_EVIDENCE_CHAIN.md` — chain traceability rules
- `constellation_2/governance/C2_ABUSE_CASES.md` — abuse cases and fail-closed expectations
- `constellation_2/governance/C2_REGRESSION_TEST_PLAN.md` — regression test plan for structural guarantees

### Schemas (Bundle A/B/C + Phase C Extension)
- `constellation_2/schemas/options_intent.v2.schema.json` — OptionsIntent v2
- `constellation_2/schemas/order_plan.v1.schema.json` — OrderPlan v1
- `constellation_2/schemas/broker_submission_record.v2.schema.json` — BrokerSubmissionRecord v2
- `constellation_2/schemas/position_lifecycle.v1.schema.json` — PositionLifecycle v1
- `constellation_2/schemas/veto_record.v1.schema.json` — VetoRecord v1
- `constellation_2/schemas/freshness_certificate.v1.schema.json` — FreshnessCertificate v1
- `constellation_2/schemas/options_chain_snapshot.v1.schema.json` — OptionsChainSnapshot v1
- `constellation_2/schemas/mapping_ledger_record.v1.schema.json` — MappingLedgerRecord v1
- `constellation_2/schemas/binding_record.v1.schema.json` — BindingRecord v1
- `constellation_2/schemas/submit_preflight_decision.v1.schema.json` — SubmitPreflightDecision v1 (offline submit boundary outcome)

### Acceptance
- `constellation_2/acceptance/C2_ACCEPTANCE_CHECKLIST.md` — deterministic acceptance checklist
- `constellation_2/acceptance/samples/sample_options_intent.v2.json` — sample OptionsIntent
- `constellation_2/acceptance/samples/sample_chain_snapshot.v1.json` — sample Chain Snapshot
- `constellation_2/acceptance/samples/sample_freshness_certificate.v1.json` — sample FreshnessCertificate (hash bound)

### Specs
- `constellation_2/specs/C2_ARCHITECTURE_ONE_PAGE.md` — this one-page diagram + manifest

### Phase B (Options Market Data Truth Spine Implementation)
- `constellation_2/phaseB/` — offline chain snapshot + freshness certificate builder (deterministic, fail-closed)

### Phase C (Offline Mapping + Submit Preflight + Evidence Writer)
- `constellation_2/phaseC/` — offline mapping + submit preflight boundary (deterministic, fail-closed)

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
- offline submit boundary decision (Phase C) without broker access
