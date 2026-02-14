---
id: C2_SCHEMA_REGISTRY_V2
title: Constellation 2.0 Schema Registry
version: 2
status: DRAFT
type: schema_registry
created: 2026-02-14
authority_level: ROOT_SUPPORT
---

# Constellation 2.0 Schema Registry

## 1. Purpose

This registry enumerates every **Constellation 2.0** schema and assigns each schema a stable identity.

Rules:
- All schemas MUST live under: `constellation_2/schemas/`
- All schemas MUST have a stable `$id` of the form: `https://constellation.local/schemas/<filename>`
- All root objects MUST declare:
  - `schema_id`
  - `schema_version`
- All root objects MUST set: `"additionalProperties": false`
- Implementations MUST refuse any schema-less artifact
- Schema mismatch is a HARD BLOCK
- Schema validation failure MUST produce `VetoRecord v1` and STOP

---

## 2. Schema Inventory (Bundle A/B/C + Phase C Extension)

### Bundle A (Contracts + Core Models)

1. OptionsIntent v2  
   Path: `constellation_2/schemas/options_intent.v2.schema.json`

2. OrderPlan v1  
   Path: `constellation_2/schemas/order_plan.v1.schema.json`

3. BrokerSubmissionRecord v2  
   Path: `constellation_2/schemas/broker_submission_record.v2.schema.json`

4. PositionLifecycle v1  
   Path: `constellation_2/schemas/position_lifecycle.v1.schema.json`

5. VetoRecord v1  
   Path: `constellation_2/schemas/veto_record.v1.schema.json`

6. FreshnessCertificate v1  
   Path: `constellation_2/schemas/freshness_certificate.v1.schema.json`

---

### Bundle B (Options Market Data Truth Spine)

7. OptionsChainSnapshot v1  
   Path: `constellation_2/schemas/options_chain_snapshot.v1.schema.json`

---

### Bundle C (Mapping + Submission Evidence Chain)

8. MappingLedgerRecord v1  
   Path: `constellation_2/schemas/mapping_ledger_record.v1.schema.json`

9. BindingRecord v1  
   Path: `constellation_2/schemas/binding_record.v1.schema.json`

---

### Phase C Extension (Offline Submit Boundary)

10. SubmitPreflightDecision v1  
    Path: `constellation_2/schemas/submit_preflight_decision.v1.schema.json`  
    Purpose: Deterministic decision artifact for offline submit preflight.  
    NOTE: Phase C fail-closed behavior still prefers **VetoRecord-only** on BLOCK; this schema exists to make the submit boundary explicit and auditable in the ALLOW case.

---

## 3. Compatibility Statement

These C2 schemas:
- are NOT compatible with any legacy schema IDs
- may evolve only by versioned replacement (v1 → v2), never in-place breaking change

---

## 4. Enforcement Requirement (Every Boundary)

Every boundary MUST perform:

- schema validation
- canonicalization (see `C2_DETERMINISM_STANDARD.md`)
- hashing (SHA-256; lowercase hex; see `C2_DETERMINISM_STANDARD.md`)

If schema validation fails:
→ `VetoRecord v1` REQUIRED  
→ STOP (no downstream writes)
