---
id: C2_DETERMINISM_STANDARD_V1
title: Constellation 2.0 Determinism and Canonicalization Standard
version: 1
status: DRAFT
type: determinism_standard
created: 2026-02-13
authority_level: ROOT_SUPPORT
---

# Constellation 2.0 Determinism Standard

## 1. Purpose

This document defines the canonical JSON serialization and hashing rules required for:

- Evidence chaining
- Cross-boundary integrity
- Audit-grade reproducibility
- Strict replay validation

All C2 truth artifacts MUST conform.

---

## 2. Canonical JSON Requirements

Every serialized artifact MUST:

1. Use UTF-8 encoding
2. Use sorted keys (lexicographic)
3. Use no insignificant whitespace
4. Use explicit numeric representations
5. Contain no trailing zeros beyond precision rules
6. Contain no implicit default fields

Serialization must produce byte-identical output given identical logical input.

---

## 3. Numeric Rules

- No scientific notation
- Decimal values must use fixed precision
- Precision rules defined per schema
- No implicit rounding
- No floating-point derived ambiguity

If precision cannot be guaranteed:
→ HARD BLOCK

---

## 4. Hashing Rules

Every artifact MUST contain:

- canonical_json_hash (SHA-256)
- upstream_hash (if derived)

Hash computation:

1. Canonicalize JSON
2. Encode as UTF-8
3. SHA-256 digest
4. Lowercase hex string

Hash MUST represent canonical form only.

---

## 5. Chain Integrity Rules

If artifact B derives from A:

- B.upstream_hash MUST equal A.canonical_json_hash
- No intermediate mutation permitted
- No field reordering allowed after hashing

If mismatch:
→ HARD BLOCK
→ VetoRecord REQUIRED

---

## 6. Replay Determinism

Given identical:

- Intent
- Chain snapshot
- Schema version
- FreshnessCertificate

The resulting OrderPlan MUST be byte-identical.

Replay validation MUST compare:

- canonical_json_hash
- all derived deterministic fields

If mismatch:
→ HARD FAIL

---

## 7. Forbidden Practices

Prohibited:

- Implicit default injection
- Time-of-day dependent formatting
- Non-deterministic ID generation
- UUIDv4
- Randomized salt
- Environment-dependent serialization
- Locale-dependent formatting

Allowed ID types:

- Hash-derived IDs only

---

## 8. Time Handling

All timestamps MUST:

- Be UTC
- Use ISO-8601 with Z suffix
- No timezone offsets
- No local time conversions

Example:
2026-02-13T21:32:45Z

---

## 9. Schema Versioning

Every artifact MUST contain:

- schema_id
- schema_version

Schema mismatch:
→ HARD BLOCK

---

## 10. Determinism Claim

C2 claims:

Given identical inputs and schema versions,
output is:

- deterministic
- canonicalized
- cryptographically verifiable
- replay-stable

C2 does NOT claim:

- absence of logical errors
- correctness of market assumptions
- profitability
