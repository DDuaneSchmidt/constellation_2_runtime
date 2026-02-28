---
doc_kind: contract
doc_id: C2_CAPITAL_AUTHORITY_LAYER_BUNDLE_A_V1
title: "Constellation 2.0 — Capital Authority Layer (Bundle A) — Design & Deploy Instructions for AI"
version: 1
status: DRAFT
created_utc: 2026-02-27T00:00:00Z
repo_root_authoritative: /home/node/constellation_2_runtime
canonical_truth_root: /home/node/constellation_2_runtime/constellation_2/runtime/truth
scope:
  - "Capital Authority Layer = Exposure Netting (A2) + Allocation Authority + Authorization Artifacts (A1)"
  - "Pre-trade enforcement + deterministic authorization artifacts"
non_goals:
  - "No new strategy engines"
  - "No UI redesign"
  - "No discretionary overrides"
  - "No post-trade-only controls"
---

# Bundle A — Capital Authority Layer v1

## 1) Authority and invariants

### A. Authority model
- Governance (this repo) defines schemas + policy.
- Git defines code behavior.
- Runtime truth under `constellation_2/runtime/truth/` is authoritative output.

### B. Required invariants
I1. Pre-trade enforcement: no order may cross the Phase D broker boundary without an AUTHORIZATION artifact with `status="AUTHORIZED"` and a matching intent hash.
I2. Cross-engine netting: exposure accounting MUST net across all engines before authorization.
I3. Determinism: identical inputs MUST produce byte-identical artifacts for:
- `risk/exposure_net_v1`
- `allocation/capital_authority_allocation_v1`
- `engine_activity/authorization_v1`
I4. Fail-closed: missing/stale/invalid inputs MUST hard fail (no “best effort” success).
I5. Atomic publish: any `latest` pointer must update atomically; partial writes must not become latest.

## 2) Governed policy input
Bundle A MUST consume the policy manifest:
- `governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json`

## 3) Governed schemas (write-time validation required)
Bundle A outputs MUST validate against governance-owned schemas at write time:

### Exposure Net (authoritative)
- Schema: `governance/04_DATA/SCHEMAS/C2/RISK/exposure_net.v1.schema.json`
- Output spine:
  - `constellation_2/runtime/truth/risk_v1/exposure_net_v1/<DAY_UTC>/exposure_net.v1.json`

### Capital Authority Allocation (authoritative)
- Schema: `governance/04_DATA/SCHEMAS/C2/ALLOCATION/capital_authority_allocation.v1.schema.json`
- Output spine:
  - `constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1/<DAY_UTC>/capital_authority_allocation.v1.json`

### Authorization Artifacts (authoritative)
- Schema: `governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json`
- Output spine:
  - `constellation_2/runtime/truth/engine_activity_v1/authorization_v1/<DAY_UTC>/<INTENT_HASH>.authorization.v1.json`

## 4) Required linkage fields
Every authorization artifact MUST include:
- `engine_id`
- `intent_id`
- `intent_hash`
- input manifest hashes (policy, exposure net, allocation)
- deterministic `decision_hash` inside authorization block

## 5) Enforcement boundary (non-bypassable)
Phase D submit boundary MUST enforce:
- If authorization artifact missing → veto with deterministic reason.
- If authorization exists but intent_hash mismatch → veto.
- If authorization status != AUTHORIZED → veto.
- If submitted order plan quantity exceeds `authorized_quantity` → veto.

## 6) Acceptance test (determinism + replay)
Bundle A acceptance requires a two-run replay test with frozen inputs:
- run twice for same day with identical input pointers
- prove byte-identical outputs via sha256 tree diff

Bundle A must output a run certificate (PASS/FAIL) listing:
- required inputs discovered + sha256
- produced outputs + sha256
- verdict + reason codes if fail

## 7) Rejection conditions (auto-fail)
- Any bypass path exists.
- Any output depends on nondeterministic ordering.
- Any missing proof of schema validation.
- Any output references a different repo root or truth root than this repo.
