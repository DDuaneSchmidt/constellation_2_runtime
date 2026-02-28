---
doc_kind: contract
doc_id: C2_BOOTSTRAP_INTENT_EMITTER_V1
title: "Constellation 2.0 — Bootstrap Intent Emitter v1 (Infrastructure Only)"
version: 1
status: DRAFT
created_utc: 2026-02-28T00:00:00Z
repo_root_authoritative: /home/node/constellation_2_runtime
canonical_truth_root: /home/node/constellation_2_runtime/constellation_2/runtime/truth
scope:
  - "Infrastructure-only deterministic emitter for ExposureIntent v1"
  - "Used solely to bootstrap pipeline validation when engines emit zero intents for a day"
non_goals:
  - "No strategy logic changes"
  - "No discretionary risk overrides"
  - "No bypass of Bundle A authorization"
---

# 1) Purpose

This tool exists to produce at least one schema-valid intent for a specified day when all strategy engines emit zero intents, so that downstream pipeline components (Phase C, OMS decisions, Bundle A authorization, Phase D enforcement) can be validated end-to-end.

This tool does not claim alpha, does not claim trade readiness, and does not bypass any capital or systemic gates.

# 2) Output

Writes exactly one ExposureIntent v1 file into:

- `constellation_2/runtime/truth/intents_v1/snapshots/<DAY_UTC>/<INTENT_HASH>.exposure_intent.v1.json`

Where:
- `INTENT_HASH = sha256(bytes(canonical_json_bytes_v1(intent_obj) + "\n"))`

# 3) Determinism

Given the same CLI args, the tool MUST produce byte-identical output.

The tool MUST generate:
- `intent_id` deterministically as sha256 of a stable JSON seed containing day + all parameters.
- `canonical_json_hash` using canonical C2 rules (hash with self-hash field forced null).

# 4) Fail-closed

The tool MUST fail closed if:
- the output file already exists
- schema validation fails
- canonicalization fails
- the day format is invalid
- any numeric constraints are invalid

# 5) Audit marker

Because ExposureIntent v1 schema does not allow arbitrary metadata, the bootstrap nature MUST be encoded in `intent_id` prefix:

- `intent_id = "c2_bootstrap_" + <64-hex-seed-hash>`

# 6) Relationship to Bundle A

Bundle A authorization remains mandatory. A bootstrap intent may be rejected by Bundle A policy (expected until policy caps are deliberately set).
