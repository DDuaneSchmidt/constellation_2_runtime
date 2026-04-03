# C2 Exposure to Options Intent Adapter V1

Status: DRAFT

Purpose:
- Define the only legal adapter path from `exposure_intent.v1` to `options_intent.v2`
  for `SHORT_VOL_DEFINED` intents in the options phaseC identity pipeline.

Authoritative inputs:
- `constellation_2/schemas/exposure_intent.v1.schema.json`
- `governance/02_REGISTRIES/C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json`
- `governance/04_DATA/SCHEMAS/C2/OPTIONS/exposure_to_options_intent_policy.v1.schema.json`

Authoritative outputs:
- `options_intent.v2.json` (ephemeral working artifact under temp run surface)
- `exposure_to_options_adapter_record.v1.json` (ephemeral working artifact under temp run surface)

Required order in options identity flow:
1. Validate exposure intent (`exposure_intent.v1`)
2. Validate adapter policy (`exposure_to_options_intent_policy.v1`)
3. Adapt to `options_intent.v2`
4. Validate adapted options intent (`options_intent.v2`)
5. Call PhaseA options mapper and PhaseC options preflight using the adapted options intent

Fail-closed invariants:
- If exposure schema is not exactly `schema_id=exposure_intent`, `schema_version=v1`: fail closed.
- If `exposure_type != SHORT_VOL_DEFINED`: fail closed.
- If `engine.engine_id` is not policy-allowlisted: fail closed.
- If exposure fields do not satisfy policy requirements for the engine: fail closed.
- If adapted output fails `options_intent.v2` validation: fail closed.
- No broker calls, no network calls, no alternate truth roots.

Determinism invariants:
- Adapter output must be deterministic for identical exposure intent bytes and policy bytes.
- `canonical_json_hash` must follow Constellation canonical self-hash convention.
- Adapter record must include sha256 lineage for input exposure intent, policy file, and output options intent.

Prohibitions:
- No implicit defaults not declared in governed policy.
- No conversion of non-`SHORT_VOL_DEFINED` intents.
- No direct pass-through of `exposure_intent.v1` into options mapper/preflight boundaries.

