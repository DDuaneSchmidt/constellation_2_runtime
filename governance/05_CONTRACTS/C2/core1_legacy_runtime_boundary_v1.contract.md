# core1_legacy_runtime_boundary_v1.contract

owner: core1_legacy_runtime_boundary_v1
scope: hardened Core 1 compatibility boundary

Compatibility mode:
- HARD_CUT_HARDENED_ONLY

Rules:
- Pre-hardening Core 1 runtime days are non-consumable by the hardened Core 1/Core 2 path.
- Translation of legacy rows is not supported.
- Mixed legacy and hardened raw-journal rows are forbidden.
- Any runtime day whose raw journal fails the hardened broker_raw_evidence_envelope.v1 schema must fail closed for Core 2 dependency use.

Boundary statuses:
- HARDENED_RUNTIME_DAY: all raw rows validate against the hardened raw envelope schema.
- LEGACY_RUNTIME_DAY_NON_CONSUMABLE: all raw rows fail the hardened raw envelope schema.
- MIXED_RUNTIME_DAY_BLOCKED: some rows validate and some do not.
- RAW_JOURNAL_MISSING: required hardened raw journal is absent.
