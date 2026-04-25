---
doc_kind: contract
doc_id: C2_TRADE_SUBMIT_READINESS_C2_CONTRACT_V1
title: "Constellation 2.0 — Trade Submit Readiness (C2-native) v1"
version: 1
status: DRAFT
created_utc: 2026-02-28T00:00:00Z
repo_root_authoritative: /home/node/constellation
canonical_execution_root_owner: sleeve_execution_root_v1
scope:
  - "Defines the C2-native readiness artifact required before Phase D broker submission."
  - "Binds readiness provenance.truth_root to the canonical sleeve-scoped execution root."
non_goals:
  - "Does not replace gate_stack_verdict or kill switch enforcement (those remain mandatory in Phase D)."
  - "Does not attempt to infer broker readiness from external systemctl state."
---

# 1) Authoritative outputs (C2 execution root)

This contract introduces a C2-native readiness spine written ONLY under:

- `truth_sleeves/<sleeve_id>/<mode>/trade_submit_readiness_c2_v1/<mode>/<ib_account>/status.json`
- `truth_sleeves/<sleeve_id>/<mode>/trade_submit_readiness_c2_v1/<mode>/<ib_account>/latest_pointer.v1.json`

Operator convenience only:

- the writer MAY emit `trade_submit_readiness_c2_v1/status.json` and `latest_pointer.v1.json` as current-day aliases
- these aliases are non-authoritative conveniences and MUST NOT be the Phase D lookup target

# 2) Governed schemas

These outputs MUST validate against:

- `governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json`
- `governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.latest_pointer.v1.schema.json`

# 3) Required evidence input

The readiness tool MUST consume the C2 handshake spine:

- `truth_sleeves/<sleeve_id>/<mode>/ib_api_handshake/latest_pointer.v1.json`
- and the referenced day artifact `ib_api_handshake.v1.json`

Readiness MUST be FAIL (fail-closed) if:
- the handshake pointer is missing, or
- the referenced handshake artifact is missing, or
- the handshake artifact does not prove an OK/PASS connection state.

# 4) Registry binding (mandatory)

The readiness tool MUST bind the IB account to governed configuration:

- `governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json`

The readiness tool MUST record `provenance.registry_sha256` as the sha256 of that registry file.

# 5) Anti-leak guarantee (mandatory)

The readiness status MUST include:

- `provenance.truth_root == /home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/<mode>`

Any readiness artifact whose provenance.truth_root differs is NON-AUTHORITATIVE and MUST NOT be accepted by Phase D submission logic.

# 6) Phase D mandatory enforcement

Phase D submission boundary MUST fail closed (no broker call) unless:

- C2-native readiness file exists at `trade_submit_readiness_c2_v1/<mode>/<ib_account>/status.json`,
- `ok == true` and `state == "OK"`,
- `environment == "PAPER"` for paper mode,
- `ib_account` matches the submission ib_account,
- and `provenance.truth_root` equals the canonical sleeve execution root.

# 6a) Execution-root precedence

- `sleeve_execution_root_v1` is the single canonical owner for readiness output root selection.
- older global/shared readiness roots are legacy only and MUST NOT be used for active submission authorization.

# 7) Determinism

The readiness writer must be deterministic for a given `--day_utc` and `--ib_account`:

- `as_of_utc` and `expires_utc` are deterministic day-anchored UTC timestamps.
- Output JSON must be stable (sorted keys, consistent formatting) as written by the tool.
