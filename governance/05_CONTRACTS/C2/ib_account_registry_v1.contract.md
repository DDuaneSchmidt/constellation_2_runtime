---
doc_kind: contract
doc_id: C2_IB_ACCOUNT_REGISTRY_CONTRACT_V1
title: "Constellation 2.0 — IB Account Registry v1 (Governed Account Allowlist)"
version: 1
status: DRAFT
created_utc: 2026-02-28T00:00:00Z
repo_root_authoritative: /home/node/constellation_2_runtime
canonical_truth_root: /home/node/constellation_2_runtime/constellation_2/runtime/truth
scope:
  - "Defines the governed allowlist of Interactive Brokers account IDs usable by Constellation 2.0."
  - "Provides account-level safety defaults (fail-closed) and required enforcement semantics."
non_goals:
  - "Does not define per-engine sizing or strategy parameters."
  - "Does not authorize live trading unless explicitly enabled by governance."
---

# 1) Registry location (authoritative)

The IB account allowlist is defined ONLY in:

- `governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json`

# 2) Required fields

Each entry MUST include:

- `account_id` (string)
- `environment` in `{PAPER, LIVE}`
- `enabled_for_submission` (boolean)
- `allowed_engine_ids` (list of engine_id strings)
- `notes` (list of strings)

# 3) Fail-closed defaults

Unless governance explicitly enables an account:

- `enabled_for_submission` MUST be `false`
- `allowed_engine_ids` MUST be an empty list

# 4) Mandatory enforcement at submission boundary (Phase D)

Any code path that can submit an order to IB MUST enforce:

1) `ib_account` MUST exist in the registry.
2) `enabled_for_submission` MUST be `true` for that account.
3) If execution mode is PAPER:
   - `environment` MUST be `PAPER`
   - `account_id` MUST start with `DU`
4) The engine attempting to submit MUST be present in `allowed_engine_ids`.

If any check fails, submission MUST FAIL CLOSED and emit a veto artifact (no broker call).

# 5) No hard-coded account IDs

Systemd units, scripts, or tools MUST NOT hard-code IB account IDs as literals for regular operation.

Account selection MUST be:
- provided explicitly as a CLI argument AND validated against the registry, OR
- resolved from a governed config that is itself validated against the registry.

# 6) Audit posture

All submissions MUST be attributable to:
- the governed registry version (by sha256 / git sha),
- the selected `account_id`,
- and the authorizing engine id.

Any submission that cannot prove those bindings is invalid.
