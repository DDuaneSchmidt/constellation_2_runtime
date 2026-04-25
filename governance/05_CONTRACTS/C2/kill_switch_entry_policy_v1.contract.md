---
id: C2_KILL_SWITCH_ENTRY_POLICY_CONTRACT_V1
title: "C2 Kill Switch Entry Policy Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_entry_policy
---

# C2 Kill Switch Entry Policy Contract V1

## Purpose

This contract governs one concern only:

- who owns new-entry permission for governed PAPER execution
- which upstream artifact decides whether entries are allowed
- and how submit-boundary code must react when that authority is missing or not green

## Canonical owner

The canonical owner of governed PAPER new-entry permission is:

- `ops/tools/run_global_kill_switch_v1.py`

The kill-switch writer is authoritative only because it derives from the governed lifecycle-aware
authorization verdict for the active PAPER sleeve.

## Canonical upstream decision input

The canonical upstream decision input is:

- `truth_sleeves/<sleeve_id>/<mode>/reports/authorization_gate_verdict_v1/<DAY>/authorization_gate_verdict.v1.json`

For active governed PAPER execution:

- `sleeve_id = PRIMARY`
- `mode = PAPER`

## Canonical output

The canonical kill-switch artifact remains under canonical truth:

- `/home/node/constellation_runtime_data/truth/risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json`

Any sleeve-scoped `risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json` copy is non-authoritative.
If such a sleeve-scoped copy exists and contradicts the canonical artifact, the canonical kill-switch
materialization path must quarantine the contradictory sleeve artifact before admission/submit consumers run.

## Required policy

- `allow_entries = true` is permitted only when the governed authorization verdict is present and
  `status in {PASS, BOOTSTRAP_PASS}`.
- Missing or invalid authorization-verdict evidence must default to `state = ACTIVE`,
  `allow_entries = false`.
- Gates classified into `economic_health_gate_verdict_v1` must not block fresh PAPER entries
  unless governance explicitly reclassifies them into the authorization verdict.
- `allow_exits` may remain true while `allow_entries` is false.
- PAPER day admission does not imply entry permission.
- Submit boundary must fail closed whenever:
  - kill-switch artifact is missing
  - kill-switch artifact is malformed
  - `state != INACTIVE`
  - `allow_entries != true`
  - a contradictory sleeve-scoped kill-switch copy remains present

## Non-authoritative inputs

The following are not allowed to decide new-entry permission on their own:

- Session Authority admission artifacts
- submit-boundary authorization status alone
- Phase C preflight allow decisions
- operator intuition not expressed through governed gate artifacts

## Stable blocker codes

- `C2_KILL_SWITCH_ACTIVE`
- `C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS`
- `C2_KILL_SWITCH_INPUT_SCHEMA_INVALID`

## Proof basis

- `governance/05_CONTRACTS/C2/bundled_c_exposure_convergence_lifecycle_v1.contract.md`
- `governance/02_REGISTRIES/GATE_CLASSIFICATION_REGISTRY_V1.json`
- `governance/05_CONTRACTS/C2/lifecycle_state_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/session_readiness_refresh_v1.contract.md`
- `governance/05_CONTRACTS/C2/submit_boundary_status_v1.contract.md`
- `governance/05_CONTRACTS/C2/trading_day_state_machine_v1.contract.md`
- `governance/05_CONTRACTS/C2/paper_startup_authorization_convergence_v1.contract.md`
- `ops/tools/run_global_kill_switch_v1.py`
