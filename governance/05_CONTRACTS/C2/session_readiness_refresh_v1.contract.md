---
id: C2_SESSION_READINESS_REFRESH_V1
title: "Session Readiness Refresh V1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Session Readiness Refresh V1

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/session_readiness_refresh_v1/<DAY>/session_readiness_refresh.v1.json`

Rules:
- this tool remains a practical startup/readiness refresh owner and monitoring/report surface
- it must emit or refresh `day_authority_decision_v1` for the target day on both successful and blocked outcomes
- early-return blocked outcomes must still materialize `day_authority_decision_v1`; they must not leave Session Authority with a missing artifact when the true state is blocked
- it owns kill-switch materialization for the live startup/control-plane path by refreshing `global_kill_switch_state.v1.json` for the target day before submit-boundary status is evaluated
- kill-switch materialization must quarantine any contradictory sleeve-scoped `risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json` residue before submit-boundary status is evaluated; the canonical truth artifact remains the sole authority
- it owns PRIMARY/PAPER authority day rollover by materializing `run_pointer_v1/*` and `run_pointer_v2/canonical_authority_head.v1.json` under both canonical truth and the PRIMARY/PAPER sleeve execution truth, while the pointed authorization verdict remains sleeve-scoped PRIMARY/PAPER truth
- it owns PRIMARY/PAPER sleeve-scoped exposure-net, capital-allocation, and `engine_activity_v1/authorization_v1` materialization for the live pre-submit control-plane path, after scoped authority pointer refresh and before submit-boundary status is evaluated
- kill-switch materialization must consume the governed PRIMARY/PAPER sleeve-scoped `authorization_gate_verdict_v1` when evaluating live PAPER entry safety; it must not treat the legacy blended gate stack or economic-health verdict as the entry-authority artifact
- it must preserve the runtime-venv interpreter path for broker-events bootstrap and must not collapse the selected interpreter by resolving the venv python symlink target
- broker-events bootstrap must surface explicit reason codes when broker dependency import fails, including `READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED`
- the readiness refresh path must not silently mask bootstrap interpreter or `ibapi` import failures
- when PAPER startup is valid, readiness refresh may report monitoring gaps, but startup authority remains bound by Session Authority and the lifecycle-aware authorization model
