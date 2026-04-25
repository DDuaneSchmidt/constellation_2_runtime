---
id: C2_PAPER_STARTUP_ADMISSION_AUTHORIZATION_BINDING_V1
title: "Paper Startup Admission Authorization Binding V1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Paper Startup Admission Authorization Binding V1

Rules:
- PAPER startup admission binds to the lifecycle-aware authorization surface, not the legacy blended gate stack
- Session Authority target-day construction must require:
  - `paper_startup_intent_input_convergence_v1`
  - `paper_startup_authorization_convergence_v1`
  - `primary_scoped_authorization_gate_verdict_v1`
  - `startup_materialization_input_convergence_v1`
  - `startup_materialization_v1`
  - `paper_trading_posture_v1`
  - `capability_state_v1`
  - `paper_policy_verdict_v1`
  - `trade_submit_readiness_c2_v1`
  - `submit_boundary_status_v1`
  - `paper_session_ledger_v1`
  - `startup_proof_validation_v1`
  - `trading_day_state_machine_v1`
- `primary_scoped_gate_stack_verdict_v1` may remain available for backward-compatible diagnostics, but it is not the PAPER startup admission authority
- `paper_startup_authorization_convergence_v1` must own and verify `paper_startup_intent_input_convergence_v1` before invoking sleeve intent generation for startup authorization
- capability bundles classified as `paper_role=ADVISORY` remain visible in PAPER policy evidence but must not block initial PAPER startup admission
