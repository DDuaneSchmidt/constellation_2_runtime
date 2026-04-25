---
id: C2_PAPER_STARTUP_AUTHORIZATION_CONVERGENCE_V1
title: "Paper Startup Authorization Convergence V1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Paper Startup Authorization Convergence V1

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/paper_startup_authorization_convergence_v1/<DAY>/paper_startup_authorization_convergence.v1.json`

Rules:
- this artifact is the owned PAPER startup convergence surface for the lifecycle-aware authorization graph
- canonical writer output must set `binding_classification = SUBSET_PROOF_ONLY`
- it must materialize the true pre-trade authorization chain before startup admission binds:
  - `bod_execution_environment_proof_v1`
  - `paper_startup_intent_input_convergence_v1`
  - `engine_daily_returns_v1`
  - `engine_correlation_matrix_v1`
  - `correlation_envelope_gate_v1`
  - `feed_attestation_gate_v1`
  - `heartbeat_gate_v1`
  - `replay_certification_gate_v1`
  - `authorization_gate_verdict_v1`
- `economic_health_gate_verdict_v1` and `gate_stack_verdict_v1` may remain visible as optional lifecycle-scoped context, but they do not control PAPER startup authorization
- `convergence_status=SUCCESS` requires `authorization_verdict_ready=true` and no blocker in the required authorization artifact inventory
- it must fail closed and emit blocker chain entries in causal order
