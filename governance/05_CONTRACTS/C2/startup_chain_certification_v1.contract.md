# startup_chain_certification_v1

This contract governs durable certification for the canonical startup chain.

Required certification inputs:
- boundary validation result
- day activation family validation result
- global context family validation result
- session authority family validation result
- execution build family validation result
- stage-admission coherence for the ordered chain

Required stage certification artifacts:
- `control_stage_day_certification_v1`
- `control_stage_context_certification_v1`
- `control_stage_session_certification_v1`
- `control_stage_execution_build_certification_v1`

Durable certification artifact:
- `startup_chain_certification_v1`

Success semantics:
- certification status is `CERTIFIED` only when:
  - boundary validator passes
  - every family validator passes
  - stage ordering is coherent
  - upstream admitted-stage requirements are satisfied in order

Failure semantics:
- certification status is `FAILED` when any required validator, stage, or coherence invariant fails
- failure MUST remain machine-readable and fail closed

Required evidence:
- exact validator outputs
- emitted stage-admission refs
- emitted stage-certification refs
- blocking reason taxonomy when certification fails

Operator/CI entrypoint law:
- one startup-chain certification CLI MAY run in validator-only mode or certification-emitting mode
- validator-only mode MUST NOT mutate truth
- certification-emitting mode MAY emit only the ratified stage-admission and certification artifacts in this contract family
