# Day Activation Package V1

## Purpose
Day_activation_package_v1 is the sealed target-day control-plane package consumed by Global Context Authority.

## Required Fields
- day_utc
- sleeve_id
- mode
- account_id
- operation_type
- context_hash
- package_hash
- target_day_admission_ref
- canonical_authority_head_ref
- authorization_gate_verdict_ref
- build_ref
- manifest_ref
- dependency_refs
- sealed=true
- sealed_utc

## Rules
- no package unless day activation closure is complete
- all refs must point at target-day-correct artifacts
- package hash must be reproducible from the same manifest, context, and dependency hashes

## Consumers
- primary consumer: global_context_authority_v1
- non-consumers in primary path: Economic, Execution, and Submit reopen only downstream sealed packages, not raw day-activation nodes
