# Day Activation Dependency Manifest Fresh Paper Entry V1

## Operation Type
fresh_paper_entry_v1

## Required Nodes
- target_day_admission_v1
- canonical_authority_head_v1
- authorization_gate_verdict_v1

## Stage Order
1. DAY_ACTIVATION
2. SEAL

## Rules
- every dependency node must declare owner, canonical path pattern, and ordering
- target_day_admission_v1 must admit and bind the target day
- canonical_authority_head_v1 must match the target day and be authoritative pass state
- authorization_gate_verdict_v1 must match the target day and pass
- unowned nodes must be surfaced explicitly as UNOWNED
- build artifact must show full blocker chain even when sealing fails
