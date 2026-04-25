# Day Activation Authority V1

## Purpose
Day Activation Authority is the narrow sealed control-plane layer above Global Context for a target trading day. It compiles already-governed day-entry control artifacts, validates day correctness and control coherence, emits day_activation_build.v1.json, and seals day_activation_package.v1.json only when closure is complete.

## Inputs
- target_day_admission_v1
- canonical_authority_head_v1
- authorization_gate_verdict_v1

## Outputs
- truth/reports/day_activation_build_v1/<DAY>/<context_hash>/day_activation_build.v1.json
- truth_sleeves/<SLEEVE>/<MODE>/day_activation_package_v1/<DAY>/<context_hash>/day_activation_package.v1.json

## Ownership Boundary
Day Activation Authority may evaluate and seal day-global and sleeve-scoped control truth. It may not invent, override, or downgrade the underlying truth-owner artifacts.

## Seal Rules
- no package unless closure is COMPLETE
- package must record dependency refs and hashes for target-day admission, canonical authority head, and authorization verdict
- same day/sleeve/mode/account/operation plus same upstream truths must reproduce the same package hash

## Downstream Consumption
- Global Context Authority consumes day_activation_package_v1 in the primary path
- Economic, Execution, and Submit do not reopen these raw control nodes in their primary paths
