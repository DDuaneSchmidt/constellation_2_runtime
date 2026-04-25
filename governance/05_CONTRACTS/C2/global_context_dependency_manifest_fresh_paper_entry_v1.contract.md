# Global Context Dependency Manifest Fresh Paper Entry V1

## Operation Type
fresh_paper_entry_v1

## Required Nodes
- day_activation_package_v1

## Stage Order
1. DAY_ACTIVATION
2. GLOBAL_CONTEXT
3. SEAL

## Rules
- every dependency node must declare owner, canonical path pattern, and ordering
- day_activation_package_v1 must be sealed and context-matched
- unowned nodes must be surfaced explicitly as UNOWNED
- build artifact must show full blocker chain even when sealing fails
