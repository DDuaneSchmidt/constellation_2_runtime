
# Global Context Package V1

## Meaning
A sealed global-context package is the first-class upstream context artifact for a specific day/sleeve/mode/account/operation context.

## Required Fields
- `day_utc`
- `sleeve_id`
- `mode`
- `account_id`
- `operation_type`
- `context_hash`
- `dependency_refs[]`
- `build_ref`
- `manifest_ref`
- `package_hash`
- `sealed=true`
- `sealed_utc`

## Lineage
Each required dependency ref must include canonical path and hash. Downstream packages must pin this package by path/hash lineage.

## Failure Model
If closure is incomplete, this package must be absent. Consumers fail closed on missing or unsealed package.
