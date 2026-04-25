# sleeve_allocation_epoch_contract.md

Ownership:
- Sleeve Allocation Epoch is the sole owner of assigned sleeve budgets for an epoch.

Inputs:
- portfolio governance snapshot
- capital authority policy manifest
- sleeve registry
- correlation gate output
- current capital authority allocation output

Outputs:
- one immutable `sleeve_allocation_epoch.v1` record per epoch

Invariants:
- epochs are immutable
- new allocation state creates a new epoch rather than mutating prior epochs
- per-sleeve assigned budget must use canonical economic sleeve ids
- epoch records must retain policy and sleeve registry hashes

Failure model:
- fail closed if required upstream allocation or policy inputs are missing
- fail closed if per-sleeve budget totals cannot be derived deterministically

Must-not-change rules:
- this contract does not replace the existing allocation writer in Phase 1
- this contract does not authorize intents
- this contract does not commit broker activity

Audit requirements:
- preserve epoch id, source snapshot identity, and input manifest
- make per-sleeve assigned, used, and remaining budget replayable
