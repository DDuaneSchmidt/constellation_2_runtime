# sleeve_governance_policy_contract.md

Ownership:
- Sleeve Governance is the sole owner of sleeve policy.

Inputs:
- governed sleeve registry
- governed capital authority policy manifest

Outputs:
- canonical sleeve-governance interpretation for each economic sleeve id

Invariants:
- the canonical economic sleeve id is the `C2_*` sleeve id carried by the capital authority policy
- execution topology scope such as `PRIMARY` must not replace economic sleeve identity
- engine to economic sleeve mapping must be deterministic and lossless

Failure model:
- fail closed if engine-to-sleeve mapping is missing, ambiguous, or contradictory
- fail closed if a runtime surface depends on bare execution scope without engine lineage

Must-not-change rules:
- this contract does not rename historical truth
- this contract does not modify execution topology bindings
- this contract does not allocate budget

Audit requirements:
- preserve explicit engine-to-sleeve mapping
- preserve any execution-scope-to-economic-sleeve adapter assumptions
- document all identity translations used by downstream consumers
