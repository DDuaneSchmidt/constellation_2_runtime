# canonical_sleeve_identity_contract.md

Ownership:
- Canonical Sleeve Identity governs the single economic sleeve id used across allocation, authorization, exposure, and reconciliation.

Inputs:
- sleeve registry
- capital authority policy manifest
- engine lineage from plans, fills, and authorization artifacts

Outputs:
- deterministic canonical sleeve identity resolution

Invariants:
- the canonical sleeve id is the economic `C2_*` sleeve id
- `engine_id -> canonical_sleeve_id` must be deterministic and lossless
- execution topology scope such as `PRIMARY` must not be used alone as canonical economic sleeve identity
- any `PRIMARY` bridge must require engine lineage

Failure model:
- fail closed if engine lineage is missing
- fail closed if a runtime surface attempts to map `PRIMARY` directly to one economic sleeve without engine lineage
- fail closed if policy introduces ambiguous engine-to-sleeve bindings

Must-not-change rules:
- this contract does not rename historical `PRIMARY` execution records
- this contract does not change execution root ownership
- this contract does not change capital policy sleeves

Audit requirements:
- preserve explicit mapping table provenance from governance policy
- preserve all adapter assumptions that translate execution scope to economic sleeve identity
