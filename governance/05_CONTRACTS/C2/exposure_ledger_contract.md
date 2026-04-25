# exposure_ledger_contract.md

Ownership:
- Exposure Ledger is the sole owner of actual position and exposure truth for this architecture slice.

Inputs:
- positions snapshot
- fill ledger
- intent authorization ledger

Outputs:
- one `exposure_ledger.v1` record per position identity

Invariants:
- exposure must be derived from fill-backed position truth
- bootstrap exposure projections must not replace actual exposure truth
- joins must use proven lineage keys: `binding_hash`, `intent_hash`, `intent_id`, and `engine_id`

Failure model:
- fail closed if position truth or fill truth is missing
- fail closed if authorization linkage cannot be proven
- fail closed on contradictory lineage between position, fill, and authorization records

Must-not-change rules:
- this contract does not depend on `exposure_net_v1` as authority
- this contract does not alter positions snapshots
- this contract does not alter execution truth

Audit requirements:
- preserve refs to position snapshot, fill ledger, and authorization ledger artifacts
- preserve canonical sleeve identity used for attribution
- preserve reason codes for linkage failures and mismatch conditions
