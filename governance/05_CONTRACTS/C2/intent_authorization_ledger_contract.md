# intent_authorization_ledger_contract.md

Ownership:
- Intent Authorization Ledger is the sole owner of authorization reservation state.

Inputs:
- governed authorization artifacts
- sleeve allocation epoch
- canonical engine-to-sleeve mapping

Outputs:
- append-only `intent_authorization_ledger.v1` events keyed by intent hash

Invariants:
- the ledger may spend only from assigned sleeve budget
- the ledger may not create budget
- every record must bind to one allocation epoch id and one canonical sleeve id
- valid states are `REQUESTED`, `AUTHORIZED`, `RESERVED`, `COMMITTED`, `RELEASED`, and `REJECTED`

Failure model:
- fail closed if authorization lineage is incomplete
- fail closed if an event would overspend remaining sleeve budget
- fail closed if idempotency cannot be proven

Must-not-change rules:
- this contract does not replace existing authorization artifacts in Phase 1
- this contract does not change submit-boundary behavior
- this contract does not redefine allocation

Audit requirements:
- preserve intent hash, intent id, engine id, epoch id, and idempotency key
- preserve explicit reason codes for every reject, reserve, commit, and release transition
