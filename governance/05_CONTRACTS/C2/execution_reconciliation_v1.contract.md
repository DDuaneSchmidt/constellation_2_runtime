# execution_reconciliation_v1.contract.md

Contract owner:
- `ops/tools/run_execution_reconciliation_day_v1.py`

Purpose:
- reconcile canonical execution evidence for a day without depending on mutable sleeve-local submit visibility
- ensure canonical lifecycle closure prevents false `NO_SUBMISSIONS_FOUND` outcomes in the touched path
- provide an operator-grade reconciliation artifact whose audit guidance prefers immutable versioned snapshots over flat aliases

Source of truth:
- canonical runtime ledger remains the primary append-only lifecycle truth for the touched path:
  - `truth/runtime_ledger_v1/<DAY_UTC>/canonical_runtime_ledger.v1.jsonl`
- canonical report path alias:
  - `truth/reports/execution_reconciliation_v1/<DAY_UTC>/execution_reconciliation.v1.json`
- immutable versioned snapshots:
  - `truth/reports/execution_reconciliation_v1/<DAY_UTC>/<SHA256>/execution_reconciliation.v1.json`

Required inputs:
- canonical runtime ledger day file
- canonical execution evidence submissions day directory
- canonical execution stream day directory
- canonical fill ledger day directory
- canonical lifecycle closure day directory

Owner behavior:
- the owner MUST emit a deterministic reconciliation report even when there are no submissions
- the report MUST expose explicit runtime-ledger provenance so operators can see that reconciliation is a projection over ledger-backed truth plus canonical day directories
- when `day_open_attempt_v1` already carries canonical runtime lifecycle provenance for the day, the report MAY carry the same narrow optional `runtime_lifecycle_ref` block as an audit linkage only
- the report MUST expose top-level `semantic_status` using `governance/05_CONTRACTS/C2/operator_semantic_taxonomy_v1.contract.md`
- once canonical lifecycle closure artifacts exist for the day, the owner MUST NOT report `NO_SUBMISSIONS_FOUND`
- the report MUST include input-manifest hashes for the touched canonical day roots
- the report MUST include audit guidance that tells operators to prefer immutable versioned snapshots and to treat the flat alias as best-effort only

Fail-closed rules:
- missing canonical submissions day dir MUST fail closed
- missing canonical fill ledger day dir MUST fail closed
- missing canonical execution-stream day dir, canonical lifecycle-closure day dir, or canonical runtime-ledger day file MUST prevent a success semantic classification
- canonical closure without canonical submission evidence MUST fail
- any internal inconsistency that would previously fail this owner MUST continue to fail

Non-goals:
- this contract does NOT observe broker status directly
- this contract does NOT mutate execution evidence
- this contract does NOT decide submit readiness

Operator intent:
- give operators one canonical reconciliation artifact that remains auditable even when the convenience alias goes stale
