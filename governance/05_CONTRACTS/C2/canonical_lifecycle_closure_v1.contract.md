# canonical_lifecycle_closure_v1.contract.md

Contract owner:
- `ops/tools/run_canonical_lifecycle_closure_day_v1.py`

Purpose:
- make same-day sleeve-local broker submissions canonically visible
- give canonical truth one first-class lifecycle closure artifact keyed by `submission_id`, `order_id`, and `perm_id`
- prevent canonical reconciliation from claiming `NO_SUBMISSIONS_FOUND` after same-day submit evidence already exists in the touched sleeve path
- make post-submit lifecycle state explicit even when broker observation, fills, or downstream portfolio propagation are not yet complete

Scope:
- PAPER execution evidence propagation in the touched path only
- canonical execution evidence visibility for same-day broker submissions
- canonical lifecycle closure reports
- explicit canonical lifecycle and downstream propagation state in the touched path

Source of truth:
- canonical runtime ledger is the primary append-only lifecycle truth:
  - `truth/runtime_ledger_v1/<DAY_UTC>/canonical_runtime_ledger.v1.jsonl`
- canonical submission evidence remains under:
  - `truth/execution_evidence_v1/submissions/<DAY_UTC>/<submission_id>/`
- canonical lifecycle closure report is the operator projection that summarizes ledger-backed truth and canonical evidence:
  - `truth/reports/canonical_lifecycle_closure_v1/<DAY_UTC>/<submission_id>/canonical_lifecycle_closure.v1.json`

Owner behavior:
- the owner MUST consume same-day sleeve-local broker submission evidence
- it MUST materialize canonical submission visibility before declaring lifecycle closure
- it MUST append canonical runtime ledger events for the touched lifecycle truth before publishing the projection report
- it MUST key the closure by `submission_id`, `order_id`, and `perm_id`
- it MUST record exact canonical and sleeve refs plus hashes for the broker submission record and execution event record when present
- it MUST consume exact-match broker observation facts when available in the touched broker fact spine ledgers:
  - `observed_order_status_fact.v1.jsonl`
  - `observed_fill_fact.v1.jsonl`
- exact-match means `order_id` and `perm_id` match the lifecycle identity, `quality_status = OK`, and attribution is at least `ATTRIBUTED` or `PARTIAL`
- mismatched or low-quality broker facts MUST NOT advance lifecycle state
- the owner MUST record the chosen broker observation ref, fill observation ref, and observation basis so operators can see whether advancement came from execution-event truth, broker facts, or both
- it MUST record explicit lifecycle state, including whether broker status is still not observed, whether fills are still not observed, whether a terminal state is observed, and whether downstream propagation is pending or complete
- it MUST record canonical refs plus hashes for downstream touched artifacts when present:
  - `fill_ledger_v1/<DAY>/<submission_id>.fill_ledger.v1.json`
  - `positions_v1/snapshots/<DAY>/...`
  - `positions_v1/effective_v1/days/<DAY>/positions_effective_pointer.v1.json`
  - `accounting_v2/nav/<DAY>/nav.v2.json`

Required invariants:
- sleeve-local same-day broker submission evidence in the touched path implies a canonical lifecycle closure artifact
- sleeve-local same-day broker submission evidence in the touched path implies canonical runtime ledger lifecycle entries
- the canonical lifecycle closure artifact MUST match `submission_id`, `order_id`, and `perm_id`
- canonical reconciliation for the same day MUST NOT report `NO_SUBMISSIONS_FOUND` once canonical lifecycle closure exists
- reports MUST prefer immutable versioned refs and hashes over mutable flat aliases where those refs are available
- the canonical lifecycle closure artifact MUST NOT silently leave downstream propagation ambiguous; it MUST express `PROPAGATION_PENDING`, `PROPAGATION_COMPLETE`, or `PROPAGATION_BLOCKED`
- if no fill is yet observed, the lifecycle artifact MUST make that pending state explicit instead of implying portfolio propagation occurred
- if fill is observed but downstream canonical positions/accounting evidence is still incomplete, the lifecycle artifact MUST surface precise pending or blocked reasons instead of leaving empty downstream truth unexplained

Non-goals:
- this contract does NOT redesign execution intent generation
- this contract does NOT make sleeve truth authoritative
- this contract does NOT weaken submit-boundary controls or duplicate protection

Fail-closed rules:
- missing broker ids (`order_id`, `perm_id`) MUST block closure
- missing canonical submission evidence after propagation MUST block closure
- identity mismatch between sleeve evidence and canonical evidence MUST block closure
- the runtime ledger MUST NOT claim broker advancement, fill observation, or propagation completion without exact supporting evidence
- downstream propagation ambiguity MUST surface as explicit propagation state and blocker_chain entries; it MUST NOT be implied away by a generic `CANONICALIZED` label

Operator intent:
- let operators prove that a same-day paper submit is canonically visible without inferring from sleeve-local artifacts alone
