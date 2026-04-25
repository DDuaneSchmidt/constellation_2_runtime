# paper_trading_integration_v1.contract.md

Contract owner:
- `ops/tools/run_paper_trading_integration_v1.py`

Purpose:
- give operators one canonical proof lane from bootstrap through same-day paper submit visibility
- fail honestly when canonical lifecycle closure, broker-status advancement, or downstream canonical propagation are still unproven

Source of truth:
- canonical runtime ledger remains the append-only lifecycle truth for the touched path:
  - `truth/runtime_ledger_v1/<DAY_UTC>/canonical_runtime_ledger.v1.jsonl`
- canonical report path:
  - `truth/reports/paper_trading_integration_v1/<DAY_UTC>/paper_trading_integration.v1.json`
- immutable versioned snapshots SHOULD also be written at:
  - `truth/reports/paper_trading_integration_v1/<DAY_UTC>/<SHA256>/paper_trading_integration.v1.json`
- when the flat alias already exists with different bytes, the owner SHOULD preserve the immutable versioned snapshot and surface the alias as stale rather than overwriting history silently

Required inputs:
- canonical PAPER bootstrap report
- canonical runtime ledger
- sleeve-local paper smoke report and request ref
- sleeve-local broker submission evidence
- canonical lifecycle closure report
- canonical execution reconciliation report
- downstream propagation state embedded in canonical lifecycle closure

PASS criteria:
- broker submission record exists
- submission identity links `submission_id`, `order_id`, and `perm_id`
- canonical lifecycle closure exists
- canonical reconciliation for the touched day does not claim `NO_SUBMISSIONS_FOUND`
- at least one of:
  - broker status advancement observed
  - fill observed
  - explicit terminal broker state observed
- if fill is observed, downstream canonical propagation MUST be complete or the report MUST fail honestly

Fail-closed rules:
- any missing required link in the chain MUST produce `integration_verdict = FAIL`
- the integration report MUST append `INTEGRATION_PASS` or `INTEGRATION_FAIL` to the canonical runtime ledger while remaining a non-authoritative projection
- the report MUST surface a precise blocker chain instead of inferring success from a single sleeve-local submit artifact
- the report MUST surface explicit verdict basis booleans so an operator can see exactly why the proof lane passed or failed
- the report MUST surface downstream propagation state from canonical lifecycle truth; it MUST NOT infer portfolio propagation from submit evidence alone
- the report MUST surface broker observation state, fill observation state, observation basis, and chosen observation refs from canonical lifecycle truth
- when both a versioned reconciliation snapshot and a flat alias exist, the versioned snapshot MUST be the preferred canonical reconciliation ref and the alias MUST be surfaced separately as best-effort only

Non-goals:
- this contract does NOT submit orders
- this contract does NOT mutate execution evidence
- this contract does NOT redefine paper bootstrap admission

Operator intent:
- distinguish “submit worked once” from “same-day canonical lifecycle is observably integrated”
