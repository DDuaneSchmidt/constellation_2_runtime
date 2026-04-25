# runtime_ledger_v1.contract.md

Contract owner:
- `constellation_2/common/runtime_ledger_v1.py`

Purpose:
- provide one append-only canonical runtime ledger for touched-path Constellation runtime truth
- record canonical session lifecycle events and canonical execution lifecycle events as immutable JSONL entries
- make reports explicit projections over ledger-backed runtime truth instead of co-equal authority surfaces

Canonical path:
- `truth/runtime_ledger_v1/<DAY_UTC>/canonical_runtime_ledger.v1.jsonl`

In-scope writers:
- `ops/tools/run_paper_session_bootstrap_v1.py`
- `ops/tools/run_canonical_lifecycle_closure_day_v1.py`
- `ops/tools/run_paper_trading_integration_v1.py`
- `ops/tools/run_position_normalization_v1.py`
- `ops/tools/run_exit_decision_engine_v1.py`
- `ops/tools/run_exit_execution_request_v1.py`
- `ops/tools/run_trade_result_closure_v1.py`

Entry requirements:
- every line MUST validate against:
  - `governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_ledger.v1.schema.json`
- every line MUST include:
  - `day_utc`
  - `event_type`
  - `produced_utc`
  - `owner_plane`
  - `owner_tool`
  - `owner_run_id`
  - `run_id`
  - `session_id`
  - `submission_id`
  - `order_id`
  - `perm_id`
  - `payload_ref`
  - `payload_hash`
  - `identity_key`
  - `identity_tuple`
  - `event_payload_summary`
- `identity_key` plus `identity_tuple` MUST preserve canonical linkage for the touched session or execution fact

Append-only rules:
- the ledger file MUST be append-only
- existing lines MUST NOT be rewritten or deleted
- the helper MAY skip appending a semantically identical event that is already present
- projections MUST NOT mutate ledger history

Ownership rules:
- canonical control-plane bootstrap truth is recorded here first in the touched bootstrap flow
- startup materialization-plane truth is recorded here first in the touched bootstrap flow
- evaluation / safety-plane truth is recorded here first in the touched bootstrap flow
- execution-plane lifecycle truth is recorded here first in the touched post-submit lifecycle flow
- position-normalization truth is recorded here first before imported positions enter exit management
- exit-decision truth is recorded here first before any exit execution request is emitted
- trade-result closure truth is recorded here first when a position is fully closed
- projection-plane reports may append projection verdict events such as `INTEGRATION_PASS` or `INTEGRATION_FAIL`, but they remain non-authoritative readers of canonical runtime truth

Bootstrap-phase event semantics:
- control-plane envelope:
  - `BOOTSTRAP_STARTED`
  - `BOOTSTRAP_READY` or `BOOTSTRAP_BLOCKED`
- startup materialization plane:
  - `STARTUP_MATERIALIZATION_STARTED`
  - `STARTUP_AUTH_CONVERGENCE_COMPLETE` or `STARTUP_AUTH_CONVERGENCE_BLOCKED`
  - `STARTUP_GATE_ARTIFACTS_READY` or `STARTUP_GATE_ARTIFACTS_BLOCKED`
  - `STARTUP_MATERIALIZATION_COMPLETE` or `STARTUP_MATERIALIZATION_BLOCKED`
- evaluation / safety plane:
  - `KILL_SWITCH_EVALUATED_ACTIVE` or `KILL_SWITCH_EVALUATED_INACTIVE`
  - `ADMISSION_GRANTED` or `ADMISSION_BLOCKED`
  - `ACTIVATION_READY` or `ACTIVATION_BLOCKED`
- position management planes:
  - `POSITION_NORMALIZATION_RECORDED`
  - `EXIT_DECISION_RECORDED`
  - `EXIT_EXECUTION_REQUESTED`
  - `TRADE_RESULT_RECORDED`

Projection relationship:
- `paper_session_bootstrap_v1`
- `canonical_lifecycle_closure_v1`
- `execution_reconciliation_v1`
- `paper_trading_integration_v1`
- sleeve and local views

These surfaces are derived-only operator projections. They must expose ledger provenance explicitly and must not regain authority over lifecycle truth.

Fail-closed rules:
- a writer MUST NOT append fabricated advancement
- broker advancement MUST NOT be claimed without exact supporting evidence
- fill observation MUST NOT be claimed without exact supporting evidence
- propagation completion MUST NOT be claimed without exact supporting evidence
- production-mode admission strictness and submit-boundary controls remain unchanged

Non-goals:
- this contract does NOT redesign strategy logic
- this contract does NOT make sleeve truth authoritative
- this contract does NOT require a repo-wide migration of legacy reports in one change
