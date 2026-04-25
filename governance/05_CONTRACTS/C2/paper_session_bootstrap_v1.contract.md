# paper_session_bootstrap_v1.contract.md

Contract owner:
- `ops/tools/run_paper_session_bootstrap_v1.py`

Purpose:
- provide one first-class PAPER session bootstrap owner flow
- freeze governed bootstrap inputs
- materialize same-day PAPER startup prerequisites before evaluating safety or readiness
- evaluate safety and readiness only after startup materialization is explicit
- emit one operator-grade canonical bootstrap report for a session day
- separate required PAPER readiness from advisory PAPER surfaces and production-only gaps

Scope:
- PAPER environment only
- canonical control-state touched by PAPER startup:
  - canonical kill switch
  - canonical target-day admission
  - canonical authority pointer head
  - canonical bootstrap report
- sleeve-local startup artifacts that PAPER bootstrap depends on:
  - positions snapshot
  - cash ledger snapshot
  - accounting NAV
  - allocation summary
  - capital risk envelope
  - capital authority allocation
  - authorization gate verdict
  - day activation readiness/package

Source of truth:
- canonical runtime truth for the touched session lifecycle is append-only:
  - `truth/runtime_ledger_v1/<DAY_UTC>/canonical_runtime_ledger.v1.jsonl`
- canonical bootstrap report is the operator projection:
  - `truth/reports/paper_session_bootstrap_v1/<DAY_UTC>/paper_session_bootstrap.v1.json`
- shared control-state MUST be canonical-first
- sleeve copies used in the touched control path are deterministic projections only, never co-equal authority

Subordinate day-readiness automation projection:
- `ops/tools/run_tomorrow_paper_startup_prep_v1.py` may invoke refreshable prerequisite owners and rerun bootstrap to emit `day_readiness_automation_v1`
- `day_readiness_automation_v1` is an operator and automation projection only
- bootstrap, `pre_open_bundle_v1`, `session_promotion_decision_v1`, and Session Authority remain canonical startup truth

Required PAPER bootstrap prerequisites:
- governed paper capital seed exists or is materialized through:
  - `ops/tools/ensure_paper_capital_seed_v1.py`
- governed operator statement exists or is materialized through:
  - `ops/tools/ensure_cash_ledger_operator_statement_v1.py --mode GOVERNED_SEED`
- canonical kill switch is materialized and any sleeve projection used by PAPER execution is byte-identical to canonical
- canonical kill switch state MUST be `INACTIVE`
- cash ledger snapshot exists
- accounting NAV exists
- capital risk envelope exists and `status == PASS`
- capital authority allocation artifact exists
- authorization readiness verdict exists and is `PASS` or `BOOTSTRAP_PASS`
- same-day IB handshake prerequisite truth must either be ready or explicitly blocked through `pre_open_bundle_v1`
- same-day primary scoped canonical authority head must either be day-aligned or explicitly blocked through `pre_open_bundle_v1`
- target-day admission exists and is `ADMIT`
- day activation readiness is `COMPLETE`

PAPER bootstrap non-prerequisites:
- PAPER bootstrap MAY proceed without production-only external readiness surfaces such as:
  - full IB handshake admission dependency chain
  - replay certification bundle completeness
  - external feed attestation dependency completeness
- target-day build closure MAY remain open for PAPER bootstrap
- hidden production dependency checks MAY remain unmet for PAPER bootstrap
- this exception applies only through the governed PAPER bootstrap admission path
- it MUST NOT weaken non-PAPER admission strictness

PAPER advisory surfaces:
- canonical authority head refresh freshness for the target day
- startup materialization presence and reportability
- these MUST be surfaced explicitly and MUST NOT silently downgrade required PAPER readiness

Bootstrap outputs:
- canonical runtime ledger session events:
  - `BOOTSTRAP_STARTED`
  - `STARTUP_MATERIALIZATION_STARTED`
  - `STARTUP_AUTH_CONVERGENCE_COMPLETE` or `STARTUP_AUTH_CONVERGENCE_BLOCKED`
  - `STARTUP_GATE_ARTIFACTS_READY` or `STARTUP_GATE_ARTIFACTS_BLOCKED`
  - `STARTUP_MATERIALIZATION_COMPLETE` or `STARTUP_MATERIALIZATION_BLOCKED`
  - `KILL_SWITCH_EVALUATED_INACTIVE` or `KILL_SWITCH_EVALUATED_ACTIVE`
  - `ADMISSION_GRANTED` or `ADMISSION_BLOCKED`
  - `ACTIVATION_READY` or `ACTIVATION_BLOCKED`
  - `BOOTSTRAP_READY` or `BOOTSTRAP_BLOCKED`
- canonical bootstrap report:
  - `truth/reports/paper_session_bootstrap_v1/<DAY_UTC>/paper_session_bootstrap.v1.json`
- the report MUST include:
  - startup materialization phase result
  - evaluation phase result
  - activation phase result
  - root blocker class
  - mode used
  - admission basis
  - frozen input refs
  - frozen input manifest sha256
  - owner run id
  - materialized/reused/skipped steps
  - canonical kill switch ref
  - sleeve projection sync status for shared control-state
  - cash ledger snapshot ref
  - NAV ref
  - capital risk envelope ref
  - allocation readiness ref
  - authorization readiness ref
  - admission ref
  - pointer refresh result
  - day activation readiness result
  - required PAPER prerequisite status
  - PAPER advisory prerequisite status
  - production-only prerequisite status
  - explicit runtime prerequisite verification with ordered owned prerequisites
  - earliest failing prerequisite id, owner tool, artifact path, blocker class, and action summary
  - fix-first-then-rerun rule
  - do-not-run-manually deeper-stage list
  - bootstrap semantic status
  - smoke-submit-allowed decision
  - blocker chain
  - runtime-ledger projection provenance
  - operator retry vs escalation guidance
  - producer metadata

Execution-plane boundary:
- this contract does NOT own execution intent generation, submit boundary, broker adapter, evidence write, or reconciliation
- execution consumers MAY read the bootstrap report, but execution ownership remains with the existing execution plane

Fail-closed rules:
- environment MUST equal `PAPER`
- session lifecycle truth MUST append to canonical runtime ledger only
- bootstrap report MUST be written to canonical truth only
- if canonical truth root is not the decision root, the tool MUST fail closed
- if shared control-state canonical/sleeve projection diverges, bootstrap MUST remain blocked
- no kill-switch evaluation or readiness decision may happen before same-day owned startup prerequisites have been materialized or explicitly blocked
- if the canonical kill switch is not `INACTIVE`, bootstrap MUST remain blocked
- if required PAPER bootstrap prerequisites are missing or non-passing, bootstrap MUST remain blocked
- bootstrap MUST emit one explicit runtime prerequisite verification projection before deeper startup progression is allowed
- that projection MUST identify the earliest failing owned prerequisite and the canonical owner tool responsible for the next fix
- when bootstrap is blocked, operator guidance MUST instruct: fix the earliest failing prerequisite, then rerun the canonical entrypoint
- blocked bootstrap output MUST identify what deeper tools must not be run manually
- if day activation is not ready, `smoke_submit_allowed` MUST be false
- `bootstrap_status = READY` MUST mean all required PAPER prerequisites passed
- unmet advisory or production-only checks MUST be explicit and separately typed; they MUST NOT be folded into a generic READY verdict without annotation

Invariants:
- canonical runtime ledger is the primary append-only lifecycle truth for the touched bootstrap flow
- canonical truth remains the only authority for shared control-state in the touched path
- sleeve/local views remain projections only
- immutable runtime truth semantics remain intact
- duplicate protection for non-smoke execution paths remains unchanged
- submit-boundary controls remain unchanged
- production-mode admission strictness remains unchanged
- PAPER smoke path remains PAPER-only and TEST_ONLY

Operator intent:
- give operators one authoritative PAPER startup report instead of spreading ownership across indirect readiness chains
- make it explicit what was frozen, what was materialized, what was reused, and what still blocks session bootstrap
- make the earliest failing owned runtime prerequisite explicit so operators do not need to reconstruct internal producer order from blocker chains
- make retryable blockers and escalation-only blockers explicit
- make `READY` readable as a PAPER-runtime fact, not as a hidden proxy for production completeness
