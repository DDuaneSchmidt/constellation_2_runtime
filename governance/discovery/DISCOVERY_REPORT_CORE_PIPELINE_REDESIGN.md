# DISCOVERY REPORT: CORE PIPELINE REDESIGN

Status: Phase 0 only  
Repo root: `/home/node/constellation`  
Generated on: `2026-04-12`  
Scope: discovery and proof only; no runtime redesign applied

## 1. Repo Proof

Required commands were run against the authoritative repo only.

```text
$ test -d /home/node/constellation && echo EXISTS
EXISTS

$ git -C /home/node/constellation rev-parse --show-toplevel
/home/node/constellation

$ git -C /home/node/constellation rev-parse --abbrev-ref HEAD
feature/implement-economic-state-authority-v1

$ git -C /home/node/constellation remote -v
origin  git@github.com:DDuaneSchmidt/constellation_2_runtime.git (fetch)
origin  git@github.com:DDuaneSchmidt/constellation_2_runtime.git (push)
```

`git -C /home/node/constellation status --short` proved the worktree is **not clean**. The output is large; representative lines:

```text
 M constellation_2/common/capability_state_v1.py
 M constellation_2/common/day_authority_decision_v1.py
 M constellation_2/phaseD/lib/submit_boundary_paper_v4.py
 M constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py
 M constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py
 M ops/tools/run_c2_paper_day_orchestrator_v2.py
?? constellation_2/common/economic_state_authority_v1.py
?? ops/tools/run_broker_fact_spine_v1.py
?? ops/tools/run_orphan_submission_backfill_day_v1.py
?? governance/05_CONTRACTS/C2/execution_build_authority_v1.contract.md
```

Finding: the branch is dirty and already contains many in-progress authority-related edits. Any migration must avoid assuming a clean baseline.

## 2. Authoritative Repo and Truth-Root Proof

Repo authority is explicit in repo files:

- `repo_role.v1.json:5` sets `"repo_role": "authoritative_source"`
- `repo_role.v1.json:6` sets `"authoritative_repo_root": "/home/node/constellation"`
- `repo_role.v1.json:10` declares `"runtime_copy_roots"`
- `repo_role.v1.json:13` points at `governance/02_REGISTRIES/REPO_AUTHORITY_V1.json`
- `governance/02_REGISTRIES/REPO_AUTHORITY_V1.json:4` sets `"authoritative_repo_root": "/home/node/constellation"`
- `governance/02_REGISTRIES/REPO_AUTHORITY_V1.json:7` declares `"runtime_copy_roots"`

Current truth-root resolution is implemented in:

- `constellation_2/common/truth_root_v1.py:29` `_governance_truth_root(repo_root: Path)`
- `constellation_2/common/truth_root_v1.py:71` `resolve_runtime_root()`
- `constellation_2/common/truth_root_v1.py:101` `resolve_truth_root(*, repo_root: Path) -> Path`
- `constellation_2/common/truth_root_v1.py:115` `truth_subpath(repo_root: Path, *parts: str) -> Path`

Finding: the repo is authoritative, but runtime truth is file-backed and resolved through governed path logic rather than a database-first authority.

## 3. Path Inventory

Required directory scan:

```text
$ find /home/node/constellation -maxdepth 3 -type d | sort
```

Relevant roots proven by that scan:

- `/home/node/constellation/constellation_2/runtime/truth`
- `/home/node/constellation/constellation_2/runtime/truth_sleeves`
- `/home/node/constellation/constellation_2/phaseA`
- `/home/node/constellation/constellation_2/phaseB`
- `/home/node/constellation/constellation_2/phaseC`
- `/home/node/constellation/constellation_2/phaseD`
- `/home/node/constellation/constellation_2/phaseF`
- `/home/node/constellation/constellation_2/phaseG`
- `/home/node/constellation/constellation_2/phaseH`
- `/home/node/constellation/constellation_2/phaseI`
- `/home/node/constellation/constellation_2/phaseJ`
- `/home/node/constellation/constellation_2/phaseL/ui`
- `/home/node/constellation/constellation_2/phaseL/ui_api`
- `/home/node/constellation/governance/02_REGISTRIES`
- `/home/node/constellation/governance/03_CONTRACTS`
- `/home/node/constellation/governance/04_DATA/SCHEMAS`
- `/home/node/constellation/governance/05_CONTRACTS`
- `/home/node/constellation/governance/contracts`
- `/home/node/constellation/ops/tools`
- `/home/node/constellation/ops/systemd/user`

Finding: current authority is spread across phases, governance registries/contracts, file-backed truth paths, and operator tooling.

## 4. Required Search Proof

Required search run:

```text
$ rg -n "position|holding|order|execution|broker|snapshot|decision|projection|replay|audit|governance|contract|invariant" /home/node/constellation
```

Representative hits establishing the current shape:

- `constellation_2/phaseA/tools/c2_map_vertical_v1.py:118` `("options_chain_snapshot.v1", chain),`
- `constellation_2/phaseA/tools/c2_map_vertical_v1.py:134` `assert res.order_plan and res.mapping_ledger_record and res.binding_record`
- `governance/02_REGISTRIES/C2_EXECUTION_DEPENDENCY_REGISTRY_V1.json:10` `market_data_snapshot_for_real_intent_generation`
- `governance/02_REGISTRIES/C2_EXECUTION_DEPENDENCY_REGISTRY_V1.json:73` execution evidence expects `{broker_submission_record.v2.json,execution_event_record.v1.json,equity_order_plan.v1.json}`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/current_system_projection.v1.schema.json:40` `"schema_id": { "const": "current_system_projection" }`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/alerts_projection.v1.schema.json:26` `"schema_id": { "const": "alerts_projection" }`
- `constellation_2/governance/C2_AUDIT_EVIDENCE_CHAIN.md:15` immutable, replayable, audit-grade evidence chain
- `ops/tools/run_runtime_replay_day_v1.py:241` writes `replay_manifest.v1.json`
- `ops/tools/run_replay_integrity_day_v2.py:222` writes `reports/replay_integrity_v2/<DAY>/`

Finding: the repo already values determinism, evidence, and replay, but current business authority is not concentrated in a single 5-object chain.

## 5. Current Surfaces by Proposed Core Object

This section maps current repo surfaces to the proposed authoritative pipeline. Mapping means "best current analogue from repo evidence", not "already equivalent".

### 5.1 Snapshot-like Surfaces

Proven current snapshot-like inputs are fragmented:

- Market data
  - `constellation_2/phaseJ/tools/market_data_ingest_v1.py:32`
  - `constellation_2/phaseJ/tools/market_data_ingest_v1.py:293`
  - `constellation_2/phaseJ/tools/market_data_ingest_v1.py:419`
- Market calendar
  - `constellation_2/phaseJ/tools/market_calendar_ingest_v1.py:11`
  - `constellation_2/phaseJ/tools/market_calendar_ingest_v1.py:188`
  - `constellation_2/phaseJ/tools/market_calendar_ingest_v1.py:202`
- Historical broker market snapshot downloader
  - `constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py:9`
  - `constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py:298`
  - `constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py:508`
- Options-chain snapshot input
  - `constellation_2/phaseA/tools/c2_map_vertical_v1.py:118`
  - `constellation_2/phaseA/lib/map_vertical_spread_v1.py:145`
- Cash and position snapshot prerequisites
  - `ops/tools/run_startup_materialization_input_convergence_v1.py:237`
  - `ops/tools/run_paper_session_admission_v1.py:433`
  - `ops/tools/run_session_readiness_refresh_v1.py:277`

Finding: there is **no single canonical trading Snapshot object** proved in the repo today. Current "what the system knew" is spread across multiple input artifacts and gates.

### 5.2 PortfolioDecision-like Surfaces

Current decision-like surfaces are also fragmented:

- Engine intent writers
  - `constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py:24`
  - `constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py:292`
  - `constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py:27`
  - `constellation_2/phaseI/vol_income_defined_risk/run/run_vol_income_defined_risk_intents_day_v1.py:28`
  - `constellation_2/phaseI/event_dislocation/run/run_event_dislocation_intents_day_v1.py:23`
  - `constellation_2/phaseI/cross_asset_trend/run/run_cross_asset_trend_intents_day_v1.py:37`
  - `constellation_2/phaseI/market_neutral_spread/run/run_market_neutral_spread_intents_day_v1.py:37`
  - `constellation_2/phaseI/defensive_tail/run/run_defensive_tail_intents_day_v1.py:17`
- Allocation
  - `constellation_2/phaseG/allocation/run/run_allocation_day_v2.py:45`
  - `constellation_2/phaseG/allocation/run/run_allocation_day_v2.py:56`
  - `constellation_2/phaseG/allocation/run/run_allocation_day_v2.py:401`
- Authorization artifacts
  - `ops/tools/run_authorization_artifacts_day_v1.py:9`
  - `ops/tools/run_authorization_artifacts_day_v1.py:288`
  - `ops/tools/run_c2_paper_day_orchestrator_v2.py:1545`

Finding: current decision authority is split across engine intents, allocation outputs, and authorization artifacts. No single immutable `PortfolioDecision` record was proven.

### 5.3 ExecutionAction-like Surfaces

Current action-like surfaces:

- Preflight and identity set generation
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:17`
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:18`
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:19`
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:20`
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:116`
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:131`
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:147`
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:322`
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:350`
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:374`
- Phase C identity materializer
  - `ops/tools/run_phasec_identity_materializer_day_v1.py:103`
  - `ops/tools/run_phasec_identity_materializer_day_v1.py:105`
  - `ops/tools/run_phasec_identity_materializer_day_v1.py:709`
  - `ops/tools/run_phasec_identity_materializer_day_v1.py:724`
- Orchestrator coupling
  - `ops/tools/run_c2_paper_day_orchestrator_v2.py:979`
  - `ops/tools/run_c2_paper_day_orchestrator_v2.py:1333`

Finding: current action authority is expressed through order plans, mapping ledger records, and binding records. That is close to an execution-action family, but it is still a multi-artifact boundary rather than one authoritative object.

### 5.4 BrokerEvent-like Surfaces

Proven broker-event surfaces:

- Submit boundary writer
  - `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:9`
  - `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:1050`
  - `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:1105`
  - `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:1109`
  - `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:1136`
  - `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:1167`
- Orphan backfill writer
  - `ops/tools/run_orphan_submission_backfill_day_v1.py:16`
  - `ops/tools/run_orphan_submission_backfill_day_v1.py:17`
  - `ops/tools/run_orphan_submission_backfill_day_v1.py:231`
  - `ops/tools/run_orphan_submission_backfill_day_v1.py:311`
  - `ops/tools/run_orphan_submission_backfill_day_v1.py:313`
- Lifecycle refresh writer
  - `ops/tools/run_submission_lifecycle_refresh_v1.py:9`
  - `ops/tools/run_submission_lifecycle_refresh_v1.py:10`
  - `ops/tools/run_submission_lifecycle_refresh_v1.py:14`
  - `ops/tools/run_submission_lifecycle_refresh_v1.py:131`
  - `ops/tools/run_submission_lifecycle_refresh_v1.py:253`
  - `ops/tools/run_submission_lifecycle_refresh_v1.py:285`
- Derived execution stream and broker fact schemas
  - `ops/tools/run_fill_ledger_day_v1.py:410`
  - `governance/04_DATA/SCHEMAS/C2/FACTS/observed_order_fact.v1.schema.json:3`
  - `governance/04_DATA/SCHEMAS/C2/FACTS/observed_order_status_fact.v1.schema.json:3`
  - `governance/04_DATA/SCHEMAS/C2/FACTS/observed_position_fact.v1.schema.json:3`

Finding: current broker-event authority is **not single-writer**. At minimum, submit, orphan backfill, and lifecycle refresh all write or refresh broker-facing event artifacts.

### 5.5 PositionState-like Surfaces

Proven position surfaces:

- Position snapshots
  - `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py:22`
  - `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py:386`
  - `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py:18`
  - `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py:295`
  - `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py:375`
- Orchestrator invokes both v2 and v4
  - `ops/tools/run_c2_paper_day_orchestrator_v2.py:1995`
  - `ops/tools/run_c2_paper_day_orchestrator_v2.py:2535`
- Session admission and readiness also invoke positions v2
  - `ops/tools/run_paper_session_admission_v1.py:67`
  - `ops/tools/run_session_readiness_refresh_v1.py:1183`
- Position lifecycle and reconciliation surfaces
  - `ops/tools/run_position_lifecycle_snapshot_v2.py:190`
  - `ops/tools/run_position_lifecycle_snapshot_v2.py:282`
  - `ops/tools/run_broker_reconciliation_day_v2.py:318`
  - `ops/tools/run_reconciled_trade_state_v1.py:8`

Finding: current position truth is **not single-writer** and not obviously reduced from one canonical broker-event stream. Snapshot v2, snapshot v4, lifecycle, reconciliation, and reconciled-trade surfaces overlap.

## 6. Current Projections and Read Models

Current projection schemas:

- `governance/04_DATA/SCHEMAS/C2/REPORTS/current_system_projection.v1.schema.json:3`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/current_system_projection.v1.schema.json:40`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/alerts_projection.v1.schema.json:3`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/alerts_projection.v1.schema.json:26`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/performance_projection.v1.schema.json:3`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/performance_projection.v1.schema.json:24`

Current projection writers:

- `ops/tools/run_current_system_projection_v1.py:15`
- `ops/tools/run_current_system_projection_v1.py:215`
- `ops/tools/run_current_system_projection_v1.py:227`
- `ops/tools/run_alerts_projection_v1.py:14`
- `ops/tools/run_alerts_projection_v1.py:43`
- `ops/tools/run_alerts_projection_v1.py:55`
- `ops/tools/run_performance_projection_v1.py:16`
- `ops/tools/run_performance_projection_v1.py:41`
- `ops/tools/run_strategy_policy_projection_v1.py:13`
- `ops/tools/run_strategy_policy_projection_v1.py:26`

Current UI/read-model readers:

- `constellation_2/phaseL/ui_api/__init__.py:2`
- `constellation_2/phaseL/ui_api/__init__.py:10`
- `constellation_2/phaseL/ui_api/alerts_read_model.py:14`
- `constellation_2/phaseL/ui_api/alerts_read_model.py:83`
- `constellation_2/phaseL/ui_api/system_summary_read_model.py:19`
- `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py:1508`
- `constellation_2/phaseL/ui/static/operator_shell/main.js:4`
- `constellation_2/phaseL/ui/static/operator_shell/main.js:10`

Finding: the repo already has a clear projection/read-model layer. This is compatible with the redesign requirement that projections remain disposable and non-authoritative.

## 7. Current Replay / Audit / Rebuild Surfaces

Replay and audit evidence:

- `constellation_2/governance/C2_AUDIT_EVIDENCE_CHAIN.md:15`
- `constellation_2/governance/C2_AUDIT_EVIDENCE_CHAIN.md:102`
- `constellation_2/common/canonical_fact_store_v1.py:42`
- `constellation_2/common/canonical_fact_store_v1.py:191`
- `ops/tools/run_runtime_replay_day_v1.py:31`
- `ops/tools/run_runtime_replay_day_v1.py:188`
- `ops/tools/run_runtime_replay_day_v1.py:241`
- `ops/tools/run_replay_certification_gate_v1.py:7`
- `ops/tools/run_replay_certification_gate_v1.py:136`
- `ops/tools/run_replay_certification_gate_v1.py:236`
- `ops/tools/run_replay_integrity_day_v2.py:7`
- `ops/tools/run_replay_integrity_day_v2.py:222`
- `ops/tools/run_replay_integrity_day_v2.py:297`

Finding: replayability and audit lineage are established design goals in the repo and materially support a promotion-based authoritative core.

## 8. Current Writers and Readers

### 8.1 Current Writers

Most relevant proven writers for authority-bearing trading artifacts:

- Snapshot-like inputs:
  - `constellation_2/phaseJ/tools/market_data_ingest_v1.py`
  - `constellation_2/phaseJ/tools/market_calendar_ingest_v1.py`
  - `ops/tools/run_options_chain_truth_promotion_day_v1.py`
  - `constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py` via dependency registry reference at `governance/02_REGISTRIES/C2_EXECUTION_DEPENDENCY_REGISTRY_V1.json:29`
- Decision-like:
  - phase I intent writers listed above
  - `constellation_2/phaseG/allocation/run/run_allocation_day_v2.py`
  - `ops/tools/run_authorization_artifacts_day_v1.py`
- Action-like:
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py`
  - `ops/tools/run_phasec_identity_materializer_day_v1.py`
- Broker-event-like:
  - `constellation_2/phaseD/lib/submit_boundary_paper_v4.py`
  - `ops/tools/run_orphan_submission_backfill_day_v1.py`
  - `ops/tools/run_submission_lifecycle_refresh_v1.py`
- Position-like:
  - `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py`
  - `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py`
  - `ops/tools/run_position_lifecycle_snapshot_v2.py`
  - `ops/tools/run_reconciled_trade_state_v1.py`
  - `ops/tools/run_broker_reconciliation_day_v2.py`

### 8.2 Current Readers

Most relevant proven readers:

- `ops/tools/run_c2_paper_day_orchestrator_v2.py`
- `constellation_2/phaseL/ui_api/*.py`
- `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`
- projection writers that read prior projections, especially `ops/tools/run_alerts_projection_v1.py:43-50`
- replay tooling in `ops/tools/run_runtime_replay_day_v1.py`

Finding: current readers are numerous, but the sharper risk is writers. Broker-event and position-state writers are already plural.

## 9. Identified Ambiguity and Split-Brain Risks

### 9.1 Snapshot Ambiguity

No single repo-proven trading snapshot object currently freezes all inputs for one promotion cycle. Market data, calendar, options chain, cash, positions, and readiness artifacts are separate.

Risk: replaying "what the system knew" requires reassembling multiple artifact families rather than reading one canonical snapshot.

### 9.2 Decision Fragmentation

Engine intents, allocation outputs, and authorization artifacts all participate in decision semantics.

Risk: there is no single authoritative answer to "what the system decided".

### 9.3 Broker Event Multi-Writer Risk

Broker-facing event artifacts are written by:

- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py`
- `ops/tools/run_orphan_submission_backfill_day_v1.py`
- `ops/tools/run_submission_lifecycle_refresh_v1.py`

Risk: broker acknowledgement truth can diverge or be repaired after the fact by more than one tool.

### 9.4 Position Truth Multi-Writer Risk

Position surfaces overlap across:

- `run_positions_snapshot_day_v2`
- `run_positions_snapshot_day_v4`
- `run_position_lifecycle_snapshot_v2`
- `run_reconciled_trade_state_v1`
- `run_broker_reconciliation_day_v2`

Risk: holdings truth is not yet narrowed to one reducer and one authoritative record family.

### 9.5 Projection Coupling Risk

Current projections are already read-facing, which is good. But `alerts_read_model.py:14-15` directly reads `reports/current_system_projection_v1/.../current_system_projection.v1.json`.

Risk: operator trust can drift toward projection artifacts if authoritative lineage is not made more explicit underneath.

## 10. Migration Hazards

Highest migration hazards proved from repo evidence:

1. Dirty branch and many in-flight authority changes.
   - Evidence: `git status --short` representative output above.

2. More than one current writer for broker-event-like truth.
   - Evidence: `submit_boundary_paper_v4.py`, `run_orphan_submission_backfill_day_v1.py`, `run_submission_lifecycle_refresh_v1.py`.

3. More than one current writer for position-like truth.
   - Evidence: `run_positions_snapshot_day_v2`, `run_positions_snapshot_day_v4`, lifecycle and reconciliation writers.

4. Orchestrator coupling to multiple generations of the same state family.
   - Evidence: `ops/tools/run_c2_paper_day_orchestrator_v2.py:1995` and `:2535` invoke both position snapshot v2 and v4.

5. Current authority model is artifact-rich and stage-rich.
   - Evidence: intents, allocation, authorization, order-plan, mapping, binding, broker-submission, execution-event, fill-ledger, position snapshots, reconciliation reports, and projections all participate.

## 11. Recommended Mapping to the New 5-Object Core

This recommendation is grounded in current repo evidence and is a migration target, not a claim that the repo already implements it.

### Snapshot

Build from current fragmented inputs:

- market data artifacts from phase J
- market calendar artifacts from phase J
- options-chain snapshot inputs from phase A / options-chain promotion
- cash/position/account attestation inputs currently used by readiness and session admission

Recommendation: converge these into one immutable `Snapshot` boundary. Current repo evidence supports the need because no such single object is presently proven.

### PortfolioDecision

Collapse current decision semantics from:

- phase I engine intents
- phase G allocation outputs
- authorization artifacts

Recommendation: produce exactly one immutable `PortfolioDecision` record per snapshot and policy version set.

### ExecutionAction

Collapse current action semantics from:

- order plan
- mapping ledger record
- binding record
- pre-submit attestations

Recommendation: keep those details, but make them children or payload sections of exactly one `ExecutionAction` authority object.

### BrokerEvent

Center on observed broker response semantics from:

- submit boundary result capture
- lifecycle refresh observations
- orphan backfill observations
- raw/observed broker facts where available

Recommendation: narrow to one append-only `BrokerEvent` writer boundary. Existing refresh/backfill tools should emit governed follow-on events, not rewrite or silently upgrade the prior authoritative event.

### PositionState

Collapse current position-like truth from:

- positions snapshot v2/v4
- lifecycle snapshots
- reconciled trade state
- reconciliation outputs

Recommendation: replace overlap with one reducer-generated `PositionState`, plus explicit discrepancy records when broker reconciliation disagrees.

## 12. Stop-Gate Assessment

The requested stop gates were checked against repo evidence.

- `repo evidence contradicts the 5-object pipeline assumption`
  - Result: **No contradiction proved.**
  - Why: current repo has a more fragmented authority model, not a stronger already-unified one.

- `there is more than one current writer for positions truth and migration cannot be proven safe`
  - Result: **Multiple writers proved; safe migration is not yet proven.**
  - Why: position v2, position v4, lifecycle, reconciliation, and reconciled-trade surfaces overlap.

- `existing governance forbids the proposed contract structure`
  - Result: **Not proven.**
  - Why: repo evidence shows extensive governance and contracts, but no proof was found that governance forbids adding a new core contract set.

- `authoritative indices/manifests cannot be updated safely`
  - Result: **Not proven.**
  - Why: risk exists because the tree is dirty, but an explicit prohibition was not found in discovery.

- `current runtime uses hidden mutable state that cannot be lineage-wrapped without breakage`
  - Result: **Not proven.**
  - Why: mutable repair risk exists, especially around refresh/backfill, but impossibility was not proven.

- `backtest path cannot be aligned with live semantics without first extracting shared contracts`
  - Result: **Likely true from current structure, but not proven as an immediate blocker for Phase 0.**
  - Why: current decision/action/position semantics are fragmented, which argues for shared contracts first.

## 13. Phase 0 Conclusion

Repo evidence supports the migration direction.

What is proved:

- The repo is authoritative and file-backed.
- Determinism, replayability, and audit evidence are already first-class goals.
- Projections and UI read models are already separable from authority.
- Current trading authority is fragmented across many artifact families.
- Broker-event-like and position-like truth already have multiple write paths.

What is not proved:

- A current single canonical `Snapshot`
- A current single canonical `PortfolioDecision`
- A current single canonical `ExecutionAction`
- A current single canonical `BrokerEvent` writer boundary
- A current single canonical `PositionState` reducer

Recommendation:

Proceed to Phase 1 only with a governance-first migration that explicitly narrows current authority into the 5-object pipeline. Do not attempt a big-bang runtime rewrite while the current branch remains dirty and while broker-event and position-state multi-writer paths remain live.
