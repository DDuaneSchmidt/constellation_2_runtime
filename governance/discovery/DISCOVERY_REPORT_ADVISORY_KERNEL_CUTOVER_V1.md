# Discovery Report: Advisory Kernel Cutover V1

## Scope

This report proves the currently implemented authority surfaces and cutover constraints for the narrow advisory kernel target:

`InvestorIntent -> Policy -> HouseholdSnapshot -> PortfolioIntent -> PromotionDecision -> PromotionRecord -> ExecutionIntent -> existing paper trading`

It is grounded only in the authoritative repo at `/home/node/constellation`.

## Phase 0 Repo Proof

- Repo root: `git -C /home/node/constellation rev-parse --show-toplevel` -> `/home/node/constellation`
- Starting branch: `git -C /home/node/constellation rev-parse --abbrev-ref HEAD` -> `feature/implement-economic-state-authority-v1`
- Working branch after isolation: `git -C /home/node/constellation branch --show-current` -> `feature/advisory-kernel-cutover-v1`
- Remote: `git -C /home/node/constellation remote -v` -> `origin git@github.com:DDuaneSchmidt/constellation_2_runtime.git`
- Worktree state: `git -C /home/node/constellation status --branch --short` proved the branch is already dirty with many unrelated modifications and untracked files before this cutover work.

## 1. Exact Proven Files And Paths

### Top advisory authorities already implemented

- `constellation_2/common/advisory/investor_intent_v1.py:73`
- `constellation_2/common/advisory/investor_intent_service_v1.py:128`
- `constellation_2/common/advisory/policy_v1.py:67`
- `constellation_2/common/advisory/policy_service_v1.py:54`
- `constellation_2/common/advisory/household_snapshot_v1.py:23`
- `constellation_2/common/advisory/household_snapshot_service_v1.py:225`
- `constellation_2/common/advisory/advisory_storage_v1.py:18`
- `constellation_2/common/advisory/advisory_storage_v1.py:26`
- `constellation_2/common/advisory/advisory_storage_v1.py:42`

### Promotion authority already implemented

- `constellation_2/common/advisor_bridge/promotion_record_v1.py:23`
- `constellation_2/common/advisor_bridge/promotion_record_service.py:38`
- `ops/tools/run_promotion_gate_v1.py:90`

### Legacy recommendation-led advisory execution surfaces still present

- `constellation_2/common/advisor_execution/planning_snapshot_v1.py:115`
- `constellation_2/common/advisor_execution/decision_plan_v1.py:26`
- `constellation_2/common/advisor_execution/official_recommendation_set_v1.py:60`
- `constellation_2/common/advisor_bridge/advisor_trade_bridge_service.py:102`
- `constellation_2/common/advisor_bridge/advisor_trade_bridge_service.py:124`
- `constellation_2/common/advisor_bridge/promotion_plane_service.py:5`
- `ops/tools/run_advisor_trade_translation_v1.py:28`
- `ops/tools/run_advisor_trade_intent_proposal_v1.py:28`
- `ops/tools/run_promotion_candidate_v1.py:29`

### Existing paper-trading intake boundary actually proven

- `constellation_2/phaseD/tools/c2_submit_paper_v5.py:49`
- `constellation_2/phaseD/tools/c2_submit_paper_v5.py:50`
- `constellation_2/phaseD/tools/c2_submit_paper_v5.py:51`
- `constellation_2/phaseD/tools/c2_submit_paper_v5.py:70`
- `constellation_2/phaseD/tools/c2_submit_paper_v5.py:72`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:191`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:653`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:793`
- `constellation_2/phaseC/tools/c2_submit_preflight_offline_v2.py:10`
- `constellation_2/phaseC/tools/c2_submit_preflight_offline_v2.py:11`
- `constellation_2/phaseC/tools/c2_submit_preflight_offline_v2.py:93`

### Canonical governance location and update surfaces actually proven

- `governance/00_INDEX.md:38`
- `governance/00_INDEX.md:56`
- `governance/00_INDEX.md:144`
- `governance/00_INDEX.md:206`
- `governance/00_MANIFEST.yaml:894`
- `governance/00_MANIFEST.yaml:907`
- `governance/00_MANIFEST.yaml:1099`
- `governance/00_MANIFEST.yaml:1111`

## 2. Exact Currently Authoritative Surfaces Already In Repo

### InvestorIntent

Proven as a real advisory authority:

- Record: `constellation_2/common/advisory/investor_intent_v1.py:73`
- Builder: `constellation_2/common/advisory/investor_intent_service_v1.py:128`
- Registry row: `constellation_2/common/authority_registry_v1.py:99`

### Policy

Proven as a real compiled advisory authority:

- Record: `constellation_2/common/advisory/policy_v1.py:67`
- Builder: `constellation_2/common/advisory/policy_service_v1.py:54`
- Registry row: `constellation_2/common/authority_registry_v1.py:101`

### HouseholdSnapshot

Proven as a real advisory household authority:

- Record: `constellation_2/common/advisory/household_snapshot_v1.py:23`
- Builder: `constellation_2/common/advisory/household_snapshot_service_v1.py:225`
- Registry row: `constellation_2/common/authority_registry_v1.py:98`

### PromotionRecord

Proven as the sole promotion authority currently recognized in the runtime registry:

- Record: `constellation_2/common/advisor_bridge/promotion_record_v1.py:23`
- Builder: `constellation_2/common/advisor_bridge/promotion_record_service.py:38`
- Registry row: `constellation_2/common/authority_registry_v1.py:106`
- Writer boundary: `ops/tools/run_promotion_gate_v1.py:90`

### Current authority weakness

`PromotionRecord` is authoritative for promotion, but its lineage is still rooted in legacy surfaces:

- `planning_snapshot_id`
- `decision_plan_id`
- `proposal_id`
- legacy `promotion_candidate/review/manual_review/gate_result`

Proof:

- `constellation_2/common/advisor_bridge/promotion_record_v1.py:32`
- `constellation_2/common/advisor_bridge/promotion_record_v1.py:34`
- `constellation_2/common/advisor_bridge/promotion_record_v1.py:35`
- `constellation_2/common/advisor_bridge/promotion_record_service.py:69`
- `constellation_2/common/advisor_bridge/promotion_record_service.py:93`

## 3. Exact Legacy Recommendation-Led Runtime Surfaces Still Capable Of Execution Influence

These remain in the advisory-to-promotion path and therefore still influence whether advisory-origin execution can happen:

### Recommendation-led upstream chain

- `official_recommendation_set_v1` remains registered as `recommendation_authority` in `constellation_2/common/authority_registry_v1.py:100`
- `planning_snapshot_v1` remains registered as `fact_authority` in `constellation_2/common/authority_registry_v1.py:102`
- `decision_plan_v1` remains registered as action authority in `constellation_2/common/authority_registry_v1.py:96`

### Trade-shaped legacy bridge surfaces

- `advisor_trade_translation_v1` is emitted by `ops/tools/run_advisor_trade_translation_v1.py:73`
- `advisor_trade_intent_proposal_v1` is emitted by `ops/tools/run_advisor_trade_intent_proposal_v1.py:73`
- `promotion_candidate_v1` still depends on `planning_snapshot`, `decision_plan`, `advisor_trade_translation`, and `advisor_trade_intent_proposal`:
  - `ops/tools/run_promotion_candidate_v1.py:29`
  - `ops/tools/run_promotion_candidate_v1.py:30`
  - `ops/tools/run_promotion_candidate_v1.py:31`
  - `ops/tools/run_promotion_candidate_v1.py:32`
  - `ops/tools/run_promotion_candidate_v1.py:63`
- `promotion_plane_service.py` still imports and validates against `DecisionPlanV1`, `PlanningSnapshotV1`, `AdvisorTradeIntentProposalV1`, and `AdvisorTradeTranslationV1`:
  - `constellation_2/common/advisor_bridge/promotion_plane_service.py:5`
  - `constellation_2/common/advisor_bridge/promotion_plane_service.py:6`
  - `constellation_2/common/advisor_bridge/promotion_plane_service.py:7`
  - `constellation_2/common/advisor_bridge/promotion_plane_service.py:8`

### Meaning

Legacy recommendation-led advisory logic no longer owns promotion authority, but it still owns the current advisory-origin trade proposal path feeding into promotion inputs. That path must be cut off or bypassed before the new kernel can become the only advisory execution path.

## 4. Exact Paper-Trading Intake Boundary Actually Proven In Code

The canonical paper submit entrypoint is:

- `constellation_2/phaseD/tools/c2_submit_paper_v5.py`

It accepts exactly two downstream intake shapes:

1. Normative path: sealed execution package
   - `--execution_package_path`
   - proof: `constellation_2/phaseD/tools/c2_submit_paper_v5.py:50`
   - proof: `constellation_2/phaseD/tools/c2_submit_paper_v5.py:72`

2. Deprecated compatibility path: raw Phase C candidate directory
   - `--phasec_out_dir`
   - only when `--legacy_raw_candidate YES`
   - proof: `constellation_2/phaseD/tools/c2_submit_paper_v5.py:49`
   - proof: `constellation_2/phaseD/tools/c2_submit_paper_v5.py:51`
   - proof: `constellation_2/phaseD/tools/c2_submit_paper_v5.py:71`

`submit_boundary_paper_v4` proves the actual artifact families loaded from a raw Phase C candidate:

- `equity_order_plan.v2.json` or `equity_order_plan.v1.json` or `order_plan.v1.json`
- `mapping_ledger_record.v2.json` or `mapping_ledger_record.v1.json`
- `binding_record.v2.json` or `binding_record.v1.json`
- `submit_preflight_decision.v1.json`
- `execution_identity_record.v1.json`
- sibling `attempt_state.v1.json` in the parent `attempt_<ID>/` directory

Proof:

- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:653`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:658`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:659`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:663`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:671`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:679`

The sealed package path is governed separately by `execution_package.v1.json`:

- schema: `governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_package.v1.schema.json:16`
- governance contract: `governance/05_CONTRACTS/C2/execution_build_authority_v1.contract.md:23`
- normative path rule: `governance/05_CONTRACTS/C2/execution_build_authority_v1.contract.md:51`

### Important cutover constraint

No repo-proven `ExecutionIntent` artifact is currently accepted by `c2_submit_paper_v5`.

Therefore the new kernel cannot hand off directly into paper submit without one of:

- a minimal adapter from `ExecutionIntent` to the existing sealed package or raw Phase C candidate path
- or a minimal extension to the paper submit boundary to accept `ExecutionIntent`

This is a real gap and must be handled explicitly in later phases.

## 5. Exact Governance Location And Update Rules Actually Proven In Repo

Canonical governance surfaces are:

- governance contracts: `governance/contracts/`
- governance index: `governance/00_INDEX.md`
- governance manifest: `governance/00_MANIFEST.yaml`

Proof:

- `governance/00_INDEX.md:38` through `governance/00_INDEX.md:49`
- `governance/00_MANIFEST.yaml:907` through `governance/00_MANIFEST.yaml:943`

Repo-proven rule:

- contracts are governed artifacts only when explicitly listed in `governance/00_MANIFEST.yaml`
- schemas are governed artifacts only when explicitly listed in `governance/00_MANIFEST.yaml`

Proof:

- `governance/00_INDEX.md:144`
- `governance/00_INDEX.md:206`

Current advisory contracts and schemas already follow that pattern:

- contracts listed in `governance/00_MANIFEST.yaml:907` through `governance/00_MANIFEST.yaml:943`
- schemas listed in `governance/00_MANIFEST.yaml:1099` through `governance/00_MANIFEST.yaml:1111`

## 6. Exact Gaps Between Current Repo And Target Kernel Architecture

### Missing authoritative runtime surfaces

These target kernel objects are not yet proven as implemented runtime authorities:

- `PortfolioIntent`
- `PromotionDecision`
- `ExecutionIntent`
- `KernelRunEnvelope`

The repo has governance contracts for `PortfolioIntent`, but no runtime record, builder, schema, storage path, or tests were proven in code.

### Promotion lineage mismatch

`PromotionRecord` exists, but it still derives from:

- `planning_snapshot_id`
- `decision_plan_id`
- `proposal_id`
- legacy promotion intermediates

## 7. Pre-Implementation Hardening Constraints

The following constraints are now binding design artifacts for later Phases 4 through 8:

1. `ExecutionIntent` must be a pure transformation of `PromotionRecord.approved_delta`.
   - no recomputation
   - no rounding
   - no live advisory-state reads

2. `PromotionRecordV2.approved_delta` must be canonicalized before authorization identity derivation.
   - deterministic ordering
   - no duplicates
   - no zero entries
   - explicit side and quantity
   - stable serialization before hashing

3. Candidate directory staging must match the proven Phase D identity-set contract exactly.
   - `equity_order_plan.v2.json`
   - `mapping_ledger_record.v2.json`
   - `binding_record.v2.json`
   - `submit_preflight_decision.v1.json`
   - `execution_identity_record.v1.json`
   - sibling `attempt_state.v1.json`

4. Idempotency must align across the advisory-to-execution boundary.
   - `PromotionRecord.idempotency_key`
   - `ExecutionIntent.idempotency_key`
   - downstream `submission_id` derivation

5. `KernelRunEnvelope` must link:
   - `PromotionRecord`
   - `ExecutionIntent`
   - `execution_package.v1.json`

6. Future implementation must fail closed if:
   - `ExecutionIntent` cannot be derived exactly from `approved_delta`
   - the candidate identity set cannot be fully constructed
   - `execution_package.v1.json` cannot be sealed with `closure_status=COMPLETE`

These constraints were added because repo proof shows that the existing paper-trading path consumes a sealed execution package plus a candidate directory identity set, not a direct advisory execution artifact.

This does not satisfy the target cutover chain:

`PortfolioIntent -> PromotionDecision -> PromotionRecord`

### Paper intake mismatch

Existing paper submit consumes:

- sealed `execution_package.v1.json`
- or legacy raw Phase C candidate directory

It does not consume an advisory execution artifact directly.

### Legacy execution influence still present

The current advisory-origin trade proposal path still runs through:

- `planning_snapshot_v1`
- `decision_plan_v1`
- `advisor_trade_translation_v1`
- `advisor_trade_intent_proposal_v1`
- `promotion_candidate_v1`

That path is not yet cut off from execution influence.

## Cutover Safety Judgment

The repo proves enough to continue into kernel governance work, but it does not prove a complete safe runtime cutover yet.

The main blockers are:

1. no runtime `PortfolioIntent`
2. no runtime `PromotionDecision`
3. no runtime `ExecutionIntent`
4. no runtime `KernelRunEnvelope`
5. current `PromotionRecord` lineage still rooted in legacy recommendation-era surfaces
6. existing paper submit boundary does not accept an advisory execution artifact directly

## Required next-step implication

Any safe kernel cutover must preserve these fail-closed facts:

- no advisory-origin executable consequence without `PromotionRecord`
- no attempt to bypass the existing paper submit boundary
- no parallel recommendation-led execution path
- no claim that `ExecutionIntent` already exists or is already accepted downstream
