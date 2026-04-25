# DISCOVERY REPORT: ADVISORY AUTHORITY REDESIGN

Status: Phase 0 only  
Repo root: `/home/node/constellation`  
Generated on: `2026-04-12`  
Scope: discovery and proof only; no runtime or governance redesign applied

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

`git -C /home/node/constellation status --short` proved the worktree is not clean. Representative lines:

```text
 M constellation_2/common/day_authority_decision_v1.py
 M constellation_2/phaseD/lib/submit_boundary_paper_v4.py
 M constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py
 M governance/00_INDEX.md
?? ops/tools/run_broker_fact_spine_v1.py
?? governance/discovery/
```

Finding: the branch is dirty and already contains in-flight authority-related work. The first patch set for this request must stay isolated to discovery only.

## 2. Repo Authority and Current Truth Roots

Repo authority is explicit:

- `repo_role.v1.json:5` sets `"repo_role": "authoritative_source"`
- `repo_role.v1.json:6` sets `"authoritative_repo_root": "/home/node/constellation"`
- `repo_role.v1.json:10` declares `"runtime_copy_roots"`
- `governance/02_REGISTRIES/REPO_AUTHORITY_V1.json:4` sets `"authoritative_repo_root": "/home/node/constellation"`
- `governance/02_REGISTRIES/REPO_AUTHORITY_V1.json:7` declares `"runtime_copy_roots"`

Trading truth-root resolution is implemented in:

- `constellation_2/common/truth_root_v1.py:7`
- `constellation_2/common/truth_root_v1.py:101`
- `constellation_2/common/truth_root_v1.py:115`

Replay and audit surfaces are already established:

- `constellation_2/governance/C2_AUDIT_EVIDENCE_CHAIN.md:2`
- `ops/tools/run_runtime_replay_day_v1.py:171`
- `ops/tools/run_replay_integrity_day_v2.py:123`

Finding: the repo has a governed trading truth root and replay model. Advisory artifacts, however, are not currently centered on that same truth root.

## 3. Path Inventory

Required directory scan:

```text
$ find /home/node/constellation -maxdepth 4 -type d | sort
```

Relevant advisory and trading paths proved by that scan:

- `/home/node/constellation/constellation_2/common/advisor_kernel`
- `/home/node/constellation/constellation_2/common/advisor_execution`
- `/home/node/constellation/constellation_2/common/advisor_bridge`
- `/home/node/constellation/constellation_2/common/advisory`
- `/home/node/constellation/constellation_2/phaseL/ui_api`
- `/home/node/constellation/constellation_2/runtime/truth`
- `/home/node/constellation/constellation_2/runtime/truth_sleeves`
- `/home/node/constellation/governance/04_DATA/SCHEMAS/C2/ADVISOR_EXECUTION`
- `/home/node/constellation/governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE`
- `/home/node/constellation/governance/contracts`
- `/home/node/constellation/governance/discovery`

Important structural finding:

- `constellation_2/common/advisory` exists as a package directory in the repo tree, but `rg --files /home/node/constellation/constellation_2/common/advisory` returned no tracked files.

Finding: the repo already separates advisory execution, advisory bridge, advisory kernel, and UI read models. There is also an empty `common/advisory` package path that can host a narrower authoritative advisory core without colliding with the current execution/kernel/bridge split.

## 4. Required Search Proof

Required search run:

```text
$ rg -n "policy|intent|household|allocation|rebalance|advisor|advisory|recommend|portfolio|position|execution|broker|snapshot|projection|governance|contract|invariant" /home/node/constellation
```

Representative advisory-side hits:

- `ops/tools/run_planning_snapshot_v1.py:149` produce `planning_snapshot_v1`
- `ops/tools/run_official_recommendation_set_v1.py:115` produce `official_recommendation_set_v1` from explicit advisor recommendation inputs
- `ops/tools/run_decision_plan_build_v1.py:38` build `decision_plan_v1`
- `ops/tools/run_promotion_candidate_v1.py:49` emit `promotion_candidate.v1.json`
- `ops/tools/run_promotion_review_v1.py:46` emit `promotion_review.v1.json`
- `ops/tools/run_promotion_gate_v1.py:82` emit `promotion_gate_result.v1.json`
- `constellation_2/phaseL/ui_api/advisory_read_model.py:23` reads `official_recommendation_set.v1.json`

Representative trading-side hits:

- `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py:18`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:9`
- `ops/tools/run_runtime_replay_day_v1.py:241`
- `governance/04_DATA/SCHEMAS/C2/REPORTS/current_system_projection.v1.schema.json:40`

Finding: the repo already has an advisory domain, but its currently proven authority model is centered on `planning_snapshot`, `official_recommendation_set`, `decision_plan`, and `promotion_*` artifacts rather than the target `InvestorIntent`, `Policy`, `HouseholdSnapshot`, `PortfolioIntent`, and `PromotionRecord`.

## 5. Exact-Name Existence Check for Target Objects

I also checked for the exact requested core-object names:

```text
$ rg -n "InvestorIntent|investor_intent|HouseholdSnapshot|household_snapshot|PortfolioIntent|portfolio_intent|PromotionRecord|promotion_record|Policy\\b|policy_contract|ADVISORY_TO_TRADING_PROMOTION_BOUNDARY" /home/node/constellation
```

Findings:

- No exact repo-proven matches were found for `InvestorIntent`, `investor_intent`, `HouseholdSnapshot`, `household_snapshot`, `PortfolioIntent`, `portfolio_intent`, `PromotionRecord`, or `promotion_record`.
- The command did return many unrelated policy references, including `strategy_policy_projection_v1.contract.md`, `post_entry_action_policy_v1.contract.md`, and other trading/runtime policies.

Finding: the requested advisory core object names are not already implemented by name in the repo.

## 6. Existing Advisory-Like Surfaces

### 6.1 Current advisory object families

The repo already contains the following advisory-side families:

- `planning_snapshot_v1`
  - `constellation_2/common/advisor_execution/planning_snapshot_v1.py:113`
  - `ops/tools/run_planning_snapshot_v1.py:149`
  - `ops/tools/run_planning_snapshot_v1.py:177`
- `official_recommendation_set_v1`
  - `constellation_2/common/advisor_execution/official_recommendation_set_v1.py:60`
  - `ops/tools/run_official_recommendation_set_v1.py:115`
  - `ops/tools/run_official_recommendation_set_v1.py:149`
- `decision_plan_v1`
  - `constellation_2/common/advisor_execution/decision_plan_v1.py:26`
  - `ops/tools/run_decision_plan_build_v1.py:38`
  - `ops/tools/run_advisor_kernel_decision_plan_v1.py:127`
- `promotion_candidate_v1`
  - `constellation_2/common/advisor_bridge/promotion_candidate_v1.py:15`
  - `ops/tools/run_promotion_candidate_v1.py:49`
  - `ops/tools/run_promotion_candidate_v1.py:63`
- `promotion_review_v1`
  - `constellation_2/common/advisor_bridge/promotion_review_v1.py:15`
  - `ops/tools/run_promotion_review_v1.py:46`
  - `ops/tools/run_promotion_review_v1.py:54`
- `promotion_manual_review_v1`
  - `constellation_2/common/advisor_bridge/promotion_manual_review_v1.py:23`
  - `ops/tools/run_promotion_manual_review_v1.py:55`
  - `ops/tools/run_promotion_manual_review_v1.py:78`
- `promotion_gate_result_v1`
  - `constellation_2/common/advisor_bridge/promotion_gate_result_v1.py:23`
  - `ops/tools/run_promotion_gate_v1.py:82`
  - `ops/tools/run_promotion_gate_v1.py:100`
- `decision_chain_v1`
  - `constellation_2/common/decision_chain_v1.py:10`
  - `ops/tools/run_decision_chain_v1.py:63`
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/decision_chain.v1.schema.json:1`

### 6.2 Current advisory kernel and derived artifacts

There is a broad advisory-kernel layer under `constellation_2/common/advisor_kernel` and matching `ops/tools` producers, including:

- `household_timeline_v1`
- `income_floor_timeline_v1`
- `quarterly_action_summary_v1`
- `decision_memo_v1`
- `decision_comparison_set_v1`
- `readiness_report_v1`
- `plan_health_snapshot_v1`
- `stress_case_view_v1`

Evidence:

- `constellation_2/common/advisor_kernel/household_timeline_v1.py`
- `constellation_2/common/advisor_kernel/income_floor_timeline_v1.py`
- `constellation_2/common/advisor_kernel/quarterly_action_summary_v1.py`
- `constellation_2/common/advisor_kernel/decision_memo_v1.py`
- `ops/tools/run_household_timeline_v1.py:32`
- `ops/tools/run_income_floor_timeline_v1.py:32`
- `ops/tools/run_quarterly_action_summary_v1.py:32`
- `ops/tools/run_decision_memo_v1.py:32`

Finding: the current advisory surface is already rich and artifact-heavy. That is evidence against expanding it further and in favor of narrowing authority.

## 7. Existing Trading Truth Surfaces Relevant to Advisory Linkage

Current trading-core surfaces remain the authoritative executable side:

- Trading truth root:
  - `constellation_2/common/truth_root_v1.py:101`
- Snapshot-like and intent/allocation surfaces:
  - `constellation_2/phaseJ/tools/market_data_ingest_v1.py:32`
  - `constellation_2/phaseG/allocation/run/run_allocation_day_v2.py:45`
  - `constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py:24`
- Execution action / submit path:
  - `constellation_2/phaseC/tools/c2_submit_preflight_offline_v1.py:17`
  - `constellation_2/phaseD/lib/submit_boundary_paper_v4.py:9`
- Broker event / execution evidence:
  - `ops/tools/run_submission_lifecycle_refresh_v1.py:14`
  - `ops/tools/run_orphan_submission_backfill_day_v1.py:16`
- Position truth:
  - `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py:22`
  - `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py:18`

Finding: current advisory artifacts are layered above a pre-existing trading truth system and should not replace it.

## 8. Current Projection and Read-Model Surfaces

Advisory UI and read models:

- `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py:36`
- `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py:1524`
- `constellation_2/phaseL/ui_api/advisory_read_model.py:20`
- `constellation_2/phaseL/ui_api/advisory_read_model.py:23`
- `constellation_2/phaseL/ui_api/advisory_read_model.py:24`
- `constellation_2/phaseL/ui_api/advisory_read_model.py:63`
- `constellation_2/phaseL/ui_api/advisory_read_model.py:94`

Important current UI behavior:

- `advisory_read_model.py:23` reads `official_recommendation_set.v1.json`
- `advisory_read_model.py:24` reads `advisor_trade_intent_proposal.v1.json`
- `advisory_read_model.py:63` explicitly says `"No governed confidence field exists on official_recommendation_set_v1."`
- `advisory_read_model.py:94` explicitly says `"Confidence is not present on the proven advisory artifacts in this repo."`

Other read/projection surfaces include:

- `ops/tools/run_decision_chain_v1.py:54` writes decision-chain report
- `ops/tools/run_runtime_trace_bundle_v1.py`
- `constellation_2/phaseL/ui_api/system_summary_read_model.py`
- `constellation_2/phaseL/ui_api/operations_read_model.py`

Finding: user-facing advisory views are already derived. The stronger problem is that `official_recommendation_set_v1` itself is treated as an authority-bearing upstream artifact, which conflicts with the requested redesign.

## 9. Current Writer and Reader Map

### 9.1 Advisory-side writers

Current advisory writers are split across multiple families:

- Planning snapshot writer
  - `ops/tools/run_planning_snapshot_v1.py`
- Recommendation writer
  - `ops/tools/run_official_recommendation_set_v1.py`
- Decision/action writer
  - `ops/tools/run_action_policy_compile_v1.py`
  - `ops/tools/run_decision_plan_build_v1.py`
  - `ops/tools/run_advisor_kernel_decision_plan_v1.py`
- Promotion writers
  - `ops/tools/run_advisor_trade_translation_v1.py`
  - `ops/tools/run_advisor_trade_intent_proposal_v1.py`
  - `ops/tools/run_promotion_candidate_v1.py`
  - `ops/tools/run_promotion_review_v1.py`
  - `ops/tools/run_promotion_manual_review_v1.py`
  - `ops/tools/run_promotion_gate_v1.py`
- Advisory kernel derived writers
  - multiple `run_*_v1.py` tools listed above

### 9.2 Current advisory readers

Key readers:

- `constellation_2/phaseL/ui_api/advisory_read_model.py`
- `ops/tools/run_decision_chain_v1.py`
- `constellation_2/common/advisor_execution/action_policy_loader.py`
- advisory-kernel services under `constellation_2/common/advisor_kernel/*`

### 9.3 Current advisory authority registry

The repo has an explicit advisory-side authority registry:

- `constellation_2/common/authority_registry_v1.py:97`
  - `official_recommendation_set_v1`
  - `authority_class`: `recommendation_authority`
  - `write_root`: `/tmp/constellation_2_foundation/advisor_runtime`
- `constellation_2/common/authority_registry_v1.py:98`
  - `planning_snapshot_v1`
  - `authority_class`: `fact_authority`
  - `write_root`: `/tmp/constellation_2_foundation/advisor_runtime`
- `constellation_2/common/authority_registry_v1.py:99`
  - `promotion_candidate_v1`
  - `authority_class`: `promotion_candidate_authority`
  - `write_root`: `/tmp/constellation_2_foundation/advisor_runtime`
- `constellation_2/common/authority_registry_v1.py:101`
  - `promotion_gate_result_v1`
  - `authority_class`: `recommendation_authority`
  - notes: `promotion-ready governance artifact, still non-executable`

Test coverage also asserts these families:

- `constellation_2/common/tests/test_authority_registry_v1.py:30`
- `constellation_2/common/tests/test_authority_registry_v1.py:31`
- `constellation_2/common/tests/test_authority_registry_v1.py:36`
- `constellation_2/common/tests/test_authority_registry_v1.py:43`
- `constellation_2/common/tests/test_authority_registry_v1.py:53`

Finding: the repo already treats some advisory artifacts as authority-bearing, but they live under a separate `advisor_runtime` root and currently elevate `official_recommendation_set_v1` itself to authority. That is materially different from the requested model.

## 10. Current Advisory Entry Semantics

Current planning snapshot semantics:

- `ops/tools/run_planning_snapshot_v1.py:149` describes `planning_snapshot_v1` as being produced from explicit advisor inputs and optional Constellation truth cash state
- `ops/tools/run_planning_snapshot_v1.py:131` reads `cash_ledger_snapshot.v1.json`
- `ops/tools/run_planning_snapshot_v1.py:138` can use `truth_cash_ledger_snapshot`
- `ops/tools/run_planning_snapshot_v1.py:177` writes `schema_id: planning_snapshot`
- `ops/tools/run_planning_snapshot_v1.py:179` derives `planning_snapshot_id`
- `ops/tools/run_planning_snapshot_v1.py:183-187` includes `accounts`, `liquidity`, `spending`, `income`, `tax_profile`

Current recommendation semantics:

- `ops/tools/run_official_recommendation_set_v1.py:115` says it produces governed `official_recommendation_set_v1` from explicit advisor recommendation inputs
- `ops/tools/run_official_recommendation_set_v1.py:149` writes `schema_id: official_recommendation_set`
- `ops/tools/run_official_recommendation_set_v1.py:151-152` uses `advisory_packet_id` and `created_at` from the planning snapshot

Current decision semantics:

- `constellation_2/common/advisor_execution/decision_plan_builder.py:11`
- `decision_plan_builder.py:12-14` requires consistency across planning snapshot, recommendation set, and action intent
- `decision_plan_builder.py:20-25` hashes planning snapshot liquidity/spending/income/tax fields into the decision basis

Finding: the current advisory chain is roughly:

`planning_snapshot -> official_recommendation_set -> action_intent/action_policy_pack -> decision_plan -> promotion_* -> decision_chain`

This is recommendation-first, not investor-intent/policy-first.

## 11. Current Advisory-to-Trading Bridge Semantics

Bridge and promotion logic is present:

- `constellation_2/common/advisor_bridge/advisor_trade_bridge_service.py:98`
  - builds `advisor_trade_translation`
- `advisor_trade_bridge_service.py:111`
  - builds `advisor_trade_intent_proposal`
- `constellation_2/common/advisor_bridge/promotion_plane_service.py:13`
  - builds `promotion_candidate`
- `promotion_plane_service.py:28-30`
  - candidate is derived from proposal + planning snapshot + decision plan
- `constellation_2/common/advisor_bridge/promotion_gate_service.py:17`
  - builds `promotion_gate_result`
- `governance/04_DATA/SCHEMAS/C2/ADVISOR_BRIDGE/promotion_gate_result.v1.schema.json:1`
  - gate statuses are `promotion_ready`, `promotion_rejected`, `promotion_blocked`

Important constraint already present:

- `constellation_2/common/authority_registry_v1.py:101`
  - `promotion_gate_result_v1` is noted as `promotion-ready governance artifact, still non-executable`

Finding: the repo already has a narrow advisory-to-trading promotion plane in spirit, but it is modeled as `promotion_candidate/review/manual_review/gate_result`, not as one immutable `PromotionRecord`.

## 12. Migration Hazards

Highest-value hazards proved from repo evidence:

1. Recommendations are currently treated as authority.
   - Evidence: `authority_registry_v1.py:97` marks `official_recommendation_set_v1` as `recommendation_authority`.
   - Risk: this conflicts directly with the target rule that recommendations must be derived and non-authoritative.

2. Advisory authority is currently rooted outside the governed trading truth root.
   - Evidence: `runtime_base_v1.py:6` default advisor runtime root is `/tmp/constellation_2_foundation/advisor_runtime`.
   - Risk: lineage, replay, and operator trust are split across two different authority roots.

3. Current planning snapshot blends household inputs and some live truth-derived cash state.
   - Evidence: `run_planning_snapshot_v1.py:131-138` reads trading cash ledger truth; `:149` describes optional truth cash state.
   - Risk: household facts and planning inputs are not yet narrowed into a clear `InvestorIntent` / `Policy` / `HouseholdSnapshot` split.

4. Current advisory chain is recommendation-first.
   - Evidence: `run_official_recommendation_set_v1.py:115` produces recommendation set from explicit advisor recommendation inputs.
   - Risk: narrative or recommendation artifacts can become hidden authority.

5. Current promotion boundary is multi-step and review-heavy.
   - Evidence: `promotion_candidate`, `promotion_review`, `promotion_manual_review`, `promotion_gate_result`.
   - Risk: promotion state is distributed across several records instead of one narrow bridge record.

6. Classification truth for a future `HouseholdSnapshot` is not yet clearly proven in the advisory domain.
   - Evidence: classification references appear in broader ops and cockpit surfaces, but no advisory-domain `household_snapshot` or classification-manifest usage contract was proven.
   - Risk: household coverage and verified/unverified asset separation may need governance before runtime extraction.

## 13. Recommendation: Where the New Authoritative Objects Should Live

This is a migration recommendation grounded in current repo structure.

### InvestorIntent

Recommended location:

- new files under `constellation_2/common/advisory/`

Why:

- `constellation_2/common/advisory` already exists as a package path but currently has no tracked files.
- This allows investor-intent authority to be kept upstream of `advisor_execution`, `advisor_kernel`, and `advisor_bridge`, rather than being embedded inside recommendation or execution packages.

### Policy

Recommended location:

- new files under `constellation_2/common/advisory/`

Why:

- Policy compilation should sit next to `InvestorIntent`, not inside `advisor_execution` or trading policy projections.
- Current `strategy_policy_projection_v1` and post-entry/trading policy contracts are different domains.

### HouseholdSnapshot

Recommended location:

- new files under `constellation_2/common/advisory/`

Why:

- Household snapshot should be an upstream frozen advisory fact object.
- Current `planning_snapshot_v1` in `advisor_execution` is too downstream and already coupled to advisory packet generation.

### PortfolioIntent

Recommended location:

- either `constellation_2/common/advisory/` or a new narrowly-scoped subpackage directly under it

Why:

- It should remain advisory-domain authority, upstream of bridge and trading.
- It should not live in `advisor_execution`, because current `decision_plan` is already closer to recommendation/action orchestration than to portfolio-intent authority.

### PromotionRecord

Recommended location:

- `constellation_2/common/advisor_bridge/`

Why:

- The repo already has `advisor_trade_translation`, `advisor_trade_intent_proposal`, and `promotion_*` artifacts in `advisor_bridge`.
- The requested narrow bridge belongs at that seam, but should replace the current multi-artifact promotion stack rather than add more bridge objects beside it.

### Derived outputs

Recommended location:

- keep under `constellation_2/common/advisor_kernel/`, `phaseL/ui_api/`, and report/schema surfaces

Why:

- These packages already behave like derived/kernel/read-model layers.
- They should remain non-authoritative and rebuildable from upstream advisory authority.

## 14. Stop-Gate Assessment

Required stop gates checked against repo evidence:

- `current repo structure already has a stronger authoritative advisory model`
  - Result: **No.**
  - Why: the current model is recommendation-first, lives under `/tmp/constellation_2_foundation/advisor_runtime`, and lacks the requested `InvestorIntent` / `Policy` / `HouseholdSnapshot` / `PortfolioIntent` split.

- `existing governance conflicts with these contract shapes`
  - Result: **Not proven.**
  - Why: the repo has many contracts and schemas, but no evidence was found that forbids a new advisory contract set.

- `household aggregation is too entangled with trading truth to separate safely`
  - Result: **Entanglement exists, but impossibility is not proven.**
  - Why: `planning_snapshot_v1` reads trading cash truth, but the object boundary is still extractable.

- `projections currently act as hidden authority and cannot be untangled safely`
  - Result: **Not proven.**
  - Why: the stronger issue is not projections; it is that recommendation artifacts themselves are currently treated as authority.

- `classification truth cannot be proven from current repo reality`
  - Result: **Partially true as a risk.**
  - Why: advisory-domain classification truth was not clearly proven as a first-class authoritative object.

- `promotion boundary cannot be implemented without breaking trading-core invariants`
  - Result: **Not proven.**
  - Why: the current repo already has a non-executable promotion plane, which supports narrowing to a stricter `PromotionRecord`.

## 15. Phase 0 Conclusion

Repo evidence supports the redesign direction.

What is proved:

- The repo already contains a real advisory stack.
- Current advisory authority is centered on `planning_snapshot`, `official_recommendation_set`, `decision_plan`, and `promotion_*`.
- Advisory artifacts are written under a separate `advisor_runtime` root.
- Recommendations are currently treated as authority-bearing artifacts.
- UI and dashboard layers are already derived from advisory artifacts.
- Trading truth remains separate and authoritative for executable market behavior.

What is not proved:

- Any existing `InvestorIntent`
- Any existing authoritative advisory `Policy` object matching the requested model
- Any existing `HouseholdSnapshot` object by that name
- Any existing `PortfolioIntent` object by that name
- Any single `PromotionRecord` object replacing the current multi-step promotion plane

Recommendation:

Proceed to Phase 1 with governance-first extraction of a smaller authoritative advisory core:

`InvestorIntent -> Policy -> HouseholdSnapshot -> PortfolioIntent -> PromotionRecord`

while demoting current recommendation-first artifacts to derived or transitional roles and preserving the existing trading core as the only executable authority.
