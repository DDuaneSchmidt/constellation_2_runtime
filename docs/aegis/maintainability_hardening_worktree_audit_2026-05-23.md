# Aegis Maintainability Hardening Worktree Audit - 2026-05-23

Scope: classify dirty worktree, clean generated/runtime artifacts, reduce oversized UI module risk, and validate without changing runtime behavior, scoring, scheduler, certification, or broker controls.

## Summary

- Tracked modified files: 129
- Untracked non-ignored files: 808
- Current changed-file inventory rows: 937
- `pages/index.js` line count reduced by extracting route metadata: 14,155 -> 13,709 lines.
- New extracted route metadata module: `constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js` (453 lines).
- Route source-inspection tests now read the imported route metadata via `operator_shell_test_sources.py` instead of depending on duplicated literals in `pages/index.js`.
- Restored `research_lab/research_store/` as an ignored local Research UI fixture; it is required by readonly Research UI tests and should not be committed.
- Full workspace extraction remains a later risk because workspace render functions still share many local helpers. This pass performs a low-risk first extraction only.

## Category Counts

- accidental/stale change: 19
- generated artifact that should not be committed: 76
- intentional production change: 613
- intentional test change: 228
- secret/config risk: 1

## Cleaned / Ignored Local Artifacts

- Removed/ignored top-level research screenshots (`research_forensic_*.png`, `research_inventory_search_*.png`, `research_ui_*.png`, `aegis-opportunities-live-after-restart.png`).
- Removed/ignored `artifacts/` and `.pytest_cache/`.
- Ignored `research_lab/research_store/` because it is local fixture/store data used by Research UI tests.
- Ignored `ops/config/*.env`; provider credentials remain outside git.
- Attempted broad `__pycache__` cleanup. Many caches were removed; older read-only cache directories remain local and ignored, with permission-denied cleanup noted as residual local noise.

## Secret / Config Risk

- `ops/config/aegis_market_data.env` is present locally and ignored by `.gitignore`; it must not be committed or printed.
- Credential leak scan found only test dummy credential assignments after excluding env files, venvs, caches, and local research store; no production secret assignment was reported.
- `ops/config/aegis_runtime_universe.json` and `ops/config/aegis_paper_trade_construction_defaults.json` are non-secret JSON config candidates and remain visible for review.

## Validation Run

- Full UI suite: `python3 -m pytest constellation_2/phaseL/ui/tests -q` -> 428 passed.
- Targeted common/runtime suite: `python3 -m pytest constellation_2/common/tests/test_aegis_operational_maturity_hardening_v1.py constellation_2/common/tests/test_aegis_event_sourced_runtime_v1.py constellation_2/common/tests/test_aegis_event_monitoring_v1.py constellation_2/common/tests/test_aegis_control_plane_v1.py -q` -> 110 passed.
- Browser click-through on managed 8787: `python3 ops/tools/aegis_operator_clickthrough_qa_v1.py --base-url http://127.0.0.1:8787` -> ok true, no failures.
- Replay for 2026-05-22: `python3 ops/tools/replay_aegis_runtime_state_v1.py --truth_root /home/node/constellation_runtime_data/truth --day 2026-05-22` -> REAL_RUNTIME / MANUAL_CAPTURE_CAPABILITY_READY / capture_ticket_count 0.
- Credential leak scan: scanned tracked + non-ignored untracked text files; only test dummy credential assignments were flagged.

## Safety Invariants

- Replay reports `autonomous_execution_allowed=false` and `broker_submit_required=false`.
- Runtime truth kernel policy fields remain `autonomous_execution_policy=DISABLED_BY_DESIGN` and `broker_submit_transmit_policy=DISABLED_BY_DESIGN`.
- `blocked_capabilities` include `AUTONOMOUS_EXECUTION_ALLOWED` and `TRADE_ADVICE_ALLOWED`.
- Capture state is capability-ready but no-ticket: `capture_ticket_status=NONE_AVAILABLE`, `capture_ticket_count=0`.

## Diff Stat Snapshot

```text
 .gitignore                                         |   10 +
 ...ion_package_builder_from_execution_intent_v1.py |   38 +-
 .../common/aegis_chatgpt_control_packet_v1.py      |  225 +-
 .../common/aegis_eod_artifact_contract_v1.py       |  170 +-
 .../common/aegis_event_monitoring_v1.py            |   10 +-
 constellation_2/common/aegis_lite_eod_v1.py        |   33 +
 .../common/aegis_lite_manual_feedback_v1.py        |    2 +
 .../common/aegis_lite_operating_status_v1.py       |   15 +-
 .../common/aegis_lite_promoted_candidates_v1.py    |   19 +
 constellation_2/common/aegis_market_context_v1.py  |   30 +-
 .../common/candidate_observability_v1.py           |   83 +-
 .../common/day_activation_authority_v1.py          |   32 +-
 .../common/economic_state_authority_v1.py          |   71 +-
 constellation_2/common/engine_universe_v1.py       |  775 +-
 .../common/execution_build_authority_v1.py         |   59 +-
 .../common/global_context_authority_v1.py          |   23 +
 .../tests/test_aegis_chatgpt_control_packet_v1.py  |  130 +-
 .../common/tests/test_aegis_control_plane_v1.py    |   18 +
 .../common/tests/test_aegis_day_run_ledger_v1.py   |   36 +-
 .../tests/test_aegis_eod_artifact_contract_v1.py   |  132 +-
 .../tests/test_aegis_lite_event_awareness_v1.py    |    7 +-
 .../tests/test_aegis_lite_operational_spine_v1.py  |   61 +-
 .../common/tests/test_aegis_market_context_v1.py   |   32 +-
 .../test_aegis_noon_preflight_rehearsal_v1.py      |   29 +-
 .../test_aegis_operator_command_contracts_v1.py    |    3 +-
 .../common/tests/test_aegis_operator_state_v1.py   |    8 +-
 .../test_aegis_portfolio_decision_pipeline_v1.py   |   86 +
 .../tests/test_aegis_truth_integrity_layer_v1.py   |   99 +-
 .../tests/test_candidate_observability_v1.py       |   23 +-
 ...est_defensive_tail_required_inputs_bridge_v1.py |   93 +
 .../tests/test_execution_build_authority_v1.py     |   33 +-
 ..._sleeve_evaluation_and_intent_arbitration_v1.py |  238 +-
 .../run/run_cash_ledger_snapshot_day_v1.py         |   32 +
 .../positions/run/run_positions_snapshot_day_v5.py |   24 +
 .../phaseG/allocation/run/run_allocation_day_v2.py |    4 +
 .../run/run_defensive_tail_intents_day_v1.py       |   60 +-
 .../run/run_event_dislocation_intents_day_v1.py    |   37 +
 .../run/run_mean_reversion_intents_day_v1.py       |   38 +
 .../run/run_trend_eq_primary_intents_day_v1.py     |   32 +-
 .../build_defensive_tail_required_inputs_day_v1.py |  169 +-
 ...istorical_market_data_snapshot_downloader_v1.py |  240 +-
 .../phaseL/ui/server/run_ops_dashboard_v1.py       | 2670 +++++-
 constellation_2/phaseL/ui/static/aegis.css         | 7326 ++++++++++++---
 constellation_2/phaseL/ui/static/index.html        |    8 +-
 .../static/operator_shell/domain_client/index.js   |  260 +
 .../ui/static/operator_shell/fixtures/mockData.js  |    2 +-
 .../phaseL/ui/static/operator_shell/main.js        | 1046 ++-
 .../ui/static/operator_shell/navigation_schema.js  |  229 +-
 .../phaseL/ui/static/operator_shell/pages/index.js | 9686 ++++++++++++++++++--
 .../tests/test_aegis_post_pivot_navigation_v1.py   |   63 +-
 ...t_aegis_readiness_kernel_ledger_authority_v1.py |   10 +-
 .../tests/test_aegis_ui_projection_service_v1.py   |    3 +-
 .../phaseL/ui/tests/test_capital_ui_api_v1.py      |    6 +-
 .../ui/tests/test_configuration_workflow_v1.py     |    5 +-
 .../ui/tests/test_financial_state_ui_api_v1.py     |    2 +-
 .../ui/tests/test_operator_workflow_layer_v1.py    |    2 +-
 .../ui/tests/test_opportunity_state_ui_api_v1.py   |   19 +
 .../ui/tests/test_outcome_state_ui_api_v1.py       |    4 +-
 .../ui/tests/test_performance_cockpit_route_v1.py  |    3 +-
 .../tests/test_policy_evolution_state_ui_api_v1.py |   10 +-
 .../ui/tests/test_product_summary_ui_api_v1.py     |    5 +-
 .../phaseL/ui/tests/test_readiness_kernel_v1.py    |  171 +-
 .../ui/tests/test_refinement_state_ui_api_v1.py    |    4 +-
 .../tests/test_reliability_issue_create_ui_v1.py   |    9 +-
 .../phaseL/ui/tests/test_reliability_ledger_v1.py  |    5 +-
 .../phaseL/ui/tests/test_value_state_ui_api_v1.py  |    4 +-
 .../phaseL/ui_api/kernel_operator_shell_v1.py      |   23 +-
 .../phaseL/ui_api/readiness_kernel_v1.py           |  101 +-
 docs/aegis_chatgpt_control_plane_audit.md          |    2 +-
 docs/aegis_daily_research_cadence.md               |   10 +-
 docs/aegis_hostile_audit_grade.md                  |   10 +-
 docs/aegis_hostile_audit_grade_improvement_plan.md |   18 +-
 docs/aegis_lite_event_awareness.md                 |    2 +-
 docs/aegis_lite_manual_paper_readiness.md          |   20 +-
 docs/aegis_lite_pivot.md                           |    4 +-
 docs/aegis_lite_research_architecture.md           |    2 +-
 .../aegis_lite_research_functionality_footprint.md |    4 +-
 docs/aegis_lite_research_high_level_functions.md   |   12 +-
 docs/aegis_lite_timer_model.md                     |    8 +-
 docs/aegis_market_context_layer.md                 |   47 +
 docs/aegis_noon_preflight_rehearsal.md             |   20 +-
 docs/aegis_operator_daily_control_plane.md         |    2 +-
 docs/aegis_operator_use_completeness_audit.md      |    4 +-
 .../C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json         |  619 +-
 .../C2_CAPITAL_AUTHORITY_POLICY_V1.json            |  149 +-
 .../C2_ECONOMIC_STATE_MANIFESTS_V1.json            |  101 +-
 .../C2_EXECUTION_BUILD_MANIFESTS_V1.json           |  225 +-
 .../02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json  |  295 +-
 .../02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json    |    8 +-
 .../C2/ACCOUNTING/accounting_nav.v2.schema.json    |  117 +-
 .../cash_ledger_snapshot.v1.schema.json            |  289 +-
 .../ECONOMIC/economic_state_package.v1.schema.json |  116 +-
 .../C2/POSITIONS/positions_snapshot.v5.schema.json |  639 +-
 .../position_lifecycle_snapshot.v2.schema.json     |  210 +-
 .../aegis_chatgpt_control_packet.v1.schema.json    |   10 +
 .../REPORTS/aegis_lite_eod_report.v1.schema.json   |  868 +-
 .../candidate_generation_manifest.v1.schema.json   |  231 +-
 .../REPORTS/event_market_snapshot.v1.schema.json   |   18 +
 .../manual_execution_receipt.v1.schema.json        |  353 +-
 .../operator_execution_queue.v1.schema.json        |    2 +
 .../risk_definition_contract.v1.schema.json        |  458 +-
 .../C2/REPORTS/target_day_admission.v1.schema.json |    2 +-
 .../C2/RISK/c2_risk_policy_registry.v1.schema.json |  239 +-
 ops/runtime/runtime_manifest.yaml                  |   72 +
 ops/runtime/supervisor.py                          |   93 +-
 ops/systemd/user/aegis-lite-eod-report-v1.service  |    3 +-
 ops/systemd/user/aegis-lite-eod-report-v1.timer    |    5 +-
 .../user/aegis-noon-preflight-rehearsal-v1.service |    2 +
 ops/tools/aegis_submit_enforcement_v1.py           |   91 +-
 ops/tools/audit_research_dataset_bindings_v1.py    |   70 +-
 ops/tools/build_aegis_audit_handoff_v1.py          |  220 +-
 ops/tools/build_ai_eod_feedback_review_v1.py       |   35 +-
 ops/tools/build_event_market_snapshot_v1.py        |  276 +
 ops/tools/run_accounting_nav_v2_day_v1.py          |   19 +
 ops/tools/run_aegis_event_monitor_v1.py            |   69 +
 ops/tools/run_aegis_lite_eod_pipeline_v1.py        |  113 +-
 ops/tools/run_aegis_noon_preflight_rehearsal_v1.py |  745 +-
 ops/tools/run_aegis_operator_projection_v1.py      |   46 +-
 ops/tools/run_c2_market_data_preopen_prepare_v1.py |   32 +-
 ..._execution_package_from_authorized_intent_v1.py |  172 +-
 ops/tools/run_intent_arbitration_v1.py             |   48 +-
 ops/tools/run_market_session_intent_engine_v1.py   |   38 +-
 ops/tools/run_portfolio_activation_gate_v1.py      |  214 +-
 ops/tools/run_portfolio_scoring_v1.py              |   96 +-
 ops/tools/run_position_lifecycle_snapshot_v2.py    |   14 +
 ops/tools/run_research_test_queue_v1.py            |   34 +
 ops/tools/run_risk_definition_contract_v1.py       |    5 +-
 ops/tools/run_sleeve_evaluation_kernel_v1.py       |  546 +-
 package.json                                       |  192 +-
 129 files changed, 29053 insertions(+), 4246 deletions(-)
```

## Name Status Snapshot

```text
M	.gitignore
M	constellation_2/common/advisory/execution_package_builder_from_execution_intent_v1.py
M	constellation_2/common/aegis_chatgpt_control_packet_v1.py
M	constellation_2/common/aegis_eod_artifact_contract_v1.py
M	constellation_2/common/aegis_event_monitoring_v1.py
M	constellation_2/common/aegis_lite_eod_v1.py
M	constellation_2/common/aegis_lite_manual_feedback_v1.py
M	constellation_2/common/aegis_lite_operating_status_v1.py
M	constellation_2/common/aegis_lite_promoted_candidates_v1.py
M	constellation_2/common/aegis_market_context_v1.py
M	constellation_2/common/candidate_observability_v1.py
M	constellation_2/common/day_activation_authority_v1.py
M	constellation_2/common/economic_state_authority_v1.py
M	constellation_2/common/engine_universe_v1.py
M	constellation_2/common/execution_build_authority_v1.py
M	constellation_2/common/global_context_authority_v1.py
M	constellation_2/common/tests/test_aegis_chatgpt_control_packet_v1.py
M	constellation_2/common/tests/test_aegis_control_plane_v1.py
M	constellation_2/common/tests/test_aegis_day_run_ledger_v1.py
M	constellation_2/common/tests/test_aegis_eod_artifact_contract_v1.py
M	constellation_2/common/tests/test_aegis_lite_event_awareness_v1.py
M	constellation_2/common/tests/test_aegis_lite_operational_spine_v1.py
M	constellation_2/common/tests/test_aegis_market_context_v1.py
M	constellation_2/common/tests/test_aegis_noon_preflight_rehearsal_v1.py
M	constellation_2/common/tests/test_aegis_operator_command_contracts_v1.py
M	constellation_2/common/tests/test_aegis_operator_state_v1.py
M	constellation_2/common/tests/test_aegis_portfolio_decision_pipeline_v1.py
M	constellation_2/common/tests/test_aegis_truth_integrity_layer_v1.py
M	constellation_2/common/tests/test_candidate_observability_v1.py
M	constellation_2/common/tests/test_defensive_tail_required_inputs_bridge_v1.py
M	constellation_2/common/tests/test_execution_build_authority_v1.py
M	constellation_2/common/tests/test_sleeve_evaluation_and_intent_arbitration_v1.py
M	constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py
M	constellation_2/phaseF/positions/run/run_positions_snapshot_day_v5.py
M	constellation_2/phaseG/allocation/run/run_allocation_day_v2.py
M	constellation_2/phaseI/defensive_tail/run/run_defensive_tail_intents_day_v1.py
M	constellation_2/phaseI/event_dislocation/run/run_event_dislocation_intents_day_v1.py
M	constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py
M	constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py
M	constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py
M	constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py
M	constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py
M	constellation_2/phaseL/ui/static/aegis.css
M	constellation_2/phaseL/ui/static/index.html
M	constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js
M	constellation_2/phaseL/ui/static/operator_shell/fixtures/mockData.js
M	constellation_2/phaseL/ui/static/operator_shell/main.js
M	constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js
M	constellation_2/phaseL/ui/static/operator_shell/pages/index.js
M	constellation_2/phaseL/ui/tests/test_aegis_post_pivot_navigation_v1.py
M	constellation_2/phaseL/ui/tests/test_aegis_readiness_kernel_ledger_authority_v1.py
M	constellation_2/phaseL/ui/tests/test_aegis_ui_projection_service_v1.py
M	constellation_2/phaseL/ui/tests/test_capital_ui_api_v1.py
M	constellation_2/phaseL/ui/tests/test_configuration_workflow_v1.py
M	constellation_2/phaseL/ui/tests/test_financial_state_ui_api_v1.py
M	constellation_2/phaseL/ui/tests/test_operator_workflow_layer_v1.py
M	constellation_2/phaseL/ui/tests/test_opportunity_state_ui_api_v1.py
M	constellation_2/phaseL/ui/tests/test_outcome_state_ui_api_v1.py
M	constellation_2/phaseL/ui/tests/test_performance_cockpit_route_v1.py
M	constellation_2/phaseL/ui/tests/test_policy_evolution_state_ui_api_v1.py
M	constellation_2/phaseL/ui/tests/test_product_summary_ui_api_v1.py
M	constellation_2/phaseL/ui/tests/test_readiness_kernel_v1.py
M	constellation_2/phaseL/ui/tests/test_refinement_state_ui_api_v1.py
M	constellation_2/phaseL/ui/tests/test_reliability_issue_create_ui_v1.py
M	constellation_2/phaseL/ui/tests/test_reliability_ledger_v1.py
M	constellation_2/phaseL/ui/tests/test_value_state_ui_api_v1.py
M	constellation_2/phaseL/ui_api/kernel_operator_shell_v1.py
M	constellation_2/phaseL/ui_api/readiness_kernel_v1.py
M	docs/aegis_chatgpt_control_plane_audit.md
M	docs/aegis_daily_research_cadence.md
M	docs/aegis_hostile_audit_grade.md
M	docs/aegis_hostile_audit_grade_improvement_plan.md
M	docs/aegis_lite_event_awareness.md
M	docs/aegis_lite_manual_paper_readiness.md
M	docs/aegis_lite_pivot.md
M	docs/aegis_lite_research_architecture.md
M	docs/aegis_lite_research_functionality_footprint.md
M	docs/aegis_lite_research_high_level_functions.md
M	docs/aegis_lite_timer_model.md
M	docs/aegis_market_context_layer.md
M	docs/aegis_noon_preflight_rehearsal.md
M	docs/aegis_operator_daily_control_plane.md
M	docs/aegis_operator_use_completeness_audit.md
M	governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json
M	governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json
M	governance/02_REGISTRIES/C2_ECONOMIC_STATE_MANIFESTS_V1.json
M	governance/02_REGISTRIES/C2_EXECUTION_BUILD_MANIFESTS_V1.json
M	governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json
M	governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json
M	governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v2.schema.json
M	governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/ECONOMIC/economic_state_package.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json
M	governance/04_DATA/SCHEMAS/C2/POSITION_LIFECYCLE/position_lifecycle_snapshot.v2.schema.json
M	governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_chatgpt_control_packet.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_lite_eod_report.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_generation_manifest.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/REPORTS/event_market_snapshot.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/REPORTS/manual_execution_receipt.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/REPORTS/operator_execution_queue.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/REPORTS/risk_definition_contract.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_admission.v1.schema.json
M	governance/04_DATA/SCHEMAS/C2/RISK/c2_risk_policy_registry.v1.schema.json
M	ops/runtime/runtime_manifest.yaml
M	ops/runtime/supervisor.py
M	ops/systemd/user/aegis-lite-eod-report-v1.service
M	ops/systemd/user/aegis-lite-eod-report-v1.timer
M	ops/systemd/user/aegis-noon-preflight-rehearsal-v1.service
M	ops/tools/aegis_submit_enforcement_v1.py
M	ops/tools/audit_research_dataset_bindings_v1.py
M	ops/tools/build_aegis_audit_handoff_v1.py
M	ops/tools/build_ai_eod_feedback_review_v1.py
M	ops/tools/build_event_market_snapshot_v1.py
M	ops/tools/run_accounting_nav_v2_day_v1.py
M	ops/tools/run_aegis_event_monitor_v1.py
M	ops/tools/run_aegis_lite_eod_pipeline_v1.py
M	ops/tools/run_aegis_noon_preflight_rehearsal_v1.py
M	ops/tools/run_aegis_operator_projection_v1.py
M	ops/tools/run_c2_market_data_preopen_prepare_v1.py
M	ops/tools/run_execution_package_from_authorized_intent_v1.py
M	ops/tools/run_intent_arbitration_v1.py
M	ops/tools/run_market_session_intent_engine_v1.py
M	ops/tools/run_portfolio_activation_gate_v1.py
M	ops/tools/run_portfolio_scoring_v1.py
M	ops/tools/run_position_lifecycle_snapshot_v2.py
M	ops/tools/run_research_test_queue_v1.py
M	ops/tools/run_risk_definition_contract_v1.py
M	ops/tools/run_sleeve_evaluation_kernel_v1.py
M	package.json
```

## Full Changed File Inventory

| Category | Git status | Path | Recommendation | Reason |
|---|---:|---|---|---|
| intentional production change | `M` | `.gitignore` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/advisory/execution_package_builder_from_execution_intent_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/aegis_chatgpt_control_packet_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/aegis_eod_artifact_contract_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/aegis_event_monitoring_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/aegis_lite_eod_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/aegis_lite_manual_feedback_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/aegis_lite_operating_status_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/aegis_lite_promoted_candidates_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/aegis_market_context_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/candidate_observability_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/day_activation_authority_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/economic_state_authority_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/engine_universe_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/execution_build_authority_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/common/global_context_authority_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_chatgpt_control_packet_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_control_plane_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_day_run_ledger_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_eod_artifact_contract_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_lite_event_awareness_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_lite_operational_spine_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_market_context_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_noon_preflight_rehearsal_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_operator_command_contracts_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_operator_state_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_portfolio_decision_pipeline_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_aegis_truth_integrity_layer_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_candidate_observability_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_defensive_tail_required_inputs_bridge_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_execution_build_authority_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/common/tests/test_sleeve_evaluation_and_intent_arbitration_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional production change | `M` | `constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseF/positions/run/run_positions_snapshot_day_v5.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseG/allocation/run/run_allocation_day_v2.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseI/defensive_tail/run/run_defensive_tail_intents_day_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseI/event_dislocation/run/run_event_dislocation_intents_day_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseI/mean_reversion/run/run_mean_reversion_intents_day_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseI/trend_eq_primary/run/run_trend_eq_primary_intents_day_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseL/ui/static/aegis.css` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseL/ui/static/index.html` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseL/ui/static/operator_shell/fixtures/mockData.js` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseL/ui/static/operator_shell/main.js` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseL/ui/static/operator_shell/pages/index.js` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_aegis_post_pivot_navigation_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_aegis_readiness_kernel_ledger_authority_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_aegis_ui_projection_service_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_capital_ui_api_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_configuration_workflow_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_financial_state_ui_api_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_operator_workflow_layer_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_opportunity_state_ui_api_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_outcome_state_ui_api_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_performance_cockpit_route_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_policy_evolution_state_ui_api_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_product_summary_ui_api_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_readiness_kernel_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_refinement_state_ui_api_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_reliability_issue_create_ui_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_reliability_ledger_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `M` | `constellation_2/phaseL/ui/tests/test_value_state_ui_api_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional production change | `M` | `constellation_2/phaseL/ui_api/kernel_operator_shell_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `constellation_2/phaseL/ui_api/readiness_kernel_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_chatgpt_control_plane_audit.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_daily_research_cadence.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_hostile_audit_grade.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_hostile_audit_grade_improvement_plan.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_lite_event_awareness.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_lite_manual_paper_readiness.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_lite_pivot.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_lite_research_architecture.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_lite_research_functionality_footprint.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_lite_research_high_level_functions.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_lite_timer_model.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_market_context_layer.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_noon_preflight_rehearsal.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_operator_daily_control_plane.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `docs/aegis_operator_use_completeness_audit.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/02_REGISTRIES/C2_ECONOMIC_STATE_MANIFESTS_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/02_REGISTRIES/C2_EXECUTION_BUILD_MANIFESTS_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/02_REGISTRIES/C2_RISK_POLICY_REGISTRY_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v2.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/ECONOMIC/economic_state_package.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/POSITION_LIFECYCLE/position_lifecycle_snapshot.v2.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_chatgpt_control_packet.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_lite_eod_report.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_generation_manifest.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/REPORTS/event_market_snapshot.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/REPORTS/manual_execution_receipt.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_execution_queue.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/REPORTS/risk_definition_contract.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_admission.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `governance/04_DATA/SCHEMAS/C2/RISK/c2_risk_policy_registry.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/runtime/runtime_manifest.yaml` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/runtime/supervisor.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/systemd/user/aegis-lite-eod-report-v1.service` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/systemd/user/aegis-lite-eod-report-v1.timer` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/systemd/user/aegis-noon-preflight-rehearsal-v1.service` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/aegis_submit_enforcement_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/audit_research_dataset_bindings_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/build_aegis_audit_handoff_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/build_ai_eod_feedback_review_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/build_event_market_snapshot_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_accounting_nav_v2_day_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_aegis_event_monitor_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_aegis_lite_eod_pipeline_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_aegis_noon_preflight_rehearsal_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_aegis_operator_projection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_c2_market_data_preopen_prepare_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_execution_package_from_authorized_intent_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_intent_arbitration_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_market_session_intent_engine_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_portfolio_activation_gate_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_portfolio_scoring_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_position_lifecycle_snapshot_v2.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_research_test_queue_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_risk_definition_contract_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `ops/tools/run_sleeve_evaluation_kernel_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `M` | `package.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `constellation_2/common/aegis_lite_schedule_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `constellation_2/common/ranked_symbol_universe_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional test change | `??` | `constellation_2/common/tests/test_account_economic_state_authority_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_automation_ai_inventory_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_candidate_decision_support_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_candidate_generation_diagnostics_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_candidate_portfolio_selection_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_candidate_readiness_repair_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_candidate_review_workflow_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_data_registry_sleeve_readiness_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_data_remediation_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_dependency_map_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_eod_opportunity_outcome_report_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_event_sourced_runtime_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_final_eod_orchestrator_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_market_candidate_architecture_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_market_data_inputs_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_noon_preflight_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_operational_maturity_hardening_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_operator_command_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_operator_state_snapshot_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_position_management_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_readiness_burnin_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_runtime_recursion_guard_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_runtime_truth_kernel_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_selected_intent_promotion_contract_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_sleeve_universe_validation_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_aegis_thesis_graph_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_allowed_symbol_source_authority_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_batch_selected_pointer_current_truth_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_canonical_symbol_universe_resolver_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_canonical_universe_authority_invariants_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_canonical_universe_discovery_restoration_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_engine_activity_authorization_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_exposure_intent_paper_submission_package_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_manual_capture_operational_invariants_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_market_data_freshness_policy_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_operator_state_snapshot_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_paper_capital_authority_policy_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_paper_trade_construction_invariants_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_portfolio_gate_candidate_report_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_readiness_domain_evaluation_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_regime_bucket_candidate_ranking_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_trade_candidate_contract_invariants_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/common/tests/test_trade_lifecycle_case_invariants_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional production change | `??` | `constellation_2/phaseL/ui/docs/manual_capture_modal_qa_v1.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `constellation_2/phaseL/ui/docs/research_pipeline_operator_ux_v1.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `constellation_2/phaseL/ui/docs/ui_sturdiness_contract_v1.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/operator_shell_test_sources.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_candidate_portfolio_selection_ui_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_command_contracts_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_domain_certification_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_intent_confidence_lifecycle_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_journal_timeline_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_operator_cockpit_ui_api_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_operator_shell_layout_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_position_management_ui_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_research_pipeline_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_runtime_timeline_workspace_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_thesis_graph_ui_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_aegis_workflow_operator_ui_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_candidate_pipeline_observability_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_hypotheses_primary_ux_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_operator_state_ui_projection_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_research_console_operator_ux_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_research_lab_readonly_integration_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_research_os_status_ui_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_research_run_ledger_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `constellation_2/phaseL/ui/tests/test_research_ui_live_validation_harness_v1.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional production change | `??` | `docs/aegis/adr/0001-dual-kernel-architecture.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/adr/0002-evidence-metrics-interpretation-recommendation-approval.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/adr/0003-ai-output-governance.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/adr/0004-human-approval-required-for-adaptive-governance.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/adr/0005-market-data-candidate-certification-semantics.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/adr/0006-runtime-schedule-event-ledger.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/eod_provider_credentials_runbook.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/maintainability_hardening_worktree_audit_2026-05-23.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/operational_invariants_v1.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/operator_ui_projection_audit.v1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/operator_ui_projection_audit.v1.txt` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/proposals/one_primary_per_regime_bucket_ranked_selection_v1.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/runtime_timeline_ui_service_runbook_v1.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `docs/aegis/trade_capture_archaeology_v1.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/02_REGISTRIES/AEGIS_DOMAIN_SOURCE_REGISTRY_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/02_REGISTRIES/AEGIS_EVIDENCE_EVENT_SCHEMA_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/02_REGISTRIES/AEGIS_PRODUCER_CONTRACTS_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/02_REGISTRIES/AEGIS_RUNTIME_EVALUATION_SCHEMA_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/02_REGISTRIES/DEPRECATED_SYMBOL_SOURCES_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/02_REGISTRIES/DEPRECATED_TRADE_CAPTURE_SOURCES_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/02_REGISTRIES/ENGINE_UNIVERSE_POLICY_V1.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_consumption_audit.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_identity_set.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_intent_plane.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/canonical_market_data_refresh_v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/canonical_universe_authority.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/canonical_universe_discovery_report.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/capital_authority_allocation.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/domain_certification.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/domain_snapshot.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/engine_activity_authorization.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/market_data_inputs.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/market_data_readiness.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trade_construction.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/ranked_symbol_universe.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/readiness_domain_evaluation.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/schedule_event_ledger.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/trade_candidate_contract.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/trade_lifecycle_case.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/trade_lifecycle_event.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/REPORTS/trade_ticket_lineage.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/RISK/engine_universe_candidate_basis.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/RISK/engine_universe_policy_registry.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/04_DATA/SCHEMAS/C2/RISK/engine_universe_resolution.v1.schema.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/05_CONTRACTS/C2/cross_asset_trend_stop_policy_v1.contract.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `governance/05_CONTRACTS/C2/equity_sleeve_stop_policy_v1.contract.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/account_economic_state_authority_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_governance/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_governance/adaptive_governance_kernel_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_governance/cross_sleeve_analysis_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_governance/evidence_model_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_governance/failure_analysis_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_governance/regime_context_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_governance/research_memory_graph_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_governance/research_queue_optimizer_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_governance/sleeve_performance_analytics_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/adaptive_research_sleeve_governance_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/capital_allocation_intelligence_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/common_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/cross_sleeve_interaction_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/event_interpretation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/failure_analysis_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/regime_detection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/research_memory_graph_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/adaptive_intelligence/research_queue_optimizer_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/blocker_state_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_decision_support_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_generation_diagnostics_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_identity_set_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_intent_plane_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_lifecycle_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_manual_capture_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_portfolio_selection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_promotion_map_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_ranking_explanation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_review_ledger_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/candidate_snapshot_plane_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/canonical_operator_state_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/capital_authority_allocation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/data_registry_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/data_remediation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/day_activation_package_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/decision_ledger_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/domain_certification_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/domain_repair_orchestrator_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/domain_source_builders_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/domain_source_registry_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/engine_activity_authorization_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/eod_opportunity_outcome_report_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/event_append_transaction_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/event_regime_trigger_evaluator_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/event_regime_trigger_registry_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/evidence_event_store_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/evidence_event_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/evolution_engine_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/final_eod_orchestrator_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/global_context_package_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/intelligence_common_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/intelligence_governance/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/intelligence_governance/ai_output_governance_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/intelligence_governance/evidence_chain_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/intelligence_governance/metric_rules_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/intelligence_governance_kernel_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/journal/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/journal/journal_event_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/manual_intent_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/market_calendar/session_calendar_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/market_data/freshness_policy_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/market_data/market_data_mode_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/market_data/market_data_provider_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/market_data/market_data_snapshot_plane_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/market_data/symbol_alias_registry_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/market_data/symbol_map_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/market_data_inputs_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operational_maturity_hardening_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_action_command_contracts_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_command_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_state/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_state/canonical_operator_state_builder_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_state/current_operator_truth_resolver_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_state/manual_capture_record_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_state/runtime_timeline_projection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_state/schedule_event_ledger_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_state/trade_candidate_contract_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/operator_state/trade_candidate_projection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/portfolio_gate_candidate_report_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/position_management_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/producer_contract_reports_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/producer_contract_validator_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/producer_contracts_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/producer_event_bridge_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/pure_runtime_evaluator_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/regime_bucket_candidate_ranking_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/regime_outcome_memory_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/repair_center_projection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/repair_orchestrator_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_hypothesis_classification_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/edge_lab_projection.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/event_earnings_data_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/evidence_chain_view.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/hypothesis_priority_engine_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/research_console_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/research_data_readiness_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/research_lab_routes.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/research_pipeline_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/research_run_ledger_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/research_store_reader.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab/research_store_refs_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_lab_execution_loop_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/research_prioritizer_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/risk_definition_contract_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/risk_governance_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/run_context_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/runtime_evaluation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/runtime_truth_kernel_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/selected_intent_promotion_contract_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/sleeve_attribution_engine_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/sleeve_challenger_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/sleeve_input_contracts_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/sleeve_readiness_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/sleeve_universe_validation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/submit_boundary_precheck_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/target_day_admission_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/thesis_graph/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/thesis_graph/thesis_graph_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/trade_lifecycle/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/trade_lifecycle/captured_ticket_history_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/trade_lifecycle/paper_trade_construction_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/trade_lifecycle/readiness_domain_evaluation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/trade_lifecycle/trade_case_projection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/trade_lifecycle/trade_lifecycle_case_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/trade_lifecycle/trade_lifecycle_event_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/trade_lifecycle/trade_ticket_projection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/trade_ticket_lineage_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/universe/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/universe/canonical_symbol_universe_resolver_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/universe/canonical_universe_authority_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/aegis/universe/canonical_universe_discovery_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/config/aegis_paper_trade_construction_defaults.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/config/aegis_runtime_universe.json` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/systemd/user/aegis-market-data-final-eod-v1.service` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/systemd/user/aegis-market-data-final-eod-v1.timer` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/systemd/user/aegis-market-data-refresh-v1.service` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/systemd/user/aegis-market-data-refresh-v1.timer` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/aegis_operator_clickthrough_qa_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_account_economic_state_authority_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_audit_bundle_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_canonical_operator_state_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_data_registry_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_dependency_map_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_journal_timeline_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_market_data_inputs_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_operational_maturity_hardening_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_research_plan_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_sleeve_input_contracts_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_sleeve_readiness_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_sleeve_universe_validation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_symbol_map_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_aegis_thesis_graph_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_candidate_identity_set_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_candidate_intent_plane_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_candidate_promotion_map_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_canonical_symbol_universe_authority_report_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_capital_authority_allocation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_day_activation_package_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_domain_certification_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_domain_source_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_engine_activity_authorization_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_global_context_package_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_hypothesis_priority_report_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_manual_capture_lifecycle_audit_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_operator_state_snapshot_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_regime_bucket_candidate_ranking_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_risk_definition_contract_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_schedule_event_ledger_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_submit_boundary_precheck_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/build_target_day_admission_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/capture_manual_trade_receipt_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/classify_aegis_research_hypothesis_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/correct_aegis_candidate_decision_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/correct_aegis_position_event_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/diff_aegis_runtime_state_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/import_governed_event_earnings_data_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/manage_us_equities_eod_source_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/promote_aegis_selected_intent_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/record_aegis_candidate_decision_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/record_aegis_candidate_review_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/record_aegis_governance_decision_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/record_aegis_intelligence_approval_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/record_aegis_operator_candidate_decision_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/record_aegis_position_risk_plan_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/record_aegis_research_review_decision_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/record_aegis_stop_event_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/refresh_aegis_market_data_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/repair_aegis_candidate_readiness_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/repair_aegis_runtime_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/replay_aegis_runtime_state_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/replay_aegis_runtime_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/research_hypotheses_clickthrough_qa_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/research_ui_live_validation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_adaptive_governance_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_candidate_portfolio_selection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_candidate_ranking_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_capital_allocation_intelligence_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_cross_sleeve_analysis_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_cross_sleeve_interaction_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_data_remediation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_event_interpretation_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_event_regime_trigger_evaluator_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_event_regime_triggered_sleeve_run_audit_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_event_sleeve_activation_audit_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_evolution_engine_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_failure_analysis_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_final_eod_certification_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_intelligence_governance_kernel_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_intraday_sleeves_now_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_readiness_burnin_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_regime_context_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_regime_detection_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_regime_outcome_memory_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_research_hypothesis_classification_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_research_lab_execution_loop_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_research_memory_graph_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_research_pipeline_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_research_prioritizer_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_research_queue_optimizer_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_research_test_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_risk_governance_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_runtime_truth_kernel_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_sleeve_attribution_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_sleeve_challenger_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_sleeve_performance_analytics_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_aegis_triggered_sleeves_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_c2_canonical_market_data_refresh_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_domain_repair_orchestrator_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_exposure_intent_paper_submission_package_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_paper_trade_construction_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_ranked_engine_candidate_basis_day_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_ranked_symbol_universe_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_challenger_comparison_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_challenger_evidence_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_challenger_track_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_human_review_dossier_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet20_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet21_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet22_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet23_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet24_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet25_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet26_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet27_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet28_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_research_lab_packet29_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/run_trade_lifecycle_case_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/show_aegis_data_remediation_ledger_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/triage_aegis_hypothesis_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/update_aegis_candidate_outcomes_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/validate_all_sleeve_ticket_lineage_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/validate_domain_source_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_automation_ai_inventory_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_candidate_generation_diagnostics_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_candidate_review_ledger_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_daily_operator_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_eod_intelligence_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_eow_intelligence_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_feature_completion_audit_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_high_roi_missing_items_review_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_operational_readiness_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_operator_brief_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_operator_inbox_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_runtime_evidence_completion_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_aegis_strategic_capability_review_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_alert_transport_proof_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_broker_lifecycle_proof_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_event_validity_evidence_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_manual_execution_receipt_evidence_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_manual_intent_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `ops/tools/write_portfolio_gate_candidate_report_v1.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| accidental/stale change | `??` | `research_lab/.gitignore` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/README.md` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/__init__.py` | review before commit | not confidently tied to this hardening pass |
| secret/config risk | `??` | `research_lab/config/providers.example.env` | ignore | local credentials/config must stay out of git |
| intentional production change | `??` | `research_lab/config/providers.example.yaml` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| generated artifact that should not be committed | `??` | `research_lab/contracts/attribution_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/audit_event.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/backtest_plan.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/backtest_result.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/bar_policy_daily_ohlcv_alpha_vantage_v1.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/bar_policy_daily_ohlcv_local_csv_v1.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/bar_policy_daily_ohlcv_stooq_v1.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/bar_policy_daily_ohlcv_tiingo_v1.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/bar_policy_daily_ohlcv_v1.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/breadth_event.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/breadth_snapshot.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/candidate.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/candidate_batch.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/candidate_outcome_summary.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/challenger_comparison_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/challenger_evidence_batch.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/challenger_research_track.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/challenger_variant.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/cost_model_snapshot.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/dataset_snapshot.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/event_cluster.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/event_family.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/event_observation.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/event_study_result.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/evidence_inventory.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/evidence_package.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/expectancy_drift_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/human_review_decision.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/human_review_dossier.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/hypothesis_intake_batch.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/hypothesis_intake_decision.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/hypothesis_proposal.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/hypothesis_proposal_batch.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/hypothesis_proposal_review.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/hypothesis_proposal_review_batch.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/intent_candidate.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/longitudinal_candidate_run.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/macro_event.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/macro_event_calendar_snapshot.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/observation_candidate.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/observation_candidate_batch.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/observation_cluster.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/observation_cluster_batch.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/operator_decision.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/outcome_record.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/paper_trial.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/paper_trial_due_outcome.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/paper_trial_inventory.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/paper_trial_observation.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/paper_trial_operations_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/paper_trial_outcome.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/paper_trial_proposal.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/paper_trial_review.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/proposal_priority_score.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/ranking_quality_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/regime_fragility_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/regime_snapshot.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/research_backlog_priority_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/research_hypothesis.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/research_intake_dossier.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/research_os_status_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/research_plan.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/research_readiness_assessment.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/research_store_integrity_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/run_record.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/sleeve_challenge.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/sleeve_comparison_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/sleeve_definition.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/sleeve_health_snapshot.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/sleeve_learning_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/sleeve_review.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/sleeve_stability_report.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/sleeve_version.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| generated artifact that should not be committed | `??` | `research_lab/contracts/universe_snapshot.schema.json` | ignore/remove | runtime/test output, cache, or local fixture |
| intentional production change | `??` | `research_lab/docs/manual_csv_acquisition.md` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| accidental/stale change | `??` | `research_lab/local_data/ohlcv/README.md` | review before commit | not confidently tied to this hardening pass |
| generated artifact that should not be committed | `??` | `research_lab/local_data/ohlcv/csv_readiness_report.json` | ignore/remove | runtime/test output, cache, or local fixture |
| accidental/stale change | `??` | `research_lab/local_data/ohlcv/download_checklist.md` | review before commit | not confidently tied to this hardening pass |
| generated artifact that should not be committed | `??` | `research_lab/local_data/ohlcv/expected_csv_files.json` | ignore/remove | runtime/test output, cache, or local fixture |
| accidental/stale change | `??` | `research_lab/local_data/ohlcv/templates/.gitkeep` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/local_data/ohlcv/templates/SPY_template.csv` | review before commit | not confidently tied to this hardening pass |
| intentional production change | `??` | `research_lab/providers/stooq_symbol_map.yaml` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| accidental/stale change | `??` | `research_lab/requirements-research.lock` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/requirements-research.txt` | review before commit | not confidently tied to this hardening pass |
| intentional production change | `??` | `research_lab/src/research_lab/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/acquisition/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/acquisition/csv_readiness.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/acquisition/csv_staging.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/acquisition/csv_template.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/acquisition/download_guide.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/acquisition/expected_files.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/acquisition/manual_sources.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/audit/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/audit/audit_log.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/backtests/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/backtests/backtest_artifacts.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/backtests/backtest_plan.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/backtests/backtest_plan_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/backtests/holding_period_backtester.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/backtests/performance_metrics.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/backtests/signal_rules.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/bars/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/bars/bar_policy.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/bars/canonical_daily_bars.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/breadth/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/breadth/breadth_builder.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/breadth/breadth_events.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/breadth/breadth_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/breadth/breadth_snapshot.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/candidates/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/candidates/candidate_batch.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/candidates/candidate_generator.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/candidates/candidate_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/candidates/candidate_scoring.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/candidates/operator_decision.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/candidates/outcome_record.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/challengers/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/challengers/challenger_comparison.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/challengers/challenger_evidence.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/challengers/challenger_track.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/challengers/challenger_variant.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/challengers/human_review_decision.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/challengers/human_review_dossier.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/cli.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/config/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/config/provider_config.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/config/runtime_config.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/config/secrets.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/contracts/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/contracts/schemas.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/costs/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/costs/cost_adjustments.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/costs/cost_model_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/costs/cost_model_snapshot.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/datasets/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/datasets/bulk_local_csv_import.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/datasets/coverage_report.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/datasets/csv_validation.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/datasets/dataset_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/datasets/dataset_snapshot.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/datasets/ohlcv_builder.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/datasets/validation.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/event_intake/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/event_intake/event_cluster.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/event_intake/event_family.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/event_intake/event_hypothesis_templates.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/event_intake/event_intake_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/event_intake/event_observation.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/event_intake/hypothesis_proposal.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/event_intake/intent_candidate.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/events/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/events/event_definitions.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/events/event_extractor.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/evidence/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/evidence/evidence_comparison.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/evidence/evidence_package.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/evidence/evidence_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/hypotheses/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/hypotheses/hypothesis_intake.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/hypotheses/hypothesis_intake_batch.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/hypotheses/hypothesis_proposal.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/hypotheses/hypothesis_proposal_batch.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/hypotheses/hypothesis_proposal_engine.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/hypotheses/hypothesis_proposal_review.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/hypotheses/hypothesis_proposal_review_batch.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/hypotheses/research_hypothesis.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/integrity/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/integrity/research_store_integrity.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/longitudinal/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/longitudinal/batch_scheduler.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/longitudinal/candidate_run.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/longitudinal/ranking_quality.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/longitudinal/sleeve_learning_report.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/macro_events/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/macro_events/macro_event_calendar.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/macro_events/macro_event_loader.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/macro_events/macro_event_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/observations/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/observations/observation_batch.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/observations/observation_candidate.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/observations/observation_cluster.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/observations/observation_cluster_batch.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/observations/observation_clustering.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/observations/observation_scanner.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/outcomes/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/outcomes/attribution_metrics.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/outcomes/attribution_report.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/outcomes/measurement_runner.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/outcomes/outcome_measurement.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/outcomes/outcome_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/due_outcomes.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/observation.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/operations.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/operations_report.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/outcome.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/paper_trial.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/paper_trial_proposal.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/paper_trial_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/paper_trials/review.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/portfolio/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/portfolio/backlog_priority.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/portfolio/evidence_inventory.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/portfolio/paper_trial_inventory.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/portfolio/sleeve_comparison.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/projections/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/projections/hypothesis_queue_projection.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/projections/operator_queue_projection.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/projections/projection_builder.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/projections/projection_contracts.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/projections/projection_health.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/projections/projection_store.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/projections/projection_validator.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/providers/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/providers/alpha_vantage_daily.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/providers/base.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/providers/factory.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/providers/local_csv_daily.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/providers/rate_limit.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/providers/stooq_daily.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/providers/tiingo_daily.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/providers/yfinance_daily.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/recovery/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/recovery/hypothesis_recovery.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/regimes/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/regimes/regime_builder.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/regimes/regime_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/regimes/regime_snapshot.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research/batch_research_plan.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research/research_plan.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research/research_plan_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research_intake/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research_intake/intake_dossier.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research_intake/intake_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research_intake/proposal_priority.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research_intake/proposal_queue.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research_intake/proposal_review.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/research_intake/readiness_assessment.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/runners/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/runners/batch_event_study_runner.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/runners/event_study_runner.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/runners/forward_returns.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/runtime/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/runtime/dependencies.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/sleeves/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/sleeves/sleeve_challenger.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/sleeves/sleeve_definition.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/sleeves/sleeve_health.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/sleeves/sleeve_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/sleeves/sleeve_review.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/stability/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/stability/expectancy_drift.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/stability/regime_fragility.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/stability/sleeve_stability.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/stability/stability_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/status/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/status/research_os_status.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/storage/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/storage/duckdb_query.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/storage/hashing.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/storage/manifest_io.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/storage/parquet_io.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/storage/paths.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/universes/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/universes/universe_builder.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/universes/universe_registry.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/workflows/__init__.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/workflows/first_api_workflow.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/workflows/first_dataset_workflow.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional production change | `??` | `research_lab/src/research_lab/workflows/first_event_study_workflow.py` | keep | source, schema, runbook, service, or ignore-rule change from hardening work |
| intentional test change | `??` | `research_lab/tests/_packet30b_helpers.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/conftest.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_aegis_reference_boundary.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_alpha_vantage_adapter_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_alpha_vantage_dataset_build.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_api_key_provider_diagnostics.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_attribution_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_backtest_costs.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_backtest_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_backtest_evidence_package.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_backtest_plan_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_batch_event_study_runner.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_bulk_local_csv_import.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_candidate_batch_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_candidate_batch_scheduler.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_candidate_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_candidate_generation.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_candidate_registry.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_candidate_scoring.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_canonical_bar_builder.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_challenger_comparison_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_challenger_evidence_batch.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_challenger_research_track.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_cost_adjustments.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_cost_model_snapshot_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_cross_sleeve_read_only.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_csv_prebuild_validation.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_csv_readiness_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_csv_staging_import.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_csv_template_generator.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_dataset_coverage_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_dataset_snapshot_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_dataset_snapshot_with_data.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_decision_quality_metrics.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_download_guide.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_event_cluster_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_event_definitions.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_event_family_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_event_intake_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_event_observation_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_event_study_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_event_study_runner.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_event_study_with_regime_and_costs.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_event_to_hypothesis_templates.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_evidence_comparison.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_evidence_inventory.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_evidence_package.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_expectancy_drift_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_expected_files_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_first_api_workflow.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_first_dataset_workflow.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_first_event_study_workflow.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_forward_returns.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_hashing_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_holding_period_backtester.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_human_review_decision.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_human_review_dossier.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_hypothesis_intake.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_hypothesis_proposal_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_hypothesis_proposal_queue.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_hypothesis_proposal_review.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_hypothesis_proposal_reviews.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_hypothesis_proposals.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_hypothesis_queue_projection.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_hypothesis_recovery.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_intent_candidate_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_local_csv_provider.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_longitudinal_candidate_run_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_longitudinal_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_multi_provider_build_failover.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_observation_candidates.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_observation_clusters.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_ohlcv_dataset_builder.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_ohlcv_quality_checks.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_operator_decision.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_outcome_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_outcome_measurement.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_outcome_registry.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_packet31_data_expansion.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_due_outcomes.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_inventory.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_observation.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_operations.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_operations_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_outcome.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_proposal.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_paper_trial_review.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_performance_metrics.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_projection_api_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_projection_contracts.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_projection_rebuild.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_projection_ui_no_empty_false_negative.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_projection_validation.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_proposal_priority_score.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_provider_config_cli.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_provider_secret_loading.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_provider_selection.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_ranking_quality.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_rate_limit_policy.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_regime_builder.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_regime_fragility_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_regime_snapshot_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_research_backlog_priority.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_research_intake_dossier.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_research_intake_routes.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_research_os_status_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_research_plan_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_research_readiness_assessment.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_research_runtime_dependencies.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_research_store_integrity.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_runtime_config.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_signal_rules.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_sleeve_challenger.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_sleeve_comparison.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_sleeve_definition_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_sleeve_governance_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_sleeve_health_snapshot.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_sleeve_learning_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_sleeve_review.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_sleeve_stability_report.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_sleeve_versioning.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_snapshot_registry.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_stability_determinism.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_stooq_adapter_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_tiingo_adapter_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_tiingo_dataset_build.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_tiingo_provider_diagnostics.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_universe_snapshot.py` | keep | test coverage for current Aegis hardening/UI contract |
| intentional test change | `??` | `research_lab/tests/test_yfinance_adapter_contract.py` | keep | test coverage for current Aegis hardening/UI contract |
| accidental/stale change | `??` | `research_lab/universes/credit_rates_research_v1.yaml` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/universes/etf_research_expanded_v1.yaml` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/universes/liquid_etf_core_v1.yaml` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/universes/local_etf_minimum_viable_v1.yaml` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/universes/local_etf_research_core_v1.yaml` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/universes/local_spy_sample_v1.yaml` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/universes/oil_energy_research_v1.yaml` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/universes/sector_research_v1.yaml` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/universes/smoke_etf_core_v1.yaml` | review before commit | not confidently tied to this hardening pass |
| accidental/stale change | `??` | `research_lab/universes/volatility_research_v1.yaml` | review before commit | not confidently tied to this hardening pass |
