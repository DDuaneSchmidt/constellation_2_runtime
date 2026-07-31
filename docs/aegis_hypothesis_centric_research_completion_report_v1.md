# Aegis Hypothesis-Centric Research Completion Report v1

## 1. What Was Implemented

Implemented a hypothesis-centered research portfolio layer above existing sleeves, candidate funnels, paper positions, evidence lineage integrity, validation, and scorecards. The sleeve remains an implementation; thesis and hypothesis now become the primary research truth units for validation, learning, and research allocation.

## 2. Files Changed

Core modules:
- `ops/aegis/research_mapping_rules_v1.py`
- `ops/aegis/research_thesis_registry_v1.py`
- `ops/aegis/hypothesis_registry_v1.py`
- `ops/aegis/hypothesis_state_machine_v1.py`
- `ops/aegis/research_allocation_score_v1.py`
- `ops/aegis/research_portfolio_manager_v1.py`
- `ops/aegis/research_portfolio_self_check_v1.py`

Runtime integrations:
- `ops/aegis/candidate_contracts_v1.py`
- `ops/aegis/paper_position_ledger_v1.py`
- `package.json`
- `aegis/modules/operator_portal/aegis.module.yaml`
- operator API/client/UI route files under `constellation_2/phaseL/ui/`

Docs and tests:
- `docs/aegis_hypothesis_centric_research_requirements_v1.md`
- `docs/aegis_hypothesis_centric_research_spec_v1.md`
- `docs/aegis_hypothesis_centric_research_design_v1.md`
- `docs/aegis_research_portfolio_management_v1.md`
- `docs/aegis_hypothesis_migration_report_v1.md`
- `constellation_2/common/tests/test_aegis_hypothesis_research_portfolio_v1.py`

## 3. New Artifacts

Generated daily artifacts:
- `reports/aegis_research_thesis_registry_v1/2026-05-31/thesis_registry.v1.json`
- `reports/aegis_hypothesis_registry_v1/2026-05-31/hypothesis_registry.v1.json`
- `reports/aegis_hypothesis_state_v1/2026-05-31/hypothesis_states.v1.json`
- `reports/aegis_research_portfolio_v1/2026-05-31/research_portfolio.v1.json`
- `reports/aegis_research_allocation_v1/2026-05-31/research_allocation.v1.json`
- `reports/aegis_research_portfolio_self_check_v1/2026-05-31/self_check.v1.json`

## 4. New NPM Commands

- `npm run aegis:research-portfolio`
- `npm run aegis:research-portfolio-self-check`

The self-check is wired into `npm run aegis:audit` before strict verified-runtime-graph validation.

## 5. Migration Results

Legacy sleeve mappings are deterministic and marked `LEGACY_INFERRED`. No migrated mapping is marked `SOURCE_DECLARED` unless the source artifact already declares both `hypothesis_id` and `thesis_id`.

Current migration maps 8 observed sleeve implementations into 8 hypotheses and 7 theses. The extra implementation is the intent simulator control sleeve, which is treated as workflow control evidence rather than investable edge proof.

## 6. Current Thesis Count

Current thesis count: 7.

## 7. Current Hypothesis Count

Current hypothesis count: 8.

## 8. Sleeve-to-Hypothesis Coverage

Sleeve-to-hypothesis coverage: 8/8 mapped, 100%.

## 9. Candidate-to-Hypothesis Coverage

Candidate-to-hypothesis coverage: 63/63 mapped, 100%.

## 10. Paper-Position-to-Hypothesis Coverage

Paper-position-to-hypothesis coverage: 36/36 mapped, 100%.

## 11. Allocation Recommendations Generated

8 deterministic allocation recommendations were generated. Current top recommendations include:
- `HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1`: `INCREASE`, score 41.1, reasons `HIGH_CANDIDATE_YIELD`, `LOW_VALIDATION_SAMPLE_COUNT`, `INSUFFICIENT_OUTCOME_HISTORY`.
- `HYP_EVENT_DISLOCATION_REPRICING_V1`: `REDUCE`, score 29.0, reasons `HIGH_CANDIDATE_YIELD`, `LOW_VALIDATION_SAMPLE_COUNT`.
- `HYP_CROSS_ASSET_TREND_PERSISTENCE_V1`: `INCREASE`, score 10.8, reasons `LOW_VALIDATION_SAMPLE_COUNT`, `INSUFFICIENT_OUTCOME_HISTORY`.

Recommendations are research effort recommendations only. They are not trade advice, broker instructions, or allocation orders.

## 12. Audit/Self-Check Results

Passed:
- `python3 -m py_compile` on all new Python files and touched candidate/position builders.
- `python3 -m pytest constellation_2/common/tests/test_aegis_hypothesis_research_portfolio_v1.py`: 10 passed.
- Combined focused tests with evidence lineage tests: 12 passed.
- `npm run aegis:research-portfolio-self-check`: ok true, failure_count 0.

`npm run aegis:audit` is expected to continue failing at the existing strict verified-runtime-graph blocker until pre-existing runtime truth evidence gaps are repaired. The new research portfolio self-check itself passes before that strict graph step.

## 13. Known Limitations

- Current mappings are mostly `LEGACY_INFERRED`; future producers should source-declare thesis/hypothesis IDs.
- Current validation depth remains weak: current-day validation-ready and validated hypothesis counts are 0.
- There are no closed paper outcomes in the current ledger, so outcome-based hypothesis proof remains incomplete.
- Allocation scoring is deterministic and explainable but intentionally simple; it should be calibrated after more closed outcomes and validation samples accumulate.

## 14. Recommended Next Improvement

Make hypothesis/thesis IDs source-declared in sleeve configuration and candidate generation inputs, then enforce `SOURCE_DECLARED` coverage targets separately from legacy-inferred coverage.
