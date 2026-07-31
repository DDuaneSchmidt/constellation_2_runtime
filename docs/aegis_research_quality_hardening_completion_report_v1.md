# Aegis Research Quality Hardening Completion Report v1

Generated: 2026-06-01

## 1. Files Created

- `docs/aegis_thesis_hypothesis_quality_review_v1.md`
- `docs/aegis_inactive_sleeve_explanation_review_v1.md`
- `docs/aegis_retirement_criteria_policy_v1.md`
- `docs/aegis_research_program_grouping_review_v1.md`
- `docs/aegis_regime_tag_taxonomy_v1.md`
- `docs/aegis_research_quality_hardening_completion_report_v1.md`
- `ops/aegis/research_quality_review_self_check_v1.py`
- `ops/tools/run_aegis_research_quality_review_self_check_v1.py`
- `constellation_2/common/tests/test_aegis_research_quality_review_self_check_v1.py`

Updated files:

- `package.json` adds `npm run aegis:research-quality-review-self-check`.
- `aegis/modules/operator_portal/aegis.module.yaml` declares the review-only command, evidence artifact, docs, and test.

## 2. Hypotheses Reviewed

Reviewed all 8 current hypotheses from the 2026-06-01 research portfolio artifact:

- `HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1` - STRONG
- `HYP_CROSS_ASSET_TREND_PERSISTENCE_V1` - ACCEPTABLE
- `HYP_DEFENSIVE_TAIL_CONVEXITY_V1` - NEEDS_REFINEMENT
- `HYP_DEFINED_RISK_VOL_PREMIUM_V1` - ACCEPTABLE
- `HYP_EQUITY_SHORT_HORIZON_MEAN_REVERSION_V1` - ACCEPTABLE
- `HYP_EVENT_DISLOCATION_REPRICING_V1` - TOO_BROAD
- `HYP_MARKET_NEUTRAL_SPREAD_CONVERGENCE_V1` - NEEDS_REFINEMENT
- `HYP_INTENT_SIMULATOR_CONTROL_V1` - NOT_TESTABLE

All 7 current theses were also reviewed. All mappings remain `LEGACY_INFERRED`; no thesis, hypothesis, sleeve, or program was changed.

## 3. Hypotheses Needing Refinement

- `HYP_DEFENSIVE_TAIL_CONVEXITY_V1`: needs explicit trigger variables, observation windows, and invalidation criteria.
- `HYP_EVENT_DISLOCATION_REPRICING_V1`: too broad; should split by event family before validation claims are made.
- `HYP_MARKET_NEUTRAL_SPREAD_CONVERGENCE_V1`: needs explicit spread definition, universe, and convergence window.
- `HYP_INTENT_SIMULATOR_CONTROL_V1`: not an alpha hypothesis; keep as control/system validation, not edge validation.

## 4. Inactive Sleeves Reviewed

Reviewed the five latest inactive non-simulator sleeves from the sleeve evaluation rollup evidence. The optional simulator sleeve was excluded from alpha dormancy review.

- `C2_DEFENSIVE_TAIL_V1`
- `C2_EVENT_DISLOCATION_V1`
- `C2_MARKET_NEUTRAL_SPREAD_V1`
- `C2_MEAN_REVERSION_EQ_V1`
- `C2_VOL_INCOME_DEFINED_RISK_V1`

## 5. Inactive Sleeve Classifications

- `C2_DEFENSIVE_TAIL_V1`: DATA_BLOCKED / MARKET_CONDITION_NOT_PRESENT; recommendation `NEEDS_MORE_DATA`.
- `C2_EVENT_DISLOCATION_V1`: NEEDS_PARAMETER_REVIEW / DATA_BLOCKED; recommendation `INVESTIGATE_RULES`.
- `C2_MARKET_NEUTRAL_SPREAD_V1`: DATA_BLOCKED; recommendation `NEEDS_MORE_DATA`.
- `C2_MEAN_REVERSION_EQ_V1`: MARKET_CONDITION_NOT_PRESENT / NEEDS_PARAMETER_REVIEW; recommendation `KEEP_WAITING`.
- `C2_VOL_INCOME_DEFINED_RISK_V1`: MARKET_CONDITION_NOT_PRESENT with prior DATA_BLOCKED evidence; recommendation `KEEP_WAITING`.

No candidate-generation behavior was changed. No sleeve was paused or retired.

## 6. Retirement Policy Summary

Created deterministic pause/watch/retirement criteria for hypotheses, sleeves, and research programs. The policy distinguishes healthy dormancy from evidence of decay or persistent blockage. A single inactive week is not sufficient for retirement.

Policy guardrails:

- `manual_review_required_before_retire: true`
- open unrealized PnL cannot prove or disprove an edge
- no broker execution implications
- no live-capital implications
- no automatic candidate-rule changes

## 7. Research Program Grouping Findings

Reviewed all 7 current research programs:

- `PROGRAM_TREND_PERSISTENCE_V1`: WELL_DEFINED, with future split watch between equity and cross-asset implementations.
- `PROGRAM_DEFENSIVE_CONVEXITY_V1`: NEEDS_RENAME to clarify defensive convexity versus general hedging.
- `PROGRAM_EQUITY_MEAN_REVERSION_V1`: WELL_DEFINED.
- `PROGRAM_EVENT_DISLOCATION_V1`: TOO_BROAD / NEEDS_SPLIT by event family.
- `PROGRAM_RELATIVE_VALUE_SPREAD_V1`: NEEDS_REFINEMENT.
- `PROGRAM_SIMULATION_CONTROL_V1`: TOO_NARROW / NEEDS_RENAME; should remain non-alpha control infrastructure.
- `PROGRAM_VOLATILITY_RISK_PREMIUM_V1`: WELL_DEFINED.

No mappings were changed automatically.

## 8. Regime Taxonomy Summary

Defined an initial taxonomy for future validation stratification across:

- market direction
- volatility
- rates and macro
- market structure
- event context

Each tag includes description, deterministic assignment rule if currently available, required data, fallback behavior, validation use, and status. Regime detection was not implemented.

## 9. Recommended Next Actions

1. Refine `HYP_EVENT_DISLOCATION_REPRICING_V1` into narrower event-family hypotheses before treating it as validation-ready.
2. Add explicit trigger, holding-period, and invalidation language to defensive tail and spread convergence hypotheses.
3. Investigate data-blocked inactive sleeves before adjusting parameters.
4. Keep mean-reversion and volatility-premium sleeves under observation until market conditions produce fair setup opportunities.
5. Apply regime tags only after deterministic detection inputs are available.

## 10. Implementation Changes Made

Implemented a lightweight review completeness checker only. It verifies that current runtime portfolio entities are covered by the review docs and writes:

`reports/aegis_research_quality_review_self_check_v1/<day_utc>/self_check.v1.json`

This checker is exposed through `npm run aegis:research-quality-review-self-check`. It is intentionally not wired into strict `npm run aegis:audit`; the review is hardening metadata, not a runtime truth readiness gate.

## 11. Tests Or Checks Run

- `npm run aegis:audit` before modifications: passed; verified runtime graph strict mode READY with zero blockers.
- `python3 -m py_compile ops/aegis/research_quality_review_self_check_v1.py ops/tools/run_aegis_research_quality_review_self_check_v1.py constellation_2/common/tests/test_aegis_research_quality_review_self_check_v1.py`: passed.
- `python3 -m pytest constellation_2/common/tests/test_aegis_research_quality_review_self_check_v1.py`: 2 passed.
- `npm run aegis:research-quality-review-self-check`: passed, failure_count 0.
- Final `npm run aegis:audit`: passed; verified runtime graph strict mode reported `graph_status: READY` and `audit_blocker_count: 0`. Runtime truth remains `PARTIAL_CONTEXT` / `BLOCKED` for trade-advice readiness due existing stale operating-status dependencies, not this review hardening.

## Completion Status

Research quality hardening is complete. The work improved review quality and operator confidence without changing trading behavior, candidate generation, broker execution, tax handling, or research allocation recommendations.
