# Aegis Research Capital Allocation Completion Report v1

Day UTC: `2026-05-31`

## What Was Implemented

Aegis now has a deterministic Research Capital Allocation layer for research-attention allocation only. It groups theses, hypotheses, and sleeves into research programs, scores each program with explicit components, emits reason-coded allocation decisions, records an event log, and exposes the result through the existing Research Portfolio surface.

This does not allocate brokerage capital, size trades, submit broker orders, automate IB execution, or perform tax harvesting.

## Files Changed

- `docs/aegis_research_capital_allocation_requirements_v1.md`
- `docs/aegis_research_capital_allocation_spec_v1.md`
- `docs/aegis_research_capital_allocation_design_v1.md`
- `docs/aegis_research_capital_allocation_completion_report_v1.md`
- `ops/aegis/research_program_registry_v1.py`
- `ops/aegis/research_capital_scoring_v1.py`
- `ops/aegis/research_allocation_decisions_v1.py`
- `ops/aegis/research_allocation_event_log_v1.py`
- `ops/aegis/research_capital_allocation_self_check_v1.py`
- `ops/aegis/research_portfolio_manager_v1.py`
- `ops/tools/build_aegis_research_capital_allocation_v1.py`
- `ops/tools/run_aegis_research_capital_allocation_self_check_v1.py`
- `constellation_2/common/tests/test_aegis_research_capital_allocation_v1.py`
- `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`
- `aegis/modules/operator_portal/aegis.module.yaml`
- `package.json`

## New Artifacts

- `reports/aegis_research_program_registry_v1/<day_utc>/research_program_registry.v1.json`
- `reports/aegis_research_capital_scoring_v1/<day_utc>/research_capital_scoring.v1.json`
- `reports/aegis_research_allocation_decisions_v1/<day_utc>/research_allocation_decisions.v1.json`
- `reports/aegis_research_allocation_event_log_v1/<day_utc>/research_allocation_event_log.v1.json`
- `reports/aegis_research_capital_allocation_self_check_v1/<day_utc>/self_check.v1.json`
- `reports/aegis_research_capital_allocation_v1/<day_utc>/research_capital_allocation.v1.json`

## New NPM Commands

- `npm run aegis:research-capital-allocation`
- `npm run aegis:research-capital-allocation-self-check`

`npm run aegis:audit` now runs both commands before the final Research Portfolio refresh and verified graph check.

## Current Metrics

Source artifact: `/home/node/constellation_runtime_data/truth/reports/aegis_research_capital_allocation_v1/2026-05-31/research_capital_allocation.v1.json`

- Current research program count: `7`
- Allocation recommendations count: `7`
- `INCREASE`: `0`
- `MAINTAIN`: `0`
- `REDUCE`: `5`
- `PAUSE`: `0`
- `RETIRE`: `0`
- `INVESTIGATE_MORE`: `2`
- Self-check status: `ok=true`, `failure_count=0`

## Scoring Model

Model version: `RESEARCH_CAPITAL_ALLOCATION_MODEL_V1`

The scoring model is deterministic and component based:

- Candidate Yield Score
- Validation Progress Score
- Evidence Quality Score
- Expected Value Signal Score
- Time-To-Decision Score
- Diversification Value Score
- Resource Efficiency Score
- Staleness / Dormancy Penalty
- Duplication Penalty
- Risk / Drawdown Penalty

When closed outcomes are unavailable, expected-value proof is explicitly marked `INSUFFICIENT_OUTCOMES` and contributes no positive expected-value score.

## Operator Visibility

The existing Research Portfolio panel now includes a Research Capital Allocation section showing:

- research program
- linked theses
- linked hypotheses
- linked sleeves
- current and recommended allocation units
- recommendation
- allocation score
- reason codes
- primary blocker
- evidence status
- validation status

## Tests And Validation

Commands run:

- `python3 -m py_compile ops/aegis/research_program_registry_v1.py ops/aegis/research_capital_scoring_v1.py ops/aegis/research_allocation_decisions_v1.py ops/aegis/research_allocation_event_log_v1.py ops/aegis/research_capital_allocation_self_check_v1.py ops/aegis/research_portfolio_manager_v1.py ops/tools/build_aegis_research_capital_allocation_v1.py ops/tools/run_aegis_research_capital_allocation_self_check_v1.py`
- `python3 -m pytest constellation_2/common/tests/test_aegis_research_capital_allocation_v1.py`
- `python3 -m pytest constellation_2/common/tests/test_aegis_research_capital_allocation_v1.py constellation_2/common/tests/test_aegis_outcome_validation_maturity_v1.py constellation_2/common/tests/test_aegis_hypothesis_research_portfolio_v1.py constellation_2/common/tests/test_aegis_evidence_lineage_integrity_v1.py`
- `npm run aegis:research-capital-allocation`
- `npm run aegis:research-capital-allocation-self-check`
- `npm run aegis:audit`

Results:

- Focused Research Capital Allocation tests: `12 passed`
- Broader focused suite: `34 passed`
- Research Capital Allocation self-check: `ok=true`, `failure_count=0`
- Full audit reached the new allocation build and self-check successfully, then stopped at the existing verified-runtime-graph blockers.

## Audit Status

`npm run aegis:audit` exits non-zero because the verified runtime graph remains `BLOCKED` for pre-existing missing evidence:

- `operator_portal:change_control_intelligence_layer_v1:REQUIRED_EVIDENCE_MISSING:aegis_change_control_advisor_score_v1,aegis_change_control_ai_review_v1,aegis_change_control_evidence_snapshot_v1`
- `operator_portal:portal_runtime_model:REQUIRED_EVIDENCE_MISSING:aegis_candidate_state,canonical_operator_state`

The new Research Capital Allocation self-check itself passes.

## Known Limitations

- Allocation recommendations are research-priority guidance only.
- No validated alpha is claimed.
- No `INCREASE` recommendations were emitted because the current outcome layer has no closed, statistically usable outcome proof.
- Current recommendations are conservative and driven primarily by candidate yield, evidence quality, validation progress, and insufficient-outcome blockers.

## Recommended Next Move

Close and score enough paper positions to produce resolved validation samples. Until closed outcomes exist, Research Capital Allocation should continue to identify promising underpowered programs and dormant programs, but it should not imply validated edge quality.
