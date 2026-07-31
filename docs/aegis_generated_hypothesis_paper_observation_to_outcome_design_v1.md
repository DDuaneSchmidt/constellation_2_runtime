# Aegis Generated Hypothesis Paper Observation-to-Outcome Design V1

Outcome readiness ownership belongs to the deterministic paper outcome auto-closure pipeline. Package 018 reads `aegis_paper_position_ledger_v1`, `aegis_paper_outcome_auto_closure_v1`, `aegis_exit_recommendations_v1`, and `aegis_outcome_registry_v1`, then classifies the Oil Shock paper observation.

A resolved outcome is recognized only when the matched outcome registry row has an outcome state other than `OPEN` or `UNKNOWN_BLOCKED`. An open row is treated as an outcome projection and not lifecycle advancement. Auto-closure reason codes map to exact blockers: same-day auto-promoted observations map to `HOLDING_PERIOD_NOT_ELAPSED`, HOLD recommendations map to `POSITION_STILL_OPEN`, missing mark rows map to `EXIT_PRICE_MISSING` or `MARK_DATA_MISSING`, and missing exit evaluation maps to `CLOSE_RULE_MISSING`.

The generated hypothesis validation proof consumes Package 018. If Package 018 reports `outcome_row_created: true`, outcome flow advances. If Package 018 is not ready, paper observation flow remains the furthest reached stage and the outcome blocker is surfaced without validation-sample advancement.
