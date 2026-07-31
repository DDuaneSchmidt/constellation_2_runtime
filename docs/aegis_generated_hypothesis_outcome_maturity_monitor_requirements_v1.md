# Aegis Generated Hypothesis Outcome Maturity Monitor Requirements v1

## Purpose

Package 019 adds read-only monitoring for open generated-hypothesis paper observations. It answers when each open observation becomes eligible for deterministic outcome creation without forcing outcomes, changing close rules, changing holding periods, creating validation samples, consuming research quality, or touching broker/live-trading paths.

## Inputs

The monitor consumes authoritative runtime artifacts only:

- `aegis_paper_position_ledger_v1` for open generated-hypothesis paper observations.
- `aegis_paper_outcome_auto_closure_v1` for close-rule, mark, and auto-closure status.
- `aegis_exit_recommendations_v1` for deterministic exit recommendation state.
- `aegis_outcome_registry_v1` for current outcome row state.
- Package 018 and validation proof artifacts as evidence references only.

## Output

The monitor writes `truth/reports/aegis_generated_hypothesis_outcome_maturity_monitor_v1/<day>/generated_hypothesis_outcome_maturity_monitor.v1.json`.

For every open generated-hypothesis paper observation it reports lineage, age, holding-period state, close-rule status, mark-data status, deterministic readiness state, earliest eligible outcome date, next expected check date, blocker reason, owner, and David action requirement.

## Non-Goals

Package 019 does not create outcomes, fabricate exit prices or close timestamps, create validation samples, advance validation proof, consume research quality, change allocation, or affect safety gates.
