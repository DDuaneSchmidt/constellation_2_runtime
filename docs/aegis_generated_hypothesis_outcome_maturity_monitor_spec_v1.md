# Aegis Generated Hypothesis Outcome Maturity Monitor Spec v1

## Artifact

`aegis_generated_hypothesis_outcome_maturity_monitor_v1` is a day-scoped read-only monitoring artifact.

Path: `reports/aegis_generated_hypothesis_outcome_maturity_monitor_v1/{day}/generated_hypothesis_outcome_maturity_monitor.v1.json`

## Readiness States

- `OUTCOME_READY`
- `HOLDING_PERIOD_NOT_ELAPSED`
- `WAITING_FOR_MARKET_DATA`
- `WAITING_FOR_CLOSE_CONDITION`
- `CLOSE_RULE_MISSING`
- `EXIT_PRICE_MISSING`
- `LINEAGE_MISMATCH`
- `UNSUPPORTED_GENERATED_HYPOTHESIS_OUTCOME`
- `UNKNOWN_DETERMINISTIC_BLOCKER`

## Observation Selection

The monitor scans authoritative paper-position ledger open positions. A position is included when it carries generated-hypothesis lineage through `candidate_lineage.hypothesis_id`, top-level `hypothesis_id`, or the generated-hypothesis auto-promoted paper tracking mode. Closed, invalidated, and resolved positions are excluded.

## Maturity Rule

Same-day auto-promoted generated-hypothesis research observations require at least one trading day before outcome auto-closure eligibility. Explicit minimum holding-period fields on the position or closure row override the default. Trading-day arithmetic is deterministic weekday arithmetic.

## Advancement Boundary

This artifact is monitoring evidence only. Generated Hypothesis Validation Proof must not advance based on Package 019 alone; Package 018 remains authoritative for whether an outcome was created today.
