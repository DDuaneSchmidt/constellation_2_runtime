# Aegis Generated Hypothesis Outcome Maturity Monitor Design v1

## Ownership

Outcome maturity ownership remains in AEGIS_SYSTEM. Package 019 observes authoritative runtime artifacts and explains deterministic readiness. It does not own outcome creation; Package 018 and the paper outcome auto-closure pipeline remain responsible for outcome creation and proof.

## Flow

1. Read the authoritative paper position ledger.
2. Select open generated-hypothesis paper observations.
3. Join each observation to auto-closure, exit recommendation, and outcome registry rows by candidate or position id.
4. Compute calendar age, trading-day age, minimum holding period, earliest eligible outcome date, and next expected check date.
5. Classify readiness using deterministic states.
6. Emit portfolio summary counts and safety assertions.

## Fail-Closed Behavior

Missing lineage reports `LINEAGE_MISMATCH`. Missing close policy reports `CLOSE_RULE_MISSING`. Missing market-data source reports `WAITING_FOR_MARKET_DATA`. Missing actionable exit price after a close condition reports `EXIT_PRICE_MISSING`. Mature positions without a stop, target, or close trigger report `WAITING_FOR_CLOSE_CONDITION`. Unknown conditions report `UNKNOWN_DETERMINISTIC_BLOCKER` rather than advancing lifecycle state.

## Safety

The builder writes only its own monitoring artifact. It does not write outcome registry rows, ledger rows, validation samples, research quality outputs, allocation artifacts, broker instructions, live-trading commands, or safety-gate changes.
