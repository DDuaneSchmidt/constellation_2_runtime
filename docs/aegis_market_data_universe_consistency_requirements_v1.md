# Aegis Market Data Universe Consistency Requirements v1

Defines the requirement that every research consumer with market-data symbols contributes to one consolidated required universe before candidate generation, mark coverage, outcomes, or generated-hypothesis validation consume market data.

## Safety

This framework is research-only. It does not permit broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, forced candidates, strategy rule changes, paper lifecycle changes, or exit rule changes.

## June 2, 2026 Expected Behavior

Oil Shock requires `DBC`, `SPY`, `USO`, and `XLE`. Open paper-position symbols remain required for mark coverage. Missing required symbols are classified before downstream consumers fail.
