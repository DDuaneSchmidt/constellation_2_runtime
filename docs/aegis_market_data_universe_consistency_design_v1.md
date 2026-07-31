# Aegis Market Data Universe Consistency Design v1

Design: market-data demand remains the request authority, while universe consistency audits demand against paper positions, active sleeves, generated hypotheses, Oil Shock, macro readiness, exit/outcome/mark coverage, and research follow-through. It is research-only and never changes trading, broker, lifecycle, or strategy gates.

## Safety

This framework is research-only. It does not permit broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, forced candidates, strategy rule changes, paper lifecycle changes, or exit rule changes.

## June 2, 2026 Expected Behavior

Oil Shock requires `DBC`, `SPY`, `USO`, and `XLE`. Open paper-position symbols remain required for mark coverage. Missing required symbols are classified before downstream consumers fail.
