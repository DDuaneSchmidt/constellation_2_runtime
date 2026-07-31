# Aegis Market Data Universe Consistency Spec v1

Specifies artifact aegis_market_data_universe_consistency_v1. The artifact reports each consumer, required/requested/returned/certified symbols, missing-from-request, requested-not-returned, returned-not-certified, owner, blocker code, David action state, and source hashes.

## Safety

This framework is research-only. It does not permit broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, forced candidates, strategy rule changes, paper lifecycle changes, or exit rule changes.

## June 2, 2026 Expected Behavior

Oil Shock requires `DBC`, `SPY`, `USO`, and `XLE`. Open paper-position symbols remain required for mark coverage. Missing required symbols are classified before downstream consumers fail.
