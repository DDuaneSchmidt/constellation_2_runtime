# Aegis Oil Shock Candidate Producer Requirements v1

## Scope
Research-only deterministic candidate producer for the approved Oil Shock hypothesis. The producer may evaluate candidate conditions and emit raw exposure-intent signals for existing Aegis candidate-contract processing. It must not create trades, broker orders, live positions, allocation mutations, paper observations, or validation outcomes directly.

## Required Inputs
- Approved Oil Shock hypothesis evidence packet.
- Approved paper sleeve blueprint.
- Approved paper tracking setup/readiness certification.
- Original source proposal when referenced by the evidence packet.
- Sleeve market data manifest and OHLCV JSONL files.
- Existing Aegis candidate-contract, entry-price certification, paper lifecycle, outcome, and validation pipelines.

## Required Statuses
The producer must emit exactly one deterministic evaluation status per run:
- `NO_MARKET_SETUP`
- `MISSING_DATA`
- `CANDIDATE_CONSTRUCTION_INCOMPLETE`
- `POLICY_INCOMPLETE`
- `VALID_CANDIDATE_SIGNAL`

## Behavioral Requirements
- Register a normal active candidate producer/sleeve row for Oil Shock.
- Include Oil Shock in candidate-generation diagnostics.
- Use approved hypothesis artifacts as authority for hypothesis ID, universe, evidence fields, entry logic, exit logic, expected holding period, risk policy, and validation plan.
- Use proposal-defined event rules only when present. Do not invent thresholds.
- Emit a normal raw signal only when all required artifacts and event conditions qualify.
- If market conditions do not qualify, emit `NO_MARKET_SETUP`.
- If required data or policy inputs are absent, emit a deterministic blocker.
- Do not bypass candidate contracts, entry-price certification, paper lifecycle, outcome validation, or audit.

## Safety Requirements
Every producer artifact must state that broker execution, live trading, trade advice, real capital, autonomous execution, order management, forced candidate creation, and paper-observation creation are disabled.
