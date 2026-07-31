# Aegis Research Daily Scorecard Requirements v1

## Purpose
Create a deterministic daily scorecard for the Command Center that answers, after each scheduled Aegis research run, whether Aegis ran, what changed, whether validation advanced, whether generated hypotheses advanced, whether anything broke, and whether David needs to act.

## Scope
The scorecard is research workflow visibility only. It summarizes existing authoritative artifacts and must not change candidate generation, paper lifecycle, outcome closure, validation scoring, research allocation, hypothesis state, or safety policy.

## Out Of Scope
Broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, new research architecture, new hypothesis generation, and modifications to candidate, paper, outcome, or validation logic are explicitly out of scope.

## Required Inputs
The scorecard must consume only existing authoritative artifacts: candidate diagnostics, candidate contracts, candidate-to-paper lifecycle, paper outcome auto-closure, outcome registry, validation samples, hypothesis workflow state, operator action queue, generated hypothesis throughput, research follow-through control, research quality engine, hypothesis decision policy, research allocation recommendation, and verified runtime graph.

## Required Output
The artifact `aegis_research_daily_scorecard_v1` must include top-level run status, daily progress status, David action count, primary message, bottleneck, daily delta metrics, validation progress, generated hypothesis progress, David action summary, health and blocker summary, input hashes, source artifact paths, and safety flags.

## Determinism
For the same target day and same input artifacts, the scorecard must produce identical research status, deltas, generated-hypothesis rows, action summary, blocker summary, and content hash except for allowed generated timestamps.

## Safety Requirements
The scorecard is read-only. It must include explicit flags that broker execution, live trading, trade advice, real capital, autonomous execution, order management, allocation mutation, paper observation creation, and hypothesis mutation are not allowed.

## UI Requirements
The Command Center must render a compact `TODAY'S RESEARCH RESULT` section above detailed daily research cards. The UI must read backend scorecard fields only and must not infer progress, actions, or blockers.
