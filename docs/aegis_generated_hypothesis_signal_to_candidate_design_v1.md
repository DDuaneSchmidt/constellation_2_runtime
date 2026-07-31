# Aegis Generated Hypothesis Signal-to-Candidate Design v1

## Design

The proof builder reads the latest target-day Oil Shock producer, signal evidence graph, candidate contracts, entry-price certification, governance bridge, paper setup bridge, candidate construction, and paper lifecycle artifacts.

It matches Oil Shock by hypothesis ID, Oil Shock text, sleeve ID, or raw signal ID prefix. It then applies a fail-closed classifier:

1. no setup or no raw signal
2. raw signal missing from signal evidence graph
3. signal schema incomplete
4. construction or governance policy missing
5. entry reference price not certified
6. rejected candidate contract
7. valid candidate contract created through the normal pipeline

The audit sequencing repair is intentionally narrow: after `aegis:oil-shock-candidate-producer`, the chain reruns `aegis:signal-evidence-graph`, `aegis:candidate-contracts`, and `aegis:candidate-to-paper-lifecycle`, then writes the signal-to-candidate proof. That fixes stale downstream artifacts without changing strategy logic or bypassing candidate contracts.

## Non-Goals

The proof does not force candidates, synthesize paper observations, approve trades, allocate capital, submit broker orders, or weaken evidence-lineage checks.
