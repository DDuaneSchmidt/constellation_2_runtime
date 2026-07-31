# AEGIS Event Dislocation Governance Trace Diagnostic Design v1

The diagnostic compares three read-only layers:

1. Raw signal or intent evidence referenced by the signal graph.
2. The Event Dislocation row emitted by `aegis_signal_evidence_graph_v1`.
3. The rejected candidate contract input emitted by `aegis_candidate_contracts_v1`.

It then performs a governance lookup against local governed registries and classifies the rejection deterministically.

The package deliberately does not call candidate construction builders, normalize candidate fields, weaken contract validation, or write any artifact except its own diagnostic report.
