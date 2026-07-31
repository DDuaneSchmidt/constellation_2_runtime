# Aegis Candidate Contracts v1

`aegis_candidate_contracts_v1` is the authoritative contract artifact for current-day paper research candidates.

The builder consumes `aegis_signal_evidence_graph_v1` and requires complete candidate fields plus certified entry reference price evidence before emitting a valid contract. Rejected raw signals retain deterministic rejection reasons and missing contract fields.

Generated-hypothesis signals are not a separate shortcut. Once a generated-hypothesis producer emits a governed raw signal and the signal evidence graph certifies the required fields, candidate contracts may create a normal valid contract through the same pipeline used by other sleeves.

The artifact does not enable trade advice, live trading, broker execution, autonomous execution, real-capital allocation, or paper observation creation by itself.
