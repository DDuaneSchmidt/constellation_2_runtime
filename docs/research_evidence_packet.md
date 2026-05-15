# research_evidence_packet.v1

`research_evidence_packet.v1` captures evidence for a hypothesis, sleeve idea, edge idea, regime idea, governance idea, overlap idea, stop logic, risk sizing idea, or behavioral state idea.

It includes the research id, hypothesis id, task id where applicable, description, research type, source data summary, replay window, instruments and regimes tested, edge family, expected holding period, methodology, methodology version, metrics, expectancy, drawdown, MAE/MFE, failure modes, limitations, reproducibility notes, artifact lineage, data snapshot refs, code version, reason codes, and lifecycle status.

This artifact is offline research evidence only. It does not authorize runtime use and cannot by itself feed Aegis Lite.

Metric conventions for `metrics_summary` should match the result ledger where possible:

- `expectancy`
- `sample_size`
- `hit_rate`
- `drawdown`
- `MAE`
- `MFE`
- `regime_expectancy`
- `forward_return_windows`
- `false_positive_rate`
- `edge_decay`
- `confidence_change`

Numeric decimal values should be encoded as strings when canonical JSON forbids floats. Evidence packets should include artifact lineage, methodology version, code version, reason codes, and reproducibility notes sufficient to reproduce the deterministic offline run.
