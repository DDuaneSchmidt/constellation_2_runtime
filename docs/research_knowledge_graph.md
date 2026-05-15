# Research Knowledge Graph

`research_knowledge_graph.v1` is a generated scaffold for future relationship tracking.

Initial fields are:

- `nodes`
- `edges`
- `hypothesis_refs`
- `edge_family_refs`
- `regime_refs`
- `sleeve_refs`
- `evidence_refs`
- `failure_mode_refs`

The graph is intentionally non-authoritative in Phase 1. It should be derived from hypotheses, evidence packets, result ledgers, and promotion artifacts. It may help humans and future tools navigate relationships, but it does not decide research status, promotion eligibility, Lite readiness, trade eligibility, risk, sizing, quarantine, or execution.

Do not add graph inference or orchestration until real usage exposes a concrete need.
