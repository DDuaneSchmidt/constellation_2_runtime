# Research Auditability

Research artifacts are offline-only and non-authoritative for trading.

Evidence, results, conclusions, and promotions must carry or reference:
- hypothesis id
- task id where applicable
- evidence refs
- result refs where applicable
- data snapshot refs where available
- methodology version
- code version or git commit
- created timestamp
- artifact lineage
- reason codes
- reproducibility notes

`research_architecture_integrity_review_v1.py` checks for missing lineage, unsafe promotion paths, legacy registry source-of-truth drift, knowledge graph authority leaks, broker coupling, and taxonomy drift.
