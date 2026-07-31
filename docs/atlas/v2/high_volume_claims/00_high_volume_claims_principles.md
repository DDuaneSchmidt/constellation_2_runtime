# Atlas V2 High-Volume Claims Principles

Atlas V2 high-volume claim intake accepts externally supplied claims and converts them into bounded learning inputs. The pipeline does not discover claims, invent claims, or create synthetic hypotheses.

Required flow:

Claims -> Mechanisms -> Contrarian Theory -> Deduplication -> Cheap Experiment Routing -> Learning System

Principles:

- Every claim is externally supplied.
- Every claim is classified into a mechanism before routing.
- Every claim receives a contrarian theory statement before deduplication metrics are finalized.
- Claims are deduplicated by claim fingerprint and mechanism-family clustering.
- Duplicate claims strengthen a mechanism cluster; they do not create independent experiments.
- Cheap experiment routing is bounded by mechanism clusters, not raw claim count.
- The output is audit and learning intake evidence only.
