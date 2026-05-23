# ADR 0002: Evidence To Approval Chain

## Status
Accepted

## Context
Adaptive intelligence must be auditable and replayable. Conclusions need lineage from source artifacts through calculated metrics to interpretations and recommendations.

## Decision
Every governed conclusion follows:
Evidence -> Metrics -> Interpretation -> Recommendation -> Human Approval -> Operational Change.

## Consequences
Facts must cite source artifacts. Metrics must cite formulas and sample sizes. Interpretations cite facts or metrics. Recommendations cite interpretations and require human approval.

## Safety Constraints
No recommendation may directly perform an action. Operational changes require external human approval and remain outside the Intelligence Governance Kernel.
