# ADR 0001: Dual-Kernel Architecture

## Status
Accepted

## Context
Aegis needs separate control over operational truth and adaptive intelligence. Runtime readiness and permissions must remain deterministic, while research and sleeve governance need durable provenance and approval records.

## Decision
Aegis uses two kernels:
- Runtime Truth Kernel: sole authority for readiness, permissions, and operational truth.
- Intelligence Governance Kernel: authority for facts, metrics, interpretations, recommendations, AI output governance, and human approval lineage.

## Consequences
Intelligence outputs can inform operators, but cannot mutate runtime truth, approve changes, execute trades, or change sleeves automatically.

## Safety Constraints
No broker submit/transmit, autonomous execution, live trading, automatic sleeve mutation, or unsupported AI claims are allowed.
