# Aegis AI Research Intelligence Requirements v1

## Purpose
Add a research-only AI intelligence layer that critiques, diagnoses, repairs, synthesizes, and de-duplicates research hypotheses without becoming state authority.

## Scope
The system produces advisory reports only for research workflows. Deterministic Aegis artifacts remain authoritative for workflow state, quality grades, hard gates, decisions, allocation recommendations, follow-through status, outcomes, validation samples, and statistical sufficiency.

## Required Dimensions
1. AI Research Critic: economic plausibility, testability, sample frequency, data quality, regime dependency, false discovery risk, confidence, and reason codes.
2. AI Root Cause Analyzer: likely causes for stalled or weak hypotheses, supporting artifacts, confidence, suggested investigation, and whether David action is likely required.
3. AI Hypothesis Repair Advisor: advisory repair options for REDESIGN or PAUSE hypotheses.
4. AI Evidence Synthesizer: advisory synthesis of outcomes, validation samples, sparse evidence, working/failing conditions, and sample limitations.
5. AI Duplicate Detector: overlap analysis across active and generated hypotheses, with duplicate score and merge/reject suggestions.
6. AI Research Intelligence Summary: compact UI read model combining all dimensions per hypothesis.

## Safety Requirements
Every artifact must include `research_only`, `ai_is_advisory_only`, `deterministic_state_authority`, `no_broker_execution`, `no_trade_advice`, `no_live_trading`, `no_real_capital`, `no_allocation_mutation`, and `no_automatic_retirement` flags. The layer must not mutate workflow state, action queues, allocation, retirement state, paper observations, sleeves, broker behavior, or safety gates.

## Inputs
The layer consumes existing deterministic artifacts including workflow state, operator action queue, generated hypothesis throughput, research quality, decision policy, allocation recommendation, follow-through control, outcome registry, validation samples, statistical sufficiency, and research portfolio.

## UI Requirement
The Research page must show a compact `AI research analysis — advisory only.` section. The UI must render backend AI artifacts and must not interpret AI as authoritative state.
