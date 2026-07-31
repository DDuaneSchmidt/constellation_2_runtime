# Aegis Hypothesis Proposal Promotion Requirements V1

## Purpose

Aegis must route discovered hypothesis proposals through research-only promotion readiness without making David the bottleneck for triage. David should only see a clear paper-promotion approval when a hypothesis has enough deterministic evidence to begin paper research tracking.

## Pipeline

The long-term pipeline is:

Hypothesis Proposal -> Evidence Packet -> Autonomous Triage -> Shadow Trial -> Promotion Packet -> One-click David Approval -> Paper Sleeve Blueprint -> Paper Tracking

## Required States

- PROPOSED
- REJECTED_DUPLICATE
- REJECTED_UNTESTABLE
- REJECTED_LOW_SAMPLE_RATE
- NEEDS_DATA
- READY_FOR_SHADOW_TRIAL
- SHADOW_VALIDATION_RUNNING
- SHADOW_VALIDATION_FAILED
- SHADOW_VALIDATION_PASSED
- PAPER_PROMOTION_RECOMMENDED
- PAPER_PROMOTION_APPROVED
- PAPER_PROMOTION_REJECTED
- PAPER_PROMOTION_DEFERRED

## Evidence Packet Requirements

Every proposal must produce a deterministic Evidence Packet containing:

- hypothesis id
- hypothesis statement
- source observation cluster
- data sources used
- instrument universe
- entry logic
- exit logic
- expected holding period
- expected sample frequency
- duplicate check
- testability score
- known risks
- required evidence fields

## Autonomous Triage Requirements

Aegis may autonomously decide:

- reject automatically
- mark as needing more data
- mark as ready for shadow trial

Automatic rejection is allowed only for deterministic reasons such as duplicate detection, untestable setup, or insufficient expected sample rate.

## Shadow Trial Requirements

Shadow validation must test:

- data availability
- sample generation feasibility
- signal stability
- duplicate or overlap with existing sleeves
- expected time to statistical sufficiency
- paper-readiness feasibility

Shadow trials are research-only. They do not create sleeves, candidates, orders, broker activity, trade advice, or capital allocation.

## Promotion Packet Requirements

Promotion Packets must be immutable and auditable. Each packet must include:

- hypothesis proposal version
- evidence packet hash
- triage result
- shadow validation result
- paper-readiness checklist
- risk policy status
- entry policy status
- exit policy status
- expected validation timeline
- why recommended
- why not rejected
- safety statement: "This is paper research only. Not trade advice. No broker execution. No live trading."

## David Approval Requirements

The UI must show a Paper Promotion Recommendations card with:

- hypothesis name
- recommendation reason
- shadow validation result
- expected sample frequency
- expected validation timeline
- risks and caveats
- Approve Paper Test
- Reject
- Defer

"Approve Paper Test" means only: approve this hypothesis for paper research tracking.

It must not mean:

- approve a trade
- approve broker execution
- approve real capital
- approve investment recommendation

## Safety Requirements

Aegis may auto-discover, auto-triage, auto-shadow-test, auto-reject weak proposals, and auto-recommend paper promotion.

Aegis may not create a new paper sleeve without David approval. Aegis may not affect real broker accounts, real trading, trade advice, live trading, capital allocation, or autonomous real-money action.

## Determinism And Auditability

For every proposal Aegis must record:

- input proposal file
- input proposal hash
- generated evidence packet hash
- triage decision
- shadow trial result
- promotion decision
- state transition history
- reason codes
- generated_at_utc
- source artifact paths

Rerunning for the same `TARGET_DAY` and same proposal inputs must produce the same states and hashes.
