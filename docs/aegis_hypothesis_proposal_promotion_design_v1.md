# Aegis Hypothesis Proposal Promotion Design V1

## Overview

The promotion pipeline turns discovered hypothesis proposals into deterministic research-only promotion readiness artifacts. It lets Aegis perform the mechanical triage and shadow validation work autonomously while reserving paper-promotion approval for David.

The MVP intentionally stops before paper sleeve creation. It produces a one-click approval queue, not sleeves or trades.

## Components

### Evidence Packet Builder

Reads immutable hypothesis proposal artifacts and associated readiness/priority context. It normalizes the proposal into a stable Evidence Packet and computes an evidence packet hash.

### Autonomous Triage

Classifies each Evidence Packet into one of the allowed triage outcomes. It records reason codes, required data, duplicate status, testability score, and transition history.

### Shadow Trial

Runs deterministic feasibility checks using the Evidence Packet and triage result. The MVP does not run market simulations; it verifies whether a proposal is ready to enter a governed shadow trial and records pass/fail reasons.

### Promotion Packet Builder

Builds immutable Promotion Packets for every proposal. A packet can recommend paper promotion only after shadow validation passes and all safety statuses remain research-only.

### Approval Queue

Extracts recommended Promotion Packets into a David-facing queue. Queue actions are approval, rejection, and deferral. Actions record approval events only.

## Artifact Model

Each aggregate artifact is day-scoped and content-addressed:

- evidence packets bind input proposal paths and hashes
- shadow trials bind evidence packet hashes
- promotion packets bind evidence and shadow hashes
- approval queue binds promotion packet hashes
- top-level promotion report binds all child hashes

`generated_at_utc` is deterministic for the target day so reruns with the same inputs produce the same states and hashes.

## Safety Boundary

Every artifact carries:

- `paper_research_only: true`
- `trade_advice_allowed: false`
- `broker_execution_allowed: false`
- `live_trading_allowed: false`
- `real_capital_allowed: false`
- `automatic_paper_sleeve_creation_allowed: false`

Approval means only "approved for paper research tracking." It does not authorize broker execution, real capital, trade advice, live trading, or automatic paper sleeve creation.

## MVP Behavior For Current Proposals

The MVP consumes:

- Oil shock reversals across energy ETFs
- Macro calendar event dislocation watch

Oil shock can become `PAPER_PROMOTION_RECOMMENDED` if readiness and shadow checks pass. Macro calendar remains `NEEDS_DATA` while macro-calendar evidence is missing.

## Future Work

Later phases may add:

- richer duplicate matching against research and paper sleeves
- real shadow sample generation from market-data windows
- statistical sufficiency estimators by strategy family
- a governed Paper Sleeve Blueprint builder that requires David approval evidence
- paper tracking materialization after blueprint approval

Those phases remain forbidden from enabling broker execution, trade advice, live trading, or real capital without separate governance.
