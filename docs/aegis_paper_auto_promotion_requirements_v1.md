# Aegis Paper Auto-Promotion Requirements v1

Date: 2026-06-01

## Objective

Create a governed paper-only auto-promotion path that converts qualified valid candidate contracts into paper-tracking research observations without requiring explicit human approval and without weakening live-capital protections.

## Problem

The current candidate-to-paper path requires:

Candidate -> `AWAITING_REVIEW` -> `APPROVED_FOR_PAPER` -> Paper Position

For research observation, this blocks the feedback loop because valid candidates can remain indefinitely in `HUMAN_REVIEW_REQUIRED` even when paper-only construction evidence exists.

## Required Separation

Human approval and paper tracking eligibility are different concepts:

- `APPROVED_FOR_PAPER` means explicit human approval.
- `AUTO_PROMOTED_TO_PAPER_TRACKING` means paper-only research observation admission.

Automatic promotion must not reuse `APPROVED_FOR_PAPER`.

## Required States

- `AUTO_PROMOTED_TO_PAPER_TRACKING`
- `AUTO_PROMOTION_BLOCKED`
- `AUTO_PROMOTION_NOT_ELIGIBLE`

These states are paper-only and must not imply trade advice, live allocation, broker order creation, autonomous execution, or manual capture authorization.

## Eligibility Requirements

A candidate may auto-promote only when all are true:

- valid candidate contract exists
- entry reference price is certified
- instrument is governed
- hypothesis linkage exists
- sleeve linkage exists
- thesis linkage exists
- paper session is available
- paper ledger artifact is available
- no critical runtime truth blocker affects paper tracking

## Required Reason Codes

- `AUTO_PROMOTION_ALLOWED`
- `AUTO_PROMOTION_BLOCKED`
- `AUTO_PROMOTION_NOT_ELIGIBLE`

## Explicitly Forbidden

Auto-promotion must never:

- create trade advice
- create manual trade packets
- create broker orders
- create live allocations
- create execution workflows
- bypass instrument governance
- enable broker submit/transmit
- enable autonomous execution
- enable manual trade capture

## Audit Requirements

Every auto-promoted paper position must record:

- `candidate_id`
- `hypothesis_id`
- `thesis_id`
- `sleeve_id`
- `auto_promotion_reason_codes`
- `source_artifacts`
- `source_hashes`
- `promotion_timestamp`

## UI Requirements

Operator UI projections should distinguish:

- Auto-Promoted To Paper Tracking
- Human Approved For Paper
- Blocked From Paper

The UI must communicate that paper tracking is a research observation and human approval is a workflow decision.

## Self-Check Requirements

Self-check must verify:

- auto-promoted positions have complete lineage
- no auto-promoted position creates trade advice
- no auto-promoted position enables manual capture
- no auto-promoted position creates broker execution
- all auto-promoted positions enter outcome registry
- all auto-promoted positions enter validation pipeline
- deterministic reruns produce identical results

## Definition of Done

- docs exist
- auto-promotion states exist
- eligible candidates automatically become paper-tracking positions
- human approval remains separate
- outcome registry receives auto-promoted positions
- validation pipeline receives auto-promoted positions
- live-capital protections remain unchanged
- audit passes
- verified graph remains `READY`
