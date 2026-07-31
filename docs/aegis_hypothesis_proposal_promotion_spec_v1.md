# Aegis Hypothesis Proposal Promotion Spec V1

## Command

`npm run aegis:hypothesis-proposal-promotion`

The command builds research-only promotion pipeline artifacts for the requested `TARGET_DAY`.

## Inputs

The MVP input source is the Research Store hypothesis proposal registry:

- `research_lab/research_store/registries/hypothesis_proposals.jsonl`
- `research_lab/research_store/event_intake/hypothesis_proposals/*.json`
- readiness artifacts under `research_lab/research_store/research_intake/readiness_assessments`
- priority artifacts under `research_lab/research_store/research_intake/priority_scores`

Input proposals whose artifact hash does not match the registry hash are rejected as stale or mismatched and do not become promotion recommendations.

## Outputs

The command writes these report families under `reports/{family}/{day}`:

- `aegis_hypothesis_proposal_promotion_v1/promotion_pipeline.v1.json`
- `aegis_hypothesis_evidence_packet_v1/evidence_packets.v1.json`
- `aegis_hypothesis_shadow_trial_v1/shadow_trials.v1.json`
- `aegis_hypothesis_promotion_packet_v1/promotion_packets.v1.json`
- `aegis_paper_promotion_approval_queue_v1/approval_queue.v1.json`

## State Machine

The pipeline uses explicit state transitions only:

- `PROPOSED -> REJECTED_DUPLICATE`
- `PROPOSED -> REJECTED_UNTESTABLE`
- `PROPOSED -> REJECTED_LOW_SAMPLE_RATE`
- `PROPOSED -> NEEDS_DATA`
- `PROPOSED -> READY_FOR_SHADOW_TRIAL`
- `READY_FOR_SHADOW_TRIAL -> SHADOW_VALIDATION_RUNNING`
- `SHADOW_VALIDATION_RUNNING -> SHADOW_VALIDATION_FAILED`
- `SHADOW_VALIDATION_RUNNING -> SHADOW_VALIDATION_PASSED`
- `SHADOW_VALIDATION_PASSED -> PAPER_PROMOTION_RECOMMENDED`
- `PAPER_PROMOTION_RECOMMENDED -> PAPER_PROMOTION_APPROVED`
- `PAPER_PROMOTION_RECOMMENDED -> PAPER_PROMOTION_REJECTED`
- `PAPER_PROMOTION_RECOMMENDED -> PAPER_PROMOTION_DEFERRED`

No hidden fallback state is allowed.

## Triage Rules

The MVP rules are deterministic:

- duplicate proposal title, hypothesis text, or explicit overlap with existing sleeves -> `REJECTED_DUPLICATE`
- missing hypothesis statement or missing instrument universe -> `REJECTED_UNTESTABLE`
- expected sample frequency below monthly -> `REJECTED_LOW_SAMPLE_RATE`
- missing required data or readiness blockers -> `NEEDS_DATA`
- otherwise -> `READY_FOR_SHADOW_TRIAL`

## Shadow Trial Rules

Shadow validation passes only when all checks pass:

- data availability is ready
- sample generation is feasible
- signal stability is acceptable
- duplicate overlap is not blocking
- expected time to statistical sufficiency is estimable
- paper-readiness feasibility is ready

Failure keeps the proposal out of the paper-promotion recommendation queue.

## Approval Semantics

Approval writes a research-only approval event. It does not create:

- broker orders
- live trades
- trade advice
- real capital allocation
- paper sleeve runtime objects
- paper positions

The approval event may be consumed later by a separately governed Paper Sleeve Blueprint builder.

## UI Contract

Command Center and Research may display the approval queue as "Paper Promotion Recommendations." Buttons are limited to:

- Approve Paper Test
- Reject
- Defer

The safety statement must be visible anywhere a recommendation can be approved:

"This is paper research only. Not trade advice. No broker execution. No live trading."
