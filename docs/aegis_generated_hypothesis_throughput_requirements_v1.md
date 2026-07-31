# Aegis Generated Hypothesis Throughput Requirements v1

## Purpose

Generated hypotheses need visible throughput tracking after approval so David can see whether autonomous research plumbing is progressing without becoming the routing bottleneck.

## Required Metrics

For each generated hypothesis, track:

- proposal state
- approval state
- paper setup state
- candidate count
- paper observation count
- open observations
- closed outcomes
- included validation samples
- excluded validation samples
- days since proposal
- days since approval
- throughput status

## Allowed Throughput Status

- `PROPOSED_ONLY`
- `NEEDS_DATA`
- `PAPER_TRACKING_READY`
- `CANDIDATES_FLOWING`
- `OBSERVATIONS_FLOWING`
- `OUTCOMES_FLOWING`
- `VALIDATION_FLOWING`
- `STALLED`

## Oil Shock Requirement

If Oil shock reversals across energy ETFs has not progressed beyond `PAPER_TRACKING_READY`, report:

- `throughput_status`: `PAPER_TRACKING_READY`
- `next_expected_step`: `candidate generation`
- no David action required unless blocked
