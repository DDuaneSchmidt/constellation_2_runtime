# Aegis Generated Hypothesis Throughput Design v1

## Inputs

The throughput report reads canonical workflow state, evidence packets, approval queue/events, paper setup, candidate lifecycle, outcome registry, and validation samples.

## Classification

The highest observed progress determines `throughput_status`:

- validation samples -> `VALIDATION_FLOWING`
- closed outcomes -> `OUTCOMES_FLOWING`
- paper observations -> `OBSERVATIONS_FLOWING`
- candidates -> `CANDIDATES_FLOWING`
- ready setup -> `PAPER_TRACKING_READY`
- data blocker -> `NEEDS_DATA`
- otherwise -> `PROPOSED_ONLY`

## Stalled Detection

Stall classification is reserved for a generated hypothesis that remains in a non-terminal state beyond the stale-state threshold while no automatic next step is flowing.

## Safety Boundary

Throughput tracking is observation-only. It does not generate orders, provide trade advice, allocate capital, or change safety gates.
