---
id: C2_OPERATOR_SUMMARY_PROVENANCE_CONTRACT_V1
title: "C2 Operator Summary Provenance Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_operator_summary_provenance
---

# C2 Operator Summary Provenance Contract V1

## Purpose

Every Core 5 operator-health conclusion must be audit-grade and replayable.

`operator_summary_provenance.v1` is the explicit record of how the canonical operator-health artifact was produced.

## Required provenance content

At minimum provenance must record:

- snapshot binding used
- lower-core artifacts read
- lower-core fields consumed
- rollup rules fired
- severity rules fired
- required-action rules fired
- omitted or degraded elements
- snapshot coherence result
- prior-state comparison summary
- traceability refs for every operator-health conclusion

## Traceability rule

Every conclusion for:

- overall operator state
- highest severity
- required operator action
- review reason class
- blocked reason class
- stale or degraded class
- last material change class

must be explainable through explicit provenance rows back to lower-core refs and rule ids.
