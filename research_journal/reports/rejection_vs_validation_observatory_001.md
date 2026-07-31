# Rejection vs Validation Observatory 001

Date: 2026-06-06
Status: Analysis only

Scope: tracks the split between `TRUE_REJECTION`, `VALIDATION_LIMITATION`, and `UNKNOWN` using current Aegis runtime truth artifacts. This report does not modify Aegis behavior, capabilities, commands, evidence producers, policies, UI surfaces, candidate state, paper positions, allocation, broker execution, live trading, or validation authority.

## Runtime Truth Gate

This report was created after running `npm run aegis:audit` and reading the latest verified runtime graph:

- Verified graph: `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-06/verified_runtime_graph.v1.json`
- Graph status: `BLOCKED`
- Runtime readiness status: `BLOCKED`
- Runtime truth classification: `PARTIAL_CONTEXT`
- Highest readiness layer: `BLOCKED`
- Audit status before report creation: failed at `aegis:candidate-to-paper-self-check` with `NON_DETERMINISTIC_OUTPUT`

No readiness is inferred from code. The observatory is read-only and uses the verified graph plus runtime truth artifacts as sources.

## Classification Rules

| Metric | Definition | Current Source |
| --- | --- | --- |
| `TRUE_REJECTION` | A resolved paper outcome with `outcome_state=CLOSED_LOSS`, or an included validation sample with negative `return_value`. | `aegis_outcome_registry_v1`, `aegis_validation_samples_v1` |
| `VALIDATION_LIMITATION` | A sample excluded because the position is still open/unresolved, plus research hypotheses still underpowered or generated hypotheses blocked before validation sample creation. | `aegis_validation_samples_v1`, `aegis_research_daily_scorecard_v1`, `aegis_generated_hypothesis_validation_proof_v1` |
| `UNKNOWN` | Explicit `unknown_blocked` outcomes or samples without an inclusion classification. | `aegis_outcome_registry_v1`, `aegis_validation_samples_v1` |

These rules intentionally do not classify closed wins as validation limitations or true rejections. Closed wins are validation evidence, but they do not answer the rejection-vs-limitation question.

## Current Split

Current day: 2026-06-06.

| Metric | Count | Evidence |
| --- | ---: | --- |
| `TRUE_REJECTION` | 3 | 3 `CLOSED_LOSS` outcomes / 3 included samples with negative returns. |
| `VALIDATION_LIMITATION` | 58 sample limitations; 10 underpowered hypotheses; 2 generated hypotheses blocked before validation sample | 58 excluded samples are `OPEN_POSITION_NOT_RESOLVED`; daily scorecard reports 10 hypotheses still underpowered and 30 samples needed for next sufficiency; generated-hypothesis validation proof reports 2 blocked and 0 reaching validation sample. |
| `UNKNOWN` | 0 | Outcome registry `unknown_blocked=0`; validation samples have no missing inclusion statuses. |

Current answer: Atlas is not yet proving that it is reducing validation limitations faster than it is discovering true rejections. The present split is dominated by validation limitations: 58 unresolved/excluded samples versus 3 true rejections, with 10 hypotheses still underpowered and no generated hypothesis reaching validation sample.

## Trend

The available runtime truth history is shorter than the requested 30-day and 90-day windows. Current artifacts provide:

- `aegis_validation_samples_v1`: 2026-05-30 through 2026-06-06.
- `aegis_outcome_registry_v1`: 2026-05-30 through 2026-06-06.
- `aegis_research_daily_scorecard_v1`: 2026-06-01 through 2026-06-06.
- `aegis_generated_hypothesis_validation_proof_v1`: 2026-06-01 through 2026-06-06.

| Window | `TRUE_REJECTION` Trend | `VALIDATION_LIMITATION` Trend | `UNKNOWN` Trend | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Current | 3 true rejections | 58 excluded/open samples; 10 underpowered hypotheses; 2 generated-hypothesis blockers | 0 | Limitation count materially exceeds true rejection count. |
| 30-day | Insufficient 30-day history. Partial 8-day proxy: 0 on 2026-05-30 to 3 on 2026-06-06. | Insufficient 30-day history. Partial 8-day proxy: 36 excluded/open samples on 2026-05-30 to 58 on 2026-06-06. | Partial 8-day proxy remains 0. | Not enough evidence for a real 30-day rate claim; the partial proxy shows limitations rising, not falling. |
| 90-day | Insufficient 90-day history. | Insufficient 90-day history. | Insufficient 90-day history. | No 90-day trend can be claimed from verified artifacts. |

## Observed Daily Series

Validation samples and outcomes:

| Day | Total Samples | Included Samples | Excluded/Open Samples | Closed Losses | Closed Wins | Unknown |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-05-30 | 36 | 0 | 36 | 0 | 0 | 0 |
| 2026-05-31 | 36 | 0 | 36 | 0 | 0 | 0 |
| 2026-06-01 | 57 | 2 | 55 | 1 | 1 | 0 |
| 2026-06-02 | 62 | 5 | 57 | 3 | 2 | 0 |
| 2026-06-03 | 63 | 12 | 51 | 9 | 3 | 0 |
| 2026-06-04 | 64 | 16 | 48 | 14 | 2 | 0 |
| 2026-06-05 | 64 | 21 | 43 | 16 | 5 | 0 |
| 2026-06-06 | 64 | 6 | 58 | 3 | 3 | 0 |

Research daily scorecard:

| Day | Closed Outcomes Total | Included Samples Total | Hypotheses Still Underpowered | Samples Needed For Next Sufficiency | Daily Status |
| --- | ---: | ---: | ---: | ---: | --- |
| 2026-06-01 | 2 | 2 | 10 | 30 | `ACTION_REQUIRED` |
| 2026-06-02 | 5 | 5 | 10 | 30 | `ACTION_REQUIRED` |
| 2026-06-03 | 12 | 12 | 10 | 30 | `ACTION_REQUIRED` |
| 2026-06-04 | 16 | 16 | 10 | 30 | `BLOCKED` |
| 2026-06-05 | 21 | 21 | 10 | 30 | `BLOCKED` |
| 2026-06-06 | 6 | 6 | 10 | 30 | `BLOCKED` |

Generated-hypothesis validation proof is flat across 2026-06-01 through 2026-06-06:

| Field | Value |
| --- | ---: |
| Generated hypotheses total | 2 |
| Blocked count | 2 |
| Reached validation sample count | 0 |
| Reached outcome count | 0 |
| Primary bottleneck | `data readiness / shadow validation` |

## Answer

No. Based on verified artifacts available on 2026-06-06, Atlas is not reducing validation limitations faster than it is discovering true rejections.

The strongest current evidence is the opposite: the current day has 3 true rejections and 58 validation-limited samples, with 10 hypotheses still underpowered and 2 generated hypotheses blocked before validation sample creation. The historical window is too short for a legitimate 30-day or 90-day rate claim. The partial 8-day proxy also does not support improvement because excluded/open samples rose from 36 to 58 while true rejections rose from 0 to 3.

## Watch Items

- Do not compare `CLOSED_LOSS` counts alone without tracking the excluded/open denominator.
- Treat the 2026-06-06 drop from 21 included samples on 2026-06-05 to 6 included samples as a runtime truth change requiring source review, not as an inferred performance improvement.
- Promote this observatory to a producer only after the report has a declared artifact schema, deterministic trend window selection, and graph-visible evidence registration.
