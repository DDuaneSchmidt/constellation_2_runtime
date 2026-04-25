---
id: C2_REFINEMENT_THRESHOLD_V1
title: "C2 Refinement Threshold Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_refinement_plane
---

# refinement_threshold_v1

Refinement thresholds are deterministic and table-driven.

Threshold order:
| precedence | reason_id | allowed action | law |
| --- | --- | --- | --- |
| 1 | `PRESERVE_TRUST_CRITICAL_TOP_LEVEL` | `preserve_top_level` | critical degraded, blocked-but-important, claim-strength-withheld, and insufficient-evidence states outrank convenience |
| 2 | `WITHHOLD_REFINEMENT_INSUFFICIENT_EVIDENCE` | `refinement_withheld` | missing value basis or missing required snapshot evidence blocks simplification |
| 3 | `COMPRESS_ACTIONABLE_SUMMARY` | `compress_summary` | actionable current items with complete evidence may be compressed but remain top-level |
| 4 | `DEMOTE_REVIEW_DELTA_TO_SECONDARY` | `demote_to_secondary` | non-critical review delta items may move below top-level |
| 5 | `PRESERVE_DRILLDOWN_VALUE_ONLY` | `preserve_drilldown_only` | lower-prominence proof remains accessible only through governed drill-down |
| 6 | `RETIRE_NOT_ALLOWED_FIRST_WAVE` | `refinement_withheld` | retirement is not legal in first-wave scope |

Threshold laws:
- refinement MUST preserve top-level visibility where trust/safety evidence requires it
- refinement MUST preserve drill-down access whenever top-level visibility is reduced
- refinement MUST withhold rather than simplify when evidence basis is incomplete
