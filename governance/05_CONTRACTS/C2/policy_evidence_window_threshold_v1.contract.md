---
id: C2_POLICY_EVIDENCE_WINDOW_THRESHOLD_V1
title: "C2 Policy Evidence Window Threshold Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_policy_evolution_plane
---

# policy_evidence_window_threshold_v1

Evidence windows are deterministic and table-driven.

Fixed first-wave windows:
| window_id | days | minimum_history | consistency_rule | failure_law |
| --- | --- | --- | --- | --- |
| `SHORT_REVIEW_WINDOW` | 2 | 2 | same target, same proposed effect across both days | if missing, evolution is withheld |
| `STABILITY_WINDOW` | 3 | 2 | at least 2 matching supporting rows in last 3 days | if conflicting, bias to preserve or withhold |
| `EXPIRY_WINDOW` | 3 | 1 prior policy row | if no renewed support by review day, proposal expires | expiry forces re-review rather than silent persistence |

Threshold order:
| precedence | threshold_result | action bias | law |
| --- | --- | --- | --- |
| 1 | `TRUST_OVERRIDE_ACTIVE` | preserve or rollback | trust protections dominate reduce/compress proposals |
| 2 | `INSUFFICIENT_HISTORY` | withhold | insufficient window support blocks evolution |
| 3 | `UNSTABLE_HISTORY` | preserve or withhold | conflicting windows may not silently drift policy |
| 4 | `CONSISTENT_STRENGTHEN_SUPPORT` | strengthen | repeated trust-critical preservation may propose stronger emphasis |
| 5 | `CONSISTENT_REDUCE_SUPPORT` | reduce | repeated non-critical secondary/drill-down evidence may propose lower emphasis |
| 6 | `CONSISTENT_COMPRESSION_SUPPORT` | compress | repeated compressible evidence may propose more compression |
| 7 | `PRIOR_POLICY_EXPIRED` | expire | stale unrenewed proposals must re-review |
| 8 | `PRIOR_POLICY_ROLLBACK_REQUIRED` | rollback | prior proposal contradicted by current governed evidence must roll back |

Expiry law:
- expiry is explicit and review-bound
- no proposal may persist silently beyond its evidence window
