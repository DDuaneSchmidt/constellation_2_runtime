# Atlas V2 Backlog Review 001

Objective: define Atlas V2 candidates using only evidence from Atlas V1 operational usage, freeze review, coverage review, and North Star metrics.

Scope: backlog review only. This report does not implement Atlas V2, add architecture, modify Atlas V1 tools, change retrieval, change synthesis, change governance, modify runtime truth, add schemas, modify sleeves, modify candidates, create trading logic, create trade advice, or alter allocation logic.

Inputs:
- `research_journal/reports/atlas_v1_freeze_review_001.md`
- `research_journal/reports/atlas_v1_usage_cycle_001.md`
- `research_journal/reports/atlas_v1_usage_cycle_002.md`
- `research_journal/reports/atlas_v1_operational_usage_cycle_001.md`
- `research_journal/reports/atlas_usage_impact_review_001.md`
- `research_journal/reports/atlas_evidence_coverage_review_001.md`
- `research_journal/reports/atlas_north_star_dashboard_spec_001.md`
- `research_journal/reports/aegis_north_star_metrics_review_001.md`

Classification options:
- `V2_BUILD_NOW`
- `V2_BUILD_LATER`
- `DEFER_TO_V3`
- `PROHIBIT`

## 1. Executive Summary

Atlas V2 should be a quality and usability upgrade to the existing read-only research preparation workflow, not a new authority layer.

The strongest evidence-backed V2 candidates are:
- Source Quality / Noise Filtering.
- Research Packet Compacting.
- Lexical Blind Spot Reduction.
- Coverage Confidence.
- Evidence Flow Dashboard.
- Concentration Monitoring.
- North Star Trend Tracking.

The highest-priority theme is not more research autonomy. It is making Atlas packets easier to trust without creating false confidence. V1 reduced evidence discovery effort, improved citation reuse, and preserved non-authoritative boundaries, but operational cycles repeatedly found meta-review noise, duplicate evidence, lexical misses, indirect failure reuse, and weak direct runtime artifact coverage.

Final recommendation: start V2 with compacting, source-quality filtering, lexical blind-spot reduction, and non-authoritative coverage confidence. Build metric-facing V2 views later, after enough repeated usage snapshots justify trend display. Prohibit authority-bearing capabilities in V2.

## 2. V1 Operational Strengths

Atlas V1 reduced evidence discovery effort.

Evidence:
- `atlas_usage_impact_review_001.md` found Atlas reduced repeated manual search and source-path collection across outcome maturity, candidate volume, capital review readiness, technical strategy evidence, and evidence concentration.
- `atlas_v1_usage_cycle_001.md` found improved workflow for outcome maturity, candidate volume, and technical strategy evidence, and neutral-to-partial improvement for capital review readiness.
- `atlas_v1_usage_cycle_002.md` estimated 15 to 25 minutes saved on capital review readiness by surfacing central observations, journal reviews, and follow-through failures.
- `atlas_v1_operational_usage_cycle_001.md` found Atlas quickly identified North Star reports, adjacent phrases, coverage gaps, prior failures, and the metric snapshot without manual JSON inspection.

Atlas V1 improved reuse.

Evidence:
- V1 surfaced direct OBS, KNW, and FAIL entries for repeated research questions.
- Follow-through surfaced `FAIL_0010` for capital review readiness in Usage Cycle 002 even when exact Negative Knowledge matching remained weak.
- Operational Usage Cycle 001 found follow-through reused `FAIL_0004`, `FAIL_0005`, `FAIL_0006`, `FAIL_0007`, `FAIL_0010`, and `FAIL_0016`.

Atlas V1 improved consistency.

Evidence:
- The freeze review found V1 repeatedly preserved read-only, citation-bound, non-authoritative boundaries.
- Usage cycles found Atlas did not change manual conclusions; it improved preparation quality.
- V1 outputs did not recommend trades, allocate capital, infer readiness, mutate journal objects, modify candidates, change sleeves, or alter runtime truth.

Atlas V1 exposed coverage limits.

Evidence:
- The coverage review classified evidence coverage as `MODERATE_COVERAGE`, not complete.
- Coverage checks made referenced-but-not-retrieved sources visible.
- Operational Usage Cycle 001 found coverage checks reduced false confidence by distinguishing direct source gaps from retrieved report packets.

## 3. V1 Operational Friction

Primary-source and meta-review material are mixed.

Evidence:
- Usage Cycle 001 found Atlas retrieves newer Atlas review reports alongside primary journal objects.
- Operational Usage Cycle 001 found `atlas_v1_usage_cycle_001.md` caused broad referenced follow-through and made North Star packets less focused.
- The coverage review identified report recursion as a high-risk blind spot.

Packets are noisy and duplicative.

Evidence:
- Usage Cycle 001 found scope boilerplate and repeated non-action language in capital-review packets.
- Usage Cycle 002 found high but manageable noise from Atlas meta-review reports, scope boilerplate, and repeated report references.
- Operational Usage Cycle 001 found contrarian output repeated the same lines across conclusion, support, and contradiction sections.

Lexical query dependence still causes misses.

Evidence:
- The coverage review found `capital review readiness` missed direct failure entries because related failures used capital-allocation wording.
- Usage Cycle 001 found capital-review readiness was the clearest lexical blind-spot case.
- Operational Usage Cycle 001 found `north star metrics` was not supported by Workflow Runner and required Query Adjacency plus separate tools.

Coverage is visible but not confidence-calibrated.

Evidence:
- Atlas correctly reports potential gaps as non-authoritative, but users still need to interpret whether gaps are material.
- The coverage review warned that a convincing source list can still miss direct object files.
- Operational Usage Cycle 001 found Workflow Runner packets could look complete unless checked against direct metric snapshots.

Direct runtime artifact coverage remains limited.

Evidence:
- The coverage review states Atlas searches Research Journal objects and reports, not raw AEGIS truth artifacts.
- North Star reports rely on runtime-derived metrics such as validation samples, paper ledgers, candidate diagnostics, and sleeve evidence certification.
- Operational Usage Cycle 001 found the North Star Metrics brief provided cleaner concentration evidence than the report-heavy Workflow Runner packet.

## 4. Candidate V2 Capabilities

| Candidate | Classification | Rationale | Boundary |
|---|---|---|---|
| Source Quality / Noise Filtering | `V2_BUILD_NOW` | Repeated usage cycles found meta-review noise, scope boilerplate, and duplicated boundary text. Filtering should separate primary journal objects, runtime-derived reports, Atlas meta-reviews, and scope boilerplate. | Must not rank truth, certify evidence, or hide cited sources. |
| Research Packet Compacting | `V2_BUILD_NOW` | V1 packets are useful but duplicative. Compacting directly addresses repeated findings in Usage Cycles 001 and 002 without changing retrieval or synthesis authority. | Must preserve citations and show omitted/merged source groups. |
| Lexical Blind Spot Reduction | `V2_BUILD_NOW` | Query wording is the highest repeated retrieval weakness. Query Adjacency already helps; V2 should standardize source-observed adjacent-query use before Workflow Runner. | Must use source-observed terms only; no semantic authority or relevance claims. |
| Coverage Confidence | `V2_BUILD_NOW` | Coverage checks currently list gaps but do not calibrate confidence. V2 should express confidence as retrieval coverage quality, not evidence completeness. | Must remain non-authoritative and avoid completeness certification. |
| Evidence Flow Dashboard | `V2_BUILD_LATER` | North Star reviews show evidence flow is the right health frame, but dashboard/display work should follow packet-quality fixes. | Read-only only; no UI implementation in this review, no decisions or readiness inference. |
| Concentration Monitoring | `V2_BUILD_LATER` | Evidence concentration is repeatedly decision-relevant: 12 of 12 included samples and 53 of 63 paper positions were concentrated in `C2_TREND_EQ_PRIMARY_V1`. | Monitoring only; no sleeve mutation or capital implication. |
| North Star Trend Tracking | `V2_BUILD_LATER` | North Star metrics are the right repeated health lens, but trend value requires more operational snapshots than current evidence provides. | Trend display only; no optimization authority or allocation implication. |
| Direct Runtime Artifact Coverage | `V2_BUILD_LATER` | Runtime-derived metrics improved packet quality, but broad raw artifact coverage risks scope expansion. Build only as read-only citation coverage after packet-quality work. | Must query existing artifacts only and never create runtime truth authority. |
| Evidence Quality Layer | `DEFER_TO_V3` | Human review still provides source reconciliation and evidence-strength judgment. V2 evidence supports source-quality filtering, not a generalized evidence-quality layer. | Would require clear boundaries before any implementation. |
| Evidence Strength Classification | `DEFER_TO_V3` | The usage impact review found humans still rank root causes and evidence strength. V2 should not automate that judgment yet. | No readiness, truth, validation, or capital relevance classification. |

## 5. Explicitly Deferred Capabilities

Deferred to later V2 after packet-quality work:
- Evidence Flow Dashboard.
- Concentration Monitoring.
- North Star Trend Tracking.
- Direct Runtime Artifact Coverage.

Deferred to V3:
- Evidence Quality Layer.
- Evidence Strength Classification.
- Broader research historian behavior.
- Knowledge graph expansion.
- Bounded hypothesis or experiment-design drafting assistants.

Reason: current evidence supports better preparation, not stronger judgment authority. V1 operational evidence shows Atlas can gather and compress evidence but still needs humans for source reconciliation, root-cause ranking, evidence-strength judgment, and final interpretation.

## 6. Not Allowed In V2

The following are classified `PROHIBIT` for V2:

| Capability | Classification | Reason |
|---|---|---|
| Search Space Evolution | `PROHIBIT` | North Star reviews found idea volume is not the binding constraint; evidence flow is. |
| Chief Scientist | `PROHIBIT` | Freeze review explicitly prohibits authority replacement; human judgment remains required. |
| Research Economist | `PROHIBIT` | No operational usage evidence shows research economics is needed before evidence flow and packet quality improve. |
| Autonomous validation | `PROHIBIT` | V1 coverage is `MODERATE_COVERAGE`; Atlas cannot certify completeness or truth. |
| Autonomous capital allocation | `PROHIBIT` | Evidence remains underpowered and capital review readiness is not established. |
| Autonomous sleeve mutation | `PROHIBIT` | Atlas is a preparation layer; sleeve mutation is outside V1 and unsupported for V2. |
| Trade recommendation authority | `PROHIBIT` | All reviewed Atlas and AEGIS boundaries prohibit trade advice and execution authority. |
| Runtime truth mutation | `PROHIBIT` | Atlas outputs are not truth artifacts and must not alter runtime truth. |
| Candidate mutation | `PROHIBIT` | V2 evidence supports review preparation only, not candidate state changes. |
| Governance modification | `PROHIBIT` | No reviewed evidence supports changing governance through Atlas. |

## 7. Recommended V2 Priority Order

1. Source Quality / Noise Filtering: `V2_BUILD_NOW`.
2. Research Packet Compacting: `V2_BUILD_NOW`.
3. Lexical Blind Spot Reduction: `V2_BUILD_NOW`.
4. Coverage Confidence: `V2_BUILD_NOW`.
5. Evidence Flow Dashboard: `V2_BUILD_LATER`.
6. Concentration Monitoring: `V2_BUILD_LATER`.
7. North Star Trend Tracking: `V2_BUILD_LATER`.
8. Direct Runtime Artifact Coverage: `V2_BUILD_LATER`.
9. Evidence Quality Layer: `DEFER_TO_V3`.
10. Evidence Strength Classification: `DEFER_TO_V3`.

Priority rationale:
- The first four reduce current operational friction without expanding authority.
- The next four make North Star evidence easier to monitor after packet quality improves.
- Evidence-quality and strength-classification features are deferred because they risk turning Atlas from preparation into judgment.

## 8. V2 Entry Criteria

Atlas V2 work should begin only if the following remain true:

- V1 continues to reduce evidence discovery effort in real usage cycles.
- V1 continues to preserve read-only, citation-bound, non-authoritative boundaries.
- V1 limitations remain operationally material: noise, duplication, lexical blind spots, indirect failure reuse, or weak direct artifact coverage.
- The proposed V2 task can be implemented without schemas, runtime truth changes, governance changes, candidate changes, sleeve changes, trade advice, or allocation logic.
- The proposed V2 task improves preparation quality, not decision authority.
- Coverage confidence is framed as retrieval confidence, not evidence completeness.
- North Star metrics remain focused on distributed mature validation evidence, evidence maturity, outcome flow, candidate-to-paper conversion quality, and evidence concentration.

Do not enter V2 for:
- Search-space expansion.
- Authority transfer.
- Autonomous validation.
- Capital allocation.
- Trade recommendation.
- Sleeve or candidate mutation.
- Runtime truth mutation.

## 9. Final Recommendation

Begin Atlas V2 planning with a narrow preparation-quality backlog:

1. Source Quality / Noise Filtering.
2. Research Packet Compacting.
3. Lexical Blind Spot Reduction.
4. Coverage Confidence.

Build metric-facing capabilities later:

5. Evidence Flow Dashboard.
6. Concentration Monitoring.
7. North Star Trend Tracking.
8. Direct Runtime Artifact Coverage.

Defer evidence-judgment capabilities to V3:

9. Evidence Quality Layer.
10. Evidence Strength Classification.

Prohibit authority-bearing capabilities in V2.

Final classification summary:
- `V2_BUILD_NOW`: Source Quality / Noise Filtering, Research Packet Compacting, Lexical Blind Spot Reduction, Coverage Confidence.
- `V2_BUILD_LATER`: Evidence Flow Dashboard, Concentration Monitoring, North Star Trend Tracking, Direct Runtime Artifact Coverage.
- `DEFER_TO_V3`: Evidence Quality Layer, Evidence Strength Classification, broader research historian behavior, knowledge graph expansion, bounded hypothesis or experiment-design drafting assistants.
- `PROHIBIT`: Search Space Evolution, Chief Scientist, Research Economist, autonomous validation, autonomous capital allocation, autonomous sleeve mutation, trade recommendation authority, runtime truth mutation, candidate mutation, governance modification.

Atlas V2 should optimize evidence preparation quality before any dashboard, trend, or runtime-artifact expansion. The goal is better human review with less noise and fewer missed sources, not more autonomous research authority.
