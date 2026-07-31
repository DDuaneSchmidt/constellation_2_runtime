# Adversary Taxonomy Impact Study 001

Objective: estimate whether Atlas-specific failure knowledge is likely to improve Research Adversary usefulness enough to justify implementation.

Scope: research-process estimate only. This report does not modify Research Adversary behavior, taxonomy state, failure records, candidate state, replay state, qualification state, governance state, paper-forward state, broker execution, live trading, position sizing, or capital allocation.

Inputs reviewed:
- `reports/atlas_v2_research_os/research_adversary_validation/failure_recall_study_001.md`
- `research_journal/design/atlas_failure_taxonomy_001.md`
- `research_journal/reports/atlas_failure_taxonomy_validation_001.md`
- `research_journal/reports/assumption_recall_study_001.md`
- `research_journal/reports/research_hours_saved_estimate_001.md`
- `research_journal/design/research_adversary_metrics_001.md`
- `research_journal/design/research_adversary_v1_1_design_001.md`

Authority boundary:
- This is an estimate, not a measured production result.
- It does not authorize candidate promotion, candidate rejection, replay override, qualification override, governance override, automatic memory writes, trade recommendations, capital recommendations, broker execution, live trading, or position sizing.
- Any future taxonomy implementation should remain generated-only, citation-bound, and human-reviewed.

## Baseline

Current Failure Recall: 23.7%.

Source basis: `failure_recall_study_001.md` evaluated 19 known failures, with 0 direct hits, 9 partial hits, and 10 misses. The implied weighted recall is:

```text
(0 direct + 0.5 * 9 partial) / 19 = 23.7%
```

Current Assumption Recall: 39.1%.

Source basis: `assumption_recall_study_001.md` reviewed 32 labeled hidden assumptions, with 1 found, 23 partial, and 8 missed:

```text
(1 found + 0.5 * 23 partial) / 32 = 39.1%
```

Current Research Hours Saved: estimated opportunity of 115 hours across 33 reviewed cases, but the current adversary evaluation summary still reports 0 measured cases and 0 measured hours saved. Therefore hours saved should be treated as an implementation opportunity, not a validated metric.

## Taxonomy Evidence

The taxonomy changes the problem from generic critique to explicit negative-knowledge lookup.

`atlas_failure_taxonomy_001.md` provides concrete categories for recurring Atlas failures, including:
- `REGIME_DEPENDENCY`
- `PROXY_DEPENDENCY`
- `RUNTIME_DEPENDENCY`
- `DATA_QUALITY`
- `WORKER_COMPATIBILITY`
- `CERTIFICATION_BLOCKER`
- `WARNING_RECURRENCE`
- `DUPLICATE_CLUSTER`
- `INSUFFICIENT_EVIDENCE`
- `MECHANISM_MISMATCH`
- `QUALIFICATION_MISMATCH`
- `OBSERVATION_DRIFT`
- `SELECTION_BIAS`
- `CONTEXT_OMISSION`

`atlas_failure_taxonomy_validation_001.md` found:
- failures reviewed: 19
- taxonomy-covered failures: 19
- taxonomy coverage: 100.0%
- ambiguous failures: 5
- ambiguity rate: 26.3%
- multi-category failures: 6
- multi-category rate: 31.6%

Interpretation: the taxonomy is broad enough to cover the known failure set, but not clean enough to expect perfect automated recall. Human-readable category matches should improve recall materially; unsupervised primary-category assignment will still need precedence rules.

## Expected Failure Recall With Taxonomy

Expected Recall With Taxonomy: 70%.

Estimated range: 63%-79%.

Rationale:
- The current 23.7% baseline is low because the adversary produced broad warnings without explicit Atlas failure-category matching.
- The taxonomy validation shows 100% historical coverage across the same 19-failure evaluation set.
- However, 26.3% ambiguity and 31.6% multi-category overlap mean some taxonomy matches will be partial, noisy, or require reviewer confirmation.
- A realistic implementation should not get full credit for every covered category. It should get credit only when it names the relevant Atlas failure class, links it to the reviewed artifact, and preserves source lineage.

Conservative scoring model:

```text
Known failures: 19
Expected direct or strong category recoveries: 10-12
Expected partial category recoveries: 5-6
Expected misses: 2-4

Central weighted recall:
(11 direct + 0.5 * 5 partial) / 19 = 71.1%
Rounded expected recall: 70%
```

This clears the `failure_mode_recall` minimum useful threshold of 60% from `research_adversary_metrics_001.md` and moves the adversary out of the retirement zone below 35%.

## Expected Assumption Recall Impact

Expected Assumption Recall With Taxonomy: 58%-65%.

Central estimate: 62%.

Rationale:
- Current weighted assumption recall is 39.1%.
- The existing adversary already provides broad useful assumption coverage, with 75.0% useful assumption rate, but most matches are partial.
- The taxonomy directly targets many exact misses from the assumption study: proxy dependency, candidate-volume-as-quality, runtime dependency, warning recurrence, worker compatibility, artifact availability, certification blocker interpretation, and duplicate-cluster assumptions.
- The taxonomy will not automatically solve all assumption extraction. Candidate-specific details such as trigger subjectivity, event metadata, small-sample profit-factor instability, and parameter sensitivity still require artifact-specific review.

Expected movement:
- Historical failure assumption recall should move from 31.6% toward roughly 58%-63%.
- Candidate retrospective assumption recall should move from 50.0% weighted recall toward roughly 62%-68%, mostly by converting proxy, duplicate-cluster, and regime assumptions from partial to more exact matches.

Judgment: enough to justify implementation if the implementation requires category-to-assumption linkage and does not merely append taxonomy labels.

## Expected Research Hours Saved Impact

Expected net research hours saved: 55-85 hours across a similar 33-case review set.

Upper opportunity estimate: 115 hours.

Rationale:
- `research_hours_saved_estimate_001.md` estimates 115 hours of potential savings across 33 retrospective cases.
- That estimate assumes earlier adversary review would kill, narrow, or accelerate rejection of weak items.
- Taxonomy lookup makes those savings more plausible because it converts repeated Rediscovery work into explicit prior-failure retrieval.
- The full 115-hour estimate should not be credited yet because implementation will add review overhead and current human-scored evaluation is incomplete.

Practical expectation:
- Same-cycle fail-fast savings for worker compatibility, artifact availability, and certification blockers should be high-confidence but small per case.
- Medium-to-large savings should come from avoiding repeated debates around evidence maturity, authority readiness, proxy dependency, fixture generalization, and candidate volume as quality.
- Net savings should remain positive if taxonomy review overhead stays below 0.5 hours per artifact and reviewers can suppress irrelevant or weak analogy matches.

## Implementation Justification

The taxonomy is likely to move the three requested metrics enough to justify implementation.

| Metric | Current | Expected with taxonomy | Minimum useful threshold | Judgment |
| --- | ---: | ---: | ---: | --- |
| Failure Recall | 23.7% | 70% central; 63%-79% range | 60% | Justifies implementation |
| Assumption Recall | 39.1% | 62% central; 58%-65% range | 65% | Borderline but likely useful |
| Research Hours Saved | 0 measured; 115 estimated opportunity | 55-85 net estimated hours per 33 similar cases | >= 0.25 hours/item | Justifies implementation if overhead is controlled |

Assumption Recall is the weakest case. The taxonomy probably moves it close to the minimum useful threshold, but it should be measured before claiming success. Failure Recall is the strongest case because the taxonomy already covers the known failure set and directly addresses the current miss pattern.

## Recommended Implementation Boundary

Implement only a read-only taxonomy lookup and critique surface:
- match reviewed artifacts to known Atlas failure categories
- cite source failure IDs or taxonomy entries
- mark confidence and ambiguity
- emit falsification questions tied to the matched category
- separate direct matches from weak analogies
- require human review for category acceptance

Do not implement:
- automatic rejection
- candidate promotion or demotion
- replay, qualification, governance, or certification override
- paper-placement influence
- automatic memory writes
- trading, capital, broker, or position-sizing behavior

## Measurement Gate

After implementation, rerun a 20-item minimum evaluation with:
- `EXACT`, `PARTIAL`, `MISS`, and `FALSE_POSITIVE` labels for failure recall
- assumption labels separated into generic assumptions and Atlas-specific assumptions
- measured reviewer overhead
- measured review time saved or avoided
- authority-violation check

Advancement condition:
- Failure Recall >= 60%
- Assumption Recall >= 60% immediately, with a path to >= 65%
- false positive rate <= 30%
- net hours saved >= 0.25 hours per reviewed item
- authority violation rate = 0

## Final Judgment

Expected Recall With Taxonomy: 70%.

The taxonomy is likely worth implementing as a generated-only, read-only adversary aid. It should materially improve Failure Recall, probably improve Assumption Recall to near the useful threshold, and make a meaningful share of the estimated 115 hours of avoided research work realizable.

The implementation is justified only if it remains a retrieval-and-critique layer. The taxonomy should help reviewers remember known Atlas failures; it should not become an authority surface.
