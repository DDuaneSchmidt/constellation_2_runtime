# Research Director Evaluation 001

Created: 2026-06-05T23:55:08Z

Status: `GENERATED_ONLY`

Scope: offline evaluation-only Research Director ranking test. No production integration, production authority, candidate promotion, replay override, qualification override, governance override, trading recommendation, capital authority, broker execution, position sizing, or automatic memory write.

## Inputs

- observations
- claims
- hypotheses
- failures
- research_debt
- adversary_reviews

## Outputs

- priority rankings
- uncertainty rankings
- research debt rankings

## Scoring Model

- priority_score: `0.30*information_gain + 0.25*bottleneck_removal + 0.15*evidence_strength + 0.10*uncertainty_reduction + 0.10*research_cost_score + 0.10*debt_reduction`
- uncertainty_score: `0.45*uncertainty_reduction + 0.25*evidence_gap + 0.20*decision_blocking + 0.10*ambiguity`
- research_debt_score: `0.40*debt_reduction + 0.25*recurrence + 0.20*maintenance_drag + 0.15*reuse_value`

## Priority Rankings

| Rank | Workstream | Score | Target Bottleneck | Evidence | Notes |
| ---: | --- | ---: | --- | --- | --- |
| 1 | Data Coverage | 0.945 | YES | Initial direct replay coverage was 6.67%, with 8/8 validations blocked by insufficient data; acquisition changed coverage and exposed later bottlenecks. | hard validation blocker; first bottleneck in sequence |
| 2 | Replay Attrition | 0.895 | YES | After daily coverage improved, multiple candidates still produced zero usable samples after trigger/regime filtering or daily-vs-intraday proxy replay. | second bottleneck after coverage repair; explains candidate-specific collapse |
| 3 | Vocabulary Mismatch | 0.879 | YES | Five candidates had trigger samples but zero final samples because `CHOP` is not emitted by the daily proxy regime classifier. | third bottleneck in sequence; specific replay vocabulary diagnosis |
| 4 | Failure Taxonomy | 0.727 | NO | Improves classification and reuse of failure patterns, but classifies blockers rather than removing the active replay validation blocker. | useful downstream analysis; not the immediate unblocker |
| 5 | Research Adversary | 0.665 | NO | Raises critique quality and falsification discipline, but generated-only review does not itself add data, replay samples, or vocabulary compatibility. | valuable adversarial layer; not direct bottleneck removal |
| 6 | Research Economics | 0.624 | NO | Helps quantify expected value and opportunity cost, but depends on evidence production being unblocked first. | useful prioritization layer; not an evidence-producing fix |
| 7 | Search Space Mapping | 0.569 | NO | Broadens long-term exploration coverage, but it does not resolve the present direct-validation data and replay bottlenecks. | long-term value; deprioritized until validation works |
| 8 | More Hypothesis Generation | 0.376 | NO | Adds candidates or mechanisms, but additional hypotheses have low marginal value while existing candidates cannot be validated cleanly. | defer while validation bottlenecks remain |

## Uncertainty Rankings

| Rank | Workstream | Score | Why It Reduces Uncertainty |
| ---: | --- | ---: | --- |
| 1 | Replay Attrition | 0.892 | After daily coverage improved, multiple candidates still produced zero usable samples after trigger/regime filtering or daily-vs-intraday proxy replay. |
| 2 | Vocabulary Mismatch | 0.861 | Five candidates had trigger samples but zero final samples because `CHOP` is not emitted by the daily proxy regime classifier. |
| 3 | Data Coverage | 0.840 | Initial direct replay coverage was 6.67%, with 8/8 validations blocked by insufficient data; acquisition changed coverage and exposed later bottlenecks. |
| 4 | Failure Taxonomy | 0.579 | Improves classification and reuse of failure patterns, but classifies blockers rather than removing the active replay validation blocker. |
| 5 | Research Adversary | 0.538 | Raises critique quality and falsification discipline, but generated-only review does not itself add data, replay samples, or vocabulary compatibility. |
| 6 | Research Economics | 0.483 | Helps quantify expected value and opportunity cost, but depends on evidence production being unblocked first. |
| 7 | Search Space Mapping | 0.472 | Broadens long-term exploration coverage, but it does not resolve the present direct-validation data and replay bottlenecks. |
| 8 | More Hypothesis Generation | 0.299 | Adds candidates or mechanisms, but additional hypotheses have low marginal value while existing candidates cannot be validated cleanly. |

## Research Debt Rankings

| Rank | Workstream | Score | Debt Removed |
| ---: | --- | ---: | --- |
| 1 | Data Coverage | 0.905 | Initial direct replay coverage was 6.67%, with 8/8 validations blocked by insufficient data; acquisition changed coverage and exposed later bottlenecks. |
| 2 | Replay Attrition | 0.866 | After daily coverage improved, multiple candidates still produced zero usable samples after trigger/regime filtering or daily-vs-intraday proxy replay. |
| 3 | Vocabulary Mismatch | 0.826 | Five candidates had trigger samples but zero final samples because `CHOP` is not emitted by the daily proxy regime classifier. |
| 4 | Failure Taxonomy | 0.761 | Improves classification and reuse of failure patterns, but classifies blockers rather than removing the active replay validation blocker. |
| 5 | Research Adversary | 0.610 | Raises critique quality and falsification discipline, but generated-only review does not itself add data, replay samples, or vocabulary compatibility. |
| 6 | Research Economics | 0.577 | Helps quantify expected value and opportunity cost, but depends on evidence production being unblocked first. |
| 7 | Search Space Mapping | 0.537 | Broadens long-term exploration coverage, but it does not resolve the present direct-validation data and replay bottlenecks. |
| 8 | More Hypothesis Generation | 0.302 | Adds candidates or mechanisms, but additional hypotheses have low marginal value while existing candidates cannot be validated cleanly. |

## Source Presence

| Workstream | Source | Present |
| --- | --- | --- |
| Data Coverage | `reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md` | YES |
| Data Coverage | `reports/atlas_v2_research_os/market_data_acquisition/market_data_acquisition_001.md` | YES |
| Data Coverage | `research_journal/reports/insufficient_data_root_cause_analysis_001.md` | YES |
| Replay Attrition | `research_journal/reports/insufficient_data_root_cause_analysis_001.md` | YES |
| Replay Attrition | `reports/atlas_v2_research_os/replay_sample_yield/latest_summary.md` | YES |
| Replay Attrition | `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json` | YES |
| Vocabulary Mismatch | `research_journal/reports/regime_filter_root_cause_001.md` | YES |
| Vocabulary Mismatch | `research_journal/reports/regime_filter_root_cause_table.csv` | YES |
| Vocabulary Mismatch | `constellation_2/common/atlas_v2_research_os/candidate_backtests.py` | YES |
| Failure Taxonomy | `research_journal/reports/priority_rankings.md` | YES |
| Failure Taxonomy | `constellation_2/common/atlas_v2_research_os/atlas_failure_taxonomy.py` | YES |
| Research Adversary | `constellation_2/common/atlas_v2_research_os/research_adversary.py` | YES |
| Research Adversary | `constellation_2/common/atlas_v2_research_os/research_adversary_evaluation.py` | YES |
| Research Economics | `research_journal/reports/priority_rankings.md` | YES |
| Search Space Mapping | `constellation_2/common/atlas_v2_research_os/mechanism_search_space.py` | YES |
| Search Space Mapping | `research_journal/reports/priority_rankings.md` | YES |
| More Hypothesis Generation | `constellation_2/common/atlas_v2_research_os/mechanism_hypothesis_generator.py` | YES |

## Measurement

- Required target order: `DATA_COVERAGE, REPLAY_ATTRITION, VOCABULARY_MISMATCH`
- Actual top three priority order: `DATA_COVERAGE, REPLAY_ATTRITION, VOCABULARY_MISMATCH`
- Target bottlenecks above lower-value work: `True`
- Lower-value work maximum priority rank: `4`
- Result: `PASS`

## Interpretation

The evaluation-only Research Director ranks `Data Coverage`, `Replay Attrition`, and `Vocabulary Mismatch` as the top three workstreams. This matches the observed bottleneck sequence: first the candidate universe lacked direct market data coverage, then candidates with data still lost usable samples during replay, then the replay failure narrowed to a regime-label vocabulary mismatch where `CHOP` could not pass an exact filter against the daily proxy classifier vocabulary.

Lower-value work remains useful, but it is downstream or enabling work. Failure taxonomy, adversary review, economics, search-space mapping, and new hypothesis generation can improve research quality, but none removes the current validation bottleneck as directly as the three ranked bottlenecks.

## Authority Boundary

- status: `GENERATED_ONLY`
- evaluation_only: `True`
- read_only_inputs: `True`
- production_pipeline_integration_authorized: `False`
- automatic_memory_writes_authorized: `False`
- candidate_promotion_authorized: `False`
- replay_override_authorized: `False`
- qualification_override_authorized: `False`
- governance_override_authorized: `False`
- trade_recommendation_authorized: `False`
- capital_recommendation_authorized: `False`
- position_sizing_authorized: `False`
- broker_execution_authorized: `False`
