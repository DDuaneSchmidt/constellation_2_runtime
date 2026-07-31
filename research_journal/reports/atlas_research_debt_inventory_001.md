# Atlas Research Debt Inventory 001

Objective: quantify Atlas research debt using existing candidate, failure, replay, qualification, validation, and coverage reports.

Scope: inventory only. This report makes no production changes, governance changes, authority changes, candidate changes, replay changes, qualification changes, validation changes, paper-forward changes, broker-execution changes, trading changes, position-sizing changes, or capital-allocation changes.

Companion register: `research_journal/reports/research_debt_register.csv`

## Inputs

- `reports/atlas_v2_research_os/final_candidate_ranking/2026-06-05/final_candidate_ranking_report.json`
- `reports/atlas_v2_research_os/candidate_review/2026-06-05/candidate_review_report.json`
- `reports/atlas_v2_research_os/candidate_backtests/2026-06-05/candidate_backtest_report.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/2026-06-05/backtest_aware_final_qualification_report.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/2026-06-05/direct_candidate_data_validation_report.json`
- `reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md`
- `reports/atlas_v2_research_os/candidate_validation_queue_001.md`
- `reports/atlas_v2_research_os/claim_hypothesis_funnel/2026-06-05/claim_hypothesis_funnel_conversion_report.json`
- `reports/atlas_v2_research_os/observation_diversity_audit/2026-06-05/observation_diversity_audit_report.json`
- `reports/atlas_v2_research_os/observation_cluster_split_experiment/2026-06-05/observation_cluster_split_experiment.json`
- `reports/atlas_v2_research_os/research_adversary_corpus/corpus_summary.md`
- `reports/atlas_v2_research_os/research_adversary_evaluation/latest/research_adversary_evaluation_summary.json`
- `reports/atlas_v2_research_os/research_adversary_validation/failure_recall_study_001.md`
- `reports/atlas_v2_research_os/memory/failure_patterns.json`
- `research_journal/reports/atlas_failure_taxonomy_validation_001.md`
- `research_journal/reports/open_paper_position_outcome_follow_through_review_001.md`

## Summary

Atlas research debt is concentrated in four areas:

1. Direct candidate validation is blocked by missing market data.
2. Candidate and replay conclusions are still proxy-dependent.
3. Hypothesis and adversary surfaces have generated more review material than has been human-resolved.
4. Known failure knowledge exists in reports, but most of it is not encoded as reusable failure memory.

Severity counts in the register:

| Severity | Count |
| --- | ---: |
| CRITICAL | 3 |
| HIGH | 9 |
| MEDIUM | 5 |
| LOW | 1 |

## Inventory

| Category | Count | Severity | Estimated impact | Suggested remediation |
| --- | ---: | --- | --- | --- |
| Unvalidated Candidates | 600 proxy-dependent candidates; 58 ready-for-paper-forward-observation candidates; 8/8 validation-queue candidates insufficient-data | CRITICAL | Candidate ranking and paper-forward readiness can outrun direct candidate evidence. | Keep conclusions research-only; route highest-ranked candidates through direct data validation before stronger interpretation. |
| Missing Market Data | 14 of 15 attributed symbols missing local data; 8/8 direct validations blocked; 6.67% unique-symbol coverage | CRITICAL | Direct replay cannot validate candidate-specific behavior; SPY proxy dependence dominates. | Stage read-only data readiness by validation leverage: DIA/QQQ, then TLT/USO/DBC, then BAC/META/MSFT/TSLA, then completion symbols. |
| Unresolved Hypotheses | 17 READY hypothesis-validation backlog items; 600 post-split hypotheses with 0 eligible candidates | HIGH | Hypothesis generation and backlog intake can accumulate without resolving downstream validation or eligibility. | Triage READY validation rows by information gain, data availability, and duplicate risk before expanding generation volume. |
| Duplicate Candidate Clusters | 50/50 observation clusters over-merged; 10 mechanism clusters with 10 candidates each | HIGH | Over-merged clusters can hide symbol/timeframe/source-type differences; mechanism clusters can double-count similar ideas. | Use split dimensions already tested: symbol group, timeframe, and source type; review representative candidates per cluster. |
| Replay Coverage Gaps | 372/600 candidates BACKTEST_WEAK or INSUFFICIENT_DATA; 180 intraday/daily mismatch penalties; 156 missing-evidence and sample-size penalties | HIGH | Replay/backtest evidence is broad but incomplete; intraday mechanisms remain weakly approximated by daily proxy bars. | Separate insufficient-data repairs from weak replay retirements; require matched timeframe coverage for intraday mechanisms. |
| Qualification Gaps | 490/600 candidates below final score 0.7; 110/228 backtest-supported candidates pass final qualification | HIGH | Qualification suppresses most candidates; pass/fail status can reflect missing evidence as much as mechanism weakness. | Review final-score penalties and classify blockers as evidence repair, candidate redesign, or rejection. |
| Failure Patterns Not Yet Encoded | 18 of 19 reviewed historical failures not in explicit `failure_patterns.json`; 10 failure-recall misses | HIGH | Negative knowledge remains report-bound and can be missed by future reviews. | Keep implementation out of scope; for planning, prioritize read-only taxonomy lookup and human-scored recall measurement. |
| Stale Observations | 47 of 51 reviewed open paper positions not near deterministic closure; 2 paper-forward outcomes recorded | HIGH | Outcome maturity remains delayed by open positions and tiny closed-sample count. | Continue deterministic outcome follow-through and validation sample refresh without forcing closure or changing exits. |
| Unresolved Adversary Findings | 150 generated-only reviews; 750 assumptions; 1,070 constraints; 600 falsification tests; 0 human-evaluated cases | HIGH | Adversary output exists but has not become accepted, rejected, or measured review knowledge. | Human-score a bounded sample; classify findings and measure usefulness, false positives, and review overhead. |

## Top 10 Research Debt Items

| Rank | Severity | Debt item | Count | Why it matters |
| ---: | --- | --- | ---: | --- |
| 1 | CRITICAL | Direct validation blocked by missing candidate-universe market data | 8 | All queued direct validations are `INSUFFICIENT_DATA`; validation block rate is 100%. |
| 2 | CRITICAL | Unique candidate symbols missing local data | 14 | Only SPY is locally available across 15 attributed symbols, leaving direct coverage at 6.67%. |
| 3 | CRITICAL | Proxy-dependent ranked candidate universe | 600 | Every evaluated candidate remains exposed to proxy-dependency weakness. |
| 4 | HIGH | Final-score qualification suppression | 490 | Most generated candidates remain below the qualification threshold after backtest-aware scoring. |
| 5 | HIGH | Backtest evidence insufficient or weak | 372 | 216 candidates are `BACKTEST_WEAK` and 156 are `INSUFFICIENT_DATA`. |
| 6 | HIGH | Observation clustering over-merged source observations | 50 | 50/50 audited clusters are `OVER_MERGED`, risking loss of symbol/timeframe/source distinctions. |
| 7 | HIGH | Ready hypothesis-validation backlog not processed | 17 | Ready hypothesis-validation items remain queued after the run exhausted selection capacity. |
| 8 | HIGH | Open paper positions not near deterministic closure | 47 | Most open positions cannot yet mature into validation samples under existing closure rules. |
| 9 | HIGH | Generated-only adversary review corpus unresolved by human scoring | 150 | The corpus contains many assumptions and tests, but zero evaluated cases in the current summary. |
| 10 | HIGH | Historical failure taxonomy not encoded in failure memory | 18 | Only one explicit failure memory exists while 19 historical failures are covered by taxonomy validation. |

## Notes By Category

### 1. Unvalidated Candidates

Count: 600 candidate records carry proxy-dependency weakness in the final candidate ranking. Of those, 58 are classified `READY_FOR_PAPER_FORWARD_OBSERVATION`, 110 are final eligible after backtest-aware qualification, and the 8-candidate direct validation queue is still 100% insufficient-data.

Severity: CRITICAL.

Estimated impact: candidate ranking can create a strong review surface before candidate-specific validation exists.

Suggested remediation: keep candidate status advisory and research-only; resolve direct data coverage for the highest-priority validation queue before treating proxy evidence as durable.

### 2. Missing Market Data

Count: 14 missing symbols out of 15 attributed symbols. Only `SPY` has local coverage. Direct validation block rate is 100%.

Severity: CRITICAL.

Estimated impact: missing data is the primary blocker preventing direct replay validation of candidate-specific behavior.

Suggested remediation: stage data readiness by validation leverage: `DIA`/`QQQ`, then `TLT`/`USO`/`DBC`, then `BAC`/`META`/`MSFT`/`TSLA`, then completion symbols.

### 3. Unresolved Hypotheses

Count: 17 ready hypothesis-validation backlog items. The observation split experiment also produced 600 post-split hypotheses with 0 eligible candidates and 0 paper-forward-ready candidates.

Severity: HIGH.

Estimated impact: Atlas can produce hypothesis volume without resolving validation or eligibility.

Suggested remediation: rank ready hypothesis-validation rows by information gain, data availability, source lineage, and duplicate risk before generating more.

### 4. Duplicate Candidate Clusters

Count: 50 of 50 audited observation clusters are `OVER_MERGED`. Candidate review also reports 10 mechanism clusters with 10 candidates each.

Severity: HIGH.

Estimated impact: over-merged clusters may hide materially different symbol, timeframe, and source-type behavior; mechanism repetition can inflate perceived evidence.

Suggested remediation: use split dimensions already demonstrated in the split experiment and keep cluster decisions human-reviewed.

### 5. Replay Coverage Gaps

Count: 216 `BACKTEST_WEAK`, 156 `INSUFFICIENT_DATA`, 180 intraday/daily mismatch penalties, 156 missing-evidence penalties, and 156 sample-size penalties.

Severity: HIGH.

Estimated impact: replay coverage is broad, but weak or proxy-bound for many candidates.

Suggested remediation: first separate data insufficiency from negative replay evidence; then require matched timeframe data for intraday mechanisms.

### 6. Qualification Gaps

Count: 490 of 600 candidates remain below final score 0.7. Only 110 of 228 backtest-supported candidates pass final qualification.

Severity: HIGH.

Estimated impact: qualification state is doing substantial suppression, but the causes mix true weakness with missing evidence and proxy penalties.

Suggested remediation: classify each major disqualification reason as data repair, candidate redesign, or rejection evidence.

### 7. Failure Patterns Not Yet Encoded

Count: 19 historical failures are covered by taxonomy validation, but only 1 explicit failure memory exists in `failure_patterns.json`. Failure recall evaluation shows 10 misses and 9 partial hits across 19 known failures.

Severity: HIGH.

Estimated impact: recurring Atlas-specific negative knowledge remains easy to miss unless a reviewer manually reads reports.

Suggested remediation: no implementation in this task; the planning priority is a human-scored, read-only taxonomy lookup evaluation.

### 8. Stale Observations

Count: 47 of 51 reviewed open paper positions are not near deterministic closure. Only 2 paper-forward outcomes are recorded in the reviewed outcome report.

Severity: HIGH.

Estimated impact: observations remain open faster than they convert into closed validation samples.

Suggested remediation: continue read-only follow-through on existing positions and refresh validation samples after deterministic closure events.

### 9. Unresolved Adversary Findings

Count: 150 generated-only adversary reviews, 750 assumptions, 1,070 constraints, and 600 falsification tests. Current evaluation summary has 0 cases evaluated and 0 measured hours saved.

Severity: HIGH.

Estimated impact: adversary output may be useful, but it is unmeasured debt until humans accept, reject, or classify it.

Suggested remediation: score a bounded sample for exact/partial/miss/false-positive outcomes and track review overhead.

## Final Judgment

Atlas research debt is CRITICAL where missing data blocks direct validation and HIGH where generated research surfaces exceed resolved evidence. The most valuable next research work is not more candidate volume. It is debt reduction: direct data coverage, validation triage, replay gap separation, qualification blocker attribution, stale-observation follow-through, and human scoring of adversary findings.

This report is inventory-only and does not change production, governance, or authority.
