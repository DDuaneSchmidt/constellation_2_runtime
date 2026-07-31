# Research Debt Dashboard

Generated: `2026-06-05T19:34:52Z`

Scope: measurement-only dashboard from existing Atlas research reports. No production, governance, authority, candidate, replay, qualification, validation, paper-forward, trading, broker, position-sizing, or capital-allocation changes.

Debt Score: `97.5`

Debt Trend: `IMPROVING`

Debt score decreased by 2.5 points from the previous dashboard.

Debt Reduction Progress: `0.1%` total count reduction from inventory baseline (`1` of `819` items reduced).

## Metrics

| Metric | Severity | Current count | Baseline | Reduced | Progress | Source |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Missing datasets | CRITICAL | 14 | 14 | 0 | 0.0% | reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md |
| Unvalidated candidates | CRITICAL | 7 | 8 | 1 | 12.5% | reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json |
| Proxy-dependent candidates | CRITICAL | 600 | 600 | 0 | 0.0% | reports/atlas_v2_research_os/final_candidate_ranking/latest.json |
| Stale observations | HIGH | 47 | 47 | 0 | 0.0% | research_journal/reports/open_paper_position_outcome_follow_through_review_001.md |
| Unresolved adversary findings | HIGH | 150 | 150 | 0 | 0.0% | reports/atlas_v2_research_os/research_adversary_corpus/corpus_summary.md |

## Scoring

Debt Score is a weighted 0-100+ index where the current count for each tracked debt class is divided by its inventory baseline and multiplied by its weight. Scores above 100 are possible if debt grows beyond the baseline.

| Metric | Weight |
| --- | ---: |
| Missing datasets | 0.25 |
| Unvalidated candidates | 0.20 |
| Proxy-dependent candidates | 0.25 |
| Stale observations | 0.15 |
| Unresolved adversary findings | 0.15 |

## Remediation Queue

- `missing_datasets`: Stage read-only market-data readiness by validation leverage: DIA/QQQ, TLT/USO/DBC, then high-overlap single-stock symbols.
- `unvalidated_candidates`: Route highest-ranked candidates through direct validation before stronger interpretation.
- `proxy_dependent_candidates`: Treat proxy-ranked candidate conclusions as research-only until candidate-specific replay exists.
- `stale_observations`: Continue deterministic outcome follow-through and refresh validation samples after natural closure events.
- `unresolved_adversary_findings`: Human-score a bounded adversary sample for exact, partial, miss, and false-positive outcomes.

## Machine-Readable Counts

```json
{
  "baselines": {
    "missing_datasets": 14,
    "proxy_dependent_candidates": 600,
    "stale_observations": 47,
    "unresolved_adversary_findings": 150,
    "unvalidated_candidates": 8
  },
  "counts": {
    "missing_datasets": 14,
    "proxy_dependent_candidates": 600,
    "stale_observations": 47,
    "unresolved_adversary_findings": 150,
    "unvalidated_candidates": 7
  },
  "debt_reduction_progress_percent": 0.1,
  "debt_score": 97.5,
  "debt_trend": "IMPROVING",
  "generated_at": "2026-06-05T19:34:52Z"
}
```
