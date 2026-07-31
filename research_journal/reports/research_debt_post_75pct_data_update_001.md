# Research Debt Post 75pct Data Update 001

Generated: `2026-06-05T19:34:52Z`

Scope: measurement-only Atlas research debt report. No production, governance, authority, candidate promotion, replay override, qualification override, validation override, paper placement, live trading, broker execution, position sizing, or capital allocation changes.

Runtime truth boundary: pre-change `npm run aegis:audit` completed with strict verified graph `BLOCKED` and `audit_blocker_count=12`. Runtime truth kernel reported `runtime_truth_classification=PARTIAL_CONTEXT`, `highest_readiness_layer=BLOCKED`, `trade_advice_allowed=false`, `manual_trade_capture_allowed=false`, and `autonomous_execution_allowed=false`.

## Inputs

- `research_journal/reports/research_debt_dashboard.md`
- `reports/atlas_v2_research_os/market_data_acquisition/market_data_acquisition_001.md`
- `reports/atlas_v2_research_os/market_data_acquisition/coverage_delta_report_001.md`
- `reports/atlas_v2_research_os/market_data_acquisition/validation_delta_report_001.md`

Command run:

```bash
python3 research_debt_tracker.py
```

Tracker output:

```text
wrote /home/node/constellation/research_journal/reports/research_debt_dashboard.md
debt_score=97.5
```

## Summary

The 75% data acquisition materially reduced data-coverage debt and validation blockage, but it did not fully retire Atlas research debt because candidate ranking and proxy-dependency artifacts still need downstream regeneration or direct replay evidence.

Candidate-symbol pair coverage moved from `2/32` to `24/32`, reaching `75.0%`. Missing dataset coverage debt fell from `14` missing symbols to `6`, while validation blockage fell from `100.0%` to `50.0%`. The tracker-observed debt score improved from `100.0` to `97.5`; when the coverage delta report's post-acquisition missing-dataset count is applied to the tracker formula, the acquisition-normalized debt score is `83.2`.

## Before vs After

| Metric | Before | After | Change | Reduction |
| --- | ---: | ---: | ---: | ---: |
| Missing datasets | 14 | 6 | -8 | 57.1% |
| Unvalidated candidates | 8 | 7 | -1 | 12.5% |
| Proxy-dependent candidates | 600 | 600 | 0 | 0.0% |
| Validation block rate | 100.0% | 50.0% | -50.0 pp | 50.0% |
| Tracker debt score | 100.0 | 97.5 | -2.5 | 2.5% |
| Acquisition-normalized debt score | 100.0 | 83.2 | -16.8 | 16.8% |
| Tracker debt reduction progress | 0.0% | 0.1% | +0.1 pp | 1 of 819 items |
| Acquisition-normalized debt reduction progress | 0.0% | 1.1% | +1.1 pp | 9 of 819 items |

## Coverage Effect

| Coverage metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Unique-symbol coverage percent | 6.67% | 60.0% | +53.33 pp |
| Candidate-symbol pair coverage percent | 6.25% | 75.0% | +68.75 pp |
| Candidate-symbol pairs covered | 2/32 | 24/32 | +22 covered pairs |
| Candidate-symbol pairs uncovered | 30/32 | 8/32 | -22 uncovered pairs |
| Symbols with data | 1 | 9 | +8 |
| Symbols missing data | 14 | 6 | -8 |

Symbols acquired and validated: `DIA`, `QQQ`, `BAC`, `META`, `MSFT`, `TSLA`, `AMZN`, `NFLX`.

Remaining missing symbols: `AAPL`, `DBC`, `GOOGL`, `JPM`, `TLT`, `USO`.

## Validation Effect

| Validation metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| `INSUFFICIENT_DATA` candidates | 8 | 7 | -1 |
| `CONFIRMED` candidates | 0 | 1 | +1 |
| Direct data exists count | 2 | 6 | +4 |
| Direct replays run | 2 | 6 | +4 |
| Validation block rate | 100.0% | 50.0% | -50.0 pp |

The acquisition converted one candidate from insufficient data to confirmed and cut the validation block rate in half. The remaining validation debt is still material because seven candidates remain insufficient-data classified.

## Proxy Dependency

The dashboard-tracked proxy-dependent candidate count remains `600 -> 600`. That count is sourced from `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`, and the acquisition did not regenerate final candidate ranking or replace proxy-ranked conclusions with candidate-specific direct replay evidence.

The acquisition did reduce proxy pressure at the candidate-symbol requirement level: uncovered candidate-symbol pairs fell from `30` to `8`, a `73.3%` reduction. This is coverage progress, not yet candidate-ranking debt retirement.

## Debt Score Reconciliation

Tracker-observed score:

```text
missing_datasets=14
unvalidated_candidates=7
proxy_dependent_candidates=600
stale_observations=47
unresolved_adversary_findings=150
debt_score=97.5
debt_reduction_progress=0.1%
```

Acquisition-normalized score using the coverage delta report's post-acquisition missing dataset count:

```text
missing_datasets=6
unvalidated_candidates=7
proxy_dependent_candidates=600
stale_observations=47
unresolved_adversary_findings=150
debt_score=83.2
debt_reduction_progress=1.1%
```

Interpretation: the direct tracker only credited the one-candidate validation improvement because its missing-dataset source still resolves to `direct_replay_coverage_audit_001.md`. The coverage delta report shows the data acquisition reduced missing-symbol debt by eight additional symbols, but that reduction is not fully reflected in the tracker score until the canonical coverage audit input is refreshed or the tracker consumes the coverage delta artifact.

## Conclusion

The 75% acquisition reduced Atlas research debt in two measurable ways: it cut missing dataset debt from `14` to `6`, and it lowered validation blockage from `100.0%` to `50.0%`. The strongest remaining debt is unchanged proxy dependency: `600` proxy-dependent candidates remain until downstream ranking/direct replay artifacts prove candidate-specific evidence.

No production or authority state was changed.
