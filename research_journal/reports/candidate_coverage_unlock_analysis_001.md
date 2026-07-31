# Candidate Coverage Unlock Analysis 001

Date: 2026-06-05
Status: Analysis only

Scope: models expected candidate validation unlock from adding missing direct replay symbol coverage. This report does not implement data loading, download data, create integrations, alter replay state, change qualification, create candidates, approve paper-forward activity, recommend trades, allocate capital, size positions, or authorize broker execution.

## Inputs

- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/candidate_data_validation_plan/latest.json`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/candidate_backtests/latest.json`
- `research_journal/design/atlas_direct_replay_data_strategy_001.md`

## Baseline

Focused direct-data validation currently covers 8 campaign candidates.

Baseline status:

- candidates reviewed: 8
- classification: 8 `INSUFFICIENT_DATA`
- direct data exists locally: 2
- direct replays run: 2
- confirmed: 0
- weakened: 0
- primary gap: partial direct data only; missing local CSVs for attributed symbols
- current proxy evidence: SPY daily proxy evidence

The focused set contains 1,215 proxy replay sample-equivalents across the 8 candidates. These are not direct validation samples yet; they are the amount of existing proxy replay evidence that could be converted into candidate-specific evidence if the required symbol coverage exists.

## Model

Definitions:

- `partially_validated`: a candidate has at least one newly covered direct symbol from its resolved symbol set.
- `fully_validated`: all currently missing direct symbols for that candidate are covered.
- `replay_evidence_improves`: proxy replay sample-equivalents that become directly inspectable, allocated by each missing symbol's share of the candidate's missing symbol set.

Conservative assumption:

- A single newly covered symbol only fully validates a candidate when that one symbol is the candidate's only missing direct symbol.
- In this focused set, no candidate has only one missing direct symbol, so single-symbol full validation impact is 0.
- Cumulative symbol sets can fully validate candidates when the full required symbol set is covered.

Existing local direct proxy coverage is treated as `SPY`; missing direct symbols are evaluated separately.

## Missing Symbols

Missing symbols in the focused set:

```text
AAPL, AMZN, BAC, DBC, DIA, GOOGL, JPM, META, MSFT, NFLX, QQQ, TLT, TSLA, USO
```

## Per-Symbol Unlock Model

| Missing Symbol | Candidates Partially Validated | Candidates Fully Validated Alone | Replay Evidence Improves, Sample-Equivalent | Candidate Mechanism Exposure |
| --- | ---: | ---: | ---: | --- |
| DIA | 3 | 0 | 187.0 | BREAKOUT, MEAN_REVERSION |
| QQQ | 3 | 0 | 187.0 | BREAKOUT, MEAN_REVERSION |
| TSLA | 3 | 0 | 86.0 | EVENT_REACTION, REVERSAL, BREAKOUT |
| META | 3 | 0 | 86.0 | EVENT_REACTION, REVERSAL, BREAKOUT |
| BAC | 3 | 0 | 86.0 | EVENT_REACTION, REVERSAL, BREAKOUT |
| MSFT | 3 | 0 | 86.0 | EVENT_REACTION, REVERSAL, BREAKOUT |
| USO | 2 | 0 | 107.7 | MEAN_REVERSION, BREAKOUT |
| TLT | 2 | 0 | 107.7 | MEAN_REVERSION, BREAKOUT |
| AMZN | 2 | 0 | 69.0 | EVENT_REACTION, REVERSAL |
| NFLX | 2 | 0 | 33.0 | EVENT_REACTION, BREAKOUT |
| DBC | 1 | 0 | 56.7 | MEAN_REVERSION |
| AAPL | 1 | 0 | 53.0 | REVERSAL |
| JPM | 1 | 0 | 53.0 | REVERSAL |
| GOOGL | 1 | 0 | 17.0 | BREAKOUT |

## Top 5 Symbols By Validation Impact

Ranked by partial candidate unlock first, then replay evidence improvement.

| Rank | Symbol | Partial Candidates | Full Candidates Alone | Replay Evidence Improves | Why It Matters |
| ---: | --- | ---: | ---: | ---: | --- |
| 1 | DIA | 3 | 0 | 187.0 | Highest sample-equivalent impact; pairs with QQQ to fully unlock 3 candidates. |
| 2 | QQQ | 3 | 0 | 187.0 | Same candidate cluster as DIA; together they complete the DIA/QQQ focused cluster. |
| 3 | TSLA | 3 | 0 | 86.0 | Appears across event reaction, reversal, and breakout exposure. |
| 4 | META | 3 | 0 | 86.0 | Appears across event reaction, reversal, and breakout exposure. |
| 5 | BAC | 3 | 0 | 86.0 | Appears across event reaction, reversal, and breakout exposure, with sector-specific value. |

MSFT ties BAC on modeled counts and replay-equivalent impact. BAC is listed in the top 5 because it also broadens sector coverage beyond mega-cap technology; MSFT should be treated as co-equal if breadth is not a priority.

## Cumulative Unlock Scenarios

| Added Symbol Set | Candidates Partially Validated | Candidates Fully Validated | Replay Evidence Partially Improved | Fully Direct Proxy Evidence Replaced |
| --- | ---: | ---: | ---: | ---: |
| DIA | 3 | 0 | 187.0 | 0 |
| DIA + QQQ | 3 | 3 | 374.0 | 374 |
| DIA + QQQ + TSLA | 6 | 3 | 460.0 | 374 |
| DIA + QQQ + TSLA + META | 6 | 3 | 546.0 | 374 |
| DIA + QQQ + TSLA + META + BAC | 6 | 3 | 632.0 | 374 |
| All 14 missing symbols | 8 | 8 | 1,215.0 | 1,215 |

Interpretation:

- DIA and QQQ are a high-impact pair, not just high-impact individual symbols.
- Adding DIA alone or QQQ alone improves evidence visibility but does not fully validate any focused candidate.
- Adding both DIA and QQQ fully covers three focused candidates because their missing direct universe is exactly `DIA, QQQ` after SPY proxy coverage.
- The next tier, TSLA/META/BAC/MSFT, broadens partial validation across event, reversal, and breakout candidates, but does not fully validate those candidates without the rest of each candidate's symbol set.

## Candidate-Level Unlock Notes

| Candidate | Mechanism / Regime | Missing Symbols | Proxy Sample Size | Full Unlock Requires |
| --- | --- | --- | ---: | --- |
| `ptc_backtest_final_469607b8340421b7` | BREAKOUT / CHOP | DIA, QQQ | 102 | DIA + QQQ |
| `ptc_backtest_final_3a4ac24107c77136` | BREAKOUT / CHOP | DIA, QQQ | 102 | DIA + QQQ |
| `ptc_backtest_final_624fdd85668e2c08` | MEAN_REVERSION / TRENDING | DBC, TLT, USO | 170 | DBC + TLT + USO |
| `ptc_backtest_final_854ad10b904e1ae9` | MEAN_REVERSION / TRENDING | DIA, QQQ | 170 | DIA + QQQ |
| `ptc_backtest_final_d5931b24bd391113` | EVENT_REACTION / CHOP | AMZN, BAC, META, MSFT, NFLX, TSLA | 96 | All six symbols plus event timing data |
| `ptc_backtest_final_4df2e8e80685a054` | REVERSAL / TRENDING | AAPL, AMZN, BAC, JPM, META, MSFT, TSLA | 371 | All seven symbols |
| `ptc_backtest_final_7d839944a8a4070a` | BREAKOUT / CHOP | TLT, USO | 102 | TLT + USO |
| `ptc_backtest_final_b23c6756bfb3263a` | BREAKOUT / CHOP | BAC, GOOGL, META, MSFT, NFLX, TSLA | 102 | All six symbols |

## Replay Evidence Improvement

Replay evidence improves in three ways:

1. Proxy evidence becomes directly testable on candidate-attributed symbols.
2. Candidate-specific sample counts can be measured instead of inferred from SPY daily proxy triggers.
3. Mechanism/regime claims can be weakened, confirmed, or split by symbol instead of staying aggregated.

Expected impact:

- Individual symbol coverage mostly creates partial validation.
- Symbol pairs or full symbol sets create full validation.
- DIA + QQQ is the first pair that materially unlocks full validation in the focused set.
- Full 14-symbol coverage would allow all 8 focused candidates to move from `INSUFFICIENT_DATA` to direct replayable, subject to sample-size and mechanism-field validation.

Important limitation: for `EVENT_REACTION`, symbol OHLCV alone is not enough for full validation. Event timestamp and event calendar coverage are also required.

## Prioritized Symbol Acquisition Order

Analysis-only priority:

1. DIA
2. QQQ
3. TSLA
4. META
5. BAC
6. MSFT
7. TLT
8. USO
9. AMZN
10. NFLX
11. DBC
12. AAPL
13. JPM
14. GOOGL

Rationale:

- DIA and QQQ together fully unlock 3 of 8 focused candidates.
- TSLA, META, BAC, and MSFT each touch 3 candidates across multiple mechanisms.
- TLT and USO each touch 2 candidates and pair together to fully unlock one breakout candidate, but they need DBC to fully unlock the related mean-reversion candidate.
- AAPL, JPM, and GOOGL have lower marginal focused-set impact but remain relevant for broader universe completeness.

## Coverage Unlock Conclusion

The highest-impact immediate unlock is not a single symbol. It is the `DIA + QQQ` pair.

Top 5 symbols by validation impact:

```text
DIA
QQQ
TSLA
META
BAC
```

With only the top 5 symbols, 6 of 8 focused candidates become partially validated and 3 of 8 become fully validated. With all 14 missing symbols, all 8 focused candidates become directly replayable, subject to event metadata and sample-size validation.

## Authority Boundary

This report is analysis only. It does not download data, implement data loaders, create integrations, alter replay, alter qualification, create candidates, approve paper-forward activity, recommend trades, allocate capital, size positions, authorize broker execution, or change governance state.
