# Direct Replay Coverage Roadmap 001

Status: PLANNING ONLY

Scope: executable roadmap to move Atlas direct replay coverage from 6.67% toward 75%+. No downloads, implementation, replay changes, qualification changes, candidate changes, governance changes, production changes, capital authority, trading authority, broker execution, position sizing, paper placement, or candidate promotion are authorized by this report.

## Inputs Reviewed

- `reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md`
- `reports/atlas_v2_research_os/candidate_validation_queue_001.md`
- `reports/atlas_v2_research_os/candidate_symbol_attribution/latest.json`
- `research_journal/design/atlas_direct_replay_data_strategy_001.md`

## 1. Current State

- coverage_percent: `6.67%` unique-symbol coverage from the coverage audit; `6.25%` candidate-symbol-pair coverage from current attribution math.
- validation_block_rate: `100.00%` because all 8 candidate validations are currently `INSUFFICIENT_DATA`.
- symbols_with_data: `1` (`SPY`).
- symbols_missing_data: `14` (`AAPL`, `AMZN`, `BAC`, `DBC`, `DIA`, `GOOGL`, `JPM`, `META`, `MSFT`, `NFLX`, `QQQ`, `TLT`, `TSLA`, `USO`).
- candidate_count: `8`.
- candidate_symbol_count: `15`.
- candidate-symbol pairs: `32`, with `2` currently covered by existing SPY data.

## 2. Phase 1

Symbols: `DIA`, `QQQ`.

- candidates unlocked: `3` newly fully covered candidate universes (`ptc_backtest_final_3a4ac24107c77136`, `ptc_backtest_final_469607b8340421b7`, `ptc_backtest_final_854ad10b904e1ae9`).
- cumulative fully covered candidates: `3/8`.
- expected coverage gain: `+18.75%` candidate-symbol-pair coverage; cumulative candidate-symbol coverage `25.00%`; cumulative unique-symbol coverage `20.00%`.
- Expected validation gain: unlocks 3 full ETF-universe candidates and reduces validation block rate from `100.00%` to `62.50%` if daily files validate successfully. It also directly addresses the top three queue priorities.
- expected information gain: HIGH: unlocks the top DIA/QQQ/SPY ETF cluster, including the top two campaign-ranked breakout candidates and one mean-reversion comparator.

## 3. Phase 2

Symbols: `BAC`, `META`, `MSFT`, `TSLA`.

- candidates unlocked: `0` newly fully covered candidate universes (None).
- cumulative fully covered candidates: `3/8`.
- expected coverage gain: `+37.50%` candidate-symbol-pair coverage; cumulative candidate-symbol coverage `62.50%`; cumulative unique-symbol coverage `46.67%`.
- Expected validation gain: no additional full candidate universe is completed by this phase alone, but it materially de-risks the broad single-stock cluster and moves candidate-symbol coverage to `62.50%`.
- expected information gain: MEDIUM-HIGH: adds the highest-overlap single-stock core and tests whether equity candidates survive beyond ETF proxy behavior, but most broad equity universes remain incomplete.

## 4. Phase 3

Symbols: `AMZN`, `NFLX`.

- candidates unlocked: `1` newly fully covered candidate universes (`ptc_backtest_final_d5931b24bd391113`).
- cumulative fully covered candidates: `4/8`.
- expected coverage gain: `+12.50%` candidate-symbol-pair coverage; cumulative candidate-symbol coverage `75.00%`; cumulative unique-symbol coverage `60.00%`.
- Expected validation gain: reaches `75.00%` candidate-symbol-pair coverage and raises full-candidate coverage to `50.00%`; several universe candidates still need GOOGL, AAPL/JPM, or rates/commodity ETF symbols.
- expected information gain: MEDIUM: reaches 75% candidate-symbol coverage and reduces single-stock proxy dependence; still leaves full-candidate gaps for GOOGL, AAPL/JPM, and rates/commodity ETFs.

## 5. Phase 4

Remaining symbols: `AAPL`, `DBC`, `GOOGL`, `JPM`, `TLT`, `USO`.

- candidates unlocked: `4` newly fully covered candidate universes (`ptc_backtest_final_4df2e8e80685a054`, `ptc_backtest_final_624fdd85668e2c08`, `ptc_backtest_final_7d839944a8a4070a`, `ptc_backtest_final_b23c6756bfb3263a`).
- cumulative fully covered candidates: `8/8`.
- expected coverage gain: `+25.00%` candidate-symbol-pair coverage; cumulative candidate-symbol coverage `100.00%`; cumulative unique-symbol coverage `100.00%`.
- Expected validation gain: completes all current attributed symbol demand, reduces validation block rate to `0.00%` on symbol availability alone, and raises full-candidate coverage to `100.00%`; rule-specific sample sufficiency and intraday requirements may still block validation.
- expected information gain: HIGH COMPLETION VALUE: closes remaining current-universe gaps, unlocks rates/commodity ETF candidates and brings full-candidate coverage above 75%.

## 6. Recommendation

- Minimum symbol set for 50% candidate-symbol-pair coverage: `BAC, DIA, META, MSFT, QQQ`; reaches `53.12%` candidate-symbol coverage and `40.00%` unique-symbol coverage.
- Minimum symbol set for 50% unique-symbol coverage: `BAC, DIA, META, MSFT, QQQ, TSLA, AMZN`; reaches `68.75%` candidate-symbol coverage and `53.33%` unique-symbol coverage.
- Minimum symbol set for 75% candidate-symbol-pair coverage: `BAC, DIA, META, MSFT, QQQ, TSLA, AMZN, NFLX`; reaches `75.00%` candidate-symbol coverage and `60.00%` unique-symbol coverage.
- Minimum symbol set for 75% unique-symbol coverage: `BAC, DIA, META, MSFT, QQQ, TSLA, AMZN, NFLX, TLT, USO, AAPL`; reaches `90.62%` candidate-symbol coverage and `80.00%` unique-symbol coverage.
- Minimum symbol set for 90% candidate-symbol-pair coverage: `BAC, DIA, META, MSFT, QQQ, TSLA, AMZN, NFLX, TLT, USO, AAPL`; reaches `90.62%` candidate-symbol coverage and `80.00%` unique-symbol coverage.
- Minimum symbol set for 90% unique-symbol coverage: `BAC, DIA, META, MSFT, QQQ, TSLA, AMZN, NFLX, TLT, USO, AAPL, DBC, GOOGL`; reaches `96.88%` candidate-symbol coverage and `93.33%` unique-symbol coverage.

Practical recommendation:

- Execute Phase 1 first because `DIA` and `QQQ` unlock the top three validation queue candidates with only two datasets.
- Execute Phase 2 next because `BAC`, `META`, `MSFT`, and `TSLA` are the highest-overlap single-stock core.
- Execute Phase 3 to reach the explicit 75% candidate-symbol-pair target with `AMZN` and `NFLX`.
- Execute Phase 4 when the goal changes from 75% pair coverage to broad full-candidate coverage or 90%+ unique-symbol coverage.

## Coverage Tables

- `research_journal/reports/coverage_unlock_table.csv`
- `research_journal/reports/coverage_unlock_table.md`

## Authority Boundary

- No downloads.
- No implementation.
- No replay changes.
- No qualification changes.
- No candidate changes.
- No governance changes.
- No production changes.
- No live trading, broker execution, capital allocation, position sizing, portfolio construction, trade recommendations, paper placement, or candidate promotion.
