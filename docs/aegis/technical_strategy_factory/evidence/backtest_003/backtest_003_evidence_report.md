# BACKTEST_003 Evidence Report

This report is evidence only. It does not certify IMP_003 or CLAIM_001 and makes no capital allocation recommendation.

## References

- Implementation reference: IMP_003 - SPY Turtle Breakout
- Claim reference: CLAIM_001 - Trend Persistence
- Test plan reference: docs/aegis/technical_strategy_factory/20_test_plan_003_imp_003.md
- Execution spec reference: docs/aegis/technical_strategy_factory/21_backtest_003_execution_spec.md
- Data/baseline spec reference: docs/aegis/technical_strategy_factory/22_backtest_003_data_and_baseline_spec.md

## Run Details

- Data vendor: Tiingo cached local CSV
- Dataset: SPY_tiingo_adjusted_daily.csv
- Download timestamp: not_applicable_local_csv
- Date range: 2000-01-03 through 2026-06-02
- Price fields used: {"Close": "adjClose", "High": "adjHigh", "Low": "adjLow", "Open": "adjOpen"}
- Adjustment policy: local CSV adjusted OHLC columns used for signal, returns, and next-open execution
- Execution open proxy: adjOpen
- Run quality: PRIMARY_ADJUSTED_OHLC
- Run classification: PRIMARY_ADJUSTED_OHLC
- Cost model: Version 1, 10 bps round-trip, 5 bps entry and 5 bps exit
- Execution policy: signal after close, execute next market open; same-day execution prohibited
- Cash return policy: 0%

## Full-Period Metrics

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_003 | 102.7207% | 2.7116% | 8.5329% | 0.3570 | 0.3160 | -29.1593% | 12.9264% | 48.0054% | 159 | 159.0000 | 57.8987 |
| baseline_a_spy_buy_and_hold | 732.0195% | 8.3522% | 19.3104% | 0.5129 | 0.6521 | -55.2011% | 16.1701% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 212.0317% | 4.4026% | 9.4673% | 0.5034 | 0.6396 | -32.3036% | 8.3216% | 50.0000% | 318 | 3.2458 | n/a |

## Random Timing Baseline

| Metric | Value |
|---|---|
| median_random_cagr | -2.3944% |
| p05_random_cagr | -5.0387% |
| p95_random_cagr | 0.6556% |
| median_random_max_drawdown | -67.0784% |
| p05_random_max_drawdown | -81.7775% |
| p95_random_max_drawdown | -44.9428% |
| median_random_sharpe | -0.1133 |
| p05_random_sharpe | -0.3154 |
| p95_random_sharpe | 0.1161 |
| imp_003_percentile_rank_cagr | 99.4000% |
| imp_003_percentile_rank_sharpe | 100.0000% |
| imp_003_percentile_rank_max_drawdown | 0.1000% |

## Subperiod Metrics

### 2000-2003

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_003 | -15.0729% | -4.0102% | 7.7474% | -0.4910 | -0.4020 | -29.1593% | 19.3305% | 25.8964% | 21 | 21.0000 | 29.2000 |
| baseline_a_spy_buy_and_hold | -19.1438% | -5.1842% | 22.5270% | -0.1245 | -0.2005 | -47.5026% | 26.8318% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -8.6622% | -2.2442% | 11.1544% | -0.1483 | -0.2392 | -26.7913% | 14.1929% | 50.0000% | 48 | 48.0000 | n/a |

### 2008

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_003 | -1.5855% | -1.5909% | 4.1745% | -0.3619 | -0.1644 | -3.7819% | 2.9220% | 7.9051% | 2 | 2.0000 | 29.0000 |
| baseline_a_spy_buy_and_hold | -36.2488% | -36.3472% | 41.2838% | -0.8842 | -1.2268 | -47.1286% | 19.8571% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -19.5288% | -19.5888% | 19.3217% | -1.0275 | -1.4045 | -26.1944% | 10.6051% | 50.0000% | 12 | 12.0000 | n/a |

### 2020

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_003 | 7.1941% | 7.2196% | 13.4051% | 0.5861 | 0.4830 | -9.2774% | 6.4360% | 58.8933% | 6 | 6.0000 | 80.3333 |
| baseline_a_spy_buy_and_hold | 17.2764% | 17.3406% | 33.3837% | 0.6461 | 0.7314 | -33.6999% | 10.1508% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 9.1515% | 9.1843% | 16.1627% | 0.6231 | 0.7026 | -17.7425% | 5.1622% | 50.0000% | 12 | 12.0000 | n/a |

### 2022

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_003 | -14.7142% | -14.8738% | 7.1685% | -2.2011 | -1.1734 | -14.7142% | 7.5420% | 13.9442% | 5 | 5.0000 | 23.6667 |
| baseline_a_spy_buy_and_hold | -18.6427% | -18.8401% | 24.2337% | -0.7366 | -1.1871 | -24.4972% | 15.0591% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -9.2085% | -9.3117% | 12.0304% | -0.7491 | -1.2054 | -12.6732% | 7.6036% | 50.0000% | 12 | 12.0000 | n/a |

### development_2000_2012

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_003 | 16.2503% | 1.1656% | 8.4077% | 0.1802 | 0.1505 | -29.1593% | 16.3518% | 38.0239% | 68 | 68.0000 | 52.8824 |
| baseline_a_spy_buy_and_hold | 24.0922% | 1.6751% | 21.5648% | 0.1849 | 0.2461 | -55.2011% | 22.1214% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 16.1481% | 1.1587% | 10.5392% | 0.1622 | 0.2158 | -32.3036% | 11.4172% | 50.0000% | 156 | 156.0000 | n/a |

### validation_2013_latest

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_003 | 74.3829% | 4.2331% | 8.6526% | 0.5236 | 0.4888 | -19.7433% | 8.3616% | 57.6763% | 91 | 91.0000 | 61.6889 |
| baseline_a_spy_buy_and_hold | 553.7297% | 15.0252% | 16.8223% | 0.9184 | 1.1197 | -33.6999% | 6.3781% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 165.2506% | 7.5440% | 8.2881% | 0.9210 | 1.1224 | -17.7425% | 3.1696% | 50.0000% | 162 | 162.0000 | n/a |

## Stress Tests

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20_bps_round_trip_cost | 87.2172% | 2.4027% | 8.5339% | 0.3216 | 0.2850 | -29.7242% | 13.7354% | 48.0054% | 159 | 159.0000 | 57.8987 |
| 50_bps_round_trip_cost | 47.4297% | 1.4806% | 8.5474% | 0.2152 | 0.1916 | -31.3936% | 16.5288% | 48.0054% | 159 | 159.0000 | 57.8987 |
| one_day_execution_delay | 80.7604% | 2.2667% | 8.7570% | 0.3005 | 0.2598 | -30.5204% | 14.4115% | 47.9904% | 159 | 159.0000 | 58.0633 |
| 40_day_breakout_perturbation | 145.3654% | 3.4568% | 9.1186% | 0.4192 | 0.3947 | -28.9615% | 10.9014% | 52.5817% | 169 | 169.0000 | 59.6667 |
| 70_day_breakout_perturbation | 112.5696% | 2.8963% | 7.9471% | 0.3999 | 0.3417 | -19.9490% | 8.1335% | 45.0399% | 145 | 145.0000 | 59.5556 |
| 10_day_low_exit_perturbation | 72.5037% | 2.0858% | 7.0286% | 0.3296 | 0.2606 | -22.0810% | 10.5849% | 38.3261% | 247 | 247.0000 | 29.6016 |
| 30_day_low_exit_perturbation | 82.4154% | 2.3020% | 9.4300% | 0.2892 | 0.2674 | -34.7187% | 14.5722% | 54.5988% | 121 | 121.0000 | 87.0000 |

## Known Limitations

- Results are hypothetical research evidence, not achieved portfolio performance.
- Results depend on the recorded data source and adjustment policy in the run manifest.
- If run_quality is PRELIMINARY_OPEN_ADJUSTMENT_LIMITATION, next-open execution uses an unadjusted or otherwise limited open-price proxy while adjusted close drives signals and close-to-close returns.
- Cash return is simplified as 0%.
- The random timing baseline preserves time in market by randomized daily exposure masks; it does not preserve the same trade-count or holding-period distribution.
- Breakout and exit-window perturbation outputs are reviews only and do not modify IMP_003.
- BACKTEST_003 alone cannot support CLAIM_001.

## Summary

{
  "artifact_count": 7,
  "backtest_id": "BACKTEST_003",
  "certification": {
    "capital_allocation_recommendation": false,
    "claim_001_certified": false,
    "imp_003_certified": false
  },
  "claim_id": "CLAIM_001",
  "data_source_type": "tiingo_cache",
  "data_vendor": "Tiingo cached local CSV",
  "date_range": {
    "end_date": "2026-06-02",
    "start_date": "2000-01-03"
  },
  "implementation_id": "IMP_003",
  "run_classification": "PRIMARY_ADJUSTED_OHLC",
  "run_quality": "PRIMARY_ADJUSTED_OHLC",
  "run_timestamp": "2026-06-03T17:20:05Z",
  "status": "EVIDENCE_GENERATED_NOT_CERTIFIED",
  "ticker": "SPY"
}
