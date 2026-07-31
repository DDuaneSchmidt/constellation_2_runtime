# BACKTEST_004 Evidence Report

This report is evidence only. It does not certify IMP_004 or CLAIM_001 and makes no capital allocation recommendation.

## References

- Implementation reference: IMP_004 - SPY Dual Momentum
- Claim reference: CLAIM_001 - Trend Persistence
- Test plan reference: docs/aegis/technical_strategy_factory/28_test_plan_004_imp_004.md
- Execution spec reference: docs/aegis/technical_strategy_factory/29_backtest_004_execution_spec.md
- Data/baseline spec reference: docs/aegis/technical_strategy_factory/30_backtest_004_data_and_baseline_spec.md

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
- Execution policy: monthly signal after signal period close, execute next market open after signal period; same-day execution prohibited
- Cash return policy: 0%

## Full-Period Metrics

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_004 | 752.1907% | 8.4505% | 13.4953% | 0.6702 | 0.7034 | -33.6999% | 7.2244% | 74.1984% | 17 | 17.0000 | 757.3750 |
| baseline_a_spy_buy_and_hold | 732.0195% | 8.3522% | 19.3104% | 0.5129 | 0.6521 | -55.2011% | 16.1701% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 212.0317% | 4.4026% | 9.4673% | 0.5034 | 0.6396 | -32.3036% | 8.3216% | 50.0000% | 318 | 3.2458 | n/a |

## Random Timing Baseline

| Metric | Value |
|---|---|
| median_random_cagr | 1.1435% |
| p05_random_cagr | -1.5600% |
| p95_random_cagr | 3.8794% |
| median_random_max_drawdown | -62.4410% |
| p05_random_max_drawdown | -75.5690% |
| p95_random_max_drawdown | -44.5734% |
| median_random_sharpe | 0.1518 |
| p05_random_sharpe | -0.0114 |
| p95_random_sharpe | 0.3121 |
| imp_004_percentile_rank_cagr | 100.0000% |
| imp_004_percentile_rank_sharpe | 100.0000% |
| imp_004_percentile_rank_max_drawdown | 0.0000% |

## Subperiod Metrics

### 2000-2003

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_004 | 8.9238% | 2.1645% | 5.0674% | 0.4492 | 0.2784 | -6.7295% | 3.2960% | 14.9402% | 3 | 3.0000 | 30.0000 |
| baseline_a_spy_buy_and_hold | -19.1438% | -5.1842% | 22.5270% | -0.1245 | -0.2005 | -47.5026% | 26.8318% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -8.6622% | -2.2442% | 11.1544% | -0.1483 | -0.2392 | -26.7913% | 14.1929% | 50.0000% | 48 | 48.0000 | n/a |

### 2008

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_004 | -4.8706% | -4.8869% | 6.4268% | -0.7446 | -0.3731 | -9.8047% | 4.9456% | 8.3004% | 1 | 1.0000 | 1676.0000 |
| baseline_a_spy_buy_and_hold | -36.2488% | -36.3472% | 41.2838% | -0.8842 | -1.2268 | -47.1286% | 19.8571% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -19.5288% | -19.5888% | 19.3217% | -1.0275 | -1.4045 | -26.1944% | 10.6051% | 50.0000% | 12 | 12.0000 | n/a |

### 2020

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_004 | 1.8301% | 1.8364% | 31.4257% | 0.2168 | 0.2257 | -33.6999% | 16.0186% | 91.6996% | 2 | 2.0000 | 397.0000 |
| baseline_a_spy_buy_and_hold | 17.2764% | 17.3406% | 33.3837% | 0.6461 | 0.7314 | -33.6999% | 10.1508% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 9.1515% | 9.1843% | 16.1627% | 0.6231 | 0.7026 | -17.7425% | 5.1622% | 50.0000% | 12 | 12.0000 | n/a |

### 2022

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_004 | -13.5187% | -13.6665% | 12.7834% | -1.0808 | -0.9939 | -13.5187% | 11.9388% | 32.6693% | 1 | 1.0000 | 731.0000 |
| baseline_a_spy_buy_and_hold | -18.6427% | -18.8401% | 24.2337% | -0.7366 | -1.1871 | -24.4972% | 15.0591% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -9.2085% | -9.3117% | 12.0304% | -0.7491 | -1.2054 | -12.6732% | 7.6036% | 50.0000% | 12 | 12.0000 | n/a |

### development_2000_2012

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_004 | 100.0594% | 5.4817% | 11.5516% | 0.5208 | 0.5268 | -18.6094% | 5.9328% | 59.7125% | 7 | 7.0000 | 882.6667 |
| baseline_a_spy_buy_and_hold | 24.0922% | 1.6751% | 21.5648% | 0.1849 | 0.2461 | -55.2011% | 22.1214% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 16.1481% | 1.1587% | 10.5392% | 0.1622 | 0.2158 | -32.3036% | 11.4172% | 50.0000% | 156 | 156.0000 | n/a |

### validation_2013_latest

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_004 | 315.3239% | 11.1999% | 15.1262% | 0.7794 | 0.8595 | -33.6999% | 8.2857% | 88.2336% | 10 | 10.0000 | 682.2000 |
| baseline_a_spy_buy_and_hold | 553.7297% | 15.0252% | 16.8223% | 0.9184 | 1.1197 | -33.6999% | 6.3781% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 165.2506% | 7.5440% | 8.2881% | 0.9210 | 1.1224 | -17.7425% | 3.1696% | 50.0000% | 162 | 162.0000 | n/a |

## Stress Tests

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20_bps_round_trip_cost | 744.9724% | 8.4156% | 13.4959% | 0.6677 | 0.7008 | -33.6999% | 7.2598% | 74.1984% | 17 | 17.0000 | 757.3750 |
| 50_bps_round_trip_cost | 723.6612% | 8.3108% | 13.4984% | 0.6605 | 0.6932 | -33.6999% | 7.3672% | 74.1984% | 17 | 17.0000 | 757.3750 |
| one_day_execution_delay | 726.0010% | 8.3224% | 13.5068% | 0.6609 | 0.6929 | -33.6999% | 7.2699% | 74.1834% | 17 | 17.0000 | 757.1250 |
| 9_month_lookback_perturbation | 505.8094% | 7.0583% | 13.2519% | 0.5823 | 0.6029 | -36.0885% | 9.3259% | 73.2651% | 27 | 27.0000 | 454.3846 |
| 15_month_lookback_perturbation | 841.1783% | 8.8591% | 13.9152% | 0.6812 | 0.7180 | -33.6999% | 9.3982% | 74.4844% | 9 | 9.0000 | 1536.5000 |
| weekly_evaluation_perturbation | 539.6204% | 7.2787% | 12.8770% | 0.6116 | 0.6268 | -31.1994% | 8.5205% | 74.0780% | 41 | 41.0000 | 336.2000 |
| quarterly_evaluation_perturbation | 540.6526% | 7.2852% | 13.7675% | 0.5809 | 0.6146 | -33.6999% | 9.3676% | 74.4844% | 13 | 13.0000 | 1018.8333 |

## Known Limitations

- Results are hypothetical research evidence, not achieved portfolio performance.
- Results depend on the recorded data source and adjustment policy in the run manifest.
- If run_quality is PRELIMINARY_OPEN_ADJUSTMENT_LIMITATION, next-open execution uses an unadjusted or otherwise limited open-price proxy while adjusted close drives signals and close-to-close returns.
- Cash return is simplified as 0%.
- The random timing baseline preserves time in market by randomized daily exposure masks; it does not preserve the same trade-count or holding-period distribution.
- Lookback and evaluation-frequency perturbation outputs are reviews only and do not modify IMP_004.
- BACKTEST_004 alone cannot support CLAIM_001.

## Summary

{
  "artifact_count": 7,
  "backtest_id": "BACKTEST_004",
  "certification": {
    "capital_allocation_recommendation": false,
    "claim_001_certified": false,
    "imp_004_certified": false
  },
  "claim_id": "CLAIM_001",
  "data_source_type": "tiingo_cache",
  "data_vendor": "Tiingo cached local CSV",
  "date_range": {
    "end_date": "2026-06-02",
    "start_date": "2000-01-03"
  },
  "implementation_id": "IMP_004",
  "run_classification": "PRIMARY_ADJUSTED_OHLC",
  "run_quality": "PRIMARY_ADJUSTED_OHLC",
  "run_timestamp": "2026-06-03T17:32:47Z",
  "status": "EVIDENCE_GENERATED_NOT_CERTIFIED",
  "ticker": "SPY"
}
