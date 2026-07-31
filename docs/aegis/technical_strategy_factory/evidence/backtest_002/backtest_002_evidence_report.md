# BACKTEST_002 Evidence Report

This report is evidence only. It does not certify IMP_002 or CLAIM_001 and makes no capital allocation recommendation.

## References

- Implementation reference: IMP_002 - SPY 52-Week Breakout
- Claim reference: CLAIM_001 - Trend Persistence
- Test plan reference: docs/aegis/technical_strategy_factory/12_test_plan_002_imp_002.md
- Execution spec reference: docs/aegis/technical_strategy_factory/13_backtest_002_execution_spec.md
- Data/baseline spec reference: docs/aegis/technical_strategy_factory/14_backtest_002_data_and_baseline_spec.md

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
| IMP_002 | 170.6739% | 3.8420% | 7.7927% | 0.5239 | 0.4445 | -17.8875% | 6.6935% | 45.9130% | 69 | 69.0000 | 128.9706 |
| baseline_a_spy_buy_and_hold | 732.0195% | 8.3522% | 19.3104% | 0.5129 | 0.6521 | -55.2011% | 16.1701% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 212.0317% | 4.4026% | 9.4673% | 0.5034 | 0.6396 | -32.3036% | 8.3216% | 50.0000% | 318 | 3.2458 | n/a |

## Random Timing Baseline

| Metric | Value |
|---|---|
| median_random_cagr | -2.5225% |
| p05_random_cagr | -5.4395% |
| p95_random_cagr | 0.6122% |
| median_random_max_drawdown | -67.2200% |
| p05_random_max_drawdown | -82.8246% |
| p95_random_max_drawdown | -43.5257% |
| median_random_sharpe | -0.1309 |
| p05_random_sharpe | -0.3605 |
| p95_random_sharpe | 0.1122 |
| imp_002_percentile_rank_cagr | 100.0000% |
| imp_002_percentile_rank_sharpe | 100.0000% |
| imp_002_percentile_rank_max_drawdown | 0.0000% |

## Subperiod Metrics

### 2000-2003

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_002 | 10.3721% | 2.5031% | 3.3243% | 0.7625 | 0.3899 | -4.0768% | 0.3591% | 8.4661% | 1 | 1.0000 | n/a |
| baseline_a_spy_buy_and_hold | -19.1438% | -5.1842% | 22.5270% | -0.1245 | -0.2005 | -47.5026% | 26.8318% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -8.6622% | -2.2442% | 11.1544% | -0.1483 | -0.2392 | -26.7913% | 14.1929% | 50.0000% | 48 | 48.0000 | n/a |

### 2008

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_002 | 0.0000% | 0.0000% | 0.0000% | n/a | n/a | 0.0000% | 0.0000% | 0.0000% | 0 | 0.0000 | n/a |
| baseline_a_spy_buy_and_hold | -36.2488% | -36.3472% | 41.2838% | -0.8842 | -1.2268 | -47.1286% | 19.8571% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -19.5288% | -19.5888% | 19.3217% | -1.0275 | -1.4045 | -26.1944% | 10.6051% | 50.0000% | 12 | 12.0000 | n/a |

### 2020

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_002 | -0.6891% | -0.6915% | 11.9351% | 0.0022 | 0.0018 | -10.7870% | 6.6629% | 50.1976% | 4 | 4.0000 | 100.0000 |
| baseline_a_spy_buy_and_hold | 17.2764% | 17.3406% | 33.3837% | 0.6461 | 0.7314 | -33.6999% | 10.1508% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 9.1515% | 9.1843% | 16.1627% | 0.6231 | 0.7026 | -17.7425% | 5.1622% | 50.0000% | 12 | 12.0000 | n/a |

### 2022

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_002 | -5.0631% | -5.1211% | 3.3200% | -1.5606 | -0.4463 | -5.4343% | 4.9765% | 4.7809% | 1 | 1.0000 | 91.0000 |
| baseline_a_spy_buy_and_hold | -18.6427% | -18.8401% | 24.2337% | -0.7366 | -1.1871 | -24.4972% | 15.0591% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -9.2085% | -9.3117% | 12.0304% | -0.7491 | -1.2054 | -12.6732% | 7.6036% | 50.0000% | 12 | 12.0000 | n/a |

### development_2000_2012

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_002 | 23.9336% | 1.6651% | 6.3536% | 0.2923 | 0.2091 | -12.5955% | 4.9018% | 30.4374% | 24 | 24.0000 | 120.7500 |
| baseline_a_spy_buy_and_hold | 24.0922% | 1.6751% | 21.5648% | 0.1849 | 0.2461 | -55.2011% | 22.1214% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 16.1481% | 1.1587% | 10.5392% | 0.1622 | 0.2158 | -32.3036% | 11.4172% | 50.0000% | 156 | 156.0000 | n/a |

### validation_2013_latest

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_002 | 118.4024% | 5.9970% | 8.9683% | 0.6958 | 0.6693 | -17.8875% | 8.0408% | 60.9069% | 45 | 45.0000 | 133.4545 |
| baseline_a_spy_buy_and_hold | 553.7297% | 15.0252% | 16.8223% | 0.9184 | 1.1197 | -33.6999% | 6.3781% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 165.2506% | 7.5440% | 8.2881% | 0.9210 | 1.1224 | -17.7425% | 3.1696% | 50.0000% | 162 | 162.0000 | n/a |

## Stress Tests

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20_bps_round_trip_cost | 161.4882% | 3.7064% | 7.7911% | 0.5072 | 0.4307 | -18.1336% | 6.8749% | 45.9130% | 69 | 69.0000 | 128.9706 |
| 50_bps_round_trip_cost | 135.7348% | 3.3000% | 7.7915% | 0.4567 | 0.3892 | -18.9024% | 7.5029% | 45.9130% | 69 | 69.0000 | 128.9706 |
| one_day_execution_delay | 167.7798% | 3.7998% | 7.9331% | 0.5109 | 0.4382 | -19.8897% | 6.3649% | 45.8979% | 69 | 69.0000 | 128.8529 |
| 200_day_breakout_perturbation | 212.2675% | 4.4055% | 8.0424% | 0.5776 | 0.5033 | -17.8875% | 6.5663% | 47.6441% | 69 | 69.0000 | 133.8529 |
| 300_day_breakout_perturbation | 152.0992% | 3.5629% | 7.6510% | 0.4969 | 0.4151 | -17.8875% | 6.9572% | 44.9496% | 69 | 69.0000 | 126.2647 |
| 50dma_exit_perturbation | 73.5360% | 2.1089% | 6.5315% | 0.3530 | 0.2667 | -18.4868% | 7.3132% | 36.9562% | 123 | 123.0000 | 57.7869 |
| 150dma_exit_perturbation | 320.3486% | 5.5871% | 8.7013% | 0.6698 | 0.6083 | -18.0222% | 5.8411% | 52.1301% | 45 | 45.0000 | 226.3182 |

## Known Limitations

- Results are hypothetical research evidence, not achieved portfolio performance.
- Results depend on the recorded data source and adjustment policy in the run manifest.
- If run_quality is PRELIMINARY_OPEN_ADJUSTMENT_LIMITATION, next-open execution uses an unadjusted or otherwise limited open-price proxy while adjusted close drives signals and close-to-close returns.
- Cash return is simplified as 0%.
- The random timing baseline preserves time in market by randomized daily exposure masks; it does not preserve the same trade-count or holding-period distribution.
- Breakout and exit-window perturbation outputs are reviews only and do not modify IMP_002.
- BACKTEST_002 alone cannot support CLAIM_001.

## Summary

{
  "artifact_count": 6,
  "backtest_id": "BACKTEST_002",
  "certification": {
    "capital_allocation_recommendation": false,
    "claim_001_certified": false,
    "imp_002_certified": false
  },
  "claim_id": "CLAIM_001",
  "data_source_type": "tiingo_cache",
  "data_vendor": "Tiingo cached local CSV",
  "date_range": {
    "end_date": "2026-06-02",
    "start_date": "2000-01-03"
  },
  "implementation_id": "IMP_002",
  "run_classification": "PRIMARY_ADJUSTED_OHLC",
  "run_quality": "PRIMARY_ADJUSTED_OHLC",
  "run_timestamp": "2026-06-03T16:53:24Z",
  "status": "EVIDENCE_GENERATED_NOT_CERTIFIED",
  "ticker": "SPY"
}
