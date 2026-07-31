# BACKTEST_001 Evidence Report

This report is evidence only. It does not certify IMP_001 or CLAIM_001 and makes no capital allocation recommendation.

## References

- Implementation reference: IMP_001 - SPY 200DMA Trend Following
- Claim reference: CLAIM_001 - Trend Persistence
- Test plan reference: docs/aegis/technical_strategy_factory/05_test_plan_001_imp_001.md
- Execution spec reference: docs/aegis/technical_strategy_factory/06_backtest_001_execution_spec.md
- Data/baseline spec reference: docs/aegis/technical_strategy_factory/07_backtest_001_data_and_baseline_spec.md

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
| IMP_001 | 466.4265% | 6.7862% | 10.9061% | 0.6581 | 0.7104 | -24.4462% | 8.3700% | 71.3533% | 163 | 163.0000 | 84.1605 |
| baseline_a_spy_buy_and_hold | 732.0195% | 8.3522% | 19.3104% | 0.5129 | 0.6521 | -55.2011% | 16.1701% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 212.0317% | 4.4026% | 9.4673% | 0.5034 | 0.6396 | -32.3036% | 8.3216% | 50.0000% | 318 | 3.2458 | n/a |

## Random Timing Baseline

| Metric | Value |
|---|---|
| median_random_cagr | 0.4659% |
| p05_random_cagr | -2.1825% |
| p95_random_cagr | 3.2517% |
| median_random_max_drawdown | -63.5368% |
| p05_random_max_drawdown | -76.7823% |
| p95_random_max_drawdown | -45.1749% |
| median_random_sharpe | 0.1099 |
| p05_random_sharpe | -0.0554 |
| p95_random_sharpe | 0.2773 |
| imp_001_percentile_rank_cagr | 100.0000% |
| imp_001_percentile_rank_sharpe | 100.0000% |
| imp_001_percentile_rank_max_drawdown | 0.0000% |

## Subperiod Metrics

### 2000-2003

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_001 | 12.4776% | 2.9895% | 6.3440% | 0.4974 | 0.3495 | -12.4814% | 3.9399% | 20.9163% | 17 | 17.0000 | 5.0000 |
| baseline_a_spy_buy_and_hold | -19.1438% | -5.1842% | 22.5270% | -0.1245 | -0.2005 | -47.5026% | 26.8318% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -8.6622% | -2.2442% | 11.1544% | -0.1483 | -0.2392 | -26.7913% | 14.1929% | 50.0000% | 48 | 48.0000 | n/a |

### 2008

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_001 | -2.4985% | -2.5070% | 1.9008% | -1.3215 | -0.2483 | -2.5793% | 2.0256% | 1.5810% | 2 | 2.0000 | 6.0000 |
| baseline_a_spy_buy_and_hold | -36.2488% | -36.3472% | 41.2838% | -0.8842 | -1.2268 | -47.1286% | 19.8571% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -19.5288% | -19.5888% | 19.3217% | -1.0275 | -1.4045 | -26.1944% | 10.6051% | 50.0000% | 12 | 12.0000 | n/a |

### 2020

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_001 | 5.5759% | 5.5956% | 17.2793% | 0.4014 | 0.3912 | -19.5245% | 12.4626% | 76.6798% | 6 | 6.0000 | 90.0000 |
| baseline_a_spy_buy_and_hold | 17.2764% | 17.3406% | 33.3837% | 0.6461 | 0.7314 | -33.6999% | 10.1508% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 9.1515% | 9.1843% | 16.1627% | 0.6231 | 0.7026 | -17.7425% | 5.1622% | 50.0000% | 12 | 12.0000 | n/a |

### 2022

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_001 | -13.9785% | -14.1308% | 8.2670% | -1.7938 | -1.2507 | -13.9785% | 9.9527% | 19.1235% | 15 | 15.0000 | 81.6250 |
| baseline_a_spy_buy_and_hold | -18.6427% | -18.8401% | 24.2337% | -0.7366 | -1.1871 | -24.4972% | 15.0591% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | -9.2085% | -9.3117% | 12.0304% | -0.7491 | -1.2054 | -12.6732% | 7.6036% | 50.0000% | 12 | 12.0000 | n/a |

### development_2000_2012

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_001 | 40.4072% | 2.6462% | 10.0155% | 0.3115 | 0.3125 | -24.4462% | 9.4893% | 57.3264% | 99 | 99.0000 | 54.5510 |
| baseline_a_spy_buy_and_hold | 24.0922% | 1.6751% | 21.5648% | 0.1849 | 0.2461 | -55.2011% | 22.1214% | 100.0000% | 1 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 16.1481% | 1.1587% | 10.5392% | 0.1622 | 0.2158 | -32.3036% | 11.4172% | 50.0000% | 156 | 156.0000 | n/a |

### validation_2013_latest

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| IMP_001 | 293.3356% | 10.7499% | 11.6804% | 0.9348 | 1.0743 | -19.5245% | 7.0938% | 84.9437% | 64 | 64.0000 | 129.5000 |
| baseline_a_spy_buy_and_hold | 553.7297% | 15.0252% | 16.8223% | 0.9184 | 1.1197 | -33.6999% | 6.3781% | 100.0000% | 0 | 1.0000 | n/a |
| baseline_b_50_spy_50_cash_monthly | 165.2506% | 7.5440% | 8.2881% | 0.9210 | 1.1224 | -17.7425% | 3.1696% | 50.0000% | 162 | 162.0000 | n/a |

## Stress Tests

| Series | total_return | cagr | annual_volatility | sharpe | sortino | maximum_drawdown | ulcer_index | time_in_market | trade_count | turnover | average_holding_period_days |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20_bps_round_trip_cost | 422.0619% | 6.4570% | 10.9113% | 0.6294 | 0.6795 | -24.8894% | 8.7519% | 71.3533% | 163 | 163.0000 | 84.1605 |
| 50_bps_round_trip_cost | 308.6493% | 5.4743% | 10.9353% | 0.5433 | 0.5868 | -26.2316% | 9.9778% | 71.3533% | 163 | 163.0000 | 84.1605 |
| one_day_execution_delay | 504.9876% | 7.0528% | 11.0435% | 0.6739 | 0.7189 | -24.3180% | 8.4272% | 71.3684% | 141 | 141.0000 | 97.7000 |
| 180dma_perturbation_review | 354.6165% | 5.9008% | 10.9491% | 0.5797 | 0.6257 | -29.7276% | 9.2391% | 71.1576% | 199 | 199.0000 | 68.6566 |
| 220dma_perturbation_review | 462.9717% | 6.7615% | 11.0342% | 0.6497 | 0.6946 | -25.0650% | 8.3677% | 71.5641% | 159 | 159.0000 | 86.4810 |

## Known Limitations

- Results are hypothetical research evidence, not achieved portfolio performance.
- Results depend on the recorded data source and adjustment policy in the run manifest.
- If run_quality is PRELIMINARY_OPEN_ADJUSTMENT_LIMITATION, next-open execution uses an unadjusted or otherwise limited open-price proxy while adjusted close drives signals and close-to-close returns.
- Cash return is simplified as 0%.
- The random timing baseline preserves time in market by randomized daily exposure masks; it does not preserve the same trade-count or holding-period distribution.
- 180DMA and 220DMA outputs are perturbation reviews only and do not modify IMP_001.
- BACKTEST_001 alone cannot support CLAIM_001.

## Summary

{
  "artifact_count": 6,
  "backtest_id": "BACKTEST_001",
  "certification": {
    "capital_allocation_recommendation": false,
    "claim_001_certified": false,
    "imp_001_certified": false
  },
  "claim_id": "CLAIM_001",
  "data_source_type": "tiingo_cache",
  "data_vendor": "Tiingo cached local CSV",
  "date_range": {
    "end_date": "2026-06-02",
    "start_date": "2000-01-03"
  },
  "implementation_id": "IMP_001",
  "run_classification": "PRIMARY_ADJUSTED_OHLC",
  "run_quality": "PRIMARY_ADJUSTED_OHLC",
  "run_timestamp": "2026-06-03T16:27:03Z",
  "status": "EVIDENCE_GENERATED_NOT_CERTIFIED",
  "ticker": "SPY"
}
