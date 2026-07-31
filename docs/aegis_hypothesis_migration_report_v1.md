# Aegis Hypothesis Migration Report v1

Day UTC: `2026-07-01`

Mappings are deterministic legacy inferences unless producer artifacts explicitly declare thesis/hypothesis IDs.

## Migrated Sleeves

- `C2_CROSS_ASSET_TREND_V1` -> `HYP_CROSS_ASSET_TREND_PERSISTENCE_V1` / `THESIS_TREND_PERSISTENCE_V1`; confidence: `LEGACY_INFERRED`
- `C2_DEFENSIVE_TAIL_V1` -> `HYP_DEFENSIVE_TAIL_CONVEXITY_V1` / `THESIS_DEFENSIVE_CONVEXITY_V1`; confidence: `LEGACY_INFERRED`
- `C2_EVENT_DISLOCATION_V1` -> `HYP_EVENT_DISLOCATION_REPRICING_V1` / `THESIS_EVENT_DISLOCATION_V1`; confidence: `LEGACY_INFERRED`
- `C2_INTENT_SIMULATOR_V1` -> `HYP_INTENT_SIMULATOR_CONTROL_V1` / `THESIS_SIMULATION_CONTROL_V1`; confidence: `LEGACY_INFERRED`
- `C2_MARKET_NEUTRAL_SPREAD_V1` -> `HYP_MARKET_NEUTRAL_SPREAD_CONVERGENCE_V1` / `THESIS_RELATIVE_VALUE_SPREAD_V1`; confidence: `LEGACY_INFERRED`
- `C2_MEAN_REVERSION_EQ_V1` -> `HYP_EQUITY_SHORT_HORIZON_MEAN_REVERSION_V1` / `THESIS_EQUITY_MEAN_REVERSION_V1`; confidence: `LEGACY_INFERRED`
- `C2_OIL_SHOCK_REVERSAL_V1` -> `ehp_cdbd8fe683acb622` / `THESIS_EVENT_DISLOCATION_V1`; confidence: `LEGACY_INFERRED`
- `C2_TREND_EQ_PRIMARY_V1` -> `HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1` / `THESIS_TREND_PERSISTENCE_V1`; confidence: `LEGACY_INFERRED`
- `C2_VOL_INCOME_DEFINED_RISK_V1` -> `HYP_DEFINED_RISK_VOL_PREMIUM_V1` / `THESIS_VOLATILITY_RISK_PREMIUM_V1`; confidence: `LEGACY_INFERRED`

## Inferred Theses

- `THESIS_DEFENSIVE_CONVEXITY_V1`: Defensive Convexity and Tail Protection
- `THESIS_EQUITY_MEAN_REVERSION_V1`: Equity Mean Reversion
- `THESIS_EVENT_DISLOCATION_V1`: Event Dislocation Mean Repricing
- `THESIS_RELATIVE_VALUE_SPREAD_V1`: Relative Value Spread Convergence
- `THESIS_SIMULATION_CONTROL_V1`: Simulation Control and Workflow Validation
- `THESIS_TREND_PERSISTENCE_V1`: Trend Persistence Across Liquid Markets
- `THESIS_VOLATILITY_RISK_PREMIUM_V1`: Defined-Risk Volatility Risk Premium

## Inferred Hypotheses

- `HYP_CROSS_ASSET_TREND_PERSISTENCE_V1`: Cross-Asset Trend Persistence; sleeves: C2_CROSS_ASSET_TREND_V1; confidence: `LEGACY_INFERRED`
- `HYP_DEFENSIVE_TAIL_CONVEXITY_V1`: Defensive Tail Convexity; sleeves: C2_DEFENSIVE_TAIL_V1; confidence: `LEGACY_INFERRED`
- `HYP_DEFINED_RISK_VOL_PREMIUM_V1`: Defined-Risk Volatility Premium; sleeves: C2_VOL_INCOME_DEFINED_RISK_V1; confidence: `LEGACY_INFERRED`
- `HYP_EQUITY_SHORT_HORIZON_MEAN_REVERSION_V1`: Equity Short-Horizon Mean Reversion; sleeves: C2_MEAN_REVERSION_EQ_V1; confidence: `LEGACY_INFERRED`
- `HYP_EVENT_DISLOCATION_REPRICING_V1`: Event Dislocation Repricing; sleeves: C2_EVENT_DISLOCATION_V1; confidence: `LEGACY_INFERRED`
- `HYP_INTENT_SIMULATOR_CONTROL_V1`: Intent Simulator Control; sleeves: C2_INTENT_SIMULATOR_V1; confidence: `LEGACY_INFERRED`
- `HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1`: Large-Cap Equity Momentum 20-60D; sleeves: C2_TREND_EQ_PRIMARY_V1; confidence: `LEGACY_INFERRED`
- `HYP_MARKET_NEUTRAL_SPREAD_CONVERGENCE_V1`: Market-Neutral Spread Convergence; sleeves: C2_MARKET_NEUTRAL_SPREAD_V1; confidence: `LEGACY_INFERRED`
- `ehp_cdbd8fe683acb622`: Oil shock reversals across energy ETFs; sleeves: C2_OIL_SHOCK_REVERSAL_V1; confidence: `LEGACY_INFERRED`

## Mappings Requiring Confirmation

All `LEGACY_INFERRED` mappings should be source-declared by future sleeve/candidate/hypothesis producer artifacts before they are treated as fully authored research truth.
