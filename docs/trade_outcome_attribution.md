# trade_outcome_attribution.v1

`trade_outcome_attribution.v1` separates sleeve quality, edge quality, governance quality, and operator execution quality.

It records recommended entry reference, actual entry/exit when available, forward returns, realized or unrealized PnL, MAE, MFE, operator slippage, skipped-trade outcome, governance adjustment effect, sleeve signal quality, implementation quality, and operator execution quality.

Aegis Lite evaluates recommended trades even when they were skipped. IB execution is not required to measure sleeve signal quality when market data is available.
