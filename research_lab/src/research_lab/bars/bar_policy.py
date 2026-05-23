from __future__ import annotations


DAILY_OHLCV_BAR_POLICY_V1 = {
    "bar_policy_version": "bp_daily_ohlcv_v1",
    "timezone": "America/New_York",
    "interval": "1d",
    "price_adjustment": {
        "open_high_low_close": "raw provider OHLC",
        "adj_close": "provider adjusted close",
    },
    "volume": "provider volume",
    "calendar": "exchange trading days inferred from available bars",
    "missing_data_policy": "warn_not_fill",
    "split_dividend_policy": "adj_close retained, OHLC not back-adjusted",
}

DAILY_OHLCV_STOOQ_BAR_POLICY_V1 = {
    "bar_policy_version": "bp_daily_ohlcv_stooq_v1",
    "timezone": "America/New_York",
    "interval": "1d",
    "price_adjustment": {
        "open_high_low_close": "raw provider OHLC",
        "adj_close": "equal to close because Stooq daily bootstrap data is treated as unadjusted",
    },
    "volume": "provider volume",
    "calendar": "exchange trading days inferred from available bars",
    "missing_data_policy": "warn_not_fill",
    "split_dividend_policy": "not adjusted unless provider supplies adjusted data",
}

DAILY_OHLCV_LOCAL_CSV_BAR_POLICY_V1 = {
    "bar_policy_version": "bp_daily_ohlcv_local_csv_v1",
    "timezone": "America/New_York",
    "interval": "1d",
    "provider": "local_csv",
    "price_adjustment": {
        "open_high_low_close": "from CSV",
        "adj_close": "from CSV if present, else equal to close",
    },
    "volume": "from CSV",
    "calendar": "inferred from available bars",
    "missing_data_policy": "warn_not_fill",
    "split_dividend_policy": "depends on user-supplied CSV; adj_close fallback is explicitly recorded",
}

DAILY_OHLCV_ALPHA_VANTAGE_BAR_POLICY_V1 = {
    "bar_policy_version": "bp_daily_ohlcv_alpha_vantage_v1",
    "timezone": "America/New_York",
    "interval": "1d",
    "provider": "alpha_vantage",
    "price_adjustment": {
        "open_high_low_close": "raw provider OHLC",
        "adj_close": "provider adjusted_close",
    },
    "volume": "provider volume",
    "calendar": "inferred from available bars",
    "missing_data_policy": "warn_not_fill",
    "split_dividend_policy": "provider dividend_amount and split_coefficient retained in raw data when available",
}

DAILY_OHLCV_TIINGO_BAR_POLICY_V1 = {
    "bar_policy_version": "bp_daily_ohlcv_tiingo_v1",
    "timezone": "America/New_York",
    "interval": "1d",
    "provider": "tiingo",
    "price_adjustment": {
        "open_high_low_close": "raw provider OHLC",
        "adj_close": "provider adjClose",
    },
    "volume": "provider raw volume",
    "calendar": "inferred from available bars",
    "missing_data_policy": "warn_not_fill",
    "split_dividend_policy": "provider divCash and splitFactor retained in raw data when available",
}


def require_bar_policy(version: str) -> dict:
    if version == DAILY_OHLCV_BAR_POLICY_V1["bar_policy_version"]:
        return dict(DAILY_OHLCV_BAR_POLICY_V1)
    if version == DAILY_OHLCV_STOOQ_BAR_POLICY_V1["bar_policy_version"]:
        return dict(DAILY_OHLCV_STOOQ_BAR_POLICY_V1)
    if version == DAILY_OHLCV_LOCAL_CSV_BAR_POLICY_V1["bar_policy_version"]:
        return dict(DAILY_OHLCV_LOCAL_CSV_BAR_POLICY_V1)
    if version == DAILY_OHLCV_ALPHA_VANTAGE_BAR_POLICY_V1["bar_policy_version"]:
        return dict(DAILY_OHLCV_ALPHA_VANTAGE_BAR_POLICY_V1)
    if version == DAILY_OHLCV_TIINGO_BAR_POLICY_V1["bar_policy_version"]:
        return dict(DAILY_OHLCV_TIINGO_BAR_POLICY_V1)
    raise ValueError(f"Unsupported bar policy: {version}")
