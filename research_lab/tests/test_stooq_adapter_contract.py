from __future__ import annotations

import pytest

from research_lab.providers.base import ProviderFetchError
from research_lab.providers.stooq_daily import STOOQ_ADJUSTMENT_POLICY, map_stooq_symbol, normalize_stooq_csv


def test_stooq_adapter_normalizes_fixture_csv_into_raw_schema() -> None:
    rows = normalize_stooq_csv(
        symbol="SPY",
        csv_text="Date,Open,High,Low,Close,Volume\n2024-01-02,100,102,99,101,12345\n",
        fetched_at="2026-05-18T00:00:00Z",
    )

    assert rows == [
        {
            "date": "2024-01-02",
            "symbol": "SPY",
            "open": "100",
            "high": "102",
            "low": "99",
            "close": "101",
            "adj_close": "101",
            "volume": "12345",
            "provider": "stooq",
            "provider_version": "stooq_csv_daily_v1",
            "fetched_at": "2026-05-18T00:00:00Z",
            "adjustment_policy": STOOQ_ADJUSTMENT_POLICY,
        }
    ]


def test_stooq_symbol_mapping_is_deterministic() -> None:
    mapping = {"SPY": "spy.us", "QQQ": "qqq.us"}

    assert map_stooq_symbol("spy", mapping=mapping) == "spy.us"
    assert map_stooq_symbol("QQQ", mapping=mapping) == "qqq.us"


def test_unknown_stooq_symbol_mapping_fails_explicitly() -> None:
    with pytest.raises(ProviderFetchError) as exc:
        map_stooq_symbol("NOPE", mapping={"SPY": "spy.us"})

    assert "No Stooq symbol mapping" in str(exc.value)
    assert exc.value.diagnostic["known_mappings"] == ["SPY"]
