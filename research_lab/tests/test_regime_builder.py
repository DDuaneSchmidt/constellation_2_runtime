from __future__ import annotations

from datetime import date, timedelta

from research_lab.regimes.regime_builder import build_regime_labels


def _rows(count: int, *, start_close: float = 100.0, step: float = 1.0) -> list[dict]:
    return [
        {
            "date": (date(2024, 1, 1) + timedelta(days=idx)).isoformat(),
            "symbol": "SPY",
            "open": start_close + idx * step,
            "high": start_close + idx * step + 1,
            "low": start_close + idx * step - 1,
            "close": start_close + idx * step,
            "adj_close": start_close + idx * step,
            "volume": 1000,
        }
        for idx in range(count)
    ]


def test_regime_builder_creates_deterministic_labels() -> None:
    labels_a = build_regime_labels(_rows(260), benchmark_symbol="SPY")
    labels_b = build_regime_labels(_rows(260), benchmark_symbol="SPY")

    assert labels_a == labels_b
    assert labels_a[-1]["benchmark_symbol"] == "SPY"
    assert "risk_regime" in labels_a[-1]


def test_spy_200dma_trend_logic_works_on_fixture_data() -> None:
    labels = build_regime_labels(_rows(260, start_close=100, step=1), benchmark_symbol="SPY")

    assert labels[-1]["spy_above_200dma"] is True
    assert labels[-1]["trend_regime"] == "bull_trend"


def test_vol_regime_handles_insufficient_lookback_as_unknown() -> None:
    labels = build_regime_labels(_rows(25), benchmark_symbol="SPY")

    assert labels[-1]["vol_regime"] == "unknown"
