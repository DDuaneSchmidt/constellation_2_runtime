from __future__ import annotations

from pathlib import Path

import pytest

from src.holdings_signature import build_report, calculate_holdings_signature


def test_calculates_selected_median_and_universe_spread() -> None:
    rows = [
        {
            "symbol": "CHEAP_QUALITY",
            "selected": "true",
            "pe": "8",
            "roe": "22",
            "revenue_growth": "18",
            "volatility": "0.12",
            "momentum": "0.15",
            "market_cap": "100",
            "forensic_risk": "0.1",
        },
        {
            "symbol": "EXPENSIVE_WEAK",
            "selected": "false",
            "pe": "30",
            "roe": "5",
            "revenue_growth": "2",
            "volatility": "0.40",
            "momentum": "-0.05",
            "market_cap": "10",
            "forensic_risk": "0.5",
        },
        {
            "symbol": "MID",
            "selected": "false",
            "pe": "15",
            "roe": "12",
            "revenue_growth": "8",
            "volatility": "0.20",
            "momentum": "0.02",
            "market_cap": "50",
            "forensic_risk": "0.2",
        },
    ]

    summary = calculate_holdings_signature(rows)

    assert summary["selected_count"] == 1
    assert summary["median_holding"]["valuation"] == 100.0
    assert summary["median_holding"]["quality"] == 100.0
    assert summary["median_holding"]["stability"] == 100.0
    assert summary["selected_minus_universe"]["valuation"] == 50.0
    assert summary["selected_minus_universe"]["forensic"] == 50.0


def test_uses_direct_percentile_columns_when_present() -> None:
    rows = [
        {"ticker": "A", "weight": "0.4", "valuation_percentile": "0.9", "quality_percentile": "85"},
        {"ticker": "B", "weight": "0", "valuation_percentile": "0.1", "quality_percentile": "15"},
    ]

    summary = calculate_holdings_signature(rows)

    assert summary["median_holding"]["valuation"] == 90.0
    assert summary["median_holding"]["quality"] == 85.0
    assert summary["holdings"][0]["symbol"] == "A"


def test_build_report_writes_required_sections(tmp_path: Path) -> None:
    input_path = tmp_path / "crazy_returns_europe_holdings.csv"
    output_path = tmp_path / "holdings_signature.md"
    input_path.write_text(
        "\n".join(
            [
                "symbol,holding,valuation_percentile,quality_percentile,growth_percentile,stability_percentile,sentiment_percentile,size_percentile,forensic_percentile",
                "AAA,yes,80,90,75,65,70,55,85",
                "BBB,no,40,45,35,50,30,60,40",
            ]
        ),
        encoding="utf-8",
    )

    build_report(input_path, output_path)
    report = output_path.read_text(encoding="utf-8")

    assert "## Median Holding" in report
    assert "## Selected Minus Universe" in report
    assert "| Valuation | 80.00 | 60.00 | 20.00 |" in report
    assert "Recurring Characteristics" in report


def test_missing_input_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_report(tmp_path / "missing.csv", tmp_path / "out.md")
