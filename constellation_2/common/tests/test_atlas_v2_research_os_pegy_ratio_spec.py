from __future__ import annotations

from pathlib import Path

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.pegy_ratio_spec import (
    MIXED_UNIT_INPUTS,
    MISSING_DIVIDEND_YIELD,
    MISSING_FORWARD_GROWTH,
    NEGATIVE_EARNINGS,
    NEGATIVE_GROWTH,
    NEGATIVE_PE_RATIO,
    ZERO_OR_NEGATIVE_DENOMINATOR,
    build_pegy_ratio_metric_spec,
    calculate_pegy_ratio,
    run_pegy_ratio_spec_report,
)

NOW = "2026-06-05T00:00:00Z"


def test_calculates_pegy_with_percent_units() -> None:
    result = calculate_pegy_ratio(pe_ratio=12, growth_rate=12, dividend_yield=3)
    assert result["valid"] is True
    assert result["unit_class"] == "PERCENT_UNITS"
    assert result["pegy_ratio"] == 0.8


def test_calculates_pegy_with_decimal_units() -> None:
    result = calculate_pegy_ratio(pe_ratio=12, growth_rate=0.12, dividend_yield=0.03)
    assert result["valid"] is True
    assert result["unit_class"] == "DECIMAL_UNITS"
    assert result["pegy_ratio"] == 80.0


def test_rejects_mixed_unit_inputs() -> None:
    result = calculate_pegy_ratio(pe_ratio=12, growth_rate=12, dividend_yield=0.03)
    assert result["valid"] is False
    assert result["classification"] == MIXED_UNIT_INPUTS


def test_invalid_cases_are_classified_separately() -> None:
    cases = [
        ({"pe_ratio": 12, "growth_rate": None, "dividend_yield": 3}, MISSING_FORWARD_GROWTH),
        ({"pe_ratio": 12, "growth_rate": 12, "dividend_yield": None}, MISSING_DIVIDEND_YIELD),
        ({"pe_ratio": 12, "growth_rate": 12, "dividend_yield": 3, "eps": -1}, NEGATIVE_EARNINGS),
        ({"pe_ratio": -12, "growth_rate": 12, "dividend_yield": 3}, NEGATIVE_PE_RATIO),
        ({"pe_ratio": 12, "growth_rate": -12, "dividend_yield": 3}, NEGATIVE_GROWTH),
        ({"pe_ratio": 12, "growth_rate": 0, "dividend_yield": 0}, ZERO_OR_NEGATIVE_DENOMINATOR),
    ]
    for kwargs, expected in cases:
        result = calculate_pegy_ratio(**kwargs)
        assert result["valid"] is False
        assert result["classification"] == expected


def test_metric_spec_report_contains_data_gate_and_guardrails(tmp_path: Path) -> None:
    report = build_pegy_ratio_metric_spec(root=tmp_path, created_at=NOW)
    assert report["metric_spec"]["metric_name"] == "PEGY ratio"
    assert report["data_gate"] == "SPEC_ONLY_UNTIL_SUFFICIENT_POINT_IN_TIME_FUNDAMENTAL_DATA_EXISTS"
    assert report["authority_boundary"]["broker_execution_authorized"] is False

    run_report = run_pegy_ratio_spec_report(root=tmp_path, created_at=NOW)
    assert run_report["report_type"] == "PEGY_RATIO_METRIC_SPEC"
    assert (tmp_path / "manual_fundamental_claims" / "pegy_ratio" / "latest.json").exists()
    assert (tmp_path / "manual_fundamental_claims" / "pegy_ratio" / "2026-06-05" / "pegy_ratio_metric_spec.json").exists()


def test_pegy_ratio_spec_cli_writes_report(tmp_path: Path) -> None:
    rc = main(["--root", str(tmp_path), "--pegy-ratio-spec-report"])
    assert rc == 0
    assert (tmp_path / "manual_fundamental_claims" / "pegy_ratio" / "latest_summary.md").exists()
