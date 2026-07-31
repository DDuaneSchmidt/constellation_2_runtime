from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .fundamental_metric_specs import build_fundamental_metric_spec
from .manual_fundamental_claim_models import AUTHORITY_BOUNDARY, OPTIONAL_CONTROLS, REQUIRED_FIELDS
from .manual_fundamental_claim_reports import write_pegy_ratio_metric_spec_report

VALID = "VALID"
MISSING_PE_RATIO = "MISSING_PE_RATIO"
MISSING_FORWARD_GROWTH = "MISSING_FORWARD_GROWTH"
MISSING_DIVIDEND_YIELD = "MISSING_DIVIDEND_YIELD"
NEGATIVE_EARNINGS = "NEGATIVE_EARNINGS"
NEGATIVE_PE_RATIO = "NEGATIVE_PE_RATIO"
NEGATIVE_GROWTH = "NEGATIVE_GROWTH"
ZERO_OR_NEGATIVE_DENOMINATOR = "ZERO_OR_NEGATIVE_DENOMINATOR"
MIXED_UNIT_INPUTS = "MIXED_UNIT_INPUTS"

INVALID_CLASSIFICATIONS = [
    MISSING_PE_RATIO,
    MISSING_FORWARD_GROWTH,
    MISSING_DIVIDEND_YIELD,
    NEGATIVE_EARNINGS,
    NEGATIVE_PE_RATIO,
    NEGATIVE_GROWTH,
    ZERO_OR_NEGATIVE_DENOMINATOR,
    MIXED_UNIT_INPUTS,
]


def calculate_pegy_ratio(
    *,
    pe_ratio: float | int | None,
    growth_rate: float | int | None,
    dividend_yield: float | int | None,
    eps: float | int | None = None,
    forward_eps: float | int | None = None,
) -> dict[str, Any]:
    if eps is not None and float(eps) < 0:
        return _invalid(NEGATIVE_EARNINGS)
    if forward_eps is not None and float(forward_eps) < 0:
        return _invalid(NEGATIVE_EARNINGS)
    if pe_ratio is None:
        return _invalid(MISSING_PE_RATIO)
    if growth_rate is None:
        return _invalid(MISSING_FORWARD_GROWTH)
    if dividend_yield is None:
        return _invalid(MISSING_DIVIDEND_YIELD)

    pe = float(pe_ratio)
    growth = float(growth_rate)
    yield_value = float(dividend_yield)
    if pe < 0:
        return _invalid(NEGATIVE_PE_RATIO)
    if growth < 0:
        return _invalid(NEGATIVE_GROWTH)
    unit_class = _unit_class(growth, yield_value)
    if unit_class == "MIXED":
        return _invalid(MIXED_UNIT_INPUTS)
    denominator = growth + yield_value
    if denominator <= 0:
        return _invalid(ZERO_OR_NEGATIVE_DENOMINATOR, units=unit_class, denominator=denominator)
    return {
        "valid": True,
        "classification": VALID,
        "pegy_ratio": pe / denominator,
        "pe_ratio": pe,
        "growth_rate": growth,
        "dividend_yield": yield_value,
        "denominator": denominator,
        "unit_class": unit_class,
        "notes": "PEGY uses P/E divided by projected EPS growth rate plus dividend yield; growth and yield must be in the same units.",
    }


def build_pegy_ratio_metric_spec(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or _now()
    spec = build_fundamental_metric_spec(
        metric_id="pegy_ratio",
        metric_name="PEGY ratio",
        formula="PEGY = P/E / (projected EPS growth rate + dividend yield)",
        required_fields=REQUIRED_FIELDS,
        optional_controls=OPTIONAL_CONTROLS,
        invalid_case_classifications=INVALID_CLASSIFICATIONS,
        unit_rules=[
            "growth_rate and dividend_yield must use the same units.",
            "If growth is entered as 12 for 12%, dividend yield should be 3 for 3%.",
            "If growth is entered as 0.12, dividend yield should be 0.03.",
            "Reject mixed-unit inputs.",
        ],
    )
    return {
        "schema_id": "atlas_v2_research_os_pegy_ratio_metric_spec",
        "schema_version": "1.0",
        "report_type": "PEGY_RATIO_METRIC_SPEC",
        "created_at": created,
        "day": created[:10],
        "metric_spec": spec,
        "calculation_rules": {
            "formula": "PEGY = P/E / (growth_rate + dividend_yield)",
            "same_unit_requirement": True,
            "valid_percent_unit_example": {"pe_ratio": 12, "growth_rate": 12, "dividend_yield": 3, "pegy_ratio": 0.8},
            "valid_decimal_unit_example": {"pe_ratio": 12, "growth_rate": 0.12, "dividend_yield": 0.03, "pegy_ratio": 80.0},
            "invalid_cases_are_classified_not_coerced": True,
        },
        "invalid_case_policy": {classification: "CLASSIFY_SEPARATELY_DO_NOT_COERCE" for classification in INVALID_CLASSIFICATIONS},
        "data_gate": "SPEC_ONLY_UNTIL_SUFFICIENT_POINT_IN_TIME_FUNDAMENTAL_DATA_EXISTS",
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Research-only metric specification.",
            "Do not label PEGY as an edge until tested.",
            "Do not use today's fundamentals to test past returns unless point-in-time data exists.",
            "No live trading, broker execution, capital allocation, position sizing, trade recommendations, candidate promotion, or production promotion.",
        ],
    }


def run_pegy_ratio_spec_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_pegy_ratio_metric_spec(root=root, created_at=created_at)
    write_pegy_ratio_metric_spec_report(report, root=root)
    return report


def _unit_class(growth_rate: float, dividend_yield: float) -> str:
    growth_decimal = abs(growth_rate) <= 1.0
    yield_decimal = abs(dividend_yield) <= 1.0
    if growth_decimal == yield_decimal:
        return "DECIMAL_UNITS" if growth_decimal else "PERCENT_UNITS"
    return "MIXED"


def _invalid(classification: str, **extra: Any) -> dict[str, Any]:
    result: dict[str, Any] = {"valid": False, "classification": classification, "pegy_ratio": None}
    result.update(extra)
    return result


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
