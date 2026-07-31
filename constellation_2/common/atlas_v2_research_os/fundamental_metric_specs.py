from __future__ import annotations

from typing import Any

FUNDAMENTAL_DATA_REQUIREMENTS = [
    "ticker",
    "date",
    "price",
    "EPS or forward EPS",
    "P/E ratio",
    "projected EPS growth rate",
    "dividend yield",
    "sector",
    "industry",
    "market cap",
    "total return",
    "benchmark return",
]

OPTIONAL_FUNDAMENTAL_CONTROLS = [
    "debt/equity",
    "ROE",
    "free cash flow yield",
    "payout ratio",
    "revenue growth",
    "earnings revision trend",
]


def build_fundamental_metric_spec(
    *,
    metric_id: str,
    metric_name: str,
    formula: str,
    required_fields: list[str] | None = None,
    optional_controls: list[str] | None = None,
    invalid_case_classifications: list[str] | None = None,
    unit_rules: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "metric_id": metric_id,
        "metric_name": metric_name,
        "metric_type": "FUNDAMENTAL_VALUATION",
        "formula": formula,
        "required_fields": list(required_fields or FUNDAMENTAL_DATA_REQUIREMENTS),
        "optional_controls": list(optional_controls or OPTIONAL_FUNDAMENTAL_CONTROLS),
        "invalid_case_classifications": list(invalid_case_classifications or []),
        "unit_rules": list(unit_rules or []),
        "calculation_authority": "SPEC_ONLY_UNTIL_REQUIRED_DATA_EXISTS",
    }
