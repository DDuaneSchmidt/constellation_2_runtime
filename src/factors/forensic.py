from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import median
from typing import Any, Mapping, Sequence


Record = Mapping[str, Any]

FACTOR_FAMILY = "forensic_quality"

METRIC_NAMES: tuple[str, ...] = (
    "accrual_ratio",
    "ocf_to_net_income",
    "fcf_conversion",
    "asset_growth",
    "receivables_growth",
    "inventory_growth",
    "beneish_proxy_risk",
    "dilution_rate",
)

DEFAULT_WEIGHTS: dict[str, float] = {
    "accrual_ratio": 0.18,
    "ocf_to_net_income": 0.16,
    "fcf_conversion": 0.14,
    "asset_growth": 0.10,
    "receivables_growth": 0.12,
    "inventory_growth": 0.10,
    "beneish_proxy_risk": 0.14,
    "dilution_rate": 0.06,
}

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "ticker": ("ticker", "symbol"),
    "net_income": ("net_income", "ni", "earnings"),
    "operating_cash_flow": ("operating_cash_flow", "ocf", "cash_flow_from_operations", "cfo"),
    "free_cash_flow": ("free_cash_flow", "fcf"),
    "total_assets": ("total_assets", "assets"),
    "receivables": ("receivables", "accounts_receivable", "accounts_receivables", "ar"),
    "inventory": ("inventory", "inventories"),
    "revenue": ("revenue", "sales"),
    "gross_margin": ("gross_margin",),
    "current_assets": ("current_assets",),
    "ppe": ("ppe", "property_plant_equipment", "net_ppe"),
    "shares_outstanding": ("shares_outstanding", "shares", "diluted_shares"),
}


@dataclass(frozen=True)
class ForensicQualityResult:
    ticker: str
    factor_family: str
    forensic_quality_score: float
    component_scores: dict[str, float]
    raw_metrics: dict[str, float]
    warnings: tuple[str, ...]
    interpretation: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _field(record: Record, canonical: str) -> Any:
    for name in FIELD_ALIASES[canonical]:
        if name in record:
            return record[name]
    return None


def _number(record: Record, canonical: str, *, required: bool = False) -> float | None:
    raw = _field(record, canonical)
    if raw is None or raw == "":
        if required:
            raise ValueError(f"Missing required forensic input: {canonical}")
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Forensic input {canonical} must be numeric") from exc
    if value != value:
        raise ValueError(f"Forensic input {canonical} must not be NaN")
    return value


def _ticker(record: Record) -> str:
    raw = _field(record, "ticker")
    return str(raw) if raw not in (None, "") else "UNKNOWN"


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0.0:
        return None
    return numerator / denominator


def _growth(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous == 0.0:
        return None
    return (current - previous) / abs(previous)


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _score_low_abs(value: float | None, caution: float, fail: float) -> float:
    if value is None:
        return 0.5
    magnitude = abs(value)
    if magnitude <= caution:
        return 1.0
    return 1.0 - _clamp((magnitude - caution) / (fail - caution))


def _score_low_positive(value: float | None, caution: float, fail: float) -> float:
    if value is None:
        return 0.5
    if value <= caution:
        return 1.0
    return 1.0 - _clamp((value - caution) / (fail - caution))


def _score_conversion(value: float | None, good: float = 1.0, weak: float = 0.4, cap: float = 1.4) -> float:
    if value is None:
        return 0.5
    if value >= good:
        return 1.0 - _clamp((value - cap) / cap) * 0.15
    return _clamp((value - weak) / (good - weak))


def _score_growth_vs_sales(metric_growth: float | None, revenue_growth: float | None) -> float:
    if metric_growth is None:
        return 0.5
    spread = metric_growth - (revenue_growth or 0.0)
    return _score_low_positive(spread, 0.05, 0.35)


def _beneish_proxy_risk(current: Record, previous: Record | None) -> float | None:
    if previous is None:
        return None

    revenue = _number(current, "revenue")
    prev_revenue = _number(previous, "revenue")
    receivables = _number(current, "receivables")
    prev_receivables = _number(previous, "receivables")
    assets = _number(current, "total_assets")
    prev_assets = _number(previous, "total_assets")
    current_assets = _number(current, "current_assets")
    prev_current_assets = _number(previous, "current_assets")
    ppe = _number(current, "ppe")
    prev_ppe = _number(previous, "ppe")
    gross_margin = _number(current, "gross_margin")
    prev_gross_margin = _number(previous, "gross_margin")

    dsri = _safe_div(_safe_div(receivables, revenue), _safe_div(prev_receivables, prev_revenue))
    gmi = _safe_div(prev_gross_margin, gross_margin)

    asset_quality = None
    prev_asset_quality = None
    if assets:
        asset_quality = 1.0 - ((current_assets or 0.0) + (ppe or 0.0)) / assets
    if prev_assets:
        prev_asset_quality = 1.0 - ((prev_current_assets or 0.0) + (prev_ppe or 0.0)) / prev_assets
    aqi = _safe_div(asset_quality, prev_asset_quality)

    parts = [
        max(0.0, (dsri or 1.0) - 1.0),
        max(0.0, (gmi or 1.0) - 1.0),
        max(0.0, (aqi or 1.0) - 1.0),
    ]
    if dsri is None and gmi is None and aqi is None:
        return None
    return sum(parts) / len(parts)


def forensic_quality_score(current: Record, previous: Record | None = None, *, weights: Mapping[str, float] | None = None) -> dict[str, Any]:
    """Score accounting-forensics quality on a 0..100 scale.

    Higher is cleaner. This is a research-only scoring primitive: it does not
    infer readiness, recommend trades, or query external data.
    """

    active_weights = dict(DEFAULT_WEIGHTS if weights is None else weights)
    missing_weights = sorted(set(METRIC_NAMES) - set(active_weights))
    if missing_weights:
        raise ValueError(f"Missing forensic score weights: {', '.join(missing_weights)}")

    net_income = _number(current, "net_income", required=True)
    operating_cash_flow = _number(current, "operating_cash_flow", required=True)
    free_cash_flow = _number(current, "free_cash_flow", required=True)
    total_assets = _number(current, "total_assets", required=True)

    prev_assets = _number(previous, "total_assets") if previous else None
    avg_assets = (total_assets + prev_assets) / 2.0 if prev_assets is not None else total_assets
    accrual_ratio = _safe_div((net_income or 0.0) - (operating_cash_flow or 0.0), avg_assets)
    ocf_to_net_income = _safe_div(operating_cash_flow, net_income)
    fcf_conversion = _safe_div(free_cash_flow, net_income)
    asset_growth = _growth(total_assets, prev_assets)
    receivables_growth = _growth(_number(current, "receivables"), _number(previous, "receivables") if previous else None)
    inventory_growth = _growth(_number(current, "inventory"), _number(previous, "inventory") if previous else None)
    revenue_growth = _growth(_number(current, "revenue"), _number(previous, "revenue") if previous else None)
    dilution_rate = _growth(
        _number(current, "shares_outstanding"),
        _number(previous, "shares_outstanding") if previous else None,
    )
    beneish_proxy_risk = _beneish_proxy_risk(current, previous)

    raw_metrics = {
        "accrual_ratio": accrual_ratio,
        "ocf_to_net_income": ocf_to_net_income,
        "fcf_conversion": fcf_conversion,
        "asset_growth": asset_growth,
        "receivables_growth": receivables_growth,
        "inventory_growth": inventory_growth,
        "beneish_proxy_risk": beneish_proxy_risk,
        "dilution_rate": dilution_rate,
    }
    component_scores = {
        "accrual_ratio": _score_low_abs(accrual_ratio, 0.04, 0.16),
        "ocf_to_net_income": _score_conversion(ocf_to_net_income),
        "fcf_conversion": _score_conversion(fcf_conversion, good=0.80, weak=0.20, cap=1.30),
        "asset_growth": _score_low_positive(asset_growth, 0.15, 0.60),
        "receivables_growth": _score_growth_vs_sales(receivables_growth, revenue_growth),
        "inventory_growth": _score_growth_vs_sales(inventory_growth, revenue_growth),
        "beneish_proxy_risk": _score_low_positive(beneish_proxy_risk, 0.05, 0.35),
        "dilution_rate": _score_low_positive(dilution_rate, 0.01, 0.15),
    }

    score = sum(component_scores[name] * float(active_weights[name]) for name in METRIC_NAMES) / sum(float(active_weights[name]) for name in METRIC_NAMES)
    warnings = tuple(name for name, value in raw_metrics.items() if value is None)
    rounded_score = round(_clamp(score) * 100.0, 2)
    interpretation = _interpretation(rounded_score, component_scores, warnings)

    return ForensicQualityResult(
        ticker=_ticker(current),
        factor_family=FACTOR_FAMILY,
        forensic_quality_score=rounded_score,
        component_scores={name: round(value * 100.0, 2) for name, value in component_scores.items()},
        raw_metrics={name: round(value, 6) for name, value in raw_metrics.items() if value is not None},
        warnings=warnings,
        interpretation=interpretation,
    ).as_dict()


def score_universe(rows: Sequence[Record], previous_rows: Sequence[Record] | None = None) -> list[dict[str, Any]]:
    previous_by_ticker = {_ticker(row): row for row in previous_rows or ()}
    scored = [forensic_quality_score(row, previous_by_ticker.get(_ticker(row))) for row in rows]
    return sorted(scored, key=lambda item: (-float(item["forensic_quality_score"]), str(item["ticker"])))


def compare_forensic_quality(
    universe_rows: Sequence[Record],
    crazy_returns_europe_rows: Sequence[Record],
    *,
    previous_universe_rows: Sequence[Record] | None = None,
    previous_crazy_returns_europe_rows: Sequence[Record] | None = None,
) -> dict[str, Any]:
    universe_scores = score_universe(universe_rows, previous_universe_rows)
    cre_scores = score_universe(crazy_returns_europe_rows, previous_crazy_returns_europe_rows)
    universe_values = [float(row["forensic_quality_score"]) for row in universe_scores]
    cre_values = [float(row["forensic_quality_score"]) for row in cre_scores]
    universe_median = round(median(universe_values), 2) if universe_values else None
    cre_median = round(median(cre_values), 2) if cre_values else None
    spread = round(cre_median - universe_median, 2) if cre_median is not None and universe_median is not None else None
    return {
        "factor_family": FACTOR_FAMILY,
        "score_scale": "0-100 higher is cleaner",
        "universe_median": universe_median,
        "crazy_returns_europe_median": cre_median,
        "crazy_returns_europe_vs_universe_spread": spread,
        "universe_count": len(universe_scores),
        "crazy_returns_europe_count": len(cre_scores),
        "universe_scores": universe_scores,
        "crazy_returns_europe_scores": cre_scores,
        "distinct_family_assessment": distinct_family_assessment(),
    }


def distinct_family_assessment() -> dict[str, Any]:
    return {
        "verdict": "distinct_but_adjacent_to_quality",
        "reason": (
            "Forensic quality focuses on accounting manipulation, cash conversion, working-capital stress, "
            "Beneish-style red flags, and dilution. Traditional quality focuses more on profitability, "
            "margin stability, returns on capital, and balance-sheet strength."
        ),
        "validation_requirement": "Treat as a distinct family only after testing incremental rank information versus the existing quality score.",
    }


def _interpretation(score: float, component_scores: Mapping[str, float], warnings: Sequence[str]) -> str:
    weak = [name for name, value in component_scores.items() if value < 0.45]
    if score >= 75.0:
        base = "clean forensic profile"
    elif score >= 55.0:
        base = "mixed forensic profile"
    else:
        base = "elevated forensic risk profile"
    weak_text = ", ".join(weak) if weak else "no major component breach"
    warning_text = ", ".join(warnings) if warnings else "full core input coverage"
    return f"{base}; weak_components={weak_text}; input_warnings={warning_text}."

