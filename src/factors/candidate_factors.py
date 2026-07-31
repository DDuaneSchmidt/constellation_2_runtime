from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable


ACTIVATION_STATUS = "research_only_not_ranked"


@dataclass(frozen=True)
class CandidateFactor:
    factor_id: str
    name: str
    family: str
    preferred_direction: str
    required_inputs: tuple[str, ...]
    formula: str
    rationale: str
    testing_notes: str
    activation_status: str = ACTIVATION_STATUS

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_FACTORS: tuple[CandidateFactor, ...] = (
    CandidateFactor(
        factor_id="value_fcf_yield_v1",
        name="FCF Yield",
        family="value",
        preferred_direction="higher_is_better",
        required_inputs=("free_cash_flow", "market_cap"),
        formula="free_cash_flow / market_cap",
        rationale="Cheap companies with cash-backed equity value may be underpriced versus reported earnings-only value screens.",
        testing_notes="Test sector-neutral ranks and exclude structurally negative FCF until separate turnaround tests exist.",
    ),
    CandidateFactor(
        factor_id="value_earnings_yield_v1",
        name="Earnings Yield",
        family="value",
        preferred_direction="higher_is_better",
        required_inputs=("net_income", "market_cap"),
        formula="net_income / market_cap",
        rationale="Inverts P/E so cheaper earnings streams rank higher while preserving a simple cross-sectional value signal.",
        testing_notes="Pair with forensic and cash conversion filters to avoid high-accrual value traps.",
    ),
    CandidateFactor(
        factor_id="value_shareholder_yield_v1",
        name="Shareholder Yield",
        family="value",
        preferred_direction="higher_is_better",
        required_inputs=("dividends_paid", "net_buybacks", "market_cap"),
        formula="(dividends_paid + net_buybacks) / market_cap",
        rationale="Capital returned to shareholders can expose disciplined cash deployment when funded internally.",
        testing_notes="Separate buyback-funded-by-debt cases from FCF-funded shareholder yield.",
    ),
    CandidateFactor(
        factor_id="value_ev_ebit_v1",
        name="EV/EBIT",
        family="value",
        preferred_direction="lower_is_better",
        required_inputs=("enterprise_value", "ebit"),
        formula="enterprise_value / ebit",
        rationale="Enterprise-value valuation compares operating earnings across capital structures.",
        testing_notes="Use positive EBIT only in baseline tests; handle financials separately.",
    ),
    CandidateFactor(
        factor_id="quality_gross_profitability_v1",
        name="Gross Profitability",
        family="quality",
        preferred_direction="higher_is_better",
        required_inputs=("gross_profit", "total_assets"),
        formula="gross_profit / total_assets",
        rationale="Gross profitability can capture durable unit economics before operating expense policy choices.",
        testing_notes="Test against ROIC to determine whether it adds incremental quality information.",
    ),
    CandidateFactor(
        factor_id="quality_fcf_margin_v1",
        name="FCF Margin",
        family="quality",
        preferred_direction="higher_is_better",
        required_inputs=("free_cash_flow", "revenue"),
        formula="free_cash_flow / revenue",
        rationale="High free-cash-flow margin indicates that revenue converts into discretionary cash after reinvestment.",
        testing_notes="Winsorize extreme revenue denominators and compare versus operating margin.",
    ),
    CandidateFactor(
        factor_id="quality_roic_v1",
        name="ROIC",
        family="quality",
        preferred_direction="higher_is_better",
        required_inputs=("nopat", "invested_capital"),
        formula="nopat / invested_capital",
        rationale="Return on invested capital measures capital efficiency and reinvestment quality.",
        testing_notes="Define invested capital consistently before cross-country testing.",
    ),
    CandidateFactor(
        factor_id="quality_roa_v1",
        name="ROA",
        family="quality",
        preferred_direction="higher_is_better",
        required_inputs=("net_income", "total_assets"),
        formula="net_income / total_assets",
        rationale="Return on assets gives a broad profitability measure that is simple and widely available.",
        testing_notes="Use as a coverage-friendly baseline, then compare against ROIC and gross profitability.",
    ),
    CandidateFactor(
        factor_id="forensic_accruals_v1",
        name="Accruals",
        family="forensic",
        preferred_direction="lower_is_better",
        required_inputs=("net_income", "operating_cash_flow", "average_total_assets"),
        formula="(net_income - operating_cash_flow) / average_total_assets",
        rationale="High accruals flag earnings that are not supported by operating cash flow.",
        testing_notes="Coordinate with forensic quality score; test as standalone and as a penalty overlay.",
    ),
    CandidateFactor(
        factor_id="forensic_cash_conversion_v1",
        name="Cash Conversion",
        family="forensic",
        preferred_direction="higher_is_better",
        required_inputs=("operating_cash_flow", "net_income"),
        formula="operating_cash_flow / net_income",
        rationale="Reported earnings are more reliable when they convert into operating cash.",
        testing_notes="Handle negative net income separately; do not reward mechanically negative ratios.",
    ),
    CandidateFactor(
        factor_id="forensic_asset_growth_v1",
        name="Asset Growth",
        family="forensic",
        preferred_direction="lower_is_better",
        required_inputs=("total_assets", "prior_total_assets"),
        formula="(total_assets - prior_total_assets) / abs(prior_total_assets)",
        rationale="Rapid asset expansion can indicate acquisition risk, capitalization risk, or lower future returns.",
        testing_notes="Sector-neutralize and isolate acquisition-driven balance-sheet jumps.",
    ),
    CandidateFactor(
        factor_id="forensic_receivables_growth_v1",
        name="Receivables Growth",
        family="forensic",
        preferred_direction="lower_vs_sales_growth_is_better",
        required_inputs=("receivables", "prior_receivables", "revenue", "prior_revenue"),
        formula="receivables_growth - revenue_growth",
        rationale="Receivables growing faster than sales can flag revenue-recognition and collection risk.",
        testing_notes="Test spread versus sales growth, not raw receivables growth alone.",
    ),
    CandidateFactor(
        factor_id="forensic_inventory_growth_v1",
        name="Inventory Growth",
        family="forensic",
        preferred_direction="lower_vs_sales_growth_is_better",
        required_inputs=("inventory", "prior_inventory", "revenue", "prior_revenue"),
        formula="inventory_growth - revenue_growth",
        rationale="Inventory growth outrunning sales can flag demand weakness or future margin pressure.",
        testing_notes="Sector-neutralize and treat retailers/manufacturers separately from asset-light businesses.",
    ),
    CandidateFactor(
        factor_id="stability_sales_stability_v1",
        name="Sales Stability",
        family="stability",
        preferred_direction="higher_is_better",
        required_inputs=("historical_revenue",),
        formula="1 - coefficient_of_variation(revenue_growth)",
        rationale="Stable sales can identify durable demand and reduce exposure to transitory growth spikes.",
        testing_notes="Use rolling multi-year windows and compare against margin stability.",
    ),
    CandidateFactor(
        factor_id="stability_eps_stability_v1",
        name="EPS Stability",
        family="stability",
        preferred_direction="higher_is_better",
        required_inputs=("historical_eps",),
        formula="1 - coefficient_of_variation(eps_growth)",
        rationale="Stable EPS can indicate durable operating economics and lower earnings surprise risk.",
        testing_notes="Adjust for one-time EPS shocks and changes in shares outstanding.",
    ),
    CandidateFactor(
        factor_id="stability_margin_stability_v1",
        name="Margin Stability",
        family="stability",
        preferred_direction="higher_is_better",
        required_inputs=("historical_operating_margin",),
        formula="1 - standard_deviation(operating_margin)",
        rationale="Stable margins can reveal pricing power, cost control, and resilient business models.",
        testing_notes="Test through inflation and slowdown regimes; compare gross and operating margin variants.",
    ),
    CandidateFactor(
        factor_id="momentum_12_1_v1",
        name="12-1 Momentum",
        family="momentum",
        preferred_direction="higher_is_better",
        required_inputs=("total_return_month_12_to_month_1",),
        formula="cumulative_return(month_-12 through month_-1)",
        rationale="Skipping the most recent month captures medium-term trend persistence while reducing short-term reversal noise.",
        testing_notes="Use point-in-time prices and validate country and sector neutrality for Europe.",
    ),
    CandidateFactor(
        factor_id="momentum_industry_momentum_v1",
        name="Industry Momentum",
        family="momentum",
        preferred_direction="higher_is_better",
        required_inputs=("industry_peer_returns",),
        formula="median_peer_12_1_momentum_by_industry",
        rationale="Industry-level trends can capture common fundamental or flow-driven leadership.",
        testing_notes="Require stable industry mapping before testing; avoid look-ahead membership changes.",
    ),
    CandidateFactor(
        factor_id="momentum_sharpe_momentum_v1",
        name="Sharpe Momentum",
        family="momentum",
        preferred_direction="higher_is_better",
        required_inputs=("historical_returns",),
        formula="mean_return / standard_deviation_return",
        rationale="Risk-adjusted momentum prefers trends with smoother realized return paths.",
        testing_notes="Compare against raw 12-1 momentum to ensure volatility scaling adds value.",
    ),
)


def candidate_factor_library() -> list[dict[str, Any]]:
    """Return the full research-only factor bench.

    The library is descriptive metadata only. It does not score securities,
    activate rank inputs, mutate Aegis candidates, or recommend trades.
    """

    return [factor.to_dict() for factor in _FACTORS]


def factors_by_family(family: str) -> list[dict[str, Any]]:
    normalized = family.strip().lower()
    return [factor.to_dict() for factor in _FACTORS if factor.family == normalized]


def get_candidate_factor(factor_id: str) -> dict[str, Any]:
    normalized = factor_id.strip().lower()
    for factor in _FACTORS:
        if factor.factor_id == normalized:
            return factor.to_dict()
    valid = ", ".join(factor.factor_id for factor in _FACTORS)
    raise ValueError(f"Unknown candidate factor '{factor_id}'. Valid factors: {valid}")


def research_bench_summary(families: Iterable[str] | None = None) -> dict[str, Any]:
    selected_families = {family.strip().lower() for family in families} if families is not None else {
        factor.family for factor in _FACTORS
    }
    counts = {family: len([factor for factor in _FACTORS if factor.family == family]) for family in sorted(selected_families)}
    return {
        "library_id": "aegis_europe_candidate_factor_library_v1",
        "activation_status": ACTIVATION_STATUS,
        "ranking_activation_allowed": False,
        "factor_count": sum(counts.values()),
        "family_counts": counts,
        "families": sorted(family for family, count in counts.items() if count > 0),
    }

