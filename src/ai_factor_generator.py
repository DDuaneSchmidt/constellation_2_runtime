from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable


CandidateRecord = dict[str, str | int]


FACTOR_FAMILIES: tuple[str, ...] = (
    "quality",
    "value",
    "momentum",
    "low_volatility",
    "earnings_revisions",
    "cash_flow",
    "balance_sheet_strength",
    "shareholder_yield",
    "small_cap_quality",
    "turnaround",
    "defensive_growth",
    "anti_ultrasafe",
    "capital_efficiency",
    "margin_stability",
    "earnings_quality",
    "financial_strength",
)


@dataclass(frozen=True)
class FactorCandidate:
    candidate_id: str
    name: str
    hypothesis: str
    factor_family: str
    economic_rationale: str
    expected_behavior: str
    why_it_may_diversify_ultrasafe: str
    complexity_score: int
    overfit_risk_score: int
    novelty_score: int

    def to_dict(self) -> CandidateRecord:
        return asdict(self)


_FAMILY_DESIGNS: dict[str, dict[str, str | int]] = {
    "quality": {
        "name": "Durable Quality Compounders",
        "hypothesis": "Companies with persistent profitability, conservative accruals, and stable reinvestment should outperform lower quality peers after controlling for sector and size.",
        "economic_rationale": "Durable quality can reflect pricing power, process discipline, and capital allocation skill that are slow for competitors to arbitrage away.",
        "expected_behavior": "Expected to lag in speculative rebounds but hold up better when earnings durability is repriced.",
        "why_it_may_diversify_ultrasafe": "It seeks equity-specific quality premia rather than the broad defensive allocation profile typically associated with UltraSafe.",
        "complexity_score": 3,
        "overfit_risk_score": 3,
        "novelty_score": 3,
    },
    "value": {
        "name": "Cash-Backed Value Re-Rating",
        "hypothesis": "Statistically cheap companies with positive free cash flow and manageable leverage should earn a re-rating premium versus cheap firms with weak cash conversion.",
        "economic_rationale": "Investors may over-penalize disliked but solvent businesses when near-term pessimism obscures cash-backed survivability.",
        "expected_behavior": "Expected to perform best during valuation normalization and struggle in long-duration growth-led markets.",
        "why_it_may_diversify_ultrasafe": "It introduces cyclically sensitive valuation exposure that may pay off in regimes where defensive safety is crowded.",
        "complexity_score": 3,
        "overfit_risk_score": 4,
        "novelty_score": 3,
    },
    "momentum": {
        "name": "Fundamental Momentum Confirmation",
        "hypothesis": "Stocks with positive price momentum confirmed by improving fundamentals should outperform price momentum names without fundamental support.",
        "economic_rationale": "Slow information diffusion can create continuation when price trends and business revisions point in the same direction.",
        "expected_behavior": "Expected to do well in persistent trends and suffer during sharp reversals or factor crashes.",
        "why_it_may_diversify_ultrasafe": "It can capture upside participation when risk appetite improves, offsetting UltraSafe-style defensiveness.",
        "complexity_score": 4,
        "overfit_risk_score": 5,
        "novelty_score": 4,
    },
    "low_volatility": {
        "name": "Low Volatility With Solvency Filter",
        "hypothesis": "Low realized volatility stocks with strong solvency should deliver better risk-adjusted returns than low volatility stocks with hidden balance sheet stress.",
        "economic_rationale": "The low-risk anomaly may be strongest when apparent stability is supported by actual financial resilience.",
        "expected_behavior": "Expected to reduce drawdowns but lag in high-beta risk-on phases.",
        "why_it_may_diversify_ultrasafe": "It isolates single-stock low-risk behavior rather than relying on a portfolio-level safety sleeve.",
        "complexity_score": 3,
        "overfit_risk_score": 3,
        "novelty_score": 2,
    },
    "earnings_revisions": {
        "name": "Revision Breadth Acceleration",
        "hypothesis": "Companies with broad, accelerating positive analyst revisions should outperform firms with isolated or decelerating upgrades.",
        "economic_rationale": "Analyst estimate changes can proxy for improving information that is incorporated gradually across market participants.",
        "expected_behavior": "Expected to work around earnings cycles and weaken when revisions are already fully capitalized.",
        "why_it_may_diversify_ultrasafe": "It responds to changing earnings information, a different driver than static safety or drawdown control.",
        "complexity_score": 4,
        "overfit_risk_score": 5,
        "novelty_score": 4,
    },
    "cash_flow": {
        "name": "Free Cash Flow Resilience",
        "hypothesis": "Companies with high free cash flow yield and stable conversion should outperform companies whose reported earnings do not convert to cash.",
        "economic_rationale": "Cash generation supports reinvestment, debt service, buybacks, and downside resilience without depending on accounting estimates alone.",
        "expected_behavior": "Expected to be defensive in tightening liquidity regimes and competitive during value recoveries.",
        "why_it_may_diversify_ultrasafe": "It adds cash-generation exposure that can compound differently from allocation-driven safety.",
        "complexity_score": 3,
        "overfit_risk_score": 3,
        "novelty_score": 3,
    },
    "balance_sheet_strength": {
        "name": "Net Cash Balance Sheet Strength",
        "hypothesis": "Companies with net cash, low refinancing risk, and positive operating income should outperform levered peers during credit stress and recoveries.",
        "economic_rationale": "Strong balance sheets create option value when weaker competitors face funding constraints.",
        "expected_behavior": "Expected to protect capital in stress and participate when strong firms deploy liquidity opportunistically.",
        "why_it_may_diversify_ultrasafe": "It expresses safety through corporate funding resilience rather than broad portfolio de-risking.",
        "complexity_score": 2,
        "overfit_risk_score": 2,
        "novelty_score": 2,
    },
    "shareholder_yield": {
        "name": "Sustainable Shareholder Yield",
        "hypothesis": "Companies returning capital through dividends and buybacks funded by free cash flow should outperform firms using debt-funded payouts.",
        "economic_rationale": "Sustainable distributions can signal capital discipline while avoiding the fragility of financially engineered yield.",
        "expected_behavior": "Expected to perform well in mature cash-generative sectors and lag aggressive reinvestment stories.",
        "why_it_may_diversify_ultrasafe": "It adds capital-return exposure that can benefit from shareholder discipline rather than volatility suppression.",
        "complexity_score": 4,
        "overfit_risk_score": 4,
        "novelty_score": 3,
    },
    "small_cap_quality": {
        "name": "Small Cap Quality Escape Velocity",
        "hypothesis": "Small companies with profitability, clean balance sheets, and improving margins should outperform small caps selected only on cheapness or growth.",
        "economic_rationale": "Quality filters can separate scalable emerging businesses from structurally fragile small-cap names.",
        "expected_behavior": "Expected to be cyclical but less fragile than broad small-cap exposure.",
        "why_it_may_diversify_ultrasafe": "It adds size and idiosyncratic growth optionality that UltraSafe may intentionally avoid.",
        "complexity_score": 5,
        "overfit_risk_score": 5,
        "novelty_score": 5,
    },
    "turnaround": {
        "name": "Confirmed Turnaround Inflection",
        "hypothesis": "Underperforming firms with improving margins, positive revisions, and declining leverage should outperform distressed firms without operational confirmation.",
        "economic_rationale": "Markets may underreact when multiple independent signs confirm that a previously weak business is improving.",
        "expected_behavior": "Expected to be lumpy, higher beta, and most useful after pessimism becomes excessive.",
        "why_it_may_diversify_ultrasafe": "It intentionally seeks recovery convexity, which can diversify a safety-first portfolio profile.",
        "complexity_score": 6,
        "overfit_risk_score": 6,
        "novelty_score": 5,
    },
    "defensive_growth": {
        "name": "Defensive Growth at Reasonable Quality",
        "hypothesis": "Moderate growers with stable margins, recurring demand, and reasonable valuations should outperform high-growth firms with fragile economics.",
        "economic_rationale": "Durable but non-exuberant growth may avoid both value traps and speculative growth compression.",
        "expected_behavior": "Expected to compound steadily but lag during speculative growth surges.",
        "why_it_may_diversify_ultrasafe": "It keeps growth exposure while avoiding dependence on a pure low-volatility or bond-like safety profile.",
        "complexity_score": 4,
        "overfit_risk_score": 4,
        "novelty_score": 4,
    },
    "anti_ultrasafe": {
        "name": "Anti-UltraSafe Cyclical Convexity",
        "hypothesis": "Select higher-beta cyclical equities with improving fundamentals should outperform defensive safety baskets during risk-on recoveries.",
        "economic_rationale": "When investors unwind safety crowding, improving cyclicals can benefit from both earnings recovery and multiple expansion.",
        "expected_behavior": "Expected to underperform during stress but outperform during broad risk appetite and cyclical acceleration.",
        "why_it_may_diversify_ultrasafe": "It is explicitly designed to move differently from UltraSafe by targeting recoveries where defensive safety may lag.",
        "complexity_score": 5,
        "overfit_risk_score": 6,
        "novelty_score": 6,
    },
    "capital_efficiency": {
        "name": "Capital Efficiency Persistence",
        "hypothesis": "Companies with high and stable returns on invested capital should outperform firms where growth requires increasingly inefficient capital deployment.",
        "economic_rationale": "Persistent capital efficiency can indicate durable moats and disciplined reinvestment opportunities.",
        "expected_behavior": "Expected to perform best when investors reward self-funded compounding and punish capital-intensive growth.",
        "why_it_may_diversify_ultrasafe": "It focuses on internal reinvestment economics rather than defensive asset allocation.",
        "complexity_score": 4,
        "overfit_risk_score": 4,
        "novelty_score": 3,
    },
    "margin_stability": {
        "name": "Margin Stability Under Pressure",
        "hypothesis": "Companies with stable gross and operating margins through demand shocks should outperform firms with more volatile margin structures.",
        "economic_rationale": "Margin stability can reveal pricing power, variable cost flexibility, and resilient demand.",
        "expected_behavior": "Expected to help in inflation, slowdown, and earnings-disappointment regimes.",
        "why_it_may_diversify_ultrasafe": "It identifies business-model resilience directly, which may diversify generic defensive screens.",
        "complexity_score": 4,
        "overfit_risk_score": 4,
        "novelty_score": 4,
    },
    "earnings_quality": {
        "name": "Accruals-Aware Earnings Quality",
        "hypothesis": "Companies with low accruals, stable cash conversion, and conservative revenue recognition should outperform high-accrual peers.",
        "economic_rationale": "High-quality earnings are less likely to reverse, reducing the risk of accounting-driven disappointment.",
        "expected_behavior": "Expected to avoid blowups and lag when markets reward aggressive reported growth.",
        "why_it_may_diversify_ultrasafe": "It controls accounting risk at the company level rather than reducing market exposure.",
        "complexity_score": 4,
        "overfit_risk_score": 4,
        "novelty_score": 3,
    },
    "financial_strength": {
        "name": "Composite Financial Strength",
        "hypothesis": "A composite of solvency, liquidity, profitability, and cash conversion should outperform any single financial health metric used alone.",
        "economic_rationale": "Multiple independent financial health signals reduce reliance on one accounting measure and capture broader survivability.",
        "expected_behavior": "Expected to be steady, lower turnover, and strongest when weak balance sheets are punished.",
        "why_it_may_diversify_ultrasafe": "It provides bottom-up balance-sheet resilience that can behave differently from top-down safety allocation.",
        "complexity_score": 5,
        "overfit_risk_score": 5,
        "novelty_score": 4,
    },
}


def generate_candidates(factor_families: Iterable[str] | None = None) -> list[CandidateRecord]:
    """Generate deterministic factor and portfolio hypotheses.

    This function is generation-only. It performs no network calls, no API calls,
    no Portfolio123 calls, no backtests, and no portfolio construction.
    """
    families = FACTOR_FAMILIES if factor_families is None else tuple(factor_families)
    return [_candidate_for_family(family).to_dict() for family in families]


def generate_by_family(factor_family: str) -> list[CandidateRecord]:
    """Generate deterministic candidates for one factor family."""
    return [_candidate_for_family(factor_family).to_dict()]


def generate_anti_ultrasafe_candidates() -> list[CandidateRecord]:
    """Generate candidates intended to diversify an UltraSafe-like profile."""
    return generate_by_family("anti_ultrasafe")


def _candidate_for_family(factor_family: str) -> FactorCandidate:
    normalized_family = factor_family.strip().lower()
    if normalized_family not in _FAMILY_DESIGNS:
        valid = ", ".join(FACTOR_FAMILIES)
        raise ValueError(f"Unknown factor family '{factor_family}'. Valid families: {valid}")

    design = _FAMILY_DESIGNS[normalized_family]
    return FactorCandidate(
        candidate_id=f"aifg_{normalized_family}_v1",
        name=str(design["name"]),
        hypothesis=str(design["hypothesis"]),
        factor_family=normalized_family,
        economic_rationale=str(design["economic_rationale"]),
        expected_behavior=str(design["expected_behavior"]),
        why_it_may_diversify_ultrasafe=str(design["why_it_may_diversify_ultrasafe"]),
        complexity_score=int(design["complexity_score"]),
        overfit_risk_score=int(design["overfit_risk_score"]),
        novelty_score=int(design["novelty_score"]),
    )
