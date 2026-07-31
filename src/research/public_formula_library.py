from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable


ACTIVATION_STATUS = "research_only_not_ranked"


@dataclass(frozen=True)
class PublicFormula:
    formula_id: str
    name: str
    category: str
    exact_formula: str
    source: str
    interpretation: str
    europe_notes: str
    preferred_direction: str
    evidence_status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_FORMULAS: tuple[PublicFormula, ...] = (
    PublicFormula(
        formula_id="growth_pegy_ratio_v1",
        name="PEGY Ratio",
        category="Growth",
        exact_formula="PEGY = P/E / (projected EPS growth rate + dividend yield)",
        source="reports/atlas_v2_research_os/manual_fundamental_claims/pegy_ratio/2026-06-06/pegy_ratio_metric_spec.json",
        interpretation="Valuation adjusted for expected EPS growth and dividend yield; lower values imply cheaper growth plus income.",
        europe_notes="Potentially useful in Europe because dividends matter more than in US growth screens, but it is gated until point-in-time forward-growth and dividend data exist.",
        preferred_direction="lower_is_better",
        evidence_status="manual_public_formula_spec",
    ),
    PublicFormula(
        formula_id="growth_sales_growth_v1",
        name="Sales Growth",
        category="Growth",
        exact_formula="SalesGr%TTM",
        source="src/short_candidate_generator.py: expensive_growth_breakdown rule",
        interpretation="Trailing sales growth; positive growth is supportive, while deceleration can flag weakening demand.",
        europe_notes="Europe regional factor evidence does not yet support a broad growth overweight, so use as a diagnostic rather than a primary Europe rank input.",
        preferred_direction="higher_is_better",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="growth_sales_growth_deceleration_v1",
        name="Sales Growth Deceleration",
        category="Growth",
        exact_formula="SalesGr%TTM - SalesGr%PYQ",
        source="src/short_candidate_generator.py: expensive_growth_breakdown rank formula",
        interpretation="Measures whether sales growth is improving or slowing versus the prior-year quarter comparison.",
        europe_notes="Useful as a risk flag for expensive European growth names; not a validated positive Europe factor in current evidence.",
        preferred_direction="higher_is_better_for_longs",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="growth_eps_growth_deceleration_v1",
        name="EPS Growth Deceleration",
        category="Growth",
        exact_formula="EPS%ChgTTM < EPS%ChgPYQ",
        source="src/short_candidate_generator.py: expensive_growth_breakdown rule",
        interpretation="Flags companies where trailing EPS growth has fallen versus the prior-year comparison.",
        europe_notes="Should be tested with local accounting conventions and currency effects before use in Europe.",
        preferred_direction="false_is_better_for_longs",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="value_fcf_yield_v1",
        name="FCF Yield",
        category="Value",
        exact_formula="free_cash_flow / market_cap",
        source="src/factors/candidate_factors.py",
        interpretation="Cash-backed value yield; higher values imply more free cash flow per unit of equity value.",
        europe_notes="Attractive for Europe where value evidence is strong, but needs point-in-time cash-flow and market-cap data.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="value_earnings_yield_v1",
        name="Earnings Yield",
        category="Value",
        exact_formula="net_income / market_cap",
        source="src/factors/candidate_factors.py",
        interpretation="Inverse P/E; cheaper earnings streams score higher.",
        europe_notes="Maps to the positive Europe value prior, but should be paired with forensic controls to avoid value traps.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="value_shareholder_yield_v1",
        name="Shareholder Yield",
        category="Value",
        exact_formula="(dividends_paid + net_buybacks) / market_cap",
        source="src/factors/candidate_factors.py",
        interpretation="Measures direct capital return to shareholders relative to equity value.",
        europe_notes="Europe dividend culture makes this more relevant than pure buyback yield, but buyback/debt funding must be separated.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="value_ev_ebit_v1",
        name="EV/EBIT",
        category="Value",
        exact_formula="enterprise_value / ebit",
        source="src/factors/candidate_factors.py",
        interpretation="Enterprise valuation relative to operating earnings; lower ratios imply cheaper operating businesses.",
        europe_notes="Useful across countries with different leverage norms, but financials need separate handling.",
        preferred_direction="lower_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="value_pe_ratio_v1",
        name="P/E Ratio",
        category="Value",
        exact_formula="PEExclXorTTM",
        source="src/short_candidate_generator.py: expensive_growth_breakdown rule",
        interpretation="Trailing P/E excluding extraordinary items; high values imply expensive earnings.",
        europe_notes="Europe value evidence is favorable, but P/E is noisier than EV/EBIT or FCF yield for cross-country comparisons.",
        preferred_direction="lower_is_better",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="quality_gross_profitability_v1",
        name="Gross Profitability",
        category="Quality",
        exact_formula="gross_profit / total_assets",
        source="src/factors/candidate_factors.py",
        interpretation="Production profitability before operating-expense and capital-structure choices.",
        europe_notes="Quality is positive over the long run but less Europe-specific than value or momentum; use as a cap or filter.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="quality_fcf_margin_v1",
        name="FCF Margin",
        category="Quality",
        exact_formula="free_cash_flow / revenue",
        source="src/factors/candidate_factors.py and src/short_candidate_generator.py: FCF(0, TTM) / Sales(0, TTM)",
        interpretation="Revenue conversion into free cash flow after reinvestment.",
        europe_notes="Relevant for separating strong European small caps from accounting-led profitability.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="quality_roic_v1",
        name="ROIC",
        category="Quality",
        exact_formula="nopat / invested_capital",
        source="src/factors/candidate_factors.py",
        interpretation="Return generated on operating capital employed.",
        europe_notes="Promising as a quality control, but invested-capital definitions need consistent cross-country normalization.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="quality_roa_v1",
        name="ROA",
        category="Quality",
        exact_formula="net_income / total_assets",
        source="src/factors/candidate_factors.py and src/short_candidate_generator.py: ROA%TTM",
        interpretation="Broad asset productivity and profitability measure.",
        europe_notes="Useful coverage-friendly fallback, but local correlation work treats ROA as partly redundant with stronger quality measures.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="quality_operating_margin_v1",
        name="Operating Margin",
        category="Quality",
        exact_formula="OpMgn%TTM",
        source="src/short_candidate_generator.py: negative_roa rule",
        interpretation="Operating profitability relative to sales.",
        europe_notes="Sector mix is important in Europe; use sector-neutral comparisons before drawing conclusions.",
        preferred_direction="higher_is_better",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="quality_margin_degradation_v1",
        name="Margin Degradation",
        category="Quality",
        exact_formula="OpMgn%TTM - OpMgn%PYQ",
        source="src/short_candidate_generator.py: margin_degradation rank formula",
        interpretation="Change in operating margin versus the prior-year quarter comparison.",
        europe_notes="Useful for cyclicals and industrials, but commodity and FX effects need explicit review.",
        preferred_direction="higher_is_better_for_longs",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="quality_gross_margin_degradation_v1",
        name="Gross Margin Degradation",
        category="Quality",
        exact_formula="GrossMgn%TTM < GrossMgn%PYQ",
        source="src/short_candidate_generator.py: margin_degradation rule",
        interpretation="Flags deterioration in product-level economics before operating expenses.",
        europe_notes="Cross-country accounting comparability and sector neutrality are required before ranking Europe holdings.",
        preferred_direction="false_is_better_for_longs",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="quality_cash_conversion_v1",
        name="Cash Conversion",
        category="Quality",
        exact_formula="operating_cash_flow / net_income",
        source="src/factors/candidate_factors.py",
        interpretation="Reported earnings quality measured by conversion into operating cash flow.",
        europe_notes="Important in small and micro caps where earnings quality risk can dominate reported profitability.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="stability_sales_stability_v1",
        name="Sales Stability",
        category="Stability",
        exact_formula="1 - coefficient_of_variation(revenue_growth)",
        source="src/factors/candidate_factors.py",
        interpretation="Rewards smoother revenue growth histories over volatile growth spikes.",
        europe_notes="Current Europe evidence shows stability is positive but not Europe-specific; keep weight small until tested.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="stability_eps_stability_v1",
        name="EPS Stability",
        category="Stability",
        exact_formula="1 - coefficient_of_variation(eps_growth)",
        source="src/factors/candidate_factors.py",
        interpretation="Rewards smoother earnings growth histories.",
        europe_notes="Needs point-in-time restatement controls and share-count adjustment for Europe.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="stability_margin_stability_v1",
        name="Margin Stability",
        category="Stability",
        exact_formula="1 - standard_deviation(operating_margin)",
        source="src/factors/candidate_factors.py",
        interpretation="Rewards companies with less volatile operating margins.",
        europe_notes="Potentially useful as a risk control, but not a primary Europe alpha family in current evidence.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="stability_leverage_v1",
        name="Debt to EBITDA",
        category="Stability",
        exact_formula="DebtTotQ / EBITDA(0, TTM)",
        source="src/short_candidate_generator.py: levered_earnings_decay rule",
        interpretation="Balance-sheet leverage relative to trailing EBITDA.",
        europe_notes="Use with sector and country financing norms; financials require exclusion or separate model treatment.",
        preferred_direction="lower_is_better",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="stability_interest_coverage_v1",
        name="Interest Coverage",
        category="Stability",
        exact_formula="IntCovTTM",
        source="src/short_candidate_generator.py: levered_earnings_decay rule",
        interpretation="Ability of earnings to cover interest expense.",
        europe_notes="Helpful in higher-rate regimes, but accounting definitions and negative EBIT cases need explicit handling.",
        preferred_direction="higher_is_better",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="momentum_12_1_v1",
        name="12-1 Momentum",
        category="Momentum",
        exact_formula="cumulative_return(month_-12 through month_-1)",
        source="src/factors/candidate_factors.py",
        interpretation="Medium-term price trend excluding the most recent month to reduce reversal noise.",
        europe_notes="Europe factor evidence shows momentum is the strongest Europe-specific family in current research.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="momentum_relative_strength_26_v1",
        name="26-Week Relative Strength",
        category="Momentum",
        exact_formula="RelStrength(26)",
        source="src/short_candidate_generator.py: weak_momentum rule",
        interpretation="Intermediate relative price strength versus the comparison universe.",
        europe_notes="A direct fit with the Europe momentum prior; needs local universe and currency-consistent price history.",
        preferred_direction="higher_is_better",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="momentum_relative_strength_13_v1",
        name="13-Week Relative Strength",
        category="Momentum",
        exact_formula="RelStrength(13)",
        source="src/short_candidate_generator.py: weak_relative_strength_negative_estimates rule",
        interpretation="Shorter intermediate relative strength signal.",
        europe_notes="Can complement 12-1 momentum, but shorter windows may increase turnover in less-liquid Europe small caps.",
        preferred_direction="higher_is_better",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="momentum_52_week_price_change_v1",
        name="52-Week Price Change",
        category="Momentum",
        exact_formula="Pr52W%Chg",
        source="src/short_candidate_generator.py: weak_momentum rule",
        interpretation="One-year price appreciation or decline.",
        europe_notes="Needs total-return adjustment for high-dividend Europe names when possible.",
        preferred_direction="higher_is_better",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="momentum_moving_average_stack_v1",
        name="Moving Average Stack",
        category="Momentum",
        exact_formula="Close(0) < SMA(50, 0); SMA(50, 0) < SMA(200, 0)",
        source="src/short_candidate_generator.py: weak_momentum rule",
        interpretation="Trend confirmation using price below declining intermediate and long moving averages.",
        europe_notes="Use as a trend-risk flag; calendar gaps and local holidays require careful price-series handling.",
        preferred_direction="false_is_better_for_longs",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="momentum_sharpe_momentum_v1",
        name="Sharpe Momentum",
        category="Momentum",
        exact_formula="mean_return / standard_deviation_return",
        source="src/factors/candidate_factors.py",
        interpretation="Risk-adjusted price trend quality.",
        europe_notes="May help avoid volatile microcap momentum, but should be tested against raw momentum for incremental value.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="industry_industry_momentum_v1",
        name="Industry Momentum",
        category="Industry",
        exact_formula="median_peer_12_1_momentum_by_industry",
        source="src/factors/candidate_factors.py",
        interpretation="Industry-level trend persistence using peer median momentum.",
        europe_notes="Requires stable Europe industry classification and point-in-time membership to avoid look-ahead bias.",
        preferred_direction="higher_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="industry_sector_exclusion_financials_v1",
        name="Financials Exclusion",
        category="Industry",
        exact_formula="Universe($Financials) = false",
        source="src/short_candidate_generator.py: DEFAULT_UNIVERSE_RULES",
        interpretation="Removes financial-sector names when formulas are not meaningful for financial balance sheets.",
        europe_notes="Especially important for Europe because banks and insurers are large benchmark weights and use different accounting economics.",
        preferred_direction="constraint",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="forensic_accruals_v1",
        name="Accruals",
        category="Forensic",
        exact_formula="(net_income - operating_cash_flow) / average_total_assets",
        source="src/factors/candidate_factors.py and reports/forensic_quality_research.md",
        interpretation="High accruals flag earnings that are not supported by operating cash flow.",
        europe_notes="Key small-cap safeguard; likely more valuable as a penalty or filter than a standalone alpha factor.",
        preferred_direction="lower_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="forensic_accrual_ratio_p123_v1",
        name="Portfolio123 Accrual Ratio",
        category="Forensic",
        exact_formula="AccrualRatioTTM",
        source="src/short_candidate_generator.py: accruals_cashflow_mismatch rule",
        interpretation="Portfolio123-style trailing accrual ratio; higher values indicate weaker earnings quality.",
        europe_notes="Needs point-in-time balance sheet and cash-flow fields for Europe before use.",
        preferred_direction="lower_is_better",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="forensic_asset_growth_v1",
        name="Asset Growth",
        category="Forensic",
        exact_formula="(total_assets - prior_total_assets) / abs(prior_total_assets)",
        source="src/factors/candidate_factors.py and reports/forensic_quality_research.md",
        interpretation="Rapid asset expansion can flag acquisition, capitalization, or lower-future-return risk.",
        europe_notes="Needs sector-neutral testing because asset growth differs strongly by industry.",
        preferred_direction="lower_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="forensic_receivables_growth_v1",
        name="Receivables Growth Spread",
        category="Forensic",
        exact_formula="receivables_growth - revenue_growth",
        source="src/factors/candidate_factors.py and reports/forensic_quality_research.md",
        interpretation="Receivables growing faster than sales can flag revenue-recognition or collection risk.",
        europe_notes="Useful for small industrial and distributor names; country reporting conventions need normalization.",
        preferred_direction="lower_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="forensic_inventory_growth_v1",
        name="Inventory Growth Spread",
        category="Forensic",
        exact_formula="inventory_growth - revenue_growth",
        source="src/factors/candidate_factors.py and reports/forensic_quality_research.md",
        interpretation="Inventory growing faster than sales can flag demand weakness or future margin pressure.",
        europe_notes="Relevant to Europe industrials, autos, and retailers; less useful for asset-light sectors.",
        preferred_direction="lower_is_better",
        evidence_status="yuval_research_recurring_factor",
    ),
    PublicFormula(
        formula_id="forensic_fcf_vs_income_v1",
        name="FCF Below Net Income",
        category="Forensic",
        exact_formula="FCF(0, TTM) < NetIncBXor(0, TTM)",
        source="src/short_candidate_generator.py: revision_collapse_poor_quality rule",
        interpretation="Flags reported earnings that exceed free cash flow.",
        europe_notes="Important in Europe small caps where working-capital and capex cycles can make earnings less reliable.",
        preferred_direction="false_is_better_for_longs",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="liquidity_min_price_v1",
        name="Minimum Price Constraint",
        category="Liquidity",
        exact_formula="Close(0) > 5",
        source="src/short_candidate_generator.py: DEFAULT_UNIVERSE_RULES",
        interpretation="Avoids very low-priced shares with higher microstructure and borrow risk.",
        europe_notes="For Europe, currency denomination must be explicit before applying a numeric threshold.",
        preferred_direction="constraint",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="liquidity_avg_daily_total_v1",
        name="Average Daily Dollar Volume Constraint",
        category="Liquidity",
        exact_formula="AvgDailyTot(60) > 2000000",
        source="src/short_candidate_generator.py: DEFAULT_UNIVERSE_RULES",
        interpretation="Requires sufficient 60-day average traded value.",
        europe_notes="Core constraint for Europe small caps; must be converted consistently across currencies.",
        preferred_direction="constraint",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="liquidity_min_market_cap_v1",
        name="Minimum Market Cap Constraint",
        category="Liquidity",
        exact_formula="MktCap > 1000",
        source="src/short_candidate_generator.py: DEFAULT_UNIVERSE_RULES",
        interpretation="Excludes very small issuers where capacity, spread, and borrow risks dominate.",
        europe_notes="Contrasts with the microcap hypothesis; should be varied in dedicated size-decomposition tests.",
        preferred_direction="constraint",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="liquidity_adr_exclusion_v1",
        name="ADR Exclusion",
        category="Liquidity",
        exact_formula="Universe($ADR) = false",
        source="src/short_candidate_generator.py: DEFAULT_UNIVERSE_RULES",
        interpretation="Avoids ADR share-class and liquidity mismatches.",
        europe_notes="Prefer local Europe listings for factor tests unless ADR handling is explicitly modeled.",
        preferred_direction="constraint",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
    PublicFormula(
        formula_id="liquidity_otc_exclusion_v1",
        name="OTC Exclusion",
        category="Liquidity",
        exact_formula="Universe($OTC) = false",
        source="src/short_candidate_generator.py: DEFAULT_UNIVERSE_RULES",
        interpretation="Avoids OTC names where liquidity and data quality are weaker.",
        europe_notes="Important for public Europe screens because OTC mirrors can duplicate local listings and distort capacity.",
        preferred_direction="constraint",
        evidence_status="portfolio123_style_formula_observed_locally",
    ),
)


def public_formula_library() -> list[dict[str, Any]]:
    """Return the research-only public formula reference.

    The library is descriptive metadata only. It does not score securities,
    activate rank inputs, mutate Aegis candidates, or recommend trades.
    """

    return [formula.to_dict() for formula in _FORMULAS]


def formulas_by_category(category: str) -> list[dict[str, Any]]:
    normalized = category.strip().lower()
    return [formula.to_dict() for formula in _FORMULAS if formula.category.lower() == normalized]


def search_formulas(query: str) -> list[dict[str, Any]]:
    terms = [term for term in query.strip().lower().split() if term]
    if not terms:
        return public_formula_library()

    matches: list[dict[str, Any]] = []
    for formula in _FORMULAS:
        haystack = " ".join(
            (
                formula.formula_id,
                formula.name,
                formula.category,
                formula.exact_formula,
                formula.source,
                formula.interpretation,
                formula.europe_notes,
                formula.preferred_direction,
                formula.evidence_status,
            )
        ).lower()
        if all(term in haystack for term in terms):
            matches.append(formula.to_dict())
    return matches


def get_formula(formula_id: str) -> dict[str, Any]:
    normalized = formula_id.strip().lower()
    for formula in _FORMULAS:
        if formula.formula_id == normalized:
            return formula.to_dict()
    valid = ", ".join(formula.formula_id for formula in _FORMULAS)
    raise ValueError(f"Unknown public formula '{formula_id}'. Valid formulas: {valid}")


def formula_library_summary() -> dict[str, Any]:
    categories = sorted({formula.category for formula in _FORMULAS})
    counts = {
        category: len([formula for formula in _FORMULAS if formula.category == category])
        for category in categories
    }
    return {
        "library_id": "public_yuval_formula_library_v1",
        "activation_status": ACTIVATION_STATUS,
        "ranking_activation_allowed": False,
        "formula_count": len(_FORMULAS),
        "category_counts": counts,
        "categories": categories,
    }


def render_markdown(formulas: Iterable[dict[str, Any]] | None = None) -> str:
    rows = list(public_formula_library() if formulas is None else formulas)
    summary = formula_library_summary()
    lines = [
        "# Public Formula Library",
        "",
        "Date: 2026-06-11",
        "",
        "Status: RESEARCH ONLY. This is a searchable reference of public/Yuval-related formulas discovered in local research artifacts so far. It does not change ranking, score securities, create candidates, allocate capital, or override the verified runtime graph.",
        "",
        "## Evidence Boundary",
        "",
        "Exact public Portfolio123 settings for the named Yuval Europe models are still not available in local artifacts. Entries below preserve the formula exactly as captured when an exact Portfolio123-style expression exists, and otherwise use the normalized baseline formula from the local research library.",
        "",
        "## Summary",
        "",
        f"- Formula count: {summary['formula_count']}",
        f"- Activation status: `{summary['activation_status']}`",
        "- Ranking activation allowed: `False`",
        "",
        "| Category | Count |",
        "| --- | ---: |",
    ]
    for category, count in summary["category_counts"].items():
        lines.append(f"| {category} | {count} |")

    for category in summary["categories"]:
        lines.extend(["", f"## {category}", "", "| Formula | Exact formula | Source | Interpretation | Europe-specific notes |"])
        lines.append("| --- | --- | --- | --- | --- |")
        for row in rows:
            if row["category"] != category:
                continue
            lines.append(
                "| {name} | `{formula}` | {source} | {interpretation} | {europe_notes} |".format(
                    name=row["name"],
                    formula=row["exact_formula"].replace("|", "\\|"),
                    source=row["source"],
                    interpretation=row["interpretation"],
                    europe_notes=row["europe_notes"],
                )
            )

    lines.extend(
        [
            "",
            "## Source Index",
            "",
            "- `src/factors/candidate_factors.py`: normalized Yuval-recurring research factor bench.",
            "- `src/short_candidate_generator.py`: exact local Portfolio123-style rules and rank formulas captured for research templates.",
            "- `reports/forensic_quality_research.md`: forensic quality family and accounting-risk interpretation.",
            "- `reports/europe_model_family_tree.md`: evidence boundary for public Yuval Europe model settings.",
            "- `reports/europe_vs_us_factor_behavior.md`: Europe-specific factor evidence and notes.",
            "- `reports/atlas_v2_research_os/manual_fundamental_claims/pegy_ratio/2026-06-06/pegy_ratio_metric_spec.json`: PEGY metric specification.",
        ]
    )
    return "\n".join(lines) + "\n"

