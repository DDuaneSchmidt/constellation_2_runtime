from __future__ import annotations

from typing import Any

CLAIM_NAME = "PEGY ratio"
CLAIM_TYPE = "FUNDAMENTAL_VALUATION"
MECHANISM_FAMILY = "VALUE_GROWTH_YIELD"
CLAIM_DESCRIPTION = (
    "Stocks with attractive PEGY ratios may offer better forward return potential because valuation is adjusted for both "
    "earnings growth and dividend yield."
)
FALSIFIABLE_HYPOTHESIS = (
    "Do low-PEGY stocks outperform comparable high-PEGY stocks after controlling for sector, market cap, quality, and value exposure?"
)

REQUIRED_FIELDS = [
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

OPTIONAL_CONTROLS = [
    "debt/equity",
    "ROE",
    "free cash flow yield",
    "payout ratio",
    "revenue growth",
    "earnings revision trend",
]

INITIAL_HYPOTHESES = [
    {
        "hypothesis_id": "H1",
        "statement": "Low PEGY decile outperforms high PEGY decile over 3/6/12-month forward total return.",
        "artifact_role": "HYPOTHESIS_SPEC_NOT_CANDIDATE",
    },
    {
        "hypothesis_id": "H2",
        "statement": "PEGY adds predictive power beyond PEG alone.",
        "artifact_role": "HYPOTHESIS_SPEC_NOT_CANDIDATE",
    },
    {
        "hypothesis_id": "H3",
        "statement": "PEGY works better in dividend-paying, mature sectors than in high-growth non-dividend sectors.",
        "artifact_role": "HYPOTHESIS_SPEC_NOT_CANDIDATE",
    },
    {
        "hypothesis_id": "H4",
        "statement": "PEGY fails when growth estimates are stale, negative, or highly revised.",
        "artifact_role": "HYPOTHESIS_SPEC_NOT_CANDIDATE",
    },
    {
        "hypothesis_id": "H5",
        "statement": "PEGY effect disappears after controlling for value, quality, and sector.",
        "artifact_role": "HYPOTHESIS_SPEC_NOT_CANDIDATE",
    },
]

RECOMMENDED_PIPELINE = [
    "Manual PEGY claim",
    "Fundamental hypothesis",
    "Data availability audit",
    "Historical cross-sectional test design",
    "Sector/size controlled replay",
    "Holdout validation",
    "Family-level evidence",
]

AUTHORITY_BOUNDARY: dict[str, bool] = {
    "research_only": True,
    "investment_advice_authorized": False,
    "trade_recommendation_authorized": False,
    "capital_allocation_authorized": False,
    "position_sizing_authorized": False,
    "broker_execution_authorized": False,
    "live_trading_authorized": False,
    "automatic_paper_placement_authorized": False,
    "candidate_promotion_authorized": False,
    "production_promotion_authorized": False,
}


def manual_pegy_claim() -> dict[str, Any]:
    return {
        "claim_name": CLAIM_NAME,
        "claim_type": CLAIM_TYPE,
        "mechanism_family": MECHANISM_FAMILY,
        "description": CLAIM_DESCRIPTION,
        "research_framing": FALSIFIABLE_HYPOTHESIS,
        "claim_status": "UNTESTED_HYPOTHESIS",
        "edge_label_allowed": False,
        "candidate_promotion_allowed": False,
    }
