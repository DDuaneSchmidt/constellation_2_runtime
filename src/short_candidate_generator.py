from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable


DEFAULT_UNIVERSE_RULES: tuple[str, ...] = (
    "DRAFT_ONLY = true",
    "Close(0) > 5",
    "AvgDailyTot(60) > 2000000",
    "MktCap > 1000",
    "Universe($ADR) = false",
    "Universe($OTC) = false",
    "Universe($Financials) = false",
)

DEFAULT_BORROW_RISK_NOTES: tuple[str, ...] = (
    "Avoid microcaps by requiring MktCap > 1000 and AvgDailyTot(60) > 2000000.",
    "Avoid obvious borrow nightmares by excluding low-price, low-liquidity, OTC, ADR, and financial-sector edge cases.",
    "Before any non-draft use, add live borrow, fee, hard-to-borrow, corporate-action, and locate checks outside this generator.",
)

TEMPLATE_IDS: tuple[str, ...] = (
    "negative_revisions",
    "negative_fcf",
    "negative_roa",
    "weak_momentum",
    "revision_collapse_poor_quality",
    "accruals_cashflow_mismatch",
    "margin_degradation",
    "expensive_growth_breakdown",
    "levered_earnings_decay",
    "weak_relative_strength_negative_estimates",
)


@dataclass(frozen=True)
class ShortCandidateTemplate:
    template_id: str
    title: str
    status: str
    thesis: str
    portfolio123_rules: tuple[str, ...]
    rank_formula: str
    entry_review_notes: tuple[str, ...]
    borrow_risk_notes: tuple[str, ...] = DEFAULT_BORROW_RISK_NOTES

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


_TEMPLATE_DESIGNS: dict[str, dict[str, object]] = {
    "negative_revisions": {
        "title": "Negative Revisions",
        "thesis": "Short candidates where analyst estimate pressure is broad enough to suggest deteriorating forward fundamentals.",
        "portfolio123_rules": (
            "CurFYEPSMean < CurFYEPSMean4WkAgo",
            "NextFYEPSMean < NextFYEPSMean4WkAgo",
            "CurFYEPSMean < CurFYEPSMean13WkAgo",
            "EPSActual(0, QTR) < EPSEstimate(0, QTR)",
        ),
        "rank_formula": "Lower is worse: FRank(\"CurFYEPSMean / CurFYEPSMean13WkAgo\", #All, #DESC) + FRank(\"NextFYEPSMean / NextFYEPSMean13WkAgo\", #All, #DESC)",
        "entry_review_notes": (
            "Confirm revisions are not a one-time accounting reset.",
            "Prefer names with negative revisions across both current-year and next-year estimates.",
        ),
    },
    "negative_fcf": {
        "title": "Negative FCF",
        "thesis": "Short candidates where reported earnings or valuation support is undermined by negative free cash flow.",
        "portfolio123_rules": (
            "FCF(0, TTM) < 0",
            "OperCashFl(0, TTM) < CapEx(0, TTM)",
            "Sales(0, TTM) > 0",
            "DebtTotQ > CashPSQ * SharesQ",
        ),
        "rank_formula": "Lower is worse: FRank(\"FCF(0, TTM) / Sales(0, TTM)\", #All, #ASC)",
        "entry_review_notes": (
            "Separate temporary working-capital drag from structurally negative cash generation.",
            "Require a clear path from cash burn to balance-sheet or valuation pressure.",
        ),
    },
    "negative_roa": {
        "title": "Negative ROA",
        "thesis": "Short candidates with poor asset productivity and weak profitability despite sufficient liquidity for testing.",
        "portfolio123_rules": (
            "ROA%TTM < 0",
            "ROE%TTM < 0",
            "OpMgn%TTM < 0",
            "Sales(0, TTM) > 0",
        ),
        "rank_formula": "Lower is worse: FRank(\"ROA%TTM\", #All, #ASC) + FRank(\"OpMgn%TTM\", #All, #ASC)",
        "entry_review_notes": (
            "Avoid early-stage biotech and single-event loss cases unless separately governed.",
            "Prefer recurring operating losses over non-cash one-time charges.",
        ),
    },
    "weak_momentum": {
        "title": "Weak Momentum",
        "thesis": "Short candidates where price action confirms deteriorating market perception.",
        "portfolio123_rules": (
            "Close(0) < SMA(50, 0)",
            "SMA(50, 0) < SMA(200, 0)",
            "Pr52W%Chg < 0",
            "RelStrength(26) < 40",
        ),
        "rank_formula": "Lower is worse: FRank(\"RelStrength(26)\", #All, #ASC) + FRank(\"Pr52W%Chg\", #All, #ASC)",
        "entry_review_notes": (
            "Avoid crowded gap-down exhaustion immediately after capitulation events.",
            "Prefer weak momentum confirmed by fundamental deterioration.",
        ),
    },
    "revision_collapse_poor_quality": {
        "title": "Revision Collapse + Poor Quality",
        "thesis": "Short candidates combining estimate cuts with low-quality accounting and poor profitability.",
        "portfolio123_rules": (
            "CurFYEPSMean < CurFYEPSMean13WkAgo * 0.9",
            "ROA%TTM < 3",
            "AccrualRatioTTM > 0.05",
            "FCF(0, TTM) < NetIncBXor(0, TTM)",
        ),
        "rank_formula": "Lower is worse: FRank(\"CurFYEPSMean / CurFYEPSMean13WkAgo\", #All, #ASC) + FRank(\"ROA%TTM\", #All, #ASC)",
        "entry_review_notes": (
            "Use this as a higher-conviction overlay, not a standalone borrow decision.",
            "Check whether estimate collapse has already been fully repriced.",
        ),
    },
    "accruals_cashflow_mismatch": {
        "title": "Accruals / Cash Flow Mismatch",
        "thesis": "Short candidates where accounting earnings quality appears weaker than headline profits imply.",
        "portfolio123_rules": (
            "NetIncBXor(0, TTM) > 0",
            "FCF(0, TTM) < 0",
            "AccrualRatioTTM > 0.08",
            "OperCashFl(0, TTM) < NetIncBXor(0, TTM)",
        ),
        "rank_formula": "Higher is worse: FRank(\"AccrualRatioTTM\", #All, #DESC)",
        "entry_review_notes": (
            "Inspect receivables, inventory, and capitalized-cost drivers before testing.",
            "Avoid penalizing seasonal working-capital timing without persistence.",
        ),
    },
    "margin_degradation": {
        "title": "Margin Degradation",
        "thesis": "Short candidates where operating economics are deteriorating before consensus fully catches down.",
        "portfolio123_rules": (
            "OpMgn%TTM < OpMgn%PYQ",
            "GrossMgn%TTM < GrossMgn%PYQ",
            "Sales(0, TTM) > Sales(4, TTM)",
            "CurFYEPSMean < CurFYEPSMean4WkAgo",
        ),
        "rank_formula": "Lower is worse: FRank(\"OpMgn%TTM - OpMgn%PYQ\", #All, #ASC)",
        "entry_review_notes": (
            "Prefer margin deterioration with continued sales growth, which can reveal poor operating leverage.",
            "Review commodity and FX exposures before assuming structural margin damage.",
        ),
    },
    "expensive_growth_breakdown": {
        "title": "Expensive Growth Breakdown",
        "thesis": "Short candidates with premium valuation, slowing growth, and weakening price confirmation.",
        "portfolio123_rules": (
            "PEExclXorTTM > 35",
            "SalesGr%TTM < SalesGr%PYQ",
            "EPS%ChgTTM < EPS%ChgPYQ",
            "Close(0) < SMA(100, 0)",
        ),
        "rank_formula": "Higher valuation and weaker growth is worse: FRank(\"PEExclXorTTM\", #All, #DESC) + FRank(\"SalesGr%TTM - SalesGr%PYQ\", #All, #ASC)",
        "entry_review_notes": (
            "Avoid shorting high-quality compounders solely on valuation.",
            "Prefer premium multiples where growth deceleration is already visible in revisions or margins.",
        ),
    },
    "levered_earnings_decay": {
        "title": "Levered Earnings Decay",
        "thesis": "Short candidates where debt load and falling earnings may increase refinancing and equity dilution risk.",
        "portfolio123_rules": (
            "DebtTotQ / EBITDA(0, TTM) > 3",
            "IntCovTTM < 3",
            "CurFYEPSMean < CurFYEPSMean13WkAgo",
            "FCF(0, TTM) < 0",
        ),
        "rank_formula": "Worse leverage and coverage rank: FRank(\"DebtTotQ / EBITDA(0, TTM)\", #All, #DESC) + FRank(\"IntCovTTM\", #All, #ASC)",
        "entry_review_notes": (
            "Exclude financials and pass any capital-structure edge case through separate review.",
            "Confirm debt metrics are meaningful for the issuer's sector.",
        ),
    },
    "weak_relative_strength_negative_estimates": {
        "title": "Weak Relative Strength + Negative Estimates",
        "thesis": "Short candidates where weak relative price behavior aligns with negative estimate pressure.",
        "portfolio123_rules": (
            "RelStrength(13) < 35",
            "RelStrength(26) < 45",
            "CurFYEPSMean < CurFYEPSMean4WkAgo",
            "NextFYEPSMean < NextFYEPSMean4WkAgo",
        ),
        "rank_formula": "Lower is worse: FRank(\"RelStrength(13)\", #All, #ASC) + FRank(\"CurFYEPSMean / CurFYEPSMean4WkAgo\", #All, #ASC)",
        "entry_review_notes": (
            "Use as a liquid short watchlist screen, not a signal to trade.",
            "Prefer names with both price and estimate deterioration rather than one isolated weak input.",
        ),
    },
}


def generate_templates(template_ids: Iterable[str] | None = None) -> list[dict[str, object]]:
    """Generate deterministic draft-only Portfolio123 short-side templates.

    This function performs no network calls, no API calls, no Portfolio123 calls,
    no backtests, no candidate promotion, and no trading or borrow lookup.
    """
    ids = TEMPLATE_IDS if template_ids is None else tuple(template_ids)
    return [_template_for_id(template_id).to_dict() for template_id in ids]


def render_markdown(templates: Iterable[dict[str, object]] | None = None) -> str:
    rows = list(generate_templates() if templates is None else templates)
    lines = [
        "# Short Candidate Templates",
        "",
        "Status: DRAFT_ONLY",
        "",
        "These are draft short-side Portfolio123 templates for research review only. They make no API calls, do not query Portfolio123, do not check live borrow, and do not create trade advice, candidates, paper positions, orders, or allocation recommendations.",
        "",
        "Common universe constraints:",
        "",
    ]
    lines.extend(f"- `{rule}`" for rule in DEFAULT_UNIVERSE_RULES)
    lines.extend(
        [
            "",
            "Common borrow-risk notes:",
            "",
        ]
    )
    lines.extend(f"- {note}" for note in DEFAULT_BORROW_RISK_NOTES)

    for index, template in enumerate(rows, 1):
        lines.extend(
            [
                "",
                f"## Template {index}: {template['title']}",
                "",
                f"Status: {template['status']}",
                "",
                f"Template ID: `{template['template_id']}`",
                "",
                f"Thesis: {template['thesis']}",
                "",
                "Portfolio123 draft rules:",
                "",
            ]
        )
        lines.extend(f"- `{rule}`" for rule in DEFAULT_UNIVERSE_RULES)
        lines.extend(f"- `{rule}`" for rule in template["portfolio123_rules"])
        lines.extend(
            [
                "",
                f"Rank formula draft: `{template['rank_formula']}`",
                "",
                "Entry review notes:",
                "",
            ]
        )
        lines.extend(f"- {note}" for note in template["entry_review_notes"])

    return "\n".join(lines) + "\n"


def generate_by_template_id(template_id: str) -> list[dict[str, object]]:
    return [_template_for_id(template_id).to_dict()]


def _template_for_id(template_id: str) -> ShortCandidateTemplate:
    normalized_id = template_id.strip().lower()
    if normalized_id not in _TEMPLATE_DESIGNS:
        valid = ", ".join(TEMPLATE_IDS)
        raise ValueError(f"Unknown short template '{template_id}'. Valid templates: {valid}")

    design = _TEMPLATE_DESIGNS[normalized_id]
    return ShortCandidateTemplate(
        template_id=f"short_{normalized_id}_v1",
        title=str(design["title"]),
        status="DRAFT_ONLY",
        thesis=str(design["thesis"]),
        portfolio123_rules=tuple(str(rule) for rule in design["portfolio123_rules"]),
        rank_formula=str(design["rank_formula"]),
        entry_review_notes=tuple(str(note) for note in design["entry_review_notes"]),
    )


if __name__ == "__main__":
    print(render_markdown(), end="")
