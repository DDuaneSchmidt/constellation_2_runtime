from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "HISTORICAL_SHORT_RESEARCH_REVIEW.md"


@dataclass(frozen=True)
class ShortTheme:
    theme_id: str
    name: str
    frequency_rank: int
    why_it_worked: str
    observable_signals: tuple[str, ...]
    failure_modes: tuple[str, ...]
    aegis_candidate_idea: str
    expected_best_use: str
    research_priority: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class FailedShortTheme:
    theme_id: str
    name: str
    why_it_failed: str
    warning_signs: tuple[str, ...]
    aegis_guardrail: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


SUCCESSFUL_SHORT_THEMES: tuple[ShortTheme, ...] = (
    ShortTheme(
        theme_id="short_theme_earnings_revision_collapse_v1",
        name="Collapsing earnings revisions",
        frequency_rank=1,
        why_it_worked=(
            "Successful shorts often had a transition from optimism to repeated estimate cuts. "
            "The edge was strongest when cuts broadened across revenue, margin, and cash-flow expectations."
        ),
        observable_signals=(
            "negative one- and three-month EPS revision breadth",
            "guidance cuts or withdrawn guidance",
            "sell-side downgrade clusters after prior optimism",
            "negative surprise followed by further estimate compression",
        ),
        failure_modes=(
            "single downgrade without follow-through",
            "cyclical trough already embedded in consensus",
            "company has credible backlog, pricing, or cost offset evidence",
        ),
        aegis_candidate_idea=(
            "Build a revision-collapse short watchlist that requires multi-horizon negative revisions, "
            "post-earnings estimate drift, and no offsetting balance-sheet strength."
        ),
        expected_best_use="primary short trigger and timing filter",
        research_priority="HIGH",
    ),
    ShortTheme(
        theme_id="short_theme_accounting_weakness_v1",
        name="Accounting weakness",
        frequency_rank=2,
        why_it_worked=(
            "Accounting quality failures created delayed repricing when reported earnings were not backed by cash, "
            "working capital quality, or conservative recognition."
        ),
        observable_signals=(
            "rising accruals relative to assets or earnings",
            "cash flow lagging net income",
            "receivables or inventory growing faster than sales",
            "non-recurring adjustments becoming recurring",
        ),
        failure_modes=(
            "high accruals explained by temporary growth investment",
            "seasonal working-capital build misclassified as deterioration",
            "short signal appears after public accounting controversy is fully priced",
        ),
        aegis_candidate_idea=(
            "Create an accounting-fragility score that combines accruals, cash-conversion gaps, "
            "working-capital pressure, and adjustment persistence."
        ),
        expected_best_use="quality veto and fraud-risk proxy",
        research_priority="HIGH",
    ),
    ShortTheme(
        theme_id="short_theme_deteriorating_profitability_v1",
        name="Deteriorating profitability",
        frequency_rank=3,
        why_it_worked=(
            "Durable short candidates usually showed worsening unit economics before the stock fully reflected "
            "the lower earnings power."
        ),
        observable_signals=(
            "gross margin compression",
            "operating margin compression",
            "ROIC or ROA deterioration",
            "negative operating leverage despite revenue growth",
        ),
        failure_modes=(
            "temporary margin compression from planned investment",
            "cost inflation already reversing",
            "mix shift looks bad in aggregate but improves lifetime economics",
        ),
        aegis_candidate_idea=(
            "Test a profitability-breakdown screen requiring margin deterioration plus weak revisions "
            "or weak cash conversion."
        ),
        expected_best_use="fundamental confirmation layer",
        research_priority="HIGH",
    ),
    ShortTheme(
        theme_id="short_theme_broken_momentum_v1",
        name="Broken momentum",
        frequency_rank=4,
        why_it_worked=(
            "Shorts worked better when fundamental weakness coincided with failed price leadership, "
            "trend breaks, or repeated inability to recover after bad news."
        ),
        observable_signals=(
            "price below medium-term moving averages",
            "relative strength breakdown versus sector",
            "failed post-earnings rebound",
            "high-volume downside gaps",
        ),
        failure_modes=(
            "mean-reversion short after an already crowded collapse",
            "price weakness caused by broad market beta rather than company-specific failure",
            "momentum break without confirming estimate or profitability damage",
        ),
        aegis_candidate_idea=(
            "Use broken momentum as a timing filter only after revision, accounting, or profitability evidence exists."
        ),
        expected_best_use="entry timing and risk control",
        research_priority="MEDIUM",
    ),
    ShortTheme(
        theme_id="short_theme_excessive_valuation_v1",
        name="Excessive valuation",
        frequency_rank=5,
        why_it_worked=(
            "Valuation mattered most when it amplified a fundamental disappointment. Expensive stocks with falling "
            "growth expectations had more room for multiple compression."
        ),
        observable_signals=(
            "premium sales or earnings multiple versus history and peers",
            "valuation unsupported by forward margin trajectory",
            "long-duration cash-flow profile during discount-rate pressure",
            "narrative valuation dependent on distant profitability",
        ),
        failure_modes=(
            "expensive compounder with improving revisions",
            "valuation-only short in a liquidity-driven risk-on tape",
            "high multiple justified by durable growth, pricing power, or scarcity",
        ),
        aegis_candidate_idea=(
            "Treat excessive valuation as a severity multiplier, not a standalone short signal."
        ),
        expected_best_use="position sizing and downside convexity estimate",
        research_priority="MEDIUM",
    ),
)


FAILED_SHORT_THEMES: tuple[FailedShortTheme, ...] = (
    FailedShortTheme(
        theme_id="failed_short_theme_valuation_only_v1",
        name="Valuation-only shorts",
        why_it_failed=(
            "High valuation alone often failed because expensive stocks can keep compounding or re-rate higher "
            "while fundamentals remain intact."
        ),
        warning_signs=(
            "positive revisions",
            "stable or expanding margins",
            "strong cash conversion",
            "sector leadership remains intact",
        ),
        aegis_guardrail="Require a non-valuation failure signal before any short candidate can pass research review.",
    ),
    FailedShortTheme(
        theme_id="failed_short_theme_crowded_consensus_v1",
        name="Crowded consensus shorts",
        why_it_failed=(
            "Widely known bearish stories often failed when borrow, positioning, and squeeze risk dominated "
            "incremental fundamental information."
        ),
        warning_signs=(
            "very high short interest",
            "high borrow cost",
            "positive catalyst risk",
            "stock rallies on bad news",
        ),
        aegis_guardrail="Add crowding and squeeze-risk checks before promoting any short theme to paper observation.",
    ),
    FailedShortTheme(
        theme_id="failed_short_theme_low_quality_without_timing_v1",
        name="Low-quality companies without timing evidence",
        why_it_failed=(
            "Weak businesses could survive for long periods when liquidity, narratives, or capital markets access "
            "delayed the recognition event."
        ),
        warning_signs=(
            "no near-term catalyst",
            "improving price momentum",
            "available refinancing path",
            "management still able to raise capital",
        ),
        aegis_guardrail="Require timing evidence from revisions, price action, or financing stress before ranking.",
    ),
    FailedShortTheme(
        theme_id="failed_short_theme_macro_beta_masked_as_short_v1",
        name="Macro beta mistaken for company failure",
        why_it_failed=(
            "Some apparent shorts were just high-beta exposure. They reversed sharply when the macro tape improved."
        ),
        warning_signs=(
            "weakness shared by entire industry",
            "company fundamentals better than peers",
            "reversal follows rates, oil, or index beta",
            "no idiosyncratic estimate damage",
        ),
        aegis_guardrail="Separate sector and factor beta from company-specific deterioration before scoring.",
    ),
)


LOCAL_ASSUMPTION_REVIEW: tuple[str, ...] = (
    "Existing local factor assumptions already include earnings revisions, earnings quality, margin stability, "
    "capital efficiency, and financial strength as long-side families; the short lab should invert these only when "
    "deterioration is observable and current.",
    "The strongest short setup is multi-signal deterioration, not a single-factor rank. Revisions, accounting quality, "
    "profitability, momentum, and valuation should be combined as independent evidence layers.",
    "Runtime truth currently blocks trade advice and execution; this review is a static research synthesis and does "
    "not validate, promote, or recommend trades.",
    "No API calls, web calls, broker calls, Portfolio123 calls, or backtests are required for this review artifact.",
)


def successful_short_themes() -> list[dict[str, object]]:
    return [theme.to_dict() for theme in sorted(SUCCESSFUL_SHORT_THEMES, key=lambda row: row.frequency_rank)]


def failed_short_themes() -> list[dict[str, object]]:
    return [theme.to_dict() for theme in FAILED_SHORT_THEMES]


def candidate_ideas_for_aegis(themes: Iterable[ShortTheme] = SUCCESSFUL_SHORT_THEMES) -> list[dict[str, str]]:
    ideas: list[dict[str, str]] = []
    for theme in sorted(themes, key=lambda row: row.frequency_rank):
        ideas.append(
            {
                "candidate_id": theme.theme_id.replace("short_theme_", "aegis_short_candidate_"),
                "source_theme": theme.name,
                "idea": theme.aegis_candidate_idea,
                "best_use": theme.expected_best_use,
                "priority": theme.research_priority,
            }
        )
    return ideas


def build_historical_short_research_review_markdown() -> tuple[str, dict[str, int]]:
    themes = sorted(SUCCESSFUL_SHORT_THEMES, key=lambda row: row.frequency_rank)
    failed = list(FAILED_SHORT_THEMES)
    ideas = candidate_ideas_for_aegis(themes)

    lines: list[str] = [
        "# Historical Short Research Review",
        "",
        "## Scope",
        "",
        "This is a deterministic local research review for Aegis Short Lab. It reviews assumptions about "
        "successful short strategies and generates candidate research ideas. It does not call APIs, fetch data, "
        "run a backtest, create candidates, promote candidates, provide trade advice, or change runtime readiness.",
        "",
        "## Local Research Assumption Review",
        "",
    ]
    lines.extend(f"- {item}" for item in LOCAL_ASSUMPTION_REVIEW)
    lines.extend(
        [
            "",
            "## Most Common Successful Short Themes",
            "",
            "| Rank | Theme | Why It Worked | Observable Signals | Aegis Candidate Idea | Best Use | Priority |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for theme in themes:
        lines.append(
            f"| {theme.frequency_rank} | {theme.name} | {theme.why_it_worked} | "
            f"{'; '.join(theme.observable_signals)} | {theme.aegis_candidate_idea} | "
            f"{theme.expected_best_use} | {theme.research_priority} |"
        )

    lines.extend(
        [
            "",
            "## Themes Likely To Fail",
            "",
            "| Theme | Why It Failed | Warning Signs | Aegis Guardrail |",
            "| --- | --- | --- | --- |",
        ]
    )
    for theme in failed:
        lines.append(
            f"| {theme.name} | {theme.why_it_failed} | {'; '.join(theme.warning_signs)} | "
            f"{theme.aegis_guardrail} |"
        )

    lines.extend(
        [
            "",
            "## Candidate Ideas For Aegis",
            "",
            "| Candidate ID | Source Theme | Idea | Best Use | Priority |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for idea in ideas:
        lines.append(
            f"| {idea['candidate_id']} | {idea['source_theme']} | {idea['idea']} | "
            f"{idea['best_use']} | {idea['priority']} |"
        )

    lines.extend(
        [
            "",
            "## Research Design Implications",
            "",
            "- Short candidates should require at least two independent deterioration signals before ranking.",
            "- Valuation should increase severity only after earnings, accounting, profitability, or momentum evidence exists.",
            "- Broken momentum is useful for timing, but weak as a standalone research thesis.",
            "- Accounting and cash-conversion weakness deserve early warning status because recognition can lag reported earnings.",
            "- Every future implementation should preserve the runtime truth invariant: consumers query verified truth and do not invent readiness.",
            "",
        ]
    )

    summary = {
        "successful_theme_count": len(themes),
        "failed_theme_count": len(failed),
        "candidate_idea_count": len(ideas),
        "local_assumption_count": len(LOCAL_ASSUMPTION_REVIEW),
    }
    return "\n".join(lines), summary


def write_historical_short_research_review(output_path: Path = DEFAULT_OUTPUT) -> dict[str, int | str]:
    markdown, summary = build_historical_short_research_review_markdown()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    return {"path": str(output_path), **summary}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the local historical short research review.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    result = write_historical_short_research_review(args.output)
    print(result)


if __name__ == "__main__":
    main()
