from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "SHORT_LAB_GO_NO_GO.md"


class DecisionOutcome(str, Enum):
    GO = "GO"
    GO_LATER = "GO_LATER"
    NO_GO = "NO_GO"


@dataclass(frozen=True)
class DecisionCriterion:
    name: str
    score: float
    weight: float
    rationale: str

    @property
    def weighted_score(self) -> float:
        return _clamp(self.score) * self.weight


@dataclass(frozen=True)
class ShortLabDecision:
    outcome: DecisionOutcome
    recommendation: str
    confidence: str
    exact_next_action: str
    estimated_probability_of_success: float
    weighted_score: float
    criteria: tuple[DecisionCriterion, ...]
    inputs: tuple[str, ...]
    authority_boundary: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["outcome"] = self.outcome.value
        return payload


DEFAULT_INPUTS = (
    "Short Edge Landscape",
    "Template Generator",
    "Hedge Investigation",
    "Research Review",
)


def default_short_lab_criteria() -> tuple[DecisionCriterion, ...]:
    return (
        DecisionCriterion(
            name="expected_value",
            score=0.58,
            weight=0.35,
            rationale=(
                "Short-side research has potential diversification value and can expose failure modes "
                "that long-only UltraSafe work may miss, but standalone short alpha is usually diluted "
                "by borrow, rebate, squeeze, and market-drift costs."
            ),
        ),
        DecisionCriterion(
            name="implementation_complexity",
            score=0.35,
            weight=0.25,
            rationale=(
                "Implementation is materially harder than a long-only screen because it needs borrow/cost "
                "assumptions, short-sale constraints, asymmetrical loss controls, and hedge interaction checks."
            ),
        ),
        DecisionCriterion(
            name="probability_of_finding_usable_edge",
            score=0.32,
            weight=0.25,
            rationale=(
                "The plausible edge rate is low to medium before paid testing: short templates can generate "
                "ideas, but usable edges need robust cost-adjusted persistence and anti-crowding evidence."
            ),
        ),
        DecisionCriterion(
            name="opportunity_cost_versus_ultrasafe",
            score=0.40,
            weight=0.15,
            rationale=(
                "The opportunity cost is high while UltraSafe complement research still has unresolved "
                "data and comparability gaps that are closer to the current validated workflow."
            ),
        ),
    )


def decide_short_lab_go_no_go(
    criteria: Iterable[DecisionCriterion] | None = None,
    *,
    inputs: Iterable[str] = DEFAULT_INPUTS,
) -> ShortLabDecision:
    rows = tuple(criteria or default_short_lab_criteria())
    if not rows:
        raise ValueError("criteria must not be empty")

    total_weight = sum(row.weight for row in rows)
    if total_weight <= 0:
        raise ValueError("criteria weights must sum to a positive value")

    weighted_score = sum(row.weighted_score for row in rows) / total_weight
    probability = _clamp(next((row.score for row in rows if row.name == "probability_of_finding_usable_edge"), weighted_score))
    complexity = _clamp(next((row.score for row in rows if row.name == "implementation_complexity"), 0.5))
    opportunity = _clamp(next((row.score for row in rows if row.name == "opportunity_cost_versus_ultrasafe"), 0.5))

    if weighted_score >= 0.70 and probability >= 0.55 and complexity >= 0.50:
        outcome = DecisionOutcome.GO
    elif weighted_score < 0.38 or probability < 0.20:
        outcome = DecisionOutcome.NO_GO
    else:
        outcome = DecisionOutcome.GO_LATER

    if outcome is DecisionOutcome.GO:
        recommendation = "Spend a small, capped Portfolio123 credit budget on the best short-side candidates."
        exact_next_action = "Select at most two short templates for paid Portfolio123 validation with explicit borrow, cost, and squeeze-risk assumptions."
        confidence = "MEDIUM"
    elif outcome is DecisionOutcome.NO_GO:
        recommendation = "Do not spend Portfolio123 credits on short-side research."
        exact_next_action = "Archive the short lab decision and continue UltraSafe complement research."
        confidence = "MEDIUM"
    else:
        recommendation = "Do not spend Portfolio123 credits yet; keep short-side research in a no-cost pre-screen."
        exact_next_action = (
            "Use the Template Generator to create 5 short-side hypotheses, score them with the existing "
            "credit-funnel style screen, and release Portfolio123 credits only if at least 2 candidates "
            "clear a 0.70 pre-testing score with explicit cost and hedge assumptions."
        )
        confidence = "MEDIUM"

    if outcome is DecisionOutcome.GO_LATER and (complexity < 0.40 or opportunity < 0.45):
        confidence = "MEDIUM_HIGH"

    return ShortLabDecision(
        outcome=outcome,
        recommendation=recommendation,
        confidence=confidence,
        exact_next_action=exact_next_action,
        estimated_probability_of_success=probability,
        weighted_score=weighted_score,
        criteria=rows,
        inputs=tuple(inputs),
        authority_boundary=(
            "Research-only Portfolio123 credit decision. No API calls, trade advice, paper readiness, "
            "broker execution, position sizing, capital allocation, or runtime readiness authority."
        ),
    )


def build_short_lab_go_no_go_markdown(decision: ShortLabDecision | None = None) -> tuple[str, dict[str, object]]:
    result = decision or decide_short_lab_go_no_go()
    lines = [
        "# Short Lab Go / No-Go\n\n",
        "## Recommendation\n\n",
        f"- Decision: `{result.outcome.value}`\n",
        f"- Recommendation: {result.recommendation}\n",
        f"- Confidence: {result.confidence}\n",
        f"- Exact next action: {result.exact_next_action}\n",
        f"- Estimated probability of success: {result.estimated_probability_of_success:.0%}\n",
        f"- Weighted decision score: {result.weighted_score:.3f}\n\n",
        "## Inputs Reviewed\n\n",
    ]
    lines.extend(f"- {name}\n" for name in result.inputs)
    lines.extend(
        [
            "\n## Criteria\n\n",
            "| Criterion | Score | Weight | Rationale |\n",
            "|---|---:|---:|---|\n",
        ]
    )
    for row in result.criteria:
        lines.append(f"| {row.name} | {row.score:.2f} | {row.weight:.2f} | {row.rationale} |\n")
    lines.extend(
        [
            "\n## Decision Logic\n\n",
            "- `GO`: weighted score at or above 0.70, usable-edge probability at or above 0.55, and implementation complexity not below 0.50.\n",
            "- `GO_LATER`: positive enough to preserve, but not strong enough for paid credits now.\n",
            "- `NO_GO`: weighted score below 0.38 or usable-edge probability below 0.20.\n\n",
            "## Authority Boundary\n\n",
            f"{result.authority_boundary}\n",
        ]
    )
    return "".join(lines), result.to_dict()


def write_short_lab_go_no_go(output: Path = DEFAULT_OUTPUT) -> dict[str, object]:
    markdown, summary = build_short_lab_go_no_go_markdown()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markdown, encoding="utf-8")
    summary["output"] = str(output)
    return summary


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the short-side Portfolio123 credit go/no-go decision.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    summary = write_short_lab_go_no_go(Path(args.output).resolve())
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
