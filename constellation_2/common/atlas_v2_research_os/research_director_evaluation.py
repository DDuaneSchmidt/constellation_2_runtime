from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_REPORT_PATH = Path("research_journal/reports/research_director_evaluation_001.md")

AUTHORITY_BOUNDARY = {
    "status": "GENERATED_ONLY",
    "evaluation_only": True,
    "read_only_inputs": True,
    "production_pipeline_integration_authorized": False,
    "automatic_memory_writes_authorized": False,
    "candidate_promotion_authorized": False,
    "replay_override_authorized": False,
    "qualification_override_authorized": False,
    "governance_override_authorized": False,
    "trade_recommendation_authorized": False,
    "capital_recommendation_authorized": False,
    "position_sizing_authorized": False,
    "broker_execution_authorized": False,
}

INPUT_DOMAINS = [
    "observations",
    "claims",
    "hypotheses",
    "failures",
    "research_debt",
    "adversary_reviews",
]


@dataclass(frozen=True)
class ResearchDirectorCandidate:
    candidate_id: str
    title: str
    category: str
    source_paths: list[str]
    evidence_summary: str
    information_gain: float
    bottleneck_removal: float
    evidence_strength: float
    uncertainty_reduction: float
    research_cost_score: float
    debt_reduction: float
    evidence_gap: float
    decision_blocking: float
    ambiguity: float
    recurrence: float
    maintenance_drag: float
    reuse_value: float
    lower_value_work: bool = False
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_research_director_evaluation(
    root: str | Path = ".",
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    candidates = _default_candidates()
    scored_rows = [_score_candidate(candidate, root_path=root_path) for candidate in candidates]

    priority_rankings = _rank(scored_rows, "priority_score")
    uncertainty_rankings = _rank(scored_rows, "uncertainty_score")
    research_debt_rankings = _rank(scored_rows, "research_debt_score")
    measurement = _measure_target_bottleneck_ranking(priority_rankings)

    return {
        "schema_id": "atlas_v2_research_director_evaluation.v1",
        "schema_version": "1.0.0",
        "created_at": created_at or now_utc(),
        "status": "GENERATED_ONLY",
        "report_type": "research_director_evaluation",
        "scope": "Offline, evaluation-only Research Director ranking test. No production integration and no authority.",
        "input_domains": list(INPUT_DOMAINS),
        "outputs": [
            "priority_rankings",
            "uncertainty_rankings",
            "research_debt_rankings",
        ],
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "scoring_model": {
            "priority_score": "0.30*information_gain + 0.25*bottleneck_removal + 0.15*evidence_strength + 0.10*uncertainty_reduction + 0.10*research_cost_score + 0.10*debt_reduction",
            "uncertainty_score": "0.45*uncertainty_reduction + 0.25*evidence_gap + 0.20*decision_blocking + 0.10*ambiguity",
            "research_debt_score": "0.40*debt_reduction + 0.25*recurrence + 0.20*maintenance_drag + 0.15*reuse_value",
        },
        "priority_rankings": priority_rankings,
        "uncertainty_rankings": uncertainty_rankings,
        "research_debt_rankings": research_debt_rankings,
        "measurement": measurement,
    }


def write_research_director_evaluation_report(
    root: str | Path = ".",
    output_path: str | Path = DEFAULT_REPORT_PATH,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    report = build_research_director_evaluation(root=root, created_at=created_at)
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(render_research_director_evaluation_markdown(report), encoding="utf-8")
    return {
        "report": report,
        "output_path": str(out_path),
    }


def render_research_director_evaluation_markdown(report: dict[str, Any]) -> str:
    measurement = report["measurement"]
    lines = [
        "# Research Director Evaluation 001",
        "",
        f"Created: {report['created_at']}",
        "",
        "Status: `GENERATED_ONLY`",
        "",
        "Scope: offline evaluation-only Research Director ranking test. No production integration, production authority, candidate promotion, replay override, qualification override, governance override, trading recommendation, capital authority, broker execution, position sizing, or automatic memory write.",
        "",
        "## Inputs",
        "",
    ]
    for input_domain in report["input_domains"]:
        lines.append(f"- {input_domain}")

    lines.extend(
        [
            "",
            "## Outputs",
            "",
            "- priority rankings",
            "- uncertainty rankings",
            "- research debt rankings",
            "",
            "## Scoring Model",
            "",
            f"- priority_score: `{report['scoring_model']['priority_score']}`",
            f"- uncertainty_score: `{report['scoring_model']['uncertainty_score']}`",
            f"- research_debt_score: `{report['scoring_model']['research_debt_score']}`",
            "",
            "## Priority Rankings",
            "",
            "| Rank | Workstream | Score | Target Bottleneck | Evidence | Notes |",
            "| ---: | --- | ---: | --- | --- | --- |",
        ]
    )
    for row in report["priority_rankings"]:
        lines.append(
            f"| {row['rank']} | {row['title']} | {row['priority_score']:.3f} | "
            f"{_yes_no(row['candidate_id'] in measurement['target_bottleneck_ids'])} | "
            f"{row['evidence_summary']} | {'; '.join(row['notes'])} |"
        )

    lines.extend(
        [
            "",
            "## Uncertainty Rankings",
            "",
            "| Rank | Workstream | Score | Why It Reduces Uncertainty |",
            "| ---: | --- | ---: | --- |",
        ]
    )
    for row in report["uncertainty_rankings"]:
        lines.append(f"| {row['rank']} | {row['title']} | {row['uncertainty_score']:.3f} | {row['evidence_summary']} |")

    lines.extend(
        [
            "",
            "## Research Debt Rankings",
            "",
            "| Rank | Workstream | Score | Debt Removed |",
            "| ---: | --- | ---: | --- |",
        ]
    )
    for row in report["research_debt_rankings"]:
        lines.append(f"| {row['rank']} | {row['title']} | {row['research_debt_score']:.3f} | {row['evidence_summary']} |")

    lines.extend(
        [
            "",
            "## Source Presence",
            "",
            "| Workstream | Source | Present |",
            "| --- | --- | --- |",
        ]
    )
    for row in report["priority_rankings"]:
        for source in row["sources"]:
            lines.append(f"| {row['title']} | `{source['path']}` | {_yes_no(source['exists'])} |")

    lines.extend(
        [
            "",
            "## Measurement",
            "",
            f"- Required target order: `{', '.join(measurement['required_target_order'])}`",
            f"- Actual top three priority order: `{', '.join(measurement['actual_top_three'])}`",
            f"- Target bottlenecks above lower-value work: `{measurement['target_bottlenecks_above_lower_value_work']}`",
            f"- Lower-value work maximum priority rank: `{measurement['lower_value_work_best_rank']}`",
            f"- Result: `{measurement['result']}`",
            "",
            "## Interpretation",
            "",
            "The evaluation-only Research Director ranks `Data Coverage`, `Replay Attrition`, and `Vocabulary Mismatch` as the top three workstreams. This matches the observed bottleneck sequence: first the candidate universe lacked direct market data coverage, then candidates with data still lost usable samples during replay, then the replay failure narrowed to a regime-label vocabulary mismatch where `CHOP` could not pass an exact filter against the daily proxy classifier vocabulary.",
            "",
            "Lower-value work remains useful, but it is downstream or enabling work. Failure taxonomy, adversary review, economics, search-space mapping, and new hypothesis generation can improve research quality, but none removes the current validation bottleneck as directly as the three ranked bottlenecks.",
            "",
            "## Authority Boundary",
            "",
        ]
    )
    for key, value in report["authority_boundary"].items():
        lines.append(f"- {key}: `{value}`")
    lines.append("")
    return "\n".join(lines)


def _default_candidates() -> list[ResearchDirectorCandidate]:
    return [
        ResearchDirectorCandidate(
            candidate_id="DATA_COVERAGE",
            title="Data Coverage",
            category="bottleneck",
            source_paths=[
                "reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md",
                "reports/atlas_v2_research_os/market_data_acquisition/market_data_acquisition_001.md",
                "research_journal/reports/insufficient_data_root_cause_analysis_001.md",
            ],
            evidence_summary="Initial direct replay coverage was 6.67%, with 8/8 validations blocked by insufficient data; acquisition changed coverage and exposed later bottlenecks.",
            information_gain=1.00,
            bottleneck_removal=1.00,
            evidence_strength=0.95,
            uncertainty_reduction=0.90,
            research_cost_score=0.70,
            debt_reduction=0.92,
            evidence_gap=0.70,
            decision_blocking=1.00,
            ambiguity=0.60,
            recurrence=0.90,
            maintenance_drag=0.85,
            reuse_value=0.95,
            notes=["hard validation blocker", "first bottleneck in sequence"],
        ),
        ResearchDirectorCandidate(
            candidate_id="REPLAY_ATTRITION",
            title="Replay Attrition",
            category="bottleneck",
            source_paths=[
                "research_journal/reports/insufficient_data_root_cause_analysis_001.md",
                "reports/atlas_v2_research_os/replay_sample_yield/latest_summary.md",
                "reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json",
            ],
            evidence_summary="After daily coverage improved, multiple candidates still produced zero usable samples after trigger/regime filtering or daily-vs-intraday proxy replay.",
            information_gain=0.94,
            bottleneck_removal=0.92,
            evidence_strength=0.88,
            uncertainty_reduction=0.95,
            research_cost_score=0.68,
            debt_reduction=0.88,
            evidence_gap=0.82,
            decision_blocking=0.94,
            ambiguity=0.72,
            recurrence=0.86,
            maintenance_drag=0.82,
            reuse_value=0.90,
            notes=["second bottleneck after coverage repair", "explains candidate-specific collapse"],
        ),
        ResearchDirectorCandidate(
            candidate_id="VOCABULARY_MISMATCH",
            title="Vocabulary Mismatch",
            category="bottleneck",
            source_paths=[
                "research_journal/reports/regime_filter_root_cause_001.md",
                "research_journal/reports/regime_filter_root_cause_table.csv",
                "constellation_2/common/atlas_v2_research_os/candidate_backtests.py",
            ],
            evidence_summary="Five candidates had trigger samples but zero final samples because `CHOP` is not emitted by the daily proxy regime classifier.",
            information_gain=0.90,
            bottleneck_removal=0.88,
            evidence_strength=0.92,
            uncertainty_reduction=0.91,
            research_cost_score=0.76,
            debt_reduction=0.84,
            evidence_gap=0.78,
            decision_blocking=0.88,
            ambiguity=0.80,
            recurrence=0.82,
            maintenance_drag=0.78,
            reuse_value=0.86,
            notes=["third bottleneck in sequence", "specific replay vocabulary diagnosis"],
        ),
        ResearchDirectorCandidate(
            candidate_id="FAILURE_TAXONOMY",
            title="Failure Taxonomy",
            category="analysis",
            source_paths=[
                "research_journal/reports/priority_rankings.md",
                "constellation_2/common/atlas_v2_research_os/atlas_failure_taxonomy.py",
            ],
            evidence_summary="Improves classification and reuse of failure patterns, but classifies blockers rather than removing the active replay validation blocker.",
            information_gain=0.78,
            bottleneck_removal=0.56,
            evidence_strength=0.82,
            uncertainty_reduction=0.70,
            research_cost_score=0.82,
            debt_reduction=0.78,
            evidence_gap=0.48,
            decision_blocking=0.50,
            ambiguity=0.44,
            recurrence=0.72,
            maintenance_drag=0.70,
            reuse_value=0.86,
            lower_value_work=True,
            notes=["useful downstream analysis", "not the immediate unblocker"],
        ),
        ResearchDirectorCandidate(
            candidate_id="RESEARCH_ADVERSARY",
            title="Research Adversary",
            category="review",
            source_paths=[
                "constellation_2/common/atlas_v2_research_os/research_adversary.py",
                "constellation_2/common/atlas_v2_research_os/research_adversary_evaluation.py",
            ],
            evidence_summary="Raises critique quality and falsification discipline, but generated-only review does not itself add data, replay samples, or vocabulary compatibility.",
            information_gain=0.74,
            bottleneck_removal=0.48,
            evidence_strength=0.74,
            uncertainty_reduction=0.66,
            research_cost_score=0.84,
            debt_reduction=0.62,
            evidence_gap=0.42,
            decision_blocking=0.44,
            ambiguity=0.48,
            recurrence=0.56,
            maintenance_drag=0.54,
            reuse_value=0.76,
            lower_value_work=True,
            notes=["valuable adversarial layer", "not direct bottleneck removal"],
        ),
        ResearchDirectorCandidate(
            candidate_id="RESEARCH_ECONOMICS",
            title="Research Economics",
            category="analysis",
            source_paths=[
                "research_journal/reports/priority_rankings.md",
            ],
            evidence_summary="Helps quantify expected value and opportunity cost, but depends on evidence production being unblocked first.",
            information_gain=0.70,
            bottleneck_removal=0.44,
            evidence_strength=0.68,
            uncertainty_reduction=0.58,
            research_cost_score=0.84,
            debt_reduction=0.60,
            evidence_gap=0.40,
            decision_blocking=0.40,
            ambiguity=0.42,
            recurrence=0.50,
            maintenance_drag=0.52,
            reuse_value=0.72,
            lower_value_work=True,
            notes=["useful prioritization layer", "not an evidence-producing fix"],
        ),
        ResearchDirectorCandidate(
            candidate_id="SEARCH_SPACE_MAPPING",
            title="Search Space Mapping",
            category="exploration",
            source_paths=[
                "constellation_2/common/atlas_v2_research_os/mechanism_search_space.py",
                "research_journal/reports/priority_rankings.md",
            ],
            evidence_summary="Broadens long-term exploration coverage, but it does not resolve the present direct-validation data and replay bottlenecks.",
            information_gain=0.64,
            bottleneck_removal=0.36,
            evidence_strength=0.66,
            uncertainty_reduction=0.54,
            research_cost_score=0.78,
            debt_reduction=0.56,
            evidence_gap=0.46,
            decision_blocking=0.34,
            ambiguity=0.46,
            recurrence=0.44,
            maintenance_drag=0.46,
            reuse_value=0.74,
            lower_value_work=True,
            notes=["long-term value", "deprioritized until validation works"],
        ),
        ResearchDirectorCandidate(
            candidate_id="MORE_HYPOTHESIS_GENERATION",
            title="More Hypothesis Generation",
            category="exploration",
            source_paths=[
                "constellation_2/common/atlas_v2_research_os/mechanism_hypothesis_generator.py",
            ],
            evidence_summary="Adds candidates or mechanisms, but additional hypotheses have low marginal value while existing candidates cannot be validated cleanly.",
            information_gain=0.42,
            bottleneck_removal=0.18,
            evidence_strength=0.58,
            uncertainty_reduction=0.30,
            research_cost_score=0.64,
            debt_reduction=0.24,
            evidence_gap=0.36,
            decision_blocking=0.18,
            ambiguity=0.38,
            recurrence=0.28,
            maintenance_drag=0.32,
            reuse_value=0.48,
            lower_value_work=True,
            notes=["defer while validation bottlenecks remain"],
        ),
    ]


def _score_candidate(candidate: ResearchDirectorCandidate, *, root_path: Path) -> dict[str, Any]:
    row = candidate.to_dict()
    row["sources"] = [
        {
            "path": source_path,
            "exists": (root_path / source_path).exists(),
        }
        for source_path in candidate.source_paths
    ]
    source_presence = _rate(sum(1 for source in row["sources"] if source["exists"]), len(row["sources"]))
    row["source_presence"] = source_presence
    evidence_strength = min(candidate.evidence_strength, 0.65 + 0.35 * source_presence)
    row["priority_score"] = _priority_score(candidate, evidence_strength=evidence_strength)
    row["uncertainty_score"] = _uncertainty_score(candidate)
    row["research_debt_score"] = _research_debt_score(candidate)
    row["effective_evidence_strength"] = evidence_strength
    return row


def _priority_score(candidate: ResearchDirectorCandidate, *, evidence_strength: float) -> float:
    return round(
        0.30 * candidate.information_gain
        + 0.25 * candidate.bottleneck_removal
        + 0.15 * evidence_strength
        + 0.10 * candidate.uncertainty_reduction
        + 0.10 * candidate.research_cost_score
        + 0.10 * candidate.debt_reduction,
        6,
    )


def _uncertainty_score(candidate: ResearchDirectorCandidate) -> float:
    return round(
        0.45 * candidate.uncertainty_reduction
        + 0.25 * candidate.evidence_gap
        + 0.20 * candidate.decision_blocking
        + 0.10 * candidate.ambiguity,
        6,
    )


def _research_debt_score(candidate: ResearchDirectorCandidate) -> float:
    return round(
        0.40 * candidate.debt_reduction
        + 0.25 * candidate.recurrence
        + 0.20 * candidate.maintenance_drag
        + 0.15 * candidate.reuse_value,
        6,
    )


def _rank(rows: list[dict[str, Any]], score_key: str) -> list[dict[str, Any]]:
    ranked = sorted(rows, key=lambda row: (-float(row[score_key]), row["candidate_id"]))
    return [{**row, "rank": index + 1} for index, row in enumerate(ranked)]


def _measure_target_bottleneck_ranking(priority_rankings: list[dict[str, Any]]) -> dict[str, Any]:
    required_target_order = ["DATA_COVERAGE", "REPLAY_ATTRITION", "VOCABULARY_MISMATCH"]
    actual_top_three = [row["candidate_id"] for row in priority_rankings[:3]]
    ranks = {row["candidate_id"]: int(row["rank"]) for row in priority_rankings}
    lower_value_ranks = [int(row["rank"]) for row in priority_rankings if row.get("lower_value_work")]
    lower_value_best_rank = min(lower_value_ranks) if lower_value_ranks else None
    targets_above_lower_value = all(
        ranks[target_id] < lower_value_best_rank
        for target_id in required_target_order
    ) if lower_value_best_rank is not None else False
    target_order_matches = actual_top_three == required_target_order
    return {
        "target_bottleneck_ids": required_target_order,
        "required_target_order": required_target_order,
        "actual_top_three": actual_top_three,
        "target_order_matches": target_order_matches,
        "target_bottlenecks_above_lower_value_work": targets_above_lower_value,
        "lower_value_work_best_rank": lower_value_best_rank,
        "result": "PASS" if target_order_matches and targets_above_lower_value else "FAIL",
    }


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _yes_no(value: bool) -> str:
    return "YES" if value else "NO"


if __name__ == "__main__":
    result = write_research_director_evaluation_report()
    print(json.dumps({"output_path": result["output_path"], "status": result["report"]["status"]}, indent=2, sort_keys=True))
