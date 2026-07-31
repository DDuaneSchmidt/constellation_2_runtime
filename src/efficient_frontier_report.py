from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from src.main import (
    DEFAULT_OUTPUT,
    FINAL_RANKING_GLOB,
    REPO_ROOT,
    FrontierCandidate,
    _as_float,
    _candidate_rows,
    _fmt_metric_pct,
    _fmt_num,
    _fmt_pct,
    _latest_path,
    _load_json,
    _runtime_status,
    complete_frontier_candidates,
    incomplete_frontier_candidates,
    ultrasafe_benchmark,
)


DISCOVERY_OUTPUT = REPO_ROOT / "docs" / "PORTFOLIO_DISCOVERY_REPORT.md"
ATLAS_REPORT_ROOT = REPO_ROOT / "reports" / "atlas_v2_research_os"


def _optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return _load_json(path)


def _optional_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def _source_line(label: str, path: Path) -> str:
    if path.exists():
        return f"- {label}: `{path.relative_to(REPO_ROOT)}`\n"
    return f"- {label}: missing at `{path.relative_to(REPO_ROOT)}`\n"


def _sorted_rows(rows: list[FrontierCandidate]) -> list[FrontierCandidate]:
    return sorted(rows, key=lambda row: (-row.final_score, row.risk, row.candidate_id))


def _best_new_challenger(rows: list[FrontierCandidate]) -> FrontierCandidate | None:
    ready_rows = [row for row in rows if row.classification == "READY_FOR_PAPER_FORWARD_OBSERVATION"]
    return _sorted_rows(ready_rows or rows)[0] if rows else None


def _best_risk_adjusted_candidate(rows: list[FrontierCandidate]) -> FrontierCandidate | None:
    finite = [row for row in rows if row.max_drawdown is not None and row.risk > 0]
    if not finite:
        return None
    return sorted(finite, key=lambda row: (-(row.final_score / row.risk), row.risk, row.candidate_id))[0]


def _most_different_candidate(rows: list[FrontierCandidate], family_report: dict[str, Any]) -> FrontierCandidate | None:
    top_rows = rows[:50]
    if not top_rows:
        return None
    mechanism_counts = Counter(row.mechanism or "UNKNOWN" for row in top_rows)
    regime_counts = Counter(row.classification or "UNKNOWN" for row in top_rows)
    family_assignments = family_report.get("candidate_family_assignments") or []
    duplicate_ids = {
        str(row.get("candidate_id"))
        for row in family_assignments
        if isinstance(row, dict) and str(row.get("duplicate_or_distinct_assessment") or "").upper().startswith("REPEATED")
    }

    def score(row: FrontierCandidate) -> tuple[float, float, float, str]:
        rarity = 1.0 / max(1, mechanism_counts[row.mechanism or "UNKNOWN"])
        classification_rarity = 1.0 / max(1, regime_counts[row.classification or "UNKNOWN"])
        duplicate_penalty = 0.25 if row.candidate_id in duplicate_ids else 0.0
        return (rarity + classification_rarity - duplicate_penalty, row.final_score, -row.risk, row.candidate_id)

    return sorted(top_rows, key=score, reverse=True)[0]


def _candidate_sentence(row: FrontierCandidate | None) -> str:
    if row is None:
        return "No candidate available from current final ranking evidence."
    return (
        f"`{row.candidate_id}` ({row.mechanism or 'n/a'}, {row.classification or 'n/a'}): "
        f"score {_fmt_num(row.final_score)}, max drawdown {_fmt_metric_pct(row.max_drawdown)}, "
        f"expectancy {_fmt_pct(row.expectancy)}, sample {row.sample_size}."
    )


def _candidate_table(rows: list[FrontierCandidate], *, limit: int = 8) -> str:
    if not rows:
        return "- None.\n"
    lines = [
        "| Candidate | Mechanism | Classification | Score | Expectancy | Max DD | N |\n",
        "|---|---|---|---:|---:|---:|---:|\n",
    ]
    for row in rows[:limit]:
        lines.append(
            f"| `{row.candidate_id}` | {row.mechanism or 'n/a'} | {row.classification or 'n/a'} | "
            f"{_fmt_num(row.final_score)} | {_fmt_pct(row.expectancy)} | {_fmt_metric_pct(row.max_drawdown)} | {row.sample_size} |\n"
        )
    return "".join(lines)


def _counter_lines(counter: dict[str, Any], *, limit: int = 8) -> str:
    if not counter:
        return "- No data exposed.\n"
    rows = sorted(counter.items(), key=lambda item: (-int(item[1]), str(item[0])))[:limit]
    return "".join(f"- {key}: {value}\n" for key, value in rows)


def build_portfolio_discovery_report_markdown(top_n: int = 50) -> tuple[str, dict[str, Any]]:
    ranking_path = _latest_path(FINAL_RANKING_GLOB)
    ranking = _load_json(ranking_path)
    rows = _candidate_rows(ranking)
    top_rows = rows[:top_n]
    complete_rows = complete_frontier_candidates(top_rows)
    incomplete_rows = incomplete_frontier_candidates(top_rows)
    ultrasafe = ultrasafe_benchmark()

    failure_path = ATLAS_REPORT_ROOT / "candidate_failure_patterns" / "latest.json"
    family_path = ATLAS_REPORT_ROOT / "candidate_family_discovery" / "latest.json"
    relevance_path = ATLAS_REPORT_ROOT / "portfolio_relevance_estimate" / "latest.json"
    memory_failure_path = ATLAS_REPORT_ROOT / "memory" / "failure_patterns.json"
    retired_path = ATLAS_REPORT_ROOT / "memory" / "retired_knowledge.json"

    failure_report = _optional_json(failure_path)
    family_report = _optional_json(family_path)
    relevance_report = _optional_json(relevance_path)
    memory_failures = _optional_list(memory_failure_path)
    retired = _optional_list(retired_path)

    best = _best_new_challenger(top_rows)
    different = _most_different_candidate(top_rows, family_report)
    risk_adjusted = _best_risk_adjusted_candidate(top_rows)
    ranking_summary = ranking.get("summary") or {}
    family_summary = family_report.get("summary") or {}
    relevance_summary = relevance_report.get("summary") or {}
    relevance_conclusion = relevance_report.get("required_conclusion") or {}
    runtime_status, active_mode, graph_path = _runtime_status()

    ready_rows = [row for row in top_rows if row.classification == "READY_FOR_PAPER_FORWARD_OBSERVATION"]
    mechanism_counts = ranking_summary.get("mechanism_distribution_top_20") or Counter(row.mechanism for row in top_rows)
    classification_counts = ranking_summary.get("classification_counts") or Counter(row.classification for row in rows)
    disqualification_counts = failure_report.get("aggregate_main_disqualification_reasons") or {}
    rejected_preview = (failure_report.get("preview_rejected_distributions") or {}).get("reasons") or {}

    complement_answer = (
        "Yes, conditionally. Current evidence found paper-forward-observation candidates and several distinct families that could "
        "complement UltraSafe as research subjects, but it did not prove allocation usefulness or an UltraSafe replacement."
        if ready_rows
        else "Not proven. Current evidence does not expose enough ready, complete-metric candidates to claim a complement."
    )

    lines = [
        "# Portfolio Discovery Report\n\n",
        "## Executive Summary\n\n",
        f"- Main question: Did we find anything that could complement UltraSafe? {complement_answer}\n",
        "- This report answers complementarity, not whether anything beat UltraSafe.\n",
        f"- UltraSafe benchmark context: CAGR {_fmt_metric_pct(ultrasafe.cagr)}, Sharpe {_fmt_num(ultrasafe.sharpe)}, max drawdown {_fmt_metric_pct(ultrasafe.max_drawdown)}; source `benchmark_override`.\n",
        f"- Local discovery evidence reviewed {len(top_rows)} top rows from final candidate ranking; {len(ready_rows)} are paper-forward-observation candidates.\n",
        f"- Complete CAGR/Sharpe/max-drawdown benchmark rows in the top {len(top_rows)}: {len(complete_rows)}; incomplete rows excluded from strict UltraSafe dominance comparison: {len(incomplete_rows)}.\n",
        f"- Runtime truth context: `{runtime_status}` in `{active_mode}`. This is read-only research reporting, not trade advice, allocation, position sizing, or paper-trade authorization.\n",
        "\n## Best New Challenger\n\n",
        f"{_candidate_sentence(best)}\n\n",
        "Why it matters: this is the strongest currently ranked local candidate for further observation. It is a challenger in the research queue sense, not an UltraSafe replacement.\n\n",
        "## Most Different Candidate\n\n",
        f"{_candidate_sentence(different)}\n\n",
        "Why it matters: complementarity needs differentiated behavior. This pick favors less common mechanism/classification exposure inside the current top local set.\n\n",
        "## Best Risk Adjusted Candidate\n\n",
        f"{_candidate_sentence(risk_adjusted)}\n\n",
        "Selection rule: highest `final_score / abs(max_drawdown)` among rows with drawdown evidence. This is a screening proxy, not a Sharpe substitute.\n\n",
        "## Failure Pattern Summary\n\n",
        f"- Aggregate disqualification reasons from candidate failure patterns:\n{_counter_lines(disqualification_counts)}",
        f"- Rejected-preview reason mix:\n{_counter_lines(rejected_preview)}",
        f"- Memory failure warnings: {len(memory_failures)} active/generated failure pattern row(s).\n\n",
        "## Candidate Graveyard Summary\n\n",
        f"- Candidates evaluated: {ranking_summary.get('candidates_evaluated', 'n/a')}\n",
        f"- Excluded candidates in final ranking: {ranking_summary.get('excluded_candidate_count', 'n/a')}\n",
        f"- Classification counts:\n{_counter_lines(classification_counts)}",
        f"- Retired knowledge rows: {len(retired)}\n\n",
        "## UltraSafe Similarity Findings\n\n",
        f"- Strict UltraSafe comparison is blocked by missing local CAGR/Sharpe fields for {len(incomplete_rows)} top local row(s).\n",
        f"- Family discovery found {family_summary.get('unique_candidate_families', 'n/a')} candidate families, {family_summary.get('near_duplicate_group_count', 'n/a')} near-duplicate groups, and {family_summary.get('genuinely_distinct_family_count', 'n/a')} genuinely distinct family bucket(s).\n",
        f"- Portfolio relevance estimate: {relevance_summary.get('required_conclusion', 'n/a')}\n",
        f"- Primary overlap risk: {relevance_summary.get('primary_overlap_risk', 'n/a')}; independent family estimate: {relevance_summary.get('independent_family_estimate_base', 'n/a')}.\n",
        "- Similarity conclusion: the useful signal is not that a local row beats UltraSafe; it is that some rows may offer differentiated mechanisms/families worth validating alongside UltraSafe.\n\n",
        "## Recommended Next Tests\n\n",
        "- Run direct candidate-data validation for the best representative of each high-priority family before any portfolio inference.\n",
        "- Paper-forward observe one representative per distinct family; avoid spending review cycles on near duplicates until the representative survives.\n",
        "- Add CAGR and Sharpe fields to local candidate outputs so strict UltraSafe benchmark comparison can move from incomplete to comparable.\n",
        "- Measure cross-family correlation/overlap against UltraSafe-like behavior before calling anything complementary.\n",
        "- Re-run failure-pattern attribution after direct-data replay to separate proxy-data failures from true mechanism failures.\n\n",
        "## Top Candidate Evidence Snapshot\n\n",
        _candidate_table(top_rows),
        "\n## Sources\n\n",
        _source_line("Final candidate ranking", ranking_path),
        _source_line("Candidate failure patterns", failure_path),
        _source_line("Candidate family discovery", family_path),
        _source_line("Portfolio relevance estimate", relevance_path),
        _source_line("Memory failure patterns", memory_failure_path),
        _source_line("Retired knowledge", retired_path),
    ]
    if graph_path:
        lines.append(f"- Verified runtime graph: `{graph_path}`\n")
    lines.append(f"- Prior efficient-frontier report path: `{DEFAULT_OUTPUT.relative_to(REPO_ROOT)}`\n")

    summary = {
        "output": str(DISCOVERY_OUTPUT),
        "source": str(ranking_path),
        "top_rows": len(top_rows),
        "ready_for_paper_forward_observation_count": len(ready_rows),
        "complete_frontier_candidate_count": len(complete_rows),
        "incomplete_frontier_candidate_count": len(incomplete_rows),
        "best_new_challenger": None if best is None else best.candidate_id,
        "most_different_candidate": None if different is None else different.candidate_id,
        "best_risk_adjusted_candidate": None if risk_adjusted is None else risk_adjusted.candidate_id,
        "could_complement_ultrasafe": bool(ready_rows),
        "runtime_status": runtime_status,
    }
    return "".join(lines), summary


def portfolio_discovery_report(args: argparse.Namespace) -> int:
    markdown, summary = build_portfolio_discovery_report_markdown(top_n=args.top_n)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markdown, encoding="utf-8")
    summary["output"] = str(output)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the UltraSafe complement discovery report.")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--output", default=str(DISCOVERY_OUTPUT))
    args = parser.parse_args()
    return portfolio_discovery_report(args)


if __name__ == "__main__":
    raise SystemExit(main())
