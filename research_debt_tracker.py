#!/usr/bin/env python3
"""Build a read-only Atlas research debt dashboard from existing reports."""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "research_journal" / "reports" / "research_debt_dashboard.md"


@dataclass(frozen=True)
class MetricSpec:
    key: str
    label: str
    baseline: int
    weight: float
    severity: str
    source: str
    remediation: str


METRICS = [
    MetricSpec(
        key="missing_datasets",
        label="Missing datasets",
        baseline=14,
        weight=0.25,
        severity="CRITICAL",
        source="reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md",
        remediation="Stage read-only market-data readiness by validation leverage: DIA/QQQ, TLT/USO/DBC, then high-overlap single-stock symbols.",
    ),
    MetricSpec(
        key="unvalidated_candidates",
        label="Unvalidated candidates",
        baseline=8,
        weight=0.20,
        severity="CRITICAL",
        source="reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json",
        remediation="Route highest-ranked candidates through direct validation before stronger interpretation.",
    ),
    MetricSpec(
        key="proxy_dependent_candidates",
        label="Proxy-dependent candidates",
        baseline=600,
        weight=0.25,
        severity="CRITICAL",
        source="reports/atlas_v2_research_os/final_candidate_ranking/latest.json",
        remediation="Treat proxy-ranked candidate conclusions as research-only until candidate-specific replay exists.",
    ),
    MetricSpec(
        key="stale_observations",
        label="Stale observations",
        baseline=47,
        weight=0.15,
        severity="HIGH",
        source="research_journal/reports/open_paper_position_outcome_follow_through_review_001.md",
        remediation="Continue deterministic outcome follow-through and refresh validation samples after natural closure events.",
    ),
    MetricSpec(
        key="unresolved_adversary_findings",
        label="Unresolved adversary findings",
        baseline=150,
        weight=0.15,
        severity="HIGH",
        source="reports/atlas_v2_research_os/research_adversary_corpus/corpus_summary.md",
        remediation="Human-score a bounded adversary sample for exact, partial, miss, and false-positive outcomes.",
    ),
]


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def int_from_regex(text: str, pattern: str, default: int = 0) -> int:
    match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    if not match:
        return default
    return int(match.group(1).replace(",", ""))


def nested_get(data: dict[str, Any], *keys: str, default: int = 0) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def count_missing_datasets() -> int:
    audit = read_text(ROOT / "reports/atlas_v2_research_os/direct_replay_coverage_audit_001.md")
    count = int_from_regex(audit, r"symbols_missing_data:\s*`?(\d+)`?")
    if count:
        return count

    validation = read_json(ROOT / "reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json")
    missing_paths = nested_get(validation, "summary", "missing_csvs", default=[])
    symbols: set[str] = set()
    if isinstance(missing_paths, list):
        for item in missing_paths:
            match = re.search(r"/([A-Z]{2,5})(?:_tiingo_adjusted_daily|_daily|_[0-9a-z]+)?\.csv$", str(item))
            if match:
                symbols.add(match.group(1))
    return len(symbols)


def count_unvalidated_candidates() -> int:
    validation = read_json(ROOT / "reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json")
    summary = validation.get("summary", {}) if isinstance(validation.get("summary"), dict) else {}
    reviewed = int(summary.get("candidates_reviewed", 0) or 0)
    confirmed = int(summary.get("confirmed", 0) or 0)
    insufficient = int(summary.get("insufficient_data", 0) or 0)
    return max(reviewed - confirmed, insufficient)


def count_proxy_dependent_candidates() -> int:
    ranking = read_json(ROOT / "reports/atlas_v2_research_os/final_candidate_ranking/latest.json")
    weaknesses = nested_get(ranking, "main_remaining_evidence_weaknesses", default={})
    if isinstance(weaknesses, dict) and "proxy_dependency" in weaknesses:
        return int(weaknesses["proxy_dependency"] or 0)
    return int(nested_get(ranking, "summary", "candidates_evaluated", default=0) or 0)


def count_stale_observations() -> int:
    follow_through = read_text(ROOT / "research_journal/reports/open_paper_position_outcome_follow_through_review_001.md")
    count = int_from_regex(follow_through, r"(\d+)\s+of\s+51\s+open positions were not near")
    if count:
        return count
    return int_from_regex(follow_through, r"No near-term deterministic proximity\s*\|\s*(\d+)")


def count_unresolved_adversary_findings() -> int:
    corpus = read_text(ROOT / "reports/atlas_v2_research_os/research_adversary_corpus/corpus_summary.md")
    created = int_from_regex(corpus, r"review artifacts created:\s*(\d+)")
    evaluation = read_json(
        ROOT / "reports/atlas_v2_research_os/research_adversary_evaluation/latest/research_adversary_evaluation_summary.json"
    )
    evaluated = int(nested_get(evaluation, "summary", "cases_evaluated", default=0) or 0)
    return max(created - evaluated, 0)


def current_counts() -> dict[str, int]:
    return {
        "missing_datasets": count_missing_datasets(),
        "unvalidated_candidates": count_unvalidated_candidates(),
        "proxy_dependent_candidates": count_proxy_dependent_candidates(),
        "stale_observations": count_stale_observations(),
        "unresolved_adversary_findings": count_unresolved_adversary_findings(),
    }


def debt_score(counts: dict[str, int]) -> float:
    score = 0.0
    for spec in METRICS:
        ratio = 0.0 if spec.baseline <= 0 else min(counts.get(spec.key, 0) / spec.baseline, 1.5)
        score += ratio * spec.weight * 100
    return round(score, 1)


def previous_score(output_path: Path) -> float | None:
    text = read_text(output_path)
    match = re.search(r"Debt Score:\s*`?([0-9]+(?:\.[0-9]+)?)", text)
    if not match:
        return None
    return float(match.group(1))


def trend_label(current: float, previous: float | None) -> tuple[str, str]:
    if previous is None:
        return "BASELINE", "No previous dashboard score found; this run establishes the measurement baseline."
    delta = round(current - previous, 1)
    if delta <= -2:
        return "IMPROVING", f"Debt score decreased by {abs(delta):.1f} points from the previous dashboard."
    if delta >= 2:
        return "WORSENING", f"Debt score increased by {delta:.1f} points from the previous dashboard."
    return "FLAT", f"Debt score changed by {delta:+.1f} points from the previous dashboard."


def reduction_progress(count: int, baseline: int) -> tuple[int, float]:
    reduced = max(baseline - count, 0)
    percent = 0.0 if baseline <= 0 else round((reduced / baseline) * 100, 1)
    return reduced, percent


def escape_csv_cell(value: str) -> str:
    return value.replace("|", "\\|")


def build_dashboard(counts: dict[str, int], output_path: Path) -> str:
    score = debt_score(counts)
    prior = previous_score(output_path)
    trend, trend_note = trend_label(score, prior)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows: list[str] = []
    for spec in METRICS:
        count = counts.get(spec.key, 0)
        reduced, percent = reduction_progress(count, spec.baseline)
        rows.append(
            "| {label} | {severity} | {count} | {baseline} | {reduced} | {percent:.1f}% | {source} |".format(
                label=spec.label,
                severity=spec.severity,
                count=count,
                baseline=spec.baseline,
                reduced=reduced,
                percent=percent,
                source=escape_csv_cell(spec.source),
            )
        )

    remediation_rows = []
    for spec in METRICS:
        remediation_rows.append(f"- `{spec.key}`: {spec.remediation}")

    total_baseline = sum(spec.baseline for spec in METRICS)
    total_current = sum(counts.get(spec.key, 0) for spec in METRICS)
    total_reduced = max(total_baseline - total_current, 0)
    total_progress = 0.0 if total_baseline == 0 else round((total_reduced / total_baseline) * 100, 1)

    return "\n".join(
        [
            "# Research Debt Dashboard",
            "",
            f"Generated: `{generated_at}`",
            "",
            "Scope: measurement-only dashboard from existing Atlas research reports. No production, governance, authority, candidate, replay, qualification, validation, paper-forward, trading, broker, position-sizing, or capital-allocation changes.",
            "",
            f"Debt Score: `{score}`",
            "",
            f"Debt Trend: `{trend}`",
            "",
            trend_note,
            "",
            f"Debt Reduction Progress: `{total_progress}%` total count reduction from inventory baseline (`{total_reduced}` of `{total_baseline}` items reduced).",
            "",
            "## Metrics",
            "",
            "| Metric | Severity | Current count | Baseline | Reduced | Progress | Source |",
            "| --- | --- | ---: | ---: | ---: | ---: | --- |",
            *rows,
            "",
            "## Scoring",
            "",
            "Debt Score is a weighted 0-100+ index where the current count for each tracked debt class is divided by its inventory baseline and multiplied by its weight. Scores above 100 are possible if debt grows beyond the baseline.",
            "",
            "| Metric | Weight |",
            "| --- | ---: |",
            *[f"| {spec.label} | {spec.weight:.2f} |" for spec in METRICS],
            "",
            "## Remediation Queue",
            "",
            *remediation_rows,
            "",
            "## Machine-Readable Counts",
            "",
            "```json",
            json.dumps(
                {
                    "generated_at": generated_at,
                    "debt_score": score,
                    "debt_trend": trend,
                    "debt_reduction_progress_percent": total_progress,
                    "counts": counts,
                    "baselines": {spec.key: spec.baseline for spec in METRICS},
                },
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ]
    )


def write_register_snapshot(counts: dict[str, int], output_path: Path) -> None:
    csv_path = output_path.with_suffix(".csv")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "severity", "current_count", "baseline", "reduced", "progress_percent", "source"])
        for spec in METRICS:
            count = counts.get(spec.key, 0)
            reduced, percent = reduction_progress(count, spec.baseline)
            writer.writerow([spec.key, spec.severity, count, spec.baseline, reduced, percent, spec.source])


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate the Atlas research debt dashboard.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Dashboard markdown output path.")
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Do not write the companion metric snapshot CSV next to the dashboard.",
    )
    args = parser.parse_args()

    output_path = args.output if args.output.is_absolute() else ROOT / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    counts = current_counts()
    dashboard = build_dashboard(counts, output_path)
    output_path.write_text(dashboard, encoding="utf-8")
    if not args.no_csv:
        write_register_snapshot(counts, output_path)

    print(f"wrote {output_path}")
    print(f"debt_score={debt_score(counts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
