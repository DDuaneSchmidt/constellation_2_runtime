from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRUTH_ROOT = Path(os.environ.get("AEGIS_TRUTH_ROOT", "/home/node/constellation_runtime_data/truth"))
DEFAULT_JOURNAL_ROOT = REPO_ROOT / "research_journal"
DEFAULT_DAY = os.environ.get("TARGET_DAY", "2026-06-03")

REQUIRED_REPORTS = (
    "first_sample_bottleneck_review_001.md",
    "outcome_evidence_concentration_watch_001.md",
    "outcome_maturity_acceleration_review_001.md",
    "outcome_bottleneck_decomposition_review_001.md",
    "evidence_accumulation_forecast_review_001.md",
    "open_paper_position_outcome_follow_through_review_001.md",
)


@dataclass(frozen=True)
class SourceSet:
    day: str
    paper_position_ledger: Path
    sleeve_performance_truth: Path
    exit_recommendations: Path
    outcome_registry: Path
    outcome_flow_audit: Path
    generated_hypothesis_outcome_maturity_monitor: Path
    validation_samples: Path
    sleeve_evidence_certification: Path
    journal_reports: tuple[Path, ...]


@dataclass(frozen=True)
class SleeveOutcomeFlow:
    sleeve_id: str
    open_positions: int
    closed_positions: int
    outcome_rows: int
    included_validation_samples: int
    excluded_validation_samples: int
    evidence_status: str
    sample_status: str
    data_quality_status: str

    @property
    def paper_observations(self) -> int:
        return self.open_positions + self.closed_positions


@dataclass(frozen=True)
class DistributionStats:
    sample_producing_sleeves: int
    total_included_samples: int
    top_sleeve_id: str
    top_sleeve_samples: int
    top_sleeve_share_pct: float


def _artifact_path(truth_root: Path, artifact_dir: str, day: str, filename: str) -> Path:
    return truth_root / "reports" / artifact_dir / day / filename


def source_set(truth_root: Path, journal_root: Path, day: str) -> SourceSet:
    report_root = journal_root / "reports"
    return SourceSet(
        day=day,
        paper_position_ledger=_artifact_path(
            truth_root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"
        ),
        sleeve_performance_truth=_artifact_path(
            truth_root, "aegis_sleeve_performance_truth_v1", day, "sleeve_performance_truth.v1.json"
        ),
        exit_recommendations=_artifact_path(
            truth_root, "aegis_exit_recommendations_v1", day, "exit_recommendations.v1.json"
        ),
        outcome_registry=_artifact_path(truth_root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        outcome_flow_audit=_artifact_path(truth_root, "aegis_outcome_flow_audit_v1", day, "outcome_flow_audit.v1.json"),
        generated_hypothesis_outcome_maturity_monitor=_artifact_path(
            truth_root,
            "aegis_generated_hypothesis_outcome_maturity_monitor_v1",
            day,
            "generated_hypothesis_outcome_maturity_monitor.v1.json",
        ),
        validation_samples=_artifact_path(truth_root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
        sleeve_evidence_certification=_artifact_path(
            truth_root, "aegis_sleeve_evidence_certification_v1", day, "sleeve_evidence_certification.v1.json"
        ),
        journal_reports=tuple(report_root / name for name in REQUIRED_REPORTS),
    )


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    return payload


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _add_sleeve(stats: dict[str, dict[str, Any]], sleeve_id: str) -> dict[str, Any]:
    if not sleeve_id:
        sleeve_id = "UNKNOWN"
    return stats.setdefault(
        sleeve_id,
        {
            "open_positions": 0,
            "closed_positions": 0,
            "outcome_rows": 0,
            "included_validation_samples": 0,
            "excluded_validation_samples": 0,
            "evidence_status": "UNKNOWN",
            "sample_status": "UNKNOWN",
            "data_quality_status": "UNKNOWN",
        },
    )


def _status_is_closed(status: str) -> bool:
    normalized = status.upper()
    return normalized in {"CLOSED", "RESOLVED", "PAPER_POSITION_CLOSED", "AUTO_CLOSED_PAPER_OUTCOME"}


def _status_is_open(status: str) -> bool:
    normalized = status.upper()
    return normalized in {"OPEN", "PAPER_POSITION_OPEN"}


def _collect_positions(ledger: dict[str, Any], stats: dict[str, dict[str, Any]]) -> None:
    rows: list[Any] = []
    for key in ("positions", "open_positions", "closed_positions", "historical_positions"):
        value = ledger.get(key)
        if isinstance(value, list):
            rows.extend(value)
    seen_position_ids: set[str] = set()
    for position in rows:
        if not isinstance(position, dict):
            continue
        position_id = str(position.get("position_id") or position.get("paper_position_id") or "")
        if position_id and position_id in seen_position_ids:
            continue
        if position_id:
            seen_position_ids.add(position_id)
        sleeve_id = str(position.get("sleeve_id") or "UNKNOWN")
        row = _add_sleeve(stats, sleeve_id)
        status = str(position.get("current_status") or position.get("status") or position.get("lifecycle_state") or "")
        if _status_is_closed(status) or position.get("exit_time"):
            row["closed_positions"] += 1
        elif _status_is_open(status) or not status:
            row["open_positions"] += 1


def _collect_sleeve_performance(truth: dict[str, Any], stats: dict[str, dict[str, Any]]) -> None:
    for sleeve in truth.get("sleeves") or []:
        if not isinstance(sleeve, dict):
            continue
        sleeve_id = str(sleeve.get("sleeve_id") or "UNKNOWN")
        row = _add_sleeve(stats, sleeve_id)
        row["open_positions"] = max(row["open_positions"], _as_int(sleeve.get("open_paper_position_count")))
        row["closed_positions"] = max(row["closed_positions"], _as_int(sleeve.get("closed_paper_position_count")))
        row["data_quality_status"] = str(sleeve.get("data_quality_status") or row["data_quality_status"])


def _collect_outcomes(outcome_registry: dict[str, Any], stats: dict[str, dict[str, Any]]) -> None:
    for outcome in outcome_registry.get("outcomes") or []:
        if not isinstance(outcome, dict):
            continue
        sleeve_id = str(outcome.get("sleeve_id") or "UNKNOWN")
        row = _add_sleeve(stats, sleeve_id)
        row["outcome_rows"] += 1


def _collect_validation_samples(validation: dict[str, Any], stats: dict[str, dict[str, Any]]) -> None:
    for sample in validation.get("samples") or []:
        if not isinstance(sample, dict):
            continue
        sleeve_id = str(sample.get("sleeve_id") or "UNKNOWN")
        row = _add_sleeve(stats, sleeve_id)
        if str(sample.get("inclusion_status") or "").upper() == "INCLUDED":
            row["included_validation_samples"] += 1
        else:
            row["excluded_validation_samples"] += 1


def _collect_certification(certification: dict[str, Any], stats: dict[str, dict[str, Any]]) -> None:
    for sleeve in certification.get("sleeves") or []:
        if not isinstance(sleeve, dict):
            continue
        sleeve_id = str(sleeve.get("sleeve_id") or "UNKNOWN")
        row = _add_sleeve(stats, sleeve_id)
        row["evidence_status"] = str(sleeve.get("evidence_status") or row["evidence_status"])
        row["sample_status"] = str(sleeve.get("sample_status") or row["sample_status"])
        row["closed_positions"] = max(row["closed_positions"], _as_int(sleeve.get("closed_position_count")))
        row["open_positions"] = max(row["open_positions"], _as_int(sleeve.get("active_position_count")))


def collect_sleeve_flows(sources: SourceSet) -> tuple[SleeveOutcomeFlow, ...]:
    stats: dict[str, dict[str, Any]] = {}
    _collect_positions(_read_json(sources.paper_position_ledger), stats)
    _collect_sleeve_performance(_read_json(sources.sleeve_performance_truth), stats)
    _collect_outcomes(_read_json(sources.outcome_registry), stats)
    _collect_validation_samples(_read_json(sources.validation_samples), stats)
    _collect_certification(_read_json(sources.sleeve_evidence_certification), stats)

    return tuple(
        SleeveOutcomeFlow(
            sleeve_id=sleeve_id,
            open_positions=_as_int(row["open_positions"]),
            closed_positions=_as_int(row["closed_positions"]),
            outcome_rows=_as_int(row["outcome_rows"]),
            included_validation_samples=_as_int(row["included_validation_samples"]),
            excluded_validation_samples=_as_int(row["excluded_validation_samples"]),
            evidence_status=str(row["evidence_status"]),
            sample_status=str(row["sample_status"]),
            data_quality_status=str(row["data_quality_status"]),
        )
        for sleeve_id, row in sorted(stats.items())
    )


def distribution_stats(flows: tuple[SleeveOutcomeFlow, ...]) -> DistributionStats:
    sample_flows = tuple(flow for flow in flows if flow.included_validation_samples > 0)
    total_samples = sum(flow.included_validation_samples for flow in flows)
    top = max(sample_flows, key=lambda flow: (flow.included_validation_samples, flow.sleeve_id), default=None)
    if top is None or total_samples == 0:
        return DistributionStats(0, total_samples, "NONE", 0, 0.0)
    return DistributionStats(
        sample_producing_sleeves=len(sample_flows),
        total_included_samples=total_samples,
        top_sleeve_id=top.sleeve_id,
        top_sleeve_samples=top.included_validation_samples,
        top_sleeve_share_pct=(top.included_validation_samples / total_samples) * 100.0,
    )


def concentration_direction(current: DistributionStats, baseline: DistributionStats | None) -> str:
    if baseline is None:
        return "NOT_ASSESSED_NO_BASELINE_DAY"
    if current.total_included_samples == 0 and baseline.total_included_samples == 0:
        return "UNCHANGED_NO_INCLUDED_SAMPLES"
    if current.sample_producing_sleeves > baseline.sample_producing_sleeves and current.top_sleeve_share_pct < baseline.top_sleeve_share_pct:
        return "MORE_DISTRIBUTED"
    if current.sample_producing_sleeves < baseline.sample_producing_sleeves or current.top_sleeve_share_pct > baseline.top_sleeve_share_pct:
        return "MORE_CONCENTRATED"
    if current.sample_producing_sleeves == baseline.sample_producing_sleeves and current.top_sleeve_share_pct == baseline.top_sleeve_share_pct:
        return "UNCHANGED_CONCENTRATION"
    return "MIXED_DISTRIBUTION_CHANGE"


def _all_paths(sources: SourceSet) -> tuple[Path, ...]:
    return (
        sources.paper_position_ledger,
        sources.sleeve_performance_truth,
        sources.exit_recommendations,
        sources.outcome_registry,
        sources.outcome_flow_audit,
        sources.generated_hypothesis_outcome_maturity_monitor,
        sources.validation_samples,
        sources.sleeve_evidence_certification,
    ) + sources.journal_reports


def _source_paths(source_sets: tuple[SourceSet, ...]) -> tuple[str, ...]:
    paths: set[str] = set()
    for sources in source_sets:
        for path in _all_paths(sources):
            if path.exists():
                paths.add(_display_path(path))
    return tuple(sorted(paths))


def _missing_source_paths(source_sets: tuple[SourceSet, ...]) -> tuple[str, ...]:
    paths: set[str] = set()
    for sources in source_sets:
        for path in _all_paths(sources):
            if not path.exists():
                paths.add(_display_path(path))
    return tuple(sorted(paths))


def _flow_table_rows(flows: tuple[SleeveOutcomeFlow, ...]) -> list[str]:
    rows = [
        "| Sleeve | Paper observations | Open positions | Closed positions | Outcome rows | Included samples | Excluded samples | Evidence state | Data quality |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for flow in flows:
        state = f"{flow.sample_status} / {flow.evidence_status}"
        rows.append(
            f"| `{flow.sleeve_id}` | {flow.paper_observations} | {flow.open_positions} | {flow.closed_positions} | {flow.outcome_rows} | {flow.included_validation_samples} | {flow.excluded_validation_samples} | {state} | {flow.data_quality_status} |"
        )
    return rows


FIRST_SAMPLE_FOCUS_SLEEVES = (
    "C2_CROSS_ASSET_TREND_V1",
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
    "C2_OIL_SHOCK_REVERSAL_V1",
)


def _flow_map(flows: tuple[SleeveOutcomeFlow, ...]) -> dict[str, SleeveOutcomeFlow]:
    return {flow.sleeve_id: flow for flow in flows}


def _priority_flow_line(flow_by_id: dict[str, SleeveOutcomeFlow], sleeve_id: str, basis: str) -> str:
    flow = flow_by_id.get(sleeve_id)
    if flow is None:
        return f"- `{sleeve_id}`: {basis}; no current sleeve-flow row found in the loaded artifacts."
    return (
        f"- `{sleeve_id}`: {basis}; paper observations={flow.paper_observations}, "
        f"open={flow.open_positions}, closed={flow.closed_positions}, included samples={flow.included_validation_samples}, "
        f"excluded samples={flow.excluded_validation_samples}, data quality={flow.data_quality_status}, state={flow.sample_status} / {flow.evidence_status}."
    )


def _first_sample_priority_source_paths(sources: SourceSet) -> tuple[str, ...]:
    names = {
        "first_sample_bottleneck_review_001.md",
        "open_paper_position_outcome_follow_through_review_001.md",
        "outcome_evidence_concentration_watch_001.md",
    }
    paths = [sources.paper_position_ledger, sources.sleeve_performance_truth, sources.exit_recommendations, sources.validation_samples]
    paths.extend(path for path in sources.journal_reports if path.name in names)
    return tuple(_display_path(path) for path in paths if path.exists())


def _first_sample_priority_lines(flows: tuple[SleeveOutcomeFlow, ...], sources: SourceSet) -> list[str]:
    flow_by_id = _flow_map(flows)
    lines = [
        "# First-Sample Priority View",
        "",
        "Priority type: evidence-flow priority only. This is not trade priority, exit priority, allocation priority, return-quality evidence, or readiness evidence.",
        "",
        "Closest first-sample sleeves:",
        _priority_flow_line(
            flow_by_id,
            "C2_MEAN_REVERSION_EQ_V1",
            "closest first-sample sleeve; First Sample Bottleneck Review cites IMO within 3 days of the existing 5-day max-hold threshold",
        ),
        _priority_flow_line(
            flow_by_id,
            "C2_VOL_INCOME_DEFINED_RISK_V1",
            "closest first-sample sleeve; First Sample Bottleneck Review cites GLD within 3 days of the existing 5-day max-hold threshold",
        ),
        "",
        "Highest distribution-impact sleeve:",
        _priority_flow_line(
            flow_by_id,
            "C2_CROSS_ASSET_TREND_V1",
            "highest distribution impact; First Sample Bottleneck Review identifies it as the deepest zero-sample non-Trend paper base",
        ),
        "",
        "Furthest first-sample sleeve:",
        _priority_flow_line(
            flow_by_id,
            "C2_OIL_SHOCK_REVERSAL_V1",
            "furthest reviewed focus sleeve; First Sample Bottleneck Review cites only one open observation and no identified near-term deterministic closure proximity",
        ),
        "",
        "Evidence basis:",
        "- First samples require existing deterministic closure and included validation-sample status; open unresolved positions remain excluded.",
        "- Data quality and mark certification are not treated as the first-sample blocker when sleeve performance truth reports PASS and no missing authorities.",
        "- The ordering is citation-bound to the First Sample Bottleneck Review and open-position follow-through evidence; it does not infer return quality or readiness.",
        "- Read-only evidence follow-through means refreshing existing outcome, validation-sample, sleeve-performance, and concentration artifacts after deterministic outcome cycles.",
        "",
        "Priority source paths:",
    ]
    source_paths = _first_sample_priority_source_paths(sources)
    if source_paths:
        lines.extend(f"- `{source_path}`" for source_path in source_paths)
    else:
        lines.append("None.")
    return lines


def render_markdown(
    *,
    day: str,
    flows: tuple[SleeveOutcomeFlow, ...],
    sources: SourceSet,
    baseline_day: str | None = None,
    baseline_flows: tuple[SleeveOutcomeFlow, ...] | None = None,
    baseline_sources: SourceSet | None = None,
) -> str:
    current_distribution = distribution_stats(flows)
    baseline_distribution = distribution_stats(baseline_flows or ()) if baseline_flows is not None else None
    direction = concentration_direction(current_distribution, baseline_distribution)

    total_open = sum(flow.open_positions for flow in flows)
    total_closed = sum(flow.closed_positions for flow in flows)
    total_outcomes = sum(flow.outcome_rows for flow in flows)
    total_included = current_distribution.total_included_samples
    sample_producers = tuple(flow for flow in flows if flow.included_validation_samples > 0)
    paper_no_outcome = tuple(
        flow for flow in flows if flow.paper_observations > 0 and flow.closed_positions == 0 and flow.included_validation_samples == 0
    )
    first_sample = tuple(
        sorted(
            (flow for flow in flows if flow.paper_observations > 0 and flow.included_validation_samples == 0),
            key=lambda flow: (-flow.closed_positions, -flow.open_positions, flow.sleeve_id),
        )
    )

    lines = [
        "# Atlas V1 Outcome Follow-Through Comparator",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_COMPARATOR",
        "",
        "This brief compares existing outcome-flow artifacts only. It does not validate truth, infer readiness, recommend trades, recommend exits, allocate capital, mutate candidates, modify sleeves, modify validation rules, modify runtime truth, or create journal objects.",
        "",
        f"Evidence day: {day}",
        f"Baseline day: {baseline_day or 'NONE'}",
        "",
        "# Evidence Flow Summary",
        "",
    ]

    if not flows:
        lines.extend(
            [
                "No sleeve outcome-flow rows were found for the provided artifact roots.",
                "",
                "# Distributed Validation Samples",
                "",
                "None.",
                "",
                "# Evidence Concentration",
                "",
                "No concentration calculation is available without sleeve outcome-flow rows.",
                "",
                "# Stalled Paths",
                "",
                "None.",
                "",
                "# First-Sample Opportunities",
                "",
                "None.",
                "",
                "# First-Sample Priority View",
                "",
                "No first-sample priority view is available without sleeve outcome-flow rows.",
                "",
                "# Source Paths",
                "",
            ]
        )
        source_sets = (sources,) if baseline_sources is None else (baseline_sources, sources)
        source_paths = _source_paths(source_sets)
        lines.extend(f"- `{source_path}`" for source_path in source_paths) if source_paths else lines.append("None.")
        missing = _missing_source_paths(source_sets)
        if missing:
            lines.extend(["", "Missing source paths:"])
            lines.extend(f"- `{source_path}`" for source_path in missing)
        lines.append("")
        return "\n".join(lines)

    lines.extend(
        [
            f"- Sleeves compared: {len(flows)}",
            f"- Paper observations: {total_open + total_closed}",
            f"- Open positions: {total_open}",
            f"- Closed positions: {total_closed}",
            f"- Outcome rows: {total_outcomes}",
            f"- Included validation samples: {total_included}",
            f"- Sample-producing sleeves: {current_distribution.sample_producing_sleeves}",
            f"- Distribution trend: {direction}",
            "",
            "# Distributed Validation Samples",
            "",
        ]
    )
    if sample_producers:
        for flow in sample_producers:
            lines.append(f"- `{flow.sleeve_id}`: {flow.included_validation_samples} included validation sample(s).")
    else:
        lines.append("No sleeves are producing included validation samples in the current artifact set.")
    lines.extend(["", *_flow_table_rows(flows), "", "# Evidence Concentration", ""])

    if current_distribution.total_included_samples == 0:
        lines.append("No included validation samples were found, so current concentration cannot be calculated.")
    else:
        lines.extend(
            [
                f"- Highest included-sample sleeve: `{current_distribution.top_sleeve_id}`",
                f"- Highest sleeve included validation samples: {current_distribution.top_sleeve_samples} of {current_distribution.total_included_samples} ({current_distribution.top_sleeve_share_pct:.1f}%).",
                f"- Sample-producing sleeve count: {current_distribution.sample_producing_sleeves}",
            ]
        )
        if baseline_distribution is not None:
            lines.extend(
                [
                    f"- Baseline highest-sleeve share: {baseline_distribution.top_sleeve_share_pct:.1f}% across {baseline_distribution.sample_producing_sleeves} sample-producing sleeve(s).",
                    f"- Current-vs-baseline distribution: {direction}",
                ]
            )
        else:
            lines.append("- Current-vs-baseline distribution: NOT_ASSESSED_NO_BASELINE_DAY")
        lines.append("- Interpretation: concentration describes evidence distribution only. It is not a readiness, quality, trade, exit, or allocation claim.")

    concentrated = tuple(flow for flow in flows if total_included and flow.included_validation_samples == current_distribution.top_sleeve_samples and flow.included_validation_samples > 0)
    if concentrated:
        lines.append("- Concentrated evidence sleeve(s): " + ", ".join(f"`{flow.sleeve_id}`" for flow in concentrated))

    lines.extend(["", "# Stalled Paths", ""])
    if paper_no_outcome:
        lines.append("Sleeves accumulating paper observations but no included outcomes/samples:")
        for flow in paper_no_outcome:
            lines.append(
                f"- `{flow.sleeve_id}`: {flow.paper_observations} paper observation(s), {flow.open_positions} open, {flow.closed_positions} closed, 0 included validation samples."
            )
    else:
        lines.append("No sleeves matched the deterministic stalled-path rule of paper observations with zero closed positions and zero included validation samples.")

    lines.extend(["", "# First-Sample Opportunities", ""])
    if first_sample:
        for flow in first_sample:
            basis = "closed-position evidence exists but no included sample" if flow.closed_positions > 0 else "open paper observations exist but no included sample"
            lines.append(
                f"- `{flow.sleeve_id}`: {basis}; paper observations={flow.paper_observations}, open={flow.open_positions}, closed={flow.closed_positions}."
            )
    else:
        lines.append("No first-sample opportunities found by the deterministic grouping rule.")

    lines.extend(["", *_first_sample_priority_lines(flows, sources), ""])

    lines.extend(["# Source Paths", ""])
    source_sets = (sources,) if baseline_sources is None else (baseline_sources, sources)
    source_paths = _source_paths(source_sets)
    if source_paths:
        lines.extend(f"- `{source_path}`" for source_path in source_paths)
    else:
        lines.append("None.")
    missing = _missing_source_paths(source_sets)
    if missing:
        lines.extend(["", "Missing source paths:"])
        lines.extend(f"- `{source_path}`" for source_path in missing)
    lines.append("")
    return "\n".join(lines)


def build_comparator_brief(
    truth_root: Path,
    journal_root: Path,
    day: str,
    baseline_day: str | None = None,
) -> str:
    sources = source_set(truth_root, journal_root, day)
    flows = collect_sleeve_flows(sources)
    baseline_sources = source_set(truth_root, journal_root, baseline_day) if baseline_day else None
    baseline_flows = collect_sleeve_flows(baseline_sources) if baseline_sources else None
    return render_markdown(
        day=day,
        flows=flows,
        sources=sources,
        baseline_day=baseline_day,
        baseline_flows=baseline_flows,
        baseline_sources=baseline_sources,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only Outcome Follow-Through Comparator brief.")
    parser.add_argument("--truth-root", default=str(DEFAULT_TRUTH_ROOT), help="Aegis truth root directory.")
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    parser.add_argument("--day", default=DEFAULT_DAY, help="Current evidence day in YYYY-MM-DD format.")
    parser.add_argument("--baseline-day", default=None, help="Optional baseline evidence day in YYYY-MM-DD format.")
    args = parser.parse_args()

    print(build_comparator_brief(Path(args.truth_root), Path(args.journal_root), args.day, args.baseline_day))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
