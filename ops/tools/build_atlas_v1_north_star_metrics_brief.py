from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_DAY = "2026-06-03"
DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")


@dataclass(frozen=True)
class Source:
    label: str
    path: Path
    data: dict[str, Any] | None

    @property
    def status(self) -> str:
        return "PRESENT" if self.data is not None else "MISSING"


@dataclass(frozen=True)
class NorthStarMetrics:
    day: str
    sources: tuple[Source, ...]
    position_count: int
    open_position_count: int
    closed_position_count: int
    total_samples: int
    included_samples: int
    excluded_samples: int
    included_sleeve_counts: tuple[tuple[str, int], ...]
    included_hypothesis_counts: tuple[tuple[str, int], ...]
    sleeves_with_included_samples: int
    hypotheses_with_included_samples: int
    evidence_status_counts: tuple[tuple[str, int], ...]
    sample_status_counts: tuple[tuple[str, int], ...]
    underpowered_count: int
    positive_evidence_count: int
    zero_sample_count: int
    building_sample_count: int
    total_raw_signals: int
    total_candidates_generated: int
    total_candidates_rejected: int
    valid_candidate_contracts: int
    rejected_candidate_contracts: int
    expected_suppressions: int
    certification_bottlenecks: int
    lifecycle_valid_contracts: int
    lifecycle_paper_positions_created: int
    largest_included_sleeve: str
    largest_included_sleeve_count: int
    largest_included_hypothesis: str
    largest_included_hypothesis_count: int
    largest_paper_sleeve: str
    largest_paper_sleeve_count: int


def _artifact_path(truth_root: Path, family: str, day: str, filename: str) -> Path:
    return truth_root / "reports" / family / day / filename


def source_paths(truth_root: Path, day: str) -> dict[str, Path]:
    return {
        "paper_position_ledger": _artifact_path(
            truth_root,
            "aegis_paper_position_ledger_v1",
            day,
            "paper_position_ledger.v1.json",
        ),
        "validation_samples": _artifact_path(
            truth_root,
            "aegis_validation_samples_v1",
            day,
            "validation_samples.v1.json",
        ),
        "sleeve_evidence_certification": _artifact_path(
            truth_root,
            "aegis_sleeve_evidence_certification_v1",
            day,
            "sleeve_evidence_certification.v1.json",
        ),
        "candidate_generation_diagnostics": _artifact_path(
            truth_root,
            "aegis_candidate_generation_diagnostics_v1",
            day,
            "candidate_generation_diagnostics.v1.json",
        ),
        "candidate_to_paper_lifecycle": _artifact_path(
            truth_root,
            "aegis_candidate_to_paper_lifecycle_v1",
            day,
            "candidate_to_paper_lifecycle.v1.json",
        ),
    }


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def _int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return 0


def _sorted_counter(counter: Counter[str]) -> tuple[tuple[str, int], ...]:
    return tuple(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def _count_by(rows: list[dict[str, Any]], field: str) -> tuple[tuple[str, int], ...]:
    counter: Counter[str] = Counter()
    for row in rows:
        value = str(row.get(field) or "UNKNOWN")
        counter[value] += 1
    return _sorted_counter(counter)


def _source(label: str, path: Path) -> Source:
    return Source(label=label, path=path, data=_load_json(path))


def collect_metrics(truth_root: Path = DEFAULT_TRUTH_ROOT, day: str = DEFAULT_DAY) -> NorthStarMetrics:
    paths = source_paths(truth_root, day)
    sources = tuple(_source(label, path) for label, path in paths.items())
    by_label = {source.label: source.data or {} for source in sources}

    ledger = by_label["paper_position_ledger"]
    samples_artifact = by_label["validation_samples"]
    certification = by_label["sleeve_evidence_certification"]
    candidate_diagnostics = by_label["candidate_generation_diagnostics"]
    lifecycle = by_label["candidate_to_paper_lifecycle"]

    samples = list(samples_artifact.get("samples") or [])
    included_rows = [row for row in samples if row.get("inclusion_status") == "INCLUDED"]
    included_sleeve_counts = _count_by(included_rows, "sleeve_id")
    included_hypothesis_counts = _count_by(included_rows, "hypothesis_id")

    sleeve_rows = list(certification.get("sleeves") or [])
    evidence_status_counts = tuple(
        sorted(
            ((str(key), _int(value)) for key, value in (certification.get("summary") or {}).get("evidence_status_counts", {}).items()),
            key=lambda item: (-item[1], item[0]),
        )
    )
    sample_status_counts = tuple(
        sorted(
            ((str(key), _int(value)) for key, value in (certification.get("summary") or {}).get("sample_status_counts", {}).items()),
            key=lambda item: (-item[1], item[0]),
        )
    )
    if not evidence_status_counts:
        evidence_status_counts = _count_by(sleeve_rows, "evidence_status")
    if not sample_status_counts:
        sample_status_counts = _count_by(sleeve_rows, "sample_status")

    rejected_details = list(candidate_diagnostics.get("rejected_raw_signal_details") or [])
    expected_suppressions = sum(
        1
        for row in rejected_details
        if row.get("expected_rejection") is True
        and row.get("rejection_reason") == "PORTFOLIO_GATE_SUPPRESSED"
    )
    certification_bottlenecks = sum(
        1 for row in rejected_details if row.get("rejection_reason") == "NON_CERTIFIED_CANDIDATE_SNAPSHOT"
    )

    position_rows = list(ledger.get("positions") or [])
    paper_sleeve_counts = _count_by(position_rows, "sleeve_id")
    largest_included_sleeve = included_sleeve_counts[0] if included_sleeve_counts else ("NONE", 0)
    largest_included_hypothesis = included_hypothesis_counts[0] if included_hypothesis_counts else ("NONE", 0)
    largest_paper_sleeve = paper_sleeve_counts[0] if paper_sleeve_counts else ("NONE", 0)
    validation_summary = samples_artifact.get("summary") or {}
    certification_summary = certification.get("summary") or {}
    lifecycle_summary = lifecycle.get("summary") or {}

    return NorthStarMetrics(
        day=day,
        sources=sources,
        position_count=_int(ledger.get("position_count")),
        open_position_count=_int(ledger.get("open_position_count")),
        closed_position_count=_int(ledger.get("closed_position_count")),
        total_samples=_int(validation_summary.get("total_samples")),
        included_samples=_int(validation_summary.get("included_samples")),
        excluded_samples=_int(validation_summary.get("excluded_samples")),
        included_sleeve_counts=included_sleeve_counts,
        included_hypothesis_counts=included_hypothesis_counts,
        sleeves_with_included_samples=len(included_sleeve_counts),
        hypotheses_with_included_samples=len(included_hypothesis_counts),
        evidence_status_counts=evidence_status_counts,
        sample_status_counts=sample_status_counts,
        underpowered_count=_int(certification_summary.get("underpowered_count")),
        positive_evidence_count=_int(certification_summary.get("positive_evidence_count")),
        zero_sample_count=dict(sample_status_counts).get("ZERO_SAMPLE", 0),
        building_sample_count=dict(sample_status_counts).get("BUILDING_SAMPLE", 0),
        total_raw_signals=_int(candidate_diagnostics.get("total_raw_signals")),
        total_candidates_generated=_int(candidate_diagnostics.get("total_candidates_generated")),
        total_candidates_rejected=_int(candidate_diagnostics.get("total_candidates_rejected")),
        valid_candidate_contracts=_int(candidate_diagnostics.get("valid_candidate_contracts")),
        rejected_candidate_contracts=_int(candidate_diagnostics.get("rejected_candidate_contracts")),
        expected_suppressions=expected_suppressions,
        certification_bottlenecks=certification_bottlenecks,
        lifecycle_valid_contracts=_int(lifecycle_summary.get("valid_candidate_contract_count")),
        lifecycle_paper_positions_created=_int(lifecycle_summary.get("paper_positions_created_count")),
        largest_included_sleeve=largest_included_sleeve[0],
        largest_included_sleeve_count=largest_included_sleeve[1],
        largest_included_hypothesis=largest_included_hypothesis[0],
        largest_included_hypothesis_count=largest_included_hypothesis[1],
        largest_paper_sleeve=largest_paper_sleeve[0],
        largest_paper_sleeve_count=largest_paper_sleeve[1],
    )


def _source_ref(metrics: NorthStarMetrics, label: str) -> str:
    for source in metrics.sources:
        if source.label == label:
            return source.path.as_posix()
    return "MISSING_SOURCE"


def _pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0%"
    return f"{(numerator / denominator) * 100:.1f}%"


def _format_counts(counts: tuple[tuple[str, int], ...]) -> str:
    if not counts:
        return "None."
    return ", ".join(f"{key}={value}" for key, value in counts)


def render_markdown(metrics: NorthStarMetrics) -> str:
    ledger = _source_ref(metrics, "paper_position_ledger")
    samples = _source_ref(metrics, "validation_samples")
    certification = _source_ref(metrics, "sleeve_evidence_certification")
    diagnostics = _source_ref(metrics, "candidate_generation_diagnostics")
    lifecycle = _source_ref(metrics, "candidate_to_paper_lifecycle")
    lines = [
        "# North Star Metrics Summary",
        "",
        "Status: READ_ONLY_CITATION_BOUND_ATLAS_V1_NORTH_STAR_METRICS_BRIEF",
        "",
        f"Day: {metrics.day}",
        "",
        "This brief calculates Atlas's five North Star metrics from existing artifacts only.",
        "",
        "It does not infer readiness beyond existing artifact states, recommend trades, allocate capital, mutate candidates, modify sleeves, change validation logic, alter runtime truth, or create UI.",
        "",
        f"- Distributed mature validation evidence: {metrics.included_samples} included validation samples across {metrics.sleeves_with_included_samples} sleeves and {metrics.hypotheses_with_included_samples} hypotheses. (source: `{samples}`)",
        f"- Evidence maturity: {metrics.underpowered_count} underpowered sleeves, {metrics.positive_evidence_count} positive evidence rows; sample statuses {_format_counts(metrics.sample_status_counts)}. (source: `{certification}`)",
        f"- Outcome flow: {metrics.position_count} paper positions -> {metrics.included_samples} included validation samples ({_pct(metrics.included_samples, metrics.position_count)}), with {metrics.open_position_count} open positions and {metrics.excluded_samples} excluded samples. (sources: `{ledger}`, `{samples}`)",
        f"- Candidate-to-paper conversion: {metrics.total_raw_signals} raw signals -> {metrics.total_candidates_generated} generated candidates -> {metrics.valid_candidate_contracts} valid contracts -> {metrics.lifecycle_paper_positions_created} paper positions. (sources: `{diagnostics}`, `{lifecycle}`)",
        f"- Evidence concentration: largest included-sample sleeve `{metrics.largest_included_sleeve}` has {metrics.largest_included_sleeve_count}/{metrics.included_samples} included samples; largest paper-position sleeve `{metrics.largest_paper_sleeve}` has {metrics.largest_paper_sleeve_count}/{metrics.position_count} paper positions. (sources: `{samples}`, `{ledger}`)",
        "",
        "# Distributed Mature Validation Evidence",
        "",
        f"- Included validation samples: {metrics.included_samples}. (source: `{samples}`)",
        f"- Sleeves with included validation samples: {metrics.sleeves_with_included_samples}. (source: `{samples}`)",
        f"- Hypotheses with included validation samples: {metrics.hypotheses_with_included_samples}. (source: `{samples}`)",
        f"- Included samples by sleeve: {_format_counts(metrics.included_sleeve_counts)} (source: `{samples}`)",
        f"- Included samples by hypothesis: {_format_counts(metrics.included_hypothesis_counts)} (source: `{samples}`)",
        "",
        "# Evidence Maturity Status",
        "",
        f"- Evidence status counts: {_format_counts(metrics.evidence_status_counts)} (source: `{certification}`)",
        f"- Sample status counts: {_format_counts(metrics.sample_status_counts)} (source: `{certification}`)",
        f"- Positive evidence rows: {metrics.positive_evidence_count}. (source: `{certification}`)",
        f"- Underpowered sleeve rows: {metrics.underpowered_count}. (source: `{certification}`)",
        "",
        "# Outcome Flow",
        "",
        f"- Paper positions: {metrics.position_count}. (source: `{ledger}`)",
        f"- Open positions: {metrics.open_position_count}. (source: `{ledger}`)",
        f"- Closed positions: {metrics.closed_position_count}. (source: `{ledger}`)",
        f"- Total validation samples: {metrics.total_samples}. (source: `{samples}`)",
        f"- Included validation samples: {metrics.included_samples}. (source: `{samples}`)",
        f"- Excluded validation samples: {metrics.excluded_samples}. (source: `{samples}`)",
        f"- Included-sample conversion from paper positions: {_pct(metrics.included_samples, metrics.position_count)}. (sources: `{ledger}`, `{samples}`)",
        "",
        "# Candidate-to-Paper Conversion Quality",
        "",
        f"- Raw signals: {metrics.total_raw_signals}. (source: `{diagnostics}`)",
        f"- Generated candidates: {metrics.total_candidates_generated}. (source: `{diagnostics}`)",
        f"- Rejected candidates: {metrics.total_candidates_rejected}. (source: `{diagnostics}`)",
        f"- Expected portfolio-gate suppressions: {metrics.expected_suppressions}. (source: `{diagnostics}`)",
        f"- Certification bottlenecks: {metrics.certification_bottlenecks}. (source: `{diagnostics}`)",
        f"- Valid candidate contracts in diagnostics: {metrics.valid_candidate_contracts}. (source: `{diagnostics}`)",
        f"- Valid candidate contracts in lifecycle: {metrics.lifecycle_valid_contracts}. (source: `{lifecycle}`)",
        f"- Paper positions created by lifecycle: {metrics.lifecycle_paper_positions_created}. (source: `{lifecycle}`)",
        "",
        "# Evidence Concentration",
        "",
        f"- Largest included-sample sleeve: `{metrics.largest_included_sleeve}` with {metrics.largest_included_sleeve_count}/{metrics.included_samples} included samples ({_pct(metrics.largest_included_sleeve_count, metrics.included_samples)}). (source: `{samples}`)",
        f"- Largest included-sample hypothesis: `{metrics.largest_included_hypothesis}` with {metrics.largest_included_hypothesis_count}/{metrics.included_samples} included samples ({_pct(metrics.largest_included_hypothesis_count, metrics.included_samples)}). (source: `{samples}`)",
        f"- Largest paper-position sleeve: `{metrics.largest_paper_sleeve}` with {metrics.largest_paper_sleeve_count}/{metrics.position_count} paper positions ({_pct(metrics.largest_paper_sleeve_count, metrics.position_count)}). (source: `{ledger}`)",
        "",
        "# Anti-Metrics Warning",
        "",
        "Raw signals, generated candidate count, total candidate inventory, paper position count, and generated hypothesis count are operational diagnostics only.",
        "",
        "They must not be used as research-health indicators, trade advice, capital-allocation support, live-readiness evidence, or proof of candidate quality.",
        "",
        "# Source Paths",
        "",
    ]
    for source in metrics.sources:
        lines.append(f"- `{source.path.as_posix()}` ({source.label}: {source.status})")
    lines.append("")
    return "\n".join(lines)


def build_brief(truth_root: Path = DEFAULT_TRUTH_ROOT, day: str = DEFAULT_DAY) -> str:
    return render_markdown(collect_metrics(truth_root=truth_root, day=day))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only North Star metrics brief.")
    parser.add_argument("--day", default=DEFAULT_DAY, help="Target evidence day in YYYY-MM-DD format.")
    parser.add_argument("--truth-root", default=str(DEFAULT_TRUTH_ROOT), help="Aegis truth root.")
    args = parser.parse_args()
    print(build_brief(truth_root=Path(args.truth_root), day=args.day))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
