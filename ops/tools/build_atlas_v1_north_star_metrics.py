from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRUTH_ROOT = Path(os.environ.get("AEGIS_TRUTH_ROOT", "/home/node/constellation_runtime_data/truth"))
DEFAULT_JOURNAL_ROOT = REPO_ROOT / "research_journal"
DEFAULT_DAY = os.environ.get("TARGET_DAY", "2026-06-03")

REQUIRED_REPORTS = (
    "aegis_north_star_metrics_review_001.md",
    "atlas_north_star_dashboard_spec_001.md",
    "evidence_flow_bottleneck_review_001.md",
    "outcome_bottleneck_decomposition_review_001.md",
    "decision_risk_monitor_001.md",
)


@dataclass(frozen=True)
class SourceSet:
    day: str
    validation_samples: Path
    sleeve_evidence_certification: Path
    paper_position_ledger: Path
    candidate_generation_diagnostics: Path
    candidate_contracts: Path
    research_quality_engine: Path
    statistical_sufficiency: Path
    verified_runtime_graph: Path
    journal_reports: tuple[Path, ...]


def _artifact_path(truth_root: Path, artifact_dir: str, day: str, filename: str) -> Path:
    return truth_root / "reports" / artifact_dir / day / filename


def source_set(truth_root: Path, journal_root: Path, day: str) -> SourceSet:
    report_root = journal_root / "reports"
    return SourceSet(
        day=day,
        validation_samples=_artifact_path(truth_root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
        sleeve_evidence_certification=_artifact_path(
            truth_root, "aegis_sleeve_evidence_certification_v1", day, "sleeve_evidence_certification.v1.json"
        ),
        paper_position_ledger=_artifact_path(truth_root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"),
        candidate_generation_diagnostics=_artifact_path(
            truth_root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json"
        ),
        candidate_contracts=_artifact_path(truth_root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        research_quality_engine=_artifact_path(
            truth_root, "aegis_research_quality_engine_v1", day, "research_quality_engine.v1.json"
        ),
        statistical_sufficiency=_artifact_path(truth_root, "aegis_statistical_sufficiency_v1", day, "statistical_sufficiency.v1.json"),
        verified_runtime_graph=_artifact_path(
            truth_root, "aegis_verified_runtime_graph_v1", day, "verified_runtime_graph.v1.json"
        ),
        journal_reports=tuple(report_root / name for name in REQUIRED_REPORTS),
    )


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return (numerator / denominator) * 100.0


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _all_paths(sources: SourceSet) -> tuple[Path, ...]:
    return (
        sources.validation_samples,
        sources.sleeve_evidence_certification,
        sources.paper_position_ledger,
        sources.candidate_generation_diagnostics,
        sources.candidate_contracts,
        sources.research_quality_engine,
        sources.statistical_sufficiency,
        sources.verified_runtime_graph,
    ) + sources.journal_reports


def _source_paths(sources: SourceSet) -> tuple[str, ...]:
    return tuple(sorted(_display_path(path) for path in _all_paths(sources) if path.exists()))


def _missing_source_paths(sources: SourceSet) -> tuple[str, ...]:
    return tuple(sorted(_display_path(path) for path in _all_paths(sources) if not path.exists()))


def _rows(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _collect_paper_positions(ledger: dict[str, Any]) -> tuple[int, int, int, Counter[str]]:
    rows: list[dict[str, Any]] = []
    for key in ("positions", "open_positions", "closed_positions", "historical_positions"):
        rows.extend(_rows(ledger, key))
    seen: set[str] = set()
    open_count = 0
    closed_count = 0
    by_sleeve: Counter[str] = Counter()
    for row in rows:
        position_id = str(row.get("position_id") or row.get("paper_position_id") or "")
        if position_id and position_id in seen:
            continue
        if position_id:
            seen.add(position_id)
        sleeve_id = str(row.get("sleeve_id") or "UNKNOWN")
        by_sleeve[sleeve_id] += 1
        status = str(row.get("current_status") or row.get("status") or row.get("lifecycle_state") or "").upper()
        if status in {"CLOSED", "RESOLVED", "PAPER_POSITION_CLOSED", "AUTO_CLOSED_PAPER_OUTCOME"} or row.get("exit_time"):
            closed_count += 1
        else:
            open_count += 1
    return open_count + closed_count, open_count, closed_count, by_sleeve


def _included_samples(validation: dict[str, Any]) -> list[dict[str, Any]]:
    return [row for row in _rows(validation, "samples") if str(row.get("inclusion_status") or "").upper() == "INCLUDED"]


def _sample_counts(validation: dict[str, Any]) -> tuple[int, int, int, Counter[str], Counter[str]]:
    samples = _rows(validation, "samples")
    included = _included_samples(validation)
    by_sleeve: Counter[str] = Counter(str(row.get("sleeve_id") or "UNKNOWN") for row in included)
    by_hypothesis: Counter[str] = Counter(str(row.get("hypothesis_id") or "UNKNOWN") for row in included)
    return len(samples), len(included), len(samples) - len(included), by_sleeve, by_hypothesis


def _status_counts(certification: dict[str, Any]) -> tuple[Counter[str], Counter[str], int]:
    sample_statuses: Counter[str] = Counter()
    evidence_statuses: Counter[str] = Counter()
    positive_count = 0
    for row in _rows(certification, "sleeves"):
        sample_statuses[str(row.get("sample_status") or "UNKNOWN")] += 1
        evidence_statuses[str(row.get("evidence_status") or "UNKNOWN")] += 1
        if str(row.get("positive_evidence_status") or row.get("positive_evidence") or "").upper() in {"POSITIVE", "PASS", "TRUE"}:
            positive_count += 1
    return sample_statuses, evidence_statuses, positive_count


def _candidate_metrics(diagnostics: dict[str, Any], contracts: dict[str, Any]) -> dict[str, int]:
    valid_contracts = _as_int(diagnostics.get("valid_candidate_contracts"))
    if valid_contracts == 0:
        candidates = contracts.get("candidate_contracts")
        if isinstance(candidates, list):
            valid_contracts = len([row for row in candidates if isinstance(row, dict)])
    return {
        "raw_signals": _as_int(diagnostics.get("total_raw_signals")),
        "generated_candidates": _as_int(diagnostics.get("total_candidates_generated")),
        "valid_candidate_contracts": valid_contracts,
        "rejected_raw_signals": len(_rows(diagnostics, "rejected_raw_signal_details")) or len(_rows(contracts, "rejected_raw_signals")),
        "pre_contract_suppressed": _as_int(contracts.get("pre_contract_suppressed_count")),
        "rejected_contracts": len(_rows(diagnostics, "rejected_candidate_contracts")) or _as_int(contracts.get("candidates_rejected")),
    }


def _capital_metrics(research_quality: dict[str, Any]) -> tuple[int, int, Counter[str]]:
    summary = research_quality.get("summary") if isinstance(research_quality.get("summary"), dict) else {}
    ready = _as_int(summary.get("ready_for_capital_review_count"))
    statuses: Counter[str] = Counter()
    for row in _rows(research_quality, "hypotheses"):
        status = str(row.get("quality_status") or row.get("decision") or row.get("status") or "UNKNOWN")
        statuses[status] += 1
        if status in {"READY_FOR_CAPITAL_REVIEW", "CAPITAL_REVIEW"}:
            ready += 1
    return ready, _as_int(summary.get("hypothesis_count")), statuses


def _sufficiency_counts(statistical_sufficiency: dict[str, Any]) -> Counter[str]:
    summary = statistical_sufficiency.get("summary") if isinstance(statistical_sufficiency.get("summary"), dict) else {}
    if summary:
        return Counter({str(key).upper(): _as_int(value) for key, value in summary.items() if key != "hypothesis_count"})
    counts: Counter[str] = Counter()
    for row in _rows(statistical_sufficiency, "hypotheses"):
        counts[str(row.get("sufficiency_state") or "UNKNOWN")] += 1
    return counts


def _graph_line(graph: dict[str, Any]) -> str:
    if not graph:
        return "- Verified graph: MISSING."
    status = graph.get("graph_status") or graph.get("status") or "UNKNOWN"
    blocker_count = _as_int(graph.get("audit_blocker_count"))
    return f"- Verified graph: {status}; audit blockers={blocker_count}."


def _counter_table(counter: Counter[str], label: str) -> list[str]:
    lines = [f"| {label} | Count |", "| --- | ---: |"]
    if not counter:
        lines.append("| None | 0 |")
        return lines
    for key, value in sorted(counter.items()):
        lines.append(f"| `{key}` | {value} |")
    return lines


def _sample_distribution_table(by_sleeve: Counter[str], by_hypothesis: Counter[str]) -> list[str]:
    lines = ["Included validation samples by sleeve:", "", "| Sleeve | Included samples |", "| --- | ---: |"]
    if by_sleeve:
        for key, value in sorted(by_sleeve.items()):
            lines.append(f"| `{key}` | {value} |")
    else:
        lines.append("| None | 0 |")
    lines.extend(["", "Included validation samples by hypothesis:", "", "| Hypothesis | Included samples |", "| --- | ---: |"])
    if by_hypothesis:
        for key, value in sorted(by_hypothesis.items()):
            lines.append(f"| `{key}` | {value} |")
    else:
        lines.append("| None | 0 |")
    return lines


def build_north_star_metrics(truth_root: Path, journal_root: Path, day: str) -> str:
    sources = source_set(truth_root, journal_root, day)
    validation = _read_json(sources.validation_samples)
    certification = _read_json(sources.sleeve_evidence_certification)
    ledger = _read_json(sources.paper_position_ledger)
    diagnostics = _read_json(sources.candidate_generation_diagnostics)
    contracts = _read_json(sources.candidate_contracts)
    research_quality = _read_json(sources.research_quality_engine)
    sufficiency = _read_json(sources.statistical_sufficiency)
    graph = _read_json(sources.verified_runtime_graph)

    total_samples, included_samples, excluded_samples, included_by_sleeve, included_by_hypothesis = _sample_counts(validation)
    total_positions, open_positions, closed_positions, paper_by_sleeve = _collect_paper_positions(ledger)
    sample_statuses, evidence_statuses, positive_count = _status_counts(certification)
    candidate = _candidate_metrics(diagnostics, contracts)
    ready_for_capital, rq_hypothesis_count, rq_statuses = _capital_metrics(research_quality)
    sufficiency_counts = _sufficiency_counts(sufficiency)

    sample_producing_sleeves = len(included_by_sleeve)
    sample_producing_hypotheses = len(included_by_hypothesis)
    top_sleeve, top_sleeve_count = included_by_sleeve.most_common(1)[0] if included_by_sleeve else ("NONE", 0)
    top_hypothesis, top_hypothesis_count = included_by_hypothesis.most_common(1)[0] if included_by_hypothesis else ("NONE", 0)
    top_paper_sleeve, top_paper_count = paper_by_sleeve.most_common(1)[0] if paper_by_sleeve else ("NONE", 0)

    lines = [
        "# Atlas V1 North Star Metrics",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_NORTH_STAR_METRICS",
        "",
        "This CLI summarizes existing evidence only. It does not validate truth, infer readiness, recommend trades, recommend exits, allocate capital, mutate candidates, modify sleeves, modify validation rules, modify runtime truth, create schemas, or create journal objects.",
        "",
        f"Evidence day: {day}",
        "",
        "# North Star Summary",
        "",
        "Primary metric: Distributed Mature Validation Evidence.",
        f"- Included validation samples: {included_samples}",
        f"- Sample-producing sleeves: {sample_producing_sleeves}",
        f"- Sample-producing hypotheses: {sample_producing_hypotheses}",
        f"- Evidence status counts: " + ", ".join(f"{key}={value}" for key, value in sorted(evidence_statuses.items())) if evidence_statuses else "- Evidence status counts: none",
        f"- Sample status counts: " + ", ".join(f"{key}={value}" for key, value in sorted(sample_statuses.items())) if sample_statuses else "- Sample status counts: none",
        f"- Positive evidence sleeve count: {positive_count}",
        f"- Ready-for-capital-review rows: {ready_for_capital}",
        "- Interpretation: aggregate samples are diagnostic only unless distributed maturity, sufficiency, independence, robustness, and policy evidence support stronger claims.",
        "",
        "# Distributed Mature Validation Evidence",
        "",
        *_sample_distribution_table(included_by_sleeve, included_by_hypothesis),
        "",
        "# Evidence Maturity Status",
        "",
        *_counter_table(sample_statuses, "Sample status"),
        "",
        *_counter_table(evidence_statuses, "Evidence status"),
        "",
        *_counter_table(sufficiency_counts, "Statistical sufficiency state"),
        "",
        f"Research quality hypotheses: {rq_hypothesis_count}",
        *_counter_table(rq_statuses, "Research quality status"),
        "",
        "# Outcome Flow",
        "",
        f"- Paper positions: {total_positions}",
        f"- Open positions: {open_positions}",
        f"- Closed positions: {closed_positions}",
        f"- Paper-position to included-sample conversion: {included_samples} / {total_positions} ({_pct(included_samples, total_positions):.1f}%).",
        f"- Validation sample inclusion: {included_samples} / {total_samples} ({_pct(included_samples, total_samples):.1f}%).",
        f"- Excluded validation samples: {excluded_samples}",
        "",
        "# Candidate-To-Paper Conversion Quality",
        "",
        f"- Raw signals: {candidate['raw_signals']}",
        f"- Generated candidates: {candidate['generated_candidates']}",
        f"- Valid candidate contracts: {candidate['valid_candidate_contracts']}",
        f"- Paper positions: {total_positions}",
        f"- Raw signal to valid contract conversion: {candidate['valid_candidate_contracts']} / {candidate['raw_signals']} ({_pct(candidate['valid_candidate_contracts'], candidate['raw_signals']):.1f}%).",
        f"- Accumulated paper positions versus current valid-contract diagnostic: {total_positions} / {candidate['valid_candidate_contracts']} ({_pct(total_positions, candidate['valid_candidate_contracts']):.1f}%).",
        "- Caveat: paper positions are accumulated ledger inventory, not a same-day valid-contract conversion numerator.",
        f"- Rejected raw signals: {candidate['rejected_raw_signals']}",
        f"- Pre-contract suppressed rows: {candidate['pre_contract_suppressed']}",
        f"- Rejected candidate contracts: {candidate['rejected_contracts']}",
        "- Interpretation: this separates upstream activity from governed contracts and paper observation flow; it is not candidate-quality or trade-readiness evidence.",
        "",
        "# Evidence Concentration",
        "",
        f"- Largest included-sample sleeve: `{top_sleeve}` with {top_sleeve_count} / {included_samples} ({_pct(top_sleeve_count, included_samples):.1f}%).",
        f"- Largest included-sample hypothesis: `{top_hypothesis}` with {top_hypothesis_count} / {included_samples} ({_pct(top_hypothesis_count, included_samples):.1f}%).",
        f"- Largest paper-position sleeve: `{top_paper_sleeve}` with {top_paper_count} / {total_positions} ({_pct(top_paper_count, total_positions):.1f}%).",
        f"- Zero-sample evaluated sleeves: {sample_statuses.get('ZERO_SAMPLE', 0)}",
        "- Interpretation: concentration is evidence distribution only. It is not a penalty, allocation instruction, or sleeve-change instruction.",
        "",
        "# Guardrails",
        "",
        _graph_line(graph),
        f"- Trade advice allowed: {validation.get('trade_advice_allowed', graph.get('trade_advice_allowed', 'UNKNOWN')) if isinstance(graph, dict) else validation.get('trade_advice_allowed', 'UNKNOWN')}",
        f"- Broker execution allowed: {validation.get('broker_execution_allowed', graph.get('broker_execution_allowed', 'UNKNOWN')) if isinstance(graph, dict) else validation.get('broker_execution_allowed', 'UNKNOWN')}",
        f"- Live trading allowed: {validation.get('live_trading_allowed', graph.get('live_trading_allowed', 'UNKNOWN')) if isinstance(graph, dict) else validation.get('live_trading_allowed', 'UNKNOWN')}",
        "- Nonzero capital-review rows are re-review triggers only, not automatic action.",
        "",
        "# Anti-Metrics Boundary",
        "",
        "Raw signals, generated candidates, total paper positions, and generated hypotheses are operational diagnostics. They are not research-health indicators unless they flow into distributed mature validation evidence.",
        "",
        "# Source Paths",
        "",
    ]
    source_paths = _source_paths(sources)
    if source_paths:
        lines.extend(f"- `{source_path}`" for source_path in source_paths)
    else:
        lines.append("None.")
    missing = _missing_source_paths(sources)
    if missing:
        lines.extend(["", "Missing source paths:"])
        lines.extend(f"- `{source_path}`" for source_path in missing)
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Atlas V1 read-only North Star Metrics brief.")
    parser.add_argument("--truth-root", default=str(DEFAULT_TRUTH_ROOT), help="Aegis truth root directory.")
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    parser.add_argument("--day", default=DEFAULT_DAY, help="Evidence day in YYYY-MM-DD format.")
    args = parser.parse_args()
    print(build_north_star_metrics(Path(args.truth_root), Path(args.journal_root), args.day))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
