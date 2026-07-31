from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from ops.tools.build_atlas_v1_outcome_follow_through_comparator import (
        DEFAULT_JOURNAL_ROOT,
        DEFAULT_TRUTH_ROOT,
        DistributionStats,
        SleeveOutcomeFlow,
        _display_path,
        collect_sleeve_flows,
        distribution_stats,
        source_set,
    )
except ModuleNotFoundError:
    from build_atlas_v1_outcome_follow_through_comparator import (
        DEFAULT_JOURNAL_ROOT,
        DEFAULT_TRUTH_ROOT,
        DistributionStats,
        SleeveOutcomeFlow,
        _display_path,
        collect_sleeve_flows,
        distribution_stats,
        source_set,
    )


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FROM_DAY = os.environ.get("FROM_DAY", "2026-06-03")
DEFAULT_TO_DAY = os.environ.get("TARGET_DAY", "2026-06-04")
REQUIRED_ARTIFACT_LABELS = (
    "paper_position_ledger",
    "sleeve_performance_truth",
    "validation_samples",
    "sleeve_evidence_certification",
)


@dataclass(frozen=True)
class DayValidity:
    day: str
    status: str
    graph_status: str
    graph_path: Path
    blockers: tuple[str, ...]
    missing_paths: tuple[Path, ...]
    source_paths: tuple[Path, ...]

    @property
    def assessable(self) -> bool:
        return self.status == "ASSESSABLE"


@dataclass(frozen=True)
class DaySnapshot:
    validity: DayValidity
    flows: tuple[SleeveOutcomeFlow, ...]
    distribution: DistributionStats


def _verified_graph_path(truth_root: Path, day: str) -> Path:
    return truth_root / "reports" / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _required_artifact_paths(truth_root: Path, journal_root: Path, day: str) -> dict[str, Path]:
    sources = source_set(truth_root, journal_root, day)
    return {
        "paper_position_ledger": sources.paper_position_ledger,
        "sleeve_performance_truth": sources.sleeve_performance_truth,
        "validation_samples": sources.validation_samples,
        "sleeve_evidence_certification": sources.sleeve_evidence_certification,
    }


def _extract_blockers(graph: dict[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    for key in ("capabilities", "verified_capabilities", "runtime_graph", "modules"):
        value = graph.get(key)
        if not isinstance(value, list):
            continue
        for row in value:
            if not isinstance(row, dict):
                continue
            row_blockers = row.get("blockers")
            if not isinstance(row_blockers, list) or not row_blockers:
                continue
            module = str(row.get("module_id") or "")
            capability = str(row.get("capability_id") or row.get("capability") or "")
            prefix = ":".join(part for part in (module, capability) if part)
            for blocker in row_blockers:
                text = str(blocker)
                blockers.append(f"{prefix}:{text}" if prefix else text)
    audit_blocker_count = graph.get("audit_blocker_count")
    if audit_blocker_count not in (None, 0, "0"):
        blockers.insert(0, f"audit_blocker_count:{audit_blocker_count}")
    return tuple(dict.fromkeys(blockers))


def evaluate_day_validity(truth_root: Path, journal_root: Path, day: str) -> DayValidity:
    graph_path = _verified_graph_path(truth_root, day)
    graph = _read_json(graph_path)
    required_paths = _required_artifact_paths(truth_root, journal_root, day)
    missing_paths = tuple(path for path in required_paths.values() if not path.exists())
    source_paths = [graph_path] if graph_path.exists() else []
    source_paths.extend(path for path in required_paths.values() if path.exists())

    if not graph_path.exists():
        return DayValidity(
            day=day,
            status="NOT_ASSESSABLE_MISSING_VERIFIED_GRAPH",
            graph_status="MISSING",
            graph_path=graph_path,
            blockers=("verified runtime graph missing",),
            missing_paths=(graph_path,) + missing_paths,
            source_paths=tuple(source_paths),
        )

    graph_status = str(graph.get("graph_status") or "UNKNOWN")
    blockers = _extract_blockers(graph)
    if graph_status != "READY":
        return DayValidity(
            day=day,
            status="NOT_ASSESSABLE_BLOCKED",
            graph_status=graph_status,
            graph_path=graph_path,
            blockers=blockers or (f"graph_status:{graph_status}",),
            missing_paths=missing_paths,
            source_paths=tuple(source_paths),
        )

    if missing_paths:
        return DayValidity(
            day=day,
            status="NOT_ASSESSABLE_MISSING_REQUIRED_ARTIFACTS",
            graph_status=graph_status,
            graph_path=graph_path,
            blockers=("required evidence artifact missing",),
            missing_paths=missing_paths,
            source_paths=tuple(source_paths),
        )

    return DayValidity(
        day=day,
        status="ASSESSABLE",
        graph_status=graph_status,
        graph_path=graph_path,
        blockers=(),
        missing_paths=(),
        source_paths=tuple(source_paths),
    )


def build_snapshot(truth_root: Path, journal_root: Path, day: str) -> DaySnapshot:
    validity = evaluate_day_validity(truth_root, journal_root, day)
    flows = collect_sleeve_flows(source_set(truth_root, journal_root, day)) if validity.assessable else ()
    return DaySnapshot(validity=validity, flows=flows, distribution=distribution_stats(flows))


def _by_sleeve(flows: tuple[SleeveOutcomeFlow, ...]) -> dict[str, SleeveOutcomeFlow]:
    return {flow.sleeve_id: flow for flow in flows}


def _delta(current: int, previous: int) -> str:
    value = current - previous
    if value > 0:
        return f"+{value}"
    return str(value)


def _total_open(flows: tuple[SleeveOutcomeFlow, ...]) -> int:
    return sum(flow.open_positions for flow in flows)


def _total_closed(flows: tuple[SleeveOutcomeFlow, ...]) -> int:
    return sum(flow.closed_positions for flow in flows)


def _zero_sample_sleeves(flows: tuple[SleeveOutcomeFlow, ...]) -> tuple[str, ...]:
    return tuple(sorted(flow.sleeve_id for flow in flows if flow.paper_observations > 0 and flow.included_validation_samples == 0))


def _first_sample_changes(from_flows: tuple[SleeveOutcomeFlow, ...], to_flows: tuple[SleeveOutcomeFlow, ...]) -> tuple[str, ...]:
    from_by = _by_sleeve(from_flows)
    changes: list[str] = []
    for flow in sorted(to_flows, key=lambda item: item.sleeve_id):
        previous = from_by.get(flow.sleeve_id)
        previous_samples = previous.included_validation_samples if previous else 0
        if previous_samples == 0 and flow.included_validation_samples > 0:
            changes.append(flow.sleeve_id)
    return tuple(changes)


def _stalled_paths(flows: tuple[SleeveOutcomeFlow, ...]) -> tuple[str, ...]:
    return tuple(
        sorted(
            flow.sleeve_id
            for flow in flows
            if flow.paper_observations > 0 and flow.included_validation_samples == 0
        )
    )


def _all_source_paths(snapshots: tuple[DaySnapshot, ...]) -> tuple[str, ...]:
    paths: list[str] = []
    for snapshot in snapshots:
        for path in snapshot.validity.source_paths:
            display = _display_path(path)
            if display not in paths:
                paths.append(display)
    return tuple(paths)


def _all_missing_paths(snapshots: tuple[DaySnapshot, ...]) -> tuple[str, ...]:
    paths: list[str] = []
    for snapshot in snapshots:
        for path in snapshot.validity.missing_paths:
            display = _display_path(path)
            if display not in paths:
                paths.append(display)
    return tuple(paths)


def _render_validity(lines: list[str], snapshot: DaySnapshot) -> None:
    validity = snapshot.validity
    lines.append(
        f"- `{validity.day}`: {validity.status}; verified graph status `{validity.graph_status}`; graph `{_display_path(validity.graph_path)}`."
    )
    if validity.blockers:
        for blocker in validity.blockers[:8]:
            lines.append(f"  - blocker: {blocker}")
    if validity.missing_paths:
        for path in validity.missing_paths[:8]:
            lines.append(f"  - missing: `{_display_path(path)}`")


def render_markdown(from_snapshot: DaySnapshot, to_snapshot: DaySnapshot) -> str:
    both_assessable = from_snapshot.validity.assessable and to_snapshot.validity.assessable
    lines = [
        "# Atlas V1 Evidence Refresh Diff",
        "",
        "Status: NON_AUTHORITATIVE_READ_ONLY_EVIDENCE_REFRESH_DIFF",
        "",
        "This brief compares existing evidence-flow artifacts only. It does not infer trading readiness, recommend trades, recommend exits, recommend allocation, modify sleeves, modify rules, create journal objects, or create reports.",
        "",
        f"From day: {from_snapshot.validity.day}",
        f"To day: {to_snapshot.validity.day}",
        "",
        "# Evidence Refresh Diff Summary",
        "",
    ]

    if not both_assessable:
        lines.extend(
            [
                "Diff status: NOT_ASSESSABLE.",
                "One or both days failed the conservative validity check. Count deltas are not assessed.",
                "",
            ]
        )
    else:
        from_open = _total_open(from_snapshot.flows)
        to_open = _total_open(to_snapshot.flows)
        from_closed = _total_closed(from_snapshot.flows)
        to_closed = _total_closed(to_snapshot.flows)
        from_included = from_snapshot.distribution.total_included_samples
        to_included = to_snapshot.distribution.total_included_samples
        lines.extend(
            [
                "Diff status: ASSESSABLE.",
                f"- Open positions: {from_open} -> {to_open} ({_delta(to_open, from_open)}).",
                f"- Closed positions: {from_closed} -> {to_closed} ({_delta(to_closed, from_closed)}).",
                f"- Included validation samples: {from_included} -> {to_included} ({_delta(to_included, from_included)}).",
                "",
            ]
        )

    lines.extend(["# Day Validity", ""])
    _render_validity(lines, from_snapshot)
    _render_validity(lines, to_snapshot)
    lines.append("")

    lines.extend(["# Sample Count Changes", ""])
    if not both_assessable:
        lines.append("NOT_ASSESSABLE because at least one day is missing, blocked, stale, or otherwise not assessable.")
    else:
        from_by = _by_sleeve(from_snapshot.flows)
        to_by = _by_sleeve(to_snapshot.flows)
        sleeve_ids = tuple(sorted(set(from_by) | set(to_by)))
        lines.append("| Sleeve | Open delta | Closed delta | Included sample delta | From included | To included |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for sleeve_id in sleeve_ids:
            before = from_by.get(sleeve_id)
            after = to_by.get(sleeve_id)
            before_open = before.open_positions if before else 0
            after_open = after.open_positions if after else 0
            before_closed = before.closed_positions if before else 0
            after_closed = after.closed_positions if after else 0
            before_samples = before.included_validation_samples if before else 0
            after_samples = after.included_validation_samples if after else 0
            lines.append(
                f"| `{sleeve_id}` | {_delta(after_open, before_open)} | {_delta(after_closed, before_closed)} | {_delta(after_samples, before_samples)} | {before_samples} | {after_samples} |"
            )
    lines.append("")

    lines.extend(["# Evidence Concentration Changes", ""])
    if not both_assessable:
        lines.append("NOT_ASSESSABLE because concentration deltas require two assessable days.")
    else:
        before = from_snapshot.distribution
        after = to_snapshot.distribution
        lines.extend(
            [
                f"- Top sleeve: `{before.top_sleeve_id}` -> `{after.top_sleeve_id}`.",
                f"- Top sleeve share: {before.top_sleeve_share_pct:.1f}% -> {after.top_sleeve_share_pct:.1f}%.",
                f"- Sample-producing sleeves: {before.sample_producing_sleeves} -> {after.sample_producing_sleeves} ({_delta(after.sample_producing_sleeves, before.sample_producing_sleeves)}).",
                f"- Zero-sample sleeves: {len(_zero_sample_sleeves(from_snapshot.flows))} -> {len(_zero_sample_sleeves(to_snapshot.flows))} ({_delta(len(_zero_sample_sleeves(to_snapshot.flows)), len(_zero_sample_sleeves(from_snapshot.flows)))}).",
            ]
        )
    lines.append("")

    lines.extend(["# First-Sample Changes", ""])
    if not both_assessable:
        lines.append("NOT_ASSESSABLE because first-sample changes require two assessable days.")
    else:
        first_samples = _first_sample_changes(from_snapshot.flows, to_snapshot.flows)
        if first_samples:
            for sleeve_id in first_samples:
                lines.append(f"- `{sleeve_id}` moved from zero included samples to one or more included samples.")
        else:
            lines.append("No first-sample sleeve changes found.")
    lines.append("")

    lines.extend(["# Stalled Path Changes", ""])
    if not both_assessable:
        lines.append("NOT_ASSESSABLE because stalled-path deltas require two assessable days.")
    else:
        before = set(_stalled_paths(from_snapshot.flows))
        after = set(_stalled_paths(to_snapshot.flows))
        added = tuple(sorted(after - before))
        removed = tuple(sorted(before - after))
        unchanged = tuple(sorted(before & after))
        lines.append("- Added stalled paths: " + (", ".join(f"`{item}`" for item in added) if added else "None."))
        lines.append("- Removed stalled paths: " + (", ".join(f"`{item}`" for item in removed) if removed else "None."))
        lines.append("- Unchanged stalled paths: " + (", ".join(f"`{item}`" for item in unchanged) if unchanged else "None."))
    lines.append("")

    lines.extend(["# Not Assessable Items", ""])
    not_assessable = tuple(snapshot.validity for snapshot in (from_snapshot, to_snapshot) if not snapshot.validity.assessable)
    if not not_assessable:
        lines.append("None.")
    else:
        for validity in not_assessable:
            lines.append(f"- `{validity.day}`: {validity.status}; graph status `{validity.graph_status}`.")
            for blocker in validity.blockers[:8]:
                lines.append(f"  - blocker: {blocker}")
    lines.append("")

    lines.extend(["# Source Paths", ""])
    source_paths = _all_source_paths((from_snapshot, to_snapshot))
    if source_paths:
        lines.extend(f"- `{path}`" for path in source_paths)
    else:
        lines.append("None.")
    missing_paths = _all_missing_paths((from_snapshot, to_snapshot))
    if missing_paths:
        lines.extend(["", "Missing source paths:"])
        lines.extend(f"- `{path}`" for path in missing_paths)
    lines.append("")
    return "\n".join(lines)


def build_evidence_refresh_diff(truth_root: Path, journal_root: Path, from_day: str, to_day: str) -> str:
    from_snapshot = build_snapshot(truth_root, journal_root, from_day)
    to_snapshot = build_snapshot(truth_root, journal_root, to_day)
    return render_markdown(from_snapshot, to_snapshot)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an Atlas V1 read-only Evidence Refresh Diff brief.")
    parser.add_argument("--truth-root", default=str(DEFAULT_TRUTH_ROOT), help="Aegis truth root directory.")
    parser.add_argument("--journal-root", default=str(DEFAULT_JOURNAL_ROOT), help="Research Journal root directory.")
    parser.add_argument("--from-day", default=DEFAULT_FROM_DAY, help="Baseline artifact day in YYYY-MM-DD format.")
    parser.add_argument("--to-day", default=DEFAULT_TO_DAY, help="Refresh artifact day in YYYY-MM-DD format.")
    args = parser.parse_args()

    print(build_evidence_refresh_diff(Path(args.truth_root), Path(args.journal_root), args.from_day, args.to_day))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
