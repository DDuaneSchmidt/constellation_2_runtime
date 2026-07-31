from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_ROOT_NAME = "replay_sample_yield"
SOURCE_REPORT_NAME = "candidate_backtests"
LOW_SAMPLE_THRESHOLD = 30

AUTHORITY_BOUNDARY = {
    "read_only_measurement": True,
    "replay_behavior_changes_allowed": False,
    "replay_override_allowed": False,
    "qualification_override_allowed": False,
    "candidate_promotion_allowed": False,
    "governance_override_allowed": False,
    "live_trading_allowed": False,
    "broker_execution_allowed": False,
    "capital_authority_allowed": False,
    "position_sizing_allowed": False,
    "automatic_paper_trade_placement_allowed": False,
}


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_replay_sample_yield(
    root: Path | str = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    low_sample_threshold: int = LOW_SAMPLE_THRESHOLD,
) -> dict[str, Any]:
    report = build_replay_sample_yield_report(
        root=Path(root),
        created_at=created_at,
        low_sample_threshold=low_sample_threshold,
    )
    write_replay_sample_yield_report(report, root=Path(root))
    return report


def build_replay_sample_yield_report(
    root: Path | str = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    low_sample_threshold: int = LOW_SAMPLE_THRESHOLD,
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or now_utc()
    source_path = root_path / SOURCE_REPORT_NAME / "latest.json"
    source_report = _read_json(source_path)
    candidates = list(source_report.get("candidates") or [])

    candidate_rows = [_candidate_sample_row(candidate, low_sample_threshold=low_sample_threshold) for candidate in candidates]
    raw_candidate_count = len(candidate_rows)
    trigger_sample_count = sum(int(row["trigger_sample_count"]) for row in candidate_rows)
    post_filter_sample_count = sum(int(row["post_filter_sample_count"]) for row in candidate_rows)

    attrition_counts = {
        "regime_filter": sum(int(row["attrition_by_filter"]["regime_filter"]["count"]) for row in candidate_rows),
        "invalidations": sum(int(row["attrition_by_filter"]["invalidations"]["count"]) for row in candidate_rows),
    }
    attrition_counts["other_or_unclassified"] = max(
        trigger_sample_count - post_filter_sample_count - attrition_counts["regime_filter"] - attrition_counts["invalidations"],
        0,
    )
    attrition_by_filter = {
        name: {
            "count": count,
            "percent_of_trigger_samples": _ratio(count, trigger_sample_count),
        }
        for name, count in attrition_counts.items()
    }
    zero_sample_candidates = [
        _candidate_sample_summary(row)
        for row in candidate_rows
        if int(row["post_filter_sample_count"]) == 0
    ]
    low_sample_candidates = [
        _candidate_sample_summary(row)
        for row in candidate_rows
        if 0 < int(row["post_filter_sample_count"]) < low_sample_threshold
    ]
    summary = {
        "raw_candidate_count": raw_candidate_count,
        "trigger_sample_count": trigger_sample_count,
        "post_filter_sample_count": post_filter_sample_count,
        "replay_sample_yield": _ratio(post_filter_sample_count, trigger_sample_count),
        "zero_sample_candidate_count": len(zero_sample_candidates),
        "low_sample_candidate_count": len(low_sample_candidates),
        "low_sample_threshold": low_sample_threshold,
        "source_report_available": source_path.exists(),
    }

    return {
        "report_type": "REPLAY_SAMPLE_YIELD",
        "version": "1.0",
        "created_at": created,
        "day": created[:10],
        "research_only": True,
        "read_only": True,
        "source_reports": {
            SOURCE_REPORT_NAME: {
                "path": str(source_path),
                "exists": source_path.exists(),
                "created_at": source_report.get("created_at"),
                "report_type": source_report.get("report_type"),
            }
        },
        "raw_candidate_count": raw_candidate_count,
        "trigger_sample_count": trigger_sample_count,
        "post_filter_sample_count": post_filter_sample_count,
        "replay_sample_yield": summary["replay_sample_yield"],
        "attrition_by_filter": attrition_by_filter,
        "zero_sample_candidates": zero_sample_candidates,
        "low_sample_candidates": low_sample_candidates,
        "candidate_sample_yield": candidate_rows,
        "summary": summary,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Read-only measurement from existing candidate_backtests/latest.json.",
            "No replay behavior changes.",
            "No candidate promotion, qualification override, governance override, live trading, broker execution, capital authority, or position sizing authority.",
        ],
    }


def write_replay_sample_yield_report(report: dict[str, Any], root: Path | str = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root)
    report_root = root_path / REPORT_ROOT_NAME
    day_root = report_root / str(report.get("day") or now_utc()[:10])
    day_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    latest_json = report_root / "latest.json"
    latest_summary = report_root / "latest_summary.md"
    dated_json = day_root / "replay_sample_yield.json"
    dated_summary = day_root / "replay_sample_yield_summary.md"
    summary_text = render_replay_sample_yield_summary(report)

    for path in (latest_json, dated_json):
        path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for path in (latest_summary, dated_summary):
        path.write_text(summary_text, encoding="utf-8")

    return {
        "latest_json": latest_json,
        "latest_summary": latest_summary,
        "dated_json": dated_json,
        "dated_summary": dated_summary,
    }


def render_replay_sample_yield_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    attrition = report.get("attrition_by_filter") or {}
    lines = [
        "# Replay Sample Yield",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Metrics",
        "",
        f"- raw_candidate_count: {summary.get('raw_candidate_count', 0)}",
        f"- trigger_sample_count: {summary.get('trigger_sample_count', 0)}",
        f"- post_filter_sample_count: {summary.get('post_filter_sample_count', 0)}",
        f"- replay_sample_yield: {summary.get('replay_sample_yield', 0)}",
        f"- zero_sample_candidate_count: {summary.get('zero_sample_candidate_count', 0)}",
        f"- low_sample_candidate_count: {summary.get('low_sample_candidate_count', 0)}",
        f"- low_sample_threshold: {summary.get('low_sample_threshold', LOW_SAMPLE_THRESHOLD)}",
        "",
        "## Attrition By Filter",
        "",
    ]
    for name, payload in attrition.items():
        lines.append(f"- {name}: {payload.get('count', 0)} ({payload.get('percent_of_trigger_samples', 0)} of trigger samples)")

    lines.extend(["", "## Zero Sample Candidates", ""])
    zero_candidates = report.get("zero_sample_candidates") or []
    if zero_candidates:
        for candidate in zero_candidates:
            lines.append(_format_candidate_line(candidate))
    else:
        lines.append("- none")

    lines.extend(["", "## Low Sample Candidates", ""])
    low_candidates = report.get("low_sample_candidates") or []
    if low_candidates:
        for candidate in low_candidates:
            lines.append(_format_candidate_line(candidate))
    else:
        lines.append("- none")

    lines.extend(["", "## Candidate Sample Yield", ""])
    for row in report.get("candidate_sample_yield") or []:
        lines.append(
            "- "
            f"{row.get('candidate_id')} | {row.get('mechanism')} | "
            f"trigger={row.get('trigger_sample_count')} | "
            f"post_filter={row.get('post_filter_sample_count')} | "
            f"yield={row.get('replay_sample_yield')} | "
            f"classification={row.get('classification')}"
        )

    lines.extend(
        [
            "",
            "## Authority Boundary",
            "",
            "- Read-only measurement only.",
            "- No replay behavior changes.",
            "- No candidate promotion, qualification override, governance override, live trading, broker execution, capital authority, or position sizing authority.",
            "",
        ]
    )
    return "\n".join(lines)


def _candidate_sample_row(candidate: dict[str, Any], *, low_sample_threshold: int) -> dict[str, Any]:
    metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    trigger_count = _int(metrics.get("trigger_count"))
    post_filter_count = _int(metrics.get("sample_size") if metrics.get("sample_size") is not None else candidate.get("sample_size"))
    regime_filtered_count = _int(metrics.get("regime_filtered_count"))
    invalidations = _int(metrics.get("invalidations"))
    other_attrition = max(trigger_count - post_filter_count - regime_filtered_count - invalidations, 0)
    return {
        "candidate_id": str(candidate.get("candidate_id") or "unknown-candidate"),
        "mechanism": str(candidate.get("mechanism") or (candidate.get("backtest_spec") or {}).get("mechanism") or "UNKNOWN"),
        "classification": str(candidate.get("classification") or "UNKNOWN"),
        "trigger_sample_count": trigger_count,
        "post_filter_sample_count": post_filter_count,
        "replay_sample_yield": _ratio(post_filter_count, trigger_count),
        "attrition_by_filter": {
            "regime_filter": {"count": regime_filtered_count, "percent_of_trigger_samples": _ratio(regime_filtered_count, trigger_count)},
            "invalidations": {"count": invalidations, "percent_of_trigger_samples": _ratio(invalidations, trigger_count)},
            "other_or_unclassified": {"count": other_attrition, "percent_of_trigger_samples": _ratio(other_attrition, trigger_count)},
        },
        "zero_sample": post_filter_count == 0,
        "low_sample": 0 < post_filter_count < low_sample_threshold,
        "warnings": list(candidate.get("warnings") or []),
        "missing_data": list(candidate.get("missing_data") or []),
    }


def _candidate_sample_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "mechanism": row.get("mechanism"),
        "classification": row.get("classification"),
        "trigger_sample_count": row.get("trigger_sample_count"),
        "post_filter_sample_count": row.get("post_filter_sample_count"),
        "replay_sample_yield": row.get("replay_sample_yield"),
        "warnings": row.get("warnings", []),
        "missing_data": row.get("missing_data", []),
    }


def _format_candidate_line(candidate: dict[str, Any]) -> str:
    return (
        "- "
        f"{candidate.get('candidate_id')} | {candidate.get('mechanism')} | "
        f"trigger={candidate.get('trigger_sample_count')} | "
        f"post_filter={candidate.get('post_filter_sample_count')} | "
        f"yield={candidate.get('replay_sample_yield')}"
    )


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _ratio(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
