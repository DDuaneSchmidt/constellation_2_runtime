from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .failure_observatory import failures_root, get_repeated_failures, list_failures
from .failure_observatory_governance import validate_failure_observatory_no_forbidden_recommendations
from .failure_observatory_models import FailureComponent, FailureSeverity, FailureSummary
from .regime_expansion import regime_counts, normalize_regime


def summarize_failures_for_day(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    all_rows = list_failures(root)
    day_value = day or _default_summary_day(all_rows)
    rows = [row for row in all_rows if _day_from_timestamp(str(row.get("timestamp") or "")) == day_value]
    component_counts = Counter(str(row.get("component") or FailureComponent.UNKNOWN.value) for row in rows)
    recommendations = _recommendations(rows)
    gate = validate_failure_observatory_no_forbidden_recommendations(recommendations)
    if gate["status"] != "PASS":
        raise ValueError(f"forbidden failure summary recommendation: {gate['failures']}")
    failures_by_regime = regime_counts([{"regime": _failure_regime(row)} for row in rows])
    summary = FailureSummary(
        day=day_value,
        total_failures=len(rows),
        critical_failures=sum(1 for row in rows if row.get("severity") == FailureSeverity.CRITICAL.value),
        governance_blocks=sum(1 for row in rows if row.get("severity") == FailureSeverity.GOVERNANCE_BLOCK.value),
        certification_blocks=sum(1 for row in rows if row.get("severity") == FailureSeverity.CERTIFICATION_BLOCK.value),
        worker_failures=sum(1 for row in rows if row.get("component") == FailureComponent.WORKER.value),
        scheduler_failures=sum(1 for row in rows if row.get("component") == FailureComponent.SCHEDULER.value),
        repeated_failure_types=[row for row in get_repeated_failures(root) if _trend_touches_day(row, rows)],
        unresolved_failures=[_brief(row) for row in rows if not row.get("resolved")],
        resolved_failures=[_brief(row) for row in rows if row.get("resolved")],
        top_components_by_failure_count=[{"component": component, "count": count} for component, count in component_counts.most_common()],
        recommended_human_review_items=recommendations,
    )
    payload = summary.to_dict()
    payload["failures_by_regime"] = failures_by_regime
    return payload


def write_failure_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    all_rows = list_failures(root)
    day_value = day or _default_summary_day(all_rows)
    summary = summarize_failures_for_day(root, day=day_value)
    rows = [row for row in all_rows if _day_from_timestamp(str(row.get("timestamp") or "")) == day_value]
    report = {
        "schema_id": "atlas_v2_research_os_failure_report_v1",
        "schema_version": "v1",
        "day": day_value,
        "summary": summary,
        "failures": rows,
    }
    out_dir = failures_root(root) / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "failure_report.json"
    summary_path = out_dir / "failure_summary.md"
    latest_path = failures_root(root) / "latest.json"
    latest_summary_path = failures_root(root) / "latest_summary.md"
    _write_json(report_path, report)
    summary_md = render_failure_summary(summary)
    summary_path.write_text(summary_md, encoding="utf-8")
    _write_json(latest_path, report)
    latest_summary_path.write_text(summary_md, encoding="utf-8")
    return {"json": report_path, "summary": summary_path, "latest": latest_path, "latest_summary": latest_summary_path}


def render_failure_summary(summary: dict[str, Any]) -> str:
    lines = [
        "# Atlas V2 Research OS Failure Summary",
        "",
        f"Day: {summary['day']}",
        f"Total failures: {summary['total_failures']}",
        f"Critical failures: {summary['critical_failures']}",
        f"Governance blocks: {summary['governance_blocks']}",
        f"Certification blocks: {summary['certification_blocks']}",
        f"Worker failures: {summary['worker_failures']}",
        f"Scheduler failures: {summary['scheduler_failures']}",
        f"Failures by regime: {summary.get('failures_by_regime', {})}",
        "",
        "## Repeated Failure Types",
    ]
    lines.extend(_json_lines(summary.get("repeated_failure_types") or []))
    lines.extend(["", "## Unresolved Failures"])
    lines.extend(_json_lines(summary.get("unresolved_failures") or []))
    lines.extend(["", "## Resolved Failures"])
    lines.extend(_json_lines(summary.get("resolved_failures") or []))
    lines.extend(["", "## Top Components"])
    lines.extend(_json_lines(summary.get("top_components_by_failure_count") or []))
    lines.extend(["", "## Recommended Human Review Items"])
    recommendations = list(summary.get("recommended_human_review_items") or [])
    lines.extend([f"- {item}" for item in recommendations] or ["- None"])
    lines.append("")
    return "\n".join(lines)


def _failure_regime(row: dict[str, Any]) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    return normalize_regime(
        metadata.get("regime")
        or metadata.get("regime_context")
        or metadata.get("regime_label")
        or row.get("regime")
    )


def _recommendations(rows: list[dict[str, Any]]) -> list[str]:
    recommendations: list[str] = []
    if any(row.get("component") == FailureComponent.WORKER.value for row in rows):
        recommendations.append("Review failed worker run.")
    if any(row.get("backlog_item_id") for row in rows):
        recommendations.append("Inspect blocked backlog item.")
    if any(row.get("component") == FailureComponent.LINEAGE.value for row in rows) or any(row.get("error_type") == "LINEAGE_VALIDATION_FAILED" for row in rows):
        recommendations.append("Check repeated lineage failure.")
    if any(row.get("severity") == FailureSeverity.GOVERNANCE_BLOCK.value for row in rows):
        recommendations.append("Review governance block.")
    if any("missing" in str(row.get("error_message") or "").lower() or "dependency" in str(row.get("error_message") or "").lower() for row in rows):
        recommendations.append("Rerun after fixing missing dependency.")
    return list(dict.fromkeys(recommendations))


def _brief(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "failure_id": row.get("failure_id"),
        "component": row.get("component"),
        "severity": row.get("severity"),
        "error_type": row.get("error_type"),
        "backlog_item_id": row.get("backlog_item_id", ""),
        "worker_run_id": row.get("worker_run_id", ""),
        "scheduler_trigger_id": row.get("scheduler_trigger_id", ""),
    }


def _trend_touches_day(trend: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    ids = {str(row.get("failure_id") or "") for row in rows}
    return bool(ids.intersection(set(trend.get("failure_ids") or [])))


def _json_lines(rows: list[Any]) -> list[str]:
    return [f"- `{json.dumps(row, sort_keys=True)}`" for row in rows] or ["- None"]


def _default_summary_day(rows: list[dict[str, Any]]) -> str:
    if rows:
        latest = max((_day_from_timestamp(str(row.get("timestamp") or "")) for row in rows), default="")
        if latest:
            return latest
    return datetime.now(UTC).date().isoformat()


def _day_from_timestamp(value: str) -> str:
    if len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
        return value[:10]
    return datetime.now(UTC).date().isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
