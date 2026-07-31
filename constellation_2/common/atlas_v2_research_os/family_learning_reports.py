from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .family_learning_engine import REPORT_DIRNAME, build_family_learning_report
from .family_learning_governance import validate_family_learning_allowed


def write_family_learning_report(report: dict[str, Any] | None = None, root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root)
    payload = report or build_family_learning_report(root=root_path)
    validate_family_learning_allowed(payload)
    report_root = root_path / REPORT_DIRNAME
    day_root = report_root / str(payload.get("day") or _today())
    day_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    dated_json = day_root / "family_learning_report.json"
    dated_summary = day_root / "family_learning_summary.md"
    latest_json = report_root / "latest.json"
    latest_summary = report_root / "latest_summary.md"
    json_text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    summary_text = render_family_learning_summary(payload)
    for path in (dated_json, latest_json):
        path.write_text(json_text, encoding="utf-8")
    for path in (dated_summary, latest_summary):
        path.write_text(summary_text, encoding="utf-8")
    return {"json": dated_json, "summary": dated_summary, "latest_json": latest_json, "latest_summary": latest_summary}


def render_family_learning_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    rows = report.get("family_learning_updates") or []
    lines = [
        "# Family Learning Report",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Summary",
        "",
        f"- Families processed: {summary.get('families_processed', 0)}",
        f"- Families strengthened: {summary.get('families_strengthened', 0)}",
        f"- Families weakened: {summary.get('families_weakened', 0)}",
        f"- Families needing data: {summary.get('families_needing_data', 0)}",
        f"- Families needing more observations: {summary.get('families_needing_more_observations', 0)}",
        f"- Families retired: {summary.get('families_retired', 0)}",
        f"- Total observations logged: {summary.get('total_observations_logged', 0)}",
        "",
        "## Family Updates",
        "",
        "| family_id | prior | sample | support | invalid | confidence_before | confidence_after | status | next evidence |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {family_id} | {prior_classification} | {sample_size} | {supporting_observations} | {invalidating_observations} | {confidence_before} | {confidence_after} | {status_after_update} | {next_required_evidence} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Research-Only Authority",
            "",
            "- Family confidence update only.",
            "- Research memory update proposal only.",
            "- Observation and retirement recommendations only.",
            "- No trade recommendation.",
            "- No capital allocation.",
            "- No position sizing.",
            "- No broker execution.",
            "- No automatic paper placement.",
            "- No candidate production promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def audit_family_learning_report(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    failures: list[str] = []
    root_path = Path(root) / REPORT_DIRNAME
    for path in root_path.rglob("family_learning_report.json") if root_path.exists() else []:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            validate_family_learning_allowed(payload)
            for row in payload.get("family_learning_updates") or []:
                required = {
                    "family_id",
                    "family_name",
                    "prior_classification",
                    "observations_logged",
                    "supporting_observations",
                    "invalidating_observations",
                    "neutral_observations",
                    "sample_size",
                    "confidence_before",
                    "confidence_after",
                    "confidence_delta",
                    "status_after_update",
                    "reason_for_update",
                    "next_required_evidence",
                }
                missing = sorted(required - set(row))
                if missing:
                    raise ValueError(f"missing update fields: {missing}")
        except Exception as exc:
            failures.append(f"{path.as_posix()}: {exc}")
    return {"family_learning_report_audit_ok": not failures, "family_learning_report_audit_failures": failures}


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
