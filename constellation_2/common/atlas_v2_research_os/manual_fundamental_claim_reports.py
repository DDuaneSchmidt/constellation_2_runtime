from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "manual_fundamental_claims/pegy_ratio"


def write_manual_pegy_claim_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    return _write_report(
        report,
        root=root,
        json_name="pegy_manual_claim.json",
        summary_name="pegy_manual_claim_summary.md",
        summary_text=render_manual_pegy_claim_summary(report),
    )


def write_pegy_ratio_metric_spec_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    return _write_report(
        report,
        root=root,
        json_name="pegy_ratio_metric_spec.json",
        summary_name="pegy_ratio_metric_spec_summary.md",
        summary_text=render_pegy_ratio_metric_spec_summary(report),
    )


def render_manual_pegy_claim_summary(report: dict[str, Any]) -> str:
    claim = report.get("manual_claim") or {}
    audit = report.get("data_availability_audit") or {}
    backlog = report.get("research_backlog_item") or {}
    lines = [
        "# PEGY Manual Fundamental Claim Intake",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Claim",
        "",
        f"- Claim name: {claim.get('claim_name')}",
        f"- Claim type: {claim.get('claim_type')}",
        f"- Mechanism family: {claim.get('mechanism_family')}",
        f"- Status: {claim.get('claim_status')}",
        "",
        "## Research Framing",
        "",
        f"- {claim.get('research_framing')}",
        "",
        "## Data Availability Audit",
        "",
    ]
    for item in audit.get("questions", []):
        lines.append(f"- {item.get('question')} {item.get('answer')} ({item.get('classification')})")
    lines.extend(
        [
            "",
            "## Backlog",
            "",
            f"- Backlog item: {backlog.get('backlog_item_id')}",
            f"- Type: {backlog.get('item_type')}",
            f"- Source: {(backlog.get('metadata') or {}).get('source')}",
            f"- State: {backlog.get('state')}",
            "",
            "## Guardrails",
            "",
            "- Research-only. No live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper placement, candidate promotion, or production promotion.",
            "- PEGY is not labeled as an edge until tested with appropriate point-in-time data.",
            "- Lookahead and survivorship-bias risks are explicit blockers until data proves otherwise.",
            "",
        ]
    )
    return "\n".join(lines)


def render_pegy_ratio_metric_spec_summary(report: dict[str, Any]) -> str:
    spec = report.get("metric_spec") or {}
    return "\n".join(
        [
            "# PEGY Ratio Metric Spec",
            "",
            f"Created: {report.get('created_at')}",
            "",
            "## Metric",
            "",
            f"- Metric name: {spec.get('metric_name')}",
            f"- Formula: {spec.get('formula')}",
            f"- Data gate: {report.get('data_gate')}",
            "",
            "## Invalid Case Policy",
            "",
            *[f"- {name}: {policy}" for name, policy in (report.get("invalid_case_policy") or {}).items()],
            "",
            "## Guardrails",
            "",
            "- Specification only until sufficient point-in-time fundamentals exist.",
            "- Do not use today's fundamentals to test past returns.",
            "- No live trading, broker execution, capital allocation, position sizing, trade recommendations, candidate promotion, or production promotion.",
            "",
        ]
    )


def _write_report(
    report: dict[str, Any],
    *,
    root: str | Path,
    json_name: str,
    summary_name: str,
    summary_text: str,
) -> dict[str, Path]:
    report_root = Path(root) / REPORT_DIRNAME
    day_root = report_root / str(report.get("day") or _today())
    day_root.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    dated_json = day_root / json_name
    dated_summary = day_root / summary_name
    latest_json = report_root / "latest.json"
    latest_summary = report_root / "latest_summary.md"
    for path in (dated_json, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (dated_summary, latest_summary):
        path.write_text(summary_text, encoding="utf-8")
    return {"json": dated_json, "summary": dated_summary, "latest_json": latest_json, "latest_summary": latest_summary}


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
