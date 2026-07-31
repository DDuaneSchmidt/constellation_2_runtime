from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .observation_import_governance import validate_observation_import_allowed

OBSERVATION_IMPORT_REPORT_ROOT = Path("reports/atlas_v2_research_os/observation_import")


def build_observation_import_report(report: dict[str, Any]) -> dict[str, Any]:
    validate_observation_import_allowed(report)
    row = dict(report)
    row.setdefault("schema_id", "atlas_v2_research_os_observation_import_report_v1")
    row.setdefault("schema_version", "v1")
    row.setdefault("day", date.today().isoformat())
    return row


def write_observation_import_report(
    report: dict[str, Any],
    *,
    root: str | Path = OBSERVATION_IMPORT_REPORT_ROOT,
    day: str | None = None,
) -> dict[str, Path]:
    payload_row = build_observation_import_report(report)
    day_value = day or payload_row.get("day") or date.today().isoformat()
    out_root = Path(root)
    out_dir = out_root / str(day_value)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "observation_import_report.json"
    summary_path = out_dir / "observation_import_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(payload_row, indent=2, sort_keys=True) + "\n"
    summary = render_observation_import_summary(payload_row)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def write_latest_observation_import_report(*, root: str | Path = OBSERVATION_IMPORT_REPORT_ROOT) -> dict[str, Path]:
    latest = Path(root) / "latest.json"
    if not latest.exists():
        from .bulk_observation_import import seed_demo_observations
        return {key: Path(value) if isinstance(value, str) else value for key, value in seed_demo_observations()["import_report"].get("report_paths", {}).items()}
    report = json.loads(latest.read_text(encoding="utf-8"))
    return write_observation_import_report(report, root=root, day=report.get("day"))


def render_observation_import_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Atlas Observation Import Summary",
        "",
        f"Raw observations: {report.get('raw_observations', 0)}",
        f"Valid observations: {report.get('valid_observations', 0)}",
        f"Invalid observations: {report.get('invalid_observations', 0)}",
        f"Duplicates skipped: {report.get('duplicates_skipped', 0)}",
        f"Clusters created: {report.get('clusters_created', 0)}",
        f"Claims created: {report.get('claims_created', 0)}",
        f"Backlog items created: {report.get('backlog_items_created', 0)}",
        "",
        "## Claim Seeds",
    ]
    for claim in report.get("claim_seeds", [])[:20]:
        lines.append(f"- {claim['claim_seed_id']} {claim['mechanism']} {claim['regime']}: {claim['claim_text']}")
    lines.extend([
        "",
        "Authority: observation import creates observation records, clusters, claim seeds, and CLAIM_INVESTIGATION backlog items only; no trading, capital, broker, sizing, portfolio, recommendation, paper placement, or promotion authority.",
        "",
    ])
    return "\n".join(lines)


def audit_observation_import_report(root: str | Path = OBSERVATION_IMPORT_REPORT_ROOT) -> dict[str, Any]:
    failures = []
    root_path = Path(root)
    if root_path.exists():
        for path in root_path.rglob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                validate_observation_import_allowed(payload)
            except Exception as exc:
                failures.append(f"{path.as_posix()}: {exc}")
    return {"observation_import_audit_ok": not failures, "observation_import_audit_failures": failures}
