from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .governance import validate_no_forbidden_artifacts
from .lineage import validate_lineage_integrity
from .priority_engine import PriorityEngine
from .research_backlog import ResearchBacklog
from .learning_lifecycle import current_state


def build_foundation_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    day_value = day or date.today().isoformat()
    store = ArtifactStore(root)
    backlog = ResearchBacklog(root)
    artifacts = store.list_artifacts()
    backlog_items = backlog.list_backlog_items()
    lineage_ok, lineage_failures = validate_lineage_integrity(store)
    forbidden_ok, forbidden_failures = validate_no_forbidden_artifacts(root)
    sample = PriorityEngine(backlog, store).select_next_items(limit=5, exploration_rate=0.0)
    report = {
        "schema_id": "atlas_v2_research_os_foundation_report_v1",
        "schema_version": "v0.1",
        "day": day_value,
        "artifact_counts": dict(Counter(item["artifact_type"] for item in artifacts)),
        "backlog_counts_by_state": dict(Counter(item["state"] for item in backlog_items)),
        "backlog_counts_by_type": dict(Counter(item["item_type"] for item in backlog_items)),
        "lifecycle_counts_by_state": dict(Counter(current_state(store, item["artifact_id"]) for item in artifacts)),
        "governance_audit_result": "PASS" if forbidden_ok else "FAIL",
        "lineage_integrity_result": "PASS" if lineage_ok else "FAIL",
        "lineage_integrity_failures": lineage_failures,
        "forbidden_artifact_audit_result": "PASS" if forbidden_ok else "FAIL",
        "forbidden_artifact_failures": forbidden_failures,
        "priority_engine_sample_output": sample,
        "open_blockers": lineage_failures + forbidden_failures,
    }
    return report


def write_foundation_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    day_value = day or date.today().isoformat()
    report = build_foundation_report(root, day=day_value)
    report_root = Path(root)
    out_dir = report_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "research_os_foundation_report.json"
    md_path = out_dir / "research_os_foundation_summary.md"
    latest_json = report_root / "latest.json"
    latest_md = report_root / "latest_summary.md"
    json_payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    json_path.write_text(json_payload, encoding="utf-8")
    latest_json.write_text(json_payload, encoding="utf-8")
    summary = render_summary(report)
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_summary(report: dict[str, Any]) -> str:
    return "\n".join([
        "# Atlas V2 Research OS Foundation Report",
        "",
        f"Day: {report['day']}",
        f"Governance audit: {report['governance_audit_result']}",
        f"Lineage integrity: {report['lineage_integrity_result']}",
        f"Forbidden artifact audit: {report['forbidden_artifact_audit_result']}",
        "",
        f"Artifact counts: {json.dumps(report['artifact_counts'], sort_keys=True)}",
        f"Backlog by state: {json.dumps(report['backlog_counts_by_state'], sort_keys=True)}",
        f"Backlog by type: {json.dumps(report['backlog_counts_by_type'], sort_keys=True)}",
        f"Lifecycle by state: {json.dumps(report['lifecycle_counts_by_state'], sort_keys=True)}",
        f"Priority sample: {json.dumps(report['priority_engine_sample_output'], sort_keys=True)}",
        f"Open blockers: {json.dumps(report['open_blockers'], sort_keys=True)}",
        "",
    ])
