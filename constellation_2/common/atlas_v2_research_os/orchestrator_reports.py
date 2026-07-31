from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT


def write_orchestrator_report(record: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    day_value = day or _day_from_started_at(record.get("started_at")) or date.today().isoformat()
    report_root = Path(root) / "orchestrator"
    out_dir = report_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(record["run_id"])
    json_path = out_dir / f"run_{run_id}.json"
    summary_path = out_dir / f"run_{run_id}_summary.md"
    latest_json = report_root / "latest.json"
    latest_summary = report_root / "latest_summary.md"
    payload = json.dumps(record, indent=2, sort_keys=True) + "\n"
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary = render_orchestrator_summary(record)
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_orchestrator_summary(record: dict[str, Any]) -> str:
    failed_gates = [row["gate_id"] for row in record.get("safety_gate_results", []) if row.get("result") != "PASS"]
    return "\n".join([
        "# Atlas V2 Research OS Orchestrator Run",
        "",
        f"Run ID: {record.get('run_id')}",
        f"Status: {record.get('status')}",
        f"Trigger: {record.get('trigger_type')}",
        f"Selected backlog items: {json.dumps(record.get('selected_backlog_items', []), sort_keys=True)}",
        f"Workers invoked: {len(record.get('workers_invoked', []))}",
        f"Artifacts created: {len(record.get('artifacts_created', []))}",
        f"Failed safety gates: {json.dumps(failed_gates, sort_keys=True)}",
        f"Lineage validation: {json.dumps(record.get('lineage_validation_result', {}), sort_keys=True)}",
        f"Skip reason: {record.get('skip_reason')}",
        "",
        "Forbidden uses: no scheduler, no autonomous hourly loop, no candidate promotion, no sleeve mutation, no trade advice, no broker execution, and no real capital allocation.",
        "",
    ])


def _day_from_started_at(started_at: Any) -> str | None:
    if not isinstance(started_at, str) or len(started_at) < 10:
        return None
    return started_at[:10]
