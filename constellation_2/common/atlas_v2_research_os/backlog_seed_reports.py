from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .research_backlog import ResearchBacklog

REPORT_DIRNAME = "backlog_seeding"


def write_seed_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    root_path = Path(root)
    day_value = day or _day_from_timestamp(str(report.get("created_at") or ""))
    payload = dict(report)
    payload.setdefault("schema_id", "atlas_v2_research_os_backlog_seeding_report_v1")
    payload.setdefault("schema_version", "v1")
    payload["day"] = day_value
    out_dir = backlog_seeding_root(root_path) / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "backlog_seeding_report.json"
    summary_path = out_dir / "backlog_seeding_summary.md"
    latest_json = backlog_seeding_root(root_path) / "latest.json"
    latest_summary = backlog_seeding_root(root_path) / "latest_summary.md"
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    summary = render_seed_summary(payload, root_path)
    json_path.write_text(serialized, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_json.write_text(serialized, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def build_backlog_seeding_report(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    latest = backlog_seeding_root(root) / "latest.json"
    if latest.exists():
        return json.loads(latest.read_text(encoding="utf-8"))
    ready = ResearchBacklog(root).get_ready_items()
    return {
        "schema_id": "atlas_v2_research_os_backlog_seeding_report_v1",
        "schema_version": "v1",
        "day": datetime.now(UTC).date().isoformat(),
        "generated_count": 0,
        "written_count": 0,
        "duplicate_count": 0,
        "blocked_count": 0,
        "ready_count_after_seeding": len(ready),
        "ready_count_by_type": dict(sorted(Counter(row.get("item_type", "") for row in ready).items())),
        "ready_count_by_mechanism": _ready_by_mechanism(ready),
        "top_ready_items": _top_ready(ready),
        "governance_result": {"status": "UNKNOWN", "violations": [], "warnings": ["no backlog seeding report exists"]},
    }


def render_seed_summary(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> str:
    ready = ResearchBacklog(root).get_ready_items()
    top = report.get("top_ready_items") or _top_ready(ready)
    lines = [
        "# Atlas V2 Research OS Backlog Seeding",
        "",
        f"Day: {report.get('day', '')}",
        f"Profile: {report.get('profile', '')}",
        f"Items generated: {report.get('generated_count', 0)}",
        f"Items written: {report.get('written_count', 0)}",
        f"Items skipped as duplicates: {report.get('duplicate_count', 0)}",
        f"Items blocked: {report.get('blocked_count', 0)}",
        f"READY count after seeding: {report.get('ready_count_after_seeding', len(ready))}",
        f"READY count by type: `{json.dumps(report.get('ready_count_by_type', {}), sort_keys=True)}`",
        f"READY count by mechanism: `{json.dumps(report.get('ready_count_by_mechanism', {}), sort_keys=True)}`",
        f"Governance result: {(report.get('governance_result', {}) or {}).get('status', 'UNKNOWN')}",
        "",
        "## Top READY Items",
    ]
    lines.extend([f"- `{json.dumps(row, sort_keys=True)}`" for row in top[:20]] or ["- None"])
    lines.extend([
        "",
        "## Authority Boundary",
        "Research workload generation only. No live trading, broker execution, capital allocation, candidate promotion, sleeve deployment, portfolio construction, position sizing, trade recommendations, or automatic paper trade placement.",
        "",
    ])
    return "\n".join(lines)


def backlog_seeding_root(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = Path(root) / REPORT_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _top_ready(ready: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = sorted(ready, key=lambda row: (-float(row.get("priority_score") or 0.0), str(row.get("backlog_item_id") or "")))[:20]
    return [{"backlog_item_id": row.get("backlog_item_id"), "item_type": row.get("item_type"), "priority_score": row.get("priority_score"), "title": row.get("title"), "mechanism_tags": row.get("mechanism_tags", [])} for row in rows]


def _ready_by_mechanism(ready: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in ready:
        tags = row.get("mechanism_tags") or (row.get("metadata", {}) or {}).get("mechanism_tags") or ["UNKNOWN"]
        for tag in tags:
            counts[str(tag)] += 1
    return dict(sorted(counts.items()))


def _day_from_timestamp(value: str) -> str:
    if len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
        return value[:10]
    return datetime.now(UTC).date().isoformat()
