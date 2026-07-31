from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .mechanism_hypothesis_generator import generate_mechanism_search_small
from .mechanism_search_governance import validate_mechanism_search_run

MECHANISM_SEARCH_REPORT_ROOT = Path("reports/atlas_v2_research_os/mechanism_search")


def build_mechanism_search_report(root: str | Path = DEFAULT_STORE_ROOT, *, run: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = run or generate_mechanism_search_small(root=root)
    governance = validate_mechanism_search_run(payload)
    mechanism_counts: dict[str, int] = {}
    for row in payload.get("hypotheses", []):
        mechanism_counts[row["mechanism"]] = mechanism_counts.get(row["mechanism"], 0) + 1
    return {
        "schema_id": "atlas_v2_research_os_mechanism_search_report_v1",
        "schema_version": "v1",
        "created_at": payload["created_at"],
        "run_id": payload["run_id"],
        "requested_limit": payload["requested_limit"],
        "emitted_count": payload["emitted_count"],
        "mechanism_counts": mechanism_counts,
        "governance_result": governance,
        "pipeline": payload.get("metadata", {}).get("pipeline", []),
        "pipeline_results": payload.get("pipeline_results", []),
        "hypotheses": payload.get("hypotheses", []),
        "research_only": True,
    }


def write_mechanism_search_report(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    report_root: str | Path = MECHANISM_SEARCH_REPORT_ROOT,
    run: dict[str, Any] | None = None,
    day: str | None = None,
) -> dict[str, Path]:
    report = build_mechanism_search_report(root, run=run)
    day_value = day or date.today().isoformat()
    out_root = Path(report_root)
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "mechanism_search_report.json"
    md_path = out_dir / "mechanism_search_summary.md"
    latest_json = out_root / "latest.json"
    latest_md = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary = render_mechanism_search_summary(report)
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_mechanism_search_summary(report: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Atlas Mechanism Search Report",
            "",
            f"Run: {report['run_id']}",
            f"Emitted hypotheses: {report['emitted_count']}",
            f"Governance: {report['governance_result']['status']}",
            f"Mechanism counts: {json.dumps(report['mechanism_counts'], sort_keys=True)}",
            f"Pipeline: {json.dumps(report['pipeline'], sort_keys=True)}",
            f"Pipeline results: {len(report['pipeline_results'])}",
            "Authority: research-only measurement and review evidence.",
            "",
        ]
    )


def audit_mechanism_search_report(root: str | Path = MECHANISM_SEARCH_REPORT_ROOT) -> dict[str, Any]:
    failures: list[str] = []
    root_path = Path(root)
    if root_path.exists():
        for path in root_path.rglob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                result = validate_mechanism_search_run(payload)
                failures.extend(f"{path.as_posix()}: {failure}" for failure in result["failures"])
            except Exception as exc:
                failures.append(f"{path.as_posix()}: {exc}")
    return {"mechanism_search_audit_ok": not failures, "mechanism_search_audit_failures": failures}
