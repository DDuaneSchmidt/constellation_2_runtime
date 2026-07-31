from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .worker_adapters import create_default_worker_adapters
from .worker_connection_rules import CONNECTED_STATUSES
from .worker_execution_records import build_worker_execution_report
from .worker_registry import validate_worker_contract

WORKER_REPORT_ROOT = Path("reports/atlas_v2_research_os/workers")


def build_worker_interface_report(*, day: str | None = None) -> dict[str, Any]:
    day_value = day or date.today().isoformat()
    adapters = create_default_worker_adapters()
    worker_rows = []
    failures = []
    for adapter in adapters:
        contract_ok, contract_failures = validate_worker_contract(adapter)
        if contract_failures:
            failures.extend(f"{adapter.worker_id}: {failure}" for failure in contract_failures)
        dry_result = adapter.dry_run([], metadata={"report_mode": "worker_interface_audit"}).to_dict()
        worker_rows.append(
            {
                "worker_id": adapter.worker_id,
                "worker_type": adapter.worker_type,
                "adapter_status": adapter.adapter_status,
                "design_status": "DESIGN_READY",
                "production_connected": False,
                "research_connected": adapter.adapter_status in CONNECTED_STATUSES,
                "source_component": adapter.source_component,
                "supported_input_artifact_types": list(adapter.supported_input_artifact_types),
                "supported_output_artifact_types": list(adapter.supported_output_artifact_types),
                "contract_ok": contract_ok,
                "contract_failures": contract_failures,
                "dry_run_result": dry_result,
            }
        )
    return {
        "schema_id": "atlas_v2_research_os_worker_interface_report_v1",
        "schema_version": "v0.1",
        "day": day_value,
        "worker_count": len(worker_rows),
        "worker_counts_by_type": dict(Counter(row["worker_type"] for row in worker_rows)),
        "adapter_counts_by_status": dict(Counter(row["adapter_status"] for row in worker_rows)),
        "contract_audit_result": "PASS" if not failures else "FAIL",
        "contract_failures": failures,
        "workers": worker_rows,
        "safety": {
            "scheduler_implemented": False,
            "autonomous_orchestration_implemented": False,
            "candidate_factory_behavior_changed": False,
            "trading_capital_sleeve_portfolio_broker_or_recommendation_surface_changed": False,
        },
    }


def write_worker_interface_report(*, day: str | None = None, report_root: str | Path = WORKER_REPORT_ROOT) -> dict[str, Path]:
    day_value = day or date.today().isoformat()
    report = build_worker_interface_report(day=day_value)
    root = Path(report_root)
    out_dir = root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "worker_interface_report.json"
    md_path = out_dir / "worker_interface_summary.md"
    latest_json = root / "latest.json"
    latest_md = root / "latest_summary.md"
    json_payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_worker_interface_summary(report)
    json_path.write_text(json_payload, encoding="utf-8")
    latest_json.write_text(json_payload, encoding="utf-8")
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_worker_interface_summary(report: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Atlas V2 Research OS Worker Interface Report",
            "",
            f"Day: {report['day']}",
            f"Worker count: {report['worker_count']}",
            f"Contract audit: {report['contract_audit_result']}",
            f"Adapter status counts: {json.dumps(report['adapter_counts_by_status'], sort_keys=True)}",
            f"Worker type counts: {json.dumps(report['worker_counts_by_type'], sort_keys=True)}",
            f"Safety: {json.dumps(report['safety'], sort_keys=True)}",
            f"Contract failures: {json.dumps(report['contract_failures'], sort_keys=True)}",
            "",
        ]
    )


def build_worker_connection_report(root: str | Path = "reports/atlas_v2_research_os", *, day: str | None = None) -> dict[str, Any]:
    day_value = day or date.today().isoformat()
    adapters = create_default_worker_adapters()
    connected = []
    placeholders = []
    failures = []
    for adapter in adapters:
        contract_ok, contract_failures = validate_worker_contract(adapter)
        row = {
            "worker_id": adapter.worker_id,
            "worker_type": adapter.worker_type,
            "adapter_status": adapter.adapter_status,
            "source_component": adapter.source_component,
            "supported_input_artifact_types": list(adapter.supported_input_artifact_types),
            "supported_output_artifact_types": list(adapter.supported_output_artifact_types),
            "contract_ok": contract_ok,
            "contract_failures": contract_failures,
        }
        if contract_failures:
            failures.extend(f"{adapter.worker_id}: {failure}" for failure in contract_failures)
        if adapter.adapter_status in CONNECTED_STATUSES:
            connected.append(row)
        else:
            placeholders.append(row)
    execution = build_worker_execution_report(root, day=day_value)
    return {
        "schema_id": "atlas_v2_research_os_worker_connection_report_v1",
        "schema_version": "v1",
        "day": day_value,
        "connected_workers": connected,
        "placeholder_workers": placeholders,
        "connected_worker_count": len(connected),
        "placeholder_worker_count": len(placeholders),
        "worker_execution_counts": execution["worker_execution_counts"],
        "governance_failures": execution["governance_failures"],
        "lineage_failures": execution["lineage_failures"],
        "blocked_outputs": execution["blocked_outputs"],
        "forbidden_artifact_attempts": execution["forbidden_artifact_attempts"],
        "contract_audit_result": "PASS" if not failures else "FAIL",
        "contract_failures": failures,
    }


def write_worker_connection_report(root: str | Path = "reports/atlas_v2_research_os", *, day: str | None = None) -> dict[str, Path]:
    day_value = day or date.today().isoformat()
    report = build_worker_connection_report(root, day=day_value)
    out_dir = Path(root) / "worker_connections" / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "worker_connection_report.json"
    md_path = out_dir / "worker_connection_summary.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_worker_connection_summary(report), encoding="utf-8")
    return {"json": json_path, "summary": md_path}


def render_worker_connection_summary(report: dict[str, Any]) -> str:
    return "\n".join([
        "# Atlas V2 Research OS Worker Connection Report",
        "",
        f"Day: {report['day']}",
        f"Connected workers: {report['connected_worker_count']}",
        f"Placeholder workers: {report['placeholder_worker_count']}",
        f"Worker execution counts: {json.dumps(report['worker_execution_counts'], sort_keys=True)}",
        f"Governance failures: {report['governance_failures']}",
        f"Lineage failures: {report['lineage_failures']}",
        f"Blocked outputs: {report['blocked_outputs']}",
        f"Forbidden artifact attempts: {report['forbidden_artifact_attempts']}",
        f"Contract audit: {report['contract_audit_result']}",
        "",
    ])
