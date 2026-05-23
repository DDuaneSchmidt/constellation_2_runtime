from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.stability.stability_registry import (
    CREATED_AT,
    CREATED_BY,
    RESEARCH_LABEL,
    finalize_report,
    latest_report,
    registry_path,
    report_root,
    write_stability_artifacts,
)
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


def _load_report_by_id(store: Path, *, family: str, report_id: str) -> dict[str, Any]:
    path = report_root(store, family) / f"{report_id}.json"
    if not path.exists():
        raise RuntimeError(f"stability report missing: {path}")
    return read_json(path)


def _overall_status(drift_status: str, fragility_status: str) -> str:
    if drift_status == "insufficient_data" or fragility_status == "insufficient_data":
        return "insufficient_data"
    if drift_status == "degrading":
        return "degrading"
    if fragility_status == "fragile":
        return "fragile"
    if drift_status == "watch" or fragility_status == "watch":
        return "watch"
    return "stable"


def _recommended_action(status: str) -> str:
    return {
        "insufficient_data": "collect_more_candidates",
        "stable": "continue_research",
        "watch": "review_sleeve",
        "fragile": "challenge_sleeve",
        "degrading": "challenge_sleeve",
    }[status]


def _next_allowed_actions(status: str) -> list[str]:
    if status == "insufficient_data":
        return ["collect_more_candidates", "continue_research"]
    if status == "stable":
        return ["continue_research", "review_sleeve"]
    if status == "watch":
        return ["review_sleeve", "revise_ranking_policy", "revise_candidate_generator"]
    return ["review_sleeve", "challenge_sleeve"]


def build_sleeve_stability_report(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    expectancy_drift_report_id: str,
    regime_fragility_report_id: str,
    store_root: Path | None = None,
    created_at: str = CREATED_AT,
    created_by: str = CREATED_BY,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    drift = _load_report_by_id(store, family="expectancy_drift", report_id=expectancy_drift_report_id)
    fragility = _load_report_by_id(store, family="regime_fragility", report_id=regime_fragility_report_id)
    if drift.get("sleeve_id") != sleeve_id or fragility.get("sleeve_id") != sleeve_id:
        raise RuntimeError("stability report sleeve mismatch")
    if drift.get("sleeve_version_id") != sleeve_version_id or fragility.get("sleeve_version_id") != sleeve_version_id:
        raise RuntimeError("stability report sleeve version mismatch")
    status = _overall_status(str(drift["drift_status"]), str(fragility["fragility_status"]))
    findings = [
        f"expectancy_drift_status={drift['drift_status']}",
        f"regime_fragility_status={fragility['fragility_status']}",
        f"measured_candidate_count={drift['measured_candidate_count']}",
    ]
    if drift.get("drift_reasons"):
        findings.append(f"drift_reason={drift['drift_reasons'][0]}")
    if fragility.get("fragility_reasons"):
        findings.append(f"fragility_reason={fragility['fragility_reasons'][0]}")
    report = {
        "sleeve_stability_report_id": "",
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "created_at": created_at,
        "created_by": created_by,
        "expectancy_drift_report_id": expectancy_drift_report_id,
        "regime_fragility_report_id": regime_fragility_report_id,
        "overall_stability_status": status,
        "key_findings": findings,
        "recommended_action": _recommended_action(status),
        "next_allowed_actions": _next_allowed_actions(status),
        "research_label": RESEARCH_LABEL,
        "schema_version": "sleeve_stability_report.v1",
        "content_hash": "",
    }
    finalize_report(report, id_field="sleeve_stability_report_id", prefix="ssr")
    validate_contract("sleeve_stability_report", report)
    return report


def sleeve_stability_markdown(report: dict[str, Any]) -> str:
    lines = ["# Sleeve Stability Report", "", report["research_label"], ""]
    lines.append(f"- sleeve_id: {report['sleeve_id']}")
    lines.append(f"- overall_stability_status: {report['overall_stability_status']}")
    lines.append(f"- recommended_action: {report['recommended_action']}")
    lines.append("")
    lines.append("## Key Findings")
    lines.extend(f"- {finding}" for finding in report["key_findings"])
    return "\n".join(lines) + "\n"


def write_sleeve_stability_report(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    expectancy_drift_report_id: str,
    regime_fragility_report_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    report = build_sleeve_stability_report(
        sleeve_id=sleeve_id,
        sleeve_version_id=sleeve_version_id,
        expectancy_drift_report_id=expectancy_drift_report_id,
        regime_fragility_report_id=regime_fragility_report_id,
        store_root=store,
        created_by=actor,
    )
    row = {
        "sleeve_stability_report_id": report["sleeve_stability_report_id"],
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "expectancy_drift_report_id": expectancy_drift_report_id,
        "regime_fragility_report_id": regime_fragility_report_id,
        "overall_stability_status": report["overall_stability_status"],
        "recommended_action": report["recommended_action"],
        "content_hash": report["content_hash"],
        "created_at": report["created_at"],
        "schema_version": report["schema_version"],
    }
    return write_stability_artifacts(
        store=store,
        family="sleeve_stability",
        report=report,
        id_field="sleeve_stability_report_id",
        contract_name="sleeve_stability_report",
        registry_name="sleeve_stability_reports.jsonl",
        registry_row=row,
        markdown=sleeve_stability_markdown(report),
        audit_action="sleeve_stability_report_written",
        actor=actor,
    )


def latest_sleeve_stability_report(*, store_root: Path | None = None, sleeve_id: str | None = None) -> dict[str, Any] | None:
    store = ensure_store_layout(store_root)
    return latest_report(
        store,
        registry_name="sleeve_stability_reports.jsonl",
        family="sleeve_stability",
        id_field="sleeve_stability_report_id",
        sleeve_id=sleeve_id,
    )


def latest_expectancy_drift_report(*, store_root: Path | None = None, sleeve_id: str | None = None) -> dict[str, Any] | None:
    store = ensure_store_layout(store_root)
    return latest_report(
        store,
        registry_name="expectancy_drift_reports.jsonl",
        family="expectancy_drift",
        id_field="expectancy_drift_report_id",
        sleeve_id=sleeve_id,
    )


def latest_regime_fragility_report(*, store_root: Path | None = None, sleeve_id: str | None = None) -> dict[str, Any] | None:
    store = ensure_store_layout(store_root)
    return latest_report(
        store,
        registry_name="regime_fragility_reports.jsonl",
        family="regime_fragility",
        id_field="regime_fragility_report_id",
        sleeve_id=sleeve_id,
    )


def stability_registry_counts(*, store_root: Path | None = None) -> dict[str, int]:
    store = ensure_store_layout(store_root)
    return {
        "expectancy_drift_reports": len(read_jsonl(registry_path(store, "expectancy_drift_reports.jsonl"))),
        "regime_fragility_reports": len(read_jsonl(registry_path(store, "regime_fragility_reports.jsonl"))),
        "sleeve_stability_reports": len(read_jsonl(registry_path(store, "sleeve_stability_reports.jsonl"))),
    }
