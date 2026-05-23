from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.producer_contracts_v1 import critical_output_schema_ids_v1, find_contract_for_schema_v1, load_producer_contract_registry_v1


def producer_contract_coverage_report_v1(*, artifact_statuses: list[dict[str, Any]], events: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    registry = load_producer_contract_registry_v1()
    critical = sorted(critical_output_schema_ids_v1(registry))
    native_required = sorted(_native_required_critical_schemas_v1(registry))
    covered = []
    missing = []
    for schema_id in critical:
        if find_contract_for_schema_v1(schema_id, registry):
            covered.append(schema_id)
        else:
            missing.append(schema_id)
    percentage = 100.0 if not critical else round((len(covered) / len(critical)) * 100, 2)
    native = _native_critical_schemas_v1(events or [])
    native_covered = [schema_id for schema_id in native_required if schema_id in native]
    native_missing = [schema_id for schema_id in native_required if schema_id not in native]
    native_percentage = 100.0 if not native_required else round((len(native_covered) / len(native_required)) * 100, 2)
    return {
        "schema_id": "aegis_producer_contract_coverage",
        "schema_version": "v1",
        "critical_output_schema_count": len(critical),
        "covered_critical_output_schema_count": len(covered),
        "critical_producer_contract_coverage": percentage,
        "critical_contract_coverage_percent": percentage,
        "covered_critical_output_schema_ids": covered,
        "missing_critical_output_schema_ids": missing,
        "legacy_scan_only_critical_producers": legacy_scan_only_critical_artifacts_v1(artifact_statuses=artifact_statuses, events=events or []),
        "critical_native_event_coverage": native_percentage,
        "critical_native_event_coverage_percent": native_percentage,
        "native_event_required_critical_output_schema_ids": native_required,
        "native_event_covered_critical_output_schema_ids": native_covered,
        "native_event_missing_critical_output_schema_ids": native_missing,
    }


def legacy_scan_only_artifacts_report_v1(*, artifact_statuses: list[dict[str, Any]], events: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "schema_id": "aegis_legacy_scan_only_artifacts",
        "schema_version": "v1",
        "legacy_scan_only_critical_producers": legacy_scan_only_critical_artifacts_v1(artifact_statuses=artifact_statuses, events=events or []),
        "noncritical_scan_only_artifacts": [],
    }


def source_data_manifest_v1(*, events: list[dict[str, Any]]) -> dict[str, Any]:
    sources = []
    for event in events:
        input_hashes = event.get("input_hashes") if isinstance(event.get("input_hashes"), dict) else {}
        source_hashes = {str(k): str(v) for k, v in input_hashes.items() if "source" in str(k).lower() or "raw" in str(k).lower()}
        if source_hashes:
            sources.append(
                {
                    "event_id": event.get("event_id"),
                    "schema_id": event.get("schema_id"),
                    "producer": event.get("producer"),
                    "source_hashes": source_hashes,
                }
            )
    return {"schema_id": "aegis_source_data_manifest", "schema_version": "v1", "sources": sources}


def manual_intent_manifest_v1(*, events: list[dict[str, Any]]) -> dict[str, Any]:
    intents = []
    for event in events:
        input_hashes = event.get("input_hashes") if isinstance(event.get("input_hashes"), dict) else {}
        intent_hashes = {str(k): str(v) for k, v in input_hashes.items() if "operator_intent" in str(k).lower() or "manual_intent" in str(k).lower()}
        if intent_hashes:
            intents.append(
                {
                    "event_id": event.get("event_id"),
                    "schema_id": event.get("schema_id"),
                    "producer": event.get("producer"),
                    "intent_hashes": intent_hashes,
                }
            )
    return {"schema_id": "aegis_manual_intent_manifest", "schema_version": "v1", "manual_intents": intents}


def render_contract_coverage_text_v1(report: dict[str, Any]) -> str:
    lines = [
        "AEGIS PRODUCER CONTRACT COVERAGE v1",
        f"critical_producer_contract_coverage: {report.get('critical_producer_contract_coverage')}%",
        f"legacy_scan_only_critical_producers: {len(report.get('legacy_scan_only_critical_producers') or [])}",
        "missing_critical_output_schema_ids:",
    ]
    lines.extend(f"- {item}" for item in report.get("missing_critical_output_schema_ids") or ["NONE"])
    return "\n".join(lines).rstrip() + "\n"


def render_blocker_states_text_v1(report: dict[str, Any]) -> str:
    lines = ["AEGIS BLOCKER STATES v1"]
    for row in report.get("blocker_state") or []:
        lines.append(f"- {row.get('blocker_id')}: {row.get('state')} repairability={row.get('repairability')}")
        lines.append(f"  next_safe_action: {row.get('next_safe_action')}")
    return "\n".join(lines).rstrip() + "\n"


def legacy_scan_only_critical_artifacts_v1(*, artifact_statuses: list[dict[str, Any]], events: list[dict[str, Any]] | None = None) -> list[str]:
    registry = load_producer_contract_registry_v1()
    critical = _native_required_critical_schemas_v1(registry)
    native = _native_critical_schemas_v1(events or [])
    missing = []
    for row in artifact_statuses:
        schema_id = str(row.get("artifact_id") or "")
        if schema_id in critical and schema_id not in native:
            missing.append(schema_id)
    return sorted(set(missing))


def _native_critical_schemas_v1(events: list[dict[str, Any]]) -> set[str]:
    native: set[str] = set()
    for event in events:
        producer = str(event.get("producer") or "")
        if producer == "ops.aegis.runtime_truth_kernel_v1.artifact_scan":
            continue
        if str(event.get("event_type") or "") not in {"EvidenceProduced", "EvidenceValidated", "EvidenceRejected"}:
            continue
        schema_id = str(event.get("schema_id") or "")
        if schema_id:
            native.add(schema_id)
    return native


def _native_required_critical_schemas_v1(registry: dict[str, Any]) -> set[str]:
    critical = critical_output_schema_ids_v1(registry)
    out: set[str] = set()
    for schema_id in critical:
        contract = find_contract_for_schema_v1(schema_id, registry)
        repair_class = str((contract or {}).get("repair_class") or "")
        if repair_class not in {"MANUAL_REQUIRED", "FORBIDDEN"}:
            out.add(schema_id)
    return out


def critical_bridge_usage_report_v1(*, events: list[dict[str, Any]]) -> dict[str, Any]:
    registry = load_producer_contract_registry_v1()
    critical = critical_output_schema_ids_v1(registry)
    bridge_producers = {"ops.aegis.runtime_truth_kernel_v1.artifact_scan", "ops.aegis.producer_event_bridge_v1"}
    latest: dict[str, dict[str, Any]] = {}
    historical = []
    for event in events:
        schema_id = str(event.get("schema_id") or "")
        event_type = str(event.get("event_type") or "")
        producer = str(event.get("producer") or "")
        if schema_id not in critical or event_type not in {"EvidenceProduced", "EvidenceValidated", "EvidenceRejected", "EvidenceExpired", "ArtifactTampered"}:
            continue
        current = latest.get(schema_id)
        sort_key = (str(event.get("created_at_utc") or ""), str(event.get("event_id") or ""))
        if current is None or sort_key >= (str(current.get("created_at_utc") or ""), str(current.get("event_id") or "")):
            latest[schema_id] = event
        if producer in bridge_producers:
            historical.append(_bridge_usage_row_v1(event))
    active = [_bridge_usage_row_v1(event) for event in latest.values() if str(event.get("producer") or "") in bridge_producers]
    return {
        "schema_id": "aegis_critical_bridge_usage",
        "schema_version": "v1",
        "critical_bridge_usage": sorted(active, key=lambda row: (str(row.get("schema_id") or ""), str(row.get("event_id") or ""))),
        "critical_bridge_usage_count": len(active),
        "historical_critical_bridge_event_count": len(historical),
    }


def _bridge_usage_row_v1(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "schema_id": event.get("schema_id"),
        "producer": event.get("producer"),
        "validation_status": event.get("validation_status"),
    }


def repair_semantics_report_v1(*, events: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for event in events:
        if str(event.get("producer") or "") != "ops.aegis.repair_orchestrator_v1":
            continue
        if str(event.get("event_type") or "") not in {"RepairAttempted", "RepairExecuted", "RepairSucceeded", "RepairCompletedWithRejectedEvidence", "RepairFailed", "RepairPlanned"}:
            continue
        rows.append(
            {
                "event_id": event.get("event_id"),
                "event_type": event.get("event_type"),
                "schema_id": event.get("schema_id"),
                "validation_status": event.get("validation_status"),
                "input_hashes": event.get("input_hashes", {}),
            }
        )
    return {"schema_id": "aegis_repair_semantics_report", "schema_version": "v1", "repair_events": rows}
