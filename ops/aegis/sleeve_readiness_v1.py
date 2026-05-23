from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.sleeve_input_contracts_v1 import contract_hash_v1


REPORT_FAMILY = "aegis_sleeve_readiness_v1"
BLOCKING_STATUSES = {"MISSING", "STALE", "UNKNOWN"}


def build_sleeve_readiness_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    registry_path, registry = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    contracts_path, contracts_payload = latest_json_v1(root, "aegis_sleeve_input_contracts_v1", day_utc, "sleeve_input_contracts.v1.json")
    data_by_id = {
        str(row.get("data_item_id") or ""): row
        for row in registry.get("data_items") or []
        if isinstance(row, dict) and row.get("data_item_id")
    }
    sleeves = []
    for contract in contracts_payload.get("contracts") or []:
        if not isinstance(contract, dict):
            continue
        sleeves.append(_readiness_for_contract(contract=contract, data_by_id=data_by_id, contracts_path=contracts_path, registry_path=registry_path))
    ready = [row for row in sleeves if row["readiness"] == "READY"]
    warning = [row for row in sleeves if row["readiness"] == "READY_WITH_WARNINGS"]
    blocked = [row for row in sleeves if row["readiness"] == "BLOCKED"]
    unknown = [row for row in sleeves if row["readiness"] == "UNKNOWN"]
    return {
        "schema_id": "aegis_sleeve_readiness",
        "schema_version": "v1",
        "artifact_id": "aegis_sleeve_readiness_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "data_registry_path": str(registry_path or ""),
        "sleeve_input_contracts_path": str(contracts_path or ""),
        "total_sleeves_enabled": len(sleeves),
        "ready_count": len(ready),
        "ready_with_warnings_count": len(warning),
        "blocked_count": len(blocked),
        "unknown_count": len(unknown),
        "sleeves": sleeves,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def write_sleeve_readiness_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    out_dir = root / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "sleeve_readiness.v1.json", payload)
    summary_path = out_dir / "sleeve_readiness.summary.txt"
    matrix_path = out_dir / "sleeve_readiness.matrix.csv"
    summary_path.write_text(render_sleeve_readiness_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_sleeve_readiness_matrix_csv_v1(payload), encoding="utf-8")
    vix_report = build_vix_stale_operator_report_v1(truth_root=root, day_utc=day_utc, readiness_payload=payload)
    vix_dir = root / "reports" / "vix_stale_operator_report_v1" / day_utc
    vix_json = write_json_v1(vix_dir / "vix_stale_operator_report.v1.json", vix_report)
    vix_txt = vix_dir / "vix_stale_operator_report.v1.txt"
    vix_txt.write_text(render_vix_stale_operator_report_v1(vix_report), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path), "vix_stale_report_json": str(vix_json), "vix_stale_report_txt": str(vix_txt)}




def build_vix_stale_operator_report_v1(*, truth_root: Path, day_utc: str, readiness_payload: dict[str, Any]) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    registry_path, registry = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    market_inputs_path, market_inputs = latest_json_v1(root, "market_data_inputs_v1", day_utc, "market_data_inputs.v1.json")
    vix_registry = _registry_item(registry, "market.volatility.VIX")
    vix_input = _market_input_record(market_inputs, "market.volatility.VIX")
    affected_required: list[str] = []
    affected_optional: list[str] = []
    for sleeve in readiness_payload.get("sleeves") if isinstance(readiness_payload.get("sleeves"), list) else []:
        if not isinstance(sleeve, dict):
            continue
        sleeve_id = str(sleeve.get("sleeve_id") or "")
        if "market.volatility.VIX" in [str(item) for item in sleeve.get("blocking_inputs") or []]:
            affected_required.append(sleeve_id)
        if "market.volatility.VIX" in [str(item) for item in sleeve.get("warning_inputs") or []]:
            affected_optional.append(sleeve_id)
    validation_status = str(vix_input.get("validation_status") or vix_registry.get("market_data_validation_status") or vix_registry.get("status") or "MISSING").upper()
    source_timestamp = str(vix_input.get("source_timestamp_utc") or vix_registry.get("data_timestamp_utc") or "")
    notes = vix_registry.get("notes") if isinstance(vix_registry.get("notes"), list) else []
    reason = str(vix_input.get("reason") or (notes[0] if notes else ""))
    return {
        "schema_id": "vix_stale_operator_report",
        "schema_version": "v1",
        "artifact_id": "vix_stale_operator_report_v1",
        "day_utc": day_utc,
        "status": "READY" if validation_status in {"VALID", "CURRENT"} else validation_status,
        "validation_status": validation_status,
        "source_timestamp_utc": source_timestamp,
        "freshness_ttl_seconds": int(vix_input.get("freshness_ttl_seconds") or 86400),
        "last_valid_date": source_timestamp[:10] if source_timestamp else "",
        "affected_sleeves": sorted(set(affected_required + affected_optional)),
        "required_blocked_sleeves": sorted(set(affected_required)),
        "optional_warning_sleeves": sorted(set(affected_optional)),
        "next_expected_refresh_path": "python3 ops/tools/build_aegis_market_data_inputs_v1.py --truth-root {truth_root} --day-utc {day_utc} --emit-events",
        "source_file_path": str(vix_input.get("source_path") or vix_registry.get("source_artifact_path") or ""),
        "raw_source_hash": str(vix_input.get("raw_source_hash") or vix_registry.get("raw_source_hash") or ""),
        "reason": reason,
        "market_data_inputs_path": str(market_inputs_path or ""),
        "data_registry_path": str(registry_path or ""),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def render_vix_stale_operator_report_v1(payload: dict[str, Any]) -> str:
    lines = [
        "VIX STALE OPERATOR REPORT v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"source_timestamp_utc: {payload.get('source_timestamp_utc')}",
        f"freshness_ttl_seconds: {payload.get('freshness_ttl_seconds')}",
        f"last_valid_date: {payload.get('last_valid_date')}",
        f"required_blocked_sleeves: {', '.join(payload.get('required_blocked_sleeves') or [])}",
        f"optional_warning_sleeves: {', '.join(payload.get('optional_warning_sleeves') or [])}",
        f"next_expected_refresh_path: {payload.get('next_expected_refresh_path')}",
        f"source_file_path: {payload.get('source_file_path')}",
        f"raw_source_hash: {payload.get('raw_source_hash')}",
        f"reason: {payload.get('reason')}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
    ]
    return "\n".join(lines) + "\n"


def _registry_item(registry: dict[str, Any], data_item_id: str) -> dict[str, Any]:
    for row in registry.get("data_items", []) if isinstance(registry.get("data_items"), list) else []:
        if isinstance(row, dict) and str(row.get("data_item_id") or "") == data_item_id:
            return row
    return {}


def _market_input_record(payload: dict[str, Any], data_item_id: str) -> dict[str, Any]:
    for row in payload.get("input_records", []) if isinstance(payload.get("input_records"), list) else []:
        if isinstance(row, dict) and str(row.get("data_item_id") or "") == data_item_id:
            return row
    return {}

def render_sleeve_readiness_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS SLEEVE READINESS v1",
        f"day_utc: {payload.get('day_utc')}",
        f"total_sleeves_enabled: {payload.get('total_sleeves_enabled')}",
        f"ready_count: {payload.get('ready_count')}",
        f"ready_with_warnings_count: {payload.get('ready_with_warnings_count')}",
        f"blocked_count: {payload.get('blocked_count')}",
        f"unknown_count: {payload.get('unknown_count')}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "sleeves:",
    ]
    for row in payload.get("sleeves") or []:
        lines.append(f"- {row.get('sleeve_id')}: {row.get('readiness')} blockers={','.join(row.get('blocking_inputs') or [])} warnings={','.join(row.get('warning_inputs') or [])}")
    return "\n".join(lines) + "\n"


def render_sleeve_readiness_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["sleeve_id", "readiness", "contract_status", "can_run_candidate_generation", "blocking_inputs", "warning_inputs", "reason"])
    writer.writeheader()
    for row in payload.get("sleeves") or []:
        writer.writerow(
            {
                "sleeve_id": row.get("sleeve_id", ""),
                "readiness": row.get("readiness", ""),
                "contract_status": row.get("contract_status", ""),
                "can_run_candidate_generation": row.get("can_run_candidate_generation", False),
                "blocking_inputs": "|".join(row.get("blocking_inputs") or []),
                "warning_inputs": "|".join(row.get("warning_inputs") or []),
                "reason": row.get("reason", ""),
            }
        )
    return out.getvalue()


def _readiness_for_contract(*, contract: dict[str, Any], data_by_id: dict[str, dict[str, Any]], contracts_path: Path | None, registry_path: Path | None) -> dict[str, Any]:
    sleeve_id = str(contract.get("sleeve_id") or "UNKNOWN")
    contract_status = str(contract.get("contract_status") or "OK").upper()
    required_rows = [_input_status(row, data_by_id, required=True) for row in contract.get("required_inputs") or [] if isinstance(row, dict)]
    optional_rows = [_input_status(row, data_by_id, required=False) for row in contract.get("optional_inputs") or [] if isinstance(row, dict)]
    blocking = [row["data_item_id"] for row in required_rows if row["blocking"]]
    warning = [row["data_item_id"] for row in optional_rows if row["warning"]]
    if contract_status != "OK":
        readiness = "BLOCKED"
        reason = f"Sleeve input contract status is {contract_status}."
        blocking = blocking or ["CONTRACT_MISSING"]
    elif blocking:
        readiness = "BLOCKED"
        reason = "Required sleeve inputs are missing or stale."
    elif warning:
        readiness = "READY_WITH_WARNINGS"
        reason = "Required inputs are available; optional context is missing or stale."
    else:
        readiness = "READY"
        reason = "All required sleeve inputs are available."
    source_artifacts = [str(path) for path in (contracts_path, registry_path) if path]
    return {
        "sleeve_id": sleeve_id,
        "readiness": readiness,
        "required_inputs_status": required_rows,
        "optional_inputs_status": optional_rows,
        "blocking_inputs": blocking,
        "warning_inputs": warning,
        "contract_status": contract_status,
        "can_run_candidate_generation": readiness in {"READY", "READY_WITH_WARNINGS"},
        "reason": reason,
        "source_artifacts": source_artifacts,
        "source_hashes": {"contract": contract_hash_v1(contract)},
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _input_status(input_row: dict[str, Any], data_by_id: dict[str, dict[str, Any]], *, required: bool) -> dict[str, Any]:
    data_item_id = str(input_row.get("data_item_id") or "")
    data = data_by_id.get(data_item_id, {})
    status = str(data.get("status") or "MISSING").upper()
    freshness = str(input_row.get("freshness_requirement") or ("CURRENT" if required else "DELAYED_ALLOWED")).upper()
    delayed_ok = freshness in {"DELAYED_ALLOWED", "PRIOR_CLOSE_ALLOWED"}
    blocking = required and (status in BLOCKING_STATUSES or (status == "DELAYED_BUT_USABLE" and not delayed_ok))
    warning = (not required) and status in BLOCKING_STATUSES
    return {
        "data_item_id": data_item_id,
        "required": required,
        "status": status,
        "freshness_requirement": freshness,
        "provider": data.get("provider") or "",
        "source_artifact_path": data.get("source_artifact_path") or "",
        "source_hash": data.get("source_hash") or "",
        "evidence_event_id": data.get("evidence_event_id") or "",
        "evidence_event_hash": data.get("evidence_event_hash") or "",
        "raw_source_hash": data.get("raw_source_hash") or "",
        "transformed_value_hash": data.get("transformed_value_hash") or "",
        "market_data_validation_status": data.get("market_data_validation_status") or "",
        "input_classification": data.get("input_classification") or ("SLEEVE_CRITICAL" if required else "ADVISORY_ONLY"),
        "readiness_effect": data.get("readiness_effect") or ("BLOCKS" if blocking else ("WARNS" if warning else "NONE")),
        "value": data.get("value"),
        "unit": data.get("unit") or "",
        "data_timestamp_utc": data.get("data_timestamp_utc") or "",
        "blocking": blocking,
        "warning": warning,
        "reason": input_row.get("reason") or "",
    }
