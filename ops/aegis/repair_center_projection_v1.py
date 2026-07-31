from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ops.aegis.domain_certification_v1 import build_domain_certification_report_v1
from ops.aegis.domain_repair_orchestrator_v1 import read_domain_repair_lifecycle_v1
from ops.aegis.operator_action_command_contracts_v1 import command_audit_path_v1, command_instance_v1, stable_hash_v1

SCHEMA_ID = "aegis_repair_center_projection"
SCHEMA_VERSION = "v1"

SECTIONS = [
    ("automatic_repair_available", "Automatic repair available"),
    ("source_setup_required", "Source setup required"),
    ("waiting_on_provider", "Waiting on provider"),
    ("repair_running", "Repair running"),
    ("repair_failed", "Repair failed"),
    ("repair_completed", "Repair completed"),
]

TERMINAL_REPAIR_STATUSES = {
    "COMPLETED",
    "SOURCE_CONFIGURED",
    "NOT_APPLICABLE_FOR_CURRENT_MODE",
    "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT",
}


def _read_command_audit_rows(truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = command_audit_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists() or not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except Exception:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _latest_repair_command_by_domain(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        command_id = str(row.get("command_id") or "")
        if command_id not in {"REPAIR_DOMAIN", "VALIDATE_SOURCE", "RECHECK_DOMAIN", "UPLOAD_SOURCE", "VIEW_SOURCE_SETUP", "DOWNLOAD_SOURCE_TEMPLATE", "DOWNLOAD_EOD_SOURCE_TEMPLATE", "CERTIFY_SOURCE", "VIEW_REPAIR_JOB"}:
            continue
        target_id = str(row.get("target_id") or "")
        if not target_id:
            continue
        previous = latest.get(target_id)
        if not previous or str(row.get("requested_at") or "") >= str(previous.get("requested_at") or ""):
            latest[target_id] = row
    return latest




def _lifecycle_failure_result(lifecycle: dict[str, Any], domain_id: str) -> dict[str, Any]:
    if str(lifecycle.get("repair_stage") or "") != "FAILED":
        return {}
    reason = str(lifecycle.get("status_reason") or "Repair failed")
    details: dict[str, Any] = {}
    for command_result in lifecycle.get("command_results", []) if isinstance(lifecycle.get("command_results"), list) else []:
        stdout_tail = str(command_result.get("stdout_tail") or "").strip()
        if not stdout_tail:
            continue
        try:
            parsed = json.loads(stdout_tail)
            reason = str(parsed.get("failure_reason") or parsed.get("message") or reason)
            details = {
                "missing_symbols": parsed.get("missing_symbols") or (parsed.get("validation_details") or {}).get("missing_symbols") or [],
                "stale_date_symbols": parsed.get("stale_date_symbols") or (parsed.get("validation_details") or {}).get("stale_date_symbols") or [],
                "invalid_ohlcv_rows": parsed.get("invalid_ohlcv_rows") or (parsed.get("validation_details") or {}).get("invalid_ohlcv_rows") or [],
                "provider_results": parsed.get("provider_results") or (parsed.get("validation_details") or {}).get("provider_results") or [],
                "artifact_path": parsed.get("artifact_path") or "",
                "provider_build_error": parsed.get("provider_build_error") or "",
            }
        except Exception:
            reason = stdout_tail[-500:] or reason
        break
    extra = ""
    if details.get("missing_symbols"):
        extra += f" Missing symbols: {', '.join(details['missing_symbols'][:20])}{'...' if len(details['missing_symbols']) > 20 else ''}."
    if details.get("stale_date_symbols"):
        extra += f" Stale prior-day rows: {', '.join(details['stale_date_symbols'][:20])}{'...' if len(details['stale_date_symbols']) > 20 else ''}."
    message = f"Repair failed for {domain_id}: {reason}.{extra}"
    return {
        "schema_id": "aegis_command_result_panel.v1",
        "status_label": "Failed",
        "result_status": "FAILED",
        "plain_english_result": message,
        "next_required_step": "Review the exact missing/stale provider rows, fix provider/source coverage, then queue repair again.",
        "failure_reason": reason,
        "failure_details": details,
        "job_id": str(lifecycle.get("event_id") or lifecycle.get("repair_attempt_id") or ""),
        "audit_id": "",
        "timestamp": str(lifecycle.get("created_at_utc") or ""),
    }

def _command_result(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    summary = row.get("response_summary") if isinstance(row.get("response_summary"), dict) else {}
    result = summary.get("command_result") if isinstance(summary.get("command_result"), dict) else {}
    if not result:
        result = {
            "status_label": str(summary.get("result_status") or row.get("result") or "Command result"),
            "result_status": str(summary.get("result_status") or row.get("result") or "UNKNOWN"),
            "plain_english_result": str(summary.get("user_message") or "Command completed."),
            "next_required_step": str(summary.get("next_state") or "Review the current repair state."),
        }
    return {
        **result,
        "audit_id": str(result.get("audit_id") or row.get("audit_id") or ""),
        "timestamp": str(result.get("timestamp") or row.get("requested_at") or ""),
    }


def _source_label(plan: dict[str, Any]) -> str:
    source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
    label = str(source.get("label") or source.get("artifact_type") or "Required source/artifact")
    path = str(source.get("path") or "")
    return f"{label}: {path}" if path else label


def _repair_mode(plan: dict[str, Any]) -> str:
    explicit = str(plan.get("repair_mode") or "")
    if explicit:
        return explicit
    reason = str(plan.get("reason") or "")
    if bool(plan.get("source_setup_required")):
        return "SOURCE_SETUP_REQUIRED"
    if "PROVIDER" in reason or "VENDOR" in reason:
        return "PROVIDER_WAIT"
    if str(plan.get("domain_id") or "") == "US_EQUITIES_EOD":
        return "AUTO_REBUILD"
    return "MANUAL_ADMIN_ACTION"


def _source_config_key(plan: dict[str, Any]) -> str:
    contract = plan.get("source_contract") if isinstance(plan.get("source_contract"), dict) else {}
    keys = contract.get("required_config_keys") if isinstance(contract.get("required_config_keys"), list) else []
    for key in keys:
        value = str(key or "").strip()
        if value:
            return value
    domain_id = str(plan.get("domain_id") or "")
    return {
        "US_EQUITIES_EOD": "AEGIS_US_EQUITIES_EOD_SOURCE_FILE",
        "MACRO_CALENDAR": "AEGIS_MACRO_CALENDAR_SOURCE_FILE",
        "EARNINGS_EVENTS": "AEGIS_EARNINGS_EVENTS_SOURCE_FILE",
        "CORPORATE_ACTIONS": "AEGIS_CORPORATE_ACTIONS_SOURCE_FILE",
        "US_EQUITIES_INTRADAY": "AEGIS_MARKET_DATA_INTRADAY_PROVIDER",
        "VOLATILITY": "AEGIS_MARKET_DATA_INTRADAY_PROVIDER",
        "RATES_BONDS": "AEGIS_MARKET_DATA_INTRADAY_PROVIDER",
    }.get(domain_id, "")


def _source_options_for_plan(plan: dict[str, Any]) -> dict[str, Any]:
    domain_id = str(plan.get("domain_id") or "")
    source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
    contract = plan.get("source_contract") if isinstance(plan.get("source_contract"), dict) else {}
    required_fields = contract.get("required_fields") if isinstance(contract.get("required_fields"), list) else []
    if not required_fields:
        required_fields = {
            "MACRO_CALENDAR": ["event_name", "time", "country", "importance", "source"],
            "EARNINGS_EVENTS": ["symbol", "company", "report_date", "report_time", "confirmed_or_estimated", "source"],
            "CORPORATE_ACTIONS": ["symbol", "action_type", "effective_date", "source"],
        }.get(domain_id, [])
    return {
        "existing_internal_builder": bool(plan.get("rebuild_command")),
        "available_provider_or_api": str(source.get("provider_or_source") or contract.get("provider_or_source") or "External governed source/API or operator-uploaded file required."),
        "static_manual_artifact_format": "JSON object with status/validation_status and rows under events/actions/calendar_rows.",
        "minimum_valid_artifact": {
            "path": str(source.get("path") or ""),
            "required_fields": list(required_fields),
            "status_fields": ["status=READY", "validation_status=VALID"],
        },
        "can_generate_from_existing_data": False,
        "generation_assessment": "No complete governed source is available in the current truth store; creating this artifact without an external source would fabricate coverage.",
        "not_system_failure_explanation": "Requires external source; not a system failure.",
    }


def _source_setup_workflow_for_plan(plan: dict[str, Any]) -> dict[str, Any]:
    domain_id = str(plan.get("domain_id") or "")
    source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
    contract = plan.get("source_contract") if isinstance(plan.get("source_contract"), dict) else {}
    fields = contract.get("required_fields") if isinstance(contract.get("required_fields"), list) else []
    if not fields:
        fields = _source_options_for_plan(plan).get("minimum_valid_artifact", {}).get("required_fields", [])
    rows_key = {"MACRO_CALENDAR": "events", "EARNINGS_EVENTS": "events", "CORPORATE_ACTIONS": "actions"}.get(domain_id, "rows")
    example_rows = {
        "MACRO_CALENDAR": {"event_name": "REPLACE_WITH_EVENT_NAME", "time": "2026-05-22T13:30:00Z", "country": "US", "importance": "HIGH", "source": "REPLACE_WITH_GOVERNED_SOURCE"},
        "EARNINGS_EVENTS": {"symbol": "REPLACE_WITH_SYMBOL", "company": "REPLACE_WITH_COMPANY", "report_date": "2026-05-22", "report_time": "AFTER_MARKET", "confirmed_or_estimated": "CONFIRMED", "source": "REPLACE_WITH_GOVERNED_SOURCE"},
        "CORPORATE_ACTIONS": {"symbol": "REPLACE_WITH_SYMBOL", "action_type": "DIVIDEND", "effective_date": "2026-05-22", "source": "REPLACE_WITH_GOVERNED_SOURCE"},
    }
    what = {
        "MACRO_CALENDAR": "A governed daily macro-event calendar used to identify scheduled economic events that can affect event-dislocation sleeves.",
        "EARNINGS_EVENTS": "A governed earnings-event calendar for tracked symbols and hypothesis/candidate universes.",
        "CORPORATE_ACTIONS": "A governed split, dividend, and corporate-action source for tracked symbols.",
    }.get(domain_id, "A governed external source required by this domain.")
    why = {
        "MACRO_CALENDAR": "Aegis needs it to decide whether event-driven sleeves have complete context before certification.",
        "EARNINGS_EVENTS": "Aegis needs it to prevent event blind spots in earnings-sensitive hypotheses and candidates.",
        "CORPORATE_ACTIONS": "Aegis needs it to avoid stale or adjusted-price mistakes around splits, dividends, and other actions.",
    }.get(domain_id, "Aegis needs it to certify domain context without fabricating coverage.")
    return {
        "schema_id": "aegis_source_setup_workflow.v1",
        "domain_id": domain_id,
        "title": "External source required — not a system failure",
        "what_this_source_is": what,
        "why_aegis_needs_it": why,
        "affected_sleeves": list(plan.get("affected_sleeves") or []),
        "affected_hypotheses": list(plan.get("affected_hypotheses") or []),
        "required_config_key": _source_config_key(plan),
        "required_file_path": str(source.get("path") or ""),
        "required_json_fields": list(fields),
        "example_valid_json_template": {
            "schema_id": contract.get("required_source") or f"{domain_id.lower()}_v1",
            "day_utc": str(plan.get("day_utc") or "2026-05-22"),
            "status": "READY",
            "validation_status": "VALID",
            rows_key: [example_rows.get(domain_id, {str(field): f"REPLACE_WITH_{str(field).upper()}" for field in fields})],
        },
        "upload_or_configure_option": "Set the required config key to a governed JSON source file, or place the completed JSON at the required file path.",
        "validation_action": "Run Validate Source after the file is configured.",
        "certification_action": "Run Re-run certification after validation succeeds.",
        "do_not_fabricate_data": True,
    }


def _exact_external_requirement_result(plan: dict[str, Any], *, status: str) -> dict[str, Any]:
    source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
    contract = plan.get("source_contract") if isinstance(plan.get("source_contract"), dict) else {}
    domain_id = str(plan.get("domain_id") or "Domain")
    label = str(source.get("label") or source.get("artifact_type") or domain_id)
    path = str(source.get("path") or "")
    config_key = _source_config_key(plan)
    setup = str(source.get("setup_required_message") or contract.get("setup_required_message") or "")
    provider = str(source.get("provider_or_source") or contract.get("provider_or_source") or "")
    if status == "SOURCE_CONFIGURED":
        message = f"Source configured for {domain_id}: {label}{f' at {path}' if path else ''}."
        next_step = str(plan.get("validation_command") or plan.get("certification_command") or "Validate the configured source and re-run domain certification.")
        status_label = "Source configured"
    elif str(plan.get("repair_mode") or "") == "PROVIDER_WAIT":
        message = f"{domain_id} is blocked by provider data freshness/availability for {label}{f' at {path}' if path else ''}."
        next_step = setup or provider or "Configure a current provider/source for this domain, rebuild the artifact, then re-run domain certification."
        status_label = "External source required — not a system failure"
    else:
        requirement = setup or f"Provide {label}{f' at {path}' if path else ''}."
        if config_key and config_key not in requirement:
            requirement = f"Set {config_key} or provide the required artifact. {requirement}"
        requirement = f"Requires external source; not a system failure. {requirement}"
        message = f"{domain_id} cannot be repaired automatically because an external source is missing. {requirement}"
        next_step = requirement
        status_label = "External source required — not a system failure"
    return {
        "schema_id": "aegis_command_result_panel.v1",
        "status_label": status_label,
        "result_status": status,
        "plain_english_result": message,
        "next_required_step": next_step,
        "job_id": "",
        "audit_id": "",
        "timestamp": "",
        "missing_source_name": label if status != "SOURCE_CONFIGURED" else "",
        "expected_source_path": path,
        "copy_required_path": path,
        "failure_reason": "" if status == "SOURCE_CONFIGURED" else str(plan.get("reason") or "EXTERNAL_REQUIREMENT_MISSING"),
        "source_config_key": config_key,
        "provider_or_source": provider,
    }


def _status_for(plan: dict[str, Any], latest_result: dict[str, Any]) -> str:
    result_status = str(latest_result.get("result_status") or "").upper()
    status_label = str(latest_result.get("status_label") or "").upper()
    if result_status in {"QUEUED", "REQUESTED"} or status_label in {"QUEUED", "REPAIR JOB QUEUED"}:
        return "QUEUED"
    if result_status in {"RUNNING", "IN_PROGRESS"}:
        return "RUNNING"
    if result_status in {"VALIDATED", "VALIDATION_SUCCEEDED"}:
        return "VALIDATING"
    if result_status in {"RECERTIFYING"}:
        return "RECERTIFYING"
    if result_status in {"COMPLETED", "SUCCEEDED", "RECERTIFIED"}:
        return "COMPLETED"
    if result_status in {"FAILED", "REPAIR_JOB_UNAVAILABLE", "NO_REPAIR_PLAN", "VALIDATION_FAILED", "SOURCE_MISSING"}:
        return "FAILED"
    if result_status in {"SOURCE_CONFIGURED", "NOT_APPLICABLE_FOR_CURRENT_MODE", "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT"}:
        return result_status
    mode = _repair_mode(plan)
    source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
    if mode == "SOURCE_SETUP_REQUIRED":
        return "SOURCE_CONFIGURED" if bool(source.get("exists")) else "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT"
    if mode == "PROVIDER_WAIT" and result_status in {"", "RECERTIFICATION_STILL_DELAYED", "SOURCE_SETUP_REQUIRED"}:
        return "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT"
    return "OPEN"


def _section_for(item: dict[str, Any]) -> str:
    status = str(item.get("status") or "")
    mode = str(item.get("repair_mode") or "")
    if status in {"QUEUED", "RUNNING", "VALIDATING", "RECERTIFYING"}:
        return "repair_running"
    if status == "FAILED":
        return "repair_failed"
    if status in {"COMPLETED", "SOURCE_CONFIGURED", "NOT_APPLICABLE_FOR_CURRENT_MODE"}:
        return "repair_completed"
    if mode == "SOURCE_SETUP_REQUIRED":
        return "source_setup_required"
    if mode == "PROVIDER_WAIT":
        return "waiting_on_provider"
    if mode in {"AUTO_REBUILD", "AUTOMATIC"}:
        return "automatic_repair_available"
    return "automatic_repair_available"


def _problem_text(plan: dict[str, Any], mode: str) -> str:
    domain = str(plan.get("domain_name") or plan.get("domain_id") or "Domain")
    reason = str(plan.get("reason") or "Domain delayed")
    diagnostics = plan.get("diagnostics") if isinstance(plan.get("diagnostics"), dict) else {}
    missing_symbols = diagnostics.get("missing_symbols") if isinstance(diagnostics.get("missing_symbols"), list) else []
    stale_symbols = diagnostics.get("stale_date_symbols") if isinstance(diagnostics.get("stale_date_symbols"), list) else []
    invalid_rows = diagnostics.get("invalid_ohlcv_rows") if isinstance(diagnostics.get("invalid_ohlcv_rows"), list) else []
    if str(plan.get("domain_id") or "") == "US_EQUITIES_EOD" and (missing_symbols or stale_symbols or invalid_rows):
        parts = ["US equities EOD provider coverage is incomplete."]
        if missing_symbols:
            parts.append(f"Missing {len(missing_symbols)} symbols.")
        if stale_symbols:
            parts.append(f"{len(stale_symbols)} symbols have stale provider rows.")
        if invalid_rows:
            parts.append(f"{len(invalid_rows)} rows have invalid OHLCV.")
        return " ".join(parts)
    if str(plan.get("domain_id") or "") == "US_EQUITIES_EOD" and "FINAL_EOD_ARTIFACT_MISSING" in reason:
        return "US equities EOD artifact is missing. Aegis can rebuild it."
    if mode == "SOURCE_SETUP_REQUIRED":
        return f"{domain} source is missing. Provide {_source_label(plan)}."
    if mode == "AUTO_REBUILD":
        if "NOT_CERTIFIED" in reason:
            return f"{domain} is not certified. Aegis rebuilds the governed source artifact, validates coverage, and recertifies it automatically."
        return f"{domain} artifact is missing. Aegis rebuilds and recertifies it automatically."
    if mode == "PROVIDER_WAIT":
        return f"{domain} is waiting on provider data."
    return f"{domain} is delayed: {reason}."


def _impact(plan: dict[str, Any]) -> str:
    sleeves = [str(item) for item in plan.get("affected_sleeves") or [] if item]
    hypotheses = [str(item) for item in plan.get("affected_hypotheses") or [] if item]
    parts = []
    if sleeves:
        parts.append("Affects sleeves: " + ", ".join(sleeves))
    if hypotheses:
        parts.append("Affects hypotheses: " + ", ".join(hypotheses))
    return " | ".join(parts) if parts else "No direct sleeve or hypothesis impact recorded."


def _primary_command(item: dict[str, Any], day_utc: str) -> dict[str, Any]:
    domain_id = str(item.get("domain_id") or "")
    status = str(item.get("status") or "")
    mode = str(item.get("repair_mode") or "")
    payload = {"domain_id": domain_id, "repair_id": item.get("repair_id"), "day_utc": day_utc, "operational_day": day_utc}
    if status in {"QUEUED", "RUNNING", "VALIDATING", "RECERTIFYING", "COMPLETED"}:
        return command_instance_v1("VIEW_REPAIR_JOB", target_type="repair_item", target_id=domain_id, label="View Job", payload=payload)
    if status == "FAILED":
        return command_instance_v1("RECHECK_DOMAIN", target_type="domain_certification", target_id=domain_id, label="Recheck Domain", payload=payload)
    if status == "SOURCE_CONFIGURED":
        return command_instance_v1("VALIDATE_SOURCE", target_type="domain_source", target_id=domain_id, label="Validate Source", payload=payload)
    if mode == "SOURCE_SETUP_REQUIRED":
        source = item.get("required_source") if isinstance(item.get("required_source"), dict) else {}
        if source.get("exists"):
            return command_instance_v1("VALIDATE_SOURCE", target_type="domain_source", target_id=domain_id, label="Validate Source", payload=payload)
        return command_instance_v1("VIEW_SOURCE_SETUP", target_type="domain_source", target_id=domain_id, label="View setup requirements", payload=payload)
    return command_instance_v1("REPAIR_DOMAIN", target_type="domain_certification", target_id=domain_id, label="Queue Repair", payload=payload)


def _secondary_commands(item: dict[str, Any], day_utc: str) -> list[dict[str, Any]]:
    domain_id = str(item.get("domain_id") or "")
    payload = {"domain_id": domain_id, "repair_id": item.get("repair_id"), "day_utc": day_utc, "operational_day": day_utc}
    commands = [
        command_instance_v1("VIEW_REPAIR_JOB", target_type="repair_item", target_id=domain_id, label="View Job", payload=payload),
        command_instance_v1("RECHECK_DOMAIN", target_type="domain_certification", target_id=domain_id, label="Recheck Domain", payload=payload),
    ]
    if str(item.get("repair_mode") or "") == "SOURCE_SETUP_REQUIRED":
        if domain_id == "US_EQUITIES_EOD":
            commands.insert(1, command_instance_v1("DOWNLOAD_EOD_SOURCE_TEMPLATE", target_type="domain_source", target_id=domain_id, label="Download CSV template", payload=payload))
            commands.insert(2, command_instance_v1("VALIDATE_SOURCE", target_type="domain_source", target_id=domain_id, label="Validate source file", payload=payload))
            commands.insert(3, command_instance_v1("CERTIFY_SOURCE", target_type="domain_source", target_id=domain_id, label="Certify from source file", payload=payload))
        else:
            commands.insert(1, command_instance_v1("DOWNLOAD_SOURCE_TEMPLATE", target_type="domain_source", target_id=domain_id, label="Download template", payload=payload))
            commands.insert(2, command_instance_v1("UPLOAD_SOURCE", target_type="domain_source", target_id=domain_id, label="Configure source path", payload=payload))
            commands.insert(3, command_instance_v1("VALIDATE_SOURCE", target_type="domain_source", target_id=domain_id, label="Validate Source", payload=payload))
        commands = [dict(command, label="Re-run certification") if command.get("command_id") == "RECHECK_DOMAIN" else command for command in commands]
    return commands


def _repair_item_from_plan(plan: dict[str, Any], *, day_utc: str, latest_row: dict[str, Any] | None, lifecycle_row: dict[str, Any] | None = None) -> dict[str, Any]:
    result = _command_result(latest_row)
    lifecycle = lifecycle_row if isinstance(lifecycle_row, dict) else {}
    lifecycle_failure = _lifecycle_failure_result(lifecycle, str(plan.get("domain_id") or ""))
    if lifecycle_failure and (str(result.get("result_status") or "").upper() != "FAILED" or "queued" in str(result.get("plain_english_result") or "").lower()):
        result = lifecycle_failure
    mode = _repair_mode(plan)
    status = str(lifecycle.get("repair_stage") or "") or _status_for(plan, result)
    if status == "DETECTED":
        status = "OPEN"
    source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
    projected_status = _status_for(plan, result)
    if status in {"SOURCE_SETUP_REQUIRED", "WAITING_FOR_INPUT", "OPEN"} and projected_status in {"SOURCE_CONFIGURED", "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT", "NOT_APPLICABLE_FOR_CURRENT_MODE"}:
        status = projected_status
    if status in {"SOURCE_CONFIGURED", "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT", "NOT_APPLICABLE_FOR_CURRENT_MODE"}:
        exact_result = _exact_external_requirement_result(plan, status=status)
        result = {**exact_result, "audit_id": str(result.get("audit_id") or exact_result.get("audit_id") or ""), "timestamp": str(result.get("timestamp") or exact_result.get("timestamp") or "")}
    requested_at = str(latest_row.get("requested_at") if latest_row else "")
    job_id = str(result.get("job_id") or (latest_row or {}).get("state_after", {}).get("job_id") if latest_row else "")
    problem_text = _problem_text(plan, mode)
    failure_details = result.get("failure_details") if isinstance(result.get("failure_details"), dict) else {}
    failed_missing = failure_details.get("missing_symbols") if isinstance(failure_details.get("missing_symbols"), list) else []
    failed_stale = failure_details.get("stale_date_symbols") if isinstance(failure_details.get("stale_date_symbols"), list) else []
    failed_invalid = failure_details.get("invalid_ohlcv_rows") if isinstance(failure_details.get("invalid_ohlcv_rows"), list) else []
    if str(plan.get("domain_id") or "") == "US_EQUITIES_EOD" and (failed_missing or failed_stale or failed_invalid):
        parts = ["US equities EOD provider coverage is incomplete."]
        if failed_missing:
            parts.append(f"Missing {len(failed_missing)} symbols.")
        if failed_stale:
            parts.append(f"{len(failed_stale)} symbols have stale provider rows.")
        if failed_invalid:
            parts.append(f"{len(failed_invalid)} rows have invalid OHLCV.")
        problem_text = " ".join(parts)
    item = {
        "repair_id": f"repair:{day_utc}:{plan.get('domain_id')}:{stable_hash_v1({'reason': plan.get('reason'), 'source': source})[:12]}",
        "domain_id": str(plan.get("domain_id") or ""),
        "target_id": str(plan.get("target_id") or plan.get("domain_id") or ""),
        "issue_type": str(plan.get("reason") or "DOMAIN_DELAYED"),
        "reason": str(plan.get("reason") or "Domain delayed"),
        "affected_sleeves": list(plan.get("affected_sleeves") or []),
        "affected_hypotheses": list(plan.get("affected_hypotheses") or []),
        "required_artifact_source": _source_label(plan),
        "required_source": source,
        "repair_mode": mode,
        "status": status,
        "requested_at": requested_at or str(lifecycle.get("created_at_utc") or ""),
        "started_at": str(result.get("started_at") or (lifecycle.get("created_at_utc") if status == "RUNNING" else "") or ""),
        "completed_at": str(result.get("completed_at") or (lifecycle.get("created_at_utc") if status in {"COMPLETED", "FAILED"} else "") or ""),
        "job_id": job_id or str(lifecycle.get("repair_attempt_id") or ""),
        "audit_id": str(result.get("audit_id") or ""),
        "result": result,
        "next_step": str(result.get("next_required_step") or lifecycle.get("status_reason") or plan.get("repair_action") or "Review repair guidance."),
        "plain_english_problem": problem_text,
        "impact": _impact(plan),
        "last_attempt": str(plan.get("last_attempt") or ""),
        "next_retry": str(lifecycle.get("next_retry_utc") or plan.get("next_retry") or ""),
        "operator_action_required": bool(plan.get("operator_action_required") or mode == "SOURCE_SETUP_REQUIRED"),
        "provider_feed_name": str(source.get("artifact_type") or source.get("label") or ""),
        "source_config_key": _source_config_key(plan),
        "diagnostics": plan.get("diagnostics") if isinstance(plan.get("diagnostics"), dict) else {},
        "source_options": _source_options_for_plan(plan),
        "source_setup_workflow": _source_setup_workflow_for_plan(plan) if mode == "SOURCE_SETUP_REQUIRED" else {},
        "not_system_failure_explanation": "Requires external source; not a system failure." if status == "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT" else "",
        "timeout_escalation_threshold": "Escalate if still unresolved after the next retry window.",
        "progress_stages": ["Queued", "Running", "Artifact rebuild", "Validation", "Domain recertification", "Complete / Failed"],
    }
    coverage_action = (item.get("diagnostics") or {}).get("provider_coverage_action_item") if isinstance(item.get("diagnostics"), dict) else {}
    if item.get("domain_id") == "US_EQUITIES_EOD" and isinstance(coverage_action, dict) and str(coverage_action.get("status") or "").upper() == "PROVIDER_COVERAGE_INCOMPLETE":
        item["next_step"] = "Upload/configure EOD source via AEGIS_US_EQUITIES_EOD_SOURCE_FILE, then validate and certify from source file."
    item["primary_command"] = _primary_command(item, day_utc)
    item["secondary_commands"] = _secondary_commands(item, day_utc)
    item["section_id"] = _section_for(item)
    return item


def _completed_items_from_audit(rows: list[dict[str, Any]], *, day_utc: str, open_domain_ids: set[str]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for domain_id, row in sorted(_latest_repair_command_by_domain(rows).items()):
        if domain_id in open_domain_ids:
            continue
        result = _command_result(row)
        status = str(result.get("result_status") or row.get("result") or "").upper()
        if status not in {"COMPLETED", "SUCCEEDED", "RECERTIFIED"}:
            continue
        item = {
            "repair_id": f"repair:{day_utc}:{domain_id}:completed",
            "domain_id": domain_id,
            "target_id": domain_id,
            "issue_type": "PREVIOUS_REPAIR",
            "reason": "Previous repair command completed.",
            "affected_sleeves": [],
            "affected_hypotheses": [],
            "required_artifact_source": "No source currently missing.",
            "required_source": {},
            "repair_mode": "AUTOMATIC",
            "status": "COMPLETED",
            "requested_at": str(row.get("requested_at") or ""),
            "started_at": "",
            "completed_at": str(result.get("timestamp") or row.get("requested_at") or ""),
            "job_id": str(result.get("job_id") or ""),
            "audit_id": str(result.get("audit_id") or row.get("audit_id") or ""),
            "result": result,
            "next_step": "No further repair action is required.",
            "plain_english_problem": f"{domain_id} repair completed.",
            "impact": "No open domain blocker remains for this repair item.",
            "last_attempt": str(row.get("requested_at") or ""),
            "next_retry": "",
            "provider_feed_name": "",
            "timeout_escalation_threshold": "",
            "progress_stages": ["Queued", "Running", "Artifact rebuild", "Validation", "Domain recertification", "Complete / Failed"],
        }
        item["primary_command"] = _primary_command(item, day_utc)
        item["secondary_commands"] = _secondary_commands(item, day_utc)
        item["section_id"] = "repair_completed"
        items.append(item)
    return items


def _latest_json_artifact(root: Path, pattern_root: Path, filename: str) -> dict[str, Any]:
    if not pattern_root.exists():
        return {}
    paths = sorted(pattern_root.rglob(filename), key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True)
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            payload["_artifact_path"] = str(path)
            return payload
    return {}


def _eod_report_visibility_item(root: Path, *, day_utc: str) -> dict[str, Any]:
    report = _latest_json_artifact(root, root / "reports" / "aegis_lite_eod_report_v1" / day_utc, "aegis_lite_eod_report.v1.json")
    if not report:
        return {}
    outcome = str(report.get("eod_outcome_status") or "")
    if outcome not in {"NORMAL_NO_OP_NO_PROMOTED_CANDIDATES", "BLOCKED_MISSING_REQUIRED_INPUT", "BLOCKED_UNCERTIFIED_SYMBOLS", "BLOCKED_PROMOTION_POLICY"}:
        return {}
    audit_path = Path(str(report.get("candidate_consumption_audit_artifact_path") or ""))
    audit: dict[str, Any] = {}
    if audit_path.exists():
        try:
            parsed = json.loads(audit_path.read_text(encoding="utf-8"))
            audit = parsed if isinstance(parsed, dict) else {}
        except Exception:
            audit = {}
    counts = audit.get("consumption_counts") if isinstance(audit.get("consumption_counts"), dict) else {}
    raw_count = int(audit.get("raw_candidate_count") or (report.get("eod_input_contract") or {}).get("raw_candidate_count") or 0)
    promoted_count = int(audit.get("promoted_candidate_count") or 0)
    excluded_count = int(audit.get("excluded_candidate_count") or max(raw_count - promoted_count, 0))
    count_text = ", ".join(f"{key}: {value}" for key, value in sorted(counts.items())) or "No exclusion counts recorded"
    normal_noop = outcome == "NORMAL_NO_OP_NO_PROMOTED_CANDIDATES"
    item = {
        "repair_id": f"repair:{day_utc}:AEGIS_LITE_EOD_REPORT:{stable_hash_v1({'outcome': outcome, 'path': report.get('_artifact_path')})[:12]}",
        "domain_id": "AEGIS_LITE_EOD_REPORT",
        "target_id": "AEGIS_LITE_EOD_REPORT",
        "issue_type": outcome,
        "reason": outcome,
        "affected_sleeves": [],
        "affected_hypotheses": [],
        "required_artifact_source": str(report.get("candidate_consumption_audit_artifact_path") or "candidate_consumption_audit_v1"),
        "required_source": {},
        "repair_mode": "AUTOMATIC" if normal_noop else "MANUAL_ADMIN_ACTION",
        "status": "COMPLETED" if normal_noop else "OPEN",
        "requested_at": "",
        "started_at": "",
        "completed_at": str(report.get("generated_at_utc") or "") if normal_noop else "",
        "job_id": "",
        "audit_id": "",
        "result": {
            "schema_id": "aegis_command_result_panel.v1",
            "status_label": "Normal no-op" if normal_noop else "Blocked",
            "result_status": "COMPLETED" if normal_noop else "BLOCKED",
            "plain_english_result": f"EOD report outcome: {outcome}. Raw candidates: {raw_count}; promoted: {promoted_count}; excluded: {excluded_count}. {count_text}.",
            "next_required_step": "No user action is required unless an operator expected promoted candidates." if normal_noop else "Review the candidate consumption audit and promotion governance inputs.",
            "job_id": "",
            "audit_id": str(audit_path) if audit_path.exists() else "",
            "timestamp": str(report.get("generated_at_utc") or ""),
        },
        "next_step": "No user action is required unless an operator expected promoted candidates." if normal_noop else "Review the candidate consumption audit and promotion governance inputs.",
        "plain_english_problem": "EOD report completed with no promoted candidates." if normal_noop else "EOD report is blocked before promoted candidates can be reported.",
        "impact": f"Affected report: aegis_lite_eod_report. Raw candidates: {raw_count}. Promoted: {promoted_count}. Excluded: {excluded_count}. {count_text}.",
        "last_attempt": str(report.get("generated_at_utc") or ""),
        "next_retry": "Not scheduled",
        "operator_action_required": not normal_noop,
        "provider_feed_name": "",
        "source_config_key": "",
        "diagnostics": {"eod_outcome_status": outcome, "candidate_consumption_counts": counts, "report_path": str(report.get("_artifact_path") or "")},
        "timeout_escalation_threshold": "No escalation required for normal no-op outcome." if normal_noop else "Escalate if promotion governance artifacts are expected but missing.",
        "progress_stages": ["Candidate manifest read", "Promotion filtering", "Coverage validation", "EOD report", "Complete / Blocked"],
    }
    item["primary_command"] = _primary_command(item, day_utc)
    item["secondary_commands"] = _secondary_commands(item, day_utc)
    item["section_id"] = "repair_completed" if normal_noop else "source_setup_required"
    return item


def build_repair_center_projection_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    report = build_domain_certification_report_v1(truth_root=root, day_utc=day_utc)
    lifecycle = read_domain_repair_lifecycle_v1(truth_root=root, day_utc=day_utc)
    latest_lifecycle = lifecycle.get("latest_by_domain") if isinstance(lifecycle.get("latest_by_domain"), dict) else {}
    audit_rows = _read_command_audit_rows(root, day_utc)
    latest_by_domain = _latest_repair_command_by_domain(audit_rows)
    plans = [row for row in report.get("domain_repair_actions") or [] if isinstance(row, dict)]
    items = [_repair_item_from_plan(plan, day_utc=day_utc, latest_row=latest_by_domain.get(str(plan.get("domain_id") or "")), lifecycle_row=latest_lifecycle.get(str(plan.get("domain_id") or ""))) for plan in plans]
    eod_item = _eod_report_visibility_item(root, day_utc=day_utc)
    if eod_item:
        items.append(eod_item)
    open_domain_ids = {str(item.get("domain_id") or "") for item in items}
    items.extend(_completed_items_from_audit(audit_rows, day_utc=day_utc, open_domain_ids=open_domain_ids))
    sections = []
    for section_id, label in SECTIONS:
        visible = [item for item in items if item.get("section_id") == section_id]
        sections.append({"section_id": section_id, "label": label, "count": len(visible), "items": visible})
    summary = {
        "total_open": len([item for item in items if item.get("status") not in TERMINAL_REPAIR_STATUSES]),
        "automatic_available": len([item for item in items if item.get("section_id") == "automatic_repair_available"]),
        "source_setup_required": len([item for item in items if item.get("section_id") == "source_setup_required"]),
        "blocked_external_requirements": len([item for item in items if item.get("status") == "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT"]),
        "source_configured": len([item for item in items if item.get("status") == "SOURCE_CONFIGURED"]),
        "not_applicable_for_current_mode": len([item for item in items if item.get("status") == "NOT_APPLICABLE_FOR_CURRENT_MODE"]),
        "repair_running": len([item for item in items if item.get("section_id") == "repair_running"]),
        "repair_failed": len([item for item in items if item.get("section_id") == "repair_failed"]),
        "repair_completed": len([item for item in items if item.get("section_id") == "repair_completed"]),
    }
    summary_banner = {
        "title": "Core system operational" if summary.get("repair_failed", 0) == 0 else "Repair attention required",
        "message": f"Core system operational. {int((report.get('summary') or {}).get('certified_domains') or 0)} domains certified. {int(summary.get('blocked_external_requirements') or 0)} external source requirements remain. {int((report.get('summary') or {}).get('blocked_sleeves') or 0)} sleeve blocked by macro-calendar source.",
        "core_system_operational": summary.get("repair_failed", 0) == 0,
        "certified_domains": int((report.get("summary") or {}).get("certified_domains") or 0),
        "external_source_requirements_remaining": int(summary.get("blocked_external_requirements") or 0),
        "blocked_sleeves": int((report.get("summary") or {}).get("blocked_sleeves") or 0),
        "blocking_source_domain": "MACRO_CALENDAR" if any(str(item.get("domain_id") or "") == "MACRO_CALENDAR" and item.get("status") == "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT" for item in items) else "",
    }
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": report.get("generated_at_utc") or "",
        "summary": summary,
        "summary_banner": summary_banner,
        "sections": sections,
        "repair_items": items,
        "domain_certification_summary": report.get("summary") or {},
        "domain_source_registry": report.get("domain_source_registry") or {},
        "domain_repair_lifecycle": lifecycle,
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }
    payload["content_hash"] = stable_hash_v1(payload)
    return payload
