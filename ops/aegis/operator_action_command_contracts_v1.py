from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from ops.aegis.domain_certification_v1 import build_domain_certification_report_v1, write_domain_certification_report_v1
from ops.aegis.domain_source_builders_v1 import build_domain_source_artifact_v1, setup_requirements_v1

SCHEMA_ID = "aegis_operator_command_registry"
SCHEMA_VERSION = "v1"
AUDIT_FAMILY = "aegis_operator_command_audit_v1"

API_COMMAND = "API_COMMAND"
IN_PAGE_DETAIL = "IN_PAGE_DETAIL"
SCROLL_FOCUS = "SCROLL_FOCUS"
EXPAND_SECTION = "EXPAND_SECTION"
EXTERNAL_LINK = "EXTERNAL_LINK"

COMMAND_IDS = {
    "START_RESEARCH",
    "VIEW_FINDINGS",
    "VIEW_WAITING_REASON",
    "VIEW_BLOCKER",
    "REPAIR_DOMAIN",
    "VIEW_QUEUE",
    "VIEW_RECOMMENDATION",
    "VIEW_PROGRESS",
    "MARK_CAPTURE_COMPLETE",
    "SCROLL_TO_HYPOTHESIS_SECTION",
    "FOCUS_HYPOTHESIS_CARD",
    "OPEN_VALID_ROUTE",
    "OPEN_IN_PAGE_MODAL",
    "VALIDATE_SOURCE",
    "UPLOAD_SOURCE",
    "DOWNLOAD_EOD_SOURCE_TEMPLATE",
    "CERTIFY_SOURCE",
    "RECHECK_DOMAIN",
    "VIEW_REPAIR_JOB",
}

REQUIRED_COMMAND_FIELDS = (
    "command_id",
    "label",
    "applies_to",
    "visible_when",
    "enabled_when",
    "action_type",
    "endpoint",
    "route",
    "input_schema",
    "success_state_transition",
    "failure_behavior",
    "audit_artifact_type",
    "safety_classification",
    "browser_acceptance_test_required",
)

SAFETY = {
    "review_only": True,
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "live_trading_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "automatic_approval_allowed": False,
    "automatic_promotion_allowed": False,
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def command_audit_dir_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / AUDIT_FAMILY / day_utc


def command_audit_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return command_audit_dir_v1(truth_root=truth_root, day_utc=day_utc) / "operator_command_audit.v1.jsonl"


def _command(
    command_id: str,
    label: str,
    applies_to: str,
    visible_when: str,
    enabled_when: str,
    action_type: str,
    *,
    endpoint: str = "",
    route: str = "",
    input_schema: dict[str, Any] | None = None,
    success_state_transition: str = "UI state remains unchanged until the authoritative projection refreshes.",
    failure_behavior: str = "Show inline error and keep the operator on the current Aegis page.",
    audit_artifact_type: str = "operator_command_audit.v1",
    safety_classification: dict[str, Any] | None = None,
    browser_acceptance_test_required: bool = True,
) -> dict[str, Any]:
    if command_id not in COMMAND_IDS:
        raise ValueError(f"unregistered_command_id:{command_id}")
    return {
        "command_id": command_id,
        "label": label,
        "applies_to": applies_to,
        "visible_when": visible_when,
        "enabled_when": enabled_when,
        "action_type": action_type,
        "endpoint": endpoint,
        "route": route,
        "input_schema": input_schema or {"type": "object", "additionalProperties": True},
        "success_state_transition": success_state_transition,
        "failure_behavior": failure_behavior,
        "audit_artifact_type": audit_artifact_type,
        "safety_classification": safety_classification or dict(SAFETY),
        "browser_acceptance_test_required": browser_acceptance_test_required,
    }


def command_registry_v1() -> dict[str, Any]:
    commands = [
        _command(
            "START_RESEARCH",
            "Start Research",
            "hypothesis",
            "Hypothesis is Ready to Start or Monitoring and not blocked.",
            "hypothesis_id is present and no blocker is active.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["hypothesis_id"], "properties": {"hypothesis_id": {"type": "string"}, "title": {"type": "string"}, "symbols": {"type": "string"}}},
            success_state_transition="Create USER_INITIATED research-run ledger event and move projection to Queued/Researching after refresh.",
        ),
        _command("VIEW_QUEUE", "View Queue", "hypothesis", "Hypothesis has queued or scheduled research.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page queue/progress details."),
        _command("VIEW_PROGRESS", "View Progress", "hypothesis", "Hypothesis has active research.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page progress details."),
        _command("VIEW_WAITING_REASON", "View Waiting Reason", "hypothesis", "Hypothesis is waiting.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page waiting reason with dependency, next retry, and expected unblock when known."),
        _command("VIEW_BLOCKER", "View Blocker", "hypothesis", "Hypothesis is blocked.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page blocker details."),
        _command("VIEW_FINDINGS", "View Findings", "hypothesis", "Hypothesis is complete or findings may exist.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open findings panel; show no-findings explanation if findings are not available."),
        _command("VIEW_RECOMMENDATION", "View Recommendation", "hypothesis", "Manual recommendation is ready.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open recommendation panel with manual IB capture guidance."),
        _command(
            "REPAIR_DOMAIN",
            "Repair",
            "domain_certification",
            "Domain certification status is DELAYED.",
            "domain_id is present and a delayed-domain repair plan exists.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["domain_id"], "properties": {"domain_id": {"type": "string"}}},
            success_state_transition="Source-missing domains return SOURCE_SETUP_REQUIRED; automatic EOD repair queues an auditable remediation job.",
        ),
        _command(
            "VALIDATE_SOURCE",
            "Validate Source",
            "domain_source",
            "A source-missing repair item has a required source path.",
            "Required source path exists or can be checked.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["domain_id"], "properties": {"domain_id": {"type": "string"}}},
            success_state_transition="Validate the configured source path and report whether certification can be rerun.",
        ),
        _command(
            "UPLOAD_SOURCE",
            "Upload/configure source",
            "domain_source",
            "A source-missing repair item requires operator/admin input.",
            "Required source path is known.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["domain_id"], "properties": {"domain_id": {"type": "string"}}},
            success_state_transition="Return exact source setup requirements; Aegis does not fabricate source files.",
        ),
        _command(
            "DOWNLOAD_EOD_SOURCE_TEMPLATE",
            "Download CSV template",
            "domain_source",
            "US_EQUITIES_EOD requires uploaded EOD source coverage.",
            "Operational day is present.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "properties": {"domain_id": {"type": "string"}, "day_utc": {"type": "string"}}},
            success_state_transition="Write the required US equities EOD universe and return a CSV template path.",
        ),
        _command(
            "CERTIFY_SOURCE",
            "Certify from source file",
            "domain_source",
            "US_EQUITIES_EOD has a configured uploaded EOD source file.",
            "AEGIS_US_EQUITIES_EOD_SOURCE_FILE points to a source file or payload.source_file is provided.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "properties": {"domain_id": {"type": "string"}, "source_file": {"type": "string"}, "day_utc": {"type": "string"}}},
            success_state_transition="Validate uploaded source, write canonical final EOD artifact, and rerun domain certification.",
        ),
        _command(
            "RECHECK_DOMAIN",
            "Recheck Domain",
            "domain_certification",
            "A repair item needs domain recertification.",
            "Domain id is present.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["domain_id"], "properties": {"domain_id": {"type": "string"}}},
            success_state_transition="Rebuild and persist the domain certification report for the operational day.",
        ),
        _command("VIEW_REPAIR_JOB", "View Job", "repair_item", "A repair command or remediation job exists.", "Repair item is present in the current projection.", IN_PAGE_DETAIL, success_state_transition="Open in-page repair job details."),
        _command(
            "MARK_CAPTURE_COMPLETE",
            "Mark IB capture complete",
            "manual_capture_ticket",
            "A valid manual-capture ticket is ready for operator recording.",
            "Ticket lineage and submit boundary hashes are present.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["ticket_id"], "properties": {"ticket_id": {"type": "string"}}},
            success_state_transition="Append manual capture record; Aegis still does not route or transmit orders.",
        ),
        _command("SCROLL_TO_HYPOTHESIS_SECTION", "Show Section", "hypotheses_summary", "Summary section has visible count.", "Target section exists in DOM.", SCROLL_FOCUS, success_state_transition="Expand and scroll/focus the target section."),
        _command("FOCUS_HYPOTHESIS_CARD", "Show Hypothesis", "hypotheses_summary", "Summary top item exists.", "Target card exists in DOM or section can be expanded.", SCROLL_FOCUS, success_state_transition="Expand section and focus/highlight the target card."),
        _command("OPEN_VALID_ROUTE", "Open", "navigation", "A declared route exists.", "Route is registered in navigation schema.", EXPAND_SECTION, success_state_transition="Use in-app route navigation only."),
        _command("OPEN_IN_PAGE_MODAL", "Open Details", "in_page_modal", "A modal/detail surface exists in the current page.", "Target modal exists in DOM.", IN_PAGE_DETAIL, success_state_transition="Open the target modal/detail surface in-page."),
    ]
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "commands": commands,
        "commands_by_id": {row["command_id"]: row for row in commands},
        **SAFETY,
    }


def command_contract_v1(command_id: str) -> dict[str, Any]:
    registry = command_registry_v1()["commands_by_id"]
    command = registry.get(str(command_id or ""))
    if not command:
        raise KeyError(f"UNKNOWN_COMMAND_ID:{command_id}")
    return command


def validate_command_registry_v1() -> dict[str, Any]:
    registry = command_registry_v1()
    errors: list[str] = []
    for command in registry["commands"]:
        for field in REQUIRED_COMMAND_FIELDS:
            if field not in command:
                errors.append(f"{command.get('command_id')}:missing:{field}")
        if command.get("action_type") == API_COMMAND and command.get("endpoint") != "/api/aegis/commands/execute":
            errors.append(f"{command.get('command_id')}:api_command_missing_router_endpoint")
    return {"ok": not errors, "errors": errors, "command_count": len(registry["commands"]), "registry": registry}


def command_for_hypothesis_status_v1(status: str) -> dict[str, Any]:
    command_id = {
        "Ready to Start": "START_RESEARCH",
        "Monitoring": "START_RESEARCH",
        "Queued": "VIEW_QUEUE",
        "Scheduled": "VIEW_QUEUE",
        "Researching": "VIEW_PROGRESS",
        "Waiting": "VIEW_WAITING_REASON",
        "Complete": "VIEW_FINDINGS",
        "Blocked": "VIEW_BLOCKER",
        "Recommendation Ready": "VIEW_RECOMMENDATION",
    }.get(str(status or ""), "START_RESEARCH")
    return command_contract_v1(command_id)


def command_instance_v1(command_id: str, *, target_type: str, target_id: str, label: str | None = None, payload: dict[str, Any] | None = None, enabled: bool = True, disabled_reason: str = "") -> dict[str, Any]:
    contract = command_contract_v1(command_id)
    return {
        **contract,
        "target_type": target_type,
        "target_id": target_id,
        "payload": payload or {},
        "label": label or contract["label"],
        "enabled": bool(enabled),
        "disabled_reason": disabled_reason,
    }


def _audit_record(command: dict[str, Any], *, truth_root: Path | str, day_utc: str, request_payload: dict[str, Any], result: dict[str, Any], state_before: dict[str, Any] | None = None, state_after: dict[str, Any] | None = None) -> dict[str, Any]:
    requested_at = utc_now_v1()
    record = {
        "schema_id": "aegis_operator_command_audit",
        "schema_version": "v1",
        "command_id": command["command_id"],
        "target_type": str(request_payload.get("target_type") or ""),
        "target_id": str(request_payload.get("target_id") or request_payload.get("hypothesis_id") or request_payload.get("domain_id") or ""),
        "requested_at": requested_at,
        "requested_by": str(request_payload.get("requested_by") or "operator-ui"),
        "result": result.get("result_status") or ("SUCCEEDED" if result.get("ok") else "FAILED"),
        "state_before": state_before or {},
        "state_after": state_after or {},
        "error": str(result.get("error_message") or ""),
        "request_payload": request_payload,
        "response_summary": {**{key: result.get(key) for key in ("ok", "result_status", "user_message", "next_state", "audit_id")}, "command_result": result.get("command_result") or {}},
        **SAFETY,
    }
    record["audit_id"] = f"operator-command:{record['command_id']}:{stable_hash_v1(record)[:20]}"
    if isinstance(record.get("response_summary"), dict) and isinstance(record["response_summary"].get("command_result"), dict):
        record["response_summary"]["command_result"]["audit_id"] = record["audit_id"]
        record["response_summary"]["command_result"].setdefault("timestamp", record["requested_at"])
    record["content_hash"] = stable_hash_v1(record)
    path = command_audit_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    return {**record, "path": str(path)}


def _command_result_panel_v1(
    *,
    status_label: str,
    plain_english_result: str,
    next_required_step: str,
    result_status: str,
    job_id: str = "",
    audit_id: str = "",
    timestamp: str = "",
    missing_source_name: str = "",
    expected_source_path: str = "",
    copy_required_path: str = "",
    failure_reason: str = "",
    next_recheck_time: str = "",
) -> dict[str, Any]:
    return {
        "schema_id": "aegis_command_result_panel.v1",
        "status_label": status_label,
        "result_status": result_status,
        "plain_english_result": plain_english_result,
        "next_required_step": next_required_step,
        "job_id": job_id,
        "audit_id": audit_id,
        "timestamp": timestamp or utc_now_v1(),
        "missing_source_name": missing_source_name,
        "expected_source_path": expected_source_path,
        "copy_required_path": copy_required_path or expected_source_path,
        "failure_reason": failure_reason,
        "next_recheck_time": next_recheck_time,
    }


def _empty_response(command: dict[str, Any], *, status: str, message: str, next_state: str = "UNCHANGED", ok: bool = True, command_result: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback_status = "Success" if ok else "Failed"
    return {
        "ok": ok,
        "command_id": command["command_id"],
        "result_status": status,
        "user_message": message,
        "next_state": next_state,
        "audit_id": "",
        "updated_projection_hint": "refresh_current_workspace",
        "error_message": "" if ok else message,
        "command_result": command_result or _command_result_panel_v1(
            status_label=fallback_status,
            plain_english_result=message,
            next_required_step=next_state if next_state != "UNCHANGED" else "No further action is required from this command.",
            result_status=status,
            failure_reason="" if ok else message,
        ),
        **SAFETY,
    }


def _domain_plan(domain_id: str, *, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    report = build_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc)
    for plan in report.get("domain_repair_actions", []) if isinstance(report.get("domain_repair_actions"), list) else []:
        if str(plan.get("domain_id") or "") == domain_id:
            return plan
    return {}


def execute_aegis_command_v1(
    payload: dict[str, Any],
    *,
    truth_root: Path | str,
    repo_root: Path | str | None = None,
    day_utc: str,
    actor: str = "operator-ui",
    repair_job_runner: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    research_store_root: Path | str | None = None,
) -> dict[str, Any]:
    request = dict(payload or {})
    request.setdefault("requested_by", actor)
    command = command_contract_v1(str(request.get("command_id") or ""))
    target_type = str(request.get("target_type") or "")
    target_id = str(request.get("target_id") or "")
    command_id = command["command_id"]
    result: dict[str, Any]
    state_before: dict[str, Any] = {}
    state_after: dict[str, Any] = {}

    try:
        if command_id == "START_RESEARCH":
            from ops.aegis.research_lab.research_console_v1 import start_research_v1

            body = dict(request.get("payload") or {})
            hypothesis_id = str(body.get("hypothesis_id") or target_id or request.get("hypothesis_id") or "").strip()
            title = str(body.get("title") or body.get("idea") or request.get("title") or hypothesis_id).strip()
            if not hypothesis_id:
                result = _empty_response(command, status="VALIDATION_FAILED", message="Start Research requires a hypothesis_id.", ok=False)
            else:
                start = start_research_v1({"hypothesis_id": hypothesis_id, "idea": title, "symbols": body.get("symbols") or ""}, actor=actor, store_root=Path(research_store_root) if research_store_root else None)
                result = {
                    **_empty_response(command, status="QUEUED", message=start.get("operator_message") or start.get("message") or f"Research started for {title}.", next_state="Queued"),
                    "research_run_id": start.get("research_run_id"),
                    "research_run_status": start.get("research_run_status"),
                    "research_run_trigger_source": start.get("research_run_trigger_source"),
                    "updated_projection_hint": "refresh_hypotheses",
                }
                state_after = {"user_facing_status": "Queued", "research_run_id": start.get("research_run_id")}
        elif command_id == "REPAIR_DOMAIN":
            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "").strip()
            plan = _domain_plan(domain_id, truth_root=truth_root, day_utc=day_utc)
            state_before = plan
            if not plan:
                message = f"No delayed repair plan is available for {domain_id}."
                result = _empty_response(command, status="NO_REPAIR_PLAN", message=message, next_state="UNCHANGED", ok=False, command_result=_command_result_panel_v1(status_label="Failed", plain_english_result=message, next_required_step="Refresh the domain certification report and try again if the domain is still delayed.", result_status="NO_REPAIR_PLAN", failure_reason=message))
            elif plan.get("source_setup_required"):
                source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
                label = str(source.get("label") or "required source")
                path = str(source.get("path") or "")
                build = build_domain_source_artifact_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id)
                if build.get("ok") is True:
                    report = build_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc)
                    paths = write_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc, payload=report)
                    domain_row = next((row for row in report.get("domains", []) if isinstance(row, dict) and str(row.get("domain_id") or "") == domain_id), {})
                    status = str(domain_row.get("certification_status") or "UNKNOWN")
                    message = f"Built source artifact for {domain_id}. Domain status is {status}."
                    result = {
                        **_empty_response(
                            command,
                            status="BUILT",
                            message=message,
                            next_state="RECERTIFIED",
                            command_result=_command_result_panel_v1(
                                status_label="Source built",
                                plain_english_result=message,
                                next_required_step="Review the refreshed domain certification status.",
                                result_status="BUILT",
                                expected_source_path=str(build.get("artifact_path") or path),
                                copy_required_path=str(build.get("artifact_path") or path),
                            ),
                        ),
                        "repair_plan": plan,
                        "build_result": build,
                        "domain_certification_paths": paths,
                    }
                    state_after = {"domain_repair_state": "BUILT", "domain_status": status, "artifact_path": build.get("artifact_path")}
                else:
                    req = setup_requirements_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id)
                    path = str(build.get("required_output_path") or path or req.get("required_output_path") or "")
                    message = str(build.get("message") or f"Source setup required for {domain_id}: {label}{f' at {path}' if path else ''}.")
                    result = {
                        **_empty_response(
                            command,
                            status=str(build.get("result_status") or "SOURCE_SETUP_REQUIRED"),
                            message=message,
                            next_state="Source setup required",
                            ok=str(build.get("result_status") or "").upper() == "SOURCE_SETUP_REQUIRED",
                            command_result=_command_result_panel_v1(
                                status_label="Source setup required",
                                plain_english_result=message,
                                next_required_step=str(plan.get("repair_action") or req.get("setup_message") or "Provide the missing source, validate it, then re-run domain certification."),
                                result_status=str(build.get("result_status") or "SOURCE_SETUP_REQUIRED"),
                                missing_source_name=label,
                                expected_source_path=path,
                                copy_required_path=path,
                                failure_reason=str(build.get("failure_reason") or ""),
                            ),
                        ),
                        "repair_plan": plan,
                        "build_result": build,
                        "missing_source_name": label,
                        "expected_source_path": path,
                    }
                    state_after = {"domain_repair_state": "SOURCE_SETUP_REQUIRED", "required_source_path": path}
            else:
                job = repair_job_runner({"domain_id": domain_id, "day_utc": day_utc, "repair_plan": plan, "requested_by": actor}) if repair_job_runner else {}
                if not job:
                    message = f"Automatic repair for {domain_id} is not available from this runtime."
                    result = _empty_response(command, status="REPAIR_JOB_UNAVAILABLE", message=message, next_state="UNCHANGED", ok=False, command_result=_command_result_panel_v1(status_label="Failed", plain_english_result=message, next_required_step="Use the listed repair steps or source setup instructions, then re-run certification.", result_status="REPAIR_JOB_UNAVAILABLE", failure_reason=message))
                else:
                    job_id = str(job.get("job_id") or "")
                    next_recheck = str(job.get("next_retry_utc") or job.get("queued_at_utc") or "")
                    job_status = str(job.get("status") or "QUEUED").upper()
                    failed_job = job_status in {"FAILED", "ERROR"}
                    failure_reason = str(job.get("failure_reason") or job.get("failure_detail") or "")
                    message = (
                        f"Repair failed for {domain_id}: {failure_reason or job_status}."
                        if failed_job
                        else f"Repair job queued for {domain_id}: {job_id or 'job recorded'}."
                    )
                    result = {
                        **_empty_response(
                            command,
                            status=job_status,
                            message=message,
                            next_state="Repair failed" if failed_job else "Repair queued",
                            ok=not failed_job,
                            command_result=_command_result_panel_v1(
                                status_label="Failed" if failed_job else "Repair job queued",
                                plain_english_result=message,
                                next_required_step="Review the failure reason, fix provider/source configuration, then queue repair again." if failed_job else "Wait for the queued repair to complete, then refresh or re-run domain certification.",
                                result_status=job_status,
                                job_id=job_id,
                                next_recheck_time=next_recheck,
                                failure_reason=failure_reason if failed_job else "",
                            ),
                        ),
                        "repair_plan": plan,
                        "job": job,
                        "job_id": job.get("job_id"),
                        "next_recheck_time": next_recheck,
                    }
                    state_after = {"domain_repair_state": job_status, "job_id": job.get("job_id"), "failure_reason": failure_reason}
        elif command_id == "UPLOAD_SOURCE":
            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "").strip()
            plan = _domain_plan(domain_id, truth_root=truth_root, day_utc=day_utc)
            state_before = plan
            source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
            label = str(source.get("label") or "required source")
            path = str(source.get("path") or "")
            message = f"Source setup required for {domain_id}: provide {label}{f' at {path}' if path else ''}."
            result = _empty_response(
                command,
                status="SOURCE_SETUP_REQUIRED",
                message=message,
                next_state="WAITING_FOR_INPUT",
                ok=True,
                command_result=_command_result_panel_v1(
                    status_label="Source setup required",
                    plain_english_result=message,
                    next_required_step="Provide or configure the required source, then run Validate Source.",
                    result_status="SOURCE_SETUP_REQUIRED",
                    missing_source_name=label,
                    expected_source_path=path,
                    copy_required_path=path,
                ),
            )
            state_after = {"repair_state": "WAITING_FOR_INPUT", "required_source_path": path}
        elif command_id == "DOWNLOAD_EOD_SOURCE_TEMPLATE":
            from ops.tools.manage_us_equities_eod_source_v1 import template_csv_v1, write_required_universe_v1

            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "US_EQUITIES_EOD").strip()
            universe = write_required_universe_v1(truth_root=Path(truth_root), day_utc=day_utc)
            template_path = Path(truth_root).expanduser().resolve() / "reports" / "final_eod_market_data_v1" / day_utc / "us_equities_eod_source_template.csv"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.write_text(template_csv_v1(day_utc=day_utc, symbols=universe["symbols"]), encoding="utf-8")
            message = f"CSV template written for {domain_id}: {template_path}."
            result = _empty_response(command, status="TEMPLATE_READY", message=message, next_state="WAITING_FOR_INPUT", command_result=_command_result_panel_v1(status_label="CSV template ready", plain_english_result=message, next_required_step="Fill every row with target-day OHLCV and set AEGIS_US_EQUITIES_EOD_SOURCE_FILE to the completed file before certifying.", result_status="TEMPLATE_READY", expected_source_path=str(template_path), copy_required_path=str(template_path)))
            state_after = {"template_path": str(template_path), "required_universe_path": universe["path"], "required_universe_size": universe["count"]}
        elif command_id == "CERTIFY_SOURCE":
            from ops.tools.manage_us_equities_eod_source_v1 import certify_source_file_v1

            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "US_EQUITIES_EOD").strip()
            source_file = str(body.get("source_file") or os.environ.get("AEGIS_US_EQUITIES_EOD_SOURCE_FILE") or "").strip()
            if domain_id != "US_EQUITIES_EOD" or not source_file:
                message = "Certify from source file requires AEGIS_US_EQUITIES_EOD_SOURCE_FILE or payload.source_file for US_EQUITIES_EOD."
                result = _empty_response(command, status="SOURCE_SETUP_REQUIRED", message=message, next_state="WAITING_FOR_INPUT", ok=False, command_result=_command_result_panel_v1(status_label="Source setup required", plain_english_result=message, next_required_step="Set AEGIS_US_EQUITIES_EOD_SOURCE_FILE to a complete CSV/JSON file, then retry.", result_status="SOURCE_SETUP_REQUIRED", missing_source_name="AEGIS_US_EQUITIES_EOD_SOURCE_FILE", expected_source_path=source_file, copy_required_path=source_file, failure_reason=message))
                state_after = {"repair_state": "WAITING_FOR_INPUT", "source_exists": False}
            else:
                cert = certify_source_file_v1(source_file=Path(source_file), day_utc=day_utc, truth_root=Path(truth_root))
                build = cert.get("build_result") if isinstance(cert.get("build_result"), dict) else {}
                validation = cert.get("validation") if isinstance(cert.get("validation"), dict) else {}
                if cert.get("ok") is True:
                    message = f"Certified US_EQUITIES_EOD from source file: {source_file}."
                    result = _empty_response(command, status="CERTIFIED", message=message, next_state="RECERTIFIED", command_result=_command_result_panel_v1(status_label="Source certified", plain_english_result=message, next_required_step="Review the refreshed domain certification status.", result_status="CERTIFIED", expected_source_path=str(build.get("artifact_path") or source_file), copy_required_path=str(build.get("artifact_path") or source_file)))
                    state_after = {"repair_state": "CERTIFIED", "source_file": source_file, "artifact_path": build.get("artifact_path"), "certification_status": build.get("certification_status")}
                else:
                    missing = validation.get("missing_symbols") or []
                    stale = validation.get("stale_symbols") or []
                    invalid = validation.get("invalid_ohlcv_rows") or []
                    dupes = validation.get("duplicate_symbols") or []
                    message = f"EOD source failed validation: missing={len(missing)} stale={len(stale)} invalid_ohlcv={len(invalid)} duplicates={len(dupes)}."
                    result = _empty_response(command, status="INVALID_SOURCE", message=message, next_state="WAITING_FOR_INPUT", ok=False, command_result=_command_result_panel_v1(status_label="Source validation failed", plain_english_result=message, next_required_step="Fix the listed source rows and rerun certification.", result_status="INVALID_SOURCE", expected_source_path=source_file, copy_required_path=source_file, failure_reason=message))
                    state_after = {"repair_state": "WAITING_FOR_INPUT", "validation": validation}
        elif command_id == "VALIDATE_SOURCE":
            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "").strip()
            plan = _domain_plan(domain_id, truth_root=truth_root, day_utc=day_utc)
            state_before = plan
            source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
            label = str(source.get("label") or "required source")
            if domain_id == "US_EQUITIES_EOD" and (body.get("source_file") or os.environ.get("AEGIS_US_EQUITIES_EOD_SOURCE_FILE")):
                from ops.tools.manage_us_equities_eod_source_v1 import validate_source_file_v1

                source_file = str(body.get("source_file") or os.environ.get("AEGIS_US_EQUITIES_EOD_SOURCE_FILE") or "")
                validation = validate_source_file_v1(source_file=Path(source_file), day_utc=day_utc, truth_root=Path(truth_root))
                ok = validation.get("validation_status") == "VALID"
                message = f"EOD source validation {'passed' if ok else 'failed'}: coverage {validation.get('coverage_percentage')}%."
                result = _empty_response(command, status="VALIDATED" if ok else "INVALID_SOURCE", message=message, next_state="READY_TO_CERTIFY" if ok else "WAITING_FOR_INPUT", ok=ok, command_result=_command_result_panel_v1(status_label="Source validated" if ok else "Source validation failed", plain_english_result=message, next_required_step="Run Certify from source file." if ok else "Fix missing/stale/invalid/duplicate rows, then validate again.", result_status="VALIDATED" if ok else "INVALID_SOURCE", expected_source_path=source_file, copy_required_path=source_file, failure_reason="" if ok else message))
                state_after = {"repair_state": "VALIDATING", "source_exists": Path(source_file).exists(), "validation": validation}
            else:
                build = build_domain_source_artifact_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id)
                path = str(build.get("artifact_path") or build.get("required_output_path") or source.get("path") or "")
                if build.get("ok") is True:
                    report = build_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc)
                    paths = write_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc, payload=report)
                    domain_row = next((row for row in report.get("domains", []) if isinstance(row, dict) and str(row.get("domain_id") or "") == domain_id), {})
                    status = str(domain_row.get("certification_status") or "UNKNOWN")
                    message = f"Source validated for {domain_id}: {path}. Domain status is {status}."
                    result = _empty_response(command, status="VALIDATED", message=message, next_state="RECERTIFIED", command_result=_command_result_panel_v1(status_label="Source validated", plain_english_result=message, next_required_step="Review the refreshed domain certification status.", result_status="VALIDATED", expected_source_path=path, copy_required_path=path))
                    state_after = {"repair_state": "VALIDATING", "source_exists": True, "domain_status": status, "written_paths": paths}
                else:
                    message = str(build.get("message") or f"Required source for {domain_id} is still missing: {label}{f' at {path}' if path else ''}.")
                    result = _empty_response(command, status=str(build.get("result_status") or "SOURCE_MISSING"), message=message, next_state="WAITING_FOR_INPUT", ok=False, command_result=_command_result_panel_v1(status_label="Source setup required", plain_english_result=message, next_required_step="Provide the missing source, then run Validate Source again.", result_status=str(build.get("result_status") or "SOURCE_MISSING"), missing_source_name=label, expected_source_path=path, copy_required_path=path, failure_reason=message))
                    state_after = {"repair_state": "WAITING_FOR_INPUT", "source_exists": False}
        elif command_id == "RECHECK_DOMAIN":
            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "").strip()
            before_plan = _domain_plan(domain_id, truth_root=truth_root, day_utc=day_utc)
            state_before = before_plan
            build = build_domain_source_artifact_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id) if domain_id in {"US_EQUITIES_EOD", "MACRO_CALENDAR", "EARNINGS_EVENTS", "CORPORATE_ACTIONS"} else {}
            report = build_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc)
            paths = write_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc, payload=report)
            after_plan = next((row for row in report.get("domain_repair_actions", []) if isinstance(row, dict) and str(row.get("domain_id") or "") == domain_id), {})
            domain_row = next((row for row in report.get("domains", []) if isinstance(row, dict) and str(row.get("domain_id") or "") == domain_id), {})
            status = str(domain_row.get("certification_status") or "UNKNOWN")
            if after_plan:
                message = f"Rechecked {domain_id}. Domain remains {status}: {after_plan.get('reason') or 'repair still open'}."
                result_status = "RECERTIFICATION_STILL_DELAYED"
                next_step = str(after_plan.get("repair_action") or "Continue the listed repair steps.")
            else:
                message = f"Rechecked {domain_id}. Domain status is {status}; no open repair plan remains."
                result_status = "COMPLETED"
                next_step = "No further repair action is required for this domain."
            result = _empty_response(command, status=result_status, message=message, next_state="RECERTIFYING", ok=not bool(after_plan), command_result=_command_result_panel_v1(status_label="Domain rechecked" if after_plan else "Repair completed", plain_english_result=message, next_required_step=next_step, result_status=result_status, failure_reason="" if not after_plan else message))
            state_after = {"repair_state": result_status, "domain_status": status, "written_paths": paths, "build_result": build}
        elif command_id == "MARK_CAPTURE_COMPLETE":
            result = _empty_response(command, status="USE_MANUAL_CAPTURE_FORM", message="Use the manual IB capture form. The command contract is registered, but this endpoint does not synthesize capture records without the validated form payload.", next_state="UNCHANGED", ok=False)
        elif command["action_type"] in {IN_PAGE_DETAIL, SCROLL_FOCUS, EXPAND_SECTION, EXTERNAL_LINK}:
            result = _empty_response(command, status="IN_PAGE_ACTION", message=f"{command['label']} is handled by the in-page command surface.", next_state="UNCHANGED")
        else:
            result = _empty_response(command, status="HANDLER_NOT_IMPLEMENTED", message=f"Command {command_id} has no handler.", next_state="UNCHANGED", ok=False)
    except Exception as exc:
        result = _empty_response(command, status="FAILED", message=f"{command['label']} failed: {exc}", next_state="UNCHANGED", ok=False)

    audit = _audit_record(command, truth_root=truth_root, day_utc=day_utc, request_payload=request, result=result, state_before=state_before, state_after=state_after)
    command_result = dict(result.get("command_result") or {})
    command_result["audit_id"] = audit["audit_id"]
    command_result.setdefault("timestamp", audit.get("requested_at") or utc_now_v1())
    return {**result, "audit_id": audit["audit_id"], "audit_record": audit, "command_result": command_result}
