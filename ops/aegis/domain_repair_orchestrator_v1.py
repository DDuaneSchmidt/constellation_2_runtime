from __future__ import annotations

import json
import shlex
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from ops.aegis.domain_certification_v1 import build_domain_certification_report_v1, stable_hash_v1, write_domain_certification_report_v1
from ops.aegis.domain_source_registry_v1 import domain_source_contract_v1, render_domain_source_command_v1, render_domain_source_path_v1

SCHEMA_ID = "aegis_domain_repair_lifecycle"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "domain_repair_lifecycle_v1"

CommandRunner = Callable[[list[str]], dict[str, Any]]


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def lifecycle_dir_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc


def lifecycle_json_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return lifecycle_dir_v1(truth_root=truth_root, day_utc=day_utc) / "domain_repair_lifecycle.v1.json"


def lifecycle_jsonl_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return lifecycle_dir_v1(truth_root=truth_root, day_utc=day_utc) / "domain_repair_lifecycle.v1.jsonl"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except Exception:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _append_event(*, truth_root: Path, day_utc: str, event: dict[str, Any]) -> dict[str, Any]:
    out = lifecycle_dir_v1(truth_root=truth_root, day_utc=day_utc)
    out.mkdir(parents=True, exist_ok=True)
    row = {
        "schema_id": "domain_repair_lifecycle_event",
        "schema_version": "v1",
        "day_utc": day_utc,
        "created_at_utc": event.get("created_at_utc") or utc_now_v1(),
        **event,
    }
    row["event_id"] = row.get("event_id") or f"domain-repair-event:{stable_hash_v1(row)[:24]}"
    path = lifecycle_jsonl_path_v1(truth_root=truth_root, day_utc=day_utc)
    existing = _read_jsonl(path)
    if not any(item.get("event_id") == row["event_id"] for item in existing):
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    return row


def _default_runner(args: list[str]) -> dict[str, Any]:
    started = utc_now_v1()
    completed = ""
    try:
        proc = subprocess.run(args, cwd=str(Path(__file__).resolve().parents[2]), text=True, capture_output=True, timeout=240)
        completed = utc_now_v1()
        return {
            "args": args,
            "started_at_utc": started,
            "completed_at_utc": completed,
            "exit_code": int(proc.returncode),
            "stdout_tail": proc.stdout[-4000:],
            "stderr_tail": proc.stderr[-4000:],
        }
    except Exception as exc:
        completed = utc_now_v1()
        return {
            "args": args,
            "started_at_utc": started,
            "completed_at_utc": completed,
            "exit_code": 99,
            "stdout_tail": "",
            "stderr_tail": str(exc),
        }


def _command_args(command: str, *, truth_root: Path, day_utc: str, domain_id: str) -> list[str]:
    rendered = command.replace("{truth_root}", str(truth_root)).replace("{day_utc}", day_utc).replace("{day}", day_utc).replace("{domain_id}", domain_id)
    args = shlex.split(rendered)
    if args and args[0] == "python3":
        args[0] = sys.executable
    return args


def _eod_rebuild_commands(*, truth_root: Path, day_utc: str) -> list[list[str]]:
    repo = Path(__file__).resolve().parents[2]
    return [
        [sys.executable, str(repo / "ops/tools/refresh_aegis_market_data_v1.py"), "--truth_root", str(truth_root), "--day", day_utc, "--market-data-mode", "FINAL_EOD_CERTIFIED"],
        [sys.executable, str(repo / "ops/tools/build_aegis_market_data_inputs_v1.py"), "--truth-root", str(truth_root), "--day-utc", day_utc, "--market-data-mode", "FINAL_EOD_CERTIFIED", "--emit-events"],
    ]


def _latest_event_by_domain(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in events:
        domain_id = str(row.get("domain_id") or "")
        if not domain_id:
            continue
        previous = latest.get(domain_id)
        if not previous or str(row.get("created_at_utc") or "") >= str(previous.get("created_at_utc") or ""):
            latest[domain_id] = row
    return latest


def read_domain_repair_lifecycle_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    events = _read_jsonl(lifecycle_jsonl_path_v1(truth_root=root, day_utc=day_utc))
    latest = _latest_event_by_domain(events)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": utc_now_v1(),
        "events": events,
        "latest_by_domain": latest,
        "summary": {
            "domain_count": len(latest),
            "completed": len([row for row in latest.values() if str(row.get("repair_stage") or "") == "COMPLETED"]),
            "failed": len([row for row in latest.values() if str(row.get("repair_stage") or "") == "FAILED"]),
            "source_setup_required": len([row for row in latest.values() if str(row.get("repair_stage") or "") == "SOURCE_SETUP_REQUIRED"]),
            "running": len([row for row in latest.values() if str(row.get("repair_stage") or "") in {"QUEUED", "RUNNING", "VALIDATING", "RECERTIFYING"}]),
        },
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }
    payload["content_hash"] = stable_hash_v1(payload)
    return payload


def write_domain_repair_lifecycle_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    out = lifecycle_dir_v1(truth_root=root, day_utc=day_utc)
    out.mkdir(parents=True, exist_ok=True)
    json_path = lifecycle_json_path_v1(truth_root=root, day_utc=day_utc)
    txt_path = out / "domain_repair_lifecycle.v1.txt"
    json_path.write_text(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    lines = ["AEGIS DOMAIN REPAIR LIFECYCLE v1", f"day_utc: {day_utc}", f"content_hash: {payload.get('content_hash')}", ""]
    for domain_id, row in sorted((payload.get("latest_by_domain") or {}).items()):
        lines.append(f"- {domain_id}: {row.get('repair_stage')} mode={row.get('repair_mode')} reason={row.get('reason') or row.get('status_reason') or ''}")
    txt_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return {"json": str(json_path), "txt": str(txt_path), "jsonl": str(lifecycle_jsonl_path_v1(truth_root=root, day_utc=day_utc))}


def _current_domain_status(report: dict[str, Any], domain_id: str) -> dict[str, Any]:
    return next((row for row in report.get("domains", []) if isinstance(row, dict) and str(row.get("domain_id") or "") == domain_id), {})


def run_domain_repair_orchestration_v1(
    *,
    truth_root: Path | str,
    repo_root: Path | str | None = None,
    day_utc: str,
    domain_id: str | None = None,
    execute: bool = False,
    runner: CommandRunner | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    command_runner = runner or _default_runner
    before = build_domain_certification_report_v1(truth_root=root, day_utc=day_utc)
    plans = [row for row in before.get("domain_repair_actions", []) if isinstance(row, dict)]
    if domain_id:
        plans = [row for row in plans if str(row.get("domain_id") or "") == str(domain_id).upper()]
    results: list[dict[str, Any]] = []
    for plan in plans:
        did = str(plan.get("domain_id") or "")
        mode = str(plan.get("repair_mode") or "")
        reason = str(plan.get("reason") or "")
        attempt_id = f"domain-repair:{day_utc}:{did}:{stable_hash_v1({'mode': mode, 'reason': reason})[:16]}"
        _append_event(truth_root=root, day_utc=day_utc, event={"event_type": "DomainRepairDetected", "repair_attempt_id": attempt_id, "domain_id": did, "repair_mode": mode, "repair_stage": "DETECTED", "reason": reason, "repair_plan": plan})
        if mode == "SOURCE_SETUP_REQUIRED":
            contract = domain_source_contract_v1(did)
            source_path = render_domain_source_path_v1(truth_root=root, day_utc=day_utc, contract=contract) if contract else Path("")
            rebuild_template = str(contract.get("rebuild_command") or "") if contract else ""
            command_results: list[dict[str, Any]] = []
            if execute and rebuild_template:
                result = command_runner(_command_args(rebuild_template, truth_root=root, day_utc=day_utc, domain_id=did))
                command_results.append(result)
            if not source_path.exists():
                event = _append_event(truth_root=root, day_utc=day_utc, event={"event_type": "DomainRepairSourceSetupRequired", "repair_attempt_id": attempt_id, "domain_id": did, "repair_mode": mode, "repair_stage": "SOURCE_SETUP_REQUIRED", "reason": reason, "source_path": str(source_path), "setup_required_message": str(contract.get("setup_required_message") or ""), "command_results": command_results})
                results.append({"domain_id": did, "repair_mode": mode, "status": "SOURCE_SETUP_REQUIRED", "event_id": event.get("event_id"), "source_path": str(source_path), "command_results": command_results})
                continue
        if mode != "AUTO_REBUILD" and mode != "SOURCE_SETUP_REQUIRED":
            event = _append_event(truth_root=root, day_utc=day_utc, event={"event_type": "DomainRepairProviderWait", "repair_attempt_id": attempt_id, "domain_id": did, "repair_mode": mode or "PROVIDER_WAIT", "repair_stage": "PROVIDER_WAIT", "reason": reason})
            results.append({"domain_id": did, "repair_mode": mode or "PROVIDER_WAIT", "status": "PROVIDER_WAIT", "event_id": event.get("event_id")})
            continue
        _append_event(truth_root=root, day_utc=day_utc, event={"event_type": "DomainRepairQueued", "repair_attempt_id": attempt_id, "domain_id": did, "repair_mode": mode, "repair_stage": "QUEUED", "reason": reason, "next_retry_utc": (datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=15)).isoformat().replace("+00:00", "Z")})
        if not execute:
            results.append({"domain_id": did, "repair_mode": mode, "status": "QUEUED", "execute": False})
            continue
        _append_event(truth_root=root, day_utc=day_utc, event={"event_type": "DomainRepairRunning", "repair_attempt_id": attempt_id, "domain_id": did, "repair_mode": mode, "repair_stage": "RUNNING", "reason": reason})
        command_results: list[dict[str, Any]] = []
        contract = domain_source_contract_v1(did)
        rebuild_template = str(contract.get("rebuild_command") or "") if contract else ""
        validation_template = str(contract.get("validation_command") or "") if contract else ""
        rebuild_command = render_domain_source_command_v1(command_template=rebuild_template, truth_root=root, day_utc=day_utc, domain_id=did) if rebuild_template else ""
        validation_command = render_domain_source_command_v1(command_template=validation_template, truth_root=root, day_utc=day_utc, domain_id=did) if validation_template else ""
        commands = [_command_args(rebuild_command, truth_root=root, day_utc=day_utc, domain_id=did)] if rebuild_command else (_eod_rebuild_commands(truth_root=root, day_utc=day_utc) if did == "US_EQUITIES_EOD" else [])
        if validation_command:
            commands.append(_command_args(validation_command, truth_root=root, day_utc=day_utc, domain_id=did))
        failed = False
        for args in commands:
            result = command_runner(args)
            command_results.append(result)
            if int(result.get("exit_code") or 0) != 0:
                failed = True
                break
        _append_event(truth_root=root, day_utc=day_utc, event={"event_type": "DomainRepairValidating", "repair_attempt_id": attempt_id, "domain_id": did, "repair_mode": mode, "repair_stage": "VALIDATING", "command_results": command_results})
        after_build = build_domain_certification_report_v1(truth_root=root, day_utc=day_utc)
        paths = write_domain_certification_report_v1(truth_root=root, day_utc=day_utc, payload=after_build)
        _append_event(truth_root=root, day_utc=day_utc, event={"event_type": "DomainRepairRecertifying", "repair_attempt_id": attempt_id, "domain_id": did, "repair_mode": mode, "repair_stage": "RECERTIFYING", "domain_certification_path": paths.get("json", "")})
        domain = _current_domain_status(after_build, did)
        status = str(domain.get("certification_status") or "UNKNOWN")
        if not failed and status == "CERTIFIED":
            event = _append_event(truth_root=root, day_utc=day_utc, event={"event_type": "DomainRepairCompleted", "repair_attempt_id": attempt_id, "domain_id": did, "repair_mode": mode, "repair_stage": "COMPLETED", "domain_status": status, "domain_certification_hash": after_build.get("content_hash", ""), "domain_certification_path": paths.get("json", "")})
            results.append({"domain_id": did, "repair_mode": mode, "status": "COMPLETED", "event_id": event.get("event_id"), "domain_status": status, "paths": paths})
        else:
            failure_reason = "COMMAND_FAILED" if failed else f"DOMAIN_STATUS_{status}"
            event = _append_event(truth_root=root, day_utc=day_utc, event={"event_type": "DomainRepairFailed", "repair_attempt_id": attempt_id, "domain_id": did, "repair_mode": mode, "repair_stage": "FAILED", "domain_status": status, "status_reason": failure_reason, "command_results": command_results, "domain_certification_path": paths.get("json", "")})
            results.append({"domain_id": did, "repair_mode": mode, "status": "FAILED", "event_id": event.get("event_id"), "domain_status": status, "failure_reason": failure_reason, "paths": paths})
    lifecycle = read_domain_repair_lifecycle_v1(truth_root=root, day_utc=day_utc)
    lifecycle_paths = write_domain_repair_lifecycle_v1(truth_root=root, day_utc=day_utc, payload=lifecycle)
    after = build_domain_certification_report_v1(truth_root=root, day_utc=day_utc)
    return {
        "schema_id": "aegis_domain_repair_orchestration",
        "schema_version": "v1",
        "day_utc": day_utc,
        "domain_id": domain_id or "ALL",
        "execute": execute,
        "results": results,
        "before_summary": before.get("summary") or {},
        "after_summary": after.get("summary") or {},
        "lifecycle": lifecycle,
        "lifecycle_paths": lifecycle_paths,
        "domain_certification_hash": after.get("content_hash"),
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }
