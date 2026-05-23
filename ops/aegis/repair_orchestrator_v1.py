from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

from ops.aegis.decision_ledger_v1 import write_decision_ledger_v1
from ops.aegis.evidence_event_store_v1 import append_evidence_event_v1
from ops.aegis.evidence_event_store_v1 import read_evidence_events_v1
from ops.aegis.evidence_event_store_v1 import rebuild_evidence_snapshot_v1
from ops.aegis.producer_event_bridge_v1 import emit_evidence_events_for_status_rows_v1
from ops.aegis.producer_contracts_v1 import repairable_contracts_by_schema_v1, load_producer_contract_registry_v1
from ops.aegis.pure_runtime_evaluator_v1 import evaluate_runtime, runtime_policy_bundle_v1
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1, stable_json_bytes_v1
from ops.aegis.event_append_transaction_v1 import sha256_file_v1


REPAIR_CLASSES = {"AUTO_SAFE", "AUTO_DETERMINISTIC", "EXTERNAL_DATA_BOUNDED", "MANUAL_REQUIRED", "FORBIDDEN"}
REPAIR_FAMILY = "aegis_runtime_repair_v1"

DEFAULT_REPAIR_REGISTRY_V1: dict[str, dict[str, Any]] = {
    "aegis_lite_operating_status": {"repair_class": "MANUAL_REQUIRED", "command": ""},
    "aegis_lite_eod_report": {"repair_class": "MANUAL_REQUIRED", "command": ""},
    "operator_execution_queue": {"repair_class": "MANUAL_REQUIRED", "command": ""},
    "manual_trade_packet": {"repair_class": "FORBIDDEN", "command": ""},
    "manual_execution_receipt": {"repair_class": "MANUAL_REQUIRED", "command": ""},
    "event_market_snapshot": {"repair_class": "AUTO_DETERMINISTIC", "command": "ops/tools/build_event_market_snapshot_v1.py"},
    "event_monitoring_status": {"repair_class": "AUTO_DETERMINISTIC", "command": "ops/tools/run_aegis_event_monitor_v1.py"},
    "event_rules_registry": {"repair_class": "AUTO_DETERMINISTIC", "command": "ops/tools/run_aegis_event_monitor_v1.py"},
    "event_validity_gate": {"repair_class": "MANUAL_REQUIRED", "command": ""},
    "alert_transport_proof": {"repair_class": "AUTO_DETERMINISTIC", "command": "ops/tools/write_alert_transport_proof_v1.py"},
    "ai_feedback_review": {"repair_class": "AUTO_DETERMINISTIC", "command": "ops/tools/build_ai_eod_feedback_review_v1.py"},
    "research_dataset_binding": {"repair_class": "AUTO_DETERMINISTIC", "command": "ops/tools/audit_research_dataset_bindings_v1.py"},
    "research_task_queue": {"repair_class": "MANUAL_REQUIRED", "command": ""},
    "promoted_candidate_evidence": {"repair_class": "FORBIDDEN", "command": ""},
}


def repair_dir_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPAIR_FAMILY / day_utc


def plan_repairs_v1(*, evaluation: dict[str, Any], repair_registry: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    contracts = repairable_contracts_by_schema_v1(load_producer_contract_registry_v1())
    items: dict[str, dict[str, Any]] = {}
    for blocker in evaluation.get("blockers") or []:
        blocker_id = str(blocker.get("blocker_id") or "")
        if not blocker_id.startswith("EVIDENCE_"):
            continue
        schema_id = blocker_id.rsplit(":", 1)[-1]
        contract = contracts.get(schema_id) or {}
        repair_class = str(contract.get("repair_class") or "MANUAL_REQUIRED")
        if repair_class not in REPAIR_CLASSES:
            repair_class = "MANUAL_REQUIRED"
        command = _contract_command(contract=contract, truth_root=Path(str(evaluation.get("truth_root") or "")), day_utc=str(evaluation["day_utc"]))
        expected_outputs = [str(item).replace("{day_utc}", str(evaluation["day_utc"])).replace("{day}", str(evaluation["day_utc"])) for item in contract.get("outputs") or []]
        items[schema_id] = {
            "schema_id": schema_id,
            "producer_id": str(contract.get("producer_id") or ""),
            "contract_version": str(contract.get("producer_version") or ""),
            "repair_class": repair_class,
            "command": command,
            "inputs": [str(item) for item in contract.get("inputs") or []],
            "expected_outputs": expected_outputs,
            "actual_outputs": [],
            "validation_status": "PLANNED",
            "event_ids_emitted": [],
            "reason": str(blocker.get("reason") or ""),
            "blocked_capabilities": _blocked_capabilities_for_schema(evaluation, schema_id),
            "execute_allowed": repair_class in {"AUTO_DETERMINISTIC", "AUTO_SAFE"} and bool(command),
        }
    return [items[key] for key in sorted(items)]


def run_repair_orchestration_v1(
    *,
    truth_root: Path,
    repo_root: Path,
    day_utc: str,
    mode: str,
    repair_class: str,
    run_id: str,
    parent_run_id: str,
    generated_at_utc: str,
    git_sha: str,
) -> dict[str, Any]:
    if mode not in {"dry-run", "execute"}:
        raise ValueError("mode must be dry-run or execute")
    if repair_class and repair_class not in REPAIR_CLASSES:
        raise ValueError("unsupported repair_class")
    before_snapshot = rebuild_evidence_snapshot_v1(truth_root=truth_root, day_utc=day_utc)
    policy = runtime_policy_bundle_v1(run_id=run_id, parent_run_id=parent_run_id, generated_at_utc=generated_at_utc, git_sha=git_sha)
    before = evaluate_runtime(day_utc, before_snapshot, policy)
    before_for_plan = {**before, "truth_root": str(Path(truth_root).expanduser().resolve())}
    plan = plan_repairs_v1(evaluation=before_for_plan)
    attempts: list[dict[str, Any]] = []
    if mode == "execute":
        _append_repair_event(
            truth_root=truth_root,
            day_utc=day_utc,
            event_type="RepairPlanned",
            run_id=run_id,
            parent_run_id=parent_run_id,
            created_at_utc=generated_at_utc,
            git_sha=git_sha,
            schema_id="aegis_runtime_repair_plan",
            payload={"plan": plan, "repair_class_filter": repair_class},
            validation_status="PLANNED",
        )
        for item in plan:
            if repair_class and item["repair_class"] != repair_class:
                continue
            if item["repair_class"] == "FORBIDDEN":
                status = "REJECTED_FORBIDDEN"
                attempts.append({**item, "status": status})
                _append_repair_event(truth_root=truth_root, day_utc=day_utc, event_type="RepairFailed", run_id=run_id, parent_run_id=parent_run_id, created_at_utc=generated_at_utc, git_sha=git_sha, schema_id=str(item["schema_id"]), payload={**item, "status": status}, validation_status=status)
                continue
            if item["repair_class"] == "MANUAL_REQUIRED":
                status = "SKIPPED_MANUAL_REQUIRED"
                attempts.append({**item, "status": status})
                _append_repair_event(truth_root=truth_root, day_utc=day_utc, event_type="RepairFailed", run_id=run_id, parent_run_id=parent_run_id, created_at_utc=generated_at_utc, git_sha=git_sha, schema_id=str(item["schema_id"]), payload={**item, "status": status}, validation_status=status)
                continue
            if not item["execute_allowed"]:
                status = "SKIPPED_NO_APPROVED_COMMAND"
                attempts.append({**item, "status": status})
                _append_repair_event(truth_root=truth_root, day_utc=day_utc, event_type="RepairFailed", run_id=run_id, parent_run_id=parent_run_id, created_at_utc=generated_at_utc, git_sha=git_sha, schema_id=str(item["schema_id"]), payload={**item, "status": status}, validation_status=status)
                continue
            _append_repair_event(truth_root=truth_root, day_utc=day_utc, event_type="RepairAttempted", run_id=run_id, parent_run_id=parent_run_id, created_at_utc=generated_at_utc, git_sha=git_sha, schema_id=str(item["schema_id"]), payload=item, validation_status="ATTEMPTED")
            before_event_ids = {str(row.get("event_id") or "") for row in read_evidence_events_v1(truth_root=truth_root, day_utc=day_utc)}
            attempt = _run_repair_command(repo_root=repo_root, command=str(item["command"]))
            after_events = read_evidence_events_v1(truth_root=truth_root, day_utc=day_utc)
            emitted = [row for row in after_events if str(row.get("event_id") or "") not in before_event_ids and str(row.get("schema_id") or "") == str(item["schema_id"]) and str(row.get("producer") or "") == str(item.get("producer_id") or "")]
            evidence_emitted = [row for row in emitted if str(row.get("event_type") or "") in {"EvidenceProduced", "EvidenceValidated", "EvidenceRejected"}]
            actual_outputs = sorted({path for row in emitted for path in row.get("artifact_paths", []) if path})
            actual_output_hashes = {path: sha256_file_v1(Path(path)) for path in actual_outputs if Path(path).exists() and Path(path).is_file()}
            command_success = attempt["exit_code"] == 0
            event_emission_success = bool(evidence_emitted)
            evidence_validation_status = _evidence_validation_status(emitted)
            after_attempt_snapshot = rebuild_evidence_snapshot_v1(truth_root=truth_root, day_utc=day_utc)
            after_attempt_policy = runtime_policy_bundle_v1(run_id=run_id + ":attempt:" + str(item["schema_id"]), parent_run_id=run_id, generated_at_utc=generated_at_utc, git_sha=git_sha)
            after_attempt = evaluate_runtime(day_utc, after_attempt_snapshot, after_attempt_policy)
            blocker_state_before = _blocker_state_for(before, str(item["schema_id"]))
            blocker_state_after = _blocker_state_for(after_attempt, str(item["schema_id"]))
            runtime_status_after = str(after_attempt.get("highest_readiness_layer") or "BLOCKED")
            repair_event_type, validation_status = _repair_outcome_v1(
                command_success=command_success,
                event_emission_success=event_emission_success,
                evidence_validation_status=evidence_validation_status,
                blocker_state_after=blocker_state_after,
            )
            enriched_attempt = {
                **item,
                **attempt,
                "command_success": command_success,
                "command_exit_status": "EXIT_0" if command_success else f"EXIT_{attempt["exit_code"]}",
                "event_emission_success": event_emission_success,
                "event_emission_status": "EMITTED" if event_emission_success else "NO_EVENT",
                "evidence_validation_status": evidence_validation_status,
                "actual_outputs": actual_outputs,
                "actual_output_hashes": actual_output_hashes,
                "emitted_event_ids": [str(row.get("event_id") or "") for row in emitted],
                "validation_status": validation_status,
                "blocker_state_before": blocker_state_before,
                "blocker_state_after": blocker_state_after,
                "runtime_status_after": runtime_status_after,
                "status": repair_event_type,
            }
            attempts.append(enriched_attempt)
            _append_repair_event(truth_root=truth_root, day_utc=day_utc, event_type=repair_event_type, run_id=run_id, parent_run_id=parent_run_id, created_at_utc=generated_at_utc, git_sha=git_sha, schema_id=str(item["schema_id"]), payload=enriched_attempt, validation_status=validation_status)
            if repair_event_type == "RepairFailed":
                break
    after_snapshot = rebuild_evidence_snapshot_v1(truth_root=truth_root, day_utc=day_utc)
    after_policy = runtime_policy_bundle_v1(run_id=run_id + ":after", parent_run_id=run_id, generated_at_utc=generated_at_utc, git_sha=git_sha)
    after = evaluate_runtime(day_utc, after_snapshot, after_policy)
    paths = write_repair_outputs_v1(truth_root=truth_root, day_utc=day_utc, before=before, after=after, plan=plan, attempts=attempts, mode=mode)
    write_decision_ledger_v1(truth_root=truth_root, evaluation=before)
    return {
        "schema_id": "aegis_runtime_repair_result",
        "schema_version": "v1",
        "day_utc": day_utc,
        "mode": mode,
        "repair_class": repair_class,
        "plan": plan,
        "attempts": attempts,
        "before_runtime_evaluation": before,
        "after_runtime_evaluation": after,
        "paths": paths,
        "dry_run_writes_evidence": False,
        "trade_advice_allowed": False,
        "autonomous_execution_allowed": False,
    }


def write_repair_outputs_v1(*, truth_root: Path, day_utc: str, before: dict[str, Any], after: dict[str, Any], plan: list[dict[str, Any]], attempts: list[dict[str, Any]], mode: str) -> dict[str, str]:
    out = repair_dir_v1(truth_root=truth_root, day_utc=day_utc)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "repair_plan_json": out / "repair_plan.v1.json",
        "repair_plan_txt": out / "repair_plan.v1.txt",
        "repair_attempts": out / "repair_attempts.jsonl",
        "before_runtime_evaluation": out / "before_runtime_evaluation.v1.json",
        "after_runtime_evaluation": out / "after_runtime_evaluation.v1.json",
        "repair_diff": out / "repair_diff.v1.txt",
    }
    plan_payload = {"schema_id": "aegis_runtime_repair_plan", "schema_version": "v1", "day_utc": day_utc, "mode": mode, "repairs": plan}
    paths["repair_plan_json"].write_bytes(stable_json_bytes_v1(plan_payload) + b"\n")
    paths["repair_plan_txt"].write_text(render_repair_plan_text_v1(plan_payload), encoding="utf-8")
    paths["repair_attempts"].write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in attempts), encoding="utf-8")
    paths["before_runtime_evaluation"].write_bytes(stable_json_bytes_v1(before) + b"\n")
    paths["after_runtime_evaluation"].write_bytes(stable_json_bytes_v1(after) + b"\n")
    paths["repair_diff"].write_text(render_repair_diff_v1(before=before, after=after), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def render_repair_plan_text_v1(plan_payload: dict[str, Any]) -> str:
    lines = ["AEGIS RUNTIME REPAIR PLAN v1", f"day_utc: {plan_payload.get('day_utc')}", f"mode: {plan_payload.get('mode')}", ""]
    for row in plan_payload.get("repairs") or []:
        lines.append(f"- {row.get('schema_id')}: {row.get('repair_class')} execute_allowed={str(bool(row.get('execute_allowed'))).lower()}")
        if row.get("command"):
            lines.append(f"  command: {row.get('command')}")
        lines.append(f"  reason: {row.get('reason')}")
    return "\n".join(lines).rstrip() + "\n"


def render_repair_diff_v1(*, before: dict[str, Any], after: dict[str, Any]) -> str:
    before_caps = before.get("capabilities") or {}
    after_caps = after.get("capabilities") or {}
    changed = []
    for cap in sorted(set(before_caps) | set(after_caps)):
        if bool(before_caps.get(cap, {}).get("allowed", False)) != bool(after_caps.get(cap, {}).get("allowed", False)):
            changed.append(f"{cap}: {before_caps.get(cap, {}).get('allowed')} -> {after_caps.get(cap, {}).get('allowed')}")
    return "\n".join(
        [
            "AEGIS RUNTIME REPAIR DIFF v1",
            f"before_hash: {before.get('deterministic_output_hash')}",
            f"after_hash: {after.get('deterministic_output_hash')}",
            "changed_capabilities:",
            *(f"- {row}" for row in changed or ["NONE"]),
            "",
        ]
    )


def _contract_command(*, contract: dict[str, Any], truth_root: Path, day_utc: str) -> str:
    command = str(contract.get("command") or "")
    if not command or command == "shared writer":
        return ""
    return command.replace("python3", sys.executable, 1).replace("{truth_root}", str(truth_root)).replace("{day_utc}", day_utc).replace("{day}", day_utc)



def _command_for(*, schema_id: str, repair: dict[str, Any], truth_root: Path, day_utc: str) -> str:
    command = str(repair.get("command") or "")
    if not command:
        return ""
    if command.endswith("write_alert_transport_proof_v1.py"):
        return f"{sys.executable} {command} --truth_root {truth_root} --day_utc {day_utc} --transport_mode GATE_ONLY"
    if command.endswith("build_ai_eod_feedback_review_v1.py"):
        return f"{sys.executable} {command} --truth_root {truth_root} --day {day_utc}"
    if command.endswith("audit_research_dataset_bindings_v1.py") or command.endswith("run_aegis_event_monitor_v1.py") or command.endswith("build_event_market_snapshot_v1.py"):
        return f"{sys.executable} {command} --truth_root {truth_root} --day_utc {day_utc}"
    return f"{sys.executable} {command} --truth_root {truth_root} --day_utc {day_utc}"


def _append_repair_event(*, truth_root: Path, day_utc: str, event_type: str, run_id: str, parent_run_id: str, created_at_utc: str, git_sha: str, schema_id: str, payload: dict[str, Any], validation_status: str) -> dict[str, Any]:
    payload_hash = stable_hash_v1(payload)
    return append_evidence_event_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        event={
            "event_id": event_type + ":" + schema_id + ":" + payload_hash[:24],
            "event_type": event_type,
            "run_id": run_id,
            "parent_run_id": parent_run_id,
            "day_utc": day_utc,
            "created_at_utc": created_at_utc,
            "producer": "ops.aegis.repair_orchestrator_v1",
            "producer_version": "repair_orchestrator.v1",
            "git_sha": git_sha or "UNKNOWN",
            "schema_id": schema_id,
            "schema_version": "v1",
            "input_hashes": {"payload": payload_hash},
            "output_hashes": {},
            "artifact_paths": [],
            "validation_status": validation_status,
            "previous_event_hash": "",
            "event_hash": "",
        },
    )



def _run_repair_command(*, repo_root: Path, command: str) -> dict[str, Any]:
    proc = subprocess.run(shlex.split(command), cwd=str(repo_root), capture_output=True, text=True, check=False, timeout=60)
    return {
        "exit_code": proc.returncode,
        "stdout_hash": stable_hash_v1(proc.stdout),
        "stderr_tail": proc.stderr[-1000:],
    }


def _blocked_capabilities_for_schema(evaluation: dict[str, Any], schema_id: str) -> list[str]:
    out = []
    for capability, row in (evaluation.get("capabilities") or {}).items():
        required = set(str(item) for item in row.get("required_evidence") or [])
        if schema_id in required and not bool(row.get("allowed", False)):
            out.append(str(capability))
    return sorted(out)


def _blocker_state_for(evaluation: dict[str, Any], schema_id: str) -> str:
    for row in evaluation.get("blocker_state") or []:
        if str(row.get("schema_id") or "") == schema_id:
            return str(row.get("state") or "")
    return "VALIDATED"


def _evidence_validation_status(events: list[dict[str, Any]]) -> str:
    event_types = {str(row.get("event_type") or "") for row in events}
    if "EvidenceRejected" in event_types:
        return "REJECTED"
    if "EvidenceValidated" in event_types:
        return "VALIDATED"
    if "EvidenceProduced" in event_types:
        return "PRODUCED_ONLY"
    return "NO_EVENT"


def _repair_outcome_v1(
    *,
    command_success: bool,
    event_emission_success: bool,
    evidence_validation_status: str,
    blocker_state_after: str,
) -> tuple[str, str]:
    if not command_success:
        return "RepairFailed", "COMMAND_FAILED"
    if not event_emission_success:
        return "RepairFailed", "NO_EVIDENCE_EVENT_EMITTED"
    if evidence_validation_status == "REJECTED":
        return "RepairCompletedWithRejectedEvidence", "REJECTED_EVIDENCE"
    if evidence_validation_status == "VALIDATED" and blocker_state_after in {"VALIDATED", "NOT_APPLICABLE"}:
        return "RepairSucceeded", "VALID"
    return "RepairExecuted", "EXECUTED_UNRESOLVED"
