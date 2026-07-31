from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from ops.aegis.operator_action_model_v1 import write_operator_action_model_v1
from ops.aegis.scheduled_run_dependency_manifest_v1 import (
    build_scheduled_run_dependency_manifest_v1,
    write_scheduled_run_dependency_manifest_v1,
)

REPORT_FAMILY = "aegis_scheduled_run_safe_repair_v1"
REPORT_FILENAME = "safe_repair.v1.json"

FORBIDDEN_TOKENS = {
    "broker",
    "ib",
    "live-trading",
    "live_trading",
    "autonomous",
    "force-candidate",
    "force_candidate",
    "bypass",
    "enable-trade-advice",
    "enable_manual_capture",
    "manual-capture-enable",
}


def scheduled_run_safe_repair_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_scheduled_run_safe_repair_v1(*, truth_root: Path | str, day_utc: str, dependency_ids: Iterable[str] | None = None, execute: bool = False, command_override: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    before_manifest = build_scheduled_run_dependency_manifest_v1(truth_root=root, day_utc=day_utc)
    targets = set(str(item) for item in dependency_ids or [])
    rows: list[dict[str, Any]] = []
    for dep in before_manifest.get("dependencies", []):
        if not isinstance(dep, dict):
            continue
        dep_id = str(dep.get("dependency_id") or "")
        if targets and dep_id not in targets and str(dep.get("dependency_key") or "") not in targets:
            continue
        if dep.get("dependency_status") == "READY":
            continue
        command = str(command_override or dep.get("repair_command") or "")
        rows.append(_repair_one(root, day_utc, dep, command, execute))
    after_manifest = build_scheduled_run_dependency_manifest_v1(truth_root=root, day_utc=day_utc)
    manifest_path = write_scheduled_run_dependency_manifest_v1(truth_root=root, day_utc=day_utc, payload=after_manifest)
    source_hashes = {str(manifest_path): _sha256(manifest_path)} if manifest_path.exists() else {}
    return {
        "schema_id": "aegis_scheduled_run_safe_repair",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": str(after_manifest.get("generated_at") or f"{day_utc}T00:00:00Z"),
        "execute": bool(execute),
        "repairs": rows,
        "summary": {
            "repair_count": len(rows),
            "success_count": sum(1 for row in rows if row.get("result") == "SUCCESS"),
            "failed_count": sum(1 for row in rows if row.get("result") == "FAILED"),
            "forbidden_count": sum(1 for row in rows if row.get("result") == "FORBIDDEN"),
            "skipped_count": sum(1 for row in rows if row.get("result") == "SKIPPED"),
        },
        "source_artifacts": {"dependency_manifest": str(manifest_path)},
        "source_hashes": source_hashes,
        "safety": {
            "trade_advice_allowed": False,
            "manual_trade_capture_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "live_trading_allowed": False,
            "policy_changed": False,
        },
    }


def write_scheduled_run_safe_repair_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    path = scheduled_run_safe_repair_path_v1(truth_root=truth_root, day_utc=day_utc)
    body = dict(payload or build_scheduled_run_safe_repair_v1(truth_root=truth_root, day_utc=day_utc))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _repair_one(root: Path, day: str, dep: Mapping[str, Any], command: str, execute: bool) -> dict[str, Any]:
    dep_id = str(dep.get("dependency_id") or "")
    before_status = str(dep.get("dependency_status") or "UNKNOWN")
    before_path = Path(str(dep.get("current_artifact_path") or ""))
    before_hashes = {str(before_path): _sha256(before_path)} if before_path.exists() else {}
    if not command:
        return _row(dep_id, command, before_status, before_status, "NO_REPAIR_AVAILABLE", ["NO_SAFE_REPAIR_AVAILABLE"], before_hashes)
    if _forbidden(command):
        return _row(dep_id, command, before_status, before_status, "FORBIDDEN", ["FORBIDDEN_REPAIR_COMMAND"], before_hashes)
    if not execute:
        return _row(dep_id, command, before_status, before_status, "SKIPPED", ["DRY_RUN_REPAIR_NOT_EXECUTED"], before_hashes)
    result = "SUCCESS"
    reason = ["SAFE_REPAIR_EXECUTED"]
    try:
        if dep.get("dependency_key") == "OPERATOR_ACTION_MODEL":
            write_operator_action_model_v1(truth_root=root, day_utc=day)
        elif dep.get("dependency_key") in {"SCHEDULED_RUN_REGISTRY", "DEPENDENCY_MANIFEST"}:
            write_scheduled_run_dependency_manifest_v1(truth_root=root, day_utc=day)
        else:
            result = "SKIPPED"
            reason = ["REPAIR_COMMAND_REQUIRES_EXTERNAL_RUNNER"]
    except Exception as exc:
        result = "FAILED"
        reason = [f"REPAIR_FAILED:{type(exc).__name__}"]
    after_hashes = {str(before_path): _sha256(before_path)} if before_path.exists() else {}
    after_status = "READY" if result == "SUCCESS" and before_path.exists() else before_status
    return _row(dep_id, command, before_status, after_status, result, reason, {**before_hashes, **after_hashes})


def _row(dep_id: str, command: str, before: str, after: str, result: str, reason_codes: list[str], source_hashes: Mapping[str, str]) -> dict[str, Any]:
    return {
        "repair_id": f"REPAIR:{dep_id}",
        "dependency_id": dep_id,
        "command": command,
        "before_status": before,
        "after_status": after,
        "result": result,
        "reason_codes": sorted(set(reason_codes)),
        "source_hashes": dict(source_hashes),
    }


def _forbidden(command: str) -> bool:
    lowered = command.lower()
    return any(token in lowered for token in FORBIDDEN_TOKENS)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
