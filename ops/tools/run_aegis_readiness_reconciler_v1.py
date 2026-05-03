#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.aegis_artifact_ledger_v1 import artifact_hash_v1, verify_promotion_attestation_v1, write_artifact_ledger_record_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.aegis_runtime_mode_v1 import CANDIDATE_TRUTH_ROOT, PRODUCTION_TRUTH_ROOT
from ops.tools.aegis_submit_enforcement_v1 import packet_currentness_v1
from ops.tools.run_aegis_control_plane_v1 import (
    DOMAIN_REGISTRY_PATH,
    control_plane_acceptance_issues_v1,
    control_plane_path,
    load_readiness_domain_registry_v1,
)

DESIRED_SCHEMA = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_desired_state.v1.schema.json"
PLAN_SCHEMA = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_readiness_reconciliation_plan.v1.schema.json"
REPORT_SCHEMA = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_readiness_reconciliation_report.v1.schema.json"
PLAN_SCHEMA_VERSION = "aegis_readiness_reconciliation_plan.v1"
REPORT_SCHEMA_VERSION = "aegis_readiness_reconciliation_report.v1"
DESIRED_SCHEMA_VERSION = "aegis_desired_state.v1"
SAFETY_POLICY_VERSION = "aegis_readiness_reconciliation_safety.v1"
PROMOTION_POLICY_VERSION = "aegis_candidate_to_production_promotion.v1"
PROMOTION_TOOLS = [
    "python3 ops/tools/run_aegis_promotion_candidate_v1.py --day_utc {day_utc} --candidate_root {candidate_truth_root} --production_root {production_truth_root}",
    "python3 ops/tools/run_aegis_promotion_validation_ledger_v1.py --day_utc {day_utc} --truth_root {candidate_truth_root} --runtime_root {runtime_root} --focused_tests_passed",
    "python3 ops/tools/run_aegis_production_promotion_gate_v1.py --day_utc {day_utc} --promotion_id {promotion_id} --candidate_root {candidate_truth_root} --production_root {production_truth_root}",
    "python3 ops/tools/promote_aegis_candidate_to_production_v1.py --day_utc {day_utc} --promotion_id {promotion_id} --candidate_root {candidate_truth_root} --production_root {production_truth_root} --promoted_by readiness_reconciler_v1",
]
FORBIDDEN_COMMAND_MARKERS = (
    "run_aegis_paper_submit_v1.py",
    "submit_order",
    "place_order",
    "execution_package_v1",
    "fills",
    "trade_outcome",
    "allocation_eligibility",
)


def now_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_bytes_v1(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def canonical_hash_v1(payload: Any) -> str:
    return hashlib.sha256(canonical_bytes_v1(payload)).hexdigest()


def read_json_v1(path: Path) -> dict[str, Any]:
    if not Path(path).exists() or not Path(path).is_file():
        return {}
    try:
        obj = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def write_json_v1(path: Path, payload: dict[str, Any]) -> None:
    resolved = Path(path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def validate_schema_v1(payload: dict[str, Any], schema_path: Path) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError("READINESS_RECONCILER_SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3]))


def git_commit_v1() -> str:
    proc = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    return str(proc.stdout or "").strip() if proc.returncode == 0 else ""


def git_dirty_status_v1() -> str:
    proc = subprocess.run(["git", "status", "--short"], cwd=str(REPO_ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    return "DIRTY" if str(proc.stdout or "").strip() else "CLEAN"


def runtime_root_for_truth_v1(truth_root: Path) -> Path:
    resolved = Path(truth_root).expanduser().resolve()
    if resolved.name in {"candidate_truth", "production_truth"}:
        return resolved.parent.resolve()
    return resolved.parent.resolve()


def plan_path_v1(*, truth_root: Path, target_day: str, plan_id: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "aegis_readiness_reconciliation_plan_v1" / target_day / f"{plan_id}.json"


def report_path_v1(*, truth_root: Path, target_day: str, plan_id: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "aegis_readiness_reconciliation_report_v1" / target_day / f"{plan_id}.json"


def _format_template(text: str, ctx: dict[str, str]) -> str:
    return text.format(**ctx)


def _path_is_under_v1(path: Path, root: Path) -> bool:
    resolved = Path(path).expanduser().resolve()
    parent = Path(root).expanduser().resolve()
    return resolved == parent or parent in resolved.parents


def _registry_dependencies_v1() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for domain in load_readiness_domain_registry_v1():
        for dep in domain.get("dependencies") or []:
            if isinstance(dep, dict):
                rows.append(dep)
    return rows


def _rendered_registry_dependencies_v1(ctx: dict[str, str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for dep in _registry_dependencies_v1():
        dep_id = str(dep.get("dependency_id") or "").strip()
        if not dep_id:
            continue
        rendered = dict(dep)
        for key in ("expected_path", "artifact_path", "producer_command", "governed_producer", "recovery_command"):
            if str(rendered.get(key) or "").strip():
                rendered[key] = _format_template(str(rendered[key]), ctx)
        out[dep_id] = rendered
    return out


def _promotion_commands_v1(ctx: dict[str, str]) -> list[str]:
    promotion_id = f"aegis-readiness-{ctx['day_utc']}-{git_commit_v1()[:12]}"
    promo_ctx = {**ctx, "promotion_id": promotion_id}
    return [_format_template(command, promo_ctx) for command in PROMOTION_TOOLS]


def build_desired_state_v1(
    *,
    target_day: str,
    truth_root: Path,
    runtime_mode: str,
    account: str,
    ib_account: str,
    sleeve_universe: list[str] | None = None,
) -> dict[str, Any]:
    truth = Path(truth_root).expanduser().resolve()
    runtime = runtime_root_for_truth_v1(truth)
    ctx = {
        "day_utc": target_day,
        "target_day": target_day,
        "truth_root": str(truth),
        "runtime_root": str(runtime),
        "candidate_truth_root": str((runtime / "candidate_truth").resolve()),
        "production_truth_root": str((runtime / "production_truth").resolve()),
        "execution_root": str((runtime / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()),
        "operator_input_root": str(runtime),
        "environment": "PAPER",
        "ib_account": ib_account,
    }
    registry = _rendered_registry_dependencies_v1(ctx)
    commands = [str(row.get("recovery_command") or "").strip() for row in registry.values() if str(row.get("recovery_command") or "").strip()]
    commands.extend(_promotion_commands_v1(ctx))
    producers = [str(row.get("governed_producer") or "").strip() for row in registry.values() if str(row.get("governed_producer") or "").strip()]
    producers.extend(_promotion_commands_v1(ctx))
    payload = {
        "schema_id": "aegis_desired_state",
        "schema_version": DESIRED_SCHEMA_VERSION,
        "target_day": target_day,
        "account": account,
        "ib_account": ib_account,
        "sleeve_universe": sleeve_universe or ["PRIMARY"],
        "runtime_root": str(runtime),
        "candidate_truth_root": str((runtime / "candidate_truth").resolve()),
        "production_truth_root": str((runtime / "production_truth").resolve()),
        "runtime_mode": str(runtime_mode).strip().upper(),
        "desired_production_commit": git_commit_v1(),
        "safety_policy_version": SAFETY_POLICY_VERSION,
        "promotion_policy_version": PROMOTION_POLICY_VERSION,
        "allowed_producers": sorted(set(producers)),
        "allowed_recovery_commands": sorted(set(commands)),
        "submit_allowed": False,
        "no_submit": True,
    }
    validate_schema_v1(payload, DESIRED_SCHEMA)
    return payload


def _inventory_rows_v1(control_plane: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("failed_current_domain_dependencies", "session_dependency_inventory", "readiness_dependency_inventory"):
        value = control_plane.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        dep_id = str(row.get("dependency_id") or row.get("artifact") or "").strip()
        if not dep_id:
            continue
        prior = by_id.get(dep_id, {})
        if not prior or str(row.get("status") or "").upper() not in {"SATISFIED", "PASS"}:
            by_id[dep_id] = {**prior, **row}
    return list(by_id.values())


def _classify_blocker_v1(row: dict[str, Any]) -> str:
    status = str(row.get("status") or "").strip().upper()
    code = str(row.get("blocking_reason") or row.get("blocker_code") or row.get("code") or "").strip().upper()
    detail = json.dumps(row, sort_keys=True).upper()
    if "NON_TRADING_DAY" in detail:
        return "NON_RECOVERABLE_NON_TRADING_DAY"
    if "OPTIONS_SNAPSHOT_ROOT_MISSING" in detail or "OPTIONS_CAPTURE_TIMEOUT" in detail or "OPTIONS_CHAIN" in detail:
        return "NON_RECOVERABLE_EXTERNAL_DATA_UNAVAILABLE"
    if "HIDDEN_DEPENDENCY" in detail:
        return "NON_RECOVERABLE_HIDDEN_DEPENDENCY"
    if "SCHEMA" in code and ("INVALID" in code or "MISSING" in code):
        return "NON_RECOVERABLE_SCHEMA_INVALID"
    if any(marker in detail for marker in ("IB_DISCONNECTED", "BROKER_UNAVAILABLE", "BROKER_ACCOUNT", "BROKER_EVENT", "IB_EVENT_TIMEOUT")):
        return "NON_RECOVERABLE_BROKER_UNAVAILABLE"
    if "POLICY" in detail or "DENIED" in detail:
        return "NON_RECOVERABLE_POLICY_DENIAL"
    if code == "UNPROMOTED_PRODUCTION_COMMIT":
        return "RECOVERABLE_UNPROMOTED_COMMIT"
    if "ARTIFACT_LEDGER_RECORD_MISSING" in detail:
        return "RECOVERABLE_LEDGER_RECORD_MISSING"
    if "PROMOTION_ATTESTATION" in detail and "MISSING" in detail:
        return "RECOVERABLE_PROMOTION_ATTESTATION_MISSING"
    if status == "MISSING" or code.endswith("_MISSING"):
        return "RECOVERABLE_MISSING_ARTIFACT"
    if status == "STALE" or code.startswith("STALE_") or "STALE_ARTIFACT" in code:
        return "RECOVERABLE_STALE_ARTIFACT"
    if status in {"FAIL", "FAILED", "BLOCKED", "DENIED", "NOT_READY"}:
        return "NON_RECOVERABLE_POLICY_DENIAL"
    return ""


def _command_forbidden_v1(command: str) -> str:
    lowered = command.lower()
    for marker in FORBIDDEN_COMMAND_MARKERS:
        if marker.lower() in lowered:
            return marker
    return ""


def _action_allowed_v1(*, command: str, producer: str, desired: dict[str, Any]) -> tuple[bool, str]:
    if not command:
        return False, "RECOVERY_COMMAND_MISSING"
    forbidden = _command_forbidden_v1(command)
    if forbidden:
        return False, f"FORBIDDEN_COMMAND:{forbidden}"
    if command not in set(desired.get("allowed_recovery_commands") or []):
        return False, "RECOVERY_COMMAND_NOT_ALLOWLISTED"
    if producer and producer not in set(desired.get("allowed_producers") or []):
        return False, "GOVERNED_PRODUCER_NOT_ALLOWLISTED"
    return True, "DRY_RUN_VALIDATED"


def _dependency_graph_v1(*, control_plane: dict[str, Any], registry: dict[str, dict[str, Any]], desired: dict[str, Any], truth_root: Path, runtime_mode: str) -> list[dict[str, Any]]:
    rows = _inventory_rows_v1(control_plane)
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        dep_id = str(row.get("dependency_id") or row.get("artifact") or "").strip()
        if dep_id:
            by_id[dep_id] = row
    graph: list[dict[str, Any]] = []
    for dep_id, dep in registry.items():
        path = Path(str(dep.get("artifact_path") or dep.get("expected_path") or "")).expanduser().resolve()
        observed = by_id.get(dep_id, {})
        artifact_payload = read_json_v1(path) if path.exists() and path.is_file() and path.suffix == ".json" else {}
        artifact_text = json.dumps(artifact_payload, sort_keys=True).upper() if artifact_payload else ""
        artifact_blocker_markers = [
            marker
            for marker in (
                "NON_TRADING_DAY",
                "OPTIONS_SNAPSHOT_ROOT_MISSING",
                "OPTIONS_CAPTURE_TIMEOUT",
                "HIDDEN_DEPENDENCY_DETECTED",
                "IB_DISCONNECTED",
                "BROKER_UNAVAILABLE",
            )
            if marker in artifact_text
        ]
        status = str(observed.get("status") or ("MISSING" if not path.exists() else "UNKNOWN")).strip().upper()
        blocker = str(observed.get("blocking_reason") or observed.get("blocker_code") or observed.get("code") or "").strip()
        row = {
            "dependency_name": dep_id,
            "domain_owner": str(dep.get("domain_owner") or ""),
            "owner": str(dep.get("domain_owner") or ""),
            "status": status,
            "blocker_code": blocker,
            "blocker_class": "",
            "artifact_expected": str(path),
            "artifact_actual": str(observed.get("artifact_path") or observed.get("expected_path") or path),
            "governed_producer": str(dep.get("governed_producer") or ""),
            "recovery_command": str(observed.get("recovery_command") or dep.get("recovery_command") or ""),
            "schema_path": str(dep.get("schema_path") or ""),
            "runtime_mode": runtime_mode,
            "truth_root": str(truth_root),
            "artifact_blocker_markers": artifact_blocker_markers,
        }
        row["blocker_class"] = _classify_blocker_v1({**row, **observed})
        graph.append(row)
    control_path = control_plane_path(truth_root=truth_root, day_utc=str(desired["target_day"]))
    if control_plane and control_path.exists():
        for issue in control_plane_acceptance_issues_v1(control_plane, actual_path=control_path):
            graph.append(
                {
                    "dependency_name": "aegis_control_plane_v1",
                    "domain_owner": "CONTROL_PLANE",
                    "owner": "CONTROL_PLANE",
                    "status": "FAIL",
                    "blocker_code": str(issue.get("code") or ""),
                    "blocker_class": _classify_blocker_v1(issue),
                    "artifact_expected": str(control_path),
                    "artifact_actual": str(control_path),
                    "governed_producer": f"python3 ops/tools/run_aegis_control_plane_v1.py --day_utc {desired['target_day']} --environment PAPER --truth_root {truth_root} --runtime_mode {runtime_mode}",
                    "recovery_command": f"python3 ops/tools/run_aegis_control_plane_v1.py --day_utc {desired['target_day']} --environment PAPER --truth_root {truth_root} --runtime_mode {runtime_mode}",
                    "schema_path": "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_control_plane.v1.schema.json",
                    "runtime_mode": runtime_mode,
                    "truth_root": str(truth_root),
                }
            )
    return graph


def _promotion_blockers_v1(*, desired: dict[str, Any], truth_root: Path) -> list[dict[str, Any]]:
    if str(desired.get("runtime_mode") or "").upper() != "PRODUCTION":
        return []
    version = read_json_v1(Path(truth_root) / "governance" / "production_version.v1.json")
    promoted = str(version.get("promoted_commit") or "").strip()
    current = git_commit_v1()
    if promoted == current:
        return []
    if git_dirty_status_v1() != "CLEAN":
        return [
            {
                "dependency_name": "production_commit_promotion",
                "status": "BLOCKED",
                "blocker_code": "REPO_DIRTY",
                "blocker_class": "NON_RECOVERABLE_POLICY_DENIAL",
                "artifact_expected": str((Path(truth_root) / "governance" / "production_version.v1.json").resolve()),
                "artifact_actual": str((Path(truth_root) / "governance" / "production_version.v1.json").resolve()),
                "owner": "PROMOTION_POLICY",
                "recovery_command": "",
                "governed_producer": "",
            }
        ]
    promotion_id = f"aegis-readiness-{desired['target_day']}-{current[:12]}"
    approval = Path(desired["candidate_truth_root"]) / "governance" / "promotion_approvals" / f"{promotion_id}.json"
    if not approval.exists():
        return [
            {
                "dependency_name": "production_commit_promotion",
                "status": "BLOCKED",
                "blocker_code": "HUMAN_APPROVAL_MISSING",
                "blocker_class": "NON_RECOVERABLE_MANUAL_APPROVAL_REQUIRED",
                "artifact_expected": str(approval.resolve()),
                "artifact_actual": "",
                "owner": "PROMOTION_POLICY",
                "recovery_command": "",
                "governed_producer": "",
            }
        ]
    return [
        {
            "dependency_name": "production_commit_promotion",
            "status": "STALE",
            "blocker_code": "UNPROMOTED_PRODUCTION_COMMIT",
            "blocker_class": "RECOVERABLE_UNPROMOTED_COMMIT",
            "artifact_expected": str((Path(truth_root) / "governance" / "production_version.v1.json").resolve()),
            "artifact_actual": str((Path(truth_root) / "governance" / "production_version.v1.json").resolve()),
            "owner": "PROMOTION_POLICY",
            "recovery_command": _promotion_commands_v1(
                {
                    "day_utc": str(desired["target_day"]),
                    "candidate_truth_root": str(desired["candidate_truth_root"]),
                    "production_truth_root": str(desired["production_truth_root"]),
                    "runtime_root": str(desired["runtime_root"]),
                }
            )[0],
            "governed_producer": _promotion_commands_v1(
                {
                    "day_utc": str(desired["target_day"]),
                    "candidate_truth_root": str(desired["candidate_truth_root"]),
                    "production_truth_root": str(desired["production_truth_root"]),
                    "runtime_root": str(desired["runtime_root"]),
                }
            )[0],
        }
    ]


def promotion_packet_state_v1(*, desired: dict[str, Any], truth_root: Path) -> dict[str, Any]:
    version = read_json_v1(Path(truth_root) / "governance" / "production_version.v1.json")
    promotion_ledger = read_json_v1(
        Path(truth_root)
        / "reports"
        / "aegis_promotion_validation_ledger_v1"
        / str(desired["target_day"])
        / "promotion_validation_ledger.v1.json"
    )
    packet = packet_currentness_v1(
        runtime_root=Path(desired["runtime_root"]),
        runtime_mode=str(desired.get("runtime_mode") or ""),
    )
    return {
        "current_git_commit": git_commit_v1(),
        "repo_dirty_status": git_dirty_status_v1(),
        "repo_clean": git_dirty_status_v1() == "CLEAN",
        "production_promoted_commit": str(version.get("promoted_commit") or ""),
        "production_version_status": str(version.get("status") or ""),
        "promotion_ledger_commit": str(promotion_ledger.get("candidate_commit") or ""),
        "promotion_ledger_promoted_commit": str(promotion_ledger.get("promoted_commit") or ""),
        "promotion_ledger_truth_root": str(promotion_ledger.get("truth_root") or ""),
        "promotion_ledger_runtime_root": str(promotion_ledger.get("runtime_root") or ""),
        "promotion_ledger_status": str(promotion_ledger.get("promotion_status") or ""),
        "packet_currentness": packet,
    }


def _ordered_actions_v1(graph: list[dict[str, Any]], desired: dict[str, Any], max_steps: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    ordered: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    nonrecoverable: list[dict[str, Any]] = []
    seen_commands: set[str] = set()
    for row in graph:
        blocker_class = str(row.get("blocker_class") or "")
        if not blocker_class:
            continue
        if blocker_class.startswith("NON_RECOVERABLE"):
            nonrecoverable.append(row)
            continue
        command = str(row.get("recovery_command") or "")
        producer = str(row.get("governed_producer") or "")
        allowed, reason = _action_allowed_v1(command=command, producer=producer, desired=desired)
        action = {
            "action_id": canonical_hash_v1({"dependency": row.get("dependency_name"), "command": command})[:24],
            "dependency_name": str(row.get("dependency_name") or ""),
            "blocker_code": str(row.get("blocker_code") or ""),
            "blocker_class": blocker_class,
            "governed_producer": producer,
            "recovery_command": command,
            "expected_input_refs": [],
            "expected_output_path": str(row.get("artifact_expected") or ""),
            "expected_schema": str(row.get("schema_path") or ""),
            "expected_truth_root": str(row.get("truth_root") or desired.get("production_truth_root") or ""),
            "expected_runtime_mode": str(row.get("runtime_mode") or desired.get("runtime_mode") or ""),
            "target_day": str(desired.get("target_day") or ""),
            "allowed_to_execute": allowed,
            "reason": reason,
        }
        if not allowed:
            skipped.append(action)
            continue
        if command in seen_commands:
            skipped.append({**action, "allowed_to_execute": False, "reason": "DUPLICATE_RECOVERY_COMMAND_ALREADY_PLANNED"})
            continue
        if len(ordered) >= max_steps:
            skipped.append({**action, "allowed_to_execute": False, "reason": "MAX_STEPS_EXCEEDED"})
            continue
        seen_commands.add(command)
        ordered.append(action)
    return ordered, skipped, nonrecoverable


def build_reconciliation_plan_v1(
    *,
    target_day: str,
    truth_root: Path,
    runtime_mode: str,
    account: str = "PAPER",
    ib_account: str = "DUO847203",
    max_steps: int = 20,
) -> dict[str, Any]:
    truth = Path(truth_root).expanduser().resolve()
    runtime = runtime_root_for_truth_v1(truth)
    mode = str(runtime_mode).strip().upper()
    ctx = {
        "day_utc": target_day,
        "target_day": target_day,
        "truth_root": str(truth),
        "runtime_root": str(runtime),
        "candidate_truth_root": str((runtime / "candidate_truth").resolve()),
        "production_truth_root": str((runtime / "production_truth").resolve()),
        "execution_root": str((runtime / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()),
        "operator_input_root": str(runtime),
        "environment": "PAPER",
        "ib_account": ib_account,
    }
    desired = build_desired_state_v1(target_day=target_day, truth_root=truth, runtime_mode=mode, account=account, ib_account=ib_account)
    registry = _rendered_registry_dependencies_v1(ctx)
    cp_path = control_plane_path(truth_root=truth, day_utc=target_day)
    control = read_json_v1(cp_path)
    cp_hash = artifact_hash_v1(cp_path) if cp_path.exists() and cp_path.is_file() else ""
    graph = _dependency_graph_v1(control_plane=control, registry=registry, desired=desired, truth_root=truth, runtime_mode=mode)
    graph.extend(_promotion_blockers_v1(desired=desired, truth_root=truth))
    ordered, skipped, nonrecoverable = _ordered_actions_v1(graph, desired, max_steps)
    canonical_blocker = str(control.get("canonical_blocker") or "")
    secondary_blockers = [
        {
            "dependency_name": str(row.get("dependency_name") or ""),
            "blocker_code": str(row.get("blocker_code") or ""),
            "blocker_class": str(row.get("blocker_class") or ""),
            "artifact_path": str(row.get("artifact_actual") or row.get("artifact_expected") or ""),
            "producer": str(row.get("governed_producer") or ""),
            "recovery_command": str(row.get("recovery_command") or ""),
        }
        for row in graph
        if str(row.get("blocker_code") or "").strip()
    ]
    plan_id = canonical_hash_v1({"target_day": target_day, "runtime_mode": mode, "desired_state": desired, "control_plane_hash": cp_hash, "graph": graph})[:24]
    plan = {
        "schema_id": "aegis_readiness_reconciliation_plan",
        "schema_version": PLAN_SCHEMA_VERSION,
        "plan_id": plan_id,
        "target_day": target_day,
        "day_utc": target_day,
        "runtime_mode": mode,
        "truth_root": str(truth),
        "runtime_root": str(runtime),
        "desired_state": desired,
        "desired_state_ref": "inline",
        "desired_state_hash": canonical_hash_v1(desired),
        "current_control_plane_ref": str(cp_path),
        "current_control_plane_hash": cp_hash,
        "canonical_blocker": canonical_blocker,
        "secondary_blockers": secondary_blockers,
        "promotion_packet_state": promotion_packet_state_v1(desired=desired, truth_root=truth),
        "repo_cleanliness": {
            "status": git_dirty_status_v1(),
            "repo_dirty_status": git_dirty_status_v1(),
            "clean": git_dirty_status_v1() == "CLEAN",
        },
        "dependency_graph": graph,
        "ordered_actions": ordered,
        "skipped_actions": skipped,
        "nonrecoverable_blockers": nonrecoverable,
        "safety_assertions": {
            "no_submit": True,
            "submit_allowed": False,
            "no_execution_package": True,
            "no_fills_or_outcomes": True,
            "no_allocation_evidence": True,
        },
        "no_submit": True,
        "dry_run_status": "BLOCKED" if nonrecoverable or any(not row.get("allowed_to_execute") for row in skipped if row.get("reason") != "DUPLICATE_RECOVERY_COMMAND_ALREADY_PLANNED") else "PASS",
        "generated_at_utc": now_iso_v1(),
        "producer_contract_v1": {},
    }
    attach_producer_contract_v1(
        plan,
        producer_name="ops/tools/run_aegis_readiness_reconciler_v1.py",
        producer_command=f"python3 ops/tools/run_aegis_readiness_reconciler_v1.py --plan-only --target-day {target_day} --runtime-mode {mode} --truth-root {truth}",
        input_artifacts=[DOMAIN_REGISTRY_PATH, cp_path],
        output_artifacts=[],
        schema_versions={"aegis_readiness_reconciliation_plan": PLAN_SCHEMA_VERSION, "aegis_desired_state": DESIRED_SCHEMA_VERSION},
    )
    validate_schema_v1(plan, PLAN_SCHEMA)
    return plan


def _validate_action_output_v1(action: dict[str, Any]) -> None:
    expected = Path(str(action.get("expected_output_path") or "")).expanduser().resolve()
    if not expected.exists():
        raise RuntimeError(f"RECONCILER_ACTION_OUTPUT_MISSING:{expected}")
    expected_root = str(action.get("expected_truth_root") or "").strip()
    if expected_root and not _path_is_under_v1(expected, Path(expected_root)):
        raise RuntimeError(f"RECONCILER_ACTION_OUTPUT_TRUTH_ROOT_MISMATCH:{expected}")
    if expected.is_file() and expected.suffix == ".json":
        payload = read_json_v1(expected)
        truth_root = str(payload.get("truth_root") or "").strip()
        if truth_root and expected_root and Path(truth_root).expanduser().resolve() != Path(expected_root).expanduser().resolve():
            raise RuntimeError(f"RECONCILER_ACTION_PAYLOAD_TRUTH_ROOT_MISMATCH:{expected}")
        runtime_mode = str(payload.get("runtime_mode") or "").strip().upper()
        expected_mode = str(action.get("expected_runtime_mode") or "").strip().upper()
        if runtime_mode and expected_mode and runtime_mode != expected_mode:
            raise RuntimeError(f"RECONCILER_ACTION_PAYLOAD_RUNTIME_MODE_MISMATCH:{expected}")
    if str(action.get("blocker_class") or "") == "RECOVERABLE_PROMOTION_ATTESTATION_MISSING":
        truth_root = Path(str(action.get("expected_truth_root") or "")).expanduser().resolve()
        runtime_mode = str(action.get("expected_runtime_mode") or "").strip().upper()
        if truth_root.name == "production_truth" and runtime_mode == "PRODUCTION":
            issues = verify_promotion_attestation_v1(
                truth_root=truth_root,
                day=str(action.get("target_day") or action.get("day_utc") or ""),
                artifact_path=expected,
                artifact_type=str(action.get("dependency_name") or "aegis_control_plane_v1"),
            )
            if issues:
                raise RuntimeError("RECONCILER_ACTION_PROMOTION_ATTESTATION_INVALID:" + json.dumps(issues, sort_keys=True))


def execute_reconciliation_plan_v1(*, plan: dict[str, Any], truth_root: Path, stop_on_first_nonrecoverable: bool = True) -> tuple[Path, dict[str, Any]]:
    if plan.get("no_submit") is not True:
        raise RuntimeError("RECONCILER_PLAN_SUBMIT_NOT_DISABLED")
    if stop_on_first_nonrecoverable and plan.get("nonrecoverable_blockers"):
        executed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
    else:
        executed = []
        failed = []
        for action in plan.get("ordered_actions") if isinstance(plan.get("ordered_actions"), list) else []:
            if action.get("allowed_to_execute") is not True:
                failed.append({**action, "result": "REFUSED", "reason": "ACTION_NOT_ALLOWED"})
                break
            forbidden = _command_forbidden_v1(str(action.get("recovery_command") or ""))
            if forbidden:
                failed.append({**action, "result": "REFUSED", "reason": f"FORBIDDEN_COMMAND:{forbidden}"})
                break
            proc = subprocess.run(str(action.get("recovery_command") or ""), cwd=str(REPO_ROOT), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
            result = {**action, "return_code": int(proc.returncode), "stdout_tail": str(proc.stdout or "").strip()[-1000:], "stderr_tail": str(proc.stderr or "").strip()[-1000:]}
            if proc.returncode not in {0, 2}:
                failed.append({**result, "result": "FAILED"})
                break
            try:
                _validate_action_output_v1(action)
            except Exception as exc:
                failed.append({**result, "result": "FAILED", "reason": str(exc)})
                break
            executed.append({**result, "result": "PASS"})
    target_day = str(plan["target_day"])
    cp_path = Path(str(plan.get("current_control_plane_ref") or "")).expanduser().resolve()
    final_control = read_json_v1(cp_path)
    final_hash = artifact_hash_v1(cp_path) if cp_path.exists() and cp_path.is_file() else ""
    report = {
        "schema_id": "aegis_readiness_reconciliation_report",
        "schema_version": REPORT_SCHEMA_VERSION,
        "target_day": target_day,
        "day_utc": target_day,
        "runtime_mode": str(plan.get("runtime_mode") or ""),
        "truth_root": str(Path(truth_root).expanduser().resolve()),
        "runtime_root": str(runtime_root_for_truth_v1(Path(truth_root))),
        "plan_ref": "",
        "plan_hash": canonical_hash_v1(plan),
        "executed_actions": executed,
        "failed_actions": failed,
        "regenerated_artifacts": [row.get("expected_output_path") for row in executed if row.get("expected_output_path")],
        "ledger_records_created": [],
        "promotion_attestations_created": [],
        "final_control_plane_ref": str(cp_path),
        "final_control_plane_hash": final_hash,
        "final_status": str(final_control.get("final_status") or ("UNKNOWN" if cp_path.exists() else "MISSING")),
        "final_canonical_blocker": str(final_control.get("canonical_blocker") or ""),
        "nonrecoverable_blockers": plan.get("nonrecoverable_blockers") or [],
        "safety_confirmations": {
            "execution_package_v1_produced": False,
            "submit_run": False,
            "fills_or_outcomes_created": False,
            "allocation_eligibility_changed": False,
        },
        "generated_at_utc": now_iso_v1(),
        "producer_contract_v1": {},
    }
    plan_out = plan_path_v1(truth_root=truth_root, target_day=target_day, plan_id=str(plan["plan_id"]))
    attach_producer_contract_v1(
        plan,
        producer_name="ops/tools/run_aegis_readiness_reconciler_v1.py",
        producer_command="python3 ops/tools/run_aegis_readiness_reconciler_v1.py --execute",
        input_artifacts=[Path(str(plan.get("current_control_plane_ref") or ""))],
        output_artifacts=[plan_out],
        schema_versions={"aegis_readiness_reconciliation_plan": PLAN_SCHEMA_VERSION},
    )
    validate_schema_v1(plan, PLAN_SCHEMA)
    write_json_v1(plan_out, plan)
    runtime = runtime_root_for_truth_v1(Path(truth_root))
    plan_ledger_path, plan_record = write_artifact_ledger_record_v1(
        artifact_path=plan_out,
        artifact_type="aegis_readiness_reconciliation_plan_v1",
        truth_root=Path(truth_root),
        runtime_root=runtime,
        runtime_mode=str(plan.get("runtime_mode") or ""),
        day=target_day,
        recovery_command="",
    )
    report["plan_ref"] = str(plan_out)
    report["ledger_records_created"].append(
        {"ledger_path": str(plan_ledger_path), "ledger_record_id": plan_record["ledger_record_id"], "artifact_path": str(plan_out)}
    )
    report_out = report_path_v1(truth_root=truth_root, target_day=target_day, plan_id=str(plan["plan_id"]))
    attach_producer_contract_v1(
        report,
        producer_name="ops/tools/run_aegis_readiness_reconciler_v1.py",
        producer_command="python3 ops/tools/run_aegis_readiness_reconciler_v1.py --execute",
        input_artifacts=[plan_out, cp_path],
        output_artifacts=[report_out],
        schema_versions={"aegis_readiness_reconciliation_report": REPORT_SCHEMA_VERSION},
    )
    validate_schema_v1(report, REPORT_SCHEMA)
    write_json_v1(report_out, report)
    write_artifact_ledger_record_v1(
        artifact_path=report_out,
        artifact_type="aegis_readiness_reconciliation_report_v1",
        truth_root=Path(truth_root),
        runtime_root=runtime,
        runtime_mode=str(plan.get("runtime_mode") or ""),
        day=target_day,
        recovery_command="",
    )
    return report_out, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan-only", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--target-day", required=True)
    parser.add_argument("--runtime-mode", choices=["CANDIDATE", "PRODUCTION"], required=True)
    parser.add_argument("--account", default="PAPER")
    parser.add_argument("--ib-account", default="DUO847203")
    parser.add_argument("--truth-root", required=True)
    parser.add_argument("--max-steps", type=int, default=20)
    parser.add_argument("--stop-on-first-nonrecoverable", action="store_true", default=True)
    args = parser.parse_args(argv)
    plan = build_reconciliation_plan_v1(
        target_day=args.target_day,
        truth_root=Path(args.truth_root),
        runtime_mode=args.runtime_mode,
        account=args.account,
        ib_account=args.ib_account,
        max_steps=max(0, int(args.max_steps)),
    )
    if not args.execute:
        print(
            json.dumps(
                {
                    "status": "PLAN_ONLY",
                    "plan_id": plan["plan_id"],
                    "dry_run_status": plan["dry_run_status"],
                    "ordered_action_count": len(plan["ordered_actions"]),
                    "nonrecoverable_blocker_count": len(plan["nonrecoverable_blockers"]),
                    "first_nonrecoverable_blocker": (plan["nonrecoverable_blockers"][0] if plan["nonrecoverable_blockers"] else {}),
                    "no_submit": True,
                },
                sort_keys=True,
            )
        )
        return 0 if plan["dry_run_status"] == "PASS" else 2
    report_path, report = execute_reconciliation_plan_v1(plan=plan, truth_root=Path(args.truth_root), stop_on_first_nonrecoverable=bool(args.stop_on_first_nonrecoverable))
    print(
        json.dumps(
            {
                "status": report["final_status"],
                "report_path": str(report_path),
                "executed_action_count": len(report["executed_actions"]),
                "failed_action_count": len(report["failed_actions"]),
                "final_canonical_blocker": report["final_canonical_blocker"],
                "nonrecoverable_blocker_count": len(report["nonrecoverable_blockers"]),
                "no_submit": True,
            },
            sort_keys=True,
        )
    )
    return 0 if not report["failed_actions"] and not report["nonrecoverable_blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
