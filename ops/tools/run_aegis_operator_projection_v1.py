#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_runtime_mode_v1 import runtime_mode_from_truth_root_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.run_aegis_control_plane_v1 import control_plane_path

SCHEMA_VERSION = "aegis_operator_projection.v1"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def operator_projection_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "aegis_operator_projection_v1" / day_utc / "operator_projection.v1.json").resolve()


def _projection_from_control_plane(ctx: bod.BodContext, control: dict[str, Any]) -> dict[str, Any]:
    final_status = str(control.get("final_status") or "UNKNOWN").strip().upper()
    blocker = str(control.get("canonical_blocker") or "").strip()
    phase = str(control.get("current_phase") or "").strip()
    current_domain = str(control.get("current_domain") or phase).strip()
    recovery_commands = [str(item) for item in (control.get("recovery_commands") if isinstance(control.get("recovery_commands"), list) else []) if str(item or "").strip()]
    evidence_paths = [str(item) for item in (control.get("evidence_paths") if isinstance(control.get("evidence_paths"), list) else []) if str(item or "").strip()]
    deferred = [str(item) for item in (control.get("deferred_phases") if isinstance(control.get("deferred_phases"), list) else []) if str(item or "").strip()]
    deferred_domains = [str(item) for item in (control.get("deferred_domains") if isinstance(control.get("deferred_domains"), list) else []) if str(item or "").strip()]
    action = str(control.get("recovery_action") or "").strip()
    current_session_sub = control.get("current_session_sub_blocker") if isinstance(control.get("current_session_sub_blocker"), dict) else {}
    session_inventory = list(control.get("session_dependency_inventory") if isinstance(control.get("session_dependency_inventory"), list) else [])
    session_failures = list(control.get("session_precheck_failures") if isinstance(control.get("session_precheck_failures"), list) else [])
    failed_current_domain = list(control.get("failed_current_domain_dependencies") if isinstance(control.get("failed_current_domain_dependencies"), list) else [])
    if current_session_sub:
        evidence_paths = [str(current_session_sub.get("evidence_path") or "")] if current_session_sub.get("evidence_path") else evidence_paths
        command = str(current_session_sub.get("recovery_command") or current_session_sub.get("producer_command") or "").strip()
        recovery_commands = [command] if command else recovery_commands
        action = str(current_session_sub.get("recovery_action") or action).strip()
    if blocker == "SESSION_AUTHORITY_PRECHECK_FAILED" and session_failures:
        evidence_paths = list(
            dict.fromkeys(
                str(row.get("evidence_path") or row.get("expected_path") or "")
                for row in session_failures
                if isinstance(row, dict) and str(row.get("evidence_path") or row.get("expected_path") or "").strip()
            )
        )
        recovery_commands = list(
            dict.fromkeys(
                str(row.get("recovery_command") or row.get("producer_command") or "")
                for row in session_failures
                if isinstance(row, dict) and str(row.get("recovery_command") or row.get("producer_command") or "").strip()
            )
        )
        action = "Resolve all listed SESSION_AUTHORITY precheck failures, then rerun session authority."
    if failed_current_domain:
        evidence_paths = list(
            dict.fromkeys(
                str(row.get("evidence_path") or row.get("expected_path") or "")
                for row in failed_current_domain
                if isinstance(row, dict) and str(row.get("evidence_path") or row.get("expected_path") or "").strip()
            )
        )
        recovery_commands = list(
            dict.fromkeys(
                str(row.get("recovery_command") or row.get("producer_command") or "")
                for row in failed_current_domain
                if isinstance(row, dict) and str(row.get("recovery_command") or row.get("producer_command") or "").strip()
            )
        )
        if blocker.endswith("_PRECHECK_FAILED"):
            action = f"Resolve all listed {current_domain} precheck failures, then rerun the control plane."
    next_valid_actions = recovery_commands[:1] if recovery_commands else ([action] if action else [])
    if blocker.endswith("_PRECHECK_FAILED") and recovery_commands:
        next_valid_actions = recovery_commands
    return {
        "schema_id": "aegis_operator_projection",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "runtime_mode": str(control.get("runtime_mode") or runtime_mode_from_truth_root_v1(ctx.truth_root)),
        "generated_at_utc": _now_iso(),
        "status": "PASS" if final_status == "READY" else "BLOCKED",
        "canonical_blocker": blocker,
        "operator_next_action": action or "No current blocker.",
        "final_status": final_status,
        "first_blocker": blocker,
        "owner": str(control.get("blocker_owner") or phase),
        "phase": phase,
        "current_phase": phase,
        "current_domain": current_domain,
        "root_cause": str(control.get("blocker_reason") or blocker or "No current blocker."),
        "current_session_sub_blocker": current_session_sub,
        "session_sub_blockers": list(control.get("session_sub_blockers") if isinstance(control.get("session_sub_blockers"), list) else []),
        "session_dependency_inventory": session_inventory,
        "session_precheck_failures": session_failures,
        "failed_current_domain_dependencies": failed_current_domain,
        "readiness_dependency_inventory": list(control.get("readiness_dependency_inventory") if isinstance(control.get("readiness_dependency_inventory"), list) else []),
        "downstream_consequences": [{"phase": item, "reason": f"Deferred by {phase}"} for item in deferred],
        "deferred_downstream_phases": deferred,
        "deferred_phases": deferred,
        "deferred_downstream_domains": deferred_domains,
        "deferred_domains": deferred_domains,
        "submit_allowed": bool(control.get("submit_allowed") is True),
        "artifact_paths": evidence_paths,
        "evidence_paths": evidence_paths,
        "next_valid_actions": next_valid_actions,
        "forbidden_actions": [],
        "unsafe_actions": ["Do not act on downstream blockers until the current control-plane phase clears."],
        "lineage_status": "SUPPORTING",
        "consistency_status": "SUPPORTING",
        "freshness_status": "SUPPORTING",
        "action_validity_status": "SUPPORTING",
        "truth_confidence": "CONTROL_PLANE_ORDERED",
        "trade_health_status": "ADVISORY_ONLY",
        "trade_health": {},
        "learning_loop_status": {"automatic_deployment_allowed": False},
        "pending_human_reviews": 0,
        "blocked_promotions": [],
        "rollback_recommendations": [],
        "human_review_required": bool(final_status != "READY"),
        "integrity_context": {
            "aegis_control_plane_path": str(control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)),
            "final_status_source": "aegis_control_plane_v1",
            "readiness_source": "aegis_control_plane_v1",
            "control_plane_role": "sole readiness authority for projection",
        },
        "confidence_in_diagnosis": "HIGH" if blocker else "MEDIUM",
        "last_updated_at_utc": _now_iso(),
        "authority_note": "Operator projection is render-only and presents aegis_control_plane_v1 readiness without recomputing or falling back to legacy surfaces.",
    }


def _projection_control_plane_unavailable(ctx: bod.BodContext, *, control_path: Path, reason: str) -> dict[str, Any]:
    return {
        "schema_id": "aegis_operator_projection",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "runtime_mode": runtime_mode_from_truth_root_v1(ctx.truth_root),
        "generated_at_utc": _now_iso(),
        "status": "BLOCKED",
        "canonical_blocker": "CONTROL_PLANE_UNAVAILABLE",
        "operator_next_action": f"Regenerate aegis_control_plane_v1 for {ctx.day_utc}; projection will not recompute readiness.",
        "final_status": "UNKNOWN",
        "first_blocker": "CONTROL_PLANE_UNAVAILABLE",
        "owner": "aegis_control_plane_v1",
        "phase": "",
        "current_phase": "",
        "current_domain": "",
        "root_cause": reason,
        "current_session_sub_blocker": {},
        "session_sub_blockers": [],
        "session_dependency_inventory": [],
        "session_precheck_failures": [],
        "failed_current_domain_dependencies": [],
        "readiness_dependency_inventory": [],
        "downstream_consequences": [],
        "deferred_downstream_phases": [],
        "deferred_phases": [],
        "deferred_downstream_domains": [],
        "deferred_domains": [],
        "submit_allowed": False,
        "artifact_paths": [str(control_path)],
        "evidence_paths": [str(control_path)],
        "next_valid_actions": [f'PYTHONPATH="$PWD" python3 ops/tools/run_aegis_control_plane_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment} --truth_root {ctx.truth_root}'],
        "forbidden_actions": [],
        "unsafe_actions": ["Do not use kernel, requirement graph, day-run, or packet artifacts to infer projection readiness while control plane is unavailable."],
        "lineage_status": "UNKNOWN",
        "consistency_status": "UNKNOWN",
        "freshness_status": "UNKNOWN",
        "action_validity_status": "UNKNOWN",
        "truth_confidence": "CONTROL_PLANE_UNAVAILABLE",
        "trade_health_status": "ADVISORY_ONLY",
        "trade_health": {},
        "learning_loop_status": {"automatic_deployment_allowed": False},
        "pending_human_reviews": 0,
        "blocked_promotions": [],
        "rollback_recommendations": [],
        "human_review_required": True,
        "integrity_context": {
            "aegis_control_plane_path": str(control_path),
            "final_status_source": "aegis_control_plane_v1",
            "control_plane_role": "sole readiness authority for projection",
        },
        "confidence_in_diagnosis": "LOW",
        "last_updated_at_utc": _now_iso(),
        "authority_note": "Operator projection is render-only and refuses to compute readiness without a current aegis_control_plane_v1 artifact.",
    }


def run_operator_projection_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    cp_path = control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    control = _read_json(cp_path)
    payload = _projection_from_control_plane(ctx, control) if str(control.get("day_utc") or "") == ctx.day_utc else _projection_control_plane_unavailable(
        ctx,
        control_path=cp_path,
        reason=f"control_plane_day={str(control.get('day_utc') or 'MISSING')} target_day={ctx.day_utc}",
    )
    path = operator_projection_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_aegis_operator_projection_v1.py",
        producer_command=f"python3 ops/tools/run_aegis_operator_projection_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        input_artifacts=[cp_path],
        output_artifacts=[path],
        schema_versions={"aegis_operator_projection": SCHEMA_VERSION},
    )
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_operator_projection_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_operator_projection_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "operator_projection_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
