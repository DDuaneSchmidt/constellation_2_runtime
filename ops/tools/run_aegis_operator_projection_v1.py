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
from ops.tools.aegis_runtime_mode_v1 import git_commit_v1, read_production_version_v1, runtime_mode_from_truth_root_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.run_aegis_control_plane_v1 import control_plane_acceptance_issues_v1, control_plane_path
from ops.tools.run_aegis_promotion_validation_ledger_v1 import promotion_validation_ledger_path

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


def _packet_commit_from_root(runtime_root: Path) -> str:
    path = runtime_root / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
    if not path.exists() or not path.is_file():
        return ""
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[:80]:
        text = line.strip()
        if text.startswith("- git_commit:"):
            return text.split(":", 1)[1].strip()
    return ""


def _latest_promotion_gate_v1(candidate_root: Path, day_utc: str) -> tuple[Path, dict[str, Any]]:
    root = candidate_root / "reports" / "aegis_production_promotion_gate_v1" / day_utc
    candidates = sorted(root.glob("*.json")) if root.exists() and root.is_dir() else []
    if not candidates:
        return root / "MISSING.json", {}
    candidates.sort(key=lambda path: (path.stat().st_mtime, str(path)))
    path = candidates[-1]
    return path, _read_json(path)


def _promotion_visibility_v1(ctx: bod.BodContext) -> dict[str, Any]:
    candidate_root = ctx.truth_root if ctx.truth_root.name == "candidate_truth" else (ctx.truth_root.parent / "candidate_truth").resolve()
    production_ledger_path = promotion_validation_ledger_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    candidate_ledger_path = promotion_validation_ledger_path(truth_root=candidate_root, day_utc=ctx.day_utc)
    ledger_path = production_ledger_path if production_ledger_path.exists() else candidate_ledger_path
    ledger = _read_json(ledger_path)
    gate_path, gate = _latest_promotion_gate_v1(candidate_root, ctx.day_utc)
    production_version = read_production_version_v1()
    evaluated_commit = git_commit_v1()
    truth_root_text = str(ledger.get("truth_root") or "").strip()
    runtime_root_text = str(ledger.get("runtime_root") or "").strip()
    candidate_commit = str(ledger.get("candidate_commit") or evaluated_commit)
    promoted_commit = str(production_version.get("promoted_commit") or ledger.get("promoted_commit") or "")
    ledger_status = str(ledger.get("promotion_status") or "").strip()
    ledger_blockers = ledger.get("blockers") if isinstance(ledger.get("blockers"), list) else []
    production_is_current = bool(
        ledger_status == "PROMOTED"
        and not ledger_blockers
        and promoted_commit
        and promoted_commit == evaluated_commit
        and str(ledger.get("promoted_commit") or "").strip() == promoted_commit
    )
    if production_is_current:
        promotion_status = "PROMOTED"
        blockers = []
        promotion_visibility_source = "aegis_promotion_validation_ledger_v1"
    else:
        promotion_status = str(gate.get("promotion_status") or ledger.get("promotion_status") or "VALIDATION_LEDGER_MISSING").strip()
        if isinstance(gate.get("blockers"), list):
            blockers = gate["blockers"]
        elif isinstance(ledger.get("blockers"), list):
            blockers = ledger["blockers"]
        else:
            blockers = [{"code": "PROMOTION_VALIDATION_LEDGER_MISSING", "path": str(ledger_path)}]
        promotion_visibility_source = "aegis_production_promotion_gate_v1" if gate else "aegis_promotion_validation_ledger_v1"
    packet_commit = str(ledger.get("packet_commit") or _packet_commit_from_root(ctx.truth_root.parent))
    truth_consistent = bool(truth_root_text and Path(truth_root_text).expanduser().resolve() == ctx.truth_root.resolve())
    runtime_consistent = bool(runtime_root_text and Path(runtime_root_text).expanduser().resolve() == ctx.truth_root.parent.resolve())
    mismatch_flags = {
        "candidate_commit_differs_from_promoted": bool(candidate_commit and promoted_commit and candidate_commit != promoted_commit),
        "evaluated_commit_differs_from_promoted": bool(evaluated_commit and promoted_commit and evaluated_commit != promoted_commit),
        "packet_commit_differs_from_evaluated": bool(packet_commit and evaluated_commit and packet_commit != evaluated_commit),
        "truth_root_mismatch": not truth_consistent,
        "runtime_root_mismatch": not runtime_consistent,
        "promotion_gate_blocked": promotion_status not in {"APPROVED_FOR_PROMOTION", "PROMOTED", "APPROVED"},
    }
    return {
        "candidate_commit": candidate_commit,
        "promoted_commit": promoted_commit,
        "evaluated_commit": evaluated_commit,
        "packet_commit": packet_commit,
        "promotion_status": promotion_status,
        "promotion_blockers": blockers,
        "promotion_mismatch_flags": mismatch_flags,
        "promotion_validation_ledger_path": str(ledger_path),
        "promotion_gate_path": str(gate_path),
        "promotion_visibility_source": promotion_visibility_source,
        "truth_root_consistency": {
            "truth_root": str(ctx.truth_root),
            "ledger_truth_root": truth_root_text,
            "consistent": truth_consistent,
        },
        "runtime_root_consistency": {
            "runtime_root": str(ctx.truth_root.parent.resolve()),
            "ledger_runtime_root": runtime_root_text,
            "consistent": runtime_consistent,
        },
    }


def _why_not_ready_summary(
    final_status: str,
    current_domain: str,
    failed_current_domain: list[Any],
    blocker: str,
    current_session_sub_blocker: dict[str, Any] | None = None,
) -> str:
    if final_status == "READY":
        return "SYSTEM READY."
    reason = blocker or "UNKNOWN"
    sub_blocker = str((current_session_sub_blocker or {}).get("sub_blocker_code") or "").strip()
    if sub_blocker:
        reason = sub_blocker
    for row in failed_current_domain:
        if not isinstance(row, dict):
            continue
        codes = row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else []
        if "NON_TRADING_DAY" in codes:
            reason = "NON_TRADING_DAY"
            break
        if reason == sub_blocker:
            continue
        reason = str(codes[0] if codes else row.get("blocking_reason") or blocker or "UNKNOWN")
        if reason:
            break
    return f"SYSTEM NOT READY BECAUSE: {current_domain or 'UNKNOWN'} -> {reason}"


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
    promotion_visibility = _promotion_visibility_v1(ctx)
    why_not_ready = _why_not_ready_summary(final_status, current_domain, failed_current_domain, blocker, current_session_sub)
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
        "why_not_ready_summary": why_not_ready,
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
        "control_plane_integrity_issues": [],
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
        "promotion_state": promotion_visibility,
        "candidate_commit": promotion_visibility["candidate_commit"],
        "promoted_commit": promotion_visibility["promoted_commit"],
        "evaluated_commit": promotion_visibility["evaluated_commit"],
        "packet_commit": promotion_visibility["packet_commit"],
        "promotion_status": promotion_visibility["promotion_status"],
        "promotion_blockers": promotion_visibility["promotion_blockers"],
        "promotion_mismatch_flags": promotion_visibility["promotion_mismatch_flags"],
        "truth_root_consistency": promotion_visibility["truth_root_consistency"],
        "runtime_root_consistency": promotion_visibility["runtime_root_consistency"],
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
        "deferred_domain_note": "Deferred domains are not failed and are not actionable until the current domain clears.",
    }


def _projection_control_plane_unavailable(
    ctx: bod.BodContext,
    *,
    control_path: Path,
    reason: str,
    blocker_code: str = "CONTROL_PLANE_UNAVAILABLE",
    integrity_issues: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    promotion_visibility = _promotion_visibility_v1(ctx)
    return {
        "schema_id": "aegis_operator_projection",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "runtime_mode": runtime_mode_from_truth_root_v1(ctx.truth_root),
        "generated_at_utc": _now_iso(),
        "status": "BLOCKED",
        "canonical_blocker": blocker_code,
        "operator_next_action": f"Regenerate aegis_control_plane_v1 for {ctx.day_utc}; projection will not recompute readiness.",
        "why_not_ready_summary": f"SYSTEM NOT READY BECAUSE: CONTROL_PLANE -> {blocker_code}",
        "final_status": "UNKNOWN",
        "first_blocker": blocker_code,
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
        "control_plane_integrity_issues": list(integrity_issues or []),
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
        "promotion_state": promotion_visibility,
        "candidate_commit": promotion_visibility["candidate_commit"],
        "promoted_commit": promotion_visibility["promoted_commit"],
        "evaluated_commit": promotion_visibility["evaluated_commit"],
        "packet_commit": promotion_visibility["packet_commit"],
        "promotion_status": promotion_visibility["promotion_status"],
        "promotion_blockers": promotion_visibility["promotion_blockers"],
        "promotion_mismatch_flags": promotion_visibility["promotion_mismatch_flags"],
        "truth_root_consistency": promotion_visibility["truth_root_consistency"],
        "runtime_root_consistency": promotion_visibility["runtime_root_consistency"],
        "pending_human_reviews": 0,
        "blocked_promotions": [],
        "rollback_recommendations": [],
        "human_review_required": True,
        "integrity_context": {
            "aegis_control_plane_path": str(control_path),
            "final_status_source": "aegis_control_plane_v1",
            "control_plane_role": "sole readiness authority for projection",
            "control_plane_integrity_issues": list(integrity_issues or []),
        },
        "confidence_in_diagnosis": "LOW",
        "last_updated_at_utc": _now_iso(),
        "authority_note": "Operator projection is render-only and refuses to compute readiness without a current aegis_control_plane_v1 artifact.",
        "deferred_domain_note": "Deferred domains are not failed and are not actionable while control plane is unavailable.",
    }


def run_operator_projection_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    cp_path = control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    control = _read_json(cp_path)
    integrity_issues = control_plane_acceptance_issues_v1(control, actual_path=cp_path) if control else [
        {"code": "CONTROL_PLANE_UNAVAILABLE", "path": str(cp_path), "detail": "control plane artifact missing or unreadable"}
    ]
    if str(control.get("day_utc") or "") != ctx.day_utc:
        payload = _projection_control_plane_unavailable(
            ctx,
            control_path=cp_path,
            reason=f"control_plane_day={str(control.get('day_utc') or 'MISSING')} target_day={ctx.day_utc}",
            blocker_code="CONTROL_PLANE_UNAVAILABLE",
            integrity_issues=integrity_issues,
        )
    elif integrity_issues:
        payload = _projection_control_plane_unavailable(
            ctx,
            control_path=cp_path,
            reason=json.dumps(integrity_issues, sort_keys=True),
            blocker_code=str(integrity_issues[0].get("code") or "CONTROL_PLANE_SELF_BINDING_INVALID"),
            integrity_issues=integrity_issues,
        )
    else:
        payload = _projection_from_control_plane(ctx, control)
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
