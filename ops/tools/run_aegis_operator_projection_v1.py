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
from ops.tools.run_unified_truth_kernel_v1 import unified_truth_kernel_path

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


def _report_path(ctx: bod.BodContext, family: str, filename: str) -> Path:
    return (ctx.truth_root / "reports" / family / ctx.day_utc / filename).resolve()


def _artifact_paths(ctx: bod.BodContext, blocker: str, graph: dict[str, Any]) -> list[str]:
    paths = [
        str(_report_path(ctx, "aegis_day_run_v1", "day_run.v1.json")),
        str(_report_path(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json")),
    ]
    if blocker == "SOURCE_REPRODUCIBILITY_BLOCKED":
        paths.append(str((bod.PROTECTION_STATUS_PATH if hasattr(bod, "PROTECTION_STATUS_PATH") else Path("/home/node/constellation_runtime_data/repo_protection_v1/status.json")).resolve()))
    if blocker.startswith("MARKET") or blocker.startswith("OPTIONS") or blocker in {"MARKET_DATA_BLOCKED", "MARKET_DATA_AUTHORITY_BLOCKED"}:
        paths.extend(
            [
                str(_report_path(ctx, "market_data_supply_v1", "market_data_supply.v1.json")),
                str(_report_path(ctx, "market_data_authority_v1", "market_data_authority.v1.json")),
                str(_report_path(ctx, "market_open_data_gate_v1", "market_open_data_gate.v1.json")),
            ]
        )
    if blocker == "NO_ELIGIBLE_OPTION_STRUCTURE":
        paths.extend(
            [
                str(_report_path(ctx, "authorization_supply_v1", "authorization_supply.v1.json")),
                str(_report_path(ctx, "structure_decision_supply_v1", "structure_decision_supply.v1.json")),
                str(_report_path(ctx, "option_structure_authorization_diagnostics_v1", "option_structure_authorization_diagnostics.v1.json")),
            ]
        )
    root = graph.get("root_requirement") if isinstance(graph.get("root_requirement"), dict) else {}
    if root.get("expected_path"):
        paths.append(str(root["expected_path"]))
    return list(dict.fromkeys(path for path in paths if path))


def _allowed_actions(action_validity: dict[str, Any]) -> list[dict[str, Any]]:
    rows = action_validity.get("action_rules") if isinstance(action_validity.get("action_rules"), list) else []
    return [row for row in rows if isinstance(row, dict) and row.get("status") == "ALLOWED"]


def _prioritized_allowed_action_labels(blocker: str, allowed: list[dict[str, Any]]) -> list[str]:
    if not allowed:
        return []
    if blocker == "SOURCE_REPRODUCIBILITY_BLOCKED":
        preferred = ["clean_and_protect_repo", "rerun_day", "rerun_requirement_graph", "rerun_operator_projection"]
    elif blocker.startswith("MARKET") or blocker.startswith("OPTIONS") or blocker in {"MARKET_DATA_BLOCKED", "MARKET_DATA_AUTHORITY_BLOCKED"}:
        preferred = ["inspect_market_data_artifacts", "rerun_day", "rerun_requirement_graph", "rerun_operator_projection"]
    elif blocker == "NO_ELIGIBLE_OPTION_STRUCTURE":
        preferred = ["inspect_authorization_diagnostics", "rerun_day", "rerun_operator_projection"]
    else:
        preferred = ["rerun_day", "rerun_requirement_graph", "rerun_operator_projection", "rerun_live_intelligence"]
    by_id = {str(row.get("action_id") or ""): row for row in allowed}
    ordered = [by_id[action_id] for action_id in preferred if action_id in by_id]
    ordered.extend(row for row in allowed if row not in ordered)
    labels = [str(row.get("label") or row.get("action_id") or "").strip() for row in ordered]
    return [label for label in labels if label][:3]


def _projection_for(
    ctx: bod.BodContext,
    ledger: dict[str, Any],
    graph: dict[str, Any],
    lineage: dict[str, Any] | None = None,
    consistency: dict[str, Any] | None = None,
    freshness: dict[str, Any] | None = None,
    action_validity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    final_status = str(ledger.get("final_status") or "UNKNOWN").strip().upper()
    phase = str(ledger.get("canonical_phase") or "").strip()
    blocker = str(ledger.get("canonical_blocker") or "").strip()
    owner = phase or "UNKNOWN"
    root_cause = str((ledger.get("root_cause_chain") or [{}])[0].get("blocker_detail") or blocker or "No blocker") if isinstance(ledger.get("root_cause_chain"), list) else blocker
    next_actions: list[str]
    unsafe_actions = ["Do not submit orders outside the day-run ledger final readiness authority."]
    if blocker == "SOURCE_REPRODUCIBILITY_BLOCKED":
        owner = "source_reproducibility_authority"
        root_cause = "Canonical repo is dirty or not protected, so current source cannot be reproduced."
        next_actions = ["Clean/protect the canonical repo.", "Rerun python3 ops/tools/run_aegis_day_v1.py --day_utc " + ctx.day_utc + " --environment PAPER."]
        unsafe_actions.append("Do not treat downstream readiness artifacts as final while source integrity is blocked.")
    elif blocker.startswith("MARKET") or blocker.startswith("OPTIONS") or blocker == "MARKET_DATA_BLOCKED":
        owner = "market_data_authority"
        root_cause = "Market data authority or supply artifacts are missing, stale, or blocked."
        next_actions = ["Inspect market_data_supply_v1 and market_data_authority_v1.", "Rerun governed market-data capture/supply commands for " + ctx.day_utc + "."]
    elif blocker == "NO_ELIGIBLE_OPTION_STRUCTURE":
        owner = "authorization_supply"
        root_cause = "Authorization exists but no governed option structure is currently eligible."
        next_actions = ["Inspect authorization_supply_v1 and option-structure diagnostics.", "Keep submit blocked unless governed market conditions or policy produce an eligible structure."]
    elif blocker:
        next_actions = [str(ledger.get("operator_next_action") or "Resolve the canonical ledger blocker and rerun the day.")]
    else:
        next_actions = ["No blocker reported by the day-run ledger."]
    allowed = _allowed_actions(action_validity or {})
    if allowed:
        next_actions = _prioritized_allowed_action_labels(blocker, allowed)
    invalid = [
        str(row.get("label") or row.get("action_id") or "")
        for row in ((action_validity or {}).get("action_rules") if isinstance((action_validity or {}).get("action_rules"), list) else [])
        if isinstance(row, dict) and row.get("status") in {"FORBIDDEN", "BLOCKED"}
    ]
    return {
        "schema_id": "aegis_operator_projection",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": _now_iso(),
        "status": "PASS" if final_status not in {"UNKNOWN", "NOT_READY"} else "BLOCKED",
        "canonical_blocker": blocker,
        "operator_next_action": next_actions[0],
        "final_status": final_status,
        "first_blocker": blocker,
        "owner": owner,
        "phase": phase,
        "root_cause": root_cause,
        "downstream_consequences": ledger.get("downstream_consequences") if isinstance(ledger.get("downstream_consequences"), list) else [],
        "artifact_paths": _artifact_paths(ctx, blocker, graph),
        "next_valid_actions": next_actions,
        "unsafe_actions": unsafe_actions + invalid,
        "lineage_status": str((lineage or {}).get("status") or "UNKNOWN"),
        "consistency_status": str((consistency or {}).get("status") or "UNKNOWN"),
        "freshness_status": str((freshness or {}).get("status") or "UNKNOWN"),
        "action_validity_status": str((action_validity or {}).get("status") or "UNKNOWN"),
        "integrity_context": {
            "evidence_lineage_index_path": str(_report_path(ctx, "evidence_lineage_index_v1", "evidence_lineage_index.v1.json")),
            "state_consistency_path": str(_report_path(ctx, "state_consistency_v1", "state_consistency.v1.json")),
            "truth_freshness_path": str(_report_path(ctx, "truth_freshness_v1", "truth_freshness.v1.json")),
            "action_validity_path": str(_report_path(ctx, "action_validity_v1", "action_validity.v1.json")),
        },
        "confidence_in_diagnosis": "CAPPED_BY_CONSISTENCY_FAILURE" if str((consistency or {}).get("status") or "") == "FAIL" else ("HIGH" if blocker else "MEDIUM"),
        "last_updated_at_utc": _now_iso(),
        "authority_note": "Operator projection is explanatory only; aegis_day_run_ledger_v1 remains final readiness authority.",
    }


def _labels(rows: list[Any]) -> list[str]:
    labels: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label") or row.get("action_id") or "").strip()
        if label:
            labels.append(label)
    return labels


def _kernel_artifact_paths(kernel: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for key in ("authoritative_artifacts", "diagnostic_artifacts", "advisory_artifacts", "unknown_or_untrusted_artifacts"):
        rows = kernel.get(key) if isinstance(kernel.get(key), list) else []
        for row in rows:
            if isinstance(row, dict):
                path = str(row.get("path") or "").strip()
                if path:
                    paths.append(path)
    return list(dict.fromkeys(paths))


def _blocker_context(ctx: bod.BodContext, blocker: str) -> dict[str, Any]:
    if blocker == "C2_KILL_SWITCH_ACTIVE":
        path = ctx.execution_root / "risk_v1" / "kill_switch_v1" / ctx.day_utc / "global_kill_switch_state.v1.json"
        return {
            "evidence_paths": [str(path.resolve())],
            "operator_next_action": "Set the global kill switch to INACTIVE through the governed kill-switch authority, then rerun the day-run ledger.",
            "next_valid_actions": ["Resolve governed kill switch state", "Rerun day"],
            "root_cause": "Global C2 kill switch is active; submission and readiness must remain blocked.",
        }
    if blocker == "SESSION_AUTHORITY_MISSING":
        path = ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json"
        return {
            "evidence_paths": [str(path.resolve())],
            "operator_next_action": f"Run python3 ops/tools/run_paper_session_bootstrap_v1.py --day_utc {ctx.day_utc}, then rerun the day-run ledger.",
            "next_valid_actions": ["Run paper session bootstrap", "Rerun day"],
            "root_cause": "Paper session authority artifact is missing for the current day.",
        }
    if blocker == "TARGET_DAY_DATE_MISMATCH":
        capital_seed_path = bod.resolve_paper_capital_seed_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
        operator_statement_path = bod.resolve_operator_statement_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
        pre_open_bundle_path = ctx.truth_root / "reports" / "pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json"
        return {
            "evidence_paths": [
                str(capital_seed_path.resolve()),
                str(operator_statement_path.resolve()),
                str(pre_open_bundle_path.resolve()),
            ],
            "operator_next_action": (
                "Provide or regenerate paper capital seed, operator statement, and pre-open bundle, "
                "then rerun run_aegis_day_v1.py."
            ),
            "next_valid_actions": [
                "Provide/regenerate paper capital seed",
                "Provide/regenerate operator statement",
                "Regenerate pre-open bundle",
            ],
            "root_cause": "Target-day prerequisites are missing or dated for a different day.",
        }
    return {}


def _projection_from_kernel(ctx: bod.BodContext, kernel: dict[str, Any]) -> dict[str, Any]:
    final_status = str(kernel.get("final_status") or "UNKNOWN").strip().upper()
    blocker = str(kernel.get("first_blocker") or kernel.get("canonical_blocker") or "").strip()
    allowed = kernel.get("allowed_operator_actions") if isinstance(kernel.get("allowed_operator_actions"), list) else []
    forbidden = kernel.get("forbidden_operator_actions") if isinstance(kernel.get("forbidden_operator_actions"), list) else []
    trade_health = kernel.get("trade_health") if isinstance(kernel.get("trade_health"), dict) else {}
    learning_loop = kernel.get("learning_loop") if isinstance(kernel.get("learning_loop"), dict) else {}
    root_cause = blocker or "No blocker reported by unified truth kernel."
    if kernel.get("unknown_or_untrusted_artifacts"):
        root_cause = f"{root_cause}; truth_confidence={kernel.get('truth_confidence')}"
    blocker_context = _blocker_context(ctx, blocker)
    evidence_paths = blocker_context.get("evidence_paths") or _kernel_artifact_paths(kernel)
    operator_next_action = str(blocker_context.get("operator_next_action") or kernel.get("operator_next_action") or "")
    next_valid_actions = list(blocker_context.get("next_valid_actions") or _labels(allowed))
    return {
        "schema_id": "aegis_operator_projection",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "runtime_mode": str(kernel.get("runtime_mode") or runtime_mode_from_truth_root_v1(ctx.truth_root)),
        "generated_at_utc": _now_iso(),
        "status": "PASS" if final_status not in {"UNKNOWN", "NOT_READY", "BLOCKED"} else "BLOCKED",
        "canonical_blocker": blocker,
        "operator_next_action": operator_next_action,
        "final_status": final_status,
        "first_blocker": blocker,
        "owner": str(kernel.get("first_blocker_owner") or ""),
        "phase": str(kernel.get("first_blocker_phase") or ""),
        "root_cause": str(blocker_context.get("root_cause") or root_cause),
        "downstream_consequences": kernel.get("downstream_consequences") if isinstance(kernel.get("downstream_consequences"), list) else [],
        "artifact_paths": evidence_paths,
        "evidence_paths": evidence_paths,
        "next_valid_actions": next_valid_actions,
        "forbidden_actions": _labels(forbidden),
        "unsafe_actions": list(kernel.get("unsafe_actions") if isinstance(kernel.get("unsafe_actions"), list) else []),
        "lineage_status": str(kernel.get("lineage_status") or "UNKNOWN"),
        "consistency_status": str(kernel.get("consistency_status") or "UNKNOWN"),
        "freshness_status": str(kernel.get("freshness_status") or "UNKNOWN"),
        "action_validity_status": str(kernel.get("action_validity_status") or "UNKNOWN"),
        "truth_confidence": str(kernel.get("truth_confidence") or "UNKNOWN"),
        "trade_health_status": str(kernel.get("trade_health_status") or "UNKNOWN"),
        "trade_health": {
            "selection_confidence": str(trade_health.get("selection_confidence") or "UNKNOWN"),
            "edge_status": str(trade_health.get("edge_status") or "UNKNOWN"),
            "regime_status": str(trade_health.get("regime_status") or "UNKNOWN"),
            "outcome_status": str(trade_health.get("outcome_status") or "UNKNOWN"),
            "human_review_required": bool(trade_health.get("human_review_required") is True),
            "automatic_deployment_allowed": bool(trade_health.get("automatic_deployment_allowed") is True),
        },
        "learning_loop_status": {
            "open_recommendation_count": int(learning_loop.get("open_recommendation_count") or 0),
            "pending_proposal_count": int(learning_loop.get("pending_proposal_count") or 0),
            "shadow_evaluation_status": str(learning_loop.get("shadow_evaluation_status") or "UNKNOWN"),
            "promotion_gate_status": str(learning_loop.get("promotion_gate_status") or "UNKNOWN"),
            "post_promotion_monitor_status": str(learning_loop.get("post_promotion_monitor_status") or "UNKNOWN"),
            "blocked_promotions": list(learning_loop.get("blocked_promotions") if isinstance(learning_loop.get("blocked_promotions"), list) else []),
            "rollback_recommended": bool(learning_loop.get("rollback_recommended") is True),
            "automatic_deployment_allowed": False,
        },
        "pending_human_reviews": int(learning_loop.get("open_recommendation_count") or 0) + int(learning_loop.get("pending_proposal_count") or 0),
        "blocked_promotions": list(learning_loop.get("blocked_promotions") if isinstance(learning_loop.get("blocked_promotions"), list) else []),
        "rollback_recommendations": ["Review rollback recommendation"] if learning_loop.get("rollback_recommended") is True else [],
        "human_review_required": bool(kernel.get("human_review_required") is True),
        "integrity_context": {
            "unified_truth_kernel_path": str(unified_truth_kernel_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)),
            "final_status_source": str(kernel.get("final_status_source") or ""),
        },
        "confidence_in_diagnosis": str(kernel.get("truth_confidence") or "UNKNOWN"),
        "last_updated_at_utc": _now_iso(),
        "authority_note": "Operator projection presents unified_truth_kernel_v1 only; readiness and actions are not recomputed here.",
    }


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
        "deferred_downstream_domains": deferred_domains,
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
            "final_status_source": "aegis_day_run_ledger_v1",
            "control_plane_role": "phase ownership and operator projection",
        },
        "confidence_in_diagnosis": "HIGH" if blocker else "MEDIUM",
        "last_updated_at_utc": _now_iso(),
        "authority_note": "Operator projection presents aegis_control_plane_v1 phase ownership; day-run ledger remains final readiness authority.",
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
        "deferred_downstream_domains": [],
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
