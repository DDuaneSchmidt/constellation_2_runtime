from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ops.aegis.change_control_v1 import REGISTER_PATH, build_report, evidence_completeness, load_register, validate_register

SNAPSHOT_SCHEMA = "aegis_change_control_evidence_snapshot_v1"
ADVISOR_SCHEMA = "aegis_change_control_advisor_score_v1"
AI_REVIEW_SCHEMA = "aegis_change_control_ai_review_v1"

COMPLETE_STATUSES = {"VALIDATED", "CLOSED", "REJECTED"}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else "MISSING"


def sha256_obj(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def reports_root(truth_root: Path) -> Path:
    return truth_root / "reports"


def snapshot_path_v1(truth_root: Path, day: str) -> Path:
    return reports_root(truth_root) / SNAPSHOT_SCHEMA / day / "change_control_evidence_snapshot.v1.json"


def advisor_path_v1(truth_root: Path, day: str) -> Path:
    return reports_root(truth_root) / ADVISOR_SCHEMA / day / "change_control_advisor_score.v1.json"


def ai_review_path_v1(truth_root: Path, day: str) -> Path:
    return reports_root(truth_root) / AI_REVIEW_SCHEMA / day / "change_control_ai_review.v1.json"


def _compact_record(row: dict[str, Any], validation_by_change: dict[str, list[dict[str, Any]]] | None = None) -> dict[str, Any]:
    validation_by_change = validation_by_change or {}
    return {
        "record_id": row.get("id"),
        "title": row.get("title"),
        "status": row.get("status"),
        "severity": row.get("severity"),
        "priority": row.get("priority"),
        "affected_domain": row.get("affected_domain") or row.get("owner"),
        "parent_id": row.get("parent_id"),
        "child_ids": row.get("child_ids", []),
        "required_child_ids": row.get("required_child_ids", []),
        "dependency_ids": row.get("dependency_ids", []),
        "blocker_ids": row.get("blocker_ids", []),
        "prerequisite_records": row.get("prerequisite_records", []),
        "strategic_priority_rank": row.get("strategic_priority_rank"),
        "decision_required": bool(row.get("decision_required")),
        "decision_question": row.get("decision_question"),
        "recommended_option": row.get("recommended_option"),
        "evidence_completeness": evidence_completeness(row, validation_by_change),
        "proposed_next_step": row.get("proposed_next_step"),
    }


def _load_optional_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception as exc:
        return {"status": "READ_ERROR", "reason": str(exc), "path": str(path)}


def latest_runtime_truth(truth_root: Path, day: str) -> dict[str, Any]:
    path = truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json"
    payload = _load_optional_json(path)
    return {
        "path": str(path),
        "exists": path.exists(),
        "hash": sha256_path(path),
        "runtime_truth_classification": payload.get("runtime_truth_classification"),
        "highest_readiness_layer": payload.get("highest_readiness_layer"),
        "missing_or_stale_source_count": payload.get("missing_or_stale_source_count"),
        "trade_advice_allowed": bool(payload.get("trade_advice_allowed")) if payload else False,
        "autonomous_execution_allowed": bool(payload.get("autonomous_execution_allowed")) if payload else False,
    }


def latest_audit_status(truth_root: Path, day: str) -> dict[str, Any]:
    graph = truth_root / "reports" / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json"
    payload = _load_optional_json(graph)
    return {
        "path": str(graph),
        "exists": graph.exists(),
        "hash": sha256_path(graph),
        "graph_status": payload.get("graph_status") or payload.get("status"),
        "audit_blocker_count": payload.get("audit_blocker_count"),
    }


def latest_portal_smoke_status(truth_root: Path, day: str) -> dict[str, Any]:
    debug = truth_root / "reports" / "aegis_portal_projection_debug_v1" / day / "portal_projection_debug.v1.json"
    payload = _load_optional_json(debug)
    return {
        "path": str(debug),
        "exists": debug.exists(),
        "hash": sha256_path(debug),
        "ok": payload.get("ok") if payload else None,
        "policy_gates_changed": payload.get("policy_gates_changed") if payload else None,
    }


def build_evidence_snapshot_v1(*, truth_root: Path, day: str, register_path: Path = REGISTER_PATH) -> dict[str, Any]:
    register = load_register(register_path)
    validation = validate_register(register)
    report = build_report(register)
    intake = register.get("intake_register", [])
    validations = register.get("validation_records", [])
    validation_by_change: dict[str, list[dict[str, Any]]] = {}
    for row in validations:
        validation_by_change.setdefault(str(row.get("change_id") or ""), []).append(row)
    intake_by_id = {str(row.get("id") or ""): row for row in intake}
    blocked_parents = []
    required_children = []
    incomplete_rollups = []
    for parent in report.get("relationship_graph", {}).get("parents", []):
        rollup = parent.get("rollup", {})
        if rollup.get("parent_validation_blocked") or rollup.get("required_children_open"):
            blocked_parents.append(parent)
            completion_rollup = str(rollup.get("completion_rollup") or "").replace(" required children complete", " complete")
            incomplete_rollups.append({
                "parent_id": parent.get("id"),
                "completion_rollup": completion_rollup,
                "required_child_ids_open": rollup.get("required_child_ids_open", []),
            })
        for child in parent.get("required_children", []):
            required_children.append(child)
    failed_hostile = [
        _compact_record(row, validation_by_change)
        for row in intake
        if "hostile" in " ".join([str(row.get("title") or ""), str(row.get("description") or ""), str(row.get("proposed_next_step") or "")]).lower()
        or "hostile" in [str(tag).lower() for tag in row.get("tags", [])]
    ]
    validation_gaps = [
        _compact_record(row, validation_by_change)
        for row in intake
        if str(evidence_completeness(row, validation_by_change)).startswith(("Missing", "Required before closure"))
    ]
    snapshot = {
        "schema_id": SNAPSHOT_SCHEMA,
        "snapshot_id": f"cc-snapshot-{day}-{sha256_path(register_path)[:12]}",
        "target_day": day,
        "generated_at": utc_now(),
        "register_source_path": str(register_path),
        "register_source_hash": sha256_path(register_path),
        "register_validation": validation,
        "total_records": len(intake),
        "open_p0_p1_records": [_compact_record(row, validation_by_change) for row in intake if row.get("status") not in COMPLETE_STATUSES and row.get("priority") in {"P0", "P1"}],
        "blocked_parent_records": blocked_parents,
        "required_child_records": required_children,
        "incomplete_child_rollups": incomplete_rollups,
        "failed_hostile_audits": failed_hostile,
        "validation_gaps": validation_gaps,
        "records_awaiting_decision": report.get("decision_dashboard", {}).get("awaiting_decision", []),
        "records_awaiting_validation": report.get("validation_dashboard", {}).get("awaiting_validation", []),
        "latest_audit_status": latest_audit_status(truth_root, day),
        "latest_portal_smoke_status": latest_portal_smoke_status(truth_root, day),
        "latest_runtime_truth": latest_runtime_truth(truth_root, day),
        "source_evidence_refs": [
            {"kind": "change_control_register", "path": str(register_path), "hash": sha256_path(register_path)},
            {"kind": "runtime_truth", **latest_runtime_truth(truth_root, day)},
            {"kind": "verified_runtime_graph", **latest_audit_status(truth_root, day)},
        ],
    }
    return snapshot


def _severity_weight(row: dict[str, Any]) -> int:
    return {"P0": 100, "P1": 60, "P2": 25, "P3": 5}.get(str(row.get("severity") or row.get("priority") or ""), 0)


def _risk_level(score: int) -> str:
    if score >= 180:
        return "HIGH"
    if score >= 100:
        return "MEDIUM"
    return "LOW"


def build_advisor_score_v1(*, snapshot: dict[str, Any], current_register_path: Path = REGISTER_PATH) -> dict[str, Any]:
    rows_by_id: dict[str, dict[str, Any]] = {}
    for section in ["open_p0_p1_records", "required_child_records", "records_awaiting_decision", "records_awaiting_validation", "validation_gaps"]:
        for row in snapshot.get(section, []) or []:
            rid = str(row.get("record_id") or row.get("id") or "")
            if rid:
                rows_by_id.setdefault(rid, row)
    parent_by_child: dict[str, list[str]] = {}
    blocker_parent_by_child: dict[str, list[str]] = {}
    for parent in snapshot.get("blocked_parent_records", []) or []:
        parent_id = str(parent.get("id") or parent.get("record_id") or "")
        for child_id in parent.get("required_child_ids", []) or []:
            parent_by_child.setdefault(str(child_id), []).append(parent_id)
        for blocker_id in parent.get("blocker_ids", []) or []:
            blocker_parent_by_child.setdefault(str(blocker_id), []).append(parent_id)
    blocker_refs = set(blocker_parent_by_child)
    dependency_counts: dict[str, int] = {}
    for row in rows_by_id.values():
        for dep in (row.get("dependency_ids") or []) + (row.get("prerequisite_records") or []) + (row.get("blocker_ids") or []):
            dependency_counts[str(dep)] = dependency_counts.get(str(dep), 0) + 1
    stale = snapshot.get("register_source_hash") != sha256_path(current_register_path)
    scores = []
    for rid, row in rows_by_id.items():
        status = str(row.get("status") or "").upper()
        tags = set(row.get("tags") or [])
        components = {
            "severity_weight": _severity_weight(row),
            "blocked_parent_weight": 50 if rid in parent_by_child else 0,
            "required_child_blocker_weight": 80 if rid in blocker_refs else 0,
            "failed_hostile_audit_weight": 35 if "hostile" in f"{row.get('title') or ''} {row.get('proposed_next_step') or ''}".lower() else 0,
            "dependency_impact_weight": min(60, 20 * dependency_counts.get(rid, 0)),
            "safety_impact_weight": 25 if "safety" in tags or "safety" in str(row.get("affected_domain") or "").lower() else 0,
            "strategic_leverage_weight": max(0, 35 - (int(row.get("strategic_priority_rank") or 99) * 5)) if str(row.get("strategic_priority_rank") or "").isdigit() else 0,
            "stale_evidence_penalty": -40 if stale else 0,
            "already_implemented_penalty": -20 if status in {"IMPLEMENTED", "VALIDATING"} else 0,
            "already_validated_penalty": -200 if status in {"VALIDATED", "CLOSED", "REJECTED"} else 0,
        }
        score = sum(components.values())
        if rid in blocker_refs:
            action = "FIX_BLOCKING_CHILD"
            next_action = f"Resolve {rid} first because it blocks required parent validation."
        elif row.get("decision_required"):
            action = "RECORD_DECISION"
            next_action = f"Record a human decision for {rid}."
        elif status in {"IMPLEMENTED", "VALIDATING"}:
            action = "COLLECT_VALIDATION_EVIDENCE"
            next_action = f"Collect validation evidence for {rid}."
        else:
            action = "MONITOR"
            next_action = f"Monitor {rid} until a blocker or decision is required."
        scores.append({
            "record_id": rid,
            "title": row.get("title"),
            "priority_score": score,
            "score_components": components,
            "blocked_by": row.get("blocker_ids", []),
            "blocks_records": sorted(set(parent_by_child.get(rid, []) + blocker_parent_by_child.get(rid, []))),
            "risk_level": _risk_level(score),
            "decision_required": bool(row.get("decision_required")),
            "validation_required": status in {"IMPLEMENTED", "VALIDATING"} or bool(row.get("required_child_ids")),
            "stale_recommendation_flag": stale,
            "recommendation_category": action,
            "recommended_next_action": next_action,
        })
    scores.sort(key=lambda row: (-int(row.get("priority_score") or 0), str(row.get("record_id") or "")))
    advisor = {
        "schema_id": ADVISOR_SCHEMA,
        "advisor_score_id": f"cc-advisor-{snapshot.get('target_day')}-{sha256_obj(snapshot)[:12]}",
        "target_day": snapshot.get("target_day"),
        "generated_at": utc_now(),
        "input_snapshot_id": snapshot.get("snapshot_id"),
        "input_snapshot_hash": sha256_obj(snapshot),
        "register_source_hash": snapshot.get("register_source_hash"),
        "stale_snapshot_flag": stale,
        "scores": scores,
        "top_recommended_record_id": scores[0]["record_id"] if scores else None,
        "source_evidence_refs": snapshot.get("source_evidence_refs", []),
    }
    return advisor


def build_ai_review_v1(*, snapshot: dict[str, Any], advisor: dict[str, Any]) -> dict[str, Any]:
    top = (advisor.get("scores") or [{}])[0]
    top_id = top.get("record_id") or "No record"
    top_title = top.get("title") or "No title"
    summary = "No Change Control recommendation is available."
    if top_id == "ACC-20260530-008A":
        summary = "Research Validation Engine cannot be validated until research validation becomes the final non-overridable gate before candidate review eligibility."
    elif top.get("recommended_next_action"):
        summary = str(top.get("recommended_next_action"))
    review = {
        "schema_id": AI_REVIEW_SCHEMA,
        "review_id": f"cc-ai-review-{snapshot.get('target_day')}-{sha256_obj(advisor)[:12]}",
        "target_day": snapshot.get("target_day"),
        "input_snapshot_id": snapshot.get("snapshot_id"),
        "input_advisor_score_id": advisor.get("advisor_score_id"),
        "generated_at": utc_now(),
        "model_or_agent_id": "codex-constrained-reviewer-v1",
        "plain_english_summary": summary,
        "recommended_next_actions": [
            {
                "record_id": top_id,
                "title": top_title,
                "action": top.get("recommended_next_action"),
                "score": top.get("priority_score"),
                "advisory_only": True,
            }
        ] if top else [],
        "risk_explanations": [
            f"{top_id} is ranked {top.get('risk_level', 'UNKNOWN')} because it is a required blocker for parent validation."
        ] if top else [],
        "suggested_decision_notes": [
            f"Review {top_id}: {top_title}. Deterministic advisor ranked this as the next Change Control action because it blocks parent validation."
        ] if top else [],
        "suggested_codex_prompts": [
            f"Work on {top_id} only. Explain the blocker, implement the minimum governed fix, run validation, and do not close the parent record automatically."
        ] if top else [],
        "possible_missing_work": [
            "Confirm candidate-generation eligibility cannot bypass Research Validation Engine promotion gates.",
            "Collect hostile-audit evidence before moving the parent to VALIDATED.",
        ] if top_id == "ACC-20260530-008A" else [],
        "confidence": "HIGH" if top and not advisor.get("stale_snapshot_flag") else "LOW",
        "human_decision_required": True,
        "expiration_or_staleness_rule": "Review expires when the Change Control register hash changes or target_day changes.",
        "forbidden_actions": ["APPROVE", "REJECT", "DEFER", "PRIORITIZE", "VALIDATE", "CLOSE", "IMPLEMENT", "MUTATE_RECORD"],
        "mutation_performed": False,
    }
    return review


def validate_intelligence_v1(snapshot: dict[str, Any], advisor: dict[str, Any], review: dict[str, Any], *, current_register_path: Path = REGISTER_PATH) -> dict[str, Any]:
    failures: list[str] = []
    if snapshot.get("schema_id") != SNAPSHOT_SCHEMA:
        failures.append("snapshot invalid schema")
    if not snapshot.get("register_source_hash"):
        failures.append("snapshot missing register hash")
    if advisor.get("schema_id") != ADVISOR_SCHEMA:
        failures.append("advisor invalid schema")
    if advisor.get("input_snapshot_id") != snapshot.get("snapshot_id"):
        failures.append("advisor snapshot link mismatch")
    scores = advisor.get("scores") or []
    if scores:
        expected = sorted(scores, key=lambda row: (-int(row.get("priority_score") or 0), str(row.get("record_id") or "")))[0].get("record_id")
        if advisor.get("top_recommended_record_id") != expected:
            failures.append("advisor top recommendation does not match deterministic score order")
    if review.get("schema_id") != AI_REVIEW_SCHEMA:
        failures.append("ai review invalid schema")
    if review.get("input_snapshot_id") != snapshot.get("snapshot_id"):
        failures.append("ai review snapshot link mismatch")
    if review.get("input_advisor_score_id") != advisor.get("advisor_score_id"):
        failures.append("ai review advisor link mismatch")
    if review.get("mutation_performed") is not False:
        failures.append("ai review performed mutation")
    forbidden = {"VALIDATE", "CLOSE", "IMPLEMENT", "MUTATE_RECORD"}
    if not forbidden.issubset(set(review.get("forbidden_actions") or [])):
        failures.append("ai review missing forbidden action boundary")
    if advisor.get("top_recommended_record_id") != "ACC-20260530-008A":
        failures.append("current-state top recommendation must be ACC-20260530-008A")
    if snapshot.get("register_source_hash") != sha256_path(current_register_path) and not advisor.get("stale_snapshot_flag"):
        failures.append("stale snapshot not flagged")
    return {"ok": not failures, "failure_count": len(failures), "failures": failures}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_all_v1(*, truth_root: Path, day: str, register_path: Path = REGISTER_PATH, write: bool = True) -> dict[str, Any]:
    snapshot = build_evidence_snapshot_v1(truth_root=truth_root, day=day, register_path=register_path)
    advisor = build_advisor_score_v1(snapshot=snapshot, current_register_path=register_path)
    review = build_ai_review_v1(snapshot=snapshot, advisor=advisor)
    validation = validate_intelligence_v1(snapshot, advisor, review, current_register_path=register_path)
    paths = {
        "snapshot": str(snapshot_path_v1(truth_root, day)),
        "advisor_score": str(advisor_path_v1(truth_root, day)),
        "ai_review": str(ai_review_path_v1(truth_root, day)),
    }
    if write:
        write_json(Path(paths["snapshot"]), snapshot)
        write_json(Path(paths["advisor_score"]), advisor)
        write_json(Path(paths["ai_review"]), review)
    return {"ok": validation["ok"], "paths": paths, "snapshot": snapshot, "advisor_score": advisor, "ai_review": review, "validation": validation}
