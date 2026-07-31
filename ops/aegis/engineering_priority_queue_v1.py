from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1
from ops.aegis.operator_portfolio_valuation_estimate_v1 import (
    build_operator_portfolio_valuation_estimate_v1,
    operator_portfolio_valuation_estimate_path_v1,
    write_operator_portfolio_valuation_estimate_v1,
)

FAMILY = "aegis_engineering_priority_queue_v1"
FILENAME = "engineering_priority_queue.v1.json"
CLASSIFICATIONS = {
    "BLOCKING",
    "DEGRADED",
    "WAITING_FOR_DATA",
    "WAITING_FOR_TIME",
    "WAITING_FOR_OPERATOR",
    "INFORMATIONAL",
}
PRIORITIES = {"P0", "P1", "P2", "P3"}
REPAIR_STATUSES = {
    "REPAIR_AVAILABLE",
    "REPAIR_UNAVAILABLE",
    "REPAIR_NEEDS_RECOVERY_PLAN",
    "REPAIR_MANUAL_INVESTIGATION",
    "VERIFY_ONLY",
}
ACTION_TYPES = {
    "USER_ACTION",
    "SYSTEM_REPAIR",
    "WAITING_FOR_DATA",
    "WAITING_FOR_TIME",
    "VERIFY_ONLY",
}
VERIFY_ONLY_COMMAND_MARKERS = (
    "npm run aegis:audit",
    "npm run aegis:portal-smoke",
    "npm run aegis:verified-graph",
    "self-check",
    " status",
    ":status",
    "-status",
    " report",
    ":report",
    "-report",
)

SAFETY = {
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_live_trading_allowed": False,
    "read_only": True,
}


def engineering_priority_queue_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / FAMILY / str(day_utc) / FILENAME


def build_engineering_priority_queue_v1(*, truth_root: Path | str, day_utc: str, requested_day: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    paths = _source_paths(root, day)
    estimate_path = operator_portfolio_valuation_estimate_path_v1(truth_root=root, day_utc=day)
    estimate_payload = read_json_v1(estimate_path)
    if not isinstance(estimate_payload, dict) or not estimate_payload:
        estimate_payload = build_operator_portfolio_valuation_estimate_v1(truth_root=root, day_utc=day)
        estimate_path = write_operator_portfolio_valuation_estimate_v1(truth_root=root, day_utc=day, payload=estimate_payload)
    paths["operator_portfolio_valuation_estimate"] = estimate_path
    payloads = {name: _read_source(path, text=name == "audit_handoff") for name, path in paths.items()}
    payloads["operator_portfolio_valuation_estimate"] = estimate_payload
    source_artifacts = [_source_artifact(name, path, payloads.get(name)) for name, path in paths.items()]

    issues: list[dict[str, Any]] = []
    issues.extend(_issues_from_runtime_truth(payloads.get("runtime_truth_kernel", {}), paths["runtime_truth_kernel"], generated_at, day))
    issues.extend(_issues_from_verified_graph(payloads.get("verified_runtime_graph", {}), paths["verified_runtime_graph"], generated_at, day))
    issues.extend(_issues_from_control_packet(payloads.get("chatgpt_control_packet", {}), paths["chatgpt_control_packet"], generated_at, day))
    issues.extend(_issues_from_queue_audit(payloads.get("command_center_queue_audit", {}), paths["command_center_queue_audit"], generated_at, day))
    issues.extend(_issues_from_candidate_diagnostics(payloads.get("candidate_generation_diagnostics", {}), paths["candidate_generation_diagnostics"], generated_at, day))
    issues.extend(_issues_from_portfolio_valuation_estimate(payloads.get("operator_portfolio_valuation_estimate", {}), paths["operator_portfolio_valuation_estimate"], generated_at, day))
    issues.extend(_issues_from_missing_sources(source_artifacts, generated_at, day))

    deduped = _dedupe_issues(issues)
    ordered = sorted(deduped, key=_issue_sort_key)
    counts = Counter(str(row.get("classification") or "INFORMATIONAL") for row in ordered)
    priority_counts = Counter(str(row.get("priority") or "P3") for row in ordered)
    action_type_counts = Counter(str(row.get("action_type") or "VERIFY_ONLY") for row in ordered)
    operator_action_rows = [row for row in ordered if row.get("action_type") == "USER_ACTION"]
    system_repair_rows = [row for row in ordered if row.get("action_type") in {"SYSTEM_REPAIR", "WAITING_FOR_DATA", "WAITING_FOR_TIME"}]
    run_health = _run_health(payloads=payloads, paths=paths, day=day)
    requested = str(requested_day or day)
    operational_day = str(_first_text(
        _dig(payloads.get("canonical_operator_state", {}), "day_utc"),
        _dig(payloads.get("runtime_truth_kernel", {}), "day_utc"),
        day,
    ))
    payload = {
        "schema_id": FAMILY,
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": day,
        "requested_day": requested,
        "source_day": day,
        "operational_day": operational_day,
        "generated_at": generated_at,
        "as_of": generated_at,
        "status": "READY",
        "summary": {
            "total_issues": len(ordered),
            "blocking_issues": counts.get("BLOCKING", 0),
            "degraded_issues": counts.get("DEGRADED", 0),
            "waiting_for_data": counts.get("WAITING_FOR_DATA", 0),
            "waiting_for_time": counts.get("WAITING_FOR_TIME", 0),
            "waiting_for_operator": counts.get("WAITING_FOR_OPERATOR", 0),
            "informational": counts.get("INFORMATIONAL", 0),
            "p0": priority_counts.get("P0", 0),
            "p1": priority_counts.get("P1", 0),
            "p2": priority_counts.get("P2", 0),
            "p3": priority_counts.get("P3", 0),
            "fix_first_count": min(5, len(ordered)),
            "operator_actions_required": len(operator_action_rows),
            "system_repair_actions": len(system_repair_rows),
            "action_type_counts": dict(action_type_counts),
            "data_readiness": _data_readiness(payloads.get("runtime_truth_kernel", {})),
            "runtime_health": _runtime_health(payloads.get("runtime_truth_kernel", {}), payloads.get("verified_runtime_graph", {})),
            "graph_validation_status": _graph_validation_status(payloads.get("verified_runtime_graph", {})),
            "runtime_readiness_status": _runtime_readiness_status(payloads.get("runtime_truth_kernel", {})),
            "runtime_readiness_explanation": _runtime_readiness_explanation(payloads.get("runtime_truth_kernel", {}), payloads.get("verified_runtime_graph", {})),
            "certified_same_day_marks": _certified_mark_status(payloads.get("operator_portfolio_valuation_estimate", {})),
            "latest_estimated_marks": _latest_estimated_mark_status(payloads.get("operator_portfolio_valuation_estimate", {})),
            "pricing_limitation": _pricing_limitation_status(payloads.get("operator_portfolio_valuation_estimate", {})),
        },
        "fix_first": ordered[:5],
        "active_blockers": [row for row in ordered if row.get("classification") == "BLOCKING"],
        "degraded_but_functional": [row for row in ordered if row.get("classification") in {"DEGRADED", "WAITING_FOR_DATA", "WAITING_FOR_TIME", "WAITING_FOR_OPERATOR"}],
        "operator_actions_required": operator_action_rows,
        "system_repair_actions": system_repair_rows,
        "informational": [row for row in ordered if row.get("classification") == "INFORMATIONAL"],
        "issues": ordered,
        "run_health": run_health,
        "day_clarity": {
            "requested_day": requested,
            "source_day": day,
            "operational_day": operational_day,
            "is_historical_operational_session": requested != day or operational_day != day,
        },
        "diagnostics": {
            "collapsed_by_default": True,
            "source_artifacts": source_artifacts,
            "raw_issue_count": len(issues),
            "deduped_issue_count": len(ordered),
        },
        "source_artifacts": source_artifacts,
        "source_artifact_paths": {name: str(path) for name, path in paths.items()},
        "source_artifact_hashes": {name: _sha256(path) for name, path in paths.items() if path.exists()},
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({k: v for k, v in payload.items() if k not in {"generated_at", "as_of", "content_hash"}})
    return payload


def write_engineering_priority_queue_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_engineering_priority_queue_v1(truth_root=truth_root, day_utc=day_utc))
    return write_json_v1(engineering_priority_queue_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "runtime_truth_kernel": root / "reports" / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json",
        "runtime_evaluation": root / "reports" / "aegis_runtime_truth_kernel_v1" / day / "runtime_evaluation.v1.json",
        "missing_stale_sources": root / "reports" / "aegis_runtime_truth_kernel_v1" / day / "missing_stale_sources.v1.json",
        "recovery_plan": root / "reports" / "aegis_runtime_truth_kernel_v1" / day / "recovery_plan.v1.txt",
        "verified_runtime_graph": root / "reports" / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json",
        "audit_handoff": root / "reports" / "aegis_audit_handoff_v1" / day / "aegis_audit_handoff.txt",
        "chatgpt_control_packet": root / "reports" / "aegis_chatgpt_control_packet_v1" / day / "aegis_chatgpt_control_packet.v1.json",
        "canonical_operator_state": root / "reports" / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json",
        "candidate_generation_diagnostics": root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json",
        "command_center_queue_audit": root / "reports" / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json",
    }


def _read_source(path: Path, *, text: bool = False) -> Any:
    if text:
        try:
            return path.read_text(encoding="utf-8")
        except OSError:
            return ""
    return read_json_v1(path)


def _source_artifact(name: str, path: Path, payload: Any) -> dict[str, Any]:
    return {
        "source_id": name,
        "path": str(path),
        "status": "AVAILABLE" if path.exists() else "MISSING",
        "timestamp": _mtime(path),
        "content_hash": _sha256(path) if path.exists() else "",
        "row_count": _row_count(payload),
    }


def _issues_from_runtime_truth(payload: Mapping[str, Any], path: Path, timestamp: str, day: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    classification = str(payload.get("runtime_truth_classification") or "UNKNOWN")
    highest = str(payload.get("highest_readiness_layer") or "UNKNOWN")
    missing_count = int(_number(payload.get("missing_or_stale_source_count"), 0) or 0)
    runtime_evaluation = payload.get("runtime_evaluation") if isinstance(payload.get("runtime_evaluation"), Mapping) else {}
    root_blockers = [row for row in runtime_evaluation.get("root_blockers", []) if isinstance(row, Mapping)] if isinstance(runtime_evaluation.get("root_blockers"), list) else []
    root_blocker_ids = [str(row.get("blocker_id") or row.get("schema_id") or "runtime blocker") for row in root_blockers]
    if classification and classification not in {"READY", "FULL_CONTEXT", "CANONICAL"}:
        if missing_count:
            cause = f"Runtime truth kernel reports highest readiness layer {highest} with {missing_count} missing or stale source(s)."
            evidence = f"runtime_truth_classification={classification}; highest_readiness_layer={highest}; missing_or_stale_source_count={missing_count}"
            repair_action = "Regenerate stale or missing runtime truth dependencies, then rerun audit."
        elif root_blocker_ids:
            cause = f"Runtime truth kernel reports highest readiness layer {highest} with explicit root blocker(s): {', '.join(root_blocker_ids[:4])}."
            evidence = f"runtime_truth_classification={classification}; highest_readiness_layer={highest}; missing_or_stale_source_count=0; root_blockers={', '.join(root_blocker_ids)}"
            repair_action = "Inspect the runtime recovery plan root blockers, then rerun audit after the authoritative evidence is repaired or explicitly accepted."
        else:
            cause = f"Runtime truth kernel reports highest readiness layer {highest}; no missing/stale count or root blocker detail was published."
            evidence = f"runtime_truth_classification={classification}; highest_readiness_layer={highest}; missing_or_stale_source_count={missing_count}"
            repair_action = "Inspect runtime truth diagnostics, then rerun audit."
        issues.append(_issue(
            day=day,
            timestamp=timestamp,
            issue=f"Runtime truth is {classification}",
            classification="BLOCKING" if highest == "BLOCKED" else "DEGRADED",
            priority="P1" if highest == "BLOCKED" else "P2",
            impact="Core readiness gates remain unavailable; operator surfaces must treat runtime truth as blocked or partial.",
            cause=cause,
            evidence=evidence,
            repair_action=repair_action,
            repair_command=f"TARGET_DAY={day} npm run aegis:audit",
            source_artifacts=[str(path)],
        ))
    for cap in payload.get("blocked_capabilities") or []:
        cap_text = str(cap)
        if not cap_text:
            continue
        issues.append(_issue(
            day=day,
            timestamp=timestamp,
            issue=f"Capability blocked: {cap_text}",
            classification=_classification_for_capability(cap_text),
            priority=_priority_for_capability(cap_text),
            impact=_impact_for_capability(cap_text),
            cause="Runtime truth kernel has not certified this capability for the requested day.",
            evidence=f"blocked_capabilities includes {cap_text}",
            repair_action="Use the recovery plan for this runtime truth dependency.",
            repair_command=f"TARGET_DAY={day} npm run aegis:audit",
            source_artifacts=[str(path)],
        ))
    return issues


def _issues_from_verified_graph(payload: Mapping[str, Any], path: Path, timestamp: str, day: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    status = str(payload.get("graph_status") or "UNKNOWN")
    blockers = payload.get("audit_blockers") if isinstance(payload.get("audit_blockers"), list) else []
    if status != "READY":
        issues.append(_issue(
            day=day,
            timestamp=timestamp,
            issue=f"Verified runtime graph is {status}",
            classification="BLOCKING",
            priority="P0",
            impact="Aegis cannot trust verified runtime evidence until the graph is ready.",
            cause="Verified graph builder did not report READY.",
            evidence=f"graph_status={status}",
            repair_action="Regenerate the verified runtime graph in strict mode.",
            repair_command=f"TARGET_DAY={day} npm run aegis:verified-graph -- --strict",
            source_artifacts=[str(path)],
        ))
    for blocker in blockers:
        text = str(blocker.get("message") if isinstance(blocker, Mapping) else blocker)
        issues.append(_issue(
            day=day,
            timestamp=timestamp,
            issue=text or "Verified graph audit blocker",
            classification="BLOCKING",
            priority="P0",
            impact="Verified graph audit has a blocker.",
            cause=text or "Audit blocker present.",
            evidence=json.dumps(blocker, sort_keys=True) if isinstance(blocker, Mapping) else text,
            repair_action="Repair the audit blocker and rebuild the verified graph.",
            repair_command=f"TARGET_DAY={day} npm run aegis:audit",
            source_artifacts=[str(path)],
        ))
    return issues


def _issues_from_control_packet(payload: Mapping[str, Any], path: Path, timestamp: str, day: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    actions = payload.get("next_operator_actions") if isinstance(payload.get("next_operator_actions"), list) else []
    reason = str(payload.get("reason_if_blocked") or "")
    for idx, action in enumerate(actions[:8]):
        action_text = str(action)
        command = _extract_command(action_text)
        issues.append(_issue(
            day=day,
            timestamp=timestamp,
            issue=_short_action_issue(action_text, idx),
            classification=_classification_for_action(action_text),
            priority=_priority_for_action(action_text, idx),
            impact="Runtime truth remains partial until this dependency is current.",
            cause=reason or "Control packet published this as a next operator action.",
            evidence=action_text,
            repair_action=_repair_action_from_text(action_text),
            repair_command=command,
            source_artifacts=[str(path)],
        ))
    return issues


def _issues_from_queue_audit(payload: Mapping[str, Any], path: Path, timestamp: str, day: str) -> list[dict[str, Any]]:
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    issues: list[dict[str, Any]] = []
    awaiting = int(_number(summary.get("incorrectly_shown_as_awaiting_review_count"), 0) or 0)
    needs = int(_number(summary.get("incorrectly_shown_as_needs_attention_count"), 0) or 0)
    if awaiting or needs:
        issues.append(_issue(
            day=day,
            timestamp=timestamp,
            issue="Command Center queue semantics degraded",
            classification="DEGRADED",
            priority="P2",
            impact="Operator action queues may include rows that do not require action.",
            cause=f"Queue audit found {awaiting} incorrectly Awaiting Review row(s) and {needs} incorrectly Needs Attention row(s).",
            evidence=f"incorrect_awaiting_review={awaiting}; incorrect_needs_attention={needs}",
            repair_action="Regenerate queue audit and verify Command Center filtering.",
            repair_command=f"TARGET_DAY={day} npm run aegis:command-center-queue-audit",
            source_artifacts=[str(path)],
        ))
    return issues


def _issues_from_candidate_diagnostics(payload: Mapping[str, Any], path: Path, timestamp: str, day: str) -> list[dict[str, Any]]:
    if not payload:
        return []
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else payload
    status = str(summary.get("candidate_generation_status") or payload.get("candidate_generation_status") or "UNKNOWN")
    interpretation = str(summary.get("operator_interpretation") or payload.get("operator_interpretation") or "")
    if status in {"READY", "COMPLETE", "PASS"}:
        return []
    classification = "WAITING_FOR_DATA" if "DATA" in interpretation.upper() else "DEGRADED"
    issues = [_issue(
        day=day,
        timestamp=timestamp,
        issue=f"Candidate generation diagnostics {status}",
        classification=classification,
        priority="P2",
        impact="Candidate run visibility is degraded until diagnostics explain the current-day run.",
        cause=f"Candidate diagnostics interpretation is {interpretation or 'not available'}.",
        evidence=f"candidate_generation_status={status}; operator_interpretation={interpretation}",
        repair_action="Review candidate generation diagnostics and rerun the diagnostics producer after inputs are repaired.",
        repair_command=f"TARGET_DAY={day} npm run aegis:candidate-diagnostics",
        source_artifacts=[str(path)],
    )]
    return issues


def _issues_from_portfolio_valuation_estimate(payload: Mapping[str, Any], path: Path, timestamp: str, day: str) -> list[dict[str, Any]]:
    if not payload:
        return []
    open_count = int(_number(payload.get("open_position_count"), 0) or 0)
    missing_count = int(_number(payload.get("missing_estimate_count"), 0) or 0)
    marked_count = int(_number(payload.get("marked_position_count"), 0) or 0)
    if open_count <= 0 or missing_count <= 0:
        return []
    if marked_count > 0:
        return [_issue(
            day=day,
            timestamp=timestamp,
            issue="Latest estimated marks are partially unavailable",
            classification="DEGRADED",
            priority="P2",
            impact="Operator portfolio estimates are partial; canonical P&L remains governed by certified mark coverage.",
            cause=f"The operator valuation estimate marked {marked_count}/{open_count} open positions.",
            evidence=f"valuation_status={payload.get('valuation_status')}; missing_estimate_count={missing_count}; latest_available_market_session={payload.get('latest_available_market_session')}",
            repair_action="Inspect latest market-data availability for the missing estimate symbols; do not use estimates for trade advice.",
            repair_command=f"TARGET_DAY={day} npm run aegis:operator-valuation-estimate",
            source_artifacts=[str(path)],
        )]
    return [_issue(
        day=day,
        timestamp=timestamp,
        issue="Latest estimated marks unavailable",
        classification="WAITING_FOR_DATA",
        priority="P1",
        impact="Operator portfolio value cannot be estimated from latest available marks; canonical P&L remains unavailable until certified marks exist.",
        cause="No latest available marks were found for open positions in the operator valuation estimate.",
        evidence=f"valuation_status={payload.get('valuation_status')}; open_position_count={open_count}; missing_estimate_count={missing_count}",
        repair_action="Regenerate market-data refresh and the operator valuation estimate, then verify whether latest marks exist.",
        repair_command=f"TARGET_DAY={day} npm run aegis:operator-valuation-estimate",
        source_artifacts=[str(path)],
    )]


def _certified_mark_status(payload: Mapping[str, Any]) -> str:
    if int(_number(payload.get("open_position_count"), 0) or 0) <= 0:
        return "NO_OPEN_POSITIONS"
    return "UNAVAILABLE" if str(payload.get("valuation_status") or "") == "ESTIMATED_NOT_CERTIFIED_FOR_TARGET_DAY" else "UNKNOWN"


def _latest_estimated_mark_status(payload: Mapping[str, Any]) -> str:
    if str(payload.get("valuation_status") or "") == "ESTIMATED_NOT_CERTIFIED_FOR_TARGET_DAY":
        return "AVAILABLE"
    return "UNAVAILABLE"


def _pricing_limitation_status(payload: Mapping[str, Any]) -> str:
    if str(payload.get("estimate_reason") or "") == "NON_TRADING_DAY_PRIOR_SESSION_MARKS":
        return "EXPECTED_NON_TRADING_DAY_LIMITATION_NOT_REPAIR"
    if str(payload.get("estimate_reason") or "") == "TARGET_DAY_MARKS_NOT_CERTIFIED":
        return "TARGET_DAY_CERTIFICATION_LIMITATION"
    return "LATEST_ESTIMATE_DATA_REPAIR_NEEDED"


def _issues_from_missing_sources(source_artifacts: list[dict[str, Any]], timestamp: str, day: str) -> list[dict[str, Any]]:
    issues = []
    for source in source_artifacts:
        if source.get("status") != "MISSING":
            continue
        source_id = str(source.get("source_id") or "source")
        issues.append(_issue(
            day=day,
            timestamp=timestamp,
            issue=f"Source artifact missing: {source_id}",
            classification="DEGRADED" if source_id in {"command_center_queue_audit", "candidate_generation_diagnostics"} else "WAITING_FOR_DATA",
            priority="P2" if source_id in {"command_center_queue_audit", "candidate_generation_diagnostics"} else "P1",
            impact="Engineering diagnostics cannot fully explain current state without this evidence.",
            cause="Expected source artifact is absent for the requested day.",
            evidence=str(source.get("path") or ""),
            repair_action=f"Regenerate {source_id} for the requested day.",
            repair_command=_repair_command_for_source(source_id, day),
            source_artifacts=[str(source.get("path") or "")],
        ))
    return issues


def _issue(*, day: str, timestamp: str, issue: str, classification: str, priority: str, impact: str, cause: str, evidence: str, repair_action: str, repair_command: str, source_artifacts: list[str]) -> dict[str, Any]:
    clean_classification = classification if classification in CLASSIFICATIONS else "INFORMATIONAL"
    clean_priority = priority if priority in PRIORITIES else "P3"
    operator = _operator_language_for_issue(issue=issue, classification=clean_classification, impact=impact, cause=cause, repair_action=repair_action)
    repair = _repair_metadata_for_issue(
        day=day,
        classification=clean_classification,
        issue=issue,
        repair_action=repair_action,
        repair_command=repair_command,
        source_artifacts=source_artifacts,
    )
    key = _stable_hash({"issue": issue, "classification": clean_classification, "cause": cause, "sources": source_artifacts})[:16]
    return {
        "issue_id": f"ENG-{day}-{key}",
        "issue": issue,
        "operator_issue": operator["issue"],
        "operator_summary": operator["summary"],
        "operator_impact": operator["impact"],
        "operator_next_step": operator["next_step"],
        "ask_aegis_question": _ask_aegis_question(operator["issue"]),
        "classification": clean_classification,
        "priority": clean_priority,
        "impact": impact,
        "cause": cause,
        "evidence": evidence,
        "human_evidence_summary": _human_evidence_summary(issue=issue, evidence=evidence, cause=cause),
        "raw_evidence": evidence,
        "repair_action": repair_action,
        "repair_status": repair["repair_status"],
        "repair_unavailable_reason": repair["repair_unavailable_reason"],
        "repair_command": repair["repair_command"],
        "verification_command": repair["verification_command"],
        "recovery_plan_id": repair["recovery_plan_id"],
        "recovery_plan_path": repair["recovery_plan_path"],
        "action_type": repair["action_type"],
        "repair_owner": "Aegis operator" if repair["action_type"] == "USER_ACTION" else "Aegis engineering",
        "source_artifacts": source_artifacts,
        "timestamp": timestamp,
        "confidence": "HIGH" if source_artifacts else "MEDIUM",
    }


def _repair_metadata_for_issue(*, day: str, classification: str, issue: str, repair_action: str, repair_command: str, source_artifacts: list[str]) -> dict[str, str]:
    original_command = str(repair_command or "").strip()
    verification_command = _verification_command_for_issue(day=day, repair_command=original_command)
    recovery_plan_path = _recovery_plan_path_for_issue(day=day, source_artifacts=source_artifacts)
    recovery_plan_id = "runtime_truth_recovery_plan" if recovery_plan_path else ""
    action_type = _action_type_for_issue(classification=classification, repair_command=original_command)

    if not original_command:
        status = "REPAIR_NEEDS_RECOVERY_PLAN" if recovery_plan_path else "REPAIR_UNAVAILABLE"
        return {
            "repair_status": status,
            "repair_unavailable_reason": "Repair command unavailable. Use Ask Aegis or inspect recovery plan.",
            "repair_command": "",
            "verification_command": verification_command,
            "recovery_plan_id": recovery_plan_id,
            "recovery_plan_path": recovery_plan_path,
            "action_type": action_type if action_type != "SYSTEM_REPAIR" else "VERIFY_ONLY",
        }

    if _is_verify_only_command(original_command):
        status = "REPAIR_NEEDS_RECOVERY_PLAN" if recovery_plan_path else "VERIFY_ONLY"
        return {
            "repair_status": status,
            "repair_unavailable_reason": "Repair command unavailable. Use Ask Aegis or inspect recovery plan.",
            "repair_command": "",
            "verification_command": verification_command,
            "recovery_plan_id": recovery_plan_id,
            "recovery_plan_path": recovery_plan_path,
            "action_type": "VERIFY_ONLY" if status == "VERIFY_ONLY" else action_type,
        }

    return {
        "repair_status": "REPAIR_AVAILABLE",
        "repair_unavailable_reason": "",
        "repair_command": original_command,
        "verification_command": verification_command,
        "recovery_plan_id": recovery_plan_id,
        "recovery_plan_path": recovery_plan_path,
        "action_type": action_type,
    }


def _action_type_for_issue(*, classification: str, repair_command: str) -> str:
    if classification == "WAITING_FOR_OPERATOR":
        return "USER_ACTION"
    if classification == "WAITING_FOR_TIME":
        return "WAITING_FOR_TIME"
    if classification == "WAITING_FOR_DATA":
        return "WAITING_FOR_DATA"
    if _is_verify_only_command(repair_command):
        return "VERIFY_ONLY"
    return "SYSTEM_REPAIR" if repair_command else "VERIFY_ONLY"


def _is_verify_only_command(command: str) -> bool:
    text = str(command or "").strip().lower()
    if not text:
        return False
    return any(marker in text for marker in VERIFY_ONLY_COMMAND_MARKERS)


def _recovery_plan_path_for_issue(*, day: str, source_artifacts: list[str]) -> str:
    for artifact in source_artifacts:
        text = str(artifact or "")
        if "aegis_runtime_truth_kernel_v1" in text:
            path = Path(text)
            if path.name != "recovery_plan.v1.txt":
                return str(path.with_name("recovery_plan.v1.txt"))
            return text
    return ""


def _human_evidence_summary(*, issue: str, evidence: str, cause: str) -> str:
    raw = str(evidence or "")
    if "root_blockers=" in raw:
        value = raw.split("root_blockers=", 1)[1].split(";", 1)[0].strip()
        return f"Runtime truth is blocked by explicit root blocker(s): {value}."
    if "missing_or_stale_source_count=" in raw:
        value = raw.split("missing_or_stale_source_count=", 1)[1].split(";", 1)[0].strip()
        return f"{value} required runtime sources are stale or missing."
    if raw.startswith("blocked_capabilities includes "):
        capability = raw.replace("blocked_capabilities includes ", "", 1).replace("_", " ").strip().title()
        return f"{capability} is not certified for the requested day."
    if "incorrect_awaiting_review=" in raw or "incorrect_needs_attention=" in raw:
        return "Command Center queue audit found rows assigned to the wrong operator-action bucket."
    if "candidate_generation_status=" in raw:
        return "Candidate generation diagnostics are not ready for the requested day."
    if raw and (raw.startswith("/") or raw.startswith("truth/")):
        return "A required engineering source artifact is missing for the requested day."
    return cause or str(issue or "Engineering evidence is available.")

def _operator_language_for_issue(*, issue: str, classification: str, impact: str, cause: str, repair_action: str) -> dict[str, str]:
    upper = issue.upper()
    if upper.startswith("RUNTIME TRUTH"):
        return {
            "issue": "Today's runtime evidence is incomplete",
            "summary": cause or "Aegis verified the evidence graph, but runtime truth remains blocked by explicit root blockers.",
            "impact": "Operator surfaces should be treated as partial until the runtime blockers are repaired or explicitly accepted.",
            "next_step": repair_action or "Inspect the runtime recovery plan, then rerun audit.",
        }
    if upper.startswith("CAPABILITY BLOCKED: DATA_READY"):
        return {
            "issue": "Today's data is not certified",
            "summary": "Aegis does not yet have enough current evidence to certify the day’s data layer.",
            "impact": impact or "Downstream views may be partial.",
            "next_step": repair_action or "Follow the runtime recovery plan for DATA_READY.",
        }
    if upper.startswith("CAPABILITY BLOCKED: EVENT_READY"):
        return {
            "issue": "Event monitoring is not certified",
            "summary": "Aegis cannot yet certify the event-monitoring inputs for the requested day.",
            "impact": impact or "Event-aware workflows may be incomplete.",
            "next_step": repair_action or "Regenerate event monitoring evidence.",
        }
    if upper.startswith("CAPABILITY BLOCKED:"):
        capability = issue.split(":", 1)[-1].strip().replace("_", " ").title()
        return {
            "issue": f"{capability} is unavailable",
            "summary": "Runtime truth has not certified this capability for the requested day.",
            "impact": impact or "This capability should not be relied on until certified.",
            "next_step": repair_action or "Use the recovery plan for this dependency.",
        }
    if upper.startswith("REGENERATE "):
        artifact = issue.replace("Regenerate ", "", 1).replace("_", " ")
        return {
            "issue": f"Refresh {artifact}",
            "summary": f"A required evidence source for {artifact} is stale or missing.",
            "impact": impact or "Runtime truth remains partial until this source is current.",
            "next_step": repair_action or f"Regenerate {artifact} for the requested day.",
        }
    if upper.startswith("SOURCE ARTIFACT MISSING:"):
        source = issue.split(":", 1)[-1].strip().replace("_", " ")
        return {
            "issue": f"Missing {source} evidence",
            "summary": "Engineering cannot fully explain current state until this evidence exists for the requested day.",
            "impact": impact or "Diagnostics are incomplete.",
            "next_step": repair_action or f"Regenerate {source}.",
        }
    return {
        "issue": issue,
        "summary": cause or "Aegis reported an engineering issue for the requested day.",
        "impact": impact or "Review the issue before relying on this workflow.",
        "next_step": repair_action or "Review details and rerun audit after repair.",
    }


def _ask_aegis_question(operator_issue: str) -> str:
    clean = operator_issue.rstrip(".?")
    return f"Why is this blocked, and what should be fixed first: {clean}?"

def _verification_command_for_issue(*, day: str, repair_command: str) -> str:
    command = str(repair_command or "").strip()
    if "npm run aegis:audit" in command and command.startswith("TARGET_DAY="):
        return command
    return f"TARGET_DAY={day} npm run aegis:audit"


def _dedupe_issues(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = str(row.get("issue_id") or row.get("issue") or "")
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _issue_sort_key(row: Mapping[str, Any]) -> tuple[int, int, str]:
    p = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}.get(str(row.get("priority")), 9)
    c = {"BLOCKING": 0, "WAITING_FOR_DATA": 1, "WAITING_FOR_OPERATOR": 2, "WAITING_FOR_TIME": 3, "DEGRADED": 4, "INFORMATIONAL": 5}.get(str(row.get("classification")), 9)
    return p, c, str(row.get("issue") or "")


def _run_health(*, payloads: Mapping[str, Any], paths: Mapping[str, Path], day: str) -> dict[str, Any]:
    canonical = payloads.get("canonical_operator_state", {}) if isinstance(payloads.get("canonical_operator_state"), Mapping) else {}
    diagnostics = payloads.get("candidate_generation_diagnostics", {}) if isinstance(payloads.get("candidate_generation_diagnostics"), Mapping) else {}
    runtime = payloads.get("runtime_truth_kernel", {}) if isinstance(payloads.get("runtime_truth_kernel"), Mapping) else {}
    return {
        "last_run": _first_text(_dig(canonical, "candidate_ui_projection", "run_visibility", "last_run_at"), _dig(diagnostics, "started_at"), _dig(diagnostics, "generated_at"), "NOT_RECORDED"),
        "last_successful_run": _first_text(_dig(canonical, "candidate_ui_projection", "run_visibility", "last_successful_run_at"), _dig(canonical, "generated_at_utc"), "NOT_RECORDED"),
        "next_expected_run": f"{day}T09:50:00-04:00",
        "run_status": _first_text(_dig(canonical, "candidate_ui_projection", "run_visibility", "run_visibility_status"), _dig(diagnostics, "candidate_generation_status"), "UNKNOWN"),
        "missing_run_visibility": not bool(_dig(canonical, "candidate_ui_projection", "run_visibility")),
        "runtime_truth_classification": str(runtime.get("runtime_truth_classification") or "UNKNOWN"),
        "source_artifacts": {name: str(path) for name, path in paths.items() if name in {"canonical_operator_state", "candidate_generation_diagnostics", "runtime_truth_kernel"}},
    }


def _data_readiness(runtime: Mapping[str, Any]) -> str:
    blocked = [str(x) for x in runtime.get("blocked_capabilities") or []]
    if "DATA_READY" in blocked:
        return "BLOCKED"
    return "READY" if runtime else "UNKNOWN"


def _runtime_health(runtime: Mapping[str, Any], graph: Mapping[str, Any]) -> str:
    if str(graph.get("graph_status") or "") == "READY" and str(runtime.get("highest_readiness_layer") or "") != "BLOCKED":
        return "RUNTIME_READY"
    if str(graph.get("graph_status") or "") == "READY":
        return "GRAPH_READY_RUNTIME_BLOCKED"
    return "GRAPH_OR_RUNTIME_DEGRADED"


def _graph_validation_status(graph: Mapping[str, Any]) -> str:
    return str(graph.get("graph_status") or "UNKNOWN")


def _runtime_readiness_status(runtime: Mapping[str, Any]) -> str:
    highest = str(runtime.get("highest_readiness_layer") or "UNKNOWN")
    classification = str(runtime.get("runtime_truth_classification") or "UNKNOWN")
    if highest == "BLOCKED":
        return "BLOCKED"
    if classification in {"READY", "FULL_CONTEXT", "CANONICAL"}:
        return "READY"
    if classification == "PARTIAL_CONTEXT":
        return "PARTIAL"
    return classification or "UNKNOWN"


def _runtime_readiness_explanation(runtime: Mapping[str, Any], graph: Mapping[str, Any]) -> str:
    graph_status = _graph_validation_status(graph)
    runtime_status = _runtime_readiness_status(runtime)
    missing_count = int(_number(runtime.get("missing_or_stale_source_count"), 0) or 0)
    if graph_status == "READY" and runtime_status == "BLOCKED":
        return f"Graph validation passed, but runtime readiness is blocked because required evidence is missing or stale. Missing or stale sources: {missing_count}."
    if graph_status == "READY" and runtime_status == "READY":
        return "The verified graph and runtime truth are both ready."
    return f"Graph validation is {graph_status}; runtime readiness is {runtime_status}."


def _classification_for_capability(capability: str) -> str:
    cap = capability.upper()
    if cap in {"DATA_READY", "EVENT_READY", "RESEARCH_READY", "FEEDBACK_READY", "ALERT_GATE_PROVEN", "ALERT_TRANSPORT_PROVEN", "ALERT_DRY_RUN_PROVEN"}:
        return "WAITING_FOR_DATA"
    if "TRADE" in cap or "BROKER" in cap or "LIVE" in cap or "AUTONOMOUS" in cap or "MANUAL" in cap:
        return "INFORMATIONAL"
    return "DEGRADED"


def _priority_for_capability(capability: str) -> str:
    cap = capability.upper()
    if cap in {"DATA_READY", "EVENT_READY"}:
        return "P1"
    if cap in {"TRADE_ADVICE_ALLOWED", "BROKER_SUBMIT_TRANSMIT", "LIVE_TRADE_READY", "AUTONOMOUS_EXECUTION_ALLOWED"}:
        return "P3"
    return "P2"


def _impact_for_capability(capability: str) -> str:
    cap = capability.upper()
    if cap == "DATA_READY":
        return "Current-day data readiness is not certified. Downstream surfaces may be partial."
    if cap == "EVENT_READY":
        return "Event monitoring readiness is incomplete."
    if "TRADE" in cap or "BROKER" in cap or "LIVE" in cap or "AUTONOMOUS" in cap:
        return "Safety gate remains disabled by policy or runtime truth; this is not a repair target unless policy changes."
    return "Runtime capability is unavailable or partial."


def _classification_for_action(text: str) -> str:
    upper = text.upper()
    if "MANUAL_EXECUTION_RECEIPT" in upper or "RECEIPT_TYPE" in upper:
        return "WAITING_FOR_OPERATOR"
    if "REGENERATE" in upper or "MISSING" in upper or "STALE" in upper or "DATA" in upper or "EVENT" in upper:
        return "WAITING_FOR_DATA"
    return "DEGRADED"


def _priority_for_action(text: str, idx: int) -> str:
    upper = text.upper()
    if "DATA_READY" in upper or "EVENT_READY" in upper:
        return "P1"
    if idx < 3:
        return "P2"
    return "P3"


def _short_action_issue(text: str, idx: int) -> str:
    prefix = text.split(":", 1)[0].strip()
    if prefix:
        return prefix[:120]
    return f"Control packet next action {idx + 1}"


def _repair_action_from_text(text: str) -> str:
    return text.split(";", 1)[0].strip() or "Review recovery action."


def _extract_command(text: str) -> str:
    marker = "run `"
    if marker in text:
        tail = text.split(marker, 1)[1]
        return tail.split("`", 1)[0]
    if "npm run" in text:
        idx = text.find("npm run")
        return text[idx:].split(";", 1)[0].strip().rstrip(".")
    return ""


def _repair_command_for_source(source_id: str, day: str) -> str:
    mapping = {
        "runtime_truth_kernel": "npm run aegis:truth-kernel",
        "verified_runtime_graph": "npm run aegis:verified-graph -- --strict",
        "chatgpt_control_packet": "npm run aegis:chatgpt:control-packet",
        "canonical_operator_state": "npm run aegis:canonical-operator-state",
        "candidate_generation_diagnostics": "npm run aegis:candidate-diagnostics",
        "command_center_queue_audit": "npm run aegis:command-center-queue-audit",
    }
    command = mapping.get(source_id, "npm run aegis:audit")
    return f"TARGET_DAY={day} {command}"


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _mtime(path: Path) -> str:
    try:
        from datetime import UTC, datetime
        return datetime.fromtimestamp(path.stat().st_mtime, UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except OSError:
        return ""


def _row_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, Mapping):
        for key in ("issues", "rows", "sleeves", "responses", "source_artifacts"):
            if isinstance(payload.get(key), list):
                return len(payload.get(key) or [])
    if isinstance(payload, str):
        return len([line for line in payload.splitlines() if line.strip()])
    return 0


def _read_json_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _dig(payload: Any, *keys: str) -> Any:
    current = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _first_text(*values: Any) -> str:
    for value in values:
        if value is not None and str(value).strip():
            return str(value)
    return ""


def _number(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
