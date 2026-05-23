from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ops.aegis.candidate_decision_support_v1 import build_candidate_decision_support_brief_v1
from ops.aegis.candidate_lifecycle_v1 import append_candidate_review_action_v1
from ops.aegis.candidate_manual_capture_v1 import append_manual_external_capture_v1


PROJECTION_VERSION = "operator_command_projection.v1"
EVENT_FAMILY = "aegis_operator_event_log_v1"
OWNERSHIP_OPERATOR_ACTIONABLE = "operator_actionable"
OWNERSHIP_SYSTEM_DIAGNOSTIC = "system_diagnostic"
OWNERSHIP_INFORMATIONAL = "informational"
OWNERSHIP_PASSIVE_HEALTH = "passive_health"

ALLOWED_COMMANDS = {
    "REVIEW_CANDIDATE",
    "WATCHLIST_CANDIDATE",
    "DISMISS_CANDIDATE",
    "REQUEST_MORE_EVIDENCE",
    "RECORD_MANUAL_EXTERNAL_CAPTURE",
    "MARK_CANDIDATE_REVIEWED",
    "OPEN_EVIDENCE",
    "REFRESH_MARKET_DATA",
    "RERUN_CANDIDATE_DIAGNOSTICS",
    "RERUN_PREFLIGHT",
    "MARK_BLOCKER_REVIEWED",
    "MARK_PROVIDER_DATA_NEEDED",
    "OPEN_HUMAN_REVIEW_DOSSIER",
    "RECORD_PAPER_OBSERVATION",
}

FORBIDDEN_COMMANDS = {
    "BUY",
    "SELL",
    "EXECUTE",
    "SUBMIT_ORDER",
    "ROUTE_ORDER",
    "ALLOCATE_CAPITAL",
    "PROMOTE_AUTOMATICALLY",
    "MUTATE_SLEEVE",
    "OVERRIDE_STALE_DATA_WITHOUT_POLICY",
}

COMMAND_EVENT_TYPES = {
    "REVIEW_CANDIDATE": "CANDIDATE_REVIEWED",
    "WATCHLIST_CANDIDATE": "CANDIDATE_WATCHLISTED",
    "DISMISS_CANDIDATE": "CANDIDATE_DISMISSED",
    "REQUEST_MORE_EVIDENCE": "MORE_EVIDENCE_REQUESTED",
    "RECORD_MANUAL_EXTERNAL_CAPTURE": "MANUAL_EXTERNAL_CAPTURE_RECORDED",
    "MARK_CANDIDATE_REVIEWED": "CANDIDATE_REVIEWED",
    "MARK_BLOCKER_REVIEWED": "BLOCKER_REVIEWED",
    "MARK_PROVIDER_DATA_NEEDED": "PROVIDER_DATA_NEEDED_MARKED",
    "REFRESH_MARKET_DATA": "MARKET_DATA_REFRESH_REQUESTED",
    "RERUN_CANDIDATE_DIAGNOSTICS": "DIAGNOSTICS_RERUN_REQUESTED",
    "RERUN_PREFLIGHT": "PREFLIGHT_RERUN_REQUESTED",
    "RECORD_PAPER_OBSERVATION": "PAPER_OBSERVATION_RECORDED",
    "OPEN_EVIDENCE": "EVIDENCE_OPENED",
    "OPEN_HUMAN_REVIEW_DOSSIER": "HUMAN_REVIEW_DOSSIER_OPENED",
}

REQUIRES_FRESH_FINGERPRINT = {"RECORD_MANUAL_EXTERNAL_CAPTURE"}

OPERATOR_TASK_TYPES = {
    "candidate_review",
    "manual_capture_available",
    "request_more_evidence",
    "paper_observation",
    "human_review_decision",
    "watchlist_review",
    "dismiss_review",
    "blocker_resolution_if_operator_actionable",
}

SYSTEM_DIAGNOSTIC_TYPES = {
    "runtime_truth_unavailable",
    "runtime_truth_partial",
    "stale_market_data",
    "missing_market_data",
    "missing_source_artifact",
    "blocked_sleeve",
    "incomplete_evidence_linkage",
    "advisory_projection_incomplete",
    "projection_inconsistency",
    "partial_readiness",
    "data_validation_failed",
    "provider_unavailable",
    "remediation_outcome",
}

INFORMATIONAL_TYPES = {
    "new_candidate_generated",
    "challenger_comparison_completed",
    "sleeve_drift_worsened",
    "dossier_prepared",
    "paper_trial_updated",
}

PASSIVE_HEALTH_TYPES = {
    "readiness",
    "advisory",
    "stale_data",
    "blocked_sleeves",
    "paper_trial_active",
    "drift_degrading",
    "fragility_watch",
}

COMMAND_LABELS = {
    "REVIEW_CANDIDATE": "Review Candidate",
    "WATCHLIST_CANDIDATE": "Watchlist Candidate",
    "DISMISS_CANDIDATE": "Dismiss Candidate",
    "REQUEST_MORE_EVIDENCE": "Request More Evidence",
    "RECORD_MANUAL_EXTERNAL_CAPTURE": "Record Manual Capture",
    "MARK_CANDIDATE_REVIEWED": "Mark Candidate Reviewed",
    "OPEN_EVIDENCE": "Open Candidate Evidence",
    "REFRESH_MARKET_DATA": "Refresh Market Data",
    "RERUN_CANDIDATE_DIAGNOSTICS": "Rerun Candidate Diagnostics",
    "RERUN_PREFLIGHT": "Rerun Preflight",
    "MARK_BLOCKER_REVIEWED": "Mark Blocker Reviewed",
    "MARK_PROVIDER_DATA_NEEDED": "Mark Provider Data Needed",
    "OPEN_HUMAN_REVIEW_DOSSIER": "Open Human Review Dossier",
    "RECORD_PAPER_OBSERVATION": "Record Paper Observation",
}

GENERIC_ACTION_LABELS = {"Review", "Open", "Investigate", "Resolve"}

WORKFLOW_ROUTES = {
    "REVIEW_CANDIDATE": "/aegis-opportunities",
    "WATCHLIST_CANDIDATE": "/aegis-opportunities",
    "DISMISS_CANDIDATE": "/aegis-opportunities",
    "REQUEST_MORE_EVIDENCE": "/aegis-opportunities",
    "RECORD_MANUAL_EXTERNAL_CAPTURE": "/aegis-opportunities",
    "MARK_CANDIDATE_REVIEWED": "/aegis-opportunities",
    "OPEN_EVIDENCE": "/aegis-opportunities",
    "REFRESH_MARKET_DATA": "/aegis-opportunities",
    "RERUN_CANDIDATE_DIAGNOSTICS": "/aegis-opportunities",
    "RERUN_PREFLIGHT": "/aegis-opportunities",
    "MARK_BLOCKER_REVIEWED": "/aegis-opportunities",
    "MARK_PROVIDER_DATA_NEEDED": "/aegis-opportunities",
    "OPEN_HUMAN_REVIEW_DOSSIER": "/aegis-edge-lab",
    "RECORD_PAPER_OBSERVATION": "/aegis-edge-lab",
}


def operator_event_log_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / EVENT_FAMILY / day_utc / "operator_events.v1.jsonl"


def operator_command_registry_v1() -> Dict[str, Any]:
    return {
        "schema_version": "operator_command_registry.v1",
        "allowed_commands": sorted(ALLOWED_COMMANDS),
        "forbidden_commands": sorted(FORBIDDEN_COMMANDS),
        "command_labels": {command: COMMAND_LABELS[command] for command in sorted(ALLOWED_COMMANDS)},
        "workflow_routes": {command: WORKFLOW_ROUTES[command] for command in sorted(ALLOWED_COMMANDS)},
        "execution_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "order_routing_allowed": False,
        "capital_allocation_allowed": False,
        "automatic_approval_allowed": False,
        "automatic_promotion_allowed": False,
    }


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _candidate_id(row: Mapping[str, Any]) -> str:
    return str(row.get("candidate_id") or row.get("id") or "").strip()


def _all_candidates(cockpit: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in _safe_list(cockpit.get("top_candidates")):
        if isinstance(row, dict):
            rows.append(row)
    candidates = cockpit.get("candidate_decisions_corrections")
    if isinstance(candidates, dict):
        for bucket in candidates.values():
            for row in _safe_list(bucket):
                if isinstance(row, dict):
                    rows.append(row)
    seen: set[str] = set()
    unique: List[Dict[str, Any]] = []
    for row in rows:
        key = _candidate_id(row) or _stable_hash(row)
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return sorted(unique, key=lambda row: (str(row.get("rank") or "999999"), str(row.get("symbol") or ""), _candidate_id(row)))


def classify_projection_ownership(item: Mapping[str, Any]) -> str:
    item_type = str(item.get("task_type") or item.get("diagnostic_type") or item.get("event_type") or item.get("health_type") or "").strip()
    owner_type = str(item.get("owner_type") or "").strip().lower()
    if item_type in OPERATOR_TASK_TYPES and owner_type == "operator":
        return OWNERSHIP_OPERATOR_ACTIONABLE
    if item_type in INFORMATIONAL_TYPES:
        return OWNERSHIP_INFORMATIONAL
    if item_type in PASSIVE_HEALTH_TYPES:
        return OWNERSHIP_PASSIVE_HEALTH
    if item_type in SYSTEM_DIAGNOSTIC_TYPES or owner_type in {"system", "infrastructure"}:
        return OWNERSHIP_SYSTEM_DIAGNOSTIC
    if item.get("primary_command") and item.get("resolution_workflow"):
        return OWNERSHIP_OPERATOR_ACTIONABLE if owner_type == "operator" else OWNERSHIP_SYSTEM_DIAGNOSTIC
    return OWNERSHIP_INFORMATIONAL


def _invalid_task_reclassification_event(task: Mapping[str, Any], reason: str, new_projection_type: str) -> Dict[str, Any]:
    return {
        "event_type": "INVALID_OPERATOR_TASK_RECLASSIFIED",
        "rejected_task_type": task.get("task_type"),
        "reason": reason,
        "original_projection_source": task.get("created_from_source_hash") or task.get("source_projection_fingerprint") or "",
        "new_projection_type": new_projection_type,
        "timestamp": _now(),
        "projection_fingerprint": _stable_hash(task),
    }


def _accept_operator_task(task: Mapping[str, Any]) -> tuple[bool, str]:
    if task.get("owner_type") != "operator":
        return False, "OWNER_NOT_OPERATOR"
    if classify_projection_ownership(task) != OWNERSHIP_OPERATOR_ACTIONABLE:
        return False, "OWNERSHIP_NOT_OPERATOR_ACTIONABLE"
    primary = str(task.get("primary_command") or "")
    if not primary:
        return False, "PRIMARY_COMMAND_MISSING"
    if primary not in ALLOWED_COMMANDS:
        return False, "PRIMARY_COMMAND_NOT_ALLOWED"
    if not task.get("resolution_workflow"):
        return False, "RESOLUTION_WORKFLOW_MISSING"
    if str(task.get("direct_action") or "") in GENERIC_ACTION_LABELS:
        return False, "GENERIC_ACTION_LABEL"
    if task.get("workflow_route") not in set(WORKFLOW_ROUTES.values()):
        return False, "WORKFLOW_ROUTE_MISSING"
    if task.get("ui_resolvable") is not True:
        return False, "NOT_UI_RESOLVABLE"
    return True, "ACCEPTED"


def _find_candidate(cockpit: Mapping[str, Any], candidate_id: str) -> Optional[Dict[str, Any]]:
    wanted = str(candidate_id or "").strip()
    for row in _all_candidates(cockpit):
        if _candidate_id(row) == wanted:
            return row
    return None


def candidate_decision_projection_v1(candidate: Mapping[str, Any], cockpit: Mapping[str, Any]) -> Dict[str, Any]:
    brief = candidate.get("decision_support_brief") if isinstance(candidate.get("decision_support_brief"), dict) else None
    if brief is None:
        brief = build_candidate_decision_support_brief_v1(candidate, cockpit)
    allowed = ["REVIEW_CANDIDATE", "WATCHLIST_CANDIDATE", "DISMISS_CANDIDATE", "REQUEST_MORE_EVIDENCE", "OPEN_EVIDENCE"]
    if brief.get("trust_classification") in {"supported", "partially_supported", "weakly_supported"} and brief.get("decision_guidance") not in {"not_recommended_due_to_missing_evidence", "blocked_do_not_capture"}:
        allowed.append("RECORD_MANUAL_EXTERNAL_CAPTURE")
    projection = {
        "schema_version": "candidate_decision_projection.v1",
        "candidate_id": brief["candidate_id"],
        "direct_answer_to_manual_capture": brief["direct_answer"],
        "trust_classification": brief["trust_classification"],
        "guidance": brief["decision_guidance"],
        "why_triggered": brief["why_triggered"],
        "evidence_for": brief["historical_support"],
        "evidence_against": brief["weakening_factors"],
        "missing_evidence_with_reason": brief["missing_evidence"],
        "sleeve_context": brief["sleeve_health_context"],
        "data_context": brief.get("runtime_context", {}),
        "review_window": brief["expected_holding_window"],
        "invalidation_conditions": brief["invalidation_conditions"],
        "allowed_commands": allowed,
        "command_warnings": _command_warnings_for_brief(brief),
        "source_artifact_ids": brief["source_artifacts"],
    }
    projection["projection_fingerprint"] = _stable_hash(projection)
    return projection


def _candidate_review_state_allows_opportunity(candidate: Mapping[str, Any]) -> bool:
    state = str(candidate.get("review_state") or candidate.get("operator_review_status") or candidate.get("candidate_status") or "review_required").strip().lower()
    if not state:
        return True
    allowed = {"active", "review_required", "watchlisted", "watchlist", "generated", "pending_review", "needs_review"}
    blocked = {"expired", "blocked", "dismissed", "rejected", "ignored", "closed"}
    if state in blocked:
        return False
    return state in allowed or state not in blocked


def candidate_is_manual_capture_eligible_v1(candidate: Mapping[str, Any], decision: Mapping[str, Any]) -> bool:
    trust = str(decision.get("trust_classification") or "").strip()
    guidance = str(decision.get("guidance") or "").strip()
    if trust not in {"supported", "partially_supported", "weakly_supported"}:
        return False
    if guidance in {"not_recommended_due_to_missing_evidence", "blocked_do_not_capture"}:
        return False
    if str(candidate.get("expired") or "").lower() == "true" or str(candidate.get("is_expired") or "").lower() == "true":
        return False
    if str(candidate.get("blocked") or "").lower() == "true" or trust == "blocked":
        return False
    return _candidate_review_state_allows_opportunity(candidate)


def _research_observation_reason(decision: Mapping[str, Any]) -> str:
    if decision.get("trust_classification") == "unsupported":
        return "Supporting evidence incomplete"
    if decision.get("guidance") == "not_recommended_due_to_missing_evidence":
        return "Manual capture not recommended because evidence is incomplete"
    if decision.get("trust_classification") == "blocked":
        return "Candidate blocked by governance or data requirement"
    return "Candidate is not eligible for manual capture"


def _observation_candidate(candidate: Mapping[str, Any], decision: Mapping[str, Any]) -> Dict[str, Any]:
    allowed = ["REQUEST_MORE_EVIDENCE", "WATCHLIST_CANDIDATE", "DISMISS_CANDIDATE", "OPEN_EVIDENCE"]
    return {
        **candidate,
        "candidate_decision_projection": {**decision, "allowed_commands": allowed},
        "manual_capture_eligible": False,
        "research_observation_reason": _research_observation_reason(decision),
        "missing_evidence_count": len(_safe_list(decision.get("missing_evidence_with_reason"))),
        "next_action": "Request More Evidence",
        "secondary_actions": ["Watchlist", "Dismiss", "Open Evidence"],
    }


def _command_warnings_for_brief(brief: Mapping[str, Any]) -> List[str]:
    warnings: List[str] = []
    if brief.get("trust_classification") in {"unsupported", "blocked"}:
        warnings.append("Manual capture is not recommended because supporting evidence is incomplete.")
    if brief.get("trust_classification") == "blocked":
        warnings.append("Blocked candidates cannot be captured through the advisory command gateway.")
    return warnings


def active_opportunity_projection_v1(cockpit: Mapping[str, Any]) -> Dict[str, Any]:
    candidates = []
    observations = []
    for row in _all_candidates(cockpit):
        decision = candidate_decision_projection_v1(row, cockpit)
        enriched = {**row, "candidate_decision_projection": decision, "decision_support_brief": row.get("decision_support_brief") or build_candidate_decision_support_brief_v1(row, cockpit)}
        if candidate_is_manual_capture_eligible_v1(enriched, decision):
            candidates.append({**enriched, "manual_capture_eligible": True})
        else:
            observations.append(_observation_candidate(enriched, decision))
    operator_snapshot = _operator_state_snapshot(cockpit)
    manual = operator_snapshot.get("manual_capture_candidate") if isinstance(operator_snapshot.get("manual_capture_candidate"), Mapping) else {}
    watchlist = operator_snapshot.get("suppressed_candidate_watchlist") if isinstance(operator_snapshot.get("suppressed_candidate_watchlist"), Mapping) else {}
    selected_count = 1 if manual.get("candidate_available") else 0
    suppressed_count = int(watchlist.get("suppressed_count") or 0)
    blocked_conversion_count = 1 if manual.get("blocker_code") else 0
    manual_capture_ticket_count = 1 if (
        selected_count
        and manual.get("manual_capture_ready") is True
        and str(manual.get("current_state") or manual.get("lifecycle_state") or "").upper() == "CAPTURE_READY"
    ) else 0
    projection = {
        "schema_version": "active_opportunity_projection.v1",
        "projection_version": PROJECTION_VERSION,
        "candidates": candidates,
        "research_observations": observations,
        "needs_evidence_candidates": observations,
        "manual_capture_candidate": dict(manual),
        "selected_exposure": dict(manual),
        "suppressed_candidate_watchlist": dict(watchlist),
        "candidate_count": len(candidates),
        "research_observation_count": len(observations),
        "selected_candidate_count": selected_count,
        "suppressed_count": suppressed_count,
        "blocked_conversion_count": blocked_conversion_count,
        "manual_capture_ticket_count": manual_capture_ticket_count,
        "has_selected_or_suppressed": bool(selected_count or suppressed_count),
        "source_artifact_hash": _stable_hash({
            "top_candidates": cockpit.get("top_candidates"),
            "source_paths": cockpit.get("source_paths"),
            "operator_state_snapshot": operator_snapshot.get("snapshot_id") or operator_snapshot.get("canonical_json_hash"),
        }),
    }
    projection["projection_fingerprint"] = _stable_hash(projection)
    return projection


def operator_task_projection_v1(cockpit: Mapping[str, Any]) -> Dict[str, Any]:
    tasks: List[Dict[str, Any]] = []
    reclassified: List[Dict[str, Any]] = []
    for candidate in _all_candidates(cockpit):
        decision = candidate_decision_projection_v1(candidate, cockpit)
        primary = _primary_command_for_decision(decision)
        task_type = "request_more_evidence" if primary == "REQUEST_MORE_EVIDENCE" else "candidate_review"
        task = {
            "task_id": f"task:{decision['candidate_id']}:{_stable_hash({'candidate_id': decision['candidate_id'], 'primary': primary})[:12]}",
            "task_type": task_type,
            "owner_type": "operator",
            "priority": "high" if decision["trust_classification"] in {"unsupported", "blocked"} else "medium",
            "title": f"Review {candidate.get('symbol') or 'candidate'} advisory candidate",
            "plain_english_problem": decision["direct_answer_to_manual_capture"],
            "why_it_matters": "The operator must decide whether to watchlist, request more evidence, dismiss, or record an external manual capture.",
            "current_status": decision["trust_classification"],
            "primary_command": primary,
            "direct_action": COMMAND_LABELS[primary],
            "resolution_workflow": "Candidate Decision Drawer",
            "workflow_route": WORKFLOW_ROUTES[primary],
            "related_command_ids": [primary, *[cmd for cmd in decision["allowed_commands"] if cmd != primary]],
            "secondary_commands": [cmd for cmd in decision["allowed_commands"] if cmd != primary],
            "related_candidate_id": decision["candidate_id"],
            "related_sleeve_id": candidate.get("sleeve_id") or candidate.get("sleeve"),
            "related_artifact_ids": decision["source_artifact_ids"],
            "evidence_state": "incomplete" if decision["missing_evidence_with_reason"] else "complete",
            "resolution_state": "open",
            "created_from_source_hash": decision["projection_fingerprint"],
            "source_projection_fingerprint": decision["projection_fingerprint"],
            "projection_version": PROJECTION_VERSION,
            "ui_resolvable": True,
            "ownership_classification": OWNERSHIP_OPERATOR_ACTIONABLE,
        }
        accepted, reason = _accept_operator_task(task)
        if accepted:
            tasks.append(task)
        else:
            reclassified.append(_invalid_task_reclassification_event(task, reason, "system_diagnostic"))
    tasks = sorted(tasks, key=lambda row: ({"high": 0, "medium": 1, "low": 2}.get(str(row["priority"]), 9), str(row["task_id"])))
    projection = {
        "schema_version": "operator_task_projection.v1",
        "projection_version": PROJECTION_VERSION,
        "tasks": tasks,
        "no_action_required": not tasks,
        "no_action_message": "No operator-owned actionable workflow is open.",
        "reclassified_invalid_tasks": reclassified,
        "audit_events": reclassified,
        "source_artifact_hash": _stable_hash({"tasks": tasks}),
    }
    projection["projection_fingerprint"] = _stable_hash(projection)
    return projection


def _opportunities(cockpit: Mapping[str, Any]) -> Mapping[str, Any]:
    value = cockpit.get("opportunities")
    return value if isinstance(value, dict) else {}


def _runtime(cockpit: Mapping[str, Any]) -> Mapping[str, Any]:
    value = cockpit.get("runtime")
    return value if isinstance(value, dict) else {}


def _source_paths(cockpit: Mapping[str, Any]) -> Mapping[str, Any]:
    value = cockpit.get("source_paths")
    return value if isinstance(value, dict) else {}


def _operator_state_snapshot(cockpit: Mapping[str, Any]) -> Mapping[str, Any]:
    value = cockpit.get("operator_state_snapshot")
    if isinstance(value, Mapping):
        data = value.get("data")
        return data if isinstance(data, Mapping) else value
    return {}


def build_operator_health_facts(cockpit: Mapping[str, Any]) -> Dict[str, Any]:
    runtime = _runtime(cockpit)
    opportunities = _opportunities(cockpit)
    operator_snapshot = _operator_state_snapshot(cockpit)
    market_data = opportunities.get("market_data_summary") if isinstance(opportunities.get("market_data_summary"), dict) else {}
    data_remediation = opportunities.get("data_remediation") if isinstance(opportunities.get("data_remediation"), dict) else market_data.get("data_remediation") if isinstance(market_data.get("data_remediation"), dict) else {}
    source_paths = _source_paths(cockpit)
    active = active_opportunity_projection_v1(cockpit)
    snapshot_manual = operator_snapshot.get("manual_capture_candidate") if isinstance(operator_snapshot.get("manual_capture_candidate"), Mapping) else {}
    snapshot_watchlist = operator_snapshot.get("suppressed_candidate_watchlist") if isinstance(operator_snapshot.get("suppressed_candidate_watchlist"), Mapping) else {}
    snapshot_blockers = operator_snapshot.get("blockers") if isinstance(operator_snapshot.get("blockers"), list) else []
    snapshot_selected_count = 1 if snapshot_manual.get("candidate_available") else 0
    snapshot_suppressed_count = int(snapshot_watchlist.get("suppressed_count") or 0)
    snapshot_blocked_conversion_count = 1 if snapshot_manual.get("blocker_code") else 0
    blocked_sleeves = [
        row for row in _safe_list(opportunities.get("sleeve_run_summary"))
        if isinstance(row, dict) and str(row.get("run_status") or "").upper() == "BLOCKED"
    ]
    missing_symbols = sorted({str(symbol).upper() for symbol in _safe_list(market_data.get("missing_symbols"))})
    stale_symbols = sorted({str(symbol).upper() for symbol in _safe_list(market_data.get("stale_symbols"))})
    if snapshot_manual.get("stale_market_data") and snapshot_manual.get("symbol"):
        stale_symbols = sorted(set(stale_symbols) | {str(snapshot_manual.get("symbol")).upper()})
    runtime_class = str(runtime.get("runtime_truth_classification") or "").upper()
    runtime_source_missing = "runtime_truth" in source_paths and not source_paths.get("runtime_truth")
    runtime_unavailable = runtime_class in {"MISSING", "BLOCKED", "UNAVAILABLE"} or runtime_source_missing
    runtime_partial = runtime_class == "PARTIAL_CONTEXT"
    if runtime_unavailable:
        runtime_truth_status = "unavailable"
    elif runtime_partial:
        runtime_truth_status = "partial"
    else:
        runtime_truth_status = "available"
    if active["candidate_count"] > 0:
        advisory_status = "Ready"
        advisory_reason = ""
        advisory_requires_diagnostic = False
    elif active["research_observation_count"] > 0:
        advisory_status = "Partial"
        advisory_reason = "Generated candidates exist, but supporting evidence is incomplete or capture guidance is not recommended."
        advisory_requires_diagnostic = True
    elif snapshot_selected_count > 0 or snapshot_suppressed_count > 0:
        advisory_status = "Partial"
        advisory_reason = "Portfolio arbitration selected an exposure or suppressed alternatives, but conversion or supporting projection context is blocked."
        advisory_requires_diagnostic = True
    elif runtime_unavailable or runtime_partial or missing_symbols or stale_symbols or blocked_sleeves:
        advisory_status = "Missing"
        advisory_reason = "Advisory projection is incomplete because runtime, data, or sleeve inputs are unresolved."
        advisory_requires_diagnostic = True
    else:
        advisory_status = "None"
        advisory_reason = "No advisory candidates are present and no evidence issue was detected."
        advisory_requires_diagnostic = False
    facts = {
        "blocked_sleeves": blocked_sleeves,
        "blocked_sleeves_count": len(blocked_sleeves),
        "missing_symbols": missing_symbols,
        "stale_symbols": stale_symbols,
        "data_warnings": [{"symbol": symbol, "status": "missing"} for symbol in missing_symbols] + [{"symbol": symbol, "status": "stale"} for symbol in stale_symbols],
        "data_warnings_count": len(missing_symbols) + len(stale_symbols),
        "data_warning_count": len(missing_symbols) + len(stale_symbols),
        "candidate_count": active["candidate_count"],
        "research_observation_count": active["research_observation_count"],
        "operator_eligible_count": active["candidate_count"],
        "selected_candidate_count": snapshot_selected_count,
        "suppressed_candidate_count": snapshot_suppressed_count,
        "blocked_conversion_count": snapshot_blocked_conversion_count,
        "manual_capture_ticket_count": active.get("manual_capture_ticket_count", 0),
        "runtime_truth_status": runtime_truth_status,
        "runtime_truth_classification": runtime_class or "UNKNOWN",
        "runtime_truth_unavailable": runtime_unavailable,
        "runtime_truth_partial": runtime_partial,
        "advisory_status": advisory_status,
        "advisory_reason": advisory_reason,
        "advisory_requires_diagnostic": advisory_requires_diagnostic,
        "candidate_counts": {
            "open_opportunities": active["candidate_count"],
            "research_observations": active["research_observation_count"],
            "selected_exposures": snapshot_selected_count,
            "suppressed_watchlist": snapshot_suppressed_count,
            "generated": active["candidate_count"] + active["research_observation_count"] + snapshot_selected_count + snapshot_suppressed_count,
        },
        "operator_state_blockers": snapshot_blockers,
        "missing_artifacts": sorted(k for k, value in source_paths.items() if not value and str(k).strip()),
        "stale_symbols": stale_symbols,
        "readiness_status": opportunities.get("noon_preflight", {}).get("readiness_status") if isinstance(opportunities.get("noon_preflight"), dict) else runtime.get("highest_readiness_layer"),
        "data_remediation": data_remediation,
        "remediation_attempts": data_remediation.get("attempts") if isinstance(data_remediation.get("attempts"), list) else [],
        "remediation_status": _remediation_overall_status(data_remediation),
        "latest_run_timestamp": str(data_remediation.get("generated_at_utc") or cockpit.get("generated_at_utc") or ""),
        "source_fingerprint": _stable_hash({"runtime": runtime, "opportunities": opportunities, "source_paths": source_paths, "active": active, "data_remediation": data_remediation}),
    }
    return facts


def _remediation_overall_status(remediation: Mapping[str, Any]) -> str:
    if not isinstance(remediation, Mapping) or not remediation:
        return "not_attempted"
    attempts = [row for row in _safe_list(remediation.get("attempts")) if isinstance(row, Mapping)]
    events = [row for row in _safe_list(remediation.get("events")) if isinstance(row, Mapping)]
    if any(row.get("healed") is True for row in attempts) or any(row.get("event_type") == "REMEDIATION_HEALED_BLOCKER" for row in events):
        return "healed"
    if any(row.get("status") == "REMEDIATION_STILL_BLOCKED" for row in attempts) or any(row.get("event_type") == "REMEDIATION_STILL_BLOCKED" for row in events):
        return "still_blocked"
    if attempts or events:
        return "attempted"
    return "not_attempted"


def _remediation_actions_for(commands: Iterable[str]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    for command in commands:
        if command in ALLOWED_COMMANDS:
            actions.append({"command_id": command, "label": COMMAND_LABELS[command], "workflow_route": WORKFLOW_ROUTES[command]})
    return actions



def _remediation_summary_for_symbol(remediation: Mapping[str, Any], symbol: str) -> Dict[str, Any]:
    symbol = str(symbol or "").upper()
    attempts = [
        row for row in _safe_list(remediation.get("attempts"))
        if isinstance(row, dict) and any(
            isinstance(blocker, dict) and str(blocker.get("blocker_id") or "") == str(row.get("blocker_id") or "") and str(blocker.get("affected_symbol") or "").upper() == symbol
            for blocker in _safe_list(remediation.get("blockers"))
        )
    ]
    events = [
        row for row in _safe_list(remediation.get("events"))
        if isinstance(row, dict) and any(
            isinstance(blocker, dict) and str(blocker.get("blocker_id") or "") == str(row.get("blocker_id") or "") and str(blocker.get("affected_symbol") or "").upper() == symbol
            for blocker in _safe_list(remediation.get("blockers"))
        )
    ]
    providers = sorted({str(row.get("provider") or "") for row in events if str(row.get("provider") or "")})
    validations = [str(row.get("validation_result") or "") for row in events if row.get("event_type") in {"DATA_VALIDATION_PASSED", "DATA_VALIDATION_FAILED"}]
    healed = any(row.get("event_type") == "REMEDIATION_HEALED_BLOCKER" for row in events) or any(row.get("healed") is True for row in attempts)
    still_blocked = any(row.get("event_type") == "REMEDIATION_STILL_BLOCKED" for row in events) or any(row.get("status") == "REMEDIATION_STILL_BLOCKED" for row in attempts)
    playbook = next((str(row.get("playbook_id") or "") for row in events if row.get("playbook_id")), "")
    if healed:
        message = "Resolved automatically using approved data remediation."
    elif events or attempts:
        message = "Still blocked after approved remediation. No fake or stale data was accepted."
    else:
        message = "Approved deterministic remediation has not run yet."
    return {
        "remediation_attempted": bool(events or attempts),
        "playbook_used": playbook,
        "providers_tried": providers,
        "validation_results": validations,
        "healed": healed,
        "still_blocked": still_blocked,
        "message": message,
    }

def _diagnostic_row(
    *,
    diagnostic_id: str,
    diagnostic_type: str,
    severity: str,
    title: str,
    affected_area: str,
    why_it_matters: str,
    operator_impact: str,
    source_facts: Mapping[str, Any],
    allowed_commands: Iterable[str] = (),
    extra: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    commands = [command for command in allowed_commands if command in ALLOWED_COMMANDS]
    row: Dict[str, Any] = {
        "diagnostic_id": diagnostic_id,
        "severity": severity,
        "diagnostic_type": diagnostic_type,
        "title": title,
        "issue": title,
        "affected_area": affected_area,
        "affected_system": affected_area,
        "why_it_matters": why_it_matters,
        "operator_impact": operator_impact,
        "remediation_status": "available" if commands else "system_managed",
        "allowed_commands": commands,
        "remediation_actions": _remediation_actions_for(commands),
        "operator_actionable": bool(commands),
        "remediation_available": bool(commands),
        "system_managed_message": "" if commands else "System-managed issue; no operator remediation available.",
        "source_facts": dict(source_facts),
        "owner_type": "system",
        "projection_version": PROJECTION_VERSION,
    }
    if extra:
        row.update(dict(extra))
    ownership = classify_projection_ownership(row)
    if ownership != OWNERSHIP_SYSTEM_DIAGNOSTIC:
        raise AssertionError(f"diagnostic misclassified as {ownership}: {row.get('diagnostic_id')}")
    row["ownership_classification"] = ownership
    row["projection_fingerprint"] = _stable_hash(row)
    row["source_projection_fingerprint"] = row["projection_fingerprint"]
    return row


def system_diagnostic_projection_v1(cockpit: Mapping[str, Any], health_facts: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    facts = dict(health_facts) if isinstance(health_facts, Mapping) else build_operator_health_facts(cockpit)
    diagnostics: List[Dict[str, Any]] = []

    if facts["runtime_truth_unavailable"] or facts["runtime_truth_partial"]:
        diagnostic_type = "runtime_truth_unavailable" if facts["runtime_truth_unavailable"] else "runtime_truth_partial"
        diagnostics.append({
            **_diagnostic_row(
                diagnostic_id="diag:runtime_truth",
                diagnostic_type=diagnostic_type,
                severity="critical" if facts["runtime_truth_unavailable"] else "warning",
                title="Runtime truth unavailable" if facts["runtime_truth_unavailable"] else "Runtime truth partial",
                affected_area="runtime_truth",
                why_it_matters="Advisory projections may be incomplete when runtime truth is unavailable or partial.",
                operator_impact="Candidate confidence may be degraded; this is not a candidate decision task.",
                allowed_commands=["RERUN_PREFLIGHT", "RERUN_CANDIDATE_DIAGNOSTICS"],
                source_facts={"runtime_truth_status": facts["runtime_truth_status"], "runtime_truth_classification": facts["runtime_truth_classification"]},
            )
        })

    if facts.get("runtime_truth_status") == "partial" and not any(row.get("diagnostic_type") == "runtime_truth_unavailable" for row in diagnostics):
        diagnostics.append(_diagnostic_row(
            diagnostic_id="diag:runtime_truth_partial_unavailable_invariant",
            diagnostic_type="runtime_truth_unavailable",
            severity="warning",
            title="Runtime truth partial",
            affected_area="runtime_truth",
            why_it_matters="Partial runtime truth can make projections incomplete and must be visible as a system diagnostic.",
            operator_impact="Projection confidence may be degraded until runtime truth is refreshed.",
            allowed_commands=["RERUN_PREFLIGHT", "RERUN_CANDIDATE_DIAGNOSTICS"],
            source_facts={"runtime_truth_status": facts.get("runtime_truth_status"), "runtime_truth_classification": facts.get("runtime_truth_classification")},
        ))

    blocked_by_symbol: Dict[str, List[str]] = {}
    for sleeve in facts["blocked_sleeves"]:
        for symbol in _safe_list(sleeve.get("blocking_inputs")):
            blocked_by_symbol.setdefault(str(symbol).upper(), []).append(str(sleeve.get("sleeve_id") or sleeve.get("sleeve") or "unknown"))
    for warning in facts["data_warnings"]:
        symbol = warning["symbol"]
        status = warning["status"]
        remediation_summary = _remediation_summary_for_symbol(facts.get("data_remediation", {}), symbol)
        diagnostics.append(_diagnostic_row(
            diagnostic_id=f"diag:market_data:{symbol}",
            diagnostic_type="missing_market_data" if status == "missing" else "stale_market_data",
            severity="critical" if symbol == "VIX" and blocked_by_symbol.get(symbol) else "warning",
            title=f"{symbol} market data {status}",
            affected_area="market_data",
            why_it_matters=f"{symbol} is required by one or more sleeve input checks; missing or stale data can block advisory readiness.",
            operator_impact="Affected sleeves stay blocked or partial until real provider/cache data is available.",
            allowed_commands=["REFRESH_MARKET_DATA", "MARK_PROVIDER_DATA_NEEDED"],
            source_facts={
                "required_canonical_symbol": symbol,
                "status": status,
                "affected_sleeves": blocked_by_symbol.get(symbol, []),
                "provider_attempts": warning.get("provider_attempts", []),
            },
            extra={
                "required_canonical_symbol": symbol,
                "symbol_status": status,
                "affected_sleeves": blocked_by_symbol.get(symbol, []),
                "provider_attempts": warning.get("provider_attempts", []),
                "remediation_summary": remediation_summary,
                "remediation_attempted": remediation_summary["remediation_attempted"],
                "remediation_playbook_id": remediation_summary["playbook_used"],
                "remediation_providers_tried": remediation_summary["providers_tried"],
                "remediation_validation_results": remediation_summary["validation_results"],
                "remediation_healed": remediation_summary["healed"],
                "remediation_still_blocked": remediation_summary["still_blocked"],
                "remediation_message": remediation_summary["message"],
            },
        ))

    for row in facts["blocked_sleeves"]:
        blocking_inputs = _safe_list(row.get("blocking_inputs"))
        commands = ["REFRESH_MARKET_DATA", "MARK_PROVIDER_DATA_NEEDED"] if blocking_inputs else []
        sleeve_id = str(row.get("sleeve_id") or row.get("sleeve") or "unknown")
        diagnostics.append(_diagnostic_row(
            diagnostic_id=f"diag:blocked_sleeve:{sleeve_id}",
            diagnostic_type="blocked_sleeve",
            severity="warning",
            title=f"Blocked sleeve: {sleeve_id}",
            affected_area="sleeve_input_validation",
            why_it_matters="A blocked sleeve cannot contribute candidates until its real input requirements are met.",
            operator_impact=f"Missing or blocked inputs: {', '.join(map(str, blocking_inputs)) if blocking_inputs else row.get('canonical_blocker') or 'not specified'}.",
            allowed_commands=commands,
            source_facts={"sleeve_id": sleeve_id, "blocking_inputs": blocking_inputs, "canonical_blocker": row.get("canonical_blocker")},
            extra={
                "affected_sleeve": sleeve_id,
                "required_canonical_symbols": [str(symbol).upper() for symbol in blocking_inputs],
            },
        ))

    if facts.get("remediation_status") == "still_blocked":
        diagnostics.append(_diagnostic_row(
            diagnostic_id="diag:data_remediation:still_blocked",
            diagnostic_type="data_validation_failed",
            severity="warning",
            title="Approved data remediation still blocked",
            affected_area="data_remediation",
            why_it_matters="Aegis attempted approved deterministic remediation but did not accept fake, stale, or invalid data.",
            operator_impact="Affected sleeves remain blocked until a current approved data source validates.",
            allowed_commands=["REFRESH_MARKET_DATA", "MARK_PROVIDER_DATA_NEEDED"],
            source_facts={"remediation_status": facts.get("remediation_status"), "remediation_attempts": facts.get("remediation_attempts", [])},
            extra={"remediation_status": facts.get("remediation_status"), "remediation_attempted": True, "remediation_still_blocked": True, "remediation_message": "Still blocked after approved remediation. No fake or stale data was accepted."},
        ))

    if facts["advisory_requires_diagnostic"]:
        diagnostics.append(_diagnostic_row(
            diagnostic_id="diag:advisory_projection",
            diagnostic_type="advisory_projection_incomplete",
            severity="warning",
            title="Advisory projection incomplete",
            affected_area="advisory_projection",
            why_it_matters="The advisory view cannot classify all generated candidates as manual-capture eligible without complete supporting evidence.",
            operator_impact="Open Opportunities may be empty while Research Observations need evidence.",
            allowed_commands=["RERUN_CANDIDATE_DIAGNOSTICS"],
            source_facts={"advisory_status": facts["advisory_status"], "advisory_reason": facts["advisory_reason"], "candidate_counts": facts["candidate_counts"]},
        ))

    for key in facts["missing_artifacts"]:
        diagnostics.append(_diagnostic_row(
            diagnostic_id=f"diag:missing_source:{key}",
            diagnostic_type="missing_source_artifact",
            severity="warning",
            title=f"Missing source artifact: {key}",
            affected_area="projection_inputs",
            why_it_matters="Projection quality is lower when source artifacts are unavailable.",
            operator_impact="The affected advisory section may be incomplete.",
            source_facts={"missing_artifact": key},
        ))

    passive = {"schema_version": "passive_health_projection.v1", "health": _passive_health_rows_from_facts(facts)}
    consistency = validate_operator_projection_consistency({}, passive, {"diagnostics": diagnostics})
    for issue in consistency:
        diagnostics.append(_diagnostic_row(
            diagnostic_id=f"diag:projection_inconsistency:{issue['rule']}",
            diagnostic_type="projection_inconsistency",
            severity="critical",
            title="Internal projection inconsistency",
            affected_area="operator_projection",
            why_it_matters="Aegis must not show unresolved health warnings while reporting no diagnostics.",
            operator_impact="Projection output may be incomplete until diagnostics are rebuilt.",
            allowed_commands=["RERUN_PREFLIGHT", "RERUN_CANDIDATE_DIAGNOSTICS"],
            source_facts=issue,
            extra={"internal_event_type": "INTERNAL_PROJECTION_INCONSISTENCY"},
        ))
    diagnostics = sorted(diagnostics, key=lambda row: ({"critical": 0, "warning": 1, "info": 2}.get(str(row.get("severity")), 9), str(row.get("diagnostic_id"))))
    projection = {
        "schema_version": "system_diagnostic_projection.v1",
        "projection_version": PROJECTION_VERSION,
        "diagnostics": diagnostics,
        "critical_count": len([row for row in diagnostics if row.get("severity") == "critical"]),
        "warning_count": len([row for row in diagnostics if row.get("severity") == "warning"]),
    }
    projection["projection_fingerprint"] = _stable_hash(projection)
    return projection


def what_changed_projection_v1(cockpit: Mapping[str, Any], health_facts: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    events: List[Dict[str, Any]] = []
    opportunities = _opportunities(cockpit)
    for candidate in _all_candidates(cockpit):
        candidate_id = _candidate_id(candidate)
        events.append({
            "event_id": f"change:new_candidate:{candidate_id}",
            "event_type": "new_candidate_generated",
            "owner_type": "system",
            "title": f"New candidate generated: {candidate.get('symbol') or 'UNKNOWN'}",
            "plain_english_summary": "Aegis generated an advisory candidate. Candidate review, if needed, appears separately in My Tasks.",
            "related_candidate_id": candidate_id,
            "related_sleeve_id": candidate.get("sleeve_id") or candidate.get("sleeve"),
            "action_required": False,
            "projection_version": PROJECTION_VERSION,
        })
    for row in _safe_list(opportunities.get("sleeve_run_summary")):
        if isinstance(row, dict) and str(row.get("run_status") or "").upper() == "BLOCKED":
            sleeve_id = row.get("sleeve_id") or row.get("sleeve") or "unknown"
            events.append({
                "event_id": f"change:blocked_sleeve:{sleeve_id}",
                "event_type": "paper_trial_updated" if "paper" in str(sleeve_id).lower() else "sleeve_drift_worsened",
                "owner_type": "system",
                "title": f"Sleeve blocked: {sleeve_id}",
                "plain_english_summary": "A sleeve was blocked by input requirements. The system diagnostic panel explains impact and remediation availability.",
                "related_sleeve_id": sleeve_id,
                "action_required": False,
                "projection_version": PROJECTION_VERSION,
            })
    for row in events:
        ownership = classify_projection_ownership(row)
        if ownership != OWNERSHIP_INFORMATIONAL:
            raise AssertionError(f"change event misclassified as {ownership}: {row.get('event_id')}")
        row["ownership_classification"] = ownership
        row["source_projection_fingerprint"] = _stable_hash(row)
    events = sorted(events, key=lambda row: str(row.get("event_id")))
    projection = {
        "schema_version": "what_changed_projection.v1",
        "projection_version": PROJECTION_VERSION,
        "events": events,
        "event_count": len(events),
    }
    projection["projection_fingerprint"] = _stable_hash(projection)
    return projection


def _passive_health_rows_from_facts(facts: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows = [
        {"health_id": "health:readiness", "health_type": "readiness", "label": "Readiness", "value": facts.get("readiness_status"), "action_required": False, "requires_diagnostic": False},
        {"health_id": "health:advisory", "health_type": "advisory", "label": "Advisory", "value": facts.get("advisory_status"), "action_required": False, "requires_diagnostic": bool(facts.get("advisory_requires_diagnostic"))},
        {"health_id": "health:runtime_truth", "health_type": "readiness", "label": "Runtime truth", "value": facts.get("runtime_truth_status"), "action_required": False, "requires_diagnostic": facts.get("runtime_truth_status") in {"unavailable", "partial"}},
        {"health_id": "health:stale_data", "health_type": "stale_data", "label": "Stale/missing data", "value": facts.get("data_warnings_count", 0), "action_required": False, "requires_diagnostic": int(facts.get("data_warnings_count") or 0) > 0},
        {"health_id": "health:blocked_sleeves", "health_type": "blocked_sleeves", "label": "Blocked sleeves", "value": facts.get("blocked_sleeves_count", 0), "action_required": False, "requires_diagnostic": int(facts.get("blocked_sleeves_count") or 0) > 0},
    ]
    for row in rows:
        row["owner_type"] = "system"
        ownership = classify_projection_ownership(row)
        if ownership != OWNERSHIP_PASSIVE_HEALTH:
            raise AssertionError(f"health row misclassified as {ownership}: {row.get('health_id')}")
        row["ownership_classification"] = ownership
        row["source_projection_fingerprint"] = _stable_hash(row)
        row["projection_version"] = PROJECTION_VERSION
    return rows


def passive_health_projection_v1(cockpit: Mapping[str, Any], health_facts: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    facts = dict(health_facts) if isinstance(health_facts, Mapping) else build_operator_health_facts(cockpit)
    rows = _passive_health_rows_from_facts(facts)
    projection = {
        "schema_version": "passive_health_projection.v1",
        "projection_version": PROJECTION_VERSION,
        "health": rows,
        "source_facts": facts,
    }
    projection["projection_fingerprint"] = _stable_hash(projection)
    return projection


def _health_value(passive_health: Mapping[str, Any], health_id: str) -> Any:
    for row in _safe_list(passive_health.get("health")):
        if isinstance(row, dict) and row.get("health_id") == health_id:
            return row.get("value")
    return None


def _health_requires_diagnostic(passive_health: Mapping[str, Any], health_id: str) -> bool:
    for row in _safe_list(passive_health.get("health")):
        if isinstance(row, dict) and row.get("health_id") == health_id:
            return bool(row.get("requires_diagnostic"))
    return False


def validate_operator_projection_consistency(today: Mapping[str, Any], passive_health: Mapping[str, Any], diagnostics: Mapping[str, Any]) -> List[Dict[str, Any]]:
    diag_rows = [row for row in _safe_list(diagnostics.get("diagnostics")) if isinstance(row, dict)]
    diag_types = {str(row.get("diagnostic_type")) for row in diag_rows}
    issues: List[Dict[str, Any]] = []
    blocked_count = int(today.get("blocked_sleeves_count") or _health_value(passive_health, "health:blocked_sleeves") or 0)
    data_warnings_count = int(today.get("data_warnings_count") or _health_value(passive_health, "health:stale_data") or 0)
    runtime_status = str(today.get("runtime_truth_status") or _health_value(passive_health, "health:runtime_truth") or "").lower()
    advisory_status = str(today.get("advisory_status") or _health_value(passive_health, "health:advisory") or "").lower()
    if blocked_count > 0 and "blocked_sleeve" not in diag_types:
        issues.append({"rule": "blocked_sleeves_require_diagnostic", "blocked_sleeves_count": blocked_count})
    if data_warnings_count > 0 and not ({"stale_market_data", "missing_market_data"} & diag_types):
        issues.append({"rule": "data_warnings_require_diagnostic", "data_warnings_count": data_warnings_count})
    if runtime_status == "unavailable" and "runtime_truth_unavailable" not in diag_types:
        issues.append({"rule": "runtime_truth_unavailable_requires_diagnostic", "runtime_truth_status": runtime_status})
    if runtime_status == "partial" and "runtime_truth_partial" not in diag_types:
        issues.append({"rule": "runtime_truth_partial_requires_diagnostic", "runtime_truth_status": runtime_status})
    if advisory_status in {"missing", "partial"} and _health_requires_diagnostic(passive_health, "health:advisory") and "advisory_projection_incomplete" not in diag_types:
        issues.append({"rule": "advisory_incomplete_requires_diagnostic", "advisory_status": advisory_status})
    if not diag_rows and any([
        blocked_count > 0,
        data_warnings_count > 0,
        runtime_status in {"unavailable", "partial"},
        advisory_status in {"missing", "partial"} and _health_requires_diagnostic(passive_health, "health:advisory"),
    ]):
        issues.append({"rule": "no_false_no_diagnostics_state"})
    return issues


def _primary_command_for_decision(decision: Mapping[str, Any]) -> str:
    if decision.get("trust_classification") == "blocked":
        return "DISMISS_CANDIDATE"
    if decision.get("trust_classification") == "unsupported":
        return "REQUEST_MORE_EVIDENCE"
    return "REVIEW_CANDIDATE"


def operator_today_projection_v1(cockpit: Mapping[str, Any], health_facts: Optional[Mapping[str, Any]] = None, tasks_projection: Optional[Mapping[str, Any]] = None, active_projection: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    tasks = dict(tasks_projection) if isinstance(tasks_projection, Mapping) else operator_task_projection_v1(cockpit)
    active = dict(active_projection) if isinstance(active_projection, Mapping) else active_opportunity_projection_v1(cockpit)
    facts = dict(health_facts) if isinstance(health_facts, Mapping) else build_operator_health_facts(cockpit)
    projection = {
        "schema_version": "operator_today_projection.v1",
        "projection_version": PROJECTION_VERSION,
        "candidate_count": active["candidate_count"],
        "sleeve_candidate_count": active["candidate_count"],
        "manual_capture_ticket_count": active.get("manual_capture_ticket_count", facts.get("manual_capture_ticket_count", 0)),
        "selected_candidate_count": facts.get("selected_candidate_count", 0),
        "suppressed_candidate_count": facts.get("suppressed_candidate_count", 0),
        "suppressed_count": facts.get("suppressed_candidate_count", 0),
        "blocked_conversion_count": facts.get("blocked_conversion_count", 0),
        "open_task_count": len([task for task in tasks["tasks"] if task["resolution_state"] == "open"]),
        "readiness": facts["readiness_status"],
        "blocked_sleeves_count": facts["blocked_sleeves_count"],
        "data_warnings_count": facts["data_warnings_count"],
        "runtime_truth_status": facts["runtime_truth_status"],
        "advisory_status": facts["advisory_status"],
        "tasks_projection_fingerprint": tasks["projection_fingerprint"],
        "active_opportunity_projection_fingerprint": active["projection_fingerprint"],
    }
    projection["projection_fingerprint"] = _stable_hash(projection)
    return projection


def review_ledger_projection_v1(cockpit: Mapping[str, Any]) -> Dict[str, Any]:
    candidates = cockpit.get("candidate_decisions_corrections") if isinstance(cockpit.get("candidate_decisions_corrections"), dict) else {}
    rows = []
    for bucket, bucket_rows in candidates.items():
        for row in _safe_list(bucket_rows):
            if isinstance(row, dict):
                rows.append({"bucket": bucket, "candidate_id": _candidate_id(row), "symbol": row.get("symbol"), "review_state": row.get("review_state") or row.get("operator_review_status")})
    projection = {
        "schema_version": "review_ledger_projection.v1",
        "projection_version": PROJECTION_VERSION,
        "rows": sorted(rows, key=lambda row: (str(row.get("bucket")), str(row.get("candidate_id")))),
    }
    projection["projection_fingerprint"] = _stable_hash(projection)
    return projection


def _attach_snapshot_identity(projection: Dict[str, Any], *, snapshot_id: str, source_fingerprint: str) -> Dict[str, Any]:
    projection = dict(projection)
    projection["snapshot_id"] = snapshot_id
    projection["source_fingerprint"] = source_fingerprint
    return projection


def _projection_inconsistency_diagnostic(*, failed_invariants: List[Dict[str, Any]], source_fingerprint: str) -> Dict[str, Any]:
    source_facts = {
        "failed_invariant_names": [row.get("invariant") for row in failed_invariants],
        "failed_invariants": failed_invariants,
        "source_fingerprint": source_fingerprint,
        "affected_projection_names": sorted({name for row in failed_invariants for name in _safe_list(row.get("affected_projection_names"))}),
    }
    return _diagnostic_row(
        diagnostic_id="diag:projection_inconsistency:operator_state_snapshot",
        diagnostic_type="projection_inconsistency",
        severity="critical",
        title="Operator projection inconsistency",
        affected_area="operator_state_snapshot",
        why_it_matters="Aegis must not show unresolved health state while reporting no system diagnostics.",
        operator_impact="Refresh is required; the inconsistent snapshot is fail-closed and diagnostics are shown instead of a false clear state.",
        allowed_commands=["RERUN_PREFLIGHT", "RERUN_CANDIDATE_DIAGNOSTICS"],
        source_facts=source_facts,
        extra={"failed_invariants": failed_invariants, "affected_projection_names": source_facts["affected_projection_names"]},
    )


def validate_operator_state_snapshot_v1(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    facts = snapshot.get("health_facts") if isinstance(snapshot.get("health_facts"), Mapping) else {}
    diagnostics_projection = snapshot.get("system_diagnostics_projection") if isinstance(snapshot.get("system_diagnostics_projection"), Mapping) else {}
    diagnostics = [row for row in _safe_list(diagnostics_projection.get("diagnostics")) if isinstance(row, dict)]
    diag_types = {str(row.get("diagnostic_type") or "") for row in diagnostics}
    failed: List[Dict[str, Any]] = []

    def fail(name: str, affected: List[str]) -> None:
        failed.append({"invariant": name, "affected_projection_names": affected, "source_fingerprint": snapshot.get("source_fingerprint")})

    blocked_count = int(facts.get("blocked_sleeve_count") or facts.get("blocked_sleeves_count") or 0)
    data_count = int(facts.get("data_warning_count") or facts.get("data_warnings_count") or 0)
    runtime_status = str(facts.get("runtime_truth_status") or "").lower()
    advisory_status = str(facts.get("advisory_status") or "").lower()
    candidate_count = int(facts.get("candidate_count") or 0)
    research_count = int(facts.get("research_observation_count") or 0)
    missing_artifacts = _safe_list(facts.get("missing_artifacts"))
    remediation_status = str(facts.get("remediation_status") or "").lower()

    if blocked_count > 0 and "blocked_sleeve" not in diag_types:
        fail("blocked_sleeve_requires_diagnostic", ["operator_today_projection", "passive_health_projection", "system_diagnostics_projection"])
    if data_count > 0 and not ({"stale_market_data", "missing_market_data", "data_validation_failed", "provider_unavailable"} & diag_types):
        fail("data_warning_requires_diagnostic", ["operator_today_projection", "passive_health_projection", "system_diagnostics_projection"])
    if runtime_status in {"unavailable", "partial"} and "runtime_truth_unavailable" not in diag_types:
        fail("runtime_truth_requires_diagnostic", ["operator_today_projection", "passive_health_projection", "system_diagnostics_projection"])
    if advisory_status in {"missing", "partial"} and (candidate_count > 0 or research_count > 0) and "advisory_projection_incomplete" not in diag_types:
        fail("advisory_incomplete_requires_diagnostic", ["active_opportunity_projection", "system_diagnostics_projection"])
    if remediation_status == "still_blocked" and not ({"data_validation_failed", "remediation_outcome"} & diag_types):
        fail("remediation_still_blocked_requires_diagnostic", ["system_diagnostics_projection"])
    if not diagnostics and (blocked_count > 0 or data_count > 0 or runtime_status in {"unavailable", "partial"} or missing_artifacts or remediation_status == "still_blocked"):
        fail("empty_diagnostics_requires_clean_health", ["operator_today_projection", "passive_health_projection", "system_diagnostics_projection"])
    return {"status": "PASS" if not failed else "FAIL", "failed_invariants": failed, "source_fingerprint": snapshot.get("source_fingerprint"), "diagnostic_count": len(diagnostics)}


def build_operator_state_snapshot_v1(cockpit: Mapping[str, Any], *, source_run_id: str = "", eod_outcome_projection: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    source_fingerprint = _stable_hash({
        "runtime": cockpit.get("runtime"),
        "opportunities": cockpit.get("opportunities"),
        "top_candidates": cockpit.get("top_candidates"),
        "candidate_decisions_corrections": cockpit.get("candidate_decisions_corrections"),
        "source_paths": cockpit.get("source_paths"),
    })
    snapshot_id = f"operator-state-snapshot:{source_fingerprint[:24]}"
    active = active_opportunity_projection_v1(cockpit)
    tasks = operator_task_projection_v1(cockpit)
    health_facts = build_operator_health_facts(cockpit)
    today = operator_today_projection_v1(cockpit, health_facts=health_facts, tasks_projection=tasks, active_projection=active)
    passive = passive_health_projection_v1(cockpit, health_facts=health_facts)
    diagnostics = system_diagnostic_projection_v1(cockpit, health_facts=health_facts)
    changed = what_changed_projection_v1(cockpit, health_facts=health_facts)
    review = review_ledger_projection_v1(cockpit)
    projections = {
        "operator_today_projection": today,
        "passive_health_projection": passive,
        "operator_tasks_projection": tasks,
        "operator_task_projection": tasks,
        "system_diagnostics_projection": diagnostics,
        "system_diagnostic_projection": diagnostics,
        "what_changed_projection": changed,
        "opportunities_projection": active,
        "active_opportunity_projection": active,
        "review_ledger_projection": review,
        "eod_outcome_projection": dict(eod_outcome_projection or {}),
    }
    projections = {key: _attach_snapshot_identity(value, snapshot_id=snapshot_id, source_fingerprint=source_fingerprint) if isinstance(value, dict) else value for key, value in projections.items()}
    snapshot: Dict[str, Any] = {
        "schema_id": "aegis_operator_state_snapshot",
        "schema_version": "v1",
        "snapshot_id": snapshot_id,
        "generated_at": _now(),
        "source_run_id": source_run_id,
        "source_fingerprint": source_fingerprint,
        "health_facts": health_facts,
        "runtime": cockpit.get("runtime") if isinstance(cockpit.get("runtime"), Mapping) else {},
        "opportunities": cockpit.get("opportunities") if isinstance(cockpit.get("opportunities"), Mapping) else {},
        "top_candidates": cockpit.get("top_candidates") if isinstance(cockpit.get("top_candidates"), list) else [],
        "candidate_decisions_corrections": cockpit.get("candidate_decisions_corrections") if isinstance(cockpit.get("candidate_decisions_corrections"), Mapping) else {},
        "source_paths": cockpit.get("source_paths") if isinstance(cockpit.get("source_paths"), Mapping) else {},
        **projections,
        "validation_result": {"status": "PENDING", "failed_invariants": []},
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "order_routing_allowed": False,
    }
    validation = validate_operator_state_snapshot_v1(snapshot)
    if validation["status"] != "PASS":
        inconsistency = _projection_inconsistency_diagnostic(failed_invariants=validation["failed_invariants"], source_fingerprint=source_fingerprint)
        diag_rows = list(snapshot["system_diagnostics_projection"].get("diagnostics") or [])
        if not any(row.get("diagnostic_type") == "projection_inconsistency" for row in diag_rows):
            diag_rows.append(inconsistency)
        diag_projection = dict(snapshot["system_diagnostics_projection"])
        diag_projection["diagnostics"] = sorted(diag_rows, key=lambda row: ({"critical": 0, "warning": 1, "info": 2}.get(str(row.get("severity")), 9), str(row.get("diagnostic_id"))))
        diag_projection["critical_count"] = len([row for row in diag_projection["diagnostics"] if row.get("severity") == "critical"])
        diag_projection["warning_count"] = len([row for row in diag_projection["diagnostics"] if row.get("severity") == "warning"])
        diag_projection["projection_fingerprint"] = _stable_hash(diag_projection)
        diag_projection = _attach_snapshot_identity(diag_projection, snapshot_id=snapshot_id, source_fingerprint=source_fingerprint)
        snapshot["system_diagnostics_projection"] = diag_projection
        snapshot["system_diagnostic_projection"] = diag_projection
        validation = validate_operator_state_snapshot_v1(snapshot)
    snapshot["validation_result"] = validation
    return snapshot


def build_operator_projections_v1(cockpit: Mapping[str, Any]) -> Dict[str, Any]:
    snapshot = build_operator_state_snapshot_v1(cockpit)
    return {
        "operator_state_snapshot": snapshot,
        "operator_today_projection": snapshot["operator_today_projection"],
        "operator_task_projection": snapshot["operator_task_projection"],
        "system_diagnostic_projection": snapshot["system_diagnostic_projection"],
        "what_changed_projection": snapshot["what_changed_projection"],
        "passive_health_projection": snapshot["passive_health_projection"],
        "active_opportunity_projection": snapshot["active_opportunity_projection"],
        "review_ledger_projection": snapshot["review_ledger_projection"],
    }


def _event_type_for_command(command_type: str) -> str:
    return COMMAND_EVENT_TYPES.get(command_type, "OPERATOR_COMMAND_RECORDED")


def _append_operator_event(*, truth_root: Path, day_utc: str, event: Dict[str, Any]) -> Dict[str, Any]:
    path = operator_event_log_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    event = dict(event)
    event["event_id"] = event.get("event_id") or f"operator-event:{_stable_hash(event)[:24]}"
    event["content_hash"] = _stable_hash(event)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return {**event, "path": str(path)}


def read_operator_events_v1(*, truth_root: Path, day_utc: str) -> List[Dict[str, Any]]:
    path = operator_event_log_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def execute_operator_command_v1(
    *,
    truth_root: Path,
    repo_root: Path,
    day_utc: str,
    cockpit_payload: Mapping[str, Any],
    request_payload: Mapping[str, Any],
) -> Dict[str, Any]:
    command_type = str(request_payload.get("command_type") or "").strip().upper()
    target_id = str(request_payload.get("target_id") or "").strip()
    idempotency_key = str(request_payload.get("idempotency_key") or "").strip() or _stable_hash(request_payload)[:24]
    actor = str(request_payload.get("actor") or request_payload.get("operator") or "operator-ui").strip()
    payload = request_payload.get("payload") if isinstance(request_payload.get("payload"), dict) else {}
    source_fingerprint = str(request_payload.get("source_projection_fingerprint") or "").strip()
    command_id = f"operator-command:{command_type}:{target_id}:{idempotency_key}"

    if command_type in FORBIDDEN_COMMANDS or command_type not in ALLOWED_COMMANDS:
        return _reject_command(truth_root=truth_root, day_utc=day_utc, command_id=command_id, command_type=command_type, target_id=target_id, actor=actor, reason="COMMAND_NOT_ALLOWED")
    candidate = _find_candidate(cockpit_payload, target_id) if target_id else None
    decision_projection = candidate_decision_projection_v1(candidate, cockpit_payload) if candidate else None
    if command_type.endswith("_CANDIDATE") or command_type == "RECORD_MANUAL_EXTERNAL_CAPTURE":
        if not candidate:
            return _reject_command(truth_root=truth_root, day_utc=day_utc, command_id=command_id, command_type=command_type, target_id=target_id, actor=actor, reason="TARGET_CANDIDATE_NOT_FOUND")
    if command_type in REQUIRES_FRESH_FINGERPRINT and decision_projection and source_fingerprint != decision_projection["projection_fingerprint"]:
        return _reject_command(truth_root=truth_root, day_utc=day_utc, command_id=command_id, command_type=command_type, target_id=target_id, actor=actor, reason="STALE_PROJECTION_FINGERPRINT")
    if command_type == "RECORD_MANUAL_EXTERNAL_CAPTURE" and decision_projection:
        if decision_projection["trust_classification"] == "blocked":
            return _reject_command(truth_root=truth_root, day_utc=day_utc, command_id=command_id, command_type=command_type, target_id=target_id, actor=actor, reason="TARGET_BLOCKED")
        if decision_projection["trust_classification"] == "unsupported" and not payload.get("unsupported_evidence_acknowledgement"):
            return _reject_command(truth_root=truth_root, day_utc=day_utc, command_id=command_id, command_type=command_type, target_id=target_id, actor=actor, reason="ACKNOWLEDGEMENT_REQUIRED")

    pre_state_hash = _stable_hash({"candidate": candidate, "cockpit_status": cockpit_payload.get("status")})
    downstream: Dict[str, Any] = {}
    if command_type in {"REVIEW_CANDIDATE", "MARK_CANDIDATE_REVIEWED", "WATCHLIST_CANDIDATE", "DISMISS_CANDIDATE", "REQUEST_MORE_EVIDENCE"}:
        action = {
            "REVIEW_CANDIDATE": "add-note",
            "MARK_CANDIDATE_REVIEWED": "add-note",
            "WATCHLIST_CANDIDATE": "watchlist",
            "DISMISS_CANDIDATE": "dismiss",
            "REQUEST_MORE_EVIDENCE": "needs-more-evidence",
        }[command_type]
        downstream["candidate_review_event"] = append_candidate_review_action_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            candidate_id=target_id,
            action=action,
            operator=actor,
            operator_note=str(request_payload.get("operator_note") or payload.get("operator_note") or payload.get("note") or ""),
            source="OPERATOR_COMMAND_GATEWAY",
        )
    elif command_type == "RECORD_MANUAL_EXTERNAL_CAPTURE":
        downstream["manual_capture_event"] = append_manual_external_capture_v1(
            truth_root=truth_root,
            day_utc=day_utc,
            candidate_id=target_id,
            manually_captured=payload.get("manually_captured", True),
            quantity=payload.get("quantity"),
            capture_timestamp=str(payload.get("capture_timestamp") or ""),
            external_execution_venue=str(payload.get("external_execution_venue") or ""),
            operator_notes=str(payload.get("operator_notes") or request_payload.get("operator_note") or ""),
            confidence_override=str(payload.get("confidence_override") or ""),
            paper_trade_only=payload.get("paper_trade_only", True),
            review_decision=str(payload.get("review_decision") or "MANUAL_CAPTURE_RECORDED"),
            operator=actor,
        )

    event = {
        "schema_version": "operator_event.v1",
        "event_type": _event_type_for_command(command_type),
        "command_id": command_id,
        "idempotency_key": idempotency_key,
        "actor": actor,
        "timestamp": _now(),
        "command_type": command_type,
        "target_id": target_id,
        "source_projection_fingerprint": source_fingerprint,
        "pre_state_hash": pre_state_hash,
        "post_state_hash": _stable_hash({"downstream": downstream, "command": command_type}),
        "result_event_id": "",
        "audit_note": str(request_payload.get("operator_note") or payload.get("operator_notes") or payload.get("note") or ""),
        "payload": payload,
        "downstream": downstream,
        "decision_support_snapshot": decision_projection,
        "manual_capture_snapshot": _manual_capture_snapshot(candidate, payload, decision_projection) if command_type == "RECORD_MANUAL_EXTERNAL_CAPTURE" else None,
        "operator_statement": "Aegis did not execute this trade." if command_type == "RECORD_MANUAL_EXTERNAL_CAPTURE" else "",
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
        "automatic_promotion_allowed": False,
    }
    event["result_event_id"] = f"result:{_stable_hash(event)[:20]}"
    appended = _append_operator_event(truth_root=truth_root, day_utc=day_utc, event=event)
    return {"ok": True, "command": {**event, "result_event_id": appended["event_id"]}, "event": appended}


def _manual_capture_snapshot(candidate: Optional[Mapping[str, Any]], payload: Mapping[str, Any], decision_projection: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    candidate = candidate or {}
    return {
        "candidate_id": _candidate_id(candidate),
        "symbol": candidate.get("symbol"),
        "direction": candidate.get("direction") or candidate.get("candidate_direction"),
        "quantity": payload.get("quantity"),
        "capture_timestamp": payload.get("capture_timestamp"),
        "external_execution_venue": payload.get("external_execution_venue", ""),
        "external_account": payload.get("external_account", ""),
        "operator_notes": payload.get("operator_notes", ""),
        "decision_support_fingerprint": decision_projection.get("projection_fingerprint") if isinstance(decision_projection, dict) else "",
        "acknowledgement_text_accepted": "I understand Aegis evidence is incomplete and Aegis did not execute this trade."
        if payload.get("unsupported_evidence_acknowledgement") else "",
        "operator_statement": "Aegis did not execute this trade.",
    }


def _reject_command(*, truth_root: Path, day_utc: str, command_id: str, command_type: str, target_id: str, actor: str, reason: str) -> Dict[str, Any]:
    event = {
        "schema_version": "operator_event.v1",
        "event_type": "OPERATOR_COMMAND_REJECTED",
        "command_id": command_id,
        "idempotency_key": command_id.rsplit(":", 1)[-1],
        "actor": actor,
        "timestamp": _now(),
        "command_type": command_type,
        "target_id": target_id,
        "rejection_reason": reason,
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
        "automatic_promotion_allowed": False,
    }
    appended = _append_operator_event(truth_root=truth_root, day_utc=day_utc, event=event)
    return {"ok": False, "rejected": True, "reason": reason, "event": appended}


def replay_operator_events_v1(events: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    ledger: Dict[str, Any] = {"candidate_states": {}, "event_count": 0, "rejected_count": 0}
    for event in events:
        ledger["event_count"] += 1
        if event.get("event_type") == "OPERATOR_COMMAND_REJECTED":
            ledger["rejected_count"] += 1
            continue
        target = str(event.get("target_id") or "")
        if target:
            ledger["candidate_states"][target] = {
                "last_event_type": event.get("event_type"),
                "last_command_type": event.get("command_type"),
                "last_event_id": event.get("event_id"),
            }
    ledger["content_hash"] = _stable_hash(ledger)
    return ledger
