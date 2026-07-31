from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


SCHEMA_ID = "attention_queue_projection"
SCHEMA_VERSION = "v1"

PRIORITY_ORDER = {
    "CRITICAL": 0,
    "ACTION_REQUIRED": 1,
    "REVIEW": 2,
    "WARNING": 3,
    "INFO": 4,
}

SAFETY_FLAGS = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
}


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _money(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "not available"
    sign = "-" if number < 0 else ""
    return f"{sign}${abs(number):,.2f}"


def _percent(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "not available"
    return f"{number:.2f}%"


def _artifact_refs(*sources: Mapping[str, Any]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for source in sources:
        for key, value in source.items():
            if not value:
                continue
            if isinstance(value, str):
                refs.append({"logical_name": str(key), "artifact_path": value, "content_hash": ""})
            elif isinstance(value, Mapping):
                path = str(value.get("artifact_path") or value.get("path") or "")
                content_hash = str(value.get("content_hash") or value.get("artifact_content_hash") or value.get("artifact_sha256") or "")
                refs.append({"logical_name": str(key), "artifact_path": path, "content_hash": content_hash})
    return refs


def _attention_item(
    *,
    priority: str,
    category: str,
    title: str,
    summary: str,
    recommended_action: str,
    target_workspace: str,
    target_route: str,
    linked_artifacts: list[dict[str, str]] | None = None,
    evidence_status: str = "AVAILABLE",
    created_at: str = "",
    expires_at: str = "",
    dismissed_status: str = "ACTIVE",
    key: str = "",
) -> dict[str, Any]:
    clean_priority = priority if priority in PRIORITY_ORDER else "INFO"
    identity = {
        "priority": clean_priority,
        "category": category,
        "title": title,
        "target_route": target_route,
        "key": key,
    }
    return {
        "attention_id": f"attention:{stable_hash_v1(identity)[:20]}",
        "priority": clean_priority,
        "category": category,
        "title": title,
        "summary": summary,
        "recommended_action": recommended_action,
        "target_workspace": target_workspace,
        "target_route": target_route,
        "linked_artifacts": linked_artifacts or [],
        "evidence_status": evidence_status,
        "created_at": created_at,
        "expires_at": expires_at,
        "dismissed_status": dismissed_status,
    }


def _capture_projection(payload: Mapping[str, Any]) -> dict[str, Any]:
    today = _safe_dict(payload.get("operator_today_projection"))
    runtime = _safe_dict(payload.get("runtime_truth_kernel") or payload.get("readiness_kernel") or payload.get("runtime_truth"))
    active = _safe_dict(payload.get("active_operator_candidate") or payload.get("trade_ticket_projection_v1"))
    manual = _safe_dict(payload.get("trade_ticket_projection_v1") or payload.get("trade_ticket_projection"))
    completed = bool(manual.get("captured_read_only") is True) or str(manual.get("lifecycle_state") or manual.get("current_state") or manual.get("state") or manual.get("historical_state") or "").upper() in {"CAPTURED_HISTORICAL", "CAPTURED_MANUALLY"}
    explicit_count = today.get("capture_ticket_count") or today.get("manual_capture_ticket_count") or runtime.get("capture_ticket_count") or active.get("manual_capture_ticket_count") or manual.get("capture_ticket_count")
    count = 0 if completed else _safe_int(explicit_count)
    return {
        "platform_capture_capability": str(today.get("platform_capture_capability") or runtime.get("platform_capture_capability") or ("READY" if runtime.get("manual_trade_capture_allowed") is True else "READY")),
        "capture_ticket_count": count,
        "capture_ticket_status": str(today.get("capture_ticket_status") or runtime.get("capture_ticket_status") or ("TICKET_READY" if count > 0 else "NONE_AVAILABLE")),
    }


def _candidate_funnel(payload: Mapping[str, Any]) -> dict[str, Any]:
    today = _safe_dict(payload.get("operator_today_projection"))
    current_day = _safe_dict(payload.get("current_day_status"))
    return _safe_dict(today.get("candidate_funnel_projection") or current_day.get("candidate_funnel_projection") or payload.get("candidate_funnel_projection"))


def _capture_items(payload: Mapping[str, Any], created_at: str) -> list[dict[str, Any]]:
    capture = _capture_projection(payload)
    count = _safe_int(capture.get("capture_ticket_count"))
    if count <= 0:
        return []
    return [
        _attention_item(
            priority="ACTION_REQUIRED",
            category="CAPTURE_TICKET",
            title=f"{count} IB capture ticket{'s' if count != 1 else ''} awaiting review",
            summary="Manual IB capture is available for an approved ticket. Broker automation remains disabled.",
            recommended_action="Review capture ticket",
            target_workspace="Captured Trades",
            target_route="/aegis-captured-trades",
            evidence_status="AVAILABLE",
            created_at=created_at,
            key="capture-ticket",
        )
    ]


def _exit_review_items(payload: Mapping[str, Any], created_at: str) -> list[dict[str, Any]]:
    projection = _safe_dict(payload.get("exit_review_projection_v1") or payload.get("exit_review_projection"))
    rows = _safe_list(projection.get("open_positions"))
    items: list[dict[str, Any]] = []
    for row_value in rows:
        row = _safe_dict(row_value)
        symbol = str(row.get("symbol") or "Position")
        decision = str(row.get("exit_decision") or "REVIEW")
        pnl = _money(row.get("unrealized_pnl") if row.get("unrealized_pnl") is not None else row.get("realized_pnl"))
        reason = str(row.get("decision_reason") or row.get("next_operator_guidance") or "Review current stop, target, and thesis state.")
        items.append(
            _attention_item(
                priority="REVIEW",
                category="EXIT_REVIEW",
                title=f"{symbol} exit review: {decision}, P&L {pnl}",
                summary=reason,
                recommended_action=str(row.get("next_operator_guidance") or "Open Exit Review"),
                target_workspace="Exit Review",
                target_route="/aegis-exit-review",
                linked_artifacts=_artifact_refs({"exit_review_projection_v1": {"content_hash": projection.get("content_hash") or ""}}),
                evidence_status="AVAILABLE" if not _safe_list(row.get("required_evidence")) else "PARTIAL",
                created_at=created_at,
                key=str(row.get("position_id") or row.get("trade_id") or symbol),
            )
        )
    return items


def _repair_items(payload: Mapping[str, Any], created_at: str) -> list[dict[str, Any]]:
    projection = _safe_dict(payload.get("repair_center_projection_v1") or payload.get("repair_center_projection"))
    items: list[dict[str, Any]] = []
    for value in _safe_list(projection.get("repair_items")):
        row = _safe_dict(value)
        status = str(row.get("status") or "")
        mode = str(row.get("repair_mode") or "")
        if status in {"COMPLETED", "SOURCE_CONFIGURED", "NOT_APPLICABLE_FOR_CURRENT_MODE"}:
            continue
        domain = str(row.get("domain_id") or row.get("target_id") or "Repair item")
        summary = ""
        affected = _safe_list(row.get("affected_sleeves") or row.get("affected_hypotheses"))
        affected_text = f" Affects {len(affected)} sleeve/hypothesis item{'s' if len(affected) != 1 else ''}." if affected else ""
        if "SOURCE" in status or "SOURCE" in mode or status == "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT":
            priority = "WARNING"
            title = f"{domain.replace('_', ' ').title()} source missing"
            recommended = "Open source setup requirements"
            summary = "External source required; not a system failure." + affected_text
        elif status in {"FAILED", "REPAIR_FAILED"}:
            priority = "CRITICAL"
            title = f"{domain.replace('_', ' ').title()} repair failed"
            recommended = "Open Repair Center"
        else:
            priority = "WARNING"
            title = str(row.get("title") or f"{domain.replace('_', ' ').title()} needs repair")
            recommended = str(row.get("primary_action_label") or "Open Repair Center")
        if not summary:
            summary = str(row.get("summary") or row.get("reason") or "Repair item requires attention.") + affected_text
        items.append(
            _attention_item(
                priority=priority,
                category="REPAIR_SOURCE",
                title=title,
                summary=summary,
                recommended_action=str(row.get("next_required_step") or recommended),
                target_workspace="Repair Center",
                target_route="/aegis-repair-center",
                linked_artifacts=_artifact_refs({"repair_center_projection_v1": {"content_hash": projection.get("content_hash") or ""}}),
                evidence_status="PARTIAL",
                created_at=created_at,
                key=str(row.get("repair_id") or domain),
            )
        )
    return items


def _candidate_certification_items(payload: Mapping[str, Any], created_at: str) -> list[dict[str, Any]]:
    funnel = _candidate_funnel(payload)
    queue = _safe_dict(funnel.get("dynamic_certification_queue"))
    requested = [str(symbol).upper() for symbol in _safe_list(queue.get("requested_symbols")) if str(symbol).strip()]
    results = [_safe_dict(row) for row in _safe_list(queue.get("certification_results"))]
    certified = [str(row.get("symbol") or "").upper() for row in results if str(row.get("status") or row.get("certification_status") or "").upper() in {"CERTIFIED", "SUCCEEDED", "SUCCESS"}]
    if not certified and str(queue.get("certification_status") or "").upper() in {"CERTIFIED", "PARTIAL"}:
        certified = requested
    if not requested and not certified:
        return []
    named = ", ".join((certified or requested)[:5])
    before_after = ""
    trend = _safe_list(funnel.get("trend_5d"))
    if len(trend) >= 2:
        before = _safe_int(_safe_dict(trend[-2]).get("excluded_uncovered_symbol_count"))
        after = _safe_int(_safe_dict(trend[-1]).get("excluded_uncovered_symbol_count"))
        reduction = max(0, before - after)
        if reduction:
            before_after = f"; uncovered exclusions dropped by {reduction}"
    return [
        _attention_item(
            priority="INFO",
            category="CANDIDATE_CERTIFICATION",
            title=f"Dynamic certification added {named}" if certified else f"Dynamic certification queued {named}",
            summary=f"Candidate Funnel updated dynamic certification for uncovered candidate pressure{before_after}.",
            recommended_action="Open Candidate Funnel",
            target_workspace="Candidate Funnel",
            target_route="/aegis-candidate-funnel",
            linked_artifacts=_artifact_refs({"dynamic_certification_queue_v1": {"artifact_path": queue.get("artifact_path") or "", "content_hash": queue.get("artifact_content_hash") or ""}}),
            evidence_status="AVAILABLE",
            created_at=created_at,
            key="dynamic-certification",
        )
    ]


def _system_items(payload: Mapping[str, Any], created_at: str) -> list[dict[str, Any]]:
    runtime = _safe_dict(payload.get("runtime_truth_kernel") or payload.get("runtime_truth") or {})
    provider_status = str(runtime.get("provider_status") or payload.get("provider_status") or "").upper()
    replay_status = str(runtime.get("replay_status") or payload.get("replay_status") or "").upper()
    items: list[dict[str, Any]] = []
    if provider_status and provider_status not in {"HEALTHY", "PASS", "OK"}:
        items.append(_attention_item(priority="WARNING", category="PROVIDER", title="Provider health needs review", summary=f"Provider status is {provider_status}.", recommended_action="Open Runtime Timeline", target_workspace="Runtime Timeline", target_route="/aegis-runtime-timeline", created_at=created_at, key="provider"))
    if replay_status and replay_status not in {"PASS", "REPLAY_PASS", "OK"}:
        items.append(_attention_item(priority="WARNING", category="REPLAY", title="Replay status needs review", summary=f"Replay status is {replay_status}.", recommended_action="Open Runtime Timeline", target_workspace="Runtime Timeline", target_route="/aegis-runtime-timeline", created_at=created_at, key="replay"))
    return items


def build_attention_queue_projection_v1(payload: Mapping[str, Any], *, day_utc: str | None = None) -> dict[str, Any]:
    today = _safe_dict(payload.get("operator_today_projection"))
    created_at = str(payload.get("generated_at_utc") or payload.get("generated_at") or today.get("generated_at_utc") or "")
    operational_day = str(day_utc or payload.get("displayed_artifact_day") or today.get("displayed_artifact_day") or today.get("current_runtime_day") or payload.get("day_utc") or "")
    performance = _safe_dict(payload.get("paper_trade_evaluation_projection_v1") or payload.get("paper_trade_evaluation_projection"))
    repair = _safe_dict(payload.get("repair_center_projection_v1") or payload.get("repair_center_projection"))
    capture = _capture_projection(payload)
    items = [
        *_capture_items(payload, created_at),
        *_exit_review_items(payload, created_at),
        *_repair_items(payload, created_at),
        *_candidate_certification_items(payload, created_at),
        *_system_items(payload, created_at),
    ]
    items = sorted(
        items,
        key=lambda row: (
            PRIORITY_ORDER.get(str(row.get("priority") or "INFO"), 99),
            str(row.get("category") or ""),
            str(row.get("title") or ""),
            str(row.get("attention_id") or ""),
        ),
    )
    priority_counts: dict[str, int] = {priority: 0 for priority in PRIORITY_ORDER}
    category_counts: dict[str, int] = {}
    for row in items:
        priority_counts[str(row.get("priority") or "INFO")] = priority_counts.get(str(row.get("priority") or "INFO"), 0) + 1
        category_counts[str(row.get("category") or "INFO")] = category_counts.get(str(row.get("category") or "INFO"), 0) + 1
    repair_summary = _safe_dict(repair.get("summary"))
    key_numbers = {
        "capture_tickets": _safe_int(capture.get("capture_ticket_count")),
        "open_trades": _safe_int(performance.get("open_trade_count")),
        "unrealized_pnl": _safe_float(performance.get("unrealized_pnl")) or 0.0,
        "active_blockers": _safe_int(repair_summary.get("total_open") or repair_summary.get("blocked_external_requirements")),
        "replay_provider_status": str(_safe_dict(repair.get("summary_banner")).get("title") or "Core system operational"),
    }
    recent_events = [
        {
            "title": str(row.get("title") or ""),
            "summary": str(row.get("summary") or ""),
            "route": str(row.get("target_route") or ""),
            "created_at": str(row.get("created_at") or ""),
        }
        for row in items[:5]
    ]
    projection = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": operational_day,
        "generated_at_utc": created_at,
        "system_health_summary": _safe_dict(repair.get("summary_banner")).get("message") or "Aegis is operational. Review the attention queue for current operator tasks.",
        "items": items,
        "active_count": len(items),
        "priority_counts": priority_counts,
        "category_counts": dict(sorted(category_counts.items())),
        "key_numbers": key_numbers,
        "platform_capture_capability": str(capture.get("platform_capture_capability") or "READY"),
        "capture_ticket_status": str(capture.get("capture_ticket_status") or "NONE_AVAILABLE"),
        "recent_important_events": recent_events,
        "empty_state_message": "No operator action required.",
        **SAFETY_FLAGS,
    }
    projection["content_hash"] = stable_hash_v1({**projection, "generated_at_utc": "", "content_hash": ""})
    return projection
