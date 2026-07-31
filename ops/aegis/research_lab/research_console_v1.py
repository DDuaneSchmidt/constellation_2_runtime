from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote
import re

from ops.aegis.research_lab.research_store_reader import DEFAULT_SLEEVE_ID, RESEARCH_LABEL, build_edge_lab_projection, default_research_store_root
from research_lab.audit.audit_log import write_audit_event
from research_lab.event_intake.event_intake_registry import (
    capture_event_observation,
    create_event_cluster,
    generate_hypothesis_proposal,
    generate_intent_candidate,
    seed_event_families,
)
from research_lab.research_intake.intake_registry import (
    assess_hypothesis_readiness,
    build_and_store_research_intake_dossier,
    convert_hypothesis_proposal_to_research_plan,
    latest_hypothesis_proposal_review,
    latest_readiness_assessment,
    latest_research_intake_dossier,
    review_hypothesis_proposal,
    score_hypothesis_proposal,
)
from research_lab.projections.hypothesis_queue_projection import projection_api_payload
from research_lab.projections.operator_queue_projection import (
    blocked_work_payload,
    evidence_payload,
    operator_home_projection_summary,
    paper_trials_payload,
    research_backlog_payload,
    research_plans_payload,
    sleeve_review_center_payload,
)
from research_lab.projections.projection_builder import rebuild_research_projections
from research_lab.projections.projection_health import projection_health
from research_lab.storage.hashing import content_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout
from ops.aegis.research_lab.research_data_readiness_v1 import default_truth_root_v1
from ops.aegis.research_lab.research_run_ledger_v1 import (
    read_research_run_ledger_v1,
    record_user_initiated_research_run_v1,
    research_run_ledger_path_v1,
    research_run_projection_for_hypothesis_v1,
)
from ops.aegis.operator_action_command_contracts_v1 import (
    command_for_hypothesis_status_v1,
    command_instance_v1,
)

SAFETY_LABELS = [
    "READ-ONLY GOVERNANCE",
    "NO BROKER EXECUTION",
    "NO LIVE TRADING",
    "RESEARCH ONLY",
]
HYPOTHETICAL_EVIDENCE_LABEL = "Backtests and model outputs are hypothetical research evidence, not achieved portfolio performance."

EVENT_FAMILY_LABELS = {
    "oil_shock": "Oil shock",
    "volatility_spike": "Volatility spike",
    "volatility_compression": "Volatility compression",
    "breadth_collapse": "Breadth collapse",
    "breadth_recovery": "Breadth recovery",
    "gap_event": "Gap event",
    "drawdown_recovery": "Drawdown recovery",
    "regime_transition": "Regime transition",
    "correlation_break": "Correlation break",
    "credit_stress": "Credit stress",
    "rate_shock": "Rate shock",
    "commodity_dislocation": "Commodity dislocation",
    "macro_headline_shock": "Macro headline shock",
    "lottery_event": "Rare dislocation",
}


def _store(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root or default_research_store_root())


def _latest_aegis_research_pipeline_v1() -> dict[str, Any]:
    root = default_truth_root_v1()
    base = root / "reports" / "aegis_research_pipeline_v1"
    if not base.exists():
        return {"ok": False, "items": [], "counts": {}, "source_path": ""}
    candidates = sorted(base.glob("*/research_pipeline.v1.json"))
    if not candidates:
        return {"ok": False, "items": [], "counts": {}, "source_path": ""}
    path = candidates[-1]
    try:
        payload = read_json(path)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "items": [], "counts": {}, "source_path": str(path), "error": str(exc)}
    items = payload.get("items") or []
    return {
        "ok": True,
        "source_path": str(path),
        "day_utc": payload.get("day_utc") or path.parent.name,
        "generated_at_utc": payload.get("generated_at_utc") or "",
        "counts": payload.get("counts") or {},
        "priority_counts": payload.get("priority_counts") or {},
        "recommended_focus_today": payload.get("recommended_focus_today") or [],
        "items": items,
        "count": len(items),
        "non_archived_count": sum(1 for item in items if str(item.get("operator_lifecycle_state") or "").upper() not in {"ARCHIVED", "REJECTED_ARCHIVED"}),
    }




def _research_lifecycle_from_aegis(item: dict[str, Any]) -> str:
    raw = str(item.get("operator_lifecycle_state") or item.get("gate_status") or item.get("current_gate") or "").upper()
    if "ARCHIVED" in raw or "REJECTED" in raw:
        return "Archived"
    if "VALIDATING" in raw or "RESULT_REVIEW" in raw:
        return "Validating"
    if "PAPER" in raw:
        return "Paper Trial"
    if "READY" in raw:
        return "Ready"
    if "RESEARCHING" in raw or "TESTING" in raw:
        return "Researching"
    if "BLOCK" in raw:
        return "Blocked"
    return "Ideas"


def _research_lifecycle_from_store(row: dict[str, Any]) -> str:
    raw = str(row.get("lifecycle_state") or row.get("lane") or row.get("proposal_status") or "").lower()
    if raw in {"archived", "rejected", "rejected_archived"}:
        return "Archived"
    if raw == "needs_data":
        return "Blocked"
    if raw == "accepted_for_research":
        return "Researching"
    if raw == "paper_trial":
        return "Paper Trial"
    if raw == "ready":
        return "Ready"
    if row.get("lifecycle_state"):
        return str(row["lifecycle_state"])
    return "Ideas"


def _normalized_search_text(row: dict[str, Any]) -> str:
    terms = [
        row.get("hypothesis_id"),
        row.get("title"),
        row.get("source_type"),
        row.get("event_type"),
        row.get("lifecycle_state"),
        row.get("tier"),
        row.get("rank"),
        row.get("data_status"),
        row.get("blocker_summary"),
        row.get("next_action"),
        *(row.get("symbols") or []),
    ]
    return " ".join(str(term) for term in terms if term).lower()


def _normalize_store_hypothesis(row: dict[str, Any]) -> dict[str, Any]:
    symbols = row.get("required_symbols") or row.get("missing_symbols") or []
    lifecycle = _research_lifecycle_from_store(row)
    normalized = {
        "hypothesis_id": str(row.get("hypothesis_proposal_id") or row.get("item_id") or ""),
        "title": str(row.get("title") or row.get("hypothesis_summary") or row.get("hypothesis_proposal_id") or "Research hypothesis"),
        "source_type": "Research Store",
        "symbols": [str(symbol) for symbol in symbols if symbol],
        "event_type": str(row.get("event_family") or row.get("event_family_id") or ""),
        "lifecycle_state": lifecycle,
        "tier": str(row.get("tier") or row.get("priority_bucket") or "Watchlist"),
        "rank": row.get("rank"),
        "data_status": str(row.get("operator_data_status") or row.get("data_requirement_status") or ""),
        "blocker_summary": str(row.get("blocker_reason") or ", ".join(row.get("blocking_items") or []) or ""),
        "next_action": str(row.get("recommended_next_action") or "Review hypothesis"),
        "operator_action_required": bool(row.get("operator_attention_required") or row.get("operator_action_required") or row.get("needs_operator_review")),
        "created_at": str(row.get("created_at") or ""),
        "updated_at": str(row.get("updated_at") or row.get("created_at") or ""),
    }
    normalized["search_text"] = _normalized_search_text(normalized)
    return normalized


def _aegis_latest_result_payload(item: dict[str, Any]) -> dict[str, Any]:
    result = item.get("latest_result")
    if isinstance(result, dict) and isinstance(result.get("latest_result"), dict) and not result.get("test_status"):
        result = result.get("latest_result")
    return result if isinstance(result, dict) else {}


def _normalize_aegis_hypothesis(item: dict[str, Any], *, generated_at: str = "") -> dict[str, Any]:
    lifecycle = _research_lifecycle_from_aegis(item)
    latest_result = _aegis_latest_result_payload(item)
    latest_result_status = str(
        item.get("latest_result_status")
        or latest_result.get("test_status")
        or latest_result.get("latest_result")
        or ""
    )
    latest_result_summary = str(item.get("latest_result_summary") or latest_result.get("result_summary") or "")
    operator_blocker = item.get("operator_blocker") if isinstance(item.get("operator_blocker"), dict) else {}
    blocker = item.get("blocker_summary") or operator_blocker.get("title") or operator_blocker.get("summary") or item.get("blocker") or ""
    if lifecycle == "Validating" and latest_result_status and latest_result_status != "No result yet":
        blocker = ""
    next_action = item.get("next_action") or item.get("recommended_action") or operator_blocker.get("next_action") or "Review research hypothesis"
    symbols = item.get("required_symbols") or item.get("symbols") or ((item.get("spec") or {}).get("universe") if isinstance(item.get("spec"), dict) else []) or []
    current_data_status = item.get("current_data_status")
    if isinstance(current_data_status, dict):
        data_status = current_data_status.get("status") or item.get("data_status") or item.get("gate_status") or item.get("current_gate") or ""
    else:
        data_status = item.get("data_status") or item.get("gate_status") or item.get("current_gate") or ""
    if latest_result_status and latest_result_status != "No result yet":
        data_status = latest_result_status
    sample_size = latest_result.get("sample_size")
    minimum_sample_size = latest_result.get("minimum_sample_size")
    next_sample_expected_at = ""
    estimated_completion_date = ""
    expected_trading_days_remaining = None
    if latest_result_status == "INCONCLUSIVE_SAMPLE_SIZE" and sample_size is not None and minimum_sample_size is not None:
        missing_samples = max(0, int(minimum_sample_size or 0) - int(sample_size or 0))
        next_sample_expected_at = "next market close"
        estimated_completion_date = f"after {missing_samples} valid observations" if missing_samples else "complete after current observation set"
        expected_trading_days_remaining = missing_samples
        next_action = f"Collect forward-return observations; sample size {sample_size}/{minimum_sample_size}."
    normalized = {
        "hypothesis_id": str(item.get("hypothesis_id") or item.get("item_id") or ""),
        "title": str(item.get("title") or item.get("readable_title") or item.get("hypothesis_id") or "Research hypothesis"),
        "source_type": "Governed Research",
        "symbols": [str(symbol) for symbol in symbols if symbol],
        "event_type": str(item.get("event_family") or item.get("event_type") or ((item.get("spec") or {}).get("event_family") if isinstance(item.get("spec"), dict) else "") or ""),
        "lifecycle_state": lifecycle,
        "tier": str(item.get("tier") or item.get("priority_tier") or "Watchlist"),
        "rank": item.get("rank"),
        "data_status": str(data_status),
        "blocker_summary": str(blocker or ""),
        "next_action": str(next_action or "Review research hypothesis"),
        "operator_action_required": bool(item.get("operator_attention_required") or operator_blocker.get("operator_action_required") or item.get("current_gate") == "RESULT_REVIEW"),
        "created_at": str(item.get("created_at") or ""),
        "updated_at": str(item.get("updated_at") or item.get("generated_at_utc") or latest_result.get("generated_at_utc") or generated_at or ""),
        "sample_size": sample_size,
        "minimum_sample_size": minimum_sample_size,
        "next_sample_expected_at": next_sample_expected_at,
        "estimated_completion_date": estimated_completion_date,
        "expected_trading_days_remaining": expected_trading_days_remaining,
        "test_status": latest_result_status,
        "latest_result": latest_result_status,
        "latest_result_summary": latest_result_summary,
        "event_data_status": str(latest_result.get("event_data_status") or ""),
        "current_gate": str(item.get("current_gate") or ""),
        "current_status": str(item.get("gate_status") or ""),
    }
    normalized["search_text"] = _normalized_search_text(normalized)
    return normalized


HYPOTHESIS_VIEW_SECTION_ORDER = [
    "recommendations_ready",
    "collecting_evidence",
    "ready_to_start",
    "researching",
    "waiting",
    "blocked",
    "completed",
]
HYPOTHESIS_VIEW_SECTION_LABELS = {
    "recommendations_ready": "Recommendations Ready",
    "collecting_evidence": "Collecting Evidence",
    "ready_to_start": "Ready to Start",
    "researching": "Researching",
    "waiting": "Waiting",
    "blocked": "Blocked",
    "completed": "Completed",
}


def _safe_anchor_id(value: str) -> str:
    core = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(value or "hypothesis")).strip("-").lower()
    return f"hypothesis-card-{core or 'hypothesis'}"


def _hypothesis_section_id(row: dict[str, Any]) -> str:
    status = str(row.get("user_facing_status") or "").strip()
    if str(row.get("test_status") or row.get("latest_result") or "").upper() == "INCONCLUSIVE_SAMPLE_SIZE":
        return "collecting_evidence"
    if status == "Recommendation Ready":
        return "recommendations_ready"
    if status == "Researching":
        return "researching"
    if status == "Blocked":
        return "blocked"
    if status == "Complete":
        return "completed"
    if status in {"Scheduled", "Queued", "Waiting"}:
        return "waiting"
    return "ready_to_start"


def _hypothesis_tier_score(row: dict[str, Any]) -> int:
    raw = str(row.get("tier") or "").lower()
    if "tier_1" in raw or "critical" in raw or "high" in raw:
        return 5
    if "tier_2" in raw or "medium" in raw:
        return 4
    if "watchlist" in raw or "tier_4" in raw:
        return 3
    if "blocked" in raw:
        return 2
    if "low" in raw or "tier_3" in raw or "tier_5" in raw:
        return 1
    return 0


def _hypothesis_rank_value(row: dict[str, Any]) -> int:
    try:
        value = int(row.get("rank"))
    except (TypeError, ValueError):
        return 999_999
    return value if value > 0 else 999_999


def _hypothesis_view_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _hypothesis_rank_value(row),
        -_hypothesis_tier_score(row),
        -int(bool(row.get("operator_action_required"))),
        str(row.get("title") or row.get("hypothesis_id") or "").lower(),
        str(row.get("hypothesis_id") or ""),
    )


def _hypothesis_display_status(row: dict[str, Any], section_id: str) -> str:
    if section_id == "ready_to_start":
        return "Ready to Start"
    if section_id == "collecting_evidence":
        return "Collecting Evidence"
    return str(row.get("user_facing_status") or "Ready to Start")


def _hypothesis_primary_action_for_item(row: dict[str, Any], section_id: str) -> dict[str, str]:
    hypothesis_id = str(row.get("hypothesis_id") or "")
    detail_href = f"/research-lab/hypotheses?dossier={quote(hypothesis_id)}" if hypothesis_id else "/research-lab/hypotheses"
    title = str(row.get("title") or "Research hypothesis")
    user_status = str(row.get("user_facing_status") or "")
    if user_status in {"Queued", "Scheduled"}:
        return {"label": "View Queue", "href": "/research-lab/hypotheses"}
    if section_id == "ready_to_start":
        return {"label": "Start Research", "href": f"/research-lab/start?hypothesis_id={quote(hypothesis_id)}"}
    if section_id == "recommendations_ready":
        return {"label": "View Recommendation", "href": detail_href}
    if section_id == "collecting_evidence":
        return {"label": "View Progress", "href": detail_href}
    if section_id == "researching":
        return {"label": "View Progress", "href": detail_href}
    if section_id == "waiting":
        return {"label": "View Waiting Reason", "href": detail_href}
    if section_id == "blocked":
        return {"label": "View Blocker", "href": detail_href}
    if section_id == "completed":
        return {"label": "View Findings", "href": detail_href}
    return {"label": f"Start {title}", "href": f"/research-lab/start?hypothesis_id={quote(hypothesis_id)}"}


def _hypothesis_view_item(row: dict[str, Any], section_id: str) -> dict[str, Any]:
    hypothesis_id = str(row.get("hypothesis_id") or "")
    title = str(row.get("title") or "Research hypothesis")
    anchor_id = _safe_anchor_id(hypothesis_id or title)
    primary_action = _hypothesis_primary_action_for_item(row, section_id)
    return {
        "schema_id": "hypothesis_view_item.v1",
        "hypothesis_id": hypothesis_id,
        "title": title,
        "section_id": section_id,
        "section_label": HYPOTHESIS_VIEW_SECTION_LABELS.get(section_id, section_id),
        "card_anchor_id": anchor_id,
        "user_facing_status": _hypothesis_display_status(row, section_id),
        "raw_user_facing_status": str(row.get("user_facing_status") or ""),
        "user_facing_explanation": str(row.get("user_facing_explanation") or ""),
        "current_stage": str(row.get("current_stage") or ""),
        "symbols": [str(symbol) for symbol in row.get("symbols") or [] if symbol],
        "last_research_run": row.get("last_research_run") if isinstance(row.get("last_research_run"), dict) else {},
        "trigger_source": str(row.get("trigger_source") or ""),
        "started_by_user": bool(row.get("started_by_user")),
        "primary_action_label": primary_action["label"],
        "primary_action_href": primary_action["href"],
        "primary_action": primary_action,
        "primary_command": row.get("primary_command") if isinstance(row.get("primary_command"), dict) else {},
        "secondary_commands": row.get("secondary_commands") if isinstance(row.get("secondary_commands"), list) else [],
        "disabled_commands": row.get("disabled_commands") if isinstance(row.get("disabled_commands"), list) else [],
        "blocker_reason": str(row.get("blocker_summary") or row.get("blocker_reason") or ""),
        "sample_size": row.get("sample_size"),
        "minimum_sample_size": row.get("minimum_sample_size"),
        "required_samples": row.get("minimum_sample_size"),
        "current_samples": row.get("sample_size"),
        "missing_samples": max(0, int(row.get("minimum_sample_size") or 0) - int(row.get("sample_size") or 0)) if section_id == "collecting_evidence" else None,
        "next_sample_expected_at": str(row.get("next_sample_expected_at") or ("next market close" if section_id == "collecting_evidence" else "")),
        "estimated_completion_date": str(row.get("estimated_completion_date") or (f"after {max(0, int(row.get('minimum_sample_size') or 0) - int(row.get('sample_size') or 0))} valid observations" if section_id == "collecting_evidence" else "")),
        "expected_trading_days_remaining": row.get("expected_trading_days_remaining") if row.get("expected_trading_days_remaining") is not None else (max(0, int(row.get("minimum_sample_size") or 0) - int(row.get("sample_size") or 0)) if section_id == "collecting_evidence" else None),
        "test_status": str(row.get("test_status") or row.get("latest_result") or ""),
        "latest_result": str(row.get("latest_result") or row.get("test_status") or ""),
        "operator_action_required": False if section_id == "collecting_evidence" else bool(row.get("operator_action_required")),
        "autonomous_engine_enabled": bool(row.get("autonomous_engine_enabled", True)),
        "diagnostics_href": "/research-lab/blocked-work" if section_id == "blocked" else "/research-lab/hypotheses",
        "detail_href": f"/research-lab/hypotheses?dossier={quote(hypothesis_id)}" if hypothesis_id else "/research-lab/hypotheses",
        "recommendation_summary": "Collecting evidence before paper validation" if section_id == "collecting_evidence" else ("Recommendation Ready" if section_id == "recommendations_ready" else "No recommendation yet"),
        "confidence_summary": str(row.get("confidence_summary") or row.get("tier") or "Not enough evidence yet"),
        "updated_at": str(row.get("updated_at") or ""),
        "created_at": str(row.get("created_at") or ""),
        "operator_action_required": bool(row.get("operator_action_required")),
    }


def build_hypothesis_view_model_v1(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {section_id: [] for section_id in HYPOTHESIS_VIEW_SECTION_ORDER}
    for row in rows:
        section_id = _hypothesis_section_id(row)
        grouped.setdefault(section_id, []).append(row)
    sections: dict[str, dict[str, Any]] = {}
    for section_id in HYPOTHESIS_VIEW_SECTION_ORDER:
        sorted_rows = sorted(grouped.get(section_id, []), key=_hypothesis_view_sort_key)
        items = [_hypothesis_view_item(row, section_id) for row in sorted_rows]
        visible_items = items
        top_item = visible_items[0] if visible_items else None
        sections[section_id] = {
            "schema_id": "hypothesis_view_section.v1",
            "section_id": section_id,
            "label": HYPOTHESIS_VIEW_SECTION_LABELS.get(section_id, section_id),
            "count": len(visible_items),
            "top_item": top_item,
            "visible_items": visible_items,
            "overflow_count": 0,
            "collapsed_by_default": len(items) == 0,
        }
    ready_top = sections["ready_to_start"]["top_item"]
    primary_cta = None
    if ready_top:
        primary_cta = {
            "label": f"Start {ready_top['title']}",
            "href": ready_top["primary_action_href"],
            "target_anchor": f"#{ready_top['card_anchor_id']}",
            "hypothesis_id": ready_top["hypothesis_id"],
        }
    summary_cards = [
        {
            "section_id": section_id,
            "label": sections[section_id]["label"],
            "count": sections[section_id]["count"],
            "top_item": sections[section_id]["top_item"],
            "href": f"#{sections[section_id]['top_item']['card_anchor_id']}" if sections[section_id]["top_item"] else f"#hypothesis-section-{section_id}",
        }
        for section_id in HYPOTHESIS_VIEW_SECTION_ORDER
    ]
    rendered_hypothesis_ids = [
        item["hypothesis_id"]
        for section in sections.values()
        for item in section["visible_items"]
    ]
    raw_hypothesis_ids = [str(row.get("hypothesis_id") or "") for row in rows]
    rendered_hypothesis_id_set = set(rendered_hypothesis_ids)
    unmapped_hypothesis_ids = [hypothesis_id for hypothesis_id in raw_hypothesis_ids if hypothesis_id not in rendered_hypothesis_id_set]
    return {
        "schema_id": "hypothesis_view_model.v1",
        "section_order": list(HYPOTHESIS_VIEW_SECTION_ORDER),
        "sections": sections,
        "summary_cards": summary_cards,
        "primary_cta": primary_cta,
        "raw_hypothesis_count": len(rows),
        "rendered_hypothesis_count": len(rendered_hypothesis_ids),
        "unmapped_hypothesis_ids": unmapped_hypothesis_ids,
    }


def _build_normalized_hypothesis_inventory(queue: dict[str, Any], aegis_pipeline: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for row in queue.get("queue") or []:
        normalized = _normalize_store_hypothesis(row)
        if normalized["hypothesis_id"] and normalized["lifecycle_state"] != "Archived":
            rows.append(normalized)
    for item in aegis_pipeline.get("items") or []:
        normalized = _normalize_aegis_hypothesis(item, generated_at=str(aegis_pipeline.get("generated_at_utc") or ""))
        if normalized["hypothesis_id"] and normalized["lifecycle_state"] != "Archived":
            rows.append(normalized)
    seen: set[str] = set()
    unique_rows: list[dict[str, Any]] = []
    for row in rows:
        key = row["hypothesis_id"]
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    research_runs = read_research_run_ledger_v1(store_root=store_root)
    for row in unique_rows:
        projection = research_run_projection_for_hypothesis_v1(row, research_runs)
        row["research_run_state"] = projection
        collecting_evidence = str(row.get("test_status") or row.get("latest_result") or "").upper() == "INCONCLUSIVE_SAMPLE_SIZE"
        row["user_facing_status"] = "Collecting Evidence" if collecting_evidence else projection["user_facing_status"]
        row["user_facing_explanation"] = "Research is active. Aegis needs more forward-return observations before this hypothesis can qualify for paper validation." if collecting_evidence else projection["user_facing_explanation"]
        if collecting_evidence:
            row["operator_action_required"] = False
        row["last_research_run"] = projection["last_research_run"]
        row["trigger_source"] = projection["trigger_source"]
        row["started_by_user"] = projection["started_by_user"]
        row["primary_action_label"] = projection["primary_action_label"]
        command = command_for_hypothesis_status_v1(row["user_facing_status"])
        hypothesis_id = str(row.get("hypothesis_id") or row.get("hypothesis_proposal_id") or row.get("item_id") or "")
        row["primary_command"] = command_instance_v1(
            command["command_id"],
            target_type="hypothesis",
            target_id=hypothesis_id,
            label=command["label"],
            payload={
                "hypothesis_id": hypothesis_id,
                "title": row.get("title") or row.get("hypothesis_summary") or hypothesis_id,
                "symbols": " ".join(str(symbol) for symbol in (row.get("symbols") or row.get("required_symbols") or row.get("related_symbols") or []) if str(symbol)),
            },
            enabled=not (row["user_facing_status"] == "Blocked" and command["command_id"] == "START_RESEARCH"),
            disabled_reason="Resolve the blocker before starting research." if row["user_facing_status"] == "Blocked" and command["command_id"] == "START_RESEARCH" else "",
        )
        row["secondary_commands"] = []
        row["disabled_commands"] = []
        row["search_text"] = " ".join(
            [
                row.get("search_text", ""),
                row["user_facing_status"],
                row["user_facing_explanation"],
                projection["trigger_source"],
            ]
        ).lower()
    counts_by_lifecycle: dict[str, int] = {}
    counts_by_source: dict[str, int] = {}
    attention = {"operator_action_required": 0, "no_operator_action_required": 0}
    for row in unique_rows:
        counts_by_lifecycle[row["lifecycle_state"]] = counts_by_lifecycle.get(row["lifecycle_state"], 0) + 1
        counts_by_source[row["source_type"]] = counts_by_source.get(row["source_type"], 0) + 1
        attention["operator_action_required" if row["operator_action_required"] else "no_operator_action_required"] += 1
    integrity_ok = all(row.get("hypothesis_id") and row.get("title") and row.get("source_type") and row.get("search_text") for row in unique_rows)
    hypothesis_view_model = build_hypothesis_view_model_v1(unique_rows)
    return {
        "all_hypotheses": unique_rows,
        "hypothesis_view_model_v1": hypothesis_view_model,
        "counts_by_lifecycle": counts_by_lifecycle,
        "counts_by_source": counts_by_source,
        "counts_by_attention": attention,
        "research_run_ledger": {
            "schema_id": "research_run_ledger.v1",
            "path": str(research_run_ledger_path_v1(store_root=store_root)),
            "count": len(research_runs),
        },
        "search_index_integrity_status": "ok" if integrity_ok else "warning",
    }


def _safety() -> dict[str, Any]:
    return {
        "safety_labels": SAFETY_LABELS,
        "hypothetical_evidence_label": HYPOTHETICAL_EVIDENCE_LABEL,
        "read_only_governance": True,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "autonomous_trading_allowed": False,
        "automatic_promotion_allowed": False,
        "sleeve_mutation_allowed": False,
        "capital_allocation_allowed": False,
        "manual_review_required": True,
    }


def _infer_event_family(text: str) -> str:
    lowered = text.lower()
    rules = [
        ("oil_shock", ["oil", "crude", "uso", "xle", "xop", "energy"]),
        ("volatility_compression", ["volatility compression", "compressed", "range expansion", "breakout"]),
        ("volatility_spike", ["volatility", "vix", "range spike", "vol spike"]),
        ("breadth_collapse", ["breadth collapse", "breadth deterioration", "weak breadth"]),
        ("breadth_recovery", ["breadth recovery", "recovery breadth"]),
        ("gap_event", ["gap", "overnight"]),
        ("drawdown_recovery", ["drawdown", "recovery thrust"]),
        ("rate_shock", ["rate", "treasury", "yield", "fomc"]),
        ("credit_stress", ["credit", "hyg", "lqd", "spread"]),
        ("commodity_dislocation", ["commodity", "dbc", "gld", "gold"]),
        ("macro_headline_shock", ["headline", "macro", "cpi", "nfp"]),
        ("regime_transition", ["regime", "risk on", "risk off", "risk_on", "risk_off"]),
    ]
    for family, needles in rules:
        if any(item in lowered for item in needles):
            return family
    return "macro_headline_shock"


def _parse_symbols(value: Any) -> list[str]:
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value or "").replace(",", " ").split()
    return sorted({str(item).strip().upper() for item in raw if str(item).strip()})


def _readiness_for(row: dict[str, Any], store: Path) -> dict[str, Any]:
    proposal_id = str(row.get("hypothesis_proposal_id") or "")
    return latest_readiness_assessment(proposal_id, store_root=store) or {}


def research_hypothesis_queue_v1(*, status: str | None = None, store_root: Path | None = None, rebuild: bool = False) -> dict[str, Any]:
    store = _store(store_root)
    if rebuild:
        rebuild_research_projections(projection="all", store_root=store, actor="operator-ui", strict=False)
    return projection_api_payload(status=status, store_root=store) | _safety()


def _queue_row_for_action(queue: dict[str, Any], hypothesis_proposal_id: str) -> dict[str, Any]:
    target = str(hypothesis_proposal_id or "")
    for row in queue.get("queue") or queue.get("hypothesis_proposals") or []:
        if str(row.get("hypothesis_proposal_id") or row.get("item_id") or "") == target:
            lane = str(row.get("lane") or row.get("proposal_status") or "proposed")
            return {
                "hypothesis_proposal_id": target,
                "title": str(row.get("title") or row.get("hypothesis_summary") or target),
                "lane": lane,
                "state": _operator_state_from_lane(lane),
                "proposal_status": str(row.get("proposal_status") or lane),
                "updated_at_utc": str(row.get("updated_at_utc") or row.get("reviewed_at") or row.get("created_at") or ""),
            }
    return {
        "hypothesis_proposal_id": target,
        "title": target,
        "lane": "unknown",
        "state": "UNKNOWN",
        "proposal_status": "unknown",
        "updated_at_utc": "",
    }


def _operator_state_from_lane(lane: str) -> str:
    normalized = str(lane or "").strip().lower()
    return {
        "proposed": "IDEA",
        "watchlist": "WATCHLIST",
        "needs_data": "BLOCKED",
        "accepted_for_research": "RESEARCHING",
        "needs_review": "IDEA",
        "rejected": "ARCHIVED",
        "archived": "ARCHIVED",
    }.get(normalized, normalized.upper() or "UNKNOWN")


def _research_action_message(action: str, before: dict[str, Any], after: dict[str, Any], result: dict[str, Any]) -> str:
    action_name = str(action or "").strip().lower()
    after_state = str(after.get("state") or "UNKNOWN")
    if action_name == "assess-readiness":
        return "Readiness assessed. The Research Pipeline was refreshed."
    if action_name == "accept_for_research":
        return "Accepted for Research." if after_state == "RESEARCHING" else f"Review recorded. Current state: {after_state}."
    if action_name == "watchlist":
        return "Moved to Watchlist."
    if action_name == "reject":
        return "Rejected and moved out of active research."
    if action_name == "archive":
        return "Archived."
    if action_name == "convert":
        return "Converted to Research Plan."
    return str(result.get("message") or "Research Pipeline action completed.")


def _dataset_date_range(store: Path, dataset_snapshot_id: str) -> tuple[str, str]:
    dataset_id = str(dataset_snapshot_id or "").strip()
    if not dataset_id:
        return "", ""
    registry_row = {}
    for row in read_jsonl(store / "registries" / "dataset_snapshots.jsonl"):
        if str(row.get("dataset_snapshot_id") or "") == dataset_id:
            registry_row = row
            break
    manifest = {}
    for filename in ("manifest.json", "dataset_snapshot.json"):
        path = store / "datasets" / dataset_id / filename
        if path.exists():
            try:
                manifest = read_json(path)
            except Exception:
                manifest = {}
            if manifest:
                break
    start = str(registry_row.get("start_date") or manifest.get("start_date") or manifest.get("start") or "").strip()
    end = str(registry_row.get("end_date") or manifest.get("end_date") or manifest.get("end") or "").strip()
    date_range = manifest.get("date_range") if isinstance(manifest.get("date_range"), dict) else {}
    start = start or str(date_range.get("start") or "").strip()
    end = end or str(date_range.get("end") or "").strip()
    return start, end


def _conversion_payload_with_defaults(store: Path, hypothesis_proposal_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload or {})
    readiness = latest_readiness_assessment(hypothesis_proposal_id, store_root=store) or {}
    dataset_candidates = readiness.get("dataset_snapshot_candidates") or []
    universe_candidates = readiness.get("universe_snapshot_candidates") or []
    if not out.get("dataset_snapshot_id") and dataset_candidates:
        out["dataset_snapshot_id"] = dataset_candidates[0].get("dataset_snapshot_id")
    if not out.get("universe_snapshot_id") and universe_candidates:
        out["universe_snapshot_id"] = universe_candidates[0].get("universe_snapshot_id")
    if not out.get("start") or not out.get("end"):
        start, end = _dataset_date_range(store, str(out.get("dataset_snapshot_id") or ""))
        out["start"] = out.get("start") or start
        out["end"] = out.get("end") or end
    return out


def research_action_failure_response(*, action: str, hypothesis_proposal_id: str, error: Exception) -> dict[str, Any]:
    reason = str(error)
    return {
        "ok": False,
        "success": False,
        "action": action,
        "hypothesis_proposal_id": hypothesis_proposal_id,
        "operator_message": f"Unable to {action.replace('-', ' ')} hypothesis.",
        "message": f"Unable to {action.replace('-', ' ')} hypothesis.",
        "failure_reason": reason,
        "projection_refreshed": False,
        **_safety(),
    }


def _research_action_response(*, action: str, before: dict[str, Any], after: dict[str, Any], result: dict[str, Any], projection_result: dict[str, Any]) -> dict[str, Any]:
    if action == "convert":
        after = dict(after)
        after["state"] = "VALIDATING"
    projection_outputs = projection_result.get("outputs") if isinstance(projection_result.get("outputs"), dict) else {}
    hypothesis_queue = projection_outputs.get("hypothesis_queue") if isinstance(projection_outputs.get("hypothesis_queue"), dict) else {}
    operator_home = projection_outputs.get("operator_home") if isinstance(projection_outputs.get("operator_home"), dict) else {}
    review_payload = result.get("hypothesis_proposal_review") if isinstance(result.get("hypothesis_proposal_review"), dict) else {}
    readiness_payload = result.get("readiness_assessment") if isinstance(result.get("readiness_assessment"), dict) else {}
    plan_payload = result.get("research_plan") if isinstance(result.get("research_plan"), dict) else {}
    now = str(
        result.get("reviewed_at")
        or result.get("created_at")
        or result.get("generated_at")
        or review_payload.get("reviewed_at")
        or readiness_payload.get("assessed_at")
        or plan_payload.get("created_at")
        or after.get("updated_at_utc")
        or utc_now_iso()
    )
    payload = {
        "ok": True,
        "success": True,
        "action": action,
        "hypothesis_proposal_id": after.get("hypothesis_proposal_id") or before.get("hypothesis_proposal_id"),
        "previous_state": before.get("state"),
        "new_state": after.get("state"),
        "previous_lane": before.get("lane"),
        "new_lane": after.get("lane"),
        "updated_lane": after.get("lane"),
        "timestamp": now or str(after.get("updated_at_utc") or ""),
        "operator_message": _research_action_message(action, before, after, result),
        "failure_reason": "",
        "projection_refreshed": True,
        "projection_build_id": (hypothesis_queue.get("build") or {}).get("projection_build_id", ""),
        "operator_projection_build_id": (operator_home.get("build") or {}).get("projection_build_id", ""),
        "transition": {
            "from": before,
            "to": after,
        },
        **_safety(),
    }
    payload.update(result)
    payload["message"] = payload["operator_message"]
    return payload


def research_intake_dossier_v1(*, hypothesis_proposal_id: str, store_root: Path | None = None) -> dict[str, Any]:
    store = _store(store_root)
    dossier = latest_research_intake_dossier(hypothesis_proposal_id, store_root=store)
    if dossier is None:
        dossier = build_and_store_research_intake_dossier(hypothesis_proposal_id=hypothesis_proposal_id, store_root=store, actor="operator-ui")["research_intake_dossier"]
    readiness = latest_readiness_assessment(hypothesis_proposal_id, store_root=store) or {}
    review = latest_hypothesis_proposal_review(hypothesis_proposal_id, store_root=store) or {}
    return {
        "ok": True,
        "dossier": dossier,
        "readiness": readiness,
        "latest_review": review,
        "operator_sections": {
            "What is the hypothesis?": dossier.get("hypothesis"),
            "Why might it matter?": "It is a research proposal for observed market-event behavior; it is not an investment recommendation.",
            "What data is required?": dossier.get("data_requirement_status"),
            "What data is missing?": readiness.get("missing_symbols") or readiness.get("blocking_items") or [],
            "What event definition is proposed?": dossier.get("event_family_id"),
            "What symbols/universe are needed?": readiness.get("required_symbols") or [],
            "What regimes should be checked?": readiness.get("regime_dimensions_supported"),
            "What is the priority?": dossier.get("recommended_next_action"),
            "What are the risks/fragility notes?": dossier.get("governance_classification"),
            "What can I do next?": readiness.get("next_allowed_actions") or [],
        },
        **_safety(),
    }


def start_research_v1(payload: dict[str, Any], *, store_root: Path | None = None, actor: str = "operator-ui") -> dict[str, Any]:
    store = _store(store_root)
    existing_hypothesis_id = str(payload.get("hypothesis_id") or payload.get("existing_hypothesis_id") or payload.get("hypothesis_proposal_id") or "").strip()
    idea = str(payload.get("idea") or payload.get("research_idea") or payload.get("title") or "").strip()
    if existing_hypothesis_id:
        research_run = record_user_initiated_research_run_v1(
            hypothesis_id=existing_hypothesis_id,
            trigger_reason="Operator clicked Start Research for an existing hypothesis in the Hypotheses workspace.",
            input_artifact_ids=[existing_hypothesis_id],
            store_root=store,
        )
        audit = write_audit_event(
            actor=actor,
            entity_type="research_run",
            entity_id=existing_hypothesis_id,
            action="research_console_existing_hypothesis_start_requested",
            previous_state_hash="",
            new_state_hash=research_run["content_hash"],
            reason="Operator requested AI research for an existing hypothesis; no study, sleeve, trade, or allocation was created.",
            metadata={"research_run_id": research_run["research_run_id"]},
            store_root=store,
        )
        projection_result = rebuild_research_projections(projection="all", store_root=store, actor=actor, strict=False)
        display_title = idea or existing_hypothesis_id
        operator_message = f"Research started for {display_title}."
        return {
            "ok": True,
            "message": operator_message,
            "operator_message": operator_message,
            "hypothesis_proposal_id": existing_hypothesis_id,
            "research_run_id": research_run["research_run_id"],
            "research_run_status": research_run["run_status"],
            "research_run_trigger_source": research_run["trigger_source"],
            "research_run": research_run,
            "audit_event": audit,
            "projection_build_id": projection_result["outputs"].get("hypothesis_queue", {}).get("build", {}).get("projection_build_id", ""),
            "operator_projection_build_id": projection_result["outputs"].get("operator_home", {}).get("build", {}).get("projection_build_id", ""),
            **_safety(),
        }
    if not idea:
        raise ValueError("Research idea is required.")
    seed_event_families(store_root=store, actor=actor)
    event_family_id = str(payload.get("event_family") or payload.get("event_family_id") or _infer_event_family(idea)).strip().lower()
    symbols = _parse_symbols(payload.get("symbols"))
    priority = str(payload.get("priority") or "watchlist").strip().lower()
    if priority not in {"low", "medium", "high", "watchlist"}:
        priority = "watchlist"
    notes = str(payload.get("notes") or "operator-started research idea; research intake only").strip()
    observation_result = capture_event_observation(
        store_root=store,
        actor=actor,
        event_family_id=event_family_id,
        title=idea,
        description=str(payload.get("description") or idea),
        source_type="operator_observation",
        source_ref="research_console",
        symbols_mentioned=symbols,
        market_context={"operator_console": True},
        suspected_mechanism=str(payload.get("suspected_mechanism") or idea),
        confidence_level=str(payload.get("confidence_level") or "unknown").strip().lower(),
        research_priority=priority,
        notes=notes,
    )
    observation = observation_result["event_observation"]
    cluster_result = create_event_cluster(
        event_family_id=event_family_id,
        observation_ids=[observation["event_observation_id"]],
        cluster_title=f"Research idea: {idea[:80]}",
        cluster_description=f"Operator-started research intake from Research Console: {idea}",
        store_root=store,
        actor=actor,
    )
    intent_result = generate_intent_candidate(event_cluster_id=cluster_result["event_cluster"]["event_cluster_id"], store_root=store, actor=actor)
    proposal_result = generate_hypothesis_proposal(intent_candidate_id=intent_result["intent_candidate"]["intent_candidate_id"], store_root=store, actor=actor)
    proposal = proposal_result["hypothesis_proposal"]
    readiness_result = assess_hypothesis_readiness(hypothesis_proposal_id=proposal["hypothesis_proposal_id"], store_root=store, actor=actor)
    priority_result = score_hypothesis_proposal(hypothesis_proposal_id=proposal["hypothesis_proposal_id"], readiness=readiness_result["readiness_assessment"], store_root=store, actor=actor)
    dossier_result = build_and_store_research_intake_dossier(hypothesis_proposal_id=proposal["hypothesis_proposal_id"], store_root=store, actor=actor)
    research_run = record_user_initiated_research_run_v1(
        hypothesis_id=proposal["hypothesis_proposal_id"],
        trigger_reason="Operator clicked Start Research in the Hypotheses workspace.",
        input_artifact_ids=[
            observation["event_observation_id"],
            cluster_result["event_cluster"]["event_cluster_id"],
            intent_result["intent_candidate"]["intent_candidate_id"],
            proposal["hypothesis_proposal_id"],
            dossier_result["research_intake_dossier"]["research_intake_dossier_id"],
        ],
        store_root=store,
    )
    audit = write_audit_event(
        actor=actor,
        entity_type="research_console",
        entity_id=proposal["hypothesis_proposal_id"],
        action="research_console_start_research_completed",
        previous_state_hash="",
        new_state_hash=content_hash({"proposal": proposal["content_hash"], "readiness": readiness_result["readiness_assessment"]["content_hash"]}),
        reason="Operator started research idea through Research Console; no study, sleeve, trade, or allocation was created.",
        metadata={"event_family_id": event_family_id},
        store_root=store,
    )
    projection_result = rebuild_research_projections(projection="all", store_root=store, actor=actor, strict=False)
    return {
        "ok": True,
        "message": "Research idea captured for human review.",
        "event_family_id": event_family_id,
        "event_observation_id": observation["event_observation_id"],
        "event_cluster_id": cluster_result["event_cluster"]["event_cluster_id"],
        "intent_candidate_id": intent_result["intent_candidate"]["intent_candidate_id"],
        "hypothesis_proposal_id": proposal["hypothesis_proposal_id"],
        "proposal_status": proposal["proposal_status"],
        "research_run_id": research_run["research_run_id"],
        "research_run_status": research_run["run_status"],
        "research_run_trigger_source": research_run["trigger_source"],
        "research_run": research_run,
        "readiness_assessment": readiness_result["readiness_assessment"],
        "priority_score": priority_result["proposal_priority_score"],
        "research_intake_dossier_id": dossier_result["research_intake_dossier"]["research_intake_dossier_id"],
        "audit_event": audit,
        "projection_build_id": projection_result["outputs"]["hypothesis_queue"]["build"]["projection_build_id"],
        "operator_projection_build_id": projection_result["outputs"].get("operator_home", {}).get("build", {}).get("projection_build_id", ""),
        **_safety(),
    }


def review_hypothesis_v1(hypothesis_proposal_id: str, payload: dict[str, Any], *, store_root: Path | None = None, actor: str = "operator-ui") -> dict[str, Any]:
    decision = str(payload.get("decision") or payload.get("review_decision") or "").strip().lower()
    reason = str(payload.get("reason") or payload.get("review_reason") or "Operator review from Research Console.").strip()
    reviewed_by = str(payload.get("reviewed_by") or actor).strip() or actor
    store = _store(store_root)
    before = _queue_row_for_action(research_hypothesis_queue_v1(store_root=store), hypothesis_proposal_id)
    result = review_hypothesis_proposal(hypothesis_proposal_id=hypothesis_proposal_id, decision=decision, reason=reason, reviewed_by=reviewed_by, store_root=store, actor=actor)
    projection_result = rebuild_research_projections(projection="all", store_root=store, actor=actor, strict=False)
    after = _queue_row_for_action(research_hypothesis_queue_v1(store_root=store), hypothesis_proposal_id)
    return _research_action_response(action=decision, before=before, after=after, result=result, projection_result=projection_result)


def assess_hypothesis_v1(hypothesis_proposal_id: str, *, store_root: Path | None = None, actor: str = "operator-ui") -> dict[str, Any]:
    store = _store(store_root)
    before = _queue_row_for_action(research_hypothesis_queue_v1(store_root=store), hypothesis_proposal_id)
    result = assess_hypothesis_readiness(hypothesis_proposal_id=hypothesis_proposal_id, store_root=store, actor=actor)
    projection_result = rebuild_research_projections(projection="all", store_root=store, actor=actor, strict=False)
    after = _queue_row_for_action(research_hypothesis_queue_v1(store_root=store), hypothesis_proposal_id)
    return _research_action_response(action="assess-readiness", before=before, after=after, result=result, projection_result=projection_result)


def convert_hypothesis_v1(hypothesis_proposal_id: str, payload: dict[str, Any], *, store_root: Path | None = None, actor: str = "operator-ui") -> dict[str, Any]:
    store = _store(store_root)
    before = _queue_row_for_action(research_hypothesis_queue_v1(store_root=store), hypothesis_proposal_id)
    conversion_payload = _conversion_payload_with_defaults(store, hypothesis_proposal_id, payload)
    result = convert_hypothesis_proposal_to_research_plan(
        hypothesis_proposal_id=hypothesis_proposal_id,
        approve=conversion_payload.get("approve") is True or str(conversion_payload.get("approve") or "").lower() == "true",
        dataset_snapshot_id=conversion_payload.get("dataset_snapshot_id"),
        universe_snapshot_id=conversion_payload.get("universe_snapshot_id"),
        start=conversion_payload.get("start"),
        end=conversion_payload.get("end"),
        store_root=store,
        actor=actor,
    )
    result["conversion_inputs"] = {
        "dataset_snapshot_id": conversion_payload.get("dataset_snapshot_id") or "",
        "universe_snapshot_id": conversion_payload.get("universe_snapshot_id") or "",
        "start": conversion_payload.get("start") or "",
        "end": conversion_payload.get("end") or "",
    }
    projection_result = rebuild_research_projections(projection="all", store_root=store, actor=actor, strict=False)
    after = _queue_row_for_action(research_hypothesis_queue_v1(store_root=store), hypothesis_proposal_id)
    return _research_action_response(action="convert", before=before, after=after, result=result, projection_result=projection_result)



def start_research_options_v1() -> dict[str, Any]:
    return {
        "ok": True,
        "event_family_options": [{"value": key, "label": label} for key, label in EVENT_FAMILY_LABELS.items()],
        "priority_options": ["watchlist", "low", "medium", "high"],
        "examples": ["Oil shock reversals", "Volatility compression breakouts", "Breadth collapse recoveries", "Gap-fill after large overnight moves", "ETF drop mean reversion"],
        **_safety(),
    }


def evidence_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return evidence_payload(store_root=_store(store_root)) | _safety()

def research_plans_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_plans_payload(store_root=_store(store_root)) | _safety()

def paper_trials_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return paper_trials_payload(store_root=_store(store_root)) | _safety()

def sleeve_review_center_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return sleeve_review_center_payload(store_root=_store(store_root)) | _safety()

def blocked_evidence_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return blocked_work_payload(store_root=_store(store_root)) | _safety()

def research_backlog_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    return research_backlog_payload(store_root=_store(store_root)) | _safety()

def research_console_v1(*, store_root: Path | None = None) -> dict[str, Any]:
    queue = research_hypothesis_queue_v1(store_root=store_root)
    paper = paper_trials_v1(store_root=store_root)
    sleeves = sleeve_review_center_v1(store_root=store_root)
    blocked = blocked_evidence_v1(store_root=store_root)
    backlog = research_backlog_v1(store_root=store_root)
    plans = research_plans_v1(store_root=store_root)
    evidence = evidence_v1(store_root=store_root)
    projection_summary = operator_home_projection_summary(store_root=store_root)
    projection_health_payload = projection_health(store_root=store_root)
    aegis_pipeline = _latest_aegis_research_pipeline_v1()
    store = _store(store_root)
    inventory = _build_normalized_hypothesis_inventory(queue, aegis_pipeline, store_root=store)
    top_attention = {"title": "Research queue projection needs rebuild/inspection", "recommended_next_action": "Rebuild research projections"} if projection_summary.get("projection_integrity_status") in {"source_missing", "error", "stale"} else (queue.get("queue") or [{}])[0]
    return {
        "ok": True,
        "read_only": True,
        "title": "Research Console",
        "summary": "Research needs your attention.",
        "primary_actions": ["Start Research", "Review Hypotheses", "Investigate Blocked Work", "Continue Paper Trials", "Review Sleeves", "View Research Backlog"],
        "start_research_examples": ["Oil shock reversals", "Volatility compression breakouts", "Breadth collapse recoveries", "Gap-fill after large overnight moves", "ETF drop mean reversion"],
        "event_family_options": [{"value": key, "label": label} for key, label in EVENT_FAMILY_LABELS.items()],
        "hypothesis_queue": queue,
        "aegis_research_pipeline": aegis_pipeline,
        "combined_hypothesis_count": len(inventory["all_hypotheses"]),
        "all_hypotheses": inventory["all_hypotheses"],
        "hypothesis_view_model_v1": inventory["hypothesis_view_model_v1"],
        "counts_by_lifecycle": inventory["counts_by_lifecycle"],
        "counts_by_source": inventory["counts_by_source"],
        "counts_by_attention": inventory["counts_by_attention"],
        "research_run_ledger": inventory["research_run_ledger"],
        "search_index_integrity_status": inventory["search_index_integrity_status"],
        "projection_summary": projection_summary,
        "projection_health": projection_health_payload,
        "paper_trials": paper,
        "sleeve_review_center": sleeves,
        "blocked_evidence": blocked,
        "research_backlog": backlog,
        "research_plans": plans,
        "evidence": evidence,
        "what_needs_attention": {
            "top_blocked_item": (blocked.get("blocked_items") or [{}])[0],
            "top_hypothesis_to_review": top_attention,
            "top_paper_trial_action": (paper.get("paper_trials") or [{}])[0],
            "top_sleeve_review_action": (sleeves.get("sleeves") or [{}])[0],
        },
        **_safety(),
    }


def explicit_research_action_placeholder_v1(action: str, entity_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "ok": False,
        "action": action,
        "entity_id": entity_id,
        "message": "This explicit research action is not wired in the operator console yet. No study, sleeve, trade, order, or allocation was created.",
        "request_payload": payload or {},
        **_safety(),
    }
