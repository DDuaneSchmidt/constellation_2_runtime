from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_chatgpt_control_packet.v1.schema.json"
PACKET_VERSION = "aegis_chatgpt_control_packet.v1"
RUNTIME_TRUTH_CLASSIFICATIONS = {
    "REAL_RUNTIME",
    "DEMO_ONLY",
    "DRY_RUN_ONLY",
    "PARTIAL_CONTEXT",
    "ADVISORY_ONLY",
    "BLOCKED",
}
STALE_AFTER = timedelta(hours=36)
ACTIONABLE_ALERT_STATUSES = {"ACTIONABLE_TRADE", "URGENT_ACTIONABLE_TRADE", "PASS"}
PASS_STATUSES = {"PASS", "READY_FOR_MANUAL_ENTRY", "ACTIONABLE_TRADE", "URGENT_ACTIONABLE_TRADE"}
TRADE_REQUIRED_FIELDS = {
    "symbol",
    "side",
    "quantity_or_sizing_guidance",
    "entry_reference_price",
    "stop_price",
    "risk_per_trade",
    "valid_until",
}


@dataclass(frozen=True)
class SourceSpec:
    key: str
    filename: str
    required_for_full_context: bool = False
    day_scoped: bool = True


SOURCE_SPECS = [
    SourceSpec("aegis_lite_operating_status", "aegis_lite_operating_status.v1.json", True),
    SourceSpec("aegis_lite_eod_report", "aegis_lite_eod_report.v1.json", True),
    SourceSpec("operator_execution_queue", "operator_execution_queue.v1.json", True),
    SourceSpec("manual_trade_packet", "manual_trade_packet.v1.json", False),
    SourceSpec("manual_execution_receipt", "manual_execution_receipt.v1.json", False),
    SourceSpec("outcome_ledger", "outcome_ledger.v1.json", False),
    SourceSpec("trade_outcome_attribution", "trade_outcome_attribution.v1.json", False),
    SourceSpec("promoted_sleeve_library", "promoted_sleeve_library.v1.json", False, False),
    SourceSpec("event_market_snapshot", "event_market_snapshot.v1.json", False),
    SourceSpec("event_monitoring_status", "event_monitoring_status.v1.json", True),
    SourceSpec("event_rules_registry", "event_rules_registry.v1.json", True),
    SourceSpec("event_awareness_ledger", "event_awareness_ledger.v1.json", False),
    SourceSpec("event_tactical_packet", "event_tactical_packet.v1.json", False),
    SourceSpec("event_validity_gate", "event_validity_gate.v1.json", False),
    SourceSpec("trade_capture_alert_gate", "trade_capture_alert_gate.v1.json", False),
    SourceSpec("trade_capture_alert_ledger", "trade_capture_alert_ledger.v1.json", False),
    SourceSpec("research_task_queue", "research_task_queue.v1.json", False, False),
    SourceSpec("operator_inbox_review_report", "operator_inbox_review_report.v1.json", False, False),
    SourceSpec("sleeve_performance_report", "sleeve_performance_report.v1.json", False),
    SourceSpec("evidence_gate", "evidence_gate.v1.json", False),
    SourceSpec("ai_feedback_review", "ai_feedback_review.v1.json", False),
    SourceSpec("research_dataset_gap", "research_dataset_gap.v1.json", False),
    SourceSpec("aegis_operator_status", "aegis_operator_status.v1.json", False),
]


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def aegis_chatgpt_control_packet_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).expanduser().resolve()
        / "reports"
        / "aegis_chatgpt_control_packet_v1"
        / day_utc
        / "aegis_chatgpt_control_packet.v1.json"
    )


def validate_aegis_chatgpt_control_packet_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)


def write_aegis_chatgpt_control_packet_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_aegis_chatgpt_control_packet_v1(payload)
    path = aegis_chatgpt_control_packet_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_aegis_chatgpt_control_packet_v1(
    *,
    truth_root: Path,
    day_utc: str,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or now_utc_v1()
    generated_dt = _parse_time(generated_at) or datetime.now(UTC).replace(microsecond=0)
    sources = _load_sources(root=root, day_utc=day_utc, generated_dt=generated_dt)
    payloads = {key: row["payload"] for key, row in sources.items()}

    lite_status = _build_lite_status(payloads)
    event_status = _build_event_status(payloads)
    market_context_status = _build_market_context_status(payloads)
    research_status = _build_research_status(payloads)
    inbox_status = _build_operator_inbox_status(payloads)
    sleeve_status = _build_sleeve_performance_status(payloads)
    ai_status = _build_ai_feedback_status(payloads)
    dataset_gaps = _build_dataset_gaps(payloads)
    actionable_items, blocked_items = _classify_actionable_items(payloads)
    missing_or_stale = _stale_or_missing_sources(sources)
    runtime_truth = _runtime_truth_classification(
        sources=sources,
        payloads=payloads,
        actionable_items=actionable_items,
        blocked_items=blocked_items,
    )
    trade_gate = _trade_advice_gate(
        runtime_truth_classification=runtime_truth,
        actionable_items=actionable_items,
        lite_status=lite_status,
    )
    do_not_claim = _do_not_claim(
        sources=sources,
        payloads=payloads,
        actionable_items=actionable_items,
        event_status=event_status,
        ai_status=ai_status,
        dataset_gaps=dataset_gaps,
    )
    readiness = _readiness_state(
        runtime_truth_classification=runtime_truth,
        trade_gate=trade_gate,
        missing_or_stale=missing_or_stale,
        actionable_items=actionable_items,
        blocked_items=blocked_items,
    )
    packet = {
        "schema_id": "aegis_chatgpt_control_packet",
        "schema_version": "v1",
        "artifact_id": "aegis_chatgpt_control_packet_v1",
        "day_utc": day_utc,
        "generated_at": generated_at,
        "packet_version": PACKET_VERSION,
        "runtime_truth_classification": runtime_truth,
        "source_artifacts_used": _source_artifacts_used(sources),
        "source_artifact_timestamps": _source_timestamps(sources),
        "stale_or_missing_sources": missing_or_stale,
        "aegis_lite_status": lite_status,
        "event_monitoring_status": event_status,
        "market_context_status": market_context_status,
        "research_lab_status": research_status,
        "operator_inbox_status": inbox_status,
        "sleeve_performance_status": sleeve_status,
        "ai_feedback_status": ai_status,
        "dataset_gaps": dataset_gaps,
        "readiness_state": readiness,
        "current_actionable_items": actionable_items,
        "blocked_items": blocked_items,
        "next_operator_actions": _next_operator_actions(readiness=readiness, trade_gate=trade_gate, payloads=payloads),
        "do_not_claim": do_not_claim,
        "safety_assertions": _safety_assertions(),
        "trade_advice_allowed": trade_gate["trade_advice_allowed"],
        "manual_trade_capture_allowed": trade_gate["manual_trade_capture_allowed"],
        "reason_if_blocked": trade_gate["reason_if_blocked"],
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "autonomous_execution_allowed": False,
        "production_mutation_allowed": False,
        "canonical_json_hash": None,
    }
    packet["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(packet)
    validate_aegis_chatgpt_control_packet_v1(packet)
    return packet


def render_aegis_chatgpt_control_packet_summary_v1(packet: dict[str, Any]) -> str:
    lines = [
        "AEGIS CHATGPT CONTROL PACKET v1",
        f"day_utc: {packet.get('day_utc')}",
        f"generated_at: {packet.get('generated_at')}",
        f"runtime_truth_classification: {packet.get('runtime_truth_classification')}",
        f"trade_advice_allowed: {packet.get('trade_advice_allowed')}",
        f"manual_trade_capture_allowed: {packet.get('manual_trade_capture_allowed')}",
        f"reason_if_blocked: {packet.get('reason_if_blocked') or 'NONE'}",
        "",
        "Readiness:",
        f"- classification: {(packet.get('readiness_state') or {}).get('classification')}",
        f"- current_actionable_items: {len(packet.get('current_actionable_items') or [])}",
        f"- blocked_items: {len(packet.get('blocked_items') or [])}",
        "",
        "Next operator actions:",
    ]
    for action in packet.get("next_operator_actions") or []:
        lines.append(f"- {action}")
    lines.extend(["", "Do Not Claim:"])
    for item in packet.get("do_not_claim") or []:
        lines.append(f"- {item}")
    return "\n".join(lines)


def _load_sources(*, root: Path, day_utc: str, generated_dt: datetime) -> dict[str, dict[str, Any]]:
    loaded: dict[str, dict[str, Any]] = {}
    for spec in SOURCE_SPECS:
        candidates = _find_candidates(root=root, spec=spec, day_utc=day_utc)
        if not candidates:
            loaded[spec.key] = {
                "key": spec.key,
                "filename": spec.filename,
                "required_for_full_context": spec.required_for_full_context,
                "status": "MISSING",
                "path": "",
                "payload": {},
                "timestamp": "",
                "staleness_status": "MISSING",
                "reason_code": "SOURCE_MISSING",
            }
            continue
        path = candidates[-1]
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("SOURCE_JSON_NOT_OBJECT")
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            loaded[spec.key] = {
                "key": spec.key,
                "filename": spec.filename,
                "required_for_full_context": spec.required_for_full_context,
                "status": "UNREADABLE",
                "path": str(path),
                "payload": {},
                "timestamp": "",
                "staleness_status": "UNREADABLE",
                "reason_code": f"SOURCE_UNREADABLE:{type(exc).__name__}",
            }
            continue
        timestamp = _artifact_timestamp(payload)
        staleness = _staleness_status(timestamp=timestamp, generated_dt=generated_dt, day_scoped=spec.day_scoped)
        loaded[spec.key] = {
            "key": spec.key,
            "filename": spec.filename,
            "required_for_full_context": spec.required_for_full_context,
            "status": "PRESENT",
            "path": str(path),
            "payload": payload,
            "timestamp": timestamp,
            "staleness_status": staleness,
            "reason_code": "SOURCE_PRESENT",
        }
    return loaded


def _find_candidates(*, root: Path, spec: SourceSpec, day_utc: str) -> list[Path]:
    paths = sorted(path for path in root.rglob(spec.filename) if path.is_file())
    if not spec.day_scoped:
        return paths
    matched = []
    for path in paths:
        payload = _read_light(path)
        if not payload or _matches_day(payload=payload, path=path, day_utc=day_utc):
            matched.append(path)
    return matched


def _read_light(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _matches_day(*, payload: dict[str, Any], path: Path, day_utc: str) -> bool:
    for key in ("day_utc", "date", "period_end"):
        if str(payload.get(key) or "") == day_utc:
            return True
    return day_utc in path.parts


def _artifact_timestamp(payload: dict[str, Any]) -> str:
    for key in ("generated_at_utc", "generated_at", "timestamp_utc", "created_at_utc", "updated_at"):
        value = str(payload.get(key) or "")
        if value:
            return value
    return ""


def _staleness_status(*, timestamp: str, generated_dt: datetime, day_scoped: bool) -> str:
    if not timestamp:
        return "UNKNOWN"
    parsed = _parse_time(timestamp)
    if parsed is None:
        return "UNKNOWN"
    if day_scoped and generated_dt - parsed > STALE_AFTER:
        return "STALE"
    return "CURRENT"


def _parse_time(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _build_lite_status(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    operating = payloads.get("aegis_lite_operating_status", {})
    report = payloads.get("aegis_lite_eod_report", {})
    queue = payloads.get("operator_execution_queue", {})
    manual_packet = payloads.get("manual_trade_packet", {})
    promoted = payloads.get("promoted_sleeve_library", {})
    queue_rows = _objects(queue.get("execution_queue"))
    candidates = _objects(manual_packet.get("trade_candidates"))
    promoted_rows = _objects(promoted.get("promoted_sleeves")) or _objects(promoted.get("sleeves"))
    return {
        "status": "PRESENT" if operating or report or queue else "MISSING",
        "last_eod_run": str(report.get("run_id") or queue.get("run_id") or operating.get("latest_run_id") or ""),
        "canonical_eod_timer": str(operating.get("canonical_eod_timer") or operating.get("lite_eod_timer_status") or report.get("canonical_eod_timer") or "UNKNOWN"),
        "broker_mode": str(operating.get("broker_mode") or report.get("broker_mode") or "UNKNOWN"),
        "ib_automation_status": str(operating.get("ib_automation_status") or report.get("ib_automation_status") or "UNKNOWN"),
        "manual_execution_only": _bool_any(operating, report, manual_packet, key="manual_execution_only"),
        "broker_submit_required": _bool_any(operating, report, manual_packet, key="broker_submit_required"),
        "execution_queue_count": len(queue_rows),
        "execution_queue_ready_count": sum(1 for row in queue_rows if str(row.get("queue_status") or "") == "READY_FOR_MANUAL_ENTRY"),
        "manual_trade_packet_present": bool(manual_packet),
        "manual_trade_candidate_count": len(candidates),
        "manual_packet_actionable_count": sum(1 for row in candidates if _candidate_declares_actionable(row)),
        "blocked_packet_count": sum(1 for row in candidates if not _candidate_declares_actionable(row)),
        "promoted_sleeve_count": len(promoted_rows),
        "all_candidates_traceable_to_promoted_sleeves": bool(manual_packet.get("all_candidates_traceable_to_promoted_sleeves", False)),
    }


def _build_event_status(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    status = payloads.get("event_monitoring_status", {})
    registry = payloads.get("event_rules_registry", {})
    ledger = payloads.get("event_awareness_ledger", {})
    packets = _all_payloads_for_key(payloads, "event_tactical_packet")
    alert_ledger = payloads.get("trade_capture_alert_ledger", {})
    return {
        "status": "PRESENT" if status else "MISSING",
        "monitor_run_id": str(status.get("monitor_run_id") or ""),
        "monitor_enabled_or_running_status": str(status.get("monitor_enabled_status") or status.get("active_status") or "UNKNOWN"),
        "last_monitor_run": str(status.get("timestamp_utc") or status.get("generated_at_utc") or ""),
        "event_rules_registry_version": str(registry.get("registry_version") or registry.get("schema_version") or ""),
        "event_rule_count": len(_objects(registry.get("event_rules"))),
        "event_ledger_status": "PRESENT" if ledger else "MISSING",
        "triggered_event_count": len(_objects(status.get("triggered_events"))),
        "blocked_event_count": len(_objects(status.get("blocked_events"))),
        "actionable_event_packet_count": sum(1 for packet in packets if _event_packet_is_actionable(packet, payloads)),
        "advisory_event_packet_count": sum(1 for packet in packets if not _event_packet_is_actionable(packet, payloads)),
        "alert_transport_status": "GATE_ONLY_NO_TRANSPORT",
        "email_delivery_status": _email_delivery_status(alert_ledger),
        "canonical_eod_state_mutated": bool(status.get("canonical_eod_state_mutated", False)),
    }


def _build_market_context_status(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    snapshot = payloads.get("event_market_snapshot", {})
    monitor = payloads.get("event_monitoring_status", {})
    context = monitor.get("market_context") if isinstance(monitor.get("market_context"), dict) else {}
    source = snapshot or context
    return {
        "status": "PRESENT" if source else "MISSING",
        "snapshot_id": str(source.get("snapshot_id") or ""),
        "generated_at_utc": str(source.get("generated_at_utc") or ""),
        "market_open_status": str(source.get("market_open_status") or "UNKNOWN"),
        "trading_day_type": str(source.get("trading_day_type") or "UNKNOWN"),
        "regime_label": str(source.get("regime_label") or "UNKNOWN"),
        "volatility_classification": str(source.get("volatility_classification") or "UNKNOWN"),
        "breadth_classification": str(source.get("breadth_classification") or "UNKNOWN"),
        "macro_event_today": bool(source.get("macro_event_today", False)),
        "macro_event_type": str(source.get("macro_event_type") or "NONE"),
        "macro_event_risk_level": str(source.get("macro_event_risk_level") or "UNKNOWN"),
        "stale_data_status": str(source.get("stale_data_status") or monitor.get("market_snapshot_freshness_status") or "MISSING"),
        "reason_codes": _strings(source.get("reason_codes")),
    }


def _build_research_status(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    queue = payloads.get("research_task_queue", {})
    tasks = _objects(queue.get("tasks"))
    return {
        "status": "PRESENT" if queue else "MISSING_OR_NOT_CURRENT",
        "open_research_tasks": sum(1 for row in tasks if str(row.get("status") or "").upper() in {"QUEUED", "RUNNING", "BLOCKED"}),
        "blocked_research_tasks": sum(1 for row in tasks if str(row.get("status") or "").upper() == "BLOCKED"),
        "promotion_candidates": sum(1 for row in tasks if "PROMOTION" in str(row.get("task_type") or "").upper()),
        "latest_awareness_report": "",
        "offline_only": True,
        "trade_authorization_allowed": False,
    }


def _build_operator_inbox_status(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    report = payloads.get("operator_inbox_review_report", {})
    return {
        "status": "PRESENT" if report else "MISSING_OR_NOT_CURRENT",
        "captured_open_count": int(report.get("captured_count") or report.get("open_count") or 0),
        "stale_open_count": int(report.get("stale_captured_count") or 0),
        "promoted_count": int(report.get("promoted_count") or 0),
        "rejected_or_archived_count": int(report.get("rejected_count") or 0) + int(report.get("archived_count") or 0),
        "high_priority_open_count": int(report.get("high_priority_open_count") or 0),
        "inbox_items_create_tasks_allowed": False,
        "inbox_items_create_trades_allowed": False,
    }


def _build_sleeve_performance_status(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    report = payloads.get("sleeve_performance_report", {})
    portfolio = report.get("portfolio_summary") if isinstance(report.get("portfolio_summary"), dict) else {}
    sleeves = _objects(report.get("sleeve_summary"))
    return {
        "status": "PRESENT" if report else "MISSING_OR_NOT_CURRENT",
        "latest_report_id": str(report.get("artifact_id") or ""),
        "total_recommended_trades": int(portfolio.get("total_recommended_trades") or 0),
        "total_executed_trades": int(portfolio.get("total_executed_trades") or 0),
        "missing_receipts": int(portfolio.get("missing_receipt_count") or 0),
        "missing_outcomes": int(portfolio.get("missing_outcome_count") or 0),
        "sleeve_return_rows": [
            {
                "sleeve_id": str(row.get("sleeve_id") or ""),
                "total_return": str(row.get("total_return") or ""),
                "average_return": str(row.get("average_return") or ""),
                "win_rate": str(row.get("win_rate") or ""),
            }
            for row in sleeves
        ],
    }


def _build_ai_feedback_status(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    gate = payloads.get("evidence_gate", {})
    review = payloads.get("ai_feedback_review", {})
    return {
        "status": "PRESENT" if review else "MISSING_OR_NOT_CURRENT",
        "latest_review_id": str(review.get("review_id") or ""),
        "evidence_gate_status": str(review.get("evidence_gate_status") or gate.get("gate_status") or "UNKNOWN"),
        "research_tasks_created_or_recommended": len(_objects(review.get("research_tasks_created"))),
        "ai_used": bool(review.get("ai_used", False)),
        "deterministic_fallback_used": bool(review.get("deterministic_fallback_used", not review)),
        "human_review_required": bool(review.get("human_review_required", True)),
        "production_mutation_allowed": bool(review.get("production_mutation", False)),
    }


def _build_dataset_gaps(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    gap = payloads.get("research_dataset_gap", {})
    rows = _objects(gap.get("dataset_gaps"))
    return {
        "status": "PRESENT" if gap else "MISSING_OR_NOT_CURRENT",
        "gaps": [
            {
                "dataset_name": str(row.get("dataset_name") or ""),
                "current_status": str(row.get("current_status") or ""),
                "blocker": str(row.get("blocker") or ""),
                "next_action": str(row.get("next_action") or ""),
            }
            for row in rows
        ],
        "missing_dataset_count": sum(1 for row in rows if str(row.get("current_status") or "").upper() == "MISSING"),
    }


def _classify_actionable_items(payloads: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    actionable: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    manual_packet = payloads.get("manual_trade_packet", {})
    for row in _objects(manual_packet.get("trade_candidates")):
        item = _normalize_trade_item(row=row, source_packet=manual_packet, source_packet_type="EOD_MANUAL_PACKET")
        blockers = _trade_item_blockers(item, payloads=payloads)
        if not blockers and _candidate_declares_actionable(row):
            actionable.append(item)
        else:
            blocked.append({**item, "blockers": sorted(set([*item.get("blockers", []), *blockers]))})
    for packet in _all_payloads_for_key(payloads, "event_tactical_packet"):
        item = _normalize_trade_item(row=packet, source_packet=packet, source_packet_type="EVENT_TACTICAL_PACKET")
        blockers = _trade_item_blockers(item, payloads=payloads)
        if _event_packet_is_actionable(packet, payloads) and not blockers:
            actionable.append(item)
        else:
            blocked.append({**item, "blockers": sorted(set([*item.get("blockers", []), *blockers]))})
    return (
        sorted(actionable, key=lambda row: (str(row.get("source_packet_type")), str(row.get("source_packet_id")), str(row.get("trade_id")))),
        sorted(blocked, key=lambda row: (str(row.get("source_packet_type")), str(row.get("source_packet_id")), str(row.get("trade_id")))),
    )


def _normalize_trade_item(*, row: dict[str, Any], source_packet: dict[str, Any], source_packet_type: str) -> dict[str, Any]:
    runtime_truth = str(row.get("runtime_truth_classification") or source_packet.get("runtime_truth_classification") or "REAL_RUNTIME").upper()
    return {
        "source_packet_type": source_packet_type,
        "source_packet_id": str(source_packet.get("packet_id") or source_packet.get("recommended_trade_id") or row.get("source_packet_id") or row.get("recommended_trade_id") or ""),
        "trade_id": str(row.get("recommended_trade_id") or row.get("candidate_id") or ""),
        "symbol": str(row.get("symbol") or "").upper(),
        "side": str(row.get("side") or "").upper(),
        "quantity_or_sizing_guidance": str(row.get("quantity_or_sizing_guidance") or row.get("quantity") or ""),
        "entry_reference_price": str(row.get("entry_reference_price") or ""),
        "stop_price": str(row.get("stop_price") or ""),
        "stop_logic": str(row.get("stop_logic") or ""),
        "risk_per_trade": str(row.get("risk_per_trade") or ""),
        "valid_until": str(row.get("valid_until") or row.get("valid_until_utc") or ""),
        "validity_gate_status": str(row.get("validity_gate_status") or row.get("gate_status") or ""),
        "alert_gate_status": str(row.get("alert_gate_status") or ""),
        "runtime_truth_classification": runtime_truth,
        "demo_mode": bool(row.get("demo_mode", runtime_truth == "DEMO_ONLY")),
        "dry_run_only": bool(row.get("dry_run_only", runtime_truth == "DRY_RUN_ONLY")),
        "broker_mode_required": "MANUAL_ONLY",
        "blockers": _strings(row.get("do_not_trade_blockers")) + _strings(row.get("blockers")),
    }


def _trade_item_blockers(item: dict[str, Any], *, payloads: dict[str, dict[str, Any]]) -> list[str]:
    blockers = []
    runtime_truth = str(item.get("runtime_truth_classification") or "").upper()
    if runtime_truth != "REAL_RUNTIME":
        blockers.append(f"{runtime_truth}_NOT_ACTIONABLE")
    if item.get("demo_mode"):
        blockers.append("DEMO_ONLY_NOT_ACTIONABLE")
    if item.get("dry_run_only"):
        blockers.append("DRY_RUN_ONLY_NOT_ACTIONABLE")
    for field in sorted(TRADE_REQUIRED_FIELDS):
        if not str(item.get(field) or "").strip():
            blockers.append(f"MISSING_{field.upper()}")
    valid_until = _parse_time(str(item.get("valid_until") or ""))
    if valid_until is not None and valid_until <= datetime.now(UTC):
        blockers.append("VALIDITY_WINDOW_EXPIRED")
    lite = payloads.get("aegis_lite_operating_status", {})
    broker_mode = str(lite.get("broker_mode") or payloads.get("aegis_lite_eod_report", {}).get("broker_mode") or "")
    if broker_mode != "MANUAL_ONLY":
        blockers.append("BROKER_MODE_NOT_MANUAL_ONLY_OR_UNKNOWN")
    return sorted(set(blockers))


def _event_packet_is_actionable(packet: dict[str, Any], payloads: dict[str, dict[str, Any]]) -> bool:
    runtime_truth = str(packet.get("runtime_truth_classification") or "REAL_RUNTIME").upper()
    if runtime_truth != "REAL_RUNTIME" or bool(packet.get("demo_mode")) or bool(packet.get("dry_run_only")):
        return False
    validity = str(packet.get("validity_gate_status") or "").upper()
    if validity and validity != "PASS":
        return False
    gate = payloads.get("trade_capture_alert_gate", {})
    if gate:
        return (
            str(gate.get("source_packet_id") or "") in {str(packet.get("recommended_trade_id") or ""), str(packet.get("packet_id") or "")}
            and str(gate.get("alert_gate_status") or "").upper() in ACTIONABLE_ALERT_STATUSES
        )
    return str(packet.get("alert_gate_status") or "").upper() in ACTIONABLE_ALERT_STATUSES


def _runtime_truth_classification(
    *,
    sources: dict[str, dict[str, Any]],
    payloads: dict[str, dict[str, Any]],
    actionable_items: list[dict[str, Any]],
    blocked_items: list[dict[str, Any]],
) -> str:
    classes = {
        str(payload.get("runtime_truth_classification") or "").upper()
        for payload in payloads.values()
        if payload and str(payload.get("runtime_truth_classification") or "")
    }
    classes.update(str(row.get("runtime_truth_classification") or "").upper() for row in actionable_items + blocked_items if row.get("runtime_truth_classification"))
    if "DEMO_ONLY" in classes:
        return "DEMO_ONLY"
    if "DRY_RUN_ONLY" in classes:
        return "DRY_RUN_ONLY"
    if any(row["required_for_full_context"] and row["status"] != "PRESENT" for row in sources.values()):
        return "PARTIAL_CONTEXT"
    if any(row["required_for_full_context"] and row["staleness_status"] == "STALE" for row in sources.values()):
        return "PARTIAL_CONTEXT"
    lite = payloads.get("aegis_lite_operating_status", {})
    broker_mode = str(lite.get("broker_mode") or payloads.get("aegis_lite_eod_report", {}).get("broker_mode") or "")
    if broker_mode != "MANUAL_ONLY":
        return "ADVISORY_ONLY"
    if actionable_items:
        return "REAL_RUNTIME"
    return "ADVISORY_ONLY"


def _trade_advice_gate(*, runtime_truth_classification: str, actionable_items: list[dict[str, Any]], lite_status: dict[str, Any]) -> dict[str, Any]:
    if runtime_truth_classification != "REAL_RUNTIME":
        return {
            "trade_advice_allowed": False,
            "manual_trade_capture_allowed": False,
            "reason_if_blocked": f"RUNTIME_TRUTH_{runtime_truth_classification}_DOES_NOT_ALLOW_TRADE_ADVICE",
        }
    if not actionable_items:
        return {"trade_advice_allowed": False, "manual_trade_capture_allowed": False, "reason_if_blocked": "NO_CURRENT_ACTIONABLE_ITEM"}
    if str(lite_status.get("broker_mode") or "") != "MANUAL_ONLY":
        return {"trade_advice_allowed": False, "manual_trade_capture_allowed": False, "reason_if_blocked": "BROKER_MODE_NOT_MANUAL_ONLY"}
    if bool(lite_status.get("broker_submit_required")):
        return {"trade_advice_allowed": False, "manual_trade_capture_allowed": False, "reason_if_blocked": "BROKER_SUBMIT_REQUIRED_UNSAFE"}
    return {"trade_advice_allowed": True, "manual_trade_capture_allowed": True, "reason_if_blocked": ""}


def _readiness_state(
    *,
    runtime_truth_classification: str,
    trade_gate: dict[str, Any],
    missing_or_stale: list[dict[str, Any]],
    actionable_items: list[dict[str, Any]],
    blocked_items: list[dict[str, Any]],
) -> dict[str, Any]:
    if trade_gate["manual_trade_capture_allowed"]:
        classification = "READY_FOR_SUPERVISED_MANUAL_CAPTURE_REVIEW"
    elif runtime_truth_classification in {"PARTIAL_CONTEXT", "BLOCKED", "DEMO_ONLY", "DRY_RUN_ONLY"}:
        classification = "ADVISORY_ONLY"
    else:
        classification = "ADVISORY_ONLY"
    return {
        "classification": classification,
        "runtime_truth_classification": runtime_truth_classification,
        "manual_trade_capture_allowed": bool(trade_gate["manual_trade_capture_allowed"]),
        "actionable_item_count": len(actionable_items),
        "blocked_item_count": len(blocked_items),
        "missing_or_stale_source_count": len(missing_or_stale),
        "primary_blocker": str(trade_gate["reason_if_blocked"] or ""),
    }


def _next_operator_actions(*, readiness: dict[str, Any], trade_gate: dict[str, Any], payloads: dict[str, dict[str, Any]]) -> list[str]:
    if trade_gate["manual_trade_capture_allowed"]:
        return [
            "Review the current actionable packet in Aegis Lite before any manual IB paper entry.",
            "Enter at most one supervised paper trade manually, then immediately enter and confirm the protective stop.",
            "Record a manual execution receipt after entry.",
        ]
    perf = payloads.get("sleeve_performance_report", {})
    portfolio = perf.get("portfolio_summary") if isinstance(perf.get("portfolio_summary"), dict) else {}
    if int(portfolio.get("missing_receipt_count") or 0):
        return ["Record missing manual execution receipts before drawing performance conclusions."]
    if int(portfolio.get("missing_outcome_count") or 0):
        return ["Record missing trade outcomes before relying on sleeve performance."]
    if readiness.get("missing_or_stale_source_count"):
        return ["Regenerate missing or stale Aegis Lite/Event/Research artifacts before asking ChatGPT for trade-capture guidance."]
    return ["Use this packet for advisory review only; no current manual trade-capture guidance is allowed."]


def _do_not_claim(
    *,
    sources: dict[str, dict[str, Any]],
    payloads: dict[str, dict[str, Any]],
    actionable_items: list[dict[str, Any]],
    event_status: dict[str, Any],
    ai_status: dict[str, Any],
    dataset_gaps: dict[str, Any],
) -> list[str]:
    claims = ["No broker submit, transmit, or autonomous execution is allowed from this packet."]
    if event_status.get("alert_transport_status") != "LIVE_EMAIL_PROVEN":
        claims.append("Live email/SMS transport is not proven; current alert status is GATE_ONLY_NO_TRANSPORT.")
    if not actionable_items:
        claims.append("No real promoted runtime actionable candidate is proven by this packet.")
    if sources.get("manual_trade_packet", {}).get("status") != "PRESENT":
        claims.append("No current manual trade packet is present.")
    if sources.get("manual_execution_receipt", {}).get("status") == "MISSING":
        claims.append("Real IB paper lifecycle proof is not established by this packet.")
    if int(dataset_gaps.get("missing_dataset_count") or 0) > 0 or dataset_gaps.get("status") != "PRESENT":
        claims.append("Research dataset binding is incomplete or not current.")
    if ai_status.get("deterministic_fallback_used"):
        claims.append("AI feedback is deterministic fallback only unless ai_used=true is present in evidence.")
    if sources.get("event_monitoring_status", {}).get("status") != "PRESENT":
        claims.append("Event monitor is not proven current/enabled by this packet.")
    if any(row["status"] != "PRESENT" for row in sources.values() if row["required_for_full_context"]):
        claims.append("This packet has partial context; ChatGPT must not provide trade advice.")
    return sorted(set(claims))


def _safety_assertions() -> dict[str, Any]:
    return {
        "no_control_packet_no_trade_advice": True,
        "partial_packet_advisory_only": True,
        "demo_or_dry_run_never_actionable": True,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "autonomous_execution_allowed": False,
        "research_artifacts_authorize_trades": False,
        "event_runs_mutate_canonical_eod_state": False,
        "ai_can_mutate_production_logic": False,
        "operator_must_verify_packet_before_manual_entry": True,
    }


def _source_artifacts_used(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for key in sorted(sources):
        row = sources[key]
        if row["status"] != "PRESENT":
            continue
        payload = row.get("payload") or {}
        rows.append(
            {
                "artifact_type": key,
                "path": row["path"],
                "schema_id": str(payload.get("schema_id") or ""),
                "artifact_id": str(payload.get("artifact_id") or ""),
                "timestamp": str(row.get("timestamp") or ""),
                "staleness_status": str(row.get("staleness_status") or ""),
            }
        )
    return rows


def _source_timestamps(sources: dict[str, dict[str, Any]]) -> dict[str, str]:
    return {key: str(sources[key].get("timestamp") or "") for key in sorted(sources)}


def _stale_or_missing_sources(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for key in sorted(sources):
        row = sources[key]
        if row["status"] != "PRESENT" or row["staleness_status"] == "STALE":
            rows.append(
                {
                    "source": key,
                    "filename": str(row.get("filename") or ""),
                    "status": str(row.get("status") or ""),
                    "staleness_status": str(row.get("staleness_status") or ""),
                    "required_for_full_context": bool(row.get("required_for_full_context")),
                    "reason_code": str(row.get("reason_code") or ""),
                }
            )
    return rows


def _all_payloads_for_key(payloads: dict[str, dict[str, Any]], key: str) -> list[dict[str, Any]]:
    payload = payloads.get(key, {})
    return [payload] if payload else []


def _candidate_declares_actionable(row: dict[str, Any]) -> bool:
    if "actionable" in row:
        return bool(row.get("actionable"))
    return str(row.get("queue_status") or row.get("executable_status") or "").upper() in PASS_STATUSES


def _email_delivery_status(alert_ledger: dict[str, Any]) -> str:
    attempts = _objects(alert_ledger.get("alert_attempts"))
    if not attempts:
        return "GATE_ONLY_NO_TRANSPORT"
    statuses = {str(row.get("delivery_status") or "").upper() for row in attempts}
    if "LIVE_EMAIL_SENT" in statuses:
        return "LIVE_EMAIL_PROVEN"
    if "DRY_RUN_MESSAGE_BODY_ONLY" in statuses:
        return "DRY_RUN_MESSAGE_BODY_ONLY"
    return "GATE_ONLY_NO_TRANSPORT"


def _bool_any(*payloads: dict[str, Any], key: str) -> bool:
    for payload in payloads:
        if key in payload:
            return bool(payload.get(key))
    return False


def _objects(value: Any) -> list[dict[str, Any]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [str(value)]
