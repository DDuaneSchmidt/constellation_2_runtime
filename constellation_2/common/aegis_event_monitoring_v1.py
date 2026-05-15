from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_lite_event_awareness_v1 import (
    EVENT_TYPES,
    build_event_alert_v1,
    build_event_awareness_ledger_v1,
    build_event_tactical_packet_v1,
    build_event_validity_gate_v1,
    build_trade_capture_alert_gate_v1,
    build_trade_capture_alert_ledger_v1,
    validate_event_awareness_artifact_v1,
    write_event_awareness_artifact_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVENT_RULES_REGISTRY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "C2_EVENT_RULES_REGISTRY_V1.json"

TACTICAL_REVIEW_STATUSES = {
    "REVIEW_ALLOWED",
    "REVIEW_BLOCKED",
    "RESEARCH_ONLY",
    "DATA_STALE",
    "NO_PROMOTED_SLEEVE",
    "LOW_CONFIDENCE",
}
SEVERITY_RANK = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
CONFIDENCE_RANK = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}


def utc_now_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_event_rules_registry_v1(path: Path | None = None) -> dict[str, Any]:
    registry_path = Path(path or DEFAULT_EVENT_RULES_REGISTRY_PATH).expanduser().resolve()
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    validate_event_awareness_artifact_v1(payload)
    present = {str(row.get("event_type") or "") for row in payload.get("event_rules", []) if isinstance(row, dict)}
    missing = sorted(EVENT_TYPES - present)
    if missing:
        raise ValueError(f"EVENT_RULES_REGISTRY_MISSING_EVENT_TYPES:{','.join(missing)}")
    return payload


def build_tactical_review_gate_v1(
    *,
    gate_id: str,
    event_id: str,
    event_rule: dict[str, Any],
    event_run_id: str,
    day_utc: str,
    evaluated_at_utc: str,
    severity: str,
    confidence: str,
    required_inputs_present: bool,
    stale_data_status: str,
    promoted_sleeve_available: bool,
) -> dict[str, Any]:
    production_status = str(event_rule.get("production_status") or "").strip().upper()
    research_status = str(event_rule.get("research_status") or "").strip().upper()
    review_rule = event_rule.get("tactical_review_rule") if isinstance(event_rule.get("tactical_review_rule"), dict) else {}
    min_severity = str(review_rule.get("min_severity") or "MEDIUM").strip().upper()
    min_confidence = str(review_rule.get("min_confidence") or "MEDIUM").strip().upper()
    requires_sleeve = bool(review_rule.get("requires_promoted_sleeve", True))
    reason_codes: list[str] = []

    if production_status == "RESEARCH_ONLY" or research_status == "RESEARCH_ONLY":
        status = "RESEARCH_ONLY"
        reason_codes.append("EVENT_RULE_RESEARCH_ONLY")
    elif not required_inputs_present or str(stale_data_status).upper() != "FRESH":
        status = "DATA_STALE"
        reason_codes.append("EVENT_DATA_NOT_FRESH_OR_INCOMPLETE")
    elif _rank(severity, SEVERITY_RANK) < _rank(min_severity, SEVERITY_RANK) or _rank(confidence, CONFIDENCE_RANK) < _rank(min_confidence, CONFIDENCE_RANK):
        status = "LOW_CONFIDENCE"
        reason_codes.append("TACTICAL_REVIEW_MINIMUMS_NOT_MET")
    elif requires_sleeve and not promoted_sleeve_available:
        status = "NO_PROMOTED_SLEEVE"
        reason_codes.append("NO_PROMOTED_SLEEVE_FOR_EVENT_RULE")
    elif production_status == "DISABLED":
        status = "REVIEW_BLOCKED"
        reason_codes.append("EVENT_RULE_PRODUCTION_DISABLED")
    else:
        status = "REVIEW_ALLOWED"
        reason_codes.append("TACTICAL_REVIEW_ALLOWED")

    payload = {
        "schema_id": "tactical_review_gate",
        "schema_version": "v1",
        "artifact_id": "tactical_review_gate_v1",
        "gate_id": gate_id,
        "event_id": event_id,
        "event_rule_id": str(event_rule.get("event_rule_id") or ""),
        "event_rule_version": str(event_rule.get("event_rule_version") or ""),
        "event_run_id": event_run_id,
        "day_utc": day_utc,
        "evaluated_at_utc": evaluated_at_utc,
        "gate_status": status,
        "tactical_review_allowed": status == "REVIEW_ALLOWED",
        "reason_codes": sorted(set(reason_codes)),
        "required_inputs_present": bool(required_inputs_present),
        "stale_data_status": str(stale_data_status or "UNKNOWN").strip().upper(),
        "production_status": production_status,
        "research_status": research_status,
        "promoted_sleeve_available": bool(promoted_sleeve_available),
        "canonical_eod_state_mutated": False,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "runtime_mutation_allowed": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_event_monitoring_status_v1(
    *,
    monitor_run_id: str,
    day_utc: str,
    timestamp_utc: str,
    data_snapshot_refs: list[str],
    event_rule_ids_evaluated: list[str],
    thresholds_evaluated: list[str],
    pass_fail_reason_codes: list[str],
    triggered_events: list[str],
    blocked_events: list[str],
    alert_levels: list[str],
    tactical_packets_created: list[str],
    validity_gate_results: list[str],
    alert_gate_results: list[str],
    email_delivery_results: list[str],
    event_awareness_ledger_path: str,
    event_rules_registry_snapshot_path: str,
) -> dict[str, Any]:
    payload = {
        "schema_id": "event_monitoring_status",
        "schema_version": "v1",
        "artifact_id": "event_monitoring_status_v1",
        "monitor_run_id": monitor_run_id,
        "day_utc": day_utc,
        "timestamp_utc": timestamp_utc,
        "data_snapshot_refs": sorted(set(data_snapshot_refs)),
        "event_rule_ids_evaluated": sorted(set(event_rule_ids_evaluated)),
        "thresholds_evaluated": sorted(set(thresholds_evaluated)),
        "pass_fail_reason_codes": sorted(set(pass_fail_reason_codes)),
        "triggered_events": sorted(set(triggered_events)),
        "blocked_events": sorted(set(blocked_events)),
        "alert_levels": sorted(set(alert_levels)),
        "tactical_packets_created": sorted(set(tactical_packets_created)),
        "validity_gate_results": sorted(set(validity_gate_results)),
        "alert_gate_results": sorted(set(alert_gate_results)),
        "email_delivery_results": sorted(set(email_delivery_results)),
        "event_awareness_ledger_path": event_awareness_ledger_path,
        "event_rules_registry_snapshot_path": event_rules_registry_snapshot_path,
        "canonical_eod_state_mutated": False,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "runtime_mutation_allowed": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def run_event_monitor_v1(
    *,
    truth_root: Path,
    day_utc: str,
    market_snapshot: dict[str, Any] | None,
    event_rules_registry_path: Path | None = None,
    monitor_run_id: str = "",
    timestamp_utc: str = "",
    current_price_by_symbol: dict[str, str] | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    ts = timestamp_utc or utc_now_iso_v1()
    run_id = monitor_run_id or f"event-monitor:{day_utc}:{ts}"
    registry = load_event_rules_registry_v1(event_rules_registry_path)
    snapshot = market_snapshot or {}
    inputs = snapshot.get("inputs") if isinstance(snapshot.get("inputs"), dict) else {}
    data_refs = _strings(snapshot.get("data_snapshot_refs")) or _strings(snapshot.get("market_data_snapshot_refs"))
    promoted = _promoted_event_types(snapshot)
    current_prices = current_price_by_symbol or {
        str(k).upper(): str(v)
        for k, v in (snapshot.get("current_prices") if isinstance(snapshot.get("current_prices"), dict) else {}).items()
    }

    registry_snapshot_path = _write_registry_snapshot(root=root, day_utc=day_utc, monitor_run_id=run_id, registry=registry)
    events: list[dict[str, Any]] = []
    tactical_packets: list[dict[str, Any]] = []
    review_gates: list[dict[str, Any]] = []
    validity_gates: list[dict[str, Any]] = []
    alert_gates: list[dict[str, Any]] = []
    alert_ledgers: list[dict[str, Any]] = []
    rule_ids: list[str] = []
    threshold_lines: list[str] = []
    reason_codes: list[str] = []
    triggered: list[str] = []
    blocked: list[str] = []
    alert_levels: list[str] = []
    packet_ids: list[str] = []
    validity_results: list[str] = []
    alert_results: list[str] = []
    email_results: list[str] = []

    for rule in sorted(registry.get("event_rules", []), key=lambda row: str(row.get("event_rule_id") or "")):
        rule_id = str(rule.get("event_rule_id") or "")
        rule_ids.append(rule_id)
        if str(rule.get("enabled_status") or "").upper() != "ENABLED":
            blocked.append(rule_id)
            reason_codes.append(f"{rule_id}:EVENT_RULE_DISABLED")
            continue
        if str(rule.get("event_type") or "") not in EVENT_TYPES:
            blocked.append(rule_id)
            reason_codes.append(f"{rule_id}:UNSUPPORTED_EVENT_TYPE")
            continue
        fresh = _freshness_status(snapshot=snapshot, rule=rule, evaluated_at_utc=ts)
        inputs_present = all(key in inputs for key in _strings(rule.get("required_inputs")))
        threshold_result = _evaluate_thresholds(rule=rule, inputs=inputs)
        threshold_lines.extend(threshold_result["lines"])
        rule_reason_codes = [f"{rule_id}:{code}" for code in threshold_result["reason_codes"]]
        if fresh != "FRESH":
            rule_reason_codes.append(f"{rule_id}:STALE_DATA")
        if not inputs_present:
            rule_reason_codes.append(f"{rule_id}:MISSING_REQUIRED_INPUTS")
        reason_codes.extend(rule_reason_codes)
        if not threshold_result["passed"] or not inputs_present or fresh != "FRESH":
            blocked.append(rule_id)
            events.append(
                build_event_alert_v1(
                    event_id=f"event:{_safe(rule_id)}:{day_utc}:blocked",
                    run_id=run_id,
                    monitor_run_id=run_id,
                    event_rule_id=rule_id,
                    event_rule_version=str(rule.get("event_rule_version") or ""),
                    day_utc=day_utc,
                    timestamp_utc=ts,
                    event_type=str(rule.get("event_type") or ""),
                    alert_level="BLOCKED",
                    severity=str((rule.get("severity_logic") or {}).get("default") or "LOW"),
                    confidence=str((rule.get("confidence_logic") or {}).get("default") or "LOW"),
                    required_inputs_present=inputs_present,
                    stale_data_status=fresh,
                    trigger_conditions=rule.get("trigger_conditions"),
                    trigger_conditions_evaluated=rule.get("trigger_conditions"),
                    thresholds_evaluated=threshold_result["lines"],
                    market_data_snapshot_refs=data_refs,
                    reason_codes=rule_reason_codes,
                    pass_fail_reason_codes=rule_reason_codes,
                    why_it_matters=str(rule.get("behavioral_thesis") or ""),
                    recommended_operator_action="No manual action. Event rule did not pass or data is unavailable.",
                )
            )
            continue

        event_type = str(rule.get("event_type") or "")
        event_id = f"event:{event_type}:{day_utc}:{_safe(run_id)}"
        severity = str((rule.get("severity_logic") or {}).get("default") or "MEDIUM").upper()
        confidence = str((rule.get("confidence_logic") or {}).get("default") or "LOW").upper()
        review_gate = build_tactical_review_gate_v1(
            gate_id=f"tactical-review:{event_id}",
            event_id=event_id,
            event_rule=rule,
            event_run_id=run_id,
            day_utc=day_utc,
            evaluated_at_utc=ts,
            severity=severity,
            confidence=confidence,
            required_inputs_present=True,
            stale_data_status=fresh,
            promoted_sleeve_available=event_type in promoted,
        )
        review_gates.append(review_gate)
        write_event_awareness_artifact_v1(truth_root=root, payload=review_gate)
        tactical_packet_id = ""
        validity_status = "NOT_RUN"
        alert_status = "NOT_RUN"
        email_status = "NOT_SENT"
        packet_created = False
        alert_level = "TACTICAL" if review_gate["gate_status"] == "REVIEW_ALLOWED" else "BLOCKED"
        if review_gate["gate_status"] != "REVIEW_ALLOWED":
            blocked.append(event_id)
        else:
            triggered.append(event_id)
            packet = _packet_from_snapshot(rule=rule, event_id=event_id, event_run_id=run_id, day_utc=day_utc, snapshot=snapshot, timestamp_utc=ts)
            if packet is not None:
                tactical_packets.append(packet)
                packet_path = write_event_awareness_artifact_v1(truth_root=root, payload=packet)
                tactical_packet_id = str(packet.get("recommended_trade_id") or packet_path)
                packet_ids.append(tactical_packet_id)
                packet_created = True
                gate = build_event_validity_gate_v1(
                    gate_id=f"event-validity:{event_id}",
                    event_packet=packet,
                    evaluated_at_utc=ts,
                    current_price=current_prices.get(str(packet.get("symbol") or "").upper(), ""),
                )
                validity_gates.append(gate)
                write_event_awareness_artifact_v1(truth_root=root, payload=gate)
                validity_status = str(gate["gate_status"])
                validity_results.append(f"{event_id}:{validity_status}")
                alert_gate = build_trade_capture_alert_gate_v1(
                    gate_id=f"trade-capture-alert:{event_id}",
                    source_packet=packet,
                    event_validity_gate=gate,
                    evaluated_at_utc=ts,
                    alert_channel="EMAIL",
                    prior_alert_attempts=[],
                )
                alert_gates.append(alert_gate)
                write_event_awareness_artifact_v1(truth_root=root, payload=alert_gate)
                alert_status = str(alert_gate["alert_gate_status"])
                alert_results.append(f"{event_id}:{alert_status}")
                alert_ledger = build_trade_capture_alert_ledger_v1(
                    run_id=f"trade-capture-alert-ledger:{run_id}",
                    day_utc=day_utc,
                    generated_at_utc=ts,
                    alert_gates=[alert_gate],
                )
                alert_ledgers.append(alert_ledger)
                write_event_awareness_artifact_v1(truth_root=root, payload=alert_ledger)
                email_status = "DRY_RUN_MESSAGE_BODY_ONLY" if alert_gate["email_sms_allowed"] else "NOT_SENT"
                email_results.append(f"{event_id}:{email_status}")
                if validity_status == "PASS" and alert_status in {"ACTIONABLE_TRADE", "URGENT_ACTIONABLE_TRADE"}:
                    alert_level = "ACTIONABLE"
        alert_levels.append(alert_level)
        event_reason_codes = _strings(review_gate.get("reason_codes")) + [f"{rule_id}:EVENT_TRIGGERED"]
        events.append(
            build_event_alert_v1(
                event_id=event_id,
                run_id=run_id,
                monitor_run_id=run_id,
                event_rule_id=rule_id,
                event_rule_version=str(rule.get("event_rule_version") or ""),
                day_utc=day_utc,
                timestamp_utc=ts,
                event_type=event_type,
                alert_level=alert_level,
                severity=severity,
                confidence=confidence,
                required_inputs_present=True,
                stale_data_status=fresh,
                assets_affected=_strings(snapshot.get("assets_affected")) or _strings(snapshot.get("symbols")) or ["SPY"],
                trigger_conditions=rule.get("trigger_conditions"),
                trigger_conditions_evaluated=rule.get("trigger_conditions"),
                thresholds_evaluated=threshold_result["lines"],
                market_data_snapshot_refs=data_refs,
                reason_codes=event_reason_codes,
                pass_fail_reason_codes=event_reason_codes,
                why_it_matters=str(rule.get("behavioral_thesis") or ""),
                recommended_operator_action=_operator_action(alert_level),
                tactical_review_requested=review_gate["gate_status"] == "REVIEW_ALLOWED",
                event_packet_created=packet_created,
                tactical_packet_id=tactical_packet_id,
                validity_gate_status=validity_status,
                alert_gate_status=alert_status,
                email_delivery_status=email_status,
            )
        )

    ledger = build_event_awareness_ledger_v1(run_id=run_id, day_utc=day_utc, generated_at_utc=ts, events=events)
    ledger_path = write_event_awareness_artifact_v1(truth_root=root, payload=ledger)
    status = build_event_monitoring_status_v1(
        monitor_run_id=run_id,
        day_utc=day_utc,
        timestamp_utc=ts,
        data_snapshot_refs=data_refs,
        event_rule_ids_evaluated=rule_ids,
        thresholds_evaluated=threshold_lines,
        pass_fail_reason_codes=reason_codes,
        triggered_events=triggered,
        blocked_events=blocked,
        alert_levels=alert_levels,
        tactical_packets_created=packet_ids,
        validity_gate_results=validity_results,
        alert_gate_results=alert_results,
        email_delivery_results=email_results or ["GATE_ONLY_NO_TRANSPORT"],
        event_awareness_ledger_path=str(ledger_path),
        event_rules_registry_snapshot_path=str(registry_snapshot_path),
    )
    status_path = write_event_awareness_artifact_v1(truth_root=root, payload=status)
    return {
        "monitoring_status": status,
        "monitoring_status_path": str(status_path),
        "event_awareness_ledger": ledger,
        "event_awareness_ledger_path": str(ledger_path),
        "event_rules_registry_snapshot_path": str(registry_snapshot_path),
        "tactical_packets": tactical_packets,
        "tactical_review_gates": review_gates,
        "validity_gates": validity_gates,
        "alert_gates": alert_gates,
        "alert_ledgers": alert_ledgers,
        "broker_submit_required": False,
        "canonical_eod_state_mutated": False,
    }


def build_event_monitoring_operator_surface_v1(*, truth_root: Path, day_utc: str, event_rules_registry_path: Path | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    registry = load_event_rules_registry_v1(event_rules_registry_path)
    status = _latest_artifact(root=root, family="event_monitoring_status_v1", day_utc=day_utc, filename="event_monitoring_status.v1.json")
    ledger = _latest_artifact(root=root, family="event_awareness_ledger_v1", day_utc=day_utc, filename="event_awareness_ledger.v1.json")
    packets = _all_artifacts(root=root, family="event_tactical_packet_v1", day_utc=day_utc, filename="event_tactical_packet.v1.json")
    validity_gates = _all_artifacts(root=root, family="event_validity_gate_v1", day_utc=day_utc, filename="event_validity_gate.v1.json")
    alert_gates = _all_artifacts(root=root, family="trade_capture_alert_gate_v1", day_utc=day_utc, filename="trade_capture_alert_gate.v1.json")
    alert_ledgers = _all_artifacts(root=root, family="trade_capture_alert_ledger_v1", day_utc=day_utc, filename="trade_capture_alert_ledger.v1.json")
    classified_packets = _classify_packets(
        packets=[row["payload"] for row in packets if isinstance(row.get("payload"), dict)],
        validity_gates=[row["payload"] for row in validity_gates if isinstance(row.get("payload"), dict)],
        alert_gates=[row["payload"] for row in alert_gates if isinstance(row.get("payload"), dict)],
    )
    return {
        "ok": True,
        "schema_id": "event_monitoring_operator_surface",
        "schema_version": "v1",
        "generated_at_utc": utc_now_iso_v1(),
        "day_utc": day_utc,
        "truth_root": str(root),
        "email_transport_status": "GATE_ONLY_NO_TRANSPORT",
        "sms_transport_status": "GATE_ONLY_NO_TRANSPORT",
        "manual_execution_only": True,
        "broker_submit_required": False,
        "canonical_eod_state_mutated": False,
        "event_rules": sorted(registry.get("event_rules", []), key=lambda row: str(row.get("event_rule_id") or "")),
        "monitor_status": status.get("payload") or _unavailable("event_monitoring_status"),
        "monitor_status_path": status.get("path", ""),
        "event_ledger": ledger.get("payload") or _unavailable("event_awareness_ledger"),
        "event_ledger_path": ledger.get("path", ""),
        "actionable_packets": classified_packets["actionable_packets"],
        "blocked_packets": classified_packets["blocked_packets"],
        "advisory_packets": classified_packets["advisory_packets"],
        "expired_packets": classified_packets["expired_packets"],
        "research_only_packets": classified_packets["research_only_packets"],
        "alert_ledgers": [row["payload"] for row in alert_ledgers if isinstance(row.get("payload"), dict)],
        "operator_workflow": [
            "Normal day: event monitor may record no alerts; canonical EOD remains official.",
            "Unusual event: monitor records rule lineage, tactical review, validity gate, and alert gate results.",
            "Only ACTIONABLE packets with validity PASS may interrupt the operator, and current email/SMS transport is message-body dry-run only.",
            "Any trade is manually entered and later recorded with a manual execution receipt.",
        ],
        "research_learning_boundary": "Event outcomes may create offline Research Lab tasks only; no production mutation or auto-promotion.",
    }


def _packet_from_snapshot(*, rule: dict[str, Any], event_id: str, event_run_id: str, day_utc: str, snapshot: dict[str, Any], timestamp_utc: str) -> dict[str, Any] | None:
    review_rule = rule.get("tactical_review_rule") if isinstance(rule.get("tactical_review_rule"), dict) else {}
    if not bool(review_rule.get("create_packet_when_review_allowed", False)):
        return None
    packet = snapshot.get("tactical_packet") if isinstance(snapshot.get("tactical_packet"), dict) else {}
    event_type = str(rule.get("event_type") or "")
    symbol = str(packet.get("symbol") or (snapshot.get("symbols") or ["SPY"])[0]).upper()
    valid_until = str(packet.get("valid_until") or snapshot.get("valid_until") or "")
    if not valid_until:
        valid_until = timestamp_utc
    return build_event_tactical_packet_v1(
        event_id=event_id,
        event_rule_id=str(rule.get("event_rule_id") or ""),
        event_rule_version=str(rule.get("event_rule_version") or ""),
        event_run_id=event_run_id,
        day_utc=day_utc,
        symbol=symbol,
        side=str(packet.get("side") or "BUY"),
        instrument_type=str(packet.get("instrument_type") or "ETF"),
        entry_reference_price=str(packet.get("entry_reference_price") or snapshot.get("current_prices", {}).get(symbol, "")),
        order_type_suggestion=str(packet.get("order_type_suggestion") or "MANUAL_LIMIT_OR_MARKET_BY_OPERATOR"),
        quantity_or_sizing_guidance=str(packet.get("quantity_or_sizing_guidance") or ""),
        stop_price=str(packet.get("stop_price") or ""),
        stop_logic=str(packet.get("stop_logic") or ""),
        risk_per_trade=str(packet.get("risk_per_trade") or ""),
        event_type=event_type,
        edge_family=str(packet.get("edge_family") or event_type),
        regime_state=str(packet.get("regime_state") or "EVENT_AWARENESS"),
        confidence=str((rule.get("confidence_logic") or {}).get("default") or "LOW"),
        execution_sensitivity=str(packet.get("execution_sensitivity") or "MEDIUM"),
        valid_until=valid_until,
        max_entry_slippage=str(packet.get("max_entry_slippage") or ""),
        invalidation_conditions=packet.get("invalidation_conditions") or [],
        inclusion_reason=str(packet.get("inclusion_reason") or f"{event_type} tactical review passed."),
        exclusion_reason=str(packet.get("exclusion_reason") or ""),
        governance_notes=str(packet.get("governance_notes") or "Non-canonical event packet. Manual execution only."),
        reason_codes=[f"{rule.get('event_rule_id')}:TACTICAL_PACKET_CREATED"],
        event_rule_enabled_status=str(rule.get("enabled_status") or "ENABLED"),
        production_status=str(rule.get("production_status") or "RESEARCH_ONLY"),
        research_status=str(rule.get("research_status") or "RESEARCH_ONLY"),
        runtime_truth_classification=_runtime_truth_classification(snapshot=snapshot, packet=packet),
    )


def _classify_packets(*, packets: list[dict[str, Any]], validity_gates: list[dict[str, Any]], alert_gates: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    validity_by_event = _latest_by(validity_gates, "event_id")
    alert_by_event = _latest_by(alert_gates, "event_id")
    alert_by_source = _latest_by(alert_gates, "source_packet_id")
    out = {
        "actionable_packets": [],
        "blocked_packets": [],
        "advisory_packets": [],
        "expired_packets": [],
        "research_only_packets": [],
    }
    for packet in sorted(packets, key=lambda row: (str(row.get("event_id") or ""), str(row.get("recommended_trade_id") or ""))):
        event_id = str(packet.get("event_id") or "")
        source_id = str(packet.get("recommended_trade_id") or "")
        validity = validity_by_event.get(event_id, {})
        alert = alert_by_source.get(source_id) or alert_by_event.get(event_id, {})
        enriched = {
            **packet,
            "validity_gate_status": str(validity.get("gate_status") or "NOT_RUN"),
            "validity_gate_blockers": _strings(validity.get("blockers")),
            "alert_gate_status": str(alert.get("alert_gate_status") or "NOT_RUN"),
            "alert_gate_blockers": _strings(alert.get("blockers")),
        }
        runtime_class = str(packet.get("runtime_truth_classification") or "REAL_RUNTIME").strip().upper()
        if str(packet.get("production_status") or "").upper() == "RESEARCH_ONLY" or str(packet.get("research_status") or "").upper() == "RESEARCH_ONLY":
            out["research_only_packets"].append(enriched)
        elif runtime_class in {"DEMO_ONLY", "DRY_RUN_ONLY"}:
            out["blocked_packets"].append(enriched)
        elif enriched["validity_gate_status"] == "PASS" and enriched["alert_gate_status"] in {"PASS", "ACTIONABLE_TRADE", "URGENT_ACTIONABLE_TRADE"}:
            out["actionable_packets"].append(enriched)
        elif enriched["alert_gate_status"] in {"EXPIRED", "MISSED_VALIDITY_WINDOW"} or "EVENT_PACKET_STALE_BEYOND_VALID_UNTIL" in enriched["validity_gate_blockers"]:
            out["expired_packets"].append(enriched)
        elif enriched["validity_gate_status"] == "BLOCKED" or enriched["alert_gate_status"] in {"BLOCKED", "INVALID"}:
            out["blocked_packets"].append(enriched)
        else:
            out["advisory_packets"].append(enriched)
    return out


def _latest_by(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        value = str(row.get(key) or "")
        if value:
            indexed[value] = row
    return indexed


def _runtime_truth_classification(*, snapshot: dict[str, Any], packet: dict[str, Any]) -> str:
    raw = str(packet.get("runtime_truth_classification") or snapshot.get("runtime_truth_classification") or "").strip().upper()
    if raw:
        return raw
    if bool(packet.get("demo_mode", False)) or bool(snapshot.get("demo_mode", False)):
        return "DEMO_ONLY"
    if bool(packet.get("dry_run_only", False)) or bool(snapshot.get("dry_run_only", False)):
        return "DRY_RUN_ONLY"
    return "REAL_RUNTIME"


def _write_registry_snapshot(*, root: Path, day_utc: str, monitor_run_id: str, registry: dict[str, Any]) -> Path:
    path = root / "reports" / "event_rules_registry_v1" / day_utc / _safe(monitor_run_id) / "event_rules_registry.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(registry) + b"\n")
    return path


def _evaluate_thresholds(*, rule: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
    lines: list[str] = []
    reason_codes: list[str] = []
    passed = True
    for threshold in rule.get("thresholds", []):
        if not isinstance(threshold, dict):
            continue
        key = str(threshold.get("input_key") or "")
        operator = str(threshold.get("operator") or "")
        expected = threshold.get("value")
        actual = inputs.get(key)
        ok = _compare(actual, operator, expected)
        if not ok:
            passed = False
            reason_codes.append(f"THRESHOLD_FAILED:{key}")
        lines.append(f"{key} {operator} {expected} actual={actual} result={'PASS' if ok else 'FAIL'}")
    if passed:
        reason_codes.append("THRESHOLDS_PASS")
    return {"passed": passed, "lines": lines, "reason_codes": reason_codes}


def _compare(actual: Any, operator: str, expected: Any) -> bool:
    if isinstance(expected, bool):
        lhs = bool(actual)
        rhs = expected
    else:
        lhs_num = _number(actual)
        rhs_num = _number(expected)
        if lhs_num is not None and rhs_num is not None:
            lhs = lhs_num
            rhs = rhs_num
        else:
            lhs = str(actual)
            rhs = str(expected)
    if operator == ">":
        return lhs > rhs
    if operator == ">=":
        return lhs >= rhs
    if operator == "<":
        return lhs < rhs
    if operator == "<=":
        return lhs <= rhs
    if operator == "==":
        return lhs == rhs
    if operator == "!=":
        return lhs != rhs
    return False


def _freshness_status(*, snapshot: dict[str, Any], rule: dict[str, Any], evaluated_at_utc: str) -> str:
    generated = str(snapshot.get("generated_at_utc") or "")
    if not generated:
        return "MISSING_TIMESTAMP"
    max_age = int((rule.get("stale_data_rules") or {}).get("max_snapshot_age_minutes") or 30)
    try:
        age = _parse_utc(evaluated_at_utc) - _parse_utc(generated)
    except ValueError:
        return "UNPARSEABLE_TIMESTAMP"
    return "FRESH" if age.total_seconds() <= max_age * 60 else "STALE"


def _promoted_event_types(snapshot: dict[str, Any]) -> set[str]:
    promoted: set[str] = set()
    for row in snapshot.get("promoted_sleeves", []):
        if not isinstance(row, dict):
            continue
        event_type = str(row.get("event_type") or row.get("edge_family") or "").strip().upper()
        if event_type:
            promoted.add(event_type)
    return promoted


def _latest_artifact(*, root: Path, family: str, day_utc: str, filename: str) -> dict[str, Any]:
    candidates = sorted((root / "reports" / family / day_utc).rglob(filename)) if (root / "reports" / family / day_utc).exists() else []
    if not candidates:
        return {"path": "", "payload": None, "error": "ARTIFACT_MISSING"}
    path = candidates[-1]
    try:
        return {"path": str(path), "payload": json.loads(path.read_text(encoding="utf-8")), "error": ""}
    except (OSError, json.JSONDecodeError) as exc:
        return {"path": str(path), "payload": None, "error": f"{type(exc).__name__}:{exc}"}


def _all_artifacts(*, root: Path, family: str, day_utc: str, filename: str) -> list[dict[str, Any]]:
    base = root / "reports" / family / day_utc
    if not base.exists():
        return []
    return [_latest_json(path) for path in sorted(base.rglob(filename))]


def _latest_json(path: Path) -> dict[str, Any]:
    try:
        return {"path": str(path), "payload": json.loads(path.read_text(encoding="utf-8")), "error": ""}
    except (OSError, json.JSONDecodeError) as exc:
        return {"path": str(path), "payload": None, "error": f"{type(exc).__name__}:{exc}"}


def _unavailable(label: str) -> dict[str, Any]:
    return {"status": "UNAVAILABLE", "reason_codes": [f"{label.upper()}_MISSING"]}


def _operator_action(alert_level: str) -> str:
    if alert_level == "ACTIONABLE":
        return "Review event tactical packet; manually act only if still valid and supervised."
    if alert_level == "TACTICAL":
        return "Review conditions manually; no operator interruption was authorized."
    return "No manual action."


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _rank(value: str, ranks: dict[str, int]) -> int:
    return ranks.get(str(value or "").strip().upper(), 0)


def _number(value: Any) -> float | None:
    try:
        return float(str(value).strip().replace("%", ""))
    except (TypeError, ValueError):
        return None


def _parse_utc(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _safe(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip()) or "event_monitor"
