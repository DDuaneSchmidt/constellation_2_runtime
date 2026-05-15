from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
EVENT_AWARENESS_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/event_awareness_ledger.v1.schema.json"
EVENT_TACTICAL_PACKET_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/event_tactical_packet.v1.schema.json"
EVENT_VALIDITY_GATE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/event_validity_gate.v1.schema.json"
TRADE_CAPTURE_ALERT_GATE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_capture_alert_gate.v1.schema.json"
TRADE_CAPTURE_ALERT_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_capture_alert_ledger.v1.schema.json"
EVENT_RULES_REGISTRY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/event_rules_registry.v1.schema.json"
EVENT_MONITORING_STATUS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/event_monitoring_status.v1.schema.json"
TACTICAL_REVIEW_GATE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/tactical_review_gate.v1.schema.json"

EVENT_TYPES = {
    "PANIC_EXHAUSTION",
    "RECOVERY_FAILURE",
    "FAILED_BREAKOUT",
    "BREADTH_COLLAPSE",
    "DEFENSIVE_ROTATION",
    "VOLATILITY_SPIKE",
    "MACRO_EVENT_REACTION",
}
ALERT_LEVELS = {"INFO", "WATCH", "TACTICAL", "ACTIONABLE", "BLOCKED"}
EXECUTION_SENSITIVITY = {"LOW", "MEDIUM", "HIGH", "EXTREME"}
ALERT_GATE_STATES = {
    "ACTIONABLE_TRADE",
    "URGENT_ACTIONABLE_TRADE",
    "INFO",
    "WATCH",
    "TACTICAL",
    "BLOCKED",
    "EXPIRED",
    "INVALID",
    "MISSED_VALIDITY_WINDOW",
}
ALERT_CHANNELS = {"SMS", "EMAIL"}
MIN_SECONDS_REMAINING = {"LOW": 30 * 60, "MEDIUM": 15 * 60, "HIGH": 5 * 60}
RUNTIME_TRUTH_CLASSIFICATIONS = {"REAL_RUNTIME", "DEMO_ONLY", "DRY_RUN_ONLY"}


def event_artifact_path_v1(*, truth_root: Path, artifact_id: str, day_utc: str, identifier: str) -> Path:
    filename = f"{artifact_id.replace('_v1', '')}.v1.json"
    return Path(truth_root).resolve() / "reports" / artifact_id / day_utc / _safe_id(identifier) / filename


def validate_event_awareness_artifact_v1(payload: dict[str, Any]) -> None:
    schema_id = str(payload.get("schema_id") or "")
    relpath = {
        "event_awareness_ledger": EVENT_AWARENESS_LEDGER_SCHEMA,
        "event_tactical_packet": EVENT_TACTICAL_PACKET_SCHEMA,
        "event_validity_gate": EVENT_VALIDITY_GATE_SCHEMA,
        "trade_capture_alert_gate": TRADE_CAPTURE_ALERT_GATE_SCHEMA,
        "trade_capture_alert_ledger": TRADE_CAPTURE_ALERT_LEDGER_SCHEMA,
        "event_rules_registry": EVENT_RULES_REGISTRY_SCHEMA,
        "event_monitoring_status": EVENT_MONITORING_STATUS_SCHEMA,
        "tactical_review_gate": TACTICAL_REVIEW_GATE_SCHEMA,
    }.get(schema_id)
    if not relpath:
        raise ValueError(f"UNSUPPORTED_AEGIS_LITE_EVENT_SCHEMA:{schema_id}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, relpath)


def write_event_awareness_artifact_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_event_awareness_artifact_v1(payload)
    identifier = str(payload.get("run_id") or payload.get("monitor_run_id") or payload.get("gate_id") or payload.get("event_run_id") or payload.get("event_id") or "index")
    path = event_artifact_path_v1(
        truth_root=truth_root,
        artifact_id=str(payload["artifact_id"]),
        day_utc=str(payload["day_utc"]),
        identifier=identifier,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_event_alert_v1(
    *,
    event_id: str,
    run_id: str,
    day_utc: str,
    timestamp_utc: str,
    event_type: str,
    alert_level: str,
    severity: str,
    confidence: str,
    monitor_run_id: str = "",
    event_rule_id: str = "",
    event_rule_version: str = "",
    assets_affected: list[str] | str | None = None,
    trigger_conditions: list[str] | str | None = None,
    trigger_conditions_evaluated: list[str] | str | None = None,
    thresholds_evaluated: list[str] | str | None = None,
    market_data_snapshot_refs: list[str] | str | None = None,
    reason_codes: list[str] | str | None = None,
    pass_fail_reason_codes: list[str] | str | None = None,
    why_it_matters: str = "",
    recommended_operator_action: str = "",
    required_inputs_present: bool = True,
    stale_data_status: str = "UNKNOWN",
    tactical_review_requested: bool = False,
    event_packet_created: bool = False,
    tactical_packet_id: str = "",
    validity_gate_status: str = "NOT_RUN",
    alert_gate_status: str = "NOT_RUN",
    email_delivery_status: str = "NOT_SENT",
) -> dict[str, Any]:
    level = _enum(alert_level, ALERT_LEVELS, "alert_level")
    gate_status = str(validity_gate_status or "NOT_RUN").strip().upper()
    if level == "ACTIONABLE" and gate_status != "PASS":
        raise ValueError("ACTIONABLE_EVENT_REQUIRES_VALIDITY_GATE_PASS")
    if level in {"INFO", "WATCH"} and event_packet_created:
        raise ValueError("INFO_WATCH_EVENTS_CANNOT_CREATE_ACTIONABLE_PACKET")
    return {
        "event_id": event_id,
        "run_id": run_id,
        "monitor_run_id": monitor_run_id or run_id,
        "event_rule_id": event_rule_id or "UNSPECIFIED_EVENT_RULE",
        "event_rule_version": event_rule_version or "v1",
        "day_utc": day_utc,
        "timestamp_utc": timestamp_utc,
        "event_type": _enum(event_type, EVENT_TYPES, "event_type"),
        "alert_level": level,
        "severity": severity,
        "confidence": confidence,
        "required_inputs_present": bool(required_inputs_present),
        "stale_data_status": str(stale_data_status or "UNKNOWN").strip().upper(),
        "assets_affected": _strings(assets_affected),
        "trigger_conditions": _strings(trigger_conditions),
        "trigger_conditions_evaluated": _strings(trigger_conditions_evaluated),
        "thresholds_evaluated": _strings(thresholds_evaluated),
        "market_data_snapshot_refs": _strings(market_data_snapshot_refs),
        "reason_codes": _strings(reason_codes),
        "pass_fail_reason_codes": _strings(pass_fail_reason_codes or reason_codes),
        "why_it_matters": why_it_matters,
        "recommended_operator_action": recommended_operator_action,
        "tactical_review_requested": bool(tactical_review_requested),
        "event_packet_created": bool(event_packet_created),
        "tactical_packet_id": tactical_packet_id,
        "validity_gate_status": gate_status,
        "alert_gate_status": str(alert_gate_status or "NOT_RUN").strip().upper(),
        "email_delivery_status": str(email_delivery_status or "NOT_SENT").strip().upper(),
        "canonical_eod_state_mutated": False,
    }


def build_event_awareness_ledger_v1(*, run_id: str, day_utc: str, generated_at_utc: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = sorted([_event_alert(row) for row in events], key=lambda row: (row["timestamp_utc"], row["event_id"]))
    payload = {
        "schema_id": "event_awareness_ledger",
        "schema_version": "v1",
        "artifact_id": "event_awareness_ledger_v1",
        "run_id": run_id,
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc,
        "events": normalized,
        "non_canonical_event_layer": True,
        "canonical_eod_state_mutated": False,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "runtime_mutation_allowed": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_event_tactical_packet_v1(
    *,
    event_id: str,
    event_run_id: str,
    day_utc: str,
    symbol: str,
    side: str,
    instrument_type: str,
    entry_reference_price: str,
    order_type_suggestion: str,
    quantity_or_sizing_guidance: str,
    stop_price: str,
    stop_logic: str,
    risk_per_trade: str,
    event_type: str,
    edge_family: str,
    regime_state: str,
    confidence: str,
    execution_sensitivity: str,
    valid_until: str,
    max_entry_slippage: str,
    invalidation_conditions: list[str] | str | None,
    inclusion_reason: str,
    event_rule_id: str = "UNSPECIFIED_EVENT_RULE",
    event_rule_version: str = "v1",
    exclusion_reason: str = "",
    governance_notes: str = "",
    manual_execution_checklist: list[str] | str | None = None,
    recommended_trade_id: str = "",
    reason_codes: list[str] | str | None = None,
    event_rule_enabled_status: str = "ENABLED",
    production_status: str = "MANUAL_PAPER_ELIGIBLE",
    research_status: str = "VALIDATED_RESEARCH",
    runtime_truth_classification: str = "REAL_RUNTIME",
) -> dict[str, Any]:
    runtime_class = _enum(runtime_truth_classification, RUNTIME_TRUTH_CLASSIFICATIONS, "runtime_truth_classification")
    payload = {
        "schema_id": "event_tactical_packet",
        "schema_version": "v1",
        "artifact_id": "event_tactical_packet_v1",
        "event_id": event_id,
        "event_rule_id": event_rule_id or "UNSPECIFIED_EVENT_RULE",
        "event_rule_version": event_rule_version or "v1",
        "event_run_id": event_run_id,
        "day_utc": day_utc,
        "recommended_trade_id": recommended_trade_id or f"event:{event_id}:{symbol.upper()}:{side.upper()}",
        "symbol": symbol.upper(),
        "side": side.upper(),
        "instrument_type": instrument_type,
        "entry_reference_price": entry_reference_price,
        "order_type_suggestion": order_type_suggestion,
        "quantity_or_sizing_guidance": quantity_or_sizing_guidance,
        "stop_price": stop_price,
        "stop_logic": stop_logic,
        "risk_per_trade": risk_per_trade,
        "event_type": _enum(event_type, EVENT_TYPES, "event_type"),
        "edge_family": edge_family,
        "regime_state": regime_state,
        "confidence": confidence,
        "execution_sensitivity": _enum(execution_sensitivity, EXECUTION_SENSITIVITY, "execution_sensitivity"),
        "valid_until": valid_until,
        "max_entry_slippage": max_entry_slippage,
        "invalidation_conditions": _strings(invalidation_conditions),
        "inclusion_reason": inclusion_reason,
        "exclusion_reason": exclusion_reason,
        "reason_codes": _strings(reason_codes),
        "governance_notes": governance_notes,
        "runtime_truth_classification": runtime_class,
        "demo_mode": runtime_class == "DEMO_ONLY",
        "dry_run_only": runtime_class == "DRY_RUN_ONLY",
        "event_rule_enabled_status": str(event_rule_enabled_status or "ENABLED").strip().upper(),
        "production_status": str(production_status or "MANUAL_PAPER_ELIGIBLE").strip().upper(),
        "research_status": str(research_status or "VALIDATED_RESEARCH").strip().upper(),
        "manual_execution_checklist": _strings(manual_execution_checklist)
        or [
            "Review validity gate status before acting.",
            "Enter manually only if still within valid_until and max_entry_slippage.",
            "Immediately enter protective stop.",
            "Record manual execution receipt with event references.",
        ],
        "non_canonical_event_packet": True,
        "canonical_eod_state_mutated": False,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "runtime_mutation_allowed": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_event_validity_gate_v1(
    *,
    gate_id: str,
    event_packet: dict[str, Any],
    evaluated_at_utc: str,
    current_price: str = "",
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    required = [
        "valid_until",
        "max_entry_slippage",
        "execution_sensitivity",
        "invalidation_conditions",
        "stop_price",
        "stop_logic",
        "risk_per_trade",
        "entry_reference_price",
        "quantity_or_sizing_guidance",
    ]
    for field in required:
        value = event_packet.get(field)
        if value in (None, "", []):
            blockers.append(f"MISSING_{field.upper()}")
    if str(event_packet.get("event_rule_enabled_status") or "ENABLED").strip().upper() == "DISABLED":
        blockers.append("EVENT_RULE_DISABLED")
    if str(event_packet.get("production_status") or "").strip().upper() == "RESEARCH_ONLY":
        blockers.append("RESEARCH_ONLY_EVENT_NOT_ACTIONABLE")
    if str(event_packet.get("research_status") or "").strip().upper() == "RESEARCH_ONLY":
        blockers.append("RESEARCH_ONLY_EVENT_NOT_ACTIONABLE")
    runtime_class = str(event_packet.get("runtime_truth_classification") or "REAL_RUNTIME").strip().upper()
    if runtime_class == "DEMO_ONLY":
        blockers.append("DEMO_ONLY_EVENT_PACKET_NOT_ACTIONABLE")
    elif runtime_class == "DRY_RUN_ONLY":
        blockers.append("DRY_RUN_ONLY_EVENT_PACKET_NOT_ACTIONABLE")
    elif runtime_class != "REAL_RUNTIME":
        blockers.append(f"UNSUPPORTED_RUNTIME_TRUTH_CLASSIFICATION:{runtime_class}")
    sensitivity = str(event_packet.get("execution_sensitivity") or "").strip().upper()
    if sensitivity == "EXTREME":
        blockers.append("EXECUTION_SENSITIVITY_EXTREME_NOT_MANUAL_CAPTURE_SAFE")
    elif sensitivity == "HIGH":
        warnings.append("HIGH_EXECUTION_SENSITIVITY_REQUIRES_OPERATOR_SPEED_WARNING")
    elif sensitivity and sensitivity not in {"LOW", "MEDIUM", "HIGH"}:
        blockers.append(f"UNSUPPORTED_EXECUTION_SENSITIVITY:{sensitivity}")
    if event_packet.get("valid_until") and _parse_utc(evaluated_at_utc) > _parse_utc(str(event_packet["valid_until"])):
        blockers.append("EVENT_PACKET_STALE_BEYOND_VALID_UNTIL")
    if current_price:
        entry = _number(event_packet.get("entry_reference_price"))
        max_slip = _slippage_value(str(event_packet.get("max_entry_slippage") or ""), entry)
        now_price = _number(current_price)
        if entry is None or max_slip is None or now_price is None:
            blockers.append("CURRENT_PRICE_SLIPPAGE_UNPARSEABLE")
        elif abs(now_price - entry) > max_slip:
            blockers.append("CURRENT_PRICE_OUTSIDE_MAX_ENTRY_SLIPPAGE")
    status = "BLOCKED" if blockers else "PASS"
    payload = {
        "schema_id": "event_validity_gate",
        "schema_version": "v1",
        "artifact_id": "event_validity_gate_v1",
        "gate_id": gate_id,
        "event_id": str(event_packet.get("event_id") or ""),
        "event_rule_id": str(event_packet.get("event_rule_id") or "UNSPECIFIED_EVENT_RULE"),
        "event_rule_version": str(event_packet.get("event_rule_version") or "v1"),
        "event_run_id": str(event_packet.get("event_run_id") or ""),
        "day_utc": str(event_packet.get("day_utc") or ""),
        "evaluated_at_utc": evaluated_at_utc,
        "current_price": current_price,
        "runtime_truth_classification": runtime_class,
        "gate_status": status,
        "actionable_allowed": status == "PASS",
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "execution_sensitivity": sensitivity,
        "valid_until": str(event_packet.get("valid_until") or ""),
        "max_entry_slippage": str(event_packet.get("max_entry_slippage") or ""),
        "canonical_eod_state_mutated": False,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "runtime_mutation_allowed": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_trade_capture_alert_gate_v1(
    *,
    gate_id: str,
    source_packet: dict[str, Any] | None,
    event_validity_gate: dict[str, Any] | None,
    evaluated_at_utc: str,
    alert_channel: str = "SMS",
    prior_alert_attempts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    channel = _enum(alert_channel, ALERT_CHANNELS, "alert_channel")
    packet = source_packet or {}
    validity = event_validity_gate or {}
    blockers: list[str] = []
    reason_codes: list[str] = []
    if not packet:
        blockers.append("SOURCE_PACKET_MISSING")
    if str(validity.get("gate_status") or "") != "PASS":
        blockers.append("EVENT_VALIDITY_GATE_NOT_PASS")
    required = [
        "symbol",
        "side",
        "entry_reference_price",
        "quantity_or_sizing_guidance",
        "risk_per_trade",
        "max_entry_slippage",
        "valid_until",
    ]
    for field in required:
        if packet.get(field) in (None, "", []):
            blockers.append(f"MISSING_{field.upper()}")
    if packet.get("stop_price") in (None, "", []) and packet.get("stop_logic") in (None, "", []):
        blockers.append("MISSING_STOP_PRICE_OR_STOP_LOGIC")
    runtime_class = str(packet.get("runtime_truth_classification") or "REAL_RUNTIME").strip().upper()
    if runtime_class == "DEMO_ONLY":
        blockers.append("DEMO_ONLY_EVENT_PACKET_NOT_ALERTABLE")
    elif runtime_class == "DRY_RUN_ONLY":
        blockers.append("DRY_RUN_ONLY_EVENT_PACKET_NOT_ALERTABLE")
    elif runtime_class != "REAL_RUNTIME":
        blockers.append(f"UNSUPPORTED_RUNTIME_TRUTH_CLASSIFICATION:{runtime_class}")
    sensitivity = str(packet.get("execution_sensitivity") or validity.get("execution_sensitivity") or "").strip().upper()
    if sensitivity == "EXTREME":
        blockers.append("EXECUTION_SENSITIVITY_EXTREME_NOT_ALERTABLE")
    elif sensitivity not in MIN_SECONDS_REMAINING:
        blockers.append(f"UNSUPPORTED_EXECUTION_SENSITIVITY:{sensitivity or 'MISSING'}")
    valid_until = str(packet.get("valid_until") or validity.get("valid_until") or "")
    seconds_remaining = 0
    min_required = MIN_SECONDS_REMAINING.get(sensitivity, 0)
    if valid_until:
        seconds_remaining = int((_parse_utc(valid_until) - _parse_utc(evaluated_at_utc)).total_seconds())
        if seconds_remaining < 0:
            blockers.append("ALERT_PACKET_EXPIRED")
        elif sensitivity in MIN_SECONDS_REMAINING and seconds_remaining < min_required:
            blockers.append("INSUFFICIENT_TIME_REMAINING")
    source_packet_id = str(packet.get("recommended_trade_id") or packet.get("packet_id") or packet.get("event_id") or "")
    if not source_packet_id:
        blockers.append("SOURCE_PACKET_ID_MISSING")
    material_hash = _material_packet_hash(packet)
    duplicate_suppressed = _duplicate_suppressed(
        prior_alert_attempts=prior_alert_attempts or [],
        source_packet_id=source_packet_id,
        alert_channel=channel,
        material_hash=material_hash,
    )
    if duplicate_suppressed:
        blockers.append("DUPLICATE_ALERT_SUPPRESSED")
    if blockers:
        if "ALERT_PACKET_EXPIRED" in blockers:
            status = "EXPIRED"
        elif "INSUFFICIENT_TIME_REMAINING" in blockers:
            status = "MISSED_VALIDITY_WINDOW"
        elif "EVENT_VALIDITY_GATE_NOT_PASS" in blockers or "DUPLICATE_ALERT_SUPPRESSED" in blockers:
            status = "BLOCKED"
        else:
            status = "INVALID"
    else:
        status = "URGENT_ACTIONABLE_TRADE" if sensitivity == "HIGH" else "ACTIONABLE_TRADE"
        reason_codes.append("TRADE_CAPTURE_ALERT_ALLOWED")
    if blockers:
        reason_codes.extend(blockers)
    sms_body = _sms_body(packet=packet, valid_until_et=_display_et(valid_until))
    email_body = _email_body(packet=packet, valid_until_et=_display_et(valid_until), reason_codes=reason_codes)
    payload = {
        "schema_id": "trade_capture_alert_gate",
        "schema_version": "v1",
        "artifact_id": "trade_capture_alert_gate_v1",
        "gate_id": gate_id,
        "event_id": str(packet.get("event_id") or validity.get("event_id") or ""),
        "event_run_id": str(packet.get("event_run_id") or validity.get("event_run_id") or ""),
        "day_utc": str(packet.get("day_utc") or validity.get("day_utc") or ""),
        "source_packet_id": source_packet_id,
        "source_packet_type": "EVENT_TACTICAL_PACKET" if str(packet.get("schema_id") or "") == "event_tactical_packet" else str(packet.get("source_packet_type") or "EVENT_TACTICAL_PACKET"),
        "evaluated_at_utc": evaluated_at_utc,
        "alert_channel": channel,
        "runtime_truth_classification": runtime_class,
        "alert_gate_status": status,
        "email_sms_allowed": status in {"ACTIONABLE_TRADE", "URGENT_ACTIONABLE_TRADE"},
        "validity_gate_status": str(validity.get("gate_status") or "MISSING"),
        "execution_sensitivity": sensitivity,
        "valid_until_utc": valid_until,
        "valid_until_display_et": _display_et(valid_until),
        "seconds_remaining_at_send": seconds_remaining,
        "minimum_seconds_required": min_required,
        "duplicate_suppressed": duplicate_suppressed,
        "blockers": sorted(set(blockers)),
        "reason_codes": sorted(set(reason_codes)),
        "no_alert_reason": ";".join(sorted(set(blockers))),
        "material_packet_hash": material_hash,
        "sms_body": sms_body,
        "email_subject": f"AEGIS ACTIONABLE TRADE {packet.get('symbol', '')} {packet.get('side', '')}".strip(),
        "email_body": email_body,
        "canonical_eod_state_mutated": False,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "runtime_mutation_allowed": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_trade_capture_alert_ledger_v1(
    *,
    run_id: str,
    day_utc: str,
    generated_at_utc: str,
    alert_gates: list[dict[str, Any]],
    prior_alert_attempts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    attempts = [dict(row) for row in prior_alert_attempts or []]
    attempts.extend(_alert_attempt_from_gate(gate) for gate in alert_gates)
    payload = {
        "schema_id": "trade_capture_alert_ledger",
        "schema_version": "v1",
        "artifact_id": "trade_capture_alert_ledger_v1",
        "run_id": run_id,
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc,
        "alert_attempts": sorted(attempts, key=lambda row: (row["sent_at_utc"], row["alert_id"])),
        "canonical_eod_state_mutated": False,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "runtime_mutation_allowed": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _event_alert(row: dict[str, Any]) -> dict[str, Any]:
    return build_event_alert_v1(
        event_id=str(row.get("event_id") or ""),
        run_id=str(row.get("run_id") or ""),
        day_utc=str(row.get("day_utc") or ""),
        timestamp_utc=str(row.get("timestamp_utc") or ""),
        event_type=str(row.get("event_type") or ""),
        alert_level=str(row.get("alert_level") or "INFO"),
        severity=str(row.get("severity") or ""),
        confidence=str(row.get("confidence") or ""),
        monitor_run_id=str(row.get("monitor_run_id") or row.get("run_id") or ""),
        event_rule_id=str(row.get("event_rule_id") or "UNSPECIFIED_EVENT_RULE"),
        event_rule_version=str(row.get("event_rule_version") or "v1"),
        assets_affected=_strings(row.get("assets_affected")),
        trigger_conditions=_strings(row.get("trigger_conditions")),
        trigger_conditions_evaluated=_strings(row.get("trigger_conditions_evaluated")),
        thresholds_evaluated=_strings(row.get("thresholds_evaluated")),
        market_data_snapshot_refs=_strings(row.get("market_data_snapshot_refs")),
        reason_codes=_strings(row.get("reason_codes")),
        pass_fail_reason_codes=_strings(row.get("pass_fail_reason_codes") or row.get("reason_codes")),
        why_it_matters=str(row.get("why_it_matters") or ""),
        recommended_operator_action=str(row.get("recommended_operator_action") or ""),
        required_inputs_present=bool(row.get("required_inputs_present", True)),
        stale_data_status=str(row.get("stale_data_status") or "UNKNOWN"),
        tactical_review_requested=bool(row.get("tactical_review_requested", False)),
        event_packet_created=bool(row.get("event_packet_created", False)),
        tactical_packet_id=str(row.get("tactical_packet_id") or ""),
        validity_gate_status=str(row.get("validity_gate_status") or "NOT_RUN"),
        alert_gate_status=str(row.get("alert_gate_status") or "NOT_RUN"),
        email_delivery_status=str(row.get("email_delivery_status") or "NOT_SENT"),
    )


def _enum(value: str, allowed: set[str], field_name: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise ValueError(f"INVALID_{field_name.upper()}:{normalized}")
    return normalized


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _safe_id(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    return cleaned or "event_awareness"


def _material_packet_hash(packet: dict[str, Any]) -> str:
    material = {
        key: packet.get(key)
        for key in [
            "recommended_trade_id",
            "event_id",
            "event_run_id",
            "symbol",
            "side",
            "entry_reference_price",
            "quantity_or_sizing_guidance",
            "stop_price",
            "stop_logic",
            "risk_per_trade",
            "valid_until",
            "max_entry_slippage",
            "execution_sensitivity",
            "invalidation_conditions",
        ]
    }
    return canonical_hash_for_c2_artifact_v1({"material": material, "canonical_json_hash": None})


def _duplicate_suppressed(
    *,
    prior_alert_attempts: list[dict[str, Any]],
    source_packet_id: str,
    alert_channel: str,
    material_hash: str,
) -> bool:
    for row in prior_alert_attempts:
        if str(row.get("source_packet_id") or "") != source_packet_id:
            continue
        if str(row.get("alert_channel") or "").upper() != alert_channel:
            continue
        if alert_channel == "SMS" and str(row.get("material_packet_hash") or "") == material_hash:
            return True
    return False


def _alert_attempt_from_gate(gate: dict[str, Any]) -> dict[str, Any]:
    allowed = bool(gate.get("email_sms_allowed"))
    return {
        "alert_id": f"alert:{_safe_id(str(gate.get('source_packet_id') or gate.get('gate_id') or 'packet'))}",
        "event_id": str(gate.get("event_id") or ""),
        "event_run_id": str(gate.get("event_run_id") or ""),
        "source_packet_id": str(gate.get("source_packet_id") or ""),
        "source_packet_type": str(gate.get("source_packet_type") or ""),
        "symbol": _symbol_from_body(str(gate.get("sms_body") or "")),
        "side": _side_from_body(str(gate.get("sms_body") or "")),
        "alert_channel": str(gate.get("alert_channel") or ""),
        "alert_urgency": str(gate.get("alert_gate_status") or ""),
        "sent_at_utc": str(gate.get("evaluated_at_utc") or ""),
        "valid_until_utc": str(gate.get("valid_until_utc") or ""),
        "valid_until_display_et": str(gate.get("valid_until_display_et") or ""),
        "seconds_remaining_at_send": int(gate.get("seconds_remaining_at_send") or 0),
        "validity_gate_status": str(gate.get("validity_gate_status") or ""),
        "alert_gate_status": str(gate.get("alert_gate_status") or ""),
        "execution_sensitivity": str(gate.get("execution_sensitivity") or ""),
        "operator_action_required": "Review Aegis packet before manual entry." if allowed else "No operator interruption.",
        "delivery_status": "DRY_RUN_MESSAGE_BODY_ONLY" if allowed else "NOT_SENT",
        "duplicate_suppressed": bool(gate.get("duplicate_suppressed", False)),
        "reason_codes": _strings(gate.get("reason_codes")),
        "no_alert_reason": str(gate.get("no_alert_reason") or ""),
        "material_packet_hash": str(gate.get("material_packet_hash") or ""),
        "sms_body": str(gate.get("sms_body") or ""),
        "email_subject": str(gate.get("email_subject") or ""),
        "email_body": str(gate.get("email_body") or ""),
    }


def _sms_body(*, packet: dict[str, Any], valid_until_et: str) -> str:
    return "\n".join(
        [
            "AEGIS ACTIONABLE TRADE",
            f"{packet.get('symbol', '')} {packet.get('side', '')}".strip(),
            f"Entry ref: {packet.get('entry_reference_price', '')}",
            f"Max slip: {packet.get('max_entry_slippage', '')}",
            f"Stop: {packet.get('stop_price') or packet.get('stop_logic') or ''}",
            f"Valid until: {valid_until_et}",
            f"Reason: {packet.get('event_type', '')}",
            "Check Aegis packet before entry.",
        ]
    )


def _email_body(*, packet: dict[str, Any], valid_until_et: str, reason_codes: list[str]) -> str:
    checklist = "\n".join(f"- {item}" for item in _strings(packet.get("manual_execution_checklist")))
    invalidation = "\n".join(f"- {item}" for item in _strings(packet.get("invalidation_conditions")))
    return "\n".join(
        [
            "AEGIS ACTIONABLE TRADE",
            f"Event type: {packet.get('event_type', '')}",
            f"Symbol: {packet.get('symbol', '')}",
            f"Side: {packet.get('side', '')}",
            f"Entry reference: {packet.get('entry_reference_price', '')}",
            f"Max slippage: {packet.get('max_entry_slippage', '')}",
            f"Valid until: {valid_until_et}",
            f"Quantity/sizing: {packet.get('quantity_or_sizing_guidance', '')}",
            f"Stop: {packet.get('stop_price') or packet.get('stop_logic') or ''}",
            f"Risk: {packet.get('risk_per_trade', '')}",
            f"Reason codes: {', '.join(reason_codes)}",
            "Invalidation conditions:",
            invalidation,
            "Manual execution checklist:",
            checklist,
            "Receipt instructions: record alert_id, event_id, fill time, fill price, stop entry, valid_until compliance, slippage compliance, and operator notes.",
        ]
    )


def _display_et(value: str) -> str:
    if not value:
        return ""
    return _parse_utc(value).astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S %Z")


def _symbol_from_body(body: str) -> str:
    lines = body.splitlines()
    return lines[1].split()[0] if len(lines) > 1 and lines[1].split() else ""


def _side_from_body(body: str) -> str:
    lines = body.splitlines()
    parts = lines[1].split() if len(lines) > 1 else []
    return parts[1] if len(parts) > 1 else ""


def _parse_utc(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _number(value: Any) -> float | None:
    try:
        return float(str(value).strip().replace("%", ""))
    except (TypeError, ValueError):
        return None


def _slippage_value(value: str, entry: float | None) -> float | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text.endswith("bps"):
        amount = _number(text[:-3])
        if amount is None or entry is None:
            return None
        return abs(entry) * amount / 10000.0
    if text.endswith("%"):
        amount = _number(text)
        if amount is None or entry is None:
            return None
        return abs(entry) * amount / 100.0
    return _number(text)
