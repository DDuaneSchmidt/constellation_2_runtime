from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.human_reviewed_paper_mode_v1 import (
    candidate_review_packet_path_v1,
    ensure_paper_session_fields_v1,
    paper_review_queue_path_v1,
    paper_session_id_from_timestamp_v1,
    paper_trade_receipts_path_v1,
)
from ops.aegis.paper_session_ledger_v1 import (
    paper_session_ledger_path_v1,
    read_paper_session_ledger_v1,
    resolve_scheduled_paper_session_v1,
)
from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.candidate_lifecycle_projection_v1 import build_and_write_candidate_lifecycle_projection_v1

REPORT_FAMILY = "aegis_paper_operator_projection_v1"
REPORT_FILENAME = "paper_operator_projection.v1.json"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def paper_operator_projection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _row_session(row: Mapping[str, Any], *, default_day: str, default_ts: str = "") -> tuple[str, str]:
    existing = str(row.get("paper_session_id") or "").strip()
    if existing:
        return existing, str(row.get("paper_session_id_derivation_source") or "existing_payload")
    day = str(row.get("originating_day") or row.get("day_utc") or default_day)
    ts = str(row.get("run_timestamp_utc") or row.get("created_at") or row.get("timestamp_utc") or row.get("timestamp") or default_ts or "")
    session_id, source, _normalized = paper_session_id_from_timestamp_v1(day_utc=day, timestamp_utc=ts)
    return session_id, source


def _runtime_kernel(root: Path, day: str) -> dict[str, Any]:
    return _read_json(root / "reports" / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json")


def _construction(root: Path, day: str) -> dict[str, Any]:
    return _read_json(root / "reports" / "paper_trade_construction_v1" / day / "paper_trade_construction.v1.json")


def _market_inputs(root: Path, day: str) -> dict[str, Any]:
    return _read_json(root / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json")


def _market_by_symbol(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    records = payload.get("input_records") if isinstance(payload.get("input_records"), list) else []
    out: dict[str, Mapping[str, Any]] = {}
    for row in records:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            out[symbol] = row
    return out


def _first_value(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _missing_construction_fields(row: Mapping[str, Any]) -> list[str]:
    missing = [str(item) for item in row.get("missing_fields", []) if str(item)] if isinstance(row.get("missing_fields"), list) else []
    if not _first_value(row.get("entry_price"), row.get("entry_reference_price")):
        missing.append("entry_price")
    if not _first_value(row.get("stop_price")):
        missing.append("stop_price")
    return sorted(set(missing))


def _position_ledger(root: Path, day: str) -> dict[str, Any]:
    return _read_json(root / "reports" / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json")


def _command_results(root: Path, day: str) -> dict[str, Any]:
    return _read_json(root / "reports" / "aegis_command_results_v1" / day / "command_results.v1.json")


def _latest_command_by_candidate(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = payload.get("results") if isinstance(payload.get("results"), list) else []
    out: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        candidate_id = str(row.get("candidate_id") or "")
        if candidate_id:
            out[candidate_id] = row
    return out




def _is_open_candidate(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("candidate_status") or "").upper() == "PAPER_POSITION_OPEN"
        or str(row.get("lifecycle_state") or "").upper() == "PAPER_POSITION_OPEN"
        or str(row.get("latest_command_status") or "").upper() == "EXECUTED"
    )


def _is_actionable_current_candidate(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("source_state") or "").upper() == "ACTIVE_REVIEW"
        and row.get("actionable") is True
        and row.get("active_review_present") is True
        and not _is_open_candidate(row)
    )


def _row_identity(row: Mapping[str, Any]) -> str:
    return _first_value(row.get("candidate_id"), row.get("candidate_contract_id"), row.get("position_id"), row.get("receipt_id"), row.get("symbol"))


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = _row_identity(row)
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        out.append(row)
    return out

def _mode_rows(kernel: Mapping[str, Any]) -> dict[str, Any]:
    graph = kernel.get("dependency_graph") if isinstance(kernel.get("dependency_graph"), Mapping) else {}
    layers = kernel.get("layers") if isinstance(kernel.get("layers"), Mapping) else {}
    def allowed(key: str) -> bool:
        row = graph.get(key) if isinstance(graph.get(key), Mapping) else {}
        if row:
            return bool(row.get("allowed") is True)
        return bool(layers.get(key) is True)
    paper_blockers = []
    for key in ("PAPER_CANDIDATES_READY", "PAPER_REVIEW_ALLOWED", "PAPER_TRADE_READY", "PAPER_TRADE_CREATION_ALLOWED"):
        row = graph.get(key) if isinstance(graph.get(key), Mapping) else {}
        if row and row.get("allowed") is not True:
            paper_blockers.extend([str(item) for item in row.get("missing_or_blocking_artifacts", []) if str(item)])
            reason = str(row.get("reason") or "").strip()
            if reason:
                paper_blockers.append(f"{key}:{reason}")
    paper_ready = all(allowed(key) for key in ("PAPER_CANDIDATES_READY", "PAPER_REVIEW_ALLOWED", "PAPER_TRADE_READY", "PAPER_TRADE_CREATION_ALLOWED"))
    advisory_row = graph.get("TRADE_ADVICE_ALLOWED") if isinstance(graph.get("TRADE_ADVICE_ALLOWED"), Mapping) else {}
    return {
        "paper_mode": {
            "status": "READY" if paper_ready else "BLOCKED",
            "paper_candidates_ready": allowed("PAPER_CANDIDATES_READY"),
            "paper_review_allowed": allowed("PAPER_REVIEW_ALLOWED"),
            "paper_trade_ready": allowed("PAPER_TRADE_READY"),
            "paper_trade_creation_allowed": allowed("PAPER_TRADE_CREATION_ALLOWED"),
            "allowed_actions": ["REVIEW_PAPER_CANDIDATES", "CREATE_PAPER_TRADES"] if paper_ready else ["REVIEW_PAPER_CANDIDATES"] if allowed("PAPER_CANDIDATES_READY") else [],
            "blocked_actions": [] if paper_ready else ["CREATE_PAPER_TRADES"],
            "blockers": sorted(set(paper_blockers)),
        },
        "advisory_mode": {
            "status": "READY" if bool(advisory_row.get("allowed") is True) else "BLOCKED",
            "trade_advice_allowed": bool(advisory_row.get("allowed") is True),
            "reason": str(advisory_row.get("reason") or "PARTIAL_CONTEXT / missing advisory dependencies"),
        },
        "live_mode": {
            "status": "DISABLED",
            "reason": "DISABLED_BY_DESIGN",
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "live_trading_allowed": False,
        },
    }


def _candidate_row(row: Mapping[str, Any], *, session_id: str, source_path: Path, construction_path: Path, carry_forward: bool, active_review_ids: set[str], opened_candidate_ids: set[str], latest_command_by_candidate: dict[str, Mapping[str, Any]], construction_by_id: dict[str, Mapping[str, Any]], skipped_by_id: dict[str, Mapping[str, Any]], diagnostics_by_id: dict[str, Mapping[str, Any]], market_by_symbol: dict[str, Mapping[str, Any]], default_ts: str, day: str) -> dict[str, Any]:
    candidate_id = str(row.get("candidate_id") or "")
    construction = construction_by_id.get(candidate_id, {})
    skipped = skipped_by_id.get(candidate_id, {})
    diag = diagnostics_by_id.get(candidate_id, {})
    symbol = str(row.get("symbol") or construction.get("symbol") or "").strip().upper()
    market = market_by_symbol.get(symbol, {}) if symbol else {}
    latest_command = latest_command_by_candidate.get(candidate_id, {})
    market_status = str(diag.get("status") or market.get("validation_status") or ("PASS" if construction else "UNKNOWN"))
    construction_status = str(construction.get("construction_status") or ("SKIPPED" if skipped else ("CONSTRUCTED" if construction else "NOT_CONSTRUCTED")))
    entry_price = _first_value(construction.get("entry_price"), construction.get("entry_reference_price"), row.get("entry_price"), row.get("entry_reference_price"))
    stop_price = _first_value(construction.get("stop_price"), row.get("stop_price"))
    target_price = _first_value(construction.get("target_price"), row.get("target_price"))
    quantity = _first_value(construction.get("quantity"), construction.get("suggested_quantity"), row.get("quantity"))
    notional_value = _first_value(construction.get("notional_value"), construction.get("suggested_notional"), row.get("notional_value"), row.get("notional"))
    risk_per_share = _first_value(construction.get("risk_per_share"), row.get("risk_per_share"))
    max_risk_amount = _first_value(construction.get("max_risk_amount"), construction.get("max_loss_estimate"), row.get("max_risk_amount"))
    risk_percent = _first_value(construction.get("risk_percent"), construction.get("estimated_notional_risk_pct"), row.get("risk_percent"))
    missing_fields = _missing_construction_fields({**dict(construction), "entry_price": entry_price, "stop_price": stop_price}) if construction else [field for field in ("entry_price", "stop_price") if not (entry_price if field == "entry_price" else stop_price)]
    stop_source = _first_value(construction.get("stop_policy_source"), row.get("stop_policy_source"))
    entry_source = _first_value(construction.get("entry_reference_source"), row.get("entry_reference_source"), "market_data_inputs_v1" if entry_price else "")
    market_ts = _first_value(construction.get("source_market_data_timestamp"), market.get("source_timestamp_utc"), market.get("retrieval_timestamp_utc"), market.get("retrieved_at_utc"))
    market_source = _first_value(construction.get("source_market_data_vendor"), market.get("source_vendor"), market.get("producer_id"), "market_data_inputs_v1" if market else "")
    paper_position_open = bool(candidate_id and candidate_id in opened_candidate_ids)
    active_review_present = bool(candidate_id and candidate_id in active_review_ids and not carry_forward and not paper_position_open)
    source_state = "ACTIVE_REVIEW" if active_review_present else ("PAPER_POSITION_OPEN" if paper_position_open else ("CARRY_FORWARD" if carry_forward else "CONSTRUCTED" if construction else "LEGACY_PARTIAL"))
    action_block_reason = ""
    if paper_position_open:
        action_block_reason = "Paper trade already recorded"
    elif not active_review_present:
        action_block_reason = "Not in active review state"
    elif missing_fields:
        action_block_reason = "Missing construction prices"
    actionable = active_review_present and not bool(missing_fields)
    return {
        "paper_session_id": str(row.get("paper_session_id") or construction.get("paper_session_id") or session_id),
        "candidate_id": candidate_id,
        "candidate_contract_id": candidate_id,
        "source_candidate_contract_id": candidate_id,
        "displayed_from": "paper_operator_projection.current_day_candidates" if active_review_present else ("paper_operator_projection.carry_forward_candidates" if carry_forward else "paper_operator_projection.constructed_candidates"),
        "source_state": source_state,
        "active_review_present": active_review_present,
        "construction_present": bool(construction),
        "actionable": actionable,
        "action_block_reason": action_block_reason,
        "action_endpoint": "/api/aegis/commands" if active_review_present else "none",
        "latest_command_id": str(latest_command.get("command_id") or ""),
        "latest_command_status": str(latest_command.get("status") or ""),
        "latest_command_message": str(latest_command.get("message") or ""),
        "latest_command_receipt_id": str(latest_command.get("receipt_id") or ""),
        "latest_command_receipt_path": str(latest_command.get("receipt_path") or ""),
        "symbol": symbol,
        "strategy": str(row.get("strategy") or row.get("sleeve_id") or row.get("engine_id") or ""),
        "sleeve": str(row.get("sleeve_id") or row.get("engine_id") or ""),
        "sleeve_id": str(row.get("sleeve_id") or row.get("engine_id") or ""),
        "raw_signal_id": str(row.get("raw_signal_id") or ""),
        "direction": str(row.get("direction") or construction.get("direction") or ""),
        "score": row.get("score"),
        "conviction": row.get("conviction"),
        "candidate_status": "PAPER_POSITION_OPEN" if paper_position_open else str(row.get("status") or row.get("current_state") or "AWAITING_REVIEW"),
        "lifecycle_state": "PAPER_POSITION_OPEN" if paper_position_open else str(row.get("current_state") or row.get("rollover_status") or ("CARRIED_FORWARD" if carry_forward else "CURRENT_DAY")),
        "paper_construction_status": construction_status,
        "construction_status": construction_status,
        "market_data_status": market_status,
        "created_at": str(row.get("created_at") or row.get("timestamp_utc") or row.get("timestamp") or default_ts),
        "source_artifact_path": str(source_path),
        "construction_artifact_path": str(construction_path),
        "carry_forward": bool(carry_forward),
        "entry_price": entry_price,
        "entry_reference_price": entry_price,
        "stop_price": stop_price,
        "target_price": target_price,
        "limit_price": _first_value(construction.get("limit_price"), row.get("limit_price")),
        "order_type": _first_value(construction.get("order_type"), row.get("order_type")),
        "time_in_force": _first_value(construction.get("time_in_force"), row.get("time_in_force")),
        "quantity": quantity,
        "notional_value": notional_value,
        "risk_per_share": risk_per_share,
        "max_risk_amount": max_risk_amount,
        "risk_percent": risk_percent,
        "reward_risk_ratio": _first_value(construction.get("reward_risk_ratio"), row.get("reward_risk_ratio")),
        "market_data_timestamp": market_ts,
        "market_data_source": market_source,
        "market_data_snapshot_used": _first_value(construction.get("source_market_data_path"), market.get("source_path"), diag.get("artifact_path_checked")),
        "construction_timestamp": _first_value(construction.get("construction_timestamp_utc"), default_ts),
        "construction_missing_fields": missing_fields,
        "entry_rationale": f"Entry from {entry_source}." if entry_source else "Entry source unavailable.",
        "stop_rationale": f"Stop from {stop_source}." if stop_source else "Stop policy unavailable.",
        "target_rationale": "Target unavailable in canonical construction." if not target_price else "Target from canonical construction.",
        "skipped_block_reason": str(skipped.get("skip_reason_code") or skipped.get("missing_field") or diag.get("missing_field") or ""),
        "blocker_reason": str(skipped.get("skip_reason_code") or skipped.get("missing_field") or diag.get("missing_field") or row.get("decision_reason") or ("Missing construction prices" if missing_fields else "")),
    }


def build_paper_operator_projection_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated = generated_at_utc or _now_iso()
    packet_path = candidate_review_packet_path_v1(truth_root=root, day_utc=day)
    queue_path = paper_review_queue_path_v1(truth_root=root, day_utc=day)
    official_session = resolve_scheduled_paper_session_v1(truth_root=root, day_utc=day)
    session_ledger = read_paper_session_ledger_v1(truth_root=root, day_utc=day)
    packet = ensure_paper_session_fields_v1(_read_json(packet_path), day_utc=day, truth_root=root)
    queue = ensure_paper_session_fields_v1(_read_json(queue_path), day_utc=day, run_timestamp_utc=str(packet.get("run_timestamp_utc") or packet.get("generated_at_utc") or ""), truth_root=root)
    construction_path = root / "reports" / "paper_trade_construction_v1" / day / "paper_trade_construction.v1.json"
    construction = _construction(root, day)
    market_inputs = _market_inputs(root, day)
    market_rows_by_symbol = _market_by_symbol(market_inputs)
    ledger = _position_ledger(root, day)
    command_results = _command_results(root, day)
    latest_commands_by_candidate = _latest_command_by_candidate(command_results)
    receipts_path = paper_trade_receipts_path_v1(truth_root=root, day_utc=day)
    receipts = _read_json(receipts_path)
    receipt_rows = receipts.get("receipts") if isinstance(receipts.get("receipts"), list) else []
    receipt_candidate_ids = {str(row.get("candidate_id") or "") for row in receipt_rows if isinstance(row, Mapping)}
    kernel = _runtime_kernel(root, day)
    modes = _mode_rows(kernel)
    session_id = str(official_session.get("paper_session_id") or packet.get("paper_session_id") or queue.get("paper_session_id") or construction.get("paper_session_id") or "")
    run_ts = str(packet.get("run_timestamp_utc") or packet.get("generated_at_utc") or queue.get("run_timestamp_utc") or "")
    session_events = official_session.get("events") if isinstance(official_session.get("events"), list) else []
    reconstructed = [event for event in session_events if isinstance(event, Mapping) and event.get("event_type") == "PAPER_SESSION_RECONSTRUCTED"]
    construction_rows = construction.get("constructed_paper_trades") if isinstance(construction.get("constructed_paper_trades"), list) else []
    skipped_rows = construction.get("skipped_candidates") if isinstance(construction.get("skipped_candidates"), list) else []
    diagnostic_rows = construction.get("market_data_diagnostics") if isinstance(construction.get("market_data_diagnostics"), list) else []
    construction_by_id = {str(row.get("candidate_id") or ""): row for row in construction_rows if isinstance(row, Mapping)}
    skipped_by_id = {str(row.get("candidate_id") or ""): row for row in skipped_rows if isinstance(row, Mapping)}
    diagnostics_by_id = {str(row.get("candidate_id") or ""): row for row in diagnostic_rows if isinstance(row, Mapping)}
    packet_candidates = packet.get("review_candidates") if isinstance(packet.get("review_candidates"), list) else []
    queue_rows = queue.get("rows") if isinstance(queue.get("rows"), list) else []
    active_review_ids = {str(row.get("candidate_id") or "") for row in packet_candidates if isinstance(row, Mapping)}
    ledger_open_rows = ledger.get("open_positions") if isinstance(ledger.get("open_positions"), list) else []
    opened_candidate_ids = set(receipt_candidate_ids) | {str(row.get("candidate_id") or "") for row in ledger_open_rows if isinstance(row, Mapping)}
    current_rows = [_candidate_row(row, session_id=session_id, source_path=packet_path, construction_path=construction_path, carry_forward=False, active_review_ids=active_review_ids, opened_candidate_ids=opened_candidate_ids, latest_command_by_candidate=latest_commands_by_candidate, construction_by_id=construction_by_id, skipped_by_id=skipped_by_id, diagnostics_by_id=diagnostics_by_id, market_by_symbol=market_rows_by_symbol, default_ts=run_ts, day=day) for row in packet_candidates if isinstance(row, Mapping)]
    current_ids = {row["candidate_id"] for row in current_rows}
    carry_rows = []
    for row in queue_rows:
        if not isinstance(row, Mapping):
            continue
        candidate_id = str(row.get("candidate_id") or "")
        carry = str(row.get("rollover_status") or "").upper() != "CURRENT_DAY" or str(row.get("originating_day") or day) != day
        if carry and candidate_id not in current_ids:
            row_session, _source = _row_session(row, default_day=day, default_ts=run_ts)
            carry_rows.append(_candidate_row(row, session_id=row_session, source_path=queue_path, construction_path=construction_path, carry_forward=True, active_review_ids=active_review_ids, opened_candidate_ids=opened_candidate_ids, latest_command_by_candidate=latest_commands_by_candidate, construction_by_id=construction_by_id, skipped_by_id=skipped_by_id, diagnostics_by_id=diagnostics_by_id, market_by_symbol=market_rows_by_symbol, default_ts=run_ts, day=day))
    open_positions = []
    for row in ledger_open_rows:
        if not isinstance(row, Mapping):
            continue
        row_session, _source = _row_session(row, default_day=str(row.get("originating_day") or day), default_ts=str(row.get("entry_time") or run_ts))
        open_positions.append({**dict(row), "paper_session_id": str(row.get("paper_session_id") or row_session), "carry_forward": str(row.get("originating_day") or day) != day})
    open_candidate_rows = [row for row in current_rows + carry_rows if _is_open_candidate(row)]
    open_positions = _dedupe_rows(open_candidate_rows + open_positions)
    actionable_current_candidates = [row for row in current_rows if _is_actionable_current_candidate(row)]
    carry_forward_context = carry_rows
    blocked = [
        row
        for row in current_rows + carry_rows
        if (row.get("paper_construction_status") == "SKIPPED" or row.get("blocker_reason"))
        and not _is_open_candidate(row)
        and not _is_actionable_current_candidate(row)
    ]
    missing_market_data_count = sum(1 for row in diagnostic_rows if isinstance(row, Mapping) and str(row.get("missing_field") or ""))
    lifecycle_projection, lifecycle_projection_path = build_and_write_candidate_lifecycle_projection_v1(truth_root=root, day_utc=day)
    lifecycle_summary = lifecycle_projection.get("summary") if isinstance(lifecycle_projection.get("summary"), Mapping) else {}
    sessions = [{
        "paper_session_id": session_id,
        "official_session_label": str(official_session.get("scheduled_run_time") or ""),
        "scheduled_run_time": str(official_session.get("scheduled_run_time") or ""),
        "session_timezone": str(official_session.get("session_timezone") or "America/New_York"),
        "execution_started_at": official_session.get("execution_started_at"),
        "execution_completed_at": official_session.get("execution_completed_at"),
        "candidate_generated_at": official_session.get("candidate_generated_at") or packet.get("generated_at_utc"),
        "canonicalized_at": official_session.get("canonicalized_at") or packet.get("generated_at_utc"),
        "run_timestamp": run_ts,
        "source_candidate_packet_path": str(packet_path),
        "current_day_candidate_count": len(current_rows),
        "constructed_trade_count": len(construction_rows),
        "skipped_count": len(skipped_rows),
        "missing_market_data_count": missing_market_data_count,
        "carry_forward_count": len(carry_rows),
        "open_paper_position_count": len(open_positions),
        "actionable_current_candidate_count": int(lifecycle_summary.get("actionable") or len(actionable_current_candidates)),
        "lifecycle_current_session_candidate_count": int(lifecycle_summary.get("current_session_total") or 0),
        "retry_count": int(official_session.get("retry_count") or len(reconstructed) or 0),
        "reconstruction_count": int(official_session.get("reconstruction_count") or len(reconstructed) or 0),
        "latest_reconstruction_reason": str(official_session.get("latest_reconstruction_reason") or ""),
        "status": str(official_session.get("status") or "SCHEDULED"),
    }] if session_id else []
    return {
        "schema_id": "aegis_paper_operator_projection",
        "schema_version": "v1",
        "artifact_id": f"aegis_paper_operator_projection_v1:{day}",
        "day_utc": day,
        "generated_at": generated,
        "generated_at_utc": generated,
        **modes,
        "sessions": sessions,
        "current_day_candidates": current_rows,
        "current_session_candidates_all": lifecycle_projection.get("current_session_candidates", []),
        "candidate_lifecycle_projection": lifecycle_projection,
        "candidate_lifecycle_projection_path": str(lifecycle_projection_path),
        "actionable_current_candidates": lifecycle_projection.get("actionable_current_candidates", actionable_current_candidates),
        "carry_forward_candidates": carry_rows,
        "carry_forward_context": lifecycle_projection.get("carry_forward_context", carry_forward_context),
        "open_paper_positions": lifecycle_projection.get("open_paper_positions", open_positions),
        "blocked_or_skipped_candidates": lifecycle_projection.get("blocked_or_skipped_candidates", blocked),
        "source_artifacts": {
            "candidate_review_packet": str(packet_path),
            "paper_review_queue": str(queue_path),
            "paper_trade_construction": str(construction_path),
            "paper_trade_receipts": str(receipts_path),
            "command_results": str(root / "reports" / "aegis_command_results_v1" / day / "command_results.v1.json"),
            "paper_session_ledger": str(paper_session_ledger_path_v1(truth_root=root, day_utc=day)),
            "candidate_lifecycle_projection": str(lifecycle_projection_path),
            "paper_position_ledger": str(root / "reports" / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json"),
            "runtime_truth_kernel": str(root / "reports" / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json"),
        },
        "paper_session_ledger": session_ledger,
        "command_results": command_results,
        "safety": {
            "trade_advice_allowed": False,
            "broker_submit_transmit_allowed": False,
            "live_trading_allowed": False,
            "autonomous_execution_allowed": False,
        },
    }


def write_paper_operator_projection_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    projection = payload or build_paper_operator_projection_v1(truth_root=root, day_utc=day_utc)
    return write_json_v1(paper_operator_projection_path_v1(truth_root=root, day_utc=day_utc), projection)


def build_and_write_paper_operator_projection_v1(*, truth_root: Path | str, day_utc: str) -> tuple[dict[str, Any], Path]:
    payload = build_paper_operator_projection_v1(truth_root=truth_root, day_utc=day_utc)
    path = write_paper_operator_projection_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    return {**payload, "artifact_path": str(path)}, path
