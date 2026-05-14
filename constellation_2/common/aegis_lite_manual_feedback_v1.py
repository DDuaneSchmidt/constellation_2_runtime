from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_lite_eod_v1 import normalize_trade_candidate_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SUPPORTED_TRADE_CLASSES = {
    "LONG_EQUITY",
    "SHORT_EQUITY",
    "LONG_CALL_OPTION",
    "LONG_PUT_OPTION",
    "ETF_ROTATION_PAIR",
}
SCHEMA_RELPATHS = {
    "manual_operator_decision": "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_operator_decision.v1.schema.json",
    "manual_execution_event": "governance/04_DATA/SCHEMAS/C2/REPORTS/manual_execution_event.v1.schema.json",
    "portfolio_position_snapshot": "governance/04_DATA/SCHEMAS/C2/REPORTS/portfolio_position_snapshot.v1.schema.json",
    "protective_order_snapshot": "governance/04_DATA/SCHEMAS/C2/REPORTS/protective_order_snapshot.v1.schema.json",
    "trade_outcome_attribution": "governance/04_DATA/SCHEMAS/C2/REPORTS/trade_outcome_attribution.v1.schema.json",
    "edge_cluster": "governance/04_DATA/SCHEMAS/C2/REPORTS/edge_cluster.v1.schema.json",
    "operator_execution_queue": "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_execution_queue.v1.schema.json",
}


def artifact_path_v1(*, truth_root: Path, artifact_id: str, day_utc: str, run_id: str) -> Path:
    filename = f"{artifact_id.replace('_v1', '')}.v1.json"
    return Path(truth_root).resolve() / "reports" / artifact_id / day_utc / _safe_run_id(run_id) / filename


def validate_manual_feedback_artifact_v1(payload: dict[str, Any]) -> None:
    schema_id = str(payload.get("schema_id") or "")
    relpath = SCHEMA_RELPATHS.get(schema_id)
    if not relpath:
        raise ValueError(f"UNSUPPORTED_AEGIS_LITE_FEEDBACK_SCHEMA:{schema_id}")
    validate_against_repo_schema_v1(payload, REPO_ROOT, relpath)


def write_manual_feedback_artifact_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_manual_feedback_artifact_v1(payload)
    path = artifact_path_v1(
        truth_root=truth_root,
        artifact_id=str(payload["artifact_id"]),
        day_utc=str(payload["day_utc"]),
        run_id=str(payload["run_id"]),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_manual_operator_decision_v1(
    *,
    day_utc: str,
    run_id: str,
    candidate_id: str,
    sleeve_id: str,
    decision: str,
    decision_time_utc: str,
    edge_cluster_id: str = "",
    decision_reason_codes: list[str] | None = None,
    operator_notes: str = "",
    deviation_from_recommendation: str = "",
    manual_review_required: bool = False,
    source_artifact_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_id": "manual_operator_decision",
        "schema_version": "v1",
        "artifact_id": "manual_operator_decision_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "candidate_id": candidate_id,
        "sleeve_id": sleeve_id,
        "edge_cluster_id": edge_cluster_id,
        "decision": str(decision).strip().upper(),
        "decision_reason_codes": _strings(decision_reason_codes),
        "operator_notes": operator_notes,
        "decision_time_utc": decision_time_utc,
        "deviation_from_recommendation": deviation_from_recommendation,
        "manual_review_required": bool(manual_review_required),
        "observational_only": True,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "source_artifact_lineage": source_artifact_lineage or [],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_manual_execution_event_v1(
    *,
    day_utc: str,
    run_id: str,
    candidate_id: str,
    symbol: str,
    direction: str,
    instrument_type: str,
    side: str,
    quantity: int,
    order_type: str,
    actual_entry_price: str,
    stop_price_entered: str,
    stop_order_type: str,
    execution_time_utc: str,
    ib_order_id: str = "",
    ib_perm_id: str = "",
    deviation_from_aegis_plan: str = "",
    operator_notes: str = "",
    source_artifact_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_id": "manual_execution_event",
        "schema_version": "v1",
        "artifact_id": "manual_execution_event_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "candidate_id": candidate_id,
        "symbol": symbol.strip().upper(),
        "direction": direction.strip().upper(),
        "instrument_type": instrument_type.strip().upper(),
        "side": side.strip().upper(),
        "quantity": int(quantity),
        "order_type": order_type.strip().upper(),
        "actual_entry_price": str(actual_entry_price),
        "stop_price_entered": str(stop_price_entered),
        "stop_order_type": str(stop_order_type).strip().upper(),
        "ib_order_id": str(ib_order_id),
        "ib_perm_id": str(ib_perm_id),
        "execution_time_utc": execution_time_utc,
        "deviation_from_aegis_plan": deviation_from_aegis_plan,
        "operator_notes": operator_notes,
        "observational_only": True,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "source_artifact_lineage": source_artifact_lineage or [],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_portfolio_position_snapshot_v1(
    *,
    day_utc: str,
    run_id: str,
    snapshot_time_utc: str,
    source: str,
    open_positions: list[dict[str, Any]],
    cash: str,
    net_liquidation: str,
    source_artifact_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    positions = [_position(row, idx + 1) for idx, row in enumerate(open_positions)]
    payload = {
        "schema_id": "portfolio_position_snapshot",
        "schema_version": "v1",
        "artifact_id": "portfolio_position_snapshot_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "snapshot_time_utc": snapshot_time_utc,
        "source": source.strip().upper(),
        "open_positions": positions,
        "cash": str(cash),
        "net_liquidation": str(net_liquidation),
        "gross_exposure": _gross_exposure(positions),
        "net_exposure": _net_exposure(positions),
        "exposure_basis": _exposure_basis(positions),
        "exposure_warnings": _exposure_warnings(positions),
        "exposure_by_symbol": _exposure_by(positions, "symbol"),
        "exposure_by_edge_cluster": _exposure_by(positions, "edge_cluster_id"),
        "exposure_by_sleeve": _exposure_by(positions, "sleeve_id"),
        "observational_only": True,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "source_artifact_lineage": source_artifact_lineage or [],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_protective_order_snapshot_v1(
    *,
    day_utc: str,
    run_id: str,
    snapshot_time_utc: str,
    protective_orders: list[dict[str, Any]],
    source_artifact_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = [_protective_order(row) for row in protective_orders]
    warnings = [
        f"{row['candidate_id']}:{row['symbol']}:MISSING_PROTECTIVE_STOP"
        for row in rows
        if row["missing_stop_warning"]
    ]
    payload = {
        "schema_id": "protective_order_snapshot",
        "schema_version": "v1",
        "artifact_id": "protective_order_snapshot_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "snapshot_time_utc": snapshot_time_utc,
        "protective_orders": rows,
        "missing_stop_warnings": warnings,
        "observational_only": True,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "source_artifact_lineage": source_artifact_lineage or [],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_edge_cluster_v1(
    *,
    day_utc: str,
    run_id: str,
    candidates: list[dict[str, Any]],
    source_artifact_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized = [normalize_trade_candidate_v1(row, ordinal=idx + 1) for idx, row in enumerate(candidates)]
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for candidate in normalized:
        key = (
            candidate["thesis_id"],
            candidate["edge_family"],
            candidate["regime_dependency"],
            candidate["direction"],
            ",".join(candidate["shared_risk_tags"]),
            ",".join(candidate["correlated_symbols"]),
            candidate["macro_sensitivity"],
            candidate["volatility_liquidity_dependency"],
        )
        grouped.setdefault(key, []).append(candidate)
    clusters: list[dict[str, Any]] = []
    for idx, (key, rows) in enumerate(sorted(grouped.items(), key=lambda item: item[0]), start=1):
        symbols = sorted({row["symbol"] for row in rows if row["symbol"]})
        clusters.append(
            {
                "edge_cluster_id": f"EDGE_CLUSTER_{idx:03d}",
                "thesis_id": key[0],
                "edge_family": key[1],
                "regime_dependency": key[2],
                "direction": key[3],
                "shared_risk_tags": key[4].split(",") if key[4] else [],
                "correlated_symbols": sorted({symbol for row in rows for symbol in row["correlated_symbols"]}),
                "candidates_in_cluster": [row["candidate_id"] for row in rows],
                "duplicate_thesis_flag": len(rows) > 1,
                "distinct_edge_flag": True,
                "recommended_portfolio_expression": ",".join(symbols),
                "concentration_warning": len(symbols) < len(rows),
                "governance_recommendation": "prefer_best_candidate_in_group" if len(rows) > 1 else "approve",
            }
        )
    payload = {
        "schema_id": "edge_cluster",
        "schema_version": "v1",
        "artifact_id": "edge_cluster_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "edge_clusters": clusters,
        "source_artifact_lineage": source_artifact_lineage or [],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_operator_execution_queue_v1(
    *,
    day_utc: str,
    run_id: str,
    candidates: list[dict[str, Any]],
    edge_clusters: dict[str, Any],
    source_artifact_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cluster_by_candidate = {
        candidate_id: row["edge_cluster_id"]
        for row in edge_clusters.get("edge_clusters", [])
        for candidate_id in row.get("candidates_in_cluster", [])
        if isinstance(row, dict)
    }
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(candidates, start=1):
        candidate = normalize_trade_candidate_v1(raw, ordinal=idx)
        trade_class = _trade_class(candidate)
        readiness_blockers = _queue_item_blockers(candidate=candidate, trade_class=trade_class)
        supported = "UNSUPPORTED_MANUAL_EXECUTION" not in readiness_blockers
        queue_status = "READY_FOR_MANUAL_ENTRY" if not readiness_blockers else ("UNSUPPORTED_MANUAL_EXECUTION" if not supported else "BLOCKED")
        rows.append(
            {
                "run_id": run_id,
                "execution_order": idx,
                "candidate_id": candidate["candidate_id"],
                "priority_rank": idx,
                "trade_class": trade_class,
                "manual_execution_recipe": _manual_recipe(raw, candidate, trade_class, supported),
                "required_orders": _required_orders(candidate, trade_class, supported),
                "stop_required": True,
                "skip_ok_flag": True,
                "overlap_group": candidate["thesis_id"],
                "edge_cluster_id": cluster_by_candidate.get(candidate["candidate_id"], ""),
                "risk_bucket": ",".join(candidate["shared_risk_tags"]) or "UNCLASSIFIED",
                "operator_confirmations": _operator_confirmations(supported),
                "queue_status": queue_status,
                "reason_codes": readiness_blockers,
            }
        )
    payload = {
        "schema_id": "operator_execution_queue",
        "schema_version": "v1",
        "artifact_id": "operator_execution_queue_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "execution_queue": rows,
        "unsupported_manual_execution_warnings": [
            f"{row['candidate_id']}:UNSUPPORTED_MANUAL_EXECUTION"
            for row in rows
            if row["queue_status"] == "UNSUPPORTED_MANUAL_EXECUTION"
        ],
        "manual_execution_only": True,
        "broker_submit_required": False,
        "transmit_automation_required": False,
        "source_artifact_lineage": source_artifact_lineage or [],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_trade_outcome_attribution_v1(
    *,
    day_utc: str,
    run_id: str,
    candidates: list[dict[str, Any]],
    decisions: list[dict[str, Any]] | None = None,
    market_outcomes: dict[str, dict[str, Any]] | None = None,
    source_artifact_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    decisions_by_candidate = {str(row.get("candidate_id")): row for row in decisions or [] if isinstance(row, dict)}
    outcomes = market_outcomes or {}
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(candidates, start=1):
        candidate = normalize_trade_candidate_v1(raw, ordinal=idx)
        decision = decisions_by_candidate.get(candidate["candidate_id"], {})
        outcome = outcomes.get(candidate["candidate_id"], {})
        skipped = str(decision.get("decision") or "").upper() in {"SKIPPED", "WATCHLIST", "REJECTED"}
        rows.append(
            {
                "candidate_id": candidate["candidate_id"],
                "run_id": run_id,
                "sleeve_id": candidate["sleeve_id"],
                "edge_cluster_id": str(raw.get("edge_cluster_id") or ""),
                "recommended_entry_reference": candidate["entry_reference_price"],
                "actual_entry_price": str(outcome.get("actual_entry_price") or ""),
                "actual_exit_price": str(outcome.get("actual_exit_price") or ""),
                "model_forward_returns": _strings(outcome.get("model_forward_returns")),
                "realized_pnl": str(outcome.get("realized_pnl") or ""),
                "unrealized_pnl": str(outcome.get("unrealized_pnl") or ""),
                "MAE": str(outcome.get("MAE") or ""),
                "MFE": str(outcome.get("MFE") or ""),
                "operator_slippage": str(outcome.get("operator_slippage") or ""),
                "skipped_trade_outcome": str(outcome.get("skipped_trade_outcome") or ("PENDING_FORWARD_ANALYSIS" if skipped else "")),
                "governance_adjustment_effect": str(outcome.get("governance_adjustment_effect") or ""),
                "sleeve_signal_quality": str(outcome.get("sleeve_signal_quality") or "PENDING_FORWARD_ANALYSIS"),
                "implementation_quality": str(outcome.get("implementation_quality") or ("NOT_IMPLEMENTED" if skipped else "PENDING")),
                "operator_execution_quality": str(outcome.get("operator_execution_quality") or ("SKIPPED" if skipped else "PENDING")),
            }
        )
    payload = {
        "schema_id": "trade_outcome_attribution",
        "schema_version": "v1",
        "artifact_id": "trade_outcome_attribution_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "attributions": rows,
        "ib_execution_required": False,
        "source_artifact_lineage": source_artifact_lineage or [],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _position(row: dict[str, Any], ordinal: int) -> dict[str, Any]:
    return {
        "position_id": str(row.get("position_id") or f"POSITION_{ordinal:03d}"),
        "candidate_id": str(row.get("candidate_id") or ""),
        "sleeve_id": str(row.get("sleeve_id") or "UNKNOWN"),
        "edge_cluster_id": str(row.get("edge_cluster_id") or ""),
        "symbol": str(row.get("symbol") or "").upper(),
        "direction": str(row.get("direction") or "").upper(),
        "quantity": int(row.get("quantity") or 0),
        "average_entry_price": str(row.get("average_entry_price") or ""),
        "current_reference_price": str(row.get("current_reference_price") or ""),
        "unrealized_pnl": str(row.get("unrealized_pnl") or ""),
    }


def _protective_order(row: dict[str, Any]) -> dict[str, Any]:
    stop_exists = bool(row.get("stop_exists"))
    quantity = int(row.get("quantity") or 0)
    stop_quantity = int(row.get("stop_quantity") or 0)
    required_quantity = abs(quantity)
    stop_price = str(row.get("stop_price") or "").strip()
    source_incomplete = not str(row.get("position_id") or "").strip() or not str(row.get("symbol") or "").strip() or required_quantity <= 0
    valid_stop = stop_exists and bool(stop_price) and stop_quantity > 0
    if source_incomplete:
        status = "UNKNOWN"
    elif not valid_stop:
        status = "UNPROTECTED"
    elif stop_quantity < required_quantity:
        status = "PARTIALLY_PROTECTED"
    else:
        status = "PROTECTED"
    return {
        "snapshot_time_utc": str(row.get("snapshot_time_utc") or ""),
        "position_id": str(row.get("position_id") or ""),
        "candidate_id": str(row.get("candidate_id") or ""),
        "symbol": str(row.get("symbol") or "").upper(),
        "quantity": quantity,
        "stop_exists": stop_exists,
        "stop_price": stop_price,
        "stop_quantity": stop_quantity,
        "protection_status": status,
        "missing_stop_warning": status != "PROTECTED",
        "operator_action_required": status != "PROTECTED",
    }


def _exposure_by(positions: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    totals: dict[str, int] = {}
    for row in positions:
        bucket = str(row.get(key) or "UNKNOWN")
        totals[bucket] = totals.get(bucket, 0) + int(row["quantity"])
    return [{"key": key_value, "net_quantity": quantity} for key_value, quantity in sorted(totals.items())]


def _exposure_basis(positions: list[dict[str, Any]]) -> str:
    if not positions:
        return "NONE"
    if all(_decimal(row.get("current_reference_price")) is not None for row in positions):
        return "NOTIONAL"
    return "QUANTITY_ONLY"


def _exposure_warnings(positions: list[dict[str, Any]]) -> list[str]:
    if positions and _exposure_basis(positions) == "QUANTITY_ONLY":
        return ["EXPOSURE_BASIS_QUANTITY_ONLY"]
    return []


def _gross_exposure(positions: list[dict[str, Any]]) -> str:
    if _exposure_basis(positions) == "NOTIONAL":
        return _decimal_string(sum(abs(Decimal(int(row["quantity"])) * _decimal(row["current_reference_price"])) for row in positions))
    return str(sum(abs(int(row["quantity"])) for row in positions))


def _net_exposure(positions: list[dict[str, Any]]) -> str:
    if _exposure_basis(positions) == "NOTIONAL":
        return _decimal_string(sum(Decimal(int(row["quantity"])) * _decimal(row["current_reference_price"]) for row in positions))
    return str(sum(int(row["quantity"]) for row in positions))


def _trade_class(candidate: dict[str, Any]) -> str:
    instrument = candidate["instrument_type"].upper()
    direction = candidate["direction"].upper()
    if instrument in {"LONG_EQUITY", "EQUITY_SPOT"} and direction == "SHORT":
        return "SHORT_EQUITY"
    if instrument in {"LONG_EQUITY", "EQUITY_SPOT"}:
        return "LONG_EQUITY"
    if instrument in {"LONG_CALL_OPTION", "LONG_PUT_OPTION", "ETF_ROTATION_PAIR"}:
        return instrument
    return instrument or "UNKNOWN"


def _queue_item_blockers(*, candidate: dict[str, Any], trade_class: str) -> list[str]:
    blockers = list(candidate.get("blockers") or [])
    if trade_class not in SUPPORTED_TRADE_CLASSES:
        blockers.append("UNSUPPORTED_MANUAL_EXECUTION")
    if int(candidate.get("suggested_quantity") or 0) <= 0:
        blockers.append("REQUESTED_QUANTITY_MISSING")
    for field_name, reason_code in (
        ("symbol", "SYMBOL_MISSING"),
        ("direction", "DIRECTION_MISSING"),
        ("instrument_type", "INSTRUMENT_TYPE_MISSING"),
        ("entry_reference_price", "ENTRY_REFERENCE_MISSING"),
        ("stop_price", "STOP_RISK_MISSING"),
        ("risk_per_trade", "RISK_PER_TRADE_MISSING"),
    ):
        if not str(candidate.get(field_name) or "").strip() or str(candidate.get(field_name) or "").strip().upper() == "UNKNOWN":
            blockers.append(reason_code)
    if str(candidate.get("executable_status") or "") != "EXECUTABLE":
        blockers.append("CANDIDATE_NOT_EXECUTABLE")
    return sorted(set(blockers))


def _manual_recipe(raw: dict[str, Any], candidate: dict[str, Any], trade_class: str, supported: bool) -> str:
    if not supported:
        return "UNSUPPORTED_MANUAL_EXECUTION"
    account = str(raw.get("account") or raw.get("account_id") or "UNSPECIFIED_ACCOUNT")
    mode = str(raw.get("mode") or raw.get("environment") or "PAPER")
    side = "SELL" if trade_class == "SHORT_EQUITY" else "BUY"
    order_type = str(raw.get("order_type") or "MKT").upper()
    stop_order_type = str(raw.get("stop_order_type") or "STP").upper()
    stop_quantity = int(raw.get("stop_quantity") or candidate["suggested_quantity"])
    return (
        f"Manual {trade_class} account={account} mode={mode} side={side} symbol={candidate['symbol']} "
        f"quantity={candidate['suggested_quantity']} order_type={order_type} "
        f"entry_instruction=use entry_ref {candidate['entry_reference_price']} as manual reference; "
        f"stop_order_type={stop_order_type} stop_price={candidate['stop_price']} stop_quantity={stop_quantity}; "
        "sequence=1 enter position, 2 immediately enter protective stop, 3 confirm stop accepted; "
        "if_price_moved_materially=skip or mark MODIFIED and require operator note; "
        "confirmations=CONFIRM_SYMBOL,CONFIRM_SIZE,CONFIRM_ENTRY,CONFIRM_STOP,CAPTURE_MANUAL_FILL"
    )


def _required_orders(candidate: dict[str, Any], trade_class: str, supported: bool) -> list[str]:
    if not supported:
        return ["UNSUPPORTED_MANUAL_EXECUTION"]
    return [f"{trade_class}_ENTRY", "PROTECTIVE_STOP"]


def _operator_confirmations(supported: bool) -> list[str]:
    if not supported:
        return ["MANUAL_REVIEW_UNSUPPORTED_STRUCTURE"]
    return ["CONFIRM_SYMBOL", "CONFIRM_SIZE", "CONFIRM_ENTRY", "CONFIRM_STOP", "CAPTURE_MANUAL_FILL"]


def _decimal(value: Any) -> Decimal | None:
    try:
        text = str(value or "").strip()
        return Decimal(text) if text else None
    except (InvalidOperation, ValueError):
        return None


def _decimal_string(value: Decimal) -> str:
    if value == value.to_integral_value():
        return str(value.quantize(Decimal("1")))
    return str(value.normalize())


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if str(value):
        return [str(value)]
    return []


def _safe_run_id(run_id: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(run_id or "").strip())
    return cleaned or "aegis_lite_manual_feedback_v1"
