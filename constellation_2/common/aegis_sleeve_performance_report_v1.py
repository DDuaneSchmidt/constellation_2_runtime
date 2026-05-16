from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_research_lab_v1 import build_research_queue_task_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_performance_report.v1.schema.json"
LIFECYCLE_STATUSES = {
    "RECOMMENDED_NOT_EXECUTED",
    "EXECUTED_OPEN",
    "EXECUTED_CLOSED",
    "MISSING_RECEIPT",
    "MISSING_OUTCOME",
    "INVALID_PACKET",
    "BLOCKED",
    "IGNORED",
    "MISSED_VALIDITY_WINDOW",
}


def sleeve_performance_report_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "sleeve_performance_report_v1" / day_utc / "sleeve_performance_report.v1.json"


def validate_sleeve_performance_report_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)


def write_sleeve_performance_report_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_sleeve_performance_report_v1(payload)
    path = sleeve_performance_report_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_sleeve_performance_report_v1(
    *,
    day_utc: str,
    generated_at_utc: str,
    manual_trade_packets: list[dict[str, Any]],
    manual_execution_receipts: list[dict[str, Any]] | None = None,
    outcome_ledgers: list[dict[str, Any]] | None = None,
    trade_outcome_attributions: list[dict[str, Any]] | None = None,
    promoted_sleeve_libraries: list[dict[str, Any]] | None = None,
    event_tactical_packets: list[dict[str, Any]] | None = None,
    trade_capture_alert_ledgers: list[dict[str, Any]] | None = None,
    regime_event_context: dict[str, Any] | None = None,
    source_artifact_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    receipts = _objects(manual_execution_receipts)
    outcomes = [row for ledger in _objects(outcome_ledgers) for row in _objects(ledger.get("outcomes"))]
    attributions = [row for artifact in _objects(trade_outcome_attributions) for row in _objects(artifact.get("attributions"))]
    events = _objects(event_tactical_packets)
    alerts = [row for ledger in _objects(trade_capture_alert_ledgers) for row in _objects(ledger.get("alert_attempts"))]
    promoted = _promoted_sleeves(promoted_sleeve_libraries or [])

    recommendation_rows = []
    for packet in _objects(manual_trade_packets):
        for row in _objects(packet.get("trade_candidates")):
            recommendation_rows.append({**row, "source_packet_id": str(packet.get("packet_id") or "")})
    recommendation_rows = sorted(recommendation_rows, key=lambda row: (str(row.get("sleeve_id") or ""), str(row.get("recommended_trade_id") or "")))

    diagnostics: list[dict[str, Any]] = []
    duplicate_recommendations = _duplicates(recommendation_rows, "recommended_trade_id")
    duplicate_receipts = _duplicates(receipts, "recommended_trade_id")
    duplicate_outcomes = _duplicates(outcomes, "trade_id")
    for label, values in [
        ("DUPLICATE_RECOMMENDATION", duplicate_recommendations),
        ("DUPLICATE_RECEIPT", duplicate_receipts),
        ("DUPLICATE_OUTCOME", duplicate_outcomes),
    ]:
        for value in values:
            diagnostics.append({"severity": "BLOCKING", "reason_code": label, "trade_id": value})

    receipts_by_trade = _index_first(receipts, "recommended_trade_id")
    outcomes_by_trade = _index_first(outcomes, "trade_id")
    attributions_by_trade = _index_first(attributions, "candidate_id")
    events_by_trade = _index_first(events, "recommended_trade_id")
    alerts_by_source_packet = _index_first(alerts, "source_packet_id")
    alerts_by_alert_id = _index_first(alerts, "alert_id")

    trade_rows = []
    for recommendation in recommendation_rows:
        trade_id = str(recommendation.get("recommended_trade_id") or "")
        receipt = receipts_by_trade.get(trade_id, {})
        outcome = outcomes_by_trade.get(trade_id, {})
        attribution = attributions_by_trade.get(trade_id, {})
        event = events_by_trade.get(trade_id, {})
        alert_id = str(receipt.get("alert_id") or outcome.get("alert_id") or "")
        alert = alerts_by_alert_id.get(alert_id) or alerts_by_source_packet.get(trade_id, {})
        row_diagnostics = []
        if trade_id in duplicate_recommendations:
            row_diagnostics.append("DUPLICATE_RECOMMENDATION")
        if trade_id in duplicate_receipts:
            row_diagnostics.append("DUPLICATE_RECEIPT")
        if trade_id in duplicate_outcomes:
            row_diagnostics.append("DUPLICATE_OUTCOME")
        lifecycle_status = _lifecycle_status(recommendation=recommendation, receipt=receipt, outcome=outcome, diagnostics=row_diagnostics)
        recommended_entry = _decimal(recommendation.get("entry_reference_price"))
        actual_entry = _decimal(receipt.get("fill_price") or outcome.get("actual_entry") or attribution.get("actual_entry_price"))
        slippage = _slippage(recommended_entry=recommended_entry, actual_entry=actual_entry)
        row = {
            "trade_id": trade_id,
            "lifecycle_status": lifecycle_status,
            "sleeve_id": str(recommendation.get("sleeve_id") or outcome.get("sleeve_id") or ""),
            "edge_family": str(recommendation.get("edge_family") or _get(promoted, str(recommendation.get("sleeve_id") or ""), "edge_family") or ""),
            "source_hypothesis_id": str(recommendation.get("source_hypothesis_id") or outcome.get("hypothesis_id") or ""),
            "symbol": str(recommendation.get("symbol") or receipt.get("actual_symbol") or "").upper(),
            "side": str(recommendation.get("side") or receipt.get("actual_side") or "").upper(),
            "recommended_entry": str(recommendation.get("entry_reference_price") or outcome.get("recommended_entry") or ""),
            "actual_fill": str(receipt.get("fill_price") or outcome.get("actual_entry") or attribution.get("actual_entry_price") or ""),
            "suggested_quantity": int(recommendation.get("suggested_quantity") or receipt.get("suggested_quantity") or outcome.get("suggested_quantity") or 0),
            "actual_quantity": int(receipt.get("actual_quantity") or outcome.get("actual_quantity") or 0),
            "expected_risk": str(receipt.get("expected_risk") or outcome.get("expected_risk") or recommendation.get("max_loss_if_stopped") or ""),
            "actual_risk": str(receipt.get("actual_risk") or outcome.get("actual_risk") or ""),
            "quantity_override": bool(receipt.get("quantity_override", False)) if receipt else bool(outcome.get("operator_override_reason")),
            "operator_override_reason": str(receipt.get("operator_override_reason") or outcome.get("operator_override_reason") or ""),
            "sizing_quality": str(receipt.get("sizing_quality") or outcome.get("sizing_quality") or ""),
            "slippage": slippage["absolute"],
            "slippage_pct": slippage["pct"],
            "recommended_stop": str(recommendation.get("stop_price") or outcome.get("recommended_stop") or ""),
            "actual_stop": str(receipt.get("stop_price") or outcome.get("actual_stop") or ""),
            "stop_entered": bool(receipt.get("stop_order_entered", False)) if receipt else False,
            "stop_matched_recommendation": _stop_matches(recommendation, receipt),
            "fill_before_valid_until": _prefer_receipt_bool(receipt=receipt, outcome=outcome, receipt_key="fill_before_valid_until", outcome_key="valid_until_respected"),
            "max_entry_slippage_respected": _prefer_receipt_bool(receipt=receipt, outcome=outcome, receipt_key="max_entry_slippage_respected", outcome_key="entry_slippage_respected"),
            "valid_until_respected": _prefer_receipt_bool(receipt=receipt, outcome=outcome, receipt_key="fill_before_valid_until", outcome_key="valid_until_respected"),
            "entry_slippage_respected": _prefer_receipt_bool(receipt=receipt, outcome=outcome, receipt_key="max_entry_slippage_respected", outcome_key="entry_slippage_respected"),
            "outcome_status": str(outcome.get("outcome_status") or attribution.get("sleeve_signal_quality") or ""),
            "return_pct": str(outcome.get("return_pct") or ""),
            "risk_adjusted_return": str(outcome.get("risk_adjusted_return") or ""),
            "max_adverse_excursion": str(outcome.get("max_adverse_excursion") or attribution.get("MAE") or ""),
            "max_favorable_excursion": str(outcome.get("max_favorable_excursion") or attribution.get("MFE") or ""),
            "failure_reason": str(outcome.get("failure_reason") or ""),
            "operator_deviation": str(outcome.get("operator_deviation") or "; ".join(_strings(receipt.get("deviations_from_recommendation"))) or ""),
            "sleeve_attribution": str(outcome.get("sleeve_attribution") or ""),
            "edge_overlap_attribution": str(outcome.get("edge_overlap_attribution") or recommendation.get("edge_overlap_result") or ""),
            "regime_state": str(recommendation.get("regime_state") or (regime_event_context or {}).get("regime_state") or ""),
            "event_id": str(event.get("event_id") or receipt.get("event_id") or outcome.get("event_id") or ""),
            "event_type": str(event.get("event_type") or outcome.get("event_type") or ""),
            "alert_id": alert_id or str(alert.get("alert_id") or ""),
            "alert_gate_status": str(alert.get("alert_gate_status") or outcome.get("alert_gate_status") or ""),
            "execution_sensitivity": str(event.get("execution_sensitivity") or outcome.get("execution_sensitivity") or ""),
            "source_packet_type": str(receipt.get("source_packet_type") or outcome.get("source_packet_type") or "EOD_MANUAL_PACKET"),
            "diagnostics": row_diagnostics,
        }
        trade_rows.append(row)

    sleeve_rows = _sleeve_summary(trade_rows, promoted)
    research_tasks = _research_task_recommendations(trade_rows, generated_at_utc=generated_at_utc)
    portfolio = _portfolio_summary(trade_rows)
    payload = {
        "schema_id": "sleeve_performance_report",
        "schema_version": "v1",
        "artifact_id": "sleeve_performance_report_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc,
        "portfolio_summary": portfolio,
        "sleeve_summary": sleeve_rows,
        "execution_quality": _execution_quality(trade_rows),
        "outcome_quality": _outcome_quality(trade_rows),
        "research_feedback": {
            "generated_or_recommended_tasks": research_tasks,
            "task_count": len(research_tasks),
            "writes_research_task_queue": False,
            "automatic_promotion_allowed": False,
            "lite_runtime_mutation_allowed": False,
        },
        "trade_lifecycle_rows": trade_rows,
        "join_diagnostics": diagnostics,
        "input_counts": {
            "manual_trade_packets": len(_objects(manual_trade_packets)),
            "manual_execution_receipts": len(receipts),
            "outcome_rows": len(outcomes),
            "trade_outcome_attributions": len(attributions),
            "promoted_sleeves": len(promoted),
            "event_tactical_packets": len(events),
            "trade_capture_alert_attempts": len(alerts),
        },
        "source_artifact_lineage": source_artifact_lineage or [],
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "runtime_mutation_allowed": False,
        "canonical_eod_state_mutated": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _portfolio_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    executed = [row for row in rows if row["lifecycle_status"] in {"EXECUTED_OPEN", "EXECUTED_CLOSED", "MISSING_OUTCOME"}]
    closed_returns = [_return_pct(row) for row in rows if row["lifecycle_status"] == "EXECUTED_CLOSED" and _return_pct(row) is not None]
    return {
        "total_recommended_trades": len(rows),
        "total_executed_trades": len(executed),
        "total_ignored_missed_trades": sum(1 for row in rows if row["lifecycle_status"] in {"IGNORED", "MISSED_VALIDITY_WINDOW", "RECOMMENDED_NOT_EXECUTED"}),
        "total_return": _fmt(sum(closed_returns)) if closed_returns else "",
        "realized_return": _fmt(sum(closed_returns)) if closed_returns else "",
        "open_trade_mark": "",
        "cash_unexecuted_exposure": "",
        "max_drawdown": _fmt(min([_decimal(row.get("max_adverse_excursion")) for row in rows if _decimal(row.get("max_adverse_excursion")) is not None] or [0])),
        "advisor_benchmark_comparison": "",
        "missing_receipt_count": sum(1 for row in rows if row["lifecycle_status"] == "MISSING_RECEIPT"),
        "missing_outcome_count": sum(1 for row in rows if row["lifecycle_status"] == "MISSING_OUTCOME"),
    }


def _sleeve_summary(rows: list[dict[str, Any]], promoted: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("sleeve_id") or "UNKNOWN"), []).append(row)
    summaries = []
    for sleeve_id in sorted(grouped):
        sleeve_rows = grouped[sleeve_id]
        returns = [_return_pct(row) for row in sleeve_rows if row["lifecycle_status"] == "EXECUTED_CLOSED" and _return_pct(row) is not None]
        wins = [value for value in returns if value > 0]
        losses = [value for value in returns if value < 0]
        mae_values = [_decimal(row.get("max_adverse_excursion")) for row in sleeve_rows if _decimal(row.get("max_adverse_excursion")) is not None]
        mfe_values = [_decimal(row.get("max_favorable_excursion")) for row in sleeve_rows if _decimal(row.get("max_favorable_excursion")) is not None]
        summary = {
            "sleeve_id": sleeve_id,
            "edge_family": str(sleeve_rows[0].get("edge_family") or _get(promoted, sleeve_id, "edge_family") or ""),
            "source_hypothesis_id": str(sleeve_rows[0].get("source_hypothesis_id") or _get(promoted, sleeve_id, "source_hypothesis_id") or ""),
            "recommended_trade_count": len(sleeve_rows),
            "executed_trade_count": sum(1 for row in sleeve_rows if row["lifecycle_status"] in {"EXECUTED_OPEN", "EXECUTED_CLOSED", "MISSING_OUTCOME"}),
            "missed_ignored_trade_count": sum(1 for row in sleeve_rows if row["lifecycle_status"] in {"IGNORED", "MISSED_VALIDITY_WINDOW", "RECOMMENDED_NOT_EXECUTED"}),
            "total_return": _fmt(sum(returns)) if returns else "",
            "average_return": _fmt(sum(returns) / len(returns)) if returns else "",
            "win_rate": _fmt((len(wins) / len(returns)) * 100) if returns else "",
            "average_win": _fmt(sum(wins) / len(wins)) if wins else "",
            "average_loss": _fmt(sum(losses) / len(losses)) if losses else "",
            "stop_hit_rate": _fmt((sum(1 for row in sleeve_rows if _stop_hit(row)) / len(sleeve_rows)) * 100) if sleeve_rows else "",
            "max_adverse_excursion": _fmt(min(mae_values)) if mae_values else "",
            "max_favorable_excursion": _fmt(max(mfe_values)) if mfe_values else "",
            "regime_performance": _group_performance(sleeve_rows, "regime_state"),
            "event_performance": _group_performance(sleeve_rows, "event_type"),
            "alert_driven_performance": _group_performance([row for row in sleeve_rows if row.get("alert_id")], "alert_gate_status"),
        }
        summaries.append(summary)
    return summaries


def _execution_quality(rows: list[dict[str, Any]]) -> dict[str, Any]:
    slippages = [_decimal(row.get("slippage")) for row in rows if _decimal(row.get("slippage")) is not None]
    return {
        "trade_count": len(rows),
        "average_slippage": _fmt(sum(slippages) / len(slippages)) if slippages else "",
        "missing_receipt_count": sum(1 for row in rows if row["lifecycle_status"] == "MISSING_RECEIPT"),
        "fills_before_valid_until": sum(1 for row in rows if row.get("fill_before_valid_until")),
        "max_entry_slippage_respected_count": sum(1 for row in rows if row.get("max_entry_slippage_respected")),
        "stop_entered_count": sum(1 for row in rows if row.get("stop_entered")),
        "stop_matched_recommendation_count": sum(1 for row in rows if row.get("stop_matched_recommendation")),
        "operator_deviation_count": sum(1 for row in rows if row.get("operator_deviation")),
        "quantity_override_count": sum(1 for row in rows if row.get("quantity_override")),
        "sizing_quality_rows": [
            {
                "trade_id": row["trade_id"],
                "suggested_quantity": row["suggested_quantity"],
                "actual_quantity": row["actual_quantity"],
                "expected_risk": row["expected_risk"],
                "actual_risk": row["actual_risk"],
                "sizing_quality": row["sizing_quality"],
                "operator_override_reason": row["operator_override_reason"],
            }
            for row in rows
        ],
        "rows": [
            {
                "trade_id": row["trade_id"],
                "recommended_entry": row["recommended_entry"],
                "actual_fill": row["actual_fill"],
                "slippage": row["slippage"],
                "slippage_pct": row["slippage_pct"],
                "stop_entered": row["stop_entered"],
                "stop_matched_recommendation": row["stop_matched_recommendation"],
                "operator_deviation": row["operator_deviation"],
            }
            for row in rows
        ],
    }


def _outcome_quality(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "rows": [
            {
                "trade_id": row["trade_id"],
                "lifecycle_status": row["lifecycle_status"],
                "outcome_status": row["outcome_status"],
                "return_pct": row["return_pct"],
                "risk_adjusted_return": row["risk_adjusted_return"],
                "stop_behavior": "STOP_ENTERED" if row["stop_entered"] else "STOP_NOT_ENTERED",
                "failure_reason": row["failure_reason"],
                "sleeve_attribution": row["sleeve_attribution"],
                "edge_overlap_attribution": row["edge_overlap_attribution"],
            }
            for row in rows
        ]
    }


def _research_task_recommendations(rows: list[dict[str, Any]], *, generated_at_utc: str) -> list[dict[str, Any]]:
    tasks: dict[str, dict[str, Any]] = {}
    for row in rows:
        hypothesis_id = str(row.get("source_hypothesis_id") or "")
        trade_id = str(row.get("trade_id") or "")
        if not hypothesis_id or not trade_id:
            continue
        reasons = []
        status = str(row.get("lifecycle_status") or "")
        outcome = str(row.get("outcome_status") or "").lower()
        failure = str(row.get("failure_reason") or "").lower()
        if status in {"MISSED_VALIDITY_WINDOW", "MISSING_OUTCOME"}:
            reasons.append(status)
        if outcome in {"loss", "stopped_out", "underperformed"} or "stop" in failure:
            reasons.append("SLEEVE_OR_STOP_FAILURE")
        overlap_attribution = str(row.get("edge_overlap_attribution") or "").lower()
        if "overlap_failure" in overlap_attribution or "overlap failure" in overlap_attribution or "correlation" in failure or "correlated" in failure:
            reasons.append("EDGE_OVERLAP_FAILURE")
        if row.get("event_id") and (outcome in {"loss", "stopped_out"} or not row.get("valid_until_respected")):
            reasons.append("EVENT_FAILURE_OR_STALE_ENTRY")
        if row.get("alert_id") and outcome in {"loss", "stopped_out"}:
            reasons.append("ALERT_FALSE_POSITIVE_REVIEW")
        if not row.get("entry_slippage_respected") or str(row.get("operator_deviation") or ""):
            reasons.append("OPERATOR_SLIPPAGE_OR_DEVIATION")
        for reason in sorted(set(reasons)):
            task_id = f"task:{hypothesis_id}:sleeve_performance:{_safe_id(trade_id)}:{_safe_id(reason)}"
            tasks[task_id] = build_research_queue_task_v1(
                task_id=task_id,
                hypothesis_id=hypothesis_id,
                task_type="FAILURE_MODE_REVIEW",
                priority="HIGH" if reason in {"SLEEVE_OR_STOP_FAILURE", "MISSED_VALIDITY_WINDOW"} else "NORMAL",
                status="QUEUED",
                required_inputs=[f"sleeve_performance_report.v1:{trade_id}"],
                output_artifact_refs=[],
                blocker_reason_codes=[reason],
                created_at_utc=generated_at_utc,
            )
    return [tasks[key] for key in sorted(tasks)]


def _lifecycle_status(*, recommendation: dict[str, Any], receipt: dict[str, Any], outcome: dict[str, Any], diagnostics: list[str]) -> str:
    if diagnostics:
        return "INVALID_PACKET"
    if not bool(recommendation.get("actionable", False)) or _strings(recommendation.get("do_not_trade_blockers")):
        return "BLOCKED"
    outcome_status = str(outcome.get("outcome_status") or "").strip().upper()
    action = str(outcome.get("operator_action_taken") or "").strip().upper()
    failure = str(outcome.get("failure_reason") or "").strip().upper()
    if not receipt:
        if action in {"IGNORED", "SKIPPED"} or outcome_status in {"IGNORED", "SKIPPED"}:
            return "IGNORED"
        if "VALIDITY" in failure or outcome_status == "MISSED_VALIDITY_WINDOW":
            return "MISSED_VALIDITY_WINDOW"
        if outcome_status in {"RECOMMENDED_NOT_EXECUTED", "NOT_EXECUTED", "MISSED_OPPORTUNITY"}:
            return "RECOMMENDED_NOT_EXECUTED"
        return "MISSING_RECEIPT"
    if not outcome:
        return "MISSING_OUTCOME"
    if outcome_status in {"OPEN", "EXECUTED_OPEN", "UNREALIZED", "PENDING"}:
        return "EXECUTED_OPEN"
    return "EXECUTED_CLOSED"


def _group_performance(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        label = str(row.get(key) or "UNKNOWN")
        grouped.setdefault(label, []).append(row)
    result = []
    for label in sorted(grouped):
        values = [_return_pct(row) for row in grouped[label] if row["lifecycle_status"] == "EXECUTED_CLOSED" and _return_pct(row) is not None]
        result.append({"name": label, "trade_count": len(grouped[label]), "total_return": _fmt(sum(values)) if values else ""})
    return result


def _promoted_sleeves(libraries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for library in libraries:
        for row in _objects(library.get("sleeves")) or _objects(library.get("promoted_sleeves")):
            sleeve_id = str(row.get("sleeve_id") or "")
            if not sleeve_id:
                continue
            rows[sleeve_id] = {
                "source_hypothesis_id": str(row.get("source_hypothesis_id") or row.get("research_hypothesis_id") or ""),
                "edge_family": str((row.get("approved_edge_families") or [""])[0] if isinstance(row.get("approved_edge_families"), list) else row.get("edge_family") or ""),
            }
    return rows


def _index_first(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    indexed = {}
    for row in sorted(rows, key=lambda item: str(item.get(key) or "")):
        value = str(row.get(key) or "")
        if value and value not in indexed:
            indexed[value] = row
    return indexed


def _duplicates(rows: list[dict[str, Any]], key: str) -> set[str]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "")
        if value:
            counts[value] = counts.get(value, 0) + 1
    return {value for value, count in counts.items() if count > 1}


def _objects(value: Any) -> list[dict[str, Any]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _decimal(value: Any) -> float | None:
    text = str(value if value is not None else "").strip().replace("%", "").replace("pct", "").replace("$", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _return_pct(row: dict[str, Any]) -> float | None:
    return _decimal(row.get("return_pct"))


def _fmt(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".rstrip("0").rstrip(".")


def _slippage(*, recommended_entry: float | None, actual_entry: float | None) -> dict[str, str]:
    if recommended_entry is None or actual_entry is None:
        return {"absolute": "", "pct": ""}
    absolute = actual_entry - recommended_entry
    pct = (absolute / recommended_entry) * 100 if recommended_entry else 0.0
    return {"absolute": _fmt(absolute), "pct": _fmt(pct)}


def _stop_matches(recommendation: dict[str, Any], receipt: dict[str, Any]) -> bool:
    if not receipt:
        return False
    recommended = _decimal(recommendation.get("stop_price"))
    actual = _decimal(receipt.get("stop_price"))
    return recommended is not None and actual is not None and abs(recommended - actual) < 0.0001


def _prefer_receipt_bool(*, receipt: dict[str, Any], outcome: dict[str, Any], receipt_key: str, outcome_key: str) -> bool:
    if receipt:
        return bool(receipt.get(receipt_key, False))
    return bool(outcome.get(outcome_key, False))


def _stop_hit(row: dict[str, Any]) -> bool:
    text = f"{row.get('outcome_status', '')} {row.get('failure_reason', '')}".lower()
    return "stop" in text


def _get(rows: dict[str, dict[str, Any]], key: str, field: str) -> str:
    return str(rows.get(key, {}).get(field) or "")


def _safe_id(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    return cleaned or "unknown"


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
