from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.paper_position_ledger_v1 import paper_position_ledger_path_v1
from ops.aegis.sleeve_evidence_certification_v1 import sleeve_evidence_certification_path_v1
from ops.aegis.sleeve_performance_truth_v1 import sleeve_performance_truth_path_v1


REPORT_FAMILY = "aegis_trend_eq_realized_vs_unrealized_diagnostic_v1"
JSON_FILENAME = "trend_eq_realized_vs_unrealized_diagnostic.v1.json"
SUMMARY_FILENAME = "trend_eq_realized_vs_unrealized_diagnostic_summary.md"
TARGET_SLEEVE_ID = "C2_TREND_EQ_PRIMARY_V1"
TREND_EQ_HYPOTHESIS_HORIZON_MIN_DAYS = 20
TREND_EQ_HYPOTHESIS_HORIZON_MAX_DAYS = 60
SUFFICIENT_SAMPLE_THRESHOLD = 20

SAFETY = {
    "review_only": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_policy_changed": False,
    "runtime_policy_changed": False,
    "no_trade_recommendations": True,
    "no_investable_edge_claims": True,
}


def trend_eq_realized_vs_unrealized_diagnostic_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / JSON_FILENAME


def trend_eq_realized_vs_unrealized_diagnostic_summary_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / SUMMARY_FILENAME


def build_trend_eq_realized_vs_unrealized_diagnostic_v1(
    *, truth_root: Path | str, day_utc: str, sleeve_id: str = TARGET_SLEEVE_ID
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    source_paths = _source_paths(root, day)
    payloads = {name: _read_json(path) for name, path in source_paths.items()}
    missing_required = [name for name, path in source_paths.items() if not path.exists()]

    ledger = payloads["paper_position_ledger"]
    sleeve_truth = payloads["sleeve_performance_truth"]
    certification = payloads["sleeve_evidence_certification"]

    open_positions = _rows_for_sleeve(ledger, sleeve_id, "open_positions")
    closed_positions = _rows_for_sleeve(ledger, sleeve_id, "closed_positions")
    all_positions = _dedupe_positions(_rows_for_sleeve(ledger, sleeve_id, "positions") + open_positions + closed_positions)
    truth_row = _row_for_sleeve(sleeve_truth, sleeve_id, "sleeves")
    cert_row = _row_for_sleeve(certification, sleeve_id, "sleeves")

    closed_diagnostics = [_closed_position_diagnostic(row) for row in closed_positions]
    open_diagnostics = [_open_position_diagnostic(row, day) for row in open_positions]
    realized_values = [_number(row.get("realized_pnl")) or Decimal("0") for row in closed_positions]
    unrealized_values = [_number(row.get("unrealized_pnl")) or Decimal("0") for row in open_positions]

    realized_pnl = _sum(realized_values)
    unrealized_pnl = _sum(unrealized_values)
    net_pnl = realized_pnl + unrealized_pnl
    average_gain = _average([value for value in realized_values if value > 0])
    average_loss = _average([value for value in realized_values if value < 0])
    win_count = len([value for value in realized_values if value > 0])
    loss_count = len([value for value in realized_values if value < 0])
    outcome_count = win_count + loss_count
    win_rate = Decimal(win_count) / Decimal(outcome_count) if outcome_count else None
    expected_value = _expected_value(win_rate, average_gain, average_loss)
    closed_holding_days = [row["holding_period_days"] for row in closed_diagnostics if row["holding_period_days"] is not None]
    open_age_days = [row["open_age_days"] for row in open_diagnostics if row["open_age_days"] is not None]

    diagnostics = {
        "bad_entries": _bad_entries_diagnostic(
            closed_count=len(closed_positions),
            win_count=win_count,
            loss_count=loss_count,
            realized_pnl=realized_pnl,
            average_gain=average_gain,
            average_loss=average_loss,
            expected_value=expected_value,
        ),
        "bad_exits": _bad_exits_diagnostic(
            closed_positions=closed_positions,
            closed_diagnostics=closed_diagnostics,
            realized_pnl=realized_pnl,
            expected_value=expected_value,
        ),
        "too_short_holding_period": _too_short_holding_period_diagnostic(
            closed_holding_days=closed_holding_days,
            open_age_days=open_age_days,
        ),
        "concentration_in_few_winners": _winner_concentration_diagnostic(open_positions),
        "temporary_unrealized_mark_to_market_noise": _mark_to_market_noise_diagnostic(
            closed_count=len(closed_positions),
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            open_age_days=open_age_days,
        ),
    }
    diagnostic_conclusion = _diagnostic_conclusion(diagnostics, realized_pnl, unrealized_pnl, len(closed_positions))

    payload = {
        "schema_id": "aegis_trend_eq_realized_vs_unrealized_diagnostic",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": _now(),
        "truth_model": "CANONICAL_DERIVED_READ_MODEL_FROM_EXISTING_AUTHORITIES",
        "purpose": "Diagnose whether Trend EQ realized versus unrealized outcomes point to bad entries, bad exits, too-short holding periods, winner concentration, or temporary mark-to-market noise.",
        "sleeve_id": sleeve_id,
        "rules": {
            "read_model_only_no_signal_logic_change": True,
            "read_model_only_no_sleeve_logic_change": True,
            "read_model_only_no_entry_logic_change": True,
            "read_model_only_no_exit_logic_change": True,
            "no_new_closures_created": True,
            "no_trade_recommendations": True,
            "no_investable_edge_claims": True,
        },
        "thresholds": {
            "sufficient_sample_threshold": SUFFICIENT_SAMPLE_THRESHOLD,
            "trend_eq_hypothesis_horizon_min_days": TREND_EQ_HYPOTHESIS_HORIZON_MIN_DAYS,
            "trend_eq_hypothesis_horizon_max_days": TREND_EQ_HYPOTHESIS_HORIZON_MAX_DAYS,
            "concentrated_top_one_positive_unrealized_share": "0.300000",
            "concentrated_top_three_positive_unrealized_share": "0.500000",
        },
        "summary": {
            "sleeve_id": sleeve_id,
            "factory_classification": _factory_classification(truth_row, cert_row),
            "total_positions": len(all_positions),
            "active_position_count": len(open_positions),
            "closed_position_count": len(closed_positions),
            "realized_pnl": _decimal_text(realized_pnl),
            "unrealized_pnl": _decimal_text(unrealized_pnl),
            "net_pnl": _decimal_text(net_pnl),
            "win_rate": _decimal_text(win_rate),
            "average_gain": _decimal_text(average_gain),
            "average_loss": _decimal_text(average_loss),
            "expected_value": _decimal_text(expected_value),
            "closed_average_holding_period_days": _decimal_text(_average([Decimal(str(v)) for v in closed_holding_days])),
            "open_average_age_days": _decimal_text(_average([Decimal(str(v)) for v in open_age_days])),
            "sample_status": cert_row.get("sample_status") or _sample_status(len(closed_positions)),
            "evidence_status": cert_row.get("evidence_status") or "NOT_EVALUABLE",
            "diagnostic_conclusion": diagnostic_conclusion,
        },
        "diagnostics": diagnostics,
        "closed_position_diagnostics": closed_diagnostics,
        "open_position_diagnostics": open_diagnostics,
        "source_artifact_paths": {name: str(path) for name, path in source_paths.items() if path.exists()},
        "source_hashes": {name: _sha256(path) for name, path in source_paths.items() if path.exists()},
        "missing_required_inputs": missing_required,
        "data_quality_status": "DATA_INCOMPLETE" if missing_required else "PASS",
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_trend_eq_realized_vs_unrealized_diagnostic_v1(
    *, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None
) -> dict[str, Path]:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_trend_eq_realized_vs_unrealized_diagnostic_v1(truth_root=root, day_utc=day_utc))
    json_path = trend_eq_realized_vs_unrealized_diagnostic_path_v1(truth_root=root, day_utc=day_utc)
    summary_path = trend_eq_realized_vs_unrealized_diagnostic_summary_path_v1(truth_root=root, day_utc=day_utc)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(json_path, json.dumps(body, indent=2, sort_keys=True) + "\n")
    _atomic_write(summary_path, render_trend_eq_realized_vs_unrealized_diagnostic_summary_v1(body))
    return {"json": json_path, "markdown": summary_path}


def render_trend_eq_realized_vs_unrealized_diagnostic_summary_v1(payload: Mapping[str, Any]) -> str:
    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), Mapping) else {}
    diagnostics = payload.get("diagnostics", {}) if isinstance(payload.get("diagnostics"), Mapping) else {}
    lines = [
        "# AEGIS Trend EQ Realized vs Unrealized Diagnostic V1",
        "",
        f"Day UTC: {payload.get('day_utc', '')}",
        "",
        "Paper research positions are research observations, not investment recommendations. This artifact makes no trade recommendations and no investable-edge claims.",
        "",
        "## Summary",
        "",
        f"- Sleeve: {summary.get('sleeve_id', '')}",
        f"- Active positions: {summary.get('active_position_count', 0)}",
        f"- Closed positions: {summary.get('closed_position_count', 0)}",
        f"- Realized P&L: {summary.get('realized_pnl', '0')}",
        f"- Unrealized P&L: {summary.get('unrealized_pnl', '0')}",
        f"- Net P&L: {summary.get('net_pnl', '0')}",
        f"- Sample status: {summary.get('sample_status', '')}",
        f"- Evidence status: {summary.get('evidence_status', '')}",
        f"- Diagnostic conclusion: {', '.join(str(item) for item in summary.get('diagnostic_conclusion', []))}",
        "",
        "## Diagnostic Areas",
        "",
    ]
    for key in (
        "bad_entries",
        "bad_exits",
        "too_short_holding_period",
        "concentration_in_few_winners",
        "temporary_unrealized_mark_to_market_noise",
    ):
        row = diagnostics.get(key, {}) if isinstance(diagnostics.get(key), Mapping) else {}
        lines.append(f"- {key}: {row.get('status', 'NOT_EVALUABLE')}")
        reasons = row.get("reasons") if isinstance(row.get("reasons"), list) else []
        if reasons:
            lines.append(f"  Reasons: {', '.join(str(reason) for reason in reasons)}")
    lines.append("")
    return "\n".join(lines)


def _source_paths(root: Path, day_utc: str) -> dict[str, Path]:
    return {
        "paper_position_ledger": paper_position_ledger_path_v1(truth_root=root, day_utc=day_utc),
        "sleeve_performance_truth": sleeve_performance_truth_path_v1(truth_root=root, day_utc=day_utc),
        "sleeve_evidence_certification": sleeve_evidence_certification_path_v1(truth_root=root, day_utc=day_utc),
    }


def _closed_position_diagnostic(row: Mapping[str, Any]) -> dict[str, Any]:
    realized = _number(row.get("realized_pnl")) or Decimal("0")
    side = str(row.get("side") or "").upper()
    entry = _number(row.get("entry_price"))
    exit_price = _number(row.get("exit_price"))
    quantity = abs(_number(row.get("quantity")) or Decimal("0"))
    flags: list[str] = []
    expected_long_move = None
    if side not in {"BUY", "LONG", "COVER", ""}:
        flags.append("SIDE_ENCODING_AMBIGUOUS_FOR_REALIZED_PNL")
    if entry is not None and exit_price is not None and quantity:
        expected_long_move = (exit_price - entry) * quantity
        if side in {"BUY", "LONG", "COVER", ""} and _sign(expected_long_move) != _sign(realized) and realized != 0:
            flags.append("REALIZED_PNL_SIGN_INCONSISTENT_WITH_LONG_PRICE_MOVE")
    return {
        "position_id": _position_id(row),
        "symbol": row.get("symbol"),
        "entry_time": row.get("entry_time") or row.get("originating_day"),
        "exit_time": row.get("exit_time") or row.get("outcome_timestamp"),
        "holding_period_days": _holding_days(row.get("entry_time") or row.get("originating_day"), row.get("exit_time") or row.get("outcome_timestamp")),
        "entry_price": _decimal_text(entry),
        "exit_price": _decimal_text(exit_price),
        "quantity": _decimal_text(quantity),
        "side": row.get("side"),
        "realized_pnl": _decimal_text(realized),
        "expected_long_price_move_pnl": _decimal_text(expected_long_move),
        "exit_trigger": _exit_trigger(row),
        "diagnostic_flags": flags,
    }


def _open_position_diagnostic(row: Mapping[str, Any], day_utc: str) -> dict[str, Any]:
    unrealized = _number(row.get("unrealized_pnl")) or Decimal("0")
    entry = _number(row.get("entry_price"))
    mark = _number(row.get("mark_price")) or _number(row.get("current_certified_mark"))
    return {
        "position_id": _position_id(row),
        "symbol": row.get("symbol"),
        "entry_time": row.get("entry_time") or row.get("originating_day"),
        "mark_timestamp_utc": row.get("mark_timestamp_utc"),
        "open_age_days": _holding_days(row.get("entry_time") or row.get("originating_day"), row.get("mark_timestamp_utc") or day_utc),
        "entry_price": _decimal_text(entry),
        "mark_price": _decimal_text(mark),
        "unrealized_pnl": _decimal_text(unrealized),
        "mark_to_market_status": "POSITIVE_UNREALIZED" if unrealized > 0 else "NEGATIVE_UNREALIZED" if unrealized < 0 else "FLAT_UNREALIZED",
    }


def _bad_entries_diagnostic(
    *,
    closed_count: int,
    win_count: int,
    loss_count: int,
    realized_pnl: Decimal,
    average_gain: Decimal | None,
    average_loss: Decimal | None,
    expected_value: Decimal | None,
) -> dict[str, Any]:
    if closed_count == 0:
        return {"status": "NOT_EVALUABLE", "reasons": ["NO_CLOSED_POSITIONS"]}
    loss_rate = Decimal(loss_count) / Decimal(win_count + loss_count) if win_count + loss_count else Decimal("0")
    reasons = []
    if realized_pnl < 0:
        reasons.append("REALIZED_PNL_NEGATIVE")
    if expected_value is not None and expected_value < 0:
        reasons.append("REALIZED_EXPECTED_VALUE_NEGATIVE")
    if loss_rate >= Decimal("0.60"):
        reasons.append("CLOSED_LOSS_RATE_AT_OR_ABOVE_60_PERCENT")
    status = "POSSIBLE_BAD_ENTRIES_OR_ADVERSE_SELECTION" if reasons else "NO_ENTRY_FAILURE_SIGNAL_IN_CLOSED_SAMPLE"
    return {
        "status": status,
        "closed_count": closed_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "loss_rate": _decimal_text(loss_rate),
        "realized_pnl": _decimal_text(realized_pnl),
        "average_gain": _decimal_text(average_gain),
        "average_loss": _decimal_text(average_loss),
        "expected_value": _decimal_text(expected_value),
        "reasons": reasons,
    }


def _bad_exits_diagnostic(
    *,
    closed_positions: list[Mapping[str, Any]],
    closed_diagnostics: list[Mapping[str, Any]],
    realized_pnl: Decimal,
    expected_value: Decimal | None,
) -> dict[str, Any]:
    if not closed_positions:
        return {"status": "NOT_EVALUABLE", "reasons": ["NO_CLOSED_POSITIONS"]}
    triggers = [_exit_trigger(row) for row in closed_positions]
    trigger_counts = {trigger: triggers.count(trigger) for trigger in sorted(set(triggers))}
    loss_trigger_count = sum(1 for row in closed_positions if (_number(row.get("realized_pnl")) or Decimal("0")) < 0)
    ambiguity_count = sum(
        1
        for row in closed_diagnostics
        if "SIDE_ENCODING_AMBIGUOUS_FOR_REALIZED_PNL" in (row.get("diagnostic_flags") or [])
    )
    reasons = []
    if realized_pnl < 0 and expected_value is not None and expected_value < 0:
        reasons.append("CLOSED_EXITS_LOCKED_NEGATIVE_REALIZED_SAMPLE")
    if loss_trigger_count > len(closed_positions) / 2:
        reasons.append("LOSS_EXITS_DOMINATE_REALIZED_SAMPLE")
    if ambiguity_count:
        reasons.append("SIDE_ENCODING_AMBIGUITY_PRESENT_IN_CLOSED_SAMPLE")
    status = "POSSIBLE_BAD_EXITS_OR_EXIT_ACCOUNTING_REVIEW_NEEDED" if reasons else "NO_EXIT_FAILURE_SIGNAL_IN_CLOSED_SAMPLE"
    return {
        "status": status,
        "closed_count": len(closed_positions),
        "negative_realized_exit_count": loss_trigger_count,
        "side_encoding_ambiguity_count": ambiguity_count,
        "exit_trigger_counts": trigger_counts,
        "reasons": reasons,
    }


def _too_short_holding_period_diagnostic(
    *, closed_holding_days: list[float], open_age_days: list[float]
) -> dict[str, Any]:
    if not closed_holding_days:
        return {"status": "NOT_EVALUABLE", "reasons": ["NO_CLOSED_POSITIONS"]}
    average_closed = sum(closed_holding_days) / len(closed_holding_days)
    median_closed = sorted(closed_holding_days)[len(closed_holding_days) // 2]
    average_open = sum(open_age_days) / len(open_age_days) if open_age_days else None
    reasons = []
    if median_closed < TREND_EQ_HYPOTHESIS_HORIZON_MIN_DAYS:
        reasons.append("MEDIAN_CLOSED_HOLDING_PERIOD_BELOW_20D_TREND_HORIZON")
    if average_closed < TREND_EQ_HYPOTHESIS_HORIZON_MIN_DAYS:
        reasons.append("AVERAGE_CLOSED_HOLDING_PERIOD_BELOW_20D_TREND_HORIZON")
    status = "TOO_SHORT_FOR_20_60D_TREND_REVIEW" if reasons else "HOLDING_PERIOD_NOT_OBVIOUSLY_TOO_SHORT"
    return {
        "status": status,
        "closed_average_holding_period_days": _decimal_text(Decimal(str(average_closed))),
        "closed_median_holding_period_days": _decimal_text(Decimal(str(median_closed))),
        "open_average_age_days": _decimal_text(Decimal(str(average_open))) if average_open is not None else None,
        "expected_trend_horizon_days": {
            "min": TREND_EQ_HYPOTHESIS_HORIZON_MIN_DAYS,
            "max": TREND_EQ_HYPOTHESIS_HORIZON_MAX_DAYS,
        },
        "reasons": reasons,
    }


def _winner_concentration_diagnostic(open_positions: list[Mapping[str, Any]]) -> dict[str, Any]:
    positive = sorted(
        [
            {
                "position_id": _position_id(row),
                "symbol": row.get("symbol"),
                "unrealized_pnl": _number(row.get("unrealized_pnl")) or Decimal("0"),
            }
            for row in open_positions
            if (_number(row.get("unrealized_pnl")) or Decimal("0")) > 0
        ],
        key=lambda row: row["unrealized_pnl"],
        reverse=True,
    )
    total_positive = _sum([row["unrealized_pnl"] for row in positive])
    if total_positive <= 0:
        return {"status": "NOT_EVALUABLE", "reasons": ["NO_POSITIVE_UNREALIZED_OPEN_POSITIONS"]}
    top_one_share = positive[0]["unrealized_pnl"] / total_positive if positive else Decimal("0")
    top_three_share = _sum([row["unrealized_pnl"] for row in positive[:3]]) / total_positive if positive else Decimal("0")
    reasons = []
    if top_one_share >= Decimal("0.30"):
        reasons.append("TOP_ONE_WINNER_EXCEEDS_30_PERCENT_OF_POSITIVE_UNREALIZED_PNL")
    if top_three_share >= Decimal("0.50"):
        reasons.append("TOP_THREE_WINNERS_EXCEED_50_PERCENT_OF_POSITIVE_UNREALIZED_PNL")
    status = "CONCENTRATED_IN_FEW_WINNERS" if reasons else "NOT_CONCENTRATED_IN_FEW_WINNERS"
    return {
        "status": status,
        "positive_unrealized_position_count": len(positive),
        "total_positive_unrealized_pnl": _decimal_text(total_positive),
        "top_one_positive_unrealized_share": _decimal_text(top_one_share),
        "top_three_positive_unrealized_share": _decimal_text(top_three_share),
        "top_positive_unrealized_positions": [
            {
                "position_id": row["position_id"],
                "symbol": row["symbol"],
                "unrealized_pnl": _decimal_text(row["unrealized_pnl"]),
            }
            for row in positive[:5]
        ],
        "reasons": reasons,
    }


def _mark_to_market_noise_diagnostic(
    *, closed_count: int, realized_pnl: Decimal, unrealized_pnl: Decimal, open_age_days: list[float]
) -> dict[str, Any]:
    total_abs = abs(realized_pnl) + abs(unrealized_pnl)
    unrealized_share = abs(unrealized_pnl) / total_abs if total_abs else Decimal("0")
    average_open_age = Decimal(str(sum(open_age_days) / len(open_age_days))) if open_age_days else None
    reasons = []
    if closed_count < SUFFICIENT_SAMPLE_THRESHOLD:
        reasons.append("REALIZED_SAMPLE_BELOW_SUFFICIENT_THRESHOLD")
    if unrealized_share >= Decimal("0.80"):
        reasons.append("NET_RESULT_DOMINATED_BY_OPEN_MARK_TO_MARKET")
    if unrealized_pnl > 0 and realized_pnl < 0:
        reasons.append("POSITIVE_UNREALIZED_OFFSETTING_NEGATIVE_REALIZED_SAMPLE")
    status = "MTM_DOMINATED_NOT_REALIZED_EVIDENCE" if reasons else "MTM_NOT_DOMINANT"
    return {
        "status": status,
        "realized_pnl": _decimal_text(realized_pnl),
        "unrealized_pnl": _decimal_text(unrealized_pnl),
        "absolute_unrealized_share_of_abs_pnl": _decimal_text(unrealized_share),
        "open_average_age_days": _decimal_text(average_open_age),
        "reasons": reasons,
    }


def _diagnostic_conclusion(
    diagnostics: Mapping[str, Any], realized_pnl: Decimal, unrealized_pnl: Decimal, closed_count: int
) -> list[str]:
    conclusions = []
    if closed_count < SUFFICIENT_SAMPLE_THRESHOLD:
        conclusions.append("REALIZED_SAMPLE_UNDERPOWERED")
    if realized_pnl < 0:
        conclusions.append("REALIZED_SAMPLE_NEGATIVE")
    if unrealized_pnl > 0 and realized_pnl < 0:
        conclusions.append("POSITIVE_UNREALIZED_PNL_OFFSETTING_NEGATIVE_REALIZED_PNL")
    for key, label in (
        ("bad_entries", "POSSIBLE_BAD_ENTRIES"),
        ("bad_exits", "POSSIBLE_BAD_EXITS_OR_ACCOUNTING_REVIEW"),
        ("too_short_holding_period", "POSSIBLE_TOO_SHORT_HOLDING_PERIOD"),
        ("concentration_in_few_winners", "POSSIBLE_WINNER_CONCENTRATION"),
        ("temporary_unrealized_mark_to_market_noise", "POSSIBLE_TEMPORARY_MARK_TO_MARKET_NOISE"),
    ):
        row = diagnostics.get(key, {}) if isinstance(diagnostics.get(key), Mapping) else {}
        status = str(row.get("status") or "")
        if status not in {"", "NOT_EVALUABLE", "NO_ENTRY_FAILURE_SIGNAL_IN_CLOSED_SAMPLE", "NO_EXIT_FAILURE_SIGNAL_IN_CLOSED_SAMPLE", "HOLDING_PERIOD_NOT_OBVIOUSLY_TOO_SHORT", "NOT_CONCENTRATED_IN_FEW_WINNERS", "MTM_NOT_DOMINANT"}:
            conclusions.append(label)
    return conclusions or ["NO_DOMINANT_DIAGNOSTIC_SIGNAL"]


def _row_for_sleeve(payload: Mapping[str, Any], sleeve_id: str, *keys: str) -> Mapping[str, Any]:
    for row in _safe_rows(payload, *keys):
        if str(row.get("sleeve_id") or "") == sleeve_id:
            return row
    return {}


def _rows_for_sleeve(payload: Mapping[str, Any], sleeve_id: str, *keys: str) -> list[Mapping[str, Any]]:
    return [row for row in _safe_rows(payload, *keys) if str(row.get("sleeve_id") or "") == sleeve_id]


def _safe_rows(payload: Any, *keys: str) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    rows: list[Mapping[str, Any]] = []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            rows.extend([row for row in value if isinstance(row, Mapping)])
    return rows


def _dedupe_positions(rows: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    seen = set()
    deduped = []
    for row in rows:
        key = _position_id(row)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _position_id(row: Mapping[str, Any]) -> str:
    return str(row.get("position_id") or row.get("paper_position_id") or row.get("candidate_id") or row.get("id") or "")


def _exit_trigger(row: Mapping[str, Any]) -> str:
    lineage = row.get("closure_lineage")
    source_receipt = row.get("exit_source_receipt")
    source_row = source_receipt.get("source_row") if isinstance(source_receipt, Mapping) else {}
    for value in (
        row.get("exit_trigger"),
        row.get("exit_recommendation"),
        lineage.get("exit_trigger") if isinstance(lineage, Mapping) else None,
        source_row.get("exit_recommendation") if isinstance(source_row, Mapping) else None,
    ):
        if str(value or ""):
            return str(value)
    return "UNKNOWN"


def _factory_classification(*rows: Mapping[str, Any]) -> str:
    for row in rows:
        value = str(row.get("factory_classification") or "").upper()
        if value:
            return value
    return "UNCLASSIFIED"


def _sample_status(closed_count: int) -> str:
    if closed_count == 0:
        return "ZERO_SAMPLE"
    if closed_count < 5:
        return "UNDERPOWERED"
    if closed_count < SUFFICIENT_SAMPLE_THRESHOLD:
        return "BUILDING_SAMPLE"
    return "SUFFICIENT_SAMPLE"


def _holding_days(start: Any, end: Any) -> float | None:
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    if not start_dt or not end_dt:
        return None
    return max(0.0, (end_dt - start_dt).total_seconds() / 86400)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    try:
        if len(text) == 10:
            return datetime.fromisoformat(text).replace(tzinfo=UTC)
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _expected_value(win_rate: Decimal | None, average_gain: Decimal | None, average_loss: Decimal | None) -> Decimal | None:
    if win_rate is None or average_gain is None or average_loss is None:
        return None
    return (win_rate * average_gain) + ((Decimal("1") - win_rate) * average_loss)


def _average(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return _sum(values) / Decimal(len(values))


def _sum(values: list[Decimal]) -> Decimal:
    total = Decimal("0")
    for value in values:
        total += value
    return total


def _sign(value: Decimal | None) -> int:
    if value is None or value == 0:
        return 0
    return 1 if value > 0 else -1


def _number(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.quantize(Decimal("0.000001")), "f")


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, Mapping) else {}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _atomic_write(path: Path, body: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(body, encoding="utf-8")
    tmp.replace(path)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
