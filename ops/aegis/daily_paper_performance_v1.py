from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.exit_strategy_analysis_v1 import build_exit_strategy_analysis_v1, exit_strategy_analysis_path_v1
from ops.aegis.paper_pnl_report_v1 import build_paper_pnl_report_v1, paper_pnl_report_path_v1
from ops.aegis.sleeve_performance_truth_v1 import build_sleeve_performance_truth_v1, sleeve_performance_truth_path_v1

REPORT_FAMILY = "aegis_daily_paper_performance_v1"
REPORT_FILENAME = "daily_paper_performance.v1.json"

SAFETY = {
    "paper_only": True,
    "human_review_required": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "automatic_exit_allowed": False,
    "live_trading_allowed": False,
}


def daily_paper_performance_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_daily_paper_performance_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paper_pnl = _read_or_build(paper_pnl_report_path_v1(truth_root=root, day_utc=day), lambda: build_paper_pnl_report_v1(truth_root=root, day_utc=day))
    exit_strategy = _read_or_build(exit_strategy_analysis_path_v1(truth_root=root, day_utc=day), lambda: build_exit_strategy_analysis_v1(truth_root=root, day_utc=day))
    sleeve_truth = _read_or_build(sleeve_performance_truth_path_v1(truth_root=root, day_utc=day), lambda: build_sleeve_performance_truth_v1(truth_root=root, day_utc=day))

    open_positions = list(_rows(paper_pnl, "open_positions"))
    closed_positions = list(_rows(paper_pnl, "closed_positions"))
    exit_rows = list(_rows(exit_strategy, "analyses", "rows"))
    exit_by_position = {str(row.get("position_id") or ""): row for row in exit_rows}
    attention = _operator_attention(open_positions=open_positions, exit_by_position=exit_by_position)
    sleeve_comparison = _sleeve_comparison(paper_pnl=paper_pnl, sleeve_truth=sleeve_truth)
    best, worst = _best_worst_positions(open_positions=open_positions, closed_positions=closed_positions)
    source_paths = {
        "paper_pnl_report": str(paper_pnl_report_path_v1(truth_root=root, day_utc=day)),
        "exit_strategy_analysis": str(exit_strategy_analysis_path_v1(truth_root=root, day_utc=day)),
        "sleeve_performance_truth": str(sleeve_performance_truth_path_v1(truth_root=root, day_utc=day)),
    }
    data_quality = _data_quality(paper_pnl=paper_pnl, sleeve_truth=sleeve_truth, attention=attention)
    payload = {
        "schema_id": "aegis_daily_paper_performance",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": _now(),
        "operating_mode": "HUMAN_REVIEWED_PAPER_MODE",
        "total_open_positions": int(paper_pnl.get("open_position_count") or len(open_positions)),
        "total_closed_positions": int(paper_pnl.get("closed_position_count") or len(closed_positions)),
        "realized_pnl": str(paper_pnl.get("realized_pnl") or "0"),
        "unrealized_pnl": str(paper_pnl.get("unrealized_pnl") or "NOT_CANONICAL"),
        "total_paper_pnl": str(paper_pnl.get("total_paper_pnl") or "NOT_CANONICAL"),
        "full_portfolio_pnl_status": str(paper_pnl.get("full_portfolio_pnl_status") or "NOT_CANONICAL"),
        "certified_unrealized_pnl": str(paper_pnl.get("certified_unrealized_pnl") or "0"),
        "certified_total_paper_pnl": str(paper_pnl.get("certified_total_paper_pnl") or paper_pnl.get("realized_pnl") or "0"),
        "mark_coverage": paper_pnl.get("mark_coverage") if isinstance(paper_pnl.get("mark_coverage"), Mapping) else {},
        "data_quality_explanation": str(paper_pnl.get("data_quality_explanation") or ""),
        "exposure": str((sleeve_truth.get("totals") or {}).get("open_exposure") or _exposure_from_positions(open_positions)),
        "pnl_by_sleeve": list(_rows(paper_pnl, "pnl_by_sleeve")),
        "pnl_by_symbol": list(_rows(paper_pnl, "pnl_by_symbol")),
        "best_paper_positions": best,
        "worst_paper_positions": worst,
        "exit_recommendations_summary": _exit_summary(exit_rows),
        "positions_needing_operator_attention": attention,
        "operator_attention_count": len(attention),
        "sleeve_comparison": sleeve_comparison,
        "data_quality_status": data_quality,
        "source_artifact_paths": source_paths,
        "source_hashes": {name: _file_hash(Path(path)) for name, path in source_paths.items()},
        "rules": {
            "attention_flags_are_review_only": True,
            "no_automatic_exits": True,
            "no_broker_or_autonomous_execution": True,
        },
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_daily_paper_performance_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_daily_paper_performance_v1(truth_root=root, day_utc=day_utc))
    path = daily_paper_performance_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def _sleeve_comparison(*, paper_pnl: Mapping[str, Any], sleeve_truth: Mapping[str, Any]) -> list[dict[str, Any]]:
    pnl_by_sleeve = {str(row.get("sleeve_id") or "UNKNOWN"): row for row in _rows(paper_pnl, "pnl_by_sleeve")}
    rows = []
    for sleeve in _rows(sleeve_truth, "sleeves"):
        sleeve_id = str(sleeve.get("sleeve_id") or "UNKNOWN")
        pnl = pnl_by_sleeve.get(sleeve_id, {})
        rows.append({
            "sleeve_id": sleeve_id,
            "candidates_generated": int(sleeve.get("candidate_count") or 0),
            "paper_trades_opened": int(sleeve.get("paper_trade_count") or 0),
            "open_pnl": str(sleeve.get("unrealized_pnl") or pnl.get("unrealized_pnl") or "NOT_CANONICAL"),
            "realized_pnl": str(sleeve.get("realized_pnl") or pnl.get("realized_pnl") or "0"),
            "total_pnl": str((sleeve_truth.get("totals") or {}).get("total_paper_pnl") if len(_rows(sleeve_truth, "sleeves")) == 1 else pnl.get("total_paper_pnl") or "NOT_CANONICAL"),
            "win_count": int(sleeve.get("win_count") or 0),
            "loss_count": int(sleeve.get("loss_count") or 0),
            "average_hold_time": sleeve.get("average_hold_time"),
            "recommendation_followed_rate": sleeve.get("recommendation_followed_rate"),
            "data_quality_status": str(sleeve.get("data_quality_status") or "UNKNOWN"),
            "open_exposure": str(sleeve.get("open_exposure") or "0"),
            "certified_unrealized_pnl": str(sleeve.get("certified_unrealized_pnl") or pnl.get("certified_unrealized_pnl") or "0"),
            "mark_coverage_by_position_pct": sleeve.get("mark_coverage_by_position_pct", pnl.get("mark_coverage_by_position_pct")),
            "mark_coverage_by_entry_notional_pct": sleeve.get("mark_coverage_by_entry_notional_pct", pnl.get("mark_coverage_by_entry_notional_pct")),
            "missing_mark_position_count": int(sleeve.get("missing_mark_position_count") or pnl.get("missing_mark_position_count") or 0),
        })
    for sleeve_id, pnl in pnl_by_sleeve.items():
        if not any(row["sleeve_id"] == sleeve_id for row in rows):
            rows.append({
                "sleeve_id": sleeve_id,
                "candidates_generated": 0,
                "paper_trades_opened": int(pnl.get("open_position_count") or 0) + int(pnl.get("closed_position_count") or 0),
                "open_pnl": str(pnl.get("unrealized_pnl") or "NOT_CANONICAL"),
                "realized_pnl": str(pnl.get("realized_pnl") or "0"),
                "total_pnl": str(pnl.get("total_paper_pnl") or "NOT_CANONICAL"),
                "win_count": 0,
                "loss_count": 0,
                "average_hold_time": None,
                "recommendation_followed_rate": None,
                "data_quality_status": str(paper_pnl.get("data_quality_status") or "UNKNOWN"),
                "open_exposure": "0",
                "certified_unrealized_pnl": str(pnl.get("certified_unrealized_pnl") or "0"),
                "mark_coverage_by_position_pct": pnl.get("mark_coverage_by_position_pct"),
                "mark_coverage_by_entry_notional_pct": pnl.get("mark_coverage_by_entry_notional_pct"),
                "missing_mark_position_count": int(pnl.get("missing_mark_position_count") or 0),
            })
    return sorted(rows, key=lambda row: str(row.get("sleeve_id") or ""))


def _operator_attention(*, open_positions: list[dict[str, Any]], exit_by_position: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for pos in open_positions:
        flags = []
        position_id = str(pos.get("position_id") or "")
        exit_row = exit_by_position.get(position_id, {})
        if not exit_row:
            flags.append(_flag("MISSING_EXIT_RECOMMENDATION", "No current exit recommendation row exists for this open position."))
        stop = exit_row.get("stop_loss_distance") if isinstance(exit_row.get("stop_loss_distance"), Mapping) else {}
        target = exit_row.get("take_profit_distance") if isinstance(exit_row.get("take_profit_distance"), Mapping) else {}
        time_stop = exit_row.get("time_stop_status") if isinstance(exit_row.get("time_stop_status"), Mapping) else {}
        if _near(stop.get("distance_pct"), threshold=Decimal("0.02")):
            flags.append(_flag("STOP_LOSS_NEAR", "Return is within 2 percentage points of the stop-loss threshold."))
        if _near(target.get("distance_pct"), threshold=Decimal("0.02")):
            flags.append(_flag("TAKE_PROFIT_NEAR", "Return is within 2 percentage points of the take-profit threshold."))
        holding = _decimal(time_stop.get("holding_days"))
        max_hold = _decimal(time_stop.get("max_hold_days"))
        if holding is not None and max_hold is not None and max_hold - holding <= Decimal("2"):
            flags.append(_flag("TIME_STOP_NEAR", "Position is within 2 days of the configured time stop."))
        if str(pos.get("mark_freshness_status") or "").upper() != "CURRENT" or str(pos.get("mark_certification_status") or "").upper() != "CERTIFIED":
            flags.append(_flag("STALE_MARK_PRICE", "Mark is missing, stale, or not certified for current paper PnL."))
        unrealized = _decimal(pos.get("unrealized_pnl"))
        exposure = _position_exposure(pos)
        if unrealized is not None and (unrealized <= Decimal("-500") or (exposure and unrealized / exposure <= Decimal("-0.05"))):
            flags.append(_flag("LARGE_UNREALIZED_LOSS", "Open position has a large unrealized loss."))
        if exposure >= Decimal("25000"):
            flags.append(_flag("HIGH_EXPOSURE", "Open position exposure is at or above $25,000."))
        if flags:
            rows.append({
                "position_id": position_id,
                "candidate_id": str(pos.get("candidate_id") or ""),
                "symbol": str(pos.get("symbol") or ""),
                "sleeve_id": str(pos.get("sleeve_id") or exit_row.get("sleeve_id") or "UNKNOWN"),
                "unrealized_pnl": str(pos.get("unrealized_pnl") or ""),
                "exposure": _decimal_text(exposure),
                "current_exit_recommendation": str(exit_row.get("current_exit_recommendation") or "MISSING"),
                "attention_flags": flags,
                "operator_action_required": True,
                "automatic_exit_allowed": False,
            })
    return rows


def _best_worst_positions(*, open_positions: list[dict[str, Any]], closed_positions: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    for pos in open_positions:
        value = _decimal(pos.get("unrealized_pnl"))
        if value is not None:
            rows.append(_position_rank_row(pos, value, "UNREALIZED"))
    for pos in closed_positions:
        value = _decimal(pos.get("realized_pnl"))
        if value is not None:
            rows.append(_position_rank_row(pos, value, "REALIZED"))
    ordered = sorted(rows, key=lambda row: _decimal(row.get("pnl")) or Decimal("0"), reverse=True)
    return ordered[:5], list(reversed(ordered[-5:])) if ordered else []


def _position_rank_row(pos: Mapping[str, Any], value: Decimal, pnl_type: str) -> dict[str, Any]:
    return {
        "position_id": str(pos.get("position_id") or ""),
        "symbol": str(pos.get("symbol") or ""),
        "sleeve_id": str(pos.get("sleeve_id") or "UNKNOWN"),
        "pnl": _decimal_text(value),
        "pnl_type": pnl_type,
    }


def _exit_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in rows:
        rec = str(row.get("current_exit_recommendation") or "MISSING")
        counts[rec] = counts.get(rec, 0) + 1
    return {"recommendation_count": len(rows), "recommendation_counts": counts, "human_review_required": True, "automatic_exit_allowed": False}


def _data_quality(*, paper_pnl: Mapping[str, Any], sleeve_truth: Mapping[str, Any], attention: list[dict[str, Any]]) -> str:
    statuses = {str(paper_pnl.get("data_quality_status") or "UNKNOWN"), str(sleeve_truth.get("data_quality_status") or "UNKNOWN")}
    if any(flag.get("flag") == "STALE_MARK_PRICE" for row in attention for flag in row.get("attention_flags", [])):
        return "DEGRADED_STALE_MARKS"
    if any(status.startswith("BLOCKED") or status.startswith("MISSING") for status in statuses):
        return "BLOCKED_MISSING_AUTHORITY"
    if any("PARTIAL" in status or "NOT_CANONICAL" in status for status in statuses):
        return "PARTIAL"
    return "PASS"


def _read_or_build(path: Path, builder) -> dict[str, Any]:
    payload = _read_json(path)
    return payload if payload else builder()


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        rows = payload.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _flag(flag: str, reason: str) -> dict[str, str]:
    return {"flag": flag, "reason": reason}


def _near(value: Any, *, threshold: Decimal) -> bool:
    number = _decimal(value)
    return number is not None and number >= Decimal("0") and number <= threshold


def _exposure_from_positions(rows: list[dict[str, Any]]) -> str:
    return _decimal_text(sum((_position_exposure(row) for row in rows), Decimal("0")))


def _position_exposure(row: Mapping[str, Any]) -> Decimal:
    qty = _decimal(row.get("quantity")) or Decimal("0")
    entry = _decimal(row.get("entry_price")) or Decimal("0")
    return abs(qty * entry)


def _decimal(value: Any) -> Decimal | None:
    try:
        if value in (None, "") or isinstance(value, bool) or str(value) == "NOT_CANONICAL":
            return None
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal | Any) -> str:
    number = value if isinstance(value, Decimal) else _decimal(value) or Decimal("0")
    normalized = number.quantize(Decimal("0.000001"))
    text = format(normalized.normalize(), "f")
    return "0" if text in {"-0", "-0.0"} else text


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
