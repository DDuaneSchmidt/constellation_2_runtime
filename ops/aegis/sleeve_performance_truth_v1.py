from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.exit_recommendations_v1 import exit_recommendations_path_v1
from ops.aegis.paper_pnl_report_v1 import build_paper_pnl_report_v1, paper_pnl_report_path_v1
from ops.aegis.paper_position_ledger_v1 import paper_position_ledger_path_v1


REPORT_FAMILY = "aegis_sleeve_performance_truth_v1"
REPORT_FILENAME = "sleeve_performance_truth.v1.json"

SAFETY = {
    "review_only": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_policy_changed": False,
    "runtime_policy_changed": False,
}


def sleeve_performance_truth_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_sleeve_performance_truth_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    source_paths = _source_paths(root, day)
    payloads = {name: _read_json(path) for name, path in source_paths.items()}
    missing = [name for name, path in source_paths.items() if name in _required_authorities() and not path.exists()]

    ledger = payloads["paper_position_ledger"]
    paper_pnl = payloads["paper_pnl_report"] or build_paper_pnl_report_v1(truth_root=root, day_utc=day)
    recommendations = payloads["exit_recommendations"]
    realized = payloads["sleeve_realized_pnl"]
    scorecard = payloads["sleeve_scorecard_daily"]
    candidate_packet = payloads["candidate_review_packet"]
    candidate_ledger = payloads["candidate_review_ledger"]

    rec_by_candidate = {
        str(row.get("candidate_id") or ""): row
        for row in _safe_rows(recommendations, "recommendations", "rows")
        if str(row.get("candidate_id") or "")
    }
    certified_marks = _mark_source_is_certified(payloads["market_data"])
    sleeves: dict[str, dict[str, Any]] = {}

    legacy_rows = _safe_rows(ledger, "legacy_captures")
    pnl_open_by_id = {str(row.get("position_id") or ""): row for row in _safe_rows(paper_pnl, "open_positions")}
    for row in _safe_rows(ledger, "open_positions"):
        if _is_legacy(row):
            continue
        pnl_row = pnl_open_by_id.get(str(row.get("position_id") or ""), row)
        sleeve = str(pnl_row.get("sleeve_id") or "")
        if not sleeve or sleeve == "UNKNOWN":
            sleeve = _sleeve_id(row, rec_by_candidate)
        bucket = _bucket(sleeves, sleeve)
        bucket["open_paper_position_count"] += 1
        bucket["paper_trade_count"] += 1
        bucket["paper_position_ids"].append(str(row.get("position_id") or ""))
        bucket["open_exposure"] += _exposure(row)
        pnl_status = str(pnl_row.get("unrealized_pnl_status") or "").upper()
        pnl_value = _number(pnl_row.get("unrealized_pnl"))
        if pnl_status != "AVAILABLE" and certified_marks and _number(row.get("unrealized_pnl")) is not None:
            pnl_status = "AVAILABLE"
            pnl_value = _number(row.get("unrealized_pnl"))
        if pnl_status == "AVAILABLE" and pnl_value is not None:
            bucket["unrealized_pnl"] += pnl_value
            bucket["certified_unrealized_pnl"] += pnl_value
            bucket["marked_open_position_count"] += 1
            bucket["certified_entry_notional"] += _exposure(row)
            bucket["unrealized_pnl_status"] = "AVAILABLE"
        else:
            bucket["unrealized_pnl_status"] = "NOT_CANONICAL"
            bucket["missing_mark_position_count"] += 1
            symbol = str(row.get("symbol") or "")
            if symbol and symbol not in bucket["missing_mark_symbols"]:
                bucket["missing_mark_symbols"].append(symbol)

    for row in _safe_rows(ledger, "closed_positions"):
        if _is_legacy(row):
            continue
        sleeve = _sleeve_id(row, rec_by_candidate)
        bucket = _bucket(sleeves, sleeve)
        pnl = _number(row.get("realized_pnl")) or Decimal("0")
        bucket["closed_paper_position_count"] += 1
        bucket["paper_trade_count"] += 1
        bucket["realized_pnl"] += pnl
        bucket["paper_realized_pnl"] += pnl
        bucket["paper_position_ids"].append(str(row.get("position_id") or ""))
        if pnl > 0:
            bucket["win_count"] += 1
        elif pnl < 0:
            bucket["loss_count"] += 1
        hold = _hold_time_days(row)
        if hold is not None:
            bucket["_hold_times"].append(hold)
        reason = _exit_reason(row)
        bucket["exit_reason_counts"][reason] = bucket["exit_reason_counts"].get(reason, 0) + 1
        rec = rec_by_candidate.get(str(row.get("candidate_id") or ""))
        if rec:
            bucket["_recommendation_denominator"] += 1
            if _recommendation_followed(row, rec):
                bucket["recommendation_followed_count"] += 1

    for row in _safe_rows(realized, "sleeves"):
        sleeve = str(row.get("sleeve_id") or "UNKNOWN")
        bucket = _bucket(sleeves, sleeve)
        value = _number(row.get("realized_pnl"))
        if value is not None:
            bucket["realized_pnl"] = value
            bucket["realized_pnl_source"] = "sleeve_realized_pnl_v1"

    candidate_rows = _safe_rows(candidate_packet, "review_candidates", "rows") or _safe_rows(candidate_ledger, "candidates", "filtered_candidates", "rows")
    for row in candidate_rows:
        sleeve = str(row.get("sleeve_id") or "UNKNOWN") or "UNKNOWN"
        bucket = _bucket(sleeves, sleeve)
        bucket["candidate_count"] += 1
        decision = str(row.get("operator_decision") or row.get("decision") or row.get("status") or "").upper()
        if decision in {"APPROVED", "PAPER_APPROVED", "PAPER_TRADE", "PAPER_POSITION_OPEN", "EXECUTED", "ACCEPTED"}:
            bucket["_approved_candidates"] += 1

    scorecard_by_sleeve = _scorecard_index(scorecard)
    for sleeve, score in scorecard_by_sleeve.items():
        bucket = _bucket(sleeves, sleeve)
        bucket["scorecard_ref"] = {
            "source": "sleeve_scorecard_daily_v1",
            "status": str(scorecard.get("status") or ""),
            "rank": score.get("rank"),
            "score": score.get("score") or score.get("score_total") or score.get("health_score"),
        }

    rows = [_finalize_bucket(row, source_paths, missing, legacy_rows) for row in sleeves.values()]
    rows.sort(key=lambda row: str(row.get("sleeve_id") or ""))
    totals = _totals(rows, legacy_rows)
    source_artifact_paths = {name: str(path) for name, path in source_paths.items() if path.exists()}
    payload = {
        "schema_id": "aegis_sleeve_performance_truth",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": _now(),
        "truth_model": "CANONICAL_DERIVED_READ_MODEL_FROM_EXISTING_AUTHORITIES",
        "authority_partitioning": {
            "paper_positions": "aegis_paper_position_ledger_v1",
            "paper_position_events": "aegis_paper_position_events_v1",
            "exit_decisions": "exit_decision_v1",
            "exit_recommendations": "READ_MODEL_ONLY",
            "account_nav": "accounting_nav/cash_ledger_snapshot/nav_history_ledger",
            "realized_sleeve_pnl": "sleeve_trade_fact_v1 + sleeve_realized_pnl_v1",
            "ranking": "scorecard_rollups_only",
        },
        "sleeves": rows,
        "sleeve_count": len(rows),
        "totals": totals,
        "legacy_captures": {
            "excluded_from_canonical_performance": True,
            "legacy_capture_count": len(legacy_rows),
            "position_ids": [str(row.get("position_id") or "") for row in legacy_rows],
        },
        "source_artifact_paths": source_artifact_paths,
        "source_hashes": {name: _sha256(path) for name, path in source_paths.items() if path.exists()},
        "missing_authorities": missing,
        "data_quality_status": _quality(missing, rows),
        "rules": {
            "unrealized_pnl_canonical_only_when_mark_source_certified": True,
            "legacy_captures_excluded_from_canonical_paper_performance": True,
            "rankings_route_through_scorecard_rollups": True,
            "exit_recommendations_are_read_model_only": True,
        },
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_sleeve_performance_truth_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_sleeve_performance_truth_v1(truth_root=root, day_utc=day_utc))
    path = sleeve_performance_truth_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "paper_position_ledger": paper_position_ledger_path_v1(truth_root=root, day_utc=day),
        "paper_position_events": root / "reports" / "aegis_paper_position_events_v1" / day / "paper_position_events.v1.jsonl",
        "exit_recommendations": exit_recommendations_path_v1(truth_root=root, day_utc=day),
        "sleeve_trade_fact": root / "reports" / "sleeve_trade_fact_v1" / day / "sleeve_trade_fact.v1.json",
        "sleeve_realized_pnl": root / "reports" / "sleeve_realized_pnl_v1" / day / "sleeve_realized_pnl.v1.json",
        "sleeve_scorecard_daily": root / "reports" / "sleeve_scorecard_daily_v1" / day / "sleeve_scorecard_daily.v1.json",
        "market_data": root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json",
        "paper_pnl_report": paper_pnl_report_path_v1(truth_root=root, day_utc=day),
        "candidate_review_packet": root / "reports" / "aegis_candidate_review_packet_v1" / day / "candidate_review_packet.v1.json",
        "candidate_review_ledger": root / "reports" / "aegis_candidate_review_ledger_v1" / day / "candidate_review_ledger.v1.json",
        "accounting_nav": root / "accounting_v2" / "nav" / day / "nav.v2.json",
        "cash_ledger_snapshot": root / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json",
    }


def _required_authorities() -> set[str]:
    return {"paper_position_ledger", "paper_position_events"}


def _bucket(sleeves: dict[str, dict[str, Any]], sleeve_id: str) -> dict[str, Any]:
    sleeve = sleeve_id or "UNKNOWN"
    return sleeves.setdefault(
        sleeve,
        {
            "sleeve_id": sleeve,
            "open_paper_position_count": 0,
            "closed_paper_position_count": 0,
            "realized_pnl": Decimal("0"),
            "paper_realized_pnl": Decimal("0"),
            "realized_pnl_source": "paper_position_ledger_v1",
            "unrealized_pnl": Decimal("0"),
            "unrealized_pnl_status": "NOT_CANONICAL",
            "certified_unrealized_pnl": Decimal("0"),
            "marked_open_position_count": 0,
            "missing_mark_position_count": 0,
            "certified_entry_notional": Decimal("0"),
            "missing_mark_symbols": [],
            "open_exposure": Decimal("0"),
            "win_count": 0,
            "loss_count": 0,
            "win_rate": None,
            "average_hold_time": None,
            "exit_reason_counts": {},
            "recommendation_followed_count": 0,
            "recommendation_followed_rate": None,
            "candidate_count": 0,
            "paper_trade_count": 0,
            "approval_rate": None,
            "source_artifact_paths": [],
            "source_hashes": {},
            "data_quality_status": "PASS",
            "missing_authorities": [],
            "paper_position_ids": [],
            "scorecard_ref": {},
            "_hold_times": [],
            "_recommendation_denominator": 0,
            "_approved_candidates": 0,
        },
    )


def _finalize_bucket(row: dict[str, Any], source_paths: dict[str, Path], missing: list[str], legacy_rows: list[dict[str, Any]]) -> dict[str, Any]:
    closed = int(row["closed_paper_position_count"])
    outcomes = int(row["win_count"]) + int(row["loss_count"])
    denominator = int(row.pop("_recommendation_denominator", 0))
    approved = int(row.pop("_approved_candidates", 0))
    holds = list(row.pop("_hold_times", []))
    if outcomes:
        row["win_rate"] = round(row["win_count"] / outcomes, 6)
    if holds:
        row["average_hold_time"] = round(sum(holds) / len(holds), 6)
    if denominator:
        row["recommendation_followed_rate"] = round(row["recommendation_followed_count"] / denominator, 6)
    if row["candidate_count"]:
        row["approval_rate"] = round(approved / row["candidate_count"], 6)
    exposure_value = row.get("open_exposure") or Decimal("0")
    certified_notional = row.get("certified_entry_notional") or Decimal("0")
    row["realized_pnl"] = _decimal_text(row["realized_pnl"])
    row["paper_realized_pnl"] = _decimal_text(row["paper_realized_pnl"])
    row["open_exposure"] = _decimal_text(exposure_value)
    row["certified_unrealized_pnl"] = _decimal_text(row.get("certified_unrealized_pnl") or Decimal("0"))
    row["certified_entry_notional"] = _decimal_text(certified_notional)
    open_count = int(row.get("open_paper_position_count") or 0)
    marked_count = int(row.get("marked_open_position_count") or 0)
    row["mark_coverage_by_position_pct"] = round((marked_count / open_count) * 100, 4) if open_count else 100.0
    row["mark_coverage_by_entry_notional_pct"] = round((float(certified_notional) / float(exposure_value)) * 100, 4) if exposure_value else 100.0
    row["missing_mark_symbols"] = sorted(row.get("missing_mark_symbols") or [])
    row["unrealized_pnl"] = _decimal_text(row["unrealized_pnl"]) if row["unrealized_pnl_status"] == "AVAILABLE" else ""
    row["source_artifact_paths"] = [str(path) for key, path in source_paths.items() if path.exists() and key in _sleeve_source_keys()]
    row["source_hashes"] = {str(path): _sha256(path) for key, path in source_paths.items() if path.exists() and key in _sleeve_source_keys()}
    row["missing_authorities"] = list(missing)
    row["legacy_capture_count_excluded"] = len(legacy_rows)
    row["data_quality_status"] = "BLOCKED" if missing else "PARTIAL" if row["unrealized_pnl_status"] == "NOT_CANONICAL" else "PASS"
    return row


def _sleeve_source_keys() -> set[str]:
    return {"paper_position_ledger", "paper_position_events", "exit_recommendations", "paper_pnl_report", "sleeve_trade_fact", "sleeve_realized_pnl", "sleeve_scorecard_daily"}


def _totals(rows: list[dict[str, Any]], legacy_rows: list[dict[str, Any]]) -> dict[str, Any]:
    realized = sum((_number(row.get("realized_pnl")) or Decimal("0")) for row in rows)
    unrealized_available = all(str(row.get("unrealized_pnl_status") or "").upper() == "AVAILABLE" for row in rows) if rows else False
    if not rows:
        realized = Decimal("0")
    unrealized = sum((_number(row.get("unrealized_pnl")) or Decimal("0")) for row in rows) if unrealized_available else Decimal("0")
    certified_unrealized = sum((_number(row.get("certified_unrealized_pnl")) or Decimal("0")) for row in rows)
    open_count = sum(int(row.get("open_paper_position_count") or 0) for row in rows)
    marked_count = sum(int(row.get("marked_open_position_count") or 0) for row in rows)
    exposure = sum((_number(row.get("open_exposure")) or Decimal("0")) for row in rows)
    certified_notional = sum((_number(row.get("certified_entry_notional")) or Decimal("0")) for row in rows)
    wins = sum(int(row.get("win_count") or 0) for row in rows)
    losses = sum(int(row.get("loss_count") or 0) for row in rows)
    return {
        "open_paper_position_count": open_count,
        "closed_paper_position_count": sum(int(row.get("closed_paper_position_count") or 0) for row in rows),
        "realized_pnl": _decimal_text(realized),
        "unrealized_pnl": _decimal_text(unrealized) if unrealized_available else "",
        "unrealized_pnl_status": "AVAILABLE" if unrealized_available else "NOT_CANONICAL",
        "total_paper_pnl": _decimal_text(realized + unrealized) if unrealized_available else "NOT_CANONICAL",
        "certified_unrealized_pnl": _decimal_text(certified_unrealized),
        "certified_total_paper_pnl": _decimal_text(realized + certified_unrealized),
        "marked_open_position_count": marked_count,
        "missing_mark_position_count": max(open_count - marked_count, 0),
        "mark_coverage_by_position_pct": round((marked_count / open_count) * 100, 4) if open_count else 100.0,
        "mark_coverage_by_entry_notional_pct": round((float(certified_notional) / float(exposure)) * 100, 4) if exposure else 100.0,
        "open_exposure": _decimal_text(exposure),
        "win_count": wins,
        "loss_count": losses,
        "win_rate": round(wins / (wins + losses), 6) if (wins + losses) else None,
        "candidate_count": sum(int(row.get("candidate_count") or 0) for row in rows),
        "paper_trade_count": sum(int(row.get("paper_trade_count") or 0) for row in rows),
        "legacy_capture_count_excluded": len(legacy_rows),
    }


def _quality(missing: list[str], rows: list[dict[str, Any]]) -> str:
    if missing:
        return "BLOCKED_MISSING_AUTHORITY"
    if any(row.get("unrealized_pnl_status") == "NOT_CANONICAL" for row in rows):
        return "PARTIAL_UNREALIZED_NOT_CANONICAL"
    return "PASS"


def _sleeve_id(row: Mapping[str, Any], rec_by_candidate: Mapping[str, Mapping[str, Any]]) -> str:
    lineage = row.get("candidate_lineage") if isinstance(row.get("candidate_lineage"), Mapping) else {}
    receipt = row.get("source_receipt") if isinstance(row.get("source_receipt"), Mapping) else {}
    rec = rec_by_candidate.get(str(row.get("candidate_id") or ""), {})
    context = rec.get("candidate_context") if isinstance(rec.get("candidate_context"), Mapping) else {}
    return str(row.get("sleeve_id") or lineage.get("sleeve_id") or receipt.get("sleeve_id") or rec.get("sleeve_id") or context.get("sleeve_id") or "UNKNOWN") or "UNKNOWN"


def _is_legacy(row: Mapping[str, Any]) -> bool:
    if str(row.get("legacy_classification") or "").upper() == "LEGACY_CAPTURE":
        return True
    receipt = row.get("source_receipt") if isinstance(row.get("source_receipt"), Mapping) else {}
    return str(receipt.get("receipt_type") or "").upper() in {"LEGACY", "LEGACY_CAPTURE"}


def _exit_reason(row: Mapping[str, Any]) -> str:
    receipt = row.get("exit_source_receipt") if isinstance(row.get("exit_source_receipt"), Mapping) else {}
    for key in ("exit_reason_selected_by_operator", "exit_reason", "reason", "action"):
        value = str(receipt.get(key) or row.get(key) or "").strip().upper()
        if value:
            return value
    return "CLOSED_NO_REASON_RECORDED"


def _recommendation_followed(row: Mapping[str, Any], rec: Mapping[str, Any]) -> bool:
    reason = _exit_reason(row)
    recommendation = str(rec.get("exit_recommendation") or "").strip().upper()
    if not recommendation:
        return False
    return recommendation == reason or recommendation in reason or reason in recommendation


def _exposure(row: Mapping[str, Any]) -> Decimal:
    notional = _number(row.get("notional"))
    if notional is not None:
        return abs(notional)
    qty = _number(row.get("quantity")) or Decimal("0")
    entry = _number(row.get("entry_price")) or Decimal("0")
    return abs(qty * entry)


def _hold_time_days(row: Mapping[str, Any]) -> float | None:
    entry = _parse_dt(row.get("entry_time"))
    exit_time = _parse_dt(row.get("exit_time"))
    if entry is None or exit_time is None or exit_time < entry:
        return None
    return (exit_time - entry).total_seconds() / 86400.0


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _mark_source_is_certified(payload: Mapping[str, Any]) -> bool:
    status = str(payload.get("status") or "").upper()
    validation = str(payload.get("validation_status") or "").upper()
    if validation in {"CERTIFIED", "VALIDATED_FINAL_EOD", "FINAL_EOD_CERTIFIED"}:
        return True
    if status in {"CERTIFIED", "FINAL_EOD_CERTIFIED"}:
        return True
    rows = payload.get("normalized_records") if isinstance(payload.get("normalized_records"), list) else []
    if rows:
        return all(str(row.get("finalization_status") or row.get("data_finality") or "").upper() in {"FINAL", "FINAL_EOD", "FINAL_EOD_CERTIFIED", "CERTIFIED"} for row in rows if isinstance(row, Mapping))
    return False


def _scorecard_index(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out = {}
    for key in ("sleeves", "scorecards", "rows"):
        rows = payload.get(key)
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and row.get("sleeve_id"):
                    out[str(row["sleeve_id"])] = row
    return out


def _safe_rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
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


def _number(value: Any) -> Decimal | None:
    try:
        if value in (None, "") or isinstance(value, bool):
            return None
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal | int | float | str) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value or "0"))
    normalized = value.quantize(Decimal("0.000001"))
    return format(normalized.normalize(), "f")


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
