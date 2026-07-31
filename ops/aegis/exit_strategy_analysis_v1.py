from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.exit_recommendations_v1 import build_exit_recommendations_v1, exit_recommendations_path_v1
from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, paper_position_ledger_path_v1

REPORT_FAMILY = "aegis_exit_strategy_analysis_v1"
REPORT_FILENAME = "exit_strategy_analysis.v1.json"

SAFETY = {
    "paper_only": True,
    "human_review_required": True,
    "operator_action_required": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "automatic_exit_allowed": False,
    "live_trading_allowed": False,
}


def exit_strategy_analysis_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_exit_strategy_analysis_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    ledger_path = paper_position_ledger_path_v1(truth_root=root, day_utc=day)
    ledger = _read_json(ledger_path) or build_paper_position_ledger_v1(truth_root=root, day_utc=day)
    rec_path_existing = exit_recommendations_path_v1(truth_root=root, day_utc=day)
    recommendations = _read_json(rec_path_existing) or build_exit_recommendations_v1(truth_root=root, day_utc=day)
    rec_by_position = {str(row.get("position_id") or ""): row for row in recommendations.get("recommendations") or [] if isinstance(row, dict)}
    rows = []
    for position in ledger.get("open_positions") or []:
        if isinstance(position, dict):
            rows.append(_analysis_row(position, rec_by_position.get(str(position.get("position_id") or ""), {}), day))
    rec_path = exit_recommendations_path_v1(truth_root=root, day_utc=day)
    payload = {
        "schema_id": "aegis_exit_strategy_analysis",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": _now(),
        "operating_mode": "HUMAN_REVIEWED_PAPER_MODE",
        "open_position_count": len(rows),
        "analyses": rows,
        "rows": rows,
        "source_artifact_paths": {"paper_position_ledger": str(ledger_path), "exit_recommendations": str(rec_path)},
        "source_hashes": {str(path): _file_hash(path) for path in (ledger_path, rec_path) if path.exists()},
        "what_would_make_me_exit_explanation": "Exit recommendations are human-reviewed. Stops, targets, trailing stops, time stops, signal invalidation, and regime invalidation can create an exit recommendation, but no automatic exit or broker submission is enabled.",
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_exit_strategy_analysis_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_exit_strategy_analysis_v1(truth_root=root, day_utc=day_utc))
    path = exit_strategy_analysis_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def _analysis_row(position: Mapping[str, Any], rec: Mapping[str, Any], day: str) -> dict[str, Any]:
    policy = rec.get("policy") if isinstance(rec.get("policy"), Mapping) else {}
    entry = _decimal(position.get("entry_price"))
    mark = _decimal(position.get("current_certified_mark") or position.get("mark_price"))
    side = str(position.get("side") or "BUY").upper()
    sign = Decimal("-1") if side in {"SELL", "SHORT"} else Decimal("1")
    return_pct = ((mark - entry) / entry * sign) if entry and mark and entry != 0 else None
    stop_loss_pct = abs(_decimal(policy.get("stop_loss_pct")) or Decimal("0"))
    take_profit_pct = abs(_decimal(policy.get("take_profit_pct")) or Decimal("0"))
    trailing_stop_pct = abs(_decimal(policy.get("trailing_stop_pct")) or Decimal("0"))
    holding_days = ((rec.get("holding_period") or {}).get("days") if isinstance(rec.get("holding_period"), Mapping) else None)
    max_hold_days = policy.get("max_hold_days") or policy.get("max_holding_days")
    stop_distance = _distance(return_pct, -stop_loss_pct) if stop_loss_pct else None
    take_profit_distance = _distance(take_profit_pct, return_pct) if take_profit_pct else None
    recommendation = str(rec.get("exit_recommendation") or "HOLD")
    triggers = []
    if stop_loss_pct:
        triggers.append(f"return <= -{_pct(stop_loss_pct)} stop-loss threshold")
    if take_profit_pct:
        triggers.append(f"return >= {_pct(take_profit_pct)} take-profit threshold")
    if trailing_stop_pct:
        triggers.append(f"trailing drawdown >= {_pct(trailing_stop_pct)} after trailing policy activates")
    if max_hold_days not in (None, ""):
        triggers.append(f"holding period reaches {max_hold_days} days")
    triggers.extend(["signal invalidation is recorded", "regime invalidation is recorded", "operator manually records an exit after review"])
    reason_codes = [str(item) for item in rec.get("reason_codes") or []]
    return {
        "position_id": str(position.get("position_id") or ""),
        "candidate_id": str(position.get("candidate_id") or ""),
        "symbol": str(position.get("symbol") or "").upper(),
        "sleeve_id": str(rec.get("sleeve_id") or "UNKNOWN"),
        "current_exit_recommendation": recommendation,
        "current_mark": str(position.get("current_certified_mark") or position.get("mark_price") or ""),
        "return_pct": round(float(return_pct), 6) if return_pct is not None else None,
        "stop_loss_distance": _distance_payload(stop_distance, stop_loss_pct, return_pct, "STOP_LOSS"),
        "take_profit_distance": _distance_payload(take_profit_distance, take_profit_pct, return_pct, "TAKE_PROFIT"),
        "trailing_stop_status": {"policy": str(policy.get("trailing_stop_policy") or policy.get("trailing_stop_rule") or "NOT_CONFIGURED"), "threshold_pct": float(trailing_stop_pct) if trailing_stop_pct else None, "triggered": "TRAILING_STOP_THRESHOLD_REACHED" in reason_codes},
        "time_stop_status": {"holding_days": holding_days, "max_hold_days": max_hold_days, "triggered": "MAX_HOLD_PERIOD_REACHED" in reason_codes},
        "signal_invalidation_status": {"invalidated": "SIGNAL_INVALIDATED" in reason_codes or recommendation == "EXIT_SIGNAL_INVALIDATED"},
        "regime_invalidation_status": {"invalidated": "REGIME_INVALIDATED" in reason_codes or recommendation == "EXIT_REGIME_INVALIDATED"},
        "what_would_cause_exit_next": triggers,
        "operator_action_required": True,
        "automatic_exit_allowed": False,
        **SAFETY,
    }



def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}

def _distance(value_a: Decimal | None, value_b: Decimal | None) -> Decimal | None:
    if value_a is None or value_b is None:
        return None
    return value_a - value_b


def _distance_payload(distance: Decimal | None, threshold: Decimal, current: Decimal | None, label: str) -> dict[str, Any]:
    return {"rule": label, "threshold_pct": float(threshold) if threshold else None, "current_return_pct": float(current) if current is not None else None, "distance_pct": float(distance) if distance is not None else None, "triggered": bool(distance is not None and distance <= 0)}


def _pct(value: Decimal) -> str:
    return f"{float(value) * 100:.2f}%"


def _decimal(value: Any) -> Decimal | None:
    try:
        if value in (None, "") or isinstance(value, bool):
            return None
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
