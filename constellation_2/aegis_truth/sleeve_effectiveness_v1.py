from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import os
from typing import Any


@dataclass(frozen=True)
class SleeveEffectivenessResultV1:
    report_path: Path
    report: dict[str, Any]


def materialize_sleeve_effectiveness_v1(*, day_utc: str, truth_root: Path, evaluation_utc: str | None = None, scheduled_run: bool = False) -> SleeveEffectivenessResultV1:
    day = _require_day(day_utc)
    truth_root = Path(truth_root).resolve()
    produced_at = _now(evaluation_utc)
    paths = {
        "trade": truth_root / "reports" / "sleeve_trade_fact_v1" / day / "sleeve_trade_fact.v1.json",
        "pnl": truth_root / "reports" / "sleeve_realized_pnl_v1" / day / "sleeve_realized_pnl.v1.json",
        "risk": truth_root / "reports" / "sleeve_risk_realization_v1" / day / "sleeve_risk_realization.v1.json",
        "fill": truth_root / "reports" / "sleeve_fill_quality_v1" / day / "sleeve_fill_quality.v1.json",
        "intent_arbitration": truth_root / "reports" / "intent_arbitration_v1" / day / "intent_arbitration.v1.json",
        "portfolio_scoring": truth_root / "reports" / "portfolio_scoring_v1" / day / "portfolio_scoring.v1.json",
        "regime_confidence": truth_root / "reports" / "regime_confidence_v1" / day / "regime_confidence.v1.json",
    }
    payloads = {key: _read_json_optional(path) or {} for key, path in paths.items()}
    blockers: list[dict[str, Any]] = []
    if str(payloads["trade"].get("status") or "").upper() == "NO_COMPLETED_TRADES":
        status = "INSUFFICIENT_EVIDENCE"
        blockers.append(_blocker("NO_COMPLETED_TRADES", "sleeve_trade_fact_v1.trade_fact_count", "> 0 completed broker-backed trade facts", 0, str(paths["trade"]), "Wait for broker-backed completed PAPER trades, then rerun sleeve economic truth measurement."))
        sleeves: list[dict[str, Any]] = []
    else:
        for key in ("trade", "pnl", "risk", "fill"):
            if str(payloads[key].get("status") or "").upper() not in {"PASS", "NO_COMPLETED_TRADES"}:
                blockers.append(_blocker(f"{key.upper()}_NOT_PASS", f"{key}.status", "PASS", payloads[key].get("status") or "MISSING", str(paths[key]), f"Run and clear {key} producer before sleeve effectiveness."))
        sleeves = _build_rows(payloads)
        status = "BLOCKED" if blockers else ("INSUFFICIENT_EVIDENCE" if not sleeves else "PASS")
    report = {
        "schema_version": "sleeve_effectiveness.v1",
        "day_utc": day,
        "status": status,
        "first_blocker": blockers[0]["blocker_code"] if blockers else "",
        "produced_at_utc": produced_at,
        "producer": "sleeve_effectiveness_v1",
        "scheduled_run": bool(scheduled_run),
        "sleeves": sleeves,
        "blockers": blockers,
        "evidence_paths": [str(path) for path in paths.values()],
    }
    out = truth_root / "reports" / "sleeve_effectiveness_v1" / day / "sleeve_effectiveness.v1.json"
    _write_json(out, report)
    return SleeveEffectivenessResultV1(report_path=out, report=report)


def _build_rows(payloads: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    pnl_by_sleeve = {_text(row.get("sleeve_id")): row for row in payloads["pnl"].get("sleeves", []) if isinstance(row, dict)}
    risk_by_sleeve = {_text(row.get("sleeve_id")): row for row in payloads["risk"].get("sleeves", []) if isinstance(row, dict)}
    fill_by_sleeve = {_text(row.get("sleeve_id")): row for row in payloads["fill"].get("sleeves", []) if isinstance(row, dict)}
    sleeves = sorted(set(pnl_by_sleeve) | set(risk_by_sleeve) | set(fill_by_sleeve))
    regime = payloads["regime_confidence"]
    regime_context = {
        "status": str(regime.get("status") or "UNKNOWN"),
        "confidence_level": str(regime.get("confidence_level") or regime.get("regime_confidence") or "UNKNOWN"),
    }
    rows: list[dict[str, Any]] = []
    for sleeve_id in sleeves:
        pnl = pnl_by_sleeve.get(sleeve_id, {})
        risk = risk_by_sleeve.get(sleeve_id, {})
        fill = fill_by_sleeve.get(sleeve_id, {})
        net_pnl = _number(pnl.get("net_pnl_after_fees"))
        trade_count = int(_number(pnl.get("trade_fact_count")))
        expected = _number(risk.get("expected_max_loss"))
        capital = _number(pnl.get("capital_used"))
        risk_score = _risk_score(risk)
        fill_score = _fill_score(fill)
        effective = net_pnl >= 0 and risk_score >= 0.5 and fill_score >= 0.0
        rows.append(
            {
                "sleeve_id": sleeve_id,
                "effectiveness_status": "INSUFFICIENT_EVIDENCE" if trade_count <= 0 else ("EFFECTIVE" if effective else "INEFFECTIVE"),
                "expectancy": round(net_pnl / trade_count, 6) if trade_count > 0 else None,
                "win_loss": "WIN" if net_pnl > 0 else ("LOSS" if net_pnl < 0 else "FLAT"),
                "net_pnl": net_pnl,
                "return_per_risk_unit": round(net_pnl / expected, 8) if expected > 0 else None,
                "return_per_capital_unit": round(net_pnl / capital, 8) if capital > 0 else None,
                "fill_quality_score": fill_score,
                "risk_realization_score": risk_score,
                "selected_intent_rank": _selected_rank(payloads["intent_arbitration"], sleeve_id),
                "selected_vs_rejected_note": "UNKNOWN_NO_REJECTED_INTENT_OUTCOME_EVIDENCE",
                "regime_context": regime_context,
                "confidence_level": _confidence(trade_count=trade_count, regime=regime_context),
            }
        )
    return rows


def _risk_score(row: dict[str, Any]) -> float:
    ratio = row.get("realized_vs_expected_risk_ratio")
    if ratio is None:
        return 0.0
    return max(0.0, round(1.0 - min(1.0, _number(ratio)), 6))


def _fill_score(row: dict[str, Any]) -> float:
    slippage = row.get("average_slippage")
    if slippage is None:
        return 0.0
    return 1.0 if _number(slippage) <= 0 else max(0.0, round(1.0 - min(1.0, abs(_number(slippage))), 6))


def _selected_rank(payload: dict[str, Any], sleeve_id: str) -> Any:
    for row in _walk(payload):
        if _text(row.get("sleeve_id") or row.get("engine_id")) == sleeve_id:
            return row.get("rank") or row.get("selected_intent_rank")
    return None


def _confidence(*, trade_count: int, regime: dict[str, Any]) -> str:
    if trade_count <= 0:
        return "NONE"
    if trade_count < 5:
        return "LOW"
    return "MEDIUM" if regime.get("confidence_level") == "UNKNOWN" else "HIGH"


def _walk(obj: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        rows.append(obj)
        for value in obj.values():
            rows.extend(_walk(value))
    elif isinstance(obj, list):
        for value in obj:
            rows.extend(_walk(value))
    return rows


def _blocker(code: str, field: str, expected: Any, actual: Any, path: str, action: str) -> dict[str, Any]:
    return {"blocker_code": code, "failed_field": field, "expected_value": expected, "actual_value": actual, "artifact_path": path, "operator_next_action": action}


def _read_json_optional(path: Path) -> dict[str, Any] | None:
    try:
        if path.exists() and path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
    except Exception:
        return None
    return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _number(value: Any) -> float:
    try:
        if value in (None, "") or isinstance(value, bool):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _text(value: Any) -> str:
    return str(value or "").strip()


def _require_day(value: str) -> str:
    text = _text(value)
    datetime.fromisoformat(text)
    if len(text) != 10:
        raise ValueError("day_utc must be YYYY-MM-DD")
    return text


def _now(value: str | None) -> str:
    return _text(value) if value else datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
