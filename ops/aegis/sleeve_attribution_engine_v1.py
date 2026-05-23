from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import all_json_v1, now_utc_v1


def build_sleeve_attribution_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    reports = [payload for _path, payload in all_json_v1(truth_root, "sleeve_performance_report.v1.json")]
    receipts = [payload for _path, payload in all_json_v1(truth_root, "manual_trade_receipt.v1.json")]
    packets = [payload for _path, payload in all_json_v1(truth_root, "manual_trade_packet.v1.json")]
    rows = _sleeve_rows(reports=reports, receipts=receipts, packets=packets)
    return {
        "schema_id": "aegis_sleeve_attribution",
        "schema_version": "v1",
        "artifact_id": "aegis_sleeve_attribution_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "truth_root": str(Path(truth_root).resolve()),
        "sleeve_count": len(rows),
        "sleeves": rows,
        "input_counts": {
            "sleeve_performance_reports": len(reports),
            "manual_trade_receipts": len(receipts),
            "manual_trade_packets": len(packets),
        },
        "safety": {
            "advisory_only": True,
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
            "missing_performance_data_policy": "UNKNOWN_NOT_ZERO",
        },
    }


def sleeve_health_scores_v1(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_id": "aegis_sleeve_health_scores",
        "schema_version": "v1",
        "artifact_id": "sleeve_health_scores_v1",
        "day_utc": payload.get("day_utc"),
        "generated_at_utc": payload.get("generated_at_utc"),
        "sleeve_health_scores": [
            {
                "sleeve_id": row.get("sleeve_id"),
                "health_score": row.get("health_score"),
                "status": row.get("status"),
                "confidence_trend": row.get("confidence_trend"),
            }
            for row in payload.get("sleeves", [])
            if isinstance(row, dict)
        ],
    }


def render_sleeve_feedback_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS SLEEVE FEEDBACK v1",
        f"day_utc: {payload.get('day_utc')}",
        f"sleeve_count: {payload.get('sleeve_count')}",
    ]
    for row in payload.get("sleeves") or []:
        lines.append(
            f"- {row.get('sleeve_id')}: status={row.get('status')} health_score={row.get('health_score')} return={row.get('return')} hit_rate={row.get('hit_rate')} warning={row.get('stale_signal_warning')}"
        )
    lines.append("Safety: advisory only; no broker submit/transmit; missing performance data is UNKNOWN.")
    return "\n".join(lines) + "\n"


def _sleeve_rows(*, reports: list[dict[str, Any]], receipts: list[dict[str, Any]], packets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sleeve_ids = set()
    for report in reports:
        for row in _trade_rows(report):
            if row.get("sleeve_id"):
                sleeve_ids.add(str(row.get("sleeve_id")))
        for row in report.get("sleeves", []) if isinstance(report.get("sleeves"), list) else []:
            if isinstance(row, dict) and row.get("sleeve_id"):
                sleeve_ids.add(str(row.get("sleeve_id")))
    for receipt in receipts:
        if receipt.get("strategy_or_sleeve"):
            sleeve_ids.add(str(receipt.get("strategy_or_sleeve")))
    for packet in packets:
        for row in packet.get("candidates", []) if isinstance(packet.get("candidates"), list) else []:
            if isinstance(row, dict) and row.get("sleeve_id"):
                sleeve_ids.add(str(row.get("sleeve_id")))
    if not sleeve_ids:
        sleeve_ids.add("UNKNOWN")
    out = []
    for sleeve_id in sorted(sleeve_ids):
        trades = [row for report in reports for row in _trade_rows(report) if str(row.get("sleeve_id") or "UNKNOWN") == sleeve_id]
        sleeve_receipts = [row for row in receipts if str(row.get("strategy_or_sleeve") or "UNKNOWN") == sleeve_id]
        candidates = [
            row
            for packet in packets
            for row in (packet.get("candidates", []) if isinstance(packet.get("candidates"), list) else [])
            if isinstance(row, dict) and str(row.get("sleeve_id") or "UNKNOWN") == sleeve_id
        ]
        ret = _metric(trades, ("return", "return_pct", "realized_return_pct", "pnl_pct"))
        hit = _hit_rate(trades)
        drawdown = _metric(trades, ("drawdown", "max_drawdown", "MAE"))
        health = _health_score(ret, hit, drawdown, bool(trades))
        status = _status(health, bool(trades))
        out.append(
            {
                "sleeve_id": sleeve_id,
                "return": ret if ret is not None else "UNKNOWN",
                "hit_rate": hit if hit is not None else "UNKNOWN",
                "drawdown": drawdown if drawdown is not None else "UNKNOWN",
                "recommendation_count": len(candidates) if candidates else "UNKNOWN",
                "executed_captured_count": len(sleeve_receipts),
                "stale_signal_warning": "UNKNOWN" if not trades and not candidates else "NO",
                "regime_sensitivity": "UNKNOWN",
                "confidence_trend": "UNKNOWN" if not trades else "STABLE",
                "health_score": health if health is not None else "UNKNOWN",
                "status": status,
                "missing_performance_data": not bool(trades),
                "recommendation_is_advisory_only": True,
            }
        )
    return out


def _trade_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("trade_rows", "rows", "trades", "recommendations"):
        rows = report.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _metric(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> float | None:
    values = []
    for row in rows:
        for key in keys:
            if key in row:
                try:
                    values.append(float(row[key]))
                    break
                except (TypeError, ValueError):
                    pass
    if not values:
        return None
    return round(sum(values), 4)


def _hit_rate(rows: list[dict[str, Any]]) -> float | None:
    if not rows:
        return None
    wins = 0
    known = 0
    for row in rows:
        status = str(row.get("outcome_status") or row.get("result") or "").upper()
        if status in {"WIN", "LOSS", "GAIN", "PROFIT"}:
            known += 1
            wins += 1 if status in {"WIN", "GAIN", "PROFIT"} else 0
    return round(wins / known, 4) if known else None


def _health_score(ret: float | None, hit: float | None, drawdown: float | None, has_data: bool) -> int | None:
    if not has_data:
        return None
    score = 50
    if ret is not None:
        score += 15 if ret > 0 else -15 if ret < 0 else 0
    if hit is not None:
        score += int((hit - 0.5) * 40)
    if drawdown is not None and drawdown < 0:
        score -= min(20, int(abs(drawdown) * 100))
    return max(0, min(100, score))


def _status(score: int | None, has_data: bool) -> str:
    if not has_data or score is None:
        return "UNKNOWN"
    if score >= 70:
        return "HEALTHY"
    if score >= 50:
        return "WATCH"
    if score >= 30:
        return "DEGRADED"
    return "SUSPEND_CANDIDATE"
