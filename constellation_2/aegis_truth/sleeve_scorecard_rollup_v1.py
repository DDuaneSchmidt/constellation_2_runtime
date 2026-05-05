from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import json
import os
from typing import Any


@dataclass(frozen=True)
class SleeveScorecardRollupResultV1:
    daily_path: Path
    weekly_path: Path
    monthly_path: Path
    daily: dict[str, Any]
    weekly: dict[str, Any]
    monthly: dict[str, Any]


def materialize_sleeve_scorecard_rollup_v1(*, day_utc: str, truth_root: Path, evaluation_utc: str | None = None, scheduled_run: bool = False) -> SleeveScorecardRollupResultV1:
    day = _require_day(day_utc)
    truth_root = Path(truth_root).resolve()
    produced_at = _now(evaluation_utc)
    daily = _build_period_report(day=day, truth_root=truth_root, period_type="daily", days=[day], produced_at=produced_at, scheduled_run=scheduled_run)
    week_days = _week_days(day)
    week_id = _week_id(day)
    weekly = _build_period_report(day=day, truth_root=truth_root, period_type="weekly", days=week_days, produced_at=produced_at, period_id=week_id, scheduled_run=scheduled_run)
    month_days = _month_days(day)
    month_id = day[:7]
    monthly = _build_period_report(day=day, truth_root=truth_root, period_type="monthly", days=month_days, produced_at=produced_at, period_id=month_id, scheduled_run=scheduled_run)
    daily_path = truth_root / "reports" / "sleeve_scorecard_daily_v1" / day / "sleeve_scorecard_daily.v1.json"
    weekly_path = truth_root / "reports" / "sleeve_scorecard_weekly_v1" / week_id / "sleeve_scorecard_weekly.v1.json"
    monthly_path = truth_root / "reports" / "sleeve_scorecard_monthly_v1" / month_id / "sleeve_scorecard_monthly.v1.json"
    _write_json(daily_path, daily)
    _write_json(weekly_path, weekly)
    _write_json(monthly_path, monthly)
    return SleeveScorecardRollupResultV1(daily_path=daily_path, weekly_path=weekly_path, monthly_path=monthly_path, daily=daily, weekly=weekly, monthly=monthly)


def _build_period_report(*, day: str, truth_root: Path, period_type: str, days: list[str], produced_at: str, period_id: str | None = None, scheduled_run: bool = False) -> dict[str, Any]:
    rows: dict[str, list[dict[str, Any]]] = {}
    evidence: list[str] = []
    for item_day in days:
        path = truth_root / "reports" / "sleeve_effectiveness_v1" / item_day / "sleeve_effectiveness.v1.json"
        payload = _read_json_optional(path)
        if not payload:
            continue
        evidence.append(str(path))
        for row in payload.get("sleeves", []):
            if isinstance(row, dict):
                rows.setdefault(_text(row.get("sleeve_id")) or "UNKNOWN", []).append(row)
    sleeve_rows = [_rollup_row(sleeve_id, items) for sleeve_id, items in sorted(rows.items())]
    return {
        "schema_version": f"sleeve_scorecard_{period_type}.v1",
        "day_utc": day,
        "period_type": period_type.upper(),
        "period_id": period_id or day,
        "status": "PASS" if sleeve_rows else "SCORECARD_NOT_ENOUGH_EVIDENCE",
        "first_blocker": "" if sleeve_rows else "SCORECARD_NOT_ENOUGH_EVIDENCE",
        "produced_at_utc": produced_at,
        "producer": "sleeve_scorecard_rollup_v1",
        "scheduled_run": bool(scheduled_run),
        "sleeves": sleeve_rows,
        "not_enough_evidence": not bool(sleeve_rows),
        "evidence_paths": evidence,
    }


def _rollup_row(sleeve_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    trade_count = len(rows)
    wins = sum(1 for row in rows if _number(row.get("net_pnl")) > 0)
    losses = sum(1 for row in rows if _number(row.get("net_pnl")) < 0)
    net = round(sum(_number(row.get("net_pnl")) for row in rows), 6)
    slippage_values = [_number(row.get("fill_quality_score")) for row in rows if row.get("fill_quality_score") is not None]
    risk_scores = [_number(row.get("risk_realization_score")) for row in rows if row.get("risk_realization_score") is not None]
    return {
        "sleeve_id": sleeve_id,
        "trade_count": trade_count,
        "completed_trade_count": trade_count,
        "win_rate": round(wins / trade_count, 6) if trade_count else None,
        "expectancy": round(net / trade_count, 6) if trade_count else None,
        "net_pnl": net,
        "max_drawdown": min(0.0, net),
        "average_slippage": round(sum(slippage_values) / len(slippage_values), 6) if slippage_values else None,
        "risk_adjusted_return": round(net / max(1.0, sum(1.0 - score for score in risk_scores)), 6) if risk_scores else None,
        "effectiveness_score": _score(rows),
        "confidence_level": "LOW" if trade_count < 5 else "MEDIUM",
        "not_enough_evidence": trade_count < 5,
        "winning_trade_count": wins,
        "losing_trade_count": losses,
    }


def _score(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    effective = sum(1 for row in rows if row.get("effectiveness_status") == "EFFECTIVE")
    return round(effective / len(rows), 6)


def _week_days(day: str) -> list[str]:
    dt = date.fromisoformat(day)
    monday = dt - timedelta(days=dt.weekday())
    return [(monday + timedelta(days=i)).isoformat() for i in range(7) if (monday + timedelta(days=i)).isoformat() <= day]


def _week_id(day: str) -> str:
    dt = date.fromisoformat(day)
    iso = dt.isocalendar()
    return f"{iso.year}-{iso.week:02d}"


def _month_days(day: str) -> list[str]:
    dt = date.fromisoformat(day)
    start = dt.replace(day=1)
    days = []
    cur = start
    while cur <= dt:
        days.append(cur.isoformat())
        cur += timedelta(days=1)
    return days


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
