from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
EVENT_MARKET_SNAPSHOT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/event_market_snapshot.v1.schema.json"
RULESET_VERSION = "aegis_market_context_rules.v1"
SNAPSHOT_STALE_AFTER_MINUTES = 30

VOLATILITY_LABELS = {"LOW_VOL", "NORMAL_VOL", "HIGH_VOL", "PANIC_VOL", "UNKNOWN"}
BREADTH_LABELS = {"STRONG_BREADTH", "MIXED_BREADTH", "WEAK_BREADTH", "BREADTH_COLLAPSE", "UNKNOWN"}
MACRO_RISK_LEVELS = {"NONE", "LOW", "MEDIUM", "HIGH", "EXTREME", "UNKNOWN"}
REGIME_LABELS = {
    "TRENDING_UP",
    "TRENDING_DOWN",
    "RANGE_BOUND",
    "HIGH_VOLATILITY",
    "PANIC",
    "RECOVERY",
    "DEFENSIVE_ROTATION",
    "UNKNOWN",
}


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def event_market_snapshot_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "event_market_snapshot_v1" / day_utc / "event_market_snapshot.v1.json"


def validate_event_market_snapshot_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, EVENT_MARKET_SNAPSHOT_SCHEMA)


def write_event_market_snapshot_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_event_market_snapshot_v1(payload)
    path = event_market_snapshot_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def classify_volatility_v1(*, vix_level: Any = "", vix_change_pct: Any = "", spy_realized_vol: Any = "", volatility_expansion_ratio: Any = "") -> str:
    vix = _number(vix_level)
    vix_change = _number(vix_change_pct)
    realized = _number(spy_realized_vol)
    expansion = _number(volatility_expansion_ratio)
    if all(value is None for value in (vix, vix_change, realized, expansion)):
        return "UNKNOWN"
    if _gte(vix, 35) or _gte(vix_change, 25) or _gte(realized, 35) or _gte(expansion, 2.0):
        return "PANIC_VOL"
    if _gte(vix, 24) or _gte(vix_change, 10) or _gte(realized, 24) or _gte(expansion, 1.35):
        return "HIGH_VOL"
    if _lt(vix, 14) and (realized is None or realized <= 12) and (expansion is None or expansion < 0.9):
        return "LOW_VOL"
    return "NORMAL_VOL"


def classify_breadth_v1(*, advancing_issues: Any = "", declining_issues: Any = "", advance_decline_delta: Any = "", breadth_down_pct: Any = "", participation_pct: Any = "") -> str:
    advancing = _number(advancing_issues)
    declining = _number(declining_issues)
    delta = _number(advance_decline_delta)
    down_pct = _number(breadth_down_pct)
    participation = _number(participation_pct)
    total = (advancing or 0) + (declining or 0)
    if down_pct is None and total > 0:
        down_pct = (declining or 0) / total * 100
    if delta is None and (advancing is not None or declining is not None):
        delta = (advancing or 0) - (declining or 0)
    if down_pct is None and delta is None and participation is None:
        return "UNKNOWN"
    if _gte(down_pct, 75) or _lte(delta, -1000):
        return "BREADTH_COLLAPSE"
    if _gte(down_pct, 60) or _lt(participation, 45):
        return "WEAK_BREADTH"
    if _lte(down_pct, 42) and _gte(delta, 500):
        return "STRONG_BREADTH"
    return "MIXED_BREADTH"


def classify_macro_event_v1(*, day_utc: str, macro_calendar: Any) -> dict[str, Any]:
    rows = _macro_rows(macro_calendar)
    today = [row for row in rows if str(row.get("date") or row.get("day_utc") or "") == day_utc]
    normalized = [_normalize_macro_event(row) for row in today]
    risk = _max_risk(normalized)
    event_types = sorted({row["event_type"] for row in normalized if row["event_type"]})
    return {
        "macro_event_today": bool(normalized),
        "macro_event_type": ",".join(event_types) if event_types else "NONE",
        "macro_event_risk_level": risk,
        "macro_events_today": sorted(normalized, key=lambda row: (row["event_time_utc"], row["event_type"], row["description"])),
    }


def classify_regime_v1(*, volatility_label: str, breadth_label: str, trend_metrics: dict[str, Any], macro_context: dict[str, Any]) -> str:
    vol = str(volatility_label or "UNKNOWN").upper()
    breadth = str(breadth_label or "UNKNOWN").upper()
    spy_return = _number(trend_metrics.get("spy_20d_return_pct"))
    index_return = _number(trend_metrics.get("spy_return_pct"))
    recent_selloff = _number(trend_metrics.get("recent_selloff_pct"))
    rebound_breadth = _number(trend_metrics.get("rebound_breadth_pct"))
    defensive = _number(trend_metrics.get("defensive_rotation_score"))
    cyclical = _number(trend_metrics.get("cyclical_relative_strength"))
    above_50 = _boolish(trend_metrics.get("spy_above_50dma"))

    if vol == "PANIC_VOL" or (_lte(index_return, -3.0) and breadth == "BREADTH_COLLAPSE"):
        return "PANIC"
    if _gte(defensive, 0.7) and _lte(cyclical, -0.3):
        return "DEFENSIVE_ROTATION"
    if vol == "HIGH_VOL":
        return "HIGH_VOLATILITY"
    if _lte(recent_selloff, -2.0) and _gte(rebound_breadth, 45) and _gte(index_return, 0):
        return "RECOVERY"
    if _gte(spy_return, 2.0) and above_50 is True and breadth in {"STRONG_BREADTH", "MIXED_BREADTH"} and vol in {"LOW_VOL", "NORMAL_VOL"}:
        return "TRENDING_UP"
    if _lte(spy_return, -2.0) and (above_50 is False or breadth in {"WEAK_BREADTH", "BREADTH_COLLAPSE"}):
        return "TRENDING_DOWN"
    if spy_return is not None and abs(spy_return) < 2.0 and vol in {"LOW_VOL", "NORMAL_VOL"}:
        return "RANGE_BOUND"
    return "UNKNOWN"


def build_event_market_snapshot_v1(
    *,
    day_utc: str,
    generated_at_utc: str,
    market_data: dict[str, Any] | None = None,
    macro_calendar: Any = None,
    source_lineage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    data = market_data if isinstance(market_data, dict) else {}
    spy = _instrument("SPY", data)
    qqq = _instrument("QQQ", data)
    vix = _instrument("VIX", data)
    breadth = _breadth_metrics(data)
    volatility = _volatility_metrics(data, vix=vix)
    trend = _trend_metrics(data, spy=spy, qqq=qqq)
    macro = classify_macro_event_v1(day_utc=day_utc, macro_calendar=macro_calendar if macro_calendar is not None else data.get("macro_events"))
    volatility_label = classify_volatility_v1(
        vix_level=volatility.get("vix_level"),
        vix_change_pct=volatility.get("vix_change_pct"),
        spy_realized_vol=volatility.get("spy_realized_vol"),
        volatility_expansion_ratio=volatility.get("volatility_expansion_ratio"),
    )
    breadth_label = classify_breadth_v1(
        advancing_issues=breadth.get("advancing_issues"),
        declining_issues=breadth.get("declining_issues"),
        advance_decline_delta=breadth.get("advance_decline_delta"),
        breadth_down_pct=breadth.get("breadth_down_pct"),
        participation_pct=breadth.get("participation_pct"),
    )
    regime_label = classify_regime_v1(
        volatility_label=volatility_label,
        breadth_label=breadth_label,
        trend_metrics={**trend, "spy_return_pct": spy.get("return_pct")},
        macro_context=macro,
    )
    missing = _missing_inputs(spy=spy, qqq=qqq, vix=vix, breadth=breadth, volatility=volatility, trend=trend)
    stale_status, stale_reasons = _stale_status(data=data, generated_at_utc=generated_at_utc, missing=missing)
    inputs = _event_inputs(
        data=data,
        spy=spy,
        qqq=qqq,
        vix=vix,
        breadth=breadth,
        volatility=volatility,
        trend=trend,
        macro=macro,
    )
    reason_codes = sorted(set([*stale_reasons, *[f"MISSING_INPUT:{key}" for key in missing]]))
    payload = {
        "schema_id": "event_market_snapshot",
        "schema_version": "v1",
        "artifact_id": "event_market_snapshot_v1",
        "snapshot_id": str(data.get("snapshot_id") or f"event_market_snapshot:{day_utc}"),
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc,
        "market_open_status": str(data.get("market_open_status") or "UNKNOWN").upper(),
        "trading_day_type": str(data.get("trading_day_type") or "UNKNOWN").upper(),
        "spy": spy,
        "qqq": qqq,
        "vix": vix,
        "breadth_metrics": breadth,
        "breadth_classification": breadth_label,
        "volatility_metrics": volatility,
        "volatility_classification": volatility_label,
        "trend_metrics": trend,
        "macro_event_today": bool(macro["macro_event_today"]),
        "macro_event_type": str(macro["macro_event_type"]),
        "macro_event_risk_level": str(macro["macro_event_risk_level"]),
        "macro_events_today": macro["macro_events_today"],
        "regime_label": regime_label,
        "regime_ruleset_version": RULESET_VERSION,
        "classification_rules": classification_rules_v1(),
        "stale_data_status": stale_status,
        "reason_codes": reason_codes,
        "inputs": inputs,
        "current_prices": {"SPY": spy.get("price", ""), "QQQ": qqq.get("price", ""), "VIX": vix.get("price", "")},
        "symbols": ["QQQ", "SPY", "VIX"],
        "assets_affected": ["SPY", "QQQ"],
        "data_snapshot_refs": _strings(data.get("data_snapshot_refs")) or [f"event_market_snapshot_v1:{day_utc}"],
        "source_lineage": sorted(source_lineage or _objects(data.get("source_lineage")), key=lambda row: (str(row.get("artifact_type") or ""), str(row.get("path") or ""))),
        "manual_execution_only": True,
        "broker_submit_required": False,
        "canonical_eod_state_mutated": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def market_context_summary_v1(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    payload = snapshot if isinstance(snapshot, dict) else {}
    return {
        "status": "PRESENT" if payload else "MISSING",
        "snapshot_id": str(payload.get("snapshot_id") or ""),
        "day_utc": str(payload.get("day_utc") or ""),
        "generated_at_utc": str(payload.get("generated_at_utc") or ""),
        "market_open_status": str(payload.get("market_open_status") or "UNKNOWN"),
        "trading_day_type": str(payload.get("trading_day_type") or "UNKNOWN"),
        "regime_label": str(payload.get("regime_label") or "UNKNOWN"),
        "volatility_classification": str(payload.get("volatility_classification") or "UNKNOWN"),
        "breadth_classification": str(payload.get("breadth_classification") or "UNKNOWN"),
        "macro_event_today": bool(payload.get("macro_event_today", False)),
        "macro_event_type": str(payload.get("macro_event_type") or "NONE"),
        "macro_event_risk_level": str(payload.get("macro_event_risk_level") or "UNKNOWN"),
        "stale_data_status": str(payload.get("stale_data_status") or "MISSING"),
        "reason_codes": _strings(payload.get("reason_codes")),
        "ruleset_version": str(payload.get("regime_ruleset_version") or RULESET_VERSION),
    }


def classification_rules_v1() -> dict[str, Any]:
    return {
        "ruleset_version": RULESET_VERSION,
        "volatility": [
            "PANIC_VOL when VIX>=35, VIX change>=25%, realized vol>=35, or expansion ratio>=2.0.",
            "HIGH_VOL when VIX>=24, VIX change>=10%, realized vol>=24, or expansion ratio>=1.35.",
            "LOW_VOL when VIX<14, realized vol<=12, and expansion ratio<0.9.",
            "NORMAL_VOL otherwise when volatility inputs exist; UNKNOWN when all volatility inputs are missing.",
        ],
        "breadth": [
            "BREADTH_COLLAPSE when breadth_down_pct>=75 or advance_decline_delta<=-1000.",
            "WEAK_BREADTH when breadth_down_pct>=60 or participation_pct<45.",
            "STRONG_BREADTH when breadth_down_pct<=42 and advance_decline_delta>=500.",
            "MIXED_BREADTH otherwise when breadth inputs exist; UNKNOWN when breadth inputs are missing.",
        ],
        "macro_event": [
            "Supported scheduled events are FOMC, CPI, JOBS_REPORT, and MAJOR_MACRO.",
            "Risk is the maximum declared risk for events matching day_utc; NONE when no event is scheduled.",
        ],
        "regime": [
            "PANIC takes precedence during PANIC_VOL or severe index loss with breadth collapse.",
            "DEFENSIVE_ROTATION takes precedence when defensive_rotation_score>=0.7 and cyclical_relative_strength<=-0.3.",
            "HIGH_VOLATILITY follows HIGH_VOL when panic/defensive rules do not apply.",
            "RECOVERY requires recent selloff, positive current return, and rebound breadth.",
            "TRENDING_UP requires positive 20-day SPY return, above 50dma, supportive breadth, and non-high volatility.",
            "TRENDING_DOWN requires negative 20-day SPY return and weak/broken trend or breadth.",
            "RANGE_BOUND requires absolute 20-day SPY return below 2% and LOW/NORMAL volatility.",
        ],
    }


def _instrument(symbol: str, data: dict[str, Any]) -> dict[str, str]:
    nested = data.get(symbol.lower()) if isinstance(data.get(symbol.lower()), dict) else data.get(symbol) if isinstance(data.get(symbol), dict) else {}
    prefix = symbol.lower()
    price = _first(nested.get("price"), data.get(f"{prefix}_price"), data.get(f"{symbol}_price"))
    prev_close = _first(nested.get("prev_close"), data.get(f"{prefix}_prev_close"), data.get(f"{symbol}_prev_close"))
    return_pct = _first(nested.get("return_pct"), data.get(f"{prefix}_return_pct"), data.get(f"{symbol}_return_pct"))
    if return_pct == "":
        return_pct = _pct_change(price, prev_close)
    return {
        "symbol": symbol,
        "price": _fmt(price),
        "prev_close": _fmt(prev_close),
        "return_pct": _fmt(return_pct),
    }


def _breadth_metrics(data: dict[str, Any]) -> dict[str, str]:
    breadth = data.get("breadth_metrics") if isinstance(data.get("breadth_metrics"), dict) else data.get("breadth") if isinstance(data.get("breadth"), dict) else {}
    advancing = _first(breadth.get("advancing_issues"), data.get("advancing_issues"))
    declining = _first(breadth.get("declining_issues"), data.get("declining_issues"))
    unchanged = _first(breadth.get("unchanged_issues"), data.get("unchanged_issues"))
    delta = _first(breadth.get("advance_decline_delta"), data.get("advance_decline_delta"))
    down_pct = _first(breadth.get("breadth_down_pct"), data.get("breadth_down_pct"))
    participation = _first(breadth.get("participation_pct"), data.get("participation_pct"))
    adv = _number(advancing)
    dec = _number(declining)
    unc = _number(unchanged)
    total = None if adv is None and dec is None and unc is None else (adv or 0) + (dec or 0) + (unc or 0)
    if delta == "":
        delta = "" if adv is None and dec is None else (adv or 0) - (dec or 0)
    if down_pct == "":
        down_pct = "" if not total else (dec or 0) / total * 100
    if participation == "":
        participation = "" if not total else ((adv or 0) + (dec or 0)) / total * 100
    return {
        "advancing_issues": _fmt(advancing),
        "declining_issues": _fmt(declining),
        "unchanged_issues": _fmt(unchanged),
        "advance_decline_delta": _fmt(delta),
        "breadth_down_pct": _fmt(down_pct),
        "participation_pct": _fmt(participation),
    }


def _volatility_metrics(data: dict[str, Any], *, vix: dict[str, str]) -> dict[str, str]:
    vol = data.get("volatility_metrics") if isinstance(data.get("volatility_metrics"), dict) else data.get("volatility") if isinstance(data.get("volatility"), dict) else {}
    realized = _first(vol.get("spy_realized_vol"), data.get("spy_realized_vol"))
    realized_prev = _first(vol.get("spy_realized_vol_prev"), data.get("spy_realized_vol_prev"))
    realized_change = _first(vol.get("realized_vol_change_pct"), data.get("realized_vol_change_pct"))
    if realized_change == "":
        realized_change = _pct_change(realized, realized_prev)
    expansion = _first(vol.get("volatility_expansion_ratio"), data.get("volatility_expansion_ratio"))
    atr = _first(vol.get("recent_atr_pct"), vol.get("atr_pct"), data.get("recent_atr_pct"), data.get("atr_pct"))
    return {
        "vix_level": _fmt(_first(vol.get("vix_level"), vix.get("price"), data.get("vix_level"))),
        "vix_change_pct": _fmt(_first(vol.get("vix_change_pct"), vix.get("return_pct"), data.get("vix_change_pct"))),
        "spy_realized_vol": _fmt(realized),
        "spy_realized_vol_prev": _fmt(realized_prev),
        "realized_vol_change_pct": _fmt(realized_change),
        "recent_atr_pct": _fmt(atr),
        "volatility_expansion_ratio": _fmt(expansion),
    }


def _trend_metrics(data: dict[str, Any], *, spy: dict[str, str], qqq: dict[str, str]) -> dict[str, Any]:
    trend = data.get("trend_metrics") if isinstance(data.get("trend_metrics"), dict) else data.get("trend") if isinstance(data.get("trend"), dict) else {}
    return {
        "spy_20d_return_pct": _fmt(_first(trend.get("spy_20d_return_pct"), data.get("spy_20d_return_pct"))),
        "spy_50d_return_pct": _fmt(_first(trend.get("spy_50d_return_pct"), data.get("spy_50d_return_pct"))),
        "qqq_20d_return_pct": _fmt(_first(trend.get("qqq_20d_return_pct"), data.get("qqq_20d_return_pct"))),
        "spy_above_20dma": _bool_value(_first(trend.get("spy_above_20dma"), data.get("spy_above_20dma"))),
        "spy_above_50dma": _bool_value(_first(trend.get("spy_above_50dma"), data.get("spy_above_50dma"))),
        "qqq_above_20dma": _bool_value(_first(trend.get("qqq_above_20dma"), data.get("qqq_above_20dma"))),
        "qqq_above_50dma": _bool_value(_first(trend.get("qqq_above_50dma"), data.get("qqq_above_50dma"))),
        "recent_selloff_pct": _fmt(_first(trend.get("recent_selloff_pct"), data.get("recent_selloff_pct"))),
        "bounce_failure": _bool_value(_first(trend.get("bounce_failure"), data.get("bounce_failure"))),
        "rebound_breadth_pct": _fmt(_first(trend.get("rebound_breadth_pct"), data.get("rebound_breadth_pct"))),
        "breakout_attempt": _bool_value(_first(trend.get("breakout_attempt"), data.get("breakout_attempt"))),
        "failed_follow_through": _bool_value(_first(trend.get("failed_follow_through"), data.get("failed_follow_through"))),
        "intraday_reversal_pct": _fmt(_first(trend.get("intraday_reversal_pct"), data.get("intraday_reversal_pct"))),
        "defensive_rotation_score": _fmt(_first(trend.get("defensive_rotation_score"), data.get("defensive_rotation_score"))),
        "cyclical_relative_strength": _fmt(_first(trend.get("cyclical_relative_strength"), data.get("cyclical_relative_strength"))),
    }


def _event_inputs(*, data: dict[str, Any], spy: dict[str, str], qqq: dict[str, str], vix: dict[str, str], breadth: dict[str, str], volatility: dict[str, str], trend: dict[str, Any], macro: dict[str, Any]) -> dict[str, Any]:
    explicit = data.get("inputs") if isinstance(data.get("inputs"), dict) else {}
    inputs = {
        "index_return_pct": spy.get("return_pct", ""),
        "qqq_return_pct": qqq.get("return_pct", ""),
        "vix_change_pct": volatility.get("vix_change_pct", ""),
        "breadth_down_pct": breadth.get("breadth_down_pct", ""),
        "advance_decline_delta": breadth.get("advance_decline_delta", ""),
        "realized_vol_change_pct": volatility.get("realized_vol_change_pct", ""),
        "macro_event_present": bool(macro.get("macro_event_today", False)),
        "event_gap_pct": _fmt(_first(data.get("event_gap_pct"), explicit.get("event_gap_pct"))),
        "late_session_stabilization": _bool_value(_first(data.get("late_session_stabilization"), explicit.get("late_session_stabilization"))),
        "recent_selloff_pct": trend.get("recent_selloff_pct", ""),
        "bounce_failure": trend.get("bounce_failure"),
        "rebound_breadth_pct": trend.get("rebound_breadth_pct", ""),
        "breakout_attempt": trend.get("breakout_attempt"),
        "failed_follow_through": trend.get("failed_follow_through"),
        "intraday_reversal_pct": trend.get("intraday_reversal_pct", ""),
        "defensive_rotation_score": trend.get("defensive_rotation_score", ""),
        "cyclical_relative_strength": trend.get("cyclical_relative_strength", ""),
    }
    for key, value in explicit.items():
        inputs[str(key)] = value
    return {key: value for key, value in sorted(inputs.items()) if value != "" and value is not None}


def _missing_inputs(*, spy: dict[str, str], qqq: dict[str, str], vix: dict[str, str], breadth: dict[str, str], volatility: dict[str, str], trend: dict[str, Any]) -> list[str]:
    required = {
        "spy_price": spy.get("price"),
        "spy_return_pct": spy.get("return_pct"),
        "qqq_price": qqq.get("price"),
        "qqq_return_pct": qqq.get("return_pct"),
        "vix_level": volatility.get("vix_level"),
        "vix_change_pct": volatility.get("vix_change_pct"),
        "breadth_down_pct": breadth.get("breadth_down_pct"),
        "advance_decline_delta": breadth.get("advance_decline_delta"),
        "spy_20d_return_pct": trend.get("spy_20d_return_pct"),
        "spy_above_50dma": trend.get("spy_above_50dma"),
    }
    return sorted(key for key, value in required.items() if value == "" or value is None)


def _stale_status(*, data: dict[str, Any], generated_at_utc: str, missing: list[str]) -> tuple[str, list[str]]:
    source_ts = str(data.get("source_timestamp_utc") or data.get("latest_source_timestamp_utc") or generated_at_utc or "")
    if missing:
        base = "MISSING_INPUT"
        reasons = ["MARKET_CONTEXT_INPUTS_MISSING"]
    else:
        base = "FRESH"
        reasons = []
    parsed_source = _parse_utc(source_ts)
    parsed_generated = _parse_utc(generated_at_utc)
    if parsed_source is None or parsed_generated is None:
        return ("STALE" if not missing else base, sorted(set([*reasons, "MARKET_CONTEXT_TIMESTAMP_UNPARSEABLE"])))
    age_minutes = (parsed_generated - parsed_source).total_seconds() / 60
    if age_minutes > SNAPSHOT_STALE_AFTER_MINUTES:
        return "STALE", sorted(set([*reasons, "MARKET_CONTEXT_SOURCE_STALE"]))
    return base, sorted(set(reasons))


def _macro_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        if isinstance(value.get("events"), list):
            return [row for row in value["events"] if isinstance(row, dict)]
        return [value]
    return []


def _normalize_macro_event(row: dict[str, Any]) -> dict[str, str]:
    event_type = str(row.get("event_type") or row.get("type") or row.get("name") or "MAJOR_MACRO").strip().upper()
    aliases = {"JOBS": "JOBS_REPORT", "NONFARM_PAYROLLS": "JOBS_REPORT", "NFP": "JOBS_REPORT", "FOMC_MEETING": "FOMC"}
    event_type = aliases.get(event_type, event_type)
    if event_type not in {"FOMC", "CPI", "JOBS_REPORT", "MAJOR_MACRO"}:
        event_type = "MAJOR_MACRO"
    risk = str(row.get("risk_level") or row.get("macro_event_risk_level") or "MEDIUM").strip().upper()
    if risk not in MACRO_RISK_LEVELS:
        risk = "MEDIUM"
    return {
        "date": str(row.get("date") or row.get("day_utc") or ""),
        "event_type": event_type,
        "event_time_utc": str(row.get("event_time_utc") or row.get("time_utc") or ""),
        "risk_level": risk,
        "description": str(row.get("description") or row.get("name") or event_type),
    }


def _max_risk(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "NONE"
    rank = {"NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "EXTREME": 4, "UNKNOWN": -1}
    return sorted((row.get("risk_level", "UNKNOWN") for row in rows), key=lambda item: rank.get(item, -1), reverse=True)[0]


def _first(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return ""


def _pct_change(value: Any, previous: Any) -> str:
    current = _number(value)
    prior = _number(previous)
    if current is None or prior in {None, 0}:
        return ""
    return _fmt(((current - prior) / prior) * 100)


def _number(value: Any) -> float | None:
    try:
        text = str(value).strip().replace("%", "").replace(",", "")
        return float(text) if text else None
    except (TypeError, ValueError):
        return None


def _fmt(value: Any) -> str:
    number = _number(value)
    if number is None:
        return ""
    text = f"{number:.6f}".rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def _bool_value(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    return _boolish(value)


def _boolish(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _gte(value: float | None, threshold: float) -> bool:
    return value is not None and value >= threshold


def _lte(value: float | None, threshold: float) -> bool:
    return value is not None and value <= threshold


def _lt(value: float | None, threshold: float) -> bool:
    return value is not None and value < threshold


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _objects(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _parse_utc(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
