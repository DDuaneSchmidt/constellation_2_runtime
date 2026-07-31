from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_ID = "narrative_operational_analytics"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "narrative_operational_analytics_v1"

SAFETY_FLAGS = {
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
}

CATEGORIES = {
    "PERFORMANCE",
    "CANDIDATE_FUNNEL",
    "CERTIFICATION",
    "SLEEVE_ATTRIBUTION",
    "EXIT_REVIEW",
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256_file(path: Path | None) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                h.update(chunk)
    except OSError:
        return ""
    return h.hexdigest()


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _safe_int(value: Any) -> int:
    number = _coerce_float(value)
    return int(number) if number is not None else 0


def _money(value: Any) -> str:
    number = _coerce_float(value)
    if number is None:
        return "not available"
    sign = "-" if number < 0 else ""
    return f"{sign}${abs(number):,.2f}"


def _percent(value: Any) -> str:
    number = _coerce_float(value)
    if number is None:
        return "not available"
    return f"{number:.2f}%"


def _latest_artifact(root: Path, family: str, day_utc: str, filename: str) -> Path | None:
    base = root / "reports" / family / day_utc
    if not base.exists() or not base.is_dir():
        return None
    direct = base / filename
    candidates = [direct] if direct.exists() and direct.is_file() else []
    candidates.extend(path for path in base.glob(f"**/{filename}") if path.exists() and path.is_file() and path != direct)
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: (path.stat().st_mtime_ns, str(path)), reverse=True)[0]


def _report_days(root: Path, families: Iterable[str], *, max_day: str, limit: int = 20) -> list[str]:
    days: set[str] = set()
    for family in families:
        base = root / "reports" / family
        if base.exists() and base.is_dir():
            days.update(path.name for path in base.iterdir() if path.is_dir() and path.name <= max_day)
    return sorted(days)[-max(1, int(limit)):]


def narrative_operational_analytics_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / "narrative_operational_analytics.v1.json"


def _paper_projection_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "paper_trade_evaluation_projection_v1" / day_utc / "paper_trade_evaluation_projection.v1.json"


def _trade_ledger_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "trade_lifecycle_ledger_v1" / day_utc / "trade_lifecycle_ledger.v1.json"


def _dynamic_certified_universe_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "dynamic_certified_universe_v1" / day_utc / "dynamic_certified_universe.v1.json"


def _statement(
    rows: list[dict[str, Any]],
    *,
    statement: str,
    category: str,
    supporting_metric: str,
    supporting_artifact_path: str,
    confidence: str = "HIGH",
    evidence_status: str = "COMPLETE",
    artifact_hash: str = "",
) -> None:
    category = category if category in CATEGORIES else "PERFORMANCE"
    if not supporting_artifact_path:
        return
    rows.append(
        {
            "statement": statement,
            "category": category,
            "supporting_metric": supporting_metric,
            "supporting_artifact_path": supporting_artifact_path,
            "supporting_artifact_hash": artifact_hash,
            "confidence": confidence,
            "evidence_status": evidence_status,
        }
    )


def _chart(chart_id: str, title: str, category: str, points: list[dict[str, Any]], *, artifact_path: str, insufficient_message: str) -> dict[str, Any]:
    status = "READY" if points else "INSUFFICIENT_HISTORY"
    return {
        "chart_id": chart_id,
        "title": title,
        "category": category,
        "status": status,
        "points": points,
        "supporting_artifact_path": artifact_path if points else "",
        "empty_state": "" if points else insufficient_message,
    }


def _performance_trend(root: Path, day_utc: str, projection: Mapping[str, Any], projection_path: Path) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for day in _report_days(root, ["portfolio_performance_truth_v1"], max_day=day_utc, limit=20):
        path = root / "reports" / "portfolio_performance_truth_v1" / day / "portfolio_performance_truth.v1.json"
        payload = _read_json(path)
        if not payload:
            continue
        ret = payload.get("portfolio_return") if isinstance(payload.get("portfolio_return"), dict) else {}
        pnl = _coerce_float(ret.get("daily_pnl") or ret.get("total_pnl") or payload.get("total_pnl"))
        if pnl is None:
            continue
        points.append({"label": day, "value": pnl, "series": "portfolio_daily_pnl", "artifact_path": str(path)})
    current_total = _coerce_float(projection.get("total_pnl"))
    if current_total is not None and not any(point.get("label") == day_utc for point in points):
        points.append({"label": day_utc, "value": current_total, "series": "paper_total_pnl", "artifact_path": str(projection_path)})
    return points if len(points) >= 2 else []


def _dynamic_universe_growth(root: Path, day_utc: str) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for day in _report_days(root, ["dynamic_certified_universe_v1"], max_day=day_utc, limit=20):
        path = _dynamic_certified_universe_path(root, day)
        payload = _read_json(path)
        if not payload:
            continue
        symbols = payload.get("dynamic_certified_symbols") if isinstance(payload.get("dynamic_certified_symbols"), list) else []
        points.append({"label": day, "value": len(symbols), "series": "dynamic_certified_symbols", "artifact_path": str(path)})
    return points if len(points) >= 2 else []


def _candidate_trend_points(trend: list[dict[str, Any]], key: str, series: str, artifact_path: str) -> list[dict[str, Any]]:
    points = [
        {"label": str(row.get("trading_day") or ""), "value": _safe_int(row.get(key)), "series": series, "artifact_path": artifact_path}
        for row in trend
        if isinstance(row, dict) and str(row.get("trading_day") or "")
    ]
    return points if len(points) >= 2 else []


def _supporting_sources(root: Path, day_utc: str, candidate_funnel_projection: Mapping[str, Any]) -> dict[str, str]:
    artifacts = candidate_funnel_projection.get("artifact_paths") if isinstance(candidate_funnel_projection.get("artifact_paths"), dict) else {}
    queue = candidate_funnel_projection.get("dynamic_certification_queue") if isinstance(candidate_funnel_projection.get("dynamic_certification_queue"), dict) else {}
    return {
        "paper_trade_evaluation_projection_v1": str(_paper_projection_path(root, day_utc)),
        "trade_lifecycle_ledger_v1": str(_trade_ledger_path(root, day_utc)),
        "candidate_consumption_audit_v1": str(artifacts.get("candidate_consumption_audit_v1") or _latest_artifact(root, "candidate_consumption_audit_v1", day_utc, "candidate_consumption_audit.v1.json") or ""),
        "dynamic_certification_queue_v1": str(queue.get("artifact_path") or _latest_artifact(root, "dynamic_certification_queue_v1", day_utc, "dynamic_certification_queue.v1.json") or ""),
        "dynamic_certified_universe_v1": str(_dynamic_certified_universe_path(root, day_utc) if _dynamic_certified_universe_path(root, day_utc).exists() else ""),
    }


def build_narrative_operational_analytics_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    paper_trade_evaluation_projection: Mapping[str, Any] | None = None,
    candidate_funnel_projection: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    projection_path = _paper_projection_path(root, day)
    projection = dict(paper_trade_evaluation_projection or _read_json(projection_path))
    funnel = dict(candidate_funnel_projection or {})
    sources = _supporting_sources(root, day, funnel)
    statements: list[dict[str, Any]] = []

    paper_path = sources["paper_trade_evaluation_projection_v1"] if projection_path.exists() or projection else ""
    paper_hash = str(projection.get("content_hash") or _sha256_file(projection_path))
    if projection and paper_path:
        total = _coerce_float(projection.get("total_pnl"))
        realized = _coerce_float(projection.get("realized_pnl"))
        unrealized = _coerce_float(projection.get("unrealized_pnl"))
        _statement(
            statements,
            statement=f"Total paper P&L is {_money(total)}: realized {_money(realized)} and unrealized {_money(unrealized)}.",
            category="PERFORMANCE",
            supporting_metric="paper_trade_evaluation_projection_v1.total_pnl",
            supporting_artifact_path=paper_path,
            artifact_hash=paper_hash,
        )
        for trade in projection.get("open_trades") or []:
            if not isinstance(trade, Mapping):
                continue
            symbol = str(trade.get("symbol") or trade.get("trade_id") or "Trade")
            entry = _coerce_float(trade.get("entry_price"))
            mark = _coerce_float(trade.get("current_mark"))
            pnl = _coerce_float(trade.get("unrealized_pnl"))
            if entry is None or mark is None or pnl is None:
                continue
            direction = "below" if mark < entry else "above" if mark > entry else "equal to"
            magnitude = "slightly " if abs(_coerce_float(trade.get("return_pct")) or 0.0) < 1 else ""
            tone = "negative" if pnl < 0 else "positive" if pnl > 0 else "flat"
            _statement(
                statements,
                statement=f"{symbol} unrealized P&L is {magnitude}{tone} because current mark is {direction} entry.",
                category="PERFORMANCE",
                supporting_metric=f"open_trades[{trade.get('trade_id') or symbol}].unrealized_pnl",
                supporting_artifact_path=paper_path,
                artifact_hash=paper_hash,
            )
        for row in projection.get("sleeve_attribution") or []:
            if not isinstance(row, Mapping):
                continue
            if _coerce_float(row.get("total_pnl")) is None:
                continue
            _statement(
                statements,
                statement=f"Sleeve {row.get('id') or 'UNATTRIBUTED'} has total paper P&L of {_money(row.get('total_pnl'))} across {_safe_int(row.get('trade_count'))} trade(s).",
                category="SLEEVE_ATTRIBUTION",
                supporting_metric=f"sleeve_attribution[{row.get('id') or 'UNATTRIBUTED'}].total_pnl",
                supporting_artifact_path=paper_path,
                artifact_hash=paper_hash,
            )

    audit_path = sources.get("candidate_consumption_audit_v1", "")
    audit_hash = _sha256_file(Path(audit_path)) if audit_path else ""
    raw = _safe_int(funnel.get("raw_candidate_count"))
    promoted = _safe_int(funnel.get("promoted_candidate_count"))
    uncovered = _safe_int(funnel.get("excluded_uncovered_symbol_count"))
    low_score = _safe_int(funnel.get("excluded_low_score_count"))
    policy = _safe_int(funnel.get("excluded_policy_count"))
    if raw and audit_path:
        if promoted == 0:
            _statement(
                statements,
                statement=f"Candidate promotion remains zero because {uncovered} candidates are outside certified coverage, {low_score} failed score, and {policy} failed policy.",
                category="CANDIDATE_FUNNEL",
                supporting_metric="candidate_funnel_projection.exclusion_counts",
                supporting_artifact_path=audit_path,
                artifact_hash=audit_hash,
            )
        else:
            _statement(
                statements,
                statement=f"{promoted} candidate(s) promoted from {raw} raw candidates after certified-universe, score, and policy checks.",
                category="CANDIDATE_FUNNEL",
                supporting_metric="candidate_funnel_projection.promoted_candidate_count",
                supporting_artifact_path=audit_path,
                artifact_hash=audit_hash,
            )

    queue = funnel.get("dynamic_certification_queue") if isinstance(funnel.get("dynamic_certification_queue"), dict) else {}
    queue_path = sources.get("dynamic_certification_queue_v1", "")
    queue_hash = _sha256_file(Path(queue_path)) if queue_path else str(queue.get("artifact_content_hash") or "")
    requested = [str(symbol).upper() for symbol in queue.get("requested_symbols") or []]
    certified = [str(row.get("symbol") or "").upper() for row in queue.get("certification_results") or [] if isinstance(row, Mapping) and str(row.get("certification_status") or "").upper() == "CERTIFIED"]
    if queue_path and requested:
        trend = funnel.get("trend_5d") if isinstance(funnel.get("trend_5d"), list) else []
        reduction = 0
        if len(trend) >= 2:
            before = _safe_int(trend[-2].get("excluded_uncovered_symbol_count")) if isinstance(trend[-2], Mapping) else 0
            after = _safe_int(trend[-1].get("excluded_uncovered_symbol_count")) if isinstance(trend[-1], Mapping) else 0
            reduction = max(0, before - after)
        named = ", ".join(certified or requested[:3])
        suffix = f", reducing uncovered exclusions by {reduction}" if reduction else ""
        _statement(
            statements,
            statement=f"Dynamic certification selected {named} from uncovered candidate pressure{suffix}.",
            category="CERTIFICATION",
            supporting_metric="dynamic_certification_queue_v1.requested_symbols",
            supporting_artifact_path=queue_path,
            artifact_hash=queue_hash,
        )

    dynamic_universe_path = sources.get("dynamic_certified_universe_v1", "")
    dynamic_universe = _read_json(Path(dynamic_universe_path)) if dynamic_universe_path else {}
    dynamic_symbols = dynamic_universe.get("dynamic_certified_symbols") if isinstance(dynamic_universe.get("dynamic_certified_symbols"), list) else []
    if dynamic_universe_path and dynamic_symbols:
        _statement(
            statements,
            statement=f"Dynamic certified universe growth is bounded to {len(dynamic_symbols)} added symbol(s), separate from the stable Tier 1 baseline.",
            category="CERTIFICATION",
            supporting_metric="dynamic_certified_universe_v1.dynamic_certified_symbols",
            supporting_artifact_path=dynamic_universe_path,
            artifact_hash=_sha256_file(Path(dynamic_universe_path)),
        )

    realized_vs_unrealized = []
    if projection and paper_path:
        for label, key in (("Realized", "realized_pnl"), ("Unrealized", "unrealized_pnl")):
            value = _coerce_float(projection.get(key))
            if value is not None:
                realized_vs_unrealized.append({"label": label, "value": value, "series": key, "artifact_path": paper_path})
    sleeve_points = []
    for row in projection.get("sleeve_attribution") or []:
        if isinstance(row, Mapping) and _coerce_float(row.get("total_pnl")) is not None:
            sleeve_points.append({"label": str(row.get("id") or "UNATTRIBUTED"), "value": _coerce_float(row.get("total_pnl")), "series": "sleeve_total_pnl", "artifact_path": paper_path})

    trend_rows = funnel.get("trend_5d") if isinstance(funnel.get("trend_5d"), list) else []
    charts = {
        "performance": [
            _chart("performance_pnl_trend", "Equity / P&L trend", "PERFORMANCE", _performance_trend(root, day, projection, projection_path), artifact_path=paper_path, insufficient_message="Not enough history yet."),
            _chart("realized_vs_unrealized_pnl", "Realized vs unrealized P&L", "PERFORMANCE", realized_vs_unrealized, artifact_path=paper_path, insufficient_message="No evaluable P&L components are available."),
            _chart("pnl_by_sleeve", "P&L by sleeve", "SLEEVE_ATTRIBUTION", sleeve_points, artifact_path=paper_path, insufficient_message="No sleeve attribution is available."),
        ],
        "candidate_funnel": [
            _chart("raw_vs_promoted_trend", "Raw vs promoted trend", "CANDIDATE_FUNNEL", _candidate_trend_points(trend_rows, "raw_candidate_count", "raw_candidates", audit_path) + _candidate_trend_points(trend_rows, "promoted_candidate_count", "promoted_candidates", audit_path), artifact_path=audit_path, insufficient_message="Not enough history yet."),
            _chart("excluded_uncovered_trend", "Excluded uncovered trend", "CANDIDATE_FUNNEL", _candidate_trend_points(trend_rows, "excluded_uncovered_symbol_count", "excluded_uncovered", audit_path), artifact_path=audit_path, insufficient_message="Not enough history yet."),
            _chart("exclusion_reason_breakdown", "Exclusion reason breakdown", "CANDIDATE_FUNNEL", [
                {"label": "Uncovered", "value": uncovered, "series": "EXCLUDED_UNCOVERED_SYMBOL", "artifact_path": audit_path},
                {"label": "Low score", "value": low_score, "series": "EXCLUDED_LOW_SCORE", "artifact_path": audit_path},
                {"label": "Policy", "value": policy, "series": "EXCLUDED_POLICY", "artifact_path": audit_path},
            ] if audit_path and raw else [], artifact_path=audit_path, insufficient_message="No candidate consumption audit counts are available."),
            _chart("dynamic_certified_universe_growth", "Dynamic certified universe growth", "CERTIFICATION", _dynamic_universe_growth(root, day), artifact_path=dynamic_universe_path, insufficient_message="Not enough history yet."),
        ],
    }

    metrics = {
        "performance": {
            "realized_pnl": projection.get("realized_pnl"),
            "unrealized_pnl": projection.get("unrealized_pnl"),
            "total_pnl": projection.get("total_pnl"),
            "return_pct": projection.get("return_pct"),
            "drawdown": None,
            "open_trade_count": projection.get("open_trade_count"),
            "closed_trade_count": projection.get("closed_trade_count"),
            "win_rate": None,
            "trade_count": projection.get("trade_count"),
        },
        "candidate_funnel": {
            "raw_candidates": raw,
            "promoted_candidates": promoted,
            "excluded_uncovered": uncovered,
            "low_score_exclusions": low_score,
            "policy_exclusions": policy,
            "dynamic_certified_universe_growth": len(dynamic_symbols),
        },
    }
    closed_complete = [row for row in projection.get("closed_trades") or [] if isinstance(row, Mapping) and _coerce_float(row.get("return_pct")) is not None]
    if len(closed_complete) >= 3:
        wins = sum(1 for row in closed_complete if (_coerce_float(row.get("return_pct")) or 0.0) > 0)
        metrics["performance"]["win_rate"] = round(wins / len(closed_complete), 6)

    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "operational_day": day,
        "day_utc": day,
        "generated_at_utc": generated_at_utc or utc_now_v1(),
        "metrics": metrics,
        "narrative_statements": statements,
        "charts": charts,
        "source_artifacts": sources,
        "evidence_policy": "Every narrative statement must include a deterministic supporting artifact path and metric. Missing history is rendered as an empty state, not a claim.",
        **SAFETY_FLAGS,
    }
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_narrative_operational_analytics_v1(*, truth_root: Path | str, payload: Mapping[str, Any]) -> dict[str, str]:
    path = narrative_operational_analytics_path_v1(truth_root=truth_root, day_utc=str(payload.get("operational_day") or payload.get("day_utc") or ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"json": str(path), "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload))}
